//! ADR 0542 acceptance 8: shared deterministic P/I/current fixture.
use super::*;
use async_trait::async_trait;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Notify;
use wendelon::storage::file_ops::{AsyncFileOps, BlockingFileOps, FileHandle, OpenFlags};
use wendelon::storage::io_uring::AlignedBuf;

mod expectation;

#[derive(Default)]
struct GateFileOps {
	inner: Mutex<BlockingFileOps>,
	unlink_path: Mutex<Option<std::path::PathBuf>>,
	unlink_hit: AtomicBool,
	unlink_entered: Notify,
	unlink_released: AtomicBool,
	unlink_release: Notify,
	armed: AtomicBool,
	writes: AtomicUsize,
	writes_at_arm: AtomicUsize,
	entered_at: Mutex<Option<std::time::Instant>>,
	events: Mutex<Vec<String>>,
	entered: Notify,
	release: Notify,
}
impl GateFileOps {
	fn inner(&self) -> BlockingFileOps {
		self.inner.lock().unwrap().clone()
	}
	fn arm_unlink(&self, path: std::path::PathBuf) {
		*self.unlink_path.lock().unwrap() = Some(path);
		self.unlink_hit.store(false, Ordering::SeqCst);
		self.unlink_released.store(false, Ordering::SeqCst);
	}
	fn release_unlink(&self) {
		self.unlink_released.store(true, Ordering::SeqCst);
		self.unlink_release.notify_waiters();
	}
	async fn wait_unlink(&self) {
		tokio::time::timeout(Duration::from_secs(5), async {
			loop {
				let entered = self.unlink_entered.notified();
				tokio::pin!(entered);
				entered.as_mut().enable();
				if self.unlink_hit.load(Ordering::SeqCst) {
					break;
				}
				entered.await;
			}
		})
		.await
		.expect("unlink gate entry within5s");
	}
	fn arm(&self) {
		self.writes_at_arm
			.store(self.writes.load(Ordering::SeqCst), Ordering::SeqCst);
		self.armed.store(true, Ordering::SeqCst);
	}
	fn release(&self) {
		self.armed.store(false, Ordering::SeqCst);
		self.events
			.lock()
			.unwrap()
			.push("release for observation/teardown".into());
		self.release.notify_waiters();
	}
	async fn wait_entered(&self) -> std::time::Instant {
		tokio::time::timeout(Duration::from_secs(5), self.entered.notified())
			.await
			.expect("gate entry within5s");
		self.entered_at.lock().unwrap().expect("entered timestamp")
	}
}
#[async_trait]
impl AsyncFileOps for GateFileOps {
	fn bind_directory_owner(
		self: Arc<Self>,
		owner: wendelon::storage::ownership::DataDirectoryOwner,
	) -> std::io::Result<Arc<dyn AsyncFileOps>> {
		*self.inner.lock().unwrap() = BlockingFileOps::with_owner(owner);
		Ok(self)
	}
	async fn open(&self, path: &Path, flags: OpenFlags) -> std::io::Result<FileHandle> {
		self.inner().open(path, flags).await
	}

	async fn read_plain_at(
		&self,
		handle: &FileHandle,
		offset: u64,
		len: usize,
	) -> std::io::Result<Vec<u8>> {
		self.inner().read_plain_at(handle, offset, len).await
	}

	async fn write_plain_at(
		&self,
		handle: &FileHandle,
		offset: u64,
		bytes: Vec<u8>,
	) -> std::io::Result<usize> {
		self.inner().write_plain_at(handle, offset, bytes).await
	}

	async fn read_at(
		&self,
		handle: &FileHandle,
		offset: u64,
		buf: AlignedBuf,
	) -> std::io::Result<(usize, AlignedBuf)> {
		self.inner().read_at(handle, offset, buf).await
	}

	async fn write_at(
		&self,
		handle: &FileHandle,
		offset: u64,
		buf: AlignedBuf,
	) -> std::io::Result<(usize, AlignedBuf)> {
		let result = self.inner().write_at(handle, offset, buf).await;
		if result.is_ok() {
			self.writes.fetch_add(1, Ordering::SeqCst);
		}
		result
	}

	async fn fsync(&self, handle: &FileHandle) -> std::io::Result<()> {
		self.inner().fsync(handle).await
	}

	async fn fdatasync(&self, handle: &FileHandle) -> std::io::Result<()> {
		if self.armed.load(Ordering::SeqCst) {
			let writes = self.writes.load(Ordering::SeqCst);
			assert!(
				writes > self.writes_at_arm.load(Ordering::SeqCst),
				"gate must follow append"
			);
			let path = std::fs::read_link(format!("/proc/self/fd/{}", handle.raw_fd()))?;
			assert!(
				path.file_name()
					.unwrap()
					.to_string_lossy()
					.starts_with("slab_")
			);
			self.events
				.lock()
				.unwrap()
				.push(format!("gate path={} writes={writes}", path.display()));
			*self.entered_at.lock().unwrap() = Some(std::time::Instant::now());
			self.entered.notify_one();
			loop {
				let release = self.release.notified();
				if !self.armed.load(Ordering::SeqCst) {
					break;
				}
				release.await;
			}
		}
		self.inner().fdatasync(handle).await
	}

	async fn read_all(&self, path: &Path) -> std::io::Result<Vec<u8>> {
		self.inner().read_all(path).await
	}

	async fn write_all(&self, path: &Path, bytes: &[u8]) -> std::io::Result<()> {
		self.inner().write_all(path, bytes).await
	}

	async fn file_len(&self, path: &Path) -> std::io::Result<Option<u64>> {
		self.inner().file_len(path).await
	}

	async fn set_len(&self, handle: &FileHandle, len: u64) -> std::io::Result<()> {
		self.inner().set_len(handle, len).await
	}

	async fn create_dir_all(&self, path: &Path) -> std::io::Result<()> {
		self.inner().create_dir_all(path).await
	}

	async fn read_dir(
		&self,
		path: &Path,
	) -> std::io::Result<Vec<wendelon::storage::DirectoryEntry>> {
		self.inner().read_dir(path).await
	}

	async fn fsync_dir(&self, path: &Path) -> std::io::Result<()> {
		self.inner().fsync_dir(path).await
	}

	async fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()> {
		self.inner().rename(from, to).await
	}

	async fn link_no_replace(&self, from: &Path, to: &Path) -> std::io::Result<()> {
		self.inner().link_no_replace(from, to).await
	}

	async fn unlink(&self, path: &Path) -> std::io::Result<()> {
		let selected = self.unlink_path.lock().unwrap().as_deref() == Some(path);
		if selected {
			self.unlink_hit.store(true, Ordering::SeqCst);
			self.unlink_entered.notify_waiters();
			loop {
				let released = self.unlink_release.notified();
				tokio::pin!(released);
				released.as_mut().enable();
				if self.unlink_released.load(Ordering::SeqCst) {
					break;
				}
				released.await;
			}
		}
		self.inner().unlink(path).await
	}
}

struct UnlinkReleaseGuard(Arc<GateFileOps>);
impl Drop for UnlinkReleaseGuard {
	fn drop(&mut self) {
		self.0.release_unlink();
	}
}

struct AcceptanceFixture {
	node: WendelonNode,
	client: Arc<Client>,
	data_dir: std::path::PathBuf,
	_serial_permit: tokio::sync::OwnedSemaphorePermit,
}

impl AcceptanceFixture {
	async fn start(node_id: u32, ops: Arc<GateFileOps>, boundary: bool) -> Self {
		let serial_permit = live_wendelon_fixture_semaphore()
			.clone()
			.acquire_owned()
			.await
			.expect("live Wendelon fixture semaphore open");
		let data_dir = std::env::temp_dir().join(format!(
			"toron-wendelon-live-{node_id}-{}",
			rand::random::<u64>()
		));
		std::fs::create_dir_all(&data_dir).expect("create Wendelon data dir");
		let address = format!("sim://node-{node_id}");
		let mut config = NodeBootstrapConfig::dev(node_id, data_dir.clone(), address.clone());
		config.slab_file_ops = Some(wendelon::node::TestSupportFileOps::new(ops));
		config.lsm_compaction_paused = true;
		if boundary {
			config.slab_max_bytes = 64 * 1024;
		}
		let node = WendelonNode::bootstrap_single_node(config)
			.await
			.expect("bootstrap Wendelon node");
		wait_for_single_node_leader(&node).await;
		create_live_tenant(&node, LIVE_TENANT).await;
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;

		let pool = BufPool::new(64, 8 * 1024 * 1024);
		let network = Arc::new(SimulatedTransportNetwork::default());
		let client_node = wendelon::schema::NodeId {
			id: node_id + 10_000,
			_reserved: 0,
		};
		let provider: Arc<dyn TransportProvider> = Arc::new(SimulatedTransportProvider::new(
			client_node,
			network,
			pool,
			TransportTier::QuicIoUring,
		));
		provider
			.publish_local(node.local_node, &address, node.service.clone())
			.expect("publish simulated Wendelon service");
		let snapshot = single_node_snapshot(node_id, address);
		let mut providers = HashMap::new();
		providers.insert(TransportTier::QuicIoUring, provider);
		let inner = wendelon::client::WendelonClient::new(wendelon::client::WendelonClientConfig {
			local_tiers: TransportTierSet::empty().with(TransportTier::QuicIoUring),
			providers,
			bootstrap: snapshot.clone(),
			topology: Arc::new(StaticTopologySource::new(snapshot)),
			pool_max_retained: 64,
			pool_max_cap_bytes: 8 * 1024 * 1024,
		});
		let client = Arc::new(Client::from_inner(inner));

		Self {
			node,
			client,
			data_dir,
			_serial_permit: serial_permit,
		}
	}
}

impl AcceptanceFixture {
	async fn close(self) {
		drop(self.client);
		self.node.shutdown().await.expect("join node shutdown");
		std::fs::remove_dir_all(self.data_dir).expect("remove fixture directory after shutdown");
	}
	async fn prepare(&self) -> TableSchemaDescriptor {
		let target = descriptor();
		install_schema(&self.node, LIVE_TENANT, &target).await;
		let control = TableSchemaDescriptor::new(
			TableObjectId::new([0x54; 16]),
			1,
			"bench",
			"acc8_control",
			&schema(),
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
			LocalityPolicy::IdentityProjection,
			SchemaEvolutionPolicy::AdditiveOnly,
		)
		.unwrap();
		install_schema(&self.node, LIVE_TENANT, &control).await;
		write_rows(&self.client, LIVE_TENANT, &control, input(0, 16, false)).await;
		target
	}
}
fn input(start: i64, rows: usize, boundary: bool) -> RecordBatch {
	let mut state = 0x5428u64;
	let payloads: Vec<String> = (0..rows)
		.map(|_| {
			if !boundary {
				return "acc8".into();
			}
			(0..2048)
				.map(|_| {
					state ^= state << 13;
					state ^= state >> 7;
					state ^= state << 17;
					char::from(b'!' + (state % 90) as u8)
				})
				.collect()
		})
		.collect();
	let tuples: Vec<_> = (0..rows)
		.map(|n| {
			(
				start + n as i64,
				Some(n as i32),
				Some(payloads[n].as_str()),
				BASE_TS + n as i64,
			)
		})
		.collect();
	batch_from_rows(&tuples)
}
async fn count(client: Arc<Client>, descriptor: &TableSchemaDescriptor) -> i64 {
	let registry = descriptor_registry_for_tenant(LIVE_TENANT, descriptor);
	let catalog = WendelonCatalog::with_client_and_registry(
		client.clone(),
		toron_tenant_id(LIVE_TENANT),
		registry.clone(),
	);
	let storage = Arc::new(WendelonStorageScan::with_descriptor_registry(
		client, registry,
	));
	let executor = Executor::with_context(
		storage,
		ExecutionContext::new(
			SessionId(5428),
			QueryId::from_u128(5428),
			toron_tenant_id(LIVE_TENANT),
			HlcBound(0),
		),
	);
	let logical = LogicalPlan::Aggregate {
		input: Box::new(LogicalPlan::Scan {
			table: TableId(1),
			projection: None,
		}),
		group_expr: vec![],
		aggr_expr: vec![(AggregateFunction::CountAll, vec![])],
	};
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind COUNT(*)");
	let batches = executor
		.execute_bound(&bound, None)
		.await
		.expect("execute COUNT(*)");
	let batch = concat_batches_for_assertion(&batches);
	batch
		.column(0)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("COUNT int64")
		.value(0)
}
fn options() -> WriteOptions {
	WriteOptions {
		await_finalization: true,
		..WriteOptions::default()
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acc8_no_injection_commits_exact16() {
	let ops = Arc::new(GateFileOps::default());
	let fixture = AcceptanceFixture::start(54280, ops, false).await;
	let d = fixture.prepare().await;
	let result = tokio::time::timeout(
		Duration::from_secs(35),
		fixture
			.client
			.table_write(LIVE_TENANT, &d, input(0, 16, false), options()),
	)
	.await
	.expect("control bound")
	.expect("committed control");
	assert_eq!(result.rows, 16);
	assert_eq!(count(fixture.client.clone(), &d).await, 16);
	fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acc8_transient_postappend_hold_commits_exact16() {
	let ops = Arc::new(GateFileOps::default());
	let fixture = AcceptanceFixture::start(54281, ops.clone(), false).await;
	let d = fixture.prepare().await;
	ops.arm();
	let client = fixture.client.clone();
	let target = d.clone();
	let mut write = tokio::spawn(async move {
		client
			.table_write(LIVE_TENANT, &target, input(0, 16, false), options())
			.await
	});
	let entered = ops.wait_entered().await;
	tokio::time::sleep_until(tokio::time::Instant::from_std(
		entered + Duration::from_millis(100),
	))
	.await;
	assert!(
		!write.is_finished(),
		"append must not acknowledge before durability"
	);
	ops.release();
	let result = tokio::time::timeout_at(
		tokio::time::Instant::from_std(entered + Duration::from_secs(35)),
		&mut write,
	)
	.await
	.expect("transient35s")
	.expect("write task")
	.expect("transient committed");
	assert_eq!(result.rows, 16);
	assert_eq!(count(fixture.client.clone(), &d).await, 16);
	eprintln!("ACC8 transient gate={:?}", ops.events.lock().unwrap());
	fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acc8_persistent_postappend_hold_has_frozen_outcome() {
	let ops = Arc::new(GateFileOps::default());
	let fixture = AcceptanceFixture::start(54282, ops.clone(), false).await;
	let d = fixture.prepare().await;
	ops.arm();
	let client = fixture.client.clone();
	let mut write = tokio::spawn(async move {
		client
			.table_write(LIVE_TENANT, &d, input(0, 16, false), options())
			.await
	});
	let entered = ops.wait_entered().await;
	let observed = tokio::time::timeout_at(
		tokio::time::Instant::from_std(entered + Duration::from_secs(35)),
		&mut write,
	)
	.await;
	let outcome = match observed {
		Ok(joined) => Some(joined.expect("write task")),
		Err(_) => None,
	};
	let pending = !write.is_finished();
	expectation::persistent(&outcome, pending);
	eprintln!(
		"ACC8 persistent elapsed_ms={} gate={:?}",
		entered.elapsed().as_millis(),
		ops.events.lock().unwrap()
	);
	ops.release();
	if pending {
		let late = tokio::time::timeout(Duration::from_secs(10), write)
			.await
			.expect("released pending work finishes")
			.expect("late task");
		eprintln!("ACC8 teardown late result={late:?}; not baseline observation");
	}
	fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acc8_rotation_compaction_retirement_under_count() {
	// ADR0542_PHASE_ONLY_BEGIN setup
	let phase_origin = std::time::Instant::now();
	let mut phase_file = std::env::var_os("ADR0542_PHASE_FILE").map(|path| {
		std::fs::OpenOptions::new()
			.write(true)
			.create_new(true)
			.open(path)
			.expect("phase side file")
	});
	let mut record_phase = |phase: &str, detail: String| {
		use std::io::Write;
		if let Some(file) = phase_file.as_mut() {
			writeln!(
				file,
				"{}\t{}\t{}",
				phase_origin.elapsed().as_nanos(),
				phase,
				detail
			)
			.expect("phase write");
			file.flush().expect("phase flush");
		}
	};
	record_phase("fixture_start", String::new());
	// ADR0542_PHASE_ONLY_END setup
	let ops = Arc::new(GateFileOps::default());
	let _unlink_release = UnlinkReleaseGuard(ops.clone());
	let fixture = AcceptanceFixture::start(54283, ops.clone(), true).await;
	let d = fixture.prepare().await;
	// ADR0542_PHASE_ONLY_BEGIN prepared
	record_phase("fixture_prepared", String::new());
	// ADR0542_PHASE_ONLY_END prepared
	let acked = Arc::new(AtomicUsize::new(0));
	let stop = Arc::new(AtomicBool::new(false));
	let client = fixture.client.clone();
	let target = d.clone();
	let observed = acked.clone();
	let quit = stop.clone();
	let reader = tokio::spawn(async move {
		let mut samples = Vec::new();
		let mut last = 0;
		while !quit.load(Ordering::SeqCst) {
			let low = observed.load(Ordering::SeqCst) as i64;
			let n = count(client.clone(), &target).await;
			let high = (observed.load(Ordering::SeqCst) + 32).min(256) as i64;
			assert!(
				n >= low && n <= high && n >= last,
				"COUNT inconsistent n={n} low={low} high={high} last={last}"
			);
			samples.push(n);
			last = n;
			tokio::time::sleep(Duration::from_millis(10)).await;
		}
		samples
	});
	let boundary = async {
		for batch_index in 0..8 {
			let receipt = fixture
				.client
				.table_write(
					LIVE_TENANT,
					&d,
					input(batch_index * 32, 32, true),
					options(),
				)
				.await
				.expect("boundary batch");
			assert_eq!(receipt.rows, 32);
			// ADR0542_PHASE_ONLY_BEGIN batch
			record_phase(
				"batch_commit",
				format!("batch={} rows={}", batch_index, receipt.rows),
			);
			// ADR0542_PHASE_ONLY_END batch
			acked.fetch_add(32, Ordering::SeqCst);
		}
		let sealed = fixture.node.slab.list_sealed_slabs().await;
		// ADR0542_PHASE_ONLY_BEGIN sealed
		record_phase("sealed_observation", format!("{sealed:?}"));
		// ADR0542_PHASE_ONLY_END sealed
		assert!(
			sealed.len() >= 5,
			"frozen rotation precondition >=5 sealed, got{}",
			sealed.len()
		);
		let before = fixture.node.slab.compaction_manifest_snapshot();
		// ADR0542_PHASE_ONLY_BEGIN epoch_before
		record_phase("manifest_before", format!("{:?}", before.manifest_epoch));
		// ADR0542_PHASE_ONLY_END epoch_before
		let ids: Vec<_> = before.runs().iter().map(|r| r.slab_id).collect();
		let first = *ids.first().expect("selected source slab");
		let first_path = fixture.data_dir.join(format!("slab_{:016x}.wnd", first.0));
		ops.arm_unlink(first_path.clone());
		record_phase(
			"unlink_barrier_armed",
			format!("source={first:?} path={}", first_path.display()),
		);
		fixture.node.handles.lsm_compaction_control.resume();
		record_phase("compaction_resume", String::new());
		loop {
			let observed = fixture.node.slab.compaction_manifest_snapshot();
			if observed.manifest_epoch != before.manifest_epoch {
				record_phase(
					"compaction_epoch_advance",
					format!("{:?}->{:?}", before.manifest_epoch, observed.manifest_epoch),
				);
				break;
			}
			tokio::time::sleep(Duration::from_millis(10)).await;
		}
		ops.wait_unlink().await;
		record_phase("barrier_hit", format!("{first:?}"));
		let snapshot = fixture.node.slab.compaction_manifest_snapshot();
		assert_ne!(
			snapshot.manifest_epoch, before.manifest_epoch,
			"manifest epoch advanced at unlink barrier"
		);
		record_phase(
			"manifest_epoch_change",
			format!("{:?}->{:?}", before.manifest_epoch, snapshot.manifest_epoch),
		);
		assert!(
			snapshot
				.delete_pending_runs()
				.any(|run| run.slab_id == first),
			"selected source DeletePending at barrier"
		);
		record_phase("DeletePending", format!("{first:?}"));
		ops.release_unlink();
		record_phase("barrier_release", format!("{first:?}"));
		while first_path.exists() {
			tokio::time::sleep(Duration::from_millis(10)).await;
		}
		record_phase("unlink", format!("{first:?}"));
		eprintln!(
			"ACC8 boundary sealed={sealed:?} before_epoch={:?} after_epoch={:?} DeletePending=[{first:?}] unlink=true",
			before.manifest_epoch, snapshot.manifest_epoch
		);
	};
	let result = tokio::time::timeout(Duration::from_secs(30), boundary).await;
	// ADR0542_PHASE_ONLY_BEGIN result
	record_phase(
		"boundary_result",
		format!("within_bound={}", result.is_ok()),
	);
	// ADR0542_PHASE_ONLY_END result
	ops.release_unlink();
	record_phase("teardown_release", String::new());
	stop.store(true, Ordering::SeqCst);
	// ADR0542_PHASE_ONLY_BEGIN stop
	record_phase("read_sampler_stop", String::new());
	// ADR0542_PHASE_ONLY_END stop
	let samples = reader.await.expect("COUNT reader joined");
	// ADR0542_PHASE_ONLY_BEGIN joined
	record_phase("read_sampler_joined", format!("samples={}", samples.len()));
	// ADR0542_PHASE_ONLY_END joined
	assert!(result.is_ok(), "frozen boundary30s miss");
	assert!(!samples.is_empty());
	assert_eq!(count(fixture.client.clone(), &d).await, 256);
	// ADR0542_PHASE_ONLY_BEGIN count
	record_phase("final_COUNT", "256".into());
	// ADR0542_PHASE_ONLY_END count
	eprintln!("ACC8 COUNT samples={samples:?}; final256");
	record_phase("teardown_start", String::new());
	fixture.close().await;
	record_phase("teardown_complete", String::new());
}
