use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

use toron::distributed::identity::WorkerId;
use toron::distributed::partition_store::InMemoryPartitionStore;
use toron::distributed::worker_registry::WorkerEndpoint;
use toron::error::StorageError;
use toron::ir::TableId;
use toron::quic::QuicEndpoint;
use toron::server::ToronWorker;
use toron::server::memory::MemoryManager;
use toron::server::transport::{QuicTransport, TcpTransport, ToronTransport};
use toron::storage::{ScanRequest, ScanStream, StorageScan};

/// ADR 626 Stage A: passive wait events, shared with the process tests.
#[path = "../../tests/support/wait_events.rs"]
mod wait_events;

#[derive(Clone, Copy, PartialEq, Eq)]
enum FixtureTransport {
	Tcp,
	/// Serves tasks and partition pulls on a loopback QUIC endpoint only — no TCP listener — and
	/// pulls its own shuffle inputs over QUIC.
	Quic,
}

/// Host mode: authenticated QUIC (mTLS) on a host address, for multinode runs. The worker's
/// listener certificate is both its server identity and its peer-client identity.
struct HostMode {
	bind: std::net::SocketAddr,
	advertise: std::net::SocketAddr,
	listener_cert: std::path::PathBuf,
	listener_key: std::path::PathBuf,
	peer_ca: std::path::PathBuf,
	cluster_domain: String,
}

/// One timestamped line on stderr: receiver-side evidence for a multinode attempt.
fn host_event(event: &str, detail: impl std::fmt::Display) {
	let unix_ms = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.map(|elapsed| elapsed.as_millis())
		.unwrap_or(0);
	eprintln!("{event} unix_ms={unix_ms} {detail}");
}

/// Test-only evidence: appends one `<this worker> <producer worker> <producer endpoint>` line per
/// sealed-pull dial, then dials through the real transport unchanged.
struct DialRecordingTransport {
	inner: Arc<dyn ToronTransport>,
	this_worker: WorkerId,
	path: std::path::PathBuf,
}

#[async_trait::async_trait]
impl ToronTransport for DialRecordingTransport {
	async fn connect(
		&self,
		address: &str,
	) -> Result<Box<dyn toron::server::transport::ToronStream>, toron::error::QueryError> {
		self.inner.connect(address).await
	}

	async fn connect_peer(
		&self,
		worker_id: &WorkerId,
		endpoint: &WorkerEndpoint,
	) -> Result<Box<dyn toron::server::transport::ToronStream>, toron::error::QueryError> {
		use std::io::Write as _;
		let line = format!(
			"{} {} {}\n",
			self.this_worker.get().as_u128(),
			worker_id.get().as_u128(),
			endpoint.as_str()
		);
		std::fs::OpenOptions::new()
			.create(true)
			.append(true)
			.open(&self.path)
			.and_then(|mut file| file.write_all(line.as_bytes()))
			.map_err(|error| toron::error::QueryError::Internal {
				message: format!("dial record failed: {error}"),
			})?;
		self.inner.connect_peer(worker_id, endpoint).await
	}
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
	let (
		worker_id,
		transport,
		record_peer_dials,
		host_mode,
		record_wait_events,
		recovery,
		loopback,
		managed_lifecycle,
	) = args()?;
	let wait_log = record_wait_events
		.as_deref()
		.map(wait_events::EventLog::create)
		.transpose()?;
	// ADR 628 B1.C: the long scan is echoed, so the evidence names the configured duration.
	let long_scan_ms = u64::try_from(long_scan().as_millis()).unwrap_or(u64::MAX);
	host_event("long_scan", format!("ms={long_scan_ms}"));
	if let Some(log) = &wait_log {
		log.emit(
			"fixture_config",
			"worker",
			serde_json::json!({ "long_scan_ms": long_scan_ms }),
		);
	}
	let peer = |inner: Arc<dyn ToronTransport>| -> Arc<dyn ToronTransport> {
		let recorded: Arc<dyn ToronTransport> = match &record_peer_dials {
			Some(path) => Arc::new(DialRecordingTransport {
				inner,
				this_worker: worker_id,
				path: path.clone(),
			}),
			None => inner,
		};
		match &wait_log {
			Some(log) => Arc::new(wait_events::ObservedTransport {
				inner: recorded,
				log: log.clone(),
				role: "pull",
			}),
			None => recorded,
		}
	};
	// ADR 628 Stage A: `--managed-lifecycle` gives this fixture the production worker's store
	// lifecycle: a managed in-memory store with the production lease term and an owned one-second
	// reaper, as the daemon runs (daemon.rs). Unset, the store is the unmanaged fixture store, as before.
	let managed_store = if managed_lifecycle {
		Some(Arc::new(InMemoryPartitionStore::managed(
			toron::distributed::budget::QueryBudget::test_default(),
			toron::distributed::partition_store::DEFAULT_WORKER_LOST_AFTER,
		)?))
	} else {
		None
	};
	// ADR 628: every manifest this worker published, so each reaper pass can report the exact
	// manifests' residency (a pure `manifest` lookup) beside the aggregate counters.
	let published: Arc<std::sync::Mutex<Vec<toron::distributed::partition_store::ManifestKey>>> =
		Arc::default();
	let partition_store: Arc<dyn toron::distributed::partition_store::PartitionStore> =
		match &managed_store {
			// ADR 628: the managed store is wrapped so every actual lease application is recorded.
			Some(store) => Arc::new(LeaseApplicationStore {
				inner: store.clone(),
				log: wait_log.clone(),
				published: published.clone(),
				calls: std::sync::atomic::AtomicU64::new(0),
			}),
			None => Arc::new(InMemoryPartitionStore::new()),
		};
	if let Some(log) = &wait_log {
		log.emit(
			"lifecycle_mode",
			"served",
			serde_json::json!({
				"managed": managed_lifecycle,
				"worker_lost_after_ms": toron::distributed::partition_store::DEFAULT_WORKER_LOST_AFTER.as_millis() as u64,
				"reaper_interval_ms": if managed_lifecycle { 1000 } else { 0 },
			}),
		);
	}
	// ADR 628: this process's identity once its endpoint is known, so a reader can validate the
	// declared roster (worker id, endpoint, mode and lease term) against each log.
	let lease_term_ms =
		toron::distributed::partition_store::PartitionStore::query_lease_term_millis(
			partition_store.as_ref(),
		);
	let identify = |endpoint: &WorkerEndpoint| {
		if let Some(log) = &wait_log {
			log.emit(
				"worker_identity",
				"served",
				serde_json::json!({
					"worker": worker_id.get().as_u128().to_string(),
					"endpoint": endpoint.as_str(),
					"pid": std::process::id(),
					"managed": managed_lifecycle,
					"lease_term_ms": lease_term_ms,
				}),
			);
		}
	};
	// Every reaper invocation is recorded at start and completion, on this worker's clock, with the
	// lifecycle snapshot before and after. A pass that changes nothing is still recorded: silence is
	// never evidence of a pass or of cleanup. The task is owned by `main` and aborted when `main`'s
	// guard drops or the process exits; it is never awaited.
	let _reaper = managed_store.clone().map(|store| {
		let log = wait_log.clone();
		ReaperTask(tokio::spawn(async move {
			use toron::distributed::partition_store::PartitionStore as _;
			let snapshot = |store: &InMemoryPartitionStore| {
				let s = store.lifecycle_snapshot();
				let keys = published.lock().expect("published manifests").clone();
				let manifests = keys
					.into_iter()
					.map(|key| {
						let mut entry = manifest_key_json(key);
						entry["resident"] = store.manifest(key).is_some().into();
						entry
					})
					.collect::<Vec<_>>();
				serde_json::json!({
					"published_manifests": manifests,
					"active_leases": s.active_leases,
					"retiring_namespaces": s.retiring_namespaces,
					"receipts": s.receipts,
					"resident_namespaces": s.resident_namespaces,
					"abandoned_reclaims": s.abandoned_reclaims,
					"live_partition_records": s.live_partition_records,
				})
			};
			let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
			let mut pass = 0u64;
			loop {
				tick.tick().await;
				pass += 1;
				let before = snapshot(&store);
				if let Some(log) = &log {
					log.emit(
						"reaper_pass_start",
						"served",
						serde_json::json!({ "pass": pass, "before": before }),
					);
				}
				store.reap_expired_queries();
				let after = snapshot(&store);
				if let Some(log) = &log {
					log.emit(
						"reaper_pass_end",
						"served",
						serde_json::json!({ "pass": pass, "after": after }),
					);
				}
			}
		}))
	});
	let worker = |endpoint: WorkerEndpoint, peer: Arc<dyn ToronTransport>| {
		Ok::<_, Box<dyn Error>>(Arc::new(
			ToronWorker::with_partition_store_and_endpoint(
				Arc::new(FixtureStorageScan::new(wait_log.clone())?),
				Arc::new(MemoryManager::new(32, 1024 * 1024)),
				partition_store.clone(),
				worker_id,
				endpoint,
			)
			.with_peer_transport(peer)
			.with_recovery_experiment_if(recovery),
		))
	};
	if let Some(host) = host_mode {
		let policy = match toron::network::tls::ListenerPolicy::load_from_paths(
			&host.listener_cert,
			&host.listener_key,
			&host.peer_ca,
			&host.cluster_domain,
			&worker_id,
		) {
			Ok(policy) => policy,
			Err(error) => {
				host_event("STARTUP_REJECTED", format!("error={error}"));
				return Err(error.into());
			}
		};
		let quic = QuicEndpoint::bind_authenticated(
			host.bind,
			policy.server_config_for(toron::network::tls::ListenerRole::WorkerTask),
		)?;
		let endpoint = WorkerEndpoint::new(host.advertise.to_string());
		let worker = worker(
			endpoint.clone(),
			peer(Arc::new(QuicTransport::authenticated(
				policy.client_config(),
				policy.cluster_domain(),
			)?)),
		)?;
		host_event(
			"LISTENING",
			format!("bind={} advertise={}", host.bind, host.advertise),
		);
		identify(&endpoint);
		announce(&endpoint).await?;
		loop {
			let connection = match quic.accept_connection().await {
				Ok(connection) => connection,
				Err(error) => {
					host_event("HANDSHAKE_REJECTED", format!("error={error}"));
					continue;
				}
			};
			let remote = connection.remote_address();
			let worker = worker.clone();
			let wait_log = wait_log.clone();
			tokio::spawn(async move {
				match QuicEndpoint::accept_stream_on(&connection).await {
					Ok(stream) => {
						host_event("STREAM_ACCEPTED", format!("remote={remote}"));
						let served = match wait_log {
							Some(log) => {
								{
									let observed = wait_events::ObservedStream::served(
										stream,
										log,
										serde_json::json!({ "remote": remote.to_string() }),
									);
									// Pull dials made while handling this stream name it, when the work stays on this task.
									wait_events::SERVED_STREAM
										.scope(
											observed.stream_id(),
											worker.handle_connection(observed),
										)
										.await
								}
							}
							None => worker.handle_connection(stream).await,
						};
						if let Err(error) = served {
							host_event(
								"CONNECTION_FAILED",
								format!("remote={remote} error={error}"),
							);
						}
					}
					Err(error) => {
						host_event("STREAM_FAILED", format!("remote={remote} error={error}"))
					}
				}
			});
		}
	}
	match transport {
		FixtureTransport::Tcp => {
			let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
			let endpoint = WorkerEndpoint::new(listener.local_addr()?.to_string());
			let worker = worker(endpoint.clone(), peer(Arc::new(TcpTransport::new())))?;
			identify(&endpoint);
			announce(&endpoint).await?;
			loop {
				let (stream, _) = listener.accept().await?;
				let worker = worker.clone();
				tokio::spawn(async move {
					if let Err(error) = worker.handle_connection(stream).await {
						eprintln!("fixture worker connection failed: {error}");
					}
				});
			}
		}
		FixtureTransport::Quic => {
			// ADR 626 Stage C: a relay control binds a known loopback port and advertises the
			// relay in front of it, so consumers and the coordinator both reach it through the
			// relay. Unset, the worker binds an ephemeral port and advertises it, as before.
			let bind = loopback.bind.unwrap_or("127.0.0.1:0".parse()?);
			let quic = QuicEndpoint::bind_loopback(bind)?;
			let endpoint =
				WorkerEndpoint::new(loopback.advertise.unwrap_or(quic.local_addr()?).to_string());
			let worker = worker(endpoint.clone(), peer(Arc::new(QuicTransport::new()?)))?;
			identify(&endpoint);
			announce(&endpoint).await?;
			loop {
				let connection = match quic.accept_connection().await {
					Ok(connection) => connection,
					Err(error) => {
						eprintln!("fixture worker QUIC handshake failed: {error}");
						continue;
					}
				};
				let remote = connection.remote_address();
				let worker = worker.clone();
				let wait_log = wait_log.clone();
				tokio::spawn(async move {
					match QuicEndpoint::accept_stream_on(&connection).await {
						Ok(stream) => {
							let served = match wait_log {
								Some(log) => {
									{
										let observed = wait_events::ObservedStream::served(
											stream,
											log,
											serde_json::json!({ "remote": remote.to_string() }),
										);
										// Pull dials made while handling this stream name it, when the work stays on this task.
										wait_events::SERVED_STREAM
											.scope(
												observed.stream_id(),
												worker.handle_connection(observed),
											)
											.await
									}
								}
								None => worker.handle_connection(stream).await,
							};
							if let Err(error) = served {
								eprintln!("fixture worker connection failed: {error}");
							}
						}
						Err(error) => eprintln!("fixture worker QUIC stream failed: {error}"),
					}
				});
			}
		}
	}
}

/// ADR 628 Stage A: forwards every `PartitionStore` call to the managed store unchanged, and records
/// each actual `issue_or_renew_query_lease` application on this worker's clock: `lease_apply_start`
/// and `lease_apply_end` with the query and the store's active-lease count before and after. The end
/// marker is an upper bound on when that lease was set (the lease, if applied, expires no earlier
/// than start + term and no later than end + term). Whether the call applied or was ignored
/// (retiring, receipt held) is not exposed per query; the counts are reported, not interpreted.
struct LeaseApplicationStore {
	inner: Arc<InMemoryPartitionStore>,
	/// Numbers every application call, so a reader pairs each start with its own end even if calls
	/// overlap.
	calls: std::sync::atomic::AtomicU64,
	log: Option<Arc<wait_events::EventLog>>,
	/// Every successfully published manifest, recorded by key (`publish_applied`) for the reaper's
	/// per-manifest residency report.
	published: Arc<std::sync::Mutex<Vec<toron::distributed::partition_store::ManifestKey>>>,
}

fn manifest_key_json(key: toron::distributed::partition_store::ManifestKey) -> serde_json::Value {
	serde_json::json!({
		"query": key.query_id.get().to_string(),
		"exchange": key.exchange_id.get(),
		"stage": key.producer_stage_id.get(),
		"task": key.producer_task_id.get(),
		"attempt": key.attempt_id.get(),
	})
}

#[async_trait::async_trait]
impl toron::distributed::partition_store::PartitionStore for LeaseApplicationStore {
	fn query_lease_term_millis(&self) -> u64 {
		self.inner.query_lease_term_millis()
	}

	fn issue_or_renew_query_lease(
		&self,
		query_id: toron::distributed::identity::QueryId,
	) -> toron::distributed::namespace_lifecycle::LeaseRenewal {
		let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
		let before = self.inner.lifecycle_snapshot().active_leases;
		if let Some(log) = &self.log {
			log.emit(
				"lease_apply_start",
				"served",
				serde_json::json!({ "query": query_id.get().to_string(), "call": call, "active_leases_before": before }),
			);
		}
		let renewal = self.inner.issue_or_renew_query_lease(query_id);
		let after = self.inner.lifecycle_snapshot().active_leases;
		if let Some(log) = &self.log {
			// ADR 628 B0: the call's own outcome, as the lifecycle decided it.
			let outcome = match renewal {
				toron::distributed::namespace_lifecycle::LeaseRenewal::Applied => {
					"Applied".to_string()
				}
				toron::distributed::namespace_lifecycle::LeaseRenewal::Ignored(reason) => {
					format!("Ignored({reason:?})")
				}
			};
			log.emit(
				"lease_apply_end",
				"served",
				serde_json::json!({ "query": query_id.get().to_string(), "call": call, "active_leases_after": after, "outcome": outcome }),
			);
		}
		renewal
	}

	fn retire_query(
		&self,
		query_id: toron::distributed::identity::QueryId,
		cause: toron::distributed::namespace_lifecycle::QueryTerminalCause,
	) {
		self.inner.retire_query(query_id, cause);
	}

	fn reap_expired_queries(&self) {
		self.inner.reap_expired_queries();
	}

	fn lifecycle_snapshot(
		&self,
	) -> toron::distributed::namespace_lifecycle::ShuffleLifecycleSnapshot {
		self.inner.lifecycle_snapshot()
	}

	fn publish(
		&self,
		manifest: toron::distributed::task::PartitionManifest,
		payloads: Vec<toron::distributed::partition_store::PartitionPayload>,
	) -> Result<
		toron::distributed::task::PartitionManifest,
		toron::distributed::partition_store::PartitionStoreError,
	> {
		let result = self.inner.publish(manifest, payloads);
		if let Ok(published) = &result {
			let key = toron::distributed::partition_store::ManifestKey::from_manifest(published);
			self.published
				.lock()
				.expect("published manifests")
				.push(key);
			if let Some(log) = &self.log {
				log.emit("publish_applied", "served", manifest_key_json(key));
			}
		}
		result
	}

	fn manifest(
		&self,
		key: toron::distributed::partition_store::ManifestKey,
	) -> Option<toron::distributed::task::PartitionManifest> {
		self.inner.manifest(key)
	}

	fn pull(
		&self,
		address: toron::distributed::task::PartitionAddress,
		expected_attempt: toron::distributed::identity::AttemptId,
	) -> Result<
		toron::distributed::partition_store::PartitionPayload,
		toron::error::PartitionMissingReason,
	> {
		self.inner.pull(address, expected_attempt)
	}

	fn open_live(
		&self,
		address: toron::distributed::task::PartitionAddress,
		schema: arrow::datatypes::SchemaRef,
	) -> Result<(), toron::distributed::partition_store::PartitionStoreError> {
		self.inner.open_live(address, schema)
	}

	fn open_live_with_downstream_cancellation(
		&self,
		address: toron::distributed::task::PartitionAddress,
		schema: arrow::datatypes::SchemaRef,
		downstream_cancellation: Option<
			toron::distributed::cancellation::DownstreamCancellationContext,
		>,
	) -> Result<(), toron::distributed::partition_store::PartitionStoreError> {
		self.inner
			.open_live_with_downstream_cancellation(address, schema, downstream_cancellation)
	}

	async fn append_live(
		&self,
		payload: toron::distributed::partition_store::PartitionPayload,
	) -> Result<(), toron::distributed::partition_store::PartitionStoreError> {
		self.inner.append_live(payload).await
	}

	fn seal_live(
		&self,
		manifest: toron::distributed::task::PartitionManifest,
	) -> Result<
		toron::distributed::task::PartitionManifest,
		toron::distributed::partition_store::PartitionStoreError,
	> {
		self.inner.seal_live(manifest)
	}

	fn cancel_live(
		&self,
		address: toron::distributed::task::PartitionAddress,
		reason: toron::error::PartitionMissingReason,
	) {
		self.inner.cancel_live(address, reason);
	}

	fn pull_live(
		&self,
		address: toron::distributed::task::PartitionAddress,
		expected_attempt: toron::distributed::identity::AttemptId,
	) -> Result<
		toron::distributed::partition_store::LivePartitionStream,
		toron::error::PartitionMissingReason,
	> {
		self.inner.pull_live(address, expected_attempt)
	}

	fn remove_query(&self, query_id: toron::distributed::identity::QueryId) {
		self.inner.remove_query(query_id);
	}

	fn cancel_query(&self, query_id: toron::distributed::identity::QueryId) {
		self.inner.cancel_query(query_id);
	}
}

/// ADR 628: the reaper task is owned by `main`; dropping this aborts it.
struct ReaperTask(tokio::task::JoinHandle<()>);

impl Drop for ReaperTask {
	fn drop(&mut self) {
		self.0.abort();
	}
}

async fn announce(endpoint: &WorkerEndpoint) -> Result<(), Box<dyn Error>> {
	let mut stdout = tokio::io::stdout();
	stdout
		.write_all(format!("READY {}\n", endpoint.as_str()).as_bytes())
		.await?;
	stdout.flush().await?;
	Ok(())
}

struct FixtureStorageScan {
	tables: HashMap<TableId, RecordBatch>,
	/// ADR 626 Stage A sized payloads (tables 6, 7, 8), built on first scan only, so a run that never
	/// reads them pays nothing for them.
	payloads: [std::sync::OnceLock<RecordBatch>; 3],
	/// ADR 627: where a long scan records its entry and return, tagged with the served stream.
	wait_log: Option<Arc<wait_events::EventLog>>,
}

impl FixtureStorageScan {
	fn table(&self, table_id: TableId) -> Result<RecordBatch, StorageError> {
		if table_id.0 == SLOW_TABLE || table_id.0 == LONG_TABLE {
			return Ok(slow_batch());
		}
		let rows = match table_id.0 {
			6 => Some((0, 16)),
			7 => Some((1, 16 * 1024)),
			8 => Some((2, 192 * 1024)),
			_ => None,
		};
		match rows {
			Some((slot, rows)) => Ok(self.payloads[slot]
				.get_or_init(|| payload(rows).expect("sized payload batch"))
				.clone()),
			None => self
				.tables
				.get(&table_id)
				.cloned()
				.ok_or(StorageError::TableNotFound { table: table_id }),
		}
	}
}

impl FixtureStorageScan {
	fn new(wait_log: Option<Arc<wait_events::EventLog>>) -> Result<Self, arrow::error::ArrowError> {
		let vals_schema = Arc::new(Schema::new(vec![
			Field::new("val", DataType::Int32, false),
			Field::new("float_val", DataType::Float64, true),
			Field::new("category", DataType::Utf8, true),
			Field::new("sample", DataType::Float64, true),
		]));
		let orders_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("customer_id", DataType::Int64, false),
		]));
		let customers_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region", DataType::Utf8, true),
		]));
		let set_left_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("set_key", DataType::Int64, true),
		]));
		let set_right_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("set_key", DataType::Int64, true),
		]));
		let vals = RecordBatch::try_new(
			vals_schema,
			vec![
				Arc::new(Int32Array::from((0..16).collect::<Vec<i32>>())),
				Arc::new(Float64Array::from(
					(0..16)
						.map(|row| Some(row as f64 + 0.25))
						.collect::<Vec<_>>(),
				)),
				Arc::new(StringArray::from(
					(0..16)
						.map(|row| Some(if row < 9 { "fence" } else { "zero" }))
						.collect::<Vec<_>>(),
				)),
				Arc::new(Float64Array::from(vec![
					Some(-6.0),
					Some(-1.0),
					Some(0.0),
					Some(1.0),
					Some(2.0),
					Some(3.0),
					Some(4.0),
					Some(5.0),
					Some(10.0),
					Some(0.0),
					Some(5.0),
					Some(5.0),
					Some(5.0),
					Some(5.0),
					Some(5.0),
					Some(10.0),
				])),
			],
		)?;
		let orders = RecordBatch::try_new(
			orders_schema,
			vec![
				Arc::new(Int64Array::from(vec![100, 101, 102, 103])),
				Arc::new(Int64Array::from(vec![1, 2, 2, 3])),
			],
		)?;
		// ADR 233 cardinal: both matched customers sit in "west", but their ids hash to different
		// join partitions (cust 2 -> p0, cust 3 -> p3 under XxHash3_64 % 4), so "west"'s rows are
		// produced by two join workers; its SUM/COUNT must merge partials across producers at the shuffle.
		let customers = RecordBatch::try_new(
			customers_schema,
			vec![
				Arc::new(Int64Array::from(vec![2, 3])),
				Arc::new(StringArray::from(vec![Some("west"), Some("west")])),
			],
		)?;
		let set_left = RecordBatch::try_new(
			set_left_schema,
			vec![
				Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
				Arc::new(Int64Array::from(vec![Some(7), None, Some(9), None])),
			],
		)?;
		let set_right = RecordBatch::try_new(
			set_right_schema,
			vec![
				Arc::new(Int64Array::from(vec![10, 11, 12])),
				Arc::new(Int64Array::from(vec![None, Some(9), Some(10)])),
			],
		)?;

		Ok(Self {
			tables: HashMap::from([
				(TableId(1), vals),
				(TableId(2), orders),
				(TableId(3), customers),
				(TableId(4), set_left),
				(TableId(5), set_right),
			]),
			payloads: std::array::from_fn(|_| std::sync::OnceLock::new()),
			wait_log,
		})
	}
}

/// ADR 626 Stage C: the slow table and how long its scan stays silent (above the 5 s `H`).
const SLOW_TABLE: u64 = 9;
const SLOW_SCAN: std::time::Duration = std::time::Duration::from_secs(8);
/// ADR 627: a scan long enough to span several 5 s lease-renewal ticks on the coordinator path.
const LONG_TABLE: u64 = 10;
const LONG_SCAN: std::time::Duration = std::time::Duration::from_secs(30);
/// ADR 628 B1.C: `--long-scan-ms` sets the long table's scan (bounded; default `LONG_SCAN`), so a
/// declared control can span several initial reliance horizons.
static LONG_SCAN_OVERRIDE: std::sync::OnceLock<std::time::Duration> = std::sync::OnceLock::new();
const LONG_SCAN_MS_RANGE: std::ops::RangeInclusive<u64> = 1_000..=600_000;

fn long_scan() -> std::time::Duration {
	LONG_SCAN_OVERRIDE.get().copied().unwrap_or(LONG_SCAN)
}

fn slow_batch() -> RecordBatch {
	RecordBatch::try_new(
		Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
		vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
	)
	.expect("slow batch")
}

/// ADR 626 Stage A sized fixtures: `rows` rows of a unique `id` (never 2 or 3, so a join with
/// `customers` matches nothing) and a 64-byte `pad`, so a shuffle on `id` carries the whole table
/// while the final result stays empty. Partition sizes are measured, not assumed.
fn payload(rows: i64) -> Result<RecordBatch, arrow::error::ArrowError> {
	let schema = Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("pad", DataType::Utf8, false),
	]));
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(
				(0..rows).map(|row| 1_000 + row).collect::<Vec<_>>(),
			)),
			Arc::new(StringArray::from(
				(0..rows)
					.map(|row| format!("{row:064}"))
					.collect::<Vec<_>>(),
			)),
		],
	)
}

#[async_trait::async_trait]
impl StorageScan for FixtureStorageScan {
	async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
		let silence = match request.table_id.0 {
			// ADR 626 Stage C: a scan that stays silent longer than the experiment's `H`, so the
			// long-silence control shows `H` bounds establishment only.
			SLOW_TABLE => Some(SLOW_SCAN),
			LONG_TABLE => Some(long_scan()),
			_ => None,
		};
		if let Some(silence) = silence {
			// ADR 627: the task's execution entry, on the worker's own clock. The served stream ties
			// it to that stream's `StartTask`; without the task-local, the binding is unavailable.
			let stream = wait_events::SERVED_STREAM.try_with(|stream| *stream).ok();
			let fields = serde_json::json!({ "table": request.table_id.0, "stream": stream });
			if let Some(log) = &self.wait_log {
				log.emit("scan_entered", "served", fields.clone());
			}
			tokio::time::sleep(silence).await;
			if let Some(log) = &self.wait_log {
				log.emit("scan_returned", "served", fields);
			}
		}
		let mut batch = self.table(request.table_id)?;
		// ADR 238: honor a pushed predicate in the source-column domain (before projection),
		// reusing the production PBC evaluator — the same `wendelon::schema::pbc::execute` the real
		// in-memory scan uses (storage.rs). Fail loud on an unevaluable program; never return
		// silently-unfiltered rows.
		if let Some(pbc) = &request.pbc_predicate {
			batch = wendelon::schema::pbc::execute(pbc, &batch).map_err(|error| {
				StorageError::InvalidPbcProgram {
					message: format!("{error:?}"),
				}
			})?;
		}
		if let Some(columns) = request.projected_columns {
			let indices = columns
				.into_iter()
				.map(|column| column.0 as usize)
				.collect::<Vec<_>>();
			batch = batch.project(&indices)?;
		}
		if let Some((morsel_id, total_morsels)) = request.morsel {
			let total_rows = batch.num_rows();
			let rows_per_morsel =
				(total_rows + total_morsels as usize - 1) / total_morsels as usize;
			let start = morsel_id.0 as usize * rows_per_morsel;
			let end = std::cmp::min(start + rows_per_morsel, total_rows);
			batch = if start < total_rows {
				batch.slice(start, end - start)
			} else {
				batch.slice(0, 0)
			};
		}
		Ok(ScanStream::from_batches(
			batch.schema(),
			request.session_id,
			request.query_id,
			toron::storage::ScanOrigin::BaseRelation,
			vec![batch],
		))
	}
}

type FixtureArgs = (
	WorkerId,
	FixtureTransport,
	Option<std::path::PathBuf>,
	Option<HostMode>,
	Option<std::path::PathBuf>,
	Option<toron::distributed::recovery::RecoveryExperiment>,
	LoopbackAddresses,
	bool,
);

/// ADR 626 Stage C: test-only loopback bind and advertised addresses (QUIC loopback mode only).
#[derive(Default)]
struct LoopbackAddresses {
	bind: Option<std::net::SocketAddr>,
	advertise: Option<std::net::SocketAddr>,
}

/// ADR 626: the recovery experiment, only when asked for.
trait WithRecoveryIf {
	fn with_recovery_experiment_if(
		self,
		experiment: Option<toron::distributed::recovery::RecoveryExperiment>,
	) -> Self;
}

impl WithRecoveryIf for ToronWorker {
	fn with_recovery_experiment_if(
		self,
		experiment: Option<toron::distributed::recovery::RecoveryExperiment>,
	) -> Self {
		match experiment {
			Some(experiment) => self.with_recovery_experiment(experiment),
			None => self,
		}
	}
}

fn invalid(message: String) -> Box<dyn Error> {
	Box::new(std::io::Error::new(
		std::io::ErrorKind::InvalidInput,
		message,
	))
}

fn args() -> Result<FixtureArgs, Box<dyn Error>> {
	let mut args = std::env::args().skip(1);
	let mut worker_u128 = None;
	let mut transport = FixtureTransport::Tcp;
	let mut record_peer_dials = None;
	let mut record_wait_events = None;
	let mut recovery_establishment_ms: Option<u64> = None;
	let mut loopback = LoopbackAddresses::default();
	let mut recovery_confirmation_ms: Option<u64> = None;
	let mut managed_lifecycle = false;
	let mut bind: Option<std::net::SocketAddr> = None;
	let mut advertise: Option<std::net::SocketAddr> = None;
	let mut tls_cert = None;
	let mut tls_key = None;
	let mut tls_ca = None;
	let mut cluster_domain = None;
	while let Some(arg) = args.next() {
		if arg == "--worker-u128" {
			worker_u128 = args.next().map(|value| value.parse::<u128>()).transpose()?;
		} else if arg == "--bind" {
			bind = args.next().map(|value| value.parse()).transpose()?;
		} else if arg == "--advertise" {
			advertise = args.next().map(|value| value.parse()).transpose()?;
		} else if arg == "--tls-cert" {
			tls_cert = args.next().map(std::path::PathBuf::from);
		} else if arg == "--tls-key" {
			tls_key = args.next().map(std::path::PathBuf::from);
		} else if arg == "--tls-ca" {
			tls_ca = args.next().map(std::path::PathBuf::from);
		} else if arg == "--cluster-domain" {
			cluster_domain = args.next();
		} else if arg == "--record-peer-dials" {
			record_peer_dials = args.next().map(std::path::PathBuf::from);
		} else if arg == "--record-wait-events" {
			record_wait_events = args.next().map(std::path::PathBuf::from);
		} else if arg == "--loopback-bind" {
			loopback.bind = args.next().map(|value| value.parse()).transpose()?;
		} else if arg == "--loopback-advertise" {
			loopback.advertise = args.next().map(|value| value.parse()).transpose()?;
		} else if arg == "--recovery-establishment-ms" {
			recovery_establishment_ms = args.next().map(|value| value.parse()).transpose()?;
		} else if arg == "--recovery-confirmation-ms" {
			recovery_confirmation_ms = args.next().map(|value| value.parse()).transpose()?;
		} else if arg == "--managed-lifecycle" {
			managed_lifecycle = true;
		} else if arg == "--long-scan-ms" {
			let ms: u64 = args
				.next()
				.ok_or_else(|| invalid("--long-scan-ms needs a value".to_string()))?
				.parse()?;
			if !LONG_SCAN_MS_RANGE.contains(&ms) {
				return Err(invalid(format!(
					"--long-scan-ms must be within {LONG_SCAN_MS_RANGE:?}, got {ms}"
				)));
			}
			LONG_SCAN_OVERRIDE
				.set(std::time::Duration::from_millis(ms))
				.map_err(|_| invalid("--long-scan-ms given twice".to_string()))?;
		} else if arg == "--transport" {
			transport = match args.next().as_deref() {
				Some("tcp") => FixtureTransport::Tcp,
				Some("quic") => FixtureTransport::Quic,
				other => {
					return Err(Box::new(std::io::Error::new(
						std::io::ErrorKind::InvalidInput,
						format!("--transport must be tcp or quic, got {other:?}"),
					)));
				}
			};
		}
	}
	let worker_u128 = worker_u128.ok_or_else(|| {
		std::io::Error::new(
			std::io::ErrorKind::InvalidInput,
			"missing required --worker-u128 argument",
		)
	})?;
	let host_mode = host_mode(bind, advertise, tls_cert, tls_key, tls_ca, cluster_domain)?;
	// ADR 626: both values or neither; invalid values refuse to start.
	let recovery = match (recovery_establishment_ms, recovery_confirmation_ms) {
		(None, None) => None,
		(Some(establishment), Some(confirmation)) => Some(
			toron::distributed::recovery::RecoveryExperiment::from_millis(
				establishment,
				confirmation,
			)
			.map_err(|error| invalid(format!("recovery experiment: {error}")))?,
		),
		_ => {
			return Err(invalid(
				"--recovery-establishment-ms and --recovery-confirmation-ms go together"
					.to_string(),
			));
		}
	};
	for address in [loopback.bind, loopback.advertise].into_iter().flatten() {
		if !address.ip().is_loopback() || transport != FixtureTransport::Quic || host_mode.is_some()
		{
			return Err(invalid(format!(
				"--loopback-bind/--loopback-advertise take loopback addresses in QUIC loopback mode, got {address}"
			)));
		}
	}
	Ok((
		WorkerId::from_u128(worker_u128),
		transport,
		record_peer_dials,
		host_mode,
		record_wait_events,
		recovery,
		loopback,
		managed_lifecycle,
	))
}

/// Host mode needs every host flag or none. It refuses a loopback or unspecified advertised
/// address: a loopback endpoint would take the unauthenticated loopback QUIC path silently.
fn host_mode(
	bind: Option<std::net::SocketAddr>,
	advertise: Option<std::net::SocketAddr>,
	tls_cert: Option<std::path::PathBuf>,
	tls_key: Option<std::path::PathBuf>,
	tls_ca: Option<std::path::PathBuf>,
	cluster_domain: Option<String>,
) -> Result<Option<HostMode>, Box<dyn Error>> {
	match (bind, advertise, tls_cert, tls_key, tls_ca, cluster_domain) {
		(None, None, None, None, None, None) => Ok(None),
		(Some(bind), Some(advertise), Some(listener_cert), Some(listener_key), Some(peer_ca), Some(cluster_domain)) => {
			if advertise.ip().is_loopback() || advertise.ip().is_unspecified() || advertise.port() == 0 {
				return Err(invalid(format!(
					"--advertise must be a routable host address and port, got {advertise}"
				)));
			}
			Ok(Some(HostMode {
				bind,
				advertise,
				listener_cert,
				listener_key,
				peer_ca,
				cluster_domain,
			}))
		}
		_ => Err(invalid(
			"host mode needs --bind, --advertise, --tls-cert, --tls-key, --tls-ca and --cluster-domain together"
				.to_string(),
		)),
	}
}
