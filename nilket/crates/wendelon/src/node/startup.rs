//! One incremental owner from directory acquisition through full node handoff.
use super::*;
use crate::storage::ownership::{
	StartupCleanupSlot, StartupCleanupStatus, StartupDrainBarrier, StorageWorker,
};
use std::panic::{AssertUnwindSafe, catch_unwind};

const STORAGE_INDEX_RESTORE_INTENT: &str = "STORAGE_INDEX_RESTORE_AUTHORITY";

fn read_restore_intent(path: &std::path::Path) -> std::io::Result<Vec<u8>> {
	let file = std::fs::File::open(path)?;
	let mut limited = std::io::Read::take(file, 257);
	let mut bytes = Vec::new();
	std::io::Read::read_to_end(&mut limited, &mut bytes)?;
	if bytes.len() > 256 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"oversized index restore intent",
		));
	}
	Ok(bytes)
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct StorageIndexRestoreIntent {
	node_id: u32,
	artifact_hash: [u8; 32],
	last_applied: Option<openraft::LogId<u32>>,
	active_slab_id: crate::schema::SlabId,
	local_slabs_hash: [u8; 32],
}

// The inventory binds absence as well as presence: every expected local file,
// its exact bytes, and the active slab identity are required before rebuilding.
fn local_slabs_hash(
	active_slab_id: crate::schema::SlabId,
	slabs: &[(crate::schema::SlabId, u64, [u8; 32])],
) -> [u8; 32] {
	let mut hash = crate::hash::blake3::Context::new();
	hash.update(b"wendelon-local-storage-restore-v1\0");
	hash.update(&active_slab_id.get().to_le_bytes());
	hash.update(&(slabs.len() as u64).to_le_bytes());
	for (id, len, digest) in slabs {
		hash.update(&id.get().to_le_bytes());
		hash.update(&len.to_le_bytes());
		hash.update(digest);
	}
	hash.finish()
}

fn restored_local_slabs_hash(
	owner: &DataDirectoryOwner,
	active_slab_id: crate::schema::SlabId,
) -> std::io::Result<[u8; 32]> {
	let _permit = owner.write_permit()?;
	let mut slabs = Vec::new();
	for entry in std::fs::read_dir(owner.path())? {
		let entry = entry?;
		let name = entry.file_name();
		let Some(name) = name.to_str() else { continue };
		let Some(id) = name
			.strip_prefix("slab_")
			.and_then(|s| s.strip_suffix(".wnd"))
		else {
			continue;
		};
		let id = u64::from_str_radix(id, 16).map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if name != format!("slab_{id:016x}.wnd") {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"noncanonical restored slab name",
			));
		}
		let mut file = std::fs::File::open(entry.path())?;
		let mut hash = crate::hash::blake3::Context::new();
		let mut len = 0_u64;
		let mut buffer = [0_u8; 64 * 1024];
		loop {
			let n = std::io::Read::read(&mut file, &mut buffer)?;
			if n == 0 {
				break;
			}
			len += n as u64;
			hash.update(&buffer[..n]);
		}
		slabs.push((crate::schema::SlabId::new(id), len, hash.finish()));
	}
	slabs.sort_unstable_by_key(|slab| slab.0);
	if !slabs.iter().any(|slab| slab.0 == active_slab_id) {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"restored active slab is missing",
		));
	}
	Ok(local_slabs_hash(active_slab_id, &slabs))
}

/// Writer: validated cluster restore, before the first restored control artifact.
/// The marker alone grants nothing: both control replay and all local slab bytes
/// must match. Partial copies keep the marker but cannot create an index.
/// Clearer: successful tier/reference recovery validation, before clean certification.
/// Returns true only when resuming the identical pending restore.
pub(super) fn record_validated_storage_restore(
	backup: &ClusterBackupImage,
	node_id: u32,
	owner: &DataDirectoryOwner,
) -> std::io::Result<bool> {
	let _permit = owner.write_permit()?;
	let path = owner.path().join(STORAGE_INDEX_RESTORE_INTENT);
	let storage = &backup
		.nodes
		.iter()
		.find(|node| node.node_id == node_id)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "restore node storage is missing"))?
		.storage;
	let mut slabs: Vec<_> = storage
		.local_slabs
		.iter()
		.map(|slab| {
			(
				slab.slab_id,
				slab.bytes.len() as u64,
				crate::hash::content::digest(&slab.bytes),
			)
		})
		.collect();
	slabs.sort_unstable_by_key(|slab| slab.0);
	let intent = StorageIndexRestoreIntent {
		node_id,
		artifact_hash: backup.control.restore_artifact_hash(),
		last_applied: backup.control.manifest.last_applied_log_id,
		active_slab_id: storage.manifest.active_slab_id,
		local_slabs_hash: local_slabs_hash(storage.manifest.active_slab_id, &slabs),
	};
	let bytes = postcard::to_stdvec(&intent).map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
	match std::fs::OpenOptions::new()
		.write(true)
		.create_new(true)
		.open(&path)
	{
		Ok(mut file) => {
			std::io::Write::write_all(&mut file, &bytes)?;
			file.sync_all()?;
			std::fs::File::open(owner.path())?.sync_all()?;
			Ok(false)
		}
		Err(error) if error.kind() == ErrorKind::AlreadyExists => {
			if read_restore_intent(&path)? == bytes {
				Ok(true)
			} else {
				Err(Error::new(
					ErrorKind::InvalidData,
					"another storage restore intent is pending",
				))
			}
		}
		Err(error) => Err(error),
	}
}

pub(super) async fn validated_index_recovery(
	owner: &DataDirectoryOwner,
	node_id: u32,
	control: &WendelonStateMachine,
) -> std::io::Result<Option<crate::storage::manager::IndexRecoveryAuthority>> {
	let intent = {
		let _permit = owner.write_permit()?;
		if owner.path().join("storage_index.redb").exists() {
			return Ok(None);
		}
		let path = owner.path().join(STORAGE_INDEX_RESTORE_INTENT);
		let bytes = match read_restore_intent(&path) {
			Ok(bytes) => bytes,
			Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
			Err(error) => return Err(error),
		};
		if bytes.len() > 256 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"oversized index restore intent",
			));
		}
		postcard::from_bytes::<StorageIndexRestoreIntent>(&bytes)
			.map_err(|e| Error::new(ErrorKind::InvalidData, e))?
	};
	let (hash, applied) = control.restored_control_identity(owner).await?;
	if intent.node_id != node_id || intent.artifact_hash != hash || intent.last_applied != applied {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"index restore intent does not match validated control replay",
		));
	}
	let disk_owner = owner.clone();
	let active_slab_id = intent.active_slab_id;
	let disk_hash =
		tokio::task::spawn_blocking(move || restored_local_slabs_hash(&disk_owner, active_slab_id))
			.await
			.map_err(Error::other)??;
	if intent.local_slabs_hash != disk_hash {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"index restore intent does not match restored local slabs",
		));
	}
	Ok(Some(
		crate::storage::manager::IndexRecoveryAuthority::verified_restore(owner.clone()),
	))
}

/// What a validated replacement restore leaves for its own bootstrap: the
/// identity of the storage artifact it restored. It is minted only after
/// `restore_backup` succeeds, never from directory contents, and grants index
/// creation over those slabs only while the disk still matches it.
pub(super) struct ReplacementRestoreEvidence {
	active_slab_id: crate::schema::SlabId,
	local_slabs_hash: [u8; 32],
}

impl ReplacementRestoreEvidence {
	pub(super) fn restored(storage: &crate::storage::manager::StorageBackupImage) -> Self {
		let mut slabs: Vec<_> = storage
			.local_slabs
			.iter()
			.map(|slab| {
				(
					slab.slab_id,
					slab.bytes.len() as u64,
					crate::hash::content::digest(&slab.bytes),
				)
			})
			.collect();
		slabs.sort_unstable_by_key(|slab| slab.0);
		Self {
			active_slab_id: storage.manifest.active_slab_id,
			local_slabs_hash: local_slabs_hash(storage.manifest.active_slab_id, &slabs),
		}
	}
}

/// The replacement-restore counterpart of `validated_index_recovery`.
pub(super) async fn validated_replacement_recovery(
	owner: &DataDirectoryOwner,
	evidence: Option<&ReplacementRestoreEvidence>,
) -> std::io::Result<Option<crate::storage::manager::IndexRecoveryAuthority>> {
	let Some(evidence) = evidence else {
		return Ok(None);
	};
	if owner.path().join("storage_index.redb").exists() {
		return Ok(None);
	}
	let disk_owner = owner.clone();
	let active_slab_id = evidence.active_slab_id;
	let disk_hash =
		tokio::task::spawn_blocking(move || restored_local_slabs_hash(&disk_owner, active_slab_id))
			.await
			.map_err(Error::other)??;
	if disk_hash != evidence.local_slabs_hash {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"replacement restore does not match the restored local slabs",
		));
	}
	Ok(Some(
		crate::storage::manager::IndexRecoveryAuthority::verified_restore(owner.clone()),
	))
}

/// A first bootstrap that stopped after creating its Control artifacts and
/// before the storage index existed may resume as a fresh storage creation.
/// It qualifies only when no storage index exists in any form, the recovered
/// Control storage is pristine, and the directory holds nothing but the exact
/// artifacts such a bootstrap leaves (no slab, staged, sidecar, restore or
/// cluster evidence). Called with the owner held, before Raft starts.
pub(super) async fn interrupted_first_creation(
	owner: &DataDirectoryOwner,
	control: &crate::consensus::WendelonLogStore,
) -> std::io::Result<bool> {
	let dir = owner.path();
	match std::fs::symlink_metadata(dir.join("storage_index.redb")) {
		Ok(_) => return Ok(false),
		Err(error) if error.kind() == ErrorKind::NotFound => {}
		Err(error) => return Err(error),
	}
	if !control.is_pristine().await {
		return Ok(false);
	}
	for entry in std::fs::read_dir(dir)? {
		let entry = entry?;
		let name = entry.file_name();
		let Some(name) = name.to_str() else {
			return Ok(false);
		};
		// ADR 0726: the root KEK directory, which a forming node writes before
		// its storage index and enrollment places before a first start, may sit
		// beside the bootstrap artifacts. Nothing else that is not a file may.
		if crate::crypto::is_root_kek_artifact_dir(&entry.path())? {
			continue;
		}
		if !entry.file_type()?.is_file() || !interrupted_bootstrap_artifact(name) {
			return Ok(false);
		}
	}
	Ok(true)
}

fn interrupted_bootstrap_artifact(name: &str) -> bool {
	let wal_segment = name
		.strip_prefix("control.wal.")
		.and_then(|rest| rest.strip_suffix(".seg"))
		.is_some_and(|digits| !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()));
	wal_segment
		|| matches!(
			name,
			"LOCK"
				| "CLEAN_SHUTDOWN"
				| ".CLEAN_SHUTDOWN.tmp"
				| "control.wal"
				| "vote.wnd" | "snapshot.wnd"
				| ".snapshot.wnd.build.tmp"
				| ".snapshot.wnd.receive.tmp"
				| "storage_index.redb.initializing"
				| "PARENT_DECISIONS"
				| ".PARENT_DECISIONS.tmp"
		)
}

pub(super) fn complete_storage_index_recovery(owner: &DataDirectoryOwner) -> std::io::Result<()> {
	let _permit = owner.write_permit()?;
	match std::fs::remove_file(owner.path().join(STORAGE_INDEX_RESTORE_INTENT)) {
		Ok(()) => std::fs::File::open(owner.path())?.sync_all(),
		Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
		Err(error) => Err(error),
	}
}

pub(super) enum TransportChoice {
	Configured,
	Provided(Arc<dyn TransportProvider>, TransportTier),
}
pub(super) enum RestoreChoice<'a> {
	None,
	Cluster(&'a ClusterBackupImage),
	Replacement(&'a ClusterBackupImage, u32),
}

pub(super) struct PublishedService {
	provider: Arc<dyn TransportProvider>,
	service: Arc<dyn HrftService>,
	node: NodeId,
	address: String,
}
impl PublishedService {
	fn withdraw(self) -> std::io::Result<()> {
		match catch_unwind(AssertUnwindSafe(|| {
			self.provider
				.withdraw_local(self.node, &self.address, &self.service)
		})) {
			Ok(result) => result,
			Err(_) => Err(Error::other("provider withdrawal panicked")),
		}
	}
}
struct PartialResources {
	owner: DataDirectoryOwner,
	raft: Option<WendelonRaft>,
	factory: Option<Arc<TransportProviderFactory>>,
	lifecycle: Option<ServerLifecycle>,
	published: Vec<PublishedService>,
	transports: Vec<NodeTransportServer>,
	handles: NodeHandlesBuilder,
	bound: Duration,
	watchdog_deadline: Duration,
	runtime: tokio::runtime::Handle,
}
impl PartialResources {
	fn fence(&self) {
		// No provider callbacks here: Drop must not run foreign code or panic.
		self.owner.mark_startup_failed();
		self.owner.begin_close();
		if let Some(lifecycle) = &self.lifecycle {
			lifecycle.begin_drain();
		}
		for server in &self.transports {
			server.begin_drain();
		}
		self.handles.abort();
		if let Some(factory) = &self.factory {
			factory.begin_close();
		}
	}
	async fn cleanup(
		self,
		barrier: Option<StartupDrainBarrier>,
		progress: Arc<std::sync::Mutex<&'static str>>,
	) -> std::io::Result<()> {
		let stage = |name| {
			*progress
				.lock()
				.unwrap_or_else(std::sync::PoisonError::into_inner) = name;
		};
		stage("provider withdrawal start");
		let mut withdrawals = Vec::new();
		for registration in self.published {
			withdrawals.push(self.runtime.spawn_blocking(move || registration.withdraw()));
		}
		stage("Raft shutdown");
		let mut first_error = None;
		if let Some(raft) = self.raft {
			if let Err(error) = raft.shutdown().await {
				first_error = Some(Error::other(format!("startup Raft shutdown: {error}")));
			}
		}
		for (name, task) in self.handles.into_named() {
			stage(name);
			if let Err(error) = await_background_task(task, name).await {
				first_error.get_or_insert(error);
			}
		}
		if let Some(factory) = self.factory {
			if let Err(error) = factory.close_and_join().await {
				first_error.get_or_insert(error);
			}
		}
		stage("request quiescence");
		if let Some(lifecycle) = self.lifecycle {
			lifecycle.wait_for_quiescence().await;
		}
		stage("transport shutdown");
		for server in self.transports {
			if let Err(error) = server.shutdown().await {
				first_error.get_or_insert(error);
			}
		}
		// Foreign blocking callbacks cannot prevent the trusted cleanup above.
		// Retain their actual handles until completion; an observer timeout only
		// reports incomplete and never abandons them or releases LOCK.
		stage("provider withdrawal join");
		let mut withdrawal_error = None;
		for task in withdrawals {
			match task.await {
				Ok(Ok(())) => {}
				Ok(Err(error)) => {
					withdrawal_error.get_or_insert(error);
				}
				Err(error) => {
					withdrawal_error.get_or_insert_with(|| {
						Error::other(format!("withdrawal task failed: {error}"))
					});
				}
			}
		}
		if let Some(error) = withdrawal_error {
			// No withdrawal certificate: dropping the barrier fences/retains it.
			return Err(self.owner.startup_incomplete_from(
				error.kind(),
				format!("provider withdrawal unproved: {error}"),
				None,
			));
		}
		if let Some(barrier) = barrier {
			barrier.complete_cleanup();
		}
		stage("storage close");
		self.owner.close(self.bound).await?;
		match first_error {
			Some(error) => Err(error),
			None => Ok(()),
		}
	}
}
pub(super) struct StartupScope {
	storage_init_fresh: bool,
	replacement_restore: Option<ReplacementRestoreEvidence>,
	resources: Option<PartialResources>,
	barrier: Option<StartupDrainBarrier>,
	watchdog: Option<crate::shutdown_watchdog::ShutdownWatchdog>,
}
impl StartupScope {
	pub(super) async fn begin(owner: DataDirectoryOwner, bound: Duration) -> std::io::Result<Self> {
		Self::arm_partial(PartialResources {
			owner,
			raft: None,
			factory: None,
			lifecycle: None,
			published: Vec::new(),
			transports: Vec::new(),
			handles: NodeHandlesBuilder::default(),
			bound,
			watchdog_deadline: crate::shutdown_watchdog::daemon_shutdown_deadline(bound),
			runtime: tokio::runtime::Handle::current(),
		})
		.await
	}
	async fn arm_partial(resources: PartialResources) -> std::io::Result<Self> {
		let prepared = crate::shutdown_watchdog::ShutdownWatchdog::prepare_for_owner(
			resources.owner.closed_signal(),
		);
		Self::arm_with_prepared(resources, prepared).await
	}
	async fn arm_with_prepared(
		resources: PartialResources,
		prepared: std::io::Result<crate::shutdown_watchdog::ShutdownWatchdog>,
	) -> std::io::Result<Self> {
		let watchdog = match prepared {
			Ok(watchdog) => watchdog,
			Err(error) => {
				// Production enters here before ANY worker/listener/registration exists.
				// Fence even this refused owner and never mark failed startup clean.
				resources.fence();
				let cleanup = resources
					.cleanup(
						None,
						Arc::new(std::sync::Mutex::new("watchdog preparation refused")),
					)
					.await;
				return Err(with_cleanup_context(error, cleanup));
			}
		};
		match resources.owner.reserve_startup_drain() {
			Ok(barrier) => Ok(Self {
				storage_init_fresh: false,
				replacement_restore: None,
				resources: Some(resources),
				barrier: Some(barrier),
				watchdog: Some(watchdog),
			}),
			Err(original) => {
				let mut scope = Self {
					storage_init_fresh: false,
					replacement_restore: None,
					resources: Some(resources),
					barrier: None,
					watchdog: Some(watchdog),
				};
				let (owner, slot) = scope
					.start_cleanup(original.to_string())
					.expect("owned startup refusal resources");
				Err(with_cleanup_context(
					original,
					cleanup_result(&owner, &slot).await,
				))
			}
		}
	}

	fn resources(&self) -> &PartialResources {
		self.resources.as_ref().expect("live startup scope")
	}
	fn resources_mut(&mut self) -> &mut PartialResources {
		self.resources.as_mut().expect("live startup scope")
	}
	pub(super) fn set_storage_init_fresh(&mut self, fresh: bool) {
		self.storage_init_fresh = fresh;
	}
	pub(super) fn storage_init_fresh(&self) -> bool {
		self.storage_init_fresh
	}
	pub(super) fn set_replacement_restore(&mut self, evidence: ReplacementRestoreEvidence) {
		self.replacement_restore = Some(evidence);
	}
	pub(super) fn replacement_restore(&self) -> Option<&ReplacementRestoreEvidence> {
		self.replacement_restore.as_ref()
	}

	pub(super) fn owner(&self) -> &DataDirectoryOwner {
		&self.resources().owner
	}
	pub(super) fn handles(&mut self) -> &mut NodeHandlesBuilder {
		&mut self.resources_mut().handles
	}
	pub(super) fn adopt_factory(&mut self, factory: Arc<TransportProviderFactory>) {
		self.resources_mut().factory = Some(factory);
	}
	pub(super) fn adopt_raft(&mut self, raft: WendelonRaft) {
		self.resources_mut().raft = Some(raft);
	}
	pub(super) fn adopt_lifecycle(&mut self, lifecycle: ServerLifecycle) {
		self.resources_mut().lifecycle = Some(lifecycle);
	}
	pub(super) fn adopt_transport(&mut self, server: NodeTransportServer) {
		self.resources_mut().transports.push(server);
	}
	pub(super) fn will_publish(
		&mut self,
		provider: Arc<dyn TransportProvider>,
		service: Arc<dyn HrftService>,
		node: NodeId,
		address: &str,
	) {
		self.resources_mut().published.push(PublishedService {
			provider,
			service,
			node,
			address: address.to_owned(),
		});
	}
	#[cfg(test)]
	pub(super) fn abort_handles(&self) -> Vec<(&'static str, tokio::task::AbortHandle)> {
		{
			let mut handles = self.resources().handles.abort_handles();
			if let Some(factory) = &self.resources().factory {
				if let Some(task) = factory.invalidation_abort_handle() {
					handles.push(("transport invalidation", task));
				}
			}
			handles
		}
	}
	#[cfg(test)]
	pub(super) async fn checkpoint(
		&self,
		cfg: &NodeBootstrapConfig,
		site: &'static str,
	) -> std::io::Result<()> {
		if let Some(fault) = &cfg.startup_fault {
			fault
				.reached(
					site,
					self.owner().clone(),
					self.abort_handles(),
					self.resources().raft.clone(),
					self.resources().lifecycle.clone(),
				)
				.await?;
		}
		Ok(())
	}

	fn start_cleanup(
		&mut self,
		mut context: String,
	) -> Option<(DataDirectoryOwner, Arc<StartupCleanupSlot>)> {
		let resources = self.resources.take()?;
		let owner = resources.owner.clone();
		let closed = owner.closed_signal();
		// Trusted fencing always happens first, including on unwind. No native
		// thread creation, foreign callback, or fallible operation occurs in Drop.
		resources.fence();
		let watchdog = self.watchdog.take();
		if let Some(watchdog) = &watchdog {
			watchdog.start(resources.watchdog_deadline);
		} else {
			context.push_str("; watchdog unavailable");
			tracing::error!("startup watchdog unavailable after trusted fence");
		}
		let bound = resources.bound;
		let runtime = resources.runtime.clone();
		let barrier = self.barrier.take();
		let progress = Arc::new(std::sync::Mutex::new("startup cleanup registration"));
		let work_progress = progress.clone();
		let future = async move {
			let result = resources.cleanup(barrier, work_progress).await;
			if closed.load(std::sync::atomic::Ordering::Acquire) {
				if let Some(watchdog) = watchdog {
					watchdog.disarm();
				}
			}
			// Otherwise its OS thread stays armed even if this future panics or
			// returns incomplete. A later certified owner close suppresses it.
			result
		};
		let slot = owner.track_startup_cleanup(future, bound, context, progress, runtime);
		Some((owner, slot))
	}
	#[cfg(test)]
	pub(super) fn set_test_watchdog_deadline(&mut self, deadline: Duration) {
		self.resources_mut().watchdog_deadline = deadline;
	}

	pub(super) async fn fail(mut self, original: Error) -> Error {
		let Some((owner, slot)) = self.start_cleanup(original.to_string()) else {
			return original;
		};
		with_cleanup_context(original, cleanup_result(&owner, &slot).await)
	}
	pub(super) fn finish_success(
		mut self,
		parts: NodeParts,
	) -> Result<WendelonNode, (Self, Error)> {
		// Validation cannot consume a single handle on failure.
		if let Err(error) = self.resources().handles.validate() {
			return Err((self, error));
		}
		let resources = self.resources_mut();
		let handles = resources
			.handles
			.build()
			.expect("validated immediately before infallible handoff");
		let transports = std::mem::take(&mut resources.transports);
		// into_node contains field moves only; no callback, await or fallible work.
		let node = parts.into_node(handles, transports);
		self.barrier
			.take()
			.expect("accepted startup barrier")
			.release_after_handoff();
		self.resources.take();
		self.watchdog
			.take()
			.expect("prepared startup watchdog")
			.disarm();
		Ok(node)
	}
	#[cfg(test)]
	async fn arm(resources: Resources) -> std::io::Result<Self> {
		Self::arm_partial(resources.into_partial()).await
	}
}
impl Drop for StartupScope {
	fn drop(&mut self) {
		let _ = self.start_cleanup("bootstrap caller cancelled or unwound".into());
	}
}
#[cfg(test)]
type Guard = StartupScope;
#[derive(Default)]
pub(super) struct NodeHandlesBuilder {
	pub(super) ring_updater: Option<JoinHandle<()>>,
	pub(super) drift_monitor: Option<JoinHandle<()>>,
	pub(super) eviction_watchdog: Option<JoinHandle<()>>,
	pub(super) lease_manager: Option<JoinHandle<()>>,
	pub(super) pending_finalization_reconciler: Option<JoinHandle<()>>,
	pub(super) cdc_finalization_release_observer: Option<JoinHandle<()>>,
	pub(super) cdc_locality_heartbeat: Option<JoinHandle<()>>,
	pub(super) epoch_reconciler: Option<JoinHandle<()>>,
	pub(super) tenant_policy_reconciler: Option<JoinHandle<()>>,
	pub(super) drop_reclaim_sweeper: Option<JoinHandle<()>>,
	pub(super) migration_sweeper: Option<JoinHandle<()>>,
	pub(super) lsm_compaction: Option<JoinHandle<()>>,
	pub(super) lsm_compaction_control: Option<LsmCompactionServiceControl>,
	pub(super) lsm_gc: Option<JoinHandle<()>>,
	pub(super) op_driver: Option<JoinHandle<()>>,
	pub(super) secondary_index_lifecycle: Option<JoinHandle<()>>,
	pub(super) tiering_sweeper: Option<JoinHandle<()>>,
	pub(super) restore_validation: Option<JoinHandle<()>>,
	pub(super) audit_chain_root_rollup: Option<JoinHandle<()>>,
	pub(super) default_tenant_seeder: Option<JoinHandle<()>>,
	pub(super) checkpoint_emitter: Option<JoinHandle<()>>,
	/// ADR 0699 S3a: signer binding and the leader's floor, ParentDecisions only.
	pub(super) decision_maintenance: Option<JoinHandle<()>>,
	extra: Vec<(&'static str, JoinHandle<()>)>,
	duplicate: bool,
}
impl NodeHandlesBuilder {
	pub(super) fn set_ring_updater(&mut self, value: JoinHandle<()>) {
		if let Some(previous) = self.ring_updater.replace(value) {
			self.extra.push(("ring updater", previous));
			self.duplicate = true;
		}
	}
	pub(super) fn set_drift_monitor(&mut self, value: JoinHandle<()>) {
		if let Some(previous) = self.drift_monitor.replace(value) {
			self.extra.push(("drift monitor", previous));
			self.duplicate = true;
		}
	}
	pub(super) fn set_eviction_watchdog(&mut self, value: JoinHandle<()>) {
		if let Some(previous) = self.eviction_watchdog.replace(value) {
			self.extra.push(("eviction watchdog", previous));
			self.duplicate = true;
		}
	}
	pub(super) fn set_lease_manager(&mut self, value: JoinHandle<()>) {
		if let Some(previous) = self.lease_manager.replace(value) {
			self.extra.push(("lease manager", previous));
			self.duplicate = true;
		}
	}
	pub(super) fn set_pending_finalization_reconciler(&mut self, value: JoinHandle<()>) {
		if let Some(previous) = self.pending_finalization_reconciler.replace(value) {
			self.extra
				.push(("pending finalization reconciler", previous));
			self.duplicate = true;
		}
	}
	pub(super) fn set_cdc_finalization_release_observer(&mut self, value: JoinHandle<()>) {
		if let Some(previous) = self.cdc_finalization_release_observer.replace(value) {
			self.extra
				.push(("cdc finalization release observer", previous));
			self.duplicate = true;
		}
	}
	pub(super) fn set_cdc_locality_heartbeat(&mut self, value: JoinHandle<()>) {
		if let Some(previous) = self.cdc_locality_heartbeat.replace(value) {
			self.extra.push(("cdc locality heartbeat", previous));
			self.duplicate = true;
		}
	}
	pub(super) fn set_epoch_reconciler(&mut self, value: JoinHandle<()>) {
		if let Some(previous) = self.epoch_reconciler.replace(value) {
			self.extra.push(("epoch reconciler", previous));
			self.duplicate = true;
		}
	}
	pub(super) fn set_tenant_policy_reconciler(&mut self, value: JoinHandle<()>) {
		if let Some(previous) = self.tenant_policy_reconciler.replace(value) {
			self.extra.push(("tenant policy reconciler", previous));
			self.duplicate = true;
		}
	}
	pub(super) fn set_drop_reclaim_sweeper(&mut self, value: JoinHandle<()>) {
		if let Some(previous) = self.drop_reclaim_sweeper.replace(value) {
			self.extra.push(("drop reclaim sweeper", previous));
			self.duplicate = true;
		}
	}
	pub(super) fn set_migration_sweeper(&mut self, value: JoinHandle<()>) {
		if let Some(previous) = self.migration_sweeper.replace(value) {
			self.extra.push(("migration sweeper", previous));
			self.duplicate = true;
		}
	}
	pub(super) fn set_lsm_compaction(&mut self, value: JoinHandle<()>) {
		if let Some(previous) = self.lsm_compaction.replace(value) {
			self.extra.push(("lsm compaction", previous));
			self.duplicate = true;
		}
	}
	pub(super) fn set_lsm_compaction_control(&mut self, value: LsmCompactionServiceControl) {
		if self.lsm_compaction_control.replace(value).is_some() {
			self.duplicate = true;
		}
	}
	pub(super) fn set_lsm_gc(&mut self, value: JoinHandle<()>) {
		if let Some(previous) = self.lsm_gc.replace(value) {
			self.extra.push(("lsm gc", previous));
			self.duplicate = true;
		}
	}
	pub(super) fn set_op_driver(&mut self, value: JoinHandle<()>) {
		if let Some(previous) = self.op_driver.replace(value) {
			self.extra.push(("op driver", previous));
			self.duplicate = true;
		}
	}
	pub(super) fn set_secondary_index_lifecycle(&mut self, value: JoinHandle<()>) {
		if let Some(previous) = self.secondary_index_lifecycle.replace(value) {
			self.extra.push(("secondary index lifecycle", previous));
			self.duplicate = true;
		}
	}
	pub(super) fn set_tiering_sweeper(&mut self, value: Option<JoinHandle<()>>) {
		if let Some(previous) = self.tiering_sweeper.take() {
			self.extra.push(("tiering sweeper", previous));
			self.duplicate = true;
		}
		self.tiering_sweeper = value;
	}
	pub(super) fn set_restore_validation(&mut self, value: Option<JoinHandle<()>>) {
		if let Some(previous) = self.restore_validation.take() {
			self.extra.push(("restore validation", previous));
			self.duplicate = true;
		}
		self.restore_validation = value;
	}
	pub(super) fn set_audit_chain_root_rollup(&mut self, value: JoinHandle<()>) {
		if let Some(previous) = self.audit_chain_root_rollup.replace(value) {
			self.extra.push(("audit chain root rollup", previous));
			self.duplicate = true;
		}
	}
	pub(super) fn set_default_tenant_seeder(&mut self, value: JoinHandle<()>) {
		if let Some(previous) = self.default_tenant_seeder.replace(value) {
			self.extra.push(("default tenant seeder", previous));
			self.duplicate = true;
		}
	}
	pub(super) fn set_decision_maintenance(&mut self, value: JoinHandle<()>) {
		if let Some(previous) = self.decision_maintenance.replace(value) {
			self.extra.push(("decision maintenance", previous));
			self.duplicate = true;
		}
	}
	pub(super) fn set_checkpoint_emitter(&mut self, value: JoinHandle<()>) {
		if let Some(previous) = self.checkpoint_emitter.replace(value) {
			self.extra.push(("checkpoint emitter", previous));
			self.duplicate = true;
		}
	}
	fn abort(&self) {
		if let Some(task) = &self.ring_updater {
			task.abort();
		}
		if let Some(task) = &self.drift_monitor {
			task.abort();
		}
		if let Some(task) = &self.eviction_watchdog {
			task.abort();
		}
		if let Some(task) = &self.lease_manager {
			task.abort();
		}
		if let Some(task) = &self.pending_finalization_reconciler {
			task.abort();
		}
		if let Some(task) = &self.cdc_finalization_release_observer {
			task.abort();
		}
		if let Some(task) = &self.cdc_locality_heartbeat {
			task.abort();
		}
		if let Some(task) = &self.epoch_reconciler {
			task.abort();
		}
		if let Some(task) = &self.tenant_policy_reconciler {
			task.abort();
		}
		if let Some(task) = &self.drop_reclaim_sweeper {
			task.abort();
		}
		if let Some(task) = &self.migration_sweeper {
			task.abort();
		}
		if let Some(task) = &self.lsm_compaction {
			task.abort();
		}
		if let Some(task) = &self.lsm_gc {
			task.abort();
		}
		if let Some(task) = &self.op_driver {
			task.abort();
		}
		if let Some(task) = &self.secondary_index_lifecycle {
			task.abort();
		}
		if let Some(task) = &self.tiering_sweeper {
			task.abort();
		}
		if let Some(task) = &self.restore_validation {
			task.abort();
		}
		if let Some(task) = &self.audit_chain_root_rollup {
			task.abort();
		}
		if let Some(task) = &self.default_tenant_seeder {
			task.abort();
		}
		if let Some(task) = &self.checkpoint_emitter {
			task.abort();
		}
		if let Some(task) = &self.decision_maintenance {
			task.abort();
		}
		for (_, task) in &self.extra {
			task.abort();
		}
	}
	fn validate(&self) -> std::io::Result<()> {
		if self.duplicate || !self.extra.is_empty() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"startup has duplicate or untransferred tasks",
			));
		}
		if self.ring_updater.is_none() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"startup missing ring_updater",
			));
		}
		if self.drift_monitor.is_none() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"startup missing drift_monitor",
			));
		}
		if self.eviction_watchdog.is_none() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"startup missing eviction_watchdog",
			));
		}
		if self.lease_manager.is_none() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"startup missing lease_manager",
			));
		}
		if self.pending_finalization_reconciler.is_none() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"startup missing pending_finalization_reconciler",
			));
		}
		if self.cdc_finalization_release_observer.is_none() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"startup missing cdc_finalization_release_observer",
			));
		}
		if self.cdc_locality_heartbeat.is_none() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"startup missing cdc_locality_heartbeat",
			));
		}
		if self.epoch_reconciler.is_none() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"startup missing epoch_reconciler",
			));
		}
		if self.tenant_policy_reconciler.is_none() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"startup missing tenant_policy_reconciler",
			));
		}
		if self.drop_reclaim_sweeper.is_none() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"startup missing drop_reclaim_sweeper",
			));
		}
		if self.migration_sweeper.is_none() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"startup missing migration_sweeper",
			));
		}
		if self.lsm_compaction.is_none() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"startup missing lsm_compaction",
			));
		}
		if self.lsm_compaction_control.is_none() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"startup missing lsm_compaction_control",
			));
		}
		if self.lsm_gc.is_none() {
			return Err(Error::new(ErrorKind::InvalidData, "startup missing lsm_gc"));
		}
		if self.op_driver.is_none() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"startup missing op_driver",
			));
		}
		if self.secondary_index_lifecycle.is_none() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"startup missing secondary_index_lifecycle",
			));
		}
		if self.audit_chain_root_rollup.is_none() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"startup missing audit_chain_root_rollup",
			));
		}
		if self.default_tenant_seeder.is_none() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"startup missing default_tenant_seeder",
			));
		}
		if self.checkpoint_emitter.is_none() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"startup missing checkpoint_emitter",
			));
		}
		Ok(())
	}
	fn build(&mut self) -> std::io::Result<NodeHandles> {
		self.validate()?; // No handle is taken until EVERY required slot is valid.
		Ok(NodeHandles {
			ring_updater: self
				.ring_updater
				.take()
				.expect("validated complete startup handle set"),
			drift_monitor: self
				.drift_monitor
				.take()
				.expect("validated complete startup handle set"),
			eviction_watchdog: self
				.eviction_watchdog
				.take()
				.expect("validated complete startup handle set"),
			lease_manager: self
				.lease_manager
				.take()
				.expect("validated complete startup handle set"),
			pending_finalization_reconciler: self
				.pending_finalization_reconciler
				.take()
				.expect("validated complete startup handle set"),
			cdc_finalization_release_observer: self
				.cdc_finalization_release_observer
				.take()
				.expect("validated complete startup handle set"),
			cdc_locality_heartbeat: self
				.cdc_locality_heartbeat
				.take()
				.expect("validated complete startup handle set"),
			epoch_reconciler: self
				.epoch_reconciler
				.take()
				.expect("validated complete startup handle set"),
			tenant_policy_reconciler: self
				.tenant_policy_reconciler
				.take()
				.expect("validated complete startup handle set"),
			drop_reclaim_sweeper: self
				.drop_reclaim_sweeper
				.take()
				.expect("validated complete startup handle set"),
			migration_sweeper: self
				.migration_sweeper
				.take()
				.expect("validated complete startup handle set"),
			lsm_compaction: Some(
				self.lsm_compaction
					.take()
					.expect("validated complete startup handle set"),
			),
			lsm_compaction_control: self
				.lsm_compaction_control
				.take()
				.expect("validated complete startup handle set"),
			lsm_gc: self
				.lsm_gc
				.take()
				.expect("validated complete startup handle set"),
			op_driver: self
				.op_driver
				.take()
				.expect("validated complete startup handle set"),
			secondary_index_lifecycle: self
				.secondary_index_lifecycle
				.take()
				.expect("validated complete startup handle set"),
			tiering_sweeper: self.tiering_sweeper.take(),
			restore_validation: self.restore_validation.take(),
			audit_chain_root_rollup: self
				.audit_chain_root_rollup
				.take()
				.expect("validated complete startup handle set"),
			default_tenant_seeder: self
				.default_tenant_seeder
				.take()
				.expect("validated complete startup handle set"),
			checkpoint_emitter: self
				.checkpoint_emitter
				.take()
				.expect("validated complete startup handle set"),
			decision_maintenance: self.decision_maintenance.take(),
			lifecycle_drain_bound: crate::node::LIFECYCLE_SERVICES_DRAIN,
		})
	}
	fn into_named(mut self) -> Vec<(&'static str, JoinHandle<()>)> {
		let mut tasks = Vec::new();
		if let Some(task) = self.ring_updater.take() {
			tasks.push(("ring updater", task));
		}
		if let Some(task) = self.drift_monitor.take() {
			tasks.push(("drift monitor", task));
		}
		if let Some(task) = self.eviction_watchdog.take() {
			tasks.push(("eviction watchdog", task));
		}
		if let Some(task) = self.lease_manager.take() {
			tasks.push(("lease manager", task));
		}
		if let Some(task) = self.pending_finalization_reconciler.take() {
			tasks.push(("pending finalization reconciler", task));
		}
		if let Some(task) = self.cdc_finalization_release_observer.take() {
			tasks.push(("cdc finalization release observer", task));
		}
		if let Some(task) = self.cdc_locality_heartbeat.take() {
			tasks.push(("cdc locality heartbeat", task));
		}
		if let Some(task) = self.epoch_reconciler.take() {
			tasks.push(("epoch reconciler", task));
		}
		if let Some(task) = self.tenant_policy_reconciler.take() {
			tasks.push(("tenant policy reconciler", task));
		}
		if let Some(task) = self.drop_reclaim_sweeper.take() {
			tasks.push(("drop reclaim sweeper", task));
		}
		if let Some(task) = self.migration_sweeper.take() {
			tasks.push(("migration sweeper", task));
		}
		if let Some(task) = self.lsm_compaction.take() {
			tasks.push(("lsm compaction", task));
		}
		if let Some(task) = self.lsm_gc.take() {
			tasks.push(("lsm gc", task));
		}
		if let Some(task) = self.op_driver.take() {
			tasks.push(("op driver", task));
		}
		if let Some(task) = self.secondary_index_lifecycle.take() {
			tasks.push(("secondary index lifecycle", task));
		}
		if let Some(task) = self.tiering_sweeper.take() {
			tasks.push(("tiering sweeper", task));
		}
		if let Some(task) = self.restore_validation.take() {
			tasks.push(("restore validation", task));
		}
		if let Some(task) = self.audit_chain_root_rollup.take() {
			tasks.push(("audit chain root rollup", task));
		}
		if let Some(task) = self.default_tenant_seeder.take() {
			tasks.push(("default tenant seeder", task));
		}
		if let Some(task) = self.checkpoint_emitter.take() {
			tasks.push(("checkpoint emitter", task));
		}
		if let Some(task) = self.decision_maintenance.take() {
			tasks.push(("decision maintenance", task));
		}
		tasks.append(&mut self.extra);
		tasks
	}
	#[cfg(test)]
	pub(super) fn abort_handles(&self) -> Vec<(&'static str, tokio::task::AbortHandle)> {
		let mut tasks = Vec::new();
		if let Some(task) = &self.ring_updater {
			tasks.push(("ring_updater", task.abort_handle()));
		}
		if let Some(task) = &self.drift_monitor {
			tasks.push(("drift_monitor", task.abort_handle()));
		}
		if let Some(task) = &self.eviction_watchdog {
			tasks.push(("eviction_watchdog", task.abort_handle()));
		}
		if let Some(task) = &self.lease_manager {
			tasks.push(("lease_manager", task.abort_handle()));
		}
		if let Some(task) = &self.pending_finalization_reconciler {
			tasks.push(("pending_finalization_reconciler", task.abort_handle()));
		}
		if let Some(task) = &self.cdc_finalization_release_observer {
			tasks.push(("cdc_finalization_release_observer", task.abort_handle()));
		}
		if let Some(task) = &self.cdc_locality_heartbeat {
			tasks.push(("cdc_locality_heartbeat", task.abort_handle()));
		}
		if let Some(task) = &self.epoch_reconciler {
			tasks.push(("epoch_reconciler", task.abort_handle()));
		}
		if let Some(task) = &self.tenant_policy_reconciler {
			tasks.push(("tenant_policy_reconciler", task.abort_handle()));
		}
		if let Some(task) = &self.drop_reclaim_sweeper {
			tasks.push(("drop_reclaim_sweeper", task.abort_handle()));
		}
		if let Some(task) = &self.migration_sweeper {
			tasks.push(("migration_sweeper", task.abort_handle()));
		}
		if let Some(task) = &self.lsm_compaction {
			tasks.push(("lsm_compaction", task.abort_handle()));
		}
		if let Some(task) = &self.lsm_gc {
			tasks.push(("lsm_gc", task.abort_handle()));
		}
		if let Some(task) = &self.op_driver {
			tasks.push(("op_driver", task.abort_handle()));
		}
		if let Some(task) = &self.secondary_index_lifecycle {
			tasks.push(("secondary_index_lifecycle", task.abort_handle()));
		}
		if let Some(task) = &self.tiering_sweeper {
			tasks.push(("tiering_sweeper", task.abort_handle()));
		}
		if let Some(task) = &self.restore_validation {
			tasks.push(("restore_validation", task.abort_handle()));
		}
		if let Some(task) = &self.audit_chain_root_rollup {
			tasks.push(("audit_chain_root_rollup", task.abort_handle()));
		}
		if let Some(task) = &self.default_tenant_seeder {
			tasks.push(("default_tenant_seeder", task.abort_handle()));
		}
		if let Some(task) = &self.checkpoint_emitter {
			tasks.push(("checkpoint_emitter", task.abort_handle()));
		}
		if let Some(task) = &self.decision_maintenance {
			tasks.push(("decision_maintenance", task.abort_handle()));
		}
		for (name, task) in &self.extra {
			tasks.push((name, task.abort_handle()));
		}
		tasks
	}
}
pub(super) struct NodeParts {
	pub(super) _directory_owner: DataDirectoryOwner,
	pub(super) storage_shutdown_timeout: Duration,
	pub(super) recovered_after_unclean_shutdown: bool,
	pub(super) local_node: NodeId,
	pub(super) slab: Arc<SlabManager>,
	pub(super) crypto: Arc<CryptoManager>,
	pub(super) tracker: Arc<NodeTracker>,
	pub(super) clock: Arc<dyn PhysicalClock>,
	pub(super) hlc: Arc<HybridLogicalClockDriver>,
	pub(super) raft: WendelonRaft,
	pub(super) log_store: WendelonLogStore,
	pub(super) state_machine: WendelonStateMachine,
	pub(super) ring: Arc<RingRouter>,
	pub(super) router: Arc<MsgRouter>,
	pub(super) service: Arc<dyn HrftService>,
	pub(super) data_server: Arc<WendelonDataServer>,
	pub(super) pool: Arc<BufPool>,
	pub(super) slab_io: IoRingHandle,
	pub(super) wal_io: IoRingHandle,
	pub(super) epoch_tx: watch::Sender<Arc<MembershipEpoch>>,
	pub(super) protocol: ProtocolVersionWindow,
	pub(super) lifecycle: ServerLifecycle,
	pub(super) transport_admission: ServerAdmissionConfig,
	pub(super) audit_sink: Arc<dyn AuditSink>,
	pub(super) audit_materialization: Arc<AuditMaterializationTracker>,
	pub(super) restore_validation: watch::Receiver<RestoreValidationStatus>,
	pub(super) retention: Arc<RetentionGuard>,
	pub(super) clock_drift_events: watch::Receiver<Option<ClockDriftEvent>>,
}
impl NodeParts {
	fn into_node(
		self,
		handles: NodeHandles,
		transport_servers: Vec<NodeTransportServer>,
	) -> WendelonNode {
		WendelonNode {
			handles,
			transport_servers,
			_directory_owner: self._directory_owner,
			storage_shutdown_timeout: self.storage_shutdown_timeout,
			recovered_after_unclean_shutdown: self.recovered_after_unclean_shutdown,
			local_node: self.local_node,
			slab: self.slab,
			crypto: self.crypto,
			tracker: self.tracker,
			clock: self.clock,
			hlc: self.hlc,
			raft: self.raft,
			log_store: self.log_store,
			state_machine: self.state_machine,
			ring: self.ring,
			router: self.router,
			service: self.service,
			data_server: self.data_server,
			pool: self.pool,
			slab_io: self.slab_io,
			wal_io: self.wal_io,
			epoch_tx: self.epoch_tx,
			protocol: self.protocol,
			lifecycle: self.lifecycle,
			transport_admission: self.transport_admission,
			audit_sink: self.audit_sink,
			audit_materialization: self.audit_materialization,
			restore_validation: self.restore_validation,
			retention: self.retention,
			clock_drift_events: self.clock_drift_events,
		}
	}
}
#[derive(Debug)]
struct StartupFailure {
	original: Error,
	cleanup: Error,
}
impl std::fmt::Display for StartupFailure {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}; startup cleanup: {}", self.original, self.cleanup)
	}
}
impl std::error::Error for StartupFailure {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		Some(&self.cleanup)
	}
}
fn with_cleanup_context(original: Error, cleanup: std::io::Result<()>) -> Error {
	match cleanup {
		Ok(()) => original,
		Err(cleanup) => Error::new(original.kind(), StartupFailure { original, cleanup }),
	}
}

async fn cleanup_result(
	owner: &DataDirectoryOwner,
	slot: &StartupCleanupSlot,
) -> std::io::Result<()> {
	match slot.wait().await {
		StartupCleanupStatus::Complete => {
			slot.join_finished().await;
			Ok(())
		}
		StartupCleanupStatus::PendingIncomplete(message) => Err(owner.startup_incomplete(message)),
		StartupCleanupStatus::Failed {
			kind,
			message,
			incomplete,
			outstanding,
		} => {
			slot.join_finished().await;
			if incomplete {
				Err(owner.startup_incomplete_from(kind, message, outstanding))
			} else {
				Err(Error::new(kind, message))
			}
		}
		StartupCleanupStatus::Running => unreachable!("wait returns a report"),
	}
}

#[cfg(test)]
struct Tasks {
	ring_updater: JoinHandle<()>,
	drift_monitor: JoinHandle<()>,
	audit_chain_root_rollup: JoinHandle<()>,
	checkpoint_emitter: JoinHandle<()>,
	epoch_reconciler: JoinHandle<()>,
}
#[cfg(test)]
struct Resources {
	owner: DataDirectoryOwner,
	raft: WendelonRaft,
	lifecycle: ServerLifecycle,
	transports: Vec<NodeTransportServer>,
	tasks: Tasks,
	bound: Duration,
	runtime: tokio::runtime::Handle,
}
#[cfg(test)]
impl Resources {
	fn fence(&self) {
		self.lifecycle.begin_drain();
		self.owner.begin_close();
		for server in &self.transports {
			server.begin_drain();
		}
		self.tasks.ring_updater.abort();
		self.tasks.drift_monitor.abort();
		self.tasks.audit_chain_root_rollup.abort();
		self.tasks.checkpoint_emitter.abort();
		self.tasks.epoch_reconciler.abort();
	}
	fn into_partial(self) -> PartialResources {
		let mut handles = NodeHandlesBuilder::default();
		handles.set_ring_updater(self.tasks.ring_updater);
		handles.set_drift_monitor(self.tasks.drift_monitor);
		handles.set_audit_chain_root_rollup(self.tasks.audit_chain_root_rollup);
		handles.set_checkpoint_emitter(self.tasks.checkpoint_emitter);
		handles.set_epoch_reconciler(self.tasks.epoch_reconciler);
		PartialResources {
			owner: self.owner,
			raft: Some(self.raft),
			factory: None,
			lifecycle: Some(self.lifecycle),
			transports: self.transports,
			published: Vec::new(),
			handles,
			bound: self.bound,
			watchdog_deadline: crate::shutdown_watchdog::daemon_shutdown_deadline(self.bound),
			runtime: self.runtime,
		}
	}
	async fn cleanup(
		self,
		barrier: Option<StartupDrainBarrier>,
		progress: Arc<std::sync::Mutex<&'static str>>,
	) -> std::io::Result<()> {
		self.into_partial().cleanup(barrier, progress).await
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::sync::atomic::{AtomicBool, Ordering};

	/// An established node's Control history is never mistaken for an
	/// interrupted first bootstrap, even with every storage artifact gone.
	#[tokio::test]
	async fn established_control_history_never_resumes_as_a_fresh_storage_creation() {
		let dir = crate::test_support::TestDir::new("established-control-missing-index");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			7113,
			dir.to_path_buf(),
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		node.shutdown().await.unwrap();
		for entry in std::fs::read_dir(&*dir).unwrap() {
			let entry = entry.unwrap();
			let name = entry.file_name().to_string_lossy().into_owned();
			if !interrupted_bootstrap_artifact(&name) {
				if entry.file_type().unwrap().is_dir() {
					std::fs::remove_dir_all(entry.path()).unwrap();
				} else {
					std::fs::remove_file(entry.path()).unwrap();
				}
			}
		}
		let owner = DataDirectoryOwner::acquire_existing(&*dir).unwrap();
		let control = crate::consensus::WendelonLogStore::open(
			dir.join("control.wal"),
			dir.join("vote.wnd"),
			dir.join("snapshot.wnd"),
			crate::storage::io_uring::spawn_io_thread(64).unwrap(),
		)
		.await
		.unwrap();
		assert!(
			!interrupted_first_creation(&owner, &control).await.unwrap(),
			"initialized Control history must not resume as a fresh creation"
		);
	}

	/// Only pristine Control evidence plus the exact artifacts an interrupted
	/// first bootstrap leaves may resume as a fresh storage creation.
	#[tokio::test]
	async fn interrupted_first_creation_needs_pristine_control_and_only_bootstrap_artifacts() {
		use openraft::storage::RaftLogStorage;
		let dir = crate::test_support::TestDir::new("interrupted-first-creation");
		let owner = DataDirectoryOwner::acquire_or_create(&*dir).unwrap();
		let io = crate::storage::io_uring::spawn_io_thread(64).unwrap();
		let (wal, vote, snapshot) = (
			dir.join("control.wal"),
			dir.join("vote.wnd"),
			dir.join("snapshot.wnd"),
		);
		let mut control =
			crate::consensus::WendelonLogStore::open(&wal, &vote, &snapshot, io.clone())
				.await
				.unwrap();
		let listing = || {
			std::fs::read_dir(&*dir)
				.unwrap()
				.map(|entry| entry.unwrap().file_name())
				.collect::<Vec<_>>()
		};
		assert!(
			interrupted_first_creation(&owner, &control).await.unwrap(),
			"pristine Control and bootstrap artifacts only: {:?}",
			listing()
		);
		// ADR 0726: the root KEK directory a forming node writes first, or
		// enrollment places, may sit beside them. It stays in place for every
		// refusal below, so a root file never masks real evidence.
		let root_dir = dir.join(".wendelon");
		std::fs::create_dir(&root_dir).unwrap();
		assert!(interrupted_first_creation(&owner, &control).await.unwrap());
		std::fs::write(root_dir.join("local-sealed-root-kek.bin"), b"sealed").unwrap();
		assert!(
			interrupted_first_creation(&owner, &control).await.unwrap(),
			"a root KEK artifact directory is neutral: {:?}",
			listing()
		);
		assert!(!fresh_storage_initialization_allowed(&dir).unwrap());
		std::fs::write(root_dir.join("other"), b"x").unwrap();
		assert!(
			!interrupted_first_creation(&owner, &control).await.unwrap(),
			"anything else in the root KEK directory is not"
		);
		std::fs::remove_file(root_dir.join("other")).unwrap();
		for (name, what) in [
			("slab_0000000000000000.wnd", "slab evidence"),
			(
				"compaction-temp-0000000000000001-slab_0000000000000001.wnd",
				"staged evidence",
			),
			(STORAGE_INDEX_RESTORE_INTENT, "restore authority"),
			(
				crate::storage::manager::AWAITING_CLUSTER_MARKER,
				"cluster replacement marker",
			),
			("notes.txt", "an unknown file"),
		] {
			std::fs::write(dir.join(name), b"x").unwrap();
			assert!(
				!interrupted_first_creation(&owner, &control).await.unwrap(),
				"{what} must not resume as a fresh creation"
			);
			std::fs::remove_file(dir.join(name)).unwrap();
		}
		std::fs::create_dir(dir.join("storage_index.redb")).unwrap();
		assert!(
			!interrupted_first_creation(&owner, &control).await.unwrap(),
			"an unreadable index is not an absent one"
		);
		std::fs::remove_dir(dir.join("storage_index.redb")).unwrap();
		control.save_vote(&openraft::Vote::new(1, 1)).await.unwrap();
		assert!(
			!interrupted_first_creation(&owner, &control).await.unwrap(),
			"a persisted vote is Control history"
		);
		drop(control);
		let reopened = crate::consensus::WendelonLogStore::open(&wal, &vote, &snapshot, io)
			.await
			.unwrap();
		assert!(
			!interrupted_first_creation(&owner, &reopened).await.unwrap(),
			"a persisted vote survives reopen"
		);
	}
	struct FailingWorker(AtomicBool);
	#[async_trait]
	impl crate::storage::ownership::StorageWorker for FailingWorker {
		async fn close_and_join(&self) -> std::io::Result<()> {
			if self.0.load(Ordering::SeqCst) {
				Err(Error::new(
					ErrorKind::PermissionDenied,
					"named startup worker close denied",
				))
			} else {
				Ok(())
			}
		}
	}
	#[tokio::test]
	async fn failed_cleanup_keeps_original_startup_error_kind_and_both_causes() {
		let dir = crate::test_support::TestDir::new("startup-error-kind");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			7112,
			dir.to_path_buf(),
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let panic_task = tokio::spawn(async {
			panic!("startup cleanup producer panic");
		});
		while !panic_task.is_finished() {
			tokio::task::yield_now().await;
		}
		let resources = Resources {
			owner: node._directory_owner.clone(),
			raft: node.raft.clone(),
			lifecycle: node.lifecycle.clone(),
			transports: Vec::new(),
			tasks: Tasks {
				ring_updater: panic_task,
				drift_monitor: tokio::spawn(std::future::pending()),
				audit_chain_root_rollup: tokio::spawn(std::future::pending()),
				checkpoint_emitter: tokio::spawn(std::future::pending()),
				epoch_reconciler: tokio::spawn(std::future::pending()),
			},
			bound: Duration::from_secs(5),
			runtime: tokio::runtime::Handle::current(),
		};
		let guard = Guard::arm(resources).await.unwrap();
		let error = guard
			.fail(Error::new(ErrorKind::InvalidInput, "original JOIN refusal"))
			.await;
		let kind = error.kind();
		let text = error.to_string();
		let reports = node._directory_owner.startup_cleanup_reports();
		node.shutdown().await.unwrap();
		assert_eq!(kind, ErrorKind::InvalidInput, "{text}");
		assert!(
			text.contains("original JOIN refusal") && text.contains("ring updater task failed"),
			"{text}"
		);
		assert!(reports.iter().any(|r| matches!(r, StartupCleanupStatus::Failed { message, .. } if message.contains("original JOIN refusal") && message.contains("ring updater task failed"))), "{reports:?}");
	}

	#[test]
	fn actual_cleanup_joins_blocked_and_panicked_tasks_preserving_storage_error() {
		const CHILD: &str = "NILKET_STARTUP_RESOURCE_CHILD";
		if std::env::var_os(CHILD).is_none() {
			let output = std::process::Command::new(std::env::current_exe().unwrap()).env(CHILD, "1")
				.arg("node::startup::tests::actual_cleanup_joins_blocked_and_panicked_tasks_preserving_storage_error").arg("--exact").arg("--nocapture").output().unwrap();
			assert!(
				output.status.success(),
				"resource control child failed: {}",
				String::from_utf8_lossy(&output.stderr)
			);
			return;
		}
		tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
			let dir = crate::test_support::TestDir::new("startup-real-cleanup");
			let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(7111, dir.to_path_buf(), "127.0.0.1:0".into())).await.unwrap();
			let owner = node._directory_owner.clone();
			let failing = Arc::new(FailingWorker(AtomicBool::new(true)));
			owner.register_worker(failing.clone()).unwrap();
			let panic_task = tokio::spawn(async { panic!("joined startup producer panic"); });
			tokio::time::timeout(Duration::from_secs(10), async { while !panic_task.is_finished() { tokio::task::yield_now().await; } }).await.unwrap();
			let (release, wait) = std::sync::mpsc::channel();
			let (started, entered) = std::sync::mpsc::channel();
			let blocked = tokio::task::spawn_blocking(move || { let _ = started.send(()); let _ = wait.recv_timeout(Duration::from_secs(10)); });
			entered.recv_timeout(Duration::from_secs(10)).unwrap();
			let resources = Resources { owner: owner.clone(), raft: node.raft.clone(), lifecycle: node.lifecycle.clone(), transports: Vec::new(),
				tasks: Tasks { ring_updater: panic_task, drift_monitor: blocked, audit_chain_root_rollup: tokio::spawn(std::future::pending()), checkpoint_emitter: tokio::spawn(std::future::pending()), epoch_reconciler: tokio::spawn(std::future::pending()) },
				bound: Duration::from_secs(5), runtime: tokio::runtime::Handle::current() };
			let barrier = owner.reserve_startup_drain().unwrap(); resources.fence();
			let progress = Arc::new(std::sync::Mutex::new("resource control"));
			let slot = owner.track_startup_cleanup(resources.cleanup(Some(barrier), progress.clone()), Duration::from_millis(20), "original JOIN error".into(), progress.clone(), tokio::runtime::Handle::current());
			tokio::time::timeout(Duration::from_secs(5), async { while *progress.lock().unwrap() != "drift monitor" { tokio::task::yield_now().await; } }).await.unwrap();
			let pending = matches!(slot.wait().await, StartupCleanupStatus::PendingIncomplete(_));
			let external_incomplete = owner.close(Duration::from_millis(20)).await.err().is_some_and(|error| error.get_ref().is_some_and(|e| e.is::<crate::storage::ownership::ShutdownIncomplete>()));
			let busy = DataDirectoryOwner::acquire_existing(&dir).is_err();
			let _ = release.send(());
			let terminal = tokio::time::timeout(Duration::from_secs(10), async { loop {
				let status = slot.wait().await; if !matches!(status, StartupCleanupStatus::PendingIncomplete(_) | StartupCleanupStatus::Running) { break status; }
				tokio::task::yield_now().await;
			}}).await.unwrap();
			let report_preserved = matches!(terminal, StartupCleanupStatus::Failed { kind: ErrorKind::PermissionDenied, ref message, incomplete: true, outstanding: Some(0) } if message.contains("named startup worker close denied"));
			let result = cleanup_result(&owner, &slot).await.unwrap_err();
			let result_preserved = result.kind() == ErrorKind::PermissionDenied && result.get_ref().is_some_and(|e| e.is::<crate::storage::ownership::ShutdownIncomplete>()) && std::error::Error::source(&result).is_some_and(|e| e.to_string().contains("named startup worker close denied"));
			failing.0.store(false, Ordering::SeqCst); owner.close(Duration::from_secs(5)).await.unwrap();
			node.shutdown().await.unwrap(); drop(owner);
			let reopened = DataDirectoryOwner::acquire_existing(&dir).unwrap(); reopened.close(Duration::from_secs(5)).await.unwrap();
			assert!(pending && external_incomplete && busy && report_preserved && result_preserved,
				"pending={pending} external_incomplete={external_incomplete} busy={busy} report={report_preserved} result={result_preserved}");
		});
	}
	#[tokio::test]
	async fn pd25_builder_refusal_keeps_every_handle_for_scope_cleanup() {
		for duplicate in [false, true] {
			let dir = crate::test_support::TestDir::new("pd25-builder-refusal");
			let owner = DataDirectoryOwner::acquire_existing(&dir).unwrap();
			let mut scope = StartupScope::begin(owner.clone(), Duration::from_secs(5))
				.await
				.unwrap();
			scope
				.handles()
				.set_ring_updater(tokio::spawn(std::future::pending()));
			if duplicate {
				scope
					.handles()
					.set_ring_updater(tokio::spawn(std::future::pending()));
			}
			let handles = scope.abort_handles();
			assert!(scope.handles().build().is_err());
			// Failed validation must leave even the valid prefix and displaced
			// duplicate available to the same cleanup that owns LOCK.
			assert_eq!(scope.abort_handles().len(), if duplicate { 2 } else { 1 });
			let error = scope
				.fail(Error::new(ErrorKind::InvalidData, "builder refusal"))
				.await;
			assert_eq!(error.kind(), ErrorKind::InvalidData);
			assert!(handles.iter().all(|(_, handle)| handle.is_finished()));
			assert!(
				owner
					.closed_signal()
					.load(std::sync::atomic::Ordering::Acquire)
			);
			let reopened = DataDirectoryOwner::acquire_existing(&dir).unwrap();
			reopened.close(Duration::from_secs(5)).await.unwrap();
		}
	}
	#[tokio::test]
	async fn pd25_watchdog_preparation_refusal_fences_before_any_construction() {
		let dir = crate::test_support::TestDir::new("pd25-watchdog-prepare-refusal");
		let owner = DataDirectoryOwner::acquire_existing(&dir).unwrap();
		assert!(owner.previous_shutdown_clean());
		std::fs::write(
			dir.join("constructor-observation"),
			b"fresh owner now has unvalidated state",
		)
		.unwrap();
		let resources = PartialResources {
			owner: owner.clone(),
			raft: None,
			factory: None,
			lifecycle: None,
			published: Vec::new(),
			transports: Vec::new(),
			handles: NodeHandlesBuilder::default(),
			bound: Duration::from_secs(5),
			watchdog_deadline: Duration::from_secs(5),
			runtime: tokio::runtime::Handle::current(),
		};
		let error = match StartupScope::arm_with_prepared(
			resources,
			Err(Error::new(
				ErrorKind::WouldBlock,
				"injected native thread creation refused",
			)),
		)
		.await
		{
			Err(error) => error,
			Ok(_) => panic!("watchdog preparation refusal ignored"),
		};
		assert_eq!(error.kind(), ErrorKind::WouldBlock);
		assert!(owner.write_permit().is_err());
		assert!(
			owner
				.closed_signal()
				.load(std::sync::atomic::Ordering::Acquire)
		);
		assert!(!dir.join("CLEAN_SHUTDOWN").exists());
		let reopened = DataDirectoryOwner::acquire_existing(&dir).unwrap();
		assert!(!reopened.previous_shutdown_clean());
		reopened.close(Duration::from_secs(5)).await.unwrap();
	}
}

/// Pre-restore, owner-held fresh authorization. ADR 0685's DataDirCensus
/// replaces this temporary same-boot predicate when the stacks join.
pub(super) fn fresh_storage_initialization_allowed(dir: &std::path::Path) -> std::io::Result<bool> {
	for entry in std::fs::read_dir(dir)? {
		let entry = entry?;
		let name = entry.file_name();
		if name != "LOCK"
			&& name != "CLEAN_SHUTDOWN"
			&& name != ".CLEAN_SHUTDOWN.tmp"
			// ADR 0726: an enrolled root KEK directory is setup, not a store.
			&& !crate::crypto::is_root_kek_artifact_dir(&entry.path())?
		{
			return Ok(false);
		}
	}
	Ok(true)
}
