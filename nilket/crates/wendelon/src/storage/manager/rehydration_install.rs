//! ADR 0131: a rehydration installer owns its slab paths until its copy is
//! registered or removed, and a released slab file stays visible until its
//! unlink and disposition finish.
//!
//! The read that fetched an admitted attempt's object hands the attempt to a
//! manager-owned installer task (`hand_off_rehydration`) and then only waits
//! for its outcome. The installer runs inside an admitted operation of the
//! directory owner (D2). It creates the temporary name, writes and syncs the
//! copy through that handle, links it without replacing, removes the
//! temporary name by identity, and registers the copy only while its attempt,
//! its admission and the slab's pointer are all still current; otherwise every
//! name it owns is removed by identity. It alone publishes the outcome, before
//! its record (the slab's exclusion) goes. A released slab file carries the
//! identity its backing read at open; its unlink belongs to a release settler
//! (`slab_file_quarantine.rs`) and its record to the local file ledger
//! (`local_file_ledger.rs`, ADR 0132).
use super::*;
use crate::storage::FileHandle;
use crate::storage::manager::tiered_io::{AdmittedRehydration, RehydrationAttempt};
use crate::storage::ownership::AdmittedOperation;
use tokio::sync::watch;

/// D5: promotion attempts in flight per manager, from admission to settlement.
const MAX_CONCURRENT_PROMOTIONS: usize = 2;

/// D3: plaintext bytes copied into one submission of the installer's write, so
/// a short write never copies a slab-sized remainder.
const INSTALL_WRITE_CHUNK_BYTES: usize = 1 << 20;

/// D5: the waits between the three tries of one cleanup step.
pub(super) const CLEANUP_RETRY_DELAYS: [Duration; 2] =
	[Duration::from_millis(100), Duration::from_millis(400)];

/// A file's (device, inode).
pub(super) type FileIdentity = (u64, u64);

pub(super) fn rehydrate_tmp_path(dir: &Path, slab_id: SlabId) -> PathBuf {
	dir.join(format!("slab_{:016x}.wnd.rehydrate.tmp", slab_id))
}

/// A slab's two rehydration names: the temporary one, then the final one.
fn rehydration_names(dir: &Path, slab_id: SlabId) -> (PathBuf, PathBuf) {
	(rehydrate_tmp_path(dir, slab_id), slab_path(dir, slab_id))
}

/// Which of a slab's two rehydration names a report concerns: the temporary
/// `slab_{id}.wnd.rehydrate.tmp` or the final `slab_{id}.wnd`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RehydrationName {
	Temporary,
	Final,
}

/// Why promotion of a slab is inhibited. The label set is stable and bounded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RehydrationInhibitionReason {
	/// The temporary name holds a file this process did not create, or one
	/// whose identity it could not read: it is never touched.
	RehydrationTempPresent,
	/// A name an attempt owned could not be removed.
	RehydrationCleanupFailed,
}

impl RehydrationInhibitionReason {
	pub const fn label(self) -> &'static str {
		match self {
			Self::RehydrationTempPresent => "rehydration_temp_present",
			Self::RehydrationCleanupFailed => "rehydration_cleanup_failed",
		}
	}
}

/// One slab whose promotion is inhibited (`rehydration_inhibited`). Reported
/// apart from the final-file quarantine, whose evidence it never changes. Only
/// promotion is inhibited, and GC only while the stuck name is the final file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RehydrationInhibition {
	pub slab_id: SlabId,
	pub reason: RehydrationInhibitionReason,
	pub name: RehydrationName,
	/// The file's identity, where it could be read.
	pub identity: Option<(u64, u64)>,
}

/// An installer's outcome as its watch carries it. A `watch` keeps its last
/// value, so a reader that first waits after settlement still sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum InstallState {
	Pending,
	Settled(InstallOutcome),
}

/// How an installer settled (D3, D5). Any inhibition it reported is published
/// with it, in `rehydration_inhibited`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum InstallOutcome {
	/// The copy is registered and serves its generation.
	Registered,
	/// Nothing was registered; every name the attempt owned that still held its
	/// candidate was removed.
	Removed,
	/// The temporary name's open failed and left a file whose identity this
	/// attempt never held: it was left in place, never removed by name alone,
	/// and reported.
	IdentityUnknown,
	/// The temporary name held a file this attempt did not create.
	NoAuthority,
	/// A name the attempt owned could not be removed after its retries.
	CleanupFailed,
}

/// D3: how far an installer has come with its names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InstallPhase {
	/// Handed off; no name created yet.
	HandedOff,
	/// The temporary name was created and its identity recorded.
	Created,
	/// The copy's bytes are written and synced.
	Written,
	/// The final name links the candidate; `tmp_present` until the temporary
	/// name is removed.
	Linked { tmp_present: bool },
}

/// One slab's installer (D1). While it exists it is the slab's exclusion: no
/// admission, GC, release unlink or copy drop of the slab.
struct InstallRecord {
	attempt: RehydrationAttempt,
	pointer: TieredSlabPointer,
	phase: InstallPhase,
	candidate: Option<FileIdentity>,
	final_name_owned: bool,
	outcome: watch::Sender<InstallState>,
}

/// The names a record owns, as its disposition reads them.
struct OwnedNames {
	candidate: Option<FileIdentity>,
	temporary: bool,
	final_name: bool,
}

#[derive(Default)]
struct InstallsState {
	installers: HashMap<SlabId, InstallRecord>,
	inhibited: BTreeMap<SlabId, RehydrationInhibition>,
}

impl InstallsState {
	/// The terminal step: any inhibition, then the outcome published, then the
	/// record removed, in one critical section, so no observer sees the slab's
	/// exclusion end without both.
	fn settle(
		&mut self,
		slab_id: SlabId,
		attempt: RehydrationAttempt,
		outcome: InstallOutcome,
		inhibition: Option<RehydrationInhibition>,
	) {
		if let Some(inhibition) = inhibition {
			tracing::warn!(
				slab_id = inhibition.slab_id.get(),
				reason = inhibition.reason.label(),
				name = ?inhibition.name,
				identity = ?inhibition.identity,
				"promotion of the slab is inhibited; it keeps serving from L2"
			);
			// One entry per slab; a final name's protection outranks a temporary one's.
			let keep = self
				.inhibited
				.get(&slab_id)
				.is_some_and(|held| held.name == RehydrationName::Final);
			if !keep {
				self.inhibited.insert(slab_id, inhibition);
			}
		}
		if let std::collections::hash_map::Entry::Occupied(record) = self.installers.entry(slab_id)
			&& record.get().attempt == attempt
		{
			record
				.get()
				.outcome
				.send_replace(InstallState::Settled(outcome));
			record.remove();
		}
	}
}

/// The manager's installers, their capacity (D5) and the promotion
/// inhibitions they reported.
pub(super) struct RehydrationInstalls {
	manager: std::sync::OnceLock<std::sync::Weak<SlabManager>>,
	promotion_slots: Arc<tokio::sync::Semaphore>,
	state: std::sync::Mutex<InstallsState>,
}

impl Default for RehydrationInstalls {
	fn default() -> Self {
		Self {
			manager: std::sync::OnceLock::new(),
			promotion_slots: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_PROMOTIONS)),
			state: std::sync::Mutex::new(InstallsState::default()),
		}
	}
}

impl RehydrationInstalls {
	/// Binds the manager its installer and release settler tasks run against.
	pub(super) fn bind(&self, manager: &Arc<SlabManager>) {
		let _ = self.manager.set(Arc::downgrade(manager));
	}

	pub(super) fn manager(&self) -> Option<Arc<SlabManager>> {
		self.manager.get().and_then(std::sync::Weak::upgrade)
	}

	fn lock(&self) -> std::sync::MutexGuard<'_, InstallsState> {
		self.state
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
	}

	/// D5: one capacity token, never waited for.
	pub(super) fn try_acquire_slot(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
		self.promotion_slots.clone().try_acquire_owned().ok()
	}

	/// Whether an installer holds the slab's names.
	pub(super) fn has_record(&self, slab_id: SlabId) -> bool {
		self.lock().installers.contains_key(&slab_id)
	}

	/// ADR 0132 D3: whether any installer still holds a slab's names.
	pub(super) fn holds_any_record(&self) -> bool {
		!self.lock().installers.is_empty()
	}

	pub(super) fn inhibition(&self, slab_id: SlabId) -> Option<RehydrationInhibition> {
		self.lock().inhibited.get(&slab_id).copied()
	}

	/// D1, D5: GC refuses the slab while an installer holds its names, or while
	/// a cleanup failure left its final name behind.
	pub(super) fn refuses_gc(&self, slab_id: SlabId) -> bool {
		let installs = self.lock();
		installs.installers.contains_key(&slab_id)
			|| installs
				.inhibited
				.get(&slab_id)
				.is_some_and(|inhibition| inhibition.name == RehydrationName::Final)
	}

	/// Records a step of `attempt`'s installer on its own record.
	fn update(
		&self,
		slab_id: SlabId,
		attempt: RehydrationAttempt,
		change: impl FnOnce(&mut InstallRecord),
	) {
		if let Some(record) = self
			.lock()
			.installers
			.get_mut(&slab_id)
			.filter(|record| record.attempt == attempt)
		{
			change(record);
		}
	}

	fn owned_names(&self, slab_id: SlabId, attempt: RehydrationAttempt) -> Option<OwnedNames> {
		let installs = self.lock();
		let record = installs
			.installers
			.get(&slab_id)
			.filter(|record| record.attempt == attempt)?;
		Some(OwnedNames {
			candidate: record.candidate,
			temporary: record.candidate.is_some()
				&& !matches!(record.phase, InstallPhase::Linked { tmp_present: false }),
			final_name: record.final_name_owned,
		})
	}
}

/// A settler's outcome, shared among its waiters.
pub(super) type ReleaseOutcome = Result<(), (ErrorKind, String)>;

/// Which route claimed a release. GC keeps its own failure propagation and
/// its own test holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ReleaseRoute {
	Release,
	Gc,
}

/// What a claim of a slab's release found.
pub(super) enum ReleaseClaim {
	/// No release is recorded for the slab.
	Absent,
	/// The release was owed; its settler now owns it.
	Claimed(watch::Receiver<Option<ReleaseOutcome>>),
	/// Another claim's settler owns it.
	InProgress(watch::Receiver<Option<ReleaseOutcome>>),
}

/// The outcome a release waiter is told: the settler's, or an error if it
/// ended without one.
pub(super) async fn wait_release(
	mut settled: watch::Receiver<Option<ReleaseOutcome>>,
) -> std::io::Result<()> {
	let outcome = match settled.wait_for(Option::is_some).await {
		Ok(outcome) => (*outcome).clone(),
		Err(_) => None,
	};
	match outcome {
		Some(Ok(())) => Ok(()),
		Some(Err((kind, message))) => Err(Error::new(kind, message)),
		None => Err(Error::other("the release settler ended without an outcome")),
	}
}

pub(super) fn release_outcome(result: &std::io::Result<()>) -> ReleaseOutcome {
	match result {
		Ok(()) => Ok(()),
		Err(error) => Err((error.kind(), error.to_string())),
	}
}

/// The reader's wait (D1): the outcome its installer published, or `None` if
/// the installer ended without one. Dropping the wait changes nothing.
pub(super) async fn wait_settled(
	mut outcome: watch::Receiver<InstallState>,
) -> Option<InstallOutcome> {
	match outcome
		.wait_for(|state| matches!(state, InstallState::Settled(_)))
		.await
	{
		Ok(state) => match *state {
			InstallState::Settled(outcome) => Some(outcome),
			InstallState::Pending => None,
		},
		Err(_) => None,
	}
}

/// D3: the whole of `bytes` written at offset 0 of `handle`, with `write_all`
/// semantics. Each submission carries at most `INSTALL_WRITE_CHUNK_BYTES`
/// copied from the current offset and takes its own write admission (a child
/// permit, inside the installer's operation). A short write advances by its
/// count; an interrupted one is retried; no progress is `WriteZero`.
async fn write_all_at(
	file_ops: &dyn AsyncFileOps,
	handle: &FileHandle,
	bytes: &[u8],
) -> std::io::Result<()> {
	let mut offset = 0usize;
	while offset < bytes.len() {
		let end = bytes
			.len()
			.min(offset.saturating_add(INSTALL_WRITE_CHUNK_BYTES));
		let mut chunk = Vec::new();
		chunk
			.try_reserve_exact(end - offset)
			.map_err(|_| Error::new(ErrorKind::OutOfMemory, "rehydration write chunk"))?;
		chunk.extend_from_slice(&bytes[offset..end]);
		match file_ops.write_plain_at(handle, offset as u64, chunk).await {
			Ok(0) => {
				return Err(Error::new(
					ErrorKind::WriteZero,
					"rehydration write made no progress",
				));
			}
			Ok(written) => offset += written.min(end - offset),
			Err(error) if error.kind() == ErrorKind::Interrupted => {}
			Err(error) => return Err(error),
		}
	}
	debug_assert_eq!(offset, bytes.len());
	Ok(())
}

/// Removes `path` only while it names `candidate`. A missing name, or another
/// file at it, is left as it is: no narrower key authorizes a deletion.
async fn remove_by_identity(
	file_ops: &dyn AsyncFileOps,
	path: &Path,
	candidate: FileIdentity,
) -> std::io::Result<()> {
	match file_ops.path_identity(path).await? {
		Some(found) if found == candidate => match file_ops.unlink(path).await {
			Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
			result => result,
		},
		_ => Ok(()),
	}
}

/// D5: a cleanup step tried up to three times, 100 ms then 400 ms apart; each
/// try runs the step afresh.
pub(super) async fn with_cleanup_retries<F, Fut>(mut step: F) -> std::io::Result<()>
where
	F: FnMut() -> Fut,
	Fut: std::future::Future<Output = std::io::Result<()>>,
{
	for delay in CLEANUP_RETRY_DELAYS {
		match step().await {
			Ok(()) => return Ok(()),
			Err(error) => tracing::debug!(%error, "a rehydration cleanup step failed; retrying"),
		}
		tokio::time::sleep(delay).await;
	}
	step().await
}

/// The attempt as its installer task owns it (D1, D5).
struct InstallJob {
	slab_id: SlabId,
	attempt: RehydrationAttempt,
	pointer: TieredSlabPointer,
	plaintext: TieredPlaintext,
	operation: Option<AdmittedOperation>,
	slot: tokio::sync::OwnedSemaphorePermit,
}

/// Why an installation stopped before its copy existed.
enum InstallStop {
	/// The temporary name held another file (its identity, if readable).
	NoAuthority(Option<FileIdentity>),
	/// The open of the temporary name failed, and a file is there now: perhaps
	/// created by this attempt, but with no identity it can claim.
	IdentityUnknown(Error, Option<FileIdentity>),
	Failed(Error),
}

/// What the installer publishes, with the inhibition published beside it.
struct Settlement {
	outcome: InstallOutcome,
	inhibition: Option<(
		RehydrationInhibitionReason,
		RehydrationName,
		Option<FileIdentity>,
	)>,
}

impl Settlement {
	fn removed() -> Self {
		Self {
			outcome: InstallOutcome::Removed,
			inhibition: None,
		}
	}

	fn temp_present(outcome: InstallOutcome, identity: Option<FileIdentity>) -> Self {
		Self {
			outcome,
			inhibition: Some((
				RehydrationInhibitionReason::RehydrationTempPresent,
				RehydrationName::Temporary,
				identity,
			)),
		}
	}
}

impl SlabManager {
	/// The slabs whose promotion this manager inhibited (ADR 0131), sorted by
	/// slab id. A separate report from `quarantined_slab_files`, whose evidence
	/// it never changes; nothing here authorizes cleanup.
	pub fn rehydration_inhibited(&self) -> Vec<RehydrationInhibition> {
		self.rehydration_installs
			.lock()
			.inhibited
			.values()
			.copied()
			.collect()
	}

	/// D1: the handoff from the read that fetched an admitted attempt's object
	/// to the manager's installer. Under the state lock, with no await between
	/// the steps: (1) the exact-token admission is confirmed (DC-305); (2) an
	/// admitted operation is taken from the directory owner (D2); (3) the
	/// record is inserted, `Pending`, and the reader subscribed; (4) the
	/// installer is spawned with the plaintext, the operation and the capacity
	/// token (D5). `None`: nothing was handed off. A refused operation ends the
	/// attempt with no retry floor, and the read returns its row.
	pub(super) async fn hand_off_rehydration(
		&self,
		pointer: &TieredSlabPointer,
		admitted: AdmittedRehydration,
		plaintext: TieredPlaintext,
	) -> Option<watch::Receiver<InstallState>> {
		let slab_id = pointer.slab_id;
		let AdmittedRehydration { attempt, slot, .. } = admitted;
		let mut state = self.state.lock().await;
		if !state
			.rehydrating
			.get(&slab_id)
			.is_some_and(|admission| admission.attempt() == attempt)
		{
			// Superseded during the fetch: nothing to install.
			return None;
		}
		let Some(manager) = self.rehydration_installs.manager() else {
			Self::end_rehydration_attempt(&mut state, slab_id, attempt);
			return None;
		};
		let operation = match &self._directory_owner {
			None => None,
			Some(owner) => match owner.admitted_operation() {
				Ok(operation) => Some(operation),
				Err(error) => {
					tracing::debug!(
						slab_id = slab_id.get(),
						%error,
						"promotion handoff refused by the directory owner; the attempt ends"
					);
					Self::end_rehydration_attempt(&mut state, slab_id, attempt);
					return None;
				}
			},
		};
		let (outcome, receiver) = watch::channel(InstallState::Pending);
		self.rehydration_installs.lock().installers.insert(
			slab_id,
			InstallRecord {
				attempt,
				pointer: pointer.clone(),
				phase: InstallPhase::HandedOff,
				candidate: None,
				final_name_owned: false,
				outcome,
			},
		);
		state
			.rehydrating
			.insert(slab_id, RehydrationAdmission::Installing(attempt));
		tokio::spawn(manager.install_rehydrated_copy(InstallJob {
			slab_id,
			attempt,
			pointer: pointer.clone(),
			plaintext,
			operation,
			slot,
		}));
		drop(state);
		Some(receiver)
	}

	/// The installer task: the whole attempt inside its admitted operation,
	/// which completes after the outcome is published and the record removed;
	/// the capacity token goes last.
	async fn install_rehydrated_copy(self: Arc<Self>, job: InstallJob) {
		let InstallJob {
			slab_id,
			attempt,
			pointer,
			plaintext,
			operation,
			slot,
		} = job;
		let body = self.install_and_settle(slab_id, attempt, &pointer, plaintext);
		match operation {
			Some(operation) => operation.run(body).await,
			None => body.await,
		}
		drop(slot);
	}

	async fn install_and_settle(
		&self,
		slab_id: SlabId,
		attempt: RehydrationAttempt,
		pointer: &TieredSlabPointer,
		plaintext: TieredPlaintext,
	) {
		let settlement = match self
			.install_candidate(slab_id, attempt, pointer, plaintext)
			.await
		{
			Ok(copy) => match self.register_candidate(slab_id, attempt, copy).await {
				None => return,
				// Revoked or superseded: never registered, and its names go.
				Some(copy) => {
					drop(copy);
					self.dispose_owned_names(slab_id, attempt).await
				}
			},
			Err(InstallStop::NoAuthority(identity)) => {
				tracing::warn!(
					slab_id = slab_id.get(),
					"a rehydration temporary name holds a file this process did not create; it is left untouched"
				);
				Settlement::temp_present(InstallOutcome::NoAuthority, identity)
			}
			Err(InstallStop::IdentityUnknown(error, identity)) => {
				tracing::warn!(
					slab_id = slab_id.get(),
					%error,
					"a created rehydration temporary file has no readable identity; it is left in place"
				);
				Settlement::temp_present(InstallOutcome::IdentityUnknown, identity)
			}
			Err(InstallStop::Failed(error)) => {
				tracing::warn!(
					slab_id = slab_id.get(),
					%error,
					"rehydration failed; its names are removed and the slab keeps serving from L2"
				);
				self.dispose_owned_names(slab_id, attempt).await
			}
		};
		self.settle_rehydration(slab_id, attempt, settlement).await;
	}

	/// D3: the candidate copy, from a temporary name this attempt created to a
	/// verified store at the final name. Every name is recorded on the record
	/// before the step that could fail after it.
	async fn install_candidate(
		&self,
		slab_id: SlabId,
		attempt: RehydrationAttempt,
		pointer: &TieredSlabPointer,
		plaintext: TieredPlaintext,
	) -> Result<RehydratedCopy, InstallStop> {
		// The copy holds exactly the object's bytes under its layout.
		let layout = plaintext.view().map_err(InstallStop::Failed)?.layout();
		if self.quarantined_slab_files.contains(slab_id) {
			return Err(InstallStop::Failed(Error::new(
				ErrorKind::AlreadyExists,
				format!("slab {slab_id}'s path holds a quarantined file; it is never replaced"),
			)));
		}
		let (tmp_path, path) = rehydration_names(&self.config.dir, slab_id);
		let file_ops = self.file_ops.as_ref();
		let flags = OpenFlags {
			read: true,
			write: true,
			create_new: true,
			..Default::default()
		};
		let tmp = match file_ops.open(&tmp_path, flags).await {
			Ok(tmp) => tmp,
			Err(error) if error.kind() == ErrorKind::AlreadyExists => {
				let identity = file_ops.path_identity(&tmp_path).await.ok().flatten();
				return Err(InstallStop::NoAuthority(identity));
			}
			// The open reads the handle's identity; one that failed after creating
			// the name leaves a file this attempt cannot name by identity, so it is
			// reported and never removed by name alone.
			Err(error) => {
				return Err(match file_ops.path_identity(&tmp_path).await {
					Ok(Some(found)) => InstallStop::IdentityUnknown(error, Some(found)),
					_ => InstallStop::Failed(error),
				});
			}
		};
		let candidate = tmp.identity();
		self.rehydration_installs
			.update(slab_id, attempt, |record| {
				record.candidate = Some(candidate);
				record.phase = InstallPhase::Created;
			});
		let written = write_all_at(file_ops, &tmp, &plaintext.bytes).await;
		// D5: the plaintext goes as soon as its bytes are written.
		drop(plaintext);
		written.map_err(InstallStop::Failed)?;
		file_ops.fsync(&tmp).await.map_err(InstallStop::Failed)?;
		drop(tmp);
		self.rehydration_installs
			.update(slab_id, attempt, |record| {
				record.phase = InstallPhase::Written
			});
		// The copy is published only where no file exists: whatever is already
		// at the slab's path is not this attempt's to replace, nor to remove.
		file_ops
			.link_no_replace(&tmp_path, &path)
			.await
			.map_err(InstallStop::Failed)?;
		// From here the final name is this attempt's, before any further step.
		self.rehydration_installs
			.update(slab_id, attempt, |record| {
				record.final_name_owned = true;
				record.phase = InstallPhase::Linked { tmp_present: true };
			});
		match file_ops.path_identity(&path).await {
			Ok(Some(found)) if found == candidate => {}
			Ok(_) => {
				return Err(InstallStop::Failed(Error::new(
					ErrorKind::InvalidData,
					format!("slab {slab_id}: the linked name does not hold the candidate"),
				)));
			}
			Err(error) => return Err(InstallStop::Failed(error)),
		}
		remove_by_identity(file_ops, &tmp_path, candidate)
			.await
			.map_err(InstallStop::Failed)?;
		self.rehydration_installs
			.update(slab_id, attempt, |record| {
				record.phase = InstallPhase::Linked { tmp_present: false };
			});
		fsync_parent_dir(file_ops, &path)
			.await
			.map_err(InstallStop::Failed)?;
		let store = LocalSlabStore::open_verified(
			&path,
			slab_id,
			self.file_ops.clone(),
			self.config.group_commit.clone(),
			candidate,
		)
		.await
		.map_err(InstallStop::Failed)?;
		Ok(RehydratedCopy {
			store: Arc::new(store),
			layout,
			generation: crate::consensus::slab_ops::TierGeneration::from(pointer),
			keep_local_until: Instant::now() + self.config.rehydration.min_local_residency,
		})
	}

	/// D3: registers `copy` only if the record names this attempt, `rehydrating`
	/// holds `Installing(attempt)` and the slab's pointer is the record's. In
	/// one critical section the copy is installed, the admission ends and the
	/// outcome is published before the record goes. Otherwise the copy comes
	/// back for disposal.
	async fn register_candidate(
		&self,
		slab_id: SlabId,
		attempt: RehydrationAttempt,
		copy: RehydratedCopy,
	) -> Option<RehydratedCopy> {
		let mut state = self.state.lock().await;
		let admitted = matches!(
			state.rehydrating.get(&slab_id),
			Some(RehydrationAdmission::Installing(current)) if *current == attempt
		);
		let mut installs = self.rehydration_installs.lock();
		let current = admitted
			&& installs.installers.get(&slab_id).is_some_and(|record| {
				record.attempt == attempt
					&& state
						.tiered
						.get(&slab_id)
						.is_some_and(|published| published.value() == &record.pointer)
			});
		if !current {
			return Some(copy);
		}
		state.rehydrating.remove(&slab_id);
		state.hot_read_counts.remove(&slab_id);
		state.install_rehydrated(&self.tier_budget, &self.local_files, slab_id, copy);
		installs.settle(slab_id, attempt, InstallOutcome::Registered, None);
		None
	}

	/// D3: every name the record owns, removed only where it still holds the
	/// candidate; then the directory is synced. A name that cannot be removed
	/// after its retries settles `CleanupFailed` and inhibits promotion; a
	/// stuck final name outranks a stuck temporary one, whatever the
	/// temporary name's result.
	async fn dispose_owned_names(
		&self,
		slab_id: SlabId,
		attempt: RehydrationAttempt,
	) -> Settlement {
		let Some(owned) = self.rehydration_installs.owned_names(slab_id, attempt) else {
			return Settlement::removed();
		};
		let Some(candidate) = owned.candidate else {
			return Settlement::removed();
		};
		let (tmp_path, path) = rehydration_names(&self.config.dir, slab_id);
		let file_ops = self.file_ops.as_ref();
		let mut stuck = None;
		for (owns, name, at) in [
			(
				owned.temporary,
				RehydrationName::Temporary,
				tmp_path.as_path(),
			),
			(owned.final_name, RehydrationName::Final, path.as_path()),
		] {
			if !owns {
				continue;
			}
			if let Err(error) =
				with_cleanup_retries(move || remove_by_identity(file_ops, at, candidate)).await
			{
				tracing::warn!(
					slab_id = slab_id.get(),
					name = ?name,
					%error,
					"a rehydration name this attempt owns could not be removed"
				);
				stuck = Some(name);
			}
		}
		if owned.temporary || owned.final_name {
			let at = path.as_path();
			if let Err(error) = with_cleanup_retries(move || fsync_parent_dir(file_ops, at)).await {
				tracing::warn!(
					slab_id = slab_id.get(),
					%error,
					"the removal of a rehydration name is not durable; a crash may bring it back"
				);
			}
		}
		match stuck {
			None => Settlement::removed(),
			Some(name) => Settlement {
				outcome: InstallOutcome::CleanupFailed,
				inhibition: Some((
					RehydrationInhibitionReason::RehydrationCleanupFailed,
					name,
					Some(candidate),
				)),
			},
		}
	}

	/// The terminal step of an attempt that registered nothing. Its admission
	/// ends if it still owns it, with DC-304's retry floor unless promotion is
	/// now inhibited; then any inhibition and the outcome are published and
	/// the record removed together.
	async fn settle_rehydration(
		&self,
		slab_id: SlabId,
		attempt: RehydrationAttempt,
		settlement: Settlement,
	) {
		let mut state = self.state.lock().await;
		if Self::end_rehydration_attempt(&mut state, slab_id, attempt) {
			if settlement.inhibition.is_some() {
				state.hot_read_counts.remove(&slab_id);
			} else {
				self.set_rehydration_retry_floor(&mut state, slab_id);
			}
		}
		let inhibition =
			settlement
				.inhibition
				.map(|(reason, name, identity)| RehydrationInhibition {
					slab_id,
					reason,
					name,
					identity,
				});
		self.rehydration_installs
			.lock()
			.settle(slab_id, attempt, settlement.outcome, inhibition);
		drop(state);
	}

	/// Test-only read: whether an installer holds the slab's names.
	#[cfg(test)]
	pub(super) fn rehydration_installing_for_test(&self, slab_id: SlabId) -> bool {
		self.rehydration_installs.has_record(slab_id)
	}

	/// Test-only: a subscription to the outcome of the installer holding the
	/// slab's names, if one does.
	#[cfg(test)]
	pub(super) fn rehydration_outcome_for_test(
		&self,
		slab_id: SlabId,
	) -> Option<watch::Receiver<InstallState>> {
		self.rehydration_installs
			.lock()
			.installers
			.get(&slab_id)
			.map(|record| record.outcome.subscribe())
	}

	/// Test-only: admits one attempt for `pointer` as the threshold read would
	/// (with a capacity token), hands `plaintext` to its installer and waits for
	/// the outcome, for a test that installs a copy without reading up to it.
	#[cfg(test)]
	pub(super) async fn rehydrate_for_test(
		&self,
		pointer: &TieredSlabPointer,
		plaintext: TieredPlaintext,
	) -> Option<InstallOutcome> {
		let slot = self
			.rehydration_installs
			.try_acquire_slot()
			.expect("premise: a free promotion slot");
		let attempt = RehydrationAttempt::issue();
		let lease = Arc::new(());
		self.state.lock().await.rehydrating.insert(
			pointer.slab_id,
			RehydrationAdmission::Fetching {
				attempt,
				lease: Arc::downgrade(&lease),
			},
		);
		let admitted = AdmittedRehydration {
			attempt,
			_lease: lease,
			slot,
		};
		let outcome = self
			.hand_off_rehydration(pointer, admitted, plaintext)
			.await?;
		wait_settled(outcome).await
	}
}
