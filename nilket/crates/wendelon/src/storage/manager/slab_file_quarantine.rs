//! Local slab files that disagree with the committed tier state, and the
//! ownership rule for every unlink of a slab path.
//!
//! Open classifies a discovered `slab_{id}.wnd` whose id has a committed tier
//! pointer as unverified evidence: a rehydrated cache copy, a leftover of a
//! crash between the first tier commit and its local unlink, or a file nothing
//! classifies. It is never opened, indexed, served, renamed over or unlinked;
//! the slab serves only through its tier generation. The set is fixed before
//! readiness is first exposed, and reporting it never authorizes cleanup.
use super::*;
use crate::storage::manager::local_file_ledger::{
	CopyDisposition, LocalFileLedger, ReleaseOrigin, ReleasePhase,
};
use crate::storage::manager::rehydration_install::{ReleaseClaim, ReleaseRoute};
use crate::storage::ownership::AdmittedOperation;
use std::sync::atomic::AtomicU64;

/// Why open classified a local slab file as disagreeing with the committed
/// state. The label set is stable and bounded.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DisagreeingSlabFileReason {
	/// The slab id has a committed tier pointer.
	TierPointer,
	/// The pointer was removed by a durable, authority-backed retirement.
	RetiredTier,
}

impl DisagreeingSlabFileReason {
	pub const fn label(self) -> &'static str {
		match self {
			Self::TierPointer => "tier_pointer",
			Self::RetiredTier => "retired_tier",
		}
	}
}

/// One local slab file held as evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QuarantinedSlabFile {
	pub slab_id: SlabId,
	/// The file's length when open classified it.
	pub bytes: u64,
	pub reason: DisagreeingSlabFileReason,
}

/// Diagnostic view of the files this manager holds as evidence.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QuarantinedSlabFilesReport {
	/// Sorted by slab id.
	pub files: Vec<QuarantinedSlabFile>,
	/// Rehydrations refused because the slab's path holds a quarantined file.
	pub rehydrations_skipped: u64,
}

/// The quarantined files of one manager. Its contribution to the process
/// gauges is added at classification and removed when the manager drops.
#[derive(Default)]
pub(super) struct SlabFileQuarantine {
	files: std::sync::RwLock<BTreeMap<SlabId, QuarantinedSlabFile>>,
	rehydrations_skipped: AtomicU64,
}

impl SlabFileQuarantine {
	fn new(files: Vec<QuarantinedSlabFile>) -> Self {
		let files = files
			.into_iter()
			.map(|file| (file.slab_id, file))
			.collect::<BTreeMap<_, _>>();
		for file in files.values() {
			metrics().observe_storage_disagreeing_slab_file(file.reason.label(), file.bytes);
		}
		Self {
			files: std::sync::RwLock::new(files),
			rehydrations_skipped: AtomicU64::new(0),
		}
	}

	/// Only bootstrap may extend this set, while publication and state are locked.
	pub(super) fn insert_bootstrap(&self, file: QuarantinedSlabFile) {
		let mut files = self.files.write().unwrap_or_else(|e| e.into_inner());
		if let std::collections::btree_map::Entry::Vacant(entry) = files.entry(file.slab_id) {
			metrics().observe_storage_disagreeing_slab_file(file.reason.label(), file.bytes);
			entry.insert(file);
		}
	}

	pub(super) fn contains(&self, slab_id: SlabId) -> bool {
		self.files
			.read()
			.unwrap_or_else(|e| e.into_inner())
			.contains_key(&slab_id)
	}

	pub(super) fn note_rehydration_skipped(&self) {
		self.rehydrations_skipped.fetch_add(1, Ordering::Relaxed);
		metrics().observe_storage_rehydration_skipped("quarantined_file");
	}

	fn report(&self) -> QuarantinedSlabFilesReport {
		QuarantinedSlabFilesReport {
			files: self
				.files
				.read()
				.unwrap_or_else(|e| e.into_inner())
				.values()
				.copied()
				.collect(),
			rehydrations_skipped: self.rehydrations_skipped.load(Ordering::Relaxed),
		}
	}
}

impl Drop for SlabFileQuarantine {
	fn drop(&mut self) {
		let files = self.files.get_mut().unwrap_or_else(|e| e.into_inner());
		let bytes = files.values().map(|file| file.bytes).sum();
		metrics().release_storage_quarantined_slab_files(files.len() as u64, bytes);
	}
}

/// Split the discovered slab files at open: an id with a committed tier pointer
/// or explicit retirement evidence is quarantined. Other ids recover from their bytes. Absence
/// from redb is never a reason to quarantine.
pub(super) async fn classify_discovered_slab_files(
	file_ops: &dyn AsyncFileOps,
	dir: &Path,
	discovered: Vec<SlabId>,
	tier_pointer_ids: &BTreeSet<SlabId>,
	retired_tier_ids: &BTreeSet<SlabId>,
) -> std::io::Result<(Vec<SlabId>, SlabFileQuarantine)> {
	let mut local = Vec::with_capacity(discovered.len());
	let mut quarantined = Vec::new();
	for id in discovered {
		if !tier_pointer_ids.contains(&id) && !retired_tier_ids.contains(&id) {
			local.push(id);
			continue;
		}
		let path = slab_path(dir, id);
		let bytes = file_ops.file_len(&path).await?.ok_or_else(|| {
			Error::new(
				ErrorKind::NotFound,
				format!("slab disappeared during recovery: {}", path.display()),
			)
		})?;
		let reason = if tier_pointer_ids.contains(&id) {
			DisagreeingSlabFileReason::TierPointer
		} else {
			DisagreeingSlabFileReason::RetiredTier
		};
		tracing::warn!(
			slab_id = id.get(),
			bytes,
			reason = reason.label(),
			"a local slab file disagrees with the committed tier state; it is kept as evidence and never served"
		);
		quarantined.push(QuarantinedSlabFile {
			slab_id: id,
			bytes,
			reason,
		});
	}
	Ok((local, SlabFileQuarantine::new(quarantined)))
}

impl SlabManager {
	/// The local slab files this manager holds as evidence because they
	/// disagree with the committed tier state. Nothing here authorizes cleanup.
	pub fn quarantined_slab_files(&self) -> QuarantinedSlabFilesReport {
		self.quarantined_slab_files.report()
	}

	/// Record that the caller's operation removed `slab_id`'s local backing (a
	/// sealed slab or a rehydrated copy) from `state`, with the identity that
	/// backing read at its open (ADR 0131 D4). The caller holds the `state`
	/// guard, so the release is atomic with the removal and needs no syscall;
	/// the file is then this process's to unlink, by that identity, through
	/// [`Self::unlink_released_slab_file`], until its settler finishes. Every
	/// store has its identity (its open fails without one), so every release
	/// of a backing is recorded, with its origin (ADR 0132 D5): a copy's is the
	/// close settle's to finish if no runtime release did.
	pub(super) fn release_slab_file(
		&self,
		slab_id: SlabId,
		identity: (u64, u64),
		origin: ReleaseOrigin,
	) {
		if !self.quarantined_slab_files.contains(slab_id) {
			self.local_files.owe(slab_id, identity, origin);
		}
	}

	/// GC releases only the file identified by the open slab handle. The
	/// caller holds X's tier lock and maintenance gate through this operation;
	/// the directory owner and allocator prevent another managed installer from
	/// replacing the checked path between the identity check and unlink, and a
	/// rehydration installer's names are refused while it holds them (ADR 0131).
	pub(super) async fn gc_unlink_owned_slab_file(&self, slab_id: SlabId) -> std::io::Result<()> {
		use std::os::unix::fs::MetadataExt;
		let path = slab_path(&self.config.dir, slab_id);
		if self.quarantined_slab_files.contains(slab_id) {
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"quarantined slab cannot be released by GC",
			));
		}
		let expected = {
			let mut state = self.state.lock().await;
			// ADR 0131 D1, D5: an installer owns the slab's names until it
			// settles, and a final name its cleanup could not remove stays out
			// of GC's reach. Records are inserted only under this lock.
			if self.rehydration_installs.refuses_gc(slab_id) {
				return Err(Error::new(
					ErrorKind::WouldBlock,
					"a rehydration installer holds or left the slab's names",
				));
			}
			let sealed = state.sealed.get(&slab_id).map(|slab| slab.value().clone());
			let rehydrated = state
				.rehydrated
				.get(&slab_id)
				.map(|entry| entry.store.clone());
			let expected = match (sealed.as_ref(), rehydrated.as_ref()) {
				(Some(a), Some(b)) => {
					let identity = a.opened_file_identity()?;
					if identity != b.opened_file_identity()? {
						return Err(Error::new(
							ErrorKind::InvalidData,
							"GC slab handles disagree",
						));
					}
					Some(identity)
				}
				(Some(slab), None) | (None, Some(slab)) => Some(slab.opened_file_identity()?),
				(None, None) => match std::fs::symlink_metadata(&path) {
					Err(error) if error.kind() == ErrorKind::NotFound => None,
					Err(error) => return Err(error),
					Ok(_) => {
						return Err(Error::new(
							ErrorKind::InvalidData,
							"GC found a slab path with no owned backing",
						));
					}
				},
			};
			if let Some(expected) = expected {
				let metadata = std::fs::symlink_metadata(&path)?;
				if !metadata.file_type().is_file() || (metadata.dev(), metadata.ino()) != expected {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"GC slab path identity changed",
					));
				}
				state.sealed.remove(&slab_id);
				state.take_rehydrated(&self.local_files, slab_id, CopyDisposition::Release);
				let origin = match sealed {
					Some(_) => ReleaseOrigin::Backing,
					None => ReleaseOrigin::Copy,
				};
				self.release_slab_file(slab_id, expected, origin);
			}
			state.hot_read_counts.remove(&slab_id);
			state.rehydrating.remove(&slab_id);
			expected
		};
		if let Some(expected) = expected {
			let matches = std::fs::symlink_metadata(&path).is_ok_and(|metadata| {
				metadata.file_type().is_file() && (metadata.dev(), metadata.ino()) == expected
			});
			if !matches {
				self.local_files.forget_owed(slab_id);
				return Err(Error::new(
					ErrorKind::InvalidData,
					"GC slab path changed before unlink",
				));
			}
			#[cfg(test)]
			pass_test_hold(&self.continuity_test_holds.gc_before_unlink_hold).await;
			// ADR 0131 D4: the unlink is the release settler's, which keeps the
			// entry `Unlinking` until it finishes. GC owns the release only if it
			// is still owed; the settler keeps GC's failure propagation, its
			// directory sync and its test holds.
			let settled = match self.claim_runtime_release(slab_id, ReleaseRoute::Gc, None)? {
				ReleaseClaim::Claimed(settled) => settled,
				ReleaseClaim::Absent | ReleaseClaim::InProgress(_) => {
					return Err(Error::new(
						ErrorKind::WouldBlock,
						"GC slab release was not owned",
					));
				}
			};
			rehydration_install::wait_release(settled).await?;
		} else if self.local_files.owes_sync(slab_id) {
			// An earlier unlink's directory sync is still owed: only it is retried.
			match self.claim_runtime_release(slab_id, ReleaseRoute::Gc, None)? {
				ReleaseClaim::Absent => {}
				ReleaseClaim::Claimed(settled) | ReleaseClaim::InProgress(settled) => {
					rehydration_install::wait_release(settled).await?;
				}
			}
		}
		Ok(())
	}

	/// Unlink `slab_id`'s local file only when this process released it and no
	/// unlink has completed since, and only while its path still names the
	/// file the release recorded (ADR 0131 D4). The unlink is the release
	/// settler's: this call claims an owed release for it, or waits for the
	/// settler that already owns it. A failed unlink returns the release to
	/// owed, so a retry finishes it. A slab id or a path is never authority on
	/// its own, and a quarantined file is never released.
	///
	/// The release carries the file's identity (device and inode) as its
	/// backing read it at open, so a file that appeared at the path since is
	/// left alone. The id-keyed conditions still hold as defence in depth:
	/// 1. the allocator only advances (`ManagerState::advance_slab_allocator`),
	///    so a released id is never allocated again;
	/// 2. every installer of a slab path uses a freshly allocated id or links
	///    without replacing (rehydration's `link_no_replace`); the call sites
	///    are pinned by an architecture census;
	/// 3. one manager per data directory: the only production constructor,
	///    `open_with_owner`, holds the directory's exclusive owner lock.
	pub(super) async fn unlink_released_slab_file(&self, slab_id: SlabId) -> std::io::Result<()> {
		// ADR 0131 D1: an installer's names are its own until it settles.
		if self.local_files.contains(slab_id) && self.rehydration_installs.has_record(slab_id) {
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"a rehydration installer holds the slab's names",
			));
		}
		match self.claim_runtime_release(slab_id, ReleaseRoute::Release, None)? {
			ReleaseClaim::Absent => Ok(()),
			ReleaseClaim::Claimed(settled) | ReleaseClaim::InProgress(settled) => {
				rehydration_install::wait_release(settled).await
			}
		}
	}

	/// ADR 0131 D4: settles the release of the file `identity` names for
	/// `slab_id`, through its one owner, and answers only with that owner's
	/// terminal result. A matching release is claimed for a settler (which, once
	/// the unlink is done, only syncs), or, if a settler already owns it, joined;
	/// either way no other release can settle it meanwhile. `Ok` means no matching
	/// release is left; a restored release (owed or `SyncOwed`) or a settler's
	/// error is returned. A cancelled caller leaves the release to its settler.
	pub(super) async fn settle_release_of(
		&self,
		slab_id: SlabId,
		identity: (u64, u64),
	) -> std::io::Result<()> {
		let settled =
			match self.claim_runtime_release(slab_id, ReleaseRoute::Release, Some(identity))? {
				ReleaseClaim::Absent => return Ok(()),
				ReleaseClaim::Claimed(settled) | ReleaseClaim::InProgress(settled) => settled,
			};
		rehydration_install::wait_release(settled).await?;
		if self.local_files.holds_release_of(slab_id, identity) {
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"the release of this slab file is still owed",
			));
		}
		Ok(())
	}

	/// ADR 0131 D4: claims `slab_id`'s release for a runtime settler, admitted
	/// by a new admitted operation of the manager's directory owner and run
	/// against this manager's file operations (ADR 0132 D2).
	fn claim_runtime_release(
		&self,
		slab_id: SlabId,
		route: ReleaseRoute,
		of: Option<(u64, u64)>,
	) -> std::io::Result<ReleaseClaim> {
		let backend = || {
			self.rehydration_installs
				.manager()
				.map(ReleaseBackend::Manager)
				.ok_or_else(|| Error::other("the manager is not bound to its release settler"))
		};
		self.local_files.claim_released_slab_file(
			slab_id,
			route,
			of,
			ReleaseAdmission::Owner(self._directory_owner.as_ref()),
			backend,
		)
	}
}

/// ADR 0132 D2: where a release settler's admitted operation comes from.
pub(super) enum ReleaseAdmission<'a> {
	/// At runtime: a new admitted operation of the directory owner, if the
	/// manager has one; refused once its close began.
	Owner(Option<&'a DataDirectoryOwner>),
	/// At close: a child of the settle operation `close` minted.
	Child(&'a AdmittedOperation),
}

/// ADR 0132 D2: the backend a release settler runs its I/O through; the
/// release body is the same either way.
pub(super) enum ReleaseBackend {
	/// At runtime: the manager's owner-bound file operations. The settler task
	/// keeps the manager alive until it ends.
	Manager(Arc<SlabManager>),
	/// At close: the transient owner-bound backend `close` created, whose
	/// mutations carry their child write permits into their blocking jobs.
	Transient(Arc<dyn AsyncFileOps>),
}

impl ReleaseBackend {
	fn file_ops(&self) -> &dyn AsyncFileOps {
		match self {
			Self::Manager(manager) => manager.file_ops.as_ref(),
			Self::Transient(file_ops) => file_ops.as_ref(),
		}
	}

	/// The manager's GC test holds; a close-time release has none.
	#[cfg(test)]
	fn test_holds(&self) -> Option<&pause_hooks::TestHolds> {
		match self {
			Self::Manager(manager) => Some(&manager.continuity_test_holds),
			Self::Transient(_) => None,
		}
	}
}

impl LocalFileLedger {
	/// ADR 0131 D4: claims `slab_id`'s release for a settler. An owed release
	/// moves to `Unlinking` and its settler is spawned, holding the admitted
	/// operation `admission` supplies (ADR 0132 D2), with no await between: the
	/// entry then leaves `Unlinking` only through that settler, never through a
	/// waiter's drop. A refused operation leaves the release owed. With `of`,
	/// only a release of that very file is claimed or joined; another is absent.
	pub(super) fn claim_released_slab_file(
		self: &Arc<Self>,
		slab_id: SlabId,
		route: ReleaseRoute,
		of: Option<(u64, u64)>,
		admission: ReleaseAdmission<'_>,
		backend: impl FnOnce() -> std::io::Result<ReleaseBackend>,
	) -> std::io::Result<ReleaseClaim> {
		let mut tables = self.lock();
		let Some(file) = tables
			.released
			.get_mut(&slab_id)
			.filter(|file| of.is_none_or(|identity| file.identity == identity))
		else {
			return Ok(ReleaseClaim::Absent);
		};
		let sync_only = match &file.phase {
			ReleasePhase::Unlinking(settled) => {
				return Ok(ReleaseClaim::InProgress(settled.clone()));
			}
			// ADR 0132 D5: a collision is publishing this file as evidence; the
			// release goes with it, or is owed again if the collision fails.
			ReleasePhase::Withheld => {
				return Err(Error::new(
					ErrorKind::WouldBlock,
					"the released file is becoming quarantine evidence",
				));
			}
			ReleasePhase::Owed => false,
			ReleasePhase::SyncOwed => true,
		};
		let backend = backend()?;
		let operation = match admission {
			ReleaseAdmission::Owner(owner) => owner
				.map(DataDirectoryOwner::admitted_operation)
				.transpose()?,
			ReleaseAdmission::Child(parent) => Some(parent.child_operation()),
		};
		let (settler, settled) = tokio::sync::watch::channel(None);
		file.phase = ReleasePhase::Unlinking(settled.clone());
		let identity = file.identity;
		drop(tables);
		let task = settle_released_slab_file(
			self.clone(),
			backend,
			slab_id,
			identity,
			route,
			sync_only,
			operation,
			settler,
		);
		tokio::spawn(task);
		Ok(ReleaseClaim::Claimed(settled))
	}
}

/// ADR 0131 D4: the release settler. Inside its admitted operation it unlinks
/// the released file only while the path still names it, then syncs the
/// directory; a `sync_only` settler (the unlink already done) only syncs. Then,
/// in one critical section, it settles the entry and publishes the outcome:
/// the entry goes only when the release is done; a failed unlink leaves it
/// owed, and a failed sync after an unlink leaves it `SyncOwed`. A mismatch or
/// a missing name ends the release with a warning: the settler has no
/// authority over another file. The body is the same at runtime and at close;
/// only its backend and its operation's parent differ (ADR 0132 D2).
async fn settle_released_slab_file(
	ledger: Arc<LocalFileLedger>,
	backend: ReleaseBackend,
	slab_id: SlabId,
	identity: (u64, u64),
	route: ReleaseRoute,
	sync_only: bool,
	operation: Option<AdmittedOperation>,
	settler: tokio::sync::watch::Sender<Option<rehydration_install::ReleaseOutcome>>,
) {
	let body = async {
		let path = slab_path(ledger.dir(), slab_id);
		let end = unlink_released_by_identity(&backend, &path, identity, route, sync_only).await;
		let mut tables = ledger.lock();
		let (phase, result) = match end {
			ReleaseEnd::Done(result) => (None, result),
			ReleaseEnd::Owed(error) => (Some(ReleasePhase::Owed), Err(error)),
			ReleaseEnd::SyncOwed(error) => (Some(ReleasePhase::SyncOwed), Err(error)),
		};
		match phase {
			Some(phase) => {
				if let Some(file) = tables.released.get_mut(&slab_id) {
					file.phase = phase;
				}
			}
			None => {
				tables.released.remove(&slab_id);
			}
		}
		settler.send_replace(Some(rehydration_install::release_outcome(&result)));
	};
	match operation {
		Some(operation) => operation.run(body).await,
		None => body.await,
	}
}

/// The settler's step and where it leaves the release. A `stat` or an unlink
/// that fails is tried afresh up to three times (ADR 0131 D5) before the
/// release is owed again. After an unlink, only the directory sync is retried,
/// on the same schedule, and never the unlink: a later occupant of the path is
/// never touched.
async fn unlink_released_by_identity(
	backend: &ReleaseBackend,
	path: &Path,
	identity: (u64, u64),
	route: ReleaseRoute,
	sync_only: bool,
) -> ReleaseEnd {
	let file_ops = backend.file_ops();
	if !sync_only {
		let mut delays = rehydration_install::CLEANUP_RETRY_DELAYS.into_iter();
		let found = loop {
			match unlink_if_released_file(file_ops, path, identity).await {
				Ok(found) => break found,
				// The file may still be there, so the release stays owed.
				Err(error) => match delays.next() {
					Some(delay) => tokio::time::sleep(delay).await,
					None => return ReleaseEnd::Owed(error),
				},
			}
		};
		match found {
			ReleasedPath::Unlinked => {}
			// The file is gone: nothing can have been installed in its place.
			ReleasedPath::Missing if route == ReleaseRoute::Release => {
				return ReleaseEnd::Done(Ok(()));
			}
			ReleasedPath::Missing => {
				return ReleaseEnd::Done(Err(Error::new(
					ErrorKind::NotFound,
					"GC slab file disappeared before unlink",
				)));
			}
			ReleasedPath::Other => {
				tracing::warn!(
					path = %path.display(),
					"a released slab path no longer names the released file; it is left alone"
				);
				return ReleaseEnd::Done(match route {
					ReleaseRoute::Release => Ok(()),
					ReleaseRoute::Gc => Err(Error::new(
						ErrorKind::InvalidData,
						"GC slab path changed before unlink",
					)),
				});
			}
		}
		#[cfg(test)]
		{
			if route == ReleaseRoute::Gc
				&& let Some(holds) = backend.test_holds()
			{
				pass_test_hold(&holds.gc_after_unlink_hold).await;
			}
		}
	}
	let synced =
		rehydration_install::with_cleanup_retries(move || fsync_parent_dir(file_ops, path)).await;
	match synced {
		// The unlink stands; its sync stays owed, so a later release retries it.
		Err(error) => ReleaseEnd::SyncOwed(error),
		Ok(()) => {
			#[cfg(test)]
			{
				if route == ReleaseRoute::Gc
					&& let Some(holds) = backend.test_holds()
				{
					pass_test_hold(&holds.gc_after_fsync_hold).await;
				}
			}
			ReleaseEnd::Done(Ok(()))
		}
	}
}

/// One try of the settler's step: `stat` the path, and unlink it only if it
/// still names the released file.
async fn unlink_if_released_file(
	file_ops: &dyn AsyncFileOps,
	path: &Path,
	identity: (u64, u64),
) -> std::io::Result<ReleasedPath> {
	match file_ops.path_identity(path).await? {
		None => Ok(ReleasedPath::Missing),
		Some(found) if found != identity => Ok(ReleasedPath::Other),
		Some(_) => match file_ops.unlink(path).await {
			Ok(()) => Ok(ReleasedPath::Unlinked),
			Err(error) if error.kind() == ErrorKind::NotFound => Ok(ReleasedPath::Missing),
			Err(error) => Err(error),
		},
	}
}

/// Where a settler leaves a release, with the result its route reports.
enum ReleaseEnd {
	/// The release is finished; its entry goes.
	Done(std::io::Result<()>),
	/// The file was not unlinked: the release is owed again.
	Owed(Error),
	/// The file was unlinked and its directory sync failed: only the sync is owed.
	SyncOwed(Error),
}

/// What one try of the release settler found at the released path.
enum ReleasedPath {
	Unlinked,
	Missing,
	/// Another file: the settler has no authority over it.
	Other,
}

/// A GC test hold: signal `entered`, then wait for `release`.
#[cfg(test)]
async fn pass_test_hold(
	hold: &std::sync::Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
) {
	let hold = hold.lock().unwrap().clone();
	if let Some((entered, release)) = hold {
		entered.add_permits(1);
		let _ = release.acquire().await;
	}
}
