//! ADR 0132: the local copy files this process must dispose of, owned with the
//! data directory rather than with the manager.
//!
//! The ledger records every registered rehydrated copy, by slab and file
//! identity, kept in step with the manager's `rehydrated` by
//! `ManagerState::{install_rehydrated, take_rehydrated}` (D5), and every
//! released slab file not yet unlinked and synced (ADR 0131 D4), each tagged
//! with its origin. It holds the data directory's path and a weak reference to
//! its manager, for notification only: no directory owner, no backend and no
//! strong manager, so the owner that keeps it as a close settler never reaches
//! itself through it (D1). After a clean close's first drain the owner calls
//! its settle (D3): every copy is moved to the released files, then each copy
//! release is finished, one at a time, by the delivered release body, with a
//! transient backend the close supplies and a child of the settle's
//! operation. A released sealed backing keeps its runtime handling.
use super::*;
use crate::storage::manager::rehydration_install::{
	FileIdentity, ReleaseClaim, ReleaseOutcome, ReleaseRoute,
};
use crate::storage::manager::slab_file_quarantine::{ReleaseAdmission, ReleaseBackend};
use crate::storage::ownership::{AdmittedOperation, CloseSettler};
use tokio::sync::watch;

/// Where a released slab file came from (D5). Only a copy's release is the
/// close settle's to finish; a sealed backing's keeps its runtime handling.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ReleaseOrigin {
	/// A rehydrated copy: released by publication, retirement, eviction, GC or
	/// the close settle.
	Copy,
	/// A sealed slab file: released by publication, retirement, compaction or GC.
	Backing,
}

/// How a copy leaves `rehydrated` (D5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CopyDisposition {
	/// The caller records the copy's release, origin `Copy`.
	Release,
	/// The file stays as quarantine evidence: no release is recorded, and no
	/// registration is left to authorize an unlink of it.
	Evidence,
}

/// D5: the local backing a publication or retirement released, with its
/// origin: the sealed slab's file if there was one, otherwise the copy's.
pub(super) fn released_backing(
	sealed: Option<Arc<LocalSlabStore>>,
	copy: Option<Arc<LocalSlabStore>>,
) -> Option<(Arc<LocalSlabStore>, ReleaseOrigin)> {
	match (sealed, copy) {
		(Some(slab), _) => Some((slab, ReleaseOrigin::Backing)),
		(None, Some(copy)) => Some((copy, ReleaseOrigin::Copy)),
		(None, None) => None,
	}
}

/// ADR 0131 D4: one released slab file: the identity its backing read at open,
/// where it came from, and whether its unlink is owed or under way.
pub(super) struct ReleasedFile {
	pub(super) identity: FileIdentity,
	pub(super) origin: ReleaseOrigin,
	pub(super) phase: ReleasePhase,
}

pub(super) enum ReleasePhase {
	Owed,
	/// The file was unlinked; only its directory sync is still owed, and only
	/// that is ever retried: the path is never examined or unlinked again.
	SyncOwed,
	/// A settler owns the unlink; its outcome arrives here.
	Unlinking(watch::Receiver<Option<ReleaseOutcome>>),
	/// ADR 0132 D5: an owed copy release whose file a collision is about to
	/// publish as quarantine evidence. No claim takes it meanwhile; it goes
	/// with the evidence's publication, or returns to `Owed` if the collision
	/// fails or is cancelled first (`EvidenceHold`).
	Withheld,
}

/// The ledger's two tables, behind one mutex never held across an await.
#[derive(Default)]
pub(super) struct LedgerTables {
	/// D5: the registered copies, `slab -> identity`, in step with `rehydrated`.
	registered: HashMap<SlabId, FileIdentity>,
	/// ADR 0131 D4: the released files this process has not finished. An entry
	/// leaves only through its settler (or as owed, when GC finds its path
	/// changed before claiming it, or when the file becomes evidence), so the
	/// exclusion lasts through the unlink.
	pub(super) released: HashMap<SlabId, ReleasedFile>,
}

/// D1: the record of the copy files this process must dispose of.
pub(super) struct LocalFileLedger {
	dir: PathBuf,
	/// Notification only: the obligation is the ledger's, not the manager's.
	manager: std::sync::OnceLock<std::sync::Weak<SlabManager>>,
	tables: std::sync::Mutex<LedgerTables>,
}

impl LocalFileLedger {
	pub(super) fn new(dir: PathBuf) -> Arc<Self> {
		Arc::new(Self {
			dir,
			manager: std::sync::OnceLock::new(),
			tables: std::sync::Mutex::new(LedgerTables::default()),
		})
	}

	/// The data directory whose slab paths the ledger's files live at.
	pub(super) fn dir(&self) -> &Path {
		&self.dir
	}

	/// D1: binds the manager the ledger notifies at close and, for an owned
	/// manager, registers the ledger as its directory owner's close settler.
	/// The owner keeps the ledger, and so the obligation, until it closes,
	/// whether or not the manager is still alive then.
	pub(super) fn bind(self: &Arc<Self>, manager: &Arc<SlabManager>) -> std::io::Result<()> {
		let _ = self.manager.set(Arc::downgrade(manager));
		match &manager._directory_owner {
			Some(owner) => owner.register_close_settler(self.clone()),
			None => Ok(()),
		}
	}

	pub(super) fn lock(&self) -> std::sync::MutexGuard<'_, LedgerTables> {
		self.tables
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
	}

	/// D5: `slab_id`'s copy, of `identity`, is registered. Called only by
	/// `ManagerState::install_rehydrated`, under the manager's state lock.
	pub(super) fn register(&self, slab_id: SlabId, identity: FileIdentity) {
		self.lock().registered.insert(slab_id, identity);
	}

	/// D5: `slab_id`'s copy left `rehydrated`. Called only by
	/// `ManagerState::take_rehydrated`, under the manager's state lock; with
	/// `Release` the caller then records the copy's release.
	pub(super) fn unregister(&self, slab_id: SlabId, disposition: CopyDisposition) {
		let identity = self.lock().registered.remove(&slab_id);
		if disposition == CopyDisposition::Evidence {
			tracing::debug!(
				slab_id = slab_id.get(),
				identity = ?identity,
				"a registered copy became quarantine evidence; nothing may unlink it"
			);
		}
	}

	/// Owes the unlink of `slab_id`'s file, by `identity`. A release already
	/// recorded for the slab stays as it is.
	pub(super) fn owe(&self, slab_id: SlabId, identity: FileIdentity, origin: ReleaseOrigin) {
		self.lock().released.entry(slab_id).or_insert(ReleasedFile {
			identity,
			origin,
			phase: ReleasePhase::Owed,
		});
	}

	pub(super) fn contains(&self, slab_id: SlabId) -> bool {
		self.lock().released.contains_key(&slab_id)
	}

	/// Drops an owed release; one whose settler runs stays the settler's.
	pub(super) fn forget_owed(&self, slab_id: SlabId) {
		let mut tables = self.lock();
		if tables
			.released
			.get(&slab_id)
			.is_some_and(|file| matches!(file.phase, ReleasePhase::Owed))
		{
			tables.released.remove(&slab_id);
		}
	}

	/// Whether the slab's release waits only for its directory sync.
	pub(super) fn owes_sync(&self, slab_id: SlabId) -> bool {
		self.lock()
			.released
			.get(&slab_id)
			.is_some_and(|file| matches!(file.phase, ReleasePhase::SyncOwed))
	}

	/// Whether a release of the file `identity` names is still recorded for the
	/// slab, in any phase: the same slab and the same file, nothing narrower.
	pub(super) fn holds_release_of(&self, slab_id: SlabId, identity: FileIdentity) -> bool {
		self.lock()
			.released
			.get(&slab_id)
			.is_some_and(|file| file.identity == identity)
	}

	/// D5: the file `identity` names at `slab_id`'s path is about to become
	/// quarantine evidence. Checked under the manager's state lock and this
	/// mutex before the collision's first fallible step; nothing here is
	/// irreversible.
	/// - A release of that very file whose settler runs: `WouldBlock`, nothing
	///   recorded; the retry reads the state afresh.
	/// - An owed release of a sealed backing naming that very file: `WouldBlock`,
	///   nothing recorded. Its unlink authority stands, so the file is not
	///   published as evidence over it.
	/// - An owed copy release of it: held out of every claim (`Withheld`) by the
	///   returned hold, which withdraws it only together with the evidence's
	///   publication ([`EvidenceHold::publish`]); a failed or cancelled
	///   collision restores it, owed.
	/// - A release whose unlink is done (`SyncOwed`) names no file at the path
	///   any more and keeps its sync until that succeeds; every release of
	///   another file is left exactly as it is.
	pub(super) fn withhold_for_evidence(
		&self,
		slab_id: SlabId,
		identity: FileIdentity,
	) -> std::io::Result<Option<EvidenceHold<'_>>> {
		let mut tables = self.lock();
		let Some(file) = tables
			.released
			.get_mut(&slab_id)
			.filter(|file| file.identity == identity)
		else {
			return Ok(None);
		};
		if matches!(file.phase, ReleasePhase::SyncOwed) {
			return Ok(None);
		}
		if !matches!(file.phase, ReleasePhase::Owed) {
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"the release of the colliding file is in flight; retry once it settles",
			));
		}
		if file.origin == ReleaseOrigin::Backing {
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"an owed release of a sealed backing names the colliding file",
			));
		}
		file.phase = ReleasePhase::Withheld;
		Ok(Some(EvidenceHold {
			ledger: self,
			slab_id,
			identity,
			published: false,
		}))
	}

	/// D3 (2), the manager gone: every registered copy moves to the released
	/// files, origin `Copy`, by the identity it registered with.
	fn release_registered_copies(&self) {
		let mut tables = self.lock();
		let registered = std::mem::take(&mut tables.registered);
		for (slab_id, identity) in registered {
			tables.released.entry(slab_id).or_insert(ReleasedFile {
				identity,
				origin: ReleaseOrigin::Copy,
				phase: ReleasePhase::Owed,
			});
		}
	}

	/// D3 (4): the registered copies and the copy releases still recorded.
	fn copies_left(&self) -> (usize, usize) {
		let tables = self.lock();
		let released = tables
			.released
			.values()
			.filter(|file| file.origin == ReleaseOrigin::Copy)
			.count();
		(tables.registered.len(), released)
	}

	/// D3: the close settle, inside the settle operation `close` minted, with
	/// the transient owner-bound backend `close` created. The operation is
	/// completed whatever the result: a failed settle fails only this close
	/// attempt, and its repeat settles again.
	async fn settle_at_close(
		self: Arc<Self>,
		operation: AdmittedOperation,
		io: Arc<dyn AsyncFileOps>,
	) -> std::io::Result<()> {
		let result = self.settle_copies(&operation, io).await;
		operation.complete();
		result
	}

	async fn settle_copies(
		self: &Arc<Self>,
		operation: &AdmittedOperation,
		io: Arc<dyn AsyncFileOps>,
	) -> std::io::Result<()> {
		// (1, 2) A live manager unpublishes its copies under its state lock, so
		// new resolutions go to L2; otherwise the ledger's registrations stand
		// for them.
		let manager = self.manager.get().and_then(std::sync::Weak::upgrade);
		match manager {
			Some(manager) => manager.release_rehydrated_copies_for_close().await?,
			None => self.release_registered_copies(),
		}
		// (3) Each copy release, one at a time, through the release body.
		let mut owed = self
			.lock()
			.released
			.iter()
			.filter(|(_, file)| file.origin == ReleaseOrigin::Copy)
			.map(|(slab_id, file)| (*slab_id, file.identity))
			.collect::<Vec<_>>();
		owed.sort_unstable();
		let mut first_error = None;
		for (slab_id, identity) in owed {
			let backend = || Ok(ReleaseBackend::Transient(io.clone()));
			let claimed = self.claim_released_slab_file(
				slab_id,
				ReleaseRoute::Release,
				Some(identity),
				ReleaseAdmission::Child(operation),
				backend,
			);
			let settled = match claimed {
				Ok(ReleaseClaim::Absent) => Ok(()),
				Ok(ReleaseClaim::Claimed(settled) | ReleaseClaim::InProgress(settled)) => {
					rehydration_install::wait_release(settled).await
				}
				Err(error) => Err(error),
			};
			if let Err(error) = settled {
				tracing::warn!(
					slab_id = slab_id.get(),
					%error,
					"a local copy could not be disposed of at close; the close stays incomplete"
				);
				first_error.get_or_insert(error);
			}
		}
		// (4) Settled only when no copy is registered or owed.
		if let Some(error) = first_error {
			return Err(error);
		}
		match self.copies_left() {
			(0, 0) => Ok(()),
			(registered, released) => Err(Error::new(
				ErrorKind::WouldBlock,
				format!(
					"{registered} registered and {released} released local copies are left at close"
				),
			)),
		}
	}

	/// Test-only read: the identity of the slab's release, if it has one.
	#[cfg(test)]
	pub(super) fn identity_for_test(&self, slab_id: SlabId) -> Option<FileIdentity> {
		self.lock().released.get(&slab_id).map(|file| file.identity)
	}

	/// Test-only read: the phase of the slab's release, if it has one.
	#[cfg(test)]
	pub(super) fn phase_for_test(&self, slab_id: SlabId) -> Option<&'static str> {
		self.lock()
			.released
			.get(&slab_id)
			.map(|file| match file.phase {
				ReleasePhase::Owed => "owed",
				ReleasePhase::SyncOwed => "sync_owed",
				ReleasePhase::Unlinking(_) => "unlinking",
				ReleasePhase::Withheld => "withheld",
			})
	}

	/// Test-only read: the origin of the slab's release, if it has one.
	#[cfg(test)]
	pub(super) fn origin_for_test(&self, slab_id: SlabId) -> Option<ReleaseOrigin> {
		self.lock().released.get(&slab_id).map(|file| file.origin)
	}

	/// Test-only read: the identity the slab's copy registered with, if any.
	#[cfg(test)]
	pub(super) fn registered_for_test(&self, slab_id: SlabId) -> Option<FileIdentity> {
		self.lock().registered.get(&slab_id).copied()
	}
}

/// D5: an owed copy release held out of every claim while a collision
/// prepares to publish its file as quarantine evidence. Published, it goes
/// with the evidence; dropped unpublished (an error or a cancellation of the
/// collision), it is owed again, exactly as before.
pub(super) struct EvidenceHold<'a> {
	ledger: &'a LocalFileLedger,
	slab_id: SlabId,
	identity: FileIdentity,
	published: bool,
}

impl EvidenceHold<'_> {
	/// Withdraws the held release. Called only in the critical section that
	/// publishes the evidence, with no await before the publication.
	pub(super) fn publish(mut self) {
		self.published = true;
		let mut tables = self.ledger.lock();
		if tables.released.get(&self.slab_id).is_some_and(|file| {
			file.identity == self.identity && matches!(file.phase, ReleasePhase::Withheld)
		}) {
			tables.released.remove(&self.slab_id);
			tracing::warn!(
				slab_id = self.slab_id.get(),
				"an owed copy release is withdrawn: its file is kept as quarantine evidence"
			);
		}
	}
}

impl Drop for EvidenceHold<'_> {
	fn drop(&mut self) {
		if self.published {
			return;
		}
		let mut tables = self.ledger.lock();
		if let Some(file) = tables.released.get_mut(&self.slab_id)
			&& file.identity == self.identity
			&& matches!(file.phase, ReleasePhase::Withheld)
		{
			file.phase = ReleasePhase::Owed;
		}
	}
}

impl CloseSettler for LocalFileLedger {
	fn settle(
		self: Arc<Self>,
		operation: AdmittedOperation,
		io: Arc<dyn AsyncFileOps>,
	) -> std::pin::Pin<Box<dyn std::future::Future<Output = std::io::Result<()>> + Send>> {
		Box::pin(self.settle_at_close(operation, io))
	}
}

impl SlabManager {
	/// D3 (1, 2), the manager alive at close: no installer may remain after
	/// the close's first drain, and every copy leaves `rehydrated` under the
	/// state lock with its release recorded, origin `Copy`, for the ledger's
	/// settle to finish.
	async fn release_rehydrated_copies_for_close(&self) -> std::io::Result<()> {
		let state = self.state.lock().await;
		if self.rehydration_installs.holds_any_record() {
			return Err(Error::other(
				"a rehydration installer outlived the close's drain",
			));
		}
		let slabs = state
			.rehydrated
			.iter()
			.map(|entry| *entry.key())
			.collect::<Vec<_>>();
		for slab_id in slabs {
			if let Some(copy) =
				state.take_rehydrated(&self.local_files, slab_id, CopyDisposition::Release)
			{
				let identity = copy.store.cached_file_identity();
				self.release_slab_file(slab_id, identity, ReleaseOrigin::Copy);
			}
		}
		drop(state);
		Ok(())
	}
}
