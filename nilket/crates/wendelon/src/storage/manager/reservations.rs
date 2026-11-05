//! ADR 0697 §4 T2: reserved slab ids, their capability, and how a dead job's ids
//! reach a terminal state. A row in `Unsubmitted` is the only positive evidence an
//! id was never exposed; anything else takes T1, and nothing revives a burned id.

use super::gc_disposal::{
	gc_quarantine_path, gc_require_quarantine_content, gc_require_quarantine_inode,
};
use super::*;
use crate::storage::index::{
	AbandonedOutput, GcFileIdentity, LoadedOp, MAX_RESERVED_OUTPUTS_PER_JOB, OpProgress,
	OpProgressUpdate, OpTicket, ReservationRefused, ReservedIdState, ReservedOp,
};
use crate::storage::sweeper::VerifiedReceipt;

/// A reserved id a job owns. In-process liveness only: dropped, the row is the
/// driver's. Not `Clone`, so ownership is one job's.
#[derive(Debug)]
pub struct ReservedSlab {
	id: SlabId,
	live: Option<Arc<DashSet<SlabId>>>,
}

impl ReservedSlab {
	pub fn id(&self) -> SlabId {
		self.id
	}
}

impl Drop for ReservedSlab {
	fn drop(&mut self) {
		if let Some(live) = &self.live {
			live.remove(&self.id);
		}
	}
}

/// The capability to name a reserved id in any staging record, file or proposal.
/// Its constructor is private: skipping exposure does not compile.
#[derive(Debug)]
pub struct ExposedSlab {
	reserved: ReservedSlab,
}

impl ExposedSlab {
	pub fn id(&self) -> SlabId {
		self.reserved.id
	}

	/// Whether this id has a durable reservation its publication must end.
	pub(super) fn has_reservation_row(&self) -> bool {
		self.reserved.live.is_some()
	}

	/// A capability on no store, for a plan with no ring behind it.
	#[cfg(test)]
	pub(crate) fn unbound_for_test(id: SlabId) -> Self {
		Self {
			reserved: ReservedSlab { id, live: None },
		}
	}
}

impl PartialEq for ExposedSlab {
	fn eq(&self, other: &Self) -> bool {
		self.id() == other.id()
	}
}

impl Eq for ExposedSlab {}

/// A job too large to ever fit the window with its own charges.
#[derive(Debug)]
pub struct OversizedReservation {
	pub requested: u64,
	pub limit: u64,
}

impl std::fmt::Display for OversizedReservation {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(
			formatter,
			"a job needs {} output slabs and at most {} can be reserved together",
			self.requested, self.limit
		)
	}
}

impl std::error::Error for OversizedReservation {}

impl ManagerState {
	/// Take a range before the commit that covers and reserves it, which may land
	/// with its answer lost. Nothing has been issued from it yet: its caller gets an
	/// id only once that commit is known, and any later allocation raises the durable
	/// bound past this point before using an id above it.
	fn take_slab_range_before_its_commit(&mut self, next: SlabId) -> std::io::Result<()> {
		self.reservation_in_flight_bound = self.reservation_in_flight_bound.max(next);
		self.advance_slab_allocator(next)
	}
}

impl SlabManager {
	/// Reserve `count` ids for one job. In a ring each has a durable, charged
	/// `Unsubmitted` row before it reaches the caller. The liveness guard exists
	/// before the row does, so the burn scanner can never take a new reservation.
	pub(super) async fn reserve_slab_ids(
		&self,
		count: usize,
	) -> std::io::Result<Vec<ReservedSlab>> {
		let requested = u64::try_from(count).map_err(|_| {
			Error::new(
				ErrorKind::InvalidInput,
				"LSM output slab reservation count does not fit in u64",
			)
		})?;
		let bound = self.control_node_bound();
		if bound && requested > MAX_RESERVED_OUTPUTS_PER_JOB {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				OversizedReservation {
					requested,
					limit: MAX_RESERVED_OUTPUTS_PER_JOB,
				},
			));
		}
		let guard = |id: SlabId| {
			if bound {
				self.live_reservations.insert(id);
			}
			ReservedSlab {
				id,
				live: bound.then(|| self.live_reservations.clone()),
			}
		};
		let mut state = self.state.lock().await;
		let start = state.next_slab_id.get();
		let end = start.checked_add(requested).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidInput,
				"LSM output slab reservation would overflow SlabId",
			)
		})?;
		if !bound {
			if end > start {
				self.cover_slab_ids_through(&mut state, SlabId::new(end - 1))
					.await?;
			}
			state.advance_slab_allocator(SlabId::new(end))?;
			return Ok((start..end).map(|id| guard(SlabId::new(id))).collect());
		}
		let ids = (start..end).map(SlabId::new).collect::<Vec<_>>();
		let guards = ids.iter().map(|id| guard(*id)).collect::<Vec<_>>();
		let index = self.require_op_index()?;
		// §4: extending the durable bound and writing the rows are one transaction.
		// The range is taken first: the commit can land with its answer lost, or
		// with this caller gone, and an id it may have written is never issued again.
		// An abandoned range is only a gap.
		if SlabId::new(end) > state.durable_slab_id_bound {
			let bound = self.lease_bound_through(end)?;
			state.take_slab_range_before_its_commit(SlabId::new(end))?;
			index.reserve_slab_ids(ids, Some(bound)).await?;
			state.durable_slab_id_bound = state.durable_slab_id_bound.max(SlabId::new(bound));
			return Ok(guards);
		}
		state.advance_slab_allocator(SlabId::new(end))?;
		drop(state);
		index.reserve_slab_ids(ids, None).await?;
		Ok(guards)
	}

	/// The only way to the capability: `Unsubmitted` to `Exposed`, under this id's
	/// publication lock and the maintenance gate, before anything names it. The
	/// caller holds at most its rewrite claims, which precede both in the lock order.
	pub(super) async fn expose_reserved_id(
		&self,
		reserved: ReservedSlab,
	) -> std::io::Result<ExposedSlab> {
		if reserved.live.is_some() {
			let _maintenance = self.maintenance_publication_gate.clone().read_owned().await;
			let tier_lock = self.tier_publication_lock(reserved.id);
			let _tier_guard = tier_lock.lock().await;
			self.require_op_index()?
				.expose_reserved_id(reserved.id)
				.await?;
		}
		Ok(ExposedSlab { reserved })
	}

	/// Whether a burned id could be exposed: it never can.
	#[cfg(test)]
	pub(super) async fn expose_reserved_id_for_test(&self, id: SlabId) -> std::io::Result<()> {
		self.require_op_index()?.expose_reserved_id(id).await
	}

	pub(super) async fn reserve_exposed_slab_ids(
		&self,
		count: usize,
	) -> std::io::Result<Vec<ExposedSlab>> {
		let mut exposed = Vec::with_capacity(count);
		for reserved in self.reserve_slab_ids(count).await? {
			exposed.push(self.expose_reserved_id(reserved).await?);
		}
		Ok(exposed)
	}

	/// A maintenance replacement's id, exposed before its staged file is named.
	pub(super) async fn reserve_exposed_slab_id(&self) -> std::io::Result<ExposedSlab> {
		self.reserve_exposed_slab_ids(1)
			.await?
			.pop()
			.ok_or_else(|| Error::other("a reservation of one returned none"))
	}

	/// Rebuild the capability for a retry or after a restart, from a durable
	/// `Exposed` row only. No transition; `Burning`, absent and `Unsubmitted` refuse.
	/// Its caller is slice 5's recovery.
	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) async fn reacquire_exposed(&self, id: SlabId) -> std::io::Result<ExposedSlab> {
		let _maintenance = self.maintenance_publication_gate.clone().read_owned().await;
		let tier_lock = self.tier_publication_lock(id);
		let _tier_guard = tier_lock.lock().await;
		let found = self.require_op_index()?.reserved_id_state(id).await?;
		// Only the winner of this insert owns the liveness it will later remove.
		if !matches!(found, Some(ReservedIdState::Exposed { abandoned: None }))
			|| !self.live_reservations.insert(id)
		{
			return Err(Error::new(
				ErrorKind::WouldBlock,
				ReservationRefused { id, found },
			));
		}
		Ok(ExposedSlab {
			reserved: ReservedSlab {
				id,
				live: Some(self.live_reservations.clone()),
			},
		})
	}

	/// A dead job's ids. Liveness first: a row still owned is left alone. Each
	/// decision rechecks the durable state in its own transaction.
	pub(crate) async fn drive_reserved_ids(
		self: &Arc<Self>,
		committer: &dyn crate::storage::TierCommitter,
	) -> std::io::Result<()> {
		let index = self.require_op_index()?;
		let mut first_error = None;
		for (id, state) in index.load_reserved_ids().await? {
			if self.live_reservations.contains(&id) {
				continue;
			}
			let step = match state {
				ReservedIdState::Unsubmitted => self.burn_dead_reservation(id, committer).await,
				ReservedIdState::Burning { op } => self.submit_burn(id, op.op_seq, committer).await,
				ReservedIdState::Exposed { abandoned: None } => {
					self.reconcile_exposed_without_owner(id).await
				}
				ReservedIdState::Exposed {
					abandoned: Some(_), ..
				} => self.begin_abandoned_cleanup(id, committer).await,
				ReservedIdState::Cleaning { .. } => self.resume_cleanup(id, committer).await,
			};
			if let Err(error) = step {
				tracing::warn!(slab_id = id.get(), %error, "a reservation is unresolved");
				first_error.get_or_insert(error);
			}
		}
		first_error.map_or(Ok(()), Err)
	}

	async fn burn_dead_reservation(
		&self,
		id: SlabId,
		committer: &dyn crate::storage::TierCommitter,
	) -> std::io::Result<()> {
		let index = self.require_op_index()?;
		let owner = self.owner_ref()?;
		committer.preflight(owner).await?;
		let ticket = {
			let _maintenance = self.maintenance_publication_gate.clone().read_owned().await;
			let tier_lock = self.tier_publication_lock(id);
			let _tier_guard = tier_lock.lock().await;
			// A job can only have re-registered by never having lost it, but the
			// liveness read above was outside this lock.
			if self.live_reservations.contains(&id) {
				return Ok(());
			}
			index.begin_burn(id, owner).await?
		};
		self.finish_burn(id, ticket, committer).await
	}

	async fn submit_burn(
		&self,
		id: SlabId,
		op_seq: u64,
		committer: &dyn crate::storage::TierCommitter,
	) -> std::io::Result<()> {
		let index = self.require_op_index()?;
		committer.preflight(self.owner_ref()?).await?;
		let ticket = index
			.load_op_intents()
			.await?
			.into_iter()
			.find_map(|op| match op {
				LoadedOp::Submittable(ticket) if ticket.key().op_seq == op_seq => Some(ticket),
				_ => None,
			})
			.ok_or_else(|| {
				Error::new(
					ErrorKind::WouldBlock,
					format!("burning id {id} has lost op {op_seq}; it remains fenced"),
				)
			})?;
		self.finish_burn(id, ticket, committer).await
	}

	/// A burn has no local effect but the row. A rejection is an anomaly: the row
	/// stays `Burning`, permanently refused for exposure.
	async fn finish_burn(
		&self,
		id: SlabId,
		ticket: OpTicket,
		committer: &dyn crate::storage::TierCommitter,
	) -> std::io::Result<()> {
		let index = self.require_op_index()?;
		match committer.submit_op(&ticket).await? {
			VerifiedReceipt::Applied { .. } => {}
			VerifiedReceipt::Rejected { reason, .. } => {
				return Err(Error::new(
					ErrorKind::WouldBlock,
					format!("a burn of {id} was rejected {reason:?}; it remains fenced"),
				));
			}
		}
		let lease = match self.canonical_tier_source.get() {
			Some(source) => Some(source.read().await?),
			None => None,
		};
		if lease
			.as_ref()
			.is_some_and(|lease| !lease.op_applied_for_current_owner(ticket.key(), ticket.digest()))
		{
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"a burn is not applied for the current owner; it remains fenced",
			));
		}
		index
			.hand_off_burn(&ticket, id, Some(Box::new(lease)))
			.await
	}

	/// A rejected install's output, moved out of the discovery namespace before its
	/// handoff: the same inode and bytes, retained for its T1. Left at its slab path,
	/// a reopen would adopt it as a run. The linked intent still fences this scope
	/// until the handoff that records this identity.
	pub(super) async fn relocate_rejected_output(
		&self,
		id: SlabId,
	) -> std::io::Result<AbandonedOutput> {
		use std::os::unix::fs::MetadataExt;
		let path = slab_path(&self.config.dir, id);
		let metadata = std::fs::symlink_metadata(&path)?;
		let (length, digest) = hash_file(&path).await?;
		let moved = abandoned_output_path(&self.config.dir, id);
		self.file_ops.link_no_replace(&path, &moved).await?;
		fsync_parent_dir(self.file_ops.as_ref(), &moved).await?;
		self.file_ops.unlink(&path).await?;
		fsync_parent_dir(self.file_ops.as_ref(), &path).await?;
		Ok(AbandonedOutput {
			file: Some(GcFileIdentity {
				length,
				digest,
				revision: 0,
				dev: metadata.dev(),
				ino: metadata.ino(),
			}),
			relocated: true,
		})
	}

	/// An `Exposed` row with no owner. Absence of an intent is not evidence by
	/// itself: every durable reference is reconciled first. A run or an intent that
	/// names this id means it is not this path's to clean up.
	pub(super) async fn reconcile_exposed_without_owner(&self, id: SlabId) -> std::io::Result<()> {
		use std::os::unix::fs::MetadataExt;
		let index = self.require_op_index()?;
		let durable = index.load_snapshot().await?;
		let named_by_intent = durable.lsm_install_intents.iter().any(|intent| {
			intent
				.commit
				.slabs_to_add
				.iter()
				.any(|run| run.slab_id == id)
				|| intent
					.local_output_files
					.iter()
					.any(|file| file.slab_id == id)
		});
		if named_by_intent {
			return Ok(());
		}
		if durable.lsm_runs.iter().any(|run| run.slab_id == id)
			|| durable
				.record_rows
				.iter()
				.chain(&durable.tiered_records)
				.any(|row| row.location.slab_id == id)
		{
			return Err(Error::new(
				ErrorKind::WouldBlock,
				format!("exposed id {id} is referenced with no owner; it remains fenced"),
			));
		}
		let path = slab_path(&self.config.dir, id);
		let file = match std::fs::symlink_metadata(&path) {
			Err(error) if error.kind() == ErrorKind::NotFound => None,
			Err(error) => return Err(error),
			Ok(metadata) if !metadata.file_type().is_file() => {
				return Err(Error::new(
					ErrorKind::WouldBlock,
					format!("exposed id {id} names something that is not a file"),
				));
			}
			Ok(metadata) => {
				let (length, digest) = hash_file(&path).await?;
				Some(GcFileIdentity {
					length,
					digest,
					revision: 0,
					dev: metadata.dev(),
					ino: metadata.ino(),
				})
			}
		};
		// Serialized with reacquisition: one of the two owns this id. The scan above
		// proves nothing about now, so liveness and the row are both decided here. The
		// transaction owns these guards: an aborted caller cannot release them while
		// its worker can still commit.
		let maintenance = self.maintenance_publication_gate.clone().read_owned().await;
		let tier_guard = self.tier_publication_lock(id).lock_owned().await;
		if self.live_reservations.contains(&id) {
			return Ok(());
		}
		index
			.abandon_exposed_id(
				id,
				AbandonedOutput {
					file,
					relocated: false,
				},
				Some(Box::new((maintenance, tier_guard))),
			)
			.await
	}

	async fn begin_abandoned_cleanup(
		self: &Arc<Self>,
		id: SlabId,
		committer: &dyn crate::storage::TierCommitter,
	) -> std::io::Result<()> {
		let index = self.require_op_index()?;
		let owner = self.owner_ref()?;
		committer.preflight(owner).await?;
		let job_id = Self::lsm_gc_job_id(ManifestEpoch::new(0), 0xD3, &[id]);
		let (claim, complete) = {
			let _maintenance = self.maintenance_publication_gate.clone().read_owned().await;
			let tier_lock = self.tier_publication_lock(id);
			let _tier_guard = tier_lock.lock().await;
			if self.live_reservations.contains(&id) {
				return Ok(());
			}
			index.begin_cleanup(id, owner, job_id).await?
		};
		self.run_cleanup(id, job_id, claim, complete, committer)
			.await
	}

	/// A cleanup a restart or an earlier pass left. With local work started, it stays
	/// fenced for slice 5's phase recovery; before that, it simply carries on.
	async fn resume_cleanup(
		self: &Arc<Self>,
		id: SlabId,
		committer: &dyn crate::storage::TierCommitter,
	) -> std::io::Result<()> {
		let index = self.require_op_index()?;
		let Some(ReservedIdState::Cleaning {
			job_id,
			claim,
			complete,
			..
		}) = index.reserved_id_state(id).await?
		else {
			return Ok(());
		};
		committer.preflight(self.owner_ref()?).await?;
		let loaded = index.load_op_intents().await?;
		let find = |seq: u64| {
			loaded.iter().find(|op| match op {
				LoadedOp::Submittable(ticket) => ticket.key().op_seq == seq,
				LoadedOp::Reserved(reserved) => reserved.key().op_seq == seq,
			})
		};
		let (Some(LoadedOp::Submittable(claim)), Some(LoadedOp::Reserved(complete))) =
			(find(claim.op_seq), find(complete.op_seq))
		else {
			return Err(Error::new(
				ErrorKind::WouldBlock,
				format!("a cleanup of {id} is past what a pass may resume; it remains fenced"),
			));
		};
		if complete.progress().is_some() {
			return Err(Error::new(
				ErrorKind::WouldBlock,
				format!("a cleanup of {id} has local progress; it remains fenced"),
			));
		}
		self.run_cleanup(id, job_id, claim.clone(), complete.clone(), committer)
			.await
	}

	/// T1 for an id with no manifest run: the claim, then local disposal, then
	/// Complete, then the purge. The local stages are detached like GC's, so an
	/// aborted pass cannot release their authority under filesystem work.
	async fn run_cleanup(
		self: &Arc<Self>,
		id: SlabId,
		job_id: CompactionJobId,
		claim: OpTicket,
		complete: ReservedOp,
		committer: &dyn crate::storage::TierCommitter,
	) -> std::io::Result<()> {
		let index = self.require_op_index()?;
		match committer.submit_op(&claim).await? {
			VerifiedReceipt::Applied { .. } => {}
			VerifiedReceipt::Rejected { reason, .. } => {
				return Err(Error::new(
					ErrorKind::WouldBlock,
					format!("the claim of {id} was rejected {reason:?}; it remains fenced"),
				));
			}
		}
		let Some(ReservedIdState::Cleaning { output, .. }) = index.reserved_id_state(id).await?
		else {
			return Err(Error::other("a cleanup lost its row"));
		};
		let original = if output.relocated {
			abandoned_output_path(&self.config.dir, id)
		} else {
			slab_path(&self.config.dir, id)
		};
		let file = output.file;
		tokio::spawn(self.clone().cleanup_dispose(
			id,
			job_id,
			claim,
			complete.key(),
			complete.digest(),
			original,
			file.clone(),
		))
		.await
		.map_err(Error::other)??;
		let ticket = index.mark_may_be_submitted(complete).await?;
		match committer.submit_op(&ticket).await? {
			VerifiedReceipt::Applied { .. } => {}
			VerifiedReceipt::Rejected { reason, .. } => {
				return Err(Error::new(
					ErrorKind::WouldBlock,
					format!("the Complete of {id} was rejected {reason:?}; it remains fenced"),
				));
			}
		}
		tokio::spawn(self.clone().cleanup_finish(id, job_id, ticket, file))
			.await
			.map_err(Error::other)?
	}

	/// The claim's Applied authorizes this, read with the owner and held throughout.
	async fn cleanup_dispose(
		self: Arc<Self>,
		id: SlabId,
		job_id: CompactionJobId,
		claim: OpTicket,
		complete: crate::consensus::slab_ops::OpKey,
		complete_digest: [u8; 32],
		original: PathBuf,
		file: Option<GcFileIdentity>,
	) -> std::io::Result<()> {
		let index = self.require_op_index()?;
		let slab = claim.key().owner.slab(id);
		// Content first, from the inode it names and outside the lease: this can be a
		// whole slab. The same inode is rechecked under the lease and the locks.
		if let Some(file) = &file {
			gc_require_quarantine_content(&original, file).await?;
		}
		#[cfg(test)]
		self.pause_cleanup_validated_for_test(1).await;
		let lease = match self.canonical_tier_source.get() {
			Some(source) => Some(source.read().await?),
			None => None,
		};
		if lease
			.as_ref()
			.is_some_and(|lease| !lease.claim_held_by_current_owner(slab, job_id))
		{
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"this cleanup does not hold the claim for the current owner; it remains fenced",
			));
		}
		let _maintenance = self.maintenance_publication_gate.clone().read_owned().await;
		let tier_lock = self.tier_publication_lock(id);
		let _tier_guard = tier_lock.lock().await;
		if self.live_reservations.contains(&id)
			|| !matches!(
				index.reserved_id_state(id).await?,
				Some(ReservedIdState::Cleaning { job_id: job, .. }) if job == job_id
			) {
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"this cleanup no longer owns its id; it remains fenced",
			));
		}
		let progress = |progress| OpProgressUpdate {
			key: complete,
			digest: complete_digest,
			progress,
		};
		if let Some(file) = &file {
			gc_require_quarantine_inode(&original, file)?;
			index
				.advance_op_progress(progress(OpProgress::QuarantineRecorded(file.clone())), None)
				.await?;
			let quarantine = gc_quarantine_path(&self.config.dir, id, job_id);
			self.file_ops
				.link_no_replace(&original, &quarantine)
				.await?;
			fsync_parent_dir(self.file_ops.as_ref(), &quarantine).await?;
			self.file_ops.unlink(&original).await?;
			fsync_parent_dir(self.file_ops.as_ref(), &original).await?;
		}
		index
			.advance_op_progress(progress(OpProgress::RunDropped(file)), None)
			.await?;
		index.hand_off_op(claim.key(), claim.digest(), None).await?;
		drop(lease);
		Ok(())
	}

	/// Complete's exact Applied and the removed set authorize the purge, held
	/// through the transaction that ends this operation and its row.
	async fn cleanup_finish(
		self: Arc<Self>,
		id: SlabId,
		job_id: CompactionJobId,
		ticket: OpTicket,
		file: Option<GcFileIdentity>,
	) -> std::io::Result<()> {
		let index = self.require_op_index()?;
		let quarantine = gc_quarantine_path(&self.config.dir, id, job_id);
		if let Some(file) = &file {
			gc_require_quarantine_content(&quarantine, file).await?;
		}
		#[cfg(test)]
		self.pause_cleanup_validated_for_test(2).await;
		let lease = match self.canonical_tier_source.get() {
			Some(source) => Some(source.read().await?),
			None => None,
		};
		if lease.as_ref().is_some_and(|lease| {
			!lease.op_applied_for_current_owner(ticket.key(), ticket.digest())
				|| !lease.removed_for_current_owner(ticket.key().owner.slab(id))
		}) {
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"a completed cleanup is not removed for the current owner; it remains fenced",
			));
		}
		let tier_lock = self.tier_publication_lock(id);
		let _tier_guard = tier_lock.lock().await;
		if let Some(file) = file {
			gc_require_quarantine_inode(&quarantine, &file)?;
			let update = |progress| OpProgressUpdate {
				key: ticket.key(),
				digest: ticket.digest(),
				progress,
			};
			index
				.advance_op_progress(update(OpProgress::PurgeAuthorized(file.clone())), None)
				.await?;
			self.file_ops.unlink(&quarantine).await?;
			fsync_parent_dir(self.file_ops.as_ref(), &quarantine).await?;
			index
				.advance_op_progress(update(OpProgress::Purged(file)), None)
				.await?;
		}
		index.hand_off_finished_cleanup(&ticket, id).await?;
		drop(lease);
		Ok(())
	}
}

/// Where a rejected install's output waits for its claim: outside the namespace a
/// reopen discovers, as a staged maintenance replacement is.
pub(super) fn abandoned_output_path(dir: &Path, id: SlabId) -> PathBuf {
	dir.join(format!("abandoned-output-{:016x}.wnd", id.get()))
}

async fn hash_file(path: &Path) -> std::io::Result<(u64, [u8; 32])> {
	let path = path.to_path_buf();
	tokio::task::spawn_blocking(move || {
		let mut file = std::fs::File::open(&path)?;
		let mut hasher = blake3::Hasher::new();
		let length = std::io::copy(&mut file, &mut hasher)?;
		Ok::<_, Error>((length, *hasher.finalize().as_bytes()))
	})
	.await
	.map_err(Error::other)?
}
