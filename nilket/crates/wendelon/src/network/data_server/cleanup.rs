//! ADR 0699 S5b: a store's certified cleanup.
//!
//! Each step, under a lifecycle permit, visits one sealed local slab: it
//! claims the slab, reads every record's identity, reads the survivor rule's
//! facts for those versions in one index transaction, checks every debt
//! version's stored copies against its debt record, and rewrites the slab
//! reclaiming exactly what the resulting plan decides. Only versions an
//! activation positively recorded as obsolete are ever reclaimed; anything
//! unidentified, owned, protected by the base or not matching its evidence
//! stays. A separate bounded sweep retires debt records that have no copy
//! left. Neither runs before this start's certified base is validated, nor
//! behind the boundary; tiered slabs are never touched.

use super::*;
use crate::storage::index::certified_base::{CoherentFacts, DebtSource, VersionKey};
use crate::storage::manager::certified_cleanup::{ContentEvidence, ReclamationPlan, StoredCopy};

/// Debt records retired per sweep step, at most.
const DEBT_SWEEP_BATCH: usize = 64;

impl WendelonDataServer {
	/// Whether this store may clean now: in mode, its base validated for this
	/// start and not behind the boundary.
	fn may_clean(&self) -> bool {
		use std::sync::atomic::Ordering;
		self.decisions.signer.is_some()
			&& self.decisions.base_validated.load(Ordering::Acquire)
			&& !self.decisions.behind_boundary.load(Ordering::Acquire)
	}

	/// One certified cleanup step over the next sealed local slab. Returns the
	/// records it reclaimed.
	pub(crate) async fn certified_cleanup_step(&self) -> std::io::Result<usize> {
		if !self.may_clean() {
			return Ok(0);
		}
		#[cfg(test)]
		if self.cleanup_paused_for_test()
			|| self
				.decisions
				.pause_local_cleanup
				.load(std::sync::atomic::Ordering::Acquire)
		{
			return Ok(0);
		}
		self.under_permit(self.certified_cleanup_work()).await
	}

	async fn certified_cleanup_work(&self) -> std::io::Result<usize> {
		use std::sync::atomic::Ordering;
		let Some(index) = self.slab.persistent_index() else {
			return Ok(0);
		};
		// No debt at all: nothing to visit.
		if index.debt_page(None, 1).await?.is_empty() {
			return Ok(0);
		}
		let gid = index.base_meta().await?.active_gid;
		let sealed = self.slab.sealed_slab_ids().await;
		let eligible: Vec<_> = {
			let mut debt_free = self.decisions.debt_free.lock().unwrap();
			debt_free.retain(|slab, _| sealed.iter().any(|sealed| sealed.get() == *slab));
			sealed
				.iter()
				.copied()
				.filter(|slab| debt_free.get(&slab.get()) != Some(&gid))
				.collect()
		};
		let Some(first) = eligible.first().copied() else {
			return Ok(0);
		};
		// A pass cursor: the next eligible slab after the last one visited,
		// wrapping.
		let last = self.decisions.cleanup_cursor.load(Ordering::Acquire);
		let slab_id = eligible
			.iter()
			.copied()
			.find(|slab| slab.get() > last)
			.unwrap_or(first);
		self.decisions
			.cleanup_cursor
			.store(slab_id.get(), Ordering::Release);
		let Some(job) = self.slab.claim_for_certified_cleanup(slab_id).await? else {
			return Ok(0);
		};
		let versions = job.versions();
		if versions.is_empty() {
			self.decisions
				.debt_free
				.lock()
				.unwrap()
				.insert(slab_id.get(), gid);
			return Ok(0);
		}
		let facts = index.version_facts(versions).await?;
		// Unavailable evidence refuses the step (retryably) and caches nothing.
		let evidence = self.content_evidence(&facts).await?;
		let plan = ReclamationPlan::decide(&job, &facts, &evidence)?;
		// Test-only trace of a step that found debt.
		#[cfg(test)]
		{
			let debt = (0..facts.candidates().len())
				.filter(|at| facts.get(*at).debt.is_some())
				.count();
			if debt > 0 {
				eprintln!(
					"CLEANUP {} slab {slab_id:?}: debt {debt}, evidence {}, reclaim {}",
					self.local_node.id,
					evidence.len(),
					plan.reclaimed()
				);
			}
		}
		if plan.reclaimed() == 0 {
			self.decisions
				.debt_free
				.lock()
				.unwrap()
				.insert(slab_id.get(), gid);
			return Ok(0);
		}
		#[cfg(test)]
		self.decisions
			.pause_at(super::child_writes::PauseStage::CleanupBeforeRewrite)
			.await;
		let state = self.state_machine.state_snapshot().await;
		Ok(self
			.slab
			.finish_certified_cleanup(job, &plan, &state.table_schemas, &state.secondary_indexes)
			.await?
			.unwrap_or(0))
	}

	/// Content evidence for every version the facts list as unprotected debt.
	/// A version whose copies were read and do not all match its debt record's
	/// kind and content gets none and stays. Evidence that cannot be
	/// established now (no descriptor, a copy that cannot be read) refuses the
	/// whole step, retryably: it is never mistaken for a debt-free slab.
	///
	/// Codex 01a20599 and 01a205cd (DECIDING-ATTRIBUTION): the copies of every
	/// debt version are read together, each physical frame read, verified and
	/// decoded once while its copies are visited
	/// ([`SlabManager::stored_copies_reusing_frames`]), instead of once per
	/// version it holds. Each version keeps the descriptor it resolved; equal
	/// descriptors share one allocation. The refusals are unchanged and in the
	/// same precedence: the first version (in candidate order) without a
	/// descriptor ends the list, and refuses unless a copy of an earlier version
	/// cannot be read.
	async fn content_evidence(
		&self,
		facts: &CoherentFacts,
	) -> std::io::Result<Vec<ContentEvidence>> {
		let unavailable = |what: &str| {
			std::io::Error::new(
				std::io::ErrorKind::WouldBlock,
				format!("cleanup evidence unavailable: {what}"),
			)
		};
		let mut checks = Vec::new();
		let mut descriptors: Vec<Arc<TableSchemaDescriptor>> = Vec::new();
		let mut no_descriptor = false;
		for at in 0..facts.candidates().len() {
			let fact = facts.get(at);
			let Some((key, debt)) = &fact.debt else {
				continue;
			};
			if fact.ambiguous
				|| fact.owned
				|| fact.base.as_ref().is_some_and(|base| base.hlc == key.hlc)
			{
				continue;
			}
			// The writer's, reader's and certifier's resolution: the reserved
			// KV table's descriptor is synthetic, never in the catalog.
			let (tenant, table) = (key.tenant, key.table);
			let Some(descriptor) = self
				.state_machine
				.read_state(move |state| {
					super::child_writes::recorded_latest_table_descriptor(state, tenant, table)
				})
				.await
			else {
				no_descriptor = true;
				break;
			};
			let expected_tombstone = match debt.source {
				DebtSource::DisplacedBase { tombstone } => tombstone,
				DebtSource::RetiredIntent { .. } => super::deletes::is_tombstone_version(
					&descriptor,
					&key.as_owned(debt.row_digest),
				),
			};
			checks.push((
				key,
				debt,
				expected_tombstone,
				share(&mut descriptors, descriptor),
			));
		}
		let copies = if checks.is_empty() {
			Vec::new()
		} else {
			match self.stored_copies(&checks).await {
				Some(copies) => copies,
				None => return Err(unavailable("a debt version's copy could not be read")),
			}
		};
		let mut evidence = Vec::new();
		for ((key, debt, expected_tombstone, _), copies) in checks.iter().zip(&copies) {
			if let Some(found) = ContentEvidence::check(**key, debt, *expected_tombstone, copies) {
				evidence.push(found);
			}
		}
		if no_descriptor {
			return Err(unavailable("no descriptor for a debt version's table"));
		}
		Ok(evidence)
	}

	/// Every stored copy of each checked version: its indexed location's kind,
	/// and a row's content digest, each copy's kind and row taken from the same
	/// location. The copies are the version's scope-checked indexed locations.
	/// `None` when any copy of any of them cannot be read.
	async fn stored_copies(&self, checks: &[EvidenceCheck<'_>]) -> Option<Vec<Vec<StoredCopy>>> {
		#[cfg(test)]
		if self
			.decisions
			.fail_copy_read_once
			.swap(false, std::sync::atomic::Ordering::AcqRel)
		{
			return None;
		}
		let mut versions = Vec::with_capacity(checks.len());
		for (key, ..) in checks {
			versions.push(
				self.slab
					.version_locations(key.tenant, Identity::new(key.key_identity), key.hlc)
					.ok()?,
			);
		}
		let (copies, _) = self
			.slab
			.stored_copies_reusing_frames(&versions, |version, _, location, row| StoredCopy {
				tombstone: location.is_tombstone,
				row_digest: if location.is_tombstone {
					None
				} else {
					super::child_writes::stored_row_digest(&checks[version].3, row).ok()
				},
			})
			.await;
		copies
	}

	/// One bounded debt sweep step: retires the debt records of the next page
	/// that have no copy left, no owner and no base entry at their HLC, each
	/// checked against its exact record in one write transaction. Returns how
	/// many it retired.
	///
	/// DC-271: a retired intent's debt is retired only while this sweep holds
	/// that reservation's stripe, from before the check through the retirement
	/// transaction. A producer holds the stripe from before its decision check
	/// through its last append, so a version it can still append keeps its debt.
	/// A busy stripe is skipped without waiting; the cursor still advances, and
	/// the debt is revisited when the sweep wraps.
	pub(crate) async fn debt_sweep_step(&self) -> std::io::Result<usize> {
		if !self.may_clean() {
			return Ok(0);
		}
		#[cfg(test)]
		if self.cleanup_paused_for_test() {
			return Ok(0);
		}
		let Some(index) = self.slab.persistent_index() else {
			return Ok(0);
		};
		let after = *self.decisions.debt_cursor.lock().unwrap();
		let page = index.debt_page(after, DEBT_SWEEP_BATCH).await?;
		let mut retired = 0;
		for (key, record) in &page {
			let _producer = match record.source {
				crate::storage::index::certified_base::DebtSource::RetiredIntent {
					rsv, ..
				} => match self.decisions.locks.try_lock(rsv) {
					Some(guard) => Some(guard),
					None => {
						#[cfg(test)]
						self.decisions
							.debt_busy_skips
							.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
						continue;
					}
				},
				crate::storage::index::certified_base::DebtSource::DisplacedBase { .. } => None,
			};
			// DC-276: zero `RECORDS_TABLE` copies is an absence only while this
			// open's recovery is complete and no publication is uncertain (the
			// authority), and no copy is in the in-memory index. The guards move
			// into the retirement's database job, through its commit.
			let Some(authority) = self.slab.try_debt_absence_authority() else {
				continue;
			};
			if self.slab.version_in_memory_index(
				key.tenant,
				Identity::new(key.key_identity),
				key.hlc,
			) {
				continue;
			}
			if index
				.retire_debt_holding(*key, record.clone(), (_producer, authority))
				.await?
			{
				retired += 1;
			}
		}
		*self.decisions.debt_cursor.lock().unwrap() = if page.len() < DEBT_SWEEP_BATCH {
			None
		} else {
			page.last().map(|(key, _)| *key)
		};
		Ok(retired)
	}
}

impl WendelonDataServer {
	/// ADR 0703: one certified LSM job on this store, when it may clean.
	/// The caller is the leader-gated LSM service (only the leader compacts).
	/// The job runs under a lifecycle permit: an interrupted one revokes the
	/// clean record, so the next start recovers it.
	pub(crate) async fn certified_lsm_step(
		&self,
		worker: &crate::storage::LsmCompactionWorker,
		now: HybridTimestamp,
		committer: &dyn crate::storage::TierCommitter,
	) -> Result<Option<crate::storage::CompactionOutcome>, crate::storage::CompactionFailure> {
		if !self.parent_decisions() || !self.may_clean() {
			return Ok(None);
		}
		#[cfg(test)]
		if self.cleanup_paused_for_test() {
			return Ok(None);
		}
		self.certified_lsm_step_unpaused(worker, now, committer)
			.await
	}

	#[cfg(test)]
	fn cleanup_paused_for_test(&self) -> bool {
		self.decisions
			.pause_cleanup
			.load(std::sync::atomic::Ordering::Acquire)
	}

	/// The step itself, past its admission checks; a test drives it directly
	/// while the store's own steps are paused.
	pub(crate) async fn certified_lsm_step_unpaused(
		&self,
		worker: &crate::storage::LsmCompactionWorker,
		now: HybridTimestamp,
		committer: &dyn crate::storage::TierCommitter,
	) -> Result<Option<crate::storage::CompactionOutcome>, crate::storage::CompactionFailure> {
		if !self.parent_decisions() || !self.may_clean() {
			return Ok(None);
		}
		let permit = match self.slab.directory_owner() {
			Some(owner) => Some(owner.lifecycle_permit()?),
			None => None,
		};
		#[cfg(test)]
		let held = self
			.decisions
			.pause_at(super::child_writes::PauseStage::CertifiedLsmStep)
			.await;
		let state = self.state_machine.state_snapshot().await;
		let result = self
			.slab
			.run_certified_lsm_tick(
				worker,
				now,
				committer,
				&state.table_schemas,
				&state.secondary_indexes,
				self,
			)
			.await;
		if let Some(permit) = permit {
			permit.complete();
		}
		#[cfg(test)]
		if held {
			self.decisions.held_outcomes.lock().unwrap().push(
				super::child_writes::HeldStepOutcome::Lsm(
					result
						.as_ref()
						.map(Clone::clone)
						.map_err(ToString::to_string),
				),
			);
		}
		result
	}
}

#[async_trait::async_trait]
impl crate::storage::manager::certified_lsm::CertifiedLsmDecider for WendelonDataServer {
	/// The job's selection, from one coherent read of exactly its versions'
	/// facts and the content evidence for their debt.
	async fn decide(
		&self,
		job: &crate::storage::manager::certified_lsm::LsmCertifiedJob,
	) -> std::io::Result<crate::storage::manager::certified_lsm::CertifiedSelection> {
		let index = self.slab.persistent_index().ok_or_else(|| {
			std::io::Error::new(std::io::ErrorKind::NotFound, "no persistent index")
		})?;
		let facts = index.version_facts(job.versions()).await?;
		let evidence = self.content_evidence(&facts).await?;
		crate::storage::manager::certified_lsm::CertifiedSelection::decide(job, &facts, &evidence)
	}
}

impl WendelonDataServer {
	/// ADR 0704: one certified tier job on this store, when it may clean. The
	/// caller is the leader-gated LSM service; the step refuses, typed, off the
	/// leader. It runs under a lifecycle permit over the step and its work.
	pub(crate) async fn certified_tier_step(
		&self,
		committer: &dyn crate::storage::TierCommitter,
	) -> std::io::Result<crate::storage::manager::certified_tier::CertifiedTierStep> {
		use crate::storage::manager::certified_observer::TierStepOutcome;
		use crate::storage::manager::certified_tier::CertifiedTierStep;
		// Wendelon ADR 0125: a tick the data server does not admit is recorded as such.
		if !self.parent_decisions() || !self.may_clean() {
			self.slab
				.certified_tier_observer()
				.count(TierStepOutcome::NotAdmitted);
			return Ok(CertifiedTierStep::Idle);
		}
		#[cfg(test)]
		if self.cleanup_paused_for_test() {
			self.slab
				.certified_tier_observer()
				.count(TierStepOutcome::NotAdmitted);
			return Ok(CertifiedTierStep::Idle);
		}
		self.certified_tier_step_unpaused(committer).await
	}

	/// The tier step itself, past its admission checks; a test drives it directly
	/// while the store's own steps are paused (wendelon ADR 0122).
	pub(crate) async fn certified_tier_step_unpaused(
		&self,
		committer: &dyn crate::storage::TierCommitter,
	) -> std::io::Result<crate::storage::manager::certified_tier::CertifiedTierStep> {
		use crate::storage::manager::certified_tier::CertifiedTierStep;
		if !self.parent_decisions() || !self.may_clean() {
			self.slab
				.certified_tier_observer()
				.count(crate::storage::manager::certified_observer::TierStepOutcome::NotAdmitted);
			return Ok(CertifiedTierStep::Idle);
		}
		let permit = match self.slab.directory_owner() {
			Some(owner) => Some(owner.lifecycle_permit()?),
			None => None,
		};
		// Wendelon ADR 0125: admitted work is observed from here, before the state snapshot,
		// the debt check and slab selection (`selecting`).
		let step = self.slab.certified_tier_observer().begin_step();
		#[cfg(test)]
		let held = self
			.decisions
			.pause_at(super::child_writes::PauseStage::CertifiedTierStep)
			.await;
		let state = self.state_machine.state_snapshot().await;
		let result = self
			.slab
			.run_certified_tier_step_observed(committer, self, &state.table_schemas, step)
			.await;
		if let Some(permit) = permit {
			permit.complete();
		}
		#[cfg(test)]
		if held {
			self.decisions.held_outcomes.lock().unwrap().push(
				super::child_writes::HeldStepOutcome::Tier(
					result
						.as_ref()
						.map(Clone::clone)
						.map_err(ToString::to_string),
				),
			);
		}
		result
	}
}

#[async_trait::async_trait]
impl crate::storage::manager::certified_tier::CertifiedTierDecider for WendelonDataServer {
	/// The job's selection, from one coherent read of exactly its versions'
	/// facts and the content evidence for their debt.
	async fn decide(
		&self,
		job: &crate::storage::manager::certified_tier::TierCertifiedJob,
	) -> std::io::Result<crate::storage::manager::certified_lsm::CertifiedSelection> {
		let index = self.slab.persistent_index().ok_or_else(|| {
			std::io::Error::new(std::io::ErrorKind::NotFound, "no persistent index")
		})?;
		let facts = index.version_facts(job.versions()).await?;
		let evidence = self.content_evidence(&facts).await?;
		crate::storage::manager::certified_lsm::CertifiedSelection::decide_tiered(
			job, &facts, &evidence,
		)
	}
}

/// One debt version to check: its key and debt record, its expected kind, and the descriptor it
/// resolved (shared with every version that resolved an equal one).
type EvidenceCheck<'a> = (
	&'a VersionKey,
	&'a crate::storage::index::certified_base::DebtRecord,
	bool,
	Arc<TableSchemaDescriptor>,
);

/// `descriptor`, as one shared allocation with any equal descriptor already resolved in this
/// call: the same value, so the same digest and kind rules, held once.
fn share(
	descriptors: &mut Vec<Arc<TableSchemaDescriptor>>,
	descriptor: TableSchemaDescriptor,
) -> Arc<TableSchemaDescriptor> {
	if let Some(shared) = descriptors.iter().find(|shared| ***shared == descriptor) {
		return shared.clone();
	}
	let shared = Arc::new(descriptor);
	descriptors.push(shared.clone());
	shared
}
