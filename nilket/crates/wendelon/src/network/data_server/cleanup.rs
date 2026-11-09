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
		if self.cleanup_paused_for_test() {
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
		let mut evidence = Vec::new();
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
					super::child_writes::table_descriptor(state, tenant, table, 0)
						.or_else(|| state.table_schemas.latest_by_id(tenant, table).cloned())
				})
				.await
			else {
				return Err(unavailable("no descriptor for a debt version's table"));
			};
			let expected_tombstone = match debt.source {
				DebtSource::DisplacedBase { tombstone } => tombstone,
				DebtSource::RetiredIntent { .. } => super::deletes::is_tombstone_version(
					&descriptor,
					&key.as_owned(debt.row_digest),
				),
			};
			let Some(copies) = self.stored_copies(key, &descriptor).await else {
				return Err(unavailable("a debt version's copy could not be read"));
			};
			if let Some(found) = ContentEvidence::check(*key, debt, expected_tombstone, &copies) {
				evidence.push(found);
			}
		}
		Ok(evidence)
	}

	/// Every stored copy of `key`: its kind, and a row's content digest.
	/// `None` when any copy cannot be read.
	async fn stored_copies(
		&self,
		key: &VersionKey,
		descriptor: &TableSchemaDescriptor,
	) -> Option<Vec<StoredCopy>> {
		#[cfg(test)]
		if self
			.decisions
			.fail_copy_read_once
			.swap(false, std::sync::atomic::Ordering::AcqRel)
		{
			return None;
		}
		let identity = Identity::new(key.key_identity);
		let locations = self
			.slab
			.version_locations(key.tenant, identity, key.hlc)
			.ok()?;
		let rows = self
			.slab
			.stored_rows_at_version(key.tenant, identity, key.hlc)
			.await
			.ok()?;
		if rows.len() != locations.len() {
			return None;
		}
		Some(
			locations
				.iter()
				.zip(&rows)
				.map(|(location, row)| StoredCopy {
					tombstone: location.is_tombstone,
					row_digest: if location.is_tombstone {
						None
					} else {
						super::child_writes::stored_row_digest(descriptor, row).ok()
					},
				})
				.collect(),
		)
	}

	/// One bounded debt sweep step: retires the debt records of the next page
	/// that have no copy left, no owner and no base entry at their HLC, each
	/// checked against its exact record in one write transaction. Returns how
	/// many it retired.
	pub(crate) async fn debt_sweep_step(&self) -> std::io::Result<usize> {
		if !self.may_clean() {
			return Ok(0);
		}
		let Some(index) = self.slab.persistent_index() else {
			return Ok(0);
		};
		let after = *self.decisions.debt_cursor.lock().unwrap();
		let page = index.debt_page(after, DEBT_SWEEP_BATCH).await?;
		let mut retired = 0;
		for (key, record) in &page {
			if index.retire_debt(*key, record.clone()).await? {
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
