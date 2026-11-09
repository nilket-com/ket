//! ADR 0699 S5b: the local rewrite that removes positively obsolete versions.
//!
//! In ParentDecisions mode every ordinary rewrite stays contained. This is the
//! one path that may reclaim: a caller claims one sealed slab, reads the
//! identity of every record in it, decides from one coherent index read which
//! versions are cleanup debt, and the slab is rewritten through the ordinary
//! maintenance rewrite with exactly those versions reclaimed. A record whose
//! table cannot be read from its frame has no identity and is never reclaimed;
//! conflicting versions stay protected as everywhere else. Tiered slabs are
//! never touched here.

use super::compaction::CounterRewritePlan;
use super::secondary_index_segments::MaintenanceClaims;
use super::*;
use crate::storage::index::certified_base::{CoherentFacts, DebtRecord, VersionKey, VersionRef};

/// One record of a claimed slab: the version the index names, the table its
/// frame stamps if any (append-only rows stamp it; primary-key rows take it
/// from their debt record), and whether its stored location is a tombstone.
#[derive(Clone, Copy, Debug)]
pub(crate) struct CleanupCandidate {
	pub(crate) version: VersionRef,
	pub(crate) frame_table: Option<TableObjectId>,
	pub(crate) tombstone: bool,
}

/// One claimed sealed slab under certified cleanup. The claim is held until
/// the job is finished or dropped.
pub(crate) struct CleanupJob {
	slab_id: SlabId,
	slab: Arc<LocalSlabStore>,
	claims: MaintenanceClaims,
	counter_plan: CounterRewritePlan,
	records: Vec<CompactionRecord>,
	candidates: Vec<CleanupCandidate>,
}

impl CleanupJob {
	/// The distinct identified versions of the slab, in order: what the
	/// coherent facts must be read for.
	pub(crate) fn versions(&self) -> Vec<VersionRef> {
		let mut versions: Vec<VersionRef> = self
			.candidates
			.iter()
			.map(|candidate| candidate.version)
			.collect();
		versions.sort_unstable();
		versions.dedup();
		versions
	}
}

/// The kind and content of a debt version's stored copies, checked against
/// its debt record from the copies themselves. Only [`ContentEvidence::check`]
/// constructs it, and only when every copy matches.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ContentEvidence {
	key: VersionKey,
	debt: DebtRecord,
	tombstone: bool,
}

/// One stored copy of a version: its kind, and for a row its content digest
/// (`None` when it could not be computed).
#[derive(Clone, Copy, Debug)]
pub(crate) struct StoredCopy {
	pub(crate) tombstone: bool,
	pub(crate) row_digest: Option<[u8; 32]>,
}

impl ContentEvidence {
	/// Evidence that every stored copy of `key` is the obsolete version its
	/// debt record names: there is at least one copy, every copy has the
	/// expected kind, and every row copy has the recorded digest. `None`
	/// otherwise: the version stays.
	pub(crate) fn check(
		key: VersionKey,
		debt: &DebtRecord,
		expected_tombstone: bool,
		copies: &[StoredCopy],
	) -> Option<Self> {
		let matches = !copies.is_empty()
			&& copies.iter().all(|copy| {
				copy.tombstone == expected_tombstone
					&& (copy.tombstone || copy.row_digest == Some(debt.row_digest))
			});
		matches.then(|| Self {
			key,
			debt: debt.clone(),
			tombstone: expected_tombstone,
		})
	}

	/// ADR 0703: whether this is the evidence for `key`'s `debt` record with
	/// copies of this kind.
	pub(crate) fn matches(&self, key: VersionKey, debt: &DebtRecord, tombstone: bool) -> bool {
		self.key == key && self.debt == *debt && self.tombstone == tombstone
	}
}

/// Which records of one claimed slab are reclaimed. Only
/// [`ReclamationPlan::decide`] constructs it, from the job's own candidates,
/// facts read for exactly those versions in one index transaction, and
/// content evidence; the destructive rewrite accepts nothing else.
#[derive(Debug)]
pub(crate) struct ReclamationPlan {
	slab_id: SlabId,
	/// The active generation the facts were read under.
	active_gid: u64,
	reclaim: Vec<bool>,
}

impl ReclamationPlan {
	/// A record is reclaimed iff the facts list positive debt for its version
	/// under exactly one table (agreeing with any table its frame stamps), no
	/// owner names it, and no active base entry of that table is at its HLC
	/// (either kind); and content evidence for that same debt record matches
	/// the record's kind. Facts read for other versions are refused outright.
	pub(crate) fn decide(
		job: &CleanupJob,
		facts: &CoherentFacts,
		evidence: &[ContentEvidence],
	) -> std::io::Result<Self> {
		if facts.candidates() != job.versions().as_slice() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"the facts were not read for this job's versions",
			));
		}
		let reclaim = job
			.candidates
			.iter()
			.map(|candidate| {
				let Ok(at) = facts.candidates().binary_search(&candidate.version) else {
					return false;
				};
				let fact = facts.get(at);
				let Some((key, debt)) = &fact.debt else {
					return false;
				};
				let key = *key;
				if fact.ambiguous
					|| candidate
						.frame_table
						.is_some_and(|table| table != key.table)
				{
					return false;
				}
				let protected_by_base = fact.base.as_ref().is_some_and(|base| base.hlc == key.hlc);
				let matched = evidence.iter().any(|evidence| {
					evidence.key == key
						&& evidence.debt == *debt
						&& evidence.tombstone == candidate.tombstone
				});
				!fact.owned && !protected_by_base && matched
			})
			.collect();
		Ok(Self {
			slab_id: job.slab_id,
			active_gid: facts.active_gid(),
			reclaim,
		})
	}

	/// How many records the plan reclaims.
	pub(crate) fn reclaimed(&self) -> usize {
		self.reclaim.iter().filter(|reclaim| **reclaim).count()
	}

	/// Marks `records` (the job's own, in its order) from the plan.
	fn apply(&self, slab_id: SlabId, records: &mut [CompactionRecord]) -> std::io::Result<()> {
		if slab_id != self.slab_id || records.len() != self.reclaim.len() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"a reclamation plan applies only to the job it was decided for",
			));
		}
		for (record, reclaim) in records.iter_mut().zip(&self.reclaim) {
			record.reclaim = *reclaim;
		}
		Ok(())
	}
}

#[cfg(test)]
impl SlabManager {
	/// Test-only: the bytes of every local sealed slab.
	pub(crate) async fn sealed_local_bytes_for_test(&self) -> u64 {
		let state = self.state.lock().await;
		state
			.sealed
			.iter()
			.map(|entry| entry.value().write_cursor())
			.sum()
	}
}

impl SlabManager {
	/// The local sealed slabs, by id.
	pub(crate) async fn sealed_slab_ids(&self) -> Vec<SlabId> {
		let state = self.state.lock().await;
		let mut slabs = state
			.sealed
			.iter()
			.map(|entry| *entry.key())
			.collect::<Vec<_>>();
		slabs.sort_unstable();
		slabs
	}

	/// Claims one sealed local slab and reads every record's identity. `None`
	/// when it is not claimable now (not sealed, not a live run, or holding
	/// pending counter frames).
	pub(crate) async fn claim_for_certified_cleanup(
		&self,
		slab_id: SlabId,
	) -> std::io::Result<Option<CleanupJob>> {
		self.require_tier_authority_ready()?;
		let Some((claims, slab)) = self.claim_sealed_slab_for_maintenance(slab_id).await else {
			return Ok(None);
		};
		let counter_plan = self.plan_counter_rewrite(&slab).await?;
		if counter_plan.pending {
			return Ok(None);
		}
		let mut records: Vec<CompactionRecord> = slab
			.index
			.iter()
			.flat_map(|entry| {
				let (tenant_id, key_identity) = *entry.key();
				let routing_locality = self.routing_locality_for_key(tenant_id, key_identity);
				entry
					.value()
					.iter()
					.map(move |loc| CompactionRecord {
						tenant_id,
						key_identity,
						routing_locality,
						old_location: *loc,
						reclaim: false,
						retention_ambiguous: false,
						retention_lag_seconds: 0,
					})
					.collect::<Vec<_>>()
			})
			.collect();
		records = Self::ordinary_compaction_records(&records, &counter_plan);
		records.sort_by_key(|record| record.old_location.offset);
		let mut candidates: Vec<CleanupCandidate> = records
			.iter()
			.map(|record| CleanupCandidate {
				version: VersionRef {
					tenant: record.tenant_id,
					key_identity: *record.key_identity.as_bytes(),
					hlc: record.old_location.hlc,
				},
				frame_table: None,
				tombstone: record.old_location.is_tombstone,
			})
			.collect();
		// A table the frame stamps must agree with the debt's.
		for indices in physical_compaction_group_indices(&records).values() {
			let Some(first) = indices.first().copied() else {
				continue;
			};
			let frame = slab.read(records[first].old_location).await?;
			if storage_multirow_index_entries_from_frame(&frame)?.is_none() {
				continue;
			}
			let batch = decode_record_batch(&frame)?;
			for index in indices {
				let record = records[*index];
				candidates[*index].frame_table =
					storage_row_table_object_id_at(&batch, record.old_location.record_row_index)?;
			}
		}
		Ok(Some(CleanupJob {
			slab_id,
			slab,
			claims,
			counter_plan,
			records,
			candidates,
		}))
	}

	/// Rewrites the claimed slab reclaiming exactly what `plan`, decided for
	/// this job, reclaims. Conflicting versions stay protected.
	pub(crate) async fn finish_certified_cleanup(
		&self,
		job: CleanupJob,
		plan: &ReclamationPlan,
		table_schemas: &TableSchemaCatalog,
		secondary_indexes: &SecondaryIndexCatalog,
	) -> std::io::Result<Option<usize>> {
		// The plan is bound to the generation its facts were read under: a
		// newer activation means deciding again.
		let index = self
			.persistent_index()
			.ok_or_else(|| Error::new(ErrorKind::NotFound, "no persistent index"))?;
		if index.base_meta().await?.active_gid != plan.active_gid {
			return Ok(None);
		}
		let mut records = job.records;
		plan.apply(job.slab_id, &mut records)?;
		Ok(self
			.compact_sealed_slab_records_with_authority(
				job.slab_id,
				job.slab,
				&records,
				SecondaryIndexCompactionContext {
					table_schemas,
					secondary_indexes,
				},
				&job.claims,
				&job.counter_plan,
				super::compaction::ReclaimAuthority::Certified(plan),
			)
			.await?
			.map(|outcome| outcome.reclaimed_records))
	}
}
