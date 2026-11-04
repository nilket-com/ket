use crate::hash::state_digest::{StateDigest, StateDigestAccumulator, visible_row_element};
use crate::schema::{
	ByteCount, CompactionJobId, CompactionLevelIndex, HybridTimestamp, Identity, ManifestEpoch,
	RecordRowIndex, RowCount, SlabId, TableObjectId, TenantId,
};
use crate::storage::core::RecordLocation;
use crate::storage::manifest::ManifestSnapshot;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::io::{Error, ErrorKind};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CompactionMergeKey {
	pub row_hlc: HybridTimestamp,
	pub row_key_id: Identity,
	pub record_row_index: RecordRowIndex,
}

impl CompactionMergeKey {
	pub const fn new(
		row_hlc: HybridTimestamp,
		row_key_id: Identity,
		record_row_index: RecordRowIndex,
	) -> Self {
		Self {
			row_hlc,
			row_key_id,
			record_row_index,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SlabState {
	Live,
	Retired { successors: Vec<SlabId> },
	DeletePending,
}

impl SlabState {
	pub const fn is_live(&self) -> bool {
		matches!(self, Self::Live)
	}

	pub const fn is_delete_pending(&self) -> bool {
		matches!(self, Self::DeletePending)
	}
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SidecarCoverage {
	#[default]
	None,
	Partial,
	Complete,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LsmRunSummary {
	pub slab_id: SlabId,
	pub compaction_level: CompactionLevelIndex,
	pub state: SlabState,
	pub min_row_hlc: HybridTimestamp,
	pub max_row_hlc: HybridTimestamp,
	pub size_bytes: ByteCount,
	pub row_count: RowCount,
	pub sidecar_coverage: SidecarCoverage,
	pub manifest_epoch: ManifestEpoch,
	pub retention_debt_score: u32,
	pub tombstone_debt_score: u32,
	pub index_coverage_debt_score: u32,
	pub layout_debt_score: u32,
}

impl LsmRunSummary {
	pub fn new(
		slab_id: SlabId,
		compaction_level: CompactionLevelIndex,
		min_row_hlc: HybridTimestamp,
		max_row_hlc: HybridTimestamp,
		size_bytes: ByteCount,
		row_count: RowCount,
		manifest_epoch: ManifestEpoch,
	) -> Self {
		Self {
			slab_id,
			compaction_level,
			state: SlabState::Live,
			min_row_hlc,
			max_row_hlc,
			size_bytes,
			row_count,
			sidecar_coverage: SidecarCoverage::None,
			manifest_epoch,
			retention_debt_score: 0,
			tombstone_debt_score: 0,
			index_coverage_debt_score: 0,
			layout_debt_score: 0,
		}
	}

	pub fn with_state(mut self, state: SlabState) -> Self {
		self.state = state;
		self
	}

	pub fn with_debt_scores(
		mut self,
		retention: u32,
		tombstone: u32,
		index_coverage: u32,
		layout: u32,
	) -> Self {
		self.retention_debt_score = retention;
		self.tombstone_debt_score = tombstone;
		self.index_coverage_debt_score = index_coverage;
		self.layout_debt_score = layout;
		self
	}

	fn dominant_reason(&self, level_size_pressure: bool) -> DominantReason {
		let candidates = [
			(self.retention_debt_score, DominantReason::RetentionDebt),
			(self.tombstone_debt_score, DominantReason::TombstoneDebt),
			(
				self.index_coverage_debt_score,
				DominantReason::IndexCoverage,
			),
			(self.layout_debt_score, DominantReason::LayoutConvergence),
		];
		candidates
			.into_iter()
			.max_by_key(|(score, reason)| (*score, Reverse(*reason as u8)))
			.and_then(|(score, reason)| if score > 0 { Some(reason) } else { None })
			.unwrap_or(if level_size_pressure {
				DominantReason::LevelSize
			} else {
				DominantReason::Operator
			})
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionPlan {
	pub job_id: CompactionJobId,
	pub source_level: CompactionLevelIndex,
	pub target_level: CompactionLevelIndex,
	pub manifest_epoch_pre: ManifestEpoch,
	pub input_slabs: Vec<SlabId>,
	pub overlapping_slabs: Vec<SlabId>,
	pub retention_floor_snapshot: HybridTimestamp,
	pub compaction_start_hlc: HybridTimestamp,
	pub dominant_reason: DominantReason,
}

impl CompactionPlan {
	pub fn touched_slabs(&self) -> Vec<SlabId> {
		let mut slabs = self.input_slabs.clone();
		slabs.extend(self.overlapping_slabs.iter().copied());
		slabs.sort_unstable();
		slabs.dedup();
		slabs
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(u8)]
pub enum DominantReason {
	LevelSize = 1,
	RetentionDebt = 2,
	TombstoneDebt = 3,
	IndexCoverage = 4,
	LayoutConvergence = 5,
	Operator = 6,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u32)]
pub enum LsmRejectCode {
	ManifestRace = 1,
	SidecarDigestMismatch = 2,
	VisibleDigestInvariance = 3,
}

impl LsmRejectCode {
	pub const ALL: [Self; 3] = [
		Self::ManifestRace,
		Self::SidecarDigestMismatch,
		Self::VisibleDigestInvariance,
	];

	pub const fn as_str(self) -> &'static str {
		match self {
			Self::ManifestRace => "manifest_race",
			Self::SidecarDigestMismatch => "sidecar_digest_mismatch",
			Self::VisibleDigestInvariance => "visible_digest_invariance",
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DropReason {
	Retention,
	TombstonePair,
	PkPrefix,
	MigratedRange,
	EmptyRun,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum VersionLifecycle {
	AmbiguousTable,
	AppendOnly,
	AppendOnlyContentHash,
	PrimaryKey,
	MigratedRangeTombstone,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionEntry {
	pub tenant_id: crate::schema::TenantId,
	/// Durable table attribution for the record location. `None` is a legacy or
	/// untyped row and must never be promoted to a guessed table fence.
	pub table_object_id: Option<crate::schema::TableObjectId>,
	pub row_key_id: Identity,
	pub row_hlc: HybridTimestamp,
	pub record_row_index: RecordRowIndex,
	pub location: RecordLocation,
	pub lifecycle: VersionLifecycle,
}

impl VersionEntry {
	pub fn merge_key(&self) -> CompactionMergeKey {
		CompactionMergeKey::new(self.row_hlc, self.row_key_id, self.record_row_index)
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SurvivorOrDrop {
	Survivor(VersionEntry),
	Drop {
		record: VersionEntry,
		reason: DropReason,
	},
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannedOutputRun {
	pub slab_id: SlabId,
	pub compaction_level: CompactionLevelIndex,
	pub rows: Vec<VersionEntry>,
	pub estimated_size_bytes: ByteCount,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LsmSourceFrame {
	pub entry: VersionEntry,
	/// ADR 0097: the physical frame this row resolves into, shared by every row
	/// that resolves into the same one. Rows of a multi-row frame outnumber the
	/// frames they live in - one owned copy per row made the retained working set
	/// proportional to the row count rather than the frame count, which is what
	/// took the node to 21 GiB before it reported an unsupported plan.
	pub frame: Arc<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LsmWrittenOutputRun {
	pub slab_id: SlabId,
	pub compaction_level: CompactionLevelIndex,
	pub temp_path: PathBuf,
	pub summary: LsmRunSummary,
	pub locations: Vec<RecordLocation>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VisibleDomainRow {
	pub tenant_id: TenantId,
	pub table_object_id: TableObjectId,
	pub row_key_id: Identity,
	pub row_hlc: HybridTimestamp,
	pub is_tombstone: bool,
}

pub fn compaction_visible_domain_summary(
	pre_rows: impl IntoIterator<Item = VisibleDomainRow>,
	post_rows: impl IntoIterator<Item = VisibleDomainRow>,
) -> CompactionDigestSummary {
	CompactionDigestSummary {
		visible_pre: visible_domain_rows_digest(pre_rows),
		visible_post: visible_domain_rows_digest(post_rows),
		version_entry_delta: StateDigest::ZERO,
	}
}

fn visible_domain_rows_digest(rows: impl IntoIterator<Item = VisibleDomainRow>) -> StateDigest {
	let mut latest: BTreeMap<(TenantId, TableObjectId, Identity), (HybridTimestamp, bool)> =
		BTreeMap::new();
	for row in rows {
		let key = (row.tenant_id, row.table_object_id, row.row_key_id);
		let value = (row.row_hlc, row.is_tombstone);
		match latest.get_mut(&key) {
			Some(existing) if existing.0 >= row.row_hlc => {}
			Some(existing) => *existing = value,
			None => {
				latest.insert(key, value);
			}
		}
	}
	let mut accumulator = StateDigestAccumulator::new();
	for ((tenant_id, table_object_id, row_key_id), (_row_hlc, is_tombstone)) in latest {
		if !is_tombstone {
			let element = visible_row_element(tenant_id, table_object_id, row_key_id);
			accumulator.add_element(element);
		}
	}
	accumulator.finish()
}

impl PlannedOutputRun {
	pub fn summary(&self, manifest_epoch: ManifestEpoch) -> LsmRunSummary {
		let min_row_hlc = self
			.rows
			.iter()
			.map(|row| row.row_hlc)
			.min()
			.unwrap_or_else(zero_hlc);
		let max_row_hlc = self
			.rows
			.iter()
			.map(|row| row.row_hlc)
			.max()
			.unwrap_or_else(zero_hlc);
		LsmRunSummary::new(
			self.slab_id,
			self.compaction_level,
			min_row_hlc,
			max_row_hlc,
			self.estimated_size_bytes,
			RowCount::new(self.rows.len() as u64),
			manifest_epoch,
		)
	}
}

pub fn survivor_records(outcome: &[SurvivorOrDrop]) -> Vec<VersionEntry> {
	outcome
		.iter()
		.filter_map(|entry| match entry {
			SurvivorOrDrop::Survivor(record) => Some(record.clone()),
			SurvivorOrDrop::Drop { .. } => None,
		})
		.collect()
}

pub fn plan_output_runs(
	records: impl IntoIterator<Item = VersionEntry>,
	target_level: CompactionLevelIndex,
	target_size_bytes: u64,
	output_slab_ids: impl IntoIterator<Item = SlabId>,
) -> std::io::Result<Vec<PlannedOutputRun>> {
	let splits = split_records_by_target_size(records, target_size_bytes)?;
	let mut output_slab_ids = output_slab_ids.into_iter();
	let mut out = Vec::new();
	for (rows, estimated_bytes) in splits {
		push_planned_output_run(
			&mut out,
			&mut output_slab_ids,
			target_level,
			rows,
			estimated_bytes,
		)?;
	}
	Ok(out)
}

/// The identity of one physical frame: rows of a multi-row frame share all three
/// fields and differ only in their row index and HLC. `Ord` so frame placement is
/// deterministic for a given input.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct PhysicalFrameKey {
	slab_id: SlabId,
	offset: u64,
	length: u32,
}

impl PhysicalFrameKey {
	fn of(record: &VersionEntry) -> Self {
		Self {
			slab_id: record.location.slab_id,
			offset: record.location.offset,
			length: record.location.length,
		}
	}
}

pub fn required_output_run_count(
	records: impl IntoIterator<Item = VersionEntry>,
	target_size_bytes: u64,
) -> std::io::Result<usize> {
	Ok(split_records_by_target_size(records, target_size_bytes)?.len())
}

fn split_records_by_target_size(
	records: impl IntoIterator<Item = VersionEntry>,
	target_size_bytes: u64,
) -> std::io::Result<Vec<(Vec<VersionEntry>, u64)>> {
	if target_size_bytes == 0 {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"compaction output target size must be greater than zero",
		));
	}
	let mut records = records.into_iter().collect::<Vec<_>>();
	records.sort_by_key(VersionEntry::merge_key);

	// ADR 0097: the unit of work is the PHYSICAL frame, not the logical row.
	//
	// `location.length` is the length of the containing frame, so charging it per
	// row multiplied a multi-row frame by the number of rows it holds - one
	// observed slab of 2.20 GiB in 8,428 frames and 47,050 rows planned as
	// 18.81 GiB. Charging it once per frame makes the estimate physical.
	//
	// Rows of one frame are also kept in one run. A run that took some rows of a
	// frame and left the rest forced the writer to split it, which it cannot do:
	// that is the `does not yet split multi-row frames` path, and it was reached
	// only after the whole working set had been built.
	//
	// Frames are placed in merge order of their first row, so a run stays close to
	// a contiguous slice of that order. A frame whose rows straddle a boundary
	// widens its run's HLC range rather than splitting; overlap between runs is
	// already handled by `overlapping_slabs` at pick time and is not an invariant.
	let mut rows_by_frame: BTreeMap<PhysicalFrameKey, Vec<VersionEntry>> = BTreeMap::new();
	let mut frame_order: Vec<PhysicalFrameKey> = Vec::new();
	for record in records {
		let key = PhysicalFrameKey::of(&record);
		match rows_by_frame.entry(key) {
			std::collections::btree_map::Entry::Vacant(slot) => {
				frame_order.push(key);
				slot.insert(vec![record]);
			}
			std::collections::btree_map::Entry::Occupied(mut slot) => {
				slot.get_mut().push(record);
			}
		}
	}

	let mut out: Vec<(Vec<VersionEntry>, u64)> = Vec::new();
	let mut current_rows: Vec<VersionEntry> = Vec::new();
	let mut current_bytes = 0_u64;
	for key in frame_order {
		let rows = rows_by_frame
			.remove(&key)
			.expect("every ordered frame key was inserted");
		let frame_bytes = u64::from(key.length).max(1);
		if !current_rows.is_empty() && current_bytes.saturating_add(frame_bytes) > target_size_bytes
		{
			out.push((std::mem::take(&mut current_rows), current_bytes));
			current_bytes = 0;
		}
		current_bytes = current_bytes.saturating_add(frame_bytes);
		current_rows.extend(rows);
	}
	if !current_rows.is_empty() {
		out.push((current_rows, current_bytes));
	}
	for (rows, _) in &mut out {
		rows.sort_by_key(VersionEntry::merge_key);
	}
	Ok(out)
}

fn push_planned_output_run(
	out: &mut Vec<PlannedOutputRun>,
	output_slab_ids: &mut impl Iterator<Item = SlabId>,
	target_level: CompactionLevelIndex,
	rows: Vec<VersionEntry>,
	estimated_bytes: u64,
) -> std::io::Result<()> {
	let slab_id = output_slab_ids.next().ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidInput,
			"not enough output slab IDs reserved for compaction",
		)
	})?;
	out.push(PlannedOutputRun {
		slab_id,
		compaction_level: target_level,
		rows,
		estimated_size_bytes: ByteCount::new(estimated_bytes),
	});
	Ok(())
}

/// Apply ADR 055 lifecycle elision to an in-memory compaction working set.
///
/// The streaming-bounded contract is enforced by callers that feed this helper
/// from slab views; this function intentionally materializes and groups one
/// compaction plan's version entries.
pub fn apply_elision(
	records: impl IntoIterator<Item = VersionEntry>,
	retention_floor: HybridTimestamp,
) -> Vec<SurvivorOrDrop> {
	let mut ordered = records.into_iter().collect::<Vec<_>>();
	ordered.sort_by_key(VersionEntry::merge_key);
	let mut drop_reasons = vec![None; ordered.len()];
	let mut groups: BTreeMap<(crate::schema::TenantId, Identity, VersionLifecycle), Vec<usize>> =
		BTreeMap::new();
	for (idx, record) in ordered.iter().enumerate() {
		groups
			.entry((record.tenant_id, record.row_key_id, record.lifecycle))
			.or_default()
			.push(idx);
	}

	for ((_, _, lifecycle), indices) in groups {
		match lifecycle {
			VersionLifecycle::AmbiguousTable => {}
			VersionLifecycle::MigratedRangeTombstone => {
				for idx in indices {
					if below_floor(&ordered[idx], retention_floor) {
						drop_reasons[idx] = Some(DropReason::MigratedRange);
					}
				}
			}
			VersionLifecycle::AppendOnly | VersionLifecycle::AppendOnlyContentHash => {
				apply_append_only_elision(&ordered, &indices, retention_floor, &mut drop_reasons);
			}
			VersionLifecycle::PrimaryKey => {
				apply_primary_key_elision(&ordered, &indices, retention_floor, &mut drop_reasons);
			}
		}
	}

	ordered
		.into_iter()
		.zip(drop_reasons)
		.map(|(record, reason)| match reason {
			Some(reason) => SurvivorOrDrop::Drop { record, reason },
			None => SurvivorOrDrop::Survivor(record),
		})
		.collect()
}

fn apply_append_only_elision(
	ordered: &[VersionEntry],
	indices: &[usize],
	retention_floor: HybridTimestamp,
	drop_reasons: &mut [Option<DropReason>],
) {
	for idx in indices {
		let record = &ordered[*idx];
		if !below_floor(record, retention_floor) {
			continue;
		}
		if record.location.is_tombstone {
			drop_reasons[*idx] = Some(DropReason::TombstonePair);
			continue;
		}
		let newer_tombstone = indices.iter().copied().find(|other_idx| {
			let other = &ordered[*other_idx];
			other.location.is_tombstone && other.row_hlc > record.row_hlc
		});
		drop_reasons[*idx] = match newer_tombstone {
			Some(tombstone_idx) if !below_floor(&ordered[tombstone_idx], retention_floor) => None,
			Some(_) => Some(DropReason::TombstonePair),
			None => Some(DropReason::Retention),
		};
	}
}

fn apply_primary_key_elision(
	ordered: &[VersionEntry],
	indices: &[usize],
	retention_floor: HybridTimestamp,
	drop_reasons: &mut [Option<DropReason>],
) {
	let boundary = indices
		.iter()
		.copied()
		.filter(|idx| below_floor(&ordered[*idx], retention_floor))
		.max_by_key(|idx| ordered[*idx].merge_key());
	for idx in indices {
		if Some(*idx) != boundary && below_floor(&ordered[*idx], retention_floor) {
			drop_reasons[*idx] = Some(DropReason::PkPrefix);
		}
	}
}

fn below_floor(record: &VersionEntry, retention_floor: HybridTimestamp) -> bool {
	record.row_hlc < retention_floor
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SidecarSwap {
	pub source_slab_id: SlabId,
	pub output_slab_id: SlabId,
	pub old_sidecar_digest: [u8; 32],
	pub new_sidecar_digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionDigestSummary {
	pub visible_pre: StateDigest,
	pub visible_post: StateDigest,
	pub version_entry_delta: StateDigest,
}

impl CompactionDigestSummary {
	pub const fn zero() -> Self {
		Self {
			visible_pre: StateDigest::ZERO,
			visible_post: StateDigest::ZERO,
			version_entry_delta: StateDigest::ZERO,
		}
	}
}

/// Durable control-ring payload for LSM manifest transitions.
///
/// V1 accepts two disjoint shapes:
///
/// - regular compaction commits retire live input runs and add output runs via
///   `slabs_to_retire` / `slabs_to_add` with optional prepared sidecar swaps;
/// - GC lifecycle commits advance already-retired runs through
///   `slabs_to_mark_delete_pending` or remove delete-pending runs through
///   `slabs_to_remove`.
///
/// Mixed regular-compaction and GC lifecycle fields are rejected during
/// manifest validation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionCommit {
	/// The one store this whole commit belongs to (ADR 0697). Every slab id,
	/// successor, sidecar reference, job id and manifest epoch below is local
	/// to it; the ring never applies them to any other store.
	pub owner: crate::consensus::OwnerRef,
	pub job_id: CompactionJobId,
	pub manifest_epoch_pre: ManifestEpoch,
	pub slabs_to_retire: Vec<SlabId>,
	pub slabs_to_add: Vec<LsmRunSummary>,
	#[serde(default)]
	pub slabs_to_mark_delete_pending: Vec<SlabId>,
	#[serde(default)]
	pub slabs_to_remove: Vec<SlabId>,
	pub sidecar_swaps: Vec<SidecarSwap>,
	pub digest_summary: CompactionDigestSummary,
}

impl CompactionCommit {
	pub fn gc_mark_delete_pending(
		owner: crate::consensus::OwnerRef,
		job_id: CompactionJobId,
		manifest_epoch_pre: ManifestEpoch,
		mut slabs_to_mark_delete_pending: Vec<SlabId>,
	) -> Self {
		slabs_to_mark_delete_pending.sort_unstable();
		Self {
			owner,
			job_id,
			manifest_epoch_pre,
			slabs_to_retire: Vec::new(),
			slabs_to_add: Vec::new(),
			slabs_to_mark_delete_pending,
			slabs_to_remove: Vec::new(),
			sidecar_swaps: Vec::new(),
			digest_summary: CompactionDigestSummary::zero(),
		}
	}

	pub fn gc_remove(
		owner: crate::consensus::OwnerRef,
		job_id: CompactionJobId,
		manifest_epoch_pre: ManifestEpoch,
		mut slabs_to_remove: Vec<SlabId>,
	) -> Self {
		slabs_to_remove.sort_unstable();
		Self {
			owner,
			job_id,
			manifest_epoch_pre,
			slabs_to_retire: Vec::new(),
			slabs_to_add: Vec::new(),
			slabs_to_mark_delete_pending: Vec::new(),
			slabs_to_remove,
			sidecar_swaps: Vec::new(),
			digest_summary: CompactionDigestSummary::zero(),
		}
	}

	pub fn is_lsm_gc_lifecycle_commit(&self) -> bool {
		(!self.slabs_to_mark_delete_pending.is_empty() || !self.slabs_to_remove.is_empty())
			&& self.slabs_to_retire.is_empty()
			&& self.slabs_to_add.is_empty()
			&& self.sidecar_swaps.is_empty()
	}

	pub fn has_lsm_gc_lifecycle_fields(&self) -> bool {
		!self.slabs_to_mark_delete_pending.is_empty() || !self.slabs_to_remove.is_empty()
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionOutcome {
	pub job_id: CompactionJobId,
	pub manifest_epoch_pre: ManifestEpoch,
	pub manifest_epoch_post: ManifestEpoch,
	pub output_slabs: Vec<LsmRunSummary>,
	pub bytes_in: u64,
	pub bytes_out: u64,
	pub rows_in: u64,
	pub rows_out: u64,
	pub elided_versions: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum CompactionFailure {
	#[error("manifest epoch changed before compaction commit")]
	ManifestRace,
	#[error("multi-source compaction deferred because a touched slab has counter frames")]
	CounterMaintenanceSkipped,
	#[error("rebuilt secondary-index sidecar digest did not match cold rebuild")]
	SidecarDigestMismatch,
	#[error("visible-domain state digest changed across compaction")]
	VisibleDigestInvariance,
	#[error("secondary-index backfill won the source-slab race")]
	BackfillRaceLost,
	#[error(transparent)]
	IoError(#[from] std::io::Error),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LsmConfig {
	pub max_l0_slab_count: u32,
	pub lsm_l0_max_age_millis: u64,
	pub lsm_level_count: u8,
	pub lsm_size_ratio: u32,
	pub lsm_l1_slab_size_target_bytes: u64,
	pub max_l1_slab_count: u32,
	pub lsm_picker_tick_millis: u64,
	pub lsm_gc_tick_millis: u64,
	pub lsm_compaction_concurrency_per_locality: u32,
	pub lsm_digest_verify_sample_rate: f32,
}

impl Default for LsmConfig {
	fn default() -> Self {
		Self {
			max_l0_slab_count: 4,
			lsm_l0_max_age_millis: 5_000,
			lsm_level_count: 4,
			lsm_size_ratio: 10,
			lsm_l1_slab_size_target_bytes: 64 << 20,
			max_l1_slab_count: 4,
			lsm_picker_tick_millis: 100,
			lsm_gc_tick_millis: 1_000,
			lsm_compaction_concurrency_per_locality: 1,
			lsm_digest_verify_sample_rate: 1.0,
		}
	}
}

impl LsmConfig {
	pub fn validate(self) -> std::io::Result<()> {
		if self.lsm_level_count != 4 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"lsm_level_count must be exactly 4 for ADR 055 V1",
			));
		}
		if self.lsm_size_ratio == 0
			|| self.lsm_l1_slab_size_target_bytes == 0
			|| self.max_l0_slab_count == 0
			|| self.max_l1_slab_count == 0
			|| self.lsm_compaction_concurrency_per_locality != 1
			|| !(0.0..=1.0).contains(&self.lsm_digest_verify_sample_rate)
		{
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"invalid ADR 055 LSM configuration",
			));
		}
		Ok(())
	}

	fn level_target_bytes(self, source_level: CompactionLevelIndex) -> Option<u64> {
		match source_level {
			CompactionLevelIndex::L1 => Some(
				self.lsm_l1_slab_size_target_bytes
					.saturating_mul(u64::from(self.max_l1_slab_count)),
			),
			CompactionLevelIndex::L2 => Some(
				self.lsm_l1_slab_size_target_bytes
					.saturating_mul(u64::from(self.max_l1_slab_count))
					.saturating_mul(u64::from(self.lsm_size_ratio)),
			),
			_ => None,
		}
	}

	pub fn output_run_target_bytes(self, target_level: CompactionLevelIndex) -> Option<u64> {
		match target_level {
			CompactionLevelIndex::L1 => Some(self.lsm_l1_slab_size_target_bytes),
			CompactionLevelIndex::L2 => Some(
				self.lsm_l1_slab_size_target_bytes
					.saturating_mul(u64::from(self.lsm_size_ratio)),
			),
			CompactionLevelIndex::L3 => Some(
				self.lsm_l1_slab_size_target_bytes
					.saturating_mul(u64::from(self.lsm_size_ratio))
					.saturating_mul(u64::from(self.lsm_size_ratio)),
			),
			_ => None,
		}
	}
}

pub fn pick_compaction(
	manifest: &ManifestSnapshot,
	config: &LsmConfig,
	now: HybridTimestamp,
) -> Option<CompactionPlan> {
	pick_compaction_where(manifest, config, now, |_| true)
}

/// ADR 0097 decision 4: choose the first candidate the caller will actually accept.
///
/// Selection skips ineligible candidates WHILE searching rather than choosing one
/// and then rejecting it. Filtering after the choice lets a single blocked
/// candidate - one in cooldown, or quarantined - suppress every other level's
/// eligible work, which turns a bounded backoff on one slab into a stall of all
/// compaction.
///
/// The granularity is the candidate each level produces: if a level's one
/// candidate is ineligible this moves to the next level, and it does not search
/// within a level for a different plan there.
pub fn pick_compaction_where(
	manifest: &ManifestSnapshot,
	config: &LsmConfig,
	now: HybridTimestamp,
	eligible: impl Fn(&CompactionPlan) -> bool,
) -> Option<CompactionPlan> {
	pick_compaction_where_excluding(manifest, config, now, |_| false, eligible)
}

/// Variant used when a source-level plan has a known deferred input. L0 plans
/// can omit that input while retaining the configured minimum run count; other
/// levels continue through their normal eligibility filter.
pub(crate) fn pick_compaction_where_excluding(
	manifest: &ManifestSnapshot,
	config: &LsmConfig,
	now: HybridTimestamp,
	excluded_l0_slab: impl Fn(SlabId) -> bool,
	eligible: impl Fn(&CompactionPlan) -> bool,
) -> Option<CompactionPlan> {
	config.validate().ok()?;
	pick_l0_compaction(manifest, config, now, &excluded_l0_slab)
		.filter(&eligible)
		.or_else(|| {
			pick_level_pressure_compaction(
				manifest,
				config,
				now,
				CompactionLevelIndex::L1,
				&eligible,
			)
		})
		.or_else(|| {
			pick_level_pressure_compaction(
				manifest,
				config,
				now,
				CompactionLevelIndex::L2,
				&eligible,
			)
		})
}

fn pick_l0_compaction(
	manifest: &ManifestSnapshot,
	config: &LsmConfig,
	now: HybridTimestamp,
	excluded: &impl Fn(SlabId) -> bool,
) -> Option<CompactionPlan> {
	let mut l0 = manifest.slabs_at_compaction_level(CompactionLevelIndex::L0);
	l0.retain(|run| !excluded(run.slab_id));
	if l0.len() < config.max_l0_slab_count as usize {
		return None;
	}
	let oldest_age = manifest.oldest_l0_slab_age_millis(now)?;
	if oldest_age < config.lsm_l0_max_age_millis {
		return None;
	}
	l0.sort_by_key(|run| (run.min_row_hlc, run.slab_id));
	let min_hlc = l0.iter().map(|run| run.min_row_hlc).min()?;
	let max_hlc = l0.iter().map(|run| run.max_row_hlc).max()?;
	let overlaps = manifest.overlapping_slabs(CompactionLevelIndex::L1, min_hlc, max_hlc);
	Some(CompactionPlan {
		job_id: job_id_for(manifest, CompactionLevelIndex::L0),
		source_level: CompactionLevelIndex::L0,
		target_level: CompactionLevelIndex::L1,
		manifest_epoch_pre: manifest.manifest_epoch,
		input_slabs: l0.into_iter().map(|run| run.slab_id).collect(),
		overlapping_slabs: overlaps.into_iter().map(|run| run.slab_id).collect(),
		retention_floor_snapshot: zero_hlc(),
		compaction_start_hlc: now,
		dominant_reason: DominantReason::LevelSize,
	})
}

fn pick_level_pressure_compaction(
	manifest: &ManifestSnapshot,
	config: &LsmConfig,
	now: HybridTimestamp,
	source_level: CompactionLevelIndex,
	eligible: &impl Fn(&CompactionPlan) -> bool,
) -> Option<CompactionPlan> {
	let target_bytes = config.level_target_bytes(source_level)?;
	let total_bytes = manifest.compaction_level_total_bytes(source_level);
	if total_bytes <= target_bytes {
		return None;
	}
	let mut candidates = manifest.slabs_at_compaction_level(source_level);
	candidates.sort_by_key(|run| {
		(
			Reverse(run.size_bytes.get()),
			Reverse(run.retention_debt_score),
			Reverse(run.tombstone_debt_score),
			Reverse(run.index_coverage_debt_score),
			Reverse(run.layout_debt_score),
			run.slab_id,
		)
	});
	let target_level = source_level.next()?;
	// ADR 0097 decision 4: walk the ranked sources and return the first the caller
	// will accept, rather than offering only the top-ranked one.
	//
	// A single quarantined run must not starve its level. The ranking here is
	// size-dominant, so the largest run is offered first - and a large run is
	// exactly the kind that hits a shape-dependent rewrite failure and stays
	// quarantined. Returning only that one turned a bounded quarantine on one piece
	// of work into an unbounded stall of every other run at the level.
	//
	// Eligibility is applied to the WHOLE candidate, overlaps included, because a
	// plan is ineligible if any slab it touches is.
	candidates.into_iter().find_map(|source| {
		let overlaps =
			manifest.overlapping_slabs(target_level, source.min_row_hlc, source.max_row_hlc);
		let plan = CompactionPlan {
			job_id: job_id_for(manifest, source_level),
			source_level,
			target_level,
			manifest_epoch_pre: manifest.manifest_epoch,
			input_slabs: vec![source.slab_id],
			overlapping_slabs: overlaps.into_iter().map(|run| run.slab_id).collect(),
			retention_floor_snapshot: zero_hlc(),
			compaction_start_hlc: now,
			dominant_reason: source.dominant_reason(true),
		};
		eligible(&plan).then_some(plan)
	})
}

fn job_id_for(manifest: &ManifestSnapshot, source_level: CompactionLevelIndex) -> CompactionJobId {
	CompactionJobId::new(
		manifest
			.manifest_epoch
			.get()
			.saturating_mul(4)
			.saturating_add(u64::from(source_level.get()))
			.saturating_add(1),
	)
}

const fn zero_hlc() -> HybridTimestamp {
	HybridTimestamp {
		physical_ms: 0,
		logical: 0,
		node_id: 0,
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn hlc(ms: u64) -> HybridTimestamp {
		HybridTimestamp {
			physical_ms: ms,
			logical: 0,
			node_id: 0,
		}
	}

	fn digest_row(row_byte: u8, row_hlc: HybridTimestamp, is_tombstone: bool) -> VisibleDomainRow {
		VisibleDomainRow {
			tenant_id: TenantId::ZERO,
			table_object_id: TableObjectId::DEFAULT_KV,
			row_key_id: Identity::new([row_byte; 32]),
			row_hlc,
			is_tombstone,
		}
	}

	#[test]
	fn config_defaults_match_adr_055() {
		let config = LsmConfig::default();
		assert_eq!(config.max_l0_slab_count, 4);
		assert_eq!(config.lsm_l0_max_age_millis, 5_000);
		assert_eq!(config.lsm_level_count, 4);
		assert_eq!(config.lsm_size_ratio, 10);
		assert_eq!(config.lsm_l1_slab_size_target_bytes, 64 << 20);
		assert_eq!(config.max_l1_slab_count, 4);
		assert_eq!(config.lsm_picker_tick_millis, 100);
		assert_eq!(config.lsm_gc_tick_millis, 1_000);
		assert_eq!(config.lsm_compaction_concurrency_per_locality, 1);
		assert_eq!(config.lsm_digest_verify_sample_rate, 1.0);
	}

	#[test]
	fn compaction_merge_key_sorts_by_hlc_key_and_row_index() {
		let key_a = Identity::new([1; 32]);
		let key_b = Identity::new([2; 32]);
		let mut keys = [
			CompactionMergeKey::new(hlc(2), key_a, RecordRowIndex::ZERO),
			CompactionMergeKey::new(hlc(1), key_b, RecordRowIndex::ZERO),
			CompactionMergeKey::new(hlc(1), key_a, RecordRowIndex::new(2)),
			CompactionMergeKey::new(hlc(1), key_a, RecordRowIndex::new(1)),
		];
		keys.sort();
		assert_eq!(
			keys[0],
			CompactionMergeKey::new(hlc(1), key_a, RecordRowIndex::new(1))
		);
		assert_eq!(
			keys[1],
			CompactionMergeKey::new(hlc(1), key_a, RecordRowIndex::new(2))
		);
		assert_eq!(
			keys[2],
			CompactionMergeKey::new(hlc(1), key_b, RecordRowIndex::ZERO)
		);
		assert_eq!(
			keys[3],
			CompactionMergeKey::new(hlc(2), key_a, RecordRowIndex::ZERO)
		);
	}

	#[test]
	fn visible_domain_summary_is_nonzero_and_equal_for_survivor_rewrite() {
		let rows = vec![digest_row(1, hlc(10), false), digest_row(2, hlc(11), false)];
		let summary = compaction_visible_domain_summary(rows.clone(), rows);

		assert_eq!(summary.visible_pre, summary.visible_post);
		assert_ne!(
			summary.visible_pre,
			CompactionDigestSummary::zero().visible_pre
		);
	}

	#[test]
	fn visible_domain_summary_uses_latest_version_per_row() {
		let old = digest_row(1, hlc(10), false);
		let new = digest_row(1, hlc(20), false);
		let summary = compaction_visible_domain_summary([old, new], [new]);

		assert_eq!(summary.visible_pre, summary.visible_post);
	}

	#[test]
	fn visible_domain_summary_detects_latest_visible_drop() {
		let row = digest_row(1, hlc(10), false);
		let summary = compaction_visible_domain_summary([row], []);

		assert_ne!(summary.visible_pre, summary.visible_post);
	}

	#[test]
	fn visible_domain_summary_treats_latest_tombstone_as_absent() {
		let data = digest_row(1, hlc(10), false);
		let tombstone = digest_row(1, hlc(20), true);
		let summary = compaction_visible_domain_summary([data, tombstone], []);

		assert_eq!(summary.visible_pre, summary.visible_post);
		assert_eq!(
			summary.visible_pre,
			CompactionDigestSummary::zero().visible_pre
		);
	}

	#[test]
	fn visible_domain_summary_detects_tombstone_drop_that_resurrects_data() {
		let data = digest_row(1, hlc(10), false);
		let tombstone = digest_row(1, hlc(20), true);
		let summary = compaction_visible_domain_summary([data, tombstone], [data]);

		assert_ne!(summary.visible_pre, summary.visible_post);
	}
}
