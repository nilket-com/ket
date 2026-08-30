use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::distributed::budget::{ByteCount, TaskBudget};
use crate::distributed::context::TaskExecutionContext;
use crate::distributed::identity::{
	AttemptId, ExchangeId, PartitionId, QueryId, StageId, TaskId, TaskIdentity, WorkerId,
};
use crate::distributed::worker_registry::WorkerEndpoint;
use crate::ir::{BoundExprProgram, ExecutionPlan, JoinReplayId, JoinStrategy, TableId};
use crate::phase_ledger::LedgerFaults;
use crate::server::spill::SpillId;
use wendelon::schema::{
	PhysicalDescriptorFingerprint, RuntimeFilterDescriptor, RuntimeFilterKeyType, RuntimeFilterKind,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ArrowIpcDigest(pub [u8; 32]);

impl ArrowIpcDigest {
	pub const fn new(value: [u8; 32]) -> Self {
		Self(value)
	}

	pub const fn get(self) -> [u8; 32] {
		self.0
	}
}

#[derive(
	Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct RowCount(pub u64);

impl RowCount {
	pub const fn new(value: u64) -> Self {
		Self(value)
	}

	pub const fn get(self) -> u64 {
		self.0
	}
}

#[derive(
	Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct PartitionCount(pub u32);

impl PartitionCount {
	pub const fn new(value: u32) -> Self {
		Self(value)
	}

	pub const fn get(self) -> u32 {
		self.0
	}
}

#[derive(
	Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct AttemptLimit(pub u32);

impl AttemptLimit {
	pub const fn new(value: u32) -> Self {
		Self(value)
	}

	pub const fn get(self) -> u32 {
		self.0
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TraceContext {
	pub trace_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskTelemetry {
	pub labels: TaskTelemetryLabels,
	pub counters: TaskTelemetryCounters,
	pub phases: TaskTelemetryPhases,
	/// Whether ADR 551's exclusive phase ledger produced an authoritative
	/// partition for this task. A withheld receipt carries its typed cause; its
	/// zero phase counters must never be interpreted as observed absence.
	#[serde(default)]
	pub phase_receipt: TaskPhaseReceipt,
	pub join_observations: Vec<JoinRuntimeObservation>,
	#[serde(default)]
	pub join_build_profiles: Vec<JoinBuildProfile>,
	#[serde(default)]
	pub existence_summary_profiles: Vec<ExistenceSummaryProfile>,
	/// Per-join attribution for the global-aggregate hash-join execution path.
	/// This is separate from the task-wide spill counter so a stable total cannot
	/// conceal a change in the runtime path that produced it.
	#[serde(default)]
	pub join_spill_observations: Vec<JoinSpillObservation>,
	/// ADR 527: requested and effective concurrency facts for the one executor path that consumes
	/// `ExecutionContext::partition_parallelism`. Absence means the task never reached that path;
	/// it must not be interpreted as sequential execution.
	#[serde(default)]
	pub partition_work_concurrency_observations: Vec<PartitionWorkConcurrencyObservation>,
	/// Decode time the dispatcher spent on this task's streamed result batches, measured on
	/// the dispatcher's thread while the task's wall was still running. Another timeline
	/// entirely, so it is retained here as raw evidence and is deliberately NOT a phase:
	/// phase counters partition the task's own wall, and folding a second timeline into
	/// them counts the transfer window at both ends.
	#[serde(default)]
	pub dispatcher_batch_decode_micros: u64,
	/// Observe-only routing receipts: what a producer wrote into each shuffle partition,
	/// and what a consumer actually received from each upstream. Behavior-neutral; they
	/// exist so a divergence can be localized to a seam instead of inferred from a plan.
	#[serde(default)]
	pub shuffle_routing_observations: Vec<ShuffleRoutingObservation>,
	/// ADR 554 follow-on: run-length structure of join output selections, per side,
	/// aggregated over this task. Zero invocations means no join gathered here.
	#[serde(default)]
	pub join_selection_locality: JoinSelectionLocalitySides,
	/// ADR 580: the shape of what each side handed to its input concatenation,
	/// per invocation rather than only in total.
	#[serde(default)]
	pub join_input_shape: JoinInputShapeSides,
}

/// Run-length structure of one side of a join's output selection, accumulated
/// across every invocation on a task.
///
/// ADR 554 follow-on. The output gather costs 3.8 ns/row at sequential indices
/// and 37.8 ns/row at scattered ones (bench `join_output_gather_shape`), a 5-10x
/// spread — so whether contiguous-slice reuse is worth a redesign depends
/// entirely on the run structure of real selections, which no artifact records
/// today.
///
/// **Buckets, not a mean.** A mean run length cannot answer the question: the
/// useful threshold depends on per-slice and per-batch overhead, which is not
/// known in advance, and a mean hides a bimodal distribution of long runs mixed
/// with scatter. `rows_in_runs_at_least` is indexed by
/// [`RUN_LENGTH_BUCKET_MINIMUMS`], and a row inside a run of length L counts in
/// every bucket whose minimum is <= L.
///
/// **Aggregated task-locally on purpose.** One record per invocation would put
/// millions of rows of receipts in an artifact to answer a question that twenty
/// counters answer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinSelectionLocality {
	/// Gathers of this side, whether or not it received any index. `kind` decides
	/// participation: a LeftSemi selection never touches the right side, so the
	/// right side records nothing. Zero means this side was never gathered — an
	/// absence, never a claim that its indices were scattered.
	pub invocations: u64,
	/// Invocations where this side contributed no index at all. An outer join
	/// whose rows all null this side is the real case. Without this counter,
	/// `selected_rows == 0` could not be told apart from a side the gather never
	/// touched — the two have opposite meanings for a locality reading.
	pub empty_invocations: u64,
	/// Index positions contributed by this side, summed across invocations. A
	/// `FillNull` row contributes nothing on the side it nulls.
	pub selected_rows: u64,
	/// Steps where the index advanced by exactly one. The `+1` fraction against
	/// `selected_rows` is the headline locality number.
	pub ascending_steps: u64,
	/// Steps that repeated the previous index. Duplicates are what a
	/// many-to-one join produces; they are neither a run nor scatter, so they are
	/// counted apart rather than folded into either.
	pub duplicate_steps: u64,
	/// Maximal ascending-by-one sequences, counting single rows as runs of one.
	pub run_count: u64,
	/// Longest such sequence. A large maximum beside a low `+1` fraction is the
	/// bimodal case a mean would have hidden.
	pub max_run: u64,
	/// Rows inside runs of at least `RUN_LENGTH_BUCKET_MINIMUMS[i]`.
	pub rows_in_runs_at_least: [u64; RUN_LENGTH_BUCKETS],
}

/// Run-length thresholds for [`JoinSelectionLocality::rows_in_runs_at_least`].
pub const RUN_LENGTH_BUCKET_MINIMUMS: [u64; RUN_LENGTH_BUCKETS] = [2, 4, 8, 16, 32, 64];

pub const RUN_LENGTH_BUCKETS: usize = 6;

impl JoinSelectionLocality {
	/// Fold one completed run into the buckets.
	pub fn observe_run(&mut self, run: u64) {
		if run == 0 {
			return;
		}
		self.run_count = self.run_count.saturating_add(1);
		self.max_run = self.max_run.max(run);
		for (bucket, minimum) in RUN_LENGTH_BUCKET_MINIMUMS.iter().enumerate() {
			if run >= *minimum {
				self.rows_in_runs_at_least[bucket] =
					self.rows_in_runs_at_least[bucket].saturating_add(run);
			}
		}
	}

	/// Merge another accumulation into this one. Used to combine invocations.
	pub fn merge(&mut self, other: &Self) {
		self.invocations = self.invocations.saturating_add(other.invocations);
		self.empty_invocations = self
			.empty_invocations
			.saturating_add(other.empty_invocations);
		self.selected_rows = self.selected_rows.saturating_add(other.selected_rows);
		self.ascending_steps = self.ascending_steps.saturating_add(other.ascending_steps);
		self.duplicate_steps = self.duplicate_steps.saturating_add(other.duplicate_steps);
		self.run_count = self.run_count.saturating_add(other.run_count);
		self.max_run = self.max_run.max(other.max_run);
		for (bucket, rows) in other.rows_in_runs_at_least.iter().enumerate() {
			self.rows_in_runs_at_least[bucket] =
				self.rows_in_runs_at_least[bucket].saturating_add(*rows);
		}
	}
}

/// Both sides of a join's selection, kept apart.
///
/// The right (probe) side is the one a run-batch emission strategy would
/// exploit: `probe_in_memory_join_rows` walks probe rows in order. The left
/// (build) side is the control — hash lookup should scatter it, and a left side
/// that came back sequential would mean the probe order, not the gather, is
/// what these numbers describe.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinSelectionLocalitySides {
	pub left: JoinSelectionLocality,
	pub right: JoinSelectionLocality,
}

/// Per-invocation shape of a join input concatenation, accumulated over a task.
///
/// ADR 580. Totals alone cannot decide that record's gate: a task reporting 400
/// batches could be one 400-batch concat or 400 single-batch ones, which point at
/// opposite projects. `concats` and `concats_at_least` give the distribution the
/// totals cannot, and the timing children say which shape the time belongs to.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinInputShape {
	/// Concatenations of this side, including empty ones.
	pub concats: u64,
	/// Concatenations that received no batch at all. Held apart so a zero in the
	/// `>= 1` bucket cannot be read as "this side was never concatenated".
	pub empty_concats: u64,
	pub batches: u64,
	pub rows: u64,
	/// Slice bytes, never allocation bytes: shuffle and scan inputs are routinely
	/// slices of a larger buffer, and the allocation is not what a concat copies.
	pub bytes: u64,
	/// Concatenations whose batch count reached
	/// `INPUT_BATCH_COUNT_BUCKET_MINIMUMS[i]`. Buckets, not a mean: the decision is
	/// whether multi-batch inputs exist and carry the time, and a mean of 400 and
	/// 1 hides both answers.
	pub concats_at_least: [u64; INPUT_BATCH_COUNT_BUCKETS],
}

/// Batch-count thresholds for [`JoinInputShape::concats_at_least`].
pub const INPUT_BATCH_COUNT_BUCKET_MINIMUMS: [u64; INPUT_BATCH_COUNT_BUCKETS] =
	[1, 2, 4, 8, 16, 64];

pub const INPUT_BATCH_COUNT_BUCKETS: usize = 6;

impl JoinInputShape {
	/// Fold one concatenation into this side.
	pub fn observe(&mut self, batches: u64, rows: u64, bytes: u64) {
		self.concats = self.concats.saturating_add(1);
		if batches == 0 {
			self.empty_concats = self.empty_concats.saturating_add(1);
		}
		self.batches = self.batches.saturating_add(batches);
		self.rows = self.rows.saturating_add(rows);
		self.bytes = self.bytes.saturating_add(bytes);
		for (bucket, minimum) in INPUT_BATCH_COUNT_BUCKET_MINIMUMS.iter().enumerate() {
			if batches >= *minimum {
				self.concats_at_least[bucket] = self.concats_at_least[bucket].saturating_add(1);
			}
		}
	}
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinInputShapeSides {
	pub left: JoinInputShape,
	pub right: JoinInputShape,
}

/// Publication state for ADR 551's task-local phase partition.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskPhaseReceipt {
	/// No phase ledger was enabled. This is not a zero-valued receipt.
	#[default]
	NotCollected,
	/// The ledger identity and acquisition reconciliation both held.
	Sealed,
	/// Instrumentation ran, but a named fault vetoed numeric attribution.
	Withheld(LedgerFaults),
}

/// A runtime filter's probe target, proven at fragmentation rather than reconstructed.
///
/// All three facts describe the same physical column: the scan that produces it, its
/// ordinal *in that scan*, and the key type. A descriptor built from this cannot pair a
/// fingerprint and an ordinal drawn from different schemas.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeFilterProbeTarget {
	pub probe_descriptor_fingerprint: PhysicalDescriptorFingerprint,
	pub physical_column_ordinal: u32,
	pub key_type: RuntimeFilterKeyType,
}

/// Whether a task records shuffle routing receipts. Off by default, and a task that
/// decodes without the field stays off — instrumentation must never switch itself on.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShuffleRoutingObservationMode {
	#[default]
	Disabled,
	Enabled,
}

/// Which side of a shuffle edge a receipt describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShuffleRoutingRole {
	Producer,
	Consumer,
}

/// One shuffle edge endpoint, recorded where the rows actually crossed it.
///
/// `key_fingerprint` is an order-independent digest of the evaluated join-key values in
/// the rows at this endpoint, so two endpoints carrying the same key set agree regardless
/// of row order or batching. `None` means no key program applied at this endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShuffleRoutingObservation {
	pub role: ShuffleRoutingRole,
	pub exchange_id: ExchangeId,
	pub stage_id: StageId,
	pub partition_id: u32,
	pub row_count: u64,
	pub schema_fingerprint: u64,
	pub key_fingerprint: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinSpillPath {
	PartitionedStreamingScanBuild,
	StreamingScanBuild,
	StreamedSpilledBuild,
	EstimateSpillFallback,
	BuildSpillFallback,
	InMemory,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinSpillObservation {
	pub replay_id: JoinReplayId,
	pub path: JoinSpillPath,
	pub spill_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionWorkConcurrencyPath {
	PartitionedStreamingScanBuild,
}

/// One runtime-scoped capability receipt. `planned_k` is the executor's bounded semaphore width,
/// not a query-wide thread count. Work-item totals cover the complete lifetime of that runtime and
/// reconcile even when it takes the sequential fallback.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionWorkConcurrencyObservation {
	pub replay_id: JoinReplayId,
	pub path: PartitionWorkConcurrencyPath,
	pub partition_count: u64,
	pub requested_concurrency: u64,
	pub available_parallelism: u64,
	pub planned_k: u64,
	pub resident_peak_bytes: u64,
	pub resident_budget_bytes: u64,
	pub work_items_started: u64,
	pub work_items_completed: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinRuntimeObservation {
	pub replay_id: JoinReplayId,
	pub strategy: JoinStrategy,
	pub build_rows: RowCount,
	pub probe_rows: RowCount,
	pub output_rows: RowCount,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinBuildProfile {
	pub replay_id: JoinReplayId,
	pub strategy: JoinStrategy,
	pub key_plan: String,
	pub build_rows: RowCount,
	pub unique_keys: RowCount,
	pub duplicate_keys: RowCount,
	pub overflow_rows: RowCount,
	pub max_rows_per_key: RowCount,
	pub row_list_value_bytes: u64,
	pub overflow_heap_capacity_bytes: u64,
	pub build_micros: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExistenceSummaryProfile {
	pub replay_id: JoinReplayId,
	pub build_rows: RowCount,
	pub unique_keys: RowCount,
	pub multi_keys: RowCount,
	pub entry_capacity: u64,
	pub bucket_capacity: u64,
	pub accounted_bytes: u64,
	pub baseline_bytes: u64,
	pub peak_combined_bytes: u64,
	pub key_count_hint: Option<u64>,
	pub page_reads: u64,
	pub build_micros: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinRuntimeObservationMode {
	#[default]
	Disabled,
	MeasureJoins {
		replay_ids: BTreeSet<JoinReplayId>,
	},
}

impl JoinRuntimeObservationMode {
	pub fn measures(&self, replay_id: JoinReplayId) -> bool {
		match self {
			Self::Disabled => false,
			Self::MeasureJoins { replay_ids } => replay_ids.contains(&replay_id),
		}
	}

	pub fn is_disabled(&self) -> bool {
		matches!(self, Self::Disabled)
	}
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskTelemetryLabels {
	pub strategy: Option<String>,
	pub stage_role: String,
	pub operator_kind: String,
	pub worker_count: Option<u32>,
	/// ADR 554 follow-on: every streaming route this task COMMITTED, by the
	/// vocabulary name `commit_stage_vocabulary` publishes. Task-local on purpose:
	/// the node-attribution ledger is one process-global stack that concurrent
	/// in-process workers interleave on, so it cannot say which task ran what.
	/// This rides the task's own telemetry, which the atlas already stamps with
	/// query, stage, task, arm and attempt.
	///
	/// Deduplicated and bounded. Read `vocabulary_overflow` before treating this
	/// as the complete set.
	///
	/// `serde(default)` like every other added telemetry field: `TaskTelemetry`
	/// crosses the worker/coordinator boundary, so a peer that predates this field
	/// must still deserialize.
	#[serde(default)]
	pub committed_vocabularies: Vec<String>,
	/// Distinct route names dropped because the bound was reached. **Any non-zero
	/// value means `committed_vocabularies` is a partial set**, so a reader can
	/// never mistake a truncated list for complete coverage. Repeats of an
	/// already-recorded name are not overflow.
	#[serde(default)]
	pub vocabulary_overflow: u32,
	/// The task-level dispatch arm that took this task, from the if/else chain in
	/// `server::execute_task_local_with_telemetry_and_stream_sink`. Six arms exist
	/// (three `drive_streamable_plan` shapes, the live shuffle producer, the live
	/// shuffle consumer, and three `execute_bound` shapes); the chain is
	/// single-shot, so an ordinary task records exactly one.
	///
	/// Kept separate from `executed_node_kinds` on purpose. This says which
	/// *driver* ran the fragment; that says which plan nodes the driver walked.
	/// One list holding both would answer neither question cleanly.
	#[serde(default)]
	pub executor_entry_points: Vec<String>,
	/// Non-zero means `executor_entry_points` is a PARTIAL set. See
	/// `vocabulary_overflow` for why a repeat is not overflow.
	#[serde(default)]
	pub entry_point_overflow: u32,
	/// Every `ExecutionPlan` variant `exec::execute_bound_node` actually dispatched
	/// on, by `ExecutionPlan::node_label`. Recorded on entry to the recursive
	/// executor, so it covers the whole fragment tree the driver walked, not just
	/// its root.
	///
	/// **Presence is not attribution.** This says a node kind executed, never how
	/// long it took or what share it held. Reading a cost from membership here is
	/// the same error as reading one from a plan dump.
	#[serde(default)]
	pub executed_node_kinds: Vec<String>,
	/// Non-zero means `executed_node_kinds` is a PARTIAL set, so an absent kind
	/// cannot be read as "never executed".
	#[serde(default)]
	pub node_kind_overflow: u32,
}

/// Bound on a task-local route set. Small on purpose: these sets name *which*
/// routes ran, and a receipt that could grow without limit would turn a label
/// into an unbounded payload on every task completion message.
pub const MAX_TASK_ROUTE_LABELS: usize = 16;

/// Record `name` into a bounded, deduplicated route set, counting what the bound
/// dropped.
///
/// One writer for every such set — committed vocabularies, executor entry points,
/// executed node kinds — so the three cannot drift into different notions of what
/// overflow means. A repeat of an already-recorded name is **not** overflow: the
/// set already covers that route, and counting it would make a complete receipt
/// read as truncated.
pub fn record_bounded_route(set: &mut Vec<String>, overflow: &mut u32, name: &str) {
	if set.iter().any(|existing| existing == name) {
		return;
	}
	if set.len() >= MAX_TASK_ROUTE_LABELS {
		*overflow = overflow.saturating_add(1);
		return;
	}
	set.push(name.to_string());
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskTelemetryCounters {
	pub pulled_shuffle_bytes: u64,
	pub left_shuffle_bytes: u64,
	pub right_shuffle_bytes: u64,
	pub published_shuffle_bytes: u64,
	pub build_rows: u64,
	pub probe_rows: u64,
	pub matched_rows: u64,
	pub emitted_rows: u64,
	pub emitted_batches: u64,
	pub spill_bytes: u64,
	/// ADR 554 follow-on: output rows served by the zero-copy slice fast path
	/// instead of `take`, per side.
	///
	/// The positive control for that path. A drop in
	/// `join_output_right_take_micros` with zero rows here would mean something
	/// else moved, not that the fast path fired — and the path only applies when
	/// a side's indices are an EXACT contiguous run over the whole selection, so
	/// its coverage is a fact to measure, never one to assume.
	#[serde(default)]
	pub join_output_left_slice_rows: u64,
	#[serde(default)]
	pub join_output_right_slice_rows: u64,
	/// ADR 554 follow-on: index rows a gather did NOT materialize, because the
	/// side's selection walked the whole source in order and the slice path reads
	/// it directly.
	///
	/// The positive control for the deferral. A fall in
	/// `join_output_nonnull_index_build_micros` with zero rows here means
	/// something else moved — and the two counters are recorded together so the
	/// avoided share is a measurement rather than an inference from which nodes
	/// happened to slice.
	#[serde(default)]
	pub join_output_index_rows_avoided: u64,
	/// Index rows a gather did materialize. Its partner above: exactly one of the
	/// two is charged per side, so their sum is every index row the charged sites
	/// walked, and the ratio is the deferral's coverage.
	#[serde(default)]
	pub join_output_index_rows_materialized: u64,
	/// ADR 580: which input route each `execute_hash_join` committed to — probing
	/// the right batches directly, or concatenating them first.
	///
	/// A receipt, so a run reads the batch-local figures only on nodes where the
	/// route actually fired and a fallback is a recorded fact rather than the
	/// absence of one.
	#[serde(default)]
	pub join_batch_local_routes: u64,
	#[serde(default)]
	pub join_concatenated_routes: u64,
	/// Output batches the batch-local route produced. It returns one per probe
	/// batch where the concatenating route returns one in total, and the consumer
	/// pays for that difference, so it is measured rather than assumed free.
	#[serde(default)]
	pub join_batch_local_output_batches: u64,
	/// Slice bytes the skipped concatenation would have copied.
	///
	/// Bytes NOT allocated, which is what the route changes. NOT a measured peak:
	/// the source batches and the per-batch outputs are still live.
	#[serde(default)]
	pub join_batch_local_concat_bytes_avoided: u64,
	/// Builds that passed the batch-local route's admission estimate and were then
	/// declined by the build itself.
	///
	/// MUST BE ZERO. The admission gate tests the estimate for the whole build and
	/// the build tests the same estimate for a row count that cannot exceed it, so
	/// a decline here contradicts the gate. It is counted because its consequence is
	/// otherwise invisible: the caller rebuilds on the concatenating path, and that
	/// duplicated work lands in framework overhead while the receipt reports an
	/// ordinary fallback.
	#[serde(default)]
	pub join_batch_local_post_admission_declines: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskTelemetryPhases {
	pub task_wall_micros: u64,
	pub shuffle_pull_decode_micros: u64,
	pub build_micros: u64,
	pub probe_micros: u64,
	pub spill_micros: u64,
	pub output_publish_micros: u64,
	pub task_setup_micros: u64,
	pub batch_encode_micros: u64,
	pub batch_send_micros: u64,
	pub batch_decode_micros: u64,
	pub dispatch_wait_micros: u64,
	pub telemetry_lock_micros: u64,
	/// ADR 554 Branch A: per-batch **aggregate accumulation** inside a fused
	/// streaming aggregate loop.
	///
	/// The boundary is semantic, not positional: it opens at the **first
	/// aggregate-accumulation instruction after input processing** and closes at
	/// the end of the loop body. That is not always the line immediately after
	/// `apply_streaming_scan_input_ops` — in the grouped scan path, closing the
	/// projection span and its receipt belongs to input processing, and charging
	/// it here would make the counter lie about what it measures.
	///
	/// It excludes scan acquisition (already metered), input processing, and
	/// **post-loop finalization** — `groups.finish(..)` and
	/// `finalize_global_aggregate_state(..)` remain in the residual by decision,
	/// because they are aggregate work but not accumulation.
	///
	/// This counter participates in `framework_overhead = task_wall - accounted`,
	/// so naming this time shrinks the residual by the same amount. It is **not**
	/// an ADR 538 component and must never enter that frozen mapping.
	///
	/// `scan_grouped` also opens a receipt-level `"fold"` stage span over the same
	/// region. The two intentionally overlap: that span is a batch-local
	/// diagnostic, and only this counter affects the task-wall partition.
	#[serde(default)]
	pub aggregate_accumulation_micros: u64,
	/// ADR 554 Branch A follow-on: per-batch **input processing** — predicate
	/// evaluation and projection expression evaluation — at the generic executor
	/// tree's `ExecutionPlan::Filter` and `ExecutionPlan::Project` nodes.
	///
	/// ADR 554 named "a separate term for scan input processing" as the candidate
	/// follow-on and pointed at `apply_streaming_scan_input_ops`. That pointer is
	/// wrong for the evidence cell: all four of its call sites are fused
	/// aggregate loops, and q06's residual sits on a producer task that records
	/// zero accumulation and therefore never enters one. The sites below are the
	/// ones a scan-and-filter producer stage actually executes.
	///
	/// A `FullyPushed` filter returns its input untouched before the bracket is
	/// reached, so it contributes nothing here. That is deliberate — there is no
	/// work to time — but it means a zero reading on a filter node does NOT
	/// discriminate "pushed to storage" from "never executed". The Project
	/// bracket is what carries the signal on a fully-pushed scan stage.
	///
	/// This counter participates in `framework_overhead = task_wall - accounted`,
	/// so naming this time shrinks the residual by the same amount. Like
	/// accumulation it is a **named subterm of the remainder**, never an ADR 538
	/// component, and the frozen phase-to-component mapping is not reopened.
	#[serde(default)]
	pub input_processing_micros: u64,
	/// Time spent concatenating a join's two inputs into single build and probe
	/// batches, at the generic `execute_hash_join` entry. `concat_batches` copies
	/// every input row, and the copy sits between the children's own brackets and
	/// the Build transition, so it was previously in no bracket at all.
	///
	/// Measured on the ADR 554 route-receipt board: join-bearing distributed nodes
	/// carried 12.2% of their wall as framework overhead against 3.0% elsewhere,
	/// with 89% of the board's 18.7 s residual on those nodes. This counter and
	/// `join_output_materialization_micros` are what test whether that is input
	/// copying, output gathering, or neither.
	///
	/// A **named subterm of the remainder**, never an ADR 538 component: the
	/// frozen phase-to-component mapping is not reopened. Like every other named
	/// subterm it participates in `framework_overhead = task_wall - accounted`, so
	/// naming this time shrinks the residual by exactly the same amount.
	#[serde(default)]
	pub join_input_materialization_micros: u64,
	/// Time inside `join_input_materialization_micros`, split by SIDE and by the
	/// SHAPE of the concatenation that spent it.
	///
	/// Nested children on the same terms as the output side split: inside the
	/// parent bracket, absent from `recompute_framework_overhead`, charged from
	/// the SAME clock read as the parent so a child cannot drift from it. The
	/// four partition the parent exactly.
	///
	/// Two axes because ADR 580's gate needs both. The sides are different work —
	/// the left input is the build side, held for the whole probe; the right is
	/// the probe side, streamed past once — and only a side that is BOTH large
	/// and genuinely multi-batch justifies an addressing change. A single-batch
	/// concatenation is already a zero-copy wrapper in arrow
	/// (`benches/join_materialization_shape.rs`), so time sitting in the
	/// single-batch children is not copy cost and cannot be removed by consuming
	/// batches directly; a split that could not tell the two apart would read
	/// wrapper overhead as evidence for a rewrite.
	#[serde(default)]
	pub join_input_left_single_batch_micros: u64,
	#[serde(default)]
	pub join_input_left_multi_batch_micros: u64,
	#[serde(default)]
	pub join_input_right_single_batch_micros: u64,
	#[serde(default)]
	pub join_input_right_multi_batch_micros: u64,
	/// Time spent gathering a join's selected rows into the output batch, in
	/// `materialize_join_selection`. Charged at the generic sites only — the
	/// spilled and streaming paths already run theirs inside the Spill and
	/// operator brackets, and charging those here would double-count against the
	/// task-wall partition.
	///
	/// Same standing as `join_input_materialization_micros`: a remainder subterm,
	/// not a component.
	#[serde(default)]
	pub join_output_materialization_micros: u64,
	/// Time inside `join_output_materialization_micros` spent in the LEFT side's
	/// complete `take` column loop.
	///
	/// **A nested child of the parent, not a sibling.** It is deliberately absent
	/// from `recompute_framework_overhead`: the parent already left the residual,
	/// and subtracting a child again would remove the same microseconds twice and
	/// break the task-wall partition. `adr554_join_output_side_children_are_not_
	/// subtracted_again` pins that.
	///
	/// One timer around the whole loop, never per column: the probe side already
	/// runs at 3.8-7.9 ns/row, and a per-column `Instant::now()` pair would be a
	/// material fraction of what it claims to measure.
	#[serde(default)]
	pub join_output_left_take_micros: u64,
	/// The RIGHT side's complete `take` column loop. Same standing as
	/// `join_output_left_take_micros`.
	///
	/// The two children do not have to sum to the parent, and normally will not:
	/// `output_total - left - right` is the shared cost — index-vector
	/// construction, `UInt32Array` creation, and final batch assembly. That
	/// residual is where the `Vec<Option<u32>>` opportunity lives.
	#[serde(default)]
	pub join_output_right_take_micros: u64,
	/// Time inside `join_output_materialization_micros` spent BUILDING the index
	/// vectors for a join whose selection cannot contain a null — `Inner`, `Cross`,
	/// and the four semi/anti kinds, whose arms reject every null-producing row
	/// variant outright.
	///
	/// Split from `join_output_nullable_index_build_micros` by ELIGIBILITY, not by
	/// convenience: only this bucket can be served by a plain `Vec<u32>` and a
	/// null-free `UInt32Array`. A single combined index-build counter could be
	/// large while saying nothing about the portion a substitution could change,
	/// which is the number this split exists to produce.
	///
	/// A nested child on the same terms as the two `take` counters: inside the
	/// parent bracket, absent from `recompute_framework_overhead`.
	#[serde(default)]
	pub join_output_nonnull_index_build_micros: u64,
	/// The same construction for the kinds that CAN produce a null index —
	/// `LeftOuter`, `RightOuter`, `FullOuter`. The control arm: it must carry the
	/// null-bitmap cost that the non-null bucket does not, and a substitution
	/// cannot touch it.
	///
	/// Its purpose is to make a small non-null figure interpretable. Without it, a
	/// small `join_output_nonnull_index_build_micros` reads equally as "index
	/// construction is cheap" and "these cells run no eligible joins", and those
	/// carry opposite conclusions.
	#[serde(default)]
	pub join_output_nullable_index_build_micros: u64,
	/// Time spent partitioning batches across an exchange boundary. Written by the executor,
	/// so it is produced on both arms: locally as in-memory repartitioning, and inside a
	/// distributed producer task. That symmetry is what makes it a paired component.
	#[serde(default)]
	pub exchange_repartition_micros: u64,
	/// Time spent awaiting and materializing a storage scan at the generic executor tree's
	/// `ExecutionPlan::Scan` node. Written at that single site only: scans fused inside a
	/// streaming operator loop are already covered by that operator's own phase bracket, and
	/// writing them here as well would break the task-wall partition. Produced on both arms
	/// by the same executor code.
	#[serde(default)]
	pub scan_micros: u64,
	pub framework_overhead_micros: u64,
}

#[cfg(test)]
mod join_spill_observation_tests {
	use super::*;

	#[test]
	fn join_spill_observations_are_typed_and_backward_compatible() {
		let legacy: TaskTelemetry = serde_json::from_str(
			r#"{"labels":{"strategy":null,"stage_role":"","operator_kind":"","worker_count":null},"counters":{"pulled_shuffle_bytes":0,"left_shuffle_bytes":0,"right_shuffle_bytes":0,"published_shuffle_bytes":0,"build_rows":0,"probe_rows":0,"matched_rows":0,"emitted_rows":0,"emitted_batches":0,"spill_bytes":0},"phases":{"task_wall_micros":0,"shuffle_pull_decode_micros":0,"build_micros":0,"probe_micros":0,"spill_micros":0,"output_publish_micros":0,"task_setup_micros":0,"batch_encode_micros":0,"batch_send_micros":0,"batch_decode_micros":0,"dispatch_wait_micros":0,"telemetry_lock_micros":0,"framework_overhead_micros":0},"join_observations":[],"join_build_profiles":[],"existence_summary_profiles":[]}"#,
		)
		.expect("legacy telemetry remains readable");
		assert!(legacy.join_spill_observations.is_empty());
		assert!(legacy.partition_work_concurrency_observations.is_empty());

		// The route receipts are label fields on the same struct, so a peer that
		// predates them must still deserialize. Asserted per field rather than as a
		// whole-struct default: `serde(default)` is easy to omit on one of four.
		assert!(legacy.labels.committed_vocabularies.is_empty());
		assert_eq!(legacy.labels.vocabulary_overflow, 0);
		assert!(legacy.labels.executor_entry_points.is_empty());
		assert_eq!(legacy.labels.entry_point_overflow, 0);
		assert!(legacy.labels.executed_node_kinds.is_empty());
		assert_eq!(legacy.labels.node_kind_overflow, 0);

		// The join materialization subterms are phase counters on the same struct.
		// A peer that predates them must still deserialize, and must read them as
		// zero rather than as absent-and-therefore-unknown: zero is the correct
		// reading, because a task that never ran the bracket spent no time in it.
		assert_eq!(legacy.phases.join_input_materialization_micros, 0);
		assert_eq!(legacy.phases.join_output_materialization_micros, 0);

		let observation = JoinSpillObservation {
			replay_id: JoinReplayId(0),
			path: JoinSpillPath::PartitionedStreamingScanBuild,
			spill_bytes: 88_559_536,
		};
		let value = serde_json::to_value(&observation).expect("serialize observation");
		assert_eq!(value["path"], "partitioned_streaming_scan_build");
		assert_eq!(value["spill_bytes"], 88_559_536);
	}

	#[test]
	fn partition_work_concurrency_observation_has_typed_route_and_exact_counts() {
		let observation = PartitionWorkConcurrencyObservation {
			replay_id: JoinReplayId(2),
			path: PartitionWorkConcurrencyPath::PartitionedStreamingScanBuild,
			partition_count: 8,
			requested_concurrency: 4,
			available_parallelism: 2,
			planned_k: 2,
			resident_peak_bytes: 64 * 1024 * 1024,
			resident_budget_bytes: 512 * 1024 * 1024,
			work_items_started: 8,
			work_items_completed: 8,
		};
		let value = serde_json::to_value(&observation).expect("serialize capability receipt");
		assert_eq!(value["path"], "partitioned_streaming_scan_build");
		assert_eq!(value["requested_concurrency"], 4);
		assert_eq!(value["available_parallelism"], 2);
		assert_eq!(value["planned_k"], 2);
		assert_eq!(value["resident_budget_bytes"], 512 * 1024 * 1024u64);
		assert_eq!(value["work_items_started"], value["work_items_completed"]);
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanShare {
	pub worker_index: u32,
	pub worker_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSpec {
	pub identity: TaskIdentity,
	pub partition: Option<PartitionId>,
	pub context: TaskExecutionContext,
	pub join_runtime_observation_mode: JoinRuntimeObservationMode,
	pub plan: ExecutionPlan,
	pub inputs: Vec<TaskInputBinding>,
	pub output: TaskOutputBinding,
	pub budget: TaskBudget,
	pub trace: Option<TraceContext>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskInputBinding {
	Scan {
		share: Option<ScanShare>,
		#[serde(default)]
		runtime_filter: Option<RuntimeFilterDescriptor>,
	},
	ShuffleConsumer {
		exchange_id: ExchangeId,
		producer_stage_id: StageId,
		partition_id: PartitionId,
		producer_partitions: Vec<ShufflePartitionRef>,
	},
}

pub fn shuffle_input_table_id(exchange_id: ExchangeId) -> TableId {
	TableId(u64::MAX - u64::from(exchange_id.get()))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerPartitionRef {
	pub address: PartitionAddress,
	pub worker_id: WorkerId,
	pub endpoint: WorkerEndpoint,
	pub size_bytes: ByteCount,
	pub digest: ArrowIpcDigest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShufflePartitionRef {
	Sealed(ProducerPartitionRef),
	Live(LiveProducerPartitionRef),
}

impl ShufflePartitionRef {
	pub const fn address(&self) -> PartitionAddress {
		match self {
			Self::Sealed(partition) => partition.address,
			Self::Live(partition) => partition.address,
		}
	}

	pub fn endpoint(&self) -> &WorkerEndpoint {
		match self {
			Self::Sealed(partition) => &partition.endpoint,
			Self::Live(partition) => &partition.endpoint,
		}
	}

	pub const fn as_sealed(&self) -> Option<&ProducerPartitionRef> {
		match self {
			Self::Sealed(partition) => Some(partition),
			Self::Live(_) => None,
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveProducerPartitionRef {
	pub address: PartitionAddress,
	pub worker_id: WorkerId,
	pub endpoint: WorkerEndpoint,
}

pub fn is_streamable_exchange_producer(spec: &TaskSpec) -> bool {
	matches!(
		spec.output,
		TaskOutputBinding::ShuffleProducer {
			publish_mode: ShufflePublishMode::Live,
			..
		}
	) && exchange_producer_plan_supports_live(spec)
}

pub fn exchange_producer_plan_supports_live(spec: &TaskSpec) -> bool {
	matches!(spec.output, TaskOutputBinding::ShuffleProducer { .. })
		&& matches!(
			&spec.plan,
			ExecutionPlan::Exchange { input, .. } if streamable_unary_plan(input)
		)
}

pub fn is_streamable_shuffle_consumer(spec: &TaskSpec) -> bool {
	spec.inputs
		.iter()
		.any(|input| matches!(input, TaskInputBinding::ShuffleConsumer { .. }))
		&& streamable_unary_plan(&spec.plan)
}

pub fn is_streamable_final_result(spec: &TaskSpec) -> bool {
	matches!(spec.output, TaskOutputBinding::FinalResult) && streamable_unary_plan(&spec.plan)
}

fn streamable_unary_plan(plan: &ExecutionPlan) -> bool {
	match plan {
		ExecutionPlan::Scan { .. } => true,
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Take { input, .. } => streamable_unary_plan(input),
		ExecutionPlan::Exchange { .. }
		| ExecutionPlan::Union { .. }
		| ExecutionPlan::Repeat { .. }
		| ExecutionPlan::Sort { .. }
		| ExecutionPlan::Window { .. }
		| ExecutionPlan::Aggregate { .. }
		| ExecutionPlan::KernelDensity { .. }
		| ExecutionPlan::Join { .. }
		// ADR 255: opaque-local — a recursive CTE is a self-contained local fixpoint, not a
		// streamable unary scan chain, and its self-reference leaf is not a streamable source.
		| ExecutionPlan::RecursiveCte { .. }
		| ExecutionPlan::RecursiveRef { .. }
		| ExecutionPlan::Range { .. }
		| ExecutionPlan::EmptyRelation { .. }
		| ExecutionPlan::SystemTable { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. } => false,
	}
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShufflePublishMode {
	Live,
	#[default]
	Sealed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskOutputBinding {
	FinalResult,
	ShuffleProducer {
		exchange_id: ExchangeId,
		partition_count: PartitionCount,
		partition_key: BoundExprProgram,
		#[serde(default)]
		publish_mode: ShufflePublishMode,
		/// Proven physical target for a runtime filter probing this producer, established
		/// at fragmentation by tracing the probe join key through projections to a base
		/// scan column. `None` means no such proof exists — a synthetic or computed key —
		/// and no runtime filter may be built. The target travels with the plan precisely
		/// so scheduling never has to rebuild it from a scan fingerprint and a
		/// post-projection ordinal, which are facts about different schemas.
		#[serde(default)]
		runtime_filter_probe_target: Option<RuntimeFilterProbeTarget>,
	},
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PartitionAddress {
	pub query_id: QueryId,
	pub exchange_id: ExchangeId,
	pub producer_stage_id: StageId,
	pub producer_task_id: TaskId,
	pub attempt_id: AttemptId,
	pub partition_id: PartitionId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionManifest {
	pub query_id: QueryId,
	pub stage_id: StageId,
	pub task_id: TaskId,
	pub attempt_id: AttemptId,
	pub exchange_id: ExchangeId,
	pub producer_worker_id: WorkerId,
	pub producer_endpoint: WorkerEndpoint,
	pub partitions: Vec<PartitionEntry>,
	#[serde(default)]
	pub runtime_filter_fragment: Option<RuntimeFilterFragment>,
	pub backend: PartitionBackend,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeFilterFragment {
	pub key_encoding_version: u32,
	pub key_types: Vec<RuntimeFilterKeyType>,
	pub kind: RuntimeFilterKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionEntry {
	pub partition_id: PartitionId,
	pub size_bytes: ByteCount,
	pub row_count: RowCount,
	pub digest: ArrowIpcDigest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionBackend {
	InMemory,
	EncryptedSpill { spill_id: SpillId },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskCompletionOutput {
	FinalResult {
		output_digest: Option<ArrowIpcDigest>,
	},
	ShuffleProducer {
		manifest: PartitionManifest,
	},
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
	pub max_attempts: AttemptLimit,
}

impl RetryPolicy {
	pub const fn one_retry() -> Self {
		Self {
			max_attempts: AttemptLimit::new(2),
		}
	}
}

impl Default for RetryPolicy {
	fn default() -> Self {
		Self::one_retry()
	}
}

#[cfg(test)]
mod shuffle_publish_mode_tests {
	use super::*;

	fn output(mode: ShufflePublishMode) -> TaskOutputBinding {
		TaskOutputBinding::ShuffleProducer {
			exchange_id: ExchangeId::new(7),
			partition_count: PartitionCount::new(1),
			partition_key: BoundExprProgram::empty(),
			publish_mode: mode,
			runtime_filter_probe_target: None,
		}
	}

	fn mode(output: &TaskOutputBinding) -> ShufflePublishMode {
		match output {
			TaskOutputBinding::ShuffleProducer { publish_mode, .. } => *publish_mode,
			TaskOutputBinding::FinalResult => panic!("expected shuffle producer"),
		}
	}

	#[test]
	fn legacy_shuffle_output_without_publish_mode_fails_safe_to_sealed() {
		let mut value = serde_json::to_value(output(ShufflePublishMode::Live)).expect("serialize");
		value
			.get_mut("ShuffleProducer")
			.and_then(serde_json::Value::as_object_mut)
			.expect("shuffle object")
			.remove("publish_mode");

		let decoded: TaskOutputBinding = serde_json::from_value(value).expect("decode legacy");
		assert_eq!(mode(&decoded), ShufflePublishMode::Sealed);
	}

	#[test]
	fn explicit_shuffle_publish_modes_round_trip() {
		for expected in [ShufflePublishMode::Live, ShufflePublishMode::Sealed] {
			let encoded = serde_json::to_vec(&output(expected)).expect("serialize");
			let decoded: TaskOutputBinding = serde_json::from_slice(&encoded).expect("deserialize");
			assert_eq!(mode(&decoded), expected);
		}
	}
}

#[cfg(test)]
mod tests {
	/// ADR 537 control: a task encoded before the proof existed decodes as `None`, so an
	/// older peer can never cause a filter to be built without proof. A new round trip
	/// preserves the target exactly.
	#[test]
	fn adr537_probe_target_legacy_decode_is_none_and_round_trip_preserves_some() {
		let legacy = serde_json::json!({
			"ShuffleProducer": {
				"exchange_id": 3,
				"partition_count": 4,
				"partition_key": BoundExprProgram::empty(),
			}
		});
		let decoded: TaskOutputBinding =
			serde_json::from_value(legacy).expect("legacy producer decodes");
		let TaskOutputBinding::ShuffleProducer {
			runtime_filter_probe_target,
			publish_mode,
			..
		} = &decoded
		else {
			panic!("producer");
		};
		assert!(
			runtime_filter_probe_target.is_none(),
			"a task without the field must carry no proof"
		);
		assert_eq!(
			*publish_mode,
			ShufflePublishMode::Sealed,
			"and must still default to sealed publication"
		);

		let target = RuntimeFilterProbeTarget {
			probe_descriptor_fingerprint: serde_json::from_value(
				serde_json::to_value([9u8; 32]).expect("fingerprint bytes"),
			)
			.expect("fingerprint from bytes"),
			physical_column_ordinal: 2,
			key_type: RuntimeFilterKeyType::Int64,
		};
		let binding = TaskOutputBinding::ShuffleProducer {
			exchange_id: ExchangeId::new(3),
			partition_count: PartitionCount::new(4),
			partition_key: BoundExprProgram::empty(),
			publish_mode: ShufflePublishMode::Live,
			runtime_filter_probe_target: Some(target.clone()),
		};
		let round_tripped: TaskOutputBinding =
			serde_json::from_str(&serde_json::to_string(&binding).expect("encode"))
				.expect("decode");
		let TaskOutputBinding::ShuffleProducer {
			runtime_filter_probe_target: Some(decoded_target),
			..
		} = &round_tripped
		else {
			panic!("producer with target");
		};
		assert_eq!(*decoded_target, target);
	}

	use super::*;
	use arrow::datatypes::{DataType, Field, Schema};
	use wendelon::schema::{
		LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, RuntimeFilterBloom,
		SchemaEvolutionPolicy, TableObjectId, TableSchemaDescriptor,
	};

	#[test]
	fn retry_policy_defaults_to_one_retry() {
		let policy = RetryPolicy::default();

		assert_eq!(policy.max_attempts, AttemptLimit::new(2));
	}

	#[test]
	fn partition_address_includes_attempt_identity() {
		let address = PartitionAddress {
			query_id: QueryId::from_u128(1),
			exchange_id: ExchangeId::new(2),
			producer_stage_id: StageId::new(3),
			producer_task_id: TaskId::new(4),
			attempt_id: AttemptId::new(5),
			partition_id: PartitionId::new(6),
		};

		assert_eq!(address.attempt_id.get(), 5);
		assert_eq!(address.partition_id.get(), 6);
	}

	#[test]
	fn scan_binding_round_trips_with_and_without_runtime_filter() {
		for runtime_filter in [None, Some(runtime_filter_descriptor())] {
			let input = TaskInputBinding::Scan {
				share: Some(ScanShare {
					worker_index: 2,
					worker_count: 4,
				}),
				runtime_filter,
			};

			let encoded = postcard::to_allocvec(&input).expect("encode scan binding");
			let decoded: TaskInputBinding =
				postcard::from_bytes(&encoded).expect("decode scan binding");

			assert_eq!(decoded, input);
		}
	}

	#[test]
	fn partition_manifest_round_trips_with_and_without_fragment() {
		for runtime_filter_fragment in [None, Some(runtime_filter_fragment())] {
			let manifest = PartitionManifest {
				query_id: QueryId::from_u128(1),
				stage_id: StageId::new(2),
				task_id: TaskId::new(3),
				attempt_id: AttemptId::new(4),
				exchange_id: ExchangeId::new(5),
				producer_worker_id: WorkerId::from_u128(6),
				producer_endpoint: WorkerEndpoint::new("127.0.0.1:9191"),
				partitions: vec![PartitionEntry {
					partition_id: PartitionId::new(7),
					size_bytes: ByteCount::new(8),
					row_count: RowCount::new(9),
					digest: ArrowIpcDigest::new([10; 32]),
				}],
				runtime_filter_fragment,
				backend: PartitionBackend::InMemory,
			};

			let encoded = postcard::to_allocvec(&manifest).expect("encode manifest");
			let decoded: PartitionManifest =
				postcard::from_bytes(&encoded).expect("decode manifest");

			assert_eq!(decoded, manifest);
		}
	}

	fn runtime_filter_descriptor() -> RuntimeFilterDescriptor {
		let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
		let descriptor = TableSchemaDescriptor::new(
			TableObjectId::new([6; 16]),
			1,
			"default",
			"runtime_filter_test",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicy::AdditiveOnly,
		)
		.expect("descriptor");
		RuntimeFilterDescriptor {
			key_encoding_version: 1,
			key_types: vec![RuntimeFilterKeyType::Int64],
			probe_key_columns: vec![0],
			kind: RuntimeFilterKind::Bloom(RuntimeFilterBloom {
				hash_seed: 7,
				bit_count: 8,
				hash_count: 1,
				byte_budget: 1,
				target_fp_rate_bp: 100,
				payload: vec![0xFF],
			}),
			probe_descriptor_fingerprint: descriptor.physical_descriptor_fingerprint,
		}
	}

	fn runtime_filter_fragment() -> RuntimeFilterFragment {
		RuntimeFilterFragment {
			key_encoding_version: 1,
			key_types: vec![RuntimeFilterKeyType::Int64],
			kind: RuntimeFilterKind::Bloom(RuntimeFilterBloom {
				hash_seed: 7,
				bit_count: 16,
				hash_count: 2,
				byte_budget: 2,
				target_fp_rate_bp: 100,
				payload: vec![0b1010_0101, 0b0101_1010],
			}),
		}
	}
}
