use super::*;

use crate::phase_ledger::OperatorKind as PhaseOperatorKind;

mod build_profile;

mod build_rows;

mod columns;

mod global_fold_profile;

mod grouped_chain_profile;

mod membership_chain_profile;

mod partitioned_build_profile;

mod residency_profile;

mod spill;

mod spill_placement;

mod spilled_hash;

mod streamed_spill;

pub(in crate::exec) mod stage_attribution;

mod streaming;

mod throughput_profile;

pub(in crate::exec) use self::build_profile::{
	record_existence_summary_profile, record_join_build_profile,
};

pub(super) use self::build_rows::{BuildRowList, JoinBuildRows, build_in_memory_join_rows};

pub(super) use self::columns::{collect_program_column_ordinals, grouped_aggregate_input_columns};

pub(in crate::exec) use self::global_fold_profile::{
	GlobalFoldJoinFilterProfile, GlobalFoldProfileLeaf,
};

pub(in crate::exec) use self::grouped_chain_profile::{
	GroupedChainPinOutcome, GroupedChainProfile, GroupedChainProfileLeaf,
	GroupedChainSpillObserver, GroupedChainSubLeaf,
};

pub(in crate::exec) use self::membership_chain_profile::{
	MembershipChainOperation, MembershipChainPhase, MembershipChainProfile, MembershipSpillRole,
};

pub(in crate::exec) use self::spill::SpilledInnerJoinResidencySnapshot;

pub(in crate::exec) use self::spill::{PinnedSpilledInnerBuildStats, SpilledInnerJoinBuild};

pub(in crate::exec) use self::spill::{
	SpilledBatchRowRef, SpilledInnerJoinMatch, SpilledJoinBuild, SpilledSemiAntiExistenceBuild,
	partition_bucket_if_non_null_with_scratch,
};

pub(crate) use self::spill::{partition_bucket, partition_bucket_with_scratch};

pub(in crate::exec) use self::spill_placement::default_join_spill_dir;

pub(super) use self::spilled_hash::*;

pub(super) use self::streamed_spill::*;

#[cfg(test)]
pub(in crate::exec) use self::streaming::{
	StreamingSemiAntiExistence, StreamingSemiAntiRuntime, StreamingSemiAntiStage,
	extract_streaming_inner_prefix_semi_anti_chain,
};

pub(in crate::exec) use self::throughput_profile::{
	SpillCacheEvictions, StreamedSpilledLeftOuterProfile, ThroughputOperationPhase,
};

/// ADR 335: the raw 64 MiB task cap false-trips accepted sf=1 controls; the output guard

/// targets the GiB-scale materializations that cgroup-kill before the engine can report a row.

pub(super) const JOIN_OUTPUT_MATERIALIZATION_MEMORY_CAP_MULTIPLIER: u64 = 16;

/// ADR 336: Phase 0 measured the largest accepted spill-completer at 440,765,522

/// retained bytes and the retained-input wall at 1,423,571,737; 16x the default task memory

/// cap sits in that gap.

pub(super) const JOIN_SPILL_RETAINED_FOOTPRINT_MEMORY_CAP_MULTIPLIER: u64 = 16;

pub(super) fn grouped_aggregate_is_order_insensitive(aggr_expr: &[BoundAggregateCall]) -> bool {
	// ADR 561: the classification is registry metadata; this predicate only
	// applies the float qualification the `InsensitiveUnlessFloat` rule names.
	aggr_expr.iter().all(|call| {
		match crate::aggregate_kernel::stable_spec_for_function(&call.function).order_sensitivity {
			crate::aggregate_kernel::AggregateOrderSensitivity::Insensitive => true,
			crate::aggregate_kernel::AggregateOrderSensitivity::InsensitiveUnlessFloat => {
				call.args.roots.iter().all(|root| {
					!matches!(
						call.args.node_type(*root),
						Some(DataType::Float32 | DataType::Float64)
					)
				})
			}
			crate::aggregate_kernel::AggregateOrderSensitivity::Sensitive => false,
		}
	})
}

pub(super) fn projected_schema_for_columns(
	schema: &SchemaRef,

	columns: &[ColumnOrdinal],
) -> Result<SchemaRef, ExecutionError> {
	let fields = columns
		.iter()
		.map(|ordinal| {
			schema
				.fields()
				.get(ordinal.0 as usize)
				.map(|field| field.as_ref().clone())
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: format!("projected spill schema ordinal {} out of bounds", ordinal.0),
				})
		})
		.collect::<Result<Vec<_>, _>>()?;

	Ok(Arc::new(Schema::new(fields)))
}

pub(super) fn project_batch_columns(
	batch: &RecordBatch,

	columns: &[ColumnOrdinal],
) -> Result<RecordBatch, ExecutionError> {
	let projected_schema = projected_schema_for_columns(&batch.schema(), columns)?;

	let projected_columns = columns
		.iter()
		.map(|ordinal| {
			batch
				.columns()
				.get(ordinal.0 as usize)
				.cloned()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: format!("projected spill column ordinal {} out of bounds", ordinal.0),
				})
		})
		.collect::<Result<Vec<_>, _>>()?;

	Ok(RecordBatch::try_new(projected_schema, projected_columns)?)
}

pub(super) fn remap_column_ordinals(
	projection: &[ColumnOrdinal],

	ordinals: &[ColumnOrdinal],
) -> Result<Vec<ColumnOrdinal>, ExecutionError> {
	ordinals
		.iter()
		.map(|ordinal| {
			projection
				.iter()
				.position(|candidate| candidate == ordinal)
				.map(|position| ColumnOrdinal(position as u32))
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: format!("projected spill key ordinal {} was not retained", ordinal.0),
				})
		})
		.collect()
}

pub(super) fn default_array_for_unused_column(
	data_type: &DataType,

	rows: usize,
) -> Result<ArrayRef, ExecutionError> {
	Ok(match data_type {
		DataType::Boolean => Arc::new(BooleanArray::from(vec![false; rows])) as ArrayRef,

		DataType::Int32 => Arc::new(Int32Array::from(vec![0; rows])) as ArrayRef,

		DataType::Int64 => Arc::new(Int64Array::from(vec![0; rows])) as ArrayRef,

		DataType::Float32 => Arc::new(Float32Array::from(vec![0.0; rows])) as ArrayRef,

		DataType::Float64 => Arc::new(Float64Array::from(vec![0.0; rows])) as ArrayRef,

		DataType::Date32 => Arc::new(Date32Array::from(vec![0; rows])) as ArrayRef,

		DataType::Utf8 => Arc::new(StringArray::from(vec![Some(""); rows])) as ArrayRef,

		DataType::Decimal128(precision, scale) => Arc::new(
			Decimal128Array::from(vec![Some(0); rows])
				.with_precision_and_scale(*precision, *scale)
				.map_err(|error| ExecutionError::Kernel {
					message: error.to_string(),
				})?,
		) as ArrayRef,

		_ => arrow::array::new_null_array(data_type, rows),
	})
}

pub(super) fn duration_micros(started: Instant) -> u64 {
	u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

/// One task-local timing handle for join execution.

///

/// ADR 551: free join functions must not receive a telemetry sink separately

/// from the phase ledger that nets nested acquisition out of operator time. The

/// handle keeps those two views from drifting and exposes the only operator-scope

/// construction point used by this module.

#[derive(Clone)]

pub(super) struct JoinTiming {
	telemetry: Option<Arc<Mutex<TaskTelemetry>>>,

	scan_meter: crate::scan_metering::ScanMeter,
}

impl JoinTiming {
	pub(super) fn from_context(context: &ExecutionContext) -> Self {
		Self {
			telemetry: context.task_telemetry.clone(),

			scan_meter: context.scan_meter.clone(),
		}
	}

	#[cfg(test)]

	pub(super) fn disabled(telemetry: Option<Arc<Mutex<TaskTelemetry>>>) -> Self {
		Self {
			telemetry,

			scan_meter: crate::scan_metering::ScanMeter::disabled(),
		}
	}

	pub(super) fn open_operator_transition(
		&self,

		kind: crate::phase_ledger::OperatorKind,
	) -> crate::phase_ledger::OperatorTransition {
		crate::phase_ledger::OperatorTransition::open(self.scan_meter.phases(), kind)
	}
}

impl std::ops::Deref for JoinTiming {
	type Target = Option<Arc<Mutex<TaskTelemetry>>>;

	fn deref(&self) -> &Self::Target {
		&self.telemetry
	}
}

pub(super) fn record_task_telemetry(
	telemetry: &Option<Arc<Mutex<TaskTelemetry>>>,

	update: impl FnOnce(&mut TaskTelemetry),
) {
	let Some(telemetry) = telemetry else {
		return;
	};

	let lock_started = Instant::now();

	if let Ok(mut telemetry) = telemetry.lock() {
		telemetry.phases.telemetry_lock_micros = telemetry
			.phases
			.telemetry_lock_micros
			.saturating_add(duration_micros(lock_started));

		update(&mut telemetry);
	}
}

/// Time a join input concatenation and charge it to the input-materialization
/// subterm.
///
/// Charged even when the body fails, for the same reason input processing is: the
/// time was spent inside the task wall either way, and dropping it on the error
/// path would silently move it back into the residual this counter exists to
/// reduce.
pub(super) fn time_join_input_materialization<T>(
	telemetry: &Option<Arc<Mutex<TaskTelemetry>>>,
	body: impl FnOnce() -> Result<T, ExecutionError>,
) -> Result<T, ExecutionError> {
	time_join_materialization(telemetry, body, |phases| {
		&mut phases.join_input_materialization_micros
	})
}

/// Time a join output gather and charge it to the output-materialization subterm.
///
/// Called only at the generic sites. The spilled and streaming paths run their
/// own `materialize_join_selection` inside the Spill and operator brackets, and
/// charging those here as well would count the same microseconds twice against a
/// task wall the two must partition.
pub(super) fn time_join_output_materialization<T>(
	telemetry: &Option<Arc<Mutex<TaskTelemetry>>>,
	body: impl FnOnce() -> Result<T, ExecutionError>,
) -> Result<T, ExecutionError> {
	time_join_materialization(telemetry, body, |phases| {
		&mut phases.join_output_materialization_micros
	})
}

/// The shared body. One writer for both subterms so the bracket boundary — open
/// before the first instruction, close after the result exists, charge on both
/// exits — cannot drift between them.
fn time_join_materialization<T>(
	telemetry: &Option<Arc<Mutex<TaskTelemetry>>>,
	body: impl FnOnce() -> Result<T, ExecutionError>,
	select: fn(&mut crate::distributed::task::TaskTelemetryPhases) -> &mut u64,
) -> Result<T, ExecutionError> {
	let started = Instant::now();
	let outcome = body();
	let elapsed = duration_micros(started);
	record_task_telemetry(telemetry, |task| {
		let slot = select(&mut task.phases);
		*slot = slot.saturating_add(elapsed);
	});
	outcome
}

/// Whether a `materialize_join_selection` call charges its per-side `take` loops
/// to the nested child counters.
///
/// An explicit type rather than an `Option`, so every call site states which it
/// is. The generic path charges; the spilled and streaming paths must not,
/// because their gathers already run inside the Spill and operator brackets and
/// charging them here would attribute the same work to two accounting families.
/// A caller cannot forget to decide — there is no default.
#[derive(Clone, Copy)]
pub(super) enum SideTakeTiming<'a> {
	/// Charge both side loops to the task's nested child counters.
	Charged(&'a Option<Arc<Mutex<TaskTelemetry>>>),
	/// Do not charge. The gather is accounted elsewhere.
	AccountedElsewhere,
}

impl SideTakeTiming<'_> {
	fn time<T>(
		self,
		side: fn(&mut crate::distributed::task::TaskTelemetryPhases) -> &mut u64,
		body: impl FnOnce() -> Result<T, ExecutionError>,
	) -> Result<T, ExecutionError> {
		let Self::Charged(telemetry) = self else {
			return body();
		};
		let started = Instant::now();
		let outcome = body();
		let elapsed = duration_micros(started);
		// Charged on the error path for the same reason the parent bracket is: the
		// time was inside the task wall either way.
		record_task_telemetry(telemetry, |task| {
			let slot = side(&mut task.phases);
			*slot = slot.saturating_add(elapsed);
		});
		outcome
	}

	fn time_left<T>(
		self,
		body: impl FnOnce() -> Result<T, ExecutionError>,
	) -> Result<T, ExecutionError> {
		self.time(|phases| &mut phases.join_output_left_take_micros, body)
	}

	fn time_right<T>(
		self,
		body: impl FnOnce() -> Result<T, ExecutionError>,
	) -> Result<T, ExecutionError> {
		self.time(|phases| &mut phases.join_output_right_take_micros, body)
	}
}

/// Environment variable enabling the join selection locality probe.
///
/// **Opt-in on purpose, unlike every other ADR 554 receipt.** The others record
/// a fact the executor already knows — a route name, a bracket's elapsed time.
/// This one walks the whole selection, so leaving it on would add unaccounted
/// O(rows) work *outside* the output bracket and inflate the framework overhead
/// it exists to explain. A diagnostic must not move the number it measures.
///
/// The cost of gating is that an ordinary atlas records nothing here. That is
/// why `run_timing_atlas` stamps the probe's state into the artifact header: a
/// reader must be able to tell "the probe was off" from "the joins had no
/// locality", and an absent counter alone cannot say which.
pub const JOIN_SELECTION_LOCALITY_ENV: &str = "TORON_JOIN_SELECTION_LOCALITY";

/// Whether the locality probe is enabled for this process.
pub fn join_selection_locality_enabled() -> bool {
	static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
	*ENABLED.get_or_init(|| std::env::var(JOIN_SELECTION_LOCALITY_ENV).is_ok())
}

/// Fold one selection's run structure into the task accumulator, when the probe
/// is enabled.
pub(super) fn record_join_selection_locality(
	telemetry: &Option<Arc<Mutex<TaskTelemetry>>>,
	kind: JoinKind,
	selection: &[JoinOutputRow],
) {
	if !join_selection_locality_enabled() {
		return;
	}
	let measured = measure_join_selection_locality(kind, selection);
	record_task_telemetry(telemetry, |task| {
		task.join_selection_locality.left.merge(&measured.left);
		task.join_selection_locality.right.merge(&measured.right);
	});
}

/// Measure one selection's run structure, per side.
///
/// Pure and unconditional so the arithmetic can be tested without touching
/// process environment — the gate is tested separately.
///
/// `kind` decides which sides participate, mirroring
/// `materialize_join_selection`'s own dispatch exactly. A side that the gather
/// does not touch records no invocation; a side it *does* touch records one even
/// when the selection contributed no index to it, which is what separates "never
/// gathered here" from "gathered nothing here". An outer join whose rows all
/// null one side is precisely that case, and it is real.
pub(super) fn measure_join_selection_locality(
	kind: JoinKind,
	selection: &[JoinOutputRow],
) -> crate::distributed::task::JoinSelectionLocalitySides {
	use crate::distributed::task::{JoinSelectionLocality, JoinSelectionLocalitySides};

	let (touches_left, touches_right) = match kind {
		JoinKind::Inner
		| JoinKind::Cross
		| JoinKind::LeftOuter
		| JoinKind::RightOuter
		| JoinKind::FullOuter => (true, true),
		JoinKind::LeftSemi | JoinKind::LeftAnti => (true, false),
		JoinKind::RightSemi | JoinKind::RightAnti => (false, true),
	};

	let mut left = JoinSelectionLocality::default();
	let mut right = JoinSelectionLocality::default();
	// `None` means the side has not seen an index yet, or the previous row nulled
	// it — either way the next index starts a fresh run rather than continuing one
	// across a gap the gather will not see as contiguous.
	let mut left_previous: Option<u32> = None;
	let mut right_previous: Option<u32> = None;
	let mut left_run = 0u64;
	let mut right_run = 0u64;

	for row in selection {
		let (left_index, right_index) = match *row {
			JoinOutputRow::Paired { left, right } => (Some(left), Some(right)),
			JoinOutputRow::LeftFillNull { left } => (Some(left), None),
			JoinOutputRow::RightFillNull { right } => (None, Some(right)),
			JoinOutputRow::LeftPreserved { left } => (Some(left), None),
			JoinOutputRow::RightPreserved { right } => (None, Some(right)),
		};
		step(&mut left, &mut left_previous, &mut left_run, left_index);
		step(&mut right, &mut right_previous, &mut right_run, right_index);
	}
	left.observe_run(left_run);
	right.observe_run(right_run);

	// One invocation per side the gather touches, counted whether or not that
	// side received any index. `empty_invocations` carries the difference, so a
	// zero row count can never be mistaken for a side that was never gathered.
	for (side, touched) in [(&mut left, touches_left), (&mut right, touches_right)] {
		if !touched {
			continue;
		}
		side.invocations = 1;
		if side.selected_rows == 0 {
			side.empty_invocations = 1;
		}
	}

	JoinSelectionLocalitySides { left, right }
}

/// Advance one side by one selection row.
///
/// A run is a maximal ascending-by-exactly-one sequence. A repeated index is
/// counted as a duplicate and **ends** the run: the gather still pays a separate
/// lookup for it, so treating duplicates as contiguous would overstate what
/// slice reuse could claim.
fn step(
	stats: &mut crate::distributed::task::JoinSelectionLocality,
	previous: &mut Option<u32>,
	run: &mut u64,
	index: Option<u32>,
) {
	let Some(index) = index else {
		// A null on this side breaks contiguity; close the run and forget the
		// predecessor so the next index cannot appear to continue it.
		stats.observe_run(*run);
		*run = 0;
		*previous = None;
		return;
	};
	stats.selected_rows = stats.selected_rows.saturating_add(1);
	match *previous {
		// `checked_add`, not `saturating_add`: at `u32::MAX` a saturating
		// successor equals the predecessor, so a REPEATED `u32::MAX` would be
		// counted as an ascending step and inflate the contiguity this probe
		// exists to measure.
		Some(last) if last.checked_add(1) == Some(index) => {
			stats.ascending_steps = stats.ascending_steps.saturating_add(1);
			*run = run.saturating_add(1);
		}
		Some(last) if index == last => {
			stats.duplicate_steps = stats.duplicate_steps.saturating_add(1);
			stats.observe_run(*run);
			*run = 1;
		}
		_ => {
			stats.observe_run(*run);
			*run = 1;
		}
	}
	*previous = Some(index);
}

pub(super) fn record_join_runtime_observation(
	telemetry: &Option<Arc<Mutex<TaskTelemetry>>>,

	mode: &JoinRuntimeObservationMode,

	observation: JoinRuntimeObservation,
) {
	if !mode.measures(observation.replay_id) {
		return;
	}

	record_task_telemetry(telemetry, |task| {
		task.join_observations.push(observation);
	});
}

pub(super) fn record_join_spill_observation(
	telemetry: &Option<Arc<Mutex<TaskTelemetry>>>,

	replay_id: JoinReplayId,

	path: crate::distributed::task::JoinSpillPath,

	spill_bytes: u64,
) {
	record_task_telemetry(telemetry, |task| {
		task.join_spill_observations
			.push(crate::distributed::task::JoinSpillObservation {
				replay_id,

				path,

				spill_bytes,
			});
	});
}

fn task_spill_bytes(telemetry: &Option<Arc<Mutex<TaskTelemetry>>>) -> u64 {
	telemetry
		.as_ref()
		.and_then(|telemetry| telemetry.lock().ok())
		.map(|telemetry| telemetry.counters.spill_bytes)
		.unwrap_or(0)
}

pub(super) fn record_batch_row_count(batches: &[RecordBatch]) -> u64 {
	batches
		.iter()
		.map(|batch| batch.num_rows() as u64)
		.sum::<u64>()
}

#[allow(clippy::too_many_arguments)]

pub(super) async fn execute_hash_join(
	left_schema: arrow::datatypes::SchemaRef,

	right_schema: arrow::datatypes::SchemaRef,

	left_batches: &[RecordBatch],

	right_batches: &[RecordBatch],

	kind: JoinKind,

	left_keys: &[crate::ir::ColumnOrdinal],

	right_keys: &[crate::ir::ColumnOrdinal],

	match_filter: Option<&BoundExprProgram>,

	null_safe_keys: bool,

	replay_id: JoinReplayId,

	strategy: JoinStrategy,

	output_schema: &arrow::datatypes::SchemaRef,

	task_budget: TaskBudget,

	telemetry: JoinTiming,

	join_runtime_observation_mode: JoinRuntimeObservationMode,

	spill_file_ops: Arc<dyn AsyncFileOps>,

	spill_dir: PathBuf,
) -> Result<Vec<RecordBatch>, ExecutionError> {
	if left_keys.len() != right_keys.len() {
		return Err(ExecutionError::InvalidExpression {
			message: "join key arity mismatch".to_string(),
		});
	}

	if let Some(output) = try_execute_streamed_spilled_left_semi_join(
		left_schema.clone(),
		right_schema.clone(),
		left_batches,
		right_batches,
		kind,
		left_keys,
		right_keys,
		match_filter,
		null_safe_keys,
		replay_id,
		strategy,
		output_schema,
		task_budget,
		telemetry.clone(),
		join_runtime_observation_mode.clone(),
		spill_file_ops.clone(),
		spill_dir.clone(),
	)
	.await?
	{
		return Ok(output);
	}

	if let Some(output) = try_execute_streamed_spilled_inner_join(
		left_schema.clone(),
		right_schema.clone(),
		left_batches,
		right_batches,
		kind,
		left_keys,
		right_keys,
		match_filter,
		null_safe_keys,
		replay_id,
		strategy,
		output_schema,
		task_budget,
		telemetry.clone(),
		join_runtime_observation_mode.clone(),
		spill_file_ops.clone(),
		spill_dir.clone(),
	)
	.await?
	{
		return Ok(output);
	}

	if let Some(output) = try_execute_partitioned_spilled_left_outer_join(
		left_schema.clone(),
		right_schema.clone(),
		left_batches,
		right_batches,
		kind,
		left_keys,
		right_keys,
		match_filter,
		null_safe_keys,
		replay_id,
		strategy,
		output_schema,
		task_budget,
		telemetry.clone(),
		join_runtime_observation_mode.clone(),
		spill_file_ops.clone(),
		spill_dir.clone(),
	)
	.await?
	{
		return Ok(output);
	}

	let left_batch =
		time_join_input_materialization(&telemetry, || concat_or_empty(left_schema, left_batches))?;

	let right_batch = time_join_input_materialization(&telemetry, || {
		concat_or_empty(right_schema, right_batches)
	})?;

	// ADR 239: CROSS is keyless — route through the explicit match-all path instead of the hash

	// build/probe. The hash path would bucket every row under `join_key_bytes([])`, which is the

	// single-bucket distributed trap; the explicit path emits every build×probe pair deliberately.

	// One bounded path serves both the in-memory and would-be-spilled cases, so they cannot diverge.

	if matches!(kind, JoinKind::Cross) {
		return execute_cross_join_match_all(
			&left_batch,
			&right_batch,
			output_schema,
			task_budget.memory.get(),
			replay_id,
			strategy,
			&telemetry,
			&join_runtime_observation_mode,
		);
	}

	let left_key_arrays = key_arrays(&left_batch, left_keys)?;

	let right_key_arrays = key_arrays(&right_batch, right_keys)?;

	record_task_telemetry(&telemetry, |task| {
		task.counters.build_rows = task
			.counters
			.build_rows
			.saturating_add(left_batch.num_rows() as u64);

		task.counters.probe_rows = task
			.counters
			.probe_rows
			.saturating_add(right_batch.num_rows() as u64);
	});

	if estimated_build_hash_bytes(&left_key_arrays, left_batch.num_rows())
		> task_budget.memory.get()
	{
		let result = execute_spilled_hash_join(
			kind,
			&left_batch,
			&right_batch,
			&left_key_arrays,
			&right_key_arrays,
			output_schema,
			match_filter,
			null_safe_keys,
			task_budget,
			telemetry.clone(),
			spill_file_ops,
			spill_dir,
		)
		.await;

		if let Ok(batches) = &result {
			record_join_runtime_observation(
				&telemetry,
				&join_runtime_observation_mode,
				JoinRuntimeObservation {
					replay_id,

					strategy,

					build_rows: RowCount::new(left_batch.num_rows() as u64),

					probe_rows: RowCount::new(right_batch.num_rows() as u64),

					output_rows: RowCount::new(record_batch_row_count(batches)),
				},
			);
		}

		return result;
	}

	let join_key_plan =
		JoinKeyPlan::from_key_arrays(&left_key_arrays, &right_key_arrays, null_safe_keys);

	let build_started = Instant::now();

	// ADR0551-SITE: join/execute_hash_join/Build/1

	let mut build_scope = telemetry.open_operator_transition(PhaseOperatorKind::Build);

	let build_rows = match build_in_memory_join_rows(
		join_key_plan,
		&left_key_arrays,
		left_batch.num_rows(),
		task_budget.memory.get(),
		null_safe_keys,
	) {
		Ok(Some(build_rows)) => build_rows,

		Err(error) => {
			build_scope.discard_before_measurement();

			return Err(error);
		}

		Ok(None) => {
			build_scope.discard_before_measurement();

			let result = execute_spilled_hash_join(
				kind,
				&left_batch,
				&right_batch,
				&left_key_arrays,
				&right_key_arrays,
				output_schema,
				match_filter,
				null_safe_keys,
				task_budget,
				telemetry.clone(),
				spill_file_ops,
				spill_dir,
			)
			.await;

			if let Ok(batches) = &result {
				record_join_runtime_observation(
					&telemetry,
					&join_runtime_observation_mode,
					JoinRuntimeObservation {
						replay_id,

						strategy,

						build_rows: RowCount::new(left_batch.num_rows() as u64),

						probe_rows: RowCount::new(right_batch.num_rows() as u64),

						output_rows: RowCount::new(record_batch_row_count(batches)),
					},
				);
			}

			return result;
		}
	};

	build_scope.finish_at_measurement();

	let build_micros = duration_micros(build_started);

	record_task_telemetry(&telemetry, |_task| {
		build_scope.commit_after_write();
	});

	record_join_build_profile(
		&telemetry,
		replay_id,
		strategy,
		join_key_plan,
		&build_rows,
		build_micros,
	);

	// ADR0551-SITE: join/execute_hash_join/Probe/1

	let mut probe_scope = telemetry.open_operator_transition(PhaseOperatorKind::Probe);

	let selection = match probe_in_memory_join_rows(
		kind,
		join_key_plan,
		&build_rows,
		left_batch.num_rows(),
		&right_key_arrays,
		right_batch.num_rows(),
		match_filter,
		&left_batch,
		&right_batch,
		null_safe_keys,
	) {
		Ok(selection) => selection,

		Err(error) => {
			probe_scope.discard_before_measurement();

			return Err(error);
		}
	};

	record_task_telemetry(&telemetry, |task| {
		probe_scope.finish_at_measurement();

		task.counters.matched_rows = task
			.counters
			.matched_rows
			.saturating_add(selection.len() as u64);
	});

	probe_scope.commit_after_write();

	// Locality probe (ADR 554 follow-on): measured on the same selection the
	// gather below is about to consume, before it is consumed, so the statistic
	// and the cost it explains describe one event.
	record_join_selection_locality(&telemetry, kind, &selection);

	let output = vec![time_join_output_materialization(&telemetry, || {
		materialize_join_selection(
			kind,
			&left_batch,
			&right_batch,
			&selection,
			output_schema,
			task_budget.memory.get(),
			SideTakeTiming::Charged(&telemetry),
		)
	})?];

	record_join_runtime_observation(
		&telemetry,
		&join_runtime_observation_mode,
		JoinRuntimeObservation {
			replay_id,

			strategy,

			build_rows: RowCount::new(left_batch.num_rows() as u64),

			probe_rows: RowCount::new(right_batch.num_rows() as u64),

			output_rows: RowCount::new(record_batch_row_count(&output)),
		},
	);

	Ok(output)
}

#[allow(clippy::too_many_arguments)]

pub(super) async fn execute_hash_join_into_global_aggregate(
	executor: &Executor,

	left_schema: arrow::datatypes::SchemaRef,

	right_schema: arrow::datatypes::SchemaRef,

	left_batches: &[RecordBatch],

	right_batches: &[RecordBatch],

	kind: JoinKind,

	left_keys: &[crate::ir::ColumnOrdinal],

	right_keys: &[crate::ir::ColumnOrdinal],

	match_filter: Option<&BoundExprProgram>,

	null_safe_keys: bool,

	replay_id: JoinReplayId,

	strategy: JoinStrategy,

	join_output_schema: &arrow::datatypes::SchemaRef,

	filter_predicate: Option<&BoundExprProgram>,

	aggr_expr: &[BoundAggregateCall],

	aggregate_state: &mut [AggState],

	task_budget: TaskBudget,

	telemetry: JoinTiming,

	join_runtime_observation_mode: JoinRuntimeObservationMode,

	spill_file_ops: Arc<dyn AsyncFileOps>,

	spill_dir: PathBuf,
) -> Result<(), ExecutionError> {
	if left_keys.len() != right_keys.len() {
		return Err(ExecutionError::InvalidExpression {
			message: "join key arity mismatch".to_string(),
		});
	}

	if kind != JoinKind::Inner {
		return Err(ExecutionError::InvalidExpression {
			message: "streaming collapsing aggregate supports only inner joins".to_string(),
		});
	}

	// ADR0551-SITE: join/execute_hash_join_into_global_aggregate/Build/1

	let mut streamed_build_scope = telemetry.open_operator_transition(PhaseOperatorKind::Build);

	let streamed_build = match try_create_streamed_spilled_inner_build(
		left_schema.clone(),
		right_schema.clone(),
		left_batches,
		kind,
		left_keys,
		right_keys,
		match_filter,
		null_safe_keys,
		task_budget,
		spill_file_ops.clone(),
		spill_dir.clone(),
	)
	.await
	{
		Ok(build) => build,

		Err(error) => {
			streamed_build_scope.discard_before_measurement();

			return Err(error);
		}
	};

	if let Some((mut spilled, key_plan)) = streamed_build {
		let attributed_spill_bytes = spilled.spill_bytes();

		record_task_telemetry(&telemetry, |task| {
			streamed_build_scope.finish_at_measurement();

			task.counters.build_rows = task
				.counters
				.build_rows
				.saturating_add(spilled.input_row_count());

			task.counters.spill_bytes = task
				.counters
				.spill_bytes
				.saturating_add(spilled.spill_bytes());
		});

		streamed_build_scope.commit_after_write();

		// ADR0551-SITE: join/execute_hash_join_into_global_aggregate/Probe/1

		let mut streamed_probe_scope = telemetry.open_operator_transition(PhaseOperatorKind::Probe);

		let result = fold_streamed_spilled_inner_join_into_global_aggregate(
			executor,
			&mut spilled,
			right_batches,
			right_keys,
			key_plan,
			null_safe_keys,
			join_output_schema,
			filter_predicate,
			aggr_expr,
			aggregate_state,
			task_budget.memory.get(),
		)
		.await;

		let build_rows = spilled.input_row_count();

		record_task_telemetry(&telemetry, |task| {
			streamed_probe_scope.finish_at_measurement();

			if let Ok((probe_rows, output_rows)) = &result {
				task.counters.probe_rows = task.counters.probe_rows.saturating_add(*probe_rows);

				task.counters.matched_rows =
					task.counters.matched_rows.saturating_add(*output_rows);
			}
		});

		streamed_probe_scope.commit_after_write();

		if let Ok((probe_rows, output_rows)) = &result {
			record_join_runtime_observation(
				&telemetry,
				&join_runtime_observation_mode,
				JoinRuntimeObservation {
					replay_id,

					strategy,

					build_rows: RowCount::new(build_rows),

					probe_rows: RowCount::new(*probe_rows),

					output_rows: RowCount::new(*output_rows),
				},
			);
		}

		record_join_spill_observation(
			&telemetry,
			replay_id,
			crate::distributed::task::JoinSpillPath::StreamedSpilledBuild,
			attributed_spill_bytes,
		);

		spilled.cleanup().await;

		result?;

		return Ok(());
	}

	streamed_build_scope.discard_before_measurement();

	let left_batch =
		time_join_input_materialization(&telemetry, || concat_or_empty(left_schema, left_batches))?;

	let right_batch = time_join_input_materialization(&telemetry, || {
		concat_or_empty(right_schema, right_batches)
	})?;

	let left_key_arrays = key_arrays(&left_batch, left_keys)?;

	let right_key_arrays = key_arrays(&right_batch, right_keys)?;

	record_task_telemetry(&telemetry, |task| {
		task.counters.build_rows = task
			.counters
			.build_rows
			.saturating_add(left_batch.num_rows() as u64);

		task.counters.probe_rows = task
			.counters
			.probe_rows
			.saturating_add(right_batch.num_rows() as u64);
	});

	if estimated_build_hash_bytes(&left_key_arrays, left_batch.num_rows())
		> task_budget.memory.get()
	{
		let spill_before = task_spill_bytes(&telemetry);

		let result = execute_spilled_hash_join_into_global_aggregate(
			executor,
			kind,
			&left_batch,
			&right_batch,
			&left_key_arrays,
			&right_key_arrays,
			join_output_schema,
			filter_predicate,
			aggr_expr,
			aggregate_state,
			match_filter,
			null_safe_keys,
			task_budget,
			telemetry.clone(),
			spill_file_ops,
			spill_dir,
		)
		.await;

		record_join_spill_observation(
			&telemetry,
			replay_id,
			crate::distributed::task::JoinSpillPath::EstimateSpillFallback,
			task_spill_bytes(&telemetry).saturating_sub(spill_before),
		);

		if let Ok(output_rows) = &result {
			record_join_runtime_observation(
				&telemetry,
				&join_runtime_observation_mode,
				JoinRuntimeObservation {
					replay_id,

					strategy,

					build_rows: RowCount::new(left_batch.num_rows() as u64),

					probe_rows: RowCount::new(right_batch.num_rows() as u64),

					output_rows: RowCount::new(*output_rows),
				},
			);
		}

		return result.map(|_| ());
	}

	let join_key_plan =
		JoinKeyPlan::from_key_arrays(&left_key_arrays, &right_key_arrays, null_safe_keys);

	let build_started = Instant::now();

	// ADR0551-SITE: join/execute_hash_join_into_global_aggregate/Build/2

	let mut build_scope = telemetry.open_operator_transition(PhaseOperatorKind::Build);

	let build_rows = match build_in_memory_join_rows(
		join_key_plan,
		&left_key_arrays,
		left_batch.num_rows(),
		task_budget.memory.get(),
		null_safe_keys,
	) {
		Ok(Some(build_rows)) => build_rows,

		Err(error) => {
			build_scope.discard_before_measurement();

			return Err(error);
		}

		Ok(None) => {
			build_scope.discard_before_measurement();

			let spill_before = task_spill_bytes(&telemetry);

			let result = execute_spilled_hash_join_into_global_aggregate(
				executor,
				kind,
				&left_batch,
				&right_batch,
				&left_key_arrays,
				&right_key_arrays,
				join_output_schema,
				filter_predicate,
				aggr_expr,
				aggregate_state,
				match_filter,
				null_safe_keys,
				task_budget,
				telemetry.clone(),
				spill_file_ops,
				spill_dir,
			)
			.await;

			record_join_spill_observation(
				&telemetry,
				replay_id,
				crate::distributed::task::JoinSpillPath::BuildSpillFallback,
				task_spill_bytes(&telemetry).saturating_sub(spill_before),
			);

			if let Ok(output_rows) = &result {
				record_join_runtime_observation(
					&telemetry,
					&join_runtime_observation_mode,
					JoinRuntimeObservation {
						replay_id,

						strategy,

						build_rows: RowCount::new(left_batch.num_rows() as u64),

						probe_rows: RowCount::new(right_batch.num_rows() as u64),

						output_rows: RowCount::new(*output_rows),
					},
				);
			}

			return result.map(|_| ());
		}
	};

	build_scope.finish_at_measurement();

	let build_micros = duration_micros(build_started);

	record_task_telemetry(&telemetry, |_task| {
		build_scope.commit_after_write();
	});

	record_join_build_profile(
		&telemetry,
		replay_id,
		strategy,
		join_key_plan,
		&build_rows,
		build_micros,
	);

	// ADR0551-SITE: join/execute_hash_join_into_global_aggregate/Probe/2

	let mut probe_scope = telemetry.open_operator_transition(PhaseOperatorKind::Probe);

	let selection = match probe_in_memory_join_rows(
		kind,
		join_key_plan,
		&build_rows,
		left_batch.num_rows(),
		&right_key_arrays,
		right_batch.num_rows(),
		match_filter,
		&left_batch,
		&right_batch,
		null_safe_keys,
	) {
		Ok(selection) => selection,

		Err(error) => {
			probe_scope.discard_before_measurement();

			return Err(error);
		}
	};

	record_task_telemetry(&telemetry, |task| {
		probe_scope.finish_at_measurement();

		task.counters.matched_rows = task
			.counters
			.matched_rows
			.saturating_add(selection.len() as u64);
	});

	probe_scope.commit_after_write();

	fold_join_selection_chunks_into_global_aggregate(
		executor,
		kind,
		&left_batch,
		&right_batch,
		&selection,
		join_output_schema,
		filter_predicate,
		aggr_expr,
		aggregate_state,
		task_budget.memory.get(),
	)?;

	record_join_runtime_observation(
		&telemetry,
		&join_runtime_observation_mode,
		JoinRuntimeObservation {
			replay_id,

			strategy,

			build_rows: RowCount::new(left_batch.num_rows() as u64),

			probe_rows: RowCount::new(right_batch.num_rows() as u64),

			output_rows: RowCount::new(selection.len() as u64),
		},
	);

	record_join_spill_observation(
		&telemetry,
		replay_id,
		crate::distributed::task::JoinSpillPath::InMemory,
		0,
	);

	Ok(())
}

/// Maximum output rows accumulated before a CROSS-join batch is flushed — bounds the match-all

/// selection allocation so a large cartesian streams out in chunks (ADR 239 D5).

pub(super) const CROSS_JOIN_OUTPUT_CHUNK_ROWS: usize = 8192;

/// Explicit keyless match-all for `JoinKind::Cross` (ADR 239 D5): emit every build×probe pair.

/// Unlike the hash path this never keys on `join_key_bytes([])`, so a CROSS join cannot be mistaken

/// for — or silently degrade into — a single-bucket empty-key hash join. The output is chunked to

/// bound allocation; because there is exactly one match-all path, in-memory and would-be-spilled

/// execution produce identical cartesian rows.

pub(super) fn execute_cross_join_match_all(
	left_batch: &RecordBatch,

	right_batch: &RecordBatch,

	output_schema: &arrow::datatypes::SchemaRef,

	memory_cap: u64,

	replay_id: JoinReplayId,

	strategy: JoinStrategy,

	telemetry: &Option<Arc<Mutex<TaskTelemetry>>>,

	join_runtime_observation_mode: &JoinRuntimeObservationMode,
) -> Result<Vec<RecordBatch>, ExecutionError> {
	let left_rows = left_batch.num_rows();

	let right_rows = right_batch.num_rows();

	record_task_telemetry(telemetry, |task| {
		task.counters.build_rows = task.counters.build_rows.saturating_add(left_rows as u64);

		task.counters.probe_rows = task.counters.probe_rows.saturating_add(right_rows as u64);
	});

	let mut output = Vec::new();

	if left_rows > 0 && right_rows > 0 {
		let mut selection: Vec<JoinOutputRow> = Vec::new();

		for left in 0..left_rows {
			let left = join_build_row_index(left)?;

			for right in 0..right_rows {
				let right = join_probe_row_index(right)?;

				selection.push(JoinOutputRow::Paired { left, right });

				if selection.len() >= CROSS_JOIN_OUTPUT_CHUNK_ROWS {
					output.push(time_join_output_materialization(telemetry, || {
						materialize_join_selection(
							JoinKind::Cross,
							left_batch,
							right_batch,
							&selection,
							output_schema,
							memory_cap,
							SideTakeTiming::Charged(telemetry),
						)
					})?);

					selection.clear();
				}
			}
		}

		if !selection.is_empty() {
			output.push(time_join_output_materialization(telemetry, || {
				materialize_join_selection(
					JoinKind::Cross,
					left_batch,
					right_batch,
					&selection,
					output_schema,
					memory_cap,
					SideTakeTiming::Charged(telemetry),
				)
			})?);
		}
	}

	if output.is_empty() {
		output.push(RecordBatch::new_empty(output_schema.clone()));
	}

	let output_rows = record_batch_row_count(&output);

	record_task_telemetry(telemetry, |task| {
		task.counters.matched_rows = task.counters.matched_rows.saturating_add(output_rows);
	});

	record_join_runtime_observation(
		telemetry,
		join_runtime_observation_mode,
		JoinRuntimeObservation {
			replay_id,

			strategy,

			build_rows: RowCount::new(left_rows as u64),

			probe_rows: RowCount::new(right_rows as u64),

			output_rows: RowCount::new(output_rows),
		},
	);

	Ok(output)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]

pub(super) enum JoinOutputRow {
	Paired { left: u32, right: u32 },

	LeftFillNull { left: u32 },

	RightFillNull { right: u32 },

	LeftPreserved { left: u32 },

	RightPreserved { right: u32 },
}

pub(super) fn materialize_join_selection(
	kind: JoinKind,

	left_batch: &RecordBatch,

	right_batch: &RecordBatch,

	selection: &[JoinOutputRow],

	output_schema: &arrow::datatypes::SchemaRef,

	memory_cap: u64,

	side_timing: SideTakeTiming<'_>,
) -> Result<RecordBatch, ExecutionError> {
	if selection.is_empty() {
		return Ok(RecordBatch::new_empty(output_schema.clone()));
	}

	let estimated_output_bytes =
		estimate_join_materialization_bytes(kind, left_batch, right_batch, selection.len());

	let materialization_cap = join_output_materialization_memory_cap(memory_cap);

	if estimated_output_bytes > materialization_cap {
		return Err(budget_exhausted(
			BudgetDimension::Memory,
			"join output materialization",
			estimated_output_bytes,
			materialization_cap,
		));
	}

	let output_columns = match kind {
		JoinKind::Inner
		| JoinKind::Cross
		| JoinKind::LeftOuter
		| JoinKind::RightOuter
		| JoinKind::FullOuter => {
			let mut left_indices = Vec::with_capacity(selection.len());

			let mut right_indices = Vec::with_capacity(selection.len());

			for row in selection {
				match *row {
					JoinOutputRow::Paired { left, right } => {
						left_indices.push(Some(left));

						right_indices.push(Some(right));
					}

					JoinOutputRow::LeftFillNull { left } => {
						left_indices.push(Some(left));

						right_indices.push(None);
					}

					JoinOutputRow::RightFillNull { right } => {
						left_indices.push(None);

						right_indices.push(Some(right));
					}

					JoinOutputRow::LeftPreserved { .. } | JoinOutputRow::RightPreserved { .. } => {
						return Err(invalid_join_selection(kind, row));
					}
				}
			}

			let left_indices = UInt32Array::from(left_indices);

			let right_indices = UInt32Array::from(right_indices);

			let mut columns = side_timing.time_left(|| {
				let mut columns =
					Vec::with_capacity(left_batch.num_columns() + right_batch.num_columns());
				for column in left_batch.columns() {
					columns.push(arrow::compute::take(column, &left_indices, None)?);
				}
				Ok(columns)
			})?;

			side_timing.time_right(|| {
				for column in right_batch.columns() {
					columns.push(arrow::compute::take(column, &right_indices, None)?);
				}
				Ok(())
			})?;

			columns
		}

		JoinKind::LeftSemi | JoinKind::LeftAnti => {
			let mut left_indices = Vec::with_capacity(selection.len());

			for row in selection {
				match *row {
					JoinOutputRow::LeftPreserved { left } => left_indices.push(Some(left)),

					_ => return Err(invalid_join_selection(kind, row)),
				}
			}

			let left_indices = UInt32Array::from(left_indices);

			side_timing.time_left(|| {
				let mut columns = Vec::with_capacity(left_batch.num_columns());
				for column in left_batch.columns() {
					columns.push(arrow::compute::take(column, &left_indices, None)?);
				}
				Ok(columns)
			})?
		}

		JoinKind::RightSemi | JoinKind::RightAnti => {
			let mut right_indices = Vec::with_capacity(selection.len());

			for row in selection {
				match *row {
					JoinOutputRow::RightPreserved { right } => right_indices.push(Some(right)),

					_ => return Err(invalid_join_selection(kind, row)),
				}
			}

			let right_indices = UInt32Array::from(right_indices);

			side_timing.time_right(|| {
				let mut columns = Vec::with_capacity(right_batch.num_columns());
				for column in right_batch.columns() {
					columns.push(arrow::compute::take(column, &right_indices, None)?);
				}
				Ok(columns)
			})?
		}
	};

	Ok(RecordBatch::try_new(output_schema.clone(), output_columns)?)
}

pub(super) fn materialize_reoriented_inner_selection(
	probe_batch: &RecordBatch,

	build_batch: &RecordBatch,

	selection: &[JoinOutputRow],

	output_schema: &arrow::datatypes::SchemaRef,

	memory_cap: u64,

	probe_columns_first: bool,
) -> Result<RecordBatch, ExecutionError> {
	if selection.is_empty() {
		return Ok(RecordBatch::new_empty(output_schema.clone()));
	}

	let estimated_output_bytes = estimate_join_materialization_bytes(
		JoinKind::Inner,
		probe_batch,
		build_batch,
		selection.len(),
	);

	let materialization_cap = join_output_materialization_memory_cap(memory_cap);

	if estimated_output_bytes > materialization_cap {
		return Err(budget_exhausted(
			BudgetDimension::Memory,
			"reoriented join output materialization",
			estimated_output_bytes,
			materialization_cap,
		));
	}

	let mut probe_indices = Vec::with_capacity(selection.len());

	let mut build_indices = Vec::with_capacity(selection.len());

	for row in selection {
		match *row {
			JoinOutputRow::Paired { left, right } => {
				build_indices.push(Some(left));

				probe_indices.push(Some(right));
			}

			_ => return Err(invalid_join_selection(JoinKind::Inner, row)),
		}
	}

	let probe_indices = UInt32Array::from(probe_indices);

	let build_indices = UInt32Array::from(build_indices);

	let mut columns = Vec::with_capacity(probe_batch.num_columns() + build_batch.num_columns());

	if probe_columns_first {
		for column in probe_batch.columns() {
			columns.push(arrow::compute::take(column, &probe_indices, None)?);
		}

		for column in build_batch.columns() {
			columns.push(arrow::compute::take(column, &build_indices, None)?);
		}
	} else {
		for column in build_batch.columns() {
			columns.push(arrow::compute::take(column, &build_indices, None)?);
		}

		for column in probe_batch.columns() {
			columns.push(arrow::compute::take(column, &probe_indices, None)?);
		}
	}

	Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
}

pub(super) fn materialize_preserved_selection(
	preserved_batch: &RecordBatch,

	selection: &[u32],

	output_schema: &arrow::datatypes::SchemaRef,

	memory_cap: u64,
) -> Result<RecordBatch, ExecutionError> {
	if selection.is_empty() {
		return Ok(RecordBatch::new_empty(output_schema.clone()));
	}

	let estimated_output_bytes = join_take_index_bytes(selection.len()).saturating_add(
		estimate_selected_batch_bytes(preserved_batch, selection.len()),
	);

	let materialization_cap = join_output_materialization_memory_cap(memory_cap);

	if estimated_output_bytes > materialization_cap {
		return Err(budget_exhausted(
			BudgetDimension::Memory,
			"semi/anti preserved output materialization",
			estimated_output_bytes,
			materialization_cap,
		));
	}

	let indices = UInt32Array::from(selection.iter().copied().map(Some).collect::<Vec<_>>());

	let mut columns = Vec::with_capacity(preserved_batch.num_columns());

	for column in preserved_batch.columns() {
		columns.push(arrow::compute::take(column, &indices, None)?);
	}

	Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
}

pub(super) fn estimate_join_materialization_bytes(
	kind: JoinKind,

	left_batch: &RecordBatch,

	right_batch: &RecordBatch,

	output_rows: usize,
) -> u64 {
	let mut bytes = 0u64;

	match kind {
		JoinKind::Inner
		| JoinKind::Cross
		| JoinKind::LeftOuter
		| JoinKind::RightOuter
		| JoinKind::FullOuter => {
			bytes = bytes
				.saturating_add(join_take_index_bytes(output_rows))
				.saturating_add(join_take_index_bytes(output_rows))
				.saturating_add(estimate_selected_batch_bytes(left_batch, output_rows))
				.saturating_add(estimate_selected_batch_bytes(right_batch, output_rows));
		}

		JoinKind::LeftSemi | JoinKind::LeftAnti => {
			bytes = bytes
				.saturating_add(join_take_index_bytes(output_rows))
				.saturating_add(estimate_selected_batch_bytes(left_batch, output_rows));
		}

		JoinKind::RightSemi | JoinKind::RightAnti => {
			bytes = bytes
				.saturating_add(join_take_index_bytes(output_rows))
				.saturating_add(estimate_selected_batch_bytes(right_batch, output_rows));
		}
	}

	bytes
}

pub(super) fn join_output_materialization_memory_cap(memory_cap: u64) -> u64 {
	memory_cap.saturating_mul(JOIN_OUTPUT_MATERIALIZATION_MEMORY_CAP_MULTIPLIER)
}

pub(super) fn join_take_index_bytes(rows: usize) -> u64 {
	let rows = u64::try_from(rows).unwrap_or(u64::MAX);

	rows.saturating_mul(std::mem::size_of::<u32>() as u64)
		.saturating_add(rows.saturating_add(7) / 8)
}

pub(super) fn join_interleave_index_bytes(rows: usize) -> u64 {
	let rows = u64::try_from(rows).unwrap_or(u64::MAX);

	rows.saturating_mul(std::mem::size_of::<(usize, usize)>() as u64)
}

pub(super) fn estimate_selected_batch_bytes(batch: &RecordBatch, output_rows: usize) -> u64 {
	batch
		.columns()
		.iter()
		.map(|column| estimate_selected_column_bytes(column, batch.num_rows(), output_rows))
		.fold(0u64, u64::saturating_add)
}

pub(super) fn estimate_spilled_inner_build_selection_bytes(
	build_pages: &[RecordBatch],

	output_rows: usize,
) -> u64 {
	let source_rows = build_pages
		.iter()
		.map(RecordBatch::num_rows)
		.fold(0usize, usize::saturating_add);

	let Some(first_page) = build_pages.first() else {
		return 0;
	};

	(0..first_page.num_columns())
		.map(|column_index| {
			let source_bytes = build_pages
				.iter()
				.map(|page| {
					u64::try_from(page.column(column_index).get_array_memory_size())
						.unwrap_or(u64::MAX)
				})
				.fold(0u64, u64::saturating_add);

			estimate_selected_bytes_from_source(
				source_bytes,
				source_rows,
				first_page.column(column_index).data_type(),
				output_rows,
			)
		})
		.fold(0u64, u64::saturating_add)
}

pub(super) fn estimate_selected_column_bytes(
	column: &ArrayRef,

	source_rows: usize,

	output_rows: usize,
) -> u64 {
	if output_rows == 0 {
		return 0;
	}

	let bytes_per_row = if source_rows == 0 {
		empty_source_column_row_width(column.data_type())
	} else {
		let source_bytes = u64::try_from(column.get_array_memory_size()).unwrap_or(u64::MAX);

		let source_rows = u64::try_from(source_rows).unwrap_or(u64::MAX);

		source_bytes.saturating_add(source_rows.saturating_sub(1)) / source_rows
	};

	let output_rows = u64::try_from(output_rows).unwrap_or(u64::MAX);

	bytes_per_row.saturating_mul(output_rows)
}

pub(super) fn estimate_selected_bytes_from_source(
	source_bytes: u64,

	source_rows: usize,

	data_type: &DataType,

	output_rows: usize,
) -> u64 {
	if output_rows == 0 {
		return 0;
	}

	let bytes_per_row = if source_rows == 0 {
		empty_source_column_row_width(data_type)
	} else {
		let source_rows = u64::try_from(source_rows).unwrap_or(u64::MAX);

		source_bytes.saturating_add(source_rows.saturating_sub(1)) / source_rows
	};

	let output_rows = u64::try_from(output_rows).unwrap_or(u64::MAX);

	bytes_per_row.saturating_mul(output_rows)
}

pub(super) fn empty_source_column_row_width(data_type: &DataType) -> u64 {
	match data_type {
		DataType::Boolean | DataType::Int8 | DataType::UInt8 => 1,

		DataType::Int16 | DataType::UInt16 => 2,

		DataType::Int32 | DataType::UInt32 | DataType::Float32 | DataType::Date32 => 4,

		DataType::Int64
		| DataType::UInt64
		| DataType::Float64
		| DataType::Timestamp(_, _)
		| DataType::Duration(_) => 8,

		DataType::Decimal128(_, _) => 16,

		DataType::Utf8 | DataType::Binary => 8,

		DataType::LargeUtf8 | DataType::LargeBinary => 16,

		_ => 16,
	}
}

pub(super) fn invalid_join_selection(kind: JoinKind, row: &JoinOutputRow) -> ExecutionError {
	ExecutionError::Kernel {
		message: format!("invalid {kind:?} join selection row: {row:?}"),
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]

pub(super) enum JoinKey {
	Int32(i32),

	Int64(i64),

	Generic(Vec<u8>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]

pub(super) enum JoinKeyPlan {
	Int32,

	Int64,

	Generic,
}

impl JoinKeyPlan {
	pub(super) fn label(self) -> &'static str {
		match self {
			Self::Int32 => "int32",

			Self::Int64 => "int64",

			Self::Generic => "generic",
		}
	}

	pub(super) fn from_key_arrays(
		left_key_arrays: &[ArrayRef],

		right_key_arrays: &[ArrayRef],

		null_safe_keys: bool,
	) -> Self {
		if null_safe_keys {
			return Self::Generic;
		}

		if left_key_arrays.len() != 1 || right_key_arrays.len() != 1 {
			return Self::Generic;
		}

		if left_key_arrays[0].data_type() != right_key_arrays[0].data_type() {
			return Self::Generic;
		}

		match left_key_arrays[0].data_type() {
			DataType::Int32 => Self::Int32,

			DataType::Int64 => Self::Int64,

			_ => Self::Generic,
		}
	}

	pub(super) fn from_build_arrays_and_probe_schema(
		build_key_arrays: &[ArrayRef],

		probe_schema: &SchemaRef,

		probe_keys: &[crate::ir::ColumnOrdinal],

		null_safe_keys: bool,
	) -> Result<Self, ExecutionError> {
		if null_safe_keys {
			return Ok(Self::Generic);
		}

		if build_key_arrays.len() != 1 || probe_keys.len() != 1 {
			return Ok(Self::Generic);
		}

		let probe_index = usize::try_from(probe_keys[0].0).expect("column ordinal fits usize");

		let Some(probe_field) = probe_schema.fields().get(probe_index) else {
			return Err(ExecutionError::InvalidExpression {
				message: format!("probe join key ordinal {probe_index} out of bounds"),
			});
		};

		if build_key_arrays[0].data_type() != probe_field.data_type() {
			return Ok(Self::Generic);
		}

		Ok(match build_key_arrays[0].data_type() {
			DataType::Int32 => Self::Int32,

			DataType::Int64 => Self::Int64,

			_ => Self::Generic,
		})
	}

	pub(super) fn from_build_schema_and_probe_schema(
		build_schema: &SchemaRef,

		build_keys: &[crate::ir::ColumnOrdinal],

		probe_schema: &SchemaRef,

		probe_keys: &[crate::ir::ColumnOrdinal],

		null_safe_keys: bool,
	) -> Result<Self, ExecutionError> {
		if null_safe_keys {
			return Ok(Self::Generic);
		}

		if build_keys.len() != 1 || probe_keys.len() != 1 {
			return Ok(Self::Generic);
		}

		let build_index = usize::try_from(build_keys[0].0).expect("column ordinal fits usize");

		let probe_index = usize::try_from(probe_keys[0].0).expect("column ordinal fits usize");

		let Some(build_field) = build_schema.fields().get(build_index) else {
			return Err(ExecutionError::InvalidExpression {
				message: format!("build join key ordinal {build_index} out of bounds"),
			});
		};

		let Some(probe_field) = probe_schema.fields().get(probe_index) else {
			return Err(ExecutionError::InvalidExpression {
				message: format!("probe join key ordinal {probe_index} out of bounds"),
			});
		};

		if build_field.data_type() != probe_field.data_type() {
			return Ok(Self::Generic);
		}

		Ok(match build_field.data_type() {
			DataType::Int32 => Self::Int32,

			DataType::Int64 => Self::Int64,

			_ => Self::Generic,
		})
	}
}

pub(super) fn join_key_at(
	join_key_plan: JoinKeyPlan,

	key_arrays: &[ArrayRef],

	row: usize,

	null_safe_keys: bool,
) -> Result<Option<JoinKey>, ExecutionError> {
	Ok(match join_key_plan {
		JoinKeyPlan::Int32 => {
			let values = single_join_key_array::<Int32Array>(key_arrays, "Int32")?;

			if values.is_null(row) {
				None
			} else {
				Some(JoinKey::Int32(values.value(row)))
			}
		}

		JoinKeyPlan::Int64 => {
			let values = single_join_key_array::<Int64Array>(key_arrays, "Int64")?;

			if values.is_null(row) {
				None
			} else {
				Some(JoinKey::Int64(values.value(row)))
			}
		}

		JoinKeyPlan::Generic if null_safe_keys => {
			Some(JoinKey::Generic(join_key_bytes_null_safe(key_arrays, row)?))
		}

		JoinKeyPlan::Generic => join_key_bytes(key_arrays, row)?.map(JoinKey::Generic),
	})
}

/// ADR 497: drive one primitive-key probe arm through the staged batch window

/// (or the historical one-key-at-a-time lookup under the ablation), leaving

/// per-row match handling identical to the scalar loop it replaces.

fn probe_primitive_key_rows<'table, K: Copy>(
	kind: JoinKind,

	rows: usize,

	key_at: impl Fn(usize) -> Option<K>,

	lookup_scalar: impl Fn(K) -> Option<&'table BuildRowList>,

	lookup_window: impl Fn(&[Option<K>], &mut Vec<Option<&'table BuildRowList>>),

	matched_build_rows: &mut Option<Vec<u64>>,

	selection: &mut Vec<JoinOutputRow>,

	residual: &mut JoinMatchResidual,

	mut profile: Option<&mut InMemoryProbeProfile>,
) -> Result<(), ExecutionError> {
	let batched = build_rows::batched_probe_prefetch_enabled();

	let window = if batched {
		build_rows::PROBE_BATCH_WINDOW
	} else {
		1
	};

	let mut window_keys = Vec::with_capacity(window);

	let mut window_lookups = Vec::with_capacity(window);

	let mut row = 0usize;

	while row < rows {
		let end = (row + window).min(rows);

		window_lookups.clear();

		let scalar_key = (!batched).then(|| key_at(row));

		if batched {
			window_keys.clear();

			window_keys.extend((row..end).map(&key_at));
		}

		if let Some(profile) = profile.as_deref_mut() {
			let keys = if batched {
				window_keys.iter().filter(|key| key.is_some()).count()
			} else {
				usize::from(scalar_key.flatten().is_some())
			};

			profile.record_lookup_window(keys);
		}

		let lookup_started = profile.as_ref().map(|_| Instant::now());

		if batched {
			lookup_window(&window_keys, &mut window_lookups);
		} else {
			window_lookups.push(scalar_key.flatten().and_then(&lookup_scalar));
		}

		if let (Some(profile), Some(started)) = (profile.as_deref_mut(), lookup_started) {
			profile.record_lookup_duration(started.elapsed());
		}

		let match_started = profile.as_ref().map(|_| Instant::now());

		for (offset, lookup) in window_lookups.iter().enumerate() {
			let row = row + offset;

			let matched = if let Some(matches) = lookup {
				append_join_matches_for_kind(
					kind,
					row,
					matches.rows(),
					matched_build_rows.as_deref_mut(),
					selection,
					residual,
				)?
			} else {
				false
			};

			if !matched {
				append_unmatched_probe_row(kind, row, selection)?;
			}
		}

		if let (Some(profile), Some(started)) = (profile.as_deref_mut(), match_started) {
			profile.record_match_expand_duration(started.elapsed());
		}

		row = end;
	}

	Ok(())
}

pub(super) fn probe_in_memory_join_rows(
	kind: JoinKind,

	join_key_plan: JoinKeyPlan,

	build_rows: &JoinBuildRows,

	left_rows: usize,

	right_key_arrays: &[ArrayRef],

	rows: usize,

	match_filter: Option<&BoundExprProgram>,

	left_batch: &RecordBatch,

	right_batch: &RecordBatch,

	null_safe_keys: bool,
) -> Result<Vec<JoinOutputRow>, ExecutionError> {
	probe_in_memory_join_rows_profiled(
		kind,
		join_key_plan,
		build_rows,
		left_rows,
		right_key_arrays,
		rows,
		match_filter,
		left_batch,
		right_batch,
		null_safe_keys,
		None,
	)
}

#[derive(Default)]

pub(super) struct InMemoryProbeProfile {
	pub(super) lookup: std::time::Duration,

	pub(super) match_expand: std::time::Duration,

	pub(super) lookup_windows: u64,

	pub(super) non_null_keys: u64,

	pub(super) supported: bool,

	pub(super) error: Option<&'static str>,
}

impl InMemoryProbeProfile {
	fn record_lookup_window(&mut self, non_null_keys: usize) {
		self.supported = true;

		let Some(lookup_windows) = self.lookup_windows.checked_add(1) else {
			self.error = Some("suffix lookup-window count overflowed");

			return;
		};

		let Ok(non_null_keys) = u64::try_from(non_null_keys) else {
			self.error = Some("suffix non-null-key count overflowed");

			return;
		};

		let Some(total_non_null_keys) = self.non_null_keys.checked_add(non_null_keys) else {
			self.error = Some("suffix non-null-key count overflowed");

			return;
		};

		self.lookup_windows = lookup_windows;

		self.non_null_keys = total_non_null_keys;
	}

	fn record_lookup_duration(&mut self, elapsed: std::time::Duration) {
		let Some(total) = self.lookup.checked_add(elapsed) else {
			self.error = Some("suffix lookup duration overflowed");

			return;
		};

		self.lookup = total;
	}

	fn record_match_expand_duration(&mut self, elapsed: std::time::Duration) {
		let Some(total) = self.match_expand.checked_add(elapsed) else {
			self.error = Some("suffix match-expand duration overflowed");

			return;
		};

		self.match_expand = total;
	}
}

#[allow(clippy::too_many_arguments)]

pub(super) fn probe_in_memory_join_rows_profiled(
	kind: JoinKind,

	join_key_plan: JoinKeyPlan,

	build_rows: &JoinBuildRows,

	left_rows: usize,

	right_key_arrays: &[ArrayRef],

	rows: usize,

	match_filter: Option<&BoundExprProgram>,

	left_batch: &RecordBatch,

	right_batch: &RecordBatch,

	null_safe_keys: bool,

	mut profile: Option<&mut InMemoryProbeProfile>,
) -> Result<Vec<JoinOutputRow>, ExecutionError> {
	let mut selection = Vec::new();

	let mut matched_build_rows = build_match_bitmap(kind, left_rows);

	let mut residual = JoinMatchResidual::new(match_filter, left_batch, right_batch);

	match join_key_plan {
		JoinKeyPlan::Int32 => {
			let values = single_join_key_array::<Int32Array>(right_key_arrays, "Int32")?;

			probe_primitive_key_rows(
				kind,
				rows,
				|row| (!values.is_null(row)).then(|| values.value(row)),
				|key| build_rows.get_int32(key),
				|keys, out| build_rows.get_int32_batch(keys, out),
				&mut matched_build_rows,
				&mut selection,
				&mut residual,
				profile.as_deref_mut(),
			)?;
		}

		JoinKeyPlan::Int64 => {
			let values = single_join_key_array::<Int64Array>(right_key_arrays, "Int64")?;

			probe_primitive_key_rows(
				kind,
				rows,
				|row| (!values.is_null(row)).then(|| values.value(row)),
				|key| build_rows.get_int64(key),
				|keys, out| build_rows.get_int64_batch(keys, out),
				&mut matched_build_rows,
				&mut selection,
				&mut residual,
				profile.as_deref_mut(),
			)?;
		}

		JoinKeyPlan::Generic => {
			if let Some(profile) = profile.as_deref_mut() {
				profile.supported = false;
			}

			for row in 0..rows {
				let key = if null_safe_keys {
					join_key_bytes_null_safe(right_key_arrays, row)?
				} else {
					let Some(key) = join_key_bytes(right_key_arrays, row)? else {
						append_unmatched_probe_row(kind, row, &mut selection)?;

						continue;
					};

					key
				};

				let key = JoinKey::Generic(key);

				let matched = if let Some(matches) = build_rows.get(&key) {
					append_join_matches_for_kind(
						kind,
						row,
						matches.rows(),
						matched_build_rows.as_deref_mut(),
						&mut selection,
						&mut residual,
					)?
				} else {
					false
				};

				if !matched {
					append_unmatched_probe_row(kind, row, &mut selection)?;
				}
			}
		}
	}

	append_build_side_after_probe_rows(
		kind,
		left_rows,
		matched_build_rows.as_deref(),
		&mut selection,
	)?;

	Ok(selection)
}

pub(super) fn single_join_key_array<'a, T: 'static>(
	key_arrays: &'a [ArrayRef],

	label: &'static str,
) -> Result<&'a T, ExecutionError> {
	key_arrays[0]
		.as_any()
		.downcast_ref::<T>()
		.ok_or_else(|| ExecutionError::InvalidExpression {
			message: format!("join key array is not {label}"),
		})
}

#[inline(always)]

pub(super) fn insert_join_build_row(build_rows: &mut JoinBuildRows, key: JoinKey, row: u32) {
	build_rows.insert(key, row);
}

pub(super) fn append_join_matches_for_kind(
	kind: JoinKind,

	probe_row: usize,

	matches: impl Iterator<Item = u32>,

	mut matched_build_rows: Option<&mut [u64]>,

	selection: &mut Vec<JoinOutputRow>,

	residual: &mut JoinMatchResidual<'_>,
) -> Result<bool, ExecutionError> {
	let right_index = join_probe_row_index(probe_row)?;

	let mut matched = false;

	for left_index in matches {
		if !residual.matches(left_index, right_index)? {
			continue;
		}

		matched = true;

		if let Some(bitmap) = matched_build_rows.as_deref_mut() {
			set_build_row_matched(bitmap, left_index);
		}

		match kind {
			JoinKind::Inner
			| JoinKind::Cross
			| JoinKind::LeftOuter
			| JoinKind::RightOuter
			| JoinKind::FullOuter => selection.push(JoinOutputRow::Paired {
				left: left_index,

				right: right_index,
			}),

			JoinKind::LeftSemi | JoinKind::LeftAnti | JoinKind::RightSemi | JoinKind::RightAnti => {
			}
		}
	}

	if matched && kind == JoinKind::RightSemi {
		selection.push(JoinOutputRow::RightPreserved { right: right_index });
	}

	Ok(matched)
}

pub(super) struct JoinMatchResidual<'a> {
	program: Option<&'a BoundExprProgram>,

	left_batch: &'a RecordBatch,

	right_batch: &'a RecordBatch,

	schema: Option<SchemaRef>,
}

impl<'a> JoinMatchResidual<'a> {
	pub(super) fn new(
		program: Option<&'a BoundExprProgram>,

		left_batch: &'a RecordBatch,

		right_batch: &'a RecordBatch,
	) -> Self {
		let schema = program.map(|_| {
			let fields = left_batch
				.schema()
				.fields()
				.iter()
				.chain(right_batch.schema().fields().iter())
				.map(|field| field.as_ref().clone())
				.collect::<Vec<_>>();

			Arc::new(Schema::new(fields))
		});

		Self {
			program,

			left_batch,

			right_batch,

			schema,
		}
	}

	pub(super) fn matches(
		&mut self,

		left_row: u32,

		right_row: u32,
	) -> Result<bool, ExecutionError> {
		let Some(program) = self.program else {
			return Ok(true);
		};

		let batch = join_match_pair_batch(
			self.left_batch,
			self.right_batch,
			left_row,
			right_row,
			self.schema
				.as_ref()
				.expect("residual schema exists when program exists")
				.clone(),
		)?;

		let mut roots =
			Executor::evaluate_bound_program_without_storage(program, &batch)?.into_iter();

		let Some(root) = roots.next() else {
			return Err(ExecutionError::InvalidExpression {
				message: "JOIN match filter must have exactly one root".to_string(),
			});
		};

		if roots.next().is_some() {
			return Err(ExecutionError::InvalidExpression {
				message: "JOIN match filter must have exactly one root".to_string(),
			});
		}

		let bools = root
			.as_any()
			.downcast_ref::<BooleanArray>()
			.ok_or_else(|| ExecutionError::InvalidExpression {
				message: "JOIN match filter must return Boolean".to_string(),
			})?;

		Ok(bools.is_valid(0) && bools.value(0))
	}
}

pub(super) fn join_match_pair_batch(
	left_batch: &RecordBatch,

	right_batch: &RecordBatch,

	left_row: u32,

	right_row: u32,

	schema: SchemaRef,
) -> Result<RecordBatch, ExecutionError> {
	let left_index = UInt32Array::from(vec![left_row]);

	let right_index = UInt32Array::from(vec![right_row]);

	let mut columns = Vec::with_capacity(left_batch.num_columns() + right_batch.num_columns());

	for column in left_batch.columns() {
		columns.push(take(column, &left_index, None)?);
	}

	for column in right_batch.columns() {
		columns.push(take(column, &right_index, None)?);
	}

	Ok(RecordBatch::try_new(schema, columns)?)
}

pub(super) fn append_unmatched_probe_row(
	kind: JoinKind,

	probe_row: usize,

	selection: &mut Vec<JoinOutputRow>,
) -> Result<(), ExecutionError> {
	let right_index = join_probe_row_index(probe_row)?;

	match kind {
		JoinKind::RightOuter | JoinKind::FullOuter => {
			selection.push(JoinOutputRow::RightFillNull { right: right_index });
		}

		JoinKind::RightAnti => {
			selection.push(JoinOutputRow::RightPreserved { right: right_index });
		}

		JoinKind::Inner
		| JoinKind::Cross
		| JoinKind::LeftOuter
		| JoinKind::LeftSemi
		| JoinKind::RightSemi
		| JoinKind::LeftAnti => {}
	}

	Ok(())
}

pub(super) fn append_build_side_after_probe_rows(
	kind: JoinKind,

	left_rows: usize,

	matched_build_rows: Option<&[u64]>,

	selection: &mut Vec<JoinOutputRow>,
) -> Result<(), ExecutionError> {
	let Some(bitmap) = matched_build_rows else {
		return Ok(());
	};

	for row in 0..left_rows {
		let matched = build_row_matched(bitmap, row);

		match kind {
			JoinKind::LeftOuter | JoinKind::FullOuter if !matched => {
				selection.push(JoinOutputRow::LeftFillNull {
					left: join_build_row_index(row)?,
				});
			}

			JoinKind::LeftSemi if matched => {
				selection.push(JoinOutputRow::LeftPreserved {
					left: join_build_row_index(row)?,
				});
			}

			JoinKind::LeftAnti if !matched => {
				selection.push(JoinOutputRow::LeftPreserved {
					left: join_build_row_index(row)?,
				});
			}

			_ => {}
		}
	}

	Ok(())
}

pub(super) fn build_match_bitmap(kind: JoinKind, rows: usize) -> Option<Vec<u64>> {
	match kind {
		JoinKind::LeftOuter | JoinKind::FullOuter | JoinKind::LeftSemi | JoinKind::LeftAnti => {
			Some(vec![0; rows.div_ceil(64)])
		}

		JoinKind::Inner
		| JoinKind::Cross
		| JoinKind::RightOuter
		| JoinKind::RightSemi
		| JoinKind::RightAnti => None,
	}
}

pub(super) fn set_build_row_matched(bitmap: &mut [u64], row: u32) {
	let row = row as usize;

	let word = row / 64;

	let bit = row % 64;

	if let Some(slot) = bitmap.get_mut(word) {
		*slot |= 1u64 << bit;
	}
}

pub(super) fn build_row_matched(bitmap: &[u64], row: usize) -> bool {
	let word = row / 64;

	let bit = row % 64;

	bitmap
		.get(word)
		.is_some_and(|slot| (*slot & (1u64 << bit)) != 0)
}

pub(super) fn join_build_row_index(row: usize) -> Result<u32, ExecutionError> {
	u32::try_from(row).map_err(|_| ExecutionError::Kernel {
		message: "join build row index exceeds UInt32 take index".to_string(),
	})
}

pub(super) fn join_probe_row_index(row: usize) -> Result<u32, ExecutionError> {
	u32::try_from(row).map_err(|_| ExecutionError::Kernel {
		message: "join probe row index exceeds UInt32 take index".to_string(),
	})
}

#[inline(always)]

pub(super) fn join_build_memory_fits(
	key_arrays: &[ArrayRef],

	rows: usize,

	memory_cap: u64,
) -> bool {
	estimated_build_hash_bytes(key_arrays, rows) <= memory_cap
}

pub(super) fn estimated_build_hash_bytes_for_batches(
	batches: &[RecordBatch],

	keys: &[crate::ir::ColumnOrdinal],
) -> Result<u64, ExecutionError> {
	let mut bytes = 0u64;

	for batch in batches {
		let key_arrays = key_arrays(batch, keys)?;

		bytes = bytes.saturating_add(estimated_build_hash_bytes(&key_arrays, batch.num_rows()));
	}

	Ok(bytes)
}

#[cfg(test)]
mod join_materialization_timing_tests {
	use super::*;
	use crate::distributed::task::TaskTelemetry;

	fn slot() -> Option<Arc<Mutex<TaskTelemetry>>> {
		Some(Arc::new(Mutex::new(TaskTelemetry::default())))
	}

	fn charged(slot: &Option<Arc<Mutex<TaskTelemetry>>>) -> (u64, u64) {
		let guard = slot.as_ref().unwrap().lock().unwrap();
		(
			guard.phases.join_input_materialization_micros,
			guard.phases.join_output_materialization_micros,
		)
	}

	fn busy() -> Result<u64, ExecutionError> {
		// Enough work that the elapsed micros round above zero on any host. A
		// sleep would be flaky under load and slow under nextest.
		let mut total = 0u64;
		for value in 0..2_000_000u64 {
			total = total.wrapping_add(value ^ (total >> 3));
		}
		Ok(total)
	}

	/// The two subterms are charged to their own counters and never to each other.
	#[test]
	fn each_bracket_charges_only_its_own_counter() {
		let task = slot();
		time_join_input_materialization(&task, busy).expect("input body succeeds");
		let (input_only, output_at_rest) = charged(&task);
		assert!(input_only > 0, "the input bracket must charge something");
		assert_eq!(
			output_at_rest, 0,
			"an input concatenation must not land in the output subterm; the two \
			 answer different questions and a shared counter answers neither"
		);

		time_join_output_materialization(&task, busy).expect("output body succeeds");
		let (input_after, output_after) = charged(&task);
		assert_eq!(
			input_after, input_only,
			"timing an output gather must not move the input counter"
		);
		assert!(output_after > 0, "the output bracket must charge something");
	}

	/// Charged on the error path too.
	///
	/// This is the case that decides whether the subterm can be trusted as a
	/// subtraction: `recompute_framework_overhead` removes it from the residual
	/// unconditionally, so a bracket that skipped failures would shrink the
	/// residual by less than it claimed and the wall partition would stop closing.
	/// A concat that exceeds its budget is a real occurrence, not a hypothetical.
	#[test]
	fn both_brackets_charge_when_the_body_fails() {
		let task = slot();
		let failing = || -> Result<u64, ExecutionError> {
			busy()?;
			Err(ExecutionError::InvalidExpression {
				message: "materialization failed".to_string(),
			})
		};

		time_join_input_materialization(&task, failing).expect_err("input body fails");
		time_join_output_materialization(&task, failing).expect_err("output body fails");

		let (input, output) = charged(&task);
		assert!(
			input > 0 && output > 0,
			"time spent before a failure is still time inside the task wall; \
			 dropping it here would silently return it to the residual these \
			 counters exist to reduce"
		);
	}

	/// Repeated brackets accumulate. A join concatenates two inputs, and the
	/// aggregate-fused variant does it again, so a counter that overwrote instead
	/// of adding would report only the last one.
	#[test]
	fn repeated_brackets_accumulate_rather_than_overwrite() {
		let task = slot();
		time_join_input_materialization(&task, busy).expect("first");
		let (after_one, _) = charged(&task);
		time_join_input_materialization(&task, busy).expect("second");
		let (after_two, _) = charged(&task);
		assert!(
			after_two > after_one,
			"two concatenations must charge more than one: {after_two} vs {after_one}"
		);
	}

	/// Absent telemetry is a no-op, not a panic. The local arm runs the same join
	/// code with no task slot.
	#[test]
	fn a_task_without_telemetry_is_unaffected() {
		let none: Option<Arc<Mutex<TaskTelemetry>>> = None;
		time_join_input_materialization(&none, busy).expect("succeeds without a slot");
		time_join_output_materialization(&none, busy).expect("succeeds without a slot");
	}
}

#[cfg(test)]
mod join_selection_locality_tests {
	use super::*;
	use crate::distributed::task::{
		JoinSelectionLocalitySides, RUN_LENGTH_BUCKET_MINIMUMS, TaskTelemetry,
	};

	/// The arithmetic is exercised through the pure measurement, never through the
	/// gated recorder: the gate reads process environment, and a test that set it
	/// would race every other test in the same process.
	fn measure(kind: JoinKind, selection: &[JoinOutputRow]) -> JoinSelectionLocalitySides {
		measure_join_selection_locality(kind, selection)
	}

	fn paired(pairs: &[(u32, u32)]) -> Vec<JoinOutputRow> {
		pairs
			.iter()
			.map(|(left, right)| JoinOutputRow::Paired {
				left: *left,
				right: *right,
			})
			.collect()
	}

	/// A fully ascending side is one run covering every row, and lands in every
	/// bucket its length reaches — and in none above it.
	#[test]
	fn an_ascending_side_is_one_run_in_every_bucket_it_reaches() {
		let selection = paired(&(0..8).map(|row| (7u32, row)).collect::<Vec<_>>());
		let sides = measure(JoinKind::Inner, &selection);

		assert_eq!(sides.right.selected_rows, 8);
		assert_eq!(
			sides.right.ascending_steps, 7,
			"seven +1 steps across 8 rows"
		);
		assert_eq!(sides.right.run_count, 1);
		assert_eq!(sides.right.max_run, 8);
		assert_eq!(
			sides.right.rows_in_runs_at_least,
			[8, 8, 8, 0, 0, 0],
			"a run of 8 covers the >=2, >=4 and >=8 buckets and no higher one; \
			 buckets are {RUN_LENGTH_BUCKET_MINIMUMS:?}"
		);

		assert_eq!(sides.left.duplicate_steps, 7);
		assert_eq!(sides.left.ascending_steps, 0);
		assert_eq!(
			sides.left.max_run, 1,
			"a repeated index is not a run: the gather still pays a lookup per row"
		);
	}

	/// Scatter produces runs of one and reaches no bucket. This is the reading
	/// that would justify abandoning slice reuse, so it must be unambiguous.
	#[test]
	fn a_scattered_side_reaches_no_bucket() {
		let sides = measure(
			JoinKind::Inner,
			&paired(&[(5, 100), (5, 3), (5, 77), (5, 42)]),
		);

		assert_eq!(sides.right.selected_rows, 4);
		assert_eq!(sides.right.ascending_steps, 0);
		assert_eq!(sides.right.run_count, 4, "four runs of one");
		assert_eq!(sides.right.max_run, 1);
		assert_eq!(sides.right.rows_in_runs_at_least, [0; 6]);
	}

	/// The bimodal case a mean run length would hide: one long run beside pure
	/// scatter. The buckets must show both.
	#[test]
	fn buckets_separate_a_long_run_from_surrounding_scatter() {
		let mut pairs: Vec<(u32, u32)> = (0..16).map(|row| (0u32, row)).collect();
		pairs.extend([(0, 900), (0, 31), (0, 700), (0, 12)]);
		let right = measure(JoinKind::Inner, &paired(&pairs)).right;

		assert_eq!(right.selected_rows, 20);
		assert_eq!(right.max_run, 16);
		assert_eq!(right.run_count, 5, "one run of 16 plus four singletons");
		assert_eq!(
			right.rows_in_runs_at_least,
			[16, 16, 16, 16, 0, 0],
			"the 16 contiguous rows are visible at every bucket up to 16; the mean \
			 run length here is 4, which would have suggested no run worth slicing"
		);
		assert_eq!(right.ascending_steps, 15);
	}

	/// A repeated `u32::MAX` is a duplicate, not an ascending step.
	///
	/// `saturating_add(1)` at the boundary returns the predecessor, so the
	/// ascending arm would swallow a repeat and report perfect contiguity on the
	/// one input where there is none. Unreachable at TPC-H scale; pinned because
	/// the counter's whole purpose is to be believed.
	#[test]
	fn a_repeated_maximum_index_is_a_duplicate_not_an_ascending_step() {
		let sides = measure(
			JoinKind::Inner,
			&paired(&[(0, u32::MAX), (0, u32::MAX), (0, u32::MAX)]),
		);

		assert_eq!(
			(sides.right.ascending_steps, sides.right.duplicate_steps),
			(0, 2),
			"u32::MAX repeated must read as duplicates; saturating successor \
			 arithmetic reports (2, 0) here"
		);
		assert_eq!(sides.right.max_run, 1);
		assert_eq!(sides.right.rows_in_runs_at_least, [0; 6]);
	}

	/// A null on one side breaks that side's run without touching the other.
	#[test]
	fn a_null_side_breaks_only_its_own_run() {
		let selection = vec![
			JoinOutputRow::Paired { left: 0, right: 0 },
			JoinOutputRow::Paired { left: 1, right: 1 },
			JoinOutputRow::RightFillNull { right: 2 },
			JoinOutputRow::Paired { left: 2, right: 3 },
		];
		let sides = measure(JoinKind::LeftOuter, &selection);

		assert_eq!(
			(sides.left.selected_rows, sides.left.max_run),
			(3, 2),
			"the left side contributes no index on the nulled row, and its run \
			 cannot span the gap the gather will not see as contiguous"
		);
		assert_eq!(
			(sides.right.selected_rows, sides.right.max_run),
			(4, 4),
			"the right side ascends through all four rows uninterrupted"
		);
	}

	/// A side the gather never touches records nothing; a side it touches records
	/// an invocation even when the selection gave it no index.
	///
	/// These two zeros mean opposite things — "not gathered" versus "gathered
	/// nothing" — and `empty_invocations` is the only thing that separates them.
	#[test]
	fn an_untouched_side_and_an_empty_side_are_distinguishable() {
		let semi = measure(
			JoinKind::LeftSemi,
			&[JoinOutputRow::LeftPreserved { left: 4 }],
		);
		assert_eq!(
			(semi.right.invocations, semi.right.empty_invocations),
			(0, 0),
			"a LeftSemi gather never touches the right side"
		);
		assert_eq!((semi.left.invocations, semi.left.selected_rows), (1, 1));

		// Every row nulls the left side: the gather DOES build left indices (all
		// null) and take() runs on them, so the invocation is real.
		let all_null_left = measure(
			JoinKind::FullOuter,
			&[
				JoinOutputRow::RightFillNull { right: 0 },
				JoinOutputRow::RightFillNull { right: 1 },
			],
		);
		assert_eq!(
			(
				all_null_left.left.invocations,
				all_null_left.left.empty_invocations,
				all_null_left.left.selected_rows
			),
			(1, 1, 0),
			"an outer join that nulls one side entirely gathered that side and got \
			 nothing; that is not the same as never gathering it"
		);
	}

	/// Invocations accumulate; the task-local receipt is a sum, not a snapshot of
	/// the last join. A max is a max across all of them.
	#[test]
	fn repeated_invocations_accumulate_on_the_task() {
		let mut total = JoinSelectionLocalitySides::default();
		for selection in [
			paired(&[(0, 0), (1, 1)]),
			paired(&(0..5).map(|r| (r, r)).collect::<Vec<_>>()),
		] {
			let measured = measure(JoinKind::Inner, &selection);
			total.left.merge(&measured.left);
			total.right.merge(&measured.right);
		}

		assert_eq!(total.left.invocations, 2);
		assert_eq!(total.left.selected_rows, 7);
		assert_eq!(total.left.max_run, 5, "the max spans invocations");
		assert_eq!(total.left.run_count, 2);
		assert_eq!(total.left.empty_invocations, 0);
	}

	/// The probe is OFF unless its environment variable is set.
	///
	/// This is the positive control's negative half: it proves an ordinary atlas
	/// records nothing here, so all-zero counters in such a run mean "probe off"
	/// and never "joins had no locality". The atlas stamps the probe's state into
	/// its artifact header for the same reason.
	#[test]
	fn the_probe_is_off_without_its_environment_variable() {
		if std::env::var(JOIN_SELECTION_LOCALITY_ENV).is_ok() {
			// A diagnostic run of the suite legitimately has it set; the assertion
			// below would then be testing the opposite contract.
			return;
		}
		let telemetry = Some(Arc::new(Mutex::new(TaskTelemetry::default())));
		record_join_selection_locality(&telemetry, JoinKind::Inner, &paired(&[(0, 0), (1, 1)]));
		assert_eq!(
			telemetry.unwrap().lock().unwrap().join_selection_locality,
			JoinSelectionLocalitySides::default(),
			"the locality probe must not run on the production join path: its \
			 O(rows) pass sits outside the output bracket and would inflate the \
			 framework overhead it exists to explain"
		);
	}
}
