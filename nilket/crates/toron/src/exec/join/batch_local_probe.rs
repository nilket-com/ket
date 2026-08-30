//! Probing a join's right side one source batch at a time, without concatenating it.
//!
//! ADR 580 measured 6,424.8 ms of board time inside the probe side's
//! `concat_batches` and priced this design end to end before authorizing it
//! (`benches/join_input_pipeline_shape.rs`: -35% to -38% of pipeline time and
//! -70% of peak allocation across the measured multi-batch probe shapes).
//!
//! **Deliberately narrow.** This route serves `JoinKind::Inner`, in
//! `execute_hash_join`, with a successful in-memory build, over more than one
//! right batch. Every other shape — spill fallback, `Cross`, the outer and
//! semi/anti kinds, single or empty inputs, and
//! `execute_hash_join_into_global_aggregate` — keeps the concatenating path
//! byte-identical. The aggregate consumer in particular was never benchmarked;
//! the bench matched its cardinality, not its fold.
//!
//! No new row addressing. `probe_in_memory_join_rows` already takes one probe
//! batch and produces batch-local `u32` indices, so each batch yields its own
//! selection over its own rows and materializes into its own output batch. The
//! whole-source slice rule in `output_materialization` applies per probe batch
//! exactly as it applied to the concatenated one.

use super::*;

/// Environment variable enabling the batch-local/concatenated equivalence check.
///
/// Opt-in for the same reason the slice equivalence check is: it runs BOTH
/// routes and compares them, so a run with it on measures neither. It also
/// withholds timing authority through the harness's `join_diagnostics_disabled`
/// helper — a new join diagnostic that does not join that helper would silently
/// claim authority it should not have.
pub const JOIN_BATCH_LOCAL_EQUIVALENCE_ENV: &str = "TORON_JOIN_BATCH_LOCAL_EQUIVALENCE";

/// Whether the batch-local equivalence check is enabled for this process.
pub fn join_batch_local_equivalence_enabled() -> bool {
	static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
	*ENABLED.get_or_init(|| std::env::var(JOIN_BATCH_LOCAL_EQUIVALENCE_ENV).is_ok())
}

/// Output batches the equivalence check has compared and found equal.
static EQUIVALENCE_BATCHES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// How many output columns the check has verified.
///
/// The positive control for the check: without it, "no mismatch" cannot be told
/// apart from "the route never fired".
pub fn join_batch_local_equivalence_columns_verified() -> u64 {
	EQUIVALENCE_BATCHES.load(std::sync::atomic::Ordering::Relaxed)
}

/// Whether this join may take the batch-local route at all.
///
/// Kind and batch count only — the build's success and the spill decision are
/// established inside the route, because they depend on work it has to do
/// anyway.
pub(super) fn batch_local_route_eligible(kind: JoinKind, right_batches: &[RecordBatch]) -> bool {
	join_batch_local_route_enabled() && batch_local_route_shape_eligible(kind, right_batches)
}

/// The shape half of eligibility, without the gate.
///
/// Separated so the scope can be tested without touching process environment: a
/// test that set the gate would race every other test in the same process, and
/// the two questions — "is this shape servable" and "is the measurement route on"
/// — are different ones.
fn batch_local_route_shape_eligible(kind: JoinKind, right_batches: &[RecordBatch]) -> bool {
	matches!(kind, JoinKind::Inner) && right_batches.len() > 1
}

/// Environment variable enabling the batch-local route AT ALL.
///
/// The route was measured and reverted for missing its retention rule. It exists
/// here only to denominate the projection that would authorize its successor: a
/// per-row cost needs the rows the route actually matched, and no artifact ever
/// persisted them. Off by default, so the engine's behaviour is unchanged unless
/// a measurement asks for it.
///
/// It joins the harness's `join_diagnostics_disabled` helper, so a run with it on
/// withholds timing authority. That is not a formality: this route is known to
/// cost more than it saves in this form, and a run that timed the board with it
/// enabled would publish a slower board as if it were a measurement of
/// something.
pub const JOIN_BATCH_LOCAL_ROUTE_ENV: &str = "TORON_JOIN_BATCH_LOCAL_MEASURE";

/// Whether the batch-local measurement route is enabled for this process.
pub fn join_batch_local_route_enabled() -> bool {
	static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
	*ENABLED.get_or_init(|| std::env::var(JOIN_BATCH_LOCAL_ROUTE_ENV).is_ok())
}

/// Record which route this join committed to.
///
/// A receipt, not an inference: a run reads the batch-local figures only on nodes
/// whose receipt says the route fired, and a fallback is a fact rather than the
/// absence of one.
pub(super) fn record_route(
	telemetry: &JoinTiming,
	batch_local: bool,
	output_batches: u64,
	concat_bytes_avoided: u64,
) {
	record_task_telemetry(telemetry, |task| {
		if batch_local {
			task.counters.join_batch_local_routes =
				task.counters.join_batch_local_routes.saturating_add(1);
			task.counters.join_batch_local_output_batches = task
				.counters
				.join_batch_local_output_batches
				.saturating_add(output_batches);
			task.counters.join_batch_local_concat_bytes_avoided = task
				.counters
				.join_batch_local_concat_bytes_avoided
				.saturating_add(concat_bytes_avoided);
		} else {
			task.counters.join_concatenated_routes =
				task.counters.join_concatenated_routes.saturating_add(1);
		}
	});
}

/// Slice bytes the skipped concatenation would have copied.
///
/// Reported as bytes NOT allocated, which is what the route actually changes.
/// It is not a measured peak: the source batches and the per-batch outputs are
/// still live, and this counter does not claim otherwise.
fn concat_bytes(batches: &[RecordBatch]) -> u64 {
	let mut bytes = 0u64;
	for batch in batches {
		for column in batch.columns() {
			bytes = bytes.saturating_add(
				column
					.to_data()
					.get_slice_memory_size()
					.map(|size| size as u64)
					.unwrap_or(0),
			);
		}
	}
	bytes
}

/// Try the batch-local route. `Ok(None)` declines and leaves the caller's
/// concatenating path untouched.
#[allow(clippy::too_many_arguments)]
pub(super) fn try_batch_local_inner_join(
	kind: JoinKind,
	left_batch: &RecordBatch,
	right_batches: &[RecordBatch],
	left_keys: &[crate::ir::ColumnOrdinal],
	right_keys: &[crate::ir::ColumnOrdinal],
	output_schema: &arrow::datatypes::SchemaRef,
	match_filter: Option<&BoundExprProgram>,
	null_safe_keys: bool,
	replay_id: JoinReplayId,
	strategy: JoinStrategy,
	task_budget: TaskBudget,
	telemetry: &JoinTiming,
	join_runtime_observation_mode: &JoinRuntimeObservationMode,
) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
	let left_key_arrays = key_arrays(left_batch, left_keys)?;

	// The spill decision is computed on exactly the quantity it is computed on
	// today — build key arrays and build rows — so this route cannot change which
	// queries spill. When it says spill, this route declines and the caller's
	// path runs unchanged.
	if estimated_build_hash_bytes(&left_key_arrays, left_batch.num_rows())
		> task_budget.memory.get()
	{
		return Ok(None);
	}

	// Key TYPES are a property of the schema, so the plan is derived from the
	// first right batch and holds for all of them.
	let first_right_key_arrays = key_arrays(&right_batches[0], right_keys)?;
	let join_key_plan =
		JoinKeyPlan::from_key_arrays(&left_key_arrays, &first_right_key_arrays, null_safe_keys);

	let build_started = Instant::now();

	// ADR0551-SITE: join/try_batch_local_inner_join/Build/1

	let mut build_scope = telemetry.open_operator_transition(PhaseOperatorKind::Build);

	let build_rows = match build_in_memory_join_rows(
		join_key_plan,
		&left_key_arrays,
		left_batch.num_rows(),
		task_budget.memory.get(),
		null_safe_keys,
	) {
		Ok(Some(build_rows)) => build_rows,

		Ok(None) => {
			// UNREACHABLE, and recorded rather than assumed. The admission gate
			// above checked the estimate for the WHOLE build, and the build's own
			// per-row check tests the same estimate for a row count that never
			// exceeds it, so a decline here contradicts the gate.
			//
			// It is recorded because the consequence is invisible otherwise: the
			// caller would run the concatenating path and build the table a second
			// time, and that duplicated work would land in framework overhead while
			// the receipt reported an ordinary concatenated route. The A/B requires
			// this counter to be zero.
			build_scope.discard_before_measurement();

			record_task_telemetry(telemetry, |task| {
				task.counters.join_batch_local_post_admission_declines = task
					.counters
					.join_batch_local_post_admission_declines
					.saturating_add(1);
			});

			return Ok(None);
		}

		Err(error) => {
			build_scope.discard_before_measurement();

			return Err(error);
		}
	};

	let build_micros = duration_micros(build_started);

	record_task_telemetry(telemetry, |task| {
		build_scope.finish_at_measurement();

		task.counters.build_rows = task
			.counters
			.build_rows
			.saturating_add(left_batch.num_rows() as u64);
	});

	record_task_telemetry(telemetry, |_task| {
		build_scope.commit_after_write();
	});

	record_join_build_profile(
		telemetry,
		replay_id,
		strategy,
		join_key_plan,
		&build_rows,
		build_micros,
	);

	let mut output = Vec::with_capacity(right_batches.len());
	let mut probe_rows = 0u64;
	let mut matched_rows = 0u64;

	for right_batch in right_batches {
		let right_key_arrays = key_arrays(right_batch, right_keys)?;

		// ADR0551-SITE: join/try_batch_local_inner_join/Probe/1

		let mut probe_scope = telemetry.open_operator_transition(PhaseOperatorKind::Probe);

		let selection = match probe_in_memory_join_rows(
			kind,
			join_key_plan,
			&build_rows,
			left_batch.num_rows(),
			&right_key_arrays,
			right_batch.num_rows(),
			match_filter,
			left_batch,
			right_batch,
			null_safe_keys,
		) {
			Ok(selection) => selection,

			Err(error) => {
				probe_scope.discard_before_measurement();

				return Err(error);
			}
		};

		record_task_telemetry(telemetry, |task| {
			probe_scope.finish_at_measurement();

			task.counters.add_matched_rows(selection.len() as u64, true);
		});

		probe_scope.commit_after_write();

		record_join_selection_locality(telemetry, kind, &selection);

		probe_rows = probe_rows.saturating_add(right_batch.num_rows() as u64);
		matched_rows = matched_rows.saturating_add(selection.len() as u64);

		let batch = time_join_output_materialization(telemetry, || {
			materialize_join_selection(
				kind,
				left_batch,
				right_batch,
				&selection,
				output_schema,
				task_budget.memory.get(),
				SideTakeTiming::Charged(telemetry),
			)
		})?;

		// A probe batch that matched nothing contributes no rows, and an empty
		// batch is noise every consumer would have to skip. The concatenating route
		// never emitted one, so emitting them here would be a shape this engine has
		// never returned rather than the multi-batch shape it already does.
		if batch.num_rows() > 0 {
			output.push(batch);
		}
	}

	// Every probe batch matched nothing. One schema-correct empty batch, because
	// an empty `Vec` and a `Vec` holding an empty batch are different answers to
	// "what is this join's schema" for anything that reads the first batch to find
	// out.
	if output.is_empty() {
		output.push(RecordBatch::new_empty(output_schema.clone()));
	}

	record_task_telemetry(telemetry, |task| {
		task.counters.probe_rows = task.counters.probe_rows.saturating_add(probe_rows);
	});

	// ONE observation for the join, not one per batch: the observation describes a
	// join, and emitting several would make a batch-local join look like several
	// joins to everything downstream that counts them.
	record_join_runtime_observation(
		telemetry,
		join_runtime_observation_mode,
		JoinRuntimeObservation {
			replay_id,

			strategy,

			build_rows: RowCount::new(left_batch.num_rows() as u64),

			probe_rows: RowCount::new(probe_rows),

			output_rows: RowCount::new(record_batch_row_count(&output)),
		},
	);

	record_route(
		telemetry,
		true,
		output.len() as u64,
		concat_bytes(right_batches),
	);

	if join_batch_local_equivalence_enabled() {
		verify_against_concatenated_route(
			kind,
			left_batch,
			right_batches,
			&left_key_arrays,
			right_keys,
			join_key_plan,
			&build_rows,
			output_schema,
			match_filter,
			null_safe_keys,
			task_budget,
			&output,
		)?;
	}

	Ok(Some(output))
}

/// Run the concatenating route as well and compare, under the opt-in diagnostic.
///
/// Fails the query on mismatch rather than logging: this runs only under an
/// explicit opt-in, where a difference means the route returns wrong rows and
/// every later number in the run is worthless. `ArrayData` equality covers the
/// data type, the length and the null bitmap as well as the values.
#[allow(clippy::too_many_arguments)]
fn verify_against_concatenated_route(
	kind: JoinKind,
	left_batch: &RecordBatch,
	right_batches: &[RecordBatch],
	left_key_arrays: &[ArrayRef],
	right_keys: &[crate::ir::ColumnOrdinal],
	join_key_plan: JoinKeyPlan,
	build_rows: &JoinBuildRows,
	output_schema: &arrow::datatypes::SchemaRef,
	match_filter: Option<&BoundExprProgram>,
	null_safe_keys: bool,
	task_budget: TaskBudget,
	produced: &[RecordBatch],
) -> Result<(), ExecutionError> {
	let right_schema = right_batches[0].schema();
	let right_batch = arrow::compute::concat_batches(&right_schema, right_batches)?;
	let right_key_arrays = key_arrays(&right_batch, right_keys)?;
	let selection = probe_in_memory_join_rows(
		kind,
		join_key_plan,
		build_rows,
		left_batch.num_rows(),
		&right_key_arrays,
		right_batch.num_rows(),
		match_filter,
		left_batch,
		&right_batch,
		null_safe_keys,
	)?;
	let _ = left_key_arrays;
	let reference = materialize_join_selection(
		kind,
		left_batch,
		&right_batch,
		&selection,
		output_schema,
		task_budget.memory.get(),
		SideTakeTiming::AccountedElsewhere,
	)?;

	// Row ORDER is part of the claim: the batch-local route walks batch 0's rows,
	// then batch 1's, which is the order the concatenated route walks them in.
	let produced = arrow::compute::concat_batches(output_schema, produced)?;
	if produced.num_rows() != reference.num_rows() {
		return Err(ExecutionError::Kernel {
			message: format!(
				"batch-local join produced {} rows against {} from the concatenated route",
				produced.num_rows(),
				reference.num_rows()
			),
		});
	}
	for column in 0..reference.num_columns() {
		if produced.column(column).to_data() != reference.column(column).to_data() {
			return Err(ExecutionError::Kernel {
				message: format!(
					"batch-local join differs from the concatenated route at column {column}"
				),
			});
		}
		EQUIVALENCE_BATCHES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
	}
	Ok(())
}

#[cfg(test)]
mod batch_local_route_tests {
	use super::*;

	/// The route serves Inner over more than one batch, and nothing else.
	///
	/// Every excluded shape here is one the bench never priced or the design
	/// cannot serve without join-wide state, so the gate is the scope statement:
	/// widening it would outrun the evidence rather than the code.
	#[test]
	fn only_inner_joins_over_several_batches_are_eligible() {
		let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
			arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
		]));
		let batch = RecordBatch::new_empty(schema);
		let one = [batch.clone()];
		let two = [batch.clone(), batch.clone()];

		assert!(batch_local_route_shape_eligible(JoinKind::Inner, &two));
		assert!(
			!batch_local_route_shape_eligible(JoinKind::Inner, &one),
			"a single batch has nothing to concatenate and nothing to gain"
		);
		assert!(!batch_local_route_shape_eligible(JoinKind::Inner, &[]));

		// The gate is the other half: an eligible shape is still not served unless
		// the measurement route is switched on.
		assert_eq!(
			batch_local_route_eligible(JoinKind::Inner, &two),
			join_batch_local_route_enabled(),
			"an eligible shape is served only when the route is enabled"
		);

		for kind in [
			JoinKind::LeftOuter,
			JoinKind::RightOuter,
			JoinKind::FullOuter,
			JoinKind::LeftSemi,
			JoinKind::LeftAnti,
			JoinKind::RightSemi,
			JoinKind::RightAnti,
			JoinKind::Cross,
		] {
			assert!(
				!batch_local_route_shape_eligible(kind, &two),
				"{kind:?} needs join-wide state or a different path, and neither is \
				 authorized by the bench that priced Inner"
			);
		}
	}

	/// A probe batch that matches nothing contributes no output batch, and the
	/// rows that DO match still arrive.
	///
	/// The regression the seal caught: with one output batch per probe batch, a
	/// first batch matching nothing left an empty batch at the head of the result,
	/// and a consumer reading only the first batch saw no rows at all. Empty
	/// batches are suppressed, so the head of the result is real output.
	#[test]
	fn a_probe_batch_that_matches_nothing_contributes_no_batch() {
		use arrow::array::Int64Array;
		use arrow::datatypes::{DataType, Field, Schema};
		use std::sync::Arc as StdArc;

		fn keyed(name: &str, keys: &[i64]) -> RecordBatch {
			let schema = StdArc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]));
			RecordBatch::try_new(schema, vec![StdArc::new(Int64Array::from(keys.to_vec()))])
				.expect("fixture batch")
		}

		let left = keyed("l_key", &[1, 2, 3]);
		// The first probe batch matches nothing; the second matches twice.
		let right = [keyed("r_key", &[70, 80]), keyed("r_key", &[2, 3])];
		let mut fields = left.schema().fields().to_vec();
		fields.extend(right[0].schema().fields().iter().cloned());
		let output_schema: arrow::datatypes::SchemaRef =
			StdArc::new(Schema::new(fields.into_iter().collect::<Vec<_>>()));

		let output = try_batch_local_inner_join(
			JoinKind::Inner,
			&left,
			&right,
			&[crate::ir::ColumnOrdinal(0)],
			&[crate::ir::ColumnOrdinal(0)],
			&output_schema,
			None,
			false,
			JoinReplayId(0),
			JoinStrategy::HashPartitioned,
			TaskBudget {
				memory: ByteCount::new(64 * 1024 * 1024),
				..TaskBudget::from_query_budget(
					crate::distributed::budget::QueryBudget::test_default(),
				)
			},
			&JoinTiming::disabled(None),
			&JoinRuntimeObservationMode::Disabled,
		)
		.expect("batch-local join")
		.expect("the route serves Inner over two batches");

		assert_eq!(
			output.len(),
			1,
			"only the matching probe batch contributes a batch"
		);
		assert!(
			output[0].num_rows() > 0,
			"the first returned batch must carry rows: a consumer reading it alone \
			 saw nothing before empty batches were suppressed"
		);
		let keys = output[0]
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("left key column");
		assert_eq!(
			keys.values(),
			&[2, 3],
			"both matches from the second probe batch survive"
		);
	}

	/// When no probe batch matches, the result is one schema-correct empty batch.
	///
	/// An empty `Vec` and a `Vec` holding an empty batch are different answers to
	/// "what is this join's schema" for anything that reads the first batch to
	/// find out.
	#[test]
	fn a_join_that_matches_nothing_returns_one_empty_batch() {
		use arrow::array::Int64Array;
		use arrow::datatypes::{DataType, Field, Schema};
		use std::sync::Arc as StdArc;

		fn keyed(name: &str, keys: &[i64]) -> RecordBatch {
			let schema = StdArc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]));
			RecordBatch::try_new(schema, vec![StdArc::new(Int64Array::from(keys.to_vec()))])
				.expect("fixture batch")
		}

		let left = keyed("l_key", &[1, 2, 3]);
		let right = [keyed("r_key", &[70, 80]), keyed("r_key", &[90])];
		let mut fields = left.schema().fields().to_vec();
		fields.extend(right[0].schema().fields().iter().cloned());
		let output_schema: arrow::datatypes::SchemaRef =
			StdArc::new(Schema::new(fields.into_iter().collect::<Vec<_>>()));

		let output = try_batch_local_inner_join(
			JoinKind::Inner,
			&left,
			&right,
			&[crate::ir::ColumnOrdinal(0)],
			&[crate::ir::ColumnOrdinal(0)],
			&output_schema,
			None,
			false,
			JoinReplayId(0),
			JoinStrategy::HashPartitioned,
			TaskBudget {
				memory: ByteCount::new(64 * 1024 * 1024),
				..TaskBudget::from_query_budget(
					crate::distributed::budget::QueryBudget::test_default(),
				)
			},
			&JoinTiming::disabled(None),
			&JoinRuntimeObservationMode::Disabled,
		)
		.expect("batch-local join")
		.expect("the route serves Inner over two batches");

		assert_eq!(output.len(), 1);
		assert_eq!(output[0].num_rows(), 0);
		assert_eq!(
			output[0].schema(),
			output_schema,
			"an empty result still carries the join's schema"
		);
	}

	/// Admission implies the build completes — across every key plan.
	///
	/// The route gates on `estimated_build_hash_bytes` for the WHOLE build; the
	/// build re-tests the same estimate per row for a count that never exceeds it.
	/// So a post-admission decline is unreachable, and
	/// `join_batch_local_post_admission_declines` must stay zero on any board. This
	/// pins the implication rather than the counter: a cap set to exactly the
	/// admission estimate must still build, on every plan the route can meet.
	#[test]
	fn a_cap_that_admits_the_build_always_completes_it() {
		use arrow::array::{Int32Array, Int64Array, StringArray};

		let rows = 512usize;
		let int32: ArrayRef = std::sync::Arc::new(Int32Array::from_iter_values(
			(0..rows).map(|row| (row % 97) as i32),
		));
		let int64: ArrayRef = std::sync::Arc::new(Int64Array::from_iter_values(
			(0..rows).map(|row| (row % 89) as i64),
		));
		let text: ArrayRef = std::sync::Arc::new(StringArray::from_iter_values(
			(0..rows).map(|row| format!("k{:04}", row % 71)),
		));

		for (label, arrays, null_safe) in [
			("int32 primitive", vec![int32.clone()], false),
			("int64 primitive", vec![int64.clone()], false),
			("generic text", vec![text.clone()], false),
			(
				"composite generic",
				vec![int64.clone(), text.clone()],
				false,
			),
			("null-safe primitive", vec![int64.clone()], true),
			("null-safe generic", vec![text.clone()], true),
		] {
			let plan = JoinKeyPlan::from_key_arrays(&arrays, &arrays, null_safe);
			let admission = estimated_build_hash_bytes(&arrays, rows);
			let built = build_in_memory_join_rows(plan, &arrays, rows, admission, null_safe)
				.unwrap_or_else(|error| panic!("{label}: build failed: {error:?}"));

			assert!(
				built.is_some(),
				"{label}: a cap equal to the admission estimate must complete the \
				 build. A decline would send the caller back to the concatenating \
				 path to build a SECOND time, and that duplicated work would land \
				 in framework overhead under an ordinary-fallback receipt"
			);
		}
	}

	/// The equivalence check and the route's own measurement are exclusive.
	#[test]
	fn the_equivalence_check_is_opt_in() {
		assert_eq!(
			join_batch_local_equivalence_enabled(),
			std::env::var(JOIN_BATCH_LOCAL_EQUIVALENCE_ENV).is_ok(),
			"the check runs both routes, so a run with it on measures neither"
		);
	}
}
