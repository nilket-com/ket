//! Timing and shape of a join's INPUT concatenation.
//!
//! `join_input_materialization_micros` names 7,146 ms of the distributed board —
//! the larger half of the named join materialization cost, and the half nothing
//! has yet taken apart. It is one scalar over two very different inputs: the
//! build side, whose rows are held for the whole probe, and the probe side,
//! whose rows stream past once.
//!
//! This module splits that scalar by side and records the shape of what each
//! side concatenated, because a redesign of the input path — building and
//! probing directly over multiple batches instead of concatenating — is a large
//! structural change and its two halves can be worth wildly different amounts.
//! Measuring first is what decides whether one, both or neither is worth it.

use super::*;

/// Enables ADR 584's replay-keyed input-materialization diagnostic.
pub const JOIN_REPLAY_INPUT_DIAGNOSTIC_ENV: &str = "TORON_JOIN_REPLAY_INPUT_DIAGNOSTIC";

/// Process-local because each performance sample is a fresh child process.
/// Reading the environment once keeps the disabled path out of the hot concat
/// loop while still letting the wrapper select the arm per child.
pub fn join_replay_input_diagnostic_enabled() -> bool {
	static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
	*ENABLED.get_or_init(|| std::env::var_os(JOIN_REPLAY_INPUT_DIAGNOSTIC_ENV).is_some())
}

fn replay_ledger_slot() -> &'static Mutex<Vec<crate::distributed::task::JoinInputReplayLedger>> {
	static LEDGER: std::sync::OnceLock<
		Mutex<Vec<crate::distributed::task::JoinInputReplayLedger>>,
	> = std::sync::OnceLock::new();
	LEDGER.get_or_init(|| Mutex::new(Vec::new()))
}

fn replay_total_concat_slot() -> &'static AtomicU64 {
	static TOTAL_CONCATS: AtomicU64 = AtomicU64::new(0);
	&TOTAL_CONCATS
}

fn replay_concat_scope_depth() -> &'static AtomicUsize {
	static DEPTH: AtomicUsize = AtomicUsize::new(0);
	&DEPTH
}

pub(super) struct JoinReplayInputDiagnosticScope(bool);

impl Drop for JoinReplayInputDiagnosticScope {
	fn drop(&mut self) {
		if self.0 {
			replay_concat_scope_depth().fetch_sub(1, Ordering::Relaxed);
		}
	}
}

/// Bound the independent concat total to the specialized replay handler.
/// Sort/output concatenations after that handler returns are deliberately not
/// join-input work and therefore do not belong in its completeness equation.
pub(super) fn begin_join_replay_input_diagnostic_scope() -> JoinReplayInputDiagnosticScope {
	let enabled = join_replay_input_diagnostic_enabled();
	if enabled {
		replay_concat_scope_depth().fetch_add(1, Ordering::Relaxed);
	}
	JoinReplayInputDiagnosticScope(enabled)
}

/// Count every call through the executor's shared join-concat primitive.
///
/// The replay ledger is useful evidence only when its recorded concat count
/// closes against this independently placed total. Keeping the counter at the
/// shared primitive means a new unwrapped call site fails the harness gate
/// instead of silently looking like zero work for its replay.
pub fn observe_join_input_concat_for_diagnostic() {
	if join_replay_input_diagnostic_enabled()
		&& replay_concat_scope_depth().load(Ordering::Relaxed) > 0
	{
		let _ = replay_total_concat_slot().fetch_update(
			Ordering::Relaxed,
			Ordering::Relaxed,
			|value| value.checked_add(1),
		);
	}
}

/// Clear ADR 584's process-local receipt before one in-process performance run.
///
/// The ledger deliberately does not extend `TaskTelemetry`: doing so would change
/// the distributed task wire format for an instrument used only by the fresh-child
/// TPC-H harness. Each performance child executes one query, so process scope is
/// the exact measurement boundary.
pub fn reset_join_replay_input_diagnostic() -> std::io::Result<()> {
	replay_ledger_slot()
		.lock()
		.map_err(|_| std::io::Error::other("join replay input diagnostic lock is poisoned"))?
		.clear();
	replay_total_concat_slot().store(0, Ordering::Relaxed);
	replay_concat_scope_depth().store(0, Ordering::Relaxed);
	Ok(())
}

/// Take the replay-keyed diagnostic and its independent completeness total
/// after the in-process query completes.
pub fn take_join_replay_input_diagnostic()
-> std::io::Result<(Vec<crate::distributed::task::JoinInputReplayLedger>, u64)> {
	let mut ledger = replay_ledger_slot()
		.lock()
		.map_err(|_| std::io::Error::other("join replay input diagnostic lock is poisoned"))?;
	let total_concats = replay_total_concat_slot().swap(0, Ordering::Relaxed);
	Ok((std::mem::take(&mut *ledger), total_concats))
}

/// Which of a join's two inputs a concatenation is building.
///
/// Named by SIDE, not by role. `left` is the build input and `right` the probe
/// input at every site this module serves — the same convention `build_rows` and
/// `probe_rows` already use — but a broadcast right-build swaps the sides
/// upstream, so a counter that claimed the role would be wrong exactly where the
/// plan is most interesting.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum JoinInputSide {
	Left,
	Right,
}

/// What one side handed to the concatenation.
///
/// Batches, rows and bytes together, because they answer different questions and
/// only the three together say what a multi-batch redesign would avoid: batch
/// count bounds what re-chunking could remove, rows scale the per-row cost, and
/// bytes are what the copy actually moves.
#[derive(Clone, Copy, Default)]
struct InputShape {
	batches: u64,
	rows: u64,
	bytes: u64,
}

impl InputShape {
	fn of(batches: &[RecordBatch]) -> Self {
		let mut shape = Self {
			batches: batches.len() as u64,
			..Self::default()
		};
		for batch in batches {
			shape.rows = shape.rows.saturating_add(batch.num_rows() as u64);
			for column in batch.columns() {
				// `get_slice_memory_size`, never `get_array_memory_size`: these
				// inputs arrive from shuffle and scan and are routinely slices of a
				// larger allocation. The allocation is not what the concat copies,
				// and charging it would overstate the very quantity a multi-batch
				// design would avoid copying.
				let bytes = column
					.to_data()
					.get_slice_memory_size()
					.map(|bytes| bytes as u64)
					.unwrap_or(0);
				shape.bytes = shape.bytes.saturating_add(bytes);
			}
		}
		shape
	}
}

/// Time one side's input concatenation.
///
/// Charges the parent `join_input_materialization_micros` and this side's nested
/// child from ONE clock read, so the child cannot drift from the parent it sits
/// inside. The children are absent from `recompute_framework_overhead` for the
/// same reason the output side children are: the parent already left the
/// residual.
///
/// Charged even when the body fails: the time was spent inside the task wall
/// either way, and dropping it on the error path would silently return it to the
/// residual these counters exist to reduce.
pub(super) fn time_join_input_materialization<T>(
	telemetry: &Option<Arc<Mutex<TaskTelemetry>>>,
	side: JoinInputSide,
	batches: &[RecordBatch],
	body: impl FnOnce() -> Result<T, ExecutionError>,
) -> Result<T, ExecutionError> {
	time_join_input_materialization_with_diagnostic(telemetry, side, batches, body)
}

/// Time a join-input concatenation performed by a specialized replay path.
///
/// Those paths bypass `execute_hash_join`, so ADR 580's task-wide brackets do
/// not own their time. ADR 584 still needs the same shape at the affected chain boundary,
/// keyed by the replay that performed it. When the diagnostic is disabled this
/// is exactly the body call: no clock read, shape walk, or telemetry growth.
pub(super) fn time_join_replay_input_materialization<T>(
	_telemetry: &Option<Arc<Mutex<TaskTelemetry>>>,
	replay_id: JoinReplayId,
	side: JoinInputSide,
	batches: &[RecordBatch],
	body: impl FnOnce() -> Result<T, ExecutionError>,
) -> Result<T, ExecutionError> {
	time_join_replay_input_materialization_with_diagnostic(
		replay_id,
		side,
		batches,
		join_replay_input_diagnostic_enabled(),
		replay_ledger_slot(),
		body,
	)
}

fn time_join_replay_input_materialization_with_diagnostic<T>(
	replay_id: JoinReplayId,
	side: JoinInputSide,
	batches: &[RecordBatch],
	replay_diagnostic: bool,
	replay_ledger: &Mutex<Vec<crate::distributed::task::JoinInputReplayLedger>>,
	body: impl FnOnce() -> Result<T, ExecutionError>,
) -> Result<T, ExecutionError> {
	if !replay_diagnostic {
		return body();
	}
	let shape = InputShape::of(batches);
	let started = Instant::now();
	let outcome = body();
	let elapsed = duration_micros(started);
	if let Ok(mut ledger) = replay_ledger.lock() {
		record_replay_input_materialization(&mut ledger, replay_id, side, shape, elapsed);
	}
	outcome
}

fn record_replay_input_materialization(
	ledger: &mut Vec<crate::distributed::task::JoinInputReplayLedger>,
	replay_id: JoinReplayId,
	side: JoinInputSide,
	shape: InputShape,
	elapsed: u64,
) {
	let replay = match ledger.iter_mut().find(|entry| entry.replay_id == replay_id) {
		Some(entry) => entry,
		None => {
			ledger.push(crate::distributed::task::JoinInputReplayLedger::for_replay(
				replay_id,
			));
			ledger
				.last_mut()
				.expect("the replay ledger entry was just appended")
		}
	};
	let side = match side {
		JoinInputSide::Left => &mut replay.left,
		JoinInputSide::Right => &mut replay.right,
	};
	side.observe(shape.batches, shape.rows, shape.bytes, elapsed);
}

fn time_join_input_materialization_with_diagnostic<T>(
	telemetry: &Option<Arc<Mutex<TaskTelemetry>>>,
	side: JoinInputSide,
	batches: &[RecordBatch],
	body: impl FnOnce() -> Result<T, ExecutionError>,
) -> Result<T, ExecutionError> {
	// Measured before the body: `batches` is what the concat is about to consume,
	// and after it runs the inputs may be gone.
	let shape = InputShape::of(batches);
	let started = Instant::now();
	let outcome = body();
	let elapsed = duration_micros(started);
	record_task_telemetry(telemetry, |task| {
		task.phases.join_input_materialization_micros = task
			.phases
			.join_input_materialization_micros
			.saturating_add(elapsed);
		// Charged by side AND by shape, because ADR 580's gate asks both. A concat
		// of one batch is arrow's zero-copy wrapper, so its time is not copy cost
		// and no multi-batch design removes it; folding it in with the rest would
		// let wrapper overhead argue for a rewrite.
		let child = match (side, shape.batches > 1) {
			(JoinInputSide::Left, false) => &mut task.phases.join_input_left_single_batch_micros,
			(JoinInputSide::Left, true) => &mut task.phases.join_input_left_multi_batch_micros,
			(JoinInputSide::Right, false) => &mut task.phases.join_input_right_single_batch_micros,
			(JoinInputSide::Right, true) => &mut task.phases.join_input_right_multi_batch_micros,
		};
		*child = child.saturating_add(elapsed);

		let side_shape = match side {
			JoinInputSide::Left => &mut task.join_input_shape.left,
			JoinInputSide::Right => &mut task.join_input_shape.right,
		};
		side_shape.observe(shape.batches, shape.rows, shape.bytes);
	});
	outcome
}

#[cfg(test)]
mod input_side_split_tests {
	use super::*;
	use crate::distributed::task::{INPUT_BATCH_COUNT_BUCKET_MINIMUMS, TaskTelemetry};
	use arrow::array::Int64Array;
	use arrow::datatypes::{DataType, Field, Schema};
	use std::sync::Arc as StdArc;

	fn batch(rows: i64) -> RecordBatch {
		let schema = StdArc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		RecordBatch::try_new(
			schema,
			vec![StdArc::new(Int64Array::from_iter_values(0..rows))],
		)
		.expect("fixture batch")
	}

	fn slot() -> Option<Arc<Mutex<TaskTelemetry>>> {
		Some(Arc::new(Mutex::new(TaskTelemetry::default())))
	}

	fn busy() -> Result<u64, ExecutionError> {
		let mut total = 0u64;
		for value in 0..2_000_000u64 {
			total = total.wrapping_add(value ^ (total >> 3));
		}
		Ok(total)
	}
	/// Each side charges its own child, and both charge the parent.
	///
	/// The zero on the other side is the discriminating half: a shared timer would
	/// pass a test that only checked the parent, and the parent is exactly the
	/// scalar this split exists to take apart.
	#[test]
	fn each_side_charges_its_own_child_and_the_shared_parent() {
		let task = slot();
		time_join_input_materialization(&task, JoinInputSide::Left, &[batch(4), batch(4)], busy)
			.expect("left concat");
		let (parent, left) = {
			let guard = task.as_ref().unwrap().lock().unwrap();
			(
				guard.phases.join_input_materialization_micros,
				guard.phases.join_input_left_multi_batch_micros,
			)
		};
		assert!(left > 0, "the left concat must charge the left child");
		assert_eq!(
			parent, left,
			"one clock read charges both, so a single-sided task's parent equals \
			 its child exactly"
		);

		time_join_input_materialization(&task, JoinInputSide::Right, &[batch(4), batch(4)], busy)
			.expect("right concat");
		let guard = task.as_ref().unwrap().lock().unwrap();
		assert!(guard.phases.join_input_right_multi_batch_micros > 0);
		assert_eq!(
			guard.phases.join_input_left_multi_batch_micros, left,
			"the right concat must not move the left child"
		);
		assert_eq!(
			guard.phases.join_input_materialization_micros,
			guard.phases.join_input_left_multi_batch_micros
				+ guard.phases.join_input_right_multi_batch_micros,
			"the children partition their parent when every site is charged"
		);
	}

	/// Shape decides the child, not just the side.
	///
	/// This is the correction ADR 580 needed before it could run: a single-batch
	/// concatenation is arrow's zero-copy wrapper, so its time is not copy cost
	/// and no multi-batch design removes it. A split that charged both shapes to
	/// one counter would let wrapper overhead argue for an addressing change.
	#[test]
	fn single_and_multi_batch_concats_charge_different_children() {
		let task = slot();
		time_join_input_materialization(&task, JoinInputSide::Left, &[batch(8)], busy)
			.expect("single-batch concat");
		let single = {
			let guard = task.as_ref().unwrap().lock().unwrap();
			assert!(
				guard.phases.join_input_left_single_batch_micros > 0,
				"a one-batch concat charges the single-batch child"
			);
			assert_eq!(
				guard.phases.join_input_left_multi_batch_micros, 0,
				"and never the multi-batch one"
			);
			guard.phases.join_input_left_single_batch_micros
		};

		time_join_input_materialization(&task, JoinInputSide::Left, &[batch(4), batch(4)], busy)
			.expect("multi-batch concat");
		let guard = task.as_ref().unwrap().lock().unwrap();
		assert!(guard.phases.join_input_left_multi_batch_micros > 0);
		assert_eq!(
			guard.phases.join_input_left_single_batch_micros, single,
			"a multi-batch concat must not move the single-batch child"
		);
	}

	/// The shape record is per invocation, not only per task.
	///
	/// A task total of four batches could be one four-batch concat or four
	/// single-batch ones, and those point at opposite projects. `concats` and the
	/// bucket histogram are what separate them.
	#[test]
	fn the_shape_record_distinguishes_one_wide_concat_from_many_narrow_ones() {
		let wide = slot();
		time_join_input_materialization(
			&wide,
			JoinInputSide::Left,
			&[batch(1), batch(1), batch(1), batch(1)],
			busy,
		)
		.expect("wide concat");

		let narrow = slot();
		for _ in 0..4 {
			time_join_input_materialization(&narrow, JoinInputSide::Left, &[batch(1)], busy)
				.expect("narrow concat");
		}

		let wide_shape = wide.as_ref().unwrap().lock().unwrap().join_input_shape.left;
		let narrow_shape = narrow
			.as_ref()
			.unwrap()
			.lock()
			.unwrap()
			.join_input_shape
			.left;

		assert_eq!(
			(wide_shape.batches, narrow_shape.batches),
			(4, 4),
			"the totals are identical, which is the whole problem"
		);
		assert_eq!((wide_shape.concats, narrow_shape.concats), (1, 4));
		assert_eq!(
			(
				wide_shape.concats_at_least[2],
				narrow_shape.concats_at_least[2]
			),
			(1, 0),
			"only the wide task has a concat of >= 4 batches; buckets are \
			 {INPUT_BATCH_COUNT_BUCKET_MINIMUMS:?}"
		);
	}

	/// The reconciliation guard: every concat lands in the buckets or is empty.
	#[test]
	fn bucket_counts_reconcile_with_the_invocation_count() {
		let task = slot();
		for batches in [0usize, 1, 2, 5, 9] {
			let inputs: Vec<RecordBatch> = (0..batches).map(|_| batch(2)).collect();
			time_join_input_materialization(&task, JoinInputSide::Right, &inputs, busy)
				.expect("concat");
		}

		let shape = task
			.as_ref()
			.unwrap()
			.lock()
			.unwrap()
			.join_input_shape
			.right;
		assert_eq!(shape.concats, 5);
		assert_eq!(shape.empty_concats, 1);
		assert_eq!(
			shape.concats_at_least[0] + shape.empty_concats,
			shape.concats,
			"every concat either reached one batch or was empty; a shortfall means \
			 an invocation was counted in neither"
		);
		assert_eq!(shape.batches, 17);
		assert_eq!(
			(
				shape.concats_at_least[1],
				shape.concats_at_least[2],
				shape.concats_at_least[3]
			),
			(3, 2, 1),
			"three concats of >= 2 batches, two of >= 4, one of >= 8"
		);
	}

	/// An empty input records a concat that happened, with no batches in it.
	#[test]
	fn an_empty_input_records_zero_batches_and_still_charges_time() {
		let task = slot();
		time_join_input_materialization(&task, JoinInputSide::Left, &[], busy)
			.expect("empty concat");

		let guard = task.as_ref().unwrap().lock().unwrap();
		let shape = guard.join_input_shape.left;
		assert_eq!(
			(shape.concats, shape.empty_concats, shape.batches),
			(1, 1, 0)
		);
		assert!(
			guard.phases.join_input_left_single_batch_micros > 0,
			"an empty concat still ran inside the task wall, and it is not \
			 multi-batch work"
		);
	}

	/// Bytes are recorded per side, from slice sizes.
	#[test]
	fn the_shape_record_carries_rows_and_slice_bytes() {
		let task = slot();
		time_join_input_materialization(
			&task,
			JoinInputSide::Left,
			&[batch(4), batch(6), batch(10)],
			busy,
		)
		.expect("left concat");

		let guard = task.as_ref().unwrap().lock().unwrap();
		let left = guard.join_input_shape.left;
		assert_eq!((left.batches, left.rows), (3, 20));
		assert!(
			left.bytes >= 20 * 8,
			"20 Int64 rows are at least 160 bytes; got {}",
			left.bytes
		);
		assert_eq!(
			guard.join_input_shape.right,
			Default::default(),
			"a left concat records nothing on the right side"
		);
	}

	/// Charged on the error path, like every other bracket in this family.
	#[test]
	fn a_failed_concat_still_charges_both_levels() {
		let task = slot();
		let failing = || -> Result<u64, ExecutionError> {
			busy()?;
			Err(ExecutionError::InvalidExpression {
				message: "concat failed".to_string(),
			})
		};
		time_join_input_materialization(&task, JoinInputSide::Right, &[batch(4)], failing)
			.expect_err("body fails");

		let guard = task.as_ref().unwrap().lock().unwrap();
		assert!(guard.phases.join_input_materialization_micros > 0);
		assert!(guard.phases.join_input_right_single_batch_micros > 0);
	}

	/// ADR 584: replay identity, side, timing shape and the batch histogram are
	/// one receipt. Two invocations for replay 3 must fold together while replay
	/// 4 remains distinct; an invocation-sized vector would fail the length check.
	#[test]
	fn replay_ledger_aggregates_by_replay_and_side() {
		let replay_ledger = Mutex::new(Vec::new());
		for batches in [vec![batch(2)], vec![batch(2), batch(3), batch(5), batch(7)]] {
			time_join_replay_input_materialization_with_diagnostic(
				JoinReplayId(3),
				JoinInputSide::Right,
				&batches,
				true,
				&replay_ledger,
				busy,
			)
			.expect("replay 3 concat");
		}
		time_join_replay_input_materialization_with_diagnostic(
			JoinReplayId(4),
			JoinInputSide::Left,
			&[batch(11), batch(13)],
			true,
			&replay_ledger,
			busy,
		)
		.expect("replay 4 concat");

		let replay_ledger = replay_ledger.lock().unwrap();
		assert_eq!(replay_ledger.len(), 2);
		let replay3 = replay_ledger
			.iter()
			.find(|entry| entry.replay_id == JoinReplayId(3))
			.expect("replay 3");
		assert_eq!(replay3.left, Default::default());
		assert!(replay3.right.single_batch_micros > 0);
		assert!(replay3.right.multi_batch_micros > 0);
		assert_eq!(replay3.right.shape.concats, 2);
		assert_eq!(replay3.right.shape.batches, 5);
		assert_eq!(replay3.right.shape.rows, 19);
		assert_eq!(replay3.right.shape.concats_at_least[0], 2);
		assert_eq!(replay3.right.shape.concats_at_least[1], 1);
		assert_eq!(replay3.right.shape.concats_at_least[2], 1);
		let replay4 = replay_ledger
			.iter()
			.find(|entry| entry.replay_id == JoinReplayId(4))
			.expect("replay 4");
		assert_eq!(replay4.left.shape.batches, 2);
		assert_eq!(replay4.right, Default::default());
	}

	/// Specialized chain replays do not own ADR 580's task-wide parent. Their
	/// opt-in receipt must therefore move only the replay ledger; charging the
	/// parent here would make the historical and same-boundary instruments count
	/// different universes under the same name.
	#[test]
	fn specialized_replay_timing_is_diagnostic_only() {
		let task = slot();
		let replay_ledger = Mutex::new(Vec::new());
		time_join_replay_input_materialization_with_diagnostic(
			JoinReplayId(2),
			JoinInputSide::Right,
			&[batch(2), batch(3)],
			true,
			&replay_ledger,
			busy,
		)
		.expect("specialized concat");
		let guard = task.as_ref().unwrap().lock().unwrap();
		assert_eq!(guard.phases.join_input_materialization_micros, 0);
		assert_eq!(guard.join_input_shape, Default::default());
		drop(guard);
		let replay_ledger = replay_ledger.lock().unwrap();
		assert_eq!(replay_ledger.len(), 1);
		assert_eq!(replay_ledger[0].replay_id, JoinReplayId(2));
		assert_eq!(replay_ledger[0].right.shape.concats, 1);
		assert_eq!(
			replay_ledger[0].right.shape.concats_at_least[1], 1,
			"two batches must land in the >=2 histogram bucket"
		);
	}

	/// Off means exactly the original body: no replay record and no task-wide
	/// counters. A helper that always records would make both pair arms
	/// instrumented and invalidate ADR 584's perturbation gate.
	#[test]
	fn specialized_replay_timing_is_absent_when_disabled() {
		let task = slot();
		let replay_ledger = Mutex::new(Vec::new());
		time_join_replay_input_materialization_with_diagnostic(
			JoinReplayId(2),
			JoinInputSide::Right,
			&[batch(2), batch(3)],
			false,
			&replay_ledger,
			busy,
		)
		.expect("unprofiled specialized concat");
		assert_eq!(
			*task.as_ref().unwrap().lock().unwrap(),
			TaskTelemetry::default()
		);
		assert!(replay_ledger.lock().unwrap().is_empty());
	}
}
