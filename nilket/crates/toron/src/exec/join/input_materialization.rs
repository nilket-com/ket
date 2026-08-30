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
}
