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
		let child = match side {
			JoinInputSide::Left => &mut task.phases.join_input_left_concat_micros,
			JoinInputSide::Right => &mut task.phases.join_input_right_concat_micros,
		};
		*child = child.saturating_add(elapsed);

		let counters = &mut task.counters;
		let (batches_slot, rows_slot, bytes_slot) = match side {
			JoinInputSide::Left => (
				&mut counters.join_input_left_batches,
				&mut counters.join_input_left_rows,
				&mut counters.join_input_left_bytes,
			),
			JoinInputSide::Right => (
				&mut counters.join_input_right_batches,
				&mut counters.join_input_right_rows,
				&mut counters.join_input_right_bytes,
			),
		};
		*batches_slot = batches_slot.saturating_add(shape.batches);
		*rows_slot = rows_slot.saturating_add(shape.rows);
		*bytes_slot = bytes_slot.saturating_add(shape.bytes);
	});
	outcome
}

#[cfg(test)]
mod input_side_split_tests {
	use super::*;
	use crate::distributed::task::TaskTelemetry;
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
		time_join_input_materialization(&task, JoinInputSide::Left, &[batch(4)], busy)
			.expect("left concat");
		let (parent, left, right) = {
			let guard = task.as_ref().unwrap().lock().unwrap();
			(
				guard.phases.join_input_materialization_micros,
				guard.phases.join_input_left_concat_micros,
				guard.phases.join_input_right_concat_micros,
			)
		};
		assert!(left > 0, "the left concat must charge the left child");
		assert_eq!(right, 0, "it must not charge the right child");
		assert_eq!(
			parent, left,
			"one clock read charges both, so a single-sided task's parent equals \
			 its child exactly"
		);

		time_join_input_materialization(&task, JoinInputSide::Right, &[batch(4)], busy)
			.expect("right concat");
		let guard = task.as_ref().unwrap().lock().unwrap();
		assert!(guard.phases.join_input_right_concat_micros > 0);
		assert_eq!(
			guard.phases.join_input_left_concat_micros, left,
			"the right concat must not move the left child"
		);
		assert_eq!(
			guard.phases.join_input_materialization_micros,
			guard.phases.join_input_left_concat_micros
				+ guard.phases.join_input_right_concat_micros,
			"the two children partition their parent when every site is charged"
		);
	}

	/// The shape counters record what the concat consumed, per side.
	#[test]
	fn the_shape_counters_record_batches_rows_and_bytes_per_side() {
		let task = slot();
		let inputs = [batch(4), batch(6), batch(10)];
		time_join_input_materialization(&task, JoinInputSide::Left, &inputs, busy)
			.expect("left concat");

		let guard = task.as_ref().unwrap().lock().unwrap();
		assert_eq!(guard.counters.join_input_left_batches, 3);
		assert_eq!(guard.counters.join_input_left_rows, 20);
		assert!(
			guard.counters.join_input_left_bytes >= 20 * 8,
			"20 Int64 rows are at least 160 bytes; got {}",
			guard.counters.join_input_left_bytes
		);
		assert_eq!(
			(
				guard.counters.join_input_right_batches,
				guard.counters.join_input_right_rows,
				guard.counters.join_input_right_bytes
			),
			(0, 0, 0),
			"a left concat records nothing on the right side"
		);
	}

	/// A single batch is still a concatenation that was asked for, and is counted.
	///
	/// The one-batch case is where a multi-batch design would save the most —
	/// `concat` of a single batch is a copy that buys nothing — so a counter that
	/// skipped it would hide the clearest evidence for the redesign.
	#[test]
	fn a_single_batch_input_is_still_recorded() {
		let task = slot();
		time_join_input_materialization(&task, JoinInputSide::Right, &[batch(8)], busy)
			.expect("right concat");

		let guard = task.as_ref().unwrap().lock().unwrap();
		assert_eq!(guard.counters.join_input_right_batches, 1);
		assert_eq!(guard.counters.join_input_right_rows, 8);
	}

	/// An empty input records a zero-batch concat, not nothing at all.
	#[test]
	fn an_empty_input_records_zero_batches_and_still_charges_time() {
		let task = slot();
		time_join_input_materialization(&task, JoinInputSide::Left, &[], busy)
			.expect("empty concat");

		let guard = task.as_ref().unwrap().lock().unwrap();
		assert_eq!(
			(
				guard.counters.join_input_left_batches,
				guard.counters.join_input_left_rows
			),
			(0, 0)
		);
		assert!(
			guard.phases.join_input_left_concat_micros > 0,
			"an empty concat still ran inside the task wall"
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
		assert!(guard.phases.join_input_right_concat_micros > 0);
	}
}
