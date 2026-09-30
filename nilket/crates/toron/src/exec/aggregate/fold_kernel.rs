//! ADR 610: the bound grouped fold's two-phase shape. Phase one assigns every row of a batch its
//! group id through the unchanged lookup paths; phase two updates each aggregate with one loop
//! specialized, once per batch, to its state kind and input type. A batch runs two-phase only
//! when every aggregate has a typed kernel; otherwise it runs the row-at-a-time loop whole.

use super::{AggState, AggregateFunction, BoundAggregateCall, NumericArrayReader};
use crate::error::ExecutionError;
use arrow::array::{Array, ArrayRef, ArrowPrimitiveType, PrimitiveArray};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

// A `const` token builds with one variant constructed; the other is the measured alternative.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::exec) enum GroupedFoldShape {
	RowAtATime,
	TwoPhase,
}

pub(in crate::exec) const GROUPED_FOLD_SHAPE: GroupedFoldShape = GroupedFoldShape::TwoPhase;

/// ADR 611: which typed call lists fold two-phase. `AllTyped` is ADR 610's rule; `AtLeastTwoTyped`
/// also requires two or more aggregates, since a one-aggregate fold has little dispatch to save.
// A `const` token builds with one variant constructed; the other is the measured alternative.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::exec) enum TwoPhaseEligibility {
	AllTyped,
	AtLeastTwoTyped,
}

pub(in crate::exec) const TWO_PHASE_ELIGIBILITY: TwoPhaseEligibility =
	TwoPhaseEligibility::AtLeastTwoTyped;

impl TwoPhaseEligibility {
	fn name(self) -> &'static str {
		match self {
			Self::AllTyped => "all_typed",
			Self::AtLeastTwoTyped => "at_least_two_typed",
		}
	}
}

/// ADR 611: why a batch took the shape it did.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::exec) enum EligibilityReason {
	/// Typed kernels for every call and the rule satisfied: two-phase.
	Eligible,
	/// Some call has no typed kernel: row-at-a-time under either rule.
	UntypedCallList,
	/// Typed for every call but fewer than two calls, under `AtLeastTwoTyped`: row-at-a-time.
	BelowAggregateMinimum,
}

impl EligibilityReason {
	/// The reason for a call list of `calls` aggregates, `typed` when every call has a kernel.
	pub(in crate::exec) fn of(rule: TwoPhaseEligibility, typed: bool, calls: usize) -> Self {
		if !typed {
			Self::UntypedCallList
		} else if rule == TwoPhaseEligibility::AtLeastTwoTyped && calls < 2 {
			Self::BelowAggregateMinimum
		} else {
			Self::Eligible
		}
	}

	fn name(self) -> &'static str {
		match self {
			Self::Eligible => "eligible",
			Self::UntypedCallList => "untyped_call_list",
			Self::BelowAggregateMinimum => "below_aggregate_minimum",
		}
	}
}

/// ADR 610: diagnostic, one JSON line per aggregate instance at finalize; absent, nothing counts.
pub(in crate::exec) const GROUPED_FOLD_KERNEL_RECEIPT_ENV: &str =
	"TORON_GROUPED_FOLD_KERNEL_RECEIPT_PATH";

/// One aggregate's typed update for a whole batch.
pub(in crate::exec) enum TypedKernel<'r, 'a> {
	CountStar,
	CountNonNull(&'r ArrayRef),
	SumInt(&'r NumericArrayReader<'a>),
	SumFloat(&'r NumericArrayReader<'a>),
	AvgInt(&'r NumericArrayReader<'a>),
	AvgDecimal(&'r NumericArrayReader<'a>),
	AvgFloat(&'r NumericArrayReader<'a>),
}

impl TypedKernel<'_, '_> {
	pub(in crate::exec) fn name(&self) -> String {
		match self {
			Self::CountStar => "typed:count_star".to_string(),
			Self::CountNonNull(_) => "typed:count_nonnull".to_string(),
			Self::SumInt(reader) => format!("typed:sum_int_{}", reader_type_name(reader)),
			Self::SumFloat(reader) => format!("typed:sum_float_{}", reader_type_name(reader)),
			Self::AvgInt(reader) => format!("typed:avg_int_{}", reader_type_name(reader)),
			Self::AvgDecimal(reader) => {
				format!("typed:avg_decimal_{}", reader_type_name(reader))
			}
			Self::AvgFloat(reader) => format!("typed:avg_float_{}", reader_type_name(reader)),
		}
	}
}

fn reader_type_name(reader: &NumericArrayReader<'_>) -> &'static str {
	match reader {
		NumericArrayReader::Int8(_) => "int8",
		NumericArrayReader::Int16(_) => "int16",
		NumericArrayReader::Int32(_) => "int32",
		NumericArrayReader::Int64(_) => "int64",
		NumericArrayReader::UInt8(_) => "uint8",
		NumericArrayReader::UInt16(_) => "uint16",
		NumericArrayReader::UInt32(_) => "uint32",
		NumericArrayReader::UInt64(_) => "uint64",
		NumericArrayReader::Float32(_) => "float32",
		NumericArrayReader::Float64(_) => "float64",
		NumericArrayReader::Decimal128(_) => "decimal128",
	}
}

/// Integer-valued readers: those `NumericArrayReader::value` returns as `NumericValue::Int`.
fn is_int_reader(reader: &NumericArrayReader<'_>) -> bool {
	!matches!(
		reader,
		NumericArrayReader::Float32(_) | NumericArrayReader::Float64(_)
	)
}

pub(in crate::exec) fn agg_state_kind(state: &AggState) -> &'static str {
	match state {
		AggState::Count(_) => "Count",
		AggState::Spread { .. } => "Spread",
		AggState::StringAgg(_) => "StringAgg",
		AggState::ScalarSubqueryValue { .. } => "ScalarSubqueryValue",
		AggState::SumInt { .. } => "SumInt",
		AggState::SumFloat { .. } => "SumFloat",
		AggState::MinInt32 { .. } => "MinInt32",
		AggState::MinInt64 { .. } => "MinInt64",
		AggState::MinFloat32 { .. } => "MinFloat32",
		AggState::MinFloat64 { .. } => "MinFloat64",
		AggState::MinDecimal128 { .. } => "MinDecimal128",
		AggState::MaxInt32 { .. } => "MaxInt32",
		AggState::MaxInt64 { .. } => "MaxInt64",
		AggState::MaxFloat32 { .. } => "MaxFloat32",
		AggState::MaxFloat64 { .. } => "MaxFloat64",
		AggState::MaxDecimal128 { .. } => "MaxDecimal128",
		AggState::AvgInt { .. } => "AvgInt",
		AggState::AvgFloat { .. } => "AvgFloat",
		AggState::AvgDecimal { .. } => "AvgDecimal",
		AggState::Quantile(_) => "Quantile",
	}
}

/// Only the functions whose states a typed kernel can hold; any other call makes the batch
/// row-at-a-time before a state template is built.
pub(in crate::exec) fn functions_may_be_typed(aggr_expr: &[BoundAggregateCall]) -> bool {
	aggr_expr.iter().all(|call| {
		matches!(
			call.function,
			AggregateFunction::CountAll
				| AggregateFunction::Count
				| AggregateFunction::Sum
				| AggregateFunction::Avg
		)
	})
}

/// A typed kernel for every aggregate, or `None`: the batch then runs row-at-a-time whole.
/// `template` holds the states `init_bound_states_with_seed` builds for this call list; every
/// group's states share their variants, since initialization depends on the calls alone.
pub(in crate::exec) fn select_typed_kernels<'r, 'a>(
	aggr_expr: &[BoundAggregateCall],
	aggr_arrays: &'r [Option<ArrayRef>],
	readers: &'r [Option<NumericArrayReader<'a>>],
	template: &[AggState],
) -> Option<Vec<TypedKernel<'r, 'a>>> {
	if template.len() != aggr_expr.len()
		|| aggr_arrays.len() != aggr_expr.len()
		|| readers.len() != aggr_expr.len()
	{
		return None;
	}
	aggr_expr
		.iter()
		.zip(template)
		.zip(aggr_arrays.iter().zip(readers))
		.map(
			|((call, state), (array, reader))| match (&call.function, state, array, reader) {
				(AggregateFunction::CountAll, AggState::Count(_), None, _) => {
					Some(TypedKernel::CountStar)
				}
				(AggregateFunction::Count, AggState::Count(_), Some(array), _) => {
					Some(TypedKernel::CountNonNull(array))
				}
				(AggregateFunction::Sum, AggState::SumInt { .. }, Some(_), Some(reader))
					if is_int_reader(reader) =>
				{
					Some(TypedKernel::SumInt(reader))
				}
				(AggregateFunction::Sum, AggState::SumFloat { .. }, Some(_), Some(reader)) => {
					Some(TypedKernel::SumFloat(reader))
				}
				(AggregateFunction::Avg, AggState::AvgInt { .. }, Some(_), Some(reader))
					if is_int_reader(reader) =>
				{
					Some(TypedKernel::AvgInt(reader))
				}
				(AggregateFunction::Avg, AggState::AvgDecimal { .. }, Some(_), Some(reader))
					if is_int_reader(reader) =>
				{
					Some(TypedKernel::AvgDecimal(reader))
				}
				(AggregateFunction::Avg, AggState::AvgFloat { .. }, Some(_), Some(reader)) => {
					Some(TypedKernel::AvgFloat(reader))
				}
				_ => None,
			},
		)
		.collect()
}

trait ToI128: Copy {
	fn to_i128(self) -> i128;
}

macro_rules! to_i128 {
	($($native:ty),*) => {
		$(impl ToI128 for $native {
			#[inline(always)]
			fn to_i128(self) -> i128 {
				self as i128
			}
		})*
	};
}

to_i128!(i8, i16, i32, i64, u8, u16, u32, u64, i128);

/// Calls `update(group, value)` for every non-null row, in row order. The null test is hoisted:
/// an input without nulls reads its values slice directly.
#[inline(always)]
fn drive_primitive<T, V, F>(
	array: &PrimitiveArray<T>,
	ids: &[u32],
	convert: impl Fn(T::Native) -> V,
	mut update: F,
) -> Result<(), ExecutionError>
where
	T: ArrowPrimitiveType,
	F: FnMut(usize, V) -> Result<(), ExecutionError>,
{
	let values = array.values();
	if values.len() != ids.len() {
		return Err(kernel_error(
			"aggregate input length differs from the batch's group ids",
		));
	}
	if array.null_count() == 0 {
		for (row, group) in ids.iter().enumerate() {
			update(*group as usize, convert(values[row]))?;
		}
	} else {
		for (row, group) in ids.iter().enumerate() {
			if array.is_valid(row) {
				update(*group as usize, convert(values[row]))?;
			}
		}
	}
	Ok(())
}

/// An integer-valued input as `NumericValue::Int` would carry it: the native value widened to
/// `i128` (a Decimal128 as its raw unscaled integer).
fn drive_int<F>(
	reader: &NumericArrayReader<'_>,
	ids: &[u32],
	update: F,
) -> Result<(), ExecutionError>
where
	F: FnMut(usize, i128) -> Result<(), ExecutionError>,
{
	match reader {
		NumericArrayReader::Int8(array) => drive_primitive(array, ids, ToI128::to_i128, update),
		NumericArrayReader::Int16(array) => drive_primitive(array, ids, ToI128::to_i128, update),
		NumericArrayReader::Int32(array) => drive_primitive(array, ids, ToI128::to_i128, update),
		NumericArrayReader::Int64(array) => drive_primitive(array, ids, ToI128::to_i128, update),
		NumericArrayReader::UInt8(array) => drive_primitive(array, ids, ToI128::to_i128, update),
		NumericArrayReader::UInt16(array) => drive_primitive(array, ids, ToI128::to_i128, update),
		NumericArrayReader::UInt32(array) => drive_primitive(array, ids, ToI128::to_i128, update),
		NumericArrayReader::UInt64(array) => drive_primitive(array, ids, ToI128::to_i128, update),
		NumericArrayReader::Decimal128(array) => {
			drive_primitive(array, ids, ToI128::to_i128, update)
		}
		NumericArrayReader::Float32(_) | NumericArrayReader::Float64(_) => Err(kernel_error(
			"integer aggregate kernel received a floating input",
		)),
	}
}

/// Any numeric input as the float accumulators take it today: a float widened to `f64`; an
/// integer-valued input through its `i128` `NumericValue::Int`, then `as f64`.
fn drive_float<F>(
	reader: &NumericArrayReader<'_>,
	ids: &[u32],
	update: F,
) -> Result<(), ExecutionError>
where
	F: FnMut(usize, f64) -> Result<(), ExecutionError>,
{
	let int = |value: i128| value as f64;
	match reader {
		NumericArrayReader::Float32(array) => drive_primitive(array, ids, |v| v as f64, update),
		NumericArrayReader::Float64(array) => drive_primitive(array, ids, |v| v, update),
		NumericArrayReader::Int8(array) => {
			drive_primitive(array, ids, |v| int(v.to_i128()), update)
		}
		NumericArrayReader::Int16(array) => {
			drive_primitive(array, ids, |v| int(v.to_i128()), update)
		}
		NumericArrayReader::Int32(array) => {
			drive_primitive(array, ids, |v| int(v.to_i128()), update)
		}
		NumericArrayReader::Int64(array) => {
			drive_primitive(array, ids, |v| int(v.to_i128()), update)
		}
		NumericArrayReader::UInt8(array) => {
			drive_primitive(array, ids, |v| int(v.to_i128()), update)
		}
		NumericArrayReader::UInt16(array) => {
			drive_primitive(array, ids, |v| int(v.to_i128()), update)
		}
		NumericArrayReader::UInt32(array) => {
			drive_primitive(array, ids, |v| int(v.to_i128()), update)
		}
		NumericArrayReader::UInt64(array) => {
			drive_primitive(array, ids, |v| int(v.to_i128()), update)
		}
		NumericArrayReader::Decimal128(array) => {
			drive_primitive(array, ids, |v| int(v.to_i128()), update)
		}
	}
}

fn kernel_error(message: &str) -> ExecutionError {
	ExecutionError::Kernel {
		message: format!("grouped fold typed kernel: {message}"),
	}
}

fn state_mismatch(kind: &str) -> ExecutionError {
	kernel_error(&format!("{kind} kernel reached a different accumulator"))
}

fn count_overflow() -> ExecutionError {
	ExecutionError::Kernel {
		message: "AVG count overflow".to_string(),
	}
}

/// Applies aggregate `index`'s kernel to every row of the batch, in row order. Updates and error
/// messages are those of the row-at-a-time loop for the same state and value.
pub(in crate::exec) fn apply_typed_kernel(
	kernel: &TypedKernel<'_, '_>,
	index: usize,
	ids: &[u32],
	states: &mut [Vec<AggState>],
) -> Result<(), ExecutionError> {
	match kernel {
		TypedKernel::CountStar => {
			for group in ids {
				match slot(states, *group as usize, index)? {
					AggState::Count(total) => *total += 1,
					_ => return Err(state_mismatch("COUNT(*)")),
				}
			}
			Ok(())
		}
		TypedKernel::CountNonNull(array) => {
			if array.len() != ids.len() {
				return Err(kernel_error(
					"aggregate input length differs from the batch's group ids",
				));
			}
			for (row, group) in ids.iter().enumerate() {
				if !array.is_null(row) {
					match slot(states, *group as usize, index)? {
						AggState::Count(total) => *total += 1,
						_ => return Err(state_mismatch("COUNT")),
					}
				}
			}
			Ok(())
		}
		TypedKernel::SumInt(reader) => drive_int(reader, ids, |group, value| {
			match slot(states, group, index)? {
				AggState::SumInt { total, seen } => {
					*total = total
						.checked_add(value)
						.ok_or_else(|| ExecutionError::Kernel {
							message: "integer SUM overflow".to_string(),
						})?;
					*seen = true;
					Ok(())
				}
				_ => Err(state_mismatch("SUM")),
			}
		}),
		TypedKernel::SumFloat(reader) => drive_float(reader, ids, |group, value| {
			match slot(states, group, index)? {
				AggState::SumFloat { total, seen } => {
					*total += value;
					*seen = true;
					Ok(())
				}
				_ => Err(state_mismatch("SUM")),
			}
		}),
		TypedKernel::AvgInt(reader) => drive_int(reader, ids, |group, value| {
			match slot(states, group, index)? {
				AggState::AvgInt { total, count } => {
					*total = total
						.checked_add(value)
						.ok_or_else(|| ExecutionError::Kernel {
							message: "integer AVG overflow".to_string(),
						})?;
					*count = count.checked_add(1).ok_or_else(count_overflow)?;
					Ok(())
				}
				_ => Err(state_mismatch("AVG")),
			}
		}),
		TypedKernel::AvgDecimal(reader) => drive_int(reader, ids, |group, value| {
			match slot(states, group, index)? {
				AggState::AvgDecimal { total, count, .. } => {
					*total = total
						.checked_add(value)
						.ok_or_else(|| ExecutionError::Kernel {
							message: "decimal AVG overflow".to_string(),
						})?;
					*count = count.checked_add(1).ok_or_else(count_overflow)?;
					Ok(())
				}
				_ => Err(state_mismatch("AVG")),
			}
		}),
		TypedKernel::AvgFloat(reader) => drive_float(reader, ids, |group, value| {
			match slot(states, group, index)? {
				AggState::AvgFloat { total, count } => {
					*total += value;
					*count = count.checked_add(1).ok_or_else(count_overflow)?;
					Ok(())
				}
				_ => Err(state_mismatch("AVG")),
			}
		}),
	}
}

#[inline(always)]
fn slot(
	states: &mut [Vec<AggState>],
	group: usize,
	index: usize,
) -> Result<&mut AggState, ExecutionError> {
	states
		.get_mut(group)
		.and_then(|group| group.get_mut(index))
		.ok_or_else(|| kernel_error("group id addresses no aggregate state"))
}

/// ADR 610: code-path counters for one aggregate instance, written once at finalize.
#[derive(Debug)]
pub(in crate::exec) struct FoldKernelReceipt {
	path: PathBuf,
	eligibility_rule: Option<TwoPhaseEligibility>,
	eligibility_reasons: BTreeMap<&'static str, u64>,
	batches_row_at_a_time: u64,
	batches_two_phase: u64,
	rows_folded: u64,
	/// Per aggregate: its function, state kind, input type, and updates per kernel name.
	aggregates: Vec<(String, String, String, BTreeMap<String, u64>)>,
	written: bool,
}

impl FoldKernelReceipt {
	pub(in crate::exec) fn new(path: PathBuf) -> Self {
		Self {
			path,
			eligibility_rule: None,
			eligibility_reasons: BTreeMap::new(),
			batches_row_at_a_time: 0,
			batches_two_phase: 0,
			rows_folded: 0,
			aggregates: Vec::new(),
			written: false,
		}
	}

	/// One batch: the shape it ran under, its rows, and per aggregate `(function, state kind, input type,
	/// kernel name)`. Each named kernel is charged the batch's rows.
	pub(in crate::exec) fn note_batch(
		&mut self,
		shape: GroupedFoldShape,
		rule: TwoPhaseEligibility,
		reason: EligibilityReason,
		rows: usize,
		aggregates: &[(String, String, String, String)],
	) {
		self.eligibility_rule.get_or_insert(rule);
		let batches = self.eligibility_reasons.entry(reason.name()).or_insert(0);
		*batches = batches.saturating_add(1);
		let rows = u64::try_from(rows).unwrap_or(u64::MAX);
		if shape == GroupedFoldShape::TwoPhase {
			self.batches_two_phase = self.batches_two_phase.saturating_add(1);
		} else {
			self.batches_row_at_a_time = self.batches_row_at_a_time.saturating_add(1);
		}
		self.rows_folded = self.rows_folded.saturating_add(rows);
		if self.aggregates.is_empty() {
			self.aggregates = aggregates
				.iter()
				.map(|(function, state, input, _)| {
					(
						function.clone(),
						state.clone(),
						input.clone(),
						BTreeMap::new(),
					)
				})
				.collect();
		}
		for (slot, (_, _, _, kernel)) in self.aggregates.iter_mut().zip(aggregates) {
			let updates = slot.3.entry(kernel.clone()).or_insert(0);
			*updates = updates.saturating_add(rows);
		}
	}

	pub(in crate::exec) fn write(
		&mut self,
		group_ids_peak_capacity_bytes: u64,
	) -> Result<(), ExecutionError> {
		if self.written {
			return Ok(());
		}
		let line = serde_json::json!({
			"kind": "grouped_fold_kernel_instance",
			"eligibility_rule": self.eligibility_rule.map(TwoPhaseEligibility::name),
			"eligibility_reasons": self.eligibility_reasons,
			"batches_row_at_a_time": self.batches_row_at_a_time,
			"batches_two_phase": self.batches_two_phase,
			"rows_folded": self.rows_folded,
			"group_ids_peak_capacity_bytes": group_ids_peak_capacity_bytes,
			"aggregates": self.aggregates.iter().map(|(function, state, input, kernels)| {
				serde_json::json!({
					"function": function,
					"state_kind": state,
					"input_type": input,
					"updates": kernels,
				})
			}).collect::<Vec<_>>(),
		});
		append_line(&self.path, &line)?;
		self.written = true;
		Ok(())
	}
}

fn append_line(path: &Path, line: &serde_json::Value) -> Result<(), ExecutionError> {
	use std::io::Write;

	let mut file = std::fs::OpenOptions::new()
		.create(true)
		.append(true)
		.open(path)
		.map_err(|error| {
			kernel_error(&format!(
				"fold kernel receipt {} could not be opened: {error}",
				path.display()
			))
		})?;
	writeln!(file, "{line}").map_err(|error| {
		kernel_error(&format!(
			"fold kernel receipt {} could not be written: {error}",
			path.display()
		))
	})
}

pub(in crate::exec) fn fold_kernel_receipt_path() -> Option<PathBuf> {
	static PATH: std::sync::OnceLock<Option<PathBuf>> = std::sync::OnceLock::new();
	PATH.get_or_init(|| std::env::var_os(GROUPED_FOLD_KERNEL_RECEIPT_ENV).map(PathBuf::from))
		.clone()
}

#[cfg(test)]
mod tests {
	use super::*;
	use arrow::array::{Decimal128Array, Float64Array, Int32Array, Int64Array};
	use std::sync::Arc;

	#[test]
	fn adr610_kernel_selection_per_state_and_input() {
		let decimal: ArrayRef = Arc::new(
			Decimal128Array::from(vec![1i128])
				.with_precision_and_scale(15, 2)
				.expect("decimal"),
		);
		let int64: ArrayRef = Arc::new(Int64Array::from(vec![1i64]));
		let int32: ArrayRef = Arc::new(Int32Array::from(vec![1i32]));
		let float: ArrayRef = Arc::new(Float64Array::from(vec![1.0f64]));
		fn reader(array: &ArrayRef) -> Option<NumericArrayReader<'_>> {
			super::super::numeric_array_reader(array).ok()
		}
		let call = |function| BoundAggregateCall {
			function,
			args: crate::ir::BoundExprProgram::empty(),
		};
		let cases: Vec<(AggregateFunction, AggState, Option<ArrayRef>, Option<&str>)> = vec![
			(
				AggregateFunction::Sum,
				AggState::SumInt {
					total: 0,
					seen: false,
				},
				Some(decimal.clone()),
				Some("typed:sum_int_decimal128"),
			),
			(
				AggregateFunction::Sum,
				AggState::SumInt {
					total: 0,
					seen: false,
				},
				Some(int64.clone()),
				Some("typed:sum_int_int64"),
			),
			(
				AggregateFunction::Sum,
				AggState::SumInt {
					total: 0,
					seen: false,
				},
				Some(float.clone()),
				None,
			),
			(
				AggregateFunction::Sum,
				AggState::SumFloat {
					total: 0.0,
					seen: false,
				},
				Some(float.clone()),
				Some("typed:sum_float_float64"),
			),
			(
				AggregateFunction::Sum,
				AggState::SumFloat {
					total: 0.0,
					seen: false,
				},
				Some(int32.clone()),
				Some("typed:sum_float_int32"),
			),
			(
				AggregateFunction::Avg,
				AggState::AvgDecimal {
					total: 0,
					count: 0,
					input_scale: 2,
				},
				Some(decimal.clone()),
				Some("typed:avg_decimal_decimal128"),
			),
			(
				AggregateFunction::Avg,
				AggState::AvgDecimal {
					total: 0,
					count: 0,
					input_scale: 2,
				},
				Some(float.clone()),
				None,
			),
			(
				AggregateFunction::Avg,
				AggState::AvgInt { total: 0, count: 0 },
				Some(int32.clone()),
				Some("typed:avg_int_int32"),
			),
			(
				AggregateFunction::Avg,
				AggState::AvgFloat {
					total: 0.0,
					count: 0,
				},
				Some(float.clone()),
				Some("typed:avg_float_float64"),
			),
			(
				AggregateFunction::CountAll,
				AggState::Count(0),
				None,
				Some("typed:count_star"),
			),
			(
				AggregateFunction::Count,
				AggState::Count(0),
				Some(int64.clone()),
				Some("typed:count_nonnull"),
			),
			(
				AggregateFunction::Min,
				AggState::MinInt64 { value: None },
				Some(int64.clone()),
				None,
			),
			// A state that disagrees with its function has no kernel.
			(
				AggregateFunction::Sum,
				AggState::Count(0),
				Some(int64.clone()),
				None,
			),
		];
		for (function, state, array, expected) in cases {
			let arrays = vec![array.clone()];
			let readers = vec![array.as_ref().and_then(reader)];
			let calls = vec![call(function.clone())];
			let kernels =
				select_typed_kernels(&calls, &arrays, &readers, std::slice::from_ref(&state));
			assert_eq!(
				kernels.map(|kernels| kernels[0].name()),
				expected.map(str::to_string),
				"{function:?} {}",
				agg_state_kind(&state)
			);
		}
		// One untyped aggregate makes the whole call list untyped.
		let arrays = vec![Some(int64.clone()), Some(int64.clone())];
		let readers = vec![reader(&int64), reader(&int64)];
		let calls = vec![call(AggregateFunction::Sum), call(AggregateFunction::Min)];
		let template = [
			AggState::SumInt {
				total: 0,
				seen: false,
			},
			AggState::MinInt64 { value: None },
		];
		assert!(select_typed_kernels(&calls, &arrays, &readers, &template).is_none());
	}

	/// Every typed kernel against today's per-row update (`add_sum_value`, `add_avg_value`, the
	/// COUNT increment) over the same rows and groups, for every reader type, with and without
	/// NULLs.
	#[test]
	fn adr610_every_kernel_matches_the_per_row_update_for_every_input_type() {
		use super::super::{add_avg_value, add_sum_value, numeric_array_reader};
		use arrow::array::{
			Float32Array, Int8Array, Int16Array, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
		};

		let ids: Vec<u32> = vec![0, 1, 0, 2, 1, 0];
		let valid = [true, false, true, true, false, true];
		let arrays = |with_nulls: bool| -> Vec<ArrayRef> {
			let mask = |row: usize| !with_nulls || valid[row];
			macro_rules! build {
				($array:ty, $values:expr) => {{
					let values = $values;
					Arc::new(<$array>::from(
						values
							.into_iter()
							.enumerate()
							.map(|(row, value)| mask(row).then_some(value))
							.collect::<Vec<_>>(),
					)) as ArrayRef
				}};
			}
			vec![
				build!(Int8Array, vec![1i8, -2, 127, -128, 3, 4]),
				build!(Int16Array, vec![1i16, -2, i16::MAX, i16::MIN, 3, 4]),
				build!(Int32Array, vec![1i32, -2, i32::MAX, i32::MIN, 3, 4]),
				build!(Int64Array, vec![1i64, -2, i64::MAX, i64::MIN, 3, 4]),
				build!(UInt8Array, vec![1u8, 2, u8::MAX, 0, 3, 4]),
				build!(UInt16Array, vec![1u16, 2, u16::MAX, 0, 3, 4]),
				build!(UInt32Array, vec![1u32, 2, u32::MAX, 0, 3, 4]),
				build!(UInt64Array, vec![1u64, 2, u64::MAX, 0, 3, 4]),
				build!(Float32Array, vec![1.5f32, -0.0, f32::NAN, 1e30, 0.1, -2.5]),
				build!(Float64Array, vec![1e16, 1.0, -1e16, 0.1, f64::NAN, -0.0]),
				Arc::new(
					Decimal128Array::from(
						vec![125i128, -7, 10i128.pow(30), -(10i128.pow(30)), 3, 4]
							.into_iter()
							.enumerate()
							.map(|(row, value)| mask(row).then_some(value))
							.collect::<Vec<_>>(),
					)
					.with_precision_and_scale(38, 2)
					.expect("decimal"),
				) as ArrayRef,
			]
		};
		let states_for = |state: &AggState| vec![vec![state.clone()]; 3];
		let mut checked = 0;
		for with_nulls in [false, true] {
			for array in arrays(with_nulls) {
				let reader = numeric_array_reader(&array).expect("numeric reader");
				let int = is_int_reader(&reader);
				let mut cases: Vec<(TypedKernel<'_, '_>, AggState)> = vec![
					(
						TypedKernel::SumFloat(&reader),
						AggState::SumFloat {
							total: 0.0,
							seen: false,
						},
					),
					(
						TypedKernel::AvgFloat(&reader),
						AggState::AvgFloat {
							total: 0.0,
							count: 0,
						},
					),
					(TypedKernel::CountNonNull(&array), AggState::Count(0)),
				];
				if int {
					cases.push((
						TypedKernel::SumInt(&reader),
						AggState::SumInt {
							total: 0,
							seen: false,
						},
					));
					cases.push((
						TypedKernel::AvgInt(&reader),
						AggState::AvgInt { total: 0, count: 0 },
					));
					cases.push((
						TypedKernel::AvgDecimal(&reader),
						AggState::AvgDecimal {
							total: 0,
							count: 0,
							input_scale: 2,
						},
					));
				}
				for (kernel, initial) in cases {
					let mut typed = states_for(&initial);
					apply_typed_kernel(&kernel, 0, &ids, &mut typed).expect("typed kernel");
					let mut reference = states_for(&initial);
					for (row, group) in ids.iter().enumerate() {
						let state = &mut reference[*group as usize][0];
						match &kernel {
							TypedKernel::CountNonNull(array) => {
								if !array.is_null(row) {
									if let AggState::Count(total) = state {
										*total += 1;
									}
								}
							}
							TypedKernel::SumInt(_) | TypedKernel::SumFloat(_) => {
								if let Some(value) = reader.value(row) {
									add_sum_value(state, value).expect("reference sum");
								}
							}
							TypedKernel::AvgInt(_)
							| TypedKernel::AvgDecimal(_)
							| TypedKernel::AvgFloat(_) => {
								if let Some(value) = reader.value(row) {
									add_avg_value(state, value).expect("reference avg");
								}
							}
							TypedKernel::CountStar => unreachable!(),
						}
					}
					assert_eq!(
						format!("{typed:?}"),
						format!("{reference:?}"),
						"{} {:?} nulls={with_nulls}",
						kernel.name(),
						array.data_type()
					);
					checked += 1;
				}
			}
		}
		// 11 types x 2 null settings x (3 kernels, plus 3 more for the 9 integer-valued types).
		assert_eq!(checked, 2 * (11 * 3 + 9 * 3));

		// COUNT(*) counts every row, NULL or not.
		let mut typed = vec![vec![AggState::Count(0)]; 3];
		apply_typed_kernel(&TypedKernel::CountStar, 0, &ids, &mut typed).expect("count star");
		assert_eq!(format!("{typed:?}"), "[[Count(3)], [Count(2)], [Count(1)]]");
	}
}
