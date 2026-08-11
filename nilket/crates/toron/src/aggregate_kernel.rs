use arrow::datatypes::{DataType, Field};

use crate::ir::{AggregateFunction, BoundAggregateCall};

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum StableAggregateKernel {
	CountAll = 0,
	Count = 1,
	Sum = 2,
	Min = 3,
	Max = 4,
	Avg = 5,
	ApproxQuantile = 6,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AggregateArity {
	Zero,
	One,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AggregateInputDomain {
	Any,
	SumNumeric,
	AvgNumeric,
	OrderedMinMax,
	QuantileNumeric,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AggregateOutputRule {
	Int64,
	SameAsInput,
	SumNumeric,
	// ADR 268: AVG — Float64 for integer/float input, widened Decimal for Decimal input (subsumes the
	// former flat `Float64` rule, which AVG was the only user of).
	AvgNumeric,
	Float64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct AggregateKernelSpec {
	pub(crate) kind: StableAggregateKernel,
	pub(crate) sql_name: &'static str,
	pub(crate) output_name: &'static str,
	pub(crate) arity: AggregateArity,
	pub(crate) input_domain: AggregateInputDomain,
	pub(crate) output_rule: AggregateOutputRule,
	pub(crate) nullable: bool,
	pub(crate) grouped_supported: bool,
}

pub(crate) const STABLE_AGGREGATE_KERNELS: &[AggregateKernelSpec] = &[
	AggregateKernelSpec {
		kind: StableAggregateKernel::CountAll,
		sql_name: "COUNT",
		output_name: "count",
		arity: AggregateArity::Zero,
		input_domain: AggregateInputDomain::Any,
		output_rule: AggregateOutputRule::Int64,
		nullable: false,
		grouped_supported: true,
	},
	AggregateKernelSpec {
		kind: StableAggregateKernel::Count,
		sql_name: "COUNT",
		output_name: "count",
		arity: AggregateArity::One,
		input_domain: AggregateInputDomain::Any,
		output_rule: AggregateOutputRule::Int64,
		nullable: false,
		grouped_supported: true,
	},
	AggregateKernelSpec {
		kind: StableAggregateKernel::Sum,
		sql_name: "SUM",
		output_name: "sum",
		arity: AggregateArity::One,
		input_domain: AggregateInputDomain::SumNumeric,
		output_rule: AggregateOutputRule::SumNumeric,
		nullable: true,
		grouped_supported: true,
	},
	AggregateKernelSpec {
		kind: StableAggregateKernel::Min,
		sql_name: "MIN",
		output_name: "min",
		arity: AggregateArity::One,
		input_domain: AggregateInputDomain::OrderedMinMax,
		output_rule: AggregateOutputRule::SameAsInput,
		nullable: true,
		grouped_supported: true,
	},
	AggregateKernelSpec {
		kind: StableAggregateKernel::Max,
		sql_name: "MAX",
		output_name: "max",
		arity: AggregateArity::One,
		input_domain: AggregateInputDomain::OrderedMinMax,
		output_rule: AggregateOutputRule::SameAsInput,
		nullable: true,
		grouped_supported: true,
	},
	AggregateKernelSpec {
		kind: StableAggregateKernel::Avg,
		sql_name: "AVG",
		output_name: "avg",
		arity: AggregateArity::One,
		input_domain: AggregateInputDomain::AvgNumeric,
		output_rule: AggregateOutputRule::AvgNumeric,
		nullable: true,
		grouped_supported: true,
	},
	AggregateKernelSpec {
		kind: StableAggregateKernel::ApproxQuantile,
		sql_name: "APPROX_QUANTILE",
		output_name: "quantile",
		arity: AggregateArity::One,
		input_domain: AggregateInputDomain::QuantileNumeric,
		output_rule: AggregateOutputRule::Float64,
		nullable: true,
		grouped_supported: true,
	},
];

pub(crate) const STABLE_AGGREGATE_SQL_NAMES: &[&str] =
	&["COUNT", "SUM", "MIN", "MAX", "AVG", "APPROX_QUANTILE"];

pub(crate) fn stable_kernel_for_function(
	function: &AggregateFunction,
) -> Option<StableAggregateKernel> {
	match function {
		AggregateFunction::CountAll => Some(StableAggregateKernel::CountAll),
		AggregateFunction::Count => Some(StableAggregateKernel::Count),
		AggregateFunction::Sum => Some(StableAggregateKernel::Sum),
		AggregateFunction::Min => Some(StableAggregateKernel::Min),
		AggregateFunction::Max => Some(StableAggregateKernel::Max),
		AggregateFunction::Avg => Some(StableAggregateKernel::Avg),
		AggregateFunction::ApproxQuantile(_) => Some(StableAggregateKernel::ApproxQuantile),
		AggregateFunction::LinearRegress => None,
	}
}

pub(crate) fn stable_spec(kernel: StableAggregateKernel) -> &'static AggregateKernelSpec {
	let spec = &STABLE_AGGREGATE_KERNELS[kernel as usize];
	debug_assert_eq!(spec.kind, kernel);
	spec
}

pub(crate) fn stable_spec_for_function(
	function: &AggregateFunction,
) -> Option<&'static AggregateKernelSpec> {
	stable_kernel_for_function(function).map(stable_spec)
}

pub(crate) fn is_stable_aggregate_sql_name(upper: &str) -> bool {
	stable_aggregate_sql_name(upper).is_some()
}

pub(crate) fn stable_aggregate_sql_name(upper: &str) -> Option<&'static str> {
	STABLE_AGGREGATE_SQL_NAMES
		.iter()
		.copied()
		.find(|name| *name == upper)
}

pub(crate) fn experimental_aggregate_sql_name(upper: &str) -> Option<&'static str> {
	match upper {
		"LINEAR_REGRESS" => Some("LINEAR_REGRESS"),
		_ => None,
	}
}

pub(crate) fn stable_output_data_type(
	function: &AggregateFunction,
	input: Option<&DataType>,
) -> Option<DataType> {
	let spec = stable_spec_for_function(function)?;
	if !input_domain_accepts(spec.input_domain, input) {
		return None;
	}
	match (spec.output_rule, input) {
		(AggregateOutputRule::Int64, _) => Some(DataType::Int64),
		(AggregateOutputRule::SameAsInput, Some(data_type)) => Some(data_type.clone()),
		(AggregateOutputRule::SumNumeric, Some(data_type)) => sum_numeric_output(data_type),
		(AggregateOutputRule::AvgNumeric, Some(data_type)) => avg_numeric_output(data_type),
		(AggregateOutputRule::Float64, Some(_)) => Some(DataType::Float64),
		(_, None) => None,
	}
}

pub(crate) fn stable_output_field(call: &BoundAggregateCall) -> Option<Field> {
	let spec = stable_spec_for_function(&call.function)?;
	let input_type = call.args.root_type(0);
	let data_type = stable_output_data_type(&call.function, input_type)
		.unwrap_or_else(|| fallback_output_data_type(spec.output_rule));
	Some(Field::new(spec.output_name, data_type, spec.nullable))
}

pub(crate) fn input_domain_accepts(domain: AggregateInputDomain, input: Option<&DataType>) -> bool {
	match domain {
		AggregateInputDomain::Any => true,
		AggregateInputDomain::SumNumeric => input.is_some_and(is_sum_numeric_input),
		AggregateInputDomain::AvgNumeric => input.is_some_and(is_avg_numeric_input),
		AggregateInputDomain::OrderedMinMax => input.is_some_and(is_ordered_min_max_input),
		AggregateInputDomain::QuantileNumeric => input.is_some_and(is_quantile_numeric_input),
	}
}

fn fallback_output_data_type(rule: AggregateOutputRule) -> DataType {
	match rule {
		// ADR 268: AvgNumeric's fallback (no input type available) is Float64, matching plain AVG.
		AggregateOutputRule::AvgNumeric | AggregateOutputRule::Float64 => DataType::Float64,
		AggregateOutputRule::Int64
		| AggregateOutputRule::SameAsInput
		| AggregateOutputRule::SumNumeric => DataType::Int64,
	}
}

fn is_quantile_numeric_input(input: &DataType) -> bool {
	matches!(
		input,
		DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64
	)
}

fn sum_numeric_output(input: &DataType) -> Option<DataType> {
	match input {
		DataType::Int8
		| DataType::Int16
		| DataType::Int32
		| DataType::Int64
		| DataType::UInt8
		| DataType::UInt16
		| DataType::UInt32
		| DataType::UInt64 => Some(DataType::Int64),
		DataType::Float32 | DataType::Float64 => Some(DataType::Float64),
		// ADR 263: SUM(Decimal128(p,s)) -> Decimal128(p,s) (the sum carries the input type).
		DataType::Decimal128(precision, scale) => Some(DataType::Decimal128(*precision, *scale)),
		_ => None,
	}
}

fn is_sum_numeric_input(input: &DataType) -> bool {
	sum_numeric_output(input).is_some()
}

// ADR 268: input-domain acceptance and output-type derivation for AVG share this one helper, so a
// high-scale Decimal whose widened average cannot fit Decimal128 is rejected by the input domain rather
// than admitted and then silently masked by `stable_output_field`'s fallback type.
fn avg_numeric_output(input: &DataType) -> Option<DataType> {
	match input {
		DataType::Int8
		| DataType::Int16
		| DataType::Int32
		| DataType::Int64
		| DataType::UInt8
		| DataType::UInt16
		| DataType::UInt32
		| DataType::UInt64
		| DataType::Float32
		| DataType::Float64 => Some(DataType::Float64),
		// ADR 268: AVG over Decimal stays Decimal (decimal-faithful), widened scale/precision.
		DataType::Decimal128(precision, scale) => decimal_avg_output_type(*precision, *scale),
		_ => None,
	}
}

// ADR 268: `AVG(Decimal128(p,s)) -> Decimal128(min(38, p+4), s+4)`. The fractional budget grows by four and
// precision grows with it so the integer-digit budget `(p-s)` is preserved. Returns `None` (a clean bind
// rejection) when the widened scale cannot fit the widened precision — e.g. a very high-scale input.
fn decimal_avg_output_type(precision: u8, scale: i8) -> Option<DataType> {
	let out_precision = precision.saturating_add(4).min(38);
	let out_scale = scale.checked_add(4)?;
	if out_scale < 0 || i16::from(out_scale) > i16::from(out_precision) {
		return None;
	}
	Some(DataType::Decimal128(out_precision, out_scale))
}

fn is_avg_numeric_input(input: &DataType) -> bool {
	avg_numeric_output(input).is_some()
}

fn is_ordered_min_max_input(input: &DataType) -> bool {
	matches!(
		input,
		DataType::Int32
			| DataType::Int64
			| DataType::Float32
			| DataType::Float64
			| DataType::Decimal128(_, _)
	)
}
