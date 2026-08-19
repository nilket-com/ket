//! Aggregate metadata registry (ADR 561).
//!
//! An aggregate capability is admitted here once, then every planner-wide rule
//! reads its metadata through the canonical kernel kind. Consumers read
//! properties; they do not match on `AggregateFunction` to re-derive a
//! classification the registry already knows.
//!
//! The registry names **what** an aggregate is — its types, its nullability,
//! its order sensitivity, which accumulator family it starts in. It never
//! describes **how** a kernel updates or finalizes: accumulation and
//! finalization live in the executor, where an algorithmic fact like Welford's
//! update differing from a running sum stays readable and testable.

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
	VarPop = 7,
	VarSamp = 8,
	StddevPop = 9,
	StddevSamp = 10,
	LinearRegress = 11,
}

impl StableAggregateKernel {
	/// Every kind, in discriminant order. The registry array below must list
	/// its entries in this same order because [`stable_spec`] indexes by
	/// discriminant; the unit test over this slice turns that requirement into
	/// a hard failure rather than a `debug_assert` a release build skips.
	#[cfg(test)]
	pub(crate) const ALL: &'static [Self] = &[
		Self::CountAll,
		Self::Count,
		Self::Sum,
		Self::Min,
		Self::Max,
		Self::Avg,
		Self::ApproxQuantile,
		Self::VarPop,
		Self::VarSamp,
		Self::StddevPop,
		Self::StddevSamp,
		Self::LinearRegress,
	];
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AggregateArity {
	Zero,
	One,
	Two,
}

impl AggregateArity {
	pub(crate) const fn len(self) -> usize {
		match self {
			Self::Zero => 0,
			Self::One => 1,
			Self::Two => 2,
		}
	}
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

/// How an aggregate's output relates to a named logical type on its input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AggregateLogicalTypeRule {
	/// The output is a plain magnitude or count and carries no logical type,
	/// whatever the input carried.
	Naked,
	/// The output is one of the input's values, so an unchanged output type
	/// keeps the input's logical type (`MIN`/`MAX`).
	Preserve,
	/// The output is in the input's unit only when that unit is additive
	/// (`SUM`/`AVG`); the additive-unit rule in the binder decides.
	AdditiveUnit,
}

/// Whether a grouped result can depend on the order rows arrive in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AggregateOrderSensitivity {
	/// The result is identical under any arrival order.
	Insensitive,
	/// Exact over integers and decimals; float rounding makes the result
	/// arrival-order dependent (`SUM`/`AVG`).
	InsensitiveUnlessFloat,
	/// The accumulator works in floating point (or is inherently sequenced),
	/// so arrival order can change the answer regardless of input type.
	Sensitive,
}

/// Which accumulator shape a function starts in.
///
/// This names the state; it does not describe the update. Four spread
/// spellings share one `Spread` family because they share one accumulator and
/// differ only at finalization — which is the executor's fact to own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AggregateStateFamily {
	Count,
	Sum,
	Min,
	Max,
	Avg,
	Quantile,
	Spread,
	/// Experimental kernels own their whole execution path and have no shared
	/// grouped accumulator.
	Experimental,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct AggregateKernelSpec {
	pub(crate) kind: StableAggregateKernel,
	pub(crate) sql_name: &'static str,
	/// Additional accepted SQL spellings. An alias must preserve the canonical
	/// entry's argument convention; a spelling with a different calling
	/// convention gets its own registration.
	pub(crate) sql_aliases: &'static [&'static str],
	/// The Rune spelling. No runtime code reads this field: its enforcement is
	/// the architecture guard's source scan, which asserts the Rune engine
	/// registers every entry's spelling — a check that can fail, unlike a field
	/// nothing consults. Mirrors how the scalar registry's rune names are held.
	#[allow(dead_code)]
	pub(crate) rune_name: &'static str,
	pub(crate) output_name: &'static str,
	pub(crate) arity: AggregateArity,
	pub(crate) input_domain: AggregateInputDomain,
	pub(crate) output_rule: AggregateOutputRule,
	pub(crate) logical_type: AggregateLogicalTypeRule,
	pub(crate) order_sensitivity: AggregateOrderSensitivity,
	pub(crate) window_eligible: bool,
	pub(crate) state_family: AggregateStateFamily,
	/// Whether a **non-empty** group can answer NULL — true for every function
	/// that skips NULL inputs, since an all-NULL group leaves the accumulator
	/// empty, and for the sample spreads below two observations.
	pub(crate) nullable_for_nonempty: bool,
	/// Whether an **empty** group (a global aggregate over zero rows) answers
	/// NULL. `COUNT` answers 0; almost everything else answers NULL. Kept
	/// separate from the non-empty fact because they are separately true.
	pub(crate) nullable_for_empty: bool,
	pub(crate) grouped_supported: bool,
	/// Experimental kernels bind and execute only behind
	/// `allow_experimental_kernels`; the gate reads this field.
	pub(crate) experimental: bool,
}

pub(crate) const STABLE_AGGREGATE_KERNELS: &[AggregateKernelSpec] = &[
	AggregateKernelSpec {
		kind: StableAggregateKernel::CountAll,
		sql_name: "COUNT",
		sql_aliases: &[],
		rune_name: "count_all",
		output_name: "count",
		arity: AggregateArity::Zero,
		input_domain: AggregateInputDomain::Any,
		output_rule: AggregateOutputRule::Int64,
		logical_type: AggregateLogicalTypeRule::Naked,
		order_sensitivity: AggregateOrderSensitivity::Insensitive,
		window_eligible: true,
		state_family: AggregateStateFamily::Count,
		nullable_for_nonempty: false,
		nullable_for_empty: false,
		grouped_supported: true,
		experimental: false,
	},
	AggregateKernelSpec {
		kind: StableAggregateKernel::Count,
		sql_name: "COUNT",
		sql_aliases: &[],
		rune_name: "count",
		output_name: "count",
		arity: AggregateArity::One,
		input_domain: AggregateInputDomain::Any,
		output_rule: AggregateOutputRule::Int64,
		logical_type: AggregateLogicalTypeRule::Naked,
		order_sensitivity: AggregateOrderSensitivity::Insensitive,
		window_eligible: true,
		state_family: AggregateStateFamily::Count,
		nullable_for_nonempty: false,
		nullable_for_empty: false,
		grouped_supported: true,
		experimental: false,
	},
	AggregateKernelSpec {
		kind: StableAggregateKernel::Sum,
		sql_name: "SUM",
		sql_aliases: &[],
		rune_name: "sum",
		output_name: "sum",
		arity: AggregateArity::One,
		input_domain: AggregateInputDomain::SumNumeric,
		output_rule: AggregateOutputRule::SumNumeric,
		logical_type: AggregateLogicalTypeRule::AdditiveUnit,
		order_sensitivity: AggregateOrderSensitivity::InsensitiveUnlessFloat,
		window_eligible: true,
		state_family: AggregateStateFamily::Sum,
		nullable_for_nonempty: true,
		nullable_for_empty: true,
		grouped_supported: true,
		experimental: false,
	},
	AggregateKernelSpec {
		kind: StableAggregateKernel::Min,
		sql_name: "MIN",
		sql_aliases: &[],
		rune_name: "min",
		output_name: "min",
		arity: AggregateArity::One,
		input_domain: AggregateInputDomain::OrderedMinMax,
		output_rule: AggregateOutputRule::SameAsInput,
		logical_type: AggregateLogicalTypeRule::Preserve,
		order_sensitivity: AggregateOrderSensitivity::Insensitive,
		window_eligible: true,
		state_family: AggregateStateFamily::Min,
		nullable_for_nonempty: true,
		nullable_for_empty: true,
		grouped_supported: true,
		experimental: false,
	},
	AggregateKernelSpec {
		kind: StableAggregateKernel::Max,
		sql_name: "MAX",
		sql_aliases: &[],
		rune_name: "max",
		output_name: "max",
		arity: AggregateArity::One,
		input_domain: AggregateInputDomain::OrderedMinMax,
		output_rule: AggregateOutputRule::SameAsInput,
		logical_type: AggregateLogicalTypeRule::Preserve,
		order_sensitivity: AggregateOrderSensitivity::Insensitive,
		window_eligible: true,
		state_family: AggregateStateFamily::Max,
		nullable_for_nonempty: true,
		nullable_for_empty: true,
		grouped_supported: true,
		experimental: false,
	},
	AggregateKernelSpec {
		kind: StableAggregateKernel::Avg,
		sql_name: "AVG",
		sql_aliases: &[],
		rune_name: "avg",
		output_name: "avg",
		arity: AggregateArity::One,
		input_domain: AggregateInputDomain::AvgNumeric,
		output_rule: AggregateOutputRule::AvgNumeric,
		logical_type: AggregateLogicalTypeRule::AdditiveUnit,
		order_sensitivity: AggregateOrderSensitivity::InsensitiveUnlessFloat,
		window_eligible: true,
		state_family: AggregateStateFamily::Avg,
		nullable_for_nonempty: true,
		nullable_for_empty: true,
		grouped_supported: true,
		experimental: false,
	},
	AggregateKernelSpec {
		kind: StableAggregateKernel::ApproxQuantile,
		sql_name: "APPROX_QUANTILE",
		sql_aliases: &[],
		rune_name: "approx_quantile",
		output_name: "quantile",
		arity: AggregateArity::One,
		input_domain: AggregateInputDomain::QuantileNumeric,
		output_rule: AggregateOutputRule::Float64,
		logical_type: AggregateLogicalTypeRule::Naked,
		order_sensitivity: AggregateOrderSensitivity::Sensitive,
		window_eligible: false,
		state_family: AggregateStateFamily::Quantile,
		nullable_for_nonempty: true,
		nullable_for_empty: true,
		grouped_supported: true,
		experimental: false,
	},
	// ADR 560: the spread family accumulates in Float64 and answers Float64,
	// unlike AVG's input-preserving rule. A variance divides and takes a square
	// root, so no decimal scale is the honest one.
	AggregateKernelSpec {
		kind: StableAggregateKernel::VarPop,
		sql_name: "VAR_POP",
		sql_aliases: &[],
		rune_name: "var_pop",
		output_name: "var_pop",
		arity: AggregateArity::One,
		input_domain: AggregateInputDomain::AvgNumeric,
		output_rule: AggregateOutputRule::Float64,
		logical_type: AggregateLogicalTypeRule::Naked,
		order_sensitivity: AggregateOrderSensitivity::Sensitive,
		window_eligible: false,
		state_family: AggregateStateFamily::Spread,
		nullable_for_nonempty: true,
		nullable_for_empty: true,
		grouped_supported: true,
		experimental: false,
	},
	AggregateKernelSpec {
		kind: StableAggregateKernel::VarSamp,
		sql_name: "VAR_SAMP",
		// ADR 560: the bare name is a cross-engine convention meaning the
		// sample form; the alias resolves here rather than in a planner match.
		sql_aliases: &["VARIANCE"],
		rune_name: "var_samp",
		output_name: "var_samp",
		arity: AggregateArity::One,
		input_domain: AggregateInputDomain::AvgNumeric,
		output_rule: AggregateOutputRule::Float64,
		logical_type: AggregateLogicalTypeRule::Naked,
		order_sensitivity: AggregateOrderSensitivity::Sensitive,
		window_eligible: false,
		state_family: AggregateStateFamily::Spread,
		nullable_for_nonempty: true,
		nullable_for_empty: true,
		grouped_supported: true,
		experimental: false,
	},
	AggregateKernelSpec {
		kind: StableAggregateKernel::StddevPop,
		sql_name: "STDDEV_POP",
		sql_aliases: &[],
		rune_name: "stddev_pop",
		output_name: "stddev_pop",
		arity: AggregateArity::One,
		input_domain: AggregateInputDomain::AvgNumeric,
		output_rule: AggregateOutputRule::Float64,
		logical_type: AggregateLogicalTypeRule::Naked,
		order_sensitivity: AggregateOrderSensitivity::Sensitive,
		window_eligible: false,
		state_family: AggregateStateFamily::Spread,
		nullable_for_nonempty: true,
		nullable_for_empty: true,
		grouped_supported: true,
		experimental: false,
	},
	AggregateKernelSpec {
		kind: StableAggregateKernel::StddevSamp,
		sql_name: "STDDEV_SAMP",
		sql_aliases: &["STDDEV"],
		rune_name: "stddev_samp",
		output_name: "stddev_samp",
		arity: AggregateArity::One,
		input_domain: AggregateInputDomain::AvgNumeric,
		output_rule: AggregateOutputRule::Float64,
		logical_type: AggregateLogicalTypeRule::Naked,
		order_sensitivity: AggregateOrderSensitivity::Sensitive,
		window_eligible: false,
		state_family: AggregateStateFamily::Spread,
		nullable_for_nonempty: true,
		nullable_for_empty: true,
		grouped_supported: true,
		experimental: false,
	},
	// ADR 561: the one experimental kernel joins the registry so the mapping
	// from `AggregateFunction` is exhaustive and its gate reads metadata. Its
	// bind and execution paths remain its own.
	AggregateKernelSpec {
		kind: StableAggregateKernel::LinearRegress,
		sql_name: "LINEAR_REGRESS",
		sql_aliases: &[],
		rune_name: "linear_regress",
		output_name: "linreg",
		arity: AggregateArity::Two,
		input_domain: AggregateInputDomain::Any,
		output_rule: AggregateOutputRule::Float64,
		logical_type: AggregateLogicalTypeRule::Naked,
		order_sensitivity: AggregateOrderSensitivity::Sensitive,
		window_eligible: false,
		state_family: AggregateStateFamily::Experimental,
		nullable_for_nonempty: true,
		nullable_for_empty: true,
		grouped_supported: false,
		experimental: true,
	},
];

/// ADR 561: alias resolution reads the entries rather than a hand match, so an
/// alias cannot exist without the canonical entry it points at.
pub(crate) fn stable_aggregate_sql_alias(upper: &str) -> Option<&'static str> {
	STABLE_AGGREGATE_KERNELS
		.iter()
		.find(|spec| !spec.experimental && spec.sql_aliases.contains(&upper))
		.map(|spec| spec.sql_name)
}

pub(crate) fn stable_kernel_for_function(function: &AggregateFunction) -> StableAggregateKernel {
	match function {
		AggregateFunction::CountAll => StableAggregateKernel::CountAll,
		AggregateFunction::Count => StableAggregateKernel::Count,
		AggregateFunction::Sum => StableAggregateKernel::Sum,
		AggregateFunction::Min => StableAggregateKernel::Min,
		AggregateFunction::Max => StableAggregateKernel::Max,
		AggregateFunction::Avg => StableAggregateKernel::Avg,
		AggregateFunction::ApproxQuantile(_) => StableAggregateKernel::ApproxQuantile,
		AggregateFunction::VarPop => StableAggregateKernel::VarPop,
		AggregateFunction::VarSamp => StableAggregateKernel::VarSamp,
		AggregateFunction::StddevPop => StableAggregateKernel::StddevPop,
		AggregateFunction::StddevSamp => StableAggregateKernel::StddevSamp,
		AggregateFunction::LinearRegress => StableAggregateKernel::LinearRegress,
	}
}

/// The construction complement of [`stable_kernel_for_function`], defined only
/// for kinds whose `AggregateFunction` carries no payload. A kind with call
/// data (`ApproxQuantile`) or its own bind path (the experimental kernel)
/// constructs through its own lowering arm. Co-located with the forward
/// mapping and round-trip tested so the pair cannot drift into a second
/// classification path.
pub(crate) fn function_for_payloadless_kind(
	kind: StableAggregateKernel,
) -> Option<AggregateFunction> {
	match kind {
		StableAggregateKernel::CountAll => Some(AggregateFunction::CountAll),
		StableAggregateKernel::Count => Some(AggregateFunction::Count),
		StableAggregateKernel::Sum => Some(AggregateFunction::Sum),
		StableAggregateKernel::Min => Some(AggregateFunction::Min),
		StableAggregateKernel::Max => Some(AggregateFunction::Max),
		StableAggregateKernel::Avg => Some(AggregateFunction::Avg),
		StableAggregateKernel::VarPop => Some(AggregateFunction::VarPop),
		StableAggregateKernel::VarSamp => Some(AggregateFunction::VarSamp),
		StableAggregateKernel::StddevPop => Some(AggregateFunction::StddevPop),
		StableAggregateKernel::StddevSamp => Some(AggregateFunction::StddevSamp),
		StableAggregateKernel::ApproxQuantile | StableAggregateKernel::LinearRegress => None,
	}
}

pub(crate) fn stable_spec(kernel: StableAggregateKernel) -> &'static AggregateKernelSpec {
	let spec = &STABLE_AGGREGATE_KERNELS[kernel as usize];
	debug_assert_eq!(spec.kind, kernel);
	spec
}

/// Every `AggregateFunction` has a registry entry (ADR 561). The mapping is
/// total: a new variant fails to compile in [`stable_kernel_for_function`]
/// rather than failing a query at run time.
pub(crate) fn stable_spec_for_function(
	function: &AggregateFunction,
) -> &'static AggregateKernelSpec {
	stable_spec(stable_kernel_for_function(function))
}

/// Resolves a call site to an entry. Name alone is deliberately not enough:
/// `COUNT` legitimately appears at two arities, so the lookup takes both and a
/// name-only resolver is not expressible against this registry.
pub(crate) fn aggregate_entry(
	upper_name: &str,
	arity: usize,
) -> Option<&'static AggregateKernelSpec> {
	let canonical = stable_aggregate_sql_alias(upper_name).unwrap_or(upper_name);
	STABLE_AGGREGATE_KERNELS
		.iter()
		.find(|spec| spec.sql_name == canonical && spec.arity.len() == arity)
}

pub(crate) fn is_stable_aggregate_sql_name(upper: &str) -> bool {
	stable_aggregate_sql_name(upper).is_some()
}

pub(crate) fn stable_aggregate_sql_name(upper: &str) -> Option<&'static str> {
	STABLE_AGGREGATE_KERNELS
		.iter()
		.find(|spec| !spec.experimental && spec.sql_name == upper)
		.map(|spec| spec.sql_name)
		.or_else(|| stable_aggregate_sql_alias(upper))
}

pub(crate) fn experimental_aggregate_sql_name(upper: &str) -> Option<&'static str> {
	STABLE_AGGREGATE_KERNELS
		.iter()
		.find(|spec| spec.experimental && spec.sql_name == upper)
		.map(|spec| spec.sql_name)
}

pub(crate) fn stable_output_data_type(
	function: &AggregateFunction,
	input: Option<&DataType>,
) -> Option<DataType> {
	let spec = stable_spec_for_function(function);
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

pub(crate) fn stable_output_field(call: &BoundAggregateCall) -> Field {
	let spec = stable_spec_for_function(&call.function);
	let input_type = call.args.root_type(0);
	let data_type = stable_output_data_type(&call.function, input_type)
		.unwrap_or_else(|| fallback_output_data_type(spec.output_rule));
	// Schema nullability is the union of the two facts: a field is nullable if
	// any group shape can answer NULL through it.
	Field::new(
		spec.output_name,
		data_type,
		spec.nullable_for_nonempty || spec.nullable_for_empty,
	)
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

#[cfg(test)]
mod adr561_registry_tests {
	use super::*;

	/// The array must list entries in discriminant order because
	/// [`stable_spec`] indexes by discriminant. ADR 560 shipped with this
	/// violated and only a `debug_assert` caught it; this test fails the build.
	#[test]
	fn registry_order_matches_kind_discriminants() {
		assert_eq!(
			STABLE_AGGREGATE_KERNELS.len(),
			StableAggregateKernel::ALL.len(),
			"every kind has exactly one entry"
		);
		for kind in StableAggregateKernel::ALL {
			assert_eq!(
				stable_spec(*kind).kind,
				*kind,
				"registry array order must match kind discriminants"
			);
		}
	}

	/// A duplicate (sql_name, arity) pair would stay reachable and resolve by
	/// whichever entry the table lists first.
	#[test]
	fn canonical_name_arity_pairs_are_unique() {
		for (index, spec) in STABLE_AGGREGATE_KERNELS.iter().enumerate() {
			for other in &STABLE_AGGREGATE_KERNELS[index + 1..] {
				assert!(
					spec.sql_name != other.sql_name || spec.arity != other.arity,
					"{} at arity {} is registered twice",
					spec.sql_name,
					spec.arity.len()
				);
			}
		}
	}

	/// An alias points at its own entry's canonical pair; it may not collide
	/// with any canonical name or any other alias, so resolution never depends
	/// on lookup order.
	#[test]
	fn aliases_collide_with_nothing() {
		let mut seen_aliases: Vec<&str> = Vec::new();
		for spec in STABLE_AGGREGATE_KERNELS {
			for alias in spec.sql_aliases {
				assert!(
					!STABLE_AGGREGATE_KERNELS
						.iter()
						.any(|other| other.sql_name == *alias),
					"alias {alias} collides with a canonical name"
				);
				assert!(
					!seen_aliases.contains(alias),
					"alias {alias} is registered twice"
				);
				seen_aliases.push(alias);
				assert_eq!(
					aggregate_entry(alias, spec.arity.len()).map(|entry| entry.kind),
					Some(spec.kind),
					"alias {alias} must resolve to its own entry"
				);
			}
		}
	}

	/// Name recognition serves SQL routing and must keep experimental kernels
	/// out of the stable set, exactly as the hand-written lists did.
	#[test]
	fn name_helpers_split_stable_from_experimental() {
		assert_eq!(stable_aggregate_sql_name("VARIANCE"), Some("VAR_SAMP"));
		assert_eq!(stable_aggregate_sql_name("STDDEV"), Some("STDDEV_SAMP"));
		assert_eq!(stable_aggregate_sql_name("COUNT"), Some("COUNT"));
		assert_eq!(stable_aggregate_sql_name("LINEAR_REGRESS"), None);
		assert_eq!(
			experimental_aggregate_sql_name("LINEAR_REGRESS"),
			Some("LINEAR_REGRESS")
		);
		assert_eq!(experimental_aggregate_sql_name("COUNT"), None);
	}

	/// The construction inverse must agree with the classification forward map,
	/// or the two would constitute the second mapping the ADR forbids.
	#[test]
	fn payloadless_construction_round_trips_through_classification() {
		let mut constructible = 0;
		for kind in StableAggregateKernel::ALL {
			if let Some(function) = function_for_payloadless_kind(*kind) {
				assert_eq!(
					stable_kernel_for_function(&function),
					*kind,
					"construction and classification disagree for {kind:?}"
				);
				constructible += 1;
			}
		}
		// Exactly the payload-carrying quantile and the experimental kernel
		// construct through their own arms.
		assert_eq!(constructible, StableAggregateKernel::ALL.len() - 2);
	}

	/// `COUNT` is the reason resolution takes name **and** arity.
	#[test]
	fn count_resolves_by_arity() {
		assert_eq!(
			aggregate_entry("COUNT", 0).map(|spec| spec.kind),
			Some(StableAggregateKernel::CountAll)
		);
		assert_eq!(
			aggregate_entry("COUNT", 1).map(|spec| spec.kind),
			Some(StableAggregateKernel::Count)
		);
		assert!(aggregate_entry("COUNT", 2).is_none());
	}
}
