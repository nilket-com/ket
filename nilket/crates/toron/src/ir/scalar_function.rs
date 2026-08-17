//! Typed scalar-function registry.
//!
//! A scalar capability is admitted here once, then every planner-wide rule reads
//! its metadata through the canonical identifier. Evaluators may still match on
//! [`ScalarFunctionEvaluator`]; they are the one layer that genuinely cares what
//! computation a function performs.

use serde::{Deserialize, Serialize};

/// Canonical identifier carried by both unbound and bound scalar expressions.
///
/// Keep this typed. A string would move name resolution onto the row path and
/// would make a misspelled function a runtime dispatch failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ScalarFunctionId {
	Abs,
	Exp,
	Pow,
	Sqrt,
}

/// The accepted argument count for a scalar function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarFunctionArity {
	Exact(usize),
}

impl ScalarFunctionArity {
	pub const fn accepts(self, count: usize) -> bool {
		match self {
			Self::Exact(expected) => count == expected,
		}
	}

	pub const fn description(self) -> &'static str {
		match self {
			Self::Exact(1) => "exactly one argument",
			Self::Exact(2) => "exactly two arguments",
			Self::Exact(_) => "the registered argument count",
		}
	}
}

/// Binder-side type rule. The output rule belongs beside the input rule so a
/// function cannot acquire a second, executor-only type table.
///
/// A rule is shared by every function that obeys it. `SQRT` reuses `EXP`'s rule
/// unchanged, which is the property that makes a registration cheap; `POW` and
/// `ABS` each needed a rule because their domains genuinely differ.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarFunctionSignature {
	UnaryFloatPreserving,
	/// Both operands must be the same float width, which is also the output.
	/// Mixed widths are rejected rather than silently promoted — promotion is a
	/// decision about the engine's coercion rules, not about this function.
	BinaryFloatPreserving,
	/// Output type equals input type across the numeric set. Integers and
	/// decimals stay themselves rather than becoming floats.
	UnaryNumericPreserving,
}

/// How NULL inputs affect the output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarFunctionNullBehavior {
	Propagates,
}

/// How non-finite floating inputs affect the output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarFunctionNonFiniteBehavior {
	Propagates,
}

/// The only function-specific choice the executor makes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarFunctionEvaluator {
	Abs,
	Exp,
	Pow,
	Sqrt,
}

/// Planner and execution metadata for one scalar capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScalarFunctionMetadata {
	pub id: ScalarFunctionId,
	pub sql_name: &'static str,
	pub rune_name: &'static str,
	pub arity: ScalarFunctionArity,
	pub signature: ScalarFunctionSignature,
	pub null_behavior: ScalarFunctionNullBehavior,
	pub non_finite_behavior: ScalarFunctionNonFiniteBehavior,
	pub deterministic: bool,
	pub pbc_pushdown_eligible: bool,
	pub evaluator: ScalarFunctionEvaluator,
}

const EXP: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Exp,
	sql_name: "EXP",
	rune_name: "exp",
	arity: ScalarFunctionArity::Exact(1),
	signature: ScalarFunctionSignature::UnaryFloatPreserving,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	deterministic: true,
	pbc_pushdown_eligible: false,
	evaluator: ScalarFunctionEvaluator::Exp,
};

const SQRT: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Sqrt,
	sql_name: "SQRT",
	rune_name: "sqrt",
	arity: ScalarFunctionArity::Exact(1),
	signature: ScalarFunctionSignature::UnaryFloatPreserving,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	deterministic: true,
	pbc_pushdown_eligible: false,
	evaluator: ScalarFunctionEvaluator::Sqrt,
};

const POW: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Pow,
	sql_name: "POW",
	rune_name: "pow",
	arity: ScalarFunctionArity::Exact(2),
	signature: ScalarFunctionSignature::BinaryFloatPreserving,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	deterministic: true,
	pbc_pushdown_eligible: false,
	evaluator: ScalarFunctionEvaluator::Pow,
};

const ABS: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Abs,
	sql_name: "ABS",
	rune_name: "abs",
	arity: ScalarFunctionArity::Exact(1),
	signature: ScalarFunctionSignature::UnaryNumericPreserving,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	deterministic: true,
	pbc_pushdown_eligible: false,
	evaluator: ScalarFunctionEvaluator::Abs,
};

/// The complete scalar-function registry.
pub const SCALAR_FUNCTIONS: &[ScalarFunctionMetadata] = &[ABS, EXP, POW, SQRT];

pub fn scalar_function_metadata(id: ScalarFunctionId) -> &'static ScalarFunctionMetadata {
	SCALAR_FUNCTIONS
		.iter()
		.find(|metadata| metadata.id == id)
		.expect("every ScalarFunctionId must have registry metadata")
}

pub fn scalar_function_from_sql_name(name: &str) -> Option<ScalarFunctionId> {
	SCALAR_FUNCTIONS
		.iter()
		.find(|metadata| metadata.sql_name.eq_ignore_ascii_case(name))
		.map(|metadata| metadata.id)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn exp_registration_is_typed_and_complete() {
		let metadata = scalar_function_metadata(ScalarFunctionId::Exp);
		assert_eq!(metadata.sql_name, "EXP");
		assert_eq!(metadata.rune_name, "exp");
		assert!(metadata.arity.accepts(1));
		assert!(!metadata.arity.accepts(2));
		assert_eq!(metadata.evaluator, ScalarFunctionEvaluator::Exp);
	}

	#[test]
	fn every_registration_is_reachable_by_both_spellings() {
		for metadata in SCALAR_FUNCTIONS {
			assert_eq!(
				scalar_function_from_sql_name(metadata.sql_name),
				Some(metadata.id),
				"{} must resolve from its own SQL name",
				metadata.sql_name
			);
			assert_eq!(
				metadata.sql_name.to_ascii_lowercase(),
				metadata.rune_name,
				"the two surfaces must spell one function the same way"
			);
			assert_eq!(scalar_function_metadata(metadata.id), metadata);
		}
	}

	/// Two entries sharing an id would make `scalar_function_metadata` return
	/// whichever was registered first, silently discarding the other's rules.
	#[test]
	fn registrations_are_unique_by_id_and_name() {
		let registered = SCALAR_FUNCTIONS.len();
		let ids = SCALAR_FUNCTIONS
			.iter()
			.map(|metadata| metadata.id)
			.collect::<std::collections::HashSet<_>>();
		assert_eq!(
			ids.len(),
			registered,
			"duplicate ScalarFunctionId registered"
		);

		let mut names = SCALAR_FUNCTIONS
			.iter()
			.map(|m| m.sql_name.to_ascii_uppercase())
			.collect::<Vec<_>>();
		names.sort_unstable();
		names.dedup();
		assert_eq!(names.len(), registered, "duplicate SQL name registered");
	}

	#[test]
	fn pow_is_the_binary_arity_case() {
		let metadata = scalar_function_metadata(ScalarFunctionId::Pow);
		assert!(!metadata.arity.accepts(1));
		assert!(metadata.arity.accepts(2));
		assert_eq!(metadata.arity.description(), "exactly two arguments");
	}
}
