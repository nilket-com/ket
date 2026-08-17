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
	Exp,
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
			Self::Exact(_) => "the registered argument count",
		}
	}
}

/// Binder-side type rule. The output rule belongs beside the input rule so a
/// function cannot acquire a second, executor-only type table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarFunctionSignature {
	UnaryFloatPreserving,
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
	Exp,
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

/// The complete scalar-function registry.
pub const SCALAR_FUNCTIONS: &[ScalarFunctionMetadata] = &[EXP];

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
}
