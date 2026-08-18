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
	Ceil,
	CharLength,
	Exp,
	Floor,
	Ln,
	Lower,
	Mod,
	Pow,
	Round,
	Sign,
	Sqrt,
	Trim,
	Upper,
	WidthBucket,
}

/// The accepted argument count for a scalar function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarFunctionArity {
	Exact(usize),
	/// An inclusive range, for a function whose trailing arguments are optional.
	Between(usize, usize),
}

impl ScalarFunctionArity {
	pub const fn accepts(self, count: usize) -> bool {
		match self {
			Self::Exact(expected) => count == expected,
			Self::Between(min, max) => min <= count && count <= max,
		}
	}

	pub const fn description(self) -> &'static str {
		match self {
			Self::Exact(1) => "exactly one argument",
			Self::Exact(2) => "exactly two arguments",
			Self::Exact(4) => "exactly four arguments",
			Self::Exact(_) => "the registered argument count",
			Self::Between(1, 2) => "one or two arguments",
			Self::Between(_, _) => "the registered argument range",
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
	/// Output type equals input type for the closed integer-or-float set.
	/// Decimals are deliberately excluded: admitting them changes a pre-existing
	/// FLOOR contract and must be its own decision.
	UnaryIntegerOrFloatPreserving,
	/// A float value, optionally followed by an integer digit count. The value's
	/// own type is the output type; the digit count does not widen it.
	FloatWithOptionalIntegerDigits,
	/// Both operands must be the same integer width, which is also the output.
	/// The float case is deliberately absent: a floating remainder is a separate
	/// admission decision, not an implied consequence of this one.
	BinaryIntegerPreserving,
	/// Output type equals input type across the string set, so a `LargeUtf8`
	/// column does not silently narrow to `Utf8`.
	UnaryStringPreserving,
	/// A string in, an integer out. The first rule whose output type is drawn
	/// from neither the input nor the argument list, which is why the string
	/// family is the real test of this metadata model.
	StringToInteger,
	/// `WIDTH_BUCKET(operand, low, high, count)`: three same-typed floats and an
	/// integer bucket count, producing a bucket ordinal. The count is a shape
	/// parameter rather than a value, so it does not participate in the output
	/// type any more than ROUND's digit count does.
	NumericBucketing,
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
	/// Reject NaN and infinity when evaluation reaches a floating value.
	/// This is intentionally evaluation-time behavior, not binder validation.
	Rejects,
}

/// The only function-specific choice the executor makes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarFunctionEvaluator {
	Abs,
	Ceil,
	CharLength,
	Exp,
	Floor,
	Ln,
	Lower,
	Mod,
	Pow,
	Round,
	Sign,
	Sqrt,
	Trim,
	Upper,
	WidthBucket,
}

/// Planner and execution metadata for one scalar capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScalarFunctionMetadata {
	pub id: ScalarFunctionId,
	/// The ANSI spelling where the standard has one. Vendor spellings belong in
	/// [`ScalarFunctionMetadata::sql_aliases`], never here.
	pub sql_name: &'static str,
	/// Additional accepted SQL spellings. The standard admits two names for the
	/// ceiling function, and a vendor spelling already in use has to keep
	/// resolving once the canonical name is adopted.
	pub sql_aliases: &'static [&'static str],
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
	sql_aliases: &[],
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
	sql_aliases: &[],
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
	sql_name: "POWER",
	// `POW` is not an ANSI spelling. It shipped first here, so it stays
	// resolvable rather than breaking a query that already uses it.
	sql_aliases: &["POW"],
	rune_name: "power",
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
	sql_aliases: &[],
	rune_name: "abs",
	arity: ScalarFunctionArity::Exact(1),
	signature: ScalarFunctionSignature::UnaryNumericPreserving,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	deterministic: true,
	pbc_pushdown_eligible: false,
	evaluator: ScalarFunctionEvaluator::Abs,
};

const LN: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Ln,
	sql_name: "LN",
	sql_aliases: &[],
	rune_name: "ln",
	arity: ScalarFunctionArity::Exact(1),
	signature: ScalarFunctionSignature::UnaryFloatPreserving,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	deterministic: true,
	pbc_pushdown_eligible: false,
	evaluator: ScalarFunctionEvaluator::Ln,
};

const ROUND: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Round,
	sql_name: "ROUND",
	sql_aliases: &[],
	rune_name: "round",
	arity: ScalarFunctionArity::Between(1, 2),
	signature: ScalarFunctionSignature::FloatWithOptionalIntegerDigits,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	deterministic: true,
	pbc_pushdown_eligible: false,
	evaluator: ScalarFunctionEvaluator::Round,
};

const CEIL: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Ceil,
	sql_name: "CEIL",
	sql_aliases: &["CEILING"],
	rune_name: "ceil",
	arity: ScalarFunctionArity::Exact(1),
	signature: ScalarFunctionSignature::UnaryFloatPreserving,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	deterministic: true,
	pbc_pushdown_eligible: false,
	evaluator: ScalarFunctionEvaluator::Ceil,
};

const FLOOR: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Floor,
	sql_name: "FLOOR",
	sql_aliases: &[],
	rune_name: "floor",
	arity: ScalarFunctionArity::Exact(1),
	signature: ScalarFunctionSignature::UnaryIntegerOrFloatPreserving,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Rejects,
	deterministic: true,
	pbc_pushdown_eligible: false,
	evaluator: ScalarFunctionEvaluator::Floor,
};

const SIGN: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Sign,
	sql_name: "SIGN",
	sql_aliases: &[],
	rune_name: "sign",
	arity: ScalarFunctionArity::Exact(1),
	signature: ScalarFunctionSignature::UnaryNumericPreserving,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	deterministic: true,
	pbc_pushdown_eligible: false,
	evaluator: ScalarFunctionEvaluator::Sign,
};

const MOD: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Mod,
	sql_name: "MOD",
	sql_aliases: &[],
	// `mod` is a keyword in Rune, so the canonical name cannot be spelled there.
	// The identifier is still one; only the spelling differs, which is what §3
	// of the record allows.
	rune_name: "modulo",
	arity: ScalarFunctionArity::Exact(2),
	signature: ScalarFunctionSignature::BinaryIntegerPreserving,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	deterministic: true,
	pbc_pushdown_eligible: false,
	evaluator: ScalarFunctionEvaluator::Mod,
};

const UPPER: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Upper,
	sql_name: "UPPER",
	sql_aliases: &[],
	rune_name: "upper",
	arity: ScalarFunctionArity::Exact(1),
	signature: ScalarFunctionSignature::UnaryStringPreserving,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	deterministic: true,
	pbc_pushdown_eligible: false,
	evaluator: ScalarFunctionEvaluator::Upper,
};

const LOWER: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Lower,
	sql_name: "LOWER",
	sql_aliases: &[],
	rune_name: "lower",
	arity: ScalarFunctionArity::Exact(1),
	signature: ScalarFunctionSignature::UnaryStringPreserving,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	deterministic: true,
	pbc_pushdown_eligible: false,
	evaluator: ScalarFunctionEvaluator::Lower,
};

const TRIM: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Trim,
	sql_name: "TRIM",
	sql_aliases: &[],
	rune_name: "trim",
	arity: ScalarFunctionArity::Exact(1),
	signature: ScalarFunctionSignature::UnaryStringPreserving,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	deterministic: true,
	pbc_pushdown_eligible: false,
	evaluator: ScalarFunctionEvaluator::Trim,
};

const CHAR_LENGTH: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::CharLength,
	sql_name: "CHAR_LENGTH",
	// The standard admits both spellings for the same function.
	sql_aliases: &["CHARACTER_LENGTH"],
	rune_name: "char_length",
	arity: ScalarFunctionArity::Exact(1),
	signature: ScalarFunctionSignature::StringToInteger,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	deterministic: true,
	pbc_pushdown_eligible: false,
	evaluator: ScalarFunctionEvaluator::CharLength,
};

const WIDTH_BUCKET: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::WidthBucket,
	sql_name: "WIDTH_BUCKET",
	sql_aliases: &[],
	rune_name: "width_bucket",
	arity: ScalarFunctionArity::Exact(4),
	signature: ScalarFunctionSignature::NumericBucketing,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	deterministic: true,
	pbc_pushdown_eligible: false,
	evaluator: ScalarFunctionEvaluator::WidthBucket,
};

/// The complete scalar-function registry.
pub const SCALAR_FUNCTIONS: &[ScalarFunctionMetadata] = &[
	ABS,
	CEIL,
	CHAR_LENGTH,
	EXP,
	FLOOR,
	LN,
	LOWER,
	MOD,
	POW,
	ROUND,
	SIGN,
	SQRT,
	TRIM,
	UPPER,
	WIDTH_BUCKET,
];

pub fn scalar_function_metadata(id: ScalarFunctionId) -> &'static ScalarFunctionMetadata {
	SCALAR_FUNCTIONS
		.iter()
		.find(|metadata| metadata.id == id)
		.expect("every ScalarFunctionId must have registry metadata")
}

pub fn scalar_function_from_sql_name(name: &str) -> Option<ScalarFunctionId> {
	SCALAR_FUNCTIONS
		.iter()
		.find(|metadata| {
			metadata.sql_name.eq_ignore_ascii_case(name)
				|| metadata
					.sql_aliases
					.iter()
					.any(|alias| alias.eq_ignore_ascii_case(name))
		})
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
			// The two surfaces share one identifier, not necessarily one
			// spelling. A divergence is allowed only where the canonical name
			// is unusable, and it is listed here so it stays a decision.
			const SPELLING_EXCEPTIONS: &[(&str, &str)] = &[("MOD", "modulo")];
			let expected = SPELLING_EXCEPTIONS
				.iter()
				.find(|(sql, _)| *sql == metadata.sql_name)
				.map_or_else(
					|| metadata.sql_name.to_ascii_lowercase(),
					|(_, rune)| (*rune).to_string(),
				);
			assert_eq!(
				metadata.rune_name, expected,
				"{} must use its canonical Rune spelling",
				metadata.sql_name
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

	/// An alias that collides with another function's name would silently
	/// resolve to whichever registration the scan reached first.
	#[test]
	fn every_sql_spelling_resolves_to_exactly_one_function() {
		let mut spellings = std::collections::HashMap::new();
		for metadata in SCALAR_FUNCTIONS {
			for spelling in std::iter::once(&metadata.sql_name).chain(metadata.sql_aliases) {
				let upper = spelling.to_ascii_uppercase();
				assert!(
					spellings.insert(upper.clone(), metadata.id).is_none(),
					"{upper} is claimed by more than one registration"
				);
				assert_eq!(
					scalar_function_from_sql_name(&upper.to_ascii_lowercase()),
					Some(metadata.id),
					"{upper} must resolve case-insensitively to its own function"
				);
			}
		}
	}

	/// The vendor spelling this engine shipped first still resolves, and the
	/// ANSI spelling is the canonical one.
	#[test]
	fn power_is_canonical_and_pow_remains_an_alias() {
		let metadata = scalar_function_metadata(ScalarFunctionId::Pow);
		assert_eq!(metadata.sql_name, "POWER");
		assert_eq!(metadata.sql_aliases, &["POW"]);
		assert_eq!(
			scalar_function_from_sql_name("pow"),
			Some(ScalarFunctionId::Pow)
		);
		assert_eq!(
			scalar_function_from_sql_name("POWER"),
			Some(ScalarFunctionId::Pow)
		);
	}

	#[test]
	fn ceiling_is_accepted_alongside_ceil() {
		assert_eq!(
			scalar_function_from_sql_name("CEILING"),
			Some(ScalarFunctionId::Ceil)
		);
		assert_eq!(
			scalar_function_from_sql_name("ceil"),
			Some(ScalarFunctionId::Ceil)
		);
	}

	#[test]
	fn pow_is_the_binary_arity_case() {
		let metadata = scalar_function_metadata(ScalarFunctionId::Pow);
		assert!(!metadata.arity.accepts(1));
		assert!(metadata.arity.accepts(2));
		assert_eq!(metadata.arity.description(), "exactly two arguments");
	}

	#[test]
	fn floor_preserves_its_narrow_domain_and_rejects_non_finite_values() {
		let metadata = scalar_function_metadata(ScalarFunctionId::Floor);
		assert_eq!(
			metadata.signature,
			ScalarFunctionSignature::UnaryIntegerOrFloatPreserving
		);
		assert_eq!(
			metadata.non_finite_behavior,
			ScalarFunctionNonFiniteBehavior::Rejects
		);
	}
}
