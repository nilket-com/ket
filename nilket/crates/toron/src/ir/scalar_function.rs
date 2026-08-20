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
	AtTimeZone,
	Ceil,
	CharLength,
	Concat,
	DateTrunc,
	Exp,
	Extract,
	Floor,
	Greatest,
	Initcap,
	Least,
	Ln,
	Log,
	Lower,
	Ltrim,
	Lpad,
	Mod,
	CurrentDate,
	Now,
	Position,
	Pow,
	Random,
	Replace,
	Reverse,
	Rpad,
	Round,
	Rtrim,
	Sign,
	Sqrt,
	Strpos,
	StringConcat,
	Substring,
	SplitPart,
	TimeBucket,
	ToChar,
	ToDate,
	ToTimestamp,
	Trim,
	TryToDate,
	TryToTimestamp,
	Trunc,
	Upper,
	WidthBucket,
	OctetLength,
	Overlay,
	Translate,
}

/// Non-child data carried by a scalar-function node.
///
/// `None` is explicit rather than represented by `Option`: absence of a
/// function parameter is a distinct node shape from a parameter whose own
/// fields happen to be optional (for example, an unbound temporal operand has
/// not resolved its zone yet).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ScalarFunctionParameter {
	None,
	Extract(Box<ExtractScalarFunctionParameter>),
	DateTrunc(Box<DateTruncScalarFunctionParameter>),
	Trim(Box<TrimScalarFunctionParameter>),
	CurrentTimestamp {
		timestamp_nanos: i64,
	},
	CurrentDate {
		days: i32,
	},
	/// The session zone a row-wise timestamp parse resolves zone-less text
	/// against, captured at bind time so every fragment parses identically.
	ToTimestamp {
		zone: Box<str>,
	},
	/// A pattern compiled at bind time, shared by rendering and parsing. The
	/// zone is absent when no zone participates, as for a `Date32` argument.
	TemporalFormat {
		pattern: Box<TemporalFormatPattern>,
		zone: Option<Box<str>>,
	},
}

/// One field of a temporal format pattern.
///
/// The set is closed. A token means the same thing in both directions — what
/// `YYYY` writes, `YYYY` reads — which is the property that keeps rendering and
/// parsing from drifting apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TemporalFormatToken {
	Year4,
	Month2,
	Day2,
	Hour24,
	Minute2,
	Second2,
	Fraction9,
}

impl TemporalFormatToken {
	/// The pattern spelling, and the only place it is written down.
	pub const fn spelling(self) -> &'static str {
		match self {
			Self::Year4 => "YYYY",
			Self::Month2 => "MM",
			Self::Day2 => "DD",
			Self::Hour24 => "HH24",
			Self::Minute2 => "MI",
			Self::Second2 => "SS",
			Self::Fraction9 => "FF9",
		}
	}

	/// Whether the token names a time-of-day field rather than a calendar field.
	pub const fn is_time_field(self) -> bool {
		match self {
			Self::Hour24 | Self::Minute2 | Self::Second2 | Self::Fraction9 => true,
			Self::Year4 | Self::Month2 | Self::Day2 => false,
		}
	}

	/// Every token, longest spelling first so a compiler scanning left to right
	/// matches `HH24` before it could match a shorter prefix.
	pub const ALL: &'static [Self] = &[
		Self::Hour24,
		Self::Year4,
		Self::Fraction9,
		Self::Month2,
		Self::Day2,
		Self::Minute2,
		Self::Second2,
	];
}

/// A compiled pattern piece: a field to substitute, or text to emit and match
/// verbatim.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TemporalFormatPiece {
	Token(TemporalFormatToken),
	Literal(Box<str>),
}

/// A pattern compiled once at bind time and carried in the scalar parameter.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TemporalFormatPattern {
	pub pieces: Vec<TemporalFormatPiece>,
}

impl TemporalFormatPattern {
	pub fn tokens(&self) -> impl Iterator<Item = TemporalFormatToken> + '_ {
		self.pieces.iter().filter_map(|piece| match piece {
			TemporalFormatPiece::Token(token) => Some(*token),
			TemporalFormatPiece::Literal(_) => None,
		})
	}

	/// Whether the pattern determines a calendar date, which parsing requires
	/// and rendering does not.
	pub fn has_date_fields(&self) -> bool {
		let mut year = false;
		let mut month = false;
		let mut day = false;
		for token in self.tokens() {
			match token {
				TemporalFormatToken::Year4 => year = true,
				TemporalFormatToken::Month2 => month = true,
				TemporalFormatToken::Day2 => day = true,
				_ => {}
			}
		}
		year && month && day
	}

	pub fn has_time_fields(&self) -> bool {
		self.tokens().any(TemporalFormatToken::is_time_field)
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExtractScalarFunctionParameter {
	pub field: crate::ir::TemporalExtractField,
	pub zone: Option<Box<str>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DateTruncScalarFunctionParameter {
	pub field: crate::ir::TemporalTruncField,
	pub zone: Option<Box<str>>,
}

/// Which edge(s) a specification-form `TRIM` removes characters from.
///
/// The trim character set remains an ordinary child expression so it can be
/// constant or row-dependent; this payload carries only the non-child grammar
/// choice that would otherwise be lost between planning and execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TrimWhere {
	Both,
	Leading,
	Trailing,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TrimScalarFunctionParameter {
	pub where_field: TrimWhere,
}

/// The accepted argument count for a scalar function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarFunctionArity {
	Exact(usize),
	/// An inclusive range, for a function whose trailing arguments are optional.
	Between(usize, usize),
	/// An open-ended minimum, for a genuinely variadic function.
	AtLeast(usize),
}

impl ScalarFunctionArity {
	pub const fn accepts(self, count: usize) -> bool {
		match self {
			Self::Exact(expected) => count == expected,
			Self::Between(min, max) => min <= count && count <= max,
			Self::AtLeast(min) => min <= count,
		}
	}

	pub const fn description(self) -> &'static str {
		match self {
			Self::Exact(0) => "no arguments",
			Self::Exact(1) => "exactly one argument",
			Self::Exact(2) => "exactly two arguments",
			Self::Exact(4) => "exactly four arguments",
			Self::Exact(_) => "the registered argument count",
			Self::Between(1, 2) => "one or two arguments",
			Self::Between(_, _) => "the registered argument range",
			Self::AtLeast(2) => "at least two arguments",
			Self::AtLeast(_) => "the registered minimum argument count",
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
	TemporalExtract,
	TemporalTrunc,
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
	/// A string, integer target length, and optional same-typed fill string.
	StringWithIntegerAndOptionalString,
	/// Two same-typed strings and an integer part number, producing the string
	/// part at that 1-based position.
	StringStringInteger,
	/// `WIDTH_BUCKET(operand, low, high, count)`: three same-typed floats and an
	/// integer bucket count, producing a bucket ordinal. The count is a shape
	/// parameter rather than a value, so it does not participate in the output
	/// type any more than ROUND's digit count does.
	NumericBucketing,
	/// Every argument shares one comparable type, which is also the output. The
	/// comparison is defined for the engine's ordered value types; temporal
	/// forms are a separate admission because they carry a unit and a zone.
	VariadicOrderedSameType,
	/// No arguments at all, producing a float. The row count comes from the
	/// batch rather than from an operand, which is what makes a niladic
	/// function different in kind from every rule above.
	NiladicFloat,
	/// No arguments at all, producing the execution's one statement timestamp.
	NiladicTimestamp,
	/// No arguments at all, producing the statement's session-zone calendar date.
	NiladicDate,
	/// A string parsed per row into a UTC instant. Zone-less text resolves
	/// against the session zone the binder captured into the node's parameter,
	/// so the evaluator never consults planner configuration.
	StringToTimestamp,
	/// A string parsed per row into a calendar date. No zone participates: a
	/// date is zone-independent, and resolving one against a session zone would
	/// make the same text parse differently in different sessions.
	StringToDate,
	/// A timestamp or date plus a constant format pattern, producing a string.
	/// The pattern is a constant argument position: it must bind to a string
	/// literal, which the binder compiles once rather than per row.
	TemporalWithConstantFormat,
	/// A `Utf8` string, a 1-based start, and an optional length. The bounds are
	/// **constant** argument positions: they must bind to integer literals, and
	/// the binder reads their values rather than evaluating them per row.
	///
	/// This is the registry's answer to a non-expression parameter. Rather than
	/// widening `ScalarFunction` with a second payload, the parameter travels as
	/// an ordinary literal argument that a signature rule validates. Every
	/// remaining grandfathered variant carries a constant of some kind, so this
	/// choice is the one that decides whether they can migrate at all.
	StringWithConstantBounds,
	/// ADR 567: `OVERLAY(x PLACING y FROM i [FOR n])` — two `Utf8` strings and
	/// SUBSTRING's constant bounds. `FROM`/`FOR` are constant argument
	/// positions validated exactly as `StringWithConstantBounds` validates
	/// them; the second string is the replacement, which shares the input's
	/// type.
	StringPairWithConstantBounds,
	/// A constant interval followed by `Int64` Unix milliseconds, producing
	/// `Int64`. The interval is a constant argument like SUBSTRING's bounds but
	/// carries a value rather than a count, which is what this registration
	/// proves the model handles.
	///
	/// The input is deliberately not a timestamp type: `TIME_BUCKET` has always
	/// refused those, and a named logical type over `Int64` is refused too
	/// because its units are not Unix milliseconds. The output type does not
	/// depend on the constant's value — `AT TIME ZONE` is the case that would,
	/// and it is a separate decision.
	Int64MillisWithConstantInterval,
	/// A timestamp and a constant IANA zone, producing a timestamp carrying that
	/// zone. This is the first rule whose **output type depends on a constant's
	/// value** rather than only on argument types — the zone string becomes part
	/// of `DataType::Timestamp`.
	TimestampWithConstantZone,
	/// One or more strings, producing the first string's type. Unlike the
	/// ordered variadic rule, this is a concatenation operation and does not
	/// require the strings to be comparable beyond sharing one string type.
	VariadicStringConcatenating,
	/// Two same-typed strings in, a 1-based character position out.
	BinaryStringToInteger,
	/// Three same-typed strings in, with the first string's type as output.
	TernaryStringPreserving,
	/// One or two strings, producing the first string's type. The optional second
	/// string is TRIM's character set; it is an expression rather than a
	/// parameter because SQL permits it to vary by row.
	StringWithOptionalString,
}

/// How NULL inputs affect the output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarFunctionNullBehavior {
	Propagates,
	Ignores,
}

/// What a function does when an individual input value cannot be processed.
///
/// This is deliberately orthogonal to [`ScalarFunctionNullBehavior`]: that
/// property answers what a NULL *input* does, while this one answers whether a
/// non-NULL input the function cannot handle becomes an error or a NULL. Folding
/// the two together would distort `Propagates` and `Ignores` for every function
/// that cannot fail at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarFunctionFailure {
	/// The function either cannot fail, or surfaces failure as an execution
	/// error that ends the statement. Its nullability follows
	/// [`ScalarFunctionNullBehavior`] alone.
	Raises,
	/// A value the function cannot process yields NULL instead of an error, so
	/// the output is nullable regardless of whether any input is.
	YieldsNull,
}

/// How non-finite floating inputs affect the output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarFunctionNonFiniteBehavior {
	Propagates,
	/// Reject NaN and infinity when evaluation reaches a floating value.
	/// This is intentionally evaluation-time behavior, not binder validation.
	Rejects,
}

/// How often a scalar function is allowed to change its result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarFunctionEvaluation {
	Deterministic,
	PerCall,
	PerStatement,
}

/// The only function-specific choice the executor makes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarFunctionEvaluator {
	Abs,
	AtTimeZone,
	Ceil,
	CharLength,
	Concat,
	DateTrunc,
	Exp,
	Extract,
	Floor,
	Greatest,
	Initcap,
	Least,
	Ln,
	Log,
	Lpad,
	Lower,
	Position,
	Mod,
	CurrentDate,
	Now,
	Pow,
	Random,
	Replace,
	Reverse,
	Rpad,
	Round,
	Sign,
	Sqrt,
	SplitPart,
	StringConcat,
	Substring,
	TimeBucket,
	ToChar,
	ToDate,
	ToTimestamp,
	Trim,
	Trunc,
	Upper,
	WidthBucket,
	OctetLength,
	Overlay,
	Translate,
}

/// Planner and execution metadata for one scalar capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScalarFunctionMetadata {
	pub id: ScalarFunctionId,
	/// The ANSI spelling where the standard has one. Vendor spellings belong in
	/// [`ScalarFunctionMetadata::sql_aliases`], never here.
	pub sql_name: &'static str,
	/// Additional accepted SQL spellings. Aliases must preserve the canonical
	/// function's argument order; a spelling with a different calling convention
	/// gets its own registration and identifier. The standard admits two names
	/// for the ceiling function, and a vendor spelling already in use has to keep
	/// resolving once the canonical name is adopted.
	pub sql_aliases: &'static [&'static str],
	pub rune_name: &'static str,
	pub arity: ScalarFunctionArity,
	pub signature: ScalarFunctionSignature,
	pub null_behavior: ScalarFunctionNullBehavior,
	/// Whether an unprocessable value becomes an error or a NULL. Read by
	/// bound-program nullability before the null-behavior derivation, so a
	/// `YieldsNull` function is nullable even over non-nullable inputs.
	pub failure: ScalarFunctionFailure,
	pub non_finite_behavior: ScalarFunctionNonFiniteBehavior,
	pub evaluation: ScalarFunctionEvaluation,
	pub pbc_pushdown_eligible: bool,
	/// Whether guard-filter pushdown may recurse through this function's operands.
	/// This is deliberately independent from PBC eligibility: a function can be
	/// safe for outer-join guard demotion without being representable in PBC.
	pub guard_filter_pushdown_safe: bool,
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
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
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
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
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
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
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
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
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
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
	evaluator: ScalarFunctionEvaluator::Ln,
};

const LOG: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Log,
	sql_name: "LOG",
	sql_aliases: &[],
	rune_name: "log",
	// This first cut admits LOG(base, value). The common one-argument
	// LOG(value) form means base 10 and is a separate semantics decision; it
	// must not be silently treated as LN(value).
	arity: ScalarFunctionArity::Exact(2),
	signature: ScalarFunctionSignature::BinaryFloatPreserving,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
	evaluator: ScalarFunctionEvaluator::Log,
};

const ROUND: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Round,
	sql_name: "ROUND",
	sql_aliases: &[],
	rune_name: "round",
	arity: ScalarFunctionArity::Between(1, 2),
	signature: ScalarFunctionSignature::FloatWithOptionalIntegerDigits,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
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
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
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
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Rejects,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
	evaluator: ScalarFunctionEvaluator::Floor,
};

const TRUNC: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Trunc,
	sql_name: "TRUNC",
	sql_aliases: &[],
	rune_name: "trunc",
	// The decimal-place form is deliberately deferred; this registration is
	// the one-argument truncation-toward-zero operation.
	arity: ScalarFunctionArity::Exact(1),
	signature: ScalarFunctionSignature::UnaryIntegerOrFloatPreserving,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
	evaluator: ScalarFunctionEvaluator::Trunc,
};

const SIGN: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Sign,
	sql_name: "SIGN",
	sql_aliases: &[],
	rune_name: "sign",
	arity: ScalarFunctionArity::Exact(1),
	signature: ScalarFunctionSignature::UnaryNumericPreserving,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
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
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
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
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
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
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
	evaluator: ScalarFunctionEvaluator::Lower,
};

const INITCAP: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Initcap,
	sql_name: "INITCAP",
	sql_aliases: &[],
	rune_name: "initcap",
	arity: ScalarFunctionArity::Exact(1),
	signature: ScalarFunctionSignature::UnaryStringPreserving,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
	evaluator: ScalarFunctionEvaluator::Initcap,
};

const REVERSE: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Reverse,
	sql_name: "REVERSE",
	sql_aliases: &[],
	rune_name: "reverse",
	arity: ScalarFunctionArity::Exact(1),
	signature: ScalarFunctionSignature::UnaryStringPreserving,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
	evaluator: ScalarFunctionEvaluator::Reverse,
};

const TRIM: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Trim,
	sql_name: "TRIM",
	sql_aliases: &[],
	rune_name: "trim",
	arity: ScalarFunctionArity::Between(1, 2),
	signature: ScalarFunctionSignature::StringWithOptionalString,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
	evaluator: ScalarFunctionEvaluator::Trim,
};

const CONCAT: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Concat,
	sql_name: "CONCAT",
	// CONCAT intentionally is not the registry identity for SQL `||`: CONCAT
	// ignores NULL operands, while `||` propagates NULL.
	sql_aliases: &[],
	rune_name: "concat",
	arity: ScalarFunctionArity::AtLeast(1),
	signature: ScalarFunctionSignature::VariadicStringConcatenating,
	null_behavior: ScalarFunctionNullBehavior::Ignores,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
	evaluator: ScalarFunctionEvaluator::Concat,
};

const STRING_CONCAT: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::StringConcat,
	sql_name: "||",
	// `||` is the strict, NULL-propagating counterpart to CONCAT. It is an
	// operator rather than a function-call spelling, so its Rune name is the
	// explicit semantic name instead of reusing `concat`.
	sql_aliases: &[],
	rune_name: "concat_strict",
	arity: ScalarFunctionArity::Exact(2),
	signature: ScalarFunctionSignature::VariadicStringConcatenating,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
	evaluator: ScalarFunctionEvaluator::StringConcat,
};

const LTRIM: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Ltrim,
	sql_name: "LTRIM",
	sql_aliases: &[],
	rune_name: "ltrim",
	arity: ScalarFunctionArity::Between(1, 2),
	signature: ScalarFunctionSignature::StringWithOptionalString,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
	evaluator: ScalarFunctionEvaluator::Trim,
};

const LPAD: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Lpad,
	sql_name: "LPAD",
	sql_aliases: &[],
	rune_name: "lpad",
	arity: ScalarFunctionArity::Between(2, 3),
	signature: ScalarFunctionSignature::StringWithIntegerAndOptionalString,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
	evaluator: ScalarFunctionEvaluator::Lpad,
};

const POSITION: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Position,
	sql_name: "POSITION",
	// STRPOS has the same kernel but reverses the two argument positions, so it
	// is a separate registration rather than an alias.
	sql_aliases: &[],
	rune_name: "position",
	arity: ScalarFunctionArity::Exact(2),
	signature: ScalarFunctionSignature::BinaryStringToInteger,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
	evaluator: ScalarFunctionEvaluator::Position,
};

const STRPOS: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Strpos,
	sql_name: "STRPOS",
	sql_aliases: &[],
	rune_name: "strpos",
	arity: ScalarFunctionArity::Exact(2),
	signature: ScalarFunctionSignature::BinaryStringToInteger,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
	evaluator: ScalarFunctionEvaluator::Position,
};

const REPLACE: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Replace,
	sql_name: "REPLACE",
	sql_aliases: &[],
	rune_name: "replace",
	arity: ScalarFunctionArity::Exact(3),
	signature: ScalarFunctionSignature::TernaryStringPreserving,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
	evaluator: ScalarFunctionEvaluator::Replace,
};

const RTRIM: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Rtrim,
	sql_name: "RTRIM",
	sql_aliases: &[],
	rune_name: "rtrim",
	arity: ScalarFunctionArity::Between(1, 2),
	signature: ScalarFunctionSignature::StringWithOptionalString,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
	evaluator: ScalarFunctionEvaluator::Trim,
};

const RPAD: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Rpad,
	sql_name: "RPAD",
	sql_aliases: &[],
	rune_name: "rpad",
	arity: ScalarFunctionArity::Between(2, 3),
	signature: ScalarFunctionSignature::StringWithIntegerAndOptionalString,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
	evaluator: ScalarFunctionEvaluator::Rpad,
};

const CHAR_LENGTH: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::CharLength,
	sql_name: "CHAR_LENGTH",
	// The standard admits both spellings for the same function.
	sql_aliases: &["CHARACTER_LENGTH", "LENGTH"],
	rune_name: "char_length",
	arity: ScalarFunctionArity::Exact(1),
	signature: ScalarFunctionSignature::StringToInteger,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
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
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
	evaluator: ScalarFunctionEvaluator::WidthBucket,
};

const GREATEST: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Greatest,
	sql_name: "GREATEST",
	sql_aliases: &[],
	rune_name: "greatest",
	arity: ScalarFunctionArity::AtLeast(2),
	signature: ScalarFunctionSignature::VariadicOrderedSameType,
	// Not ANSI, so there is no canonical NULL rule to inherit. Oracle, MySQL,
	// and SQLite all propagate; PostgreSQL skips NULLs and documents that as a
	// deviation. Propagating is the compatibility default, and skipping would
	// be its own explicit PostgreSQL-compatibility decision.
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
	evaluator: ScalarFunctionEvaluator::Greatest,
};

const LEAST: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Least,
	sql_name: "LEAST",
	sql_aliases: &[],
	rune_name: "least",
	arity: ScalarFunctionArity::AtLeast(2),
	signature: ScalarFunctionSignature::VariadicOrderedSameType,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
	evaluator: ScalarFunctionEvaluator::Least,
};

const RANDOM: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Random,
	// Not ANSI. `RANDOM` is a vendor spelling admitted here as the reference
	// non-deterministic function, not as a standards-completeness claim.
	sql_name: "RANDOM",
	sql_aliases: &[],
	rune_name: "random",
	arity: ScalarFunctionArity::Exact(0),
	signature: ScalarFunctionSignature::NiladicFloat,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	// The first `false` in the registry. Five planner rules already read this
	// through `Expr::is_deterministic`; until now none of them had a case that
	// exercised the declining branch.
	evaluation: ScalarFunctionEvaluation::PerCall,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
	evaluator: ScalarFunctionEvaluator::Random,
};

const NOW: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Now,
	sql_name: "NOW",
	// CURRENT_TIMESTAMP is the SQL value-function spelling of the same
	// statement-stable instant. It is an alias, not a second clock source.
	sql_aliases: &["CURRENT_TIMESTAMP"],
	rune_name: "now",
	arity: ScalarFunctionArity::Exact(0),
	signature: ScalarFunctionSignature::NiladicTimestamp,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::PerStatement,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
	evaluator: ScalarFunctionEvaluator::Now,
};

const CURRENT_DATE: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::CurrentDate,
	sql_name: "CURRENT_DATE",
	sql_aliases: &[],
	rune_name: "current_date",
	arity: ScalarFunctionArity::Exact(0),
	signature: ScalarFunctionSignature::NiladicDate,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::PerStatement,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
	evaluator: ScalarFunctionEvaluator::CurrentDate,
};

const SUBSTRING: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Substring,
	sql_name: "SUBSTRING",
	sql_aliases: &[],
	rune_name: "substring",
	arity: ScalarFunctionArity::Between(2, 3),
	signature: ScalarFunctionSignature::StringWithConstantBounds,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: true,
	evaluator: ScalarFunctionEvaluator::Substring,
};

const SPLIT_PART: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::SplitPart,
	sql_name: "SPLIT_PART",
	sql_aliases: &[],
	rune_name: "split_part",
	arity: ScalarFunctionArity::Exact(3),
	signature: ScalarFunctionSignature::StringStringInteger,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
	evaluator: ScalarFunctionEvaluator::SplitPart,
};

const OCTET_LENGTH: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::OctetLength,
	// ADR 567: the UTF-8 byte count, versus CHAR_LENGTH's character count.
	sql_name: "OCTET_LENGTH",
	sql_aliases: &[],
	rune_name: "octet_length",
	arity: ScalarFunctionArity::Exact(1),
	signature: ScalarFunctionSignature::StringToInteger,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
	evaluator: ScalarFunctionEvaluator::OctetLength,
};

const OVERLAY: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Overlay,
	// ADR 567: the standard's defining identity —
	// SUBSTRING(x FROM 1 FOR i-1) || y || SUBSTRING(x FROM i+n) with n
	// defaulting to CHAR_LENGTH(y) — character-positioned.
	sql_name: "OVERLAY",
	sql_aliases: &[],
	rune_name: "overlay",
	arity: ScalarFunctionArity::Between(3, 4),
	signature: ScalarFunctionSignature::StringPairWithConstantBounds,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
	evaluator: ScalarFunctionEvaluator::Overlay,
};

const TRANSLATE: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Translate,
	// ADR 567: the three-argument character map — the de-facto convention.
	// The standard's `TRANSLATE ... USING` names a translation object this
	// engine does not have and stays rejected rather than reinterpreted.
	sql_name: "TRANSLATE",
	sql_aliases: &[],
	rune_name: "translate",
	arity: ScalarFunctionArity::Exact(3),
	signature: ScalarFunctionSignature::TernaryStringPreserving,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
	evaluator: ScalarFunctionEvaluator::Translate,
};

const TO_TIMESTAMP: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::ToTimestamp,
	sql_name: "TO_TIMESTAMP",
	sql_aliases: &[],
	rune_name: "to_timestamp",
	arity: ScalarFunctionArity::Between(1, 2),
	signature: ScalarFunctionSignature::StringToTimestamp,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
	evaluator: ScalarFunctionEvaluator::ToTimestamp,
};

const TRY_TO_TIMESTAMP: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::TryToTimestamp,
	sql_name: "TRY_TO_TIMESTAMP",
	sql_aliases: &[],
	rune_name: "try_to_timestamp",
	arity: ScalarFunctionArity::Between(1, 2),
	signature: ScalarFunctionSignature::StringToTimestamp,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::YieldsNull,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
	evaluator: ScalarFunctionEvaluator::ToTimestamp,
};

const TO_CHAR: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::ToChar,
	sql_name: "TO_CHAR",
	sql_aliases: &[],
	rune_name: "to_char",
	arity: ScalarFunctionArity::Exact(2),
	signature: ScalarFunctionSignature::TemporalWithConstantFormat,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
	evaluator: ScalarFunctionEvaluator::ToChar,
};

const TO_DATE: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::ToDate,
	sql_name: "TO_DATE",
	sql_aliases: &[],
	rune_name: "to_date",
	arity: ScalarFunctionArity::Exact(1),
	signature: ScalarFunctionSignature::StringToDate,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
	evaluator: ScalarFunctionEvaluator::ToDate,
};

const TRY_TO_DATE: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::TryToDate,
	sql_name: "TRY_TO_DATE",
	sql_aliases: &[],
	rune_name: "try_to_date",
	arity: ScalarFunctionArity::Exact(1),
	signature: ScalarFunctionSignature::StringToDate,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::YieldsNull,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: false,
	evaluator: ScalarFunctionEvaluator::ToDate,
};

const TIME_BUCKET: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::TimeBucket,
	sql_name: "TIME_BUCKET",
	sql_aliases: &[],
	rune_name: "time_bucket",
	arity: ScalarFunctionArity::Exact(2),
	signature: ScalarFunctionSignature::Int64MillisWithConstantInterval,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: true,
	evaluator: ScalarFunctionEvaluator::TimeBucket,
};

const AT_TIME_ZONE: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::AtTimeZone,
	sql_name: "AT_TIME_ZONE",
	sql_aliases: &[],
	rune_name: "at_time_zone",
	arity: ScalarFunctionArity::Exact(2),
	signature: ScalarFunctionSignature::TimestampWithConstantZone,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: true,
	evaluator: ScalarFunctionEvaluator::AtTimeZone,
};

const DATE_TRUNC: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::DateTrunc,
	sql_name: "DATE_TRUNC",
	sql_aliases: &[],
	rune_name: "date_trunc",
	arity: ScalarFunctionArity::Exact(1),
	signature: ScalarFunctionSignature::TemporalTrunc,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: true,
	evaluator: ScalarFunctionEvaluator::DateTrunc,
};

const EXTRACT: ScalarFunctionMetadata = ScalarFunctionMetadata {
	id: ScalarFunctionId::Extract,
	sql_name: "EXTRACT",
	sql_aliases: &[],
	rune_name: "extract",
	arity: ScalarFunctionArity::Exact(1),
	signature: ScalarFunctionSignature::TemporalExtract,
	null_behavior: ScalarFunctionNullBehavior::Propagates,
	failure: ScalarFunctionFailure::Raises,
	non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates,
	evaluation: ScalarFunctionEvaluation::Deterministic,
	pbc_pushdown_eligible: false,
	guard_filter_pushdown_safe: true,
	evaluator: ScalarFunctionEvaluator::Extract,
};

/// The complete scalar-function registry.
pub const SCALAR_FUNCTIONS: &[ScalarFunctionMetadata] = &[
	ABS,
	AT_TIME_ZONE,
	CEIL,
	CHAR_LENGTH,
	CONCAT,
	STRING_CONCAT,
	DATE_TRUNC,
	EXP,
	EXTRACT,
	FLOOR,
	GREATEST,
	INITCAP,
	LEAST,
	LN,
	LOG,
	LPAD,
	LOWER,
	LTRIM,
	MOD,
	CURRENT_DATE,
	NOW,
	POSITION,
	POW,
	RANDOM,
	REPLACE,
	REVERSE,
	RPAD,
	ROUND,
	RTRIM,
	SIGN,
	SQRT,
	STRPOS,
	SUBSTRING,
	TIME_BUCKET,
	TO_CHAR,
	TO_DATE,
	TO_TIMESTAMP,
	TRIM,
	TRUNC,
	TRY_TO_DATE,
	TRY_TO_TIMESTAMP,
	UPPER,
	WIDTH_BUCKET,
	SPLIT_PART,
	OCTET_LENGTH,
	OVERLAY,
	TRANSLATE,
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
			const SPELLING_EXCEPTIONS: &[(&str, &str)] =
				&[("MOD", "modulo"), ("||", "concat_strict")];
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
	fn current_timestamp_is_a_statement_stable_alias_of_now() {
		let metadata = scalar_function_metadata(ScalarFunctionId::Now);
		assert_eq!(metadata.sql_aliases, &["CURRENT_TIMESTAMP"]);
		assert_eq!(metadata.evaluation, ScalarFunctionEvaluation::PerStatement);
		assert_eq!(
			scalar_function_from_sql_name("current_timestamp"),
			Some(ScalarFunctionId::Now)
		);
	}

	#[test]
	fn current_date_is_a_statement_stable_date_registration() {
		let metadata = scalar_function_metadata(ScalarFunctionId::CurrentDate);
		assert_eq!(metadata.sql_name, "CURRENT_DATE");
		assert_eq!(metadata.rune_name, "current_date");
		assert_eq!(metadata.signature, ScalarFunctionSignature::NiladicDate);
		assert_eq!(metadata.evaluation, ScalarFunctionEvaluation::PerStatement);
		assert_eq!(
			scalar_function_from_sql_name("current_date"),
			Some(ScalarFunctionId::CurrentDate)
		);
	}

	#[test]
	fn strpos_is_a_distinct_registration_with_the_position_evaluator() {
		assert_eq!(
			scalar_function_from_sql_name("STRPOS"),
			Some(ScalarFunctionId::Strpos)
		);
		assert_eq!(
			scalar_function_metadata(ScalarFunctionId::Strpos).evaluator,
			ScalarFunctionEvaluator::Position
		);
		assert!(
			scalar_function_metadata(ScalarFunctionId::Strpos)
				.sql_aliases
				.is_empty()
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
