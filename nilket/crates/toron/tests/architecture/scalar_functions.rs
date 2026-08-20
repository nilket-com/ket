//! ADR 553 — typed scalar-function registry guards.

use std::collections::BTreeSet;

use super::support::*;

// EXTRACT and DATE_TRUNC were the final grandfathered variants before ADR 553;
// both now use the generic Expr::ScalarFunction representation.
const GRANDFATHERED_SCALAR_VARIANTS: &[&str] = &[];

// Every other Expr form is explicitly classified here. A new variant must be
// classified in review; it cannot silently enlarge the scalar-function set.
const NON_SCALAR_EXPR_VARIANTS: &[&str] = &[
	"Column",
	"Literal",
	"UntypedNull",
	"LogicalAscription",
	"SqlLogicalCast",
	"SqlConvert",
	"Cast",
	"TryCast",
	"Add",
	"Sub",
	"Mul",
	"Div",
	"Eq",
	"Neq",
	"Lt",
	"Lte",
	"Gt",
	"Gte",
	"And",
	"Or",
	"Not",
	"InList",
	"Like",
	"Case",
	"IsNull",
	"IsNotNull",
	// ADR 566: a predicate operator like Eq or IsNull — grammar, not a
	// name-resolved scalar call, so it is not a registry candidate.
	"IsDistinctFrom",
];

fn source(relative: &str) -> String {
	std::fs::read_to_string(manifest_dir().join(relative)).expect("read Toron source")
}

fn enum_variant_names(enum_body: &str) -> BTreeSet<String> {
	enum_body
		.lines()
		.filter_map(|line| {
			let trimmed = line.trim_start();
			trimmed
				.chars()
				.next()
				.filter(|first| first.is_ascii_uppercase())?;
			let name = trimmed
				.chars()
				.take_while(|character| character.is_ascii_alphanumeric() || *character == '_')
				.collect::<String>();
			(!name.is_empty()).then_some(name)
		})
		.collect()
}

/// A registration's const is the SCREAMING_SNAKE_CASE of its identifier, so
/// `CharLength` is `CHAR_LENGTH`. Uppercasing alone silently looks for
/// `CHARLENGTH` and reports a missing registration that is really present.
fn const_name(id: &str) -> String {
	let mut name = String::new();
	for (index, character) in id.char_indices() {
		if character.is_ascii_uppercase() && index > 0 {
			name.push('_');
		}
		name.push(character.to_ascii_uppercase());
	}
	name
}

/// The `SCALAR_FUNCTIONS` slice body — a bracket list rather than a brace block.
fn scalar_functions_list(registry: &str) -> String {
	registry
		.split_once("pub const SCALAR_FUNCTIONS")
		.and_then(|(_, rest)| rest.split_once(';'))
		.expect("SCALAR_FUNCTIONS declaration")
		.0
		.to_string()
}

fn scalar_function_variant_problems(expr_body: &str) -> Vec<String> {
	let observed = enum_variant_names(expr_body)
		.into_iter()
		.filter(|name| !NON_SCALAR_EXPR_VARIANTS.contains(&name.as_str()))
		.collect::<BTreeSet<_>>();
	let expected = GRANDFATHERED_SCALAR_VARIANTS
		.iter()
		.copied()
		.chain(["ScalarFunction"])
		.map(str::to_owned)
		.collect::<BTreeSet<_>>();

	if observed == expected {
		Vec::new()
	} else {
		let missing = expected.difference(&observed).cloned().collect::<Vec<_>>();
		let unexpected = observed.difference(&expected).cloned().collect::<Vec<_>>();
		let mut problems = Vec::new();
		if !missing.is_empty() {
			problems.push(format!("missing scalar-function variants: {missing:?}"));
		}
		if !unexpected.is_empty() {
			problems.push(format!(
				"unexpected scalar-function variants: {unexpected:?}"
			));
		}
		problems
	}
}

#[test]
pub(crate) fn adr553_scalar_functions_use_the_registry_not_new_expr_variants() {
	let ir = source("src/ir.rs");
	let registry = source("src/ir/scalar_function.rs");
	let execution_ir = source("src/ir/execution.rs");
	let binder = binder_source();
	let evaluator = source("src/exec/expr.rs");
	let planner = source("src/sql_planner.rs");
	let rune = source("src/rune_engine.rs");
	let expr_body = brace_blocks_after_marker(&ir, "pub enum Expr")
		.into_iter()
		.next()
		.expect("Expr enum body")
		.1;
	let bound_expr_nodes = brace_blocks_after_marker(&execution_ir, "pub enum BoundExprNode")
		.into_iter()
		.next()
		.expect("BoundExprNode body")
		.1;
	let bound_scalar_function = brace_blocks_after_marker(&bound_expr_nodes, "ScalarFunction")
		.into_iter()
		.next()
		.expect("BoundExprNode::ScalarFunction body")
		.1;

	assert!(
		scalar_function_variant_problems(&expr_body).is_empty(),
		"{}",
		scalar_function_variant_problems(&expr_body).join("\n")
	);
	assert!(bound_scalar_function.contains("id: ScalarFunctionId"));
	assert!(bound_scalar_function.contains("args: Vec<BoundExprId>"));
	let registered_ids = enum_variant_names(
		&brace_blocks_after_marker(&registry, "pub enum ScalarFunctionId")
			.into_iter()
			.next()
			.expect("ScalarFunctionId enum body")
			.1,
	);
	assert_eq!(
		registered_ids,
		BTreeSet::from([
			"Abs".to_string(),
			"AtTimeZone".to_string(),
			"Ceil".to_string(),
			"CharLength".to_string(),
			"Concat".to_string(),
			"CurrentDate".to_string(),
			"StringConcat".to_string(),
			"DateTrunc".to_string(),
			"Exp".to_string(),
			"Extract".to_string(),
			"Floor".to_string(),
			"Greatest".to_string(),
			"Initcap".to_string(),
			"Least".to_string(),
			"Ln".to_string(),
			"Log".to_string(),
			"Ltrim".to_string(),
			"Lpad".to_string(),
			"Mod".to_string(),
			"Now".to_string(),
			"OctetLength".to_string(),
			"Overlay".to_string(),
			"Position".to_string(),
			"Pow".to_string(),
			"Random".to_string(),
			"Replace".to_string(),
			"Reverse".to_string(),
			"Rpad".to_string(),
			"Round".to_string(),
			"Rtrim".to_string(),
			"Lower".to_string(),
			"Sign".to_string(),
			"Sqrt".to_string(),
			"Strpos".to_string(),
			"Substring".to_string(),
			"SplitPart".to_string(),
			"TimeBucket".to_string(),
			"ToChar".to_string(),
			"ToDate".to_string(),
			"ToTimestamp".to_string(),
			"Translate".to_string(),
			"Trim".to_string(),
			"TryToDate".to_string(),
			"TryToTimestamp".to_string(),
			"Trunc".to_string(),
			"Upper".to_string(),
			"WidthBucket".to_string(),
		]),
		"admitting a function is a review decision; update this set deliberately"
	);

	// Every identifier must carry a complete registration. An id without
	// metadata compiles and only fails when a query reaches the lookup.
	for id in &registered_ids {
		let metadata = brace_blocks_after_marker(
			&registry,
			&format!("const {}: ScalarFunctionMetadata", const_name(id)),
		)
		.into_iter()
		.next()
		.unwrap_or_else(|| panic!("{id} has no ScalarFunctionMetadata const"))
		.1;
		let evaluator_id = match id.as_str() {
			"Ltrim" | "Rtrim" => "Trim",
			"Strpos" => "Position",
			// A strict and a TRY spelling share one computation; only their
			// failure metadata differs, which is the point of ADR 558.
			"TryToTimestamp" => "ToTimestamp",
			"TryToDate" => "ToDate",
			_ => id.as_str(),
		};
		assert!(
			metadata.contains(&format!(
				"evaluator: ScalarFunctionEvaluator::{evaluator_id}"
			)),
			"{id} metadata has the wrong evaluator"
		);
		for property in [
			format!("id: ScalarFunctionId::{id}"),
			"sql_name: \"".to_string(),
			"sql_aliases: &[".to_string(),
			"rune_name: \"".to_string(),
			"arity: ScalarFunctionArity::".to_string(),
			"signature: ScalarFunctionSignature::".to_string(),
			"null_behavior: ScalarFunctionNullBehavior::".to_string(),
			"failure: ScalarFunctionFailure::".to_string(),
			"non_finite_behavior: ScalarFunctionNonFiniteBehavior::".to_string(),
			"evaluation: ScalarFunctionEvaluation::".to_string(),
			"pbc_pushdown_eligible: ".to_string(),
		] {
			assert!(
				metadata.contains(&property),
				"{id} metadata missing {property}"
			);
		}
		let rune_name = metadata
			.split_once("rune_name: \"")
			.and_then(|(_, rest)| rest.split_once('"'))
			.expect("rune_name literal")
			.0
			.to_string();
		assert!(
			rune.contains(&format!("m.function([\"{rune_name}\"], {rune_name})")),
			"{id} is registered but not reachable from Rune as {rune_name}"
		);
		assert!(
			scalar_functions_list(&registry).contains(&const_name(id)),
			"{id} has metadata but is absent from SCALAR_FUNCTIONS, so lookups panic"
		);
	}
	assert!(binder.contains("fn bind_scalar_function"));
	assert!(evaluator.contains("fn evaluate_scalar_function"));
	assert!(planner.contains("fn lower_scalar_function"));
}

#[test]
pub(crate) fn adr553_guard_rejects_an_unlisted_scalar_variant() {
	let ir = source("src/ir.rs");
	let expr_body = brace_blocks_after_marker(&ir, "pub enum Expr")
		.into_iter()
		.next()
		.expect("Expr enum body")
		.1;
	let unlisted_variant = format!("{expr_body}\n\tPow(Box<Expr>),");

	assert!(
		scalar_function_variant_problems(&unlisted_variant)
			.iter()
			.any(|problem| problem.contains("unexpected scalar-function variants: [\"Pow\"]")),
		"the closed-variant guard must reject a scalar name it does not know"
	);
}
