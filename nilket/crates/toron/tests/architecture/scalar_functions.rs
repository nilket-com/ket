//! ADR 553 — typed scalar-function registry guards.

use std::collections::BTreeSet;

use super::support::*;

const GRANDFATHERED_SCALAR_VARIANTS: &[&str] = &[
	"Floor",
	"TimeBucket",
	"AtTimeZone",
	"Extract",
	"DateTrunc",
	"Substring",
];

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
	let exp_metadata = brace_blocks_after_marker(&registry, "const EXP: ScalarFunctionMetadata")
		.into_iter()
		.next()
		.expect("EXP metadata body")
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
	assert_eq!(
		enum_variant_names(
			&brace_blocks_after_marker(&registry, "pub enum ScalarFunctionId")
				.into_iter()
				.next()
				.expect("ScalarFunctionId enum body")
				.1
		),
		BTreeSet::from(["Exp".to_string()])
	);
	for property in [
		"id: ScalarFunctionId::Exp",
		"sql_name: \"EXP\"",
		"rune_name: \"exp\"",
		"pbc_pushdown_eligible: false",
		"non_finite_behavior: ScalarFunctionNonFiniteBehavior::Propagates",
		"evaluator: ScalarFunctionEvaluator::Exp",
	] {
		assert!(
			exp_metadata.contains(property),
			"EXP metadata missing {property}"
		);
	}
	assert!(registry.contains("pub const SCALAR_FUNCTIONS"));
	assert!(binder.contains("fn bind_scalar_function"));
	assert!(evaluator.contains("fn evaluate_scalar_function"));
	assert!(planner.contains("fn lower_scalar_function"));
	assert!(rune.contains("m.function([\"exp\"], exp)"));
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
