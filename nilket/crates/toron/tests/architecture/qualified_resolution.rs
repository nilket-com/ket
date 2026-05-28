#![allow(unused_imports)]

use super::support::*;

fn binder_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source")
}

fn planner_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source")
}

fn rune_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/rune_engine.rs"))
		.expect("read Rune engine source")
}

fn error_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/error.rs")).expect("read error source")
}

fn ir_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR source")
}

#[test]
pub(crate) fn adr121_scalar_qualifier_rejects_are_gone() {
	let binder = binder_source();
	let planner = planner_source();
	for forbidden in [
		"ORDER BY only supports unqualified output column names",
		"GROUP BY keys must be simple unqualified columns",
		"qualified HAVING column references are not supported",
		"qualified aggregate projection references are not supported",
	] {
		assert!(
			!binder.contains(forbidden) && !planner.contains(forbidden),
			"converted scalar qualifier reject remains: {forbidden}"
		);
	}
	let hidden_sort_refs = brace_blocks_after_marker(&planner, "fn validate_hidden_sort_key_refs")
		.into_iter()
		.next()
		.expect("hidden sort validator")
		.1;
	assert!(!hidden_sort_refs.contains("column.qualifier.is_some() ||"));
}

#[test]
pub(crate) fn adr121_qualified_wildcard_and_returning_stay_out_of_scope() {
	let planner = planner_source();
	for required in [
		"qualified wildcard projections are not supported",
		"mixed wildcard projections are not supported",
		"qualified RETURNING columns are not supported",
		"qualified wildcard RETURNING projections are not supported",
	] {
		assert!(
			planner.contains(required),
			"deferred wildcard/RETURNING reject disappeared: {required}"
		);
	}
	let returning = brace_blocks_after_marker(&planner, "fn lower_returning_projection")
		.into_iter()
		.next()
		.expect("lower_returning_projection")
		.1;
	assert!(returning.contains("expr_contains_qualified_column(&expr)"));
}

#[test]
pub(crate) fn adr121_qualifier_resolution_uses_single_resolver() {
	let binder = binder_source();
	assert!(binder.contains("fn resolve_qualified_column("));
	let join_resolver = brace_blocks_after_marker(&binder, "fn resolve_join_column")
		.into_iter()
		.next()
		.expect("resolve_join_column")
		.1;
	assert!(join_resolver.contains("resolve_qualified_column(column_ref, self)"));
}

#[test]
pub(crate) fn adr121_join_key_qualifier_resolution_preserved() {
	let binder = binder_source();
	assert!(binder.contains("fn bind_join_key_pair("));
	assert!(binder.contains("fn resolve_join_column_across("));
	assert!(binder.contains("validate_join_key_types("));
	let join_tests = std::fs::read_to_string(manifest_dir().join("tests/join_kind_test.rs"))
		.expect("read join kind tests");
	assert!(join_tests.contains("join_kind_serde_round_trips_all_eight_variants"));
	assert!(join_tests.contains("bind_join_promotes_outer_and_semi_anti_schemas"));
}

#[test]
pub(crate) fn adr121_qualifier_resolution_errors_are_named() {
	let error = error_source();
	for required in [
		"UnknownTableRef",
		"AmbiguousTableRef",
		"UnknownColumnInRelation",
		"AmbiguousColumnRef",
	] {
		assert!(
			error.contains(required),
			"missing named error surface {required}"
		);
	}
}

#[test]
pub(crate) fn adr121_alias_hides_base_qualifier() {
	let binder = binder_source();
	let alias_arm = binder
		.split("LogicalPlan::Alias")
		.nth(1)
		.and_then(|rest| rest.split("LogicalPlan::Filter").next())
		.expect("Alias bind arm");
	assert!(alias_arm.contains("bind_alias_column_rename(input.plan, columns)"));
	assert!(alias_arm.contains("RelationScope::alias(alias.clone(), &plan.schema())"));
	assert!(!binder.contains("collect_qualifiers"));
}

#[test]
pub(crate) fn adr121_alias_column_lists_feed_relation_scope_names() {
	let binder = binder_source();
	let alias_arm = binder
		.split("LogicalPlan::Alias")
		.nth(1)
		.and_then(|rest| rest.split("LogicalPlan::Filter").next())
		.expect("Alias bind arm");
	assert!(
		alias_arm.find("bind_alias_column_rename(input.plan, columns)")
			< alias_arm.find("RelationScope::alias(alias.clone(), &plan.schema())")
	);
	let behavior =
		std::fs::read_to_string(manifest_dir().join("tests/qualified_resolution_test.rs"))
			.expect("read qualified resolution tests");
	assert!(behavior.contains("alias_column_list_renames_qualified_column_scope"));
}

#[test]
pub(crate) fn adr121_rune_alias_qcol_surface_exposed() {
	let rune = rune_source();
	assert!(rune.contains("pub fn alias(self, name: String) -> Self"));
	assert!(rune.contains("pub fn qcol(rel: String, col: String) -> RuneExpr"));
	assert!(rune.contains("m.function([\"qcol\"], qcol).build()?;"));
	assert!(rune.contains("m.associated_function(\"alias\", RuneDataFrame::alias)?;"));
}

#[test]
pub(crate) fn adr121_column_ref_remains_the_only_qualified_column_ir() {
	let ir = ir_source();
	assert!(ir.contains("pub struct ColumnRef"));
	assert!(ir.contains("pub qualifier: Option<TableRef>"));
	assert!(ir.contains("pub fn qualified_column"));
	assert!(!ir.contains("QualifiedColumn"));
	assert!(!ir.contains("Wildcard("));
}

#[test]
pub(crate) fn adr121_no_executor_pbc_wendelon_change() {
	for path in [
		manifest_dir().join("src/exec.rs"),
		manifest_dir().join("src/pbc.rs"),
	] {
		let source = std::fs::read_to_string(&path).expect("read Toron source");
		assert!(
			!source.contains("ADR 121") && !source.contains("adr121"),
			"ADR 121 marker leaked into {}",
			path.display()
		);
	}
	let offenders = source_lines(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.filter(|(_, _, line)| line.contains("ADR 121") || line.contains("adr121"))
		.collect::<Vec<_>>();
	assert!(
		offenders.is_empty(),
		"ADR 121 marker leaked into Wendelon:\n{}",
		format_offenders(&wendelon_manifest_dir(), &offenders)
	);
}
