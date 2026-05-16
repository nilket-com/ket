use toron::error::FrontendError;
use toron::ir::{AggregateFunction, Expr, LogicalPlan};
use toron::sql_planner::SqlPlanner;

#[test]
fn existing_global_aggregates_byte_identical_after_accounting() {
	let plan = plan_sql("SELECT COUNT(*), SUM(amount) FROM sales WHERE amount > 100");

	let LogicalPlan::Aggregate {
		input,
		group_expr,
		aggr_expr,
	} = plan
	else {
		panic!("expected aggregate plan");
	};
	assert!(group_expr.is_empty());
	assert_eq!(aggr_expr.len(), 2);
	assert!(matches!(aggr_expr[0].0, AggregateFunction::CountAll));
	assert_column(&aggr_expr[0].1[0], "count");
	assert!(matches!(aggr_expr[1].0, AggregateFunction::Sum));
	assert_column(&aggr_expr[1].1[0], "amount");
	assert!(matches!(input.as_ref(), LogicalPlan::Filter { .. }));
}

#[test]
fn mixed_aggregate_and_non_aggregate_projection_without_group_by_rejects() {
	assert_unsupported("SELECT region, SUM(amount) FROM sales");
	assert_unsupported("SELECT region = 'west', SUM(amount) FROM sales");
}

#[test]
fn wildcard_mixed_with_aggregate_rejects() {
	assert_unsupported("SELECT *, COUNT(*) FROM sales");
}

#[test]
fn unsupported_projection_function_mixed_with_aggregate_rejects() {
	assert_unsupported("SELECT mystery(amount), SUM(amount) FROM sales");
	assert_unsupported("SELECT SUM(amount), MEDIAN(amount) FROM sales");
}

#[test]
fn malformed_aggregate_arity_or_modifiers_reject_cleanly() {
	for sql in [
		"SELECT COUNT() FROM sales",
		"SELECT COUNT(amount, region) FROM sales",
		"SELECT COUNT(DISTINCT amount) FROM sales",
		"SELECT SUM(DISTINCT amount) FROM sales",
		"SELECT SUM() FROM sales",
		"SELECT SUM(amount, region) FROM sales",
		"SELECT LINEAR_REGRESS(amount) FROM sales",
		"SELECT APPROX_QUANTILE(amount) FROM sales",
		"SELECT APPROX_QUANTILE(amount, region) FROM sales",
	] {
		assert_unsupported(sql);
	}
}

fn plan_sql(sql: &str) -> LogicalPlan {
	SqlPlanner::new().plan(sql).expect("plan sql")
}

fn assert_unsupported(sql: &str) {
	let err = SqlPlanner::new()
		.plan(sql)
		.expect_err("query should be unsupported");
	assert!(
		matches!(err, FrontendError::Unsupported { .. }),
		"{sql}: expected unsupported, got {err}"
	);
}

fn assert_column(expr: &Expr, expected: &str) {
	match expr {
		Expr::Column(column) => assert_eq!(column.name.0, expected),
		other => panic!("expected column {expected}, got {other:?}"),
	}
}
