use toron::error::FrontendError;
use toron::ir::LogicalPlan;
use toron::sql_planner::SqlPlanner;

#[test]
fn simple_select_still_lowers_after_clause_accounting() {
	let plan = plan_sql("SELECT id FROM t WHERE id > 10");

	assert!(matches!(plan, LogicalPlan::Project { .. }));
}

#[test]
fn top_level_order_by_limit_offset_is_supported() {
	let plan = plan_sql("SELECT id FROM t ORDER BY id DESC LIMIT 2 OFFSET 1");

	let LogicalPlan::Sort {
		input,
		order_by,
		limit,
		offset,
	} = plan
	else {
		panic!("expected top-level sort");
	};
	assert_eq!(limit, Some(2));
	assert_eq!(offset, 1);
	assert_eq!(order_by.len(), 1);
	assert!(order_by[0].descending);
	assert!(!order_by[0].nulls_first);
	assert!(matches!(
		input.as_ref(),
		LogicalPlan::Exchange {
			partition_count: 1,
			key,
			..
		} if key.is_empty()
	));
}

#[test]
fn limit_and_offset_without_order_by_are_unsupported() {
	for sql in [
		"SELECT id FROM t LIMIT 1",
		"SELECT id FROM t OFFSET 1",
		"SELECT id FROM t LIMIT 1 OFFSET 1",
	] {
		assert_unsupported(sql);
	}
}

#[test]
fn sqlparser_062_comma_limit_variant_remains_unsupported() {
	assert_unsupported_message(
		"SELECT id FROM t ORDER BY id LIMIT 1, 2",
		"LIMIT offset, count",
	);
}

#[test]
fn unsupported_query_level_clauses_are_loud() {
	for sql in [
		"WITH cte AS (SELECT id FROM t) SELECT id FROM cte",
		"SELECT id FROM t ORDER BY id FETCH FIRST 1 ROWS ONLY",
		"SELECT id FROM t FOR UPDATE",
	] {
		assert_unsupported(sql);
	}
}

#[test]
fn unsupported_select_level_clauses_are_loud() {
	for sql in [
		"SELECT DISTINCT id FROM t",
		"SELECT id FROM t GROUP BY id",
		"SELECT id FROM t HAVING id > 1",
		"SELECT id FROM t QUALIFY id > 1",
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

fn assert_unsupported_message(sql: &str, diagnostic: &str) {
	let err = SqlPlanner::new()
		.plan(sql)
		.expect_err("query should be unsupported");
	assert!(
		matches!(err, FrontendError::Unsupported { .. }),
		"{sql}: expected unsupported, got {err}"
	);
	assert!(
		err.to_string().contains(diagnostic),
		"{sql}: expected diagnostic {diagnostic:?}, got {err}"
	);
}
