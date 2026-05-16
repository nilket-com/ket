use std::time::Duration;

use toron::error::FrontendError;
use toron::ir::{Expr, LogicalPlan, ScalarValue};
use toron::sql_planner::SqlPlanner;

#[test]
fn bare_wildcard_projection_stays_byte_identical() {
	let plan = plan_sql("SELECT * FROM sales");

	assert!(matches!(
		plan,
		LogicalPlan::ScanNamed {
			table,
			projection: None,
		} if table.schema.is_none() && table.table == "sales"
	));
}

#[test]
fn explicit_projection_stays_byte_identical() {
	let plan = plan_sql("SELECT region, amount FROM sales WHERE amount > 10");

	let LogicalPlan::Project {
		input,
		exprs,
		output_names,
	} = plan
	else {
		panic!("expected project");
	};
	assert_eq!(exprs.len(), 2);
	assert_eq!(output_names, vec![None, None]);
	assert_column(&exprs[0], "region");
	assert_column(&exprs[1], "amount");

	let LogicalPlan::Filter { input, predicate } = input.as_ref() else {
		panic!("expected project over filter");
	};
	assert_amount_gt_10(predicate);
	assert!(matches!(
		input.as_ref(),
		LogicalPlan::ScanNamed {
			table,
			projection: None,
		} if table.schema.is_none() && table.table == "sales"
	));
}

#[test]
fn mixed_wildcard_projection_rejects_order_independently() {
	for sql in ["SELECT region, * FROM sales", "SELECT *, region FROM sales"] {
		assert_unsupported(sql);
	}
}

#[test]
fn multiple_unqualified_wildcards_reject() {
	assert_unsupported("SELECT *, * FROM sales");
}

#[test]
fn wildcard_mixed_with_qualified_wildcard_rejects() {
	assert_unsupported("SELECT *, sales.* FROM sales");
}

#[test]
fn qualified_wildcard_mixed_before_unqualified_wildcard_rejects() {
	assert_unsupported("SELECT sales.*, * FROM sales");
}

#[test]
fn explicit_projection_still_lowers() {
	let plan = plan_sql("SELECT region FROM sales");

	let LogicalPlan::Project {
		input,
		exprs,
		output_names,
	} = plan
	else {
		panic!("expected project");
	};
	assert_eq!(exprs.len(), 1);
	assert_eq!(output_names, vec![None]);
	assert_column(&exprs[0], "region");
	assert!(matches!(
		input.as_ref(),
		LogicalPlan::ScanNamed {
			table,
			projection: None,
		} if table.schema.is_none() && table.table == "sales"
	));
}

#[test]
fn non_aggregate_alias_flows_into_project_output_names() {
	let plan = plan_sql("SELECT amount AS a FROM sales");

	let LogicalPlan::Project {
		exprs,
		output_names,
		..
	} = plan
	else {
		panic!("expected project");
	};
	assert_eq!(exprs.len(), 1);
	assert_column(&exprs[0], "amount");
	assert_eq!(output_names, vec![Some("a".to_string())]);
}

#[test]
fn time_bucket_wrong_arity_rejects_cleanly() {
	for sql in [
		"SELECT TIME_BUCKET() FROM sales",
		"SELECT TIME_BUCKET(ts) FROM sales",
		"SELECT TIME_BUCKET(ts, '1h', ts) FROM sales",
	] {
		assert_unsupported(sql);
	}
}

#[test]
fn valid_time_bucket_still_lowers() {
	let plan = plan_sql("SELECT TIME_BUCKET(ts, '1h') FROM sales");

	let LogicalPlan::Project { exprs, .. } = plan else {
		panic!("expected project");
	};
	assert_eq!(exprs.len(), 1);
	let Expr::TimeBucket(interval, inner) = &exprs[0] else {
		panic!("expected time bucket");
	};
	assert_eq!(*interval, Duration::from_secs(3600));
	assert_column(inner, "ts");
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
		Expr::Column(column) => {
			assert!(column.qualifier.is_none());
			assert_eq!(column.name.0, expected);
		}
		other => panic!("expected column {expected}, got {other:?}"),
	}
}

fn assert_amount_gt_10(expr: &Expr) {
	let Expr::Gt(left, right) = expr else {
		panic!("expected amount > 10 predicate");
	};
	assert_column(left, "amount");
	assert!(matches!(
		right.as_ref(),
		Expr::Literal(ScalarValue::Int32(Some(10)))
	));
}
