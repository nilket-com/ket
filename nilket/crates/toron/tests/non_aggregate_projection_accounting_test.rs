use std::time::Duration;

use toron::error::FrontendError;
use toron::ir::{Expr, LogicalPlan, ScalarValue, TemporalExtractField, TemporalTruncField};
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
	// ADR 553: TIME_BUCKET lowers through the registry, carrying its interval
	// as a constant argument. Only the reading moved; the contract asserted —
	// a 1h bucket over `ts` — is unchanged.
	let Expr::ScalarFunction { id, args, .. } = &exprs[0] else {
		panic!("expected time bucket");
	};
	assert_eq!(*id, toron::ir::ScalarFunctionId::TimeBucket);
	assert!(
		matches!(
			&args[0],
			Expr::Literal(ScalarValue::Interval(Some(nanos)))
				if *nanos == Duration::from_secs(3600).as_nanos() as i64
		),
		"expected a 1h constant interval, got {:?}",
		args[0]
	);
	assert_column(&args[1], "ts");
}

#[test]
fn extract_at_time_zone_lowers() {
	let plan = plan_sql("SELECT EXTRACT(HOUR FROM ts AT TIME ZONE 'America/Chicago') FROM sales");

	let LogicalPlan::Project { exprs, .. } = plan else {
		panic!("expected project");
	};
	let Expr::ScalarFunction {
		id: toron::ir::ScalarFunctionId::Extract,
		args,
		parameter: payload,
	} = &exprs[0]
	else {
		panic!("expected extract");
	};
	let toron::ir::ScalarFunctionParameter::Extract(payload) = payload else {
		panic!("expected extract payload");
	};
	assert_eq!(payload.field, TemporalExtractField::Hour);
	// ADR 553: AT TIME ZONE lowers through the registry, carrying its zone as a
	// constant argument. The contract asserted is unchanged.
	let Some((inner, zone)) = toron::ir::as_at_time_zone(&args[0]) else {
		panic!("expected AT TIME ZONE operand");
	};
	assert_eq!(zone, "America/Chicago");
	assert_column(inner, "ts");
}

#[test]
fn date_part_alias_lowers_to_extract() {
	let plan = plan_sql("SELECT date_part('doy', signup_date) FROM sales");

	let LogicalPlan::Project { exprs, .. } = plan else {
		panic!("expected project");
	};
	let Expr::ScalarFunction {
		id: toron::ir::ScalarFunctionId::Extract,
		args,
		parameter: payload,
	} = &exprs[0]
	else {
		panic!("expected extract");
	};
	let toron::ir::ScalarFunctionParameter::Extract(payload) = payload else {
		panic!("expected extract payload");
	};
	assert_eq!(payload.field, TemporalExtractField::DayOfYear);
	assert_column(&args[0], "signup_date");
}

#[test]
fn date_trunc_lowers() {
	let plan = plan_sql("SELECT date_trunc('day', ts) FROM sales");

	let LogicalPlan::Project { exprs, .. } = plan else {
		panic!("expected project");
	};
	let Expr::ScalarFunction {
		id: toron::ir::ScalarFunctionId::DateTrunc,
		args,
		parameter: payload,
	} = &exprs[0]
	else {
		panic!("expected date_trunc");
	};
	let toron::ir::ScalarFunctionParameter::DateTrunc(payload) = payload else {
		panic!("expected date_trunc payload");
	};
	assert_eq!(payload.field, TemporalTruncField::Day);
	assert_column(&args[0], "ts");
}

#[test]
fn temporal_operation_unsupported_shapes_reject_cleanly() {
	for sql in [
		"SELECT ts AT TIME ZONE zone_name FROM sales",
		"SELECT date_part('epoch', ts) FROM sales",
		"SELECT date_trunc('week', ts) FROM sales",
		"SELECT EXTRACT(ISODOW FROM ts) FROM sales",
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
