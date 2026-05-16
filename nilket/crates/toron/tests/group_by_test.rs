use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use toron::binder::{PlannerConfig, bind};
use toron::catalog::{MockCatalog, TableRef};
use toron::distributed::budget::{QueryBudget, TaskBudget};
use toron::distributed::determinism::{SortPolicy, canonical_arrow_ipc_digest};
use toron::distributed::fragment::{FragmentError, FragmentPlanConfig, fragment_plan};
use toron::distributed::identity::{ExchangeId, QueryId, StageId};
use toron::distributed::task::{PartitionCount, TaskInputBinding, TaskOutputBinding};
use toron::error::{BindingError, FrontendError, StorageError};
use toron::exec::Executor;
use toron::ir::{
	AggregateFunction, ColumnOrdinal, ExecutionExpr, ExecutionPlan, Expr, FilterDisposition,
	LogicalPlan, ScalarValue, SortExpr, TableId,
};
use toron::sql_planner::SqlPlanner;
use toron::storage::{ScanRequest, ScanStream, StorageScan};
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptorV1, RowIdentityPolicy, SchemaEvolutionPolicyV1,
	TableObjectId, TableSchemaDescriptor,
};

#[test]
fn group_by_lowers_to_aggregate_over_group_keyed_exchange() {
	let plan = plan_sql("SELECT region, SUM(amount), COUNT(*) FROM sales GROUP BY region");

	let LogicalPlan::Aggregate {
		input,
		group_expr,
		aggr_expr,
	} = plan
	else {
		panic!("expected aggregate plan");
	};
	assert_eq!(group_expr.len(), 1);
	assert_column(&group_expr[0], "region");
	assert_eq!(aggr_expr.len(), 2);
	assert!(matches!(aggr_expr[0].0, AggregateFunction::Sum));
	assert_column(&aggr_expr[0].1[0], "amount");
	assert!(matches!(aggr_expr[1].0, AggregateFunction::CountAll));

	let LogicalPlan::Exchange {
		input,
		partition_count,
		key,
	} = input.as_ref()
	else {
		panic!("expected exchange below aggregate");
	};
	assert_eq!(*partition_count, 4);
	assert_eq!(key.len(), 1);
	assert_column(&key[0], "region");
	assert!(matches!(input.as_ref(), LogicalPlan::ScanNamed { .. }));
}

#[test]
fn group_by_requires_at_least_one_aggregate_call() {
	assert_unsupported("SELECT region FROM sales GROUP BY region");
}

#[test]
fn group_by_projection_must_be_exact_full_key_list_then_aggregates() {
	let plan = plan_sql("SELECT region, status, COUNT(*) FROM sales GROUP BY region, status");

	let LogicalPlan::Aggregate {
		group_expr,
		aggr_expr,
		..
	} = plan
	else {
		panic!("expected aggregate plan");
	};
	assert_eq!(group_expr.len(), 2);
	assert_column(&group_expr[0], "region");
	assert_column(&group_expr[1], "status");
	assert_eq!(aggr_expr.len(), 1);
	assert!(matches!(aggr_expr[0].0, AggregateFunction::CountAll));
}

#[test]
fn min_max_aggregate_lowers_in_scalar_and_grouped_projections() {
	let scalar = plan_sql("SELECT MIN(amount), MAX(id) FROM sales");
	let LogicalPlan::Aggregate {
		group_expr,
		aggr_expr,
		input,
	} = scalar
	else {
		panic!("expected scalar aggregate");
	};
	assert!(group_expr.is_empty());
	assert_eq!(aggr_expr.len(), 2);
	assert!(matches!(aggr_expr[0].0, AggregateFunction::Min));
	assert_column(&aggr_expr[0].1[0], "amount");
	assert!(matches!(aggr_expr[1].0, AggregateFunction::Max));
	assert_column(&aggr_expr[1].1[0], "id");
	assert!(matches!(input.as_ref(), LogicalPlan::ScanNamed { .. }));

	let grouped = plan_sql(
		"SELECT region, MIN(amount), MAX(amount) FROM sales \
		 GROUP BY region HAVING MIN(amount) > 7 ORDER BY MAX(amount) DESC LIMIT 2",
	);
	let LogicalPlan::Sort {
		input, order_by, ..
	} = grouped
	else {
		panic!("expected grouped sort");
	};
	assert_column(&order_by[0].expr, "max");
	let LogicalPlan::Exchange { input, .. } = input.as_ref() else {
		panic!("expected terminal exchange");
	};
	let LogicalPlan::Filter { input, predicate } = input.as_ref() else {
		panic!("expected grouped HAVING filter");
	};
	assert!(matches!(
		predicate,
		Expr::Gt(left, _)
			if matches!(left.as_ref(), Expr::Column(column) if column.name.0 == "min")
	));
	let LogicalPlan::Aggregate {
		input,
		group_expr,
		aggr_expr,
	} = input.as_ref()
	else {
		panic!("expected grouped aggregate");
	};
	assert_eq!(group_expr.len(), 1);
	assert_column(&group_expr[0], "region");
	assert_eq!(aggr_expr.len(), 2);
	assert!(matches!(aggr_expr[0].0, AggregateFunction::Min));
	assert!(matches!(aggr_expr[1].0, AggregateFunction::Max));
	assert!(matches!(input.as_ref(), LogicalPlan::Exchange { .. }));
}

#[test]
fn avg_aggregate_lowers_in_scalar_and_grouped_projections() {
	let scalar = plan_sql("SELECT AVG(amount), AVG(price) FROM sales");
	let LogicalPlan::Aggregate {
		input,
		group_expr,
		aggr_expr,
	} = scalar
	else {
		panic!("expected scalar aggregate");
	};
	assert!(group_expr.is_empty());
	assert_eq!(aggr_expr.len(), 2);
	assert!(matches!(aggr_expr[0].0, AggregateFunction::Avg));
	assert_column(&aggr_expr[0].1[0], "amount");
	assert!(matches!(aggr_expr[1].0, AggregateFunction::Avg));
	assert_column(&aggr_expr[1].1[0], "price");
	assert!(matches!(input.as_ref(), LogicalPlan::ScanNamed { .. }));

	let grouped = plan_sql(
		"SELECT status, AVG(amount) FROM sales \
		 GROUP BY status HAVING AVG(amount) > 7 ORDER BY AVG(amount) DESC LIMIT 2",
	);
	let LogicalPlan::Sort {
		input, order_by, ..
	} = grouped
	else {
		panic!("expected grouped sort");
	};
	assert_column(&order_by[0].expr, "avg");
	let LogicalPlan::Exchange { input, .. } = input.as_ref() else {
		panic!("expected terminal exchange");
	};
	let LogicalPlan::Filter { input, predicate } = input.as_ref() else {
		panic!("expected grouped HAVING filter");
	};
	assert!(matches!(
		predicate,
		Expr::Gt(left, _)
			if matches!(left.as_ref(), Expr::Column(column) if column.name.0 == "avg")
	));
	let LogicalPlan::Aggregate {
		input,
		group_expr,
		aggr_expr,
	} = input.as_ref()
	else {
		panic!("expected grouped aggregate");
	};
	assert_eq!(group_expr.len(), 1);
	assert_column(&group_expr[0], "status");
	assert_eq!(aggr_expr.len(), 1);
	assert!(matches!(aggr_expr[0].0, AggregateFunction::Avg));
	assert_column(&aggr_expr[0].1[0], "amount");
	assert!(matches!(input.as_ref(), LogicalPlan::Exchange { .. }));
}

#[test]
fn group_by_projection_subset_reorder_alias_or_aggregate_first_rejects() {
	for sql in [
		"SELECT region, COUNT(*) FROM sales GROUP BY region, status",
		"SELECT status, region, COUNT(*) FROM sales GROUP BY region, status",
		"SELECT COUNT(*), region FROM sales GROUP BY region",
		"SELECT region AS r, COUNT(*) FROM sales GROUP BY region",
		"SELECT region, COUNT(*) AS n FROM sales GROUP BY region",
	] {
		assert_unsupported(sql);
	}
}

#[test]
fn group_by_accepts_only_stable_aggregate_set() {
	for sql in [
		"SELECT region, COUNT(*) FROM sales GROUP BY region",
		"SELECT region, COUNT(amount) FROM sales GROUP BY region",
		"SELECT region, SUM(amount) FROM sales GROUP BY region",
		"SELECT region, AVG(amount) FROM sales GROUP BY region",
	] {
		assert!(plan_sql(sql).is_aggregate(), "{sql}");
	}
	for sql in [
		"SELECT region, LINEAR_REGRESS(amount, id) FROM sales GROUP BY region",
		"SELECT region, APPROX_QUANTILE(amount, 0.5) FROM sales GROUP BY region",
	] {
		assert_unsupported(sql);
	}
}

#[test]
fn group_by_keys_must_be_simple_unqualified_input_columns() {
	for sql in [
		"SELECT region, COUNT(*) FROM sales GROUP BY sales.region",
		"SELECT region, COUNT(*) FROM sales GROUP BY 1",
		"SELECT region, COUNT(*) FROM sales GROUP BY region + 1",
		"SELECT sales.region, COUNT(*) FROM sales GROUP BY region",
	] {
		assert_unsupported(sql);
	}
}

#[test]
fn group_by_over_explicit_join_rejects() {
	assert_unsupported(
		"SELECT region, COUNT(*) FROM sales \
		 INNER JOIN regions ON sales.region = regions.region \
		 GROUP BY region",
	);
}

#[test]
fn group_by_over_decorrelated_exists_rejects() {
	assert_unsupported(
		"SELECT region, COUNT(*) FROM sales \
		 WHERE EXISTS (SELECT 1 FROM regions WHERE regions.region = sales.region) \
		 GROUP BY region",
	);
}

#[test]
fn group_by_with_order_by_lowers_to_terminal_sort_over_aggregate() {
	let plan = plan_sql("SELECT region, COUNT(*) FROM sales GROUP BY region ORDER BY region");

	let LogicalPlan::Sort {
		input,
		order_by,
		limit,
		offset,
	} = plan
	else {
		panic!("expected sort over grouped aggregate");
	};
	assert_eq!(limit, None);
	assert_eq!(offset, 0);
	assert_eq!(order_by.len(), 1);
	assert_column(&order_by[0].expr, "region");
	assert!(!order_by[0].descending);

	let LogicalPlan::Exchange {
		input,
		partition_count,
		key,
	} = input.as_ref()
	else {
		panic!("expected terminal exchange below sort");
	};
	assert_eq!(*partition_count, 1);
	assert!(key.is_empty());
	assert_grouped_aggregate_over_exchange(input.as_ref());
}

#[test]
fn group_by_with_order_by_ordinal_limit_offset_lowers() {
	let plan = plan_sql(
		"SELECT region, SUM(amount) FROM sales GROUP BY region \
		 ORDER BY 2 DESC LIMIT 10 OFFSET 1",
	);

	let LogicalPlan::Sort {
		input,
		order_by,
		limit,
		offset,
	} = plan
	else {
		panic!("expected sort over grouped aggregate");
	};
	assert_eq!(limit, Some(10));
	assert_eq!(offset, 1);
	assert_eq!(order_by.len(), 1);
	assert!(order_by[0].descending);
	assert!(matches!(
		order_by[0].expr,
		Expr::Literal(ScalarValue::Int32(Some(2)))
	));
	assert!(matches!(
		input.as_ref(),
		LogicalPlan::Exchange {
			partition_count: 1,
			key,
			input,
		} if key.is_empty() && matches!(input.as_ref(), LogicalPlan::Aggregate { .. })
	));
}

#[test]
fn group_by_order_by_aggregate_call_rewrites_to_projected_output_field() {
	let plan =
		plan_sql("SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY SUM(amount) DESC");

	let LogicalPlan::Sort {
		order_by, input, ..
	} = plan
	else {
		panic!("expected sort over grouped aggregate");
	};
	assert_eq!(order_by.len(), 1);
	assert_column(&order_by[0].expr, "sum");
	assert!(order_by[0].descending);
	assert!(matches!(
		input.as_ref(),
		LogicalPlan::Exchange {
			partition_count: 1,
			key,
			input,
		} if key.is_empty() && matches!(input.as_ref(), LogicalPlan::Aggregate { .. })
	));
}

#[test]
fn group_by_having_and_order_by_aggregate_calls_rewrite_together() {
	let plan = plan_sql(
		"SELECT region, SUM(amount) FROM sales GROUP BY region \
		 HAVING SUM(amount) > 10 ORDER BY SUM(amount) DESC",
	);

	let LogicalPlan::Sort {
		order_by, input, ..
	} = plan
	else {
		panic!("expected sort over HAVING filter");
	};
	assert_eq!(order_by.len(), 1);
	assert_column(&order_by[0].expr, "sum");
	assert!(order_by[0].descending);
	let LogicalPlan::Exchange { input, .. } = input.as_ref() else {
		panic!("expected terminal exchange");
	};
	let LogicalPlan::Filter { predicate, .. } = input.as_ref() else {
		panic!("expected HAVING filter");
	};
	assert!(matches!(
		predicate,
		Expr::Gt(left, _)
			if matches!(left.as_ref(), Expr::Column(column) if column.name.0 == "sum")
	));
}

#[test]
fn group_by_order_by_aggregate_call_rewrite_is_case_insensitive() {
	let plan =
		plan_sql("SELECT region, sum(amount) FROM sales GROUP BY region ORDER BY SUM(amount) DESC");

	let LogicalPlan::Sort { order_by, .. } = plan else {
		panic!("expected sort over grouped aggregate");
	};
	assert_column(&order_by[0].expr, "sum");
}

#[test]
fn group_by_order_by_qualified_column_reference_still_reaches_lower_expr() {
	let plan = plan_sql("SELECT region, COUNT(*) FROM sales GROUP BY region ORDER BY sales.region");

	let LogicalPlan::Sort { order_by, .. } = plan else {
		panic!("expected sort over grouped aggregate");
	};
	assert_qualified_column(&order_by[0].expr, "sales", "region");
}

#[test]
fn having_aggregate_call_rewrites_to_projected_output_field() {
	let plan = plan_sql("SELECT region, COUNT(*) FROM sales GROUP BY region HAVING COUNT(*) > 5");

	let LogicalPlan::Filter { input, predicate } = plan else {
		panic!("expected HAVING filter over aggregate");
	};
	assert!(matches!(
		predicate,
		Expr::Gt(left, right)
			if matches!(left.as_ref(), Expr::Column(column) if column.name.0 == "count")
				&& matches!(right.as_ref(), Expr::Literal(ScalarValue::Int32(Some(5))))
	));
	assert_grouped_aggregate_over_exchange(input.as_ref());
}

#[test]
fn having_with_order_by_lowers_filter_inside_terminal_exchange() {
	let plan = plan_sql(
		"SELECT region, SUM(amount) FROM sales GROUP BY region \
		 HAVING SUM(amount) > 1000 ORDER BY 2 DESC LIMIT 10 OFFSET 1",
	);

	let LogicalPlan::Sort {
		input,
		order_by,
		limit,
		offset,
	} = plan
	else {
		panic!("expected sort over HAVING filter");
	};
	assert_eq!(limit, Some(10));
	assert_eq!(offset, 1);
	assert_eq!(order_by.len(), 1);
	assert!(order_by[0].descending);
	let LogicalPlan::Exchange {
		input,
		partition_count,
		key,
	} = input.as_ref()
	else {
		panic!("expected terminal exchange below sort");
	};
	assert_eq!(*partition_count, 1);
	assert!(key.is_empty());
	let LogicalPlan::Filter { input, predicate } = input.as_ref() else {
		panic!("expected HAVING filter below terminal exchange");
	};
	assert!(matches!(
		predicate,
		Expr::Gt(left, _)
			if matches!(left.as_ref(), Expr::Column(column) if column.name.0 == "sum")
	));
	assert_grouped_aggregate_over_exchange(input.as_ref());
}

#[test]
fn having_group_key_predicate_lowers_without_aggregate_rewrite() {
	let plan =
		plan_sql("SELECT region, COUNT(*) FROM sales GROUP BY region HAVING region = 'EMEA'");

	let LogicalPlan::Filter { predicate, .. } = plan else {
		panic!("expected HAVING filter");
	};
	assert!(matches!(
		predicate,
		Expr::Eq(left, right)
			if matches!(left.as_ref(), Expr::Column(column) if column.name.0 == "region")
				&& matches!(right.as_ref(), Expr::Literal(ScalarValue::Utf8(Some(value))) if value == "EMEA")
	));
}

#[test]
fn having_aggregate_rewrite_is_case_insensitive() {
	let plan =
		plan_sql("SELECT region, sum(amount) FROM sales GROUP BY region HAVING SUM(amount) > 10");

	let LogicalPlan::Filter { predicate, .. } = plan else {
		panic!("expected HAVING filter");
	};
	assert!(matches!(
		predicate,
		Expr::Gt(left, _)
			if matches!(left.as_ref(), Expr::Column(column) if column.name.0 == "sum")
	));
}

#[test]
fn having_aggregate_call_argument_must_match_projected_aggregate() {
	assert_unsupported_message(
		"SELECT region, SUM(amount) FROM sales GROUP BY region HAVING SUM(other_amount) > 1000",
		"HAVING aggregate-call must appear in SELECT list",
	);
	assert_unsupported_message(
		"SELECT region, SUM(amount) FROM sales GROUP BY region HAVING SUM(sales.amount) > 10",
		"HAVING aggregate-call must appear in SELECT list",
	);
}

#[test]
fn order_by_aggregate_call_argument_must_match_projected_aggregate() {
	assert_unsupported_message(
		"SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY SUM(other_amount)",
		"ORDER BY aggregate-call must appear in SELECT list",
	);
}

#[test]
fn scalar_aggregate_having_lowers_to_filter_over_aggregate() {
	let plan = plan_sql("SELECT SUM(amount) FROM sales HAVING SUM(amount) > 1000");

	let LogicalPlan::Filter { input, predicate } = plan else {
		panic!("expected HAVING filter over scalar aggregate");
	};
	assert!(matches!(
		predicate,
		Expr::Gt(left, right)
			if matches!(left.as_ref(), Expr::Column(column) if column.name.0 == "sum")
				&& matches!(right.as_ref(), Expr::Literal(ScalarValue::Int32(Some(1000))))
	));
	assert_scalar_sum_aggregate(input.as_ref());
}

#[test]
fn scalar_aggregate_order_by_lowers_to_sort_over_exchange_over_aggregate() {
	let plan = plan_sql("SELECT SUM(amount) FROM sales ORDER BY SUM(amount) DESC");

	let LogicalPlan::Sort {
		input,
		order_by,
		limit,
		offset,
	} = plan
	else {
		panic!("expected sort over scalar aggregate");
	};
	assert_eq!(limit, None);
	assert_eq!(offset, 0);
	assert_eq!(order_by.len(), 1);
	assert_column(&order_by[0].expr, "sum");
	assert!(order_by[0].descending);
	let LogicalPlan::Exchange {
		input,
		partition_count,
		key,
	} = input.as_ref()
	else {
		panic!("expected terminal exchange below scalar sort");
	};
	assert_eq!(*partition_count, 1);
	assert!(key.is_empty());
	assert_scalar_sum_aggregate(input.as_ref());
}

#[test]
fn scalar_aggregate_dual_rewrite_having_and_order_by() {
	let plan = plan_sql(
		"SELECT SUM(amount) FROM sales \
		 HAVING SUM(amount) > 1000 ORDER BY SUM(amount) DESC LIMIT 1",
	);

	let LogicalPlan::Sort {
		input,
		order_by,
		limit,
		offset,
	} = plan
	else {
		panic!("expected sort over scalar HAVING filter");
	};
	assert_eq!(limit, Some(1));
	assert_eq!(offset, 0);
	assert_eq!(order_by.len(), 1);
	assert_column(&order_by[0].expr, "sum");
	assert!(order_by[0].descending);
	let LogicalPlan::Exchange { input, .. } = input.as_ref() else {
		panic!("expected terminal exchange");
	};
	let LogicalPlan::Filter { input, predicate } = input.as_ref() else {
		panic!("expected scalar HAVING filter below terminal exchange");
	};
	assert!(matches!(
		predicate,
		Expr::Gt(left, _)
			if matches!(left.as_ref(), Expr::Column(column) if column.name.0 == "sum")
	));
	assert_scalar_sum_aggregate(input.as_ref());
}

#[test]
fn scalar_aggregate_order_by_ordinal_lowers_without_aggregate_rewrite() {
	let plan = plan_sql("SELECT SUM(amount) FROM sales ORDER BY 1 DESC");

	let LogicalPlan::Sort { order_by, .. } = plan else {
		panic!("expected sort over scalar aggregate");
	};
	assert!(matches!(
		order_by[0].expr,
		Expr::Literal(ScalarValue::Int32(Some(1)))
	));
	assert!(order_by[0].descending);
}

#[test]
fn scalar_aggregate_having_ambiguity_is_rejected() {
	assert_unsupported_message(
		"SELECT COUNT(*), COUNT(*) FROM events HAVING COUNT(*) > 0",
		"ambiguous HAVING aggregate-call",
	);
}

#[test]
fn scalar_aggregate_order_by_ambiguity_is_rejected() {
	assert_unsupported_message(
		"SELECT COUNT(*), COUNT(*) FROM events ORDER BY COUNT(*)",
		"ambiguous ORDER BY aggregate-call",
	);
}

#[test]
fn scalar_aggregate_order_by_not_in_select_rejects() {
	assert_unsupported_message(
		"SELECT COUNT(*) FROM sales ORDER BY SUM(amount)",
		"ORDER BY aggregate-call must appear in SELECT list",
	);
}

#[test]
fn scalar_aggregate_having_not_in_select_rejects() {
	assert_unsupported_message(
		"SELECT COUNT(*) FROM sales HAVING SUM(amount) > 0",
		"HAVING aggregate-call must appear in SELECT list",
	);
}

#[test]
fn scalar_aggregate_having_qualified_reference_rejects() {
	assert_unsupported_message(
		"SELECT SUM(amount) FROM sales HAVING t.amount > 0",
		"qualified HAVING column references are not supported",
	);
}

#[test]
fn having_without_group_by_and_without_aggregate_still_rejects() {
	assert_unsupported_message(
		"SELECT region FROM sales HAVING region = 'EMEA'",
		"HAVING requires GROUP BY or an aggregate projection",
	);
}

#[test]
fn scalar_aggregate_order_by_computed_aggregate_call_expression_still_rejects() {
	assert_unsupported("SELECT SUM(amount) FROM sales ORDER BY SUM(amount) + 100");
	assert_unsupported("SELECT SUM(amount) FROM sales ORDER BY -SUM(amount)");
}

#[test]
fn scalar_aggregate_having_experimental_aggregate_rejects() {
	assert_unsupported("SELECT LINEAR_REGRESS(x, y) FROM t HAVING LINEAR_REGRESS(x, y) > 0");
}

#[test]
fn having_aggregate_call_must_appear_in_select_list() {
	assert_unsupported_message(
		"SELECT region FROM sales GROUP BY region HAVING SUM(amount) > 100",
		"HAVING aggregate-call must appear in SELECT list",
	);
}

#[test]
fn having_aggregate_call_ambiguity_is_rejected() {
	for sql in [
		"SELECT region, COUNT(*), COUNT(amount) FROM sales GROUP BY region HAVING COUNT(*) > 5",
		"SELECT region, COUNT(*), COUNT(*) FROM sales GROUP BY region HAVING COUNT(*) > 5",
		"SELECT count, COUNT(amount) FROM events GROUP BY count HAVING COUNT(amount) > 0",
	] {
		assert_unsupported_message(sql, "ambiguous HAVING aggregate-call");
	}
}

#[test]
fn order_by_aggregate_call_ambiguity_is_rejected() {
	for sql in [
		"SELECT region, COUNT(*), COUNT(amount) FROM sales GROUP BY region ORDER BY COUNT(*)",
		"SELECT region, COUNT(*), COUNT(*) FROM sales GROUP BY region ORDER BY COUNT(*)",
		"SELECT count, COUNT(amount) FROM events GROUP BY count ORDER BY COUNT(amount)",
	] {
		assert_unsupported_message(sql, "ambiguous ORDER BY aggregate-call");
	}
}

#[test]
fn having_qualified_column_reference_is_rejected() {
	assert_unsupported_message(
		"SELECT region, COUNT(*) FROM sales GROUP BY region HAVING t.region = 'EMEA'",
		"qualified HAVING column references are not supported",
	);
}

#[test]
fn order_by_computed_aggregate_call_expression_still_rejects() {
	assert_unsupported(
		"SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY SUM(amount) + 100",
	);
	assert_unsupported(
		"SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY -SUM(amount)",
	);
}

#[test]
fn having_integer_literal_is_not_an_ordinal_reference() {
	let plan = plan_sql("SELECT region, COUNT(*) FROM sales GROUP BY region HAVING 2 > 100");

	let LogicalPlan::Filter { predicate, .. } = plan else {
		panic!("expected HAVING filter");
	};
	assert!(matches!(
		predicate,
		Expr::Gt(left, right)
			if matches!(left.as_ref(), Expr::Literal(ScalarValue::Int32(Some(2))))
				&& matches!(right.as_ref(), Expr::Literal(ScalarValue::Int32(Some(100))))
	));
}

#[test]
fn having_generated_name_form_resolves_without_rewrite() {
	let plan = plan_sql("SELECT region, COUNT(*) FROM sales GROUP BY region HAVING count > 0");

	let LogicalPlan::Filter { predicate, .. } = plan else {
		panic!("expected HAVING filter");
	};
	assert!(matches!(
		predicate,
		Expr::Gt(left, _)
			if matches!(left.as_ref(), Expr::Column(column) if column.name.0 == "count")
	));
}

#[test]
fn having_preserves_inherited_group_by_rejects() {
	assert_unsupported(
		"SELECT region, COUNT(*) FROM sales \
		 INNER JOIN regions ON sales.region = regions.region \
		 GROUP BY region HAVING COUNT(*) > 1",
	);
	assert_unsupported(
		"SELECT DISTINCT region, COUNT(*) FROM sales GROUP BY region HAVING COUNT(*) > 1",
	);
	assert_unsupported(
		"SELECT region, SUM(amount) FROM sales GROUP BY region HAVING SUM(amount) + 100 > 1000",
	);
	assert_unsupported(
		"SELECT region, SUM(amount) FROM sales GROUP BY region HAVING SUM(COUNT(*)) > 5",
	);
	assert_unsupported("SELECT region, COUNT(*) AS n FROM sales GROUP BY region HAVING n > 0");
}

#[test]
fn distinct_is_unsupported_not_dropped() {
	assert_unsupported("SELECT DISTINCT region, COUNT(*) FROM sales GROUP BY region");
}

#[tokio::test]
async fn grouped_aggregate_sql_matches_hand_built_aggregate_plan() {
	let sql =
		plan_sql("SELECT region, COUNT(*), COUNT(amount), SUM(amount) FROM sales GROUP BY region");
	let hand_built = hand_built_grouped_plan();
	let sql_results = run_logical(&sql).await;
	let hand_built_results = run_logical(&hand_built).await;

	assert_eq!(
		canonical_arrow_ipc_digest(&sql_results, SortPolicy::FullRowLex).expect("sql digest"),
		canonical_arrow_ipc_digest(&hand_built_results, SortPolicy::FullRowLex)
			.expect("hand digest")
	);
	assert_eq!(
		grouped_rows(&sql_results),
		HashMap::from([
			(Some("east".to_string()), (3, 2, Some(40))),
			(Some("west".to_string()), (2, 1, Some(20))),
			(None, (1, 1, Some(7))),
		])
	);
}

#[tokio::test]
async fn grouped_aggregate_order_by_binds_against_aggregate_output() {
	let (catalog, _) = fixture();
	for (sql, expected_ordinal, expected_descending) in [
		(
			"SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY region",
			ColumnOrdinal(0),
			false,
		),
		(
			"SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY sum DESC",
			ColumnOrdinal(1),
			true,
		),
		(
			"SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY SUM(amount) DESC",
			ColumnOrdinal(1),
			true,
		),
		(
			"SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY 2 DESC",
			ColumnOrdinal(1),
			true,
		),
	] {
		let logical = plan_sql(sql);
		let bound = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind grouped sort");
		let ExecutionPlan::Sort { order_by, .. } = bound else {
			panic!("expected sort for {sql}");
		};
		assert_eq!(order_by[0].ordinal, expected_ordinal, "{sql}");
		assert_eq!(order_by[0].descending, expected_descending, "{sql}");
	}
}

#[tokio::test]
async fn min_max_bind_with_identity_output_types_and_reject_deferred_domains() {
	let (catalog, _) = fixture();
	let logical = plan_sql("SELECT MIN(amount), MAX(id) FROM sales");
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind MIN/MAX");
	let ExecutionPlan::Aggregate { output_schema, .. } = bound else {
		panic!("expected aggregate");
	};
	assert_eq!(output_schema.field(0).name(), "min");
	assert_eq!(output_schema.field(0).data_type(), &DataType::Int32);
	assert!(output_schema.field(0).is_nullable());
	assert_eq!(output_schema.field(1).name(), "max");
	assert_eq!(output_schema.field(1).data_type(), &DataType::Int64);
	assert!(output_schema.field(1).is_nullable());

	for (sql, expected) in [
		(
			"SELECT MIN(price) FROM sales",
			"MIN expects Int32 or Int64 input, got Float64",
		),
		(
			"SELECT MAX(status) FROM sales",
			"MAX expects Int32 or Int64 input, got Utf8",
		),
	] {
		let logical = plan_sql(sql);
		let err = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect_err("deferred MIN/MAX domain should reject");
		assert!(
			matches!(&err, BindingError::TypeMismatch { message } if message == expected),
			"{sql}: expected {expected:?}, got {err}"
		);
	}

	for (function, expected) in [
		(AggregateFunction::Min, "MIN requires one argument"),
		(AggregateFunction::Max, "MAX requires one argument"),
	] {
		let logical = LogicalPlan::Aggregate {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("sales"),
				projection: None,
			}),
			group_expr: vec![],
			aggr_expr: vec![(function, vec![Expr::column("amount"), Expr::column("id")])],
		};
		let err = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect_err("hand-built MIN/MAX with extra args should reject");
		assert!(
			matches!(&err, BindingError::UnsupportedExpression { message } if message == expected),
			"expected {expected:?}, got {err}"
		);
	}
}

#[tokio::test]
async fn avg_bind_with_float64_output_and_numeric_domain() {
	let (catalog, _) = fixture();
	let logical = plan_sql("SELECT AVG(amount), AVG(id), AVG(price) FROM sales");
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind AVG");
	let ExecutionPlan::Aggregate { output_schema, .. } = bound else {
		panic!("expected aggregate");
	};
	assert_eq!(output_schema.fields().len(), 3);
	for field in output_schema.fields() {
		assert_eq!(field.name(), "avg");
		assert_eq!(field.data_type(), &DataType::Float64);
		assert!(field.is_nullable());
	}

	let err = bind(
		&plan_sql("SELECT AVG(status) FROM sales"),
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect_err("AVG over Utf8 should reject");
	assert!(
		matches!(&err, BindingError::TypeMismatch { message } if message == "AVG expects numeric input, got Utf8"),
		"expected AVG type mismatch, got {err}"
	);

	let logical = LogicalPlan::Aggregate {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("sales"),
			projection: None,
		}),
		group_expr: vec![],
		aggr_expr: vec![(
			AggregateFunction::Avg,
			vec![Expr::column("amount"), Expr::column("id")],
		)],
	};
	let err = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect_err("hand-built AVG with extra args should reject");
	assert!(
		matches!(&err, BindingError::UnsupportedExpression { message } if message == "AVG requires one argument"),
		"expected AVG arity reject, got {err}"
	);
}

#[test]
fn min_max_aggregate_rejects_invalid_and_unselected_references() {
	assert_unsupported_message("SELECT MIN() FROM sales", "MIN requires one argument");
	assert_unsupported("SELECT MIN(*) FROM sales");
	assert_unsupported("SELECT MIN(DISTINCT amount) FROM sales");
	assert_unsupported_message(
		"SELECT MIN(amount), MIN(amount) FROM sales HAVING MIN(amount) > 0",
		"ambiguous HAVING aggregate-call",
	);
	assert_unsupported_message(
		"SELECT MIN(amount) FROM sales ORDER BY MAX(id)",
		"ORDER BY aggregate-call must appear in SELECT list",
	);
}

#[test]
fn avg_aggregate_rejects_invalid_and_unselected_references() {
	assert_unsupported_message("SELECT AVG() FROM sales", "AVG requires one argument");
	assert_unsupported("SELECT AVG(*) FROM sales");
	assert_unsupported("SELECT AVG(DISTINCT amount) FROM sales");
	assert_unsupported_message(
		"SELECT AVG(amount), AVG(amount) FROM sales HAVING AVG(amount) > 0",
		"ambiguous HAVING aggregate-call",
	);
	assert_unsupported_message(
		"SELECT AVG(amount) FROM sales ORDER BY SUM(amount)",
		"ORDER BY aggregate-call must appear in SELECT list",
	);
}

#[tokio::test]
async fn grouped_aggregate_order_by_duplicate_generated_name_is_ambiguous() {
	let (catalog, _) = fixture();
	let logical = plan_sql(
		"SELECT region, COUNT(*), COUNT(amount) FROM sales GROUP BY region ORDER BY count DESC",
	);

	let err = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect_err("duplicate generated aggregate name should be ambiguous");
	assert!(matches!(
		err,
		BindingError::AmbiguousColumn { column } if column == "count"
	));
}

#[tokio::test]
async fn grouped_aggregate_having_binds_against_aggregate_output() {
	let (catalog, _) = fixture();
	let logical =
		plan_sql("SELECT region, SUM(amount) FROM sales GROUP BY region HAVING SUM(amount) > 10");
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind HAVING");

	let ExecutionPlan::Filter {
		input,
		predicate,
		disposition,
	} = bound
	else {
		panic!("expected bound HAVING filter");
	};
	assert_eq!(disposition, FilterDisposition::EvaluateInToron);
	assert!(matches!(
		predicate,
		ExecutionExpr::Gt(left, right)
			if matches!(left.as_ref(), ExecutionExpr::Column(column)
				if column.name.0 == "sum" && column.ordinal == ColumnOrdinal(1))
				&& matches!(right.as_ref(), ExecutionExpr::Literal(ScalarValue::Int64(Some(10))))
	));
	assert!(matches!(input.as_ref(), ExecutionPlan::Aggregate { .. }));
}

#[tokio::test]
async fn avg_having_binds_with_float64_literal_coercion() {
	let (catalog, _) = fixture();
	let logical =
		plan_sql("SELECT status, AVG(amount) FROM sales GROUP BY status HAVING AVG(amount) > 10");
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind AVG HAVING");

	let ExecutionPlan::Filter {
		input,
		predicate,
		disposition,
	} = bound
	else {
		panic!("expected bound HAVING filter");
	};
	assert_eq!(disposition, FilterDisposition::EvaluateInToron);
	assert!(matches!(
		predicate,
		ExecutionExpr::Gt(left, right)
			if matches!(left.as_ref(), ExecutionExpr::Column(column)
				if column.name.0 == "avg" && column.ordinal == ColumnOrdinal(1))
				&& matches!(right.as_ref(), ExecutionExpr::Literal(ScalarValue::Float64(Some(value))) if *value == 10.0)
	));
	assert!(matches!(input.as_ref(), ExecutionPlan::Aggregate { .. }));
}

#[tokio::test]
async fn grouped_aggregate_having_group_key_binds_against_aggregate_output() {
	let (catalog, _) = fixture();
	let logical =
		plan_sql("SELECT region, COUNT(*) FROM sales GROUP BY region HAVING region = 'east'");
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind HAVING group key");

	let ExecutionPlan::Filter { predicate, .. } = bound else {
		panic!("expected bound HAVING filter");
	};
	assert!(matches!(
		predicate,
		ExecutionExpr::Eq(left, right)
			if matches!(left.as_ref(), ExecutionExpr::Column(column)
				if column.name.0 == "region" && column.ordinal == ColumnOrdinal(0))
				&& matches!(right.as_ref(), ExecutionExpr::Literal(ScalarValue::Utf8(Some(value))) if value == "east")
	));
}

#[tokio::test]
async fn grouped_aggregate_having_generated_name_duplicate_is_ambiguous() {
	let (catalog, _) = fixture();
	let logical = plan_sql(
		"SELECT region, COUNT(*), COUNT(amount) FROM sales GROUP BY region HAVING count > 0",
	);

	let err = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect_err("duplicate generated HAVING name should be ambiguous");
	assert!(matches!(
		err,
		BindingError::AmbiguousColumn { column } if column == "count"
	));
}

#[tokio::test]
async fn grouped_aggregate_having_non_output_column_rejects_at_bind() {
	let (catalog, _) = fixture();
	let logical = plan_sql("SELECT region, COUNT(*) FROM sales GROUP BY region HAVING raw_col > 5");

	let err = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect_err("non-output HAVING column should reject");
	assert!(matches!(
		err,
		BindingError::UnknownColumn { column } if column == "raw_col"
	));
}

#[tokio::test]
async fn grouped_aggregate_order_by_executes_after_aggregation() {
	let top_two = run_logical(&plan_sql(
		"SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY SUM(amount) DESC LIMIT 2",
	))
	.await;
	assert_eq!(
		ordered_region_sum_rows(&top_two),
		vec![
			(Some("east".to_string()), Some(40)),
			(Some("west".to_string()), Some(20)),
		]
	);

	let offset = run_logical(&plan_sql(
		"SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY 2 DESC LIMIT 1 OFFSET 1",
	))
	.await;
	assert_eq!(
		ordered_region_sum_rows(&offset),
		vec![(Some("west".to_string()), Some(20))]
	);
}

#[tokio::test]
async fn grouped_aggregate_having_executes_after_aggregation() {
	let results = run_logical(&plan_sql(
		"SELECT region, SUM(amount) FROM sales GROUP BY region HAVING SUM(amount) > 20",
	))
	.await;
	assert_eq!(
		ordered_region_sum_rows(&results),
		vec![(Some("east".to_string()), Some(40))]
	);

	let count_results = run_logical(&plan_sql(
		"SELECT region, COUNT(*) FROM sales GROUP BY region HAVING COUNT(*) > 2",
	))
	.await;
	assert_eq!(
		region_count_rows(&count_results),
		vec![(Some("east".to_string()), 3)]
	);
}

#[tokio::test]
async fn grouped_aggregate_having_order_by_limit_offset_executes() {
	let results = run_logical(&plan_sql(
		"SELECT region, SUM(amount) FROM sales GROUP BY region \
		 HAVING SUM(amount) > 5 ORDER BY SUM(amount) DESC LIMIT 1 OFFSET 1",
	))
	.await;
	assert_eq!(
		ordered_region_sum_rows(&results),
		vec![(Some("west".to_string()), Some(20))]
	);
}

#[tokio::test]
async fn scalar_aggregate_having_executes_with_one_row_output() {
	let results = run_logical(&plan_sql(
		"SELECT SUM(amount) FROM sales \
		 HAVING SUM(amount) > 0 ORDER BY SUM(amount) DESC LIMIT 1",
	))
	.await;

	assert_eq!(scalar_sum_rows(&results), vec![Some(67)]);
}

#[tokio::test]
async fn scalar_aggregate_having_filters_to_zero_rows_when_predicate_false() {
	let results = run_logical(&plan_sql(
		"SELECT SUM(amount) FROM sales HAVING SUM(amount) > 1000",
	))
	.await;

	assert!(scalar_sum_rows(&results).is_empty());
}

#[tokio::test]
async fn scalar_aggregate_having_empty_input_pins_inherited_behavior() {
	let aggregate_results = run_logical(&plan_sql("SELECT SUM(amount) FROM empty_sales")).await;
	assert_eq!(scalar_sum_rows(&aggregate_results), vec![None]);

	let having_results = run_logical(&plan_sql(
		"SELECT SUM(amount) FROM empty_sales HAVING SUM(amount) > 0",
	))
	.await;

	assert!(scalar_sum_rows(&having_results).is_empty());
}

#[tokio::test]
async fn scalar_min_max_execute_with_identity_types_and_null_semantics() {
	let results = run_logical(&plan_sql("SELECT MIN(amount), MAX(id) FROM sales")).await;
	assert_eq!(
		scalar_min_amount_max_id_rows(&results),
		vec![(Some(7), Some(6))]
	);

	let empty = run_logical(&plan_sql("SELECT MIN(amount), MAX(id) FROM empty_sales")).await;
	assert_eq!(scalar_min_amount_max_id_rows(&empty), vec![(None, None)]);
}

#[tokio::test]
async fn grouped_min_max_execute_with_having_order_by_and_all_null_group() {
	let ordered = run_logical(&plan_sql(
		"SELECT region, MIN(amount), MAX(id) FROM sales GROUP BY region \
		 HAVING MIN(amount) > 7 ORDER BY MAX(id) DESC LIMIT 2",
	))
	.await;
	assert_eq!(
		region_min_amount_max_id_rows(&ordered),
		vec![
			(Some("west".to_string()), Some(20), Some(5)),
			(Some("east".to_string()), Some(10), Some(4)),
		]
	);

	let by_status = run_logical(&plan_sql(
		"SELECT status, MIN(amount) FROM sales GROUP BY status ORDER BY status",
	))
	.await;
	assert_eq!(
		status_min_amount_rows(&by_status),
		vec![
			(Some("closed".to_string()), None),
			(Some("open".to_string()), Some(7)),
		]
	);

	let empty = run_logical(&plan_sql(
		"SELECT region, MIN(amount) FROM empty_sales GROUP BY region",
	))
	.await;
	assert_eq!(empty.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
}

#[tokio::test]
async fn scalar_avg_execute_with_float64_output_and_null_semantics() {
	let results = run_logical(&plan_sql("SELECT AVG(amount), AVG(price) FROM sales")).await;
	assert_eq!(scalar_float64_column_rows(&results, 0), vec![Some(16.75)]);
	assert_eq!(scalar_float64_column_rows(&results, 1), vec![Some(17.25)]);

	let empty = run_logical(&plan_sql("SELECT AVG(amount) FROM empty_sales")).await;
	assert_eq!(scalar_float64_column_rows(&empty, 0), vec![None]);

	let all_null = run_logical(&plan_sql("SELECT AVG(all_null_amount) FROM sales")).await;
	assert_eq!(scalar_float64_column_rows(&all_null, 0), vec![None]);
}

#[tokio::test]
async fn grouped_avg_execute_with_having_order_by_and_all_null_group() {
	let by_status = run_logical(&plan_sql(
		"SELECT status, AVG(amount) FROM sales GROUP BY status ORDER BY status",
	))
	.await;
	assert_eq!(
		status_avg_rows(&by_status),
		vec![
			(Some("closed".to_string()), None),
			(Some("open".to_string()), Some(16.75)),
		]
	);

	let filtered = run_logical(&plan_sql(
		"SELECT status, AVG(amount) FROM sales GROUP BY status \
		 HAVING AVG(amount) > 10 ORDER BY AVG(amount) DESC LIMIT 1",
	))
	.await;
	assert_eq!(
		status_avg_rows(&filtered),
		vec![(Some("open".to_string()), Some(16.75))]
	);

	let empty = run_logical(&plan_sql(
		"SELECT status, AVG(amount) FROM empty_sales GROUP BY status",
	))
	.await;
	assert_eq!(empty.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
}

#[tokio::test]
async fn avg_float64_sticky_nan_behavior_is_pinned() {
	let scalar = run_logical(&plan_sql("SELECT AVG(score) FROM sales")).await;
	let values = scalar_float64_column_rows(&scalar, 0);
	assert_eq!(values.len(), 1);
	assert!(values[0].expect("scalar AVG score").is_nan());

	let grouped = run_logical(&plan_sql(
		"SELECT status, AVG(score) FROM sales GROUP BY status ORDER BY status",
	))
	.await;
	let rows = status_avg_rows(&grouped);
	assert_eq!(rows.len(), 2);
	assert_eq!(rows[0], (Some("closed".to_string()), Some(3.0)));
	assert_eq!(rows[1].0, Some("open".to_string()));
	assert!(rows[1].1.expect("open score AVG").is_nan());
}

#[tokio::test]
async fn ordered_grouped_aggregate_sql_matches_hand_built_sort_plan() {
	let sql = plan_sql(
		"SELECT region, SUM(amount) FROM sales GROUP BY region \
		 ORDER BY SUM(amount) DESC LIMIT 10 OFFSET 1",
	);
	let hand_built = hand_built_ordered_grouped_sum_plan(Some(10), 1);
	let sql_results = run_logical(&sql).await;
	let hand_built_results = run_logical(&hand_built).await;

	assert_eq!(
		canonical_arrow_ipc_digest(&sql_results, SortPolicy::None).expect("sql digest"),
		canonical_arrow_ipc_digest(&hand_built_results, SortPolicy::None).expect("hand digest")
	);
}

#[tokio::test]
async fn scalar_min_max_sql_matches_hand_built_aggregate_plan() {
	let sql = plan_sql("SELECT MIN(amount), MAX(id) FROM sales");
	let hand_built = hand_built_scalar_min_max_plan();
	let sql_results = run_logical(&sql).await;
	let hand_built_results = run_logical(&hand_built).await;

	assert_eq!(
		canonical_arrow_ipc_digest(&sql_results, SortPolicy::FullRowLex).expect("sql digest"),
		canonical_arrow_ipc_digest(&hand_built_results, SortPolicy::FullRowLex)
			.expect("hand digest")
	);
}

#[tokio::test]
async fn grouped_min_max_having_order_by_sql_matches_hand_built_plan() {
	let sql = plan_sql(
		"SELECT region, MIN(amount), MAX(id) FROM sales GROUP BY region \
		 HAVING MIN(amount) > 7 ORDER BY MAX(id) DESC LIMIT 2",
	);
	let hand_built = hand_built_ordered_having_grouped_min_max_plan(Some(2), 0, 7);
	let sql_results = run_logical(&sql).await;
	let hand_built_results = run_logical(&hand_built).await;

	assert_eq!(
		canonical_arrow_ipc_digest(&sql_results, SortPolicy::None).expect("sql digest"),
		canonical_arrow_ipc_digest(&hand_built_results, SortPolicy::None).expect("hand digest")
	);
}

#[tokio::test]
async fn scalar_avg_sql_matches_hand_built_aggregate_plan() {
	let sql = plan_sql("SELECT AVG(amount) FROM sales");
	let hand_built = hand_built_scalar_avg_plan();
	let sql_results = run_logical(&sql).await;
	let hand_built_results = run_logical(&hand_built).await;

	assert_eq!(
		canonical_arrow_ipc_digest(&sql_results, SortPolicy::FullRowLex).expect("sql digest"),
		canonical_arrow_ipc_digest(&hand_built_results, SortPolicy::FullRowLex)
			.expect("hand digest")
	);
}

#[tokio::test]
async fn grouped_avg_having_order_by_sql_matches_hand_built_plan() {
	let sql = plan_sql(
		"SELECT status, AVG(amount) FROM sales GROUP BY status \
		 HAVING AVG(amount) > 10 ORDER BY AVG(amount) DESC LIMIT 1",
	);
	let hand_built = hand_built_ordered_having_grouped_avg_plan(Some(1), 0, 10);
	let sql_results = run_logical(&sql).await;
	let hand_built_results = run_logical(&hand_built).await;

	assert_eq!(
		canonical_arrow_ipc_digest(&sql_results, SortPolicy::None).expect("sql digest"),
		canonical_arrow_ipc_digest(&hand_built_results, SortPolicy::None).expect("hand digest")
	);
}

#[tokio::test]
async fn grouped_aggregate_having_sql_matches_hand_built_filter_plan() {
	let sql =
		plan_sql("SELECT region, SUM(amount) FROM sales GROUP BY region HAVING SUM(amount) > 10");
	let hand_built = hand_built_having_grouped_sum_plan();
	let sql_results = run_logical(&sql).await;
	let hand_built_results = run_logical(&hand_built).await;

	assert_eq!(
		canonical_arrow_ipc_digest(&sql_results, SortPolicy::FullRowLex).expect("sql digest"),
		canonical_arrow_ipc_digest(&hand_built_results, SortPolicy::FullRowLex)
			.expect("hand digest")
	);
}

#[tokio::test]
async fn grouped_aggregate_having_order_by_sql_matches_hand_built_sort_plan() {
	let sql = plan_sql(
		"SELECT region, SUM(amount) FROM sales GROUP BY region \
		 HAVING SUM(amount) > 10 ORDER BY SUM(amount) DESC LIMIT 10 OFFSET 1",
	);
	let hand_built = hand_built_ordered_having_grouped_sum_plan(Some(10), 1);
	let sql_results = run_logical(&sql).await;
	let hand_built_results = run_logical(&hand_built).await;

	assert_eq!(
		canonical_arrow_ipc_digest(&sql_results, SortPolicy::None).expect("sql digest"),
		canonical_arrow_ipc_digest(&hand_built_results, SortPolicy::None).expect("hand digest")
	);
}

#[tokio::test]
async fn scalar_aggregate_having_sql_matches_hand_built_filter_plan() {
	let sql = plan_sql("SELECT SUM(amount) FROM sales HAVING SUM(amount) > 10");
	let hand_built = hand_built_having_scalar_sum_plan(10);
	let sql_results = run_logical(&sql).await;
	let hand_built_results = run_logical(&hand_built).await;

	assert_eq!(
		canonical_arrow_ipc_digest(&sql_results, SortPolicy::FullRowLex).expect("sql digest"),
		canonical_arrow_ipc_digest(&hand_built_results, SortPolicy::FullRowLex)
			.expect("hand digest")
	);
}

#[tokio::test]
async fn scalar_aggregate_having_order_by_sql_matches_hand_built_plan() {
	let sql = plan_sql(
		"SELECT SUM(amount) FROM sales \
		 HAVING SUM(amount) > 10 ORDER BY SUM(amount) DESC LIMIT 1",
	);
	let hand_built = hand_built_ordered_having_scalar_sum_plan(Some(1), 0, 10);
	let sql_results = run_logical(&sql).await;
	let hand_built_results = run_logical(&hand_built).await;

	assert_eq!(
		canonical_arrow_ipc_digest(&sql_results, SortPolicy::None).expect("sql digest"),
		canonical_arrow_ipc_digest(&hand_built_results, SortPolicy::None).expect("hand digest")
	);
}

#[tokio::test]
async fn grouped_aggregate_bound_plan_fragments_to_single_group_keyed_exchange() {
	let (catalog, _) = fixture();
	let logical = plan_sql("SELECT region, COUNT(*) FROM sales GROUP BY region");
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind");
	let stages = fragment_plan(
		bound,
		FragmentPlanConfig::new(
			QueryId::new(),
			TaskBudget::from_query_budget(QueryBudget::test_default()),
		),
	)
	.expect("fragment");

	assert_eq!(stages.len(), 2);
	assert_eq!(stages[0].stage_id, StageId::new(0));
	assert_eq!(stages[1].stage_id, StageId::new(1));
	assert_eq!(stages[1].upstream_stages, vec![StageId::new(0)]);
	assert_eq!(stages[0].tasks.len(), 1);
	assert_eq!(stages[1].tasks.len(), 4);
	assert!(matches!(
		&stages[0].tasks[0].spec.output,
		TaskOutputBinding::ShuffleProducer {
			partition_count,
			partition_key,
			..
		} if *partition_count == PartitionCount::new(4) && partition_key.len() == 1
	));
	for task in &stages[1].tasks {
		assert!(matches!(task.spec.output, TaskOutputBinding::FinalResult));
		assert!(matches!(
			task.spec.inputs[0],
			TaskInputBinding::ShuffleConsumer { .. }
		));
		assert!(matches!(task.spec.plan, ExecutionPlan::Aggregate { .. }));
	}
}

#[tokio::test]
async fn grouped_aggregate_having_fragments_through_single_group_keyed_exchange() {
	let (catalog, _) = fixture();
	let logical =
		plan_sql("SELECT region, SUM(amount) FROM sales GROUP BY region HAVING SUM(amount) > 10");
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind");
	let stages = fragment_plan(bound, fragment_config()).expect("fragment HAVING aggregate");

	assert_eq!(stages.len(), 2);
	assert_eq!(stages[1].upstream_stages, vec![StageId::new(0)]);
	assert_eq!(stages[1].tasks.len(), 4);
	for task in &stages[1].tasks {
		assert!(matches!(
			task.spec.plan,
			ExecutionPlan::Filter { ref input, .. }
				if matches!(input.as_ref(), ExecutionPlan::Aggregate { .. })
		));
		assert!(matches!(task.spec.output, TaskOutputBinding::FinalResult));
	}
}

#[tokio::test]
async fn grouped_aggregate_order_by_fragments_to_terminal_sort_stage() {
	let (catalog, _) = fixture();
	let logical = plan_sql(
		"SELECT region, SUM(amount) FROM sales GROUP BY region \
		 ORDER BY SUM(amount) DESC LIMIT 10 OFFSET 1",
	);
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind");
	let stages = fragment_plan(bound, fragment_config()).expect("fragment ordered aggregate");

	assert_eq!(stages.len(), 3);
	assert_eq!(stages[0].stage_id, StageId::new(0));
	assert_eq!(stages[1].stage_id, StageId::new(1));
	assert_eq!(stages[2].stage_id, StageId::new(2));
	assert!(stages[0].upstream_stages.is_empty());
	assert_eq!(stages[1].upstream_stages, vec![StageId::new(0)]);
	assert_eq!(stages[2].upstream_stages, vec![StageId::new(1)]);

	assert!(matches!(
		&stages[0].tasks[0].spec.output,
		TaskOutputBinding::ShuffleProducer {
			exchange_id,
			partition_count,
			partition_key,
		} if *exchange_id == ExchangeId::new(0)
			&& *partition_count == PartitionCount::new(4)
			&& partition_key.len() == 1
	));
	assert_eq!(stages[1].tasks.len(), 4);
	for (partition, task) in stages[1].tasks.iter().enumerate() {
		assert!(matches!(task.spec.plan, ExecutionPlan::Aggregate { .. }));
		assert!(matches!(
			&task.spec.inputs[0],
			TaskInputBinding::ShuffleConsumer {
				exchange_id,
				producer_stage_id,
				partition_id,
				..
			} if *exchange_id == ExchangeId::new(0)
				&& *producer_stage_id == StageId::new(0)
				&& partition_id.0 == u32::try_from(partition).unwrap()
		));
		assert!(matches!(
			&task.spec.output,
			TaskOutputBinding::ShuffleProducer {
				exchange_id,
				partition_count,
				partition_key,
			} if *exchange_id == ExchangeId::new(1)
				&& *partition_count == PartitionCount::new(1)
				&& partition_key.is_empty()
		));
	}

	assert_eq!(stages[2].tasks.len(), 1);
	let terminal = &stages[2].tasks[0].spec;
	assert!(matches!(terminal.output, TaskOutputBinding::FinalResult));
	assert!(matches!(
		&terminal.inputs[0],
		TaskInputBinding::ShuffleConsumer {
			exchange_id,
			producer_stage_id,
			partition_id,
			..
		} if *exchange_id == ExchangeId::new(1)
			&& *producer_stage_id == StageId::new(1)
			&& partition_id.0 == 0
	));
	assert!(matches!(
		&terminal.plan,
		ExecutionPlan::Sort {
			order_by,
			limit: Some(10),
			offset: 1,
			..
		} if order_by[0].ordinal == ColumnOrdinal(1) && order_by[0].descending
	));
}

#[tokio::test]
async fn grouped_aggregate_having_order_by_fragments_filter_into_aggregate_stage() {
	let (catalog, _) = fixture();
	let logical = plan_sql(
		"SELECT region, SUM(amount) FROM sales GROUP BY region \
		 HAVING SUM(amount) > 10 ORDER BY SUM(amount) DESC LIMIT 10 OFFSET 1",
	);
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind");
	let stages =
		fragment_plan(bound, fragment_config()).expect("fragment ordered HAVING aggregate");

	assert_eq!(stages.len(), 3);
	assert_eq!(stages[0].stage_id, StageId::new(0));
	assert_eq!(stages[1].stage_id, StageId::new(1));
	assert_eq!(stages[2].stage_id, StageId::new(2));
	assert_eq!(stages[1].upstream_stages, vec![StageId::new(0)]);
	assert_eq!(stages[2].upstream_stages, vec![StageId::new(1)]);
	for task in &stages[1].tasks {
		assert!(matches!(
			task.spec.plan,
			ExecutionPlan::Filter { ref input, .. }
				if matches!(input.as_ref(), ExecutionPlan::Aggregate { .. })
		));
		assert!(matches!(
			&task.spec.output,
			TaskOutputBinding::ShuffleProducer {
				exchange_id,
				partition_count,
				partition_key,
			} if *exchange_id == ExchangeId::new(1)
				&& *partition_count == PartitionCount::new(1)
				&& partition_key.is_empty()
		));
	}
	assert!(matches!(
		&stages[2].tasks[0].spec.plan,
		ExecutionPlan::Sort {
			order_by,
			limit: Some(10),
			offset: 1,
			..
		} if order_by[0].ordinal == ColumnOrdinal(1) && order_by[0].descending
	));
}

#[tokio::test]
async fn scalar_aggregate_having_only_fragments_to_single_task_stage() {
	let (catalog, _) = fixture();
	let logical = plan_sql("SELECT SUM(amount) FROM sales HAVING SUM(amount) > 10");
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind scalar HAVING aggregate");
	let stages = fragment_plan(bound, fragment_config()).expect("fragment scalar HAVING");

	assert_eq!(stages.len(), 1);
	assert_eq!(stages[0].stage_id, StageId::new(0));
	assert!(stages[0].upstream_stages.is_empty());
	assert_eq!(stages[0].tasks.len(), 1);
	let task = &stages[0].tasks[0].spec;
	assert_eq!(task.partition, None);
	assert!(matches!(task.output, TaskOutputBinding::FinalResult));
	assert!(matches!(task.inputs[0], TaskInputBinding::Scan { .. }));
	assert!(matches!(
		&task.plan,
		ExecutionPlan::Filter { input, .. }
			if matches!(
				input.as_ref(),
				ExecutionPlan::Aggregate { group_expr, .. } if group_expr.is_empty()
			)
	));
}

#[tokio::test]
async fn scalar_aggregate_ordered_fragments_to_generic_two_stage() {
	let (catalog, _) = fixture();
	let logical = plan_sql("SELECT SUM(amount) FROM sales ORDER BY SUM(amount) DESC");
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind ordered scalar aggregate");
	let stages = fragment_plan(bound, fragment_config()).expect("fragment ordered scalar");

	assert_eq!(stages.len(), 2);
	assert_eq!(stages[0].stage_id, StageId::new(0));
	assert_eq!(stages[1].stage_id, StageId::new(1));
	assert!(stages[0].upstream_stages.is_empty());
	assert_eq!(stages[1].upstream_stages, vec![StageId::new(0)]);
	assert_eq!(stages[0].tasks.len(), 1);
	assert_eq!(stages[1].tasks.len(), 1);
	assert!(matches!(
		&stages[0].tasks[0].spec.output,
		TaskOutputBinding::ShuffleProducer {
			exchange_id,
			partition_count,
			partition_key,
		} if *exchange_id == ExchangeId::new(0)
			&& *partition_count == PartitionCount::new(1)
			&& partition_key.is_empty()
	));
	assert!(matches!(
		&stages[0].tasks[0].spec.plan,
		ExecutionPlan::Exchange {
			input,
			partition_count: 1,
			key,
		} if key.is_empty()
			&& matches!(
				input.as_ref(),
				ExecutionPlan::Aggregate { group_expr, .. } if group_expr.is_empty()
			)
	));
	let terminal = &stages[1].tasks[0].spec;
	assert!(matches!(terminal.output, TaskOutputBinding::FinalResult));
	assert!(matches!(
		&terminal.inputs[0],
		TaskInputBinding::ShuffleConsumer {
			exchange_id,
			producer_stage_id,
			partition_id,
			..
		} if *exchange_id == ExchangeId::new(0)
			&& *producer_stage_id == StageId::new(0)
			&& partition_id.0 == 0
	));
	assert!(matches!(
		&terminal.plan,
		ExecutionPlan::Sort {
			order_by,
			limit: None,
			offset: 0,
			..
		} if order_by[0].ordinal == ColumnOrdinal(0) && order_by[0].descending
	));
}

#[tokio::test]
async fn non_aggregate_two_exchange_terminal_sort_still_rejects() {
	let (catalog, _) = fixture();
	let bound = bind(
		&project_between_exchanges_sort_plan(),
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect("bind non-aggregate two-exchange sort");

	assert_eq!(
		fragment_plan(bound, fragment_config()).expect_err("two exchanges should reject"),
		FragmentError::MultipleExchangeBoundaries { count: 2 }
	);
}

#[tokio::test]
async fn filter_without_aggregate_between_terminal_exchanges_still_rejects() {
	let (catalog, _) = fixture();
	let bound = bind(
		&filter_without_aggregate_between_exchanges_sort_plan(),
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect("bind non-aggregate filtered two-exchange sort");

	assert_eq!(
		fragment_plan(bound, fragment_config()).expect_err("two exchanges should reject"),
		FragmentError::MultipleExchangeBoundaries { count: 2 }
	);
}

async fn run_logical(logical: &LogicalPlan) -> Vec<RecordBatch> {
	let (catalog, storage) = fixture();
	let bound = bind(logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind");
	Executor::new(storage)
		.execute_bound(&bound, None)
		.await
		.expect("execute")
}

fn hand_built_grouped_plan() -> LogicalPlan {
	LogicalPlan::Aggregate {
		input: Box::new(LogicalPlan::Exchange {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("sales"),
				projection: None,
			}),
			partition_count: 4,
			key: vec![Expr::column("region")],
		}),
		group_expr: vec![Expr::column("region")],
		aggr_expr: vec![
			(AggregateFunction::CountAll, vec![Expr::column("count")]),
			(AggregateFunction::Count, vec![Expr::column("amount")]),
			(AggregateFunction::Sum, vec![Expr::column("amount")]),
		],
	}
}

fn hand_built_ordered_grouped_sum_plan(limit: Option<usize>, offset: usize) -> LogicalPlan {
	LogicalPlan::Sort {
		input: Box::new(LogicalPlan::Exchange {
			input: Box::new(LogicalPlan::Aggregate {
				input: Box::new(LogicalPlan::Exchange {
					input: Box::new(LogicalPlan::ScanNamed {
						table: TableRef::new("sales"),
						projection: None,
					}),
					partition_count: 4,
					key: vec![Expr::column("region")],
				}),
				group_expr: vec![Expr::column("region")],
				aggr_expr: vec![(AggregateFunction::Sum, vec![Expr::column("amount")])],
			}),
			partition_count: 1,
			key: Vec::new(),
		}),
		order_by: vec![SortExpr {
			expr: Expr::Literal(ScalarValue::Int32(Some(2))),
			descending: true,
			nulls_first: false,
		}],
		limit,
		offset,
	}
}

fn hand_built_having_grouped_sum_plan() -> LogicalPlan {
	LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Aggregate {
			input: Box::new(LogicalPlan::Exchange {
				input: Box::new(LogicalPlan::ScanNamed {
					table: TableRef::new("sales"),
					projection: None,
				}),
				partition_count: 4,
				key: vec![Expr::column("region")],
			}),
			group_expr: vec![Expr::column("region")],
			aggr_expr: vec![(AggregateFunction::Sum, vec![Expr::column("amount")])],
		}),
		predicate: Expr::Gt(
			Box::new(Expr::column("sum")),
			Box::new(Expr::Literal(ScalarValue::Int32(Some(10)))),
		),
	}
}

fn hand_built_ordered_having_grouped_sum_plan(limit: Option<usize>, offset: usize) -> LogicalPlan {
	LogicalPlan::Sort {
		input: Box::new(LogicalPlan::Exchange {
			input: Box::new(hand_built_having_grouped_sum_plan()),
			partition_count: 1,
			key: Vec::new(),
		}),
		order_by: vec![SortExpr {
			expr: Expr::Literal(ScalarValue::Int32(Some(2))),
			descending: true,
			nulls_first: false,
		}],
		limit,
		offset,
	}
}

fn hand_built_scalar_sum_plan() -> LogicalPlan {
	LogicalPlan::Aggregate {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("sales"),
			projection: None,
		}),
		group_expr: vec![],
		aggr_expr: vec![(AggregateFunction::Sum, vec![Expr::column("amount")])],
	}
}

fn hand_built_scalar_min_max_plan() -> LogicalPlan {
	LogicalPlan::Aggregate {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("sales"),
			projection: None,
		}),
		group_expr: vec![],
		aggr_expr: vec![
			(AggregateFunction::Min, vec![Expr::column("amount")]),
			(AggregateFunction::Max, vec![Expr::column("id")]),
		],
	}
}

fn hand_built_scalar_avg_plan() -> LogicalPlan {
	LogicalPlan::Aggregate {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("sales"),
			projection: None,
		}),
		group_expr: vec![],
		aggr_expr: vec![(AggregateFunction::Avg, vec![Expr::column("amount")])],
	}
}

fn hand_built_having_scalar_sum_plan(threshold: i32) -> LogicalPlan {
	LogicalPlan::Filter {
		input: Box::new(hand_built_scalar_sum_plan()),
		predicate: Expr::Gt(
			Box::new(Expr::column("sum")),
			Box::new(Expr::Literal(ScalarValue::Int32(Some(threshold)))),
		),
	}
}

fn hand_built_ordered_having_scalar_sum_plan(
	limit: Option<usize>,
	offset: usize,
	threshold: i32,
) -> LogicalPlan {
	LogicalPlan::Sort {
		input: Box::new(LogicalPlan::Exchange {
			input: Box::new(hand_built_having_scalar_sum_plan(threshold)),
			partition_count: 1,
			key: Vec::new(),
		}),
		order_by: vec![SortExpr {
			expr: Expr::column("sum"),
			descending: true,
			nulls_first: false,
		}],
		limit,
		offset,
	}
}

fn hand_built_ordered_having_grouped_avg_plan(
	limit: Option<usize>,
	offset: usize,
	threshold: i32,
) -> LogicalPlan {
	LogicalPlan::Sort {
		input: Box::new(LogicalPlan::Exchange {
			input: Box::new(LogicalPlan::Filter {
				input: Box::new(LogicalPlan::Aggregate {
					input: Box::new(LogicalPlan::Exchange {
						input: Box::new(LogicalPlan::ScanNamed {
							table: TableRef::new("sales"),
							projection: None,
						}),
						partition_count: 4,
						key: vec![Expr::column("status")],
					}),
					group_expr: vec![Expr::column("status")],
					aggr_expr: vec![(AggregateFunction::Avg, vec![Expr::column("amount")])],
				}),
				predicate: Expr::Gt(
					Box::new(Expr::column("avg")),
					Box::new(Expr::Literal(ScalarValue::Int32(Some(threshold)))),
				),
			}),
			partition_count: 1,
			key: Vec::new(),
		}),
		order_by: vec![SortExpr {
			expr: Expr::column("avg"),
			descending: true,
			nulls_first: false,
		}],
		limit,
		offset,
	}
}

fn hand_built_ordered_having_grouped_min_max_plan(
	limit: Option<usize>,
	offset: usize,
	threshold: i32,
) -> LogicalPlan {
	LogicalPlan::Sort {
		input: Box::new(LogicalPlan::Exchange {
			input: Box::new(LogicalPlan::Filter {
				input: Box::new(LogicalPlan::Aggregate {
					input: Box::new(LogicalPlan::Exchange {
						input: Box::new(LogicalPlan::ScanNamed {
							table: TableRef::new("sales"),
							projection: None,
						}),
						partition_count: 4,
						key: vec![Expr::column("region")],
					}),
					group_expr: vec![Expr::column("region")],
					aggr_expr: vec![
						(AggregateFunction::Min, vec![Expr::column("amount")]),
						(AggregateFunction::Max, vec![Expr::column("id")]),
					],
				}),
				predicate: Expr::Gt(
					Box::new(Expr::column("min")),
					Box::new(Expr::Literal(ScalarValue::Int32(Some(threshold)))),
				),
			}),
			partition_count: 1,
			key: Vec::new(),
		}),
		order_by: vec![SortExpr {
			expr: Expr::column("max"),
			descending: true,
			nulls_first: false,
		}],
		limit,
		offset,
	}
}

fn project_between_exchanges_sort_plan() -> LogicalPlan {
	LogicalPlan::Sort {
		input: Box::new(LogicalPlan::Exchange {
			input: Box::new(LogicalPlan::Project {
				input: Box::new(LogicalPlan::Exchange {
					input: Box::new(LogicalPlan::ScanNamed {
						table: TableRef::new("sales"),
						projection: None,
					}),
					partition_count: 4,
					key: vec![Expr::column("region")],
				}),
				exprs: vec![Expr::column("region")],
			}),
			partition_count: 1,
			key: Vec::new(),
		}),
		order_by: vec![SortExpr {
			expr: Expr::column("region"),
			descending: false,
			nulls_first: false,
		}],
		limit: None,
		offset: 0,
	}
}

fn filter_without_aggregate_between_exchanges_sort_plan() -> LogicalPlan {
	LogicalPlan::Sort {
		input: Box::new(LogicalPlan::Exchange {
			input: Box::new(LogicalPlan::Filter {
				input: Box::new(LogicalPlan::Project {
					input: Box::new(LogicalPlan::Exchange {
						input: Box::new(LogicalPlan::ScanNamed {
							table: TableRef::new("sales"),
							projection: None,
						}),
						partition_count: 4,
						key: vec![Expr::column("region")],
					}),
					exprs: vec![Expr::column("region")],
				}),
				predicate: Expr::Eq(
					Box::new(Expr::column("region")),
					Box::new(Expr::Literal(ScalarValue::Utf8(Some("east".to_string())))),
				),
			}),
			partition_count: 1,
			key: Vec::new(),
		}),
		order_by: vec![SortExpr {
			expr: Expr::column("region"),
			descending: false,
			nulls_first: false,
		}],
		limit: None,
		offset: 0,
	}
}

fn grouped_rows(results: &[RecordBatch]) -> HashMap<Option<String>, (i64, i64, Option<i64>)> {
	let mut rows = HashMap::new();
	for batch in results {
		let regions = batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("region column");
		let count_all = batch
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("count all column");
		let count_amount = batch
			.column(2)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("count amount column");
		let sums = batch
			.column(3)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("sum column");
		for row in 0..batch.num_rows() {
			let region = if regions.is_null(row) {
				None
			} else {
				Some(regions.value(row).to_string())
			};
			let sum = if sums.is_null(row) {
				None
			} else {
				Some(sums.value(row))
			};
			rows.insert(region, (count_all.value(row), count_amount.value(row), sum));
		}
	}
	rows
}

fn ordered_region_sum_rows(results: &[RecordBatch]) -> Vec<(Option<String>, Option<i64>)> {
	let mut rows = Vec::new();
	for batch in results {
		let regions = batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("region column");
		let sums = batch
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("sum column");
		for row in 0..batch.num_rows() {
			let region = if regions.is_null(row) {
				None
			} else {
				Some(regions.value(row).to_string())
			};
			let sum = if sums.is_null(row) {
				None
			} else {
				Some(sums.value(row))
			};
			rows.push((region, sum));
		}
	}
	rows
}

fn scalar_sum_rows(results: &[RecordBatch]) -> Vec<Option<i64>> {
	let mut rows = Vec::new();
	for batch in results {
		let sums = batch
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("sum column");
		for row in 0..batch.num_rows() {
			rows.push(if sums.is_null(row) {
				None
			} else {
				Some(sums.value(row))
			});
		}
	}
	rows
}

fn scalar_float64_column_rows(results: &[RecordBatch], column_idx: usize) -> Vec<Option<f64>> {
	let mut rows = Vec::new();
	for batch in results {
		let values = batch
			.column(column_idx)
			.as_any()
			.downcast_ref::<Float64Array>()
			.expect("Float64 column");
		for row in 0..batch.num_rows() {
			rows.push(if values.is_null(row) {
				None
			} else {
				Some(values.value(row))
			});
		}
	}
	rows
}

fn scalar_min_amount_max_id_rows(results: &[RecordBatch]) -> Vec<(Option<i32>, Option<i64>)> {
	let mut rows = Vec::new();
	for batch in results {
		let mins = batch
			.column(0)
			.as_any()
			.downcast_ref::<Int32Array>()
			.expect("min amount column");
		let maxes = batch
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("max id column");
		for row in 0..batch.num_rows() {
			let min = if mins.is_null(row) {
				None
			} else {
				Some(mins.value(row))
			};
			let max = if maxes.is_null(row) {
				None
			} else {
				Some(maxes.value(row))
			};
			rows.push((min, max));
		}
	}
	rows
}

fn status_avg_rows(results: &[RecordBatch]) -> Vec<(Option<String>, Option<f64>)> {
	let mut rows = Vec::new();
	for batch in results {
		let statuses = batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("status column");
		let avgs = batch
			.column(1)
			.as_any()
			.downcast_ref::<Float64Array>()
			.expect("avg column");
		for row in 0..batch.num_rows() {
			let status = if statuses.is_null(row) {
				None
			} else {
				Some(statuses.value(row).to_string())
			};
			let avg = if avgs.is_null(row) {
				None
			} else {
				Some(avgs.value(row))
			};
			rows.push((status, avg));
		}
	}
	rows
}

fn region_min_amount_max_id_rows(
	results: &[RecordBatch],
) -> Vec<(Option<String>, Option<i32>, Option<i64>)> {
	let mut rows = Vec::new();
	for batch in results {
		let regions = batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("region column");
		let mins = batch
			.column(1)
			.as_any()
			.downcast_ref::<Int32Array>()
			.expect("min amount column");
		let maxes = batch
			.column(2)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("max id column");
		for row in 0..batch.num_rows() {
			let region = if regions.is_null(row) {
				None
			} else {
				Some(regions.value(row).to_string())
			};
			let min = if mins.is_null(row) {
				None
			} else {
				Some(mins.value(row))
			};
			let max = if maxes.is_null(row) {
				None
			} else {
				Some(maxes.value(row))
			};
			rows.push((region, min, max));
		}
	}
	rows
}

fn status_min_amount_rows(results: &[RecordBatch]) -> Vec<(Option<String>, Option<i32>)> {
	let mut rows = Vec::new();
	for batch in results {
		let statuses = batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("status column");
		let mins = batch
			.column(1)
			.as_any()
			.downcast_ref::<Int32Array>()
			.expect("min amount column");
		for row in 0..batch.num_rows() {
			let status = if statuses.is_null(row) {
				None
			} else {
				Some(statuses.value(row).to_string())
			};
			let min = if mins.is_null(row) {
				None
			} else {
				Some(mins.value(row))
			};
			rows.push((status, min));
		}
	}
	rows
}

fn region_count_rows(results: &[RecordBatch]) -> Vec<(Option<String>, i64)> {
	let mut rows = Vec::new();
	for batch in results {
		let regions = batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("region column");
		let counts = batch
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("count column");
		for row in 0..batch.num_rows() {
			let region = if regions.is_null(row) {
				None
			} else {
				Some(regions.value(row).to_string())
			};
			rows.push((region, counts.value(row)));
		}
	}
	rows
}

fn assert_grouped_aggregate_over_exchange(plan: &LogicalPlan) {
	let LogicalPlan::Aggregate {
		input,
		group_expr,
		aggr_expr,
	} = plan
	else {
		panic!("expected aggregate below terminal exchange");
	};
	assert_eq!(group_expr.len(), 1);
	assert_column(&group_expr[0], "region");
	assert_eq!(aggr_expr.len(), 1);
	let LogicalPlan::Exchange {
		input,
		partition_count,
		key,
	} = input.as_ref()
	else {
		panic!("expected group-keyed exchange below aggregate");
	};
	assert_eq!(*partition_count, 4);
	assert_eq!(key.len(), 1);
	assert_column(&key[0], "region");
	assert!(matches!(input.as_ref(), LogicalPlan::ScanNamed { .. }));
}

fn assert_scalar_sum_aggregate(plan: &LogicalPlan) {
	let LogicalPlan::Aggregate {
		input,
		group_expr,
		aggr_expr,
	} = plan
	else {
		panic!("expected scalar aggregate");
	};
	assert!(group_expr.is_empty());
	assert_eq!(aggr_expr.len(), 1);
	assert!(matches!(aggr_expr[0].0, AggregateFunction::Sum));
	assert_column(&aggr_expr[0].1[0], "amount");
	assert!(matches!(input.as_ref(), LogicalPlan::ScanNamed { .. }));
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

fn assert_unsupported_message(sql: &str, expected: &str) {
	let err = SqlPlanner::new()
		.plan(sql)
		.expect_err("query should be unsupported");
	assert!(
		matches!(&err, FrontendError::Unsupported { message } if message == expected),
		"{sql}: expected unsupported message {expected:?}, got {err}"
	);
}

fn assert_column(expr: &Expr, expected: &str) {
	match expr {
		Expr::Column(column) => assert_eq!(column.name.0, expected),
		other => panic!("expected column {expected}, got {other:?}"),
	}
}

fn assert_qualified_column(expr: &Expr, expected_qualifier: &str, expected_name: &str) {
	match expr {
		Expr::Column(column) => {
			assert_eq!(
				column
					.qualifier
					.as_ref()
					.map(|qualifier| qualifier.table.as_str()),
				Some(expected_qualifier)
			);
			assert_eq!(column.name.0, expected_name);
		}
		other => {
			panic!("expected qualified column {expected_qualifier}.{expected_name}, got {other:?}")
		}
	}
}

fn fragment_config() -> FragmentPlanConfig {
	FragmentPlanConfig::new(
		QueryId::new(),
		TaskBudget::from_query_budget(QueryBudget::test_default()),
	)
}

trait LogicalPlanExt {
	fn is_aggregate(&self) -> bool;
}

impl LogicalPlanExt for LogicalPlan {
	fn is_aggregate(&self) -> bool {
		matches!(self, LogicalPlan::Aggregate { .. })
	}
}

fn fixture() -> (MockCatalog, Arc<FixtureScan>) {
	let sales_schema = sales_schema();
	let empty_sales_schema = sales_schema.clone();
	let regions_schema = regions_schema();
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(
			TableId(1),
			descriptor(1, "sales", sales_schema.as_ref(), "id"),
		)
		.expect("register sales");
	catalog
		.register_descriptor(
			TableId(2),
			descriptor(2, "regions", regions_schema.as_ref(), "region"),
		)
		.expect("register regions");
	catalog
		.register_descriptor(
			TableId(3),
			descriptor(3, "empty_sales", empty_sales_schema.as_ref(), "id"),
		)
		.expect("register empty sales");
	let storage = Arc::new(FixtureScan {
		batches: HashMap::from([
			(TableId(1), sales_batch(sales_schema)),
			(TableId(2), regions_batch(regions_schema)),
			(TableId(3), empty_sales_batch(empty_sales_schema)),
		]),
	});
	(catalog, storage)
}

fn sales_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("region", DataType::Utf8, true),
		Field::new("amount", DataType::Int32, true),
		Field::new("status", DataType::Utf8, true),
		Field::new("price", DataType::Float64, true),
		Field::new("all_null_amount", DataType::Int32, true),
		Field::new("score", DataType::Float64, true),
	]))
}

fn regions_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("region", DataType::Utf8, false),
		Field::new("active", DataType::Int32, false),
	]))
}

fn sales_batch(schema: SchemaRef) -> RecordBatch {
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])),
			Arc::new(StringArray::from(vec![
				Some("east"),
				Some("west"),
				Some("east"),
				Some("east"),
				Some("west"),
				None,
			])),
			Arc::new(Int32Array::from(vec![
				Some(10),
				Some(20),
				None,
				Some(30),
				None,
				Some(7),
			])),
			Arc::new(StringArray::from(vec![
				Some("open"),
				Some("open"),
				Some("closed"),
				Some("open"),
				Some("open"),
				Some("open"),
			])),
			Arc::new(Float64Array::from(vec![
				Some(10.5),
				Some(20.5),
				None,
				Some(30.5),
				None,
				Some(7.5),
			])),
			Arc::new(Int32Array::from(vec![None::<i32>; 6])),
			Arc::new(Float64Array::from(vec![
				Some(1.0),
				Some(f64::NAN),
				Some(3.0),
				Some(5.0),
				None,
				Some(7.0),
			])),
		],
	)
	.expect("sales batch")
}

fn empty_sales_batch(schema: SchemaRef) -> RecordBatch {
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(Vec::<i64>::new())),
			Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
			Arc::new(Int32Array::from(Vec::<Option<i32>>::new())),
			Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
			Arc::new(Float64Array::from(Vec::<Option<f64>>::new())),
			Arc::new(Int32Array::from(Vec::<Option<i32>>::new())),
			Arc::new(Float64Array::from(Vec::<Option<f64>>::new())),
		],
	)
	.expect("empty sales batch")
}

fn regions_batch(schema: SchemaRef) -> RecordBatch {
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(StringArray::from(vec!["east", "west"])),
			Arc::new(Int32Array::from(vec![1, 1])),
		],
	)
	.expect("regions batch")
}

fn descriptor(
	id_byte: u8,
	table: &str,
	schema: &Schema,
	primary_key: &str,
) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new(
		TableObjectId::new([id_byte; 16]),
		1,
		"default",
		table,
		schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single(primary_key)),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.expect("descriptor")
}

struct FixtureScan {
	batches: HashMap<TableId, RecordBatch>,
}

#[async_trait::async_trait]
impl StorageScan for FixtureScan {
	async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
		let mut batch = self
			.batches
			.get(&request.table_id)
			.expect("fixture table")
			.clone();
		if let Some(projected_columns) = request.projected_columns.as_ref() {
			let schema = batch.schema();
			let fields = projected_columns
				.iter()
				.map(|ordinal| schema.field(ordinal.0 as usize).as_ref().clone())
				.collect::<Vec<_>>();
			let columns = projected_columns
				.iter()
				.map(|ordinal| batch.column(ordinal.0 as usize).clone())
				.collect::<Vec<_>>();
			batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
		}
		Ok(ScanStream::from_batches(
			batch.schema(),
			request.session_id,
			request.query_id,
			vec![batch],
		))
	}
}
