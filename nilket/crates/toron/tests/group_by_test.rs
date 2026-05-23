use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Float32Array, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use toron::binder::{PlannerConfig, bind};
use toron::catalog::{MockCatalog, TableRef};
use toron::distributed::budget::{QueryBudget, TaskBudget};
use toron::distributed::determinism::{SortPolicy, canonical_arrow_ipc_digest};
use toron::distributed::fragment::{FragmentError, FragmentPlanConfig, fragment_plan};
use toron::distributed::identity::{ExchangeId, QueryId, StageId};
use toron::distributed::task::{PartitionCount, TaskInputBinding, TaskOutputBinding};
use toron::error::{BindingError, ExecutionError, FrontendError, StorageError};
use toron::exec::Executor;
use toron::ir::{
	AggregateFunction, BoundBinaryOp, BoundExprId, BoundExprNode, BoundExprProgram, ColumnOrdinal,
	ExecutionPlan, Expr, FilterDisposition, LogicalPlan, ScalarValue, SortExpr, TableId,
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
fn group_by_projection_subset_reorder_alias_or_aggregate_first_uses_project() {
	for sql in [
		"SELECT region, COUNT(*) FROM sales GROUP BY region, status",
		"SELECT status, region, COUNT(*) FROM sales GROUP BY region, status",
		"SELECT COUNT(*), region FROM sales GROUP BY region",
		"SELECT region AS r, COUNT(*) FROM sales GROUP BY region",
		"SELECT region, COUNT(*) AS n FROM sales GROUP BY region",
	] {
		assert_project_over_aggregate(&plan_sql(sql));
	}
}

#[tokio::test]
async fn post_aggregate_aliases_bind_to_final_schema_and_order_by() {
	let (catalog, _) = fixture();
	let logical =
		plan_sql("SELECT region AS r, COUNT(*) AS n FROM sales GROUP BY region ORDER BY n DESC");
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind aliased grouped aggregate");
	assert_eq!(bound.schema().field(0).name(), "r");
	assert_eq!(bound.schema().field(1).name(), "n");
	let ExecutionPlan::Sort { order_by, .. } = bound else {
		panic!("expected sort");
	};
	assert_eq!(order_by[0].ordinal, ColumnOrdinal(1));
	assert!(order_by[0].descending);
}

#[tokio::test]
async fn having_only_aggregate_does_not_leak_into_bound_output_schema() {
	let (catalog, _) = fixture();
	let logical = plan_sql("SELECT region FROM sales GROUP BY region HAVING SUM(amount) > 20");
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind hidden HAVING aggregate");
	assert_eq!(bound.schema().fields().len(), 1);
	assert_eq!(bound.schema().field(0).name(), "region");
}

#[tokio::test]
async fn existing_expr_surface_computed_aggregate_projection_binds() {
	let (catalog, _) = fixture();
	let logical = plan_sql("SELECT region, SUM(amount) > 10 AS large FROM sales GROUP BY region");
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind computed aggregate projection");
	assert_eq!(bound.schema().field(0).name(), "region");
	assert_eq!(bound.schema().field(1).name(), "large");
	assert_eq!(bound.schema().field(1).data_type(), &DataType::Boolean);
}

#[tokio::test]
async fn scalar_aggregate_aliases_bind_to_final_schema_and_order_by() {
	let (catalog, _) = fixture();
	let count = bind(
		&plan_sql("SELECT COUNT(*) AS n FROM sales"),
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect("bind scalar aggregate alias");
	assert_eq!(count.schema().field(0).name(), "n");

	let alias_sort = bind(
		&plan_sql("SELECT SUM(amount) AS total FROM sales ORDER BY total"),
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect("bind scalar aggregate ORDER BY alias");
	let ExecutionPlan::Sort { order_by, .. } = alias_sort else {
		panic!("expected scalar alias sort");
	};
	assert_eq!(order_by[0].ordinal, ColumnOrdinal(0));

	let aggregate_call_sort = bind(
		&plan_sql("SELECT SUM(amount) AS total FROM sales ORDER BY SUM(amount)"),
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect("bind scalar aggregate ORDER BY visible aggregate");
	assert_eq!(aggregate_call_sort.schema().field(0).name(), "total");
	let ExecutionPlan::Sort { order_by, .. } = aggregate_call_sort else {
		panic!("expected scalar aggregate-call sort");
	};
	assert_eq!(order_by[0].ordinal, ColumnOrdinal(0));

	let having = bind(
		&plan_sql("SELECT SUM(amount) AS total FROM sales HAVING SUM(amount) > 10"),
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect("bind scalar aggregate alias with HAVING");
	assert_eq!(having.schema().field(0).name(), "total");
}

#[tokio::test]
async fn computed_aggregate_arithmetic_binds_and_executes() {
	let (catalog, _) = fixture();
	for sql in [
		"SELECT SUM(amount) + 1 FROM sales",
		"SELECT region, SUM(amount) * 1.1 FROM sales GROUP BY region",
		"SELECT region, SUM(amount) + COUNT(*) FROM sales GROUP BY region",
		"SELECT SUM(amount + 1) FROM sales",
		"SELECT COUNT(amount + 1) FROM sales",
		"SELECT AVG(price * 2.0) FROM sales",
		"SELECT MIN(amount + id), MAX(amount + id) FROM sales",
		"SELECT COUNT(TRY_CAST(score AS INT)) FROM sales",
		"SELECT MIN(score * 1.0) FROM sales",
	] {
		bind(&plan_sql(sql), &catalog, &PlannerConfig::default())
			.await
			.unwrap_or_else(|err| panic!("{sql}: expected bind success, got {err}"));
	}

	let sum = run_logical(&plan_sql("SELECT SUM(amount + 1) FROM sales")).await;
	assert_eq!(scalar_sum_rows(&sum), vec![Some(71)]);

	let count = run_logical(&plan_sql("SELECT COUNT(amount + 1) FROM sales")).await;
	assert_eq!(scalar_int64_column_rows(&count, 0), vec![Some(4)]);

	let avg = run_logical(&plan_sql("SELECT AVG(price * 2.0) FROM sales")).await;
	assert_eq!(scalar_float64_column_rows(&avg, 0), vec![Some(34.5)]);

	let min_max = run_logical(&plan_sql(
		"SELECT MIN(amount + id), MAX(amount + id) FROM sales",
	))
	.await;
	assert_eq!(scalar_int64_column_rows(&min_max, 0), vec![Some(11)]);
	assert_eq!(scalar_int64_column_rows(&min_max, 1), vec![Some(34)]);

	let count_try_cast =
		run_logical(&plan_sql("SELECT COUNT(TRY_CAST(score AS INT)) FROM sales")).await;
	assert_eq!(scalar_int64_column_rows(&count_try_cast, 0), vec![Some(4)]);

	let min_nan = run_logical(&plan_sql("SELECT MIN(score * 1.0) FROM sales")).await;
	let observed = scalar_float64_column_rows(&min_nan, 0)[0].expect("MIN(score * 1.0)");
	assert!(observed.is_nan());
}

#[test]
fn computed_aggregate_signatures_preserve_structural_identity() {
	let plan = plan_sql(
		"SELECT SUM(amount + id), SUM(id + amount), COUNT(*), COUNT(amount), COUNT(amount + 1) \
		 FROM sales",
	);
	let LogicalPlan::Aggregate {
		group_expr,
		aggr_expr,
		..
	} = plan
	else {
		panic!("expected scalar aggregate");
	};
	assert!(group_expr.is_empty());
	assert_eq!(aggr_expr.len(), 5);
	assert!(matches!(aggr_expr[0].0, AggregateFunction::Sum));
	assert!(matches!(aggr_expr[1].0, AggregateFunction::Sum));
	assert!(matches!(
		&aggr_expr[0].1[0],
		Expr::Add(left, right)
			if matches!(left.as_ref(), Expr::Column(column) if column.name.0 == "amount")
				&& matches!(right.as_ref(), Expr::Column(column) if column.name.0 == "id")
	));
	assert!(matches!(
		&aggr_expr[1].1[0],
		Expr::Add(left, right)
			if matches!(left.as_ref(), Expr::Column(column) if column.name.0 == "id")
				&& matches!(right.as_ref(), Expr::Column(column) if column.name.0 == "amount")
	));
	assert!(matches!(aggr_expr[2].0, AggregateFunction::CountAll));
	assert_column(&aggr_expr[2].1[0], "count");
	assert!(matches!(aggr_expr[3].0, AggregateFunction::Count));
	assert_column(&aggr_expr[3].1[0], "amount");
	assert!(matches!(aggr_expr[4].0, AggregateFunction::Count));
	assert!(matches!(&aggr_expr[4].1[0], Expr::Add(_, _)));
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
		"SELECT region, LINEAR_REGRESS(amount + 1, id) FROM sales GROUP BY region",
		"SELECT region, APPROX_QUANTILE(amount + 1, 0.5) FROM sales GROUP BY region",
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
fn having_aggregate_call_argument_not_in_select_is_hidden_when_names_do_not_collide() {
	let plan =
		plan_sql("SELECT region, COUNT(*) FROM sales GROUP BY region HAVING SUM(amount) > 1000");
	assert_project_over_aggregate(&plan);
	assert_unsupported_message(
		"SELECT region, SUM(amount) FROM sales GROUP BY region HAVING SUM(sales.amount) > 10",
		"qualified HAVING column references are not supported",
	);
}

#[test]
fn order_by_aggregate_call_argument_must_match_projected_aggregate() {
	assert_unsupported_message(
		"SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY SUM(other_amount)",
		"ORDER BY <aggregate-call> requires the aggregate to appear in the SELECT list",
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
fn scalar_aggregate_having_duplicate_signature_is_deduplicated() {
	let plan = plan_sql("SELECT COUNT(*), COUNT(*) FROM events HAVING COUNT(*) > 0");
	assert_project_over_aggregate(&plan);
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
		"ORDER BY <aggregate-call> requires the aggregate to appear in the SELECT list",
	);
}

#[test]
fn scalar_aggregate_having_not_in_select_is_computed_and_dropped() {
	let plan = plan_sql("SELECT COUNT(*) FROM sales HAVING SUM(amount) > 0");
	assert_project_over_aggregate(&plan);
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
fn scalar_aggregate_order_by_computed_aggregate_call_arithmetic_lowers() {
	let plan = plan_sql("SELECT SUM(amount) FROM sales ORDER BY SUM(amount) + 100");
	let LogicalPlan::Project {
		input,
		exprs,
		output_names,
	} = plan
	else {
		panic!("expected final project stripping hidden ORDER BY key");
	};
	assert_eq!(exprs.len(), 1);
	assert_column(&exprs[0], "sum");
	assert_eq!(output_names, vec![Some("sum".to_string())]);
	let LogicalPlan::Sort {
		input, order_by, ..
	} = input.as_ref()
	else {
		panic!("expected scalar aggregate sort");
	};
	assert_column(&order_by[0].expr, "__toron_order_key_0");
	let LogicalPlan::Exchange { input, .. } = input.as_ref() else {
		panic!("expected terminal exchange");
	};
	let LogicalPlan::Project { exprs, .. } = input.as_ref() else {
		panic!("expected hidden sort-key project");
	};
	assert_column(&exprs[0], "sum");
	assert!(matches!(
		&exprs[1],
		Expr::Add(left, right)
			if matches!(left.as_ref(), Expr::Column(column) if column.name.0 == "sum")
				&& matches!(right.as_ref(), Expr::Literal(ScalarValue::Int32(Some(100))))
	));
	assert_unsupported("SELECT SUM(amount) FROM sales ORDER BY -SUM(amount)");
}

#[test]
fn scalar_aggregate_having_experimental_aggregate_rejects() {
	assert_unsupported("SELECT LINEAR_REGRESS(x, y) FROM t HAVING LINEAR_REGRESS(x, y) > 0");
}

#[test]
fn having_aggregate_call_not_in_select_is_computed_and_dropped() {
	let plan = plan_sql("SELECT region FROM sales GROUP BY region HAVING SUM(amount) > 100");
	let LogicalPlan::Project {
		input,
		exprs,
		output_names,
	} = plan
	else {
		panic!("expected project dropping HAVING-only aggregate");
	};
	assert_eq!(exprs.len(), 1);
	assert_eq!(output_names, vec![None]);
	assert_column(&exprs[0], "region");
	let LogicalPlan::Filter { input, predicate } = input.as_ref() else {
		panic!("expected HAVING filter below project");
	};
	assert!(matches!(
		predicate,
		Expr::Gt(left, _)
			if matches!(left.as_ref(), Expr::Column(column) if column.name.0 == "sum")
	));
	let LogicalPlan::Aggregate { aggr_expr, .. } = input.as_ref() else {
		panic!("expected aggregate below filter");
	};
	assert_eq!(aggr_expr.len(), 1);
	assert!(matches!(aggr_expr[0].0, AggregateFunction::Sum));
}

#[test]
fn having_aggregate_call_ambiguity_is_rejected() {
	for sql in [
		"SELECT region, COUNT(*), COUNT(amount) FROM sales GROUP BY region HAVING COUNT(*) > 5",
		"SELECT count, COUNT(amount) FROM events GROUP BY count HAVING COUNT(amount) > 0",
	] {
		assert_unsupported_message(sql, "ambiguous HAVING aggregate-call");
	}
	let duplicate_signature = plan_sql(
		"SELECT region, COUNT(*), COUNT(*) FROM sales GROUP BY region HAVING COUNT(*) > 5",
	);
	assert_project_over_aggregate(&duplicate_signature);
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
fn order_by_computed_aggregate_call_arithmetic_lowers() {
	let plan = plan_sql(
		"SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY SUM(amount) + 100",
	);
	let LogicalPlan::Project {
		input,
		exprs,
		output_names,
	} = plan
	else {
		panic!("expected final project stripping hidden ORDER BY key");
	};
	assert_eq!(exprs.len(), 2);
	assert_column(&exprs[0], "region");
	assert_column(&exprs[1], "sum");
	assert_eq!(
		output_names,
		vec![Some("region".to_string()), Some("sum".to_string())]
	);
	let LogicalPlan::Sort {
		input, order_by, ..
	} = input.as_ref()
	else {
		panic!("expected grouped aggregate sort");
	};
	assert_column(&order_by[0].expr, "__toron_order_key_0");
	let LogicalPlan::Exchange { input, .. } = input.as_ref() else {
		panic!("expected terminal exchange");
	};
	let LogicalPlan::Project { exprs, .. } = input.as_ref() else {
		panic!("expected hidden sort-key project");
	};
	assert_column(&exprs[0], "region");
	assert_column(&exprs[1], "sum");
	assert!(matches!(
		&exprs[2],
		Expr::Add(left, right)
			if matches!(left.as_ref(), Expr::Column(column) if column.name.0 == "sum")
				&& matches!(right.as_ref(), Expr::Literal(ScalarValue::Int32(Some(100))))
	));
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
	let logical = plan_sql("SELECT MIN(amount), MAX(id), MIN(price), MAX(rating32) FROM sales");
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
	assert_eq!(output_schema.field(2).name(), "min");
	assert_eq!(output_schema.field(2).data_type(), &DataType::Float64);
	assert!(output_schema.field(2).is_nullable());
	assert_eq!(output_schema.field(3).name(), "max");
	assert_eq!(output_schema.field(3).data_type(), &DataType::Float32);
	assert!(output_schema.field(3).is_nullable());

	for (sql, expected) in [
		(
			"SELECT MIN(region) FROM sales",
			"MIN expects Int32, Int64, Float32, or Float64 input, got Utf8",
		),
		(
			"SELECT MAX(status) FROM sales",
			"MAX expects Int32, Int64, Float32, or Float64 input, got Utf8",
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
	assert_project_over_aggregate(&plan_sql(
		"SELECT MIN(amount), MIN(amount) FROM sales HAVING MIN(amount) > 0",
	));
	assert_unsupported_message(
		"SELECT MIN(amount) FROM sales ORDER BY MAX(id)",
		"ORDER BY <aggregate-call> requires the aggregate to appear in the SELECT list",
	);
}

#[test]
fn avg_aggregate_rejects_invalid_and_unselected_references() {
	assert_unsupported_message("SELECT AVG() FROM sales", "AVG requires one argument");
	assert_unsupported("SELECT AVG(*) FROM sales");
	assert_unsupported("SELECT AVG(DISTINCT amount) FROM sales");
	assert_project_over_aggregate(&plan_sql(
		"SELECT AVG(amount), AVG(amount) FROM sales HAVING AVG(amount) > 0",
	));
	assert_unsupported_message(
		"SELECT AVG(amount) FROM sales ORDER BY SUM(amount)",
		"ORDER BY <aggregate-call> requires the aggregate to appear in the SELECT list",
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
async fn grouped_aggregate_can_project_only_aggregates_and_drop_group_keys() {
	let (catalog, _) = fixture();
	let one_key_logical = plan_sql("SELECT COUNT(*) FROM sales GROUP BY region");
	let one_key_bound = bind(&one_key_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind aggregate-only projection over one group key");
	assert_eq!(one_key_bound.schema().fields().len(), 1);
	assert_eq!(one_key_bound.schema().field(0).name(), "count");
	let one_key = run_logical(&one_key_logical).await;
	assert_eq!(sorted_int64_column_rows(&one_key, 0), vec![1, 2, 3]);

	let two_key_logical = plan_sql("SELECT COUNT(*) FROM sales GROUP BY region, status");
	let two_key_bound = bind(&two_key_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind aggregate-only projection over two group keys");
	assert_eq!(two_key_bound.schema().fields().len(), 1);
	assert_eq!(two_key_bound.schema().field(0).name(), "count");
	let two_key = run_logical(&two_key_logical).await;
	assert_eq!(sorted_int64_column_rows(&two_key, 0), vec![1, 1, 2, 2]);
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
	assert_binary_column_literal(
		&predicate,
		BoundBinaryOp::Gt,
		"sum",
		ColumnOrdinal(1),
		|value| matches!(value, ScalarValue::Int64(Some(10))),
	);
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
	assert_binary_column_literal(
		&predicate,
		BoundBinaryOp::Gt,
		"avg",
		ColumnOrdinal(1),
		|value| matches!(value, ScalarValue::Float64(Some(value)) if *value == 10.0),
	);
	assert!(matches!(input.as_ref(), ExecutionPlan::Aggregate { .. }));
}

#[tokio::test]
async fn float32_min_having_binds_with_literal_coercion() {
	let (catalog, _) = fixture();
	for (sql, expected) in [
		(
			"SELECT MIN(rating32) FROM sales HAVING MIN(rating32) > 0",
			ScalarValue::Float32(Some(0.0)),
		),
		(
			"SELECT MIN(rating32) FROM sales HAVING MIN(rating32) > 0.0",
			ScalarValue::Float32(Some(0.0)),
		),
	] {
		let logical = plan_sql(sql);
		let bound = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind Float32 MIN HAVING");
		let ExecutionPlan::Filter { predicate, .. } = bound else {
			panic!("expected bound HAVING filter for {sql}");
		};
		assert_binary_column_literal(
			&predicate,
			BoundBinaryOp::Gt,
			"min",
			ColumnOrdinal(0),
			|value| value == &expected,
		);
	}
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
	assert_binary_column_literal(
		&predicate,
		BoundBinaryOp::Eq,
		"region",
		ColumnOrdinal(0),
		|value| matches!(value, ScalarValue::Utf8(Some(value)) if value == "east"),
	);
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
async fn scalar_arithmetic_executes_with_widening_and_division_semantics() {
	let aggregate_results = run_logical(&plan_sql(
		"SELECT SUM(amount) + 1, SUM(amount) / 2, AVG(amount) * 2.0 FROM sales",
	))
	.await;
	assert_eq!(
		scalar_int64_column_rows(&aggregate_results, 0),
		vec![Some(68)]
	);
	assert_eq!(
		scalar_int64_column_rows(&aggregate_results, 1),
		vec![Some(33)]
	);
	assert_eq!(
		scalar_float64_column_rows(&aggregate_results, 2),
		vec![Some(33.5)]
	);

	let integer_zero = run_logical(&plan_sql("SELECT amount / (amount - amount) FROM sales")).await;
	assert_eq!(
		scalar_int32_column_rows(&integer_zero, 0),
		vec![None, None, None, None, None, None]
	);

	let float_zero = run_logical(&plan_sql("SELECT price / 0.0 FROM sales")).await;
	let values = scalar_float64_column_rows(&float_zero, 0);
	assert_eq!(values.len(), 6);
	assert!(values[0].expect("row 0").is_infinite());
	assert!(values[1].expect("row 1").is_infinite());
	assert_eq!(values[2], None);
	assert!(values[3].expect("row 3").is_infinite());
	assert_eq!(values[4], None);
	assert!(values[5].expect("row 5").is_infinite());
}

#[tokio::test]
async fn grouped_post_aggregate_arithmetic_executes() {
	let results = run_logical(&plan_sql(
		"SELECT region, SUM(amount) + COUNT(*) FROM sales GROUP BY region",
	))
	.await;

	assert_eq!(
		region_int64_rows(&results, 1),
		HashMap::from([
			(Some("east".to_string()), Some(43)),
			(Some("west".to_string()), Some(22)),
			(None, Some(8)),
		])
	);
}

#[tokio::test]
async fn grouped_having_arithmetic_over_aggregate_executes() {
	let results = run_logical(&plan_sql(
		"SELECT region, SUM(amount) FROM sales GROUP BY region \
		 HAVING SUM(amount) + 100 > 125",
	))
	.await;

	assert_eq!(
		ordered_region_sum_rows(&results),
		vec![(Some("east".to_string()), Some(40))]
	);
}

#[tokio::test]
async fn arithmetic_bind_injects_only_widening_casts() {
	let (catalog, _) = fixture();
	let bound = bind(
		&plan_sql("SELECT amount + id, rating32 + price FROM sales"),
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect("bind arithmetic widening");

	let ExecutionPlan::Project { exprs, .. } = bound else {
		panic!("expected projection");
	};
	assert_add_with_casted_left(&exprs, 0, DataType::Int64, "amount", "id", DataType::Int64);
	assert_add_with_casted_left(
		&exprs,
		1,
		DataType::Float64,
		"rating32",
		"price",
		DataType::Float64,
	);
}

#[tokio::test]
async fn integer_arithmetic_overflow_is_a_kernel_error() {
	let (catalog, storage) = fixture();
	let bound = bind(
		&plan_sql("SELECT amount + 2147483647 FROM sales"),
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect("bind overflowing arithmetic");

	let err = Executor::new(storage)
		.execute_bound(&bound, None)
		.await
		.expect_err("overflow should fail");
	match err {
		ExecutionError::Kernel { message } => assert!(
			message.starts_with("arithmetic overflow: Add(Int32)"),
			"unexpected overflow message: {message}"
		),
		other => panic!("expected kernel overflow error, got {other:?}"),
	}
}

#[tokio::test]
async fn sql_cast_numeric_primitive_lattice_executes() {
	let results = run_logical(&plan_sql(
		"SELECT \
			CAST(amount AS BIGINT), \
			CAST(id AS INT), \
			CAST(rating32 AS DOUBLE), \
			CAST(price AS REAL) \
		 FROM sales",
	))
	.await;

	assert_eq!(
		scalar_int64_column_rows(&results, 0),
		vec![Some(10), Some(20), None, Some(30), None, Some(7)]
	);
	assert_eq!(
		scalar_int32_column_rows(&results, 1),
		vec![Some(1), Some(2), Some(3), Some(4), Some(5), Some(6)]
	);
	assert_eq!(
		scalar_float64_column_rows(&results, 2),
		vec![Some(1.5), Some(2.5), None, Some(3.5), Some(4.5), Some(0.25)]
	);
	assert_eq!(
		scalar_float32_column_rows(&results, 3),
		vec![
			Some(10.5_f32),
			Some(20.5_f32),
			None,
			Some(30.5_f32),
			None,
			Some(7.5_f32)
		]
	);
}

#[tokio::test]
async fn post_aggregate_cast_executes() {
	let scalar = run_logical(&plan_sql(
		"SELECT CAST(SUM(amount) AS DOUBLE), CAST(COUNT(*) AS DOUBLE) FROM sales",
	))
	.await;
	assert_eq!(scalar_float64_column_rows(&scalar, 0), vec![Some(67.0)]);
	assert_eq!(scalar_float64_column_rows(&scalar, 1), vec![Some(6.0)]);

	let grouped = run_logical(&plan_sql(
		"SELECT region, SUM(amount) FROM sales GROUP BY region \
		 HAVING CAST(SUM(amount) AS INT) > 20",
	))
	.await;
	assert_eq!(
		ordered_region_sum_rows(&grouped),
		vec![(Some("east".to_string()), Some(40))]
	);
}

#[tokio::test]
async fn sql_cast_in_where_and_order_by_executes() {
	let filtered = run_logical(&plan_sql(
		"SELECT amount FROM sales WHERE CAST(amount AS BIGINT) > 10",
	))
	.await;
	assert_eq!(
		scalar_int32_column_rows(&filtered, 0),
		vec![Some(20), Some(30)]
	);

	let ordered = run_logical(&plan_sql(
		"SELECT region, SUM(amount) FROM sales GROUP BY region \
		 ORDER BY CAST(SUM(amount) AS FLOAT32) DESC",
	))
	.await;
	assert_eq!(
		ordered_region_sum_rows(&ordered),
		vec![
			(Some("east".to_string()), Some(40)),
			(Some("west".to_string()), Some(20)),
			(None, Some(7)),
		]
	);
}

#[tokio::test]
async fn sql_try_cast_numeric_primitive_lattice_executes() {
	let results = run_logical(&plan_sql(
		"SELECT \
			TRY_CAST(amount AS BIGINT), \
			SAFE_CAST(price AS DOUBLE), \
			TRY_CAST(price AS INT), \
			TRY_CAST(rating32 AS DOUBLE) \
		 FROM sales",
	))
	.await;

	assert_eq!(
		scalar_int64_column_rows(&results, 0),
		vec![Some(10), Some(20), None, Some(30), None, Some(7)]
	);
	assert_eq!(
		scalar_float64_column_rows(&results, 1),
		vec![Some(10.5), Some(20.5), None, Some(30.5), None, Some(7.5)]
	);
	assert_eq!(
		scalar_int32_column_rows(&results, 2),
		vec![Some(10), Some(20), None, Some(30), None, Some(7)]
	);
	assert_eq!(
		scalar_float64_column_rows(&results, 3),
		vec![Some(1.5), Some(2.5), None, Some(3.5), Some(4.5), Some(0.25)]
	);
}

#[tokio::test]
async fn post_aggregate_try_cast_executes() {
	let scalar = run_logical(&plan_sql(
		"SELECT TRY_CAST(SUM(amount) AS DOUBLE) FROM sales",
	))
	.await;
	assert_eq!(scalar_float64_column_rows(&scalar, 0), vec![Some(67.0)]);

	let grouped = run_logical(&plan_sql(
		"SELECT region, TRY_CAST(SUM(amount) AS INT) FROM sales \
		 GROUP BY region HAVING TRY_CAST(SUM(amount) AS INT) IS NOT NULL \
		 ORDER BY region",
	))
	.await;
	assert_eq!(
		region_int32_rows(&grouped, 1),
		HashMap::from([
			(None, Some(7)),
			(Some("east".to_string()), Some(40)),
			(Some("west".to_string()), Some(20)),
		])
	);
}

#[tokio::test]
async fn order_by_try_cast_aggregate_uses_hidden_projection() {
	let ordered = run_logical(&plan_sql(
		"SELECT region, SUM(amount) FROM sales GROUP BY region \
		 ORDER BY TRY_CAST(SUM(amount) AS FLOAT32) DESC",
	))
	.await;
	assert_eq!(
		ordered_region_sum_rows(&ordered),
		vec![
			(Some("east".to_string()), Some(40)),
			(Some("west".to_string()), Some(20)),
			(None, Some(7)),
		]
	);
}

#[tokio::test]
async fn order_by_computed_aggregate_expressions_execute() {
	for sql in [
		"SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY SUM(amount) + 100 DESC",
		"SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY SUM(amount) * 1.5 DESC",
		"SELECT region, SUM(amount), COUNT(*) FROM sales GROUP BY region ORDER BY SUM(amount) / COUNT(*) DESC",
		"SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY SUM(amount) + 1 DESC, region",
	] {
		let ordered = run_logical(&plan_sql(sql)).await;
		assert_eq!(
			ordered_region_sum_rows(&ordered),
			vec![
				(Some("east".to_string()), Some(40)),
				(Some("west".to_string()), Some(20)),
				(None, Some(7)),
			],
			"{sql}"
		);
	}

	let having = run_logical(&plan_sql(
		"SELECT region, SUM(amount) FROM sales GROUP BY region \
		 HAVING SUM(amount) > 10 ORDER BY SUM(amount) + 1 DESC",
	))
	.await;
	assert_eq!(
		ordered_region_sum_rows(&having),
		vec![
			(Some("east".to_string()), Some(40)),
			(Some("west".to_string()), Some(20)),
		]
	);
}

#[tokio::test]
async fn order_by_alias_and_literal_only_computed_keys_execute() {
	let alias = run_logical(&plan_sql(
		"SELECT region, SUM(amount) AS s FROM sales GROUP BY region ORDER BY s + 1 DESC",
	))
	.await;
	assert_eq!(
		ordered_region_sum_rows(&alias),
		vec![
			(Some("east".to_string()), Some(40)),
			(Some("west".to_string()), Some(20)),
			(None, Some(7)),
		]
	);

	let scalar = run_logical(&plan_sql("SELECT SUM(amount) FROM sales ORDER BY 1 + 1")).await;
	assert_eq!(scalar_sum_rows(&scalar), vec![Some(67)]);

	let grouped = run_logical(&plan_sql(
		"SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY 1 + 1, region",
	))
	.await;
	assert_eq!(
		ordered_region_sum_rows(&grouped),
		vec![
			(Some("east".to_string()), Some(40)),
			(Some("west".to_string()), Some(20)),
			(None, Some(7)),
		]
	);
}

#[test]
fn order_by_computed_aggregate_refs_must_be_visible_outputs() {
	for sql in [
		"SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY amount + 1",
		"SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY sales.region + 1",
	] {
		assert_unsupported_message(
			sql,
			"ORDER BY computed aggregate expression must reference visible output columns",
		);
	}
}

#[test]
fn order_by_computed_aggregate_preserves_structural_rejects() {
	assert_unsupported_message(
		"SELECT region, COUNT(*) FROM sales GROUP BY region ORDER BY SUM(amount) + 1",
		"ORDER BY <aggregate-call> requires the aggregate to appear in the SELECT list",
	);
	assert_unsupported_message(
		"SELECT region, region, SUM(amount) FROM sales GROUP BY region ORDER BY SUM(amount) + 1",
		"ORDER BY computed aggregate expression requires unique output names",
	);
}

#[tokio::test]
async fn grouped_computed_aggregate_arguments_execute_with_having_and_order_by() {
	let grouped = run_logical(&plan_sql(
		"SELECT region, SUM(amount + id) FROM sales GROUP BY region",
	))
	.await;
	assert_eq!(
		region_int64_rows(&grouped, 1),
		HashMap::from([
			(Some("east".to_string()), Some(45)),
			(Some("west".to_string()), Some(22)),
			(None, Some(13)),
		])
	);

	let ordered = run_logical(&plan_sql(
		"SELECT region, SUM(amount + id) FROM sales GROUP BY region \
		 HAVING SUM(amount + id) > 20 ORDER BY SUM(amount + id) DESC",
	))
	.await;
	assert_eq!(
		ordered_region_sum_rows(&ordered),
		vec![
			(Some("east".to_string()), Some(45)),
			(Some("west".to_string()), Some(22)),
		]
	);

	let grouped_try_cast = run_logical(&plan_sql(
		"SELECT region, SUM(TRY_CAST(amount AS BIGINT)) FROM sales GROUP BY region",
	))
	.await;
	assert_eq!(
		region_int64_rows(&grouped_try_cast, 1),
		HashMap::from([
			(Some("east".to_string()), Some(40)),
			(Some("west".to_string()), Some(20)),
			(None, Some(7)),
		])
	);

	let grouped_avg = run_logical(&plan_sql(
		"SELECT region, AVG(price * 1.5) FROM sales GROUP BY region",
	))
	.await;
	assert_eq!(
		region_float64_rows(&grouped_avg, 1),
		HashMap::from([
			(Some("east".to_string()), Some(30.75)),
			(Some("west".to_string()), Some(30.75)),
			(None, Some(11.25)),
		])
	);
}

#[test]
fn aggregate_arguments_reject_nested_aggregates() {
	for sql in [
		"SELECT SUM(COUNT(amount)) FROM sales",
		"SELECT SUM(amount + SUM(id)) FROM sales",
		"SELECT COUNT(AVG(price)) FROM sales",
		"SELECT SUM(CAST(SUM(amount) AS BIGINT)) FROM sales",
	] {
		assert_unsupported_message(sql, "aggregate arguments must not contain aggregate calls");
	}
}

#[test]
fn computed_aggregate_arguments_preserve_output_name_ambiguity() {
	assert_unsupported_message(
		"SELECT SUM(amount + 1), SUM(amount + 2) FROM sales HAVING SUM(amount + 1) > 0",
		"ambiguous HAVING aggregate-call",
	);
}

#[tokio::test]
async fn computed_aggregate_arguments_preserve_binder_domain_errors() {
	let (catalog, _) = fixture();
	let err = bind(
		&plan_sql("SELECT SUM(amount > 0) FROM sales"),
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect_err("SUM over Boolean should reject in binder");
	assert!(
		matches!(
			err,
			BindingError::TypeMismatch { ref message }
				if message == "SUM expects numeric input, got Boolean"
		),
		"unexpected SUM domain error: {err:?}"
	);

	let err = bind(
		&plan_sql("SELECT SUM(CAST(region AS BIGINT)) FROM sales"),
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect_err("inner CAST over Utf8 should reject in binder");
	assert!(
		matches!(
			err,
			BindingError::TypeMismatch { ref message }
				if message == "unsupported CAST: Utf8 to Int64; only Int32, Int64, Float32, and Float64 are supported"
		),
		"unexpected CAST domain error: {err:?}"
	);
}

#[tokio::test]
async fn non_aggregate_computed_order_by_remains_out_of_scope() {
	let (catalog, _) = fixture();
	let logical = plan_sql("SELECT amount FROM sales ORDER BY amount + 1");

	let err = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect_err("non-aggregate computed ORDER BY should remain unsupported");
	assert!(
		matches!(
			&err,
			BindingError::UnsupportedExpression { message }
				if message == "ORDER BY keys must reference output columns or positive ordinals"
		),
		"expected ORDER BY catch-all, got {err}"
	);
}

#[tokio::test]
async fn try_cast_nulls_data_domain_failures() {
	let i64_narrowing = run_logical(&plan_sql(
		"SELECT TRY_CAST(CAST(id + 3000000000 AS BIGINT) AS INT) FROM sales",
	))
	.await;
	assert_eq!(
		scalar_int32_column_rows(&i64_narrowing, 0),
		vec![None, None, None, None, None, None]
	);

	let float_to_int = run_logical(&plan_sql("SELECT TRY_CAST(score AS INT) FROM sales")).await;
	assert_eq!(
		scalar_int32_column_rows(&float_to_int, 0),
		vec![Some(1), None, Some(3), Some(5), None, Some(7)]
	);

	let inf_to_int = run_logical(&plan_sql("SELECT TRY_CAST(price / 0.0 AS INT) FROM sales")).await;
	assert_eq!(
		scalar_int32_column_rows(&inf_to_int, 0),
		vec![None, None, None, None, None, None]
	);

	let float_narrowing = run_logical(&plan_sql(
		"SELECT TRY_CAST(price * 1e40 AS REAL) FROM sales",
	))
	.await;
	assert_eq!(
		scalar_float32_column_rows(&float_narrowing, 0),
		vec![None, None, None, None, None, None]
	);
}

#[tokio::test]
async fn try_cast_float64_to_float32_preserves_nan_and_inf() {
	let score = run_logical(&plan_sql("SELECT TRY_CAST(score AS REAL) FROM sales")).await;
	let rows = scalar_float32_column_rows(&score, 0);
	assert_eq!(rows.len(), 6);
	assert_eq!(rows[0], Some(1.0));
	assert!(rows[1].expect("NaN row").is_nan());
	assert_eq!(rows[2], Some(3.0));
	assert_eq!(rows[3], Some(5.0));
	assert_eq!(rows[4], None);
	assert_eq!(rows[5], Some(7.0));

	let inf = run_logical(&plan_sql("SELECT TRY_CAST(price / 0.0 AS REAL) FROM sales")).await;
	let rows = scalar_float32_column_rows(&inf, 0);
	assert_eq!(rows.len(), 6);
	assert_eq!(rows[0], Some(f32::INFINITY));
	assert_eq!(rows[1], Some(f32::INFINITY));
	assert_eq!(rows[2], None);
	assert_eq!(rows[3], Some(f32::INFINITY));
	assert_eq!(rows[4], None);
	assert_eq!(rows[5], Some(f32::INFINITY));
}

#[tokio::test]
async fn bind_try_cast_preserves_identity_node() {
	let (catalog, _) = fixture();
	let bound = bind(
		&plan_sql("SELECT TRY_CAST(amount AS INT) FROM sales"),
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect("bind identity try cast");

	assert!(
		bound.schema().field(0).is_nullable(),
		"TryCast output schema must be nullable"
	);
	let ExecutionPlan::Project { exprs, .. } = bound else {
		panic!("expected projection");
	};
	assert_cast_root(&exprs, 0, true, DataType::Int32, "amount", DataType::Int32);
}

#[tokio::test]
async fn safe_cast_is_alias_for_try_cast() {
	let try_results = run_logical(&plan_sql("SELECT TRY_CAST(amount AS BIGINT) FROM sales")).await;
	let safe_results =
		run_logical(&plan_sql("SELECT SAFE_CAST(amount AS BIGINT) FROM sales")).await;
	assert_eq!(
		scalar_int64_column_rows(&try_results, 0),
		scalar_int64_column_rows(&safe_results, 0)
	);
}

#[tokio::test]
async fn try_cast_over_strict_cast_propagates_inner_error() {
	let err =
		execute_sql_err("SELECT TRY_CAST(CAST(price * 1000000000.0 AS INT) AS BIGINT) FROM sales")
			.await;
	assert_kernel_error_prefix(err, "cast failed: Float64 to Int32:");
}

#[tokio::test]
async fn bind_explicit_cast_preserves_identity_node() {
	let (catalog, _) = fixture();
	let bound = bind(
		&plan_sql("SELECT CAST(amount AS INT) FROM sales"),
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect("bind identity cast");

	let ExecutionPlan::Project { exprs, .. } = bound else {
		panic!("expected projection");
	};
	assert_cast_root(&exprs, 0, false, DataType::Int32, "amount", DataType::Int32);
}

#[tokio::test]
async fn sql_cast_numeric_errors_are_kernel_errors() {
	let err = execute_sql_err("SELECT CAST(price * 1000000000.0 AS INT) FROM sales").await;
	assert_kernel_error_prefix(err, "cast failed: Float64 to Int32:");

	let err = execute_sql_err("SELECT CAST(price * 1e40 AS REAL) FROM sales").await;
	assert_kernel_error_prefix(err, "cast failed: Float64 to Float32:");

	let err = execute_sql_err("SELECT CAST(score AS INT) FROM sales").await;
	assert_kernel_error_prefix(err, "cast failed: Float64 to Int32: NaN");
}

#[tokio::test]
async fn sql_cast_rejects_unsupported_targets_sources_and_syntax() {
	assert_unsupported_message(
		"SELECT CAST(amount AS FLOAT) FROM sales",
		"unsupported CAST target type: FLOAT (precision-parameter ambiguous; use REAL, DOUBLE PRECISION, FLOAT32, or FLOAT64)",
	);
	assert_unsupported("SELECT CAST(amount AS VARCHAR) FROM sales");
	assert_unsupported_message(
		"SELECT amount::BIGINT FROM sales",
		"unsupported cast syntax: ::; only CAST(... AS ...) is supported",
	);
	assert_unsupported_message(
		"SELECT CAST(amount AS BIGINT FORMAT '999') FROM sales",
		"CAST FORMAT clause is not supported",
	);

	let (catalog, _) = fixture();
	let err = bind(
		&plan_sql("SELECT CAST(region AS BIGINT) FROM sales"),
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect_err("non-numeric cast source should fail binding");
	assert!(
		matches!(
			err,
			BindingError::TypeMismatch { ref message }
				if message == "unsupported CAST: Utf8 to Int64; only Int32, Int64, Float32, and Float64 are supported"
		),
		"unexpected binding error: {err:?}"
	);
}

#[tokio::test]
async fn sql_try_cast_rejects_unsupported_targets_sources_and_syntax() {
	assert_unsupported("SELECT TRY_CAST(amount AS VARCHAR) FROM sales");
	assert_unsupported_message(
		"SELECT TRY_CAST(amount AS FLOAT) FROM sales",
		"unsupported CAST target type: FLOAT (precision-parameter ambiguous; use REAL, DOUBLE PRECISION, FLOAT32, or FLOAT64)",
	);
	assert_unsupported_message(
		"SELECT TRY_CAST(amount AS BIGINT FORMAT '999') FROM sales",
		"CAST FORMAT clause is not supported",
	);
	assert_unsupported_message(
		"SELECT amount::BIGINT FROM sales",
		"unsupported cast syntax: ::; only CAST(... AS ...) is supported",
	);

	let (catalog, _) = fixture();
	let err = bind(
		&plan_sql("SELECT TRY_CAST(region AS BIGINT) FROM sales"),
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect_err("non-numeric try cast source should fail binding");
	assert!(
		matches!(
			err,
			BindingError::TypeMismatch { ref message }
				if message == "unsupported TRY_CAST/SAFE_CAST: Utf8 to Int64; only Int32, Int64, Float32, and Float64 are supported"
		),
		"unexpected binding error: {err:?}"
	);
}

#[tokio::test]
async fn aggregate_argument_cast_and_try_cast_execute() {
	let strict = run_logical(&plan_sql("SELECT SUM(CAST(amount AS BIGINT)) FROM sales")).await;
	assert_eq!(scalar_int64_column_rows(&strict, 0), vec![Some(67)]);

	let fallible = run_logical(&plan_sql(
		"SELECT SUM(TRY_CAST(amount AS BIGINT)) FROM sales",
	))
	.await;
	assert_eq!(scalar_int64_column_rows(&fallible, 0), vec![Some(67)]);

	let safe = run_logical(&plan_sql(
		"SELECT COUNT(SAFE_CAST(id AS BIGINT)) FROM sales",
	))
	.await;
	assert_eq!(scalar_int64_column_rows(&safe, 0), vec![Some(6)]);
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
async fn float_min_max_execute_with_sticky_nan_and_signed_zero() {
	let finite = run_logical(&plan_sql("SELECT MIN(price), MAX(price) FROM sales")).await;
	assert_eq!(scalar_float64_column_rows(&finite, 0), vec![Some(7.5)]);
	assert_eq!(scalar_float64_column_rows(&finite, 1), vec![Some(30.5)]);

	let empty = run_logical(&plan_sql("SELECT MIN(price), MAX(price) FROM empty_sales")).await;
	assert_eq!(scalar_float64_column_rows(&empty, 0), vec![None]);
	assert_eq!(scalar_float64_column_rows(&empty, 1), vec![None]);

	let sticky_nan = run_logical(&plan_sql("SELECT MIN(score), MAX(score) FROM sales")).await;
	let min_score = scalar_float64_column_rows(&sticky_nan, 0);
	let max_score = scalar_float64_column_rows(&sticky_nan, 1);
	assert!(min_score[0].expect("MIN(score)").is_nan());
	assert!(max_score[0].expect("MAX(score)").is_nan());

	let signed_zero = run_logical(&plan_sql("SELECT MIN(zero64), MAX(zero64) FROM sales")).await;
	let min_zero = scalar_float64_column_rows(&signed_zero, 0)[0].expect("MIN(zero64)");
	let max_zero = scalar_float64_column_rows(&signed_zero, 1)[0].expect("MAX(zero64)");
	assert_eq!(min_zero.to_bits(), (-0.0f64).to_bits());
	assert_eq!(max_zero.to_bits(), 0.0f64.to_bits());

	let rating = run_logical(&plan_sql("SELECT MIN(rating32), MAX(rating32) FROM sales")).await;
	assert_eq!(scalar_float32_column_rows(&rating, 0), vec![Some(0.25)]);
	assert_eq!(scalar_float32_column_rows(&rating, 1), vec![Some(4.5)]);
}

#[tokio::test]
async fn float_avg_min_max_same_query_nan_consistency() {
	let results = run_logical(&plan_sql(
		"SELECT AVG(score), MIN(score), MAX(score) FROM sales",
	))
	.await;

	assert!(
		scalar_float64_column_rows(&results, 0)[0]
			.expect("AVG(score)")
			.is_nan()
	);
	assert!(
		scalar_float64_column_rows(&results, 1)[0]
			.expect("MIN(score)")
			.is_nan()
	);
	assert!(
		scalar_float64_column_rows(&results, 2)[0]
			.expect("MAX(score)")
			.is_nan()
	);
}

#[tokio::test]
async fn grouped_float_min_max_execute_with_sticky_nan() {
	let finite = run_logical(&plan_sql(
		"SELECT status, MIN(price), MAX(price) FROM sales GROUP BY status ORDER BY status",
	))
	.await;
	assert_eq!(
		status_min_max_float64_rows(&finite),
		vec![
			(Some("closed".to_string()), None, None),
			(Some("open".to_string()), Some(7.5), Some(30.5)),
		]
	);

	let sticky_nan = run_logical(&plan_sql(
		"SELECT status, MIN(score), MAX(score) FROM sales GROUP BY status ORDER BY status",
	))
	.await;
	let rows = status_min_max_float64_rows(&sticky_nan);
	assert_eq!(rows.len(), 2);
	assert_eq!(rows[0], (Some("closed".to_string()), Some(3.0), Some(3.0)));
	assert_eq!(rows[1].0, Some("open".to_string()));
	assert!(rows[1].1.expect("open MIN(score)").is_nan());
	assert!(rows[1].2.expect("open MAX(score)").is_nan());
}

#[tokio::test]
async fn float32_min_having_literals_execute() {
	for sql in [
		"SELECT MIN(rating32) FROM sales HAVING MIN(rating32) > 0",
		"SELECT MIN(rating32) FROM sales HAVING MIN(rating32) > 0.0",
	] {
		let results = run_logical(&plan_sql(sql)).await;
		assert_eq!(scalar_float32_column_rows(&results, 0), vec![Some(0.25)]);
	}
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
		} if *partition_count == PartitionCount::new(4) && partition_key.root_count() == 1
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
			&& partition_key.root_count() == 1
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
				&& partition_key.is_empty_roots()
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
				&& partition_key.is_empty_roots()
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
				ExecutionPlan::Aggregate { group_expr, .. } if group_expr.is_empty_roots()
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
			&& partition_key.is_empty_roots()
	));
	assert!(matches!(
		&stages[0].tasks[0].spec.plan,
		ExecutionPlan::Exchange {
			input,
			partition_count: 1,
			key,
		} if key.is_empty_roots()
			&& matches!(
				input.as_ref(),
				ExecutionPlan::Aggregate { group_expr, .. } if group_expr.is_empty_roots()
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

async fn execute_sql_err(sql: &str) -> ExecutionError {
	let (catalog, storage) = fixture();
	let bound = bind(&plan_sql(sql), &catalog, &PlannerConfig::default())
		.await
		.expect("bind");
	Executor::new(storage)
		.execute_bound(&bound, None)
		.await
		.expect_err("execute should fail")
}

fn assert_kernel_error_prefix(err: ExecutionError, expected_prefix: &str) {
	match err {
		ExecutionError::Kernel { message } => assert!(
			message.starts_with(expected_prefix),
			"expected kernel message prefix {expected_prefix:?}, got {message:?}"
		),
		other => panic!("expected kernel error, got {other:?}"),
	}
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
				output_names: vec![None],
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
					output_names: vec![None],
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

fn scalar_int32_column_rows(results: &[RecordBatch], column_idx: usize) -> Vec<Option<i32>> {
	let mut rows = Vec::new();
	for batch in results {
		let values = batch
			.column(column_idx)
			.as_any()
			.downcast_ref::<Int32Array>()
			.expect("Int32 column");
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

fn scalar_int64_column_rows(results: &[RecordBatch], column_idx: usize) -> Vec<Option<i64>> {
	let mut rows = Vec::new();
	for batch in results {
		let values = batch
			.column(column_idx)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("Int64 column");
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

fn sorted_int64_column_rows(results: &[RecordBatch], column_idx: usize) -> Vec<i64> {
	let mut rows = scalar_int64_column_rows(results, column_idx)
		.into_iter()
		.map(|value| value.expect("non-null Int64 row"))
		.collect::<Vec<_>>();
	rows.sort_unstable();
	rows
}

fn region_int64_rows(
	results: &[RecordBatch],
	value_column_idx: usize,
) -> HashMap<Option<String>, Option<i64>> {
	let mut rows = HashMap::new();
	for batch in results {
		let regions = batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("region column");
		let values = batch
			.column(value_column_idx)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("Int64 value column");
		for row in 0..batch.num_rows() {
			let region = if regions.is_null(row) {
				None
			} else {
				Some(regions.value(row).to_string())
			};
			let value = if values.is_null(row) {
				None
			} else {
				Some(values.value(row))
			};
			rows.insert(region, value);
		}
	}
	rows
}

fn region_float64_rows(
	results: &[RecordBatch],
	value_column_idx: usize,
) -> HashMap<Option<String>, Option<f64>> {
	let mut rows = HashMap::new();
	for batch in results {
		let regions = batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("region column");
		let values = batch
			.column(value_column_idx)
			.as_any()
			.downcast_ref::<Float64Array>()
			.expect("Float64 value column");
		for row in 0..batch.num_rows() {
			let region = if regions.is_null(row) {
				None
			} else {
				Some(regions.value(row).to_string())
			};
			let value = if values.is_null(row) {
				None
			} else {
				Some(values.value(row))
			};
			rows.insert(region, value);
		}
	}
	rows
}

fn region_int32_rows(
	results: &[RecordBatch],
	value_column_idx: usize,
) -> HashMap<Option<String>, Option<i32>> {
	let mut rows = HashMap::new();
	for batch in results {
		let regions = batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("region column");
		let values = batch
			.column(value_column_idx)
			.as_any()
			.downcast_ref::<Int32Array>()
			.expect("Int32 value column");
		for row in 0..batch.num_rows() {
			let region = if regions.is_null(row) {
				None
			} else {
				Some(regions.value(row).to_string())
			};
			let value = if values.is_null(row) {
				None
			} else {
				Some(values.value(row))
			};
			rows.insert(region, value);
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

fn scalar_float32_column_rows(results: &[RecordBatch], column_idx: usize) -> Vec<Option<f32>> {
	let mut rows = Vec::new();
	for batch in results {
		let values = batch
			.column(column_idx)
			.as_any()
			.downcast_ref::<Float32Array>()
			.expect("Float32 column");
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

fn status_min_max_float64_rows(
	results: &[RecordBatch],
) -> Vec<(Option<String>, Option<f64>, Option<f64>)> {
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
			.downcast_ref::<Float64Array>()
			.expect("min column");
		let maxes = batch
			.column(2)
			.as_any()
			.downcast_ref::<Float64Array>()
			.expect("max column");
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
			let max = if maxes.is_null(row) {
				None
			} else {
				Some(maxes.value(row))
			};
			rows.push((status, min, max));
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

fn assert_project_over_aggregate(plan: &LogicalPlan) {
	let LogicalPlan::Project { input, .. } = plan else {
		panic!("expected project over aggregate shape, got {plan:?}");
	};
	match input.as_ref() {
		LogicalPlan::Aggregate { .. } => {}
		LogicalPlan::Filter { input, .. }
			if matches!(input.as_ref(), LogicalPlan::Aggregate { .. }) => {}
		other => panic!("expected aggregate or filter over aggregate below project, got {other:?}"),
	}
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

fn assert_binary_column_literal(
	program: &BoundExprProgram,
	expected_op: BoundBinaryOp,
	expected_column_name: &str,
	expected_column_ordinal: ColumnOrdinal,
	literal_matches: impl FnOnce(&ScalarValue) -> bool,
) {
	let root = program.single_root().expect("single expression root");
	let BoundExprNode::Binary { op, left, right } = program.node(root).expect("root node") else {
		panic!("expected binary root, got {:?}", program.node(root));
	};
	assert_eq!(op, &expected_op);
	assert_column_node(
		program,
		*left,
		expected_column_name,
		Some(expected_column_ordinal),
		None,
	);
	let BoundExprNode::Literal(literal) = program.node(*right).expect("right node") else {
		panic!(
			"expected literal right child, got {:?}",
			program.node(*right)
		);
	};
	let value = program
		.literals
		.get(*literal as usize)
		.expect("literal slot");
	assert!(literal_matches(value), "unexpected literal value {value:?}");
}

fn assert_add_with_casted_left(
	program: &BoundExprProgram,
	root_index: usize,
	left_cast_type: DataType,
	left_column_name: &str,
	right_column_name: &str,
	right_column_type: DataType,
) {
	let root = program.roots[root_index];
	let BoundExprNode::Binary {
		op: BoundBinaryOp::Add,
		left,
		right,
	} = program.node(root).expect("add root")
	else {
		panic!("expected Add root, got {:?}", program.node(root));
	};
	let BoundExprNode::Cast { input, target } = program.node(*left).expect("left cast") else {
		panic!("expected casted left child, got {:?}", program.node(*left));
	};
	assert_eq!(target, &left_cast_type);
	assert_column_node(program, *input, left_column_name, None, None);
	assert_column_node(
		program,
		*right,
		right_column_name,
		None,
		Some(&right_column_type),
	);
}

fn assert_cast_root(
	program: &BoundExprProgram,
	root_index: usize,
	try_cast: bool,
	target_type: DataType,
	column_name: &str,
	column_type: DataType,
) {
	let root = program.roots[root_index];
	let input = match program.node(root).expect("cast root") {
		BoundExprNode::TryCast { input, target } if try_cast => {
			assert_eq!(target, &target_type);
			*input
		}
		BoundExprNode::Cast { input, target } if !try_cast => {
			assert_eq!(target, &target_type);
			*input
		}
		other => panic!("expected cast root, got {other:?}"),
	};
	assert_column_node(program, input, column_name, None, Some(&column_type));
}

fn assert_column_node(
	program: &BoundExprProgram,
	id: BoundExprId,
	expected_name: &str,
	expected_ordinal: Option<ColumnOrdinal>,
	expected_type: Option<&DataType>,
) {
	let BoundExprNode::Column(column) = program.node(id).expect("column node") else {
		panic!("expected column node, got {:?}", program.node(id));
	};
	assert_eq!(column.name.0, expected_name);
	if let Some(expected_ordinal) = expected_ordinal {
		assert_eq!(column.ordinal, expected_ordinal);
	}
	if let Some(expected_type) = expected_type {
		assert_eq!(&column.data_type, expected_type);
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
		Field::new("rating32", DataType::Float32, true),
		Field::new("zero64", DataType::Float64, true),
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
			Arc::new(Float32Array::from(vec![
				Some(1.5),
				Some(2.5),
				None,
				Some(3.5),
				Some(4.5),
				Some(0.25),
			])),
			Arc::new(Float64Array::from(vec![
				Some(0.0),
				Some(-0.0),
				None,
				Some(0.0),
				None,
				Some(-0.0),
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
			Arc::new(Float32Array::from(Vec::<Option<f32>>::new())),
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
