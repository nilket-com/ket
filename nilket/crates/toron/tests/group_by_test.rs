use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
	Array, Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use toron::binder::{PlannerConfig, bind};
use toron::catalog::{MockCatalog, TableRef};
use toron::distributed::budget::{QueryBudget, TaskBudget};
use toron::distributed::determinism::{SortPolicy, canonical_arrow_ipc_digest};
use toron::distributed::fragment::{FragmentPlanConfig, fragment_plan};
use toron::distributed::identity::{ExchangeId, QueryId, StageId};
use toron::distributed::task::{PartitionCount, TaskInputBinding, TaskOutputBinding};
use toron::error::{BindingError, ExecutionError, FrontendError, StorageError};
use toron::exec::Executor;
use toron::ir::{
	AggregateFunction, BoundBinaryOp, BoundExprId, BoundExprNode, BoundExprProgram,
	CUSTOMER_ID_LOGICAL_TYPE, ColumnOrdinal, ExecutionPlan, Expr, FilterDisposition, JoinKind,
	JoinStrategyIntent, LogicalPlan, ScalarFunctionEvaluation, ScalarFunctionId,
	ScalarFunctionParameter, ScalarValue, SortExpr, SystemTableKind, TableId,
	scalar_function_metadata,
};
use toron::rune_engine::{RuneEngine, RuneSandboxConfig};
use toron::sql_planner::SqlPlanner;
use toron::storage::{ScanRequest, ScanStream, StorageScan};
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, SchemaEvolutionPolicy, TableObjectId,
	TableSchemaDescriptor,
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
		"SELECT region, APPROX_QUANTILE(amount, 0.5) FROM sales GROUP BY region",
		"SELECT region, APPROX_QUANTILE(amount + 1, 0.5) FROM sales GROUP BY region",
	] {
		assert!(plan_sql(sql).is_aggregate(), "{sql}");
	}
	for sql in [
		"SELECT region, LINEAR_REGRESS(amount, id) FROM sales GROUP BY region",
		"SELECT region, LINEAR_REGRESS(amount + 1, id) FROM sales GROUP BY region",
	] {
		assert_unsupported(sql);
	}
}

#[tokio::test]
async fn adr546_stable_quantile_executes_globally_and_grouped_without_experimental_flag() {
	let global = run_logical(&plan_sql(
		"SELECT APPROX_QUANTILE(amount, 0.5) AS p50 FROM sales",
	))
	.await;
	let p50 = global[0]
		.column(0)
		.as_any()
		.downcast_ref::<Float64Array>()
		.expect("global quantile");
	assert_eq!(p50.value(0), 20.0);

	let grouped = run_logical(&plan_sql(
		"SELECT region, APPROX_QUANTILE(amount, 0.5) AS p50 \
		 FROM sales GROUP BY region",
	))
	.await;
	let mut rows = Vec::new();
	for batch in grouped {
		let regions = batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("region");
		let quantiles = batch
			.column(1)
			.as_any()
			.downcast_ref::<Float64Array>()
			.expect("grouped quantile");
		for row in 0..batch.num_rows() {
			rows.push((
				(!regions.is_null(row)).then(|| regions.value(row).to_owned()),
				quantiles.value(row),
			));
		}
	}
	rows.sort_by(|left, right| left.0.cmp(&right.0));
	assert_eq!(
		rows,
		vec![
			(None, 7.0),
			(Some("east".to_string()), 30.0),
			(Some("west".to_string()), 20.0),
		]
	);
}

const ADR547_TUKEY_SQL: &str = "WITH samples AS ( \
       SELECT category, value FROM measurements \
       WHERE category IS NOT NULL AND value IS NOT NULL), \
     quartiles AS ( \
       SELECT category, \
              APPROX_QUANTILE(value, 0.25) AS q1, \
              APPROX_QUANTILE(value, 0.50) AS median, \
              APPROX_QUANTILE(value, 0.75) AS q3 \
       FROM samples GROUP BY category), \
     classified AS ( \
       SELECT s.category, s.value, q.q1, q.median, q.q3, \
              q.q1 - 1.5 * (q.q3 - q.q1) AS lower_fence, \
              q.q3 + 1.5 * (q.q3 - q.q1) AS upper_fence \
       FROM samples s JOIN quartiles q ON s.category = q.category), \
     summaries AS ( \
       SELECT category, \
              MIN(CASE WHEN value >= lower_fence THEN value END) AS whisker_low, \
              MIN(q1) AS q1, MIN(median) AS median, MIN(q3) AS q3, \
              MAX(CASE WHEN value <= upper_fence THEN value END) AS whisker_high \
       FROM classified GROUP BY category), \
     outliers AS ( \
       SELECT category, value AS outlier FROM classified \
       WHERE value < lower_fence OR value > upper_fence) \
   SELECT s.category, s.whisker_low, s.q1, s.median, s.q3, s.whisker_high, o.outlier \
   FROM summaries s LEFT JOIN outliers o ON s.category = o.category \
   ORDER BY s.category, o.outlier";

#[test]
fn adr547_tukey_relation_preserves_fences_whiskers_outliers_and_null_policy() {
	// The canonical five-CTE shape needs the same enlarged harness stack as other deep-plan
	// fixtures. A default test-thread overflow is a harness limit, not a planner lineage result.
	std::thread::Builder::new()
		.name("adr547-tukey-relation".to_string())
		.stack_size(64 * 1024 * 1024)
		.spawn(|| {
			let runtime = tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.expect("ADR 547 runtime");
			runtime.block_on(async {
				let (catalog, storage) = tukey_fixture();
				let logical = SqlPlanner::new()
					.plan(ADR547_TUKEY_SQL)
					.expect("plan canonical Tukey relation");
				let bound = bind(&logical, &catalog, &PlannerConfig::default())
					.await
					.expect("bind canonical Tukey relation");

				let expected_fields = [
					("category", DataType::Utf8, true),
					("whisker_low", DataType::Float64, true),
					("q1", DataType::Float64, true),
					("median", DataType::Float64, true),
					("q3", DataType::Float64, true),
					("whisker_high", DataType::Float64, true),
					("outlier", DataType::Float64, true),
				];
				assert_eq!(bound.schema().fields().len(), expected_fields.len());
				for (field, (name, data_type, nullable)) in
					bound.schema().fields().iter().zip(expected_fields)
				{
					assert_eq!(field.name(), name);
					assert_eq!(field.data_type(), &data_type);
					assert_eq!(field.is_nullable(), nullable);
				}

				let neutral_sql = ADR547_TUKEY_SQL
					.replace(
						"SELECT s.category, s.whisker_low, s.q1, s.median, s.q3, s.whisker_high, o.outlier",
						"SELECT s.category AS key, s.whisker_low AS low, s.q1, s.median, s.q3, \
						 s.whisker_high AS high, o.outlier AS value",
					)
					.replace("ORDER BY s.category, o.outlier", "ORDER BY key, value");
				assert_ne!(neutral_sql, ADR547_TUKEY_SQL);
				let neutral = SqlPlanner::new()
					.plan(&neutral_sql)
					.expect("plan neutral Tukey aliases");
				let neutral = bind(&neutral, &catalog, &PlannerConfig::default())
					.await
					.expect("bind neutral Tukey aliases");
				assert_eq!(
					neutral
						.schema()
						.fields()
						.iter()
						.map(|field| field.name().as_str())
						.collect::<Vec<_>>(),
					vec!["key", "low", "q1", "median", "q3", "high", "value"],
				);

				let results = Executor::new(storage)
					.execute_bound(&bound, None)
					.await
					.expect("execute canonical Tukey relation");
				assert_eq!(
					tukey_rows(&results),
					vec![
						("fence".to_string(), -6.0, 0.0, 2.0, 4.0, 10.0, None),
						(
							"spread".to_string(),
							0.0,
							10.0,
							40.0,
							70.0,
							80.0,
							Some(-100.0)
						),
						(
							"spread".to_string(),
							0.0,
							10.0,
							40.0,
							70.0,
							80.0,
							Some(-100.0)
						),
						(
							"spread".to_string(),
							0.0,
							10.0,
							40.0,
							70.0,
							80.0,
							Some(200.0)
						),
						(
							"spread".to_string(),
							0.0,
							10.0,
							40.0,
							70.0,
							80.0,
							Some(300.0)
						),
						("zero".to_string(), 5.0, 5.0, 5.0, 5.0, 5.0, Some(0.0)),
						("zero".to_string(), 5.0, 5.0, 5.0, 5.0, 5.0, Some(10.0)),
					],
				);
			});
		})
		.expect("spawn ADR 547 harness thread")
		.join()
		.expect("ADR 547 harness thread");
}

#[test]
fn adr546_quantile_parameter_range_refuses_during_planning() {
	for sql in [
		"SELECT APPROX_QUANTILE(amount, -0.1) FROM sales",
		"SELECT APPROX_QUANTILE(amount, 1.1) FROM sales",
		"SELECT APPROX_QUANTILE(amount, NULL) FROM sales",
		"SELECT APPROX_QUANTILE(amount, amount) FROM sales",
		"SELECT APPROX_QUANTILE(amount, NaN) FROM sales",
		"SELECT APPROX_QUANTILE(amount, Infinity) FROM sales",
	] {
		assert_unsupported_message(
			sql,
			"APPROX_QUANTILE requires a finite quantile literal in [0, 1]",
		);
	}
}

#[tokio::test]
async fn adr546_small_exact_oracle_covers_every_physical_type_and_mixed_signs() {
	let results = run_logical(&plan_sql(
		"SELECT \
		 APPROX_QUANTILE(id, 0), APPROX_QUANTILE(id, 0.25), \
		 APPROX_QUANTILE(id, 0.5), APPROX_QUANTILE(id, 0.75), APPROX_QUANTILE(id, 1), \
		 APPROX_QUANTILE(amount, 0), APPROX_QUANTILE(amount, 0.25), \
		 APPROX_QUANTILE(amount, 0.5), APPROX_QUANTILE(amount, 0.75), APPROX_QUANTILE(amount, 1), \
		 APPROX_QUANTILE(rating32, 0), APPROX_QUANTILE(rating32, 0.25), \
		 APPROX_QUANTILE(rating32, 0.5), APPROX_QUANTILE(rating32, 0.75), APPROX_QUANTILE(rating32, 1), \
		 APPROX_QUANTILE(price, 0), APPROX_QUANTILE(price, 0.25), \
		 APPROX_QUANTILE(price, 0.5), APPROX_QUANTILE(price, 0.75), APPROX_QUANTILE(price, 1), \
		 APPROX_QUANTILE(amount - 20, 0.25), APPROX_QUANTILE(price - 20.0, 0.25) \
		 FROM sales",
	))
	.await;
	let batch = &results[0];
	let observed = (0..batch.num_columns())
		.map(|column| {
			batch
				.column(column)
				.as_any()
				.downcast_ref::<Float64Array>()
				.expect("quantile output")
				.value(0)
		})
		.collect::<Vec<_>>();
	assert_eq!(
		observed,
		vec![
			1.0, 2.0, 4.0, 5.0, 6.0, // Int64
			7.0, 10.0, 20.0, 20.0, 30.0, // Int32
			0.25, 1.5, 2.5, 3.5, 4.5, // Float32
			7.5, 10.5, 20.5, 20.5, 30.5, // Float64
			-10.0, -9.5, // mixed-sign canonical ordering
		]
	);
}

#[tokio::test]
async fn adr546_null_type_and_nonfinite_boundaries_refuse_at_the_stated_layers() {
	let all_null = run_logical(&plan_sql(
		"SELECT APPROX_QUANTILE(all_null_amount, 0.5) FROM sales",
	))
	.await;
	assert_eq!(scalar_float64_column_rows(&all_null, 0), vec![None]);
	let empty = run_logical(&plan_sql(
		"SELECT APPROX_QUANTILE(amount, 0.5) FROM empty_sales",
	))
	.await;
	assert_eq!(scalar_float64_column_rows(&empty, 0), vec![None]);

	assert_kernel_error_prefix(
		execute_sql_err("SELECT APPROX_QUANTILE(score, 0.5) FROM sales").await,
		"APPROX_QUANTILE observation refused: float64 optimizer fact scalar must be finite",
	);

	let (catalog, _) = fixture();
	let decimal = plan_sql("SELECT APPROX_QUANTILE(decimal_price, 0.5) FROM sales");
	assert!(matches!(
		bind(&decimal, &catalog, &PlannerConfig::default()).await,
		Err(BindingError::TypeMismatch { .. })
	));
	let logical = LogicalPlan::Aggregate {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("sales"),
			projection: None,
		}),
		group_expr: Vec::new(),
		aggr_expr: vec![(
			AggregateFunction::ApproxQuantile(0.5),
			vec![Expr::LogicalAscription {
				expr: Box::new(Expr::column("id")),
				logical_type: CUSTOMER_ID_LOGICAL_TYPE,
			}],
		)],
	};
	assert!(matches!(
		bind(&logical, &catalog, &PlannerConfig::default()).await,
		Err(BindingError::TypeMismatch { .. })
	));
}

#[tokio::test]
async fn adr546_sql_and_rune_use_the_same_stable_global_and_grouped_kernel() {
	let sql_global = plan_sql("SELECT APPROX_QUANTILE(amount, 0.5) FROM sales");
	let rune_global = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .aggregate([], [approx_quantile(col("amount"), 0.5)])
        }
    "#,
	)
	.await;
	assert_eq!(
		canonical_arrow_ipc_digest(&run_logical(&sql_global).await, SortPolicy::FullRowLex)
			.expect("SQL global quantile digest"),
		canonical_arrow_ipc_digest(&run_logical(&rune_global).await, SortPolicy::FullRowLex)
			.expect("Rune global quantile digest")
	);

	let sql = plan_sql("SELECT region, APPROX_QUANTILE(amount, 0.5) FROM sales GROUP BY region");
	let rune = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .aggregate([col("region")], [approx_quantile(col("amount"), 0.5)])
        }
    "#,
	)
	.await;
	assert_eq!(
		canonical_arrow_ipc_digest(&run_logical(&sql).await, SortPolicy::FullRowLex)
			.expect("SQL quantile digest"),
		canonical_arrow_ipc_digest(&run_logical(&rune).await, SortPolicy::FullRowLex)
			.expect("Rune quantile digest")
	);

	let invalid_rune = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .aggregate([], [approx_quantile(col("amount"), 1.1)])
        }
    "#,
	)
	.await;
	let (catalog, _) = fixture();
	assert!(matches!(
		bind(&invalid_rune, &catalog, &PlannerConfig::default()).await,
		Err(BindingError::UnsupportedExpression { message })
			if message == "APPROX_QUANTILE requires a finite quantile literal in [0, 1]"
	));
}

#[test]
fn group_by_keys_must_be_simple_input_columns() {
	for sql in [
		"SELECT region, COUNT(*) FROM sales GROUP BY 1",
		"SELECT region, COUNT(*) FROM sales GROUP BY region + 1",
	] {
		assert_unsupported(sql);
	}
}

#[test]
fn qualified_group_by_keys_and_projections_lower() {
	plan_sql("SELECT region, COUNT(*) FROM sales GROUP BY sales.region");
	plan_sql("SELECT sales.region, COUNT(*) FROM sales GROUP BY region");
}

#[tokio::test]
async fn group_by_inner_join_counts_matched_rows() {
	let results = run_logical(&plan_sql(
		"SELECT left_dups.label, COUNT(*) \
		 FROM left_dups JOIN right_dups ON left_dups.k = right_dups.k \
		 GROUP BY left_dups.label",
	))
	.await;
	assert_eq!(
		sorted_utf8_int64_rows(&results, 0, 1),
		vec![(Some("A".to_string()), 4), (Some("B".to_string()), 1)]
	);
}

#[tokio::test]
async fn group_by_left_outer_join_counts_null_padded_bucket() {
	let results = run_logical(&plan_sql(
		"SELECT right_dups.tier, COUNT(*) \
		 FROM left_dups LEFT JOIN right_dups ON left_dups.k = right_dups.k \
		 GROUP BY right_dups.tier",
	))
	.await;
	assert_eq!(
		sorted_utf8_int64_rows(&results, 0, 1),
		vec![
			(None, 2),
			(Some("gold".to_string()), 4),
			(Some("silver".to_string()), 1),
		]
	);
}

#[tokio::test]
async fn group_by_right_outer_join_counts_null_padded_bucket() {
	let results = run_logical(&plan_sql(
		"SELECT left_dups.label, COUNT(*) \
		 FROM left_dups RIGHT JOIN right_dups ON left_dups.k = right_dups.k \
		 GROUP BY left_dups.label",
	))
	.await;
	assert_eq!(
		sorted_utf8_int64_rows(&results, 0, 1),
		vec![
			(None, 2),
			(Some("A".to_string()), 4),
			(Some("B".to_string()), 1),
		]
	);
}

#[tokio::test]
async fn group_by_full_outer_join_counts_null_padded_buckets() {
	let results = run_logical(&plan_sql(
		"SELECT right_dups.tier, COUNT(*) \
		 FROM left_dups FULL JOIN right_dups ON left_dups.k = right_dups.k \
		 GROUP BY right_dups.tier",
	))
	.await;
	assert_eq!(
		sorted_utf8_int64_rows(&results, 0, 1),
		vec![
			(None, 2),
			(Some("gold".to_string()), 4),
			(Some("orphan".to_string()), 2),
			(Some("silver".to_string()), 1),
		]
	);
}

#[tokio::test]
async fn group_by_decorrelated_exists_aggregates_after_semi_join() {
	let results = run_logical(&plan_sql(
		"SELECT region, COUNT(*) FROM sales \
		 WHERE EXISTS (SELECT 1 FROM regions WHERE regions.region = sales.region) \
		 GROUP BY region",
	))
	.await;
	assert_eq!(
		region_count_rows(&results),
		vec![(Some("east".to_string()), 3), (Some("west".to_string()), 2)]
	);
}

#[test]
fn group_by_join_composes_with_existing_filter_pushdown() {
	let plan = plan_sql(
		"SELECT left_dups.label, COUNT(*) \
		 FROM left_dups JOIN right_dups ON left_dups.k = right_dups.k \
		 WHERE right_dups.tier = 'gold' \
		 GROUP BY left_dups.label",
	);

	let LogicalPlan::Aggregate { input, .. } = plan else {
		panic!("expected aggregate over join");
	};
	let LogicalPlan::Exchange { input, .. } = input.as_ref() else {
		panic!("expected group-key exchange below aggregate");
	};
	let LogicalPlan::Join { right, .. } = input.as_ref() else {
		panic!("expected join below group-key exchange");
	};
	assert!(
		matches!(right.as_ref(), LogicalPlan::Filter { .. }),
		"right-local WHERE predicate should stay pushed into join input"
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
	let qualified = plan_sql(
		"SELECT region, SUM(amount) FROM sales GROUP BY region HAVING SUM(sales.amount) > 10",
	);
	let LogicalPlan::Filter { input, predicate } = qualified else {
		panic!("expected qualified HAVING aggregate to rewrite to filter");
	};
	assert!(matches!(input.as_ref(), LogicalPlan::Aggregate { .. }));
	assert!(matches!(
		predicate,
		Expr::Gt(left, _)
			if matches!(left.as_ref(), Expr::Column(column) if column.name.0 == "sum")
	));
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

#[tokio::test]
async fn scalar_aggregate_having_unknown_qualified_reference_rejects_at_bind() {
	let logical = plan_sql("SELECT SUM(amount) FROM sales HAVING t.amount > 0");
	let (catalog, _) = fixture();
	let err = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect_err("unknown HAVING qualifier should reject at bind");
	assert!(matches!(
		err,
		BindingError::UnknownTableRef { qualifier } if qualifier == "t"
	));
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
fn having_aggregate_call_resolves_unless_group_key_clash() {
	// ADR 267: distinct same-function HAVING aggregate-calls now resolve by signature (was rejected as
	// ambiguous on the shared natural name).
	let _ = plan_sql(
		"SELECT region, COUNT(*), COUNT(amount) FROM sales GROUP BY region HAVING COUNT(*) > 5",
	);
	let duplicate_signature = plan_sql(
		"SELECT region, COUNT(*), COUNT(*) FROM sales GROUP BY region HAVING COUNT(*) > 5",
	);
	assert_project_over_aggregate(&duplicate_signature);
	// A HAVING aggregate whose natural name clashes with a GROUP BY key is still genuine ambiguity.
	assert_unsupported_message(
		"SELECT count, COUNT(amount) FROM events GROUP BY count HAVING COUNT(amount) > 0",
		"ambiguous HAVING aggregate-call",
	);
}

#[test]
fn order_by_aggregate_call_resolves_unless_group_key_clash() {
	// ADR 267: a distinct same-function ORDER BY aggregate-call resolves by signature to its own unique
	// internal output column (`ORDER BY COUNT(*)` picks the COUNT(*) entry, not COUNT(amount)).
	let _ = plan_sql(
		"SELECT region, COUNT(*), COUNT(amount) FROM sales GROUP BY region ORDER BY COUNT(*)",
	);
	for sql in [
		// Two identical COUNT(*) projection columns produce two equally-named final columns, so ORDER BY is
		// genuinely ambiguous between them (the duplicate is in the final projection, not the entry set).
		"SELECT region, COUNT(*), COUNT(*) FROM sales GROUP BY region ORDER BY COUNT(*)",
		// An aggregate whose natural name clashes with a GROUP BY key stays genuine ambiguity.
		"SELECT count, COUNT(amount) FROM events GROUP BY count ORDER BY COUNT(amount)",
	] {
		assert_unsupported_message(sql, "ambiguous ORDER BY aggregate-call");
	}
}

#[tokio::test]
async fn having_unknown_qualified_column_reference_rejects_at_bind() {
	let logical =
		plan_sql("SELECT region, COUNT(*) FROM sales GROUP BY region HAVING t.region = 'EMEA'");
	let (catalog, _) = fixture();
	let err = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect_err("unknown HAVING qualifier should reject at bind");
	assert!(matches!(
		err,
		BindingError::UnknownTableRef { qualifier } if qualifier == "t"
	));
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
		"SELECT DISTINCT region, COUNT(*) FROM sales GROUP BY region HAVING COUNT(*) > 1",
	);
	assert_unsupported(
		"SELECT region, SUM(amount) FROM sales GROUP BY region HAVING SUM(COUNT(*)) > 5",
	);
	assert_unsupported("SELECT region, COUNT(*) AS n FROM sales GROUP BY region HAVING n > 0");
}

#[tokio::test]
async fn select_distinct_single_and_multi_column_dedups_rows() {
	let single = run_logical(&plan_sql("SELECT DISTINCT region FROM sales")).await;
	assert_eq!(
		sorted_utf8_column_rows(&single, 0),
		vec![None, Some("east".to_string()), Some("west".to_string())]
	);

	let multi = run_logical(&plan_sql("SELECT DISTINCT region, status FROM sales")).await;
	assert_eq!(
		sorted_utf8_pair_rows(&multi, 0, 1),
		vec![
			(None, Some("open".to_string())),
			(Some("east".to_string()), Some("closed".to_string())),
			(Some("east".to_string()), Some("open".to_string())),
			(Some("west".to_string()), Some("open".to_string())),
		]
	);
}

#[tokio::test]
async fn select_distinct_nulls_collapse_to_one() {
	let results = run_logical(&plan_sql("SELECT DISTINCT all_null_amount FROM sales")).await;
	assert_eq!(scalar_int32_column_rows(&results, 0), vec![None]);
}

#[tokio::test]
async fn sql_union_all_preserves_duplicates_and_left_schema() {
	let results = run_logical(&plan_sql(
		"SELECT k AS value FROM left_dups UNION ALL SELECT k FROM right_dups",
	))
	.await;

	assert_eq!(
		sorted_int64_column_rows(&results, 0),
		vec![1, 1, 1, 1, 2, 2, 3, 3, 4, 4]
	);
	assert_eq!(results[0].schema().field(0).name(), "value");
}

#[tokio::test]
async fn sql_union_distinct_deduplicates_union_all_output() {
	let results = run_logical(&plan_sql(
		"SELECT k FROM left_dups UNION SELECT k FROM right_dups",
	))
	.await;

	assert_eq!(sorted_int64_column_rows(&results, 0), vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn sql_intersect_and_except_use_null_equal_set_membership() {
	let intersect = run_logical(&plan_sql(
		"SELECT amount FROM sales INTERSECT SELECT all_null_amount FROM sales",
	))
	.await;
	let except = run_logical(&plan_sql(
		"SELECT all_null_amount FROM sales EXCEPT SELECT amount FROM sales",
	))
	.await;
	let minus = run_logical(&plan_sql(
		"SELECT all_null_amount FROM sales MINUS SELECT amount FROM sales",
	))
	.await;

	assert_eq!(scalar_int32_column_rows(&intersect, 0), vec![None]);
	assert_eq!(row_count(&except), 0);
	assert_eq!(row_count(&minus), 0);
}

#[tokio::test]
async fn intersect_all_is_min_not_distinct() {
	let results = run_logical(&plan_sql(
		"SELECT k FROM left_dups INTERSECT ALL SELECT k FROM right_dups",
	))
	.await;

	assert_eq!(sorted_int64_column_rows(&results, 0), vec![1, 1, 2]);
}

#[tokio::test]
async fn except_all_subtracts_exactly() {
	let results = run_logical(&plan_sql(
		"SELECT k FROM left_dups EXCEPT ALL SELECT k FROM right_dups",
	))
	.await;

	assert_eq!(sorted_int64_column_rows(&results, 0), vec![3, 3]);
}

#[tokio::test]
async fn minus_all_aliases_except_all() {
	let except = run_logical(&plan_sql(
		"SELECT k FROM left_dups EXCEPT ALL SELECT k FROM right_dups",
	))
	.await;
	let minus = run_logical(&plan_sql(
		"SELECT k FROM left_dups MINUS ALL SELECT k FROM right_dups",
	))
	.await;

	assert_eq!(
		sorted_int64_column_rows(&minus, 0),
		sorted_int64_column_rows(&except, 0)
	);
}

#[tokio::test]
async fn intersect_except_all_are_null_equal() {
	let intersect = run_logical(&plan_sql(
		"SELECT all_null_amount FROM sales INTERSECT ALL SELECT amount FROM sales",
	))
	.await;
	let except = run_logical(&plan_sql(
		"SELECT all_null_amount FROM sales EXCEPT ALL SELECT amount FROM sales",
	))
	.await;

	assert_eq!(sorted_int32_optional_rows(&intersect, 0), vec![None, None]);
	assert_eq!(
		sorted_int32_optional_rows(&except, 0),
		vec![None, None, None, None]
	);
}

#[tokio::test]
async fn except_all_zero_count_emits_no_row() {
	let results = run_logical(&plan_sql(
		"SELECT k FROM left_dups EXCEPT ALL SELECT k FROM left_dups",
	))
	.await;

	assert_eq!(row_count(&results), 0);
}

#[tokio::test]
async fn repeat_zero_count_emits_no_row() {
	let (_, storage) = fixture();
	let plan = repeat_test_plan(vec![
		(10, ScalarValue::Int64(Some(0))),
		(20, ScalarValue::Int64(Some(0))),
	]);
	let results = Executor::new(storage)
		.execute_bound(&plan, None)
		.await
		.expect("execute repeat");

	assert_eq!(row_count(&results), 0);
	assert_eq!(results[0].schema().field(0).name(), "value");
}

#[tokio::test]
async fn repeat_rejects_null_and_negative_counts() {
	for (count, expected) in [
		(ScalarValue::Int64(None), "REPEAT count must not be NULL"),
		(
			ScalarValue::Int64(Some(-1)),
			"REPEAT count must not be negative",
		),
	] {
		let (_, storage) = fixture();
		let plan = repeat_test_plan(vec![(10, count)]);
		let err = Executor::new(storage)
			.execute_bound(&plan, None)
			.await
			.expect_err("invalid repeat count should fail");
		match err {
			ExecutionError::InvalidExpression { message } => assert!(
				message.contains(expected),
				"expected {expected:?}, got {message:?}"
			),
			other => panic!("expected invalid expression, got {other:?}"),
		}
	}
}

#[tokio::test]
async fn ordinary_join_stays_null_unequal() {
	let results = run_logical(&plan_sql(
		"SELECT sales.id \
		 FROM sales JOIN sales AS other ON sales.amount = other.all_null_amount",
	))
	.await;

	assert_eq!(row_count(&results), 0);
}

#[test]
fn sql_rejects_deferred_set_operation_forms() {
	assert_unsupported("SELECT k FROM left_dups UNION BY NAME SELECT k FROM right_dups");
	assert_unsupported("SELECT k FROM left_dups INTERSECT ALL BY NAME SELECT k FROM right_dups");
}

#[tokio::test]
async fn select_distinct_star_groups_all_visible_columns() {
	let (catalog, _) = fixture();
	let logical = plan_sql("SELECT DISTINCT * FROM sales");
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind SELECT DISTINCT *");
	assert_eq!(
		field_names(&bound.schema()),
		vec![
			"id",
			"region",
			"amount",
			"status",
			"price",
			"all_null_amount",
			"score",
			"rating32",
			"zero64",
			"decimal_price",
			"decimal_wide",
		]
	);

	let results = run_logical(&logical).await;
	let row_count = results.iter().map(RecordBatch::num_rows).sum::<usize>();
	assert_eq!(row_count, 6, "id must stay part of DISTINCT * grouping");
}

#[tokio::test]
async fn select_distinct_alias_and_expression_preserve_output_schema() {
	let (catalog, _) = fixture();
	let logical = plan_sql("SELECT DISTINCT amount + 1 AS adjusted FROM sales");
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind aliased DISTINCT expression");
	assert_eq!(field_names(&bound.schema()), vec!["adjusted"]);

	let results = run_logical(&logical).await;
	assert_eq!(
		sorted_int32_optional_rows(&results, 0),
		vec![None, Some(8), Some(11), Some(21), Some(31)]
	);
}

#[tokio::test]
async fn select_distinct_duplicate_output_names_group_by_position() {
	let (catalog, _) = fixture();
	let logical = plan_sql("SELECT DISTINCT region AS x, status AS x FROM sales");
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind duplicate-name DISTINCT");
	assert_eq!(field_names(&bound.schema()), vec!["x", "x"]);

	let ExecutionPlan::Aggregate {
		input,
		group_expr,
		aggr_expr,
		..
	} = &bound
	else {
		panic!("expected DISTINCT to bind to aggregate, got {bound:?}");
	};
	assert!(matches!(input.as_ref(), ExecutionPlan::Exchange { .. }));
	assert!(aggr_expr.is_empty());
	assert_column_node(
		group_expr,
		group_expr.roots[0],
		"x",
		Some(ColumnOrdinal(0)),
		Some(&DataType::Utf8),
	);
	assert_column_node(
		group_expr,
		group_expr.roots[1],
		"x",
		Some(ColumnOrdinal(1)),
		Some(&DataType::Utf8),
	);

	let results = run_logical(&logical).await;
	assert_eq!(
		sorted_utf8_pair_rows(&results, 0, 1),
		vec![
			(None, Some("open".to_string())),
			(Some("east".to_string()), Some("closed".to_string())),
			(Some("east".to_string()), Some("open".to_string())),
			(Some("west".to_string()), Some("open".to_string())),
		]
	);
}

#[tokio::test]
async fn select_distinct_where_order_by_limit_compose_in_existing_tail_order() {
	let results = run_logical(&plan_sql(
		"SELECT DISTINCT region FROM sales WHERE region IS NOT NULL ORDER BY region LIMIT 1",
	))
	.await;
	assert_eq!(
		sorted_utf8_column_rows(&results, 0),
		vec![Some("east".to_string())]
	);
}

#[tokio::test]
async fn select_distinct_order_by_non_projected_column_rejects() {
	let (catalog, _) = fixture();
	let logical = plan_sql("SELECT DISTINCT region FROM sales ORDER BY amount");
	let err = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect_err("hidden ORDER BY source column should fail after DISTINCT");
	assert!(
		matches!(err, BindingError::UnknownColumn { ref column } if column == "amount"),
		"expected unknown amount column, got {err:?}"
	);
}

#[tokio::test]
async fn adr553_sql_and_rune_exp_share_a_bound_plan_and_digest() {
	let sql_logical = plan_sql("SELECT EXP(price) FROM sales");
	let rune_logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").project([exp(col("price"))])
        }
    "#,
	)
	.await;

	let (catalog, _) = fixture();
	let sql_bound = bind(&sql_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind SQL EXP");
	let rune_bound = bind(&rune_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind Rune exp");
	assert_eq!(format!("{sql_bound:?}"), format!("{rune_bound:?}"));
	assert_eq!(
		canonical_arrow_ipc_digest(&run_logical(&sql_logical).await, SortPolicy::FullRowLex)
			.expect("SQL EXP digest"),
		canonical_arrow_ipc_digest(&run_logical(&rune_logical).await, SortPolicy::FullRowLex)
			.expect("Rune exp digest")
	);
}

#[tokio::test]
async fn adr553_sql_and_rune_pow_share_a_bound_plan_and_digest() {
	let sql_logical = plan_sql("SELECT POW(price, score) FROM sales");
	let rune_logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").project([power(col("price"), col("score"))])
        }
    "#,
	)
	.await;

	let (catalog, _) = fixture();
	let sql_bound = bind(&sql_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind SQL POW");
	let rune_bound = bind(&rune_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind Rune pow");
	assert_eq!(format!("{sql_bound:?}"), format!("{rune_bound:?}"));
	assert_eq!(
		canonical_arrow_ipc_digest(&run_logical(&sql_logical).await, SortPolicy::FullRowLex)
			.expect("SQL POW digest"),
		canonical_arrow_ipc_digest(&run_logical(&rune_logical).await, SortPolicy::FullRowLex)
			.expect("Rune pow digest")
	);
}

#[tokio::test]
async fn adr553_sql_and_rune_sqrt_share_a_bound_plan_and_digest() {
	let sql_logical = plan_sql("SELECT SQRT(price) FROM sales");
	let rune_logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").project([sqrt(col("price"))])
        }
    "#,
	)
	.await;

	let (catalog, _) = fixture();
	let sql_bound = bind(&sql_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind SQL SQRT");
	let rune_bound = bind(&rune_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind Rune sqrt");
	assert_eq!(format!("{sql_bound:?}"), format!("{rune_bound:?}"));
	assert_eq!(
		canonical_arrow_ipc_digest(&run_logical(&sql_logical).await, SortPolicy::FullRowLex)
			.expect("SQL SQRT digest"),
		canonical_arrow_ipc_digest(&run_logical(&rune_logical).await, SortPolicy::FullRowLex)
			.expect("Rune sqrt digest")
	);
}

/// `ABS` keeps the input's own type. Under a float-only signature every one of
/// these but `price` would be rejected, so this is what separates
/// `UnaryNumericPreserving` from reusing `EXP`'s rule.
#[tokio::test]
async fn adr553_abs_preserves_each_numeric_input_type() {
	let (catalog, _) = fixture();
	for (column, expected) in [
		("amount", DataType::Int32),
		("id", DataType::Int64),
		("rating32", DataType::Float32),
		("price", DataType::Float64),
		("decimal_price", DataType::Decimal128(15, 2)),
	] {
		let logical = plan_sql(&format!("SELECT ABS({column}) FROM sales"));
		let bound = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.unwrap_or_else(|error| panic!("bind ABS({column}): {error:?}"));
		assert_eq!(
			bound.schema().field(0).data_type(),
			&expected,
			"ABS({column}) must preserve its input type"
		);
	}
}

#[tokio::test]
async fn adr553_abs_rejects_a_non_numeric_input() {
	let (catalog, _) = fixture();
	let logical = plan_sql("SELECT ABS(region) FROM sales");
	let err = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect_err("ABS of a string should not bind");
	assert!(
		matches!(err, BindingError::TypeMismatch { ref message } if message.contains("ABS expects a numeric input")),
		"expected a numeric-domain rejection, got {err:?}"
	);
}

/// Mixed float widths are refused rather than promoted. Promotion would be a
/// decision about the engine's coercion rules, and this locks that it has not
/// been made silently inside one function's binder arm.
#[tokio::test]
async fn adr553_pow_rejects_mixed_float_widths() {
	let (catalog, _) = fixture();
	let logical = plan_sql("SELECT POW(price, rating32) FROM sales");
	let err = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect_err("POW across float widths should not bind");
	assert!(
		matches!(err, BindingError::TypeMismatch { ref message } if message.contains("same float type")),
		"expected a mixed-width rejection, got {err:?}"
	);
}

/// Registry arity is enforced at SQL lowering, before binding ever sees the
/// call, so the rejection is a frontend error rather than a type mismatch.
#[tokio::test]
async fn adr553_pow_rejects_a_single_argument() {
	// The alias resolves, but the diagnostic names the canonical function, so a
	// reader is pointed at the ANSI spelling rather than the one they typed.
	assert_unsupported_message(
		"SELECT POW(price) FROM sales",
		"POWER expects exactly two arguments, got 1",
	);
	assert_unsupported_message(
		"SELECT POWER(price) FROM sales",
		"POWER expects exactly two arguments, got 1",
	);
}

#[tokio::test]
async fn adr553_sql_and_rune_ln_share_a_bound_plan_and_digest() {
	let sql_logical = plan_sql("SELECT LN(price) FROM sales");
	let rune_logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").project([ln(col("price"))])
        }
    "#,
	)
	.await;

	let (catalog, _) = fixture();
	let sql_bound = bind(&sql_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind SQL LN");
	let rune_bound = bind(&rune_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind Rune ln");
	assert_eq!(format!("{sql_bound:?}"), format!("{rune_bound:?}"));
	assert_eq!(
		canonical_arrow_ipc_digest(&run_logical(&sql_logical).await, SortPolicy::FullRowLex)
			.expect("SQL LN digest"),
		canonical_arrow_ipc_digest(&run_logical(&rune_logical).await, SortPolicy::FullRowLex)
			.expect("Rune ln digest")
	);
}

/// SQL's optional digit count is Rune's explicit one, the way `lag` spells its
/// offset. `ROUND(x)` and `round(x, lit(0))` must therefore agree exactly.
#[tokio::test]
async fn adr553_sql_round_default_matches_rune_explicit_zero_digits() {
	let sql_logical = plan_sql("SELECT ROUND(price) FROM sales");
	let rune_logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").project([round(col("price"), lit(0))])
        }
    "#,
	)
	.await;

	assert_eq!(
		canonical_arrow_ipc_digest(&run_logical(&sql_logical).await, SortPolicy::FullRowLex)
			.expect("SQL ROUND digest"),
		canonical_arrow_ipc_digest(&run_logical(&rune_logical).await, SortPolicy::FullRowLex)
			.expect("Rune round digest")
	);
}

/// The optional argument is the point of `Between(1, 2)`: both spellings must
/// bind, and the digit count must not widen the output type.
#[tokio::test]
async fn adr553_round_accepts_both_arities_without_changing_output_type() {
	let (catalog, _) = fixture();
	for sql in [
		"SELECT ROUND(price) FROM sales",
		"SELECT ROUND(price, 2) FROM sales",
	] {
		let logical = plan_sql(sql);
		let bound = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.unwrap_or_else(|error| panic!("bind {sql}: {error:?}"));
		assert_eq!(
			bound.schema().field(0).data_type(),
			&DataType::Float64,
			"{sql} must keep the value's own type"
		);
	}

	let narrow = plan_sql("SELECT ROUND(rating32, 1) FROM sales");
	let bound = bind(&narrow, &catalog, &PlannerConfig::default())
		.await
		.expect("bind ROUND over Float32");
	assert_eq!(
		bound.schema().field(0).data_type(),
		&DataType::Float32,
		"an Int32 digit count must not widen a Float32 value"
	);
}

#[tokio::test]
async fn adr553_round_rejects_a_non_integer_digit_count() {
	let (catalog, _) = fixture();
	let logical = plan_sql("SELECT ROUND(price, score) FROM sales");
	let err = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect_err("a float digit count should not bind");
	assert!(
		matches!(err, BindingError::TypeMismatch { ref message } if message.contains("integer digit count")),
		"expected a digit-count rejection, got {err:?}"
	);
}

#[tokio::test]
async fn adr553_round_rejects_a_third_argument() {
	assert_unsupported_message(
		"SELECT ROUND(price, 2, 3) FROM sales",
		"ROUND expects one or two arguments, got 3",
	);
}

/// `CEIL` has its own sqlparser grammar form, so it never reaches the registry's
/// function path. Both spellings must still land on the one registry identifier.
#[tokio::test]
async fn adr553_sql_and_rune_ceil_share_a_bound_plan_and_digest() {
	let sql_logical = plan_sql("SELECT CEIL(price) FROM sales");
	let rune_logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").project([ceil(col("price"))])
        }
    "#,
	)
	.await;

	let (catalog, _) = fixture();
	let sql_bound = bind(&sql_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind SQL CEIL");
	let rune_bound = bind(&rune_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind Rune ceil");
	assert_eq!(format!("{sql_bound:?}"), format!("{rune_bound:?}"));
	assert_eq!(
		canonical_arrow_ipc_digest(&run_logical(&sql_logical).await, SortPolicy::FullRowLex)
			.expect("SQL CEIL digest"),
		canonical_arrow_ipc_digest(&run_logical(&rune_logical).await, SortPolicy::FullRowLex)
			.expect("Rune ceil digest")
	);
}

#[tokio::test]
async fn adr553_sql_and_rune_floor_share_a_bound_plan_and_digest() {
	let sql_logical = plan_sql("SELECT FLOOR(price) FROM sales");
	let rune_logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").project([floor(col("price"))])
        }
    "#,
	)
	.await;

	let (catalog, _) = fixture();
	let sql_bound = bind(&sql_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind SQL FLOOR");
	let rune_bound = bind(&rune_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind Rune floor");
	assert_eq!(format!("{sql_bound:?}"), format!("{rune_bound:?}"));
	assert_eq!(
		canonical_arrow_ipc_digest(&run_logical(&sql_logical).await, SortPolicy::FullRowLex)
			.expect("SQL FLOOR digest"),
		canonical_arrow_ipc_digest(&run_logical(&rune_logical).await, SortPolicy::FullRowLex)
			.expect("Rune floor digest")
	);
}

#[test]
fn adr553_floor_lowers_through_the_registry_after_migration() {
	let floor = plan_sql("SELECT FLOOR(price) FROM sales");
	let LogicalPlan::Project { exprs, .. } = floor else {
		panic!("expected FLOOR projection");
	};
	assert!(
		matches!(
			exprs.as_slice(),
			[Expr::ScalarFunction {
				id: ScalarFunctionId::Floor,
				args,
				..
			}] if matches!(args.as_slice(), [Expr::Column(column)] if column.name.0 == "price")
		),
		"FLOOR must lower through the typed scalar-function registry"
	);
}

#[tokio::test]
async fn adr553_ceil_and_floor_are_both_registry_registrations() {
	let ceil = plan_sql("SELECT CEIL(price) FROM sales");
	let floor = plan_sql("SELECT FLOOR(price) FROM sales");
	assert!(
		format!("{ceil:?}").contains("ScalarFunction"),
		"CEIL must lower through the registry"
	);
	assert!(
		format!("{floor:?}").contains("ScalarFunction"),
		"FLOOR must lower through the registry"
	);
}

#[tokio::test]
async fn adr553_sql_and_rune_sign_share_a_bound_plan_and_digest() {
	let sql_logical = plan_sql("SELECT SIGN(amount) FROM sales");
	let rune_logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").project([sign(col("amount"))])
        }
    "#,
	)
	.await;

	let (catalog, _) = fixture();
	let sql_bound = bind(&sql_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind SQL SIGN");
	let rune_bound = bind(&rune_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind Rune sign");
	assert_eq!(format!("{sql_bound:?}"), format!("{rune_bound:?}"));
	assert_eq!(
		canonical_arrow_ipc_digest(&run_logical(&sql_logical).await, SortPolicy::FullRowLex)
			.expect("SQL SIGN digest"),
		canonical_arrow_ipc_digest(&run_logical(&rune_logical).await, SortPolicy::FullRowLex)
			.expect("Rune sign digest")
	);
}

/// `SIGN` reuses `ABS`'s rule rather than returning a fixed integer, so a
/// decimal's sign is that decimal's own `1`.
#[tokio::test]
async fn adr553_sign_preserves_each_numeric_input_type() {
	let (catalog, _) = fixture();
	for (column, expected) in [
		("amount", DataType::Int32),
		("id", DataType::Int64),
		("rating32", DataType::Float32),
		("price", DataType::Float64),
		("decimal_price", DataType::Decimal128(15, 2)),
	] {
		let logical = plan_sql(&format!("SELECT SIGN({column}) FROM sales"));
		let bound = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.unwrap_or_else(|error| panic!("bind SIGN({column}): {error:?}"));
		assert_eq!(
			bound.schema().field(0).data_type(),
			&expected,
			"SIGN({column}) must preserve its input type"
		);
	}
}

#[tokio::test]
async fn adr553_sql_and_rune_mod_share_a_bound_plan_and_digest() {
	let sql_logical = plan_sql("SELECT MOD(amount, amount) FROM sales");
	let rune_logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").project([modulo(col("amount"), col("amount"))])
        }
    "#,
	)
	.await;

	let (catalog, _) = fixture();
	let sql_bound = bind(&sql_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind SQL MOD");
	let rune_bound = bind(&rune_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind Rune mod");
	assert_eq!(format!("{sql_bound:?}"), format!("{rune_bound:?}"));
	assert_eq!(
		canonical_arrow_ipc_digest(&run_logical(&sql_logical).await, SortPolicy::FullRowLex)
			.expect("SQL MOD digest"),
		canonical_arrow_ipc_digest(&run_logical(&rune_logical).await, SortPolicy::FullRowLex)
			.expect("Rune mod digest")
	);
}

/// The float case is absent on purpose: a floating remainder is its own
/// admission decision, not an implied consequence of admitting `MOD`.
#[tokio::test]
async fn adr553_mod_rejects_float_operands() {
	let (catalog, _) = fixture();
	let logical = plan_sql("SELECT MOD(price, price) FROM sales");
	let err = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect_err("a floating remainder should not bind");
	assert!(
		matches!(err, BindingError::TypeMismatch { ref message } if message.contains("same integer type")),
		"expected an integer-domain rejection, got {err:?}"
	);
}

/// The ANSI spelling and the vendor spelling this engine shipped first must
/// produce the same plan, not merely both resolve.
#[tokio::test]
async fn adr553_power_and_pow_produce_the_same_bound_plan() {
	let (catalog, _) = fixture();
	let ansi = plan_sql("SELECT POWER(price, score) FROM sales");
	let vendor = plan_sql("SELECT POW(price, score) FROM sales");
	let ansi_bound = bind(&ansi, &catalog, &PlannerConfig::default())
		.await
		.expect("bind POWER");
	let vendor_bound = bind(&vendor, &catalog, &PlannerConfig::default())
		.await
		.expect("bind POW");
	assert_eq!(format!("{ansi_bound:?}"), format!("{vendor_bound:?}"));
}

/// `CEIL` reaches the engine through sqlparser's dedicated grammar form while
/// `CEILING` arrives as an ordinary function call, so the two spellings travel
/// different routes and must still converge on one identifier.
#[tokio::test]
async fn adr553_ceiling_and_ceil_produce_the_same_bound_plan() {
	let (catalog, _) = fixture();
	let short = plan_sql("SELECT CEIL(price) FROM sales");
	let long = plan_sql("SELECT CEILING(price) FROM sales");
	let short_bound = bind(&short, &catalog, &PlannerConfig::default())
		.await
		.expect("bind CEIL");
	let long_bound = bind(&long, &catalog, &PlannerConfig::default())
		.await
		.expect("bind CEILING");
	assert_eq!(format!("{short_bound:?}"), format!("{long_bound:?}"));
}

#[tokio::test]
async fn adr553_sql_and_rune_upper_share_a_bound_plan_and_digest() {
	let sql_logical = plan_sql("SELECT UPPER(region) FROM sales");
	let rune_logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").project([upper(col("region"))])
        }
    "#,
	)
	.await;

	let (catalog, _) = fixture();
	let sql_bound = bind(&sql_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind SQL UPPER");
	let rune_bound = bind(&rune_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind Rune upper");
	assert_eq!(format!("{sql_bound:?}"), format!("{rune_bound:?}"));
	assert_eq!(
		canonical_arrow_ipc_digest(&run_logical(&sql_logical).await, SortPolicy::FullRowLex)
			.expect("SQL UPPER digest"),
		canonical_arrow_ipc_digest(&run_logical(&rune_logical).await, SortPolicy::FullRowLex)
			.expect("Rune upper digest")
	);
}

/// `UPPER`, `LOWER`, and `TRIM` return a string; `CHAR_LENGTH` returns an
/// integer drawn from neither the input nor the arguments. That asymmetry is
/// the whole point of the string cut and it is what `StringToInteger` encodes.
#[tokio::test]
async fn adr553_string_functions_type_their_outputs_independently_of_their_inputs() {
	let (catalog, _) = fixture();
	for (sql, expected) in [
		("SELECT UPPER(region) FROM sales", DataType::Utf8),
		("SELECT LOWER(region) FROM sales", DataType::Utf8),
		("SELECT TRIM(region) FROM sales", DataType::Utf8),
		("SELECT CHAR_LENGTH(region) FROM sales", DataType::Int64),
		(
			"SELECT CHARACTER_LENGTH(region) FROM sales",
			DataType::Int64,
		),
	] {
		let logical = plan_sql(sql);
		let bound = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.unwrap_or_else(|error| panic!("bind {sql}: {error:?}"));
		assert_eq!(
			bound.schema().field(0).data_type(),
			&expected,
			"{sql} produced the wrong output type"
		);
	}
}

#[tokio::test]
async fn adr559_rune_to_timestamp_keeps_its_bare_arity_after_becoming_variadic() {
	// Making the Rune spelling variadic must not drop the bare arity ADR 558
	// shipped, so the one-argument call keeps binding and evaluating.
	let sql = run_logical(&plan_sql(
		"SELECT TO_TIMESTAMP('2024-01-01 00:00:00') FROM sales",
	))
	.await;
	let rune = run_logical(
		&rune_plan(
			r#"
        pub fn main(ctx) {
            ctx.table("sales").project([to_timestamp([lit_str("2024-01-01 00:00:00")])])
        }
    "#,
		)
		.await,
	)
	.await;
	assert_eq!(
		canonical_arrow_ipc_digest(&sql, SortPolicy::FullRowLex).expect("SQL digest"),
		canonical_arrow_ipc_digest(&rune, SortPolicy::FullRowLex).expect("Rune digest"),
		"the bare arity must survive the variadic spelling"
	);
}

#[tokio::test]
async fn adr558_temporal_parsers_reach_sql_and_rune_through_the_registry() {
	let strict = run_logical(&plan_sql("SELECT TO_DATE('2024-01-01') FROM sales")).await;
	let dates = strict[0]
		.column(0)
		.as_any()
		.downcast_ref::<arrow::array::Date32Array>()
		.expect("TO_DATE Date32 output");
	assert!(!dates.is_null(0));

	// The TRY spelling answers NULL where the strict one ends the statement,
	// which is the whole reason they are separate registrations.
	let lenient = run_logical(&plan_sql("SELECT TRY_TO_DATE('not a date') FROM sales")).await;
	assert!(
		lenient[0]
			.column(0)
			.as_any()
			.downcast_ref::<arrow::array::Date32Array>()
			.expect("TRY_TO_DATE Date32 output")
			.is_null(0),
		"TRY_TO_DATE must answer NULL rather than fail"
	);
	let strict_error = execute_sql_err("SELECT TO_DATE('not a date') FROM sales").await;
	assert!(
		strict_error.to_string().contains("not a date"),
		"the strict failure must name the offending value, got {strict_error}"
	);

	let rune = run_logical(
		&rune_plan(
			r#"
        pub fn main(ctx) {
            ctx.table("sales").project([to_date(lit_str("2024-01-01"))])
        }
    "#,
		)
		.await,
	)
	.await;
	assert_eq!(
		canonical_arrow_ipc_digest(&strict, SortPolicy::FullRowLex).expect("SQL TO_DATE digest"),
		canonical_arrow_ipc_digest(&rune, SortPolicy::FullRowLex).expect("Rune to_date digest"),
		"SQL and Rune must reach one registry identifier"
	);
}

#[tokio::test]
async fn adr553_first_string_batch_uses_the_predicted_registry_shapes() {
	let ltrim = run_logical(&plan_sql("SELECT LTRIM('xxeast', 'x') FROM sales")).await;
	assert_eq!(
		utf8_column_rows(&ltrim, 0),
		vec![
			Some("east".to_string()),
			Some("east".to_string()),
			Some("east".to_string()),
			Some("east".to_string()),
			Some("east".to_string()),
			Some("east".to_string()),
		]
	);
	let rtrim = run_logical(&plan_sql("SELECT RTRIM('eastxx', 'x') FROM sales")).await;
	assert_eq!(
		utf8_column_rows(&rtrim, 0)[0..1],
		[Some("east".to_string())]
	);

	let concat = run_logical(&plan_sql("SELECT CONCAT(region, '-', status) FROM sales")).await;
	assert_eq!(
		utf8_column_rows(&concat, 0),
		vec![
			Some("east-open".to_string()),
			Some("west-open".to_string()),
			Some("east-closed".to_string()),
			Some("east-open".to_string()),
			Some("west-open".to_string()),
			Some("-open".to_string()),
		],
		"CONCAT ignores NULL operands rather than propagating them"
	);

	let position = run_logical(&plan_sql("SELECT POSITION('a' IN region) FROM sales")).await;
	let positions = position[0]
		.column(0)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("POSITION Int64 output");
	assert_eq!(
		(0..positions.len())
			.map(|row| (!positions.is_null(row)).then(|| positions.value(row)))
			.collect::<Vec<_>>(),
		vec![Some(2), Some(0), Some(2), Some(2), Some(0), None]
	);
	let strpos = run_logical(&plan_sql("SELECT STRPOS(region, 'a') FROM sales")).await;
	let strpos_values = strpos[0]
		.column(0)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("STRPOS Int64 output");
	assert_eq!(strpos_values.value(0), 2);
	assert_eq!(strpos_values.value(1), 0);

	let replace = run_logical(&plan_sql("SELECT REPLACE(region, 'e', 'E') FROM sales")).await;
	assert_eq!(
		utf8_column_rows(&replace, 0),
		vec![
			Some("East".to_string()),
			Some("wEst".to_string()),
			Some("East".to_string()),
			Some("East".to_string()),
			Some("wEst".to_string()),
			None,
		]
	);
}

/// SQL `||` is the strict, NULL-propagating counterpart to `CONCAT`. Chaining
/// must preserve the left-associated expression shape, and Rune exposes the
/// same semantics under the explicit `concat_strict` name rather than taking
/// the NULL-ignoring `concat` name.
#[tokio::test]
async fn adr553_strict_concat_matches_between_sql_and_rune() {
	let sql_logical = plan_sql("SELECT region || '-' || status FROM sales");
	let rune_logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").project([
                concat_strict([
                    concat_strict([col("region"), lit_str("-")]),
                    col("status"),
                ])
            ])
        }
    "#,
	)
	.await;

	let (catalog, _) = fixture();
	let sql_bound = bind(&sql_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind SQL strict concat");
	let rune_bound = bind(&rune_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind Rune strict concat");
	assert_eq!(format!("{sql_bound:?}"), format!("{rune_bound:?}"));

	let rows = run_logical(&sql_logical).await;
	assert_eq!(
		utf8_column_rows(&rows, 0),
		vec![
			Some("east-open".to_string()),
			Some("west-open".to_string()),
			Some("east-closed".to_string()),
			Some("east-open".to_string()),
			Some("west-open".to_string()),
			None,
		],
		"|| propagates NULL through a chained concatenation"
	);
	assert_eq!(
		canonical_arrow_ipc_digest(&rows, SortPolicy::FullRowLex)
			.expect("SQL strict concat digest"),
		canonical_arrow_ipc_digest(&run_logical(&rune_logical).await, SortPolicy::FullRowLex)
			.expect("Rune strict concat digest")
	);
}

#[tokio::test]
async fn adr553_second_batch_uses_the_predicted_existing_signatures() {
	let reverse = run_logical(&plan_sql("SELECT REVERSE(region) FROM sales")).await;
	assert_eq!(
		utf8_column_rows(&reverse, 0),
		vec![
			Some("tsae".to_string()),
			Some("tsew".to_string()),
			Some("tsae".to_string()),
			Some("tsae".to_string()),
			Some("tsew".to_string()),
			None,
		]
	);

	let initcap = run_logical(&plan_sql("SELECT INITCAP('hELLO, wORLD') FROM sales")).await;
	assert_eq!(
		utf8_column_rows(&initcap, 0)[0..1],
		[Some("Hello, World".to_string())]
	);

	let trunc = run_logical(&plan_sql("SELECT TRUNC(0.0 - price) FROM sales")).await;
	let trunc_values = trunc[0]
		.column(0)
		.as_any()
		.downcast_ref::<Float64Array>()
		.expect("TRUNC Float64 output");
	assert_eq!(
		(0..trunc_values.len())
			.map(|row| (!trunc_values.is_null(row)).then(|| trunc_values.value(row)))
			.collect::<Vec<_>>(),
		vec![
			Some(-10.0),
			Some(-20.0),
			None,
			Some(-30.0),
			None,
			Some(-7.0)
		]
	);

	let log = run_logical(&plan_sql(
		"SELECT LOG(CAST(10.0 AS DOUBLE), CAST(1000.0 AS DOUBLE)) FROM sales",
	))
	.await;
	let log_values = log[0]
		.column(0)
		.as_any()
		.downcast_ref::<Float64Array>()
		.expect("LOG Float64 output");
	assert!((log_values.value(0) - 3.0).abs() < 1e-12);

	// These arities remain deliberately unadmitted until their semantics have
	// an explicit registry shape: LOG(value) is base-10, while TRUNC(value,
	// digits) is decimal-place truncation rather than a second float operand.
	assert_unsupported("SELECT LOG(1000.0) FROM sales");
	assert_unsupported("SELECT TRUNC(price, 2) FROM sales");
}

#[tokio::test]
async fn adr553_third_batch_uses_two_new_shapes_for_three_string_functions() {
	let lpad = run_logical(&plan_sql("SELECT LPAD(region, 6, 'xy') FROM sales")).await;
	assert_eq!(
		utf8_column_rows(&lpad, 0),
		vec![
			Some("xyeast".to_string()),
			Some("xywest".to_string()),
			Some("xyeast".to_string()),
			Some("xyeast".to_string()),
			Some("xywest".to_string()),
			None,
		]
	);

	let rpad = run_logical(&plan_sql("SELECT RPAD(region, 6, 'xy') FROM sales")).await;
	assert_eq!(
		utf8_column_rows(&rpad, 0),
		vec![
			Some("eastxy".to_string()),
			Some("westxy".to_string()),
			Some("eastxy".to_string()),
			Some("eastxy".to_string()),
			Some("westxy".to_string()),
			None,
		]
	);

	let truncation = run_logical(&plan_sql(
		"SELECT LPAD('east', 2, 'x'), RPAD('east', 2, 'x') FROM sales",
	))
	.await;
	assert_eq!(
		utf8_column_rows(&truncation, 0)[0..1],
		[Some("ea".to_string())]
	);
	assert_eq!(
		utf8_column_rows(&truncation, 1)[0..1],
		[Some("ea".to_string())]
	);

	let default_fill = run_logical(&plan_sql(
		"SELECT LPAD('east', 6), RPAD('east', 6) FROM sales",
	))
	.await;
	assert_eq!(
		utf8_column_rows(&default_fill, 0)[0..1],
		[Some("  east".to_string())]
	);
	assert_eq!(
		utf8_column_rows(&default_fill, 1)[0..1],
		[Some("east  ".to_string())]
	);

	let split_part = run_logical(&plan_sql("SELECT SPLIT_PART('a/b/c', '/', 2) FROM sales")).await;
	assert_eq!(
		utf8_column_rows(&split_part, 0),
		vec![
			Some("b".to_string()),
			Some("b".to_string()),
			Some("b".to_string()),
			Some("b".to_string()),
			Some("b".to_string()),
			Some("b".to_string()),
		]
	);

	let missing = run_logical(&plan_sql("SELECT SPLIT_PART('a/b', '/', 3) FROM sales")).await;
	assert_eq!(
		utf8_column_rows(&missing, 0),
		vec![Some(String::new()); 6],
		"SPLIT_PART returns an empty string for a missing field"
	);
	let error = execute_sql_err("SELECT SPLIT_PART('a/b', '/', 0) FROM sales").await;
	assert!(
		matches!(error, ExecutionError::InvalidExpression { ref message } if message.contains("part number must be positive")),
		"SPLIT_PART index 0 should be rejected, got {error:?}"
	);
	// The current SQL planner rejects unary-negative integer literals before
	// binding; keep that boundary explicit rather than letting it be accidental.
	// Once unary-minus literals parse, move this expectation to the executor's
	// "part number must be positive" error instead.
	assert_unsupported("SELECT SPLIT_PART('a/b', '/', -1) FROM sales");

	for sql in [
		"SELECT LPAD('x', 0 - amount) FROM sales",
		"SELECT SPLIT_PART('a/b', '/', 0 - amount) FROM sales",
	] {
		let error = execute_sql_err(sql).await;
		assert!(
			matches!(error, ExecutionError::InvalidExpression { ref message } if message.contains("must be") && (message.contains("non-negative") || message.contains("positive"))),
			"runtime negative scalar argument should be rejected for {sql}, got {error:?}"
		);
	}
}

#[tokio::test]
async fn adr553_sql_and_rune_third_batch_share_a_bound_plan_and_digest() {
	let sql_logical = plan_sql(
		"SELECT LPAD(region, 6, 'xy'), RPAD(region, 6, 'xy'), SPLIT_PART('a/b/c', '/', 2) FROM sales",
	);
	let rune_logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").project([
                lpad([col("region"), lit(6), lit_str("xy")]),
                rpad([col("region"), lit(6), lit_str("xy")]),
                split_part(lit_str("a/b/c"), lit_str("/"), lit(2)),
            ])
        }
    "#,
	)
	.await;

	let (catalog, _) = fixture();
	let sql_bound = bind(&sql_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind SQL third batch");
	let rune_bound = bind(&rune_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind Rune third batch");
	assert_eq!(format!("{sql_bound:?}"), format!("{rune_bound:?}"));
	assert_eq!(
		canonical_arrow_ipc_digest(&run_logical(&sql_logical).await, SortPolicy::FullRowLex)
			.expect("SQL third batch digest"),
		canonical_arrow_ipc_digest(&run_logical(&rune_logical).await, SortPolicy::FullRowLex)
			.expect("Rune third batch digest")
	);
}

#[tokio::test]
async fn adr553_string_functions_reject_a_numeric_input() {
	let (catalog, _) = fixture();
	for sql in [
		"SELECT UPPER(price) FROM sales",
		"SELECT CHAR_LENGTH(amount) FROM sales",
	] {
		let logical = plan_sql(sql);
		let err = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.unwrap_err();
		assert!(
			matches!(err, BindingError::TypeMismatch { ref message } if message.contains("expects a string input")),
			"{sql}: expected a string-domain rejection, got {err:?}"
		);
	}
}

/// The specification forms select a side and a character set. The side is
/// non-child metadata, so the two plans must remain structurally distinct all
/// the way through binding rather than collapsing to plain BOTH-space trim.
#[tokio::test]
async fn adr553_trim_specifications_preserve_their_side_in_bound_identity() {
	let (catalog, _) = fixture();
	let leading = bind(
		&plan_sql("SELECT TRIM(LEADING 'x' FROM region) FROM sales"),
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect("bind TRIM LEADING");
	let trailing = bind(
		&plan_sql("SELECT TRIM(TRAILING 'x' FROM region) FROM sales"),
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect("bind TRIM TRAILING");
	assert_ne!(
		format!("{leading:?}"),
		format!("{trailing:?}"),
		"TRIM side metadata must not collapse during binding"
	);
	assert_eq!(leading.schema().field(0).data_type(), &DataType::Utf8);
	assert_eq!(trailing.schema().field(0).data_type(), &DataType::Utf8);

	for sql in [
		"SELECT TRIM(BOTH 'x' FROM region) FROM sales",
		"SELECT TRIM('x' FROM region) FROM sales",
	] {
		bind(&plan_sql(sql), &catalog, &PlannerConfig::default())
			.await
			.unwrap_or_else(|error| panic!("bind {sql}: {error:?}"));
	}
	assert_unsupported_message(
		"SELECT TRIM(region, 'xyz') FROM sales",
		"TRIM with a character list is not supported",
	);
	assert_eq!(
		utf8_column_rows(
			&run_logical(&plan_sql("SELECT TRIM(LEADING 'e' FROM region) FROM sales")).await,
			0,
		),
		vec![
			Some("ast".to_string()),
			Some("west".to_string()),
			Some("ast".to_string()),
			Some("ast".to_string()),
			Some("west".to_string()),
			None,
		],
		"the specification character set must reach the public SQL execution path"
	);
	// The bare specification without a trim character does not parse in this
	// dialect at all, so it never reaches the engine's rejection.
	assert!(
		SqlPlanner::new()
			.plan("SELECT TRIM(LEADING FROM region) FROM sales")
			.is_err(),
		"a bare TRIM specification must not plan"
	);
}

#[tokio::test]
async fn adr553_sql_and_rune_width_bucket_share_a_bound_plan_and_digest() {
	let sql_logical = plan_sql("SELECT WIDTH_BUCKET(price, 0.0, 100.0, 4) FROM sales");
	let rune_logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").project([width_bucket(col("price"), lit_f64(0.0), lit_f64(100.0), lit(4))])
        }
    "#,
	)
	.await;

	let (catalog, _) = fixture();
	let sql_bound = bind(&sql_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind SQL WIDTH_BUCKET");
	let rune_bound = bind(&rune_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind Rune width_bucket");
	assert_eq!(format!("{sql_bound:?}"), format!("{rune_bound:?}"));
	assert_eq!(
		canonical_arrow_ipc_digest(&run_logical(&sql_logical).await, SortPolicy::FullRowLex)
			.expect("SQL WIDTH_BUCKET digest"),
		canonical_arrow_ipc_digest(&run_logical(&rune_logical).await, SortPolicy::FullRowLex)
			.expect("Rune width_bucket digest")
	);
}

/// The bucket ordinal is a count, so it is `Int64` regardless of whether the
/// range is measured in `Float32` or `Float64`.
#[tokio::test]
async fn adr553_width_bucket_returns_an_ordinal_not_a_measurement() {
	let (catalog, _) = fixture();
	for sql in [
		"SELECT WIDTH_BUCKET(price, 0.0, 100.0, 4) FROM sales",
		"SELECT WIDTH_BUCKET(rating32, 0.0, 9.0, 3) FROM sales",
	] {
		let logical = plan_sql(sql);
		let bound = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.unwrap_or_else(|error| panic!("bind {sql}: {error:?}"));
		assert_eq!(
			bound.schema().field(0).data_type(),
			&DataType::Int64,
			"{sql} must produce a bucket ordinal"
		);
	}
}

/// A mixed-width range would let the bucket width depend on which operand won a
/// promotion, so the three range operands must agree.
#[tokio::test]
async fn adr553_width_bucket_rejects_a_mixed_width_range() {
	let (catalog, _) = fixture();
	let logical = plan_sql("SELECT WIDTH_BUCKET(rating32, price, price, 4) FROM sales");
	let err = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect_err("a Float32 operand against Float64 bounds should not bind");
	assert!(
		matches!(err, BindingError::TypeMismatch { ref message } if message.contains("share one float type")),
		"expected a mixed-width rejection, got {err:?}"
	);
}

#[tokio::test]
async fn adr553_width_bucket_rejects_a_wrong_argument_count() {
	assert_unsupported_message(
		"SELECT WIDTH_BUCKET(price, 0.0, 100.0) FROM sales",
		"WIDTH_BUCKET expects exactly four arguments, got 3",
	);
}

/// Rune spells a variadic call as a list, as `project` and `case` already do,
/// so SQL's `GREATEST(a, b, c)` is `greatest([a, b, c])`.
#[tokio::test]
async fn adr553_sql_and_rune_greatest_share_a_bound_plan_and_digest() {
	let sql_logical = plan_sql("SELECT GREATEST(amount, all_null_amount) FROM sales");
	let rune_logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").project([greatest([col("amount"), col("all_null_amount")])])
        }
    "#,
	)
	.await;

	let (catalog, _) = fixture();
	let sql_bound = bind(&sql_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind SQL GREATEST");
	let rune_bound = bind(&rune_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind Rune greatest");
	assert_eq!(format!("{sql_bound:?}"), format!("{rune_bound:?}"));
	assert_eq!(
		canonical_arrow_ipc_digest(&run_logical(&sql_logical).await, SortPolicy::FullRowLex)
			.expect("SQL GREATEST digest"),
		canonical_arrow_ipc_digest(&run_logical(&rune_logical).await, SortPolicy::FullRowLex)
			.expect("Rune greatest digest")
	);
}

/// `AtLeast(2)` is the point of this cut: two arguments and five must both
/// bind, and the output keeps the shared argument type.
#[tokio::test]
async fn adr553_greatest_accepts_any_count_above_its_minimum() {
	let (catalog, _) = fixture();
	for sql in [
		"SELECT LEAST(amount, all_null_amount) FROM sales",
		"SELECT GREATEST(amount, all_null_amount, amount, all_null_amount, amount) FROM sales",
	] {
		let logical = plan_sql(sql);
		let bound = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.unwrap_or_else(|error| panic!("bind {sql}: {error:?}"));
		assert_eq!(
			bound.schema().field(0).data_type(),
			&DataType::Int32,
			"{sql} must keep the shared argument type"
		);
	}
}

#[tokio::test]
async fn adr553_greatest_rejects_a_single_argument() {
	assert_unsupported_message(
		"SELECT GREATEST(amount) FROM sales",
		"GREATEST expects at least two arguments, got 1",
	);
}

#[tokio::test]
async fn adr553_greatest_rejects_mixed_argument_types() {
	let (catalog, _) = fixture();
	let logical = plan_sql("SELECT GREATEST(amount, price) FROM sales");
	let err = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect_err("comparing Int32 against Float64 should not bind");
	assert!(
		matches!(err, BindingError::TypeMismatch { ref message } if message.contains("share one type")),
		"expected a mixed-type rejection, got {err:?}"
	);
}

/// `RANDOM()` is the registry's per-call non-deterministic entry. Its rows cannot
/// be digest-compared across surfaces — that is the whole point — so the
/// equivalence assertion is on the bound plan alone.
#[tokio::test]
async fn adr553_sql_and_rune_random_share_a_bound_plan() {
	let sql_logical = plan_sql("SELECT RANDOM() FROM sales");
	let rune_logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").project([random()])
        }
    "#,
	)
	.await;

	let (catalog, _) = fixture();
	let sql_bound = bind(&sql_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind SQL RANDOM");
	let rune_bound = bind(&rune_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind Rune random");
	assert_eq!(format!("{sql_bound:?}"), format!("{rune_bound:?}"));
	assert_eq!(sql_bound.schema().field(0).data_type(), &DataType::Float64);
}

#[tokio::test]
async fn adr553_random_is_the_registry_s_first_non_deterministic_entry() {
	let non_deterministic = Expr::ScalarFunction {
		id: ScalarFunctionId::Random,
		args: Vec::new(),
		parameter: ScalarFunctionParameter::None,
	};
	assert!(
		!non_deterministic.is_deterministic(),
		"RANDOM must report itself non-deterministic"
	);
	// Non-determinism is infectious through argument position: an otherwise
	// pure function carrying RANDOM cannot be treated as pure either.
	let carrier = Expr::ScalarFunction {
		id: ScalarFunctionId::Abs,
		args: vec![non_deterministic],
		parameter: ScalarFunctionParameter::None,
	};
	assert!(!carrier.is_deterministic(), "ABS(RANDOM()) is not pure");
	assert!(
		Expr::ScalarFunction {
			id: ScalarFunctionId::Abs,
			args: vec![Expr::column("amount")],
			parameter: ScalarFunctionParameter::None,
		}
		.is_deterministic(),
		"an ordinary registration stays deterministic"
	);
}

#[tokio::test]
async fn adr556_now_is_statement_scoped_but_not_deterministic() {
	let now = Expr::ScalarFunction {
		id: ScalarFunctionId::Now,
		args: Vec::new(),
		parameter: ScalarFunctionParameter::None,
	};
	assert!(
		!now.is_deterministic(),
		"NOW must not be treated as a pure expression"
	);
	assert_eq!(
		scalar_function_metadata(ScalarFunctionId::Now).evaluation,
		ScalarFunctionEvaluation::PerStatement
	);
}

/// Non-determinism has to survive nesting. Before `RANDOM()` existed nothing
/// could reach these paths, and `is_deterministic` answered `true` for every
/// form but a bare call — so `RANDOM() < 0.5` read as pure and the rules that
/// gate on it would have pushed or duplicated it.
#[tokio::test]
async fn adr553_non_determinism_survives_every_expression_form() {
	let random = Expr::ScalarFunction {
		id: ScalarFunctionId::Random,
		args: Vec::new(),
		parameter: ScalarFunctionParameter::None,
	};
	let column = || Box::new(Expr::column("amount"));
	let impure = [
		Expr::Lt(Box::new(random.clone()), column()),
		Expr::Add(Box::new(random.clone()), column()),
		Expr::And(Box::new(random.clone()), column()),
		Expr::Not(Box::new(random.clone())),
		Expr::IsNull(Box::new(random.clone())),
		Expr::Cast(Box::new(random.clone()), DataType::Float32),
		Expr::InList {
			expr: column(),
			list: vec![random.clone()],
			negated: false,
		},
		Expr::Case {
			when_then: vec![(Box::new(random.clone()), column())],
			else_expr: None,
		},
		Expr::Case {
			when_then: vec![(column(), column())],
			else_expr: Some(Box::new(random.clone())),
		},
	];
	for expr in impure {
		assert!(
			!expr.is_deterministic(),
			"non-determinism must not be hidden by nesting: {expr:?}"
		);
	}

	// The same shapes without RANDOM stay pure, so the assertions above are
	// detecting the nested call rather than the wrapper.
	let pure = [
		Expr::Lt(column(), column()),
		Expr::Not(column()),
		Expr::Case {
			when_then: vec![(column(), column())],
			else_expr: Some(column()),
		},
	];
	for expr in pure {
		assert!(expr.is_deterministic(), "{expr:?} has nothing impure in it");
	}
}

/// SUBSTRING had no Rune spelling before the migration. Registering it closes
/// that gap, and the two surfaces must agree on the bound plan and the rows.
#[tokio::test]
async fn adr553_sql_and_rune_substring_share_a_bound_plan_and_digest() {
	let sql_logical = plan_sql("SELECT SUBSTRING(region FROM 2 FOR 3) FROM sales");
	let rune_logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").project([substring(col("region"), lit_i64(2), lit_i64(3))])
        }
    "#,
	)
	.await;

	let (catalog, _) = fixture();
	let sql_bound = bind(&sql_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind SQL SUBSTRING");
	let rune_bound = bind(&rune_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind Rune substring");
	assert_eq!(format!("{sql_bound:?}"), format!("{rune_bound:?}"));
	assert_eq!(
		canonical_arrow_ipc_digest(&run_logical(&sql_logical).await, SortPolicy::FullRowLex)
			.expect("SQL SUBSTRING digest"),
		canonical_arrow_ipc_digest(&run_logical(&rune_logical).await, SortPolicy::FullRowLex)
			.expect("Rune substring digest")
	);
}

/// The bounds are constant argument positions. A column in a bound position
/// binds structurally but must be refused, because the executor reads the bound
/// once rather than per row — this is the rule that makes the parameter model
/// safe, and nothing else in the registry has one.
#[tokio::test]
async fn adr553_substring_rejects_a_non_constant_bound() {
	let (catalog, _) = fixture();
	let logical = LogicalPlan::Project {
		input: Box::new(LogicalPlan::ScanNamed {
			table: toron::catalog::TableRef::new("sales"),
			projection: None,
		}),
		exprs: vec![Expr::ScalarFunction {
			id: ScalarFunctionId::Substring,
			args: vec![Expr::column("region"), Expr::column("amount")],
			parameter: ScalarFunctionParameter::None,
		}],
		output_names: vec![None],
	};
	let err = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect_err("a column bound should not bind");
	assert!(
		matches!(err, BindingError::TypeMismatch { ref message } if message.contains("must be an integer literal")),
		"expected a constant-bound rejection, got {err:?}"
	);
}

#[tokio::test]
async fn adr553_random_rejects_arguments() {
	assert_unsupported_message(
		"SELECT RANDOM(price) FROM sales",
		"RANDOM expects no arguments, got 1",
	);
}

#[tokio::test]
async fn rune_distinct_matches_sql_distinct_plan_and_rows() {
	let sql_logical = plan_sql("SELECT DISTINCT region, status FROM sales");
	let rune_logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .project([col("region"), col("status")])
               .distinct()
        }
    "#,
	)
	.await;

	let (catalog, _) = fixture();
	let sql_bound = bind(&sql_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind SQL DISTINCT");
	let rune_bound = bind(&rune_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind Rune DISTINCT");
	assert_eq!(format!("{sql_bound:?}"), format!("{rune_bound:?}"));
	assert_eq!(
		canonical_arrow_ipc_digest(&run_logical(&sql_logical).await, SortPolicy::FullRowLex)
			.expect("sql digest"),
		canonical_arrow_ipc_digest(&run_logical(&rune_logical).await, SortPolicy::FullRowLex)
			.expect("rune digest")
	);

	let rune_star = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").distinct()
        }
    "#,
	)
	.await;
	assert_eq!(
		canonical_arrow_ipc_digest(
			&run_logical(&plan_sql("SELECT DISTINCT * FROM sales")).await,
			SortPolicy::FullRowLex
		)
		.expect("sql star digest"),
		canonical_arrow_ipc_digest(&run_logical(&rune_star).await, SortPolicy::FullRowLex)
			.expect("rune star digest")
	);
}

#[tokio::test]
async fn select_distinct_inner_join_dedups_projected_join_rows() {
	let results = run_logical(&plan_sql(
		"SELECT DISTINCT left_dups.label, right_dups.tier \
		 FROM left_dups JOIN right_dups ON left_dups.k = right_dups.k",
	))
	.await;
	assert_eq!(
		sorted_utf8_pair_rows(&results, 0, 1),
		vec![
			(Some("A".to_string()), Some("gold".to_string())),
			(Some("B".to_string()), Some("silver".to_string())),
		]
	);
}

#[tokio::test]
async fn select_distinct_left_outer_join_dedups_after_null_padding() {
	let results = run_logical(&plan_sql(
		"SELECT DISTINCT left_dups.label, right_dups.tier \
		 FROM left_dups LEFT JOIN right_dups ON left_dups.k = right_dups.k",
	))
	.await;
	let rows = sorted_utf8_pair_rows(&results, 0, 1);
	assert!(
		rows.iter().any(|row| row.1.is_none()),
		"left outer fixture must include null-padded misses: {rows:?}"
	);
	assert_eq!(
		rows,
		vec![
			(Some("A".to_string()), Some("gold".to_string())),
			(Some("B".to_string()), Some("silver".to_string())),
			(Some("missing".to_string()), None),
		]
	);
}

#[tokio::test]
async fn select_distinct_right_outer_join_dedups_after_null_padding() {
	let results = run_logical(&plan_sql(
		"SELECT DISTINCT left_dups.label, right_dups.tier \
		 FROM left_dups RIGHT JOIN right_dups ON left_dups.k = right_dups.k",
	))
	.await;
	let rows = sorted_utf8_pair_rows(&results, 0, 1);
	assert!(
		rows.iter().any(|row| row.0.is_none()),
		"right outer fixture must include null-padded misses: {rows:?}"
	);
	assert_eq!(
		rows,
		vec![
			(None, Some("orphan".to_string())),
			(Some("A".to_string()), Some("gold".to_string())),
			(Some("B".to_string()), Some("silver".to_string())),
		]
	);
}

#[tokio::test]
async fn select_distinct_full_outer_join_dedups_after_null_padding() {
	let results = run_logical(&plan_sql(
		"SELECT DISTINCT left_dups.label, right_dups.tier \
		 FROM left_dups FULL JOIN right_dups ON left_dups.k = right_dups.k",
	))
	.await;
	let rows = sorted_utf8_pair_rows(&results, 0, 1);
	assert!(
		rows.iter().any(|row| row.0.is_none()) && rows.iter().any(|row| row.1.is_none()),
		"full outer fixture must include null-padded misses from both sides: {rows:?}"
	);
	assert_eq!(
		rows,
		vec![
			(None, Some("orphan".to_string())),
			(Some("A".to_string()), Some("gold".to_string())),
			(Some("B".to_string()), Some("silver".to_string())),
			(Some("missing".to_string()), None),
		]
	);
}

#[tokio::test]
async fn select_distinct_join_star_dedups_full_join_output() {
	let (catalog, _) = fixture();
	let logical = plan_sql(
		"SELECT DISTINCT * \
		 FROM left_dups JOIN right_dups ON left_dups.k = right_dups.k",
	);
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind DISTINCT join star");
	assert_eq!(
		field_names(&bound.schema()),
		vec!["k", "label", "k", "tier"]
	);

	let results = run_logical(&logical).await;
	assert_eq!(
		row_count(&results),
		2,
		"duplicate full join rows must collapse"
	);
	assert_eq!(
		sorted_int64_pair_rows(&results, 0, 2),
		vec![(Some(1), Some(1)), (Some(2), Some(2))]
	);
}

#[tokio::test]
async fn select_distinct_join_duplicate_output_names_group_by_position() {
	let (catalog, _) = fixture();
	let logical = plan_sql(
		"SELECT DISTINCT left_dups.k AS x, right_dups.k AS x \
		 FROM left_dups JOIN right_dups ON left_dups.k = right_dups.k",
	);
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind duplicate-name DISTINCT join");
	assert_eq!(field_names(&bound.schema()), vec!["x", "x"]);

	let ExecutionPlan::Aggregate {
		group_expr,
		aggr_expr,
		..
	} = &bound
	else {
		panic!("expected DISTINCT join to bind to aggregate, got {bound:?}");
	};
	assert!(aggr_expr.is_empty());
	assert_column_node(
		group_expr,
		group_expr.roots[0],
		"x",
		Some(ColumnOrdinal(0)),
		Some(&DataType::Int64),
	);
	assert_column_node(
		group_expr,
		group_expr.roots[1],
		"x",
		Some(ColumnOrdinal(1)),
		Some(&DataType::Int64),
	);

	let results = run_logical(&logical).await;
	assert_eq!(
		sorted_int64_pair_rows(&results, 0, 1),
		vec![(Some(1), Some(1)), (Some(2), Some(2))]
	);
}

#[tokio::test]
async fn select_distinct_join_order_by_limit_runs_after_dedup() {
	let results = run_logical(&plan_sql(
		"SELECT DISTINCT left_dups.label \
		 FROM left_dups JOIN right_dups ON left_dups.k = right_dups.k \
		 ORDER BY left_dups.label DESC LIMIT 1",
	))
	.await;
	assert_eq!(utf8_column_rows(&results, 0), vec![Some("B".to_string())]);
}

#[tokio::test]
async fn rune_distinct_over_join_matches_sql_plan_and_rows() {
	let sql_logical = plan_sql(
		"SELECT DISTINCT left_dups.label, right_dups.tier \
		 FROM left_dups JOIN right_dups ON left_dups.k = right_dups.k",
	);
	let rune_logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("left_dups")
               .join(
                   ctx.table("right_dups"),
                   qcol("left_dups", "k").eq(qcol("right_dups", "k"))
               )
               .project([qcol("left_dups", "label"), qcol("right_dups", "tier")])
               .distinct()
        }
    "#,
	)
	.await;

	let (catalog, _) = fixture();
	let sql_bound = bind(&sql_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind SQL DISTINCT join");
	let rune_bound = bind(&rune_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind Rune DISTINCT join");
	assert_eq!(
		field_names(&sql_bound.schema()),
		field_names(&rune_bound.schema())
	);
	assert_distinct_join_execution_shape(&sql_bound);
	assert_distinct_join_execution_shape(&rune_bound);
	assert_eq!(
		canonical_arrow_ipc_digest(&run_logical(&sql_logical).await, SortPolicy::FullRowLex)
			.expect("sql digest"),
		canonical_arrow_ipc_digest(&run_logical(&rune_logical).await, SortPolicy::FullRowLex)
			.expect("rune digest")
	);
}

#[tokio::test]
async fn distinct_lowers_to_empty_aggregate_marker_no_new_execution_op() {
	let logical = plan_sql("SELECT DISTINCT region, status FROM sales");
	let LogicalPlan::Distinct { input } = &logical else {
		panic!("expected logical DISTINCT marker, got {logical:?}");
	};
	assert!(matches!(input.as_ref(), LogicalPlan::Project { .. }));

	let (catalog, _) = fixture();
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind DISTINCT");
	let ExecutionPlan::Aggregate {
		input,
		group_expr,
		aggr_expr,
		..
	} = &bound
	else {
		panic!("expected DISTINCT to lower to aggregate, got {bound:?}");
	};
	assert!(aggr_expr.is_empty());
	assert_eq!(group_expr.root_count(), 2);
	assert_column_node(
		group_expr,
		group_expr.roots[0],
		"region",
		Some(ColumnOrdinal(0)),
		Some(&DataType::Utf8),
	);
	assert_column_node(
		group_expr,
		group_expr.roots[1],
		"status",
		Some(ColumnOrdinal(1)),
		Some(&DataType::Utf8),
	);
	let ExecutionPlan::Exchange {
		input,
		partition_count,
		key,
	} = input.as_ref()
	else {
		panic!("expected keyed exchange under DISTINCT aggregate");
	};
	assert_eq!(*partition_count, 4);
	assert_eq!(key, group_expr);
	assert!(matches!(input.as_ref(), ExecutionPlan::Project { .. }));
}

#[tokio::test]
async fn distinct_join_lowers_to_existing_empty_aggregate_no_new_execution_op() {
	let logical = plan_sql(
		"SELECT DISTINCT left_dups.label, right_dups.tier \
		 FROM left_dups JOIN right_dups ON left_dups.k = right_dups.k",
	);
	let LogicalPlan::Distinct { input } = &logical else {
		panic!("expected logical DISTINCT marker, got {logical:?}");
	};
	assert!(matches!(input.as_ref(), LogicalPlan::Project { .. }));

	let (catalog, _) = fixture();
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind DISTINCT join");
	let ExecutionPlan::Aggregate {
		input,
		group_expr,
		aggr_expr,
		..
	} = &bound
	else {
		panic!("expected DISTINCT join to lower to aggregate, got {bound:?}");
	};
	assert!(aggr_expr.is_empty());
	assert_eq!(group_expr.root_count(), 2);
	let ExecutionPlan::Exchange {
		input,
		partition_count,
		key,
	} = input.as_ref()
	else {
		panic!("expected keyed exchange under DISTINCT join aggregate");
	};
	assert_eq!(*partition_count, 4);
	assert_eq!(key, group_expr);
	assert!(matches!(input.as_ref(), ExecutionPlan::Project { .. }));
	assert!(
		contains_execution_join(input),
		"DISTINCT dedup exchange must run above the joined rows"
	);
}

#[tokio::test]
async fn count_distinct_ignores_null_and_rewrites_to_count_arg() {
	let logical = plan_sql("SELECT COUNT(DISTINCT amount) FROM sales");
	let (group_expr, aggr_expr) = find_aggregate_over_distinct(&logical);
	assert!(group_expr.is_empty());
	assert_eq!(aggr_expr.len(), 1);
	assert!(matches!(aggr_expr[0].0, AggregateFunction::Count));

	// The discriminating proof: COUNT(DISTINCT amount) dedups and ignores NULL -> 4 (a raw COUNT or a
	// non-deduped count would differ).
	let results = run_logical(&logical).await;
	assert_eq!(scalar_int64_column_rows(&results, 0), vec![Some(4)]);
}

#[tokio::test]
async fn count_distinct_grouped_dedups_per_group() {
	let results = run_logical(&plan_sql(
		"SELECT region, COUNT(DISTINCT amount) FROM sales GROUP BY region",
	))
	.await;
	assert_eq!(
		region_count_rows(&results),
		vec![
			(None, 1),
			(Some("east".to_string()), 2),
			(Some("west".to_string()), 1),
		]
	);
}

#[tokio::test]
async fn distinct_aggregate_having_rewrites_same_distinct_call() {
	let results = run_logical(&plan_sql(
		"SELECT region, COUNT(DISTINCT amount) FROM sales \
		 GROUP BY region HAVING COUNT(DISTINCT amount) > 1",
	))
	.await;
	assert_eq!(
		region_count_rows(&results),
		vec![(Some("east".to_string()), 2)]
	);
}

#[tokio::test]
async fn sum_min_max_distinct_grouped_share_existing_dedup() {
	let sum = run_logical(&plan_sql(
		"SELECT region, SUM(DISTINCT amount) FROM sales GROUP BY region",
	))
	.await;
	assert_eq!(
		ordered_region_sum_rows(&sum),
		vec![
			(None, Some(7)),
			(Some("east".to_string()), Some(40)),
			(Some("west".to_string()), Some(20)),
		]
	);

	let min_max = run_logical(&plan_sql(
		"SELECT region, MIN(DISTINCT amount), MAX(DISTINCT amount) \
		 FROM sales GROUP BY region",
	))
	.await;
	assert_eq!(
		region_min_amount_max_amount_rows(&min_max),
		vec![
			(None, Some(7), Some(7)),
			(Some("east".to_string()), Some(10), Some(30)),
			(Some("west".to_string()), Some(20), Some(20)),
		]
	);
}

#[tokio::test]
async fn count_distinct_over_join_uses_distinct_below_group_exchange() {
	let logical = plan_sql(
		"SELECT left_dups.label, COUNT(DISTINCT right_dups.tier) \
		 FROM left_dups JOIN right_dups ON left_dups.k = right_dups.k \
		 GROUP BY left_dups.label",
	);
	let (group_expr, aggr_expr) = find_aggregate_over_distinct(&logical);
	assert_eq!(group_expr.len(), 1, "grouped by label");
	assert_eq!(aggr_expr.len(), 1);
	assert!(matches!(aggr_expr[0].0, AggregateFunction::Count));
	assert!(
		contains_logical_join(&logical),
		"distinct aggregate dedup must run after the join output"
	);

	let results = run_logical(&logical).await;
	assert_eq!(
		sorted_utf8_int64_rows(&results, 0, 1),
		vec![(Some("A".to_string()), 1), (Some("B".to_string()), 1)]
	);
}

#[test]
fn distinct_aggregate_multi_dedup_forms_now_plan() {
	// ADR 240 lifted the four ADR-234 single-dedup fences; each now lowers to a fan-out + recombine.
	for sql in [
		"SELECT COUNT(DISTINCT amount), SUM(price) FROM sales",
		"SELECT COUNT(DISTINCT amount), COUNT(DISTINCT id) FROM sales",
		"SELECT AVG(DISTINCT amount) FROM sales",
		"SELECT COUNT(DISTINCT amount + 1) FROM sales",
	] {
		assert!(
			SqlPlanner::new().plan(sql).is_ok(),
			"ADR 240 multi-dedup form must now lower: {sql}"
		);
	}
}

// ----- ADR 240 multi-dedup behavior gates -----
// sales.amount = [10, 20, NULL, 30, NULL, 7]; region = [east, west, east, east, west, NULL].

#[tokio::test]
async fn mixed_distinct_and_plain_aggregate_executes() {
	// D4: a plain branch (SUM(id)) recombines with a distinct branch (COUNT(DISTINCT amount)).
	let results = run_logical(&plan_sql(
		"SELECT COUNT(DISTINCT amount), SUM(id) FROM sales",
	))
	.await;
	assert_eq!(
		scalar_int64_column_rows(&results, 0),
		vec![Some(4)],
		"distinct amounts {{10,20,30,7}}"
	);
	assert_eq!(
		scalar_int64_column_rows(&results, 1),
		vec![Some(21)],
		"SUM(id) = 1+2+3+4+5+6"
	);
}

#[tokio::test]
async fn mixed_plain_branch_duplicate_output_names_stay_ordinal_safe() {
	// The logical Aggregate output names are function-level (`sum`, `sum`). ADR 240 branch
	// normalization must not resolve both plain aggregates through the first `sum` column.
	let results = run_logical(&plan_sql(
		"SELECT COUNT(DISTINCT amount), SUM(id), SUM(amount) FROM sales",
	))
	.await;
	assert_eq!(scalar_int64_column_rows(&results, 0), vec![Some(4)]);
	assert_eq!(
		scalar_int64_column_rows(&results, 1),
		vec![Some(21)],
		"SUM(id) = 1+2+3+4+5+6"
	);
	assert_eq!(
		scalar_int64_column_rows(&results, 2),
		vec![Some(67)],
		"SUM(amount) ignores NULLs: 10+20+30+7"
	);
}

#[tokio::test]
async fn multiple_distinct_arguments_execute() {
	// D2: two dedup branches over different arguments recombine.
	let results = run_logical(&plan_sql(
		"SELECT COUNT(DISTINCT amount), COUNT(DISTINCT region) FROM sales",
	))
	.await;
	assert_eq!(scalar_int64_column_rows(&results, 0), vec![Some(4)]);
	assert_eq!(
		scalar_int64_column_rows(&results, 1),
		vec![Some(2)],
		"distinct regions {{east, west}} — NULL ignored by COUNT"
	);
}

#[tokio::test]
async fn same_distinct_argument_shares_one_dedup_branch() {
	// D2: COUNT/MIN/MAX over the same distinct argument share ONE dedup branch (one Distinct node).
	let logical = plan_sql(
		"SELECT COUNT(DISTINCT amount), MIN(DISTINCT amount), MAX(DISTINCT amount) FROM sales",
	);
	assert_eq!(
		count_distinct_nodes(&logical),
		1,
		"aggregates over the same distinct argument must share one dedup branch"
	);
	let results = run_logical(&logical).await;
	assert_eq!(scalar_int64_column_rows(&results, 0), vec![Some(4)]);
	assert_eq!(scalar_int32_column_rows(&results, 1), vec![Some(7)]);
	assert_eq!(scalar_int32_column_rows(&results, 2), vec![Some(30)]);
}

#[tokio::test]
async fn avg_distinct_uses_avg_over_dedup_and_null_semantics() {
	// D5: AVG(DISTINCT) is ordinary AVG over the deduped rows; an all-NULL distinct set returns NULL
	// (not 0 / divide-by-zero).
	let avg = run_logical(&plan_sql("SELECT AVG(DISTINCT amount) FROM sales")).await;
	assert_eq!(
		scalar_float64_column_rows(&avg, 0),
		vec![Some(16.75)],
		"avg of distinct {{10,20,30,7}} = 67/4"
	);
	let empty = run_logical(&plan_sql("SELECT AVG(DISTINCT all_null_amount) FROM sales")).await;
	assert_eq!(
		scalar_float64_column_rows(&empty, 0),
		vec![None],
		"AVG over an all-NULL distinct set is NULL"
	);
}

#[tokio::test]
async fn computed_distinct_argument_executes() {
	// D3: a computed distinct argument is projected before dedup.
	let results = run_logical(&plan_sql("SELECT COUNT(DISTINCT amount + 1) FROM sales")).await;
	assert_eq!(
		scalar_int64_column_rows(&results, 0),
		vec![Some(4)],
		"distinct {{11,21,31,8}}"
	);
}

#[tokio::test]
async fn grouped_multi_dedup_all_null_distinct_column_keeps_group() {
	// D6: every group survives even when its distinct column is entirely NULL (COUNT = 0); the
	// recombine needs no dedicated group-domain branch. Locks the no-groups-branch decision.
	let results = run_logical(&plan_sql(
		"SELECT region, COUNT(DISTINCT all_null_amount), COUNT(DISTINCT amount) \
		 FROM sales GROUP BY region",
	))
	.await;
	let all_null = region_int64_rows(&results, 1);
	let amount = region_int64_rows(&results, 2);
	assert_eq!(
		all_null.len(),
		3,
		"all three groups (incl the NULL group) survive"
	);
	for region in [None, Some("east".to_string()), Some("west".to_string())] {
		assert_eq!(
			all_null.get(&region),
			Some(&Some(0)),
			"all-NULL distinct column yields count 0, not a dropped group: {region:?}"
		);
	}
	assert_eq!(amount.get(&None), Some(&Some(1))); // {7}
	assert_eq!(amount.get(&Some("east".to_string())), Some(&Some(2))); // {10, 30}
	assert_eq!(amount.get(&Some("west".to_string())), Some(&Some(1))); // {20}
}

#[tokio::test]
async fn global_multi_dedup_recombines_with_cross_join() {
	// D7: the global recombine uses a keyless Cross (the ADR 239 scalar-local path), not a synthetic key.
	let logical = plan_sql("SELECT COUNT(DISTINCT amount), COUNT(DISTINCT region) FROM sales");
	assert!(
		plan_contains_cross_join(&logical),
		"global multi-dedup recombine must use a keyless Cross join"
	);
	let results = run_logical(&logical).await;
	assert_eq!(scalar_int64_column_rows(&results, 0), vec![Some(4)]);
	assert_eq!(scalar_int64_column_rows(&results, 1), vec![Some(2)]);
}

#[tokio::test]
async fn multi_dedup_over_join_executes() {
	// D8: a mixed distinct + plain aggregate over a JOIN base. Each branch clones the already-lowered
	// join. left.k=1 -> A x gold (2x2=4 rows); k=2 -> B x silver (1 row).
	let results = run_logical(&plan_sql(
		"SELECT left_dups.label, COUNT(DISTINCT right_dups.tier), COUNT(*) \
		 FROM left_dups JOIN right_dups ON left_dups.k = right_dups.k \
		 GROUP BY left_dups.label",
	))
	.await;
	let distinct_tier = region_int64_rows(&results, 1);
	let total = region_int64_rows(&results, 2);
	assert_eq!(distinct_tier.get(&Some("A".to_string())), Some(&Some(1))); // {gold}
	assert_eq!(distinct_tier.get(&Some("B".to_string())), Some(&Some(1))); // {silver}
	assert_eq!(total.get(&Some("A".to_string())), Some(&Some(4)));
	assert_eq!(total.get(&Some("B".to_string())), Some(&Some(1)));
}

fn count_distinct_nodes(plan: &LogicalPlan) -> usize {
	match plan {
		LogicalPlan::Distinct { input } => 1 + count_distinct_nodes(input),
		LogicalPlan::Project { input, .. }
		| LogicalPlan::Filter { input, .. }
		| LogicalPlan::Exchange { input, .. }
		| LogicalPlan::Aggregate { input, .. } => count_distinct_nodes(input),
		LogicalPlan::Join { left, right, .. } => {
			count_distinct_nodes(left) + count_distinct_nodes(right)
		}
		_ => 0,
	}
}

fn plan_contains_cross_join(plan: &LogicalPlan) -> bool {
	match plan {
		LogicalPlan::Join {
			left, right, kind, ..
		} => {
			matches!(kind, JoinKind::Cross)
				|| plan_contains_cross_join(left)
				|| plan_contains_cross_join(right)
		}
		LogicalPlan::Project { input, .. }
		| LogicalPlan::Filter { input, .. }
		| LogicalPlan::Exchange { input, .. }
		| LogicalPlan::Aggregate { input, .. }
		| LogicalPlan::Distinct { input } => plan_contains_cross_join(input),
		_ => false,
	}
}

#[test]
fn public_group_by_without_aggregate_still_rejects() {
	assert_unsupported_message(
		"SELECT region FROM sales GROUP BY region",
		"GROUP BY requires at least one aggregate call",
	);
}

#[tokio::test]
async fn group_by_join_having_filters_after_aggregate() {
	let results = run_logical(&plan_sql(
		"SELECT left_dups.label, COUNT(*) \
		 FROM left_dups JOIN right_dups ON left_dups.k = right_dups.k \
		 GROUP BY left_dups.label HAVING COUNT(*) > 1",
	))
	.await;
	assert_eq!(
		sorted_utf8_int64_rows(&results, 0, 1),
		vec![(Some("A".to_string()), 4)]
	);
}

#[tokio::test]
async fn group_by_join_order_by_limit_runs_after_aggregate() {
	let results = run_logical(&plan_sql(
		"SELECT left_dups.label, COUNT(*) \
		 FROM left_dups JOIN right_dups ON left_dups.k = right_dups.k \
		 GROUP BY left_dups.label ORDER BY COUNT(*) DESC LIMIT 1",
	))
	.await;
	assert_eq!(
		sorted_utf8_int64_rows(&results, 0, 1),
		vec![(Some("A".to_string()), 4)]
	);
}

#[tokio::test]
async fn global_aggregate_over_join_still_works() {
	let results = run_logical(&plan_sql(
		"SELECT COUNT(*) FROM left_dups JOIN right_dups ON left_dups.k = right_dups.k",
	))
	.await;
	assert_eq!(scalar_int64_column_rows(&results, 0), vec![Some(5)]);
}

#[tokio::test]
async fn rune_group_by_over_join_matches_sql_plan_and_rows() {
	let sql_logical = plan_sql(
		"SELECT left_dups.label, COUNT(*) \
		 FROM left_dups JOIN right_dups ON left_dups.k = right_dups.k \
		 GROUP BY left_dups.label",
	);
	let rune_logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("left_dups")
               .join(
                   ctx.table("right_dups"),
                   qcol("left_dups", "k").eq(qcol("right_dups", "k"))
               )
               .aggregate([qcol("left_dups", "label")], [count_all()])
        }
    "#,
	)
	.await;

	let (catalog, _) = fixture();
	let sql_bound = bind(&sql_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind SQL group-by join");
	let rune_bound = bind(&rune_logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind Rune group-by join");
	assert_eq!(
		field_names(&sql_bound.schema()),
		field_names(&rune_bound.schema())
	);
	assert!(contains_execution_join(&sql_bound));
	assert!(contains_execution_join(&rune_bound));
	assert_eq!(
		canonical_arrow_ipc_digest(&run_logical(&sql_logical).await, SortPolicy::FullRowLex)
			.expect("sql digest"),
		canonical_arrow_ipc_digest(&run_logical(&rune_logical).await, SortPolicy::FullRowLex)
			.expect("rune digest")
	);
}

#[tokio::test]
async fn distinct_over_and_inside_derived_tables_is_supported() {
	let outer = plan_sql("SELECT DISTINCT region FROM (SELECT region FROM sales) s");
	assert!(matches!(outer, LogicalPlan::Distinct { .. }));
	assert_eq!(
		sorted_utf8_column_rows(&run_logical(&outer).await, 0),
		vec![None, Some("east".to_string()), Some("west".to_string())]
	);

	let inner = plan_sql("SELECT region FROM (SELECT DISTINCT region FROM sales) s");
	assert!(
		contains_logical_distinct(&inner),
		"DISTINCT inside derived table should lower to the existing marker"
	);
	assert_eq!(
		sorted_utf8_column_rows(&run_logical(&inner).await, 0),
		vec![None, Some("east".to_string()), Some("west".to_string())]
	);

	let duplicate_names =
		plan_sql("SELECT DISTINCT * FROM (SELECT region AS x, status AS x FROM sales) s");
	assert_eq!(row_count(&run_logical(&duplicate_names).await), 4);
}

#[tokio::test]
async fn distinct_inside_exists_is_accepted_and_stripped() {
	let logical = plan_sql(
		"SELECT region FROM sales \
		 WHERE EXISTS (SELECT DISTINCT region FROM regions WHERE regions.region = sales.region)",
	);
	assert!(
		!contains_logical_distinct(&logical),
		"EXISTS-internal DISTINCT should be existence-inert and not build a Distinct plan"
	);
	assert_eq!(
		sorted_utf8_column_rows(&run_logical(&logical).await, 0),
		vec![
			Some("east".to_string()),
			Some("east".to_string()),
			Some("east".to_string()),
			Some("west".to_string()),
			Some("west".to_string()),
		]
	);
}

#[tokio::test]
async fn derived_table_join_body_executes() {
	// ADR 254: the derived-table joins-inside fence is lifted — a join-bodied derived table now lowers
	// (the same "subplan as a named relation" capability CTEs use). Inner JOIN on k matches left_dups
	// (A,A@k=1; B@k=2) with right_dups (gold,gold@k=1; silver@k=2) -> bucket A x4, B x1.
	let results = run_logical(&plan_sql(
		"SELECT bucket, COUNT(*) \
		 FROM (SELECT left_dups.label AS bucket \
		       FROM left_dups JOIN right_dups ON left_dups.k = right_dups.k) d \
		 GROUP BY bucket",
	))
	.await;
	assert_eq!(
		sorted_utf8_int64_rows(&results, 0, 1),
		vec![(Some("A".to_string()), 4), (Some("B".to_string()), 1)]
	);
}

#[tokio::test]
async fn derived_table_comma_list_body_executes() {
	let results = run_logical(&plan_sql(
		"SELECT bucket, COUNT(*) \
		 FROM (SELECT left_dups.label AS bucket \
		       FROM left_dups, right_dups \
		       WHERE left_dups.k = right_dups.k) d \
		 GROUP BY bucket",
	))
	.await;
	assert_eq!(
		sorted_utf8_int64_rows(&results, 0, 1),
		vec![(Some("A".to_string()), 4), (Some("B".to_string()), 1)]
	);
}

#[test]
fn derived_table_comma_list_keeps_nested_derived_fences() {
	assert_unsupported_message(
		"SELECT bucket \
		 FROM (SELECT left_dups.label AS bucket \
		       FROM left_dups, (SELECT k FROM right_dups) r \
		       WHERE left_dups.k = r.k) d",
		"nested derived tables are not supported",
	);
	assert_unsupported_message(
		"SELECT bucket \
		 FROM (SELECT left_dups.label AS bucket \
		       FROM left_dups JOIN (SELECT k FROM right_dups) r ON left_dups.k = r.k) d",
		"nested derived tables are not supported",
	);
}

// ---------------------------------------------------------------------------
// ADR 254 — common table expressions (non-recursive WITH)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn count_star_over_a_subquery_carries_the_input_row_count() {
	// An empty projection is legitimate: `COUNT(*)` over a subquery needs the row
	// count and no column. A zero-column batch gives Arrow no way to infer
	// cardinality, so the projection must carry the input row count explicitly.
	// Before that fix every shape below failed with
	// "must either specify a row count or at least one column".
	let direct = run_logical(&plan_sql("SELECT COUNT(*) FROM sales")).await;
	assert_eq!(scalar_int64_column_rows(&direct, 0), vec![Some(6)]);

	for sql in [
		"SELECT COUNT(*) FROM (SELECT region FROM sales) s",
		"SELECT COUNT(*) FROM (SELECT region, amount FROM sales) s",
		"SELECT COUNT(*) FROM (SELECT region FROM sales WHERE region IS NOT NULL) s",
		"SELECT COUNT(1) FROM (SELECT region FROM sales) s",
		"WITH a AS (SELECT region FROM sales) SELECT COUNT(*) FROM a",
		"SELECT COUNT(*) FROM (SELECT region, COUNT(*) AS n FROM sales GROUP BY region) s",
	] {
		let results = run_logical(&plan_sql(sql)).await;
		let expected = if sql.contains("GROUP BY") { 3 } else { 6 };
		assert_eq!(
			scalar_int64_column_rows(&results, 0),
			vec![Some(expected)],
			"counting a subquery must agree with its cardinality: {sql}"
		);
	}

	// A grouped outer aggregate always projected a group key, so it never hit the
	// zero-column path; it must keep working.
	let grouped = run_logical(&plan_sql(
		"SELECT s.region, COUNT(*) FROM (SELECT region FROM sales) s GROUP BY s.region",
	))
	.await;
	assert_eq!(row_count(&grouped), 3);
}

#[tokio::test]
async fn time_bucket_floors_pre_epoch_timestamps_and_refuses_nonpositive_intervals() {
	// `TIME_BUCKET` truncated toward zero, so every pre-epoch timestamp landed one
	// bucket late and -1 shared the epoch bucket with +1. It also divided by zero
	// for a zero interval, aborting the process rather than returning an error.
	let mut config = PlannerConfig::default();
	config.allow_experimental_kernels = true;
	let (catalog, storage) = fixture();

	let bucket = |value: i64, secs: u64| LogicalPlan::Project {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("sales"),
			projection: None,
		}),
		exprs: vec![toron::ir::time_bucket_scalar_function(
			std::time::Duration::from_secs(secs),
			Expr::Literal(ScalarValue::Int64(Some(value))),
		)],
		output_names: vec![None],
	};

	// Floor, not truncation. Under `/` these returned -1000, -1000, -1000 and 0.
	for (input, expected) in [
		(-1_i64, -1000_i64),
		(-1000, -1000),
		(-1001, -2000),
		(-1500, -2000),
		// Exact positive boundaries and interiors are unchanged by the fix.
		(0, 0),
		(999, 0),
		(1000, 1000),
		(1001, 1000),
		(86_400_000, 86_400_000),
	] {
		let bound = bind(&bucket(input, 1), &catalog, &config)
			.await
			.expect("bind time bucket");
		let results = Executor::new(storage.clone())
			.execute_bound(&bound, None)
			.await
			.expect("execute time bucket");
		let first = scalar_int64_column_rows(&results, 0)
			.first()
			.copied()
			.flatten();
		assert_eq!(
			first,
			Some(expected),
			"TIME_BUCKET({input}, 1s) must floor to {expected}"
		);
	}

	// Nulls pass through rather than bucketing to the epoch.
	let null_plan = LogicalPlan::Project {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("sales"),
			projection: None,
		}),
		exprs: vec![toron::ir::time_bucket_scalar_function(
			std::time::Duration::from_secs(60),
			Expr::Literal(ScalarValue::Int64(None)),
		)],
		output_names: vec![None],
	};
	let bound = bind(&null_plan, &catalog, &config)
		.await
		.expect("bind null");
	let results = Executor::new(storage.clone())
		.execute_bound(&bound, None)
		.await
		.expect("execute null");
	assert_eq!(
		scalar_int64_column_rows(&results, 0).first().copied(),
		Some(None),
		"a null timestamp must stay null"
	);

	// A zero interval returns a typed error. It previously panicked with
	// "attempt to divide by zero", which no `Result` could report.
	let bound = bind(&bucket(1500, 0), &catalog, &config)
		.await
		.expect("bind zero interval");
	let error = Executor::new(storage.clone())
		.execute_bound(&bound, None)
		.await
		.expect_err("a zero interval must be refused, not divided by");
	assert!(
		error.to_string().contains("positive interval"),
		"expected a typed nonpositive-interval refusal, got: {error}"
	);

	// Near `i64::MIN` the mathematical bucket start is below the representable
	// range for every allowed interval: a 1m bucket of i64::MIN would be
	// -9_223_372_036_854_780_000. Unchecked this panicked in debug and wrapped to
	// a positive timestamp in release.
	for (label, secs) in [("1m", 60_u64), ("1h", 3_600), ("1d", 86_400)] {
		let bound = bind(&bucket(i64::MIN, secs), &catalog, &config)
			.await
			.expect("bind extreme timestamp");
		let error = Executor::new(storage.clone())
			.execute_bound(&bound, None)
			.await
			.expect_err("an unrepresentable bucket start must be refused");
		assert!(
			error.to_string().contains("not representable"),
			"{label} at i64::MIN must refuse as unrepresentable, got: {error}"
		);
	}
}

#[tokio::test]
async fn time_bucket_executes_ungated_from_sql_and_rune() {
	// Acceptance 1 is about execution under the DEFAULT config from both
	// frontends. Binding raw IR proves neither: it skips lowering entirely, so a
	// frontend could still refuse or mis-lower and this would pass.
	let sql = run_logical(&plan_sql(
		"SELECT TIME_BUCKET(k, '1m') FROM left_dups ORDER BY 1",
	))
	.await;
	assert_eq!(
		scalar_int64_column_rows(&sql, 0),
		vec![Some(0), Some(0), Some(0), Some(0), Some(0)],
		"SQL must execute TIME_BUCKET without the experimental flag"
	);

	let rune = run_logical(
		&rune_plan(
			r#"
        pub fn main(ctx) {
            ctx.table("left_dups")
               .project([time_bucket("1m", col("k"))])
        }
    "#,
		)
		.await,
	)
	.await;
	// Row count alone would pass on a mis-lowering: `col("k")` returns the same
	// five rows. Every fixture value is under a minute from the epoch, so a
	// correctly lowered bucket is five zeros — and the SQL arm above is the
	// reference, which makes this a frontend-parity assertion rather than a
	// restatement of the expected constant.
	assert_eq!(
		scalar_int64_column_rows(&rune, 0),
		scalar_int64_column_rows(&sql, 0),
		"Rune must produce the same buckets as SQL, not merely the same row count"
	);
	assert_eq!(
		scalar_int64_column_rows(&rune, 0),
		vec![Some(0); 5],
		"every fixture timestamp belongs to the epoch bucket"
	);
}

#[tokio::test]
async fn time_bucket_refuses_timestamp_and_float_inputs() {
	// Acceptance 2 names these two explicitly; neither was controlled, so the
	// "Int64 only" claim rested on the absence of a test rather than a refusal.
	let (catalog, _storage) = fixture();
	let config = PlannerConfig::default();

	let over = |expr: Expr| LogicalPlan::Project {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("sales"),
			projection: None,
		}),
		exprs: vec![toron::ir::time_bucket_scalar_function(
			std::time::Duration::from_secs(60),
			expr,
		)],
		output_names: vec![None],
	};

	for (label, inner) in [
		(
			"Timestamp",
			Expr::Literal(ScalarValue::Timestamp {
				value: Some(1_600_000_000_000),
				unit: arrow::datatypes::TimeUnit::Millisecond,
			}),
		),
		("Float64", Expr::column("price")),
	] {
		let error = bind(&over(inner), &catalog, &config)
			.await
			.err()
			.unwrap_or_else(|| panic!("{label} input must refuse, but it bound"));
		assert!(
			matches!(error, BindingError::TypeMismatch { .. }),
			"{label} must refuse with a typed mismatch, got: {error}"
		);
	}
}

#[tokio::test]
async fn rune_refuses_unsupported_time_bucket_intervals_at_lowering() {
	// Parity is only asserted if Rune is actually invoked. The allowlist control
	// exercises the shared parser and SQL; this exercises the frontend that used
	// to coerce an unknown interval to a zero duration and divide by zero.
	let config = RuneSandboxConfig::v1_default();
	let engine = RuneEngine::with_config(config).expect("Rune engine");
	let script = |interval: &str| {
		format!(
			r#"pub fn main(ctx) {{ ctx.table("left_dups").project([time_bucket("{interval}", col("k"))]) }}"#
		)
	};

	for accepted in ["1m", "1h", "1d"] {
		engine
			.execute_script(&script(accepted), &config)
			.await
			.unwrap_or_else(|error| panic!("Rune must accept {accepted}: {error}"));
	}

	for rejected in ["5m", "1s", "", "1 m", "1D", "1w"] {
		let error = engine
			.execute_script(&script(rejected), &config)
			.await
			.err()
			.unwrap_or_else(|| panic!("Rune must refuse interval {rejected:?}"));
		assert!(
			error
				.to_string()
				.contains("unsupported time_bucket interval"),
			"Rune must name the unsupported interval {rejected:?}, got: {error}"
		);
	}
}

#[tokio::test]
async fn time_bucket_refuses_named_logical_types() {
	// A named logical type over Int64 carries units that are not Unix
	// milliseconds. Checking only the physical type accepted, say, a customer id
	// as a timestamp. FLOOR refuses these for the same reason.
	let mut config = PlannerConfig::default();
	config.allow_experimental_kernels = true;
	let (catalog, _storage) = fixture();

	let plan = LogicalPlan::Project {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("sales"),
			projection: None,
		}),
		exprs: vec![toron::ir::time_bucket_scalar_function(
			std::time::Duration::from_secs(60),
			Expr::LogicalAscription {
				expr: Box::new(Expr::column("id")),
				logical_type: CUSTOMER_ID_LOGICAL_TYPE,
			},
		)],
		output_names: vec![None],
	};
	let error = bind(&plan, &catalog, &config)
		.await
		.expect_err("a named logical type must not bind as milliseconds");
	assert!(
		matches!(error, BindingError::TypeMismatch { .. }),
		"expected a typed mismatch, got: {error}"
	);
}

#[test]
fn time_bucket_interval_allowlist_is_shared_by_sql_and_rune() {
	// SQL rejected unknown intervals while Rune coerced them to a zero duration.
	// Both now consult one parser, so parity cannot drift back apart.
	for accepted in ["1m", "1h", "1d"] {
		assert!(
			toron::ir::time_bucket_interval_from_literal(accepted).is_some(),
			"{accepted} must be accepted"
		);
		assert!(
			SqlPlanner::new()
				.plan(&format!(
					"SELECT TIME_BUCKET(id, '{accepted}') FROM left_dups"
				))
				.is_ok(),
			"SQL must accept {accepted}"
		);
	}
	for rejected in ["5m", "1s", "", "1 m", "1D", "1w"] {
		assert!(
			toron::ir::time_bucket_interval_from_literal(rejected).is_none(),
			"{rejected} must be rejected"
		);
		let error = SqlPlanner::new()
			.plan(&format!(
				"SELECT TIME_BUCKET(id, '{rejected}') FROM left_dups"
			))
			.expect_err("SQL must reject an unsupported interval");
		assert!(
			error
				.to_string()
				.contains("unsupported time_bucket interval"),
			"unexpected SQL error for {rejected}: {error}"
		);
	}
}

#[tokio::test]
async fn cte_resolves_like_inline_subquery() {
	// `WITH a AS (q) SELECT … FROM a` resolves identically to the inline derived-table spelling.
	let cte = run_logical(&plan_sql(
		"WITH a AS (SELECT label, COUNT(*) AS n FROM left_dups GROUP BY label) SELECT label, n FROM a",
	))
	.await;
	let inline = run_logical(&plan_sql(
		"SELECT label, n FROM (SELECT label, COUNT(*) AS n FROM left_dups GROUP BY label) a",
	))
	.await;
	assert_eq!(
		sorted_utf8_int64_rows(&cte, 0, 1),
		sorted_utf8_int64_rows(&inline, 0, 1),
		"a CTE resolves like the inline subquery"
	);
	assert_eq!(
		sorted_utf8_int64_rows(&cte, 0, 1),
		vec![
			(Some("A".to_string()), 2),
			(Some("B".to_string()), 1),
			(Some("missing".to_string()), 2),
		]
	);
}

#[tokio::test]
async fn cte_body_with_join_binds_and_executes() {
	// The deliver-the-name gate: a join-bodied CTE plans + executes (the case the derived-table framing
	// would have rejected). A ⋈ on k -> label A x4, B x1.
	let results = run_logical(&plan_sql(
		"WITH j AS (SELECT left_dups.label AS label \
		            FROM left_dups JOIN right_dups ON left_dups.k = right_dups.k) \
		 SELECT label, COUNT(*) FROM j GROUP BY label",
	))
	.await;
	assert_eq!(
		sorted_utf8_int64_rows(&results, 0, 1),
		vec![(Some("A".to_string()), 4), (Some("B".to_string()), 1)]
	);
}

#[tokio::test]
async fn multiple_ctes_later_references_earlier() {
	// Linear scope: `b` references the earlier sibling `a` by its renamed output column `grp` — b can only
	// bind if it sees a's projected schema (and not the catalog `left_dups`, which has no `grp`).
	let results = run_logical(&plan_sql(
		"WITH a AS (SELECT label AS grp FROM left_dups), \
		      b AS (SELECT grp, COUNT(*) AS n FROM a GROUP BY grp) \
		 SELECT grp, n FROM b",
	))
	.await;
	assert_eq!(
		sorted_utf8_int64_rows(&results, 0, 1),
		vec![
			(Some("A".to_string()), 2),
			(Some("B".to_string()), 1),
			(Some("missing".to_string()), 2),
		]
	);
}

#[tokio::test]
async fn cte_referenced_in_a_join() {
	// A CTE referenced inside a JOIN binds as a named relation on the join side.
	let results = run_logical(&plan_sql(
		"WITH a AS (SELECT k, label FROM left_dups) \
		 SELECT a.label, COUNT(*) FROM a JOIN right_dups ON a.k = right_dups.k GROUP BY a.label",
	))
	.await;
	assert_eq!(
		sorted_utf8_int64_rows(&results, 0, 1),
		vec![(Some("A".to_string()), 4), (Some("B".to_string()), 1)]
	);
}

#[tokio::test]
async fn cte_shadows_catalog_table() {
	// A CTE named like a catalog table shadows it: this `left_dups` reads right_dups (tier AS label), so
	// grouping yields right_dups's tiers (gold x2, orphan x2, silver x1), not the real left_dups labels.
	let results = run_logical(&plan_sql(
		"WITH left_dups AS (SELECT k, tier AS label FROM right_dups) \
		 SELECT label, COUNT(*) FROM left_dups GROUP BY label",
	))
	.await;
	assert_eq!(
		sorted_utf8_int64_rows(&results, 0, 1),
		vec![
			(Some("gold".to_string()), 2),
			(Some("orphan".to_string()), 2),
			(Some("silver".to_string()), 1),
		]
	);
}

#[tokio::test]
async fn cte_column_alias_list_renames_output() {
	// `WITH a(x, y) AS (SELECT label, k …)` renames the output columns; `x` (was `label`) is projectable.
	let results = run_logical(&plan_sql(
		"WITH a(x, y) AS (SELECT label, k FROM left_dups) SELECT x, COUNT(*) FROM a GROUP BY x",
	))
	.await;
	assert_eq!(
		sorted_utf8_int64_rows(&results, 0, 1),
		vec![
			(Some("A".to_string()), 2),
			(Some("B".to_string()), 1),
			(Some("missing".to_string()), 2),
		]
	);
}

#[tokio::test]
async fn cte_table_arguments_still_reject() {
	// A CTE name must not swallow table-function syntax. Arguments are rejected before CTE lookup.
	assert_unsupported_message(
		"WITH a AS (SELECT k FROM left_dups) SELECT * FROM a(1)",
		"unsupported table factor: table arguments",
	);
}

#[tokio::test]
async fn recursive_cte_counter_series_enumerates() {
	// ADR 255: the canonical local `UNION ALL` fixpoint. Seed {1} (PK lookup on sales.id), recursive term
	// `n + 1 WHERE n < 5` enumerates 1..5 and terminates delta-empty when 5 < 5 fails.
	let results = run_logical(&plan_sql(
		"WITH RECURSIVE t(n) AS (\
		   SELECT MIN(id) FROM sales \
		   UNION ALL \
		   SELECT n + 1 FROM t WHERE n < 5\
		 ) SELECT n FROM t",
	))
	.await;
	assert_eq!(sorted_int64_column_rows(&results, 0), vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn recursive_cte_terminates_on_empty_recursive_term() {
	// The recursive term yields no rows on the first pass (1 < 1 is false) -> result is just the seed.
	let results = run_logical(&plan_sql(
		"WITH RECURSIVE t(n) AS (\
		   SELECT MIN(id) FROM sales \
		   UNION ALL \
		   SELECT n + 1 FROM t WHERE n < 1\
		 ) SELECT n FROM t",
	))
	.await;
	assert_eq!(sorted_int64_column_rows(&results, 0), vec![1]);
}

#[tokio::test]
async fn recursive_cte_outer_query_consumes_result() {
	// The outer query references the recursive CTE like any relation and filters it.
	let results = run_logical(&plan_sql(
		"WITH RECURSIVE t(n) AS (\
		   SELECT MIN(id) FROM sales \
		   UNION ALL \
		   SELECT n + 1 FROM t WHERE n < 5\
		 ) SELECT n FROM t WHERE n > 2",
	))
	.await;
	assert_eq!(sorted_int64_column_rows(&results, 0), vec![3, 4, 5]);
}

#[tokio::test]
async fn recursive_cte_nonterminating_hits_iteration_cap() {
	// A recursive term with no termination predicate grows forever -> the safety cap errors (no hang).
	let logical = plan_sql(
		"WITH RECURSIVE t(n) AS (\
		   SELECT MIN(id) FROM sales \
		   UNION ALL \
		   SELECT n + 1 FROM t\
		 ) SELECT n FROM t",
	);
	let (catalog, storage) = fixture();
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind unbounded recursive CTE");
	Executor::new(storage)
		.execute_bound(&bound, None)
		.await
		.expect_err("an unbounded recursive query must hit the iteration cap, not hang");
}

#[tokio::test]
async fn recursive_cte_union_distinct_and_malformed_shapes_reject() {
	// UNION (distinct), a join in the recursive term, a missing column list, and a non-`UNION ALL` body
	// all reject at lowering with precise messages.
	assert_unsupported_message(
		"WITH RECURSIVE t(n) AS (SELECT MIN(id) FROM sales UNION SELECT n + 1 FROM t WHERE n < 5) SELECT n FROM t",
		"recursive UNION (distinct) is not supported; use UNION ALL",
	);
	assert_unsupported_message(
		"WITH RECURSIVE t AS (SELECT MIN(id) FROM sales UNION ALL SELECT n + 1 FROM t WHERE n < 5) SELECT n FROM t",
		"a recursive CTE requires an explicit column list, e.g. WITH RECURSIVE t(n) AS (...)",
	);
	// A recursive term whose body does not reference the CTE is not actually recursive.
	assert_unsupported_message(
		"WITH RECURSIVE t(n) AS (SELECT MIN(id) FROM sales UNION ALL SELECT id FROM sales WHERE id = 2) SELECT n FROM t",
		"a recursive CTE's recursive term must reference the CTE",
	);
	// A join inside the recursive term is beyond the first-cut exchange-free shape.
	assert!(
		SqlPlanner::new()
			.plan(
				"WITH RECURSIVE t(n) AS (\
				   SELECT MIN(id) FROM sales \
				   UNION ALL \
				   SELECT s.id FROM t JOIN sales s ON t.n = s.id\
				 ) SELECT n FROM t"
			)
			.is_err(),
		"a join in the recursive term must reject (deferred to the distributed/graph cut)"
	);
}

// ---------------------------------------------------------------------------
// ADR 256 — uncorrelated subquery predicates (IN / EXISTS / NOT EXISTS)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn in_subquery_lowers_to_semi_join() {
	// `k IN (SELECT k FROM right_dups)` → keyed LeftSemi membership. left_dups k=[1,1,2,3,3] vs right
	// k∈{1,2,4} → k∈{1,2} → A:2, B:1.
	let results = run_logical(&plan_sql(
		"SELECT left_dups.label, COUNT(*) FROM left_dups \
		 WHERE left_dups.k IN (SELECT k FROM right_dups) GROUP BY left_dups.label",
	))
	.await;
	assert_eq!(
		sorted_utf8_int64_rows(&results, 0, 1),
		vec![(Some("A".to_string()), 2), (Some("B".to_string()), 1)]
	);
}

#[tokio::test]
async fn uncorrelated_exists_passes_all_rows_iff_subquery_nonempty() {
	// Keyless LeftSemi: non-empty subquery ⇒ all rows; empty ⇒ none.
	let nonempty = run_logical(&plan_sql(
		"SELECT left_dups.label, COUNT(*) FROM left_dups \
		 WHERE EXISTS (SELECT 1 FROM right_dups) GROUP BY left_dups.label",
	))
	.await;
	assert_eq!(
		sorted_utf8_int64_rows(&nonempty, 0, 1),
		vec![
			(Some("A".to_string()), 2),
			(Some("B".to_string()), 1),
			(Some("missing".to_string()), 2),
		]
	);
	let empty = run_logical(&plan_sql(
		"SELECT left_dups.label, COUNT(*) FROM left_dups \
		 WHERE EXISTS (SELECT 1 FROM empty_sales) GROUP BY left_dups.label",
	))
	.await;
	assert!(
		empty.iter().all(|batch| batch.num_rows() == 0),
		"an empty subquery passes no rows"
	);
}

#[tokio::test]
async fn not_exists_lowers_to_anti_join() {
	// Uncorrelated NOT EXISTS → keyless LeftAnti: over an empty subquery all rows pass; over a non-empty
	// subquery none do.
	let over_empty = run_logical(&plan_sql(
		"SELECT left_dups.label, COUNT(*) FROM left_dups \
		 WHERE NOT EXISTS (SELECT 1 FROM empty_sales) GROUP BY left_dups.label",
	))
	.await;
	assert_eq!(
		sorted_utf8_int64_rows(&over_empty, 0, 1),
		vec![
			(Some("A".to_string()), 2),
			(Some("B".to_string()), 1),
			(Some("missing".to_string()), 2),
		]
	);
	let over_nonempty = run_logical(&plan_sql(
		"SELECT left_dups.label, COUNT(*) FROM left_dups \
		 WHERE NOT EXISTS (SELECT 1 FROM right_dups) GROUP BY left_dups.label",
	))
	.await;
	assert!(
		over_nonempty.iter().all(|batch| batch.num_rows() == 0),
		"NOT EXISTS over a non-empty subquery passes no rows"
	);
}

#[tokio::test]
async fn scalar_subquery_comparison_stays_rejected() {
	// Correlated IN now decorrelates (259); a non-aggregate scalar subquery in a comparison stays deferred.
	assert_unsupported(
		"SELECT left_dups.k FROM left_dups WHERE left_dups.k = (SELECT k FROM right_dups)",
	);
}

// ---------------------------------------------------------------------------
// ADR 258 — NOT IN (subquery), null-aware
// ---------------------------------------------------------------------------

#[tokio::test]
async fn not_in_subquery_excludes_members() {
	// left_dups k=[1,1,2,3,3] NOT IN right_dups k∈{1,2,4} → keeps k=3,3 (label "missing").
	let results = run_logical(&plan_sql(
		"SELECT left_dups.label FROM left_dups WHERE left_dups.k NOT IN (SELECT k FROM right_dups)",
	))
	.await;
	assert_eq!(
		sorted_utf8_column_rows(&results, 0),
		vec![Some("missing".to_string()), Some("missing".to_string())]
	);
}

#[tokio::test]
async fn not_in_with_null_in_subquery_returns_no_rows() {
	// sales.region has a NULL → a NULL in the subquery key empties the whole NOT IN result.
	let results = run_logical(&plan_sql(
		"SELECT sales.id FROM sales WHERE sales.region NOT IN (SELECT region FROM sales s)",
	))
	.await;
	let rows: usize = results.iter().map(|b| b.num_rows()).sum();
	assert_eq!(rows, 0, "a NULL in the subquery key empties NOT IN");
}

#[tokio::test]
async fn not_in_lowers_to_anti_joins_with_null_guard_and_null_x_filter() {
	// Plan-shape gate: NOT IN desugars to anti-joins (not a semi-join), an `IS NOT NULL` filter on the
	// outer key (null-`x` exclusion), and an `IS NULL` null-guard over the subquery. (The runtime null-`x`
	// / null-guard execution can't be tested here — the FixtureScan mock drops pushed `IS NULL` scan
	// filters; the manual NOT-EXISTS-expansion probe covered the runtime semantics, and real storage
	// honors the filters.)
	let plan = plan_sql(
		"SELECT left_dups.label FROM left_dups WHERE left_dups.k NOT IN (SELECT k FROM right_dups)",
	);
	let debug = format!("{plan:?}");
	assert!(
		debug.contains("LeftAnti") && !debug.contains("LeftSemi"),
		"NOT IN lowers to anti-joins, not a semi-join: {debug}"
	);
	assert!(
		debug.contains("IsNotNull"),
		"NOT IN adds the null-x filter: {debug}"
	);
	assert!(
		debug.contains("IsNull"),
		"NOT IN adds the null-guard: {debug}"
	);
}

#[tokio::test]
async fn cross_type_not_in_stays_rejected() {
	// Cross-type membership key (Int64 k vs Int32 amount) rejects at bind — the deferred coercion gap.
	// (Correlated NOT IN now decorrelates per 259.)
	let logical = plan_sql(
		"SELECT left_dups.k FROM left_dups WHERE left_dups.k NOT IN (SELECT amount FROM sales)",
	);
	let (catalog, _) = fixture();
	bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect_err("cross-type NOT IN membership key must reject at bind");
}

// ---------------------------------------------------------------------------
// ADR 257 — scalar subqueries (provably-scalar / global-aggregate form)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn scalar_aggregate_subquery_in_projection_attaches_value() {
	// `(SELECT AVG(amount) FROM sales)` is provably scalar (a global aggregate, row=1). It pulls up into
	// a scalar Cross broadcast onto every region row. amount=[10,20,_,30,_,7] → AVG = 67/4 = 16.75.
	let plan = plan_sql("SELECT region, (SELECT AVG(amount) FROM sales) FROM regions");
	assert!(
		plan_contains_cross_join(&plan),
		"a projection scalar subquery lowers into a Cross: {plan:?}"
	);
	let results = run_logical(&plan).await;
	assert_eq!(
		row_count(&results),
		2,
		"one row per region, no multiplication"
	);
	assert_eq!(
		sorted_utf8_column_rows(&results, 0),
		vec![Some("east".to_string()), Some("west".to_string())]
	);
	assert_eq!(
		scalar_float64_column_rows(&results, 1),
		vec![Some(16.75), Some(16.75)],
		"every region row carries the global AVG"
	);
}

#[tokio::test]
async fn scalar_aggregate_subquery_in_where_filters() {
	// `id > (SELECT COUNT(*) FROM regions)` filters per-row against the scalar (regions has 2 rows). The
	// scalar column rides a Cross above the scan, so the comparison is a residual Toron filter (not a
	// pushed scan predicate that the FixtureScan would drop). id=[1..6] > 2 → ids 3,4,5,6.
	let plan = plan_sql("SELECT id FROM sales WHERE id > (SELECT COUNT(*) FROM regions)");
	assert!(
		plan_contains_cross_join(&plan),
		"a WHERE scalar subquery lowers into a Cross: {plan:?}"
	);
	let results = run_logical(&plan).await;
	assert_eq!(
		sorted_int64_column_rows(&results, 0),
		vec![3, 4, 5, 6],
		"only rows above the scalar count survive"
	);
}

#[tokio::test]
async fn scalar_aggregate_subquery_in_having_filters() {
	// ADR 320: HAVING scalars recombine after the grouped aggregate. The scalar threshold is the single
	// global regions COUNT (2), so only east's grouped COUNT (3) survives.
	let plan = plan_sql(
		"SELECT region, COUNT(*) FROM sales \
		 GROUP BY region HAVING COUNT(*) > (SELECT COUNT(*) FROM regions)",
	);
	let results = run_logical(&plan).await;
	assert_eq!(row_count(&results), 1);
	assert_eq!(
		sorted_utf8_column_rows(&results, 0),
		vec![Some("east".to_string())]
	);
	assert_eq!(scalar_int64_column_rows(&results, 1), vec![Some(3)]);
	let fields = results[0].schema().fields().clone();
	assert_eq!(
		fields.len(),
		2,
		"synthetic scalar/key columns must be hidden"
	);
	assert!(
		fields
			.iter()
			.all(|field| !field.name().contains("__scalar_")
				&& !field.name().contains("__having_scalar_")),
		"visible schema must not leak HAVING scalar internals: {fields:?}"
	);
}

#[test]
fn having_scalar_deferred_shapes_stay_rejected() {
	assert_unsupported_message(
		"SELECT region, SUM(amount) FROM sales \
		 GROUP BY region HAVING SUM(amount) > \
		   (SELECT amount FROM sales)",
		"non-aggregate scalar subqueries are not supported",
	);
	assert_unsupported_message(
		"SELECT sales.region, SUM(sales.amount) FROM sales \
		 GROUP BY sales.region HAVING SUM(sales.amount) > \
		   (SELECT SUM(other.amount) FROM sales AS other WHERE other.region = sales.region)",
		"correlated scalar subqueries are not supported",
	);
	assert_unsupported_message(
		"SELECT region, SUM(amount), \
		        (SELECT SUM(amount) FROM sales WHERE region = 'west') \
		 FROM sales GROUP BY region",
		"scalar subqueries in GROUP BY/HAVING are not supported",
	);
}

#[tokio::test]
async fn one_side_scalar_cross_broadcasts_the_scalar() {
	// The binder extension (Phase 1): `outer × (global aggregate)` is a Cross with a provably-scalar
	// (row=1) RIGHT side. On main it rejected ("CROSS JOIN requires a broadcast-eligible build side"); the
	// `ScalarBroadcastCross` branch now binds it by broadcasting the 1-row aggregate to every outer row.
	let plan = plan_sql("SELECT region, (SELECT AVG(amount) FROM sales) FROM regions");
	assert!(plan_contains_cross_join(&plan));
	let (catalog, _storage) = fixture();
	let bound = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("outer × scalar Cross must bind by broadcasting the scalar side");
	assert!(
		contains_execution_join(&bound),
		"the bound plan recombines the scalar via a Join: {bound:?}"
	);
}

#[tokio::test]
async fn non_aggregate_and_correlated_scalar_subqueries_stay_rejected() {
	// A non-aggregate scalar subquery `(SELECT x FROM t)` needs a runtime cardinality assert (deferred).
	assert_unsupported_message(
		"SELECT region, (SELECT region FROM regions) FROM regions",
		"non-aggregate scalar subqueries are not supported",
	);
	// ADR 260 lowers a WHERE-correlated aggregate scalar; a correlation in the aggregate ARGUMENT is
	// undecorrelatable by GROUP BY and stays deferred.
	assert_unsupported_message(
		"SELECT region FROM regions \
		 WHERE active < (SELECT AVG(amount + regions.region) FROM sales WHERE sales.region = regions.region)",
		"correlated scalar subqueries are not supported",
	);
}

#[test]
fn group_by_inside_exists_subquery_still_rejects() {
	assert_unsupported(
		"SELECT region, COUNT(*) FROM sales \
		 WHERE EXISTS (SELECT region, COUNT(*) FROM regions \
		               WHERE regions.region = sales.region GROUP BY region) \
		 GROUP BY region",
	);
}

#[tokio::test]
async fn distinct_over_aggregate_inputs_still_rejects() {
	let aggregate = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .aggregate([col("region")], [count_all()])
               .distinct()
        }
    "#,
	)
	.await;
	let (catalog, _) = fixture();
	let err = bind(&aggregate, &catalog, &PlannerConfig::default())
		.await
		.expect_err("Rune DISTINCT over aggregate should fail");
	assert!(
		matches!(err, BindingError::UnsupportedExpression { ref message } if message == "DISTINCT over aggregate inputs is not supported"),
		"expected Rune aggregate DISTINCT fence, got {err:?}"
	);
}

#[tokio::test]
async fn distinct_over_joined_aggregate_input_still_rejects() {
	let aggregate = LogicalPlan::Aggregate {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("sales"),
			projection: None,
		}),
		group_expr: vec![Expr::column("region")],
		aggr_expr: vec![(AggregateFunction::CountAll, Vec::new())],
	};
	let logical = LogicalPlan::Distinct {
		input: Box::new(LogicalPlan::Join {
			left: Box::new(aggregate),
			right: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("regions"),
				projection: None,
			}),
			kind: JoinKind::Inner,
			on: vec![(Expr::column("region"), Expr::column("region"))],
			on_filter: None,
			null_safe_keys: false,
			strategy_intent: JoinStrategyIntent::Auto,
		}),
	};
	let (catalog, _) = fixture();
	let err = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect_err("DISTINCT over joined aggregate input should fail");
	assert!(
		matches!(err, BindingError::UnsupportedExpression { ref message } if message == "DISTINCT over aggregate inputs is not supported"),
		"expected nested aggregate DISTINCT fence, got {err:?}"
	);
}

#[test]
fn distinct_with_aggregate_group_by_or_having_is_explicit_unsupported() {
	assert_unsupported_message(
		"SELECT DISTINCT COUNT(*) FROM sales",
		"DISTINCT with aggregate projections is not supported",
	);
	assert_unsupported_message(
		"SELECT DISTINCT region FROM sales GROUP BY region",
		"DISTINCT with GROUP BY is not supported",
	);
	assert_unsupported_message(
		"SELECT DISTINCT region FROM sales HAVING region IS NOT NULL",
		"DISTINCT with HAVING is not supported",
	);
	assert_unsupported_message(
		"SELECT DISTINCT ON (region) region FROM sales",
		"DISTINCT ON is not supported",
	);
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
	// ADR 267: two MINs + two MAXs collide on the natural names "min"/"max", so each disambiguates to a
	// unique internal `__agg_<i>` output name (the identity data types are unchanged).
	assert_eq!(output_schema.field(0).name(), "__agg_0");
	assert_eq!(output_schema.field(0).data_type(), &DataType::Int32);
	assert!(output_schema.field(0).is_nullable());
	assert_eq!(output_schema.field(1).name(), "__agg_1");
	assert_eq!(output_schema.field(1).data_type(), &DataType::Int64);
	assert!(output_schema.field(1).is_nullable());
	assert_eq!(output_schema.field(2).name(), "__agg_2");
	assert_eq!(output_schema.field(2).data_type(), &DataType::Float64);
	assert!(output_schema.field(2).is_nullable());
	assert_eq!(output_schema.field(3).name(), "__agg_3");
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
	// ADR 267: three AVGs collide on the natural name "avg", so each disambiguates to a unique internal
	// `__agg_<i>` output name (Float64 output unchanged).
	let names: Vec<&str> = output_schema
		.fields()
		.iter()
		.map(|field| field.name().as_str())
		.collect();
	assert_eq!(names, ["__agg_0", "__agg_1", "__agg_2"]);
	for field in output_schema.fields() {
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
	// AVG(DISTINCT amount) is now supported (ADR 240) — covered by the multi-dedup behavior gates.
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

	// ADR 267: the two unaliased COUNTs disambiguate to unique internal `__agg_<i>` names, so the natural
	// name "count" is no longer an addressable output column — `ORDER BY count` is now unknown, not
	// ambiguous (alias the aggregates to order by them).
	let err = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect_err("unaliased duplicate aggregate name is not addressable");
	assert!(matches!(
		err,
		BindingError::UnknownColumn { column } if column == "count"
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
	let logical = plan_sql(
		"SELECT status, AVG(amount) FROM sales GROUP BY status HAVING AVG(amount) > 10.00",
	);
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
async fn float_arithmetic_decimal_point_literals_rewrite_to_float_literals() {
	let multiplied = run_logical(&plan_sql("SELECT price * 0.0001 FROM sales")).await;
	let rows = scalar_float64_column_rows(&multiplied, 0);
	let expected = [
		Some(0.00105),
		Some(0.00205),
		None,
		Some(0.00305),
		None,
		Some(0.00075),
	];
	assert_eq!(rows.len(), expected.len());
	for (actual, expected) in rows.iter().zip(expected) {
		match (actual, expected) {
			(Some(actual), Some(expected)) => {
				assert!(
					(actual - expected).abs() < 1e-12,
					"expected {expected}, got {actual}"
				);
			}
			(None, None) => {}
			other => panic!("unexpected Float64 multiplication row: {other:?}"),
		}
	}
}

#[tokio::test]
async fn floor_preserves_the_closed_numeric_type_matrix_and_nulls() {
	let results = run_logical(&plan_sql(
		"SELECT FLOOR(id), FLOOR(amount), FLOOR(price), FLOOR(rating32) FROM sales",
	))
	.await;

	assert_eq!(results[0].schema().field(0).data_type(), &DataType::Int64);
	assert_eq!(results[0].schema().field(1).data_type(), &DataType::Int32);
	assert_eq!(results[0].schema().field(2).data_type(), &DataType::Float64);
	assert_eq!(results[0].schema().field(3).data_type(), &DataType::Float32);
	assert_eq!(
		scalar_int64_column_rows(&results, 0),
		vec![Some(1), Some(2), Some(3), Some(4), Some(5), Some(6)]
	);
	assert_eq!(
		scalar_int32_column_rows(&results, 1),
		vec![Some(10), Some(20), None, Some(30), None, Some(7)]
	);
	assert_eq!(
		scalar_float64_column_rows(&results, 2),
		vec![Some(10.0), Some(20.0), None, Some(30.0), None, Some(7.0)]
	);
	assert_eq!(
		scalar_float32_column_rows(&results, 3),
		vec![Some(1.0), Some(2.0), None, Some(3.0), Some(4.0), Some(0.0)]
	);

	let (catalog, _) = fixture();
	let decimal = plan_sql("SELECT FLOOR(decimal_price) FROM sales");
	let err = bind(&decimal, &catalog, &PlannerConfig::default())
		.await
		.expect_err("Decimal128 FLOOR must refuse at bind time");
	assert!(
		matches!(err, BindingError::TypeMismatch { .. }),
		"expected typed FLOOR type mismatch, got {err}"
	);

	let logical = LogicalPlan::Project {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("sales"),
			projection: None,
		}),
		exprs: vec![Expr::ScalarFunction {
			id: ScalarFunctionId::Floor,
			args: vec![Expr::LogicalAscription {
				expr: Box::new(Expr::column("id")),
				logical_type: CUSTOMER_ID_LOGICAL_TYPE,
			}],
			parameter: ScalarFunctionParameter::None,
		}],
		output_names: vec![None],
	};
	let err = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect_err("named logical FLOOR must refuse at bind time");
	assert!(
		matches!(err, BindingError::TypeMismatch { .. }),
		"expected named logical FLOOR type mismatch, got {err}"
	);
}

#[tokio::test]
async fn floor_rejects_non_finite_values_after_reaching_evaluation() {
	let finite = run_logical(&plan_sql("SELECT FLOOR(price) FROM sales")).await;
	assert_eq!(
		scalar_float64_column_rows(&finite, 0),
		vec![Some(10.0), Some(20.0), None, Some(30.0), None, Some(7.0)]
	);

	let nan = execute_sql_err("SELECT FLOOR(score) FROM sales").await;
	assert_kernel_error_prefix(nan, "FLOOR requires finite Float64 input: NaN");
	let infinity = execute_sql_err("SELECT FLOOR(price / 0.0) FROM sales").await;
	assert_kernel_error_prefix(infinity, "FLOOR requires finite Float64 input: inf");
}

#[tokio::test]
async fn floor_reaches_filter_ordering_and_computed_aggregate_arguments() {
	let filtered = run_logical(&plan_sql(
		"SELECT FLOOR(price) AS floored FROM sales \
		 WHERE FLOOR(price) >= 10.0 ORDER BY floored",
	))
	.await;
	assert_eq!(
		scalar_float64_column_rows(&filtered, 0),
		vec![Some(10.0), Some(20.0), Some(30.0)]
	);

	let aggregate = run_logical(&plan_sql("SELECT SUM(FLOOR(price)) FROM sales")).await;
	assert_eq!(scalar_float64_column_rows(&aggregate, 0), vec![Some(67.0)]);

	assert_unsupported_message(
		"SELECT FLOOR(id TO DAY) FROM sales",
		"datetime FLOOR is not supported",
	);
}

#[tokio::test]
async fn floor_alias_first_bucketing_is_sparse_and_handles_negative_bins() {
	let logical = plan_sql(
		"WITH bucketed AS (\
		   SELECT FLOOR((price - 20.0) / 10.0) * 10.0 AS bin_start \
		   FROM sales WHERE price + 0.0 IS NOT NULL) \
		 SELECT bin_start, bin_start + 10.0 AS bin_end, COUNT(*) AS bin_count \
		 FROM bucketed GROUP BY bin_start ORDER BY bin_start",
	);
	// FixtureScan does not interpret storage-pushed predicates. The arithmetic identity keeps this
	// null filter on Toron's evaluation rail while preserving the canonical query's semantics.
	let results = run_logical(&logical).await;

	assert_eq!(
		scalar_float64_column_rows(&results, 0),
		vec![Some(-20.0), Some(-10.0), Some(0.0), Some(10.0)]
	);
	assert_eq!(
		scalar_float64_column_rows(&results, 1),
		vec![Some(-10.0), Some(0.0), Some(10.0), Some(20.0)]
	);
	assert_eq!(
		scalar_int64_column_rows(&results, 2),
		vec![Some(1), Some(1), Some(1), Some(1)]
	);

	assert_unsupported("SELECT FLOOR(price), COUNT(*) FROM sales GROUP BY FLOOR(price)");
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

	// ADR 267: the two unaliased COUNTs disambiguate to unique internal `__agg_<i>` names, so "count" is
	// no longer an addressable output column — `HAVING count` is now unknown, not ambiguous.
	let err = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect_err("unaliased duplicate aggregate name is not addressable");
	assert!(matches!(
		err,
		BindingError::UnknownColumn { column } if column == "count"
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

// ADR 267: multiple same-function aggregates in one projection.
//
// `SUM(amount)` and `SUM(rating32)` both carry the natural name "sum"; before 267 the second collapsed
// onto the first and the projection was rejected as `ambiguous aggregate projection`. The fixture data is
// deliberately rank-inverting between the two columns: SUM(amount) ranks east(40) > west(20) > null(7),
// while SUM(rating32) ranks west(7.0) > east(5.0) > null(0.25). That inversion is what makes the ORDER BY
// / HAVING cases *discriminating* — they could only pass if the engine resolves each aggregate-call to its
// own column, never a same-named sibling's. (The mock scan ignores pushed `WHERE` predicates, so the
// null-region group is filtered via HAVING, not WHERE — see the residual-filter behavior in the q06 path.)

#[tokio::test]
async fn same_function_aggregate_projection_binds_distinct_internal_names() {
	let (catalog, _) = fixture();
	// Unaliased: the two SUMs disambiguate to stable, unique internal `__agg_<i>` output columns.
	let bound = bind(
		&plan_sql("SELECT region, SUM(amount), SUM(rating32) FROM sales GROUP BY region"),
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect("bind two same-function aggregates");
	let ExecutionPlan::Aggregate { output_schema, .. } = bound else {
		panic!("expected aggregate root for matching projection");
	};
	assert_eq!(output_schema.field(0).name(), "region");
	assert_eq!(output_schema.field(1).name(), "__agg_0");
	assert_eq!(output_schema.field(1).data_type(), &DataType::Int64);
	assert!(output_schema.field(1).is_nullable());
	assert_eq!(output_schema.field(2).name(), "__agg_1");
	assert!(output_schema.field(2).is_nullable());
}

#[tokio::test]
async fn aliased_same_function_aggregates_use_aliases_not_internal_names() {
	// Aliased: the internal `__agg_<i>` names must NOT leak — the visible labels are the user's aliases,
	// and the two columns hold distinct per-region values (proving they are not collapsed).
	let results = run_logical(&plan_sql(
		"SELECT region, SUM(amount) AS total_amount, SUM(rating32) AS total_rating \
		 FROM sales GROUP BY region ORDER BY region",
	))
	.await;
	let schema = results[0].schema();
	assert_eq!(schema.field(0).name(), "region");
	assert_eq!(schema.field(1).name(), "total_amount");
	assert_eq!(schema.field(2).name(), "total_rating");
	assert_eq!(
		utf8_column_rows(&results, 0),
		vec![Some("east".to_string()), Some("west".to_string()), None]
	);
	assert_eq!(
		scalar_int64_column_rows(&results, 1),
		vec![Some(40), Some(20), Some(7)]
	);
}

#[tokio::test]
async fn order_by_same_function_aggregate_resolves_to_its_own_column() {
	// `ORDER BY SUM(rating32)` must sort by the rating column (west, east, null), NOT by the same-named
	// SUM(amount) (which would give east, west, null). The rank inversion of east/west is the discriminator.
	let results = run_logical(&plan_sql(
		"SELECT region, SUM(amount), SUM(rating32) \
		 FROM sales GROUP BY region ORDER BY SUM(rating32) DESC",
	))
	.await;
	assert_eq!(
		utf8_column_rows(&results, 0),
		vec![Some("west".to_string()), Some("east".to_string()), None]
	);
}

#[tokio::test]
async fn having_same_function_aggregate_filters_by_its_own_column() {
	// `HAVING SUM(rating32) > 6` keeps only west (7.0); east (5.0) and null (0.25) are dropped. Filtering by
	// the same-named SUM(amount) (east 40, west 20, null 7) would keep east and west, so a [west]-only
	// result proves the HAVING aggregate-call resolves to the rating column, not its same-named sibling.
	let results = run_logical(&plan_sql(
		"SELECT region, SUM(amount), SUM(rating32) \
		 FROM sales GROUP BY region HAVING SUM(rating32) > 6 ORDER BY region",
	))
	.await;
	assert_eq!(
		utf8_column_rows(&results, 0),
		vec![Some("west".to_string())]
	);
}

#[tokio::test]
async fn aggregate_derived_table_binds_and_executes() {
	let results = run_logical(&plan_sql(
		"SELECT bucket, total \
		 FROM (SELECT region AS bucket, SUM(amount) AS total FROM sales GROUP BY region) AS p \
		 WHERE total > 10 ORDER BY bucket",
	))
	.await;

	assert_eq!(
		ordered_region_sum_rows(&results),
		vec![
			(Some("east".to_string()), Some(40)),
			(Some("west".to_string()), Some(20)),
		]
	);
}

#[tokio::test]
async fn global_aggregate_derived_table_binds_and_executes() {
	let results = run_logical(&plan_sql(
		"SELECT n FROM (SELECT COUNT(*) AS n FROM sales) AS p",
	))
	.await;

	assert_eq!(scalar_sum_rows(&results), vec![Some(6)]);
}

#[tokio::test]
async fn aggregate_derived_having_binds_and_executes() {
	let results = run_logical(&plan_sql(
		"SELECT bucket, n \
		 FROM (SELECT region AS bucket, COUNT(*) AS n FROM sales GROUP BY region HAVING COUNT(*) > 2) AS p",
	))
	.await;

	assert_eq!(
		region_count_rows(&results),
		vec![(Some("east".to_string()), 3)]
	);
}

// Runs on an explicit large stack: this derived-table top-k's bind+execute future recursion overflows
// libtest's default ~2MB test-thread stack (cumulative bind-dispatch frame growth; deep but finite — the CLI
// and production run it fine on their larger stacks). Same treatment as `q10_executes_in_both_modes` in the
// tpch harness. (Pre-existing; surfaced when running the full suite — unrelated to the deferred-scalar path.)
#[test]
fn sql_aggregate_derived_top_k_executes() {
	std::thread::Builder::new()
		.stack_size(64 * 1024 * 1024)
		.spawn(|| {
			let runtime = tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.expect("tokio runtime");
			runtime.block_on(async {
				let results = run_logical(&plan_sql(
					"SELECT bucket, total \
					 FROM (SELECT region AS bucket, SUM(amount) AS total FROM sales GROUP BY region \
					       ORDER BY SUM(amount) + 1 DESC LIMIT 2) AS p \
					 WHERE total > 0",
				))
				.await;

				assert_eq!(
					ordered_region_sum_rows(&results),
					vec![
						(Some("east".to_string()), Some(40)),
						(Some("west".to_string()), Some(20)),
					]
				);
			});
		})
		.expect("spawn derived-top-k thread")
		.join()
		.expect("derived-top-k thread panicked");
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
async fn decimal_multiply_coerces_non_literal_integer_operands() {
	let (catalog, storage) = fixture();
	let logical = plan_sql(
		"SELECT \
		   decimal_price * amount, \
		   decimal_price * id, \
		   amount * decimal_price, \
		   decimal_price * (amount + 1), \
		   decimal_price * 2 \
		 FROM sales",
	);
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind Decimal x Int multiply");
	assert_eq!(
		bound.schema().field(0).data_type(),
		&DataType::Decimal128(26, 2)
	);
	assert_eq!(
		bound.schema().field(1).data_type(),
		&DataType::Decimal128(35, 2)
	);
	assert_eq!(
		bound.schema().field(2).data_type(),
		&DataType::Decimal128(26, 2)
	);
	assert_eq!(
		bound.schema().field(3).data_type(),
		&DataType::Decimal128(26, 2)
	);
	assert_eq!(
		bound.schema().field(4).data_type(),
		&DataType::Decimal128(31, 4),
		"Decimal x Int literal remains ADR 266's literal path"
	);
	let wide = bind(
		&plan_sql("SELECT decimal_wide * id FROM sales"),
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect("bind wide Decimal x Int64 multiply");
	assert_eq!(
		wide.schema().field(0).data_type(),
		&DataType::Decimal128(38, 2),
		"wide Decimal x Int64 keeps the existing Decimal mul precision cap at bind"
	);

	let ExecutionPlan::Project { exprs, .. } = &bound else {
		panic!("expected project");
	};
	assert_mul_with_casted_right(
		exprs,
		0,
		DataType::Decimal128(10, 0),
		"decimal_price",
		"amount",
	);
	assert_mul_with_casted_left(
		exprs,
		2,
		DataType::Decimal128(10, 0),
		"amount",
		"decimal_price",
	);
	assert_mul_with_casted_right_expr(exprs, 3, DataType::Decimal128(10, 0));

	let results = Executor::new(storage)
		.execute_bound(&bound, None)
		.await
		.expect("execute Decimal x Int multiply");
	assert_eq!(
		scalar_decimal128_column_rows(&results, 0),
		vec![
			Some(12_340),
			Some(-10_000),
			None,
			Some(30_000),
			None,
			Some(1_750),
		]
	);
	assert_eq!(
		scalar_decimal128_column_rows(&results, 1),
		vec![
			Some(1_234),
			Some(-1_000),
			None,
			Some(4_000),
			Some(49_995),
			Some(1_500),
		]
	);
	assert_eq!(
		scalar_decimal128_column_rows(&results, 2),
		vec![
			Some(12_340),
			Some(-10_000),
			None,
			Some(30_000),
			None,
			Some(1_750),
		]
	);
	assert_eq!(
		scalar_decimal128_column_rows(&results, 3),
		vec![
			Some(13_574),
			Some(-10_500),
			None,
			Some(31_000),
			None,
			Some(2_000),
		]
	);
	assert_eq!(
		scalar_decimal128_column_rows(&results, 4),
		vec![
			Some(246_800),
			Some(-100_000),
			None,
			Some(200_000),
			Some(1_999_800),
			Some(50_000),
		]
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
		"SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY sales.amount + 1",
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
fn computed_aggregate_arguments_resolve_by_distinct_signature() {
	// ADR 267: two SUMs over different computed arguments are distinct signatures, so they disambiguate to
	// unique internal names and the HAVING aggregate-call resolves to its own entry (was rejected as
	// ambiguous on the shared natural "sum").
	let _ =
		plan_sql("SELECT SUM(amount + 1), SUM(amount + 2) FROM sales HAVING SUM(amount + 1) > 0");
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
				if message == "unsupported CAST: Utf8 to Int64; only identity casts or Int32, Int64, Float32, and Float64 numeric conversions are supported"
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
				if message == "unsupported CAST: Utf8 to Int64; only identity casts or Int32, Int64, Float32, and Float64 numeric conversions are supported"
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
				if message == "unsupported TRY_CAST/SAFE_CAST: Utf8 to Int64; only identity casts or Int32, Int64, Float32, and Float64 numeric conversions are supported"
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
		FragmentPlanConfig::new_for_test(
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
			publish_mode: _,
			runtime_filter_probe_target: None,
		} if *exchange_id == ExchangeId::new(1)
			&& *partition_count == PartitionCount::new(4)
			&& partition_key.root_count() == 1
	));
	assert_eq!(stages[1].tasks.len(), 4);
	for (partition, task) in stages[1].tasks.iter().enumerate() {
		assert!(matches!(
			task.spec.plan,
			ExecutionPlan::Exchange { ref input, partition_count: 1, ref key }
				if key.is_empty_roots()
					&& matches!(input.as_ref(), ExecutionPlan::Aggregate { .. })
		));
		assert!(matches!(
				&task.spec.inputs[0],
				TaskInputBinding::ShuffleConsumer {
					exchange_id,
					producer_stage_id,
					partition_id,
					..
				} if *exchange_id == ExchangeId::new(1)
					&& *producer_stage_id == StageId::new(0)
					&& partition_id.0 == u32::try_from(partition).unwrap()
		));
		assert!(matches!(
			&task.spec.output,
			TaskOutputBinding::ShuffleProducer {
				exchange_id,
				partition_count,
				partition_key,
				publish_mode: _,
				runtime_filter_probe_target: None,
			} if *exchange_id == ExchangeId::new(0)
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
		} if *exchange_id == ExchangeId::new(0)
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
async fn derived_grouped_aggregate_top_k_with_outer_filter_fragments_to_terminal_sort_tail() {
	let (catalog, _) = fixture();
	let logical = plan_sql(
		"SELECT bucket, total \
		 FROM (SELECT region AS bucket, SUM(amount) AS total FROM sales GROUP BY region \
		       ORDER BY SUM(amount) DESC LIMIT 2) AS p \
		 WHERE total > 0",
	);
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind derived ordered aggregate");
	let stages =
		fragment_plan(bound, fragment_config()).expect("fragment derived ordered aggregate");

	assert_eq!(stages.len(), 3);
	let terminal = &stages[2].tasks[0].spec.plan;
	assert!(
		!matches!(terminal, ExecutionPlan::Sort { .. }),
		"outer derived consumers should stay above the terminal sort"
	);
	assert!(contains_execution_sort(terminal));
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
			ExecutionPlan::Exchange { ref input, partition_count: 1, ref key }
				if key.is_empty_roots()
					&& matches!(
						input.as_ref(),
						ExecutionPlan::Filter { input, .. }
							if matches!(input.as_ref(), ExecutionPlan::Aggregate { .. })
					)
		));
		assert!(matches!(
			&task.spec.output,
			TaskOutputBinding::ShuffleProducer {
				exchange_id,
				partition_count,
				partition_key,
				publish_mode: _,
				runtime_filter_probe_target: None,
			} if *exchange_id == ExchangeId::new(0)
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
async fn scalar_aggregate_having_only_fragments_to_gathered_terminal_fold() {
	let (catalog, _) = fixture();
	let logical = plan_sql("SELECT SUM(amount) FROM sales HAVING SUM(amount) > 10");
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind scalar HAVING aggregate");
	let stages = fragment_plan(bound, fragment_config()).expect("fragment scalar HAVING");

	// ADR 531: under the distributed posture the binder gathers the global fold's input,
	// so the scan produces into a single-partition shuffle and the fold + HAVING filter
	// run exactly once in a one-task terminal stage.
	assert_eq!(stages.len(), 2);
	assert_eq!(stages[0].stage_id, StageId::new(0));
	assert!(stages[0].upstream_stages.is_empty());
	assert_eq!(stages[1].upstream_stages, vec![StageId::new(0)]);
	assert_eq!(stages[0].tasks.len(), 1);
	assert!(matches!(
		&stages[0].tasks[0].spec.output,
		TaskOutputBinding::ShuffleProducer {
			exchange_id,
			partition_count,
			partition_key,
			publish_mode: _,
			runtime_filter_probe_target: None,
		} if *exchange_id == ExchangeId::new(0)
			&& *partition_count == PartitionCount::new(1)
			&& partition_key.is_empty_roots()
	));
	assert_eq!(stages[1].tasks.len(), 1);
	let task = &stages[1].tasks[0].spec;
	assert!(matches!(task.output, TaskOutputBinding::FinalResult));
	assert!(matches!(
		task.inputs[0],
		TaskInputBinding::ShuffleConsumer { exchange_id, .. }
			if exchange_id == ExchangeId::new(0)
	));
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
async fn scalar_aggregate_ordered_fragments_to_gathered_three_stage_chain() {
	let (catalog, _) = fixture();
	let logical = plan_sql("SELECT SUM(amount) FROM sales ORDER BY SUM(amount) DESC");
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind ordered scalar aggregate");
	let stages = fragment_plan(bound, fragment_config()).expect("fragment ordered scalar");

	// ADR 531: the binder's gather under the global fold stacks beneath the ORDER BY
	// funnel, so the plan fragments as scan producer -> one-task fold -> one-task sort.
	assert_eq!(stages.len(), 3);
	assert_eq!(stages[0].stage_id, StageId::new(0));
	assert_eq!(stages[1].stage_id, StageId::new(1));
	assert_eq!(stages[2].stage_id, StageId::new(2));
	assert!(stages[0].upstream_stages.is_empty());
	assert_eq!(stages[1].upstream_stages, vec![StageId::new(0)]);
	assert_eq!(stages[2].upstream_stages, vec![StageId::new(1)]);
	assert_eq!(stages[0].tasks.len(), 1);
	assert_eq!(stages[1].tasks.len(), 1);
	assert_eq!(stages[2].tasks.len(), 1);
	assert!(matches!(
		&stages[0].tasks[0].spec.output,
		TaskOutputBinding::ShuffleProducer {
			exchange_id,
			partition_count,
			partition_key,
			publish_mode: _,
			runtime_filter_probe_target: None,
		} if *exchange_id == ExchangeId::new(1)
			&& *partition_count == PartitionCount::new(1)
			&& partition_key.is_empty_roots()
	));
	let fold = &stages[1].tasks[0].spec;
	assert!(matches!(
		&fold.inputs[0],
		TaskInputBinding::ShuffleConsumer {
			exchange_id,
			producer_stage_id,
			..
		} if *exchange_id == ExchangeId::new(1) && *producer_stage_id == StageId::new(0)
	));
	assert!(matches!(
		&fold.output,
		TaskOutputBinding::ShuffleProducer {
			exchange_id,
			partition_count,
			partition_key,
			publish_mode: _,
			runtime_filter_probe_target: None,
		} if *exchange_id == ExchangeId::new(0)
			&& *partition_count == PartitionCount::new(1)
			&& partition_key.is_empty_roots()
	));
	assert!(matches!(
		&fold.plan,
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
	let terminal = &stages[2].tasks[0].spec;
	assert!(matches!(terminal.output, TaskOutputBinding::FinalResult));
	assert!(matches!(
		&terminal.inputs[0],
		TaskInputBinding::ShuffleConsumer {
			exchange_id,
			producer_stage_id,
			partition_id,
			..
		} if *exchange_id == ExchangeId::new(0)
			&& *producer_stage_id == StageId::new(1)
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
async fn non_aggregate_two_exchange_terminal_sort_fragments_recursively() {
	let (catalog, _) = fixture();
	let bound = bind(
		&project_between_exchanges_sort_plan(),
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect("bind non-aggregate two-exchange sort");

	let stages = fragment_plan(bound, fragment_config()).expect("fragment recursive sort");

	assert_two_exchange_terminal_sort_fragments(&stages);
}

#[tokio::test]
async fn filter_without_aggregate_between_terminal_exchanges_fragments_recursively() {
	let (catalog, _) = fixture();
	let bound = bind(
		&filter_without_aggregate_between_exchanges_sort_plan(),
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect("bind non-aggregate filtered two-exchange sort");

	let stages = fragment_plan(bound, fragment_config()).expect("fragment recursive sort");

	assert_two_exchange_terminal_sort_fragments(&stages);
}

fn assert_two_exchange_terminal_sort_fragments(
	stages: &[toron::distributed::scheduler::StagePlan],
) {
	assert_eq!(stages.len(), 3);
	assert!(stages[0].upstream_stages.is_empty());
	assert_eq!(stages[1].upstream_stages, vec![StageId::new(0)]);
	assert_eq!(stages[2].upstream_stages, vec![StageId::new(1)]);
	assert!(matches!(
		&stages[0].tasks[0].spec.output,
		TaskOutputBinding::ShuffleProducer {
			exchange_id,
			partition_count,
			..
		} if *exchange_id == ExchangeId::new(1)
			&& *partition_count == PartitionCount::new(4)
	));
	assert!(matches!(
		&stages[1].tasks[0].spec.output,
		TaskOutputBinding::ShuffleProducer {
			exchange_id,
			partition_count,
			partition_key,
			publish_mode: _,
			runtime_filter_probe_target: None,
		} if *exchange_id == ExchangeId::new(0)
			&& *partition_count == PartitionCount::new(1)
			&& partition_key.is_empty_roots()
	));
	assert!(matches!(
		&stages[2].tasks[0].spec.output,
		TaskOutputBinding::FinalResult
	));
	assert!(matches!(
		&stages[2].tasks[0].spec.plan,
		ExecutionPlan::Sort { .. }
	));
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

fn row_count(results: &[RecordBatch]) -> usize {
	results.iter().map(RecordBatch::num_rows).sum()
}

fn repeat_test_plan(rows: Vec<(i64, ScalarValue)>) -> ExecutionPlan {
	let input_schema = Arc::new(Schema::new(vec![
		Field::new("value", DataType::Int64, false),
		Field::new("repeat_count", DataType::Int64, true),
	]));
	let output_schema = Arc::new(Schema::new(vec![Field::new(
		"value",
		DataType::Int64,
		false,
	)]));
	ExecutionPlan::Repeat {
		input: Box::new(ExecutionPlan::SystemTable {
			kind: SystemTableKind::OptimizerExplain,
			schema: input_schema,
			rows: rows
				.into_iter()
				.map(|(value, count)| vec![ScalarValue::Int64(Some(value)), count])
				.collect(),
		}),
		count_column: ColumnOrdinal(1),
		output_schema,
	}
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

fn scalar_decimal128_column_rows(results: &[RecordBatch], column_idx: usize) -> Vec<Option<i128>> {
	let mut rows = Vec::new();
	for batch in results {
		let values = batch
			.column(column_idx)
			.as_any()
			.downcast_ref::<Decimal128Array>()
			.expect("Decimal128 column");
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

fn sorted_int32_optional_rows(results: &[RecordBatch], column_idx: usize) -> Vec<Option<i32>> {
	let mut rows = scalar_int32_column_rows(results, column_idx);
	rows.sort();
	rows
}

fn utf8_column_rows(results: &[RecordBatch], column_idx: usize) -> Vec<Option<String>> {
	let mut rows = Vec::new();
	for batch in results {
		let values = batch
			.column(column_idx)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("Utf8 column");
		for row in 0..batch.num_rows() {
			rows.push(if values.is_null(row) {
				None
			} else {
				Some(values.value(row).to_string())
			});
		}
	}
	rows
}

fn sorted_utf8_column_rows(results: &[RecordBatch], column_idx: usize) -> Vec<Option<String>> {
	let mut rows = utf8_column_rows(results, column_idx);
	rows.sort();
	rows
}

fn sorted_utf8_pair_rows(
	results: &[RecordBatch],
	left_idx: usize,
	right_idx: usize,
) -> Vec<(Option<String>, Option<String>)> {
	let mut rows = Vec::new();
	for batch in results {
		let left = batch
			.column(left_idx)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("left Utf8 column");
		let right = batch
			.column(right_idx)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("right Utf8 column");
		for row in 0..batch.num_rows() {
			let left = if left.is_null(row) {
				None
			} else {
				Some(left.value(row).to_string())
			};
			let right = if right.is_null(row) {
				None
			} else {
				Some(right.value(row).to_string())
			};
			rows.push((left, right));
		}
	}
	rows.sort();
	rows
}

fn sorted_utf8_int64_rows(
	results: &[RecordBatch],
	utf8_idx: usize,
	int64_idx: usize,
) -> Vec<(Option<String>, i64)> {
	let mut rows = Vec::new();
	for batch in results {
		let labels = batch
			.column(utf8_idx)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("Utf8 column");
		let values = batch
			.column(int64_idx)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("Int64 column");
		for row in 0..batch.num_rows() {
			let label = if labels.is_null(row) {
				None
			} else {
				Some(labels.value(row).to_string())
			};
			rows.push((label, values.value(row)));
		}
	}
	rows.sort();
	rows
}

fn sorted_int64_pair_rows(
	results: &[RecordBatch],
	left_idx: usize,
	right_idx: usize,
) -> Vec<(Option<i64>, Option<i64>)> {
	let mut rows = Vec::new();
	for batch in results {
		let left = batch
			.column(left_idx)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("left Int64 column");
		let right = batch
			.column(right_idx)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("right Int64 column");
		for row in 0..batch.num_rows() {
			let left = if left.is_null(row) {
				None
			} else {
				Some(left.value(row))
			};
			let right = if right.is_null(row) {
				None
			} else {
				Some(right.value(row))
			};
			rows.push((left, right));
		}
	}
	rows.sort();
	rows
}

fn assert_distinct_join_execution_shape(plan: &ExecutionPlan) {
	let ExecutionPlan::Aggregate {
		input, aggr_expr, ..
	} = plan
	else {
		panic!("expected DISTINCT join aggregate, got {plan:?}");
	};
	assert!(aggr_expr.is_empty());
	let ExecutionPlan::Exchange { input, .. } = input.as_ref() else {
		panic!("expected DISTINCT join exchange, got {input:?}");
	};
	let ExecutionPlan::Project { input, .. } = input.as_ref() else {
		panic!("expected DISTINCT join project, got {input:?}");
	};
	assert!(matches!(input.as_ref(), ExecutionPlan::Join { .. }));
}

fn contains_execution_join(plan: &ExecutionPlan) -> bool {
	match plan {
		ExecutionPlan::Join { .. } => true,
		ExecutionPlan::Union { left, right, .. } => {
			contains_execution_join(left) || contains_execution_join(right)
		}
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Window { input, .. }
		| ExecutionPlan::Take { input, .. }
		| ExecutionPlan::Repeat { input, .. }
		| ExecutionPlan::KernelDensity { input, .. } => contains_execution_join(input),
		ExecutionPlan::EmptyRelation { .. }
		| ExecutionPlan::SystemTable { .. }
		| ExecutionPlan::Scan { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. }
		| ExecutionPlan::RecursiveCte { .. }
		| ExecutionPlan::RecursiveRef { .. } => false,
	}
}

fn contains_logical_join(plan: &LogicalPlan) -> bool {
	match plan {
		LogicalPlan::Join { .. } => true,
		LogicalPlan::Union { left, right, .. } => {
			contains_logical_join(left) || contains_logical_join(right)
		}
		LogicalPlan::Alias { input, .. }
		| LogicalPlan::Filter { input, .. }
		| LogicalPlan::Project { input, .. }
		| LogicalPlan::Distinct { input }
		| LogicalPlan::Aggregate { input, .. }
		| LogicalPlan::Exchange { input, .. }
		| LogicalPlan::Sort { input, .. }
		| LogicalPlan::Window { input, .. }
		| LogicalPlan::Take { input, .. }
		| LogicalPlan::Repeat { input, .. }
		| LogicalPlan::KernelDensity { input, .. } => contains_logical_join(input),
		LogicalPlan::SetOpAll { left, right, .. } => {
			contains_logical_join(left) || contains_logical_join(right)
		}
		LogicalPlan::Scan { .. }
		| LogicalPlan::ScanNamed { .. }
		| LogicalPlan::Explain { .. }
		| LogicalPlan::ExplainAnalyze { .. }
		| LogicalPlan::CreateTable { .. }
		| LogicalPlan::CreateLogicalType { .. }
		| LogicalPlan::CreateTableAsSelect { .. }
		| LogicalPlan::DropTable { .. }
		| LogicalPlan::Delete { .. }
		| LogicalPlan::Update { .. }
		| LogicalPlan::Insert { .. }
		| LogicalPlan::InsertSelect { .. }
		| LogicalPlan::RecursiveCte { .. }
		| LogicalPlan::RecursiveRef { .. }
		| LogicalPlan::DeferredExists { .. }
		| LogicalPlan::DeferredScalarSubquery { .. } => false,
	}
}

fn contains_logical_distinct(plan: &LogicalPlan) -> bool {
	match plan {
		LogicalPlan::Distinct { .. } => true,
		LogicalPlan::Alias { input, .. }
		| LogicalPlan::Filter { input, .. }
		| LogicalPlan::Project { input, .. }
		| LogicalPlan::Aggregate { input, .. }
		| LogicalPlan::Exchange { input, .. }
		| LogicalPlan::Sort { input, .. }
		| LogicalPlan::Window { input, .. }
		| LogicalPlan::Take { input, .. }
		| LogicalPlan::Repeat { input, .. }
		| LogicalPlan::KernelDensity { input, .. } => contains_logical_distinct(input),
		LogicalPlan::Join { left, right, .. } => {
			contains_logical_distinct(left) || contains_logical_distinct(right)
		}
		LogicalPlan::Union { left, right, .. } => {
			contains_logical_distinct(left) || contains_logical_distinct(right)
		}
		LogicalPlan::SetOpAll { left, right, .. } => {
			contains_logical_distinct(left) || contains_logical_distinct(right)
		}
		LogicalPlan::Scan { .. }
		| LogicalPlan::ScanNamed { .. }
		| LogicalPlan::Explain { .. }
		| LogicalPlan::ExplainAnalyze { .. }
		| LogicalPlan::CreateTable { .. }
		| LogicalPlan::CreateLogicalType { .. }
		| LogicalPlan::CreateTableAsSelect { .. }
		| LogicalPlan::DropTable { .. }
		| LogicalPlan::Delete { .. }
		| LogicalPlan::Update { .. }
		| LogicalPlan::Insert { .. }
		| LogicalPlan::InsertSelect { .. }
		| LogicalPlan::RecursiveCte { .. }
		| LogicalPlan::RecursiveRef { .. }
		| LogicalPlan::DeferredExists { .. }
		| LogicalPlan::DeferredScalarSubquery { .. } => false,
	}
}

fn contains_execution_sort(plan: &ExecutionPlan) -> bool {
	match plan {
		ExecutionPlan::Sort { .. } => true,
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Window { input, .. }
		| ExecutionPlan::Take { input, .. }
		| ExecutionPlan::Repeat { input, .. }
		| ExecutionPlan::KernelDensity { input, .. } => contains_execution_sort(input),
		ExecutionPlan::Join { left, right, .. } => {
			contains_execution_sort(left) || contains_execution_sort(right)
		}
		ExecutionPlan::Union { left, right, .. } => {
			contains_execution_sort(left) || contains_execution_sort(right)
		}
		ExecutionPlan::EmptyRelation { .. }
		| ExecutionPlan::SystemTable { .. }
		| ExecutionPlan::Scan { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. }
		| ExecutionPlan::RecursiveCte { .. }
		| ExecutionPlan::RecursiveRef { .. } => false,
	}
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

fn region_min_amount_max_amount_rows(
	results: &[RecordBatch],
) -> Vec<(Option<String>, Option<i32>, Option<i32>)> {
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
			.downcast_ref::<Int32Array>()
			.expect("max amount column");
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
	rows.sort_by(|left, right| left.0.cmp(&right.0));
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

/// ADR 240: a distinct aggregate now lowers to a fan-out + recombine wrapped in normalizing/branch
/// Projects. Walk through those wrappers to the dedup branch's `Aggregate` over a `Distinct` input
/// (its argument is a generated dedup column), returning its `(group_expr, aggr_expr)`.
fn find_aggregate_over_distinct(
	plan: &LogicalPlan,
) -> (&[Expr], &[(AggregateFunction, Vec<Expr>)]) {
	fn search(plan: &LogicalPlan) -> Option<(&[Expr], &[(AggregateFunction, Vec<Expr>)])> {
		match plan {
			LogicalPlan::Aggregate {
				input,
				group_expr,
				aggr_expr,
			} => {
				let mut inner = input.as_ref();
				if let LogicalPlan::Exchange { input, .. } = inner {
					inner = input.as_ref();
				}
				if matches!(inner, LogicalPlan::Distinct { .. }) {
					return Some((group_expr.as_slice(), aggr_expr.as_slice()));
				}
				search(input)
			}
			LogicalPlan::Project { input, .. }
			| LogicalPlan::Filter { input, .. }
			| LogicalPlan::Exchange { input, .. }
			| LogicalPlan::Distinct { input } => search(input),
			LogicalPlan::Join { left, right, .. } => search(left).or_else(|| search(right)),
			_ => None,
		}
	}
	search(plan).expect("plan must contain an aggregate over a distinct input")
}

async fn rune_plan(script: &str) -> LogicalPlan {
	let config = RuneSandboxConfig::v1_default();
	let engine = RuneEngine::with_config(config).expect("Rune engine");
	engine
		.execute_script(script, &config)
		.await
		.expect("Rune-authored plan")
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

fn field_names(schema: &SchemaRef) -> Vec<&str> {
	schema
		.fields()
		.iter()
		.map(|field| field.name().as_str())
		.collect()
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

fn assert_mul_with_casted_right(
	program: &BoundExprProgram,
	root_index: usize,
	right_cast_type: DataType,
	left_column_name: &str,
	right_column_name: &str,
) {
	let root = program.roots[root_index];
	let BoundExprNode::Binary {
		op: BoundBinaryOp::Mul,
		left,
		right,
	} = program.node(root).expect("mul root")
	else {
		panic!("expected Mul root, got {:?}", program.node(root));
	};
	assert_column_node(program, *left, left_column_name, None, None);
	let BoundExprNode::Cast { input, target } = program.node(*right).expect("right cast") else {
		panic!(
			"expected casted right child, got {:?}",
			program.node(*right)
		);
	};
	assert_eq!(target, &right_cast_type);
	assert_column_node(program, *input, right_column_name, None, None);
}

fn assert_mul_with_casted_left(
	program: &BoundExprProgram,
	root_index: usize,
	left_cast_type: DataType,
	left_column_name: &str,
	right_column_name: &str,
) {
	let root = program.roots[root_index];
	let BoundExprNode::Binary {
		op: BoundBinaryOp::Mul,
		left,
		right,
	} = program.node(root).expect("mul root")
	else {
		panic!("expected Mul root, got {:?}", program.node(root));
	};
	let BoundExprNode::Cast { input, target } = program.node(*left).expect("left cast") else {
		panic!("expected casted left child, got {:?}", program.node(*left));
	};
	assert_eq!(target, &left_cast_type);
	assert_column_node(program, *input, left_column_name, None, None);
	assert_column_node(program, *right, right_column_name, None, None);
}

fn assert_mul_with_casted_right_expr(
	program: &BoundExprProgram,
	root_index: usize,
	right_cast_type: DataType,
) {
	let root = program.roots[root_index];
	let BoundExprNode::Binary {
		op: BoundBinaryOp::Mul,
		left,
		right,
	} = program.node(root).expect("mul root")
	else {
		panic!("expected Mul root, got {:?}", program.node(root));
	};
	assert_column_node(program, *left, "decimal_price", None, None);
	let BoundExprNode::Cast { input, target } = program.node(*right).expect("right cast") else {
		panic!(
			"expected casted right child, got {:?}",
			program.node(*right)
		);
	};
	assert_eq!(target, &right_cast_type);
	assert!(
		matches!(
			program.node(*input),
			Some(BoundExprNode::Binary {
				op: BoundBinaryOp::Add,
				..
			})
		),
		"computed Int expression must be cast as a whole, got {:?}",
		program.node(*input)
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
	FragmentPlanConfig::new_for_test(
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
	let left_dups_schema = left_dups_schema();
	let right_dups_schema = right_dups_schema();
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
	catalog
		.register_descriptor(
			TableId(4),
			descriptor(4, "left_dups", left_dups_schema.as_ref(), "k"),
		)
		.expect("register left_dups");
	catalog
		.register_descriptor(
			TableId(5),
			descriptor(5, "right_dups", right_dups_schema.as_ref(), "k"),
		)
		.expect("register right_dups");
	let storage = Arc::new(FixtureScan {
		batches: HashMap::from([
			(TableId(1), sales_batch(sales_schema)),
			(TableId(2), regions_batch(regions_schema)),
			(TableId(3), empty_sales_batch(empty_sales_schema)),
			(TableId(4), left_dups_batch(left_dups_schema)),
			(TableId(5), right_dups_batch(right_dups_schema)),
		]),
	});
	(catalog, storage)
}

fn tukey_fixture() -> (MockCatalog, Arc<TukeyScan>) {
	let schema = Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("category", DataType::Utf8, true),
		Field::new("value", DataType::Float64, true),
	]));
	let mut ids = Vec::new();
	let mut categories = Vec::new();
	let mut values = Vec::new();
	let mut push = |category: Option<&'static str>, value: Option<f64>| {
		ids.push(ids.len() as i64 + 1);
		categories.push(category);
		values.push(value);
	};

	// q1=0, median=2, q3=4: both mathematical fences (-6 and 10) are observations.
	for value in [-6.0, -1.0, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 10.0] {
		push(Some("fence"), Some(value));
	}
	// q1=10, median=40, q3=70: observed whiskers 0/80 differ from fences -80/160 and
	// raw min/max -100/300. The repeated low outlier pins observation multiplicity.
	for value in [
		-100.0, -100.0, 0.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 200.0, 300.0,
	] {
		push(Some("spread"), Some(value));
	}
	// Zero IQR: unequal values are outliers and repeated ordinary values do not create rows.
	for value in [0.0, 5.0, 5.0, 5.0, 5.0, 5.0, 10.0] {
		push(Some("zero"), Some(value));
	}
	push(None, Some(123.0));
	push(Some("all_null"), None);

	let batch = RecordBatch::try_new(
		schema.clone(),
		vec![
			Arc::new(Int64Array::from(ids)),
			Arc::new(StringArray::from(categories)),
			Arc::new(Float64Array::from(values)),
		],
	)
	.expect("Tukey fixture batch");
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(TableId(6), descriptor(6, "measurements", &schema, "id"))
		.expect("register Tukey measurements");
	(catalog, Arc::new(TukeyScan { batch }))
}

type TukeyRow = (String, f64, f64, f64, f64, f64, Option<f64>);

fn tukey_rows(results: &[RecordBatch]) -> Vec<TukeyRow> {
	let mut rows = Vec::new();
	for batch in results {
		let categories = batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("Tukey category");
		let floats = (1..=6)
			.map(|column| {
				batch
					.column(column)
					.as_any()
					.downcast_ref::<Float64Array>()
					.expect("Tukey numeric column")
			})
			.collect::<Vec<_>>();
		for row in 0..batch.num_rows() {
			rows.push((
				categories.value(row).to_string(),
				floats[0].value(row),
				floats[1].value(row),
				floats[2].value(row),
				floats[3].value(row),
				floats[4].value(row),
				(!floats[5].is_null(row)).then(|| floats[5].value(row)),
			));
		}
	}
	rows
}

struct TukeyScan {
	batch: RecordBatch,
}

#[async_trait::async_trait]
impl StorageScan for TukeyScan {
	async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
		let mut batch = self.batch.clone();
		if let Some(predicate) = &request.pbc_predicate {
			batch = wendelon::schema::pbc::execute(predicate, &batch).map_err(|error| {
				StorageError::InvalidPbcProgram {
					message: format!("{error:?}"),
				}
			})?;
		}
		if let Some(projected_columns) = request.projected_columns {
			let indices = projected_columns
				.into_iter()
				.map(|ordinal| ordinal.0 as usize)
				.collect::<Vec<_>>();
			batch = batch.project(&indices)?;
		}
		Ok(ScanStream::from_batches(
			batch.schema(),
			request.session_id,
			request.query_id,
			toron::storage::ScanOrigin::BaseRelation,
			vec![batch],
		))
	}
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
		Field::new("decimal_price", DataType::Decimal128(15, 2), true),
		Field::new("decimal_wide", DataType::Decimal128(25, 2), true),
	]))
}

fn regions_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("region", DataType::Utf8, false),
		Field::new("active", DataType::Int32, false),
	]))
}

fn left_dups_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("k", DataType::Int64, false),
		Field::new("label", DataType::Utf8, true),
	]))
}

fn right_dups_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("k", DataType::Int64, false),
		Field::new("tier", DataType::Utf8, true),
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
			Arc::new(
				Decimal128Array::from(vec![
					Some(1_234_i128),
					Some(-500),
					None,
					Some(1_000),
					Some(9_999),
					Some(250),
				])
				.with_precision_and_scale(15, 2)
				.expect("decimal_price column"),
			),
			Arc::new(
				Decimal128Array::from(vec![
					Some(123_456_789_012_345_i128),
					Some(-222_222_222_222_222),
					None,
					Some(1_000_000_000_000),
					Some(99_999_999_999_999),
					Some(42),
				])
				.with_precision_and_scale(25, 2)
				.expect("decimal_wide column"),
			),
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
			Arc::new(
				Decimal128Array::from(Vec::<Option<i128>>::new())
					.with_precision_and_scale(15, 2)
					.expect("empty decimal_price column"),
			),
			Arc::new(
				Decimal128Array::from(Vec::<Option<i128>>::new())
					.with_precision_and_scale(25, 2)
					.expect("empty decimal_wide column"),
			),
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

fn left_dups_batch(schema: SchemaRef) -> RecordBatch {
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(vec![1, 1, 2, 3, 3])),
			Arc::new(StringArray::from(vec![
				Some("A"),
				Some("A"),
				Some("B"),
				Some("missing"),
				Some("missing"),
			])),
		],
	)
	.expect("left_dups batch")
}

fn right_dups_batch(schema: SchemaRef) -> RecordBatch {
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(vec![1, 1, 2, 4, 4])),
			Arc::new(StringArray::from(vec![
				Some("gold"),
				Some("gold"),
				Some("silver"),
				Some("orphan"),
				Some("orphan"),
			])),
		],
	)
	.expect("right_dups batch")
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
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single(primary_key)),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
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
			if projected_columns.is_empty() {
				let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
				batch = RecordBatch::try_new_with_options(
					Arc::new(Schema::empty()),
					Vec::new(),
					&options,
				)?;
			} else {
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
		}
		Ok(ScanStream::from_batches(
			batch.schema(),
			request.session_id,
			request.query_id,
			toron::storage::ScanOrigin::BaseRelation,
			vec![batch],
		))
	}
}

#[tokio::test]
async fn adr560_spread_spellings_and_aliases_agree_through_sql() {
	let rows = run_logical(&plan_sql(
		"SELECT VAR_POP(amount), VAR_SAMP(amount), STDDEV_POP(amount), STDDEV_SAMP(amount), \
		 VARIANCE(amount), STDDEV(amount) FROM sales",
	))
	.await;
	let read = |idx: usize| {
		rows[0]
			.column(idx)
			.as_any()
			.downcast_ref::<arrow::array::Float64Array>()
			.expect("Float64 spread output")
			.value(0)
	};
	// The bare names are documented aliases of the sample forms, so the
	// mapping is asserted rather than left to folklore.
	assert!(
		(read(4) - read(1)).abs() < 1e-12,
		"VARIANCE must equal VAR_SAMP"
	);
	assert!(
		(read(5) - read(3)).abs() < 1e-12,
		"STDDEV must equal STDDEV_SAMP"
	);
	assert!(
		read(1) > read(0),
		"the sample form divides by a smaller denominator, so it must exceed the population form"
	);
	assert!((read(2) - read(0).sqrt()).abs() < 1e-12);
	assert!((read(3) - read(1).sqrt()).abs() < 1e-12);
}

#[tokio::test]
async fn adr560_decimal_input_narrows_to_float64_at_its_declared_scale() {
	// decimal_price is Decimal128(15, 2) holding 12.34, -5.00, NULL, 10.00,
	// 99.99, 2.50. The reader yields the unscaled i128, so a spread that ignored
	// the scale would be wrong by a factor of 10^4 — large enough to look like a
	// different column rather than a rounding difference.
	let (catalog, _) = fixture();
	let logical = plan_sql("SELECT VAR_POP(decimal_price) FROM sales");
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind VAR_POP over Decimal128");
	assert_eq!(
		bound.schema().field(0).data_type(),
		&DataType::Float64,
		"a spread narrows a decimal rather than pretending to a scale"
	);

	let rows = run_logical(&logical).await;
	let observed = rows[0]
		.column(0)
		.as_any()
		.downcast_ref::<Float64Array>()
		.expect("Float64 spread output")
		.value(0);
	let values = [12.34_f64, -5.00, 10.00, 99.99, 2.50];
	let mean = values.iter().sum::<f64>() / values.len() as f64;
	let expected = values
		.iter()
		.map(|value| (value - mean).powi(2))
		.sum::<f64>()
		/ values.len() as f64;
	assert!(
		(observed - expected).abs() < 1e-6,
		"expected {expected}, got {observed}"
	);
}

#[tokio::test]
async fn adr560_grouped_spread_skips_nulls_rather_than_counting_them() {
	// A NULL must not reach the accumulator at all, so a group containing one
	// answers exactly as the same group without it. Comparing against a
	// NULL-free filter makes the claim falsifiable rather than assumed.
	let with_nulls = run_logical(&plan_sql(
		"SELECT region, VAR_SAMP(amount) FROM sales GROUP BY region ORDER BY region",
	))
	.await;
	let without_nulls = run_logical(&plan_sql(
		"SELECT region, VAR_SAMP(amount) FROM sales WHERE amount IS NOT NULL \
		 GROUP BY region ORDER BY region",
	))
	.await;
	let read = |batches: &Vec<RecordBatch>| {
		batches[0]
			.column(1)
			.as_any()
			.downcast_ref::<Float64Array>()
			.expect("Float64 spread output")
			.iter()
			.collect::<Vec<_>>()
	};
	assert_eq!(
		read(&with_nulls),
		read(&without_nulls),
		"a skipped NULL must leave the spread unchanged"
	);
}

fn string_agg_value(batches: &[RecordBatch]) -> Option<String> {
	let column = batches[0].column(batches[0].num_columns() - 1);
	let values = column
		.as_any()
		.downcast_ref::<StringArray>()
		.expect("STRING_AGG Utf8 output");
	(!values.is_null(0)).then(|| values.value(0).to_string())
}

#[tokio::test]
async fn adr562_string_agg_orders_skips_nulls_and_reverses_under_desc() {
	// region row 6 is NULL: it contributes neither element nor separator.
	let ascending = run_logical(&plan_sql(
		"SELECT STRING_AGG(region, ', ' ORDER BY id) FROM sales",
	))
	.await;
	assert_eq!(
		string_agg_value(&ascending).as_deref(),
		Some("east, west, east, east, west")
	);

	let descending = run_logical(&plan_sql(
		"SELECT STRING_AGG(region, ', ' ORDER BY id DESC) FROM sales",
	))
	.await;
	assert_eq!(
		string_agg_value(&descending).as_deref(),
		Some("west, east, east, west, east")
	);

	// Skipping is equivalence with pre-filtering, not merely absence.
	let filtered = run_logical(&plan_sql(
		"SELECT STRING_AGG(region, ', ' ORDER BY id) FROM sales WHERE region IS NOT NULL",
	))
	.await;
	assert_eq!(string_agg_value(&ascending), string_agg_value(&filtered));
}

#[tokio::test]
async fn adr562_string_agg_null_keys_place_last_ascending_first_descending() {
	// amount is NULL at rows 3 (east) and 5 (west); row 6's region is NULL and
	// is skipped as a value regardless of its key.
	let ascending = run_logical(&plan_sql(
		"SELECT STRING_AGG(region, ',' ORDER BY amount) FROM sales",
	))
	.await;
	assert_eq!(
		string_agg_value(&ascending).as_deref(),
		Some("east,west,east,east,west"),
		"NULL keys must place last under ASC"
	);
	let descending = run_logical(&plan_sql(
		"SELECT STRING_AGG(region, ',' ORDER BY amount DESC) FROM sales",
	))
	.await;
	assert_eq!(
		string_agg_value(&descending).as_deref(),
		Some("east,west,east,west,east"),
		"NULL keys must place first under DESC"
	);
}

#[tokio::test]
async fn adr562_string_agg_multi_key_sorts_lexicographically_with_directions() {
	let rows = run_logical(&plan_sql(
		"SELECT STRING_AGG(region, ',' ORDER BY status, id DESC) FROM sales",
	))
	.await;
	// closed (row 3, east) first; then open by id descending: 5,4,2,1.
	assert_eq!(
		string_agg_value(&rows).as_deref(),
		Some("east,west,east,west,east")
	);
}

#[tokio::test]
async fn adr562_string_agg_empty_and_all_null_groups_answer_null() {
	let empty = run_logical(&plan_sql(
		"SELECT STRING_AGG(region, ',' ORDER BY id) FROM empty_sales",
	))
	.await;
	assert_eq!(
		string_agg_value(&empty),
		None,
		"an empty group answers NULL"
	);

	// Row 6 is the only member of its id group and its region is NULL, so the
	// group is all-NULL without relying on a filter the test mock may drop.
	let grouped = run_logical(&plan_sql(
		"SELECT id, STRING_AGG(region, ',' ORDER BY id) FROM sales GROUP BY id ORDER BY id",
	))
	.await;
	let concats = grouped[0]
		.column(1)
		.as_any()
		.downcast_ref::<StringArray>()
		.expect("STRING_AGG column");
	assert!(
		concats.is_null(5),
		"an all-NULL group answers NULL, got {:?}",
		concats.value(5)
	);
	assert_eq!(concats.value(0), "east");
}

#[tokio::test]
async fn adr562_string_agg_grouped_concatenates_per_group() {
	let rows = run_logical(&plan_sql(
		"SELECT status, STRING_AGG(region, '|' ORDER BY id) FROM sales \
		 GROUP BY status ORDER BY status",
	))
	.await;
	let statuses = rows[0]
		.column(0)
		.as_any()
		.downcast_ref::<StringArray>()
		.expect("status column");
	let concats = rows[0]
		.column(1)
		.as_any()
		.downcast_ref::<StringArray>()
		.expect("STRING_AGG column");
	assert_eq!(statuses.value(0), "closed");
	assert_eq!(concats.value(0), "east");
	assert_eq!(statuses.value(1), "open");
	assert_eq!(concats.value(1), "east|west|east|west");
}

#[tokio::test]
async fn adr562_string_agg_rejects_unordered_non_literal_null_and_distinct_forms() {
	let unordered = SqlPlanner::new()
		.plan("SELECT STRING_AGG(region, ',') FROM sales")
		.expect_err("unordered STRING_AGG must be rejected");
	assert!(
		unordered.to_string().contains("nondeterministic"),
		"the rejection must name the reason, got {unordered}"
	);

	let non_literal = SqlPlanner::new()
		.plan("SELECT STRING_AGG(region, status ORDER BY id) FROM sales")
		.expect_err("a non-literal separator must be rejected");
	assert!(non_literal.to_string().contains("string literal"));

	let null_separator = SqlPlanner::new()
		.plan("SELECT STRING_AGG(region, NULL ORDER BY id) FROM sales")
		.expect_err("a NULL separator must be rejected");
	assert!(
		null_separator
			.to_string()
			.contains("not a STRING_AGG separator")
	);

	let distinct = SqlPlanner::new()
		.plan("SELECT STRING_AGG(DISTINCT region, ',' ORDER BY id) FROM sales")
		.expect_err("STRING_AGG DISTINCT is deferred");
	assert!(distinct.to_string().contains("deferred"));

	let windowed = SqlPlanner::new()
		.plan("SELECT STRING_AGG(region, ',' ORDER BY id) OVER (PARTITION BY status) FROM sales")
		.expect_err("STRING_AGG is not window eligible");
	assert!(windowed.to_string().contains("window aggregate functions"));

	// An empty separator concatenates without joining text.
	let bare = run_logical(&plan_sql(
		"SELECT STRING_AGG(region, '' ORDER BY id) FROM sales",
	))
	.await;
	assert_eq!(
		string_agg_value(&bare).as_deref(),
		Some("eastwesteasteastwest")
	);
}

#[tokio::test]
async fn adr562_rune_string_agg_matches_sql_through_one_identifier() {
	let sql = run_logical(&plan_sql(
		"SELECT status, STRING_AGG(region, ', ' ORDER BY id DESC) FROM sales \
		 GROUP BY status ORDER BY status",
	))
	.await;
	let rune = run_logical(
		&rune_plan(
			r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .aggregate([col("status")], [string_agg(col("region"), ", ", [desc(col("id"))])])
               .sort([asc(col("status"))])
        }
    "#,
		)
		.await,
	)
	.await;
	assert_eq!(
		canonical_arrow_ipc_digest(&sql, SortPolicy::FullRowLex).expect("SQL digest"),
		canonical_arrow_ipc_digest(&rune, SortPolicy::FullRowLex).expect("Rune digest"),
		"SQL and Rune must reach one registry identifier and one result"
	);
}

#[tokio::test]
async fn adr563_exact_quantile_names_are_refused_with_a_directive() {
	// The refusal is a decision, not ignorance: each error names the honest
	// alternative instead of dumping the expression.
	let median = SqlPlanner::new()
		.plan("SELECT MEDIAN(price) FROM sales")
		.expect_err("MEDIAN must be refused");
	assert!(
		median.to_string().contains("APPROX_QUANTILE(value, 0.5)"),
		"MEDIAN's refusal must direct to the median spelling, got {median}"
	);

	let cont_plain = SqlPlanner::new()
		.plan("SELECT PERCENTILE_CONT(0.5, price) FROM sales")
		.expect_err("plain PERCENTILE_CONT must be refused");
	assert!(cont_plain.to_string().contains("APPROX_QUANTILE(value, q)"));

	let cont_within = SqlPlanner::new()
		.plan("SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) FROM sales")
		.expect_err("WITHIN GROUP PERCENTILE_CONT must be refused");
	assert!(
		cont_within
			.to_string()
			.contains("APPROX_QUANTILE(value, q)")
	);

	let disc = SqlPlanner::new()
		.plan("SELECT PERCENTILE_DISC(0.5, price) FROM sales")
		.expect_err("PERCENTILE_DISC must be refused");
	assert!(disc.to_string().contains("does not compute"));

	let grouped = SqlPlanner::new()
		.plan("SELECT status, MEDIAN(price) FROM sales GROUP BY status")
		.expect_err("the refusal must fire under GROUP BY too");
	assert!(grouped.to_string().contains("APPROX_QUANTILE(value, 0.5)"));

	// The table refuses exactly its names: a typo still gets the generic error,
	// never a quantile lecture.
	let typo = SqlPlanner::new()
		.plan("SELECT MEDIANN(price) FROM sales")
		.expect_err("an unknown name still errs generically");
	assert!(
		!typo.to_string().contains("APPROX_QUANTILE"),
		"the refusal table must not swallow unknown names, got {typo}"
	);
}
