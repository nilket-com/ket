use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
	Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use toron::binder::{PlannerConfig, bind};
use toron::catalog::{MockCatalog, TableRef};
use toron::distributed::budget::{QueryBudget, TaskBudget};
use toron::distributed::fragment::{FragmentPlanConfig, fragment_plan};
use toron::distributed::identity::QueryId;
use toron::error::{BindingError, ExecutionError, FrontendError, StorageError};
use toron::exec::Executor;
use toron::ir::{
	BoundBinaryOp, BoundExprNode, BroadcastBuildSide, CUSTOMER_ID_LOGICAL_TYPE,
	DURABLE_LOGICAL_TYPE_MIN_ID, ExecutionPlan, Expr, FilterDisposition, JoinKind, JoinStrategy,
	JoinStrategyIntent, JoinStrategyPinSource, LogicalPlan, LogicalTypeId, ScalarValue,
	SystemTableKind, TableId, decode_column_logical_type, field_with_logical_type_metadata,
};
use toron::rune_engine::{RuneEngine, RuneSandboxConfig};
use toron::sql_planner::SqlPlanner;
use toron::storage::{ScanRequest, ScanStream, StorageScan, TenantId};
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, SchemaEvolutionPolicy, TableObjectId,
	TableSchemaDescriptor,
	pbc::{OP_IN_LIST, OP_NOT},
};
use wendelon_client::{
	LogicalTypeDefinition, LogicalTypePhysical, LogicalTypeUnit, UnitOffset, UnitQuantityKind,
	UnitScale,
};

fn sandbox_config() -> RuneSandboxConfig {
	RuneSandboxConfig {
		wall_timeout: Duration::from_secs(2),
		..RuneSandboxConfig::v1_default()
	}
}

fn fragment_config() -> FragmentPlanConfig {
	FragmentPlanConfig::new_for_test(
		QueryId::new(),
		TaskBudget::from_query_budget(QueryBudget::test_default()),
	)
}

const RUNE_TEMPERATURE_CELSIUS_DELTA: LogicalTypeId = LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID);
const RUNE_TEMPERATURE_CELSIUS: LogicalTypeId = LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID + 2);
const RUNE_TEMPERATURE_KELVIN: LogicalTypeId = LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID + 3);

async fn rune_plan(script: &str) -> Result<LogicalPlan, FrontendError> {
	let config = sandbox_config();
	let engine = RuneEngine::with_config(config).expect("engine");
	engine.execute_script(script, &config).await
}

#[tokio::test]
async fn rune_ctx_sql_select_returns_lazy_dataframe() {
	let plan = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT id, amount FROM sales")
        }
    "#,
	)
	.await
	.expect("ctx.sql SELECT");

	assert!(contains_scan_named(&plan, "sales"));
	assert!(!is_terminal_plan(&plan));
	assert!(matches!(plan, LogicalPlan::Project { .. }));
}

#[tokio::test]
async fn rune_ctx_sql_rejects_multi_statement_input() {
	let error = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT id FROM sales; DROP TABLE sales")
        }
    "#,
	)
	.await
	.expect_err("ctx.sql must reject multiple statements");

	assert_frontend_message_contains(error, "exactly one SQL statement");
}

#[tokio::test]
async fn rune_ctx_sql_rejects_create_insert_update_delete_drop() {
	for query in [
		"CREATE TABLE created (id INT)",
		"CREATE TABLE created AS SELECT id FROM sales",
		"INSERT INTO sales (id, region, amount, status) VALUES (7, 'east', 9, 'open')",
		"UPDATE sales SET amount = 99 WHERE id = 1",
		"DELETE FROM sales WHERE id = 1",
		"DROP TABLE sales",
	] {
		let script = format!(
			r#"
        pub fn main(ctx) {{
            ctx.sql({query:?})
        }}
    "#
		);

		let error = rune_plan(&script)
			.await
			.expect_err("ctx.sql must reject terminal statements");
		assert_frontend_message_contains(error, "ctx.sql accepts single query relations only");
	}
}

#[tokio::test]
async fn ctx_sql_explain_is_read_only_relation() {
	let plan = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("EXPLAIN SELECT * FROM sales")
        }
    "#,
	)
	.await
	.expect("ctx.sql EXPLAIN SELECT");

	assert!(matches!(plan, LogicalPlan::Explain { .. }));
	assert!(!is_terminal_plan(&plan));
}

#[tokio::test]
async fn ctx_sql_explain_analyze_is_read_only_relation() {
	let plan = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("EXPLAIN ANALYZE SELECT * FROM sales")
        }
    "#,
	)
	.await
	.expect("ctx.sql EXPLAIN ANALYZE SELECT");

	assert!(matches!(plan, LogicalPlan::ExplainAnalyze { .. }));
	assert!(!is_terminal_plan(&plan));
}

#[tokio::test]
async fn ctx_sql_explain_rejects_mutation_explain() {
	let error = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("EXPLAIN INSERT INTO sales (id, region, amount, status) VALUES (7, 'east', 9, 'open')")
        }
    "#,
	)
	.await
	.expect_err("ctx.sql must reject EXPLAIN over mutation");

	assert_frontend_message_contains(error, "EXPLAIN supports SELECT queries only");
}

#[tokio::test]
async fn rune_sql_group_by_alias_can_feed_outer_sum_and_bind() {
	let logical = sql_group_by_alias_outer_sum_plan().await;
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind Rune aggregate over SQL alias");

	assert_eq!(bound.schema().fields().len(), 1);
	assert_eq!(bound.schema().field(0).name(), "sum");
}

#[tokio::test]
async fn rune_sql_group_by_alias_outer_sum_executes_if_existing_harness_supports_it() {
	let batches = run_logical(&sql_group_by_alias_outer_sum_plan().await).await;

	assert_eq!(int64_column_rows(&batches, 0), vec![Some(6)]);
}

#[tokio::test]
async fn rune_sql_filter_then_rune_aggregate_composes_single_logical_plan() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT * FROM sales WHERE status = 'open'")
               .filter(col("amount").gt(lit(10)))
               .aggregate([], [sum(col("amount"))])
        }
    "#,
	)
	.await
	.expect("compose SQL filter with Rune aggregate");

	let LogicalPlan::Aggregate { input, .. } = &logical else {
		panic!("expected outer Rune aggregate, got {logical:?}");
	};
	assert!(matches!(input.as_ref(), LogicalPlan::Filter { .. }));

	let batches = run_logical(&logical).await;
	assert_eq!(int64_column_rows(&batches, 0), vec![Some(50)]);
}

#[tokio::test]
async fn rune_sql_composition_preserves_existing_pushdown_path() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT * FROM sales")
               .filter(col("amount").gt(lit(10)))
        }
    "#,
	)
	.await
	.expect("compose SQL scan with Rune filter");
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind composed Rune SQL filter");
	let (_, disposition, scan) = find_filter(&bound);

	assert_eq!(*disposition, FilterDisposition::FullyPushed);
	assert!(
		scan_pbc(scan).is_some(),
		"composed filter must reach scan PBC"
	);
}

#[tokio::test]
async fn rune_dataframe_top_k_executes() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .sort([col("amount").desc().nulls_last(), col("id").asc()])
               .limit(2)
               .project([col("id"), col("amount")])
        }
    "#,
	)
	.await
	.expect("Rune dataframe top-k");

	let batches = run_logical(&logical).await;
	assert_eq!(int64_column_rows(&batches, 0), vec![Some(4), Some(2)]);
	assert_eq!(int32_column_rows(&batches, 1), vec![Some(30), Some(20)]);
}

#[tokio::test]
async fn rune_sql_relation_top_k_executes() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT id, amount FROM sales")
               .sort([desc(col("id"))])
               .limit(3)
        }
    "#,
	)
	.await
	.expect("Rune SQL relation top-k");

	let batches = run_logical(&logical).await;
	assert_eq!(
		int64_column_rows(&batches, 0),
		vec![Some(6), Some(5), Some(4)]
	);
}

#[tokio::test]
async fn rune_null_ordering_top_k_executes() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .sort([asc(col("amount")).nulls_first()])
               .limit(2)
               .project([col("id")])
        }
    "#,
	)
	.await
	.expect("Rune null ordering top-k");

	let batches = run_logical(&logical).await;
	assert_eq!(int64_column_rows(&batches, 0), vec![Some(3), Some(5)]);
}

#[tokio::test]
async fn rune_limit_without_sort_rejects() {
	for script in [
		r#"
        pub fn main(ctx) {
            ctx.table("sales").limit(1)
        }
    "#,
		r#"
        pub fn main(ctx) {
            ctx.table("sales").offset(1)
        }
    "#,
	] {
		let error = rune_plan(script)
			.await
			.expect_err("Rune limit/offset without sort should reject");
		assert_frontend_message_contains(error, "LIMIT/OFFSET without ORDER BY is not supported");
	}
}

#[tokio::test]
async fn rune_take_is_unordered_and_chainable() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .take(4)
               .take(2)
               .project([col("id")])
        }
    "#,
	)
	.await
	.expect("Rune unordered take");
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind Rune unordered take");

	let ExecutionPlan::Project { input, .. } = bound else {
		panic!("expected project over take");
	};
	let ExecutionPlan::Take { input, limit, .. } = input.as_ref() else {
		panic!("expected outer take");
	};
	assert_eq!(*limit, 2);
	let ExecutionPlan::Take { input, limit, .. } = input.as_ref() else {
		panic!("expected inner take");
	};
	assert_eq!(*limit, 4);
	let ExecutionPlan::Scan { row_limit, .. } = input.as_ref() else {
		panic!("expected scan under take chain");
	};
	assert_eq!(*row_limit, Some(2));
}

#[tokio::test]
async fn rune_inner_join_executes_equivalently_to_sql_inner_join() {
	let rune_logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").alias("s")
               .join(
                   ctx.table("regions").alias("r"),
                   qcol("s", "region").eq(qcol("r", "region"))
               )
               .project_named([qcol("s", "id").named("id")])
        }
    "#,
	)
	.await
	.expect("Rune join plan");
	let sql_logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT sales.id FROM sales JOIN regions ON sales.region = regions.region")
        }
    "#,
	)
	.await
	.expect("SQL join plan");

	assert_eq!(
		sorted_int64_column_rows(&run_logical(&rune_logical).await, 0),
		sorted_int64_column_rows(&run_logical(&sql_logical).await, 0)
	);
}

#[tokio::test]
async fn rune_join_wraps_both_inputs_in_hash_exchanges() {
	let logical = rune_region_join_plan().await;
	let LogicalPlan::Join {
		left,
		right,
		kind,
		on,
		strategy_intent,
		..
	} = logical
	else {
		panic!("expected Rune join, got {logical:?}");
	};

	assert_eq!(kind, JoinKind::Inner);
	assert_eq!(
		strategy_intent,
		JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::HashPartitioned,
			source: JoinStrategyPinSource::Rune,
			build_side: None,
		}
	);
	assert_eq!(on.len(), 1);
	let LogicalPlan::Exchange {
		partition_count: left_partitions,
		key: left_key,
		..
	} = left.as_ref()
	else {
		panic!("expected left exchange, got {left:?}");
	};
	let LogicalPlan::Exchange {
		partition_count: right_partitions,
		key: right_key,
		..
	} = right.as_ref()
	else {
		panic!("expected right exchange, got {right:?}");
	};
	assert_eq!(*left_partitions, 4);
	assert_eq!(*right_partitions, 4);
	assert_eq!(left_key.len(), 1);
	assert_eq!(right_key.len(), 1);
	assert_qualified_column(&left_key[0], "s", "region");
	assert_qualified_column(&right_key[0], "r", "region");
}

#[tokio::test]
async fn rune_authored_broadcast_join_constructs_validated_broadcast_plan() {
	let logical = rune_region_broadcast_join_plan().await;
	let LogicalPlan::Join {
		left,
		right,
		kind,
		on,
		strategy_intent,
		..
	} = logical
	else {
		panic!("expected Rune broadcast join, got {logical:?}");
	};

	assert_eq!(kind, JoinKind::Inner);
	assert_eq!(
		strategy_intent,
		JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::Broadcast,
			source: JoinStrategyPinSource::Rune,
			build_side: Some(BroadcastBuildSide::Left),
		}
	);
	assert_eq!(on.len(), 1);
	assert_qualified_column(&on[0].0, "r", "region");
	assert_qualified_column(&on[0].1, "s", "region");

	assert!(
		!matches!(left.as_ref(), LogicalPlan::Exchange { .. }),
		"broadcast build side must remain logical until bind"
	);
	assert!(
		contains_scan_named(left.as_ref(), "regions"),
		"receiver/build side must contain regions scan"
	);
	assert!(
		!matches!(right.as_ref(), LogicalPlan::Exchange { .. }),
		"broadcast probe side must remain bare"
	);
	assert!(
		contains_scan_named(right.as_ref(), "sales"),
		"argument/probe side must contain sales scan"
	);
}

#[tokio::test]
async fn rune_explain_broadcast_join_yields_decision_rows() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("regions").alias("r")
               .broadcast_join(
                   ctx.table("sales").alias("s"),
                   qcol("r", "region").eq(qcol("s", "region"))
               )
               .explain()
        }
    "#,
	)
	.await
	.expect("Rune explain broadcast join");

	let LogicalPlan::Explain { .. } = logical else {
		panic!("expected Rune explain plan, got {logical:?}");
	};
	let batches = run_logical(&logical).await;

	assert_eq!(
		string_column_rows(&batches, 0),
		vec![Some("join".to_string())]
	);
	assert_eq!(
		string_column_rows(&batches, 1),
		vec![Some("join[0]".to_string())]
	);
	assert_eq!(
		string_column_rows(&batches, 2),
		vec![Some("broadcast".to_string())]
	);
	assert_eq!(
		string_column_rows(&batches, 3),
		vec![Some("natural:pinned_by_author".to_string())]
	);
}

#[tokio::test]
async fn rune_analyze_lowers_to_explain_analyze() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").analyze()
        }
    "#,
	)
	.await
	.expect("Rune analyze");

	let LogicalPlan::ExplainAnalyze { .. } = logical else {
		panic!("expected Rune analyze plan, got {logical:?}");
	};
}

#[tokio::test]
async fn sql_explain_matches_rune_explain_rows() {
	let sql = SqlPlanner::new()
		.plan(
			"EXPLAIN SELECT /*+ BROADCAST(r) */ * \
			 FROM regions r INNER JOIN sales s ON r.region = s.region",
		)
		.expect("plan SQL EXPLAIN");
	let rune = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("regions").alias("r")
               .broadcast_join(
                   ctx.table("sales").alias("s"),
                   qcol("r", "region").eq(qcol("s", "region"))
               )
               .explain()
        }
    "#,
	)
	.await
	.expect("Rune explain broadcast join");

	assert_eq!(
		explain_rows_for_assertion(&run_logical(&sql).await),
		explain_rows_for_assertion(&run_logical(&rune).await)
	);
}

#[tokio::test]
async fn rune_explain_renders_readable_scan_filter_receipts() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .filter(col("amount").gt(lit(10)))
               .explain()
        }
    "#,
	)
	.await
	.expect("Rune explain scan filter");

	let batches = run_logical(&logical).await;

	assert_eq!(
		string_column_rows(&batches, 0),
		vec![Some("scan_filter".to_string())]
	);
	assert_eq!(
		string_column_rows(&batches, 1),
		vec![Some("scan[0] table_id=1".to_string())]
	);
	assert_eq!(
		string_column_rows(&batches, 2),
		vec![Some("fully_pushed".to_string())]
	);
	let predicate_text = string_column_rows(&batches, 4);
	let pushed_predicate_text = string_column_rows(&batches, 5);
	let residual_predicate_text = string_column_rows(&batches, 6);
	assert!(
		predicate_text
			.first()
			.and_then(|text| text.as_deref())
			.is_some_and(|text| text.contains("amount")),
		"predicate text should render the Rune filter column: {predicate_text:?}"
	);
	assert!(
		pushed_predicate_text
			.first()
			.and_then(|text| text.as_deref())
			.is_some_and(|text| text.contains("amount")),
		"pushed predicate text should render the Rune filter column: {pushed_predicate_text:?}"
	);
	assert_eq!(residual_predicate_text, vec![None]);
}

#[tokio::test]
async fn explain_binds_but_does_not_execute_inner_query() {
	let logical = SqlPlanner::new()
		.plan("EXPLAIN SELECT * FROM sales")
		.expect("plan EXPLAIN scan");
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind EXPLAIN scan");
	assert!(matches!(
		bound,
		ExecutionPlan::SystemTable {
			kind: SystemTableKind::OptimizerExplain,
			..
		}
	));

	let batches = Executor::new(Arc::new(PanicScan))
		.execute_bound(&bound, None)
		.await
		.expect("execute EXPLAIN system table");

	assert_eq!(total_rows(&batches), 0);
}

#[tokio::test]
async fn broadcast_join_qualifier_resolution_matches_hash() {
	let hash = rune_region_join_plan().await;
	let broadcast = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").alias("s")
               .broadcast_join(
                   ctx.table("regions").alias("r"),
                   qcol("s", "region").eq(qcol("r", "region"))
               )
        }
    "#,
	)
	.await
	.expect("Rune broadcast join plan");

	let LogicalPlan::Join {
		left: hash_left,
		right: hash_right,
		on: hash_on,
		null_safe_keys: false,
		strategy_intent: hash_strategy,
		..
	} = hash
	else {
		panic!("expected hash join");
	};
	let LogicalPlan::Join {
		left: broadcast_left,
		on: broadcast_on,
		null_safe_keys: false,
		strategy_intent: broadcast_strategy,
		..
	} = broadcast
	else {
		panic!("expected broadcast join");
	};
	assert_eq!(
		hash_strategy,
		JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::HashPartitioned,
			source: JoinStrategyPinSource::Rune,
			build_side: None,
		}
	);
	assert_eq!(
		broadcast_strategy,
		JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::Broadcast,
			source: JoinStrategyPinSource::Rune,
			build_side: Some(BroadcastBuildSide::Left),
		}
	);

	assert_eq!(hash_on.len(), 1);
	assert_eq!(broadcast_on.len(), 1);
	assert_qualified_column(&hash_on[0].0, "s", "region");
	assert_qualified_column(&hash_on[0].1, "r", "region");
	assert_qualified_column(&broadcast_on[0].0, "s", "region");
	assert_qualified_column(&broadcast_on[0].1, "r", "region");

	let LogicalPlan::Exchange {
		key: hash_left_key, ..
	} = hash_left.as_ref()
	else {
		panic!("expected hash left exchange");
	};
	let LogicalPlan::Exchange {
		key: hash_right_key,
		..
	} = hash_right.as_ref()
	else {
		panic!("expected hash right exchange");
	};
	assert_qualified_column(&hash_left_key[0], "s", "region");
	assert_qualified_column(&hash_right_key[0], "r", "region");
	assert!(
		!matches!(broadcast_left.as_ref(), LogicalPlan::Exchange { .. }),
		"Rune broadcast join should leave exchange insertion to the binder"
	);
	assert!(contains_scan_named(broadcast_left.as_ref(), "sales"));
}

#[tokio::test]
async fn rune_join_multi_column_predicate_decomposes_to_key_pairs() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").alias("l")
               .join(
                   ctx.table("sales").alias("r"),
                   qcol("l", "region").eq(qcol("r", "region"))
                       .and(qcol("l", "id").eq(qcol("r", "id")))
               )
        }
    "#,
	)
	.await
	.expect("Rune multi-key join plan");
	let LogicalPlan::Join {
		left, right, on, ..
	} = logical
	else {
		panic!("expected Rune join, got {logical:?}");
	};

	assert_eq!(on.len(), 2);
	let LogicalPlan::Exchange { key: left_key, .. } = left.as_ref() else {
		panic!("expected left exchange");
	};
	let LogicalPlan::Exchange { key: right_key, .. } = right.as_ref() else {
		panic!("expected right exchange");
	};
	assert_eq!(left_key.len(), 2);
	assert_eq!(right_key.len(), 2);
	assert_qualified_column(&left_key[0], "l", "region");
	assert_qualified_column(&right_key[0], "r", "region");
	assert_qualified_column(&left_key[1], "l", "id");
	assert_qualified_column(&right_key[1], "r", "id");
}

#[tokio::test]
async fn rune_join_named_methods_lower_to_existing_join_kinds() {
	for (method, expected) in [
		("join", JoinKind::Inner),
		("inner_join", JoinKind::Inner),
		("left_join", JoinKind::LeftOuter),
		("right_join", JoinKind::RightOuter),
		("full_join", JoinKind::FullOuter),
		("left_semi_join", JoinKind::LeftSemi),
		("right_semi_join", JoinKind::RightSemi),
		("left_anti_join", JoinKind::LeftAnti),
		("right_anti_join", JoinKind::RightAnti),
	] {
		let script = format!(
			r#"
        pub fn main(ctx) {{
            ctx.table("sales").alias("s")
               .{method}(
                   ctx.table("regions").alias("r"),
                   qcol("s", "region").eq(qcol("r", "region"))
               )
        }}
    "#
		);
		let logical = rune_plan(&script).await.expect("Rune named join method");
		let LogicalPlan::Join {
			kind,
			strategy_intent,
			left,
			right,
			..
		} = logical
		else {
			panic!("expected Rune join for {method}");
		};
		assert_eq!(kind, expected, "{method}");
		assert_eq!(
			strategy_intent,
			JoinStrategyIntent::Pinned {
				strategy: JoinStrategy::HashPartitioned,
				source: JoinStrategyPinSource::Rune,
				build_side: None,
			},
			"{method}"
		);
		assert!(
			matches!(
				left.as_ref(),
				LogicalPlan::Exchange {
					partition_count: 4,
					..
				}
			),
			"{method} left input must remain hash-exchanged"
		);
		assert!(
			matches!(
				right.as_ref(),
				LogicalPlan::Exchange {
					partition_count: 4,
					..
				}
			),
			"{method} right input must remain hash-exchanged"
		);
	}
}

#[tokio::test]
async fn rune_join_project_named_qualified_outputs_execute() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").alias("s")
               .join(
                   ctx.table("regions").alias("r"),
                   qcol("s", "region").eq(qcol("r", "region"))
               )
               .project_named([
                   qcol("s", "id").named("sale_id"),
                   qcol("r", "label").named("region_label"),
               ])
        }
    "#,
	)
	.await
	.expect("Rune qualified join projection");
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind Rune qualified join projection");
	assert_schema_names(bound.schema().as_ref(), &["sale_id", "region_label"]);

	let batches = execute_logical(&logical)
		.await
		.expect("execute Rune qualified join projection");
	let mut rows = int64_column_rows(&batches, 0)
		.into_iter()
		.zip(string_column_rows(&batches, 1))
		.collect::<Vec<_>>();
	rows.sort_by_key(|(id, _)| *id);
	assert_eq!(
		rows,
		vec![
			(Some(1), Some("East".to_string())),
			(Some(2), Some("West".to_string())),
			(Some(3), Some("East".to_string())),
			(Some(4), Some("East".to_string())),
			(Some(5), Some("West".to_string())),
		]
	);
}

#[tokio::test]
async fn rune_join_then_take_remains_chainable() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").alias("s")
               .join(
                   ctx.table("regions").alias("r"),
                   qcol("s", "region").eq(qcol("r", "region"))
               )
               .take(3)
               .project_named([qcol("s", "id").named("id")])
        }
    "#,
	)
	.await
	.expect("Rune join then take");

	let batches = run_logical(&logical).await;
	assert_eq!(total_rows(&batches), 3);
}

#[tokio::test]
async fn rune_join_accepts_filtered_dataframe_inputs() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").alias("s")
               .filter(qcol("s", "status").eq(lit_str("open")))
               .join(
                   ctx.table("regions").alias("r"),
                   qcol("s", "region").eq(qcol("r", "region"))
               )
               .project_named([qcol("s", "id").named("id")])
        }
    "#,
	)
	.await
	.expect("Rune join with filtered input");

	assert_eq!(
		sorted_int64_column_rows(&run_logical(&logical).await, 0),
		vec![Some(1), Some(2), Some(4), Some(5)]
	);
}

#[tokio::test]
async fn rune_join_accepts_sql_relation_input_when_keys_resolve() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT id, region FROM sales")
               .alias("s")
               .join(
                   ctx.table("regions").alias("r"),
                   qcol("s", "region").eq(qcol("r", "region"))
               )
               .project_named([qcol("s", "id").named("id")])
        }
    "#,
	)
	.await
	.expect("Rune join with ctx.sql input");

	assert_eq!(
		sorted_int64_column_rows(&run_logical(&logical).await, 0),
		vec![Some(1), Some(2), Some(3), Some(4), Some(5)]
	);
}

#[tokio::test]
async fn rune_join_unqualified_ambiguous_key_rejects() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").alias("s")
               .join(ctx.table("sales").alias("r"), col("id").eq(col("id")))
        }
    "#,
	)
	.await
	.expect("Rune ambiguous join plan");
	let error = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect_err("ambiguous unqualified join key must reject");

	assert!(matches!(
		error,
		BindingError::AmbiguousColumnRef { name, .. } if name == "id"
	));
}

#[tokio::test]
async fn rune_join_computed_key_rejects() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").alias("s")
               .join(
                   ctx.table("regions").alias("r"),
                   qcol("s", "id").add(lit_i64(1)).eq(qcol("r", "region"))
               )
        }
    "#,
	)
	.await
	.expect("Rune computed join key plan");
	let error = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect_err("computed join key must reject");

	assert!(matches!(error, BindingError::JoinComputedKeyUnsupported));
}

#[tokio::test]
async fn rune_join_predicate_without_equi_key_rejects() {
	let error = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").alias("s")
               .join(
                   ctx.table("regions").alias("r"),
                   qcol("s", "id").gt(qcol("r", "region"))
               )
        }
    "#,
	)
	.await
	.expect_err("non-equi Rune join predicate must reject");

	assert_frontend_message_contains(error, "JOIN predicate requires equality key predicates");
}

#[tokio::test]
async fn rune_broadcast_join_predicate_without_equi_key_rejects() {
	let error = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("regions").alias("r")
               .broadcast_join(
                   ctx.table("sales").alias("s"),
                   qcol("r", "region").gt(qcol("s", "region"))
               )
        }
    "#,
	)
	.await
	.expect_err("non-equi Rune broadcast join predicate must reject");

	assert_frontend_message_contains(error, "JOIN predicate requires equality key predicates");
}

#[tokio::test]
async fn rune_join_logical_identity_mismatch_rejects() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").alias("s")
               .join(
                   ctx.table("sales").alias("r"),
                   qcol("s", "id").as_logical_type::<CustomerId>()
                       .eq(qcol("r", "id").as_logical_type::<ProductId>())
               )
        }
    "#,
	)
	.await
	.expect("Rune nominal join plan");
	let error = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect_err("nominal mismatch join key must reject");

	assert_nominal_mismatch(error, "CustomerId", "ProductId", "Int64");
}

#[tokio::test]
async fn rune_authored_broadcast_join_preserves_logical_identity() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").alias("s")
               .broadcast_join(
                   ctx.table("regions").alias("r"),
                   qcol("s", "region").eq(qcol("r", "region"))
               )
               .project_named([qcol("s", "id").named("id")])
        }
    "#,
	)
	.await
	.expect("Rune broadcast join logical identity plan");
	let bound = bind(
		&logical,
		&catalog_with_sales_id_logical(),
		&PlannerConfig::default(),
	)
	.await
	.expect("bind broadcast join over identity-bearing sales");
	let ExecutionPlan::Project { output_schema, .. } = bound else {
		panic!("expected project over broadcast join");
	};

	assert_eq!(
		decode_column_logical_type(output_schema.field(0)).expect("decode joined output identity"),
		CUSTOMER_ID_LOGICAL_TYPE
	);
}

#[tokio::test]
async fn rune_top_k_of_top_k_fragments_and_executes() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .sort([col("id").desc()])
               .limit(4)
               .sort([col("id").asc()])
               .limit(2)
        }
    "#,
	)
	.await
	.expect("Rune stacked top-k plan");
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind Rune stacked top-k plan");

	let stages = fragment_plan(bound, fragment_config()).expect("fragment stacked top-k plan");
	assert_eq!(stages.len(), 3);
	assert!(matches!(
		stages[0].tasks[0].spec.output,
		toron::distributed::task::TaskOutputBinding::ShuffleProducer { .. }
	));
	assert!(matches!(
		stages[1].tasks[0].spec.output,
		toron::distributed::task::TaskOutputBinding::ShuffleProducer { .. }
	));
	assert!(matches!(
		stages[2].tasks[0].spec.output,
		toron::distributed::task::TaskOutputBinding::FinalResult
	));

	let batches = execute_logical(&logical)
		.await
		.expect("execute Rune stacked top-k plan");
	let ids = batches[0]
		.column(0)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("id column");
	assert_eq!(
		(0..ids.len()).map(|row| ids.value(row)).collect::<Vec<_>>(),
		vec![3, 4]
	);
}

#[tokio::test]
async fn rune_sql_parse_error_maps_to_frontend_error_without_capability_escape() {
	let error = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT * FROM")
        }
    "#,
	)
	.await
	.expect_err("invalid SQL should fail through frontend error path");

	let message = frontend_message(error);
	assert!(message.contains("SQL parse failed"), "{message}");
	assert!(!message.contains("Rune sandbox violation"), "{message}");
	assert!(!message.contains("/home/"), "{message}");
}

#[tokio::test]
async fn rune_project_named_sql_multi_hop_continuation_binds_and_executes() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT region, amount FROM sales WHERE status = 'open'")
               .filter(col("amount").gt(lit(10)))
               .project_named([
                   col("region").named("bucket"),
                   col("amount").named("net"),
               ])
               .aggregate([col("bucket")], [sum(col("net")), count_all()])
               .project_named([
                   col("bucket").named("bucket"),
                   col("sum").named("gross"),
                   col("count").named("n"),
               ])
               .filter(col("gross").gt(lit(20)))
        }
    "#,
	)
	.await
	.expect("multi-hop named continuation");

	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind multi-hop named continuation");
	assert_schema_names(bound.schema().as_ref(), &["bucket", "gross", "n"]);

	let batches = run_logical(&logical).await;
	assert_eq!(
		string_column_rows(&batches, 0),
		vec![Some("east".to_string())]
	);
	assert_eq!(int64_column_rows(&batches, 1), vec![Some(30)]);
	assert_eq!(int64_column_rows(&batches, 2), vec![Some(1)]);
}

#[tokio::test]
async fn product_0102_rune_aggregate_produces_the_preaggregated_pie_shape() {
	let source = r#"
        pub fn main(ctx) {
            ctx.table("pie_input")
               .aggregate([col("category")], [sum(col("value"))])
               .project_named([
                   col("category").named("category"),
                   col("sum").named("value"),
               ])
               .sort([col("category").DIRECTION()])
        }
	"#;
	let ascending_source = source.replace("DIRECTION", "asc");
	let descending_source = source.replace("DIRECTION", "desc");
	let ascending = rune_plan(&ascending_source)
		.await
		.expect("PRODUCT-0102 aggregate expression is registered in production Rune");
	let descending = rune_plan(&descending_source)
		.await
		.expect("PRODUCT-0103 opposite sort is registered in production Rune");

	let batches = run_pie_logical(&ascending).await;
	assert_eq!(
		string_column_rows(&batches, 0),
		vec![Some("A".to_owned()), Some("B".to_owned())]
	);
	assert_eq!(int64_column_rows(&batches, 1), vec![Some(5), Some(4)]);
	let batches = run_pie_logical(&descending).await;
	assert_eq!(
		string_column_rows(&batches, 0),
		vec![Some("B".to_owned()), Some("A".to_owned())]
	);
	assert_eq!(int64_column_rows(&batches, 1), vec![Some(4), Some(5)]);
}

#[tokio::test]
async fn rune_project_named_sets_project_output_names() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .project_named([
                   col("region").named("bucket"),
                   col("amount").named("net"),
               ])
        }
    "#,
	)
	.await
	.expect("named project plan");

	let LogicalPlan::Project { output_names, .. } = logical else {
		panic!("expected named Project, got {logical:?}");
	};
	assert_eq!(
		output_names,
		vec![Some("bucket".to_string()), Some("net".to_string())]
	);
}

#[tokio::test]
async fn rune_project_named_binds_visible_schema() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .project_named([
                   col("region").named("bucket"),
                   col("amount").named("net"),
               ])
        }
    "#,
	)
	.await
	.expect("named project plan");
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind named project");

	assert_schema_names(bound.schema().as_ref(), &["bucket", "net"]);
}

#[tokio::test]
async fn rune_project_named_chains_collapse_adjacent_projects() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT id, amount FROM sales")
               .project_named([
                   col("id").named("sale_id"),
                   col("amount").named("net"),
               ])
               .project_named([
                   col("sale_id").named("id2"),
               ])
        }
    "#,
	)
	.await
	.expect("project_named chain");
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind project_named chain");

	assert_schema_names(bound.schema().as_ref(), &["id2"]);
	assert_eq!(execution_project_count(&bound), 1);
}

#[tokio::test]
async fn rune_aggregate_helper_pack_binds_stable_outputs() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .aggregate([], [
                   count_all(),
                   count(col("amount")),
                   sum(col("amount")),
                   avg(col("amount")),
                   min(col("amount")),
                   max(col("amount")),
               ])
        }
    "#,
	)
	.await
	.expect("aggregate helper pack plan");
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind stable aggregate helpers");

	// ADR 267: the two COUNTs share the natural name "count", so they disambiguate to stable unique
	// internal names (`__agg_<position>`); the singleton SUM/AVG/MIN/MAX keep their natural names.
	assert_schema_names(
		bound.schema().as_ref(),
		&["__agg_0", "__agg_1", "sum", "avg", "min", "max"],
	);
}

#[tokio::test]
async fn rune_project_named_post_aggregate_renames_outputs() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .aggregate([], [sum(col("amount"))])
               .project_named([col("sum").named("gross")])
        }
    "#,
	)
	.await
	.expect("post aggregate named project plan");
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind post aggregate rename");

	assert_schema_names(bound.schema().as_ref(), &["gross"]);
}

#[tokio::test]
async fn rune_duplicate_aggregate_output_name_is_not_addressable() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .aggregate([], [sum(col("amount")), sum(col("id"))])
               .project_named([col("sum").named("gross")])
        }
    "#,
	)
	.await
	.expect("duplicate aggregate output plan");
	// ADR 267: the two SUMs disambiguate to unique internal names, so the natural "sum" referenced by the
	// downstream project is no longer an addressable output column (alias the aggregates to project them).
	let error = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect_err("unaliased duplicate aggregate name is not addressable");

	assert!(matches!(
		error,
		BindingError::UnknownColumn { column } if column == "sum"
	));
}

#[tokio::test]
async fn rune_named_expr_rejects_misused_named_expression() {
	let error = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .aggregate([], [sum(col("amount").named("net"))])
        }
    "#,
	)
	.await
	.expect_err("RuneNamedExpr must not be accepted as RuneExpr");

	let message = frontend_message(error);
	assert!(!message.contains("Rune capability denied"), "{message}");
	assert!(!message.contains("/home/"), "{message}");
}

#[tokio::test]
async fn rune_named_projection_preserves_adr079_sql_relation_guards() {
	let multi_statement = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT id FROM sales; DROP TABLE sales")
        }
    "#,
	)
	.await
	.expect_err("multi statement SQL must remain rejected");
	assert_frontend_message_contains(multi_statement, "exactly one SQL statement");

	let ddl = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("DROP TABLE sales")
        }
    "#,
	)
	.await
	.expect_err("terminal SQL must remain rejected");
	assert_frontend_message_contains(ddl, "ctx.sql accepts single query relations only");

	let alias_bound = bind(
		&sql_group_by_alias_outer_sum_plan().await,
		&catalog(),
		&PlannerConfig::default(),
	)
	.await
	.expect("ADR079 SQL alias continuation still binds");
	assert_schema_names(alias_bound.schema().as_ref(), &["sum"]);

	let pushdown = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT * FROM sales")
               .filter(col("amount").gt(lit(10)))
        }
    "#,
	)
	.await
	.expect("compose SQL scan with Rune filter");
	let bound = bind(&pushdown, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind composed Rune SQL filter");
	let (_, disposition, scan) = find_filter(&bound);
	assert_eq!(*disposition, FilterDisposition::FullyPushed);
	assert!(scan_pbc(scan).is_some());
}

#[tokio::test]
async fn rune_lit_str_filters_utf8_column_executes() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(col("status").eq(lit_str("open")))
        }
    "#,
	)
	.await
	.expect("lit_str filter plan");

	let batches = run_logical(&logical).await;
	assert_eq!(
		int64_column_rows(&batches, 0),
		vec![Some(1), Some(2), Some(4), Some(5), Some(6)]
	);
}

#[tokio::test]
async fn rune_lit_f64_filters_float64_column_executes() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(col("price").gt(lit_f64(15.5)))
        }
    "#,
	)
	.await
	.expect("lit_f64 filter plan");

	let batches = run_logical(&logical).await;
	assert_eq!(int64_column_rows(&batches, 0), vec![Some(2), Some(4)]);
}

#[tokio::test]
async fn rune_lit_bool_filters_boolean_column_executes() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(col("active").eq(lit_bool(true)))
        }
    "#,
	)
	.await
	.expect("lit_bool filter plan");

	let batches = run_logical(&logical).await;
	assert_eq!(
		int64_column_rows(&batches, 0),
		vec![Some(1), Some(3), Some(4), Some(6)]
	);
}

#[tokio::test]
async fn rune_lit_i64_filters_int64_column_executes() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(col("id").eq(lit_i64(4)))
        }
    "#,
	)
	.await
	.expect("lit_i64 filter plan");

	let batches = run_logical(&logical).await;
	assert_eq!(int64_column_rows(&batches, 0), vec![Some(4)]);
}

#[tokio::test]
async fn rune_lit_i64_preserves_wide_integer_in_logical_plan() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(col("id").eq(lit_i64(3000000000)))
        }
    "#,
	)
	.await
	.expect("wide lit_i64 filter plan");

	let LogicalPlan::Filter { predicate, .. } = logical else {
		panic!("expected filter plan");
	};
	let Expr::Eq(_, right) = predicate else {
		panic!("expected equality predicate, got {predicate:?}");
	};
	assert!(matches!(
		right.as_ref(),
		Expr::Literal(ScalarValue::Int64(Some(3_000_000_000)))
	));
}

#[tokio::test]
async fn rune_lit_timestamp_lowers_explicit_offset_to_utc_nanos() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(col("id").eq(lit_timestamp("2024-01-01T00:00:00-06:00")))
        }
    "#,
	)
	.await
	.expect("lit_timestamp filter plan");

	let LogicalPlan::Filter { predicate, .. } = logical else {
		panic!("expected filter plan");
	};
	let Expr::Eq(_, right) = predicate else {
		panic!("expected equality predicate, got {predicate:?}");
	};
	assert!(matches!(
		right.as_ref(),
		Expr::Literal(ScalarValue::Timestamp {
			value: Some(1_704_088_800_000_000_000),
			unit: TimeUnit::Nanosecond
		})
	));

	let err = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(col("id").eq(lit_timestamp("2024-01-01 00:00:00")))
        }
    "#,
	)
	.await
	.expect_err("offset-less timestamp should reject");
	assert!(
		err.to_string().contains("explicit offset"),
		"unexpected lit_timestamp error: {err}"
	);
}

#[tokio::test]
async fn rune_predicate_operator_compound_filter_executes() {
	let logical = predicate_operator_compound_filter_plan().await;

	let batches = run_logical(&logical).await;
	assert_eq!(
		int64_column_rows(&batches, 0),
		vec![Some(1), Some(2), Some(3)]
	);
}

#[tokio::test]
async fn rune_predicate_operators_preserve_pushdown_path() {
	let logical = predicate_operator_compound_filter_plan().await;
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind compound Rune predicate filter");
	let (_, disposition, scan) = find_filter(&bound);

	assert_eq!(*disposition, FilterDisposition::FullyPushed);
	assert!(
		scan_pbc(scan).is_some(),
		"compound predicate must reach scan PBC"
	);
}

#[tokio::test]
async fn rune_logical_not_executes_against_boolean_column() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(col("active").logical_not())
        }
    "#,
	)
	.await
	.expect("logical_not filter plan");

	let batches = run_logical(&logical).await;
	assert_eq!(int64_column_rows(&batches, 0), vec![Some(2), Some(5)]);
}

#[tokio::test]
async fn rune_null_predicate_executes_against_nullable_column() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(col("price").is_null())
        }
    "#,
	)
	.await
	.expect("is_null filter plan");

	let batches = run_logical(&logical).await;
	assert_eq!(int64_column_rows(&batches, 0), vec![Some(3)]);
}

#[tokio::test]
async fn rune_arithmetic_computed_projection_executes() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .project_named([
                   col("amount").add(lit(1)).named("amount_plus_one"),
                   col("amount").div(lit(2)).named("amount_half"),
                   col("price").mul(lit_f64(2.0)).named("price_x2"),
                   col("id").sub(lit_i64(1)).named("zero_based_id"),
                   col("amount").add(col("price")).named("amount_plus_price"),
               ])
        }
    "#,
	)
	.await
	.expect("arithmetic projection plan");
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind arithmetic projection");
	assert_schema_names(
		bound.schema().as_ref(),
		&[
			"amount_plus_one",
			"amount_half",
			"price_x2",
			"zero_based_id",
			"amount_plus_price",
		],
	);

	let batches = run_logical(&logical).await;
	assert_eq!(
		int32_column_rows(&batches, 0),
		vec![Some(11), Some(21), None, Some(31), None, Some(8)]
	);
	assert_eq!(
		int32_column_rows(&batches, 1),
		vec![Some(5), Some(10), None, Some(15), None, Some(3)]
	);
	assert_eq!(
		float64_column_rows(&batches, 2),
		vec![
			Some(21.0),
			Some(40.5),
			None,
			Some(61.5),
			Some(11.0),
			Some(14.5)
		]
	);
	assert_eq!(
		int64_column_rows(&batches, 3),
		vec![Some(0), Some(1), Some(2), Some(3), Some(4), Some(5)]
	);
	assert_eq!(
		float64_column_rows(&batches, 4),
		vec![
			Some(20.5),
			Some(40.25),
			None,
			Some(60.75),
			None,
			Some(14.25)
		]
	);
}

#[tokio::test]
async fn rune_arithmetic_residual_filter_disposition_is_evaluate_in_toron() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .filter(col("amount").add(lit(1)).gt(lit(20)))
        }
    "#,
	)
	.await
	.expect("arithmetic residual filter plan");
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind arithmetic residual filter");
	let (_, disposition, scan) = find_filter(&bound);

	assert_eq!(*disposition, FilterDisposition::EvaluateInToron);
	assert!(
		scan_pbc(scan).is_none(),
		"arithmetic predicate must not reach scan PBC"
	);

	let batches = run_logical(&logical).await;
	assert_eq!(int64_column_rows(&batches, 0), vec![Some(2), Some(4)]);
}

#[tokio::test]
async fn rune_arithmetic_aggregate_argument_executes() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .aggregate([], [sum(col("amount").add(lit(1)))])
        }
    "#,
	)
	.await
	.expect("arithmetic aggregate argument plan");

	let batches = run_logical(&logical).await;
	assert_eq!(int64_column_rows(&batches, 0), vec![Some(71)]);
}

#[tokio::test]
async fn rune_arithmetic_integer_division_by_zero_is_null() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .project_named([
                   col("amount").div(col("amount").sub(col("amount"))).named("q")
               ])
        }
    "#,
	)
	.await
	.expect("integer division by zero plan");

	let batches = run_logical(&logical).await;
	assert_eq!(
		int32_column_rows(&batches, 0),
		vec![None, None, None, None, None, None]
	);
}

#[tokio::test]
async fn rune_arithmetic_integer_overflow_is_kernel_error() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .project_named([
                   lit_i64(9223372036854775807).add(lit_i64(1)).named("overflow")
               ])
        }
    "#,
	)
	.await
	.expect("overflowing arithmetic plan");

	let error = execute_logical(&logical)
		.await
		.expect_err("overflow should fail");
	match error {
		ExecutionError::Kernel { message } => assert!(
			message.contains("arithmetic overflow"),
			"unexpected overflow message: {message}"
		),
		other => panic!("expected kernel overflow error, got {other:?}"),
	}
}

#[tokio::test]
async fn rune_cast_projection_executes() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .project_named([
                   col("amount").cast(type_i64()).named("amount_i64"),
                   col("id").cast(type_i32()).named("id_i32"),
                   col("price").cast(type_f32()).named("price_f32"),
                   col("amount").cast(type_f64()).named("amount_f64"),
               ])
        }
    "#,
	)
	.await
	.expect("cast projection plan");
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind cast projection");
	assert_schema_names(
		bound.schema().as_ref(),
		&["amount_i64", "id_i32", "price_f32", "amount_f64"],
	);
	assert_schema_types(
		bound.schema().as_ref(),
		&[
			DataType::Int64,
			DataType::Int32,
			DataType::Float32,
			DataType::Float64,
		],
	);

	let batches = run_logical(&logical).await;
	assert_eq!(
		int64_column_rows(&batches, 0),
		vec![Some(10), Some(20), None, Some(30), None, Some(7)]
	);
	assert_eq!(
		int32_column_rows(&batches, 1),
		vec![Some(1), Some(2), Some(3), Some(4), Some(5), Some(6)]
	);
	assert_eq!(
		float32_column_rows(&batches, 2),
		vec![
			Some(10.5),
			Some(20.25),
			None,
			Some(30.75),
			Some(5.5),
			Some(7.25)
		]
	);
	assert_eq!(
		float64_column_rows(&batches, 3),
		vec![Some(10.0), Some(20.0), None, Some(30.0), None, Some(7.0)]
	);
}

#[tokio::test]
async fn rune_try_cast_nulls_data_domain_failures() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .project_named([
                   col("id").add(lit_i64(3000000000)).try_cast(type_i32()).named("id_i32"),
                   col("price").mul(lit_f64(1e40)).try_cast(type_f32()).named("price_f32"),
               ])
        }
    "#,
	)
	.await
	.expect("try_cast overflow projection plan");

	let batches = run_logical(&logical).await;
	assert_eq!(
		int32_column_rows(&batches, 0),
		vec![None, None, None, None, None, None]
	);
	assert_eq!(
		float32_column_rows(&batches, 1),
		vec![None, None, None, None, None, None]
	);
}

#[tokio::test]
async fn rune_strict_cast_domain_failure_is_kernel_error() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .project_named([
                   col("id").add(lit_i64(3000000000)).cast(type_i32()).named("id_i32"),
               ])
        }
    "#,
	)
	.await
	.expect("strict cast overflow projection plan");

	let error = execute_logical(&logical)
		.await
		.expect_err("strict cast overflow should fail");
	match error {
		ExecutionError::Kernel { message } => assert!(
			message.contains("cast failed"),
			"unexpected cast failure message: {message}"
		),
		other => panic!("expected kernel cast error, got {other:?}"),
	}
}

#[tokio::test]
async fn rune_cast_predicate_is_residual() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .filter(col("amount").cast(type_i64()).gt(lit_i64(10)))
        }
    "#,
	)
	.await
	.expect("cast residual filter plan");
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind cast residual filter");
	let (_, disposition, scan) = find_filter(&bound);

	assert_eq!(*disposition, FilterDisposition::EvaluateInToron);
	assert!(
		scan_pbc(scan).is_none(),
		"cast predicate must not reach scan PBC"
	);

	let batches = run_logical(&logical).await;
	assert_eq!(int64_column_rows(&batches, 0), vec![Some(2), Some(4)]);
}

#[tokio::test]
async fn rune_cast_feeds_aggregate_argument() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .aggregate([], [sum(col("amount").try_cast(type_i64()))])
        }
    "#,
	)
	.await
	.expect("cast aggregate argument plan");

	let batches = run_logical(&logical).await;
	assert_eq!(int64_column_rows(&batches, 0), vec![Some(67)]);
}

#[tokio::test]
async fn rune_in_list_pushes_to_scan_pbc() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(col("id").in_list([lit_i64(1), lit_i64(2)]))
        }
    "#,
	)
	.await
	.expect("in_list filter plan");
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind in_list filter");
	let (_, disposition, scan) = find_filter(&bound);
	let pbc = scan_pbc(scan).expect("in_list must reach scan PBC");

	assert_eq!(*disposition, FilterDisposition::FullyPushed);
	assert!(pbc.contains(&OP_IN_LIST));

	let batches = run_logical(&logical).await;
	assert_eq!(int64_column_rows(&batches, 0), vec![Some(1), Some(2)]);
}

#[tokio::test]
async fn rune_not_in_list_pushes_to_scan_pbc() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(col("status").not_in_list([lit_str("open")]))
        }
    "#,
	)
	.await
	.expect("not_in_list filter plan");
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind not_in_list filter");
	let (_, disposition, scan) = find_filter(&bound);
	let pbc = scan_pbc(scan).expect("not_in_list must reach scan PBC");

	assert_eq!(*disposition, FilterDisposition::FullyPushed);
	assert!(pbc.contains(&OP_IN_LIST));
	assert!(pbc.contains(&OP_NOT));

	let batches = run_logical(&logical).await;
	assert_eq!(int64_column_rows(&batches, 0), vec![Some(3)]);
}

#[tokio::test]
async fn rune_not_in_list_excludes_null_target() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(col("region").not_in_list([lit_str("east")]))
        }
    "#,
	)
	.await
	.expect("not_in_list nullable target plan");

	let batches = run_logical(&logical).await;
	assert_eq!(int64_column_rows(&batches, 0), vec![Some(2), Some(5)]);
}

#[tokio::test]
async fn rune_in_list_literal_coercion_executes() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(col("id").in_list([lit(1), lit_i64(2)]))
        }
    "#,
	)
	.await
	.expect("in_list coercion filter plan");

	let batches = run_logical(&logical).await;
	assert_eq!(int64_column_rows(&batches, 0), vec![Some(1), Some(2)]);
}

#[tokio::test]
async fn rune_in_list_computed_member_is_residual() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(
                col("id").in_list([
                    col("amount").sub(lit_i64(9)),
                    col("amount").sub(lit_i64(18)),
                ])
            )
        }
    "#,
	)
	.await
	.expect("in_list computed member filter plan");
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind computed in_list filter");
	let (_, disposition, scan) = find_filter(&bound);

	assert_eq!(*disposition, FilterDisposition::EvaluateInToron);
	assert!(
		scan_pbc(scan).is_none(),
		"computed IN list member must not reach scan PBC"
	);

	let batches = run_logical(&logical).await;
	assert_eq!(int64_column_rows(&batches, 0), vec![Some(1), Some(2)]);
}

#[tokio::test]
async fn rune_lit_null_comparison_is_residual() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(col("amount").eq(lit_null()))
        }
    "#,
	)
	.await
	.expect("lit_null comparison filter plan");
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind lit_null comparison filter");
	let (_, disposition, scan) = find_filter(&bound);

	assert_eq!(*disposition, FilterDisposition::EvaluateInToron);
	assert!(scan_pbc(scan).is_none());

	let batches = run_logical(&logical).await;
	assert_eq!(int64_column_rows(&batches, 0), Vec::<Option<i64>>::new());
}

#[tokio::test]
async fn rune_lit_null_in_list_member_is_residual() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(col("status").in_list([lit_null(), lit_str("closed")]))
        }
    "#,
	)
	.await
	.expect("lit_null IN list plan");
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind lit_null IN list filter");
	let (_, disposition, scan) = find_filter(&bound);

	assert_eq!(*disposition, FilterDisposition::EvaluateInToron);
	assert!(scan_pbc(scan).is_none());

	let batches = run_logical(&logical).await;
	assert_eq!(int64_column_rows(&batches, 0), vec![Some(3)]);
}

#[tokio::test]
async fn rune_in_list_empty_list_panics() {
	let error = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(col("id").in_list([]))
        }
    "#,
	)
	.await
	.expect_err("empty IN list must fail in Rune frontend");

	assert_frontend_message_contains(error, "IN list must not be empty");
}

#[tokio::test]
async fn rune_in_list_over_limit_panics() {
	let members = (0..33)
		.map(|value| format!("lit_i64({value})"))
		.collect::<Vec<_>>()
		.join(", ");
	let script = format!(
		r#"
        pub fn main(ctx) {{
            ctx.table("sales").filter(col("id").in_list([{members}]))
        }}
    "#
	);
	let error = rune_plan(&script)
		.await
		.expect_err("over-limit IN list must fail in Rune frontend");

	assert_frontend_message_contains(error, "IN list exceeds 32 items");
}

#[tokio::test]
async fn rune_in_list_mismatch_is_binding_error() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(col("status").in_list([lit(1)]))
        }
    "#,
	)
	.await
	.expect("in_list type mismatch plan");
	let error = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect_err("IN list mismatch must fail in binder");

	match error {
		BindingError::TypeMismatch { message } => assert!(
			message.contains("IN operands"),
			"unexpected IN list mismatch message: {message}"
		),
		other => panic!("expected IN list type mismatch, got {other:?}"),
	}
}

#[tokio::test]
async fn rune_like_filters_utf8_column_residual() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(col("status").like("o%"))
        }
    "#,
	)
	.await
	.expect("LIKE filter plan");
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind LIKE filter");
	let (predicate, disposition, scan) = find_filter(&bound);

	assert_eq!(*disposition, FilterDisposition::EvaluateInToron);
	assert!(
		scan_pbc(scan).is_none(),
		"LIKE predicate must stay out of scan PBC"
	);
	assert_single_like(predicate, "o%", false, false);

	let batches = run_logical(&logical).await;
	assert_eq!(
		int64_column_rows(&batches, 0),
		vec![Some(1), Some(2), Some(4), Some(5), Some(6)]
	);
}

#[tokio::test]
async fn rune_ilike_filters_utf8_column_residual() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(col("status").ilike("CLO%"))
        }
    "#,
	)
	.await
	.expect("ILIKE filter plan");
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind ILIKE filter");
	let (predicate, disposition, scan) = find_filter(&bound);

	assert_eq!(*disposition, FilterDisposition::EvaluateInToron);
	assert!(
		scan_pbc(scan).is_none(),
		"ILIKE predicate must stay out of scan PBC"
	);
	assert_single_like(predicate, "CLO%", false, true);

	let batches = run_logical(&logical).await;
	assert_eq!(int64_column_rows(&batches, 0), vec![Some(3)]);
}

#[tokio::test]
async fn rune_not_like_and_not_ilike_filter_rows() {
	let not_like = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(col("status").not_like("o%"))
        }
    "#,
	)
	.await
	.expect("NOT LIKE filter plan");
	let bound = bind(&not_like, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind NOT LIKE filter");
	let (predicate, disposition, scan) = find_filter(&bound);
	assert_eq!(*disposition, FilterDisposition::EvaluateInToron);
	assert!(scan_pbc(scan).is_none());
	assert_single_like(predicate, "o%", true, false);
	let batches = run_logical(&not_like).await;
	assert_eq!(int64_column_rows(&batches, 0), vec![Some(3)]);

	let not_ilike = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(col("status").not_ilike("CLO%"))
        }
    "#,
	)
	.await
	.expect("NOT ILIKE filter plan");
	let bound = bind(&not_ilike, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind NOT ILIKE filter");
	let (predicate, disposition, scan) = find_filter(&bound);
	assert_eq!(*disposition, FilterDisposition::EvaluateInToron);
	assert!(scan_pbc(scan).is_none());
	assert_single_like(predicate, "CLO%", true, true);
	let batches = run_logical(&not_ilike).await;
	assert_eq!(
		int64_column_rows(&batches, 0),
		vec![Some(1), Some(2), Some(4), Some(5), Some(6)]
	);
}

#[tokio::test]
async fn rune_like_residual_splits_from_pushable_conjunct() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .filter(col("id").eq(lit_i64(3)).and(col("status").like("c%")))
        }
    "#,
	)
	.await
	.expect("LIKE residual split plan");
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind LIKE residual split");
	let (predicate, disposition, scan) = find_filter(&bound);

	assert_eq!(*disposition, FilterDisposition::EvaluateInToron);
	assert_single_like(predicate, "c%", false, false);
	assert!(
		scan_pbc(scan).is_some(),
		"pushable id equality must still reach scan PBC"
	);

	let batches = run_logical(&logical).await;
	assert_eq!(int64_column_rows(&batches, 0), vec![Some(3)]);
}

#[tokio::test]
async fn rune_like_reuses_binder_utf8_type_error() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(col("amount").like("1%"))
        }
    "#,
	)
	.await
	.expect("LIKE type error plan");
	let error = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect_err("LIKE on non-Utf8 column must fail in binder");

	match error {
		BindingError::TypeMismatch { message } => assert!(
			message.contains("LIKE expects Utf8 input, got Int32"),
			"unexpected LIKE type mismatch message: {message}"
		),
		other => panic!("expected LIKE type mismatch, got {other:?}"),
	}
}

#[tokio::test]
async fn rune_case_projection_executes() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").project([
                case([
                    when(col("amount").gt(lit(20)), lit_str("big")),
                    when(col("status").eq(lit_str("closed")), lit_str("closed")),
                ], lit_str("small")),
                case([
                    when(col("status").eq(lit_str("closed")), lit_i64(0)),
                    when(col("amount").gt(lit(20)), lit_i64(2)),
                ], lit_i64(1)),
            ])
        }
    "#,
	)
	.await
	.expect("Rune CASE projection plan");

	let batches = run_logical(&logical).await;
	assert_eq!(
		string_column_rows(&batches, 0),
		vec![
			Some("small".to_string()),
			Some("small".to_string()),
			Some("closed".to_string()),
			Some("big".to_string()),
			Some("small".to_string()),
			Some("small".to_string()),
		]
	);
	assert_eq!(
		int64_column_rows(&batches, 1),
		vec![Some(1), Some(1), Some(0), Some(2), Some(1), Some(1)]
	);
}

#[tokio::test]
async fn rune_case_numeric_widening_stays_naked() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").project([
                case([when(col("id").eq(lit_i64(1)), col("amount"))], lit_f64(1.5))
            ])
        }
    "#,
	)
	.await
	.expect("Rune CASE numeric widening plan");
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind Rune CASE numeric widening");
	let ExecutionPlan::Project { exprs, .. } = bound else {
		panic!("expected project");
	};

	assert_eq!(exprs.root_type(0), Some(&DataType::Float64));
	assert_eq!(exprs.root_logical_type(0), Some(LogicalTypeId::NAKED));
}

#[tokio::test]
async fn rune_case_empty_branches_reject() {
	let error = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").project([case([], lit(0))])
        }
    "#,
	)
	.await
	.expect_err("empty CASE branches must fail in Rune frontend");

	assert_frontend_message_contains(error, "CASE requires at least one WHEN branch");
}

#[tokio::test]
async fn rune_case_type_errors_inherited() {
	let non_boolean = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").project([case([when(col("id"), lit(1))], lit(0))])
        }
    "#,
	)
	.await
	.expect("Rune CASE non-Boolean condition plan");
	let error = bind(&non_boolean, &catalog(), &PlannerConfig::default())
		.await
		.expect_err("non-Boolean CASE condition must fail in binder");
	assert_binding_type_mismatch_contains(error, "CASE WHEN expected Boolean");

	let incompatible_results = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").project([
                case([when(col("id").eq(lit_i64(1)), col("amount"))], col("status"))
            ])
        }
    "#,
	)
	.await
	.expect("Rune CASE incompatible result plan");
	let error = bind(&incompatible_results, &catalog(), &PlannerConfig::default())
		.await
		.expect_err("incompatible CASE results must fail in binder");
	assert_binding_type_mismatch_contains(error, "CASE result branches cannot be unified");
}

#[tokio::test]
async fn rune_case_filter_is_residual_and_keeps_pushable_conjunct() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .filter(
                   col("id").eq(lit_i64(3)).and(
                       case([when(col("status").eq(lit_str("closed")), lit_bool(true))], lit_bool(false))
                   )
               )
        }
    "#,
	)
	.await
	.expect("Rune CASE residual split plan");
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind Rune CASE residual split");
	let (predicate, disposition, scan) = find_filter(&bound);

	assert_eq!(*disposition, FilterDisposition::EvaluateInToron);
	assert_single_case(predicate);
	assert!(
		scan_pbc(scan).is_some(),
		"pushable id equality must still reach scan PBC"
	);

	let batches = run_logical(&logical).await;
	assert_eq!(int64_column_rows(&batches, 0), vec![Some(3)]);
}

#[tokio::test]
async fn rune_case_eager_eval_masked_error_fails() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").project([
                case([when(col("id").lt(lit_i64(0)), lit(2147483647).add(lit(1)))], lit(0))
            ])
        }
    "#,
	)
	.await
	.expect("Rune CASE masked overflow plan");

	let error = execute_logical(&logical)
		.await
		.expect_err("masked overflow branch should still fail");
	match error {
		ExecutionError::Kernel { message } => {
			assert!(message.starts_with("arithmetic overflow: Add(Int32)"));
		}
		other => panic!("expected kernel overflow, got {other:?}"),
	}
}

#[tokio::test]
async fn rune_case_carries_logical_identity() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").project([
                case([
                    when(col("id").eq(lit_i64(1)), col("id").as_logical_type::<CustomerId>())
                ], lit_i64(0))
            ])
        }
    "#,
	)
	.await
	.expect("Rune CASE logical identity plan");
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind Rune CASE logical identity");
	let ExecutionPlan::Project {
		exprs,
		output_schema,
		..
	} = bound
	else {
		panic!("expected project");
	};

	assert_eq!(exprs.root_type(0), Some(&DataType::Int64));
	assert_eq!(exprs.root_logical_type(0), Some(CUSTOMER_ID_LOGICAL_TYPE));
	assert_eq!(
		decode_column_logical_type(output_schema.field(0)).expect("decode projection identity"),
		CUSTOMER_ID_LOGICAL_TYPE
	);
}

#[tokio::test]
async fn rune_lit_null_case_result_types_from_sibling_branch() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").project([
                case([when(col("active"), lit_null())], col("amount"))
            ])
        }
    "#,
	)
	.await
	.expect("Rune lit_null CASE plan");

	let batches = run_logical(&logical).await;
	assert_eq!(
		int32_column_rows(&batches, 0),
		vec![None, Some(20), None, None, None, None]
	);
}

#[tokio::test]
async fn rune_lit_null_projection_materializes_null_column() {
	let logical = rune_plan(
		r#"
	    pub fn main(ctx) {
	        ctx.table("sales").project([lit_null()])
        }
    "#,
	)
	.await
	.expect("Rune lit_null projection plan");
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind contextless lit_null projection");

	let ExecutionPlan::Project {
		exprs,
		output_schema,
		..
	} = &bound
	else {
		panic!("expected project");
	};
	assert_eq!(exprs.root_type(0), Some(&DataType::Null));
	assert_eq!(output_schema.field(0).data_type(), &DataType::Null);

	let batches = Executor::new(Arc::new(FixtureScan {
		batches: HashMap::from([(TableId(1), sales_batch(sales_schema()))]),
	}))
	.execute_bound(&bound, None)
	.await
	.expect("execute lit_null projection");
	assert_null_column(&batches, 0, 6);
}

#[tokio::test]
async fn rune_case_mixed_identities_reject() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").project([
                case([
                    when(col("id").eq(lit_i64(1)), col("id").as_logical_type::<CustomerId>())
                ], col("id").as_logical_type::<ProductId>())
            ])
        }
    "#,
	)
	.await
	.expect("Rune CASE mixed identity plan");
	let error = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect_err("mixed CASE identities must fail in binder");

	assert_nominal_mismatch(error, "CustomerId", "ProductId", "Int64");
}

#[tokio::test]
async fn rune_case_identity_widening_rejects() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").project([
                case([
                    when(col("id").eq(lit_i64(1)), col("id").as_logical_type::<CustomerId>())
                ], lit_f64(1.5))
            ])
        }
    "#,
	)
	.await
	.expect("Rune CASE identity widening plan");
	let error = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect_err("CASE identity widening must fail in binder");
	match error {
		BindingError::TypeMismatch { message } => {
			assert!(message.contains("CASE logical result"), "{message}");
			assert!(
				message.contains("cast explicitly to erase identity before widening"),
				"{message}"
			);
		}
		other => panic!("expected CASE identity widening type mismatch, got {other:?}"),
	}
}

#[tokio::test]
async fn rune_nominal_turbofish_binds_vs_literal() {
	bind_rune_nominal_filter(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(col("id").as_logical_type::<CustomerId>().eq(lit_i64(1)))
        }
    "#,
	)
	.await
	.expect("logical column vs naked literal should bind");
}

#[tokio::test]
async fn rune_nominal_same_identity_binds() {
	bind_rune_nominal_filter(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(
                col("id").as_logical_type::<CustomerId>()
                    .eq(col("id").as_logical_type::<CustomerId>())
            )
        }
    "#,
	)
	.await
	.expect("same nominal identity should bind");
}

#[tokio::test]
async fn rune_nominal_different_identity_rejects() {
	let error = bind_rune_nominal_filter(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(
                col("id").as_logical_type::<CustomerId>()
                    .eq(col("id").as_logical_type::<ProductId>())
            )
        }
    "#,
	)
	.await
	.expect_err("different nominal identities must reject");

	assert_nominal_mismatch(error, "CustomerId", "ProductId", "Int64");
}

#[tokio::test]
async fn rune_nominal_one_side_oops_rejects() {
	let error = bind_rune_nominal_filter(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(
                col("id").as_logical_type::<CustomerId>().eq(col("id"))
            )
        }
    "#,
	)
	.await
	.expect_err("logical vs naked non-literal must reject");

	assert_nominal_mismatch(error, "CustomerId", "naked", "Int64");
}

#[tokio::test]
async fn rune_nominal_ascription_physical_mismatch_rejects() {
	let error = bind_rune_nominal_filter(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(
                col("amount").as_logical_type::<CustomerId>().eq(lit_i64(1))
            )
        }
    "#,
	)
	.await
	.expect_err("logical ascription must match the physical type");

	match error {
		BindingError::TypeMismatch { message } => {
			assert!(message.contains("CustomerId expects Int64"), "{message}");
			assert!(message.contains("got Int32"), "{message}");
		}
		other => panic!("expected logical ascription physical mismatch, got {other:?}"),
	}
}

#[tokio::test]
async fn rune_nominal_both_side_cast_binds() {
	bind_rune_nominal_filter(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(
                col("id").as_logical_type::<CustomerId>().cast(type_i64())
                    .eq(col("id").as_logical_type::<ProductId>().cast(type_i64()))
            )
        }
    "#,
	)
	.await
	.expect("explicit casts on both sides erase nominal identity");
}

#[tokio::test]
async fn rune_identity_arithmetic_rejects_and_cast_erases_all_operators() {
	for (script, context) in [
		(
			r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .project_named([
                   col("id").as_logical_type::<CustomerId>().add(lit_i64(1)).named("bad")
               ])
        }
    "#,
			"identity addition",
		),
		(
			r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .project_named([
                   col("id").as_logical_type::<CustomerId>().sub(lit_i64(1)).named("bad")
               ])
        }
    "#,
			"identity subtraction",
		),
		(
			r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .project_named([
                   col("id").as_logical_type::<CustomerId>().mul(lit_i64(2)).named("bad")
               ])
        }
    "#,
			"identity multiplication",
		),
		(
			r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .project_named([
                   col("id").as_logical_type::<CustomerId>().div(lit_i64(2)).named("bad")
               ])
        }
    "#,
			"identity division",
		),
		(
			r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .aggregate([], [sum(col("id").as_logical_type::<CustomerId>())])
        }
    "#,
			"identity SUM",
		),
		(
			r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .aggregate([], [avg(col("id").as_logical_type::<CustomerId>())])
        }
    "#,
			"identity AVG",
		),
	] {
		let logical = rune_plan(script)
			.await
			.unwrap_or_else(|error| panic!("{context} plan failed: {error:?}"));
		let error = bind(&logical, &catalog(), &PlannerConfig::default())
			.await
			.unwrap_err();
		assert_binding_type_mismatch_contains(error, "not an arithmetic quantity");
	}

	let arithmetic = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .project_named([
                   col("id").as_logical_type::<CustomerId>()
                       .cast(type_i64())
                       .add(lit_i64(1))
                       .named("raw_id_plus_one"),
                   col("id").as_logical_type::<CustomerId>()
                       .cast(type_i64())
                       .sub(lit_i64(1))
                       .named("raw_id_minus_one"),
                   col("id").as_logical_type::<CustomerId>()
                       .cast(type_i64())
                       .mul(lit_i64(2))
                       .named("raw_id_times_two"),
                   col("id").as_logical_type::<CustomerId>()
                       .cast(type_i64())
                       .div(lit_i64(1))
                       .named("raw_id_div_one")
               ])
        }
    "#,
	)
	.await
	.expect("cast-erased identity arithmetic plan");
	let bound = bind(&arithmetic, &catalog(), &PlannerConfig::default())
		.await
		.expect("cast-erased identity arithmetic binds");
	let ExecutionPlan::Project {
		exprs,
		output_schema,
		..
	} = bound
	else {
		panic!("expected cast-erased arithmetic project");
	};
	for index in 0..4 {
		assert_eq!(exprs.root_logical_type(index), Some(LogicalTypeId::NAKED));
		assert_eq!(
			decode_column_logical_type(output_schema.field(index))
				.expect("decode cast-erased arithmetic identity"),
			LogicalTypeId::NAKED
		);
	}
	let batches = run_logical(&arithmetic).await;
	assert_eq!(
		int64_column_rows(&batches, 0),
		vec![Some(2), Some(3), Some(4), Some(5), Some(6), Some(7)]
	);
	assert_eq!(
		int64_column_rows(&batches, 1),
		vec![Some(0), Some(1), Some(2), Some(3), Some(4), Some(5)]
	);
	assert_eq!(
		int64_column_rows(&batches, 2),
		vec![Some(2), Some(4), Some(6), Some(8), Some(10), Some(12)]
	);
	assert_eq!(
		int64_column_rows(&batches, 3),
		vec![Some(1), Some(2), Some(3), Some(4), Some(5), Some(6)]
	);

	let aggregates = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .aggregate([], [
                   sum(col("id").as_logical_type::<CustomerId>().cast(type_i64())),
                   avg(col("id").as_logical_type::<CustomerId>().cast(type_i64())),
               ])
        }
    "#,
	)
	.await
	.expect("cast-erased identity aggregate plan");
	let bound = bind(&aggregates, &catalog(), &PlannerConfig::default())
		.await
		.expect("cast-erased identity aggregates bind");
	assert_eq!(
		decode_column_logical_type(bound.schema().field(0))
			.expect("decode cast-erased SUM identity"),
		LogicalTypeId::NAKED
	);
	assert_eq!(
		decode_column_logical_type(bound.schema().field(1))
			.expect("decode cast-erased AVG identity"),
		LogicalTypeId::NAKED
	);
	let batches = run_logical(&aggregates).await;
	assert_eq!(int64_column_rows(&batches, 0), vec![Some(21)]);
	assert_eq!(float64_column_rows(&batches, 1), vec![Some(3.5)]);
}

#[tokio::test]
async fn rune_nominal_one_side_cast_still_rejects() {
	let error = bind_rune_nominal_filter(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(
                col("id").as_logical_type::<CustomerId>().cast(type_i64())
                    .eq(col("id").as_logical_type::<ProductId>())
            )
        }
    "#,
	)
	.await
	.expect_err("one-side cast must not bypass nominal mismatch");

	assert_nominal_mismatch(error, "naked", "ProductId", "Int64");
}

#[tokio::test]
async fn rune_nominal_in_list_same_identity_binds() {
	bind_rune_nominal_filter(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(
                col("id").as_logical_type::<CustomerId>().in_list([
                    lit_i64(1).as_logical_type::<CustomerId>(),
                    lit_i64(2).as_logical_type::<CustomerId>(),
                ])
            )
        }
    "#,
	)
	.await
	.expect("same nominal identity IN list should bind");
}

#[tokio::test]
async fn rune_nominal_in_list_different_identity_rejects() {
	let error = bind_rune_nominal_filter(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(
                col("id").as_logical_type::<CustomerId>().in_list([
                    lit_i64(1).as_logical_type::<ProductId>(),
                ])
            )
        }
    "#,
	)
	.await
	.expect_err("different nominal identity in IN list must reject");

	assert_nominal_mismatch(error, "CustomerId", "ProductId", "Int64");
}

#[tokio::test]
async fn rune_nominal_ascribed_literal_identity_is_preserved() {
	let error = bind_rune_nominal_filter(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(
                lit_i64(1).as_logical_type::<CustomerId>().eq(col("price"))
            )
        }
    "#,
	)
	.await
	.expect_err("implicit literal coercion must preserve explicit logical identity");

	assert_nominal_mismatch(error, "CustomerId", "naked", "Float64");
}

#[tokio::test]
async fn rune_convert_matches_sql_convert_bind_path() {
	let catalog = catalog_with_dimensioned_price();
	let sql = SqlPlanner::new()
		.plan("SELECT CONVERT(price, Kilometer) AS km FROM sales")
		.expect("plan SQL CONVERT");
	let rune = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").project_named([convert(col("price"), "Kilometer").named("km")])
        }
    "#,
	)
	.await
	.expect("Rune CONVERT");

	let sql_bound = bind(&sql, &catalog, &PlannerConfig::default())
		.await
		.expect("bind SQL CONVERT");
	let rune_bound = bind(&rune, &catalog, &PlannerConfig::default())
		.await
		.expect("bind Rune CONVERT");

	for bound in [sql_bound, rune_bound] {
		let ExecutionPlan::Project {
			exprs,
			output_schema,
			..
		} = bound
		else {
			panic!("expected project");
		};
		let root = exprs.single_root().expect("single projection root");
		assert!(matches!(
			exprs.node(root),
			Some(BoundExprNode::Binary {
				op: BoundBinaryOp::Mul,
				..
			})
		));
		assert_eq!(exprs.root_type(0), Some(&DataType::Float64));
		assert_eq!(
			exprs.root_logical_type(0),
			Some(LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID + 1))
		);
		assert_eq!(
			decode_column_logical_type(output_schema.field(0))
				.expect("decode convert output identity"),
			LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID + 1)
		);
	}
}

#[tokio::test]
async fn rune_point_delta_arithmetic_matches_sql_bind_path() {
	let catalog = catalog_with_temperature_points();
	let sql = SqlPlanner::new()
		.plan(
			"SELECT celsius - celsius AS dc, celsius + celsius_delta AS c1, celsius_delta + celsius AS c2, celsius - celsius_delta AS c3 FROM temps",
		)
		.expect("plan SQL point/delta arithmetic");
	let rune = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("temps").project_named([
                col("celsius").sub(col("celsius")).named("dc"),
                col("celsius").add(col("celsius_delta")).named("c1"),
                col("celsius_delta").add(col("celsius")).named("c2"),
                col("celsius").sub(col("celsius_delta")).named("c3"),
            ])
        }
    "#,
	)
	.await
	.expect("Rune point/delta arithmetic");

	let sql_bound = bind(&sql, &catalog, &PlannerConfig::default())
		.await
		.expect("bind SQL point/delta arithmetic");
	let rune_bound = bind(&rune, &catalog, &PlannerConfig::default())
		.await
		.expect("bind Rune point/delta arithmetic");

	for bound in [sql_bound, rune_bound] {
		let ExecutionPlan::Project {
			exprs,
			output_schema,
			..
		} = bound
		else {
			panic!("expected point/delta project");
		};
		for (index, expected) in [
			RUNE_TEMPERATURE_CELSIUS_DELTA,
			RUNE_TEMPERATURE_CELSIUS,
			RUNE_TEMPERATURE_CELSIUS,
			RUNE_TEMPERATURE_CELSIUS,
		]
		.into_iter()
		.enumerate()
		{
			assert_eq!(exprs.root_logical_type(index), Some(expected));
			assert_eq!(
				decode_column_logical_type(output_schema.field(index))
					.expect("decode point/delta output identity"),
				expected
			);
		}
	}

	let aggregate = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("temps").aggregate([], [avg(col("celsius"))])
        }
    "#,
	)
	.await
	.expect("Rune point AVG");
	let bound = bind(&aggregate, &catalog, &PlannerConfig::default())
		.await
		.expect("bind Rune point AVG");
	assert_eq!(
		decode_column_logical_type(bound.schema().field(0)).expect("decode point AVG identity"),
		RUNE_TEMPERATURE_CELSIUS
	);

	for (script, context, diagnostic) in [
		(
			r#"
        pub fn main(ctx) {
            ctx.table("temps").project_named([
                col("celsius").add(col("celsius")).named("bad")
            ])
        }
    "#,
			"point + point",
			"point/delta quantity algebra",
		),
		(
			r#"
        pub fn main(ctx) {
            ctx.table("temps").aggregate([], [sum(col("celsius"))])
        }
    "#,
			"SUM(point)",
			"point/delta quantity algebra",
		),
		(
			r#"
        pub fn main(ctx) {
            ctx.table("temps").project_named([
                col("kelvin").sub(col("kelvin")).named("bad")
            ])
        }
    "#,
			"point without delta unit",
			"has no declared delta unit",
		),
	] {
		let logical = rune_plan(script)
			.await
			.unwrap_or_else(|error| panic!("{context} plan failed: {error:?}"));
		let error = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.unwrap_err();
		assert_binding_type_mismatch_contains(error, diagnostic);
	}
}

async fn predicate_operator_compound_filter_plan() -> LogicalPlan {
	rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").filter(
                col("id").lt(lit_i64(4))
                    .and(col("amount").gte(lit(10)))
                    .and(col("amount").lte(lit(20)))
                    .or(col("status").ne(lit_str("open")).and(col("price").is_null()))
                    .and(col("region").is_not_null())
            )
        }
    "#,
	)
	.await
	.expect("compound predicate operator filter plan")
}

async fn sql_group_by_alias_outer_sum_plan() -> LogicalPlan {
	rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT region, COUNT(1) AS cnt FROM sales GROUP BY region")
               .aggregate([], [sum(col("cnt"))])
        }
    "#,
	)
	.await
	.expect("Rune aggregate over SQL group-by alias")
}

async fn rune_region_join_plan() -> LogicalPlan {
	rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").alias("s")
               .join(
                   ctx.table("regions").alias("r"),
                   qcol("s", "region").eq(qcol("r", "region"))
               )
        }
    "#,
	)
	.await
	.expect("Rune region join plan")
}

async fn rune_region_broadcast_join_plan() -> LogicalPlan {
	rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("regions").alias("r")
               .broadcast_join(
                   ctx.table("sales").alias("s"),
                   qcol("r", "region").eq(qcol("s", "region"))
               )
        }
    "#,
	)
	.await
	.expect("Rune region broadcast join plan")
}

async fn bind_rune_nominal_filter(script: &str) -> Result<ExecutionPlan, BindingError> {
	let logical = rune_plan(script).await.expect("Rune nominal plan");
	bind(&logical, &catalog(), &PlannerConfig::default()).await
}

fn assert_nominal_mismatch(
	error: BindingError,
	expected_left: &str,
	expected_right: &str,
	expected_physical: &str,
) {
	match error {
		BindingError::NominalTypeMismatch {
			left,
			right,
			physical,
		} => {
			assert_eq!(left, expected_left);
			assert_eq!(right, expected_right);
			assert_eq!(physical, expected_physical);
		}
		other => panic!("expected nominal type mismatch, got {other:?}"),
	}
}

fn assert_binding_type_mismatch_contains(error: BindingError, expected: &str) {
	match error {
		BindingError::TypeMismatch { message } => assert!(
			message.contains(expected),
			"expected binding type mismatch containing {expected:?}, got {message:?}"
		),
		other => panic!("expected binding type mismatch, got {other:?}"),
	}
}

async fn run_logical(logical: &LogicalPlan) -> Vec<RecordBatch> {
	execute_logical(logical).await.expect("execute")
}

async fn execute_logical(logical: &LogicalPlan) -> Result<Vec<RecordBatch>, ExecutionError> {
	let bound = bind(logical, &catalog(), &PlannerConfig::default()).await?;
	Executor::new(Arc::new(FixtureScan {
		batches: HashMap::from([
			(TableId(1), sales_batch(sales_schema())),
			(TableId(2), regions_batch(regions_schema())),
		]),
	}))
	.execute_bound(&bound, None)
	.await
}

fn catalog() -> MockCatalog {
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(
			TableId(1),
			descriptor(1, "sales", sales_schema().as_ref(), "id"),
		)
		.expect("register sales");
	catalog
		.register_descriptor(
			TableId(2),
			descriptor(2, "regions", regions_schema().as_ref(), "region"),
		)
		.expect("register regions");
	catalog
}

async fn run_pie_logical(logical: &LogicalPlan) -> Vec<RecordBatch> {
	let mut pie_catalog = catalog();
	pie_catalog
		.register_descriptor(
			TableId(3),
			descriptor(3, "pie_input", pie_input_schema().as_ref(), "id"),
		)
		.expect("register pie input");
	let bound = bind(logical, &pie_catalog, &PlannerConfig::default())
		.await
		.expect("bind PRODUCT-0102 aggregate plan");
	Executor::new(Arc::new(FixtureScan {
		batches: HashMap::from([(TableId(3), pie_input_batch(pie_input_schema()))]),
	}))
	.execute_bound(&bound, None)
	.await
	.expect("execute PRODUCT-0102 aggregate plan")
}

fn catalog_with_sales_id_logical() -> MockCatalog {
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(
			TableId(1),
			descriptor(1, "sales", sales_schema_with_logical_id().as_ref(), "id"),
		)
		.expect("register sales");
	catalog
		.register_descriptor(
			TableId(2),
			descriptor(2, "regions", regions_schema().as_ref(), "region"),
		)
		.expect("register regions");
	catalog
}

fn catalog_with_dimensioned_price() -> MockCatalog {
	let mut catalog = MockCatalog::new();
	catalog
		.register_logical_type_for_tenant(
			TenantId(1),
			LogicalTypeDefinition {
				logical_type_id: DURABLE_LOGICAL_TYPE_MIN_ID,
				name: "Mile".to_string(),
				physical: LogicalTypePhysical::Float64,
				refinement: None,
				unit: Some(LogicalTypeUnit {
					dimension: "Length".to_string(),
					scale_to_base: UnitScale {
						numerator: 201168,
						denominator: 125,
					},
					quantity_kind: UnitQuantityKind::Delta,
					offset_to_base: UnitOffset::ZERO,
					delta_unit: None,
				}),
			},
		)
		.expect("register Mile");
	catalog
		.register_logical_type_for_tenant(
			TenantId(1),
			LogicalTypeDefinition {
				logical_type_id: DURABLE_LOGICAL_TYPE_MIN_ID + 1,
				name: "Kilometer".to_string(),
				physical: LogicalTypePhysical::Float64,
				refinement: None,
				unit: Some(LogicalTypeUnit {
					dimension: "Length".to_string(),
					scale_to_base: UnitScale {
						numerator: 1000,
						denominator: 1,
					},
					quantity_kind: UnitQuantityKind::Delta,
					offset_to_base: UnitOffset::ZERO,
					delta_unit: None,
				}),
			},
		)
		.expect("register Kilometer");
	catalog
		.register_descriptor(
			TableId(1),
			descriptor(
				1,
				"sales",
				sales_schema_with_dimensioned_price().as_ref(),
				"id",
			),
		)
		.expect("register sales");
	catalog
}

fn catalog_with_temperature_points() -> MockCatalog {
	let mut catalog = MockCatalog::new();
	for definition in [
		LogicalTypeDefinition {
			logical_type_id: RUNE_TEMPERATURE_CELSIUS_DELTA.0,
			name: "CelsiusDelta".to_string(),
			physical: LogicalTypePhysical::Float64,
			refinement: None,
			unit: Some(LogicalTypeUnit {
				dimension: "Temperature".to_string(),
				scale_to_base: UnitScale {
					numerator: 1,
					denominator: 1,
				},
				quantity_kind: UnitQuantityKind::Delta,
				offset_to_base: UnitOffset::ZERO,
				delta_unit: None,
			}),
		},
		LogicalTypeDefinition {
			logical_type_id: RUNE_TEMPERATURE_CELSIUS.0,
			name: "Celsius".to_string(),
			physical: LogicalTypePhysical::Float64,
			refinement: None,
			unit: Some(LogicalTypeUnit {
				dimension: "Temperature".to_string(),
				scale_to_base: UnitScale {
					numerator: 1,
					denominator: 1,
				},
				quantity_kind: UnitQuantityKind::Point,
				offset_to_base: UnitOffset {
					numerator: 5463,
					denominator: 20,
				},
				delta_unit: Some("CelsiusDelta".to_string()),
			}),
		},
		LogicalTypeDefinition {
			logical_type_id: RUNE_TEMPERATURE_KELVIN.0,
			name: "Kelvin".to_string(),
			physical: LogicalTypePhysical::Float64,
			refinement: None,
			unit: Some(LogicalTypeUnit {
				dimension: "Temperature".to_string(),
				scale_to_base: UnitScale {
					numerator: 1,
					denominator: 1,
				},
				quantity_kind: UnitQuantityKind::Point,
				offset_to_base: UnitOffset::ZERO,
				delta_unit: None,
			}),
		},
	] {
		catalog
			.register_logical_type_for_tenant(TenantId(1), definition)
			.expect("register temperature logical type");
	}
	catalog
		.register_descriptor(
			TableId(1),
			descriptor(1, "temps", temperature_schema().as_ref(), "id"),
		)
		.expect("register temps");
	catalog
}

fn sales_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("region", DataType::Utf8, true),
		Field::new("amount", DataType::Int32, true),
		Field::new("status", DataType::Utf8, true),
		Field::new("price", DataType::Float64, true),
		Field::new("active", DataType::Boolean, true),
	]))
}

fn pie_input_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("category", DataType::Utf8, false),
		Field::new("value", DataType::Int64, false),
	]))
}

fn sales_schema_with_logical_id() -> SchemaRef {
	Arc::new(Schema::new(vec![
		field_with_logical_type_metadata("id", DataType::Int64, false, CUSTOMER_ID_LOGICAL_TYPE),
		Field::new("region", DataType::Utf8, true),
		Field::new("amount", DataType::Int32, true),
		Field::new("status", DataType::Utf8, true),
		Field::new("price", DataType::Float64, true),
		Field::new("active", DataType::Boolean, true),
	]))
}

fn sales_schema_with_dimensioned_price() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("region", DataType::Utf8, true),
		Field::new("amount", DataType::Int32, true),
		Field::new("status", DataType::Utf8, true),
		field_with_logical_type_metadata(
			"price",
			DataType::Float64,
			true,
			LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID),
		),
		Field::new("active", DataType::Boolean, true),
	]))
}

fn temperature_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		field_with_logical_type_metadata(
			"celsius",
			DataType::Float64,
			true,
			RUNE_TEMPERATURE_CELSIUS,
		),
		field_with_logical_type_metadata(
			"celsius_delta",
			DataType::Float64,
			true,
			RUNE_TEMPERATURE_CELSIUS_DELTA,
		),
		field_with_logical_type_metadata(
			"kelvin",
			DataType::Float64,
			true,
			RUNE_TEMPERATURE_KELVIN,
		),
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
				Some(20.25),
				None,
				Some(30.75),
				Some(5.5),
				Some(7.25),
			])),
			Arc::new(BooleanArray::from(vec![
				Some(true),
				Some(false),
				Some(true),
				Some(true),
				Some(false),
				Some(true),
			])),
		],
	)
	.expect("sales batch")
}

fn pie_input_batch(schema: SchemaRef) -> RecordBatch {
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(vec![1, 2, 3])),
			Arc::new(StringArray::from(vec!["A", "A", "B"])),
			Arc::new(Int64Array::from(vec![2, 3, 4])),
		],
	)
	.expect("pie input batch")
}

fn regions_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("region", DataType::Utf8, false),
		Field::new("label", DataType::Utf8, false),
	]))
}

fn regions_batch(schema: SchemaRef) -> RecordBatch {
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(StringArray::from(vec!["east", "west"])),
			Arc::new(StringArray::from(vec!["East", "West"])),
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
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single(primary_key)),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("descriptor")
}

struct FixtureScan {
	batches: HashMap<TableId, RecordBatch>,
}

struct PanicScan;

#[async_trait::async_trait]
impl StorageScan for PanicScan {
	async fn scan(&self, _request: ScanRequest) -> Result<ScanStream, StorageError> {
		panic!("EXPLAIN must not execute the inner query")
	}
}

#[async_trait::async_trait]
impl StorageScan for FixtureScan {
	async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
		let mut batch = self
			.batches
			.get(&request.table_id)
			.expect("fixture table")
			.clone();
		if let Some(pbc) = request.pbc_predicate {
			batch = wendelon::schema::pbc::execute(&pbc, &batch).map_err(|error| {
				StorageError::InvalidPbcProgram {
					message: format!("{error:?}"),
				}
			})?;
		}
		if let Some(projected_columns) = request.projected_columns.as_ref() {
			let row_count = batch.num_rows();
			let schema = batch.schema();
			let fields = projected_columns
				.iter()
				.map(|ordinal| schema.field(ordinal.0 as usize).as_ref().clone())
				.collect::<Vec<_>>();
			let columns = projected_columns
				.iter()
				.map(|ordinal| batch.column(ordinal.0 as usize).clone())
				.collect::<Vec<_>>();
			let projected_schema = Arc::new(Schema::new(fields));
			batch = if columns.is_empty() {
				RecordBatch::try_new_with_options(
					projected_schema,
					columns,
					&RecordBatchOptions::new().with_row_count(Some(row_count)),
				)
				.expect("project fixture batch")
			} else {
				RecordBatch::try_new(projected_schema, columns).expect("project fixture batch")
			};
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

fn contains_scan_named(plan: &LogicalPlan, table: &str) -> bool {
	match plan {
		LogicalPlan::ScanNamed { table: name, .. } => name == &TableRef::new(table),
		LogicalPlan::Alias { input, .. }
		| LogicalPlan::Filter { input, .. }
		| LogicalPlan::Project { input, .. }
		| LogicalPlan::Aggregate { input, .. }
		| LogicalPlan::Exchange { input, .. }
		| LogicalPlan::Sort { input, .. }
		| LogicalPlan::Take { input, .. }
		| LogicalPlan::Explain { input }
		| LogicalPlan::CreateTableAsSelect { source: input, .. }
		| LogicalPlan::InsertSelect { source: input, .. } => contains_scan_named(input, table),
		LogicalPlan::Join { left, right, .. } => {
			contains_scan_named(left, table) || contains_scan_named(right, table)
		}
		_ => false,
	}
}

fn is_terminal_plan(plan: &LogicalPlan) -> bool {
	matches!(
		plan,
		LogicalPlan::CreateTable { .. }
			| LogicalPlan::CreateTableAsSelect { .. }
			| LogicalPlan::DropTable { .. }
			| LogicalPlan::Delete { .. }
			| LogicalPlan::Update { .. }
			| LogicalPlan::Insert { .. }
			| LogicalPlan::InsertSelect { .. }
	)
}

fn find_filter(
	plan: &ExecutionPlan,
) -> (
	&toron::ir::BoundExprProgram,
	&FilterDisposition,
	&ExecutionPlan,
) {
	match plan {
		ExecutionPlan::Project { input, .. } | ExecutionPlan::Sort { input, .. } => {
			find_filter(input)
		}
		ExecutionPlan::Filter {
			input,
			predicate,
			disposition,
		} => (predicate, disposition, input),
		other => panic!("expected filter plan, got {other:?}"),
	}
}

fn scan_pbc(plan: &ExecutionPlan) -> Option<&[u8]> {
	match plan {
		ExecutionPlan::Scan { pbc_program, .. } => pbc_program.as_ref().map(|pbc| pbc.0.as_slice()),
		ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. } => scan_pbc(input),
		ExecutionPlan::Join { left, right, .. } => scan_pbc(left).or_else(|| scan_pbc(right)),
		_ => None,
	}
}

fn execution_project_count(plan: &ExecutionPlan) -> usize {
	match plan {
		ExecutionPlan::Project { input, .. } => 1 + execution_project_count(input),
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. } => execution_project_count(input),
		ExecutionPlan::Join { left, right, .. } => {
			execution_project_count(left) + execution_project_count(right)
		}
		ExecutionPlan::CreateTableAsSelect { source, .. }
		| ExecutionPlan::Update { source, .. }
		| ExecutionPlan::InsertSelect { source, .. } => execution_project_count(source),
		_ => 0,
	}
}

fn assert_single_like(
	predicate: &toron::ir::BoundExprProgram,
	expected_pattern: &str,
	expected_negated: bool,
	expected_case_insensitive: bool,
) {
	match predicate
		.single_root()
		.and_then(|root| predicate.node(root))
	{
		Some(BoundExprNode::Like {
			pattern,
			negated,
			case_insensitive,
			..
		}) => {
			assert_eq!(pattern, expected_pattern);
			assert_eq!(*negated, expected_negated);
			assert_eq!(*case_insensitive, expected_case_insensitive);
		}
		other => panic!("expected single LIKE predicate, got {other:?}"),
	}
}

fn assert_single_case(predicate: &toron::ir::BoundExprProgram) {
	match predicate
		.single_root()
		.and_then(|root| predicate.node(root))
	{
		Some(BoundExprNode::Case {
			when_then,
			else_input,
		}) => {
			assert_eq!(when_then.len(), 1);
			assert!(predicate.node(*else_input).is_some());
		}
		other => panic!("expected single CASE predicate, got {other:?}"),
	}
}

fn int64_column_rows(batches: &[RecordBatch], column: usize) -> Vec<Option<i64>> {
	batches
		.iter()
		.flat_map(|batch| {
			batch
				.column(column)
				.as_any()
				.downcast_ref::<Int64Array>()
				.expect("Int64 column")
				.iter()
				.collect::<Vec<_>>()
		})
		.collect()
}

fn sorted_int64_column_rows(batches: &[RecordBatch], column: usize) -> Vec<Option<i64>> {
	let mut rows = int64_column_rows(batches, column);
	rows.sort();
	rows
}

fn total_rows(batches: &[RecordBatch]) -> usize {
	batches.iter().map(RecordBatch::num_rows).sum()
}

fn assert_qualified_column(expr: &Expr, qualifier: &str, name: &str) {
	let Expr::Column(column) = expr else {
		panic!("expected column expression, got {expr:?}");
	};
	assert_eq!(
		column
			.qualifier
			.as_ref()
			.map(|qualifier| qualifier.table.as_str()),
		Some(qualifier)
	);
	assert_eq!(column.name.0.as_str(), name);
}

fn int32_column_rows(batches: &[RecordBatch], column: usize) -> Vec<Option<i32>> {
	batches
		.iter()
		.flat_map(|batch| {
			batch
				.column(column)
				.as_any()
				.downcast_ref::<Int32Array>()
				.expect("Int32 column")
				.iter()
				.collect::<Vec<_>>()
		})
		.collect()
}

fn float32_column_rows(batches: &[RecordBatch], column: usize) -> Vec<Option<f32>> {
	batches
		.iter()
		.flat_map(|batch| {
			batch
				.column(column)
				.as_any()
				.downcast_ref::<Float32Array>()
				.expect("Float32 column")
				.iter()
				.collect::<Vec<_>>()
		})
		.collect()
}

fn float64_column_rows(batches: &[RecordBatch], column: usize) -> Vec<Option<f64>> {
	batches
		.iter()
		.flat_map(|batch| {
			batch
				.column(column)
				.as_any()
				.downcast_ref::<Float64Array>()
				.expect("Float64 column")
				.iter()
				.collect::<Vec<_>>()
		})
		.collect()
}

fn string_column_rows(batches: &[RecordBatch], column: usize) -> Vec<Option<String>> {
	batches
		.iter()
		.flat_map(|batch| {
			batch
				.column(column)
				.as_any()
				.downcast_ref::<StringArray>()
				.expect("Utf8 column")
				.iter()
				.map(|value| value.map(str::to_string))
				.collect::<Vec<_>>()
		})
		.collect()
}

fn explain_rows_for_assertion(batches: &[RecordBatch]) -> Vec<Vec<Option<String>>> {
	let mut rows = Vec::new();
	for batch in batches {
		for row_idx in 0..batch.num_rows() {
			let mut row = Vec::new();
			for column_idx in 0..batch.num_columns() {
				let array = batch.column(column_idx);
				if array.is_null(row_idx) {
					row.push(None);
					continue;
				}
				let value = match array.data_type() {
					DataType::Int64 => array
						.as_any()
						.downcast_ref::<Int64Array>()
						.expect("Int64 explain column")
						.value(row_idx)
						.to_string(),
					DataType::Utf8 => array
						.as_any()
						.downcast_ref::<StringArray>()
						.expect("Utf8 explain column")
						.value(row_idx)
						.to_string(),
					other => panic!("unexpected EXPLAIN data type {other:?}"),
				};
				row.push(Some(value));
			}
			rows.push(row);
		}
	}
	rows
}

fn assert_null_column(batches: &[RecordBatch], column: usize, expected_len: usize) {
	let mut len = 0;
	for batch in batches {
		let array = batch.column(column);
		assert_eq!(array.data_type(), &DataType::Null);
		len += array.len();
	}
	assert_eq!(len, expected_len);
}

fn assert_schema_names(schema: &Schema, expected: &[&str]) {
	let actual = schema
		.fields()
		.iter()
		.map(|field| field.name().as_str())
		.collect::<Vec<_>>();
	assert_eq!(actual, expected);
}

fn assert_schema_types(schema: &Schema, expected: &[DataType]) {
	let actual = schema
		.fields()
		.iter()
		.map(|field| field.data_type().clone())
		.collect::<Vec<_>>();
	assert_eq!(actual, expected);
}

fn frontend_message(error: FrontendError) -> String {
	match error {
		FrontendError::SqlParse { message }
		| FrontendError::RuneCompile { message, .. }
		| FrontendError::RuneRuntime { message, .. }
		| FrontendError::Unsupported { message }
		| FrontendError::RuneSandbox { message, .. } => message,
	}
}

fn assert_frontend_message_contains(error: FrontendError, expected: &str) {
	let message = frontend_message(error);
	assert!(
		message.contains(expected),
		"expected frontend error containing {expected:?}, got {message:?}"
	);
}
