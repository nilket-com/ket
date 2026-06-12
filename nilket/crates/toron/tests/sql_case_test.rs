use std::sync::Arc;

use arrow::array::{Array, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use toron::binder::{PlannerConfig, bind};
use toron::catalog::MockCatalog;
use toron::error::{BindingError, ExecutionError};
use toron::exec::Executor;
use toron::ir::{
	BoundExprNode, ExecutionPlan, Expr, FilterDisposition, LogicalPlan, LogicalTypeId, TableId,
};
use toron::sql_planner::SqlPlanner;
use toron::storage::InMemoryStorageScan;
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, SchemaEvolutionPolicy, TableObjectId,
	TableSchemaDescriptor,
};

#[tokio::test]
async fn case_searched_projection_executes() {
	let batches =
		run_sql("SELECT CASE WHEN amount > 20 THEN 'big' ELSE 'small' END FROM sales").await;
	assert_eq!(
		string_column_rows(&batches, 0),
		vec![
			Some("small".to_string()),
			Some("small".to_string()),
			Some("small".to_string()),
			Some("big".to_string()),
			Some("big".to_string()),
			Some("small".to_string()),
		]
	);
}

#[tokio::test]
async fn case_simple_desugars_and_executes() {
	let plan = plan_sql(
		"SELECT CASE status WHEN 'open' THEN 'O' WHEN 'closed' THEN 'C' ELSE 'X' END FROM sales",
	);
	let LogicalPlan::Project { exprs, .. } = &plan else {
		panic!("expected project");
	};
	let Expr::Case { when_then, .. } = &exprs[0] else {
		panic!("expected CASE expression, got {:?}", exprs[0]);
	};
	assert!(matches!(when_then[0].0.as_ref(), Expr::Eq(_, _)));

	let batches = run_logical(&plan).await;
	assert_eq!(
		string_column_rows(&batches, 0),
		vec![
			Some("O".to_string()),
			Some("O".to_string()),
			Some("C".to_string()),
			Some("O".to_string()),
			Some("X".to_string()),
			Some("X".to_string()),
		]
	);
}

#[tokio::test]
async fn case_first_true_wins() {
	let batches = run_sql(
		"SELECT CASE WHEN id > 0 THEN 'first' WHEN id = 1 THEN 'second' ELSE 'else' END FROM sales",
	)
	.await;
	assert_eq!(
		string_column_rows(&batches, 0),
		vec![Some("first".to_string()); 6]
	);
}

#[tokio::test]
async fn case_no_match_uses_else() {
	let batches = run_sql("SELECT CASE WHEN id < 0 THEN 'hit' ELSE 'miss' END FROM sales").await;
	assert_eq!(
		string_column_rows(&batches, 0),
		vec![Some("miss".to_string()); 6]
	);
}

#[tokio::test]
async fn case_missing_else_returns_null() {
	let batches = run_sql("SELECT CASE WHEN id = 1 THEN amount END FROM sales").await;
	assert_eq!(
		int32_column_rows(&batches, 0),
		vec![Some(10), None, None, None, None, None]
	);
}

#[tokio::test]
async fn case_nullable_condition_is_no_match() {
	let batches = run_sql("SELECT CASE WHEN flag THEN 'yes' ELSE 'no' END FROM sales").await;
	assert_eq!(
		string_column_rows(&batches, 0),
		vec![
			Some("yes".to_string()),
			Some("no".to_string()),
			Some("no".to_string()),
			Some("yes".to_string()),
			Some("no".to_string()),
			Some("no".to_string()),
		]
	);
}

#[tokio::test]
async fn case_numeric_branches_widen() {
	let batches = run_sql("SELECT CASE WHEN id = 1 THEN amount ELSE 1.5 END FROM sales").await;
	assert_eq!(
		float64_column_rows(&batches, 0),
		vec![
			Some(10.0),
			Some(1.5),
			Some(1.5),
			Some(1.5),
			Some(1.5),
			Some(1.5)
		]
	);
}

#[tokio::test]
async fn case_identical_text_branches_execute() {
	let batches =
		run_sql("SELECT CASE WHEN id = 1 THEN status ELSE 'fallback' END FROM sales").await;
	assert_eq!(
		string_column_rows(&batches, 0),
		vec![
			Some("open".to_string()),
			Some("fallback".to_string()),
			Some("fallback".to_string()),
			Some("fallback".to_string()),
			Some("fallback".to_string()),
			Some("fallback".to_string()),
		]
	);
}

#[tokio::test]
async fn case_type_mismatch_rejects_at_bind() {
	let err = bind_sql_result("SELECT CASE WHEN id = 1 THEN amount ELSE status END FROM sales")
		.await
		.expect_err("CASE type mismatch should reject");
	match err {
		BindingError::TypeMismatch { message } => {
			assert!(message.contains("CASE result branches cannot be unified"));
		}
		other => panic!("unexpected error: {other:?}"),
	}
}

#[tokio::test]
async fn case_in_where_executes_as_residual() {
	let sql = "SELECT id FROM sales WHERE CASE WHEN amount > 20 THEN true ELSE false END";
	let bound = bind_sql(sql).await;
	let (predicate, disposition) = find_filter(&bound);
	assert_eq!(*disposition, FilterDisposition::EvaluateInToron);
	assert!(matches!(
		predicate
			.single_root()
			.and_then(|root| predicate.node(root)),
		Some(BoundExprNode::Case { .. })
	));

	let batches = execute_bound(&bound).await.expect("execute CASE WHERE");
	assert_eq!(ids(&batches), vec![4, 5]);
}

#[tokio::test]
async fn case_in_order_by_executes() {
	let batches = run_sql(
		"SELECT SUM(amount) FROM sales ORDER BY CASE WHEN SUM(amount) > 0 THEN 0 ELSE 1 END",
	)
	.await;
	assert_eq!(int64_column_rows(&batches, 0), vec![Some(87)]);
}

#[tokio::test]
async fn case_in_aggregate_argument_executes() {
	let batches =
		run_sql("SELECT SUM(CASE WHEN status = 'open' THEN amount ELSE 0 END) FROM sales").await;
	assert_eq!(int64_column_rows(&batches, 0), vec![Some(40)]);
}

#[test]
fn case_nested_aggregate_in_aggregate_argument_rejects() {
	let err = SqlPlanner::new()
		.plan("SELECT SUM(CASE WHEN amount > 0 THEN COUNT(*) ELSE 0 END) FROM sales")
		.expect_err("nested aggregate inside CASE should reject");
	assert!(
		err.to_string()
			.contains("aggregate arguments must not contain aggregate calls")
	);
}

#[tokio::test]
async fn case_in_having_executes() {
	let batches = run_sql(
		"SELECT SUM(amount) FROM sales HAVING CASE WHEN SUM(amount) > 0 THEN true ELSE false END",
	)
	.await;
	assert_eq!(int64_column_rows(&batches, 0), vec![Some(87)]);
}

#[tokio::test]
async fn case_all_naked_result_stays_naked() {
	let bound = bind_sql("SELECT CASE WHEN id = 1 THEN amount ELSE 0 END FROM sales").await;
	let ExecutionPlan::Project { exprs, .. } = bound else {
		panic!("expected project");
	};
	assert_eq!(exprs.root_logical_type(0), Some(LogicalTypeId::NAKED));
}

#[tokio::test]
async fn case_numeric_widening_all_naked_still_works() {
	let bound = bind_sql("SELECT CASE WHEN id = 1 THEN amount ELSE 1.5 END FROM sales").await;
	let ExecutionPlan::Project { exprs, .. } = bound else {
		panic!("expected project");
	};
	assert_eq!(exprs.root_type(0), Some(&DataType::Float64));
	assert_eq!(exprs.root_logical_type(0), Some(LogicalTypeId::NAKED));
}

#[tokio::test]
async fn case_divide_guard_is_safe_via_null() {
	let batches =
		run_sql("SELECT CASE WHEN amount <> 0 THEN 100 / amount ELSE 0 END FROM sales").await;
	assert_eq!(
		int32_column_rows(&batches, 0),
		vec![Some(10), Some(0), Some(0), Some(3), Some(2), Some(14)]
	);
}

#[tokio::test]
async fn case_branches_eval_eagerly_masked_hard_error_fails() {
	let err = run_sql_result("SELECT CASE WHEN id < 0 THEN 2147483647 + 1 ELSE 0 END FROM sales")
		.await
		.expect_err("masked overflow branch should still fail");
	match err {
		ExecutionError::Kernel { message } => {
			assert!(message.starts_with("arithmetic overflow: Add(Int32)"));
		}
		other => panic!("unexpected error: {other:?}"),
	}
}

#[tokio::test]
async fn coalesce_returns_first_non_null() {
	let batches = run_sql("SELECT COALESCE(amount, id) FROM sales").await;
	assert_eq!(
		int32_column_rows(&batches, 0),
		vec![Some(10), Some(0), Some(3), Some(30), Some(40), Some(7)]
	);
}

#[tokio::test]
async fn coalesce_single_arg_is_identity() {
	let plan = plan_sql("SELECT COALESCE(amount) FROM sales");
	let LogicalPlan::Project { exprs, .. } = &plan else {
		panic!("expected project");
	};
	assert!(matches!(exprs[0], Expr::Column(_)));

	let batches = run_logical(&plan).await;
	assert_eq!(
		int32_column_rows(&batches, 0),
		vec![Some(10), Some(0), None, Some(30), Some(40), Some(7)]
	);
}

#[tokio::test]
async fn coalesce_explicit_null_argument_uses_typed_fallback() {
	let batches = run_sql("SELECT COALESCE(NULL, amount) FROM sales").await;
	assert_eq!(
		int32_column_rows(&batches, 0),
		vec![Some(10), Some(0), None, Some(30), Some(40), Some(7)]
	);
}

#[tokio::test]
async fn coalesce_numeric_branches_widen() {
	let batches = run_sql("SELECT COALESCE(amount, 1.5) FROM sales").await;
	assert_eq!(
		float64_column_rows(&batches, 0),
		vec![
			Some(10.0),
			Some(0.0),
			Some(1.5),
			Some(30.0),
			Some(40.0),
			Some(7.0)
		]
	);
}

#[tokio::test]
async fn sugar_family_all_null_projects_null() {
	let bound = bind_sql(
		"SELECT \
		 COALESCE(NULL, NULL), \
		 IF(true, NULL, NULL), \
		 IFNULL(NULL, NULL), \
		 NVL(NULL, NULL), \
		 NVL2(amount, NULL, NULL) \
		 FROM sales",
	)
	.await;
	let ExecutionPlan::Project {
		exprs,
		output_schema,
		..
	} = &bound
	else {
		panic!("expected project");
	};
	for idx in 0..5 {
		assert_eq!(exprs.root_type(idx), Some(&DataType::Null));
		assert_eq!(output_schema.field(idx).data_type(), &DataType::Null);
		assert!(output_schema.field(idx).is_nullable());
	}

	let batches = execute_bound(&bound).await.expect("execute all-NULL sugar");
	for idx in 0..5 {
		assert_null_column(&batches, idx, 6);
	}
}

#[tokio::test]
async fn nullif_returns_null_when_equal_and_first_arg_when_unequal() {
	let batches = run_sql("SELECT NULLIF(status, 'open') FROM sales").await;
	assert_eq!(
		string_column_rows(&batches, 0),
		vec![
			None,
			None,
			Some("closed".to_string()),
			None,
			None,
			Some("pending".to_string()),
		]
	);
}

#[tokio::test]
async fn nullif_result_type_is_first_argument_type() {
	let bound = bind_sql("SELECT NULLIF(amount, 0) FROM sales").await;
	let ExecutionPlan::Project { exprs, .. } = &bound else {
		panic!("expected project");
	};
	assert_eq!(exprs.root_type(0), Some(&DataType::Int32));

	let batches = execute_bound(&bound).await.expect("execute NULLIF");
	assert_eq!(
		int32_column_rows(&batches, 0),
		vec![Some(10), None, None, Some(30), Some(40), Some(7)]
	);
}

#[tokio::test]
async fn if_function_executes_and_iif_is_alias() {
	let if_rows = run_sql("SELECT IF(flag, 'yes', 'no') FROM sales").await;
	let iif_rows = run_sql("SELECT IIF(flag, 'yes', 'no') FROM sales").await;
	let expected = vec![
		Some("yes".to_string()),
		Some("no".to_string()),
		Some("no".to_string()),
		Some("yes".to_string()),
		Some("no".to_string()),
		Some("no".to_string()),
	];
	assert_eq!(string_column_rows(&if_rows, 0), expected);
	assert_eq!(string_column_rows(&iif_rows, 0), expected);
}

#[tokio::test]
async fn if_null_result_branch_uses_typed_case_null() {
	let batches = run_sql("SELECT IF(flag, NULL, amount) FROM sales").await;
	assert_eq!(
		int32_column_rows(&batches, 0),
		vec![None, Some(0), None, None, Some(40), Some(7)]
	);
}

#[tokio::test]
async fn if_condition_must_be_boolean() {
	let err = bind_sql_result("SELECT IF(amount, 1, 0) FROM sales")
		.await
		.expect_err("IF condition should require Boolean");
	match err {
		BindingError::TypeMismatch { message } => {
			assert!(message.contains("CASE WHEN expected Boolean"));
		}
		other => panic!("unexpected error: {other:?}"),
	}
}

#[tokio::test]
async fn if_explicit_null_condition_is_no_match() {
	let batches = run_sql("SELECT IF(NULL, 1, 0) FROM sales").await;
	assert_eq!(
		int32_column_rows(&batches, 0),
		vec![Some(0), Some(0), Some(0), Some(0), Some(0), Some(0)]
	);
}

#[test]
fn if_arity_not_three_rejects() {
	let err = SqlPlanner::new()
		.plan("SELECT IF(flag, 1) FROM sales")
		.expect_err("IF arity should reject");
	assert!(
		err.to_string()
			.contains("IF/IIF requires exactly three arguments")
	);
}

#[tokio::test]
async fn nullif_first_explicit_null_operand_projects_null() {
	let bound = bind_sql("SELECT NULLIF(NULL, amount) FROM sales").await;
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

	let batches = execute_bound(&bound).await.expect("execute NULLIF");
	assert_null_column(&batches, 0, 6);
}

#[tokio::test]
async fn nullif_two_contextless_nulls_still_rejects() {
	let err = bind_sql_result("SELECT NULLIF(NULL, NULL) FROM sales")
		.await
		.expect_err("NULLIF comparison with two contextless NULL operands should reject");
	match err {
		BindingError::UnsupportedExpression { message } => {
			assert!(message.contains("NULL literal has no type in this position"));
		}
		BindingError::TypeMismatch { message } => {
			assert!(message.contains("NULL"));
		}
		other => panic!("unexpected error: {other:?}"),
	}
}

#[tokio::test]
async fn nullif_second_explicit_null_operand_returns_first_arg() {
	let batches = run_sql("SELECT NULLIF(amount, NULL) FROM sales").await;
	assert_eq!(
		int32_column_rows(&batches, 0),
		vec![Some(10), Some(0), None, Some(30), Some(40), Some(7)]
	);
}

#[tokio::test]
async fn ifnull_and_nvl_match_two_arg_coalesce() {
	let batches = run_sql("SELECT IFNULL(NULL, status), NVL(amount, id) FROM sales").await;
	assert_eq!(
		string_column_rows(&batches, 0),
		vec![
			Some("open".to_string()),
			Some("open".to_string()),
			Some("closed".to_string()),
			Some("open".to_string()),
			None,
			Some("pending".to_string()),
		]
	);
	assert_eq!(
		int32_column_rows(&batches, 1),
		vec![Some(10), Some(0), Some(3), Some(30), Some(40), Some(7)]
	);
}

#[test]
fn ifnull_nvl_arity_not_two_rejects() {
	for sql in [
		"SELECT IFNULL(amount) FROM sales",
		"SELECT NVL(amount, id, 0) FROM sales",
	] {
		let err = SqlPlanner::new()
			.plan(sql)
			.expect_err("arity should reject");
		assert!(
			err.to_string()
				.contains("IFNULL/NVL requires exactly two arguments")
		);
	}
}

#[tokio::test]
async fn nvl2_desugars_and_executes() {
	let plan = plan_sql("SELECT NVL2(amount, 'has', 'missing') FROM sales");
	let LogicalPlan::Project { exprs, .. } = &plan else {
		panic!("expected project");
	};
	let Expr::Case { when_then, .. } = &exprs[0] else {
		panic!("expected NVL2 to lower to CASE");
	};
	assert!(matches!(when_then[0].0.as_ref(), Expr::IsNotNull(_)));

	let batches = run_logical(&plan).await;
	assert_eq!(
		string_column_rows(&batches, 0),
		vec![
			Some("has".to_string()),
			Some("has".to_string()),
			Some("missing".to_string()),
			Some("has".to_string()),
			Some("has".to_string()),
			Some("has".to_string()),
		]
	);
}

#[test]
fn nvl2_arity_not_three_rejects() {
	for sql in [
		"SELECT NVL2(amount, 1) FROM sales",
		"SELECT NVL2(amount, 1, 0, 2) FROM sales",
	] {
		let err = SqlPlanner::new()
			.plan(sql)
			.expect_err("NVL2 arity should reject");
		assert!(
			err.to_string()
				.contains("NVL2 requires exactly three arguments")
		);
	}
}

#[tokio::test]
async fn conditional_sugar_in_where_order_by_having_and_aggregate_arg() {
	let where_bound = bind_sql("SELECT id FROM sales WHERE IF(amount > 20, true, false)").await;
	let (predicate, disposition) = find_filter(&where_bound);
	assert_eq!(*disposition, FilterDisposition::EvaluateInToron);
	assert!(matches!(
		predicate
			.single_root()
			.and_then(|root| predicate.node(root)),
		Some(BoundExprNode::Case { .. })
	));
	let where_rows = execute_bound(&where_bound).await.expect("execute IF WHERE");
	assert_eq!(ids(&where_rows), vec![4, 5]);

	let order_rows =
		run_sql("SELECT SUM(amount) FROM sales ORDER BY IF(SUM(amount) > 0, 0, 1)").await;
	assert_eq!(int64_column_rows(&order_rows, 0), vec![Some(87)]);

	let aggregate_rows = run_sql("SELECT SUM(IF(status = 'open', amount, 0)) FROM sales").await;
	assert_eq!(int64_column_rows(&aggregate_rows, 0), vec![Some(40)]);

	let having_rows =
		run_sql("SELECT SUM(amount) FROM sales HAVING IF(SUM(amount) > 0, true, false)").await;
	assert_eq!(int64_column_rows(&having_rows, 0), vec![Some(87)]);
}

#[tokio::test]
async fn conditional_sugar_in_derived_table_executes() {
	let batches = run_sql(
		"SELECT x FROM (SELECT COALESCE(amount, id) AS x FROM sales) AS d WHERE IF(x > 20, true, false)",
	)
	.await;
	assert_eq!(int32_column_rows(&batches, 0), vec![Some(30), Some(40)]);
}

#[test]
fn conditional_sugar_in_update_scalar_exprs_plans() {
	let plan = SqlPlanner::new()
		.plan(
			"UPDATE sales SET amount = COALESCE(NULL, amount) \
			 WHERE IF(flag, true, false) \
			 RETURNING NULLIF(amount, 0)",
		)
		.expect("plan UPDATE with conditional sugar");
	let LogicalPlan::Update {
		assignments,
		predicate,
		returning,
		..
	} = &plan
	else {
		panic!("expected update plan");
	};
	assert!(matches!(assignments[0].1, Expr::Case { .. }));
	assert!(matches!(predicate, Some(Expr::Case { .. })));
	let Some(toron::ir::ReturningProjection::Exprs { exprs, .. }) = returning else {
		panic!("expected expression returning");
	};
	assert!(matches!(exprs[0], Expr::Case { .. }));
}

#[tokio::test]
async fn conditional_sugar_inherits_case_eager_hard_error() {
	let err = run_sql_result("SELECT IF(true, 0, 2147483647 + 1) FROM sales")
		.await
		.expect_err("masked IF overflow branch should still fail");
	match err {
		ExecutionError::Kernel { message } => {
			assert!(message.starts_with("arithmetic overflow: Add(Int32)"));
		}
		other => panic!("unexpected error: {other:?}"),
	}
}

#[tokio::test]
async fn bare_select_null_projects_null_array() {
	let bound = bind_sql("SELECT NULL FROM sales").await;
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
	assert!(output_schema.field(0).is_nullable());

	let batches = execute_bound(&bound).await.expect("execute bare NULL");
	assert_null_column(&batches, 0, 6);
}

#[tokio::test]
async fn select_null_respects_empty_input_row_count() {
	let bound = bind_sql("SELECT NULL FROM sales WHERE id < 0").await;
	assert_eq!(bound.schema().field(0).data_type(), &DataType::Null);

	let batches = execute_bound(&bound)
		.await
		.expect("execute empty NULL projection");
	assert_null_column(&batches, 0, 0);
}

#[tokio::test]
async fn null_result_is_not_a_coercion_participant() {
	let err = bind_sql_result("SELECT COALESCE(NULL, NULL) + 1 FROM sales")
		.await
		.expect_err("DataType::Null should not widen into arithmetic");
	match err {
		BindingError::TypeMismatch { message } => {
			assert!(message.contains("Add") || message.contains("arithmetic"));
		}
		other => panic!("unexpected error: {other:?}"),
	}
}

#[tokio::test]
async fn cast_null_as_type_projects_typed_null() {
	let bound = bind_sql("SELECT CAST(NULL AS BIGINT), TRY_CAST(NULL AS BIGINT) FROM sales").await;
	let ExecutionPlan::Project { exprs, .. } = &bound else {
		panic!("expected project");
	};
	assert_eq!(exprs.root_type(0), Some(&DataType::Int64));
	assert_eq!(exprs.root_type(1), Some(&DataType::Int64));

	let batches = execute_bound(&bound).await.expect("execute NULL casts");
	let expected = vec![None, None, None, None, None, None];
	assert_eq!(int64_column_rows(&batches, 0), expected);
	assert_eq!(int64_column_rows(&batches, 1), expected);
}

#[tokio::test]
async fn null_is_null_and_is_not_null_bind_without_sentinel() {
	let batches = run_sql("SELECT NULL IS NULL, NULL IS NOT NULL FROM sales").await;
	assert_eq!(
		bool_column_rows(&batches, 0),
		vec![
			Some(true),
			Some(true),
			Some(true),
			Some(true),
			Some(true),
			Some(true)
		]
	);
	assert_eq!(
		bool_column_rows(&batches, 1),
		vec![
			Some(false),
			Some(false),
			Some(false),
			Some(false),
			Some(false),
			Some(false)
		]
	);
}

#[tokio::test]
async fn arithmetic_with_null_operand_executes() {
	let batches = run_sql("SELECT amount + NULL FROM sales").await;
	assert_eq!(int32_column_rows(&batches, 0), vec![None; 6]);
}

#[test]
fn procedural_if_statement_remains_unsupported() {
	let err = SqlPlanner::new()
		.plan("IF true THEN SELECT 1; END IF")
		.expect_err("procedural IF should remain unsupported");
	assert!(
		err.to_string().contains("unsupported SQL statement")
			|| err.to_string().contains("sql parser error")
	);
}

#[test]
fn conditional_sugar_rejects_non_plain_args_and_modifiers() {
	for sql in [
		"SELECT COALESCE(*) FROM sales",
		"SELECT COALESCE(DISTINCT amount, 0) FROM sales",
	] {
		let err = SqlPlanner::new()
			.plan(sql)
			.expect_err("non-plain conditional sugar call should reject");
		assert!(
			err.to_string()
				.contains("supports only plain positional arguments")
				|| err
					.to_string()
					.contains("does not support argument clauses")
		);
	}
}

fn plan_sql(sql: &str) -> LogicalPlan {
	SqlPlanner::new().plan(sql).expect("plan SQL")
}

async fn bind_sql(sql: &str) -> ExecutionPlan {
	bind_sql_result(sql).await.expect("bind SQL")
}

async fn bind_sql_result(sql: &str) -> Result<ExecutionPlan, BindingError> {
	let catalog = catalog();
	bind(&plan_sql(sql), &catalog, &PlannerConfig::default()).await
}

async fn run_sql(sql: &str) -> Vec<RecordBatch> {
	run_sql_result(sql).await.expect("execute SQL")
}

async fn run_sql_result(sql: &str) -> Result<Vec<RecordBatch>, ExecutionError> {
	let bound = bind_sql(sql).await;
	execute_bound(&bound).await
}

async fn run_logical(plan: &LogicalPlan) -> Vec<RecordBatch> {
	let catalog = catalog();
	let bound = bind(plan, &catalog, &PlannerConfig::default())
		.await
		.expect("bind logical plan");
	execute_bound(&bound).await.expect("execute logical plan")
}

async fn execute_bound(bound: &ExecutionPlan) -> Result<Vec<RecordBatch>, ExecutionError> {
	Executor::new(Arc::new(InMemoryStorageScan {
		table_data: sales_batch(schema()),
	}))
	.execute_bound(bound, None)
	.await
}

fn find_filter(plan: &ExecutionPlan) -> (&toron::ir::BoundExprProgram, &FilterDisposition) {
	match plan {
		ExecutionPlan::Project { input, .. } | ExecutionPlan::Sort { input, .. } => {
			find_filter(input)
		}
		ExecutionPlan::Filter {
			predicate,
			disposition,
			..
		} => (predicate, disposition),
		other => panic!("expected filter plan, got {other:?}"),
	}
}

fn ids(batches: &[RecordBatch]) -> Vec<i32> {
	batches
		.iter()
		.flat_map(|batch| {
			batch
				.column(0)
				.as_any()
				.downcast_ref::<Int32Array>()
				.expect("id column")
				.iter()
				.map(|value| value.expect("id value"))
				.collect::<Vec<_>>()
		})
		.collect()
}

fn int32_column_rows(batches: &[RecordBatch], column_idx: usize) -> Vec<Option<i32>> {
	batches
		.iter()
		.flat_map(|batch| {
			batch
				.column(column_idx)
				.as_any()
				.downcast_ref::<Int32Array>()
				.expect("Int32 column")
				.iter()
				.collect::<Vec<_>>()
		})
		.collect()
}

fn int64_column_rows(batches: &[RecordBatch], column_idx: usize) -> Vec<Option<i64>> {
	batches
		.iter()
		.flat_map(|batch| {
			batch
				.column(column_idx)
				.as_any()
				.downcast_ref::<Int64Array>()
				.expect("Int64 column")
				.iter()
				.collect::<Vec<_>>()
		})
		.collect()
}

fn bool_column_rows(batches: &[RecordBatch], column_idx: usize) -> Vec<Option<bool>> {
	batches
		.iter()
		.flat_map(|batch| {
			batch
				.column(column_idx)
				.as_any()
				.downcast_ref::<BooleanArray>()
				.expect("Boolean column")
				.iter()
				.collect::<Vec<_>>()
		})
		.collect()
}

fn float64_column_rows(batches: &[RecordBatch], column_idx: usize) -> Vec<Option<f64>> {
	batches
		.iter()
		.flat_map(|batch| {
			batch
				.column(column_idx)
				.as_any()
				.downcast_ref::<Float64Array>()
				.expect("Float64 column")
				.iter()
				.collect::<Vec<_>>()
		})
		.collect()
}

fn string_column_rows(batches: &[RecordBatch], column_idx: usize) -> Vec<Option<String>> {
	batches
		.iter()
		.flat_map(|batch| {
			batch
				.column(column_idx)
				.as_any()
				.downcast_ref::<StringArray>()
				.expect("Utf8 column")
				.iter()
				.map(|value| value.map(str::to_string))
				.collect::<Vec<_>>()
		})
		.collect()
}

fn assert_null_column(batches: &[RecordBatch], column_idx: usize, expected_len: usize) {
	let mut len = 0;
	for batch in batches {
		let column = batch.column(column_idx);
		assert_eq!(column.data_type(), &DataType::Null);
		len += column.len();
	}
	assert_eq!(len, expected_len);
}

fn catalog() -> MockCatalog {
	let mut catalog = MockCatalog::new();
	let schema = schema();
	catalog
		.register_descriptor(TableId(1), descriptor(schema.as_ref()))
		.expect("register sales");
	catalog
}

fn schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int32, false),
		Field::new("amount", DataType::Int32, true),
		Field::new("status", DataType::Utf8, true),
		Field::new("flag", DataType::Boolean, true),
	]))
}

fn sales_batch(schema: SchemaRef) -> RecordBatch {
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
			Arc::new(Int32Array::from(vec![
				Some(10),
				Some(0),
				None,
				Some(30),
				Some(40),
				Some(7),
			])),
			Arc::new(StringArray::from(vec![
				Some("open"),
				Some("open"),
				Some("closed"),
				Some("open"),
				None,
				Some("pending"),
			])),
			Arc::new(arrow::array::BooleanArray::from(vec![
				Some(true),
				Some(false),
				None,
				Some(true),
				None,
				Some(false),
			])),
		],
	)
	.expect("sales batch")
}

fn descriptor(schema: &Schema) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new(
		TableObjectId::new([108; 16]),
		1,
		"default",
		"sales",
		schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("descriptor")
}
