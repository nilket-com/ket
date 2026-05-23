use std::sync::Arc;

use arrow::array::{Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use toron::binder::{PlannerConfig, bind};
use toron::catalog::MockCatalog;
use toron::error::{BindingError, FrontendError};
use toron::exec::Executor;
use toron::ir::{BoundExprNode, ExecutionPlan, Expr, FilterDisposition, LogicalPlan, TableId};
use toron::sql_planner::SqlPlanner;
use toron::storage::InMemoryStorageScan;
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptorV1, RowIdentityPolicy, SchemaEvolutionPolicyV1,
	TableObjectId, TableSchemaDescriptor,
	pbc::{OP_IN_LIST, OP_NOT},
};

#[test]
fn planner_lowers_in_list_and_between_shapes() {
	let plan = plan_sql(
		"SELECT id FROM sales \
		 WHERE status IN ('closed', 'pending') AND amount BETWEEN 10 AND 30",
	);

	let LogicalPlan::Project { input, .. } = plan else {
		panic!("expected project");
	};
	let LogicalPlan::Filter { predicate, .. } = input.as_ref() else {
		panic!("expected filter");
	};
	let Expr::And(left, right) = predicate else {
		panic!("expected conjunction, got {predicate:?}");
	};
	assert!(matches!(
		left.as_ref(),
		Expr::InList {
			expr,
			list,
			negated: false,
		} if matches!(expr.as_ref(), Expr::Column(column) if column.name.0 == "status")
			&& list.len() == 2
	));
	assert!(matches!(right.as_ref(), Expr::And(_, _)));
}

#[tokio::test]
async fn where_in_list_pushes_op_in_list_to_wendelon() {
	let bound = bind_sql("SELECT id FROM sales WHERE status IN ('closed', 'missing')").await;
	let (_, disposition, scan) = find_filter(&bound);
	assert_eq!(*disposition, FilterDisposition::FullyPushed);
	let pbc = scan_pbc(scan).expect("PBC predicate");
	assert!(pbc.contains(&OP_IN_LIST));

	assert_ids(
		"SELECT id FROM sales WHERE status IN ('closed', 'missing')",
		&[3],
	)
	.await;
}

#[tokio::test]
async fn where_not_in_list_pushes_op_in_list_plus_not() {
	let bound = bind_sql("SELECT id FROM sales WHERE status NOT IN ('open')").await;
	let (_, disposition, scan) = find_filter(&bound);
	assert_eq!(*disposition, FilterDisposition::FullyPushed);
	let pbc = scan_pbc(scan).expect("PBC predicate");
	assert!(pbc.contains(&OP_IN_LIST));
	assert_eq!(*pbc.last().expect("last opcode"), OP_NOT);

	assert_ids("SELECT id FROM sales WHERE status NOT IN ('open')", &[3, 6]).await;
}

#[tokio::test]
async fn in_list_at_max_items_pushes_without_pbc_limit_error() {
	let members = (0..32).map(|value| value.to_string()).collect::<Vec<_>>();
	let sql = format!(
		"SELECT id FROM sales WHERE amount IN ({})",
		members.join(", ")
	);
	let bound = bind_sql(&sql).await;
	let (_, disposition, scan) = find_filter(&bound);
	assert_eq!(*disposition, FilterDisposition::FullyPushed);
	let pbc = scan_pbc(scan).expect("PBC predicate");
	assert!(pbc.contains(&OP_IN_LIST));
	wendelon::schema::pbc::Program::compile(pbc).expect("Wendelon accepts max IN list PBC");
}

#[tokio::test]
async fn where_in_list_residual_evals_when_not_pushdown_eligible() {
	let sql = "SELECT id FROM sales WHERE amount + 1 IN (11, 21)";
	let bound = bind_sql(sql).await;
	let (_, disposition, scan) = find_filter(&bound);
	assert_eq!(*disposition, FilterDisposition::EvaluateInToron);
	assert!(scan_pbc(scan).is_none());

	assert_ids(sql, &[1, 2]).await;
}

#[tokio::test]
async fn where_in_list_filters_rows() {
	assert_ids(
		"SELECT id FROM sales WHERE status IN ('closed', 'pending')",
		&[3, 6],
	)
	.await;
}

#[tokio::test]
async fn where_not_in_list_filters_rows() {
	assert_ids(
		"SELECT id FROM sales WHERE status NOT IN ('open', 'closed')",
		&[6],
	)
	.await;
}

#[tokio::test]
async fn in_list_with_column_or_arithmetic_member_reuses_existing_lowering() {
	let sql = "SELECT id FROM sales WHERE amount IN (id + 9, id + 18)";
	let bound = bind_sql(sql).await;
	let (predicate, disposition, scan) = find_filter(&bound);
	assert_eq!(*disposition, FilterDisposition::EvaluateInToron);
	assert!(scan_pbc(scan).is_none());
	assert!(matches!(
		predicate.single_root().and_then(|root| predicate.node(root)),
		Some(BoundExprNode::InList { list, .. }) if list.len() == 2
	));

	assert_ids(sql, &[1, 2]).await;
}

#[tokio::test]
async fn in_list_type_mismatch_fails_at_binding() {
	let plan = plan_sql("SELECT id FROM sales WHERE status IN (1)");
	let err = bind(&plan, &catalog(), &PlannerConfig::default())
		.await
		.expect_err("bind must reject mismatched IN operand types");

	assert!(matches!(
		err,
		BindingError::TypeMismatch { message } if message.contains("IN operands")
	));
}

#[test]
fn in_list_rejects_empty_over_limit_subquery_unnest_rowvalue_or_null() {
	let over_limit = format!(
		"SELECT id FROM sales WHERE id IN ({})",
		(0..33)
			.map(|value| value.to_string())
			.collect::<Vec<_>>()
			.join(", ")
	);
	for sql in [
		"SELECT id FROM sales WHERE id IN ()",
		over_limit.as_str(),
		"SELECT id FROM sales WHERE id IN (SELECT id FROM sales)",
		"SELECT id FROM sales WHERE id IN UNNEST([1, 2])",
		"SELECT id FROM sales WHERE (id, amount) IN ((1, 10))",
		"SELECT id FROM sales WHERE id IN (1, NULL)",
	] {
		assert_plan_rejected(sql);
	}
}

#[tokio::test]
async fn where_between_desugars_and_filters() {
	let plan = plan_sql("SELECT id FROM sales WHERE amount BETWEEN 10 AND 30");
	let LogicalPlan::Project { input, .. } = plan else {
		panic!("expected project");
	};
	let LogicalPlan::Filter { predicate, .. } = input.as_ref() else {
		panic!("expected filter");
	};
	assert!(matches!(predicate, Expr::And(_, _)));

	assert_ids(
		"SELECT id FROM sales WHERE amount BETWEEN 10 AND 30",
		&[1, 2, 4],
	)
	.await;
}

#[tokio::test]
async fn where_not_between_filters() {
	assert_ids(
		"SELECT id FROM sales WHERE amount NOT BETWEEN 10 AND 30",
		&[5, 6],
	)
	.await;
}

#[tokio::test]
async fn between_pbc_matches_equivalent_and_chain() {
	let between = pbc_for("SELECT id FROM sales WHERE amount BETWEEN 10 AND 30").await;
	let equivalent = pbc_for("SELECT id FROM sales WHERE amount >= 10 AND amount <= 30").await;

	assert_eq!(between, equivalent);
	assert!(between.iter().all(|opcode| *opcode != OP_IN_LIST));
}

#[test]
fn like_ilike_case_subquery_in_remain_unsupported() {
	for sql in [
		"SELECT id FROM sales WHERE status LIKE 'o%'",
		"SELECT id FROM sales WHERE status ILIKE 'o%'",
		"SELECT id FROM sales WHERE status SIMILAR TO 'o%'",
		"SELECT id FROM sales WHERE status RLIKE 'o.*'",
		"SELECT id FROM sales WHERE CASE WHEN id = 1 THEN true ELSE false END",
		"SELECT id FROM sales WHERE id IN (SELECT id FROM sales)",
	] {
		assert_plan_rejected(sql);
	}
}

#[test]
fn alter_table_remains_unsupported() {
	assert_plan_rejected("ALTER TABLE sales ADD COLUMN extra INT");
}

fn plan_sql(sql: &str) -> LogicalPlan {
	SqlPlanner::new().plan(sql).expect("plan SQL")
}

async fn bind_sql(sql: &str) -> ExecutionPlan {
	let catalog = catalog();
	bind(&plan_sql(sql), &catalog, &PlannerConfig::default())
		.await
		.expect("bind SQL")
}

async fn pbc_for(sql: &str) -> Vec<u8> {
	let bound = bind_sql(sql).await;
	let (_, disposition, scan) = find_filter(&bound);
	assert_eq!(*disposition, FilterDisposition::FullyPushed);
	scan_pbc(scan).expect("PBC predicate").to_vec()
}

async fn assert_ids(sql: &str, expected: &[i32]) {
	let catalog = catalog();
	let bound = bind(&plan_sql(sql), &catalog, &PlannerConfig::default())
		.await
		.expect("bind SQL");
	let batches = Executor::new(Arc::new(InMemoryStorageScan {
		table_data: sales_batch(schema()),
	}))
	.execute_bound(&bound, None)
	.await
	.expect("execute SQL");
	assert_eq!(ids(&batches), expected);
}

fn assert_plan_rejected(sql: &str) {
	match SqlPlanner::new().plan(sql) {
		Err(FrontendError::Unsupported { .. }) => {}
		Err(_) => {}
		Ok(plan) => panic!("{sql}: expected rejection, got {plan:?}"),
	}
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
	]))
}

fn sales_batch(schema: SchemaRef) -> RecordBatch {
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
			Arc::new(Int32Array::from(vec![
				Some(10),
				Some(20),
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
		],
	)
	.expect("sales batch")
}

fn descriptor(schema: &Schema) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new(
		TableObjectId::new([74; 16]),
		1,
		"default",
		"sales",
		schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.expect("descriptor")
}
