use std::sync::Arc;

use arrow::array::{Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use toron::binder::{PlannerConfig, bind};
use toron::catalog::MockCatalog;
use toron::error::{BindingError, FrontendError};
use toron::exec::Executor;
use toron::ir::{
	BoundBinaryOp, BoundExprNode, BoundExprProgram, BoundUnaryOp, ColumnOrdinal, ExecutionPlan,
	Expr, FilterDisposition, LogicalPlan, ScalarValue, TableId,
};
use toron::sql_planner::SqlPlanner;
use toron::storage::InMemoryStorageScan;
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptorV1, RowIdentityPolicy, SchemaEvolutionPolicyV1,
	TableObjectId, TableSchemaDescriptor,
	pbc::{OP_IN_LIST, OP_NOT, OP_OR},
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
async fn where_like_and_ilike_filter_rows_without_pbc_opcode() {
	let like_sql = "SELECT id FROM sales WHERE status LIKE 'o%'";
	let bound = bind_sql(like_sql).await;
	let (predicate, disposition, scan) = find_filter(&bound);
	assert_eq!(*disposition, FilterDisposition::EvaluateInToron);
	assert!(scan_pbc(scan).is_none());
	assert!(matches!(
		predicate
			.single_root()
			.and_then(|root| predicate.node(root)),
		Some(BoundExprNode::Like {
			negated: false,
			case_insensitive: false,
			..
		})
	));
	assert_ids(like_sql, &[1, 2, 4]).await;

	let ilike_sql = "SELECT id FROM sales WHERE status ILIKE 'PEN%'";
	let bound = bind_sql(ilike_sql).await;
	let (_, disposition, scan) = find_filter(&bound);
	assert_eq!(*disposition, FilterDisposition::EvaluateInToron);
	assert!(scan_pbc(scan).is_none());
	assert_ids(ilike_sql, &[6]).await;
}

#[tokio::test]
async fn where_not_like_filters_rows() {
	assert_ids("SELECT id FROM sales WHERE status NOT LIKE 'o%'", &[3, 6]).await;
	assert_ids(
		"SELECT id FROM sales WHERE status NOT ILIKE 'PEN%'",
		&[1, 2, 3, 4],
	)
	.await;
}

#[tokio::test]
async fn where_like_residual_splits_from_pbc_conjunct_and_prunes_pushed_column() {
	let sql = "SELECT status FROM sales WHERE id = 3 AND status LIKE 'c%'";
	let bound = bind_sql(sql).await;
	let (predicate, disposition, scan) = find_filter(&bound);

	assert_eq!(*disposition, FilterDisposition::EvaluateInToron);
	assert!(matches!(
		predicate
			.single_root()
			.and_then(|root| predicate.node(root)),
		Some(BoundExprNode::Like { .. })
	));
	let ExecutionPlan::Scan {
		projection,
		pbc_program,
		schema,
		..
	} = scan
	else {
		panic!("expected scan input, got {scan:?}");
	};
	assert!(pbc_program.is_some());
	assert_eq!(projection.as_deref(), Some(&[ColumnOrdinal(2)][..]));
	assert_eq!(schema.field(0).name(), "status");
	assert_statuses(sql, &["closed"]).await;
}

#[tokio::test]
async fn where_fully_pushable_or_pushes_whole_or() {
	let sql = "SELECT id FROM sales WHERE id = 1 OR id = 3";
	let bound = bind_sql(sql).await;
	let (_, disposition, scan) = find_filter(&bound);

	assert_eq!(*disposition, FilterDisposition::FullyPushed);
	let pbc = scan_pbc(scan).expect("PBC predicate");
	assert!(pbc.contains(&OP_OR));
	assert_ids(sql, &[1, 3]).await;
}

#[tokio::test]
async fn where_fully_pushable_not_pushes_whole_not() {
	let plan = sales_filter(Expr::Not(Box::new(id_eq(1))));
	let bound = bind_logical(&plan).await;
	let (_, disposition, scan) = find_filter(&bound);

	assert_eq!(*disposition, FilterDisposition::FullyPushed);
	let pbc = scan_pbc(scan).expect("PBC predicate");
	assert_eq!(*pbc.last().expect("last opcode"), OP_NOT);
	assert_ids_for_plan(&plan, &[2, 3, 4, 5, 6]).await;
}

#[tokio::test]
async fn where_mixed_or_branch_does_not_push_partial_branch() {
	let sql = "SELECT id FROM sales WHERE id = 3 OR status LIKE 'o%'";
	let bound = bind_sql(sql).await;
	let (_, disposition, scan) = find_filter(&bound);

	assert_eq!(*disposition, FilterDisposition::EvaluateInToron);
	assert!(scan_pbc(scan).is_none());
	assert_ids(sql, &[1, 2, 3, 4]).await;
}

#[tokio::test]
async fn where_common_pushable_conjunct_factors_out_with_minimal_residual() {
	let sql = "SELECT id FROM sales \
		WHERE (id = 3 AND status LIKE 'c%') OR (id = 3 AND status LIKE 'x%')";
	let bound = bind_sql(sql).await;
	let (predicate, disposition, scan) = find_filter(&bound);

	assert_eq!(*disposition, FilterDisposition::EvaluateInToron);
	assert!(matches!(
		predicate
			.single_root()
			.and_then(|root| predicate.node(root)),
		Some(BoundExprNode::Binary {
			op: BoundBinaryOp::Or,
			..
		})
	));
	assert!(
		!program_contains_column_named(predicate, "id"),
		"minimal residual must not repeat pushed id equality: {predicate:?}"
	);
	assert!(scan_pbc(scan).is_some());
	assert_ids(sql, &[3]).await;
}

#[tokio::test]
async fn where_common_pk_conjunct_minimal_residual_keeps_point_read_profile() {
	let sql = "SELECT id FROM sales \
		WHERE (id = 3 AND status LIKE 'c%') OR (id = 3 AND status LIKE 'x%')";
	let bound = bind_sql(sql).await;
	let (_, _, scan) = find_filter(&bound);
	let ExecutionPlan::Scan { profile_hint, .. } = scan else {
		panic!("expected scan input, got {scan:?}");
	};

	assert_eq!(*profile_hint, toron::ir::QueryProfileHint::PointRead);
}

#[tokio::test]
async fn where_multiple_common_factors_strip_from_each_or_branch() {
	let sql = "SELECT id FROM sales \
		WHERE (id = 6 AND amount = 7 AND status LIKE 'p%') \
		   OR (id = 6 AND amount = 7 AND status LIKE 'z%')";
	let bound = bind_sql(sql).await;
	let (predicate, disposition, scan) = find_filter(&bound);

	assert_eq!(*disposition, FilterDisposition::EvaluateInToron);
	assert!(matches!(
		predicate
			.single_root()
			.and_then(|root| predicate.node(root)),
		Some(BoundExprNode::Binary {
			op: BoundBinaryOp::Or,
			..
		})
	));
	for column in ["id", "amount"] {
		assert!(
			!program_contains_column_named(predicate, column),
			"minimal residual must not repeat pushed column {column:?}: {predicate:?}"
		);
	}
	assert!(scan_pbc(scan).is_some());
	assert_ids(sql, &[6]).await;
}

#[tokio::test]
async fn where_common_factor_branch_strips_to_true_removes_residual_when_safe() {
	let sql = "SELECT id FROM sales WHERE id = 3 OR (id = 3 AND status LIKE 'x%')";
	let bound = bind_sql(sql).await;
	let (predicate, disposition, scan) = find_filter(&bound);

	assert_eq!(*disposition, FilterDisposition::FullyPushed);
	assert!(
		program_contains_column_named(predicate, "id"),
		"fully pushed predicate should be the pushed id equality: {predicate:?}"
	);
	assert!(
		!program_contains_column_named(predicate, "status"),
		"branch stripped to TRUE should remove LIKE residual: {predicate:?}"
	);
	assert!(scan_pbc(scan).is_some());
	assert_ids(sql, &[3]).await;
}

#[tokio::test]
async fn where_structurally_different_equivalent_terms_do_not_factor() {
	let sql = "SELECT id FROM sales \
		WHERE (id = 3 AND status LIKE 'c%') OR (3 = id AND status LIKE 'x%')";
	let bound = bind_sql(sql).await;
	let (_, disposition, scan) = find_filter(&bound);

	assert_eq!(*disposition, FilterDisposition::EvaluateInToron);
	assert!(scan_pbc(scan).is_none());
	assert_ids(sql, &[3]).await;
}

#[tokio::test]
async fn where_not_or_minimal_residual_keeps_only_unpushed_branch_negations() {
	let plan = sales_filter(Expr::Not(Box::new(Expr::Or(
		Box::new(id_eq(3)),
		Box::new(status_like("o%")),
	))));
	let bound = bind_logical(&plan).await;
	let (predicate, disposition, scan) = find_filter(&bound);

	assert_eq!(*disposition, FilterDisposition::EvaluateInToron);
	assert!(matches!(
		predicate
				.single_root()
				.and_then(|root| predicate.node(root)),
			Some(BoundExprNode::Unary {
				op: BoundUnaryOp::Not,
				input,
			}) if matches!(predicate.node(*input), Some(BoundExprNode::Like { .. }))
	));
	let pbc = scan_pbc(scan).expect("PBC predicate");
	assert_eq!(*pbc.last().expect("last opcode"), OP_NOT);
	assert_ids_for_plan(&plan, &[6]).await;
}

#[tokio::test]
async fn where_not_and_mixed_does_not_push_negated_single_side() {
	let plan = sales_filter(Expr::Not(Box::new(Expr::And(
		Box::new(id_eq(3)),
		Box::new(id_plus_one_gt_zero()),
	))));
	let bound = bind_logical(&plan).await;
	let (_, disposition, scan) = find_filter(&bound);

	assert_eq!(*disposition, FilterDisposition::EvaluateInToron);
	assert!(scan_pbc(scan).is_none());
	assert_ids_for_plan(&plan, &[1, 2, 4, 5, 6]).await;
}

#[tokio::test]
async fn boolean_factoring_over_budget_falls_back_without_wrong_results() {
	let branches = (0..17)
		.map(|_| "(id = 3 AND status LIKE 'c%')")
		.collect::<Vec<_>>()
		.join(" OR ");
	let sql = format!("SELECT id FROM sales WHERE {branches}");
	let bound = bind_sql(&sql).await;
	let (_, disposition, scan) = find_filter(&bound);

	assert_eq!(*disposition, FilterDisposition::EvaluateInToron);
	assert!(scan_pbc(scan).is_none());
	assert_ids(&sql, &[3]).await;
}

#[test]
fn predicate_pushdown_3vl_property_random_rows_including_nulls() {
	let values = [K3::True, K3::False, K3::Null];
	for p in values {
		for r1 in values {
			for r2 in values {
				let original = k3_or(k3_and(p, r1), k3_and(p, r2));
				assert_k3_split_identity("common OR factor", original, p, k3_or(r1, r2));

				let original = k3_or(k3_and(p, r1), p);
				assert_k3_split_identity("common OR branch strips to true", original, p, K3::True);

				let original = k3_not(k3_or(p, r1));
				assert_k3_split_identity("NOT(OR) single branch", original, k3_not(p), k3_not(r1));

				let original = k3_not(k3_or(k3_or(p, r1), r2));
				let pushed = k3_and(k3_not(p), k3_not(r1));
				assert_k3_split_identity("NOT(OR) branch negations", original, pushed, k3_not(r2));

				let original = k3_and(p, r1);
				assert_k3_split_identity("top-level AND residual split", original, p, r1);
			}
		}
	}
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
fn unsupported_pattern_predicates_and_case_subquery_in_remain_unsupported() {
	for sql in [
		"SELECT id FROM sales WHERE status LIKE ANY ('o%')",
		"SELECT id FROM sales WHERE status LIKE status",
		"SELECT id FROM sales WHERE status LIKE 'o%' ESCAPE '\\'",
		"SELECT id FROM sales WHERE status SIMILAR TO 'o%'",
		"SELECT id FROM sales WHERE status RLIKE 'o.*'",
		"SELECT id FROM sales WHERE status ~~ 'o%'",
		"SELECT id FROM sales WHERE status ~~* 'o%'",
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
	bind_logical(&plan_sql(sql)).await
}

async fn bind_logical(plan: &LogicalPlan) -> ExecutionPlan {
	let catalog = catalog();
	bind(plan, &catalog, &PlannerConfig::default())
		.await
		.expect("bind logical plan")
}

async fn pbc_for(sql: &str) -> Vec<u8> {
	let bound = bind_sql(sql).await;
	let (_, disposition, scan) = find_filter(&bound);
	assert_eq!(*disposition, FilterDisposition::FullyPushed);
	scan_pbc(scan).expect("PBC predicate").to_vec()
}

async fn assert_ids(sql: &str, expected: &[i32]) {
	assert_ids_for_plan(&plan_sql(sql), expected).await;
}

async fn assert_ids_for_plan(plan: &LogicalPlan, expected: &[i32]) {
	let catalog = catalog();
	let bound = bind(plan, &catalog, &PlannerConfig::default())
		.await
		.expect("bind plan");
	let batches = Executor::new(Arc::new(InMemoryStorageScan {
		table_data: sales_batch(schema()),
	}))
	.execute_bound(&bound, None)
	.await
	.expect("execute SQL");
	assert_eq!(ids(&batches), expected);
}

fn sales_filter(predicate: Expr) -> LogicalPlan {
	LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Scan {
			table: TableId(1),
			projection: None,
		}),
		predicate,
	}
}

fn id_eq(value: i32) -> Expr {
	Expr::Eq(
		Box::new(Expr::column("id")),
		Box::new(Expr::Literal(ScalarValue::Int32(Some(value)))),
	)
}

fn status_like(pattern: &str) -> Expr {
	Expr::Like {
		expr: Box::new(Expr::column("status")),
		pattern: pattern.to_string(),
		negated: false,
		case_insensitive: false,
	}
}

fn id_plus_one_gt_zero() -> Expr {
	Expr::Gt(
		Box::new(Expr::Add(
			Box::new(Expr::column("id")),
			Box::new(Expr::Literal(ScalarValue::Int32(Some(1)))),
		)),
		Box::new(Expr::Literal(ScalarValue::Int32(Some(0)))),
	)
}

async fn assert_statuses(sql: &str, expected: &[&str]) {
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
	assert_eq!(statuses(&batches), expected);
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

fn program_contains_column_named(program: &BoundExprProgram, name: &str) -> bool {
	program
		.nodes
		.iter()
		.any(|node| matches!(node, BoundExprNode::Column(column) if column.name.0 == name))
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

fn statuses(batches: &[RecordBatch]) -> Vec<String> {
	batches
		.iter()
		.flat_map(|batch| {
			batch
				.column(0)
				.as_any()
				.downcast_ref::<StringArray>()
				.expect("status column")
				.iter()
				.map(|value| value.expect("status value").to_string())
				.collect::<Vec<_>>()
		})
		.collect()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum K3 {
	True,
	False,
	Null,
}

fn k3_not(value: K3) -> K3 {
	match value {
		K3::True => K3::False,
		K3::False => K3::True,
		K3::Null => K3::Null,
	}
}

fn k3_and(left: K3, right: K3) -> K3 {
	match (left, right) {
		(K3::False, _) | (_, K3::False) => K3::False,
		(K3::True, K3::True) => K3::True,
		_ => K3::Null,
	}
}

fn k3_or(left: K3, right: K3) -> K3 {
	match (left, right) {
		(K3::True, _) | (_, K3::True) => K3::True,
		(K3::False, K3::False) => K3::False,
		_ => K3::Null,
	}
}

fn assert_k3_split_identity(label: &str, original: K3, pushed: K3, residual: K3) {
	assert_eq!(
		k3_and(pushed, residual),
		original,
		"{label}: pushed AND residual must equal original"
	);
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
