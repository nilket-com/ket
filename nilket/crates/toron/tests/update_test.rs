use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use toron::binder::{PlannerConfig, bind};
use toron::catalog::{Catalog, MockCatalog, TableBinding, TableRef};
use toron::coordinator::QueryCoordinator;
use toron::distributed::budget::{QueryBudget, TaskBudget};
use toron::distributed::fragment::{FragmentError, FragmentPlanConfig, fragment_plan};
use toron::distributed::identity::QueryId;
use toron::error::{BindingError, QueryError, StorageError};
use toron::exec::{ExecutionContext, Executor};
use toron::ir::{
	BoundExprNode, BoundExprProgram, BoundUnaryOp, ColumnOrdinal, CreateTableRowIdentity,
	ExecutionPlan, Expr, FilterDisposition, LogicalPlan, QueryProfileHint, ScalarValue, SessionId,
	TableId,
};
use toron::sql_planner::SqlPlanner;
use toron::storage::{
	HlcBound, InMemoryTableStorage, RowIdentityProjection, StorageWrite, TenantId, WriteOutcome,
	WriteRequest,
};
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptorV1, RowIdentityPolicy, SchemaEvolutionPolicyV1,
	TableObjectId, TableSchemaDescriptor,
};

#[test]
fn planner_accepts_update_where() {
	let plan = SqlPlanner::new()
		.plan("UPDATE app.people SET note = 'new', amount = amount + 1 WHERE id = 7")
		.expect("plan update");

	let LogicalPlan::Update {
		table,
		assignments,
		predicate,
		..
	} = plan
	else {
		panic!("expected update plan");
	};

	assert_eq!(table, TableRef::with_schema("app", "people"));
	assert_eq!(assignments.len(), 2);
	assert_eq!(assignments[0].0, "note");
	assert_eq!(assignments[1].0, "amount");
	assert!(matches!(predicate, Some(Expr::Eq(_, _))));
}

#[test]
fn planner_accepts_update_returning_star() {
	let plan = SqlPlanner::new()
		.plan("UPDATE people SET note = 'new' WHERE id = 7 RETURNING *")
		.expect("plan update returning");

	let LogicalPlan::Update { returning, .. } = plan else {
		panic!("expected update plan");
	};
	assert!(returning.is_some());
}

#[test]
fn planner_accepts_update_without_where() {
	let plan = SqlPlanner::new()
		.plan("UPDATE people SET note = 'new'")
		.expect("plan update without WHERE");

	let LogicalPlan::Update {
		table,
		assignments,
		predicate,
		..
	} = plan
	else {
		panic!("expected update plan");
	};

	assert_eq!(table, TableRef::new("people"));
	assert_eq!(assignments.len(), 1);
	assert_eq!(assignments[0].0, "note");
	assert!(predicate.is_none());
}

#[test]
fn planner_accepts_update_without_where_returning() {
	let plan = SqlPlanner::new()
		.plan("UPDATE people SET note = 'new' RETURNING *")
		.expect("plan update without WHERE returning");

	let LogicalPlan::Update {
		predicate,
		returning,
		..
	} = plan
	else {
		panic!("expected update plan");
	};
	assert!(predicate.is_none());
	assert!(returning.is_some());
}

#[test]
fn planner_rejects_update_from_returning_conflict_and_parser_rejects_output_order_by_limit() {
	for (sql, diagnostic) in [
		(
			"UPDATE people SET note = 'new' FROM src WHERE id = 1",
			"FROM",
		),
		(
			"UPDATE OR REPLACE people SET note = 'new' WHERE id = 1",
			"OR",
		),
	] {
		assert_frontend_error(sql, diagnostic);
	}

	for sql in [
		"UPDATE people SET note = 'new' OUTPUT inserted.note WHERE id = 1",
		"UPDATE people SET note = 'new' WHERE id = 1 ORDER BY id",
		"UPDATE people SET note = 'new' WHERE id = 1 LIMIT 1",
	] {
		assert!(SqlPlanner::new().plan(sql).is_err(), "{sql} should reject");
	}
}

#[test]
fn planner_rejects_update_target_alias_join_and_qualified_assignment() {
	for (sql, diagnostic) in [
		("UPDATE people AS p SET note = 'new' WHERE id = 1", "alias"),
		(
			"UPDATE people JOIN src ON people.id = src.id SET note = 'new' WHERE people.id = 1",
			"joins",
		),
		(
			"UPDATE people SET people.note = 'new' WHERE id = 1",
			"qualified",
		),
	] {
		assert_frontend_error(sql, diagnostic);
	}
}

#[test]
fn planner_rejects_empty_tuple_or_duplicate_assignments() {
	assert!(
		SqlPlanner::new()
			.plan("UPDATE people SET WHERE id = 1")
			.is_err()
	);
	assert_frontend_error(
		"UPDATE people SET (note, amount) = (1, 2) WHERE id = 1",
		"tuple assignment",
	);
	assert_frontend_error(
		"UPDATE people SET note = 'a', note = 'b' WHERE id = 1",
		"duplicate UPDATE assignment target",
	);
}

#[tokio::test]
async fn binder_rejects_update_on_append_only_table() {
	let catalog = append_only_catalog().await;
	let plan = SqlPlanner::new()
		.plan("UPDATE people SET note = 'new' WHERE id = 1")
		.expect("plan update");

	let err = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect_err("append-only table should reject update");

	assert_binding_error(err, "UPDATE requires a PRIMARY KEY table");
}

#[tokio::test]
async fn update_returning_still_rejects_append_only_table() {
	let catalog = append_only_catalog().await;
	let plan = SqlPlanner::new()
		.plan("UPDATE people SET note = 'new' WHERE id = 1 RETURNING *")
		.expect("plan update returning");

	let err = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect_err("append-only table should reject update returning");

	assert_binding_error(err, "UPDATE requires a PRIMARY KEY table");
}

#[tokio::test]
async fn binder_rejects_update_without_where_on_append_only_table() {
	let catalog = append_only_catalog().await;
	let plan = SqlPlanner::new()
		.plan("UPDATE people SET note = 'new'")
		.expect("plan update");

	let err = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect_err("append-only table should reject update");

	assert_binding_error(err, "UPDATE requires a PRIMARY KEY table");
}

#[tokio::test]
async fn binder_rejects_update_on_unsupported_identity_table() {
	let mut catalog = MockCatalog::new();
	let schema = people_schema(true);
	let binding = TableBinding::new_with_primary_key_columns(
		TableId(77),
		TableObjectId::new([0x77; 16]),
		1,
		fingerprint_for_schema("weird", schema.as_ref()),
		schema,
		HashSet::from([ColumnOrdinal(0)]),
	);
	catalog
		.register_table(TableRef::new("weird"), binding)
		.expect("register unsupported identity table");
	let plan = SqlPlanner::new()
		.plan("UPDATE weird SET note = 'new' WHERE id = 1")
		.expect("plan update");

	let err = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect_err("unsupported identity should reject update");

	assert_binding_error(err, "UPDATE requires a PRIMARY KEY table");
}

#[tokio::test]
async fn binder_rejects_primary_key_assignment() {
	let (catalog, _) = pk_catalog(people_schema(true)).await;
	let plan = SqlPlanner::new()
		.plan("UPDATE people SET id = 2 WHERE id = 1")
		.expect("plan update");

	let err = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect_err("primary key update should reject");

	assert_binding_error(err, "PRIMARY KEY");
}

#[tokio::test]
async fn binder_rejects_update_without_where_primary_key_assignment() {
	let (catalog, _) = pk_catalog(people_schema(true)).await;
	let plan = SqlPlanner::new()
		.plan("UPDATE people SET id = 2")
		.expect("plan update");

	let err = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect_err("primary key update should reject");

	assert_binding_error(err, "PRIMARY KEY");
}

#[tokio::test]
async fn binder_update_pk_equality_uses_point_read_profile() {
	let (catalog, _) = pk_catalog(people_schema(true)).await;
	let bound = bind_update(&catalog, "UPDATE people SET note = 'new' WHERE id = 1").await;

	let scan = update_where_source_scan(&bound);
	let ExecutionPlan::Scan {
		row_identity,
		profile_hint,
		primary_key_columns,
		..
	} = scan
	else {
		panic!("expected update source scan, got {scan:?}");
	};
	assert_eq!(*row_identity, RowIdentityProjection::None);
	assert_eq!(*profile_hint, QueryProfileHint::PointRead);
	assert_eq!(primary_key_columns.as_slice(), &[ColumnOrdinal(0)]);
}

#[tokio::test]
async fn binder_update_non_pk_predicate_uses_scan_profile() {
	let (catalog, _) = pk_catalog(people_schema(true)).await;
	let bound = bind_update(&catalog, "UPDATE people SET note = 'new' WHERE amount > 10").await;

	let scan = update_where_source_scan(&bound);
	let ExecutionPlan::Scan { profile_hint, .. } = scan else {
		panic!("expected update source scan, got {scan:?}");
	};
	assert_eq!(*profile_hint, QueryProfileHint::Scan);
}

#[tokio::test]
async fn binder_update_splits_pbc_conjunct_from_like_residual() {
	let (catalog, _) = pk_catalog(people_schema(true)).await;
	let bound = bind_update(
		&catalog,
		"UPDATE people SET note = 'new' WHERE id = 1 AND note LIKE 'a%'",
	)
	.await;

	let ExecutionPlan::Update {
		source,
		profile_hint,
		..
	} = &bound
	else {
		panic!("expected update plan, got {bound:?}");
	};
	assert_eq!(*profile_hint, QueryProfileHint::PointRead);
	let ExecutionPlan::Project { input, .. } = source.as_ref() else {
		panic!("expected update source project, got {source:?}");
	};
	let ExecutionPlan::Filter {
		input,
		predicate,
		disposition,
	} = input.as_ref()
	else {
		panic!("expected update WHERE source filter, got {input:?}");
	};
	assert_eq!(*disposition, FilterDisposition::EvaluateInToron);
	assert!(matches!(
		predicate
			.single_root()
			.and_then(|root| predicate.node(root)),
		Some(BoundExprNode::Like { .. })
	));
	let ExecutionPlan::Scan {
		pbc_program,
		profile_hint,
		..
	} = input.as_ref()
	else {
		panic!("expected update WHERE source scan, got {input:?}");
	};
	assert!(pbc_program.is_some());
	assert_eq!(*profile_hint, QueryProfileHint::PointRead);
}

#[tokio::test]
async fn update_not_or_extraction_uses_pushed_source_scan_plus_minimal_residual() {
	let (catalog, _) = pk_catalog(people_schema(true)).await;
	let plan = LogicalPlan::Update {
		table: TableRef::new("people"),
		assignments: vec![(
			"note".to_string(),
			Expr::Literal(ScalarValue::Utf8(Some("new".to_string()))),
		)],
		predicate: Some(Expr::Not(Box::new(Expr::Or(
			Box::new(Expr::Eq(
				Box::new(Expr::column("id")),
				Box::new(Expr::Literal(ScalarValue::Int32(Some(1)))),
			)),
			Box::new(Expr::Like {
				expr: Box::new(Expr::column("note")),
				pattern: "a%".to_string(),
				negated: false,
				case_insensitive: false,
			}),
		)))),
		returning: None,
	};
	let bound = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("bind update");

	let ExecutionPlan::Update { source, .. } = &bound else {
		panic!("expected update plan, got {bound:?}");
	};
	let ExecutionPlan::Project { input, .. } = source.as_ref() else {
		panic!("expected update source project, got {source:?}");
	};
	let ExecutionPlan::Filter {
		input,
		predicate,
		disposition,
	} = input.as_ref()
	else {
		panic!("expected update WHERE source filter, got {input:?}");
	};
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
	assert!(
		!program_contains_column(predicate, ColumnOrdinal(0)),
		"minimal residual must not repeat pushed id equality: {predicate:?}"
	);
	let ExecutionPlan::Scan {
		pbc_program,
		profile_hint,
		..
	} = input.as_ref()
	else {
		panic!("expected update WHERE source scan, got {input:?}");
	};
	assert!(pbc_program.is_some());
	assert_eq!(*profile_hint, QueryProfileHint::Scan);
}

#[tokio::test]
async fn binder_update_without_where_uses_full_scan_profile_no_filter() {
	let (catalog, _) = pk_catalog(people_schema(true)).await;
	let bound = bind_update(&catalog, "UPDATE people SET note = 'new'").await;

	let scan = update_all_rows_source_scan(&bound);
	let ExecutionPlan::Scan {
		row_identity,
		profile_hint,
		primary_key_columns,
		..
	} = scan
	else {
		panic!("expected update source scan, got {scan:?}");
	};
	assert_eq!(*row_identity, RowIdentityProjection::None);
	assert_eq!(*profile_hint, QueryProfileHint::Scan);
	assert_eq!(primary_key_columns.as_slice(), &[ColumnOrdinal(0)]);
}

#[tokio::test]
async fn update_zero_matches_returns_row_count_zero_without_write() {
	let fixture = UpdateFixture::new(people_schema(true)).await;
	fixture
		.seed(people_batch(
			fixture.binding.schema.clone(),
			&[(1, 10, Some("old"))],
		))
		.await;

	let affected = fixture
		.execute("UPDATE people SET note = 'new' WHERE id = 99")
		.await
		.expect("zero-match update");

	assert_eq!(affected, 0);
	assert_eq!(fixture.writer.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn zero_row_update_returning_completes_row_count_zero_without_batch_or_write() {
	let fixture = UpdateFixture::new(people_schema(true)).await;
	fixture
		.seed(people_batch(
			fixture.binding.schema.clone(),
			&[(1, 10, Some("old"))],
		))
		.await;

	let results = fixture
		.coordinator
		.execute_query(
			&fixture
				.planner
				.plan("UPDATE people SET note = 'new' WHERE id = 99 RETURNING *")
				.expect("plan update returning"),
			None,
		)
		.await
		.expect("zero-match update returning");

	assert!(results.is_empty());
	assert_eq!(fixture.writer.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn update_writes_full_replacement_row() {
	let fixture = UpdateFixture::new(people_schema(true)).await;
	fixture
		.seed(people_batch(
			fixture.binding.schema.clone(),
			&[(1, 10, Some("old"))],
		))
		.await;

	let affected = fixture
		.execute("UPDATE people SET note = 'new' WHERE id = 1")
		.await
		.expect("update one row");

	assert_eq!(affected, 1);
	let payload = fixture.writer.single_payload();
	assert_eq!(payload.schema().as_ref(), fixture.binding.schema.as_ref());
	assert_eq!(int32_values(&payload, 0), vec![1]);
	assert_eq!(int64_values(&payload, 1), vec![10]);
	assert_eq!(string_values(&payload, 2), vec![Some("new".to_string())]);
}

#[tokio::test]
async fn update_all_rows_writes_full_table_replacement() {
	let fixture = UpdateFixture::new(people_schema(true)).await;
	fixture
		.seed(people_batch(
			fixture.binding.schema.clone(),
			&[(1, 10, Some("old")), (2, 20, Some("older"))],
		))
		.await;

	let affected = fixture
		.execute("UPDATE people SET amount = amount + 5, note = 'new'")
		.await
		.expect("update all rows");

	assert_eq!(affected, 2);
	let payload = fixture.writer.single_payload();
	assert_eq!(payload.schema().as_ref(), fixture.binding.schema.as_ref());
	assert_eq!(int32_values(&payload, 0), vec![1, 2]);
	assert_eq!(int64_values(&payload, 1), vec![15, 25]);
	assert_eq!(
		string_values(&payload, 2),
		vec![Some("new".to_string()), Some("new".to_string())]
	);
}

#[tokio::test]
async fn update_all_rows_zero_rows_table_is_no_op() {
	let fixture = UpdateFixture::new(people_schema(true)).await;
	fixture
		.seed(people_batch(fixture.binding.schema.clone(), &[]))
		.await;

	let affected = fixture
		.execute("UPDATE people SET note = 'new'")
		.await
		.expect("update empty table");

	assert_eq!(affected, 0);
	assert_eq!(fixture.writer.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn update_returning_emits_post_update_values() {
	let fixture = UpdateFixture::new(people_schema(true)).await;
	fixture
		.seed(people_batch(
			fixture.binding.schema.clone(),
			&[(1, 10, Some("old"))],
		))
		.await;

	let results = fixture
		.coordinator
		.execute_query(
			&fixture
				.planner
				.plan(
					"UPDATE people SET amount = amount + 5, note = 'new' WHERE id = 1 RETURNING id, amount, note",
				)
				.expect("plan update returning"),
			None,
		)
		.await
		.expect("update returning");

	assert_eq!(results.len(), 1);
	let batch = &results[0];
	assert_eq!(int32_values(batch, 0), vec![1]);
	assert_eq!(int64_values(batch, 1), vec![15]);
	assert_eq!(string_values(batch, 2), vec![Some("new".to_string())]);
	let payload = fixture.writer.single_payload();
	assert_eq!(int64_values(&payload, 1), vec![15]);
	assert_eq!(string_values(&payload, 2), vec![Some("new".to_string())]);
}

#[tokio::test]
async fn update_all_rows_returning_emits_post_update_rows() {
	let fixture = UpdateFixture::new(people_schema(true)).await;
	fixture
		.seed(people_batch(
			fixture.binding.schema.clone(),
			&[(1, 10, Some("old")), (2, 20, Some("older"))],
		))
		.await;

	let results = fixture
		.coordinator
		.execute_query(
			&fixture
				.planner
				.plan(
					"UPDATE people SET amount = amount + 5, note = 'new' RETURNING id, amount, note",
				)
				.expect("plan update returning"),
			None,
		)
		.await
		.expect("update returning");

	assert_eq!(results.len(), 1);
	let batch = &results[0];
	assert_eq!(int32_values(batch, 0), vec![1, 2]);
	assert_eq!(int64_values(batch, 1), vec![15, 25]);
	assert_eq!(
		string_values(batch, 2),
		vec![Some("new".to_string()), Some("new".to_string())]
	);
	let payload = fixture.writer.single_payload();
	assert_eq!(int64_values(&payload, 1), vec![15, 25]);
	assert_eq!(
		string_values(&payload, 2),
		vec![Some("new".to_string()), Some("new".to_string())]
	);
}

#[tokio::test]
async fn update_assignments_read_original_row_values() {
	let fixture = UpdateFixture::new(swap_schema()).await;
	fixture
		.seed(swap_batch(fixture.binding.schema.clone(), &[(1, 10, 20)]))
		.await;

	let affected = fixture
		.execute("UPDATE people SET a = b, b = a WHERE id = 1")
		.await
		.expect("swap update");

	assert_eq!(affected, 1);
	let payload = fixture.writer.single_payload();
	assert_eq!(int32_values(&payload, 1), vec![20]);
	assert_eq!(int32_values(&payload, 2), vec![10]);
}

#[tokio::test]
async fn update_all_rows_respects_simultaneous_assignment() {
	let fixture = UpdateFixture::new(swap_schema()).await;
	fixture
		.seed(swap_batch(
			fixture.binding.schema.clone(),
			&[(1, 10, 20), (2, 30, 40)],
		))
		.await;

	let affected = fixture
		.execute("UPDATE people SET a = b, b = a")
		.await
		.expect("swap update all rows");

	assert_eq!(affected, 2);
	let payload = fixture.writer.single_payload();
	assert_eq!(int32_values(&payload, 1), vec![20, 40]);
	assert_eq!(int32_values(&payload, 2), vec![10, 30]);
}

#[tokio::test]
async fn update_null_for_non_nullable_column_fails_before_write() {
	let fixture = UpdateFixture::new(people_schema(false)).await;
	fixture
		.seed(people_batch(
			fixture.binding.schema.clone(),
			&[(1, 10, Some("old"))],
		))
		.await;

	let err = fixture
		.execute("UPDATE people SET note = NULL WHERE id = 1")
		.await
		.expect_err("non-null target should fail before write");

	assert!(
		err.to_string()
			.contains("UPDATE produced NULL for non-nullable column: note"),
		"unexpected error: {err}"
	);
	assert_eq!(fixture.writer.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn update_all_rows_null_for_non_nullable_column_fails_before_write() {
	let fixture = UpdateFixture::new(people_schema(false)).await;
	fixture
		.seed(people_batch(
			fixture.binding.schema.clone(),
			&[(1, 10, Some("old"))],
		))
		.await;

	let err = fixture
		.execute("UPDATE people SET note = NULL")
		.await
		.expect_err("non-null target should fail before write");

	assert!(
		err.to_string()
			.contains("UPDATE produced NULL for non-nullable column: note"),
		"unexpected error: {err}"
	);
	assert_eq!(fixture.writer.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn update_type_coercion_matches_insert_select_policy() {
	let fixture = UpdateFixture::new(people_schema(true)).await;
	fixture
		.seed(people_batch(
			fixture.binding.schema.clone(),
			&[(1, 10, Some("old"))],
		))
		.await;

	let affected = fixture
		.execute("UPDATE people SET amount = 7 WHERE id = 1")
		.await
		.expect("update with numeric coercion");

	assert_eq!(affected, 1);
	let payload = fixture.writer.single_payload();
	assert_eq!(payload.schema().field(1).data_type(), &DataType::Int64);
	assert_eq!(int64_values(&payload, 1), vec![7]);
}

#[tokio::test]
async fn update_all_rows_type_coercion_matches_insert_select_policy() {
	let fixture = UpdateFixture::new(people_schema(true)).await;
	fixture
		.seed(people_batch(
			fixture.binding.schema.clone(),
			&[(1, 10, Some("old")), (2, 20, Some("older"))],
		))
		.await;

	let affected = fixture
		.execute("UPDATE people SET amount = 7")
		.await
		.expect("update all rows with numeric coercion");

	assert_eq!(affected, 2);
	let payload = fixture.writer.single_payload();
	assert_eq!(payload.schema().field(1).data_type(), &DataType::Int64);
	assert_eq!(int64_values(&payload, 1), vec![7, 7]);
}

#[tokio::test]
async fn update_descriptor_evolved_invalidates_without_retry() {
	let storage = Arc::new(InMemoryTableStorage::new());
	let (catalog, binding) = pk_catalog(people_schema(true)).await;
	storage
		.write(write_request(
			&binding,
			people_batch(binding.schema.clone(), &[(1, 10, Some("old"))]),
		))
		.await
		.expect("seed table");
	let catalog = Arc::new(catalog);
	let writer = Arc::new(CapturingWrite::descriptor_evolved(binding.legacy_table_id));
	let executor = Executor::with_context(
		storage,
		ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(1),
			TenantId(1),
			HlcBound(1),
		),
	);
	let coordinator =
		QueryCoordinator::with_writer(catalog.clone(), executor, PlannerConfig::default(), writer);
	let plan = SqlPlanner::new()
		.plan("UPDATE people SET note = 'new' WHERE id = 1")
		.expect("plan update");

	let err = coordinator
		.execute_query(&plan, None)
		.await
		.expect_err("descriptor evolution should surface");

	assert!(matches!(
		err,
		QueryError::Storage(StorageError::DescriptorEvolved { table_id }) if table_id == binding.legacy_table_id
	));
	assert!(
		catalog
			.resolve_table(&TableRef::new("people"))
			.await
			.is_err()
	);
}

#[tokio::test]
async fn fragmenter_rejects_direct_update_fragmentation() {
	let (catalog, binding) = pk_catalog(people_schema(true)).await;
	let bound = bind_update(&catalog, "UPDATE people SET note = 'new' WHERE id = 1").await;

	let err = fragment_plan(
		bound,
		FragmentPlanConfig::new(
			QueryId::from_u128(1),
			TaskBudget::from_query_budget(QueryBudget::test_default()),
		),
	)
	.expect_err("update should not fragment");

	assert_eq!(binding.primary_key_columns.len(), 1);
	assert_eq!(err, FragmentError::UpdateNotFragmentable);
}

#[tokio::test]
async fn fragmenter_rejects_direct_update_without_where_fragmentation() {
	let (catalog, binding) = pk_catalog(people_schema(true)).await;
	let bound = bind_update(&catalog, "UPDATE people SET note = 'new'").await;

	let err = fragment_plan(
		bound,
		FragmentPlanConfig::new(
			QueryId::from_u128(1),
			TaskBudget::from_query_budget(QueryBudget::test_default()),
		),
	)
	.expect_err("update should not fragment");

	assert_eq!(binding.primary_key_columns.len(), 1);
	assert_eq!(err, FragmentError::UpdateNotFragmentable);
}

async fn bind_update(catalog: &MockCatalog, sql: &str) -> ExecutionPlan {
	let plan = SqlPlanner::new().plan(sql).expect("plan update");
	bind(&plan, catalog, &PlannerConfig::default())
		.await
		.expect("bind update")
}

struct UpdateFixture {
	storage: Arc<InMemoryTableStorage>,
	binding: TableBinding,
	coordinator: QueryCoordinator,
	writer: Arc<CapturingWrite>,
	planner: SqlPlanner,
}

impl UpdateFixture {
	async fn new(schema: SchemaRef) -> Self {
		let storage = Arc::new(InMemoryTableStorage::new());
		let (catalog, binding) = pk_catalog(schema).await;
		let writer = Arc::new(CapturingWrite::default());
		let executor = Executor::with_context(
			storage.clone(),
			ExecutionContext::new(
				SessionId(1),
				QueryId::from_u128(1),
				TenantId(1),
				HlcBound(1),
			),
		);
		let coordinator = QueryCoordinator::with_writer(
			Arc::new(catalog),
			executor,
			PlannerConfig::default(),
			writer.clone(),
		);
		Self {
			storage,
			binding,
			coordinator,
			writer,
			planner: SqlPlanner::new(),
		}
	}

	async fn seed(&self, batch: RecordBatch) {
		self.storage
			.write(write_request(&self.binding, batch))
			.await
			.expect("seed table");
	}

	async fn execute(&self, sql: &str) -> Result<i64, QueryError> {
		let results = self
			.coordinator
			.execute_query(&self.planner.plan(sql).expect("plan update"), None)
			.await?;
		Ok(affected_count(&results))
	}
}

#[derive(Default)]
struct CapturingWrite {
	calls: AtomicUsize,
	payloads: Mutex<Vec<RecordBatch>>,
	fail_descriptor_evolved: Mutex<Option<TableId>>,
	inner: InMemoryTableStorage,
}

impl CapturingWrite {
	fn descriptor_evolved(table_id: TableId) -> Self {
		Self {
			fail_descriptor_evolved: Mutex::new(Some(table_id)),
			..Self::default()
		}
	}

	fn single_payload(&self) -> RecordBatch {
		let payloads = self.payloads.lock().expect("payload lock");
		assert_eq!(payloads.len(), 1);
		payloads[0].clone()
	}
}

#[async_trait]
impl StorageWrite for CapturingWrite {
	async fn write(&self, request: WriteRequest) -> Result<WriteOutcome, StorageError> {
		self.calls.fetch_add(1, Ordering::SeqCst);
		if let Some(table_id) = *self.fail_descriptor_evolved.lock().expect("fail lock") {
			return Err(StorageError::DescriptorEvolved { table_id });
		}
		let payload = request.payload.clone();
		self.payloads.lock().expect("payload lock").push(payload);
		self.inner.write(request).await
	}
}

async fn pk_catalog(schema: SchemaRef) -> (MockCatalog, TableBinding) {
	let catalog = MockCatalog::new();
	let binding = catalog
		.create_table_with_outcome(
			&TableRef::new("people"),
			schema,
			CreateTableRowIdentity::PrimaryKey {
				column: "id".to_string(),
			},
			false,
		)
		.await
		.expect("create primary-key table")
		.binding;
	(catalog, binding)
}

fn program_contains_column(program: &BoundExprProgram, ordinal: ColumnOrdinal) -> bool {
	program
		.nodes
		.iter()
		.any(|node| matches!(node, BoundExprNode::Column(column) if column.ordinal == ordinal))
}

async fn append_only_catalog() -> MockCatalog {
	let catalog = MockCatalog::new();
	catalog
		.create_table(&TableRef::new("people"), people_schema(true), false)
		.await
		.expect("create append-only table");
	catalog
}

fn update_where_source_scan(plan: &ExecutionPlan) -> &ExecutionPlan {
	let ExecutionPlan::Update { source, .. } = plan else {
		panic!("expected update plan, got {plan:?}");
	};
	let ExecutionPlan::Project { input, .. } = source.as_ref() else {
		panic!("expected update source project, got {source:?}");
	};
	let ExecutionPlan::Filter { input, .. } = input.as_ref() else {
		panic!("expected update WHERE source filter, got {input:?}");
	};
	let ExecutionPlan::Scan { .. } = input.as_ref() else {
		panic!("expected update WHERE source scan, got {input:?}");
	};
	input.as_ref()
}

fn update_all_rows_source_scan(plan: &ExecutionPlan) -> &ExecutionPlan {
	let ExecutionPlan::Update { source, .. } = plan else {
		panic!("expected update plan, got {plan:?}");
	};
	let ExecutionPlan::Project { input, .. } = source.as_ref() else {
		panic!("expected update source project, got {source:?}");
	};
	let ExecutionPlan::Scan { .. } = input.as_ref() else {
		panic!("expected no-WHERE update source scan, got {input:?}");
	};
	input.as_ref()
}

fn affected_count(results: &[RecordBatch]) -> i64 {
	assert_eq!(results.len(), 1);
	let affected = results[0]
		.column(0)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("affected column");
	assert_eq!(affected.len(), 1);
	affected.value(0)
}

fn write_request(binding: &TableBinding, payload: RecordBatch) -> WriteRequest {
	WriteRequest {
		session_id: SessionId(1),
		query_id: QueryId::from_u128(1),
		tenant_id: TenantId(1),
		table_id: binding.legacy_table_id,
		table_object_id: binding.table,
		physical_descriptor_fingerprint: binding.fingerprint,
		payload,
		profile_hint: None,
		trace_context: None,
	}
}

fn assert_frontend_error(sql: &str, diagnostic: &str) {
	let err = SqlPlanner::new().plan(sql).expect_err("SQL should reject");
	assert!(
		err.to_string().contains(diagnostic),
		"expected diagnostic {diagnostic:?}, got {err}"
	);
}

fn assert_binding_error(err: BindingError, diagnostic: &str) {
	assert!(
		err.to_string().contains(diagnostic),
		"expected diagnostic {diagnostic:?}, got {err}"
	);
}

fn people_schema(note_nullable: bool) -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int32, false),
		Field::new("amount", DataType::Int64, false),
		Field::new("note", DataType::Utf8, note_nullable),
	]))
}

fn swap_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int32, false),
		Field::new("a", DataType::Int32, false),
		Field::new("b", DataType::Int32, false),
	]))
}

fn people_batch(schema: SchemaRef, rows: &[(i32, i64, Option<&str>)]) -> RecordBatch {
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int32Array::from(
				rows.iter().map(|row| row.0).collect::<Vec<_>>(),
			)),
			Arc::new(Int64Array::from(
				rows.iter().map(|row| row.1).collect::<Vec<_>>(),
			)),
			Arc::new(StringArray::from(
				rows.iter().map(|row| row.2).collect::<Vec<_>>(),
			)),
		],
	)
	.expect("people batch")
}

fn swap_batch(schema: SchemaRef, rows: &[(i32, i32, i32)]) -> RecordBatch {
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int32Array::from(
				rows.iter().map(|row| row.0).collect::<Vec<_>>(),
			)),
			Arc::new(Int32Array::from(
				rows.iter().map(|row| row.1).collect::<Vec<_>>(),
			)),
			Arc::new(Int32Array::from(
				rows.iter().map(|row| row.2).collect::<Vec<_>>(),
			)),
		],
	)
	.expect("swap batch")
}

fn int32_values(batch: &RecordBatch, column: usize) -> Vec<i32> {
	let array = batch
		.column(column)
		.as_any()
		.downcast_ref::<Int32Array>()
		.expect("Int32 column");
	(0..array.len()).map(|index| array.value(index)).collect()
}

fn int64_values(batch: &RecordBatch, column: usize) -> Vec<i64> {
	let array = batch
		.column(column)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("Int64 column");
	(0..array.len()).map(|index| array.value(index)).collect()
}

fn string_values(batch: &RecordBatch, column: usize) -> Vec<Option<String>> {
	let array = batch
		.column(column)
		.as_any()
		.downcast_ref::<StringArray>()
		.expect("Utf8 column");
	(0..array.len())
		.map(|index| (!array.is_null(index)).then(|| array.value(index).to_string()))
		.collect()
}

fn fingerprint_for_schema(
	table: &str,
	schema: &Schema,
) -> wendelon::schema::PhysicalDescriptorFingerprint {
	TableSchemaDescriptor::new(
		TableObjectId::new([0xA5; 16]),
		1,
		"default",
		table,
		schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.expect("descriptor")
	.physical_descriptor_fingerprint
}
