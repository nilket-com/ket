use std::sync::Arc;

use arrow::array::{
	Array, Date32Array, Int32Array, Int64Array, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use toron::binder::{PlannerConfig, bind};
use toron::catalog::{
	Catalog, CreateTableOutcome, CtasProvisionOutcome, CtasProvisionRequest,
	DescriptorRefreshMetrics, InvalidatedDescriptor, MockCatalog, TableBinding, TableRef,
};
use toron::coordinator::{DistributedQueryCoordinator, QueryCoordinator};
use toron::distributed::budget::{QueryBudget, TaskBudget};
use toron::distributed::fragment::{FragmentError, FragmentPlanConfig, fragment_plan};
use toron::distributed::identity::QueryId;
use toron::distributed::worker_registry::StaticWorkerRegistry;
use toron::error::{CatalogError, ExecutionError, QueryError, StorageError};
use toron::exec::{ExecutionContext, Executor};
use toron::ir::{CreateTableRowIdentity, ExecutionPlan, LogicalPlan, QueryProfileHint, SessionId};
use toron::server::memory::MemoryManager;
use toron::server::transport::{ToronStream, ToronTransport};
use toron::sql_planner::SqlPlanner;
use toron::storage::{
	DropTableStorageRequest, HlcBound, InMemoryStorageScan, InMemoryTableStorage,
	MutationResolution, ResolvedMutationOutcome, StorageAdmin, StorageWrite, TenantId,
	WriteOutcome, WriteRequest,
};

#[test]
fn planner_accepts_create_table_as_select_surface() {
	let plan = SqlPlanner::new()
		.plan("CREATE TABLE copy AS SELECT id, name FROM people")
		.expect("plan CTAS");

	let LogicalPlan::CreateTableAsSelect {
		table,
		intent_request,
		if_not_exists,
		or_replace,
		source,
		..
	} = plan
	else {
		panic!("expected CTAS logical plan");
	};

	assert_eq!(table, TableRef::new("copy"));
	assert_eq!(
		intent_request,
		"CREATE TABLE copy AS SELECT id, name FROM people"
	);
	assert!(!if_not_exists);
	assert!(!or_replace);
	assert!(matches!(*source, LogicalPlan::Project { .. }));
}

#[test]
fn planner_accepts_create_or_replace_table_as_select_surface() {
	let plan = SqlPlanner::new()
		.plan("CREATE OR REPLACE TABLE copy AS SELECT id, name FROM people")
		.expect("plan CTAS replace");

	let LogicalPlan::CreateTableAsSelect {
		table,
		intent_request,
		if_not_exists,
		or_replace,
		source,
	} = plan
	else {
		panic!("expected CTAS logical plan");
	};

	assert_eq!(table, TableRef::new("copy"));
	assert_eq!(
		intent_request,
		"CREATE OR REPLACE TABLE copy AS SELECT id, name FROM people"
	);
	assert!(!if_not_exists);
	assert!(or_replace);
	assert!(matches!(*source, LogicalPlan::Project { .. }));
}

#[tokio::test]
async fn adr548_intent_fingerprint_includes_the_canonical_ddl_mode() {
	let catalog = people_catalog().await;
	let planner = SqlPlanner::new();
	let create = planner
		.plan("CREATE TABLE copy AS SELECT id, name FROM people")
		.expect("plan create CTAS");
	let replace = planner
		.plan("CREATE OR REPLACE TABLE copy AS SELECT id, name FROM people")
		.expect("plan replacement CTAS");
	let ExecutionPlan::CreateTableAsSelect {
		intent_fingerprint: create_fingerprint,
		..
	} = bind(&create, &catalog, &PlannerConfig::default())
		.await
		.expect("bind create CTAS")
	else {
		panic!("expected create CTAS execution plan");
	};
	let ExecutionPlan::CreateTableAsSelect {
		intent_fingerprint: replace_fingerprint,
		..
	} = bind(&replace, &catalog, &PlannerConfig::default())
		.await
		.expect("bind replacement CTAS")
	else {
		panic!("expected replacement CTAS execution plan");
	};
	assert_ne!(
		create_fingerprint, replace_fingerprint,
		"one caller query id must not resume a saga authored under another DDL mode"
	);
}

#[test]
fn planner_rejects_unsupported_ctas_shapes() {
	for (sql, diagnostic) in [
		(
			"CREATE TABLE copy (id INT) AS SELECT id FROM people",
			"explicit column lists",
		),
		("CREATE TABLE copy LIKE people", "LIKE"),
		("CREATE TABLE copy CLONE people", "CLONE"),
	] {
		assert_frontend_error(sql, diagnostic);
	}
}

#[test]
fn planner_accepts_create_table_if_not_exists_as_select() {
	let plan = SqlPlanner::new()
		.plan("CREATE TABLE IF NOT EXISTS copy AS SELECT id FROM people")
		.expect("plan CTAS IF NOT EXISTS");

	let LogicalPlan::CreateTableAsSelect {
		table,
		if_not_exists,
		or_replace,
		source,
		..
	} = plan
	else {
		panic!("expected CTAS logical plan");
	};

	assert_eq!(table, TableRef::new("copy"));
	assert!(if_not_exists);
	assert!(!or_replace);
	assert!(matches!(*source, LogicalPlan::Project { .. }));
}

#[test]
fn ctas_if_not_exists_is_accepted() {
	SqlPlanner::new()
		.plan("CREATE TABLE IF NOT EXISTS copy AS SELECT id FROM people")
		.expect("CTAS IF NOT EXISTS should now plan");
}

#[test]
fn ctas_or_replace_rejects_if_not_exists_combo() {
	assert_frontend_error(
		"CREATE OR REPLACE TABLE IF NOT EXISTS copy AS SELECT id FROM people",
		"CREATE TABLE cannot combine OR REPLACE and IF NOT EXISTS",
	);
}

#[test]
fn planner_rejects_create_or_replace_if_not_exists_ctas() {
	assert_frontend_error(
		"CREATE OR REPLACE TABLE IF NOT EXISTS copy AS SELECT id FROM people",
		"CREATE TABLE cannot combine OR REPLACE and IF NOT EXISTS",
	);
}

#[test]
fn planner_still_rejects_ctas_explicit_column_list() {
	assert_frontend_error(
		"CREATE TABLE copy (id INT) AS SELECT id FROM people",
		"explicit column lists",
	);
}

#[tokio::test]
async fn binder_infers_ctas_schema_without_target_identity() {
	let catalog = people_catalog().await;
	let plan = SqlPlanner::new()
		.plan("CREATE TABLE copy AS SELECT id, name FROM people")
		.expect("plan CTAS");

	let bound = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("bind CTAS");
	let ExecutionPlan::CreateTableAsSelect {
		table,
		schema,
		if_not_exists,
		or_replace,
		source,
		..
	} = bound
	else {
		panic!("expected CTAS execution plan");
	};

	assert_eq!(table, TableRef::new("copy"));
	assert!(!if_not_exists);
	assert!(!or_replace);
	assert_eq!(schema.fields().len(), 2);
	assert_field(&schema, 0, "id", &DataType::Int32, false);
	assert_field(&schema, 1, "name", &DataType::Utf8, true);
	assert!(matches!(*source, ExecutionPlan::Project { .. }));
}

#[tokio::test]
async fn ctas_if_not_exists_binder_surfaces_source_errors_when_target_exists() {
	let (_catalog, _storage, coordinator, planner) = coordinator();
	coordinator
		.execute_query(
			&planner
				.plan("CREATE TABLE copy (id INT NOT NULL)")
				.expect("plan target"),
			None,
		)
		.await
		.expect("create target");

	let err = coordinator
		.execute_query(
			&planner
				.plan("CREATE TABLE IF NOT EXISTS copy AS SELECT id FROM missing")
				.expect("plan CTAS IF NOT EXISTS"),
			None,
		)
		.await
		.expect_err("source bind error should surface");

	assert!(
		err.to_string().contains("table not found"),
		"unexpected error: {err}"
	);
}

#[tokio::test]
async fn ctas_if_not_exists_compatible_existing_target_skips_source_execution() {
	let (catalog, _storage, coordinator, planner) = coordinator();
	catalog
		.create_table(
			&TableRef::new("people"),
			schema(&[
				Field::new("id", DataType::Int32, false),
				Field::new("name", DataType::Utf8, true),
			]),
			false,
		)
		.await
		.expect("register source descriptor only");
	coordinator
		.execute_query(
			&planner
				.plan("CREATE TABLE copy (id INT NOT NULL, name TEXT)")
				.expect("plan target"),
			None,
		)
		.await
		.expect("create compatible target");

	let results = coordinator
		.execute_query(
			&planner
				.plan("CREATE TABLE IF NOT EXISTS copy AS SELECT id, name FROM people")
				.expect("plan CTAS IF NOT EXISTS"),
			None,
		)
		.await
		.expect("compatible target should skip source execution");

	assert!(results.is_empty());
}

#[tokio::test]
async fn ctas_if_not_exists_incompatible_existing_target_returns_schema_mismatch() {
	let (catalog, _storage, coordinator, planner) = coordinator();
	catalog
		.create_table(
			&TableRef::new("people"),
			schema(&[
				Field::new("id", DataType::Int32, false),
				Field::new("name", DataType::Utf8, true),
			]),
			false,
		)
		.await
		.expect("register source descriptor only");
	coordinator
		.execute_query(
			&planner
				.plan("CREATE TABLE copy (id BIGINT NOT NULL)")
				.expect("plan target"),
			None,
		)
		.await
		.expect("create incompatible target");

	let err = coordinator
		.execute_query(
			&planner
				.plan("CREATE TABLE IF NOT EXISTS copy AS SELECT id, name FROM people")
				.expect("plan CTAS IF NOT EXISTS"),
			None,
		)
		.await
		.expect_err("incompatible target should fail");

	assert!(
		matches!(
			err,
			QueryError::Catalog(CatalogError::SchemaMismatch { .. })
		),
		"unexpected error: {err}"
	);
}

#[tokio::test]
async fn create_or_replace_table_as_select_creates_when_absent() {
	let (_catalog, _storage, coordinator, planner) = coordinator();

	for sql in [
		"CREATE TABLE people (id INT NOT NULL, name TEXT)",
		"INSERT INTO people VALUES (1, 'Alice'), (2, 'Bob')",
		"CREATE OR REPLACE TABLE copy AS SELECT id, name FROM people",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan SQL"), None)
			.await
			.expect(sql);
	}

	let batch = concat_query(
		&coordinator,
		&planner,
		"SELECT id, name FROM copy ORDER BY id",
	)
	.await;
	assert_eq!(int32_column(&batch, 0).values(), &[1, 2]);
	assert_eq!(string_column(&batch, 1).value(0), "Alice");
	assert_eq!(string_column(&batch, 1).value(1), "Bob");
}

#[tokio::test]
async fn create_or_replace_table_as_select_publishes_then_retires_the_old_table() {
	let (catalog, storage, coordinator, planner) = coordinator();

	for sql in [
		"CREATE TABLE people (id INT NOT NULL, name TEXT)",
		"INSERT INTO people VALUES (1, 'Alice'), (2, 'Bob')",
		"CREATE TABLE copy (old_id INT NOT NULL)",
		"INSERT INTO copy VALUES (99)",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan setup SQL"), None)
			.await
			.expect(sql);
	}
	let before = catalog
		.resolve_table(&TableRef::new("copy"))
		.await
		.expect("resolve copy");

	coordinator
		.execute_query(
			&planner
				.plan("CREATE OR REPLACE TABLE copy AS SELECT name FROM people")
				.expect("plan replace CTAS"),
			None,
		)
		.await
		.expect("staged replacement");

	let after = catalog
		.resolve_table(&TableRef::new("copy"))
		.await
		.expect("resolve copy after refusal");
	assert_ne!(before.table, after.table);
	assert_eq!(storage.table_count(), 2);
	assert!(matches!(
		catalog.resolve_table_id(before.local_table_id).await,
		Err(CatalogError::TableNotFound { .. })
	));
	let batch = concat_query(
		&coordinator,
		&planner,
		"SELECT name FROM copy ORDER BY name",
	)
	.await;
	assert_eq!(batch.num_rows(), 2);
	assert_eq!(string_column(&batch, 0).value(0), "Alice");
	assert_eq!(string_column(&batch, 0).value(1), "Bob");
}

#[tokio::test]
async fn ctas_or_replace_source_failure_leaves_the_old_binding_live() {
	let (catalog, _storage, coordinator, planner) = coordinator();

	for sql in [
		"CREATE TABLE target (id INT NOT NULL)",
		"INSERT INTO target VALUES (7)",
		"CREATE TABLE source_without_storage (id INT NOT NULL)",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan setup SQL"), None)
			.await
			.expect(sql);
	}
	let before = catalog
		.resolve_table(&TableRef::new("target"))
		.await
		.expect("resolve target before replace");

	let err = coordinator
		.execute_query(
			&planner
				.plan("CREATE OR REPLACE TABLE target AS SELECT id FROM source_without_storage")
				.expect("plan replace CTAS"),
			None,
		)
		.await
		.expect_err("source without storage must fail before provisioning");
	let rendered = err.to_string();
	assert!(
		rendered.contains("table not found"),
		"expected the source failure, got: {rendered}"
	);

	let after = catalog
		.resolve_table(&TableRef::new("target"))
		.await
		.expect("target remains after source failure");
	assert_eq!(before.local_table_id, after.local_table_id);
	assert_eq!(before.table, after.table);
	assert_eq!(before.fingerprint, after.fingerprint);

	let batch = concat_query(&coordinator, &planner, "SELECT id FROM target").await;
	assert_eq!(int32_column(&batch, 0).values(), &[7]);
}

#[tokio::test]
async fn binder_rejects_invalid_ctas_inferred_schema() {
	for (sql, diagnostic) in [
		(
			"CREATE TABLE dup AS SELECT id, id FROM people",
			"duplicate CTAS output column",
		),
		(
			"CREATE TABLE reserved AS SELECT id AS __row_hlc__ FROM people",
			"reserved for storage metadata",
		),
		(
			"CREATE TABLE erased AS SELECT NULL AS erased FROM people",
			"unsupported CTAS output column type",
		),
	] {
		let catalog = people_catalog().await;
		let plan = SqlPlanner::new().plan(sql).expect("plan CTAS");
		let err = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect_err("invalid CTAS schema should reject");
		assert!(
			err.to_string().contains(diagnostic),
			"expected diagnostic {diagnostic:?}, got {err}"
		);
	}

	let catalog = MockCatalog::new();
	catalog
		.create_table(
			&TableRef::new("dated"),
			schema(&[Field::new("d", DataType::Date32, false)]),
			false,
		)
		.await
		.expect("create Date32 source");
	let plan = SqlPlanner::new()
		.plan("CREATE TABLE dated_copy AS SELECT d FROM dated")
		.expect("plan Date32 CTAS");
	let bound = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("Date32 CTAS should bind");
	let ExecutionPlan::CreateTableAsSelect { schema, .. } = bound else {
		panic!("expected CTAS execution plan");
	};
	assert_field(&schema, 0, "d", &DataType::Date32, false);
}

#[tokio::test]
async fn query_coordinator_executes_projection_ctas() {
	let (catalog, _storage, coordinator, planner) = coordinator();

	for sql in [
		"CREATE TABLE people (id INT NOT NULL, name TEXT)",
		"INSERT INTO people VALUES (1, 'Alice'), (2, 'Bob')",
		"CREATE TABLE copy AS SELECT id, name FROM people",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan SQL"), None)
			.await
			.expect(sql);
	}

	let binding = catalog
		.resolve_table(&TableRef::new("copy"))
		.await
		.expect("resolve CTAS table");
	assert_field(&binding.schema, 0, "id", &DataType::Int32, false);
	assert_field(&binding.schema, 1, "name", &DataType::Utf8, true);

	let batch = concat_query(
		&coordinator,
		&planner,
		"SELECT id, name FROM copy ORDER BY id",
	)
	.await;
	let ids = int32_column(&batch, 0);
	let names = string_column(&batch, 1);
	assert_eq!(ids.values(), &[1, 2]);
	assert_eq!(names.value(0), "Alice");
	assert_eq!(names.value(1), "Bob");
}

#[tokio::test]
async fn query_coordinator_executes_date32_projection_ctas() {
	let (catalog, _storage, coordinator, planner) = coordinator();

	for sql in [
		"CREATE TABLE dated (id INT NOT NULL, d DATE)",
		"INSERT INTO dated VALUES (1, DATE '2024-01-01'), (2, Date32 '2024-02-29')",
		"CREATE TABLE dated_copy AS SELECT id, d FROM dated",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan SQL"), None)
			.await
			.expect(sql);
	}

	let binding = catalog
		.resolve_table(&TableRef::new("dated_copy"))
		.await
		.expect("resolve Date32 CTAS table");
	assert_field(&binding.schema, 0, "id", &DataType::Int32, false);
	assert_field(&binding.schema, 1, "d", &DataType::Date32, true);

	let batch = concat_query(
		&coordinator,
		&planner,
		"SELECT id, d FROM dated_copy ORDER BY id",
	)
	.await;
	let dates = date32_column(&batch, 1);
	assert_eq!(int32_column(&batch, 0).values(), &[1, 2]);
	assert_eq!(dates.values(), &[19_723, 19_782]);
}

#[tokio::test]
async fn query_coordinator_executes_timestamptz_projection_ctas() {
	let (catalog, _storage, coordinator, planner) = coordinator();

	for sql in [
		"CREATE TABLE events (id INT NOT NULL, ts TIMESTAMPTZ)",
		"INSERT INTO events VALUES (1, TIMESTAMPTZ '2024-01-01T00:00:00Z'), (2, TIMESTAMPTZ '2024-01-01T06:00:00Z')",
		"CREATE TABLE event_copy AS SELECT id, ts FROM events",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan SQL"), None)
			.await
			.expect(sql);
	}

	let binding = catalog
		.resolve_table(&TableRef::new("event_copy"))
		.await
		.expect("resolve TIMESTAMPTZ CTAS table");
	assert_field(&binding.schema, 0, "id", &DataType::Int32, false);
	assert_field(
		&binding.schema,
		1,
		"ts",
		&DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
		true,
	);

	let batch = concat_query(
		&coordinator,
		&planner,
		"SELECT id, ts FROM event_copy ORDER BY id",
	)
	.await;
	let timestamps = timestamp_ns_column(&batch, 1);
	assert_eq!(int32_column(&batch, 0).values(), &[1, 2]);
	assert_eq!(
		timestamps.values(),
		&[1_704_067_200_000_000_000, 1_704_088_800_000_000_000]
	);
	assert_eq!(
		timestamps.data_type(),
		&DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
	);
}

#[tokio::test]
async fn query_coordinator_executes_cast_null_ctas_with_typed_schema() {
	let (catalog, _storage, coordinator, planner) = coordinator();

	for sql in [
		"CREATE TABLE people (id INT NOT NULL, name TEXT)",
		"INSERT INTO people VALUES (1, 'Alice'), (2, 'Bob')",
		"CREATE TABLE nulls AS SELECT CAST(NULL AS BIGINT) AS maybe_id FROM people",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan SQL"), None)
			.await
			.expect(sql);
	}

	let binding = catalog
		.resolve_table(&TableRef::new("nulls"))
		.await
		.expect("resolve CTAS table");
	assert_field(&binding.schema, 0, "maybe_id", &DataType::Int64, true);

	let batch = concat_query(&coordinator, &planner, "SELECT maybe_id FROM nulls").await;
	assert_eq!(batch.column(0).data_type(), &DataType::Int64);
	assert_eq!(batch.column(0).null_count(), 2);
	assert_eq!(
		int64_column(&batch, 0).iter().collect::<Vec<_>>(),
		vec![None, None]
	);
}

#[tokio::test]
async fn ctas_if_not_exists_absent_target_creates_and_writes_like_current_ctas() {
	let (catalog, _storage, coordinator, planner) = coordinator();

	for sql in [
		"CREATE TABLE people (id INT NOT NULL, name TEXT)",
		"INSERT INTO people VALUES (1, 'Alice'), (2, 'Bob')",
		"CREATE TABLE IF NOT EXISTS copy AS SELECT id, name FROM people",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan SQL"), None)
			.await
			.expect(sql);
	}

	let binding = catalog
		.resolve_table(&TableRef::new("copy"))
		.await
		.expect("resolve CTAS table");
	assert_field(&binding.schema, 0, "id", &DataType::Int32, false);
	assert_field(&binding.schema, 1, "name", &DataType::Utf8, true);

	let batch = concat_query(
		&coordinator,
		&planner,
		"SELECT id, name FROM copy ORDER BY id",
	)
	.await;
	assert_eq!(int32_column(&batch, 0).values(), &[1, 2]);
	assert_eq!(string_column(&batch, 1).value(0), "Alice");
	assert_eq!(string_column(&batch, 1).value(1), "Bob");
}

#[tokio::test]
async fn or_replace_losing_a_create_race_does_not_destroy_the_winner() {
	// A preflight absence check is not proof the target is still absent when the
	// mutation runs. The winner below is real: pre-created, populated, and durable
	// before the statement starts. The wrapper hides it from the statement's FIRST
	// local resolution only, which is exactly what losing a create race looks like
	// from inside the statement.
	//
	// Fixed path: the staged provision compares the captured absent binding with
	// the now-visible winner, nothing is dropped, and the statement loses before
	// a provisional object or row write exists.
	//
	// Withdrawn path, measured by restoring it: the drop helper performed a SECOND
	// resolution, saw the winner, and destroyed it — and the statement then
	// returned `Ok`. Losing a create race silently replaced another actor's
	// committed table with no error at all. This control fails there on
	// `expect_err`, before it even reaches the survival assertions.
	let base = Arc::new(MockCatalog::new());
	let storage = Arc::new(InMemoryTableStorage::new());
	let admin = Arc::new(CountingStorageAdmin::new(storage.clone()));
	let planner = SqlPlanner::new();

	// Build the winner and the source through the unwrapped catalog.
	let setup = QueryCoordinator::with_writer(
		base.clone(),
		Executor::new(storage.clone()),
		PlannerConfig::default(),
		storage.clone(),
	)
	.with_storage_admin(admin.clone());
	for sql in [
		"CREATE TABLE people (id INT NOT NULL)",
		"INSERT INTO people VALUES (1)",
		"CREATE TABLE copy (winner_id INT NOT NULL)",
		"INSERT INTO copy VALUES (99)",
	] {
		setup
			.execute_query(&planner.plan(sql).expect("plan setup"), None)
			.await
			.expect(sql);
	}
	let winner_before = base
		.resolve_table(&TableRef::new("copy"))
		.await
		.expect("winner exists before the statement");
	let inventory_before = storage.table_count();
	let winner_rows_before = concat_query(&setup, &planner, "SELECT winner_id FROM copy").await;
	assert_eq!(int32_column(&winner_rows_before, 0).values(), &[99]);
	assert_eq!(admin.drop_count(), 0, "setup must not drop anything");

	// Run the statement through a catalog that hides `copy` from its first lookup.
	let hiding = Arc::new(HidesTargetOnFirstResolve::new(
		base.clone(),
		TableRef::new("copy"),
	));
	let racer = QueryCoordinator::with_writer(
		hiding.clone(),
		Executor::new(storage.clone()),
		PlannerConfig::default(),
		storage.clone(),
	)
	.with_storage_admin(admin.clone());

	let error = racer
		.execute_query(
			&planner
				.plan("CREATE OR REPLACE TABLE copy AS SELECT id FROM people")
				.expect("plan replace CTAS"),
			None,
		)
		.await
		.expect_err("losing the create race must fail the statement");
	assert!(
		hiding.hid_once(),
		"the statement must have crossed the synthetic race window"
	);

	// The winner is untouched: same binding and object id, same rows, same
	// storage inventory, and no storage-admin drop ran.
	let winner_after = base
		.resolve_table(&TableRef::new("copy"))
		.await
		.unwrap_or_else(|error| panic!("the winner must survive, got: {error}"));
	assert_eq!(winner_before.table, winner_after.table, "object id changed");
	assert_eq!(
		winner_before.local_table_id, winner_after.local_table_id,
		"local binding changed"
	);
	assert_eq!(
		winner_before.fingerprint, winner_after.fingerprint,
		"descriptor fingerprint changed"
	);
	assert_eq!(
		storage.table_count(),
		inventory_before,
		"storage inventory changed"
	);
	assert_eq!(
		admin.drop_count(),
		0,
		"no storage-admin drop may run on an OR REPLACE path: {error}"
	);
	let winner_rows_after = concat_query(&setup, &planner, "SELECT winner_id FROM copy").await;
	assert_eq!(int32_column(&winner_rows_after, 0).values(), &[99]);

	// The counter is live: an explicit DROP in this same state increments it, so
	// the zero above is evidence that no drop ran rather than an unobservable one.
	setup
		.execute_query(&planner.plan("DROP TABLE copy").expect("plan drop"), None)
		.await
		.expect("explicit drop of the winner");
	assert_eq!(admin.drop_count(), 1, "the drop counter must be observable");
}

#[tokio::test]
async fn ctas_if_not_exists_compatible_raced_in_target_succeeds_without_writing() {
	let base = Arc::new(MockCatalog::new());
	let racing_catalog = Arc::new(RacingCatalog::new(
		base,
		TableRef::new("copy"),
		schema(&[
			Field::new("id", DataType::Int32, false),
			Field::new("name", DataType::Utf8, true),
		]),
	));
	let storage = Arc::new(InMemoryTableStorage::new());
	let setup = QueryCoordinator::with_writer(
		racing_catalog.clone(),
		Executor::new(storage.clone()),
		PlannerConfig::default(),
		storage.clone(),
	)
	.with_storage_admin(storage.clone());
	let planner = SqlPlanner::new();
	for sql in [
		"CREATE TABLE people (id INT NOT NULL, name TEXT)",
		"INSERT INTO people VALUES (1, 'Alice')",
	] {
		setup
			.execute_query(&planner.plan(sql).expect("plan setup"), None)
			.await
			.expect(sql);
	}
	let no_write = QueryCoordinator::with_writer(
		racing_catalog.clone(),
		Executor::new(storage),
		PlannerConfig::default(),
		Arc::new(FailingWrite),
	);

	let results = no_write
		.execute_query(
			&planner
				.plan("CREATE TABLE IF NOT EXISTS copy AS SELECT id, name FROM people")
				.expect("plan raced CTAS"),
			None,
		)
		.await
		.expect("compatible raced-in table should skip write");

	assert!(results.is_empty());
	let binding = racing_catalog
		.resolve_table(&TableRef::new("copy"))
		.await
		.expect("raced target registered");
	assert_field(&binding.schema, 0, "id", &DataType::Int32, false);
	assert_field(&binding.schema, 1, "name", &DataType::Utf8, true);
}

#[tokio::test]
async fn ctas_if_not_exists_incompatible_raced_in_target_returns_schema_mismatch() {
	let base = Arc::new(MockCatalog::new());
	let racing_catalog = Arc::new(RacingCatalog::new(
		base,
		TableRef::new("copy"),
		schema(&[Field::new("id", DataType::Int64, false)]),
	));
	let storage = Arc::new(InMemoryTableStorage::new());
	let coordinator = QueryCoordinator::with_writer(
		racing_catalog,
		Executor::new(storage.clone()),
		PlannerConfig::default(),
		storage,
	);
	let planner = SqlPlanner::new();
	for sql in [
		"CREATE TABLE people (id INT NOT NULL, name TEXT)",
		"INSERT INTO people VALUES (1, 'Alice')",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan setup"), None)
			.await
			.expect(sql);
	}

	let err = coordinator
		.execute_query(
			&planner
				.plan("CREATE TABLE IF NOT EXISTS copy AS SELECT id, name FROM people")
				.expect("plan raced CTAS"),
			None,
		)
		.await
		.expect_err("incompatible raced-in table should fail");

	assert!(
		matches!(
			err,
			QueryError::Catalog(CatalogError::SchemaMismatch { .. })
		),
		"unexpected error: {err}"
	);
}

#[tokio::test]
async fn query_coordinator_executes_aggregate_and_join_ctas() {
	let (_catalog, _storage, coordinator, planner) = coordinator();

	for sql in [
		"CREATE TABLE sales (region TEXT, amount BIGINT NOT NULL)",
		"INSERT INTO sales VALUES ('east', 100), ('east', 200)",
		"CREATE TABLE totals AS SELECT region, SUM(amount) AS total FROM sales GROUP BY region",
		"CREATE TABLE left_table (id INT NOT NULL, label TEXT)",
		"CREATE TABLE right_table (left_id INT NOT NULL, score BIGINT NOT NULL)",
		"INSERT INTO left_table VALUES (1, 'a'), (2, 'b')",
		"INSERT INTO right_table VALUES (1, 100), (2, 200)",
		"CREATE TABLE joined AS SELECT left_table.id AS id, label, score FROM left_table INNER JOIN right_table ON left_table.id = right_table.left_id",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan SQL"), None)
			.await
			.expect(sql);
	}

	let totals = concat_query(&coordinator, &planner, "SELECT region, total FROM totals").await;
	assert_eq!(string_column(&totals, 0).value(0), "east");
	assert_eq!(int64_column(&totals, 1).value(0), 300);

	let joined = concat_query(
		&coordinator,
		&planner,
		"SELECT id, label, score FROM joined ORDER BY id",
	)
	.await;
	assert_eq!(int32_column(&joined, 0).values(), &[1, 2]);
	assert_eq!(string_column(&joined, 1).value(0), "a");
	assert_eq!(string_column(&joined, 1).value(1), "b");
	assert_eq!(int64_column(&joined, 2).values(), &[100, 200]);
}

#[tokio::test]
async fn query_coordinator_empty_source_ctas_initializes_queryable_table() {
	let (_catalog, _storage, coordinator, planner) = coordinator();

	for sql in [
		"CREATE TABLE src (id INT NOT NULL)",
		"INSERT INTO src VALUES (1)",
		"CREATE TABLE empty_copy AS SELECT id FROM src WHERE id > 100",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan SQL"), None)
			.await
			.expect(sql);
	}

	let batch = concat_query(&coordinator, &planner, "SELECT id FROM empty_copy").await;
	assert_eq!(batch.num_rows(), 0);
	assert_eq!(batch.num_columns(), 1);
}

#[tokio::test]
async fn query_coordinator_ctas_failures_before_catalog_mutation_do_not_create_target() {
	for (sql, diagnostic, target) in [
		(
			"CREATE TABLE ghost AS SELECT missing FROM src",
			"unknown column",
			"ghost",
		),
		(
			"CREATE TABLE dup AS SELECT id, id FROM src",
			"duplicate CTAS output column",
			"dup",
		),
		(
			"CREATE TABLE reserved AS SELECT id AS __row_hlc__ FROM src",
			"reserved for storage metadata",
			"reserved",
		),
	] {
		let (catalog, _storage, coordinator, planner) = coordinator();
		coordinator
			.execute_query(
				&planner
					.plan("CREATE TABLE src (id INT NOT NULL)")
					.expect("plan create src"),
				None,
			)
			.await
			.expect("create src");

		let err = coordinator
			.execute_query(&planner.plan(sql).expect("plan CTAS"), None)
			.await
			.expect_err("invalid CTAS should reject");
		assert!(
			err.to_string().contains(diagnostic),
			"expected diagnostic {diagnostic:?}, got {err}"
		);
		assert!(
			catalog.resolve_table(&TableRef::new(target)).await.is_err(),
			"target table {target} should not be created"
		);
	}
}

#[tokio::test]
async fn query_coordinator_ctas_write_failure_leaves_no_public_catalog_entry() {
	let catalog = Arc::new(MockCatalog::new());
	let storage = Arc::new(InMemoryTableStorage::new());
	let planner = SqlPlanner::new();
	let setup = QueryCoordinator::with_writer(
		catalog.clone(),
		Executor::new(storage.clone()),
		PlannerConfig::default(),
		storage.clone(),
	);

	for sql in [
		"CREATE TABLE src (id INT NOT NULL)",
		"INSERT INTO src VALUES (1)",
	] {
		setup
			.execute_query(&planner.plan(sql).expect("plan setup SQL"), None)
			.await
			.expect(sql);
	}

	let failing = QueryCoordinator::with_writer(
		catalog.clone(),
		Executor::new(storage.clone()),
		PlannerConfig::default(),
		Arc::new(FailingWrite),
	);
	let err = failing
		.execute_query(
			&planner
				.plan("CREATE TABLE failed_copy AS SELECT id FROM src")
				.expect("plan CTAS"),
			None,
		)
		.await
		.expect_err("CTAS write should fail");
	assert!(
		err.to_string().contains("forced CTAS write failure"),
		"unexpected error: {err}"
	);

	assert!(matches!(
		catalog.resolve_table(&TableRef::new("failed_copy")).await,
		Err(CatalogError::TableNotFound { .. })
	));
}

#[tokio::test]
async fn adr548_known_not_committed_rows_reclaim_with_the_row_phase_cause() {
	let catalog = Arc::new(MockCatalog::new());
	let storage = Arc::new(InMemoryTableStorage::new());
	let planner = SqlPlanner::new();
	let setup = QueryCoordinator::with_writer(
		catalog.clone(),
		Executor::new(storage.clone()),
		PlannerConfig::default(),
		storage.clone(),
	);
	for sql in [
		"CREATE TABLE src (id INT NOT NULL)",
		"INSERT INTO src VALUES (1)",
	] {
		setup
			.execute_query(&planner.plan(sql).expect("plan setup SQL"), None)
			.await
			.expect(sql);
	}
	let coordinator = QueryCoordinator::with_writer(
		catalog.clone(),
		Executor::new(storage.clone()),
		PlannerConfig::default(),
		Arc::new(KnownNotCommittedCtasWrite),
	)
	.with_storage_admin(storage.clone());
	let plan = planner
		.plan("CREATE TABLE rejected_copy AS SELECT id FROM src")
		.expect("plan CTAS");
	let query_id = QueryId::new();
	let error = coordinator
		.execute_mutation_with_query_id(&plan, query_id)
		.await
		.expect_err("known-not-committed write returns its row error");
	assert!(error.to_string().contains("known-not-committed CTAS write"));
	let saga = catalog
		.resolve_ctas_saga(wendelon_client::CtasOperationId::new(
			*query_id.get().as_bytes(),
		))
		.await
		.expect("resolve saga")
		.expect("saga remains attributable");
	assert_eq!(saga.phase, wendelon_client::CtasSagaPhase::Reclaimed);
	assert_eq!(
		saga.terminal_cause,
		Some(wendelon_client::CtasTerminalCause::RowKnownNotCommitted)
	);
	assert!(matches!(
		catalog.resolve_table(&TableRef::new("rejected_copy")).await,
		Err(CatalogError::TableNotFound { .. })
	));
}

#[tokio::test]
async fn adr548_rows_committed_then_lost_publication_cas_reclaims_as_conflict() {
	let base = Arc::new(MockCatalog::new());
	let storage = Arc::new(InMemoryTableStorage::new());
	let admin = Arc::new(CountingStorageAdmin::new(storage.clone()));
	let planner = SqlPlanner::new();
	let setup = QueryCoordinator::with_writer(
		base.clone(),
		Executor::new(storage.clone()),
		PlannerConfig::default(),
		storage.clone(),
	)
	.with_storage_admin(admin.clone());
	for sql in [
		"CREATE TABLE src (id INT NOT NULL)",
		"INSERT INTO src VALUES (1), (2)",
	] {
		setup
			.execute_query(&planner.plan(sql).expect("plan setup SQL"), None)
			.await
			.expect(sql);
	}
	let source = base
		.resolve_table(&TableRef::new("src"))
		.await
		.expect("resolve source schema");
	let racing = Arc::new(RacingCatalog::at_publication(
		base.clone(),
		TableRef::new("raced_copy"),
		source.schema.clone(),
	));
	let coordinator = QueryCoordinator::with_writer(
		racing,
		Executor::new(storage.clone()),
		PlannerConfig::default(),
		storage.clone(),
	)
	.with_storage_admin(admin.clone());
	let plan = planner
		.plan("CREATE TABLE raced_copy AS SELECT id FROM src")
		.expect("plan raced CTAS");
	let query_id = QueryId::new();
	let storage_before = storage.table_count();
	let error = coordinator
		.execute_mutation_with_query_id(&plan, query_id)
		.await
		.expect_err("the concurrent winner must win publication");
	assert!(error.to_string().contains("binding_changed"), "{error}");

	let winner = base
		.resolve_table(&TableRef::new("raced_copy"))
		.await
		.expect("concurrent winner remains bound");
	assert_eq!(winner.schema.as_ref(), source.schema.as_ref());
	let saga = base
		.resolve_ctas_saga(wendelon_client::CtasOperationId::new(
			*query_id.get().as_bytes(),
		))
		.await
		.expect("resolve saga")
		.expect("saga remains attributable");
	assert_eq!(
		saga.phase,
		wendelon_client::CtasSagaPhase::Conflict,
		"error={error}; drops={}; saga={saga:?}",
		admin.drop_count()
	);
	assert_eq!(
		saga.terminal_cause,
		Some(wendelon_client::CtasTerminalCause::PublicationConflict)
	);
	assert_eq!(admin.drop_count(), 1, "populated provisional is reclaimed");
	assert_eq!(
		storage.table_count(),
		storage_before,
		"provisional storage must not remain stranded"
	);
}

#[tokio::test]
async fn adr548_restart_resolves_committed_rows_without_reexecuting_the_source() {
	let inner_catalog = Arc::new(MockCatalog::new());
	let catalog = Arc::new(ExpiredLeaseCatalog::new(inner_catalog.clone()));
	let storage = Arc::new(InMemoryTableStorage::new());
	let writer = Arc::new(AmbiguousCommittedCtasWrite::new(storage.clone()));
	let planner = SqlPlanner::new();
	let setup = QueryCoordinator::with_writer(
		catalog.clone(),
		Executor::new(storage.clone()),
		PlannerConfig::default(),
		writer.clone(),
	)
	.with_storage_admin(storage.clone());
	for sql in [
		"CREATE TABLE src (id INT NOT NULL)",
		"INSERT INTO src VALUES (1), (2)",
	] {
		setup
			.execute_query(&planner.plan(sql).expect("plan setup SQL"), None)
			.await
			.expect(sql);
	}
	let plan = planner
		.plan("CREATE TABLE resumed_copy AS SELECT id FROM src")
		.expect("plan CTAS");
	let query_id = QueryId::new();
	let context = ExecutionContext::new(SessionId(91), query_id, TenantId(1), HlcBound(1));
	let coordinator = QueryCoordinator::with_writer(
		catalog.clone(),
		Executor::with_context(storage.clone(), context),
		PlannerConfig::default(),
		writer,
	)
	.with_storage_admin(storage.clone());
	let first = coordinator
		.execute_mutation_with_query_id(&plan, query_id)
		.await
		.expect_err("first activation loses the committed write response");
	assert!(first.to_string().contains("ambiguous committed CTAS write"));
	assert!(matches!(
		catalog.resolve_table(&TableRef::new("resumed_copy")).await,
		Err(CatalogError::TableNotFound { .. })
	));

	// Remove the source storage while leaving its descriptor bindable. A replay
	// can succeed only by resolving the retained row outcome; re-running SELECT
	// would now fail with TableNotFound.
	let source = catalog
		.resolve_table(&TableRef::new("src"))
		.await
		.expect("resolve source");
	storage
		.drop_table_storage(DropTableStorageRequest {
			session_id: SessionId(92),
			query_id: QueryId::new(),
			tenant_id: TenantId(1),
			table_id: source.local_table_id,
			table_object_id: source.table,
			schema_generation: source.schema_generation,
			physical_descriptor_fingerprint: source.fingerprint,
			profile_hint: None,
			trace_context: None,
		})
		.await
		.expect("remove source storage after the first activation");
	catalog.expire_lease_for_resolution();

	coordinator
		.execute_mutation_with_query_id(&plan, query_id)
		.await
		.expect("recovery activation resolves and publishes committed rows");
	let saga = catalog
		.resolve_ctas_saga(wendelon_client::CtasOperationId::new(
			*query_id.get().as_bytes(),
		))
		.await
		.expect("resolve saga")
		.expect("saga exists");
	assert_eq!(saga.phase, wendelon_client::CtasSagaPhase::Complete);
	assert_eq!(saga.owner_epoch, 2);
	assert_eq!(saga.row_count, Some(2));
	let batch = concat_query(
		&coordinator,
		&planner,
		"SELECT id FROM resumed_copy ORDER BY id",
	)
	.await;
	assert_eq!(int32_column(&batch, 0).values(), &[1, 2]);
}

#[tokio::test]
async fn distributed_coordinator_ctas_uses_distributed_source_path() {
	let catalog = Arc::new(people_catalog().await);
	let coordinator = DistributedQueryCoordinator::new(
		catalog,
		Arc::new(StaticWorkerRegistry::new([])),
		Arc::new(MemoryManager::new(1, 1024 * 1024)),
		Arc::new(FailingTransport),
		PlannerConfig::default(),
		QueryBudget::test_default(),
	);
	let plan = SqlPlanner::new()
		.plan("CREATE TABLE copy AS SELECT id, name FROM people")
		.expect("plan CTAS");

	let err = coordinator
		.execute_query(&plan)
		.await
		.expect_err("distributed CTAS should request source workers");

	assert!(
		err.to_string().contains("no live worker available"),
		"unexpected error: {err}"
	);
	assert!(
		!err.to_string()
			.contains("distributed CREATE TABLE AS SELECT execution is not wired"),
		"old coordinator reject should be lifted: {err}"
	);
}

#[tokio::test]
async fn distributed_ctas_if_not_exists_existing_target_does_not_request_workers() {
	let catalog = Arc::new(people_catalog().await);
	catalog
		.create_table(
			&TableRef::new("copy"),
			schema(&[
				Field::new("id", DataType::Int32, false),
				Field::new("name", DataType::Utf8, true),
			]),
			false,
		)
		.await
		.expect("create compatible target");
	let coordinator = DistributedQueryCoordinator::new(
		catalog,
		Arc::new(StaticWorkerRegistry::new([])),
		Arc::new(MemoryManager::new(1, 1024 * 1024)),
		Arc::new(FailingTransport),
		PlannerConfig::default(),
		QueryBudget::test_default(),
	);
	let plan = SqlPlanner::new()
		.plan("CREATE TABLE IF NOT EXISTS copy AS SELECT id, name FROM people")
		.expect("plan CTAS IF NOT EXISTS");

	let results = coordinator
		.execute_query(&plan)
		.await
		.expect("compatible target should skip distributed source workers");

	assert!(results.is_empty());
}

#[tokio::test]
async fn executor_and_fragmenter_reject_direct_ctas_execution() {
	let catalog = people_catalog().await;
	let source = catalog
		.resolve_table(&TableRef::new("people"))
		.await
		.expect("resolve people");
	let plan = ExecutionPlan::CreateTableAsSelect {
		table: TableRef::new("copy"),
		schema: source.schema.clone(),
		intent_fingerprint: wendelon_client::CtasIntentFingerprint::new([0; 32]),
		if_not_exists: false,
		or_replace: false,
		source: Box::new(scan_plan(&source)),
	};
	let executor = Executor::with_context(
		empty_storage(),
		ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(1),
			TenantId(1),
			HlcBound(1),
		),
	);

	let err = executor
		.execute_bound(&plan, None)
		.await
		.expect_err("executor should reject CTAS");
	assert!(matches!(
		err,
		ExecutionError::NotImplemented {
			feature: "CREATE TABLE AS SELECT execution is coordinator-only"
		}
	));

	let err = fragment_plan(
		plan,
		FragmentPlanConfig::new_for_test(
			QueryId::from_u128(1),
			TaskBudget::from_query_budget(QueryBudget::test_default()),
		),
	)
	.expect_err("fragmenter should reject CTAS");
	assert_eq!(err, FragmentError::CreateTableAsSelectNotFragmentable);
}

#[tokio::test]
async fn catalog_create_outcome_reports_created_true_and_false() {
	let catalog = MockCatalog::new();
	let table = TableRef::new("copy");
	let table_schema = schema(&[Field::new("id", DataType::Int32, false)]);

	let created = catalog
		.create_table_with_outcome(
			&table,
			table_schema.clone(),
			CreateTableRowIdentity::AppendOnly,
			true,
		)
		.await
		.expect("fresh create");
	assert!(created.created);

	let existing = catalog
		.create_table_with_outcome(
			&table,
			table_schema,
			CreateTableRowIdentity::AppendOnly,
			true,
		)
		.await
		.expect("compatible existing");
	assert!(!existing.created);
	assert_eq!(
		created.binding.local_table_id,
		existing.binding.local_table_id
	);
}

async fn people_catalog() -> MockCatalog {
	let catalog = MockCatalog::new();
	catalog
		.create_table(
			&TableRef::new("people"),
			schema(&[
				Field::new("id", DataType::Int32, false),
				Field::new("name", DataType::Utf8, true),
			]),
			false,
		)
		.await
		.expect("create people");
	catalog
}

fn coordinator() -> (
	Arc<MockCatalog>,
	Arc<InMemoryTableStorage>,
	QueryCoordinator,
	SqlPlanner,
) {
	let catalog = Arc::new(MockCatalog::new());
	let storage = Arc::new(InMemoryTableStorage::new());
	let executor = Executor::new(storage.clone());
	let coordinator = QueryCoordinator::with_writer(
		catalog.clone(),
		executor,
		PlannerConfig::default(),
		storage.clone(),
	)
	.with_storage_admin(storage.clone());
	(catalog, storage, coordinator, SqlPlanner::new())
}

async fn concat_query(
	coordinator: &QueryCoordinator,
	planner: &SqlPlanner,
	sql: &str,
) -> RecordBatch {
	let results = coordinator
		.execute_query(&planner.plan(sql).expect("plan query"), None)
		.await
		.expect("execute query");
	let schema = results[0].schema();
	arrow::compute::concat_batches(&schema, results.iter()).expect("concat query results")
}

fn assert_frontend_error(sql: &str, diagnostic: &str) {
	let err = SqlPlanner::new().plan(sql).expect_err("SQL should reject");
	assert!(
		err.to_string().contains(diagnostic),
		"expected diagnostic {diagnostic:?}, got {err}"
	);
}

fn assert_field(schema: &SchemaRef, idx: usize, name: &str, data_type: &DataType, nullable: bool) {
	let field = schema.field(idx);
	assert_eq!(field.name(), name);
	assert_eq!(field.data_type(), data_type);
	assert_eq!(field.is_nullable(), nullable);
}

fn schema(fields: &[Field]) -> SchemaRef {
	Arc::new(Schema::new(fields.to_vec()))
}

fn empty_storage() -> Arc<InMemoryStorageScan> {
	let schema = Arc::new(Schema::empty());
	let batch = RecordBatch::new_empty(schema);
	Arc::new(InMemoryStorageScan { table_data: batch })
}

fn scan_plan(binding: &TableBinding) -> ExecutionPlan {
	ExecutionPlan::Scan {
		local_table_id: binding.local_table_id,
		table: binding.table,
		fingerprint: binding.fingerprint,
		schema: binding.schema.clone(),
		projection: None,
		pbc_program: None,
		row_identity: toron::storage::RowIdentityProjection::None,
		row_limit: None,
		profile_hint: QueryProfileHint::Scan,
		estimated_byte_size: None,
		primary_key_columns: vec![],
		column_ndv: Vec::new(),
		column_range: Vec::new(),
		column_range_distributions: Vec::new(),
		column_heavy_hitters: Vec::new(),
		column_group_top_k: Vec::new(),
		column_group_ndv: Vec::new(),
	}
}

fn int32_column(batch: &RecordBatch, index: usize) -> &Int32Array {
	batch
		.column(index)
		.as_any()
		.downcast_ref::<Int32Array>()
		.expect("Int32 column")
}

fn int64_column(batch: &RecordBatch, index: usize) -> &Int64Array {
	batch
		.column(index)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("Int64 column")
}

fn date32_column(batch: &RecordBatch, index: usize) -> &Date32Array {
	batch
		.column(index)
		.as_any()
		.downcast_ref::<Date32Array>()
		.expect("Date32 column")
}

fn timestamp_ns_column(batch: &RecordBatch, index: usize) -> &TimestampNanosecondArray {
	batch
		.column(index)
		.as_any()
		.downcast_ref::<TimestampNanosecondArray>()
		.expect("TimestampNanosecond column")
}

fn string_column(batch: &RecordBatch, index: usize) -> &StringArray {
	batch
		.column(index)
		.as_any()
		.downcast_ref::<StringArray>()
		.expect("Utf8 column")
}

struct RacingCatalog {
	inner: Arc<MockCatalog>,
	target: TableRef,
	raced_schema: SchemaRef,
	race_at_publication: bool,
}

impl RacingCatalog {
	fn new(inner: Arc<MockCatalog>, target: TableRef, raced_schema: SchemaRef) -> Self {
		Self {
			inner,
			target,
			raced_schema,
			race_at_publication: false,
		}
	}

	fn at_publication(inner: Arc<MockCatalog>, target: TableRef, raced_schema: SchemaRef) -> Self {
		Self {
			inner,
			target,
			raced_schema,
			race_at_publication: true,
		}
	}
}

/// Hides an existing table from the FIRST local resolution only.
///
/// That is the shape of a lost create race as the statement experiences it: the
/// preflight absence check misses, and every later lookup sees the winner. The
/// winner is real and pre-created, so "did the statement destroy it" is a
/// question about durable state rather than about the fixture.
struct HidesTargetOnFirstResolve {
	inner: Arc<MockCatalog>,
	target: TableRef,
	hidden: std::sync::atomic::AtomicBool,
}

impl HidesTargetOnFirstResolve {
	fn new(inner: Arc<MockCatalog>, target: TableRef) -> Self {
		Self {
			inner,
			target,
			hidden: std::sync::atomic::AtomicBool::new(false),
		}
	}

	fn hid_once(&self) -> bool {
		self.hidden.load(std::sync::atomic::Ordering::SeqCst)
	}
}

#[async_trait]
impl Catalog for HidesTargetOnFirstResolve {
	fn scoped(self: Arc<Self>, _tenant_id: TenantId) -> Arc<dyn Catalog> {
		self
	}

	fn descriptor_refresh_metrics(&self) -> Arc<DescriptorRefreshMetrics> {
		self.inner.descriptor_refresh_metrics()
	}

	async fn resolve_table(&self, table: &TableRef) -> Result<TableBinding, CatalogError> {
		if table == &self.target && !self.hidden.swap(true, std::sync::atomic::Ordering::SeqCst) {
			return Err(CatalogError::TableNotFound {
				table: table.table.clone(),
			});
		}
		self.inner.resolve_table(table).await
	}

	async fn resolve_table_local(&self, table: &TableRef) -> Result<TableBinding, CatalogError> {
		if table == &self.target && !self.hidden.swap(true, std::sync::atomic::Ordering::SeqCst) {
			return Err(CatalogError::TableNotFound {
				table: table.table.clone(),
			});
		}
		self.inner.resolve_table_local(table).await
	}

	async fn resolve_table_id(
		&self,
		table: toron::ir::TableId,
	) -> Result<TableBinding, CatalogError> {
		self.inner.resolve_table_id(table).await
	}

	async fn resolve_durable_logical_type_by_name(
		&self,
		name: &str,
	) -> Result<toron::ir::LogicalTypeSpec, CatalogError> {
		self.inner.resolve_durable_logical_type_by_name(name).await
	}

	async fn invalidate_table_descriptor(
		&self,
		table: toron::ir::TableId,
	) -> Result<Option<InvalidatedDescriptor>, CatalogError> {
		self.inner.invalidate_table_descriptor(table).await
	}

	async fn create_table_with_outcome(
		&self,
		name: &TableRef,
		schema: SchemaRef,
		row_identity: CreateTableRowIdentity,
		if_not_exists: bool,
	) -> Result<CreateTableOutcome, CatalogError> {
		self.inner
			.create_table_with_outcome(name, schema, row_identity, if_not_exists)
			.await
	}

	async fn provision_ctas(
		&self,
		request: CtasProvisionRequest,
	) -> Result<CtasProvisionOutcome, CatalogError> {
		self.inner.provision_ctas(request).await
	}

	async fn mutate_ctas_saga(
		&self,
		mutation: wendelon_client::CtasSagaMutation,
	) -> Result<wendelon_client::CtasSagaEntry, CatalogError> {
		self.inner.mutate_ctas_saga(mutation).await
	}

	async fn resolve_ctas_saga(
		&self,
		operation_id: wendelon_client::CtasOperationId,
	) -> Result<Option<wendelon_client::CtasSagaEntry>, CatalogError> {
		self.inner.resolve_ctas_saga(operation_id).await
	}

	async fn resolve_ctas_object(
		&self,
		table_object_id: wendelon_client::TableObjectId,
	) -> Result<TableBinding, CatalogError> {
		self.inner.resolve_ctas_object(table_object_id).await
	}

	fn descriptor_for_ctas_binding(
		&self,
		binding: &TableBinding,
	) -> Result<wendelon_client::TableSchemaDescriptor, CatalogError> {
		self.inner.descriptor_for_ctas_binding(binding)
	}

	async fn drop_table(
		&self,
		name: &TableRef,
		if_exists: bool,
	) -> Result<Option<TableBinding>, CatalogError> {
		self.inner.drop_table(name, if_exists).await
	}
}

#[async_trait]
impl Catalog for RacingCatalog {
	fn scoped(self: Arc<Self>, _tenant_id: TenantId) -> Arc<dyn Catalog> {
		self
	}

	fn descriptor_refresh_metrics(&self) -> Arc<DescriptorRefreshMetrics> {
		self.inner.descriptor_refresh_metrics()
	}

	async fn resolve_table(&self, table: &TableRef) -> Result<TableBinding, CatalogError> {
		self.inner.resolve_table(table).await
	}

	async fn resolve_table_local(&self, table: &TableRef) -> Result<TableBinding, CatalogError> {
		self.inner.resolve_table_local(table).await
	}

	async fn resolve_table_id(
		&self,
		table: toron::ir::TableId,
	) -> Result<TableBinding, CatalogError> {
		self.inner.resolve_table_id(table).await
	}

	async fn resolve_durable_logical_type_by_name(
		&self,
		name: &str,
	) -> Result<toron::ir::LogicalTypeSpec, CatalogError> {
		self.inner.resolve_durable_logical_type_by_name(name).await
	}

	async fn invalidate_table_descriptor(
		&self,
		table: toron::ir::TableId,
	) -> Result<Option<InvalidatedDescriptor>, CatalogError> {
		self.inner.invalidate_table_descriptor(table).await
	}

	async fn create_table_with_outcome(
		&self,
		name: &TableRef,
		schema: SchemaRef,
		row_identity: CreateTableRowIdentity,
		if_not_exists: bool,
	) -> Result<CreateTableOutcome, CatalogError> {
		if if_not_exists && name == &self.target {
			let _ = self
				.inner
				.create_table(name, self.raced_schema.clone(), true)
				.await?;
		}
		self.inner
			.create_table_with_outcome(name, schema, row_identity, if_not_exists)
			.await
	}

	async fn provision_ctas(
		&self,
		request: CtasProvisionRequest,
	) -> Result<CtasProvisionOutcome, CatalogError> {
		if !self.race_at_publication && request.name == self.target {
			let _ = self
				.inner
				.create_table(&request.name, self.raced_schema.clone(), true)
				.await?;
		}
		self.inner.provision_ctas(request).await
	}

	async fn mutate_ctas_saga(
		&self,
		mutation: wendelon_client::CtasSagaMutation,
	) -> Result<wendelon_client::CtasSagaEntry, CatalogError> {
		if self.race_at_publication
			&& matches!(&mutation, wendelon_client::CtasSagaMutation::Publish { .. })
		{
			let _ = self
				.inner
				.create_table(&self.target, self.raced_schema.clone(), true)
				.await?;
		}
		self.inner.mutate_ctas_saga(mutation).await
	}

	async fn resolve_ctas_saga(
		&self,
		operation_id: wendelon_client::CtasOperationId,
	) -> Result<Option<wendelon_client::CtasSagaEntry>, CatalogError> {
		self.inner.resolve_ctas_saga(operation_id).await
	}

	async fn resolve_ctas_object(
		&self,
		table_object_id: wendelon_client::TableObjectId,
	) -> Result<TableBinding, CatalogError> {
		self.inner.resolve_ctas_object(table_object_id).await
	}

	fn descriptor_for_ctas_binding(
		&self,
		binding: &TableBinding,
	) -> Result<wendelon_client::TableSchemaDescriptor, CatalogError> {
		self.inner.descriptor_for_ctas_binding(binding)
	}

	async fn drop_table(
		&self,
		name: &TableRef,
		if_exists: bool,
	) -> Result<Option<TableBinding>, CatalogError> {
		self.inner.drop_table(name, if_exists).await
	}
}

struct CountingStorageAdmin {
	inner: Arc<InMemoryTableStorage>,
	drops: std::sync::atomic::AtomicUsize,
}

impl CountingStorageAdmin {
	fn new(inner: Arc<InMemoryTableStorage>) -> Self {
		Self {
			inner,
			drops: std::sync::atomic::AtomicUsize::new(0),
		}
	}

	fn drop_count(&self) -> usize {
		self.drops.load(std::sync::atomic::Ordering::SeqCst)
	}
}

#[async_trait]
impl toron::storage::StorageAdmin for CountingStorageAdmin {
	async fn drop_table_storage(
		&self,
		request: toron::storage::DropTableStorageRequest,
	) -> Result<toron::storage::DropTableStorageOutcome, toron::error::StorageError> {
		self.drops.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
		self.inner.drop_table_storage(request).await
	}
}

struct FailingTransport;

#[async_trait]
impl ToronTransport for FailingTransport {
	async fn connect(&self, _address: &str) -> Result<Box<dyn ToronStream>, QueryError> {
		Err(QueryError::Internal {
			message: "transport should not be used for CTAS".to_string(),
		})
	}
}

struct FailingWrite;

#[async_trait]
impl StorageWrite for FailingWrite {
	async fn write(&self, _request: WriteRequest) -> Result<WriteOutcome, StorageError> {
		Err(StorageError::Internal {
			message: "forced CTAS write failure".to_string(),
		})
	}
}

struct KnownNotCommittedCtasWrite;

#[async_trait]
impl StorageWrite for KnownNotCommittedCtasWrite {
	async fn write(&self, _request: WriteRequest) -> Result<WriteOutcome, StorageError> {
		Err(StorageError::Internal {
			message: "known-not-committed CTAS write".to_string(),
		})
	}

	async fn resolve_mutation(
		&self,
		_tenant_id: TenantId,
		_operation_id: wendelon::schema::ClientWriteId,
	) -> Result<ResolvedMutationOutcome, StorageError> {
		Ok(ResolvedMutationOutcome {
			resolution: MutationResolution::Present,
			write_status: wendelon::schema::WriteStatus::INTERNAL_ERROR.0,
			stop_reason: wendelon::schema::MutationStopReason::NONE.0,
			commit_knowledge: wendelon::schema::MutationCommitKnowledge::KNOWN_NOT_COMMITTED.0,
			delete_counts: None,
		})
	}
}

struct AmbiguousCommittedCtasWrite {
	inner: Arc<InMemoryTableStorage>,
	failed_once: std::sync::atomic::AtomicBool,
	resolution_calls: std::sync::atomic::AtomicUsize,
}

impl AmbiguousCommittedCtasWrite {
	fn new(inner: Arc<InMemoryTableStorage>) -> Self {
		Self {
			inner,
			failed_once: std::sync::atomic::AtomicBool::new(false),
			resolution_calls: std::sync::atomic::AtomicUsize::new(0),
		}
	}
}

#[async_trait]
impl StorageWrite for AmbiguousCommittedCtasWrite {
	async fn write(&self, request: WriteRequest) -> Result<WriteOutcome, StorageError> {
		let is_ctas = request
			.operation_id
			.is_some_and(|id| id.sequence_no == 0x4354_4153_0000_0001);
		if is_ctas
			&& !self
				.failed_once
				.swap(true, std::sync::atomic::Ordering::SeqCst)
		{
			let _ = self.inner.write(request).await?;
			return Err(StorageError::Internal {
				message: "ambiguous committed CTAS write".to_string(),
			});
		}
		self.inner.write(request).await
	}

	async fn resolve_mutation(
		&self,
		_tenant_id: TenantId,
		_operation_id: wendelon::schema::ClientWriteId,
	) -> Result<ResolvedMutationOutcome, StorageError> {
		let call = self
			.resolution_calls
			.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
		Ok(if call == 0 {
			ResolvedMutationOutcome {
				resolution: MutationResolution::Unknown,
				write_status: 0,
				stop_reason: 0,
				commit_knowledge: wendelon::schema::MutationCommitKnowledge::UNSPECIFIED.0,
				delete_counts: None,
			}
		} else {
			ResolvedMutationOutcome {
				resolution: MutationResolution::Present,
				write_status: wendelon::schema::WriteStatus::OK.0,
				stop_reason: wendelon::schema::MutationStopReason::NONE.0,
				commit_knowledge: wendelon::schema::MutationCommitKnowledge::COMMITTED.0,
				delete_counts: None,
			}
		})
	}
}

struct ExpiredLeaseCatalog {
	inner: Arc<MockCatalog>,
	expire_on_resolve: std::sync::atomic::AtomicBool,
}

impl ExpiredLeaseCatalog {
	fn new(inner: Arc<MockCatalog>) -> Self {
		Self {
			inner,
			expire_on_resolve: std::sync::atomic::AtomicBool::new(false),
		}
	}

	fn expire_lease_for_resolution(&self) {
		self.expire_on_resolve
			.store(true, std::sync::atomic::Ordering::SeqCst);
	}
}

#[async_trait]
impl Catalog for ExpiredLeaseCatalog {
	fn scoped(self: Arc<Self>, _tenant_id: TenantId) -> Arc<dyn Catalog> {
		self
	}

	fn descriptor_refresh_metrics(&self) -> Arc<DescriptorRefreshMetrics> {
		self.inner.descriptor_refresh_metrics()
	}

	async fn resolve_table(&self, table: &TableRef) -> Result<TableBinding, CatalogError> {
		self.inner.resolve_table(table).await
	}

	async fn resolve_table_local(&self, table: &TableRef) -> Result<TableBinding, CatalogError> {
		self.inner.resolve_table_local(table).await
	}

	async fn resolve_table_id(
		&self,
		table: toron::ir::TableId,
	) -> Result<TableBinding, CatalogError> {
		self.inner.resolve_table_id(table).await
	}

	async fn resolve_durable_logical_type_by_name(
		&self,
		name: &str,
	) -> Result<toron::ir::LogicalTypeSpec, CatalogError> {
		self.inner.resolve_durable_logical_type_by_name(name).await
	}

	async fn invalidate_table_descriptor(
		&self,
		table: toron::ir::TableId,
	) -> Result<Option<InvalidatedDescriptor>, CatalogError> {
		self.inner.invalidate_table_descriptor(table).await
	}

	async fn provision_ctas(
		&self,
		request: CtasProvisionRequest,
	) -> Result<CtasProvisionOutcome, CatalogError> {
		self.inner.provision_ctas(request).await
	}

	async fn mutate_ctas_saga(
		&self,
		mutation: wendelon_client::CtasSagaMutation,
	) -> Result<wendelon_client::CtasSagaEntry, CatalogError> {
		self.inner.mutate_ctas_saga(mutation).await
	}

	async fn resolve_ctas_saga(
		&self,
		operation_id: wendelon_client::CtasOperationId,
	) -> Result<Option<wendelon_client::CtasSagaEntry>, CatalogError> {
		let mut entry = self.inner.resolve_ctas_saga(operation_id).await?;
		if self
			.expire_on_resolve
			.load(std::sync::atomic::Ordering::SeqCst)
		{
			if let Some(entry) = entry.as_mut() {
				entry.owner_lease_expires_at_ms = 0;
			}
		}
		Ok(entry)
	}

	async fn resolve_ctas_object(
		&self,
		table_object_id: wendelon_client::TableObjectId,
	) -> Result<TableBinding, CatalogError> {
		self.inner.resolve_ctas_object(table_object_id).await
	}

	fn descriptor_for_ctas_binding(
		&self,
		binding: &TableBinding,
	) -> Result<wendelon_client::TableSchemaDescriptor, CatalogError> {
		self.inner.descriptor_for_ctas_binding(binding)
	}

	async fn create_table_with_outcome(
		&self,
		name: &TableRef,
		schema: SchemaRef,
		row_identity: CreateTableRowIdentity,
		if_not_exists: bool,
	) -> Result<CreateTableOutcome, CatalogError> {
		self.inner
			.create_table_with_outcome(name, schema, row_identity, if_not_exists)
			.await
	}

	async fn drop_table(
		&self,
		name: &TableRef,
		if_exists: bool,
	) -> Result<Option<TableBinding>, CatalogError> {
		self.inner.drop_table(name, if_exists).await
	}
}
