use std::sync::Arc;

use arrow::array::{Array, Date32Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use toron::binder::{PlannerConfig, TABLE_WRITE_MAX_ROWS, bind};
use toron::catalog::{Catalog, MockCatalog, TableBinding, TableRef};
use toron::coordinator::{DistributedQueryCoordinator, QueryCoordinator};
use toron::distributed::budget::{QueryBudget, TaskBudget};
use toron::distributed::fragment::{FragmentError, FragmentPlanConfig, fragment_plan};
use toron::distributed::identity::QueryId;
use toron::distributed::worker_registry::StaticWorkerRegistry;
use toron::error::{BindingError, ExecutionError, QueryError, StorageError};
use toron::exec::{ExecutionContext, Executor};
use toron::ir::{
	ColumnOrdinal, ExecutionPlan, InsertLiteral, LogicalPlan, QueryProfileHint, ScalarValue,
	SessionId, TableId,
};
use toron::server::memory::MemoryManager;
use toron::server::transport::{ToronStream, ToronTransport};
use toron::sql_planner::SqlPlanner;
use toron::storage::{
	HlcBound, InMemoryStorageScan, InMemoryTableStorage, RowIdentityProjection, ScanRequest,
	StorageScan, StorageWrite, TenantId, WriteRequest,
};
use wendelon::schema::{STORAGE_ROW_KEY_ID_COLUMN, TableObjectId};

#[test]
fn planner_accepts_literal_insert_surface() {
	let plan = SqlPlanner::new()
		.plan(
			"INSERT INTO app.sales (name, id, sold_on) \
			 VALUES ('Alice', 1, DATE '2024-01-01'), ('Bob', 2, Date32 '2024-02-29')",
		)
		.expect("plan insert");

	let LogicalPlan::Insert {
		table,
		columns,
		rows,
		..
	} = plan
	else {
		panic!("expected insert plan");
	};

	assert_eq!(table, TableRef::with_schema("app", "sales"));
	assert_eq!(
		columns,
		Some(vec![
			"name".to_string(),
			"id".to_string(),
			"sold_on".to_string()
		])
	);
	assert_eq!(
		rows,
		vec![
			vec![
				InsertLiteral::Utf8("Alice".to_string()),
				InsertLiteral::Int64(1),
				InsertLiteral::Date32(19_723)
			],
			vec![
				InsertLiteral::Utf8("Bob".to_string()),
				InsertLiteral::Int64(2),
				InsertLiteral::Date32(19_782)
			],
		]
	);
}

#[test]
fn planner_accepts_insert_select_surface() {
	let plan = SqlPlanner::new()
		.plan("INSERT INTO dst (b, a) SELECT x, y FROM src")
		.expect("plan insert select");

	let LogicalPlan::InsertSelect {
		table,
		columns,
		source,
		..
	} = plan
	else {
		panic!("expected insert select plan");
	};

	assert_eq!(table, TableRef::new("dst"));
	assert_eq!(columns, Some(vec!["b".to_string(), "a".to_string()]));
	assert!(matches!(*source, LogicalPlan::Project { .. }));
}

#[test]
fn planner_accepts_insert_returning_surface() {
	let plan = SqlPlanner::new()
		.plan("INSERT INTO people (id, name) VALUES (1, 'Alice') RETURNING name, id + 1 AS next_id")
		.expect("plan insert returning");

	let LogicalPlan::Insert { returning, .. } = plan else {
		panic!("expected insert plan");
	};
	assert!(returning.is_some());

	let plan = SqlPlanner::new()
		.plan("INSERT INTO dst (b, a) SELECT x, y FROM src RETURNING *")
		.expect("plan insert select returning");
	let LogicalPlan::InsertSelect { returning, .. } = plan else {
		panic!("expected insert select plan");
	};
	assert!(returning.is_some());
}

#[test]
fn planner_accepts_full_column_null_and_negative_literals() {
	let plan = SqlPlanner::new()
		.plan("INSERT INTO readings VALUES (NULL, -1)")
		.expect("plan insert");

	let LogicalPlan::Insert {
		table,
		columns,
		rows,
		..
	} = plan
	else {
		panic!("expected insert plan");
	};

	assert_eq!(table, TableRef::new("readings"));
	assert_eq!(columns, None);
	assert_eq!(
		rows,
		vec![vec![InsertLiteral::Null, InsertLiteral::Int64(-1)]]
	);
}

#[test]
fn planner_rejects_unsupported_insert_shapes() {
	for (sql, diagnostic) in [
		("INSERT t (a) VALUES (1)", "missing INTO"),
		("INSERT INTO TABLE t (a) VALUES (1)", "TABLE keyword"),
		(
			"INSERT INTO t (a) VALUES (1) ON CONFLICT DO NOTHING",
			"ON CONFLICT",
		),
		("INSERT OR REPLACE INTO t (a) VALUES (1)", "OR"),
		("INSERT IGNORE INTO t (a) VALUES (1)", "IGNORE"),
		("REPLACE INTO t (a) VALUES (1)", "REPLACE INTO"),
		(
			"INSERT INTO catalog.public.t (a) VALUES (1)",
			"only one- or two-part",
		),
		("INSERT INTO t (catalog.a) VALUES (1)", "qualified column"),
		(
			"INSERT INTO t (a) VALUES (1) ORDER BY a",
			"must be VALUES only",
		),
		(
			"INSERT INTO t (a) VALUES (1) LIMIT 1",
			"must be VALUES only",
		),
		("INSERT OVERWRITE TABLE t (a) VALUES (1)", "OVERWRITE"),
		("INSERT INTO t (a) VALUES (1 + 2)", "must be literals"),
		("INSERT INTO t (a) VALUES (now())", "must be literals"),
	] {
		assert_frontend_error(sql, diagnostic);
	}
}

#[test]
fn planner_rejects_returning_aggregate_window_subquery_and_qualified_wildcard() {
	for (sql, diagnostic) in [
		(
			"INSERT INTO t (a) VALUES (1) RETURNING count(a)",
			"aggregate",
		),
		(
			"INSERT INTO t (a) VALUES (1) RETURNING row_number() OVER ()",
			"window",
		),
		(
			"INSERT INTO t (a) VALUES (1) RETURNING (SELECT a FROM t)",
			"subqueries",
		),
		(
			"INSERT INTO t (a) VALUES (1) RETURNING t.*",
			"qualified wildcard",
		),
	] {
		assert_frontend_error(sql, diagnostic);
	}
}

#[tokio::test]
async fn binder_reorders_columns_and_fills_typed_nulls() {
	let catalog = MockCatalog::new();
	catalog
		.create_table(
			&TableRef::new("people"),
			schema(&[
				Field::new("id", DataType::Int32, false),
				Field::new("name", DataType::Utf8, true),
				Field::new("score", DataType::Float64, true),
			]),
			false,
		)
		.await
		.expect("create table");
	let plan = SqlPlanner::new()
		.plan("INSERT INTO people (name, id) VALUES ('Alice', 7)")
		.expect("plan insert");

	let bound = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("bind insert");
	let ExecutionPlan::Insert { rows, schema, .. } = bound else {
		panic!("expected insert execution plan");
	};

	assert_eq!(
		rows,
		vec![vec![
			ScalarValue::Int32(Some(7)),
			ScalarValue::Utf8(Some("Alice".to_string())),
			ScalarValue::Float64(None),
		]]
	);
	assert_eq!(schema.field(0).name(), "id");
	assert_eq!(schema.field(1).name(), "name");
	assert_eq!(schema.field(2).name(), "score");
}

#[tokio::test]
async fn binder_coerces_all_create_table_types() {
	let catalog = MockCatalog::new();
	catalog
		.create_table(
			&TableRef::new("metrics"),
			schema(&[
				Field::new("i32_value", DataType::Int32, false),
				Field::new("i64_value", DataType::Int64, false),
				Field::new("f32_value", DataType::Float32, false),
				Field::new("f64_value", DataType::Float64, false),
				Field::new("label", DataType::Utf8, false),
				Field::new("active", DataType::Boolean, false),
				Field::new("event_date", DataType::Date32, false),
			]),
			false,
		)
		.await
		.expect("create metrics");
	let plan = SqlPlanner::new()
		.plan(
			"INSERT INTO metrics VALUES \
			 (1, 9007199254740991, 1.25, 2.5, 'ok', TRUE, DATE '2024-01-01')",
		)
		.expect("plan insert");

	let bound = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("bind insert");
	let ExecutionPlan::Insert { rows, .. } = bound else {
		panic!("expected insert execution plan");
	};

	assert_eq!(
		rows,
		vec![vec![
			ScalarValue::Int32(Some(1)),
			ScalarValue::Int64(Some(9_007_199_254_740_991)),
			ScalarValue::Float32(Some(1.25)),
			ScalarValue::Float64(Some(2.5)),
			ScalarValue::Utf8(Some("ok".to_string())),
			ScalarValue::Boolean(Some(true)),
			ScalarValue::Date32(Some(19_723)),
		]]
	);
}

#[tokio::test]
async fn binder_emits_insert_select_target_columns_in_source_order() {
	let catalog = insert_select_catalog().await;
	let plan = SqlPlanner::new()
		.plan("INSERT INTO dst (b, a) SELECT x, y FROM src")
		.expect("plan insert select");

	let bound = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("bind insert select");
	let ExecutionPlan::InsertSelect {
		schema,
		target_columns,
		source,
		..
	} = bound
	else {
		panic!("expected insert select execution plan");
	};

	assert_eq!(schema.field(0).name(), "a");
	assert_eq!(schema.field(1).name(), "b");
	assert_eq!(
		target_columns
			.iter()
			.map(|column| column.0)
			.collect::<Vec<_>>(),
		vec![1, 0]
	);
	assert_eq!(source.schema().fields().len(), 2);
}

#[tokio::test]
async fn binder_rejects_invalid_insert_select_bindings() {
	for (sql, diagnostic) in [
		(
			"INSERT INTO dst (a, b) SELECT x FROM src",
			"source has 1 columns; expected 2",
		),
		(
			"INSERT INTO dst (missing) SELECT x FROM src",
			"unknown column",
		),
		(
			"INSERT INTO dst (b) SELECT label FROM src",
			"omits non-nullable column",
		),
		(
			"INSERT INTO dst (a) SELECT label FROM src",
			"not compatible",
		),
	] {
		let catalog = insert_select_catalog().await;
		let plan = SqlPlanner::new().plan(sql).expect("plan insert select");
		let err = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect_err("bind should reject");
		assert!(
			err.to_string().contains(diagnostic),
			"expected diagnostic {diagnostic:?}, got {err}"
		);
	}
}

#[tokio::test]
async fn binder_rejects_invalid_insert_bindings() {
	for (sql, diagnostic) in [
		(
			"INSERT INTO people (id, id) VALUES (1, 2)",
			"duplicate INSERT target column",
		),
		("INSERT INTO people (missing) VALUES (1)", "unknown column"),
		(
			"INSERT INTO people (id, name) VALUES (1)",
			"has 1 values; expected 2",
		),
		(
			"INSERT INTO people (name) VALUES ('Alice')",
			"omits non-nullable column",
		),
		(
			"INSERT INTO people (id) VALUES (NULL)",
			"NULL is not allowed",
		),
		(
			"INSERT INTO people (id) VALUES (99999999999)",
			"overflows column",
		),
		(
			"INSERT INTO people (name) VALUES (1)",
			"not compatible with column",
		),
	] {
		let catalog = people_catalog().await;
		let plan = SqlPlanner::new().plan(sql).expect("plan insert");
		let err = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect_err("bind should reject");
		assert!(
			err.to_string().contains(diagnostic),
			"expected diagnostic {diagnostic:?}, got {err}"
		);
	}
}

#[tokio::test]
async fn binder_returning_rejects_unknown_and_storage_synthetic_columns() {
	let catalog = people_catalog().await;
	for (sql, diagnostic) in [
		(
			"INSERT INTO people (id, name) VALUES (1, 'Alice') RETURNING missing".to_string(),
			"unknown column",
		),
		(
			format!(
				"INSERT INTO people (id, name) VALUES (1, 'Alice') RETURNING {STORAGE_ROW_KEY_ID_COLUMN}"
			),
			"storage metadata",
		),
	] {
		let plan = SqlPlanner::new().plan(&sql).expect("plan insert returning");
		let err = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect_err("RETURNING binding should reject");
		assert!(
			err.to_string().contains(diagnostic),
			"expected diagnostic {diagnostic:?}, got {err}"
		);
	}
}

#[tokio::test]
async fn binder_rejects_insert_row_cap() {
	let catalog = people_catalog().await;
	let rows = vec![vec![InsertLiteral::Int64(1)]; TABLE_WRITE_MAX_ROWS + 1];
	let plan = LogicalPlan::Insert {
		table: TableRef::new("people"),
		columns: Some(vec!["id".to_string()]),
		rows,
		returning: None,
	};

	let err = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect_err("row cap should reject");

	assert!(matches!(err, BindingError::UnsupportedExpression { .. }));
	assert!(err.to_string().contains("exceeds maximum"));
}

#[tokio::test]
async fn binder_rejects_empty_insert_rows() {
	let catalog = people_catalog().await;
	let plan = LogicalPlan::Insert {
		table: TableRef::new("people"),
		columns: Some(vec!["id".to_string()]),
		rows: Vec::new(),
		returning: None,
	};

	let err = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect_err("empty insert rows should reject");

	assert!(matches!(err, BindingError::UnsupportedExpression { .. }));
	assert!(err.to_string().contains("at least one row"));
}

#[tokio::test]
async fn query_coordinator_executes_insert_and_select_round_trip() {
	let catalog = Arc::new(MockCatalog::new());
	let storage = Arc::new(InMemoryTableStorage::new());
	let executor = Executor::new(storage.clone());
	let coordinator =
		QueryCoordinator::with_writer(catalog, executor, PlannerConfig::default(), storage.clone());
	let planner = SqlPlanner::new();

	coordinator
		.execute_query(
			&planner
				.plan("CREATE TABLE people (id INT NOT NULL, name TEXT)")
				.expect("plan create"),
			None,
		)
		.await
		.expect("create");
	coordinator
		.execute_query(
			&planner
				.plan("INSERT INTO people VALUES (1, 'Alice'), (2, 'Bob')")
				.expect("plan insert"),
			None,
		)
		.await
		.expect("insert");

	let results = coordinator
		.execute_query(
			&planner
				.plan("SELECT id, name FROM people ORDER BY id")
				.expect("plan select"),
			None,
		)
		.await
		.expect("select");
	let schema = results[0].schema();
	let batch = arrow::compute::concat_batches(&schema, results.iter()).expect("concat results");

	assert_eq!(batch.num_rows(), 2);
	let ids = batch
		.column(0)
		.as_any()
		.downcast_ref::<Int32Array>()
		.expect("id column");
	let names = batch
		.column(1)
		.as_any()
		.downcast_ref::<StringArray>()
		.expect("name column");
	assert_eq!(ids.value(0), 1);
	assert_eq!(ids.value(1), 2);
	assert_eq!(names.value(0), "Alice");
	assert_eq!(names.value(1), "Bob");
}

#[tokio::test]
async fn query_coordinator_executes_date32_insert_round_trip() {
	let catalog = Arc::new(MockCatalog::new());
	let storage = Arc::new(InMemoryTableStorage::new());
	let executor = Executor::new(storage.clone());
	let coordinator =
		QueryCoordinator::with_writer(catalog, executor, PlannerConfig::default(), storage.clone());
	let planner = SqlPlanner::new();

	for sql in [
		"CREATE TABLE events (id INT NOT NULL, event_date DATE)",
		"INSERT INTO events VALUES (1, DATE '2024-01-01'), (2, Date32 '2024-02-29')",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan SQL"), None)
			.await
			.expect(sql);
	}

	let results = coordinator
		.execute_query(
			&planner
				.plan("SELECT id, event_date FROM events ORDER BY id")
				.expect("plan select"),
			None,
		)
		.await
		.expect("select");
	let schema = results[0].schema();
	let batch = arrow::compute::concat_batches(&schema, results.iter()).expect("concat results");
	let dates = batch
		.column(1)
		.as_any()
		.downcast_ref::<Date32Array>()
		.expect("Date32 column");

	assert_eq!(batch.column(1).data_type(), &DataType::Date32);
	assert_eq!(dates.values(), &[19_723, 19_782]);
}

#[tokio::test]
async fn insert_values_returning_emits_inserted_rows() {
	let catalog = Arc::new(MockCatalog::new());
	let storage = Arc::new(InMemoryTableStorage::new());
	let executor = Executor::new(storage.clone());
	let coordinator =
		QueryCoordinator::with_writer(catalog, executor, PlannerConfig::default(), storage.clone());
	let planner = SqlPlanner::new();

	coordinator
		.execute_query(
			&planner
				.plan("CREATE TABLE people (id INT NOT NULL, name TEXT)")
				.expect("plan create"),
			None,
		)
		.await
		.expect("create table");
	let results = coordinator
		.execute_query(
			&planner
				.plan("INSERT INTO people (name, id) VALUES ('Alice', 1), ('Bob', 2) RETURNING name, id + 1 AS next_id")
				.expect("plan insert returning"),
			None,
		)
		.await
		.expect("insert returning");
	assert_eq!(results.len(), 1);
	let batch = &results[0];
	assert_eq!(batch.schema().field(0).name(), "name");
	assert_eq!(batch.schema().field(1).name(), "next_id");
	let names = batch
		.column(0)
		.as_any()
		.downcast_ref::<StringArray>()
		.expect("name column");
	let next_ids = batch
		.column(1)
		.as_any()
		.downcast_ref::<Int32Array>()
		.expect("next_id column");
	assert_eq!(names.value(0), "Alice");
	assert_eq!(names.value(1), "Bob");
	assert_eq!(next_ids.values(), &[2, 3]);

	let stored = concat_query(
		&coordinator,
		&planner,
		"SELECT id, name FROM people ORDER BY id",
	)
	.await;
	assert_eq!(
		stored
			.column(0)
			.as_any()
			.downcast_ref::<Int32Array>()
			.expect("id column")
			.values(),
		&[1, 2]
	);
}

#[tokio::test]
async fn query_coordinator_executes_insert_select_composition_fixtures() {
	let catalog = Arc::new(MockCatalog::new());
	let storage = Arc::new(InMemoryTableStorage::new());
	let executor = Executor::new(storage.clone());
	let coordinator =
		QueryCoordinator::with_writer(catalog, executor, PlannerConfig::default(), storage.clone());
	let planner = SqlPlanner::new();

	for sql in [
		"CREATE TABLE src (id INT NOT NULL, amount BIGINT NOT NULL)",
		"CREATE TABLE totals (id INT NOT NULL, total BIGINT NOT NULL)",
		"INSERT INTO src VALUES (1, 100), (1, 200), (2, 50)",
		"INSERT INTO totals (id, total) SELECT id, SUM(amount) FROM src GROUP BY id",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan SQL"), None)
			.await
			.expect(sql);
	}
	let totals = concat_query(
		&coordinator,
		&planner,
		"SELECT id, total FROM totals ORDER BY id",
	)
	.await;
	let ids = totals
		.column(0)
		.as_any()
		.downcast_ref::<Int32Array>()
		.expect("id column");
	let totals_values = totals
		.column(1)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("total column");
	assert_eq!(ids.values(), &[1, 2]);
	assert_eq!(totals_values.values(), &[300, 50]);

	for sql in [
		"CREATE TABLE left_table (id INT NOT NULL, label TEXT)",
		"CREATE TABLE right_table (left_id INT NOT NULL, score BIGINT NOT NULL)",
		"CREATE TABLE joined (id INT NOT NULL, label TEXT, score BIGINT NOT NULL)",
		"INSERT INTO left_table VALUES (1, 'a'), (2, 'b')",
		"INSERT INTO right_table VALUES (1, 100), (2, 200)",
		"INSERT INTO joined (score, id, label) SELECT score, id, label FROM left_table INNER JOIN right_table ON left_table.id = right_table.left_id",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan SQL"), None)
			.await
			.expect(sql);
	}
	let joined = concat_query(
		&coordinator,
		&planner,
		"SELECT id, label, score FROM joined ORDER BY id",
	)
	.await;
	let joined_ids = joined
		.column(0)
		.as_any()
		.downcast_ref::<Int32Array>()
		.expect("id column");
	let labels = joined
		.column(1)
		.as_any()
		.downcast_ref::<StringArray>()
		.expect("label column");
	let scores = joined
		.column(2)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("score column");
	assert_eq!(joined_ids.values(), &[1, 2]);
	assert_eq!(labels.value(0), "a");
	assert_eq!(labels.value(1), "b");
	assert_eq!(scores.values(), &[100, 200]);
}

#[tokio::test]
async fn query_coordinator_insert_select_reorders_and_fills_nullable_targets() {
	let catalog = Arc::new(MockCatalog::new());
	let storage = Arc::new(InMemoryTableStorage::new());
	let executor = Executor::new(storage.clone());
	let coordinator =
		QueryCoordinator::with_writer(catalog, executor, PlannerConfig::default(), storage.clone());
	let planner = SqlPlanner::new();

	for sql in [
		"CREATE TABLE src (x INT NOT NULL, y TEXT)",
		"CREATE TABLE dst (a TEXT, b INT NOT NULL, c BIGINT)",
		"INSERT INTO src VALUES (7, 'seven')",
		"INSERT INTO dst (b, a) SELECT x, y FROM src",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan SQL"), None)
			.await
			.expect(sql);
	}

	let batch = concat_query(&coordinator, &planner, "SELECT a, b, c FROM dst").await;
	let labels = batch
		.column(0)
		.as_any()
		.downcast_ref::<StringArray>()
		.expect("a column");
	let values = batch
		.column(1)
		.as_any()
		.downcast_ref::<Int32Array>()
		.expect("b column");
	let omitted = batch
		.column(2)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("c column");
	assert_eq!(labels.value(0), "seven");
	assert_eq!(values.value(0), 7);
	assert!(omitted.is_null(0));
}

#[tokio::test]
async fn insert_select_returning_preserves_target_reorder_and_coercion() {
	let catalog = Arc::new(MockCatalog::new());
	let storage = Arc::new(InMemoryTableStorage::new());
	let executor = Executor::new(storage.clone());
	let coordinator =
		QueryCoordinator::with_writer(catalog, executor, PlannerConfig::default(), storage.clone());
	let planner = SqlPlanner::new();

	for sql in [
		"CREATE TABLE src (x INT NOT NULL, y TEXT)",
		"CREATE TABLE dst (a TEXT, b INT NOT NULL, c BIGINT)",
		"INSERT INTO src VALUES (7, 'seven')",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan SQL"), None)
			.await
			.expect(sql);
	}

	let results = coordinator
		.execute_query(
			&planner
				.plan("INSERT INTO dst (b, a) SELECT x, y FROM src RETURNING *")
				.expect("plan insert select returning"),
			None,
		)
		.await
		.expect("insert select returning");
	assert_eq!(results.len(), 1);
	let batch = &results[0];
	assert_eq!(batch.schema().field(0).name(), "a");
	assert_eq!(batch.schema().field(1).name(), "b");
	assert_eq!(batch.schema().field(2).name(), "c");
	assert_eq!(
		batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("a column")
			.value(0),
		"seven"
	);
	assert_eq!(
		batch
			.column(1)
			.as_any()
			.downcast_ref::<Int32Array>()
			.expect("b column")
			.value(0),
		7
	);
	assert!(batch.column(2).is_null(0));
}

#[tokio::test]
async fn query_coordinator_insert_select_empty_source_does_not_write() {
	let catalog = Arc::new(MockCatalog::new());
	let storage = Arc::new(InMemoryTableStorage::new());
	let executor = Executor::new(storage.clone());
	let coordinator =
		QueryCoordinator::with_writer(catalog, executor, PlannerConfig::default(), storage.clone());
	let planner = SqlPlanner::new();

	for sql in [
		"CREATE TABLE src (id INT NOT NULL)",
		"CREATE TABLE dst (id INT NOT NULL)",
		"INSERT INTO src VALUES (1)",
		"INSERT INTO dst SELECT id FROM src WHERE id > 10",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan SQL"), None)
			.await
			.expect(sql);
	}

	let err = coordinator
		.execute_query(
			&planner.plan("SELECT id FROM dst").expect("plan select"),
			None,
		)
		.await
		.expect_err("empty source must not initialize table storage");
	assert!(
		err.to_string().contains("table not found"),
		"unexpected error: {err}"
	);
}

#[tokio::test]
async fn query_coordinator_insert_select_rejects_runtime_null_and_cast_overflow_before_write() {
	for (setup, insert_sql, diagnostic) in [
		(
			vec![
				"CREATE TABLE src (id INT)",
				"CREATE TABLE dst (id INT NOT NULL)",
				"INSERT INTO src VALUES (NULL)",
			],
			"INSERT INTO dst SELECT id FROM src",
			"non-nullable column",
		),
		(
			vec![
				"CREATE TABLE src (id BIGINT NOT NULL)",
				"CREATE TABLE dst (id INT NOT NULL)",
				"INSERT INTO src VALUES (2147483648)",
			],
			"INSERT INTO dst SELECT id FROM src",
			"cast failed",
		),
	] {
		let catalog = Arc::new(MockCatalog::new());
		let storage = Arc::new(InMemoryTableStorage::new());
		let executor = Executor::new(storage.clone());
		let coordinator = QueryCoordinator::with_writer(
			catalog,
			executor,
			PlannerConfig::default(),
			storage.clone(),
		);
		let planner = SqlPlanner::new();

		for sql in setup {
			coordinator
				.execute_query(&planner.plan(sql).expect("plan SQL"), None)
				.await
				.expect(sql);
		}

		let err = coordinator
			.execute_query(&planner.plan(insert_sql).expect("plan insert select"), None)
			.await
			.expect_err("insert select should reject before write");
		assert!(
			err.to_string().contains(diagnostic),
			"expected diagnostic {diagnostic:?}, got {err}"
		);
		let select_err = coordinator
			.execute_query(
				&planner.plan("SELECT id FROM dst").expect("plan select"),
				None,
			)
			.await
			.expect_err("target should not be written");
		assert!(select_err.to_string().contains("table not found"));
	}
}

#[tokio::test]
async fn query_coordinator_insert_select_enforces_total_row_cap_before_write() {
	let catalog = Arc::new(MockCatalog::new());
	let storage = Arc::new(InMemoryTableStorage::new());
	let executor = Executor::new(storage.clone());
	let coordinator = QueryCoordinator::with_writer(
		catalog.clone(),
		executor,
		PlannerConfig::default(),
		storage.clone(),
	);
	let planner = SqlPlanner::new();

	for sql in [
		"CREATE TABLE src (id INT NOT NULL)",
		"CREATE TABLE dst (id INT NOT NULL)",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan SQL"), None)
			.await
			.expect(sql);
	}

	let binding = catalog
		.resolve_table(&TableRef::new("src"))
		.await
		.expect("resolve src");
	let payload = RecordBatch::try_new(
		binding.schema.clone(),
		vec![Arc::new(Int32Array::from(
			(0..=TABLE_WRITE_MAX_ROWS as i32).collect::<Vec<_>>(),
		))],
	)
	.expect("source payload");
	storage
		.write(write_request(&binding, payload))
		.await
		.expect("seed oversized source");

	let err = coordinator
		.execute_query(
			&planner
				.plan("INSERT INTO dst SELECT id FROM src")
				.expect("plan insert select"),
			None,
		)
		.await
		.expect_err("row cap should reject");
	assert!(
		err.to_string().contains("exceeds maximum"),
		"unexpected error: {err}"
	);
	let select_err = coordinator
		.execute_query(
			&planner.plan("SELECT id FROM dst").expect("plan select"),
			None,
		)
		.await
		.expect_err("target should not be written");
	assert!(select_err.to_string().contains("table not found"));
}

#[tokio::test]
async fn in_memory_table_storage_seeds_appends_and_rejects_invalid_writes() {
	let binding = people_catalog()
		.await
		.resolve_table(&TableRef::new("people"))
		.await
		.expect("resolve people");
	let storage = Arc::new(InMemoryTableStorage::new());

	let mut missing_scan = scan_request(&binding);
	missing_scan.table_id = TableId(binding.local_table_id.0 + 1000);
	let missing_err = match storage.scan(missing_scan).await {
		Ok(_) => panic!("missing table scan should reject"),
		Err(err) => err,
	};
	assert!(matches!(
		missing_err,
		StorageError::TableNotFound { table } if table == TableId(binding.local_table_id.0 + 1000)
	));

	let first = storage
		.write(write_request(
			&binding,
			people_batch(binding.schema.clone(), vec![1], vec![Some("Alice")]),
		))
		.await
		.expect("seed table");
	let second = storage
		.write(write_request(
			&binding,
			people_batch(binding.schema.clone(), vec![2], vec![Some("Bob")]),
		))
		.await
		.expect("append table");
	assert_eq!(first.rows, 1);
	assert_eq!(second.rows, 1);

	let batches = Executor::new(storage.clone())
		.execute_bound(&scan_plan(&binding), None)
		.await
		.expect("scan appended table");
	let batch =
		arrow::compute::concat_batches(&binding.schema, batches.iter()).expect("concat scan");
	assert_eq!(batch.num_rows(), 2);
	let ids = batch
		.column(0)
		.as_any()
		.downcast_ref::<Int32Array>()
		.expect("id column");
	let names = batch
		.column(1)
		.as_any()
		.downcast_ref::<StringArray>()
		.expect("name column");
	assert_eq!(ids.value(0), 1);
	assert_eq!(ids.value(1), 2);
	assert_eq!(names.value(0), "Alice");
	assert_eq!(names.value(1), "Bob");

	let bad_schema_err = storage
		.write(write_request(&binding, mismatched_people_batch()))
		.await
		.expect_err("schema mismatch should reject");
	assert!(
		matches!(bad_schema_err, StorageError::Internal { ref message } if message.contains("schema does not match")),
		"unexpected error: {bad_schema_err}"
	);

	let mut bad_identity = write_request(
		&binding,
		people_batch(binding.schema.clone(), vec![3], vec![Some("Carol")]),
	);
	bad_identity.table_object_id = TableObjectId::new([0xED; 16]);
	let bad_identity_err = storage
		.write(bad_identity)
		.await
		.expect_err("identity mismatch should reject");
	assert!(matches!(
		bad_identity_err,
		StorageError::DescriptorEvolved {
			table_id: TableId(1)
		}
	));
}

#[tokio::test]
async fn query_coordinator_default_writer_rejects_insert() {
	let catalog = Arc::new(people_catalog().await);
	let executor = Executor::new(empty_storage());
	let coordinator = QueryCoordinator::new(catalog, executor, PlannerConfig::default());
	let plan = SqlPlanner::new()
		.plan("INSERT INTO people (id, name) VALUES (1, 'Alice')")
		.expect("plan insert");

	let err = coordinator
		.execute_query(&plan, None)
		.await
		.expect_err("default writer should reject insert");

	assert!(
		err.to_string()
			.contains("INSERT execution requires a configured StorageWrite"),
		"unexpected error: {err}"
	);
}

#[tokio::test]
async fn distributed_query_coordinator_default_writer_rejects_insert_values() {
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
		.plan("INSERT INTO people (id, name) VALUES (1, 'Alice')")
		.expect("plan insert");

	let err = coordinator
		.execute_query(&plan)
		.await
		.expect_err("distributed insert should reach writer boundary");

	assert!(
		err.to_string()
			.contains("INSERT execution requires a configured StorageWrite"),
		"unexpected error: {err}"
	);
}

#[tokio::test]
async fn distributed_query_coordinator_insert_select_uses_distributed_source_path() {
	let catalog = Arc::new(insert_select_catalog().await);
	let coordinator = DistributedQueryCoordinator::new(
		catalog,
		Arc::new(StaticWorkerRegistry::new([])),
		Arc::new(MemoryManager::new(1, 1024 * 1024)),
		Arc::new(FailingTransport),
		PlannerConfig::default(),
		QueryBudget::test_default(),
	);
	let plan = SqlPlanner::new()
		.plan("INSERT INTO dst (a, b) SELECT x, y FROM src")
		.expect("plan insert select");

	let err = coordinator
		.execute_query(&plan)
		.await
		.expect_err("distributed insert select should request source workers");

	assert!(
		err.to_string().contains("no live worker available"),
		"unexpected error: {err}"
	);
	assert!(
		!err.to_string()
			.contains("distributed INSERT INTO ... SELECT execution is not wired"),
		"old coordinator reject should be lifted: {err}"
	);
}

#[tokio::test]
async fn executor_rejects_direct_insert_execution() {
	let executor = Executor::with_context(
		empty_storage(),
		ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(1),
			TenantId(1),
			HlcBound(1),
		),
	);
	let binding = people_catalog()
		.await
		.resolve_table(&TableRef::new("people"))
		.await
		.expect("resolve people");
	let plan = ExecutionPlan::Insert {
		local_table_id: binding.local_table_id,
		table: binding.table,
		fingerprint: binding.fingerprint,
		schema: binding.schema,
		rows: vec![vec![
			ScalarValue::Int32(Some(1)),
			ScalarValue::Utf8(Some("Alice".to_string())),
		]],
		returning: None,
	};

	let err = executor
		.execute_bound(&plan, None)
		.await
		.expect_err("executor should reject insert");

	assert!(matches!(
		err,
		ExecutionError::NotImplemented {
			feature: "INSERT execution is coordinator-only"
		}
	));
}

#[tokio::test]
async fn executor_rejects_direct_insert_select_execution() {
	let executor = Executor::with_context(
		empty_storage(),
		ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(1),
			TenantId(1),
			HlcBound(1),
		),
	);
	let catalog = insert_select_catalog().await;
	let src = catalog
		.resolve_table(&TableRef::new("src"))
		.await
		.expect("resolve src");
	let dst = catalog
		.resolve_table(&TableRef::new("dst"))
		.await
		.expect("resolve dst");
	let plan = insert_select_plan(&dst, scan_plan(&src));

	let err = executor
		.execute_bound(&plan, None)
		.await
		.expect_err("executor should reject insert select");

	assert!(matches!(
		err,
		ExecutionError::NotImplemented {
			feature: "INSERT INTO ... SELECT execution is coordinator-only"
		}
	));
}

#[tokio::test]
async fn fragmenter_rejects_direct_insert_fragmentation() {
	let binding = people_catalog()
		.await
		.resolve_table(&TableRef::new("people"))
		.await
		.expect("resolve people");
	let plan = ExecutionPlan::Insert {
		local_table_id: binding.local_table_id,
		table: binding.table,
		fingerprint: binding.fingerprint,
		schema: binding.schema,
		rows: vec![vec![
			ScalarValue::Int32(Some(1)),
			ScalarValue::Utf8(Some("Alice".to_string())),
		]],
		returning: None,
	};

	let err = fragment_plan(
		plan,
		FragmentPlanConfig::new(
			QueryId::from_u128(1),
			TaskBudget::from_query_budget(QueryBudget::test_default()),
		),
	)
	.expect_err("fragmenter should reject insert");

	assert_eq!(err, FragmentError::InsertNotFragmentable);
}

#[tokio::test]
async fn fragmenter_rejects_direct_insert_select_fragmentation() {
	let catalog = insert_select_catalog().await;
	let src = catalog
		.resolve_table(&TableRef::new("src"))
		.await
		.expect("resolve src");
	let dst = catalog
		.resolve_table(&TableRef::new("dst"))
		.await
		.expect("resolve dst");
	let plan = insert_select_plan(&dst, scan_plan(&src));

	let err = fragment_plan(
		plan,
		FragmentPlanConfig::new(
			QueryId::from_u128(1),
			TaskBudget::from_query_budget(QueryBudget::test_default()),
		),
	)
	.expect_err("fragmenter should reject insert select");

	assert_eq!(err, FragmentError::InsertSelectNotFragmentable);
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

async fn insert_select_catalog() -> MockCatalog {
	let catalog = MockCatalog::new();
	catalog
		.create_table(
			&TableRef::new("src"),
			schema(&[
				Field::new("x", DataType::Int32, false),
				Field::new("y", DataType::Int32, false),
				Field::new("label", DataType::Utf8, true),
			]),
			false,
		)
		.await
		.expect("create src");
	catalog
		.create_table(
			&TableRef::new("dst"),
			schema(&[
				Field::new("a", DataType::Int32, false),
				Field::new("b", DataType::Int32, true),
			]),
			false,
		)
		.await
		.expect("create dst");
	catalog
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

fn schema(fields: &[Field]) -> SchemaRef {
	Arc::new(Schema::new(fields.to_vec()))
}

fn empty_storage() -> Arc<InMemoryStorageScan> {
	let schema = Arc::new(Schema::empty());
	let batch = RecordBatch::new_empty(schema);
	Arc::new(InMemoryStorageScan { table_data: batch })
}

fn write_request(binding: &TableBinding, payload: RecordBatch) -> WriteRequest {
	WriteRequest {
		session_id: SessionId(1),
		query_id: QueryId::from_u128(1),
		tenant_id: TenantId(1),
		table_id: binding.local_table_id,
		table_object_id: binding.table,
		physical_descriptor_fingerprint: binding.fingerprint,
		payload,
		profile_hint: None,
		trace_context: None,
		operation_id: None,
		deadline: None,
	}
}

fn scan_request(binding: &TableBinding) -> ScanRequest {
	ScanRequest {
		session_id: SessionId(1),
		query_id: QueryId::from_u128(1),
		tenant_id: TenantId(1),
		table_id: binding.local_table_id,
		table_object_id: binding.table,
		physical_descriptor_fingerprint: binding.fingerprint,
		read_bound: HlcBound(1),
		projected_columns: None,
		pbc_predicate: None,
		runtime_filter: None,
		row_identity: RowIdentityProjection::None,
		profile_hint: None,
		trace_context: None,
		morsel: None,
		row_limit: None,
		downstream_cancellation: None,
		deadline: None,
	}
}

fn scan_plan(binding: &TableBinding) -> ExecutionPlan {
	ExecutionPlan::Scan {
		local_table_id: binding.local_table_id,
		table: binding.table,
		fingerprint: binding.fingerprint,
		schema: binding.schema.clone(),
		projection: None,
		pbc_program: None,
		row_limit: None,
		row_identity: toron::storage::RowIdentityProjection::None,
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

fn insert_select_plan(binding: &TableBinding, source: ExecutionPlan) -> ExecutionPlan {
	ExecutionPlan::InsertSelect {
		local_table_id: binding.local_table_id,
		table: binding.table,
		fingerprint: binding.fingerprint,
		schema: binding.schema.clone(),
		target_columns: vec![ColumnOrdinal(0), ColumnOrdinal(1)],
		source: Box::new(source),
		returning: None,
	}
}

fn people_batch(schema: SchemaRef, ids: Vec<i32>, names: Vec<Option<&str>>) -> RecordBatch {
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int32Array::from(ids)),
			Arc::new(StringArray::from(names)),
		],
	)
	.expect("people batch")
}

fn mismatched_people_batch() -> RecordBatch {
	RecordBatch::try_new(
		schema(&[
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, true),
		]),
		vec![
			Arc::new(Int64Array::from(vec![3_i64])),
			Arc::new(StringArray::from(vec![Some("Carol")])),
		],
	)
	.expect("mismatched people batch")
}

struct FailingTransport;

#[async_trait]
impl ToronTransport for FailingTransport {
	async fn connect(&self, _address: &str) -> Result<Box<dyn ToronStream>, QueryError> {
		Err(QueryError::Internal {
			message: "transport should not be used for INSERT".to_string(),
		})
	}
}
