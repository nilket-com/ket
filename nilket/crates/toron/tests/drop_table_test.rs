use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use arrow::array::{Array, Int32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use toron::binder::{PlannerConfig, bind};
use toron::catalog::{Catalog, MockCatalog, TableBinding, TableRef, WendelonCatalog};
use toron::coordinator::{DistributedQueryCoordinator, QueryCoordinator};
use toron::distributed::budget::{QueryBudget, TaskBudget};
use toron::distributed::fragment::{FragmentError, FragmentPlanConfig, fragment_plan};
use toron::distributed::identity::QueryId;
use toron::distributed::worker_registry::StaticWorkerRegistry;
use toron::error::{CatalogError, ExecutionError, QueryError, StorageError};
use toron::exec::{ExecutionContext, Executor};
use toron::ir::{ExecutionPlan, LogicalPlan, SessionId, TableId};
use toron::server::memory::MemoryManager;
use toron::server::transport::{ToronStream, ToronTransport};
use toron::sql_planner::SqlPlanner;
use toron::storage::{
	DropTableStorageOutcome, DropTableStorageRequest, HlcBound, InMemoryStorageScan,
	InMemoryTableStorage, StorageAdmin, TenantId,
};
use wendelon::schema::{
	LocalityPolicy, MultiRowLayoutDescriptor, RecordLayoutPolicy, RowIdentityPolicy,
	SchemaEvolutionPolicy, TableObjectId, TableSchemaDescriptor,
};

#[test]
fn planner_accepts_drop_table_surface() {
	let plan = SqlPlanner::new()
		.plan("DROP TABLE sales")
		.expect("plan drop table");
	let LogicalPlan::DropTable { name, if_exists } = plan else {
		panic!("expected drop table plan");
	};
	assert_eq!(name, TableRef::new("sales"));
	assert!(!if_exists);

	let plan = SqlPlanner::new()
		.plan("DROP TABLE IF EXISTS app.sales")
		.expect("plan drop table if exists");
	let LogicalPlan::DropTable { name, if_exists } = plan else {
		panic!("expected drop table plan");
	};
	assert_eq!(name, TableRef::with_schema("app", "sales"));
	assert!(if_exists);
}

#[test]
fn planner_rejects_unsupported_drop_table_shapes() {
	for (sql, diagnostic) in [
		("DROP VIEW v", "only TABLE is supported"),
		("DROP TABLE a, b", "exactly one target table"),
		("DROP TABLE sales CASCADE", "CASCADE"),
		("DROP TABLE sales RESTRICT", "RESTRICT"),
		("DROP TABLE sales PURGE", "PURGE"),
		("DROP TEMPORARY TABLE sales", "TEMPORARY"),
		("DROP TABLE a.b.c", "only one- or two-part"),
	] {
		assert_frontend_error(sql, diagnostic);
	}
}

#[tokio::test]
async fn binder_emits_identity_free_drop_table() {
	let catalog = MockCatalog::new();
	let plan = SqlPlanner::new()
		.plan("DROP TABLE IF EXISTS app.sales")
		.expect("plan drop table");

	let bound = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("bind drop table");
	let ExecutionPlan::DropTable { name, if_exists } = bound else {
		panic!("expected drop table execution plan");
	};
	assert_eq!(name, TableRef::with_schema("app", "sales"));
	assert!(if_exists);
}

#[tokio::test]
async fn mock_catalog_drop_table_semantics() {
	let catalog = MockCatalog::new();
	let name = TableRef::new("sales");
	let created = catalog
		.create_table(
			&name,
			schema(&[Field::new("id", DataType::Int32, false)]),
			false,
		)
		.await
		.expect("create table");

	let dropped = catalog
		.drop_table(&name, false)
		.await
		.expect("drop table")
		.expect("existing table returns binding");
	assert_eq!(dropped.local_table_id, created.local_table_id);
	assert!(catalog.resolve_table(&name).await.is_err());

	let missing = expect_drop_catalog_err(catalog.drop_table(&name, false).await);
	assert!(matches!(
		missing,
		CatalogError::TableNotFound { ref table } if table == "sales"
	));

	let absent = catalog
		.drop_table(&name, true)
		.await
		.expect("drop missing if exists");
	assert!(absent.is_none());
}

#[tokio::test]
async fn mock_catalog_drop_table_schema_alias_cleanup_is_exact() {
	let catalog = MockCatalog::new();
	let app_name = TableRef::with_schema("app", "sales");
	let other_name = TableRef::with_schema("other", "sales");
	let schema = schema(&[Field::new("id", DataType::Int32, false)]);

	let app = catalog
		.create_table(&app_name, schema.clone(), false)
		.await
		.expect("create app.sales");
	let other = catalog
		.create_table(&other_name, schema, false)
		.await
		.expect("create other.sales");
	let unqualified = catalog
		.resolve_table(&TableRef::new("sales"))
		.await
		.expect("resolve unqualified alias");
	assert_eq!(unqualified.local_table_id, app.local_table_id);

	catalog
		.drop_table(&app_name, false)
		.await
		.expect("drop app.sales");

	assert!(catalog.resolve_table(&app_name).await.is_err());
	assert!(
		catalog
			.resolve_table(&TableRef::new("sales"))
			.await
			.is_err()
	);
	let still_other = catalog
		.resolve_table(&other_name)
		.await
		.expect("other.sales remains");
	assert_eq!(still_other.local_table_id, other.local_table_id);
}

#[tokio::test]
async fn query_coordinator_executes_drop_table() {
	let (catalog, storage, coordinator, planner) = coordinator();

	for sql in [
		"CREATE TABLE sales (id INT NOT NULL)",
		"INSERT INTO sales VALUES (1)",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan setup SQL"), None)
			.await
			.expect(sql);
	}
	let before_drop = concat_query(&coordinator, &planner, "SELECT id FROM sales").await;
	assert_eq!(int32_column(&before_drop, 0).values(), &[1]);
	assert_eq!(storage.table_count(), 1);

	let dropped = coordinator
		.execute_query(
			&planner.plan("DROP TABLE sales").expect("plan drop table"),
			None,
		)
		.await
		.expect("execute drop table");
	assert!(dropped.is_empty());
	assert_eq!(storage.table_count(), 0);

	let err = coordinator
		.execute_query(
			&planner.plan("SELECT id FROM sales").expect("plan select"),
			None,
		)
		.await
		.expect_err("select after drop should fail at bind");
	assert!(
		err.to_string().contains("table not found"),
		"unexpected error: {err}"
	);

	assert!(
		catalog
			.resolve_table(&TableRef::new("sales"))
			.await
			.is_err()
	);
	assert!(
		coordinator
			.execute_query(
				&planner
					.plan("DROP TABLE IF EXISTS missing")
					.expect("plan drop missing"),
				None,
			)
			.await
			.expect("drop missing if exists")
			.is_empty()
	);
}

#[tokio::test]
async fn query_coordinator_recreate_after_drop_isolated() {
	let (_catalog, _storage, coordinator, planner) = coordinator();

	for sql in [
		"CREATE TABLE sales (id INT NOT NULL)",
		"INSERT INTO sales VALUES (1)",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan setup SQL"), None)
			.await
			.expect(sql);
	}
	coordinator
		.execute_query(
			&planner.plan("DROP TABLE sales").expect("plan drop table"),
			None,
		)
		.await
		.expect("drop table");
	for sql in [
		"CREATE TABLE sales (id INT NOT NULL)",
		"INSERT INTO sales VALUES (2)",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan recreate SQL"), None)
			.await
			.expect(sql);
	}

	let after_recreate = concat_query(&coordinator, &planner, "SELECT id FROM sales").await;
	assert_eq!(int32_column(&after_recreate, 0).values(), &[2]);
}

#[tokio::test]
async fn query_coordinator_ctas_cleanup_allows_rerun() {
	let (_catalog, _storage, coordinator, planner) = coordinator();

	for sql in [
		"CREATE TABLE src (id INT NOT NULL)",
		"INSERT INTO src VALUES (1), (2)",
		"CREATE TABLE copy AS SELECT id FROM src",
		"DROP TABLE copy",
		"CREATE TABLE copy AS SELECT id FROM src",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan SQL"), None)
			.await
			.expect(sql);
	}

	let copy = concat_query(&coordinator, &planner, "SELECT id FROM copy ORDER BY id").await;
	assert_eq!(int32_column(&copy, 0).values(), &[1, 2]);
}

#[tokio::test]
async fn distributed_coordinator_executes_drop_table_before_fragment() {
	let catalog = Arc::new(MockCatalog::new());
	catalog
		.create_table(
			&TableRef::new("sales"),
			schema(&[Field::new("id", DataType::Int32, false)]),
			false,
		)
		.await
		.expect("create table");
	let admin = Arc::new(SpyStorageAdmin::new());
	let coordinator = DistributedQueryCoordinator::new(
		catalog.clone(),
		Arc::new(StaticWorkerRegistry::new([])),
		Arc::new(MemoryManager::new(1, 1024 * 1024)),
		Arc::new(FailingTransport),
		PlannerConfig::default(),
		QueryBudget::test_default(),
	)
	.with_storage_admin(admin.clone());
	let plan = SqlPlanner::new()
		.plan("DROP TABLE sales")
		.expect("plan drop table");

	let results = coordinator
		.execute_query(&plan)
		.await
		.expect("distributed drop table");

	assert!(results.is_empty());
	assert_eq!(admin.call_count(), 1);
	assert!(
		catalog
			.resolve_table(&TableRef::new("sales"))
			.await
			.is_err()
	);
}

#[tokio::test]
async fn drop_table_if_exists_missing_does_not_call_storage_admin() {
	let catalog = Arc::new(MockCatalog::new());
	let storage = Arc::new(InMemoryTableStorage::new());
	let spy = Arc::new(SpyStorageAdmin::new());
	let executor = Executor::new(storage.clone());
	let coordinator =
		QueryCoordinator::with_writer(catalog, executor, PlannerConfig::default(), storage)
			.with_storage_admin(spy.clone());
	let planner = SqlPlanner::new();

	let result = coordinator
		.execute_query(
			&planner
				.plan("DROP TABLE IF EXISTS missing")
				.expect("plan drop missing"),
			None,
		)
		.await
		.expect("drop missing if exists");

	assert!(result.is_empty());
	assert_eq!(spy.call_count(), 0);
}

#[tokio::test]
async fn drop_table_storage_failure_preserves_catalog_reachability() {
	let catalog = Arc::new(MockCatalog::new());
	let storage = Arc::new(InMemoryTableStorage::new());
	let failing = Arc::new(AlwaysFailStorageAdmin);
	let executor = Executor::new(storage.clone());
	let coordinator = QueryCoordinator::with_writer_deleter_and_admin(
		catalog.clone(),
		executor,
		PlannerConfig::default(),
		storage.clone(),
		storage,
		failing,
	);
	let planner = SqlPlanner::new();

	for sql in [
		"CREATE TABLE sales (id INT NOT NULL)",
		"INSERT INTO sales VALUES (1)",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan setup SQL"), None)
			.await
			.expect(sql);
	}

	let first = coordinator
		.execute_query(
			&planner.plan("DROP TABLE sales").expect("plan drop table"),
			None,
		)
		.await
		.expect_err("storage failure should surface");
	assert!(matches!(
		first,
		QueryError::Storage(StorageError::Internal { .. })
	));
	assert!(catalog.resolve_table(&TableRef::new("sales")).await.is_ok());

	let retry = coordinator
		.execute_query(&planner.plan("DROP TABLE sales").expect("plan retry"), None)
		.await
		.expect_err("retry still fails at storage");
	assert!(matches!(
		retry,
		QueryError::Storage(StorageError::Internal { .. })
	));
}

#[tokio::test]
async fn drop_table_stale_scan_returns_table_not_found() {
	let catalog = Arc::new(MockCatalog::new());
	let storage = Arc::new(InMemoryTableStorage::new());
	let executor = Executor::new(storage.clone());
	let coordinator = QueryCoordinator::with_writer_deleter_and_admin(
		catalog.clone(),
		Executor::new(storage.clone()),
		PlannerConfig::default(),
		storage.clone(),
		storage.clone(),
		storage.clone(),
	);
	let planner = SqlPlanner::new();

	for sql in [
		"CREATE TABLE sales (id INT NOT NULL)",
		"INSERT INTO sales VALUES (1)",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan setup SQL"), None)
			.await
			.expect(sql);
	}

	let select = planner.plan("SELECT id FROM sales").expect("plan select");
	let bound = bind(&select, catalog.as_ref(), &PlannerConfig::default())
		.await
		.expect("bind select");
	let first = executor
		.execute_bound(&bound, None)
		.await
		.expect("first execute");
	assert_eq!(int32_column(&first[0], 0).values(), &[1]);

	coordinator
		.execute_query(
			&planner.plan("DROP TABLE sales").expect("plan drop table"),
			None,
		)
		.await
		.expect("drop table");

	let stale = executor
		.execute_bound(&bound, None)
		.await
		.expect_err("stale scan should fail");
	assert!(matches!(
		stale,
		ExecutionError::Storage(StorageError::TableNotFound { .. })
	));
}

#[tokio::test]
async fn drop_with_no_admin_sink_returns_capability_diagnostic() {
	let catalog = Arc::new(MockCatalog::new());
	let storage = Arc::new(InMemoryTableStorage::new());
	let executor = Executor::new(storage.clone());
	let coordinator =
		QueryCoordinator::with_writer(catalog, executor, PlannerConfig::default(), storage);
	let planner = SqlPlanner::new();

	for sql in [
		"CREATE TABLE sales (id INT NOT NULL)",
		"INSERT INTO sales VALUES (1)",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan setup SQL"), None)
			.await
			.expect(sql);
	}

	let err = coordinator
		.execute_query(
			&planner.plan("DROP TABLE sales").expect("plan drop table"),
			None,
		)
		.await
		.expect_err("default admin sink should reject");
	assert!(matches!(
		err,
		QueryError::Storage(StorageError::Internal { ref message })
			if message == "DROP TABLE physical reclaim requires a configured StorageAdmin"
	));
}

#[tokio::test]
async fn drop_admin_failure_then_retry_succeeds() {
	let catalog = Arc::new(MockCatalog::new());
	let storage = Arc::new(InMemoryTableStorage::new());
	let toggle = Arc::new(ToggleStorageAdmin::new(storage.clone()));
	let executor = Executor::new(storage.clone());
	let coordinator = QueryCoordinator::with_writer_deleter_and_admin(
		catalog.clone(),
		executor,
		PlannerConfig::default(),
		storage.clone(),
		storage,
		toggle,
	);
	let planner = SqlPlanner::new();

	for sql in [
		"CREATE TABLE sales (id INT NOT NULL)",
		"INSERT INTO sales VALUES (1)",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan setup SQL"), None)
			.await
			.expect(sql);
	}

	let first = coordinator
		.execute_query(
			&planner.plan("DROP TABLE sales").expect("plan drop table"),
			None,
		)
		.await
		.expect_err("first admin attempt fails");
	assert!(matches!(
		first,
		QueryError::Storage(StorageError::Internal { .. })
	));
	assert!(catalog.resolve_table(&TableRef::new("sales")).await.is_ok());

	coordinator
		.execute_query(&planner.plan("DROP TABLE sales").expect("plan retry"), None)
		.await
		.expect("retry succeeds");
	assert!(
		catalog
			.resolve_table(&TableRef::new("sales"))
			.await
			.is_err()
	);
}

#[tokio::test]
async fn drop_uses_storage_admin_before_catalog_removal() {
	// This exercises `drop_existing_table_with_storage_admin`, which OR REPLACE
	// used to reach as well. Destructive replacement is refused until staged
	// publication exists, so the ordering is asserted through the explicit DROP
	// that still reaches the same helper.
	let catalog = Arc::new(MockCatalog::new());
	let storage = Arc::new(InMemoryTableStorage::new());
	let admin = Arc::new(CatalogVisibilityStorageAdmin::new(catalog.clone()));
	let executor = Executor::new(storage.clone());
	let coordinator = QueryCoordinator::with_writer_deleter_and_admin(
		catalog.clone(),
		executor,
		PlannerConfig::default(),
		storage.clone(),
		storage,
		admin.clone(),
	);
	let planner = SqlPlanner::new();

	coordinator
		.execute_query(
			&planner
				.plan("CREATE TABLE sales (id INT NOT NULL)")
				.expect("plan create"),
			None,
		)
		.await
		.expect("create table");
	coordinator
		.execute_query(&planner.plan("DROP TABLE sales").expect("plan drop"), None)
		.await
		.expect("drop table");

	assert_eq!(admin.call_count(), 1);
	assert!(
		admin.saw_catalog_binding_during_admin(),
		"catalog binding should still be reachable while storage admin drop runs"
	);
	assert!(
		catalog
			.resolve_table(&TableRef::new("sales"))
			.await
			.is_err(),
		"the dropped table must no longer resolve"
	);
}

#[tokio::test]
async fn wendelon_dependent_index_reject_propagates_through_drop() {
	// Same helper as the withdrawn OR REPLACE path; the rejection and the
	// table's survival are asserted through the explicit DROP.
	let catalog = Arc::new(MockCatalog::new());
	let storage = Arc::new(InMemoryTableStorage::new());
	let admin = Arc::new(DependentIndexStorageAdmin);
	let executor = Executor::new(storage.clone());
	let coordinator = QueryCoordinator::with_writer_deleter_and_admin(
		catalog.clone(),
		executor,
		PlannerConfig::default(),
		storage.clone(),
		storage,
		admin,
	);
	let planner = SqlPlanner::new();

	coordinator
		.execute_query(
			&planner
				.plan("CREATE TABLE sales (id INT NOT NULL)")
				.expect("plan create"),
			None,
		)
		.await
		.expect("create table");

	let err = coordinator
		.execute_query(&planner.plan("DROP TABLE sales").expect("plan drop"), None)
		.await
		.expect_err("dependent index rejection should propagate");

	assert!(matches!(
		err,
		QueryError::Storage(StorageError::Internal { ref message })
			if message.contains("active dependent indexes") && message.contains("drop them first")
	));
	assert!(
		catalog.resolve_table(&TableRef::new("sales")).await.is_ok(),
		"storage-admin rejection must preserve catalog reachability"
	);
}

#[test]
fn wendelon_create_row_identity_policy_is_request_driven_after_adr062() {
	let source = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/catalog.rs"))
		.expect("read catalog source");

	assert!(source.contains("CreateTableRowIdentity::AppendOnly"));
	assert!(source.contains("CreateTableRowIdentity::PrimaryKey"));
	assert!(source.contains("RowIdentityPolicy::AppendOnly"));
	assert!(source.contains("RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single"));
	assert!(!source.contains("RowIdentityPolicy::ByOrdinal"));
}

/// Wendelon ADR 0118 D6: a Wendelon table's local binding is never removed by
/// name; it is removed by the binding the statement captured, and a replay of
/// the same completion is a no-op.
#[tokio::test]
async fn wendelon_catalog_drop_table_semantics() {
	let name = TableRef::with_schema("bench", "orders");
	let descriptor = mock_descriptor(
		&name,
		schema(&[Field::new("id", DataType::Int32, false)]).as_ref(),
		table_object_id(TableId(7)),
	);
	let catalog = WendelonCatalog::from_descriptors([(TableId(7), descriptor.clone())])
		.expect("wendelon catalog");

	let by_name = expect_drop_catalog_err(catalog.drop_table(&name, true).await);
	assert!(matches!(by_name, CatalogError::AdminFailed { .. }));
	assert!(by_name.to_string().contains("not removed by name"));
	let binding = catalog
		.resolve_table(&name)
		.await
		.expect("a refused removal by name keeps the binding");

	catalog
		.complete_table_drop(&binding)
		.await
		.expect("completion by the captured binding");
	assert!(catalog.resolve_table(&name).await.is_err());
	catalog
		.complete_table_drop(&binding)
		.await
		.expect("a replayed completion is a no-op");

	let missing = catalog
		.drop_table(&TableRef::new("missing"), true)
		.await
		.expect("missing if exists");
	assert!(missing.is_none());
	let err = expect_drop_catalog_err(catalog.drop_table(&TableRef::new("missing"), false).await);
	assert!(matches!(
		err,
		CatalogError::TableNotFound { ref table } if table == "missing"
	));
}

/// Wendelon ADR 0118 D6: while the storage DROP of the captured binding is in
/// flight, the name is rebound to a fresh table (another object, another local
/// id, as a descriptor refresh after a re-create installs it). The original
/// DROP finishes after its authoritative storage success, and the fresh binding
/// stays intact: the completion never resolves the name.
#[tokio::test]
async fn a_delayed_drop_completion_across_a_name_reuse_leaves_the_fresh_binding() {
	let name = TableRef::with_schema("bench", "orders");
	let fields = schema(&[Field::new("id", DataType::Int32, false)]);
	let old = mock_descriptor(&name, fields.as_ref(), table_object_id(TableId(7)));
	let fresh = mock_descriptor(&name, fields.as_ref(), table_object_id(TableId(8)));
	let catalog =
		Arc::new(WendelonCatalog::from_descriptors([(TableId(7), old)]).expect("wendelon catalog"));
	let admin = Arc::new(RebindingStorageAdmin {
		catalog: catalog.clone(),
		name: name.clone(),
		fresh: Some(fresh.clone()),
	});
	let coordinator = wendelon_drop_coordinator(catalog.clone(), admin);
	coordinator
		.execute_query(
			&SqlPlanner::new()
				.plan("DROP TABLE bench.orders")
				.expect("plan drop"),
			None,
		)
		.await
		.expect("the original DROP finishes after its storage success");
	let bound = catalog
		.resolve_table(&name)
		.await
		.expect("the fresh binding stays");
	assert_eq!(bound.table, fresh.table_object_id, "the fresh object");
}

/// Wendelon ADR 0118 D6: the captured local entry is gone by the time the
/// authoritative storage DROP answers (a refresh removed it). The DROP finishes;
/// nothing else is removed.
#[tokio::test]
async fn a_drop_completion_after_its_local_entry_left_finishes() {
	let name = TableRef::with_schema("bench", "orders");
	let fields = schema(&[Field::new("id", DataType::Int32, false)]);
	let old = mock_descriptor(&name, fields.as_ref(), table_object_id(TableId(7)));
	let other = TableRef::with_schema("bench", "other");
	let unrelated = mock_descriptor(&other, fields.as_ref(), table_object_id(TableId(9)));
	let catalog = Arc::new(
		WendelonCatalog::from_descriptors([(TableId(7), old), (TableId(9), unrelated)])
			.expect("wendelon catalog"),
	);
	let admin = Arc::new(RebindingStorageAdmin {
		catalog: catalog.clone(),
		name: name.clone(),
		fresh: None,
	});
	let coordinator = wendelon_drop_coordinator(catalog.clone(), admin);
	coordinator
		.execute_query(
			&SqlPlanner::new()
				.plan("DROP TABLE bench.orders")
				.expect("plan drop"),
			None,
		)
		.await
		.expect("the DROP finishes");
	assert!(catalog.resolve_table(&name).await.is_err());
	assert!(
		catalog.resolve_table(&other).await.is_ok(),
		"an unrelated binding stays"
	);
}

fn wendelon_drop_coordinator(
	catalog: Arc<WendelonCatalog>,
	admin: Arc<dyn StorageAdmin>,
) -> QueryCoordinator {
	let storage = Arc::new(InMemoryTableStorage::new());
	QueryCoordinator::with_writer_deleter_and_admin(
		catalog,
		Executor::new(storage.clone()),
		PlannerConfig::default(),
		storage.clone(),
		storage,
		admin,
	)
}

/// A storage admin whose DROP answers success after, in flight, the name is
/// rebound to `fresh` (Some) or the captured local entry is removed (None).
struct RebindingStorageAdmin {
	catalog: Arc<WendelonCatalog>,
	name: TableRef,
	fresh: Option<TableSchemaDescriptor>,
}

#[async_trait]
impl StorageAdmin for RebindingStorageAdmin {
	async fn drop_table_storage(
		&self,
		request: DropTableStorageRequest,
	) -> Result<DropTableStorageOutcome, StorageError> {
		let registry = self.catalog.descriptor_registry();
		registry.invalidate_descriptor_for_tenant(TenantId(1), request.table_id);
		if let Some(fresh) = &self.fresh {
			registry
				.sync_descriptor_for_tenant(TenantId(1), &self.name, fresh.clone())
				.expect("rebind the name to the fresh table");
		}
		Ok(DropTableStorageOutcome::new(1))
	}
}

#[tokio::test]
async fn executor_and_fragmenter_reject_direct_drop_table_execution() {
	let executor = Executor::with_context(
		empty_storage(),
		ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(1),
			TenantId(1),
			HlcBound(1),
		),
	);
	let plan = ExecutionPlan::DropTable {
		name: TableRef::new("sales"),
		if_exists: false,
	};

	let err = executor
		.execute_bound(&plan, None)
		.await
		.expect_err("executor should reject DROP TABLE");
	assert!(matches!(
		err,
		ExecutionError::NotImplemented {
			feature: "DROP TABLE execution is coordinator-only"
		}
	));

	let err = fragment_plan(
		plan,
		FragmentPlanConfig::new_for_test(
			QueryId::from_u128(1),
			TaskBudget::from_query_budget(QueryBudget::test_default()),
		),
	)
	.expect_err("fragmenter should reject DROP TABLE");
	assert_eq!(err, FragmentError::DropTableNotFragmentable);
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

struct SpyStorageAdmin {
	calls: AtomicUsize,
}

impl SpyStorageAdmin {
	fn new() -> Self {
		Self {
			calls: AtomicUsize::new(0),
		}
	}

	fn call_count(&self) -> usize {
		self.calls.load(Ordering::SeqCst)
	}
}

#[async_trait]
impl StorageAdmin for SpyStorageAdmin {
	async fn drop_table_storage(
		&self,
		_request: DropTableStorageRequest,
	) -> Result<DropTableStorageOutcome, StorageError> {
		self.calls.fetch_add(1, Ordering::SeqCst);
		Ok(DropTableStorageOutcome::new(0))
	}
}

struct AlwaysFailStorageAdmin;

#[async_trait]
impl StorageAdmin for AlwaysFailStorageAdmin {
	async fn drop_table_storage(
		&self,
		_request: DropTableStorageRequest,
	) -> Result<DropTableStorageOutcome, StorageError> {
		Err(StorageError::Internal {
			message: "admin failure".to_string(),
		})
	}
}

struct ToggleStorageAdmin {
	storage: Arc<InMemoryTableStorage>,
	failed: AtomicBool,
}

impl ToggleStorageAdmin {
	fn new(storage: Arc<InMemoryTableStorage>) -> Self {
		Self {
			storage,
			failed: AtomicBool::new(false),
		}
	}
}

#[async_trait]
impl StorageAdmin for ToggleStorageAdmin {
	async fn drop_table_storage(
		&self,
		request: DropTableStorageRequest,
	) -> Result<DropTableStorageOutcome, StorageError> {
		if !self.failed.swap(true, Ordering::SeqCst) {
			return Err(StorageError::Internal {
				message: "transient admin failure".to_string(),
			});
		}
		self.storage.drop_table_storage(request).await
	}
}

struct CatalogVisibilityStorageAdmin {
	catalog: Arc<MockCatalog>,
	calls: AtomicUsize,
	saw_catalog_binding: AtomicBool,
}

impl CatalogVisibilityStorageAdmin {
	fn new(catalog: Arc<MockCatalog>) -> Self {
		Self {
			catalog,
			calls: AtomicUsize::new(0),
			saw_catalog_binding: AtomicBool::new(false),
		}
	}

	fn call_count(&self) -> usize {
		self.calls.load(Ordering::SeqCst)
	}

	fn saw_catalog_binding_during_admin(&self) -> bool {
		self.saw_catalog_binding.load(Ordering::SeqCst)
	}
}

#[async_trait]
impl StorageAdmin for CatalogVisibilityStorageAdmin {
	async fn drop_table_storage(
		&self,
		request: DropTableStorageRequest,
	) -> Result<DropTableStorageOutcome, StorageError> {
		self.calls.fetch_add(1, Ordering::SeqCst);
		let visible = self
			.catalog
			.resolve_table_id(request.table_id)
			.await
			.is_ok();
		self.saw_catalog_binding.store(visible, Ordering::SeqCst);
		Ok(DropTableStorageOutcome::new(0))
	}
}

struct DependentIndexStorageAdmin;

#[async_trait]
impl StorageAdmin for DependentIndexStorageAdmin {
	async fn drop_table_storage(
		&self,
		_request: DropTableStorageRequest,
	) -> Result<DropTableStorageOutcome, StorageError> {
		Err(StorageError::Internal {
			message: "table sales has 1 active dependent indexes; drop them first".to_string(),
		})
	}
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

fn expect_drop_catalog_err(result: Result<Option<TableBinding>, CatalogError>) -> CatalogError {
	match result {
		Ok(_) => panic!("expected catalog error"),
		Err(error) => error,
	}
}

fn schema(fields: &[Field]) -> SchemaRef {
	Arc::new(Schema::new(fields.to_vec()))
}

fn empty_storage() -> Arc<InMemoryStorageScan> {
	let schema = Arc::new(Schema::empty());
	let batch = RecordBatch::new_empty(schema);
	Arc::new(InMemoryStorageScan { table_data: batch })
}

fn mock_descriptor(
	name: &TableRef,
	schema: &Schema,
	table: TableObjectId,
) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new_with_layout(
		table,
		1,
		name.schema.as_deref().unwrap_or("default"),
		&name.table,
		schema,
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("descriptor")
}

fn table_object_id(table_id: TableId) -> TableObjectId {
	let mut bytes = [0_u8; 16];
	bytes[8..].copy_from_slice(&table_id.0.to_be_bytes());
	TableObjectId::new(bytes)
}

fn int32_column(batch: &RecordBatch, index: usize) -> &Int32Array {
	batch
		.column(index)
		.as_any()
		.downcast_ref::<Int32Array>()
		.expect("Int32 column")
}

struct FailingTransport;

#[async_trait]
impl ToronTransport for FailingTransport {
	async fn connect(&self, _address: &str) -> Result<Box<dyn ToronStream>, QueryError> {
		Err(QueryError::Internal {
			message: "transport should not be used for DROP TABLE".to_string(),
		})
	}
}

/// Wendelon ADR 0118 (r2): a DROP blocked by a backup hold after its Prepare
/// (Fenced) fails and keeps the binding; the retry, a new statement on the same
/// name, still names the OLD object, and its success completes the drop.
#[tokio::test]
async fn a_hold_blocked_drop_retries_bound_to_the_old_object() {
	let name = TableRef::with_schema("bench", "orders");
	let fields = schema(&[Field::new("id", DataType::Int32, false)]);
	let old = mock_descriptor(&name, fields.as_ref(), table_object_id(TableId(7)));
	let catalog = Arc::new(
		WendelonCatalog::from_descriptors([(TableId(7), old.clone())]).expect("wendelon catalog"),
	);
	let admin = Arc::new(HoldOnceStorageAdmin {
		calls: std::sync::Mutex::new(Vec::new()),
	});
	let coordinator = wendelon_drop_coordinator(catalog.clone(), admin.clone());
	let plan = SqlPlanner::new()
		.plan("DROP TABLE bench.orders")
		.expect("plan drop");
	let blocked = coordinator
		.execute_query(&plan, None)
		.await
		.expect_err("blocked by the hold");
	assert!(
		matches!(
			blocked,
			QueryError::Storage(StorageError::BackupHoldActive { .. })
		),
		"{blocked:?}"
	);
	assert!(
		catalog.resolve_table(&name).await.is_ok(),
		"the binding is kept"
	);
	coordinator
		.execute_query(&plan, None)
		.await
		.expect("the retry completes the drop");
	assert!(catalog.resolve_table(&name).await.is_err());
	assert_eq!(
		*admin.calls.lock().unwrap(),
		vec![old.table_object_id, old.table_object_id],
		"both attempts name the old object"
	);
}

/// A storage admin whose first DROP is blocked by a hold after its Prepare.
struct HoldOnceStorageAdmin {
	calls: std::sync::Mutex<Vec<TableObjectId>>,
}

#[async_trait]
impl StorageAdmin for HoldOnceStorageAdmin {
	async fn drop_table_storage(
		&self,
		request: DropTableStorageRequest,
	) -> Result<DropTableStorageOutcome, StorageError> {
		let mut calls = self.calls.lock().unwrap();
		calls.push(request.table_object_id);
		if calls.len() == 1 {
			let progress = wendelon_client::HeldDropProgress::Fenced;
			return Err(StorageError::BackupHoldActive {
				progress,
				source: wendelon_client::ClientError::Admin(
					wendelon_client::WendelonAdminError::BackupHoldActive { progress },
				),
			});
		}
		Ok(DropTableStorageOutcome::new(1))
	}
}
