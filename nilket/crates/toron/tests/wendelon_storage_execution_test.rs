use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU64;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use arrow::array::{Array, ArrayRef, FixedSizeBinaryArray, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use toron::binder::{DurableEvidenceBinding, OptimizerRuntimeEvidence, PlannerConfig, bind};
use toron::catalog::{
	Catalog, DistinctCountFact, MockCatalog, TableBinding, TableOptimizerFacts, TableRef,
	TableRowIdentity, WendelonCatalog, WendelonDescriptorRegistry,
};
use toron::coordinator::QueryCoordinator;
use toron::distributed::budget::ByteCount;
use toron::distributed::determinism::{SortPolicy, canonical_arrow_ipc_digest};
use toron::distributed::task::RowCount;
use toron::exec::{ExecutionContext, Executor};
use toron::ir::{
	AggregateFunction, ColumnOrdinal, ExecutionPlan, Expr, FilterDisposition, JoinKind,
	JoinStrategyIntent, LogicalPlan, QueryId, QueryProfileHint, ScalarValue, SessionId, TableId,
};
use toron::storage::{
	HlcBound, InMemoryStorageScan, InMemoryTableStorage, StorageWrite, TenantId as ToronTenantId,
	WendelonStorageScan, WendelonStorageWrite, WriteRequest,
};
use wendelon::client::{ClusterPeerWire, ClusterStatusWire, StaticTopologySource};
use wendelon::consensus::WendelonRequest;
use wendelon::integration::compute::{
	COMPUTE_STORAGE_CONTRACT_VERSION, SchemaGeneration, SchemaIntentId, SchemaIntentOrigin,
	SchemaMutationKind, SchemaObjectKind, SchemaObjectRef, SchemaUpdateEnvelope,
	encode_schema_update_envelope_v1,
};
use wendelon::network::pool::BufPool;
use wendelon::network::transport::{
	NodeRole, NodeRoleSet, TransportProvider, TransportTier, TransportTierSet,
};
use wendelon::node::{NodeBootstrapConfig, WendelonNode};
use wendelon::schema::{
	LocalityPolicy, MultiRowLayoutDescriptor, PrimaryKeyDescriptor, RUNTIME_FILTER_HASH_SEED,
	RUNTIME_FILTER_KEY_ENCODING_VERSION, RecordBodyCompressionPolicy, RecordLayoutPolicy,
	RowIdentityPolicy, RuntimeFilterBloom, RuntimeFilterDescriptor, RuntimeFilterKeyType,
	RuntimeFilterKind, STORAGE_ROW_KEY_ID_COLUMN, SchemaEvolutionPolicy, TableObjectId,
	TableSchemaDescriptor, TenantId as WendelonTenantId, encode_runtime_filter_key,
	encode_table_schema_descriptor_v1, insert_runtime_filter_encoded_key,
	pbc::{OP_GT, OP_PROJECT, OP_PUSH_COL, OP_PUSH_INT},
};
use wendelon::simulator::network::{SimulatedTransportNetwork, SimulatedTransportProvider};
use wendelon_client::{Client, RowIdentityProjection, TableQueryOptions, WriteOptions};

const LIVE_TENANT: WendelonTenantId = WendelonTenantId::new(1);
const OTHER_TENANT: WendelonTenantId = WendelonTenantId::new(2);
const HOUR_MS: i64 = 60 * 60 * 1000;
const BASE_TS: i64 = 1_599_998_400_000;

#[tokio::test]
async fn toron_executes_pbc_projection_against_live_wendelon_table_query() {
	let fixture = LiveWendelonFixture::start(31).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	write_rows(&fixture.client, LIVE_TENANT, &descriptor, batch()).await;

	let ids = query_ids_gt(&fixture, &descriptor, LIVE_TENANT, 100).await;
	assert_eq!(ids, vec![2, 4]);

	fixture.node.handles.abort();
	let _ = std::fs::remove_dir_all(fixture.data_dir);
}

#[tokio::test]
async fn wendelon_storage_scan_keeps_live_tenants_isolated() {
	let fixture = LiveWendelonFixture::start(33).await;
	create_live_tenant(&fixture.node, OTHER_TENANT).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	install_schema(&fixture.node, OTHER_TENANT, &descriptor).await;
	write_rows(&fixture.client, LIVE_TENANT, &descriptor, batch()).await;
	write_rows(
		&fixture.client,
		OTHER_TENANT,
		&descriptor,
		batch_from_rows(&[
			(10, Some(75), Some("north"), BASE_TS),
			(11, Some(250), Some("south"), BASE_TS + HOUR_MS),
		]),
	)
	.await;

	let live_ids = query_ids_gt(&fixture, &descriptor, LIVE_TENANT, 100).await;
	let other_ids = query_ids_gt(&fixture, &descriptor, OTHER_TENANT, 100).await;

	assert_eq!(live_ids, vec![2, 4]);
	assert_eq!(other_ids, vec![11]);

	fixture.node.handles.abort();
	let _ = std::fs::remove_dir_all(fixture.data_dir);
}

#[tokio::test]
#[ignore = "simulated fixture does not yet expose authenticated schema-update admin operations"]
async fn wendelon_catalog_create_table_submits_schema_update_request() {
	let fixture = LiveWendelonFixture::start(39).await;
	let catalog = WendelonCatalog::with_client(
		fixture.client.clone(),
		LIVE_TENANT,
		std::iter::empty::<(TableId, TableSchemaDescriptor)>(),
	)
	.expect("wendelon catalog");
	let table = TableRef::with_schema("bench", "adr038_created");
	let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

	let binding = catalog
		.create_table(&table, schema.clone(), false)
		.await
		.expect("create table through Wendelon schema update");

	assert_eq!(binding.schema.as_ref(), schema.as_ref());
	assert_eq!(
		catalog
			.resolve_table(&table)
			.await
			.expect("registry updated after create")
			.legacy_table_id,
		binding.legacy_table_id
	);

	fixture.node.handles.abort();
	let _ = std::fs::remove_dir_all(fixture.data_dir);
}

#[tokio::test]
async fn sql_created_pk_table_insert_supersedes_visible_row() {
	let fixture = LiveWendelonFixture::start(41).await;
	let catalog = Arc::new(MockCatalog::new());
	let planner = toron::sql_planner::SqlPlanner::new();
	let create_coordinator = QueryCoordinator::new(
		catalog.clone(),
		Executor::new(Arc::new(InMemoryStorageScan {
			table_data: RecordBatch::new_empty(Arc::new(Schema::empty())),
		})),
		PlannerConfig::default(),
	);

	create_coordinator
		.execute_query(
			&planner
				.plan("CREATE TABLE bench.sql_pk (id BIGINT NOT NULL PRIMARY KEY, note TEXT)")
				.expect("plan SQL PK create"),
			None,
		)
		.await
		.expect("execute SQL PK create");

	let table = TableRef::with_schema("bench", "sql_pk");
	let binding = catalog
		.resolve_table(&table)
		.await
		.expect("resolve SQL-created PK table");
	let descriptor = descriptor_from_sql_created_binding(&table, &binding);
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	fixture
		.node
		.data_server
		.refresh_leases_once(&fixture.node.ring, &*fixture.node.clock, 60_000)
		.await;

	let descriptors = HashMap::from([(binding.legacy_table_id, descriptor)]);
	let registry = Arc::new(
		WendelonDescriptorRegistry::from_descriptors(descriptors)
			.expect("registry from SQL-created descriptor"),
	);
	let executor = Executor::with_context(
		Arc::new(WendelonStorageScan::with_descriptor_registry(
			fixture.client.clone(),
			registry.clone(),
		)),
		ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(62),
			ToronTenantId(1),
			HlcBound(u64::MAX),
		),
	);
	let writer = Arc::new(WendelonStorageWrite::with_descriptor_registry(
		fixture.client.clone(),
		registry,
	));
	let coordinator =
		QueryCoordinator::with_writer(catalog, executor, PlannerConfig::default(), writer);

	for sql in [
		"INSERT INTO bench.sql_pk (id, note) VALUES (1, 'old')",
		"INSERT INTO bench.sql_pk (id, note) VALUES (1, 'new')",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan insert"), None)
			.await
			.expect("execute insert");
	}

	let result = coordinator
		.execute_query(
			&planner
				.plan("SELECT id, note FROM bench.sql_pk WHERE id = 1")
				.expect("plan select"),
			None,
		)
		.await
		.expect("select superseded row");

	assert_eq!(id_notes_from_batches(&result), vec![(1, "new".to_string())]);

	fixture.node.handles.abort();
	let _ = std::fs::remove_dir_all(fixture.data_dir);
}

#[tokio::test]
async fn live_wendelon_sql_pk_update_supersedes_visible_row() {
	let fixture = LiveWendelonFixture::start(52).await;
	let catalog = Arc::new(MockCatalog::new());
	let planner = toron::sql_planner::SqlPlanner::new();
	let create_coordinator = QueryCoordinator::new(
		catalog.clone(),
		Executor::new(Arc::new(InMemoryStorageScan {
			table_data: RecordBatch::new_empty(Arc::new(Schema::empty())),
		})),
		PlannerConfig::default(),
	);

	create_coordinator
		.execute_query(
			&planner
				.plan(
					"CREATE TABLE bench.sql_pk_update (id BIGINT NOT NULL PRIMARY KEY, note TEXT)",
				)
				.expect("plan SQL PK create"),
			None,
		)
		.await
		.expect("execute SQL PK create");

	let table = TableRef::with_schema("bench", "sql_pk_update");
	let binding = catalog
		.resolve_table(&table)
		.await
		.expect("resolve SQL-created PK table");
	let descriptor = descriptor_from_sql_created_binding(&table, &binding);
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	fixture
		.node
		.data_server
		.refresh_leases_once(&fixture.node.ring, &*fixture.node.clock, 60_000)
		.await;

	let descriptors = HashMap::from([(binding.legacy_table_id, descriptor)]);
	let registry = Arc::new(
		WendelonDescriptorRegistry::from_descriptors(descriptors)
			.expect("registry from SQL-created descriptor"),
	);
	let executor = Executor::with_context(
		Arc::new(WendelonStorageScan::with_descriptor_registry(
			fixture.client.clone(),
			registry.clone(),
		)),
		ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(63),
			ToronTenantId(1),
			HlcBound(u64::MAX),
		),
	);
	let writer = Arc::new(WendelonStorageWrite::with_descriptor_registry(
		fixture.client.clone(),
		registry,
	));
	let coordinator =
		QueryCoordinator::with_writer(catalog, executor, PlannerConfig::default(), writer);

	coordinator
		.execute_query(
			&planner
				.plan("INSERT INTO bench.sql_pk_update (id, note) VALUES (1, 'old')")
				.expect("plan insert"),
			None,
		)
		.await
		.expect("execute insert");
	let update = coordinator
		.execute_query(
			&planner
				.plan("UPDATE bench.sql_pk_update SET note = 'new' WHERE id = 1")
				.expect("plan update"),
			None,
		)
		.await
		.expect("execute update");
	assert_eq!(affected_count_from_batches(&update), 1);

	let result = coordinator
		.execute_query(
			&planner
				.plan("SELECT id, note FROM bench.sql_pk_update WHERE id = 1")
				.expect("plan select"),
			None,
		)
		.await
		.expect("select superseded row");

	assert_eq!(id_notes_from_batches(&result), vec![(1, "new".to_string())]);

	fixture.node.handles.abort();
	let _ = std::fs::remove_dir_all(fixture.data_dir);
}

#[tokio::test]
async fn wendelon_declined_time_bucket_filter_matches_in_memory_baseline() {
	let fixture = LiveWendelonFixture::start(34).await;
	let descriptor = descriptor();
	let input = batch();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	write_rows(&fixture.client, LIVE_TENANT, &descriptor, input.clone()).await;

	let plan = LogicalPlan::Project {
		input: Box::new(LogicalPlan::Filter {
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
			predicate: Expr::Eq(
				Box::new(Expr::TimeBucket(
					Duration::from_millis(HOUR_MS as u64),
					Box::new(Expr::column("ts")),
				)),
				Box::new(Expr::Literal(ScalarValue::Int64(Some(BASE_TS)))),
			),
		}),
		output_names: vec![None],
		exprs: vec![Expr::column("id")],
	};
	let config = PlannerConfig {
		allow_experimental_kernels: true,
		..PlannerConfig::default()
	};
	let bound = bind_plan(&descriptor, &plan, &config).await;
	assert_filter_declined_without_pbc(&bound);

	let live = execute_wendelon_plan(&fixture, &descriptor, LIVE_TENANT, &bound).await;
	let baseline = execute_in_memory_plan(LIVE_TENANT, input, &bound).await;
	assert_batches_equal(&baseline, &live);
	assert_eq!(ids_from_single_column(&live), vec![1, 2, 4]);

	fixture.node.handles.abort();
	let _ = std::fs::remove_dir_all(fixture.data_dir);
}

#[tokio::test]
async fn wendelon_table_query_runtime_filter_fetches_omitted_probe_key() {
	let fixture = LiveWendelonFixture::start(36).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	write_rows(&fixture.client, LIVE_TENANT, &descriptor, batch()).await;

	let options = TableQueryOptions {
		physical_descriptor_fingerprint: Some(descriptor.physical_descriptor_fingerprint),
		..TableQueryOptions::default()
	};
	let pbc = pbc_project_amounts_gt_100();
	let filtered = fixture
		.client
		.table_query_with_runtime_filter(
			LIVE_TENANT,
			&descriptor,
			pbc.clone(),
			Some(runtime_filter_for_i64_values(&descriptor, &[2, 4])),
			options,
		)
		.await
		.expect("runtime-filtered table query");
	let unfiltered = fixture
		.client
		.table_query(LIVE_TENANT, &descriptor, pbc, options)
		.await
		.expect("unfiltered table query");

	assert_batches_equal(&unfiltered.batches, &filtered.batches);
	assert_eq!(
		amounts_from_single_column(&filtered.batches),
		vec![150, 200]
	);

	fixture.node.handles.abort();
	let _ = std::fs::remove_dir_all(fixture.data_dir);
}

#[tokio::test]
async fn wendelon_table_query_runtime_filter_reduces_boundary_rows() {
	let fixture = LiveWendelonFixture::start(37).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	write_rows(
		&fixture.client,
		LIVE_TENANT,
		&descriptor,
		selective_batch(RUNTIME_FILTER_BOUNDARY_ROWS),
	)
	.await;

	let filter = runtime_filter_for_i64_values(&descriptor, &[2, 4]);
	let unfiltered = table_query_id_amounts(&fixture, &descriptor, None).await;
	let filtered = table_query_id_amounts(&fixture, &descriptor, Some(filter)).await;
	let unfiltered_rows = row_count(&unfiltered);
	let filtered_rows = row_count(&filtered);
	let unfiltered_bytes = encode_batches(&unfiltered).len();
	let filtered_bytes = encode_batches(&filtered).len();

	assert!(filtered_rows < unfiltered_rows);
	assert!(filtered_bytes < unfiltered_bytes);

	let unfiltered_joined = joined_amounts_for_ids(&unfiltered, &[2, 4]);
	let filtered_joined = joined_amounts_for_ids(&filtered, &[2, 4]);
	let unfiltered_digest =
		canonical_arrow_ipc_digest(&[unfiltered_joined], SortPolicy::FullRowLex)
			.expect("unfiltered digest");
	let filtered_digest = canonical_arrow_ipc_digest(&[filtered_joined], SortPolicy::FullRowLex)
		.expect("filtered digest");
	assert_eq!(unfiltered_digest, filtered_digest);
	println!(
		"adr013_production_wendelon_runtime_filter rows_off={unfiltered_rows} rows_on={filtered_rows} bytes_off={unfiltered_bytes} bytes_on={filtered_bytes} digest={}",
		hex32(unfiltered_digest.get()),
	);

	fixture.node.handles.abort();
	let _ = std::fs::remove_dir_all(fixture.data_dir);
}

#[tokio::test]
async fn wendelon_table_query_appends_row_identity_for_filter_candidates() {
	let fixture = LiveWendelonFixture::start(39).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	write_rows(&fixture.client, LIVE_TENANT, &descriptor, batch()).await;

	let options = TableQueryOptions {
		physical_descriptor_fingerprint: Some(descriptor.physical_descriptor_fingerprint),
		row_identity: RowIdentityProjection::Append,
		..TableQueryOptions::default()
	};
	let first = fixture
		.client
		.table_query(LIVE_TENANT, &descriptor, pbc_amount_gt_100(), options)
		.await
		.expect("row identity table query");
	let second = fixture
		.client
		.table_query(LIVE_TENANT, &descriptor, pbc_amount_gt_100(), options)
		.await
		.expect("row identity table query");

	let batch = concat_batches_for_assertion(&first.batches);
	assert_eq!(
		batch.num_columns(),
		descriptor.arrow_schema().unwrap().fields().len() + 1
	);
	let schema = batch.schema();
	let identity_field = schema.field(batch.num_columns() - 1);
	assert_eq!(identity_field.name(), STORAGE_ROW_KEY_ID_COLUMN);
	assert_eq!(identity_field.data_type(), &DataType::FixedSizeBinary(32));
	assert_eq!(ids_from_single_column(&first.batches), vec![2, 4]);
	let first_ids = row_key_id_bytes(&batch);
	let second_batch = concat_batches_for_assertion(&second.batches);
	let second_ids = row_key_id_bytes(&second_batch);
	assert_eq!(first_ids, second_ids);
	assert_eq!(first_ids.len(), 2);

	fixture.node.handles.abort();
	let _ = std::fs::remove_dir_all(fixture.data_dir);
}

#[tokio::test]
async fn collect_select_star_limit_3_stops_whole_record_scan_before_full_scan() {
	let fixture = LiveWendelonFixture::start(120).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	write_rows(
		&fixture.client,
		LIVE_TENANT,
		&descriptor,
		selective_batch(12),
	)
	.await;

	let full = fixture
		.client
		.table_query(
			LIVE_TENANT,
			&descriptor,
			Vec::new(),
			whole_record_select_star_options(&descriptor, None),
		)
		.await
		.expect("full whole-record select star");
	let limited = fixture
		.client
		.table_query(
			LIVE_TENANT,
			&descriptor,
			Vec::new(),
			whole_record_select_star_options(&descriptor, NonZeroU64::new(3)),
		)
		.await
		.expect("limited whole-record select star");

	assert!(row_count(&full.batches) > 3);
	assert_eq!(row_count(&limited.batches), 3);
	assert_eq!(
		limited.batches.len(),
		3,
		"LIMIT 3 must return three per-location whole-record batches, not one post-scan rebatch"
	);
	assert!(
		limited.batches.len() < full.batches.len(),
		"LIMIT 3 must stop before the full whole-record scan"
	);
	for batch in &limited.batches {
		assert_select_star_prefix_schema(&descriptor, batch);
	}

	fixture.node.handles.abort();
	let _ = std::fs::remove_dir_all(fixture.data_dir);
}

#[tokio::test]
async fn stream_select_star_consume_three_then_drop_stops_whole_record_scan() {
	let fixture = LiveWendelonFixture::start(121).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	write_rows(
		&fixture.client,
		LIVE_TENANT,
		&descriptor,
		selective_batch(12),
	)
	.await;

	let full = fixture
		.client
		.table_query(
			LIVE_TENANT,
			&descriptor,
			Vec::new(),
			whole_record_select_star_options(&descriptor, None),
		)
		.await
		.expect("full whole-record select star");
	assert!(full.batches.len() > 3);

	let mut stream = fixture
		.client
		.table_query_stream(
			LIVE_TENANT,
			&descriptor,
			Vec::new(),
			whole_record_select_star_options(&descriptor, None),
		)
		.await
		.expect("streaming whole-record select star");
	let mut consumed = Vec::new();
	for _ in 0..3 {
		let batch = stream
			.next_batch()
			.await
			.expect("stream batch")
			.expect("expected another streamed batch");
		assert_eq!(
			batch.num_rows(),
			1,
			"whole-record stream should emit per location before cursor drop"
		);
		assert_select_star_prefix_schema(&descriptor, &batch);
		consumed.push(batch);
	}
	drop(stream);

	assert_eq!(row_count(&consumed), 3);
	assert!(
		consumed.len() < full.batches.len(),
		"consuming three batches then dropping must stop before full whole-record materialization"
	);

	fixture.node.handles.abort();
	let _ = std::fs::remove_dir_all(fixture.data_dir);
}

#[tokio::test]
async fn wendelon_table_query_exact_set_runtime_filter_matches_bloom_and_reduces_boundary_rows() {
	let fixture = LiveWendelonFixture::start(38).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	write_rows(
		&fixture.client,
		LIVE_TENANT,
		&descriptor,
		selective_batch(RUNTIME_FILTER_BOUNDARY_ROWS),
	)
	.await;

	let build_ids = [2, 4];
	let unfiltered = table_query_id_amounts(&fixture, &descriptor, None).await;
	let bloom = table_query_id_amounts(
		&fixture,
		&descriptor,
		Some(runtime_filter_for_i64_values(&descriptor, &build_ids)),
	)
	.await;
	let exact = table_query_id_amounts(
		&fixture,
		&descriptor,
		Some(runtime_filter_exact_for_i64_values(&descriptor, &build_ids)),
	)
	.await;

	let unfiltered_rows = row_count(&unfiltered);
	let bloom_rows = row_count(&bloom);
	let exact_rows = row_count(&exact);
	let unfiltered_bytes = encode_batches(&unfiltered).len();
	let bloom_bytes = encode_batches(&bloom).len();
	let exact_bytes = encode_batches(&exact).len();
	assert!(bloom_rows < unfiltered_rows);
	assert!(exact_rows <= bloom_rows);
	assert!(exact_bytes <= bloom_bytes);
	let exact_fp = false_positive_rows(&exact, &build_ids);
	assert_eq!(exact_fp, 0);

	let unfiltered_digest = canonical_arrow_ipc_digest(
		&[joined_amounts_for_ids(&unfiltered, &build_ids)],
		SortPolicy::FullRowLex,
	)
	.expect("unfiltered digest");
	let bloom_digest = canonical_arrow_ipc_digest(
		&[joined_amounts_for_ids(&bloom, &build_ids)],
		SortPolicy::FullRowLex,
	)
	.expect("bloom digest");
	let exact_digest = canonical_arrow_ipc_digest(
		&[joined_amounts_for_ids(&exact, &build_ids)],
		SortPolicy::FullRowLex,
	)
	.expect("exact digest");
	assert_eq!(bloom_digest, unfiltered_digest);
	assert_eq!(exact_digest, unfiltered_digest);
	println!(
		"adr014_exact_small_set_runtime_filter rows_off={unfiltered_rows} rows_bloom={bloom_rows} rows_exact={exact_rows} bytes_off={unfiltered_bytes} bytes_bloom={bloom_bytes} bytes_exact={exact_bytes} exact_false_positive_rows=0 digest={}",
		hex32(exact_digest.get()),
	);

	fixture.node.handles.abort();
	let _ = std::fs::remove_dir_all(fixture.data_dir);
}

#[tokio::test]
async fn wendelon_grouped_aggregate_matches_in_memory_baseline() {
	let fixture = LiveWendelonFixture::start(35).await;
	let descriptor = descriptor();
	let input = batch();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	write_rows(&fixture.client, LIVE_TENANT, &descriptor, input.clone()).await;

	let plan = LogicalPlan::Aggregate {
		input: Box::new(LogicalPlan::Scan {
			table: TableId(1),
			projection: None,
		}),
		group_expr: vec![Expr::column("region")],
		aggr_expr: vec![
			(AggregateFunction::Sum, vec![Expr::column("amount")]),
			(AggregateFunction::Count, vec![Expr::column("amount")]),
			(AggregateFunction::CountAll, vec![]),
		],
	};
	let bound = bind_plan(&descriptor, &plan, &PlannerConfig::default()).await;

	let live = execute_wendelon_plan(&fixture, &descriptor, LIVE_TENANT, &bound).await;
	let baseline = execute_in_memory_plan(LIVE_TENANT, input, &bound).await;
	assert_batches_equal(&baseline, &live);

	fixture.node.handles.abort();
	let _ = std::fs::remove_dir_all(fixture.data_dir);
}

#[tokio::test]
async fn durable_runtime_verdict_counters_survive_coordinator_restart() {
	let fixture = LiveWendelonFixture::start(122).await;
	let (catalog, storage) = durable_star_fixture().await;
	let runtime_evidence = durable_optimizer_runtime_evidence(&fixture).await;
	let coordinator = durable_star_coordinator(
		Arc::clone(&catalog),
		Arc::clone(&storage),
		Arc::clone(&runtime_evidence),
		1,
	);

	execute_star_query(&coordinator).await;
	assert_eq!(
		optimizer_runtime_verdict_count(&coordinator).await,
		1,
		"recording coordinator should see the flushed durable verdict"
	);

	drop(coordinator);
	drop(runtime_evidence);
	let restarted_runtime_evidence = durable_optimizer_runtime_evidence(&fixture).await;
	let restarted_coordinator =
		durable_star_coordinator(catalog, storage, Arc::clone(&restarted_runtime_evidence), 0);

	assert_eq!(
		optimizer_runtime_verdict_count(&restarted_coordinator).await,
		1,
		"fresh coordinator/evidence handle should read the prior durable verdict"
	);

	fixture.node.handles.abort();
	let _ = std::fs::remove_dir_all(fixture.data_dir);
}

#[tokio::test]
async fn cross_coordinator_verdicts_are_shared() {
	let fixture = LiveWendelonFixture::start(123).await;
	let (catalog, storage) = durable_star_fixture().await;
	let writer_runtime_evidence = durable_optimizer_runtime_evidence(&fixture).await;
	let writer = durable_star_coordinator(
		Arc::clone(&catalog),
		Arc::clone(&storage),
		writer_runtime_evidence,
		1,
	);
	let reader_runtime_evidence = durable_optimizer_runtime_evidence(&fixture).await;
	let reader =
		durable_star_coordinator(catalog, storage, Arc::clone(&reader_runtime_evidence), 0);

	execute_star_query(&writer).await;
	reader_runtime_evidence.refresh_durable_cache().await;

	assert_eq!(
		optimizer_runtime_verdict_count(&reader).await,
		1,
		"second coordinator should see verdicts it did not record"
	);

	fixture.node.handles.abort();
	let _ = std::fs::remove_dir_all(fixture.data_dir);
}

#[tokio::test]
async fn durable_optimizer_decisions_survive_coordinator_restart() {
	let fixture = LiveWendelonFixture::start(127).await;
	let (catalog, storage) = durable_star_fixture().await;
	let runtime_evidence = durable_optimizer_runtime_evidence(&fixture).await;
	let coordinator = durable_star_coordinator(
		Arc::clone(&catalog),
		Arc::clone(&storage),
		Arc::clone(&runtime_evidence),
		0,
	);

	execute_star_query(&coordinator).await;
	runtime_evidence.flush_decision_deltas().await;
	assert_eq!(
		optimizer_decision_count(&coordinator).await,
		1,
		"recording coordinator should see the flushed durable decision"
	);

	drop(coordinator);
	drop(runtime_evidence);
	let restarted_runtime_evidence = durable_optimizer_runtime_evidence(&fixture).await;
	let restarted_coordinator =
		durable_star_coordinator(catalog, storage, Arc::clone(&restarted_runtime_evidence), 0);

	assert_eq!(
		optimizer_decision_count(&restarted_coordinator).await,
		1,
		"fresh coordinator/evidence handle should read the prior durable decision"
	);

	fixture.node.handles.abort();
	let _ = std::fs::remove_dir_all(fixture.data_dir);
}

#[tokio::test]
async fn cross_coordinator_decisions_are_shared_after_refresh() {
	let fixture = LiveWendelonFixture::start(128).await;
	let (catalog, storage) = durable_star_fixture().await;
	let writer_runtime_evidence = durable_optimizer_runtime_evidence(&fixture).await;
	let writer = durable_star_coordinator(
		Arc::clone(&catalog),
		Arc::clone(&storage),
		Arc::clone(&writer_runtime_evidence),
		0,
	);
	let reader_runtime_evidence = durable_optimizer_runtime_evidence(&fixture).await;
	let reader =
		durable_star_coordinator(catalog, storage, Arc::clone(&reader_runtime_evidence), 0);

	execute_star_query(&writer).await;
	writer_runtime_evidence.flush_decision_deltas().await;
	reader_runtime_evidence.refresh_durable_cache().await;

	assert_eq!(
		optimizer_decision_count(&reader).await,
		1,
		"second coordinator should see decisions it did not record"
	);

	fixture.node.handles.abort();
	let _ = std::fs::remove_dir_all(fixture.data_dir);
}

#[tokio::test]
async fn decision_telemetry_survives_durable_refresh() {
	let fixture = LiveWendelonFixture::start(129).await;
	let (catalog, storage) = durable_star_fixture().await;
	let runtime_evidence = durable_optimizer_runtime_evidence(&fixture).await;
	let coordinator = durable_star_coordinator(
		Arc::clone(&catalog),
		Arc::clone(&storage),
		Arc::clone(&runtime_evidence),
		0,
	);

	execute_star_query(&coordinator).await;
	runtime_evidence.flush_decision_deltas().await;
	let report = runtime_evidence.refresh_durable_cache().await;

	assert!(!report.failed, "durable decision refresh should succeed");
	assert_eq!(
		optimizer_decision_count(&coordinator).await,
		1,
		"coordinator should still see flushed decisions after durable refresh"
	);

	fixture.node.handles.abort();
	let _ = std::fs::remove_dir_all(fixture.data_dir);
}

#[tokio::test]
async fn demoted_join_verdict_reaches_durable_lane() {
	let fixture = LiveWendelonFixture::start(124).await;
	let (catalog, storage) = durable_star_fixture().await;
	let runtime_evidence = durable_optimizer_runtime_evidence(&fixture).await;
	let coordinator = durable_star_coordinator(
		Arc::clone(&catalog),
		Arc::clone(&storage),
		Arc::clone(&runtime_evidence),
		1,
	);

	let batch = execute_demoted_star_query(&coordinator).await;
	assert!(
		batch.num_rows() > 0,
		"demoted star query should return user rows"
	);
	assert_eq!(
		optimizer_runtime_flat_star_verdict_count(&coordinator).await,
		1,
		"demoted star query should flush a durable flat-star verdict"
	);
	assert_eq!(
		optimizer_runtime_flat_star_verdict_count_by_provenance(&coordinator, "demoted").await,
		1,
		"demoted star query should flush the durable demoted sibling"
	);

	drop(coordinator);
	drop(runtime_evidence);
	let restarted_runtime_evidence = durable_optimizer_runtime_evidence(&fixture).await;
	let restarted_coordinator =
		durable_star_coordinator(catalog, storage, Arc::clone(&restarted_runtime_evidence), 0);

	assert_eq!(
		optimizer_runtime_flat_star_verdict_count(&restarted_coordinator).await,
		1,
		"fresh coordinator/evidence handle should read the demoted query verdict"
	);
	assert_eq!(
		optimizer_runtime_flat_star_verdict_count_by_provenance(&restarted_coordinator, "demoted")
			.await,
		1,
		"fresh coordinator/evidence handle should read the demoted sibling"
	);

	fixture.node.handles.abort();
	let _ = std::fs::remove_dir_all(fixture.data_dir);
}

#[tokio::test]
async fn sql_mixed_chain_null_rejection_demotes_nested_left_join_and_reaches_durable_verdict() {
	let fixture = LiveWendelonFixture::start(125).await;
	let (catalog, storage) = durable_star_fixture().await;
	let runtime_evidence = durable_optimizer_runtime_evidence(&fixture).await;
	let coordinator = durable_star_coordinator(
		Arc::clone(&catalog),
		Arc::clone(&storage),
		Arc::clone(&runtime_evidence),
		1,
	);

	let batch = execute_durable_sql(&coordinator, DURABLE_MIXED_STAR_SQL).await;
	assert!(
		batch.num_rows() > 0,
		"natural mixed-chain SQL should return user rows"
	);
	assert_eq!(
		optimizer_runtime_flat_star_verdict_count(&coordinator).await,
		1,
		"natural mixed-chain SQL should flush a durable flat-star verdict"
	);

	drop(coordinator);
	drop(runtime_evidence);
	let restarted_runtime_evidence = durable_optimizer_runtime_evidence(&fixture).await;
	let restarted_coordinator =
		durable_star_coordinator(catalog, storage, Arc::clone(&restarted_runtime_evidence), 0);

	assert_eq!(
		optimizer_runtime_flat_star_verdict_count(&restarted_coordinator).await,
		1,
		"fresh coordinator/evidence handle should read the mixed-chain verdict"
	);

	fixture.node.handles.abort();
	let _ = std::fs::remove_dir_all(fixture.data_dir);
}

#[tokio::test]
async fn mixed_chain_demoted_results_match_undemoted() {
	let fixture = LiveWendelonFixture::start(126).await;
	let (catalog, storage) = durable_star_fixture().await;
	let runtime_evidence = durable_optimizer_runtime_evidence(&fixture).await;
	let coordinator = durable_star_coordinator(catalog, storage, runtime_evidence, 0);

	let demoted = execute_durable_sql(&coordinator, DURABLE_MIXED_STAR_SQL).await;
	let undemoted = execute_durable_sql(&coordinator, DURABLE_MIXED_STAR_CASE_WRAPPED_SQL).await;

	assert_eq!(int64_quad_rows(&demoted), int64_quad_rows(&undemoted));

	fixture.node.handles.abort();
	let _ = std::fs::remove_dir_all(fixture.data_dir);
}

const DURABLE_STAR_SQL: &str = "SELECT o.id, c.id, p.id, s.id \
	FROM orders o \
	INNER JOIN customers c ON o.customer_id = c.id \
	INNER JOIN products p ON o.product_id = p.id \
	INNER JOIN stores s ON o.store_id = s.id";

const DURABLE_MIXED_STAR_SQL: &str = "SELECT o.id, c.id, p.id, s.id \
	FROM orders o \
	LEFT JOIN customers c ON o.customer_id = c.id \
	INNER JOIN products p ON o.product_id = p.id \
	INNER JOIN stores s ON o.store_id = s.id \
	WHERE c.tier > 0";

const DURABLE_MIXED_STAR_CASE_WRAPPED_SQL: &str = "SELECT o.id, c.id, p.id, s.id \
	FROM orders o \
	LEFT JOIN customers c ON o.customer_id = c.id \
	INNER JOIN products p ON o.product_id = p.id \
	INNER JOIN stores s ON o.store_id = s.id \
	WHERE CASE WHEN c.tier > 0 THEN true ELSE false END";

async fn durable_optimizer_runtime_evidence(
	fixture: &LiveWendelonFixture,
) -> Arc<OptimizerRuntimeEvidence> {
	let evidence = OptimizerRuntimeEvidence::with_wendelon(DurableEvidenceBinding::new(
		Arc::clone(&fixture.client),
		LIVE_TENANT,
		Duration::from_secs(1),
	));
	let report = evidence.refresh_durable_cache().await;
	assert!(
		!report.failed,
		"initial durable evidence refresh should succeed"
	);
	evidence
}

fn durable_star_coordinator(
	catalog: Arc<MockCatalog>,
	storage: Arc<InMemoryTableStorage>,
	runtime_evidence: Arc<OptimizerRuntimeEvidence>,
	measured_accrual_sample_rate: u32,
) -> QueryCoordinator {
	QueryCoordinator::new(
		catalog,
		Executor::with_context(
			storage,
			ExecutionContext::new(
				SessionId(1),
				QueryId::from_u128(207),
				ToronTenantId(1),
				HlcBound(1),
			),
		),
		PlannerConfig {
			auto_broadcast_max_build_bytes: Some(ByteCount::new(64 * 1024 * 1024)),
			measured_accrual_sample_rate,
			runtime_evidence,
			..PlannerConfig::default()
		},
	)
}

async fn execute_star_query(coordinator: &QueryCoordinator) -> RecordBatch {
	execute_durable_sql(coordinator, DURABLE_STAR_SQL).await
}

async fn execute_durable_sql(coordinator: &QueryCoordinator, sql: &str) -> RecordBatch {
	let plan = toron::sql_planner::SqlPlanner::new()
		.plan(sql)
		.expect("plan durable SQL query");
	execute_single_coordinator_batch(coordinator, &plan).await
}

async fn execute_demoted_star_query(coordinator: &QueryCoordinator) -> RecordBatch {
	let plan = demoted_durable_star_plan();
	execute_single_coordinator_batch(coordinator, &plan).await
}

fn demoted_durable_star_plan() -> LogicalPlan {
	let orders = TableRef::new("orders");
	let customers = TableRef::new("customers");
	let products = TableRef::new("products");
	let stores = TableRef::new("stores");

	let filtered_outer_join = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Join {
			left: Box::new(LogicalPlan::ScanNamed {
				table: orders.clone(),
				projection: None,
			}),
			right: Box::new(LogicalPlan::ScanNamed {
				table: customers.clone(),
				projection: None,
			}),
			kind: JoinKind::LeftOuter,
			on: vec![(
				Expr::qualified_column(orders.clone(), "customer_id"),
				Expr::qualified_column(customers.clone(), "id"),
			)],
			strategy_intent: JoinStrategyIntent::Auto,
		}),
		predicate: Expr::Gt(
			Box::new(Expr::qualified_column(customers.clone(), "tier")),
			Box::new(Expr::Literal(ScalarValue::Int64(Some(0)))),
		),
	};
	let with_products = LogicalPlan::Join {
		left: Box::new(filtered_outer_join),
		right: Box::new(LogicalPlan::ScanNamed {
			table: products.clone(),
			projection: None,
		}),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(orders.clone(), "product_id"),
			Expr::qualified_column(products.clone(), "id"),
		)],
		strategy_intent: JoinStrategyIntent::Auto,
	};
	let with_stores = LogicalPlan::Join {
		left: Box::new(with_products),
		right: Box::new(LogicalPlan::ScanNamed {
			table: stores.clone(),
			projection: None,
		}),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(orders.clone(), "store_id"),
			Expr::qualified_column(stores.clone(), "id"),
		)],
		strategy_intent: JoinStrategyIntent::Auto,
	};

	LogicalPlan::Project {
		input: Box::new(with_stores),
		exprs: vec![
			Expr::qualified_column(orders, "id"),
			Expr::qualified_column(customers, "id"),
			Expr::qualified_column(products, "id"),
			Expr::qualified_column(stores, "id"),
		],
		output_names: vec![
			Some("order_id".to_string()),
			Some("customer_id".to_string()),
			Some("product_id".to_string()),
			Some("store_id".to_string()),
		],
	}
}

async fn optimizer_runtime_verdict_count(coordinator: &QueryCoordinator) -> i64 {
	let plan = toron::sql_planner::SqlPlanner::new()
		.plan("SELECT * FROM information_schema.optimizer_runtime_verdicts")
		.expect("plan optimizer runtime verdicts query");
	let batch = execute_single_coordinator_batch(coordinator, &plan).await;
	let kinds = string_column_values(&batch, 0);
	let verdicts = string_column_values(&batch, 1);
	let counts = int64_column_values(&batch, 2);
	kinds
		.iter()
		.zip(verdicts.iter())
		.zip(counts.iter())
		.find_map(|((kind, verdict), count)| {
			(kind == "flat_star" && verdict == "over_eager").then_some(*count)
		})
		.unwrap_or(0)
}

async fn optimizer_runtime_flat_star_verdict_count(coordinator: &QueryCoordinator) -> i64 {
	let plan = toron::sql_planner::SqlPlanner::new()
		.plan("SELECT * FROM information_schema.optimizer_runtime_verdicts")
		.expect("plan optimizer runtime verdicts query");
	let batch = execute_single_coordinator_batch(coordinator, &plan).await;
	let kinds = string_column_values(&batch, 0);
	let counts = int64_column_values(&batch, 2);
	kinds
		.iter()
		.zip(counts.iter())
		.filter_map(|(kind, count)| (kind == "flat_star").then_some(*count))
		.sum()
}

async fn optimizer_runtime_flat_star_verdict_count_by_provenance(
	coordinator: &QueryCoordinator,
	provenance: &str,
) -> i64 {
	let plan = toron::sql_planner::SqlPlanner::new()
		.plan("SELECT * FROM information_schema.optimizer_runtime_verdicts")
		.expect("plan optimizer runtime verdicts query");
	let batch = execute_single_coordinator_batch(coordinator, &plan).await;
	let kinds = string_column_values(&batch, 0);
	let provenances = string_column_values(&batch, 3);
	let counts = int64_column_values(&batch, 2);
	kinds
		.iter()
		.zip(provenances.iter())
		.zip(counts.iter())
		.filter_map(|((kind, row_provenance), count)| {
			(kind == "flat_star" && row_provenance == provenance).then_some(*count)
		})
		.sum()
}

async fn optimizer_decision_count(coordinator: &QueryCoordinator) -> i64 {
	let plan = toron::sql_planner::SqlPlanner::new()
		.plan("SELECT * FROM information_schema.optimizer_decisions")
		.expect("plan optimizer decisions query");
	let batch = execute_single_coordinator_batch(coordinator, &plan).await;
	let kinds = string_column_values(&batch, 0);
	let outcomes = string_column_values(&batch, 1);
	let provenances = string_column_values(&batch, 4);
	let counts = int64_column_values(&batch, 3);
	kinds
		.iter()
		.zip(outcomes.iter())
		.zip(provenances.iter())
		.zip(counts.iter())
		.find_map(|(((kind, outcome), provenance), count)| {
			(kind == "flat_star" && outcome == "reordered" && provenance == "native")
				.then_some(*count)
		})
		.unwrap_or(0)
}

async fn execute_single_coordinator_batch(
	coordinator: &QueryCoordinator,
	plan: &LogicalPlan,
) -> RecordBatch {
	let batches = coordinator
		.execute_query(plan, None)
		.await
		.expect("execute coordinator query");
	concat_batches_for_assertion(&batches)
}

async fn durable_star_fixture() -> (Arc<MockCatalog>, Arc<InMemoryTableStorage>) {
	let mut catalog = MockCatalog::new();
	let storage = Arc::new(InMemoryTableStorage::new());

	let orders = durable_star_descriptor(
		221,
		"orders",
		Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("customer_id", DataType::Int64, false),
			Field::new("product_id", DataType::Int64, false),
			Field::new("store_id", DataType::Int64, false),
		])),
		"id",
	);
	let customers = durable_star_descriptor(
		222,
		"customers",
		Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("tier", DataType::Int64, false),
		])),
		"id",
	);
	let products = durable_star_id_descriptor(223, "products");
	let stores = durable_star_id_descriptor(224, "stores");

	for (table_id, descriptor) in [
		(TableId(221), &orders),
		(TableId(222), &customers),
		(TableId(223), &products),
		(TableId(224), &stores),
	] {
		catalog
			.register_descriptor(table_id, descriptor.clone())
			.expect("register durable star descriptor");
	}
	seed_durable_star_facts(&mut catalog);

	write_durable_star_table(
		storage.as_ref(),
		TableId(221),
		&orders,
		durable_star_record_batch(
			orders.arrow_schema().expect("orders schema").into(),
			vec![
				Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef,
				Arc::new(Int64Array::from(vec![10, 20, 10, 30])) as ArrayRef,
				Arc::new(Int64Array::from(vec![100, 100, 200, 300])) as ArrayRef,
				Arc::new(Int64Array::from(vec![1000, 2000, 1000, 3000])) as ArrayRef,
			],
		),
	)
	.await;
	write_durable_star_table(
		storage.as_ref(),
		TableId(222),
		&customers,
		durable_star_record_batch(
			customers.arrow_schema().expect("customers schema").into(),
			vec![
				Arc::new(Int64Array::from(vec![10, 20, 30, 40])) as ArrayRef,
				Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef,
			],
		),
	)
	.await;
	write_durable_star_table(
		storage.as_ref(),
		TableId(223),
		&products,
		durable_star_id_batch(
			products.arrow_schema().expect("products schema").into(),
			&[100, 200, 300],
		),
	)
	.await;
	write_durable_star_table(
		storage.as_ref(),
		TableId(224),
		&stores,
		durable_star_id_batch(
			stores.arrow_schema().expect("stores schema").into(),
			&[1000, 2000, 3000],
		),
	)
	.await;

	(Arc::new(catalog), storage)
}

fn seed_durable_star_facts(catalog: &mut MockCatalog) {
	for (table, rows, ndv) in [
		("orders", 100_000, vec![(1, 100), (2, 100), (3, 100)]),
		("customers", 5_000, vec![(0, 100), (1, 4)]),
		("products", 1_000, vec![(0, 100)]),
		("stores", 100, vec![(0, 100)]),
	] {
		catalog
			.seed_optimizer_facts(
				&TableRef::new(table),
				TableOptimizerFacts {
					row_upper_bound: Some(RowCount::new(rows)),
					column_ndv: ndv
						.into_iter()
						.map(|(ordinal, distinct)| {
							(ColumnOrdinal(ordinal), DistinctCountFact::Exact(distinct))
						})
						.collect::<BTreeMap<_, _>>(),
					..TableOptimizerFacts::default()
				},
			)
			.expect("seed durable optimizer facts");
	}
}

async fn write_durable_star_table(
	storage: &InMemoryTableStorage,
	table_id: TableId,
	descriptor: &TableSchemaDescriptor,
	payload: RecordBatch,
) {
	storage
		.write(WriteRequest {
			session_id: SessionId(1),
			query_id: QueryId::from_u128(207),
			tenant_id: ToronTenantId(1),
			table_id,
			table_object_id: descriptor.table_object_id,
			physical_descriptor_fingerprint: descriptor.physical_descriptor_fingerprint,
			payload,
			profile_hint: Some(QueryProfileHint::Scan),
			trace_context: None,
		})
		.await
		.expect("write durable star table");
}

fn durable_star_id_descriptor(id: u8, name: &str) -> TableSchemaDescriptor {
	durable_star_descriptor(
		id,
		name,
		Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
		"id",
	)
}

fn durable_star_descriptor(
	id: u8,
	name: &str,
	schema: SchemaRef,
	primary_key: &str,
) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new(
		TableObjectId::new([id; 16]),
		1,
		"default",
		name,
		schema.as_ref(),
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single(primary_key)),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("durable star descriptor")
}

fn durable_star_id_batch(schema: SchemaRef, values: &[i64]) -> RecordBatch {
	durable_star_record_batch(schema, vec![Arc::new(Int64Array::from(values.to_vec()))])
}

fn durable_star_record_batch(schema: SchemaRef, columns: Vec<ArrayRef>) -> RecordBatch {
	RecordBatch::try_new(schema, columns).expect("durable star record batch")
}

fn string_column_values(batch: &RecordBatch, column: usize) -> Vec<String> {
	let array = batch
		.column(column)
		.as_any()
		.downcast_ref::<StringArray>()
		.expect("string array");
	(0..array.len())
		.map(|row| array.value(row).to_string())
		.collect()
}

fn int64_column_values(batch: &RecordBatch, column: usize) -> Vec<i64> {
	let array = batch
		.column(column)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("int64 array");
	(0..array.len()).map(|row| array.value(row)).collect()
}

fn int64_quad_rows(batch: &RecordBatch) -> Vec<(i64, i64, i64, i64)> {
	let first = int64_column_values(batch, 0);
	let second = int64_column_values(batch, 1);
	let third = int64_column_values(batch, 2);
	let fourth = int64_column_values(batch, 3);
	let mut rows = first
		.into_iter()
		.zip(second)
		.zip(third)
		.zip(fourth)
		.map(|(((first, second), third), fourth)| (first, second, third, fourth))
		.collect::<Vec<_>>();
	rows.sort_unstable();
	rows
}

async fn query_ids_gt(
	fixture: &LiveWendelonFixture,
	descriptor: &TableSchemaDescriptor,
	tenant: WendelonTenantId,
	threshold: i32,
) -> Vec<i64> {
	let tenant_id = toron_tenant_id(tenant);
	let registry = descriptor_registry_for_tenant(tenant, descriptor);
	let catalog = WendelonCatalog::with_client_and_registry(
		fixture.client.clone(),
		tenant_id,
		Arc::clone(&registry),
	);
	let storage = Arc::new(WendelonStorageScan::with_descriptor_registry(
		fixture.client.clone(),
		registry,
	));
	let executor = Executor::with_context(
		storage,
		ExecutionContext::new(
			SessionId(10),
			QueryId::from_u128(20),
			tenant_id,
			HlcBound(0),
		),
	);
	let plan = LogicalPlan::Project {
		input: Box::new(LogicalPlan::Filter {
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
			predicate: Expr::Gt(
				Box::new(Expr::column("amount")),
				Box::new(Expr::Literal(ScalarValue::Int32(Some(threshold)))),
			),
		}),
		output_names: vec![None],
		exprs: vec![Expr::column("id")],
	};
	let bound = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("bind");

	let results = executor
		.execute_bound(&bound, None)
		.await
		.expect("execute live Wendelon-backed query");

	let batch = concat_batches_for_assertion(&results);
	let ids = batch
		.column(0)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("id column");
	ids.values().to_vec()
}

async fn bind_plan(
	descriptor: &TableSchemaDescriptor,
	plan: &LogicalPlan,
	config: &PlannerConfig,
) -> ExecutionPlan {
	let catalog =
		WendelonCatalog::from_descriptors([(TableId(1), descriptor.clone())]).expect("catalog");
	bind(plan, &catalog, config).await.expect("bind")
}

async fn execute_wendelon_plan(
	fixture: &LiveWendelonFixture,
	descriptor: &TableSchemaDescriptor,
	tenant: WendelonTenantId,
	bound: &ExecutionPlan,
) -> Vec<RecordBatch> {
	let storage = Arc::new(WendelonStorageScan::with_descriptor_registry(
		fixture.client.clone(),
		descriptor_registry_for_tenant(tenant, descriptor),
	));
	let executor = Executor::with_context(storage, execution_context(tenant));
	executor
		.execute_bound(bound, None)
		.await
		.expect("execute live Wendelon-backed query")
}

async fn execute_in_memory_plan(
	tenant: WendelonTenantId,
	batch: RecordBatch,
	bound: &ExecutionPlan,
) -> Vec<RecordBatch> {
	let storage = Arc::new(InMemoryStorageScan { table_data: batch });
	let executor = Executor::with_context(storage, execution_context(tenant));
	executor
		.execute_bound(bound, None)
		.await
		.expect("execute in-memory baseline")
}

async fn table_query_id_amounts(
	fixture: &LiveWendelonFixture,
	descriptor: &TableSchemaDescriptor,
	runtime_filter: Option<RuntimeFilterDescriptor>,
) -> Vec<RecordBatch> {
	let options = TableQueryOptions {
		physical_descriptor_fingerprint: Some(descriptor.physical_descriptor_fingerprint),
		..TableQueryOptions::default()
	};
	fixture
		.client
		.table_query_with_runtime_filter(
			LIVE_TENANT,
			descriptor,
			pbc_project_id_amounts(),
			runtime_filter,
			options,
		)
		.await
		.expect("table query id/amounts")
		.batches
}

fn execution_context(tenant: WendelonTenantId) -> ExecutionContext {
	ExecutionContext::new(
		SessionId(10),
		QueryId::from_u128(20),
		toron_tenant_id(tenant),
		HlcBound(0),
	)
}

fn descriptor_registry_for_tenant(
	tenant: WendelonTenantId,
	descriptor: &TableSchemaDescriptor,
) -> Arc<WendelonDescriptorRegistry> {
	Arc::new(
		WendelonDescriptorRegistry::from_descriptors_for_tenant(
			toron_tenant_id(tenant),
			[(TableId(1), descriptor.clone())],
		)
		.expect("descriptor registry"),
	)
}

fn toron_tenant_id(tenant: WendelonTenantId) -> ToronTenantId {
	ToronTenantId(tenant.get())
}

fn assert_filter_declined_without_pbc(plan: &ExecutionPlan) {
	match plan {
		ExecutionPlan::Project { input, .. } => assert_filter_declined_without_pbc(input),
		ExecutionPlan::Filter {
			input, disposition, ..
		} => {
			assert_eq!(*disposition, FilterDisposition::EvaluateInToron);
			assert_scan_has_no_pbc(input);
		}
		other => panic!("expected project/filter plan, got {other:?}"),
	}
}

fn assert_scan_has_no_pbc(plan: &ExecutionPlan) {
	match plan {
		ExecutionPlan::EmptyRelation { .. } => panic!("EmptyRelation has no scan"),
		ExecutionPlan::SystemTable { .. } => panic!("SystemTable has no scan"),
		ExecutionPlan::Scan { pbc_program, .. } => assert!(
			pbc_program.is_none(),
			"declined filter must not attach a PBC program"
		),
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Take { input, .. } => assert_scan_has_no_pbc(input),
		ExecutionPlan::Join { left, right, .. } => {
			assert_scan_has_no_pbc(left);
			assert_scan_has_no_pbc(right);
		}
		ExecutionPlan::CreateTable { .. } => panic!("CREATE TABLE has no scan"),
		ExecutionPlan::CreateLogicalType { .. } => panic!("CREATE LOGICAL TYPE has no scan"),
		ExecutionPlan::CreateTableAsSelect { .. } => {
			panic!("CREATE TABLE AS SELECT has no scan")
		}
		ExecutionPlan::DropTable { .. } => panic!("DROP TABLE has no scan"),
		ExecutionPlan::Delete { .. } => panic!("DELETE has no scan"),
		ExecutionPlan::Update { .. } => panic!("UPDATE has no scan"),
		ExecutionPlan::Insert { .. } => panic!("INSERT has no scan"),
		ExecutionPlan::InsertSelect { .. } => panic!("INSERT INTO ... SELECT has no scan"),
	}
}

fn assert_batches_equal(expected: &[RecordBatch], actual: &[RecordBatch]) {
	assert_eq!(row_count(actual), row_count(expected), "row count");
	let expected = concat_batches_for_assertion(expected);
	let actual = concat_batches_for_assertion(actual);
	assert_eq!(
		encode_batches(&[expected]),
		encode_batches(&[actual]),
		"record batch IPC payloads"
	);
}

fn concat_batches_for_assertion(batches: &[RecordBatch]) -> RecordBatch {
	let schema = batches.first().expect("at least one batch").schema();
	arrow::compute::concat_batches(&schema, batches.iter()).expect("concat batches")
}

fn encode_batches(batches: &[RecordBatch]) -> Vec<u8> {
	let schema = batches.first().expect("at least one batch").schema();
	let mut bytes = Vec::new();
	{
		let mut writer = StreamWriter::try_new(&mut bytes, &schema).expect("IPC writer");
		for batch in batches {
			writer.write(batch).expect("write IPC batch");
		}
		writer.finish().expect("finish IPC stream");
	}
	bytes
}

fn ids_from_single_column(batches: &[RecordBatch]) -> Vec<i64> {
	let batch = concat_batches_for_assertion(batches);
	let ids = batch
		.column(0)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("id column");
	ids.values().to_vec()
}

fn row_key_id_bytes(batch: &RecordBatch) -> Vec<[u8; 32]> {
	let row_key_ids = batch
		.column(batch.num_columns() - 1)
		.as_any()
		.downcast_ref::<FixedSizeBinaryArray>()
		.expect("row key id column");
	assert_eq!(row_key_ids.null_count(), 0);
	(0..row_key_ids.len())
		.map(|row| row_key_ids.value(row).try_into().expect("32-byte row id"))
		.collect()
}

fn whole_record_select_star_options(
	descriptor: &TableSchemaDescriptor,
	row_limit: Option<NonZeroU64>,
) -> TableQueryOptions {
	TableQueryOptions {
		physical_descriptor_fingerprint: Some(descriptor.physical_descriptor_fingerprint),
		row_identity: RowIdentityProjection::Append,
		row_limit,
		..TableQueryOptions::default()
	}
}

fn assert_select_star_prefix_schema(descriptor: &TableSchemaDescriptor, batch: &RecordBatch) {
	let descriptor_schema = descriptor.arrow_schema().expect("descriptor schema");
	assert!(batch.num_columns() > descriptor_schema.fields().len());
	for (actual, expected) in batch
		.schema()
		.fields()
		.iter()
		.take(descriptor_schema.fields().len())
		.zip(descriptor_schema.fields().iter())
	{
		assert_eq!(actual.as_ref(), expected.as_ref());
	}
	assert_eq!(
		batch.schema().field(batch.num_columns() - 1).name(),
		STORAGE_ROW_KEY_ID_COLUMN
	);
}

fn amounts_from_single_column(batches: &[RecordBatch]) -> Vec<i32> {
	let batch = concat_batches_for_assertion(batches);
	assert_eq!(batch.num_columns(), 1);
	let amounts = batch
		.column(0)
		.as_any()
		.downcast_ref::<Int32Array>()
		.expect("amount column");
	amounts.values().to_vec()
}

fn row_count(batches: &[RecordBatch]) -> usize {
	batches.iter().map(RecordBatch::num_rows).sum()
}

fn false_positive_rows(batches: &[RecordBatch], build_ids: &[i64]) -> usize {
	let mut rows = 0usize;
	for batch in batches {
		assert_eq!(batch.num_columns(), 2);
		let ids = batch
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("id column");
		for row in 0..batch.num_rows() {
			if !build_ids.contains(&ids.value(row)) {
				rows += 1;
			}
		}
	}
	rows
}

fn joined_amounts_for_ids(batches: &[RecordBatch], build_ids: &[i64]) -> RecordBatch {
	let mut amounts = Vec::new();
	for batch in batches {
		assert_eq!(batch.num_columns(), 2);
		let ids = batch
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("id column");
		let batch_amounts = batch
			.column(1)
			.as_any()
			.downcast_ref::<Int32Array>()
			.expect("amount column");
		for row in 0..batch.num_rows() {
			if build_ids.contains(&ids.value(row)) {
				amounts.push(if batch_amounts.is_null(row) {
					None
				} else {
					Some(batch_amounts.value(row))
				});
			}
		}
	}
	RecordBatch::try_new(
		Arc::new(Schema::new(vec![Field::new(
			"amount",
			DataType::Int32,
			true,
		)])),
		vec![Arc::new(Int32Array::from(amounts)) as ArrayRef],
	)
	.expect("joined amounts batch")
}

fn hex32(bytes: [u8; 32]) -> String {
	bytes
		.iter()
		.map(|byte| format!("{byte:02x}"))
		.collect::<Vec<_>>()
		.join("")
}

#[tokio::test]
async fn wendelon_storage_scan_maps_server_descriptor_evolution() {
	let fixture = LiveWendelonFixture::start(32).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	write_rows(&fixture.client, LIVE_TENANT, &descriptor, batch()).await;

	let mut evolved = descriptor.clone();
	evolved.record_body_compression_policy = RecordBodyCompressionPolicy::Lz4Frame;
	evolved
		.refresh_descriptor_fingerprints()
		.expect("refresh evolved fingerprint");
	assert_ne!(
		evolved.physical_descriptor_fingerprint,
		descriptor.physical_descriptor_fingerprint
	);
	let pbc = Vec::new();
	let options = TableQueryOptions {
		physical_descriptor_fingerprint: Some(evolved.physical_descriptor_fingerprint),
		..TableQueryOptions::default()
	};
	let err = fixture
		.client
		.table_query(LIVE_TENANT, &evolved, pbc, options)
		.await
		.expect_err("server detects stale physical descriptor pin");

	assert!(
		matches!(
			&err,
			wendelon_client::ClientError::TableDescriptorEvolved { .. }
		),
		"expected TableDescriptorEvolved, got {err:?}"
	);

	fixture.node.handles.abort();
	let _ = std::fs::remove_dir_all(fixture.data_dir);
}

struct LiveWendelonFixture {
	node: WendelonNode,
	client: Arc<Client>,
	data_dir: std::path::PathBuf,
	_serial_permit: tokio::sync::OwnedSemaphorePermit,
}

impl LiveWendelonFixture {
	async fn start(node_id: u32) -> Self {
		let serial_permit = live_wendelon_fixture_semaphore()
			.clone()
			.acquire_owned()
			.await
			.expect("live Wendelon fixture semaphore open");
		let data_dir = std::env::temp_dir().join(format!(
			"toron-wendelon-live-{node_id}-{}",
			rand::random::<u64>()
		));
		std::fs::create_dir_all(&data_dir).expect("create Wendelon data dir");
		let address = format!("sim://node-{node_id}");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			node_id,
			data_dir.clone(),
			address.clone(),
		))
		.await
		.expect("bootstrap Wendelon node");
		wait_for_single_node_leader(&node).await;
		create_live_tenant(&node, LIVE_TENANT).await;
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;

		let pool = BufPool::new(64, 8 * 1024 * 1024);
		let network = Arc::new(SimulatedTransportNetwork::default());
		let client_node = wendelon::schema::NodeId {
			id: node_id + 10_000,
			_reserved: 0,
		};
		let provider: Arc<dyn TransportProvider> = Arc::new(SimulatedTransportProvider::new(
			client_node,
			network,
			pool,
			TransportTier::QuicIoUring,
		));
		provider
			.publish_local(node.local_node, &address, node.service.clone())
			.expect("publish simulated Wendelon service");
		let snapshot = single_node_snapshot(node_id, address);
		let mut providers = HashMap::new();
		providers.insert(TransportTier::QuicIoUring, provider);
		let inner = wendelon::client::WendelonClient::new(wendelon::client::WendelonClientConfig {
			local_tiers: TransportTierSet::empty().with(TransportTier::QuicIoUring),
			providers,
			bootstrap: snapshot.clone(),
			topology: Arc::new(StaticTopologySource::new(snapshot)),
			pool_max_retained: 64,
			pool_max_cap_bytes: 8 * 1024 * 1024,
		});
		let client = Arc::new(Client::with_client_uuid(inner, [node_id as u8; 16]));

		Self {
			node,
			client,
			data_dir,
			_serial_permit: serial_permit,
		}
	}
}

fn live_wendelon_fixture_semaphore() -> &'static Arc<tokio::sync::Semaphore> {
	static LIVE_WENDELON_FIXTURE_SEMAPHORE: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();
	LIVE_WENDELON_FIXTURE_SEMAPHORE.get_or_init(|| Arc::new(tokio::sync::Semaphore::new(1)))
}

async fn create_live_tenant(node: &WendelonNode, tenant: WendelonTenantId) {
	node.create_tenant(tenant, u64::MAX, 1)
		.await
		.expect("create live Toron test tenant");
	node.data_server.sync_tenant_policy_cache_once().await;
}

async fn wait_for_single_node_leader(node: &WendelonNode) {
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			return;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for single-node Wendelon leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
}

fn single_node_snapshot(node_id: u32, address: String) -> ClusterStatusWire {
	ClusterStatusWire {
		epoch: 1,
		leader: Some(node_id),
		replication_factor: 1,
		peers: vec![ClusterPeerWire {
			node_id,
			address,
			tier_bits: TransportTierSet::empty()
				.with(TransportTier::QuicIoUring)
				.as_u8(),
			role_bits: NodeRoleSet::empty()
				.with(NodeRole::Data)
				.with(NodeRole::Control)
				.as_u8(),
			capability_bits: 0,
		}],
	}
}

async fn install_schema(
	node: &WendelonNode,
	tenant: WendelonTenantId,
	descriptor: &TableSchemaDescriptor,
) {
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(schema_update(
			tenant, descriptor,
		)))
		.await
		.expect("install schema");
}

fn schema_update(tenant: WendelonTenantId, descriptor: &TableSchemaDescriptor) -> Vec<u8> {
	let payload = encode_table_schema_descriptor_v1(descriptor).expect("encode descriptor");
	let intent_byte = tenant.get() as u8;
	let envelope = SchemaUpdateEnvelope {
		contract_version: COMPUTE_STORAGE_CONTRACT_VERSION,
		origin: SchemaIntentOrigin::ComputeClient,
		tenant_id: tenant,
		object: SchemaObjectRef {
			kind: SchemaObjectKind::Table,
			namespace: descriptor.namespace.clone(),
			name: descriptor.table_name.clone(),
		},
		expected_generation: SchemaGeneration::ZERO,
		requested_generation: SchemaGeneration::new(descriptor.schema_generation),
		intent_id: SchemaIntentId::new([intent_byte; 16]),
		mutation: SchemaMutationKind::Create,
		payload,
	};
	encode_schema_update_envelope_v1(&envelope).expect("encode schema update")
}

async fn write_rows(
	client: &Client,
	tenant: WendelonTenantId,
	descriptor: &TableSchemaDescriptor,
	batch: RecordBatch,
) {
	let row_count = batch.num_rows();
	let receipt = client
		.table_write(
			tenant,
			descriptor,
			batch,
			WriteOptions {
				await_finalization: true,
				..WriteOptions::default()
			},
		)
		.await
		.expect("write typed table rows");
	assert_eq!(receipt.rows as usize, row_count);
}

fn descriptor() -> TableSchemaDescriptor {
	let schema = schema();
	TableSchemaDescriptor::new(
		TableObjectId::new([0x91; 16]),
		1,
		"bench",
		"storage_execution",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::IdentityProjection,
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("descriptor")
}

fn descriptor_from_sql_created_binding(
	table: &TableRef,
	binding: &TableBinding,
) -> TableSchemaDescriptor {
	let row_identity = match &binding.row_identity {
		TableRowIdentity::PrimaryKey { column, .. } => {
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single(column.as_str()))
		}
		TableRowIdentity::AppendOnly => RowIdentityPolicy::AppendOnly,
		TableRowIdentity::Unsupported => panic!("SQL-created descriptor has unsupported identity"),
	};

	TableSchemaDescriptor::new_with_layout(
		binding.table,
		binding.schema_generation,
		table.schema.as_deref().unwrap_or("default"),
		&table.table,
		binding.schema.as_ref(),
		row_identity,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("descriptor from SQL-created binding")
}

fn schema() -> Schema {
	Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("amount", DataType::Int32, true),
		Field::new("region", DataType::Utf8, true),
		Field::new("ts", DataType::Int64, false),
	])
}

fn batch() -> RecordBatch {
	batch_from_rows(&[
		(1, Some(50), Some("north"), BASE_TS + 5 * 60 * 1000),
		(2, Some(150), Some("east"), BASE_TS + 55 * 60 * 1000),
		(3, None, Some("north"), BASE_TS + HOUR_MS + 15 * 60 * 1000),
		(4, Some(200), Some("west"), BASE_TS + 30 * 60 * 1000),
	])
}

fn batch_from_rows(rows: &[(i64, Option<i32>, Option<&str>, i64)]) -> RecordBatch {
	let schema = Arc::new(schema());
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(
				rows.iter().map(|(id, _, _, _)| *id).collect::<Vec<_>>(),
			)) as ArrayRef,
			Arc::new(Int32Array::from(
				rows.iter()
					.map(|(_, amount, _, _)| *amount)
					.collect::<Vec<_>>(),
			)) as ArrayRef,
			Arc::new(StringArray::from(
				rows.iter()
					.map(|(_, _, region, _)| *region)
					.collect::<Vec<_>>(),
			)) as ArrayRef,
			Arc::new(Int64Array::from(
				rows.iter()
					.map(|(_, _, _, timestamp)| *timestamp)
					.collect::<Vec<_>>(),
			)) as ArrayRef,
		],
	)
	.expect("batch")
}

fn id_notes_from_batches(batches: &[RecordBatch]) -> Vec<(i64, String)> {
	let mut rows = Vec::new();
	for batch in batches {
		let ids = batch
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("id column");
		let notes = batch
			.column(1)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("note column");
		for row in 0..batch.num_rows() {
			rows.push((ids.value(row), notes.value(row).to_string()));
		}
	}
	rows
}

fn affected_count_from_batches(batches: &[RecordBatch]) -> i64 {
	assert_eq!(batches.len(), 1);
	let affected = batches[0]
		.column(0)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("affected column");
	assert_eq!(affected.len(), 1);
	affected.value(0)
}

// Keep the live Wendelon scan comfortably under the fixed table-query wall-clock
// budget; these gates assert runtime-filter boundary reduction, not scan scale.
const RUNTIME_FILTER_BOUNDARY_ROWS: i64 = 128;

fn selective_batch(rows: i64) -> RecordBatch {
	let schema = Arc::new(schema());
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from_iter_values(0..rows)) as ArrayRef,
			Arc::new(Int32Array::from_iter_values(
				(0..rows).map(|row| i32::try_from(row * 10).expect("amount fits i32")),
			)) as ArrayRef,
			Arc::new(StringArray::from(vec![Some("bulk"); rows as usize])) as ArrayRef,
			Arc::new(Int64Array::from_iter_values(
				(0..rows).map(|row| BASE_TS + row * 1000),
			)) as ArrayRef,
		],
	)
	.expect("selective batch")
}

fn pbc_project_amounts_gt_100() -> Vec<u8> {
	let mut bytecode = Vec::new();
	bytecode.push(OP_PUSH_COL);
	bytecode.extend_from_slice(&1_u32.to_le_bytes());
	bytecode.push(OP_PUSH_INT);
	bytecode.extend_from_slice(&100_i64.to_le_bytes());
	bytecode.push(OP_GT);
	bytecode.push(OP_PROJECT);
	bytecode.extend_from_slice(&1_u16.to_le_bytes());
	bytecode.extend_from_slice(&1_u32.to_le_bytes());
	bytecode
}

fn pbc_amount_gt_100() -> Vec<u8> {
	let mut bytecode = Vec::new();
	bytecode.push(OP_PUSH_COL);
	bytecode.extend_from_slice(&1_u32.to_le_bytes());
	bytecode.push(OP_PUSH_INT);
	bytecode.extend_from_slice(&100_i64.to_le_bytes());
	bytecode.push(OP_GT);
	bytecode
}

fn pbc_project_id_amounts() -> Vec<u8> {
	let mut bytecode = Vec::new();
	bytecode.push(OP_PROJECT);
	bytecode.extend_from_slice(&2_u16.to_le_bytes());
	bytecode.extend_from_slice(&0_u32.to_le_bytes());
	bytecode.extend_from_slice(&1_u32.to_le_bytes());
	bytecode
}

fn runtime_filter_for_i64_values(
	descriptor: &TableSchemaDescriptor,
	values: &[i64],
) -> RuntimeFilterDescriptor {
	let bit_count = 4096;
	let hash_count = 3;
	let byte_budget = bit_count / 8;
	let mut payload = vec![0; byte_budget as usize];
	let array = Int64Array::from(values.to_vec());
	for row in 0..array.len() {
		let encoded = encode_runtime_filter_key(&array, row)
			.expect("encode runtime filter key")
			.expect("non-null key");
		insert_runtime_filter_encoded_key(
			&mut payload,
			bit_count,
			hash_count,
			RUNTIME_FILTER_HASH_SEED,
			&encoded,
		);
	}
	RuntimeFilterDescriptor {
		key_encoding_version: RUNTIME_FILTER_KEY_ENCODING_VERSION,
		key_types: vec![RuntimeFilterKeyType::Int64],
		probe_key_columns: vec![0],
		kind: RuntimeFilterKind::Bloom(RuntimeFilterBloom {
			hash_seed: RUNTIME_FILTER_HASH_SEED,
			bit_count,
			hash_count,
			byte_budget,
			target_fp_rate_bp: 100,
			payload,
		}),
		probe_descriptor_fingerprint: descriptor.physical_descriptor_fingerprint,
	}
}

fn runtime_filter_exact_for_i64_values(
	descriptor: &TableSchemaDescriptor,
	values: &[i64],
) -> RuntimeFilterDescriptor {
	let array = Int64Array::from(values.to_vec());
	let mut keys = Vec::new();
	for row in 0..array.len() {
		keys.push(
			encode_runtime_filter_key(&array, row)
				.expect("encode runtime filter key")
				.expect("non-null key"),
		);
	}
	RuntimeFilterDescriptor {
		key_encoding_version: RUNTIME_FILTER_KEY_ENCODING_VERSION,
		key_types: vec![RuntimeFilterKeyType::Int64],
		probe_key_columns: vec![0],
		kind: RuntimeFilterKind::ExactSet(
			wendelon::schema::runtime_filter_exact_set_from_encoded_keys(keys, 16 * 1024)
				.expect("exact runtime filter"),
		),
		probe_descriptor_fingerprint: descriptor.physical_descriptor_fingerprint,
	}
}
