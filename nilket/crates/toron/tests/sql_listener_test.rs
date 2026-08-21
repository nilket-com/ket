use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ArrayRef, Float32Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use rcgen::{
	BasicConstraints, Certificate, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa,
	KeyUsagePurpose,
};
use tokio::io::AsyncWriteExt;
use toron::audit::AuditSinksConfig;
use toron::config::{
	AuditConfig, AuditEnabledConfig, CoordinatorConfig, DescriptorBootstrap,
	DescriptorBootstrapEntry, ObservabilityConfig, SqlConfig, SqlTransport, TlsConfig,
	ToronBootstrapConfig, ToronCatalogConfig, ToronNodeConfig, ToronStorageConfig,
	ToronWendelonBootstrapPeer, ToronWendelonStorageConfig, ToronWorkerConfig, TracingFormat,
};
use toron::daemon::ToronDaemon;
use toron::distributed::budget::{ByteCount, TenantBudget, TenantBudgetPolicy};
use toron::distributed::identity::{QueryId, WorkerId};
use toron::network::tls::{
	DEFAULT_CLUSTER_DOMAIN, canonical_client_dns_name, canonical_daemon_dns_name,
	client_config_from_paths,
};
use toron::quic::QuicEndpoint;
use toron::sql_wire::{
	CancelRequest, Complete, MutationCommitKnowledge, MutationResolution, MutationStopReason,
	RuneFailure, RuneFailureKind, RuneLanguageSurfaceVersion, RuneRequestProtocolVersion,
	RuneSandboxContractVersion, RuneSandboxViolation, RuneVersionAxis, SqlError, SqlErrorCode,
	SqlFrame, SqlMessage, SqlProtocolVersion, read_frame as read_sql_frame,
	write_frame as write_sql_frame,
};
use toron::storage::TenantId as ToronTenantId;
use toron_client::{CtasCompositePhase, ResolvedOperationOutcome, ToronClient};
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
	LocalityPolicy, MultiRowLayoutDescriptor, PrimaryKeyDescriptor, RecordLayoutPolicy,
	RowIdentityPolicy, SchemaEvolutionPolicy, TableObjectId, TableSchemaDescriptor,
	TenantId as WendelonTenantId, encode_table_schema_descriptor_v1,
};
use wendelon::simulator::network::{SimulatedTransportNetwork, SimulatedTransportProvider};
use wendelon_client::{Client, WriteOptions};

const LIVE_TENANT: WendelonTenantId = WendelonTenantId::new(1);
const BASE_TS: i64 = 1_599_998_400_000;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_client_health_probe_reaches_a_live_sql_listener_without_query_state() {
	let fixture = LiveWendelonFixture::start(142).await;
	let config = toron_config_with_descriptor(descriptor(), true);
	let daemon =
		ToronDaemon::start_with_wendelon_client_for_harness(config, Arc::clone(&fixture.client))
			.await
			.expect("start Toron SQL daemon");
	let endpoint = daemon.handle().sql_endpoint.expect("sql endpoint");

	ToronClient::loopback(endpoint.to_string())
		.expect("construct public loopback client")
		.health()
		.await
		.expect("side-effect-free health response");

	daemon.shutdown().await.expect("shutdown daemon");
	fixture.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn toron_sql_select_from_bootstrapped_descriptor_executes_over_wire() {
	let fixture = LiveWendelonFixture::start(43).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	write_rows(&fixture.client, LIVE_TENANT, &descriptor, batch()).await;

	let mut config = toron_config_with_descriptor(descriptor.clone(), true);
	let audit_path = enable_audit(&mut config, "sql-success-audit");
	let daemon =
		ToronDaemon::start_with_wendelon_client_for_harness(config, Arc::clone(&fixture.client))
			.await
			.expect("start Toron SQL daemon");
	let endpoint = daemon.handle().sql_endpoint.expect("sql endpoint");

	let response = execute_sql(
		endpoint,
		"SELECT id, amount, region FROM storage_execution WHERE amount > 100",
	)
	.await
	.expect("SQL select response");

	assert_eq!(response.complete.row_count, 2);
	let mut rows = projected_rows(&response.batches);
	rows.sort();
	assert_eq!(
		rows,
		vec![
			(2, Some(150), Some("east".to_string())),
			(4, Some(200), Some("west".to_string())),
		]
	);
	let audit = wait_for_audit_records(
		&audit_path,
		&[
			"toron.lifecycle.startup",
			"toron.sql.query.completed",
			"query_outcome",
			"row_count_bucket",
		],
	)
	.await;
	assert!(!audit.contains("SELECT id, amount"));
	assert!(!audit.contains("storage_execution"));

	daemon.shutdown().await.expect("shutdown daemon");
	fixture.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rune_source_executes_over_the_authenticated_query_boundary_with_typed_failures() {
	let fixture = LiveWendelonFixture::start(1003).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	write_rows(&fixture.client, LIVE_TENANT, &descriptor, batch()).await;

	let mut config = toron_config_with_descriptor(descriptor, true);
	let audit_path = enable_audit(&mut config, "product-0003-rune-source");
	let daemon =
		ToronDaemon::start_with_wendelon_client_for_harness(config, Arc::clone(&fixture.client))
			.await
			.expect("start Toron daemon");
	let endpoint = daemon.handle().sql_endpoint.expect("query endpoint");
	let client = ToronClient::loopback(endpoint.to_string()).expect("public client");

	let result = client
		.execute_rune(
			r#"pub fn main(workspace) {
				workspace.table("storage_execution")
					.filter(col("amount").gt(lit(100)))
					.project([col("id"), col("amount"), col("region")])
			}"#,
		)
		.await
		.expect("Rune query result");
	let toron_client::RuneQueryResult::Batches(result) = result else {
		panic!("an ordinary Rune relation returns batches, not a receipt");
	};
	assert_eq!(result.row_count, 2);
	let batches = result
		.batches
		.iter()
		.flat_map(|bytes| {
			StreamReader::try_new(bytes.as_slice(), None)
				.expect("open Rune Arrow IPC stream")
				.map(|batch| batch.expect("read Rune batch"))
		})
		.collect::<Vec<_>>();
	assert_eq!(projected_rows(&batches).len(), 2);

	let composed = client
		.execute_rune(
			r#"pub fn main(workspace) {
				workspace.table("storage_execution")
					.filter(col("amount").gt(lit(0)))
					.aggregate([col("region")], [sum(col("amount"))])
					.sort([col("region").asc()])
					.limit(2)
					.project([col("region"), col("sum")])
			}"#,
		)
		.await
		.expect("composed Rune relation result");
	let toron_client::RuneQueryResult::Batches(composed) = composed else {
		panic!("an ordinary Rune relation returns batches, not a receipt");
	};
	assert_eq!(
		composed.row_count, 2,
		"the public Rune route must preserve filter, aggregate, sort, limit, and projection"
	);

	let sandbox = client
		.execute_rune("pub fn main(workspace) { std::fs::read_to_string(\"x\"); workspace.table(\"storage_execution\") }")
		.await
		.expect_err("filesystem capability must be denied");
	assert!(matches!(
		sandbox,
		toron_client::ClientError::Rune(RuneFailure {
			kind: RuneFailureKind::Sandbox(RuneSandboxViolation::CapabilityDenied { .. }),
			..
		})
	));

	let compile = client
		.execute_rune("pub fn main(workspace) { workspace.table( }")
		.await
		.expect_err("invalid Rune must fail compilation");
	let toron_client::ClientError::Rune(RuneFailure {
		kind: RuneFailureKind::Compile,
		span,
		..
	}) = compile
	else {
		panic!("expected typed Rune compile failure: {compile:?}");
	};
	assert!(span.is_some(), "Rune compiler location must cross the wire");

	let audit = wait_for_audit_records(&audit_path, &["query_frontend", "rune"]).await;
	assert!(!audit.contains("\"query_frontend\":\"sql\""));

	// ADR 574 corrective gate: the whole wire path for a materialization, not
	// merely daemon emission. Against the original cut — which appended the
	// response variant but never gave the client a decoder arm — this returns
	// ClientError::Protocol("unexpected response message") and fails here.
	let materialized = client
		.execute_rune(r#"pub fn main(ctx) { ctx.range(6).materialize("wire_kept") }"#)
		.await
		.expect("a materialization must cross the wire as a typed receipt");
	let toron_client::RuneQueryResult::Materialized(receipt) = materialized else {
		panic!("materialize returns a receipt, not batches: {materialized:?}");
	};
	assert_eq!(receipt.table_name, "wire_kept");
	assert!(
		!receipt.namespace.is_empty(),
		"the receipt carries the resolved identity"
	);
	assert_eq!(receipt.rows_written, 6);
	assert_eq!(
		receipt.certificate_format_version,
		boson::V1_FORMAT_VERSION,
		"the receipt reports the format the attachment was recorded at"
	);

	// The created table is queryable by the name the receipt reported.
	let readback = client
		.execute_rune(&format!(
			"pub fn main(ctx) {{ ctx.table(\"{}\") }}",
			receipt.table_name
		))
		.await
		.expect("the materialized table is queryable");
	let toron_client::RuneQueryResult::Batches(readback) = readback else {
		panic!("an ordinary relation returns batches");
	};
	assert_eq!(readback.row_count, 6);

	daemon.shutdown().await.expect("shutdown daemon");
	fixture.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_rune_version_axis_refuses_skew_with_requested_and_supported_versions() {
	let daemon = ToronDaemon::start(toron_config_with_descriptor(descriptor(), true))
		.await
		.expect("start daemon");
	let endpoint = daemon.handle().sql_endpoint.expect("query endpoint");
	for axis in [
		RuneVersionAxis::RequestProtocol,
		RuneVersionAxis::LanguageSurface,
		RuneVersionAxis::SandboxContract,
	] {
		let mut frame = SqlFrame::execute_rune(
			QueryId::new(),
			"pub fn main(workspace) { workspace.table(\"storage_execution\") }".to_owned(),
			None,
		);
		let SqlMessage::ExecuteRune(request) = &mut frame.message else {
			unreachable!();
		};
		match axis {
			RuneVersionAxis::RequestProtocol => {
				request.request_protocol_version = RuneRequestProtocolVersion(77)
			}
			RuneVersionAxis::LanguageSurface => {
				request.language_surface_version = RuneLanguageSurfaceVersion(77)
			}
			RuneVersionAxis::SandboxContract => {
				request.sandbox_contract_version = RuneSandboxContractVersion(77)
			}
		}
		let failure = execute_rune_until_failure(endpoint, frame).await;
		let RuneFailureKind::VersionSkew(skew) = failure.kind else {
			panic!("expected version skew for {axis:?}: {failure:?}");
		};
		assert_eq!(skew.axis, axis);
		assert_eq!(skew.requested, 77);
		// ADR 574: each axis reports its own current version. The request
		// protocol advanced for the materialization receipt; the language
		// surface and sandbox contract did not, and asserting them by value
		// here is what fails a reflex bump of all three.
		let expected = match axis {
			RuneVersionAxis::RequestProtocol => RuneRequestProtocolVersion::CURRENT.0,
			RuneVersionAxis::LanguageSurface => RuneLanguageSurfaceVersion::CURRENT.0,
			RuneVersionAxis::SandboxContract => RuneSandboxContractVersion::CURRENT.0,
		};
		assert_eq!(skew.supported, vec![expected]);
	}
	assert_eq!(
		(
			RuneRequestProtocolVersion::CURRENT.0,
			RuneLanguageSurfaceVersion::CURRENT.0,
			RuneSandboxContractVersion::CURRENT.0,
		),
		(2, 1, 1),
		"ADR 574 moves the request-protocol axis alone"
	);
	daemon.shutdown().await.expect("shutdown daemon");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn order_by_and_top_k_return_ordered_rows_over_live_wendelon() {
	let fixture = LiveWendelonFixture::start(535).await;
	let descriptor = categorical_descriptor();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	let rows = [
		("Cat4", 4.4),
		("Cat1", 1.1),
		("Cat6", 6.6),
		("Cat3", 3.3),
		("Cat2", 2.2),
		("Cat5", 5.5),
		("Cat3", 33.3),
	];
	for (cat, val) in rows {
		write_rows(
			&fixture.client,
			LIVE_TENANT,
			&descriptor,
			categorical_batch(cat, val),
		)
		.await;
	}

	// Wendelon ADR 0088: response batching means the number of frames a live
	// producer emits no longer tracks the number of stored rows, so this test
	// can no longer establish "more batches than the pacing depth" by counting
	// them. That subject moved to a Toron-owned synthetic producer —
	// `order_by_completes_when_the_producer_exceeds_the_live_pacing_depth` in
	// sort_ordering_test.rs — which emits seven batches by construction. What
	// remains here is what this test uniquely owns: a global sort and a top-k
	// over a real Wendelon node return the right rows in the right order,
	// within the statement timeout.

	let mut config = toron_config_with_descriptor(descriptor, true);
	let audit_path = enable_audit(&mut config, "adr535-global-sort");
	let daemon =
		ToronDaemon::start_with_wendelon_client_for_harness(config, Arc::clone(&fixture.client))
			.await
			.expect("start Toron SQL daemon");
	let endpoint = daemon.handle().sql_endpoint.expect("sql endpoint");

	let ascending = tokio::time::timeout(
		Duration::from_secs(2),
		execute_sql(endpoint, "SELECT cat, val FROM test2 ORDER BY cat"),
	)
	.await
	.expect("ORDER BY must not wait for statement timeout")
	.expect("ascending ORDER BY response");
	assert_eq!(
		categorical_rows(&ascending.batches),
		vec![
			("Cat1".to_string(), 1.1),
			("Cat2".to_string(), 2.2),
			("Cat3".to_string(), 3.3),
			("Cat3".to_string(), 33.3),
			("Cat4".to_string(), 4.4),
			("Cat5".to_string(), 5.5),
			("Cat6".to_string(), 6.6),
		]
	);

	let descending = execute_sql(
		endpoint,
		"SELECT cat, val FROM test2 ORDER BY cat DESC, val DESC LIMIT 3 OFFSET 1",
	)
	.await
	.expect("descending top-k response");
	assert_eq!(
		categorical_rows(&descending.batches),
		vec![
			("Cat5".to_string(), 5.5),
			("Cat4".to_string(), 4.4),
			("Cat3".to_string(), 33.3),
		]
	);

	let audit = wait_for_audit_records(&audit_path, &["toron.sql.query.completed"]).await;
	assert!(!audit.contains("toron.sql.query.timed_out"));

	daemon.shutdown().await.expect("shutdown daemon");
	fixture.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_statement_timeout_is_not_laundered_into_a_planning_refusal() {
	let fixture = LiveWendelonFixture::start(536).await;
	let descriptor = categorical_descriptor();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	write_rows(
		&fixture.client,
		LIVE_TENANT,
		&descriptor,
		categorical_batch("Cat1", 1.1),
	)
	.await;

	let mut config = toron_config_with_descriptor(descriptor, true);
	config.sql.as_mut().expect("SQL config").statement_timeout = Duration::from_nanos(1);
	let audit_path = enable_audit(&mut config, "adr535-real-timeout");
	let daemon =
		ToronDaemon::start_with_wendelon_client_for_harness(config, Arc::clone(&fixture.client))
			.await
			.expect("start Toron SQL daemon");
	let endpoint = daemon.handle().sql_endpoint.expect("sql endpoint");

	let error = execute_sql(endpoint, "SELECT cat, val FROM test2")
		.await
		.expect_err("one-nanosecond statement budget must time out");
	assert_eq!(error.code, SqlErrorCode::StatementTimeout);
	let audit = wait_for_audit_records(&audit_path, &["toron.sql.query.timed_out"]).await;
	assert!(!audit.contains("planning_error"));

	daemon.shutdown().await.expect("shutdown daemon");
	fixture.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fail_closed_sql_success_audit_failure_returns_internal_before_result_batches() {
	let fixture = LiveWendelonFixture::start(84).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	write_rows(&fixture.client, LIVE_TENANT, &descriptor, batch()).await;

	let mut config = toron_config_with_descriptor(descriptor.clone(), true);
	enable_fail_closed_unavailable_audit(&mut config, "sql-fail-closed-success-audit");
	let daemon =
		ToronDaemon::start_with_wendelon_client_for_harness(config, Arc::clone(&fixture.client))
			.await
			.expect("start Toron SQL daemon");
	let endpoint = daemon.handle().sql_endpoint.expect("sql endpoint");

	let (batches, error) = execute_sql_until_error(
		endpoint,
		"SELECT id, amount, region FROM storage_execution WHERE amount > 100",
	)
	.await;

	assert!(
		batches.is_empty(),
		"result batches were streamed before required terminal audit"
	);
	assert_eq!(error.code, SqlErrorCode::Internal);
	assert_eq!(error.message, "audit unavailable");

	daemon.shutdown().await.expect("shutdown daemon");
	fixture.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_listener_update_complete_frame_carries_row_count() {
	let fixture = LiveWendelonFixture::start(53).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	write_rows(&fixture.client, LIVE_TENANT, &descriptor, batch()).await;

	let config = toron_config_with_descriptor(descriptor.clone(), true);
	let daemon =
		ToronDaemon::start_with_wendelon_client_for_harness(config, Arc::clone(&fixture.client))
			.await
			.expect("start Toron SQL daemon");
	let endpoint = daemon.handle().sql_endpoint.expect("sql endpoint");

	let update = execute_sql(
		endpoint,
		"UPDATE storage_execution SET amount = 175 WHERE id = 2",
	)
	.await
	.expect("SQL update response");

	assert_eq!(update.complete.row_count, 1);
	assert!(update.batches.is_empty());

	let response = execute_sql(
		endpoint,
		"SELECT id, amount, region FROM storage_execution WHERE id = 2",
	)
	.await
	.expect("SQL select after update");
	assert_eq!(response.complete.row_count, 1);
	assert_eq!(
		projected_rows(&response.batches),
		vec![(2, Some(175), Some("east".to_string()))]
	);

	daemon.shutdown().await.expect("shutdown daemon");
	fixture.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_listener_dml_returning_sends_result_batches_then_complete() {
	let fixture = LiveWendelonFixture::start(54).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	write_rows(&fixture.client, LIVE_TENANT, &descriptor, batch()).await;

	let config = toron_config_with_descriptor(descriptor.clone(), true);
	let daemon =
		ToronDaemon::start_with_wendelon_client_for_harness(config, Arc::clone(&fixture.client))
			.await
			.expect("start Toron SQL daemon");
	let endpoint = daemon.handle().sql_endpoint.expect("sql endpoint");

	let response = execute_sql(
		endpoint,
		"UPDATE storage_execution SET amount = 175 WHERE id = 2 RETURNING id, amount, region",
	)
	.await
	.expect("SQL update returning response");

	assert_eq!(response.complete.row_count, 1);
	assert_eq!(
		projected_rows(&response.batches),
		vec![(2, Some(175), Some("east".to_string()))]
	);

	daemon.shutdown().await.expect("shutdown daemon");
	fixture.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn toron_sql_select_executes_over_authenticated_remote_listener() {
	let fixture = LiveWendelonFixture::start(45).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	write_rows(&fixture.client, LIVE_TENANT, &descriptor, batch()).await;

	let worker_id = WorkerId::from_u128(45);
	let pki = TestPki::generate(worker_id, "1");
	let config = toron_config_with_descriptor_tls(descriptor.clone(), worker_id, &pki);
	let daemon =
		ToronDaemon::start_with_wendelon_client_for_harness(config, Arc::clone(&fixture.client))
			.await
			.expect("start Toron SQL daemon");
	let endpoint = connectable_loopback_addr(daemon.handle().sql_endpoint.expect("sql endpoint"));
	let server_name = canonical_daemon_dns_name(&worker_id, DEFAULT_CLUSTER_DOMAIN);

	let response = execute_sql_authenticated(
		endpoint,
		&server_name,
		&pki.client_cert,
		&pki.client_key,
		&pki.ca_cert,
		"SELECT id, amount, region FROM storage_execution WHERE amount > 100",
	)
	.await
	.expect("SQL select response over mTLS");

	assert_eq!(response.complete.row_count, 2);
	let mut rows = projected_rows(&response.batches);
	rows.sort();
	assert_eq!(
		rows,
		vec![
			(2, Some(150), Some("east".to_string())),
			(4, Some(200), Some("west".to_string())),
		]
	);
	let rune = execute_rune_authenticated(
		endpoint,
		&server_name,
		&pki.client_cert,
		&pki.client_key,
		&pki.ca_cert,
		"pub fn main(workspace) { workspace.table(\"storage_execution\").project([col(\"id\")]) }",
	)
	.await;
	assert_eq!(
		rune.complete.row_count, 4,
		"Rune must inherit SQL's authenticated transport and tenant authorization"
	);

	daemon.shutdown().await.expect("shutdown daemon");
	fixture.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tenant_7_sql_resolves_tenant_7_bootstrapped_descriptor() {
	let fixture = LiveWendelonFixture::start(46).await;
	let tenant = WendelonTenantId::new(7);
	create_live_tenant(&fixture.node, tenant).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, tenant, &descriptor).await;
	write_rows(&fixture.client, tenant, &descriptor, batch()).await;

	let worker_id = WorkerId::from_u128(46);
	let pki = TestPki::generate(worker_id, "7");
	let mut config = toron_config_with_descriptor_tls_for_tenant(
		descriptor.clone(),
		ToronTenantId(7),
		worker_id,
		&pki,
	);
	config.node.tenant_id = ToronTenantId(7);
	let daemon =
		ToronDaemon::start_with_wendelon_client_for_harness(config, Arc::clone(&fixture.client))
			.await
			.expect("start Toron SQL daemon");
	let endpoint = connectable_loopback_addr(daemon.handle().sql_endpoint.expect("sql endpoint"));
	let server_name = canonical_daemon_dns_name(&worker_id, DEFAULT_CLUSTER_DOMAIN);

	let response = execute_sql_authenticated(
		endpoint,
		&server_name,
		&pki.client_cert,
		&pki.client_key,
		&pki.ca_cert,
		"SELECT id, amount, region FROM storage_execution WHERE amount > 100",
	)
	.await
	.expect("SQL select response over mTLS");
	assert_eq!(response.complete.row_count, 2);

	daemon.shutdown().await.expect("shutdown daemon");
	fixture.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tenant_8_sql_resolves_tenant_8_bootstrapped_descriptor() {
	let fixture = LiveWendelonFixture::start(49).await;
	let tenant = WendelonTenantId::new(8);
	create_live_tenant(&fixture.node, tenant).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, tenant, &descriptor).await;
	write_rows(&fixture.client, tenant, &descriptor, batch()).await;

	let worker_id = WorkerId::from_u128(49);
	let pki = TestPki::generate(worker_id, "8");
	let mut config = toron_config_with_descriptor_tls_for_tenant(
		descriptor.clone(),
		ToronTenantId(8),
		worker_id,
		&pki,
	);
	config.node.tenant_id = ToronTenantId(7);
	let daemon =
		ToronDaemon::start_with_wendelon_client_for_harness(config, Arc::clone(&fixture.client))
			.await
			.expect("start Toron SQL daemon");
	let endpoint = connectable_loopback_addr(daemon.handle().sql_endpoint.expect("sql endpoint"));
	let server_name = canonical_daemon_dns_name(&worker_id, DEFAULT_CLUSTER_DOMAIN);

	let response = execute_sql_authenticated(
		endpoint,
		&server_name,
		&pki.client_cert,
		&pki.client_key,
		&pki.ca_cert,
		"SELECT id, amount, region FROM storage_execution WHERE amount > 100",
	)
	.await
	.expect("tenant 8 SQL select response over mTLS");
	assert_eq!(response.complete.row_count, 2);

	daemon.shutdown().await.expect("shutdown daemon");
	fixture.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tenant_8_cannot_resolve_tenant_7_only_bootstrapped_descriptor() {
	let fixture = LiveWendelonFixture::start(47).await;
	let tenant = WendelonTenantId::new(7);
	create_live_tenant(&fixture.node, tenant).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, tenant, &descriptor).await;
	write_rows(&fixture.client, tenant, &descriptor, batch()).await;

	let worker_id = WorkerId::from_u128(47);
	let pki = TestPki::generate(worker_id, "8");
	let mut config =
		toron_config_with_descriptor_tls_for_tenant(descriptor, ToronTenantId(7), worker_id, &pki);
	config.node.tenant_id = ToronTenantId(7);
	let daemon =
		ToronDaemon::start_with_wendelon_client_for_harness(config, Arc::clone(&fixture.client))
			.await
			.expect("start Toron SQL daemon");
	let endpoint = connectable_loopback_addr(daemon.handle().sql_endpoint.expect("sql endpoint"));
	let server_name = canonical_daemon_dns_name(&worker_id, DEFAULT_CLUSTER_DOMAIN);

	let error = execute_sql_authenticated(
		endpoint,
		&server_name,
		&pki.client_cert,
		&pki.client_key,
		&pki.ca_cert,
		"SELECT id, amount, region FROM storage_execution WHERE amount > 100",
	)
	.await
	.expect_err("tenant 8 must not see tenant 7 bootstrapped descriptor");
	assert_eq!(error.code, SqlErrorCode::CatalogError);
	assert!(
		error.message.contains("storage_execution") || error.message.contains("TableId"),
		"{error:?}"
	);

	daemon.shutdown().await.expect("shutdown daemon");
	fixture.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loopback_sql_uses_config_tenant_for_bootstrapped_descriptor() {
	let fixture = LiveWendelonFixture::start(50).await;
	let tenant = WendelonTenantId::new(7);
	create_live_tenant(&fixture.node, tenant).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, tenant, &descriptor).await;
	write_rows(&fixture.client, tenant, &descriptor, batch()).await;

	let mut config = toron_config_with_descriptor_for_tenant(descriptor, ToronTenantId(7), true);
	config.node.tenant_id = ToronTenantId(7);
	let daemon =
		ToronDaemon::start_with_wendelon_client_for_harness(config, Arc::clone(&fixture.client))
			.await
			.expect("start Toron SQL daemon");
	let endpoint = daemon.handle().sql_endpoint.expect("sql endpoint");

	let response = execute_sql(
		endpoint,
		"SELECT id, amount, region FROM storage_execution WHERE amount > 100",
	)
	.await
	.expect("loopback tenant 7 SQL select response");

	assert_eq!(response.complete.row_count, 2);

	daemon.shutdown().await.expect("shutdown daemon");
	fixture.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn global_sql_connection_cap_refuses_excess_incoming_before_handshake() {
	let mut config = toron_config_with_descriptor(descriptor(), true);
	config
		.sql
		.as_mut()
		.expect("sql config")
		.max_concurrent_connections = NonZeroU32::new(1);
	let daemon = ToronDaemon::start(config).await.expect("start daemon");
	let endpoint = daemon.handle().sql_endpoint.expect("sql endpoint");
	let client = QuicEndpoint::client().expect("first sql client");
	let _held_stream = client
		.connect_stream(endpoint)
		.await
		.expect("first SQL connection");

	assert_transport_rejects_without_sql_frame(endpoint).await;

	daemon.shutdown().await.expect("shutdown daemon");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loopback_sql_tenant_connection_cap_uses_config_tenant() {
	let mut config = toron_config_with_descriptor(descriptor(), true);
	config.node.tenant_id = ToronTenantId(7);
	config.tenant_budgets = TenantBudgetPolicy::new(
		TenantBudget::new(ByteCount::new(8), ByteCount::new(32)),
		HashMap::from([(ToronTenantId(7), tenant_connection_budget(1))]),
	);
	let daemon = ToronDaemon::start(config).await.expect("start daemon");
	let endpoint = daemon.handle().sql_endpoint.expect("sql endpoint");
	let client = QuicEndpoint::client().expect("first sql client");
	let _held_stream = client
		.connect_stream(endpoint)
		.await
		.expect("first loopback tenant SQL connection");

	assert_transport_rejects_without_sql_frame(endpoint).await;

	daemon.shutdown().await.expect("shutdown daemon");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_sql_tenant_connection_cap_closes_excess_after_san_resolution() {
	let worker_id = WorkerId::from_u128(57);
	let pki = TestPki::generate(worker_id, "7");
	let mut config = toron_config_with_descriptor_tls_for_tenant(
		descriptor(),
		ToronTenantId(7),
		worker_id,
		&pki,
	);
	config.tenant_budgets = TenantBudgetPolicy::new(
		TenantBudget::new(ByteCount::new(8), ByteCount::new(32)),
		HashMap::from([(ToronTenantId(7), tenant_connection_budget(1))]),
	);
	let daemon = ToronDaemon::start(config).await.expect("start daemon");
	let endpoint = connectable_loopback_addr(daemon.handle().sql_endpoint.expect("sql endpoint"));
	let server_name = canonical_daemon_dns_name(&worker_id, DEFAULT_CLUSTER_DOMAIN);
	let first_client_config =
		client_config_from_paths(&pki.client_cert, &pki.client_key, &pki.ca_cert)
			.expect("first client TLS config");
	let first_client =
		QuicEndpoint::client_authenticated(first_client_config).expect("first sql client");
	let _held_stream = first_client
		.connect_stream_with_server_name(endpoint, &server_name)
		.await
		.expect("first remote tenant SQL connection");

	assert_authenticated_transport_rejects_without_sql_frame(
		endpoint,
		&server_name,
		&pki.client_cert,
		&pki.client_key,
		&pki.ca_cert,
	)
	.await;

	daemon.shutdown().await.expect("shutdown daemon");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_declared_request_length_rejected_before_allocation_or_decode() {
	let mut config = toron_config_with_descriptor(descriptor(), true);
	let sql = config.sql.as_mut().expect("sql config");
	sql.max_request_frame_bytes = NonZeroU32::new(8).expect("request cap");
	sql.max_statement_bytes = NonZeroU32::new(8).expect("statement cap");
	let audit_path = enable_audit(&mut config, "request-frame-audit");
	let daemon = ToronDaemon::start(config).await.expect("start daemon");
	let endpoint = daemon.handle().sql_endpoint.expect("sql endpoint");
	let client = QuicEndpoint::client().expect("oversized sql client");
	let mut stream = client.connect_stream(endpoint).await.expect("connect sql");

	stream
		.write_all(&9_u32.to_le_bytes())
		.await
		.expect("write oversized declared length");
	stream.finish().await.expect("finish oversized request");
	let read = tokio::time::timeout(Duration::from_secs(1), read_sql_frame(&mut stream))
		.await
		.expect("transport should close without waiting for payload");

	assert!(
		read.is_err(),
		"oversized request-frame rejection must not emit a SQL error frame"
	);
	let audit = wait_for_audit_records(
		&audit_path,
		&["toron.sql.edge.request_frame_too_large", "edge_denial"],
	)
	.await;
	assert!(!audit.contains("declared_len"));

	daemon.shutdown().await.expect("shutdown daemon");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_decoded_execute_statement_returns_resource_exhausted_without_planning() {
	let mut config = toron_config_with_descriptor(descriptor(), true);
	let sql = config.sql.as_mut().expect("sql config");
	sql.max_request_frame_bytes = NonZeroU32::new(4096).expect("request cap");
	sql.max_statement_bytes = NonZeroU32::new(8).expect("statement cap");
	let audit_path = enable_audit(&mut config, "statement-cap-audit");
	let daemon = ToronDaemon::start(config).await.expect("start daemon");
	let endpoint = daemon.handle().sql_endpoint.expect("sql endpoint");

	let error = execute_sql(endpoint, "not valid sql and over the cap")
		.await
		.expect_err("oversized statement rejected");

	assert_eq!(error.code, SqlErrorCode::ResourceExhausted);
	assert!(error.message.contains("configured byte limit"), "{error:?}");
	let audit = wait_for_audit_records(
		&audit_path,
		&["toron.sql.edge.statement_too_large", "edge_denial"],
	)
	.await;
	assert!(!audit.contains("not valid sql"));

	daemon.shutdown().await.expect("shutdown daemon");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn audit_records_planned_error_without_raw_sql() {
	let mut config = toron_config_with_descriptor(descriptor(), true);
	let audit_path = enable_audit(&mut config, "planned-error-audit");
	let daemon = ToronDaemon::start(config).await.expect("start daemon");
	let endpoint = daemon.handle().sql_endpoint.expect("sql endpoint");

	let error = execute_sql(endpoint, "not valid sql")
		.await
		.expect_err("parse error");

	assert_eq!(error.code, SqlErrorCode::SyntaxError);
	let audit = wait_for_audit_records(
		&audit_path,
		&[
			"toron.sql.query.planned_error",
			"sql_error_code_class",
			"syntax_error",
			"statement_class",
			"unknown",
		],
	)
	.await;
	assert!(!audit.contains("not valid sql"));

	daemon.shutdown().await.expect("shutdown daemon");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_below_request_frame_cap_is_not_statement_capped() {
	let mut config = toron_config_with_descriptor(descriptor(), true);
	let sql = config.sql.as_mut().expect("sql config");
	sql.max_request_frame_bytes = NonZeroU32::new(4096).expect("request cap");
	sql.max_statement_bytes = NonZeroU32::new(1).expect("statement cap");
	let audit_path = enable_audit(&mut config, "cancel-audit");
	let daemon = ToronDaemon::start(config).await.expect("start daemon");
	let endpoint = daemon.handle().sql_endpoint.expect("sql endpoint");
	let client = QuicEndpoint::client().expect("cancel sql client");
	let mut stream = client.connect_stream(endpoint).await.expect("connect sql");
	let query_id = QueryId::new();
	write_sql_frame(
		&mut stream,
		&SqlFrame {
			protocol_version: SqlProtocolVersion::Current,
			query_id,
			message: SqlMessage::Cancel(CancelRequest { query_id }),
		},
	)
	.await
	.expect("write cancel");
	stream.finish().await.expect("finish cancel request");

	let frame = read_sql_frame(&mut stream)
		.await
		.expect("read cancel response");
	match frame.message {
		SqlMessage::Error(error) => {
			assert_eq!(error.code, SqlErrorCode::Cancelled);
		}
		other => panic!("unexpected cancel response: {other:?}"),
	}
	let audit = wait_for_audit_records(
		&audit_path,
		&["toron.sql.query.cancelled", "statement_class", "cancel"],
	)
	.await;
	assert!(!audit.contains("query cancellation accepted"));

	daemon.shutdown().await.expect("shutdown daemon");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_result_batches_not_constrained_by_inbound_request_cap() {
	let fixture = LiveWendelonFixture::start(51).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	let rows = (0..64)
		.map(|index| {
			(
				i64::from(index),
				Some(index),
				Some("region"),
				BASE_TS + i64::from(index) * 60 * 1000,
			)
		})
		.collect::<Vec<_>>();
	write_rows(
		&fixture.client,
		LIVE_TENANT,
		&descriptor,
		batch_from_rows(&rows),
	)
	.await;

	let mut config = toron_config_with_descriptor(descriptor, true);
	let sql = config.sql.as_mut().expect("sql config");
	sql.max_request_frame_bytes = NonZeroU32::new(512).expect("request cap");
	sql.max_statement_bytes = NonZeroU32::new(512).expect("statement cap");
	let daemon =
		ToronDaemon::start_with_wendelon_client_for_harness(config, Arc::clone(&fixture.client))
			.await
			.expect("start Toron SQL daemon");
	let endpoint = daemon.handle().sql_endpoint.expect("sql endpoint");
	let client = QuicEndpoint::client().expect("sql client");
	let mut stream = client.connect_stream(endpoint).await.expect("connect sql");
	let query_id = QueryId::new();
	write_sql_frame(
		&mut stream,
		&SqlFrame::execute(
			query_id,
			"SELECT id, amount, region, ts FROM storage_execution".to_string(),
			None,
		),
	)
	.await
	.expect("write sql request");
	stream.finish().await.expect("finish sql request");

	let mut saw_result_batch_larger_than_cap = false;
	loop {
		let frame = read_sql_frame(&mut stream)
			.await
			.expect("read sql response");
		match frame.message {
			SqlMessage::ResultBatch(batch) => {
				saw_result_batch_larger_than_cap |= batch.arrow_ipc_bytes.len() > 512;
			}
			SqlMessage::Complete(complete) => {
				assert_eq!(complete.row_count, 64);
				break;
			}
			SqlMessage::Error(error) => panic!("unexpected SQL error: {error:?}"),
			other => panic!("unexpected SQL response frame: {other:?}"),
		}
	}

	assert!(
		saw_result_batch_larger_than_cap,
		"outbound result batches must not be constrained by inbound request cap"
	);

	daemon.shutdown().await.expect("shutdown daemon");
	fixture.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_over_per_task_budget_returns_resource_exhausted() {
	let fixture = LiveWendelonFixture::start(48).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	write_rows(&fixture.client, LIVE_TENANT, &descriptor, batch()).await;

	let mut config = toron_config_with_descriptor(descriptor, true);
	config.tenant_budgets = TenantBudgetPolicy::new(
		TenantBudget::new(ByteCount::new(1), ByteCount::new(1)),
		HashMap::new(),
	);
	let daemon =
		ToronDaemon::start_with_wendelon_client_for_harness(config, Arc::clone(&fixture.client))
			.await
			.expect("start Toron SQL daemon");
	let endpoint = daemon.handle().sql_endpoint.expect("sql endpoint");

	let error = execute_sql(
		endpoint,
		"SELECT id, amount, region FROM storage_execution ORDER BY amount",
	)
	.await
	.expect_err("sort should exceed tenant per-task budget");

	assert_eq!(error.code, SqlErrorCode::ResourceExhausted);
	assert_eq!(error.message, "tenant memory budget exhausted");

	daemon.shutdown().await.expect("shutdown daemon");
	fixture.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn toron_sql_uses_self_worker_registry_entry() {
	let descriptor = descriptor();
	let config = toron_config_with_descriptor(descriptor, false);
	let daemon = ToronDaemon::start(config).await.expect("start daemon");
	let endpoint = daemon.handle().sql_endpoint.expect("sql endpoint");

	let error = execute_sql(endpoint, "SELECT id FROM storage_execution")
		.await
		.expect_err("self-worker disabled should leave no live worker");

	assert_eq!(error.code, SqlErrorCode::NoLiveWorker);

	daemon.shutdown().await.expect("shutdown daemon");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn toron_sql_complete_frame_carries_stable_row_count() {
	let fixture = LiveWendelonFixture::start(44).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	write_rows(&fixture.client, LIVE_TENANT, &descriptor, batch()).await;

	let config = toron_config_with_descriptor(descriptor, true);
	let daemon =
		ToronDaemon::start_with_wendelon_client_for_harness(config, Arc::clone(&fixture.client))
			.await
			.expect("start Toron SQL daemon");
	let endpoint = daemon.handle().sql_endpoint.expect("sql endpoint");

	let create_existing = execute_sql(
		endpoint,
		"CREATE TABLE IF NOT EXISTS storage_execution \
		 (id BIGINT NOT NULL PRIMARY KEY, amount INT, region TEXT, ts BIGINT NOT NULL)",
	)
	.await
	.expect("create-if-not-exists response");
	assert_eq!(create_existing.complete.row_count, 0);
	assert!(create_existing.batches.is_empty());

	let insert = execute_sql(
		endpoint,
		"INSERT INTO storage_execution (id, amount, region, ts) \
		 VALUES (5, 250, 'south', 1599998400000)",
	)
	.await
	.expect("insert response");
	assert_eq!(insert.complete.row_count, 1);
	assert!(insert.batches.is_empty());

	let delete = execute_sql(endpoint, "DELETE FROM storage_execution WHERE id = 5")
		.await
		.expect("delete response");
	assert_eq!(delete.complete.row_count, 1);
	assert!(delete.batches.is_empty());

	let drop_missing = execute_sql(endpoint, "DROP TABLE IF EXISTS missing_table")
		.await
		.expect("drop-if-exists response");
	assert_eq!(drop_missing.complete.row_count, 0);
	assert!(drop_missing.batches.is_empty());

	daemon.shutdown().await.expect("shutdown daemon");
	fixture.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_sql_client_resolves_a_mutation_by_id_without_replaying_it() {
	let fixture = LiveWendelonFixture::start(542).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	let config = toron_config_with_descriptor(descriptor, true);
	let daemon =
		ToronDaemon::start_with_wendelon_client_for_harness(config, Arc::clone(&fixture.client))
			.await
			.expect("start Toron SQL daemon");
	let endpoint = daemon.handle().sql_endpoint.expect("sql endpoint");
	let client = ToronClient::loopback(endpoint.to_string()).expect("public client");
	let operation_query_id = QueryId::new();

	client
		.execute_with_query_id(
			operation_query_id,
			"INSERT INTO storage_execution (id, amount, region, ts) \
			 VALUES (542, 7, 'resolved', 1599998400000)",
			"UTC",
		)
		.await
		.expect("execute named mutation");
	let outcome = client
		.resolve_mutation(operation_query_id)
		.await
		.expect("resolve without replay");
	let ResolvedOperationOutcome::Mutation(outcome) = outcome else {
		panic!("ordinary INSERT must resolve as a row mutation");
	};

	assert_eq!(outcome.operation_query_id, operation_query_id);
	assert_eq!(outcome.resolution, MutationResolution::Present);
	assert_eq!(outcome.write_status, 0);
	assert_eq!(outcome.stop_reason, MutationStopReason::None);
	assert_eq!(outcome.commit_knowledge, MutationCommitKnowledge::Committed);
	assert_eq!(outcome.delete_counts, None);

	let rows = client
		.execute("SELECT id FROM storage_execution WHERE id = 542")
		.await
		.expect("read mutation result");
	assert_eq!(
		rows.row_count, 1,
		"resolution must not submit a second write"
	);

	daemon.shutdown().await.expect("shutdown daemon");
	fixture.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_sql_client_resolves_all_four_ctas_phases_by_id() {
	let fixture = LiveWendelonFixture::start(548).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	write_rows(&fixture.client, LIVE_TENANT, &descriptor, batch()).await;
	let config = toron_config_with_descriptor(descriptor, true);
	let daemon =
		ToronDaemon::start_with_wendelon_client_for_harness(config, Arc::clone(&fixture.client))
			.await
			.expect("start Toron SQL daemon");
	let endpoint = daemon.handle().sql_endpoint.expect("sql endpoint");
	let client = ToronClient::loopback(endpoint.to_string()).expect("public client");
	let operation_query_id = QueryId::new();

	client
		.execute_with_query_id(
			operation_query_id,
			"CREATE TABLE ctas_resolution AS SELECT id, amount, region, ts FROM storage_execution",
			"UTC",
		)
		.await
		.expect("execute named CTAS");
	let outcome = client
		.resolve_mutation(operation_query_id)
		.await
		.expect("resolve composite CTAS without replay");
	let ResolvedOperationOutcome::Ctas(outcome) = outcome else {
		panic!("CTAS must resolve through the composite outcome surface");
	};

	assert_eq!(outcome.operation_query_id, operation_query_id);
	assert_eq!(outcome.phase, CtasCompositePhase::Complete);
	assert_eq!(outcome.row_count, Some(4));
	assert_eq!(
		outcome.catalog_commit_knowledge,
		MutationCommitKnowledge::Committed
	);
	assert_eq!(
		outcome.row_commit_knowledge,
		MutationCommitKnowledge::Committed
	);
	assert_eq!(
		outcome.publication_commit_knowledge,
		MutationCommitKnowledge::Committed
	);
	assert_eq!(
		outcome.retirement_commit_knowledge,
		MutationCommitKnowledge::Committed
	);
	assert_eq!(outcome.catalog_cause, None);
	assert_eq!(outcome.row_cause, None);
	assert_eq!(outcome.publication_cause, None);
	assert_eq!(outcome.retirement_cause, None);

	let rows = client
		.execute("SELECT id FROM ctas_resolution")
		.await
		.expect("published CTAS remains queryable after resolution");
	assert_eq!(rows.row_count, 4);

	let replacement_query_id = QueryId::new();
	client
		.execute_with_query_id(
			replacement_query_id,
			"CREATE OR REPLACE TABLE ctas_resolution AS SELECT id, amount, region, ts FROM storage_execution WHERE id > 2",
			"UTC",
		)
		.await
		.expect("stage, publish, and retire a replacement over live Wendelon");
	let ResolvedOperationOutcome::Ctas(replacement) = client
		.resolve_mutation(replacement_query_id)
		.await
		.expect("resolve completed replacement")
	else {
		panic!("replacement CTAS must retain its composite outcome");
	};
	assert_eq!(replacement.phase, CtasCompositePhase::Complete);
	assert_eq!(replacement.row_count, Some(2));
	let rows = client
		.execute("SELECT id FROM ctas_resolution")
		.await
		.expect("replacement remains queryable after old storage retirement");
	assert_eq!(rows.row_count, 2);

	daemon.shutdown().await.expect("shutdown daemon");
	fixture.shutdown();
}

#[tokio::test]
async fn toron_daemon_without_sql_section_preserves_worker_only_start() {
	let mut config = toron_config_with_descriptor(descriptor(), true);
	config.sql = None;

	let daemon = ToronDaemon::start(config).await.expect("start daemon");

	assert!(daemon.handle().worker_endpoint.ip().is_loopback());
	assert!(daemon.handle().sql_endpoint.is_none());

	daemon.shutdown().await.expect("shutdown daemon");
}

#[derive(Debug)]
struct SqlResponse {
	batches: Vec<RecordBatch>,
	complete: Complete,
}

fn enable_audit(config: &mut ToronBootstrapConfig, tag: &str) -> PathBuf {
	let dir = fresh_dir(tag);
	let audit_path = dir.join("toron-audit.ndjson");
	config.audit = AuditConfig::Enabled(Box::new(AuditEnabledConfig {
		node_id: "toron-n-1".to_string(),
		node_numeric_id: 1,
		local_path: audit_path.clone(),
		queue_capacity: NonZeroUsize::new(64).expect("nonzero queue capacity"),
		fail_closed: false,
		fail_closed_ack_timeout: Duration::from_millis(500),
		sinks: AuditSinksConfig::default(),
	}));
	audit_path
}

fn enable_fail_closed_unavailable_audit(config: &mut ToronBootstrapConfig, tag: &str) {
	let audit_path_that_is_a_directory = fresh_dir(tag);
	config.audit = AuditConfig::Enabled(Box::new(AuditEnabledConfig {
		node_id: "toron-n-1".to_string(),
		node_numeric_id: 1,
		local_path: audit_path_that_is_a_directory,
		queue_capacity: NonZeroUsize::new(64).expect("nonzero queue capacity"),
		fail_closed: true,
		fail_closed_ack_timeout: Duration::from_millis(50),
		sinks: AuditSinksConfig::default(),
	}));
}

async fn wait_for_audit_records(path: &Path, required: &[&str]) -> String {
	let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
	loop {
		let body = std::fs::read_to_string(path).unwrap_or_default();
		if required.iter().all(|needle| body.contains(needle)) {
			return body;
		}
		if tokio::time::Instant::now() >= deadline {
			panic!(
				"audit file {} missing {required:?}; body:\n{body}",
				path.display()
			);
		}
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
}

async fn execute_sql_until_error(
	endpoint: std::net::SocketAddr,
	statement: &str,
) -> (Vec<RecordBatch>, SqlError) {
	let client = toron::quic::QuicEndpoint::client().expect("sql client");
	let mut stream = client.connect_stream(endpoint).await.expect("connect sql");
	let query_id = QueryId::new();
	write_sql_frame(
		&mut stream,
		&SqlFrame::execute(query_id, statement.to_string(), None),
	)
	.await
	.expect("write sql request");
	stream.finish().await.expect("finish sql request");

	let mut batches = Vec::new();
	loop {
		let frame = read_sql_frame(&mut stream)
			.await
			.expect("read sql response");
		match frame.message {
			SqlMessage::ResultBatch(batch) => {
				let reader = StreamReader::try_new(batch.arrow_ipc_bytes.as_slice(), None)
					.expect("open Arrow IPC stream");
				for batch in reader {
					batches.push(batch.expect("read Arrow IPC batch"));
				}
			}
			SqlMessage::Complete(complete) => {
				panic!("expected SQL error, got complete {complete:?} after {batches:?}")
			}
			SqlMessage::Error(error) => return (batches, error),
			other => panic!("unexpected SQL response frame: {other:?}"),
		}
	}
}

async fn execute_rune_until_failure(endpoint: SocketAddr, frame: SqlFrame) -> RuneFailure {
	let client = QuicEndpoint::client().expect("Rune client");
	let mut stream = client.connect_stream(endpoint).await.expect("connect Rune");
	write_sql_frame(&mut stream, &frame)
		.await
		.expect("write Rune request");
	stream.finish().await.expect("finish Rune request");
	let response = read_sql_frame(&mut stream)
		.await
		.expect("read Rune failure");
	match response.message {
		SqlMessage::RuneError(failure) => failure,
		other => panic!("expected Rune failure, got {other:?}"),
	}
}

async fn execute_sql(
	endpoint: std::net::SocketAddr,
	statement: &str,
) -> Result<SqlResponse, SqlError> {
	let client = toron::quic::QuicEndpoint::client().expect("sql client");
	let mut stream = client.connect_stream(endpoint).await.expect("connect sql");
	let query_id = QueryId::new();
	write_sql_frame(
		&mut stream,
		&SqlFrame::execute(query_id, statement.to_string(), None),
	)
	.await
	.expect("write sql request");
	stream.finish().await.expect("finish sql request");

	let mut batches = Vec::new();
	loop {
		let frame = read_sql_frame(&mut stream)
			.await
			.expect("read sql response");
		match frame.message {
			SqlMessage::ResultBatch(batch) => {
				let reader = StreamReader::try_new(batch.arrow_ipc_bytes.as_slice(), None)
					.expect("open Arrow IPC stream");
				for batch in reader {
					batches.push(batch.expect("read Arrow IPC batch"));
				}
			}
			SqlMessage::Complete(complete) => return Ok(SqlResponse { batches, complete }),
			SqlMessage::Error(error) => return Err(error),
			other => panic!("unexpected SQL response frame: {other:?}"),
		}
	}
}

async fn execute_sql_authenticated(
	endpoint: SocketAddr,
	server_name: &str,
	cert: &Path,
	key: &Path,
	ca: &Path,
	statement: &str,
) -> Result<SqlResponse, SqlError> {
	let client_config = client_config_from_paths(cert, key, ca).expect("client TLS config");
	let client = QuicEndpoint::client_authenticated(client_config).expect("authenticated client");
	let mut stream = client
		.connect_stream_with_server_name(endpoint, server_name)
		.await
		.expect("connect authenticated sql");
	let query_id = QueryId::new();
	write_sql_frame(
		&mut stream,
		&SqlFrame::execute(query_id, statement.to_string(), None),
	)
	.await
	.expect("write sql request");
	stream.finish().await.expect("finish sql request");

	let mut batches = Vec::new();
	loop {
		let frame = read_sql_frame(&mut stream)
			.await
			.expect("read sql response");
		match frame.message {
			SqlMessage::ResultBatch(batch) => {
				let reader = StreamReader::try_new(batch.arrow_ipc_bytes.as_slice(), None)
					.expect("open Arrow IPC stream");
				for batch in reader {
					batches.push(batch.expect("read Arrow IPC batch"));
				}
			}
			SqlMessage::Complete(complete) => return Ok(SqlResponse { batches, complete }),
			SqlMessage::Error(error) => return Err(error),
			other => panic!("unexpected SQL response frame: {other:?}"),
		}
	}
}

async fn execute_rune_authenticated(
	endpoint: SocketAddr,
	server_name: &str,
	cert: &Path,
	key: &Path,
	ca: &Path,
	source: &str,
) -> SqlResponse {
	let client_config = client_config_from_paths(cert, key, ca).expect("client TLS config");
	let client = QuicEndpoint::client_authenticated(client_config).expect("authenticated client");
	let mut stream = client
		.connect_stream_with_server_name(endpoint, server_name)
		.await
		.expect("connect authenticated Rune request");
	let query_id = QueryId::new();
	write_sql_frame(
		&mut stream,
		&SqlFrame::execute_rune(query_id, source.to_owned(), None),
	)
	.await
	.expect("write Rune request");
	stream.finish().await.expect("finish Rune request");

	let mut batches = Vec::new();
	loop {
		let frame = read_sql_frame(&mut stream)
			.await
			.expect("read Rune response");
		match frame.message {
			SqlMessage::ResultBatch(batch) => {
				let reader = StreamReader::try_new(batch.arrow_ipc_bytes.as_slice(), None)
					.expect("open Arrow IPC stream");
				for batch in reader {
					batches.push(batch.expect("read Arrow IPC batch"));
				}
			}
			SqlMessage::Complete(complete) => return SqlResponse { batches, complete },
			SqlMessage::RuneError(error) => panic!("unexpected Rune failure: {error:?}"),
			other => panic!("unexpected Rune response frame: {other:?}"),
		}
	}
}

async fn assert_transport_rejects_without_sql_frame(endpoint: SocketAddr) {
	let client = QuicEndpoint::client().expect("over-cap sql client");
	let stream = client.connect_stream(endpoint).await;
	if let Ok(mut stream) = stream {
		let query_id = QueryId::new();
		let write = write_sql_frame(
			&mut stream,
			&SqlFrame::execute(query_id, "SELECT 1".to_string(), None),
		)
		.await;
		if write.is_ok() {
			let _ = stream.finish().await;
			let read = read_sql_frame(&mut stream).await;
			assert!(
				read.is_err(),
				"connection-cap rejection must close transport without SQL frame"
			);
		}
	}
}

async fn assert_authenticated_transport_rejects_without_sql_frame(
	endpoint: SocketAddr,
	server_name: &str,
	cert: &Path,
	key: &Path,
	ca: &Path,
) {
	let client_config =
		client_config_from_paths(cert, key, ca).expect("over-cap client TLS config");
	let client = QuicEndpoint::client_authenticated(client_config).expect("over-cap sql client");
	let stream = client
		.connect_stream_with_server_name(endpoint, server_name)
		.await;
	if let Ok(mut stream) = stream {
		let query_id = QueryId::new();
		let write = write_sql_frame(
			&mut stream,
			&SqlFrame::execute(query_id, "SELECT 1".to_string(), None),
		)
		.await;
		if write.is_ok() {
			let _ = stream.finish().await;
			let read = read_sql_frame(&mut stream).await;
			assert!(
				read.is_err(),
				"connection-cap rejection must close transport without SQL frame"
			);
		}
	}
}

fn tenant_connection_budget(max_connections: u32) -> TenantBudget {
	let mut budget = TenantBudget::new(ByteCount::new(8), ByteCount::new(32));
	budget.max_concurrent_connections = NonZeroU32::new(max_connections);
	budget
}

fn projected_rows(batches: &[RecordBatch]) -> Vec<(i64, Option<i32>, Option<String>)> {
	let mut rows = Vec::new();
	for batch in batches {
		let ids = batch
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("id column");
		let amounts = batch
			.column(1)
			.as_any()
			.downcast_ref::<Int32Array>()
			.expect("amount column");
		let regions = batch
			.column(2)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("region column");
		for row in 0..batch.num_rows() {
			rows.push((
				ids.value(row),
				(!amounts.is_null(row)).then(|| amounts.value(row)),
				(!regions.is_null(row)).then(|| regions.value(row).to_string()),
			));
		}
	}
	rows
}

struct TestPki {
	ca_cert: PathBuf,
	daemon_cert: PathBuf,
	daemon_key: PathBuf,
	client_cert: PathBuf,
	client_key: PathBuf,
}

impl TestPki {
	fn generate(worker_id: WorkerId, principal: &str) -> Self {
		let dir = fresh_dir("tls-sql");
		let ca = ca_cert();
		let daemon_dns = canonical_daemon_dns_name(&worker_id, DEFAULT_CLUSTER_DOMAIN);
		let daemon = leaf_cert(
			&daemon_dns,
			&[
				ExtendedKeyUsagePurpose::ServerAuth,
				ExtendedKeyUsagePurpose::ClientAuth,
			],
		);
		let client_dns = canonical_client_dns_name(principal, DEFAULT_CLUSTER_DOMAIN);
		let client = leaf_cert(&client_dns, &[ExtendedKeyUsagePurpose::ClientAuth]);
		let pki = Self {
			ca_cert: dir.join("ca.pem"),
			daemon_cert: dir.join("daemon.pem"),
			daemon_key: dir.join("daemon.key"),
			client_cert: dir.join("client.pem"),
			client_key: dir.join("client.key"),
		};
		std::fs::write(&pki.ca_cert, ca.serialize_pem().expect("CA PEM")).expect("write CA");
		write_signed_cert(&pki.daemon_cert, &pki.daemon_key, &daemon, &ca);
		write_signed_cert(&pki.client_cert, &pki.client_key, &client, &ca);
		pki
	}
}

fn toron_config_with_descriptor_tls(
	descriptor: TableSchemaDescriptor,
	worker_id: WorkerId,
	pki: &TestPki,
) -> ToronBootstrapConfig {
	toron_config_with_descriptor_tls_for_tenant(descriptor, ToronTenantId(1), worker_id, pki)
}

fn toron_config_with_descriptor_tls_for_tenant(
	descriptor: TableSchemaDescriptor,
	bootstrap_tenant: ToronTenantId,
	worker_id: WorkerId,
	pki: &TestPki,
) -> ToronBootstrapConfig {
	let mut config = toron_config_with_descriptor_for_tenant(descriptor, bootstrap_tenant, true);
	config.node.worker_id = worker_id;
	config.sql.as_mut().expect("sql config").listener_bind = "0.0.0.0:0".parse().expect("sql bind");
	config.tls = Some(TlsConfig {
		listener_cert: pki.daemon_cert.clone(),
		listener_key: pki.daemon_key.clone(),
		peer_ca: pki.ca_cert.clone(),
		cluster_domain: DEFAULT_CLUSTER_DOMAIN.to_string(),
	});
	config
}

fn toron_config_with_descriptor(
	descriptor: TableSchemaDescriptor,
	include_self_worker: bool,
) -> ToronBootstrapConfig {
	toron_config_with_descriptor_for_tenant(descriptor, ToronTenantId(1), include_self_worker)
}

fn toron_config_with_descriptor_for_tenant(
	descriptor: TableSchemaDescriptor,
	bootstrap_tenant: ToronTenantId,
	include_self_worker: bool,
) -> ToronBootstrapConfig {
	let descriptor_path = write_descriptor_bootstrap_for_tenant(bootstrap_tenant, descriptor);
	ToronBootstrapConfig {
		node: ToronNodeConfig {
			worker_id: WorkerId::new(),
			tenant_id: ToronTenantId(1),
			drain_timeout: Duration::from_secs(1),
		},
		worker: ToronWorkerConfig {
			task_listener_bind: "127.0.0.1:0".parse().expect("task bind"),
			advertised_endpoint: None,
			query_memory_per_task_bytes: None,
			query_spill_bytes_reserved_bytes: None,
			memory_budget_bytes: 64 * 1024 * 1024,
		},
		tenant_budgets: TenantBudgetPolicy::omitted_config(ByteCount::new(64 * 1024 * 1024)),
		admission: None,
		storage: ToronStorageConfig {
			wendelon: ToronWendelonStorageConfig {
				bootstrap_peers: vec![ToronWendelonBootstrapPeer {
					node_id: 1,
					address: "127.0.0.1:7001".to_string(),
				}],
				client_bind: "127.0.0.1:0".parse().expect("client bind"),
				tls: None,
				cluster_domain: wendelon::network::tls::DEFAULT_CLUSTER_DOMAIN.to_string(),
				pool_max_retained: 64,
				pool_max_cap_bytes: 8 * 1024 * 1024,
			},
		},
		catalog: ToronCatalogConfig {
			descriptor_bootstrap_path: Some(descriptor_path),
		},
		sql: Some(SqlConfig {
			transport: SqlTransport::Quic,
			listener_bind: "127.0.0.1:0".parse().expect("sql bind"),
			statement_timeout: Duration::from_secs(3),
			max_concurrent_connections: None,
			max_request_frame_bytes: NonZeroU32::new(
				toron::sql_wire::DEFAULT_SQL_REQUEST_FRAME_BYTES,
			)
			.expect("default SQL request cap"),
			max_statement_bytes: NonZeroU32::new(toron::sql_wire::DEFAULT_SQL_STATEMENT_BYTES)
				.expect("default SQL statement cap"),
		}),
		coordinator: CoordinatorConfig {
			include_self_worker,
			static_workers: Vec::new(),
		},
		tls: None,
		observability: ObservabilityConfig {
			level: "info".to_string(),
			format: TracingFormat::Pretty,
			listeners: None,
			telemetry: None,
		},
		audit: AuditConfig::Disabled,
	}
}

fn ca_cert() -> Certificate {
	let mut params = CertificateParams::new(Vec::new());
	params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
	params.key_usages = vec![
		KeyUsagePurpose::DigitalSignature,
		KeyUsagePurpose::KeyCertSign,
	];
	params
		.distinguished_name
		.push(DnType::CommonName, "toron-sql-listener-test-ca");
	Certificate::from_params(params).expect("CA cert")
}

fn leaf_cert(dns_name: &str, extended_key_usages: &[ExtendedKeyUsagePurpose]) -> Certificate {
	let mut params = CertificateParams::new(vec![dns_name.to_string()]);
	params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
	params.extended_key_usages = extended_key_usages.to_vec();
	params.distinguished_name.push(DnType::CommonName, dns_name);
	Certificate::from_params(params).expect("leaf cert")
}

fn write_signed_cert(cert_path: &Path, key_path: &Path, cert: &Certificate, ca: &Certificate) {
	std::fs::write(
		cert_path,
		cert.serialize_pem_with_signer(ca).expect("signed cert PEM"),
	)
	.expect("write cert");
	std::fs::write(key_path, cert.serialize_private_key_pem()).expect("write key");
}

fn connectable_loopback_addr(addr: SocketAddr) -> SocketAddr {
	SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), addr.port())
}

fn write_descriptor_bootstrap_for_tenant(
	tenant_id: ToronTenantId,
	descriptor: TableSchemaDescriptor,
) -> PathBuf {
	let dir = fresh_dir("descriptor-bootstrap");
	let path = dir.join("descriptors.json");
	let bootstrap = DescriptorBootstrap {
		tables: vec![DescriptorBootstrapEntry {
			tenant_id: tenant_id.0,
			table_id: 1,
			descriptor,
			optimizer_facts: None,
		}],
	};
	std::fs::write(&path, serde_json::to_string_pretty(&bootstrap).unwrap())
		.expect("write descriptor bootstrap");
	path
}

struct LiveWendelonFixture {
	node: WendelonNode,
	client: Arc<Client>,
	data_dir: PathBuf,
}

impl LiveWendelonFixture {
	async fn start(node_id: u32) -> Self {
		let data_dir = fresh_dir(&format!("wendelon-live-{node_id}"));
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
		}
	}

	fn shutdown(self) {
		self.node.handles.abort();
		let _ = std::fs::remove_dir_all(self.data_dir);
	}
}

async fn create_live_tenant(node: &WendelonNode, tenant: WendelonTenantId) {
	node.create_tenant(tenant, u64::MAX, 1)
		.await
		.expect("create live Toron SQL test tenant");
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

fn categorical_descriptor() -> TableSchemaDescriptor {
	let schema = categorical_schema();
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x53; 16]),
		1,
		"bench",
		"test2",
		&schema,
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("categorical descriptor")
}

fn categorical_schema() -> Schema {
	Schema::new(vec![
		Field::new("cat", DataType::Utf8, false),
		Field::new("val", DataType::Float32, false),
	])
}

fn categorical_batch(cat: &str, val: f32) -> RecordBatch {
	RecordBatch::try_new(
		Arc::new(categorical_schema()),
		vec![
			Arc::new(StringArray::from(vec![cat])) as ArrayRef,
			Arc::new(Float32Array::from(vec![val])) as ArrayRef,
		],
	)
	.expect("categorical batch")
}

fn categorical_rows(batches: &[RecordBatch]) -> Vec<(String, f32)> {
	let mut rows = Vec::new();
	for batch in batches {
		let cats = batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("cat column");
		let vals = batch
			.column(1)
			.as_any()
			.downcast_ref::<Float32Array>()
			.expect("val column");
		for row in 0..batch.num_rows() {
			rows.push((cats.value(row).to_string(), vals.value(row)));
		}
	}
	rows
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
		(
			3,
			None,
			Some("north"),
			BASE_TS + 60 * 60 * 1000 + 15 * 60 * 1000,
		),
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

fn fresh_dir(tag: &str) -> PathBuf {
	let dir = std::env::temp_dir().join(format!(
		"toron-sql-listener-{tag}-{}-{}",
		std::process::id(),
		rand::random::<u64>()
	));
	std::fs::create_dir_all(&dir).expect("create temp dir");
	dir
}

/// ADR 577 real-time exhibit. Opt-in: it deliberately runs past the 30-second
/// transport boundary the finding reported, so it costs minutes rather than
/// milliseconds and does not belong in the ordinary suite.
///
/// ```sh
/// cargo test -p toron --test sql_listener_test \\
///   adr0577_live_long_queries_reach_their_statement_deadline -- --ignored --nocapture
/// ```
///
/// The compressed-timescale control in `quic.rs` proves keep-alive beats an
/// idle timeout; only this proves it at the real 30-second boundary with the
/// real policy constants, over the **public `ToronClient`** — the layer that
/// reported the original failure, and the one whose own 120-second deadline
/// must remain the outer bound rather than the transport's.
///
/// Every case asserts its elapsed time. A query that finishes too quickly, or
/// a timeout that fires instantly, fails the exhibit rather than passing it
/// without exercising the boundary it names.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real-time: deliberately exceeds 30s and 60s boundaries"]
async fn adr0577_live_long_queries_reach_their_statement_deadline() {
	// The generated range is a Rune verb (ADR 569) with no SQL spelling, and
	// Rune is the frontend the reported failure used. The projected
	// `seeded_random` is what makes the query slow enough to cross the
	// boundaries under test — ADR 578 removed the reason to avoid
	// `count_all()`, but a bare count would prune to cardinality and finish
	// far too fast to exercise anything here.
	const PAST_30S: &str = r#"pub fn main(ctx) { ctx.range(120000000).project_named([seeded_random(lit_i64(1), col("ordinal")).named("r")]).aggregate([], [count(col("r"))]) }"#;
	const PAST_60S: &str = r#"pub fn main(ctx) { ctx.range(240000000).project_named([seeded_random(lit_i64(1), col("ordinal")).named("r")]).aggregate([], [count(col("r"))]) }"#;

	fn rows_of(result: toron_client::RuneQueryResult) -> u64 {
		match result {
			toron_client::RuneQueryResult::Batches(batches) => batches.row_count,
			other => panic!("an aggregate returns batches, not {other:?}"),
		}
	}

	let fixture = LiveWendelonFixture::start(1577).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	write_rows(&fixture.client, LIVE_TENANT, &descriptor, batch()).await;

	// Case 1: silent well past 30s, with a statement deadline above it. Before
	// this cut the connection died at 30.01s regardless of that setting.
	let mut config = toron_config_with_descriptor(descriptor.clone(), true);
	config.sql.as_mut().expect("SQL config").statement_timeout = Duration::from_secs(180);
	let daemon =
		ToronDaemon::start_with_wendelon_client_for_harness(config, Arc::clone(&fixture.client))
			.await
			.expect("start Toron SQL daemon");
	let endpoint = daemon.handle().sql_endpoint.expect("sql endpoint");
	let client = ToronClient::loopback(endpoint.to_string()).expect("public client");

	let started = std::time::Instant::now();
	let response = client
		.execute_rune_request(toron_client::ExecuteRuneRequest::current(
			PAST_30S.to_owned(),
			None,
		))
		.await
		.expect("a silent query under its statement deadline must succeed");
	let elapsed = started.elapsed();
	assert!(
		elapsed > Duration::from_secs(31),
		"the exhibit must actually cross the old 30s boundary, took {elapsed:?}"
	);
	assert_eq!(rows_of(response), 1);
	println!("ADR 577 case 1: silent success after {elapsed:?}");

	// Case 3: a query that exceeds its own request deadline arrives as a typed
	// statement timeout. The elapsed bounds are the point — an instant refusal
	// would satisfy the type assertion while proving nothing about deadlines,
	// and a 30s one would mean the transport still won.
	let started = std::time::Instant::now();
	let error = client
		.execute_rune_request(toron_client::ExecuteRuneRequest::current(
			PAST_30S.to_owned(),
			Some(5),
		))
		.await
		.expect_err("a 5s request budget must time out");
	let elapsed = started.elapsed();
	let toron_client::ClientError::Rune(failure) = &error else {
		panic!("a deadline overrun must not surface as a transport error: {error:?}");
	};
	assert!(
		matches!(
			failure.kind,
			RuneFailureKind::Execution(SqlErrorCode::StatementTimeout)
		),
		"a deadline overrun is a statement timeout: {failure:?}"
	);
	assert!(
		elapsed >= Duration::from_secs(4),
		"it must run to roughly its 5s deadline, not refuse instantly: {elapsed:?}"
	);
	assert!(
		elapsed < Duration::from_secs(30),
		"and must land on its own deadline, not the old 30s transport one: {elapsed:?}"
	);
	println!("ADR 577 case 3: typed timeout after {elapsed:?}");
	daemon.shutdown().await.expect("shutdown daemon");

	// Case 2: a request-level override beyond the configured 60s default — the
	// case a merely larger static idle timeout could not guarantee, since the
	// caller may raise the deadline above whatever number the transport picked.
	// The override stays under the client's own 120s deadline, so success also
	// shows that deadline remains the outer bound.
	let mut config = toron_config_with_descriptor(descriptor, true);
	config.sql.as_mut().expect("SQL config").statement_timeout = Duration::from_secs(60);
	let daemon =
		ToronDaemon::start_with_wendelon_client_for_harness(config, Arc::clone(&fixture.client))
			.await
			.expect("start Toron SQL daemon");
	let endpoint = daemon.handle().sql_endpoint.expect("sql endpoint");
	let client = ToronClient::loopback(endpoint.to_string()).expect("public client");

	let started = std::time::Instant::now();
	let response = client
		.execute_rune_request(toron_client::ExecuteRuneRequest::current(
			PAST_60S.to_owned(),
			Some(110),
		))
		.await
		.expect("a request override above the configured default must survive");
	let elapsed = started.elapsed();
	assert!(
		elapsed > Duration::from_secs(61),
		"the override case must actually cross the 60s default, took {elapsed:?}"
	);
	assert!(
		elapsed < Duration::from_secs(120),
		"and must finish inside the client's own deadline: {elapsed:?}"
	);
	assert_eq!(rows_of(response), 1);
	println!("ADR 577 case 2: override success after {elapsed:?}");

	daemon.shutdown().await.expect("shutdown daemon");
	fixture.shutdown();
}
