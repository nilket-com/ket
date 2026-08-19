#![allow(unused_imports)]

use super::super::support::*;
use super::super::{
	core_invariants::*, network_tls::*, observability_audit::*, observability_telemetry::*,
	predicate_pushdown::*, ring_placement::*, sql_planner_surface::*, storage_wendelon_boundary::*,
};
use super::{
	cancellation::*, descriptor_sync::*, resource_governance::*, scheduler_dispatch::*,
	sql_ingress::*, worker_lifecycle::*,
};

#[test]
pub(crate) fn adr049_sql_tenant_source_is_authenticated_peer_identity() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon source");
	let tls = std::fs::read_to_string(manifest_dir().join("src/network/tls.rs")).expect("read TLS");

	assert!(daemon.contains("SqlTenantSource::AuthenticatedPeer"));
	assert!(daemon.contains("tenant_source.resolve(&connection)"));
	assert!(tls.contains("pub fn accepted_sql_tenant("));
	assert!(tls.contains(".peer_identity()"));
	assert!(tls.contains("downcast::<Vec<CertificateDer<'static>>>()"));
	assert!(tls.contains("extract_dns_sans(leaf)"));
}

#[test]
pub(crate) fn adr049_loopback_sql_retains_config_tenant_fallback() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon source");

	assert!(daemon.contains("SqlTenantSource::LoopbackConfig(config.node.tenant_id)"));
	assert!(daemon.contains("QuicEndpoint::bind_loopback(sql_config.listener_bind)"));
}

#[test]
pub(crate) fn adr049_remote_sql_never_falls_back_to_config_tenant() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon source");
	let resolve = brace_blocks_after_marker(&daemon, "fn resolve(&self, connection")
		.into_iter()
		.next()
		.expect("SqlTenantSource::resolve")
		.1;

	assert!(daemon.contains("SqlTenantSource::AuthenticatedPeer"));
	assert!(daemon.contains("policy.cluster_domain().to_string()"));
	assert!(resolve.contains("accepted_sql_tenant(connection, cluster_domain)"));
	assert!(!resolve.contains("config.node.tenant_id"));
}

#[test]
pub(crate) fn adr049_sql_client_san_requires_canonical_numeric_tenant() {
	let admitted = admit_san(
		"client-7.cluster.local",
		ListenerRole::Sql,
		DEFAULT_CLUSTER_DOMAIN,
	)
	.expect("admit tenant 7 client");
	assert_eq!(
		admitted,
		PeerKind::Client {
			principal: "7".to_string(),
			tenant_id: TenantId(7),
		}
	);

	for san in [
		"client-0.cluster.local",
		"client-007.cluster.local",
		"client-+7.cluster.local",
		"client-0x7.cluster.local",
	] {
		assert!(
			admit_san(san, ListenerRole::Sql, DEFAULT_CLUSTER_DOMAIN).is_err(),
			"{san} should not be admitted"
		);
	}
}

#[test]
pub(crate) fn adr049_sql_client_san_rejects_non_numeric_principal_for_tenant() {
	for san in [
		"client-analytics.cluster.local",
		"client-tenant7.cluster.local",
		"client-seven.cluster.local",
	] {
		assert!(
			matches!(
				admit_san(san, ListenerRole::Sql, DEFAULT_CLUSTER_DOMAIN),
				Err(toron::network::tls::TlsError::SanShapeRejected)
			),
			"{san} should reject as a tenant route"
		);
	}
}

#[test]
pub(crate) fn adr049_sql_listener_still_rejects_sibling_daemon_identity() {
	let worker_id = WorkerId::from_u128(0x550e_8400_e29b_41d4_a716_4466_5544_0000);
	let daemon_san = canonical_daemon_dns_name(&worker_id, DEFAULT_CLUSTER_DOMAIN);

	assert!(admit_san(&daemon_san, ListenerRole::Sql, DEFAULT_CLUSTER_DOMAIN).is_err());
	assert!(
		admit_san(
			&daemon_san,
			ListenerRole::WorkerTask,
			DEFAULT_CLUSTER_DOMAIN
		)
		.is_ok()
	);
}

#[test]
pub(crate) fn adr049_quic_peer_identity_extracted_before_sql_frame_read() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon source");
	let loop_body = brace_blocks_after_marker(&daemon, "async fn run_sql_accept_loop")
		.into_iter()
		.next()
		.expect("run_sql_accept_loop")
		.1;

	let resolve_pos = loop_body
		.find("tenant_source.resolve(&connection)")
		.expect("tenant extraction");
	let stream_pos = loop_body
		.find("QuicEndpoint::accept_stream_on(&connection)")
		.expect("stream accept");
	assert!(resolve_pos < stream_pos);
	assert!(!loop_body[..stream_pos].contains("read_sql_frame"));
}

#[test]
pub(crate) fn adr049_peer_kind_client_carries_tenant_id() {
	let tls = std::fs::read_to_string(manifest_dir().join("src/network/tls.rs")).expect("read TLS");
	let peer_kind = brace_blocks_after_marker(&tls, "pub enum PeerKind")
		.into_iter()
		.next()
		.expect("PeerKind")
		.1;

	assert!(peer_kind.contains("Client {"));
	assert!(peer_kind.contains("principal: String"));
	assert!(peer_kind.contains("tenant_id: TenantId"));
}

#[test]
pub(crate) fn adr049_coordinator_scopes_catalog_before_binding() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");
	let body = brace_blocks_after_marker(&coordinator, "async fn execute_query_with_admission")
		.into_iter()
		.next()
		.expect("execute_query_with_admission")
		.1;

	let scoped_pos = body
		.find("Arc::clone(&self.catalog).scoped(context.tenant_id)")
		.expect("scoped catalog");
	let bind_pos = body.find("bind_with_statement_timestamp(").expect("bind");
	assert!(scoped_pos < bind_pos);
	assert!(!body.contains("bind(plan, self.catalog.as_ref()"));
}

#[test]
pub(crate) fn adr049_descriptor_registry_keys_refs_and_descriptors_by_tenant() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");

	for required in [
		"by_ref: HashMap<(TenantId, TableRef), TableId>",
		"by_id: HashMap<(TenantId, TableId), TableBinding>",
		"descriptors: HashMap<(TenantId, TableId), wendelon_client::TableSchemaDescriptor>",
		"finalized_drops: HashMap<(TenantId, TableId), DropOperationId>",
		"register_descriptor_for_tenant",
	] {
		assert!(
			catalog.contains(required),
			"missing tenant-keyed token {required}"
		);
	}
}

#[test]
pub(crate) fn adr049_create_and_drop_table_use_scoped_tenant() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");

	assert!(catalog.contains("fn scoped(self: Arc<Self>, tenant_id: TenantId)"));
	assert!(catalog.contains("TableSchemaUpdateRequest::create("));
	assert!(catalog.contains("wendelon_tenant_id(self.tenant_id)"));
	assert!(catalog.contains("tenant_id: wendelon_tenant_id(self.tenant_id)"));
	assert!(catalog.contains(".consume_finalized_drop("));
	assert!(catalog.contains("self.tenant_id,"));
	assert!(catalog.contains("remove_table_for_tenant(self.tenant_id"));
}

#[test]
pub(crate) fn adr049_storage_descriptor_lookup_uses_request_tenant() {
	let storage =
		std::fs::read_to_string(manifest_dir().join("src/storage.rs")).expect("read storage");

	assert_eq!(
		storage
			.matches(".descriptor(request.tenant_id, request.table_id)")
			.count(),
		3
	);
	assert!(storage.contains("mark_finalized_drop(request.tenant_id, request.table_id"));
}

#[test]
pub(crate) fn adr049_task_context_and_storage_requests_preserve_tenant() {
	let context = std::fs::read_to_string(manifest_dir().join("src/distributed/context.rs"))
		.expect("read task context");
	let exec = std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec");
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");

	assert!(context.contains("pub tenant_id: TenantId"));
	assert!(context.contains("context.tenant_id"));
	for required in [
		"tenant_id: self.context.tenant_id",
		"tenant_id: context.tenant_id",
	] {
		assert!(
			exec.contains(required) || coordinator.contains(required),
			"missing tenant propagation token {required}"
		);
	}
}

#[test]
pub(crate) fn adr049_no_sql_wire_tenant_claim_or_message_variant() {
	let wire = std::fs::read_to_string(manifest_dir().join("../toron-client/src/protocol.rs"))
		.expect("read SQL wire");
	let cli = std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI");

	assert!(!wire.contains("tenant_id"));
	assert!(!wire.contains("TenantRoute"));
	assert!(!wire.contains("ClientIdentity"));
	assert!(!cli.contains("--tenant"));
	for forbidden in [
		"TORON_SQL_TOKEN",
		"Authorization",
		"Bearer",
		"JWT",
		"OIDC",
		"SPIFFE",
	] {
		assert!(!wire.contains(forbidden), "SQL wire contains {forbidden}");
	}
}

#[test]
pub(crate) fn adr049_no_new_sql_error_codes() {
	let wire = std::fs::read_to_string(manifest_dir().join("../toron-client/src/protocol.rs"))
		.expect("read SQL wire");
	let error_codes = brace_blocks_after_marker(&wire, "pub enum SqlErrorCode")
		.into_iter()
		.next()
		.expect("SqlErrorCode")
		.1;

	for forbidden in [
		"Tls",
		"Auth",
		"Certificate",
		"TenantRoute",
		"ClientIdentity",
	] {
		assert!(
			!error_codes.contains(forbidden),
			"SqlErrorCode gained ADR 049-forbidden token {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr049_no_per_tenant_metric_labels() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability");

	assert_bounded_resource_tenant_labels_only(&observability);
	assert!(!observability.contains("tenant=\""));
	assert!(observability.contains("reason=\\\"{}\\\""));
}

#[test]
pub(crate) fn adr049_no_wendelon_production_changes() {
	let wendelon_source = source_lines(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.map(|(_, _, line)| line)
		.collect::<Vec<_>>()
		.join("\n");

	for forbidden in [
		"accepted_sql_tenant",
		"SqlTenantSource",
		"client-<tenant_id>",
		"TenantRoute",
	] {
		assert!(
			!wendelon_source.contains(forbidden),
			"Wendelon source contains Toron ADR 049 token {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr049_prior_adr04x_guards_continue_to_pass() {
	let architecture_test = architecture_corpus_source();

	for prefix in [
		"adr040_", "adr041_", "adr042_", "adr043_", "adr044_", "adr045_", "adr046_", "adr047_",
		"adr048_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
}

#[test]
pub(crate) fn adr049_nilket_root_remains_runtime_free() {
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");

	for forbidden in ["tokio", "quinn", "rustls", "clap", "tracing"] {
		assert!(
			!nilket_manifest.contains(forbidden),
			"nilket manifest gained direct runtime dependency {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr049_fixture_worker_source_unchanged() {
	let fixture = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker source");

	assert!(!fixture.contains("tenant_id"));
	assert!(!fixture.contains("TlsConfig"));
	assert!(!fixture.contains("daemon::"));
}

#[test]
pub(crate) fn adr049_new_types_are_brand_clean_and_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"ToronTenantRoute",
		"ToronClientIdentity",
		"ToronAcceptedSqlConnection",
		"TenantRouteV1",
		"ClientIdentityV1",
		"AcceptedSqlConnectionV1",
	]);
}
