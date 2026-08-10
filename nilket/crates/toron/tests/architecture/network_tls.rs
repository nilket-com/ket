#![allow(unused_imports)]

use super::support::*;
use super::{
	core_invariants::*, distributed_execution::*, observability_audit::*,
	observability_telemetry::*, predicate_pushdown::*, ring_placement::*, sql_planner_surface::*,
	storage_wendelon_boundary::*,
};

#[test]
pub(crate) fn adr044_toron_tls_config_surface_exists_and_is_optional() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");

	for required in [
		"pub tls: Option<TlsConfig>",
		"pub struct TlsConfig",
		"# [tls]",
		"DEFAULT_CLUSTER_DOMAIN",
		"normalize_tls_config",
		"!task_listener_bind.ip().is_loopback() && tls.is_none()",
		"normalize_sql_config(sql, tls.is_some())",
		"!endpoint.ip().is_loopback() && tls.is_none()",
	] {
		assert!(
			config.contains(required),
			"missing ADR 044 TLS config token {required}"
		);
	}
}

#[test]
pub(crate) fn adr044_tls_policy_uses_exact_san_admission_per_listener() {
	let tls = std::fs::read_to_string(manifest_dir().join("src/network/tls.rs"))
		.expect("read TLS policy");

	for required in [
		"pub enum ListenerRole",
		"WorkerTask",
		"Sql",
		"pub enum PeerKind",
		"pub struct ListenerPolicy",
		"pub enum TlsError",
		"AUTH_QUIC_ALPN",
		"canonical_daemon_dns_name",
		"canonical_client_dns_name",
		"ExactServerCertVerifier",
		"SanClientCertVerifier",
		"admit_san",
		"(\"toron\", ListenerRole::WorkerTask)",
		"(\"toron\", ListenerRole::Sql) => Err(TlsError::SanRoleRejected)",
		"(\"client\", ListenerRole::Sql)",
		"(\"client\", ListenerRole::WorkerTask) => Err(TlsError::SanRoleRejected)",
		"Uuid::parse_str(id_token)",
	] {
		assert!(
			tls.contains(required),
			"missing ADR 044 TLS policy token {required}"
		);
	}
}

#[test]
pub(crate) fn adr044_daemon_selects_authenticated_quic_only_for_non_loopback_tls_listeners() {
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");
	let transport = std::fs::read_to_string(manifest_dir().join("src/server/transport.rs"))
		.expect("read Toron transport source");

	assert!(daemon.contains("ListenerPolicy::load"));
	assert!(daemon.contains("QuicEndpoint::bind_authenticated"));
	assert!(daemon.contains("server_config_for(ListenerRole::WorkerTask)"));
	assert!(daemon.contains("server_config_for(ListenerRole::Sql)"));
	assert!(daemon.contains("QuicEndpoint::bind_loopback"));
	assert!(daemon.contains("QuicTransport::authenticated"));
	assert!(transport.contains("connect_stream_with_server_name(remote, &server_name)"));
	assert!(transport.contains("canonical_daemon_dns_name(&worker.worker_id, cluster_domain)"));
}

#[test]
pub(crate) fn adr044_sql_cli_remote_tls_flags_do_not_add_nilket_runtime_dependencies() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");

	for required in [
		"--server-name",
		"--cert",
		"--key",
		"--ca",
		"TORON_SQL_SERVER_NAME",
		"TORON_SQL_CLIENT_CERT",
		"TORON_SQL_CLIENT_KEY",
		"TORON_SQL_CA",
		"toron_client::ToronClient::authenticated",
		"toron_client::ToronClientTls",
		"failed to initialize Toron SQL client",
	] {
		assert!(
			cli.contains(required),
			"missing ADR 044 SQL CLI token {required}"
		);
	}
	for forbidden in ["tokio", "quinn", "rustls", "arrow-cast"] {
		assert!(
			!nilket_manifest.contains(forbidden),
			"nilket manifest gained direct runtime dependency {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr044_does_not_change_sql_wire_or_add_token_auth_models() {
	let wire = std::fs::read_to_string(manifest_dir().join("../toron-client/src/protocol.rs"))
		.expect("read SQL wire");

	for forbidden in ["Tls", "Auth", "Principal", "Certificate", "client_cert"] {
		assert!(
			!wire.contains(forbidden),
			"SQL wire gained ADR 044-forbidden token {forbidden}"
		);
	}
	for path in [
		"src/cli.rs",
		"src/daemon.rs",
		"src/quic.rs",
		"src/network/tls.rs",
		"src/server/transport.rs",
	] {
		let source = std::fs::read_to_string(manifest_dir().join(path)).expect("read source");
		for forbidden in [
			"TORON_SQL_TOKEN",
			"Authorization",
			"Bearer",
			"TOFU",
			"fingerprint",
			"SPIFFE",
			"OIDC",
		] {
			assert!(
				!source.contains(forbidden),
				"{path} gained ADR 044-forbidden token auth model {forbidden}"
			);
		}
	}
}

#[test]
pub(crate) fn adr044_no_cert_reload_or_revocation_scope_creep() {
	assert_toron_sources_do_not_contain(&[
		"reload_cert",
		"watch_cert",
		"refresh_cert",
		"CRL",
		"OCSP",
	]);
}

#[test]
pub(crate) fn adr044_san_admission_is_exact_shape_per_listener() {
	let worker_id = WorkerId::from_u128(0x550e_8400_e29b_41d4_a716_4466_5544_0000);
	let daemon_san = canonical_daemon_dns_name(&worker_id, DEFAULT_CLUSTER_DOMAIN);
	let admitted = admit_san(
		&daemon_san,
		ListenerRole::WorkerTask,
		DEFAULT_CLUSTER_DOMAIN,
	)
	.expect("admit daemon SAN");
	assert_eq!(admitted, PeerKind::SiblingDaemon { worker_id });

	let client_san = canonical_client_dns_name("7", DEFAULT_CLUSTER_DOMAIN);
	let admitted = admit_san(&client_san, ListenerRole::Sql, DEFAULT_CLUSTER_DOMAIN)
		.expect("admit client SAN");
	assert_eq!(
		admitted,
		PeerKind::Client {
			principal: "7".to_string(),
			tenant_id: TenantId(7),
		}
	);

	for (san, role) in [
		(
			"toron-550e8400-e29b-41d4-a716-446655440000.evil.cluster.local",
			ListenerRole::WorkerTask,
		),
		("*.cluster.local", ListenerRole::WorkerTask),
		("client-analytics.cluster.local", ListenerRole::Sql),
		("client-007.cluster.local", ListenerRole::Sql),
		("client-0.cluster.local", ListenerRole::Sql),
		("client-analytics_dev.cluster.local", ListenerRole::Sql),
		("toron-not-a-uuid.cluster.local", ListenerRole::WorkerTask),
		("foo-analytics.cluster.local", ListenerRole::Sql),
	] {
		assert!(
			admit_san(san, role, DEFAULT_CLUSTER_DOMAIN).is_err(),
			"{san} should not be admitted for {role:?}"
		);
	}
	assert!(
		admit_san(
			&client_san,
			ListenerRole::WorkerTask,
			DEFAULT_CLUSTER_DOMAIN
		)
		.is_err()
	);
	assert!(admit_san(&daemon_san, ListenerRole::Sql, DEFAULT_CLUSTER_DOMAIN).is_err());
}

#[test]
pub(crate) fn adr044_loopback_diagnostic_text_is_brand_clean_and_stable() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");

	for required in [
		"worker listener requires authenticated transport for non-loopback bind address",
		"sql listener requires authenticated transport for non-loopback bind address",
		"coordinator static workers require authenticated transport for non-loopback endpoints",
	] {
		assert!(config.contains(required), "missing diagnostic {required}");
	}
	for forbidden in [
		"Toron TLS",
		"toron TLS",
		"ToronTls",
		"toron-tls",
		"remote SQL auth",
	] {
		assert!(
			!config.contains(forbidden),
			"config diagnostic surface contains project-branded TLS token {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr044_tenant_id_remains_server_stamped() {
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");
	let wire = std::fs::read_to_string(manifest_dir().join("../toron-client/src/protocol.rs"))
		.expect("read SQL wire");

	assert!(daemon.contains("SqlTenantSource::LoopbackConfig(config.node.tenant_id)"));
	assert!(daemon.contains("accepted_sql_tenant(connection, cluster_domain)"));
	for source in [daemon.as_str(), wire.as_str()] {
		for forbidden in ["client_tenant", "tenant_claim", "tenant_id: Option"] {
			assert!(
				!source.contains(forbidden),
				"ADR 044 must not add client-selected tenant token {forbidden}"
			);
		}
	}
}

#[test]
pub(crate) fn adr044_ready_lines_are_unchanged() {
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");

	assert_eq!(
		daemon
			.matches("println!(\"READY {}\", daemon.handle().worker_endpoint);")
			.count(),
		1
	);
	assert_eq!(
		daemon
			.matches("println!(\"SQL_READY {sql_endpoint}\");")
			.count(),
		1
	);
}

#[test]
pub(crate) fn adr045_observability_section_is_optional_for_backward_compat() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");

	for required in [
		"pub struct ObservabilityConfig",
		"pub struct ObservabilityListeners",
		"pub listeners: Option<ObservabilityListeners>",
		"listeners: Option<RawObservabilityListeners>",
		"listeners: None",
		"[observability]",
		"# [observability.listeners]",
		"normalize_observability_config(raw.observability, tls.is_some(), base_dir)",
	] {
		assert!(
			config.contains(required),
			"missing ADR 045 optional config token {required}"
		);
	}
}

#[test]
pub(crate) fn adr045_health_listener_is_loopback_only_always() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");

	assert!(config.contains("OBSERVABILITY_HEALTH_LOOPBACK_DIAGNOSTIC"));
	assert!(config.contains("!health.ip().is_loopback()"));
	assert!(config.contains("observability health listener must bind a loopback address"));
}

#[test]
pub(crate) fn adr045_ready_and_metrics_require_tls_when_configured() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");

	assert!(config.contains("OBSERVABILITY_REQUIRES_TLS_DIAGNOSTIC"));
	assert!(config.contains("if !tls_present"));
	assert!(daemon.contains("config.observability.listeners.is_some() && tls_policy.is_none()"));
	assert!(daemon.contains("rustls_server_config_for(ListenerRole::Observability)"));
	assert!(daemon.contains("TlsAcceptor::from"));
}

#[test]
pub(crate) fn adr045_monitor_san_prefix_admission_is_per_listener() {
	let monitor_san = canonical_monitor_dns_name("prometheus", DEFAULT_CLUSTER_DOMAIN);
	let admitted = admit_san(
		&monitor_san,
		ListenerRole::Observability,
		DEFAULT_CLUSTER_DOMAIN,
	)
	.expect("admit monitor SAN");
	assert_eq!(
		admitted,
		PeerKind::Monitor {
			principal: "prometheus".to_string(),
		}
	);

	assert!(
		admit_san(
			&monitor_san,
			ListenerRole::WorkerTask,
			DEFAULT_CLUSTER_DOMAIN
		)
		.is_err()
	);
	assert!(admit_san(&monitor_san, ListenerRole::Sql, DEFAULT_CLUSTER_DOMAIN).is_err());

	let worker_id = WorkerId::from_u128(0x550e_8400_e29b_41d4_a716_4466_5544_0000);
	let daemon_san = canonical_daemon_dns_name(&worker_id, DEFAULT_CLUSTER_DOMAIN);
	let client_san = canonical_client_dns_name("analytics", DEFAULT_CLUSTER_DOMAIN);
	for san in [
		daemon_san.as_str(),
		client_san.as_str(),
		"monitor_ops.cluster.local",
	] {
		assert!(
			admit_san(san, ListenerRole::Observability, DEFAULT_CLUSTER_DOMAIN).is_err(),
			"observability listener should reject {san}"
		);
	}
}

#[test]
pub(crate) fn adr045_observability_listeners_use_hand_rolled_http_no_hyper_no_axum() {
	let manifest_dir = manifest_dir();
	for path in [
		"src/network/observability.rs",
		"src/network/observability_http.rs",
	] {
		let source = std::fs::read_to_string(manifest_dir.join(path)).expect("read source");
		assert!(source.contains("read_request"));
		assert!(source.contains("write_response"));
		for forbidden in [
			"hyper",
			"axum",
			"actix",
			"tower",
			"Request<",
			"Response<",
			"keep-alive",
			"http2",
			"quinn",
		] {
			assert!(
				!source.contains(forbidden),
				"{path} contains ADR 045-forbidden HTTP framework/protocol token {forbidden}"
			);
		}
	}
}

#[test]
pub(crate) fn adr045_metrics_uses_prometheus_text_format_no_histograms() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability source");
	let adr045_observability = observability
		// ADR 543's branded shuffle lifecycle family is a later aggregate-only
		// extension, not part of ADR 045's initial stable metric set.
		.split("fn render_shuffle_lifecycle_metrics")
		.next()
		.expect("pre-audit observability source");

	assert!(adr045_observability.contains("text/plain; version=0.0.4; charset=utf-8"));
	assert!(adr045_observability.contains("# TYPE daemon_ready gauge"));
	assert!(adr045_observability.contains("listener_accepts_total"));
	assert!(adr045_observability.contains("metrics_scrapes_total"));
	for forbidden in ["_bucket", "_sum", "_count", "histogram", "Histogram"] {
		assert!(
			!adr045_observability.contains(forbidden),
			"ADR 045 metrics must not introduce histogram token {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr045_no_revocation_no_audit_no_traces_in_observability() {
	for path in [
		"src/network/observability.rs",
		"src/network/observability_http.rs",
		"src/network/tls.rs",
	] {
		let source = std::fs::read_to_string(manifest_dir().join(path)).expect("read source");
		for forbidden in [
			"RevocationSnapshot",
			"CRL",
			"OCSP",
			"AuditSink",
			"audit_event",
			"opentelemetry",
			"OTLP",
			"ActorPermission",
		] {
			if path == "src/network/observability.rs" && forbidden == "audit_event" {
				continue;
			}
			assert!(
				!source.contains(forbidden),
				"{path} contains ADR 045-forbidden observability expansion token {forbidden}"
			);
		}
	}
}

#[test]
pub(crate) fn adr045_observability_ready_line_is_appended_not_replaced() {
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");

	let ready = daemon
		.find("println!(\"READY {}\", daemon.handle().worker_endpoint);")
		.expect("READY line");
	let sql_ready = daemon
		.find("println!(\"SQL_READY {sql_endpoint}\");")
		.expect("SQL_READY line");
	let observability_ready = daemon
		.find("\"OBSERVABILITY_READY health={} ready={} metrics={}\"")
		.expect("OBSERVABILITY_READY line");
	assert!(ready < sql_ready);
	assert!(sql_ready < observability_ready);
}

#[test]
pub(crate) fn adr045_one_tls_material_set_per_daemon_still_holds() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");
	let tls = std::fs::read_to_string(manifest_dir().join("src/network/tls.rs"))
		.expect("read TLS source");

	assert_eq!(config.matches("tls: Option<RawTlsConfig>").count(), 1);
	assert_eq!(config.matches("pub tls: Option<TlsConfig>").count(), 1);
	assert_eq!(daemon.matches("ListenerPolicy::load").count(), 1);
	assert!(tls.contains("server_config_worker_task"));
	assert!(tls.contains("server_config_sql"));
	assert!(tls.contains("server_config_observability"));
	for forbidden in [
		"[observability.tls]",
		"observability_tls",
		"ready_tls",
		"metrics_tls",
	] {
		assert!(
			!config.contains(forbidden) && !daemon.contains(forbidden),
			"ADR 045 must reuse daemon TLS material set, found {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr045_initial_metric_set_is_functional_brand_clean_and_stable() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability source");
	let adr045_observability = observability
		// ADR 543's branded shuffle lifecycle family is a later aggregate-only
		// extension, not part of ADR 045's initial stable metric set.
		.split("fn render_shuffle_lifecycle_metrics")
		.next()
		.expect("pre-audit observability source");
	let adr045_stable_observability = adr045_observability
		.replace(
			"toron_downstream_storage_cancellations_total",
			"downstream_storage_cancellations_total",
		)
		.replace("toron_otlp_spans_dropped_total", "otlp_spans_dropped_total");

	for required in [
		"daemon_ready",
		"daemon_uptime_seconds",
		"listener_up",
		"listener_accepts_total",
		"listener_rejects_total",
		"metrics_scrapes_total",
		"descriptors_loaded",
		"static_workers_configured",
		"listener=\\\"{}\\\"",
		"reason=\\\"{}\\\"",
	] {
		assert!(
			observability.contains(required),
			"missing ADR 045 metric token {required}"
		);
	}
	for forbidden in [
		"toron_",
		"worker_id",
		"query_id",
		"task_id",
		"table_id",
		"sql_text",
		"certificate",
	] {
		assert!(
			!adr045_stable_observability.contains(forbidden),
			"ADR 045 metrics include forbidden high-cardinality/branded token {forbidden}"
		);
	}
	assert_bounded_resource_tenant_labels_only(&observability);
}

#[test]
pub(crate) fn adr045_ready_snapshot_excludes_high_cardinality_identity_fields() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability source");

	for required in [
		"pub struct ReadinessSnapshot",
		"pub ready: bool",
		"pub worker_listener_bound: bool",
		"pub sql_listener_bound: bool",
		"pub observability_listeners_bound: bool",
		"pub descriptors_loaded: u64",
		"pub static_workers_configured: u32",
		"pub uptime_seconds: u64",
		"pub degraded_reason: Option<String>",
	] {
		assert!(
			observability.contains(required),
			"missing ADR 045 readiness field {required}"
		);
	}
	let readiness_struct =
		brace_blocks_after_marker(&observability, "pub struct ReadinessSnapshot")
			.into_iter()
			.next()
			.expect("ReadinessSnapshot")
			.1;
	let readiness_json = brace_blocks_after_marker(&observability, "pub fn to_json")
		.into_iter()
		.next()
		.expect("ReadinessSnapshot::to_json")
		.1;
	let readiness_surface = format!("{readiness_struct}\n{readiness_json}");
	for forbidden in [
		"worker_id",
		"tenant_id",
		"query_id",
		"task_id",
		"table_id",
		"sql_text",
		"certificate",
		"endpoint",
		"uuid",
	] {
		assert!(
			!readiness_surface.contains(forbidden),
			"ADR 045 readiness surface includes forbidden token {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr045_observability_http_has_per_request_read_timeout() {
	let http = std::fs::read_to_string(manifest_dir().join("src/network/observability_http.rs"))
		.expect("read observability HTTP source");
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability source");

	assert!(http.contains("pub const READ_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);"));
	assert!(http.contains("MAX_REQUEST_HEADER_BYTES"));
	assert!(observability.contains("timeout(READ_REQUEST_TIMEOUT, read_request(stream))"));
}

#[test]
pub(crate) fn adr045_new_observability_types_are_brand_clean_and_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"ToronObservabilityConfig",
		"ToronTracingFormat",
		"RawToronObservabilityConfig",
		"ToronReadinessSnapshot",
		"ToronMetricsRegistry",
		"ToronObservabilityListeners",
		"ObservabilityConfigV1",
		"ReadinessSnapshotV1",
		"MetricsRegistryV1",
	]);
}

#[test]
pub(crate) fn adr046_toron_inspect_is_not_reserved() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");

	assert!(cli.contains("[verb, rest @ ..] if verb == \"inspect\" => run_inspect(rest)"));
	assert!(cli.contains("fn run_inspect(args: &[String]) -> ExitCode"));
	assert!(!cli.contains("reserved(\"inspect\")"));
	assert!(!cli.contains("Toron inspect is reserved in ADR 040"));
}

#[test]
pub(crate) fn adr046_toron_inspect_is_client_only() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");
	let inspect_body = brace_blocks_after_marker(&cli, "async fn fetch_ready_with_tcp_connect")
		.into_iter()
		.next()
		.expect("fetch_ready_with_tcp_connect body")
		.1;

	assert!(cli.contains("TcpStream::connect(options.endpoint)"));
	for required in ["TlsConnector::from", "read_response"] {
		assert!(
			inspect_body.contains(required),
			"inspect client missing {required}"
		);
	}
	for forbidden in [
		"ToronDaemon",
		"ToronWorker",
		"DistributedQueryCoordinator",
		"WendelonCatalog",
		"DescriptorRegistry",
		"MetricsRegistry",
		"StorageScan",
	] {
		assert!(
			!inspect_body.contains(forbidden),
			"inspect client constructs daemon machinery token {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr046_toron_inspect_dials_ready_endpoint() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");
	let inspect_body = brace_blocks_after_marker(&cli, "async fn fetch_ready_with_tcp_connect")
		.into_iter()
		.next()
		.expect("fetch_ready_with_tcp_connect body")
		.1;

	assert!(inspect_body.contains("crate::network::observability::READY_PATH"));
	assert!(inspect_body.contains("write_request("));
	assert!(inspect_body.contains("&options.endpoint.to_string()"));
	assert!(!inspect_body.contains("&tls.server_name"));
	for forbidden in [
		"HEALTH_PATH",
		"METRICS_PATH",
		"\"/inspect\"",
		"\"/workers\"",
		"\"/config\"",
	] {
		assert!(
			!inspect_body.contains(forbidden),
			"inspect client must not dial {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr046_toron_inspect_endpoint_precedence_flag_env_default() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");
	let parse_body = brace_blocks_after_marker(&cli, "fn parse_inspect_args")
		.into_iter()
		.next()
		.expect("parse_inspect_args body")
		.1;

	assert!(cli.contains("const DEFAULT_INSPECT_ENDPOINT: &str = \"127.0.0.1:7781\";"));
	assert!(parse_body.contains("\"--endpoint\""));
	assert!(parse_body.contains("std::env::var(\"TORON_INSPECT_ENDPOINT\")"));
	assert!(parse_body.contains("DEFAULT_INSPECT_ENDPOINT"));
	assert!(parse_body.contains("invalid TORON_INSPECT_ENDPOINT"));
}

#[test]
pub(crate) fn adr046_toron_inspect_requires_all_four_tls_flags_together() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");
	let parse_body = brace_blocks_after_marker(&cli, "fn parse_inspect_args")
		.into_iter()
		.next()
		.expect("parse_inspect_args body")
		.1;

	for required in [
		"TORON_INSPECT_SERVER_NAME",
		"TORON_INSPECT_CLIENT_CERT",
		"TORON_INSPECT_CLIENT_KEY",
		"TORON_INSPECT_CA",
		"toron inspect requires --server-name, --cert, --key, and --ca",
	] {
		assert!(parse_body.contains(required), "missing {required}");
	}
	assert!(!parse_body.contains("endpoint.ip().is_loopback()"));
	let tls = std::fs::read_to_string(manifest_dir().join("src/network/tls.rs"))
		.expect("read TLS source");

	assert!(tls.contains("canonical_monitor_dns_name"));
	assert!(tls.contains("\"monitor\""));
	assert!(!tls.contains("inspect-"));
	assert!(!cli.contains("inspect-"));
}

#[test]
pub(crate) fn adr046_toron_inspect_renders_text_by_default_json_with_flag() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");
	let render_body = brace_blocks_after_marker(&cli, "fn render_readiness_text")
		.into_iter()
		.next()
		.expect("render_readiness_text body")
		.1;

	for required in [
		"ready:",
		"worker_listener_bound:",
		"sql_listener_bound:",
		"observability_listeners_bound:",
		"descriptors_loaded:",
		"static_workers_configured:",
		"uptime_seconds:",
		"degraded_reason:",
		"serde_json::to_string",
		"null",
	] {
		assert!(render_body.contains(required), "missing {required}");
	}
	for forbidden in [
		"endpoint",
		"worker_id",
		"tenant_id",
		"query_id",
		"task_id",
		"table",
		"sql_text",
		"certificate",
		"uuid",
	] {
		assert!(
			!render_body.contains(forbidden),
			"inspect text renderer leaks {forbidden}"
		);
	}
	let run_body = brace_blocks_after_marker(&cli, "async fn run_inspect_async")
		.into_iter()
		.next()
		.expect("run_inspect_async body")
		.1;

	assert!(run_body.contains("print!(\"{}\", response.body)"));
	assert!(run_body.contains("!response.body.ends_with('\\n')"));
	assert!(!run_body.contains("trim_end"));
	assert!(!run_body.contains("pretty"));
	assert!(!run_body.contains("envelope"));

	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability source");
	assert!(observability.contains("serde::Deserialize"));
	assert!(cli.contains("serde_json::from_slice"));
	assert!(!observability.contains("deny_unknown_fields"));
	assert!(!cli.contains("deny_unknown_fields"));
}

#[test]
pub(crate) fn adr046_toron_inspect_exit_codes_are_pinned_to_0_1_2() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");
	let run_body = brace_blocks_after_marker(&cli, "async fn run_inspect_async")
		.into_iter()
		.next()
		.expect("run_inspect_async body")
		.1;

	assert!(run_body.contains("ExitCode::SUCCESS"));
	assert!(run_body.contains("ExitCode::from(1)"));
	assert!(run_body.contains("ExitCode::from(2)"));
	assert!(!run_body.contains("ExitCode::from(3)"));
}

#[test]
pub(crate) fn adr046_inspect_handles_only_200_and_503_status_codes() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");
	let inspect_body = brace_blocks_after_marker(&cli, "async fn fetch_ready_with_tcp_connect")
		.into_iter()
		.next()
		.expect("fetch_ready_with_tcp_connect body")
		.1;

	assert!(inspect_body.contains("response.status_code != 200"));
	assert!(inspect_body.contains("response.status_code != 503"));
	assert!(inspect_body.contains("InspectError::UnexpectedStatus"));
	assert!(cli.contains("unexpected HTTP status {status_code}"));
}

#[test]
pub(crate) fn adr046_inspect_has_tcp_connect_timeout() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");

	assert!(cli.contains("pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);"));
	assert!(cli.contains("tokio::time::timeout(connect_timeout, connect)"));
	assert!(cli.contains("TcpStream::connect(options.endpoint)"));
	assert!(cli.contains("CONNECT_TIMEOUT"));
}

#[test]
pub(crate) fn adr046_no_new_daemon_wire_surface_in_observability_module() {
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon source");
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability source");
	let sql_wire = std::fs::read_to_string(manifest_dir().join("../toron-client/src/protocol.rs"))
		.expect("read SQL wire");

	for forbidden in ["INSPECT_PATH", "INFO_PATH", "VERSION_PATH", "WORKERS_PATH"] {
		assert!(
			!observability.contains(forbidden),
			"ADR 046 must not add observability endpoint {forbidden}"
		);
	}
	assert!(daemon.contains("println!(\"READY {}\", daemon.handle().worker_endpoint);"));
	assert!(daemon.contains("println!(\"SQL_READY {sql_endpoint}\");"));
	assert!(daemon.contains("\"OBSERVABILITY_READY health={} ready={} metrics={}\""));
	assert!(!sql_wire.contains("Inspect"));
	assert!(!sql_wire.contains("AuthToken"));
}

#[test]
pub(crate) fn adr047_tls_pki_crate_exists_under_workspace_glob() {
	let root_manifest =
		std::fs::read_to_string(workspace_root_dir().join("Cargo.toml")).expect("read workspace");
	assert!(root_manifest.contains("\"crates/*\""));
	assert!(
		workspace_root_dir()
			.join("crates/tls-pki/Cargo.toml")
			.exists()
	);
}

#[test]
pub(crate) fn adr047_tls_pki_has_no_quinn_no_tokio_no_tracing_deps() {
	let manifest = std::fs::read_to_string(workspace_root_dir().join("crates/tls-pki/Cargo.toml"))
		.expect("read tls-pki manifest");

	for forbidden in [
		"quinn",
		"tokio",
		"tokio-rustls",
		"tracing",
		"async-trait",
		"reqwest",
		"hyper",
		"axum",
		"actix-web",
	] {
		assert!(
			!manifest.contains(forbidden),
			"tls-pki manifest must not contain dependency token {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr047_toron_tls_module_consumes_tls_pki_substrate() {
	let tls = std::fs::read_to_string(manifest_dir().join("src/network/tls.rs"))
		.expect("read Toron TLS source");

	for required in [
		"use tls_pki::",
		"TlsMaterial",
		"build_client_cert_chain_verifier",
		"build_server_cert_chain_verifier",
		"build_pki_rustls_server_config",
		"build_pki_rustls_client_config",
		"canonical_dns_name",
		"extract_dns_sans",
		"pub enum TlsError",
		"pub enum ListenerRole",
		"pub enum PeerKind",
		"ExactServerCertVerifier",
		"SanClientCertVerifier",
		"canonical_daemon_dns_name",
		"canonical_client_dns_name",
		"canonical_monitor_dns_name",
		"AUTH_QUIC_ALPN",
		"OBSERVABILITY_HTTP_ALPN",
	] {
		assert!(
			tls.contains(required),
			"Toron TLS source missing {required}"
		);
	}
}

#[test]
pub(crate) fn adr047_wendelon_tls_module_consumes_tls_pki_substrate() {
	let tls = std::fs::read_to_string(wendelon_manifest_dir().join("src/network/tls.rs"))
		.expect("read Wendelon TLS source");

	for required in [
		"pub use tls_pki::DEFAULT_CLUSTER_DOMAIN",
		"pub struct TlsMaterial(tls_pki::TlsMaterial)",
		"tls_pki::TlsMaterial::load",
		"tls_pki::load_ca_roots",
		"tls_pki::build_client_cert_chain_verifier",
		"tls_pki::build_server_cert_chain_verifier",
		"tls_pki::build_rustls_server_config_with_alpns",
		"tls_pki::build_rustls_client_config_with_alpns",
		"tls_pki::canonical_dns_name(\"node\"",
		"HRFT_ALPN",
		"ActorIdentity",
		"ActorTrustRoots",
		"VerifiedTransportPeer",
	] {
		assert!(
			tls.contains(required),
			"Wendelon TLS source missing {required}"
		);
	}
}

#[test]
pub(crate) fn adr047_no_wendelon_internal_types_in_tls_pki_public_api() {
	let source = tls_pki_source_text();

	for forbidden in [
		"RevocationSnapshot",
		"ActorPermission",
		"ActorIdentity",
		"ActorTrustRoots",
		"VerifiedTransportPeer",
		"ACTOR_CLAIMS_OID_DOTTED",
		"WendelonNode",
		"NodeId",
		"authorize_break_glass_scope",
		"authorize_actor",
	] {
		assert!(
			!source.contains(forbidden),
			"tls-pki public/source surface contains Wendelon-internal token {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr047_no_toron_internal_types_in_tls_pki_public_api() {
	let source = tls_pki_source_text();

	for forbidden in [
		"WorkerId",
		"ToronWorker",
		"ToronDaemon",
		"SqlClientTlsOptions",
		"InspectTlsOptions",
		"quinn::",
	] {
		assert!(
			!source.contains(forbidden),
			"tls-pki public/source surface contains Toron-internal token {forbidden}"
		);
	}
	// TORON-532's development PKI carries the protocol's generic
	// `tenant_id` actor claim for the scoped storage service principal. The
	// concrete Toron identities above remain forbidden from this crate.
}

#[test]
pub(crate) fn adr047_admit_san_accepts_parameterized_prefix_set() {
	let san = std::fs::read_to_string(workspace_root_dir().join("crates/tls-pki/src/san.rs"))
		.expect("read tls-pki san source");

	assert!(san.contains("pub fn admit_san("));
	assert!(san.contains("allowed_prefixes: &[&str]"));
	assert!(san.contains("allowed_prefixes.contains(&prefix)"));
	assert_eq!(
		tls_pki::admit_san("node-7.cluster.local", DEFAULT_CLUSTER_DOMAIN, &["node"])
			.expect("admit node")
			.id_token,
		"7"
	);
	assert!(
		tls_pki::admit_san("node-7.cluster.local", DEFAULT_CLUSTER_DOMAIN, &["client"]).is_err()
	);
}

#[test]
pub(crate) fn adr047_canonical_dns_name_parameterized_by_prefix() {
	let san = std::fs::read_to_string(workspace_root_dir().join("crates/tls-pki/src/san.rs"))
		.expect("read tls-pki san source");

	assert!(san.contains("pub fn canonical_dns_name("));
	assert!(san.contains("prefix: &str"));
	assert_eq!(
		tls_pki::canonical_dns_name("node", "7", DEFAULT_CLUSTER_DOMAIN),
		"node-7.cluster.local"
	);
	assert_eq!(
		tls_pki::canonical_dns_name(
			"toron",
			"550e8400-e29b-41d4-a716-446655440000",
			DEFAULT_CLUSTER_DOMAIN
		),
		"toron-550e8400-e29b-41d4-a716-446655440000.cluster.local"
	);
}

#[test]
pub(crate) fn adr047_no_alpn_constants_in_tls_pki() {
	let source = tls_pki_source_text();

	for forbidden in ["HRFT_ALPN", "AUTH_QUIC_ALPN", "OBSERVABILITY_HTTP_ALPN"] {
		assert!(
			!source.contains(forbidden),
			"tls-pki must not declare ring-local ALPN constant {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr047_byte_identical_post_extraction_with_prior_cycle_guards() {
	let architecture_test = architecture_corpus_source();

	for prefix in ["adr042_", "adr043_", "adr044_", "adr045_", "adr046_"] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
	for required in [
		"adr044_tls_policy_uses_exact_san_admission_per_listener",
		"adr045_ready_snapshot_excludes_high_cardinality_identity_fields",
		"adr046_toron_inspect_exit_codes_are_pinned_to_0_1_2",
	] {
		assert!(
			architecture_test.contains(required),
			"prior-cycle guard {required} must remain present"
		);
	}
}

#[test]
pub(crate) fn adr047_toron_der_and_cbor_parsing_delegates_to_tls_pki() {
	let manifest_dir = manifest_dir();
	let offenders = source_lines(&manifest_dir.join("src"))
		.into_iter()
		.filter(|(_, _, line)| {
			line.contains("DerReader")
				|| line.contains("CborReader")
				|| line.contains("SUBJECT_ALT_NAME_OID")
				|| line.contains("DNS_NAME_TAG")
		})
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"Toron source must delegate DER/CBOR parsing to tls-pki:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
pub(crate) fn adr047_new_tls_pki_types_are_brand_clean_and_no_v1_suffix() {
	let source = tls_pki_source_text();

	for forbidden in [
		"ToronTls",
		"WendelonTls",
		"NilketTls",
		"ToronPki",
		"WendelonPki",
		"TlsMaterialV1",
		"PkiErrorV1",
		"PeerIdentityV1",
		"DerReaderV1",
		"CborReaderV1",
	] {
		assert!(
			!source.contains(forbidden),
			"tls-pki contains forbidden branded/V1 token {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr048_observability_http_substrate_is_retargeted_to_observability_namespace() {
	let root_manifest =
		std::fs::read_to_string(workspace_root_dir().join("Cargo.toml")).expect("read workspace");
	assert!(root_manifest.contains("\"crates/*\""));
	assert!(
		workspace_root_dir()
			.join("crates/observability/Cargo.toml")
			.exists()
	);
	assert!(
		!workspace_root_dir()
			.join("crates/observability-http/Cargo.toml")
			.exists(),
		"ADR 065 retarget removes the flat observability-http crate"
	);
}

#[test]
pub(crate) fn adr048_observability_http_depends_only_on_tokio_io() {
	let manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/observability/Cargo.toml"))
			.expect("read observability manifest");
	let http_source = observability_http_source_text();
	assert!(manifest.contains(r#"default = []"#));
	assert!(manifest.contains(r#"http = ["dep:tokio"]"#));
	assert!(manifest.contains(
		r#"tokio = { version = "1.37", default-features = false, features = ["io-util", "macros", "rt"] }"#
	));
	for forbidden in [
		"hyper",
		"hyper-util",
		"axum",
		"actix-web",
		"tower",
		"reqwest",
		"ureq",
		"h2",
		"httparse",
		"bytes::",
		"tokio-util",
		"rustls",
		"tokio-rustls",
		"quinn",
		"tls-pki",
		"tracing",
	] {
		assert!(
			!http_source.contains(forbidden),
			"observability-http source must not contain dependency token {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr048_observability_http_has_bounded_header_and_body_limits() {
	let lib =
		std::fs::read_to_string(workspace_root_dir().join("crates/observability/src/http/mod.rs"))
			.expect("read observability::http module");

	for required in [
		"pub const MAX_REQUEST_HEADER_BYTES: usize = 8 * 1024;",
		"pub const MAX_RESPONSE_HEADER_BYTES: usize = 8 * 1024;",
		"pub const MAX_RESPONSE_BODY_BYTES: usize = 64 * 1024;",
	] {
		assert!(lib.contains(required), "missing bounded limit {required}");
	}
}

#[test]
pub(crate) fn adr048_observability_http_does_not_own_timeouts() {
	let source = observability_http_source_text();

	for forbidden in [
		"Duration",
		"tokio::time",
		"timeout",
		"READ_REQUEST_TIMEOUT",
		"CONNECT_TIMEOUT",
	] {
		assert!(
			!source.contains(forbidden),
			"observability-http must not own timeout token {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr048_observability_http_public_surface_is_small() {
	let source = observability_http_source_text();

	for required in [
		"pub const MAX_REQUEST_HEADER_BYTES",
		"pub const MAX_RESPONSE_HEADER_BYTES",
		"pub const MAX_RESPONSE_BODY_BYTES",
		"pub struct RequestLine",
		"pub struct HttpResponse",
		"pub async fn read_request",
		"pub fn parse_request_line",
		"pub async fn read_response",
		"pub async fn write_request",
		"pub async fn write_response",
	] {
		assert!(
			source.contains(required),
			"observability-http must expose {required}"
		);
	}
	for forbidden in [
		"pub struct Router",
		"pub struct Listener",
		"pub fn listen",
		"pub fn dispatch",
		"pub fn route",
		"pub struct StatusCode",
		"pub enum Status",
		"rustls::",
		"quinn::",
		"tls_pki::",
		"tracing::",
		"TcpListener",
		"TcpStream",
	] {
		assert!(
			!source.contains(forbidden),
			"observability-http must not expose {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr048_toron_observability_http_module_is_shim_only() {
	let http = std::fs::read_to_string(manifest_dir().join("src/network/observability_http.rs"))
		.expect("read observability HTTP shim");

	assert!(http.contains("pub const READ_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);"));
	assert!(http.contains("pub use observability::http::"));
	for required in [
		"HttpResponse",
		"MAX_REQUEST_HEADER_BYTES",
		"MAX_RESPONSE_HEADER_BYTES",
		"MAX_RESPONSE_BODY_BYTES",
		"RequestLine",
		"parse_request_line",
		"read_request",
		"read_response",
		"write_request",
		"write_response",
	] {
		assert!(http.contains(required), "shim missing re-export {required}");
	}
	for forbidden in ["header_end", "content_length", "body_len", "loop {"] {
		assert!(
			!http.contains(forbidden),
			"observability HTTP shim contains parser token {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr048_toron_observability_and_inspect_use_shared_http_helper() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability source");
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");

	assert!(observability.contains("observability_http::parse_request_line"));
	assert!(cli.contains("observability_http::write_request"));
	assert!(cli.contains("observability_http::read_response"));
}

#[test]
pub(crate) fn adr048_no_new_toron_observability_routes() {
	let observability =
		std::fs::read_to_string(manifest_dir().join("src/network/observability.rs"))
			.expect("read observability source");

	for required in [
		"pub const HEALTH_PATH: &str = \"/health\";",
		"pub const READY_PATH: &str = \"/ready\";",
		"pub const METRICS_PATH: &str = \"/metrics\";",
	] {
		assert!(
			observability.contains(required),
			"missing observability route {required}"
		);
	}
	assert_eq!(observability.matches("_PATH: &str").count(), 3);
	assert!(!observability.contains("INSPECT_PATH"));
	assert!(!observability.contains("INFO_PATH"));
	assert!(!observability.contains("VERSION_PATH"));
	assert!(!observability.contains("WORKERS_PATH"));
}

#[test]
pub(crate) fn adr048_no_nilket_root_runtime_dependency_change() {
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");

	for forbidden in [
		"tokio",
		"clap",
		"tracing",
		"rustls",
		"tokio-rustls",
		"observability-http",
	] {
		assert!(
			!nilket_manifest.contains(forbidden),
			"nilket manifest gained direct runtime dependency {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr048_no_wendelon_inspect_branch_this_cycle() {
	let cli = std::fs::read_to_string(wendelon_manifest_dir().join("src/cli.rs"))
		.expect("read Wendelon CLI source");

	assert!(cli.contains("[cmd, rest @ ..] if cmd == \"inspect\""));
	assert!(cli.contains("struct InspectOptions"));
	assert!(cli.contains("observability_http::write_request"));
	assert!(!cli.contains("ToronInspect"));
}

#[test]
pub(crate) fn adr048_wendelon_observability_http_helpers_remain_local() {
	let observability =
		std::fs::read_to_string(wendelon_manifest_dir().join("src/metrics/observability.rs"))
			.expect("read Wendelon observability source");
	let exporter = std::fs::read_to_string(wendelon_manifest_dir().join("src/metrics/exporter.rs"))
		.expect("read Wendelon exporter source");

	for required in [
		"async fn read_request<S",
		"fn parse_request_line(",
		"async fn write_response<S",
	] {
		assert!(
			observability.contains(required),
			"Wendelon observability source missing local helper {required}"
		);
	}
	assert!(exporter.contains("async fn handle_connection("));
	assert!(exporter.contains("content-type: text/plain"));
	assert!(!observability.contains("observability_http"));
	assert!(!exporter.contains("observability_http"));
}

#[test]
pub(crate) fn adr048_prior_adr04x_guards_continue_to_pass() {
	let architecture_test = architecture_corpus_source();

	for prefix in [
		"adr040_", "adr041_", "adr042_", "adr043_", "adr044_", "adr045_", "adr046_", "adr047_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
	for required in [
		"adr040_nilket_root_has_no_role_runtime_dependencies",
		"adr045_ready_snapshot_excludes_high_cardinality_identity_fields",
		"adr046_toron_inspect_exit_codes_are_pinned_to_0_1_2",
		"adr047_byte_identical_post_extraction_with_prior_cycle_guards",
	] {
		assert!(
			architecture_test.contains(required),
			"prior-cycle guard {required} must remain present"
		);
	}
}

#[test]
pub(crate) fn adr048_new_observability_http_types_are_brand_clean_and_no_v1_suffix() {
	let source = observability_http_source_text();

	for forbidden in [
		"ToronHttp",
		"WendelonHttp",
		"NilketHttp",
		"HttpResponseV1",
		"RequestLineV1",
		"ObservabilityHttpV1",
	] {
		assert!(
			!source.contains(forbidden),
			"observability-http contains forbidden branded/V1 token {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr046_inspect_uses_hand_rolled_http_no_http_crate_no_hyper_no_reqwest() {
	let cargo =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read Cargo.toml");
	let http = std::fs::read_to_string(manifest_dir().join("src/network/observability_http.rs"))
		.expect("read observability HTTP source");

	for forbidden in [
		"http =",
		"hyper",
		"reqwest",
		"ureq",
		"tower",
		"axum",
		"actix-web",
		"hyper-util",
	] {
		if forbidden == "http =" {
			assert!(
				!cargo
					.lines()
					.map(str::trim)
					.any(|line| line.starts_with("http =")),
				"Toron Cargo.toml must not include a direct http crate dependency"
			);
			continue;
		}
		assert!(
			!cargo.contains(forbidden),
			"Toron Cargo.toml must not include {forbidden}"
		);
	}
	for required in ["write_request", "read_response", "HttpResponse"] {
		assert!(
			http.contains(required),
			"missing HTTP client helper {required}"
		);
	}
}

#[test]
pub(crate) fn adr046_nilket_root_stays_runtime_free() {
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");

	for forbidden in ["tokio", "clap", "tracing", "rustls", "tokio-rustls"] {
		assert!(
			!nilket_manifest.contains(forbidden),
			"nilket manifest gained direct runtime dependency {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr046_new_inspect_types_are_brand_clean_and_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"ToronInspect",
		"ToronReadyClient",
		"ToronHttpResponse",
		"InspectOptionsV1",
		"InspectTlsOptionsV1",
		"InspectErrorV1",
		"ReadyClientV1",
		"HttpResponseV1",
	]);
}

#[test]
pub(crate) fn adr044_remote_static_worker_requires_tls_and_san_binding() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let transport = std::fs::read_to_string(manifest_dir().join("src/server/transport.rs"))
		.expect("read Toron transport source");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");

	assert!(config.contains("NON_LOOPBACK_STATIC_WORKER_DIAGNOSTIC"));
	assert!(config.contains("!endpoint.ip().is_loopback() && tls.is_none()"));
	assert!(transport.contains("async fn connect_worker("));
	assert!(transport.contains("canonical_daemon_dns_name(&worker.worker_id, cluster_domain)"));
	assert!(transport.contains("connect_stream_with_server_name(remote, &server_name)"));
	assert!(server.contains("transport.connect_worker(worker).await"));
}

#[test]
pub(crate) fn adr044_one_tls_material_set_per_daemon() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");

	assert!(config.contains("tls: Option<RawTlsConfig>"));
	assert!(config.contains("pub tls: Option<TlsConfig>"));
	for forbidden in ["[worker.tls]", "[sql.tls]", "worker_tls", "sql_tls"] {
		assert!(
			!config.contains(forbidden),
			"ADR 044 must use one shared TLS material set, found {forbidden}"
		);
	}
	assert_eq!(daemon.matches("ListenerPolicy::load").count(), 1);
	assert!(daemon.contains("server_config_for(ListenerRole::WorkerTask)"));
	assert!(daemon.contains("server_config_for(ListenerRole::Sql)"));
	assert!(daemon.contains("policy.client_config()"));
}

#[test]
pub(crate) fn adr044_san_convention_mirrors_wendelon_shape() {
	let worker_id = WorkerId::from_u128(0x550e_8400_e29b_41d4_a716_4466_5544_0000);
	assert_eq!(
		canonical_daemon_dns_name(&worker_id, DEFAULT_CLUSTER_DOMAIN),
		"toron-550e8400-e29b-41d4-a716-446655440000.cluster.local"
	);
	assert_eq!(
		canonical_client_dns_name("analytics", DEFAULT_CLUSTER_DOMAIN),
		"client-analytics.cluster.local"
	);

	let wendelon_tls = std::fs::read_to_string(wendelon_manifest_dir().join("src/network/tls.rs"))
		.expect("read Wendelon TLS source");
	assert!(wendelon_tls.contains("pub fn canonical_node_dns_name"));
	assert!(wendelon_tls.contains("canonical_dns_name(\"node\""));
}

#[test]
pub(crate) fn adr044_sql_listener_client_admission_is_client_prefix_only() {
	assert!(
		admit_san(
			"client-7.cluster.local",
			ListenerRole::Sql,
			DEFAULT_CLUSTER_DOMAIN
		)
		.is_ok()
	);
	for san in [
		"toron-550e8400-e29b-41d4-a716-446655440000.cluster.local",
		"node-7.cluster.local",
		"service-analytics.cluster.local",
		"client-analytics.cluster.local",
		"client-.cluster.local",
	] {
		assert!(
			admit_san(san, ListenerRole::Sql, DEFAULT_CLUSTER_DOMAIN).is_err(),
			"SQL listener should reject {san}"
		);
	}
}

#[test]
pub(crate) fn adr044_no_spiffe_svid_or_uri_san_identity_model() {
	for path in [
		"src/network/tls.rs",
		"src/cli.rs",
		"src/daemon.rs",
		"src/server/transport.rs",
	] {
		let source = std::fs::read_to_string(manifest_dir().join(path)).expect("read source");
		for forbidden in [
			"SPIFFE",
			"spiffe",
			"SVID",
			"uniformResourceIdentifier",
			"UriSan",
			"URI SAN",
		] {
			assert!(
				!source.contains(forbidden),
				"{path} contains ADR 044-forbidden identity model token {forbidden}"
			);
		}
	}
}

#[test]
pub(crate) fn adr044_new_tls_policy_names_are_brand_clean_and_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"ToronTlsConfig",
		"ToronTlsListenerPolicy",
		"ToronTlsError",
		"RawToronTlsConfig",
		"DEFAULT_TORON_CLUSTER_DOMAIN",
		"TORON_AUTH_QUIC_ALPN",
		"canonical_toron_dns_name",
		"canonical_toron_client_dns_name",
		"ToronQuicTls",
		"ToronMtls",
		"TlsConfigV1",
		"ListenerPolicyV1",
	]);
}
