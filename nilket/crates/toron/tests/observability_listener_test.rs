use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rcgen::{
	BasicConstraints, Certificate, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa,
	KeyUsagePurpose,
};
use rustls::pki_types::{
	CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, pem::PemObject,
};
use rustls::{ClientConfig, RootCertStore};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use toron::catalog::{DescriptorRefreshMetrics, DescriptorSyncMetrics};
use toron::config::{
	CoordinatorConfig, LISTENER_BIND_COLLISION_DIAGNOSTIC,
	OBSERVABILITY_HEALTH_LOOPBACK_DIAGNOSTIC, OBSERVABILITY_REQUIRES_TLS_DIAGNOSTIC,
	ObservabilityConfig, ObservabilityListeners, SqlConfig, SqlTransport, TlsConfig,
	ToronBootstrapConfig, ToronCatalogConfig, ToronNodeConfig, ToronStorageConfig,
	ToronWendelonBootstrapPeer, ToronWendelonStorageConfig, ToronWorkerConfig, TracingFormat,
	load_toron_bootstrap_config,
};
use toron::daemon::ToronDaemon;
use toron::distributed::budget::{
	AdmissionAttempt, AdmissionResource, ByteCount, TenantAdmissionController,
	TenantAdmissionRejectionCounters, TenantBudget, TenantBudgetPolicy,
};
use toron::distributed::identity::WorkerId;
use toron::network::observability::{MetricsRegistry, MetricsRegistryConfig, ReadinessSnapshot};
use toron::network::tls::{
	DEFAULT_CLUSTER_DOMAIN, canonical_client_dns_name, canonical_daemon_dns_name,
	canonical_monitor_dns_name,
};
use toron::storage::TenantId;

struct TestPki {
	dir: PathBuf,
	ca_cert: PathBuf,
	daemon_cert: PathBuf,
	daemon_key: PathBuf,
	monitor_cert: PathBuf,
	monitor_key: PathBuf,
	client_cert: PathBuf,
	client_key: PathBuf,
}

fn ready_snapshot() -> ReadinessSnapshot {
	ReadinessSnapshot {
		ready: true,
		worker_listener_bound: true,
		sql_listener_bound: true,
		observability_listeners_bound: true,
		descriptors_loaded: 0,
		static_workers_configured: 0,
		uptime_seconds: 0,
		degraded_reason: None,
	}
}

fn sample_resource_metrics_body() -> String {
	let mut per_tenant = HashMap::new();
	per_tenant.insert(
		TenantId(7),
		TenantBudget::new(ByteCount::new(8), ByteCount::new(16)),
	);
	let policy = TenantBudgetPolicy::new(
		TenantBudget::new(ByteCount::new(8), ByteCount::new(32)),
		per_tenant,
	);
	let tenant_admission = TenantAdmissionController::new(policy.clone(), None);
	let admission_rejections = TenantAdmissionRejectionCounters::new(&policy);
	let _configured = match tenant_admission.try_admit(TenantId(7), ByteCount::new(8)) {
		AdmissionAttempt::Granted(reservation) => reservation,
		other => panic!("expected configured admission grant, got {other:?}"),
	};
	let _other = match tenant_admission.try_admit(TenantId(9), ByteCount::new(8)) {
		AdmissionAttempt::Granted(reservation) => reservation,
		other => panic!("expected other admission grant, got {other:?}"),
	};
	admission_rejections.increment(TenantId(7), AdmissionResource::Memory);
	admission_rejections.increment(TenantId(9), AdmissionResource::Concurrency);
	let metrics = MetricsRegistry::new(MetricsRegistryConfig {
		descriptors_loaded: 0,
		static_workers_configured: 0,
		tenant_budgets: policy,
		descriptor_sync_metrics: Arc::new(DescriptorSyncMetrics::default()),
		descriptor_refresh_metrics: Arc::new(DescriptorRefreshMetrics::default()),
		tenant_admission,
		admission_rejections,
	});

	metrics.render_text(&ready_snapshot())
}

#[test]
fn toron_config_missing_observability_section_loads_byte_identical_to_adr044() {
	let dir = fresh_dir("observability-config-missing");
	let config_path = dir.join("toron.toml");
	std::fs::write(&config_path, minimal_toml_without_observability()).expect("write config");

	let config = load_toron_bootstrap_config(&config_path).expect("load config");

	assert_eq!(config.observability.level, "info");
	assert_eq!(config.observability.format, TracingFormat::Pretty);
	assert!(config.observability.listeners.is_none());
}

#[test]
fn toron_health_endpoint_rejects_non_loopback_bind() {
	let worker_id = WorkerId::from_u128(45);
	let pki = TestPki::generate(worker_id, "prometheus");
	let config_path = pki.dir.join("health-non-loopback.toml");
	std::fs::write(
		&config_path,
		toml_with_observability_listeners(
			&pki,
			worker_id,
			"0.0.0.0:7780",
			"127.0.0.1:7781",
			"127.0.0.1:7782",
			true,
		),
	)
	.expect("write config");

	let error = load_toron_bootstrap_config(&config_path).expect_err("reject config");
	assert!(
		error
			.to_string()
			.contains(OBSERVABILITY_HEALTH_LOOPBACK_DIAGNOSTIC),
		"{error}"
	);
}

#[test]
fn toron_config_observability_port_collision_rejected() {
	let worker_id = WorkerId::from_u128(46);
	let pki = TestPki::generate(worker_id, "prometheus");
	let config_path = pki.dir.join("port-collision.toml");
	std::fs::write(
		&config_path,
		toml_with_observability_listeners(
			&pki,
			worker_id,
			"127.0.0.1:7780",
			"127.0.0.1:7780",
			"127.0.0.1:7782",
			true,
		),
	)
	.expect("write config");

	let error = load_toron_bootstrap_config(&config_path).expect_err("reject config");
	assert!(
		error
			.to_string()
			.contains(LISTENER_BIND_COLLISION_DIAGNOSTIC),
		"{error}"
	);
}

#[test]
fn toron_ready_and_metrics_require_tls_when_configured() {
	let worker_id = WorkerId::from_u128(47);
	let pki = TestPki::generate(worker_id, "prometheus");
	let config_path = pki.dir.join("observability-no-tls.toml");
	std::fs::write(
		&config_path,
		toml_with_observability_listeners(
			&pki,
			worker_id,
			"127.0.0.1:7780",
			"127.0.0.1:7781",
			"127.0.0.1:7782",
			false,
		),
	)
	.expect("write config");

	let error = load_toron_bootstrap_config(&config_path).expect_err("reject config");
	assert!(
		error
			.to_string()
			.contains(OBSERVABILITY_REQUIRES_TLS_DIAGNOSTIC),
		"{error}"
	);
}

#[tokio::test]
async fn toron_health_endpoint_returns_200_ok_with_lowercase_ok_body() {
	let worker_id = WorkerId::from_u128(48);
	let pki = TestPki::generate(worker_id, "prometheus");
	let daemon = ToronDaemon::start(observability_config(worker_id, &pki))
		.await
		.expect("start daemon");
	let endpoint = daemon
		.handle()
		.observability_endpoints
		.expect("observability endpoints")
		.health;

	let response = plain_http_request(endpoint, "GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
		.await
		.expect("health response");

	assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
	assert!(response.contains("Content-Length: 3\r\n"));
	assert_eq!(response_body(&response), "ok\n");

	daemon.shutdown().await.expect("shutdown daemon");
}

#[tokio::test]
async fn toron_ready_endpoint_emits_json_snapshot_with_expected_fields_in_stable_order() {
	let worker_id = WorkerId::from_u128(49);
	let pki = TestPki::generate(worker_id, "prometheus");
	let daemon = ToronDaemon::start(observability_config(worker_id, &pki))
		.await
		.expect("start daemon");
	let endpoint = daemon
		.handle()
		.observability_endpoints
		.expect("observability endpoints")
		.ready;
	let server_name = canonical_daemon_dns_name(&worker_id, DEFAULT_CLUSTER_DOMAIN);

	let response = mtls_http_request(
		endpoint,
		&server_name,
		&pki.monitor_cert,
		&pki.monitor_key,
		&pki.ca_cert,
		"GET /ready HTTP/1.1\r\nHost: localhost\r\n\r\n",
	)
	.await
	.expect("ready response");
	let body = response_body(&response);

	assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
	for token in [
		"\"ready\":true",
		"\"worker_listener_bound\":true",
		"\"sql_listener_bound\":true",
		"\"observability_listeners_bound\":true",
		"\"descriptors_loaded\":0",
		"\"static_workers_configured\":0",
		"\"uptime_seconds\":",
		"\"degraded_reason\":null",
	] {
		assert!(body.contains(token), "missing {token} in {body}");
	}
	assert_fields_in_order(
		body,
		&[
			"\"ready\"",
			"\"worker_listener_bound\"",
			"\"sql_listener_bound\"",
			"\"observability_listeners_bound\"",
			"\"descriptors_loaded\"",
			"\"static_workers_configured\"",
			"\"uptime_seconds\"",
			"\"degraded_reason\"",
		],
	);
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
		assert!(!body.contains(forbidden), "{forbidden} leaked into /ready");
	}

	daemon.shutdown().await.expect("shutdown daemon");
}

#[tokio::test]
async fn toron_metrics_endpoint_emits_prometheus_text_format_with_brand_clean_names() {
	let worker_id = WorkerId::from_u128(50);
	let pki = TestPki::generate(worker_id, "prometheus");
	let daemon = ToronDaemon::start(observability_config(worker_id, &pki))
		.await
		.expect("start daemon");
	let endpoint = daemon
		.handle()
		.observability_endpoints
		.expect("observability endpoints")
		.metrics;
	let server_name = canonical_daemon_dns_name(&worker_id, DEFAULT_CLUSTER_DOMAIN);

	let response = mtls_http_request(
		endpoint,
		&server_name,
		&pki.monitor_cert,
		&pki.monitor_key,
		&pki.ca_cert,
		"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n",
	)
	.await
	.expect("metrics response");
	let body = response_body(&response);

	assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
	assert!(response.contains("text/plain; version=0.0.4; charset=utf-8"));
	for metric in [
		"daemon_ready 1\n",
		"daemon_uptime_seconds ",
		"listener_up{listener=\"health\"} 1\n",
		"listener_up{listener=\"ready\"} 1\n",
		"listener_up{listener=\"metrics\"} 1\n",
		"listener_accepts_total{listener=\"metrics\"} 1\n",
		"listener_rejects_total{listener=\"ready\",reason=\"mtls_handshake_failure\"} 0\n",
		"metrics_scrapes_total 1\n",
		"descriptors_loaded 0\n",
		"static_workers_configured 0\n",
		"tenant_memory_bytes_used{tenant_id=\"other\"} 0\n",
		"tenant_memory_bytes_limit{tenant_id=\"other\"} 67108864\n",
		"tenant_concurrency_active{tenant_id=\"other\"} 0\n",
		"tenant_concurrency_limit{tenant_id=\"other\"} 1\n",
		"tenant_admission_rejections_total{tenant_id=\"other\",resource=\"memory\"} 0\n",
		"tenant_admission_rejections_total{tenant_id=\"other\",resource=\"concurrency\"} 0\n",
	] {
		assert!(body.contains(metric), "missing metric {metric} in {body}");
	}
	for forbidden in ["toron_", "query_id", "task_id", "sql_text", "_bucket"] {
		assert!(
			!body.contains(forbidden),
			"{forbidden} leaked into /metrics"
		);
	}

	daemon.shutdown().await.expect("shutdown daemon");
}

#[test]
fn resource_metrics_label_configured_tenant() {
	let body = sample_resource_metrics_body();

	for metric in [
		"tenant_memory_bytes_used{tenant_id=\"7\"} 8\n",
		"tenant_memory_bytes_limit{tenant_id=\"7\"} 16\n",
		"tenant_concurrency_active{tenant_id=\"7\"} 1\n",
		"tenant_concurrency_limit{tenant_id=\"7\"} 2\n",
		"tenant_admission_rejections_total{tenant_id=\"7\",resource=\"memory\"} 1\n",
		"tenant_admission_rejections_total{tenant_id=\"7\",resource=\"concurrency\"} 0\n",
	] {
		assert!(body.contains(metric), "missing metric {metric} in {body}");
	}
}

#[test]
fn resource_metrics_bucket_unconfigured_tenant_as_other() {
	let body = sample_resource_metrics_body();

	for metric in [
		"tenant_memory_bytes_used{tenant_id=\"other\"} 8\n",
		"tenant_concurrency_active{tenant_id=\"other\"} 1\n",
		"tenant_admission_rejections_total{tenant_id=\"other\",resource=\"concurrency\"} 1\n",
	] {
		assert!(body.contains(metric), "missing metric {metric} in {body}");
	}
	assert!(!body.contains("tenant_id=\"9\""));
}

#[test]
fn resource_metrics_expose_memory_and_concurrency_limits() {
	let body = sample_resource_metrics_body();

	for metric in [
		"tenant_memory_bytes_limit{tenant_id=\"7\"} 16\n",
		"tenant_concurrency_limit{tenant_id=\"7\"} 2\n",
		"tenant_memory_bytes_limit{tenant_id=\"other\"} 32\n",
		"tenant_concurrency_limit{tenant_id=\"other\"} 4\n",
	] {
		assert!(body.contains(metric), "missing metric {metric} in {body}");
	}
}

#[test]
fn resource_admission_rejections_are_labeled_by_resource() {
	let body = sample_resource_metrics_body();

	for metric in [
		"tenant_admission_rejections_total{tenant_id=\"7\",resource=\"memory\"} 1\n",
		"tenant_admission_rejections_total{tenant_id=\"7\",resource=\"concurrency\"} 0\n",
		"tenant_admission_rejections_total{tenant_id=\"other\",resource=\"memory\"} 0\n",
		"tenant_admission_rejections_total{tenant_id=\"other\",resource=\"concurrency\"} 1\n",
	] {
		assert!(body.contains(metric), "missing metric {metric} in {body}");
	}
}

#[test]
fn queue_and_borrow_metrics_are_configured_plus_other_no_table_labels() {
	let mut per_tenant = HashMap::new();
	per_tenant.insert(
		TenantId(7),
		TenantBudget::with_admission_limits(
			ByteCount::new(8),
			ByteCount::new(8),
			NonZeroU32::new(1).expect("nonzero"),
			NonZeroU32::new(1).expect("nonzero"),
			NonZeroU32::new(1).expect("nonzero"),
			1,
			ByteCount::new(8),
			1,
		),
	);
	let policy = TenantBudgetPolicy::new(
		TenantBudget::new(ByteCount::new(8), ByteCount::new(8)),
		per_tenant,
	);
	let tenant_admission = TenantAdmissionController::new(
		policy.clone(),
		Some(toron::distributed::budget::AdmissionPolicy {
			max_queued_queries: 1,
			max_queue_wait_ms: toron::distributed::budget::DurationMillis::new(100),
			shared_memory: ByteCount::new(8),
			shared_concurrent_queries: 1,
		}),
	);
	let _owned = match tenant_admission.try_admit(TenantId(7), ByteCount::new(8)) {
		AdmissionAttempt::Granted(reservation) => reservation,
		other => panic!("expected owned grant, got {other:?}"),
	};
	let _borrowed = match tenant_admission.try_admit(TenantId(7), ByteCount::new(8)) {
		AdmissionAttempt::Granted(reservation) => reservation,
		other => panic!("expected borrowed grant, got {other:?}"),
	};
	let _queued = match tenant_admission.try_admit(TenantId(7), ByteCount::new(8)) {
		AdmissionAttempt::Queued(queued) => queued,
		other => panic!("expected queued admission, got {other:?}"),
	};
	assert!(matches!(
		tenant_admission.try_admit(TenantId(7), ByteCount::new(8)),
		AdmissionAttempt::Rejected(_)
	));
	let metrics = MetricsRegistry::new(MetricsRegistryConfig {
		descriptors_loaded: 0,
		static_workers_configured: 0,
		tenant_budgets: policy,
		descriptor_sync_metrics: Arc::new(DescriptorSyncMetrics::default()),
		descriptor_refresh_metrics: Arc::new(DescriptorRefreshMetrics::default()),
		tenant_admission,
		admission_rejections: TenantAdmissionRejectionCounters::new(&TenantBudgetPolicy::new(
			TenantBudget::new(ByteCount::new(8), ByteCount::new(8)),
			HashMap::new(),
		)),
	});
	let body = metrics.render_text(&ready_snapshot());

	for metric in [
		"tenant_admission_queue_depth{tenant_id=\"7\"} 1\n",
		"tenant_admission_borrowed_memory_bytes{tenant_id=\"7\"} 8\n",
		"tenant_admission_borrowed_concurrency{tenant_id=\"7\"} 1\n",
		"tenant_admission_queue_full_rejections_total{tenant_id=\"7\"} 1\n",
		"tenant_admission_queue_depth{tenant_id=\"other\"} 0\n",
	] {
		assert!(body.contains(metric), "missing metric {metric} in {body}");
	}
	for forbidden in [
		"table_id",
		"query_id",
		"object_id",
		"descriptor",
		"fingerprint",
	] {
		let offenders = body
			.lines()
			.filter(|line| line.contains("tenant_admission_") && line.contains(forbidden))
			.collect::<Vec<_>>();
		assert!(
			offenders.is_empty(),
			"{forbidden} leaked into admission metrics: {offenders:?}"
		);
	}
}

#[test]
fn general_daemon_metrics_do_not_gain_tenant_labels() {
	let body = sample_resource_metrics_body();

	assert!(body.contains("listener_accepts_total{listener=\"metrics\"} 0\n"));
	assert!(!body.contains("listener_accepts_total{tenant_id"));
	assert!(!body.contains("listener_rejects_total{tenant_id"));
	assert!(!body.contains("daemon_ready{tenant_id"));
	assert!(!body.contains("daemon_uptime_seconds{tenant_id"));
	assert!(!body.contains("descriptors_loaded{tenant_id"));
	assert!(!body.contains("static_workers_configured{tenant_id"));
}

#[test]
fn catalog_descriptor_sync_metrics_are_aggregate_outcome_only() {
	let body = sample_resource_metrics_body();

	for metric in [
		"catalog_descriptor_sync_requests_total{outcome=\"success\"} 0\n",
		"catalog_descriptor_sync_requests_total{outcome=\"not_found\"} 0\n",
		"catalog_descriptor_sync_requests_total{outcome=\"throttled\"} 0\n",
		"catalog_descriptor_sync_requests_total{outcome=\"error\"} 0\n",
	] {
		assert!(body.contains(metric), "missing metric {metric} in {body}");
	}
	for forbidden in [
		"catalog_descriptor_sync_requests_total{tenant_id",
		"catalog_descriptor_sync_requests_total{namespace",
		"catalog_descriptor_sync_requests_total{table",
		"catalog_descriptor_sync_requests_total{object",
	] {
		assert!(!body.contains(forbidden), "{forbidden} leaked into metrics");
	}
}

#[test]
fn catalog_descriptor_refresh_metrics_are_aggregate_outcome_only() {
	let body = sample_resource_metrics_body();

	for metric in [
		"catalog_descriptor_refresh_requests_total{outcome=\"invalidated\"} 0\n",
		"catalog_descriptor_refresh_requests_total{outcome=\"missing\"} 0\n",
		"catalog_descriptor_refresh_requests_total{outcome=\"replacement\"} 0\n",
	] {
		assert!(body.contains(metric), "missing metric {metric} in {body}");
	}
	for forbidden in [
		"catalog_descriptor_refresh_requests_total{tenant_id",
		"catalog_descriptor_refresh_requests_total{namespace",
		"catalog_descriptor_refresh_requests_total{table",
		"catalog_descriptor_refresh_requests_total{object",
		"catalog_descriptor_refresh_requests_total{fingerprint",
	] {
		assert!(!body.contains(forbidden), "{forbidden} leaked into metrics");
	}
}

#[tokio::test]
async fn toron_observability_ready_metrics_preserve_responses_post_extraction() {
	let worker_id = WorkerId::from_u128(54);
	let pki = TestPki::generate(worker_id, "prometheus");
	let daemon = ToronDaemon::start(observability_config(worker_id, &pki))
		.await
		.expect("start daemon");
	let endpoints = daemon
		.handle()
		.observability_endpoints
		.expect("observability endpoints");
	let server_name = canonical_daemon_dns_name(&worker_id, DEFAULT_CLUSTER_DOMAIN);

	let ready_response = mtls_http_request(
		endpoints.ready,
		&server_name,
		&pki.monitor_cert,
		&pki.monitor_key,
		&pki.ca_cert,
		"GET /ready HTTP/1.1\r\nHost: localhost\r\n\r\n",
	)
	.await
	.expect("ready response");
	let metrics_response = mtls_http_request(
		endpoints.metrics,
		&server_name,
		&pki.monitor_cert,
		&pki.monitor_key,
		&pki.ca_cert,
		"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n",
	)
	.await
	.expect("metrics response");

	assert!(ready_response.starts_with("HTTP/1.1 200 OK\r\n"));
	assert!(ready_response.contains("Content-Type: application/json; charset=utf-8\r\n"));
	assert!(response_body(&ready_response).contains("\"ready\":true"));
	assert!(metrics_response.starts_with("HTTP/1.1 200 OK\r\n"));
	assert!(
		metrics_response.contains("Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n")
	);
	assert!(response_body(&metrics_response).contains("metrics_scrapes_total 1\n"));

	daemon.shutdown().await.expect("shutdown daemon");
}

#[tokio::test]
async fn toron_ready_and_metrics_reject_peer_without_monitor_san_prefix() {
	let worker_id = WorkerId::from_u128(51);
	let pki = TestPki::generate(worker_id, "prometheus");
	let daemon = ToronDaemon::start(observability_config(worker_id, &pki))
		.await
		.expect("start daemon");
	let endpoints = daemon
		.handle()
		.observability_endpoints
		.expect("observability endpoints");
	let server_name = canonical_daemon_dns_name(&worker_id, DEFAULT_CLUSTER_DOMAIN);

	assert_mtls_request_rejected(
		endpoints.ready,
		&server_name,
		&pki.client_cert,
		&pki.client_key,
		&pki.ca_cert,
		"GET /ready HTTP/1.1\r\nHost: localhost\r\n\r\n",
	)
	.await;
	assert_mtls_request_rejected(
		endpoints.metrics,
		&server_name,
		&pki.daemon_cert,
		&pki.daemon_key,
		&pki.ca_cert,
		"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n",
	)
	.await;

	daemon.shutdown().await.expect("shutdown daemon");
}

#[test]
fn toron_observability_ready_stdout_line_appended_after_sql_ready_with_pinned_format() {
	let daemon = std::fs::read_to_string(
		std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/daemon.rs"),
	)
	.expect("read daemon source");

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

#[tokio::test]
async fn toron_observability_listener_closes_slow_client_after_read_timeout() {
	let worker_id = WorkerId::from_u128(52);
	let pki = TestPki::generate(worker_id, "prometheus");
	let daemon = ToronDaemon::start(observability_config(worker_id, &pki))
		.await
		.expect("start daemon");
	let endpoint = daemon
		.handle()
		.observability_endpoints
		.expect("observability endpoints")
		.health;
	let mut stream = TcpStream::connect(connectable_loopback_addr(endpoint))
		.await
		.expect("connect health");
	stream.write_all(b"GET /").await.expect("partial request");
	stream.flush().await.expect("flush partial request");

	let mut byte = [0u8; 1];
	let read = tokio::time::timeout(Duration::from_secs(7), stream.read(&mut byte))
		.await
		.expect("server closed slow client within timeout")
		.expect("read slow client close");

	assert_eq!(read, 0);

	daemon.shutdown().await.expect("shutdown daemon");
}

impl TestPki {
	fn generate(worker_id: WorkerId, monitor_principal: &str) -> Self {
		let dir = fresh_dir("observability-listener");
		let ca = ca_cert();
		let daemon_dns = canonical_daemon_dns_name(&worker_id, DEFAULT_CLUSTER_DOMAIN);
		let monitor_dns = canonical_monitor_dns_name(monitor_principal, DEFAULT_CLUSTER_DOMAIN);
		let client_dns = canonical_client_dns_name("analytics", DEFAULT_CLUSTER_DOMAIN);
		let daemon = leaf_cert(
			&daemon_dns,
			&[
				ExtendedKeyUsagePurpose::ServerAuth,
				ExtendedKeyUsagePurpose::ClientAuth,
			],
		);
		let monitor = leaf_cert(&monitor_dns, &[ExtendedKeyUsagePurpose::ClientAuth]);
		let client = leaf_cert(&client_dns, &[ExtendedKeyUsagePurpose::ClientAuth]);
		let pki = Self {
			ca_cert: dir.join("ca.pem"),
			daemon_cert: dir.join("daemon.pem"),
			daemon_key: dir.join("daemon.key"),
			monitor_cert: dir.join("monitor.pem"),
			monitor_key: dir.join("monitor.key"),
			client_cert: dir.join("client.pem"),
			client_key: dir.join("client.key"),
			dir,
		};
		std::fs::write(&pki.ca_cert, ca.serialize_pem().expect("CA PEM")).expect("write CA");
		write_signed_cert(&pki.daemon_cert, &pki.daemon_key, &daemon, &ca);
		write_signed_cert(&pki.monitor_cert, &pki.monitor_key, &monitor, &ca);
		write_signed_cert(&pki.client_cert, &pki.client_key, &client, &ca);
		pki
	}
}

fn observability_config(worker_id: WorkerId, pki: &TestPki) -> ToronBootstrapConfig {
	ToronBootstrapConfig {
		node: ToronNodeConfig {
			worker_id,
			tenant_id: TenantId(1),
			drain_timeout: Duration::from_secs(1),
		},
		worker: ToronWorkerConfig {
			task_listener_bind: "127.0.0.1:0".parse().expect("worker bind"),
			advertised_endpoint: None,
			memory_budget_bytes: 1024 * 1024,
		},
		tenant_budgets: TenantBudgetPolicy::omitted_config(ByteCount::new(1024 * 1024)),
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
			descriptor_bootstrap_path: None,
		},
		sql: Some(SqlConfig {
			transport: SqlTransport::Quic,
			listener_bind: "127.0.0.1:0".parse().expect("sql bind"),
			statement_timeout: Duration::from_secs(1),
		}),
		coordinator: CoordinatorConfig {
			include_self_worker: true,
			static_workers: Vec::new(),
		},
		tls: Some(TlsConfig {
			listener_cert: pki.daemon_cert.clone(),
			listener_key: pki.daemon_key.clone(),
			peer_ca: pki.ca_cert.clone(),
			cluster_domain: DEFAULT_CLUSTER_DOMAIN.to_string(),
		}),
		observability: ObservabilityConfig {
			level: "info".to_string(),
			format: TracingFormat::Pretty,
			listeners: Some(ObservabilityListeners {
				health: "127.0.0.1:0".parse().expect("health bind"),
				ready: "127.0.0.1:0".parse().expect("ready bind"),
				metrics: "127.0.0.1:0".parse().expect("metrics bind"),
			}),
		},
	}
}

async fn plain_http_request(endpoint: SocketAddr, request: &str) -> std::io::Result<String> {
	let mut stream = TcpStream::connect(connectable_loopback_addr(endpoint)).await?;
	stream.write_all(request.as_bytes()).await?;
	stream.flush().await?;
	let mut bytes = Vec::new();
	stream.read_to_end(&mut bytes).await?;
	String::from_utf8(bytes).map_err(|error| Error::new(ErrorKind::InvalidData, error))
}

async fn mtls_http_request(
	endpoint: SocketAddr,
	server_name: &str,
	cert: &Path,
	key: &Path,
	ca: &Path,
	request: &str,
) -> std::io::Result<String> {
	let connector = tls_connector(cert, key, ca)?;
	let stream = TcpStream::connect(connectable_loopback_addr(endpoint)).await?;
	let server_name = ServerName::try_from(server_name.to_string())
		.map_err(|error| Error::new(ErrorKind::InvalidInput, error))?;
	let mut stream = connector
		.connect(server_name, stream)
		.await
		.map_err(|error| Error::new(ErrorKind::PermissionDenied, error))?;
	stream.write_all(request.as_bytes()).await?;
	stream.flush().await?;
	let mut bytes = Vec::new();
	read_to_end_allowing_unannounced_tls_close(&mut stream, &mut bytes).await?;
	String::from_utf8(bytes).map_err(|error| Error::new(ErrorKind::InvalidData, error))
}

async fn read_to_end_allowing_unannounced_tls_close(
	stream: &mut tokio_rustls::client::TlsStream<TcpStream>,
	bytes: &mut Vec<u8>,
) -> std::io::Result<()> {
	let mut chunk = [0u8; 1024];
	loop {
		match stream.read(&mut chunk).await {
			Ok(0) => return Ok(()),
			Ok(read) => bytes.extend_from_slice(&chunk[..read]),
			Err(error) if error.kind() == ErrorKind::UnexpectedEof => return Ok(()),
			Err(error) => return Err(error),
		}
	}
}

async fn assert_mtls_request_rejected(
	endpoint: SocketAddr,
	server_name: &str,
	cert: &Path,
	key: &Path,
	ca: &Path,
	request: &str,
) {
	let result = tokio::time::timeout(
		Duration::from_secs(3),
		mtls_http_request(endpoint, server_name, cert, key, ca, request),
	)
	.await;
	match result {
		Err(_) | Ok(Err(_)) => {}
		Ok(Ok(response)) => assert!(
			!response.starts_with("HTTP/1.1 200 OK"),
			"request unexpectedly succeeded: {response}"
		),
	}
}

fn tls_connector(cert: &Path, key: &Path, ca: &Path) -> std::io::Result<TlsConnector> {
	let _ = rustls::crypto::ring::default_provider().install_default();
	let cert_chain = CertificateDer::pem_file_iter(cert)
		.map_err(|error| Error::new(ErrorKind::InvalidData, error))?
		.collect::<Result<Vec<_>, _>>()
		.map_err(|error| Error::new(ErrorKind::InvalidData, error))?;
	let key = PrivatePkcs8KeyDer::from_pem_file(key)
		.map(PrivateKeyDer::from)
		.map_err(|error| Error::new(ErrorKind::InvalidData, error))?;
	let mut roots = RootCertStore::empty();
	for cert in CertificateDer::pem_file_iter(ca)
		.map_err(|error| Error::new(ErrorKind::InvalidData, error))?
	{
		roots
			.add(cert.map_err(|error| Error::new(ErrorKind::InvalidData, error))?)
			.map_err(|error| Error::new(ErrorKind::InvalidData, error))?;
	}
	let mut config = ClientConfig::builder()
		.with_root_certificates(roots)
		.with_client_auth_cert(cert_chain, key)
		.map_err(|error| Error::new(ErrorKind::InvalidData, error))?;
	config.alpn_protocols = vec![b"http/1.1".to_vec()];
	Ok(TlsConnector::from(Arc::new(config)))
}

fn response_body(response: &str) -> &str {
	response
		.split("\r\n\r\n")
		.nth(1)
		.expect("HTTP response body")
}

fn assert_fields_in_order(body: &str, fields: &[&str]) {
	let mut cursor = 0;
	for field in fields {
		let offset = body[cursor..]
			.find(field)
			.unwrap_or_else(|| panic!("missing field {field} in {body}"));
		cursor += offset + field.len();
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
		.push(DnType::CommonName, "toron-observability-test-ca");
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

fn minimal_toml_without_observability() -> String {
	"
[node]
worker_id = \"00000000-0000-0000-0000-000000000045\"
tenant_id = 1

[worker]
task_listener_bind = \"127.0.0.1:0\"

[storage.wendelon]
bootstrap_peers = [{ node_id = 1, address = \"127.0.0.1:7001\" }]
client_bind = \"127.0.0.1:0\"
"
	.to_string()
}

fn toml_with_observability_listeners(
	pki: &TestPki,
	worker_id: WorkerId,
	health: &str,
	ready: &str,
	metrics: &str,
	include_tls: bool,
) -> String {
	let tls = if include_tls {
		format!(
			"
[tls]
listener_cert = \"{}\"
listener_key = \"{}\"
peer_ca = \"{}\"
cluster_domain = \"{}\"
",
			pki.daemon_cert.display(),
			pki.daemon_key.display(),
			pki.ca_cert.display(),
			DEFAULT_CLUSTER_DOMAIN
		)
	} else {
		String::new()
	};
	format!(
		"
[node]
worker_id = \"{}\"
tenant_id = 1

[worker]
task_listener_bind = \"127.0.0.1:0\"

[storage.wendelon]
bootstrap_peers = [{{ node_id = 1, address = \"127.0.0.1:7001\" }}]
client_bind = \"127.0.0.1:0\"

[observability]
level = \"info\"
format = \"pretty\"

[observability.listeners]
health = \"{}\"
ready = \"{}\"
metrics = \"{}\"
{}
",
		worker_id.get(),
		health,
		ready,
		metrics,
		tls
	)
}

fn fresh_dir(tag: &str) -> PathBuf {
	let nanos = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.expect("system time after epoch")
		.as_nanos();
	let dir = std::env::temp_dir().join(format!("{tag}-{}-{nanos}", std::process::id()));
	std::fs::create_dir_all(&dir).expect("create temp dir");
	dir
}
