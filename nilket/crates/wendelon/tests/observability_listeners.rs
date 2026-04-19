#[path = "support/tls_pki.rs"]
mod tls_pki;

use async_trait::async_trait;
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::pki_types::ServerName;
use uuid::Uuid;
use wendelon::audit::{AuditSink, LocalAuditSink};
use wendelon::metrics::observability::{
	self, DrainState, ListenerAddresses, ObservabilityHandles, ReadinessReporter, ReadinessSnapshot,
};
use wendelon::network::{
	ActorTrustRoots, TlsMaterial, canonical_node_dns_name, rustls_client_config_with_alpn,
	rustls_server_config_with_peer_roots,
};

fn fresh_dir(tag: &str) -> std::path::PathBuf {
	let dir = std::env::temp_dir().join(format!(
		"wendelon-observability-listeners-{tag}-{}-{}",
		std::process::id(),
		Uuid::new_v4()
	));
	std::fs::create_dir_all(&dir).unwrap();
	dir
}

#[derive(Clone)]
struct FixedReporter {
	snapshot: ReadinessSnapshot,
}

#[async_trait]
impl ReadinessReporter for FixedReporter {
	async fn snapshot(&self) -> ReadinessSnapshot {
		self.snapshot.clone()
	}
}

#[tokio::test]
async fn listeners_enforce_health_ready_and_metrics_posture() {
	let root = fresh_dir("auth");
	let pki = tls_pki::write_test_pki(&root.join("pki"));
	let audit_path = root.join("audit.ndjson");
	let server_material =
		TlsMaterial::load(&pki.server_cert, &pki.server_key, &pki.ca_cert).unwrap();
	let trust_roots = Arc::new(ActorTrustRoots::load(&pki.ca_cert, &pki.ops_ca_cert).unwrap());
	let audit_sink: Arc<dyn AuditSink> = Arc::new(LocalAuditSink::open(&audit_path).unwrap());
	let acceptor = TlsAcceptor::from(Arc::new(
		rustls_server_config_with_peer_roots(
			&server_material,
			trust_roots.combined_roots(),
			Vec::new(),
		)
		.unwrap(),
	));
	let handles = observability::spawn(
		ListenerAddresses {
			health: "127.0.0.1:0".into(),
			ready: "127.0.0.1:0".into(),
			metrics: "127.0.0.1:0".into(),
		},
		acceptor,
		Arc::new(FixedReporter {
			snapshot: ReadinessSnapshot {
				node_id: 1,
				epoch: 42,
				leader_id: Some(1),
				cluster_members: vec![1, 2, 3],
				drain_state: DrainState::None,
				audit_materialization_lag_entries: 0,
				degraded_reason: None,
			},
		}),
		1,
		tls_pki::CLUSTER_DOMAIN,
		trust_roots,
		audit_sink,
	)
	.await
	.unwrap();

	let health = request_health(handles.health.local_addr).await;
	assert!(
		health.starts_with("HTTP/1.1 200 OK"),
		"unexpected health: {health}"
	);
	assert!(health.ends_with("ok\n"), "unexpected health body: {health}");

	let ready = request_mtls(
		handles.ready.local_addr,
		"/ready",
		&pki.data_client_cert,
		&pki.data_client_key,
		&pki.ca_cert,
	)
	.await;
	assert!(
		ready.starts_with("HTTP/1.1 200 OK"),
		"unexpected ready: {ready}"
	);
	assert!(
		ready.contains("\"node_id\":\"n-01\""),
		"unexpected ready body: {ready}"
	);
	assert!(
		ready.contains("\"cluster_members\":[\"n-01\",\"n-02\",\"n-03\"]"),
		"unexpected ready body: {ready}"
	);

	let metrics = request_mtls(
		handles.metrics.local_addr,
		"/metrics",
		&pki.operator_cert,
		&pki.operator_key,
		&pki.ca_cert,
	)
	.await;
	assert!(
		metrics.starts_with("HTTP/1.1 200 OK"),
		"unexpected metrics: {metrics}"
	);
	assert!(
		metrics.contains("wendelon_metrics_scrapes_total"),
		"unexpected metrics body: {metrics}"
	);

	let denied = request_mtls(
		handles.metrics.local_addr,
		"/metrics",
		&pki.data_client_cert,
		&pki.data_client_key,
		&pki.ca_cert,
	)
	.await;
	assert!(
		denied.is_empty() || !denied.starts_with("HTTP/1.1 200 OK"),
		"data-node certificate unexpectedly scraped metrics: {denied}"
	);
	let audit = read_audit_log(&audit_path);
	assert!(audit.contains("\"event_type\":\"observability.ready\""));
	assert!(audit.contains("\"event_type\":\"observability.metrics\""));

	shutdown(handles).await;
}

#[tokio::test]
async fn observability_rejects_wrong_ca_domains_and_marks_break_glass_audit() {
	let root = fresh_dir("dual-ca");
	let pki = tls_pki::write_test_pki(&root.join("pki"));
	let audit_path = root.join("audit.ndjson");
	let server_material =
		TlsMaterial::load(&pki.server_cert, &pki.server_key, &pki.ca_cert).unwrap();
	let trust_roots = Arc::new(ActorTrustRoots::load(&pki.ca_cert, &pki.ops_ca_cert).unwrap());
	let audit_sink: Arc<dyn AuditSink> = Arc::new(LocalAuditSink::open(&audit_path).unwrap());
	let acceptor = TlsAcceptor::from(Arc::new(
		rustls_server_config_with_peer_roots(
			&server_material,
			trust_roots.combined_roots(),
			Vec::new(),
		)
		.unwrap(),
	));
	let handles = observability::spawn(
		ListenerAddresses {
			health: "127.0.0.1:0".into(),
			ready: "127.0.0.1:0".into(),
			metrics: "127.0.0.1:0".into(),
		},
		acceptor,
		Arc::new(FixedReporter {
			snapshot: ReadinessSnapshot {
				node_id: 1,
				epoch: 42,
				leader_id: Some(1),
				cluster_members: vec![1, 2, 3],
				drain_state: DrainState::None,
				audit_materialization_lag_entries: 0,
				degraded_reason: None,
			},
		}),
		1,
		tls_pki::CLUSTER_DOMAIN,
		trust_roots,
		audit_sink,
	)
	.await
	.unwrap();

	let break_glass = request_mtls(
		handles.metrics.local_addr,
		"/metrics",
		&pki.break_glass_cert,
		&pki.break_glass_key,
		&pki.ca_cert,
	)
	.await;
	assert!(
		break_glass.starts_with("HTTP/1.1 200 OK"),
		"unexpected break-glass metrics response: {break_glass}"
	);

	let operator_wrong_ca = request_mtls(
		handles.metrics.local_addr,
		"/metrics",
		&pki.operator_on_node_ca_cert,
		&pki.operator_on_node_ca_key,
		&pki.ca_cert,
	)
	.await;
	assert!(
		operator_wrong_ca.is_empty() || !operator_wrong_ca.starts_with("HTTP/1.1 200 OK"),
		"wrong-CA operator certificate unexpectedly scraped metrics: {operator_wrong_ca}"
	);

	let data_node_wrong_ca = request_mtls(
		handles.ready.local_addr,
		"/ready",
		&pki.data_node_on_ops_ca_cert,
		&pki.data_node_on_ops_ca_key,
		&pki.ca_cert,
	)
	.await;
	assert!(
		data_node_wrong_ca.is_empty() || !data_node_wrong_ca.starts_with("HTTP/1.1 200 OK"),
		"wrong-CA data-node certificate unexpectedly read /ready: {data_node_wrong_ca}"
	);

	let audit = read_audit_log(&audit_path);
	assert!(audit.contains("\"event_type\":\"observability.metrics\""));
	assert!(audit.contains("\"actor_role\":\"break_glass_admin\""));
	assert!(audit.contains("\"actor_trust_domain\":\"ops\""));
	assert!(audit.contains("\"break_glass\":true"));

	shutdown(handles).await;
}

async fn request_health(addr: std::net::SocketAddr) -> String {
	let mut stream = TcpStream::connect(addr).await.unwrap();
	stream
		.write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
		.await
		.unwrap();
	stream.shutdown().await.unwrap();
	let mut buf = Vec::new();
	stream.read_to_end(&mut buf).await.unwrap();
	String::from_utf8(buf).unwrap()
}

async fn request_mtls(
	addr: std::net::SocketAddr,
	path: &str,
	cert_path: &std::path::Path,
	key_path: &std::path::Path,
	ca_path: &std::path::Path,
) -> String {
	let material = TlsMaterial::load(cert_path, key_path, ca_path).unwrap();
	let config = rustls_client_config_with_alpn(&material, Vec::new()).unwrap();
	let connector = TlsConnector::from(Arc::new(config));
	let tcp = TcpStream::connect(addr).await.unwrap();
	let server_name =
		ServerName::try_from(canonical_node_dns_name(1, tls_pki::CLUSTER_DOMAIN)).unwrap();
	let mut stream = connector.connect(server_name, tcp).await.unwrap();
	let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
	stream.write_all(request.as_bytes()).await.unwrap();
	stream.shutdown().await.unwrap();
	let mut buf = Vec::new();
	match stream.read_to_end(&mut buf).await {
		Ok(_) => {}
		Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {}
		Err(err) => panic!("failed to read HTTPS response: {err}"),
	}
	String::from_utf8(buf).unwrap()
}

fn read_audit_log(path: &Path) -> String {
	std::fs::read_to_string(path).unwrap_or_default()
}

async fn shutdown(handles: ObservabilityHandles) {
	for handle in [&handles.health, &handles.ready, &handles.metrics] {
		handle.task.abort();
	}
	for task in [
		handles.health.task,
		handles.ready.task,
		handles.metrics.task,
	] {
		let _ = task.await;
	}
}
