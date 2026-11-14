#[path = "../src/test_support/test_dir.rs"]
mod test_dir;
#[path = "support/tls_pki.rs"]
mod tls_pki;

use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use tokio_rustls::TlsAcceptor;
use wendelon::audit::{AuditSink, LocalAuditSink};
use wendelon::consensus::RetentionStatus;
use wendelon::metrics::observability::{
	self, DrainState, ListenerAddresses, ObservabilityHandles, ReadinessReporter, ReadinessSnapshot,
};
use wendelon::network::{ActorTrustRoots, TlsMaterial, rustls_server_config_with_peer_roots};

fn fresh_dir(tag: &str) -> test_dir::TestDir {
	test_dir::TestDir::new(&format!("wendelon-inspect-cli-{tag}"))
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

async fn spawn_ready(
	snapshot: ReadinessSnapshot,
) -> (
	test_dir::TestDir,
	ObservabilityHandles,
	tls_pki::TestPkiPaths,
	PathBuf,
) {
	let root = fresh_dir("round-trip");
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
		Arc::new(FixedReporter { snapshot }),
		1,
		tls_pki::CLUSTER_DOMAIN,
		trust_roots,
		audit_sink,
		None,
	)
	.await
	.unwrap();
	(root, handles, pki, audit_path)
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

fn ready_snapshot() -> ReadinessSnapshot {
	ReadinessSnapshot {
		node_id: 1,
		epoch: 7,
		leader_id: Some(1),
		cluster_members: vec![1],
		drain_state: DrainState::None,
		audit_materialization_lag_entries: 0,
		degraded_reason: None,
		fips_mode: cfg!(feature = "fips"),
		fips_self_test_last_ts: cfg!(feature = "fips").then_some(1_710_000_000),
		control_retention: RetentionStatus::compacting(),
		consensus_timing: wendelon::consensus::ConsensusTimingStatus::default_profile(),
	}
}

fn inspect_output(
	endpoint: std::net::SocketAddr,
	pki: &tls_pki::TestPkiPaths,
	cert: &Path,
	key: &Path,
	extra: &[&str],
) -> std::process::Output {
	let mut command = Command::new(env!("CARGO_BIN_EXE_wendelon"));
	command.args([
		"inspect",
		"--endpoint",
		&endpoint.to_string(),
		"--server-name",
		"node-1.cluster.local",
		"--cert",
		cert.to_str().unwrap(),
		"--key",
		key.to_str().unwrap(),
		"--ca",
		pki.ca_cert.to_str().unwrap(),
	]);
	command.args(extra);
	command.output().unwrap()
}

fn assert_authorization_denied_audit(audit_path: &Path) {
	let audit = std::fs::read_to_string(audit_path).unwrap();
	for expected in [
		"\"event_type\":\"identity.authorization_denied\"",
		"\"stage\":\"authorization\"",
		"\"required_permission\":\"observability_ready\"",
		"\"actor_role\":\"query_session\"",
	] {
		assert!(audit.contains(expected), "missing {expected} in {audit}");
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wendelon_inspect_actor_authorization_round_trip() {
	let (_root, handles, pki, audit_path) = spawn_ready(ready_snapshot()).await;
	let success = inspect_output(
		handles.ready.local_addr,
		&pki,
		&pki.operator_cert,
		&pki.operator_key,
		&[],
	);
	assert_eq!(success.status.code(), Some(0), "{success:?}");
	let stdout = String::from_utf8(success.stdout).unwrap();
	assert!(stdout.contains("node_id: \"n-01\""), "{stdout}");
	assert!(stdout.contains("degraded_reason: null"), "{stdout}");

	let denied = inspect_output(
		handles.ready.local_addr,
		&pki,
		&pki.query_session_cert,
		&pki.query_session_key,
		&[],
	);
	assert_eq!(denied.status.code(), Some(2), "{denied:?}");
	assert!(denied.stdout.is_empty(), "{denied:?}");
	let stderr = String::from_utf8(denied.stderr).unwrap();
	assert!(
		stderr.contains("wendelon inspect: failed to"),
		"unexpected stderr: {stderr}"
	);
	assert_authorization_denied_audit(&audit_path);

	shutdown(handles).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wendelon_inspect_json_mode_preserves_ready_body() {
	let (_root, handles, pki, _audit_path) = spawn_ready(ready_snapshot()).await;
	let output = inspect_output(
		handles.ready.local_addr,
		&pki,
		&pki.operator_cert,
		&pki.operator_key,
		&["--json"],
	);
	assert_eq!(output.status.code(), Some(0), "{output:?}");
	let stdout = String::from_utf8(output.stdout).unwrap();
	assert!(stdout.ends_with('\n'), "{stdout:?}");
	assert!(stdout.contains("\"node_id\":\"n-01\""), "{stdout}");
	assert!(
		stdout.contains("\"audit_materialization_lag_entries\":0"),
		"{stdout}"
	);
	assert!(stdout.contains("\"degraded_reason\":null"), "{stdout}");
	// The node's effective consensus timing profile is part of the forwarded body.
	let body: serde_json::Value = serde_json::from_str(&stdout).unwrap();
	assert_eq!(
		body["consensus_timing"],
		serde_json::json!({
			"base_millis": 50, "heartbeat_millis": 50, "election_min_millis": 150,
			"election_max_millis": 300, "snapshot_chunk_millis": 200
		})
	);

	shutdown(handles).await;
}
