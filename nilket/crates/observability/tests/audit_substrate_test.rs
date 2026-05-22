use observability::audit::{
	ArchiveAuditConfig, AuditArchiveLockMode, AuditAuthority, AuditEvent, AuditFormat, AuditNodeId,
	AuditOutcome, AuditRootSigner, AuditSink, LocalAuditSink, NoopAuditObserver,
	emit_signed_chain_root, replay_audit_chain, verify_local_audit_evidence,
};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const FORMAT: AuditFormat = AuditFormat {
	schema_version: 4,
	genesis_tag: b"test-audit-genesis-v1",
	chain_root_tag: b"test-audit-chain-root-v1",
	root_signature_tag: "ed25519",
	chain_root_operation: "audit.chain_root",
	sink_failure_operation: "audit.sink_failure",
	syslog_app_name: "test-ring",
	syslog_enterprise_id: "test@47450",
	default_kafka_client_id: "test-audit",
	parse_node_numeric,
};

fn parse_node_numeric(raw: &str) -> std::io::Result<u32> {
	raw.parse::<u32>().map_err(|err| {
		std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			format!("invalid test node id: {err}"),
		)
	})
}

fn fresh_audit_path(tag: &str) -> std::path::PathBuf {
	let unique = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap()
		.as_nanos();
	let dir = std::env::temp_dir().join(format!(
		"observability-audit-{tag}-{}-{unique}",
		std::process::id()
	));
	std::fs::create_dir_all(&dir).unwrap();
	dir.join("audit.ndjson")
}

fn node(id: u32) -> AuditNodeId {
	AuditNodeId::new(id.to_string(), id)
}

#[test]
fn local_audit_sink_replays_and_verifies_chain() {
	let path = fresh_audit_path("local-chain");
	let sink = LocalAuditSink::open(&path, FORMAT).unwrap();
	sink.emit(
		AuditEvent::new(
			AuditAuthority::NodeLocal,
			"crypto.startup",
			node(7),
			"selected cipher",
		)
		.with_parameter("cipher", "aes256-gcm"),
	)
	.unwrap();
	sink.emit(
		AuditEvent::new(
			AuditAuthority::RaftLog,
			"tenant.created",
			node(7),
			"tenant created",
		)
		.with_raft_log_index(42),
	)
	.unwrap();

	let head = replay_audit_chain(FORMAT, &path).unwrap().unwrap();
	assert_eq!(head.sequence_number, 2);
	assert_eq!(head.hash_algorithm_tag, "blake3-256");

	let report = verify_local_audit_evidence(FORMAT, &path).unwrap();
	assert!(report.ok(), "{:?}", report.issues);
	assert_eq!(report.node_local_records, 1);
	assert_eq!(report.raft_log_records, 1);
	assert_eq!(report.last_materialized_raft_log_index, Some(42));
}

#[test]
fn verifier_reports_tampered_record_hash() {
	let path = fresh_audit_path("tamper");
	let sink = LocalAuditSink::open(&path, FORMAT).unwrap();
	sink.emit(
		AuditEvent::new(
			AuditAuthority::NodeLocal,
			"observability.metrics",
			node(9),
			"served metrics",
		)
		.with_outcome(AuditOutcome::Allowed),
	)
	.unwrap();
	let text = std::fs::read_to_string(&path).unwrap();
	std::fs::write(&path, text.replace("served metrics", "tampered metrics")).unwrap();

	let report = verify_local_audit_evidence(FORMAT, &path).unwrap();
	assert!(!report.ok());
	assert!(
		report
			.issues
			.iter()
			.any(|issue| issue.code == "record_hash_mismatch")
	);
}

#[test]
fn signed_chain_root_verifies_against_previous_head() {
	let path = fresh_audit_path("signed-root");
	let sink = LocalAuditSink::open(&path, FORMAT).unwrap();
	sink.emit(AuditEvent::new(
		AuditAuthority::NodeLocal,
		"crypto.startup",
		node(11),
		"selected cipher",
	))
	.unwrap();
	let signer = AuditRootSigner::from_seed([11; 32]);
	emit_signed_chain_root(&sink, FORMAT, node(11), &signer).unwrap();

	let report = verify_local_audit_evidence(FORMAT, &path).unwrap();
	assert!(report.ok(), "{:?}", report.issues);
	assert_eq!(report.signed_root_records, 1);
	assert_eq!(report.signed_roots_verified, 1);
}

#[test]
fn archive_projection_rolls_segments_without_replacing_local_authority() {
	let path = fresh_audit_path("archive");
	let archive_dir = path.parent().unwrap().join("archive");
	let sink = LocalAuditSink::open_with_archive(
		&path,
		Some(ArchiveAuditConfig {
			directory: archive_dir.clone(),
			max_segment_bytes: 500,
			lock_mode: AuditArchiveLockMode::Governance,
			retain_days: Some(7),
		}),
		FORMAT,
		Arc::new(NoopAuditObserver),
	)
	.unwrap();
	for idx in 0..8 {
		sink.emit(AuditEvent::new(
			AuditAuthority::NodeLocal,
			"observability.metrics",
			node(33),
			format!("served metrics {idx} {}", "x".repeat(120)),
		))
		.unwrap();
	}

	let disposition = sink.evidence_disposition();
	assert!(disposition.authoritative_local_chain);
	assert!(disposition.archived_local_chain);
	assert_eq!(
		disposition.archive_lock_mode,
		Some(AuditArchiveLockMode::Governance)
	);
	assert!(std::fs::read_dir(&archive_dir).unwrap().any(|entry| {
		entry
			.unwrap()
			.path()
			.extension()
			.and_then(|ext| ext.to_str())
			== Some("ndjson")
	}));
}
