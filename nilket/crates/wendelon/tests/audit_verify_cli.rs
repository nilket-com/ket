use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use uuid::Uuid;
use wendelon::audit::{
	AuditAuthority, AuditEvent, AuditOperation, AuditRootSigner, AuditSink, LocalAuditSink,
	emit_signed_chain_root,
};
use wendelon::schema::NodeId;

fn fresh_dir(tag: &str) -> PathBuf {
	let dir = std::env::temp_dir().join(format!(
		"wendelon-audit-verify-cli-{tag}-{}-{}",
		std::process::id(),
		Uuid::new_v4()
	));
	std::fs::create_dir_all(&dir).unwrap();
	dir
}

fn node(id: u32) -> NodeId {
	NodeId { id, _reserved: 0 }
}

#[test]
fn audit_verify_cli_reports_clean_chain() {
	let dir = fresh_dir("clean");
	let path = dir.join("audit.ndjson");
	let sink: Arc<dyn AuditSink> = Arc::new(LocalAuditSink::open(&path).unwrap());
	sink.emit(AuditEvent::new(
		AuditAuthority::NodeLocal,
		AuditOperation::CryptoStartup,
		node(21),
		"selected_cipher=aes256-gcm",
	))
	.unwrap();
	sink.emit(
		AuditEvent::new(
			AuditAuthority::RaftLog,
			AuditOperation::ControlRingEpochAdvanced,
			node(21),
			"advanced epoch",
		)
		.with_raft_log_index(99),
	)
	.unwrap();
	let signer = AuditRootSigner::from_seed([21; 32]);
	emit_signed_chain_root(&*sink, node(21), &signer).unwrap();

	let output = Command::new(env!("CARGO_BIN_EXE_wendelon"))
		.args(["audit", "verify", path.to_str().unwrap()])
		.output()
		.unwrap();
	assert!(output.status.success(), "{output:?}");
	let stdout = String::from_utf8(output.stdout).unwrap();
	assert!(stdout.contains("\"signed_roots_verified\": 1"));
	assert!(stdout.contains("\"last_materialized_raft_log_index\": 99"));
	assert!(stdout.contains("\"issues\": []"));
}

#[test]
fn audit_verify_cli_reports_bad_evidence_with_nonzero_exit() {
	let dir = fresh_dir("bad");
	let path = dir.join("audit.ndjson");
	let sink = LocalAuditSink::open(&path).unwrap();
	sink.emit(AuditEvent::new(
		AuditAuthority::NodeLocal,
		AuditOperation::CryptoStartup,
		node(22),
		"selected_cipher=aes256-gcm",
	))
	.unwrap();
	let mut contents = std::fs::read_to_string(&path).unwrap();
	contents.push_str("{not-json\n");
	std::fs::write(&path, contents).unwrap();

	let output = Command::new(env!("CARGO_BIN_EXE_wendelon"))
		.args(["audit", "verify", path.to_str().unwrap()])
		.output()
		.unwrap();
	assert_eq!(output.status.code(), Some(1), "{output:?}");
	let stdout = String::from_utf8(output.stdout).unwrap();
	assert!(stdout.contains("\"code\": \"malformed_json\""));
}
