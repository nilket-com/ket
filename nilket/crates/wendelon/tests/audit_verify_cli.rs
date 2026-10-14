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

	let trust_path = dir.join("trust.json");
	std::fs::write(
		&trust_path,
		format!(
			r#"{{"version":1,"keys":{{"n-21":"{}"}}}}"#,
			signer.verifying_key_hex()
		),
	)
	.unwrap();
	let unanchored = Command::new(env!("CARGO_BIN_EXE_wendelon"))
		.args(["audit", "verify", path.to_str().unwrap()])
		.output()
		.unwrap();
	assert_eq!(unanchored.status.code(), Some(1));
	let text = String::from_utf8(unanchored.stdout).unwrap();
	assert!(text.contains("audit_root_unanchored"));
	assert!(text.contains("\"signed_roots_verified\": 0"));
	assert!(text.contains("\"root_trust\": \"Unanchored\""));

	let output = Command::new(env!("CARGO_BIN_EXE_wendelon"))
		.args([
			"audit",
			"verify",
			path.to_str().unwrap(),
			"--trusted-audit-roots",
			trust_path.to_str().unwrap(),
		])
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

#[test]
fn audit_verify_cli_refuses_unknown_node_and_wrong_trusted_key() {
	let dir = fresh_dir("wrong-trust");
	let path = dir.join("audit.ndjson");
	let sink = LocalAuditSink::open(&path).unwrap();
	sink.emit(AuditEvent::new(
		AuditAuthority::NodeLocal,
		AuditOperation::CryptoStartup,
		node(21),
		"ready",
	))
	.unwrap();
	let signer = AuditRootSigner::from_seed([21; 32]);
	emit_signed_chain_root(&sink, node(21), &signer).unwrap();
	let trust = dir.join("trust.json");
	for (wire, key, code) in [
		(
			"n-22",
			signer.verifying_key_hex(),
			"audit_root_untrusted_node",
		),
		(
			"n-21",
			AuditRootSigner::from_seed([22; 32]).verifying_key_hex(),
			"audit_root_key_mismatch",
		),
	] {
		std::fs::write(
			&trust,
			format!(r#"{{"version":1,"keys":{{"{wire}":"{key}"}}}}"#),
		)
		.unwrap();
		let output = Command::new(env!("CARGO_BIN_EXE_wendelon"))
			.args([
				"audit",
				"verify",
				path.to_str().unwrap(),
				"--trusted-audit-roots",
				trust.to_str().unwrap(),
			])
			.output()
			.unwrap();
		assert_eq!(output.status.code(), Some(1), "{output:?}");
		let text = String::from_utf8(output.stdout).unwrap();
		assert!(text.contains(code));
		assert!(text.contains("\"signed_roots_verified\": 0"));
		assert!(!text.contains(&key));
		assert!(!String::from_utf8(output.stderr).unwrap().contains(&key));
	}
}
#[test]
fn audit_verify_cli_sanitizes_and_refuses_malformed_trust_inputs() {
	let dir = fresh_dir("bad-trust");
	let path = dir.join("audit.ndjson");
	std::fs::write(&path, "").unwrap();
	let trust = dir.join("trust.json");
	let key = AuditRootSigner::from_seed([21; 32]).verifying_key_hex();
	let good = format!(r#"{{"version":1,"keys":{{"n-21":"{key}"}}}}"#);
	for text in [
		good.replace("\"version\":1", "\"version\":2"),
		good.replace("\"version\":1", "\"version\":1,\"version\":1"),
		good.replace("\"version\":1", "\"version\":1,\"secret\":\"SENSITIVE\""),
		good.replace(
			&format!("\"n-21\":\"{key}\""),
			&format!("\"n-21\":\"{key}\",\"n-21\":\"{key}\""),
		),
		good.replace("n-21", "n-021"),
		good.replace(&key, &key.to_uppercase()),
		good.replace(&key, "SENSITIVE"),
		good.replace(&key, &"00".repeat(32)),
		"SENSITIVE".into(),
		"SENSITIVE".repeat(131073),
	] {
		std::fs::write(&trust, &text).unwrap();
		let output = Command::new(env!("CARGO_BIN_EXE_wendelon"))
			.args([
				"audit",
				"verify",
				path.to_str().unwrap(),
				"--trusted-audit-roots",
				trust.to_str().unwrap(),
			])
			.output()
			.unwrap();
		assert_eq!(output.status.code(), Some(2), "{output:?}");
		assert!(output.stdout.is_empty());
		let err = String::from_utf8(output.stderr).unwrap();
		assert!(!err.contains(&key));
		assert!(!err.contains("SENSITIVE"));
	}
}
