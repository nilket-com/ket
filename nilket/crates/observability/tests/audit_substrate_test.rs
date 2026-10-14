#![cfg(feature = "audit")]

use observability::audit::{
	ArchiveAuditConfig, AuditArchiveLockMode, AuditAuthority, AuditEvent, AuditFormat, AuditNodeId,
	AuditOutcome, AuditRootSigner, AuditRootTrust, AuditSink, LocalAuditSink, NoopAuditObserver,
	RootTrustStatus, emit_signed_chain_root, replay_audit_chain, verify_local_audit_evidence,
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
	canonical_node_wire: |id| id.to_string(),
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
	#[cfg(not(feature = "fips"))]
	assert_eq!(head.hash_algorithm_tag, "blake3-256");
	#[cfg(feature = "fips")]
	assert_eq!(head.hash_algorithm_tag, "sha256");

	let report = verify_local_audit_evidence(FORMAT, &path, None).unwrap();
	assert!(report.ok(), "{:?}", report.issues);
	assert_eq!(report.root_trust, RootTrustStatus::NoRoots);
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

	let report = verify_local_audit_evidence(FORMAT, &path, None).unwrap();
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

	let trust = AuditRootTrust::new(
		FORMAT,
		[(
			11,
			ed25519_dalek::SigningKey::from_bytes(&[11; 32])
				.verifying_key()
				.to_bytes(),
		)],
	)
	.unwrap();
	let report = verify_local_audit_evidence(FORMAT, &path, Some(&trust)).unwrap();
	assert_eq!(report.root_trust, RootTrustStatus::Anchored);
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

fn anchored_fixture() -> (AuditRootTrust, observability::audit::SignedAuditChainRoot) {
	let signer = AuditRootSigner::from_seed([61; 32]);
	let trust = AuditRootTrust::new(
		FORMAT,
		[(
			11,
			ed25519_dalek::SigningKey::from_bytes(&[61; 32])
				.verifying_key()
				.to_bytes(),
		)],
	)
	.unwrap();
	let head = observability::audit::AuditChainHead {
		sequence_number: 1,
		record_hash: "00".repeat(32),
		hash_algorithm_tag: "test-hash".into(),
	};
	(trust, signer.sign_chain_head(FORMAT, &node(11), &head))
}
#[test]
fn audit_root_trust_refuses_duplicate_weak_noncanonical_and_torsion_keys() {
	use observability::audit::AuditTrustError as E;
	let key = ed25519_dalek::SigningKey::from_bytes(&[61; 32])
		.verifying_key()
		.to_bytes();
	assert!(matches!(
		AuditRootTrust::new(FORMAT, [(11, key), (11, key)]),
		Err(E::DuplicateNode { node: 11 })
	));
	assert!(matches!(
		AuditRootTrust::new(FORMAT, [(0, key)]),
		Err(E::InvalidNode)
	));
	let mut identity = [0; 32];
	identity[0] = 1;
	let mut noncanonical = [255; 32];
	noncanonical[0] = 238;
	noncanonical[31] = 127;
	let torsion = curve25519_dalek::constants::ED25519_BASEPOINT_POINT
		+ curve25519_dalek::constants::EIGHT_TORSION[1];
	for key in [
		identity,
		[0; 32],
		noncanonical,
		torsion.compress().to_bytes(),
	] {
		assert!(matches!(
			AuditRootTrust::new(FORMAT, [(11, key)]),
			Err(E::InvalidTrustedKey { node: 11 })
		));
	}
}
#[test]
fn audit_root_verifier_is_anchored_strict_and_has_frozen_first_error_order() {
	use observability::audit::{AuditRootVerifyError as E, verify_signed_chain_root as verify};
	let (trust, root) = anchored_fixture();
	verify(FORMAT, &root, &trust).unwrap();
	let empty = AuditRootTrust::new(FORMAT, []).unwrap();
	let mut bad = root.clone();
	bad.signature_algorithm_tag = "SECRET".into();
	bad.node_id = "SECRET".into();
	bad.public_key = "SECRET".into();
	assert_eq!(verify(FORMAT, &bad, &empty), Err(E::Algorithm));
	bad.signature_algorithm_tag = "ed25519".into();
	assert_eq!(verify(FORMAT, &bad, &empty), Err(E::NodeParse));
	bad.node_id = "011".into();
	assert_eq!(verify(FORMAT, &bad, &empty), Err(E::NonCanonicalNode));
	bad.node_id = "11".into();
	assert_eq!(
		verify(FORMAT, &bad, &empty),
		Err(E::UntrustedNode { node: 11 })
	);
	assert_eq!(verify(FORMAT, &bad, &trust), Err(E::MalformedKey));
	bad.public_key = AuditRootSigner::from_seed([62; 32]).verifying_key_hex();
	assert_eq!(
		verify(FORMAT, &bad, &trust),
		Err(E::KeyMismatch { node: 11 })
	);
	bad.public_key = root.public_key.clone();
	bad.signature = "SECRET".into();
	assert_eq!(verify(FORMAT, &bad, &trust), Err(E::MalformedSignature));
	bad.signature = "00".repeat(64);
	assert_eq!(verify(FORMAT, &bad, &trust), Err(E::Signature));
	// S >= group order is a noncanonical scalar; strict verification must refuse it.
	bad.signature = format!(
		"{}{}",
		&root.signature[..64],
		"edd3f55c1a631258d69cf7a2def9de1400000000000000000000000000000010"
	);
	assert_eq!(verify(FORMAT, &bad, &trust), Err(E::Signature));
	let other = AuditFormat {
		chain_root_tag: b"other-namespace",
		..FORMAT
	};
	assert_eq!(
		verify(other, &root, &trust),
		Err(E::UntrustedNode { node: 11 })
	);
	assert!(!E::MalformedSignature.to_string().contains("SECRET"));
}
#[test]
fn audit_root_reports_unanchored_unknown_mismatch_and_mixed_roots() {
	let path = fresh_audit_path("trust-report");
	let sink = LocalAuditSink::open(&path, FORMAT).unwrap();
	sink.emit(AuditEvent::new(
		AuditAuthority::NodeLocal,
		"startup",
		node(11),
		"ready",
	))
	.unwrap();
	let signer = AuditRootSigner::from_seed([61; 32]);
	emit_signed_chain_root(&sink, FORMAT, node(11), &signer).unwrap();
	let (trust, _) = anchored_fixture();
	let no_map = verify_local_audit_evidence(FORMAT, &path, None).unwrap();
	assert_eq!(no_map.root_trust, RootTrustStatus::Unanchored);
	assert_eq!(no_map.signed_roots_verified, 0);
	assert!(!no_map.ok());
	assert!(
		no_map
			.issues
			.iter()
			.any(|e| e.code == "audit_root_unanchored")
	);
	let empty = AuditRootTrust::new(FORMAT, []).unwrap();
	let unknown = verify_local_audit_evidence(FORMAT, &path, Some(&empty)).unwrap();
	assert_eq!(unknown.signed_roots_verified, 0);
	assert!(
		unknown
			.issues
			.iter()
			.any(|e| e.code == "audit_root_untrusted_node")
	);
	let wrong = AuditRootTrust::new(
		FORMAT,
		[(
			11,
			ed25519_dalek::SigningKey::from_bytes(&[62; 32])
				.verifying_key()
				.to_bytes(),
		)],
	)
	.unwrap();
	let mismatch = verify_local_audit_evidence(FORMAT, &path, Some(&wrong)).unwrap();
	assert!(
		mismatch
			.issues
			.iter()
			.any(|e| e.code == "audit_root_key_mismatch")
	);
	// A self-signed rogue second root is still an ordinary valid chain record.
	emit_signed_chain_root(
		&sink,
		FORMAT,
		node(11),
		&AuditRootSigner::from_seed([62; 32]),
	)
	.unwrap();
	let mixed = verify_local_audit_evidence(FORMAT, &path, Some(&trust)).unwrap();
	assert_eq!(mixed.signed_root_records, 2);
	assert_eq!(mixed.signed_roots_verified, 1);
	assert_eq!(mixed.root_trust, RootTrustStatus::Unanchored);
	assert!(!mixed.ok());
}
#[test]
fn audit_root_whole_report_checks_metadata_malformed_and_strict_refusals() {
	use observability::audit::SignedAuditChainRoot;
	for (tag, mutate, code) in [
		("noncanonical", 0, "audit_root_noncanonical_node"),
		("malformed-key", 1, "audit_root_malformed"),
		("bad-signature", 2, "audit_root_bad_signature"),
		("metadata", 3, "signed_root_sequence_mismatch"),
		("malformed-json", 4, "malformed_signed_root"),
	] {
		let path = fresh_audit_path(tag);
		let sink = LocalAuditSink::open(&path, FORMAT).unwrap();
		sink.emit(AuditEvent::new(
			AuditAuthority::NodeLocal,
			"startup",
			node(11),
			"ready",
		))
		.unwrap();
		let (trust, _) = anchored_fixture();
		let mut root: SignedAuditChainRoot = AuditRootSigner::from_seed([61; 32]).sign_chain_head(
			FORMAT,
			&node(11),
			&sink.chain_head().unwrap().unwrap(),
		);
		match mutate {
			0 => root.node_id = "011".into(),
			1 => root.public_key = "SECRET".into(),
			2 => root.signature = "00".repeat(64),
			3 => {
				let mut head = sink.chain_head().unwrap().unwrap();
				head.sequence_number = 99;
				root =
					AuditRootSigner::from_seed([61; 32]).sign_chain_head(FORMAT, &node(11), &head);
			}
			_ => {}
		}
		let value = if mutate == 4 {
			serde_json::json!({"secret":"SECRET"})
		} else {
			serde_json::to_value(root).unwrap()
		};
		sink.emit(
			AuditEvent::new(
				AuditAuthority::NodeLocal,
				FORMAT.chain_root_operation,
				node(11),
				"root",
			)
			.with_parameter("chain_head", value),
		)
		.unwrap();
		let report = verify_local_audit_evidence(FORMAT, &path, Some(&trust)).unwrap();
		assert_eq!(report.signed_roots_verified, 0);
		assert_eq!(report.root_trust, RootTrustStatus::Unanchored);
		assert!(
			report.issues.iter().any(|e| e.code == code),
			"{:?}",
			report.issues
		);
		assert!(report.issues.iter().all(|e| !e.message.contains("SECRET")));
	}
}
