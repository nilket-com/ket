use super::*;
use IdentityKeyError as E;
use IdentityKeyRole::{Audit, Lease, Transfer};

fn config() -> NodeBootstrapConfig {
	let mut cfg = NodeBootstrapConfig::dev(7, std::env::temp_dir(), "127.0.0.1:0".into());
	cfg.lease_signing_secret = [41; 32];
	cfg.transfer_signing_secret = [42; 32];
	cfg.audit_root_signing_secret = [46; 32];
	cfg.trusted_audit_root_keys.insert(
		7,
		audit_root_verifying_key_bytes(cfg.audit_root_signing_secret),
	);
	cfg.trusted_lease_signers
		.insert(7, lease_verifying_key_bytes(cfg.lease_signing_secret));
	cfg.trusted_transfer_signers
		.insert(7, transfer_verifying_key_bytes(cfg.transfer_signing_secret));
	cfg.allow_dev_keys = false;
	cfg
}
fn reason(cfg: &NodeBootstrapConfig) -> IdentityKeyError {
	let err = validate_identity_keys(cfg).unwrap_err();
	assert_eq!(err.kind(), ErrorKind::InvalidInput);
	*err.get_ref()
		.unwrap()
		.downcast_ref::<IdentityKeyError>()
		.unwrap()
}

#[test]
fn identity_distinct_keys_pass_and_dev_constructor_explicitly_allows_dev() {
	validate_identity_keys(&config()).unwrap();
	let cfg = NodeBootstrapConfig::dev(7, std::env::temp_dir(), "127.0.0.1:0".into());
	assert!(cfg.allow_dev_keys);
	validate_identity_keys(&cfg).unwrap();
}

#[test]
fn identity_missing_local_entries_precede_matches_lease_then_transfer() {
	let mut cfg = config();
	cfg.trusted_lease_signers.clear();
	cfg.trusted_transfer_signers.clear();
	assert_eq!(
		reason(&cfg),
		E::MissingLocal {
			role: Lease,
			node: 7
		}
	);
	cfg.trusted_lease_signers.insert(7, [0; 32]);
	assert_eq!(
		reason(&cfg),
		E::MissingLocal {
			role: Transfer,
			node: 7
		}
	);
}

#[test]
fn identity_local_mismatches_precede_reuse_lease_then_transfer() {
	let mut cfg = config();
	cfg.trusted_lease_signers.insert(7, [0; 32]);
	cfg.trusted_transfer_signers.insert(7, [0; 32]);
	cfg.transfer_signing_secret = cfg.lease_signing_secret;
	assert_eq!(
		reason(&cfg),
		E::LocalKeyMismatch {
			role: Lease,
			node: 7
		}
	);
	cfg.trusted_lease_signers
		.insert(7, lease_verifying_key_bytes(cfg.lease_signing_secret));
	assert_eq!(
		reason(&cfg),
		E::LocalKeyMismatch {
			role: Transfer,
			node: 7
		}
	);
}

#[test]
fn identity_same_secret_precedes_public_role_and_identity_reuse() {
	let mut cfg = config();
	cfg.transfer_signing_secret = cfg.lease_signing_secret;
	let key = lease_verifying_key_bytes(cfg.lease_signing_secret);
	cfg.trusted_transfer_signers.insert(7, key);
	cfg.trusted_lease_signers.insert(1, key);
	assert_eq!(
		reason(&cfg),
		E::SecretRoleReuse {
			first_role: Lease,
			second_role: Transfer,
			node: 7
		}
	);
}

#[test]
fn identity_cross_role_reuse_covers_different_ids_in_numeric_order() {
	let mut cfg = config();
	let key = lease_verifying_key_bytes([43; 32]);
	cfg.trusted_lease_signers.insert(256, key);
	cfg.trusted_lease_signers.insert(2, key);
	cfg.trusted_transfer_signers.insert(257, key);
	cfg.trusted_transfer_signers.insert(3, key);
	assert_eq!(
		reason(&cfg),
		E::PublicRoleReuse {
			first_role: Lease,
			first_node: 2,
			second_role: Transfer,
			second_node: 3
		}
	);
}

#[test]
fn identity_cross_role_same_id_reuse_is_refused() {
	let mut cfg = config();
	let key = lease_verifying_key_bytes([43; 32]);
	cfg.trusted_lease_signers.insert(2, key);
	cfg.trusted_transfer_signers.insert(2, key);
	assert_eq!(
		reason(&cfg),
		E::PublicRoleReuse {
			first_role: Lease,
			first_node: 2,
			second_role: Transfer,
			second_node: 2
		}
	);
}

#[test]
fn identity_duplicate_keys_are_refused_lease_then_transfer_and_numeric_id() {
	let mut cfg = config();
	let lease = lease_verifying_key_bytes([43; 32]);
	let transfer = transfer_verifying_key_bytes([44; 32]);
	for id in [256, 2] {
		cfg.trusted_lease_signers.insert(id, lease);
		cfg.trusted_transfer_signers.insert(id, transfer);
	}
	assert_eq!(
		reason(&cfg),
		E::IdentityReuse {
			role: Lease,
			first_node: 2,
			node: 256
		}
	);
	cfg.trusted_lease_signers.remove(&256);
	assert_eq!(
		reason(&cfg),
		E::IdentityReuse {
			role: Transfer,
			first_node: 2,
			node: 256
		}
	);
}

#[test]
fn identity_allow_dev_keys_never_bypasses_consistency_or_separation() {
	let mut cfg = config();
	cfg.allow_dev_keys = true;
	cfg.trusted_lease_signers.clear();
	assert_eq!(
		reason(&cfg),
		E::MissingLocal {
			role: Lease,
			node: 7
		}
	);
	cfg.trusted_lease_signers.insert(7, [0; 32]);
	assert_eq!(
		reason(&cfg),
		E::LocalKeyMismatch {
			role: Lease,
			node: 7
		}
	);
	cfg.trusted_lease_signers
		.insert(7, lease_verifying_key_bytes(cfg.lease_signing_secret));
	cfg.trusted_lease_signers
		.insert(2, cfg.trusted_transfer_signers[&7]);
	assert_eq!(
		reason(&cfg),
		E::PublicRoleReuse {
			first_role: Lease,
			first_node: 2,
			second_role: Transfer,
			second_node: 7
		}
	);
	cfg.trusted_lease_signers.remove(&2);
	cfg.trusted_lease_signers
		.insert(2, cfg.trusted_lease_signers[&7]);
	assert_eq!(
		reason(&cfg),
		E::IdentityReuse {
			role: Lease,
			first_node: 2,
			node: 7
		}
	);
}

#[test]
fn identity_dev_locals_check_both_families_and_roles_before_peers() {
	for (role, secret) in [
		(Lease, dev_lease_signing_secret(9)),
		(Lease, dev_transfer_signing_secret(9)),
		(Transfer, dev_lease_signing_secret(9)),
		(Transfer, dev_transfer_signing_secret(9)),
		(Lease, dev_audit_root_signing_secret(9)),
		(Transfer, dev_audit_root_signing_secret(9)),
		(Audit, dev_lease_signing_secret(9)),
		(Audit, dev_transfer_signing_secret(9)),
		(Audit, dev_audit_root_signing_secret(9)),
	] {
		let mut cfg = config();
		cfg.trusted_transfer_signers
			.insert(9, transfer_verifying_key_bytes([45; 32]));
		match role {
			Audit => {
				cfg.audit_root_signing_secret = secret;
				cfg.trusted_audit_root_keys
					.insert(7, audit_root_verifying_key_bytes(secret));
			}
			Lease => {
				cfg.lease_signing_secret = secret;
				cfg.trusted_lease_signers
					.insert(7, lease_verifying_key_bytes(secret));
			}
			Transfer => {
				cfg.transfer_signing_secret = secret;
				cfg.trusted_transfer_signers
					.insert(7, transfer_verifying_key_bytes(secret));
			}
		}
		cfg.trusted_lease_signers
			.insert(2, lease_verifying_key_bytes(dev_lease_signing_secret(2)));
		assert_eq!(reason(&cfg), E::DevKey { role, node: 7 });
	}
	let mut cfg = NodeBootstrapConfig::dev(7, std::env::temp_dir(), "127.0.0.1:0".into());
	cfg.allow_dev_keys = false;
	assert_eq!(
		reason(&cfg),
		E::DevKey {
			role: Lease,
			node: 7
		}
	);
}

#[test]
fn identity_peer_dev_keys_cover_both_families_roles_and_union_ids() {
	for role in [Lease, Transfer, Audit] {
		for source_role in [Lease, Transfer, Audit] {
			let mut cfg = config();
			// Derivation id9 exists only in the opposite role, not under the installed id2.
			let key = match source_role {
				Audit => audit_root_verifying_key_bytes(dev_audit_root_signing_secret(9)),
				Lease => lease_verifying_key_bytes(dev_lease_signing_secret(9)),
				Transfer => transfer_verifying_key_bytes(dev_transfer_signing_secret(9)),
			};
			match role {
				Audit => {
					cfg.trusted_audit_root_keys.insert(2, key);
					cfg.trusted_lease_signers
						.insert(9, lease_verifying_key_bytes([45; 32]));
				}
				Lease => {
					cfg.trusted_lease_signers.insert(2, key);
					cfg.trusted_transfer_signers
						.insert(9, transfer_verifying_key_bytes([45; 32]));
				}
				Transfer => {
					cfg.trusted_transfer_signers.insert(2, key);
					cfg.trusted_lease_signers
						.insert(9, lease_verifying_key_bytes([45; 32]));
				}
			}
			assert_eq!(reason(&cfg), E::DevKey { role, node: 2 });
			cfg.allow_dev_keys = true;
			validate_identity_keys(&cfg).unwrap();
		}
	}
}

#[test]
fn identity_peer_dev_order_is_role_then_numeric_id_and_after_reuse() {
	let mut cfg = config();
	cfg.trusted_lease_signers.insert(
		256,
		lease_verifying_key_bytes(dev_lease_signing_secret(256)),
	);
	cfg.trusted_lease_signers
		.insert(2, lease_verifying_key_bytes(dev_lease_signing_secret(2)));
	cfg.trusted_transfer_signers.insert(
		1,
		transfer_verifying_key_bytes(dev_transfer_signing_secret(1)),
	);
	assert_eq!(
		reason(&cfg),
		E::DevKey {
			role: Lease,
			node: 2
		}
	);
	cfg.trusted_lease_signers
		.insert(3, cfg.trusted_lease_signers[&2]);
	assert_eq!(
		reason(&cfg),
		E::IdentityReuse {
			role: Lease,
			first_node: 2,
			node: 3
		}
	);
}

#[test]
fn identity_outside_finite_set_is_explicitly_not_detected() {
	let mut cfg = config();
	cfg.trusted_lease_signers.insert(
		2,
		lease_verifying_key_bytes(dev_transfer_signing_secret(999)),
	);
	validate_identity_keys(&cfg).unwrap();
}

#[test]
fn identity_error_source_contains_only_role_and_node_metadata() {
	let error = invalid(E::PublicRoleReuse {
		first_role: Lease,
		first_node: 2,
		second_role: Transfer,
		second_node: 3,
	});
	assert_eq!(error.kind(), ErrorKind::InvalidInput);
	assert_eq!(
		error.get_ref().unwrap().downcast_ref::<IdentityKeyError>(),
		Some(&E::PublicRoleReuse {
			first_role: Lease,
			first_node: 2,
			second_role: Transfer,
			second_node: 3
		})
	);
	assert!(std::mem::size_of::<IdentityKeyError>() < 32);
}

#[test]
#[cfg(not(feature = "fips"))]
fn identity_node_validate_enforces_policy_after_existing_preconditions() {
	let mut cfg = NodeBootstrapConfig::dev(7, std::env::temp_dir(), "127.0.0.1:0".into());
	cfg.validate().unwrap();
	cfg.allow_dev_keys = false;
	let error = cfg.validate().unwrap_err();
	assert_eq!(
		error.get_ref().unwrap().downcast_ref::<IdentityKeyError>(),
		Some(&E::DevKey {
			role: Lease,
			node: 7
		})
	);
	cfg.node_id = 0;
	let error = cfg.validate().unwrap_err();
	assert!(
		error
			.get_ref()
			.unwrap()
			.downcast_ref::<IdentityKeyError>()
			.is_none()
	);
}

#[test]
fn identity_audit_presence_and_match_follow_all_prior_role_presence() {
	let mut cfg = config();
	cfg.trusted_audit_root_keys.clear();
	cfg.trusted_lease_signers.insert(7, [0; 32]);
	assert_eq!(
		reason(&cfg),
		E::MissingLocal {
			role: Audit,
			node: 7
		}
	);
	cfg.trusted_lease_signers
		.insert(7, lease_verifying_key_bytes(cfg.lease_signing_secret));
	cfg.trusted_audit_root_keys.insert(7, [0; 32]);
	assert_eq!(
		reason(&cfg),
		E::LocalKeyMismatch {
			role: Audit,
			node: 7
		}
	);
}
#[test]
fn identity_three_role_pair_priority_and_secret_stage_are_frozen() {
	for (first_role, second_role) in [(Lease, Transfer), (Lease, Audit), (Transfer, Audit)] {
		let mut cfg = config();
		let secret = match first_role {
			Lease => cfg.lease_signing_secret,
			Transfer => cfg.transfer_signing_secret,
			Audit => unreachable!(),
		};
		match second_role {
			Transfer => {
				cfg.transfer_signing_secret = secret;
				cfg.trusted_transfer_signers
					.insert(7, transfer_verifying_key_bytes(secret));
			}
			Audit => {
				cfg.audit_root_signing_secret = secret;
				cfg.trusted_audit_root_keys
					.insert(7, audit_root_verifying_key_bytes(secret));
			}
			Lease => unreachable!(),
		}
		cfg.trusted_lease_signers
			.insert(1, cfg.trusted_transfer_signers[&7]);
		assert_eq!(
			reason(&cfg),
			E::SecretRoleReuse {
				first_role,
				second_role,
				node: 7
			}
		);
	}
	let mut cfg = config();
	for id in [2, 256] {
		cfg.trusted_lease_signers
			.insert(id, lease_verifying_key_bytes([51; 32]));
		cfg.trusted_transfer_signers
			.insert(id + 1, lease_verifying_key_bytes([51; 32]));
		cfg.trusted_audit_root_keys
			.insert(id + 2, lease_verifying_key_bytes([51; 32]));
	}
	assert_eq!(
		reason(&cfg),
		E::PublicRoleReuse {
			first_role: Lease,
			first_node: 2,
			second_role: Transfer,
			second_node: 3
		}
	);
	cfg.trusted_transfer_signers.remove(&3);
	cfg.trusted_transfer_signers.remove(&257);
	assert_eq!(
		reason(&cfg),
		E::PublicRoleReuse {
			first_role: Lease,
			first_node: 2,
			second_role: Audit,
			second_node: 4
		}
	);
	cfg.trusted_lease_signers.remove(&2);
	cfg.trusted_lease_signers.remove(&256);
	cfg.trusted_transfer_signers
		.insert(3, lease_verifying_key_bytes([51; 32]));
	assert_eq!(
		reason(&cfg),
		E::PublicRoleReuse {
			first_role: Transfer,
			first_node: 3,
			second_role: Audit,
			second_node: 4
		}
	);
}
#[test]
fn identity_audit_duplicates_and_dev_authorization_do_not_bypass_separation() {
	let mut cfg = config();
	cfg.allow_dev_keys = true;
	let key = audit_root_verifying_key_bytes([51; 32]);
	cfg.trusted_audit_root_keys.insert(256, key);
	cfg.trusted_audit_root_keys.insert(2, key);
	assert_eq!(
		reason(&cfg),
		E::IdentityReuse {
			role: Audit,
			first_node: 2,
			node: 256
		}
	);
	cfg.trusted_lease_signers.insert(3, key);
	assert_eq!(
		reason(&cfg),
		E::PublicRoleReuse {
			first_role: Lease,
			first_node: 3,
			second_role: Audit,
			second_node: 2
		}
	);
}
#[test]
fn identity_audit_rollup_seed_matches_audit_enrollment_and_other_roles_differ() {
	let cfg = config();
	let signer = crate::audit::AuditRootSigner::from_seed(cfg.audit_root_signing_secret);
	assert_eq!(
		signer.verifying_key_hex(),
		crate::audit::hex_string(&cfg.trusted_audit_root_keys[&7])
	);
	assert_ne!(
		cfg.trusted_audit_root_keys[&7],
		cfg.trusted_lease_signers[&7]
	);
	assert_ne!(
		cfg.trusted_audit_root_keys[&7],
		cfg.trusted_transfer_signers[&7]
	);
	let source = include_str!("../../node.rs");
	assert!(source.contains("AuditRootSigner::from_seed(cfg.audit_root_signing_secret)"));
}

#[test]
fn identity_dev_recognition_includes_ids_only_in_audit_map_for_every_role() {
	for role in [Lease, Transfer, Audit] {
		for secret in [
			dev_lease_signing_secret(9),
			dev_transfer_signing_secret(9),
			dev_audit_root_signing_secret(9),
		] {
			let mut cfg = config();
			cfg.trusted_audit_root_keys
				.insert(9, audit_root_verifying_key_bytes([51; 32]));
			let key = audit_root_verifying_key_bytes(secret);
			match role {
				Lease => {
					cfg.trusted_lease_signers.insert(2, key);
				}
				Transfer => {
					cfg.trusted_transfer_signers.insert(2, key);
				}
				Audit => {
					cfg.trusted_audit_root_keys.insert(2, key);
				}
			}
			assert_eq!(reason(&cfg), E::DevKey { role, node: 2 });
			cfg.allow_dev_keys = true;
			validate_identity_keys(&cfg).unwrap();
		}
	}
}

// ADR 0721: the decision role.

fn decision_config() -> NodeBootstrapConfig {
	use crate::consensus::decisions::decision_verifying_key_bytes;
	let secret = [51; 32];
	config()
		.with_parent_decisions_for_disposable_store()
		.with_decision_keys(
			secret,
			BTreeMap::from([(7, decision_verifying_key_bytes(secret))]),
		)
}

fn decision_refusal(cfg: &NodeBootstrapConfig) -> String {
	let error = resolve_decision_keys(cfg).unwrap_err();
	assert_eq!(error.kind(), ErrorKind::InvalidInput);
	error.to_string()
}

#[test]
fn decision_keys_of_their_own_pass_without_dev_keys() {
	let cfg = decision_config();
	assert!(!cfg.allow_dev_keys);
	let (secret, trusted) = resolve_decision_keys(&cfg).unwrap();
	assert_eq!(secret, [51; 32]);
	assert_eq!(trusted.len(), 1);
	// Nothing of the secret is printed with the configuration.
	let printed = format!("{cfg:?}");
	assert!(printed.contains("decision_signing_secret: Some(<redacted>)"));
	assert!(!printed.contains(&format!("{:?}", [51u8; 32])));
}

#[test]
fn development_decision_keys_need_allow_dev_keys() {
	use crate::consensus::decisions::{decision_verifying_key_bytes, dev_decision_signing_secret};
	// Unset keys are the development keys.
	let mut unset = config().with_parent_decisions_for_disposable_store();
	assert!(decision_refusal(&unset).contains("needs identity.allow_dev_keys"));
	unset.allow_dev_keys = true;
	resolve_decision_keys(&unset).unwrap();
	// A real local key beside a peer's development key is refused too.
	let mut mixed = decision_config();
	mixed.trusted_decision_signers.insert(
		8,
		decision_verifying_key_bytes(dev_decision_signing_secret(8)),
	);
	assert!(decision_refusal(&mixed).contains("needs identity.allow_dev_keys"));
}

#[test]
fn a_decision_key_is_its_nodes_own_and_no_other_roles() {
	use crate::consensus::decisions::decision_verifying_key_bytes;
	let mut absent = decision_config();
	absent.trusted_decision_signers.clear();
	absent
		.trusted_decision_signers
		.insert(8, decision_verifying_key_bytes([52; 32]));
	assert!(decision_refusal(&absent).contains("the local key is missing"));

	let mut differs = decision_config();
	differs
		.trusted_decision_signers
		.insert(7, decision_verifying_key_bytes([52; 32]));
	assert!(decision_refusal(&differs).contains("the local key is missing"));

	let mut reused = decision_config();
	reused.decision_signing_secret = Some(crate::node::DecisionSigningSecret(
		reused.lease_signing_secret,
	));
	reused
		.trusted_decision_signers
		.insert(7, decision_verifying_key_bytes(reused.lease_signing_secret));
	assert!(decision_refusal(&reused).contains("reused from another role"));

	let mut twice = decision_config();
	twice
		.trusted_decision_signers
		.insert(8, decision_verifying_key_bytes([51; 32]));
	assert!(decision_refusal(&twice).contains("names two nodes"));
	for refusal in [
		decision_refusal(&absent),
		decision_refusal(&differs),
		decision_refusal(&reused),
		decision_refusal(&twice),
	] {
		assert!(
			!refusal.contains("51, 51") && !refusal.contains("3333"),
			"{refusal}"
		);
	}
}
