//! Startup consistency and explicit authorization for recognized dev signing keys.
//!
//! Recognition covers all three derivation families for the finite set S of the local id and
//! all three trusted-map id sets, even when a key is installed under another id or role.
//! A dev key derived from an id outside S is not detected. Enumerating all 2^32 ids is
//! not a bounded startup algorithm; arbitrary weak-key detection needs separate provenance.
use crate::network::{
	audit_root_verifying_key_bytes, dev_audit_root_signing_secret, dev_lease_signing_secret,
	dev_transfer_signing_secret, lease_verifying_key_bytes, transfer_verifying_key_bytes,
};
use crate::node::NodeBootstrapConfig;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Error, ErrorKind};

/// Signing role named in an identity configuration refusal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityKeyRole {
	Lease,
	Transfer,
	Audit,
}

/// Typed startup refusal. Only roles and node ids are retained, never key material.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityKeyError {
	MissingLocal {
		role: IdentityKeyRole,
		node: u32,
	},
	LocalKeyMismatch {
		role: IdentityKeyRole,
		node: u32,
	},
	SecretRoleReuse {
		first_role: IdentityKeyRole,
		second_role: IdentityKeyRole,
		node: u32,
	},
	PublicRoleReuse {
		first_role: IdentityKeyRole,
		first_node: u32,
		second_role: IdentityKeyRole,
		second_node: u32,
	},
	IdentityReuse {
		role: IdentityKeyRole,
		first_node: u32,
		node: u32,
	},
	DevKey {
		role: IdentityKeyRole,
		node: u32,
	},
}
impl std::fmt::Display for IdentityKeyError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "identity signing key refused: {self:?}")
	}
}
impl std::error::Error for IdentityKeyError {}
fn invalid(reason: IdentityKeyError) -> Error {
	Error::new(ErrorKind::InvalidInput, reason)
}

pub(super) fn validate_identity_keys(cfg: &NodeBootstrapConfig) -> std::io::Result<()> {
	use IdentityKeyError as E;
	use IdentityKeyRole::{Audit, Lease, Transfer};
	let roles = [
		(Lease, &cfg.trusted_lease_signers),
		(Transfer, &cfg.trusted_transfer_signers),
		(Audit, &cfg.trusted_audit_root_keys),
	];
	// All existence checks precede all local-match checks, in Lease/Transfer/Audit order.
	for (role, map) in roles {
		if !map.contains_key(&cfg.node_id) {
			return Err(invalid(E::MissingLocal {
				role,
				node: cfg.node_id,
			}));
		}
	}
	let local = [
		(Lease, lease_verifying_key_bytes(cfg.lease_signing_secret)),
		(
			Transfer,
			transfer_verifying_key_bytes(cfg.transfer_signing_secret),
		),
		(
			Audit,
			audit_root_verifying_key_bytes(cfg.audit_root_signing_secret),
		),
	];
	for ((role, map), (_, key)) in roles.into_iter().zip(local) {
		if map.get(&cfg.node_id) != Some(&key) {
			return Err(invalid(E::LocalKeyMismatch {
				role,
				node: cfg.node_id,
			}));
		}
	}
	// Stage 3 is frozen: secret pairs LT/LA/TA, public pairs LT/LA/TA,
	// then within-role identity reuse L/T/A. Node traversal is ascending numeric id.
	let secrets = [
		cfg.lease_signing_secret,
		cfg.transfer_signing_secret,
		cfg.audit_root_signing_secret,
	];
	for (first, second) in [(0, 1), (0, 2), (1, 2)] {
		let (first_role, _) = roles[first];
		let (second_role, _) = roles[second];
		if secrets[first] == secrets[second] {
			return Err(invalid(E::SecretRoleReuse {
				first_role,
				second_role,
				node: cfg.node_id,
			}));
		}
	}
	for (first, second) in [(0, 1), (0, 2), (1, 2)] {
		let (first_role, first_map) = roles[first];
		let (second_role, second_map) = roles[second];
		let mut second_ids = BTreeMap::new();
		for (&id, &key) in second_map {
			second_ids.entry(key).or_insert(id);
		}
		for (&first_node, key) in first_map {
			if let Some(&second_node) = second_ids.get(key) {
				return Err(invalid(E::PublicRoleReuse {
					first_role,
					first_node,
					second_role,
					second_node,
				}));
			}
		}
	}
	for (role, map) in roles {
		let mut first_ids = BTreeMap::new();
		for (&node, &key) in map {
			if let Some(&first_node) = first_ids.get(&key) {
				return Err(invalid(E::IdentityReuse {
					role,
					first_node,
					node,
				}));
			}
			first_ids.insert(key, node);
		}
	}
	// Only the recognized-dev prohibition can be disabled.
	if !cfg.allow_dev_keys {
		let ids: BTreeSet<_> = std::iter::once(cfg.node_id)
			.chain(cfg.trusted_lease_signers.keys().copied())
			.chain(cfg.trusted_transfer_signers.keys().copied())
			.chain(cfg.trusted_audit_root_keys.keys().copied())
			.collect();
		let mut recognized = BTreeSet::new();
		for id in ids {
			recognized.insert(audit_root_verifying_key_bytes(
				dev_audit_root_signing_secret(id),
			));
			recognized.insert(lease_verifying_key_bytes(dev_lease_signing_secret(id)));
			recognized.insert(transfer_verifying_key_bytes(dev_transfer_signing_secret(
				id,
			)));
		}
		for (role, key) in local {
			if recognized.contains(&key) {
				return Err(invalid(E::DevKey {
					role,
					node: cfg.node_id,
				}));
			}
		}
		for (role, map) in roles {
			for (&node, key) in map {
				if recognized.contains(key) {
					return Err(invalid(E::DevKey { role, node }));
				}
			}
		}
	}
	Ok(())
}

#[cfg(test)]
mod tests;
