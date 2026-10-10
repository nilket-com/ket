//! Dormant authentication of an administrator's Admission permission statement against a derived
//! administrator view. A checked statement is authenticated against that view only: it is not an
//! issuer authorization, carries no committed-provenance claim, and implements no lifecycle,
//! registry or replay semantics. Its whole context must be re-bound to the administrator installed
//! at the pre-entry cut by the later issuer gate. No signing or key custody happens here.
use super::super::{MutationScope, redacted_debug};
use super::admin_install::{
	AdminContext2, InstalledAdmin2, admin_context2, admin_key2, admin_namespace2,
};
use super::types::{AuthorityRef2, Log2};
use ed25519_dalek::{Signature, VerifyingKey};

const MAGIC: &[u8; 8] = b"VISADS02";
const VERSION: u16 = 1;
const DOMAIN: &[u8] = b"VISROOT2-ADMIN-STATEMENT\0";
const SIGNED: usize = 273;
pub(in crate::storage::mutation_visibility::root2) const ADMIN_STATEMENT_BYTES2: usize = 337;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct AdmissionNamespace2 {
	pub generation: [u8; 16],
	pub tenant: u64,
	pub scope: MutationScope,
	pub table: [u8; 16],
	pub schema: u64,
}
redacted_debug!(AdmissionNamespace2, "AdmissionNamespace2");
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct PermissionKey2 {
	pub authority: AuthorityRef2,
	pub namespace: AdmissionNamespace2,
}
redacted_debug!(PermissionKey2, "PermissionKey2");
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(in crate::storage::mutation_visibility::root2) enum AdminAction2 {
	Grant = 1,
	Revoke = 2,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum ZeroField2 {
	ContextGeneration,
	ContextCluster,
	ContextEpoch,
	AuthorityId,
	AuthorityRevision,
	NamespaceGeneration,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum StatementError2 {
	Length,
	Magic,
	Version,
	Reserved,
	Action,
	ZeroField(ZeroField2),
	InvalidLog,
	Namespace,
	NotCanonical,
	Context,
	Scope,
	Signature,
}
#[derive(Clone, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct CheckedStatement2 {
	action: AdminAction2,
	key: PermissionKey2,
	context: AdminContext2,
}
redacted_debug!(CheckedStatement2, "CheckedStatement2");

struct Parsed {
	action: AdminAction2,
	key: PermissionKey2,
	context: AdminContext2,
	signature: [u8; 64],
}

fn array<const N: usize>(b: &[u8], at: usize) -> [u8; N] {
	b[at..at + N].try_into().expect("fixed statement offset")
}
fn u64_at(b: &[u8], at: usize) -> u64 {
	u64::from_le_bytes(array(b, at))
}
// Steps 6-8 of the structural order: context, key, namespace.
fn check_fields(context: &AdminContext2, key: &PermissionKey2) -> Result<(), StatementError2> {
	use StatementError2 as E;
	for (zero, field) in [
		(context.generation == [0; 16], ZeroField2::ContextGeneration),
		(context.cluster == [0; 16], ZeroField2::ContextCluster),
		(context.initial_epoch == 0, ZeroField2::ContextEpoch),
	] {
		if zero {
			return Err(E::ZeroField(field));
		}
	}
	let l = context.installed_at;
	if l.term == 0 || l.node == 0 || l.index == 0 {
		return Err(E::InvalidLog);
	}
	for (zero, field) in [
		(key.authority.id == [0; 16], ZeroField2::AuthorityId),
		(key.authority.revision == 0, ZeroField2::AuthorityRevision),
		(
			key.namespace.generation == [0; 16],
			ZeroField2::NamespaceGeneration,
		),
	] {
		if zero {
			return Err(E::ZeroField(field));
		}
	}
	let n = &key.namespace;
	let shaped = match n.scope {
		MutationScope::Raw => n.table == [0; 16] && n.schema == 0,
		MutationScope::Table => n.table != [0; 16] && n.schema > 0,
	};
	if !shaped {
		return Err(E::Namespace);
	}
	Ok(())
}
fn write(
	action: AdminAction2,
	key: &PermissionKey2,
	context: &AdminContext2,
	signature: &[u8; 64],
) -> [u8; ADMIN_STATEMENT_BYTES2] {
	let mut b = [0u8; ADMIN_STATEMENT_BYTES2];
	let mut at = 0;
	let mut put = |bytes: &[u8]| {
		b[at..at + bytes.len()].copy_from_slice(bytes);
		at += bytes.len();
	};
	put(MAGIC);
	put(&VERSION.to_le_bytes());
	put(&[action as u8, 0]);
	put(&context.generation);
	put(&context.cluster);
	put(&context.installed_at.term.to_le_bytes());
	put(&context.installed_at.node.to_le_bytes());
	put(&context.installed_at.index.to_le_bytes());
	put(&context.bootstrap_fingerprint);
	put(&context.bootstrap_command);
	put(&context.genesis);
	put(&context.initial_epoch.to_le_bytes());
	put(&context.manifest_pin);
	put(&key.authority.id);
	put(&key.authority.revision.to_le_bytes());
	put(&key.namespace.generation);
	put(&key.namespace.tenant.to_le_bytes());
	put(&[key.namespace.scope as u8]);
	put(&key.namespace.table);
	put(&key.namespace.schema.to_le_bytes());
	put(signature);
	b
}
// Structural parse, steps 1-9 in order; the first failure wins.
fn parse(bytes: &[u8]) -> Result<Parsed, StatementError2> {
	use StatementError2 as E;
	if bytes.len() != ADMIN_STATEMENT_BYTES2 {
		return Err(E::Length);
	}
	if &bytes[0..8] != MAGIC {
		return Err(E::Magic);
	}
	if u16::from_le_bytes(array(bytes, 8)) != VERSION {
		return Err(E::Version);
	}
	if bytes[11] != 0 {
		return Err(E::Reserved);
	}
	let action = match bytes[10] {
		1 => AdminAction2::Grant,
		2 => AdminAction2::Revoke,
		_ => return Err(E::Action),
	};
	let context = AdminContext2 {
		generation: array(bytes, 12),
		cluster: array(bytes, 28),
		installed_at: Log2 {
			term: u64_at(bytes, 44),
			node: u32::from_le_bytes(array(bytes, 52)),
			index: u64_at(bytes, 56),
		},
		bootstrap_fingerprint: array(bytes, 64),
		bootstrap_command: array(bytes, 96),
		genesis: array(bytes, 128),
		initial_epoch: u64_at(bytes, 160),
		manifest_pin: array(bytes, 168),
	};
	// The scope byte is checked with the namespace rules, after the zero-field checks.
	let scope = match bytes[248] {
		0 => Some(MutationScope::Raw),
		1 => Some(MutationScope::Table),
		_ => None,
	};
	let key = PermissionKey2 {
		authority: AuthorityRef2 {
			id: array(bytes, 200),
			revision: u64_at(bytes, 216),
		},
		namespace: AdmissionNamespace2 {
			generation: array(bytes, 224),
			tenant: u64_at(bytes, 240),
			scope: scope.unwrap_or(MutationScope::Raw),
			table: array(bytes, 249),
			schema: u64_at(bytes, 265),
		},
	};
	// An unknown scope tag is a namespace refusal, after any zero-field refusal.
	check_fields(&context, &key)?;
	if scope.is_none() {
		return Err(E::Namespace);
	}
	let signature = array(bytes, SIGNED);
	// Every byte is either checked above or free data, so this cannot fail; kept as a guard.
	if write(action, &key, &context, &signature)[..] != *bytes {
		return Err(E::NotCanonical);
	}
	Ok(Parsed {
		action,
		key,
		context,
		signature,
	})
}

/// Canonical bytes for a statement; refuses exactly the inputs the parser would refuse.
pub(in crate::storage::mutation_visibility::root2) fn encode_admin_statement2(
	action: AdminAction2,
	key: &PermissionKey2,
	context: &AdminContext2,
	signature: &[u8; 64],
) -> Result<[u8; ADMIN_STATEMENT_BYTES2], StatementError2> {
	check_fields(context, key)?;
	Ok(write(action, key, context, signature))
}
/// The signed bytes: the domain, then everything but the signature, after a structural parse.
pub(in crate::storage::mutation_visibility::root2) fn admin_statement_preimage2(
	bytes: &[u8; ADMIN_STATEMENT_BYTES2],
) -> Result<Vec<u8>, StatementError2> {
	parse(bytes)?;
	let mut preimage = Vec::with_capacity(DOMAIN.len() + SIGNED);
	preimage.extend_from_slice(DOMAIN);
	preimage.extend_from_slice(&bytes[..SIGNED]);
	Ok(preimage)
}
/// Order: structure, the whole context, the exact administrator scope, then the strict signature.
pub(in crate::storage::mutation_visibility::root2) fn check_admin_statement2(
	admin: &InstalledAdmin2,
	bytes: &[u8],
) -> Result<CheckedStatement2, StatementError2> {
	let p = parse(bytes)?;
	if p.context != admin_context2(admin) {
		return Err(StatementError2::Context);
	}
	let ns = admin_namespace2(admin);
	let n = &p.key.namespace;
	if n.generation != p.context.generation
		|| (n.tenant, n.scope, n.table, n.schema) != (ns.tenant, ns.scope, ns.table, ns.schema)
	{
		return Err(StatementError2::Scope);
	}
	let preimage =
		admin_statement_preimage2(bytes.try_into().map_err(|_| StatementError2::Length)?)?;
	VerifyingKey::from_bytes(&admin_key2(admin))
		.map_err(|_| StatementError2::Signature)?
		.verify_strict(&preimage, &Signature::from_bytes(&p.signature))
		.map_err(|_| StatementError2::Signature)?;
	Ok(CheckedStatement2 {
		action: p.action,
		key: p.key,
		context: p.context,
	})
}
pub(in crate::storage::mutation_visibility::root2) fn checked_action2(
	c: &CheckedStatement2,
) -> AdminAction2 {
	c.action
}
pub(in crate::storage::mutation_visibility::root2) fn checked_key2(
	c: &CheckedStatement2,
) -> PermissionKey2 {
	c.key
}
pub(in crate::storage::mutation_visibility::root2) fn checked_context2(
	c: &CheckedStatement2,
) -> AdminContext2 {
	c.context
}
