//! Dormant pure VISADG01 governance envelope; no production caller.
//! Describes supplied digest/action permission, not command or committed provenance proof.
//! S3 derives action exhaustively from the same Command2 whose digest is recomputed and bound
//! to entry.d; a caller-asserted action is never accepted at integration.
use crate::storage::mutation_visibility::redacted_debug;
use crate::storage::mutation_visibility::root2::admin_install::{
	AdminContext2, InstalledAdmin2, admin_context2, admin_governance2,
};
use crate::storage::mutation_visibility::root2::types::{Digest2, Log2};
use ed25519_dalek::{Signature, VerifyingKey};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum GovernanceAction2 {
	EnrollSelector,
	EnrollAuthority,
	RevokeAuthority,
	Grant,
	Release,
	Cut,
	Resolve,
	EnrollLeaseIssuer,
	RevokeLeaseIssuer,
}
impl GovernanceAction2 {
	pub(in crate::storage::mutation_visibility::root2) const ALL: [Self; 9] = [
		Self::EnrollSelector,
		Self::EnrollAuthority,
		Self::RevokeAuthority,
		Self::Grant,
		Self::Release,
		Self::Cut,
		Self::Resolve,
		Self::EnrollLeaseIssuer,
		Self::RevokeLeaseIssuer,
	];
	pub(in crate::storage::mutation_visibility::root2) const fn bit(self) -> u32 {
		match self {
			Self::EnrollSelector => 1,
			Self::EnrollAuthority => 2,
			Self::RevokeAuthority => 4,
			Self::Grant => 8,
			Self::Release => 16,
			Self::Cut => 32,
			Self::Resolve => 64,
			Self::EnrollLeaseIssuer => 128,
			Self::RevokeLeaseIssuer => 256,
		}
	}
	pub(in crate::storage::mutation_visibility::root2) const fn command_tag(self) -> u8 {
		match self {
			Self::EnrollSelector => 2,
			Self::EnrollAuthority => 3,
			Self::RevokeAuthority => 4,
			Self::Grant => 5,
			Self::Release => 6,
			Self::Cut => 7,
			Self::Resolve => 10,
			Self::EnrollLeaseIssuer => 12,
			Self::RevokeLeaseIssuer => 13,
		}
	}
}
fn permits(mask: u32, action: GovernanceAction2) -> bool {
	mask & action.bit() != 0
}
#[cfg(test)]
#[path = "tests/governance_permissions.rs"]
mod tests;
const MAGIC: &[u8; 8] = b"VISADG01";
const VERSION: u16 = 1;
const DOMAIN: &[u8] = b"VISROOT2-GOVERNANCE-COMMAND\0";
pub(in crate::storage::mutation_visibility::root2) const GOVERNANCE_COMMAND_BYTES2: usize = 296;
const SIGNED: usize = 232;
/// `InstalledAt` is any invalid full `Log2` (a zero term, node or index), not a zero struct.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum GovernanceField2 {
	Generation,
	Cluster,
	Epoch,
	InstalledAt,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum GovernanceCommandError2 {
	Length,
	Magic,
	Version,
	Reserved,
	ZeroField(GovernanceField2),
	NoGovernance,
	Action,
	Context,
	Digest,
	Signature,
}
pub(in crate::storage::mutation_visibility::root2) struct CheckedGovernanceCommand2 {
	context: AdminContext2,
	action: GovernanceAction2,
	command_digest: Digest2,
}
redacted_debug!(CheckedGovernanceCommand2, "CheckedGovernanceCommand2");
impl CheckedGovernanceCommand2 {
	pub(in crate::storage::mutation_visibility::root2) fn context(&self) -> AdminContext2 {
		self.context
	}
	pub(in crate::storage::mutation_visibility::root2) fn action(&self) -> GovernanceAction2 {
		self.action
	}
	pub(in crate::storage::mutation_visibility::root2) fn command_digest(&self) -> Digest2 {
		self.command_digest
	}
}
fn array<const N: usize>(b: &[u8], at: usize) -> [u8; N] {
	let mut out = [0u8; N];
	out.copy_from_slice(&b[at..at + N]);
	out
}
fn u64_at(b: &[u8], at: usize) -> u64 {
	u64::from_le_bytes(array(b, at))
}

fn check_context(context: &AdminContext2) -> Result<(), GovernanceCommandError2> {
	use GovernanceField2 as F;
	let l = context.installed_at;
	for (zero, field) in [
		(context.generation == [0; 16], F::Generation),
		(context.cluster == [0; 16], F::Cluster),
		(context.initial_epoch == 0, F::Epoch),
		(l.term == 0 || l.node == 0 || l.index == 0, F::InstalledAt),
	] {
		if zero {
			return Err(GovernanceCommandError2::ZeroField(field));
		}
	}
	Ok(())
}
fn write(
	context: &AdminContext2,
	command_digest: &Digest2,
	signature: &[u8; 64],
) -> [u8; GOVERNANCE_COMMAND_BYTES2] {
	let mut b = [0u8; GOVERNANCE_COMMAND_BYTES2];
	let mut at = 0;
	let mut put = |bytes: &[u8]| {
		b[at..at + bytes.len()].copy_from_slice(bytes);
		at += bytes.len();
	};
	put(MAGIC);
	put(&VERSION.to_le_bytes());
	put(&[0, 0]);
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
	put(command_digest);
	put(signature);
	b
}
/// Structural parse, in order: length, magic, version, reserved, then the zero fields.
pub(in crate::storage::mutation_visibility::root2) fn parse_governance_command2(
	bytes: &[u8],
) -> Result<(AdminContext2, Digest2, [u8; 64]), GovernanceCommandError2> {
	use GovernanceCommandError2 as E;
	if bytes.len() != GOVERNANCE_COMMAND_BYTES2 {
		return Err(E::Length);
	}
	if &bytes[0..8] != MAGIC {
		return Err(E::Magic);
	}
	if u16::from_le_bytes(array(bytes, 8)) != VERSION {
		return Err(E::Version);
	}
	if bytes[10..12] != [0, 0] {
		return Err(E::Reserved);
	}
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
	check_context(&context)?;
	Ok((context, array(bytes, 200), array(bytes, SIGNED)))
}

pub(in crate::storage::mutation_visibility::root2) fn encode_governance_command2(
	context: &AdminContext2,
	command_digest: Digest2,
	signature: &[u8; 64],
) -> Result<[u8; GOVERNANCE_COMMAND_BYTES2], GovernanceCommandError2> {
	check_context(context)?;
	Ok(write(context, &command_digest, signature))
}

/// The signed bytes: the domain, then the 232 bytes before the signature, after a structural parse.
pub(in crate::storage::mutation_visibility::root2) fn governance_command_preimage2(
	bytes: &[u8; GOVERNANCE_COMMAND_BYTES2],
) -> Result<Vec<u8>, GovernanceCommandError2> {
	parse_governance_command2(bytes)?;
	let mut preimage = Vec::with_capacity(DOMAIN.len() + SIGNED);
	preimage.extend_from_slice(DOMAIN);
	preimage.extend_from_slice(&bytes[..SIGNED]);
	Ok(preimage)
}

/// Order: structure, profile, action, context, digest, strict governance signature.
pub(in crate::storage::mutation_visibility::root2) fn check_governance_command2(
	admin: &InstalledAdmin2,
	action: GovernanceAction2,
	bytes: &[u8],
	command_digest: Digest2,
) -> Result<CheckedGovernanceCommand2, GovernanceCommandError2> {
	let (context, embedded, signature) = parse_governance_command2(bytes)?;
	let role = admin_governance2(admin).ok_or(GovernanceCommandError2::NoGovernance)?;
	if !permits(role.actions(), action) {
		return Err(GovernanceCommandError2::Action);
	}
	if context != admin_context2(admin) {
		return Err(GovernanceCommandError2::Context);
	}
	if embedded != command_digest {
		return Err(GovernanceCommandError2::Digest);
	}
	let fixed: &[u8; GOVERNANCE_COMMAND_BYTES2] = bytes
		.try_into()
		.map_err(|_| GovernanceCommandError2::Length)?;
	let preimage = governance_command_preimage2(fixed)?;
	VerifyingKey::from_bytes(&role.key())
		.map_err(|_| GovernanceCommandError2::Signature)?
		.verify_strict(&preimage, &Signature::from_bytes(&signature))
		.map_err(|_| GovernanceCommandError2::Signature)?;
	Ok(CheckedGovernanceCommand2 {
		context,
		action,
		command_digest: embedded,
	})
}
