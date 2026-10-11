//! Dormant pure check of an administrator command envelope (`VISADC01`, 296 bytes): exactly the
//! installed administrator's context and a supplied command digest, signed under the domain
//! `VISROOT2-ADMIN-COMMAND\0` with the administrator key (strict Ed25519).
//!
//! A `CheckedAdminCommand2` is NOT proof of a command's kind, of any action or profile permission
//! (the profile-1 manifest authorizes only Grant/Revoke statements), of a namespace, of committed
//! provenance or of pre-entry installation. It is not an authorization token: nothing here yields a
//! `VerifiedCommand2`, a trusted entry or a root, and nothing calls this module yet. No signing.
use crate::storage::mutation_visibility::redacted_debug;
use crate::storage::mutation_visibility::root2::admin_install::{
	AdminContext2, InstalledAdmin2, admin_context2, admin_key2,
};
use crate::storage::mutation_visibility::root2::types::{Digest2, Log2};
use ed25519_dalek::{Signature, VerifyingKey};

const MAGIC: &[u8; 8] = b"VISADC01";
const VERSION: u16 = 1;
const DOMAIN: &[u8] = b"VISROOT2-ADMIN-COMMAND\0";
pub(in crate::storage::mutation_visibility::root2) const ADMIN_COMMAND_BYTES2: usize = 296;
const SIGNED: usize = 232;

/// `InstalledAt` is any invalid full `Log2` (a zero term, node or index), not a zero struct.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum EnvelopeField2 {
	Generation,
	Cluster,
	Epoch,
	InstalledAt,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum EnvelopeError2 {
	Length,
	Magic,
	Version,
	Reserved,
	ZeroField(EnvelopeField2),
	Context,
	Digest,
	Signature,
}

/// The context and digest an envelope authenticated. Not a command, permission, namespace,
/// provenance or installation proof (see the module documentation).
pub(in crate::storage::mutation_visibility::root2) struct CheckedAdminCommand2 {
	context: AdminContext2,
	command_digest: Digest2,
}
redacted_debug!(CheckedAdminCommand2, "CheckedAdminCommand2");
impl CheckedAdminCommand2 {
	pub(in crate::storage::mutation_visibility::root2) fn context(&self) -> AdminContext2 {
		self.context
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

fn check_context(context: &AdminContext2) -> Result<(), EnvelopeError2> {
	use EnvelopeField2 as F;
	let l = context.installed_at;
	for (zero, field) in [
		(context.generation == [0; 16], F::Generation),
		(context.cluster == [0; 16], F::Cluster),
		(context.initial_epoch == 0, F::Epoch),
		(l.term == 0 || l.node == 0 || l.index == 0, F::InstalledAt),
	] {
		if zero {
			return Err(EnvelopeError2::ZeroField(field));
		}
	}
	Ok(())
}

fn write(
	context: &AdminContext2,
	command_digest: &Digest2,
	signature: &[u8; 64],
) -> [u8; ADMIN_COMMAND_BYTES2] {
	let mut b = [0u8; ADMIN_COMMAND_BYTES2];
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
fn parse(bytes: &[u8]) -> Result<(AdminContext2, Digest2, [u8; 64]), EnvelopeError2> {
	use EnvelopeError2 as E;
	if bytes.len() != ADMIN_COMMAND_BYTES2 {
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

/// Layout only (no signing), as `encode_admin_statement2`.
pub(in crate::storage::mutation_visibility::root2) fn encode_admin_command2(
	context: &AdminContext2,
	command_digest: Digest2,
	signature: &[u8; 64],
) -> Result<[u8; ADMIN_COMMAND_BYTES2], EnvelopeError2> {
	check_context(context)?;
	Ok(write(context, &command_digest, signature))
}

/// The signed bytes: the domain, then the 232 bytes before the signature, after a structural parse.
pub(in crate::storage::mutation_visibility::root2) fn admin_command_preimage2(
	bytes: &[u8; ADMIN_COMMAND_BYTES2],
) -> Result<Vec<u8>, EnvelopeError2> {
	parse(bytes)?;
	let mut preimage = Vec::with_capacity(DOMAIN.len() + SIGNED);
	preimage.extend_from_slice(DOMAIN);
	preimage.extend_from_slice(&bytes[..SIGNED]);
	Ok(preimage)
}

/// Order: structure, the whole context, the embedded digest against the supplied one, then the
/// strict signature under the administrator key.
pub(in crate::storage::mutation_visibility::root2) fn check_admin_command2(
	admin: &InstalledAdmin2,
	bytes: &[u8],
	command_digest: Digest2,
) -> Result<CheckedAdminCommand2, EnvelopeError2> {
	let (context, embedded, signature) = parse(bytes)?;
	if context != admin_context2(admin) {
		return Err(EnvelopeError2::Context);
	}
	if embedded != command_digest {
		return Err(EnvelopeError2::Digest);
	}
	let fixed: &[u8; ADMIN_COMMAND_BYTES2] =
		bytes.try_into().map_err(|_| EnvelopeError2::Length)?;
	let preimage = admin_command_preimage2(fixed)?;
	VerifyingKey::from_bytes(&admin_key2(admin))
		.map_err(|_| EnvelopeError2::Signature)?
		.verify_strict(&preimage, &Signature::from_bytes(&signature))
		.map_err(|_| EnvelopeError2::Signature)?;
	Ok(CheckedAdminCommand2 {
		context,
		command_digest: embedded,
	})
}
