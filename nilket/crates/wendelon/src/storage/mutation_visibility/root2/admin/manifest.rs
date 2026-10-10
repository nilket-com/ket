use super::errors::{AdminManifestError2, AdminResult2};
use super::key::check_enrollable_admin_key2;
use crate::storage::mutation_visibility::root2::{codec::hash, types::Digest2};
use crate::storage::mutation_visibility::{MutationScope, redacted_debug};

pub(in crate::storage::mutation_visibility::root2) const ADMIN_MANIFEST_BYTES2: usize = 137;
pub(in crate::storage::mutation_visibility::root2) const ADMIN_MANIFEST_READ_BOUND2: usize = 4096;
const MAGIC: &[u8; 8] = b"VISADM2\0";
const DOMAIN: &[u8] = b"VISROOT2-ADMIN-MANIFEST\0";
const ACTIONS: u32 = 3;

#[derive(Clone, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct AdminManifest2 {
	pub generation: [u8; 16],
	pub cluster: [u8; 16],
	pub initial_epoch: u64,
	pub admin_id: [u8; 16],
	pub admin_key: [u8; 32],
	pub namespace: AdminNamespace2,
}
redacted_debug!(AdminManifest2, "AdminManifest2");
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct AdminNamespace2 {
	pub tenant: u64,
	pub scope: MutationScope,
	pub table: [u8; 16],
	pub schema: u64,
}
redacted_debug!(AdminNamespace2, "AdminNamespace2");

fn array<const N: usize>(b: &[u8], at: usize) -> [u8; N] {
	b[at..at + N].try_into().expect("fixed manifest offset")
}
fn u64_at(b: &[u8], at: usize) -> u64 {
	u64::from_le_bytes(array(b, at))
}
fn check_fields(m: &AdminManifest2) -> AdminResult2<()> {
	if m.generation == [0; 16]
		|| m.cluster == [0; 16]
		|| m.initial_epoch == 0
		|| m.admin_id == [0; 16]
	{
		return Err(AdminManifestError2::ZeroField);
	}
	let n = &m.namespace;
	let valid = match n.scope {
		MutationScope::Raw => n.table == [0; 16] && n.schema == 0,
		MutationScope::Table => n.table != [0; 16] && n.schema != 0,
	};
	if !valid {
		return Err(AdminManifestError2::Namespace);
	}
	Ok(())
}
impl AdminManifest2 {
	/// Profile 1 only. The input is borrowed and never modified.
	pub(in crate::storage::mutation_visibility::root2) fn decode(
		input: &[u8],
	) -> AdminResult2<Self> {
		if input.len() > ADMIN_MANIFEST_READ_BOUND2 {
			return Err(AdminManifestError2::OverReadBound);
		}
		if input.len() != ADMIN_MANIFEST_BYTES2 {
			return Err(AdminManifestError2::Length);
		}
		if &input[0..8] != MAGIC {
			return Err(AdminManifestError2::Magic);
		}
		if u16::from_le_bytes(array(input, 8)) != 1 {
			return Err(AdminManifestError2::UnsupportedFormat);
		}
		if u16::from_le_bytes(array(input, 10)) != 1 {
			return Err(AdminManifestError2::UnsupportedProfile);
		}
		let (generation, cluster, admin_id) =
			(array(input, 12), array(input, 28), array(input, 52));
		let initial_epoch = u64_at(input, 44);
		if generation == [0; 16] || cluster == [0; 16] || initial_epoch == 0 || admin_id == [0; 16]
		{
			return Err(AdminManifestError2::ZeroField);
		}
		if u32::from_le_bytes(array(input, 100)) != ACTIONS {
			return Err(AdminManifestError2::UnsupportedAction);
		}
		let scope = match input[112] {
			0 => MutationScope::Raw,
			1 => MutationScope::Table,
			_ => return Err(AdminManifestError2::Namespace),
		};
		let manifest = Self {
			generation,
			cluster,
			initial_epoch,
			admin_id,
			admin_key: array(input, 68),
			namespace: AdminNamespace2 {
				tenant: u64_at(input, 104),
				scope,
				table: array(input, 113),
				schema: u64_at(input, 129),
			},
		};
		check_fields(&manifest)?;
		check_enrollable_admin_key2(&manifest.admin_key)?;
		if manifest.encode()? != *input {
			return Err(AdminManifestError2::NotCanonical);
		}
		Ok(manifest)
	}
	pub(in crate::storage::mutation_visibility::root2) fn encode(
		&self,
	) -> AdminResult2<[u8; ADMIN_MANIFEST_BYTES2]> {
		check_fields(self)?;
		check_enrollable_admin_key2(&self.admin_key)?;
		let mut b = [0u8; ADMIN_MANIFEST_BYTES2];
		b[0..8].copy_from_slice(MAGIC);
		b[8..10].copy_from_slice(&1u16.to_le_bytes());
		b[10..12].copy_from_slice(&1u16.to_le_bytes());
		b[12..28].copy_from_slice(&self.generation);
		b[28..44].copy_from_slice(&self.cluster);
		b[44..52].copy_from_slice(&self.initial_epoch.to_le_bytes());
		b[52..68].copy_from_slice(&self.admin_id);
		b[68..100].copy_from_slice(&self.admin_key);
		b[100..104].copy_from_slice(&ACTIONS.to_le_bytes());
		b[104..112].copy_from_slice(&self.namespace.tenant.to_le_bytes());
		b[112] = self.namespace.scope as u8;
		b[113..129].copy_from_slice(&self.namespace.table);
		b[129..137].copy_from_slice(&self.namespace.schema.to_le_bytes());
		Ok(b)
	}
}
/// Digest math only; it does not make the bytes or the digest a provisioned pin.
pub(in crate::storage::mutation_visibility::root2) fn admin_manifest_digest2(
	bytes: &[u8; ADMIN_MANIFEST_BYTES2],
) -> Digest2 {
	hash(DOMAIN, bytes)
}
pub(in crate::storage::mutation_visibility::root2) fn check_admin_manifest_digest2(
	bytes: &[u8; ADMIN_MANIFEST_BYTES2],
	expected: &Digest2,
) -> AdminResult2<()> {
	if admin_manifest_digest2(bytes) != *expected {
		return Err(AdminManifestError2::DigestMismatch);
	}
	Ok(())
}
