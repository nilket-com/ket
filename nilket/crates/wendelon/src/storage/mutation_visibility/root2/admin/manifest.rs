use super::errors::{AdminManifestError2, AdminResult2, ManifestRole3, ReuseField3, RolePair3};
use super::key::check_enrollable_admin_key2;
use crate::storage::mutation_visibility::root2::{codec::hash, types::Digest2};
use crate::storage::mutation_visibility::{MutationScope, redacted_debug};

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

// The governance role sits at [137..189): `governance_id` [137..153), `governance_key` [153..185),
// `governance_actions` u32 [185..189); the recovery role follows at [189..241).
const GOVERNANCE_ACTIONS: u32 = 0x1FF;

/// The profile identifier only; it carries no role data. Profile 3 is the only manifest form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum ManifestProfile2 {
	Three,
}

/// The governance role of a manifest; its action mask is fixed (`0x1FF`).
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct GovernanceRole2 {
	id: [u8; 16],
	key: [u8; 32],
}
redacted_debug!(GovernanceRole2, "GovernanceRole2");
impl GovernanceRole2 {
	/// Order: a zero id (`ZeroField`), then key enrollability (`NonCanonicalKey`).
	pub(in crate::storage::mutation_visibility::root2) fn new(
		id: [u8; 16],
		key: [u8; 32],
	) -> AdminResult2<Self> {
		if id == [0; 16] {
			return Err(AdminManifestError2::ZeroField);
		}
		check_enrollable_admin_key2(&key)?;
		Ok(Self { id, key })
	}
	pub(in crate::storage::mutation_visibility::root2) fn id(&self) -> [u8; 16] {
		self.id
	}
	pub(in crate::storage::mutation_visibility::root2) fn key(&self) -> [u8; 32] {
		self.key
	}
	pub(in crate::storage::mutation_visibility::root2) fn actions(&self) -> u32 {
		GOVERNANCE_ACTIONS
	}
}

// ---- The manifest, with its exact canonical bytes ----

/// The administrator manifest: profile 3, the only form (the enum collapses in d).
#[derive(Clone, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum AnyAdminManifest2 {
	Three(AdminManifestThree3),
}
redacted_debug!(AnyAdminManifest2, "AnyAdminManifest2");

/// The exact canonical bytes of one manifest. Built only by the manifest enum's `encode`; the
/// bytes beyond `len` are always zero, so the derived equality is exact-byte equality.
#[derive(Clone, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct ManifestBytes2 {
	storage: [u8; ADMIN_MANIFEST_P3_BYTES3],
	len: usize,
}
redacted_debug!(ManifestBytes2, "ManifestBytes2");
impl ManifestBytes2 {
	pub(in crate::storage::mutation_visibility::root2) fn as_slice(&self) -> &[u8] {
		&self.storage[..self.len]
	}
}

impl AnyAdminManifest2 {
	/// The profile-3 decoder.
	pub(in crate::storage::mutation_visibility::root2) fn decode(
		input: &[u8],
	) -> AdminResult2<Self> {
		Ok(Self::Three(AdminManifestThree3::decode(input)?))
	}
	/// Revalidates the manifest through its own encoder.
	pub(in crate::storage::mutation_visibility::root2) fn encode(
		&self,
	) -> AdminResult2<ManifestBytes2> {
		let Self::Three(m) = self;
		let (storage, len) = (m.encode()?, ADMIN_MANIFEST_P3_BYTES3);
		Ok(ManifestBytes2 { storage, len })
	}
	pub(in crate::storage::mutation_visibility::root2) fn profile(&self) -> ManifestProfile2 {
		ManifestProfile2::Three
	}
	/// The Admission fields, descriptive only: never a whole-manifest pin or request identity.
	pub(in crate::storage::mutation_visibility::root2) fn admission(&self) -> &AdminManifest2 {
		let Self::Three(m) = self;
		m.base()
	}
}

/// Digest math over exact canonical bytes (the manifest domain); not trust.
pub(in crate::storage::mutation_visibility::root2) fn any_manifest_digest2(
	bytes: &ManifestBytes2,
) -> Digest2 {
	hash(DOMAIN, bytes.as_slice())
}

pub(in crate::storage::mutation_visibility::root2) const ADMIN_MANIFEST_P3_BYTES3: usize = 241;
const RECOVERY_ACTIONS3: u32 = 0x1F;
// Reserved inert bits: Freeze=1, Unfreeze=2, RotateAdmin=4, RotateGovernance=8, RetireRoot=16.
// This format slice executes none of them. Unfreeze requires a later operator decision.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct RecoveryRole3 {
	id: [u8; 16],
	key: [u8; 32],
}
redacted_debug!(RecoveryRole3, "RecoveryRole3");
impl RecoveryRole3 {
	pub(in crate::storage::mutation_visibility::root2) fn new(
		id: [u8; 16],
		key: [u8; 32],
	) -> AdminResult2<Self> {
		if id == [0; 16] {
			return Err(AdminManifestError2::ZeroField);
		}
		check_enrollable_admin_key2(&key)
			.map_err(|_| AdminManifestError2::KeyThree(ManifestRole3::Recovery))?;
		Ok(Self { id, key })
	}
	pub(in crate::storage::mutation_visibility::root2) fn id(&self) -> [u8; 16] {
		self.id
	}
	pub(in crate::storage::mutation_visibility::root2) fn key(&self) -> [u8; 32] {
		self.key
	}
	pub(in crate::storage::mutation_visibility::root2) fn actions(&self) -> u32 {
		RECOVERY_ACTIONS3
	}
}
#[derive(Clone, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct AdminManifestThree3 {
	base: AdminManifest2,
	governance: GovernanceRole2,
	recovery: RecoveryRole3,
}
redacted_debug!(AdminManifestThree3, "AdminManifestThree3");
/// Steps 1-5: the read bound, the one length (241) before any header access, magic, format,
/// profile 3.
fn profile_of_three(input: &[u8]) -> AdminResult2<ManifestProfile2> {
	if input.len() > ADMIN_MANIFEST_READ_BOUND2 {
		return Err(AdminManifestError2::OverReadBound);
	}
	if input.len() != ADMIN_MANIFEST_P3_BYTES3 {
		return Err(AdminManifestError2::Length);
	}
	if &input[..8] != MAGIC {
		return Err(AdminManifestError2::Magic);
	}
	if u16::from_le_bytes(array(input, 8)) != 1 {
		return Err(AdminManifestError2::UnsupportedFormat);
	}
	if u16::from_le_bytes(array(input, 10)) != 3 {
		return Err(AdminManifestError2::UnsupportedProfile);
	}
	Ok(ManifestProfile2::Three)
}
fn check_three(m: &AdminManifestThree3) -> AdminResult2<()> {
	let b = &m.base;
	if b.generation == [0; 16]
		|| b.cluster == [0; 16]
		|| b.initial_epoch == 0
		|| b.admin_id == [0; 16]
		|| m.governance.id == [0; 16]
		|| m.recovery.id == [0; 16]
	{
		return Err(AdminManifestError2::ZeroField);
	}
	check_fields(b)?;
	for (pair, aid, ak, bid, bk) in [
		(
			RolePair3::AdminGovernance,
			b.admin_id,
			b.admin_key,
			m.governance.id,
			m.governance.key,
		),
		(
			RolePair3::AdminRecovery,
			b.admin_id,
			b.admin_key,
			m.recovery.id,
			m.recovery.key,
		),
		(
			RolePair3::GovernanceRecovery,
			m.governance.id,
			m.governance.key,
			m.recovery.id,
			m.recovery.key,
		),
	] {
		if aid == bid {
			return Err(AdminManifestError2::ReuseThree {
				pair,
				field: ReuseField3::Id,
			});
		}
		if ak == bk {
			return Err(AdminManifestError2::ReuseThree {
				pair,
				field: ReuseField3::Key,
			});
		}
	}
	for (role, key) in [
		(ManifestRole3::Admin, b.admin_key),
		(ManifestRole3::Governance, m.governance.key),
		(ManifestRole3::Recovery, m.recovery.key),
	] {
		check_enrollable_admin_key2(&key).map_err(|_| AdminManifestError2::KeyThree(role))?;
	}
	Ok(())
}
impl AdminManifestThree3 {
	pub(in crate::storage::mutation_visibility::root2) fn new(
		base: AdminManifest2,
		governance: GovernanceRole2,
		recovery: RecoveryRole3,
	) -> AdminResult2<Self> {
		let m = Self {
			base,
			governance,
			recovery,
		};
		check_three(&m)?;
		Ok(m)
	}
	pub(in crate::storage::mutation_visibility::root2) fn base(&self) -> &AdminManifest2 {
		&self.base
	}
	pub(in crate::storage::mutation_visibility::root2) fn governance(&self) -> GovernanceRole2 {
		self.governance
	}
	pub(in crate::storage::mutation_visibility::root2) fn recovery(&self) -> RecoveryRole3 {
		self.recovery
	}
	pub(in crate::storage::mutation_visibility::root2) fn decode(
		input: &[u8],
	) -> AdminResult2<Self> {
		profile_of_three(input)?;
		if array::<16>(input, 12) == [0; 16]
			|| array::<16>(input, 28) == [0; 16]
			|| u64_at(input, 44) == 0
			|| array::<16>(input, 52) == [0; 16]
			|| array::<16>(input, 137) == [0; 16]
			|| array::<16>(input, 189) == [0; 16]
		{
			return Err(AdminManifestError2::ZeroField);
		}
		for (at, mask, role) in [
			(100, ACTIONS, ManifestRole3::Admin),
			(185, GOVERNANCE_ACTIONS, ManifestRole3::Governance),
			(237, RECOVERY_ACTIONS3, ManifestRole3::Recovery),
		] {
			if u32::from_le_bytes(array(input, at)) != mask {
				return Err(AdminManifestError2::ActionThree(role));
			}
		}
		let scope = match input[112] {
			0 => MutationScope::Raw,
			1 => MutationScope::Table,
			_ => return Err(AdminManifestError2::Namespace),
		};
		let m = Self {
			base: AdminManifest2 {
				generation: array(input, 12),
				cluster: array(input, 28),
				initial_epoch: u64_at(input, 44),
				admin_id: array(input, 52),
				admin_key: array(input, 68),
				namespace: AdminNamespace2 {
					tenant: u64_at(input, 104),
					scope,
					table: array(input, 113),
					schema: u64_at(input, 129),
				},
			},
			governance: GovernanceRole2 {
				id: array(input, 137),
				key: array(input, 153),
			},
			recovery: RecoveryRole3 {
				id: array(input, 189),
				key: array(input, 205),
			},
		};
		check_three(&m)?;
		if m.encode()?[..] != *input {
			return Err(AdminManifestError2::NotCanonical);
		}
		Ok(m)
	}
	pub(in crate::storage::mutation_visibility::root2) fn encode(
		&self,
	) -> AdminResult2<[u8; ADMIN_MANIFEST_P3_BYTES3]> {
		// `check_three` covers every base check, so the base layout is written here directly.
		check_three(self)?;
		let base = &self.base;
		let mut b = [0; ADMIN_MANIFEST_P3_BYTES3];
		b[0..8].copy_from_slice(MAGIC);
		b[8..10].copy_from_slice(&1u16.to_le_bytes());
		b[10..12].copy_from_slice(&3u16.to_le_bytes());
		b[12..28].copy_from_slice(&base.generation);
		b[28..44].copy_from_slice(&base.cluster);
		b[44..52].copy_from_slice(&base.initial_epoch.to_le_bytes());
		b[52..68].copy_from_slice(&base.admin_id);
		b[68..100].copy_from_slice(&base.admin_key);
		b[100..104].copy_from_slice(&ACTIONS.to_le_bytes());
		b[104..112].copy_from_slice(&base.namespace.tenant.to_le_bytes());
		b[112] = base.namespace.scope as u8;
		b[113..129].copy_from_slice(&base.namespace.table);
		b[129..137].copy_from_slice(&base.namespace.schema.to_le_bytes());
		b[137..153].copy_from_slice(&self.governance.id);
		b[153..185].copy_from_slice(&self.governance.key);
		b[185..189].copy_from_slice(&GOVERNANCE_ACTIONS.to_le_bytes());
		b[189..205].copy_from_slice(&self.recovery.id);
		b[205..237].copy_from_slice(&self.recovery.key);
		b[237..241].copy_from_slice(&RECOVERY_ACTIONS3.to_le_bytes());
		Ok(b)
	}
}
