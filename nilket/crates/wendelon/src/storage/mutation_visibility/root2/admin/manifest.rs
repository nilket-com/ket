use super::errors::{AdminManifestError2, AdminResult2, ManifestRole3, ReuseField3, RolePair3};
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

// ---- Profile 2 (additive and dormant: no production caller) ----
// A profile-2 manifest is the profile-1 layout with `profile = 2` followed by a governance role:
// `governance_id` [137..153), `governance_key` [153..185), `governance_actions` u32 [185..189).
// Nothing here grants governance authority: a decoded manifest is data, not a provisioned root,
// request or pin, and the profile-1 API and every consumer are unchanged.

pub(in crate::storage::mutation_visibility::root2) const ADMIN_MANIFEST_P2_BYTES2: usize = 189;
const PROFILE_ONE: u16 = 1;
const PROFILE_TWO: u16 = 2;
const GOVERNANCE_ACTIONS: u32 = 0x1FF;

/// The profile identifier only; it carries no role data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum ManifestProfile2 {
	One,
	Two,
	Three,
}

/// The governance role of a profile-2 manifest; its action mask is fixed (`0x1FF`).
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

/// A complete profile-2 manifest: the Admission fields plus the governance role.
#[derive(Clone, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct AdminManifestTwo2 {
	base: AdminManifest2,
	governance: GovernanceRole2,
}
redacted_debug!(AdminManifestTwo2, "AdminManifestTwo2");

/// Full validation of a base and a role (constructor and encode order): the base fields
/// (`ZeroField`, then `Namespace`), the governance id (`ZeroField`), `RoleReuse`, then
/// `NonCanonicalKey` (the Admission key, then the governance key). Decode checks every zero field
/// and both action masks before this, so its own order is the documented 13 steps.
fn check_two(base: &AdminManifest2, governance: &GovernanceRole2) -> AdminResult2<()> {
	check_fields(base)?;
	if governance.id == [0; 16] {
		return Err(AdminManifestError2::ZeroField);
	}
	if governance.id == base.admin_id || governance.key == base.admin_key {
		return Err(AdminManifestError2::RoleReuse);
	}
	check_enrollable_admin_key2(&base.admin_key)?;
	check_enrollable_admin_key2(&governance.key)
}

/// Steps 1-6 of the profile-2 decode order: the read bound, a length in {137, 189} before any
/// header access, magic, format, profile in {1, 2}, then profile/length pairing.
fn profile_of(input: &[u8]) -> AdminResult2<ManifestProfile2> {
	if input.len() > ADMIN_MANIFEST_READ_BOUND2 {
		return Err(AdminManifestError2::OverReadBound);
	}
	if input.len() != ADMIN_MANIFEST_BYTES2 && input.len() != ADMIN_MANIFEST_P2_BYTES2 {
		return Err(AdminManifestError2::Length);
	}
	if &input[0..8] != MAGIC {
		return Err(AdminManifestError2::Magic);
	}
	if u16::from_le_bytes(array(input, 8)) != 1 {
		return Err(AdminManifestError2::UnsupportedFormat);
	}
	let profile = match u16::from_le_bytes(array(input, 10)) {
		PROFILE_ONE => ManifestProfile2::One,
		PROFILE_TWO => ManifestProfile2::Two,
		_ => return Err(AdminManifestError2::UnsupportedProfile),
	};
	let expected = match profile {
		ManifestProfile2::One => ADMIN_MANIFEST_BYTES2,
		ManifestProfile2::Two => ADMIN_MANIFEST_P2_BYTES2,
		ManifestProfile2::Three => return Err(AdminManifestError2::UnsupportedProfile),
	};
	if input.len() != expected {
		return Err(AdminManifestError2::ProfileLength);
	}
	Ok(profile)
}

/// Structural peek only (steps 1-6); no field, key or canonicality validation.
pub(in crate::storage::mutation_visibility::root2) fn manifest_profile2(
	input: &[u8],
) -> AdminResult2<ManifestProfile2> {
	profile_of(input)
}

impl AdminManifestTwo2 {
	pub(in crate::storage::mutation_visibility::root2) fn new(
		base: AdminManifest2,
		governance: GovernanceRole2,
	) -> AdminResult2<Self> {
		check_two(&base, &governance)?;
		Ok(Self { base, governance })
	}
	/// A borrowed descriptive Admission subview; never a profile-1 root, request or pin.
	pub(in crate::storage::mutation_visibility::root2) fn base(&self) -> &AdminManifest2 {
		&self.base
	}
	pub(in crate::storage::mutation_visibility::root2) fn governance(&self) -> GovernanceRole2 {
		self.governance
	}
	/// Order: steps 1-6 (`profile_of`); a profile-1 input -> `UnsupportedProfile`; `ZeroField`
	/// (header, then governance id); `UnsupportedAction` (Admission, then governance); `Namespace`;
	/// `RoleReuse`; `NonCanonicalKey` (Admission, then governance); `NotCanonical`.
	pub(in crate::storage::mutation_visibility::root2) fn decode(
		input: &[u8],
	) -> AdminResult2<Self> {
		if profile_of(input)? != ManifestProfile2::Two {
			return Err(AdminManifestError2::UnsupportedProfile);
		}
		let (generation, cluster, admin_id) =
			(array(input, 12), array(input, 28), array(input, 52));
		let initial_epoch = u64_at(input, 44);
		let governance_id: [u8; 16] = array(input, 137);
		if generation == [0; 16]
			|| cluster == [0; 16]
			|| initial_epoch == 0
			|| admin_id == [0; 16]
			|| governance_id == [0; 16]
		{
			return Err(AdminManifestError2::ZeroField);
		}
		if u32::from_le_bytes(array(input, 100)) != ACTIONS
			|| u32::from_le_bytes(array(input, 185)) != GOVERNANCE_ACTIONS
		{
			return Err(AdminManifestError2::UnsupportedAction);
		}
		let scope = match input[112] {
			0 => MutationScope::Raw,
			1 => MutationScope::Table,
			_ => return Err(AdminManifestError2::Namespace),
		};
		let base = AdminManifest2 {
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
		let governance = GovernanceRole2 {
			id: governance_id,
			key: array(input, 153),
		};
		check_two(&base, &governance)?;
		let manifest = Self { base, governance };
		if manifest.encode()?[..] != *input {
			return Err(AdminManifestError2::NotCanonical);
		}
		Ok(manifest)
	}
	/// Revalidates the complete base and role (`check_two`), then writes the profile-1 layout with
	/// `profile = 2` and the governance tail.
	pub(in crate::storage::mutation_visibility::root2) fn encode(
		&self,
	) -> AdminResult2<[u8; ADMIN_MANIFEST_P2_BYTES2]> {
		check_two(&self.base, &self.governance)?;
		let head = self.base.encode()?;
		let mut b = [0u8; ADMIN_MANIFEST_P2_BYTES2];
		b[..ADMIN_MANIFEST_BYTES2].copy_from_slice(&head);
		b[10..12].copy_from_slice(&PROFILE_TWO.to_le_bytes());
		b[137..153].copy_from_slice(&self.governance.id);
		b[153..185].copy_from_slice(&self.governance.key);
		b[185..189].copy_from_slice(&GOVERNANCE_ACTIONS.to_le_bytes());
		Ok(b)
	}
}

/// Digest math only (the profile-1 domain); not validation, provenance or a pin.
pub(in crate::storage::mutation_visibility::root2) fn admin_manifest_two_digest2(
	bytes: &[u8; ADMIN_MANIFEST_P2_BYTES2],
) -> Digest2 {
	hash(DOMAIN, bytes)
}

// ---- M2: either profile, with its exact canonical bytes (dormant beyond owner/inventory) ----

/// Either profile. Decoding dispatches structurally and then fully validates the selected
/// profile; there is no conversion between profiles.
#[derive(Clone, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum AnyAdminManifest2 {
	One(AdminManifest2),
	Two(AdminManifestTwo2),
	Three(AdminManifestThree3),
}
redacted_debug!(AnyAdminManifest2, "AnyAdminManifest2");

/// The exact canonical bytes of one manifest. Built only by `AnyAdminManifest2::encode`; the
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
	/// `manifest_profile2` (steps 1-6), then the selected profile's full decoder.
	pub(in crate::storage::mutation_visibility::root2) fn decode(
		input: &[u8],
	) -> AdminResult2<Self> {
		Ok(match profile_of_three(input)? {
			ManifestProfile2::One => Self::One(AdminManifest2::decode(input)?),
			ManifestProfile2::Two => Self::Two(AdminManifestTwo2::decode(input)?),
			ManifestProfile2::Three => Self::Three(AdminManifestThree3::decode(input)?),
		})
	}
	/// Revalidates the selected profile through its own encoder.
	pub(in crate::storage::mutation_visibility::root2) fn encode(
		&self,
	) -> AdminResult2<ManifestBytes2> {
		let mut storage = [0u8; ADMIN_MANIFEST_P3_BYTES3];
		let len = match self {
			Self::One(m) => {
				storage[..ADMIN_MANIFEST_BYTES2].copy_from_slice(&m.encode()?);
				ADMIN_MANIFEST_BYTES2
			}
			Self::Two(m) => {
				storage[..ADMIN_MANIFEST_P2_BYTES2].copy_from_slice(&m.encode()?);
				ADMIN_MANIFEST_P2_BYTES2
			}
			Self::Three(m) => {
				storage.copy_from_slice(&m.encode()?);
				ADMIN_MANIFEST_P3_BYTES3
			}
		};
		Ok(ManifestBytes2 { storage, len })
	}
	pub(in crate::storage::mutation_visibility::root2) fn profile(&self) -> ManifestProfile2 {
		match self {
			Self::One(_) => ManifestProfile2::One,
			Self::Two(_) => ManifestProfile2::Two,
			Self::Three(_) => ManifestProfile2::Three,
		}
	}
	/// The Admission fields, descriptive only: never a whole-manifest pin or request identity.
	pub(in crate::storage::mutation_visibility::root2) fn admission(&self) -> &AdminManifest2 {
		match self {
			Self::One(m) => m,
			Self::Two(m) => m.base(),
			Self::Three(m) => m.base(),
		}
	}
}

/// Digest math over exact canonical bytes (either profile, the same domain); not trust.
pub(in crate::storage::mutation_visibility::root2) fn any_manifest_digest2(
	bytes: &ManifestBytes2,
) -> Digest2 {
	hash(DOMAIN, bytes.as_slice())
}

// Profile 3 is additive: the profile-1/2 decoders and structural peek retain their exact errors.
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
fn profile_of_three(input: &[u8]) -> AdminResult2<ManifestProfile2> {
	if input.len() > ADMIN_MANIFEST_READ_BOUND2 {
		return Err(AdminManifestError2::OverReadBound);
	}
	if !matches!(
		input.len(),
		ADMIN_MANIFEST_BYTES2 | ADMIN_MANIFEST_P2_BYTES2 | ADMIN_MANIFEST_P3_BYTES3
	) {
		return Err(AdminManifestError2::Length);
	}
	if &input[..8] != MAGIC {
		return Err(AdminManifestError2::Magic);
	}
	if u16::from_le_bytes(array(input, 8)) != 1 {
		return Err(AdminManifestError2::UnsupportedFormat);
	}
	let (profile, len) = match u16::from_le_bytes(array(input, 10)) {
		1 => (ManifestProfile2::One, ADMIN_MANIFEST_BYTES2),
		2 => (ManifestProfile2::Two, ADMIN_MANIFEST_P2_BYTES2),
		3 => (ManifestProfile2::Three, ADMIN_MANIFEST_P3_BYTES3),
		_ => return Err(AdminManifestError2::UnsupportedProfile),
	};
	if input.len() != len {
		return Err(AdminManifestError2::ProfileLength);
	}
	Ok(profile)
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
		if profile_of_three(input)? != ManifestProfile2::Three {
			return Err(AdminManifestError2::UnsupportedProfile);
		}
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
