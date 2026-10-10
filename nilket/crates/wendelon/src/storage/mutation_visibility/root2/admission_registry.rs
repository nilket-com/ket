//! Dormant Admission permission registry: pure codecs, lifecycle transitions and accounting. The
//! transitions operate on a map already semantically validated by `validate_permissions2` (or a
//! successor produced by these transitions) and on a semantically valid pre-entry authority state;
//! that is a consistency precondition, not provenance. A section-decoded map is descriptive,
//! untrusted data. Nothing here authorizes an issuer, proves committed history or builds root
//! state; the issuer gate and root integration are held.
use super::super::{MutationScope, redacted_debug};
use super::admin_statement::{AdmissionNamespace2, PermissionKey2, ZeroField2};
use super::persistent::Map;
use super::types::{AuthorityRef2, AuthorityRegistration2, Log2, REGISTRY_BYTES2};
use super::validation::authority_key;

pub(in crate::storage::mutation_visibility::root2) const PERMISSION_KEY_BYTES2: usize = 73;
pub(in crate::storage::mutation_visibility::root2) const PERMISSION_RECORD_BYTES2: usize = 113;
pub(in crate::storage::mutation_visibility::root2) const PERMISSION_CHARGE2: usize = 117;
pub(in crate::storage::mutation_visibility::root2) const PERMISSIONS2: usize = 4096;
pub(in crate::storage::mutation_visibility::root2) const PERMISSION_SECTION_BYTES2: usize = 524288;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct PermissionRecord2 {
	pub key: PermissionKey2,
	pub enrolled: Log2,
	pub revoked: Option<Log2>,
}
redacted_debug!(PermissionRecord2, "PermissionRecord2");
pub(in crate::storage::mutation_visibility::root2) type PermissionMap2 =
	Map<[u8; PERMISSION_KEY_BYTES2], PermissionRecord2>;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct PermissionLimits2 {
	pub count: usize,
	pub section: usize,
	pub registry: usize,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum PermissionOutcome2 {
	Granted(PermissionRecord2),
	GrantDuplicate(PermissionRecord2),
	Revoked(PermissionRecord2),
	RevokeDuplicate(PermissionRecord2),
	UnknownAuthority,
	AuthorityRevoked(Log2),
	Capacity,
	UnknownPermission,
}
redacted_debug!(PermissionOutcome2, "PermissionOutcome2");
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct PermissionDelta2 {
	pub records: usize,
	pub registry: usize,
	pub coordinate: Option<u64>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct PermissionSummary2 {
	pub count: usize,
	pub registry: usize,
	pub section: usize,
	pub coordinates: Map<u64, ()>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum RegistryError2 {
	ZeroField(ZeroField2),
	Namespace,
	Length,
	Count,
	Framing,
	Order,
	Log,
	Coordinate,
	Generation,
	MapKey,
	Reference,
	NotLater,
	Accounting,
	Limits,
	Overflow,
}
type Result<T> = std::result::Result<T, RegistryError2>;
type Transition2 = (PermissionMap2, PermissionOutcome2, PermissionDelta2);

const NO_DELTA: PermissionDelta2 = PermissionDelta2 {
	records: 0,
	registry: 0,
	coordinate: None,
};
const KEY: usize = PERMISSION_KEY_BYTES2;
const RECORD: usize = PERMISSION_RECORD_BYTES2;

fn array<const N: usize>(b: &[u8], at: usize) -> [u8; N] {
	b[at..at + N].try_into().expect("fixed registry offset")
}
fn u64_at(b: &[u8], at: usize) -> u64 {
	u64::from_le_bytes(array(b, at))
}
fn check_limits(l: PermissionLimits2) -> Result<()> {
	if l.count > PERMISSIONS2
		|| l.section > PERMISSION_SECTION_BYTES2
		|| l.registry > REGISTRY_BYTES2
	{
		return Err(RegistryError2::Limits);
	}
	Ok(())
}
fn section_bytes(count: usize) -> Result<usize> {
	count
		.checked_mul(PERMISSION_CHARGE2)
		.and_then(|n| n.checked_add(4))
		.ok_or(RegistryError2::Overflow)
}
fn valid_log(l: Log2) -> bool {
	l.term != 0 && l.node != 0 && l.index != 0
}
// The statement key order: zero fields first, then the namespace shape.
fn key_zeros(k: &PermissionKey2) -> Result<()> {
	for (zero, field) in [
		(k.authority.id == [0; 16], ZeroField2::AuthorityId),
		(k.authority.revision == 0, ZeroField2::AuthorityRevision),
		(
			k.namespace.generation == [0; 16],
			ZeroField2::NamespaceGeneration,
		),
	] {
		if zero {
			return Err(RegistryError2::ZeroField(field));
		}
	}
	Ok(())
}
fn key_shape(n: &AdmissionNamespace2) -> Result<()> {
	let shaped = match n.scope {
		MutationScope::Raw => n.table == [0; 16] && n.schema == 0,
		MutationScope::Table => n.table != [0; 16] && n.schema > 0,
	};
	if shaped {
		Ok(())
	} else {
		Err(RegistryError2::Namespace)
	}
}
fn write_key(k: &PermissionKey2) -> [u8; KEY] {
	let mut b = [0u8; KEY];
	b[0..16].copy_from_slice(&k.authority.id);
	b[16..24].copy_from_slice(&k.authority.revision.to_le_bytes());
	b[24..40].copy_from_slice(&k.namespace.generation);
	b[40..48].copy_from_slice(&k.namespace.tenant.to_le_bytes());
	b[48] = k.namespace.scope as u8;
	b[49..65].copy_from_slice(&k.namespace.table);
	b[65..73].copy_from_slice(&k.namespace.schema.to_le_bytes());
	b
}
fn write_log(b: &mut [u8], l: Option<Log2>) {
	if let Some(l) = l {
		b[0..8].copy_from_slice(&l.term.to_le_bytes());
		b[8..12].copy_from_slice(&l.node.to_le_bytes());
		b[12..20].copy_from_slice(&l.index.to_le_bytes());
	}
}
// All-zero is None; a partially zero coordinate is refused.
fn read_log(b: &[u8]) -> Result<Option<Log2>> {
	if b.iter().all(|x| *x == 0) {
		return Ok(None);
	}
	let l = Log2 {
		term: u64_at(b, 0),
		node: u32::from_le_bytes(array(b, 8)),
		index: u64_at(b, 12),
	};
	if valid_log(l) {
		Ok(Some(l))
	} else {
		Err(RegistryError2::Log)
	}
}
fn check_record(r: &PermissionRecord2) -> Result<()> {
	key_zeros(&r.key)?;
	key_shape(&r.key.namespace)?;
	if !valid_log(r.enrolled) || r.revoked.is_some_and(|x| !valid_log(x)) {
		return Err(RegistryError2::Log);
	}
	if r.revoked.is_some_and(|x| x.index <= r.enrolled.index) {
		return Err(RegistryError2::Coordinate);
	}
	Ok(())
}

pub(in crate::storage::mutation_visibility::root2) fn permission_limits2() -> PermissionLimits2 {
	PermissionLimits2 {
		count: PERMISSIONS2,
		section: PERMISSION_SECTION_BYTES2,
		registry: REGISTRY_BYTES2,
	}
}
/// Canonical 73 bytes, identical to statement bytes 200..273; refuses what decode refuses.
pub(in crate::storage::mutation_visibility::root2) fn encode_permission_key2(
	k: &PermissionKey2,
) -> Result<[u8; KEY]> {
	key_zeros(k)?;
	key_shape(&k.namespace)?;
	Ok(write_key(k))
}
pub(in crate::storage::mutation_visibility::root2) fn decode_permission_key2(
	b: &[u8],
) -> Result<PermissionKey2> {
	if b.len() != KEY {
		return Err(RegistryError2::Length);
	}
	let scope = match b[48] {
		0 => Some(MutationScope::Raw),
		1 => Some(MutationScope::Table),
		_ => None,
	};
	let k = PermissionKey2 {
		authority: AuthorityRef2 {
			id: array(b, 0),
			revision: u64_at(b, 16),
		},
		namespace: AdmissionNamespace2 {
			generation: array(b, 24),
			tenant: u64_at(b, 40),
			scope: scope.unwrap_or(MutationScope::Raw),
			table: array(b, 49),
			schema: u64_at(b, 65),
		},
	};
	// An unknown scope tag is a namespace refusal, after any zero-field refusal.
	key_zeros(&k)?;
	if scope.is_none() {
		return Err(RegistryError2::Namespace);
	}
	key_shape(&k.namespace)?;
	Ok(k)
}
pub(in crate::storage::mutation_visibility::root2) fn encode_permission_record2(
	r: &PermissionRecord2,
) -> Result<[u8; RECORD]> {
	check_record(r)?;
	let mut b = [0u8; RECORD];
	b[..KEY].copy_from_slice(&write_key(&r.key));
	write_log(&mut b[KEY..KEY + 20], Some(r.enrolled));
	write_log(&mut b[KEY + 20..], r.revoked);
	Ok(b)
}
pub(in crate::storage::mutation_visibility::root2) fn decode_permission_record2(
	b: &[u8],
) -> Result<PermissionRecord2> {
	if b.len() != RECORD {
		return Err(RegistryError2::Length);
	}
	let key = decode_permission_key2(&b[..KEY])?;
	let enrolled = read_log(&b[KEY..KEY + 20])?.ok_or(RegistryError2::Log)?;
	let revoked = read_log(&b[KEY + 20..])?;
	let r = PermissionRecord2 {
		key,
		enrolled,
		revoked,
	};
	check_record(&r)?;
	Ok(r)
}
/// Count, then framed records in the map's byte-key order. Bounds precede the one allocation.
pub(in crate::storage::mutation_visibility::root2) fn encode_permission_section2(
	m: &PermissionMap2,
	l: PermissionLimits2,
) -> Result<Vec<u8>> {
	check_limits(l)?;
	if m.len() > l.count {
		return Err(RegistryError2::Count);
	}
	let n = section_bytes(m.len())?;
	if n > l.section {
		return Err(RegistryError2::Count);
	}
	let mut b = Vec::with_capacity(n);
	b.extend((m.len() as u32).to_le_bytes());
	for (k, r) in m {
		if *k != encode_permission_key2(&r.key)? {
			return Err(RegistryError2::MapKey);
		}
		b.extend((RECORD as u32).to_le_bytes());
		b.extend(encode_permission_record2(r)?);
	}
	Ok(b)
}
/// Structural decode only: the result is untrusted data with no generation, coordinate or
/// authority relation established.
pub(in crate::storage::mutation_visibility::root2) fn decode_permission_section2(
	b: &[u8],
	l: PermissionLimits2,
) -> Result<PermissionMap2> {
	check_limits(l)?;
	if b.len() > l.section || b.len() < 4 {
		return Err(RegistryError2::Length);
	}
	let count = u32::from_le_bytes(array(b, 0)) as usize;
	if count > l.count {
		return Err(RegistryError2::Count);
	}
	if section_bytes(count)? != b.len() {
		return Err(RegistryError2::Length);
	}
	let mut m = PermissionMap2::new();
	let mut at = 4;
	for _ in 0..count {
		if u32::from_le_bytes(array(b, at)) as usize != RECORD {
			return Err(RegistryError2::Framing);
		}
		let r = decode_permission_record2(&b[at + 4..at + 4 + RECORD])?;
		let k = write_key(&r.key);
		if m.last_key_value().is_some_and(|(p, _)| *p >= k) {
			return Err(RegistryError2::Order);
		}
		m.insert(k, r);
		at += PERMISSION_CHARGE2;
	}
	Ok(m)
}
/// Semantic Grant at entry `at`. `registry_used` is the caller's TOTAL root registry usage.
pub(in crate::storage::mutation_visibility::root2) fn grant_permission2(
	m: &PermissionMap2,
	authority: Option<&AuthorityRegistration2>,
	key: &PermissionKey2,
	at: Log2,
	registry_used: usize,
	l: PermissionLimits2,
) -> Result<Transition2> {
	check_limits(l)?;
	let k = encode_permission_key2(key)?;
	if !valid_log(at) {
		return Err(RegistryError2::Log);
	}
	if authority.is_some_and(|a| a.authority != key.authority) {
		return Err(RegistryError2::Reference);
	}
	let permission_bytes = m
		.len()
		.checked_mul(PERMISSION_CHARGE2)
		.ok_or(RegistryError2::Overflow)?;
	if registry_used < permission_bytes {
		return Err(RegistryError2::Accounting);
	}
	if let Some(r) = m.get(&k) {
		if r.enrolled.index >= at.index || r.revoked.is_some_and(|x| x.index >= at.index) {
			return Err(RegistryError2::NotLater);
		}
		return Ok((m.clone(), PermissionOutcome2::GrantDuplicate(*r), NO_DELTA));
	}
	let Some(a) = authority else {
		return Ok((m.clone(), PermissionOutcome2::UnknownAuthority, NO_DELTA));
	};
	if a.enrolled.index >= at.index {
		return Err(RegistryError2::NotLater);
	}
	if let Some(r) = a.revoked {
		if r.index >= at.index {
			return Err(RegistryError2::NotLater);
		}
		return Ok((m.clone(), PermissionOutcome2::AuthorityRevoked(r), NO_DELTA));
	}
	let registry = registry_used
		.checked_add(PERMISSION_CHARGE2)
		.ok_or(RegistryError2::Overflow)?;
	let section = section_bytes(m.len().checked_add(1).ok_or(RegistryError2::Overflow)?)?;
	if m.len() >= l.count || section > l.section || registry > l.registry {
		return Ok((m.clone(), PermissionOutcome2::Capacity, NO_DELTA));
	}
	let record = PermissionRecord2 {
		key: *key,
		enrolled: at,
		revoked: None,
	};
	let mut next = m.clone();
	next.insert(k, record);
	Ok((
		next,
		PermissionOutcome2::Granted(record),
		PermissionDelta2 {
			records: 1,
			registry: PERMISSION_CHARGE2,
			coordinate: Some(at.index),
		},
	))
}
/// Semantic Revoke at entry `at`; no capacity check and no new record.
pub(in crate::storage::mutation_visibility::root2) fn revoke_permission2(
	m: &PermissionMap2,
	key: &PermissionKey2,
	at: Log2,
) -> Result<Transition2> {
	let k = encode_permission_key2(key)?;
	if !valid_log(at) {
		return Err(RegistryError2::Log);
	}
	let Some(r) = m.get(&k) else {
		return Ok((m.clone(), PermissionOutcome2::UnknownPermission, NO_DELTA));
	};
	if r.enrolled.index >= at.index || r.revoked.is_some_and(|x| x.index >= at.index) {
		return Err(RegistryError2::NotLater);
	}
	if r.revoked.is_some() {
		return Ok((m.clone(), PermissionOutcome2::RevokeDuplicate(*r), NO_DELTA));
	}
	let record = PermissionRecord2 {
		revoked: Some(at),
		..*r
	};
	let mut next = m.clone();
	next.insert(k, record);
	Ok((
		next,
		PermissionOutcome2::Revoked(record),
		PermissionDelta2 {
			records: 0,
			registry: 0,
			coordinate: Some(at.index),
		},
	))
}
fn claim(coords: &mut Map<u64, ()>, existing: &Map<u64, ()>, index: u64) -> Result<()> {
	if existing.contains_key(&index) || coords.contains_key(&index) {
		return Err(RegistryError2::Coordinate);
	}
	coords.insert(index, ());
	Ok(())
}
/// Full rebuild of the permission summary. It relies on the caller's valid authority map and
/// complete set of existing non-permission coordinates (bootstrap included); it validates neither
/// those authority entries nor the authentic inclusion of any coordinate.
pub(in crate::storage::mutation_visibility::root2) fn validate_permissions2(
	m: &PermissionMap2,
	generation: [u8; 16],
	bootstrap: Log2,
	covered: Log2,
	authorities: &Map<[u8; 24], AuthorityRegistration2>,
	existing: &Map<u64, ()>,
	l: PermissionLimits2,
) -> Result<PermissionSummary2> {
	use RegistryError2 as E;
	check_limits(l)?;
	if generation == [0; 16] {
		return Err(E::Generation);
	}
	if !valid_log(bootstrap) || !valid_log(covered) {
		return Err(E::Log);
	}
	if bootstrap.index > covered.index {
		return Err(E::Coordinate);
	}
	let count = m.len();
	let section = section_bytes(count)?;
	let registry = count.checked_mul(PERMISSION_CHARGE2).ok_or(E::Overflow)?;
	if count > l.count || section > l.section || registry > l.registry {
		return Err(E::Count);
	}
	let mut coordinates = Map::new();
	for (k, r) in m {
		if *k != encode_permission_key2(&r.key)? {
			return Err(E::MapKey);
		}
		if r.key.namespace.generation != generation {
			return Err(E::Generation);
		}
		if !valid_log(r.enrolled) || r.revoked.is_some_and(|x| !valid_log(x)) {
			return Err(E::Log);
		}
		let e = r.enrolled.index;
		if e <= bootstrap.index
			|| e > covered.index
			|| r.revoked
				.is_some_and(|x| x.index <= e || x.index > covered.index)
		{
			return Err(E::Coordinate);
		}
		claim(&mut coordinates, existing, e)?;
		if let Some(x) = r.revoked {
			claim(&mut coordinates, existing, x.index)?;
		}
		let a = authorities
			.get(&authority_key(r.key.authority).map_err(|_| E::Reference)?)
			.ok_or(E::Reference)?;
		if a.authority != r.key.authority
			|| a.enrolled.index >= e
			|| a.revoked.is_some_and(|x| x.index <= e)
		{
			return Err(E::Reference);
		}
	}
	Ok(PermissionSummary2 {
		count,
		registry,
		section,
		coordinates,
	})
}
/// Incremental summary update in O(log N); the input coordinate set is a documented
/// precondition and is not scanned.
pub(in crate::storage::mutation_visibility::root2) fn apply_permission_delta2(
	s: &PermissionSummary2,
	d: PermissionDelta2,
	l: PermissionLimits2,
) -> Result<PermissionSummary2> {
	use RegistryError2 as E;
	check_limits(l)?;
	let registry = s.count.checked_mul(PERMISSION_CHARGE2).ok_or(E::Overflow)?;
	let section = registry.checked_add(4).ok_or(E::Overflow)?;
	if s.registry != registry || s.section != section {
		return Err(E::Accounting);
	}
	if s.count > l.count || s.section > l.section || s.registry > l.registry {
		return Err(E::Count);
	}
	let coordinate = match (d.records, d.registry, d.coordinate) {
		(1, PERMISSION_CHARGE2, Some(c)) | (0, 0, Some(c)) => Some(c),
		(0, 0, None) => None,
		_ => return Err(E::Accounting),
	};
	if coordinate == Some(0) {
		return Err(E::Coordinate);
	}
	let count = s.count.checked_add(d.records).ok_or(E::Overflow)?;
	let registry = s.registry.checked_add(d.registry).ok_or(E::Overflow)?;
	let section = s.section.checked_add(d.registry).ok_or(E::Overflow)?;
	if count > l.count || registry > l.registry || section > l.section {
		return Err(E::Count);
	}
	let mut next = s.clone();
	if let Some(c) = coordinate {
		if next.coordinates.contains_key(&c) {
			return Err(E::Coordinate);
		}
		next.coordinates.insert(c, ());
	}
	next.count = count;
	next.registry = registry;
	next.section = section;
	Ok(next)
}
