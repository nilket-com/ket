use super::admission_registry::{
	PERMISSION_CHARGE2, PERMISSION_RECORD_BYTES2, PERMISSION_SECTION_BYTES2, PERMISSIONS2,
	PermissionLimits2, PermissionMap2, RegistryError2, decode_permission_section2,
	encode_permission_key2, encode_permission_record2, permission_limits2,
};
use super::{codec::*, trust::TrustedView2, types::*, validation::*};
use super::{
	incremental::Derived2,
	persistent::Map,
	stored::{Cuts2, StoredParent2},
};
use crate::storage::mutation_visibility::MutationIdentity;
#[derive(Clone)]
pub struct Root2 {
	pub(super) state: State2,
	pub(super) limits: Limits2,
}
/// Parsed bytes are never authority or a ready snapshot.
pub struct UnverifiedRoot2 {
	state: State2,
}
impl std::fmt::Debug for Root2 {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str("Root2 { redacted }")
	}
}
impl std::fmt::Debug for UnverifiedRoot2 {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str("UnverifiedRoot2 { redacted }")
	}
}
// One layout drives both checked sizing and final writing. Record-sized Wire2 temporaries
// remain; neither pass stages a whole body or a permission-section Vec.
fn emit_log(l: Log2, emit: &mut impl FnMut(&[u8])) -> Result2<()> {
	valid_log(l)?;
	emit(&l.term.to_le_bytes());
	emit(&l.node.to_le_bytes());
	emit(&l.index.to_le_bytes());
	Ok(())
}
fn emit_records<'a, T: Wire2 + 'a>(
	xs: impl Iterator<Item = &'a T>,
	count: usize,
	emit: &mut impl FnMut(&[u8]),
) -> Result2<()> {
	emit(
		&u32::try_from(count)
			.map_err(|_| RootError2::Overflow)?
			.to_le_bytes(),
	);
	for x in xs {
		let bytes = x.bytes()?;
		emit(
			&u32::try_from(bytes.len())
				.map_err(|_| RootError2::Overflow)?
				.to_le_bytes(),
		);
		emit(&bytes);
	}
	Ok(())
}
// The registry's entrypoint set is frozen. Stream its existing key/record codecs
// privately here, preserving the original section bounds/key/record refusal order.
fn visit_permission_section2(
	m: &PermissionMap2,
	l: PermissionLimits2,
	emit: &mut impl FnMut(&[u8]),
) -> Result<(), RegistryError2> {
	if l.count > PERMISSIONS2
		|| l.section > PERMISSION_SECTION_BYTES2
		|| l.registry > REGISTRY_BYTES2
	{
		return Err(RegistryError2::Limits);
	}
	if m.len() > l.count {
		return Err(RegistryError2::Count);
	}
	let n = m
		.len()
		.checked_mul(PERMISSION_CHARGE2)
		.and_then(|n| n.checked_add(4))
		.ok_or(RegistryError2::Overflow)?;
	if n > l.section {
		return Err(RegistryError2::Count);
	}
	emit(&(m.len() as u32).to_le_bytes());
	for (k, r) in m {
		if *k != encode_permission_key2(&r.key)? {
			return Err(RegistryError2::MapKey);
		}
		emit(&(PERMISSION_RECORD_BYTES2 as u32).to_le_bytes());
		emit(&encode_permission_record2(r)?);
	}
	Ok(())
}
fn emit_state_body(s: &State2, u: &Usage2, emit: &mut impl FnMut(&[u8])) -> Result2<()> {
	emit(&s.generation);
	emit(&s.cluster);
	emit_log(s.bootstrap, emit)?;
	emit_log(s.covered, emit)?;
	emit(&s.fingerprint);
	emit(&s.command_digest);
	emit(&s.initial_epoch.to_le_bytes());
	emit(&s.control_digest);
	emit(&s.control_len.to_le_bytes());
	emit(&(u.parents as u32).to_le_bytes());
	emit(&(u.held as u64).to_le_bytes());
	emit(&(u.history as u64).to_le_bytes());
	emit(&(u.registry as u64).to_le_bytes());
	emit_records(s.selectors.values(), s.selectors.len(), emit)?;
	emit_records(s.authorities.values(), s.authorities.len(), emit)?;
	emit_records(s.issuers.values(), s.issuers.len(), emit)?;
	visit_permission_section2(&s.permissions, permission_limits2(), emit).map_err(section_error)?;
	emit_records(s.lineages.values(), s.lineages.len(), emit)?;
	emit_records(s.cuts.iter(), s.cuts.len(), emit)?;
	emit_records(s.parents.values(), s.parents.len(), emit)
}
fn envelope_size(body: usize, max: usize) -> Result2<(u32, usize)> {
	// Match envelope_versioned's Limit-before-Overflow refusal order.
	if body > max {
		return Err(RootError2::Limit);
	}
	let wire = u32::try_from(body).map_err(|_| RootError2::Overflow)?;
	let total = body.checked_add(48).ok_or(RootError2::Overflow)?;
	Ok((wire, total))
}
fn encode_state(s: &State2) -> Result2<Vec<u8>> {
	validate_state(s)?;
	let u = usage(s)?;
	let mut body = Some(0usize);
	// Finish record validation in its original order before the envelope length refusal.
	emit_state_body(s, &u, &mut |part| {
		body = body.and_then(|n| n.checked_add(part.len()));
	})?;
	let body = body.ok_or(RootError2::Overflow)?;
	let (wire, total) = envelope_size(body, ROOT_BYTES2)?;
	let mut b = Vec::with_capacity(total);
	b.extend_from_slice(b"VISAUT04");
	b.extend_from_slice(&4u16.to_le_bytes());
	b.extend_from_slice(&[1, 0]);
	b.extend_from_slice(&wire.to_le_bytes());
	emit_state_body(s, &u, &mut |part| b.extend_from_slice(part))?;
	if b.len() != total - 32 {
		return Err(RootError2::Codec);
	}
	let sum = hash(b"", &b);
	b.extend_from_slice(&sum);
	if b.len() != total {
		return Err(RootError2::Codec);
	}
	Ok(b)
}
#[cfg(test)]
#[path = "tests/encode_reference.rs"]
mod reference;
#[cfg(test)]
pub(super) fn encode_state_reference(s: &State2) -> Result2<Vec<u8>> {
	reference::encode_state_reference(s)
}
// Structural section faults: a count above the absolute cap is a Limit, anything else Codec.
fn section_error(e: RegistryError2) -> RootError2 {
	match e {
		RegistryError2::Count => RootError2::Limit,
		_ => RootError2::Codec,
	}
}
// The exact permission section, count word included, bounded by its declared count first.
fn read_permissions(r: &mut Read2<'_>) -> Result2<PermissionMap2> {
	let head = r.remaining().get(..4).ok_or(RootError2::Codec)?;
	let count = u32::from_le_bytes(head.try_into().map_err(|_| RootError2::Codec)?) as usize;
	if count > PERMISSIONS2 {
		return Err(RootError2::Limit);
	}
	let n = count
		.checked_mul(PERMISSION_CHARGE2)
		.and_then(|n| n.checked_add(4))
		.ok_or(RootError2::Overflow)?;
	decode_permission_section2(r.take(n)?, permission_limits2()).map_err(section_error)
}
fn sorted_map<K: Ord + Clone, T: Clone>(
	xs: Vec<T>,
	key: impl Fn(&T) -> Result2<K>,
) -> Result2<Map<K, T>> {
	let mut result = Map::new();
	for x in xs {
		let k = key(&x)?;
		if result.last_key_value().is_some_and(|(p, _)| p >= &k) {
			return Err(RootError2::Codec);
		}
		result.insert(k, x);
	}
	Ok(result)
}
impl UnverifiedRoot2 {
	/// Scalars only (bootstrap, covered, fingerprint, command digest): no root, state or authority.
	pub(in crate::storage::mutation_visibility::root2) fn origin_anchor_fields2(
		&self,
	) -> (Log2, Log2, Digest2, Digest2) {
		(
			self.state.bootstrap,
			self.state.covered,
			self.state.fingerprint,
			self.state.command_digest,
		)
	}
	#[cfg(test)]
	pub(super) fn fixture_fields(&self) -> ([u8; 16], [u8; 16], Log2, Digest2) {
		(
			self.state.generation,
			self.state.cluster,
			self.state.covered,
			self.state.fingerprint,
		)
	}
	pub fn decode(bytes: &[u8]) -> Result2<Self> {
		let mut r = Read2::new(open_versioned(bytes, b"VISAUT04", 4, ROOT_BYTES2)?);
		let generation = r.array()?;
		let cluster = r.array()?;
		let bootstrap = r.log()?;
		let covered = r.log()?;
		let fingerprint = r.array()?;
		let command_digest = r.array()?;
		let initial_epoch = r.u64()?;
		let control_digest = r.array()?;
		let control_len = r.u64()?;
		let saved = Usage2 {
			parents: r.u32()? as usize,
			held: usize::try_from(r.u64()?).map_err(|_| RootError2::Overflow)?,
			history: usize::try_from(r.u64()?).map_err(|_| RootError2::Overflow)?,
			registry: usize::try_from(r.u64()?).map_err(|_| RootError2::Overflow)?,
		};
		let selectors = sorted_map(read_vec::<Selector2>(&mut r, REGISTRY_COUNT2, 64)?, |s| {
			s.binding.encode().map_err(|_| RootError2::Codec)
		})?;
		let authorities = sorted_map(
			read_vec::<AuthorityRegistration2>(&mut r, REGISTRY_COUNT2, 117)?,
			|a| authority_key(a.authority),
		)?;
		let issuers = sorted_map(
			read_vec::<IssuerRegistration2>(&mut r, REGISTRY_COUNT2, 77)?,
			|a| Ok(a.node.to_be_bytes()),
		)?;
		let permissions = read_permissions(&mut r)?;
		let lineages = sorted_map(read_vec::<LineageRecord2>(&mut r, LINEAGES2, 294)?, |g| {
			Ok(g.latest.lineage)
		})?;
		let cuts = Cuts2::from_parsed(read_vec::<EpochCut2>(&mut r, CUTS2, 68)?);
		let parents = sorted_map(
			read_vec::<StoredParent2>(&mut r, PARENT_SLOTS2, CLAIM_BYTES2 + 107)?,
			|p| {
				p.claim
					.manifest
					.identity
					.encode()
					.map_err(|_| RootError2::Codec)
			},
		)?;
		r.end()?;
		let mut state = State2 {
			generation,
			cluster,
			bootstrap,
			covered,
			fingerprint,
			command_digest,
			initial_epoch,
			control_digest,
			control_len,
			selectors,
			authorities,
			issuers,
			permissions,
			lineages,
			cuts,
			parents,
			derived: Derived2::default(),
		};
		// Build runs the full validator before deriving any trusted summary.
		state.derived = Derived2::build(&state)?;
		state.cuts.seal_validated();
		if state.derived.usage != saved {
			return Err(RootError2::RootInvariant);
		}
		Ok(Self { state })
	}
	pub fn encode(&self) -> Result2<Vec<u8>> {
		encode_state(&self.state)
	}
	/// Fake provenance install; never compiled in ordinary builds.
	#[cfg(test)]
	pub(super) fn fixture_install(
		self,
		g: [u8; 16],
		c: [u8; 16],
		covered: Log2,
		fingerprint: Digest2,
		artifact_digest: Digest2,
	) -> Result2<Root2> {
		if (
			self.state.generation,
			self.state.cluster,
			self.state.covered,
			self.state.fingerprint,
		) != (g, c, covered, fingerprint)
			|| hash(b"", &self.encode()?) != artifact_digest
		{
			return Err(RootError2::TrustMismatch);
		}
		Ok(Root2 {
			state: self.state,
			limits: Limits2::default(),
		})
	}
}
impl Root2 {
	pub fn encode(&self) -> Result2<Vec<u8>> {
		encode_state(&self.state)
	}
	pub fn usage(&self) -> Result2<Usage2> {
		Ok(self.state.derived.usage)
	}
	pub fn covered(&self) -> Log2 {
		self.state.covered
	}
	pub fn lookup(&self, id: &MutationIdentity) -> Result2<Option<ParentRecord2>> {
		if id.generation != self.state.generation {
			return Err(RootError2::TrustMismatch);
		}
		let k = id.encode().map_err(|_| RootError2::Codec)?;
		Ok(self.state.parents.get(&k).map(StoredParent2::record))
	}
	pub fn lookup_at(
		&self,
		id: &MutationIdentity,
		view: &TrustedView2,
	) -> Result2<Option<ParentRecord2>> {
		let (g, c, b, k) = view.fields();
		if (g, c, b)
			!= (
				self.state.generation,
				self.state.cluster,
				self.state.bootstrap,
			) || k < b.index
			|| k > self.state.covered.index
		{
			return Err(RootError2::TrustMismatch);
		}
		let Some(mut p) = self.lookup(id)? else {
			return Ok(None);
		};
		if p.reservation.index > k {
			return Ok(None);
		}
		if p.terminal.is_some_and(|t| t.first().index > k) {
			p.terminal = None
		}
		Ok(Some(p))
	}
	pub fn epoch_at(&self, view: &TrustedView2) -> Result2<u64> {
		let (g, c, b, k) = view.fields();
		if (g, c, b)
			!= (
				self.state.generation,
				self.state.cluster,
				self.state.bootstrap,
			) || k < b.index
			|| k > self.state.covered.index
		{
			return Err(RootError2::TrustMismatch);
		}
		Ok(self
			.state
			.cuts
			.at(k)
			.map_or(self.state.initial_epoch, |c| c.new))
	}
	#[cfg(test)]
	pub(super) fn lower_limits(mut self, l: Limits2) -> Self {
		let d = Limits2::default();
		assert!(
			l.slots <= d.slots
				&& l.parent <= d.parent
				&& l.history <= d.history
				&& l.registry <= d.registry
				&& l.lineages <= d.lineages
				&& l.cuts <= d.cuts
				&& l.registrations <= d.registrations
				&& l.permissions <= d.permissions
		);
		self.limits = l;
		self
	}
}

#[cfg(test)]
#[path = "tests/encoding_size.rs"]
mod sizing_tests;
