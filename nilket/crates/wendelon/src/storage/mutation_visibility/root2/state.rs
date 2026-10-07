use super::super::MutationIdentity;
use super::{codec::*, trust::TrustedView2, types::*, validation::*};
use super::{
	incremental::Derived2,
	persistent::Map,
	stored::{Cuts2, StoredParent2},
};
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
fn encode_state(s: &State2) -> Result2<Vec<u8>> {
	validate_state(s)?;
	let u = usage(s)?;
	let mut b = Vec::new();
	b.extend(s.generation);
	b.extend(s.cluster);
	log(&mut b, s.bootstrap)?;
	log(&mut b, s.covered)?;
	b.extend(s.fingerprint);
	b.extend(s.command_digest);
	b.extend(s.initial_epoch.to_le_bytes());
	b.extend(s.control_digest);
	b.extend(s.control_len.to_le_bytes());
	b.extend((u.parents as u32).to_le_bytes());
	b.extend((u.held as u64).to_le_bytes());
	b.extend((u.history as u64).to_le_bytes());
	b.extend((u.registry as u64).to_le_bytes());
	write_vec(&mut b, s.selectors.values().cloned(), s.selectors.len())?;
	write_vec(&mut b, s.authorities.values().cloned(), s.authorities.len())?;
	write_vec(&mut b, s.lineages.values().cloned(), s.lineages.len())?;
	write_vec(&mut b, s.cuts.iter().cloned(), s.cuts.len())?;
	write_vec(&mut b, s.parents.values().cloned(), s.parents.len())?;
	envelope(b"VISAUT02", &b, ROOT_BYTES2)
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
		let mut r = Read2::new(open(bytes, b"VISAUT02", ROOT_BYTES2)?);
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
		);
		self.limits = l;
		self
	}
}
