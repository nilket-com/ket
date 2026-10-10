//! Summaries derived only from fully validated state; never wire authority.
use super::admission_registry::PERMISSION_SECTION_BYTES2;
use super::{codec::*, command::Command2, persistent::Map, types::*, validation::*};
#[derive(Clone, PartialEq, Eq, Debug)]
pub(super) struct Derived2 {
	pub usage: Usage2,
	pub nodes: Map<NodeRef2, ()>,
	pub non_grants: Map<u64, ()>,
	pub originals: Map<Vec<u8>, Vec<u8>>,
}
impl Default for Derived2 {
	fn default() -> Self {
		Self {
			usage: Usage2 {
				parents: 0,
				held: 0,
				history: 0,
				registry: 0,
			},
			nodes: Map::new(),
			non_grants: Map::new(),
			originals: Map::new(),
		}
	}
}
fn add(a: usize, b: usize) -> Result2<usize> {
	a.checked_add(b).ok_or(RootError2::Overflow)
}
impl Derived2 {
	pub fn build(s: &State2) -> Result2<Self> {
		validate_state(s)?;
		let mut d = Self {
			usage: usage(s)?,
			..Self::default()
		};
		d.non_grants.insert(s.bootstrap.index, ());
		for a in s.selectors.values() {
			d.non_grants.insert(a.enrolled.index, ());
		}
		for a in s.authorities.values() {
			d.nodes.insert(a.node, ());
			d.non_grants.insert(a.enrolled.index, ());
			if let Some(l) = a.revoked {
				d.non_grants.insert(l.index, ());
			}
		}
		for g in s.lineages.values() {
			d.nodes.insert(
				NodeRef2 {
					node: g.latest.lease.holder,
					incarnation: g.latest.incarnation,
				},
				(),
			);
			if let Some(c) = g.closure.filter(|c| c.cause == 1) {
				d.non_grants.insert(c.log.index, ());
			}
		}
		for c in &s.cuts {
			d.non_grants.insert(c.log.index, ());
		}
		for p in s.permissions.values() {
			d.non_grants.insert(p.enrolled.index, ());
			if let Some(l) = p.revoked {
				d.non_grants.insert(l.index, ());
			}
		}
		for p in s.parents.values() {
			d.parent(p)?;
			d.non_grants.insert(p.reservation.index, ());
			if let Some(t) = p.terminal {
				d.non_grants.insert(t.first().index, ());
			}
		}
		Ok(d)
	}
	fn parent(&mut self, p: &super::stored::StoredParent2) -> Result2<()> {
		if self.non_grants.contains_key(&p.claim.original.log.index) {
			return Err(RootError2::RootInvariant);
		}
		let key = p.claim.original.reference()?.bytes()?;
		let value = p.claim.original.bytes()?;
		if self.originals.get(&key).is_some_and(|old| old != &value) {
			return Err(RootError2::RootInvariant);
		}
		self.originals.insert(key, value);
		for plan in &p.claim.plans {
			for n in &plan.nodes {
				self.nodes.insert(*n, ());
			}
		}
		Ok(())
	}
}
pub(super) fn cached_capacity(s: &State2, l: Limits2) -> Result2<()> {
	let u = s.derived.usage;
	if u.parents > l.slots
		|| u.held > l.parent
		|| u.history > l.history
		|| u.registry > l.registry
		|| s.lineages.len() > l.lineages
		|| s.cuts.len() > l.cuts
		|| s.selectors.len() > l.registrations
		|| s.authorities.len() > l.registrations
		|| s.permissions.len() > l.permissions
		|| permission_section_bytes(s.permissions.len())? > PERMISSION_SECTION_BYTES2
	{
		return Err(RootError2::Limit);
	}
	if add(add(add(ROOT_BASE2, u.held)?, u.history)?, u.registry)? > ROOT_BYTES2 {
		return Err(RootError2::Limit);
	}
	Ok(())
}
pub(super) fn update(old: &State2, next: &mut State2, cmd: &Command2) -> Result2<()> {
	let mut d = old.derived.clone();
	match cmd {
		Command2::EnrollSelector(b) => {
			let key = b.encode().map_err(|_| RootError2::Codec)?;
			if !old.selectors.contains_key(&key) {
				if let Some(a) = next.selectors.get(&key) {
					d.usage.registry = add(d.usage.registry, 68)?;
					d.non_grants.insert(a.enrolled.index, ());
				}
			}
		}
		Command2::EnrollAuthority { authority, .. } => {
			let key = authority_key(*authority)?;
			if !old.authorities.contains_key(&key) {
				if let Some(a) = next.authorities.get(&key) {
					d.usage.registry = add(d.usage.registry, 121)?;
					d.nodes.insert(a.node, ());
					d.non_grants.insert(a.enrolled.index, ());
				}
			}
		}
		Command2::RevokeAuthority(authority) => {
			let key = authority_key(*authority)?;
			if old
				.authorities
				.get(&key)
				.is_some_and(|a| a.revoked.is_none())
			{
				if let Some(l) = next.authorities.get(&key).and_then(|a| a.revoked) {
					d.non_grants.insert(l.index, ());
				}
			}
		}
		Command2::Grant { lineage, .. } => {
			if !old.lineages.contains_key(lineage) {
				if let Some(g) = next.lineages.get(lineage) {
					d.usage.history = add(d.usage.history, 298)?;
					d.nodes.insert(
						NodeRef2 {
							node: g.latest.lease.holder,
							incarnation: g.latest.incarnation,
						},
						(),
					);
				}
			}
		}
		Command2::Release { lineage, .. } => {
			if old
				.lineages
				.get(lineage)
				.is_some_and(|g| g.closure.is_none())
			{
				if let Some(c) = next.lineages.get(lineage).and_then(|g| g.closure) {
					d.non_grants.insert(c.log.index, ());
				}
			}
		}
		Command2::Cut { .. } => {
			if next.cuts.len() > old.cuts.len() {
				d.usage.history = add(d.usage.history, 72)?;
				d.non_grants.insert(
					next.cuts.last().ok_or(RootError2::RootInvariant)?.log.index,
					(),
				);
			}
		}
		Command2::Reserve(c) => {
			let key = c
				.manifest
				.identity
				.encode()
				.map_err(|_| RootError2::Codec)?;
			if !old.parents.contains_key(&key) {
				if let Some(p) = next.parents.get(&key) {
					valid_parent(p)?;
					if p.claim.original.log.index < next.bootstrap.index {
						return Err(RootError2::RootInvariant);
					}
					d.parent(p)?;
					d.usage.parents = add(d.usage.parents, 1)?;
					d.usage.held = add(
						d.usage.held,
						usize::try_from(p.held_bytes).map_err(|_| RootError2::Overflow)?,
					)?;
					d.non_grants.insert(p.reservation.index, ());
				}
			}
		}
		Command2::Finalize { identity, .. } | Command2::Resolve { identity, .. } => {
			let key = identity.encode().map_err(|_| RootError2::Codec)?;
			if old.parents.get(&key).is_some_and(|p| p.terminal.is_none()) {
				if let Some(p) = next.parents.get(&key) {
					if let Some(t) = p.terminal {
						valid_parent(p)?;
						d.non_grants.insert(t.first().index, ());
					}
				}
			}
		}
		Command2::Bootstrap { .. } | Command2::Cover { .. } => {}
	}
	next.derived = d;
	cached_capacity(next, Limits2::default())?;
	if next.derived.nodes.len() > 256 {
		return Err(RootError2::Limit);
	}
	Ok(())
}
