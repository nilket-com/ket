use super::{codec::*, types::*};
use std::collections::BTreeSet;

pub(super) fn nonzero<const N: usize>(b: [u8; N]) -> Result2<()> {
	if b == [0; N] {
		Err(RootError2::Codec)
	} else {
		Ok(())
	}
}
pub(super) fn valid_hlc(h: Hlc2) -> Result2<()> {
	if h.physical_ms >= (1u64 << 48) || h.node == 0 {
		Err(RootError2::Codec)
	} else {
		Ok(())
	}
}
pub(super) fn valid_node(n: NodeRef2) -> Result2<()> {
	nonzero(n.incarnation)?;
	if n.node == 0 {
		Err(RootError2::Codec)
	} else {
		Ok(())
	}
}
pub(super) fn valid_authority(a: AuthorityRef2) -> Result2<()> {
	nonzero(a.id)?;
	if a.revision == 0 {
		Err(RootError2::Codec)
	} else {
		Ok(())
	}
}
pub(super) fn authority_key(a: AuthorityRef2) -> Result2<[u8; 24]> {
	a.bytes()?.try_into().map_err(|_| RootError2::Codec)
}
pub(super) fn valid_lease(l: &SignedLease2) -> Result2<()> {
	valid_hlc(l.start)?;
	valid_hlc(l.expiry)?;
	if l.epoch == 0
		|| l.holder == 0
		|| l.issuer == 0
		|| l.range_start > l.range_end
		|| l.start >= l.expiry
	{
		Err(RootError2::Codec)
	} else {
		Ok(())
	}
}
pub(super) fn valid_original(g: &OriginalGrant2) -> Result2<()> {
	nonzero(g.lineage)?;
	nonzero(g.incarnation)?;
	valid_lease(&g.lease)?;
	valid_log(g.log)?;
	if g.lease.min_commit_index > g.log.index {
		return Err(RootError2::Codec);
	}
	if let Some(p) = g.predecessor {
		p.bytes()?;
		if p.lineage == g.lineage || p.log.index >= g.log.index {
			return Err(RootError2::Codec);
		}
	}
	Ok(())
}
pub(super) fn valid_lineage(g: &LineageRecord2) -> Result2<()> {
	valid_original(&g.latest)?;
	if let Some(c) = g.closure {
		valid_log(c.log)?;
		if c.log.index <= g.latest.log.index
			|| !matches!((c.cause, c.successor), (1 | 3, None) | (2, Some(_)))
		{
			return Err(RootError2::Codec);
		}
		if let Some(n) = c.successor {
			nonzero(n)?;
			if n == g.latest.lineage {
				return Err(RootError2::Codec);
			}
		}
	}
	Ok(())
}
pub(super) fn ordered_nodes(ns: &[NodeRef2]) -> Result2<()> {
	let mut ids = BTreeSet::new();
	let mut inc = BTreeSet::new();
	let mut prev = None;
	for n in ns {
		valid_node(*n)?;
		if prev.is_some_and(|p| p >= *n) || !ids.insert(n.node) || !inc.insert(n.incarnation) {
			return Err(RootError2::Codec);
		}
		prev = Some(*n)
	}
	Ok(())
}
pub(super) fn valid_plan(p: &ReplicaPlan2) -> Result2<()> {
	if !matches!(p.nodes.len(), 1 | 3) {
		return Err(RootError2::Codec);
	}
	ordered_nodes(&p.nodes)
}
pub(super) fn valid_claim(c: &ClaimProjection2) -> Result2<()> {
	c.manifest.validate().map_err(|_| RootError2::Codec)?;
	valid_original(&c.original)?;
	valid_authority(c.authority)?;
	let m = &c.manifest;
	let l = &c.original.lease;
	if m.admission.epoch != l.epoch
		|| m.admission.holder != l.holder
		|| c.plans.len() != m.children.len()
	{
		return Err(RootError2::Codec);
	}
	let mut at = 0;
	for (ci, child) in m.children.iter().enumerate() {
		if child.range_start < l.range_start || child.range_end > l.range_end {
			return Err(RootError2::Codec);
		}
		let plan = &c.plans[ci];
		valid_plan(plan)?;
		if plan.child != ci as u32
			|| !plan.nodes.contains(&NodeRef2 {
				node: l.holder,
				incarnation: c.original.incarnation,
			}) {
			return Err(RootError2::Codec);
		}
		for ri in 0..child.versions.len() {
			let Some(s) = c.stamps.get(at) else {
				return Err(RootError2::Codec);
			};
			valid_hlc(s.hlc)?;
			if s.child != ci as u32 || s.row != ri as u32 || s.hlc < l.start || s.hlc >= l.expiry {
				return Err(RootError2::Codec);
			}
			at += 1;
		}
	}
	if at != c.stamps.len() {
		return Err(RootError2::Codec);
	}
	// Fixed structural maximum without recursive serialization or allocation.
	let n =
		2usize
			+ 4 + m.validate().map_err(|_| RootError2::Codec)?
			+ 32 + 257
			+ 32 + 1 + 24
			+ 2 + at * 24
			+ 2 + c
			.plans
			.iter()
			.map(|p| 4 + 6 + 20 * p.nodes.len())
			.sum::<usize>()
			+ 8 + 32 + 64;
	if n > CLAIM_BYTES2 {
		return Err(RootError2::Limit);
	}
	Ok(())
}
pub(super) fn valid_attestation_shape(a: &HolderAttestation2) -> Result2<()> {
	a.grant.bytes()?;
	valid_log(a.reservation)?;
	valid_authority(a.authority)?;
	if a.children.len() > 256 {
		return Err(RootError2::Limit);
	}
	for (i, c) in a.children.iter().enumerate() {
		if c.child != i as u32 || c.nodes.is_empty() || c.nodes.len() > 3 {
			return Err(RootError2::Codec);
		}
		ordered_nodes(&c.nodes)?
	}
	Ok(())
}
pub(super) fn valid_parent(p: &impl super::stored::ParentView2) -> Result2<()> {
	let (claim, reservation, held_bytes, terminal) = p.parts();
	valid_claim(claim)?;
	valid_log(reservation)?;
	if claim.original.log.index >= reservation.index || held_bytes != claim.held_bytes()? {
		return Err(RootError2::Codec);
	}
	if let Some(t) = terminal {
		valid_log(t.first())?;
		if t.first().index <= reservation.index {
			return Err(RootError2::Codec);
		}
		if let Terminal2::Refused {
			first,
			cause,
			anchor,
		} = t
		{
			valid_log(anchor)?;
			if !(1..=4).contains(&cause) || first.index < anchor.index {
				return Err(RootError2::Codec);
			}
		}
	}
	Ok(())
}
pub(super) fn same_lineage(old: &OriginalGrant2, new: &OriginalGrant2) -> bool {
	old.lineage == new.lineage
		&& old.incarnation == new.incarnation
		&& old.lease.epoch == new.lease.epoch
		&& old.lease.holder == new.lease.holder
		&& old.lease.range_start == new.lease.range_start
		&& old.lease.range_end == new.lease.range_end
		&& old.predecessor == new.predecessor
}
pub(super) fn dominates(new: &SignedLease2, old: &SignedLease2) -> bool {
	new.start >= old.start
		&& new.expiry >= old.expiry
		&& new.min_commit_index >= old.min_commit_index
}
pub(super) fn old_original_fits(old: &OriginalGrant2, latest: &OriginalGrant2) -> bool {
	same_lineage(old, latest)
		&& old.log.index <= latest.log.index
		&& dominates(&latest.lease, &old.lease)
		&& (old.log.index != latest.log.index || old == latest)
}
pub(super) fn epoch(s: &State2) -> u64 {
	s.cuts.last().map_or(s.initial_epoch, |c| c.new)
}
pub(super) fn usage(s: &State2) -> Result2<Usage2> {
	let held = s.parents.values().try_fold(0usize, |n, p| {
		n.checked_add(usize::try_from(p.held_bytes).map_err(|_| RootError2::Overflow)?)
			.ok_or(RootError2::Overflow)
	})?;
	let history = s
		.lineages
		.len()
		.checked_mul(298)
		.and_then(|n| s.cuts.len().checked_mul(72).and_then(|m| n.checked_add(m)))
		.ok_or(RootError2::Overflow)?;
	let registry = s
		.selectors
		.len()
		.checked_mul(68)
		.and_then(|n| {
			s.authorities
				.len()
				.checked_mul(121)
				.and_then(|m| n.checked_add(m))
		})
		.ok_or(RootError2::Overflow)?;
	Ok(Usage2 {
		parents: s.parents.len(),
		held,
		history,
		registry,
	})
}
pub(super) fn capacity(s: &State2, l: Limits2) -> Result2<()> {
	let u = usage(s)?;
	if u.parents > l.slots
		|| u.held > l.parent
		|| u.history > l.history
		|| u.registry > l.registry
		|| s.lineages.len() > l.lineages
		|| s.cuts.len() > l.cuts
		|| s.selectors.len() > l.registrations
		|| s.authorities.len() > l.registrations
	{
		return Err(RootError2::Limit);
	}
	let total = 232usize
		.checked_add(u.held)
		.and_then(|n| n.checked_add(u.history))
		.and_then(|n| n.checked_add(u.registry))
		.ok_or(RootError2::Overflow)?;
	if total > ROOT_BYTES2 {
		return Err(RootError2::Limit);
	}
	Ok(())
}
pub(super) fn trigger(
	s: &State2,
	p: &impl super::stored::ParentView2,
) -> Result2<Option<(u8, Log2)>> {
	let (claim, _, _, _) = p.parts();
	let mut candidates = Vec::new();
	let g = s
		.lineages
		.get(&claim.original.lineage)
		.ok_or(RootError2::RootInvariant)?;
	if let Some(c) = g.closure {
		candidates.push((
			match c.cause {
				1 => 2,
				2 => 3,
				3 => 1,
				_ => return Err(RootError2::RootInvariant),
			},
			c.log,
		))
	}
	if let Some(a) = s
		.authorities
		.get(&authority_key(claim.authority)?)
		.and_then(|a| a.revoked)
	{
		candidates.push((4, a))
	}
	candidates.sort_by_key(|(cause, l)| (l.index, *cause));
	Ok(candidates.first().copied())
}
pub(super) fn validate_state(s: &State2) -> Result2<()> {
	nonzero(s.generation)?;
	nonzero(s.cluster)?;
	valid_log(s.bootstrap)?;
	valid_log(s.covered)?;
	if s.bootstrap.index > s.covered.index || s.initial_epoch == 0 {
		return Err(RootError2::RootInvariant);
	}
	capacity(s, Limits2::default())?;
	let mut coords = BTreeSet::from([s.bootstrap.index]);
	let coord = |l: Log2| -> Result2<()> {
		valid_log(l)?;
		if l.index < s.bootstrap.index || l.index > s.covered.index {
			Err(RootError2::RootInvariant)
		} else {
			Ok(())
		}
	};
	let mut e = s.initial_epoch;
	let mut last = s.bootstrap.index;
	for c in &s.cuts {
		c.bytes()?;
		coord(c.log)?;
		if c.prior != e || c.log.index <= last || !coords.insert(c.log.index) {
			return Err(RootError2::RootInvariant);
		}
		e = c.new;
		last = c.log.index;
	}
	for (k, v) in &s.selectors {
		if *k != v.binding.encode().map_err(|_| RootError2::Codec)?
			|| v.binding.generation != s.generation
		{
			return Err(RootError2::RootInvariant);
		}
		coord(v.enrolled)?;
		if !coords.insert(v.enrolled.index) {
			return Err(RootError2::RootInvariant);
		}
	}
	let mut nodes = BTreeSet::new();
	for (k, a) in &s.authorities {
		if *k != authority_key(a.authority)? {
			return Err(RootError2::RootInvariant);
		}
		a.bytes()?;
		coord(a.enrolled)?;
		if !coords.insert(a.enrolled.index) {
			return Err(RootError2::RootInvariant);
		}
		nodes.insert(a.node);
		if let Some(l) = a.revoked {
			coord(l)?;
			if l.index <= a.enrolled.index || !coords.insert(l.index) {
				return Err(RootError2::RootInvariant);
			}
		}
	}
	// Register latest-grant coordinates before closures: a supersession may share
	// its successor's original grant entry, irrespective of lineage sort order.
	for g in s.lineages.values() {
		coord(g.latest.log)?;
		if !coords.insert(g.latest.log.index) {
			return Err(RootError2::RootInvariant);
		}
	}
	let mut ranges = s
		.lineages
		.values()
		.map(|g| {
			(
				g.latest.lease.epoch,
				g.latest.lease.range_start,
				g.latest.lease.range_end,
			)
		})
		.collect::<Vec<_>>();
	ranges.sort_unstable();
	ranges.dedup();
	for pair in ranges.windows(2) {
		if pair[0].0 == pair[1].0 && pair[0].2 >= pair[1].1 {
			return Err(RootError2::RootInvariant);
		}
	}
	for (id, g) in &s.lineages {
		valid_lineage(g)?;
		let o = &g.latest;
		let l = &o.lease;
		if *id != o.lineage {
			return Err(RootError2::RootInvariant);
		}
		coord(o.log)?;
		nodes.insert(NodeRef2 {
			node: l.holder,
			incarnation: o.incarnation,
		});
		if let Some(p) = o.predecessor {
			let prev = s
				.lineages
				.get(&p.lineage)
				.ok_or(RootError2::RootInvariant)?;
			if prev.latest.reference()? != p
				|| prev.closure.is_none()
				|| prev.latest.lease.range_start != l.range_start
				|| prev.latest.lease.range_end != l.range_end
			{
				return Err(RootError2::RootInvariant);
			}
		}
		let epoch_at = |idx| {
			s.cuts
				.iter()
				.filter(|c| c.log.index <= idx)
				.last()
				.map_or(s.initial_epoch, |c| c.new)
		};
		if epoch_at(o.log.index) != l.epoch {
			return Err(RootError2::RootInvariant);
		}
		if let Some(c) = g.closure {
			coord(c.log)?;
			match c.cause {
				1 => {
					if epoch_at(c.log.index) != l.epoch || !coords.insert(c.log.index) {
						return Err(RootError2::RootInvariant);
					}
				}
				2 => {
					let next = s
						.lineages
						.get(&c.successor.ok_or(RootError2::RootInvariant)?)
						.ok_or(RootError2::RootInvariant)?;
					if next.latest.predecessor != Some(o.reference()?)
						|| next.latest.log.index < c.log.index
						|| epoch_at(c.log.index) != l.epoch
					{
						return Err(RootError2::RootInvariant);
					}
					if next.latest.log.index == c.log.index {
						if next.latest.log != c.log {
							return Err(RootError2::RootInvariant);
						}
					} else if !coords.insert(c.log.index) {
						return Err(RootError2::RootInvariant);
					}
				}
				3 => {
					if !s
						.cuts
						.iter()
						.any(|cut| cut.prior == l.epoch && cut.log == c.log)
					{
						return Err(RootError2::RootInvariant);
					}
				}
				_ => return Err(RootError2::RootInvariant),
			}
		} else if l.epoch != e {
			return Err(RootError2::RootInvariant);
		}
	}

	let mut non_grant_coords = BTreeSet::new();
	non_grant_coords.insert(s.bootstrap.index);
	for g in s.lineages.values() {
		if let Some(c) = g.closure.filter(|c| c.cause == 1) {
			non_grant_coords.insert(c.log.index);
		}
	}
	for a in s.selectors.values() {
		non_grant_coords.insert(a.enrolled.index);
	}
	for a in s.authorities.values() {
		non_grant_coords.insert(a.enrolled.index);
		if let Some(l) = a.revoked {
			non_grant_coords.insert(l.index);
		}
	}
	for c in &s.cuts {
		non_grant_coords.insert(c.log.index);
	}
	for p in s.parents.values() {
		non_grant_coords.insert(p.reservation.index);
		if let Some(t) = p.terminal {
			non_grant_coords.insert(t.first().index);
		}
	}
	let mut originals = std::collections::BTreeMap::new();
	for (k, p) in &s.parents {
		valid_parent(p)?;
		let c = &p.claim;
		let id = c.manifest.identity;
		if *k != id.encode().map_err(|_| RootError2::Codec)? || id.generation != s.generation {
			return Err(RootError2::RootInvariant);
		}
		coord(p.reservation)?;
		if !coords.insert(p.reservation.index) {
			return Err(RootError2::RootInvariant);
		}

		coord(c.original.log)?;
		if non_grant_coords.contains(&c.original.log.index) {
			return Err(RootError2::RootInvariant);
		}
		let admission_epoch = s
			.cuts
			.iter()
			.filter(|cut| cut.log.index <= p.reservation.index)
			.last()
			.map_or(s.initial_epoch, |cut| cut.new);
		if admission_epoch != c.original.lease.epoch {
			return Err(RootError2::RootInvariant);
		}
		let line = s
			.lineages
			.get(&c.original.lineage)
			.ok_or(RootError2::RootInvariant)?;
		if line
			.closure
			.is_some_and(|cl| cl.log.index <= p.reservation.index)
		{
			return Err(RootError2::RootInvariant);
		}

		let latest = &s
			.lineages
			.get(&c.original.lineage)
			.ok_or(RootError2::RootInvariant)?
			.latest;
		if !old_original_fits(&c.original, latest) {
			return Err(RootError2::RootInvariant);
		}
		let refkey = c.original.reference()?.bytes()?;
		let b = c.original.bytes()?;
		if originals
			.insert(refkey, b.clone())
			.is_some_and(|old| old != b)
		{
			return Err(RootError2::RootInvariant);
		}
		let a = s
			.authorities
			.get(&authority_key(c.authority)?)
			.ok_or(RootError2::RootInvariant)?;
		if a.node
			!= (NodeRef2 {
				node: c.original.lease.holder,
				incarnation: c.original.incarnation,
			}) || a.enrolled.index >= p.reservation.index
		{
			return Err(RootError2::RootInvariant);
		}
		if a.revoked.is_some_and(|l| l.index <= p.reservation.index) {
			return Err(RootError2::RootInvariant);
		}
		let selector = s
			.selectors
			.get(
				&c.manifest
					.binding()
					.encode()
					.map_err(|_| RootError2::Codec)?,
			)
			.ok_or(RootError2::RootInvariant)?;
		if selector.enrolled.index >= p.reservation.index {
			return Err(RootError2::RootInvariant);
		}
		for plan in &c.plans {
			for n in &plan.nodes {
				nodes.insert(*n);
			}
		}
		if let Some(t) = p.terminal {
			coord(t.first())?;
			if !coords.insert(t.first().index) {
				return Err(RootError2::RootInvariant);
			}
			match t {
				Terminal2::Accepted { first, .. } => {
					if trigger(s, p)?.is_some_and(|(_, l)| l.index <= first.index) {
						return Err(RootError2::RootInvariant);
					}
				}
				Terminal2::Refused { cause, anchor, .. } => {
					if trigger(s, p)? != Some((cause, anchor)) {
						return Err(RootError2::RootInvariant);
					}
				}
			}
		}
	}
	if nodes.len() > 256 {
		return Err(RootError2::Limit);
	}
	Ok(())
}
