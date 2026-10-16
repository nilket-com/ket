//! Bounded role history. Parsed history is descriptive, never provenance or an active pin.
use super::admin::key::check_enrollable_admin_key2;
use super::{codec::*, state::Root2, types::*};
use std::collections::BTreeSet;

pub(super) const SUCCESSORS3: usize = 64;
#[derive(Clone, PartialEq, Eq)]
pub(super) struct Epoch3 {
	pub(super) key: Digest2,
	pub(super) activated: Log2,
}
#[derive(Clone, PartialEq, Eq)]
pub(super) struct RoleEpochs3 {
	pub(super) original: [Digest2; 3],
	pub(super) frozen: bool,
	pub(super) transition: Log2,
	pub(super) successors: [Vec<Epoch3>; 3],
}
// The retained transition makes freeze/unfreeze envelopes single-use.
impl RoleEpochs3 {
	pub(super) fn empty(original: [Digest2; 3]) -> Self {
		Self {
			original,
			frozen: false,
			transition: Log2 {
				term: 0,
				node: 0,
				index: 0,
			},
			successors: std::array::from_fn(|_| Vec::new()),
		}
	}
	pub(super) fn active(&self, role: usize) -> (u64, Digest2) {
		let history = &self.successors[role];
		(
			history.len() as u64,
			history.last().map_or(self.original[role], |e| e.key),
		)
	}
	pub(super) fn charge(&self) -> Result2<usize> {
		let n = self.successors.iter().try_fold(0usize, |n, h| {
			if h.len() > SUCCESSORS3 {
				return Err(RootError2::Limit);
			}
			n.checked_add(h.len()).ok_or(RootError2::Overflow)
		})?;
		n.checked_mul(52)
			.and_then(|n| n.checked_add(120))
			.ok_or(RootError2::Overflow)
	}
	pub(super) fn write(&self, emit: &mut impl FnMut(&[u8])) -> Result2<()> {
		self.charge()?;
		for key in self.original {
			emit(&key);
		}
		emit(&std::array::from_fn::<_, 3, _>(|i| {
			self.successors[i].len() as u8
		}));
		for h in &self.successors {
			for e in h {
				emit(&e.key);
				emit(&e.activated.term.to_le_bytes());
				emit(&e.activated.node.to_le_bytes());
				emit(&e.activated.index.to_le_bytes());
			}
		}
		emit(&[u8::from(self.frozen)]);
		emit(&self.transition.term.to_le_bytes());
		emit(&self.transition.node.to_le_bytes());
		emit(&self.transition.index.to_le_bytes());
		Ok(())
	}
	pub(super) fn read(r: &mut Read2<'_>) -> Result2<Self> {
		let original = [r.array()?, r.array()?, r.array()?];
		let counts: [u8; 3] = r.array()?;
		if counts.iter().any(|&n| usize::from(n) > SUCCESSORS3) {
			return Err(RootError2::Limit);
		}
		let mut successors = std::array::from_fn(|_| Vec::new());
		for i in 0..3 {
			successors[i]
				.try_reserve_exact(usize::from(counts[i]))
				.map_err(|_| RootError2::Limit)?;
			for _ in 0..counts[i] {
				successors[i].push(Epoch3 {
					key: r.array()?,
					activated: r.log()?,
				});
			}
		}
		let frozen = match r.u8()? {
			0 => false,
			1 => true,
			_ => return Err(RootError2::Codec),
		};
		let transition = r.optional_log()?.unwrap_or(Log2 {
			term: 0,
			node: 0,
			index: 0,
		});
		if transition.index == 0 && (frozen || counts == [0; 3]) {
			return Err(RootError2::Codec);
		}
		Ok(Self {
			original,
			frozen,
			transition,
			successors,
		})
	}
}
pub(super) fn charge(s: &State2) -> Result2<usize> {
	s.role_epochs.as_ref().map_or(Ok(0), |e| e.charge())
}
pub(super) fn validate(s: &State2, coords: &mut BTreeSet<u64>) -> Result2<()> {
	let Some(e) = &s.role_epochs else {
		return Ok(());
	};
	if s.control_len != super::admin::manifest::ADMIN_MANIFEST_P3_BYTES3 as u64
		|| (e.successors.iter().all(Vec::is_empty) && e.transition.index == 0)
	{
		return Err(RootError2::RootInvariant);
	}
	if e.transition.index == 0 {
		if e.frozen
			|| e.transition
				!= (Log2 {
					term: 0,
					node: 0,
					index: 0,
				}) {
			return Err(RootError2::RootInvariant);
		}
	} else {
		valid_log(e.transition)?;
		if e.transition.index <= s.bootstrap.index
			|| e.transition.index > s.covered.index
			|| !coords.insert(e.transition.index)
		{
			return Err(RootError2::RootInvariant);
		}
	}
	e.charge()?;
	let mut keys = BTreeSet::new();
	for key in e
		.original
		.iter()
		.chain(e.successors.iter().flatten().map(|e| &e.key))
	{
		if check_enrollable_admin_key2(key).is_err() || !keys.insert(*key) {
			return Err(RootError2::RootInvariant);
		}
	}
	for history in &e.successors {
		let mut previous = s.bootstrap.index;
		for epoch in history {
			valid_log(epoch.activated)?;
			if epoch.activated.index <= previous
				|| epoch.activated.index > s.covered.index
				|| !coords.insert(epoch.activated.index)
			{
				return Err(RootError2::RootInvariant);
			}
			previous = epoch.activated.index;
		}
	}
	Ok(())
}
/// Metadata from the existing private verification producer; it is bound to the exact input cut.
pub(super) type Seed3 = ([Digest2; 3], Log2, Digest2, Digest2);
pub(super) fn rotate(
	root: &mut Root2,
	role: usize,
	key: Digest2,
	at: Log2,
	seed: Option<Seed3>,
) -> Result2<super::command::ApplyResult2> {
	use super::command::ApplyResult2::{Denied, Registered};
	let (original, covered, fingerprint, command) = seed.ok_or(RootError2::TrustMismatch)?;
	let s = &root.state;
	if (covered, fingerprint, command) != (s.covered, s.fingerprint, s.command_digest)
		|| s.control_len != super::admin::manifest::ADMIN_MANIFEST_P3_BYTES3 as u64
		|| role >= 3
	{
		return Err(RootError2::TrustMismatch);
	}
	if check_enrollable_admin_key2(&key).is_err() {
		return Ok(Denied {
			reason: 20,
			reference: None,
		});
	}
	if original.contains(&key)
		|| s.role_epochs
			.as_ref()
			.is_some_and(|e| e.successors.iter().flatten().any(|e| e.key == key))
	{
		return Ok(Denied {
			reason: 21,
			reference: None,
		});
	}
	if s.role_epochs
		.as_ref()
		.is_some_and(|e| e.successors[role].len() >= SUCCESSORS3)
	{
		return Ok(Denied {
			reason: 22,
			reference: None,
		});
	}
	let delta = if s.role_epochs.is_none() { 172 } else { 52 };
	let history = s
		.derived
		.usage
		.history
		.checked_add(delta)
		.ok_or(RootError2::Overflow)?;
	let total = super::validation::ROOT_BASE2
		.checked_add(s.derived.usage.held)
		.and_then(|n| n.checked_add(history))
		.and_then(|n| n.checked_add(s.derived.usage.registry))
		.ok_or(RootError2::Overflow)?;
	if history > root.limits.history || total > ROOT_BYTES2 {
		return Ok(Denied {
			reason: 11,
			reference: None,
		});
	}
	let e = root
		.state
		.role_epochs
		.get_or_insert_with(|| Box::new(RoleEpochs3::empty(original)));
	if e.original != original {
		return Err(RootError2::TrustMismatch);
	}
	e.successors[role].push(Epoch3 { key, activated: at });
	Ok(Registered(at))
}
