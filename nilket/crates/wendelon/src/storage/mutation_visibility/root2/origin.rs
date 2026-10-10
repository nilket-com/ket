//! Dormant pre-root and origin layer over Root2: covers the committed log from its genesis
//! membership entry through the first Root2 Bootstrap and beyond. Pure transitions over opaque
//! trusted inputs; decoded bytes never become state (snapshot install is a separate, held step).
use super::codec::{Read2, envelope, open, valid_log};
use super::command::{ApplyResult2, Command2};
use super::entry_identity::EntryPosition2;
use super::state::{Root2, UnverifiedRoot2};
use super::trust::{CommittedEntry2, TrustedEntry2, VerifiedCommand2};
use super::types::*;
use crate::storage::mutation_visibility::redacted_debug;

const MAGIC: &[u8; 8] = b"VISMAS02";
const EMPTY: u8 = 0;
const PRE_ROOT: u8 = 1;
const ROOT: u8 = 2;
const PRE_ROOT_BYTES: usize = 109;
const ORIGIN_BYTES: usize = 100;
// Tag, Origin2, nested length, then the largest nested VISAUT03 envelope.
const BODY_MAX: usize = 1 + ORIGIN_BYTES + 4 + ROOT_BYTES2 + 48;

/// Classification of one committed entry, supplied by its producer; equality of the class to the
/// decoded entry is that producer's guarantee, not something this module can check.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum EntryClass2 {
	Blank,
	Membership,
	NonRoot2,
	Bootstrap(Digest2),
	OtherRoot2(Digest2),
	MalformedRoot2,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum PreRootStep2 {
	Genesis,
	Scanned,
	Malformed,
	AlreadyProcessed,
}
#[derive(Clone, Copy, PartialEq, Eq)]
struct PreRoot2 {
	genesis: Digest2,
	scanned_through: EntryPosition2,
	last_scanned: Digest2,
	malformed: u32,
	first_malformed: Option<EntryPosition2>,
}
#[derive(Clone, Copy, PartialEq, Eq)]
struct Origin2 {
	genesis: Digest2,
	bootstrap_fingerprint: Digest2,
	bootstrap_command: Digest2,
	malformed_before_bootstrap: u32,
}
#[derive(Clone)]
enum StateInner2 {
	Empty,
	PreRoot(PreRoot2),
	Root(Box<Root2>, Origin2),
}
/// Built only by `empty_authority2` and the transitions below.
pub(in crate::storage::mutation_visibility::root2) struct MutationAuthorityState2 {
	inner: StateInner2,
}
enum UnverifiedBody2 {
	Empty,
	PreRoot(PreRoot2),
	Root(Origin2, UnverifiedRoot2),
}
/// Parsed VISMAS02 bytes. Inspection only: nothing here yields a Root2 or authority state.
pub(in crate::storage::mutation_visibility::root2) struct UnverifiedAuthority2 {
	body: UnverifiedBody2,
}

const GENESIS: EntryPosition2 = EntryPosition2 {
	term: 0,
	node: 0,
	index: 0,
};
fn positive(p: EntryPosition2) -> bool {
	p.term >= 1 && p.node >= 1 && p.index >= 1
}
fn same_log(p: EntryPosition2, l: Log2) -> bool {
	(p.term, p.node, p.index) == (l.term, l.node, l.index)
}
fn check_pre_root(p: &PreRoot2) -> Result2<()> {
	let s = p.scanned_through;
	let anchored = if s.index == 0 {
		s == GENESIS && p.last_scanned == p.genesis
	} else {
		positive(s)
	};
	let malformed = match p.first_malformed {
		None => p.malformed == 0,
		Some(m) => p.malformed > 0 && positive(m) && m.index <= s.index,
	};
	if anchored && malformed {
		Ok(())
	} else {
		Err(RootError2::RootInvariant)
	}
}
// The original bootstrap pair is still visible in the root only while nothing has followed it.
fn check_origin(
	origin: &Origin2,
	bootstrap: Log2,
	covered: Log2,
	fp: Digest2,
	command: Digest2,
) -> Result2<()> {
	valid_log(bootstrap).map_err(|_| RootError2::RootInvariant)?;
	if covered == bootstrap
		&& (origin.bootstrap_fingerprint, origin.bootstrap_command) != (fp, command)
	{
		return Err(RootError2::RootInvariant);
	}
	Ok(())
}

/// Copies of the facts the Bootstrap left in the origin wrapper; they never change afterwards.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct OriginFacts2 {
	pub genesis: Digest2,
	pub bootstrap_fingerprint: Digest2,
	pub bootstrap_command: Digest2,
}
redacted_debug!(OriginFacts2, "OriginFacts2");
/// Read-only view of a rooted state: a shared root and copies of its origin facts.
pub(in crate::storage::mutation_visibility::root2) fn root_and_origin2(
	state: &MutationAuthorityState2,
) -> Option<(&Root2, OriginFacts2)> {
	match &state.inner {
		StateInner2::Root(root, origin) => Some((
			root,
			OriginFacts2 {
				genesis: origin.genesis,
				bootstrap_fingerprint: origin.bootstrap_fingerprint,
				bootstrap_command: origin.bootstrap_command,
			},
		)),
		_ => None,
	}
}
pub(in crate::storage::mutation_visibility::root2) fn empty_authority2() -> MutationAuthorityState2
{
	MutationAuthorityState2 {
		inner: StateInner2::Empty,
	}
}
pub(in crate::storage::mutation_visibility::root2) fn coverage2(
	state: &MutationAuthorityState2,
) -> Option<EntryPosition2> {
	match &state.inner {
		StateInner2::Empty => None,
		StateInner2::PreRoot(p) => Some(p.scanned_through),
		StateInner2::Root(root, _) => {
			let l = root.state.covered;
			Some(EntryPosition2 {
				term: l.term,
				node: l.node,
				index: l.index,
			})
		}
	}
}
pub(in crate::storage::mutation_visibility::root2) fn advance_pre_root2(
	state: &MutationAuthorityState2,
	entry: &CommittedEntry2,
) -> Result2<(MutationAuthorityState2, PreRootStep2)> {
	let (position, identity, class) = entry.entry_fields();
	let next = |p: PreRoot2, step| {
		check_pre_root(&p)?;
		Ok((
			MutationAuthorityState2 {
				inner: StateInner2::PreRoot(p),
			},
			step,
		))
	};
	match &state.inner {
		StateInner2::Empty => {
			if position.index != 0 {
				return Err(RootError2::LineageGap);
			}
			if position != GENESIS || class != EntryClass2::Membership {
				return Err(RootError2::TrustMismatch);
			}
			next(
				PreRoot2 {
					genesis: identity,
					scanned_through: GENESIS,
					last_scanned: identity,
					malformed: 0,
					first_malformed: None,
				},
				PreRootStep2::Genesis,
			)
		}
		StateInner2::PreRoot(p) => {
			let s = p.scanned_through;
			if position.index == s.index {
				return if (position, identity) == (s, p.last_scanned) {
					Ok((
						MutationAuthorityState2 {
							inner: StateInner2::PreRoot(*p),
						},
						PreRootStep2::AlreadyProcessed,
					))
				} else {
					Err(RootError2::TrustMismatch)
				};
			}
			let expected = s.index.checked_add(1).ok_or(RootError2::Overflow)?;
			if position.index != expected {
				return Err(RootError2::LineageGap);
			}
			if !positive(position) {
				return Err(RootError2::TrustMismatch);
			}
			let mut q = *p;
			q.scanned_through = position;
			q.last_scanned = identity;
			let step = match class {
				EntryClass2::Bootstrap(_) => return Err(RootError2::TrustMismatch),
				EntryClass2::Blank | EntryClass2::Membership | EntryClass2::NonRoot2 => {
					PreRootStep2::Scanned
				}
				EntryClass2::OtherRoot2(_) | EntryClass2::MalformedRoot2 => {
					q.malformed = q.malformed.saturating_add(1);
					q.first_malformed.get_or_insert(position);
					PreRootStep2::Malformed
				}
			};
			next(q, step)
		}
		StateInner2::Root(..) => Err(RootError2::TrustMismatch),
	}
}
pub(in crate::storage::mutation_visibility::root2) fn bootstrap2(
	state: &MutationAuthorityState2,
	committed: &CommittedEntry2,
	entry: &TrustedEntry2,
	verified: &VerifiedCommand2,
	command: &Command2,
) -> Result2<(MutationAuthorityState2, ApplyResult2)> {
	let StateInner2::PreRoot(p) = &state.inner else {
		return Err(RootError2::TrustMismatch);
	};
	let (position, identity, class) = committed.entry_fields();
	let (_, _, log, fingerprint, digest) = entry.fields();
	let EntryClass2::Bootstrap(d) = class else {
		return Err(RootError2::TrustMismatch);
	};
	let expected = p
		.scanned_through
		.index
		.checked_add(1)
		.ok_or(RootError2::Overflow)?;
	if d != digest
		|| !same_log(position, log)
		|| identity != fingerprint
		|| position.index != expected
		|| !matches!(command, Command2::Bootstrap { .. })
	{
		return Err(RootError2::TrustMismatch);
	}
	let (root, result) = Root2::reduce(None, entry, verified, command)?;
	let origin = Origin2 {
		genesis: p.genesis,
		bootstrap_fingerprint: fingerprint,
		bootstrap_command: digest,
		malformed_before_bootstrap: p.malformed,
	};
	let s = &root.state;
	check_origin(
		&origin,
		s.bootstrap,
		s.covered,
		s.fingerprint,
		s.command_digest,
	)?;
	Ok((
		MutationAuthorityState2 {
			inner: StateInner2::Root(Box::new(root), origin),
		},
		result,
	))
}
pub(in crate::storage::mutation_visibility::root2) fn advance_root2(
	state: &MutationAuthorityState2,
	entry: &TrustedEntry2,
	verified: &VerifiedCommand2,
	command: &Command2,
) -> Result2<(MutationAuthorityState2, ApplyResult2)> {
	let StateInner2::Root(root, origin) = &state.inner else {
		return Err(RootError2::TrustMismatch);
	};
	let (next, result) = Root2::reduce(Some(root), entry, verified, command)?;
	Ok((
		MutationAuthorityState2 {
			inner: StateInner2::Root(Box::new(next), *origin),
		},
		result,
	))
}

fn position(b: &mut Vec<u8>, p: EntryPosition2) {
	b.extend(p.term.to_le_bytes());
	b.extend(p.node.to_le_bytes());
	b.extend(p.index.to_le_bytes());
}
fn read_position(r: &mut Read2<'_>) -> Result2<EntryPosition2> {
	Ok(EntryPosition2 {
		term: r.u64()?,
		node: r.u32()?,
		index: r.u64()?,
	})
}
fn write_pre_root(b: &mut Vec<u8>, p: &PreRoot2) -> Result2<()> {
	check_pre_root(p)?;
	b.push(PRE_ROOT);
	b.extend(p.genesis);
	position(b, p.scanned_through);
	b.extend(p.last_scanned);
	b.extend(p.malformed.to_le_bytes());
	b.push(u8::from(p.first_malformed.is_some()));
	position(b, p.first_malformed.unwrap_or(GENESIS));
	Ok(())
}
fn write_root(b: &mut Vec<u8>, origin: &Origin2, root: &[u8]) -> Result2<()> {
	b.push(ROOT);
	b.extend(origin.genesis);
	b.extend(origin.bootstrap_fingerprint);
	b.extend(origin.bootstrap_command);
	b.extend(origin.malformed_before_bootstrap.to_le_bytes());
	b.extend(
		u32::try_from(root.len())
			.map_err(|_| RootError2::Overflow)?
			.to_le_bytes(),
	);
	b.extend(root);
	Ok(())
}
fn seal(body: Vec<u8>) -> Result2<Vec<u8>> {
	envelope(MAGIC, &body, BODY_MAX)
}
pub(in crate::storage::mutation_visibility::root2) fn encode_authority2(
	state: &MutationAuthorityState2,
) -> Result2<Vec<u8>> {
	let mut b = Vec::new();
	match &state.inner {
		StateInner2::Empty => b.push(EMPTY),
		StateInner2::PreRoot(p) => write_pre_root(&mut b, p)?,
		StateInner2::Root(root, origin) => {
			let s = &root.state;
			check_origin(
				origin,
				s.bootstrap,
				s.covered,
				s.fingerprint,
				s.command_digest,
			)?;
			write_root(&mut b, origin, &root.encode()?)?;
		}
	}
	seal(b)
}
impl UnverifiedAuthority2 {
	pub(in crate::storage::mutation_visibility::root2) fn decode(bytes: &[u8]) -> Result2<Self> {
		let body = open(bytes, MAGIC, BODY_MAX)?;
		let mut r = Read2::new(body);
		let decoded = match r.u8()? {
			EMPTY => UnverifiedBody2::Empty,
			PRE_ROOT => {
				if body.len() != 1 + PRE_ROOT_BYTES {
					return Err(RootError2::Codec);
				}
				let genesis = r.array()?;
				let scanned_through = read_position(&mut r)?;
				let last_scanned = r.array()?;
				let malformed = r.u32()?;
				let present = r.u8()?;
				let first = read_position(&mut r)?;
				let first_malformed = match present {
					0 if first == GENESIS => None,
					1 => Some(first),
					_ => return Err(RootError2::Codec),
				};
				let p = PreRoot2 {
					genesis,
					scanned_through,
					last_scanned,
					malformed,
					first_malformed,
				};
				check_pre_root(&p)?;
				UnverifiedBody2::PreRoot(p)
			}
			ROOT => {
				let origin = Origin2 {
					genesis: r.array()?,
					bootstrap_fingerprint: r.array()?,
					bootstrap_command: r.array()?,
					malformed_before_bootstrap: r.u32()?,
				};
				let n = r.u32()? as usize;
				if n > ROOT_BYTES2 + 48 {
					return Err(RootError2::Limit);
				}
				let root = UnverifiedRoot2::decode(r.take(n)?)?;
				let (bootstrap, covered, fp, command) = root.origin_anchor_fields2();
				check_origin(&origin, bootstrap, covered, fp, command)?;
				UnverifiedBody2::Root(origin, root)
			}
			_ => return Err(RootError2::Codec),
		};
		r.end()?;
		let parsed = Self { body: decoded };
		if parsed.encode()? != bytes {
			return Err(RootError2::Codec);
		}
		Ok(parsed)
	}
	pub(in crate::storage::mutation_visibility::root2) fn encode(&self) -> Result2<Vec<u8>> {
		let mut b = Vec::new();
		match &self.body {
			UnverifiedBody2::Empty => b.push(EMPTY),
			UnverifiedBody2::PreRoot(p) => write_pre_root(&mut b, p)?,
			UnverifiedBody2::Root(origin, root) => write_root(&mut b, origin, &root.encode()?)?,
		}
		seal(b)
	}
}
#[cfg(test)]
impl UnverifiedAuthority2 {
	pub(super) fn tag_for_test(&self) -> u8 {
		match self.body {
			UnverifiedBody2::Empty => EMPTY,
			UnverifiedBody2::PreRoot(_) => PRE_ROOT,
			UnverifiedBody2::Root(..) => ROOT,
		}
	}
}
#[cfg(test)]
impl MutationAuthorityState2 {
	pub(super) fn malformed_for_test(&self) -> Option<(u32, Option<EntryPosition2>)> {
		match &self.inner {
			StateInner2::PreRoot(p) => Some((p.malformed, p.first_malformed)),
			StateInner2::Root(_, o) => Some((o.malformed_before_bootstrap, None)),
			StateInner2::Empty => None,
		}
	}
	pub(super) fn is_root_for_test(&self) -> bool {
		matches!(self.inner, StateInner2::Root(..))
	}
}
/// Test-only PreRoot at (1,1,1) whose single scanned entry was malformed, with a chosen count;
/// exercises saturation without four billion transitions.
#[cfg(test)]
pub(super) fn pre_root_with_malformed_for_test(
	count: u32,
	genesis: Digest2,
) -> MutationAuthorityState2 {
	let at = EntryPosition2 {
		term: 1,
		node: 1,
		index: 1,
	};
	MutationAuthorityState2 {
		inner: StateInner2::PreRoot(PreRoot2 {
			genesis,
			scanned_through: at,
			last_scanned: [7; 32],
			malformed: count,
			first_malformed: Some(at),
		}),
	}
}
/// Test-only PreRoot already scanned through `scanned`, checked by the runtime validator; exercises
/// the next-index overflow boundary without u64::MAX transitions.
#[cfg(test)]
pub(super) fn pre_root_scanned_through_for_test(
	scanned: EntryPosition2,
	last: Digest2,
	genesis: Digest2,
) -> MutationAuthorityState2 {
	let p = PreRoot2 {
		genesis,
		scanned_through: scanned,
		last_scanned: last,
		malformed: 0,
		first_malformed: None,
	};
	check_pre_root(&p).expect("valid PreRoot fixture");
	MutationAuthorityState2 {
		inner: StateInner2::PreRoot(p),
	}
}
