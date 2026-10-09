//! Version-1 identity of one Control Ring entry, computed from its decoded value in a single
//! streamed SHA-256 pass. Byte math only: a caller-supplied value is not thereby committed, and
//! nothing here adapts a real openraft entry.
use super::types::Digest2;
use crate::consensus::WendelonRequest;
use aws_lc_rs::digest::{Context, SHA256};
use openraft::{BasicNode, Membership};
use serde::Serialize;

const DOMAIN: &[u8] = b"VISROOT2-ENTRY\0";
const VERSION: u16 = 1;
const BLANK: u8 = 0;
const NORMAL: u8 = 1;
const MEMBERSHIP: u8 = 2;

/// Raft coordinates as committed; zero is permitted (genesis is term 0, node 0, index 0).
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct EntryPosition2 {
	pub term: u64,
	pub node: u32,
	pub index: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum EntryIdentityError2 {
	Serialize,
	Overflow,
	CountMismatch,
}
type IdentityResult2<T> = Result<T, EntryIdentityError2>;

pub(in crate::storage::mutation_visibility::root2) fn blank_entry_identity2(
	at: EntryPosition2,
) -> Digest2 {
	let state = start(at, BLANK, 0);
	state
		.ctx
		.finish()
		.as_ref()
		.try_into()
		.expect("sha256 width")
}
pub(in crate::storage::mutation_visibility::root2) fn normal_entry_identity2(
	at: EntryPosition2,
	request: &WendelonRequest,
) -> IdentityResult2<Digest2> {
	identity(at, NORMAL, request)
}
pub(in crate::storage::mutation_visibility::root2) fn membership_entry_identity2(
	at: EntryPosition2,
	membership: &Membership<u32, BasicNode>,
) -> IdentityResult2<Digest2> {
	identity(at, MEMBERSHIP, membership)
}

struct HashState {
	ctx: Context,
	count: u64,
	overflowed: bool,
}
// Postcard writes straight into the digest; no buffer proportional to the payload is held.
struct HashFlavor<'a>(&'a mut HashState);
impl postcard::ser_flavors::Flavor for HashFlavor<'_> {
	type Output = ();
	fn try_extend(&mut self, data: &[u8]) -> postcard::Result<()> {
		let next = u64::try_from(data.len())
			.ok()
			.and_then(|n| self.0.count.checked_add(n));
		let Some(next) = next else {
			self.0.overflowed = true;
			return Err(postcard::Error::SerializeBufferFull);
		};
		self.0.count = next;
		self.0.ctx.update(data);
		Ok(())
	}
	fn try_push(&mut self, data: u8) -> postcard::Result<()> {
		self.try_extend(&[data])
	}
	fn finalize(self) -> postcard::Result<()> {
		Ok(())
	}
}
fn start(at: EntryPosition2, kind: u8, len: u64) -> HashState {
	let mut ctx = Context::new(&SHA256);
	ctx.update(DOMAIN);
	ctx.update(&VERSION.to_le_bytes());
	ctx.update(&at.term.to_le_bytes());
	ctx.update(&at.node.to_le_bytes());
	ctx.update(&at.index.to_le_bytes());
	ctx.update(&[kind]);
	ctx.update(&len.to_le_bytes());
	HashState {
		ctx,
		count: 0,
		overflowed: false,
	}
}
fn finish(state: HashState, expected: u64) -> IdentityResult2<Digest2> {
	if state.count != expected {
		return Err(EntryIdentityError2::CountMismatch);
	}
	Ok(state
		.ctx
		.finish()
		.as_ref()
		.try_into()
		.expect("sha256 width"))
}
// Two deterministic passes over the same value: size first (it frames the preimage), then hash.
fn identity<T: Serialize + ?Sized>(
	at: EntryPosition2,
	kind: u8,
	value: &T,
) -> IdentityResult2<Digest2> {
	let size = postcard::experimental::serialized_size(value)
		.map_err(|_| EntryIdentityError2::Serialize)?;
	let len = u64::try_from(size).map_err(|_| EntryIdentityError2::Overflow)?;
	let mut state = start(at, kind, len);
	if postcard::serialize_with_flavor(value, HashFlavor(&mut state)).is_err() {
		return Err(if state.overflowed {
			EntryIdentityError2::Overflow
		} else {
			EntryIdentityError2::Serialize
		});
	}
	finish(state, len)
}

/// Feeds `bytes` into a state whose running count starts at `count`; exercises overflow without
/// a multi-gigabyte value.
#[cfg(test)]
pub(super) fn stream_probe(count: u64, bytes: &[u8]) -> IdentityResult2<u64> {
	use postcard::ser_flavors::Flavor;
	let mut state = start(
		EntryPosition2 {
			term: 0,
			node: 0,
			index: 0,
		},
		NORMAL,
		0,
	);
	state.count = count;
	match HashFlavor(&mut state).try_extend(bytes) {
		Ok(()) => Ok(state.count),
		Err(_) if state.overflowed => Err(EntryIdentityError2::Overflow),
		Err(_) => Err(EntryIdentityError2::Serialize),
	}
}
/// Applies the post-condition to a state that streamed `count` bytes against a declared length.
#[cfg(test)]
pub(super) fn finish_probe(count: u64, expected: u64) -> IdentityResult2<Digest2> {
	let mut state = start(
		EntryPosition2 {
			term: 0,
			node: 0,
			index: 0,
		},
		NORMAL,
		expected,
	);
	state.count = count;
	finish(state, expected)
}
