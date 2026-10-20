//! The test side of the Root2 context export (C-S6b contract §5): an independent parser and
//! encoder of the format, used to build exact tampered exports, and `verify`, which is the
//! production reader (`operator_proof::verify_context_export`, replaying the chain with the real
//! `verify_core` under `role_requirement`). Every refusal the tests assert is the production one.
use crate::consensus::ROOT2_COMMAND_ENVELOPE_MAX1;
use crate::storage::mutation_visibility::root2::codec::hash;
pub(super) use crate::storage::mutation_visibility::root2::operator_proof::ContextRefusal as Refusal;
use crate::storage::mutation_visibility::root2::operator_proof::{
	VerifiedContext, verify_context_export,
};
use crate::storage::mutation_visibility::root2::role_command::{CONTEXT_BYTES3, ROLE_PROOF_BYTES3};
use crate::storage::mutation_visibility::root2::types::{Digest2, Log2};

pub(super) const CONTEXT_DOMAIN: &[u8] = b"VISROOT2-ROLE3-CONTEXT\0";
const MANIFEST_BYTES: usize = 241;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Record {
	pub(super) role: u8,
	pub(super) epoch: u64,
	pub(super) key: Digest2,
	pub(super) activated: Log2,
	pub(super) command: Vec<u8>,
	pub(super) sidecar: Vec<u8>,
}
/// Every field as stored, before any trust decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Parsed {
	pub(super) context: [u8; CONTEXT_BYTES3],
	pub(super) manifest: Vec<u8>,
	pub(super) node_id: u32,
	pub(super) stop: Log2,
	pub(super) frozen: bool,
	pub(super) transition: Option<Log2>,
	pub(super) retired: Option<Log2>,
	pub(super) successors: Vec<Record>,
}
struct Cursor<'a>(&'a [u8]);
impl<'a> Cursor<'a> {
	fn take(&mut self, n: usize) -> Result<&'a [u8], Refusal> {
		if self.0.len() < n {
			return Err(Refusal::Truncated);
		}
		let (head, tail) = self.0.split_at(n);
		self.0 = tail;
		Ok(head)
	}
	fn array<const N: usize>(&mut self) -> Result<[u8; N], Refusal> {
		Ok(self.take(N)?.try_into().expect("taken width"))
	}
	fn u8(&mut self) -> Result<u8, Refusal> {
		Ok(self.take(1)?[0])
	}
	fn u32(&mut self) -> Result<u32, Refusal> {
		Ok(u32::from_le_bytes(self.array()?))
	}
	fn u64(&mut self) -> Result<u64, Refusal> {
		Ok(u64::from_le_bytes(self.array()?))
	}
	fn log(&mut self) -> Result<Log2, Refusal> {
		Ok(Log2 {
			term: self.u64()?,
			node: self.u32()?,
			index: self.u64()?,
		})
	}
	fn boolean(&mut self) -> Result<bool, Refusal> {
		match self.u8()? {
			0 => Ok(false),
			1 => Ok(true),
			_ => Err(Refusal::Boolean),
		}
	}
	fn optional(&mut self) -> Result<Option<Log2>, Refusal> {
		let present = self.boolean()?;
		let log = self.log()?;
		match (present, log) {
			(true, log) => Ok(Some(log)),
			(
				false,
				Log2 {
					term: 0,
					node: 0,
					index: 0,
				},
			) => Ok(None),
			(false, _) => Err(Refusal::Fill),
		}
	}
}

/// §5.1: digest, version, coverage, booleans, fill and caps, with the count checked before any
/// allocation.
pub(super) fn parse(bytes: &[u8]) -> Result<Parsed, Refusal> {
	let Some(split) = bytes.len().checked_sub(32) else {
		return Err(Refusal::Truncated);
	};
	let (body, digest) = bytes.split_at(split);
	if hash(b"", body)[..] != *digest {
		return Err(Refusal::Digest);
	}
	let mut r = Cursor(body);
	if r.take(8)? != b"WRCTXEXP" {
		return Err(Refusal::Magic);
	}
	if u16::from_le_bytes(r.array()?) != 1 {
		return Err(Refusal::Version);
	}
	if r.array::<2>()? != [0, 0] {
		return Err(Refusal::Reserved);
	}
	let context = r.array()?;
	let manifest = r.take(MANIFEST_BYTES)?.to_vec();
	let node_id = r.u32()?;
	let stop = r.log()?;
	if r.u8()? != 1 {
		return Err(Refusal::Coverage);
	}
	let frozen = r.boolean()?;
	let transition = r.optional()?;
	let retired = r.optional()?;
	let count = r.u32()?;
	if count > 192 {
		return Err(Refusal::Count);
	}
	let mut successors = Vec::with_capacity(count as usize);
	let mut per_role = [0usize; 3];
	for _ in 0..count {
		let role = r.u8()?;
		if !(1..=3).contains(&role) {
			return Err(Refusal::Role);
		}
		per_role[usize::from(role - 1)] += 1;
		if per_role[usize::from(role - 1)] > 64 {
			return Err(Refusal::RoleCount);
		}
		let epoch = r.u64()?;
		let key = r.array()?;
		let activated = r.log()?;
		let len = r.u32()? as usize;
		if len > ROOT2_COMMAND_ENVELOPE_MAX1 {
			return Err(Refusal::CommandLength);
		}
		let command = r.take(len)?.to_vec();
		if r.u32()? as usize != ROLE_PROOF_BYTES3 {
			return Err(Refusal::SidecarLength);
		}
		let sidecar = r.take(ROLE_PROOF_BYTES3)?.to_vec();
		successors.push(Record {
			role,
			epoch,
			key,
			activated,
			command,
			sidecar,
		});
	}
	if !r.0.is_empty() {
		return Err(Refusal::Trailing);
	}
	Ok(Parsed {
		context,
		manifest,
		node_id,
		stop,
		frozen,
		transition,
		retired,
		successors,
	})
}

fn put_log(b: &mut Vec<u8>, l: Log2) {
	b.extend(l.term.to_le_bytes());
	b.extend(l.node.to_le_bytes());
	b.extend(l.index.to_le_bytes());
}
/// The test's own encoder: exact field order, any values (so refusals can be built), sealed with
/// the trailing digest.
pub(super) fn encode(p: &Parsed) -> Vec<u8> {
	let mut b = b"WRCTXEXP".to_vec();
	b.extend(1u16.to_le_bytes());
	b.extend([0, 0]);
	b.extend(p.context);
	b.extend(&p.manifest);
	b.extend(p.node_id.to_le_bytes());
	put_log(&mut b, p.stop);
	b.push(1);
	b.push(u8::from(p.frozen));
	for l in [p.transition, p.retired] {
		b.push(u8::from(l.is_some()));
		put_log(
			&mut b,
			l.unwrap_or(Log2 {
				term: 0,
				node: 0,
				index: 0,
			}),
		);
	}
	b.extend((p.successors.len() as u32).to_le_bytes());
	for s in &p.successors {
		b.push(s.role);
		b.extend(s.epoch.to_le_bytes());
		b.extend(s.key);
		put_log(&mut b, s.activated);
		b.extend((s.command.len() as u32).to_le_bytes());
		b.extend(&s.command);
		b.extend((s.sidecar.len() as u32).to_le_bytes());
		b.extend(&s.sidecar);
	}
	reseal(b)
}
/// Appends the integrity digest over `body`.
pub(super) fn reseal(mut body: Vec<u8>) -> Vec<u8> {
	let digest = hash(b"", &body);
	body.extend(digest);
	body
}
/// Re-seals an already sealed export after a byte-level edit.
pub(super) fn resealed(sealed: &[u8]) -> Vec<u8> {
	reseal(sealed[..sealed.len() - 32].to_vec())
}

/// The production reader, unchanged.
pub(super) fn verify(bytes: &[u8], trusted_manifest: &[u8]) -> Result<VerifiedContext, Refusal> {
	verify_context_export(bytes, trusted_manifest)
}
/// A LogId20 as the export carries it.
pub(super) fn log20(l: Log2) -> [u8; 20] {
	let mut b = [0; 20];
	b[..8].copy_from_slice(&l.term.to_le_bytes());
	b[8..12].copy_from_slice(&l.node.to_le_bytes());
	b[12..].copy_from_slice(&l.index.to_le_bytes());
	b
}
