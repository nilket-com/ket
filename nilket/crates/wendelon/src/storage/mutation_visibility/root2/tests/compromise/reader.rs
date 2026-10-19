//! Test-only reader of the Root2 context export (C-S6b contract §5). It parses the format
//! independently of the writer, checks the trusted manifest, and replays the successor chain with
//! the production role-proof signature check (`slot_signed`, the check `verify_for` itself runs)
//! under the keys active before each record. The production reader belongs to the key custody
//! facade slice; this one exists so the §5.3 refusals are exercised now.
use crate::consensus::ROOT2_COMMAND_ENVELOPE_MAX1;
use crate::storage::mutation_visibility::root2::admin::key::check_enrollable_admin_key2;
use crate::storage::mutation_visibility::root2::admin::manifest::{
	AnyAdminManifest2, any_manifest_digest2,
};
use crate::storage::mutation_visibility::root2::codec::hash;
use crate::storage::mutation_visibility::root2::command::Command2;
use crate::storage::mutation_visibility::root2::role_command::{
	CONTEXT_BYTES3, ROLE_PROOF_BYTES3, RoleForm3, parse as parse_proof, slot_signed,
};
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Refusal {
	Truncated,
	Digest,
	Magic,
	Version,
	Reserved,
	Coverage,
	Boolean,
	Fill,
	Count,
	RoleCount,
	Role,
	CommandLength,
	SidecarLength,
	Trailing,
	Manifest,
	Pin,
	Decode,
	Deployment,
	Kind,
	Key,
	Enrollable,
	Duplicate,
	Epoch,
	Order,
	Proof,
}

/// An audit-derived value: not chain-verified, so there is no accessor that yields it as trusted
/// history, only the named fail-safe uses.
#[derive(Debug)]
pub(super) struct AuditDerived<T>(T);
impl AuditDerived<bool> {
	/// A frozen export refuses role actions other than Unfreeze.
	pub(super) fn refuses_while_frozen(&self) -> bool {
		self.0
	}
}
impl AuditDerived<Option<Log2>> {
	/// A retirement refuses every further action.
	pub(super) fn refuses_as_retired(&self) -> bool {
		self.0.is_some()
	}
	/// The Freeze/Unfreeze binding: a forged value fails safe as a consumed `Denied(24)`.
	pub(super) fn freeze_binding(&self) -> Log2 {
		self.0.unwrap_or(Log2 {
			term: 0,
			node: 0,
			index: 0,
		})
	}
}
pub(super) struct Verified {
	/// `(role, epoch, key)` of every re-verified successor, in log order.
	pub(super) history: Vec<(u8, u64, Digest2)>,
	/// The active `(epoch, key)` per role after the chain.
	pub(super) active: [(u64, Digest2); 3],
	pub(super) context_authenticated: bool,
	pub(super) frozen: AuditDerived<bool>,
	pub(super) transition: AuditDerived<Option<Log2>>,
	pub(super) retired: AuditDerived<Option<Log2>>,
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

fn rotation(command: &Command2) -> Option<(u8, Digest2)> {
	match command {
		Command2::RotateAdmin(key) => Some((1, *key)),
		Command2::RotateGovernance(key) => Some((2, *key)),
		Command2::RotateRecovery(key) => Some((3, *key)),
		_ => None,
	}
}
/// The role pair a rotation's `VISROL03` proof must carry, as the production verifier requires:
/// admin and governance by recovery, recovery jointly by admin and governance.
fn role_requirement(role: u8) -> [u8; 2] {
	if role == 3 { [1, 2] } else { [3, 0] }
}
/// The proof binds this context and command and verifies under the active keys, slot by slot,
/// with the production signature check.
fn proof_verifies(
	sidecar: &[u8],
	context: Digest2,
	command: Digest2,
	expected: [u8; 2],
	keys: &[Vec<Digest2>; 3],
) -> bool {
	if parse_proof(sidecar).is_err() || sidecar[12..44] != context || sidecar[44..76] != command {
		return false;
	}
	for (slot, at) in [76usize, 150].into_iter().enumerate() {
		if sidecar[at] != expected[slot] {
			return false;
		}
		if expected[slot] == 0 {
			continue;
		}
		let history = &keys[usize::from(expected[slot] - 1)];
		let epoch = u64::from_le_bytes(sidecar[at + 1..at + 9].try_into().unwrap());
		if sidecar[at + 9] != 1 || epoch != history.len() as u64 - 1 {
			return false;
		}
		let key = history.last().expect("an original key");
		if slot_signed(sidecar, expected, slot, RoleForm3::Legacy, key) != Ok(true) {
			return false;
		}
	}
	true
}

/// §5: parse, bind to the trusted manifest, replay the chain.
pub(super) fn verify(bytes: &[u8], trusted_manifest: &[u8]) -> Result<Verified, Refusal> {
	let p = parse(bytes)?;
	if p.manifest != trusted_manifest {
		return Err(Refusal::Manifest);
	}
	let Ok(AnyAdminManifest2::Three(m)) = AnyAdminManifest2::decode(&p.manifest) else {
		return Err(Refusal::Manifest);
	};
	let any = AnyAdminManifest2::Three(m.clone());
	let pin = any_manifest_digest2(&any.encode().map_err(|_| Refusal::Manifest)?);
	if pin[..] != p.context[156..188] {
		return Err(Refusal::Pin);
	}
	let generation: [u8; 16] = p.context[..16].try_into().unwrap();
	let cluster: [u8; 16] = p.context[16..32].try_into().unwrap();
	let installed = u64::from_le_bytes(p.context[44..52].try_into().unwrap());
	let context = hash(CONTEXT_DOMAIN, &p.context);
	let mut keys: [Vec<Digest2>; 3] = [
		vec![any.admission().admin_key],
		vec![m.governance().key()],
		vec![m.recovery().key()],
	];
	let mut last = installed;
	let mut history = Vec::new();
	for s in &p.successors {
		let (g, c, command) = Command2::decode(&s.command).map_err(|_| Refusal::Decode)?;
		if (g, c) != (generation, cluster) {
			return Err(Refusal::Deployment);
		}
		let (role, key) = rotation(&command).ok_or(Refusal::Kind)?;
		if role != s.role {
			return Err(Refusal::Kind);
		}
		if key != s.key {
			return Err(Refusal::Key);
		}
		check_enrollable_admin_key2(&key).map_err(|_| Refusal::Enrollable)?;
		if keys.iter().flatten().any(|k| *k == key) {
			return Err(Refusal::Duplicate);
		}
		let i = usize::from(role - 1);
		if s.epoch != keys[i].len() as u64 {
			return Err(Refusal::Epoch);
		}
		if s.activated.index <= last {
			return Err(Refusal::Order);
		}
		let digest = command.digest(g, c).map_err(|_| Refusal::Decode)?;
		if !proof_verifies(&s.sidecar, context, digest, role_requirement(role), &keys) {
			return Err(Refusal::Proof);
		}
		last = s.activated.index;
		keys[i].push(key);
		history.push((role, s.epoch, key));
	}
	Ok(Verified {
		history,
		active: std::array::from_fn(|i| (keys[i].len() as u64 - 1, *keys[i].last().unwrap())),
		context_authenticated: !p.successors.is_empty(),
		frozen: AuditDerived(p.frozen),
		transition: AuditDerived(p.transition),
		retired: AuditDerived(p.retired),
	})
}
