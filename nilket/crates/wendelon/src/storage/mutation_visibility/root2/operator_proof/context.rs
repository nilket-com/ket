//! The production reader of the Root2 context export (C-S6b contract §5). It parses the versioned
//! `WRCTXEXP` bytes, binds them to the operator's trusted manifest and replays the successor
//! chain with the production role-proof verifier (`verify_core` under `role_requirement`), each
//! record under the keys active before it. Only the replayed chain authenticates anything; every
//! other field is returned as `AuditDerived`, readable only through fail-safe predicates.
use crate::consensus::ROOT2_COMMAND_ENVELOPE_MAX1;
use crate::storage::mutation_visibility::root2::admin::key::check_enrollable_admin_key2;
use crate::storage::mutation_visibility::root2::codec::hash;
use crate::storage::mutation_visibility::root2::command::Command2;
use crate::storage::mutation_visibility::root2::role_command::{
	CONTEXT_BYTES3, ROLE_PROOF_BYTES3, context_from_bytes, role_requirement, verify_core,
};
use crate::storage::mutation_visibility::root2::role_view::RoleView3;
use crate::storage::mutation_visibility::root2::types::{Digest2, Log2};

const MAGIC: &[u8; 8] = b"WRCTXEXP";
const MANIFEST_BYTES: usize = 241;
const SUCCESSORS: u32 = 192;
const ROLE_SUCCESSORS: usize = 64;

/// Why an export is refused, in the reader's order: format (§5.1), trust binding (§5.2), then the
/// chain replay (§5.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextRefusal {
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

/// An audit-derived value: not chain-verified, so nothing yields it as trusted history; it is
/// readable only through the named fail-safe predicates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuditDerived<T>(T);
impl AuditDerived<bool> {
	/// A frozen export refuses admission statements and governance commands.
	pub fn refuses_while_frozen(&self) -> bool {
		self.0
	}
}
impl AuditDerived<Option<[u8; 20]>> {
	/// A retirement refuses every further action.
	pub fn refuses_as_retired(&self) -> bool {
		self.0.is_some()
	}
	/// The Freeze/Unfreeze binding (a LogId20): a forged value fails safe as a consumed
	/// `Denied(24)`.
	pub fn freeze_binding(&self) -> [u8; 20] {
		self.0.unwrap_or([0; 20])
	}
}
impl AuditDerived<u32> {
	/// Two exports from the same asserted node are never two sources.
	pub fn same_node_as(&self, other: &Self) -> bool {
		self.0 == other.0
	}
}

/// The successors a reader replayed: per role, each re-verified successor key in epoch order
/// (epoch 1 first). Only `verify_context_export` builds one, so a view built over it is a view of
/// re-verified history.
#[derive(Clone, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct Chain3 {
	successors: [Vec<Digest2>; 3],
}
impl Chain3 {
	pub(in crate::storage::mutation_visibility::root2) fn successors(&self) -> &[Vec<Digest2>; 3] {
		&self.successors
	}
}

/// A context export that passed every check. The context, manifest, history and active epochs
/// are chain-verified (the context itself only when `context_authenticated`); the rest is
/// audit-derived.
pub struct VerifiedContext {
	context: [u8; CONTEXT_BYTES3],
	manifest: [u8; MANIFEST_BYTES],
	view: RoleView3,
	original: [Digest2; 3],
	chain: Chain3,
	history: Vec<(u8, u64, [u8; 32])>,
	context_authenticated: bool,
	frozen: AuditDerived<bool>,
	transition: AuditDerived<Option<[u8; 20]>>,
	retired: AuditDerived<Option<[u8; 20]>>,
	node_id: AuditDerived<u32>,
	// Audit-derived and carried as such; by design no accessor reads them.
	#[allow(dead_code)]
	stop: AuditDerived<[u8; 20]>,
	#[allow(dead_code)]
	activations: AuditDerived<Vec<[u8; 20]>>,
}
impl VerifiedContext {
	pub fn context(&self) -> &[u8; CONTEXT_BYTES3] {
		&self.context
	}
	pub fn manifest(&self) -> &[u8; MANIFEST_BYTES] {
		&self.manifest
	}
	/// `(role, epoch, key)` of every re-verified successor, in log order.
	pub fn history(&self) -> &[(u8, u64, [u8; 32])] {
		&self.history
	}
	/// The active `(epoch, key)` per role (admin, governance, recovery) after the chain.
	pub fn active(&self) -> [(u64, [u8; 32]); 3] {
		std::array::from_fn(|r| self.view.active(r))
	}
	/// `(role, epoch)` of `key` anywhere in this root's history, originals included.
	pub fn identity(&self, key: &[u8; 32]) -> Option<(u8, u64)> {
		(0..3).find_map(|r| {
			std::iter::once(&self.original[r])
				.chain(&self.chain.successors()[r])
				.position(|k| k == key)
				.map(|e| (r as u8 + 1, e as u64))
		})
	}
	/// Whether at least one successor bound this context; with none, nothing did.
	pub fn context_authenticated(&self) -> bool {
		self.context_authenticated
	}
	pub fn frozen(&self) -> &AuditDerived<bool> {
		&self.frozen
	}
	pub fn transition(&self) -> &AuditDerived<Option<[u8; 20]>> {
		&self.transition
	}
	pub fn retired(&self) -> &AuditDerived<Option<[u8; 20]>> {
		&self.retired
	}
	pub fn node_id(&self) -> &AuditDerived<u32> {
		&self.node_id
	}
	/// The view the checkers run under: built by the reader from its own replayed chain.
	pub(in crate::storage::mutation_visibility::root2) fn view(&self) -> &RoleView3 {
		&self.view
	}
	/// The key the replayed history holds for `(role, epoch)`.
	pub(in crate::storage::mutation_visibility::root2) fn key_at(
		&self,
		role: u8,
		epoch: u64,
	) -> Option<Digest2> {
		let r = usize::from(role).checked_sub(1).filter(|r| *r < 3)?;
		match usize::try_from(epoch).ok()? {
			0 => Some(self.original[r]),
			e => self.chain.successors()[r].get(e - 1).copied(),
		}
	}
}

struct Record {
	role: u8,
	epoch: u64,
	key: Digest2,
	activated: Log2,
	command: Vec<u8>,
	sidecar: Vec<u8>,
}
struct Parsed {
	context: [u8; CONTEXT_BYTES3],
	manifest: [u8; MANIFEST_BYTES],
	node_id: u32,
	stop: Log2,
	frozen: bool,
	transition: Option<Log2>,
	retired: Option<Log2>,
	successors: Vec<Record>,
}

struct Cursor<'a>(&'a [u8]);
impl<'a> Cursor<'a> {
	fn take(&mut self, n: usize) -> Result<&'a [u8], ContextRefusal> {
		if self.0.len() < n {
			return Err(ContextRefusal::Truncated);
		}
		let (head, tail) = self.0.split_at(n);
		self.0 = tail;
		Ok(head)
	}
	fn array<const N: usize>(&mut self) -> Result<[u8; N], ContextRefusal> {
		Ok(self.take(N)?.try_into().expect("taken width"))
	}
	fn u8(&mut self) -> Result<u8, ContextRefusal> {
		Ok(self.take(1)?[0])
	}
	fn u32(&mut self) -> Result<u32, ContextRefusal> {
		Ok(u32::from_le_bytes(self.array()?))
	}
	fn u64(&mut self) -> Result<u64, ContextRefusal> {
		Ok(u64::from_le_bytes(self.array()?))
	}
	fn log(&mut self) -> Result<Log2, ContextRefusal> {
		Ok(Log2 {
			term: self.u64()?,
			node: self.u32()?,
			index: self.u64()?,
		})
	}
	fn boolean(&mut self) -> Result<bool, ContextRefusal> {
		match self.u8()? {
			0 => Ok(false),
			1 => Ok(true),
			_ => Err(ContextRefusal::Boolean),
		}
	}
	fn optional(&mut self) -> Result<Option<Log2>, ContextRefusal> {
		let present = self.boolean()?;
		let log = self.log()?;
		match (present, log) {
			(true, log) => Ok(Some(log)),
			(false, l) if (l.term, l.node, l.index) == (0, 0, 0) => Ok(None),
			(false, _) => Err(ContextRefusal::Fill),
		}
	}
}

/// §5.1: the integrity digest, version, coverage, booleans, fill and caps, with the count checked
/// before any allocation and every record's lengths before its bytes.
fn parse(bytes: &[u8]) -> Result<Parsed, ContextRefusal> {
	use ContextRefusal as R;
	let Some(split) = bytes.len().checked_sub(32) else {
		return Err(R::Truncated);
	};
	let (body, digest) = bytes.split_at(split);
	if hash(b"", body)[..] != *digest {
		return Err(R::Digest);
	}
	let mut r = Cursor(body);
	if r.take(8)? != MAGIC {
		return Err(R::Magic);
	}
	if u16::from_le_bytes(r.array()?) != 1 {
		return Err(R::Version);
	}
	if r.array::<2>()? != [0, 0] {
		return Err(R::Reserved);
	}
	let context = r.array()?;
	let manifest = r.array()?;
	let node_id = r.u32()?;
	let stop = r.log()?;
	if r.u8()? != 1 {
		return Err(R::Coverage);
	}
	let frozen = r.boolean()?;
	let transition = r.optional()?;
	let retired = r.optional()?;
	let count = r.u32()?;
	if count > SUCCESSORS {
		return Err(R::Count);
	}
	let mut successors = Vec::with_capacity(count as usize);
	let mut per_role = [0usize; 3];
	for _ in 0..count {
		let role = r.u8()?;
		if !(1..=3).contains(&role) {
			return Err(R::Role);
		}
		per_role[usize::from(role - 1)] += 1;
		if per_role[usize::from(role - 1)] > ROLE_SUCCESSORS {
			return Err(R::RoleCount);
		}
		let epoch = r.u64()?;
		let key = r.array()?;
		let activated = r.log()?;
		let len = r.u32()? as usize;
		if len > ROOT2_COMMAND_ENVELOPE_MAX1 {
			return Err(R::CommandLength);
		}
		let command = r.take(len)?.to_vec();
		if r.u32()? as usize != ROLE_PROOF_BYTES3 {
			return Err(R::SidecarLength);
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
		return Err(R::Trailing);
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

fn log20(l: Log2) -> [u8; 20] {
	let mut b = [0; 20];
	b[..8].copy_from_slice(&l.term.to_le_bytes());
	b[8..12].copy_from_slice(&l.node.to_le_bytes());
	b[12..].copy_from_slice(&l.index.to_le_bytes());
	b
}
/// The rotation's role code (1 admin, 2 governance, 3 recovery) and enrolled key.
fn rotation(command: &Command2) -> Option<(u8, Digest2)> {
	match command {
		Command2::RotateAdmin(key) => Some((1, *key)),
		Command2::RotateGovernance(key) => Some((2, *key)),
		Command2::RotateRecovery(key) => Some((3, *key)),
		_ => None,
	}
}

/// C-S6b §5, steps 1-4 in order: parse; bind the manifest to the operator's trusted bytes and the
/// context's pin; replay every successor under the keys active before it; return the verified
/// chain with every unverifiable field typed as audit-derived.
pub fn verify_context_export(
	bytes: &[u8],
	trusted_manifest: &[u8],
) -> Result<VerifiedContext, ContextRefusal> {
	use ContextRefusal as R;
	// 1. Format.
	let p = parse(bytes)?;
	// 2. The trusted manifest byte for byte, then its pin in the context.
	if p.manifest[..] != *trusted_manifest {
		return Err(R::Manifest);
	}
	if !super::is_profile_three(&p.manifest) {
		return Err(R::Manifest);
	}
	let mut chain = Chain3 {
		successors: [Vec::new(), Vec::new(), Vec::new()],
	};
	let mut view = RoleView3::offline(&p.context, &p.manifest, &chain).ok_or(R::Pin)?;
	let original: [Digest2; 3] = std::array::from_fn(|r| view.active(r).1);
	// 3. Replay.
	let context = context_from_bytes(&p.context);
	let mut last = context.installed_at.index;
	let mut history = Vec::with_capacity(p.successors.len());
	for s in &p.successors {
		let (g, c, command) = Command2::decode(&s.command).map_err(|_| R::Decode)?;
		if (g, c) != (context.generation, context.cluster) {
			return Err(R::Deployment);
		}
		let (role, key) = rotation(&command).ok_or(R::Kind)?;
		if role != s.role {
			return Err(R::Kind);
		}
		if key != s.key {
			return Err(R::Key);
		}
		check_enrollable_admin_key2(&key).map_err(|_| R::Enrollable)?;
		if original.contains(&key) || chain.successors.iter().flatten().any(|k| *k == key) {
			return Err(R::Duplicate);
		}
		let i = usize::from(role - 1);
		if s.epoch != chain.successors[i].len() as u64 + 1 {
			return Err(R::Epoch);
		}
		if s.activated.index <= last {
			return Err(R::Order);
		}
		let digest = command.digest(g, c).map_err(|_| R::Decode)?;
		let (expected, form) = role_requirement(&command, s.sidecar[76]).ok_or(R::Kind)?;
		if verify_core(&view, &s.sidecar, digest, expected, form) != Ok(true) {
			return Err(R::Proof);
		}
		last = s.activated.index;
		chain.successors[i].push(key);
		history.push((role, s.epoch, key));
		view = RoleView3::offline(&p.context, &p.manifest, &chain).ok_or(R::Pin)?;
	}
	// 4. The verified chain and the audit-derived rest.
	Ok(VerifiedContext {
		context: p.context,
		manifest: p.manifest,
		view,
		original,
		chain,
		history,
		context_authenticated: !p.successors.is_empty(),
		frozen: AuditDerived(p.frozen),
		transition: AuditDerived(p.transition.map(log20)),
		retired: AuditDerived(p.retired.map(log20)),
		node_id: AuditDerived(p.node_id),
		stop: AuditDerived(log20(p.stop)),
		activations: AuditDerived(p.successors.iter().map(|s| log20(s.activated)).collect()),
	})
}
