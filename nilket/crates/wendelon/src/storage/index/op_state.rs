//! The owner's durable half of ADR 0697 §5.6: every allocated operation has
//! exactly one local row from the moment it exists, so the local row count is the
//! allocated span and cannot understate what the state machine retains.
//!
//! - `Intent`: undecided, or decided with its local effects not yet durable.
//! - `AckPending`: its local effects are durable; only the acknowledgement remains.
//! - Absent: at or below `known_acked_through`.

use super::*;
use crate::consensus::slab_ops::{OPS_WINDOW_W, OpKey, SlabCommand};
use redb::ReadableTableMetadata;

const OP_INTENTS_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("slab_op_intents_v1");
const OP_ACK_PENDING_TABLE: TableDefinition<u64, bool> =
	TableDefinition::new("slab_op_ack_pending_v1");
const REJECTED_TIER_CANDIDATES_TABLE: TableDefinition<u64, &[u8]> =
	TableDefinition::new("rejected_tier_candidates_v1");
const META_OP_STATE_VERSION: &str = "op_state_version_v1";
const META_NEXT_OP_SEQ: &str = "next_op_seq_v1";
const META_KNOWN_ACKED_THROUGH: &str = "known_acked_through_v1";
const OP_STATE_VERSION: u64 = 1;

/// What an intent's phase guarantees.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum OpPhase {
	/// Never handed to Raft by any process.
	Reserved,
	/// Written before the first `client_write`. It guarantees nothing about
	/// delivery: the operation may or may not have been appended.
	MayBeSubmitted,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct OpIntent {
	pub digest: [u8; 32],
	pub command: SlabCommand,
	pub phase: OpPhase,
	/// The exact local input authority an earlier op of this workflow left for
	/// this one, which it must still find.
	pub expected_inputs: Option<Vec<LsmInputAuthority>>,
}

/// A rejected tier candidate's identity, kept after its operation's Intent is
/// gone. The upload is retained, never deleted on this evidence.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct RejectedTierCandidate {
	pub op_seq: u64,
	pub slab_id: SlabId,
	pub l2_uri: String,
	pub payload_hash: [u8; 32],
}

/// The sole argument a committer can submit: its durable intent exists and is
/// `MayBeSubmitted`. Only this module constructs one, so a proposal without a
/// durable intent, or a reserved operation that skipped its transition, does not
/// typecheck.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpTicket {
	key: OpKey,
	digest: [u8; 32],
	command: SlabCommand,
	expected_inputs: Option<Vec<LsmInputAuthority>>,
}

impl OpTicket {
	pub fn key(&self) -> OpKey {
		self.key
	}

	pub fn digest(&self) -> [u8; 32] {
		self.digest
	}

	pub fn command(&self) -> &SlabCommand {
		&self.command
	}
}

/// A later operation of a workflow, durable but not yet submittable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReservedOp {
	key: OpKey,
	digest: [u8; 32],
	command: SlabCommand,
	expected_inputs: Option<Vec<LsmInputAuthority>>,
}

impl ReservedOp {
	pub(crate) fn key(&self) -> OpKey {
		self.key
	}

	pub(crate) fn command(&self) -> &SlabCommand {
		&self.command
	}
}

/// A workflow's whole span: the operation to submit now and its later ones.
#[derive(Debug)]
pub(crate) struct OpAllocation {
	pub first: OpTicket,
	pub later: Vec<ReservedOp>,
	/// The linked LSM intent as stored, naming these operations.
	pub linked: Option<LsmInstallIntent>,
}

/// An allocated operation as a reopen finds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum LoadedOp {
	Submittable(OpTicket),
	Reserved(ReservedOp),
}

impl LoadedOp {
	/// The input authority this operation's workflow left for it.
	pub(crate) fn reserved_inputs(&self) -> Option<&[LsmInputAuthority]> {
		match self {
			Self::Submittable(ticket) => ticket.expected_inputs.as_deref(),
			Self::Reserved(reserved) => reserved.expected_inputs.as_deref(),
		}
	}

	/// Whether this is a reserved op whose earlier workflow step has not left
	/// its evidence: it cannot run yet.
	pub(crate) fn awaits_its_workflow(&self) -> bool {
		matches!(self, Self::Reserved(reserved) if reserved.expected_inputs.is_none())
	}
}

/// The oldest allocated sequence still holding the window, for status.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OpHolder {
	pub op_seq: u64,
	/// The transition, while its intent still names one.
	pub transition: Option<&'static str>,
	/// Why it holds the window.
	pub state: OpHolderState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OpHolderState {
	Reserved,
	MayBeSubmitted,
	AwaitingAck {
		acked: bool,
	},
	/// A sequence in the span with no row: the invariant is broken.
	Missing,
}

/// The window has no room for a producer's whole span. Refused before anything
/// is allocated or written; only new work is refused, never a drain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TerminalBacklog {
	pub next_op_seq: u64,
	pub known_acked_through: u64,
	pub requested: u64,
	pub holder: Option<OpHolder>,
}

impl std::fmt::Display for TerminalBacklog {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(
			f,
			"TerminalBacklog: {} more ops do not fit the window after {} (next {}); held by {:?}",
			self.requested, self.known_acked_through, self.next_op_seq, self.holder
		)
	}
}

impl std::error::Error for TerminalBacklog {}

/// The durable operation state is absent, partial or inconsistent. Never read as
/// a fresh sequence space: `next_op_seq` does not default.
#[derive(Debug)]
struct OpStateInvalid(String);

impl std::fmt::Display for OpStateInvalid {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "operation state is unusable: {}", self.0)
	}
}

impl std::error::Error for OpStateInvalid {}

fn op_state_invalid(reason: impl Into<String>) -> Error {
	Error::new(ErrorKind::InvalidData, OpStateInvalid(reason.into()))
}

pub(crate) fn is_op_state_invalid(error: &Error) -> bool {
	error
		.get_ref()
		.is_some_and(|cause| cause.is::<OpStateInvalid>())
}

fn encode_intent(intent: &OpIntent) -> std::io::Result<Vec<u8>> {
	postcard::to_stdvec(intent).map_err(Error::other)
}

fn decode_intent(op_seq: u64, bytes: &[u8]) -> std::io::Result<OpIntent> {
	let intent: OpIntent = postcard::from_bytes(bytes)
		.map_err(|error| op_state_invalid(format!("intent {op_seq} does not decode: {error}")))?;
	if intent.command.digest() != intent.digest {
		return Err(op_state_invalid(format!(
			"intent {op_seq} does not match its digest"
		)));
	}
	Ok(intent)
}

fn loaded_op(op_seq: u64, intent: OpIntent) -> LoadedOp {
	let key = OpKey {
		owner: intent.command.owner(),
		op_seq,
	};
	match intent.phase {
		OpPhase::MayBeSubmitted => LoadedOp::Submittable(OpTicket {
			key,
			digest: intent.digest,
			command: intent.command,
			expected_inputs: intent.expected_inputs,
		}),
		OpPhase::Reserved => LoadedOp::Reserved(ReservedOp {
			key,
			digest: intent.digest,
			command: intent.command,
			expected_inputs: intent.expected_inputs,
		}),
	}
}

/// Write the initial operation state of a store being created.
pub(super) fn initialize_op_state(tx: &redb::WriteTransaction) -> std::io::Result<()> {
	let mut meta = tx.open_table(META_TABLE).map_err(redb_to_io)?;
	meta.insert(META_OP_STATE_VERSION, &OP_STATE_VERSION)
		.map_err(redb_to_io)?;
	meta.insert(META_NEXT_OP_SEQ, &1).map_err(redb_to_io)?;
	meta.insert(META_KNOWN_ACKED_THROUGH, &0)
		.map_err(redb_to_io)?;
	drop(meta);
	tx.open_table(OP_INTENTS_TABLE).map_err(redb_to_io)?;
	tx.open_table(OP_ACK_PENDING_TABLE).map_err(redb_to_io)?;
	tx.open_table(REJECTED_TIER_CANDIDATES_TABLE)
		.map_err(redb_to_io)?;
	Ok(())
}

fn read_counters(meta: &impl ReadableTable<&'static str, u64>) -> std::io::Result<(u64, u64)> {
	let read = |key: &'static str| -> std::io::Result<Option<u64>> {
		Ok(meta
			.get(key)
			.map_err(redb_to_io)?
			.map(|value| value.value()))
	};
	match (
		read(META_OP_STATE_VERSION)?,
		read(META_NEXT_OP_SEQ)?,
		read(META_KNOWN_ACKED_THROUGH)?,
	) {
		(Some(OP_STATE_VERSION), Some(next), Some(known)) => {
			if next == 0 || known >= next {
				return Err(op_state_invalid(format!(
					"next_op_seq {next} and known_acked_through {known} are inconsistent"
				)));
			}
			Ok((next, known))
		}
		(Some(version), Some(_), Some(_)) => Err(op_state_invalid(format!(
			"unsupported operation state version {version}"
		))),
		_ => Err(op_state_invalid(
			"operation state metadata is absent or partial",
		)),
	}
}

/// §5.6's invariant, read at open: every sequence in the span has exactly one
/// row, and no row lies outside it.
pub(super) fn validate_op_state(tx: &redb::WriteTransaction) -> std::io::Result<()> {
	let meta = tx.open_table(META_TABLE).map_err(redb_to_io)?;
	let (next, known) = read_counters(&meta)?;
	drop(meta);
	let intents = tx.open_table(OP_INTENTS_TABLE).map_err(redb_to_io)?;
	let pending = tx.open_table(OP_ACK_PENDING_TABLE).map_err(redb_to_io)?;
	let span = next - 1 - known;
	if span > OPS_WINDOW_W {
		return Err(op_state_invalid(format!(
			"allocated span {span} exceeds the window"
		)));
	}
	for seq in known + 1..next {
		let intent = intents.get(&seq).map_err(redb_to_io)?;
		let ack = pending.get(&seq).map_err(redb_to_io)?;
		match (&intent, &ack) {
			(Some(bytes), None) => {
				decode_intent(seq, bytes.value())?;
			}
			(None, Some(_)) => {}
			(None, None) => return Err(op_state_invalid(format!("seq {seq} has no row"))),
			(Some(_), Some(_)) => return Err(op_state_invalid(format!("seq {seq} has two rows"))),
		}
	}
	let rows = intents.len().map_err(redb_to_io)? + pending.len().map_err(redb_to_io)?;
	if rows != span {
		return Err(op_state_invalid(format!(
			"{rows} rows for an allocated span of {span}"
		)));
	}
	Ok(())
}

fn oldest_holder(
	known: u64,
	next: u64,
	intents: &impl ReadableTable<u64, &'static [u8]>,
	pending: &impl ReadableTable<u64, bool>,
) -> std::io::Result<Option<OpHolder>> {
	let op_seq = known + 1;
	if op_seq >= next {
		return Ok(None);
	}
	if let Some(bytes) = intents.get(&op_seq).map_err(redb_to_io)? {
		let intent = decode_intent(op_seq, bytes.value())?;
		let state = match intent.phase {
			OpPhase::Reserved => OpHolderState::Reserved,
			OpPhase::MayBeSubmitted => OpHolderState::MayBeSubmitted,
		};
		let transition = match intent.command {
			SlabCommand::Tier(_) => "tier",
			SlabCommand::Retire(_) => "retire",
			SlabCommand::Compaction(_) => "compaction",
			SlabCommand::GcRemove(_) => "gc_remove",
		};
		return Ok(Some(OpHolder {
			op_seq,
			transition: Some(transition),
			state,
		}));
	}
	let state = match pending.get(&op_seq).map_err(redb_to_io)? {
		Some(acked) => OpHolderState::AwaitingAck {
			acked: acked.value(),
		},
		None => OpHolderState::Missing,
	};
	Ok(Some(OpHolder {
		op_seq,
		transition: None,
		state,
	}))
}

/// A fault at an operation transaction's commit: nothing committed, or it
/// committed and its caller never learns.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OpTxnCut {
	BeforeCommit,
	AfterCommit,
}

fn commit_op_txn(
	tx: redb::WriteTransaction,
	#[cfg(test)] cut: Option<OpTxnCut>,
) -> std::io::Result<()> {
	#[cfg(test)]
	if cut == Some(OpTxnCut::BeforeCommit) {
		drop(tx);
		return Err(Error::other("injected fault before the op commit"));
	}
	tx.commit().map_err(redb_to_io)?;
	#[cfg(test)]
	if cut == Some(OpTxnCut::AfterCommit) {
		return Err(Error::other("injected loss of the op commit's result"));
	}
	Ok(())
}

/// What else a handoff makes durable, in its one transaction.
enum HandoffEvidence {
	None,
	Removal(OpKey, Vec<LsmInputAuthority>),
	RejectedTier(RejectedTierCandidate),
}

impl PersistentLocalIndex {
	#[cfg(test)]
	pub(crate) fn cut_next_op_txn(&self, cut: OpTxnCut) {
		*self.op_txn_cut.lock().unwrap() = Some(cut);
	}

	#[cfg(test)]
	fn take_op_txn_cut(&self) -> Option<OpTxnCut> {
		self.op_txn_cut.lock().unwrap().take()
	}

	/// Pauses the next handoff's transaction after it starts, before its commit.
	#[cfg(test)]
	pub(crate) fn hold_next_handoff_txn(
		&self,
	) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
		let (entered_tx, entered_rx) = std::sync::mpsc::channel();
		let (release_tx, release_rx) = std::sync::mpsc::channel();
		*self.op_handoff_hold.lock().unwrap() = Some((entered_tx, release_rx));
		(entered_rx, release_tx)
	}

	/// Allocate a workflow's whole span in one transaction, with its linked LSM
	/// intent if it has one: there is no crash interval between the two, and no
	/// ticket exists unless the commit returned Ok. The backlog bound is checked
	/// before anything is written. The first operation is `MayBeSubmitted`; each
	/// later one stays `Reserved` until its own transition.
	pub(crate) async fn allocate_ops(
		&self,
		commands: Vec<SlabCommand>,
		linked: Option<LsmInstallIntent>,
	) -> std::io::Result<OpAllocation> {
		if commands.is_empty() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"an operation workflow needs at least one command",
			));
		}
		#[cfg(test)]
		let cut = self.take_op_txn_cut();
		let _write = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				let keys;
				let mut stored = None;
				{
					let mut meta = tx.open_table(META_TABLE).map_err(redb_to_io)?;
					let (next, known) = read_counters(&meta)?;
					let mut intents = tx.open_table(OP_INTENTS_TABLE).map_err(redb_to_io)?;
					let pending = tx.open_table(OP_ACK_PENDING_TABLE).map_err(redb_to_io)?;
					let requested = commands.len() as u64;
					let last = (next - 1).checked_add(requested);
					let fits = last.is_some_and(|last| last - known <= OPS_WINDOW_W);
					let Some(after) = next.checked_add(requested).filter(|_| fits) else {
						let holder = oldest_holder(known, next, &intents, &pending)?;
						return Err(Error::new(
							ErrorKind::WouldBlock,
							TerminalBacklog {
								next_op_seq: next,
								known_acked_through: known,
								requested,
								holder,
							},
						));
					};
					let mut allocated = Vec::with_capacity(commands.len());
					for (offset, command) in commands.into_iter().enumerate() {
						let op_seq = next + offset as u64;
						let intent = OpIntent {
							digest: command.digest(),
							phase: if offset == 0 {
								OpPhase::MayBeSubmitted
							} else {
								OpPhase::Reserved
							},
							command,
							expected_inputs: None,
						};
						let bytes = encode_intent(&intent)?;
						intents
							.insert(&op_seq, bytes.as_slice())
							.map_err(redb_to_io)?;
						allocated.push((op_seq, intent));
					}
					meta.insert(META_NEXT_OP_SEQ, &after).map_err(redb_to_io)?;
					keys = allocated;
				}
				if let Some(mut intent) = linked {
					let commit_bytes = serde_json::to_vec(&intent.commit).map_err(Error::other)?;
					if *blake3::hash(&commit_bytes).as_bytes() != intent.commit_digest {
						return Err(Error::new(
							ErrorKind::InvalidInput,
							"LSM intent digest mismatch",
						));
					}
					intent.ops = keys
						.iter()
						.map(|(op_seq, op)| OpKey {
							owner: op.command.owner(),
							op_seq: *op_seq,
						})
						.collect();
					let job = intent.commit.job_id.get();
					let bytes = serde_json::to_vec(&intent).map_err(Error::other)?;
					let mut table = tx
						.open_table(LSM_INSTALL_INTENTS_TABLE)
						.map_err(redb_to_io)?;
					if table.get(&job).map_err(redb_to_io)?.is_some() {
						return Err(Error::new(
							ErrorKind::AlreadyExists,
							"an LSM intent already exists for this job",
						));
					}
					table.insert(&job, bytes.as_slice()).map_err(redb_to_io)?;
					drop(table);
					stored = Some(intent);
				}
				commit_op_txn(
					tx,
					#[cfg(test)]
					cut,
				)?;
				let mut loaded = keys.into_iter().map(|(op_seq, op)| loaded_op(op_seq, op));
				let Some(LoadedOp::Submittable(first)) = loaded.next() else {
					return Err(Error::other("the first allocated op is submittable"));
				};
				let later = loaded
					.map(|op| match op {
						LoadedOp::Reserved(reserved) => Ok(reserved),
						LoadedOp::Submittable(_) => {
							Err(Error::other("a later allocated op is reserved"))
						}
					})
					.collect::<std::io::Result<Vec<_>>>()?;
				Ok(OpAllocation {
					first,
					later,
					linked: stored,
				})
			})
			.await
	}

	/// The durable transition before a reserved operation's first submission.
	/// Commit uncertainty yields no ticket, so nothing can be submitted on it.
	pub(crate) async fn mark_may_be_submitted(
		&self,
		reserved: ReservedOp,
	) -> std::io::Result<OpTicket> {
		#[cfg(test)]
		let cut = self.take_op_txn_cut();
		let _write = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				{
					let mut intents = tx.open_table(OP_INTENTS_TABLE).map_err(redb_to_io)?;
					let op_seq = reserved.key.op_seq;
					let mut intent = match intents.get(&op_seq).map_err(redb_to_io)? {
						Some(bytes) => decode_intent(op_seq, bytes.value())?,
						None => {
							return Err(op_state_invalid(format!(
								"reserved op {op_seq} has no intent"
							)));
						}
					};
					if intent.digest != reserved.digest {
						return Err(op_state_invalid(format!(
							"reserved op {op_seq} changed its digest"
						)));
					}
					intent.phase = OpPhase::MayBeSubmitted;
					let bytes = encode_intent(&intent)?;
					intents
						.insert(&op_seq, bytes.as_slice())
						.map_err(redb_to_io)?;
				}
				commit_op_txn(
					tx,
					#[cfg(test)]
					cut,
				)?;
				Ok(OpTicket {
					key: reserved.key,
					digest: reserved.digest,
					command: reserved.command,
					expected_inputs: reserved.expected_inputs,
				})
			})
			.await
	}

	/// Every undecided operation, each minting only its original identity: a
	/// retry never gets a new sequence.
	pub(crate) async fn load_op_intents(&self) -> std::io::Result<Vec<LoadedOp>> {
		self.db
			.run(|db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				let intents = tx.open_table(OP_INTENTS_TABLE).map_err(redb_to_io)?;
				let mut loaded = Vec::new();
				for row in intents.iter().map_err(redb_to_io)? {
					let (op_seq, bytes) = row.map_err(redb_to_io)?;
					let op_seq = op_seq.value();
					loaded.push(loaded_op(op_seq, decode_intent(op_seq, bytes.value())?));
				}
				Ok(loaded)
			})
			.await
	}

	/// This operation's local effects are durable: one transaction deletes its
	/// intent and writes `AckPending`, clearing its exact linked LSM intent with
	/// it. The Ack never precedes this.
	pub(crate) async fn hand_off_op(
		&self,
		key: OpKey,
		digest: [u8; 32],
		linked: Option<LsmInstallIntent>,
	) -> std::io::Result<()> {
		self.hand_off_op_with(key, digest, linked, HandoffEvidence::None, None)
			.await
	}

	/// Hand off under `authority`, which the blocking transaction itself owns
	/// until it has committed or rolled back. Cancelling the caller then cannot
	/// release the authority while the commit is still to come.
	pub(crate) async fn hand_off_op_under(
		&self,
		key: OpKey,
		digest: [u8; 32],
		authority: Box<dyn std::any::Any + Send>,
	) -> std::io::Result<()> {
		self.hand_off_op_with(key, digest, None, HandoffEvidence::None, Some(authority))
			.await
	}

	async fn hand_off_op_with(
		&self,
		key: OpKey,
		digest: [u8; 32],
		linked: Option<LsmInstallIntent>,
		evidence: HandoffEvidence,
		authority: Option<Box<dyn std::any::Any + Send>>,
	) -> std::io::Result<()> {
		let linked = linked
			.map(|intent| {
				serde_json::to_vec(&intent)
					.map(|bytes| (intent, bytes))
					.map_err(Error::other)
			})
			.transpose()?;
		#[cfg(test)]
		let cut = self.take_op_txn_cut();
		let _write = self.write_lock.lock().await;
		#[cfg(test)]
		let hold = self.op_handoff_hold.lock().unwrap().take();
		self.db
			.run(move |db| {
				let _authority = authority;
				#[cfg(test)]
				if let Some((entered, release)) = hold {
					let _ = entered.send(());
					let _ = release.recv();
				}
				let tx = db.begin_write().map_err(redb_to_io)?;
				{
					let mut intents = tx.open_table(OP_INTENTS_TABLE).map_err(redb_to_io)?;
					let mut pending = tx.open_table(OP_ACK_PENDING_TABLE).map_err(redb_to_io)?;
					let op_seq = key.op_seq;
					let intent = match intents.get(&op_seq).map_err(redb_to_io)? {
						Some(bytes) => decode_intent(op_seq, bytes.value())?,
						None => {
							return Err(op_state_invalid(format!(
								"op {op_seq} has no intent to hand off"
							)));
						}
					};
					if intent.digest != digest || intent.command.owner() != key.owner {
						return Err(op_state_invalid(format!(
							"op {op_seq} is not the operation handed off"
						)));
					}
					intents.remove(&op_seq).map_err(redb_to_io)?;
					pending.insert(&op_seq, &false).map_err(redb_to_io)?;
					if let Some((intent, bytes)) = &linked {
						if !intent.ops.contains(&key) {
							return Err(op_state_invalid(format!(
								"op {op_seq} is not linked to this LSM intent"
							)));
						}
						let job = intent.commit.job_id.get();
						let mut table = tx
							.open_table(LSM_INSTALL_INTENTS_TABLE)
							.map_err(redb_to_io)?;
						let matches = table
							.get(&job)
							.map_err(redb_to_io)?
							.is_some_and(|existing| existing.value() == bytes.as_slice());
						if !matches {
							return Err(Error::new(
								ErrorKind::InvalidData,
								"LSM intent changed or disappeared",
							));
						}
						table.remove(&job).map_err(redb_to_io)?;
					}
					match evidence {
						HandoffEvidence::None => {}
						HandoffEvidence::Removal(removal, inputs) => {
							let seq = removal.op_seq;
							let mut reserved = match intents.get(&seq).map_err(redb_to_io)? {
								Some(bytes) => decode_intent(seq, bytes.value())?,
								None => {
									return Err(op_state_invalid(format!(
										"reserved removal {seq} has no intent"
									)));
								}
							};
							reserved.expected_inputs = Some(inputs);
							let bytes = encode_intent(&reserved)?;
							intents.insert(&seq, bytes.as_slice()).map_err(redb_to_io)?;
						}
						HandoffEvidence::RejectedTier(candidate) => {
							let bytes = postcard::to_stdvec(&candidate).map_err(Error::other)?;
							tx.open_table(REJECTED_TIER_CANDIDATES_TABLE)
								.map_err(redb_to_io)?
								.insert(&candidate.op_seq, bytes.as_slice())
								.map_err(redb_to_io)?;
						}
					}
				}
				commit_op_txn(
					tx,
					#[cfg(test)]
					cut,
				)
			})
			.await
	}

	/// Hand off a GC mark and leave its reserved removal the exact input authority
	/// the mark's own revision change produced, in one transaction: the mark's
	/// linked intent, which held the authority until now, goes with it.
	pub(crate) async fn hand_off_gc_mark(
		&self,
		ticket: &OpTicket,
		intent: LsmInstallIntent,
		removal: OpKey,
		inputs: Vec<LsmInputAuthority>,
	) -> std::io::Result<()> {
		self.hand_off_op_with(
			ticket.key(),
			ticket.digest(),
			Some(intent),
			HandoffEvidence::Removal(removal, inputs),
			None,
		)
		.await
	}

	/// Hand off a definitely rejected tier operation, recording its candidate's
	/// identity in the same transaction: the Intent was its only record.
	pub(crate) async fn hand_off_rejected_tier(&self, ticket: &OpTicket) -> std::io::Result<()> {
		let SlabCommand::Tier(op) = ticket.command() else {
			return Err(op_state_invalid(format!(
				"op {:?} is not a tier operation",
				ticket.key()
			)));
		};
		let candidate = RejectedTierCandidate {
			op_seq: ticket.key().op_seq,
			slab_id: op.tier.slab.local_id(),
			l2_uri: op.tier.l2_uri.clone(),
			payload_hash: op.tier.payload_hash,
		};
		self.hand_off_op_with(
			ticket.key(),
			ticket.digest(),
			None,
			HandoffEvidence::RejectedTier(candidate),
			None,
		)
		.await
	}

	/// Every rejected tier candidate still accounted for.
	pub(crate) async fn load_rejected_tier_candidates(
		&self,
	) -> std::io::Result<Vec<RejectedTierCandidate>> {
		self.db
			.run(|db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				let table = tx
					.open_table(REJECTED_TIER_CANDIDATES_TABLE)
					.map_err(redb_to_io)?;
				let mut rows = Vec::new();
				for row in table.iter().map_err(redb_to_io)? {
					let (_, bytes) = row.map_err(redb_to_io)?;
					rows.push(postcard::from_bytes(bytes.value()).map_err(Error::other)?);
				}
				Ok(rows)
			})
			.await
	}

	/// Whether `k` more operations fit, for a producer about to create local state
	/// it would have to undo. Allocation still checks for itself.
	pub(crate) async fn ensure_op_room(&self, requested: u64) -> std::io::Result<()> {
		self.db
			.run(move |db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				let meta = tx.open_table(META_TABLE).map_err(redb_to_io)?;
				let (next, known) = read_counters(&meta)?;
				let fits = (next - 1)
					.checked_add(requested)
					.is_some_and(|last| last - known <= OPS_WINDOW_W);
				if fits {
					return Ok(());
				}
				let intents = tx.open_table(OP_INTENTS_TABLE).map_err(redb_to_io)?;
				let pending = tx.open_table(OP_ACK_PENDING_TABLE).map_err(redb_to_io)?;
				let holder = oldest_holder(known, next, &intents, &pending)?;
				Err(Error::new(
					ErrorKind::WouldBlock,
					TerminalBacklog {
						next_op_seq: next,
						known_acked_through: known,
						requested,
						holder,
					},
				))
			})
			.await
	}

	/// The rows an Ack still has to reach or a prefix read has to retire.
	pub(crate) async fn load_ack_pending(&self) -> std::io::Result<Vec<(u64, bool)>> {
		self.db
			.run(|db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				let pending = tx.open_table(OP_ACK_PENDING_TABLE).map_err(redb_to_io)?;
				let mut rows = Vec::new();
				for row in pending.iter().map_err(redb_to_io)? {
					let (op_seq, acked) = row.map_err(redb_to_io)?;
					rows.push((op_seq.value(), acked.value()));
				}
				Ok(rows)
			})
			.await
	}

	/// Exactly the sequences an Ack receipt accepted become acked.
	pub(crate) async fn mark_ops_acked(&self, seqs: Vec<u64>) -> std::io::Result<()> {
		#[cfg(test)]
		let cut = self.take_op_txn_cut();
		let _write = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				{
					let mut pending = tx.open_table(OP_ACK_PENDING_TABLE).map_err(redb_to_io)?;
					for seq in seqs {
						// A sequence already below the known prefix has no row.
						if pending.get(&seq).map_err(redb_to_io)?.is_some() {
							pending.insert(&seq, &true).map_err(redb_to_io)?;
						}
					}
				}
				commit_op_txn(
					tx,
					#[cfg(test)]
					cut,
				)
			})
			.await
	}

	/// A barrier read's canonical prefix: delete the covered rows and advance
	/// `known_acked_through` in one transaction. A stale value changes nothing,
	/// and a prefix crossing a local intent or past an allocated sequence is an
	/// anomaly that changes nothing either.
	pub(crate) async fn advance_known_acked_through(&self, canonical: u64) -> std::io::Result<()> {
		#[cfg(test)]
		let cut = self.take_op_txn_cut();
		let _write = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				{
					let mut meta = tx.open_table(META_TABLE).map_err(redb_to_io)?;
					let (next, known) = read_counters(&meta)?;
					if canonical <= known {
						return Ok(());
					}
					if canonical >= next {
						return Err(op_state_invalid(format!(
							"canonical prefix {canonical} is past allocated {next}"
						)));
					}
					let intents = tx.open_table(OP_INTENTS_TABLE).map_err(redb_to_io)?;
					let mut pending = tx.open_table(OP_ACK_PENDING_TABLE).map_err(redb_to_io)?;
					for seq in known + 1..=canonical {
						if intents.get(&seq).map_err(redb_to_io)?.is_some() {
							return Err(op_state_invalid(format!(
								"canonical prefix {canonical} crosses the open intent {seq}"
							)));
						}
						pending.remove(&seq).map_err(redb_to_io)?;
					}
					meta.insert(META_KNOWN_ACKED_THROUGH, &canonical)
						.map_err(redb_to_io)?;
				}
				commit_op_txn(
					tx,
					#[cfg(test)]
					cut,
				)
			})
			.await
	}

	/// The oldest allocated sequence holding the window, for status.
	pub(crate) async fn oldest_op_holder(&self) -> std::io::Result<Option<OpHolder>> {
		self.db
			.run(|db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				let meta = tx.open_table(META_TABLE).map_err(redb_to_io)?;
				let (next, known) = read_counters(&meta)?;
				let intents = tx.open_table(OP_INTENTS_TABLE).map_err(redb_to_io)?;
				let pending = tx.open_table(OP_ACK_PENDING_TABLE).map_err(redb_to_io)?;
				oldest_holder(known, next, &intents, &pending)
			})
			.await
	}
}

#[cfg(test)]
pub(crate) fn remove_op_state_meta_for_test(path: &Path, key: &str) {
	let db = Database::open(path).unwrap();
	let write_txn = db.begin_write().unwrap();
	{
		let mut meta = write_txn.open_table(META_TABLE).unwrap();
		meta.remove(key).unwrap();
	}
	write_txn.commit().unwrap();
}

#[cfg(test)]
#[path = "op_state/tests.rs"]
mod tests;
