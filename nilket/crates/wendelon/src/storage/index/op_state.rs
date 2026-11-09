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
use std::sync::Arc;

const OP_INTENTS_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("slab_op_intents_v1");
const OP_ACK_PENDING_TABLE: TableDefinition<u64, (bool, [u8; 32])> =
	TableDefinition::new("slab_op_ack_pending_v1");
const RESERVED_SLAB_IDS_TABLE: TableDefinition<u64, &[u8]> =
	TableDefinition::new("reserved_slab_ids_v1");
const REJECTED_TIER_CANDIDATES_TABLE: TableDefinition<u64, &[u8]> =
	TableDefinition::new("rejected_tier_candidates_v1");
const META_OP_STATE_VERSION: &str = "op_state_version_v1";
const META_NEXT_OP_SEQ: &str = "next_op_seq_v1";
const META_KNOWN_ACKED_THROUGH: &str = "known_acked_through_v1";
const OP_STATE_VERSION: u64 = 3;

/// What an intent's phase guarantees.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum OpPhase {
	/// Never handed to Raft by any process.
	Reserved,
	/// Written before the first `client_write`. It guarantees nothing about
	/// delivery: the operation may or may not have been appended.
	MayBeSubmitted,
}

/// The exact local file a terminal workflow disposes of: its content, its
/// publication revision, and the inode that content was read from.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct GcFileIdentity {
	pub length: u64,
	pub digest: [u8; 32],
	pub revision: u64,
	pub dev: u64,
	pub ino: u64,
}

/// How far a terminal workflow's local work has gone (ADR 0697 §6.1), on the
/// operation that continues it. The order is the T1 order: nothing here precedes
/// its authority, and each step names exactly what the last one recorded.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum OpProgress {
	/// On a mark: its DeletePending run is durable, in the transaction that wrote
	/// this, and `left` is the input authority its handoff leaves for its removal.
	MarkPublished { left: Vec<LsmInputAuthority> },
	/// On a mark, from its allocation: what a rejection restores, exactly.
	MarkPrepared {
		pre_run: LsmRunSummary,
		pre_delete_pending: bool,
	},
	/// P1/P2: recorded before the quarantine link. The original and the
	/// quarantine entry on disk say which of the two it reached.
	QuarantineRecorded(GcFileIdentity),
	/// P3: the run is gone, durably, in the transaction that wrote this. `None`
	/// only for a store outside a ring whose file an earlier local release took.
	RunDropped(Option<GcFileIdentity>),
	/// P3c: Complete applied, and purging this entry is authorized. It says
	/// nothing about whether the unlink has happened.
	PurgeAuthorized(GcFileIdentity),
	/// P4: the unlink and its directory sync finished.
	Purged(GcFileIdentity),
	/// On a mark: decided rejected and undone. Its reserved removal must be
	/// Cancelled before the two operations hand off together.
	ClaimRejected {
		reason: crate::consensus::slab_ops::SlabRejection,
		pre_run: LsmRunSummary,
		/// Whether the pin table already said delete-pending before the mark.
		pre_delete_pending: bool,
		removal: OpKey,
	},
}

impl OpProgress {
	/// Whether this may directly follow `previous` on one operation's row.
	fn follows(&self, previous: Option<&OpProgress>) -> bool {
		match (previous, self) {
			(None, Self::QuarantineRecorded(_) | Self::ClaimRejected { .. }) => true,
			(None, Self::RunDropped(None)) => true,
			(Some(Self::MarkPrepared { .. }), Self::ClaimRejected { .. }) => true,
			(Some(Self::MarkPrepared { .. }), Self::MarkPublished { .. }) => true,
			(Some(Self::QuarantineRecorded(recorded)), Self::RunDropped(Some(file))) => {
				recorded == file
			}
			(Some(Self::RunDropped(Some(dropped))), Self::PurgeAuthorized(file)) => dropped == file,
			(Some(Self::PurgeAuthorized(authorized)), Self::Purged(file)) => authorized == file,
			_ => false,
		}
	}
}

/// One step of an operation's local progress, written in the caller's transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OpProgressUpdate {
	pub key: OpKey,
	pub digest: [u8; 32],
	pub progress: OpProgress,
}

/// What this store durably knows about one sequence, asked about one digest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OpEvidence {
	/// This exact operation is undecided, or decided with local work outstanding.
	Intent(Option<OpProgress>),
	/// This exact operation's local work is finished; its row says so.
	Finished,
	/// At or below the acknowledged prefix. No row remains, so nothing here says
	/// which operation it was: a callback naming it is an obsolete duplicate to
	/// ignore, and this is never positive evidence of any local effect.
	BelowPrefix,
	/// The sequence holds a different operation, or none.
	Unknown,
}

/// ADR 0703: the active generation is not the one a certified selection was
/// decided under; nothing was written.
#[derive(Debug)]
pub(crate) struct GenerationMoved;

impl std::fmt::Display for GenerationMoved {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str("the active generation moved after the certified selection")
	}
}

impl std::error::Error for GenerationMoved {}

/// A mark decided rejected, whose reserved removal still has to be Cancelled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RejectedClaim {
	pub mark: OpTicket,
	pub removal: LoadedOp,
	pub pre_run: LsmRunSummary,
}

impl RejectedClaim {
	pub(crate) fn op_seqs(&self) -> Vec<u64> {
		let removal = match &self.removal {
			LoadedOp::Submittable(ticket) => ticket.key(),
			LoadedOp::Reserved(reserved) => reserved.key(),
		};
		vec![self.mark.key().op_seq, removal.op_seq]
	}
}

/// Delete exactly the linked LSM intent whose local work has finished, in the
/// caller's transaction. It must be the one this workflow wrote.
pub(crate) fn complete_linked_lsm_intent(
	tx: &redb::WriteTransaction,
	intent: &LsmInstallIntent,
) -> std::io::Result<()> {
	let bytes = serde_json::to_vec(intent).map_err(Error::other)?;
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
	drop(table);
	super::lsm_certificate::remove_certificate_of(tx, intent)?;
	Ok(())
}

pub(crate) fn apply_op_progress(
	tx: &redb::WriteTransaction,
	update: &OpProgressUpdate,
) -> std::io::Result<()> {
	let mut intents = tx.open_table(OP_INTENTS_TABLE).map_err(redb_to_io)?;
	let op_seq = update.key.op_seq;
	let mut intent = match intents.get(&op_seq).map_err(redb_to_io)? {
		Some(bytes) => decode_intent(op_seq, bytes.value())?,
		None => {
			return Err(op_state_invalid(format!(
				"op {op_seq} has no intent to record its progress"
			)));
		}
	};
	if intent.digest != update.digest || intent.command.owner() != update.key.owner {
		return Err(op_state_invalid(format!(
			"op {op_seq} is not the operation whose progress this is"
		)));
	}
	if !update.progress.follows(intent.progress.as_ref()) {
		return Err(op_state_invalid(format!(
			"op {op_seq} cannot move from {:?} to {:?}",
			intent.progress, update.progress
		)));
	}
	intent.progress = Some(update.progress.clone());
	let bytes = encode_intent(&intent)?;
	intents
		.insert(&op_seq, bytes.as_slice())
		.map_err(redb_to_io)?;
	Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct OpIntent {
	pub digest: [u8; 32],
	pub command: SlabCommand,
	pub phase: OpPhase,
	/// How far this operation's own workflow has gone locally.
	pub progress: Option<OpProgress>,
	/// The exact local input authority an earlier op of this workflow left for
	/// this one, which it must still find.
	pub expected_inputs: Option<Vec<LsmInputAuthority>>,
}

/// A file an abandoned reservation still has to dispose of, by its exact identity.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct AbandonedOutput {
	/// Absent when the job never left a file at this id's path: nothing to dispose
	/// of, though the id's terminal transition is still owed.
	pub file: Option<GcFileIdentity>,
	/// Whether the file was moved out of the discovery namespace, to its abandoned
	/// name. One left at its slab path is not opened by a reopen: open holds back every
	/// reserved id, since only a durable publication makes a slab.
	pub relocated: bool,
}

/// One reservation's lifecycle (ADR 0697 §4 T2). `Unsubmitted` is the only positive
/// evidence that an id was never exposed.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum ReservedIdState {
	Unsubmitted,
	Exposed {
		abandoned: Option<AbandonedOutput>,
	},
	/// The charge became these two allocated operations.
	Cleaning {
		job_id: crate::schema::CompactionJobId,
		claim: OpKey,
		complete: OpKey,
		output: AbandonedOutput,
	},
	/// The charge became this one allocated operation. Never revived.
	Burning {
		op: OpKey,
	},
	/// The ring definitely rejected this cleanup's claim and Cancelled its Complete, so both
	/// operations have ended and hold no window. Nothing authorizes disposing of the file, so
	/// it stays as evidence, under the identity recorded here, and this id is never reused.
	ClaimRefused {
		job_id: crate::schema::CompactionJobId,
		reason: crate::consensus::slab_ops::SlabRejection,
		output: AbandonedOutput,
	},
}

impl ReservedIdState {
	/// Whether this row still holds window capacity for its terminal transition.
	fn charged(&self) -> bool {
		matches!(self, Self::Unsubmitted | Self::Exposed { .. })
	}
}

/// Each charged reservation holds this many sequences: the worst case it can need,
/// a claim and its Complete (ADR 0697 §4 T3).
pub(crate) const RESERVATION_CHARGE: u64 = 2;

/// The largest output count a single job can be admitted with: its charges, and
/// its own install operation.
pub(crate) const MAX_RESERVED_OUTPUTS_PER_JOB: u64 = (OPS_WINDOW_W - 1) / RESERVATION_CHARGE;

/// `S + k + 2C <= W`. Checked throughout, including the sequences a charge promises.
fn admits(next: u64, known: u64, requested: u64, charged: u64) -> bool {
	(next - 1)
		.checked_sub(known)
		.and_then(|span| span.checked_add(requested))
		.zip(charged.checked_mul(RESERVATION_CHARGE))
		.and_then(|(span, promised)| span.checked_add(promised))
		.is_some_and(|total| total <= OPS_WINDOW_W)
		&& (next - 1)
			.checked_add(requested)
			.and_then(|last| last.checked_add(charged.checked_mul(RESERVATION_CHARGE)?))
			.is_some()
}

fn decode_reserved(bytes: &[u8]) -> std::io::Result<ReservedIdState> {
	postcard::from_bytes(bytes)
		.map_err(|error| op_state_invalid(format!("reserved id row does not decode: {error}")))
}

fn charged_reservations(table: &impl ReadableTable<u64, &'static [u8]>) -> std::io::Result<u64> {
	let mut charged = 0;
	for row in table.iter().map_err(redb_to_io)? {
		let (_, bytes) = row.map_err(redb_to_io)?;
		if decode_reserved(bytes.value())?.charged() {
			charged += 1;
		}
	}
	Ok(charged)
}

fn read_reserved(
	table: &impl ReadableTable<u64, &'static [u8]>,
	id: SlabId,
) -> std::io::Result<Option<ReservedIdState>> {
	table
		.get(&id.get())
		.map_err(redb_to_io)?
		.map(|bytes| decode_reserved(bytes.value()))
		.transpose()
}

fn write_reserved(
	table: &mut redb::Table<u64, &'static [u8]>,
	id: SlabId,
	state: &ReservedIdState,
) -> std::io::Result<()> {
	let bytes = postcard::to_stdvec(state).map_err(Error::other)?;
	table
		.insert(&id.get(), bytes.as_slice())
		.map_err(redb_to_io)?;
	Ok(())
}

/// A reservation transition that does not apply to the row found. Typed: a burned,
/// burning, absent or already exposed id is refused, never revived.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ReservationRefused {
	pub id: SlabId,
	pub found: Option<ReservedIdState>,
}

impl std::fmt::Display for ReservationRefused {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(
			formatter,
			"reserved id {} cannot make this transition from {:?}",
			self.id, self.found
		)
	}
}

impl std::error::Error for ReservationRefused {}

fn refused(id: SlabId, found: Option<ReservedIdState>) -> Error {
	Error::new(ErrorKind::WouldBlock, ReservationRefused { id, found })
}

/// Promote reservations in the caller's publication transaction: each id is now a
/// live slab, proven by the records and manifest row written with it, and its
/// charge is released. Only an exposed id can have been published.
pub(crate) fn promote_reservations(
	tx: &redb::WriteTransaction,
	ids: &[SlabId],
) -> std::io::Result<()> {
	let mut rows = tx.open_table(RESERVED_SLAB_IDS_TABLE).map_err(redb_to_io)?;
	for id in ids {
		let state = read_reserved(&rows, *id)?;
		if !matches!(state, Some(ReservedIdState::Exposed { abandoned: None })) {
			return Err(refused(*id, state));
		}
		rows.remove(&id.get()).map_err(redb_to_io)?;
	}
	Ok(())
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
	progress: Option<OpProgress>,
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
	progress: Option<OpProgress>,
	expected_inputs: Option<Vec<LsmInputAuthority>>,
}

impl ReservedOp {
	pub(crate) fn key(&self) -> OpKey {
		self.key
	}

	pub(crate) fn digest(&self) -> [u8; 32] {
		self.digest
	}

	pub(crate) fn command(&self) -> &SlabCommand {
		&self.command
	}
}

/// A live workflow's hold on its operations. Ephemeral and per-store, never durable
/// authority: it says only that a running task, or a worker that can still write,
/// owns these sequences. Recovery acquires the same hold before it acts on a row.
#[derive(Debug)]
pub(crate) struct OpOwnership {
	seqs: Vec<u64>,
	live: Arc<dashmap::DashSet<u64>>,
}

impl OpOwnership {
	/// All of `seqs`, or none. A failed attempt holds nothing, so it removes nothing.
	fn acquire(live: &Arc<dashmap::DashSet<u64>>, seqs: Vec<u64>) -> Option<Self> {
		for (held, seq) in seqs.iter().enumerate() {
			if !live.insert(*seq) {
				for release in &seqs[..held] {
					live.remove(release);
				}
				return None;
			}
		}
		Some(Self {
			seqs,
			live: live.clone(),
		})
	}
}

impl Drop for OpOwnership {
	fn drop(&mut self) {
		for seq in &self.seqs {
			self.live.remove(seq);
		}
	}
}

/// A workflow's whole span: the operation to submit now and its later ones.
#[derive(Debug)]
pub(crate) struct OpAllocation {
	pub first: OpTicket,
	pub later: Vec<ReservedOp>,
	/// The linked LSM intent as stored, naming these operations.
	pub linked: Option<LsmInstallIntent>,
	/// Held from before these rows were visible.
	pub ownership: OpOwnership,
}

/// An allocated operation as a reopen finds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum LoadedOp {
	Submittable(OpTicket),
	Reserved(ReservedOp),
}

impl LoadedOp {
	pub(crate) fn key(&self) -> OpKey {
		match self {
			Self::Submittable(ticket) => ticket.key(),
			Self::Reserved(reserved) => reserved.key(),
		}
	}

	pub(crate) fn digest(&self) -> [u8; 32] {
		match self {
			Self::Submittable(ticket) => ticket.digest(),
			Self::Reserved(reserved) => reserved.digest(),
		}
	}

	pub(crate) fn command(&self) -> &SlabCommand {
		match self {
			Self::Submittable(ticket) => ticket.command(),
			Self::Reserved(reserved) => reserved.command(),
		}
	}

	/// How far this operation's own workflow has gone locally.
	pub(crate) fn progress(&self) -> Option<&OpProgress> {
		match self {
			Self::Submittable(ticket) => ticket.progress.as_ref(),
			Self::Reserved(reserved) => reserved.progress.as_ref(),
		}
	}

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

/// A closed store's raw row, read or replaced without going through anything that decodes it.
#[cfg(test)]
pub(crate) fn op_row_bytes_at(path: &std::path::Path, op_seq: u64) -> Vec<u8> {
	let db = redb::Database::open(path).unwrap();
	let tx = db.begin_read().unwrap();
	let intents = tx.open_table(OP_INTENTS_TABLE).unwrap();
	let row = intents.get(&op_seq).unwrap().unwrap();
	row.value().to_vec()
}

#[cfg(test)]
pub(crate) fn set_op_row_bytes_at(path: &std::path::Path, op_seq: u64, bytes: &[u8]) {
	let db = redb::Database::open(path).unwrap();
	let tx = db.begin_write().unwrap();
	{
		let mut intents = tx.open_table(OP_INTENTS_TABLE).unwrap();
		intents.insert(&op_seq, bytes).unwrap();
	}
	tx.commit().unwrap();
}

/// Where a row's progress starts: past its digest, command, phase and option tag.
#[cfg(test)]
pub(crate) fn op_row_progress_offset(bytes: &[u8]) -> usize {
	let intent: OpIntent = postcard::from_bytes(bytes).unwrap();
	32 + postcard::to_stdvec(&intent.command).unwrap().len() + 2
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
			progress: intent.progress,
			expected_inputs: intent.expected_inputs,
		}),
		OpPhase::Reserved => LoadedOp::Reserved(ReservedOp {
			key,
			digest: intent.digest,
			command: intent.command,
			progress: intent.progress,
			expected_inputs: intent.expected_inputs,
		}),
	}
}

/// Allocate sequences a row's charge already promised: no admission check, since
/// the charge is in the sum. Sequence exhaustion still refuses.
fn allocate_charged(
	tx: &redb::WriteTransaction,
	commands: Vec<SlabCommand>,
	live: &Arc<dashmap::DashSet<u64>>,
) -> std::io::Result<(Vec<(u64, OpIntent)>, OpOwnership)> {
	let mut meta = tx.open_table(META_TABLE).map_err(redb_to_io)?;
	let (next, _) = read_counters(&meta)?;
	let after = next
		.checked_add(commands.len() as u64)
		.ok_or_else(|| op_state_invalid("operation sequences exhausted"))?;
	let ownership = OpOwnership::acquire(live, (next..after).collect())
		.ok_or_else(|| op_state_invalid("a fresh sequence already has an owner"))?;
	let mut intents = tx.open_table(OP_INTENTS_TABLE).map_err(redb_to_io)?;
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
			progress: None,
			expected_inputs: None,
		};
		let bytes = encode_intent(&intent)?;
		intents
			.insert(&op_seq, bytes.as_slice())
			.map_err(redb_to_io)?;
		allocated.push((op_seq, intent));
	}
	meta.insert(META_NEXT_OP_SEQ, &after).map_err(redb_to_io)?;
	Ok((allocated, ownership))
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
	tx.open_table(RESERVED_SLAB_IDS_TABLE).map_err(redb_to_io)?;
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

/// Whether a row's recorded phase is one its own workflow writes. Every write is checked by
/// `follows`, which says nothing of a row already on disk. A mark's three phases belong to
/// marks, nothing else records any but the two terminal workflows, and neither of those
/// reaches a purge before its Complete could have been submitted.
fn progress_fits_its_row(intent: &OpIntent) -> bool {
	let Some(progress) = &intent.progress else {
		return true;
	};
	let disposes = matches!(
		intent.command,
		SlabCommand::GcRemove(_) | SlabCommand::Complete(_)
	);
	match progress {
		OpProgress::MarkPrepared { .. }
		| OpProgress::MarkPublished { .. }
		| OpProgress::ClaimRejected { .. } => {
			matches!(&intent.command, SlabCommand::Compaction(commit)
				if !commit.slabs_to_mark_delete_pending.is_empty())
		}
		OpProgress::QuarantineRecorded(_) | OpProgress::RunDropped(_) => disposes,
		OpProgress::PurgeAuthorized(_) | OpProgress::Purged(_) => {
			disposes && intent.phase == OpPhase::MayBeSubmitted
		}
	}
}

/// Whether the operation a reservation links is the one it names: its id, its job, its kind.
fn links_this_id(state: &ReservedIdState, key: OpKey, id: u64, command: &SlabCommand) -> bool {
	match (state, command) {
		(ReservedIdState::Burning { .. }, SlabCommand::BurnReserved(burn)) => {
			burn.slab.local_id().get() == id
		}
		(ReservedIdState::Cleaning { job_id, claim, .. }, SlabCommand::Claim(op)) => {
			key == *claim && op.job_id == *job_id && op.slab.local_id().get() == id
		}
		(
			ReservedIdState::Cleaning {
				job_id, complete, ..
			},
			SlabCommand::Complete(op),
		) => key == *complete && op.job_id == *job_id && op.slab.local_id().get() == id,
		_ => false,
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
				let intent = decode_intent(seq, bytes.value())?;
				let consistent = progress_fits_its_row(&intent);
				if !consistent {
					return Err(op_state_invalid(format!(
						"op {seq} cannot have reached {:?}",
						intent.progress
					)));
				}
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
	let reserved = tx.open_table(RESERVED_SLAB_IDS_TABLE).map_err(redb_to_io)?;
	let charged = charged_reservations(&reserved)?;
	if !admits(next, known, 0, charged) {
		return Err(op_state_invalid(format!(
			"{charged} charged reservations do not fit a span of {span} in the window"
		)));
	}
	for row in reserved.iter().map_err(redb_to_io)? {
		let (id, bytes) = row.map_err(redb_to_io)?;
		let (id, state) = (id.value(), decode_reserved(bytes.value())?);
		// A row that spent its charge is bounded by the operation it links to.
		let linked: Vec<OpKey> = match &state {
			ReservedIdState::Burning { op } => vec![*op],
			ReservedIdState::Cleaning {
				claim, complete, ..
			} => vec![*claim, *complete],
			_ => Vec::new(),
		};
		// A cleanup's claim hands off once its run is dropped, and its Ack can prune it long
		// before Complete ends this row. Before that point its absence is a real hole.
		let claim_has_ended = match &state {
			ReservedIdState::Cleaning { complete, .. } => {
				match intents.get(&complete.op_seq).map_err(redb_to_io)? {
					Some(bytes) => matches!(
						decode_intent(complete.op_seq, bytes.value())?.progress,
						Some(
							OpProgress::RunDropped(_)
								| OpProgress::PurgeAuthorized(_)
								| OpProgress::Purged(_)
						)
					),
					None => false,
				}
			}
			_ => false,
		};
		for key in linked {
			if claim_has_ended
				&& matches!(&state, ReservedIdState::Cleaning { claim, .. } if *claim == key)
				&& intents.get(&key.op_seq).map_err(redb_to_io)?.is_none()
			{
				continue;
			}
			if key.op_seq <= known
				|| (intents.get(&key.op_seq).map_err(redb_to_io)?.is_none()
					&& pending.get(&key.op_seq).map_err(redb_to_io)?.is_none())
			{
				return Err(op_state_invalid(format!(
					"reserved id {id} links op {} with no row",
					key.op_seq
				)));
			}
			if let Some(bytes) = intents.get(&key.op_seq).map_err(redb_to_io)?
				&& !links_this_id(
					&state,
					key,
					id,
					&decode_intent(key.op_seq, bytes.value())?.command,
				) {
				return Err(op_state_invalid(format!(
					"reserved id {id} links op {}, which is another operation",
					key.op_seq
				)));
			}
		}
	}
	Ok(())
}

fn oldest_holder(
	known: u64,
	next: u64,
	intents: &impl ReadableTable<u64, &'static [u8]>,
	pending: &impl ReadableTable<u64, (bool, [u8; 32])>,
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
			SlabCommand::Claim(_) => "claim",
			SlabCommand::Complete(_) => "complete",
			SlabCommand::BurnReserved(_) => "burn",
		};
		return Ok(Some(OpHolder {
			op_seq,
			transition: Some(transition),
			state,
		}));
	}
	let state = match pending.get(&op_seq).map_err(redb_to_io)? {
		Some(acked) => OpHolderState::AwaitingAck {
			acked: acked.value().0,
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
	/// A rejected install's outputs leave the intent and become their reservations'
	/// own abandoned evidence, for every output, in this one transaction.
	AbandonOutputs(Vec<(SlabId, AbandonedOutput)>),
	/// A run-less T1 finished: this row goes with its Complete.
	CleanupFinished(SlabId),
	/// A burn was applied: this row goes with its operation.
	BurnFinished(SlabId),
	/// A removal's local work is finished: the row must say so.
	RemovalFinished,
	Removal(OpKey, Vec<LsmInputAuthority>),
	RejectedTier(RejectedTierCandidate),
}

impl PersistentLocalIndex {
	#[cfg(test)]
	pub(crate) fn cut_next_run_publication(&self, cut: OpTxnCut) {
		*self.run_publication_cut.lock().unwrap() = Some(cut);
	}

	#[cfg(test)]
	pub(crate) fn cut_next_op_txn(&self, cut: OpTxnCut) {
		*self.op_txn_cut.lock().unwrap() = Some(cut);
	}

	#[cfg(test)]
	pub(super) fn take_op_txn_cut(&self) -> Option<OpTxnCut> {
		let mut skip = self.op_txn_cut_skip.lock().unwrap();
		if let Some((left, cut)) = skip.as_mut() {
			if *left == 0 {
				let cut = *cut;
				*skip = None;
				return Some(cut);
			}
			*left -= 1;
			return None;
		}
		drop(skip);
		self.op_txn_cut.lock().unwrap().take()
	}

	/// The next sequence this store would allocate.
	#[cfg(test)]
	pub(crate) async fn next_op_seq_for_test(&self) -> std::io::Result<u64> {
		self.db
			.run(|db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				let meta = tx.open_table(META_TABLE).map_err(redb_to_io)?;
				Ok(meta
					.get(META_NEXT_OP_SEQ)
					.map_err(redb_to_io)?
					.map_or(1, |value| value.value()))
			})
			.await
	}

	/// Record one step of an operation's local progress. The transaction owns
	/// `authority` until it ends, whatever happens to its caller.
	pub(crate) async fn advance_op_progress(
		&self,
		update: OpProgressUpdate,
		authority: Option<Box<dyn std::any::Any + Send>>,
	) -> std::io::Result<()> {
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
				apply_op_progress(&tx, &update)?;
				commit_op_txn(
					tx,
					#[cfg(test)]
					cut,
				)
			})
			.await
	}

	/// Replace one reservation's stored state, as nothing else can.
	#[cfg(test)]
	pub(crate) async fn force_reserved_id_for_test(
		&self,
		id: SlabId,
		state: ReservedIdState,
	) -> std::io::Result<()> {
		self.db
			.run(move |db| {
				let bytes = postcard::to_stdvec(&state).map_err(Error::other)?;
				let tx = db.begin_write().map_err(redb_to_io)?;
				{
					let mut reserved =
						tx.open_table(RESERVED_SLAB_IDS_TABLE).map_err(redb_to_io)?;
					reserved
						.insert(&id.get(), bytes.as_slice())
						.map_err(redb_to_io)?;
				}
				tx.commit().map_err(redb_to_io)
			})
			.await
	}

	/// Rewrite one row's stored phases as nothing else can: for what a reopen does with a
	/// row no workflow writes.
	#[cfg(test)]
	pub(crate) async fn force_op_row_for_test(
		&self,
		op_seq: u64,
		edit: impl FnOnce(&mut OpIntent) + Send + 'static,
	) -> std::io::Result<()> {
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				{
					let mut intents = tx.open_table(OP_INTENTS_TABLE).map_err(redb_to_io)?;
					let mut intent = match intents.get(&op_seq).map_err(redb_to_io)? {
						Some(bytes) => decode_intent(op_seq, bytes.value())?,
						None => return Err(op_state_invalid(format!("no row {op_seq}"))),
					};
					edit(&mut intent);
					let bytes = encode_intent(&intent)?;
					intents
						.insert(&op_seq, bytes.as_slice())
						.map_err(redb_to_io)?;
				}
				tx.commit().map_err(redb_to_io)
			})
			.await
	}

	/// Cut the transaction `skip` transactions from now that takes a cut.
	#[cfg(test)]
	pub(crate) fn cut_nth_op_txn(&self, skip: u8, cut: OpTxnCut) {
		*self.op_txn_cut_skip.lock().unwrap() = Some((u32::from(skip), cut));
	}

	/// Cut the run-drop transaction: the progress step after the next one.
	#[cfg(test)]
	pub(crate) fn cut_next_progress_txn_after_one(&self, cut: OpTxnCut) {
		*self.op_txn_cut_skip.lock().unwrap() = Some((1, cut));
	}

	/// Cut the step after the run drop: the reserved removal's transition.
	#[cfg(test)]
	pub(crate) fn cut_next_progress_txn_after_two(&self, cut: OpTxnCut) {
		*self.op_txn_cut_skip.lock().unwrap() = Some((2, cut));
	}

	/// Write `ids`' `Unsubmitted` rows and their charge, before any caller holds an
	/// id. When they extend the durable bound, `high_water` rises in this same
	/// transaction. `Err` exposes nothing; an ambiguous commit is reconciled from
	/// the durable row, whose id no caller ever received.
	pub(crate) async fn reserve_slab_ids(
		&self,
		ids: Vec<SlabId>,
		high_water: Option<u64>,
	) -> std::io::Result<()> {
		#[cfg(test)]
		let cut = self.take_op_txn_cut();
		let _write = self.write_lock.lock().await;
		#[cfg(test)]
		let hold = self.op_handoff_hold.lock().unwrap().take();
		self.db
			.run(move |db| {
				#[cfg(test)]
				if let Some((entered, release)) = hold {
					let _ = entered.send(());
					let _ = release.recv();
				}
				let tx = db.begin_write().map_err(redb_to_io)?;
				{
					let mut meta = tx.open_table(META_TABLE).map_err(redb_to_io)?;
					let (next, known) = read_counters(&meta)?;
					let mut rows = tx.open_table(RESERVED_SLAB_IDS_TABLE).map_err(redb_to_io)?;
					let charged = charged_reservations(&rows)? + ids.len() as u64;
					if !admits(next, known, 0, charged) {
						let intents = tx.open_table(OP_INTENTS_TABLE).map_err(redb_to_io)?;
						let pending = tx.open_table(OP_ACK_PENDING_TABLE).map_err(redb_to_io)?;
						let holder = oldest_holder(known, next, &intents, &pending)?;
						return Err(Error::new(
							ErrorKind::WouldBlock,
							TerminalBacklog {
								next_op_seq: next,
								known_acked_through: known,
								requested: ids.len() as u64 * RESERVATION_CHARGE,
								holder,
							},
						));
					}
					for id in &ids {
						if let Some(found) = read_reserved(&rows, *id)? {
							return Err(refused(*id, Some(found)));
						}
						write_reserved(&mut rows, *id, &ReservedIdState::Unsubmitted)?;
					}
					if let Some(bound) = high_water {
						let current = meta
							.get(META_SLAB_ID_HIGH_WATER)
							.map_err(redb_to_io)?
							.map(|value| value.value())
							.ok_or_else(store_identity_missing)?;
						if bound < current {
							return Err(Error::new(
								ErrorKind::InvalidInput,
								format!("slab id high-water only rises: {bound} < {current}"),
							));
						}
						meta.insert(META_SLAB_ID_HIGH_WATER, &bound)
							.map_err(redb_to_io)?;
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

	/// Move one row by its durable state, never a cached read.
	async fn transition_reserved_id(
		&self,
		id: SlabId,
		authority: Option<Box<dyn std::any::Any + Send>>,
		apply: impl FnOnce(
			Option<ReservedIdState>,
		) -> Result<ReservedIdState, Box<Option<ReservedIdState>>>
		+ Send
		+ 'static,
	) -> std::io::Result<()> {
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
					let mut rows = tx.open_table(RESERVED_SLAB_IDS_TABLE).map_err(redb_to_io)?;
					let next =
						apply(read_reserved(&rows, id)?).map_err(|found| refused(id, *found))?;
					write_reserved(&mut rows, id, &next)?;
				}
				commit_op_txn(
					tx,
					#[cfg(test)]
					cut,
				)
			})
			.await
	}

	/// The only writer of `Exposed`: only from a present `Unsubmitted` row.
	pub(crate) async fn expose_reserved_id(&self, id: SlabId) -> std::io::Result<()> {
		self.transition_reserved_id(id, None, |found| match found {
			Some(ReservedIdState::Unsubmitted) => Ok(ReservedIdState::Exposed { abandoned: None }),
			other => Err(Box::new(other)),
		})
		.await
	}

	/// Record that an exposed id's job is gone, with what it left. The transaction
	/// owns `authority` until it ends, whatever happens to its caller.
	pub(crate) async fn abandon_exposed_id(
		&self,
		id: SlabId,
		output: AbandonedOutput,
		authority: Option<Box<dyn std::any::Any + Send>>,
	) -> std::io::Result<()> {
		self.transition_reserved_id(id, authority, move |found| match found {
			Some(ReservedIdState::Exposed { abandoned: None }) => Ok(ReservedIdState::Exposed {
				abandoned: Some(output),
			}),
			other => Err(Box::new(other)),
		})
		.await
	}

	/// A reservation's durable state.
	pub(crate) async fn reserved_id_state(
		&self,
		id: SlabId,
	) -> std::io::Result<Option<ReservedIdState>> {
		self.db
			.run(move |db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				read_reserved(
					&tx.open_table(RESERVED_SLAB_IDS_TABLE).map_err(redb_to_io)?,
					id,
				)
			})
			.await
	}

	/// Every reservation row.
	pub(crate) async fn load_reserved_ids(
		&self,
	) -> std::io::Result<Vec<(SlabId, ReservedIdState)>> {
		self.db
			.run(|db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				let rows = tx.open_table(RESERVED_SLAB_IDS_TABLE).map_err(redb_to_io)?;
				let mut loaded = Vec::new();
				for row in rows.iter().map_err(redb_to_io)? {
					let (id, bytes) = row.map_err(redb_to_io)?;
					loaded.push((SlabId::new(id.value()), decode_reserved(bytes.value())?));
				}
				Ok(loaded)
			})
			.await
	}

	/// `Unsubmitted` to `Burning` and the burn's own operation, in one transaction,
	/// so `Burning` cannot exist without its operation. Spends this row's charge, so
	/// capacity cannot refuse it.
	pub(crate) async fn begin_burn(
		&self,
		id: SlabId,
		owner: crate::consensus::OwnerRef,
	) -> std::io::Result<(OpTicket, OpOwnership)> {
		let command = SlabCommand::BurnReserved(crate::consensus::slab_ops::BurnOp {
			slab: owner.slab(id),
		});
		#[cfg(test)]
		let cut = self.take_op_txn_cut();
		let _write = self.write_lock.lock().await;
		let live = self.live_ops.clone();
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				let ticket;
				let ownership;
				{
					let mut rows = tx.open_table(RESERVED_SLAB_IDS_TABLE).map_err(redb_to_io)?;
					let found = read_reserved(&rows, id)?;
					if found != Some(ReservedIdState::Unsubmitted) {
						return Err(refused(id, found));
					}
					let (mut allocated, held) = allocate_charged(&tx, vec![command], &live)?;
					ownership = held;
					let (seq, intent) = allocated.remove(0);
					let key = OpKey {
						owner: intent.command.owner(),
						op_seq: seq,
					};
					write_reserved(&mut rows, id, &ReservedIdState::Burning { op: key })?;
					let LoadedOp::Submittable(loaded) = loaded_op(seq, intent) else {
						return Err(op_state_invalid("a burn is submittable at once"));
					};
					ticket = loaded;
				}
				commit_op_txn(
					tx,
					#[cfg(test)]
					cut,
				)?;
				Ok((ticket, ownership))
			})
			.await
	}

	/// An abandoned id's charge becomes its claim and Complete, explicitly, in one
	/// transaction. A retry finds `Cleaning` and is refused, so it cannot spend the
	/// charge twice.
	pub(crate) async fn begin_cleanup(
		&self,
		id: SlabId,
		owner: crate::consensus::OwnerRef,
		job_id: crate::schema::CompactionJobId,
	) -> std::io::Result<(OpTicket, ReservedOp, OpOwnership)> {
		use crate::consensus::slab_ops::{ClaimOp, CompleteOp};
		let slab = owner.slab(id);
		let commands = vec![
			SlabCommand::Claim(ClaimOp { slab, job_id }),
			SlabCommand::Complete(CompleteOp { slab, job_id }),
		];
		#[cfg(test)]
		let cut = self.take_op_txn_cut();
		let _write = self.write_lock.lock().await;
		let live = self.live_ops.clone();
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				let pair;
				let ownership;
				{
					let mut rows = tx.open_table(RESERVED_SLAB_IDS_TABLE).map_err(redb_to_io)?;
					let found = read_reserved(&rows, id)?;
					let Some(ReservedIdState::Exposed {
						abandoned: Some(output),
					}) = found
					else {
						return Err(refused(id, found));
					};
					let (mut allocated, held) = allocate_charged(&tx, commands, &live)?;
					ownership = held;
					let (complete_seq, complete) = allocated.remove(1);
					let (claim_seq, claim) = allocated.remove(0);
					let keys = (
						OpKey {
							owner: claim.command.owner(),
							op_seq: claim_seq,
						},
						OpKey {
							owner: complete.command.owner(),
							op_seq: complete_seq,
						},
					);
					write_reserved(
						&mut rows,
						id,
						&ReservedIdState::Cleaning {
							job_id,
							claim: keys.0,
							complete: keys.1,
							output,
						},
					)?;
					let (LoadedOp::Submittable(claim), LoadedOp::Reserved(complete)) = (
						loaded_op(claim_seq, claim),
						loaded_op(complete_seq, complete),
					) else {
						return Err(op_state_invalid("a cleanup's phases are fixed"));
					};
					pair = (claim, complete);
				}
				commit_op_txn(
					tx,
					#[cfg(test)]
					cut,
				)?;
				Ok((pair.0, pair.1, ownership))
			})
			.await
	}

	/// A rejected install hands off with every output's identity moving to its
	/// reservation, so nothing is pruned before its cleanup evidence exists.
	pub(crate) async fn hand_off_rejected_install(
		&self,
		ticket: &OpTicket,
		intent: LsmInstallIntent,
		outputs: Vec<(SlabId, AbandonedOutput)>,
	) -> std::io::Result<()> {
		self.hand_off_op_with(
			ticket.key(),
			ticket.digest(),
			Some(intent),
			HandoffEvidence::AbandonOutputs(outputs),
			None,
		)
		.await
	}

	/// An applied burn's operation and row end together.
	pub(crate) async fn hand_off_burn(
		&self,
		ticket: &OpTicket,
		id: SlabId,
		authority: Option<Box<dyn std::any::Any + Send>>,
	) -> std::io::Result<()> {
		self.hand_off_op_with(
			ticket.key(),
			ticket.digest(),
			None,
			HandoffEvidence::BurnFinished(id),
			authority,
		)
		.await
	}

	/// A finished cleanup's Complete and row end together.
	/// A cleanup whose claim was rejected and whose Complete was Cancelled: both rows and the
	/// reservation's new state, in one transaction, so no `Cleaning` row outlives what it links.
	pub(crate) async fn hand_off_refused_cleanup(
		&self,
		claim: &OpTicket,
		complete: &OpTicket,
		id: SlabId,
		reason: crate::consensus::slab_ops::SlabRejection,
		authority: Option<Box<dyn std::any::Any + Send>>,
	) -> std::io::Result<()> {
		let (claim_key, claim_digest) = (claim.key(), claim.digest());
		let (complete_key, complete_digest) = (complete.key(), complete.digest());
		#[cfg(test)]
		let cut = self.take_op_txn_cut();
		let _write = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let _authority = authority;
				let tx = db.begin_write().map_err(redb_to_io)?;
				{
					let mut reserved =
						tx.open_table(RESERVED_SLAB_IDS_TABLE).map_err(redb_to_io)?;
					let Some(ReservedIdState::Cleaning {
						job_id,
						claim: found_claim,
						complete: found_complete,
						output,
					}) = read_reserved(&reserved, id)?
					else {
						return Err(op_state_invalid(format!(
							"reserved id {id} has no cleanup to refuse"
						)));
					};
					if found_claim != claim_key || found_complete != complete_key {
						return Err(op_state_invalid(format!(
							"reserved id {id} does not link these operations"
						)));
					}
					let mut intents = tx.open_table(OP_INTENTS_TABLE).map_err(redb_to_io)?;
					let mut pending = tx.open_table(OP_ACK_PENDING_TABLE).map_err(redb_to_io)?;
					for (key, digest) in
						[(claim_key, claim_digest), (complete_key, complete_digest)]
					{
						let op_seq = key.op_seq;
						let intent = match intents.get(&op_seq).map_err(redb_to_io)? {
							Some(bytes) => decode_intent(op_seq, bytes.value())?,
							None => {
								return Err(op_state_invalid(format!(
									"op {op_seq} has no intent to hand off"
								)));
							}
						};
						if intent.digest != digest || intent.progress.is_some() {
							return Err(op_state_invalid(format!(
								"op {op_seq} is not an undisposed cleanup operation"
							)));
						}
						intents.remove(&op_seq).map_err(redb_to_io)?;
						pending
							.insert(&op_seq, &(false, digest))
							.map_err(redb_to_io)?;
					}
					write_reserved(
						&mut reserved,
						id,
						&ReservedIdState::ClaimRefused {
							job_id,
							reason,
							output,
						},
					)?;
				}
				commit_op_txn(
					tx,
					#[cfg(test)]
					cut,
				)
			})
			.await
	}

	pub(crate) async fn hand_off_finished_cleanup(
		&self,
		ticket: &OpTicket,
		id: SlabId,
	) -> std::io::Result<()> {
		self.hand_off_op_with(
			ticket.key(),
			ticket.digest(),
			None,
			HandoffEvidence::CleanupFinished(id),
			None,
		)
		.await
	}

	/// One operation's durable local progress, as it stands now.
	pub(crate) async fn op_progress(&self, op_seq: u64) -> std::io::Result<Option<OpProgress>> {
		self.db
			.run(move |db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				let intents = tx.open_table(OP_INTENTS_TABLE).map_err(redb_to_io)?;
				let row = intents.get(&op_seq).map_err(redb_to_io)?;
				row.map(|bytes| decode_intent(op_seq, bytes.value()))
					.transpose()
					.map(|intent| intent.and_then(|intent| intent.progress))
			})
			.await
	}

	/// What is durably known about one operation.
	pub(crate) async fn op_evidence(
		&self,
		op_seq: u64,
		digest: [u8; 32],
	) -> std::io::Result<OpEvidence> {
		self.db
			.run(move |db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				let intents = tx.open_table(OP_INTENTS_TABLE).map_err(redb_to_io)?;
				if let Some(bytes) = intents.get(&op_seq).map_err(redb_to_io)? {
					let intent = decode_intent(op_seq, bytes.value())?;
					return Ok(if intent.digest == digest {
						OpEvidence::Intent(intent.progress)
					} else {
						OpEvidence::Unknown
					});
				}
				let pending = tx.open_table(OP_ACK_PENDING_TABLE).map_err(redb_to_io)?;
				if let Some(row) = pending.get(&op_seq).map_err(redb_to_io)? {
					return Ok(if row.value().1 == digest {
						OpEvidence::Finished
					} else {
						OpEvidence::Unknown
					});
				}
				let meta = tx.open_table(META_TABLE).map_err(redb_to_io)?;
				let acked = meta
					.get(META_KNOWN_ACKED_THROUGH)
					.map_err(redb_to_io)?
					.map_or(0, |value| value.value());
				Ok(if op_seq <= acked {
					OpEvidence::BelowPrefix
				} else {
					OpEvidence::Unknown
				})
			})
			.await
	}

	/// Every mark decided rejected, with what is needed to finish it.
	pub(crate) async fn load_rejected_claims(&self) -> std::io::Result<Vec<RejectedClaim>> {
		self.db
			.run(|db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				let intents = tx.open_table(OP_INTENTS_TABLE).map_err(redb_to_io)?;
				let mut claims = Vec::new();
				for row in intents.iter().map_err(redb_to_io)? {
					let (seq, bytes) = row.map_err(redb_to_io)?;
					let op_seq = seq.value();
					let intent = decode_intent(op_seq, bytes.value())?;
					let Some(OpProgress::ClaimRejected {
						pre_run, removal, ..
					}) = intent.progress.clone()
					else {
						continue;
					};
					let LoadedOp::Submittable(mark) = loaded_op(op_seq, intent) else {
						return Err(op_state_invalid(format!(
							"rejected mark {op_seq} was never submittable"
						)));
					};
					let removal_row = match intents.get(&removal.op_seq).map_err(redb_to_io)? {
						Some(bytes) => decode_intent(removal.op_seq, bytes.value())?,
						None => {
							return Err(op_state_invalid(format!(
								"rejected mark {op_seq} has lost its removal {}",
								removal.op_seq
							)));
						}
					};
					claims.push(RejectedClaim {
						mark,
						removal: loaded_op(removal.op_seq, removal_row),
						pre_run,
					});
				}
				Ok(claims)
			})
			.await
	}

	/// Finish a rejected mark: both operations hand off and the linked intent
	/// clears together, only after the removal's Cancel is definite.
	pub(crate) async fn hand_off_rejected_claim(
		&self,
		mark: &OpTicket,
		removal: &OpTicket,
		linked: LsmInstallIntent,
		authority: Option<Box<dyn std::any::Any + Send>>,
	) -> std::io::Result<()> {
		if !linked.ops.contains(&mark.key()) || !linked.ops.contains(&removal.key()) {
			return Err(op_state_invalid(format!(
				"LSM intent for job {} does not link ops {:?} and {:?}",
				linked.commit.job_id.get(),
				mark.key(),
				removal.key()
			)));
		}
		let (mark_key, mark_digest) = (mark.key(), mark.digest());
		let (removal_key, removal_digest) = (removal.key(), removal.digest());
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
					for (key, digest) in [(mark_key, mark_digest), (removal_key, removal_digest)] {
						let op_seq = key.op_seq;
						let intent = match intents.get(&op_seq).map_err(redb_to_io)? {
							Some(bytes) => decode_intent(op_seq, bytes.value())?,
							None => {
								return Err(op_state_invalid(format!(
									"op {op_seq} has no intent to hand off"
								)));
							}
						};
						if intent.digest != digest {
							return Err(op_state_invalid(format!(
								"op {op_seq} is not the operation handed off"
							)));
						}
						if op_seq == mark_key.op_seq
							&& !matches!(
								&intent.progress,
								Some(OpProgress::ClaimRejected { removal, .. })
									if *removal == removal_key
							) {
							return Err(op_state_invalid(format!(
								"mark {op_seq} is not rejected with this removal"
							)));
						}
						intents.remove(&op_seq).map_err(redb_to_io)?;
						pending
							.insert(&op_seq, &(false, digest))
							.map_err(redb_to_io)?;
					}
				}
				complete_linked_lsm_intent(&tx, &linked)?;
				commit_op_txn(
					tx,
					#[cfg(test)]
					cut,
				)
			})
			.await
	}

	/// Pauses the next allocation after its commit, before its caller gets a result.
	#[cfg(test)]
	pub(crate) fn hold_after_next_allocation_commit(
		&self,
	) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
		let (entered_tx, entered_rx) = std::sync::mpsc::channel();
		let (release_tx, release_rx) = std::sync::mpsc::channel();
		*self.op_after_commit_hold.lock().unwrap() = Some((entered_tx, release_rx));
		(entered_rx, release_tx)
	}

	/// Pauses the next reservation's commit.
	#[cfg(test)]
	pub(crate) fn hold_next_reservation_commit(
		&self,
	) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
		self.hold_next_handoff_txn()
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
		self.allocate_ops_with(commands, linked, None, None).await
	}

	/// ADR 0703: a certified install's workflow. Its intent and certificate are
	/// written with the operation only while the active generation is the one
	/// its selection was decided under; otherwise `Ok(None)` and nothing is
	/// written.
	pub(crate) async fn allocate_certified_ops(
		&self,
		commands: Vec<SlabCommand>,
		linked: LsmInstallIntent,
		certificate: super::LsmCertificate,
	) -> std::io::Result<Option<OpAllocation>> {
		match self
			.allocate_ops_with(commands, Some(linked), None, Some(certificate))
			.await
		{
			Ok(allocation) => Ok(Some(allocation)),
			Err(error)
				if error
					.get_ref()
					.is_some_and(|inner| inner.is::<GenerationMoved>()) =>
			{
				Ok(None)
			}
			Err(error) => Err(error),
		}
	}

	/// A GC mark's workflow, recording what a rejection of it restores.
	pub(crate) async fn allocate_mark(
		&self,
		commands: Vec<SlabCommand>,
		linked: LsmInstallIntent,
		pre_run: LsmRunSummary,
		pre_delete_pending: bool,
	) -> std::io::Result<OpAllocation> {
		self.allocate_ops_with(
			commands,
			Some(linked),
			Some(OpProgress::MarkPrepared {
				pre_run,
				pre_delete_pending,
			}),
			None,
		)
		.await
	}

	/// Whether any workflow holds these sequences; if not, hold them all.
	pub(crate) fn try_acquire_ops(&self, seqs: Vec<u64>) -> Option<OpOwnership> {
		OpOwnership::acquire(&self.live_ops, seqs)
	}

	async fn allocate_ops_with(
		&self,
		commands: Vec<SlabCommand>,
		linked: Option<LsmInstallIntent>,
		first_progress: Option<OpProgress>,
		certificate: Option<super::LsmCertificate>,
	) -> std::io::Result<OpAllocation> {
		if certificate.is_some() && linked.is_none() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"an LSM certificate needs its install intent",
			));
		}
		if commands.is_empty() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"an operation workflow needs at least one command",
			));
		}
		#[cfg(test)]
		let cut = self.take_op_txn_cut();
		let _write = self.write_lock.lock().await;
		let live = self.live_ops.clone();
		#[cfg(test)]
		let after_commit = self.op_after_commit_hold.lock().unwrap().take();
		let mut first_progress = first_progress;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				let keys;
				let ownership;
				let mut stored = None;
				{
					let mut meta = tx.open_table(META_TABLE).map_err(redb_to_io)?;
					let (next, known) = read_counters(&meta)?;
					let mut intents = tx.open_table(OP_INTENTS_TABLE).map_err(redb_to_io)?;
					let pending = tx.open_table(OP_ACK_PENDING_TABLE).map_err(redb_to_io)?;
					let requested = commands.len() as u64;
					let charged = charged_reservations(
						&tx.open_table(RESERVED_SLAB_IDS_TABLE).map_err(redb_to_io)?,
					)?;
					let fits = admits(next, known, requested, charged);
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
					// Held before these rows can be visible, and carried by this worker
					// until it can no longer write, whatever became of its caller.
					ownership = OpOwnership::acquire(&live, (next..after).collect())
						.ok_or_else(|| op_state_invalid("a fresh sequence already has an owner"))?;
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
							progress: if offset == 0 {
								first_progress.take()
							} else {
								None
							},
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
					// ADR 0703: bound to its generation in this same transaction.
					if let Some(certificate) = &certificate {
						if !super::lsm_certificate::insert_certificate(&tx, &intent, certificate)? {
							return Err(Error::new(ErrorKind::WouldBlock, GenerationMoved));
						}
					}
					stored = Some(intent);
				}
				commit_op_txn(
					tx,
					#[cfg(test)]
					cut,
				)?;
				#[cfg(test)]
				if let Some((entered, release)) = after_commit {
					let _ = entered.send(());
					let _ = release.recv();
				}
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
					ownership,
				})
			})
			.await
	}

	/// The durable transition before a reserved operation's first submission.
	/// Commit uncertainty yields no ticket, so nothing can be submitted on it. A
	/// removal is T1's Complete: it can be submitted only after the run it
	/// removes is durably gone.
	pub(crate) async fn mark_may_be_submitted(
		&self,
		reserved: ReservedOp,
	) -> std::io::Result<OpTicket> {
		self.transition_reserved(reserved, true).await
	}

	/// The same transition for the sole purpose of Cancelling a reserved removal
	/// whose claim was rejected: no local work ever preceded it.
	pub(crate) async fn reserved_for_cancel(
		&self,
		reserved: ReservedOp,
	) -> std::io::Result<OpTicket> {
		self.transition_reserved(reserved, false).await
	}

	async fn transition_reserved(
		&self,
		reserved: ReservedOp,
		submitting: bool,
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
					if submitting
						&& matches!(
							intent.command,
							SlabCommand::GcRemove(_) | SlabCommand::Complete(_)
						) && !matches!(intent.progress, Some(OpProgress::RunDropped(_)))
					{
						return Err(op_state_invalid(format!(
							"removal {op_seq} cannot be submitted from {:?}",
							intent.progress
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
					progress: reserved.progress,
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
					if matches!(
						evidence,
						HandoffEvidence::RemovalFinished | HandoffEvidence::CleanupFinished(_)
					) && !matches!(
						intent.progress,
						Some(OpProgress::Purged(_)) | Some(OpProgress::RunDropped(None))
					) {
						return Err(op_state_invalid(format!(
							"removal {op_seq} cannot hand off from {:?}",
							intent.progress
						)));
					}
					intents.remove(&op_seq).map_err(redb_to_io)?;
					pending
						.insert(&op_seq, &(false, digest))
						.map_err(redb_to_io)?;
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
						drop(table);
						super::lsm_certificate::remove_certificate_of(&tx, intent)?;
					}
					match evidence {
						HandoffEvidence::None | HandoffEvidence::RemovalFinished => {}
						HandoffEvidence::AbandonOutputs(outputs) => {
							let mut rows =
								tx.open_table(RESERVED_SLAB_IDS_TABLE).map_err(redb_to_io)?;
							for (id, output) in outputs {
								let state = read_reserved(&rows, id)?;
								if !matches!(
									state,
									Some(ReservedIdState::Exposed { abandoned: None })
								) {
									return Err(op_state_invalid(format!(
										"output {id} cannot be abandoned from {state:?}"
									)));
								}
								write_reserved(
									&mut rows,
									id,
									&ReservedIdState::Exposed {
										abandoned: Some(output),
									},
								)?;
							}
						}
						HandoffEvidence::CleanupFinished(id)
						| HandoffEvidence::BurnFinished(id) => {
							tx.open_table(RESERVED_SLAB_IDS_TABLE)
								.map_err(redb_to_io)?
								.remove(&id.get())
								.map_err(redb_to_io)?
								.ok_or_else(|| {
									op_state_invalid(format!("reserved id {id} has no row"))
								})?;
						}
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

	/// Hand off a removal whose Complete applied and whose purge finished. Its
	/// linked intent went with the run; this row is what remains of it.
	pub(crate) async fn hand_off_finished_removal(&self, ticket: &OpTicket) -> std::io::Result<()> {
		self.hand_off_op_with(
			ticket.key(),
			ticket.digest(),
			None,
			HandoffEvidence::RemovalFinished,
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
				let charged = charged_reservations(
					&tx.open_table(RESERVED_SLAB_IDS_TABLE).map_err(redb_to_io)?,
				)?;
				if admits(next, known, requested, charged) {
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
					rows.push((op_seq.value(), acked.value().0));
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
						let digest = pending
							.get(&seq)
							.map_err(redb_to_io)?
							.map(|row| row.value().1);
						if let Some(digest) = digest {
							pending.insert(&seq, &(true, digest)).map_err(redb_to_io)?;
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

/// A closed store's operation-state version, as a store of another build would have it.
#[cfg(test)]
pub(crate) fn set_op_state_version_for_test(path: &Path, version: Option<u64>) -> u64 {
	let db = Database::open(path).unwrap();
	let write_txn = db.begin_write().unwrap();
	let found = {
		let mut meta = write_txn.open_table(META_TABLE).unwrap();
		meta.insert(META_OP_STATE_VERSION, &version.unwrap_or(OP_STATE_VERSION))
			.unwrap()
			.map(|value| value.value())
			.unwrap()
	};
	write_txn.commit().unwrap();
	found
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
