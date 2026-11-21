//! Wendelon ADR 0120: ParentDecisions CREATE TABLE AS SELECT chains, the
//! decision-state core (slice 1). A chain publishes exactly its declared chunks
//! (each one's canonical rows, bound by its request digest) or ends terminal and
//! unpublished. Every transition is decided here, at apply, from replicated
//! state; nothing trusts the coordinator's word.
//!
//! Begin, Publish and Conclude change the catalog too: they run with the whole
//! applied state ([`apply_begin`], [`apply_publish`], [`apply_conclude`]),
//! check everything, perform the one fallible catalog step first, and only then
//! change the decision state, so a refusal changes nothing.

use super::{
	Claim, DecisionRefusal, DecisionResult, DecisionState, Operation, ParentKey, ParentNamespace,
	ParentStatus, RefusalCause,
};
use crate::consensus::raft_storage::SmState;
use crate::schema::{TableObjectId, TableSchemaDescriptor, TenantId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// At most this many chunks in one declaration (Toron's chain bound).
pub const CTAS_MAX_CHUNKS: usize = 128;
/// One declared chunk on the wire: `sequence_no` (u64 BE), `rows` (u32 BE) and
/// `request_digest` (32 bytes).
pub const CTAS_CHUNK_ENTRY_BYTES: usize = 8 + 4 + 32;
/// The largest declaration field a command may carry: the count and
/// [`CTAS_MAX_CHUNKS`] entries (5,634 bytes).
pub const CTAS_DECLARATION_MAX_BYTES: usize = 2 + CTAS_MAX_CHUNKS * CTAS_CHUNK_ENTRY_BYTES;
/// The owner's lease, from Begin or a Renew's stamp.
pub const CTAS_OWNER_LEASE_MS: u64 = 60_000;
/// A Begin's operation id is admitted only within this window of the stamp.
pub const CTAS_OPERATION_WINDOW_MS: u64 = 15 * 60 * 1_000;
/// How long a terminal chain is kept (its replayable answer) before it may be
/// pruned, on the committed decision clock.
pub const CTAS_TERMINAL_RETENTION_MS: u64 = CTAS_OPERATION_WINDOW_MS;
/// Non-terminal chains at once.
pub const CTAS_MAX_ACTIVE_CHAINS: usize = 64;
/// Chains held at once, terminal summaries included (reserved at Begin).
pub const CTAS_CHAIN_CAPACITY: usize = 256;
/// Privately retained descriptors: a slot is charged at Begin, released at
/// Publish, and kept through abandonment.
pub const CTAS_RETAINED_DESCRIPTORS_MAX: u32 = 1_024;
/// The bytes those retained descriptors may hold, by their v1 encoding.
pub const CTAS_RETAINED_DESCRIPTOR_BYTES_MAX: u64 = 16 << 20;

/// One declared chunk: the parent key's sequence, its row count and the
/// request digest its claim must carry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkDecl {
	pub sequence_no: u64,
	pub rows: u32,
	pub request_digest: [u8; 32],
}

/// Why a declaration does not encode, decode or validate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeclarationError {
	/// Shorter than its count field.
	Short,
	/// More than [`CTAS_MAX_CHUNKS`] advertised (or given to encode).
	TooManyChunks,
	/// The length is not exactly the count's.
	LengthMismatch,
	/// A chunk of zero rows, or more than one write's.
	RowsOutOfRange,
	/// Sequence numbers not consecutive from the first.
	NotConsecutive,
}

/// A declaration's bytes as a command carries them, bounded on the wire: the
/// deserializer refuses more than [`CTAS_DECLARATION_MAX_BYTES`] before it
/// copies anything.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CtasDeclarationBytes(Vec<u8>);

impl CtasDeclarationBytes {
	pub fn as_slice(&self) -> &[u8] {
		&self.0
	}

	/// Test-only: arbitrary bytes, to exercise the bounded deserializer.
	#[cfg(test)]
	pub(crate) fn unchecked_for_test(bytes: Vec<u8>) -> Self {
		Self(bytes)
	}
}

impl Serialize for CtasDeclarationBytes {
	fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		serializer.serialize_bytes(&self.0)
	}
}

impl<'de> Deserialize<'de> for CtasDeclarationBytes {
	fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		struct Bounded;
		impl<'de> serde::de::Visitor<'de> for Bounded {
			type Value = CtasDeclarationBytes;

			fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
				write!(f, "at most {CTAS_DECLARATION_MAX_BYTES} declaration bytes")
			}

			fn visit_bytes<E: serde::de::Error>(self, bytes: &[u8]) -> Result<Self::Value, E> {
				if bytes.len() > CTAS_DECLARATION_MAX_BYTES {
					return Err(E::invalid_length(bytes.len(), &self));
				}
				Ok(CtasDeclarationBytes(bytes.to_vec()))
			}

			fn visit_seq<A: serde::de::SeqAccess<'de>>(
				self,
				mut seq: A,
			) -> Result<Self::Value, A::Error> {
				let hint = seq.size_hint().unwrap_or(0);
				if hint > CTAS_DECLARATION_MAX_BYTES {
					return Err(serde::de::Error::invalid_length(hint, &self));
				}
				let mut bytes = Vec::with_capacity(hint);
				while let Some(byte) = seq.next_element::<u8>()? {
					if bytes.len() == CTAS_DECLARATION_MAX_BYTES {
						return Err(serde::de::Error::invalid_length(bytes.len() + 1, &self));
					}
					bytes.push(byte);
				}
				Ok(CtasDeclarationBytes(bytes))
			}
		}
		deserializer.deserialize_bytes(Bounded)
	}
}

/// The declaration's codec: a `u16` count (BE), then `count` entries of
/// [`CTAS_CHUNK_ENTRY_BYTES`]. More than [`CTAS_MAX_CHUNKS`] is refused, never
/// truncated.
pub fn encode_declaration(chunks: &[ChunkDecl]) -> Result<CtasDeclarationBytes, DeclarationError> {
	if chunks.len() > CTAS_MAX_CHUNKS {
		return Err(DeclarationError::TooManyChunks);
	}
	let mut bytes = Vec::with_capacity(2 + chunks.len() * CTAS_CHUNK_ENTRY_BYTES);
	bytes.extend_from_slice(&(chunks.len() as u16).to_be_bytes());
	for chunk in chunks {
		bytes.extend_from_slice(&chunk.sequence_no.to_be_bytes());
		bytes.extend_from_slice(&chunk.rows.to_be_bytes());
		bytes.extend_from_slice(&chunk.request_digest);
	}
	Ok(CtasDeclarationBytes(bytes))
}

/// Decodes a declaration. The advertised count is bounded, and the length
/// checked against it with checked arithmetic, before any vector is allocated.
pub fn decode_declaration(bytes: &[u8]) -> Result<Vec<ChunkDecl>, DeclarationError> {
	let count: [u8; 2] = bytes
		.get(..2)
		.and_then(|count| count.try_into().ok())
		.ok_or(DeclarationError::Short)?;
	let count = usize::from(u16::from_be_bytes(count));
	if count > CTAS_MAX_CHUNKS {
		return Err(DeclarationError::TooManyChunks);
	}
	let expected = count
		.checked_mul(CTAS_CHUNK_ENTRY_BYTES)
		.and_then(|entries| entries.checked_add(2))
		.ok_or(DeclarationError::LengthMismatch)?;
	if bytes.len() != expected {
		return Err(DeclarationError::LengthMismatch);
	}
	let mut chunks = Vec::with_capacity(count);
	for entry in bytes[2..].chunks_exact(CTAS_CHUNK_ENTRY_BYTES) {
		chunks.push(ChunkDecl {
			sequence_no: u64::from_be_bytes(entry[..8].try_into().expect("8 bytes")),
			rows: u32::from_be_bytes(entry[8..12].try_into().expect("4 bytes")),
			request_digest: entry[12..].try_into().expect("32 bytes"),
		});
	}
	Ok(chunks)
}

/// The declared row count: each chunk 1..=one write's rows, sequence numbers
/// consecutive from the first.
fn validate_declaration(chunks: &[ChunkDecl]) -> Result<u64, DeclarationError> {
	let mut total = 0u64;
	for (at, chunk) in chunks.iter().enumerate() {
		if chunk.rows == 0 || chunk.rows as usize > crate::schema::table::TABLE_WRITE_MAX_ROWS {
			return Err(DeclarationError::RowsOutOfRange);
		}
		if chunks[0].sequence_no.checked_add(at as u64) != Some(chunk.sequence_no) {
			return Err(DeclarationError::NotConsecutive);
		}
		total += u64::from(chunk.rows);
	}
	Ok(total)
}

/// Why an abandoned chain ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CtasAbandonCause {
	/// The owner found a chunk inadmissible before its Reserve.
	ChunkInadmissible,
	/// The owner's deadline passed.
	Deadline,
	/// A declared chunk was refused.
	ChunkRefused,
	/// Recovery found a declared chunk never reserved: its bytes lived only in
	/// the lost owner.
	SourceBytesLost,
}

/// A chain's phase. `Recovering` is entered once and never left except to a
/// terminal phase; `Published` is the terminal success.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CtasPhase {
	Dispatching,
	Recovering,
	Published { at: u64 },
	Abandoned { cause: CtasAbandonCause, at: u64 },
	Conflict { at: u64 },
}

impl CtasPhase {
	pub fn is_terminal(self) -> bool {
		!matches!(self, Self::Dispatching | Self::Recovering)
	}
}

/// Wendelon ADR 0120 D6: the owner commitment a secret names, as a Begin
/// carries it and owner commands are checked against it.
pub fn ctas_owner_commitment(secret: &[u8; 32]) -> [u8; 32] {
	*blake3::hash(secret).as_bytes()
}

/// One operation's chain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CtasChain {
	pub object: TableObjectId,
	pub fingerprint: [u8; 32],
	pub chunks: Vec<ChunkDecl>,
	pub row_count: u64,
	/// The winning owner's commitment, `BLAKE3(secret)`. The secret is checked
	/// at authenticated admission and never enters a command; every owner
	/// command names this commitment, which apply checks.
	pub dispatch_hash: [u8; 32],
	pub epoch: u64,
	pub lease_until_ms: u64,
	/// The committed decision clock when the chain became terminal (0 before):
	/// prune's durable time anchor, never a later command's own stamp alone.
	pub terminal_at_ms: u64,
	pub phase: CtasPhase,
	/// Bit `i`: chunk `i`'s parent was decided Accepted, or Refused.
	pub accepted: u128,
	pub refused: u128,
	/// The retained-descriptor bytes this chain is charged.
	pub retained_bytes: u64,
}

impl CtasChain {
	fn declared_bits(&self) -> u128 {
		match self.chunks.len() {
			0 => 0,
			n if n >= 128 => u128::MAX,
			n => (1u128 << n) - 1,
		}
	}

	/// The declared chunk a key's sequence and digest name.
	fn index_of(&self, sequence_no: u64, digest: [u8; 32]) -> Option<usize> {
		let first = self.chunks.first()?.sequence_no;
		let at = usize::try_from(sequence_no.checked_sub(first)?).ok()?;
		let chunk = self.chunks.get(at)?;
		(chunk.sequence_no == sequence_no && chunk.request_digest == digest).then_some(at)
	}

	fn key_of(&self, tenant: TenantId, op: [u8; 16], at: usize) -> ParentKey {
		ParentKey {
			tenant,
			namespace: ParentNamespace::Table(self.object),
			client_uuid: op,
			sequence_no: self.chunks[at].sequence_no,
		}
	}
}

/// An object id retired by a terminal unpublished chain: never reused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CtasRetired {
	pub op: [u8; 16],
	pub at: u64,
}

/// The CTAS part of the decision state.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CtasState {
	chains: BTreeMap<(TenantId, [u8; 16]), CtasChain>,
	/// The provisional objects chains route: present from Begin until a
	/// successful Publish (kept after Abandon or Conflict).
	by_object: BTreeMap<(TenantId, TableObjectId), [u8; 16]>,
	/// Permanent: object ids a chain retired unpublished.
	retired: BTreeMap<(TenantId, TableObjectId), CtasRetired>,
	/// Permanent while the object is bound: its Publish's log index.
	visible_from: BTreeMap<(TenantId, TableObjectId), u64>,
	retained_count: u32,
	retained_bytes: u64,
}

impl CtasState {
	pub fn chain(&self, tenant: TenantId, op: [u8; 16]) -> Option<&CtasChain> {
		self.chains.get(&(tenant, op))
	}

	/// The chain routing `object` as a provisional CTAS object, if any.
	pub fn chain_of_object(
		&self,
		tenant: TenantId,
		object: TableObjectId,
	) -> Option<([u8; 16], &CtasChain)> {
		let op = *self.by_object.get(&(tenant, object))?;
		Some((op, self.chains.get(&(tenant, op))?))
	}

	/// Whether `object` is routed by a chain or retired by one: neither a Begin
	/// nor an ordinary Create may use it.
	pub fn claims_object(&self, tenant: TenantId, object: TableObjectId) -> bool {
		self.by_object.contains_key(&(tenant, object))
			|| self.retired.contains_key(&(tenant, object))
	}

	pub fn retired(&self, tenant: TenantId, object: TableObjectId) -> Option<CtasRetired> {
		self.retired.get(&(tenant, object)).copied()
	}

	/// The log index from which a published chain's table is visible.
	pub fn visible_from(&self, tenant: TenantId, object: TableObjectId) -> Option<u64> {
		self.visible_from.get(&(tenant, object)).copied()
	}

	/// The retained-descriptor budget in use: (count, bytes).
	pub fn retained(&self) -> (u32, u64) {
		(self.retained_count, self.retained_bytes)
	}

	/// ADR 0120 D9: whether a certified generation with cut `t` excludes this
	/// object's rows: it was retired (abandoned or conflicted) at or before `t`.
	pub fn excludes(&self, tenant: TenantId, object: TableObjectId, t: u64) -> bool {
		self.retired
			.get(&(tenant, object))
			.is_some_and(|retired| retired.at <= t)
	}

	/// ADR 0120 D9: whether a chain still dispatches or recovers (the backup
	/// roster refuses until it settles).
	pub fn unsettled(&self) -> bool {
		self.chains
			.values()
			.any(|chain| matches!(chain.phase, CtasPhase::Dispatching | CtasPhase::Recovering))
	}

	/// Whether any object was retired at or before `t` (the excluding policy).
	pub fn retired_at_or_before(&self, t: u64) -> bool {
		self.retired.values().any(|retired| retired.at <= t)
	}

	/// Whether an object was retired in `(after, through]` (the idle trigger).
	pub fn retired_in(&self, after: u64, through: u64) -> bool {
		after < through
			&& self
				.retired
				.values()
				.any(|retired| retired.at > after && retired.at <= through)
	}

	/// Records a chunk parent's terminal decision in its chain, from the
	/// transition that made it (`conclude`). Only a body whose key and digest
	/// match the declaration counts; each chunk is recorded once.
	pub(super) fn note_terminal(&mut self, key: &ParentKey, digest: [u8; 32], accepted: bool) {
		let ParentNamespace::Table(object) = key.namespace else {
			return;
		};
		let Some(op) = self.by_object.get(&(key.tenant, object)).copied() else {
			return;
		};
		if key.client_uuid != op {
			return;
		}
		let Some(chain) = self.chains.get_mut(&(key.tenant, op)) else {
			return;
		};
		let Some(at) = chain.index_of(key.sequence_no, digest) else {
			return;
		};
		let bit = 1u128 << at;
		if (chain.accepted | chain.refused) & bit != 0 {
			return;
		}
		if accepted {
			chain.accepted |= bit;
		} else {
			chain.refused |= bit;
		}
	}
}

/// Why a CTAS command changed nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CtasRefusal {
	Declaration(DeclarationError),
	/// Not an AppendOnly, generation-1 descriptor of ADR 0119's declared types.
	Descriptor,
	/// The object id is not exclusively this Begin's (the KV id included).
	ObjectNotExclusive,
	/// The operation id is outside its window at the stamp.
	OperationExpired,
	/// Another statement holds this operation id.
	FingerprintMismatch,
	/// No chain under this operation.
	Unknown,
	/// The command does not name the winning owner's commitment.
	NotOwner,
	StaleEpoch,
	WrongPhase,
	/// Recovery before the owner's lease lapsed.
	LeaseLive,
	/// Publish before every chunk was accepted.
	ChunksIncomplete,
	/// A declared chunk is still Reserved: recovery waits.
	StillReserved,
	/// Chain or retained-descriptor capacity is exhausted.
	Capacity,
	/// A fresh operation's target name is already bound (IF NOT EXISTS then
	/// validates the winner).
	NameBound,
	/// A Reserve outside the declaration (operation, sequence, digest or rows).
	/// Definitive: the declaration never changes, so the same request is never
	/// admitted.
	ChunkUndeclared,
	/// Prune before the chain's retention lapsed on the committed clock, or before
	/// every key its operation names is at or below the retirement floor.
	PruneEarly,
}

impl From<CtasRefusal> for DecisionRefusal {
	fn from(refusal: CtasRefusal) -> Self {
		DecisionRefusal::Ctas(refusal)
	}
}

/// The first statement to Begin an operation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CtasBegin {
	pub tenant: TenantId,
	pub op: [u8; 16],
	pub fingerprint: [u8; 32],
	pub descriptor: TableSchemaDescriptor,
	pub declaration: CtasDeclarationBytes,
	pub dispatch_hash: [u8; 32],
	pub stamp_ms: u64,
}

fn chain_mut(
	decisions: &mut DecisionState,
	tenant: TenantId,
	op: [u8; 16],
) -> Result<&mut CtasChain, DecisionRefusal> {
	decisions
		.ctas
		.chains
		.get_mut(&(tenant, op))
		.ok_or(CtasRefusal::Unknown.into())
}

/// The command names the winning owner's commitment. Admission proved the
/// secret behind it; apply binds the command to that exact owner.
fn named_owner(chain: &CtasChain, owner: &[u8; 32]) -> bool {
	chain.dispatch_hash == *owner
}

/// The answer a terminal chain gives every repeated command: success stays
/// success, so a lost Publish acknowledgement replays.
fn terminal_answer(tenant: TenantId, chain: &CtasChain) -> Option<DecisionResult> {
	match chain.phase {
		CtasPhase::Published { at } => Some(DecisionResult::CtasPublished {
			tenant,
			object: chain.object,
			visible_from: at,
			replayed: true,
		}),
		CtasPhase::Abandoned { cause, at } => Some(DecisionResult::CtasAbandoned {
			cause,
			at,
			replayed: true,
		}),
		CtasPhase::Conflict { at } => Some(DecisionResult::CtasConflict { at, replayed: true }),
		CtasPhase::Dispatching | CtasPhase::Recovering => None,
	}
}

/// ADR 0120 D3: Begin. The first Begin chooses the declaration, object and
/// owner; a same-fingerprint retry answers that chain unchanged. The catalog's
/// provisional descriptor is inserted first; the chain only after it.
pub(super) fn apply_begin(
	state: &mut SmState,
	decisions: &mut DecisionState,
	begin: &CtasBegin,
) -> Result<DecisionResult, DecisionRefusal> {
	let (tenant, op) = (begin.tenant, begin.op);
	if let Some(chain) = decisions.ctas.chains.get(&(tenant, op)) {
		return if chain.fingerprint == begin.fingerprint {
			Ok(DecisionResult::CtasBegun {
				tenant,
				object: chain.object,
				replayed: true,
			})
		} else {
			Err(CtasRefusal::FingerprintMismatch.into())
		};
	}
	if decisions.hold.is_some() {
		return Err(DecisionRefusal::BackupHold);
	}
	if !state
		.tenants
		.get(&tenant)
		.is_some_and(|entry| entry.state.allows_writes())
	{
		return Err(DecisionRefusal::TenantNotWritable);
	}
	// Judged on the committed decision clock, as a Reserve's issuance is: the
	// applied clock or this stamp, whichever is later, never the stamp alone.
	let clock = decisions.clock_ms.max(begin.stamp_ms);
	let issued = crate::schema::uuid_v7_unix_millis(op).ok_or(CtasRefusal::OperationExpired)?;
	if issued > clock.saturating_add(super::DECISION_CLOCK_SKEW_MS)
		|| issued.saturating_add(CTAS_OPERATION_WINDOW_MS) < clock
	{
		return Err(CtasRefusal::OperationExpired.into());
	}
	let descriptor = &begin.descriptor;
	let object = descriptor.table_object_id;
	// The reserved KV id, independent of what the catalog holds.
	if object == TableObjectId::DEFAULT_KV {
		return Err(CtasRefusal::ObjectNotExclusive.into());
	}
	if descriptor.schema_generation != 1
		|| !matches!(
			descriptor.row_identity_policy,
			crate::schema::RowIdentityPolicy::AppendOnly
		) || !crate::schema::table::parent_decisions_append_only_schema(descriptor)
		|| descriptor.validate().is_err()
	{
		return Err(CtasRefusal::Descriptor.into());
	}
	let chunks =
		decode_declaration(begin.declaration.as_slice()).map_err(CtasRefusal::Declaration)?;
	let row_count = validate_declaration(&chunks).map_err(CtasRefusal::Declaration)?;
	// Exclusive: no descriptor of the id at any generation (provisional or not),
	// no drop outcome (ADR 0118 D3), not routed or retired by a chain.
	if state
		.table_schemas
		.latest_by_id_including_provisional(tenant, object)
		.is_some()
		|| state.table_drop_outcomes.get(tenant, object).is_some()
		|| decisions.ctas.claims_object(tenant, object)
	{
		return Err(CtasRefusal::ObjectNotExclusive.into());
	}
	// A fresh operation's name must be unbound now; Publish checks it again.
	if state
		.table_schemas
		.latest_by_name(tenant, &descriptor.namespace, &descriptor.table_name)
		.is_some()
	{
		return Err(CtasRefusal::NameBound.into());
	}
	let active = decisions
		.ctas
		.chains
		.values()
		.filter(|chain| !chain.phase.is_terminal())
		.count();
	let bytes = crate::schema::table::encode_table_schema_descriptor_v1(descriptor)
		.map_err(|_| CtasRefusal::Descriptor)?
		.len() as u64;
	if active >= CTAS_MAX_ACTIVE_CHAINS
		|| decisions.ctas.chains.len() >= CTAS_CHAIN_CAPACITY
		|| decisions.ctas.retained_count >= CTAS_RETAINED_DESCRIPTORS_MAX
		|| decisions.ctas.retained_bytes.saturating_add(bytes) > CTAS_RETAINED_DESCRIPTOR_BYTES_MAX
	{
		return Err(CtasRefusal::Capacity.into());
	}
	// The one fallible step first: nothing has changed if it refuses.
	state
		.table_schemas
		.insert_provisional(tenant, descriptor.clone())
		.map_err(|_| CtasRefusal::Descriptor)?;
	decisions.ctas.chains.insert(
		(tenant, op),
		CtasChain {
			object,
			fingerprint: begin.fingerprint,
			chunks,
			row_count,
			dispatch_hash: begin.dispatch_hash,
			epoch: 1,
			lease_until_ms: clock.saturating_add(CTAS_OWNER_LEASE_MS),
			terminal_at_ms: 0,
			phase: CtasPhase::Dispatching,
			accepted: 0,
			refused: 0,
			retained_bytes: bytes,
		},
	);
	decisions.clock_ms = clock;
	decisions.ctas.by_object.insert((tenant, object), op);
	decisions.ctas.retained_count += 1;
	decisions.ctas.retained_bytes += bytes;
	Ok(DecisionResult::CtasBegun {
		tenant,
		object,
		replayed: false,
	})
}

/// ADR 0120 D4: a first Reserve that is a CTAS chunk: its object is routed by a
/// chain, or its key's client id names a CTAS operation. A key named by an
/// operation is that operation's chunk in every phase and never an ordinary
/// write, so a chunk delayed past Publish (which ends the object's routing)
/// cannot land as one. Only the declared chunk, with its digest and row count,
/// of the chain that routes this object, while it dispatches.
pub(super) fn is_chunk(decisions: &DecisionState, claim: &Claim, object: TableObjectId) -> bool {
	decisions
		.ctas
		.chain_of_object(claim.key.tenant, object)
		.is_some()
		|| decisions
			.ctas
			.chain(claim.key.tenant, claim.key.client_uuid)
			.is_some()
}

/// See [`is_chunk`].
pub(super) fn admit_first_reserve(
	decisions: &DecisionState,
	claim: &Claim,
	object: TableObjectId,
) -> Result<(), DecisionRefusal> {
	if object == TableObjectId::DEFAULT_KV {
		return Err(DecisionRefusal::TableUnavailable);
	}
	let Some(chain) = decisions
		.ctas
		.chain(claim.key.tenant, claim.key.client_uuid)
		.filter(|chain| chain.object == object)
	else {
		// Another operation's key on a routed object, or this operation's key
		// on another object.
		return Err(CtasRefusal::ChunkUndeclared.into());
	};
	if chain.phase != CtasPhase::Dispatching {
		return Err(CtasRefusal::WrongPhase.into());
	}
	if claim.operation != Operation::Write {
		return Err(CtasRefusal::ChunkUndeclared.into());
	}
	let at = chain
		.index_of(claim.key.sequence_no, claim.request_digest)
		.ok_or(CtasRefusal::ChunkUndeclared)?;
	let rows = claim
		.children
		.iter()
		.try_fold(0u64, |sum, child| sum.checked_add(u64::from(child.rows)));
	if rows != Some(u64::from(chain.chunks[at].rows)) {
		return Err(CtasRefusal::ChunkUndeclared.into());
	}
	Ok(())
}

pub(super) fn renew(
	decisions: &mut DecisionState,
	tenant: TenantId,
	op: [u8; 16],
	epoch: u64,
	owner: &[u8; 32],
	stamp_ms: u64,
) -> Result<DecisionResult, DecisionRefusal> {
	let clock = committed_clock(decisions, stamp_ms);
	let chain = chain_mut(decisions, tenant, op)?;
	if !named_owner(chain, owner) {
		return Err(CtasRefusal::NotOwner.into());
	}
	if chain.epoch != epoch {
		return Err(CtasRefusal::StaleEpoch.into());
	}
	if chain.phase != CtasPhase::Dispatching {
		return Err(CtasRefusal::WrongPhase.into());
	}
	chain.lease_until_ms = chain
		.lease_until_ms
		.max(clock.saturating_add(CTAS_OWNER_LEASE_MS));
	let lease_until_ms = chain.lease_until_ms;
	decisions.clock_ms = clock;
	Ok(DecisionResult::CtasRenewed { lease_until_ms })
}

/// ADR 0120 D7: takeover, once the lease lapsed. One way: no first Reserve is
/// admitted afterwards; recorded parents still finish.
pub(super) fn recover(
	decisions: &mut DecisionState,
	tenant: TenantId,
	op: [u8; 16],
	new_epoch: u64,
	stamp_ms: u64,
) -> Result<DecisionResult, DecisionRefusal> {
	let clock = committed_clock(decisions, stamp_ms);
	let chain = chain_mut(decisions, tenant, op)?;
	if let Some(answer) = terminal_answer(tenant, chain) {
		return Ok(answer);
	}
	if chain.phase == CtasPhase::Recovering && chain.epoch == new_epoch {
		return Ok(DecisionResult::CtasRecovering { epoch: new_epoch });
	}
	if chain.phase != CtasPhase::Dispatching {
		return Err(CtasRefusal::WrongPhase.into());
	}
	if clock < chain.lease_until_ms {
		return Err(CtasRefusal::LeaseLive.into());
	}
	if chain.epoch.checked_add(1) != Some(new_epoch) {
		return Err(CtasRefusal::StaleEpoch.into());
	}
	chain.phase = CtasPhase::Recovering;
	chain.epoch = new_epoch;
	decisions.clock_ms = clock;
	Ok(DecisionResult::CtasRecovering { epoch: new_epoch })
}

/// The owner's Publish: every declared chunk accepted.
pub(super) fn apply_publish(
	state: &mut SmState,
	decisions: &mut DecisionState,
	tenant: TenantId,
	op: [u8; 16],
	epoch: u64,
	owner: &[u8; 32],
	stamp_ms: u64,
	log_index: u64,
) -> Result<DecisionResult, DecisionRefusal> {
	let chain = chain_mut(decisions, tenant, op)?;
	if !named_owner(chain, owner) {
		return Err(CtasRefusal::NotOwner.into());
	}
	if let CtasPhase::Published { .. } = chain.phase {
		return Ok(terminal_answer(tenant, chain).expect("published is terminal"));
	}
	if chain.epoch != epoch {
		return Err(CtasRefusal::StaleEpoch.into());
	}
	if chain.phase != CtasPhase::Dispatching {
		return Err(CtasRefusal::WrongPhase.into());
	}
	if chain.refused != 0 || chain.accepted != chain.declared_bits() {
		return Err(CtasRefusal::ChunksIncomplete.into());
	}
	publish_or_conflict(state, decisions, tenant, op, stamp_ms, log_index)
}

/// ADR 0120 D7: recovery's rule, from the recorded evidence alone.
pub(super) fn apply_conclude(
	state: &mut SmState,
	decisions: &mut DecisionState,
	tenant: TenantId,
	op: [u8; 16],
	epoch: u64,
	stamp_ms: u64,
	log_index: u64,
) -> Result<DecisionResult, DecisionRefusal> {
	let chain = chain_mut(decisions, tenant, op)?;
	if let Some(answer) = terminal_answer(tenant, chain) {
		return Ok(answer);
	}
	if chain.phase != CtasPhase::Recovering {
		return Err(CtasRefusal::WrongPhase.into());
	}
	if chain.epoch != epoch {
		return Err(CtasRefusal::StaleEpoch.into());
	}
	match conclude_outcome(decisions, tenant, op) {
		ConcludeOutcome::Abandon(cause) => Ok(abandon_now(
			decisions, tenant, op, cause, stamp_ms, log_index,
		)),
		ConcludeOutcome::Publish => {
			publish_or_conflict(state, decisions, tenant, op, stamp_ms, log_index)
		}
		ConcludeOutcome::Wait => Err(CtasRefusal::StillReserved.into()),
	}
}

/// What a recovering chain's Conclude decides now. One predicate, shared by
/// apply and by maintenance's proposals (ADR 0120 D7, D9).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ConcludeOutcome {
	/// Refused evidence (even with other chunks still Reserved: abandonment
	/// concludes them), or a declared chunk never reserved (its bytes are lost).
	Abandon(CtasAbandonCause),
	/// Every declared chunk accepted (refused under a backup hold, at apply).
	Publish,
	/// A chunk with no evidence is still Reserved: it finishes first.
	Wait,
}

pub(super) fn conclude_outcome(
	decisions: &DecisionState,
	tenant: TenantId,
	op: [u8; 16],
) -> ConcludeOutcome {
	let chain = &decisions.ctas.chains[&(tenant, op)];
	if chain.refused != 0 {
		return ConcludeOutcome::Abandon(CtasAbandonCause::ChunkRefused);
	}
	if chain.accepted == chain.declared_bits() {
		return ConcludeOutcome::Publish;
	}
	let reserved = (0..chain.chunks.len())
		.filter(|at| (chain.accepted | chain.refused) & (1u128 << at) == 0)
		.map(|at| chain.key_of(tenant, op, at))
		.any(|key| {
			decisions.keys.get(&key).is_some_and(|entry| {
				decisions
					.bodies
					.get(&entry.reservation)
					.is_some_and(|body| body.status == ParentStatus::Reserved)
			})
		});
	if reserved {
		ConcludeOutcome::Wait
	} else {
		ConcludeOutcome::Abandon(CtasAbandonCause::SourceBytesLost)
	}
}

/// The owner's Abandon.
pub(super) fn abandon(
	decisions: &mut DecisionState,
	tenant: TenantId,
	op: [u8; 16],
	epoch: u64,
	owner: &[u8; 32],
	cause: CtasAbandonCause,
	stamp_ms: u64,
	log_index: u64,
) -> Result<DecisionResult, DecisionRefusal> {
	let chain = chain_mut(decisions, tenant, op)?;
	if !named_owner(chain, owner) {
		return Err(CtasRefusal::NotOwner.into());
	}
	match chain.phase {
		CtasPhase::Abandoned { .. } | CtasPhase::Conflict { .. } => {
			return Ok(terminal_answer(tenant, chain).expect("terminal"));
		}
		CtasPhase::Published { .. } | CtasPhase::Recovering => {
			return Err(CtasRefusal::WrongPhase.into());
		}
		CtasPhase::Dispatching => {}
	}
	if chain.epoch != epoch {
		return Err(CtasRefusal::StaleEpoch.into());
	}
	Ok(abandon_now(
		decisions, tenant, op, cause, stamp_ms, log_index,
	))
}

/// Publishes when the name is still unbound, otherwise ends in Conflict. Under a
/// backup hold (ADR 0736) neither happens: the binding is a logical catalog
/// change. The catalog is bound first; the decision state follows in the same
/// apply, and the object leaves provisional routing, so ordinary writes are
/// admitted at once.
fn publish_or_conflict(
	state: &mut SmState,
	decisions: &mut DecisionState,
	tenant: TenantId,
	op: [u8; 16],
	stamp_ms: u64,
	log_index: u64,
) -> Result<DecisionResult, DecisionRefusal> {
	if decisions.hold.is_some() {
		return Err(DecisionRefusal::BackupHold);
	}
	let object = decisions.ctas.chains[&(tenant, op)].object;
	let descriptor = state
		.table_schemas
		.get_including_provisional(tenant, object, 1)
		.filter(|_| state.table_schemas.is_provisional(tenant, object))
		.ok_or(CtasRefusal::Descriptor)?;
	if state
		.table_schemas
		.latest_by_name(tenant, &descriptor.namespace, &descriptor.table_name)
		.is_some()
	{
		let clock = committed_clock(decisions, stamp_ms);
		decisions.clock_ms = clock;
		close(decisions, tenant, op, log_index);
		let chain = decisions
			.ctas
			.chains
			.get_mut(&(tenant, op))
			.expect("present");
		chain.phase = CtasPhase::Conflict { at: log_index };
		chain.terminal_at_ms = clock;
		decisions
			.ctas
			.retired
			.insert((tenant, object), CtasRetired { op, at: log_index });
		return Ok(DecisionResult::CtasConflict {
			at: log_index,
			replayed: false,
		});
	}
	// The one fallible step first: nothing has changed if it refuses.
	state
		.table_schemas
		.publish_provisional(tenant, object, 1)
		.map_err(|_| CtasRefusal::Descriptor)?;
	state.table_lifecycle.ensure_inherit_cluster(
		tenant,
		object,
		crate::schema::HybridTimestamp {
			physical_ms: 0,
			logical: 0,
			node_id: 0,
		},
	);
	let clock = committed_clock(decisions, stamp_ms);
	decisions.clock_ms = clock;
	let chain = decisions
		.ctas
		.chains
		.get_mut(&(tenant, op))
		.expect("present");
	chain.phase = CtasPhase::Published { at: log_index };
	chain.terminal_at_ms = clock;
	let released = chain.retained_bytes;
	chain.retained_bytes = 0;
	decisions.ctas.retained_count -= 1;
	decisions.ctas.retained_bytes -= released;
	decisions.ctas.by_object.remove(&(tenant, object));
	decisions
		.ctas
		.visible_from
		.insert((tenant, object), log_index);
	Ok(DecisionResult::CtasPublished {
		tenant,
		object,
		visible_from: log_index,
		replayed: false,
	})
}

fn abandon_now(
	decisions: &mut DecisionState,
	tenant: TenantId,
	op: [u8; 16],
	cause: CtasAbandonCause,
	stamp_ms: u64,
	log_index: u64,
) -> DecisionResult {
	let clock = committed_clock(decisions, stamp_ms);
	decisions.clock_ms = clock;
	close(decisions, tenant, op, log_index);
	let chain = decisions
		.ctas
		.chains
		.get_mut(&(tenant, op))
		.expect("present");
	chain.phase = CtasPhase::Abandoned {
		cause,
		at: log_index,
	};
	chain.terminal_at_ms = clock;
	let object = chain.object;
	decisions
		.ctas
		.retired
		.insert((tenant, object), CtasRetired { op, at: log_index });
	DecisionResult::CtasAbandoned {
		cause,
		at: log_index,
		replayed: false,
	}
}

/// The committed decision clock a stamped CTAS command is judged on and raises:
/// the applied clock or its stamp, whichever is later.
fn committed_clock(decisions: &DecisionState, stamp_ms: u64) -> u64 {
	decisions.clock_ms.max(stamp_ms)
}

/// ADR 0120 D9: prunes a terminal chain's record. Only once its retention
/// lapsed on the committed clock (from `terminal_at_ms`) and its operation was
/// issued at or below the retirement floor, so every key it names refuses a
/// first Reserve without the chain (D4). The object's `retired`, `by_object`
/// routing, `visible_from` and retained descriptor stay.
pub(super) fn prune(
	decisions: &mut DecisionState,
	tenant: TenantId,
	op: [u8; 16],
) -> Result<DecisionResult, DecisionRefusal> {
	let chain = decisions
		.ctas
		.chains
		.get(&(tenant, op))
		.ok_or(CtasRefusal::Unknown)?;
	if matches!(chain.phase, CtasPhase::Dispatching | CtasPhase::Recovering) {
		return Err(CtasRefusal::WrongPhase.into());
	}
	let issued = crate::schema::uuid_v7_unix_millis(op).ok_or(CtasRefusal::Unknown)?;
	if issued > decisions.floor_ms
		|| decisions.clock_ms
			< chain
				.terminal_at_ms
				.saturating_add(CTAS_TERMINAL_RETENTION_MS)
	{
		return Err(CtasRefusal::PruneEarly.into());
	}
	decisions.ctas.chains.remove(&(tenant, op));
	Ok(DecisionResult::CtasPruned { tenant, op })
}

/// ADR 0120 D9: the CTAS commands leader maintenance proposes now, at the gated
/// clock reading `now`, with no client traffic: Recover for a dispatching chain
/// whose owner's lease lapsed on the committed clock, Conclude for a recovering
/// chain whose Conclude can decide now (`conclude_outcome`; no publication under a
/// backup hold), and Prune for a
/// terminal chain past its retention whose operation is at or below the floor.
/// At most `limit`. The reducer decides each; one it refuses is derived again on
/// a later tick only while its condition still holds.
pub(crate) fn maintenance_due(
	decisions: &DecisionState,
	now: u64,
	limit: usize,
) -> Vec<super::DecisionCommand> {
	let clock = committed_clock(decisions, now);
	let mut due = Vec::new();
	for (&(tenant, op), chain) in &decisions.ctas.chains {
		if due.len() >= limit {
			break;
		}
		match chain.phase {
			CtasPhase::Dispatching if clock >= chain.lease_until_ms => {
				due.push(super::DecisionCommand::CtasRecover {
					tenant,
					op,
					new_epoch: chain.epoch.saturating_add(1),
					stamp_ms: now,
				});
			}
			// The reducer's own predicate: nothing that must wait, and no
			// publication the backup hold refuses, is proposed (so no recurring
			// refusal fills a batch while a later chain could progress).
			CtasPhase::Recovering => match conclude_outcome(decisions, tenant, op) {
				ConcludeOutcome::Wait => {}
				ConcludeOutcome::Publish if decisions.hold.is_some() => {}
				_ => due.push(super::DecisionCommand::CtasConclude {
					tenant,
					op,
					epoch: chain.epoch,
					stamp_ms: now,
				}),
			},
			CtasPhase::Published { .. }
			| CtasPhase::Abandoned { .. }
			| CtasPhase::Conflict { .. }
				if crate::schema::uuid_v7_unix_millis(op)
					.is_some_and(|issued| issued <= decisions.floor_ms)
					&& decisions.clock_ms
						>= chain
							.terminal_at_ms
							.saturating_add(CTAS_TERMINAL_RETENTION_MS) =>
			{
				due.push(super::DecisionCommand::CtasPrune { tenant, op });
			}
			_ => {}
		}
	}
	due
}

/// Concludes every declared chunk still Reserved as `Refused { CtasAbandoned }`
/// in this apply (at most [`CTAS_MAX_CHUNKS`]).
fn close(decisions: &mut DecisionState, tenant: TenantId, op: [u8; 16], log_index: u64) {
	let chain = &decisions.ctas.chains[&(tenant, op)];
	let reserved: Vec<u64> = (0..chain.chunks.len())
		.filter_map(|at| decisions.keys.get(&chain.key_of(tenant, op, at)))
		.map(|entry| entry.reservation)
		.filter(|rsv| {
			decisions
				.bodies
				.get(rsv)
				.is_some_and(|body| body.status == ParentStatus::Reserved)
		})
		.collect();
	for rsv in reserved {
		super::conclude(decisions, rsv, Some(RefusalCause::CtasAbandoned), log_index)
			.expect("a Reserved chunk precedes this apply");
	}
}

#[cfg(test)]
#[path = "ctas_tests.rs"]
mod tests;
