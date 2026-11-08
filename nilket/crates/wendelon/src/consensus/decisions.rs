//! Parent write decisions under the `ParentDecisions` authority (ADR 0699 S2).
//!
//! A pure reducer over replicated state. A parent is reserved against open
//! range lineages, then reaches exactly one terminal status: Accepted on a
//! quorum of child acknowledgements, or Refused when a bound lineage closed or
//! the coordinator aborted. The first terminal status wins.
//!
//! Three structures with separate accounting:
//! - `lineages`: one entry per currently granted range of the fixed range
//!   universe, with a generation that is never reused.
//! - `bodies`: every reserved claim and its status, kept until certified
//!   reclamation (S5) exists. Nothing in S2 removes a body.
//! - `keys`: the retry index. `AdvanceFloor` retires terminal entries issued at
//!   or before the floor; a retired key answers `OutcomeUnavailable` for good.
//!
//! Nothing outside this module's tests constructs a decision command, and in a
//! non-test build every signed command is refused as `Unverifiable`: the
//! signature scheme arrives with admission (S3).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::consensus::StoreIncarnation;
use crate::consensus::raft_storage::SmState;
use crate::network::ring::{RingState, membership_from_epoch_config};
use crate::schema::{
	COUNTER_ATTEMPT_HORIZON_MS, COUNTER_INCARNATION_LIFETIME_MS, HybridTimestamp, Identity,
	LeaseDescriptor, NodeId, ReleaseLeaseRequestMeta, TableObjectId, TenantId, uuid_v7_unix_millis,
};
use crate::storage::counter::DEFAULT_COUNTER_CLOCK_SKEW_MS;

/// `H`: the longest a caller retries one parent. Borrowed from the counter
/// SDK's horizon as a placeholder; whether table and KV callers fit it is an
/// open S3 obligation, not settled here.
pub(crate) const DECISION_ATTEMPT_HORIZON_MS: u64 = COUNTER_ATTEMPT_HORIZON_MS;
/// `S`: how far a parent's issuance may lead the committed clock.
pub(crate) const DECISION_CLOCK_SKEW_MS: u64 = DEFAULT_COUNTER_CLOCK_SKEW_MS;
/// `E`: how long after issuance a key stays presentable.
pub(crate) const DECISION_EXPIRY_MS: u64 =
	COUNTER_INCARNATION_LIFETIME_MS + DECISION_ATTEMPT_HORIZON_MS + DECISION_CLOCK_SKEW_MS;
const _: () = assert!(DECISION_CLOCK_SKEW_MS <= DECISION_ATTEMPT_HORIZON_MS);
const _: () = assert!(
	DECISION_EXPIRY_MS
		>= COUNTER_INCARNATION_LIFETIME_MS + DECISION_ATTEMPT_HORIZON_MS + DECISION_CLOCK_SKEW_MS
);

/// Children of one parent.
pub(crate) const MAX_CHILDREN: usize = 64;
/// Members of one child's replica plan.
pub(crate) const MAX_PLAN_MEMBERS: usize = 16;
/// Rows of one child.
pub(crate) const MAX_ROWS_PER_CHILD: u32 = 1 << 20;
/// Encoded bytes of one claim.
pub(crate) const MAX_CLAIM_BYTES: u64 = 64 * 1024;
/// Parents reserved and not yet terminal: a concurrency limit, not a lifetime one.
pub(crate) const LIVE_PARENT_SLOTS: u32 = 4096;
/// Encoded claim bytes of every retained body. Bodies are reclaimed only by S5,
/// so until then this limit can legitimately stop admission.
pub(crate) const MAX_RETAINED_CLAIM_BYTES: u64 = 256 * 1024 * 1024;

/// Which identity space a parent's key lives in. CTAS and counters have none in
/// this checkpoint, so they cannot be expressed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ParentNamespace {
	Kv,
	Table(TableObjectId),
}

/// One logical write: the caller's v7 uuid and sequence, per tenant and namespace.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ParentKey {
	pub tenant: TenantId,
	pub namespace: ParentNamespace,
	pub client_uuid: [u8; 16],
	pub sequence_no: u64,
}

impl ParentKey {
	/// The issuance time its uuid carries; `None` when it is not a UUIDv7.
	pub fn issued_ms(&self) -> Option<u64> {
		uuid_v7_unix_millis(self.client_uuid)
	}
}

/// A node's store, as admitted by the incarnation registry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Signer {
	pub node: u32,
	pub incarnation: StoreIncarnation,
}

/// A grant of one range of the range universe.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lineage {
	pub range_end: u64,
	pub epoch: u64,
	pub holder: NodeId,
	pub incarnation: StoreIncarnation,
	pub generation: u64,
}

/// The lineage a claim is bound to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LineageRef {
	pub range_start: u64,
	pub generation: u64,
}

/// One child: a contiguous ring-position range inside one lineage, written to
/// one replica plan.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildClaim {
	pub range_start: u64,
	pub range_end: u64,
	pub lineage: LineageRef,
	pub holder: Signer,
	/// The replica plan, holder included, each with its current incarnation.
	pub plan: Vec<Signer>,
	pub rows: u32,
	pub content: [u8; 32],
}

/// What a parent claims. Children are in canonical order: strictly increasing,
/// non-overlapping ranges. The coordinator is named separately, as the child
/// holding the ring position of the caller's first input row, which is how the
/// caller routes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Claim {
	pub key: ParentKey,
	pub request_digest: [u8; 32],
	pub commit_hlc: HybridTimestamp,
	pub coordinator: u16,
	pub first_row_position: u64,
	pub children: Vec<ChildClaim>,
	/// ADR 0699 S4b: what the parent publishes.
	pub operation: Operation,
}

/// What a parent publishes: rows, or the tombstone of one selected row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Operation {
	Write,
	Delete {
		request: DeleteRequest,
		selection: DeleteSelection,
	},
}

/// A delete of exactly one row id (ADR 0699 S4b).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteRequest {
	pub table: TableObjectId,
	pub schema_generation: u64,
	pub row: Identity,
}

/// The delete's selection at its cut, frozen in the record. It is the
/// coordinator's attested evaluation: the reducer checks its structure, not
/// the absence or presence it reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteSelection {
	pub cut: u64,
	pub outcome: DeleteOutcome,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeleteOutcome {
	/// The row's winner at the cut was a live version: a tombstone is published.
	Deleted {
		winner_rsv: u64,
		winner_hlc: HybridTimestamp,
	},
	/// The row's winner at the cut was already a tombstone.
	AlreadyDeleted {
		winner_rsv: u64,
		winner_hlc: HybridTimestamp,
	},
	/// No Accepted version of the row at the cut.
	NotFound,
}

/// A delete that published nothing, recorded terminal (RecordNoop). Signed by
/// the coordinator: the holder of the row's position.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoopBody {
	pub key: ParentKey,
	pub request_digest: [u8; 32],
	pub request: DeleteRequest,
	pub selection: DeleteSelection,
	pub position: u64,
	pub coordinator: Signer,
	pub lineage: LineageRef,
}

/// A recorded RecordNoop, retained like a body until S5's reclamation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoopRecord {
	pub dec: u64,
	pub body: NoopBody,
	pub bytes: u64,
}

/// The request digest of a single-row delete: fixed-width fields under its own
/// derivation domain.
pub fn delete_request_digest(tenant: TenantId, request: &DeleteRequest) -> [u8; 32] {
	let mut hasher = blake3::Hasher::new_derive_key("wendelon delete request v1");
	hasher.update(&tenant.get().to_le_bytes());
	hasher.update(request.table.as_bytes());
	hasher.update(&request.schema_generation.to_le_bytes());
	hasher.update(request.row.as_bytes());
	*hasher.finalize().as_bytes()
}

/// The routing locality of a row id, for a PrimaryKey table whose locality is
/// a function of the id alone; `None` for any other table.
pub fn delete_row_locality(
	tenant: TenantId,
	descriptor: &crate::schema::TableSchemaDescriptor,
	row: Identity,
) -> Option<crate::schema::Locality> {
	use crate::schema::{LocalityPolicy, RowIdentityPolicy};
	if !matches!(
		descriptor.row_identity_policy,
		RowIdentityPolicy::PrimaryKey(_)
	) {
		return None;
	}
	match descriptor.locality_policy {
		LocalityPolicy::IdentityProjection => Some(crate::schema::Locality::from(row)),
		LocalityPolicy::TableSpreadProjection(spread) => {
			crate::schema::table::table_spread_locality(
				tenant,
				descriptor.table_object_id,
				row,
				spread,
			)
			.ok()
		}
		LocalityPolicy::ColumnProjection(_) => None,
	}
}

/// The ring position of a row id: its partition for a partitioned table, its
/// locality otherwise, as a stored record of it is placed.
pub fn delete_row_position(
	tenant: TenantId,
	descriptor: &crate::schema::TableSchemaDescriptor,
	row: Identity,
) -> Option<u64> {
	let locality = delete_row_locality(tenant, descriptor, row)?;
	match crate::schema::table::partitioned_prefix_width(descriptor) {
		Some(bits) => crate::schema::table::partition_for_locality(locality, bits)
			.ok()
			.map(|partition| partition.get()),
		None => Some(locality.get()),
	}
}

impl Claim {
	fn coordinator_child(&self) -> Option<&ChildClaim> {
		self.children.get(usize::from(self.coordinator))
	}
}

/// A node's development decision-signing secret.
///
/// # Security
/// Publicly derivable. The `ParentDecisions` mode is development-only and
/// requires dev keys; production key custody is a production-required item.
pub fn dev_decision_signing_secret(node_id: u32) -> [u8; 32] {
	crate::hash::sha256::digest(format!("wendelon-dev-decision-signing-key-{node_id}").as_bytes())
}

pub fn decision_verifying_key_bytes(secret: [u8; 32]) -> [u8; 32] {
	ed25519_dalek::SigningKey::from_bytes(&secret)
		.verifying_key()
		.to_bytes()
}

/// Domain-separated digest of a whole claim; every acknowledgement binds it.
pub fn claim_digest(claim: &Claim) -> [u8; 32] {
	let bytes = postcard::to_stdvec(claim).expect("a claim encodes");
	let mut hasher = blake3::Hasher::new_derive_key("wendelon parent decision claim v1");
	hasher.update(&bytes);
	*hasher.finalize().as_bytes()
}

/// A child's acknowledgement that it holds its rows durably, bound to one
/// reservation of one claim.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildAckBody {
	pub key: ParentKey,
	pub reservation: u64,
	pub claim_digest: [u8; 32],
	pub ordinal: u16,
	pub range_start: u64,
	pub range_end: u64,
	pub lineage: LineageRef,
	pub holder_incarnation: StoreIncarnation,
	pub rows: u32,
	pub content: [u8; 32],
	pub acker: Signer,
}

/// The coordinator's abort of one reservation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbortBody {
	pub key: ParentKey,
	pub reservation: u64,
	pub claim_digest: [u8; 32],
	pub coordinator: Signer,
	pub lineage: LineageRef,
}

/// A body with its signer and signature. Verification is `evidence::verify`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Signed<T> {
	pub body: T,
	pub signer: Signer,
	#[serde(with = "serde_bytes")]
	pub signature: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DecisionCommand {
	/// Signed by the coordinator child's holder.
	Reserve {
		claim: Signed<Claim>,
		stamp_ms: u64,
	},
	Finalize {
		key: ParentKey,
		request_digest: [u8; 32],
		acks: Vec<Signed<ChildAckBody>>,
	},
	/// Refuses a reserved parent whose bound lineage closed; needs no signature.
	Resolve {
		key: ParentKey,
		request_digest: [u8; 32],
	},
	Abort {
		abort: Signed<AbortBody>,
	},
	/// Retires terminal retry keys issued at or before `floor_ms`. Bodies stay.
	AdvanceFloor {
		floor_ms: u64,
		stamp_ms: u64,
	},
	/// Binds a store incarnation to its decision-signing key, immutably. The
	/// leader proposes it only for the node authenticated on the connection,
	/// whose configured decision key it is.
	BindSigner {
		signer: Signer,
		key: [u8; 32],
	},
	/// ADR 0699 S4b: records a delete that published nothing, signed by its
	/// coordinator.
	RecordNoop {
		noop: Signed<NoopBody>,
		stamp_ms: u64,
	},
	/// ADR 0699 S5a: the leader proposes a generation over a closed cut.
	ProposeTarget {
		target: Target,
	},
	/// ADR 0699 S5a: a member's report of its durably staged inventory.
	Report {
		report: Signed<ReportBody>,
	},
	/// ADR 0699 S5a: makes the pending generation current and erases its cut.
	CommitGeneration {
		gid: u64,
	},
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RefusalCause {
	LineageClosed,
	Aborted,
}

/// A parent's status. A terminal status carries `dec`, the Raft log index of
/// the command that first made it terminal; later commands never move it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ParentStatus {
	Reserved,
	Accepted {
		dec: u64,
	},
	Refused {
		cause: RefusalCause,
		dec: u64,
	},
	/// ADR 0699 S4b: a recorded RecordNoop; `dec` is its own log index. Never a
	/// body's status.
	Noop {
		dec: u64,
	},
}

impl ParentStatus {
	pub fn is_terminal(self) -> bool {
		!matches!(self, Self::Reserved)
	}

	/// The log index of the first terminal decision.
	pub fn dec(self) -> Option<u64> {
		match self {
			Self::Reserved => None,
			Self::Accepted { dec } | Self::Refused { dec, .. } | Self::Noop { dec } => Some(dec),
		}
	}
}

/// Why a command changed nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DecisionRefusal {
	/// The store runs today's authority.
	ModeToday,
	/// No signature this build can verify.
	Unverifiable,
	/// The signer is not the identity the body binds.
	SignerMismatch,
	/// The key names a different request.
	Conflict,
	/// The key's uuid is not a UUIDv7.
	InvalidIdentity,
	/// Issued more than `S` ahead of the committed clock.
	IdentityFromFuture,
	/// Issued at or before the retirement cutoff and not on record.
	OutcomeUnavailable,
	/// No retry record for a key that could still be presented.
	UnknownParent,
	MalformedClaim,
	LineageClosed,
	PlanMismatch,
	MigrationActive,
	TenantNotWritable,
	TableUnavailable,
	LiveSlotsFull,
	RetainedFull,
	CounterOverflow,
	/// A log index already used by a body, or a decision not after its Reserve.
	LogIndexOutOfOrder,
	/// A binding for a store incarnation that is not its node's current one.
	SignerNotCurrent,
	/// A key that is not a valid Ed25519 verifying key.
	MalformedKey,
	/// A second key for an incarnation, or one key for two nodes.
	SignerRebind,
	/// An acknowledgement that does not bind this reservation's claim.
	EvidenceMismatch,
	FloorNotAdvanced,
	FloorTooRecent,
	/// ADR 0699 S5a: the cut is not closed over terminal decisions and retired keys.
	TargetNotAdmissible,
	/// A generation that is not the next, not pending, or not on the current one.
	StaleGeneration,
	/// A report that is malformed, from a non-member or disagrees.
	ReportMismatch,
	/// Not every member has reported.
	BarrierIncomplete,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DecisionResult {
	/// The parent is reserved (fresh or a duplicate).
	Reserved {
		reservation: u64,
	},
	/// The parent is terminal (now or earlier).
	Terminal {
		reservation: u64,
		status: ParentStatus,
	},
	/// Valid acknowledgements, not yet a quorum for every child.
	PendingEvidence,
	/// Every bound lineage is still open.
	StillOpen,
	FloorAdvanced {
		floor_ms: u64,
		retired_keys: u64,
	},
	/// The incarnation is bound to the key (now or earlier).
	SignerBound,
	Refused(DecisionRefusal),
	/// ADR 0699 S5a.
	TargetRecorded {
		gid: u64,
	},
	ReportRecorded {
		gid: u64,
		reports: u32,
	},
	GenerationCommitted {
		gid: u64,
	},
}

/// What a retry of `key` with `request_digest` learns. Pure over state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Answer {
	New,
	Reserved,
	Accepted,
	Refused(RefusalCause),
	Conflict,
	InvalidIdentity,
	/// Never New and never known-not-committed.
	OutcomeUnavailable,
	/// A recorded RecordNoop.
	Noop,
}

/// A reserved parent's body. `rsv` is the Raft log index of the Reserve that
/// created it and is the body's key; `status.dec()` is its first terminal index.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParentRecord {
	pub rsv: u64,
	pub claim: Claim,
	pub claim_digest: [u8; 32],
	pub claim_bytes: u64,
	pub status: ParentStatus,
}

impl ParentRecord {
	/// The status a reader at committed cut `cut` must use: none before the
	/// Reserve, Reserved until the first terminal decision, then that decision.
	pub fn status_at(&self, cut: u64) -> Option<ParentStatus> {
		if cut < self.rsv {
			return None;
		}
		match self.status.dec() {
			Some(dec) if dec <= cut => Some(self.status),
			_ => Some(ParentStatus::Reserved),
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyEntry {
	/// The record's log index: a body's `rsv` or a Noop's `dec`.
	pub reservation: u64,
	pub request_digest: [u8; 32],
	pub record: RecordKind,
}

/// Which map a retry key's record is in.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecordKind {
	Body,
	Noop,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionState {
	lineages: BTreeMap<u64, Lineage>,
	next_generation: u64,
	/// Bodies by `rsv`, the Reserve's Raft log index.
	bodies: BTreeMap<u64, ParentRecord>,
	keys: BTreeMap<ParentKey, KeyEntry>,
	floor_ms: u64,
	clock_ms: u64,
	live: u32,
	retained_claim_bytes: u64,
	/// Each store incarnation's decision-signing key. Immutable once bound and
	/// never pruned here, so retained evidence keeps verifying on replay; current
	/// authority is checked separately against the incarnation registry.
	signers: BTreeMap<(u32, StoreIncarnation), [u8; 32]>,
	/// ADR 0699 S4b: recorded RecordNoops by `dec`, retained like bodies.
	noops: BTreeMap<u64, NoopRecord>,
	/// ADR 0699 S5a: the current certificate and the pending target.
	generations: Generations,
}

impl Default for DecisionState {
	fn default() -> Self {
		Self {
			lineages: BTreeMap::new(),
			next_generation: 1,
			bodies: BTreeMap::new(),
			keys: BTreeMap::new(),
			floor_ms: 0,
			clock_ms: 0,
			live: 0,
			retained_claim_bytes: 0,
			signers: BTreeMap::new(),
			noops: BTreeMap::new(),
			generations: Generations::default(),
		}
	}
}

impl DecisionState {
	pub fn lineages(&self) -> &BTreeMap<u64, Lineage> {
		&self.lineages
	}
	pub fn bodies(&self) -> &BTreeMap<u64, ParentRecord> {
		&self.bodies
	}
	pub fn body(&self, reservation: u64) -> Option<&ParentRecord> {
		self.bodies.get(&reservation)
	}
	pub fn noops(&self) -> &BTreeMap<u64, NoopRecord> {
		&self.noops
	}
	pub fn generations(&self) -> &Generations {
		&self.generations
	}
	pub fn retained_bodies(&self) -> usize {
		self.bodies.len()
	}
	pub fn retry_keys(&self) -> usize {
		self.keys.len()
	}
	pub fn live(&self) -> u32 {
		self.live
	}
	pub fn floor_ms(&self) -> u64 {
		self.floor_ms
	}
	pub fn clock_ms(&self) -> u64 {
		self.clock_ms
	}
	pub fn retained_claim_bytes(&self) -> u64 {
		self.retained_claim_bytes
	}
	pub fn next_generation(&self) -> u64 {
		self.next_generation
	}

	/// The cutoff at or below which an unrecorded key is no longer presentable.
	fn retirement_cutoff(&self) -> u64 {
		self.floor_ms
			.max(self.clock_ms.saturating_sub(DECISION_EXPIRY_MS))
	}

	/// Checks the redundant counters against the maps. Run after decode.
	pub fn signer_key(&self, signer: Signer) -> Option<[u8; 32]> {
		self.signers
			.get(&(signer.node, signer.incarnation))
			.copied()
	}

	pub(crate) fn validate(&self) -> Result<(), String> {
		let mut seen_keys = std::collections::BTreeMap::new();
		for ((node, _), key) in &self.signers {
			if ed25519_dalek::VerifyingKey::from_bytes(key).is_err() {
				return Err("decisions: a bound signer key is not a verifying key".into());
			}
			if *seen_keys.entry(*key).or_insert(*node) != *node {
				return Err("decisions: one signer key is bound to two nodes".into());
			}
		}
		let live = self
			.bodies
			.values()
			.filter(|body| !body.status.is_terminal())
			.count();
		if u32::try_from(live).ok() != Some(self.live) {
			return Err(format!(
				"decisions: live {} but {live} reserved bodies",
				self.live
			));
		}
		let bytes = self
			.bodies
			.values()
			.map(|body| body.claim_bytes)
			.chain(self.noops.values().map(|noop| noop.bytes))
			.try_fold(0u64, |sum, bytes| sum.checked_add(bytes));
		if bytes != Some(self.retained_claim_bytes) {
			return Err("decisions: retained bytes do not match the bodies and noops".into());
		}
		for (key, entry) in &self.keys {
			let names = match entry.record {
				RecordKind::Body => self
					.bodies
					.get(&entry.reservation)
					.map(|body| (body.claim.key, body.claim.request_digest)),
				RecordKind::Noop => self
					.noops
					.get(&entry.reservation)
					.map(|noop| (noop.body.key, noop.body.request_digest)),
			};
			if names != Some((*key, entry.request_digest)) {
				return Err("decisions: a retry key names no record of that key and digest".into());
			}
		}
		let mut noop_keys = std::collections::BTreeSet::new();
		for (dec, noop) in &self.noops {
			if noop.dec != *dec
				|| noop.body.selection.cut >= *dec
				|| self.bodies.contains_key(dec)
				|| postcard::to_stdvec(&noop.body).map(|bytes| bytes.len() as u64) != Ok(noop.bytes)
				|| matches!(noop.body.selection.outcome, DeleteOutcome::Deleted { .. })
			{
				return Err("decisions: a noop's coordinates, size or outcome are invalid".into());
			}
			if !noop_keys.insert(noop.body.key) {
				return Err("decisions: two noops share a key".into());
			}
		}
		let mut body_keys = std::collections::BTreeSet::new();
		for (rsv, body) in &self.bodies {
			if body.rsv != *rsv {
				return Err("decisions: a body is not stored at its reservation index".into());
			}
			if body.status.dec().is_some_and(|dec| dec <= *rsv) {
				return Err(
					"decisions: a terminal decision does not follow its reservation".into(),
				);
			}
			if body.claim_digest != claim_digest(&body.claim)
				|| postcard::to_stdvec(&body.claim).map(|bytes| bytes.len() as u64)
					!= Ok(body.claim_bytes)
			{
				return Err("decisions: a body's digest or size does not match its claim".into());
			}
			if !body_keys.insert(body.claim.key) || noop_keys.contains(&body.claim.key) {
				return Err("decisions: two records share a key".into());
			}
			if matches!(body.status, ParentStatus::Noop { .. }) {
				return Err("decisions: a body carries a noop status".into());
			}
			// A live parent is always reachable by its retry key.
			if !body.status.is_terminal()
				&& self
					.keys
					.get(&body.claim.key)
					.map(|entry| entry.reservation)
					!= Some(*rsv)
			{
				return Err("decisions: a reserved body has lost its retry key".into());
			}
		}
		let generations = &self.generations;
		let fr = generations.fr();
		if self.bodies.range(..=fr).next().is_some() || self.noops.range(..=fr).next().is_some() {
			return Err("decisions: a record at or below the certified cut is retained".into());
		}
		if generations
			.current
			.as_ref()
			.is_some_and(|cert| cert.chunks.len() != generations::GENERATION_CHUNKS)
			|| generations.next_gid <= generations.current_gid()
			|| generations.pending.as_ref().is_some_and(|pending| {
				pending.target.gid >= generations.next_gid
					|| pending.target.predecessor_gid != generations.current_gid()
					|| pending.digest != generations::target_digest(&pending.target)
					|| pending.reports.len() > pending.target.members.len()
			}) {
			return Err("decisions: generation state is inconsistent".into());
		}
		for lineage in self.lineages.values() {
			if lineage.generation >= self.next_generation {
				return Err("decisions: a lineage at or above next_generation".into());
			}
		}
		Ok(())
	}
}

/// The epoch of the latest committed ring configuration.
fn current_epoch(state: &SmState) -> Option<u64> {
	state
		.epoch_history
		.last_key_value()
		.map(|(_, config)| config.epoch)
}

fn current_incarnation(state: &SmState, node: u32) -> Option<StoreIncarnation> {
	state
		.incarnations
		.get(&node)
		.and_then(|registry| registry.current)
}

fn lineage_open(
	state: &SmState,
	decisions: &DecisionState,
	lineage: LineageRef,
	holder: Signer,
) -> bool {
	let Some(entry) = decisions.lineages.get(&lineage.range_start) else {
		return false;
	};
	entry.generation == lineage.generation
		&& entry.holder.id == holder.node
		&& entry.incarnation == holder.incarnation
		&& Some(entry.epoch) == current_epoch(state)
		&& current_incarnation(state, holder.node) == Some(entry.incarnation)
}

/// The reserved parents a Resolve would refuse now: some bound lineage closed,
/// and this node holds the lineage now covering the coordinator child's range.
/// The new holder of the coordinator's range is the preferred driver.
pub(crate) fn resolvable_by(
	state: &SmState,
	node: u32,
	limit: usize,
) -> Vec<(ParentKey, [u8; 32])> {
	let Some(decisions) = state.decisions.as_ref() else {
		return Vec::new();
	};
	decisions
		.bodies
		.values()
		.filter(|body| !body.status.is_terminal())
		.filter(|body| any_lineage_closed(state, decisions, &body.claim))
		.filter(|body| {
			body.claim.coordinator_child().is_some_and(|child| {
				decisions
					.lineages
					.range(..=child.range_start)
					.next_back()
					.is_some_and(|(_, lineage)| {
						lineage.holder.id == node && child.range_start <= lineage.range_end
					})
			})
		})
		.take(limit)
		.map(|body| (body.claim.key, body.claim.request_digest))
		.collect()
}

/// Whether `lineage` is open for `holder` under the applied state: what a child
/// holder checks before writing (ADR 0699 S3b).
pub(crate) fn lineage_is_open(state: &SmState, lineage: LineageRef, holder: Signer) -> bool {
	state
		.decisions
		.as_ref()
		.is_some_and(|decisions| lineage_open(state, decisions, lineage, holder))
}

fn any_lineage_closed(state: &SmState, decisions: &DecisionState, claim: &Claim) -> bool {
	claim
		.children
		.iter()
		.any(|child| !lineage_open(state, decisions, child.lineage, child.holder))
}

// ============================================================
//  LINEAGE MAINTENANCE (called from the lease, release,
//  incarnation and epoch apply arms, in this mode only)
// ============================================================

/// What an accepted grant does to its lineage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GrantEffect {
	Renewal,
	NewLineage {
		generation: u64,
		incarnation: StoreIncarnation,
	},
}

/// Admission of a lease grant under this mode, before today's overlap rule
/// runs. `Err` means `LeaseDenied` with nothing changed.
pub(crate) fn admit_lease_grant(
	state: &SmState,
	lease: &LeaseDescriptor,
) -> Result<GrantEffect, ()> {
	let decisions = state.decisions.as_ref().ok_or(())?;
	let (_, config) = state.epoch_history.last_key_value().ok_or(())?;
	if lease.epoch != config.epoch || lease.lease_start >= lease.lease_expiry {
		return Err(());
	}
	let incarnation = current_incarnation(state, lease.holder.id).ok_or(())?;
	// The range universe: exactly one of the holder's merged primary ranges in
	// the current ring. The primary does not depend on the replication factor.
	let ring = RingState::build(&membership_from_epoch_config(config), 1);
	if !ring
		.primary_ranges_for(lease.holder)
		.contains(&(lease.range_start, lease.range_end))
	{
		return Err(());
	}
	let continuing = state.leases.get(&lease.range_start).filter(|existing| {
		existing.range_end == lease.range_end
			&& existing.holder == lease.holder
			&& existing.epoch == lease.epoch
			&& existing.lease_expiry > lease.lease_start
	});
	let lineage = decisions
		.lineages
		.get(&lease.range_start)
		.filter(|lineage| {
			lineage.range_end == lease.range_end
				&& lineage.holder == lease.holder
				&& lineage.epoch == lease.epoch
				&& lineage.incarnation == incarnation
		});
	if let (Some(existing), Some(_)) = (continuing, lineage) {
		// A renewal never shortens the grant it continues.
		if lease.lease_expiry < existing.lease_expiry {
			return Err(());
		}
		return Ok(GrantEffect::Renewal);
	}
	let generation = decisions.next_generation;
	generation.checked_add(1).ok_or(())?;
	Ok(GrantEffect::NewLineage {
		generation,
		incarnation,
	})
}

/// Records an accepted grant. Called only after today's arm inserted the lease.
pub(crate) fn record_grant(state: &mut SmState, lease: &LeaseDescriptor, effect: GrantEffect) {
	let Some(decisions) = state.decisions.as_mut() else {
		return;
	};
	if let GrantEffect::NewLineage {
		generation,
		incarnation,
	} = effect
	{
		decisions.next_generation = generation + 1;
		decisions.lineages.insert(
			lease.range_start,
			Lineage {
				range_end: lease.range_end,
				epoch: lease.epoch,
				holder: lease.holder,
				incarnation,
				generation,
			},
		);
	}
}

/// Records a matching release: its lineage ends.
pub(crate) fn record_release(state: &mut SmState, release: &ReleaseLeaseRequestMeta) {
	let Some(decisions) = state.decisions.as_mut() else {
		return;
	};
	if decisions
		.lineages
		.get(&release.range_start)
		.is_some_and(|lineage| {
			lineage.range_end == release.range_end
				&& lineage.epoch == release.epoch
				&& lineage.holder == release.holder
		}) {
		decisions.lineages.remove(&release.range_start);
	}
}

/// Removes every lineage of another epoch or of a superseded incarnation.
pub(crate) fn prune_lineages(state: &mut SmState) {
	let epoch = current_epoch(state);
	let current: BTreeMap<u32, Option<StoreIncarnation>> = state
		.incarnations
		.iter()
		.map(|(node, registry)| (*node, registry.current))
		.collect();
	let Some(decisions) = state.decisions.as_mut() else {
		return;
	};
	decisions.lineages.retain(|_, lineage| {
		Some(lineage.epoch) == epoch
			&& current.get(&lineage.holder.id).copied().flatten() == Some(lineage.incarnation)
	});
}

// ============================================================
//  EVIDENCE BOUNDARY
// ============================================================

pub(crate) mod evidence {
	use super::{
		AbortBody, ChildAckBody, Claim, DecisionRefusal, DecisionState, NoopBody, ReportBody,
		Signed, Signer,
	};
	use ed25519_dalek::{Signature, Signer as _, SigningKey, VerifyingKey};
	use serde::Serialize;

	/// A body whose signature was verified against the identity it binds. Only
	/// [`verify`] makes one.
	pub(crate) struct Verified<T>(T);

	impl<T> Verified<T> {
		pub(crate) fn body(&self) -> &T {
			&self.0
		}
	}

	/// The identity a body binds, and its signature domain.
	pub(crate) trait Evidence: Clone + Serialize {
		const DOMAIN: &'static str;
		fn bound_signer(&self) -> Option<Signer>;
	}

	impl Evidence for Claim {
		const DOMAIN: &'static str = "wendelon parent decision reserve v1";
		fn bound_signer(&self) -> Option<Signer> {
			self.coordinator_child().map(|child| child.holder)
		}
	}

	impl Evidence for ChildAckBody {
		const DOMAIN: &'static str = "wendelon parent decision child ack v1";
		fn bound_signer(&self) -> Option<Signer> {
			Some(self.acker)
		}
	}

	impl Evidence for ReportBody {
		const DOMAIN: &'static str = "wendelon parent decision generation report v1";
		fn bound_signer(&self) -> Option<Signer> {
			Some(self.reporter)
		}
	}

	impl Evidence for NoopBody {
		const DOMAIN: &'static str = "wendelon parent decision noop v1";
		fn bound_signer(&self) -> Option<Signer> {
			Some(self.coordinator)
		}
	}

	impl Evidence for AbortBody {
		const DOMAIN: &'static str = "wendelon parent decision abort v1";
		fn bound_signer(&self) -> Option<Signer> {
			Some(self.coordinator)
		}
	}

	/// The signed bytes: a length-prefixed domain, the body and the signer.
	pub(crate) fn signed_payload<T: Evidence>(body: &T, signer: Signer) -> Vec<u8> {
		let mut payload = Vec::new();
		payload.extend_from_slice(&(T::DOMAIN.len() as u32).to_le_bytes());
		payload.extend_from_slice(T::DOMAIN.as_bytes());
		payload.extend(postcard::to_stdvec(body).expect("evidence encodes"));
		payload.extend(postcard::to_stdvec(&signer).expect("signer encodes"));
		payload
	}

	/// Signs `body` as `signer` with that incarnation's decision key. Admission
	/// (S3b/c) signs acknowledgements and claims with it.
	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) fn sign<T: Evidence>(key: &SigningKey, body: T, signer: Signer) -> Signed<T> {
		let signature = key.sign(&signed_payload(&body, signer)).to_bytes().to_vec();
		Signed {
			body,
			signer,
			signature,
		}
	}

	/// Ed25519 (strict) against the key replicated for that exact incarnation.
	/// An unbound incarnation verifies nothing. Whether the signer may still act
	/// is checked by the caller against current authority.
	pub(crate) fn verify<T: Evidence>(
		decisions: &DecisionState,
		signed: &Signed<T>,
	) -> Result<Verified<T>, DecisionRefusal> {
		if signed.body.bound_signer() != Some(signed.signer) {
			return Err(DecisionRefusal::SignerMismatch);
		}
		let key = decisions
			.signer_key(signed.signer)
			.ok_or(DecisionRefusal::Unverifiable)?;
		let key = VerifyingKey::from_bytes(&key).map_err(|_| DecisionRefusal::Unverifiable)?;
		let signature: [u8; 64] = signed
			.signature
			.as_slice()
			.try_into()
			.map_err(|_| DecisionRefusal::Unverifiable)?;
		key.verify_strict(
			&signed_payload(&signed.body, signed.signer),
			&Signature::from_bytes(&signature),
		)
		.map_err(|_| DecisionRefusal::Unverifiable)?;
		Ok(Verified(signed.body.clone()))
	}
}

// ============================================================
//  REDUCER
// ============================================================

/// Applies one decision command. Every refusal leaves state unchanged,
/// including the committed clock.
/// `log_index` is the Raft log index of the entry being applied: a Reserve's
/// becomes the parent's `rsv`, a first terminal decision's becomes its `dec`.
pub(crate) fn apply(
	state: &mut SmState,
	command: DecisionCommand,
	log_index: u64,
) -> DecisionResult {
	let Some(mut decisions) = state.decisions.take() else {
		return DecisionResult::Refused(DecisionRefusal::ModeToday);
	};
	let result = reduce(state, &mut decisions, command, log_index);
	state.decisions = Some(decisions);
	result
}

fn reduce(
	state: &SmState,
	decisions: &mut DecisionState,
	command: DecisionCommand,
	log_index: u64,
) -> DecisionResult {
	let outcome = match command {
		DecisionCommand::Reserve { claim, stamp_ms } => {
			reserve(state, decisions, &claim, stamp_ms, log_index)
		}
		DecisionCommand::Finalize {
			key,
			request_digest,
			acks,
		} => finalize(state, decisions, key, request_digest, &acks, log_index),
		DecisionCommand::Resolve {
			key,
			request_digest,
		} => resolve(state, decisions, key, request_digest, log_index),
		DecisionCommand::Abort { abort } => abort_parent(state, decisions, &abort, log_index),
		DecisionCommand::AdvanceFloor { floor_ms, stamp_ms } => {
			advance_floor(decisions, floor_ms, stamp_ms)
		}
		DecisionCommand::BindSigner { signer, key } => bind_signer(state, decisions, signer, key),
		DecisionCommand::RecordNoop { noop, stamp_ms } => {
			record_noop(state, decisions, &noop, stamp_ms, log_index)
		}
		DecisionCommand::ProposeTarget { target } => {
			generations::propose_target(state, decisions, target, log_index)
		}
		DecisionCommand::Report { report } => generations::record_report(state, decisions, &report),
		DecisionCommand::CommitGeneration { gid } => {
			generations::commit_generation(state, decisions, gid, log_index)
		}
	};
	outcome.unwrap_or_else(DecisionResult::Refused)
}

/// The record a retry lookup finds, after its request digest matched.
fn lookup(
	decisions: &DecisionState,
	key: &ParentKey,
	request_digest: &[u8; 32],
) -> Result<Option<(u64, ParentStatus)>, DecisionRefusal> {
	let Some(entry) = decisions.keys.get(key) else {
		return Ok(None);
	};
	if entry.request_digest != *request_digest {
		return Err(DecisionRefusal::Conflict);
	}
	let status = match entry.record {
		RecordKind::Body => {
			decisions
				.bodies
				.get(&entry.reservation)
				.expect("a retry key names a retained body")
				.status
		}
		RecordKind::Noop => ParentStatus::Noop {
			dec: entry.reservation,
		},
	};
	Ok(Some((entry.reservation, status)))
}

fn existing_result(reservation: u64, status: ParentStatus) -> DecisionResult {
	match status {
		ParentStatus::Reserved => DecisionResult::Reserved { reservation },
		status => DecisionResult::Terminal {
			reservation,
			status,
		},
	}
}

/// A key with no retry record: unpresentable at or below the cutoff.
fn unrecorded(decisions: &DecisionState, key: &ParentKey) -> DecisionRefusal {
	match key.issued_ms() {
		None => DecisionRefusal::InvalidIdentity,
		Some(issued) if issued <= decisions.retirement_cutoff() => {
			DecisionRefusal::OutcomeUnavailable
		}
		Some(_) => DecisionRefusal::UnknownParent,
	}
}

fn reserve(
	state: &SmState,
	decisions: &mut DecisionState,
	signed: &Signed<Claim>,
	stamp_ms: u64,
	log_index: u64,
) -> Result<DecisionResult, DecisionRefusal> {
	// A recorded parent answers as recorded, with no evidence check: a retry
	// after authority moved must still learn its result.
	if let Some((reservation, status)) =
		lookup(decisions, &signed.body.key, &signed.body.request_digest)?
	{
		return Ok(existing_result(reservation, status));
	}
	let verified = evidence::verify(decisions, signed)?;
	let claim = verified.body();
	let clock_ms = fresh_clock(decisions, &claim.key, stamp_ms)?;
	validate_claim(state, decisions, claim)?;
	if let Operation::Delete { selection, .. } = &claim.operation {
		if selection.cut >= log_index {
			return Err(DecisionRefusal::MalformedClaim);
		}
	}
	if decisions.live >= LIVE_PARENT_SLOTS {
		return Err(DecisionRefusal::LiveSlotsFull);
	}
	let claim_bytes = postcard::to_stdvec(claim)
		.map(|bytes| bytes.len() as u64)
		.map_err(|_| DecisionRefusal::MalformedClaim)?;
	if claim_bytes > MAX_CLAIM_BYTES {
		return Err(DecisionRefusal::MalformedClaim);
	}
	let retained = decisions
		.retained_claim_bytes
		.checked_add(claim_bytes)
		.filter(|total| *total <= MAX_RETAINED_CLAIM_BYTES)
		.ok_or(DecisionRefusal::RetainedFull)?;
	// The reservation is the Reserve entry's own log index, never a counter.
	let reservation = log_index;
	if decisions.bodies.contains_key(&reservation) {
		return Err(DecisionRefusal::LogIndexOutOfOrder);
	}
	// Every check passed: commit.
	decisions.clock_ms = clock_ms;
	decisions.retained_claim_bytes = retained;
	decisions.live += 1;
	decisions.keys.insert(
		claim.key,
		KeyEntry {
			reservation,
			request_digest: claim.request_digest,
			record: RecordKind::Body,
		},
	);
	decisions.bodies.insert(
		reservation,
		ParentRecord {
			rsv: reservation,
			claim: claim.clone(),
			claim_digest: claim_digest(claim),
			claim_bytes,
			status: ParentStatus::Reserved,
		},
	);
	Ok(DecisionResult::Reserved { reservation })
}

/// E0 and E1 for an unrecorded key: its identity form, issuance not ahead of
/// the committed clock, and not at or below the retirement cutoff. Returns the
/// clock the command commits.
fn fresh_clock(
	decisions: &DecisionState,
	key: &ParentKey,
	stamp_ms: u64,
) -> Result<u64, DecisionRefusal> {
	let clock_ms = decisions.clock_ms.max(stamp_ms);
	let issued = key.issued_ms().ok_or(DecisionRefusal::InvalidIdentity)?;
	let latest = clock_ms
		.checked_add(DECISION_CLOCK_SKEW_MS)
		.ok_or(DecisionRefusal::CounterOverflow)?;
	if issued > latest {
		return Err(DecisionRefusal::IdentityFromFuture);
	}
	let cutoff = decisions
		.floor_ms
		.max(clock_ms.saturating_sub(DECISION_EXPIRY_MS));
	if issued <= cutoff {
		return Err(DecisionRefusal::OutcomeUnavailable);
	}
	Ok(clock_ms)
}

/// The table a delete names, checked as a matched delete and a RecordNoop
/// both require: the namespace is that table, the tenant writes, the table is
/// admitting at the request's generation with a supported shape. Returns the
/// row's position.
fn delete_target(
	state: &SmState,
	key: &ParentKey,
	request: &DeleteRequest,
) -> Result<u64, DecisionRefusal> {
	use DecisionRefusal::*;
	if key.namespace != ParentNamespace::Table(request.table)
		|| request.table == crate::schema::TableObjectId::DEFAULT_KV
	{
		return Err(MalformedClaim);
	}
	let tenant = state.tenants.get(&key.tenant).ok_or(TenantNotWritable)?;
	if !tenant.state.allows_writes() {
		return Err(TenantNotWritable);
	}
	if state.table_drop_blocks_admission(key.tenant, request.table) {
		return Err(TableUnavailable);
	}
	let descriptor = state
		.table_schemas
		.get(key.tenant, request.table, request.schema_generation)
		.ok_or(TableUnavailable)?;
	delete_row_position(key.tenant, descriptor, request.row).ok_or(MalformedClaim)
}

/// ADR 0699 S4b: records a delete that published nothing. Lookup first, so the
/// first record of a key wins; then freshness, the coordinator's signature and
/// current authority over the row's position, and the retained-bytes budget.
/// The outcome itself is the coordinator's attested evaluation at the cut.
fn record_noop(
	state: &SmState,
	decisions: &mut DecisionState,
	signed: &Signed<NoopBody>,
	stamp_ms: u64,
	log_index: u64,
) -> Result<DecisionResult, DecisionRefusal> {
	use DecisionRefusal::*;
	if let Some((reservation, status)) =
		lookup(decisions, &signed.body.key, &signed.body.request_digest)?
	{
		return Ok(existing_result(reservation, status));
	}
	let verified = evidence::verify(decisions, signed)?;
	let noop = verified.body();
	let clock_ms = fresh_clock(decisions, &noop.key, stamp_ms)?;
	if matches!(noop.selection.outcome, DeleteOutcome::Deleted { .. })
		|| noop.selection.cut >= log_index
		|| noop.request_digest != delete_request_digest(noop.key.tenant, &noop.request)
	{
		return Err(MalformedClaim);
	}
	let position = delete_target(state, &noop.key, &noop.request)?;
	if position != noop.position {
		return Err(MalformedClaim);
	}
	let lineage = decisions
		.lineages
		.get(&noop.lineage.range_start)
		.ok_or(LineageClosed)?;
	if !lineage_open(state, decisions, noop.lineage, noop.coordinator)
		|| current_incarnation(state, noop.coordinator.node) != Some(noop.coordinator.incarnation)
		|| position < noop.lineage.range_start
		|| position > lineage.range_end
	{
		return Err(LineageClosed);
	}
	let bytes = postcard::to_stdvec(noop)
		.map(|bytes| bytes.len() as u64)
		.map_err(|_| MalformedClaim)?;
	let retained = decisions
		.retained_claim_bytes
		.checked_add(bytes)
		.filter(|total| *total <= MAX_RETAINED_CLAIM_BYTES)
		.ok_or(RetainedFull)?;
	if decisions.bodies.contains_key(&log_index) || decisions.noops.contains_key(&log_index) {
		return Err(LogIndexOutOfOrder);
	}
	decisions.clock_ms = clock_ms;
	decisions.retained_claim_bytes = retained;
	decisions.keys.insert(
		noop.key,
		KeyEntry {
			reservation: log_index,
			request_digest: noop.request_digest,
			record: RecordKind::Noop,
		},
	);
	decisions.noops.insert(
		log_index,
		NoopRecord {
			dec: log_index,
			body: noop.clone(),
			bytes,
		},
	);
	Ok(DecisionResult::Terminal {
		reservation: log_index,
		status: ParentStatus::Noop { dec: log_index },
	})
}

fn validate_claim(
	state: &SmState,
	decisions: &DecisionState,
	claim: &Claim,
) -> Result<(), DecisionRefusal> {
	use DecisionRefusal::*;
	let children = &claim.children;
	if children.is_empty() || children.len() > MAX_CHILDREN {
		return Err(MalformedClaim);
	}
	let mut previous_end: Option<u64> = None;
	for child in children {
		if child.range_start > child.range_end
			|| child.rows == 0
			|| child.rows > MAX_ROWS_PER_CHILD
			|| child.plan.is_empty()
			|| child.plan.len() > MAX_PLAN_MEMBERS
		{
			return Err(MalformedClaim);
		}
		if previous_end.is_some_and(|end| child.range_start <= end) {
			return Err(MalformedClaim);
		}
		previous_end = Some(child.range_end);
	}
	let coordinator = claim.coordinator_child().ok_or(MalformedClaim)?;
	if !(coordinator.range_start..=coordinator.range_end).contains(&claim.first_row_position) {
		return Err(MalformedClaim);
	}

	let tenant = state
		.tenants
		.get(&claim.key.tenant)
		.ok_or(TenantNotWritable)?;
	if !tenant.state.allows_writes() {
		return Err(TenantNotWritable);
	}
	match claim.key.namespace {
		ParentNamespace::Table(table) => {
			if table == crate::schema::TableObjectId::DEFAULT_KV
				|| state
					.table_schemas
					.latest_by_id(claim.key.tenant, table)
					.is_none() || state.table_drop_blocks_admission(claim.key.tenant, table)
			{
				return Err(TableUnavailable);
			}
		}
		// ADR 0699 S4b: a KV parent writes one key: one child of one row at one
		// position, which is the coordinator child. It never deletes.
		ParentNamespace::Kv => {
			if claim.operation != Operation::Write {
				return Err(MalformedClaim);
			}
			let only = &children[0];
			if children.len() != 1
				|| claim.coordinator != 0
				|| only.rows != 1
				|| only.range_start != only.range_end
				|| only.range_start != claim.first_row_position
			{
				return Err(MalformedClaim);
			}
		}
	}

	// ADR 0699 S4b: a matched delete publishes one tombstone at the row's
	// position, above the version it selected.
	if let Operation::Delete { request, selection } = &claim.operation {
		let DeleteOutcome::Deleted { winner_hlc, .. } = selection.outcome else {
			return Err(MalformedClaim);
		};
		let position = delete_target(state, &claim.key, request)?;
		let only = &children[0];
		if children.len() != 1
			|| claim.coordinator != 0
			|| only.rows != 1
			|| only.range_start != position
			|| only.range_end != position
			|| claim.first_row_position != position
			|| claim.commit_hlc <= winner_hlc
			|| claim.request_digest != delete_request_digest(claim.key.tenant, request)
		{
			return Err(MalformedClaim);
		}
	}

	let (_, config) = state.epoch_history.last_key_value().ok_or(PlanMismatch)?;
	let ring = RingState::build(
		&membership_from_epoch_config(config),
		tenant.replication_factor as usize,
	);
	let segments = ring.replica_segments();
	for child in children {
		let lineage = decisions
			.lineages
			.get(&child.lineage.range_start)
			.ok_or(LineageClosed)?;
		if !lineage_open(state, decisions, child.lineage, child.holder)
			|| child.range_start < child.lineage.range_start
			|| child.range_end > lineage.range_end
		{
			return Err(LineageClosed);
		}
		let mut planned: Vec<u32> = Vec::with_capacity(child.plan.len());
		for member in &child.plan {
			if current_incarnation(state, member.node) != Some(member.incarnation) {
				return Err(PlanMismatch);
			}
			planned.push(member.node);
		}
		if !child.plan.contains(&child.holder) {
			return Err(PlanMismatch);
		}
		planned.sort_unstable();
		let mut intersecting = 0usize;
		for segment in &segments {
			if segment.end < child.range_start || child.range_end < segment.start {
				continue;
			}
			intersecting += 1;
			if segment.replicas.first().map(|node| node.id) != Some(child.holder.node) {
				return Err(PlanMismatch);
			}
			let mut replicas: Vec<u32> = segment.replicas.iter().map(|node| node.id).collect();
			replicas.sort_unstable();
			if replicas != planned {
				return Err(PlanMismatch);
			}
		}
		if intersecting == 0 {
			return Err(PlanMismatch);
		}
		if state.migrations.values().any(|migration| {
			migration.state != crate::consensus::MigrationState::Completed
				&& migration.range_start <= child.range_end
				&& child.range_start <= migration.range_end
		}) {
			return Err(MigrationActive);
		}
	}
	Ok(())
}

/// Moves a reserved parent to its one terminal status, decided at `log_index`.
fn conclude(
	decisions: &mut DecisionState,
	reservation: u64,
	outcome: Option<RefusalCause>,
	log_index: u64,
) -> Result<DecisionResult, DecisionRefusal> {
	if log_index <= reservation {
		return Err(DecisionRefusal::LogIndexOutOfOrder);
	}
	let status = match outcome {
		None => ParentStatus::Accepted { dec: log_index },
		Some(cause) => ParentStatus::Refused {
			cause,
			dec: log_index,
		},
	};
	let body = decisions
		.bodies
		.get_mut(&reservation)
		.expect("a reserved parent has a body");
	debug_assert!(!body.status.is_terminal());
	body.status = status;
	decisions.live -= 1;
	Ok(DecisionResult::Terminal {
		reservation,
		status,
	})
}

fn finalize(
	state: &SmState,
	decisions: &mut DecisionState,
	key: ParentKey,
	request_digest: [u8; 32],
	acks: &[Signed<ChildAckBody>],
	log_index: u64,
) -> Result<DecisionResult, DecisionRefusal> {
	let (reservation, status) =
		lookup(decisions, &key, &request_digest)?.ok_or_else(|| unrecorded(decisions, &key))?;
	if status.is_terminal() {
		return Ok(existing_result(reservation, status));
	}
	let body = &decisions.bodies[&reservation];
	// Closure first: a closed lineage refuses whatever evidence arrived.
	if any_lineage_closed(state, decisions, &body.claim) {
		return conclude(
			decisions,
			reservation,
			Some(RefusalCause::LineageClosed),
			log_index,
		);
	}
	let verified = acks
		.iter()
		.map(|ack| evidence::verify(decisions, ack))
		.collect::<Result<Vec<_>, _>>()?;
	if !acks_reach_quorum(state, &key, reservation, body, &verified)? {
		return Ok(DecisionResult::PendingEvidence);
	}
	conclude(decisions, reservation, None, log_index)
}

/// Whether verified acknowledgements bind this reservation's claim and reach,
/// for every child, a majority of its plan that includes its holder.
fn acks_reach_quorum(
	state: &SmState,
	key: &ParentKey,
	reservation: u64,
	body: &ParentRecord,
	verified: &[evidence::Verified<ChildAckBody>],
) -> Result<bool, DecisionRefusal> {
	let children = &body.claim.children;
	let mut ackers: Vec<Vec<u32>> = vec![Vec::new(); children.len()];
	for ack in verified {
		let ack = ack.body();
		let child = children
			.get(usize::from(ack.ordinal))
			.ok_or(DecisionRefusal::EvidenceMismatch)?;
		let binds = ack.key == *key
			&& ack.reservation == reservation
			&& ack.claim_digest == body.claim_digest
			&& ack.range_start == child.range_start
			&& ack.range_end == child.range_end
			&& ack.lineage == child.lineage
			&& ack.holder_incarnation == child.holder.incarnation
			&& ack.rows == child.rows
			&& ack.content == child.content
			&& child.plan.contains(&ack.acker)
			&& current_incarnation(state, ack.acker.node) == Some(ack.acker.incarnation);
		if !binds {
			return Err(DecisionRefusal::EvidenceMismatch);
		}
		let seen = &mut ackers[usize::from(ack.ordinal)];
		if seen.contains(&ack.acker.node) {
			return Err(DecisionRefusal::EvidenceMismatch);
		}
		seen.push(ack.acker.node);
	}
	Ok(children.iter().zip(&ackers).all(|(child, seen)| {
		seen.contains(&child.holder.node) && seen.len() > child.plan.len() / 2
	}))
}

fn resolve(
	state: &SmState,
	decisions: &mut DecisionState,
	key: ParentKey,
	request_digest: [u8; 32],
	log_index: u64,
) -> Result<DecisionResult, DecisionRefusal> {
	let (reservation, status) =
		lookup(decisions, &key, &request_digest)?.ok_or_else(|| unrecorded(decisions, &key))?;
	if status.is_terminal() {
		return Ok(existing_result(reservation, status));
	}
	if any_lineage_closed(state, decisions, &decisions.bodies[&reservation].claim) {
		return conclude(
			decisions,
			reservation,
			Some(RefusalCause::LineageClosed),
			log_index,
		);
	}
	Ok(DecisionResult::StillOpen)
}

fn abort_parent(
	state: &SmState,
	decisions: &mut DecisionState,
	signed: &Signed<AbortBody>,
	log_index: u64,
) -> Result<DecisionResult, DecisionRefusal> {
	let Some(entry) = decisions.keys.get(&signed.body.key).copied() else {
		return Err(unrecorded(decisions, &signed.body.key));
	};
	let body = &decisions.bodies[&entry.reservation];
	if body.claim_digest != signed.body.claim_digest {
		return Err(DecisionRefusal::Conflict);
	}
	if body.status.is_terminal() {
		return Ok(existing_result(entry.reservation, body.status));
	}
	let verified = evidence::verify(decisions, signed)?;
	let abort = verified.body();
	let coordinator = body
		.claim
		.coordinator_child()
		.expect("a reserved claim names its coordinator");
	if abort.reservation != entry.reservation
		|| abort.coordinator != coordinator.holder
		|| abort.lineage != coordinator.lineage
	{
		return Err(DecisionRefusal::EvidenceMismatch);
	}
	let cause = if any_lineage_closed(state, decisions, &body.claim) {
		RefusalCause::LineageClosed
	} else {
		RefusalCause::Aborted
	};
	conclude(decisions, entry.reservation, Some(cause), log_index)
}

/// Binds an incarnation to its key. Idempotent for the same key; a different
/// key, or a key already bound to another node, is refused.
fn bind_signer(
	state: &SmState,
	decisions: &mut DecisionState,
	signer: Signer,
	key: [u8; 32],
) -> Result<DecisionResult, DecisionRefusal> {
	if ed25519_dalek::VerifyingKey::from_bytes(&key).is_err() {
		return Err(DecisionRefusal::MalformedKey);
	}
	match decisions.signer_key(signer) {
		Some(bound) if bound == key => return Ok(DecisionResult::SignerBound),
		Some(_) => return Err(DecisionRefusal::SignerRebind),
		None => {}
	}
	if current_incarnation(state, signer.node) != Some(signer.incarnation) {
		return Err(DecisionRefusal::SignerNotCurrent);
	}
	if decisions
		.signers
		.iter()
		.any(|((node, _), bound)| *bound == key && *node != signer.node)
	{
		return Err(DecisionRefusal::SignerRebind);
	}
	decisions
		.signers
		.insert((signer.node, signer.incarnation), key);
	Ok(DecisionResult::SignerBound)
}

/// E3: retires terminal retry keys issued at or before the new floor. Bodies,
/// Refused ones included, stay until certified reclamation (S5).
fn advance_floor(
	decisions: &mut DecisionState,
	floor_ms: u64,
	stamp_ms: u64,
) -> Result<DecisionResult, DecisionRefusal> {
	let clock_ms = decisions.clock_ms.max(stamp_ms);
	let limit = clock_ms
		.checked_sub(DECISION_EXPIRY_MS)
		.ok_or(DecisionRefusal::FloorTooRecent)?;
	if floor_ms <= decisions.floor_ms {
		return Err(DecisionRefusal::FloorNotAdvanced);
	}
	if floor_ms > limit {
		return Err(DecisionRefusal::FloorTooRecent);
	}
	decisions.clock_ms = clock_ms;
	decisions.floor_ms = floor_ms;
	let bodies = &decisions.bodies;
	let before = decisions.keys.len();
	// Retires keys only: bodies and noops stay until S5's reclamation.
	decisions.keys.retain(|key, entry| {
		let issued = key.issued_ms().expect("a recorded key is a UUIDv7");
		let terminal = match entry.record {
			RecordKind::Body => bodies[&entry.reservation].status.is_terminal(),
			RecordKind::Noop => true,
		};
		!(terminal && issued <= floor_ms)
	});
	Ok(DecisionResult::FloorAdvanced {
		floor_ms,
		retired_keys: (before - decisions.keys.len()) as u64,
	})
}

/// What an authoritative lookup finds for a key and request digest (ADR 0699
/// S3c). A recorded parent returns its **original** retained claim; an absent
/// key says only whether a new claim may be made for it now.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Lookup {
	/// No record; `eligible` is false when the key can no longer be presented.
	Absent {
		eligible: bool,
	},
	Recorded {
		record: ParentRecord,
	},
	Conflict,
	InvalidIdentity,
	/// ADR 0699 S4b: a recorded RecordNoop.
	Noop {
		record: NoopRecord,
	},
}

/// The authoritative lookup over applied state: no clock and no current
/// authority are consulted, so a retry after either moved still learns its
/// recorded parent.
pub fn lookup_parent(
	decisions: &DecisionState,
	key: &ParentKey,
	request_digest: &[u8; 32],
) -> Lookup {
	match lookup(decisions, key, request_digest) {
		Err(_) => Lookup::Conflict,
		Ok(Some((dec, ParentStatus::Noop { .. }))) => Lookup::Noop {
			record: decisions.noops[&dec].clone(),
		},
		Ok(Some((reservation, _))) => Lookup::Recorded {
			record: decisions.bodies[&reservation].clone(),
		},
		Ok(None) => match unrecorded(decisions, key) {
			DecisionRefusal::InvalidIdentity => Lookup::InvalidIdentity,
			DecisionRefusal::OutcomeUnavailable => Lookup::Absent { eligible: false },
			_ => Lookup::Absent { eligible: true },
		},
	}
}

/// What a retry learns. The digest is compared before any status is returned.
pub fn answer(decisions: &DecisionState, key: &ParentKey, request_digest: &[u8; 32]) -> Answer {
	match lookup(decisions, key, request_digest) {
		Err(_) => Answer::Conflict,
		Ok(Some((_, ParentStatus::Reserved))) => Answer::Reserved,
		Ok(Some((_, ParentStatus::Accepted { .. }))) => Answer::Accepted,
		Ok(Some((_, ParentStatus::Refused { cause, .. }))) => Answer::Refused(cause),
		Ok(Some((_, ParentStatus::Noop { .. }))) => Answer::Noop,
		Ok(None) => match unrecorded(decisions, key) {
			DecisionRefusal::InvalidIdentity => Answer::InvalidIdentity,
			DecisionRefusal::OutcomeUnavailable => Answer::OutcomeUnavailable,
			_ => Answer::New,
		},
	}
}

// ============================================================
//  SNAPSHOT ENVELOPE (format 10)
// ============================================================

pub(crate) const DECISION_SNAPSHOT_ENVELOPE_VERSION: u8 = 1;

#[derive(Serialize)]
pub(crate) struct DecisionSnapshotRef<'a> {
	pub version: u8,
	pub sm: &'a SmState,
	pub decisions: &'a DecisionState,
}

#[derive(Deserialize)]
pub(crate) struct DecisionSnapshot {
	pub version: u8,
	pub sm: SmState,
	pub decisions: DecisionState,
}

#[cfg(test)]
#[path = "decisions_tests.rs"]
pub(crate) mod tests;

pub mod generations;
pub use generations::{Certificate, ChunkSum, Generations, ReportBody, Target};
