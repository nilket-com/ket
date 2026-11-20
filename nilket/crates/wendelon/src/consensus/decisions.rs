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

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::consensus::raft_storage::SmState;
use crate::consensus::{EpochConfig, StoreIncarnation};
use crate::network::ring::{RingState, membership_from_epoch_config};
use crate::network::transport::{NodeRole, NodeRoleSet};
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
/// ADR 0700: how far a new claim's commit HLC may lead the leader's stamp,
/// the decision clock gate's own bound. It bounds admission of future physical
/// timestamps only; it is not a real-time ordering guarantee.
pub(crate) const DECISION_COMMIT_HLC_LEAD_MS: u64 = crate::consensus::monitor::MAX_OFFSET_MS;
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

	/// Wendelon ADR 0113: the operation identity, without its namespace.
	pub fn operation(&self) -> (TenantId, [u8; 16], u64) {
		(self.tenant, self.client_uuid, self.sequence_no)
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
	/// ADR 0729: a version-producing claim under this generation carries a
	/// commit HLC whose physical time is above this. Set when the generation
	/// begins, from the ring clock, so its versions order after every version
	/// reserved before it began.
	pub floor_ms: u64,
	/// ADR 0729: no takeover of this generation is admitted until the ring
	/// clock is past this. Zero unless a takeover began the generation.
	pub protected_until_ms: u64,
	/// ADR 0729: set by a takeover and cleared by the successor's first grant,
	/// which continues this generation without an earlier lease on record.
	pub first_grant_pending: bool,
}

/// ADR 0729: a replica's request to take a range's coordinating authority from
/// a holder whose lease lapsed. Signed by the successor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TakeOverBody {
	pub range_start: u64,
	/// The generation being replaced: consumed by the first event that ends it.
	pub generation: u64,
	pub successor: Signer,
}

/// ADR 0106 (wendelon): a promoted replacement's statement that its store `store`
/// at slot `slot` holds the certified base at `gid` (`target_digest`; none at
/// genesis) and every child its recovery owes through the applied cut `k`.
/// Signed by (slot, store) only after its own stored data was validated.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryBody {
	pub token: u128,
	pub slot: u32,
	pub r_new: u32,
	pub birth: instances::Birth,
	pub store: StoreIncarnation,
	pub gid: u64,
	pub target_digest: Option<[u8; 32]>,
	pub k: u64,
}

/// ADR 0108 (wendelon): a recovered replacement's activation. Store `store`
/// at slot `slot`, recorded as recovered at (`recovered_gid`, `k`), held every
/// child owed to its slot that was applied through `through`, with its active
/// base at certificate `certified`. Signed by (slot, store).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivationBody {
	pub token: u128,
	pub slot: u32,
	pub r_new: u32,
	pub birth: instances::Birth,
	pub store: StoreIncarnation,
	pub recovered_gid: u64,
	pub k: u64,
	pub through: u64,
	pub certified: u64,
}

/// ADR 0108 (wendelon): the secondary-index lifecycle phase a store reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexReceiptPhase {
	/// The store's historical backfill is complete under its own cut.
	Backfilled,
	/// The store has collected the Dropping index's local state.
	DropCollected,
}

/// ADR 0108 (wendelon): a store's receipt that it completed `phase` for the
/// index, signed by the store it names; that store must be its slot's current.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexReceiptBody {
	pub tenant: TenantId,
	pub index: crate::schema::IndexObjectId,
	pub phase: IndexReceiptPhase,
	pub signer: Signer,
}

/// ADR 0108 (wendelon): an index's receipts, one set per phase, so a backfill
/// receipt never counts toward the drop.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexReceipts {
	pub backfilled: BTreeSet<Signer>,
	pub drop_collected: BTreeSet<Signer>,
}

/// ADR 0108 (wendelon): the catalog change a receipt completes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexTransition {
	/// Building becomes Ready.
	Ready,
	/// The Dropping index is removed.
	Removed,
}

/// ADR 0729: how long past a lease's own expiry, on the ring clock, before its
/// range may be taken over. A margin, not a bound: a lease's start is the
/// requester's clock reading, which nothing gates (only its TTL is clamped), so
/// a future-dated lease delays eligibility by however far ahead it was dated.
pub const TAKEOVER_GRACE_MS: u64 = 2 * crate::consensus::monitor::MAX_OFFSET_MS;
/// ADR 0729: a successor's protection when no lease was on record to size it.
pub const TAKEOVER_PROTECT_DEFAULT_MS: u64 = 3_000;
/// ADR 0729: bounds on a successor's protection, whatever the lapsed lease's
/// own duration was.
pub const TAKEOVER_PROTECT_MIN_MS: u64 = 1_000;
pub const TAKEOVER_PROTECT_MAX_MS: u64 = 60_000;

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

/// The most row ids one delete statement names (wendelon ADR 0112). A larger
/// statement is refused whole, never split.
pub const MAX_DELETE_ROWS: usize = 512;

/// One delete statement (wendelon ADR 0112): 1 to [`MAX_DELETE_ROWS`] row ids,
/// in canonical order (strictly increasing identity bytes, so no duplicate).
/// Arrival order is not part of it; the set is.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteRequest {
	pub table: TableObjectId,
	pub schema_generation: u64,
	/// Bounded at decode: a longer sequence is refused before it is allocated.
	#[serde(deserialize_with = "deserialize_delete_rows")]
	pub rows: Vec<Identity>,
}

impl DeleteRequest {
	/// Whether the rows are a canonical statement: 1 to [`MAX_DELETE_ROWS`],
	/// strictly increasing.
	pub fn is_canonical(&self) -> bool {
		(1..=MAX_DELETE_ROWS).contains(&self.rows.len())
			&& self
				.rows
				.windows(2)
				.all(|pair| pair[0].as_bytes() < pair[1].as_bytes())
	}

	/// The tombstone target of one of the statement's rows.
	pub fn target(&self, row: Identity) -> TombstoneTarget {
		TombstoneTarget {
			table: self.table,
			schema_generation: self.schema_generation,
			row,
		}
	}
}

/// Wendelon ADR 0112: a delete statement's rows, at most [`MAX_DELETE_ROWS`] at
/// decode. Canonical order and alignment are the reducer's checks.
fn deserialize_delete_rows<'de, D: serde::Deserializer<'de>>(
	deserializer: D,
) -> Result<Vec<Identity>, D::Error> {
	crate::consensus::core::deserialize_bounded_seq(deserializer, MAX_DELETE_ROWS)
}

/// A selection's outcomes, at most [`MAX_DELETE_ROWS`] at decode.
fn deserialize_delete_outcomes<'de, D: serde::Deserializer<'de>>(
	deserializer: D,
) -> Result<Vec<DeleteOutcome>, D::Error> {
	crate::consensus::core::deserialize_bounded_seq(deserializer, MAX_DELETE_ROWS)
}

/// One row's tombstone: what a delete child publishes per matched row, and what
/// repair and recovery rebuild.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TombstoneTarget {
	pub table: TableObjectId,
	pub schema_generation: u64,
	pub row: Identity,
}

/// The delete's selection at its ONE cut, frozen in the record: one outcome per
/// requested row, aligned with [`DeleteRequest::rows`]. It is the coordinator's
/// attested evaluation: the reducer checks its structure, not the absence or
/// presence it reports.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteSelection {
	pub cut: u64,
	/// Bounded at decode, as the rows are.
	#[serde(deserialize_with = "deserialize_delete_outcomes")]
	pub outcomes: Vec<DeleteOutcome>,
}

impl DeleteSelection {
	/// The rows the statement publishes a tombstone for: those whose winner at
	/// the cut was live, in the request's (identity) order.
	pub fn matched<'a>(
		&'a self,
		request: &'a DeleteRequest,
	) -> impl Iterator<Item = (Identity, DeleteOutcome)> + 'a {
		request
			.rows
			.iter()
			.copied()
			.zip(self.outcomes.iter().copied())
			.filter(|(_, outcome)| matches!(outcome, DeleteOutcome::Deleted { .. }))
	}

	/// (deleted, already deleted, not found).
	pub fn counts(&self) -> [u32; 3] {
		let mut counts = [0u32; 3];
		for outcome in &self.outcomes {
			counts[match outcome {
				DeleteOutcome::Deleted { .. } => 0,
				DeleteOutcome::AlreadyDeleted { .. } => 1,
				DeleteOutcome::NotFound => 2,
			}] += 1;
		}
		counts
	}

	/// The highest HLC among the live winners the statement deletes.
	pub fn max_deleted_winner(&self) -> Option<HybridTimestamp> {
		self.outcomes
			.iter()
			.filter_map(|outcome| match outcome {
				DeleteOutcome::Deleted { winner_hlc, .. } => Some(*winner_hlc),
				_ => None,
			})
			.max()
	}
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

/// A delete statement that published nothing, recorded terminal (RecordNoop).
/// Signed by the coordinator: the holder of the statement's lowest requested
/// position, which is `position`.
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

/// The request digest of a delete statement (wendelon ADR 0112): its count and
/// every row, in canonical order, under its own derivation domain. Equal for any
/// arrival order of one set; different for any other set.
pub fn delete_request_digest(tenant: TenantId, request: &DeleteRequest) -> [u8; 32] {
	let mut hasher = blake3::Hasher::new_derive_key("wendelon delete request v2");
	hasher.update(&tenant.get().to_le_bytes());
	hasher.update(request.table.as_bytes());
	hasher.update(&request.schema_generation.to_le_bytes());
	hasher.update(&(request.rows.len() as u64).to_le_bytes());
	for row in &request.rows {
		hasher.update(row.as_bytes());
	}
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
/// Publicly derivable. Accepted only under `identity.allow_dev_keys`
/// (ADR 0721); a daemon names real decision keys in its configuration. Key
/// rotation and custody beyond a configured secret remain open.
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
	/// ADR 0729: moves a range's coordinating authority to a replica once its
	/// holder's lease lapsed on the ring clock. Signed by the successor.
	TakeOverLineage {
		takeover: Signed<TakeOverBody>,
		stamp_ms: u64,
	},
	/// ADR 0736: enters the backup hold `id`, or returns the current token when
	/// `id` already holds it. Another hold present is a conflict.
	BeginBackupHold {
		id: u128,
	},
	/// ADR 0736: ends the hold whose token is exactly (`id`, `entered_at`).
	ReleaseBackupHold {
		id: u128,
		entered_at: u64,
	},
	/// ADR 0104 (wendelon): the first identity of the Raft store born `birth`,
	/// for `slot`. The leader proposes it only for the slot authenticated on
	/// the connection.
	GrantInstance {
		birth: instances::Birth,
		slot: u32,
		kind: instances::GrantKind,
	},
	/// ADR 0104 (wendelon): opens the replacement `token` of `slot`'s voter.
	BeginReplacement {
		token: u128,
		slot: u32,
	},
	/// ADR 0104 (wendelon): records Done once the applied membership is the
	/// replacement's target; retires the old instance.
	CompleteReplacement {
		token: u128,
	},
	/// ADR 0106 (wendelon): the current promoted replacement's signed recovery
	/// statement, recorded once per token.
	RecoveryProved {
		proof: Signed<RecoveryBody>,
	},
	/// ADR 0108 (wendelon): the recovered replacement's signed activation,
	/// recorded once per token; it ends the slot's preparation.
	ActivateReplacement {
		proof: Signed<ActivationBody>,
	},
	/// ADR 0108 (wendelon): a store's secondary-index receipt.
	IndexReceipt {
		proof: Signed<IndexReceiptBody>,
	},
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RefusalCause {
	LineageClosed,
	Aborted,
	/// Wendelon ADR 0114 stage (a): concluded by its table's drop Prepare while
	/// still Reserved. Production from format 31 (wendelon ADR 0118 D2 is its
	/// compatibility fence); appended, so every earlier tag is unchanged.
	TableDropped,
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
	/// ADR 0700: a new claim's commit HLC is more than
	/// [`DECISION_COMMIT_HLC_LEAD_MS`] ahead of the leader's stamp.
	CommitFromFuture,
	/// ADR 0720: no write has been admitted yet and the cluster is not formed:
	/// the applied Raft membership is joint, has a learner, or its voters are
	/// not the latest epoch's peers. Nothing is admitted and nothing is sealed.
	FormationIncomplete,
	/// ADR 0720: the membership sealed by the first admitted write is not the
	/// one in effect. No new parent is admitted.
	MembershipSealed,
	/// ADR 0729: a version-producing claim whose commit HLC is not above the
	/// floor of a lineage it binds. Retryable with a later commit HLC.
	BelowTenureFloor,
	/// ADR 0729: a takeover naming a lineage that does not exist or a
	/// generation that is no longer the range's.
	TakeoverStale,
	/// ADR 0729: the holder's lease has not lapsed on the ring clock, or the
	/// generation is still protected.
	TakeoverNotLapsed,
	/// ADR 0729: the successor is the holder, not a current data member, or
	/// not a replica of every segment the range spans.
	TakeoverIneligible,
	/// ADR 0729: some tenant's replication factor is below the number of data
	/// members, so a range's replicas are not all data members.
	UnsupportedShape,
	/// ADR 0736: another backup hold is held.
	HoldConflict,
	/// ADR 0736: no hold has exactly this token.
	HoldMismatch,
	/// ADR 0736: a new reservation while a backup hold is held; nothing was
	/// reserved.
	BackupHold,
	/// ADR 0104 (wendelon): grants and replacements.
	SlotOutOfRange,
	SlotServed,
	SlotGranted,
	SlotNotVoter,
	InstanceRetired,
	ReplacementOpen,
	UnknownReplacement,
	GrantIntentMismatch,
	GrantedToAnotherBirth,
	InstanceSpaceExhausted,
	MembershipNotUniform,
	ReplacementIncomplete,
	/// ADR 0105 (wendelon): the slot is between its replacement's Done and its
	/// activation; it takes no new lease, lineage or coordination.
	SlotBeingReplaced,
	/// ADR 0106 (wendelon): the statement is not the current promoted
	/// replacement's (token, slot, instance, birth, store, serving instance).
	RecoveryNotAuthorized,
	/// ADR 0106 (wendelon): it names another certificate than the current one.
	RecoveryStale,
	/// ADR 0106 (wendelon): another statement is already recorded for the token.
	RecoveryConflict,
	/// ADR 0106 (wendelon): its cut is not below the entry that records it.
	RecoveryCutAhead,
	/// ADR 0108 (wendelon): the activation is not the current recovered
	/// replacement's (token, slot, instance, birth, store, recorded recovery,
	/// serving instance).
	ActivationNotAuthorized,
	/// ADR 0108 (wendelon): another activation is already recorded for the token.
	ActivationConflict,
	/// ADR 0108 (wendelon): its cut is below the recovery cut, or not below the
	/// entry that records it.
	ActivationCutOutOfRange,
	/// ADR 0108 (wendelon): its certificate is below the recorded recovery's or
	/// above the current one.
	ActivationCertificateOutOfRange,
	/// ADR 0108 (wendelon): the receipt's store is not its slot's current one.
	IndexReceiptNotCurrent,
	/// ADR 0108 (wendelon): the receipt names no index in the catalog.
	IndexReceiptUnknownIndex,
	/// ADR 0108 (wendelon): the receipt's phase is not the index's: Backfilled
	/// needs Building or Ready, DropCollected needs Dropping.
	IndexReceiptWrongPhase,
	/// ADR 0108 (wendelon): a voter of the applied membership maps to no slot,
	/// so whether every slot's current store reported cannot be proved.
	IndexReceiptMembershipUnknown,
}

/// ADR 0721: how the key a store signs with stands against the replicated
/// binding of its incarnation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LocalSignerBinding {
	/// The store's incarnation is not its node's current one: nothing is
	/// judged about its key.
	NotCurrent,
	/// Current, and no key is bound yet.
	Unbound,
	/// Current, and the bound key is the one the store signs with.
	Bound,
	/// Current, and another key is bound. Everything this store signs is
	/// refused by [`evidence::verify`]; bindings are immutable, so only the
	/// right secret repairs it.
	Mismatch,
}

/// Compares `local_key`, the verifying key of the secret `signer` signs with,
/// with what applied state binds for that exact incarnation.
pub(crate) fn local_signer_binding(
	state: &SmState,
	signer: Signer,
	local_key: [u8; 32],
) -> LocalSignerBinding {
	if current_incarnation(state, signer.node) != Some(signer.incarnation) {
		return LocalSignerBinding::NotCurrent;
	}
	match state
		.decisions
		.as_ref()
		.and_then(|decisions| decisions.signer_key(signer))
	{
		None => LocalSignerBinding::Unbound,
		Some(bound) if bound == local_key => LocalSignerBinding::Bound,
		Some(_) => LocalSignerBinding::Mismatch,
	}
}

/// ADR 0720: what the first admitted write fixed. `placement` is every peer of
/// the serving epoch with whether it holds the Data role, which is all a ring is
/// built from; the voters at that moment were exactly those peers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealedMembership {
	placement: BTreeSet<(u32, bool)>,
}

impl SealedMembership {
	pub fn placement(&self) -> &BTreeSet<(u32, bool)> {
		&self.placement
	}
}

/// What a ring is built from: each peer and whether it holds the Data role.
/// Addresses, tiers, capabilities and the epoch number place nothing.
pub(crate) fn placement_of(config: &EpochConfig) -> BTreeSet<(u32, bool)> {
	config
		.peers
		.values()
		.map(|peer| {
			(
				peer.node_id.id,
				NodeRoleSet::from_u8(peer.role_bits).contains(NodeRole::Data),
			)
		})
		.collect()
}

/// The placement of a formed cluster, or `None` while it is forming: the applied
/// Raft membership must be uniform, hold no learner but a replacement's reserved
/// instance, and have exactly the latest epoch's peers as its voters' slots.
#[cfg(test)]
pub(crate) fn formed_placement(state: &SmState) -> Option<BTreeSet<(u32, bool)>> {
	placement_under(state, state.decisions.as_ref())
}

/// `formed_placement`, with the decision state given: the reducer holds it
/// apart from `state` while it applies a command.
fn placement_under(
	state: &SmState,
	decisions: Option<&DecisionState>,
) -> Option<BTreeSet<(u32, bool)>> {
	let membership = state.last_membership.membership();
	if membership.get_joint_config().len() != 1 {
		return None;
	}
	// ADR 0104 (wendelon): voters are Raft instances; placement compares slots.
	let slot = |instance: u32| match decisions {
		None => Some(instance),
		Some(decisions) => instances::slot_in(decisions, instance).ok(),
	};
	let voters: BTreeSet<u32> = membership.voter_ids().collect();
	let voter_slots: BTreeSet<u32> = voters
		.iter()
		.map(|voter| slot(*voter))
		.collect::<Option<_>>()?;
	if voter_slots.len() != voters.len() {
		return None;
	}
	// The only tolerated learner is the instance an open replacement reserved,
	// on a slot that already votes.
	let reserved = decisions.and_then(|decisions| decisions.instances().reserved_instance());
	for (node, _) in membership.nodes() {
		if !voters.contains(node)
			&& (Some(*node) != reserved || !voter_slots.contains(&slot(*node)?))
		{
			return None;
		}
	}
	let (_, config) = state.epoch_history.last_key_value()?;
	let peers: BTreeSet<u32> = config.peers.values().map(|peer| peer.node_id.id).collect();
	if voters.is_empty() || peers != voter_slots {
		return None;
	}
	Some(placement_of(config))
}

/// ADR 0720: whether an epoch advance may apply. Once a write has been admitted,
/// every configuration the advance would introduce must place exactly as the
/// sealed one does. A configuration already on record is not rewritten by the
/// advance, so it is not judged here.
pub(crate) fn admit_epoch_advance(
	state: &SmState,
	record: &crate::consensus::AdvanceEpochRecord,
) -> Result<(), &'static str> {
	let Some(sealed) = state
		.decisions
		.as_ref()
		.and_then(|decisions| decisions.sealed.as_ref())
	else {
		return Ok(());
	};
	for config in [&record.from, &record.to] {
		if !state.epoch_history.contains_key(&config.epoch)
			&& placement_of(config) != sealed.placement
		{
			return Err(
				"MembershipSealed: an epoch advance may not change placement once a write has been admitted",
			);
		}
	}
	Ok(())
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
	/// ADR 0729: the range's lineage is now the successor's, at `generation`.
	TookOver {
		range_start: u64,
		generation: u64,
	},
	/// ADR 0736: the backup hold's token (now or earlier).
	HoldEntered {
		id: u128,
		entered_at: u64,
	},
	/// ADR 0736: the hold named by its exact token has ended.
	HoldReleased,
	/// ADR 0104 (wendelon): the instance granted to the asking birth.
	InstanceGranted {
		instance: u32,
	},
	/// ADR 0104 (wendelon): the replacement's record (now or earlier).
	ReplacementRecorded {
		slot: u32,
		r_old: u32,
		phase: instances::ReplacementPhase,
	},
	/// ADR 0104 (wendelon): the replacement is Done (now or earlier).
	ReplacementCompleted {
		r_new: u32,
	},
	/// ADR 0106 (wendelon): the replacement's recovery is recorded (now, or
	/// earlier with exactly this statement).
	RecoveryRecorded {
		token: u128,
		gid: u64,
		k: u64,
	},
	/// ADR 0108 (wendelon): the store's index receipt is recorded, and the
	/// catalog change it completes, if any.
	IndexReceiptRecorded {
		tenant: TenantId,
		index: crate::schema::IndexObjectId,
		transition: Option<IndexTransition>,
	},
	/// ADR 0108 (wendelon): the replacement's activation is recorded (now, or
	/// earlier with exactly this statement).
	ActivationRecorded {
		token: u128,
		through: u64,
		certified: u64,
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
	/// ADR 0720: set by the first admitted Reserve, never cleared and never
	/// pruned, so a store whose records have all been retired still knows that
	/// writes began and under which membership.
	sealed: Option<SealedMembership>,
	/// ADR 0736: the backup hold, by its token (`id`, `entered_at`).
	hold: Option<(u128, u64)>,
	/// ADR 0104 (wendelon): granted Raft instances and replacements.
	instances: instances::InstanceState,
	/// ADR 0108 (wendelon): secondary-index receipts by (tenant, index), per
	/// phase, keyed by the reporting store.
	index_receipts: BTreeMap<(TenantId, crate::schema::IndexObjectId), IndexReceipts>,
	/// Wendelon ADR 0113: the table each retained TABLE key's operation identity
	/// (tenant, client UUID, sequence) is recorded under. A table operation identity
	/// is tenant-wide across tables. Derived from `keys`: never serialized, rebuilt
	/// and checked on every decode, updated with every insert and retirement of a
	/// table key. KV keys are not in it.
	#[serde(skip)]
	table_operations: BTreeMap<(TenantId, [u8; 16], u64), crate::schema::TableObjectId>,
	/// Wendelon ADR 0114: the Reserved bodies of each table, by reservation.
	/// Derived from `bodies`: never serialized, rebuilt and checked on every
	/// decode, updated at Reserve and at every terminal transition; an emptied
	/// bucket is removed, so its size is bounded by the live parents.
	#[serde(skip)]
	live_by_table: BTreeMap<(TenantId, crate::schema::TableObjectId), BTreeSet<u64>>,
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
			sealed: None,
			hold: None,
			instances: instances::InstanceState::default(),
			index_receipts: BTreeMap::new(),
			table_operations: BTreeMap::new(),
			live_by_table: BTreeMap::new(),
		}
	}
}

impl DecisionState {
	/// ADR 0736: the backup hold's token, if one is held.
	pub fn backup_hold(&self) -> Option<(u128, u64)> {
		self.hold
	}

	/// ADR 0104 (wendelon): granted Raft instances and replacements.
	pub fn instances(&self) -> &instances::InstanceState {
		&self.instances
	}

	/// ADR 0108 (wendelon): whether `store` holds its `phase` receipt for the
	/// index.
	pub fn holds_index_receipt(
		&self,
		tenant: TenantId,
		index: crate::schema::IndexObjectId,
		phase: IndexReceiptPhase,
		store: Signer,
	) -> bool {
		self.index_receipts
			.get(&(tenant, index))
			.is_some_and(|receipts| match phase {
				IndexReceiptPhase::Backfilled => receipts.backfilled.contains(&store),
				IndexReceiptPhase::DropCollected => receipts.drop_collected.contains(&store),
			})
	}

	/// ADR 0108 (wendelon): an index entering Dropping keeps none of its
	/// backfill receipts, as the shared catalog clears its own set.
	pub(crate) fn index_entered_dropping(
		&mut self,
		tenant: TenantId,
		index: crate::schema::IndexObjectId,
	) {
		if let Some(receipts) = self.index_receipts.get_mut(&(tenant, index)) {
			receipts.backfilled.clear();
		}
	}

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
	pub fn sealed(&self) -> Option<&SealedMembership> {
		self.sealed.as_ref()
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
	/// Wendelon ADR 0113: rebuilds the table-operation index from the retained keys.
	/// Two retained table keys with one operation identity under different tables
	/// refuse: a state that holds them breaks the tenant-wide identity.
	/// Rebuilds the derived indexes after a decode: ADR 0113's table operations
	/// and ADR 0114's Reserved bodies by table.
	pub(crate) fn restore_table_operations(&mut self) -> Result<(), String> {
		self.table_operations = derive_table_operations(&self.keys)?;
		self.live_by_table = derive_live_by_table(&self.bodies);
		Ok(())
	}

	/// ADR 0114: notes a Reserved table body.
	fn note_live(&mut self, key: &ParentKey, reservation: u64) {
		if let ParentNamespace::Table(table) = key.namespace {
			self.live_by_table
				.entry((key.tenant, table))
				.or_default()
				.insert(reservation);
		}
	}

	/// ADR 0114: forgets a body that left Reserved; an emptied bucket is removed.
	fn forget_live(&mut self, reservation: u64) {
		let Some(body) = self.bodies.get(&reservation) else {
			return;
		};
		if let ParentNamespace::Table(table) = body.claim.key.namespace {
			let bucket = (body.claim.key.tenant, table);
			if let Some(live) = self.live_by_table.get_mut(&bucket) {
				live.remove(&reservation);
				if live.is_empty() {
					self.live_by_table.remove(&bucket);
				}
			}
		}
	}

	/// ADR 0114 stage (a): concludes every Reserved parent of exactly `table`
	/// within `tenant` as `Refused { TableDropped }` at `log_index`, visiting
	/// only that table's live bucket. All premises are checked before any change:
	/// a reservation at or after `log_index` refuses the whole batch, unchanged.
	/// Terminal parents, other tables, KV and keys are untouched.
	pub(crate) fn conclude_reserved_for_drop(
		&mut self,
		tenant: TenantId,
		table: crate::schema::TableObjectId,
		log_index: u64,
	) -> Result<Vec<u64>, DecisionRefusal> {
		let targets: Vec<u64> = self
			.live_by_table
			.get(&(tenant, table))
			.map(|live| live.iter().copied().collect())
			.unwrap_or_default();
		if targets.iter().any(|rsv| log_index <= *rsv) {
			return Err(DecisionRefusal::LogIndexOutOfOrder);
		}
		for rsv in &targets {
			conclude(self, *rsv, Some(RefusalCause::TableDropped), log_index)
				.expect("every premise was checked before the batch");
		}
		Ok(targets)
	}

	/// ADR 0114: the Reserved reservations of one table (the ParentDecisions
	/// drop Prepare reads them; wendelon ADR 0118 D1).
	pub(crate) fn live_of_table(
		&self,
		tenant: TenantId,
		table: crate::schema::TableObjectId,
	) -> Vec<u64> {
		self.live_by_table
			.get(&(tenant, table))
			.map(|live| live.iter().copied().collect())
			.unwrap_or_default()
	}

	/// The table a retained table operation identity is recorded under.
	pub fn table_of_operation(
		&self,
		operation: (TenantId, [u8; 16], u64),
	) -> Option<crate::schema::TableObjectId> {
		self.table_operations.get(&operation).copied()
	}

	/// Records a newly retained key's table operation identity.
	fn note_table_operation(&mut self, key: &ParentKey) {
		if let ParentNamespace::Table(table) = key.namespace {
			self.table_operations.insert(key.operation(), table);
		}
	}

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
		self.instances
			.validate()
			.map_err(|error| format!("decisions: instances: {error}"))?;
		// ADR 0720: a record exists only after an admitted write, and that write
		// sealed. A state holding records and no seal was not written by this build.
		match &self.sealed {
			None if !self.bodies.is_empty() => {
				return Err("decisions: parent records exist and no membership is sealed".into());
			}
			Some(sealed) if sealed.placement.is_empty() => {
				return Err("decisions: the sealed membership is empty".into());
			}
			Some(sealed) => {
				let mut nodes = std::collections::BTreeSet::new();
				if !sealed.placement.iter().all(|(node, _)| nodes.insert(*node)) {
					return Err("decisions: the sealed membership names a node twice".into());
				}
			}
			None => {}
		}
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
				|| !noop.body.request.is_canonical()
				|| noop.body.selection.outcomes.len() != noop.body.request.rows.len()
				|| noop
					.body
					.selection
					.outcomes
					.iter()
					.any(|outcome| matches!(outcome, DeleteOutcome::Deleted { .. }))
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
		// Wendelon ADR 0113, last: the table-operation index is exactly what the keys
		// imply (a key-level fault is reported by its own check above first).
		if derive_table_operations(&self.keys)? != self.table_operations {
			return Err("decisions: the table-operation index does not match the keys".into());
		}
		if derive_live_by_table(&self.bodies) != self.live_by_table {
			return Err("decisions: the live-by-table index does not match the bodies".into());
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
	/// ADR 0729: a successor's first grant after its takeover. The generation
	/// the takeover installed continues.
	FirstGrant,
	NewLineage {
		generation: u64,
		incarnation: StoreIncarnation,
		/// ADR 0729: the ring clock this entry sets, and the floor it installs.
		clock_ms: u64,
		floor_ms: u64,
	},
}

/// ADR 0729: the data members of a configuration.
pub(crate) fn data_members(config: &EpochConfig) -> BTreeSet<u32> {
	placement_of(config)
		.into_iter()
		.filter_map(|(node, data)| data.then_some(node))
		.collect()
}

/// ADR 0729: whether any lineage is held by a node other than its range's
/// ring primary.
pub(crate) fn any_lineage_off_primary(state: &SmState) -> bool {
	let Some(decisions) = state.decisions.as_ref() else {
		return false;
	};
	let Some((_, config)) = state.epoch_history.last_key_value() else {
		return false;
	};
	let ring = RingState::build(&membership_from_epoch_config(config), 1);
	decisions.lineages.iter().any(|(start, lineage)| {
		!ring
			.primary_ranges_for(lineage.holder)
			.contains(&(*start, lineage.range_end))
	})
}

/// Admission of a lease grant under this mode, before today's overlap rule
/// runs. `Err` means `LeaseDenied` with nothing changed.
///
/// ADR 0729: a grant names the generation it expects (`None`: no lineage) and
/// is one of a renewal, a successor's first grant, a regrant after a gap (a
/// new generation for the same holder), or the ring primary's first grant
/// where no lineage exists. `stamp_ms` is the batch's replicated leader stamp;
/// it is used only where a generation begins.
///
/// ADR 0108 (wendelon): the holder's incarnation is the one its request
/// carries, and it must be the slot's current one; a lingering old process of
/// a replaced slot, carrying its own, is denied.
pub(crate) fn admit_lease_grant(
	state: &SmState,
	lease: &LeaseDescriptor,
	expected_generation: Option<u64>,
	holder_incarnation: StoreIncarnation,
	stamp_ms: u64,
) -> Result<GrantEffect, ()> {
	let decisions = state.decisions.as_ref().ok_or(())?;
	let (_, config) = state.epoch_history.last_key_value().ok_or(())?;
	if lease.epoch != config.epoch || lease.lease_start >= lease.lease_expiry {
		return Err(());
	}
	if current_incarnation(state, lease.holder.id) != Some(holder_incarnation) {
		return Err(());
	}
	let incarnation = holder_incarnation;
	match decisions.lineages.get(&lease.range_start) {
		Some(lineage) => {
			if lineage.range_end != lease.range_end
				|| lineage.holder != lease.holder
				|| lineage.epoch != lease.epoch
				|| lineage.incarnation != incarnation
				|| expected_generation != Some(lineage.generation)
			{
				return Err(());
			}
			let continuing = state.leases.get(&lease.range_start).filter(|existing| {
				existing.range_end == lease.range_end
					&& existing.holder == lease.holder
					&& existing.epoch == lease.epoch
					&& existing.lease_expiry > lease.lease_start
			});
			if let Some(existing) = continuing {
				// A renewal never shortens the grant it continues.
				if lease.lease_expiry < existing.lease_expiry {
					return Err(());
				}
				return Ok(GrantEffect::Renewal);
			}
			if lineage.first_grant_pending {
				return Ok(GrantEffect::FirstGrant);
			}
			// A regrant after a gap: a new generation for the same holder, so
			// what it left reserved under the old one closes.
		}
		None => {
			if expected_generation.is_some() {
				return Err(());
			}
			// The range universe: exactly one of the holder's merged primary
			// ranges in the current ring. The primary does not depend on the
			// replication factor.
			let ring = RingState::build(&membership_from_epoch_config(config), 1);
			if !ring
				.primary_ranges_for(lease.holder)
				.contains(&(lease.range_start, lease.range_end))
			{
				return Err(());
			}
		}
	}
	let generation = decisions.next_generation;
	generation.checked_add(1).ok_or(())?;
	let clock_ms = decisions.clock_ms.max(stamp_ms);
	let floor_ms = clock_ms
		.checked_add(DECISION_COMMIT_HLC_LEAD_MS)
		.ok_or(())?;
	Ok(GrantEffect::NewLineage {
		generation,
		incarnation,
		clock_ms,
		floor_ms,
	})
}

/// Records an accepted grant. Called only after today's arm inserted the lease.
pub(crate) fn record_grant(state: &mut SmState, lease: &LeaseDescriptor, effect: GrantEffect) {
	let Some(decisions) = state.decisions.as_mut() else {
		return;
	};
	match effect {
		GrantEffect::Renewal => {}
		GrantEffect::FirstGrant => {
			if let Some(lineage) = decisions.lineages.get_mut(&lease.range_start) {
				lineage.first_grant_pending = false;
			}
		}
		GrantEffect::NewLineage {
			generation,
			incarnation,
			clock_ms,
			floor_ms,
		} => {
			decisions.next_generation = generation + 1;
			decisions.clock_ms = clock_ms;
			decisions.lineages.insert(
				lease.range_start,
				Lineage {
					range_end: lease.range_end,
					epoch: lease.epoch,
					holder: lease.holder,
					incarnation,
					generation,
					floor_ms,
					protected_until_ms: 0,
					first_grant_pending: false,
				},
			);
		}
	}
}

/// ADR 0729: the ranges `node` should now propose to take over, as its own
/// applied state and clock see them: held by another node, lapsed, and past
/// this node's place in the order of the range's replicas after the holder
/// (`stagger_ms` per place). A proposal policy only: it can delay this node, it
/// cannot veto it, and the reducer alone admits a takeover. Bounded by `limit`.
pub(crate) fn takeover_candidates(
	state: &SmState,
	node: u32,
	now_ms: u64,
	stagger_ms: u64,
	limit: usize,
) -> Vec<(u64, u64)> {
	let Some(decisions) = state.decisions.as_ref() else {
		return Vec::new();
	};
	let Some((_, config)) = state.epoch_history.last_key_value() else {
		return Vec::new();
	};
	let data = data_members(config);
	if decisions.sealed.is_none() || !data.contains(&node) {
		return Vec::new();
	}
	let ring = RingState::build(&membership_from_epoch_config(config), data.len());
	let segments = ring.replica_segments();
	decisions
		.lineages
		.iter()
		.filter(|(_, lineage)| lineage.holder.id != node && lineage.epoch == config.epoch)
		.filter_map(|(start, lineage)| {
			let segment = segments
				.iter()
				.find(|segment| segment.start <= lineage.range_end && *start <= segment.end)?;
			let place = segment
				.replicas
				.iter()
				.filter(|replica| replica.id != lineage.holder.id)
				.position(|replica| replica.id == node)? as u64;
			let lapsed_after = match state.leases.get(start) {
				Some(lease) => lease
					.lease_expiry
					.physical_ms
					.checked_add(TAKEOVER_GRACE_MS)?
					.max(lineage.protected_until_ms),
				None => lineage.protected_until_ms,
			};
			let due = lapsed_after.checked_add(place.checked_mul(stagger_ms)?)?;
			(now_ms > due).then_some((*start, lineage.generation))
		})
		.take(limit)
		.collect()
}

/// ADR 0729: moves a range's lineage to `successor` at a new generation.
fn take_over(
	state: &SmState,
	decisions: &mut DecisionState,
	signed: &Signed<TakeOverBody>,
	stamp_ms: u64,
) -> Result<DecisionResult, DecisionRefusal> {
	use DecisionRefusal::*;
	let verified = evidence::verify(decisions, signed)?;
	let body = *verified.body();
	// ADR 0105 (wendelon): a slot being replaced takes no lineage.
	if decisions.instances.preparing(body.successor.node) {
		return Err(SlotBeingReplaced);
	}
	let lineage = *decisions
		.lineages
		.get(&body.range_start)
		.ok_or(TakeoverStale)?;
	let (_, config) = state.epoch_history.last_key_value().ok_or(TakeoverStale)?;
	if lineage.generation != body.generation || lineage.epoch != config.epoch {
		return Err(TakeoverStale);
	}
	if lineage.holder.id == body.successor.node {
		return Err(TakeoverIneligible);
	}
	// The sealed placement, in effect: a takeover moves authority, never data.
	let sealed = decisions.sealed.as_ref().ok_or(FormationIncomplete)?;
	if placement_under(state, Some(&*decisions)).as_ref() != Some(&sealed.placement) {
		return Err(MembershipSealed);
	}
	if state.migrations.values().any(|migration| {
		migration.state != crate::consensus::MigrationState::Completed
			&& migration.range_start <= lineage.range_end
			&& body.range_start <= migration.range_end
	}) {
		return Err(MigrationActive);
	}
	if current_incarnation(state, body.successor.node) != Some(body.successor.incarnation) {
		return Err(SignerNotCurrent);
	}
	// The supported shape: every segment's replicas are all the data members.
	let data = data_members(config);
	if !data.contains(&body.successor.node) {
		return Err(TakeoverIneligible);
	}
	if state
		.tenants
		.values()
		.any(|tenant| (tenant.replication_factor as usize) < data.len())
	{
		return Err(UnsupportedShape);
	}
	let ring = RingState::build(&membership_from_epoch_config(config), data.len());
	let mut intersecting = 0usize;
	for segment in ring.replica_segments() {
		if segment.end < body.range_start || lineage.range_end < segment.start {
			continue;
		}
		intersecting += 1;
		if !segment
			.replicas
			.iter()
			.any(|node| node.id == body.successor.node)
		{
			return Err(TakeoverIneligible);
		}
	}
	if intersecting == 0 {
		return Err(TakeoverIneligible);
	}
	// The lapse, on the ring's own clock. It is the trigger; the fence is the
	// generation this entry closes.
	let now = decisions.clock_ms.max(stamp_ms);
	if now <= lineage.protected_until_ms {
		return Err(TakeoverNotLapsed);
	}
	let lease = state.leases.get(&body.range_start);
	if let Some(lease) = lease {
		let lapsed_after = lease
			.lease_expiry
			.physical_ms
			.checked_add(TAKEOVER_GRACE_MS)
			.ok_or(CounterOverflow)?;
		if lapsed_after >= now {
			return Err(TakeoverNotLapsed);
		}
	}
	let protect_ms = lease
		.map(|lease| {
			lease
				.lease_expiry
				.physical_ms
				.saturating_sub(lease.lease_start.physical_ms)
		})
		.unwrap_or(TAKEOVER_PROTECT_DEFAULT_MS)
		.clamp(TAKEOVER_PROTECT_MIN_MS, TAKEOVER_PROTECT_MAX_MS);
	let floor_ms = now
		.checked_add(DECISION_COMMIT_HLC_LEAD_MS)
		.ok_or(CounterOverflow)?;
	let protected_until_ms = now.checked_add(protect_ms).ok_or(CounterOverflow)?;
	let generation = decisions.next_generation;
	let next_generation = generation.checked_add(1).ok_or(CounterOverflow)?;
	// Every check passed: commit.
	decisions.clock_ms = now;
	decisions.next_generation = next_generation;
	decisions.lineages.insert(
		body.range_start,
		Lineage {
			range_end: lineage.range_end,
			epoch: lineage.epoch,
			holder: NodeId {
				id: body.successor.node,
				_reserved: 0,
			},
			incarnation: body.successor.incarnation,
			generation,
			floor_ms,
			protected_until_ms,
			first_grant_pending: true,
		},
	);
	Ok(DecisionResult::TookOver {
		range_start: body.range_start,
		generation,
	})
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

/// ADR 0105 (wendelon): a Done replacement's new store becomes its slot's
/// current incarnation. `Ok(Some(registry))` is a first promotion's new
/// registry; `Ok(None)` the recorded promotion's exact retry, which changes
/// nothing and is a historical answer only. Every refusal changes nothing.
pub(crate) fn promote_incarnation(
	state: &mut SmState,
	request: crate::consensus::PromoteIncarnation,
) -> Result<Option<crate::consensus::IncarnationRegistry>, crate::consensus::SlabRefusal> {
	use crate::consensus::SlabRefusal;
	let Some(decisions) = state.decisions.as_ref() else {
		return Err(SlabRefusal::ReplacementNotDone);
	};
	if decisions.instances.promotion_authority(&request)?.is_some() {
		return Ok(None);
	}
	// A first promotion needs the replacement's instance to serve its slot now:
	// not retired, a voter in every configuration of the applied membership
	// (the uniform swap away from it commits before the Done that retires it),
	// and not the old instance of an open replacement.
	if !instance_serves(state, decisions, request.r_new) {
		return Err(SlabRefusal::PromotionStale);
	}
	let node = request.node;
	let registry = state.incarnations.get(&node).cloned().unwrap_or_default();
	if registry.current != Some(request.expected) {
		return Err(SlabRefusal::StaleSupersede {
			node,
			current: registry.current,
		});
	}
	if request.incarnation == request.expected
		|| registry.retired.contains(&request.incarnation)
		|| registry
			.accepted
			.iter()
			.any(|(incarnation, _)| *incarnation == request.incarnation)
	{
		return Err(SlabRefusal::RetiredIncarnation {
			node,
			incarnation: request.incarnation,
		});
	}
	// P1: takeover moves the old store's lineages first; nothing is pruned here.
	let held = decisions
		.lineages
		.values()
		.filter(|lineage| lineage.holder.id == node && lineage.incarnation == request.expected)
		.count();
	if held > 0 {
		return Err(SlabRefusal::LineagesStillHeld {
			count: u32::try_from(held).unwrap_or(u32::MAX),
		});
	}
	let registry = state.incarnations.entry(node).or_default();
	registry.current = Some(request.incarnation);
	registry.retired.insert(request.expected);
	registry
		.accepted
		.insert((request.incarnation, Some(request.expected)));
	let registry = registry.clone();
	state
		.decisions
		.as_mut()
		.expect("checked above")
		.instances
		.record_promotion(
			request.token,
			instances::Promotion {
				expected: request.expected,
				incarnation: request.incarnation,
			},
		);
	Ok(Some(registry))
}

/// ADR 0106 (wendelon): whether `ancestor` is `store` itself or one of the
/// stores `store` superseded at `slot`, by the recorded promotion links
/// `(successor, Some(predecessor))` in the slot's registry. Finite and acyclic:
/// an incarnation is never accepted twice. It authorizes copying a child that
/// names an ancestor, never acknowledging or counting for it.
pub(crate) fn in_recorded_ancestry(
	state: &SmState,
	slot: u32,
	store: StoreIncarnation,
	ancestor: StoreIncarnation,
) -> bool {
	let Some(registry) = state.incarnations.get(&slot) else {
		return store == ancestor;
	};
	let mut at = store;
	// Bounded by the registry: each step follows one recorded link, and no
	// incarnation is accepted twice.
	for _ in 0..=registry.accepted.len() {
		if at == ancestor {
			return true;
		}
		let Some(previous) = registry
			.accepted
			.iter()
			.find_map(|(successor, predecessor)| (*successor == at).then_some(*predecessor))
			.flatten()
		else {
			return false;
		};
		at = previous;
	}
	false
}

/// ADR 0106 (wendelon): whether (`token`, `r_new`, `birth`, `store`) is slot
/// `slot`'s CURRENT promoted replacement: Done with `r_new` granted to `birth`
/// under `token`, promoted to `store`, `r_new` still serving and `store` the
/// slot's current incarnation. An old token, instance or store is not.
pub(crate) fn current_replacement(
	state: &SmState,
	slot: u32,
	token: u128,
	r_new: u32,
	birth: instances::Birth,
	store: StoreIncarnation,
) -> bool {
	let Some(decisions) = state.decisions.as_ref() else {
		return false;
	};
	let Some(replacement) = decisions.instances.replacement(token) else {
		return false;
	};
	replacement.slot == slot
		&& replacement.phase == instances::ReplacementPhase::Done { r_new }
		&& replacement.promoted.map(|promotion| promotion.incarnation) == Some(store)
		&& decisions.instances.record(r_new).is_some_and(|record| {
			record.birth == birth && record.kind == instances::GrantKind::Replacement { token }
		}) && instance_serves(state, decisions, r_new)
		&& current_incarnation(state, slot) == Some(store)
}

/// ADR 0105/0106 (wendelon): `r_new` serves its slot now: not retired, a voter
/// in every configuration of the applied membership (the uniform swap away from
/// it commits before the Done that retires it), and not the old instance of an
/// open replacement.
fn instance_serves(state: &SmState, decisions: &DecisionState, r_new: u32) -> bool {
	!decisions.instances.is_retired(r_new)
		&& state
			.last_membership
			.membership()
			.get_joint_config()
			.iter()
			.all(|config| config.contains(&r_new))
		&& decisions
			.instances
			.open_replacement()
			.is_none_or(|(_, open)| open.r_old != r_new)
}

/// ADR 0106 (wendelon): records a promoted replacement's signed recovery
/// statement. The reducer judges fields and authority, never bytes: the
/// signer states it only after validating its own stored data. An exact
/// recorded statement answers its historical success first (certification may
/// have advanced since); a first one needs the current promoted replacement,
/// the current certificate and a cut below this entry.
fn record_recovery(
	state: &SmState,
	decisions: &mut DecisionState,
	proof: &Signed<RecoveryBody>,
	log_index: u64,
) -> Result<DecisionResult, DecisionRefusal> {
	use DecisionRefusal::*;
	let body = *evidence::verify(decisions, proof)?.body();
	let replacement = decisions
		.instances
		.replacement(body.token)
		.ok_or(RecoveryNotAuthorized)?;
	let instances::ReplacementPhase::Done { r_new } = replacement.phase else {
		return Err(RecoveryNotAuthorized);
	};
	let granted = decisions
		.instances
		.record(r_new)
		.map(|record| (record.birth, record.kind));
	if replacement.slot != body.slot
		|| r_new != body.r_new
		|| replacement.promoted.map(|promotion| promotion.incarnation) != Some(body.store)
		|| granted
			!= Some((
				body.birth,
				instances::GrantKind::Replacement { token: body.token },
			)) {
		return Err(RecoveryNotAuthorized);
	}
	let statement = instances::Recovery {
		gid: body.gid,
		target_digest: body.target_digest,
		k: body.k,
	};
	let recorded = DecisionResult::RecoveryRecorded {
		token: body.token,
		gid: body.gid,
		k: body.k,
	};
	match replacement.recovered {
		Some(existing) if existing == statement => return Ok(recorded),
		Some(_) => return Err(RecoveryConflict),
		None => {}
	}
	if !instance_serves(state, decisions, r_new)
		|| current_incarnation(state, body.slot) != Some(body.store)
	{
		return Err(RecoveryNotAuthorized);
	}
	let current = match &decisions.generations.current {
		None => (0, None),
		Some(certificate) => (certificate.gid, Some(certificate.target_digest)),
	};
	if (body.gid, body.target_digest) != current {
		return Err(RecoveryStale);
	}
	if body.k >= log_index {
		return Err(RecoveryCutAhead);
	}
	decisions.instances.record_recovery(body.token, statement);
	Ok(recorded)
}

/// ADR 0108 (wendelon): records a recovered replacement's signed activation.
/// As for the recovery statement, the reducer judges fields and authority:
/// the signer states it only after holding every owed child through its cut.
/// An exact recorded activation answers its historical success first; a first
/// one needs the current recovered replacement, a cut from its recovery cut to
/// below this entry, and a certificate from its recovery's to the current one.
fn record_activation(
	state: &SmState,
	decisions: &mut DecisionState,
	proof: &Signed<ActivationBody>,
	log_index: u64,
) -> Result<DecisionResult, DecisionRefusal> {
	use DecisionRefusal::*;
	let body = *evidence::verify(decisions, proof)?.body();
	let replacement = decisions
		.instances
		.replacement(body.token)
		.ok_or(ActivationNotAuthorized)?;
	let instances::ReplacementPhase::Done { r_new } = replacement.phase else {
		return Err(ActivationNotAuthorized);
	};
	let granted = decisions
		.instances
		.record(r_new)
		.map(|record| (record.birth, record.kind));
	if replacement.slot != body.slot
		|| r_new != body.r_new
		|| replacement.promoted.map(|promotion| promotion.incarnation) != Some(body.store)
		|| granted
			!= Some((
				body.birth,
				instances::GrantKind::Replacement { token: body.token },
			)) {
		return Err(ActivationNotAuthorized);
	}
	// The recovery the activation names is immutable once recorded: an exact
	// retry must name it too, before any historical answer (Codex 01a1e41e R1).
	let Some(recovery) = replacement.recovered else {
		return Err(ActivationNotAuthorized);
	};
	if (recovery.gid, recovery.k) != (body.recovered_gid, body.k) {
		return Err(ActivationNotAuthorized);
	}
	let statement = instances::Activation {
		incarnation: body.store,
		through: body.through,
		certified: body.certified,
	};
	let recorded = DecisionResult::ActivationRecorded {
		token: body.token,
		through: body.through,
		certified: body.certified,
	};
	match replacement.activated {
		Some(existing) if existing == statement => return Ok(recorded),
		Some(_) => return Err(ActivationConflict),
		None => {}
	}
	if !instance_serves(state, decisions, r_new)
		|| current_incarnation(state, body.slot) != Some(body.store)
	{
		return Err(ActivationNotAuthorized);
	}
	if body.through < body.k || body.through >= log_index {
		return Err(ActivationCutOutOfRange);
	}
	let current = decisions
		.generations
		.current
		.as_ref()
		.map_or(0, |certificate| certificate.gid);
	if body.certified < recovery.gid || body.certified > current {
		return Err(ActivationCertificateOutOfRange);
	}
	decisions.instances.record_activation(body.token, statement);
	Ok(recorded)
}

/// ADR 0108 (wendelon): records a store's index receipt in its phase's set. The
/// phase must be the index's, and its store its slot's current one. A
/// transition counts only its own phase, at every voter slot's current store:
/// Building becomes Ready on all backfill receipts; a Dropping index is removed,
/// with its receipts, on all drop receipts. `apply` changes the catalog.
fn record_index_receipt(
	state: &SmState,
	decisions: &mut DecisionState,
	proof: &Signed<IndexReceiptBody>,
) -> Result<DecisionResult, DecisionRefusal> {
	use crate::schema::SecondaryIndexState::*;
	use DecisionRefusal::*;
	let body = *evidence::verify(decisions, proof)?.body();
	if current_incarnation(state, body.signer.node) != Some(body.signer.incarnation) {
		return Err(IndexReceiptNotCurrent);
	}
	let row = state
		.secondary_indexes
		.get(body.tenant, body.index)
		.ok_or(IndexReceiptUnknownIndex)?;
	match (body.phase, row.state) {
		(IndexReceiptPhase::Backfilled, Building | Ready)
		| (IndexReceiptPhase::DropCollected, Dropping) => {}
		_ => return Err(IndexReceiptWrongPhase),
	}
	// The decisions are out of `state` while reducing: slots come from them.
	// Every voter must map to a slot; an unknown one makes completeness
	// unprovable, refused before anything is recorded (Codex 01a1e41e R2).
	let Some(slots) = state
		.last_membership
		.voter_ids()
		.map(|voter| instances::slot_in(decisions, voter).ok())
		.collect::<Option<Vec<u32>>>()
	else {
		return Err(IndexReceiptMembershipUnknown);
	};
	let key = (body.tenant, body.index);
	let receipts = decisions.index_receipts.entry(key).or_default();
	let set = match body.phase {
		IndexReceiptPhase::Backfilled => &mut receipts.backfilled,
		IndexReceiptPhase::DropCollected => &mut receipts.drop_collected,
	};
	set.insert(body.signer);
	let complete = !slots.is_empty()
		&& slots.iter().all(|&slot| {
			current_incarnation(state, slot).is_some_and(|incarnation| {
				set.contains(&Signer {
					node: slot,
					incarnation,
				})
			})
		});
	let transition = match body.phase {
		IndexReceiptPhase::Backfilled if complete && row.state == Building => {
			Some(IndexTransition::Ready)
		}
		IndexReceiptPhase::DropCollected if complete => {
			decisions.index_receipts.remove(&key);
			Some(IndexTransition::Removed)
		}
		_ => None,
	};
	Ok(DecisionResult::IndexReceiptRecorded {
		tenant: body.tenant,
		index: body.index,
		transition,
	})
}

/// ADR 0108 (wendelon) D3: whether a replacement's store may start Full, from
/// CURRENT applied state, never from a record somewhere in history: its
/// instance is the r_new of a Done replacement of its slot whose activation and
/// promotion name `store`; `store` is its slot's current incarnation and bound;
/// the instance serves (not retired, a voter in every configuration, not the
/// old instance of an open replacement); its grant's birth is the page's. Any
/// supersession of the instance or the store refuses. The reducer's answer to
/// an exact activation retry is a separate, historical matter.
pub(crate) fn replacement_start_authority(
	state: &SmState,
	identity: instances::RaftIdentity,
	page_birth: instances::Birth,
	store: StoreIncarnation,
) -> Result<(), String> {
	let refuse = |why: &str| Err(format!("ReplacementNotCurrent: {why}"));
	let Some(decisions) = state.decisions.as_ref() else {
		return refuse("no decision state");
	};
	let Some(record) = decisions.instances.record(identity.instance) else {
		return refuse("the instance was never granted");
	};
	let instances::GrantKind::Replacement { token } = record.kind else {
		return refuse("the instance was not granted for a replacement");
	};
	if record.birth != page_birth {
		return refuse("the grant's birth is not this store's");
	}
	let Some(replacement) = decisions.instances.replacement(token) else {
		return refuse("no replacement under the grant's token");
	};
	if replacement.phase
		!= (instances::ReplacementPhase::Done {
			r_new: identity.instance,
		}) {
		return refuse("the replacement is not Done for this instance");
	}
	if replacement
		.activated
		.map(|activation| activation.incarnation)
		!= Some(store)
		|| replacement.promoted.map(|promotion| promotion.incarnation) != Some(store)
	{
		return refuse("no activation of this store is recorded");
	}
	if current_incarnation(state, replacement.slot) != Some(store) {
		return refuse("this store is no longer its slot's current one");
	}
	if !decisions.signers.contains_key(&(replacement.slot, store)) {
		return refuse("this store's signer is not bound");
	}
	if !instance_serves(state, decisions, identity.instance) {
		return refuse("the instance no longer serves its slot");
	}
	Ok(())
}

/// ADR 0105 (wendelon): whether `slot` is between its replacement's Done and
/// its activation, when it takes no new lease, lineage or coordination.
pub(crate) fn slot_preparing(state: &SmState, slot: u32) -> bool {
	state
		.decisions
		.as_ref()
		.is_some_and(|decisions| decisions.instances.preparing(slot))
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

	impl Evidence for super::RecoveryBody {
		const DOMAIN: &'static str = "wendelon replacement recovery v1";
		fn bound_signer(&self) -> Option<Signer> {
			Some(Signer {
				node: self.slot,
				incarnation: self.store,
			})
		}
	}

	impl Evidence for super::ActivationBody {
		const DOMAIN: &'static str = "wendelon replacement activation v1";
		fn bound_signer(&self) -> Option<Signer> {
			Some(Signer {
				node: self.slot,
				incarnation: self.store,
			})
		}
	}

	impl Evidence for super::IndexReceiptBody {
		const DOMAIN: &'static str = "wendelon secondary index receipt v1";
		fn bound_signer(&self) -> Option<Signer> {
			Some(self.signer)
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

	impl Evidence for super::TakeOverBody {
		const DOMAIN: &'static str = "wendelon parent decision lineage takeover v1";
		fn bound_signer(&self) -> Option<Signer> {
			Some(self.successor)
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
	// ADR 0729: the old holder's applied lease goes with its generation.
	if let DecisionResult::TookOver { range_start, .. } = result {
		state.leases.remove(&range_start);
	}
	// ADR 0108 (wendelon): the catalog change a receipt completed.
	if let DecisionResult::IndexReceiptRecorded {
		tenant,
		index,
		transition: Some(transition),
	} = result
	{
		match transition {
			IndexTransition::Ready => state.secondary_indexes.promote_to_ready(tenant, index),
			IndexTransition::Removed => state.secondary_indexes.remove_dropped(tenant, index),
		}
	}
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
		DecisionCommand::TakeOverLineage { takeover, stamp_ms } => {
			take_over(state, decisions, &takeover, stamp_ms)
		}
		DecisionCommand::BeginBackupHold { id } => begin_backup_hold(decisions, id, log_index),
		DecisionCommand::ReleaseBackupHold { id, entered_at } => {
			release_backup_hold(decisions, id, entered_at)
		}
		DecisionCommand::GrantInstance { birth, slot, kind } => {
			instances::grant(state, decisions, birth, slot, kind)
		}
		DecisionCommand::BeginReplacement { token, slot } => {
			instances::begin_replacement(state, decisions, token, slot)
		}
		DecisionCommand::CompleteReplacement { token } => {
			instances::complete_replacement(state, decisions, token)
		}
		DecisionCommand::RecoveryProved { proof } => {
			record_recovery(state, decisions, &proof, log_index)
		}
		DecisionCommand::ActivateReplacement { proof } => {
			record_activation(state, decisions, &proof, log_index)
		}
		DecisionCommand::IndexReceipt { proof } => record_index_receipt(state, decisions, &proof),
	};
	outcome.unwrap_or_else(DecisionResult::Refused)
}

/// Wendelon ADR 0113: the table-operation index the retained keys imply. Two
/// retained table keys with one operation identity under different tables refuse.
/// ADR 0114: the Reserved table bodies by (tenant, table).
fn derive_live_by_table(
	bodies: &BTreeMap<u64, ParentRecord>,
) -> BTreeMap<(TenantId, crate::schema::TableObjectId), BTreeSet<u64>> {
	let mut live: BTreeMap<_, BTreeSet<u64>> = BTreeMap::new();
	for (rsv, body) in bodies {
		if body.status.is_terminal() {
			continue;
		}
		if let ParentNamespace::Table(table) = body.claim.key.namespace {
			live.entry((body.claim.key.tenant, table))
				.or_default()
				.insert(*rsv);
		}
	}
	live
}

fn derive_table_operations(
	keys: &BTreeMap<ParentKey, KeyEntry>,
) -> Result<BTreeMap<(TenantId, [u8; 16], u64), crate::schema::TableObjectId>, String> {
	let mut operations = BTreeMap::new();
	for key in keys.keys() {
		if let ParentNamespace::Table(table) = key.namespace
			&& operations.insert(key.operation(), table).is_some()
		{
			return Err(
				"decisions: one table operation identity is recorded under two tables".into(),
			);
		}
	}
	Ok(operations)
}

/// The record a retry lookup finds, after its request digest matched.
fn lookup(
	decisions: &DecisionState,
	key: &ParentKey,
	request_digest: &[u8; 32],
) -> Result<Option<(u64, ParentStatus)>, DecisionRefusal> {
	// Wendelon ADR 0113: a table operation identity already recorded under another
	// table conflicts, whatever this request is: it neither reserves nor answers
	// with the other table's record.
	if let ParentNamespace::Table(table) = key.namespace
		&& decisions
			.table_operations
			.get(&key.operation())
			.is_some_and(|recorded| *recorded != table)
	{
		return Err(DecisionRefusal::Conflict);
	}
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
	// ADR 0736: after the recorded-parent replay above and the identity and
	// retirement checks, so a recorded, retired or malformed identity keeps its
	// own answer; only an eligible new reservation is refused, recording nothing.
	if decisions.hold.is_some() {
		return Err(DecisionRefusal::BackupHold);
	}
	// ADR 0105 (wendelon): a slot being replaced coordinates nothing new.
	let coordinator = claim
		.children
		.get(usize::from(claim.coordinator))
		.map(|child| child.holder.node);
	if coordinator.is_some_and(|node| decisions.instances.preparing(node)) {
		return Err(DecisionRefusal::SlotBeingReplaced);
	}
	commit_hlc_not_ahead(claim, stamp_ms)?;
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
	// ADR 0720: the last check, so a claim refused for any other reason never
	// seals. The first admitted write fixes the membership it was admitted
	// under; every later one is admitted only under that membership.
	let formed = placement_under(state, Some(&*decisions));
	let seal = match (&decisions.sealed, formed) {
		(None, None) => return Err(DecisionRefusal::FormationIncomplete),
		(None, Some(placement)) => Some(SealedMembership { placement }),
		(Some(sealed), Some(placement)) if sealed.placement == placement => None,
		(Some(_), _) => return Err(DecisionRefusal::MembershipSealed),
	};
	// Every check passed: commit.
	if seal.is_some() {
		decisions.sealed = seal;
	}
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
	decisions.note_table_operation(&claim.key);
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
	decisions.note_live(&claim.key, reservation);
	Ok(DecisionResult::Reserved { reservation })
}

/// ADR 0700: a new claim's commit HLC leads the replicated leader stamp by at
/// most [`DECISION_COMMIT_HLC_LEAD_MS`]. No lower bound: transit and queueing
/// put every honest claim behind the stamp.
fn commit_hlc_not_ahead(claim: &Claim, stamp_ms: u64) -> Result<(), DecisionRefusal> {
	let latest = stamp_ms
		.checked_add(DECISION_COMMIT_HLC_LEAD_MS)
		.ok_or(DecisionRefusal::CounterOverflow)?;
	if claim.commit_hlc.physical_ms > latest {
		return Err(DecisionRefusal::CommitFromFuture);
	}
	Ok(())
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

/// The table a delete statement names, checked as a matched delete and a
/// RecordNoop both require: a canonical statement (wendelon ADR 0112), the
/// namespace is that table, the tenant writes, the table is admitting at the
/// request's generation with a supported shape. Returns every row's position,
/// aligned with the request's rows.
fn delete_target(
	state: &SmState,
	key: &ParentKey,
	request: &DeleteRequest,
) -> Result<Vec<u64>, DecisionRefusal> {
	use DecisionRefusal::*;
	if !request.is_canonical() {
		return Err(MalformedClaim);
	}
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
	request
		.rows
		.iter()
		.map(|row| delete_row_position(key.tenant, descriptor, *row).ok_or(MalformedClaim))
		.collect()
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
	if noop.selection.outcomes.len() != noop.request.rows.len()
		|| noop
			.selection
			.outcomes
			.iter()
			.any(|outcome| matches!(outcome, DeleteOutcome::Deleted { .. }))
		|| noop.selection.cut >= log_index
		|| noop.request_digest != delete_request_digest(noop.key.tenant, &noop.request)
	{
		return Err(MalformedClaim);
	}
	// Wendelon ADR 0112: the signer holds the statement's lowest requested
	// position.
	let position = delete_target(state, &noop.key, &noop.request)?
		.into_iter()
		.min()
		.ok_or(MalformedClaim)?;
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
	// Wendelon ADR 0112: a noop is bounded as a claim is.
	if bytes > MAX_CLAIM_BYTES {
		return Err(MalformedClaim);
	}
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
	decisions.note_table_operation(&noop.key);
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

	// Wendelon ADR 0112 (after ADR 0699 S4b): a matched delete statement publishes
	// one tombstone per row whose winner at the cut was live, above every winner
	// it selected. Its children partition exactly those rows: each matched
	// position lies in one child, a child's range runs from its lowest to its
	// highest matched position and its row count is theirs, and the coordinator
	// child holds the lowest matched position.
	if let Operation::Delete { request, selection } = &claim.operation {
		let positions = delete_target(state, &claim.key, request)?;
		if selection.outcomes.len() != positions.len()
			|| claim.request_digest != delete_request_digest(claim.key.tenant, request)
		{
			return Err(MalformedClaim);
		}
		let matched: Vec<u64> = positions
			.iter()
			.zip(&selection.outcomes)
			.filter(|(_, outcome)| matches!(outcome, DeleteOutcome::Deleted { .. }))
			.map(|(position, _)| *position)
			.collect();
		let Some(max_winner) = selection.max_deleted_winner() else {
			return Err(MalformedClaim);
		};
		if claim.commit_hlc <= max_winner
			|| matched.iter().min() != Some(&claim.first_row_position)
			|| claim.coordinator_child().map(|child| child.range_start)
				!= Some(claim.first_row_position)
		{
			return Err(MalformedClaim);
		}
		let mut per_child: Vec<(u32, u64, u64)> = vec![(0, u64::MAX, 0); children.len()];
		for position in &matched {
			let at = children
				.iter()
				.position(|child| child.range_start <= *position && *position <= child.range_end)
				.ok_or(MalformedClaim)?;
			let (count, low, high) = &mut per_child[at];
			*count += 1;
			*low = (*low).min(*position);
			*high = (*high).max(*position);
		}
		for (child, (count, low, high)) in children.iter().zip(per_child) {
			if child.rows != count || child.range_start != low || child.range_end != high {
				return Err(MalformedClaim);
			}
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
		// ADR 0729: a version of this generation orders after every version
		// reserved before the generation began.
		if claim.commit_hlc.physical_ms <= lineage.floor_ms {
			return Err(BelowTenureFloor);
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
			// ADR 0729: the holder is the lineage's, checked above, and in the
			// plan; it need not be the segment's first replica.
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
	decisions.forget_live(reservation);
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
/// ADR 0736: enters the hold, or returns the current token for the same id.
fn begin_backup_hold(
	decisions: &mut DecisionState,
	id: u128,
	log_index: u64,
) -> Result<DecisionResult, DecisionRefusal> {
	match decisions.hold {
		Some((held, entered_at)) if held == id => {
			Ok(DecisionResult::HoldEntered { id, entered_at })
		}
		Some(_) => Err(DecisionRefusal::HoldConflict),
		None => {
			decisions.hold = Some((id, log_index));
			Ok(DecisionResult::HoldEntered {
				id,
				entered_at: log_index,
			})
		}
	}
}

/// ADR 0736: ends the hold only when both parts of its token match.
fn release_backup_hold(
	decisions: &mut DecisionState,
	id: u128,
	entered_at: u64,
) -> Result<DecisionResult, DecisionRefusal> {
	if decisions.hold != Some((id, entered_at)) {
		return Err(DecisionRefusal::HoldMismatch);
	}
	decisions.hold = None;
	Ok(DecisionResult::HoldReleased)
}

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
	// Wendelon ADR 0113: the index follows the keys it is derived from.
	let keys = &decisions.keys;
	decisions.table_operations.retain(|operation, table| {
		keys.contains_key(&ParentKey {
			tenant: operation.0,
			namespace: ParentNamespace::Table(*table),
			client_uuid: operation.1,
			sequence_no: operation.2,
		})
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
pub mod instances;
pub use generations::{Certificate, ChunkSum, Generations, ReportBody, Target};

/// Wendelon ADR 0113: what the applied decisions say about a TABLE operation
/// identity, from the identity alone (no namespace, no request): the public
/// resolve. Key presence comes first; the replicated retirement cutoff decides
/// only for an identity with no retained key. A KV key never answers here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableResolution {
	/// Committed. A delete carries its frozen counts.
	Accepted {
		dec: u64,
		commit_hlc: HybridTimestamp,
		delete_counts: Option<[u32; 3]>,
	},
	/// A delete that published nothing: committed at its noop, with its counts.
	/// A noop carries no commit HLC.
	Noop { dec: u64, counts: [u32; 3] },
	/// Durably refused. Its selection, if any, had no effect.
	Refused { delete: bool },
	/// Recorded and not yet decided: pending, whatever the cutoff.
	Reserved { delete: bool },
	/// No retained key, and the identity can still be presented.
	Unknown,
	/// No retained key, at or below the cutoff: retired, or never seen. No claim
	/// that it ever committed.
	Expired,
	/// Not a UUIDv7.
	InvalidIdentity,
}

pub fn resolve_table_operation(
	decisions: &DecisionState,
	tenant: TenantId,
	client_uuid: [u8; 16],
	sequence_no: u64,
) -> TableResolution {
	if let Some(table) = decisions.table_of_operation((tenant, client_uuid, sequence_no)) {
		let key = ParentKey {
			tenant,
			namespace: ParentNamespace::Table(table),
			client_uuid,
			sequence_no,
		};
		let entry = decisions.keys[&key];
		match entry.record {
			RecordKind::Noop => {
				return TableResolution::Noop {
					dec: entry.reservation,
					counts: decisions.noops[&entry.reservation].body.selection.counts(),
				};
			}
			RecordKind::Body => {
				let body = &decisions.bodies[&entry.reservation];
				let selection = match &body.claim.operation {
					Operation::Delete { selection, .. } => Some(selection),
					_ => None,
				};
				let delete = selection.is_some();
				return match body.status {
					ParentStatus::Accepted { dec } => TableResolution::Accepted {
						dec,
						commit_hlc: body.claim.commit_hlc,
						delete_counts: selection.map(DeleteSelection::counts),
					},
					ParentStatus::Refused { .. } => TableResolution::Refused { delete },
					ParentStatus::Reserved | ParentStatus::Noop { .. } => {
						TableResolution::Reserved { delete }
					}
				};
			}
		}
	}
	match crate::schema::uuid_v7_unix_millis(client_uuid) {
		None => TableResolution::InvalidIdentity,
		Some(issued) if issued <= decisions.retirement_cutoff() => TableResolution::Expired,
		Some(_) => TableResolution::Unknown,
	}
}
