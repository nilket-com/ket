use crate::schema::{FinalizationMarker, HybridTimestamp, LeaseDescriptor, NodeId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Payload of a `TierSlab` Raft entry (ADR 006).
///
/// Commited when the background sweeper has successfully streamed
/// a sealed L1 slab to L2 and verified the round-trip hash. The
/// state machine records the pointer so every replica knows that
/// subsequent reads against this slab must pivot to L2; the local
/// `SlabManager` observes the same event via a tier observer and
/// unlinks its own L1 file.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TieredSlab {
	/// Identity of the slab that has been promoted to L2.
	pub slab_id: u64,
	/// Opaque URI the L2 [`ObjectStore`] hands back from `put`.
	/// Treated as an opaque string by the state machine — only
	/// the read path interprets it.
	///
	/// [`ObjectStore`]: crate::storage::l2::ObjectStore
	pub l2_uri: String,
	/// blake3 of the slab payload as uploaded. Used by the read
	/// path to detect bit-rot between L2 fetches and the sweeper's
	/// original upload.
	pub payload_hash: [u8; 32],
}

/// The unified Node ID type used across Wendelon.
/// OpenRaft requires the `NodeId` type to implement various standard traits,
/// so we wrap our `bytemuck` struct in a newtype or implement them directly.
impl std::fmt::Display for NodeId {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.id)
	}
}

/// Lifecycle state of a [`MigrationLedgerEntry`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationState {
	/// Entry seeded; no node has successfully pulled a page yet.
	Pending,
	/// The logical owner has started ingesting retained versions
	/// via `append_migration`. Not durably used today, but
	/// reserved so a richer progress view can land without a
	/// schema migration.
	BackfillInProgress,
	/// The logical owner has drained the range at the pinned
	/// floor and committed `MigrationComplete`. The ledger entry
	/// is now authoritative: future chained migrations source
	/// from `completed_by` rather than the original replicas.
	Completed,
}

/// One row of the Raft-persisted Migration Ledger (Plan 019).
///
/// When the Control Ring advances from `ring(E)` to `ring(E+1)`,
/// every reassigned hash range gets a ledger entry so both the
/// read-path fallback and the background sweeper converge on the
/// same lineage. The `migration_hlc_floor` is pinned once at
/// creation and never slides — source and destination must agree
/// exactly on which versions belong to the migration set.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationLedgerEntry {
	/// Deterministic identifier derived from (`to_epoch`,
	/// `range_start`, `range_end`). Survives restarts because it
	/// is stamped on Raft commit, not minted per process.
	pub migration_id: u64,
	pub range_start: u64,
	pub range_end: u64,
	/// Epoch the range used to live in.
	pub from_epoch: u64,
	/// Epoch the range now logically belongs to.
	pub to_epoch: u64,
	/// Current logical owner. Only this node may issue a
	/// `MIGRATE_REQ` against a peer for this entry.
	pub logical_owner: NodeId,
	/// Ancestor migration, if any. Set when a chained migration
	/// lands before the previous one completed so descendants can
	/// be reconciled in [`apply_request`].
	pub upstream_migration_id: Option<u64>,
	/// Ordered pull candidates. First candidate is the preferred
	/// source; followers try the rest on `INTERNAL_ERROR`. The
	/// list is mutable inside the ledger — `MigrationComplete`
	/// advances descendants to prefer the new physical holder.
	pub source_replicas: Vec<NodeId>,
	/// Pinned MVCC cutoff. Migration ingests every version with
	/// `hlc >= migration_hlc_floor`.
	pub migration_hlc_floor: HybridTimestamp,
	pub state: MigrationState,
	pub completed_by: Option<NodeId>,
	pub completed_at_hlc: Option<HybridTimestamp>,
}

/// Proposed by the Control Ring during an epoch transition to
/// seed one ledger entry per reassigned range. Kept as a thin
/// value type so future Control Ring work can propose many of
/// these in a single batch without reshaping `WendelonRequest`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeedMigration {
	pub entry: MigrationLedgerEntry,
}

/// Proposed by the logical owner once it has drained the ledger
/// range at the pinned floor. The redundant range / epoch / floor
/// fields let the state machine reject a stale commit rather than
/// blindly trusting a replayed `migration_id`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationComplete {
	pub migration_id: u64,
	pub range_start: u64,
	pub range_end: u64,
	pub to_epoch: u64,
	pub completed_by: NodeId,
	pub completed_at_hlc: HybridTimestamp,
	pub migration_hlc_floor: HybridTimestamp,
}

/// Durable peer description for a committed epoch config.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpochPeer {
	pub node_id: NodeId,
	pub address: String,
	pub tier_bits: u8,
	pub role_bits: u8,
}

/// Durable projection of a committed `ring(E)` membership snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpochConfig {
	pub epoch: u64,
	pub peers: BTreeMap<u32, EpochPeer>,
}

/// Committed control-ring transition from `ring(E)` to `ring(E+1)`.
///
/// The Control Ring persists both sides of the boundary first, then
/// computes and commits deterministic migration seeds afterward. That
/// lets a later leader reconstruct the exact expected seed batch if
/// the original leader fails mid-transition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdvanceEpochRecord {
	pub from: EpochConfig,
	pub to: EpochConfig,
}

/// Logical "this range moved away" marker held on prior source
/// replicas. Fences the range out of future migration-source
/// scans without requiring an immediate slab rewrite; physical
/// reclaim happens later during tombstone-aware compaction. The
/// compactor may drop local records in `[range_start, range_end]`
/// whose `hlc >= migration_hlc_floor`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigratedRangeTombstone {
	pub migration_id: u64,
	pub range_start: u64,
	pub range_end: u64,
	pub to_epoch: u64,
	pub completed_by: NodeId,
	pub migration_hlc_floor: HybridTimestamp,
}

/// The state machine application data (mutations).
/// In HashRaft, the actual data (Arrow batches) never enters the Raft log (ADR 003).
/// The Raft log only contains control-ring metadata and Data Ring finalization markers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WendelonRequest {
	/// A Data Ring quorum has been achieved; advance the E_safe watermark.
	Finalize(FinalizationMarker),
	/// Grant a Per-Range Lease to a Data Node.
	AcquireLease(LeaseDescriptor),
	/// Commit the next epoch/ring config before auto-seeding the
	/// Migration Ledger for the reassigned ranges.
	AdvanceEpoch(AdvanceEpochRecord),
	/// A namespace creation or schema registry update.
	#[serde(with = "serde_bytes")]
	SchemaUpdate(Vec<u8>),
	/// A sealed slab has been promoted from L1 to L2 (ADR 006).
	/// Committing this entry causes every replica's state machine
	/// to record the L2 pointer and — via the tier observer — the
	/// local `SlabManager` to pivot reads and unlink the L1 file.
	TierSlab(TieredSlab),
	/// Seed a Migration Ledger entry for a reassigned range
	/// (Plan 019). Idempotent: a replay with the same
	/// `migration_id` is a no-op.
	SeedMigration(SeedMigration),
	/// Finish a migration: transitions the ledger entry to
	/// `Completed`, installs a tombstone on prior source
	/// replicas, and advances any descendant entries that were
	/// sourcing from the now-superseded physical holder.
	MigrationComplete(MigrationComplete),
}

/// The response returned to the client after a Raft command is committed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WendelonResponse {
	/// The command was successfully committed and applied.
	Success,
	/// The lease was denied because another node currently holds a valid lease.
	LeaseDenied,
}

// The Wendelon implementation of the `openraft::RaftTypeConfig` trait.
// This trait wires our custom types into the OpenRaft engine.
openraft::declare_raft_types!(
	/// The configuration type for the Wendelon Control Ring.
	pub WendelonTypeConfig:
		D = WendelonRequest,
		R = WendelonResponse,
		NodeId = u32,
		Node = openraft::BasicNode,
		Entry = openraft::Entry<WendelonTypeConfig>,
		SnapshotData = std::io::Cursor<Vec<u8>>,
		AsyncRuntime = openraft::TokioRuntime,
);

/// The concrete Wendelon Raft alias.
pub type WendelonRaft = openraft::Raft<WendelonTypeConfig>;
