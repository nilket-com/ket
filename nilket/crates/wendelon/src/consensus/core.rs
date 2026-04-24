use crate::crypto::{InstallTenantKekVersion, RetireTenantKekVersion, TenantKeyset};
use crate::network::tls::ActorTrustDomain;
use crate::schema::{
	FinalizationMarker, HybridTimestamp, Identity, LeaseDescriptor, NodeId,
	ReleaseLeaseRequestMeta, SlabId, TenantId, TenantKekId,
};
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
	pub slab_id: SlabId,
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
	/// `true` when the L2 object at `l2_uri` already carries an
	/// encrypted tiered-slab footer. Lets the plaintext-rewrite
	/// sweeper short-circuit without a GET on every pass once steady
	/// state is reached. Trailing field with `#[serde(default)]` so
	/// older Raft entries deserialize as `false` (the safe, accurate
	/// classification — the rewrite sweeper will GET and stamp the
	/// truth back through a fresh `TierSlab` commit on the next pass).
	#[serde(default)]
	pub is_encrypted: bool,
	/// Tenant that owns the slab payload. Trailing optional field so
	/// older Raft entries deserialize cleanly and can be backfilled
	/// by a later `TierSlab` refresh without a separate migration.
	#[serde(default)]
	pub tenant_id: Option<TenantId>,
	/// Tenant KEK version referenced by the tiered slab footer when
	/// `is_encrypted = true`. Used for retirement accounting. Older
	/// entries default to `None` and are conservatively treated as
	/// "not yet accounted" until a later sweeper refresh stamps the
	/// authoritative value through replicated state.
	#[serde(default)]
	pub tenant_kek_id: Option<TenantKekId>,
}

/// Payload of a `RetireTieredSlab` Raft entry.
///
/// Committed when a previously-tiered slab no longer has any live
/// rows after MVCC retention pruning. Every replica removes the
/// pointer from replicated state and the local `SlabManager`
/// observes the apply to drop any cached rows / rehydrated copy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetireTieredSlab {
	pub slab_id: SlabId,
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

/// Tenant lifecycle tracked by the authoritative state machine.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TenantLifecycleState {
	/// Reserved for a future multi-step provisioning flow. The W5
	/// create path commits directly to `Active`.
	Creating,
	Active,
	Suspended,
	Deleting,
	Deleted,
}

impl TenantLifecycleState {
	pub fn allows_writes(self) -> bool {
		matches!(self, Self::Active)
	}

	pub fn allows_reads(self) -> bool {
		matches!(self, Self::Active | Self::Suspended)
	}

	pub fn can_transition_to(self, next: Self) -> bool {
		matches!(
			(self, next),
			(Self::Creating, Self::Active)
				| (Self::Active, Self::Suspended)
				| (Self::Active, Self::Deleting)
				| (Self::Active, Self::Deleted)
				| (Self::Suspended, Self::Active)
				| (Self::Suspended, Self::Deleting)
				| (Self::Suspended, Self::Deleted)
				| (Self::Deleting, Self::Deleted)
		)
	}
}

/// Authoritative tenant metadata replicated through Raft.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantRecord {
	pub tenant_id: TenantId,
	pub state: TenantLifecycleState,
	pub quota_bytes: u64,
	pub used_bytes: u64,
	/// Stored now so per-tenant topology work can reuse the same
	/// authoritative tenant record once replication-factor routing lands.
	pub replication_factor: u32,
}

/// Raft-persisted form of a finalized write. The streamable
/// [`FinalizationMarker`] remains the client-facing control-plane
/// receipt; the replicated wrapper adds the tenant binding and the
/// authoritative byte charge for quota accounting.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantFinalization {
	pub marker: FinalizationMarker,
	pub tenant_id: TenantId,
	pub key_identity: Identity,
	pub charged_bytes: u64,
	#[serde(default = "default_finalized_record_count")]
	pub record_count: u32,
}

const fn default_finalized_record_count() -> u32 {
	1
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateTenant {
	pub tenant_id: TenantId,
	pub quota_bytes: u64,
	pub replication_factor: u32,
	pub tenant_keyset: TenantKeyset,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateTenantQuota {
	pub tenant_id: TenantId,
	pub quota_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RevokedActorKeyId {
	pub trust_domain: ActorTrustDomain,
	#[serde(with = "serde_bytes")]
	pub key_id: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RevokeActorKey {
	pub trust_domain: ActorTrustDomain,
	#[serde(with = "serde_bytes")]
	pub key_id: Vec<u8>,
	#[serde(default)]
	pub revoked_at_ms: u64,
}

impl RevokeActorKey {
	pub fn key(&self) -> RevokedActorKeyId {
		RevokedActorKeyId {
			trust_domain: self.trust_domain,
			key_id: self.key_id.clone(),
		}
	}
}

/// The state machine application data (mutations).
/// In HashRaft, the actual data (Arrow batches) never enters the Raft log (ADR 003).
/// The Raft log only contains control-ring metadata and Data Ring finalization markers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WendelonRequest {
	/// A Data Ring quorum has been achieved; advance the E_safe watermark.
	Finalize(TenantFinalization),
	/// Grant a Per-Range Lease to a Data Node.
	AcquireLease(LeaseDescriptor),
	/// Release a previously granted Per-Range Lease.
	ReleaseLease(ReleaseLeaseRequestMeta),
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
	/// A previously-tiered slab has become fully reclaimable and its
	/// L2 pointer should be removed from replicated state.
	RetireTieredSlab(RetireTieredSlab),
	/// Seed a Migration Ledger entry for a reassigned range
	/// (Plan 019). Idempotent: a replay with the same
	/// `migration_id` is a no-op.
	SeedMigration(SeedMigration),
	/// Finish a migration: transitions the ledger entry to
	/// `Completed`, installs a tombstone on prior source
	/// replicas, and advances any descendant entries that were
	/// sourcing from the now-superseded physical holder.
	MigrationComplete(MigrationComplete),
	/// Provision a tenant record plus its initial Tenant KEK in one
	/// Raft commit so bootstrap and operator-driven tenant creation
	/// exercise the same state-machine path.
	CreateTenant(CreateTenant),
	/// Update the authoritative storage quota for a tenant.
	UpdateTenantQuota(UpdateTenantQuota),
	/// Reject future writes while leaving reads available.
	SuspendTenant(TenantId),
	/// Re-open writes after a prior suspension.
	ResumeTenant(TenantId),
	/// Begin or complete tenant deletion. Empty tenants move
	/// directly to `Deleted`; non-empty tenants park in `Deleting`
	/// until later cleanup drives them to zero usage.
	DeleteTenant(TenantId),
	/// Install a fresh Tenant KEK version and mark it active for
	/// subsequent slab DEK wraps. Historical versions remain in the
	/// state machine so pre-rotation slabs keep decrypting.
	///
	/// Keep durable tenant-crypto variants at the end of the enum so
	/// postcard discriminants for older variants remain
	/// upgrade-compatible.
	InstallTenantKekVersion(InstallTenantKekVersion),
	/// Destructively retire a historical Tenant KEK version once the
	/// replicated tiered-slab accounting proves no encrypted slab still
	/// references it.
	RetireTenantKekVersion(RetireTenantKekVersion),
	/// Revoke an actor certificate key_id within a trust domain. The
	/// denylist is replicated so transport and observability admission
	/// do not depend on best-effort local refresh.
	RevokeActorKey(RevokeActorKey),
}

/// The response returned to the client after a Raft command is committed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WendelonResponse {
	/// The command was successfully committed and applied.
	Success,
	/// The lease was denied because another node currently holds a valid lease.
	LeaseDenied,
	/// The command reached the state machine but was rejected by
	/// semantic validation (for example a duplicate KEK id).
	Error(String),
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
		SnapshotData = crate::consensus::raft_storage::SnapshotDataFile,
		AsyncRuntime = openraft::TokioRuntime,
);

/// The concrete Wendelon Raft alias.
pub type WendelonRaft = openraft::Raft<WendelonTypeConfig>;
