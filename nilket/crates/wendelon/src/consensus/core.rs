use crate::crypto::{InstallTenantKekVersion, RetireTenantKekVersion, TenantKeyset};
use crate::integration::compute::{DropOperationId, SchemaGeneration};
use crate::network::tls::ActorTrustDomain;
use crate::schema::{
	CtasSagaMutation, FinalizationMarker, HybridTimestamp, Identity, LeaseDescriptor, NodeId,
	PolicyGeneration, ReleaseLeaseRequestMeta, SlabId, SupersessionMetadata, TableObjectId,
	TableRetentionPolicy, TenantId, TenantKekId, TombstoneMetadata,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Payload of a `TierSlab` Raft entry (ADR 006).
///
/// A random 128-bit identity for one store, fixed at its creation (ADR 0697).
/// A slab id is local to the store incarnation that issued it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct StoreIncarnation(u128);

impl StoreIncarnation {
	pub(crate) fn fresh() -> Self {
		loop {
			let value = uuid::Uuid::new_v4().as_u128();
			if value != 0 {
				return Self(value);
			}
		}
	}

	pub(crate) fn from_raw(value: u128) -> Self {
		Self(value)
	}

	pub fn get(self) -> u128 {
		self.0
	}
}

/// The store that owns a slab: its node and that store's incarnation. Only a
/// `SlabManager` mints one for its own store; the control ring decodes and
/// compares them but never builds one from a bare slab id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OwnerRef {
	node: u32,
	incarnation: StoreIncarnation,
}

impl OwnerRef {
	pub(crate) fn new(node: u32, incarnation: StoreIncarnation) -> Self {
		Self { node, incarnation }
	}

	pub fn node(self) -> u32 {
		self.node
	}

	pub fn incarnation(self) -> StoreIncarnation {
		self.incarnation
	}

	/// A fixed owner for unit fixtures that build ring commands without a store.
	#[cfg(test)]
	pub(crate) fn for_test(node: u32, incarnation: u128) -> Self {
		Self::new(node, StoreIncarnation::from_raw(incarnation))
	}

	/// The owner-qualified name of one of this store's slabs.
	pub fn slab(self, slab: SlabId) -> SlabRef {
		SlabRef { owner: self, slab }
	}
}

/// A slab named across the control ring: the bare id is local to `owner`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SlabRef {
	owner: OwnerRef,
	slab: SlabId,
}

impl SlabRef {
	pub fn owner(self) -> OwnerRef {
		self.owner
	}

	/// The local id, meaningful only inside `owner`'s store.
	pub fn local_id(self) -> SlabId {
		self.slab
	}
}

/// Commited when the background sweeper has successfully streamed
/// a sealed L1 slab to L2 and verified the round-trip hash. The
/// state machine records the pointer so every replica knows that
/// subsequent reads against this slab must pivot to L2; the local
/// `SlabManager` observes the same event via a tier observer and
/// unlinks its own L1 file.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TieredSlab {
	/// The owner-qualified slab that has been promoted to L2 (ADR 0697).
	pub slab: SlabRef,
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
	/// ADR 039 advisory: stats format advertised by the slab footer
	/// when known. The slab tail remains authoritative.
	#[serde(default)]
	pub stats_format_version: Option<u16>,
	/// ADR 039 advisory: stats-block ciphertext length when known.
	#[serde(default)]
	pub stats_block_len: Option<u32>,
	/// ADR 039 advisory: BLAKE3(stats ciphertext)[0..16] when known.
	#[serde(default)]
	pub stats_block_hash128: Option<[u8; 16]>,
	/// ADR 040 visibility proof for CSB aggregate answers.
	/// `Untracked` means this pointer cannot prove aggregate
	/// short-circuit visibility.
	#[serde(default)]
	pub tombstone_metadata: TombstoneMetadata,
	/// ADR 042 visibility proof for CSB aggregate answers.
	/// `Untracked` means this pointer cannot prove aggregate
	/// short-circuit visibility.
	#[serde(default)]
	pub supersession_metadata: SupersessionMetadata,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TableDropPrepareRequest {
	V1 {
		operation_id: DropOperationId,
		expected_generation: SchemaGeneration,
	},
}

impl TableDropPrepareRequest {
	pub fn current(operation_id: DropOperationId, expected_generation: SchemaGeneration) -> Self {
		Self::V1 {
			operation_id,
			expected_generation,
		}
	}

	pub fn operation_id(self) -> DropOperationId {
		match self {
			Self::V1 { operation_id, .. } => operation_id,
		}
	}

	pub fn expected_generation(self) -> SchemaGeneration {
		match self {
			Self::V1 {
				expected_generation,
				..
			} => expected_generation,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TableReclaimCompletionRequest {
	V1 {
		operation_id: DropOperationId,
		fence_log_index: u64,
	},
}

impl TableReclaimCompletionRequest {
	pub fn current(operation_id: DropOperationId, fence_log_index: u64) -> Self {
		Self::V1 {
			operation_id,
			fence_log_index,
		}
	}

	pub fn operation_id(self) -> DropOperationId {
		match self {
			Self::V1 { operation_id, .. } => operation_id,
		}
	}

	pub fn fence_log_index(self) -> u64 {
		match self {
			Self::V1 {
				fence_log_index, ..
			} => fence_log_index,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TableDropFinalizeRequest {
	V1 {
		operation_id: DropOperationId,
		fence_log_index: u64,
		reclaim_completion_log_index: u64,
	},
}

impl TableDropFinalizeRequest {
	pub fn current(
		operation_id: DropOperationId,
		fence_log_index: u64,
		reclaim_completion_log_index: u64,
	) -> Self {
		Self::V1 {
			operation_id,
			fence_log_index,
			reclaim_completion_log_index,
		}
	}

	pub fn operation_id(self) -> DropOperationId {
		match self {
			Self::V1 { operation_id, .. } => operation_id,
		}
	}

	pub fn fence_log_index(self) -> u64 {
		match self {
			Self::V1 {
				fence_log_index, ..
			} => fence_log_index,
		}
	}

	pub fn reclaim_completion_log_index(self) -> u64 {
		match self {
			Self::V1 {
				reclaim_completion_log_index,
				..
			} => reclaim_completion_log_index,
		}
	}
}

/// ADR 0095 logical finalize. Unlike the replay-only ADR 039/0093 finalize,
/// this stage does not require physical reclaim evidence: apply converts the
/// admission fence into a tombstone and records durable debt.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableDropLogicalFinalizeRequest {
	pub operation_id: DropOperationId,
	pub fence_log_index: u64,
}

/// ADR 0095 physical settlement after every stamped participant attests.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableDropSettlementRequest {
	pub operation_id: DropOperationId,
	pub finalize_log_index: u64,
	pub ownership_epoch: u64,
	pub attested_participants: crate::schema::TableDropParticipantSet,
}

/// Payload of a `RegisterIncarnation` Raft entry (ADR 0697 §3): record a
/// node's current store incarnation. A first registration names no
/// predecessor; a replacement names the exact current one it supersedes. A
/// retired incarnation never becomes current again.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterIncarnation {
	pub node: u32,
	pub incarnation: StoreIncarnation,
	pub supersedes: Option<StoreIncarnation>,
}

/// Why the state machine refused a slab command without applying or
/// recording anything. A refusal is not an outcome: it grants no cleanup
/// authority and does not prove an earlier attempt never applied.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SlabRefusal {
	/// The command's owner is not its node's current store incarnation.
	NotCurrentOwner {
		owner: OwnerRef,
		current: Option<StoreIncarnation>,
	},
	/// The command's owner node is not the leader that appended the entry.
	NotAppendingLeader { owner_node: u32, leader_node: u32 },
	/// A registration that does not name the node's current incarnation.
	StaleSupersede {
		node: u32,
		current: Option<StoreIncarnation>,
	},
	/// A registration of an incarnation that was already retired.
	RetiredIncarnation {
		node: u32,
		incarnation: StoreIncarnation,
	},
	/// An operation past its owner's window. A correct owner never sends one.
	OutsideWindow {
		key: crate::consensus::slab_ops::OpKey,
		acked_through: u64,
	},
	/// An acknowledgement batch larger than the window.
	AckBatchTooLarge { len: u64 },
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
/// ADR 0090: the transfer state of one partition, derived from the Migration
/// Ledger. A partition in flight is served by exactly one node — the entry's
/// `logical_owner` — and a partition not in flight is served by the ring. The
/// type carries a single `serving` node, so "both owners" and "neither owner"
/// are unrepresentable outcomes rather than guarded ones.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionTransfer {
	pub partition: crate::schema::PartitionId,
	/// The one node serving this partition while the transfer is in flight.
	pub serving: NodeId,
	/// Preferred pull source, when the ledger names one.
	pub source: Option<NodeId>,
}

/// The locality span a partition covers: its prefix followed by every suffix.
/// Contiguous by construction, so the range-shaped Migration Ledger can carry a
/// whole partition without a schema change.
pub fn partition_span(
	partition: crate::schema::PartitionId,
	prefix_bits: u8,
) -> std::io::Result<(u64, u64)> {
	if prefix_bits == 0 || prefix_bits >= 64 {
		return Err(std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			format!("partition prefix_bits must be in 1..=63, got {prefix_bits}"),
		));
	}
	let suffix_mask = u64::MAX >> prefix_bits;
	Ok((partition.get(), partition.get() | suffix_mask))
}

/// ADR 0090: the in-flight transfer covering a partition, if any.
///
/// A partition is covered by an entry whose range contains its representative
/// position — the partition value itself, which is also the single ring
/// position the whole partition routes by. Two in-flight entries covering the
/// same partition with different owners is a both-owners hazard and is refused
/// rather than resolved by precedence: a malformed ledger must fail closed, not
/// pick a side.
pub fn partition_transfer_for(
	partition: crate::schema::PartitionId,
	migrations: &std::collections::HashMap<u64, MigrationLedgerEntry>,
) -> std::io::Result<Option<PartitionTransfer>> {
	let position = partition.get();
	let mut covering = migrations
		.values()
		.filter(|entry| {
			entry.state != MigrationState::Completed
				&& entry.range_start <= position
				&& position <= entry.range_end
		})
		.collect::<Vec<_>>();
	covering.sort_by_key(|entry| entry.migration_id);
	let Some(first) = covering.first() else {
		return Ok(None);
	};
	if covering
		.iter()
		.any(|entry| entry.logical_owner.id != first.logical_owner.id)
	{
		return Err(std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			format!(
				"partition {position:#x} is claimed by in-flight migrations with \
				 different owners; refusing to serve it from either",
			),
		));
	}
	Ok(Some(PartitionTransfer {
		partition,
		serving: first.logical_owner,
		source: first.source_replicas.first().copied(),
	}))
}

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
	#[serde(default)]
	pub capability_bits: u8,
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
	pub completed_at_hlc: HybridTimestamp,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetTableRetentionPolicy {
	pub tenant_id: TenantId,
	pub table_object_id: TableObjectId,
	pub expected_policy_generation: PolicyGeneration,
	pub new_policy: TableRetentionPolicy,
	pub updated_at_hlc: HybridTimestamp,
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
	/// Retired by ADR 0697; the position stays so later variants keep their wire tags.
	RetiredTierSlab(crate::consensus::slab_ops::RetiredSlabCommand),
	/// A previously-tiered slab has become fully reclaimable and its
	/// L2 pointer should be removed from replicated state.
	/// Retired by ADR 0697; the position stays so later variants keep their wire tags.
	RetiredRetireTieredSlab(crate::consensus::slab_ops::RetiredSlabCommand),
	/// Commit an ADR 055 LSM manifest transition. The control ring
	/// carries the compaction digest summary but does not introduce
	/// any separate digest mutation or eviction marker.
	/// Retired by ADR 0697; the position stays so later variants keep their wire tags.
	RetiredCompactionCommit(crate::consensus::slab_ops::RetiredSlabCommand),
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
	/// Update per-table lifecycle retention policy through the typed
	/// engine mutation surface. User-facing policy syntax is resolved
	/// by client tools before this request is submitted.
	SetTableRetentionPolicy(SetTableRetentionPolicy),
	/// ADR 039 Wendelon-internal DROP prepare/fence stage.
	TableDropPrepare(TableDropPrepareRequest),
	/// ADR 039 Wendelon-internal DROP reclaim-completion stage.
	TableReclaimCompletion(TableReclaimCompletionRequest),
	/// ADR 039 Wendelon-internal DROP finalize stage.
	TableDropFinalize(TableDropFinalizeRequest),
	/// ADR 548 replicated CTAS staged-publication lifecycle mutation.
	CtasSaga(CtasSagaMutation),
	/// ADR 0094 replay-only bridge. No live producer may construct this.
	ReplayLegacyCtasRowDispatched(crate::schema::ReplayLegacyCtasRowDispatched),
	/// ADR 0095 logical completion: fence becomes admission-authoritative
	/// tombstone and replicated physical-reclaim debt.
	TableDropLogicalFinalize(TableDropLogicalFinalizeRequest),
	/// ADR 0095 physical settlement after the complete stamped set attests.
	TableDropSettlement(TableDropSettlementRequest),
	/// V-S1 durable carrier, held without domain-state effects. Its wire
	/// position is pinned by the Root2 audit vectors.
	Root2Command(crate::consensus::Root2Carrier1),
	/// Record a node's current store incarnation (ADR 0697). Appended after
	/// the Root2 carrier so its pinned wire position does not move.
	RegisterIncarnation(RegisterIncarnation),
	/// One operation of a slab's owner (ADR 0697 §5).
	SlabOp(crate::consensus::slab_ops::SlabOp),
	/// Close an operation by its original digest (ADR 0697 §5.4).
	CancelOp(crate::consensus::slab_ops::CancelOp),
	/// The owner has durably finished these operations (ADR 0697 §5.3).
	AckOutcomes(crate::consensus::slab_ops::AckOutcomes),
	/// A parent write decision (ADR 0699 S2). Only the `ParentDecisions` WAL
	/// format carries it; today's format refuses it both ways.
	Decision(crate::consensus::decisions::DecisionCommand),
	/// ADR 0701: one holder's lease renewals in one entry, each applied in
	/// declared order exactly as an `AcquireLease`. `ParentDecisions` only.
	AcquireLeases(LeaseBatch),
}

/// ADR 0701: at most [`crate::schema::MAX_LEASES_PER_BATCH`] leases. Decoding
/// refuses a longer sequence from its length, before allocating it; the
/// envelope's other rules (one holder and epoch, no duplicate start, well-formed
/// ranges, not empty) are checked whole at apply.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseBatch(Vec<crate::schema::LeaseDescriptor>);

impl LeaseBatch {
	/// `None` above the cap.
	pub fn new(leases: Vec<crate::schema::LeaseDescriptor>) -> Option<Self> {
		(leases.len() <= crate::schema::MAX_LEASES_PER_BATCH).then_some(Self(leases))
	}

	pub fn leases(&self) -> &[crate::schema::LeaseDescriptor] {
		&self.0
	}
}

impl Serialize for LeaseBatch {
	fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		serializer.collect_seq(&self.0)
	}
}

impl<'de> Deserialize<'de> for LeaseBatch {
	fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		deserialize_bounded_seq(deserializer, crate::schema::MAX_LEASES_PER_BATCH).map(Self)
	}
}

/// ADR 0701: one lease's outcome in a batch, in declared order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseGrantResult {
	pub range_start: u64,
	pub granted: bool,
}

/// ADR 0701: a batch's results, bounded like the batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseGrantResults(Vec<LeaseGrantResult>);

impl LeaseGrantResults {
	/// From an applied batch, which is itself bounded.
	pub(crate) fn from_applied(results: Vec<LeaseGrantResult>) -> Self {
		Self(results)
	}

	pub fn results(&self) -> &[LeaseGrantResult] {
		&self.0
	}
}

impl Serialize for LeaseGrantResults {
	fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		serializer.collect_seq(&self.0)
	}
}

impl<'de> Deserialize<'de> for LeaseGrantResults {
	fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		deserialize_bounded_seq(deserializer, crate::schema::MAX_LEASES_PER_BATCH).map(Self)
	}
}

/// A sequence of at most `max` elements; a longer declared length is refused
/// before any element is allocated or read.
fn deserialize_bounded_seq<'de, D, T>(deserializer: D, max: usize) -> Result<Vec<T>, D::Error>
where
	D: serde::Deserializer<'de>,
	T: Deserialize<'de>,
{
	struct Bounded<T>(usize, std::marker::PhantomData<T>);
	impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for Bounded<T> {
		type Value = Vec<T>;
		fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
			write!(f, "a sequence of at most {} elements", self.0)
		}
		fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<T>, A::Error> {
			if seq.size_hint().is_some_and(|len| len > self.0) {
				return Err(serde::de::Error::custom(format!(
					"sequence longer than {}",
					self.0
				)));
			}
			let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(self.0));
			while let Some(item) = seq.next_element()? {
				if out.len() == self.0 {
					return Err(serde::de::Error::custom(format!(
						"sequence longer than {}",
						self.0
					)));
				}
				out.push(item);
			}
			Ok(out)
		}
	}
	deserializer.deserialize_seq(Bounded(max, std::marker::PhantomData))
}

impl WendelonRequest {
	/// ADR 0092: every WAL-body request variant, by name, in declaration
	/// order. Paired with [`Self::variant_name`]'s total match, this is the
	/// exact-set guard behind the consensus-body fixture corpus: appending a
	/// variant moves no existing golden bytes, so the corpus alone cannot see
	/// it — the count and this list can. Adding a variant here means the WAL
	/// format version must move with it.
	pub const ALL_VARIANT_NAMES: [&'static str; 28] = [
		"finalize",
		"acquire_lease",
		"release_lease",
		"advance_epoch",
		"schema_update",
		"seed_migration",
		"migration_complete",
		"create_tenant",
		"update_tenant_quota",
		"suspend_tenant",
		"resume_tenant",
		"delete_tenant",
		"install_tenant_kek_version",
		"retire_tenant_kek_version",
		"revoke_actor_key",
		"set_table_retention_policy",
		"table_drop_prepare",
		"table_reclaim_completion",
		"table_drop_finalize",
		"ctas_saga",
		"replay_legacy_ctas_row_dispatched",
		"table_drop_logical_finalize",
		"table_drop_settlement",
		"root2_command",
		"register_incarnation",
		"slab_op",
		"cancel_op",
		"ack_outcomes",
	];

	/// Variants only the `ParentDecisions` WAL format (10) carries, appended
	/// after [`Self::ALL_VARIANT_NAMES`]. Today's format 8 refuses them on write
	/// and on read, so its body set is exactly the list above.
	pub const PARENT_DECISIONS_VARIANT_NAMES: [&'static str; 2] = ["decision", "acquire_leases"];

	/// True for a variant today's WAL format must never carry.
	pub fn is_parent_decisions_only(&self) -> bool {
		matches!(self, Self::Decision(_) | Self::AcquireLeases(_))
	}

	/// Total by construction: a new variant fails this match at compile time,
	/// which is the earliest possible moment to learn the WAL body shape is
	/// about to change.
	pub fn variant_name(&self) -> &'static str {
		match self {
			Self::Finalize(_) => "finalize",
			Self::AcquireLease(_) => "acquire_lease",
			Self::ReleaseLease(_) => "release_lease",
			Self::AdvanceEpoch(_) => "advance_epoch",
			Self::SchemaUpdate(_) => "schema_update",
			Self::RetiredTierSlab(never)
			| Self::RetiredRetireTieredSlab(never)
			| Self::RetiredCompactionCommit(never) => match *never {},
			Self::SeedMigration(_) => "seed_migration",
			Self::MigrationComplete(_) => "migration_complete",
			Self::CreateTenant(_) => "create_tenant",
			Self::UpdateTenantQuota(_) => "update_tenant_quota",
			Self::SuspendTenant(_) => "suspend_tenant",
			Self::ResumeTenant(_) => "resume_tenant",
			Self::DeleteTenant(_) => "delete_tenant",
			Self::InstallTenantKekVersion(_) => "install_tenant_kek_version",
			Self::RetireTenantKekVersion(_) => "retire_tenant_kek_version",
			Self::RevokeActorKey(_) => "revoke_actor_key",
			Self::SetTableRetentionPolicy(_) => "set_table_retention_policy",
			Self::TableDropPrepare(_) => "table_drop_prepare",
			Self::TableReclaimCompletion(_) => "table_reclaim_completion",
			Self::TableDropFinalize(_) => "table_drop_finalize",
			Self::CtasSaga(_) => "ctas_saga",
			Self::ReplayLegacyCtasRowDispatched(_) => "replay_legacy_ctas_row_dispatched",
			Self::TableDropLogicalFinalize(_) => "table_drop_logical_finalize",
			Self::TableDropSettlement(_) => "table_drop_settlement",
			Self::Root2Command(_) => "root2_command",
			Self::RegisterIncarnation(_) => "register_incarnation",
			Self::SlabOp(_) => "slab_op",
			Self::CancelOp(_) => "cancel_op",
			Self::AckOutcomes(_) => "ack_outcomes",
			Self::Decision(_) => "decision",
			Self::AcquireLeases(_) => "acquire_leases",
		}
	}
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
	/// Committed and held: the Root2 carrier is dormant, not domain-applied.
	Root2Dormant,
	/// Refused without applying or recording anything (ADR 0697).
	Refused(SlabRefusal),
	/// Operation evidence (ADR 0697 §5).
	Op(crate::consensus::slab_ops::OpResponse),
	/// The result of a parent write decision (ADR 0699 S2).
	Decision(crate::consensus::decisions::DecisionResult),
	/// ADR 0701: one result per lease of an applied batch, in declared order.
	LeaseResults(LeaseGrantResults),
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
