use crate::schema::{
    AcquireLeaseRequestMeta, AcquireLeaseResponseMeta, FinalizationMarker, LeaseDescriptor, NodeId,
    RaftEpochEntryFixed, ReadRequestMeta, ReadResponseMeta, TransferToken, WriteRequestMeta,
    WriteResponseMeta,
};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use tokio::sync::mpsc;

// ============================================================
//  CONTROL-RING META — placeholder (non-wire) structs.
//
//  `TransferLeadershipRequest/ResponseMeta` and
//  `ClusterStatusResponseMeta` are still plain-Rust stubs — the
//  control-ring wire layout for those flows is not yet frozen.
//  Lease metadata moved to `src/schema/core.rs` as part of
//  Plan 018 (per-range leases); the POD versions are imported
//  above and referenced directly by the `ControlRing` trait.
// ============================================================

#[derive(Clone, Debug)]
pub struct TransferLeadershipRequestMeta {
    pub source_epoch: u64,
    pub from_leader: NodeId,
    pub to_leader: NodeId,
}

#[derive(Clone, Debug)]
pub struct TransferLeadershipResponseMeta {
    pub accepted: bool,
    /// Maps to ReasonCode table (TBD); 0 = granted.
    pub reason_code: u8,
}

#[derive(Clone, Debug)]
pub struct ClusterStatusResponseMeta {
    pub current_epoch: u64,
    pub leader: NodeId,
    pub vnode_count: u32,
    pub replication_factor: u32,
}

// ============================================================
//  DATA RING — bulk key/value traffic.
//
//  Per ADR 003 (Stellarator), payload bytes never enter the
//  Raft log. `write` returns a `FinalizationMarker` only when
//  the write crosses a finalization boundary; the marker is
//  what drives E_safe advancement on followers.
//
//  `read` is served locally on the leader and on followers
//  whose `e_safe_index` >= the key's last mutation index.
//
//  `stream_finalization` is consumed by followers and by
//  observability sidecars to keep a live view of commit
//  progress.
//
//  `bulk_write` is the batched fast path; implementations
//  SHOULD pipeline acknowledgements and MAY reorder responses
//  relative to request arrival order so long as each response
//  carries its originating `ClientWriteId`.
// ============================================================
#[async_trait]
pub trait DataRing: Send + Sync {
    async fn write(
        &self,
        meta: WriteRequestMeta,
        payload: RecordBatch,
    ) -> std::io::Result<(WriteResponseMeta, Option<FinalizationMarker>)>;

    async fn read(
        &self,
        meta: ReadRequestMeta,
        key: RecordBatch,
    ) -> std::io::Result<(ReadResponseMeta, Option<RecordBatch>)>;

    async fn stream_finalization(
        &self,
        start_log_index: u64,
        epoch: u64,
    ) -> std::io::Result<mpsc::Receiver<FinalizationMarker>>;

    async fn bulk_write(
        &self,
        requests: mpsc::Receiver<(WriteRequestMeta, RecordBatch)>,
    ) -> std::io::Result<mpsc::Receiver<WriteResponseMeta>>;
}

// ============================================================
//  CONTROL RING — leadership, leases, epochs, status.
//
//  Runs on a separate mTLS-gated port and is rate-limited.
//  Callers are cluster administrators or cooperating peer
//  nodes; never application clients.
//
//  `advance_epoch` accepts the new cluster config as an Arrow
//  RecordBatch (epoch_entry_var_schema) and returns the
//  finalized `RaftEpochEntryFixed` plus a RecordBatch
//  containing the materialized voter/learner set for caller
//  convenience.
//
//  `get_cluster_status` returns the status meta, the active
//  lease for the queried range (if any), and a RecordBatch
//  describing the current ring topology.
// ============================================================
#[async_trait]
pub trait ControlRing: Send + Sync {
    async fn transfer_leadership(
        &self,
        req: TransferLeadershipRequestMeta,
    ) -> std::io::Result<(TransferLeadershipResponseMeta, Option<TransferToken>)>;

    async fn acquire_lease(
        &self,
        req: AcquireLeaseRequestMeta,
    ) -> std::io::Result<(AcquireLeaseResponseMeta, Option<LeaseDescriptor>)>;

    async fn advance_epoch(
        &self,
        config: RecordBatch,
    ) -> std::io::Result<(RaftEpochEntryFixed, RecordBatch)>;

    async fn get_cluster_status(
        &self,
    ) -> std::io::Result<(ClusterStatusResponseMeta, Option<LeaseDescriptor>, RecordBatch)>;
}
