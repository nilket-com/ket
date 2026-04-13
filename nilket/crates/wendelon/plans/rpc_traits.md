5. Rust RPC Service Traits (Replacing gRPC Service Defs)
rust


Copy
use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;
use bytes::Bytes;

// ============================================================
//  data ring TRAIT
//  Hot-path RPCs. Implementors: LeaderDataRing, FollowerProxy.
//  Transport: QUIC bidirectional streams via quinn.
//  Recommended: keepalive = 10s, max_frame = 5 MiB.
// ============================================================
#[async_trait::async_trait]
pub trait DataRing: Send + Sync + 'static {
    /// Unary write. Returns after commit (or cache hit for duplicates).
    /// The Arrow RecordBatch contains a single-row key+value batch.
    /// On DUPLICATE, returns the original cached response byte-for-byte.
    async fn write(
        &self,
        meta: WriteRequestMeta,
        payload: RecordBatch,          // 1-row: write_payload_schema()
    ) -> (WriteResponseMeta, Option<FinalizationMarker>);

    /// Unary read with consistency options.
    /// Returns metadata + optional Arrow value batch (None on KEY_NOT_FOUND).
    async fn read(
        &self,
        meta: ReadRequestMeta,
        key: RecordBatch,              // 1-row: read_key_schema()
    ) -> (ReadResponseMeta, Option<RecordBatch>);  // read_value_schema()

    /// Server-streaming: leader pushes FinalizationMarkers as entries
    /// are committed. Client subscribes starting from a log_index.
    ///
    /// The stream ends with a sentinel marker (epoch=0) if the
    /// epoch changes (client must resubscribe to new leader).
    ///
    /// Backpressure: bounded channel; leader drops slowest subscriber
    /// after 10s of blocked sends (subscriber reconnects from
    /// last_received_log_index).
    async fn stream_finalization(
        &self,
        start_log_index: u64,
        epoch: u64,
    ) -> mpsc::Receiver<FinalizationMarker>;

    /// Client→server streaming bulk write.
    /// Arrow RecordBatches have N rows each (N ≤ 1024).
    /// Responses are 1:1 per input row.
    ///
    /// Leader commits rows in batch-order, emitting one
    /// WriteResponseMeta per row on the response channel.
    async fn bulk_write(
        &self,
        requests: mpsc::Receiver<(WriteRequestMeta, RecordBatch)>,
    ) -> mpsc::Receiver<WriteResponseMeta>;
}

// ============================================================
//  control ring TRAIT
//  Administrative RPCs. Rate-limited. Separate QUIC endpoint
//  with mTLS on a dedicated port.
// ============================================================

#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct TransferLeadershipRequestMeta {
    pub target: NodeId,                     //  8 bytes
    /// Max time to complete transfer; default 5000ms
    pub timeout_ms: u32,                    //  4 bytes
    pub _pad: [u8; 4],                      //  4 bytes
}

#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct TransferLeadershipResponseMeta {
    /// 1 = success, 0 = failure
    pub success: u8,
    pub _pad: [u8; 7],
    pub new_epoch: u64,
    // TransferToken delivered as trailing struct in the frame
    // Error detail (if any) delivered as a UTF-8 Arrow payload
}

#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct AcquireLeaseRequestMeta {
    pub epoch: u64,
    /// Requested duration in ms; server may cap
    pub duration_ms: u32,
    pub _pad: [u8; 4],
}

#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct AcquireLeaseResponseMeta {
    /// 1 = granted, 0 = denied
    pub granted: u8,
    pub _pad: [u8; 7],
    // LeaseDescriptor delivered as trailing struct
    // Error detail in Arrow payload
}

#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct ClusterStatusResponseMeta {
    pub current_epoch: u64,
    pub current_term: u64,
    pub leader: NodeId,
    pub commit_index: u64,
    pub e_safe_index: u64,
    pub voter_count: u32,
    pub learner_count: u32,
    // LeaseDescriptor as trailing struct
    // Voter/learner lists as Arrow payload (epoch_entry_var_schema)
}

#[async_trait::async_trait]
pub trait ControlRing: Send + Sync + 'static {
    /// Initiate graceful leadership transfer.
    /// Leader creates TransferToken, replicates, steps down.
    async fn transfer_leadership(
        &self,
        req: TransferLeadershipRequestMeta,
    ) -> (TransferLeadershipResponseMeta, Option<TransferToken>);

    /// Request or renew a read lease.
    async fn acquire_lease(
        &self,
        req: AcquireLeaseRequestMeta,
    ) -> (AcquireLeaseResponseMeta, Option<LeaseDescriptor>);

    /// Force epoch advance (config changes, shard reassignment).
    /// Voter/learner lists and metadata map arrive as Arrow payload.
    async fn advance_epoch(
        &self,
        config: RecordBatch,           // epoch_entry_var_schema()
    ) -> (RaftEpochEntryFixed, RecordBatch);  // echoed back

    /// Query cluster health and current epoch state.
    async fn get_cluster_status(
        &self,
    ) -> (ClusterStatusResponseMeta, Option<LeaseDescriptor>, RecordBatch);
}
