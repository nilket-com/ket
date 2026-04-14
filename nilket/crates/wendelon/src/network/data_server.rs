//! Data Ring server (ADR 003 — the Stellarator Model).
//!
//! [`WendelonDataServer`] implements the [`DataRing`] trait by
//! bifurcating every client write: the Arrow IPC payload is
//! replicated directly to the data replica set over HRFT streams
//! (ADR 004) and only an 88-byte [`FinalizationMarker`] enters the
//! Raft Control Ring to advance the `E_safe` watermark.
//!
//! ## Why fan-out lives above the slab
//!
//! The slab is the durability boundary for *one* node; quorum is
//! the durability boundary for *the write*. The fan-out wraps
//! [`SlabManager::append`] so the local append and the remote
//! replica appends both count toward quorum under the same
//! majority rule. Payload bytes never enter the Raft log — only
//! the finalization marker does, per the Stellarator invariant.
//!
//! ## V1 scope
//!
//! * Static replica set resolved through [`ReplicaRouter`]. A real
//!   deployment will feed this from the ADR 002 Unified Hash Ring
//!   projection off the Raft `ring(E)` snapshot; today we accept
//!   any `Arc<dyn ReplicaRouter>` so tests can inject a trivial
//!   in-memory topology.
//! * Concurrent dials through [`TransportProviderFactory`]. The
//!   factory already collapses duplicate dials per peer, so we
//!   spawn one task per remote replica without extra coordination.
//! * Quorum = `(RF / 2) + 1`. If the local node is in the replica
//!   set, its slab append counts as one ack.
//!
//! ## Idempotency
//!
//! A [`DashMap`] keyed on `(client_uuid, sequence_no)` deduplicates
//! retries. A hit replays the original [`WriteResponseMeta`] with
//! `status` overwritten to [`WriteStatus::DUPLICATE`] so the client
//! can tell a fresh success apart from a replay.

use crate::consensus::{WendelonRaft, WendelonRequest, WendelonStateMachine};
use crate::network::core::{read_frame, write_frame, HRFT_MAGIC};
use crate::network::factory::TransportProviderFactory;
use crate::network::pool::BufPool;
use crate::network::router::DataRingServer;
use crate::schema::{
    Consistency, FinalizationMarker, FrameHeader, HybridTimestamp, MsgType, NodeId,
    ReadRequestMeta, ReadResponseMeta, ReadStatus, ReplicaWriteMeta, ReplicaWriteRespMeta,
    ReplicaWriteStatus, WriteRequestMeta, WriteResponseMeta, WriteStatus,
};
use crate::storage::core::StorageEngine;
use crate::storage::manager::SlabManager;
use arrow::array::{Array, LargeBinaryArray, LargeBinaryBuilder};
use arrow::datatypes::Schema;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use dashmap::DashMap;
use highway::{HighwayHash, HighwayHasher, Key as HighwayKey};
use std::io::{Cursor, Error, ErrorKind};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::network::core::HrftFrame;
use crate::network::traits::DataRing;

/// HighwayHash seed. Stable for the lifetime of the process —
/// a real deployment derives this from the ring seed recorded in
/// `RaftEpochEntry` metadata so every node hashes keys the same
/// way; for V1 we pick a fixed constant and document the bind.
const HIGHWAY_KEY: HighwayKey = HighwayKey([
    0x5745_4e44_454c_4f4e,
    0x4841_5348_4b45_5901,
    0x4441_5441_5249_4e47,
    0x5354_454c_4c41_524f,
]);

type IdempotencyKey = ([u8; 16], u64);

// ============================================================
//  REPLICA ROUTER
// ============================================================

/// Resolves a `key_hash` to the replica set that should store it.
///
/// The contract is deliberately narrow: return the ordered list of
/// `NodeId`s that form the replica set for this key. The ordering
/// is not consulted by the fan-out (every member must ack), but a
/// stable ordering helps tracing and topology tooling line up.
///
/// A real implementation projects off the Raft `ring(E)` snapshot
/// and filters for data-capable nodes (ADR 002). The V1 fallback
/// is [`StaticReplicaRouter`].
pub trait ReplicaRouter: Send + Sync {
    /// Members of the replica set for `key_hash`. The order is
    /// stable for a given topology so traces align; callers must
    /// not depend on the first entry being the local node.
    fn replicas(&self, key_hash: u64) -> Vec<NodeId>;

    /// Effective replication factor for this router. Exposed so the
    /// quorum math is source-of-truthed in one place.
    fn replication_factor(&self) -> usize;
}

/// Static ring router. Holds the full node list and an RF; resolves
/// each key's replica set as `RF` consecutive nodes starting at
/// `key_hash % nodes.len()`. This is a stand-in for the full ADR 002
/// projection — good enough for a bring-up where membership is
/// stable, fine for tests, and will be swapped for a real snapshot
/// reader once the Unified Hash Ring projection lands.
pub struct StaticReplicaRouter {
    nodes: Vec<NodeId>,
    rf: usize,
}

impl StaticReplicaRouter {
    /// Build a router over `nodes` with replication factor `rf`.
    /// The node list is sorted by numeric id for deterministic
    /// ordering; duplicate ids are deduplicated.
    pub fn new(mut nodes: Vec<NodeId>, rf: usize) -> Self {
        nodes.sort_by_key(|n| n.id);
        nodes.dedup_by_key(|n| n.id);
        let rf = rf.max(1);
        Self { nodes, rf }
    }
}

impl ReplicaRouter for StaticReplicaRouter {
    fn replicas(&self, key_hash: u64) -> Vec<NodeId> {
        if self.nodes.is_empty() {
            return Vec::new();
        }
        let n = self.nodes.len();
        let take = self.rf.min(n);
        let start = (key_hash as usize) % n;
        (0..take)
            .map(|i| self.nodes[(start + i) % n])
            .collect()
    }

    fn replication_factor(&self) -> usize {
        self.rf.min(self.nodes.len().max(1))
    }
}

// ============================================================
//  SERVER
// ============================================================

/// Data Ring front door. Owns the local slab, a client to the Raft
/// Control Ring, and everything needed to run the bifurcated write
/// path: a [`ReplicaRouter`] to resolve the replica set and a
/// [`TransportProviderFactory`] to open HRFT streams to remote
/// replicas.
pub struct WendelonDataServer {
    slab: Arc<SlabManager>,
    raft: WendelonRaft,
    /// Shared SM handle. Needed so `read` can consult the E_safe
    /// watermark for the epoch the client is asking against; also
    /// hints the "current epoch" for write-side validation.
    state_machine: WendelonStateMachine,
    /// Local node identity, populated into every response's
    /// `leader_hint` and used to decide whether the local slab
    /// append counts toward quorum on a given key.
    local_node: NodeId,
    /// Resolves a `key_hash` to the replica set that should hold
    /// it. Injected so the projection source (Unified Hash Ring
    /// snapshot, static dev topology, test mock) is the caller's
    /// concern, not this file's.
    router: Arc<dyn ReplicaRouter>,
    /// Shared connection factory. Caches sessions per `NodeId`,
    /// negotiates tiers, and collapses duplicate concurrent dials
    /// so spawning one fan-out task per peer is safe without extra
    /// locks here.
    factory: Arc<TransportProviderFactory>,
    /// Shared buffer pool. Inbound REPLICATE frames draw their
    /// payload sections from it and the response `meta` buffer is
    /// encoded into a pooled Vec on the reply path.
    pool: Arc<BufPool>,
    /// Exactly-once cache. `WriteResponseMeta` is `Copy`, so
    /// lookups are O(1) under DashMap's shard striping.
    idempotency: DashMap<IdempotencyKey, WriteResponseMeta>,
}

impl WendelonDataServer {
    pub fn new(
        slab: Arc<SlabManager>,
        raft: WendelonRaft,
        state_machine: WendelonStateMachine,
        local_node: NodeId,
        router: Arc<dyn ReplicaRouter>,
        factory: Arc<TransportProviderFactory>,
        pool: Arc<BufPool>,
    ) -> Self {
        Self {
            slab,
            raft,
            state_machine,
            local_node,
            router,
            factory,
            pool,
            idempotency: DashMap::new(),
        }
    }

    /// Fan-out write to a single remote replica. Opens one bidi
    /// stream, ships the REPLICATE_REQ frame, and awaits the
    /// REPLICATE_RESP. Every failure — dial, send, protocol,
    /// negative ack — surfaces as `Err` so the caller can simply
    /// count `Ok` results as acks.
    async fn replicate_to_remote(
        factory: Arc<TransportProviderFactory>,
        pool: Arc<BufPool>,
        target: NodeId,
        meta: ReplicaWriteMeta,
        arrow_bytes: Arc<Vec<u8>>,
    ) -> std::io::Result<()> {
        let conn = factory.get_connection(target).await?;
        let mut stream = conn.open_bi().await?;

        let meta_bytes = bytemuck::bytes_of(&meta);
        let header = FrameHeader {
            magic: HRFT_MAGIC,
            version: 1,
            msg_type: MsgType::REPLICATE_REQ.0,
            flags: 0,
            stream_id: 0,
            meta_len: meta_bytes.len() as u32,
            arrow_len: arrow_bytes.len() as u32,
            trailing_len: 0,
        };
        write_frame(
            &mut *stream,
            &header,
            meta_bytes,
            Some(arrow_bytes.as_slice()),
            None,
        )
        .await?;
        stream
            .finish()
            .await
            .map_err(|e| Error::new(ErrorKind::BrokenPipe, e))?;

        let resp = read_frame(&mut *stream, &pool).await?;
        if resp.header.msg_type != MsgType::REPLICATE_RESP.0 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "expected REPLICATE_RESP, got msg_type=0x{:02x}",
                    resp.header.msg_type
                ),
            ));
        }
        if resp.meta.len() < std::mem::size_of::<ReplicaWriteRespMeta>() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "REPLICATE_RESP meta too small",
            ));
        }
        let ack: &ReplicaWriteRespMeta =
            bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<ReplicaWriteRespMeta>()]);
        if ack.status != ReplicaWriteStatus::OK {
            return Err(Error::new(
                ErrorKind::Other,
                format!("replica {target:?} rejected write: status={}", ack.status.0),
            ));
        }
        Ok(())
    }

    /// Handle an inbound REPLICATE_REQ frame: decode the POD
    /// metadata, verify the payload hash, append to the local
    /// slab, and build the REPLICATE_RESP frame. Surfaces both
    /// transient and logical failures via the `status` byte rather
    /// than tearing down the HRFT stream — the sender's ack count
    /// is the only signal that matters for quorum.
    async fn handle_replicate(&self, frame: HrftFrame) -> std::io::Result<HrftFrame> {
        if frame.meta.len() < std::mem::size_of::<ReplicaWriteMeta>() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "REPLICATE_REQ meta too small",
            ));
        }
        let meta: ReplicaWriteMeta = *bytemuck::from_bytes(
            &frame.meta[..std::mem::size_of::<ReplicaWriteMeta>()],
        );
        let arrow = frame.arrow_payload.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidData,
                "REPLICATE_REQ missing arrow payload",
            )
        })?;

        let arrow_bytes: &[u8] = &arrow[..];
        let status = if blake3_32(arrow_bytes) != meta.payload_hash {
            ReplicaWriteStatus::HASH_MISMATCH
        } else {
            match self
                .slab
                .append(meta.key_hash, meta.commit_hlc, arrow_bytes)
                .await
            {
                Ok(_) => ReplicaWriteStatus::OK,
                Err(_) => ReplicaWriteStatus::INTERNAL_ERROR,
            }
        };

        let resp = ReplicaWriteRespMeta {
            status,
            _pad: [0; 7],
        };
        let mut body = self.pool.acquire();
        body.as_mut_vec()
            .extend_from_slice(bytemuck::bytes_of(&resp));
        let header = FrameHeader {
            magic: HRFT_MAGIC,
            version: 1,
            msg_type: MsgType::REPLICATE_RESP.0,
            flags: 0,
            stream_id: frame.header.stream_id,
            meta_len: body.len() as u32,
            arrow_len: 0,
            trailing_len: 0,
        };
        Ok(HrftFrame {
            header,
            meta: body,
            arrow_payload: None,
            trailing: None,
        })
    }
}

// ============================================================
//  DATA RING SERVER (INBOUND DISPATCH)
// ============================================================

#[async_trait]
impl DataRingServer for WendelonDataServer {
    async fn handle_data_frame(&self, frame: HrftFrame) -> std::io::Result<Option<HrftFrame>> {
        let msg_type = MsgType(frame.header.msg_type);
        match msg_type {
            MsgType::REPLICATE_REQ => self.handle_replicate(frame).await.map(Some),
            other => Err(Error::new(
                ErrorKind::Unsupported,
                format!(
                    "Data Ring handler has no decoder for msg_type=0x{:02x}",
                    other.0
                ),
            )),
        }
    }
}

#[async_trait]
impl DataRing for WendelonDataServer {
    async fn write(
        &self,
        meta: WriteRequestMeta,
        payload: RecordBatch,
    ) -> std::io::Result<(WriteResponseMeta, Option<FinalizationMarker>)> {
        // 1. Proxy-hop guard — two-hop cap per ADR 003 §2.
        if meta.proxy_hop >= 2 {
            return Ok((error_response(&meta, WriteStatus::PROXY_LIMIT, self.local_node), None));
        }

        // 2. Epoch validation. The authoritative current epoch is
        // the highest epoch the SM has seen an E_safe advancement
        // for; a client that believes in an older epoch gets a
        // redirect, not a silent accept.
        let sm = self.state_machine.state_snapshot().await;
        let current_epoch = sm.e_safe_watermarks.keys().copied().max().unwrap_or(0);
        if meta.expected_epoch < current_epoch {
            return Ok((error_response(&meta, WriteStatus::EPOCH_STALE, self.local_node), None));
        }

        // 3. Idempotency fast path.
        let key = (meta.write_id.client_uuid, meta.write_id.sequence_no);
        if let Some(cached) = self.idempotency.get(&key) {
            let mut resp = *cached.value();
            resp.status = WriteStatus::DUPLICATE;
            return Ok((resp, None));
        }

        // 4. Pull the key out of the Arrow payload and hash it.
        let key_bytes = extract_first_binary(&payload, "key")?;
        let key_hash = highway_hash(key_bytes);

        // 5. Serialize the RecordBatch back to Arrow IPC. These
        // bytes are what both the local slab and every remote
        // replica store verbatim — hashing once here means every
        // ack is computed against the same authoritative blob.
        let frame_bytes = Arc::new(encode_batch(&payload)?);
        let payload_hash = blake3_32(frame_bytes.as_slice());

        // 6. HLC for this write. The real impl merges `leader_hlc`
        // with `meta.client_hlc` and bumps logical; V1 carries the
        // client HLC straight through.
        let commit_hlc = meta.client_hlc;

        // 7. Resolve the replica set. An empty set is a
        // configuration bug — refuse rather than silently dropping
        // to a pre-Stellarator single-node append.
        let replicas = self.router.replicas(key_hash);
        if replicas.is_empty() {
            return Ok((
                error_response(&meta, WriteStatus::INTERNAL_ERROR, self.local_node),
                None,
            ));
        }
        let rf = replicas.len();
        let quorum = (rf / 2) + 1;

        // 8. Local append (if this node is in the replica set) and
        // concurrent fan-out to the remaining replicas. Each remote
        // leg runs on its own task so head-of-line blocking on any
        // single peer only slows *that* leg — the quorum race still
        // completes as soon as the first majority of acks arrive.
        let mut acks: usize = 0;
        let local_is_replica = replicas.iter().any(|n| n.id == self.local_node.id);
        if local_is_replica {
            // The io_uring append is the hot path — drive it first
            // so the local leg is usually the fastest ack.
            match self
                .slab
                .append(key_hash, commit_hlc, frame_bytes.as_slice())
                .await
            {
                Ok(_) => acks += 1,
                Err(e) => {
                    // A local durability failure is terminal — we
                    // cannot finalize a write we could not even put
                    // on our own NVMe. Propagate rather than falling
                    // back to a pure remote quorum, which would make
                    // the local index silently diverge.
                    return Err(e);
                }
            }
        }

        let replica_meta = ReplicaWriteMeta {
            commit_hlc,
            epoch: meta.expected_epoch,
            key_hash,
            payload_hash,
        };
        let mut join: JoinSet<std::io::Result<()>> = JoinSet::new();
        for target in replicas.iter().copied() {
            if target.id == self.local_node.id {
                continue;
            }
            let factory = self.factory.clone();
            let pool = self.pool.clone();
            let bytes = frame_bytes.clone();
            join.spawn(async move {
                Self::replicate_to_remote(factory, pool, target, replica_meta, bytes).await
            });
        }

        // 9. Quorum race. Stop polling as soon as the majority is
        // in — outstanding tasks keep running to completion so
        // slower replicas still catch up, but we do not block the
        // client on them.
        while acks < quorum {
            match join.join_next().await {
                Some(Ok(Ok(()))) => acks += 1,
                Some(_) => { /* leg failed — keep racing */ }
                None => break, // no more remote legs in flight
            }
        }

        if acks < quorum {
            return Ok((
                error_response(&meta, WriteStatus::INTERNAL_ERROR, self.local_node),
                None,
            ));
        }

        // 10. Finalization. `log_index` / `term` are unknown until
        // Raft places the entry; submit with zeros and fill them
        // from the ClientWriteResponse below. The SM's
        // `apply_request` only reads `epoch` and `commit_hlc`, so
        // the placeholder zeros are safe.
        let mut marker = FinalizationMarker {
            log_index: 0,
            term: 0,
            epoch: meta.expected_epoch,
            commit_hlc,
            payload_hash,
            e_safe_index: 0,
            _reserved: 0,
        };

        let (status, log_index, term) = match self
            .raft
            .client_write(WendelonRequest::Finalize(marker))
            .await
        {
            Ok(resp) => (
                WriteStatus::OK,
                resp.log_id.index,
                resp.log_id.leader_id.term,
            ),
            Err(_) => (WriteStatus::INTERNAL_ERROR, 0, 0),
        };
        marker.log_index = log_index;
        marker.term = term;
        marker.e_safe_index = log_index;

        // 11. Response meta, cached for future idempotent replays.
        let response = WriteResponseMeta {
            status,
            _pad1: [0; 7],
            log_index,
            term,
            epoch: meta.expected_epoch,
            commit_hlc,
            leader_hint: self.local_node,
            echoed_write_id: meta.write_id,
            has_finalization: if meta.await_finalization != 0 { 1 } else { 0 },
            _pad2: [0; 7],
        };
        self.idempotency.insert(key, response);

        let trailing = if meta.await_finalization != 0 && status == WriteStatus::OK {
            Some(marker)
        } else {
            None
        };
        Ok((response, trailing))
    }

    async fn read(
        &self,
        meta: ReadRequestMeta,
        key: RecordBatch,
    ) -> std::io::Result<(ReadResponseMeta, Option<RecordBatch>)> {
        // 1. Consistency gate. LINEARIZABLE requires the local SM
        // to have observed an E_safe advancement in this epoch at
        // or beyond the client's min_hlc. SERIALIZABLE and STALE
        // skip the gate — the former trusts the leader's commit
        // frontier, the latter explicitly accepts stale data.
        if meta.consistency == Consistency::LINEARIZABLE {
            let sm = self.state_machine.state_snapshot().await;
            let watermark = sm
                .e_safe_watermarks
                .get(&meta.expected_epoch)
                .copied()
                .unwrap_or(HybridTimestamp {
                    physical_ms: 0,
                    logical: 0,
                    node_id: 0,
                });
            if watermark < meta.min_hlc {
                return Ok((
                    error_read_response(&meta, ReadStatus::E_SAFE_BEHIND, self.local_node),
                    None,
                ));
            }
        }

        // 2. Hash the key payload and probe the slab index.
        let key_bytes = extract_first_binary(&key, "key")?;
        let key_hash = highway_hash(key_bytes);
        let Some(location) = self.slab.lookup(key_hash) else {
            return Ok((
                error_read_response(&meta, ReadStatus::KEY_NOT_FOUND, self.local_node),
                None,
            ));
        };

        // 3. Fetch the stored Arrow bytes and re-materialise them
        // into a RecordBatch. We then project out just the `value`
        // column, shaping the response to `read_value_schema`.
        let bytes = self.slab.read(location).await?;
        let stored = decode_batch(&bytes)?;
        let value_bytes = extract_first_binary(&stored, "value")?;
        let value_batch = build_value_batch(value_bytes)?;
        let value_len = value_bytes.len() as u32;

        let response = ReadResponseMeta {
            status: ReadStatus::OK,
            _pad1: [0; 7],
            value_hlc: location.hlc,
            read_at_index: 0,
            epoch: meta.expected_epoch,
            leader_hint: self.local_node,
            value_len,
            _pad2: [0; 4],
        };
        Ok((response, Some(value_batch)))
    }

    async fn stream_finalization(
        &self,
        _start_log_index: u64,
        _epoch: u64,
    ) -> std::io::Result<mpsc::Receiver<FinalizationMarker>> {
        let (_tx, rx) = mpsc::channel(1);
        Ok(rx)
    }

    async fn bulk_write(
        &self,
        _requests: mpsc::Receiver<(WriteRequestMeta, RecordBatch)>,
    ) -> std::io::Result<mpsc::Receiver<WriteResponseMeta>> {
        let (_tx, rx) = mpsc::channel(1);
        Ok(rx)
    }
}

// ============================================================
//  ERROR RESPONSE HELPERS
// ============================================================

fn error_response(
    meta: &WriteRequestMeta,
    status: WriteStatus,
    leader_hint: NodeId,
) -> WriteResponseMeta {
    WriteResponseMeta {
        status,
        _pad1: [0; 7],
        log_index: 0,
        term: 0,
        epoch: meta.expected_epoch,
        commit_hlc: meta.client_hlc,
        leader_hint,
        echoed_write_id: meta.write_id,
        has_finalization: 0,
        _pad2: [0; 7],
    }
}

fn error_read_response(
    meta: &ReadRequestMeta,
    status: ReadStatus,
    leader_hint: NodeId,
) -> ReadResponseMeta {
    ReadResponseMeta {
        status,
        _pad1: [0; 7],
        value_hlc: HybridTimestamp {
            physical_ms: 0,
            logical: 0,
            node_id: 0,
        },
        read_at_index: 0,
        epoch: meta.expected_epoch,
        leader_hint,
        value_len: 0,
        _pad2: [0; 4],
    }
}

// ============================================================
//  ARROW / HASH HELPERS
// ============================================================

/// Read the first value of a named `LargeBinary` column from a
/// single-row RecordBatch. Both `write_payload_schema` and
/// `read_key_schema` are single-row / LargeBinary-shaped, so this
/// covers every Data Ring request variant today.
fn extract_first_binary<'a>(
    batch: &'a RecordBatch,
    column: &'static str,
) -> std::io::Result<&'a [u8]> {
    let col = batch
        .column_by_name(column)
        .ok_or_else(|| Error::new(ErrorKind::InvalidData, format!("missing '{column}' column")))?;
    let arr = col.as_any().downcast_ref::<LargeBinaryArray>().ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidData,
            format!("'{column}' column is not LargeBinary"),
        )
    })?;
    if arr.is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!("'{column}' column is empty"),
        ));
    }
    Ok(arr.value(0))
}

/// Encode a RecordBatch as an Arrow IPC stream.
fn encode_batch(batch: &RecordBatch) -> std::io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())
            .map_err(|e| Error::new(ErrorKind::InvalidData, format!("arrow stream init: {e}")))?;
        writer
            .write(batch)
            .map_err(|e| Error::new(ErrorKind::InvalidData, format!("arrow stream write: {e}")))?;
        writer
            .finish()
            .map_err(|e| Error::new(ErrorKind::InvalidData, format!("arrow stream finish: {e}")))?;
    }
    Ok(buf)
}

/// Inverse of [`encode_batch`]. Returns the first batch in the
/// stream — Data Ring payloads are always single-batch today.
fn decode_batch(bytes: &[u8]) -> std::io::Result<RecordBatch> {
    let mut reader = StreamReader::try_new(Cursor::new(bytes), None)
        .map_err(|e| Error::new(ErrorKind::InvalidData, format!("arrow stream open: {e}")))?;
    match reader.next() {
        Some(Ok(batch)) => Ok(batch),
        Some(Err(e)) => Err(Error::new(
            ErrorKind::InvalidData,
            format!("arrow stream read: {e}"),
        )),
        None => Err(Error::new(
            ErrorKind::InvalidData,
            "arrow stream contained no record batch",
        )),
    }
}

/// Wrap raw value bytes in a one-row RecordBatch shaped to
/// `read_value_schema` so callers speak the same dialect on write
/// and read paths.
fn build_value_batch(value: &[u8]) -> std::io::Result<RecordBatch> {
    let mut builder = LargeBinaryBuilder::new();
    builder.append_value(value);
    let array = builder.finish();
    let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
        "value",
        arrow::datatypes::DataType::LargeBinary,
        true,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(array)])
        .map_err(|e| Error::new(ErrorKind::InvalidData, format!("build value batch: {e}")))
}

fn highway_hash(bytes: &[u8]) -> u64 {
    let mut hasher = HighwayHasher::new(HIGHWAY_KEY);
    hasher.append(bytes);
    hasher.finalize64()
}

fn blake3_32(bytes: &[u8]) -> [u8; 32] {
    *blake3::hash(bytes).as_bytes()
}

// ============================================================
//  TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn n(id: u32) -> NodeId {
        NodeId { id, _reserved: 0 }
    }

    #[test]
    fn static_router_wraps_around_ring() {
        let router = StaticReplicaRouter::new(vec![n(1), n(2), n(3), n(4)], 3);
        // start index 3 → {3, 0, 1} = nodes 4, 1, 2
        let replicas = router.replicas(3);
        assert_eq!(replicas.len(), 3);
        assert_eq!(replicas[0], n(4));
        assert_eq!(replicas[1], n(1));
        assert_eq!(replicas[2], n(2));
    }

    #[test]
    fn static_router_dedups_and_sorts() {
        let router =
            StaticReplicaRouter::new(vec![n(3), n(1), n(3), n(2)], 5);
        // RF clamped to available node count (3).
        let replicas = router.replicas(0);
        assert_eq!(replicas.len(), 3);
        assert_eq!(replicas[0], n(1));
        assert_eq!(replicas[1], n(2));
        assert_eq!(replicas[2], n(3));
    }

    #[test]
    fn static_router_rf_one_is_single_replica() {
        let router = StaticReplicaRouter::new(vec![n(1), n(2), n(3)], 1);
        assert_eq!(router.replicas(0).len(), 1);
    }

    #[test]
    fn static_router_empty_node_list_returns_empty() {
        let router = StaticReplicaRouter::new(vec![], 3);
        assert!(router.replicas(0).is_empty());
    }
}
