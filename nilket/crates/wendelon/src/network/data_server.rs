//! Data Ring server (ADR 003 — the Stellarator Model).
//!
//! [`WendelonDataServer`] implements the [`DataRing`] trait by
//! bridging the two rings: every client write lands on local NVMe
//! *first* via [`SlabManager`], and only after the `io_uring`
//! append completes does a lightweight [`FinalizationMarker`]
//! enter the Control Ring's Raft log via `raft.client_write(...)`.
//!
//! This keeps the Stellarator invariant honest: Arrow IPC payloads
//! never cross the Raft log; only finalization metadata does, so
//! the Control Ring commit cadence is decoupled from the bulk data
//! rate.
//!
//! ## V1 scope (deliberately thin)
//!
//! * `write` runs at RF=1 — no replica fan-out yet. ADR 004's
//!   quorum path will splice in above the slab append without
//!   moving the Raft handoff.
//! * `read` serves from the local slab index only. Cross-node
//!   routing and follower forwarding live above this layer.
//! * `bulk_write` and `stream_finalization` return empty
//!   receivers — the surface compiles, the wiring lands with the
//!   subscriber bus.
//!
//! ## Idempotency
//!
//! A [`DashMap`] keyed on `(client_uuid, sequence_no)` deduplicates
//! retries. A hit replays the original [`WriteResponseMeta`] with
//! `status` overwritten to [`WriteStatus::DUPLICATE`] so the client
//! can tell a fresh success apart from a replay. The TTL and ack
//! window from ADR 003 §3 are tracked as follow-ups; today the
//! cache grows monotonically until process restart.

use crate::consensus::{WendelonRaft, WendelonRequest, WendelonStateMachine};
use crate::schema::{
    Consistency, FinalizationMarker, HybridTimestamp, NodeId, ReadRequestMeta, ReadResponseMeta,
    ReadStatus, WriteRequestMeta, WriteResponseMeta, WriteStatus,
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

/// Data Ring front door. Handles the two ADR-critical paths:
/// disk-first write with Raft-backed finalization, and local
/// linearizable / serializable / stale reads.
pub struct WendelonDataServer {
    slab: Arc<SlabManager>,
    raft: WendelonRaft,
    /// Shared SM handle. Needed so `read` can consult the E_safe
    /// watermark for the epoch the client is asking against; also
    /// hints the "current epoch" for write-side validation.
    state_machine: WendelonStateMachine,
    /// Local node identity, populated into every response's
    /// `leader_hint` until we have a cheap async way to pull the
    /// real current leader from `raft.current_leader()` on the
    /// hot path.
    local_node: NodeId,
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
    ) -> Self {
        Self {
            slab,
            raft,
            state_machine,
            local_node,
            idempotency: DashMap::new(),
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

        // 5. Serialize the RecordBatch back to Arrow IPC — these
        // bytes are what land in the slab and get returned verbatim
        // on reads. Storing the whole batch (rather than just the
        // value) keeps the on-disk record self-describing so future
        // compaction and cross-tier migration can operate on it
        // without rehydrating via the SM.
        let frame_bytes = encode_batch(&payload)?;

        // 6. HLC for this write. The real impl merges `leader_hlc`
        // with `meta.client_hlc` and bumps logical; V1 carries the
        // client HLC straight through, which is correct for
        // single-node and good-enough for the first multi-node
        // bring-up.
        let commit_hlc = meta.client_hlc;

        // 7. Disk append. io_uring CQE for the write completes
        // before we return here — the storage engine awaits it
        // internally.
        let location = self.slab.append(key_hash, commit_hlc, &frame_bytes).await?;

        // 8. Build the finalization marker. `log_index` / `term`
        // are unknown until Raft places the entry; we submit with
        // zeros and fill them from the ClientWriteResponse below.
        // The SM's `apply_request` only reads `epoch` and
        // `commit_hlc`, so the placeholder zeros are safe.
        let mut marker = FinalizationMarker {
            log_index: 0,
            term: 0,
            epoch: meta.expected_epoch,
            commit_hlc,
            payload_hash: blake3_32(&frame_bytes),
            e_safe_index: 0,
            _reserved: 0,
        };

        // 9. Raft handoff — commits the finalization across the
        // Control Ring. Disk is already durable at this point, so
        // a transient Raft failure surfaces as INTERNAL_ERROR to
        // the client but doesn't cost us the stored bytes (they
        // remain at `location` for the client's next retry).
        let (status, log_index, term) = match self.raft.client_write(
            WendelonRequest::Finalize(marker),
        ).await
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

        // 10. Response meta, cached for future idempotent replays.
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
        // Unused binding — `payload` itself is no longer needed
        // past step 7. Letting it drop here releases the Arrow
        // buffers before the Raft client_write await.
        let _ = location;
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
            // Per-record log index tracking isn't plumbed into the
            // slab index yet; surface 0 as a sentinel and fill it
            // once the Data Ring finalization side plumbs the
            // committed log_index back into the slab record.
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
        // Subscriber bus lands with the finalization-broadcast task
        // that tails the SM's apply stream. For now return a closed
        // channel so callers see a clean EOF instead of the handler
        // panicking on unwrap.
        let (_tx, rx) = mpsc::channel(1);
        Ok(rx)
    }

    async fn bulk_write(
        &self,
        _requests: mpsc::Receiver<(WriteRequestMeta, RecordBatch)>,
    ) -> std::io::Result<mpsc::Receiver<WriteResponseMeta>> {
        // Batched fast path is deferred — a correct but unpipelined
        // fallback would drive `write` serially over the receiver,
        // which defeats the purpose. Return an empty channel with
        // a TODO so the trait surface stays complete without
        // silently shipping a slow path.
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
