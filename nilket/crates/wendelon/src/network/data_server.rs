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

use crate::consensus::monitor::{NodeTracker, PhysicalClock};
use crate::consensus::{WendelonRaft, WendelonRequest, WendelonResponse, WendelonStateMachine};
use crate::network::core::{read_frame, write_frame, HRFT_MAGIC};
use crate::network::factory::TransportProviderFactory;
use crate::network::pool::BufPool;
use crate::network::ring::RingRouter;
use crate::network::router::DataRingServer;
use crate::network::transport::MembershipEpoch;
use crate::schema::{
    wpbc, Consistency, FinalizationMarker, FrameHeader, HybridTimestamp, LeaseDescriptor, MsgType,
    NodeId, QueryRequestMeta, QueryResponseMeta, ReadRequestMeta, ReadResponseMeta, ReadStatus,
    ReplicaWriteMeta, ReplicaWriteRespMeta, ReplicaWriteStatus, WriteRequestMeta,
    WriteResponseMeta, WriteStatus,
};
use crate::storage::core::StorageEngine;
use crate::storage::manager::SlabManager;
use arrow::array::{Array, LargeBinaryArray, LargeBinaryBuilder};
use arrow::compute::concat_batches;
use arrow::datatypes::Schema;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use dashmap::DashMap;
use highway::{HighwayHash, HighwayHasher, Key as HighwayKey};
use std::io::{Cursor, Error, ErrorKind};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};

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
    /// Optional liveness + drift tracker (ADR 001, ADR 012). When
    /// populated, every inbound peer-to-peer RPC feeds the peer's
    /// HLC physical component into it so the drift monitor and
    /// eviction watchdog can see the cluster.
    tracker: Option<Arc<NodeTracker>>,
    /// Local snapshot of leases granted to this node (plan 018).
    /// Every entry is a `LeaseDescriptor` the Raft Leader has
    /// already committed. The write path and the LINEARIZABLE
    /// read path consult this list before accepting traffic.
    ///
    /// The cache is intentionally a small `Vec` rather than a
    /// range-indexed structure: a data node holds at most O(vnodes
    /// / RF) leases, so a linear scan is cheap and overlap-free
    /// by construction (the state machine rejects overlaps on grant).
    leases: Arc<RwLock<Vec<LeaseDescriptor>>>,
    /// When `Some`, every inbound write / LINEARIZABLE read is
    /// fenced against the lease cache. `None` (the default) leaves
    /// the gate disabled so existing single-node tests and the
    /// multi-node bring-up path before a lease manager has spun
    /// up do not spuriously reject traffic. Production bootstrap
    /// calls [`Self::with_lease_gate`] to flip this on after the
    /// lease manager has registered its first grant.
    lease_gate_enabled: bool,
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
            tracker: None,
            leases: Arc::new(RwLock::new(Vec::new())),
            lease_gate_enabled: false,
        }
    }

    /// Wire a [`NodeTracker`] into the server so every inbound peer
    /// RPC ingests the peer's HLC. Builder-style so production
    /// bootstrap can attach the tracker without rewriting every
    /// call site, and tests stay construction-trivial.
    pub fn with_tracker(mut self, tracker: Arc<NodeTracker>) -> Self {
        self.tracker = Some(tracker);
        self
    }

    /// Enable the per-range lease gate (plan 018). Must be toggled
    /// after the lease manager has filed its first grant — turning
    /// it on with an empty cache rejects every write.
    pub fn with_lease_gate(mut self, enabled: bool) -> Self {
        self.lease_gate_enabled = enabled;
        self
    }

    /// Shared handle to the lease cache. The lease manager writes
    /// to it; the write/read paths read from it. Exposed so the
    /// manager task (which lives in a separate module) can take an
    /// `Arc` without poking at private fields.
    pub fn lease_cache(&self) -> Arc<RwLock<Vec<LeaseDescriptor>>> {
        self.leases.clone()
    }

    /// Test-only hook to seed a lease into the local cache without
    /// driving the full Raft grant path. Production code must never
    /// call this — the manager is the sole writer of real leases.
    #[cfg(test)]
    pub fn seed_lease_for_test(&self, lease: LeaseDescriptor) {
        self.leases.write().expect("lease cache poisoned").push(lease);
    }

    /// Inspect the lease cache for a valid lease that covers
    /// `key_hash` at `now` in `epoch`. Returns `true` if such a
    /// lease exists and is held by the local node.
    fn has_valid_lease(&self, key_hash: u64, epoch: u64, now: HybridTimestamp) -> bool {
        let guard = self.leases.read().expect("lease cache poisoned");
        guard.iter().any(|l| {
            l.holder.id == self.local_node.id
                && l.epoch == epoch
                && l.range_start <= key_hash
                && key_hash <= l.range_end
                && now < l.lease_expiry
        })
    }

    /// Single pass of the lease manager (plan 018):
    ///
    ///  1. Read the current ring epoch + the set of primary ranges
    ///     owned by this node.
    ///  2. Drop any cached lease whose epoch no longer matches — the
    ///     cluster may have just transitioned to E+1, which invalidates
    ///     every prior lease.
    ///  3. For each owned range, propose a `WendelonRequest::AcquireLease`
    ///     to the Raft Leader. On a `Success` commit, insert/replace the
    ///     lease in the local cache. Denials are surfaced as `false` in
    ///     the returned bool but are not an error — a concurrent holder
    ///     may legitimately still own the range during a handoff.
    ///
    /// Returns the number of ranges the node successfully fenced.
    pub async fn refresh_leases_once(
        &self,
        ring: &RingRouter,
        clock: &dyn PhysicalClock,
        ttl_ms: u32,
    ) -> usize {
        let snapshot = ring.snapshot();
        let epoch = snapshot.epoch();
        let ranges = snapshot.primary_ranges_for(self.local_node);

        {
            let mut guard = self.leases.write().expect("lease cache poisoned");
            guard.retain(|l| l.epoch == epoch);
        }

        let now_ms = clock.now_ms();
        let now_hlc = HybridTimestamp {
            physical_ms: now_ms,
            logical: 0,
            node_id: self.local_node.id,
        };
        let expiry_hlc = HybridTimestamp {
            physical_ms: now_ms.saturating_add(ttl_ms as u64),
            logical: 0,
            node_id: self.local_node.id,
        };

        let mut granted = 0usize;
        for (range_start, range_end) in ranges {
            let lease = LeaseDescriptor {
                epoch,
                holder: self.local_node,
                range_start,
                range_end,
                lease_start: now_hlc,
                lease_expiry: expiry_hlc,
                min_commit_index: 0,
                signature: [0; 64],
                _pad: [0; 8],
            };
            let outcome = self
                .raft
                .client_write(WendelonRequest::AcquireLease(lease))
                .await;
            match outcome {
                Ok(resp) => match resp.data {
                    WendelonResponse::Success => {
                        let mut guard = self.leases.write().expect("lease cache poisoned");
                        if let Some(slot) = guard
                            .iter_mut()
                            .find(|l| l.range_start == range_start && l.range_end == range_end)
                        {
                            *slot = lease;
                        } else {
                            guard.push(lease);
                        }
                        granted += 1;
                    }
                    WendelonResponse::LeaseDenied => {
                        // Another holder (or a stale-epoch request)
                        // blocks us — skip, keep the old entry if
                        // any. The next tick retries.
                    }
                },
                Err(e) => {
                    tracing::warn!(
                        range_start,
                        range_end,
                        epoch,
                        error = %e,
                        "lease manager: client_write failed; will retry next tick"
                    );
                }
            }
        }
        granted
    }

    /// Spawn the lease manager background task. Fires on every
    /// `epoch_rx.changed()` event and on a steady `tick` cadence so
    /// a node that missed a watch wake-up (e.g. because its Raft
    /// client_write errored mid-transfer) still renews before
    /// `ttl_ms` elapses.
    ///
    /// The task terminates when `epoch_rx` is closed (every sender
    /// dropped). Callers hold the returned [`JoinHandle`] on the
    /// `NodeHandles` struct so dropping the node aborts the loop.
    pub fn spawn_lease_manager(
        self: Arc<Self>,
        ring: Arc<RingRouter>,
        clock: Arc<dyn PhysicalClock>,
        mut epoch_rx: watch::Receiver<Arc<MembershipEpoch>>,
        tick: Duration,
        ttl_ms: u32,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            // Seed immediately so the cache is warm before the gate
            // is flipped on elsewhere in the bootstrap sequence.
            let _ = self.refresh_leases_once(&ring, clock.as_ref(), ttl_ms).await;
            let mut ticker = tokio::time::interval(tick);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    changed = epoch_rx.changed() => {
                        if changed.is_err() {
                            break; // senders dropped — clean exit
                        }
                    }
                    _ = ticker.tick() => {}
                }
                let _ = self.refresh_leases_once(&ring, clock.as_ref(), ttl_ms).await;
            }
        })
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

    /// Handle an inbound QUERY_REQ frame (ADR 005 pushdown scan).
    ///
    /// 1. Decode the POD `QueryRequestMeta` and fish the WPBC bytes
    ///    out of the trailing section.
    /// 2. Enforce the same epoch + consistency gates as the point
    ///    read path — the scan spans many keys but the freshness
    ///    guarantee is identical.
    /// 3. Scan the local `SlabManager` for every `key_hash` in the
    ///    inclusive `[key_range_start, key_range_end]` window,
    ///    materialise each record's Arrow batch, run WPBC over it,
    ///    and collect the non-empty results.
    /// 4. Concatenate the surviving batches into a single Arrow IPC
    ///    stream and package it alongside a POD response meta.
    ///
    /// Failures surface via the `status` byte (`EPOCH_STALE`,
    /// `E_SAFE_BEHIND`, `KEY_NOT_FOUND`, `INTERNAL_ERROR`) rather
    /// than tearing down the HRFT stream — the client inspects the
    /// meta and decides whether to retry or surface an error.
    async fn handle_query(&self, frame: HrftFrame) -> std::io::Result<HrftFrame> {
        if frame.meta.len() < std::mem::size_of::<QueryRequestMeta>() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "QUERY_REQ meta too small",
            ));
        }
        let meta: QueryRequestMeta =
            *bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<QueryRequestMeta>()]);

        // Extract the WPBC bytecode. `bytecode_len == 0` is a
        // legal "no filter, no projection" — skip the trailing
        // section entirely in that case so a missing trailing is
        // not a protocol error.
        let bytecode: &[u8] = if meta.bytecode_len == 0 {
            &[]
        } else {
            let trailing = frame.trailing.as_ref().ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidData,
                    "QUERY_REQ declared bytecode_len > 0 but trailing is absent",
                )
            })?;
            let want = meta.bytecode_len as usize;
            if trailing.len() < want {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "QUERY_REQ trailing is shorter than declared bytecode_len",
                ));
            }
            &trailing[..want]
        };

        let (resp_meta, arrow_bytes) = execute_query_plan(
            &self.slab,
            &self.state_machine,
            self.local_node,
            &meta,
            bytecode,
        )
        .await?;

        let mut body = self.pool.acquire();
        body.as_mut_vec().extend_from_slice(bytemuck::bytes_of(&resp_meta));
        let arrow_payload = arrow_bytes.map(|bytes| {
            let mut buf = self.pool.acquire();
            buf.as_mut_vec().extend_from_slice(&bytes);
            buf
        });
        let arrow_len = resp_meta.arrow_len;

        let header = FrameHeader {
            magic: HRFT_MAGIC,
            version: 1,
            msg_type: MsgType::QUERY_RESP.0,
            flags: 0,
            stream_id: frame.header.stream_id,
            meta_len: body.len() as u32,
            arrow_len,
            trailing_len: 0,
        };
        Ok(HrftFrame {
            header,
            meta: body,
            arrow_payload,
            trailing: None,
        })
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
        // Peer liveness/drift ingestion (ADR 001, ADR 012). The HLC
        // originator in `commit_hlc.node_id` is another Data Ring
        // member, so its physical_ms feeds directly into the
        // NodeTracker without further validation.
        if let Some(tracker) = &self.tracker {
            let peer = NodeId {
                id: meta.commit_hlc.node_id,
                _reserved: 0,
            };
            tracker.record_hlc(peer, meta.commit_hlc);
        }
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
            MsgType::QUERY_REQ => self.handle_query(frame).await.map(Some),
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

        // 4a. Per-range lease gate (plan 018). When enabled, this
        // node accepts the write only if it holds a valid lease
        // covering `key_hash` in the client's epoch. A node that is
        // not the primary for this key returns `NOT_LEADER` so the
        // Smart Client reroutes; a node that is the primary but
        // whose lease has lapsed returns `LEASE_EXPIRED` so the
        // client retries once the manager has renewed.
        if self.lease_gate_enabled {
            let is_primary = self
                .router
                .replicas(key_hash)
                .first()
                .map(|n| n.id == self.local_node.id)
                .unwrap_or(false);
            if !is_primary {
                return Ok((
                    error_response(&meta, WriteStatus::NOT_LEADER, self.local_node),
                    None,
                ));
            }
            if !self.has_valid_lease(key_hash, meta.expected_epoch, meta.client_hlc) {
                return Ok((
                    error_response(&meta, WriteStatus::LEASE_EXPIRED, self.local_node),
                    None,
                ));
            }
        }

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

        // 2a. Per-range lease gate for LINEARIZABLE reads (plan 018).
        // A local point-read is only authoritative while a lease
        // fences writes for this range to this node — anything
        // weaker opens a window where a stale follower could serve
        // a pre-failover value.
        if self.lease_gate_enabled && meta.consistency == Consistency::LINEARIZABLE {
            let is_primary = self
                .router
                .replicas(key_hash)
                .first()
                .map(|n| n.id == self.local_node.id)
                .unwrap_or(false);
            if !is_primary {
                return Ok((
                    error_read_response(&meta, ReadStatus::NOT_LEADER, self.local_node),
                    None,
                ));
            }
            if !self.has_valid_lease(key_hash, meta.expected_epoch, meta.min_hlc) {
                return Ok((
                    error_read_response(&meta, ReadStatus::LEASE_EXPIRED, self.local_node),
                    None,
                ));
            }
        }

        let Some(location) = self.slab.lookup_as_of(key_hash, meta.as_of_hlc) else {
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
//  QUERY EXECUTION — INDEPENDENT OF TRANSPORT
// ============================================================

/// Execute a [`QueryRequestMeta`] + WPBC program against a local
/// [`SlabManager`]. Returns the POD response meta plus the
/// concatenated Arrow IPC bytes when `status == OK`.
///
/// Split out from the frame-handler so tests can drive the full
/// query path without standing up an HRFT server or an openraft
/// instance — `WendelonDataServer` only needs the meta + bytes pair
/// to pack the outbound frame.
pub(crate) async fn execute_query_plan(
    slab: &SlabManager,
    state_machine: &WendelonStateMachine,
    local_node: NodeId,
    meta: &QueryRequestMeta,
    bytecode: &[u8],
) -> std::io::Result<(QueryResponseMeta, Option<Vec<u8>>)> {
    // 1. Epoch staleness. The authoritative epoch is the highest
    // one the SM has observed an E_safe advance for; a client
    // believing in a past epoch gets a redirect so it can refresh
    // topology rather than fork the read.
    let sm = state_machine.state_snapshot().await;
    let current_epoch = sm.e_safe_watermarks.keys().copied().max().unwrap_or(0);
    if meta.expected_epoch < current_epoch {
        return Ok((query_error_meta(meta, ReadStatus::EPOCH_STALE, local_node), None));
    }

    // 2. LINEARIZABLE gate — the scan is only authoritative once
    // the local SM has observed E_safe at-or-past the client's
    // min_hlc for the requested epoch.
    if meta.consistency == Consistency::LINEARIZABLE {
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
                query_error_meta(meta, ReadStatus::E_SAFE_BEHIND, local_node),
                None,
            ));
        }
    }

    // 3. Compile WPBC once so per-row evaluation skips repeated
    // decoding when the scan hits many records.
    // `wpbc::execute` already does this internally per call, but
    // we mirror the pattern so a follow-up that batches rows can
    // lift the compile out of the loop without touching semantics.
    let locations =
        slab.scan_as_of(meta.key_range_start, meta.key_range_end, meta.as_of_hlc);

    let mut filtered: Vec<RecordBatch> = Vec::new();
    let mut max_hlc = HybridTimestamp {
        physical_ms: 0,
        logical: 0,
        node_id: 0,
    };
    for (_key_hash, location) in locations {
        let bytes = slab.read(location).await?;
        let batch = decode_batch(&bytes)?;
        let result = wpbc::execute(bytecode, &batch)?;
        if result.num_rows() == 0 {
            continue;
        }
        if location.hlc > max_hlc {
            max_hlc = location.hlc;
        }
        filtered.push(result);
    }

    // 4. Empty result is a legitimate answer. Clients map
    // KEY_NOT_FOUND to an empty batch on the receive side, so we
    // avoid the cost of encoding a zero-row stream.
    if filtered.is_empty() {
        return Ok((
            query_error_meta(meta, ReadStatus::KEY_NOT_FOUND, local_node),
            None,
        ));
    }

    // 5. Concatenate. Every record in a single scan shares the
    // write-path schema and WPBC applies the same projection to
    // each, so the post-filter schema is uniform.
    let schema = filtered[0].schema();
    let concatenated = concat_batches(&schema, &filtered)
        .map_err(|e| Error::new(ErrorKind::InvalidData, format!("WPBC concat: {e}")))?;
    let rows = concatenated.num_rows() as u64;
    let arrow_bytes = encode_batch(&concatenated)?;
    let arrow_len = arrow_bytes.len() as u32;

    let resp = QueryResponseMeta {
        status: ReadStatus::OK,
        _pad1: [0; 7],
        value_hlc: max_hlc,
        epoch: meta.expected_epoch,
        leader_hint: local_node,
        rows,
        arrow_len,
        _pad2: [0; 4],
    };
    Ok((resp, Some(arrow_bytes)))
}

fn query_error_meta(
    meta: &QueryRequestMeta,
    status: ReadStatus,
    leader_hint: NodeId,
) -> QueryResponseMeta {
    QueryResponseMeta {
        status,
        _pad1: [0; 7],
        value_hlc: HybridTimestamp {
            physical_ms: 0,
            logical: 0,
            node_id: 0,
        },
        epoch: meta.expected_epoch,
        leader_hint,
        rows: 0,
        arrow_len: 0,
        _pad2: [0; 4],
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
    use crate::schema::wpbc::{OP_AND, OP_GTE, OP_PROJECT, OP_PUSH_COL, OP_PUSH_INT};
    use crate::schema::write_payload_schema;
    use crate::storage::core::StorageEngine;
    use crate::storage::io_uring::spawn_io_thread;
    use crate::storage::manager::SlabManagerConfig;

    fn n(id: u32) -> NodeId {
        NodeId { id, _reserved: 0 }
    }

    fn fresh_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "wendelon-data-server-{tag}-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn hlc(physical_ms: u64, logical: u32, node_id: u32) -> HybridTimestamp {
        HybridTimestamp {
            physical_ms,
            logical,
            node_id,
        }
    }

    fn kv_batch(key: &[u8], value: &[u8]) -> RecordBatch {
        let schema = Arc::new(write_payload_schema());
        let mut keys = LargeBinaryBuilder::new();
        keys.append_value(key);
        let mut values = LargeBinaryBuilder::new();
        values.append_value(value);
        RecordBatch::try_new(
            schema,
            vec![Arc::new(keys.finish()), Arc::new(values.finish())],
        )
        .unwrap()
    }

    async fn make_manager() -> Arc<SlabManager> {
        let dir = fresh_dir("query");
        let io = spawn_io_thread(64).unwrap();
        Arc::new(
            SlabManager::open(SlabManagerConfig {
                dir,
                max_slab_bytes: 1 << 30,
                io,
                l2: None,
            })
            .unwrap(),
        )
    }

    async fn seed_record(
        mgr: &SlabManager,
        key_hash: u64,
        hlc: HybridTimestamp,
        key: &[u8],
        value: &[u8],
    ) {
        let batch = kv_batch(key, value);
        let bytes = encode_batch(&batch).unwrap();
        mgr.append(key_hash, hlc, &bytes).await.unwrap();
    }

    fn base_meta() -> QueryRequestMeta {
        QueryRequestMeta {
            min_hlc: hlc(0, 0, 0),
            expected_epoch: 0,
            consistency: Consistency::SERIALIZABLE,
            _pad: [0; 7],
            key_range_start: 0,
            key_range_end: u64::MAX,
            bytecode_len: 0,
            _pad2: [0; 4],
            as_of_hlc: hlc(0, 0, 0),
        }
    }

    #[test]
    fn static_router_wraps_around_ring() {
        let router = StaticReplicaRouter::new(vec![n(1), n(2), n(3), n(4)], 3);
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

    #[tokio::test]
    async fn scan_collects_entries_in_inclusive_range() {
        let mgr = make_manager().await;
        seed_record(&mgr, 10, hlc(1, 0, 1), b"k10", b"v10").await;
        seed_record(&mgr, 50, hlc(2, 0, 1), b"k50", b"v50").await;
        seed_record(&mgr, 99, hlc(3, 0, 1), b"k99", b"v99").await;

        let hits = mgr.scan(10, 50);
        let keys: Vec<u64> = hits.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, vec![10, 50]);

        let full = mgr.scan(0, u64::MAX);
        assert_eq!(full.len(), 3);

        let miss = mgr.scan(60, 90);
        assert!(miss.is_empty());

        // Inverted range → empty, never panics.
        let inverted = mgr.scan(100, 5);
        assert!(inverted.is_empty());
    }

    #[tokio::test]
    async fn query_passthrough_returns_all_rows_in_range() {
        let mgr = make_manager().await;
        seed_record(&mgr, 1, hlc(1, 0, 1), b"a", b"v1").await;
        seed_record(&mgr, 2, hlc(2, 0, 1), b"b", b"v2").await;
        seed_record(&mgr, 3, hlc(3, 0, 1), b"c", b"v3").await;

        let sm = WendelonStateMachine::new();
        let (resp, arrow) =
            execute_query_plan(&mgr, &sm, n(1), &base_meta(), &[]).await.unwrap();
        assert_eq!(resp.status, ReadStatus::OK);
        assert_eq!(resp.rows, 3);
        assert_eq!(resp.value_hlc, hlc(3, 0, 1));
        let bytes = arrow.expect("OK must carry arrow bytes");
        let batch = decode_batch(&bytes).unwrap();
        assert_eq!(batch.num_rows(), 3);
    }

    #[tokio::test]
    async fn query_range_filters_out_of_window_rows() {
        let mgr = make_manager().await;
        seed_record(&mgr, 5, hlc(1, 0, 1), b"a", b"va").await;
        seed_record(&mgr, 42, hlc(2, 0, 1), b"b", b"vb").await;
        seed_record(&mgr, 100, hlc(3, 0, 1), b"c", b"vc").await;

        let sm = WendelonStateMachine::new();
        let mut meta = base_meta();
        meta.key_range_start = 10;
        meta.key_range_end = 50;
        let (resp, _) = execute_query_plan(&mgr, &sm, n(1), &meta, &[]).await.unwrap();
        assert_eq!(resp.status, ReadStatus::OK);
        assert_eq!(resp.rows, 1);
    }

    #[tokio::test]
    async fn query_empty_scan_returns_key_not_found() {
        let mgr = make_manager().await;
        let sm = WendelonStateMachine::new();
        let (resp, arrow) =
            execute_query_plan(&mgr, &sm, n(1), &base_meta(), &[]).await.unwrap();
        assert_eq!(resp.status, ReadStatus::KEY_NOT_FOUND);
        assert_eq!(resp.rows, 0);
        assert_eq!(resp.arrow_len, 0);
        assert!(arrow.is_none());
    }

    #[tokio::test]
    async fn linearizable_gate_fires_when_watermark_behind() {
        let mgr = make_manager().await;
        seed_record(&mgr, 1, hlc(1, 0, 1), b"k", b"v").await;
        let sm = WendelonStateMachine::new();
        // Watermark remains at default (0,0,0); client demands (5,0,0).
        let mut meta = base_meta();
        meta.consistency = Consistency::LINEARIZABLE;
        meta.min_hlc = hlc(5, 0, 0);
        let (resp, arrow) =
            execute_query_plan(&mgr, &sm, n(1), &meta, &[]).await.unwrap();
        assert_eq!(resp.status, ReadStatus::E_SAFE_BEHIND);
        assert_eq!(resp.leader_hint, n(1));
        assert!(arrow.is_none());
    }

    #[tokio::test]
    async fn linearizable_gate_passes_once_watermark_catches_up() {
        let mgr = make_manager().await;
        seed_record(&mgr, 1, hlc(1, 0, 1), b"k", b"v").await;
        let sm = WendelonStateMachine::new();
        sm.seed_e_safe_for_test(0, hlc(10, 0, 1)).await;
        let mut meta = base_meta();
        meta.consistency = Consistency::LINEARIZABLE;
        meta.min_hlc = hlc(5, 0, 1);
        let (resp, _) = execute_query_plan(&mgr, &sm, n(1), &meta, &[]).await.unwrap();
        assert_eq!(resp.status, ReadStatus::OK);
        assert_eq!(resp.rows, 1);
    }

    #[tokio::test]
    async fn epoch_stale_returns_redirect() {
        let mgr = make_manager().await;
        seed_record(&mgr, 1, hlc(1, 0, 1), b"k", b"v").await;
        let sm = WendelonStateMachine::new();
        // The SM has advanced to epoch 7; client still thinks we
        // are on epoch 3 → EPOCH_STALE, not a silent scan.
        sm.seed_e_safe_for_test(7, hlc(1, 0, 1)).await;
        let mut meta = base_meta();
        meta.expected_epoch = 3;
        let (resp, _) = execute_query_plan(&mgr, &sm, n(1), &meta, &[]).await.unwrap();
        assert_eq!(resp.status, ReadStatus::EPOCH_STALE);
        assert_eq!(resp.leader_hint, n(1));
    }

    #[tokio::test]
    async fn wpbc_filter_drops_rows_below_threshold() {
        // Stored records carry an Int64 `age` column so WPBC can
        // actually compare against a literal — the write-path
        // schema is LargeBinary and WPBC does not support bytes,
        // so tests that exercise filter opcodes use their own
        // schema via direct slab.append.
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![
            Field::new("age", DataType::Int64, false),
        ]));
        let mgr = make_manager().await;
        for (key_hash, age) in [(1u64, 17i64), (2, 22), (3, 40)] {
            let ages = Int64Array::from(vec![age]);
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(ages)]).unwrap();
            let bytes = encode_batch(&batch).unwrap();
            mgr.append(key_hash, hlc(age as u64, 0, 1), &bytes)
                .await
                .unwrap();
        }

        // age >= 18 AND age >= 18 (duplicated to exercise OP_AND)
        let mut bytecode = Vec::new();
        bytecode.push(OP_PUSH_COL);
        bytecode.extend_from_slice(&0u32.to_le_bytes());
        bytecode.push(OP_PUSH_INT);
        bytecode.extend_from_slice(&18i64.to_le_bytes());
        bytecode.push(OP_GTE);
        bytecode.push(OP_PUSH_COL);
        bytecode.extend_from_slice(&0u32.to_le_bytes());
        bytecode.push(OP_PUSH_INT);
        bytecode.extend_from_slice(&18i64.to_le_bytes());
        bytecode.push(OP_GTE);
        bytecode.push(OP_AND);

        let sm = WendelonStateMachine::new();
        let mut meta = base_meta();
        meta.bytecode_len = bytecode.len() as u32;
        let (resp, _) =
            execute_query_plan(&mgr, &sm, n(1), &meta, &bytecode).await.unwrap();
        assert_eq!(resp.status, ReadStatus::OK);
        // Rows 2 (age 22) and 3 (age 40) survive; row 1 (age 17) drops.
        assert_eq!(resp.rows, 2);
    }

    #[tokio::test]
    async fn wpbc_projection_narrows_schema() {
        let mgr = make_manager().await;
        seed_record(&mgr, 1, hlc(1, 0, 1), b"a", b"va").await;
        seed_record(&mgr, 2, hlc(2, 0, 1), b"b", b"vb").await;
        // PROJECT column 0 (the `key` column) only — WPBC compiles
        // a terminal PROJECT with no preceding filter, so every row
        // is kept and only the `key` column survives.
        let mut bytecode = Vec::new();
        bytecode.push(OP_PROJECT);
        bytecode.extend_from_slice(&1u16.to_le_bytes());
        bytecode.extend_from_slice(&0u32.to_le_bytes());

        let sm = WendelonStateMachine::new();
        let mut meta = base_meta();
        meta.bytecode_len = bytecode.len() as u32;
        let (resp, arrow) =
            execute_query_plan(&mgr, &sm, n(1), &meta, &bytecode).await.unwrap();
        assert_eq!(resp.status, ReadStatus::OK);
        assert_eq!(resp.rows, 2);
        let batch = decode_batch(&arrow.unwrap()).unwrap();
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "key");
    }

    // ========================================================
    //  LEASE GATE TESTS (plan 018)
    //
    //  These tests drive the Data Ring lease gate end-to-end by
    //  standing up a single-node cluster via
    //  `WendelonNode::bootstrap_single_node`. The gate logic is
    //  exercised against the real `WendelonDataServer` from that
    //  bootstrap (the node pubs it on the struct).
    // ========================================================

    use crate::node::{NodeBootstrapConfig, WendelonNode};
    use crate::schema::{
        ClientWriteId, Consistency as SchemaConsistency, DurabilityLevel, WriteRequestMeta,
    };

    fn bootstrap_tempdir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "wendelon-lease-{tag}-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn simple_write_meta(epoch: u64) -> WriteRequestMeta {
        WriteRequestMeta {
            write_id: ClientWriteId {
                client_uuid: [7u8; 16],
                sequence_no: 1,
                idempotency_ttl_ms: 60_000,
                _pad: 0,
            },
            client_hlc: hlc(10, 0, 0),
            expected_epoch: epoch,
            proxy_hop: 0,
            durability: DurabilityLevel::QUORUM,
            await_finalization: 0,
            _pad1: [0; 5],
            proxy_source: NodeId {
                id: 0,
                _reserved: 0,
            },
            key_len: 1,
            value_len: 1,
        }
    }

    fn single_kv_batch() -> RecordBatch {
        let schema = Arc::new(write_payload_schema());
        let mut keys = LargeBinaryBuilder::new();
        keys.append_value(b"k");
        let mut values = LargeBinaryBuilder::new();
        values.append_value(b"v");
        RecordBatch::try_new(
            schema,
            vec![Arc::new(keys.finish()), Arc::new(values.finish())],
        )
        .unwrap()
    }

    fn simple_read_meta(epoch: u64) -> ReadRequestMeta {
        ReadRequestMeta {
            min_hlc: hlc(0, 0, 0),
            expected_epoch: epoch,
            consistency: SchemaConsistency::LINEARIZABLE,
            _pad: [0; 3],
            key_len: 1,
            _pad2: [0; 8],
            as_of_hlc: hlc(0, 0, 0),
        }
    }

    fn single_key_batch() -> RecordBatch {
        let schema = Arc::new(crate::schema::read_key_schema());
        let mut keys = LargeBinaryBuilder::new();
        keys.append_value(b"k");
        RecordBatch::try_new(schema, vec![Arc::new(keys.finish())]).unwrap()
    }

    #[tokio::test]
    async fn write_rejected_when_lease_gate_on_and_cache_empty() {
        let dir = bootstrap_tempdir("empty-cache");
        let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
            11,
            dir,
            "127.0.0.1:0".into(),
        ))
        .await
        .unwrap();
        // Force the cache empty so the gate can't find a lease.
        node.data_server
            .lease_cache()
            .write()
            .unwrap()
            .clear();

        let (resp, _) = node
            .data_server
            .write(simple_write_meta(1), single_kv_batch())
            .await
            .unwrap();
        assert_eq!(resp.status, WriteStatus::LEASE_EXPIRED);
        assert_eq!(resp.leader_hint, node.local_node);
    }

    #[tokio::test]
    async fn linearizable_read_rejected_when_lease_missing() {
        let dir = bootstrap_tempdir("lin-read");
        let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
            12,
            dir,
            "127.0.0.1:0".into(),
        ))
        .await
        .unwrap();
        node.data_server.lease_cache().write().unwrap().clear();

        let (resp, _) = node
            .data_server
            .read(simple_read_meta(1), single_key_batch())
            .await
            .unwrap();
        assert_eq!(resp.status, ReadStatus::LEASE_EXPIRED);
    }

    #[tokio::test]
    async fn refresh_leases_once_clears_stale_epoch_entries() {
        let dir = bootstrap_tempdir("stale-epoch");
        let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
            13,
            dir,
            "127.0.0.1:0".into(),
        ))
        .await
        .unwrap();
        // Seed a lease from a prior epoch — the cache must drop it
        // on the next refresh tick because the current ring epoch
        // is 1, not 99.
        node.data_server.seed_lease_for_test(crate::schema::LeaseDescriptor {
            epoch: 99,
            holder: node.local_node,
            range_start: 0,
            range_end: u64::MAX,
            lease_start: hlc(0, 0, node.local_node.id),
            lease_expiry: hlc(u64::MAX / 2, 0, node.local_node.id),
            min_commit_index: 0,
            signature: [0; 64],
            _pad: [0; 8],
        });
        // Give Raft a moment to elect and ring updater to seed.
        for _ in 0..64 {
            tokio::task::yield_now().await;
            if node.raft.current_leader().await == Some(node.local_node.id) {
                break;
            }
        }
        let granted = node
            .data_server
            .refresh_leases_once(&node.ring, &*node.clock, 60_000)
            .await;
        // Exactly one primary range on a single-node cluster, so one grant.
        assert!(granted >= 1);
        let cache = node.data_server.lease_cache();
        let entries = cache.read().unwrap().clone();
        assert!(
            entries.iter().all(|l| l.epoch == 1),
            "stale-epoch leases must be evicted; cache={entries:?}"
        );
        assert!(entries.iter().any(|l| l.holder.id == node.local_node.id));
    }

    #[tokio::test]
    async fn write_accepted_once_lease_manager_has_granted() {
        let dir = bootstrap_tempdir("accept");
        let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
            14,
            dir,
            "127.0.0.1:0".into(),
        ))
        .await
        .unwrap();
        // Wait for Raft leadership and drive one explicit refresh
        // so the cache is guaranteed to be warm before the write
        // arrives — the spawned manager is on a 1s tick so tests
        // that race against it would be flaky.
        for _ in 0..64 {
            tokio::task::yield_now().await;
            if node.raft.current_leader().await == Some(node.local_node.id) {
                break;
            }
        }
        node.data_server
            .refresh_leases_once(&node.ring, &*node.clock, 60_000)
            .await;

        let mut meta = simple_write_meta(1);
        // Push client_hlc into the granted window (the manager used
        // `clock.now_ms()` for lease_start; any value at-or-after 0
        // and before expiry is fine).
        meta.client_hlc = hlc(1, 0, 0);
        let (resp, _) = node
            .data_server
            .write(meta, single_kv_batch())
            .await
            .unwrap();
        assert_eq!(resp.status, WriteStatus::OK, "got resp={resp:?}");
    }
}
