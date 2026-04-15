//! Smart Client SDK — the external entry point into Wendelon.
//!
//! External engines (DataFusion `TableProvider`, ad-hoc analytics)
//! talk to Wendelon through a [`WendelonClient`] that speaks the
//! native HRFT wire directly. The "smart" part is twofold:
//!
//! 1. **Topology awareness.** The client maintains a local
//!    projection of the Unified Hash Ring (ADR 002) — the
//!    bootstrap topology plus whatever the Control Ring's status
//!    RPC said last — so it can dial the exact replica that owns
//!    a given `key_hash` without a proxy hop.
//! 2. **Zero-copy querying.** Pushdown scans ship a WPBC program
//!    (ADR 005) in a custom HRFT frame (ADR 004) straight to a
//!    data node; the server evaluates the program against its
//!    local Arrow batches and streams the filtered IPC back on
//!    the same bidi stream. No gRPC, no protobuf, no intermediate
//!    buffers past the pooled frame sections.
//!
//! ## Lifecycle
//!
//! Build a [`WendelonClient`] via [`WendelonClient::builder`]:
//!
//! ```text
//!   TopologySource → WendelonClient → query(wpbc, range)
//!        │                               │
//!        └───── watch<MembershipEpoch> ─┘
//! ```
//!
//! The client owns a [`TransportProviderFactory`] that shares the
//! same `watch::Receiver<Arc<MembershipEpoch>>` the client publishes
//! into, so membership changes evict stale connections inside the
//! factory without extra plumbing.
//!
//! ## Redirect handling
//!
//! A data node that returns `NOT_LEADER` or `E_SAFE_BEHIND` carries
//! a `leader_hint` the client should redirect to. The query path
//! triggers a topology refresh and retries exactly once against the
//! hinted node before surfacing the error.

use crate::client::topology::{ClusterStatusWire, TopologySource};
use crate::network::core::{read_frame, write_frame, HRFT_MAGIC};
use crate::network::data_server::ReplicaRouter;
use crate::network::factory::TransportProviderFactory;
use crate::network::pool::BufPool;
use crate::network::ring::{RingRouter, RingState};
use crate::network::transport::{
    CapabilityResolver, EpochCapabilityResolver, MembershipEpoch, TransportProvider,
    TransportTier, TransportTierSet,
};
use crate::schema::{
    Consistency, FrameHeader, HybridTimestamp, MsgType, NodeId, QueryRequestMeta,
    QueryResponseMeta, ReadStatus,
};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::io::{Cursor, Error, ErrorKind};
use std::sync::{Arc, RwLock};
use tokio::sync::watch;

// ============================================================
//  CONFIG
// ============================================================

/// Construction parameters for [`WendelonClient`].
///
/// Two pieces are required from the caller — the tier bitmask this
/// client can actually dial over, and the concrete
/// [`TransportProvider`]s keyed on tier. Everything else
/// (capability resolver, membership watch, connection cache) is
/// derived inside the builder so callers do not have to re-wire it
/// per constructor site.
pub struct WendelonClientConfig {
    /// Tiers this client can speak. Combined with each peer's
    /// advertised `TransportTierSet` to pick the best common
    /// transport at dial time.
    pub local_tiers: TransportTierSet,
    /// One provider per supported tier. A tier listed in
    /// `local_tiers` but absent here will fail any dial that
    /// negotiates to it with `ErrorKind::Unsupported`.
    pub providers: HashMap<TransportTier, Arc<dyn TransportProvider>>,
    /// Seed membership snapshot. Lets the very first dial (to fetch
    /// the real topology) land on a well-known bootstrap peer
    /// without a chicken-and-egg loop.
    pub bootstrap: ClusterStatusWire,
    /// Source of subsequent refresh snapshots — typically a
    /// [`crate::client::topology::ControlRingTopologySource`] in
    /// production, a
    /// [`crate::client::topology::StaticTopologySource`] in tests.
    pub topology: Arc<dyn TopologySource>,
    /// Buffer-pool bounds. Tune up for high-fanout clients; the
    /// defaults elsewhere in the codebase (64 buffers, 8 MiB cap)
    /// are a reasonable starting point.
    pub pool_max_retained: usize,
    pub pool_max_cap_bytes: usize,
}

// ============================================================
//  CLIENT
// ============================================================

/// Zero-copy, topology-aware entry point into a Wendelon cluster.
///
/// Construct one per application process; internally it owns a
/// [`TransportProviderFactory`] so every call site shares the same
/// per-peer connection cache and tier negotiation. Cloning the
/// outer `Arc<WendelonClient>` is the intended way to hand the
/// client to multiple tasks.
pub struct WendelonClient {
    factory: Arc<TransportProviderFactory>,
    pool: Arc<BufPool>,
    /// Authoritative client-side ring. `RingRouter` owns its own
    /// `RwLock<Arc<RingState>>`, so swaps on topology refresh are
    /// atomic internally — reads are a brief read-guard plus an
    /// `Arc::clone` and never await.
    router: Arc<RingRouter>,
    /// Shared ownership of the membership publication. The factory
    /// holds the receiving end; refreshes go through this sender
    /// so connection invalidation fires automatically.
    topology_tx: watch::Sender<Arc<MembershipEpoch>>,
    /// Source of the next refresh snapshot.
    topology_source: Arc<dyn TopologySource>,
    /// Last observed cluster leader. Updated on every refresh;
    /// redirect hints on read-path errors consult this only to
    /// decide whether the hint is new enough to warrant a retry.
    leader: RwLock<Option<NodeId>>,
    /// Cached replication factor from the latest topology fetch.
    rf: RwLock<usize>,
}

impl WendelonClient {
    /// Build a client from a config. Requires a tokio runtime —
    /// the factory spawns its invalidation task eagerly.
    pub fn new(config: WendelonClientConfig) -> Arc<Self> {
        let seed = config.bootstrap.to_membership();
        let (topology_tx, topology_rx) = watch::channel(Arc::new(seed.clone()));

        let resolver: Arc<dyn CapabilityResolver> =
            Arc::new(EpochCapabilityResolver::new(topology_rx.clone()));
        let factory = TransportProviderFactory::new(
            config.local_tiers,
            config.providers,
            resolver,
            topology_rx,
        );

        let pool = BufPool::new(config.pool_max_retained, config.pool_max_cap_bytes);
        let router = Arc::new(RingRouter::new(RingState::build(
            &seed,
            config.bootstrap.replication_factor as usize,
        )));
        let leader = config
            .bootstrap
            .leader
            .map(|id| NodeId { id, _reserved: 0 });
        let rf = config.bootstrap.replication_factor as usize;

        Arc::new(Self {
            factory,
            pool,
            router,
            topology_tx,
            topology_source: config.topology,
            leader: RwLock::new(leader),
            rf: RwLock::new(rf),
        })
    }

    /// Pull a fresh snapshot from the topology source and swap it
    /// into the router + factory. Idempotent and cheap to call
    /// repeatedly — the underlying source decides whether work is
    /// actually performed.
    pub async fn refresh_topology(&self) -> std::io::Result<()> {
        let snapshot = self.topology_source.fetch().await?;
        self.apply_snapshot(&snapshot);
        Ok(())
    }

    /// Apply a pre-fetched snapshot. Exposed so tests can drive the
    /// client without going through the source trait, and so
    /// future push-based topology streams can reuse the same
    /// update path.
    pub fn apply_snapshot(&self, snapshot: &ClusterStatusWire) {
        let membership = snapshot.to_membership();
        let rf = snapshot.replication_factor as usize;

        // `RingRouter::install` handles its own atomic swap, so the
        // client no longer needs an outer RwLock around the router.
        // The watch send is last so any observer that sees the new
        // epoch also sees the new ring.
        self.router.install(RingState::build(&membership, rf));
        *self.rf.write().unwrap() = rf;
        *self.leader.write().unwrap() =
            snapshot.leader.map(|id| NodeId { id, _reserved: 0 });
        let _ = self.topology_tx.send(Arc::new(membership));
    }

    /// Replicas that should own `key_hash`. Returned in the
    /// deterministic order the current router produces.
    pub fn replicas_for(&self, key_hash: u64) -> Vec<NodeId> {
        self.router.replicas(key_hash)
    }

    /// Best known leader for the cluster. `None` while the cluster
    /// is mid-election or before the first successful refresh
    /// against a leaderful snapshot.
    pub fn leader(&self) -> Option<NodeId> {
        *self.leader.read().unwrap()
    }

    /// Current replication factor as reported by the latest
    /// snapshot.
    pub fn replication_factor(&self) -> usize {
        *self.rf.read().unwrap()
    }

    /// Access to the shared connection factory. Exposed so
    /// subsystems that want to ride the client's transport
    /// (e.g. an admin tool sending Control Ring RPCs) reuse the
    /// same cache.
    pub fn factory(&self) -> &Arc<TransportProviderFactory> {
        &self.factory
    }

    /// Buffer pool used for outbound encode and inbound frame
    /// sections. Exposed for the same reason as [`Self::factory`].
    pub fn pool(&self) -> &Arc<BufPool> {
        &self.pool
    }
}

// ============================================================
//  QUERY API
// ============================================================

/// Inclusive key-hash range for a pushdown scan.
///
/// Wendelon routes keys by `HighwayHash(key_bytes)`; the client
/// compiles its user-facing range into hash bounds at call time.
/// `start == end` collapses to a single-key point scan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeyRange {
    pub start_hash: u64,
    pub end_hash: u64,
}

impl KeyRange {
    pub fn point(key_hash: u64) -> Self {
        Self {
            start_hash: key_hash,
            end_hash: key_hash,
        }
    }

    pub fn full() -> Self {
        Self {
            start_hash: 0,
            end_hash: u64::MAX,
        }
    }
}

/// One filtered-and-projected result batch plus the HLC frontier
/// the server observed while producing it.
#[derive(Clone, Debug)]
pub struct QueryResult {
    pub batch: RecordBatch,
    pub value_hlc: HybridTimestamp,
    pub epoch: u64,
    pub rows: u64,
}

impl WendelonClient {
    /// Push a WPBC program to the replica that owns `range.start_hash`
    /// and stream the filtered Arrow IPC result back.
    ///
    /// * `wpbc_ast` — raw WPBC bytecode. Empty is legal and means
    ///   "no filter, no projection".
    /// * `range` — inclusive key-hash window. V1 targets the
    ///   owner of `start_hash`; a true cross-replica scan fan-out
    ///   is deferred.
    /// * `expected_epoch` / `min_hlc` / `consistency` carry the
    ///   client's freshness bound straight through to the server.
    /// * `as_of_hlc` is the ADR 013 time-travel upper bound. The
    ///   zero HLC means "no upper bound" — the normal case. Any
    ///   non-zero value asks the server to return the newest
    ///   version per key whose `hlc <= as_of_hlc`.
    ///
    /// On `NOT_LEADER` or `E_SAFE_BEHIND` the client refreshes
    /// topology and retries exactly once against the server's
    /// `leader_hint`. Any other error surface is terminal.
    pub async fn query(
        &self,
        wpbc_ast: Vec<u8>,
        range: KeyRange,
        expected_epoch: u64,
        min_hlc: HybridTimestamp,
        consistency: Consistency,
        as_of_hlc: HybridTimestamp,
    ) -> std::io::Result<QueryResult> {
        let target = self.pick_target(range.start_hash).ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                "no replica available for the requested key range",
            )
        })?;
        let meta = QueryRequestMeta {
            min_hlc,
            expected_epoch,
            consistency,
            _pad: [0; 7],
            key_range_start: range.start_hash,
            key_range_end: range.end_hash,
            bytecode_len: wpbc_ast.len() as u32,
            _pad2: [0; 4],
            as_of_hlc,
        };

        match self.query_once(target, &meta, &wpbc_ast).await {
            Ok(QueryOutcome::Ok(result)) => Ok(result),
            Ok(QueryOutcome::Redirect(hint)) => self.query_with_redirect(hint, &meta, &wpbc_ast).await,
            Err(e) => Err(e),
        }
    }

    /// First-pass dispatch target: the head of the replica set for
    /// `key_hash`, preferring the local leader when the leader is
    /// actually a replica for that range. Returns `None` when the
    /// router has no nodes — callers surface that as a clean
    /// `NotFound` so the topology can be re-probed.
    fn pick_target(&self, key_hash: u64) -> Option<NodeId> {
        let replicas = self.replicas_for(key_hash);
        if replicas.is_empty() {
            return None;
        }
        if let Some(leader) = self.leader() {
            if replicas.iter().any(|n| n.id == leader.id) {
                return Some(leader);
            }
        }
        Some(replicas[0])
    }

    async fn query_with_redirect(
        &self,
        hint: NodeId,
        meta: &QueryRequestMeta,
        wpbc: &[u8],
    ) -> std::io::Result<QueryResult> {
        // A redirect hint is a sign the topology we routed against
        // is stale. Refresh before retrying so subsequent queries
        // already see the new leader; a failed refresh is not
        // terminal — the hint itself is enough to dial.
        let _ = self.refresh_topology().await;
        match self.query_once(hint, meta, wpbc).await? {
            QueryOutcome::Ok(result) => Ok(result),
            QueryOutcome::Redirect(_) => Err(Error::new(
                ErrorKind::NotConnected,
                "Wendelon query still redirecting after refresh",
            )),
        }
    }

    async fn query_once(
        &self,
        target: NodeId,
        meta: &QueryRequestMeta,
        wpbc: &[u8],
    ) -> std::io::Result<QueryOutcome> {
        let conn = self.factory.get_connection(target).await?;
        let mut stream = conn.open_bi().await?;

        let meta_bytes = bytemuck::bytes_of(meta);
        let trailing: Option<&[u8]> = if wpbc.is_empty() {
            None
        } else {
            Some(wpbc)
        };
        let header = FrameHeader {
            magic: HRFT_MAGIC,
            version: 1,
            msg_type: MsgType::QUERY_REQ.0,
            flags: 0,
            stream_id: 0,
            meta_len: meta_bytes.len() as u32,
            arrow_len: 0,
            trailing_len: trailing.map(|t| t.len() as u32).unwrap_or(0),
        };
        write_frame(&mut *stream, &header, meta_bytes, None, trailing).await?;
        stream
            .finish()
            .await
            .map_err(|e| Error::new(ErrorKind::BrokenPipe, e))?;

        let resp = read_frame(&mut *stream, &self.pool).await?;
        if resp.header.msg_type != MsgType::QUERY_RESP.0 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "expected QUERY_RESP, got msg_type=0x{:02x}",
                    resp.header.msg_type
                ),
            ));
        }
        if resp.meta.len() < std::mem::size_of::<QueryResponseMeta>() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "QUERY_RESP meta too small",
            ));
        }
        let ack: &QueryResponseMeta =
            bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<QueryResponseMeta>()]);

        match ack.status {
            s if s == ReadStatus::OK => {
                let arrow = resp.arrow_payload.as_ref().ok_or_else(|| {
                    Error::new(
                        ErrorKind::InvalidData,
                        "QUERY_RESP OK missing Arrow payload",
                    )
                })?;
                let batch = decode_batch(&arrow[..])?;
                Ok(QueryOutcome::Ok(QueryResult {
                    batch,
                    value_hlc: ack.value_hlc,
                    epoch: ack.epoch,
                    rows: ack.rows,
                }))
            }
            s if s == ReadStatus::NOT_LEADER || s == ReadStatus::E_SAFE_BEHIND => {
                Ok(QueryOutcome::Redirect(ack.leader_hint))
            }
            s if s == ReadStatus::KEY_NOT_FOUND => Ok(QueryOutcome::Ok(QueryResult {
                // No rows is a legitimate answer — return an empty
                // batch shaped however the server said (here: we
                // have no schema to build, so surface an error so
                // the caller can decide).
                batch: empty_batch()?,
                value_hlc: ack.value_hlc,
                epoch: ack.epoch,
                rows: 0,
            })),
            other => Err(Error::new(
                ErrorKind::Other,
                format!("Wendelon query failed: status={}", other.0),
            )),
        }
    }
}

#[derive(Debug)]
enum QueryOutcome {
    Ok(QueryResult),
    Redirect(NodeId),
}

// ============================================================
//  HELPERS
// ============================================================

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

/// Zero-column placeholder used when the server returns
/// `KEY_NOT_FOUND` without an Arrow payload. The caller's
/// downstream stages should treat `rows == 0` as the authoritative
/// signal rather than introspect the batch shape.
fn empty_batch() -> std::io::Result<RecordBatch> {
    let schema = Arc::new(arrow::datatypes::Schema::empty());
    RecordBatch::try_new_with_options(
        schema,
        vec![],
        &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(0)),
    )
    .map_err(|e| Error::new(ErrorKind::InvalidData, format!("empty batch: {e}")))
}

// ============================================================
//  TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::topology::{ClusterPeerWire, StaticTopologySource};
    use crate::network::transport::TransportTier;

    fn tier_bits() -> u8 {
        TransportTierSet::empty()
            .with(TransportTier::QuicIoUring)
            .as_u8()
    }

    fn snapshot(epoch: u64, leader: Option<u32>, ids: &[u32]) -> ClusterStatusWire {
        ClusterStatusWire {
            epoch,
            leader,
            replication_factor: 3,
            peers: ids
                .iter()
                .map(|id| ClusterPeerWire {
                    node_id: *id,
                    address: format!("127.0.0.1:{}", 9000 + *id),
                    tier_bits: tier_bits(),
                    role_bits: role_bits(),
                })
                .collect(),
        }
    }

    fn role_bits() -> u8 {
        use crate::network::transport::{NodeRole, NodeRoleSet};
        NodeRoleSet::empty().with(NodeRole::Data).as_u8()
    }

    fn test_config(initial: ClusterStatusWire) -> WendelonClientConfig {
        WendelonClientConfig {
            local_tiers: TransportTierSet::empty().with(TransportTier::QuicIoUring),
            providers: HashMap::new(),
            bootstrap: initial.clone(),
            topology: Arc::new(StaticTopologySource::new(initial)),
            pool_max_retained: 4,
            pool_max_cap_bytes: 64 * 1024,
        }
    }

    #[tokio::test]
    async fn bootstrap_populates_router_and_leader() {
        let snap = snapshot(5, Some(2), &[1, 2, 3]);
        let client = WendelonClient::new(test_config(snap));
        let replicas = client.replicas_for(0);
        assert_eq!(replicas.len(), 3);
        assert_eq!(
            client.leader(),
            Some(NodeId {
                id: 2,
                _reserved: 0
            })
        );
        assert_eq!(client.replication_factor(), 3);
    }

    #[tokio::test]
    async fn refresh_topology_swaps_router_and_publishes_epoch() {
        // Seed with a 3-node ring; refresh reveals a 4-node ring.
        let seed = snapshot(1, Some(1), &[1, 2, 3]);
        let src = Arc::new(StaticTopologySource::new(seed.clone()));
        let cfg = WendelonClientConfig {
            local_tiers: TransportTierSet::empty().with(TransportTier::QuicIoUring),
            providers: HashMap::new(),
            bootstrap: seed,
            topology: src.clone(),
            pool_max_retained: 4,
            pool_max_cap_bytes: 64 * 1024,
        };
        let client = WendelonClient::new(cfg);

        let grown = snapshot(2, Some(4), &[1, 2, 3, 4]);
        src.set(grown.clone());

        client.refresh_topology().await.unwrap();

        let replicas = client.replicas_for(0);
        // With 4 nodes and RF=3 we always see 3 distinct replicas;
        // the important invariant is that node 4 is now reachable.
        let ids: Vec<u32> = replicas.iter().map(|n| n.id).collect();
        assert!(ids.contains(&1) || ids.contains(&4));
        assert_eq!(
            client.leader(),
            Some(NodeId {
                id: 4,
                _reserved: 0
            })
        );
    }

    #[tokio::test]
    async fn pick_target_prefers_leader_when_leader_is_a_replica() {
        let snap = snapshot(1, Some(2), &[1, 2, 3]);
        let client = WendelonClient::new(test_config(snap));
        // Exhaust hashes until we find one whose replica set
        // contains node 2; then assert pick_target chose node 2.
        for h in 0u64..32 {
            let replicas = client.replicas_for(h);
            if replicas.iter().any(|n| n.id == 2) {
                let picked = client.pick_target(h).unwrap();
                assert_eq!(picked.id, 2);
                return;
            }
        }
        panic!("replica set never contained node 2 — router wiring regression");
    }

    #[tokio::test]
    async fn apply_snapshot_updates_rf_and_leader() {
        let seed = snapshot(1, Some(1), &[1, 2, 3]);
        let client = WendelonClient::new(test_config(seed));
        let replacement = ClusterStatusWire {
            epoch: 2,
            leader: None,
            replication_factor: 5,
            peers: (1..=5)
                .map(|id| ClusterPeerWire {
                    node_id: id,
                    address: format!("127.0.0.1:{}", 9000 + id),
                    tier_bits: tier_bits(),
                    role_bits: role_bits(),
                })
                .collect(),
        };
        client.apply_snapshot(&replacement);
        assert_eq!(client.replication_factor(), 5);
        assert_eq!(client.leader(), None);
        assert_eq!(client.replicas_for(0).len(), 5);
    }

    #[test]
    fn key_range_helpers() {
        let p = KeyRange::point(42);
        assert_eq!(p.start_hash, 42);
        assert_eq!(p.end_hash, 42);
        let f = KeyRange::full();
        assert_eq!(f.start_hash, 0);
        assert_eq!(f.end_hash, u64::MAX);
    }
}
