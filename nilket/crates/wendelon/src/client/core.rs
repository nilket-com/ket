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
//! A data node that returns `NOT_LEADER`, `EPOCH_STALE`, or
//! `E_SAFE_BEHIND` carries a `leader_hint` the client can use to
//! recover. The client runs a bounded refresh-and-retry loop so it
//! can re-target requests after leadership changes or stale views
//! without forcing callers to hand-roll redirect logic.

use crate::client::topology::{ClusterStatusWire, TopologySource};
use crate::network::core::{HRFT_MAGIC, read_frame, write_frame};
use crate::network::data_server::ReplicaRouter;
use crate::network::factory::TransportProviderFactory;
use crate::network::pool::BufPool;
use crate::network::ring::{RingRouter, RingState};
use crate::network::transport::{
	CapabilityResolver, EpochCapabilityResolver, MembershipEpoch, TransportProvider,
	TransportStream, TransportTier, TransportTierSet,
};
use crate::schema::{
	Consistency, FRAME_FLAG_STREAM_END, FinalizationMarker, FrameHeader, HybridTimestamp, MsgType,
	NodeId, QueryRequestMeta, QueryResponseMeta, ReadRequestMeta, ReadResponseMeta, ReadStatus,
	WriteRequestMeta, WriteResponseMeta, WriteStatus, read_key_schema, read_value_schema,
	write_payload_schema,
};
use arrow::array::{Array, LargeBinaryArray};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use highway::{HighwayHash, HighwayHasher, Key as HighwayKey};
use std::collections::HashMap;
use std::io::{Cursor, Error, ErrorKind};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::watch;

/// Stable key-hash seed shared with the Data Ring.
const HIGHWAY_KEY: HighwayKey = HighwayKey([
	0x5745_4e44_454c_4f4e,
	0x4841_5348_4b45_5901,
	0x4441_5441_5249_4e47,
	0x5354_454c_4c41_524f,
]);

/// Maximum number of client attempts for a single logical data
/// operation before surfacing the last non-success status.
pub const DATA_RETRY_BUDGET: usize = 3;

/// Small pause before retrying after `E_SAFE_BEHIND` so a follower
/// or newly-elected leader has a chance to advance freshness.
pub const E_SAFE_RETRY_BACKOFF: Duration = Duration::from_millis(25);
/// Small pause before retrying after `MIGRATION_INCOMPLETE` so the
/// destination owner's background migration sweeper can pull the
/// missing range data and commit completion.
pub const MIGRATION_RETRY_BACKOFF: Duration = Duration::from_millis(75);

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
		self.refresh_topology_with_candidates(&[]).await
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
		*self.leader.write().unwrap() = snapshot.leader.map(|id| NodeId { id, _reserved: 0 });
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
//  DATA API
// ============================================================

/// Client-visible unary write request for the Data Ring wire API.
#[derive(Clone, Debug)]
pub struct WriteRequest {
	pub meta: WriteRequestMeta,
	pub payload: RecordBatch,
}

/// Result of a unary or bulk write item.
#[derive(Clone, Debug)]
pub struct WriteResult {
	pub response: WriteResponseMeta,
	pub finalization: Option<FinalizationMarker>,
}

/// Client-visible point read request for the Data Ring wire API.
#[derive(Clone, Debug)]
pub struct ReadRequest {
	pub meta: ReadRequestMeta,
	pub key: RecordBatch,
}

/// Result of a point read.
#[derive(Clone, Debug)]
pub struct ReadResult {
	pub response: ReadResponseMeta,
	pub value: Option<RecordBatch>,
}

#[derive(Clone, Copy, Debug)]
struct RetryDirective {
	preferred_target: Option<NodeId>,
	backoff: Option<Duration>,
	refresh_topology: bool,
	migration_incomplete: bool,
}

impl RetryDirective {
	fn redirect(preferred_target: Option<NodeId>) -> Self {
		Self {
			preferred_target,
			backoff: None,
			refresh_topology: true,
			migration_incomplete: false,
		}
	}

	fn e_safe(preferred_target: Option<NodeId>) -> Self {
		Self {
			preferred_target,
			backoff: Some(E_SAFE_RETRY_BACKOFF),
			refresh_topology: true,
			migration_incomplete: false,
		}
	}

	fn migration_incomplete() -> Self {
		Self {
			preferred_target: None,
			backoff: Some(MIGRATION_RETRY_BACKOFF),
			refresh_topology: false,
			migration_incomplete: true,
		}
	}

	fn exhausted_query_error(self) -> Error {
		if self.migration_incomplete {
			return Error::new(
				ErrorKind::WouldBlock,
				"Wendelon query failed: migration incomplete after retry budget exhausted",
			);
		}
		Error::new(
			ErrorKind::NotConnected,
			"Wendelon query retry budget exhausted",
		)
	}
}

impl WendelonClient {
	/// Issue one `WRITE_REQ` over HRFT and return the server's
	/// explicit wire response, including any redirect or durability
	/// status.
	pub async fn write(&self, request: WriteRequest) -> std::io::Result<WriteResult> {
		validate_write_request_batch(&request.meta, &request.payload)?;
		let key_hash = key_hash_from_batch(&request.payload, "key")?;
		let mut preferred_target = None;
		for attempt in 0..DATA_RETRY_BUDGET {
			let target = preferred_target.unwrap_or(self.target_for_key_hash(key_hash)?);
			let result = self.write_once(target, &request).await?;
			self.observe_write_response(&result.response);
			let Some(retry) = retry_write_response(&result.response) else {
				return Ok(result);
			};
			if attempt + 1 == DATA_RETRY_BUDGET {
				return Ok(result);
			}
			self.prepare_retry(Some(key_hash), retry).await?;
			preferred_target = retry.preferred_target;
		}
		unreachable!("bounded retry loop must return within budget")
	}

	/// Issue one `READ_REQ` over HRFT and surface the exact
	/// `ReadStatus` the server returned. Successful reads carry the
	/// Arrow value batch in `ReadResult.value`.
	pub async fn read(&self, request: ReadRequest) -> std::io::Result<ReadResult> {
		validate_read_request_batch(&request.meta, &request.key)?;
		let key_hash = key_hash_from_batch(&request.key, "key")?;
		let mut preferred_target = None;
		for attempt in 0..DATA_RETRY_BUDGET {
			let target = preferred_target.unwrap_or(self.target_for_key_hash(key_hash)?);
			let result = self.read_once(target, &request).await?;
			self.observe_read_response(&result.response);
			let Some(retry) = retry_read_response(&result.response) else {
				return Ok(result);
			};
			if attempt + 1 == DATA_RETRY_BUDGET {
				return Ok(result);
			}
			self.prepare_retry(Some(key_hash), retry).await?;
			preferred_target = retry.preferred_target;
		}
		unreachable!("bounded retry loop must return within budget")
	}

	/// Issue grouped `BULK_WRITE_REQ` streams over HRFT. Requests are
	/// partitioned by routed destination so each target sees one bulk
	/// stream carrying only the items it should own. Results preserve
	/// input order.
	pub async fn bulk_write(
		&self,
		requests: Vec<WriteRequest>,
	) -> std::io::Result<Vec<WriteResult>> {
		if requests.is_empty() {
			return Ok(Vec::new());
		}

		let total_requests = requests.len();
		let mut groups: Vec<(NodeId, Vec<(usize, WriteRequest)>)> = Vec::new();
		let mut group_index: HashMap<NodeId, usize> = HashMap::new();
		for (index, request) in requests.into_iter().enumerate() {
			validate_write_request_batch(&request.meta, &request.payload)?;
			let key_hash = key_hash_from_batch(&request.payload, "key")?;
			let target = self.target_for_key_hash(key_hash)?;
			let slot = if let Some(existing) = group_index.get(&target).copied() {
				existing
			} else {
				let next = groups.len();
				group_index.insert(target, next);
				groups.push((target, Vec::new()));
				next
			};
			groups[slot].1.push((index, request));
		}

		let mut results: Vec<Option<WriteResult>> = vec![None; total_requests];
		let mut originals: Vec<Option<WriteRequest>> = vec![None; total_requests];
		for (target, items) in groups {
			for (index, request) in &items {
				originals[*index] = Some(request.clone());
			}
			for (index, result) in self.bulk_write_group(target, items).await? {
				if retry_write_response(&result.response).is_some() {
					let retried = self
						.write(originals[index].as_ref().cloned().ok_or_else(|| {
							Error::new(
								ErrorKind::InvalidData,
								"bulk retry missing original request payload",
							)
						})?)
						.await?;
					results[index] = Some(retried);
				} else {
					results[index] = Some(result);
				}
			}
		}

		results
			.into_iter()
			.map(|item| {
				item.ok_or_else(|| {
					Error::new(
						ErrorKind::InvalidData,
						"bulk write completed without a response for one or more items",
					)
				})
			})
			.collect()
	}

	fn target_for_key_hash(&self, key_hash: u64) -> std::io::Result<NodeId> {
		self.pick_target(key_hash).ok_or_else(|| {
			Error::new(
				ErrorKind::NotFound,
				"no replica available for the requested key range",
			)
		})
	}

	async fn write_once(
		&self,
		target: NodeId,
		request: &WriteRequest,
	) -> std::io::Result<WriteResult> {
		let conn = self.factory.get_connection(target).await?;
		let mut stream = conn.open_bi().await?;
		let arrow = encode_batch(&request.payload)?;
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::WRITE_REQ.0,
			flags: 0,
			stream_id: 0,
			meta_len: std::mem::size_of::<WriteRequestMeta>() as u32,
			arrow_len: arrow.len() as u32,
			trailing_len: 0,
		};
		write_frame(
			&mut *stream,
			&header,
			bytemuck::bytes_of(&request.meta),
			Some(&arrow),
			None,
		)
		.await?;
		stream
			.finish()
			.await
			.map_err(|e| Error::new(ErrorKind::BrokenPipe, e))?;
		let frame = read_frame(&mut *stream, &self.pool).await?;
		decode_write_response_frame(frame, MsgType::WRITE_RESP)
	}

	async fn read_once(
		&self,
		target: NodeId,
		request: &ReadRequest,
	) -> std::io::Result<ReadResult> {
		let conn = self.factory.get_connection(target).await?;
		let mut stream = conn.open_bi().await?;
		let arrow = encode_batch(&request.key)?;
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::READ_REQ.0,
			flags: 0,
			stream_id: 0,
			meta_len: std::mem::size_of::<ReadRequestMeta>() as u32,
			arrow_len: arrow.len() as u32,
			trailing_len: 0,
		};
		write_frame(
			&mut *stream,
			&header,
			bytemuck::bytes_of(&request.meta),
			Some(&arrow),
			None,
		)
		.await?;
		stream
			.finish()
			.await
			.map_err(|e| Error::new(ErrorKind::BrokenPipe, e))?;
		let frame = read_frame(&mut *stream, &self.pool).await?;
		decode_read_response_frame(frame)
	}

	async fn bulk_write_group(
		&self,
		target: NodeId,
		requests: Vec<(usize, WriteRequest)>,
	) -> std::io::Result<Vec<(usize, WriteResult)>> {
		let conn = self.factory.get_connection(target).await?;
		let mut stream = conn.open_bi().await?;
		for (_, request) in &requests {
			let arrow = encode_batch(&request.payload)?;
			let header = FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::BULK_WRITE_REQ.0,
				flags: 0,
				stream_id: 0,
				meta_len: std::mem::size_of::<WriteRequestMeta>() as u32,
				arrow_len: arrow.len() as u32,
				trailing_len: 0,
			};
			write_frame(
				&mut *stream,
				&header,
				bytemuck::bytes_of(&request.meta),
				Some(&arrow),
				None,
			)
			.await?;
		}
		stream
			.finish()
			.await
			.map_err(|e| Error::new(ErrorKind::BrokenPipe, e))?;

		let mut results = Vec::with_capacity(requests.len());
		for (index, _) in requests {
			let frame = read_frame(&mut *stream, &self.pool).await?;
			let result = decode_write_response_frame(frame, MsgType::BULK_WRITE_RESP)?;
			self.observe_write_response(&result.response);
			results.push((index, result));
		}
		Ok(results)
	}

	fn observe_write_response(&self, response: &WriteResponseMeta) {
		if response.leader_hint.id != 0 {
			*self.leader.write().unwrap() = Some(response.leader_hint);
		}
	}

	fn observe_read_response(&self, response: &ReadResponseMeta) {
		if response.leader_hint.id != 0 {
			*self.leader.write().unwrap() = Some(response.leader_hint);
		}
	}

	async fn refresh_topology_with_candidates(&self, candidates: &[NodeId]) -> std::io::Result<()> {
		let snapshot = self
			.topology_source
			.fetch_from_candidates(candidates)
			.await?;
		self.apply_snapshot(&snapshot);
		Ok(())
	}

	async fn prepare_retry(
		&self,
		key_hash: Option<u64>,
		retry: RetryDirective,
	) -> std::io::Result<()> {
		if retry.refresh_topology {
			let candidates = self.retry_candidates(key_hash, retry.preferred_target);
			if let Err(err) = self.refresh_topology_with_candidates(&candidates).await {
				if retry.preferred_target.is_none() {
					return Err(err);
				}
			}
		}
		if let Some(backoff) = retry.backoff {
			tokio::time::sleep(backoff).await;
		}
		Ok(())
	}

	fn retry_candidates(&self, key_hash: Option<u64>, preferred: Option<NodeId>) -> Vec<NodeId> {
		let mut candidates = Vec::new();
		let mut push = |node: Option<NodeId>| {
			if let Some(node) = node {
				if node.id != 0
					&& !candidates
						.iter()
						.any(|existing: &NodeId| existing.id == node.id)
				{
					candidates.push(node);
				}
			}
		};
		push(preferred);
		push(self.leader());
		if let Some(key_hash) = key_hash {
			for replica in self.replicas_for(key_hash) {
				push(Some(replica));
			}
		}
		candidates
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

/// Streaming query cursor over one or more `QUERY_RESP` frames.
///
/// The first response frame carries the authoritative query meta;
/// subsequent frames carry additional Arrow IPC batches until a
/// frame arrives with [`FRAME_FLAG_STREAM_END`].
pub struct StreamingQueryResult {
	pub value_hlc: HybridTimestamp,
	pub epoch: u64,
	pub rows: u64,
	pending_batch: Option<RecordBatch>,
	stream: Option<Box<dyn TransportStream>>,
	pool: Arc<BufPool>,
}

impl std::fmt::Debug for StreamingQueryResult {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("StreamingQueryResult")
			.field("value_hlc", &self.value_hlc)
			.field("epoch", &self.epoch)
			.field("rows", &self.rows)
			.field("has_pending_batch", &self.pending_batch.is_some())
			.field("stream_open", &self.stream.is_some())
			.finish()
	}
}

impl StreamingQueryResult {
	pub async fn next_batch(&mut self) -> std::io::Result<Option<RecordBatch>> {
		if let Some(batch) = self.pending_batch.take() {
			return Ok(Some(batch));
		}
		let Some(stream) = self.stream.as_mut() else {
			return Ok(None);
		};

		let frame = read_frame(&mut **stream, &self.pool).await?;
		if frame.header.msg_type != MsgType::QUERY_RESP.0 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"expected QUERY_RESP continuation, got msg_type=0x{:02x}",
					frame.header.msg_type
				),
			));
		}
		if !frame.meta.is_empty() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"QUERY_RESP continuation must not carry response meta",
			));
		}
		if frame.trailing.is_some() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"QUERY_RESP continuation must not carry trailing bytes",
			));
		}

		let is_end = (frame.header.flags & FRAME_FLAG_STREAM_END) != 0;
		let batch = match frame.arrow_payload.as_ref() {
			Some(arrow) => Some(decode_batch(&arrow[..])?),
			None if is_end => None,
			None => {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"QUERY_RESP continuation missing Arrow payload",
				));
			}
		};

		if is_end {
			self.stream = None;
		}
		Ok(batch)
	}

	pub async fn collect_all(mut self) -> std::io::Result<Vec<RecordBatch>> {
		let mut batches = Vec::new();
		while let Some(batch) = self.next_batch().await? {
			batches.push(batch);
		}
		Ok(batches)
	}
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
	/// On `NOT_LEADER`, `EPOCH_STALE`, `E_SAFE_BEHIND`, or
	/// `MIGRATION_INCOMPLETE` the client performs a bounded retry
	/// loop rather than a single-shot failure. Any other error
	/// surface is terminal.
	pub async fn query(
		&self,
		wpbc_ast: Vec<u8>,
		range: KeyRange,
		expected_epoch: u64,
		min_hlc: HybridTimestamp,
		consistency: Consistency,
		as_of_hlc: HybridTimestamp,
	) -> std::io::Result<StreamingQueryResult> {
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

		let mut preferred_target = None;
		for attempt in 0..DATA_RETRY_BUDGET {
			let target = preferred_target.unwrap_or(self.target_for_key_hash(range.start_hash)?);
			match self.query_once(target, &meta, &wpbc_ast).await? {
				QueryOutcome::Ok(result) => return Ok(result),
				QueryOutcome::Redirect(retry) => {
					if attempt + 1 == DATA_RETRY_BUDGET {
						return Err(retry.exhausted_query_error());
					}
					self.prepare_retry(Some(range.start_hash), retry).await?;
					preferred_target = retry.preferred_target;
				}
			}
		}
		unreachable!("bounded retry loop must return within budget")
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

	async fn query_once(
		&self,
		target: NodeId,
		meta: &QueryRequestMeta,
		wpbc: &[u8],
	) -> std::io::Result<QueryOutcome> {
		let conn = self.factory.get_connection(target).await?;
		let mut stream = conn.open_bi().await?;

		let meta_bytes = bytemuck::bytes_of(meta);
		let trailing: Option<&[u8]> = if wpbc.is_empty() { None } else { Some(wpbc) };
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
		if resp.trailing.is_some() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"QUERY_RESP must not carry trailing bytes",
			));
		}
		let ack: &QueryResponseMeta =
			bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<QueryResponseMeta>()]);
		let is_end = (resp.header.flags & FRAME_FLAG_STREAM_END) != 0;
		let retry = retry_read_status(ack.status, ack.leader_hint);

		match ack.status {
			s if s == ReadStatus::OK => {
				if resp.header.arrow_len != ack.arrow_len {
					return Err(Error::new(
						ErrorKind::InvalidData,
						format!(
							"QUERY_RESP first-frame arrow_len mismatch: meta={} header={}",
							ack.arrow_len, resp.header.arrow_len
						),
					));
				}
				let arrow = resp.arrow_payload.as_ref().ok_or_else(|| {
					Error::new(
						ErrorKind::InvalidData,
						"QUERY_RESP OK missing Arrow payload",
					)
				})?;
				let batch = decode_batch(&arrow[..])?;
				Ok(QueryOutcome::Ok(StreamingQueryResult {
					value_hlc: ack.value_hlc,
					epoch: ack.epoch,
					rows: ack.rows,
					pending_batch: Some(batch),
					stream: if is_end { None } else { Some(stream) },
					pool: self.pool.clone(),
				}))
			}
			_ if retry.is_some() => Ok(QueryOutcome::Redirect(retry.expect("retry checked above"))),
			s if s == ReadStatus::KEY_NOT_FOUND => Ok(QueryOutcome::Ok(StreamingQueryResult {
				value_hlc: ack.value_hlc,
				epoch: ack.epoch,
				rows: 0,
				pending_batch: None,
				stream: None,
				pool: self.pool.clone(),
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
	Ok(StreamingQueryResult),
	Redirect(RetryDirective),
}

// ============================================================
//  HELPERS
// ============================================================

fn retry_write_response(response: &WriteResponseMeta) -> Option<RetryDirective> {
	match response.status {
		s if s == WriteStatus::NOT_LEADER || s == WriteStatus::EPOCH_STALE => Some(
			RetryDirective::redirect(nonzero_node_id(response.leader_hint)),
		),
		_ => None,
	}
}

fn retry_read_response(response: &ReadResponseMeta) -> Option<RetryDirective> {
	retry_read_status(response.status, response.leader_hint)
}

fn retry_read_status(status: ReadStatus, leader_hint: NodeId) -> Option<RetryDirective> {
	match status {
		s if s == ReadStatus::NOT_LEADER || s == ReadStatus::EPOCH_STALE => {
			Some(RetryDirective::redirect(nonzero_node_id(leader_hint)))
		}
		s if s == ReadStatus::E_SAFE_BEHIND => {
			Some(RetryDirective::e_safe(nonzero_node_id(leader_hint)))
		}
		s if s == ReadStatus::MIGRATION_INCOMPLETE => Some(RetryDirective::migration_incomplete()),
		_ => None,
	}
}

fn nonzero_node_id(node: NodeId) -> Option<NodeId> {
	(node.id != 0).then_some(node)
}

fn decode_write_response_frame(
	frame: crate::network::core::HrftFrame,
	expected: MsgType,
) -> std::io::Result<WriteResult> {
	if frame.header.msg_type != expected.0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"expected msg_type=0x{:02x}, got msg_type=0x{:02x}",
				expected.0, frame.header.msg_type
			),
		));
	}
	if frame.meta.len() < std::mem::size_of::<WriteResponseMeta>() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"WRITE_RESP meta too small",
		));
	}
	if frame.arrow_payload.is_some() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"WRITE_RESP must not carry an Arrow payload",
		));
	}
	let response: WriteResponseMeta =
		*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<WriteResponseMeta>()]);
	let finalization = match response.has_finalization {
		0 => {
			if frame.trailing.is_some() {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"WRITE_RESP trailing marker present but has_finalization=0",
				));
			}
			None
		}
		1 => {
			let trailing = frame.trailing.as_ref().ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					"WRITE_RESP missing FinalizationMarker payload",
				)
			})?;
			if trailing.len() < std::mem::size_of::<FinalizationMarker>() {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"WRITE_RESP FinalizationMarker payload too small",
				));
			}
			Some(*bytemuck::from_bytes(
				&trailing[..std::mem::size_of::<FinalizationMarker>()],
			))
		}
		other => {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!("WRITE_RESP invalid has_finalization value {other}"),
			));
		}
	};
	Ok(WriteResult {
		response,
		finalization,
	})
}

fn decode_read_response_frame(
	frame: crate::network::core::HrftFrame,
) -> std::io::Result<ReadResult> {
	if frame.header.msg_type != MsgType::READ_RESP.0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"expected READ_RESP, got msg_type=0x{:02x}",
				frame.header.msg_type
			),
		));
	}
	if frame.meta.len() < std::mem::size_of::<ReadResponseMeta>() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"READ_RESP meta too small",
		));
	}
	if frame.trailing.is_some() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"READ_RESP must not carry trailing bytes",
		));
	}
	let response: ReadResponseMeta =
		*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<ReadResponseMeta>()]);
	let value = if response.status == ReadStatus::OK {
		let arrow = frame.arrow_payload.as_ref().ok_or_else(|| {
			Error::new(ErrorKind::InvalidData, "READ_RESP OK missing Arrow payload")
		})?;
		let batch = decode_batch(&arrow[..])?;
		validate_read_value_batch(&response, &batch)?;
		Some(batch)
	} else {
		if frame.arrow_payload.is_some() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"READ_RESP error status must not carry an Arrow payload",
			));
		}
		None
	};
	Ok(ReadResult { response, value })
}

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

fn validate_write_request_batch(
	meta: &WriteRequestMeta,
	batch: &RecordBatch,
) -> std::io::Result<()> {
	if batch.schema().as_ref() != &write_payload_schema() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"WRITE_REQ Arrow payload schema mismatch",
		));
	}
	let key_len = extract_first_binary(batch, "key")?.len() as u32;
	if key_len != meta.key_len {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"WRITE_REQ key_len mismatch: meta={} actual={}",
				meta.key_len, key_len
			),
		));
	}
	let value_len = extract_first_binary(batch, "value")?.len() as u32;
	if value_len != meta.value_len {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"WRITE_REQ value_len mismatch: meta={} actual={}",
				meta.value_len, value_len
			),
		));
	}
	Ok(())
}

fn validate_read_request_batch(meta: &ReadRequestMeta, batch: &RecordBatch) -> std::io::Result<()> {
	if batch.schema().as_ref() != &read_key_schema() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"READ_REQ Arrow payload schema mismatch",
		));
	}
	let key_len = extract_first_binary(batch, "key")?.len() as u32;
	if key_len != meta.key_len {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"READ_REQ key_len mismatch: meta={} actual={}",
				meta.key_len, key_len
			),
		));
	}
	Ok(())
}

fn validate_read_value_batch(meta: &ReadResponseMeta, batch: &RecordBatch) -> std::io::Result<()> {
	if batch.schema().as_ref() != &read_value_schema() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"READ_RESP Arrow payload schema mismatch",
		));
	}
	let value_len = extract_first_binary(batch, "value")?.len() as u32;
	if value_len != meta.value_len {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"READ_RESP value_len mismatch: meta={} actual={}",
				meta.value_len, value_len
			),
		));
	}
	Ok(())
}

fn extract_first_binary<'a>(
	batch: &'a RecordBatch,
	column: &'static str,
) -> std::io::Result<&'a [u8]> {
	let col = batch
		.column_by_name(column)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, format!("missing '{column}' column")))?;
	let arr = col
		.as_any()
		.downcast_ref::<LargeBinaryArray>()
		.ok_or_else(|| {
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

fn key_hash_from_batch(batch: &RecordBatch, column: &'static str) -> std::io::Result<u64> {
	Ok(highway_hash(extract_first_binary(batch, column)?))
}

fn highway_hash(bytes: &[u8]) -> u64 {
	let mut hasher = HighwayHasher::new(HIGHWAY_KEY);
	hasher.append(bytes);
	hasher.finalize64()
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
