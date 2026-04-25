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
//!    a given routing locality without a proxy hop.
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
use crate::network::factory::TransportProviderFactory;
use crate::network::pool::BufPool;
use crate::network::ring::{RingRouter, RingState};
use crate::network::transport::{
	CapabilityResolver, EpochCapabilityResolver, MembershipEpoch, TransportProvider,
	TransportStream, TransportTier, TransportTierSet,
};
use crate::schema::{
	Consistency, FRAME_FLAG_STREAM_END, FinalizationMarker, FrameHeader, HybridTimestamp, Identity,
	Locality, MsgType, NodeId, QueryRequestMeta, QueryResponseMeta, ReadRequestMeta,
	ReadResponseMeta, ReadStatus, TableQueryRequestMeta, TableSchemaDescriptorV1,
	TableWriteRequestMeta, TableWriteResponseMeta, TenantId, WpbcRejectCode, WriteRequestMeta,
	WriteResponseMeta, WriteStatus, column_projection_prefix_ranges, read_key_schema,
	read_value_schema, table_spread_prefix_ranges, validate_table_write_request_batch,
	write_payload_schema,
};
use arrow::array::{Array, LargeBinaryArray};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::io::{Cursor, Error, ErrorKind};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::watch;

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
/// Small pause before retrying a write whose payload is already
/// durable but whose Control Ring finalization has not yet
/// committed.
pub const FINALIZATION_RETRY_BACKOFF: Duration = Duration::from_millis(40);
/// Small pause before retrying a topology/lease readiness miss.
pub const LEASE_REFRESH_RETRY_BACKOFF: Duration = Duration::from_millis(25);
/// Small pause before retrying a bounded post-auth admission miss.
pub const BACKPRESSURE_RETRY_BACKOFF: Duration = Duration::from_millis(20);

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
	/// Cached epoch from the latest topology fetch.
	epoch: RwLock<u64>,
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
		let epoch = config.bootstrap.epoch;

		Arc::new(Self {
			factory,
			pool,
			router,
			topology_tx,
			topology_source: config.topology,
			leader: RwLock::new(leader),
			rf: RwLock::new(rf),
			epoch: RwLock::new(epoch),
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
		*self.epoch.write().unwrap() = snapshot.epoch;
		*self.leader.write().unwrap() = snapshot.leader.map(|id| NodeId { id, _reserved: 0 });
		let _ = self.topology_tx.send(Arc::new(membership));
	}

	/// Replicas that should own `locality`. Returned in the
	/// deterministic order the current router produces.
	pub fn replicas_for_locality(&self, locality: Locality) -> Vec<NodeId> {
		self.router.replicas_for_locality(locality)
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

	/// Current epoch from the latest applied topology snapshot.
	pub fn epoch(&self) -> u64 {
		*self.epoch.read().unwrap()
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

/// Client-visible typed table write request for ADR 037.
#[derive(Clone, Debug)]
pub struct TableWriteRequest {
	pub meta: TableWriteRequestMeta,
	pub descriptor: TableSchemaDescriptorV1,
	pub payload: RecordBatch,
}

/// Result of a typed table write.
#[derive(Clone, Debug)]
pub struct TableWriteResult {
	pub response: TableWriteResponseMeta,
	pub finalization: Option<FinalizationMarker>,
}

/// Client-visible typed table scan request for ADR 037.
#[derive(Clone, Debug)]
pub struct TableQueryRequest {
	pub meta: TableQueryRequestMeta,
	pub descriptor: TableSchemaDescriptorV1,
	pub wpbc: Vec<u8>,
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

	fn finalization_pending(preferred_target: Option<NodeId>) -> Self {
		Self {
			preferred_target,
			backoff: Some(FINALIZATION_RETRY_BACKOFF),
			refresh_topology: true,
			migration_incomplete: false,
		}
	}

	fn lease_refresh(preferred_target: Option<NodeId>) -> Self {
		Self {
			preferred_target,
			backoff: Some(LEASE_REFRESH_RETRY_BACKOFF),
			refresh_topology: true,
			migration_incomplete: false,
		}
	}

	fn backpressure(preferred_target: Option<NodeId>) -> Self {
		Self {
			preferred_target,
			backoff: Some(BACKPRESSURE_RETRY_BACKOFF),
			refresh_topology: false,
			migration_incomplete: false,
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
		let key_locality = Locality::from(request.meta.key_identity);
		let mut preferred_target = None;
		for attempt in 0..DATA_RETRY_BUDGET {
			let target = preferred_target.unwrap_or(self.target_for_locality(key_locality)?);
			let result = self.write_once(target, &request).await?;
			self.observe_write_response(&result.response);
			let Some(retry) = retry_write_response(&result.response) else {
				return Ok(result);
			};
			if attempt + 1 == DATA_RETRY_BUDGET {
				return Ok(result);
			}
			self.prepare_retry(Some(key_locality), retry).await?;
			preferred_target = retry.preferred_target;
		}
		unreachable!("bounded retry loop must return within budget")
	}

	/// Issue one `TABLE_WRITE_REQ` over HRFT. The accepting data node
	/// may split a multi-row batch across multiple replica sets while
	/// preserving one parent finalization for the logical write.
	pub async fn table_write(
		&self,
		request: TableWriteRequest,
	) -> std::io::Result<TableWriteResult> {
		let admission = validate_table_write_request_batch(
			&request.meta,
			&request.payload,
			&request.descriptor,
		)
		.map_err(|err| Error::new(ErrorKind::InvalidInput, err.to_string()))?;
		let Some(key_locality) = admission.localities.first().copied() else {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"TABLE_WRITE_REQ must contain at least one row",
			));
		};
		let mut preferred_target = None;
		for attempt in 0..DATA_RETRY_BUDGET {
			let target = preferred_target.unwrap_or(self.target_for_locality(key_locality)?);
			let result = self.table_write_once(target, &request).await?;
			self.observe_write_response(&result.response.write);
			if result.response.reject_code != WpbcRejectCode::NONE.0 {
				return Ok(result);
			}
			let Some(retry) = retry_write_response(&result.response.write) else {
				return Ok(result);
			};
			if attempt + 1 == DATA_RETRY_BUDGET {
				return Ok(result);
			}
			self.prepare_retry(Some(key_locality), retry).await?;
			preferred_target = retry.preferred_target;
		}
		unreachable!("bounded retry loop must return within budget")
	}

	/// Issue one `TABLE_QUERY_REQ` over HRFT. The descriptor is
	/// carried client-side so the smart client can route the request
	/// to a table-owned locality arc while the server validates the
	/// authoritative schema generation from its catalog.
	pub async fn table_query(
		&self,
		request: TableQueryRequest,
	) -> std::io::Result<StreamingQueryResult> {
		validate_table_query_request(&request)?;
		let route_locality =
			table_query_route_locality(request.meta.tenant_id, &request.descriptor)?;
		let mut preferred_target = None;
		for attempt in 0..DATA_RETRY_BUDGET {
			let target = preferred_target.unwrap_or(self.target_for_locality(route_locality)?);
			match self.table_query_once(target, &request).await? {
				QueryOutcome::Ok(result) => return Ok(result),
				QueryOutcome::Redirect(retry) => {
					if attempt + 1 == DATA_RETRY_BUDGET {
						return Err(retry.exhausted_query_error());
					}
					self.prepare_retry(Some(route_locality), retry).await?;
					preferred_target = retry.preferred_target;
				}
			}
		}
		unreachable!("bounded retry loop must return within budget")
	}

	/// Issue one `READ_REQ` over HRFT and surface the exact
	/// `ReadStatus` the server returned. Successful reads carry the
	/// Arrow value batch in `ReadResult.value`.
	pub async fn read(&self, request: ReadRequest) -> std::io::Result<ReadResult> {
		validate_read_request_batch(&request.meta, &request.key)?;
		let key_locality = Locality::from(request.meta.key_identity);
		let mut preferred_target = None;
		for attempt in 0..DATA_RETRY_BUDGET {
			let target = preferred_target.unwrap_or(self.target_for_locality(key_locality)?);
			let result = self.read_once(target, &request).await?;
			self.observe_read_response(&result.response);
			let Some(retry) = retry_read_response(&result.response) else {
				return Ok(result);
			};
			if attempt + 1 == DATA_RETRY_BUDGET {
				return Ok(result);
			}
			self.prepare_retry(Some(key_locality), retry).await?;
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
			let key_locality = Locality::from(request.meta.key_identity);
			let target = self.target_for_locality(key_locality)?;
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

	fn target_for_locality(&self, locality: Locality) -> std::io::Result<NodeId> {
		self.pick_target(locality).ok_or_else(|| {
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

	async fn table_write_once(
		&self,
		target: NodeId,
		request: &TableWriteRequest,
	) -> std::io::Result<TableWriteResult> {
		let conn = self.factory.get_connection(target).await?;
		let mut stream = conn.open_bi().await?;
		let arrow = encode_batch(&request.payload)?;
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::TABLE_WRITE_REQ.0,
			flags: 0,
			stream_id: 0,
			meta_len: std::mem::size_of::<TableWriteRequestMeta>() as u32,
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
		decode_table_write_response_frame(frame)
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

	async fn table_query_once(
		&self,
		target: NodeId,
		request: &TableQueryRequest,
	) -> std::io::Result<QueryOutcome> {
		let conn = self.factory.get_connection(target).await?;
		let mut stream = conn.open_bi().await?;

		let meta_bytes = bytemuck::bytes_of(&request.meta);
		let trailing: Option<&[u8]> = if request.wpbc.is_empty() {
			None
		} else {
			Some(&request.wpbc)
		};
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::TABLE_QUERY_REQ.0,
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
		decode_initial_query_response(resp, stream, self.pool.clone(), MsgType::TABLE_QUERY_RESP)
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
		locality: Option<Locality>,
		retry: RetryDirective,
	) -> std::io::Result<()> {
		if retry.refresh_topology {
			let candidates = self.retry_candidates(locality, retry.preferred_target);
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

	fn retry_candidates(
		&self,
		locality: Option<Locality>,
		preferred: Option<NodeId>,
	) -> Vec<NodeId> {
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
		if let Some(locality) = locality {
			for replica in self.replicas_for_locality(locality) {
				push(Some(replica));
			}
		}
		candidates
	}
}

// ============================================================
//  QUERY API
// ============================================================

/// Inclusive routing-locality range for a pushdown scan.
///
/// Wendelon routes records by the 64-bit `Locality` projection of
/// their 256-bit `Identity`. `start == end` collapses to a single
/// locality bucket / point scan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueryLocalityRange {
	pub start_locality: Locality,
	pub end_locality: Locality,
}

impl QueryLocalityRange {
	pub fn point(locality: impl Into<Locality>) -> Self {
		let locality = locality.into();
		Self {
			start_locality: locality,
			end_locality: locality,
		}
	}

	pub fn full() -> Self {
		Self {
			start_locality: Locality::ZERO,
			end_locality: Locality::MAX,
		}
	}
}

pub type KeyRange = QueryLocalityRange;

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
	expected_msg_type: MsgType,
}

impl std::fmt::Debug for StreamingQueryResult {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("StreamingQueryResult")
			.field("value_hlc", &self.value_hlc)
			.field("epoch", &self.epoch)
			.field("rows", &self.rows)
			.field("has_pending_batch", &self.pending_batch.is_some())
			.field("stream_open", &self.stream.is_some())
			.field("expected_msg_type", &self.expected_msg_type.0)
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
		if frame.header.msg_type != self.expected_msg_type.0 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"expected query continuation msg_type=0x{:02x}, got msg_type=0x{:02x}",
					self.expected_msg_type.0, frame.header.msg_type
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
	/// Push a WPBC program to the replica that owns
	/// `range.start_locality`
	/// and stream the filtered Arrow IPC result back.
	///
	/// * `wpbc_ast` — raw WPBC bytecode. Empty is legal and means
	///   "no filter, no projection".
	/// * `range` — inclusive locality window. V1 targets the owner
	///   of `start_locality`; a true cross-replica scan fan-out is
	///   deferred.
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
		tenant_id: TenantId,
		wpbc_ast: Vec<u8>,
		range: QueryLocalityRange,
		expected_epoch: u64,
		min_hlc: HybridTimestamp,
		consistency: Consistency,
		as_of_hlc: HybridTimestamp,
	) -> std::io::Result<StreamingQueryResult> {
		let meta = QueryRequestMeta {
			min_hlc,
			expected_epoch,
			tenant_id,
			consistency,
			_pad: [0; 7],
			locality_range_start: range.start_locality.get(),
			locality_range_end: range.end_locality.get(),
			bytecode_len: wpbc_ast.len() as u32,
			_pad2: [0; 4],
			as_of_hlc,
		};

		let mut preferred_target = None;
		for attempt in 0..DATA_RETRY_BUDGET {
			let target =
				preferred_target.unwrap_or(self.target_for_locality(range.start_locality)?);
			match self.query_once(target, &meta, &wpbc_ast).await? {
				QueryOutcome::Ok(result) => return Ok(result),
				QueryOutcome::Redirect(retry) => {
					if attempt + 1 == DATA_RETRY_BUDGET {
						return Err(retry.exhausted_query_error());
					}
					self.prepare_retry(Some(range.start_locality), retry)
						.await?;
					preferred_target = retry.preferred_target;
				}
			}
		}
		unreachable!("bounded retry loop must return within budget")
	}

	/// First-pass dispatch target: the head of the replica set for
	/// `locality`, preferring the local leader when the leader is
	/// actually a replica for that range. Returns `None` when the
	/// router has no nodes — callers surface that as a clean
	/// `NotFound` so the topology can be re-probed.
	fn pick_target(&self, locality: Locality) -> Option<NodeId> {
		let replicas = self.replicas_for_locality(locality);
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
		decode_initial_query_response(resp, stream, self.pool.clone(), MsgType::QUERY_RESP)
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
		s if s == WriteStatus::LEASE_EXPIRED || s == WriteStatus::INVALID_LEASE_SIGNATURE => Some(
			RetryDirective::lease_refresh(nonzero_node_id(response.leader_hint)),
		),
		s if s == WriteStatus::FINALIZATION_PENDING => Some(RetryDirective::finalization_pending(
			nonzero_node_id(response.leader_hint),
		)),
		s if s == WriteStatus::BACKPRESSURED => Some(RetryDirective::backpressure(
			nonzero_node_id(response.leader_hint),
		)),
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
		s if s == ReadStatus::LEASE_EXPIRED || s == ReadStatus::INVALID_LEASE_SIGNATURE => {
			Some(RetryDirective::lease_refresh(nonzero_node_id(leader_hint)))
		}
		s if s == ReadStatus::E_SAFE_BEHIND => {
			Some(RetryDirective::e_safe(nonzero_node_id(leader_hint)))
		}
		s if s == ReadStatus::BACKPRESSURED => {
			Some(RetryDirective::backpressure(nonzero_node_id(leader_hint)))
		}
		s if s == ReadStatus::MIGRATION_INCOMPLETE => Some(RetryDirective::migration_incomplete()),
		_ => None,
	}
}

fn query_failure_error(status: ReadStatus) -> Error {
	match status {
		s if s == ReadStatus::RETENTION_EXPIRED => {
			Error::new(ErrorKind::Other, "Wendelon query failed: RETENTION_EXPIRED")
		}
		s if s == ReadStatus::QUERY_INVALID_PROGRAM => Error::new(
			ErrorKind::InvalidInput,
			"Wendelon query failed: QUERY_INVALID_PROGRAM",
		),
		s if s == ReadStatus::QUERY_BUDGET_EXCEEDED => Error::new(
			ErrorKind::Other,
			"Wendelon query failed: QUERY_BUDGET_EXCEEDED",
		),
		other => Error::new(
			ErrorKind::Other,
			format!("Wendelon query failed: status={}", other.0),
		),
	}
}

fn decode_initial_query_response(
	resp: crate::network::core::HrftFrame,
	stream: Box<dyn TransportStream>,
	pool: Arc<BufPool>,
	expected_msg_type: MsgType,
) -> std::io::Result<QueryOutcome> {
	if resp.header.msg_type != expected_msg_type.0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"expected query response msg_type=0x{:02x}, got msg_type=0x{:02x}",
				expected_msg_type.0, resp.header.msg_type
			),
		));
	}
	if resp.meta.len() < std::mem::size_of::<QueryResponseMeta>() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"query response meta too small",
		));
	}
	if resp.trailing.is_some() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"query response must not carry trailing bytes",
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
						"query response first-frame arrow_len mismatch: meta={} header={}",
						ack.arrow_len, resp.header.arrow_len
					),
				));
			}
			let arrow = resp.arrow_payload.as_ref().ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					"query response OK missing Arrow payload",
				)
			})?;
			let batch = decode_batch(&arrow[..])?;
			Ok(QueryOutcome::Ok(StreamingQueryResult {
				value_hlc: ack.value_hlc,
				epoch: ack.epoch,
				rows: ack.rows,
				pending_batch: Some(batch),
				stream: if is_end { None } else { Some(stream) },
				pool,
				expected_msg_type,
			}))
		}
		_ if retry.is_some() => Ok(QueryOutcome::Redirect(retry.expect("retry checked above"))),
		s if s == ReadStatus::KEY_NOT_FOUND => Ok(QueryOutcome::Ok(StreamingQueryResult {
			value_hlc: ack.value_hlc,
			epoch: ack.epoch,
			rows: 0,
			pending_batch: None,
			stream: None,
			pool,
			expected_msg_type,
		})),
		other => Err(query_failure_error(other)),
	}
}

fn validate_table_query_request(request: &TableQueryRequest) -> std::io::Result<()> {
	request.descriptor.validate()?;
	if request.meta.table_object_id != request.descriptor.table_object_id {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"TABLE_QUERY_REQ table_object_id does not match descriptor",
		));
	}
	if request.meta.schema_generation != request.descriptor.schema_generation {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"TABLE_QUERY_REQ schema_generation does not match descriptor",
		));
	}
	let bytecode_len = u32::try_from(request.wpbc.len()).map_err(|_| {
		Error::new(
			ErrorKind::InvalidInput,
			"TABLE_QUERY_REQ WPBC bytecode exceeds u32 length",
		)
	})?;
	if request.meta.bytecode_len != bytecode_len {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			format!(
				"TABLE_QUERY_REQ bytecode_len mismatch: meta={} actual={}",
				request.meta.bytecode_len, bytecode_len
			),
		));
	}
	Ok(())
}

fn table_query_route_locality(
	tenant_id: TenantId,
	descriptor: &TableSchemaDescriptorV1,
) -> std::io::Result<Locality> {
	match descriptor.locality_policy {
		crate::schema::LocalityPolicyV1::TableSpreadProjection(spread) => {
			let ranges = table_spread_prefix_ranges(tenant_id, descriptor.table_object_id, spread)?;
			ranges.first().map(|(start, _)| *start).ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidInput,
					"table spread descriptor produced no scan ranges",
				)
			})
		}
		crate::schema::LocalityPolicyV1::IdentityProjection => Ok(Locality::ZERO),
		crate::schema::LocalityPolicyV1::ColumnProjection(ref projection) => {
			let ranges =
				column_projection_prefix_ranges(tenant_id, descriptor.table_object_id, projection)?;
			ranges.first().map(|(start, _)| *start).ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidInput,
					"column projection descriptor produced no scan ranges",
				)
			})
		}
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

fn decode_table_write_response_frame(
	frame: crate::network::core::HrftFrame,
) -> std::io::Result<TableWriteResult> {
	if frame.header.msg_type != MsgType::TABLE_WRITE_RESP.0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"expected TABLE_WRITE_RESP, got msg_type=0x{:02x}",
				frame.header.msg_type
			),
		));
	}
	if frame.meta.len() < std::mem::size_of::<TableWriteResponseMeta>() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"TABLE_WRITE_RESP meta too small",
		));
	}
	if frame.arrow_payload.is_some() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"TABLE_WRITE_RESP must not carry an Arrow payload",
		));
	}
	let response: TableWriteResponseMeta =
		*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<TableWriteResponseMeta>()]);
	let finalization = match response.write.has_finalization {
		0 => {
			if frame.trailing.is_some() {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"TABLE_WRITE_RESP trailing marker present but has_finalization=0",
				));
			}
			None
		}
		1 => {
			let trailing = frame.trailing.as_ref().ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					"TABLE_WRITE_RESP missing FinalizationMarker payload",
				)
			})?;
			if trailing.len() < std::mem::size_of::<FinalizationMarker>() {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"TABLE_WRITE_RESP FinalizationMarker payload too small",
				));
			}
			Some(*bytemuck::from_bytes(
				&trailing[..std::mem::size_of::<FinalizationMarker>()],
			))
		}
		other => {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!("TABLE_WRITE_RESP invalid has_finalization value {other}"),
			));
		}
	};
	Ok(TableWriteResult {
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
	validate_key_identity(
		meta.tenant_id,
		meta.key_identity,
		extract_first_binary(batch, "key")?,
	)?;
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
	validate_key_identity(
		meta.tenant_id,
		meta.key_identity,
		extract_first_binary(batch, "key")?,
	)?;
	Ok(())
}

fn validate_key_identity(
	tenant_id: TenantId,
	expected: Identity,
	key_bytes: &[u8],
) -> std::io::Result<()> {
	let computed = Identity::blake3_tenant(tenant_id, key_bytes);
	if expected == computed {
		return Ok(());
	}
	Err(Error::new(
		ErrorKind::InvalidData,
		format!(
			"key_identity mismatch: meta={} computed={}",
			expected, computed
		),
	))
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

// ============================================================
//  TESTS
// ============================================================

#[cfg(test)]
mod tests {
	use super::*;
	use crate::client::topology::{ClusterPeerWire, StaticTopologySource};
	use crate::network::transport::TransportTier;
	use crate::schema::{
		ClientWriteId, ClusteringColumnSpecV1, ColumnLocalityDescriptorV1, LocalityPolicyV1,
		PrimaryKeyDescriptorV1, ProjectionFunctionV1, SchemaEvolutionPolicyV1, TableObjectId,
		TableSchemaDescriptorV1, TableSpreadLocalityDescriptorV1,
	};
	use arrow::datatypes::{DataType, Field, Schema};

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

	fn table_query_descriptor(locality_policy: LocalityPolicyV1) -> TableSchemaDescriptorV1 {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, true),
		]);
		TableSchemaDescriptorV1::new(
			TableObjectId::new([0x51; 16]),
			3,
			"client",
			"items",
			&schema,
			PrimaryKeyDescriptorV1::single("id"),
			locality_policy,
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap()
	}

	#[tokio::test]
	async fn bootstrap_populates_router_and_leader() {
		let snap = snapshot(5, Some(2), &[1, 2, 3]);
		let client = WendelonClient::new(test_config(snap));
		let replicas = client.replicas_for_locality(Locality::ZERO);
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

		let replicas = client.replicas_for_locality(Locality::ZERO);
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

	#[test]
	fn retry_read_status_keeps_terminal_query_failures_non_retryable() {
		let zero = NodeId {
			id: 0,
			_reserved: 0,
		};
		assert!(retry_read_status(ReadStatus::QUERY_INVALID_PROGRAM, zero).is_none());
		assert!(retry_read_status(ReadStatus::QUERY_BUDGET_EXCEEDED, zero).is_none());
		assert!(retry_read_status(ReadStatus::RETENTION_EXPIRED, zero).is_none());
	}

	#[test]
	fn table_query_route_uses_first_table_spread_prefix() {
		let tenant_id = TenantId::new(9);
		let spread = TableSpreadLocalityDescriptorV1 {
			spread_factor: 4,
			prefix_bits: 12,
		};
		let descriptor = table_query_descriptor(LocalityPolicyV1::TableSpreadProjection(spread));
		let expected =
			table_spread_prefix_ranges(tenant_id, descriptor.table_object_id, spread).unwrap()[0].0;

		assert_eq!(
			table_query_route_locality(tenant_id, &descriptor).unwrap(),
			expected
		);
	}

	#[test]
	fn table_query_route_uses_first_column_projection_prefix() {
		let tenant_id = TenantId::new(9);
		let projection = ColumnLocalityDescriptorV1 {
			columns: vec![ClusteringColumnSpecV1 {
				name: "id".to_owned(),
				width_bits: 16,
				string_prefix_bytes: None,
			}],
			projection: ProjectionFunctionV1::ZOrder,
			spread_factor: 4,
			prefix_bits: 12,
		};
		let descriptor =
			table_query_descriptor(LocalityPolicyV1::ColumnProjection(projection.clone()));
		let expected =
			column_projection_prefix_ranges(tenant_id, descriptor.table_object_id, &projection)
				.unwrap()[0]
				.0;

		assert_eq!(
			table_query_route_locality(tenant_id, &descriptor).unwrap(),
			expected
		);
	}

	#[test]
	fn table_query_request_validation_checks_descriptor_and_bytecode_len() {
		let descriptor = table_query_descriptor(LocalityPolicyV1::default_table_spread());
		let request = TableQueryRequest {
			meta: TableQueryRequestMeta {
				min_hlc: bytemuck::Zeroable::zeroed(),
				expected_epoch: 7,
				tenant_id: TenantId::new(9),
				table_object_id: descriptor.table_object_id,
				schema_generation: descriptor.schema_generation,
				consistency: Consistency::LINEARIZABLE,
				_pad: [0; 7],
				bytecode_len: 2,
				_pad2: [0; 4],
				as_of_hlc: bytemuck::Zeroable::zeroed(),
			},
			descriptor,
			wpbc: vec![0x01, 0x02],
		};
		validate_table_query_request(&request).unwrap();

		let mut bad = request.clone();
		bad.meta.bytecode_len = 1;
		let err = validate_table_query_request(&bad).unwrap_err();
		assert_eq!(err.kind(), ErrorKind::InvalidInput);
	}

	#[test]
	fn retry_write_status_treats_finalization_pending_as_retryable() {
		let leader = NodeId {
			id: 7,
			_reserved: 0,
		};
		let response = WriteResponseMeta {
			status: WriteStatus::FINALIZATION_PENDING,
			_pad1: [0; 7],
			log_index: 0,
			term: 0,
			epoch: 11,
			commit_hlc: HybridTimestamp {
				physical_ms: 123,
				logical: 4,
				node_id: 7,
			},
			leader_hint: leader,
			echoed_write_id: ClientWriteId {
				client_uuid: [0xAB; 16],
				sequence_no: 9,
				idempotency_ttl_ms: 60_000,
				_pad: 0,
			},
			has_finalization: 0,
			_pad2: [0; 7],
		};

		let retry = retry_write_response(&response).expect("status must be retryable");
		assert_eq!(retry.preferred_target, Some(leader));
		assert_eq!(retry.backoff, Some(FINALIZATION_RETRY_BACKOFF));
		assert!(retry.refresh_topology);
	}

	#[test]
	fn retry_write_status_treats_lease_gate_failures_as_retryable() {
		let leader = NodeId {
			id: 7,
			_reserved: 0,
		};
		for status in [
			WriteStatus::LEASE_EXPIRED,
			WriteStatus::INVALID_LEASE_SIGNATURE,
		] {
			let response = WriteResponseMeta {
				status,
				_pad1: [0; 7],
				log_index: 0,
				term: 0,
				epoch: 11,
				commit_hlc: HybridTimestamp {
					physical_ms: 123,
					logical: 4,
					node_id: 7,
				},
				leader_hint: leader,
				echoed_write_id: ClientWriteId {
					client_uuid: [0xAB; 16],
					sequence_no: 9,
					idempotency_ttl_ms: 60_000,
					_pad: 0,
				},
				has_finalization: 0,
				_pad2: [0; 7],
			};

			let retry = retry_write_response(&response).expect("status must be retryable");
			assert_eq!(retry.preferred_target, Some(leader));
			assert_eq!(retry.backoff, Some(LEASE_REFRESH_RETRY_BACKOFF));
			assert!(retry.refresh_topology);
		}
	}

	#[test]
	fn retry_write_status_treats_backpressure_as_retryable_without_refresh() {
		let response = WriteResponseMeta {
			status: WriteStatus::BACKPRESSURED,
			_pad1: [0; 7],
			log_index: 0,
			term: 0,
			epoch: 11,
			commit_hlc: HybridTimestamp {
				physical_ms: 123,
				logical: 4,
				node_id: 7,
			},
			leader_hint: NodeId {
				id: 0,
				_reserved: 0,
			},
			echoed_write_id: ClientWriteId {
				client_uuid: [0xAB; 16],
				sequence_no: 9,
				idempotency_ttl_ms: 60_000,
				_pad: 0,
			},
			has_finalization: 0,
			_pad2: [0; 7],
		};

		let retry = retry_write_response(&response).expect("status must be retryable");
		assert_eq!(retry.preferred_target, None);
		assert_eq!(retry.backoff, Some(BACKPRESSURE_RETRY_BACKOFF));
		assert!(!retry.refresh_topology);
	}

	#[test]
	fn retry_read_status_treats_lease_gate_failures_as_retryable() {
		let leader = NodeId {
			id: 7,
			_reserved: 0,
		};
		for status in [
			ReadStatus::LEASE_EXPIRED,
			ReadStatus::INVALID_LEASE_SIGNATURE,
		] {
			let retry = retry_read_status(status, leader).expect("status must be retryable");
			assert_eq!(retry.preferred_target, Some(leader));
			assert_eq!(retry.backoff, Some(LEASE_REFRESH_RETRY_BACKOFF));
			assert!(retry.refresh_topology);
		}
	}

	#[test]
	fn retry_read_status_treats_backpressure_as_retryable_without_refresh() {
		let zero = NodeId {
			id: 0,
			_reserved: 0,
		};
		let retry =
			retry_read_status(ReadStatus::BACKPRESSURED, zero).expect("status must be retryable");
		assert_eq!(retry.preferred_target, None);
		assert_eq!(retry.backoff, Some(BACKPRESSURE_RETRY_BACKOFF));
		assert!(!retry.refresh_topology);
	}

	#[test]
	fn query_failure_error_messages_name_terminal_query_statuses() {
		assert_eq!(
			query_failure_error(ReadStatus::QUERY_INVALID_PROGRAM).to_string(),
			"Wendelon query failed: QUERY_INVALID_PROGRAM"
		);
		assert_eq!(
			query_failure_error(ReadStatus::QUERY_BUDGET_EXCEEDED).to_string(),
			"Wendelon query failed: QUERY_BUDGET_EXCEEDED"
		);
		assert_eq!(
			query_failure_error(ReadStatus::RETENTION_EXPIRED).to_string(),
			"Wendelon query failed: RETENTION_EXPIRED"
		);
		assert_eq!(
			query_failure_error(ReadStatus::TENANT_UNAVAILABLE).to_string(),
			format!(
				"Wendelon query failed: status={}",
				ReadStatus::TENANT_UNAVAILABLE.0
			)
		);
	}

	#[tokio::test]
	async fn pick_target_prefers_leader_when_leader_is_a_replica() {
		let snap = snapshot(1, Some(2), &[1, 2, 3]);
		let client = WendelonClient::new(test_config(snap));
		// Exhaust hashes until we find one whose replica set
		// contains node 2; then assert pick_target chose node 2.
		for h in 0u64..32 {
			let locality = Locality::new(h);
			let replicas = client.replicas_for_locality(locality);
			if replicas.iter().any(|n| n.id == 2) {
				let picked = client.pick_target(locality).unwrap();
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
		assert_eq!(client.replicas_for_locality(Locality::ZERO).len(), 5);
	}

	#[test]
	fn key_range_helpers() {
		let p = KeyRange::point(42);
		assert_eq!(p.start_locality, Locality::new(42));
		assert_eq!(p.end_locality, Locality::new(42));
		let f = KeyRange::full();
		assert_eq!(f.start_locality, Locality::ZERO);
		assert_eq!(f.end_locality, Locality::MAX);
	}
}
