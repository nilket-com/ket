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
//! 2. **Zero-copy querying.** Pushdown scans ship a PBC program
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
//!   TopologySource → WendelonClient → query(pbc, range)
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
use crate::integration::compute::{
	decode_logical_type_definition, decode_logical_type_definitions,
};
use crate::metrics::NetworkCompressionPath;
use crate::network::control_server::{
	CONTROL_REASON_AUTHORIZATION_FAILED, CONTROL_REASON_GRANTED, CONTROL_REASON_INVALID_CONFIG,
	CONTROL_REASON_NOT_FOUND, CONTROL_REASON_NOT_LEADER,
};
use crate::network::core::{HRFT_MAGIC, read_frame, write_frame};
use crate::network::factory::TransportProviderFactory;
use crate::network::pool::BufPool;
use crate::network::ring::{RingRouter, RingState};
use crate::network::transport::{
	CapabilityResolver, EpochCapabilityResolver, MembershipEpoch, NodeCapability,
	TransportProvider, TransportStream, TransportTier, TransportTierSet,
};
use crate::schema::table::decode_table_schema_descriptors;
use crate::schema::table::encode_descriptor_wire_name;
use crate::schema::{
	CdcCheckpointMeta, CdcCursor, CdcDecodeError, CdcFrameMeta, CdcRejectMeta, CdcSubscribeBound,
	CdcSubscribeRequestMeta, Consistency, FRAME_FLAG_STREAM_END, FinalizationMarker, FrameHeader,
	HybridTimestamp, Identity, Locality, LogicalSchemaFingerprint, LogicalTypeGetRequestMeta,
	LogicalTypeGetResponseMeta, LogicalTypeListRequestMeta, LogicalTypeListResponseMeta, MsgType,
	NodeId, PbcRejectCode, QUERY_REQUEST_FLAG_DISABLE_STATS_PRUNING, QueryRequestMeta,
	QueryResponseMeta, ReadRequestMeta, ReadResponseMeta, ReadStatus, RuntimeFilterDescriptor,
	STORAGE_ROW_KEY_ID_COLUMN, StateDigest, TableDeleteRowsRequestMeta,
	TableDeleteRowsResponseMeta, TableDescriptorGetRequestMeta, TableDescriptorGetResponseMeta,
	TableDescriptorListRequestMeta, TableDescriptorListResponseMeta, TableObjectId,
	TableOptimizerFacts, TableOptimizerFactsGetRequestMeta, TableOptimizerFactsGetResponseMeta,
	TableQueryCancelRequestMeta, TableQueryCancelResponseMeta, TableQueryRequestId,
	TableQueryRequestMeta, TableSchemaDescriptor, TableWriteRequestMeta, TableWriteResponseMeta,
	TenantId, WriteRequestMeta, WriteResponseMeta, WriteStatus, WriterNodeId,
	column_projection_prefix_ranges, decode_table_optimizer_facts,
	decode_table_schema_descriptor_v1, read_key_schema, read_value_schema,
	table_spread_prefix_ranges, validate_table_write_request_batch, write_payload_schema,
};
use arrow::array::{Array, FixedSizeBinaryBuilder, LargeBinaryArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use std::any::Any;
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
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
	pub descriptor: TableSchemaDescriptor,
	pub payload: RecordBatch,
}

/// Result of a typed table write.
#[derive(Clone, Debug)]
pub struct TableWriteResult {
	pub response: TableWriteResponseMeta,
	pub finalization: Option<FinalizationMarker>,
}

/// Client-visible append-only row delete request for ADR 047.
#[derive(Clone, Debug)]
pub struct TableDeleteRowsRequest {
	pub meta: TableDeleteRowsRequestMeta,
	pub descriptor: TableSchemaDescriptor,
	pub row_key_ids: Vec<Identity>,
}

/// Result of an append-only row delete.
#[derive(Clone, Debug)]
pub struct TableDeleteRowsResult {
	pub response: TableDeleteRowsResponseMeta,
	pub finalization: Option<FinalizationMarker>,
}

#[derive(Clone, Debug)]
pub struct TableDescriptorGetResult {
	pub response: TableDescriptorGetResponseMeta,
	pub descriptor: Option<TableSchemaDescriptor>,
}

#[derive(Clone, Debug)]
pub struct TableDescriptorListResult {
	pub response: TableDescriptorListResponseMeta,
	pub descriptors: Vec<TableSchemaDescriptor>,
}

#[derive(Clone, Debug)]
pub struct TableOptimizerFactsGetResult {
	pub response: TableOptimizerFactsGetResponseMeta,
	pub facts: Option<TableOptimizerFacts>,
}

#[derive(Clone, Debug)]
pub struct LogicalTypeGetResult {
	pub response: LogicalTypeGetResponseMeta,
	pub definition: Option<crate::integration::compute::LogicalTypeDefinition>,
}

#[derive(Clone, Debug)]
pub struct LogicalTypeListResult {
	pub response: LogicalTypeListResponseMeta,
	pub definitions: Vec<crate::integration::compute::LogicalTypeDefinition>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RowIdentityProjection {
	#[default]
	None,
	Append,
}

impl RowIdentityProjection {
	pub const NONE: u8 = 0;
	pub const APPEND: u8 = 1;

	pub const fn as_u8(self) -> u8 {
		match self {
			Self::None => Self::NONE,
			Self::Append => Self::APPEND,
		}
	}

	pub const fn from_u8(value: u8) -> Option<Self> {
		match value {
			Self::NONE => Some(Self::None),
			Self::APPEND => Some(Self::Append),
			_ => None,
		}
	}
}

/// Client-visible typed table scan request for ADR 037.
#[derive(Clone, Debug)]
pub struct TableQueryRequest {
	pub meta: TableQueryRequestMeta,
	pub descriptor: TableSchemaDescriptor,
	pub pbc: Vec<u8>,
	pub runtime_filter: Option<RuntimeFilterDescriptor>,
	pub row_identity: RowIdentityProjection,
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
		// Client-side preview admission: used to pick a routing
		// target via the first row's locality. Pass `client_hlc` as
		// the row_hlc and `WriterNodeId::ZERO` as the writer for
		// the preview — for AppendOnly the resulting identities are
		// not load-bearing on the client side because locality is
		// request-level (ADR 045 §4a) and computed from
		// `(tenant_id, table_object_id, parent_write_id)`, all of
		// which the client and server see identically. So
		// preview-admission and server-admission produce the same
		// `localities[0]` for AppendOnly even though the per-row
		// identities differ. For PK-keyed tables identity is
		// HLC-independent, so preview and server agree on identities
		// too.
		let admission = validate_table_write_request_batch(
			&request.meta,
			&request.payload,
			&request.descriptor,
			WriterNodeId::ZERO,
			request.meta.client_hlc,
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
			if result.response.reject_code != PbcRejectCode::NONE.0 {
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

	/// Issue one `TABLE_DELETE_REQ` over HRFT. The accepting data node
	/// resolves the row IDs against the local version index and emits
	/// tombstone-only multi-row records for currently visible rows.
	pub async fn table_delete(
		&self,
		request: TableDeleteRowsRequest,
	) -> std::io::Result<TableDeleteRowsResult> {
		validate_table_delete_request(&request)?;
		let route_locality =
			table_query_route_locality(request.meta.tenant_id, &request.descriptor)?;
		let mut preferred_target = None;
		for attempt in 0..DATA_RETRY_BUDGET {
			let target = preferred_target.unwrap_or(self.target_for_locality(route_locality)?);
			let result = self.table_delete_once(target, &request).await?;
			self.observe_write_response(&result.response.write);
			if result.response.reject_code != PbcRejectCode::NONE.0 {
				return Ok(result);
			}
			let Some(retry) = retry_write_response(&result.response.write) else {
				return Ok(result);
			};
			if attempt + 1 == DATA_RETRY_BUDGET {
				return Ok(result);
			}
			self.prepare_retry(Some(route_locality), retry).await?;
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
		self.table_query_with_attempt_hook_internal(request, None)
			.await
	}

	pub async fn table_query_with_attempt_hook<F>(
		&self,
		request: TableQueryRequest,
		mut hook: F,
	) -> std::io::Result<StreamingQueryResult>
	where
		F: FnMut(TableQueryAttempt) -> std::io::Result<TableQueryAttemptGuard> + Send,
	{
		self.table_query_with_attempt_hook_internal(request, Some(&mut hook))
			.await
	}

	async fn table_query_with_attempt_hook_internal(
		&self,
		request: TableQueryRequest,
		mut hook: Option<&mut TableQueryAttemptHook<'_>>,
	) -> std::io::Result<StreamingQueryResult> {
		let mut validation_request = request.clone();
		if validation_request.meta.request_id.is_zero() {
			validation_request.meta.request_id = TableQueryRequestId::random();
		}
		validate_table_query_request(&validation_request)?;
		let route_locality =
			table_query_route_locality(request.meta.tenant_id, &request.descriptor)?;
		let requested_stats_pruning =
			request.meta._pad[0] & QUERY_REQUEST_FLAG_DISABLE_STATS_PRUNING == 0;
		let mut preferred_target = None;
		for attempt in 0..DATA_RETRY_BUDGET {
			let target = preferred_target.unwrap_or(self.target_for_locality(route_locality)?);
			let effective_stats_pruning =
				self.slab_stats_pruning_for_target(target, requested_stats_pruning);
			let mut attempt_request = if effective_stats_pruning == requested_stats_pruning {
				request.clone()
			} else {
				let mut adjusted = request.clone();
				adjusted.meta._pad[0] |= QUERY_REQUEST_FLAG_DISABLE_STATS_PRUNING;
				adjusted
			};
			attempt_request.meta.request_id = TableQueryRequestId::random();
			validate_table_query_request(&attempt_request)?;
			let attempt_guard = match hook.as_deref_mut() {
				Some(hook) => Some(hook(TableQueryAttempt {
					target,
					tenant_id: attempt_request.meta.tenant_id,
					request_id: attempt_request.meta.request_id,
				})?),
				None => None,
			};
			match self
				.table_query_once(target, &attempt_request, attempt_guard)
				.await?
			{
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

	pub async fn cancel_table_query_attempt(
		&self,
		target: NodeId,
		tenant_id: TenantId,
		request_id: TableQueryRequestId,
	) -> std::io::Result<TableQueryCancelResponseMeta> {
		if request_id.is_zero() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"TABLE_QUERY_CANCEL_REQ request_id must be non-zero",
			));
		}
		let request = TableQueryCancelRequestMeta {
			tenant_id,
			request_id,
		};
		let conn = self.factory.get_connection(target).await?;
		let mut stream = conn.open_bi().await?;
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::TABLE_QUERY_CANCEL_REQ.0,
			flags: 0,
			stream_id: 0,
			meta_len: std::mem::size_of::<TableQueryCancelRequestMeta>() as u32,
			arrow_len: 0,
			trailing_len: 0,
		};
		write_frame(
			&mut *stream,
			&header,
			bytemuck::bytes_of(&request),
			None,
			None,
		)
		.await?;
		stream
			.finish()
			.await
			.map_err(|e| Error::new(ErrorKind::BrokenPipe, e))?;
		let frame = read_frame(&mut *stream, &self.pool).await?;
		decode_table_query_cancel_response_frame(frame)
	}

	/// Open a CDC subscription stream for one tenant table.
	pub async fn cdc_subscribe(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		start_cursor: CdcCursor,
		bound: CdcSubscribeBound,
		logical_schema_fingerprint: LogicalSchemaFingerprint,
		resume_digest: Option<StateDigest>,
	) -> std::io::Result<CdcSubscription> {
		let target = match self.leader() {
			Some(leader) => leader,
			None => {
				self.refresh_topology().await?;
				self.leader().ok_or_else(|| {
					Error::new(
						ErrorKind::NotConnected,
						"CDC subscribe requires a known data-ring leader",
					)
				})?
			}
		};
		let conn = self.factory.get_connection(target).await?;
		let mut stream = conn.open_bi().await?;
		let meta = CdcSubscribeRequestMeta {
			tenant_id,
			table_object_id,
			start_cursor,
			bound_raw: bound.as_raw(),
			resume_digest_present: u8::from(resume_digest.is_some()),
			_pad: [0; 6],
			logical_schema_fingerprint: *logical_schema_fingerprint.as_bytes(),
			resume_visible_state_digest: resume_digest.unwrap_or(StateDigest::ZERO),
		};
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::CDC_SUBSCRIBE_REQ.0,
			flags: 0,
			stream_id: 0,
			meta_len: std::mem::size_of::<CdcSubscribeRequestMeta>() as u32,
			arrow_len: 0,
			trailing_len: 0,
		};
		write_frame(&mut *stream, &header, bytemuck::bytes_of(&meta), None, None).await?;
		stream
			.finish()
			.await
			.map_err(|e| Error::new(ErrorKind::BrokenPipe, e))?;
		Ok(CdcSubscription {
			stream: Some(stream),
			pool: self.pool.clone(),
		})
	}

	/// Fetch the latest authoritative table descriptor by exact
	/// tenant/namespace/table name from the Control Ring leader.
	pub async fn get_table_schema_descriptor(
		&self,
		tenant_id: TenantId,
		namespace: &str,
		table_name: &str,
	) -> std::io::Result<Option<TableSchemaDescriptor>> {
		let request = TableDescriptorGetRequestMeta {
			tenant_id,
			namespace: encode_descriptor_wire_name("namespace", namespace)?,
			table_name: encode_descriptor_wire_name("table name", table_name)?,
		};
		let mut preferred_target = self.leader();
		for attempt in 0..DATA_RETRY_BUDGET {
			let target = match preferred_target.or_else(|| self.leader()) {
				Some(target) => target,
				None => {
					self.refresh_topology().await?;
					self.leader().ok_or_else(|| {
						Error::new(
							ErrorKind::NotConnected,
							"table descriptor get requires a known Control Ring leader",
						)
					})?
				}
			};
			let result = self.table_descriptor_get_once(target, &request).await?;
			if result.response.leader_hint.id != 0 {
				*self.leader.write().unwrap() = Some(result.response.leader_hint);
			}
			match result.response.reason_code {
				code if result.response.accepted == 1 && code == CONTROL_REASON_GRANTED => {
					return Ok(result.descriptor);
				}
				CONTROL_REASON_NOT_FOUND => return Ok(None),
				CONTROL_REASON_NOT_LEADER => {
					if attempt + 1 == DATA_RETRY_BUDGET {
						return Err(Error::new(
							ErrorKind::NotConnected,
							"Wendelon table descriptor get retry budget exhausted",
						));
					}
					let retry =
						RetryDirective::redirect(nonzero_node_id(result.response.leader_hint));
					self.prepare_retry(None, retry).await?;
					preferred_target = retry.preferred_target.or_else(|| self.leader());
				}
				CONTROL_REASON_AUTHORIZATION_FAILED => {
					return Err(Error::new(
						ErrorKind::PermissionDenied,
						"Wendelon table descriptor get authorization failed",
					));
				}
				CONTROL_REASON_INVALID_CONFIG => {
					return Err(Error::new(
						ErrorKind::InvalidInput,
						"Wendelon table descriptor get request was invalid",
					));
				}
				other => {
					return Err(Error::new(
						ErrorKind::Other,
						format!("Wendelon table descriptor get failed: reason_code={other}"),
					));
				}
			}
		}
		unreachable!("bounded retry loop must return within budget")
	}

	/// Fetch advisory optimizer facts for a descriptor identity from
	/// the Control Ring leader. Absence and not-found responses map to
	/// `None`; transport/protocol failures remain errors so callers can
	/// decide whether to fail open.
	pub async fn get_table_optimizer_facts(
		&self,
		tenant_id: TenantId,
		namespace: &str,
		table_name: &str,
		descriptor: &TableSchemaDescriptor,
	) -> std::io::Result<Option<TableOptimizerFacts>> {
		let request = TableOptimizerFactsGetRequestMeta {
			tenant_id,
			namespace: encode_descriptor_wire_name("namespace", namespace)?,
			table_name: encode_descriptor_wire_name("table name", table_name)?,
			schema_generation: descriptor.schema_generation,
			physical_descriptor_fingerprint: *descriptor.physical_descriptor_fingerprint.as_bytes(),
		};
		let mut preferred_target = self.leader();
		for attempt in 0..DATA_RETRY_BUDGET {
			let target = match preferred_target.or_else(|| self.leader()) {
				Some(target) => target,
				None => {
					self.refresh_topology().await?;
					self.leader().ok_or_else(|| {
						Error::new(
							ErrorKind::NotConnected,
							"table optimizer facts get requires a known Control Ring leader",
						)
					})?
				}
			};
			let result = self
				.table_optimizer_facts_get_once(target, &request)
				.await?;
			if result.response.leader_hint.id != 0 {
				*self.leader.write().unwrap() = Some(result.response.leader_hint);
			}
			match result.response.reason_code {
				code if result.response.accepted == 1 && code == CONTROL_REASON_GRANTED => {
					return Ok(result.facts);
				}
				CONTROL_REASON_NOT_FOUND => return Ok(None),
				CONTROL_REASON_NOT_LEADER => {
					if attempt + 1 == DATA_RETRY_BUDGET {
						return Err(Error::new(
							ErrorKind::NotConnected,
							"table optimizer facts get retry budget exhausted",
						));
					}
					let retry =
						RetryDirective::redirect(nonzero_node_id(result.response.leader_hint));
					self.prepare_retry(None, retry).await?;
					preferred_target = retry.preferred_target.or_else(|| self.leader());
				}
				CONTROL_REASON_AUTHORIZATION_FAILED => {
					return Err(Error::new(
						ErrorKind::PermissionDenied,
						"table optimizer facts get authorization failed",
					));
				}
				CONTROL_REASON_INVALID_CONFIG => {
					return Err(Error::new(
						ErrorKind::InvalidInput,
						"table optimizer facts get request was invalid",
					));
				}
				other => {
					return Err(Error::new(
						ErrorKind::Other,
						format!("table optimizer facts get failed: reason_code={other}"),
					));
				}
			}
		}
		unreachable!("bounded retry loop must return within budget")
	}

	/// Fetch the latest authoritative table descriptors for a tenant
	/// from the Control Ring leader.
	pub async fn list_table_schema_descriptors(
		&self,
		tenant_id: TenantId,
	) -> std::io::Result<Vec<TableSchemaDescriptor>> {
		let request = TableDescriptorListRequestMeta { tenant_id };
		let mut preferred_target = self.leader();
		for attempt in 0..DATA_RETRY_BUDGET {
			let target = match preferred_target.or_else(|| self.leader()) {
				Some(target) => target,
				None => {
					self.refresh_topology().await?;
					self.leader().ok_or_else(|| {
						Error::new(
							ErrorKind::NotConnected,
							"table descriptor list requires a known Control Ring leader",
						)
					})?
				}
			};
			let result = self.table_descriptor_list_once(target, &request).await?;
			if result.response.leader_hint.id != 0 {
				*self.leader.write().unwrap() = Some(result.response.leader_hint);
			}
			match result.response.reason_code {
				code if result.response.accepted == 1 && code == CONTROL_REASON_GRANTED => {
					return Ok(result.descriptors);
				}
				CONTROL_REASON_NOT_LEADER => {
					if attempt + 1 == DATA_RETRY_BUDGET {
						return Err(Error::new(
							ErrorKind::NotConnected,
							"Wendelon table descriptor list retry budget exhausted",
						));
					}
					let retry =
						RetryDirective::redirect(nonzero_node_id(result.response.leader_hint));
					self.prepare_retry(None, retry).await?;
					preferred_target = retry.preferred_target.or_else(|| self.leader());
				}
				CONTROL_REASON_AUTHORIZATION_FAILED => {
					return Err(Error::new(
						ErrorKind::PermissionDenied,
						"Wendelon table descriptor list authorization failed",
					));
				}
				CONTROL_REASON_INVALID_CONFIG => {
					return Err(Error::new(
						ErrorKind::InvalidInput,
						"Wendelon table descriptor list request was invalid",
					));
				}
				other => {
					return Err(Error::new(
						ErrorKind::Other,
						format!("Wendelon table descriptor list failed: reason_code={other}"),
					));
				}
			}
		}
		unreachable!("bounded retry loop must return within budget")
	}

	/// Fetch a tenant logical type definition by exact name from the
	/// Control Ring leader.
	pub async fn get_logical_type_definition(
		&self,
		tenant_id: TenantId,
		name: &str,
	) -> std::io::Result<Option<crate::integration::compute::LogicalTypeDefinition>> {
		let request = LogicalTypeGetRequestMeta {
			tenant_id,
			name: encode_descriptor_wire_name("logical type name", name)?,
		};
		let mut preferred_target = self.leader();
		for attempt in 0..DATA_RETRY_BUDGET {
			let target = match preferred_target.or_else(|| self.leader()) {
				Some(target) => target,
				None => {
					self.refresh_topology().await?;
					self.leader().ok_or_else(|| {
						Error::new(
							ErrorKind::NotConnected,
							"logical type get requires a known Control Ring leader",
						)
					})?
				}
			};
			let result = self.logical_type_get_once(target, &request).await?;
			if result.response.leader_hint.id != 0 {
				*self.leader.write().unwrap() = Some(result.response.leader_hint);
			}
			match result.response.reason_code {
				code if result.response.accepted == 1 && code == CONTROL_REASON_GRANTED => {
					return Ok(result.definition);
				}
				CONTROL_REASON_NOT_FOUND => return Ok(None),
				CONTROL_REASON_NOT_LEADER => {
					if attempt + 1 == DATA_RETRY_BUDGET {
						return Err(Error::new(
							ErrorKind::NotConnected,
							"Wendelon logical type get retry budget exhausted",
						));
					}
					let retry =
						RetryDirective::redirect(nonzero_node_id(result.response.leader_hint));
					self.prepare_retry(None, retry).await?;
					preferred_target = retry.preferred_target.or_else(|| self.leader());
				}
				CONTROL_REASON_AUTHORIZATION_FAILED => {
					return Err(Error::new(
						ErrorKind::PermissionDenied,
						"Wendelon logical type get authorization failed",
					));
				}
				CONTROL_REASON_INVALID_CONFIG => {
					return Err(Error::new(
						ErrorKind::InvalidInput,
						"Wendelon logical type get request was invalid",
					));
				}
				other => {
					return Err(Error::new(
						ErrorKind::Other,
						format!("Wendelon logical type get failed: reason_code={other}"),
					));
				}
			}
		}
		unreachable!("bounded retry loop must return within budget")
	}

	/// Fetch all tenant logical type definitions from the Control Ring
	/// leader for catalog sync.
	pub async fn list_logical_type_definitions(
		&self,
		tenant_id: TenantId,
	) -> std::io::Result<Vec<crate::integration::compute::LogicalTypeDefinition>> {
		let request = LogicalTypeListRequestMeta { tenant_id };
		let mut preferred_target = self.leader();
		for attempt in 0..DATA_RETRY_BUDGET {
			let target = match preferred_target.or_else(|| self.leader()) {
				Some(target) => target,
				None => {
					self.refresh_topology().await?;
					self.leader().ok_or_else(|| {
						Error::new(
							ErrorKind::NotConnected,
							"logical type list requires a known Control Ring leader",
						)
					})?
				}
			};
			let result = self.logical_type_list_once(target, &request).await?;
			if result.response.leader_hint.id != 0 {
				*self.leader.write().unwrap() = Some(result.response.leader_hint);
			}
			match result.response.reason_code {
				code if result.response.accepted == 1 && code == CONTROL_REASON_GRANTED => {
					return Ok(result.definitions);
				}
				CONTROL_REASON_NOT_LEADER => {
					if attempt + 1 == DATA_RETRY_BUDGET {
						return Err(Error::new(
							ErrorKind::NotConnected,
							"Wendelon logical type list retry budget exhausted",
						));
					}
					let retry =
						RetryDirective::redirect(nonzero_node_id(result.response.leader_hint));
					self.prepare_retry(None, retry).await?;
					preferred_target = retry.preferred_target.or_else(|| self.leader());
				}
				CONTROL_REASON_AUTHORIZATION_FAILED => {
					return Err(Error::new(
						ErrorKind::PermissionDenied,
						"Wendelon logical type list authorization failed",
					));
				}
				CONTROL_REASON_INVALID_CONFIG => {
					return Err(Error::new(
						ErrorKind::InvalidInput,
						"Wendelon logical type list request was invalid",
					));
				}
				other => {
					return Err(Error::new(
						ErrorKind::Other,
						format!("Wendelon logical type list failed: reason_code={other}"),
					));
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

	async fn table_delete_once(
		&self,
		target: NodeId,
		request: &TableDeleteRowsRequest,
	) -> std::io::Result<TableDeleteRowsResult> {
		let conn = self.factory.get_connection(target).await?;
		let mut stream = conn.open_bi().await?;
		let payload = table_delete_rows_batch(&request.row_key_ids)?;
		let arrow = encode_batch(&payload)?;
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::TABLE_DELETE_REQ.0,
			flags: 0,
			stream_id: 0,
			meta_len: std::mem::size_of::<TableDeleteRowsRequestMeta>() as u32,
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
		decode_table_delete_response_frame(frame)
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

	async fn table_descriptor_get_once(
		&self,
		target: NodeId,
		request: &TableDescriptorGetRequestMeta,
	) -> std::io::Result<TableDescriptorGetResult> {
		let conn = self.factory.get_connection(target).await?;
		let mut stream = conn.open_bi().await?;
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::TABLE_DESCRIPTOR_GET_REQ.0,
			flags: 0,
			stream_id: 0,
			meta_len: std::mem::size_of::<TableDescriptorGetRequestMeta>() as u32,
			arrow_len: 0,
			trailing_len: 0,
		};
		write_frame(
			&mut *stream,
			&header,
			bytemuck::bytes_of(request),
			None,
			None,
		)
		.await?;
		stream
			.finish()
			.await
			.map_err(|e| Error::new(ErrorKind::BrokenPipe, e))?;
		let frame = read_frame(&mut *stream, &self.pool).await?;
		decode_table_descriptor_get_response_frame(frame)
	}

	async fn table_descriptor_list_once(
		&self,
		target: NodeId,
		request: &TableDescriptorListRequestMeta,
	) -> std::io::Result<TableDescriptorListResult> {
		let conn = self.factory.get_connection(target).await?;
		let mut stream = conn.open_bi().await?;
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::TABLE_DESCRIPTOR_LIST_REQ.0,
			flags: 0,
			stream_id: 0,
			meta_len: std::mem::size_of::<TableDescriptorListRequestMeta>() as u32,
			arrow_len: 0,
			trailing_len: 0,
		};
		write_frame(
			&mut *stream,
			&header,
			bytemuck::bytes_of(request),
			None,
			None,
		)
		.await?;
		stream
			.finish()
			.await
			.map_err(|e| Error::new(ErrorKind::BrokenPipe, e))?;
		let frame = read_frame(&mut *stream, &self.pool).await?;
		decode_table_descriptor_list_response_frame(frame)
	}

	async fn table_optimizer_facts_get_once(
		&self,
		target: NodeId,
		request: &TableOptimizerFactsGetRequestMeta,
	) -> std::io::Result<TableOptimizerFactsGetResult> {
		let conn = self.factory.get_connection(target).await?;
		let mut stream = conn.open_bi().await?;
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::TABLE_OPTIMIZER_FACTS_GET_REQ.0,
			flags: 0,
			stream_id: 0,
			meta_len: std::mem::size_of::<TableOptimizerFactsGetRequestMeta>() as u32,
			arrow_len: 0,
			trailing_len: 0,
		};
		write_frame(
			&mut *stream,
			&header,
			bytemuck::bytes_of(request),
			None,
			None,
		)
		.await?;
		stream
			.finish()
			.await
			.map_err(|e| Error::new(ErrorKind::BrokenPipe, e))?;
		let frame = read_frame(&mut *stream, &self.pool).await?;
		decode_table_optimizer_facts_get_response_frame(frame)
	}

	async fn logical_type_get_once(
		&self,
		target: NodeId,
		request: &LogicalTypeGetRequestMeta,
	) -> std::io::Result<LogicalTypeGetResult> {
		let conn = self.factory.get_connection(target).await?;
		let mut stream = conn.open_bi().await?;
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::LOGICAL_TYPE_GET_REQ.0,
			flags: 0,
			stream_id: 0,
			meta_len: std::mem::size_of::<LogicalTypeGetRequestMeta>() as u32,
			arrow_len: 0,
			trailing_len: 0,
		};
		write_frame(
			&mut *stream,
			&header,
			bytemuck::bytes_of(request),
			None,
			None,
		)
		.await?;
		stream
			.finish()
			.await
			.map_err(|e| Error::new(ErrorKind::BrokenPipe, e))?;
		let frame = read_frame(&mut *stream, &self.pool).await?;
		decode_logical_type_get_response_frame(frame)
	}

	async fn logical_type_list_once(
		&self,
		target: NodeId,
		request: &LogicalTypeListRequestMeta,
	) -> std::io::Result<LogicalTypeListResult> {
		let conn = self.factory.get_connection(target).await?;
		let mut stream = conn.open_bi().await?;
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::LOGICAL_TYPE_LIST_REQ.0,
			flags: 0,
			stream_id: 0,
			meta_len: std::mem::size_of::<LogicalTypeListRequestMeta>() as u32,
			arrow_len: 0,
			trailing_len: 0,
		};
		write_frame(
			&mut *stream,
			&header,
			bytemuck::bytes_of(request),
			None,
			None,
		)
		.await?;
		stream
			.finish()
			.await
			.map_err(|e| Error::new(ErrorKind::BrokenPipe, e))?;
		let frame = read_frame(&mut *stream, &self.pool).await?;
		decode_logical_type_list_response_frame(frame)
	}

	async fn table_query_once(
		&self,
		target: NodeId,
		request: &TableQueryRequest,
		attempt_guard: Option<TableQueryAttemptGuard>,
	) -> std::io::Result<QueryOutcome> {
		let conn = self.factory.get_connection(target).await?;
		let mut stream = conn.open_bi().await?;

		let meta_bytes = bytemuck::bytes_of(&request.meta);
		let runtime_filter_bytes = request
			.runtime_filter
			.as_ref()
			.map(postcard::to_allocvec)
			.transpose()
			.map_err(|err| {
				Error::new(
					ErrorKind::InvalidInput,
					format!("TABLE_QUERY_REQ runtime filter encode failed: {err}"),
				)
			})?;
		let mut trailing_bytes = Vec::with_capacity(
			request
				.pbc
				.len()
				.saturating_add(runtime_filter_bytes.as_ref().map(Vec::len).unwrap_or(0)),
		);
		trailing_bytes.extend_from_slice(&request.pbc);
		if let Some(runtime_filter_bytes) = &runtime_filter_bytes {
			trailing_bytes.extend_from_slice(runtime_filter_bytes);
		}
		let trailing = if trailing_bytes.is_empty() {
			None
		} else {
			Some(trailing_bytes.as_slice())
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
		decode_initial_query_response(
			resp,
			stream,
			self.pool.clone(),
			MsgType::TABLE_QUERY_RESP,
			StreamingQueryMetaMode::TerminalMetadata,
			attempt_guard,
		)
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
	meta_mode: StreamingQueryMetaMode,
	_table_query_attempt_guard: Option<TableQueryAttemptGuard>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StreamingQueryMetaMode {
	StrictContinuations,
	TerminalMetadata,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TableQueryAttempt {
	pub target: NodeId,
	pub tenant_id: TenantId,
	pub request_id: TableQueryRequestId,
}

pub type TableQueryAttemptGuard = Box<dyn Any + Send + Sync + 'static>;
pub type TableQueryAttemptHook<'a> =
	dyn FnMut(TableQueryAttempt) -> std::io::Result<TableQueryAttemptGuard> + Send + 'a;

pub struct CdcSubscription {
	stream: Option<Box<dyn TransportStream>>,
	pool: Arc<BufPool>,
}

impl std::fmt::Debug for CdcSubscription {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("CdcSubscription")
			.field("stream_open", &self.stream.is_some())
			.finish()
	}
}

#[derive(Debug)]
pub enum CdcFrame {
	Frame {
		meta: CdcFrameMeta,
		batch: RecordBatch,
	},
	Checkpoint {
		meta: CdcCheckpointMeta,
	},
	Reject {
		meta: CdcRejectMeta,
	},
	End,
}

impl CdcSubscription {
	pub async fn next_frame(&mut self) -> std::io::Result<CdcFrame> {
		let Some(stream) = self.stream.as_mut() else {
			return Ok(CdcFrame::End);
		};
		let frame = read_frame(&mut **stream, &self.pool).await?;
		match MsgType(frame.header.msg_type) {
			MsgType::CDC_FRAME => {
				if frame.meta.len() < std::mem::size_of::<CdcFrameMeta>() {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"CDC_FRAME meta too small",
					));
				}
				let meta: CdcFrameMeta =
					*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<CdcFrameMeta>()]);
				meta.source().map_err(cdc_decode_error)?;
				if (frame.header.flags & FRAME_FLAG_STREAM_END) != 0
					&& frame.arrow_payload.is_none()
				{
					self.stream = None;
					return Ok(CdcFrame::End);
				}
				let arrow = frame.arrow_payload.as_ref().ok_or_else(|| {
					Error::new(ErrorKind::InvalidData, "CDC_FRAME missing Arrow payload")
				})?;
				Ok(CdcFrame::Frame {
					meta,
					batch: decode_batch(&arrow[..])?,
				})
			}
			MsgType::CDC_CHECKPOINT => {
				if frame.meta.len() < std::mem::size_of::<CdcCheckpointMeta>() {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"CDC_CHECKPOINT meta too small",
					));
				}
				if frame.arrow_payload.is_some() || frame.trailing.is_some() {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"CDC_CHECKPOINT must not carry Arrow or trailing payload",
					));
				}
				let meta: CdcCheckpointMeta =
					*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<CdcCheckpointMeta>()]);
				meta.digest_domain().map_err(cdc_decode_error)?;
				Ok(CdcFrame::Checkpoint { meta })
			}
			MsgType::CDC_REJECT => {
				if frame.meta.len() < std::mem::size_of::<CdcRejectMeta>() {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"CDC_REJECT meta too small",
					));
				}
				if frame.arrow_payload.is_some() || frame.trailing.is_some() {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"CDC_REJECT must not carry Arrow or trailing payload",
					));
				}
				let meta: CdcRejectMeta =
					*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<CdcRejectMeta>()]);
				meta.code().map_err(cdc_decode_error)?;
				self.stream = None;
				Ok(CdcFrame::Reject { meta })
			}
			other => Err(Error::new(
				ErrorKind::InvalidData,
				format!("unexpected CDC msg_type=0x{:02x}", other.0),
			)),
		}
	}
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
			.field("meta_mode", &self.meta_mode)
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
		let is_end = (frame.header.flags & FRAME_FLAG_STREAM_END) != 0;
		if !frame.meta.is_empty() {
			if self.meta_mode != StreamingQueryMetaMode::TerminalMetadata || !is_end {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"QUERY_RESP continuation must not carry response meta",
				));
			}
			if frame.meta.len() < std::mem::size_of::<QueryResponseMeta>() {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"query terminal response meta too small",
				));
			}
			if frame.trailing.is_some() {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"QUERY_RESP continuation must not carry trailing bytes",
				));
			}
			let ack: &QueryResponseMeta =
				bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<QueryResponseMeta>()]);
			if frame.header.arrow_len != ack.arrow_len {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"query terminal response arrow_len mismatch: meta={} header={}",
						ack.arrow_len, frame.header.arrow_len
					),
				));
			}
			self.value_hlc = ack.value_hlc;
			self.epoch = ack.epoch;
			self.rows = ack.rows;
			self.stream = None;
			return match ack.status {
				s if s == ReadStatus::OK || s == ReadStatus::KEY_NOT_FOUND => {
					match frame.arrow_payload.as_ref() {
						Some(arrow) => Ok(Some(decode_batch(&arrow[..])?)),
						None => Ok(None),
					}
				}
				other => Err(query_failure_error(other)),
			};
		}
		if frame.trailing.is_some() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"QUERY_RESP continuation must not carry trailing bytes",
			));
		}
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

	pub async fn drain_all(&mut self) -> std::io::Result<Vec<RecordBatch>> {
		let mut batches = Vec::new();
		while let Some(batch) = self.next_batch().await? {
			batches.push(batch);
		}
		Ok(batches)
	}

	pub async fn collect_all(mut self) -> std::io::Result<Vec<RecordBatch>> {
		self.drain_all().await
	}
}

impl WendelonClient {
	/// Push a PBC program to the replica that owns
	/// `range.start_locality`
	/// and stream the filtered Arrow IPC result back.
	///
	/// * `pbc_ast` — raw PBC bytecode. Empty is legal and means
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
		pbc_ast: Vec<u8>,
		range: QueryLocalityRange,
		expected_epoch: u64,
		min_hlc: HybridTimestamp,
		consistency: Consistency,
		as_of_hlc: HybridTimestamp,
		stats_pruning_enabled: bool,
	) -> std::io::Result<StreamingQueryResult> {
		let mut preferred_target = None;
		for attempt in 0..DATA_RETRY_BUDGET {
			let target =
				preferred_target.unwrap_or(self.target_for_locality(range.start_locality)?);
			let mut pad = [0; 7];
			if !self.slab_stats_pruning_for_target(target, stats_pruning_enabled) {
				pad[0] |= QUERY_REQUEST_FLAG_DISABLE_STATS_PRUNING;
			}
			let meta = QueryRequestMeta {
				min_hlc,
				expected_epoch,
				tenant_id,
				consistency,
				_pad: pad,
				locality_range_start: range.start_locality.get(),
				locality_range_end: range.end_locality.get(),
				bytecode_len: pbc_ast.len() as u32,
				_pad2: [0; 4],
				as_of_hlc,
			};
			match self.query_once(target, &meta, &pbc_ast).await? {
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

	/// True iff the named peer advertises `capability` in the current
	/// membership snapshot. Returns false for peers absent from the
	/// snapshot, which means the dispatcher silently skips the
	/// optional path rather than racing against a stale view.
	fn peer_advertises(&self, target: NodeId, capability: NodeCapability) -> bool {
		self.topology_tx
			.borrow()
			.peers
			.get(&target)
			.is_some_and(|caps| caps.capabilities.contains(capability))
	}

	/// Resolve the effective `stats_pruning_enabled` flag for a single
	/// dispatch attempt against `target`. If the caller asked for
	/// stats pruning but the chosen target peer does not advertise
	/// `SlabStatsPruning`, the flag is silently forced off and the
	/// `pbc_slab_stats_skipped_no_capability_total` counter is
	/// incremented (per ADR 036 §"Capability negotiation contract").
	fn slab_stats_pruning_for_target(&self, target: NodeId, requested: bool) -> bool {
		if !requested {
			return false;
		}
		if self.peer_advertises(target, NodeCapability::SlabStatsPruning) {
			return true;
		}
		crate::metrics::global()
			.pbc_slab_stats_skipped_no_capability_total
			.inc();
		false
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
		pbc: &[u8],
	) -> std::io::Result<QueryOutcome> {
		let conn = self.factory.get_connection(target).await?;
		let mut stream = conn.open_bi().await?;

		let meta_bytes = bytemuck::bytes_of(meta);
		let trailing: Option<&[u8]> = if pbc.is_empty() { None } else { Some(pbc) };
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
		decode_initial_query_response(
			resp,
			stream,
			self.pool.clone(),
			MsgType::QUERY_RESP,
			StreamingQueryMetaMode::StrictContinuations,
			None,
		)
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
		s if s == ReadStatus::TABLE_DESCRIPTOR_EVOLVED => Error::new(
			ErrorKind::InvalidData,
			"Wendelon query failed: TABLE_DESCRIPTOR_EVOLVED",
		),
		s if s == ReadStatus::QUERY_BUDGET_EXCEEDED => Error::new(
			ErrorKind::Other,
			"Wendelon query failed: QUERY_BUDGET_EXCEEDED",
		),
		s if s == ReadStatus::QUERY_CANCELLED => Error::new(
			ErrorKind::Interrupted,
			"Wendelon query failed: QUERY_CANCELLED",
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
	meta_mode: StreamingQueryMetaMode,
	attempt_guard: Option<TableQueryAttemptGuard>,
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
			let batch = match resp.arrow_payload.as_ref() {
				Some(arrow) => Some(decode_batch(&arrow[..])?),
				None if is_end && meta_mode == StreamingQueryMetaMode::TerminalMetadata => None,
				None => {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"query response OK missing Arrow payload",
					));
				}
			};
			Ok(QueryOutcome::Ok(StreamingQueryResult {
				value_hlc: ack.value_hlc,
				epoch: ack.epoch,
				rows: ack.rows,
				pending_batch: batch,
				stream: if is_end { None } else { Some(stream) },
				pool,
				expected_msg_type,
				meta_mode,
				_table_query_attempt_guard: attempt_guard,
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
			meta_mode,
			_table_query_attempt_guard: attempt_guard,
		})),
		other => Err(query_failure_error(other)),
	}
}

fn validate_table_query_request(request: &TableQueryRequest) -> std::io::Result<()> {
	request.descriptor.validate()?;
	if request.meta.request_id.is_zero() {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"TABLE_QUERY_REQ request_id must be non-zero",
		));
	}
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
	if request.meta.physical_descriptor_fingerprint
		!= *request
			.descriptor
			.physical_descriptor_fingerprint
			.as_bytes()
	{
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"TABLE_QUERY_REQ physical_descriptor_fingerprint does not match descriptor",
		));
	}
	if request.meta.row_identity_projection != request.row_identity.as_u8() {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"TABLE_QUERY_REQ row_identity_projection does not match typed request",
		));
	}
	let bytecode_len = u32::try_from(request.pbc.len()).map_err(|_| {
		Error::new(
			ErrorKind::InvalidInput,
			"TABLE_QUERY_REQ PBC bytecode exceeds u32 length",
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
	let runtime_filter_len = match &request.runtime_filter {
		Some(runtime_filter) => {
			let encoded = postcard::to_allocvec(runtime_filter).map_err(|err| {
				Error::new(
					ErrorKind::InvalidInput,
					format!("TABLE_QUERY_REQ runtime filter encode failed: {err}"),
				)
			})?;
			u32::try_from(encoded.len()).map_err(|_| {
				Error::new(
					ErrorKind::InvalidInput,
					"TABLE_QUERY_REQ runtime filter exceeds u32 length",
				)
			})?
		}
		None => 0,
	};
	if request.meta.runtime_filter_len != runtime_filter_len {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			format!(
				"TABLE_QUERY_REQ runtime_filter_len mismatch: meta={} actual={}",
				request.meta.runtime_filter_len, runtime_filter_len
			),
		));
	}
	Ok(())
}

fn table_query_route_locality(
	tenant_id: TenantId,
	descriptor: &TableSchemaDescriptor,
) -> std::io::Result<Locality> {
	match descriptor.locality_policy {
		crate::schema::LocalityPolicy::TableSpreadProjection(spread) => {
			let ranges = table_spread_prefix_ranges(tenant_id, descriptor.table_object_id, spread)?;
			ranges.first().map(|(start, _)| *start).ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidInput,
					"table spread descriptor produced no scan ranges",
				)
			})
		}
		crate::schema::LocalityPolicy::IdentityProjection => Ok(Locality::ZERO),
		crate::schema::LocalityPolicy::ColumnProjection(ref projection) => {
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

fn decode_table_delete_response_frame(
	frame: crate::network::core::HrftFrame,
) -> std::io::Result<TableDeleteRowsResult> {
	if frame.header.msg_type != MsgType::TABLE_DELETE_RESP.0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"expected TABLE_DELETE_RESP, got msg_type=0x{:02x}",
				frame.header.msg_type
			),
		));
	}
	if frame.meta.len() < std::mem::size_of::<TableDeleteRowsResponseMeta>() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"TABLE_DELETE_RESP meta too small",
		));
	}
	if frame.arrow_payload.is_some() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"TABLE_DELETE_RESP must not carry an Arrow payload",
		));
	}
	let response: TableDeleteRowsResponseMeta =
		*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<TableDeleteRowsResponseMeta>()]);
	let finalization = match response.write.has_finalization {
		0 => {
			if frame.trailing.is_some() {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"TABLE_DELETE_RESP trailing marker present but has_finalization=0",
				));
			}
			None
		}
		1 => {
			let trailing = frame.trailing.as_ref().ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					"TABLE_DELETE_RESP missing FinalizationMarker payload",
				)
			})?;
			if trailing.len() < std::mem::size_of::<FinalizationMarker>() {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"TABLE_DELETE_RESP FinalizationMarker payload too small",
				));
			}
			Some(*bytemuck::from_bytes(
				&trailing[..std::mem::size_of::<FinalizationMarker>()],
			))
		}
		other => {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!("TABLE_DELETE_RESP invalid has_finalization value {other}"),
			));
		}
	};
	Ok(TableDeleteRowsResult {
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

fn decode_table_descriptor_get_response_frame(
	frame: crate::network::core::HrftFrame,
) -> std::io::Result<TableDescriptorGetResult> {
	if frame.header.msg_type != MsgType::TABLE_DESCRIPTOR_GET_RESP.0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"expected TABLE_DESCRIPTOR_GET_RESP, got msg_type=0x{:02x}",
				frame.header.msg_type
			),
		));
	}
	if frame.meta.len() != std::mem::size_of::<TableDescriptorGetResponseMeta>() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"TABLE_DESCRIPTOR_GET_RESP meta size mismatch",
		));
	}
	if frame.arrow_payload.is_some() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"TABLE_DESCRIPTOR_GET_RESP must not carry an Arrow payload",
		));
	}
	let response: TableDescriptorGetResponseMeta =
		*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<TableDescriptorGetResponseMeta>()]);
	let descriptor = if response.accepted == 1 {
		if response.reason_code != CONTROL_REASON_GRANTED {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"TABLE_DESCRIPTOR_GET_RESP accepted response must use granted reason",
			));
		}
		let trailing = frame.trailing.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"TABLE_DESCRIPTOR_GET_RESP accepted response missing descriptor payload",
			)
		})?;
		if response.descriptor_len as usize != trailing.len() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"TABLE_DESCRIPTOR_GET_RESP descriptor_len mismatch: meta={} actual={}",
					response.descriptor_len,
					trailing.len()
				),
			));
		}
		Some(decode_table_schema_descriptor_v1(&trailing[..])?)
	} else {
		if response.accepted != 0 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"TABLE_DESCRIPTOR_GET_RESP invalid accepted value {}",
					response.accepted
				),
			));
		}
		if response.descriptor_len != 0 || frame.trailing.is_some() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"TABLE_DESCRIPTOR_GET_RESP rejected response must not carry descriptor payload",
			));
		}
		None
	};
	Ok(TableDescriptorGetResult {
		response,
		descriptor,
	})
}

fn decode_table_descriptor_list_response_frame(
	frame: crate::network::core::HrftFrame,
) -> std::io::Result<TableDescriptorListResult> {
	if frame.header.msg_type != MsgType::TABLE_DESCRIPTOR_LIST_RESP.0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"expected TABLE_DESCRIPTOR_LIST_RESP, got msg_type=0x{:02x}",
				frame.header.msg_type
			),
		));
	}
	if frame.meta.len() != std::mem::size_of::<TableDescriptorListResponseMeta>() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"TABLE_DESCRIPTOR_LIST_RESP meta size mismatch",
		));
	}
	if frame.arrow_payload.is_some() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"TABLE_DESCRIPTOR_LIST_RESP must not carry an Arrow payload",
		));
	}
	let response: TableDescriptorListResponseMeta = *bytemuck::from_bytes(
		&frame.meta[..std::mem::size_of::<TableDescriptorListResponseMeta>()],
	);
	let descriptors = if response.accepted == 1 {
		if response.reason_code != CONTROL_REASON_GRANTED {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"TABLE_DESCRIPTOR_LIST_RESP accepted response must use granted reason",
			));
		}
		let trailing = frame.trailing.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"TABLE_DESCRIPTOR_LIST_RESP accepted response missing descriptors payload",
			)
		})?;
		if response.descriptors_len as usize != trailing.len() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"TABLE_DESCRIPTOR_LIST_RESP descriptors_len mismatch: meta={} actual={}",
					response.descriptors_len,
					trailing.len()
				),
			));
		}
		decode_table_schema_descriptors(&trailing[..])?
	} else {
		if response.accepted != 0 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"TABLE_DESCRIPTOR_LIST_RESP invalid accepted value {}",
					response.accepted
				),
			));
		}
		if response.descriptors_len != 0 || frame.trailing.is_some() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"TABLE_DESCRIPTOR_LIST_RESP rejected response must not carry descriptors payload",
			));
		}
		Vec::new()
	};
	Ok(TableDescriptorListResult {
		response,
		descriptors,
	})
}

fn decode_table_optimizer_facts_get_response_frame(
	frame: crate::network::core::HrftFrame,
) -> std::io::Result<TableOptimizerFactsGetResult> {
	if frame.header.msg_type != MsgType::TABLE_OPTIMIZER_FACTS_GET_RESP.0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"expected TABLE_OPTIMIZER_FACTS_GET_RESP, got msg_type=0x{:02x}",
				frame.header.msg_type
			),
		));
	}
	if frame.meta.len() != std::mem::size_of::<TableOptimizerFactsGetResponseMeta>() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"TABLE_OPTIMIZER_FACTS_GET_RESP meta size mismatch",
		));
	}
	if frame.arrow_payload.is_some() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"TABLE_OPTIMIZER_FACTS_GET_RESP must not carry an Arrow payload",
		));
	}
	let response: TableOptimizerFactsGetResponseMeta = *bytemuck::from_bytes(
		&frame.meta[..std::mem::size_of::<TableOptimizerFactsGetResponseMeta>()],
	);
	let facts = if response.accepted == 1 {
		if response.reason_code != CONTROL_REASON_GRANTED {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"TABLE_OPTIMIZER_FACTS_GET_RESP accepted response must use granted reason",
			));
		}
		if response.facts_len == 0 {
			if frame.trailing.is_some() {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"TABLE_OPTIMIZER_FACTS_GET_RESP empty response must not carry facts payload",
				));
			}
			None
		} else {
			let trailing = frame.trailing.as_ref().ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					"TABLE_OPTIMIZER_FACTS_GET_RESP accepted response missing facts payload",
				)
			})?;
			if response.facts_len as usize != trailing.len() {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"TABLE_OPTIMIZER_FACTS_GET_RESP facts_len mismatch: meta={} actual={}",
						response.facts_len,
						trailing.len()
					),
				));
			}
			Some(decode_table_optimizer_facts(&trailing[..])?)
		}
	} else {
		if response.accepted != 0 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"TABLE_OPTIMIZER_FACTS_GET_RESP invalid accepted value {}",
					response.accepted
				),
			));
		}
		if response.facts_len != 0 || frame.trailing.is_some() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"TABLE_OPTIMIZER_FACTS_GET_RESP rejected response must not carry facts payload",
			));
		}
		None
	};
	Ok(TableOptimizerFactsGetResult { response, facts })
}

fn decode_logical_type_get_response_frame(
	frame: crate::network::core::HrftFrame,
) -> std::io::Result<LogicalTypeGetResult> {
	if frame.header.msg_type != MsgType::LOGICAL_TYPE_GET_RESP.0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"expected LOGICAL_TYPE_GET_RESP, got msg_type=0x{:02x}",
				frame.header.msg_type
			),
		));
	}
	if frame.meta.len() != std::mem::size_of::<LogicalTypeGetResponseMeta>() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"LOGICAL_TYPE_GET_RESP meta size mismatch",
		));
	}
	if frame.arrow_payload.is_some() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"LOGICAL_TYPE_GET_RESP must not carry an Arrow payload",
		));
	}
	let response: LogicalTypeGetResponseMeta =
		*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<LogicalTypeGetResponseMeta>()]);
	let definition = if response.accepted == 1 {
		if response.reason_code != CONTROL_REASON_GRANTED {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"LOGICAL_TYPE_GET_RESP accepted response must use granted reason",
			));
		}
		let trailing = frame.trailing.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"LOGICAL_TYPE_GET_RESP accepted response missing definition payload",
			)
		})?;
		if response.definition_len as usize != trailing.len() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"LOGICAL_TYPE_GET_RESP definition_len mismatch: meta={} actual={}",
					response.definition_len,
					trailing.len()
				),
			));
		}
		Some(decode_logical_type_definition(&trailing[..])?)
	} else {
		if response.accepted != 0 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"LOGICAL_TYPE_GET_RESP invalid accepted value {}",
					response.accepted
				),
			));
		}
		if response.definition_len != 0 || frame.trailing.is_some() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"LOGICAL_TYPE_GET_RESP rejected response must not carry definition payload",
			));
		}
		None
	};
	Ok(LogicalTypeGetResult {
		response,
		definition,
	})
}

fn decode_logical_type_list_response_frame(
	frame: crate::network::core::HrftFrame,
) -> std::io::Result<LogicalTypeListResult> {
	if frame.header.msg_type != MsgType::LOGICAL_TYPE_LIST_RESP.0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"expected LOGICAL_TYPE_LIST_RESP, got msg_type=0x{:02x}",
				frame.header.msg_type
			),
		));
	}
	if frame.meta.len() != std::mem::size_of::<LogicalTypeListResponseMeta>() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"LOGICAL_TYPE_LIST_RESP meta size mismatch",
		));
	}
	if frame.arrow_payload.is_some() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"LOGICAL_TYPE_LIST_RESP must not carry an Arrow payload",
		));
	}
	let response: LogicalTypeListResponseMeta =
		*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<LogicalTypeListResponseMeta>()]);
	let definitions = if response.accepted == 1 {
		if response.reason_code != CONTROL_REASON_GRANTED {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"LOGICAL_TYPE_LIST_RESP accepted response must use granted reason",
			));
		}
		let trailing = frame.trailing.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"LOGICAL_TYPE_LIST_RESP accepted response missing definitions payload",
			)
		})?;
		if response.definitions_len as usize != trailing.len() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"LOGICAL_TYPE_LIST_RESP definitions_len mismatch: meta={} actual={}",
					response.definitions_len,
					trailing.len()
				),
			));
		}
		decode_logical_type_definitions(&trailing[..])?
	} else {
		if response.accepted != 0 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"LOGICAL_TYPE_LIST_RESP invalid accepted value {}",
					response.accepted
				),
			));
		}
		if response.definitions_len != 0 || frame.trailing.is_some() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"LOGICAL_TYPE_LIST_RESP rejected response must not carry definitions payload",
			));
		}
		Vec::new()
	};
	Ok(LogicalTypeListResult {
		response,
		definitions,
	})
}

fn decode_table_query_cancel_response_frame(
	frame: crate::network::core::HrftFrame,
) -> std::io::Result<TableQueryCancelResponseMeta> {
	if frame.header.msg_type != MsgType::TABLE_QUERY_CANCEL_RESP.0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"expected TABLE_QUERY_CANCEL_RESP, got msg_type=0x{:02x}",
				frame.header.msg_type
			),
		));
	}
	if frame.meta.len() != std::mem::size_of::<TableQueryCancelResponseMeta>() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"TABLE_QUERY_CANCEL_RESP meta size mismatch",
		));
	}
	if frame.arrow_payload.is_some() || frame.trailing.is_some() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"TABLE_QUERY_CANCEL_RESP must not carry payload bytes",
		));
	}
	let response: TableQueryCancelResponseMeta =
		*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<TableQueryCancelResponseMeta>()]);
	Ok(response)
}

fn decode_batch(bytes: &[u8]) -> std::io::Result<RecordBatch> {
	crate::network::ipc::wire_decode_batch(bytes)
}

fn cdc_decode_error(err: CdcDecodeError) -> Error {
	Error::new(ErrorKind::InvalidData, err)
}

fn encode_batch(batch: &RecordBatch) -> std::io::Result<Vec<u8>> {
	let mut buf = Vec::new();
	let started = Instant::now();
	let options = crate::network::ipc::wire_ipc_write_options().map_err(|err| {
		crate::network::ipc::observe_wire_encode_error(NetworkCompressionPath::ClientBatch);
		err
	})?;
	{
		let mut writer = StreamWriter::try_new_with_options(&mut buf, &batch.schema(), options)
			.map_err(|e| {
				crate::network::ipc::observe_wire_encode_error(NetworkCompressionPath::ClientBatch);
				Error::new(ErrorKind::InvalidData, format!("arrow stream init: {e}"))
			})?;
		writer.write(batch).map_err(|e| {
			crate::network::ipc::observe_wire_encode_error(NetworkCompressionPath::ClientBatch);
			Error::new(ErrorKind::InvalidData, format!("arrow stream write: {e}"))
		})?;
		writer.finish().map_err(|e| {
			crate::network::ipc::observe_wire_encode_error(NetworkCompressionPath::ClientBatch);
			Error::new(ErrorKind::InvalidData, format!("arrow stream finish: {e}"))
		})?;
	}
	crate::network::ipc::observe_wire_encode_success(
		NetworkCompressionPath::ClientBatch,
		buf.len() as u64,
		started.elapsed(),
	);
	Ok(buf)
}

fn table_delete_rows_schema() -> Schema {
	Schema::new(vec![Field::new(
		STORAGE_ROW_KEY_ID_COLUMN,
		DataType::FixedSizeBinary(32),
		false,
	)])
}

fn table_delete_rows_batch(row_key_ids: &[Identity]) -> std::io::Result<RecordBatch> {
	let mut builder = FixedSizeBinaryBuilder::with_capacity(row_key_ids.len(), 32);
	for row_key_id in row_key_ids {
		builder
			.append_value(row_key_id.as_bytes())
			.map_err(|e| Error::new(ErrorKind::InvalidInput, format!("row key id append: {e}")))?;
	}
	RecordBatch::try_new(
		Arc::new(table_delete_rows_schema()),
		vec![Arc::new(builder.finish())],
	)
	.map_err(|e| {
		Error::new(
			ErrorKind::InvalidInput,
			format!("TABLE_DELETE_REQ payload: {e}"),
		)
	})
}

fn validate_table_delete_request(request: &TableDeleteRowsRequest) -> std::io::Result<()> {
	request
		.descriptor
		.validate()
		.map_err(|e| Error::new(ErrorKind::InvalidInput, e.to_string()))?;
	if request.meta.table_object_id != request.descriptor.table_object_id {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"TABLE_DELETE_REQ table_object_id does not match descriptor",
		));
	}
	if request.meta.schema_generation != request.descriptor.schema_generation {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"TABLE_DELETE_REQ schema_generation does not match descriptor",
		));
	}
	if request.row_key_ids.is_empty() {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"TABLE_DELETE_REQ requires at least one row key id",
		));
	}
	if request.row_key_ids.len() != request.meta.row_count as usize {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			format!(
				"TABLE_DELETE_REQ row_count mismatch: meta={} actual={}",
				request.meta.row_count,
				request.row_key_ids.len()
			),
		));
	}
	Ok(())
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
	use crate::network::tls::ActorIdentity;
	use crate::network::transport::{
		TransportConnection, TransportProvider, TransportStream, TransportTier,
	};
	use crate::schema::{
		ClientWriteId, ClusteringColumnSpecV1, ColumnLocalityDescriptorV1, LocalityPolicy, NdvFact,
		PrimaryKeyDescriptorV1, ProjectionFunctionV1, RowIdentityPolicy, SchemaEvolutionPolicyV1,
		TableObjectId, TableOptimizerFacts, TableSchemaDescriptor, TableSpreadLocalityDescriptorV1,
		encode_table_optimizer_facts, encode_table_schema_descriptor_v1,
		encode_table_schema_descriptors,
	};
	use arrow::datatypes::{DataType, Field, Schema};
	use async_trait::async_trait;
	use std::collections::VecDeque;
	use std::pin::Pin;
	use std::task::{Context, Poll};
	use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};

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
					capability_bits: 0,
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

	#[derive(Clone)]
	struct DescriptorGetResponseSpec {
		meta: TableDescriptorGetResponseMeta,
		trailing: Option<Vec<u8>>,
	}

	struct ScriptedDescriptorGetProvider {
		responses: Arc<std::sync::Mutex<HashMap<u32, VecDeque<DescriptorGetResponseSpec>>>>,
		opened_targets: Arc<std::sync::Mutex<Vec<u32>>>,
		pool: Arc<BufPool>,
	}

	impl ScriptedDescriptorGetProvider {
		fn new(responses: Vec<(u32, DescriptorGetResponseSpec)>) -> Arc<Self> {
			let mut by_target: HashMap<u32, VecDeque<DescriptorGetResponseSpec>> = HashMap::new();
			for (target, response) in responses {
				by_target.entry(target).or_default().push_back(response);
			}
			Arc::new(Self {
				responses: Arc::new(std::sync::Mutex::new(by_target)),
				opened_targets: Arc::new(std::sync::Mutex::new(Vec::new())),
				pool: BufPool::new(8, 64 * 1024),
			})
		}

		fn opened_targets(&self) -> Vec<u32> {
			self.opened_targets.lock().unwrap().clone()
		}
	}

	struct ScriptedDescriptorGetConnection {
		remote: NodeId,
		responses: Arc<std::sync::Mutex<HashMap<u32, VecDeque<DescriptorGetResponseSpec>>>>,
		opened_targets: Arc<std::sync::Mutex<Vec<u32>>>,
		pool: Arc<BufPool>,
		actor: ActorIdentity,
	}

	#[async_trait]
	impl TransportProvider for ScriptedDescriptorGetProvider {
		async fn connect(
			&self,
			target: NodeId,
			_address: &str,
		) -> std::io::Result<Arc<dyn TransportConnection>> {
			Ok(Arc::new(ScriptedDescriptorGetConnection {
				remote: target,
				responses: self.responses.clone(),
				opened_targets: self.opened_targets.clone(),
				pool: self.pool.clone(),
				actor: ActorIdentity::data_node(target),
			}))
		}
	}

	struct DuplexTransportStream {
		inner: DuplexStream,
	}

	impl AsyncRead for DuplexTransportStream {
		fn poll_read(
			mut self: Pin<&mut Self>,
			cx: &mut Context<'_>,
			buf: &mut ReadBuf<'_>,
		) -> Poll<std::io::Result<()>> {
			Pin::new(&mut self.inner).poll_read(cx, buf)
		}
	}

	impl AsyncWrite for DuplexTransportStream {
		fn poll_write(
			mut self: Pin<&mut Self>,
			cx: &mut Context<'_>,
			buf: &[u8],
		) -> Poll<std::io::Result<usize>> {
			Pin::new(&mut self.inner).poll_write(cx, buf)
		}

		fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
			Pin::new(&mut self.inner).poll_flush(cx)
		}

		fn poll_shutdown(
			mut self: Pin<&mut Self>,
			cx: &mut Context<'_>,
		) -> Poll<std::io::Result<()>> {
			Pin::new(&mut self.inner).poll_shutdown(cx)
		}
	}

	#[async_trait]
	impl TransportStream for DuplexTransportStream {
		fn stream_id(&self) -> u64 {
			0
		}

		async fn finish(&mut self) -> std::io::Result<()> {
			self.inner.shutdown().await
		}
	}

	#[async_trait]
	impl TransportConnection for ScriptedDescriptorGetConnection {
		async fn open_bi(&self) -> std::io::Result<Box<dyn TransportStream>> {
			let response = self
				.responses
				.lock()
				.unwrap()
				.get_mut(&self.remote.id)
				.and_then(VecDeque::pop_front)
				.ok_or_else(|| {
					Error::new(
						ErrorKind::NotFound,
						format!("no scripted response for node {}", self.remote.id),
					)
				})?;
			self.opened_targets.lock().unwrap().push(self.remote.id);
			let (client, mut server) = tokio::io::duplex(64 * 1024);
			let pool = self.pool.clone();
			tokio::spawn(async move {
				let request = read_frame(&mut server, &pool)
					.await
					.expect("descriptor-get request frame");
				assert_eq!(request.header.msg_type, MsgType::TABLE_DESCRIPTOR_GET_REQ.0);
				let header = FrameHeader {
					magic: HRFT_MAGIC,
					version: 1,
					msg_type: MsgType::TABLE_DESCRIPTOR_GET_RESP.0,
					flags: 0,
					stream_id: request.header.stream_id,
					meta_len: std::mem::size_of::<TableDescriptorGetResponseMeta>() as u32,
					arrow_len: 0,
					trailing_len: response
						.trailing
						.as_ref()
						.map(|bytes| bytes.len() as u32)
						.unwrap_or(0),
				};
				write_frame(
					&mut server,
					&header,
					bytemuck::bytes_of(&response.meta),
					None,
					response.trailing.as_deref(),
				)
				.await
				.expect("descriptor-get response write");
				server.shutdown().await.expect("server stream shutdown");
			});
			Ok(Box::new(DuplexTransportStream { inner: client }))
		}

		async fn accept_bi(&self) -> std::io::Result<Box<dyn TransportStream>> {
			unimplemented!("client tests never accept inbound streams")
		}

		fn remote_id(&self) -> NodeId {
			self.remote
		}

		fn actor_identity(&self) -> &ActorIdentity {
			&self.actor
		}

		fn transport_type(&self) -> TransportTier {
			TransportTier::QuicIoUring
		}

		fn is_closed(&self) -> bool {
			false
		}
	}

	fn table_query_descriptor(locality_policy: LocalityPolicy) -> TableSchemaDescriptor {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, true),
		]);
		TableSchemaDescriptor::new(
			TableObjectId::new([0x51; 16]),
			3,
			"client",
			"items",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			locality_policy,
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap()
	}

	fn descriptor_get_config(
		bootstrap: ClusterStatusWire,
		topology: Arc<dyn TopologySource>,
		provider: Arc<dyn TransportProvider>,
	) -> WendelonClientConfig {
		let mut providers = HashMap::new();
		providers.insert(TransportTier::QuicIoUring, provider);
		WendelonClientConfig {
			local_tiers: TransportTierSet::empty().with(TransportTier::QuicIoUring),
			providers,
			bootstrap,
			topology,
			pool_max_retained: 8,
			pool_max_cap_bytes: 64 * 1024,
		}
	}

	fn descriptor_get_response(
		accepted: u8,
		reason_code: u8,
		leader_hint: NodeId,
		trailing: Option<Vec<u8>>,
	) -> DescriptorGetResponseSpec {
		DescriptorGetResponseSpec {
			meta: TableDescriptorGetResponseMeta {
				accepted,
				reason_code,
				_pad1: [0; 6],
				leader_hint,
				descriptor_len: trailing
					.as_ref()
					.map(|bytes| bytes.len() as u32)
					.unwrap_or(0),
				_pad2: [0; 4],
			},
			trailing,
		}
	}

	#[test]
	fn client_table_optimizer_facts_response_decodes_optional_payload() {
		let descriptor = table_query_descriptor(LocalityPolicy::default_table_spread());
		let facts = TableOptimizerFacts {
			table_object_id: descriptor.table_object_id,
			schema_generation: descriptor.schema_generation,
			physical_descriptor_fingerprint: descriptor.physical_descriptor_fingerprint,
			producer_epoch: 7,
			row_upper_bound: Some(42),
			column_ndv: vec![crate::schema::ColumnNdvFact {
				column_ordinal: 1,
				distinct: NdvFact::Approx {
					estimate: 42,
					rel_err_ppm: 26_000,
				},
			}],
			column_range: Vec::new(),
			column_range_distributions: Vec::new(),
			column_heavy_hitters: Vec::new(),
			column_group_top_k: Vec::new(),
		};
		let facts_bytes = encode_table_optimizer_facts(&facts).expect("encode facts");
		let frame = crate::network::core::HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::TABLE_OPTIMIZER_FACTS_GET_RESP.0,
				flags: 0,
				stream_id: 0,
				meta_len: std::mem::size_of::<TableOptimizerFactsGetResponseMeta>() as u32,
				arrow_len: 0,
				trailing_len: facts_bytes.len() as u32,
			},
			meta: crate::network::pool::PooledBuf::orphan(
				bytemuck::bytes_of(&TableOptimizerFactsGetResponseMeta {
					accepted: 1,
					reason_code: CONTROL_REASON_GRANTED,
					_pad1: [0; 6],
					leader_hint: NodeId {
						id: 1,
						_reserved: 0,
					},
					facts_len: facts_bytes.len() as u32,
					_pad2: [0; 4],
				})
				.to_vec(),
			),
			arrow_payload: None,
			trailing: Some(crate::network::pool::PooledBuf::orphan(facts_bytes)),
		};

		let decoded = decode_table_optimizer_facts_get_response_frame(frame).expect("decode facts");
		assert_eq!(decoded.facts, Some(facts));
	}

	#[test]
	fn client_table_optimizer_facts_response_rejects_length_mismatch() {
		let frame = crate::network::core::HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::TABLE_OPTIMIZER_FACTS_GET_RESP.0,
				flags: 0,
				stream_id: 0,
				meta_len: std::mem::size_of::<TableOptimizerFactsGetResponseMeta>() as u32,
				arrow_len: 0,
				trailing_len: 1,
			},
			meta: crate::network::pool::PooledBuf::orphan(
				bytemuck::bytes_of(&TableOptimizerFactsGetResponseMeta {
					accepted: 1,
					reason_code: CONTROL_REASON_GRANTED,
					_pad1: [0; 6],
					leader_hint: NodeId {
						id: 1,
						_reserved: 0,
					},
					facts_len: 2,
					_pad2: [0; 4],
				})
				.to_vec(),
			),
			arrow_payload: None,
			trailing: Some(crate::network::pool::PooledBuf::orphan(vec![0])),
		};

		let err = decode_table_optimizer_facts_get_response_frame(frame).unwrap_err();
		assert!(err.to_string().contains("facts_len mismatch"));
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
	async fn client_get_table_schema_descriptor_retries_to_leader() {
		let descriptor = table_query_descriptor(LocalityPolicy::default_table_spread());
		let descriptor_bytes = encode_table_schema_descriptor_v1(&descriptor).unwrap();
		let leader = NodeId {
			id: 2,
			_reserved: 0,
		};
		let provider = ScriptedDescriptorGetProvider::new(vec![
			(
				1,
				descriptor_get_response(0, CONTROL_REASON_NOT_LEADER, leader, None),
			),
			(
				2,
				descriptor_get_response(1, CONTROL_REASON_GRANTED, leader, Some(descriptor_bytes)),
			),
		]);
		let bootstrap = snapshot(1, Some(1), &[1, 2, 3]);
		let source = Arc::new(StaticTopologySource::new(snapshot(2, Some(2), &[1, 2, 3])));
		let client =
			WendelonClient::new(descriptor_get_config(bootstrap, source, provider.clone()));

		let actual = client
			.get_table_schema_descriptor(TenantId::new(7), "client", "items")
			.await
			.unwrap()
			.expect("descriptor");

		assert_eq!(actual, descriptor);
		assert_eq!(provider.opened_targets(), vec![1, 2]);
		assert_eq!(client.leader(), Some(leader));
	}

	#[tokio::test]
	async fn client_get_table_schema_descriptor_maps_not_found_to_none() {
		let leader = NodeId {
			id: 1,
			_reserved: 0,
		};
		let provider = ScriptedDescriptorGetProvider::new(vec![(
			1,
			descriptor_get_response(0, CONTROL_REASON_NOT_FOUND, leader, None),
		)]);
		let bootstrap = snapshot(1, Some(1), &[1, 2, 3]);
		let source = Arc::new(StaticTopologySource::new(bootstrap.clone()));
		let client =
			WendelonClient::new(descriptor_get_config(bootstrap, source, provider.clone()));

		let actual = client
			.get_table_schema_descriptor(TenantId::new(7), "client", "missing")
			.await
			.unwrap();

		assert!(actual.is_none());
		assert_eq!(provider.opened_targets(), vec![1]);
	}

	#[test]
	fn client_get_table_schema_descriptor_rejects_descriptor_len_mismatch() {
		let descriptor = table_query_descriptor(LocalityPolicy::default_table_spread());
		let descriptor_bytes = encode_table_schema_descriptor_v1(&descriptor).unwrap();
		let pool = BufPool::new(2, 64 * 1024);
		let mut meta_buf = pool.acquire();
		let meta = TableDescriptorGetResponseMeta {
			accepted: 1,
			reason_code: CONTROL_REASON_GRANTED,
			_pad1: [0; 6],
			leader_hint: NodeId {
				id: 1,
				_reserved: 0,
			},
			descriptor_len: descriptor_bytes.len() as u32 + 1,
			_pad2: [0; 4],
		};
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&meta));
		let mut trailing = pool.acquire();
		trailing.as_mut_vec().extend_from_slice(&descriptor_bytes);
		let frame = crate::network::core::HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::TABLE_DESCRIPTOR_GET_RESP.0,
				flags: 0,
				stream_id: 0,
				meta_len: std::mem::size_of::<TableDescriptorGetResponseMeta>() as u32,
				arrow_len: 0,
				trailing_len: descriptor_bytes.len() as u32,
			},
			meta: meta_buf,
			arrow_payload: None,
			trailing: Some(trailing),
		};

		let err = decode_table_descriptor_get_response_frame(frame).unwrap_err();
		assert_eq!(err.kind(), ErrorKind::InvalidData);
		assert!(err.to_string().contains("descriptor_len mismatch"));
	}

	#[test]
	fn client_list_table_schema_descriptors_decodes_descriptor_payload() {
		let descriptor = table_query_descriptor(LocalityPolicy::default_table_spread());
		let descriptors_bytes =
			encode_table_schema_descriptors(std::slice::from_ref(&descriptor)).unwrap();
		let pool = BufPool::new(2, 64 * 1024);
		let mut meta_buf = pool.acquire();
		let meta = TableDescriptorListResponseMeta {
			accepted: 1,
			reason_code: CONTROL_REASON_GRANTED,
			_pad1: [0; 6],
			leader_hint: NodeId {
				id: 1,
				_reserved: 0,
			},
			descriptors_len: descriptors_bytes.len() as u32,
			_pad2: [0; 4],
		};
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&meta));
		let mut trailing = pool.acquire();
		trailing.as_mut_vec().extend_from_slice(&descriptors_bytes);
		let frame = crate::network::core::HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::TABLE_DESCRIPTOR_LIST_RESP.0,
				flags: 0,
				stream_id: 0,
				meta_len: std::mem::size_of::<TableDescriptorListResponseMeta>() as u32,
				arrow_len: 0,
				trailing_len: descriptors_bytes.len() as u32,
			},
			meta: meta_buf,
			arrow_payload: None,
			trailing: Some(trailing),
		};

		let result = decode_table_descriptor_list_response_frame(frame).unwrap();
		assert_eq!(result.descriptors, vec![descriptor]);
	}

	#[test]
	fn client_list_table_schema_descriptors_rejects_length_mismatch() {
		let descriptor = table_query_descriptor(LocalityPolicy::default_table_spread());
		let descriptors_bytes = encode_table_schema_descriptors(&[descriptor]).unwrap();
		let pool = BufPool::new(2, 64 * 1024);
		let mut meta_buf = pool.acquire();
		let meta = TableDescriptorListResponseMeta {
			accepted: 1,
			reason_code: CONTROL_REASON_GRANTED,
			_pad1: [0; 6],
			leader_hint: NodeId {
				id: 1,
				_reserved: 0,
			},
			descriptors_len: descriptors_bytes.len() as u32 + 1,
			_pad2: [0; 4],
		};
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&meta));
		let mut trailing = pool.acquire();
		trailing.as_mut_vec().extend_from_slice(&descriptors_bytes);
		let frame = crate::network::core::HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::TABLE_DESCRIPTOR_LIST_RESP.0,
				flags: 0,
				stream_id: 0,
				meta_len: std::mem::size_of::<TableDescriptorListResponseMeta>() as u32,
				arrow_len: 0,
				trailing_len: descriptors_bytes.len() as u32,
			},
			meta: meta_buf,
			arrow_payload: None,
			trailing: Some(trailing),
		};

		let err = decode_table_descriptor_list_response_frame(frame).unwrap_err();
		assert_eq!(err.kind(), ErrorKind::InvalidData);
		assert!(err.to_string().contains("descriptors_len mismatch"));
	}

	#[tokio::test]
	async fn slab_stats_pruning_gate_skips_peers_without_capability() {
		use crate::network::transport::{NodeCapability, NodeCapabilitySet};
		let mut snap = snapshot(1, Some(1), &[1, 2]);
		snap.peers[0].capability_bits = NodeCapabilitySet::empty()
			.with(NodeCapability::SlabStatsPruning)
			.as_u8();
		snap.peers[1].capability_bits = 0;
		let client = WendelonClient::new(test_config(snap));
		let advertising = NodeId {
			id: 1,
			_reserved: 0,
		};
		let silent = NodeId {
			id: 2,
			_reserved: 0,
		};

		assert!(client.peer_advertises(advertising, NodeCapability::SlabStatsPruning));
		assert!(!client.peer_advertises(silent, NodeCapability::SlabStatsPruning));

		let metric = &crate::metrics::global().pbc_slab_stats_skipped_no_capability_total;
		let before = metric.get();

		assert!(client.slab_stats_pruning_for_target(advertising, true));
		assert_eq!(
			metric.get(),
			before,
			"advertising peer must not increment the skip counter"
		);

		assert!(!client.slab_stats_pruning_for_target(silent, true));
		assert_eq!(
			metric.get(),
			before + 1,
			"non-advertising peer must increment the skip counter exactly once"
		);

		assert!(!client.slab_stats_pruning_for_target(silent, false));
		assert_eq!(
			metric.get(),
			before + 1,
			"caller-disabled requests must not increment the skip counter even on non-advertising peers"
		);
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
		assert!(retry_read_status(ReadStatus::TABLE_DESCRIPTOR_EVOLVED, zero).is_none());
		assert!(retry_read_status(ReadStatus::QUERY_CANCELLED, zero).is_none());
	}

	#[test]
	fn table_query_route_uses_first_table_spread_prefix() {
		let tenant_id = TenantId::new(9);
		let spread = TableSpreadLocalityDescriptorV1 {
			spread_factor: 4,
			prefix_bits: 12,
		};
		let descriptor = table_query_descriptor(LocalityPolicy::TableSpreadProjection(spread));
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
			table_query_descriptor(LocalityPolicy::ColumnProjection(projection.clone()));
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
		let descriptor = table_query_descriptor(LocalityPolicy::default_table_spread());
		let request = TableQueryRequest {
			meta: TableQueryRequestMeta {
				min_hlc: bytemuck::Zeroable::zeroed(),
				expected_epoch: 7,
				tenant_id: TenantId::new(9),
				table_object_id: descriptor.table_object_id,
				schema_generation: descriptor.schema_generation,
				physical_descriptor_fingerprint: *descriptor
					.physical_descriptor_fingerprint
					.as_bytes(),
				consistency: Consistency::LINEARIZABLE,
				row_identity_projection: RowIdentityProjection::None.as_u8(),
				_pad: [0; 6],
				bytecode_len: 2,
				runtime_filter_len: 0,
				as_of_hlc: bytemuck::Zeroable::zeroed(),
				row_limit_rows: 0,
				request_id: TableQueryRequestId::from_u128(1),
				trace_id: [0; 16],
				trace_parent_span_id: [0; 8],
				trace_flags: 0,
				_trace_pad: [0; 7],
			},
			descriptor,
			pbc: vec![0x01, 0x02],
			runtime_filter: None,
			row_identity: RowIdentityProjection::None,
		};
		validate_table_query_request(&request).unwrap();

		let mut bad = request.clone();
		bad.meta.bytecode_len = 1;
		let err = validate_table_query_request(&bad).unwrap_err();
		assert_eq!(err.kind(), ErrorKind::InvalidInput);

		let mut bad = request.clone();
		bad.meta.physical_descriptor_fingerprint = [0xFF; 32];
		let err = validate_table_query_request(&bad).unwrap_err();
		assert_eq!(err.kind(), ErrorKind::InvalidInput);

		let mut bad = request.clone();
		bad.meta.request_id = TableQueryRequestId::ZERO;
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
			query_failure_error(ReadStatus::TABLE_DESCRIPTOR_EVOLVED).to_string(),
			"Wendelon query failed: TABLE_DESCRIPTOR_EVOLVED"
		);
		assert_eq!(
			query_failure_error(ReadStatus::QUERY_CANCELLED).to_string(),
			"Wendelon query failed: QUERY_CANCELLED"
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
	async fn table_query_terminal_error_after_data_fails_cursor() {
		let (client_side, mut server_side) = tokio::io::duplex(4096);
		let terminal = QueryResponseMeta {
			status: ReadStatus::QUERY_BUDGET_EXCEEDED,
			_pad1: [0; 7],
			value_hlc: HybridTimestamp {
				physical_ms: 0,
				logical: 0,
				node_id: 0,
			},
			epoch: 7,
			leader_hint: NodeId {
				id: 1,
				_reserved: 0,
			},
			rows: 0,
			arrow_len: 0,
			_pad2: [0; 4],
		};
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::TABLE_QUERY_RESP.0,
			flags: FRAME_FLAG_STREAM_END,
			stream_id: 9,
			meta_len: std::mem::size_of::<QueryResponseMeta>() as u32,
			arrow_len: 0,
			trailing_len: 0,
		};
		write_frame(
			&mut server_side,
			&header,
			bytemuck::bytes_of(&terminal),
			None,
			None,
		)
		.await
		.unwrap();
		tokio::io::AsyncWriteExt::shutdown(&mut server_side)
			.await
			.unwrap();

		let mut result = StreamingQueryResult {
			value_hlc: terminal.value_hlc,
			epoch: terminal.epoch,
			rows: 1,
			pending_batch: Some(RecordBatch::new_empty(Arc::new(Schema::empty()))),
			stream: Some(Box::new(DuplexTransportStream { inner: client_side })),
			pool: BufPool::new(4, 4096),
			expected_msg_type: MsgType::TABLE_QUERY_RESP,
			meta_mode: StreamingQueryMetaMode::TerminalMetadata,
			_table_query_attempt_guard: None,
		};

		assert!(result.next_batch().await.unwrap().is_some());
		let err = result.next_batch().await.unwrap_err();
		assert_eq!(err.kind(), ErrorKind::Other);
		assert!(err.to_string().contains("QUERY_BUDGET_EXCEEDED"));
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
					capability_bits: 0,
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
