//! Per-ring dispatch table built on top of [`HrftService`].
//!
//! The QUIC server in [`crate::network::server`] accepts one frame
//! per bidi stream and hands it to a `HrftService`. Production
//! deployments want that single service to fan out to three
//! independent subsystems keyed off `msg_type`:
//!
//! ```text
//!   0x01..=0x0F  →  Data Ring
//!   0x10..=0x1F  →  Control Ring core RPCs
//!   0x20..=0x2F  →  Raft internal, with exact-match Data Ring extensions
//!   0x30..=0x3F  →  Exact-match protocol extensions
//! ```
//!
//! The router uses an explicit dispatch table rather than trusting
//! arithmetic ranges blindly. Most families are range-grouped, but
//! peer-only and admin extensions are matched by exact `MsgType`
//! first so future additions do not collide silently.
//!
//! Each subsystem implements a narrow trait ([`DataRingServer`],
//! [`ControlRingServer`], [`RaftInternalServer`]) and is plugged
//! into a [`MsgRouter`] via [`MsgRouterBuilder`]. The router itself
//! implements [`HrftService`], so it drops straight into
//! [`crate::network::server::serve`].
//!
//! ## Why three traits instead of one dispatch map
//!
//! A `HashMap<u8, Arc<dyn Handler>>` would be one line, but the
//! three traits encode a guarantee at compile time: the Raft
//! internal handler never sees a Control Ring frame, and vice
//! versa. That isolation matches how the subsystems are owned at
//! runtime — the Raft engine is not aware of tenant-facing RPCs.
//!
//! ## Internal demux
//!
//! Each trait takes a whole [`HrftFrame`] because within a ring the
//! implementor still needs to switch on `msg_type` to decode the
//! right metadata struct. Making the trait per-RPC would push that
//! decode into the router and defeat the isolation.

use crate::network::core::{HrftFrame, read_frame_from_header, read_frame_header, write_frame};
use crate::network::pool::BufPool;
use crate::network::server::{HrftService, RequestContext};
use crate::network::transport::TransportStream;
use crate::schema::{FrameHeader, MsgType};
use async_trait::async_trait;
use std::io::{Error, ErrorKind};
use std::sync::Arc;

// ============================================================
//  PER-RING TRAITS
// ============================================================

/// Handler for Data Ring frames: `msg_type` in `0x01..=0x0F`, plus
/// exact-match Data Ring extension frames.
#[async_trait]
pub trait DataRingServer: Send + Sync + 'static {
	async fn handle_data_frame(
		&self,
		context: RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<Option<HrftFrame>>;

	/// Optional client-streaming entry point. The default keeps the
	/// unary "read one frame, write at most one frame" behavior so
	/// implementors only need to override it for true streaming RPCs.
	async fn handle_data_stream(
		&self,
		context: RequestContext,
		first_header: FrameHeader,
		mut stream: Box<dyn TransportStream>,
		pool: Arc<BufPool>,
	) -> std::io::Result<()> {
		let frame = read_frame_from_header(&mut *stream, &pool, first_header).await?;
		match self.handle_data_frame(context, frame).await? {
			Some(resp) => {
				let is_table_query_response = resp.header.msg_type == MsgType::TABLE_QUERY_RESP.0;
				if let Err(err) = write_frame(
					&mut *stream,
					&resp.header,
					&resp.meta,
					resp.arrow_payload.as_deref(),
					resp.trailing.as_deref(),
				)
				.await
				{
					if is_table_query_response {
						crate::metrics::global().observe_table_query_cancellation("stream_dropped");
					}
					return Err(err);
				}
				if let Err(err) = stream.finish().await {
					if is_table_query_response {
						crate::metrics::global().observe_table_query_cancellation("stream_dropped");
					}
					return Err(err);
				}
			}
			None => {
				stream.finish().await?;
			}
		}
		Ok(())
	}
}

/// Handler for Control Ring frames routed by the explicit control/admin
/// dispatch table.
#[async_trait]
pub trait ControlRingServer: Send + Sync + 'static {
	async fn handle_control_frame(
		&self,
		context: RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<Option<HrftFrame>>;
}

/// Handler for internal Raft frames: current `msg_type` allocation
/// `0x20..=0x2F`.
#[async_trait]
pub trait RaftInternalServer: Send + Sync + 'static {
	async fn handle_raft_frame(
		&self,
		context: RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<Option<HrftFrame>>;
}

// ============================================================
//  ROUTER
// ============================================================

/// Ring-aware dispatcher. Each slot is optional so a node can be
/// stood up with, say, only the Raft internal surface during
/// bootstrap testing.
pub struct MsgRouter {
	data: Option<Arc<dyn DataRingServer>>,
	control: Option<Arc<dyn ControlRingServer>>,
	raft: Option<Arc<dyn RaftInternalServer>>,
}

impl MsgRouter {
	pub fn builder() -> MsgRouterBuilder {
		MsgRouterBuilder::default()
	}
}

#[async_trait]
impl HrftService for MsgRouter {
	async fn handle_frame(
		&self,
		context: RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<Option<HrftFrame>> {
		let msg_type = frame.header.msg_type;
		enforce_ingress(&context, MsgType(msg_type))?;
		match msg_type {
			x if x == MsgType::PENDING_WITNESS_REQ.0
				|| x == MsgType::PENDING_WITNESS_RESP.0
				|| x == MsgType::TABLE_WRITE_REQ.0
				|| x == MsgType::TABLE_WRITE_RESP.0
				|| x == MsgType::TABLE_REPLICATE_REQ.0
				|| x == MsgType::TABLE_REPLICATE_RESP.0
				|| x == MsgType::TABLE_QUERY_REQ.0
				|| x == MsgType::TABLE_QUERY_RESP.0
				|| x == MsgType::TABLE_QUERY_CANCEL_REQ.0
				|| x == MsgType::TABLE_QUERY_CANCEL_RESP.0
				|| x == MsgType::TABLE_DELETE_REQ.0
				|| x == MsgType::TABLE_DELETE_RESP.0
				|| x == MsgType::MUTATION_OUTCOME_REQ.0
				|| x == MsgType::MUTATION_OUTCOME_RESP.0
				|| x == MsgType::MUTATION_OUTCOME_PIN_REQ.0
				|| x == MsgType::MUTATION_OUTCOME_PIN_RESP.0
				|| x == MsgType::DROP_RECLAIM_SLICE_REQ.0
				|| x == MsgType::DROP_RECLAIM_SLICE_RESP.0
				|| x == MsgType::AGG_REQ.0
				|| x == MsgType::AGG_RESP.0
				|| x == MsgType::COUNTER_ADD_REQ.0
				|| x == MsgType::COUNTER_ADD_RESP.0
				|| x == MsgType::COUNTER_READ_REQ.0
				|| x == MsgType::COUNTER_READ_RESP.0
				|| x == MsgType::COUNTER_REPLICATE_REQ.0
				|| x == MsgType::COUNTER_REPLICATE_RESP.0
				|| x == MsgType::COUNTER_ROLLBACK_REQ.0
				|| x == MsgType::COUNTER_ROLLBACK_RESP.0
				|| x == MsgType::TABLE_CHILD_WRITE_REQ.0
				|| x == MsgType::TABLE_CHILD_WRITE_RESP.0
				|| x == MsgType::TABLE_CHILD_REPLICATE_REQ.0
				|| x == MsgType::TABLE_CHILD_REPLICATE_RESP.0
				|| x == MsgType::TABLE_CHILD_FETCH_REQ.0
				|| x == MsgType::TABLE_CHILD_FETCH_RESP.0 =>
			{
				match &self.data {
					Some(h) => h.handle_data_frame(context, frame).await,
					None => Err(unhandled("Data Ring", msg_type)),
				}
			}
			0x01..=0x0F => match &self.data {
				Some(h) => h.handle_data_frame(context, frame).await,
				None => Err(unhandled("Data Ring", msg_type)),
			},
			0x10..=0x1F => match &self.control {
				Some(h) => h.handle_control_frame(context, frame).await,
				None => Err(unhandled("Control Ring", msg_type)),
			},
			x if is_control_extension_request(x) => match &self.control {
				Some(h) => h.handle_control_frame(context, frame).await,
				None => Err(unhandled("Control Ring", msg_type)),
			},
			x if is_extension_response(x) => Err(Error::new(
				ErrorKind::InvalidData,
				format!("unexpected response HRFT msg_type=0x{:02x}", msg_type),
			)),
			0x20..=0x2F => match &self.raft {
				Some(h) => h.handle_raft_frame(context, frame).await,
				None => Err(unhandled("Raft internal", msg_type)),
			},
			_ => Err(Error::new(
				ErrorKind::InvalidData,
				format!("unknown HRFT msg_type=0x{:02x}", msg_type),
			)),
		}
	}

	async fn handle_stream(
		&self,
		mut stream: Box<dyn TransportStream>,
		pool: Arc<BufPool>,
		context: RequestContext,
	) -> std::io::Result<()> {
		let header = read_frame_header(&mut *stream).await?;
		enforce_ingress(&context, MsgType(header.msg_type))?;
		if header.msg_type == MsgType::BULK_WRITE_REQ.0
			|| header.msg_type == MsgType::STREAM_FIN_REQ.0
			|| header.msg_type == MsgType::QUERY_REQ.0
			|| header.msg_type == MsgType::TABLE_QUERY_REQ.0
		{
			return match &self.data {
				Some(h) => h.handle_data_stream(context, header, stream, pool).await,
				None => Err(unhandled("Data Ring", header.msg_type)),
			};
		}

		let frame = read_frame_from_header(&mut *stream, &pool, header).await?;
		match self.handle_frame(context, frame).await? {
			Some(resp) => {
				let is_table_query_response = resp.header.msg_type == MsgType::TABLE_QUERY_RESP.0;
				if let Err(err) = write_frame(
					&mut *stream,
					&resp.header,
					&resp.meta,
					resp.arrow_payload.as_deref(),
					resp.trailing.as_deref(),
				)
				.await
				{
					if is_table_query_response {
						crate::metrics::global().observe_table_query_cancellation("stream_dropped");
					}
					return Err(err);
				}
				if let Err(err) = stream.finish().await {
					if is_table_query_response {
						crate::metrics::global().observe_table_query_cancellation("stream_dropped");
					}
					return Err(err);
				}
			}
			None => {
				stream.finish().await?;
			}
		}
		Ok(())
	}
}

fn is_control_extension_request(msg_type: u8) -> bool {
	[
		MsgType::SCHEMA_UPDATE_REQ,
		MsgType::REVOCATION_ADMIN_REQ,
		MsgType::SCHEMA_DROP_REQ,
		MsgType::TABLE_DESCRIPTOR_GET_REQ,
		MsgType::LOGICAL_TYPE_GET_REQ,
		MsgType::LOGICAL_TYPE_LIST_REQ,
		MsgType::TABLE_DESCRIPTOR_LIST_REQ,
		MsgType::TABLE_OPTIMIZER_FACTS_GET_REQ,
		MsgType::SECONDARY_INDEX_STATUS_REQ,
		MsgType::CTAS_SAGA_REQ,
		MsgType::MAINTENANCE_REPORT_REQ,
		MsgType::MAINTENANCE_REQUEUE_REQ,
		// ADR 0699 S3a. Node-required: not in ACTOR_AUTHORIZED_CONTROL_REQUESTS.
		MsgType::DECISION_PROPOSE_REQ,
		MsgType::DECISION_LOOKUP_REQ,
		MsgType::DECISION_CUT_REQ,
	]
	.iter()
	.any(|candidate| candidate.0 == msg_type)
}

fn is_extension_response(msg_type: u8) -> bool {
	[
		MsgType::SCHEMA_UPDATE_RESP,
		MsgType::REVOCATION_ADMIN_RESP,
		MsgType::SCHEMA_DROP_RESP,
		MsgType::TABLE_DESCRIPTOR_GET_RESP,
		MsgType::LOGICAL_TYPE_GET_RESP,
		MsgType::LOGICAL_TYPE_LIST_RESP,
		MsgType::TABLE_DESCRIPTOR_LIST_RESP,
		MsgType::TABLE_OPTIMIZER_FACTS_GET_RESP,
		MsgType::SECONDARY_INDEX_STATUS_RESP,
		MsgType::CTAS_SAGA_RESP,
		MsgType::MAINTENANCE_REPORT_RESP,
		MsgType::MAINTENANCE_REQUEUE_RESP,
		MsgType::DECISION_PROPOSE_RESP,
		MsgType::DECISION_LOOKUP_RESP,
		MsgType::DECISION_CUT_RESP,
	]
	.iter()
	.any(|candidate| candidate.0 == msg_type)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IngressRequirement {
	NodeRequired,
	ActorAuthorizedControl,
}

/// Control requests deliberately reachable by authenticated non-node actors.
/// Every other known HRFT message is conservatively node-required. Therefore a
/// new opcode added to `MsgType::ALL` cannot become actor-authorized by omission.
const ACTOR_AUTHORIZED_CONTROL_REQUESTS: &[MsgType] = &[
	MsgType::EPOCH_ADV_REQ,
	MsgType::STATUS_REQ,
	MsgType::JOIN_REQ,
	MsgType::TENANT_ADMIN_REQ,
	MsgType::AUDIT_CHAIN_HEAD_REQ,
	MsgType::SCHEMA_UPDATE_REQ,
	MsgType::REVOCATION_ADMIN_REQ,
	MsgType::SCHEMA_DROP_REQ,
	MsgType::TABLE_DESCRIPTOR_GET_REQ,
	MsgType::LOGICAL_TYPE_GET_REQ,
	MsgType::LOGICAL_TYPE_LIST_REQ,
	MsgType::TABLE_DESCRIPTOR_LIST_REQ,
	MsgType::TABLE_OPTIMIZER_FACTS_GET_REQ,
	MsgType::SECONDARY_INDEX_STATUS_REQ,
	MsgType::CTAS_SAGA_REQ,
	MsgType::MAINTENANCE_REPORT_REQ,
	MsgType::MAINTENANCE_REQUEUE_REQ,
];

fn ingress_requirement(msg_type: MsgType) -> Option<IngressRequirement> {
	if ACTOR_AUTHORIZED_CONTROL_REQUESTS.contains(&msg_type) {
		return Some(IngressRequirement::ActorAuthorizedControl);
	}
	MsgType::ALL
		.contains(&msg_type)
		.then_some(IngressRequirement::NodeRequired)
}

fn enforce_ingress(context: &RequestContext, msg_type: MsgType) -> std::io::Result<()> {
	match ingress_requirement(msg_type) {
		Some(IngressRequirement::ActorAuthorizedControl) => Ok(()),
		Some(IngressRequirement::NodeRequired) => {
			context.require_authenticated_node(&format!("HRFT msg_type=0x{:02x}", msg_type.0))?;
			Ok(())
		}
		None => Err(Error::new(
			ErrorKind::InvalidData,
			format!("unclassified HRFT msg_type=0x{:02x}", msg_type.0),
		)),
	}
}

// ============================================================
//  BUILDER
// ============================================================

/// Fluent builder for [`MsgRouter`]. Unset rings return
/// `ErrorKind::Unsupported` for any frame that lands on them.
#[derive(Default)]
pub struct MsgRouterBuilder {
	data: Option<Arc<dyn DataRingServer>>,
	control: Option<Arc<dyn ControlRingServer>>,
	raft: Option<Arc<dyn RaftInternalServer>>,
}

impl MsgRouterBuilder {
	pub fn with_data_ring(mut self, handler: Arc<dyn DataRingServer>) -> Self {
		self.data = Some(handler);
		self
	}

	pub fn with_control_ring(mut self, handler: Arc<dyn ControlRingServer>) -> Self {
		self.control = Some(handler);
		self
	}

	pub fn with_raft_internal(mut self, handler: Arc<dyn RaftInternalServer>) -> Self {
		self.raft = Some(handler);
		self
	}

	pub fn build(self) -> MsgRouter {
		MsgRouter {
			data: self.data,
			control: self.control,
			raft: self.raft,
		}
	}
}

// ============================================================
//  ERROR HELPER
// ============================================================

fn unhandled(ring: &'static str, msg_type: u8) -> Error {
	Error::new(
		ErrorKind::Unsupported,
		format!("{ring} handler not installed (msg_type=0x{msg_type:02x})"),
	)
}

#[cfg(test)]
mod tests {
	use super::{
		ACTOR_AUTHORIZED_CONTROL_REQUESTS, ControlRingServer, DataRingServer, MsgRouterBuilder,
		enforce_ingress, ingress_requirement, is_control_extension_request, is_extension_response,
	};
	use crate::network::core::{HRFT_MAGIC, HrftFrame};
	use crate::network::server::{HrftService, RequestContext};
	use crate::network::tls::{ActorIdentity, ActorRole};
	use crate::schema::{FrameHeader, MsgType, NodeId};
	use async_trait::async_trait;
	use std::sync::Arc;
	use std::sync::atomic::{AtomicUsize, Ordering};

	fn operator_context() -> RequestContext {
		RequestContext {
			remote: None,
			actor: ActorIdentity::new(
				ActorRole::Operator,
				None,
				None,
				vec!["cluster".to_owned()],
				vec![1, 2, 3, 4],
			)
			.unwrap(),
		}
	}

	fn node_context(id: u32) -> RequestContext {
		let node = NodeId { id, _reserved: 0 };
		RequestContext {
			remote: Some(node),
			actor: ActorIdentity::data_node(node),
		}
	}

	fn frame(msg_type: MsgType) -> HrftFrame {
		HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: msg_type.0,
				flags: 0,
				stream_id: 1,
				meta_len: 0,
				arrow_len: 0,
				trailing_len: 0,
			},
			meta: crate::network::pool::PooledBuf::orphan(Vec::new()),
			arrow_payload: None,
			trailing: None,
		}
	}

	#[test]
	fn every_control_extension_preempts_the_raft_numeric_range() {
		for request in [
			MsgType::SCHEMA_UPDATE_REQ,
			MsgType::REVOCATION_ADMIN_REQ,
			MsgType::SCHEMA_DROP_REQ,
			MsgType::TABLE_DESCRIPTOR_GET_REQ,
			MsgType::LOGICAL_TYPE_GET_REQ,
			MsgType::LOGICAL_TYPE_LIST_REQ,
			MsgType::TABLE_DESCRIPTOR_LIST_REQ,
			MsgType::TABLE_OPTIMIZER_FACTS_GET_REQ,
			MsgType::SECONDARY_INDEX_STATUS_REQ,
		] {
			assert!(
				is_control_extension_request(request.0),
				"0x{:02x}",
				request.0
			);
		}
		assert!(is_control_extension_request(MsgType::SCHEMA_UPDATE_REQ.0));
		assert!(!is_control_extension_request(0x20));
	}

	#[test]
	fn extension_responses_are_never_dispatched_as_requests() {
		for response in [
			MsgType::SCHEMA_UPDATE_RESP,
			MsgType::REVOCATION_ADMIN_RESP,
			MsgType::SCHEMA_DROP_RESP,
			MsgType::TABLE_DESCRIPTOR_GET_RESP,
			MsgType::LOGICAL_TYPE_GET_RESP,
			MsgType::LOGICAL_TYPE_LIST_RESP,
			MsgType::TABLE_DESCRIPTOR_LIST_RESP,
			MsgType::TABLE_OPTIMIZER_FACTS_GET_RESP,
			MsgType::SECONDARY_INDEX_STATUS_RESP,
		] {
			assert!(is_extension_response(response.0), "0x{:02x}", response.0);
		}
	}

	#[test]
	fn ingress_census_classifies_every_named_message_and_nothing_unknown() {
		for msg_type in MsgType::ALL {
			assert!(
				ingress_requirement(*msg_type).is_some(),
				"0x{:02x}",
				msg_type.0
			);
		}
		assert!(ingress_requirement(MsgType(0xFF)).is_none());
		for request in ACTOR_AUTHORIZED_CONTROL_REQUESTS {
			assert!(
				(0x10..=0x1F).contains(&request.0) || is_control_extension_request(request.0),
				"actor-authorized request 0x{:02x} must route to Control Ring",
				request.0
			);
		}
	}

	#[test]
	fn operator_ingress_is_control_only_and_node_ingress_keeps_attribution() {
		let operator = operator_context();
		for msg_type in MsgType::ALL {
			assert_eq!(
				enforce_ingress(&operator, *msg_type).is_ok(),
				ACTOR_AUTHORIZED_CONTROL_REQUESTS.contains(msg_type),
				"operator ingress classification drifted for 0x{:02x}",
				msg_type.0
			);
		}
		for denied in [
			MsgType::WRITE_REQ,
			MsgType::READ_REQ,
			MsgType::QUERY_REQ,
			MsgType::APPEND_ENTRIES_REQ,
		] {
			let error = enforce_ingress(&operator, denied).unwrap_err();
			assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
			assert!(error.to_string().contains("authenticated data node"));
		}
		for allowed in [
			MsgType::STATUS_REQ,
			MsgType::TABLE_DESCRIPTOR_GET_REQ,
			MsgType::SCHEMA_UPDATE_REQ,
			MsgType::SECONDARY_INDEX_STATUS_REQ,
		] {
			enforce_ingress(&operator, allowed).unwrap();
		}

		let node = node_context(7);
		assert_eq!(node.require_authenticated_node("control").unwrap().id, 7);
		enforce_ingress(&node, MsgType::APPEND_ENTRIES_REQ).unwrap();
	}

	struct CountingControl(AtomicUsize);
	struct CountingData(AtomicUsize);

	#[async_trait]
	impl DataRingServer for CountingData {
		async fn handle_data_frame(
			&self,
			_context: RequestContext,
			_frame: HrftFrame,
		) -> std::io::Result<Option<HrftFrame>> {
			self.0.fetch_add(1, Ordering::Relaxed);
			Ok(None)
		}
	}

	#[async_trait]
	impl ControlRingServer for CountingControl {
		async fn handle_control_frame(
			&self,
			_context: RequestContext,
			_frame: HrftFrame,
		) -> std::io::Result<Option<HrftFrame>> {
			self.0.fetch_add(1, Ordering::Relaxed);
			Ok(None)
		}
	}

	#[tokio::test]
	async fn secondary_index_status_routes_to_control_ring() {
		let control = Arc::new(CountingControl(AtomicUsize::new(0)));
		let router = MsgRouterBuilder::default()
			.with_control_ring(control.clone())
			.build();
		router
			.handle_frame(
				operator_context(),
				frame(MsgType::SECONDARY_INDEX_STATUS_REQ),
			)
			.await
			.unwrap();
		assert_eq!(control.0.load(Ordering::Relaxed), 1);
	}

	/// ADR 0699 S3a: a forwarded decision reaches the control ring from an
	/// authenticated node, never from another actor, and its response is never
	/// dispatched as a request.
	#[tokio::test]
	async fn a_decision_proposal_routes_to_control_only_from_a_node() {
		let control = Arc::new(CountingControl(AtomicUsize::new(0)));
		let router = MsgRouterBuilder::default()
			.with_control_ring(control.clone())
			.build();
		router
			.handle_frame(node_context(2), frame(MsgType::DECISION_PROPOSE_REQ))
			.await
			.unwrap();
		assert_eq!(control.0.load(Ordering::Relaxed), 1);
		let error = router
			.handle_frame(operator_context(), frame(MsgType::DECISION_PROPOSE_REQ))
			.await
			.unwrap_err();
		assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
		assert!(
			router
				.handle_frame(node_context(2), frame(MsgType::DECISION_PROPOSE_RESP))
				.await
				.is_err()
		);
		assert_eq!(control.0.load(Ordering::Relaxed), 1);
	}

	#[tokio::test]
	async fn a_decision_lookup_routes_to_control_only_from_a_node() {
		let control = Arc::new(CountingControl(AtomicUsize::new(0)));
		let router = MsgRouterBuilder::default()
			.with_control_ring(control.clone())
			.build();
		router
			.handle_frame(node_context(3), frame(MsgType::DECISION_LOOKUP_REQ))
			.await
			.unwrap();
		assert_eq!(control.0.load(Ordering::Relaxed), 1);
		assert!(
			router
				.handle_frame(operator_context(), frame(MsgType::DECISION_LOOKUP_REQ))
				.await
				.is_err()
		);
		assert!(
			router
				.handle_frame(node_context(3), frame(MsgType::DECISION_LOOKUP_RESP))
				.await
				.is_err()
		);
		assert_eq!(control.0.load(Ordering::Relaxed), 1);
	}

	#[tokio::test]
	async fn a_decision_cut_routes_to_control_only_from_a_node() {
		let control = Arc::new(CountingControl(AtomicUsize::new(0)));
		let router = MsgRouterBuilder::default()
			.with_control_ring(control.clone())
			.build();
		router
			.handle_frame(node_context(3), frame(MsgType::DECISION_CUT_REQ))
			.await
			.unwrap();
		assert_eq!(control.0.load(Ordering::Relaxed), 1);
		for (context, msg_type) in [
			(operator_context(), MsgType::DECISION_CUT_REQ),
			(node_context(3), MsgType::DECISION_CUT_RESP),
		] {
			assert!(router.handle_frame(context, frame(msg_type)).await.is_err());
		}
		assert_eq!(control.0.load(Ordering::Relaxed), 1);
	}

	#[tokio::test]
	async fn mutation_outcome_resolution_routes_to_data_ring_and_stays_node_only() {
		let data = Arc::new(CountingData(AtomicUsize::new(0)));
		let router = MsgRouterBuilder::default()
			.with_data_ring(data.clone())
			.build();
		router
			.handle_frame(node_context(7), frame(MsgType::MUTATION_OUTCOME_REQ))
			.await
			.unwrap();
		assert_eq!(data.0.load(Ordering::Relaxed), 1);
		let error = router
			.handle_frame(operator_context(), frame(MsgType::MUTATION_OUTCOME_REQ))
			.await
			.unwrap_err();
		assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
	}
}
