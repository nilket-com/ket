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
//!   0x20..=0x2F  →  Raft internal
//!   0x30..=0x3F  →  Control/admin extensions
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

/// Handler for Data Ring frames: `msg_type` in `0x01..=0x0F`.
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
				write_frame(
					&mut *stream,
					&resp.header,
					&resp.meta,
					resp.arrow_payload.as_deref(),
					resp.trailing.as_deref(),
				)
				.await?;
				stream.finish().await?;
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
		match msg_type {
			x if x == MsgType::PENDING_WITNESS_REQ.0 => match &self.data {
				Some(h) => h.handle_data_frame(context, frame).await,
				None => Err(unhandled("Data Ring", msg_type)),
			},
			0x01..=0x0F => match &self.data {
				Some(h) => h.handle_data_frame(context, frame).await,
				None => Err(unhandled("Data Ring", msg_type)),
			},
			0x10..=0x1F => match &self.control {
				Some(h) => h.handle_control_frame(context, frame).await,
				None => Err(unhandled("Control Ring", msg_type)),
			},
			x if x == MsgType::REVOCATION_ADMIN_REQ.0 => match &self.control {
				Some(h) => h.handle_control_frame(context, frame).await,
				None => Err(unhandled("Control Ring", msg_type)),
			},
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
		if header.msg_type == MsgType::BULK_WRITE_REQ.0
			|| header.msg_type == MsgType::STREAM_FIN_REQ.0
			|| header.msg_type == MsgType::QUERY_REQ.0
		{
			return match &self.data {
				Some(h) => h.handle_data_stream(context, header, stream, pool).await,
				None => Err(unhandled("Data Ring", header.msg_type)),
			};
		}

		let frame = read_frame_from_header(&mut *stream, &pool, header).await?;
		match self.handle_frame(context, frame).await? {
			Some(resp) => {
				write_frame(
					&mut *stream,
					&resp.header,
					&resp.meta,
					resp.arrow_payload.as_deref(),
					resp.trailing.as_deref(),
				)
				.await?;
				stream.finish().await?;
			}
			None => {
				stream.finish().await?;
			}
		}
		Ok(())
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
