//! openraft [`RaftNetwork`] / [`RaftNetworkFactory`] over the
//! transport abstraction from ADR 011.
//!
//! Slice 1 introduced the transport traits; Slice 2 routes the
//! Control Ring's Raft RPCs through them. The HRFT frame codec,
//! the one-frame-per-bidi-stream wire shape, and the
//! `Result<Resp, RaftError>` response envelope are all unchanged
//! — what moved is *who* the client talks to: instead of a
//! hard-coded `quinn::Endpoint`, the factory now holds an
//! `Arc<dyn TransportProvider>` and every dial goes through it.
//!
//! ## Wire shape
//!
//! Every RPC rides one bidi stream:
//!
//! ```text
//!   client → server:   FrameHeader { msg_type = *_REQ }  ||  postcard(Req)
//!   server → client:   FrameHeader { msg_type = *_RESP } ||  postcard(Resp)
//! ```
//!
//! Request and response bodies are serde-encoded via
//! [`crate::network::wire`]. The header itself is the fixed 24-byte
//! `#[repr(C)] + bytemuck::Pod` struct written as raw bytes.
//!
//! ## Connection caching
//!
//! Each [`WendelonRaftNetwork`] caches one `Arc<dyn
//! TransportConnection>`. The first RPC dials lazily through the
//! provider; subsequent RPCs reuse the cached connection until it
//! reports `is_closed()`, at which point the next RPC redials. The
//! trait object is `Arc`-backed so cloning to escape the borrow of
//! `self.connection` is free.
//!
//! Slice 3 will lift caching out of this struct into a shared
//! factory keyed by `NodeId` so that client-initiated Data-Ring
//! streams share the same connection pool as Raft; for now the
//! per-peer cache here is the whole story.
//!
//! ## Error mapping
//!
//! Two distinct error classes travel different paths:
//!
//! * **Transport-level failures** — parse, dial, stream open, send,
//!   recv, decode — collapse to [`RPCError::Unreachable`]. openraft
//!   treats `Unreachable` as a transient condition (backoff + retry)
//!   rather than a state-machine-fatal error, which matches reality:
//!   the peer may come back, TLS may resume, a transient blip should
//!   not poison the log.
//! * **Logical peer errors** — a `RaftError` returned from the remote
//!   `openraft::Raft` handle — ride in the successful response body
//!   as `Result<Resp, RaftError>` and surface client-side as
//!   [`RPCError::RemoteError`], preserving the source error so
//!   `ForwardToLeader` hints and fatal-shutdown signals propagate.

use crate::consensus::WendelonTypeConfig;
use crate::network::core::{read_frame, write_frame, HRFT_MAGIC};
use crate::network::pool::BufPool;
use crate::network::transport::{TransportConnection, TransportProvider};
use crate::network::wire;
use crate::schema::{FrameHeader, MsgType, NodeId};
use openraft::error::{
	InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
	AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
	VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::io::{Error, ErrorKind};
use std::sync::Arc;

// ============================================================
//  FACTORY
// ============================================================

/// Produces one [`WendelonRaftNetwork`] per Raft peer.
///
/// The provider is injected rather than constructed here so tests
/// can hand in an in-process `TransportProvider` without spinning
/// up a real QUIC endpoint.
pub struct WendelonRaftNetworkFactory {
	/// Dial side of the transport. Today always
	/// [`crate::network::quic_transport::QuicTransportProvider`];
	/// Slice 3 will wrap it in a tier-negotiating adapter.
	provider: Arc<dyn TransportProvider>,
	/// Free list recycled across every peer RPC: postcard encode
	/// outputs and inbound frame sections both draw from and return
	/// to this pool.
	pool: Arc<BufPool>,
}

impl WendelonRaftNetworkFactory {
	pub fn new(provider: Arc<dyn TransportProvider>, pool: Arc<BufPool>) -> Self {
		Self { provider, pool }
	}
}

impl RaftNetworkFactory<WendelonTypeConfig> for WendelonRaftNetworkFactory {
	type Network = WendelonRaftNetwork;

	async fn new_client(&mut self, target: u32, node: &BasicNode) -> Self::Network {
		WendelonRaftNetwork {
			target,
			address: node.addr.clone(),
			provider: self.provider.clone(),
			pool: self.pool.clone(),
			connection: None,
		}
	}
}

// ============================================================
//  PER-PEER NETWORK
// ============================================================

/// Single-peer network handle with a lazily-dialed transport
/// connection. The concrete transport (QUIC today, RDMA or TCP in
/// the future) is invisible to this struct — everything goes
/// through the trait object.
pub struct WendelonRaftNetwork {
	/// openraft peer id — used to attribute logical errors via
	/// [`RemoteError::new`] when the peer responds with a `RaftError`,
	/// and also the `NodeId.id` the provider dials against.
	target: u32,
	/// Peer address supplied by openraft's `BasicNode`. The
	/// provider parses this; storing the raw string avoids
	/// committing to a specific address family at this layer.
	address: String,
	provider: Arc<dyn TransportProvider>,
	/// Shared buffer pool — postcard encode outputs and inbound
	/// frame sections recycle through it so the per-peer loop never
	/// allocates at steady state.
	pool: Arc<BufPool>,
	/// `None` until the first successful dial; cleared implicitly
	/// via [`Self::ensure_connection`] whenever the cached
	/// connection reports `is_closed()`.
	connection: Option<Arc<dyn TransportConnection>>,
}

impl WendelonRaftNetwork {
	/// Return a usable connection, dialing if the cache is empty or
	/// the previous session has closed. `Arc<dyn TransportConnection>`
	/// clones are cheap — returning one by value avoids tangling
	/// the caller in the borrow of `self.connection`.
	async fn ensure_connection(&mut self) -> std::io::Result<Arc<dyn TransportConnection>> {
		if let Some(conn) = &self.connection {
			if !conn.is_closed() {
				return Ok(conn.clone());
			}
		}
		let target_id = NodeId {
			id: self.target,
			_reserved: 0,
		};
		let conn = self.provider.connect(target_id, &self.address).await?;
		self.connection = Some(conn.clone());
		Ok(conn)
	}

	/// One-shot request/response over a fresh bidi stream.
	///
	/// * Opens a new bidi stream on the cached connection.
	/// * Sends one HRFT frame carrying `postcard(req)` in `meta`.
	/// * Reads one HRFT frame in reply and decodes `meta` as `Resp`.
	async fn call<Req, Resp>(&mut self, msg_type: MsgType, req: &Req) -> std::io::Result<Resp>
	where
		Req: Serialize,
		Resp: DeserializeOwned,
	{
		let conn = self.ensure_connection().await?;
		let mut stream = conn.open_bi().await?;

		let body = wire::encode_into(&self.pool, req)?;
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: msg_type.0,
			flags: 0,
			stream_id: 0,
			meta_len: body.len() as u32,
			arrow_len: 0,
			trailing_len: 0,
		};
		write_frame(&mut *stream, &header, &body, None, None).await?;
		stream
			.finish()
			.await
			.map_err(|e| Error::new(ErrorKind::BrokenPipe, e))?;
		// Return the encode buffer to the pool before blocking on
		// the reply — the server's latency window is where another
		// peer task could most benefit from reusing it.
		drop(body);

		let resp_frame = read_frame(&mut *stream, &self.pool).await?;
		wire::decode::<Resp>(&resp_frame.meta)
	}
}

impl RaftNetwork<WendelonTypeConfig> for WendelonRaftNetwork {
	async fn append_entries(
		&mut self,
		rpc: AppendEntriesRequest<WendelonTypeConfig>,
		_option: RPCOption,
	) -> Result<AppendEntriesResponse<u32>, RPCError<u32, BasicNode, RaftError<u32>>> {
		let target = self.target;
		let envelope: Result<AppendEntriesResponse<u32>, RaftError<u32>> = self
			.call(MsgType::APPEND_ENTRIES_REQ, &rpc)
			.await
			.map_err(unreachable)?;
		envelope.map_err(|e| RPCError::RemoteError(RemoteError::new(target, e)))
	}

	async fn install_snapshot(
		&mut self,
		rpc: InstallSnapshotRequest<WendelonTypeConfig>,
		_option: RPCOption,
	) -> Result<
		InstallSnapshotResponse<u32>,
		RPCError<u32, BasicNode, RaftError<u32, InstallSnapshotError>>,
	> {
		let target = self.target;
		let envelope: Result<InstallSnapshotResponse<u32>, RaftError<u32, InstallSnapshotError>> =
			self.call(MsgType::INSTALL_SNAPSHOT_REQ, &rpc)
				.await
				.map_err(unreachable)?;
		envelope.map_err(|e| RPCError::RemoteError(RemoteError::new(target, e)))
	}

	async fn vote(
		&mut self,
		rpc: VoteRequest<u32>,
		_option: RPCOption,
	) -> Result<VoteResponse<u32>, RPCError<u32, BasicNode, RaftError<u32>>> {
		let target = self.target;
		let envelope: Result<VoteResponse<u32>, RaftError<u32>> = self
			.call(MsgType::VOTE_REQ, &rpc)
			.await
			.map_err(unreachable)?;
		envelope.map_err(|e| RPCError::RemoteError(RemoteError::new(target, e)))
	}
}

// ============================================================
//  ERROR MAPPING
// ============================================================

/// Collapse any `std::io::Error` into an `RPCError::Unreachable`
/// generic over the three different `RaftError<…>` shapes the trait
/// uses. `NetworkError::new` takes `&impl std::error::Error`, which
/// `io::Error` satisfies.
fn unreachable<E>(e: std::io::Error) -> RPCError<u32, BasicNode, E>
where
	E: std::error::Error,
{
	// `Unreachable` is the openraft signal for a transient
	// transport problem — retryable without tearing down the
	// state machine. Any hard I/O failure here qualifies.
	let _ = NetworkError::new(&e); // kept for parity with other error paths
	RPCError::Unreachable(Unreachable::new(&e))
}
