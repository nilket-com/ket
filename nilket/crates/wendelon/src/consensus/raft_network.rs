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
//! Two layers keep a session to a peer. Each [`WendelonRaftNetwork`] caches one
//! `Arc<dyn TransportConnection>`, and the dial provider it is given in production, the
//! shared [`crate::network::factory::TransportProviderFactory`], keeps a session per peer of
//! its own. Both hand back a cached session until it reports `is_closed()`.
//!
//! ADR 0735: a session whose peer died without a word does not report itself closed until
//! its transport gives up on it, while openraft drops each call at its own budget. So a call
//! that ends without a complete, valid response marks the client's session suspect (the
//! mark is set before the call can first wait, and cleared only by a decoded response, so a
//! dropped call leaves it set), and the next call drops the suspect session here and asks
//! the provider to [`TransportProvider::discard`] exactly that session before asking it for
//! one. A valid response, a Raft error body included, keeps the session.
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
use crate::consensus::decision_mode::DecisionMode;
use crate::network::core::{HRFT_MAGIC, read_frame, write_frame};
use crate::network::pool::BufPool;
use crate::network::transport::{TransportConnection, TransportProvider};
use crate::network::wire;
use crate::schema::{FrameHeader, MsgType, NodeId};
use openraft::BasicNode;
use openraft::error::{
	InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
	AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
	VoteRequest, VoteResponse,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
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
	/// Raft frames carry this authority's envelope version.
	decision_mode: DecisionMode,
}

impl WendelonRaftNetworkFactory {
	pub fn new(provider: Arc<dyn TransportProvider>, pool: Arc<BufPool>) -> Self {
		Self {
			provider,
			pool,
			decision_mode: DecisionMode::Today,
		}
	}

	pub(crate) fn with_decision_mode(mut self, decision_mode: DecisionMode) -> Self {
		self.decision_mode = decision_mode;
		self
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
			suspect: false,
			decision_mode: self.decision_mode,
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
	/// ADR 0735: the last call did not end in a complete, valid response. Set before a call
	/// can first wait and cleared only by a decoded response, so a dropped call leaves it set.
	suspect: bool,
	decision_mode: DecisionMode,
}

impl WendelonRaftNetwork {
	/// Return a usable connection, dialing if the cache is empty or
	/// the previous session has closed. `Arc<dyn TransportConnection>`
	/// clones are cheap — returning one by value avoids tangling
	/// the caller in the borrow of `self.connection`.
	async fn ensure_connection(&mut self) -> std::io::Result<Arc<dyn TransportConnection>> {
		let target_id = NodeId {
			id: self.target,
			_reserved: 0,
		};
		// ADR 0735: a session the last call got no answer on is selected by neither layer.
		if self.suspect
			&& let Some(suspect) = self.connection.take()
		{
			self.provider.discard(target_id, &suspect);
		}
		// Set before anything below can wait: a call that is dropped, or fails, from here on
		// leaves it set for the next one.
		self.suspect = true;
		if let Some(conn) = &self.connection {
			if !conn.is_closed() {
				return Ok(conn.clone());
			}
		}
		let conn = self.provider.connect(target_id, &self.address).await?;
		self.connection = Some(conn.clone());
		Ok(conn)
	}

	/// One-shot request/response over a fresh bidi stream.
	///
	/// * Opens a new bidi stream on the cached connection.
	/// * Sends one HRFT frame carrying `postcard(req)` in `meta`.
	/// * Reads one HRFT frame in reply and decodes `meta` as `Resp`.
	async fn call<Req, Resp>(
		&mut self,
		msg_type: MsgType,
		expected: MsgType,
		req: &Req,
	) -> std::io::Result<Resp>
	where
		Req: Serialize,
		Resp: DeserializeOwned,
	{
		let conn = self.ensure_connection().await?;
		let mut stream = conn.open_bi().await?;

		let body = wire::encode_into(&self.pool, req)?;
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: self.decision_mode.raft_frame_version(),
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
		self.decision_mode.check_raft_response(
			resp_frame.header.version,
			resp_frame.header.msg_type,
			expected.0,
		)?;
		let response = wire::decode::<Resp>(&resp_frame.meta)?;
		// A complete, valid response: the session answers. A Raft error inside it is still
		// an answer.
		self.suspect = false;
		Ok(response)
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
			.call(
				MsgType::APPEND_ENTRIES_REQ,
				MsgType::APPEND_ENTRIES_RESP,
				&rpc,
			)
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
			self.call(
				MsgType::INSTALL_SNAPSHOT_REQ,
				MsgType::INSTALL_SNAPSHOT_RESP,
				&rpc,
			)
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
			.call(MsgType::VOTE_REQ, MsgType::VOTE_RESP, &rpc)
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

// ============================================================
//  TESTS
// ============================================================

/// DC-237: what the Raft client does after a call to a peer got no answer, through the cache
/// layers production has: this per-peer client over the real [`TransportProviderFactory`], over
/// a scripted lowest-level provider that counts its dials and says which session each stream
/// was opened on. No cluster, no QUIC, virtual time.
#[cfg(test)]
mod session_recovery_tests {
	use super::*;
	use crate::network::factory::TransportProviderFactory;
	use crate::network::tls::ActorIdentity;
	use crate::network::transport::{
		CapabilityResolver, EpochCapabilityResolver, MembershipEpoch, NodeCapabilitySet, NodeRole,
		NodeRoleSet, PeerCapabilities, TransportStream, TransportTier, TransportTierSet,
	};
	use async_trait::async_trait;
	use std::collections::HashMap;
	use std::pin::Pin;
	use std::sync::Mutex;
	use std::task::{Context, Poll};
	use std::time::Duration;
	use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};

	const PEER: u32 = 7;
	const ADDRESS: &str = "127.0.0.1:9001";
	const TIER: TransportTier = TransportTier::QuicIoUring;
	/// The budget openraft gives one AppendEntries call at the 250 ms profile.
	const CALL_BUDGET: Duration = Duration::from_millis(250);

	struct Stream(DuplexStream);

	impl AsyncRead for Stream {
		fn poll_read(
			mut self: Pin<&mut Self>,
			cx: &mut Context<'_>,
			buf: &mut ReadBuf<'_>,
		) -> Poll<std::io::Result<()>> {
			Pin::new(&mut self.0).poll_read(cx, buf)
		}
	}

	impl AsyncWrite for Stream {
		fn poll_write(
			mut self: Pin<&mut Self>,
			cx: &mut Context<'_>,
			buf: &[u8],
		) -> Poll<std::io::Result<usize>> {
			Pin::new(&mut self.0).poll_write(cx, buf)
		}
		fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
			Pin::new(&mut self.0).poll_flush(cx)
		}
		fn poll_shutdown(
			mut self: Pin<&mut Self>,
			cx: &mut Context<'_>,
		) -> Poll<std::io::Result<()>> {
			Pin::new(&mut self.0).poll_shutdown(cx)
		}
	}

	#[async_trait]
	impl TransportStream for Stream {
		fn stream_id(&self) -> u64 {
			0
		}
		async fn finish(&mut self) -> std::io::Result<()> {
			use tokio::io::AsyncWriteExt;
			self.0.shutdown().await
		}
	}

	/// What the scripted provider saw, shared with its sessions.
	#[derive(Default)]
	struct Seen {
		/// Dials that reached the LOWEST provider.
		dials: Mutex<usize>,
		/// The session each stream was opened on, in order.
		streams: Mutex<Vec<usize>>,
		/// The far ends of streams nobody answers, kept so that they stay open and silent.
		unanswered: Mutex<Vec<DuplexStream>>,
		/// Signalled when a stream that nobody answers has been opened.
		pending: tokio::sync::Notify,
	}

	/// What a session does with each stream opened on it. No session ever reports itself
	/// closed.
	#[derive(Clone, Copy, Debug, PartialEq, Eq)]
	enum Behaviour {
		/// One correctly framed AppendEntries success.
		Answers,
		/// One correctly framed response whose body is a Raft error: an answer all the same.
		AnswersWithARaftError,
		/// Nothing, ever, with the stream left open: a peer that died without a word.
		Silent,
		/// The stream cannot be opened: a transport error the call returns.
		FailsToOpen,
	}

	struct Session {
		id: usize,
		behaviour: Behaviour,
		seen: Arc<Seen>,
		pool: Arc<BufPool>,
		actor: ActorIdentity,
	}

	#[async_trait]
	impl TransportConnection for Session {
		async fn open_bi(&self) -> std::io::Result<Box<dyn TransportStream>> {
			self.seen.streams.lock().unwrap().push(self.id);
			if self.behaviour == Behaviour::FailsToOpen {
				return Err(std::io::Error::new(
					ErrorKind::ConnectionReset,
					"the session cannot open a stream",
				));
			}
			let (near, mut far) = tokio::io::duplex(64 * 1024);
			if self.behaviour != Behaviour::Silent {
				let raft_error = self.behaviour == Behaviour::AnswersWithARaftError;
				let pool = self.pool.clone();
				tokio::spawn(async move {
					let Ok(request) = read_frame(&mut far, &pool).await else {
						return;
					};
					assert_eq!(request.header.msg_type, MsgType::APPEND_ENTRIES_REQ.0);
					let answer: Result<AppendEntriesResponse<u32>, RaftError<u32>> = if raft_error {
						Err(RaftError::Fatal(openraft::error::Fatal::Stopped))
					} else {
						Ok(AppendEntriesResponse::Success)
					};
					let body = wire::encode_into(&pool, &answer).unwrap();
					let header = FrameHeader {
						magic: HRFT_MAGIC,
						version: DecisionMode::Today.raft_frame_version(),
						msg_type: MsgType::APPEND_ENTRIES_RESP.0,
						flags: 0,
						stream_id: 0,
						meta_len: body.len() as u32,
						arrow_len: 0,
						trailing_len: 0,
					};
					let _ = write_frame(&mut far, &header, &body, None, None).await;
				});
			} else {
				self.seen.unanswered.lock().unwrap().push(far);
				self.seen.pending.notify_one();
			}
			Ok(Box::new(Stream(near)))
		}
		async fn accept_bi(&self) -> std::io::Result<Box<dyn TransportStream>> {
			unreachable!("a dialled session accepts nothing")
		}
		fn remote_id(&self) -> Option<NodeId> {
			Some(NodeId {
				id: PEER,
				_reserved: 0,
			})
		}
		fn actor_identity(&self) -> &ActorIdentity {
			&self.actor
		}
		fn transport_type(&self) -> TransportTier {
			TIER
		}
		fn is_closed(&self) -> bool {
			false
		}
	}

	/// The lowest provider: session `n` (from 0) behaves as `script(n)`.
	struct Scripted {
		seen: Arc<Seen>,
		pool: Arc<BufPool>,
		script: fn(usize) -> Behaviour,
	}

	#[async_trait]
	impl TransportProvider for Scripted {
		async fn connect(
			&self,
			target: NodeId,
			_address: &str,
		) -> std::io::Result<Arc<dyn TransportConnection>> {
			let id = {
				let mut dials = self.seen.dials.lock().unwrap();
				*dials += 1;
				*dials - 1
			};
			Ok(Arc::new(Session {
				id,
				behaviour: (self.script)(id),
				seen: self.seen.clone(),
				pool: self.pool.clone(),
				actor: ActorIdentity::data_node(target),
			}))
		}

		fn withdraw_local(
			&self,
			_node_id: NodeId,
			_address: &str,
			_service: &Arc<dyn crate::network::server::HrftService>,
		) -> std::io::Result<()> {
			Ok(())
		}
	}

	/// This peer's Raft client as production builds it: over the real factory, over the
	/// scripted provider. The factory and the membership sender are returned to keep them alive.
	async fn client(
		script: fn(usize) -> Behaviour,
	) -> (
		WendelonRaftNetwork,
		Arc<Seen>,
		Arc<TransportProviderFactory>,
		tokio::sync::watch::Sender<Arc<MembershipEpoch>>,
	) {
		let seen = Arc::new(Seen::default());
		let pool = BufPool::new(64, 1024 * 1024);
		let mut peers = HashMap::new();
		peers.insert(
			NodeId {
				id: PEER,
				_reserved: 0,
			},
			PeerCapabilities {
				address: ADDRESS.into(),
				tiers: TransportTierSet::empty().with(TIER),
				roles: NodeRoleSet::empty().with(NodeRole::Data),
				capabilities: NodeCapabilitySet::empty(),
			},
		);
		let (tx, rx) = tokio::sync::watch::channel(Arc::new(MembershipEpoch { epoch: 1, peers }));
		let resolver: Arc<dyn CapabilityResolver> =
			Arc::new(EpochCapabilityResolver::new(rx.clone()));
		let mut providers: HashMap<TransportTier, Arc<dyn TransportProvider>> = HashMap::new();
		providers.insert(
			TIER,
			Arc::new(Scripted {
				seen: seen.clone(),
				pool: pool.clone(),
				script,
			}),
		);
		let factory = TransportProviderFactory::new(
			TransportTierSet::empty().with(TIER),
			providers,
			resolver,
			rx,
		);
		let dial: Arc<dyn TransportProvider> = factory.clone();
		let network = WendelonRaftNetworkFactory::new(dial, pool)
			.new_client(
				PEER,
				&BasicNode {
					addr: ADDRESS.into(),
				},
			)
			.await;
		(network, seen, factory, tx)
	}

	fn heartbeat() -> AppendEntriesRequest<WendelonTypeConfig> {
		AppendEntriesRequest {
			vote: openraft::Vote::new_committed(2, 3),
			prev_log_id: None,
			entries: Vec::new(),
			leader_commit: None,
		}
	}

	/// One AppendEntries under the caller's budget, as openraft makes it: `None` when the
	/// budget ran out and the call was dropped; otherwise whether it was answered with success.
	async fn call(network: &mut WendelonRaftNetwork) -> Option<bool> {
		outcome(network)
			.await
			.map(|answer| matches!(answer, Ok(AppendEntriesResponse::Success)))
	}

	async fn outcome(
		network: &mut WendelonRaftNetwork,
	) -> Option<Result<AppendEntriesResponse<u32>, RPCError<u32, BasicNode, RaftError<u32>>>> {
		tokio::time::timeout(
			CALL_BUDGET,
			network.append_entries(heartbeat(), RPCOption::new(CALL_BUDGET)),
		)
		.await
		.ok()
	}

	/// The control: a session that answers is reused. Three calls, one dial at the lowest
	/// provider, every stream on that one session.
	#[tokio::test(start_paused = true)]
	async fn a_session_that_answers_is_reused_across_calls() {
		let (mut network, seen, _factory, _membership) = client(|_| Behaviour::Answers).await;
		for _ in 0..3 {
			assert_eq!(call(&mut network).await, Some(true));
		}
		assert_eq!(*seen.dials.lock().unwrap(), 1);
		assert_eq!(*seen.streams.lock().unwrap(), [0, 0, 0]);
	}

	/// The case: the first session is not closed and answers nothing (its peer died); a new
	/// dial would give a session that answers. The first call is seen to be waiting on the
	/// silent session and is dropped at its budget. The NEXT call must reach a new session at
	/// the lowest provider and be answered: the client does not go on talking into a session
	/// that left its last call unanswered.
	#[tokio::test(start_paused = true)]
	async fn the_call_after_an_unanswered_one_reaches_a_new_session() {
		let (mut network, seen, _factory, _membership) = client(|session| {
			if session == 0 {
				Behaviour::Silent
			} else {
				Behaviour::Answers
			}
		})
		.await;
		{
			let first = call(&mut network);
			tokio::pin!(first);
			// Synchronise on the silent stream being open before judging the cancellation.
			tokio::select! {
				biased;
				_ = seen.pending.notified() => {}
				answer = &mut first => panic!("the first call ended before its stream was held: {answer:?}"),
			}
			assert_eq!(first.await, None, "the silent session answered");
		}
		assert_eq!(*seen.dials.lock().unwrap(), 1);
		assert_eq!(*seen.streams.lock().unwrap(), [0]);
		// The silent session is still there and still not closed; nothing made it go away.
		assert_eq!(seen.unanswered.lock().unwrap().len(), 1);

		let second = call(&mut network).await;
		let (dials, streams) = (
			*seen.dials.lock().unwrap(),
			seen.streams.lock().unwrap().clone(),
		);
		assert_eq!(
			(second, dials, streams.as_slice()),
			(Some(true), 2, &[0, 1][..]),
			"after an unanswered call the next one did not reach a new session: answered {second:?}, lowest-provider dials {dials}, sessions used {streams:?}"
		);
	}

	/// A call that returns a transport error (the session cannot open a stream) is followed
	/// by a call on a new session.
	#[tokio::test(start_paused = true)]
	async fn a_call_that_failed_on_its_session_is_followed_by_a_call_on_a_new_one() {
		let (mut network, seen, _factory, _membership) = client(|session| {
			if session == 0 {
				Behaviour::FailsToOpen
			} else {
				Behaviour::Answers
			}
		})
		.await;
		assert!(matches!(
			outcome(&mut network).await,
			Some(Err(RPCError::Unreachable(_)))
		));
		assert_eq!(call(&mut network).await, Some(true));
		assert_eq!(*seen.dials.lock().unwrap(), 2);
		assert_eq!(*seen.streams.lock().unwrap(), [0, 1]);
	}

	/// A valid response whose body is a Raft error is an answer: the session is kept, the
	/// next call uses it, and nothing is dialled again. The error reaches openraft as the
	/// peer's own (RemoteError), as before.
	#[tokio::test(start_paused = true)]
	async fn a_raft_error_answer_keeps_the_session() {
		let (mut network, seen, _factory, _membership) =
			client(|_| Behaviour::AnswersWithARaftError).await;
		for _ in 0..3 {
			assert!(matches!(
				outcome(&mut network).await,
				Some(Err(RPCError::RemoteError(_)))
			));
		}
		assert_eq!(*seen.dials.lock().unwrap(), 1);
		assert_eq!(*seen.streams.lock().unwrap(), [0, 0, 0]);
	}

	/// The factory discards only the very session it is given: a stale session leaves a
	/// newer cached one in place, and the next request for the peer gets that newer one with
	/// no further dial; the cached session itself is removed, and the next request dials.
	#[tokio::test(start_paused = true)]
	async fn the_factory_discards_exactly_the_session_it_is_given() {
		let (_network, seen, factory, _membership) = client(|_| Behaviour::Answers).await;
		let peer = NodeId {
			id: PEER,
			_reserved: 0,
		};
		let first = factory.connect(peer, ADDRESS).await.unwrap();
		assert_eq!(*seen.dials.lock().unwrap(), 1);
		factory.discard(peer, &first);
		let second = factory.connect(peer, ADDRESS).await.unwrap();
		assert_eq!(*seen.dials.lock().unwrap(), 2);
		assert!(!Arc::ptr_eq(&first, &second));
		// A caller still holding the first, now stale, session cannot remove the second.
		factory.discard(peer, &first);
		let again = factory.connect(peer, ADDRESS).await.unwrap();
		assert!(Arc::ptr_eq(&again, &second));
		assert_eq!(*seen.dials.lock().unwrap(), 2, "no further dial");
		// Nothing is cached for another peer, and discarding there changes nothing.
		factory.discard(
			NodeId {
				id: PEER + 1,
				_reserved: 0,
			},
			&second,
		);
		assert!(Arc::ptr_eq(
			&factory.connect(peer, ADDRESS).await.unwrap(),
			&second
		));
	}

	/// After a stale discard the Raft client's next call reuses the newer session another
	/// caller installed, with no third dial.
	#[tokio::test(start_paused = true)]
	async fn after_a_stale_discard_the_client_reuses_the_newer_session() {
		let (mut network, seen, factory, _membership) = client(|session| {
			if session == 0 {
				Behaviour::Silent
			} else {
				Behaviour::Answers
			}
		})
		.await;
		let peer = NodeId {
			id: PEER,
			_reserved: 0,
		};
		{
			let first = call(&mut network);
			tokio::pin!(first);
			tokio::select! {
				biased;
				_ = seen.pending.notified() => {}
				answer = &mut first => panic!("ended before its stream was held: {answer:?}"),
			}
			assert_eq!(first.await, None);
		}
		// Another caller of the factory has meanwhile found session 0 wanting and installed
		// session 1.
		let stale = factory.connect(peer, ADDRESS).await.unwrap();
		factory.discard(peer, &stale);
		let newer = factory.connect(peer, ADDRESS).await.unwrap();
		assert_eq!(*seen.dials.lock().unwrap(), 2);
		// The client's own suspect session 0 is discarded at the factory, which no longer
		// holds it, and the call is made on the newer session 1.
		assert_eq!(call(&mut network).await, Some(true));
		assert_eq!(*seen.dials.lock().unwrap(), 2, "no third dial");
		assert_eq!(*seen.streams.lock().unwrap(), [0, 1]);
		assert!(Arc::ptr_eq(
			&factory.connect(peer, ADDRESS).await.unwrap(),
			&newer
		));
	}
}
