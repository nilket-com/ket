//! An in-process transport for SDK table-mutation tests. Every stream records the
//! request bytes the client writes; on `finish` it decodes the frame and asks the
//! responder, with the request's message type and metadata bytes, for the response's
//! message type and metadata bytes. A `None` answer closes the stream instead. No
//! server code runs: the responder chooses every byte of the answer.
#![allow(dead_code)]

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use wendelon::client::{
	ClusterPeerWire, ClusterStatusWire, StaticTopologySource, WendelonClient, WendelonClientConfig,
};
use wendelon::network::core::{HRFT_MAGIC, read_frame, write_frame};
use wendelon::network::pool::BufPool;
use wendelon::network::tls::ActorIdentity;
use wendelon::network::transport::{
	NodeRole, NodeRoleSet, TransportConnection, TransportProvider, TransportStream, TransportTier,
	TransportTierSet,
};
use wendelon::schema::{FrameHeader, NodeId};

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Request message type and metadata in; response message type and metadata out.
pub type Responder = Arc<dyn Fn(u8, &[u8]) -> Option<(u8, Vec<u8>)> + Send + Sync>;

/// What the transport saw and how it answers.
#[derive(Clone, Default)]
pub struct MutationWire {
	pub requests: Arc<Mutex<Vec<(u8, Vec<u8>)>>>,
	responder: Option<Responder>,
}

impl MutationWire {
	pub fn answering(responder: Responder) -> Self {
		Self {
			requests: Arc::default(),
			responder: Some(responder),
		}
	}

	pub fn requests(&self) -> Vec<(u8, Vec<u8>)> {
		self.requests.lock().unwrap().clone()
	}
}

struct WireStream {
	written: Vec<u8>,
	reply: Vec<u8>,
	read_at: usize,
	wire: MutationWire,
}

impl AsyncRead for WireStream {
	fn poll_read(
		mut self: Pin<&mut Self>,
		_cx: &mut Context<'_>,
		buf: &mut ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		let at = self.read_at;
		let n = buf.remaining().min(self.reply.len() - at);
		buf.put_slice(&self.reply[at..at + n]);
		self.read_at += n;
		Poll::Ready(Ok(()))
	}
}

impl AsyncWrite for WireStream {
	fn poll_write(
		mut self: Pin<&mut Self>,
		_cx: &mut Context<'_>,
		buf: &[u8],
	) -> Poll<std::io::Result<usize>> {
		self.written.extend_from_slice(buf);
		Poll::Ready(Ok(buf.len()))
	}

	fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
		Poll::Ready(Ok(()))
	}

	fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
		Poll::Ready(Ok(()))
	}
}

impl TransportStream for WireStream {
	fn stream_id(&self) -> u64 {
		0
	}

	fn finish<'life0, 'async_trait>(
		&'life0 mut self,
	) -> BoxFuture<'async_trait, std::io::Result<()>>
	where
		'life0: 'async_trait,
		Self: 'async_trait,
	{
		Box::pin(async move {
			let bytes = std::mem::take(&mut self.written);
			let pool = BufPool::new(4, 64 * 1024);
			let frame = read_frame(&mut bytes.as_slice(), &pool).await?;
			let request = (frame.header.msg_type, frame.meta.to_vec());
			self.wire.requests.lock().unwrap().push(request.clone());
			let answer = self
				.wire
				.responder
				.as_ref()
				.and_then(|responder| responder(request.0, &request.1));
			if let Some((msg_type, meta)) = answer {
				let meta = meta.as_slice();
				let header = FrameHeader {
					magic: HRFT_MAGIC,
					version: 1,
					msg_type,
					flags: 0,
					stream_id: 0,
					meta_len: meta.len() as u32,
					arrow_len: 0,
					trailing_len: 0,
				};
				let mut reply = Vec::new();
				write_frame(&mut reply, &header, meta, None, None).await?;
				self.reply = reply;
			}
			Ok(())
		})
	}
}

struct WireConnection {
	remote: NodeId,
	actor: ActorIdentity,
	wire: MutationWire,
}

impl TransportConnection for WireConnection {
	fn open_bi<'life0, 'async_trait>(
		&'life0 self,
	) -> BoxFuture<'async_trait, std::io::Result<Box<dyn TransportStream>>>
	where
		'life0: 'async_trait,
		Self: 'async_trait,
	{
		let stream: Box<dyn TransportStream> = Box::new(WireStream {
			written: Vec::new(),
			reply: Vec::new(),
			read_at: 0,
			wire: self.wire.clone(),
		});
		Box::pin(async move { Ok(stream) })
	}

	fn accept_bi<'life0, 'async_trait>(
		&'life0 self,
	) -> BoxFuture<'async_trait, std::io::Result<Box<dyn TransportStream>>>
	where
		'life0: 'async_trait,
		Self: 'async_trait,
	{
		unimplemented!("the client never accepts inbound streams")
	}

	fn remote_id(&self) -> Option<NodeId> {
		Some(self.remote)
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

struct WireProvider {
	wire: MutationWire,
}

impl TransportProvider for WireProvider {
	fn withdraw_local(
		&self,
		_node_id: wendelon::schema::NodeId,
		_address: &str,
		_service: &std::sync::Arc<dyn wendelon::network::server::HrftService>,
	) -> std::io::Result<()> {
		// This provider does not retain publish_local registrations.
		Ok(())
	}

	fn connect<'life0, 'life1, 'async_trait>(
		&'life0 self,
		target: NodeId,
		_address: &'life1 str,
	) -> BoxFuture<'async_trait, std::io::Result<Arc<dyn TransportConnection>>>
	where
		'life0: 'async_trait,
		'life1: 'async_trait,
		Self: 'async_trait,
	{
		let connection: Arc<dyn TransportConnection> = Arc::new(WireConnection {
			remote: target,
			actor: ActorIdentity::data_node(target),
			wire: self.wire.clone(),
		});
		Box::pin(async move { Ok(connection) })
	}
}

/// A fresh inner client over `wire`: one process's SDK connection.
pub fn inner_client(wire: &MutationWire) -> Arc<WendelonClient> {
	let tiers = TransportTierSet::empty().with(TransportTier::QuicIoUring);
	let snapshot = ClusterStatusWire {
		epoch: 1,
		leader: Some(1),
		replication_factor: 1,
		peers: vec![ClusterPeerWire {
			node_id: 1,
			address: "127.0.0.1:9001".to_string(),
			tier_bits: tiers.as_u8(),
			role_bits: NodeRoleSet::empty().with(NodeRole::Data).as_u8(),
			capability_bits: 0,
		}],
	};
	let mut providers: HashMap<TransportTier, Arc<dyn TransportProvider>> = HashMap::new();
	providers.insert(
		TransportTier::QuicIoUring,
		Arc::new(WireProvider { wire: wire.clone() }),
	);
	WendelonClient::new(WendelonClientConfig {
		local_tiers: tiers,
		providers,
		bootstrap: snapshot.clone(),
		topology: Arc::new(StaticTopologySource::new(snapshot)),
		pool_max_retained: 4,
		pool_max_cap_bytes: 64 * 1024,
	})
}
