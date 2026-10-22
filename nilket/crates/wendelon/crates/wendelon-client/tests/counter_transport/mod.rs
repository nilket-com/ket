//! An in-process transport for SDK counter-add tests. Every stream records the
//! request bytes the client writes; on `finish` it decodes the request and asks
//! the responder for a `COUNTER_ADD_RESP`. No responder (or a `None` answer)
//! closes the stream instead, so the add fails after its request is on the wire.
//! Each test crate uses part of it.
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
use wendelon::schema::{
	ClientWriteId, CounterAddRequestMeta, CounterAddResponseMeta, FrameHeader, MsgType, NodeId,
};

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub type Responder =
	Arc<dyn Fn(&CounterAddRequestMeta) -> Option<CounterAddResponseMeta> + Send + Sync>;

/// What the transport saw and how it answers.
#[derive(Clone, Default)]
pub struct CounterWire {
	pub requests: Arc<Mutex<Vec<CounterAddRequestMeta>>>,
	responder: Option<Responder>,
}

impl CounterWire {
	pub fn answering(responder: Responder) -> Self {
		Self {
			requests: Arc::default(),
			responder: Some(responder),
		}
	}

	pub fn requests(&self) -> Vec<CounterAddRequestMeta> {
		self.requests.lock().unwrap().clone()
	}
}

/// A response to `request` with the given status and values, everything else zero.
pub fn response(
	request: &CounterAddRequestMeta,
	status: wendelon::schema::WriteStatus,
	previous_value: u64,
	current_value: u64,
) -> CounterAddResponseMeta {
	let mut response: CounterAddResponseMeta = bytemuck::Zeroable::zeroed();
	response.status = status;
	response.echoed_write_id = request.write_id;
	response.previous_value = previous_value;
	response.current_value = current_value;
	response
}

/// The write id a captured request carried.
pub fn write_id(request: &CounterAddRequestMeta) -> ClientWriteId {
	request.write_id
}

struct WireStream {
	written: Vec<u8>,
	reply: Vec<u8>,
	read_at: usize,
	wire: CounterWire,
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
			assert_eq!(frame.header.msg_type, MsgType::COUNTER_ADD_REQ.0);
			let meta: &[u8] = &frame.meta;
			let request: CounterAddRequestMeta =
				*bytemuck::from_bytes(&meta[..std::mem::size_of::<CounterAddRequestMeta>()]);
			self.wire.requests.lock().unwrap().push(request);
			let answer = self
				.wire
				.responder
				.as_ref()
				.and_then(|responder| responder(&request));
			if let Some(answer) = answer {
				let meta = bytemuck::bytes_of(&answer);
				let header = FrameHeader {
					magic: HRFT_MAGIC,
					version: 1,
					msg_type: MsgType::COUNTER_ADD_RESP.0,
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
	wire: CounterWire,
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
	wire: CounterWire,
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
pub fn inner_client(wire: &CounterWire) -> Arc<WendelonClient> {
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
