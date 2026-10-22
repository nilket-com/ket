use crate::network::pool::BufPool;
use crate::network::server::{HrftService, RequestContext};
use crate::network::tls::ActorIdentity;
use crate::network::transport::{
	TransportConnection, TransportProvider, TransportStream, TransportTier,
};
use crate::schema::NodeId;
use async_trait::async_trait;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Error, ErrorKind};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SimulatedTransportFaultAction {
	Delay(Duration),
	Error { kind: ErrorKind, message: String },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SimulatedTransportFaultOp {
	Connect,
	OpenBi,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InjectedSimulatedTransportFault {
	op: SimulatedTransportFaultOp,
	source: Option<NodeId>,
	target: Option<NodeId>,
	after_matches: usize,
	action: SimulatedTransportFaultAction,
}

impl InjectedSimulatedTransportFault {
	pub fn on(op: SimulatedTransportFaultOp, action: SimulatedTransportFaultAction) -> Self {
		Self {
			op,
			source: None,
			target: None,
			after_matches: 0,
			action,
		}
	}

	pub fn from(mut self, source: NodeId) -> Self {
		self.source = Some(source);
		self
	}

	pub fn to(mut self, target: NodeId) -> Self {
		self.target = Some(target);
		self
	}

	pub fn after_matches(mut self, count: usize) -> Self {
		self.after_matches = count;
		self
	}

	fn matches(&self, op: SimulatedTransportFaultOp, source: NodeId, target: NodeId) -> bool {
		self.op == op
			&& self
				.source
				.map(|candidate| candidate == source)
				.unwrap_or(true)
			&& self
				.target
				.map(|candidate| candidate == target)
				.unwrap_or(true)
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SimulatedTransportEventKind {
	Published {
		address: String,
	},
	Unpublished,
	RouteBlocked,
	RouteHealed,
	Connect,
	OpenBi,
	Fault {
		op: SimulatedTransportFaultOp,
		action: SimulatedTransportFaultAction,
	},
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimulatedTransportEvent {
	pub step: u64,
	pub source: Option<NodeId>,
	pub target: Option<NodeId>,
	pub kind: SimulatedTransportEventKind,
}

struct PublishedSimulatedService {
	address: String,
	service: Arc<dyn HrftService>,
}

#[derive(Default)]
pub struct SimulatedTransportNetwork {
	services: Mutex<HashMap<NodeId, PublishedSimulatedService>>,
	blocked_routes: Mutex<HashSet<(NodeId, NodeId)>>,
	faults: Mutex<VecDeque<InjectedSimulatedTransportFault>>,
	events: Mutex<Vec<SimulatedTransportEvent>>,
	next_step: AtomicU64,
}

impl SimulatedTransportNetwork {
	pub fn block_route(&self, source: NodeId, target: NodeId) {
		self.blocked_routes.lock().unwrap().insert((source, target));
		self.record_event(
			Some(source),
			Some(target),
			SimulatedTransportEventKind::RouteBlocked,
		);
	}

	pub fn heal_route(&self, source: NodeId, target: NodeId) {
		if self
			.blocked_routes
			.lock()
			.unwrap()
			.remove(&(source, target))
		{
			self.record_event(
				Some(source),
				Some(target),
				SimulatedTransportEventKind::RouteHealed,
			);
		}
	}

	pub fn block_bidirectional(&self, left: NodeId, right: NodeId) {
		self.block_route(left, right);
		self.block_route(right, left);
	}

	pub fn heal_bidirectional(&self, left: NodeId, right: NodeId) {
		self.heal_route(left, right);
		self.heal_route(right, left);
	}

	pub fn inject(&self, fault: InjectedSimulatedTransportFault) {
		self.faults.lock().unwrap().push_back(fault);
	}

	pub fn events(&self) -> Vec<SimulatedTransportEvent> {
		self.events.lock().unwrap().clone()
	}

	pub fn clear_events(&self) {
		self.events.lock().unwrap().clear();
	}

	pub fn unpublish(&self, node_id: NodeId) {
		self.services.lock().unwrap().remove(&node_id);
		self.record_event(
			None,
			Some(node_id),
			SimulatedTransportEventKind::Unpublished,
		);
	}

	pub fn publish(&self, node_id: NodeId, address: &str, service: Arc<dyn HrftService>) {
		self.services.lock().unwrap().insert(
			node_id,
			PublishedSimulatedService {
				address: address.to_string(),
				service,
			},
		);
		self.record_event(
			None,
			Some(node_id),
			SimulatedTransportEventKind::Published {
				address: address.to_string(),
			},
		);
	}

	fn route_is_blocked(&self, source: NodeId, target: NodeId) -> bool {
		self.blocked_routes
			.lock()
			.unwrap()
			.contains(&(source, target))
	}

	fn resolve(
		&self,
		target: NodeId,
		expected_address: &str,
	) -> std::io::Result<Arc<dyn HrftService>> {
		let guard = self.services.lock().unwrap();
		let published = guard.get(&target).ok_or_else(|| {
			Error::new(
				ErrorKind::NotFound,
				format!("simulated transport target {} is not published", target.id),
			)
		})?;
		if published.address != expected_address {
			return Err(Error::new(
				ErrorKind::AddrNotAvailable,
				format!(
					"simulated transport target {} expected address {expected_address}, published {}",
					target.id, published.address
				),
			));
		}
		Ok(published.service.clone())
	}

	fn contains(&self, target: NodeId, expected_address: &str) -> bool {
		self.services
			.lock()
			.unwrap()
			.get(&target)
			.map(|published| published.address == expected_address)
			.unwrap_or(false)
	}

	async fn apply_fault(
		&self,
		op: SimulatedTransportFaultOp,
		source: NodeId,
		target: NodeId,
	) -> std::io::Result<()> {
		let action = {
			let mut faults = self.faults.lock().unwrap();
			let mut matched_action = None;
			for index in 0..faults.len() {
				if !faults[index].matches(op, source, target) {
					continue;
				}
				if faults[index].after_matches > 0 {
					faults[index].after_matches -= 1;
					break;
				}
				matched_action = Some(faults.remove(index).unwrap().action);
				break;
			}
			matched_action
		};

		match action {
			Some(action) => {
				self.record_event(
					Some(source),
					Some(target),
					SimulatedTransportEventKind::Fault {
						op,
						action: action.clone(),
					},
				);
				match action {
					SimulatedTransportFaultAction::Delay(delay) => {
						tokio::time::sleep(delay).await;
						Ok(())
					}
					SimulatedTransportFaultAction::Error { kind, message } => {
						Err(Error::new(kind, message))
					}
				}
			}
			None => Ok(()),
		}
	}

	fn route_blocked_error(op: SimulatedTransportFaultOp, source: NodeId, target: NodeId) -> Error {
		Error::new(
			ErrorKind::ConnectionRefused,
			format!(
				"simulated transport {op:?} blocked by partition from {} to {}",
				source.id, target.id
			),
		)
	}

	fn enforce_route(
		&self,
		op: SimulatedTransportFaultOp,
		source: NodeId,
		target: NodeId,
	) -> std::io::Result<()> {
		if self.route_is_blocked(source, target) {
			let err = Self::route_blocked_error(op, source, target);
			let action = SimulatedTransportFaultAction::Error {
				kind: err.kind(),
				message: err.to_string(),
			};
			self.record_event(
				Some(source),
				Some(target),
				SimulatedTransportEventKind::Fault { op, action },
			);
			return Err(err);
		}
		Ok(())
	}

	fn record_event(
		&self,
		source: Option<NodeId>,
		target: Option<NodeId>,
		kind: SimulatedTransportEventKind,
	) {
		let mut events = self.events.lock().unwrap();
		let step = self.next_step.fetch_add(1, Ordering::SeqCst) + 1;
		events.push(SimulatedTransportEvent {
			step,
			source,
			target,
			kind,
		});
	}
}

struct SimulatedTransportStream {
	inner: DuplexStream,
}

impl AsyncRead for SimulatedTransportStream {
	fn poll_read(
		mut self: Pin<&mut Self>,
		cx: &mut TaskContext<'_>,
		buf: &mut ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		Pin::new(&mut self.inner).poll_read(cx, buf)
	}
}

impl AsyncWrite for SimulatedTransportStream {
	fn poll_write(
		mut self: Pin<&mut Self>,
		cx: &mut TaskContext<'_>,
		buf: &[u8],
	) -> Poll<std::io::Result<usize>> {
		Pin::new(&mut self.inner).poll_write(cx, buf)
	}

	fn poll_flush(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
		Pin::new(&mut self.inner).poll_flush(cx)
	}

	fn poll_shutdown(
		mut self: Pin<&mut Self>,
		cx: &mut TaskContext<'_>,
	) -> Poll<std::io::Result<()>> {
		Pin::new(&mut self.inner).poll_shutdown(cx)
	}
}

#[async_trait]
impl TransportStream for SimulatedTransportStream {
	fn stream_id(&self) -> u64 {
		0
	}

	async fn finish(&mut self) -> std::io::Result<()> {
		use tokio::io::AsyncWriteExt;
		self.inner.shutdown().await
	}
}

pub struct SimulatedTransportConnection {
	local: NodeId,
	remote: NodeId,
	network: Arc<SimulatedTransportNetwork>,
	expected_address: String,
	pool: Arc<BufPool>,
	tier: TransportTier,
	actor: ActorIdentity,
}

#[async_trait]
impl TransportConnection for SimulatedTransportConnection {
	async fn open_bi(&self) -> std::io::Result<Box<dyn TransportStream>> {
		self.network.record_event(
			Some(self.local),
			Some(self.remote),
			SimulatedTransportEventKind::OpenBi,
		);
		self.network
			.enforce_route(SimulatedTransportFaultOp::OpenBi, self.local, self.remote)?;
		self.network
			.apply_fault(SimulatedTransportFaultOp::OpenBi, self.local, self.remote)
			.await?;
		let service = self.network.resolve(self.remote, &self.expected_address)?;
		let (client_side, server_side) = tokio::io::duplex(64 * 1024);
		let pool = self.pool.clone();
		let remote = self.local;
		tokio::spawn(async move {
			let _ = service
				.handle_stream(
					Box::new(SimulatedTransportStream { inner: server_side }),
					pool,
					RequestContext {
						remote: Some(remote),
						actor: ActorIdentity::data_node(remote),
					},
				)
				.await;
		});
		Ok(Box::new(SimulatedTransportStream { inner: client_side }))
	}

	async fn accept_bi(&self) -> std::io::Result<Box<dyn TransportStream>> {
		unreachable!("simulated transport connections do not accept server-initiated streams")
	}

	fn remote_id(&self) -> Option<NodeId> {
		Some(self.remote)
	}

	fn actor_identity(&self) -> &ActorIdentity {
		&self.actor
	}

	fn transport_type(&self) -> TransportTier {
		self.tier
	}

	fn is_closed(&self) -> bool {
		!self.network.contains(self.remote, &self.expected_address)
	}
}

pub struct SimulatedTransportProvider {
	local: NodeId,
	network: Arc<SimulatedTransportNetwork>,
	pool: Arc<BufPool>,
	tier: TransportTier,
}

impl SimulatedTransportProvider {
	pub fn new(
		local: NodeId,
		network: Arc<SimulatedTransportNetwork>,
		pool: Arc<BufPool>,
		tier: TransportTier,
	) -> Self {
		Self {
			local,
			network,
			pool,
			tier,
		}
	}
}

#[async_trait]
impl TransportProvider for SimulatedTransportProvider {
	fn withdraw_local(
		&self,
		node_id: NodeId,
		address: &str,
		service: &Arc<dyn HrftService>,
	) -> std::io::Result<()> {
		let removed = {
			let mut services = self
				.network
				.services
				.lock()
				.unwrap_or_else(std::sync::PoisonError::into_inner);
			let matches = services.get(&node_id).is_some_and(|entry| {
				entry.address == address && Arc::ptr_eq(&entry.service, service)
			});
			if matches {
				services.remove(&node_id);
			}
			matches
		};
		if removed {
			self.network.record_event(
				None,
				Some(node_id),
				SimulatedTransportEventKind::Unpublished,
			);
		}
		Ok(())
	}

	async fn connect(
		&self,
		target: NodeId,
		address: &str,
	) -> std::io::Result<Arc<dyn TransportConnection>> {
		self.network.record_event(
			Some(self.local),
			Some(target),
			SimulatedTransportEventKind::Connect,
		);
		self.network
			.enforce_route(SimulatedTransportFaultOp::Connect, self.local, target)?;
		self.network
			.apply_fault(SimulatedTransportFaultOp::Connect, self.local, target)
			.await?;
		self.network.resolve(target, address)?;
		Ok(Arc::new(SimulatedTransportConnection {
			local: self.local,
			remote: target,
			network: self.network.clone(),
			expected_address: address.to_string(),
			pool: self.pool.clone(),
			tier: self.tier,
			actor: ActorIdentity::data_node(target),
		}))
	}

	fn publish_local(
		&self,
		node_id: NodeId,
		address: &str,
		service: Arc<dyn HrftService>,
	) -> std::io::Result<()> {
		self.network.publish(node_id, address, service);
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::network::core::{HRFT_MAGIC, HrftFrame, read_frame, write_frame};
	use crate::network::server::HrftService;
	use crate::schema::{FrameHeader, MsgType};

	struct EchoService;

	#[async_trait]
	impl HrftService for EchoService {
		async fn handle_frame(
			&self,
			_context: RequestContext,
			frame: HrftFrame,
		) -> std::io::Result<Option<HrftFrame>> {
			Ok(Some(frame))
		}
	}

	fn node(id: u32) -> NodeId {
		NodeId { id, _reserved: 0 }
	}

	#[tokio::test]
	async fn pd25_old_withdrawal_cannot_remove_a_newer_shared_provider_registration() {
		let network = Arc::new(SimulatedTransportNetwork::default());
		let provider = SimulatedTransportProvider::new(
			node(1),
			network.clone(),
			BufPool::new(8, 16 * 1024),
			TransportTier::QuicIoUring,
		);
		let first: Arc<dyn HrftService> = Arc::new(EchoService);
		let second: Arc<dyn HrftService> = Arc::new(EchoService);
		provider
			.publish_local(node(2), "sim://node-2", first.clone())
			.unwrap();
		provider
			.publish_local(node(2), "sim://node-2", second.clone())
			.unwrap();
		provider
			.withdraw_local(node(2), "sim://node-2", &first)
			.unwrap();
		assert!(Arc::ptr_eq(
			&network
				.services
				.lock()
				.unwrap()
				.get(&node(2))
				.unwrap()
				.service,
			&second
		));
		assert!(provider.connect(node(2), "sim://node-2").await.is_ok());
		provider
			.withdraw_local(node(2), "sim://node-2", &second)
			.unwrap();
		assert!(!network.services.lock().unwrap().contains_key(&node(2)));
	}

	#[tokio::test]
	async fn simulated_transport_round_trips_a_frame() {
		let pool = BufPool::new(8, 16 * 1024);
		let network = Arc::new(SimulatedTransportNetwork::default());
		let provider = SimulatedTransportProvider::new(
			node(1),
			network.clone(),
			pool.clone(),
			TransportTier::QuicIoUring,
		);
		provider
			.publish_local(node(2), "sim://node-2", Arc::new(EchoService))
			.unwrap();

		let conn = provider.connect(node(2), "sim://node-2").await.unwrap();
		let mut stream = conn.open_bi().await.unwrap();
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::STATUS_REQ.0,
			flags: 0,
			stream_id: 7,
			meta_len: 3,
			arrow_len: 0,
			trailing_len: 0,
		};
		write_frame(&mut *stream, &header, b"abc", None, None)
			.await
			.unwrap();
		stream.finish().await.unwrap();
		let frame = read_frame(&mut *stream, &pool).await.unwrap();
		assert_eq!(frame.header.msg_type, MsgType::STATUS_REQ.0);
		assert_eq!(&frame.meta[..], b"abc");
		let events = network.events();
		assert!(
			events
				.iter()
				.any(|event| matches!(event.kind, SimulatedTransportEventKind::Connect))
		);
		assert!(
			events
				.iter()
				.any(|event| matches!(event.kind, SimulatedTransportEventKind::OpenBi))
		);
	}

	#[tokio::test]
	async fn simulated_transport_faults_are_one_shot_and_targetable() {
		let pool = BufPool::new(8, 16 * 1024);
		let network = Arc::new(SimulatedTransportNetwork::default());
		let provider = SimulatedTransportProvider::new(
			node(1),
			network.clone(),
			pool,
			TransportTier::QuicIoUring,
		);
		provider
			.publish_local(node(2), "sim://node-2", Arc::new(EchoService))
			.unwrap();
		network.inject(
			InjectedSimulatedTransportFault::on(
				SimulatedTransportFaultOp::Connect,
				SimulatedTransportFaultAction::Error {
					kind: ErrorKind::ConnectionRefused,
					message: "partitioned".to_string(),
				},
			)
			.from(node(1))
			.to(node(2)),
		);

		let first = provider.connect(node(2), "sim://node-2").await;
		assert!(first.is_err(), "expected injected connect fault to fail");
		assert_eq!(
			first.err().map(|err| err.kind()),
			Some(ErrorKind::ConnectionRefused)
		);
		provider.connect(node(2), "sim://node-2").await.unwrap();
		let events = network.events();
		assert!(events.iter().any(|event| matches!(
			&event.kind,
			SimulatedTransportEventKind::Fault {
				op: SimulatedTransportFaultOp::Connect,
				action: SimulatedTransportFaultAction::Error { .. },
			}
		)));
	}

	#[tokio::test]
	async fn simulated_transport_faults_preserve_order_when_delayed_matches_overlap() {
		let pool = BufPool::new(8, 16 * 1024);
		let network = Arc::new(SimulatedTransportNetwork::default());
		let provider = SimulatedTransportProvider::new(
			node(1),
			network.clone(),
			pool,
			TransportTier::QuicIoUring,
		);
		provider
			.publish_local(node(2), "sim://node-2", Arc::new(EchoService))
			.unwrap();
		network.inject(
			InjectedSimulatedTransportFault::on(
				SimulatedTransportFaultOp::Connect,
				SimulatedTransportFaultAction::Error {
					kind: ErrorKind::ConnectionAborted,
					message: "delayed-first".to_string(),
				},
			)
			.from(node(1))
			.to(node(2))
			.after_matches(1),
		);
		network.inject(
			InjectedSimulatedTransportFault::on(
				SimulatedTransportFaultOp::Connect,
				SimulatedTransportFaultAction::Error {
					kind: ErrorKind::ConnectionReset,
					message: "immediate-second".to_string(),
				},
			)
			.from(node(1))
			.to(node(2)),
		);

		provider.connect(node(2), "sim://node-2").await.unwrap();
		let second = provider.connect(node(2), "sim://node-2").await;
		assert_eq!(
			second.err().map(|err| err.kind()),
			Some(ErrorKind::ConnectionAborted)
		);
		let third = provider.connect(node(2), "sim://node-2").await;
		assert_eq!(
			third.err().map(|err| err.kind()),
			Some(ErrorKind::ConnectionReset)
		);
		provider.connect(node(2), "sim://node-2").await.unwrap();
	}

	#[tokio::test]
	async fn simulated_transport_routes_can_be_partitioned_and_healed() {
		let pool = BufPool::new(8, 16 * 1024);
		let network = Arc::new(SimulatedTransportNetwork::default());
		let provider = SimulatedTransportProvider::new(
			node(1),
			network.clone(),
			pool,
			TransportTier::QuicIoUring,
		);
		provider
			.publish_local(node(2), "sim://node-2", Arc::new(EchoService))
			.unwrap();
		network.block_route(node(1), node(2));

		let blocked = provider.connect(node(2), "sim://node-2").await;
		assert_eq!(
			blocked.err().map(|err| err.kind()),
			Some(ErrorKind::ConnectionRefused)
		);

		network.heal_route(node(1), node(2));
		provider.connect(node(2), "sim://node-2").await.unwrap();

		let events = network.events();
		assert!(
			events
				.iter()
				.any(|event| matches!(event.kind, SimulatedTransportEventKind::RouteBlocked))
		);
		assert!(
			events
				.iter()
				.any(|event| matches!(event.kind, SimulatedTransportEventKind::RouteHealed))
		);
	}
}
