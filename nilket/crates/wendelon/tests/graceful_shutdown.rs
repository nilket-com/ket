#[path = "support/control_ring.rs"]
mod support;

use async_trait::async_trait;
use std::io::ErrorKind;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use wendelon::network::core::HrftFrame;
use wendelon::network::pool::PooledBuf;
use wendelon::network::server::{HrftService, LifecycleService, RequestContext, ServerLifecycle};
use wendelon::network::tls::ActorIdentity;
use wendelon::node::{NodeBootstrapConfig, WendelonNode};
use wendelon::schema::{FrameHeader, MsgType, NodeId};

fn status_request(stream_id: u32) -> HrftFrame {
	HrftFrame {
		header: FrameHeader {
			magic: wendelon::network::core::HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::STATUS_REQ.0,
			flags: 0,
			stream_id,
			meta_len: 0,
			arrow_len: 0,
			trailing_len: 0,
		},
		meta: PooledBuf::orphan(Vec::new()),
		arrow_payload: None,
		trailing: None,
	}
}

fn context(remote: NodeId) -> RequestContext {
	RequestContext {
		remote: Some(remote),
		actor: ActorIdentity::data_node(remote),
	}
}

#[tokio::test]
async fn node_rejects_new_requests_after_drain_begins() {
	let registry = support::shared_loopback_registry();
	let provider = support::loopback_provider(
		NodeId {
			id: 1,
			_reserved: 0,
		},
		registry,
	);
	let node_dir = support::tempdir("graceful-node");
	let node = WendelonNode::bootstrap_with_provider(
		NodeBootstrapConfig::dev(1, node_dir.to_path_buf(), "loopback://node-1".into()),
		provider,
	)
	.await
	.unwrap();

	let remote = NodeId {
		id: 77,
		_reserved: 0,
	};
	let resp = node
		.service
		.handle_frame(context(remote), status_request(1))
		.await
		.unwrap()
		.unwrap();
	assert_eq!(resp.header.msg_type, MsgType::STATUS_RESP.0);

	node.begin_drain();
	let err = node
		.service
		.handle_frame(context(remote), status_request(2))
		.await
		.unwrap_err();
	assert_eq!(err.kind(), ErrorKind::ConnectionAborted);

	node.shutdown().await.unwrap();
}

struct BlockingService {
	started: Arc<Notify>,
	release: Arc<Notify>,
}

#[async_trait]
impl HrftService for BlockingService {
	async fn handle_frame(
		&self,
		_context: RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<Option<HrftFrame>> {
		self.started.notify_waiters();
		self.release.notified().await;
		Ok(Some(HrftFrame {
			header: FrameHeader {
				magic: frame.header.magic,
				version: frame.header.version,
				msg_type: MsgType::STATUS_RESP.0,
				flags: 0,
				stream_id: frame.header.stream_id,
				meta_len: 0,
				arrow_len: 0,
				trailing_len: 0,
			},
			meta: PooledBuf::orphan(Vec::new()),
			arrow_payload: None,
			trailing: None,
		}))
	}
}

#[tokio::test]
async fn drain_waits_for_in_flight_request_to_complete() {
	let lifecycle = ServerLifecycle::new();
	let started = Arc::new(Notify::new());
	let release = Arc::new(Notify::new());
	let service: Arc<dyn HrftService> = Arc::new(LifecycleService::new(
		Arc::new(BlockingService {
			started: started.clone(),
			release: release.clone(),
		}),
		lifecycle.clone(),
	));
	let remote = NodeId {
		id: 88,
		_reserved: 0,
	};

	let started_wait = started.notified();
	let in_flight = {
		let service = service.clone();
		tokio::spawn(async move {
			service
				.handle_frame(context(remote), status_request(9))
				.await
		})
	};
	started_wait.await;

	lifecycle.begin_drain();
	let err = service
		.handle_frame(context(remote), status_request(10))
		.await
		.unwrap_err();
	assert_eq!(err.kind(), ErrorKind::ConnectionAborted);
	assert!(
		tokio::time::timeout(Duration::from_millis(50), lifecycle.wait_for_quiescence())
			.await
			.is_err(),
		"drain should wait for the in-flight request"
	);

	release.notify_waiters();
	let resp = in_flight.await.unwrap().unwrap().unwrap();
	assert_eq!(resp.header.msg_type, MsgType::STATUS_RESP.0);
	assert_eq!(lifecycle.in_flight(), 0);
	tokio::time::timeout(Duration::from_secs(1), lifecycle.wait_for_quiescence())
		.await
		.unwrap();
}
