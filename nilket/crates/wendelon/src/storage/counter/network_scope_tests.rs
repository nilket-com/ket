//! Public dispatch boundary only; private storage reasons are asserted in the parent module.
use super::*;
use crate::network::core::{HRFT_MAGIC, HrftFrame};
use crate::network::router::DataRingServer;
use crate::network::server::RequestContext;
use crate::network::tls::ActorIdentity;
use crate::node::{NodeBootstrapConfig, WendelonNode};
use crate::schema::{
	CounterReplicaAck, CounterReplicaStatus, CounterReplicateRequestMeta,
	CounterRollbackRequestMeta, FrameHeader, MsgType,
};
fn context(origin: NodeId) -> RequestContext {
	RequestContext {
		remote: Some(origin),
		actor: ActorIdentity::data_node(origin),
	}
}
fn tentative(key: Identity, op: u64) -> CounterReplicateRequestMeta {
	CounterReplicateRequestMeta {
		commit_hlc: hlc(op),
		op_id: op,
		value: 11,
		epoch: 999,
		tenant_id: tenant(7),
		key_identity: key,
		phase: CounterReplicaPhase::TENTATIVE,
		_pad: [0; 7],
	}
}
fn frame(node: &WendelonNode, msg: u8, meta: &[u8]) -> HrftFrame {
	let mut buf = node.pool.acquire();
	buf.as_mut_vec().extend_from_slice(meta);
	HrftFrame {
		header: FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: msg,
			flags: 0,
			stream_id: 1,
			meta_len: buf.len() as u32,
			arrow_len: 0,
			trailing_len: 0,
		},
		meta: buf,
		arrow_payload: None,
		trailing: None,
	}
}
async fn dispatch(
	node: &WendelonNode,
	ctx: RequestContext,
	msg: u8,
	meta: &[u8],
) -> std::io::Result<CounterReplicaAck> {
	let response =
		DataRingServer::handle_data_frame(node.data_server.as_ref(), ctx, frame(node, msg, meta))
			.await?
			.expect("counter ack");
	Ok(*bytemuck::from_bytes(
		&response.meta[..std::mem::size_of::<CounterReplicaAck>()],
	))
}
async fn boot(tag: &str) -> (TestDir, WendelonNode) {
	let d = TestDir::new(tag);
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		48,
		d.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	(d, node)
}
async fn shutdown(node: WendelonNode) {
	node.handles.abort();
	node.raft.shutdown().await.unwrap();
	node.shutdown().await.unwrap();
}
#[tokio::test]
async fn b1d_dispatch_context_then_phase_then_zero_missing_mismatch_order() {
	let (_d, node) = boot("counter-scope-dispatch-order").await;
	let key = identity(b"wire");
	let mut meta = tentative(Identity::new([0; 32]), 1);
	meta.phase = CounterReplicaPhase(255);
	let before = node.slab.local_l1_bytes().await;
	for ctx in [
		RequestContext {
			remote: None,
			actor: ActorIdentity::data_node(super::node(1)),
		},
		RequestContext {
			remote: Some(super::node(2)),
			actor: ActorIdentity::data_node(super::node(1)),
		},
		context(NodeId {
			id: 1,
			_reserved: 1,
		}),
	] {
		let ack = dispatch(
			&node,
			ctx,
			MsgType::COUNTER_REPLICATE_REQ.0,
			bytemuck::bytes_of(&meta),
		)
		.await
		.unwrap();
		assert_eq!(ack.status, CounterReplicaStatus::NOT_AUTHORIZED);
	}
	let error = dispatch(
		&node,
		context(super::node(1)),
		MsgType::COUNTER_REPLICATE_REQ.0,
		bytemuck::bytes_of(&meta),
	)
	.await
	.unwrap_err();
	assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
	meta.phase = CounterReplicaPhase::COMMIT;
	let ack = dispatch(
		&node,
		context(super::node(1)),
		MsgType::COUNTER_REPLICATE_REQ.0,
		bytemuck::bytes_of(&meta),
	)
	.await
	.unwrap();
	assert_eq!(ack.status, CounterReplicaStatus::INTERNAL_ERROR);
	assert_eq!(ack.status.0, 3);
	meta.key_identity = key;
	assert_eq!(
		dispatch(
			&node,
			context(super::node(1)),
			MsgType::COUNTER_REPLICATE_REQ.0,
			bytemuck::bytes_of(&meta)
		)
		.await
		.unwrap()
		.status,
		CounterReplicaStatus::INTERNAL_ERROR,
	);
	assert_eq!(node.slab.local_l1_bytes().await, before);
	let mut rollback = CounterRollbackRequestMeta {
		key_identity: Identity::new([0; 32]),
		op_id: 999,
		epoch: 999,
		tenant_id: tenant(7),
	};
	for ctx in [
		RequestContext {
			remote: None,
			actor: ActorIdentity::data_node(super::node(1)),
		},
		RequestContext {
			remote: Some(super::node(2)),
			actor: ActorIdentity::data_node(super::node(1)),
		},
		context(NodeId {
			id: 1,
			_reserved: 1,
		}),
	] {
		assert_eq!(
			dispatch(
				&node,
				ctx,
				MsgType::COUNTER_ROLLBACK_REQ.0,
				bytemuck::bytes_of(&rollback)
			)
			.await
			.unwrap()
			.status,
			CounterReplicaStatus::NOT_AUTHORIZED
		);
	}
	assert_eq!(
		dispatch(
			&node,
			context(super::node(1)),
			MsgType::COUNTER_ROLLBACK_REQ.0,
			bytemuck::bytes_of(&rollback)
		)
		.await
		.unwrap()
		.status,
		CounterReplicaStatus::INTERNAL_ERROR
	);
	rollback.key_identity = key;
	assert_eq!(
		dispatch(
			&node,
			context(super::node(1)),
			MsgType::COUNTER_ROLLBACK_REQ.0,
			bytemuck::bytes_of(&rollback)
		)
		.await
		.unwrap()
		.status,
		CounterReplicaStatus::OK
	);
	assert_eq!(node.slab.local_l1_bytes().await, before);
	meta.phase = CounterReplicaPhase::TENTATIVE;
	dispatch(
		&node,
		context(super::node(1)),
		MsgType::COUNTER_REPLICATE_REQ.0,
		bytemuck::bytes_of(&meta),
	)
	.await
	.unwrap();
	let before = node.slab.local_l1_bytes().await;
	meta.phase = CounterReplicaPhase::COMMIT;
	meta.key_identity = identity(b"wrong");
	assert_eq!(
		dispatch(
			&node,
			context(super::node(1)),
			MsgType::COUNTER_REPLICATE_REQ.0,
			bytemuck::bytes_of(&meta)
		)
		.await
		.unwrap()
		.status,
		CounterReplicaStatus::INTERNAL_ERROR
	);
	assert_eq!(node.slab.local_l1_bytes().await, before);
	for (key, expected) in [
		(Identity::new([0; 32]), CounterReplicaStatus::INTERNAL_ERROR),
		(identity(b"wrong"), CounterReplicaStatus::INTERNAL_ERROR),
	] {
		let rollback = CounterRollbackRequestMeta {
			key_identity: key,
			op_id: 1,
			epoch: 999,
			tenant_id: tenant(7),
		};
		assert_eq!(
			dispatch(
				&node,
				context(super::node(1)),
				MsgType::COUNTER_ROLLBACK_REQ.0,
				bytemuck::bytes_of(&rollback)
			)
			.await
			.unwrap()
			.status,
			expected
		);
		assert_eq!(node.slab.local_l1_bytes().await, before);
		assert!(node.slab.counter_lookup(tenant(7), meta_key()).is_some());
	}
	let rollback = CounterRollbackRequestMeta {
		key_identity: key,
		op_id: 1,
		epoch: 999,
		tenant_id: tenant(7),
	};
	assert_eq!(
		dispatch(
			&node,
			context(super::node(1)),
			MsgType::COUNTER_ROLLBACK_REQ.0,
			bytemuck::bytes_of(&rollback)
		)
		.await
		.unwrap()
		.status,
		CounterReplicaStatus::OK
	);
	assert!(node.slab.counter_lookup(tenant(7), key).is_none());
	shutdown(node).await;
}
fn meta_key() -> Identity {
	identity(b"wire")
}
#[tokio::test]
async fn b1d_dispatch_uses_authenticated_origin_not_epoch_or_target_for_scope() {
	let (_d, node) = boot("counter-scope-wire-origin").await;
	let a = identity(b"a");
	let b = identity(b"b");
	let one = tentative(a, 1);
	let mut two = tentative(b, 1);
	two.value = 22;
	two.epoch = 1;
	for (origin, meta) in [(1, one), (2, two)] {
		assert_eq!(
			dispatch(
				&node,
				context(super::node(origin)),
				MsgType::COUNTER_REPLICATE_REQ.0,
				bytemuck::bytes_of(&meta)
			)
			.await
			.unwrap()
			.status,
			CounterReplicaStatus::OK
		);
	}
	assert_eq!(
		node.slab
			.counter_read_value(node.slab.counter_lookup(tenant(7), a).unwrap())
			.await
			.unwrap(),
		11
	);
	assert_eq!(
		node.slab
			.counter_read_value(node.slab.counter_lookup(tenant(7), b).unwrap())
			.await
			.unwrap(),
		22
	);
	let wrong = CounterRollbackRequestMeta {
		key_identity: a,
		op_id: 1,
		epoch: 0,
		tenant_id: tenant(7),
	};
	let before = node.slab.local_l1_bytes().await;
	assert_eq!(
		dispatch(
			&node,
			context(super::node(2)),
			MsgType::COUNTER_ROLLBACK_REQ.0,
			bytemuck::bytes_of(&wrong)
		)
		.await
		.unwrap()
		.status,
		CounterReplicaStatus::INTERNAL_ERROR
	);
	assert_eq!(node.slab.local_l1_bytes().await, before);
	assert!(node.slab.counter_lookup(tenant(7), a).is_some());
	assert!(node.slab.counter_lookup(tenant(7), b).is_some());
	let mut commit = one;
	commit.phase = CounterReplicaPhase::COMMIT;
	commit.epoch = 0;
	assert_eq!(
		dispatch(
			&node,
			context(super::node(1)),
			MsgType::COUNTER_REPLICATE_REQ.0,
			bytemuck::bytes_of(&commit)
		)
		.await
		.unwrap()
		.status,
		CounterReplicaStatus::OK
	);
	let rollback = CounterRollbackRequestMeta {
		key_identity: b,
		op_id: 1,
		epoch: 0,
		tenant_id: tenant(7),
	};
	assert_eq!(
		dispatch(
			&node,
			context(super::node(2)),
			MsgType::COUNTER_ROLLBACK_REQ.0,
			bytemuck::bytes_of(&rollback)
		)
		.await
		.unwrap()
		.status,
		CounterReplicaStatus::OK
	);
	assert!(node.slab.counter_lookup(tenant(7), a).is_some());
	assert!(node.slab.counter_lookup(tenant(7), b).is_none());
	shutdown(node).await;
}
