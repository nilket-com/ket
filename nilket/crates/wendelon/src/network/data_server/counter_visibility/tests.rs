//! Real public CounterRead regressions. Captured outcomes are asserted after drain.
use super::*;
use crate::storage::counter::{CounterKey, CounterSlab, counter_identity};

async fn read(
	server: &WendelonDataServer,
	pool: &Arc<BufPool>,
	local: NodeId,
	key: Identity,
) -> CounterReadResponseMeta {
	let request = CounterReadRequestMeta {
		expected_epoch: server.admission_snapshot().current_epoch,
		tenant_id: tenant(),
		key_identity: key,
	};
	let mut meta = pool.acquire();
	meta.as_mut_vec()
		.extend_from_slice(bytemuck::bytes_of(&request));
	let frame = HrftFrame {
		header: FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::COUNTER_READ_REQ.0,
			flags: 0,
			stream_id: 1,
			meta_len: meta.len() as u32,
			arrow_len: 0,
			trailing_len: 0,
		},
		meta,
		arrow_payload: None,
		trailing: None,
	};
	let response = DataRingServer::handle_data_frame(
		server,
		RequestContext {
			remote: Some(local),
			actor: actor(ActorRole::Operator, None),
		},
		frame,
	)
	.await
	.unwrap()
	.unwrap();
	*bytemuck::from_bytes(&response.meta[..std::mem::size_of::<CounterReadResponseMeta>()])
}
fn put_witness(server: &WendelonDataServer, origin: NodeId, key: &[u8], value: u64) {
	// Independent frozen .pw2 bytes, not the production encoder.
	let mut bytes = vec![0; 104 + key.len() + 4];
	bytes[..8].copy_from_slice(b"CTRWIT02");
	bytes[8..10].copy_from_slice(&2u16.to_le_bytes());
	let len = bytes.len() as u32;
	bytes[12..16].copy_from_slice(&len.to_le_bytes());
	bytes[16..20].copy_from_slice(&origin.id.to_le_bytes());
	bytes[24..32].copy_from_slice(&1u64.to_le_bytes());
	bytes[32..56].fill(7);
	bytes[56..60].copy_from_slice(&(key.len() as u32).to_le_bytes());
	bytes[64..72].copy_from_slice(&value.to_le_bytes());
	bytes[80..88].copy_from_slice(&1u64.to_le_bytes());
	bytes[96..100].copy_from_slice(&1u32.to_le_bytes());
	bytes[104..104 + key.len()].copy_from_slice(key);
	bytes[104 + key.len()..].copy_from_slice(&999u32.to_le_bytes());
	let dir = server
		.slab
		.data_dir()
		.join("counter-witness")
		.join(format!("tenant-{}", tenant().get()));
	std::fs::create_dir_all(&dir).unwrap();
	std::fs::write(dir.join("counter-witness-0000000000000001.pw2"), bytes).unwrap();
}

#[tokio::test]
async fn ctr4_public_read_hides_tentative_before_failed_add_rollback() {
	let key = counter_identity(&CounterKey(b"k".to_vec()));
	let other = counter_identity(&CounterKey(b"remote-conflict".to_vec()));
	let fx = build_counter_replication_fixture("ctr4-public-dirty", key).await;
	// Initialization occurs before the add, so a first-read reconciler cannot hide the bug.
	fx.leader.counter_leader_for(tenant()).await.unwrap();
	let origin = fx.leader_node.local_node;
	fx.follower
		.counter_replica
		.replicate_tentative(
			origin,
			1,
			tenant(),
			other,
			99,
			crate::schema::HybridTimestamp {
				physical_ms: 1,
				logical: 0,
				node_id: 0,
			},
		)
		.await
		.unwrap();
	let gate = fx
		.follower
		.counter_replica
		.ctr4_pause_operation(origin, tenant(), 1)
		.await;
	let hold = gate;
	let task = tokio::spawn(dispatch_counter_add_shared(
		fx.leader.clone(),
		fx.leader_node.pool.clone(),
		origin,
		1,
		5,
		key,
	));
	let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
	loop {
		if fx.leader_node.slab.lookup(tenant(), key).unwrap().is_some() {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"setup: local tentative did not append"
		);
		tokio::task::yield_now().await;
	}
	let location = fx.leader_node.slab.lookup(tenant(), key).unwrap().unwrap();
	assert_eq!(
		fx.leader_node
			.slab
			.counter_read_value(location)
			.await
			.unwrap(),
		5
	);
	assert!(
		!task.is_finished(),
		"setup: remote gate must hold the in-flight add"
	);
	let gate_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
	while hold.participant_count() < 2 {
		assert!(
			tokio::time::Instant::now() < gate_deadline,
			"setup: remote operation never reached held gate"
		);
		tokio::task::yield_now().await;
	}
	let witness_dir = fx
		.leader_node
		.slab
		.data_dir()
		.join("counter-witness")
		.join(format!("tenant-{}", tenant().get()));
	assert!(
		std::fs::read_dir(&witness_dir).unwrap().next().is_none(),
		"setup: witness exists before sample"
	);
	assert!(
		!task.is_finished(),
		"setup: remote gate must still hold the add"
	);
	let during = read(fx.leader.as_ref(), &fx.leader_node.pool, origin, key).await;
	drop(hold);
	let add = task.await.unwrap();
	fx.follower
		.counter_replica
		.rollback(origin, 1, tenant(), other)
		.await
		.unwrap();
	let after = read(fx.leader.as_ref(), &fx.leader_node.pool, origin, key).await;
	let local_raw = fx.leader_node.slab.lookup(tenant(), key).unwrap();
	shutdown_lease_node(fx.leader_node).await;
	shutdown_lease_node(fx.follower_node).await;
	assert_eq!(add.status, WriteStatus::INTERNAL_ERROR);
	assert_eq!((add.previous_value, add.current_value), (0, 0));
	assert_eq!(
		(during.status, during.value),
		(ReadStatus::KEY_NOT_FOUND, 0),
		"an un-witnessed tentative must never be publicly visible"
	);
	assert_eq!((after.status, after.value), (ReadStatus::KEY_NOT_FOUND, 0));
	assert_eq!(local_raw, None);
}
#[tokio::test]
async fn ctr4_owner_read_sees_witness_before_replica_marker() {
	let key = counter_identity(&CounterKey(b"k".to_vec()));
	let fx = build_counter_replication_fixture("ctr4-public-witness", key).await;
	put_witness(fx.leader.as_ref(), fx.leader_node.local_node, b"k", 55);
	let during = read(
		fx.leader.as_ref(),
		&fx.leader_node.pool,
		fx.leader_node.local_node,
		key,
	)
	.await;
	let replica = fx
		.leader
		.counter_replica
		.read_committed(tenant(), key)
		.await
		.unwrap();
	shutdown_lease_node(fx.leader_node).await;
	shutdown_lease_node(fx.follower_node).await;
	assert_eq!((during.status, during.value), (ReadStatus::OK, 55));
	assert_eq!(
		replica, None,
		"no marker exists for the unavailable recorded peer999"
	);
}
#[tokio::test]
async fn ctr4_nonowner_cached_leader_cannot_expose_witness_only_value() {
	let key = counter_identity(&CounterKey(b"k".to_vec()));
	let fx = build_counter_replication_fixture("ctr4-public-nonowner", key).await;
	put_witness(fx.follower.as_ref(), fx.follower_node.local_node, b"k", 55);
	let cached = fx.follower.counter_leader_for(tenant()).await.unwrap();
	let cached_value = cached.read(&CounterKey(b"k".to_vec())).await.unwrap();
	let response = read(
		fx.follower.as_ref(),
		&fx.follower_node.pool,
		fx.follower_node.local_node,
		key,
	)
	.await;
	shutdown_lease_node(fx.leader_node).await;
	shutdown_lease_node(fx.follower_node).await;
	assert_eq!(cached_value, Some(55));
	assert_eq!(
		(response.status, response.value),
		(ReadStatus::KEY_NOT_FOUND, 0),
		"cache existence does not establish current-key ownership"
	);
}
