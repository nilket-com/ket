#[path = "support/control_ring.rs"]
mod support;

use arrow::array::LargeBinaryBuilder;
use arrow::record_batch::RecordBatch;
use std::collections::BTreeMap;
use std::sync::Arc;
use wendelon::network::traits::DataRing;
use wendelon::network::{LeaseAuthority, dev_lease_signing_secret, lease_verifying_key_bytes};
use wendelon::node::{NodeBootstrapConfig, WendelonNode};
use wendelon::schema::{
	ClientWriteId, HybridTimestamp, LeaseDescriptor, WriteRequestMeta, WriteStatus,
	write_payload_schema,
};

fn hlc(physical_ms: u64, logical: u32, node_id: u32) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms,
		logical,
		node_id,
	}
}

fn kv_batch(key: &[u8], value: &[u8]) -> RecordBatch {
	let schema = Arc::new(write_payload_schema());
	let mut keys = LargeBinaryBuilder::new();
	keys.append_value(key);
	let mut values = LargeBinaryBuilder::new();
	values.append_value(value);
	RecordBatch::try_new(
		schema,
		vec![Arc::new(keys.finish()), Arc::new(values.finish())],
	)
	.unwrap()
}

fn write_meta(node_id: u32) -> WriteRequestMeta {
	WriteRequestMeta {
		write_id: ClientWriteId {
			client_uuid: [7; 16],
			sequence_no: 1,
			idempotency_ttl_ms: 10_000,
			_pad: 0,
		},
		client_hlc: hlc(1, 0, node_id),
		expected_epoch: 1,
		proxy_hop: 0,
		durability: wendelon::schema::DurabilityLevel::QUORUM,
		await_finalization: 0,
		_pad1: [0; 5],
		proxy_source: wendelon::schema::NodeId {
			id: 0,
			_reserved: 0,
		},
		key_len: 1,
		value_len: 1,
	}
}

fn bootstrap_cfg(
	tag: &str,
	node_id: u32,
	secret: [u8; 32],
	trusted_key: [u8; 32],
) -> NodeBootstrapConfig {
	let mut trusted_lease_signers = BTreeMap::new();
	trusted_lease_signers.insert(node_id, trusted_key);
	NodeBootstrapConfig {
		node_id,
		data_dir: support::tempdir(tag),
		slab_max_bytes: 64 * 1024 * 1024,
		rf: 1,
		io_depth: 64,
		advertise_address: "127.0.0.1:0".into(),
		lease_signing_secret: secret,
		trusted_lease_signers,
	}
}

fn unsigned_lease(node: &WendelonNode) -> LeaseDescriptor {
	LeaseDescriptor {
		epoch: 1,
		holder: node.local_node,
		range_start: 0,
		range_end: u64::MAX,
		lease_start: hlc(1, 0, node.local_node.id),
		lease_expiry: hlc(100_000, 0, node.local_node.id),
		min_commit_index: 0,
		issuer: node.local_node,
		signature: [0; 64],
	}
}

#[tokio::test]
async fn leader_issues_signed_lease_that_data_ring_accepts() {
	let secret = dev_lease_signing_secret(101);
	let trusted = lease_verifying_key_bytes(secret);
	let node = WendelonNode::bootstrap_single_node(bootstrap_cfg(
		"lease-signing-happy",
		101,
		secret,
		trusted,
	))
	.await
	.unwrap();
	support::wait_for_local_leader(&node).await;

	let granted = node
		.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;
	assert_eq!(granted, 1);

	let cache = node.data_server.lease_cache();
	let lease = cache.read().unwrap()[0];
	assert_ne!(lease.signature, [0; 64]);
	assert_eq!(lease.issuer, node.local_node);

	let verifier = LeaseAuthority::from_secret_bytes(node.local_node, secret, &{
		let mut map = BTreeMap::new();
		map.insert(node.local_node.id, trusted);
		map
	})
	.unwrap();
	verifier.verify_lease(&lease).unwrap();

	let (resp, _fin) = DataRing::write(
		&*node.data_server,
		write_meta(node.local_node.id),
		kv_batch(b"k", b"v"),
	)
	.await
	.unwrap();
	assert_eq!(resp.status, WriteStatus::OK);

	support::abort_node(&node);
}

#[tokio::test]
async fn tampered_signed_lease_is_rejected_at_write_gate() {
	let secret = dev_lease_signing_secret(111);
	let trusted = lease_verifying_key_bytes(secret);
	let node = WendelonNode::bootstrap_single_node(bootstrap_cfg(
		"lease-signing-tamper",
		111,
		secret,
		trusted,
	))
	.await
	.unwrap();
	support::wait_for_local_leader(&node).await;
	node.data_server.lease_cache().write().unwrap().clear();

	let signer = LeaseAuthority::from_secret_bytes(node.local_node, secret, &{
		let mut map = BTreeMap::new();
		map.insert(node.local_node.id, trusted);
		map
	})
	.unwrap();
	let mut lease = signer.sign_lease(unsigned_lease(&node));
	lease.range_end = u64::MAX - 1;
	node.data_server.lease_cache().write().unwrap().push(lease);

	let (resp, _fin) = DataRing::write(
		&*node.data_server,
		write_meta(node.local_node.id),
		kv_batch(b"k", b"v"),
	)
	.await
	.unwrap();
	assert_eq!(resp.status, WriteStatus::INVALID_LEASE_SIGNATURE);

	support::abort_node(&node);
}

#[tokio::test]
async fn wrong_key_signed_lease_is_rejected_at_write_gate() {
	let trusted_secret = dev_lease_signing_secret(121);
	let wrong_secret = dev_lease_signing_secret(122);
	let trusted = lease_verifying_key_bytes(trusted_secret);
	let node = WendelonNode::bootstrap_single_node(bootstrap_cfg(
		"lease-signing-wrong-key",
		121,
		trusted_secret,
		trusted,
	))
	.await
	.unwrap();
	support::wait_for_local_leader(&node).await;
	node.data_server.lease_cache().write().unwrap().clear();

	let wrong_signer = LeaseAuthority::from_secret_bytes(node.local_node, wrong_secret, &{
		let mut map = BTreeMap::new();
		map.insert(node.local_node.id, lease_verifying_key_bytes(wrong_secret));
		map
	})
	.unwrap();
	let forged = wrong_signer.sign_lease(unsigned_lease(&node));
	node.data_server.lease_cache().write().unwrap().push(forged);

	let (resp, _fin) = DataRing::write(
		&*node.data_server,
		write_meta(node.local_node.id),
		kv_batch(b"k", b"v"),
	)
	.await
	.unwrap();
	assert_eq!(resp.status, WriteStatus::INVALID_LEASE_SIGNATURE);

	support::abort_node(&node);
}
