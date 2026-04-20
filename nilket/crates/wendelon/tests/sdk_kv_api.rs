#[path = "support/control_ring.rs"]
mod support;

use std::sync::Arc;
use wendelon::client::{ClusterPeerWire, ClusterStatusWire, WendelonClient};
use wendelon::consensus::{UpdateTenantQuota, WendelonRequest};
use wendelon::network::server::HrftService;
use wendelon::network::transport::{NodeRole, NodeRoleSet, TransportTier, TransportTierSet};
use wendelon_client::{Client, ClientError, ReadOptions, WriteOptions};

async fn refresh_leases(node: &wendelon::node::WendelonNode) {
	support::wait_for_local_leader(node).await;
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, wendelon::node::LEASE_TTL_MS)
		.await;
}

fn snapshot(node_id: u32, address: &str) -> ClusterStatusWire {
	let tier_bits = TransportTierSet::empty()
		.with(TransportTier::QuicIoUring)
		.as_u8();
	let role_bits = NodeRoleSet::empty()
		.with(NodeRole::Data)
		.with(NodeRole::Control)
		.as_u8();
	ClusterStatusWire {
		epoch: 1,
		leader: Some(node_id),
		replication_factor: 1,
		peers: vec![ClusterPeerWire {
			node_id,
			address: address.to_string(),
			tier_bits,
			role_bits,
		}],
	}
}

#[tokio::test]
async fn sdk_write_and_read_round_trip_over_inner_client() {
	let node = support::bootstrap_node("sdk-roundtrip", 41).await;
	refresh_leases(&node).await;

	let service: Arc<dyn HrftService> = node.router.clone();
	let inner = WendelonClient::new(support::loopback_client_config(
		node.local_node,
		service,
		snapshot(node.local_node.id, "loopback://node-41"),
	));
	let sdk = Client::from_inner(inner);

	let write = sdk
		.write(
			wendelon::schema::TenantId::ZERO,
			b"sdk-key",
			b"sdk-value",
			WriteOptions::default(),
		)
		.await
		.unwrap();
	assert!(!write.duplicate);

	let read = sdk
		.read(
			wendelon::schema::TenantId::ZERO,
			b"sdk-key",
			ReadOptions::default(),
		)
		.await
		.unwrap()
		.expect("key should exist");
	assert_eq!(read.value, b"sdk-value");

	support::abort_node(&node);
}

#[tokio::test]
async fn sdk_surfaces_quota_exceeded_without_retry_loop() {
	let node = support::bootstrap_node("sdk-quota", 42).await;
	refresh_leases(&node).await;
	node.raft
		.client_write(WendelonRequest::UpdateTenantQuota(UpdateTenantQuota {
			tenant_id: wendelon::schema::TenantId::ZERO,
			quota_bytes: 0,
		}))
		.await
		.unwrap();
	node.data_server.sync_tenant_policy_cache_once().await;

	let service: Arc<dyn HrftService> = node.router.clone();
	let inner = WendelonClient::new(support::loopback_client_config(
		node.local_node,
		service,
		snapshot(node.local_node.id, "loopback://node-42"),
	));
	let sdk = Client::from_inner(inner);

	let err = sdk
		.write(
			wendelon::schema::TenantId::ZERO,
			b"sdk-quota-key",
			b"sdk-quota-value",
			WriteOptions::default(),
		)
		.await
		.unwrap_err();
	assert!(matches!(
		err,
		ClientError::QuotaExceeded {
			tenant_id: wendelon::schema::TenantId::ZERO
		}
	));

	support::abort_node(&node);
}
