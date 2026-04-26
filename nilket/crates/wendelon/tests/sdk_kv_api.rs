#[path = "support/control_ring.rs"]
mod support;

use arrow::array::LargeBinaryArray;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use wendelon::client::{ClusterPeerWire, ClusterStatusWire, WendelonClient};
use wendelon::consensus::{UpdateTenantQuota, WendelonRequest};
use wendelon::network::server::HrftService;
use wendelon::network::transport::{NodeRole, NodeRoleSet, TransportTier, TransportTierSet};
use wendelon::schema::{HybridTimestamp, Locality, TenantId};
use wendelon_client::{
	Client, ClientError, QueryLocalityRange, QueryOptions, ReadConsistency, ReadOptions,
	WriteOptions,
};

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
			capability_bits: 0,
		}],
	}
}

fn hlc(physical_ms: u64, logical: u32, node_id: u32) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms,
		logical,
		node_id,
	}
}

fn extract_single_kv(batch: &RecordBatch) -> (Vec<u8>, Vec<u8>) {
	let keys: &LargeBinaryArray = batch
		.column_by_name("key")
		.unwrap()
		.as_any()
		.downcast_ref()
		.unwrap();
	let values: &LargeBinaryArray = batch
		.column_by_name("value")
		.unwrap()
		.as_any()
		.downcast_ref()
		.unwrap();
	assert_eq!(batch.num_rows(), 1, "test expects one row");
	(keys.value(0).to_vec(), values.value(0).to_vec())
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

#[tokio::test]
async fn sdk_query_reads_retained_snapshot_as_of_hlc() {
	let node = support::bootstrap_node("sdk-query-time-travel", 43).await;
	refresh_leases(&node).await;

	let service: Arc<dyn HrftService> = node.router.clone();
	let inner = WendelonClient::new(support::loopback_client_config(
		node.local_node,
		service,
		snapshot(node.local_node.id, "loopback://node-43"),
	));
	let sdk = Client::from_inner(inner);
	let tenant_id = TenantId::ZERO;
	let key = b"sdk-query-key";

	let first = sdk
		.write(
			tenant_id,
			key,
			b"value-v1",
			WriteOptions {
				client_hlc: Some(hlc(10, 0, 43)),
				..WriteOptions::default()
			},
		)
		.await
		.unwrap();
	let second = sdk
		.write(
			tenant_id,
			key,
			b"value-v2",
			WriteOptions {
				client_hlc: Some(hlc(20, 0, 43)),
				..WriteOptions::default()
			},
		)
		.await
		.unwrap();
	assert!(second.commit_hlc >= first.commit_hlc);

	let locality = Locality::from(Client::identity_for_key(tenant_id, key));
	let historical = sdk
		.query(
			tenant_id,
			Vec::new(),
			QueryOptions {
				range: QueryLocalityRange::point(locality),
				consistency: ReadConsistency::STALE,
				min_hlc: None,
				as_of_hlc: Some(first.commit_hlc),
				stats_pruning_enabled: true,
			},
		)
		.await
		.unwrap();
	assert_eq!(historical.rows, 1);
	let historical_row = historical
		.batches
		.first()
		.expect("historical query should return one batch");
	assert_eq!(
		extract_single_kv(historical_row),
		(key.to_vec(), b"value-v1".to_vec())
	);

	let latest = sdk
		.query(
			tenant_id,
			Vec::new(),
			QueryOptions {
				range: QueryLocalityRange::point(locality),
				consistency: ReadConsistency::STALE,
				..QueryOptions::default()
			},
		)
		.await
		.unwrap();
	assert_eq!(latest.rows, 1);
	let latest_row = latest
		.batches
		.first()
		.expect("latest query should return one batch");
	assert_eq!(
		extract_single_kv(latest_row),
		(key.to_vec(), b"value-v2".to_vec())
	);

	support::abort_node(&node);
}
