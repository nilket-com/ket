#[path = "support/control_ring.rs"]
mod support;

use arrow::array::{LargeBinaryArray, LargeBinaryBuilder};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use wendelon::consensus::{TierObserver, WendelonStateMachine};
use wendelon::crypto::{CryptoManager, test_support};
use wendelon::metrics::exporter;
use wendelon::network::traits::DataRing;
use wendelon::node::LEASE_TTL_MS;
use wendelon::schema::{
	ClientWriteId, Consistency, FinalizationMarker, HybridTimestamp, Identity, ReadRequestMeta,
	ReadStatus, TenantId, WriteRequestMeta, WriteStatus, read_key_schema, write_payload_schema,
};
use wendelon::storage::core::StorageEngine;
use wendelon::storage::io_uring::spawn_io_thread;
use wendelon::storage::l2::{LocalFsObjectStore, ObjectStore};
use wendelon::storage::manager::{SlabManager, SlabManagerConfig};
use wendelon::storage::sweeper::{ManagerTierObserver, SlabSweeper, SweeperConfig, TierCommitter};

fn hlc(physical_ms: u64, node_id: u32) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms,
		logical: 0,
		node_id,
	}
}

fn id(locality: u64) -> Identity {
	let mut bytes = [0u8; 32];
	bytes[..8].copy_from_slice(&locality.to_le_bytes());
	Identity::new(bytes)
}

fn tenant() -> TenantId {
	TenantId::ZERO
}

fn test_crypto() -> Arc<CryptoManager> {
	Arc::new(test_support::seeded_dev_crypto_manager(tenant()).unwrap())
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

fn key_batch(key: &[u8]) -> RecordBatch {
	let schema = Arc::new(read_key_schema());
	let mut keys = LargeBinaryBuilder::new();
	keys.append_value(key);
	RecordBatch::try_new(schema, vec![Arc::new(keys.finish())]).unwrap()
}

fn write_meta(node_id: u32, key: &[u8], value: &[u8]) -> WriteRequestMeta {
	WriteRequestMeta {
		write_id: ClientWriteId {
			client_uuid: [7; 16],
			sequence_no: 1,
			idempotency_ttl_ms: 30_000,
			_pad: 0,
		},
		client_hlc: hlc(10, node_id),
		expected_epoch: 1,
		tenant_id: tenant(),
		proxy_hop: 0,
		durability: wendelon::schema::DurabilityLevel::QUORUM,
		await_finalization: 0,
		_pad1: [0; 5],
		proxy_source: wendelon::schema::NodeId {
			id: 0,
			_reserved: 0,
		},
		key_len: key.len() as u32,
		value_len: value.len() as u32,
	}
}

fn read_meta(node_id: u32, key: &[u8]) -> ReadRequestMeta {
	ReadRequestMeta {
		min_hlc: hlc(0, node_id),
		expected_epoch: 1,
		tenant_id: tenant(),
		consistency: Consistency::LINEARIZABLE,
		_pad: [0; 3],
		key_len: key.len() as u32,
		_pad2: [0; 8],
		as_of_hlc: hlc(0, 0),
	}
}

fn extract_value(batch: &RecordBatch) -> Vec<u8> {
	let values: &LargeBinaryArray = batch
		.column_by_name("value")
		.unwrap()
		.as_any()
		.downcast_ref()
		.unwrap();
	values.value(0).to_vec()
}

fn fresh_dir(tag: &str) -> std::path::PathBuf {
	let dir = std::env::temp_dir().join(format!(
		"wendelon-metrics-smoke-{tag}-{}-{}",
		std::process::id(),
		uuid::Uuid::new_v4()
	));
	std::fs::create_dir_all(&dir).unwrap();
	dir
}

async fn scrape(addr: std::net::SocketAddr) -> String {
	let mut stream = TcpStream::connect(addr).await.unwrap();
	stream
		.write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
		.await
		.unwrap();
	stream.shutdown().await.unwrap();
	let mut buf = Vec::new();
	stream.read_to_end(&mut buf).await.unwrap();
	let resp = String::from_utf8(buf).unwrap();
	assert!(
		resp.starts_with("HTTP/1.1 200 OK"),
		"unexpected response: {resp}"
	);
	let (_, body) = resp.split_once("\r\n\r\n").unwrap();
	body.to_string()
}

fn metric_value(body: &str, name: &str) -> u64 {
	body.lines()
		.find_map(|line| {
			let mut fields = line.split_whitespace();
			let metric = fields.next()?;
			if metric != name {
				return None;
			}
			fields.next()?.parse::<u64>().ok()
		})
		.unwrap_or(0)
}

struct DirectApplyCommitter {
	observer: Arc<dyn TierObserver>,
}

#[async_trait]
impl TierCommitter for DirectApplyCommitter {
	async fn commit_tier_slab(&self, tier: wendelon::consensus::TieredSlab) -> std::io::Result<()> {
		self.observer.on_slab_tiered(&tier, 0).await
	}
}

async fn build_manager_with_sealed_slabs(
	l2: Arc<dyn ObjectStore>,
) -> (
	Arc<SlabManager>,
	Arc<dyn TierCommitter>,
	WendelonStateMachine,
) {
	let dir = fresh_dir("l1");
	let io = spawn_io_thread(64).unwrap();
	let manager = Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir,
			max_slab_bytes: 1,
			io,
			l2: Some(l2),
			rehydration: wendelon::storage::manager::RehydrationPolicy::default(),
			group_commit: wendelon::storage::GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap(),
	);
	manager
		.append(tenant(), id(1), hlc(1, 1), b"hello-1")
		.await
		.unwrap();
	manager
		.append(tenant(), id(2), hlc(2, 1), b"hello-2")
		.await
		.unwrap();
	manager
		.append(tenant(), id(3), hlc(3, 1), b"hello-3")
		.await
		.unwrap();

	let sm = WendelonStateMachine::new();
	let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
	let committer: Arc<dyn TierCommitter> = Arc::new(DirectApplyCommitter { observer });
	(manager, committer, sm)
}

#[tokio::test]
async fn scrape_endpoint_exposes_data_ring_and_storage_metrics() {
	let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let exporter = exporter::spawn_on(listener).await.unwrap();

	let node = support::bootstrap_node("metrics", 1).await;
	support::wait_for_local_leader(&node).await;
	let granted = node
		.data_server
		.refresh_leases_once(&node.ring, &*node.clock, LEASE_TTL_MS)
		.await;
	assert!(granted > 0);

	let key = b"metrics-key";
	let value = b"metrics-value";
	let (write_resp, _marker): (
		wendelon::schema::WriteResponseMeta,
		Option<FinalizationMarker>,
	) = node
		.data_server
		.write(
			write_meta(node.local_node.id, key, value),
			kv_batch(key, value),
		)
		.await
		.unwrap();
	assert_eq!(write_resp.status, WriteStatus::OK);

	let (read_resp, read_batch) = node
		.data_server
		.read(read_meta(node.local_node.id, key), key_batch(key))
		.await
		.unwrap();
	assert_eq!(read_resp.status, ReadStatus::OK);
	assert_eq!(extract_value(&read_batch.unwrap()), value);

	let l2_root = fresh_dir("l2");
	let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_root).unwrap());
	let (manager, committer, sm) = build_manager_with_sealed_slabs(l2.clone()).await;
	let sweeper = SlabSweeper::new(
		manager,
		l2,
		committer,
		sm,
		SweeperConfig {
			scan_interval: std::time::Duration::from_millis(10),
			min_slab_bytes: 0,
			max_upload_bytes_per_sec: None,
			key_prefix: "metrics/".into(),
			max_plaintext_rewrites_per_pass: 1,
		},
	);
	let report = sweeper.sweep_once().await.unwrap();
	assert!(report.tiered > 0);

	let body = scrape(exporter.local_addr).await;
	assert!(body.contains("# HELP wendelon_metrics_scrapes_total"));
	assert!(body.contains("wendelon_data_ring_write_requests_total"));
	assert!(body.contains("wendelon_data_ring_read_requests_total"));
	assert!(body.contains("wendelon_storage_tiered_slabs_total"));
	assert!(body.contains("wendelon_storage_migration_reclaimed_slabs_total"));
	assert!(metric_value(&body, "wendelon_metrics_scrapes_total") >= 1);
	assert!(metric_value(&body, "wendelon_data_ring_write_requests_total") >= 1);
	assert!(metric_value(&body, "wendelon_data_ring_read_requests_total") >= 1);
	assert!(metric_value(&body, "wendelon_data_ring_lease_grants_total") >= 1);
	assert!(metric_value(&body, "wendelon_storage_tiered_slabs_total") >= report.tiered as u64);

	exporter.task.abort();
	support::abort_node(&node);
}
