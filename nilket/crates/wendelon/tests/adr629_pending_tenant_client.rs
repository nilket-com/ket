//! Public error mapping only; native tests pin the complete refusal metadata.
#[path = "support/control_ring.rs"]
mod support;

use std::sync::Arc;
use std::time::Duration;
use wendelon::client::{ClusterPeerWire, ClusterStatusWire, WendelonClient};
use wendelon::consensus::TenantFinalization;
use wendelon::network::pending_finalizations::{
	PendingFinalizationRecord, PendingFinalizationStore, pending_finalizations_path,
};
use wendelon::network::transport::{NodeRole, NodeRoleSet, TransportTier, TransportTierSet};
use wendelon::schema::{
	ClientWriteId, DurabilityLevel, FinalizationMarker, HybridTimestamp, Identity, TenantId,
};
use wendelon_client::{Client, ClientError, WriteOptions};

#[tokio::test]
async fn existing_foreign_pending_maps_to_public_authorization_failed_both_orders() {
	let node = support::bootstrap_node("b1b-public-refusal", 1).await;
	support::wait_for_local_leader(&node).await;
	node.handles.pending_finalization_reconciler.abort();
	tokio::time::timeout(Duration::from_secs(2), async {
		while !node.handles.pending_finalization_reconciler.is_finished() {
			tokio::task::yield_now().await;
		}
	})
	.await
	.expect("reconciler cancellation");
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;
	let tiers = TransportTierSet::empty().with(TransportTier::QuicIoUring);
	let snapshot = ClusterStatusWire {
		epoch: 1,
		leader: Some(1),
		replication_factor: 1,
		peers: vec![ClusterPeerWire {
			node_id: 1,
			address: "loopback://node-1".into(),
			tier_bits: tiers.as_u8(),
			role_bits: NodeRoleSet::empty()
				.with(NodeRole::Data)
				.with(NodeRole::Control)
				.as_u8(),
			capability_bits: 0,
		}],
	};
	let inner = WendelonClient::new(support::loopback_client_config(
		node.local_node,
		node.router.clone(),
		snapshot,
	));
	let pending =
		PendingFinalizationStore::open(pending_finalizations_path(node.slab.data_dir())).unwrap();
	for (order, (owner, requester)) in [(7, 8), (8, 7)].into_iter().enumerate() {
		let uuid = [0xB1 + order as u8; 16];
		let id = ClientWriteId {
			client_uuid: uuid,
			sequence_no: 1,
			idempotency_ttl_ms: 60_000,
			_pad: 0,
		};
		let record = PendingFinalizationRecord::new(
			id,
			true,
			TenantFinalization {
				tenant_id: TenantId::new(owner),
				key_identity: Identity::new([0xA4; 32]),
				charged_bytes: 777,
				record_count: 1,
				marker: FinalizationMarker {
					log_index: 991,
					term: 9,
					epoch: 1,
					commit_hlc: HybridTimestamp {
						physical_ms: 998,
						logical: 7,
						node_id: 91,
					},
					payload_hash: [0xFA; 32],
					e_safe_index: 991,
					durability: DurabilityLevel::QUORUM,
					_pad: [0; 7],
				},
			},
		);
		pending.upsert(record.clone()).await.unwrap();
		let before_bytes = node.slab.local_l1_bytes().await;
		let client = Client::with_client_uuid(Arc::clone(&inner), uuid);
		assert!(matches!(
			client
				.write(
					TenantId::new(requester),
					b"pending-collision",
					b"B-value",
					WriteOptions::default()
				)
				.await,
			Err(ClientError::AuthorizationFailed)
		));
		assert_eq!(pending.get(record.id).await.unwrap(), Some(record.clone()));
		assert_eq!(node.slab.local_l1_bytes().await, before_bytes);
		pending.remove(record.id).await.unwrap();
	}
	drop(inner);
	drop(pending);
	node.handles.abort();
	tokio::time::timeout(Duration::from_secs(10), async {
		node.raft.shutdown().await.unwrap();
		node.shutdown().await.unwrap();
	})
	.await
	.expect("bounded node shutdown");
}
