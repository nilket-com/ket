//! ADR 0728 at a real node: an upload sealed under a tenant KEK, held between
//! its local checks and its control-ring decision while that version is rotated
//! away and retired through the ring. Development provider, synthetic keys.
//! The row is appended to storage directly; no client write is involved.
use super::*;
use crate::consensus::WendelonRequest;
use crate::crypto::RetireTenantKekVersion;
use crate::schema::{HybridTimestamp, Identity, TenantId};
use crate::storage::{
	OpTicket, RaftCommitter, SlabSweeper, StorageEngine, SweeperConfig, TierCommitter,
	VerifiedReceipt,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Notify;

/// The production committer, with its first first-publication held at the
/// submission itself: after every local check, before the ring sees it.
struct HeldAtSubmission {
	inner: RaftCommitter,
	held: AtomicBool,
	reached: Notify,
	release: Notify,
}

#[async_trait::async_trait]
impl TierCommitter for HeldAtSubmission {
	fn should_propose(&self) -> bool {
		self.inner.should_propose()
	}
	fn may_offload(&self) -> bool {
		self.inner.may_offload()
	}
	async fn preflight(&self, owner: crate::consensus::OwnerRef) -> std::io::Result<()> {
		self.inner.preflight(owner).await
	}
	async fn submit_op(&self, ticket: &OpTicket) -> std::io::Result<VerifiedReceipt> {
		if ticket.command().is_first_publication() && !self.held.swap(true, Ordering::AcqRel) {
			self.reached.notify_one();
			self.release.notified().await;
		}
		self.inner.submit_op(ticket).await
	}
	async fn cancel_op(&self, ticket: &OpTicket) -> std::io::Result<VerifiedReceipt> {
		self.inner.cancel_op(ticket).await
	}
	async fn ack_outcomes(
		&self,
		owner: crate::consensus::OwnerRef,
		seqs: Vec<u64>,
	) -> std::io::Result<crate::consensus::slab_ops::AckReceipt> {
		self.inner.ack_outcomes(owner, seqs).await
	}
	async fn op_prefix(&self, owner: crate::consensus::OwnerRef) -> std::io::Result<Option<u64>> {
		self.inner.op_prefix(owner).await
	}
	async fn op_view(
		&self,
		key: crate::consensus::slab_ops::OpKey,
		digest: [u8; 32],
		slabs: Vec<crate::consensus::SlabRef>,
	) -> std::io::Result<crate::consensus::slab_ops::OpView> {
		self.inner.op_view(key, digest, slabs).await
	}
	async fn secondary_index_catalogs(
		&self,
	) -> std::io::Result<(
		crate::schema::TableSchemaCatalog,
		crate::schema::SecondaryIndexCatalog,
	)> {
		self.inner.secondary_index_catalogs().await
	}
}

async fn default_tenant_active_kek(node: &WendelonNode) -> crate::schema::TenantKekId {
	let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
	loop {
		let active = node
			.state_machine
			.read_state(|state| {
				state
					.tenant_keysets
					.get(&TenantId::ZERO)
					.and_then(|keyset| keyset.active_tenant_kek_id)
			})
			.await;
		if let Some(active) = active {
			return active;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"the default tenant was not provisioned"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
}

#[tokio::test]
async fn a_publication_whose_key_was_retired_while_held_is_rejected_and_its_row_stays_readable() {
	let dir = crate::test_support::TestDir::new("dc231-held-publication");
	let l2_dir = crate::test_support::TestDir::new("dc231-held-publication-l2");
	let mut cfg = NodeBootstrapConfig::dev(31, dir.to_path_buf(), "127.0.0.1:0".into());
	cfg.slab_max_bytes = 1;
	cfg.l2 = Some(ObjectStoreConfig::LocalFs {
		root: l2_dir.to_path_buf(),
	});
	cfg.tiering.scan_interval = Duration::from_secs(300);
	cfg.tiering.min_slab_bytes = u64::MAX;
	cfg.tiering.max_plaintext_rewrites_per_pass = 0;
	cfg.tiering.max_dek_rewraps_per_pass = 0;
	cfg.tiering.op_driver_interval = Duration::from_secs(3600);
	let object_store = cfg.l2.as_ref().unwrap().build().unwrap();
	let node = WendelonNode::bootstrap_single_node(cfg).await.unwrap();
	let sealed_under = default_tenant_active_kek(&node).await;

	let key = Identity::blake3(b"dc231-row");
	let at = |physical_ms| HybridTimestamp {
		physical_ms,
		logical: 0,
		node_id: node.local_node.id,
	};
	let location = node
		.slab
		.append(TenantId::ZERO, key, at(1), b"acknowledged-row")
		.await
		.unwrap();
	// A second append seals the first row's slab.
	node.slab
		.append(
			TenantId::ZERO,
			Identity::blake3(b"dc231-filler"),
			at(2),
			b"filler",
		)
		.await
		.unwrap();

	let committer = Arc::new(HeldAtSubmission {
		inner: RaftCommitter::new(node.raft.clone()).with_state_machine(node.state_machine.clone()),
		held: AtomicBool::new(false),
		reached: Notify::new(),
		release: Notify::new(),
	});
	let sweeper = Arc::new(SlabSweeper::new(
		node.slab.clone(),
		object_store.clone(),
		committer.clone(),
		node.state_machine.clone(),
		SweeperConfig {
			scan_interval: Duration::from_millis(10),
			op_driver_interval: Duration::from_millis(500),
			min_slab_bytes: 0,
			max_upload_bytes_per_sec: None,
			key_prefix: "sealed/".into(),
			max_plaintext_rewrites_per_pass: 0,
			max_dek_rewraps_per_pass: 0,
		},
	));
	let pass = tokio::spawn({
		let sweeper = sweeper.clone();
		async move { sweeper.sweep_once().await }
	});
	// The barrier: the object is encrypted, uploaded and verified, the binding
	// check has passed, the operation is allocated; the ring has not seen it.
	tokio::time::timeout(Duration::from_secs(30), committer.reached.notified())
		.await
		.expect("the pass reached its first publication");
	let slab = node.slab.owner_ref().unwrap().slab(location.slab_id);
	assert!(
		!node
			.state_machine
			.read_state(move |state| state.tiered_slabs.contains_key(&slab))
			.await
	);

	// Rotate, then retire the version the held object was sealed under, each
	// decided by the ring.
	let rotated = node.rotate_tenant_kek(TenantId::ZERO).await.unwrap();
	assert_ne!(rotated, sealed_under);
	let retired = node
		.raft
		.client_write(WendelonRequest::RetireTenantKekVersion(
			RetireTenantKekVersion {
				tenant_id: TenantId::ZERO,
				tenant_kek_id: sealed_under,
			},
		))
		.await
		.unwrap()
		.data;
	assert_eq!(retired, crate::consensus::WendelonResponse::Success);

	let candidates = object_store.list("sealed/").await.unwrap();
	assert_eq!(candidates.len(), 1, "{candidates:?}");

	committer.release.notify_one();
	let report = tokio::time::timeout(Duration::from_secs(60), pass)
		.await
		.expect("the pass ended")
		.unwrap()
		.unwrap();

	// The ring rejected the publication: no pointer, and the row is still served
	// from the local file, which is still there.
	let pointer = node
		.state_machine
		.read_state(move |state| state.tiered_slabs.get(&slab).cloned())
		.await;
	assert!(
		pointer.is_none(),
		"a committed pointer names retired tenant KEK {sealed_under:?}: {pointer:?}"
	);
	assert_eq!(report.tiered, 0, "{report:?}");
	assert_eq!(report.failed, 1, "{report:?}");
	assert!(!node.slab.is_tiered(location.slab_id).await);
	assert!(node.slab.slab_file_exists_for_test(location.slab_id));
	assert_eq!(node.slab.read(location).await.unwrap(), b"acknowledged-row");

	// One more pass, with nothing held: the slab is sealed again under the
	// active version and published.
	let report = tokio::time::timeout(Duration::from_secs(60), sweeper.sweep_once())
		.await
		.expect("the second pass ended")
		.unwrap();
	let pointer = node
		.state_machine
		.read_state(move |state| state.tiered_slabs.get(&slab).cloned())
		.await
		.unwrap_or_else(|| panic!("the second pass published nothing: {report:?}"));
	assert_eq!(pointer.tenant_kek_id, Some(rotated));
	assert!(node.slab.is_tiered(location.slab_id).await);
	assert!(!node.slab.slab_file_exists_for_test(location.slab_id));
	assert_eq!(node.slab.read(location).await.unwrap(), b"acknowledged-row");
	// The rejected candidate is not deleted by this change. It stays uploaded,
	// unreferenced, and durably recorded as a rejected candidate.
	let objects = object_store.list("sealed/").await.unwrap();
	assert_eq!(objects.len(), 2, "{objects:?}");
	assert!(objects.iter().any(|object| object.uri == pointer.l2_uri));
	let rejected = node
		.slab
		.require_op_index()
		.unwrap()
		.load_rejected_tier_candidates()
		.await
		.unwrap();
	assert_eq!(rejected.len(), 1, "{rejected:?}");
	assert_eq!(rejected[0].slab_id, location.slab_id);
	assert_eq!(rejected[0].l2_uri, candidates[0].uri);
	assert_ne!(rejected[0].l2_uri, pointer.l2_uri);
	node.shutdown().await.unwrap();
}
