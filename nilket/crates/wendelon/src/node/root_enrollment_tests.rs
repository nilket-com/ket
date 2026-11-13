//! ADR 0726 at the node: who may generate a root KEK, what an established
//! store does when its root is gone or is another one, and what a joiner
//! needs before its first start. Development provider, synthetic roots.
use super::*;
use crate::crypto::test_support::{enroll_root_artifact, place_fresh_dev_root_artifact};

const ARTIFACT: &str = ".wendelon/dev-root-kek.bin";

fn dev(id: u32, dir: &crate::test_support::TestDir) -> NodeBootstrapConfig {
	NodeBootstrapConfig::dev(id, dir.to_path_buf(), "127.0.0.1:0".into())
}

async fn formed(id: u32, dir: &crate::test_support::TestDir) -> WendelonNode {
	let node = WendelonNode::bootstrap_single_node(dev(id, dir))
		.await
		.unwrap();
	assert!(
		node.state_machine
			.read_state(|state| state.root_check.is_some())
			.await
	);
	node
}

async fn root_check(node: &WendelonNode) -> crate::crypto::RootCheck {
	node.state_machine
		.read_state(|state| state.root_check.clone())
		.await
		.expect("a formed node has a root check")
}

fn refused(result: std::io::Result<WendelonNode>, reason: &str) -> std::io::Error {
	let error = match result {
		Err(error) => error,
		Ok(_) => panic!("startup must be refused with {reason}"),
	};
	assert!(error.to_string().contains(reason), "{error}");
	error
}

#[tokio::test]
async fn a_forming_node_generates_its_root_and_establishes_the_check_before_the_first_tenant() {
	let dir = crate::test_support::TestDir::new("root-forming");
	assert!(!dir.to_path_buf().join(ARTIFACT).exists());
	let node = formed(1, &dir).await;
	assert!(dir.to_path_buf().join(ARTIFACT).exists());
	let check = root_check(&node).await;
	assert_eq!(check, node.crypto.candidate_root_check().unwrap());
	assert_eq!(
		node.crypto.root_binding_status(),
		crate::crypto::RootBindingStatus::Verified(check.id())
	);
	// The default tenant's KEK exists and is usable under the verified root.
	assert!(
		node.state_machine
			.read_state(|state| state.tenants.contains_key(&TenantId::ZERO))
			.await
	);
	node.crypto.active_tenant_kek_id(TenantId::ZERO).unwrap();
	// A reopen verifies the same root against the recovered check.
	node.shutdown().await.unwrap();
	let again = formed(1, &dir).await;
	assert_eq!(root_check(&again).await, check);
	assert_eq!(
		again.crypto.root_binding_status(),
		crate::crypto::RootBindingStatus::Verified(check.id())
	);
	again.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_fresh_forming_store_adopts_a_root_already_in_place() {
	let supplied = crate::test_support::TestDir::new("root-adopt-source");
	place_fresh_dev_root_artifact(&supplied.to_path_buf()).unwrap();
	let dir = crate::test_support::TestDir::new("root-adopt");
	enroll_root_artifact(&supplied.to_path_buf(), &dir.to_path_buf()).unwrap();
	let before = std::fs::read(dir.to_path_buf().join(ARTIFACT)).unwrap();
	let node = formed(1, &dir).await;
	// The artifact was read, not replaced, and the cluster's check is its root's.
	assert_eq!(
		std::fs::read(dir.to_path_buf().join(ARTIFACT)).unwrap(),
		before
	);
	let expected =
		crate::crypto::test_support::persistent_dev_crypto_manager(&supplied.to_path_buf())
			.unwrap()
			.candidate_root_check()
			.unwrap();
	assert_eq!(root_check(&node).await, expected);
	node.shutdown().await.unwrap();
}

#[tokio::test]
async fn an_established_store_never_regenerates_a_missing_root() {
	let dir = crate::test_support::TestDir::new("root-missing");
	let node = formed(1, &dir).await;
	node.shutdown().await.unwrap();
	let artifact = dir.to_path_buf().join(ARTIFACT);
	std::fs::remove_file(&artifact).unwrap();
	for _ in 0..2 {
		let error = refused(
			WendelonNode::bootstrap_single_node(dev(1, &dir)).await,
			"RootKekMissing",
		);
		assert_eq!(error.kind(), ErrorKind::NotFound);
		assert!(
			!artifact.exists(),
			"a root was generated for an established store"
		);
	}
}

#[tokio::test]
async fn an_established_store_refuses_a_root_that_does_not_reproduce_its_check() {
	let dir = crate::test_support::TestDir::new("root-other");
	let node = formed(1, &dir).await;
	let check = root_check(&node).await;
	node.shutdown().await.unwrap();
	let artifact = dir.to_path_buf().join(ARTIFACT);
	let original = std::fs::read(&artifact).unwrap();
	std::fs::remove_file(&artifact).unwrap();
	place_fresh_dev_root_artifact(&dir.to_path_buf()).unwrap();
	let other = std::fs::read(&artifact).unwrap();
	assert_ne!(other, original);
	refused(
		WendelonNode::bootstrap_single_node(dev(1, &dir)).await,
		"RootKekMismatch",
	);
	// Nothing was rewritten, and the right root still opens the store.
	assert_eq!(std::fs::read(&artifact).unwrap(), other);
	std::fs::write(&artifact, &original).unwrap();
	let again = formed(1, &dir).await;
	assert_eq!(root_check(&again).await, check);
	again.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_joiner_without_the_cluster_root_refuses_before_any_network() {
	let dir = crate::test_support::TestDir::new("root-joiner");
	let mut cfg = dev(2, &dir);
	cfg.cluster = ClusterBootstrapConfig::Join {
		seeds: vec![SeedNodeConfig {
			node_id: 1,
			address: "127.0.0.1:1".into(),
		}],
	};
	let error = refused(
		WendelonNode::bootstrap_with_provider(cfg, Arc::new(NoDialTransportProvider)).await,
		"RootKekNotEnrolled",
	);
	assert_eq!(error.kind(), ErrorKind::NotFound);
	assert!(!dir.to_path_buf().join(ARTIFACT).exists());
}

/// A forming node that is not the sole member, or that already holds tenants,
/// establishes nothing: the routine reads applied state, not configuration.
#[tokio::test]
async fn the_establishment_routine_acts_only_for_a_sole_member_with_no_keys() {
	let dir = crate::test_support::TestDir::new("root-routine");
	let node = formed(1, &dir).await;
	let local = node.local_node;
	let run =
		|| establish_root_check_if_forming(&node.raft, &node.state_machine, &node.crypto, local);
	assert_eq!(run().await.unwrap(), RootCheckFormation::AlreadyPresent);

	// Tenants without a check: never inferred.
	let mut state = node.state_machine.state_snapshot().await;
	state.root_check = None;
	node.state_machine
		.seed_complete_snapshot_state_for_test(state.clone())
		.await;
	assert!(
		matches!(run().await.unwrap(), RootCheckFormation::NotForming(reason) if reason.contains("tenants or tenant KEKs"))
	);

	// No check, no keys, but an epoch that held another member.
	state.tenants.clear();
	state.tenant_keysets.clear();
	let mut epoch = state
		.epoch_history
		.values()
		.next_back()
		.cloned()
		.expect("a formed node has its genesis epoch");
	let peer = epoch.peers.values().next().cloned().unwrap();
	epoch.peers.insert(local.id + 1, peer);
	epoch.epoch += 1;
	state.epoch_history.insert(epoch.epoch, epoch);
	node.state_machine
		.seed_complete_snapshot_state_for_test(state.clone())
		.await;
	assert!(
		matches!(run().await.unwrap(), RootCheckFormation::NotForming(reason) if reason.contains("another member"))
	);
	assert!(
		node.state_machine
			.read_state(|state| state.root_check.is_none())
			.await,
		"nothing was established"
	);

	// The sole member with no keys and no check: the continuation case.
	state.epoch_history.pop_last();
	node.state_machine
		.seed_complete_snapshot_state_for_test(state)
		.await;
	assert_eq!(run().await.unwrap(), RootCheckFormation::Established);
	assert_eq!(
		root_check(&node).await,
		node.crypto.candidate_root_check().unwrap()
	);
	// A retry after an unknown outcome finds it and changes nothing.
	assert_eq!(run().await.unwrap(), RootCheckFormation::AlreadyPresent);
	node.shutdown().await.unwrap();
}
