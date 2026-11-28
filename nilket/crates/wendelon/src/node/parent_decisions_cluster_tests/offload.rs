//! ADR 0725: every replica offloads its own cold slabs. Three ParentDecisions
//! nodes share one object store; each seals a slab holding the same rows, and
//! each, the two followers included, uploads its own object, has its pointer
//! committed through the leader, and drops its local file.

use super::*;
use crate::network::decision_proposals::{ForwardedDecision, ForwardedHold};
use crate::schema::SlabId;
use crate::storage::core::StorageEngine;
use crate::storage::l2::ObjectStore;

const TICK: std::time::Duration = std::time::Duration::from_millis(100);

/// A ready cluster whose three nodes tier to one shared object store.
async fn offload_cluster(
	tag: &str,
) -> (
	Cluster,
	u32,
	TableSchemaDescriptor,
	crate::test_support::TestDir,
) {
	let root = crate::test_support::TestDir::new(&format!("offload-l2-{tag}"));
	let path = root.to_path_buf();
	// The members share one root KEK by the enrollment procedure of ADR 0726,
	// which `Cluster::start_with` performs: the forming member's root artifact
	// is copied into each joiner's data directory before it first starts.
	let (cluster, leader, descriptor) = ready_with(Cluster::start_with(move |_, cfg| {
		cfg.l2 = Some(crate::storage::ObjectStoreConfig::LocalFs { root: path.clone() });
		cfg.tiering.scan_interval = std::time::Duration::from_millis(200);
		cfg.tiering.op_driver_interval = std::time::Duration::from_millis(200);
		None
	}))
	.await;
	(cluster, leader, descriptor, root)
}

fn store(root: &crate::test_support::TestDir) -> crate::storage::l2::LocalFsObjectStore {
	crate::storage::l2::LocalFsObjectStore::new(root.to_path_buf()).unwrap()
}

fn followers(leader: u32) -> Vec<u32> {
	NODES.into_iter().filter(|node| *node != leader).collect()
}

/// Each node's Raft term and the leader it knows. Terms only grow, so equal
/// readings on every node before and after an interval mean no election
/// completed inside it.
fn terms(cluster: &Cluster) -> Vec<(u32, u64, Option<u32>)> {
	cluster
		.nodes
		.iter()
		.map(|node| {
			let metrics = node.raft.metrics().borrow().clone();
			(
				node.local_node.id,
				metrics.current_term,
				metrics.current_leader,
			)
		})
		.collect()
}

pub(super) async fn write_rows(
	cluster: &Cluster,
	leader: u32,
	descriptor: &TableSchemaDescriptor,
	rows: &[(i64, &str)],
) -> Expected {
	assert_committed(
		&write_through(
			cluster,
			leader,
			write_meta(descriptor, rows.len(), uuid_v7()),
			named_rows(descriptor, rows),
		)
		.await,
		"the rows to offload",
	);
	rows.iter()
		.map(|(id, value)| (*id, (*value).to_owned()))
		.collect()
}

/// The stored frame of every one of `ids` at `node`, read from its own store.
async fn frames_at(
	cluster: &Cluster,
	node: u32,
	descriptor: &TableSchemaDescriptor,
	ids: &[i64],
) -> Vec<Vec<u8>> {
	let server = &cluster.node(node).data_server;
	let slab = &cluster.node(node).slab;
	let mut frames = Vec::new();
	for id in ids {
		let key = server
			.row_identity_and_locality_for_test(
				TenantId::ZERO,
				descriptor,
				&named_rows(descriptor, &[(*id, "x")]),
			)
			.0;
		let versions = owned_versions(cluster, node, key).await;
		assert_eq!(versions.len(), 1, "row {id} at {node}");
		let locations = slab
			.version_locations(TenantId::ZERO, key, versions[0])
			.unwrap();
		assert_eq!(locations.len(), 1, "row {id} at {node}");
		frames.push(slab.read(locations[0]).await.unwrap().to_vec());
	}
	frames
}

/// Waits until `node`'s slab `id` is offloaded: a committed pointer, out of the
/// sealed set, and its file gone from the node's own directory. The pointer is
/// installed before the file is unlinked, so the file is the fact checked. At
/// every poll the slab is on local disk or has a committed pointer, never
/// neither. Checked before any read of the tiered slab, so a file that a later
/// rehydration writes back is not mistaken for one never reclaimed.
pub(super) async fn until_offloaded(cluster: &Cluster, node: u32, id: SlabId, what: &str) {
	let slab = &cluster.node(node).slab;
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		let sealed = slab.sealed_slab_ids().await.contains(&id);
		let on_disk = slab.slab_file_exists_for_test(id);
		let pointer = slab.tier_pointer(id).await;
		assert!(
			on_disk || pointer.is_some(),
			"{what}: slab {id:?} at {node} is neither on disk nor tiered"
		);
		if pointer.is_some() && !sealed && !on_disk {
			return;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"{what}: slab {id:?} at {node} never offloaded (sealed={sealed} on_disk={on_disk})"
		);
		tokio::time::sleep(TICK).await;
	}
}

/// Waits until `node`'s tier authority reports ready.
pub(super) async fn until_ready(cluster: &Cluster, node: u32, what: &str) {
	let slab = &cluster.node(node).slab;
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		let reason = slab.tier_authority_readiness_reason();
		if reason.is_none() {
			return;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"{what}: {node} is still unready: {reason:?}"
		);
		tokio::time::sleep(TICK).await;
	}
}

async fn until_no_intents(cluster: &Cluster, node: u32, what: &str) {
	let index = cluster.node(node).slab.require_op_index().unwrap();
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		let intents = index.load_op_intents().await.unwrap();
		if intents.is_empty() {
			return;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"{what}: {node} still holds intents {intents:?}"
		);
		tokio::time::sleep(TICK).await;
	}
}

/// `node`'s committed pointer for its slab `id`, which the ring holds under
/// that node's own owner and whose object is in the store with its hash.
async fn committed_object(
	cluster: &Cluster,
	root: &crate::test_support::TestDir,
	node: u32,
	id: SlabId,
) -> String {
	let leader = cluster.leader().await;
	committed_object_at(cluster, root, node, id, leader).await
}

/// `committed_object`, with the ring read at `ring_node`.
async fn committed_object_at(
	cluster: &Cluster,
	root: &crate::test_support::TestDir,
	node: u32,
	id: SlabId,
	ring_node: u32,
) -> String {
	let slab = &cluster.node(node).slab;
	let owner = slab.owner_ref().unwrap();
	let pointer = slab.tier_pointer(id).await.expect("a local pointer");
	let ring = cluster
		.node(ring_node)
		.state_machine
		.read_state(move |state| state.tiered_slabs.get(&owner.slab(id)).cloned())
		.await
		.expect("the ring holds the pointer under the node's own owner");
	assert_eq!(ring.l2_uri, pointer.l2_uri);
	assert!(
		pointer.l2_uri.contains(&format!(
			"{:08x}-{:032x}/",
			owner.node(),
			owner.incarnation().get()
		)),
		"{node}'s object is under its own owner prefix: {}",
		pointer.l2_uri
	);
	let bytes = store(root).get(&pointer.l2_uri).await.unwrap();
	assert_eq!(crate::hash::content::digest(&bytes), ring.payload_hash);
	pointer.l2_uri
}

fn hold_owner_ops(
	cluster: &Cluster,
	leader: u32,
	sender: u32,
) -> (
	Arc<tokio::sync::Notify>,
	Arc<tokio::sync::Notify>,
	Arc<std::sync::Mutex<Option<ForwardedDecision>>>,
) {
	let (reached, release, held) = (
		Arc::new(tokio::sync::Notify::new()),
		Arc::new(tokio::sync::Notify::new()),
		Arc::new(std::sync::Mutex::new(None)),
	);
	cluster
		.node(leader)
		.data_server
		.decision_peer_for_test()
		.proposals
		.as_ref()
		.unwrap()
		.hold_forwarded_for_test(ForwardedHold {
			sender,
			matches: |request| matches!(request, ForwardedDecision::OwnerOp(_)),
			reached: reached.clone(),
			release: release.clone(),
			held: held.clone(),
		});
	(reached, release, held)
}

/// The requirement: all three replicas, the two followers while they follow,
/// offload their own sealed slab, read its rows back exactly from their own
/// store, and have their acknowledgements accepted.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn every_replica_offloads_its_own_sealed_slab_while_the_leader_is_stable() {
	let (cluster, leader, descriptor, root) = offload_cluster("all").await;
	let ids = [1i64, 2, 3, 4];
	let expected = write_rows(
		&cluster,
		leader,
		&descriptor,
		&[(1, "a"), (2, "b"), (3, "c"), (4, "d")],
	)
	.await;
	let before = terms(&cluster);
	let mut local_frames = std::collections::BTreeMap::new();
	for node in NODES {
		local_frames.insert(node, frames_at(&cluster, node, &descriptor, &ids).await);
	}
	let mut sealed = std::collections::BTreeMap::new();
	for node in NODES {
		sealed.insert(
			node,
			cluster.node(node).slab.force_seal_active_for_test().await,
		);
	}
	let mut uris = std::collections::BTreeSet::new();
	for node in NODES {
		until_offloaded(&cluster, node, sealed[&node], "offload").await;
		uris.insert(committed_object(&cluster, &root, node, sealed[&node]).await);
	}
	assert_eq!(uris.len(), NODES.len(), "one object per owner");
	// Each node's own store answers its rows from its object: the local file is
	// gone, and the frames are exactly the ones it stored.
	for node in NODES {
		assert_eq!(
			frames_at(&cluster, node, &descriptor, &ids).await,
			local_frames[&node],
			"{node}'s rows from its own tiered slab"
		);
	}
	expect_everywhere(&cluster, &descriptor, &expected, "after every offload").await;
	// The followers' operations are handed off and acknowledged from where they are.
	let owners: Vec<_> = followers(leader)
		.into_iter()
		.map(|node| cluster.node(node).slab.owner_ref().unwrap())
		.collect();
	cluster
		.until(
			"every follower's acknowledgement is accepted",
			move |state| {
				owners.iter().all(|owner| {
					state
						.op_tables
						.get(owner)
						.is_some_and(|table| table.acked_through >= 1)
				})
			},
		)
		.await;
	for node in NODES {
		until_no_intents(&cluster, node, "after its offload").await;
	}
	// No election completed anywhere in the interval: the followers followed.
	assert_eq!(terms(&cluster), before);
	assert!(before.iter().all(|(_, _, known)| *known == Some(leader)));
}

/// A follower whose own apply observer fails on its committed pointer still
/// ends with the slab offloaded: the failure is not completion, and the
/// established local paths finish the work.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_failed_apply_observer_leaves_a_followers_offload_to_finish_locally() {
	let (cluster, leader, descriptor, root) = offload_cluster("observer").await;
	let follower = followers(leader)[0];
	let ids = [1i64, 2];
	write_rows(&cluster, leader, &descriptor, &[(1, "a"), (2, "b")]).await;
	let frames = frames_at(&cluster, follower, &descriptor, &ids).await;
	let owner = cluster.node(follower).slab.owner_ref().unwrap();
	crate::storage::sweeper::tier_apply_failure::arm(owner);
	let id = cluster
		.node(follower)
		.slab
		.force_seal_active_for_test()
		.await;
	until_offloaded(&cluster, follower, id, "after the failed observer").await;
	assert!(
		!crate::storage::sweeper::tier_apply_failure::is_armed(owner),
		"the follower's observer was called for its own pointer and failed"
	);
	committed_object(&cluster, &root, follower, id).await;
	until_no_intents(&cluster, follower, "after the failed observer").await;
	assert_eq!(
		frames_at(&cluster, follower, &descriptor, &ids).await,
		frames
	);
}

/// A follower's Submit that never answers is an unknown outcome: the local
/// slab stays, nothing is unlinked, and the follower resolves the operation
/// itself. The late Submit then changes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_followers_unanswered_submit_keeps_its_slab_until_it_is_resolved() {
	let (cluster, leader, descriptor, root) = offload_cluster("unanswered").await;
	let follower = followers(leader)[0];
	let ids = [1i64, 2];
	let expected = write_rows(&cluster, leader, &descriptor, &[(1, "a"), (2, "b")]).await;
	let frames = frames_at(&cluster, follower, &descriptor, &ids).await;
	let (reached, release, held) = hold_owner_ops(&cluster, leader, follower);
	let slab = cluster.node(follower).slab.clone();
	let id = slab.force_seal_active_for_test().await;
	tokio::time::timeout(BOUND, reached.notified())
		.await
		.expect("the follower's Submit reached the leader's handler");
	assert!(matches!(
		held.lock().unwrap().as_ref(),
		Some(ForwardedDecision::OwnerOp(op))
			if op.claimed_owner() == slab.owner_ref().unwrap()
	));
	// Held at the leader: nothing is committed, so nothing is unlinked.
	assert!(slab.sealed_slab_ids().await.contains(&id));
	assert!(slab.slab_file_exists_for_test(id));
	assert!(slab.tier_pointer(id).await.is_none());
	// The follower's attempt ends unknown; it resolves the operation and
	// publishes the slab, while the first Submit is still held.
	until_offloaded(&cluster, follower, id, "with its first Submit held").await;
	let uri = committed_object(&cluster, &root, follower, id).await;
	// The late Submit is answered from the record and changes nothing.
	release.notify_one();
	until_no_intents(&cluster, follower, "after the late Submit").await;
	tokio::time::sleep(std::time::Duration::from_secs(1)).await;
	assert_eq!(committed_object(&cluster, &root, follower, id).await, uri);
	assert_eq!(
		frames_at(&cluster, follower, &descriptor, &ids).await,
		frames
	);
	expect_everywhere(&cluster, &descriptor, &expected, "after the late Submit").await;
}

/// Releases a held handler when dropped, so a failed assertion cannot strand it.
struct ReleaseOnDrop(Arc<tokio::sync::Notify>);

impl Drop for ReleaseOnDrop {
	fn drop(&mut self) {
		self.0.notify_one();
	}
}

/// The leader loses the cluster while it holds a follower's Submit. The other
/// two nodes elect a new leader; only then is the held Submit released, at a
/// node that no longer leads a quorum. The follower's slab is never without
/// both its local file and a committed pointer, and it ends offloaded under the
/// new leader, read back exactly.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_leader_change_during_a_followers_offload_never_loses_the_slab() {
	let (cluster, leader, descriptor, root) = offload_cluster("leader-change").await;
	let follower = followers(leader)[0];
	let other = followers(leader)[1];
	let ids = [1i64, 2];
	write_rows(&cluster, leader, &descriptor, &[(1, "a"), (2, "b")]).await;
	let frames = frames_at(&cluster, follower, &descriptor, &ids).await;
	let term_before = cluster.node(follower).raft.metrics().borrow().current_term;
	let (reached, release, _held) = hold_owner_ops(&cluster, leader, follower);
	let release = ReleaseOnDrop(release);
	let slab = cluster.node(follower).slab.clone();
	let id = slab.force_seal_active_for_test().await;
	tokio::time::timeout(BOUND, reached.notified())
		.await
		.expect("the follower's Submit reached the leader's handler");
	// The old leader is cut off both ways: nobody can dial it, and it can reach
	// nobody. It stays open, with the Submit held in its handler.
	cluster.network.unpublish(node_id(leader));
	for peer in [follower, other] {
		cluster.network.block_route(node_id(leader), node_id(peer));
		cluster.network.block_route(node_id(peer), node_id(leader));
	}
	// Observed, not assumed: both remaining nodes follow a new leader in a later term.
	let deadline = tokio::time::Instant::now() + BOUND;
	let new_leader = loop {
		let seen: Vec<(u64, Option<u32>)> = [follower, other]
			.iter()
			.map(|node| {
				let metrics = cluster.node(*node).raft.metrics().borrow().clone();
				(metrics.current_term, metrics.current_leader)
			})
			.collect();
		if seen[0] == seen[1]
			&& seen[0].0 > term_before
			&& seen[0].1.is_some_and(|known| known != leader)
		{
			break seen[0].1.unwrap();
		}
		assert!(
			slab.slab_file_exists_for_test(id) || slab.tier_pointer(id).await.is_some(),
			"during the election"
		);
		assert!(
			tokio::time::Instant::now() < deadline,
			"no new leader among the remaining two: {seen:?}"
		);
		tokio::time::sleep(TICK).await;
	};
	// Only now does the old leader's handler continue, with no quorum behind it.
	drop(release);
	until_offloaded(&cluster, follower, id, "under the new leader").await;
	committed_object_at(&cluster, &root, follower, id, new_leader).await;
	until_no_intents(&cluster, follower, "under the new leader").await;
	assert_eq!(
		frames_at(&cluster, follower, &descriptor, &ids).await,
		frames
	);
	let after = cluster.node(follower).raft.metrics().borrow().clone();
	assert_eq!(after.current_leader, Some(new_leader), "one leader change");
}

/// A follower stops with its Submit pending; the leader commits it while the
/// follower is down. Reopened, the follower applies its own pointer, drops the
/// local file and hands the operation off.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_restarted_follower_completes_the_offload_committed_while_it_was_down() {
	let (mut cluster, leader, descriptor, root) = offload_cluster("restart").await;
	let follower = followers(leader)[0];
	let ids = [1i64, 2];
	let expected = write_rows(&cluster, leader, &descriptor, &[(1, "a"), (2, "b")]).await;
	let frames = frames_at(&cluster, follower, &descriptor, &ids).await;
	let (reached, release, _held) = hold_owner_ops(&cluster, leader, follower);
	let id = cluster
		.node(follower)
		.slab
		.force_seal_active_for_test()
		.await;
	let owner = cluster.node(follower).slab.owner_ref().unwrap();
	tokio::time::timeout(BOUND, reached.notified())
		.await
		.expect("the follower's Submit reached the leader's handler");
	cluster.stop(follower, true).await;
	release.notify_one();
	cluster
		.until(
			"the held Submit commits with its owner down",
			move |state| state.tiered_slabs.contains_key(&owner.slab(id)),
		)
		.await;
	cluster.reopen(follower).await;
	until_settled(&cluster, &descriptor).await;
	assert_eq!(cluster.node(follower).slab.owner_ref().unwrap(), owner);
	until_offloaded(&cluster, follower, id, "after its restart").await;
	committed_object(&cluster, &root, follower, id).await;
	until_no_intents(&cluster, follower, "after its restart").await;
	// The reopen found a pending Tier and fenced; with it resolved the store is
	// ready again, and it never led to get there.
	until_ready(&cluster, follower, "after its restart").await;
	let metrics = cluster.node(follower).raft.metrics().borrow().clone();
	assert_ne!(metrics.state, openraft::ServerState::Leader);
	assert_eq!(metrics.current_leader, Some(leader));
	assert_eq!(
		frames_at(&cluster, follower, &descriptor, &ids).await,
		frames
	);
	expect_everywhere(&cluster, &descriptor, &expected, "after the restart").await;
}

/// Every regular file under `root`, recursively.
fn objects_under(root: &std::path::Path) -> Vec<std::path::PathBuf> {
	let mut files = Vec::new();
	let mut pending = vec![root.to_path_buf()];
	while let Some(dir) = pending.pop() {
		for entry in std::fs::read_dir(&dir).unwrap() {
			let path = entry.unwrap().path();
			if path.is_dir() {
				pending.push(path);
			} else {
				files.push(path);
			}
		}
	}
	files
}

/// ADR 0726: a member that joins with a root KEK of its own (not the forming
/// member's artifact) is told so by the cluster's root check. It keeps
/// replicating and voting, reports `RootKekMismatch`, uploads nothing, keeps
/// its slab on local disk, and refuses an encrypted read, while the two
/// members that share the cluster's root offload.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_member_with_another_root_keeps_its_vote_and_does_no_key_work() {
	let stranger = NODES[2];
	let root = crate::test_support::TestDir::new("offload-l2-stranger");
	let path = root.to_path_buf();
	let (cluster, leader, descriptor) = ready_with(Cluster::start_with(move |id, cfg| {
		if id == stranger {
			// A separately generated development root, as a node that was never
			// given the cluster's artifact would have. Synthetic, per test.
			crate::crypto::test_support::place_fresh_dev_root_artifact(&cfg.data_dir).unwrap();
		}
		cfg.l2 = Some(crate::storage::ObjectStoreConfig::LocalFs { root: path.clone() });
		cfg.tiering.scan_interval = std::time::Duration::from_millis(200);
		cfg.tiering.op_driver_interval = std::time::Duration::from_millis(200);
		None
	}))
	.await;
	assert_ne!(leader, stranger, "the forming member leads this fixture");
	let check = cluster
		.node(leader)
		.state_machine
		.read_state(|state| state.root_check.clone())
		.await
		.expect("the cluster's root check");
	for node in NODES {
		let status = cluster.node(node).crypto.root_binding_status();
		if node == stranger {
			assert_eq!(
				status,
				crate::crypto::RootBindingStatus::Mismatch(check.id())
			);
		} else {
			assert_eq!(
				status,
				crate::crypto::RootBindingStatus::Verified(check.id())
			);
		}
	}
	use crate::metrics::observability::ReadinessReporter;
	assert_eq!(
		cluster
			.node(stranger)
			.readiness_reporter()
			.snapshot()
			.await
			.degraded_reason
			.as_deref(),
		Some("RootKekMismatch")
	);

	let expected = write_rows(
		&cluster,
		leader,
		&descriptor,
		&[(1, "a"), (2, "b"), (3, "c")],
	)
	.await;
	let mut sealed = std::collections::BTreeMap::new();
	for node in NODES {
		sealed.insert(
			node,
			cluster.node(node).slab.force_seal_active_for_test().await,
		);
	}
	let mut uris = Vec::new();
	for node in NODES.into_iter().filter(|node| *node != stranger) {
		until_offloaded(
			&cluster,
			node,
			sealed[&node],
			"a member of the cluster's root",
		)
		.await;
		uris.push(committed_object(&cluster, &root, node, sealed[&node]).await);
	}
	// Many sweeper passes of the stranger later, it has uploaded nothing and
	// still holds its slab.
	tokio::time::sleep(std::time::Duration::from_secs(3)).await;
	let slab = &cluster.node(stranger).slab;
	assert!(slab.slab_file_exists_for_test(sealed[&stranger]));
	assert!(slab.tier_pointer(sealed[&stranger]).await.is_none());
	assert_eq!(
		objects_under(&root.to_path_buf()).len(),
		2,
		"only the two members of the cluster's root uploaded"
	);
	// It cannot read a member's object either.
	let bytes = store(&root).get(&uris[0]).await.unwrap();
	let error = cluster
		.node(stranger)
		.crypto
		.decrypt_tiered_slab(sealed[&NODES[0]], &bytes)
		.expect_err("the stranger's root opens nothing of the cluster's");
	assert_eq!(
		crate::crypto::RootBindingRefusal::of(&error),
		Some(crate::crypto::RootBindingRefusal::Mismatch)
	);
	// Every path that would propose a tenant KEK stops at the local binding,
	// before anything is proposed.
	let keysets_before = cluster
		.node(leader)
		.state_machine
		.read_state(|state| state.tenant_keysets.clone())
		.await;
	for error in [
		cluster
			.node(stranger)
			.crypto
			.prepare_rotation(TenantId::ZERO)
			.map(|_| ())
			.unwrap_err(),
		cluster
			.node(stranger)
			.rotate_tenant_kek(TenantId::ZERO)
			.await
			.map(|_| ())
			.unwrap_err(),
		cluster
			.node(stranger)
			.create_tenant(TenantId::new(91), 1 << 20, 3)
			.await
			.unwrap_err(),
	] {
		assert_eq!(
			crate::crypto::RootBindingRefusal::of(&error),
			Some(crate::crypto::RootBindingRefusal::Mismatch),
			"{error}"
		);
	}
	assert_eq!(
		cluster
			.node(leader)
			.state_machine
			.read_state(|state| state.tenant_keysets.clone())
			.await,
		keysets_before
	);
	// It still replicates and votes: it is a voter, and it applies a later write.
	let voters: std::collections::BTreeSet<u32> = cluster
		.node(leader)
		.state_machine
		.read_state(|state| state.last_membership.membership().voter_ids().collect())
		.await;
	assert!(voters.contains(&stranger));
	let more = write_rows(&cluster, leader, &descriptor, &[(4, "d")]).await;
	let mut all = expected;
	all.extend(more);
	expect_everywhere(&cluster, &descriptor, &all, "with a mismatching member").await;
}

/// DC-276 control C3 (Codex 01a1f7b7), test-only, the pre-fix observation: a
/// missed durable rebuild on a healthy tiered store. Store S holds a verified
/// tiered slab, so its open skips the durable index rebuild (`open.rs:1241`,
/// pending tier verification), and the Ready publication later clears that
/// pending set without a rebuild (`tier_authority.rs:1609`). A refused version
/// v's child publication failed on S's local active slab, so v's bytes are
/// durable with no record row, and v has exact `RetiredIntent` debt. The sweep is
/// held across the restart (`hold_cleanup_at_start_for_test`). After the reopen
/// and tier verification: no durable record of v, and v's recovered local row,
/// with its manifest digest, in memory. The sweep is then released. The intended
/// invariant: v's exact debt is not retired while its copy is indexed in memory.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn dc276_a_tiered_stores_skipped_rebuild_keeps_the_debt_of_a_copy_only_in_memory() {
	use crate::network::data_server::child_writes::persist_pause;
	use crate::storage::index::certified_base::{DebtSource, VersionKey};
	use std::sync::atomic::Ordering::Release;
	let (mut cluster, leader, descriptor, _root) = offload_cluster("dc276").await;
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	let tenant = TenantId::ZERO;
	let batch = named_rows(&descriptor, &[(81, "refused")]);
	let store = cluster
		.node(leader)
		.data_server
		.primary_for_test(tenant, &descriptor, &batch)
		.await
		.unwrap();
	let wait = |what: &'static str| {
		let deadline = tokio::time::Instant::now() + BOUND;
		move || assert!(tokio::time::Instant::now() < deadline, "never: {what}")
	};

	// A row of the table, certified and tiered on S: a verified tiered slab.
	write_rows(&cluster, leader, &descriptor, &[(1, "a")]).await;
	jump_floor(&cluster, now + 60_000).await;
	let index = cluster.node(store).slab.persistent_index().unwrap();
	let check = wait("S activated generation 1");
	while index.base_meta().await.unwrap().active_gid < 1 {
		check();
		tokio::time::sleep(TICK).await;
	}
	let tiered = cluster.node(store).slab.force_seal_active_for_test().await;
	until_offloaded(&cluster, store, tiered, "S's tiered slab").await;

	// The DC-276 failure schedule for v on S's local active slab.
	let slab = cluster.node(store).slab.clone();
	let server = cluster.node(store).data_server.clone();
	let incarnation = index.store_incarnation();
	let after = persist_pause::arm_after_begin(&slab, descriptor.table_object_id);
	let meta = write_meta(&descriptor, 1, uuid_v7_at(now + 60_001));
	let writer = server.clone();
	let writing = tokio::spawn(async move { writer.table_write_for_test(meta, batch).await });
	tokio::time::timeout(BOUND, after.reached.notified())
		.await
		.expect("premise: the worker began its intent");
	let record = record_of(&cluster, key_of(&descriptor, &meta))
		.await
		.expect("premise: the parent is reserved");
	let rsv = record.rsv;
	let version = after.manifest()[0];
	let version_key = VersionKey {
		tenant,
		table: version.table,
		key_identity: version.key_identity,
		hlc: version.hlc,
	};
	abort(&cluster, &record).await;
	server
		.decision_peer_for_test()
		.pause_cleanup
		.store(true, Release);
	index.fail_next_delta.store(true, Release);
	after.release.notify_one();
	tokio::time::timeout(BOUND, after.ended.notified())
		.await
		.expect("premise: the worker's append block ended");
	let _ = tokio::time::timeout(BOUND, writing)
		.await
		.expect("the write ends");
	assert_eq!(
		slab.tier_authority_readiness_reason().as_deref(),
		Some("TierAuthorityPublicationPending(RestartRequired)"),
		"premise: the fault hit this child's row publication"
	);
	jump_floor(&cluster, now + 120_000).await;
	let check = wait("S activated a generation covering the reservation");
	while index.base_meta().await.unwrap().active_gid < 2 {
		check();
		tokio::time::sleep(TICK).await;
	}
	let exact_debt = |index: Arc<crate::storage::index::PersistentLocalIndex>| async move {
		index
			.debt_page(None, 1 << 20)
			.await
			.unwrap()
			.into_iter()
			.filter(|(key, record)| *key == version_key && record.row_digest == version.row_digest)
			.collect::<Vec<_>>()
	};
	let debt = exact_debt(index.clone()).await;
	assert_eq!(debt.len(), 1, "premise: v's exact debt");
	assert_eq!(
		debt[0].1.source,
		DebtSource::RetiredIntent { rsv, ordinal: 0 }
	);
	drop((index, slab, server));

	// The restart, with the sweep held from start.
	cluster
		.configs
		.get_mut(&store)
		.unwrap()
		.hold_cleanup_at_start_for_test = true;
	cluster.stop(store, false).await;
	cluster.reopen(store).await;
	let slab = cluster.node(store).slab.clone();
	let index = slab.persistent_index().unwrap();
	assert_eq!(
		index.store_incarnation(),
		incarnation,
		"premise: the same store"
	);
	assert!(
		slab.tier_pointer(tiered).await.is_some(),
		"premise: S holds a tiered slab"
	);
	assert_eq!(
		exact_debt(index.clone()).await.len(),
		1,
		"premise: the debt survived the restart"
	);
	until_ready(&cluster, store, "S's tier verification").await;

	// After verification: no durable record of v; v's recovered row in memory.
	let durable = index
		.owned_version_copies(tenant, vec![version])
		.await
		.unwrap();
	let identity = crate::schema::Identity::new(version.key_identity);
	let locations = slab
		.version_locations(tenant, identity, version.hlc)
		.unwrap();
	let rows = slab
		.stored_rows_at_version(tenant, identity, version.hlc, None)
		.await
		.unwrap();
	let digests: Vec<_> = rows
		.iter()
		.map(|row| crate::network::data_server::child_writes::stored_row_digest(&descriptor, row))
		.collect();
	eprintln!(
		"DC-276 C3 after verification: durable_prefix_records={durable:?} in_memory={locations:?} digests={digests:?} tiered_slab={tiered:?}"
	);
	assert_eq!(
		durable,
		vec![0],
		"premise: no RECORDS_TABLE row for v (rebuild skipped)"
	);
	assert!(
		locations.iter().all(|location| location.slab_id != tiered),
		"premise: v's copy is on a local slab"
	);
	assert!(
		digests.iter().any(|digest| digest
			.as_ref()
			.is_ok_and(|digest| *digest == version.row_digest)),
		"premise: v's recovered row, with its manifest digest, is in memory"
	);

	// The ordinary sweep, released.
	let server = cluster.node(store).data_server.clone();
	// The sweep acts only once the base is validated (`may_clean`); wait for it so
	// a skipped sweep is not mistaken for a kept debt.
	let validated = server.base_validated_for_test();
	let check = wait("S's base validated after the reopen");
	while !validated.load(std::sync::atomic::Ordering::Acquire) {
		check();
		tokio::time::sleep(TICK).await;
	}
	// Only the sweep is released: local certified cleanup stays held until the
	// sweep's outcome is observed with the copy still present.
	server
		.decision_peer_for_test()
		.pause_local_cleanup
		.store(true, Release);
	server
		.decision_peer_for_test()
		.pause_cleanup
		.store(false, Release);
	let swept = server
		.debt_sweep_step()
		.await
		.expect("the sweep pass completes");
	let debt_after = exact_debt(index.clone()).await;
	let in_memory_after = slab
		.version_locations(tenant, identity, version.hlc)
		.unwrap();
	eprintln!(
		"DC-276 C3 sweep pass: retired={swept} exact_debt={debt_after:?} in_memory={in_memory_after:?}"
	);
	assert_eq!(
		in_memory_after.len(),
		1,
		"premise: v's copy is still indexed in memory"
	);
	assert_eq!(
		debt_after.len(),
		1,
		"v's exact debt is kept while its copy is indexed in memory"
	);
	server
		.decision_peer_for_test()
		.pause_local_cleanup
		.store(false, Release);

	// Eventual: ordinary certified cleanup erases the local copy, then the sweep
	// retires the debt.
	slab.force_seal_active_for_test().await;
	let check = wait("cleanup erased v's local copy, then its debt retired");
	loop {
		let _ = server.debt_sweep_step().await;
		let copy = slab
			.version_locations(tenant, identity, version.hlc)
			.unwrap();
		let durable = index
			.owned_version_copies(tenant, vec![version])
			.await
			.unwrap();
		let debt = exact_debt(index.clone()).await;
		if copy.is_empty() && durable == vec![0] && debt.is_empty() {
			break;
		}
		check();
		tokio::time::sleep(TICK).await;
	}
	eprintln!("DC-276 C3 eventual: the copy was erased and the debt retired");
}

/// Wendelon ADR 0132 K10: a follower promotes its own offloaded slab by
/// ordinary reads, then `Node::shutdown` closes its storage with the slab
/// manager alive. The close disposes of the registered copy and records a
/// clean shutdown; the reopened node finds no file at the slab's path,
/// quarantines nothing, and promotes the slab again by ordinary reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_node_shutdown_disposes_of_a_promoted_copy_and_the_reopened_node_promotes_again() {
	let (mut cluster, leader, descriptor, _root) = offload_cluster("close-disposal").await;
	let follower = followers(leader)[0];
	let ids = [1i64];
	write_rows(&cluster, leader, &descriptor, &[(1, "a")]).await;
	let frames = frames_at(&cluster, follower, &descriptor, &ids).await;
	let id = cluster
		.node(follower)
		.slab
		.force_seal_active_for_test()
		.await;
	until_offloaded(&cluster, follower, id, "before the promotion").await;
	until_no_intents(&cluster, follower, "before the promotion").await;
	let threshold = cluster.configs[&follower]
		.rehydration
		.promote_after_l2_reads as usize;
	assert!(threshold >= 1, "premise: promotion is enabled");
	for n in 1..=threshold {
		assert_eq!(
			frames_at(&cluster, follower, &descriptor, &ids).await,
			frames,
			"read {n} before the shutdown"
		);
	}
	let slab = &cluster.node(follower).slab;
	assert!(
		slab.is_rehydrated(id).await,
		"premise: ordinary reads registered a copy"
	);
	assert!(
		slab.slab_file_exists_for_test(id),
		"premise: the copy's file"
	);
	assert!(
		cluster.close(follower).await,
		"the shutdown recorded a clean close"
	);
	cluster.reopen(follower).await;
	until_ready(&cluster, follower, "after the reopen").await;
	let slab = &cluster.node(follower).slab;
	assert!(
		!slab.slab_file_exists_for_test(id),
		"the shutdown disposed of the copy's file"
	);
	let report = slab.quarantined_slab_files();
	assert!(
		report.files.iter().all(|file| file.slab_id != id),
		"the reopened node quarantines nothing of the slab: {report:?}"
	);
	assert!(!slab.is_rehydrated(id).await, "no copy before the reads");
	for n in 1..=threshold {
		assert_eq!(
			frames_at(&cluster, follower, &descriptor, &ids).await,
			frames,
			"read {n} after the reopen"
		);
	}
	let slab = &cluster.node(follower).slab;
	assert!(
		slab.is_rehydrated(id).await,
		"the reopened node promotes the slab again"
	);
	assert_eq!(slab.quarantined_slab_files().rehydrations_skipped, 0);
}
