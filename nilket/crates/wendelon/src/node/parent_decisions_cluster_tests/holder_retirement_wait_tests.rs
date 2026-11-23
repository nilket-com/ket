//! Wendelon ADR 0126 D3, through the production child loop: one coordinated attempt computes ONE
//! absolute end for its discretionary waiting, shared by every local holder child. A write whose
//! rows span several of the leader's own segments, under a log-indexed retirement fence that never
//! clears, answers within about one budget; a wait per child would take one budget per child.

use super::write_deadline::shut_down;
use super::*;
use crate::schema::SlabId;
use crate::storage::manager::tier_authority::HOLDER_RETIREMENT_WAIT;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn one_attempt_shares_one_wait_end_across_its_local_children() {
	let (cluster, leader, descriptor) = ready().await;
	let server = cluster.node(leader).data_server.clone();
	let slab = cluster.node(leader).slab.clone();
	// Rows whose primary is the leader: each segment of them is a child the leader holds.
	let mut ids = Vec::new();
	for id in 1..4_000i64 {
		let primary = server
			.primary_for_test(TenantId::ZERO, &descriptor, &rows(&descriptor, &[id]))
			.await
			.expect("every position is leased");
		if primary == leader {
			ids.push(id);
			if ids.len() == 48 {
				break;
			}
		}
	}
	let meta = write_meta(&descriptor, ids.len(), uuid_v7());
	let fence = SlabId::new(0x7700);
	slab.raise_retirement_fence_for_test(fence);
	let started = std::time::Instant::now();
	let (answer, _) = server
		.table_write_for_test(meta, rows(&descriptor, &ids))
		.await
		.unwrap();
	let elapsed = started.elapsed();
	slab.clear_retirement_fence_for_test(fence);
	let record = record_of(&cluster, key_of(&descriptor, &meta))
		.await
		.expect("reserved");
	let local = record
		.claim
		.children
		.iter()
		.filter(|child| child.holder.node == leader)
		.count();
	eprintln!(
		"ADR0126 shared wait: {} rows, {} children ({local} local); answer {:?}/{:?}; elapsed {elapsed:?}; wait turns {}",
		ids.len(),
		record.claim.children.len(),
		answer.write.status,
		answer.write.mutation_commit_knowledge(),
		slab.holder_retirement_waits_for_test()
	);
	assert!(
		local >= 3,
		"premise: at least three local children ({local})"
	);
	assert_eq!(
		answer.write.status,
		WriteStatus::WRITE_STALLED,
		"{answer:?}"
	);
	assert!(
		slab.holder_retirement_waits_for_test() > 0,
		"an eligible wait began"
	);
	assert!(
		elapsed < HOLDER_RETIREMENT_WAIT * 2,
		"{elapsed:?}: one end for the attempt, not one per child ({local} local children)"
	);
	shut_down(cluster).await;
}

/// R2: a caller deadline shorter than the cap bounds the holder's wait through `coordinate`.
/// The leader's own holder meets a persistent fence; once its wait is witnessed, the end it was
/// forwarded and the moment its owned work ends are both read. Ignoring the caller's deadline
/// would forward `entry + cap` and end the work only after the cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_shorter_caller_deadline_bounds_the_holders_wait_and_its_owned_work() {
	use super::write_deadline::until_reached;
	const CALLER_MS: u64 = 300;
	assert!(std::time::Duration::from_millis(CALLER_MS) * 3 < HOLDER_RETIREMENT_WAIT);
	let (cluster, leader, descriptor) = ready().await;
	let server = cluster.node(leader).data_server.clone();
	let slab = cluster.node(leader).slab.clone();
	let mut chosen = None;
	for id in 1..400i64 {
		let primary = server
			.primary_for_test(TenantId::ZERO, &descriptor, &rows(&descriptor, &[id]))
			.await
			.expect("every position is leased");
		if primary == leader {
			chosen = Some(id);
			break;
		}
	}
	let id = chosen.expect("a row whose primary is the leader");
	let mut meta = write_meta(&descriptor, 1, uuid_v7());
	meta.deadline_remaining_millis = CALLER_MS;
	let fence = SlabId::new(0x7700);
	slab.raise_retirement_fence_for_test(fence);
	let after = crate::network::data_server::child_writes::persist_pause::arm_after_begin(
		&slab,
		descriptor.table_object_id,
	);
	let started = tokio::time::Instant::now();
	let write = {
		let server = server.clone();
		let batch = rows(&descriptor, &[id]);
		tokio::spawn(async move { server.table_write_for_test(meta, batch).await })
	};
	until_reached("the holder after its begin", &after.reached).await;
	after.release.notify_one();
	tokio::time::timeout(BOUND, slab.holder_retirement_wait_began_for_test(1))
		.await
		.expect("the owned holder is in its eligible wait");
	let forwarded = slab
		.holder_retirement_wait_end_for_test()
		.expect("the wait's forwarded end");
	until_reached("the holder's owned work ended", &after.ended).await;
	let ended = started.elapsed();
	slab.clear_retirement_fence_for_test(fence);
	let (answer, _) = tokio::time::timeout(BOUND, write)
		.await
		.expect("the write answers")
		.unwrap()
		.unwrap();
	let forwarded_after = forwarded.saturating_duration_since(started);
	eprintln!(
		"ADR0126 caller deadline {CALLER_MS} ms, cap {HOLDER_RETIREMENT_WAIT:?}: forwarded end \
		 +{forwarded_after:?}; owned work ended +{ended:?}; answer {:?}/{:?}",
		answer.write.status,
		answer.write.mutation_commit_knowledge(),
	);
	assert!(
		forwarded_after <= std::time::Duration::from_millis(CALLER_MS + 400),
		"the forwarded end is the caller's deadline, not the cap: +{forwarded_after:?}"
	);
	assert!(
		ended < HOLDER_RETIREMENT_WAIT - std::time::Duration::from_millis(100),
		"the owned work ended by the caller's deadline, not the cap: +{ended:?}"
	);
	assert_ne!(answer.write.status, WriteStatus::OK, "{answer:?}");
	shut_down(cluster).await;
}
