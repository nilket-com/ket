//! Wendelon ADR 0128: the node's frame allowance is charged only while a verification call
//! holds frames, and is returned on every path: a refusal mid-call, and the end of work whose
//! caller was cancelled.

use super::*;

const ROWS: i64 = 48;

async fn bounded<T>(what: &str, f: impl std::future::Future<Output = T>) -> T {
	tokio::time::timeout(std::time::Duration::from_secs(30), f)
		.await
		.unwrap_or_else(|_| panic!("{what}: timed out"))
}

fn child() -> Reserved {
	let rows: Vec<(i64, &str)> = (1..=ROWS).map(|id| (id, "value")).collect();
	reserved(&rows, 1)
}

/// A fence raised between two versions refuses the call; the frames it held are released.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refusal_between_versions_returns_the_calls_charge() {
	let r = child();
	let (_dir, slab) = manager().await;
	let planned = plan(&r, r.holder, r.holder.node, ChildRole::Holder).unwrap();
	persist_child(
		slab.clone(),
		machine(&r.state).await,
		Arc::new(ParentLocks::new()),
		admitted(&slab, planned).await,
		authority(&r, ChildRole::Holder),
	)
	.await
	.expect("the child persists");
	let allowance = slab.verifier_frame_allowance().clone();
	assert_eq!(allowance.used(), 0, "nothing charged between calls");
	let index = slab.persistent_index().unwrap();
	let intent = index.parent_intent(r.rsv, 0).await.unwrap().unwrap();
	let (reached, release) = verify_pause::arm(&slab, (ROWS / 2) as usize);
	let task = {
		let (slab, descriptor) = (slab.clone(), r.descriptor.clone());
		tokio::spawn(async move {
			let index = slab.persistent_index().unwrap();
			verify_owned_rows(&slab, &index, &intent, &descriptor).await
		})
	};
	bounded("the pause between versions", reached.notified()).await;
	let held = allowance.used();
	assert!(held > 0, "the paused call holds frames");
	let fence = SlabId::new(0x7700);
	slab.raise_retirement_fence_for_test(fence);
	release.notify_one();
	let result = bounded("the call", task).await.unwrap();
	slab.clear_retirement_fence_for_test(fence);
	eprintln!(
		"ADR0128 charge: held while paused {held} B, after the refusal {} B -> {result:?}",
		allowance.used()
	);
	assert!(
		matches!(result, Err(ChildRefusal::Unverified(_))),
		"{result:?}"
	);
	assert_eq!(allowance.used(), 0, "the refusal returned the charge");
}

/// A caller cancelled while its spawned persist is inside verification: the work runs on to
/// Complete, and its charge is returned when that work ends, not before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancelled_callers_running_work_returns_its_charge_when_it_ends() {
	let r = child();
	let (_dir, slab) = manager().await;
	let planned = plan(&r, r.holder, r.holder.node, ChildRole::Holder).unwrap();
	let allowance = slab.verifier_frame_allowance().clone();
	let (reached, release) = verify_pause::arm(&slab, (ROWS / 2) as usize);
	let caller = tokio::spawn(persist_child(
		slab.clone(),
		machine(&r.state).await,
		Arc::new(ParentLocks::new()),
		admitted(&slab, planned).await,
		authority(&r, ChildRole::Holder),
	));
	bounded("the pause between versions", reached.notified()).await;
	let held = allowance.used();
	assert!(held > 0, "the paused work holds frames");
	caller.abort();
	let _ = caller.await;
	// The caller is gone; its spawned work is still paused and still charged.
	tokio::time::sleep(std::time::Duration::from_millis(100)).await;
	assert_eq!(
		allowance.used(),
		held,
		"the cancelled caller's work still holds its frames"
	);
	assert_eq!(
		intent_state(&slab, r.rsv).await,
		Some(ParentIntentState::Intent)
	);
	release.notify_one();
	bounded("the work's end", async {
		while intent_state(&slab, r.rsv).await != Some(ParentIntentState::Complete) {
			tokio::time::sleep(std::time::Duration::from_millis(10)).await;
		}
	})
	.await;
	bounded("the charge's return", async {
		while allowance.used() != 0 {
			tokio::time::sleep(std::time::Duration::from_millis(10)).await;
		}
	})
	.await;
	eprintln!(
		"ADR0128 charge: held by a cancelled caller's work {held} B, after its end {} B",
		allowance.used()
	);
}
