//! The gate between a write attempt and its deadline, alone.

use super::{AttemptGate, answer_by_deadline};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

fn past() -> Instant {
	Instant::now() - Duration::from_millis(1)
}

/// Sets its flag when dropped.
struct Dropped(Arc<AtomicBool>);
impl Drop for Dropped {
	fn drop(&mut self) {
		self.0.store(true, Ordering::SeqCst);
	}
}

#[test]
fn a_claim_past_the_deadline_refuses_before_any_timer_ran() {
	// The late poll: the clock is past the deadline and nothing has expired
	// the gate yet.
	let gate = AttemptGate::until(Some(past()));
	assert!(gate.claim().is_none());
	// The refusal is final, whatever a later clock reading says.
	assert!(gate.claim_at(past() - Duration::from_secs(60)).is_none());
	assert!(gate.expire(), "the attempt never claimed");
}

#[test]
fn an_expired_gate_is_never_claimed_and_an_owned_gate_is_never_expired() {
	let far = Instant::now() + Duration::from_secs(3600);
	let expired = AttemptGate::until(Some(far));
	assert!(expired.expire());
	assert!(expired.claim().is_none(), "Expired never becomes Owned");
	assert!(expired.expire());

	let owned = AttemptGate::until(Some(far));
	assert!(owned.claim().is_some());
	assert!(!owned.expire(), "Owned never becomes Expired");
	assert!(owned.claim().is_none(), "one claim per attempt");

	let unbounded = AttemptGate::unbounded();
	assert!(unbounded.claim().is_some());
}

#[tokio::test]
async fn a_ready_result_wins_a_tie_with_the_deadline() {
	let deadline = Instant::now() + Duration::from_millis(40);
	let gate = AttemptGate::until(Some(deadline));
	let answered = AtomicUsize::new(0);
	// The attempt's first poll returns its result, and the deadline is due
	// by the time that poll ends.
	let attempt = async {
		let owned = gate.claim();
		std::thread::sleep(Duration::from_millis(120));
		owned.is_some()
	};
	let result = answer_by_deadline(&gate, attempt, || {
		answered.fetch_add(1, Ordering::SeqCst);
	})
	.await;
	assert!(Instant::now() >= deadline);
	assert_eq!(result, Some(true), "the ready result is the answer");
	assert_eq!(answered.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_deadline_already_past_never_polls_the_attempt() {
	let gate = AttemptGate::until(Some(past()));
	let polled = AtomicBool::new(false);
	let result = answer_by_deadline(
		&gate,
		async { polled.store(true, Ordering::SeqCst) },
		|| panic!("nothing was claimed"),
	)
	.await;
	assert_eq!(result, None);
	assert!(!polled.load(Ordering::SeqCst));
	assert!(gate.claim().is_none());
}

#[tokio::test]
async fn an_attempt_that_has_not_claimed_is_dropped_at_its_deadline() {
	let gate = AttemptGate::until(Some(Instant::now() + Duration::from_millis(40)));
	let dropped = Arc::new(AtomicBool::new(false));
	let guard = Dropped(dropped.clone());
	let result = answer_by_deadline(
		&gate,
		async move {
			let _guard = guard;
			std::future::pending::<()>().await;
		},
		|| panic!("nothing was claimed"),
	)
	.await;
	assert_eq!(result, None);
	assert!(dropped.load(Ordering::SeqCst), "nothing had been claimed");
	assert!(gate.claim().is_none(), "it can never claim afterwards");
}

#[tokio::test]
async fn an_attempt_that_claimed_is_answered_at_its_deadline_and_polled_to_its_end() {
	let gate = AttemptGate::until(Some(Instant::now() + Duration::from_millis(40)));
	let release = Arc::new(tokio::sync::Notify::new());
	let answered = Arc::new(tokio::sync::Notify::new());
	let finished = AtomicBool::new(false);
	let attempt = async {
		let _owned = gate.claim().expect("claimed in time");
		release.notified().await;
		finished.store(true, Ordering::SeqCst);
		7
	};
	let driven = answer_by_deadline(&gate, attempt, || answered.notify_one());
	let observer = async {
		answered.notified().await;
		// The answer is given while the attempt is still held.
		assert!(!finished.load(Ordering::SeqCst));
		release.notify_one();
	};
	let (result, ()) = tokio::join!(driven, observer);
	assert_eq!(result, None, "its own result is not a second answer");
	assert!(finished.load(Ordering::SeqCst), "polled to its end");
}

#[tokio::test]
async fn an_attempt_without_a_deadline_is_only_awaited() {
	let gate = AttemptGate::unbounded();
	let result = answer_by_deadline(
		&gate,
		async {
			tokio::time::sleep(Duration::from_millis(20)).await;
			gate.claim().is_some()
		},
		|| panic!("no deadline"),
	)
	.await;
	assert_eq!(result, Some(true));
}
