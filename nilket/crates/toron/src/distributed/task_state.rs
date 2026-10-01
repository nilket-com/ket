use std::collections::{HashMap, HashSet};

use crate::distributed::identity::{AttemptId, QueryId, StageId, TaskId, TaskIdentity, WorkerId};
use crate::error::TaskFailureKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskKey {
	pub query_id: QueryId,
	pub stage_id: StageId,
	pub task_id: TaskId,
}

impl TaskKey {
	pub const fn new(query_id: QueryId, stage_id: StageId, task_id: TaskId) -> Self {
		Self {
			query_id,
			stage_id,
			task_id,
		}
	}

	pub const fn from_identity(identity: TaskIdentity) -> Self {
		Self {
			query_id: identity.query_id,
			stage_id: identity.stage_id,
			task_id: identity.task_id,
		}
	}

	pub const fn attempt(self, attempt_id: AttemptId) -> TaskIdentity {
		TaskIdentity::new(self.query_id, self.stage_id, self.task_id, attempt_id)
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttemptState {
	Pending,
	Dispatched { worker_id: WorkerId },
	Running { worker_id: WorkerId },
	Completed { worker_id: WorkerId },
	FailedRetryable { kind: TaskFailureKind },
	FailedTerminal { kind: TaskFailureKind },
	Cancelled,
}

impl AttemptState {
	pub const fn is_terminal(&self) -> bool {
		matches!(
			self,
			Self::Completed { .. }
				| Self::FailedRetryable { .. }
				| Self::FailedTerminal { .. }
				| Self::Cancelled
		)
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcceptanceState {
	Open,
	LiveAccepted {
		attempt_id: AttemptId,
		worker_id: WorkerId,
		drained: bool,
	},
	Invalidated {
		attempt_id: AttemptId,
		worker_id: WorkerId,
	},
	Failed {
		kind: TaskFailureKind,
	},
	Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidatedTask {
	pub key: TaskKey,
	pub invalidated_attempt: AttemptId,
	pub replacement_attempt: AttemptId,
	pub worker_id: WorkerId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct CancelledTaskCount(pub usize);

impl CancelledTaskCount {
	pub const fn new(value: usize) -> Self {
		Self(value)
	}

	pub const fn get(self) -> usize {
		self.0
	}
}

#[derive(Debug, Default)]
pub struct TaskStateStore {
	attempts: HashMap<TaskIdentity, AttemptState>,
	acceptance: HashMap<TaskKey, AcceptanceState>,
	invalidated_attempts: HashSet<TaskIdentity>,
}

impl TaskStateStore {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn spawn_attempt(&mut self, identity: TaskIdentity) {
		self.attempts.insert(identity, AttemptState::Pending);
		self.acceptance
			.entry(TaskKey::from_identity(identity))
			.or_insert(AcceptanceState::Open);
	}

	pub fn set_attempt_state(&mut self, identity: TaskIdentity, state: AttemptState) {
		self.attempts.insert(identity, state);
	}

	pub fn attempt_state(&self, identity: TaskIdentity) -> Option<&AttemptState> {
		self.attempts.get(&identity)
	}

	pub fn running_attempt(&self, key: TaskKey) -> Option<(TaskIdentity, WorkerId)> {
		self.attempts
			.iter()
			.filter_map(|(identity, state)| {
				// ADR 626: a superseded attempt is never a running input, even while its local
				// future and its replacement coexist.
				if TaskKey::from_identity(*identity) == key
					&& !self.invalidated_attempts.contains(identity)
					&& let AttemptState::Running { worker_id } = state
				{
					Some((*identity, *worker_id))
				} else {
					None
				}
			})
			.min_by_key(|(identity, _)| identity.attempt_id)
	}

	pub fn pending_attempt(&self, key: TaskKey) -> Option<TaskIdentity> {
		self.attempts
			.iter()
			.filter(|(identity, state)| {
				TaskKey::from_identity(**identity) == key && matches!(state, AttemptState::Pending)
			})
			.map(|(identity, _)| *identity)
			.min_by_key(|identity| identity.attempt_id)
	}

	pub fn acceptance_state(&self, key: TaskKey) -> AcceptanceState {
		self.acceptance
			.get(&key)
			.cloned()
			.unwrap_or(AcceptanceState::Open)
	}

	pub fn accept_completed(
		&mut self,
		identity: TaskIdentity,
		worker_id: WorkerId,
	) -> Result<(), AcceptanceError> {
		if self.invalidated_attempts.contains(&identity) {
			return Err(AcceptanceError::StaleAttempt);
		}

		match self.attempts.get(&identity) {
			Some(AttemptState::Completed {
				worker_id: completed_worker,
			}) if *completed_worker == worker_id => {}
			Some(_) => return Err(AcceptanceError::AttemptNotCompleted),
			None => return Err(AcceptanceError::UnknownAttempt),
		}

		let key = TaskKey::from_identity(identity);
		match self.acceptance_state(key) {
			AcceptanceState::Open => {
				self.acceptance.insert(
					key,
					AcceptanceState::LiveAccepted {
						attempt_id: identity.attempt_id,
						worker_id,
						drained: false,
					},
				);
				Ok(())
			}
			AcceptanceState::Invalidated { .. } => Err(AcceptanceError::StaleAttempt),
			AcceptanceState::LiveAccepted { .. }
			| AcceptanceState::Failed { .. }
			| AcceptanceState::Cancelled => Err(AcceptanceError::NotOpen),
		}
	}

	pub fn mark_drained(&mut self, key: TaskKey) -> Result<(), AcceptanceError> {
		match self.acceptance.get_mut(&key) {
			Some(AcceptanceState::LiveAccepted { drained, .. }) => {
				*drained = true;
				Ok(())
			}
			Some(_) => Err(AcceptanceError::NotLiveAccepted),
			None => Err(AcceptanceError::NotLiveAccepted),
		}
	}

	pub fn fail_task(&mut self, key: TaskKey, kind: TaskFailureKind) {
		self.acceptance
			.insert(key, AcceptanceState::Failed { kind });
	}

	pub fn cancel_task(&mut self, key: TaskKey) {
		self.acceptance.insert(key, AcceptanceState::Cancelled);
	}

	pub fn cancel_query(&mut self, query_id: QueryId) -> CancelledTaskCount {
		let keys = self
			.acceptance
			.keys()
			.filter(|key| key.query_id == query_id)
			.copied()
			.collect::<Vec<_>>();
		for key in &keys {
			self.acceptance.insert(*key, AcceptanceState::Cancelled);
		}
		for (identity, state) in &mut self.attempts {
			if identity.query_id == query_id && !state.is_terminal() {
				*state = AttemptState::Cancelled;
			}
		}
		CancelledTaskCount::new(keys.len())
	}

	pub fn reopen_invalidated(&mut self, key: TaskKey) -> Result<(), AcceptanceError> {
		match self.acceptance_state(key) {
			AcceptanceState::Invalidated { .. } => {
				if self.pending_attempt(key).is_none() {
					return Err(AcceptanceError::UnknownAttempt);
				}
				self.acceptance.insert(key, AcceptanceState::Open);
				Ok(())
			}
			AcceptanceState::Open => Ok(()),
			AcceptanceState::LiveAccepted { .. } => Err(AcceptanceError::NotOpen),
			AcceptanceState::Failed { .. } | AcceptanceState::Cancelled => {
				Err(AcceptanceError::NotOpen)
			}
		}
	}

	pub fn invalidate_task(&mut self, key: TaskKey) -> Option<InvalidatedTask> {
		let state = self.acceptance_state(key);
		let AcceptanceState::LiveAccepted {
			attempt_id,
			worker_id,
			drained,
		} = state
		else {
			return None;
		};

		if drained {
			return None;
		}

		let replacement_attempt = attempt_id.next();
		self.invalidated_attempts.insert(key.attempt(attempt_id));
		self.acceptance.insert(
			key,
			AcceptanceState::Invalidated {
				attempt_id,
				worker_id,
			},
		);
		let replacement_identity = key.attempt(replacement_attempt);
		self.attempts
			.insert(replacement_identity, AttemptState::Pending);
		Some(InvalidatedTask {
			key,
			invalidated_attempt: attempt_id,
			replacement_attempt,
			worker_id,
		})
	}

	/// ADR 626: classify an attempt's outcome without mutating anything. Only `Current` may be
	/// applied; `Stale` is ignored; `Unknown` and `WrongWorker` are protocol violations.
	pub fn outcome_disposition(
		&self,
		identity: TaskIdentity,
		worker_id: WorkerId,
	) -> OutcomeDisposition {
		let Some(state) = self.attempts.get(&identity) else {
			return OutcomeDisposition::Unknown;
		};
		match state {
			AttemptState::Pending => return OutcomeDisposition::Unknown,
			AttemptState::Dispatched {
				worker_id: running_on,
			}
			| AttemptState::Running {
				worker_id: running_on,
			} => {
				if *running_on != worker_id {
					return OutcomeDisposition::WrongWorker;
				}
			}
			_ => return OutcomeDisposition::Stale,
		}
		if self.invalidated_attempts.contains(&identity) {
			return OutcomeDisposition::Stale;
		}
		let key = TaskKey::from_identity(identity);
		let superseded = self.attempts.keys().any(|other| {
			TaskKey::from_identity(*other) == key && other.attempt_id > identity.attempt_id
		});
		if superseded {
			return OutcomeDisposition::Stale;
		}
		match self.acceptance_state(key) {
			AcceptanceState::Open => OutcomeDisposition::Current,
			_ => OutcomeDisposition::Stale,
		}
	}

	/// ADR 626: supersede a running attempt whose inputs were invalidated. It is retired (no longer
	/// `Running`, so never a running input), its late outcome becomes `Stale`, and a pending
	/// replacement attempt is created unless one exists. Acceptance stays open.
	pub fn supersede_attempt(&mut self, identity: TaskIdentity) -> Option<TaskIdentity> {
		if !matches!(
			self.attempts.get(&identity),
			Some(AttemptState::Running { .. } | AttemptState::Dispatched { .. })
		) {
			return None;
		}
		self.invalidated_attempts.insert(identity);
		self.attempts.insert(identity, AttemptState::Cancelled);
		let key = TaskKey::from_identity(identity);
		let replacement = key.attempt(identity.attempt_id.next());
		self.attempts
			.entry(replacement)
			.or_insert(AttemptState::Pending);
		Some(replacement)
	}

	/// ADR 626: invalidate one query's accepted producers on `worker_id`, leaving every other
	/// query's state alone.
	pub fn invalidate_worker_in_query(
		&mut self,
		query_id: QueryId,
		worker_id: WorkerId,
	) -> Vec<InvalidatedTask> {
		let mut keys = self
			.acceptance
			.iter()
			.filter(|(key, state)| {
				key.query_id == query_id
					&& matches!(state, AcceptanceState::LiveAccepted { worker_id: accepted, .. } if *accepted == worker_id)
			})
			.map(|(key, _)| *key)
			.collect::<Vec<_>>();
		keys.sort();
		keys.into_iter()
			.filter_map(|key| self.invalidate_task(key))
			.collect()
	}

	pub fn invalidate_worker(&mut self, worker_id: WorkerId) -> Vec<InvalidatedTask> {
		let mut invalidated = Vec::new();
		let keys = self.acceptance.keys().copied().collect::<Vec<_>>();

		for key in keys {
			let AcceptanceState::LiveAccepted {
				worker_id: accepted_worker,
				..
			} = self.acceptance_state(key)
			else {
				continue;
			};

			if accepted_worker == worker_id
				&& let Some(invalidated_task) = self.invalidate_task(key)
			{
				invalidated.push(invalidated_task);
			};
		}

		invalidated
	}
}

/// ADR 626: what a dispatched attempt's outcome is, decided before any state changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutcomeDisposition {
	/// The attempt is the task's current, running attempt on this worker: apply the outcome.
	Current,
	/// Expected and harmless: the attempt was invalidated or superseded, already finished (a
	/// duplicate), or its task was closed. Ignore it without mutating anything.
	Stale,
	/// A protocol violation: no such attempt was ever dispatched.
	Unknown,
	/// A protocol violation: the attempt runs on another worker.
	WrongWorker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcceptanceError {
	UnknownAttempt,
	AttemptNotCompleted,
	NotOpen,
	NotLiveAccepted,
	StaleAttempt,
}

#[cfg(test)]
mod tests {
	use super::*;

	fn identity(attempt_id: AttemptId) -> TaskIdentity {
		TaskIdentity::new(
			QueryId::from_u128(1),
			StageId::new(2),
			TaskId::new(3),
			attempt_id,
		)
	}

	#[test]
	fn outcome_disposition_separates_current_stale_and_violations() {
		let worker = WorkerId::from_u128(9);
		let other = WorkerId::from_u128(8);
		let mut state = TaskStateStore::new();
		let first = identity(AttemptId::first());
		assert_eq!(
			state.outcome_disposition(first, worker),
			OutcomeDisposition::Unknown
		);
		state.spawn_attempt(first);
		assert_eq!(
			state.outcome_disposition(first, worker),
			OutcomeDisposition::Unknown
		);
		state.set_attempt_state(first, AttemptState::Running { worker_id: worker });
		assert_eq!(
			state.outcome_disposition(first, worker),
			OutcomeDisposition::Current
		);
		assert_eq!(
			state.outcome_disposition(first, other),
			OutcomeDisposition::WrongWorker
		);
		// A superseded running attempt is stale; its replacement is pending, not yet current.
		let replacement = state.supersede_attempt(first).expect("supersede");
		assert_eq!(
			state.outcome_disposition(first, worker),
			OutcomeDisposition::Stale
		);
		assert_eq!(
			state.outcome_disposition(replacement, worker),
			OutcomeDisposition::Unknown
		);
		state.set_attempt_state(replacement, AttemptState::Running { worker_id: other });
		assert_eq!(
			state.outcome_disposition(replacement, other),
			OutcomeDisposition::Current
		);
		// A finished attempt reporting again is a harmless duplicate.
		state.set_attempt_state(replacement, AttemptState::Completed { worker_id: other });
		assert_eq!(
			state.outcome_disposition(replacement, other),
			OutcomeDisposition::Stale
		);
	}

	#[test]
	fn a_closed_task_makes_its_running_attempt_stale() {
		let worker = WorkerId::from_u128(9);
		let mut state = TaskStateStore::new();
		let first = identity(AttemptId::first());
		state.spawn_attempt(first);
		state.set_attempt_state(first, AttemptState::Running { worker_id: worker });
		state.fail_task(TaskKey::from_identity(first), TaskFailureKind::Cancelled);
		assert_eq!(
			state.outcome_disposition(first, worker),
			OutcomeDisposition::Stale
		);
		assert_eq!(
			state.supersede_attempt(identity(AttemptId::first().next())),
			None
		);
	}

	#[test]
	fn producer_loss_invalidates_acceptance_without_rewriting_completed_attempt() {
		let worker_id = WorkerId::from_u128(9);
		let attempt = identity(AttemptId::first());
		let mut store = TaskStateStore::new();
		store.spawn_attempt(attempt);
		store.set_attempt_state(attempt, AttemptState::Completed { worker_id });
		store
			.accept_completed(attempt, worker_id)
			.expect("accept completed attempt");

		let invalidated = store.invalidate_worker(worker_id);

		assert_eq!(
			invalidated,
			vec![InvalidatedTask {
				key: TaskKey::from_identity(attempt),
				invalidated_attempt: AttemptId::first(),
				replacement_attempt: AttemptId::new(1),
				worker_id,
			}]
		);
		assert_eq!(
			store.attempt_state(attempt),
			Some(&AttemptState::Completed { worker_id })
		);
		assert_eq!(
			store.attempt_state(identity(AttemptId::new(1))),
			Some(&AttemptState::Pending)
		);
		assert_eq!(
			store.acceptance_state(TaskKey::from_identity(attempt)),
			AcceptanceState::Invalidated {
				attempt_id: AttemptId::first(),
				worker_id,
			}
		);
	}

	#[test]
	fn stale_completed_attempt_rejected_after_invalidation() {
		let worker_id = WorkerId::from_u128(9);
		let stale_attempt = identity(AttemptId::first());
		let replacement = identity(AttemptId::new(1));
		let mut store = TaskStateStore::new();
		store.spawn_attempt(stale_attempt);
		store.set_attempt_state(stale_attempt, AttemptState::Completed { worker_id });
		store
			.accept_completed(stale_attempt, worker_id)
			.expect("accept completed attempt");

		store.invalidate_worker(worker_id);

		assert_eq!(
			store.accept_completed(stale_attempt, worker_id),
			Err(AcceptanceError::StaleAttempt)
		);

		store
			.reopen_invalidated(TaskKey::from_identity(stale_attempt))
			.expect("reopen replacement attempt");
		store.set_attempt_state(replacement, AttemptState::Completed { worker_id });
		store
			.accept_completed(replacement, worker_id)
			.expect("replacement attempt accepted");
	}

	#[test]
	fn invalidation_exposes_replacement_pending_attempt() {
		let worker_id = WorkerId::from_u128(9);
		let stale_attempt = identity(AttemptId::first());
		let key = TaskKey::from_identity(stale_attempt);
		let mut store = TaskStateStore::new();
		store.spawn_attempt(stale_attempt);
		store.set_attempt_state(stale_attempt, AttemptState::Completed { worker_id });
		store
			.accept_completed(stale_attempt, worker_id)
			.expect("accept completed attempt");

		store.invalidate_worker(worker_id);

		assert_eq!(
			store.pending_attempt(key),
			Some(identity(AttemptId::new(1)))
		);
	}

	#[test]
	fn explicit_task_invalidation_records_invalidated_state() {
		let worker_id = WorkerId::from_u128(9);
		let attempt = identity(AttemptId::first());
		let key = TaskKey::from_identity(attempt);
		let mut store = TaskStateStore::new();
		store.spawn_attempt(attempt);
		store.set_attempt_state(attempt, AttemptState::Completed { worker_id });
		store
			.accept_completed(attempt, worker_id)
			.expect("accept completed attempt");

		let invalidated = store.invalidate_task(key).expect("invalidate");

		assert_eq!(
			invalidated,
			InvalidatedTask {
				key,
				invalidated_attempt: AttemptId::first(),
				replacement_attempt: AttemptId::new(1),
				worker_id,
			}
		);
		assert_eq!(
			store.acceptance_state(key),
			AcceptanceState::Invalidated {
				attempt_id: AttemptId::first(),
				worker_id,
			}
		);
		assert_eq!(
			store.pending_attempt(key),
			Some(key.attempt(AttemptId::new(1)))
		);
		store
			.reopen_invalidated(key)
			.expect("reopen replacement attempt");
		assert_eq!(store.acceptance_state(key), AcceptanceState::Open);
	}

	#[test]
	fn explicit_task_invalidation_skips_drained_task() {
		let worker_id = WorkerId::from_u128(9);
		let attempt = identity(AttemptId::first());
		let key = TaskKey::from_identity(attempt);
		let mut store = TaskStateStore::new();
		store.spawn_attempt(attempt);
		store.set_attempt_state(attempt, AttemptState::Completed { worker_id });
		store
			.accept_completed(attempt, worker_id)
			.expect("accept completed attempt");
		store.mark_drained(key).expect("drain");

		assert_eq!(store.invalidate_task(key), None);
		assert_eq!(
			store.acceptance_state(key),
			AcceptanceState::LiveAccepted {
				attempt_id: AttemptId::first(),
				worker_id,
				drained: true,
			}
		);
	}

	#[test]
	fn pending_attempt_uses_lowest_pending_attempt_for_task() {
		let key = TaskKey::from_identity(identity(AttemptId::first()));
		let mut store = TaskStateStore::new();
		store.spawn_attempt(identity(AttemptId::new(2)));
		store.spawn_attempt(identity(AttemptId::new(1)));

		assert_eq!(
			store.pending_attempt(key),
			Some(identity(AttemptId::new(1)))
		);
	}

	#[test]
	fn drained_producer_is_not_invalidated_on_worker_loss() {
		let worker_id = WorkerId::from_u128(9);
		let attempt = identity(AttemptId::first());
		let key = TaskKey::from_identity(attempt);
		let mut store = TaskStateStore::new();
		store.spawn_attempt(attempt);
		store.set_attempt_state(attempt, AttemptState::Completed { worker_id });
		store
			.accept_completed(attempt, worker_id)
			.expect("accept completed attempt");
		store.mark_drained(key).expect("mark drained");

		assert!(store.invalidate_worker(worker_id).is_empty());
		assert_eq!(
			store.acceptance_state(key),
			AcceptanceState::LiveAccepted {
				attempt_id: AttemptId::first(),
				worker_id,
				drained: true,
			}
		);
		assert_eq!(store.attempt_state(identity(AttemptId::new(1))), None);
	}

	#[test]
	fn stage_barrier_reads_acceptance_layer_not_bare_completed_attempt() {
		let worker_id = WorkerId::from_u128(9);
		let attempt = identity(AttemptId::first());
		let key = TaskKey::from_identity(attempt);
		let mut store = TaskStateStore::new();
		store.spawn_attempt(attempt);
		store.set_attempt_state(attempt, AttemptState::Completed { worker_id });

		assert_eq!(
			store.attempt_state(attempt),
			Some(&AttemptState::Completed { worker_id })
		);
		assert_eq!(store.acceptance_state(key), AcceptanceState::Open);

		store
			.accept_completed(attempt, worker_id)
			.expect("accept completed attempt");
		assert_eq!(
			store.acceptance_state(key),
			AcceptanceState::LiveAccepted {
				attempt_id: AttemptId::first(),
				worker_id,
				drained: false,
			}
		);
	}

	#[test]
	fn cancelled_task_closes_acceptance_index() {
		let attempt = identity(AttemptId::first());
		let key = TaskKey::from_identity(attempt);
		let mut store = TaskStateStore::new();
		store.spawn_attempt(attempt);

		store.cancel_task(key);

		assert_eq!(store.acceptance_state(key), AcceptanceState::Cancelled);
	}

	#[test]
	fn cancelled_query_rejects_late_task_completion() {
		let worker_id = WorkerId::from_u128(1);
		let attempt = identity(AttemptId::first());
		let key = TaskKey::from_identity(attempt);
		let mut store = TaskStateStore::new();
		store.spawn_attempt(attempt);
		store.set_attempt_state(attempt, AttemptState::Running { worker_id });

		let cancelled = store.cancel_query(attempt.query_id);

		assert_eq!(cancelled, CancelledTaskCount::new(1));
		assert_eq!(store.acceptance_state(key), AcceptanceState::Cancelled);
		assert_eq!(store.attempt_state(attempt), Some(&AttemptState::Cancelled));

		store.set_attempt_state(attempt, AttemptState::Completed { worker_id });
		assert_eq!(
			store.accept_completed(attempt, worker_id),
			Err(AcceptanceError::NotOpen)
		);
	}
}
