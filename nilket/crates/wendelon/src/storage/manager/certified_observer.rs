//! Wendelon ADR 0125: where this store's certified tier work stands, for diagnosis (DC-289).
//!
//! Fixed-size state, per store: how many certified tier steps ended in each outcome, and one
//! coherent snapshot of the observed step (its stage, when that stage was entered, when the step
//! began, its slab, op and generation, and the last observed step's outcome and identity). One
//! step at a time owns the snapshot: a step begun while another is observed is counted only,
//! never replacing it. Every change publishes the whole snapshot to the exported series, which
//! render it from one copy. Nothing reads this state to decide anything.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

/// How one certified tier step ended, in the order a step can stop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum TierStepOutcome {
	/// The data server did not admit a step (not ParentDecisions, or it may not clean).
	NotAdmitted,
	/// Refused: the store is not in ParentDecisions mode.
	NotInMode,
	/// Refused: certified tiered cleanup is fenced on this store.
	Fenced,
	/// Refused: this node does not lead.
	NotLeader,
	/// Refused: no canonical tier state is bound.
	NoCanonicalSource,
	/// No certified debt is visible in the op index.
	NoDebt,
	/// Debt is visible but no tiered slab is eligible.
	NoEligibleSlab,
	/// The chosen slab was no longer eligible once its claim was held.
	ClaimLost,
	/// The selection named nothing to reclaim.
	NothingSelected,
	/// The active generation moved before allocation.
	GenerationMoved,
	/// The step returned `Rewritten` (its certified `Tier` decided Applied and delivered).
	Rewritten,
	/// The step returned `Retired` (its certified `Retire` decided Applied and delivered).
	Retired,
	/// Any other error.
	Failed,
}

impl TierStepOutcome {
	pub(crate) const ALL: [Self; 13] = [
		Self::NotAdmitted,
		Self::NotInMode,
		Self::Fenced,
		Self::NotLeader,
		Self::NoCanonicalSource,
		Self::NoDebt,
		Self::NoEligibleSlab,
		Self::ClaimLost,
		Self::NothingSelected,
		Self::GenerationMoved,
		Self::Rewritten,
		Self::Retired,
		Self::Failed,
	];

	pub(crate) const fn label(self) -> &'static str {
		match self {
			Self::NotAdmitted => "not_admitted",
			Self::NotInMode => "not_in_mode",
			Self::Fenced => "fenced",
			Self::NotLeader => "not_leader",
			Self::NoCanonicalSource => "no_canonical_source",
			Self::NoDebt => "no_debt",
			Self::NoEligibleSlab => "no_eligible_slab",
			Self::ClaimLost => "claim_lost",
			Self::NothingSelected => "nothing_selected",
			Self::GenerationMoved => "generation_moved",
			Self::Rewritten => "rewritten",
			Self::Retired => "retired",
			Self::Failed => "failed",
		}
	}
}

/// Where the observed step stands. Each stage is entered before the work or wait it names.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum TierJobStage {
	/// No step observed.
	#[default]
	Idle,
	/// Admitted: the state snapshot, the debt check and slab selection (no slab yet).
	Selecting,
	/// A slab is chosen: waiting for its exclusive rewrite claim.
	ClaimWaiting,
	/// The claim is held: rechecking eligibility, reading and decoding the published object.
	Reading,
	/// Deciding the selection.
	Deciding,
	/// Building, uploading and reading back a replacement, or staging a retirement.
	Preparing,
	/// Preflight, history reservation and allocation: no op yet.
	Allocating,
	/// The op is allocated: submitting it, awaiting its verified answer and handoff.
	Submitting,
	/// Accepted: installing (or retiring) it on this store.
	Delivering,
	/// Deciding and recording the aged-row credit.
	Crediting,
	/// After the credit: recording the replacement, retaining the superseded object.
	Finishing,
}

impl TierJobStage {
	pub(crate) const ALL: [Self; 11] = [
		Self::Idle,
		Self::Selecting,
		Self::ClaimWaiting,
		Self::Reading,
		Self::Deciding,
		Self::Preparing,
		Self::Allocating,
		Self::Submitting,
		Self::Delivering,
		Self::Crediting,
		Self::Finishing,
	];

	pub(crate) const fn label(self) -> &'static str {
		match self {
			Self::Idle => "idle",
			Self::Selecting => "selecting",
			Self::ClaimWaiting => "claim_waiting",
			Self::Reading => "reading",
			Self::Deciding => "deciding",
			Self::Preparing => "preparing",
			Self::Allocating => "allocating",
			Self::Submitting => "submitting",
			Self::Delivering => "delivering",
			Self::Crediting => "crediting",
			Self::Finishing => "finishing",
		}
	}
}

/// The last completed observed step that obtained an op: how it ended and its identity (0
/// where not known). Op-less steps never replace it; a later op-bearing step does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TierStepLast {
	pub(crate) outcome: TierStepOutcome,
	pub(crate) slab: u64,
	pub(crate) op_seq: u64,
	pub(crate) active_gid: u64,
	pub(crate) duration_ms: u64,
	pub(crate) finished_unix_ms: u64,
}

/// Wendelon ADR 0125: one coherent, fixed-size view of the observed step, rendered whole.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct TierStepSnapshot {
	pub(crate) stage: TierJobStage,
	pub(crate) stage_entered_unix_ms: u64,
	pub(crate) started_unix_ms: u64,
	pub(crate) slab: u64,
	pub(crate) op_seq: u64,
	pub(crate) active_gid: u64,
	pub(crate) last: Option<TierStepLast>,
}

/// The observed step's ownership of the snapshot. `None` where a step is not observed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StepToken(u64);

#[derive(Default)]
pub(crate) struct Owned {
	/// The observing step's token; 0 when none.
	owner: u64,
	next: u64,
	snapshot: TierStepSnapshot,
}

/// Wendelon ADR 0125: the per-store observer (fixed size).
#[derive(Default)]
pub(crate) struct CertifiedTierObserver {
	outcomes: [AtomicU64; 13],
	owned: std::sync::Mutex<Owned>,
}

fn now_unix_ms() -> u64 {
	std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.map_or(0, |elapsed| {
			u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
		})
}

impl CertifiedTierObserver {
	/// An admitted step begins: it is observed at `Selecting` when no other step is, and gets
	/// the token its later transitions need. `None` while another step is observed.
	pub(crate) fn begin_step(&self) -> Option<StepToken> {
		let mut owned = self.owned.lock().unwrap_or_else(|e| e.into_inner());
		if owned.owner != 0 {
			return None;
		}
		owned.next += 1;
		owned.owner = owned.next;
		let now = now_unix_ms();
		let last = owned.snapshot.last;
		owned.snapshot = TierStepSnapshot {
			stage: TierJobStage::Selecting,
			stage_entered_unix_ms: now,
			started_unix_ms: now,
			last,
			..TierStepSnapshot::default()
		};
		publish(&owned.snapshot);
		Some(StepToken(owned.owner))
	}

	/// Changes the observed step's snapshot, when `step` is the observed one.
	fn update(&self, step: Option<StepToken>, change: impl FnOnce(&mut TierStepSnapshot)) {
		let Some(StepToken(token)) = step else {
			return;
		};
		let mut owned = self.owned.lock().unwrap_or_else(|e| e.into_inner());
		if owned.owner == token {
			change(&mut owned.snapshot);
			publish(&owned.snapshot);
		}
	}

	/// The observed step enters `stage`, before the work or wait it names.
	pub(crate) fn enter(&self, step: Option<StepToken>, stage: TierJobStage) {
		self.update(step, |snapshot| {
			snapshot.stage = stage;
			snapshot.stage_entered_unix_ms = now_unix_ms();
		});
	}

	/// The observed step chose `slab` and now waits for its claim.
	pub(crate) fn claim_waiting(&self, step: Option<StepToken>, slab: u64) {
		self.update(step, |snapshot| {
			snapshot.slab = slab;
			snapshot.stage = TierJobStage::ClaimWaiting;
			snapshot.stage_entered_unix_ms = now_unix_ms();
		});
	}

	/// The observed step's active generation, once its selection names it.
	pub(crate) fn note_active_gid(&self, step: Option<StepToken>, gid: u64) {
		self.update(step, |snapshot| snapshot.active_gid = gid);
	}

	/// The observed step's certified op is allocated: it is submitting it.
	pub(crate) fn submitting(&self, step: Option<StepToken>, op_seq: u64) {
		self.update(step, |snapshot| {
			snapshot.op_seq = op_seq;
			snapshot.stage = TierJobStage::Submitting;
			snapshot.stage_entered_unix_ms = now_unix_ms();
		});
	}

	/// A step that is not observed ended with `outcome`: counted only.
	pub(crate) fn count(&self, outcome: TierStepOutcome) {
		self.outcomes[outcome as usize].fetch_add(1, Relaxed);
		crate::metrics::global().observe_certified_tier_step(outcome.label());
	}

	/// A step ended with `outcome`: counted; when it is the observed step, the snapshot returns
	/// to `Idle`, and if it obtained an op it becomes the last step (see the retention rule
	/// below).
	pub(crate) fn end_step(&self, step: Option<StepToken>, outcome: TierStepOutcome) {
		self.count(outcome);
		let Some(StepToken(token)) = step else {
			return;
		};
		let mut owned = self.owned.lock().unwrap_or_else(|e| e.into_inner());
		if owned.owner != token {
			return;
		}
		let now = now_unix_ms();
		let current = owned.snapshot;
		// The last slot keeps the last completed observed step that obtained an op (op_seq
		// recorded at allocation, failures after allocation included): an op-less step (no debt,
		// nothing selected, a refusal, a failure before allocation) leaves it as it was, so the
		// one identity tying a delivery to its install, credit and retention survives the idle
		// ticks between samples. A later op-bearing step replaces it.
		let last = if current.op_seq != 0 {
			Some(TierStepLast {
				outcome,
				slab: current.slab,
				op_seq: current.op_seq,
				active_gid: current.active_gid,
				duration_ms: now.saturating_sub(current.started_unix_ms),
				finished_unix_ms: now,
			})
		} else {
			current.last
		};
		owned.snapshot = TierStepSnapshot {
			stage: TierJobStage::Idle,
			stage_entered_unix_ms: now,
			last,
			..TierStepSnapshot::default()
		};
		owned.owner = 0;
		publish(&owned.snapshot);
	}

	/// DC-292 (Codex 01a206ae): the current snapshot, one coherent copy, WITHOUT waiting: `None`
	/// while another holder (a step publishing its stage) has the lock. For a close's drain
	/// timeout, which must not wait before it requests an abort.
	pub(crate) fn try_snapshot(&self) -> Option<TierStepSnapshot> {
		match self.owned.try_lock() {
			Ok(owned) => Some(owned.snapshot),
			Err(std::sync::TryLockError::Poisoned(poisoned)) => {
				Some(poisoned.into_inner().snapshot)
			}
			Err(std::sync::TryLockError::WouldBlock) => None,
		}
	}

	/// Test-only (DC-292): holds this observer's lock, as a step publishing its stage holds it.
	#[cfg(test)]
	pub(crate) fn hold_lock_for_test(&self) -> std::sync::MutexGuard<'_, Owned> {
		self.owned.lock().unwrap_or_else(|e| e.into_inner())
	}

	/// Test-only: the current snapshot, one coherent copy.
	#[cfg(test)]
	pub(crate) fn snapshot(&self) -> TierStepSnapshot {
		self.owned
			.lock()
			.unwrap_or_else(|e| e.into_inner())
			.snapshot
	}

	/// Test-only (wendelon ADR 0125): this store's outcome counts.
	#[cfg(test)]
	pub(crate) fn outcomes_for_test(&self) -> [u64; 13] {
		std::array::from_fn(|at| self.outcomes[at].load(Relaxed))
	}

	/// Test-only: this store's snapshot rendered exactly as the exported series render it (set
	/// into an isolated registry and rendered by it).
	#[cfg(test)]
	pub(crate) fn render_for_test(&self) -> String {
		let registry = crate::metrics::isolated_for_test();
		registry.set_certified_tier_snapshot(self.snapshot());
		registry.render_prometheus()
	}
}

fn publish(snapshot: &TierStepSnapshot) {
	crate::metrics::global().set_certified_tier_snapshot(*snapshot);
}

#[cfg(test)]
mod tests {
	use super::*;

	/// A step begun while another is observed is counted only: the observed step's snapshot is
	/// not replaced, and only its own end makes it the last step.
	#[test]
	fn a_concurrent_step_never_replaces_the_observed_one() {
		let observer = CertifiedTierObserver::default();
		let a = observer.begin_step();
		assert!(a.is_some());
		observer.claim_waiting(a, 7);
		let b = observer.begin_step();
		assert_eq!(b, None, "another step is observed");
		observer.enter(b, TierJobStage::Deciding);
		observer.end_step(b, TierStepOutcome::NotLeader);
		let snapshot = observer.snapshot();
		assert_eq!(
			(snapshot.stage, snapshot.slab),
			(TierJobStage::ClaimWaiting, 7)
		);
		assert_eq!(snapshot.last, None);
		assert_eq!(
			observer.outcomes_for_test()[TierStepOutcome::NotLeader as usize],
			1
		);
		observer.submitting(a, 41);
		observer.end_step(a, TierStepOutcome::Retired);
		let snapshot = observer.snapshot();
		assert_eq!(snapshot.stage, TierJobStage::Idle);
		let last = snapshot.last.expect("the observed step is the last");
		assert_eq!(
			(last.outcome, last.slab, last.op_seq),
			(TierStepOutcome::Retired, 7, 41)
		);
		assert!(observer.begin_step().is_some(), "the slot is free again");
	}

	/// The last slot keeps the last completed step that obtained an op: op-less steps (no debt,
	/// nothing selected after a slab was chosen) leave it, while their counters advance and the
	/// stage returns to idle; a later op-bearing step, a failure after allocation included,
	/// replaces it.
	#[test]
	fn the_last_slot_keeps_the_last_step_that_obtained_an_op() {
		let observer = CertifiedTierObserver::default();
		let step = observer.begin_step();
		observer.claim_waiting(step, 3);
		observer.note_active_gid(step, 30);
		observer.submitting(step, 7);
		observer.end_step(step, TierStepOutcome::Rewritten);
		let kept = observer.snapshot().last.expect("an op-bearing step");
		assert_eq!(
			(kept.outcome, kept.slab, kept.op_seq, kept.active_gid),
			(TierStepOutcome::Rewritten, 3, 7, 30)
		);
		let step = observer.begin_step();
		observer.end_step(step, TierStepOutcome::NoDebt);
		let step = observer.begin_step();
		observer.claim_waiting(step, 4);
		observer.end_step(step, TierStepOutcome::NothingSelected);
		let snapshot = observer.snapshot();
		assert_eq!(snapshot.stage, TierJobStage::Idle);
		assert_eq!(snapshot.last, Some(kept), "op-less steps leave the slot");
		let counts = observer.outcomes_for_test();
		assert_eq!(
			(
				counts[TierStepOutcome::NoDebt as usize],
				counts[TierStepOutcome::NothingSelected as usize]
			),
			(1, 1)
		);
		let step = observer.begin_step();
		observer.claim_waiting(step, 5);
		observer.submitting(step, 9);
		observer.end_step(step, TierStepOutcome::Failed);
		let replaced = observer.snapshot().last.expect("replaced");
		assert_eq!(
			(replaced.outcome, replaced.slab, replaced.op_seq),
			(TierStepOutcome::Failed, 5, 9),
			"a failure after allocation replaces it"
		);
	}

	/// The values of the observer's rendered families, `series{labels}` -> value.
	fn values(text: &str) -> std::collections::BTreeMap<String, u64> {
		text.lines()
			.filter(|line| line.starts_with("wendelon_certified_tier_"))
			.filter_map(|line| {
				let (series, value) = line.rsplit_once(' ')?;
				Some((series.to_owned(), value.parse().ok()?))
			})
			.collect()
	}

	fn stage_of(values: &std::collections::BTreeMap<String, u64>) -> Vec<&'static str> {
		TierJobStage::ALL
			.iter()
			.filter(|stage| {
				values.get(&format!(
					"wendelon_certified_tier_job_stage{{stage=\"{}\"}}",
					stage.label()
				)) == Some(&1)
			})
			.map(|stage| stage.label())
			.collect()
	}

	fn field(values: &std::collections::BTreeMap<String, u64>, field: &str) -> u64 {
		values[&format!("wendelon_certified_tier_job{{field=\"{field}\"}}")]
	}

	/// A fresh registry renders `idle` as the one stage, every field 0 and no last outcome.
	#[test]
	fn a_fresh_registry_renders_idle() {
		let values = values(&crate::metrics::isolated_for_test().render_prometheus());
		assert_eq!(stage_of(&values), vec!["idle"]);
		for name in [
			"stage_entered_unix_ms",
			"started_unix_ms",
			"slab",
			"op_seq",
			"active_gid",
		] {
			assert_eq!(field(&values, name), 0, "{name}");
		}
		for outcome in TierStepOutcome::ALL {
			let series = format!(
				"wendelon_certified_tier_last_job_outcome{{outcome=\"{}\"}}",
				outcome.label()
			);
			assert_eq!(values.get(&series), Some(&0), "{series}");
		}
	}

	/// Snapshots set while the registry renders: every render shows exactly one stage and the
	/// slab, op and generation of the very snapshot that stage belongs to, never a mix.
	#[test]
	fn every_render_is_one_coherent_snapshot() {
		let registry = crate::metrics::isolated_for_test();
		let a = TierStepSnapshot {
			stage: TierJobStage::Delivering,
			stage_entered_unix_ms: 11,
			started_unix_ms: 10,
			slab: 1,
			op_seq: 101,
			active_gid: 1001,
			last: None,
		};
		let b = TierStepSnapshot {
			stage: TierJobStage::ClaimWaiting,
			stage_entered_unix_ms: 22,
			started_unix_ms: 20,
			slab: 2,
			op_seq: 0,
			active_gid: 0,
			last: Some(TierStepLast {
				outcome: TierStepOutcome::Retired,
				slab: 1,
				op_seq: 101,
				active_gid: 1001,
				duration_ms: 5,
				finished_unix_ms: 15,
			}),
		};
		let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
		let writer = {
			let stop = stop.clone();
			std::thread::spawn(move || {
				let mut flip = false;
				while !stop.load(Relaxed) {
					registry.set_certified_tier_snapshot(if flip { a } else { b });
					flip = !flip;
				}
			})
		};
		for _ in 0..2_000 {
			let values = values(&registry.render_prometheus());
			let seen = (
				stage_of(&values),
				field(&values, "slab"),
				field(&values, "op_seq"),
				field(&values, "active_gid"),
				field(&values, "stage_entered_unix_ms"),
			);
			assert!(
				seen == (vec!["delivering"], 1, 101, 1001, 11)
					|| seen == (vec!["claim_waiting"], 2, 0, 0, 22)
					|| seen == (vec!["idle"], 0, 0, 0, 0),
				"a mixed render: {seen:?}"
			);
		}
		stop.store(true, Relaxed);
		writer.join().unwrap();
	}
}
