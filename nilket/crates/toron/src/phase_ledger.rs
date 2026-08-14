//! The task phase ledger: exclusive operator time, derived structurally (ADR 551).
//!
//! ADR 538 reported `operator` as build + probe + spill measured by brackets that
//! also contained base-relation reads, and `scan` from a single bare-node site.
//! The two therefore overlapped on fused paths and left a hole on others, which is
//! why ADR 550 withdrew the component fork.
//!
//! This ledger fixes the boundary by construction rather than by convention. An
//! operator bracket is a **scope**; every acquisition interval recorded while a
//! scope is open is attributed to the innermost one; and closing a scope
//! subtracts what it contained:
//!
//! ```text
//! inclusive operator
//!   = exclusive operator
//!   + nested base scan
//!   + nested materialized/live shuffle
//!   + nested live decode transport
//! ```
//!
//! The fused live-poll diagnostic total is deliberately **not** a term. It
//! overlaps the pull/wait and decode intervals that are terms, so including it
//! would double-count; it is a bound, not eligible evidence.
//!
//! Nothing here clamps, and that is structural rather than a habit: every total
//! is accumulated with `checked_add` and every subtraction with `checked_sub`.
//! Underflow, a clock that went backwards, an arithmetic overflow, a scope closed
//! out of order, and a scope never closed are each recorded as a fault, and any
//! fault **vetoes the receipt** — attribution computed over a broken partition is
//! corrupted evidence, and a run that reports it as a number is worse than one
//! that refuses. The refusal is typed, so a withheld receipt names its cause.

use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::scan_metering::Clock;

/// Which term of the identity an acquisition interval belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AcquisitionKind {
	/// Reading a base relation — `scan`.
	BaseScan,
	/// Materialized exchange input, live stream setup, and live pull/wait —
	/// `shuffle`.
	Shuffle,
	/// Decoding a live producer payload — `transport`.
	Transport,
	/// The fused live-poll total. Recorded for diagnosis and **never** netted out:
	/// it overlaps `Shuffle` and `Transport`, so subtracting it would remove the
	/// same wall time twice.
	LiveDiagnostic,
}

impl AcquisitionKind {
	/// Whether this interval is eligible to be subtracted from an enclosing
	/// operator bracket.
	fn is_eligible(self) -> bool {
		!matches!(self, Self::LiveDiagnostic)
	}
}

/// Which join phase a bracket measures.
///
/// ADR 538's receipt keeps `build_micros`, `probe_micros` and `spill_micros`
/// apart, and downstream tooling consumes them individually. Exclusivity must not
/// be bought by collapsing them into one `operator` figure: the aggregate is
/// **derived** from these three, never a replacement for them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperatorKind {
	Build,
	Probe,
	Spill,
}

/// Why a receipt cannot be trusted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerFaults {
	/// Nested acquisition exceeded the bracket that contained it.
	pub operator_underflow: u64,
	/// A clock reading went backwards.
	pub clock_regressions: u64,
	/// A scope was closed while it was not the innermost open one.
	pub mismatched_closes: u64,
	/// A scope was still open when the ledger was sealed.
	pub unclosed_scopes: u64,
	/// A total would have overflowed. Recorded rather than wrapped or saturated:
	/// a clamped total is a wrong number that looks like a right one.
	pub arithmetic_overflow: u64,
	/// The identity did not hold at seal time. Checked rather than assumed: the
	/// arithmetic that produces it is the arithmetic being trusted.
	pub identity_violation: u64,
	/// A scope was dropped without an explicit `commit` or `discard`. The branch
	/// that dropped it never said whether its work was authoritative, so no honest
	/// figure can be derived from it — and an unresolved branch must become a
	/// visible veto rather than silent inflation.
	pub unresolved_operator_scope: u64,
	/// The measurement closure that was supposed to freeze a bracket never ran, so
	/// no interval was taken where the legacy value would have been. Publishing
	/// zero would look like "this phase did no work" rather than "this phase was
	/// never measured".
	pub measurement_boundary_missed: u64,
	/// A transition the lifecycle does not allow — finishing twice, or discarding
	/// a token at the wrong stage.
	pub invalid_scope_transition: u64,
	/// A bracket finished while one of its children was finished but unresolved.
	/// The child's span could then land nowhere its parent accounted for, so the
	/// nesting the receipt describes would not be the nesting that happened.
	pub finished_child_outstanding: u64,
	/// The two ledgers disagreed about the same intervals. Every interval is
	/// written to both at the moment it is measured, so a divergence means one of
	/// them missed a write or took a different classification — and neither
	/// figure can be trusted after that.
	pub reconciliation_mismatch: u64,
}

impl LedgerFaults {
	pub fn is_clean(&self) -> bool {
		*self == Self::default()
	}
}

/// Totals a sealed ledger publishes, once.
///
/// **Global totals are the component receipts; only the nested terms enter the
/// identity.** An acquisition performed outside every operator bracket — a bare
/// scan, which is the ordinary case — is real `scan` time and belongs in the
/// receipt, but it was never inside an operator and so cannot be subtracted from
/// one. Testing the identity against global totals would make it false by
/// definition the moment a plan reads anything outside a join.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PhaseTotals {
	pub inclusive_operator_micros: u64,
	/// Raw phase identities, kept apart because ADR 538's receipt keeps them
	/// apart and downstream tooling reads them individually.
	pub exclusive_build_micros: u64,
	pub exclusive_probe_micros: u64,
	pub exclusive_spill_micros: u64,
	/// Component receipts: all acquisition, nested or not.
	pub base_scan_micros: u64,
	pub shuffle_micros: u64,
	pub transport_micros: u64,
	pub live_diagnostic_micros: u64,
	/// Identity terms: only acquisition that happened inside an operator bracket.
	pub nested_base_scan_micros: u64,
	pub nested_shuffle_micros: u64,
	pub nested_transport_micros: u64,
}

impl PhaseTotals {
	/// The aggregate operator figure, **derived** from the three raw phases.
	///
	/// Deliberately a method rather than a field: nothing can publish it in place
	/// of build, probe and spill, because it does not exist until asked for.
	/// `None` on overflow — never a sentinel.
	pub fn exclusive_operator_micros(&self) -> Option<u64> {
		self.exclusive_build_micros
			.checked_add(self.exclusive_probe_micros)
			.and_then(|sum| sum.checked_add(self.exclusive_spill_micros))
	}

	/// Every eligible **nested** term — base scan, shuffle, and transport that
	/// occurred inside an operator bracket. The live diagnostic total is excluded
	/// by construction, and unnested acquisition is not a term at all.
	///
	/// `None` on overflow. It must not fall back to `u64::MAX`: that is a value an
	/// honest total could hold, so a saturated sum can compare equal to a real one
	/// and make a broken partition look sound.
	pub fn nested_eligible_micros(&self) -> Option<u64> {
		self.nested_base_scan_micros
			.checked_add(self.nested_shuffle_micros)
			.and_then(|sum| sum.checked_add(self.nested_transport_micros))
	}

	/// The ADR 551 identity, checked end to end. `None` means the arithmetic
	/// itself overflowed, which is a different defect from the identity failing.
	pub fn identity_check(&self) -> Option<bool> {
		let exclusive = self.exclusive_operator_micros()?;
		let nested = self.nested_eligible_micros()?;
		let sum = exclusive.checked_add(nested)?;
		Some(self.inclusive_operator_micros == sum)
	}

	/// The identity, as a plain predicate. An overflow reads as "does not hold".
	pub fn identity_holds(&self) -> bool {
		self.identity_check() == Some(true)
	}
}

#[derive(Debug, Default, Clone, Copy)]
struct NestedTerms {
	base_scan_micros: u64,
	shuffle_micros: u64,
	transport_micros: u64,
}

impl NestedTerms {
	fn add(&mut self, kind: AcquisitionKind, micros: u64) -> bool {
		let slot = match kind {
			AcquisitionKind::BaseScan => &mut self.base_scan_micros,
			AcquisitionKind::Shuffle => &mut self.shuffle_micros,
			AcquisitionKind::Transport => &mut self.transport_micros,
			AcquisitionKind::LiveDiagnostic => return true,
		};
		match slot.checked_add(micros) {
			Some(sum) => {
				*slot = sum;
				true
			}
			None => false,
		}
	}

	fn merge(&mut self, other: Self) -> bool {
		self.add(AcquisitionKind::BaseScan, other.base_scan_micros)
			&& self.add(AcquisitionKind::Shuffle, other.shuffle_micros)
			&& self.add(AcquisitionKind::Transport, other.transport_micros)
	}

	fn total(&self) -> Option<u64> {
		self.base_scan_micros
			.checked_add(self.shuffle_micros)
			.and_then(|sum| sum.checked_add(self.transport_micros))
	}
}

#[derive(Debug)]
struct OperatorFrame {
	kind: OperatorKind,
	id: u64,
	opened_micros: u64,
	/// Acquisition recorded while this frame was innermost, held **per kind** and
	/// **uncommitted**.
	///
	/// ADR 551: a frame may be discarded — several existing timers start before a
	/// speculative path and publish only if it is selected. Committing these terms
	/// globally at observation time would make a rejected candidate's reads part
	/// of the identity even though its bracket never became operator time. They
	/// are committed on a successful close, or transferred to a committed parent
	/// on discard.
	nested: NestedTerms,
	/// Children that have finished but not yet resolved. A frame may not finish
	/// while any remain: its exclusive figure would be computed without their
	/// spans, and their later commit would have nowhere correct to go.
	outstanding_finished_children: u64,
	/// Committed child brackets' whole spans, netted out of this frame.
	nested_child_operator_micros: u64,
}

#[derive(Debug, Default)]
struct LedgerState {
	stack: Vec<OperatorFrame>,
	next_scope_id: u64,
	sealed: bool,
	totals: PhaseTotals,
	faults: LedgerFaults,
}

/// What sealing produced, and if nothing, why.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SealOutcome {
	/// The partition held; these totals are the receipt.
	Sealed(PhaseTotals),
	/// A fault made the attribution untrustworthy. The faults travel with the
	/// refusal so a withheld receipt names its own cause.
	Withheld(LedgerFaults),
	/// Sealed before. Publishing twice would let the same wall time enter a
	/// component more than once.
	AlreadySealed,
}

impl SealOutcome {
	pub fn totals(self) -> Option<PhaseTotals> {
		match self {
			Self::Sealed(totals) => Some(totals),
			Self::Withheld(_) | Self::AlreadySealed => None,
		}
	}

	pub fn is_sealed(self) -> bool {
		matches!(self, Self::Sealed(_))
	}
}

/// Exclusive task phases, derived from nesting the ledger records itself.
#[derive(Debug)]
pub struct TaskPhaseLedger {
	clock: Arc<dyn Clock>,
	state: Mutex<LedgerState>,
}

impl TaskPhaseLedger {
	pub fn new(clock: Arc<dyn Clock>) -> Self {
		Self {
			clock,
			state: Mutex::new(LedgerState::default()),
		}
	}

	fn lock(&self) -> std::sync::MutexGuard<'_, LedgerState> {
		self.state
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner())
	}

	/// Open an operator bracket. The returned scope closes on drop, so an early
	/// return or an error cannot silently lose the time it covered.
	pub fn open_operator(self: &Arc<Self>, kind: OperatorKind) -> OperatorScope {
		let opened_micros = self.clock.now_micros();
		let mut state = self.lock();
		let id = state.next_scope_id;
		state.next_scope_id += 1;
		state.stack.push(OperatorFrame {
			kind,
			id,
			opened_micros,
			nested: NestedTerms::default(),
			outstanding_finished_children: 0,
			nested_child_operator_micros: 0,
		});
		drop(state);
		OperatorScope {
			ledger: Arc::clone(self),
			id,
			closed: false,
		}
	}

	/// Record an acquisition interval, attributing it to the innermost open
	/// operator bracket if there is one.
	pub fn record_acquisition(&self, kind: AcquisitionKind, micros: u64) {
		let mut state = self.lock();

		// The component receipt takes every acquisition, nested or not.
		let mut overflowed = false;
		{
			let totals = &mut state.totals;
			let global = match kind {
				AcquisitionKind::BaseScan => &mut totals.base_scan_micros,
				AcquisitionKind::Shuffle => &mut totals.shuffle_micros,
				AcquisitionKind::Transport => &mut totals.transport_micros,
				AcquisitionKind::LiveDiagnostic => &mut totals.live_diagnostic_micros,
			};
			match global.checked_add(micros) {
				Some(sum) => *global = sum,
				None => overflowed = true,
			}
		}

		// The identity takes only what happened inside an operator bracket, and it
		// is held on the frame until that frame commits. A discarded bracket never
		// contributes it.
		if kind.is_eligible()
			&& let Some(frame) = state.stack.last_mut()
			&& !frame.nested.add(kind, micros)
		{
			overflowed = true;
		}

		if overflowed {
			state.faults.arithmetic_overflow += 1;
		}
	}

	/// A clock reading that went backwards, seen by a caller outside this ledger.
	pub fn record_clock_regression(&self) {
		self.lock().faults.clock_regressions += 1;
	}

	/// Close a bracket's timer and **freeze** what it measured, without deciding
	/// whether it publishes.
	///
	/// ADR 551: the legacy interval is taken at the duration-measurement point,
	/// which is often not where the value is written — in the join paths it is
	/// evaluated inside the telemetry closure, after the mutex is acquired.
	/// Freezing here and publishing later reproduces that boundary exactly; a
	/// single-stage close would extend the interval to wherever the decision was
	/// made and silently widen every operator figure.
	fn finish_scope(&self, id: u64) -> FinishedFrame {
		let closed_micros = self.clock.now_micros();
		let mut state = self.lock();

		let Some(position) = state.stack.iter().rposition(|frame| frame.id == id) else {
			state.faults.mismatched_closes += 1;
			return FinishedFrame::faulted();
		};
		if position + 1 != state.stack.len() {
			state.faults.mismatched_closes += 1;
		}

		if state.stack[position].outstanding_finished_children > 0 {
			// A child froze its measurement and has not said whether it publishes.
			// Finishing here would compute this frame's exclusive time without that
			// child's span, and the child's later commit would have no live parent
			// to transfer into.
			state.faults.finished_child_outstanding += 1;
			state.stack.remove(position);
			return FinishedFrame::faulted();
		}
		let frame = state.stack.remove(position);
		let parent_id = state.stack.last().map(|parent| parent.id);
		if let Some(parent) = state.stack.last_mut() {
			parent.outstanding_finished_children += 1;
		}

		let Some(inclusive) = closed_micros.checked_sub(frame.opened_micros) else {
			state.faults.clock_regressions += 1;
			return FinishedFrame::faulted();
		};
		let Some(nested_own) = frame.nested.total() else {
			state.faults.arithmetic_overflow += 1;
			return FinishedFrame::faulted();
		};
		let Some(nested_total) = nested_own.checked_add(frame.nested_child_operator_micros) else {
			state.faults.arithmetic_overflow += 1;
			return FinishedFrame::faulted();
		};
		let Some(exclusive) = inclusive.checked_sub(nested_total) else {
			state.faults.operator_underflow += 1;
			return FinishedFrame::faulted();
		};

		FinishedFrame {
			kind: frame.kind,
			parent_id,
			inclusive,
			exclusive,
			nested: frame.nested,
			nested_child_operator_micros: frame.nested_child_operator_micros,
			faulted: false,
		}
	}

	fn commit_finished(&self, finished: &FinishedFrame) {
		if finished.faulted {
			return;
		}
		let mut state = self.lock();
		let mut overflowed = false;
		{
			let phase = match finished.kind {
				OperatorKind::Build => &mut state.totals.exclusive_build_micros,
				OperatorKind::Probe => &mut state.totals.exclusive_probe_micros,
				OperatorKind::Spill => &mut state.totals.exclusive_spill_micros,
			};
			match phase.checked_add(finished.exclusive) {
				Some(sum) => *phase = sum,
				None => overflowed = true,
			}
		}

		let totals = &mut state.totals;
		for (slot, micros) in [
			(
				&mut totals.nested_base_scan_micros,
				finished.nested.base_scan_micros,
			),
			(
				&mut totals.nested_shuffle_micros,
				finished.nested.shuffle_micros,
			),
			(
				&mut totals.nested_transport_micros,
				finished.nested.transport_micros,
			),
		] {
			match slot.checked_add(micros) {
				Some(sum) => *slot = sum,
				None => overflowed = true,
			}
		}

		let parent = finished
			.parent_id
			.and_then(|id| state.stack.iter_mut().rposition(|frame| frame.id == id));
		let handed = match parent {
			Some(position) => {
				let parent = &mut state.stack[position];
				parent.outstanding_finished_children =
					parent.outstanding_finished_children.saturating_sub(1);
				parent
					.nested_child_operator_micros
					.checked_add(finished.inclusive)
					.map(|sum| parent.nested_child_operator_micros = sum)
			}
			None => state
				.totals
				.inclusive_operator_micros
				.checked_add(finished.inclusive)
				.map(|sum| state.totals.inclusive_operator_micros = sum),
		};
		if handed.is_none() || overflowed {
			state.faults.arithmetic_overflow += 1;
		}
	}

	fn discard_finished(&self, finished: &FinishedFrame) {
		if finished.faulted {
			return;
		}
		let mut state = self.lock();
		let mut overflowed = false;
		let parent = finished
			.parent_id
			.and_then(|id| state.stack.iter_mut().rposition(|frame| frame.id == id));
		match parent {
			Some(position) => {
				let parent = &mut state.stack[position];
				parent.outstanding_finished_children =
					parent.outstanding_finished_children.saturating_sub(1);
				if !parent.nested.merge(finished.nested) {
					overflowed = true;
				}
				match parent
					.nested_child_operator_micros
					.checked_add(finished.nested_child_operator_micros)
				{
					Some(sum) => parent.nested_child_operator_micros = sum,
					None => overflowed = true,
				}
			}
			None => match state
				.totals
				.inclusive_operator_micros
				.checked_add(finished.nested_child_operator_micros)
			{
				Some(sum) => state.totals.inclusive_operator_micros = sum,
				None => overflowed = true,
			},
		}
		if overflowed {
			state.faults.arithmetic_overflow += 1;
		}
	}

	fn record_unresolved_scope(&self) {
		self.lock().faults.unresolved_operator_scope += 1;
	}

	fn record_measurement_boundary_missed(&self) {
		self.lock().faults.measurement_boundary_missed += 1;
	}

	fn record_invalid_transition(&self) {
		self.lock().faults.invalid_scope_transition += 1;
	}
	fn close_scope(&self, id: u64, disposition: ScopeDisposition) {
		let closed_micros = self.clock.now_micros();
		let mut state = self.lock();

		let Some(position) = state.stack.iter().rposition(|frame| frame.id == id) else {
			// Already closed, or never opened here.
			state.faults.mismatched_closes += 1;
			return;
		};
		if position + 1 != state.stack.len() {
			// Closed out of order: the partition this ledger reports would not
			// describe the nesting that actually happened.
			state.faults.mismatched_closes += 1;
		}

		let frame = state.stack.remove(position);

		if disposition == ScopeDisposition::Unresolved {
			// The branch that dropped this scope never said whether its work was
			// authoritative. Committing by default would make an unclassified exit
			// silently authoritative — the exact inflation this ledger exists to
			// prevent — so an unresolved scope is a fault and the receipt is
			// withheld. Its timer is closed; nothing is published.
			state.faults.unresolved_operator_scope += 1;
			return;
		}

		if disposition == ScopeDisposition::Discard {
			// A speculative bracket that was not selected. It contributes no
			// operator time and no identity terms — reporting it would turn a
			// rejected candidate into measured operator work. Its acquisition is
			// still real and still in the global receipt.
			//
			// Two things move, and both must: its own acquisition terms, and the
			// spans of any children it already committed. A committed child has
			// **already published** its exclusive time and identity terms, so its
			// span still has to be netted out somewhere — otherwise it sits in the
			// totals while nothing subtracts it, and the identity fails.
			let mut overflowed = false;
			match state.stack.last_mut() {
				Some(parent) => {
					if !parent.nested.merge(frame.nested) {
						overflowed = true;
					}
					match parent
						.nested_child_operator_micros
						.checked_add(frame.nested_child_operator_micros)
					{
						Some(sum) => parent.nested_child_operator_micros = sum,
						None => overflowed = true,
					}
				}
				// A discarded **root**: its committed children are retained as
				// independently published outermost scopes. They were selected and
				// did real operator work; the rejection of a speculative bracket
				// around them does not unmake it. Promoting their spans to the
				// inclusive total is what keeps the identity true, since their
				// exclusive time and identity terms are already published.
				None => match state
					.totals
					.inclusive_operator_micros
					.checked_add(frame.nested_child_operator_micros)
				{
					Some(sum) => state.totals.inclusive_operator_micros = sum,
					None => overflowed = true,
				},
			}
			if overflowed {
				state.faults.arithmetic_overflow += 1;
			}
			return;
		}

		let Some(inclusive) = closed_micros.checked_sub(frame.opened_micros) else {
			state.faults.clock_regressions += 1;
			return;
		};
		let Some(nested_own) = frame.nested.total() else {
			state.faults.arithmetic_overflow += 1;
			return;
		};
		let Some(nested_total) = nested_own.checked_add(frame.nested_child_operator_micros) else {
			state.faults.arithmetic_overflow += 1;
			return;
		};
		let Some(exclusive) = inclusive.checked_sub(nested_total) else {
			// Nested work exceeded its bracket: the two were measured against
			// different timelines, so neither figure is usable.
			state.faults.operator_underflow += 1;
			return;
		};

		// Exclusive time accrues per frame, into that frame's own phase. Build,
		// probe and spill stay separate all the way to publication: downstream
		// tooling reads them individually, and exclusivity must not be bought by
		// collapsing them into a single `operator` figure.
		let mut overflowed = false;
		{
			let phase = match frame.kind {
				OperatorKind::Build => &mut state.totals.exclusive_build_micros,
				OperatorKind::Probe => &mut state.totals.exclusive_probe_micros,
				OperatorKind::Spill => &mut state.totals.exclusive_spill_micros,
			};
			match phase.checked_add(exclusive) {
				Some(sum) => *phase = sum,
				None => overflowed = true,
			}
		}

		// Committing publishes this frame's identity terms.
		let totals = &mut state.totals;
		for (slot, micros) in [
			(
				&mut totals.nested_base_scan_micros,
				frame.nested.base_scan_micros,
			),
			(
				&mut totals.nested_shuffle_micros,
				frame.nested.shuffle_micros,
			),
			(
				&mut totals.nested_transport_micros,
				frame.nested.transport_micros,
			),
		] {
			match slot.checked_add(micros) {
				Some(sum) => *slot = sum,
				None => overflowed = true,
			}
		}

		let handed_up = match state.stack.last_mut() {
			// A committed frame hands its whole span to its parent, so an outer
			// bracket nets out everything beneath it and not merely its own
			// direct acquisitions.
			Some(parent) => parent
				.nested_child_operator_micros
				.checked_add(inclusive)
				.map(|sum| parent.nested_child_operator_micros = sum),
			// Only outermost brackets contribute to the inclusive total. Adding
			// inner frames as well would count their wall twice — once on its own
			// and once inside the parent that contains it — and the identity
			// would fail for exactly the nested shapes it exists to describe.
			None => state
				.totals
				.inclusive_operator_micros
				.checked_add(inclusive)
				.map(|sum| state.totals.inclusive_operator_micros = sum),
		};
		if handed_up.is_none() || overflowed {
			state.faults.arithmetic_overflow += 1;
		}
	}

	/// Seal the ledger and publish its totals — **exactly once**.
	///
	/// The outcome is typed rather than an undifferentiated `None`, because a
	/// withheld production receipt has to carry its cause: "this task measured a
	/// clock that went backwards" and "this task was sealed twice" are different
	/// defects with different fixes, and a bare absence names neither.
	/// Seal the phase ledger.
	///
	/// **Not the production seal path.** `ScanMeter::seal_receipt` is, because it
	/// reconciles this ledger against the acquisition ledger first; publishing
	/// from here would skip that check. Crate-visible so the meter can call it,
	/// and guarded so nothing else does.
	pub(crate) fn seal(&self) -> SealOutcome {
		let mut state = self.lock();
		if state.sealed {
			return SealOutcome::AlreadySealed;
		}
		state.sealed = true;
		let unclosed = state.stack.len() as u64;
		state.faults.unclosed_scopes += unclosed;
		state.stack.clear();

		// The identity is checked here, not assumed. Everything above computes it;
		// sealing is the one place that can refuse to publish a partition that
		// does not actually add up.
		match state.totals.identity_check() {
			Some(true) => {}
			Some(false) => state.faults.identity_violation += 1,
			None => state.faults.arithmetic_overflow += 1,
		}

		if !state.faults.is_clean() {
			return SealOutcome::Withheld(state.faults);
		}
		SealOutcome::Sealed(state.totals)
	}

	pub fn faults(&self) -> LedgerFaults {
		self.lock().faults
	}

	/// Record that the two ledgers disagreed at publication.
	pub fn record_reconciliation_mismatch(&self) {
		self.lock().faults.reconciliation_mismatch += 1;
	}

	/// Totals as they stand, ignoring the seal. For controls and diagnosis only.
	pub fn totals(&self) -> PhaseTotals {
		self.lock().totals
	}
}

/// An open operator bracket.
///
/// Closes on drop, so an early return, an error, or a panic unwinding through the
/// operator still accounts for the time the bracket covered.
#[derive(Debug)]
pub struct OperatorScope {
	ledger: Arc<TaskPhaseLedger>,
	id: u64,
	closed: bool,
}

/// Whether a closing bracket's time becomes operator time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScopeDisposition {
	/// Explicitly publishes selected authoritative work.
	Commit,
	/// Explicitly rejects the work, or hands it off to be timed elsewhere.
	Discard,
	/// Dropped without either. A defect, not a default.
	Unresolved,
}

impl OperatorScope {
	/// Stop this bracket's timer and freeze what it measured, without deciding
	/// whether it publishes. Call this exactly where the legacy
	/// `duration_micros(started)` is evaluated.
	pub fn finish(mut self) -> FinishedOperatorScope {
		self.closed = true;
		let finished = self.ledger.finish_scope(self.id);
		FinishedOperatorScope {
			ledger: Arc::clone(&self.ledger),
			finished,
			resolved: false,
		}
	}

	/// Close and commit. Equivalent to dropping, and provided so a caller can be
	/// clear about where the bracket ends.
	pub fn commit(mut self) {
		self.closed = true;
		self.ledger.close_scope(self.id, ScopeDisposition::Commit);
	}

	/// Close **without** publishing this bracket as operator time.
	///
	/// For timers that start before a speculative path and publish only if it is
	/// selected: a rejected candidate did real work, but that work is not the
	/// operator phase it was a candidate for, and reporting it would inflate
	/// exactly the figure this arc exists to make trustworthy. The reads it
	/// performed stay in the global receipt and move to a committed parent.
	pub fn discard(mut self) {
		self.closed = true;
		self.ledger.close_scope(self.id, ScopeDisposition::Discard);
	}
}

impl Drop for OperatorScope {
	/// Dropping resolves **nothing**. It closes the timer and records
	/// `unresolved_operator_scope`, which withholds the receipt.
	///
	/// Committing by default was the earlier design and it is unsafe: a census of
	/// the engine's own timers found 29 sites with a post-start early return, 21
	/// of them ordinary successful returns rather than speculative rejections. An
	/// implicit commit would have made every unclassified branch authoritative
	/// without anyone deciding that it should be. A missed branch must be a
	/// visible veto, not silent inflation.
	///
	/// Error propagation stays safe: a failing task publishes no receipt at all,
	/// and if an error is caught and execution continues, the unresolved fault
	/// stops that path producing evidence.
	fn drop(&mut self) {
		if !self.closed {
			self.closed = true;
			self.ledger
				.close_scope(self.id, ScopeDisposition::Unresolved);
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::scan_metering::ManualClock;

	fn ledger() -> (Arc<ManualClock>, Arc<TaskPhaseLedger>) {
		let clock = Arc::new(ManualClock::new());
		let ledger = Arc::new(TaskPhaseLedger::new(clock.clone()));
		(clock, ledger)
	}

	/// The ADR 551 identity, as an exact equality on injected-clock values:
	/// inclusive = exclusive + nested base scan + nested shuffle + nested transport.
	#[test]
	fn every_eligible_nested_acquisition_is_netted_out_of_operator() {
		let (clock, ledger) = ledger();
		let scope = ledger.open_operator(OperatorKind::Build);

		clock.advance(3); // operator work before acquiring
		ledger.record_acquisition(AcquisitionKind::BaseScan, 11);
		clock.advance(11);
		ledger.record_acquisition(AcquisitionKind::Shuffle, 7);
		clock.advance(7);
		ledger.record_acquisition(AcquisitionKind::Transport, 5);
		clock.advance(5);
		clock.advance(2); // operator work after acquiring
		scope.commit();

		let totals = ledger
			.seal()
			.totals()
			.expect("no faults, so the receipt stands");
		assert_eq!(totals.inclusive_operator_micros, 28);
		assert_eq!(totals.base_scan_micros, 11);
		assert_eq!(totals.shuffle_micros, 7);
		assert_eq!(totals.transport_micros, 5);
		assert_eq!(
			totals.exclusive_operator_micros(),
			Some(5),
			"3 + 2 of real operator work"
		);
		assert!(totals.identity_holds());
	}

	/// An exchange-origin read nested inside an operator is netted out too, not
	/// just a base scan. This is the control that would pass if only `scan` were
	/// subtracted and `shuffle`/`transport` were left inside the bracket.
	#[test]
	fn a_nested_exchange_read_is_netted_out_of_operator() {
		let (clock, ledger) = ledger();
		let scope = ledger.open_operator(OperatorKind::Build);

		// A live poll: pull/wait is shuffle, decode is transport, and the fused
		// diagnostic total covers both.
		ledger.record_acquisition(AcquisitionKind::Shuffle, 13);
		clock.advance(13);
		ledger.record_acquisition(AcquisitionKind::Transport, 4);
		clock.advance(4);
		ledger.record_acquisition(AcquisitionKind::LiveDiagnostic, 17);
		clock.advance(1); // the operator's own work
		scope.commit();

		let totals = ledger.seal().totals().expect("no faults");
		assert_eq!(totals.inclusive_operator_micros, 18);
		assert_eq!(
			totals.exclusive_operator_micros(),
			Some(1),
			"both exchange terms are netted out, leaving only operator work"
		);
		assert_eq!(
			totals.live_diagnostic_micros, 17,
			"the fused total is retained as a bound"
		);
		assert!(
			totals.identity_holds(),
			"the diagnostic total is not a term: including it would double-count"
		);
	}

	/// Nested brackets: an outer operator nets out everything beneath it, not
	/// only its own direct acquisitions.
	#[test]
	fn an_outer_bracket_nets_out_acquisition_inside_an_inner_one() {
		let (clock, ledger) = ledger();
		let outer = ledger.open_operator(OperatorKind::Build);
		clock.advance(2);
		{
			let inner = ledger.open_operator(OperatorKind::Build);
			ledger.record_acquisition(AcquisitionKind::BaseScan, 9);
			clock.advance(9);
			clock.advance(1);
			inner.commit();
		}
		clock.advance(3);
		outer.commit();

		let totals = ledger.seal().totals().expect("no faults");
		// Outer spans 15; inner spans 10 of it, of which 9 was acquisition. Only
		// the outermost bracket contributes to the inclusive total, so the inner
		// span is not counted twice.
		assert_eq!(totals.inclusive_operator_micros, 15);
		assert_eq!(totals.base_scan_micros, 9);
		assert_eq!(
			totals.exclusive_operator_micros(),
			Some(6),
			"1 inside the inner bracket, 5 outside it"
		);
		assert!(
			totals.identity_holds(),
			"the identity holds across nesting, not just for a single bracket"
		);
	}

	/// Nested acquisition exceeding its bracket means the two were measured on
	/// different timelines. That vetoes the receipt rather than clamping to zero.
	#[test]
	fn operator_underflow_vetoes_the_receipt() {
		let (clock, ledger) = ledger();
		let scope = ledger.open_operator(OperatorKind::Build);
		clock.advance(4);
		ledger.record_acquisition(AcquisitionKind::BaseScan, 40);
		scope.commit();

		assert_eq!(ledger.faults().operator_underflow, 1);
		// The refusal names its own cause: a withheld production receipt has to
		// say which defect withheld it.
		assert!(matches!(
			ledger.seal(),
			SealOutcome::Withheld(faults) if faults.operator_underflow == 1
		));
	}

	/// A clock that went backwards vetoes the receipt.
	#[test]
	fn a_clock_regression_vetoes_the_receipt() {
		let (_clock, ledger) = ledger();
		ledger.record_clock_regression();
		assert!(matches!(ledger.seal(), SealOutcome::Withheld(_)));
	}

	/// A scope still open at seal time vetoes the receipt: its time was never
	/// partitioned, so the totals do not describe the task.
	#[test]
	fn an_unclosed_scope_vetoes_the_receipt() {
		let (clock, ledger) = ledger();
		let scope = ledger.open_operator(OperatorKind::Build);
		clock.advance(5);
		std::mem::forget(scope); // never closed, and never dropped

		assert!(matches!(ledger.seal(), SealOutcome::Withheld(_)));
		assert_eq!(ledger.faults().unclosed_scopes, 1);
	}

	/// Closing out of order vetoes the receipt: the reported partition would not
	/// describe the nesting that actually happened.
	#[test]
	fn closing_a_scope_out_of_order_vetoes_the_receipt() {
		let (clock, ledger) = ledger();
		let outer = ledger.open_operator(OperatorKind::Build);
		let inner = ledger.open_operator(OperatorKind::Build);
		clock.advance(3);
		outer.commit();
		inner.commit();

		assert!(ledger.faults().mismatched_closes >= 1);
		assert!(matches!(ledger.seal(), SealOutcome::Withheld(_)));
	}

	/// An early return that resolves nothing is a **fault**, not a commit. This is
	/// the control for the design change: committing on drop would make an
	/// unclassified branch authoritative by accident, which is how a census of 29
	/// early-return sites would have silently inflated operator time.
	#[test]
	fn dropping_an_unresolved_scope_vetoes_the_receipt() {
		let (clock, ledger) = ledger();

		fn operator_that_returns_early(
			ledger: &Arc<TaskPhaseLedger>,
			clock: &ManualClock,
		) -> Result<(), &'static str> {
			let _scope = ledger.open_operator(OperatorKind::Build);
			clock.advance(6);
			Err("early return that resolves neither commit nor discard")
		}

		assert!(operator_that_returns_early(&ledger, &clock).is_err());
		match ledger.seal() {
			SealOutcome::Withheld(faults) => {
				assert_eq!(faults.unresolved_operator_scope, 1);
				assert_eq!(
					faults.unclosed_scopes, 0,
					"the timer was closed; it was the disposition that was missing"
				);
			}
			other => panic!("an unresolved branch must withhold, got {other:?}"),
		}
	}

	/// The same branch, resolved explicitly, is not a fault — so the veto above is
	/// about the missing decision, not about early returns as such.
	#[test]
	fn an_early_return_that_discards_explicitly_is_not_a_fault() {
		let (clock, ledger) = ledger();

		fn operator_that_rejects(
			ledger: &Arc<TaskPhaseLedger>,
			clock: &ManualClock,
		) -> Result<(), &'static str> {
			let scope = ledger.open_operator(OperatorKind::Build);
			clock.advance(6);
			scope.discard();
			Err("rejected candidate, explicitly resolved")
		}

		assert!(operator_that_rejects(&ledger, &clock).is_err());
		let totals = ledger
			.seal()
			.totals()
			.expect("an explicitly resolved branch is not a fault");
		assert_eq!(totals.inclusive_operator_micros, 0);
		assert_eq!(totals.exclusive_operator_micros(), Some(0));
	}

	/// Totals publish exactly once. A second seal returns nothing, so the same
	/// wall time cannot enter a component twice.
	#[test]
	fn totals_publish_exactly_once() {
		let (clock, ledger) = ledger();
		let scope = ledger.open_operator(OperatorKind::Build);
		clock.advance(2);
		scope.commit();

		assert!(ledger.seal().is_sealed());
		assert_eq!(
			ledger.seal(),
			SealOutcome::AlreadySealed,
			"a ledger publishes once"
		);
	}

	// ---- bare versus fused -------------------------------------------------

	const POLL_COST: u64 = 7;

	fn one_row() -> arrow::record_batch::RecordBatch {
		use arrow::array::Int64Array;
		use arrow::datatypes::{DataType, Field, Schema};
		let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
		arrow::record_batch::RecordBatch::try_new(
			schema,
			vec![Arc::new(Int64Array::from(vec![1_i64]))],
		)
		.expect("batch builds")
	}

	/// A stream that charges the clock a fixed amount inside each poll, so the
	/// acquisition it performs is identical between runs.
	fn scripted_stream(clock: Arc<ManualClock>, batches: usize) -> crate::storage::ScanStream {
		use futures::StreamExt;
		let inner = futures::stream::unfold(0usize, move |emitted| {
			let clock = clock.clone();
			async move {
				clock.advance(POLL_COST);
				(emitted < batches).then(|| (Ok(one_row()), emitted + 1))
			}
		})
		.boxed();
		crate::storage::ScanStream::new(
			one_row().schema(),
			crate::ir::SessionId(1),
			crate::ir::QueryId::new(),
			crate::storage::ScanOrigin::BaseRelation,
			inner,
		)
	}

	/// Consume the scripted stream, charging `consumer_cost` per batch, either
	/// inside an operator bracket (fused) or outside one (bare).
	/// Consume the scripted stream, charging `consumer_cost` per batch.
	///
	/// `inside_join_bracket` models a fused **join**, the only operator ADR 538
	/// scopes and ADR 551 preserves. Fused *aggregate* compute is deliberately not
	/// bracketed: it stays in remainder, exactly as it did before, so this helper
	/// covers both shapes without implying aggregate work is operator time.
	async fn run_shape(inside_join_bracket: bool, consumer_cost: u64) -> (u64, PhaseTotals) {
		use crate::scan_metering::{ScanAcquisitionLedger, ScanMeter};
		use futures::StreamExt;

		let clock = Arc::new(ManualClock::new());
		let acquisition = Arc::new(ScanAcquisitionLedger::new());
		let phases = Arc::new(TaskPhaseLedger::new(clock.clone()));
		let meter = ScanMeter::new(clock.clone(), acquisition.clone(), phases.clone());

		let stream = scripted_stream(clock.clone(), 4);
		let scope = inside_join_bracket.then(|| phases.open_operator(OperatorKind::Build));

		let mut stream = meter
			.open(async move { Ok(stream) })
			.await
			.expect("open succeeds");
		while let Some(batch) = stream.next().await {
			batch.expect("scripted stream yields batches");
			clock.advance(consumer_cost);
		}
		// Nothing is recorded here. The meter writes each interval into both
		// ledgers at the moment it measures it; reconstructing intervals by
		// differencing cumulative counters is exactly what ADR 551 forbids, and
		// doing it here as well would double-count every read.
		let acquired = acquisition.base_relation_micros();
		// Resolved explicitly: dropping resolves nothing and would veto.
		if let Some(scope) = scope {
			scope.commit();
		}

		let totals = phases.seal().totals().expect("no faults");
		(acquired, totals)
	}

	/// The control ADR 550 lacked: the same base-relation read attributes the same
	/// `scan` whether it happens at a bare node or inside a fused operator, even
	/// when the consumer around it does different amounts of work.
	#[tokio::test]
	async fn bare_and_fused_reads_attribute_equal_scan_acquisition() {
		let (bare_acquired, bare_totals) = run_shape(false, 0).await;
		let (fused_acquired, fused_totals) = run_shape(false, 1_000).await;

		assert_eq!(
			bare_acquired, fused_acquired,
			"identical acquisition despite different consumer work around the read"
		);
		assert_eq!(
			bare_acquired,
			5 * POLL_COST,
			"four batches plus the terminating poll"
		);
		assert_eq!(
			bare_totals.base_scan_micros, fused_totals.base_scan_micros,
			"the component receipt is the same on both shapes"
		);

		for totals in [bare_totals, fused_totals] {
			assert_eq!(
				totals.inclusive_operator_micros, 0,
				"neither shape is inside a join, so neither reports operator time"
			);
			assert_eq!(
				totals.nested_base_scan_micros, 0,
				"unnested acquisition is a receipt, never an identity term"
			);
			assert!(
				totals.identity_holds(),
				"the identity holds trivially when nothing is nested"
			);
		}
	}

	/// A fused **join** brackets its scan, so the read is netted out of operator
	/// while still appearing whole in the `scan` receipt.
	#[tokio::test]
	async fn a_fused_join_bracket_nets_out_the_scan_it_contains() {
		let (acquired, totals) = run_shape(true, 1_000).await;

		assert_eq!(acquired, 5 * POLL_COST);
		assert_eq!(
			totals.base_scan_micros, acquired,
			"the whole read is still the scan receipt"
		);
		assert_eq!(
			totals.nested_base_scan_micros, acquired,
			"and all of it was nested inside the join"
		);
		assert_eq!(
			totals.exclusive_operator_micros(),
			Some(4_000),
			"only the join's own work remains: 4 batches x 1_000"
		);
		assert!(totals.identity_holds());
	}

	/// Arithmetic that would overflow vetoes the receipt instead of wrapping or
	/// saturating. A clamped total is a wrong number wearing the shape of a right
	/// one, which is the failure mode this whole arc exists to remove.
	#[test]
	fn an_arithmetic_overflow_vetoes_the_receipt() {
		let (_clock, ledger) = ledger();
		ledger.record_acquisition(AcquisitionKind::BaseScan, u64::MAX);
		ledger.record_acquisition(AcquisitionKind::BaseScan, 1);

		match ledger.seal() {
			SealOutcome::Withheld(faults) => assert_eq!(faults.arithmetic_overflow, 1),
			other => panic!("overflow must withhold the receipt, got {other:?}"),
		}
	}

	/// A withheld receipt carries its cause, so the two ways of getting nothing
	/// are distinguishable at the type level.
	#[test]
	fn withholding_and_double_sealing_are_distinct_outcomes() {
		let (_faulted_clock, faulted) = ledger();
		let (clock, clean) = ledger();

		let scope = clean.open_operator(OperatorKind::Build);
		clock.advance(1);
		scope.commit();

		assert!(matches!(clean.seal(), SealOutcome::Sealed(_)));
		assert_eq!(clean.seal(), SealOutcome::AlreadySealed);

		faulted.record_clock_regression();
		assert!(matches!(
			faulted.seal(),
			SealOutcome::Withheld(faults) if faults.clock_regressions == 1
		));
	}

	/// Build, probe and spill keep their own identities through the ledger. The
	/// aggregate `operator` figure is derived from them and never replaces them,
	/// because ADR 538's receipt and the tooling downstream read all three.
	#[test]
	fn build_probe_and_spill_keep_separate_exclusive_totals() {
		let (clock, ledger) = ledger();

		for (kind, own_work, nested_read) in [
			(OperatorKind::Build, 4_u64, 10_u64),
			(OperatorKind::Probe, 6, 20),
			(OperatorKind::Spill, 2, 5),
		] {
			let scope = ledger.open_operator(kind);
			ledger.record_acquisition(AcquisitionKind::BaseScan, nested_read);
			clock.advance(nested_read);
			clock.advance(own_work);
			scope.commit();
		}

		let totals = ledger.seal().totals().expect("no faults");
		assert_eq!(totals.exclusive_build_micros, 4);
		assert_eq!(totals.exclusive_probe_micros, 6);
		assert_eq!(totals.exclusive_spill_micros, 2);
		assert_eq!(
			totals.exclusive_operator_micros(),
			Some(12),
			"the aggregate is derived from the three, not stored instead of them"
		);
		assert_eq!(totals.nested_base_scan_micros, 35);
		assert_eq!(totals.inclusive_operator_micros, 47);
		assert!(totals.identity_holds());
	}

	/// Totals that overflow only when combined across scopes still withhold. A
	/// per-scope check would pass each frame and publish a broken sum.
	#[test]
	fn a_cross_scope_overflow_vetoes_the_receipt() {
		let (clock, ledger) = ledger();

		// Two separate brackets, neither overflowing on its own.
		let scope = ledger.open_operator(OperatorKind::Build);
		ledger.record_acquisition(AcquisitionKind::BaseScan, u64::MAX / 2 + 1);
		clock.advance(1);
		scope.commit();

		let scope = ledger.open_operator(OperatorKind::Probe);
		ledger.record_acquisition(AcquisitionKind::Shuffle, u64::MAX / 2 + 1);
		clock.advance(1);
		scope.commit();

		// Each frame underflowed against its own tiny span, and the nested terms
		// together exceed u64. Either way the receipt is withheld, never clamped.
		match ledger.seal() {
			SealOutcome::Withheld(faults) => assert!(
				faults.arithmetic_overflow > 0 || faults.operator_underflow > 0,
				"a cross-scope overflow is a named fault, got {faults:?}"
			),
			other => panic!("cross-scope overflow must withhold, got {other:?}"),
		}
	}

	/// `nested_eligible_micros` must not report a saturated sentinel: `u64::MAX`
	/// is a value an honest total could hold, so a clamped sum could compare equal
	/// to a real one and make a broken partition look sound.
	#[test]
	fn an_overflowing_nested_sum_is_none_rather_than_a_sentinel() {
		let totals = PhaseTotals {
			nested_base_scan_micros: u64::MAX,
			nested_shuffle_micros: 1,
			inclusive_operator_micros: u64::MAX,
			..PhaseTotals::default()
		};

		assert_eq!(totals.nested_eligible_micros(), None);
		assert_eq!(
			totals.identity_check(),
			None,
			"an overflow is distinguishable from the identity simply failing"
		);
		assert!(!totals.identity_holds());
	}

	/// A discarded bracket publishes no operator time and no identity terms: a
	/// rejected speculative candidate did real work, but that work is not the
	/// operator phase it was a candidate for.
	#[test]
	fn a_discarded_scope_contributes_no_operator_or_identity_terms() {
		let (clock, ledger) = ledger();
		let scope = ledger.open_operator(OperatorKind::Build);
		ledger.record_acquisition(AcquisitionKind::BaseScan, 9);
		clock.advance(9);
		clock.advance(4); // speculative compute that was rejected
		scope.discard();

		let totals = ledger.seal().totals().expect("discarding is not a fault");
		assert_eq!(totals.inclusive_operator_micros, 0);
		assert_eq!(totals.exclusive_build_micros, 0);
		assert_eq!(
			totals.nested_base_scan_micros, 0,
			"a discarded bracket contributes no identity term"
		);
		assert_eq!(
			totals.base_scan_micros, 9,
			"but the read really happened, so it stays in the receipt"
		);
		assert!(totals.identity_holds());
	}

	/// A discarded child inside a committed parent transfers its acquisition to
	/// that parent, so the read is still netted out of the bracket that contained
	/// it — while the rejected candidate's own compute stays in the parent's
	/// exclusive time, which is where it actually happened.
	#[test]
	fn a_discarded_child_transfers_acquisition_to_its_committed_parent() {
		let (clock, ledger) = ledger();
		let outer = ledger.open_operator(OperatorKind::Probe);
		clock.advance(2);
		{
			let inner = ledger.open_operator(OperatorKind::Build);
			ledger.record_acquisition(AcquisitionKind::BaseScan, 9);
			clock.advance(9);
			clock.advance(1); // rejected candidate's own work
			inner.discard();
		}
		clock.advance(3);
		outer.commit();

		let totals = ledger.seal().totals().expect("no faults");
		assert_eq!(totals.inclusive_operator_micros, 15);
		assert_eq!(
			totals.nested_base_scan_micros, 9,
			"the discarded child's read transferred to its committed parent"
		);
		assert_eq!(
			totals.exclusive_probe_micros, 6,
			"2 + 1 (rejected candidate's compute) + 3 remain the parent's own work"
		);
		assert_eq!(totals.exclusive_build_micros, 0);
		assert!(totals.identity_holds());
	}

	/// A discarded middle bracket must hand its committed child's span up, not
	/// just its own acquisition. The child already published its exclusive time
	/// and identity terms, so if nothing nets its span out of the grandparent the
	/// identity fails and the whole receipt is withheld.
	#[test]
	fn a_discarded_frame_transfers_its_committed_children_upward() {
		let (clock, ledger) = ledger();
		let outer = ledger.open_operator(OperatorKind::Probe);
		clock.advance(2);
		{
			let middle = ledger.open_operator(OperatorKind::Spill);
			clock.advance(1);
			{
				let inner = ledger.open_operator(OperatorKind::Build);
				ledger.record_acquisition(AcquisitionKind::BaseScan, 9);
				clock.advance(9);
				clock.advance(3);
				inner.commit();
			}
			clock.advance(1);
			middle.discard();
		}
		clock.advance(2);
		outer.commit();

		let totals = ledger.seal().totals().expect("no faults");
		// Outer spans 18; the committed inner bracket spans 12 of it.
		assert_eq!(totals.inclusive_operator_micros, 18);
		assert_eq!(totals.exclusive_build_micros, 3, "inner kept its own work");
		assert_eq!(
			totals.exclusive_spill_micros, 0,
			"the discarded middle published none"
		);
		assert_eq!(
			totals.exclusive_probe_micros, 6,
			"outer's own 2 + 2, plus the discarded middle's 1 + 1 of rejected work"
		);
		assert_eq!(totals.nested_base_scan_micros, 9);
		assert!(
			totals.identity_holds(),
			"the committed grandchild's span was netted out of the outer bracket"
		);
	}

	/// A committed child under a **discarded root** is retained as an
	/// independently published outermost scope. It was selected and did real
	/// operator work; rejecting a speculative bracket around it does not unmake
	/// that, and its exclusive time is already published.
	#[test]
	fn a_committed_child_under_a_discarded_root_is_retained() {
		let (clock, ledger) = ledger();
		let root = ledger.open_operator(OperatorKind::Spill);
		clock.advance(2);
		{
			let child = ledger.open_operator(OperatorKind::Build);
			ledger.record_acquisition(AcquisitionKind::BaseScan, 5);
			clock.advance(5);
			clock.advance(4);
			child.commit();
		}
		clock.advance(3);
		root.discard();

		let totals = ledger.seal().totals().expect("no faults");
		assert_eq!(
			totals.inclusive_operator_micros, 9,
			"the retained child's span, not the discarded root's 14"
		);
		assert_eq!(totals.exclusive_build_micros, 4);
		assert_eq!(totals.exclusive_spill_micros, 0);
		assert_eq!(totals.nested_base_scan_micros, 5);
		assert_eq!(
			totals.base_scan_micros, 5,
			"the read is in the receipt either way"
		);
		assert!(totals.identity_holds());
	}
	/// `finish()` freezes the interval. Arbitrary delay before `commit()` — the
	/// telemetry lock wait, the rest of a closure — changes nothing.
	#[test]
	fn finish_freezes_the_interval_and_delay_before_commit_changes_nothing() {
		let (clock, ledger) = ledger();
		let scope = ledger.open_operator(OperatorKind::Build);
		clock.advance(30);
		let finished = scope.finish();
		clock.advance(9_000); // lock wait, closure teardown, anything at all
		finished.commit();

		let totals = ledger.seal().totals().expect("no faults");
		assert_eq!(
			totals.inclusive_operator_micros, 30,
			"the frozen interval is what publishes"
		);
		assert_eq!(totals.exclusive_build_micros, 30);
		assert!(totals.identity_holds());
	}

	/// `finish()` then `discard()` publishes no operator term at all.
	#[test]
	fn a_finished_scope_that_discards_publishes_no_operator_term() {
		let (clock, ledger) = ledger();
		let scope = ledger.open_operator(OperatorKind::Probe);
		ledger.record_acquisition(AcquisitionKind::BaseScan, 5);
		clock.advance(5);
		clock.advance(7);
		let finished = scope.finish();
		clock.advance(100);
		finished.discard();

		let totals = ledger.seal().totals().expect("discarding is not a fault");
		assert_eq!(totals.inclusive_operator_micros, 0);
		assert_eq!(totals.exclusive_probe_micros, 0);
		assert_eq!(totals.nested_base_scan_micros, 0);
		assert_eq!(totals.base_scan_micros, 5, "the read stays in the receipt");
		assert!(totals.identity_holds());
	}

	/// Dropping the finished token is a fault, exactly as dropping the unfinished
	/// one is. Freezing a measurement is not deciding what to do with it.
	#[test]
	fn dropping_a_finished_scope_vetoes_the_receipt() {
		let (clock, ledger) = ledger();
		let scope = ledger.open_operator(OperatorKind::Build);
		clock.advance(4);
		let finished = scope.finish();
		drop(finished);

		match ledger.seal() {
			SealOutcome::Withheld(faults) => {
				assert_eq!(faults.unresolved_operator_scope, 1);
			}
			other => panic!("an unresolved finished scope must withhold, got {other:?}"),
		}
	}

	/// Nested acquisition and child terms are frozen at `finish()`: work recorded
	/// after it belongs to whatever encloses the bracket, not to the bracket.
	#[test]
	fn nested_terms_are_frozen_at_finish() {
		let (clock, ledger) = ledger();
		let outer = ledger.open_operator(OperatorKind::Probe);
		clock.advance(1);
		let inner = ledger.open_operator(OperatorKind::Build);
		ledger.record_acquisition(AcquisitionKind::BaseScan, 6);
		clock.advance(6);
		clock.advance(2);
		let finished_inner = inner.finish();

		// Recorded after the inner bracket froze: it belongs to the outer one.
		ledger.record_acquisition(AcquisitionKind::Shuffle, 4);
		clock.advance(4);
		finished_inner.commit();
		clock.advance(1);
		outer.finish().commit();

		let totals = ledger.seal().totals().expect("no faults");
		assert_eq!(
			totals.exclusive_build_micros, 2,
			"the inner bracket kept only its own work, frozen at finish"
		);
		assert_eq!(totals.nested_base_scan_micros, 6);
		assert_eq!(
			totals.nested_shuffle_micros, 4,
			"acquisition after the freeze went to the enclosing bracket"
		);
		assert!(totals.identity_holds());
	}

	/// Commit publishes exactly once — the type makes a second resolution
	/// unrepresentable, so this pins the accounting rather than the API.
	#[test]
	fn a_finished_scope_commits_exactly_once() {
		let (clock, ledger) = ledger();
		let scope = ledger.open_operator(OperatorKind::Spill);
		clock.advance(12);
		scope.finish().commit();

		let totals = ledger.seal().totals().expect("no faults");
		assert_eq!(totals.exclusive_spill_micros, 12);
		assert_eq!(totals.inclusive_operator_micros, 12);
	}

	/// A parent may not finish while a finished child is still unresolved.
	///
	/// The parent's exclusive figure would be computed without that child's span,
	/// and the child's later commit would find no live parent to transfer into —
	/// so the span would land in the inclusive total while also sitting inside the
	/// parent's exclusive time. A veto, not a silent double count.
	#[test]
	fn a_parent_finishing_before_its_finished_child_resolves_vetoes() {
		let (clock, ledger) = ledger();
		let outer = ledger.open_operator(OperatorKind::Probe);
		clock.advance(1);
		let inner = ledger.open_operator(OperatorKind::Build);
		clock.advance(5);
		let finished_inner = inner.finish();
		clock.advance(2);

		// The parent freezes while the child is still undecided.
		let finished_outer = outer.finish();
		finished_inner.commit();
		finished_outer.commit();

		match ledger.seal() {
			SealOutcome::Withheld(faults) => {
				assert_eq!(faults.finished_child_outstanding, 1);
			}
			other => panic!("an outstanding finished child must withhold, got {other:?}"),
		}
	}

	/// Resolving the child by commit clears the reservation, so the parent may
	/// then finish normally.
	#[test]
	fn a_child_committing_clears_the_parent_reservation() {
		let (clock, ledger) = ledger();
		let outer = ledger.open_operator(OperatorKind::Probe);
		clock.advance(1);
		let inner = ledger.open_operator(OperatorKind::Build);
		ledger.record_acquisition(AcquisitionKind::BaseScan, 4);
		clock.advance(4);
		clock.advance(3);
		inner.finish().commit();
		clock.advance(2);
		outer.finish().commit();

		let totals = ledger.seal().totals().expect("no faults");
		assert_eq!(totals.inclusive_operator_micros, 10);
		assert_eq!(totals.exclusive_build_micros, 3);
		assert_eq!(
			totals.exclusive_probe_micros, 3,
			"1 + 2, less the child's span"
		);
		assert_eq!(totals.nested_base_scan_micros, 4);
		assert!(totals.identity_holds());
	}

	/// Resolving the child by discard clears it too — the reservation is about
	/// having decided, not about which way.
	#[test]
	fn a_child_discarding_clears_the_parent_reservation() {
		let (clock, ledger) = ledger();
		let outer = ledger.open_operator(OperatorKind::Probe);
		clock.advance(1);
		let inner = ledger.open_operator(OperatorKind::Build);
		ledger.record_acquisition(AcquisitionKind::BaseScan, 4);
		clock.advance(4);
		clock.advance(3);
		inner.finish().discard();
		clock.advance(2);
		outer.finish().commit();

		let totals = ledger.seal().totals().expect("no faults");
		assert_eq!(totals.inclusive_operator_micros, 10);
		assert_eq!(
			totals.exclusive_build_micros, 0,
			"the child published nothing"
		);
		assert_eq!(
			totals.nested_base_scan_micros, 4,
			"but its read transferred to the committed parent"
		);
		assert_eq!(
			totals.exclusive_probe_micros, 6,
			"10 less the transferred read"
		);
		assert!(totals.identity_holds());
	}

	// ---- OperatorTransition: the 62-writer helper -------------------------

	/// Disabled telemetry reads no clock and mutates no ledger.
	#[test]
	fn a_disabled_transition_performs_no_timing_work() {
		let mut transition = OperatorTransition::open(None, OperatorKind::Build);
		assert!(matches!(transition, OperatorTransition::Disabled));
		transition.finish_at_measurement();
		assert!(matches!(transition, OperatorTransition::Disabled));
		transition.commit_after_write();
	}

	/// The closure freezes at the measurement line; teardown after it changes
	/// nothing. This is the boundary pilot 1 got wrong.
	#[test]
	fn a_transition_freezes_at_the_measurement_line_not_the_write() {
		let (clock, ledger) = ledger();
		let mut transition = OperatorTransition::open(Some(&ledger), OperatorKind::Build);
		clock.advance(25);

		// Stands in for the telemetry closure body.
		transition.finish_at_measurement();

		clock.advance(5_000); // lock teardown, trailing closure statements, anything
		transition.commit_after_write();

		let totals = ledger.seal().totals().expect("no faults");
		assert_eq!(totals.exclusive_build_micros, 25);
		assert_eq!(totals.inclusive_operator_micros, 25);
	}

	/// A closure that never ran yields a named fault, never a zero phase.
	#[test]
	fn a_skipped_measurement_closure_is_a_named_fault_not_a_zero_phase() {
		let (clock, ledger) = ledger();
		let transition = OperatorTransition::open(Some(&ledger), OperatorKind::Probe);
		clock.advance(14);

		// The closure did not run: no finish_at_measurement.
		transition.commit_after_write();

		match ledger.seal() {
			SealOutcome::Withheld(faults) => {
				assert_eq!(faults.measurement_boundary_missed, 1);
				assert_eq!(
					faults.unresolved_operator_scope, 0,
					"the scope was resolved; it was the measurement that never happened"
				);
			}
			other => panic!("a skipped measurement must withhold, got {other:?}"),
		}
	}

	/// A rejection before the measurement boundary discards cleanly, with no
	/// missed-measurement fault: nothing was supposed to be measured.
	#[test]
	fn a_pre_measurement_rejection_discards_without_a_missed_boundary_fault() {
		let (clock, ledger) = ledger();
		let transition = OperatorTransition::open(Some(&ledger), OperatorKind::Build);
		ledger.record_acquisition(AcquisitionKind::BaseScan, 3);
		clock.advance(3);
		clock.advance(6);
		transition.discard_before_measurement();

		let totals = ledger
			.seal()
			.totals()
			.expect("a clean pre-measurement rejection is not a fault");
		assert_eq!(totals.inclusive_operator_micros, 0);
		assert_eq!(totals.exclusive_build_micros, 0);
		assert_eq!(totals.base_scan_micros, 3, "the read stays in the receipt");
		assert!(totals.identity_holds());
	}

	/// Finishing twice is an impossible transition and vetoes.
	#[test]
	fn finishing_a_transition_twice_vetoes() {
		let (clock, ledger) = ledger();
		let mut transition = OperatorTransition::open(Some(&ledger), OperatorKind::Build);
		clock.advance(8);
		transition.finish_at_measurement();
		clock.advance(3);
		transition.finish_at_measurement();
		transition.commit_after_write();

		match ledger.seal() {
			SealOutcome::Withheld(faults) => assert_eq!(faults.invalid_scope_transition, 1),
			other => panic!("a double finish must withhold, got {other:?}"),
		}
	}

	/// Hoisted shape: the freeze happens **before** the telemetry closure, at the
	/// legacy `duration_micros` line, and the commit happens **inside** it after
	/// the legacy write.
	///
	/// The ordering is what distinguishes this shape from pilot 1's. Freezing
	/// inside the closure would add the telemetry-lock wait to the interval;
	/// committing outside it would publish even when the closure never ran.
	#[test]
	fn the_hoisted_shape_freezes_before_the_closure_and_commits_inside_it() {
		let (clock, ledger) = ledger();
		let mut transition = OperatorTransition::open(Some(&ledger), OperatorKind::Build);
		clock.advance(18);

		// Legacy: `let build_micros = duration_micros(build_started);`
		transition.finish_at_measurement();

		// The closure runs: lock wait, then the legacy write, then the commit.
		clock.advance(700); // lock wait — must not enter the interval
		transition.commit_after_write();

		let totals = ledger.seal().totals().expect("no faults");
		assert_eq!(
			totals.exclusive_build_micros, 18,
			"the lock wait after the hoisted measurement is excluded"
		);
	}

	/// The reason the commit sits inside the closure: a closure that never runs
	/// leaves the finished token unresolved, which vetoes rather than publishing
	/// a phase whose legacy writer never executed.
	#[test]
	fn a_hoisted_shape_whose_closure_never_runs_vetoes() {
		let (clock, ledger) = ledger();
		let mut transition = OperatorTransition::open(Some(&ledger), OperatorKind::Build);
		clock.advance(18);
		transition.finish_at_measurement();

		// The closure is never invoked, so it is dropped still holding the token.
		let never_ran = move || transition.commit_after_write();
		drop(never_ran);

		match ledger.seal() {
			SealOutcome::Withheld(faults) => {
				assert_eq!(faults.unresolved_operator_scope, 1);
				assert_eq!(
					faults.measurement_boundary_missed, 0,
					"the measurement happened; it was the publication that did not"
				);
			}
			other => panic!("an unrun closure must withhold, got {other:?}"),
		}
	}

	/// A diagnostic read of the timer does not freeze it.
	///
	/// Some envelopes read `duration_micros(started)` twice: once conditionally
	/// for a throughput profile, and once for the legacy phase write. Only the
	/// second is the authoritative measurement. A "first duration read wins"
	/// refactor would shorten attribution — and only when profiling happens to be
	/// enabled, which is a configuration-dependent discrepancy nobody would find
	/// by reading the code.
	#[test]
	fn a_profile_read_before_the_phase_measurement_does_not_shorten_attribution() {
		let (clock, ledger) = ledger();
		let mut transition = OperatorTransition::open(Some(&ledger), OperatorKind::Build);

		clock.advance(10);
		// Diagnostic profile read happens here. It observes 10 and freezes nothing.
		clock.advance(20);
		// The authoritative phase measurement, inside the telemetry closure.
		transition.finish_at_measurement();
		transition.commit_after_write();

		let totals = ledger.seal().totals().expect("no faults");
		assert_eq!(
			totals.exclusive_build_micros, 30,
			"the phase measurement is authoritative, not the earlier profile read"
		);
	}

	/// One timer start may feed several mutually exclusive legacy writers after
	/// fallback attempts. A fallback that selects no route leaves the transition
	/// active; exactly the writer eventually selected freezes and publishes it.
	#[test]
	fn one_start_can_publish_at_exactly_one_alternative_writer() {
		let (clock, ledger) = ledger();
		let mut transition = OperatorTransition::open(Some(&ledger), OperatorKind::Build);

		clock.advance(7);
		let first_candidate = None::<()>;
		assert!(first_candidate.is_none(), "the first route falls through");

		clock.advance(13);
		// The second route is selected. This is its legacy phase measurement and
		// write; no second transition is opened for the same start.
		transition.finish_at_measurement();
		transition.commit_after_write();

		let totals = ledger.seal().totals().expect("no faults");
		assert_eq!(totals.exclusive_build_micros, 20);
		assert_eq!(totals.inclusive_operator_micros, 20);
	}
}

/// A bracket's frozen measurement, before it is known whether it publishes.
#[derive(Debug, Clone, Copy)]
struct FinishedFrame {
	kind: OperatorKind,
	parent_id: Option<u64>,
	inclusive: u64,
	exclusive: u64,
	nested: NestedTerms,
	nested_child_operator_micros: u64,
	faulted: bool,
}

impl FinishedFrame {
	fn faulted() -> Self {
		Self {
			kind: OperatorKind::Build,
			parent_id: None,
			inclusive: 0,
			exclusive: 0,
			nested: NestedTerms::default(),
			nested_child_operator_micros: 0,
			faulted: true,
		}
	}
}

/// A bracket whose timer has stopped but whose disposition is still open.
///
/// ADR 551: the legacy interval ends at the duration-measurement point, which is
/// frequently earlier than the place the decision can be made — in the join paths
/// the measurement sits inside the telemetry closure, after the mutex is taken.
/// Freezing at that boundary and publishing later reproduces the legacy interval
/// exactly. Delay between the two changes nothing.
///
/// Consuming `commit`/`discard` make double resolution unrepresentable, and
/// dropping unresolved is a fault just as it is before `finish`.
#[derive(Debug)]
pub struct FinishedOperatorScope {
	ledger: Arc<TaskPhaseLedger>,
	finished: FinishedFrame,
	resolved: bool,
}

impl FinishedOperatorScope {
	/// Publish the frozen interval at the legacy writer.
	pub fn commit(mut self) {
		self.resolved = true;
		self.ledger.commit_finished(&self.finished);
	}

	/// Drop the frozen interval: this bracket is not authoritative.
	pub fn discard(mut self) {
		self.resolved = true;
		self.ledger.discard_finished(&self.finished);
	}
}

impl Drop for FinishedOperatorScope {
	fn drop(&mut self) {
		if !self.resolved {
			self.ledger.record_unresolved_scope();
		}
	}
}

/// The lifecycle of one legacy phase envelope, as a single value.
///
/// ADR 551: the measurement boundary and the publication boundary are different
/// points — the legacy `duration_micros(started)` is evaluated inside the
/// telemetry closure, after the mutex is acquired, while the decision to publish
/// can only be made after that closure returns. Two independent `Option`s across
/// that gap would let "disabled", "never measured" and "measured but undecided"
/// blur together, and 62 sites would each re-derive the same slot logic.
///
/// The closure performs exactly one transition: `Active` → `Finished`.
#[derive(Debug)]
pub enum OperatorTransition {
	/// No receipt is being collected: no clock read, no ledger mutation.
	Disabled,
	/// Timing, measurement not yet taken.
	Active(OperatorScope),
	/// Measurement frozen at the legacy boundary, publication undecided.
	Finished(FinishedOperatorScope),
}

impl OperatorTransition {
	/// Open a bracket, or `Disabled` when no receipt is being collected.
	pub fn open(phases: Option<&Arc<TaskPhaseLedger>>, kind: OperatorKind) -> Self {
		match phases {
			Some(phases) => Self::Active(phases.open_operator(kind)),
			None => Self::Disabled,
		}
	}

	/// Freeze the interval, called **inside** the telemetry closure at the exact
	/// line the legacy duration is measured.
	pub fn finish_at_measurement(&mut self) {
		let taken = std::mem::replace(self, Self::Disabled);
		*self = match taken {
			Self::Active(scope) => Self::Finished(scope.finish()),
			Self::Disabled => Self::Disabled,
			// Finishing twice: the second call would measure a boundary that has
			// already passed.
			Self::Finished(finished) => {
				finished.ledger.record_invalid_transition();
				Self::Finished(finished)
			}
		};
	}

	/// Publish, after the legacy write has completed.
	pub fn commit_after_write(self) {
		match self {
			Self::Finished(finished) => finished.commit(),
			Self::Disabled => {}
			// The closure never ran, so nothing was measured where the legacy
			// value would have been taken. Publishing zero would claim the phase
			// did no work; this says it was never measured.
			Self::Active(scope) => {
				scope.ledger.record_measurement_boundary_missed();
				scope.discard();
			}
		}
	}

	/// Reject before the measurement boundary is reached.
	pub fn discard_before_measurement(self) {
		match self {
			Self::Active(scope) => scope.discard(),
			Self::Disabled => {}
			// Already measured: this is the wrong stage to be rejecting from, so
			// it vetoes — but the token is still resolved so nothing leaks.
			Self::Finished(finished) => {
				finished.ledger.record_invalid_transition();
				finished.discard();
			}
		}
	}
}
