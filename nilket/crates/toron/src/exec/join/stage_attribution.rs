//! ADR 507: the composed-gap attribution contract.
//!
//! Toron already grew two stage-attribution profilers independently — `ChainStageProfiler` for the
//! reoriented grouped-chain path and `LocalJoinPhaseAccumulator` for the fused inner-join global
//! aggregate — and both, without coordinating, converged on the same shape: a set of named stages
//! carrying time and row flow, a declared *total*, and an explicit *unattributed residual* so any
//! un-timed span reads as an honest gap rather than false precision. That convergence is the signal
//! that the shape is real. This module makes it a contract: one set of reconciliation rules every
//! profile must satisfy, so a number emitted by one path means the same thing as the same number
//! emitted by another — or is explicitly declared not to.
//!
//! The contract is pure arithmetic over already-measured counters. It samples nothing and performs
//! no I/O; a profiler feeds it the numbers it collected and asks whether they reconcile. That keeps
//! it testable against a real emitted profile (see the golden profile below) rather than an ideal.
//!
//! ## Two things the grounding forced into the contract that prose alone missed
//!
//! **Time reconciles in nested bands, not a single subtraction.** The chain profiler's stage timers
//! sum to its *drive* span, not to `execute_ms`: the reoriented-chain replay attributes 10.68 s of stages inside an 11.14 s
//! drive (a 0.45 s in-drive residual), but its `execute_ms` is ~17.4 s. The ~6.3 s between
//! drive and execute — build, load, finalize — is a second band the single-profiler view never
//! named. The contract models the chain explicitly: `stage_sum <= drive <= execute`, each step's
//! residual reported, so "reconcile with execute" cannot silently mean "reconcile with drive".
//!
//! **Byte counters come in three kinds, and only one of them sums.** From the reoriented-chain replay path's real fields:
//!   - *flow* — bytes that pass once. `build_spill_bytes` + `probe_spill_bytes` reconcile to a spill
//!     total by addition.
//!   - *peak* — a high-water residency (`max_index_bytes`, `max_pinned_build_bytes`). Reconciles by
//!     `max`, and adding it into a flow total is a category error the contract refuses to express.
//!   - *subset of a wider scope* — a counter deliberately narrower than the whole it sits inside.
//!     the reoriented-chain replay-1 `build_spill_bytes + probe_spill_bytes` (897,712,848) is single-replay
//!     reservoir scope; the query's `spill_bytes` telemetry (897,713,384) is whole-query scope over
//!     every reservoir and replay. The 536-byte remainder is spill outside replay 1, by
//!     construction — the two counters were never required to sum. This is the ADR-507 step-2
//!     determination: *declared non-summing, whole-query is a superset*, not unattributed I/O.

use serde::Serialize;

/// Which physical quantity a byte counter measures, and therefore how counters in that domain
/// combine. The kind is what stops a residency high-water from being added into a throughput total.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::exec) enum ByteDomainKind {
	/// Additive: bytes that flow through once. Parts sum to the domain whole. Reader-side (the flow
	/// reconciler it labels needs the whole-query counter); see the boundary note below.
	#[allow(dead_code)]
	Flow,
	/// A high-water footprint. Reconciles by `max`; summing parts is meaningless.
	Peak,
}

/// A reconciliation could not hold: the parts describe an impossible relationship to the whole.
/// Distinct from a *residual within boundary*, which is a normal, reported outcome — this is the
/// contract catching a profile that does not add up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::exec) enum AttributionError {
	/// A nested time band is inverted: an inner span exceeds the outer one that must contain it.
	TimeBandInverted {
		inner: &'static str,
		inner_micros: u64,
		outer: &'static str,
		outer_micros: u64,
	},
	/// Flow parts sum to more than the domain whole, past the declared boundary — the counters
	/// cannot all be measuring the same non-overlapping bytes.
	FlowOverAttributed {
		domain: &'static str,
		parts_total: u64,
		whole: u64,
	},
	/// A flow residual is within the whole but outside the boundary declared before measuring.
	FlowResidualOverBoundary {
		domain: &'static str,
		residual: u64,
		boundary: u64,
	},
	/// A declared subset is larger than the wider scope it is supposed to sit inside.
	SubsetExceedsWiderScope {
		domain: &'static str,
		subset: u64,
		wider_whole: u64,
	},
}

impl std::fmt::Display for AttributionError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::TimeBandInverted {
				inner,
				inner_micros,
				outer,
				outer_micros,
			} => write!(
				f,
				"attribution time band inverted: {inner} ({inner_micros}us) exceeds enclosing {outer} ({outer_micros}us)"
			),
			Self::FlowOverAttributed {
				domain,
				parts_total,
				whole,
			} => write!(
				f,
				"attribution flow domain {domain} over-attributed: parts sum {parts_total} exceeds whole {whole}"
			),
			Self::FlowResidualOverBoundary {
				domain,
				residual,
				boundary,
			} => write!(
				f,
				"attribution flow domain {domain} residual {residual} exceeds declared boundary {boundary}"
			),
			Self::SubsetExceedsWiderScope {
				domain,
				subset,
				wider_whole,
			} => write!(
				f,
				"attribution subset domain {domain} subset {subset} exceeds wider scope {wider_whole}"
			),
		}
	}
}

/// The three nested time spans of any attributed execution, innermost first.
///
/// `stage_attributed <= drive <= execute` is an invariant, not a hope: a profiler that reports
/// otherwise has double-counted or mismeasured, and `reconcile` refuses it. The two residuals are
/// first-class outputs — the in-drive gap (framework overhead inside the timed loop) and the
/// drive-to-execute gap (setup, build, finalize outside it) — so no reader has to rediscover that
/// stage timers do not cover the whole query.
///
/// Reader-side: the outer `execute` band comes from the perf row, which the profiler does not hold,
/// so a profiler reconciles only the inner band via [`reconcile_stage_within_drive`]. The full
/// three-band form is the step-3 reader's, and is pinned against the reoriented-chain replay's real numbers in tests now.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub(in crate::exec) struct TimeBands {
	pub stage_attributed_micros: u64,
	pub drive_total_micros: u64,
	pub execute_total_micros: u64,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(in crate::exec) struct TimeReconciliation {
	/// Time inside the drive loop not attributed to any stage.
	pub in_drive_residual_micros: u64,
	/// Time in the query outside the drive loop entirely (build, load, finalize).
	pub drive_to_execute_micros: u64,
}

/// Reconcile the innermost time band alone: attributed stages within their drive span.
///
/// A profiler knows its stage timers and its drive total but not the query's `execute_ms` — that is
/// added by the reader who also holds the perf row. This is the profiler's entry point; it returns
/// the in-drive residual and refuses an inverted band, and [`TimeBands::reconcile`] reuses it for the
/// inner step of the full three-band chain.
pub(in crate::exec) fn reconcile_stage_within_drive(
	stage_attributed_micros: u64,
	drive_total_micros: u64,
) -> Result<u64, AttributionError> {
	drive_total_micros
		.checked_sub(stage_attributed_micros)
		.ok_or(AttributionError::TimeBandInverted {
			inner: "stage_attributed",
			inner_micros: stage_attributed_micros,
			outer: "drive_total",
			outer_micros: drive_total_micros,
		})
}

#[allow(dead_code)]
impl TimeBands {
	pub(in crate::exec) fn reconcile(self) -> Result<TimeReconciliation, AttributionError> {
		let in_drive_residual_micros =
			reconcile_stage_within_drive(self.stage_attributed_micros, self.drive_total_micros)?;
		let drive_to_execute_micros = self
			.execute_total_micros
			.checked_sub(self.drive_total_micros)
			.ok_or(AttributionError::TimeBandInverted {
				inner: "drive_total",
				inner_micros: self.drive_total_micros,
				outer: "execute_total",
				outer_micros: self.execute_total_micros,
			})?;
		Ok(TimeReconciliation {
			in_drive_residual_micros,
			drive_to_execute_micros,
		})
	}
}

/// A flow byte domain: named parts that must sum to a whole within a boundary declared up front.
///
/// The boundary is stated before the numbers are read, per ADR 507 — a tolerance chosen after seeing
/// the residual is not a boundary. A residual within it is reported as unattributed bytes *in this
/// domain*; a residual outside it, or parts exceeding the whole, is an error, not a footnote. The
/// node-attribution profiler reconciles per-node exclusive spill against the root spill delta through
/// this.
#[derive(Debug, Clone)]
pub(in crate::exec) struct FlowByteDomain<'a> {
	pub name: &'static str,
	pub parts: &'a [(&'static str, u64)],
	pub whole: u64,
	pub boundary: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(in crate::exec) struct FlowByteReconciliation {
	pub domain: &'static str,
	pub parts_total: u64,
	pub whole: u64,
	/// Bytes in the whole not covered by any named part. First-class, never a subtraction the reader
	/// performs.
	pub unattributed_bytes: u64,
}

impl FlowByteDomain<'_> {
	pub(in crate::exec) fn reconcile(&self) -> Result<FlowByteReconciliation, AttributionError> {
		let parts_total = self
			.parts
			.iter()
			.fold(0u64, |total, (_, bytes)| total.saturating_add(*bytes));
		let unattributed_bytes =
			self.whole
				.checked_sub(parts_total)
				.ok_or(AttributionError::FlowOverAttributed {
					domain: self.name,
					parts_total,
					whole: self.whole,
				})?;
		if unattributed_bytes > self.boundary {
			return Err(AttributionError::FlowResidualOverBoundary {
				domain: self.name,
				residual: unattributed_bytes,
				boundary: self.boundary,
			});
		}
		Ok(FlowByteReconciliation {
			domain: self.name,
			parts_total,
			whole: self.whole,
			unattributed_bytes,
		})
	}
}

// The subset reconciler below is the reader-side half of the byte contract: the whole-query spill
// counter it needs is assembled by the driver after the profiler's final flush, so it is joined from
// a perf row, not emitted by a profiler. Pinned against the reoriented-chain replay's real numbers in
// tests; annotated rather than fabricating a premature in-engine caller.
/// A counter deliberately narrower than a whole it nests inside — the resolution the contract offers
/// instead of forcing two differently-scoped counters to sum. The subset must not exceed the wider
/// scope; the remainder is attributed elsewhere on purpose, and the `reason` says where.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(in crate::exec) struct SubsetByteReconciliation {
	pub domain: &'static str,
	pub subset: u64,
	pub wider_whole: u64,
	/// `wider_whole - subset`: bytes accounted to a wider scope than this domain measures, by design.
	pub remainder: u64,
	pub reason: &'static str,
}

/// Reconcile a subset counter against the wider-scoped whole it lives inside. `Ok` iff the subset
/// does not exceed the whole; the remainder is returned, never treated as an error.
#[allow(dead_code)]
pub(in crate::exec) fn reconcile_subset_of_wider_scope(
	domain: &'static str,
	subset: u64,
	wider_whole: u64,
	reason: &'static str,
) -> Result<SubsetByteReconciliation, AttributionError> {
	let remainder =
		wider_whole
			.checked_sub(subset)
			.ok_or(AttributionError::SubsetExceedsWiderScope {
				domain,
				subset,
				wider_whole,
			})?;
	Ok(SubsetByteReconciliation {
		domain,
		subset,
		wider_whole,
		remainder,
		reason,
	})
}

/// Reconcile a peak (high-water) domain: the domain footprint is the `max` of its parts, never the
/// sum. Returned as a distinct type so a peak can never be handed to a flow reconciler by mistake.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(in crate::exec) struct PeakByteReconciliation {
	pub domain: &'static str,
	pub kind: ByteDomainKind,
	pub peak_bytes: u64,
}

pub(in crate::exec) fn reconcile_peak_domain(
	domain: &'static str,
	parts: &[(&'static str, u64)],
) -> PeakByteReconciliation {
	let peak_bytes = parts.iter().map(|(_, bytes)| *bytes).max().unwrap_or(0);
	PeakByteReconciliation {
		domain,
		kind: ByteDomainKind::Peak,
		peak_bytes,
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	// the reoriented grouped-chain path's actual emitted profile (a real reoriented grouped-chain replay profile) and its perf row. The
	// contract is pinned against real numbers, so a change that makes the arithmetic diverge from a
	// query Toron actually ran fails here rather than passing against an idealized fixture.
	mod reoriented_chain_golden {
		pub const STAGE_MICROS: [u64; 7] = [
			624_145, 1_408_041, 7_987_837, 435_502, 114_561, 101_211, 9_588,
		];
		pub const DRIVE_TOTAL_MICROS: u64 = 11_135_092;
		pub const IN_DRIVE_RESIDUAL_MICROS: u64 = 454_207;
		// execute_ms_median 16,883.9 for the profile run -> micros.
		pub const EXECUTE_MICROS: u64 = 16_883_900;

		pub const BUILD_SPILL_BYTES: u64 = 24_317_328;
		pub const PROBE_SPILL_BYTES: u64 = 873_395_520;
		pub const WHOLE_QUERY_SPILL_BYTES: u64 = 897_713_384;
		pub const REPLAY1_REMAINDER_BYTES: u64 = 536;

		pub const MAX_INDEX_BYTES: u64 = 134_217_728;
		pub const MAX_PINNED_BUILD_BYTES: u64 = 23_426_616;

		pub fn stage_sum() -> u64 {
			STAGE_MICROS.iter().sum()
		}
	}

	#[test]
	fn reoriented_chain_time_reconciles_in_nested_bands() {
		let bands = TimeBands {
			stage_attributed_micros: reoriented_chain_golden::stage_sum(),
			drive_total_micros: reoriented_chain_golden::DRIVE_TOTAL_MICROS,
			execute_total_micros: reoriented_chain_golden::EXECUTE_MICROS,
		};
		let reconciliation = bands.reconcile().expect("nested bands reconcile");

		// The in-drive residual is the profiler's own published figure, to the microsecond.
		assert_eq!(
			reconciliation.in_drive_residual_micros,
			reoriented_chain_golden::IN_DRIVE_RESIDUAL_MICROS
		);
		assert_eq!(
			reoriented_chain_golden::stage_sum() + reconciliation.in_drive_residual_micros,
			reoriented_chain_golden::DRIVE_TOTAL_MICROS
		);
		// The second band the single-profiler view never named: ~5.75 s of the ~16.9 s execute is
		// outside the drive loop. Its existence is the point — the contract forbids reconciling
		// stages straight to execute as if this band were zero.
		assert_eq!(
			reconciliation.drive_to_execute_micros,
			reoriented_chain_golden::EXECUTE_MICROS - reoriented_chain_golden::DRIVE_TOTAL_MICROS
		);
		assert!(reconciliation.drive_to_execute_micros > 5_000_000);
	}

	#[test]
	fn inverted_time_band_is_rejected_not_clamped() {
		let bands = TimeBands {
			stage_attributed_micros: 100,
			drive_total_micros: 90,
			execute_total_micros: 200,
		};
		assert_eq!(
			bands.reconcile(),
			Err(AttributionError::TimeBandInverted {
				inner: "stage_attributed",
				inner_micros: 100,
				outer: "drive_total",
				outer_micros: 90,
			})
		);
	}

	#[test]
	fn reoriented_chain_replay1_spill_is_a_declared_subset_not_unattributed_io() {
		// The step-2 determination, executable. Replay-1 build+probe is single-replay reservoir
		// scope; whole-query spill telemetry is a superset over every reservoir and replay. Forcing
		// them into a flow domain would call the 536-byte remainder "unattributed I/O" — wrong. The
		// contract models it as a subset, and the remainder is spill outside replay 1, by design.
		let subset =
			reoriented_chain_golden::BUILD_SPILL_BYTES + reoriented_chain_golden::PROBE_SPILL_BYTES;
		let reconciliation = reconcile_subset_of_wider_scope(
			"replay1_reservoir_spill",
			subset,
			reoriented_chain_golden::WHOLE_QUERY_SPILL_BYTES,
			"whole-query spill telemetry spans every reservoir and replay; replay-1 covers one",
		)
		.expect("replay-1 spill is a subset of whole-query spill");

		assert_eq!(
			reconciliation.remainder,
			reoriented_chain_golden::REPLAY1_REMAINDER_BYTES
		);
		assert_eq!(reconciliation.subset, 897_712_848);
	}

	#[test]
	fn a_clean_flow_domain_reconciles_within_boundary() {
		// If the two counters HAD shared a scope, this is how they would reconcile — flow, summing,
		// residual as unattributed bytes. Boundary declared before the numbers: 1 KiB.
		let parts = [
			("build", reoriented_chain_golden::BUILD_SPILL_BYTES),
			("probe", reoriented_chain_golden::PROBE_SPILL_BYTES),
		];
		let domain = FlowByteDomain {
			name: "same_scope_spill",
			parts: &parts,
			whole: reoriented_chain_golden::WHOLE_QUERY_SPILL_BYTES,
			boundary: 1024,
		};
		let reconciliation = domain.reconcile().expect("within 1 KiB boundary");
		assert_eq!(reconciliation.unattributed_bytes, 536);
		assert_eq!(reconciliation.parts_total, 897_712_848);
	}

	#[test]
	fn a_flow_residual_past_its_boundary_is_an_error() {
		let parts = [("build", 100), ("probe", 200)];
		let domain = FlowByteDomain {
			name: "tight",
			parts: &parts,
			whole: 1000,
			boundary: 50,
		};
		assert_eq!(
			domain.reconcile(),
			Err(AttributionError::FlowResidualOverBoundary {
				domain: "tight",
				residual: 700,
				boundary: 50,
			})
		);
	}

	#[test]
	fn flow_parts_exceeding_the_whole_are_over_attribution() {
		let parts = [("a", 600), ("b", 600)];
		let domain = FlowByteDomain {
			name: "overlap",
			parts: &parts,
			whole: 1000,
			boundary: u64::MAX,
		};
		assert_eq!(
			domain.reconcile(),
			Err(AttributionError::FlowOverAttributed {
				domain: "overlap",
				parts_total: 1200,
				whole: 1000,
			})
		);
	}

	#[test]
	fn a_subset_larger_than_its_wider_scope_is_rejected() {
		assert_eq!(
			reconcile_subset_of_wider_scope("bad", 1000, 900, "reason"),
			Err(AttributionError::SubsetExceedsWiderScope {
				domain: "bad",
				subset: 1000,
				wider_whole: 900,
			})
		);
	}

	#[test]
	fn peak_bytes_reconcile_by_max_never_by_sum() {
		// The residency high-waters. Adding them would invent 157 MB of footprint that never existed
		// simultaneously; the domain footprint is the larger of the two.
		let parts = [
			("index", reoriented_chain_golden::MAX_INDEX_BYTES),
			(
				"pinned_build",
				reoriented_chain_golden::MAX_PINNED_BUILD_BYTES,
			),
		];
		let reconciliation = reconcile_peak_domain("replay1_residency_peak", &parts);
		assert_eq!(reconciliation.kind, ByteDomainKind::Peak);
		assert_eq!(
			reconciliation.peak_bytes,
			reoriented_chain_golden::MAX_INDEX_BYTES
		);
		assert!(
			reconciliation.peak_bytes
				< reoriented_chain_golden::MAX_INDEX_BYTES
					+ reoriented_chain_golden::MAX_PINNED_BUILD_BYTES
		);
	}
}
