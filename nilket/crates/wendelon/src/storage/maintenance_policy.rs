//! ADR 0097 decision 4: how a maintenance failure is classified, and what the
//! scheduler is allowed to do about it.
//!
//! The live incident this exists for: an LSM compaction refused the same slab with
//! the same shape-dependent error every 100 ms for hours, rebuilding a 21 GiB
//! working set each time. Retrying a failure that is a function of the input's
//! shape cannot succeed, so the cost has to be bounded.
//!
//! Classification happens at the failure's ORIGIN, not by inspecting
//! `io::ErrorKind` after the fact. A blanket rule over kinds would be wrong in both
//! directions: `NotFound` is a race with retirement in one place and a broken
//! ownership invariant in another, and `InvalidInput` covers both a malformed
//! request and a structural defect in stored bytes. A site that knows its failure
//! is shape-dependent says so by constructing the error with
//! [`deterministic_failure`]; everything else is retryable by default.
//!
//! That default favours AVAILABILITY, not safety, and the tradeoff is real: an
//! untagged deterministic failure is never quarantined, so it retries forever at
//! the 5 s cooldown cap. That is far cheaper than the 100 ms loop this replaces,
//! but it is still an unbounded loop, and it is invisible unless someone is
//! looking. The `retryable` metric label therefore doubles as the
//! missing-tag signal: a retryable class that never clears is either a race that
//! never resolves or a deterministic failure whose origin was never tagged, and
//! both want a human.

use crate::schema::SlabId;
use crate::storage::index::PersistentLocalIndex;
use crate::storage::lsm::CompactionFailure;
use std::collections::BTreeMap;
use std::fmt;
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use uuid::Uuid;

/// Why a maintenance attempt cannot succeed by being repeated.
///
/// Bounded metric labels only. This is NOT persisted: the receipt key carries the
/// [`DeterministicOrigin`], and the category is derived from it. Persisting both
/// would create a consistency invariant - origin and category agreeing - that the
/// decoder would have to enforce on every read, for no gain.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DeterministicFailureReason {
	/// The output writer cannot express the shape the plan asked for.
	UnsupportedFrameShape,
	/// Stored bytes do not satisfy an invariant the rewrite depends on.
	CorruptSourceFrame,
	/// A rewrite was attempted without the ownership that makes it safe.
	OwnershipViolation,
	/// A digest or domain invariant did not hold across the rewrite.
	InvarianceViolation,
}

impl DeterministicFailureReason {
	pub const ALL: [Self; 4] = [
		Self::UnsupportedFrameShape,
		Self::CorruptSourceFrame,
		Self::OwnershipViolation,
		Self::InvarianceViolation,
	];

	/// Bounded label for metrics. Slab ids and messages belong in the receipt.
	pub const fn label(self) -> &'static str {
		match self {
			Self::UnsupportedFrameShape => "unsupported_frame_shape",
			Self::CorruptSourceFrame => "corrupt_source_frame",
			Self::OwnershipViolation => "ownership_violation",
			Self::InvarianceViolation => "invariance_violation",
		}
	}
}

/// ADR 0097 decision 4: the specific SITE a deterministic failure came from.
///
/// Distinct from [`DeterministicFailureReason`], which is the bounded metric
/// category. Nine of these sites map to `CorruptSourceFrame`, and keying the
/// attempt budget on the category would pool them: an unrelated defect could spend
/// another's budget, and a slab that failed once for each of three reasons would
/// quarantine as though one failure had recurred three times.
///
/// Origin-specific keys also stop one site inheriting another's PRE-QUARANTINE
/// attempts across a binary upgrade. They do not expire a quarantine: a persisted
/// quarantine survives upgrades deliberately, and is cleared only by the explicit
/// requeue operation, because a new binary is a claim that the defect is fixed and
/// not evidence of it.
///
/// Codes are explicit and permanent: they are written into durable receipts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum DeterministicOrigin {
	LsmWriterCannotSplitMultiRowFrame,
	LsmRewriteProducedNoSurvivors,
	LsmRewriteFrameNotMultiRow,
	LsmRewriteDuplicateRowIdentity,
	LsmRewriteKeptWrongRowCount,
	LsmSourceFrameMissingPlannedSurvivor,
	LsmInconsistentSourceFrames,
	LsmRewrittenFrameCarriesUnplannedRow,
	LsmMultiRowSourceFrameHasNoEntries,
	LsmOutputLocationMissingPlannedRow,
	MaintenanceRewriteWithoutClaim,
	SidecarDigestMismatch,
	VisibleDomainInvariance,
}

impl DeterministicOrigin {
	pub(crate) const fn code(self) -> u16 {
		match self {
			Self::LsmWriterCannotSplitMultiRowFrame => 1,
			Self::LsmRewriteProducedNoSurvivors => 2,
			Self::LsmRewriteFrameNotMultiRow => 3,
			Self::LsmRewriteDuplicateRowIdentity => 4,
			Self::LsmRewriteKeptWrongRowCount => 5,
			Self::LsmSourceFrameMissingPlannedSurvivor => 6,
			Self::LsmInconsistentSourceFrames => 7,
			Self::LsmRewrittenFrameCarriesUnplannedRow => 8,
			Self::LsmMultiRowSourceFrameHasNoEntries => 9,
			Self::LsmOutputLocationMissingPlannedRow => 10,
			Self::MaintenanceRewriteWithoutClaim => 11,
			Self::SidecarDigestMismatch => 12,
			Self::VisibleDomainInvariance => 13,
		}
	}

	pub(crate) const fn from_code(code: u16) -> Option<Self> {
		Some(match code {
			1 => Self::LsmWriterCannotSplitMultiRowFrame,
			2 => Self::LsmRewriteProducedNoSurvivors,
			3 => Self::LsmRewriteFrameNotMultiRow,
			4 => Self::LsmRewriteDuplicateRowIdentity,
			5 => Self::LsmRewriteKeptWrongRowCount,
			6 => Self::LsmSourceFrameMissingPlannedSurvivor,
			7 => Self::LsmInconsistentSourceFrames,
			8 => Self::LsmRewrittenFrameCarriesUnplannedRow,
			9 => Self::LsmMultiRowSourceFrameHasNoEntries,
			10 => Self::LsmOutputLocationMissingPlannedRow,
			11 => Self::MaintenanceRewriteWithoutClaim,
			12 => Self::SidecarDigestMismatch,
			13 => Self::VisibleDomainInvariance,
			_ => return None,
		})
	}

	/// The bounded metric category this site reports under.
	pub(crate) const fn reason(self) -> DeterministicFailureReason {
		match self {
			Self::LsmWriterCannotSplitMultiRowFrame => {
				DeterministicFailureReason::UnsupportedFrameShape
			}
			Self::MaintenanceRewriteWithoutClaim => DeterministicFailureReason::OwnershipViolation,
			Self::SidecarDigestMismatch | Self::VisibleDomainInvariance => {
				DeterministicFailureReason::InvarianceViolation
			}
			_ => DeterministicFailureReason::CorruptSourceFrame,
		}
	}
}

/// ADR 0097 decision 4: WHICH maintenance job the work belongs to.
///
/// Part of the work identity alongside the slab set and the origin. A job's
/// success proves only that ITS rewrite over those slabs now works: a completed
/// sidecar backfill over a slab says nothing about the LSM rewrite that failed on
/// it, and a claim-violation origin is raised by a function retention and
/// tombstone reclaim share, so the origin alone cannot say whose budget was spent.
/// Without the operation in the identity, unrelated success over the same slab
/// set erased a receipt and restarted its attempt count at one.
///
/// Admission is decided per operation for the same reason: a receipt one job can
/// never clear must not bar another job from the slabs.
///
/// Codes are explicit and permanent: they are written into durable receipts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum MaintenanceOperation {
	LsmCompaction,
	RetentionPrune,
	SecondaryIndexBackfill,
	TombstoneReclaim,
}

impl MaintenanceOperation {
	#[cfg(test)]
	pub(crate) const ALL: [Self; 4] = [
		Self::LsmCompaction,
		Self::RetentionPrune,
		Self::SecondaryIndexBackfill,
		Self::TombstoneReclaim,
	];

	pub(crate) const fn code(self) -> u16 {
		match self {
			Self::LsmCompaction => 1,
			Self::RetentionPrune => 2,
			Self::SecondaryIndexBackfill => 3,
			Self::TombstoneReclaim => 4,
		}
	}

	pub(crate) const fn from_code(code: u16) -> Option<Self> {
		Some(match code {
			1 => Self::LsmCompaction,
			2 => Self::RetentionPrune,
			3 => Self::SecondaryIndexBackfill,
			4 => Self::TombstoneReclaim,
			_ => return None,
		})
	}

	/// Bounded label for logs and the report.
	pub(crate) const fn label(self) -> &'static str {
		match self {
			Self::LsmCompaction => "lsm_compaction",
			Self::RetentionPrune => "retention_prune",
			Self::SecondaryIndexBackfill => "secondary_index_backfill",
			Self::TombstoneReclaim => "tombstone_reclaim",
		}
	}
}

/// What a COMPLETED attempt proved, as declared by the caller that knows.
///
/// `execute` cannot tell from an `Ok(T)`: retention returns `Ok` with no
/// compaction when a slab had nothing to prune, and a backfill candidate replaced
/// before its claim is neither success nor failure. Only a rewrite that actually
/// published is evidence that the failure a receipt records is gone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SuccessDisposition {
	/// The job published a rewrite over the slab set. Clears this operation's
	/// receipts and cooldown for the set.
	Rewrote,
	/// The job owned the slab set, scanned it, and found nothing to rewrite.
	/// Every receipt stands as it was; ADR 0098 decision 2 records a neutral
	/// cooldown so the same scan is not repeated on the next tick.
	Neutral,
	/// The job did not get to rewrite: the claim found the slab set pinned,
	/// retired, or replaced, or the scan found work a pin gate deferred. Nothing
	/// is proven about the set's contents. The reservation is released and
	/// nothing else is recorded: the next attempt is still the next attempt.
	Unclaimed,
}

/// The payload a deterministic failure carries inside an `io::Error`, so the class
/// travels with the error through the `io::Result` signatures the storage paths
/// already use, without every one of them changing its error type.
#[derive(Debug)]
pub(crate) struct DeterministicFailure {
	origin: DeterministicOrigin,
	detail: String,
}

impl DeterministicFailure {
	pub(crate) fn origin(&self) -> DeterministicOrigin {
		self.origin
	}
}

impl fmt::Display for DeterministicFailure {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.detail)
	}
}

impl std::error::Error for DeterministicFailure {}

/// Construct an `io::Error` that names itself deterministic.
///
/// `kind` stays whatever the site would otherwise have used, so existing callers
/// that match on it are unaffected; the class rides along in the payload.
pub(crate) fn deterministic_failure(
	kind: ErrorKind,
	origin: DeterministicOrigin,
	detail: impl Into<String>,
) -> Error {
	Error::new(
		kind,
		DeterministicFailure {
			origin,
			detail: detail.into(),
		},
	)
}

/// What the scheduler may do with a failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MaintenanceFailureClass {
	/// Repeating the work may succeed: a race, or a transient I/O condition. Gets a
	/// short cooldown so a busy loser does not spin, but never quarantines.
	Retryable,
	/// Repeating the work will fail the same way. Backs off on a slow schedule and
	/// quarantines after a bounded number of attempts.
	Deterministic(DeterministicOrigin),
}

#[cfg(test)]
impl MaintenanceFailureClass {
	pub const fn is_deterministic(self) -> bool {
		matches!(self, Self::Deterministic(_))
	}

	/// Bounded metric label.
	pub const fn label(self) -> &'static str {
		match self {
			Self::Retryable => "retryable",
			Self::Deterministic(origin) => origin.reason().label(),
		}
	}
}

/// Classify a compaction failure.
///
/// The two race variants are retryable by definition. The two invariance variants
/// are deterministic by definition - a digest that did not match will not match on
/// a retry of the same inputs. An `IoError` is deterministic only if its origin
/// said so.
pub(crate) fn classify_compaction_failure(failure: &CompactionFailure) -> MaintenanceFailureClass {
	match failure {
		CompactionFailure::ManifestRace
		| CompactionFailure::CounterMaintenanceSkipped
		| CompactionFailure::BackfillRaceLost => MaintenanceFailureClass::Retryable,
		CompactionFailure::SidecarDigestMismatch => {
			MaintenanceFailureClass::Deterministic(DeterministicOrigin::SidecarDigestMismatch)
		}
		CompactionFailure::VisibleDigestInvariance => {
			MaintenanceFailureClass::Deterministic(DeterministicOrigin::VisibleDomainInvariance)
		}
		CompactionFailure::IoError(error) => classify_io_error(error),
	}
}

/// Classify a bare `io::Error` by its payload, never by its kind.
pub(crate) fn classify_io_error(error: &Error) -> MaintenanceFailureClass {
	// Walk the source chain, not just the immediate payload: an intermediate layer
	// that wraps the error while PRESERVING it as a source must not silently
	// reclassify a deterministic failure as retryable. (A layer that re-wraps by
	// `to_string()` does drop the class - there is nothing left to find - which is
	// why origins pass the error itself. `restringifying_a_failure_loses_its_class`
	// pins that hazard.)
	// Bounded: a pathological `source()` cycle must not hang classification, which
	// runs on the failure path of every maintenance attempt.
	const MAX_CHAIN_LINKS: usize = 32;
	let mut next: Option<&(dyn std::error::Error + 'static)> = error.get_ref().map(|e| e as _);
	for _ in 0..MAX_CHAIN_LINKS {
		let Some(inner) = next else {
			break;
		};
		if let Some(failure) = inner.downcast_ref::<DeterministicFailure>() {
			return MaintenanceFailureClass::Deterministic(failure.origin());
		}
		// A nested `io::Error` breaks the ordinary chain: its `source()` delegates
		// to its PAYLOAD's source rather than exposing the payload, so walking
		// `source()` alone would step straight past a wrapped deterministic failure.
		next = match inner.downcast_ref::<Error>() {
			Some(nested) => nested.get_ref().map(|e| e as _),
			None => inner.source(),
		};
	}
	MaintenanceFailureClass::Retryable
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn races_are_retryable_and_invariance_failures_are_not() {
		assert_eq!(
			classify_compaction_failure(&CompactionFailure::ManifestRace),
			MaintenanceFailureClass::Retryable
		);
		assert_eq!(
			classify_compaction_failure(&CompactionFailure::BackfillRaceLost),
			MaintenanceFailureClass::Retryable
		);
		assert_eq!(
			classify_compaction_failure(&CompactionFailure::SidecarDigestMismatch),
			MaintenanceFailureClass::Deterministic(DeterministicOrigin::SidecarDigestMismatch)
		);
	}

	/// The refinement that matters: an `io::ErrorKind` carries no class of its own.
	/// The SAME kind is retryable or deterministic depending on where it came from,
	/// so a blanket rule over kinds would be wrong in both directions.
	#[test]
	fn error_kind_alone_never_decides_the_class() {
		for kind in [
			ErrorKind::NotFound,
			ErrorKind::InvalidInput,
			ErrorKind::InvalidData,
			ErrorKind::Unsupported,
		] {
			let untagged = CompactionFailure::IoError(Error::new(kind, "plain"));
			assert_eq!(
				classify_compaction_failure(&untagged),
				MaintenanceFailureClass::Retryable,
				"an untagged {kind:?} defaults to retryable"
			);

			let tagged = CompactionFailure::IoError(deterministic_failure(
				kind,
				DeterministicOrigin::LsmInconsistentSourceFrames,
				"tagged at its origin",
			));
			assert_eq!(
				classify_compaction_failure(&tagged),
				MaintenanceFailureClass::Deterministic(
					DeterministicOrigin::LsmInconsistentSourceFrames
				),
				"the same {kind:?} is deterministic when its origin says so"
			);
		}
	}

	#[test]
	fn a_tagged_failure_keeps_its_kind_and_message_for_existing_callers() {
		let error = deterministic_failure(
			ErrorKind::Unsupported,
			DeterministicOrigin::LsmWriterCannotSplitMultiRowFrame,
			"writer does not yet split multi-row frames",
		);
		assert_eq!(error.kind(), ErrorKind::Unsupported);
		assert!(
			error
				.to_string()
				.contains("does not yet split multi-row frames")
		);
	}

	#[test]
	fn deterministic_schedule_is_three_attempts_then_quarantine() {
		// t0 attempt fails once -> wait 5s; fails again -> wait 30s; the third
		// failure exhausts the budget and quarantines.
		assert_eq!(deterministic_backoff(1), Some(Duration::from_secs(5)));
		assert_eq!(deterministic_backoff(2), Some(Duration::from_secs(30)));
		assert_eq!(
			deterministic_backoff(DETERMINISTIC_ATTEMPTS_BEFORE_QUARANTINE),
			None,
			"the third failure quarantines rather than scheduling a fourth build"
		);
		assert_eq!(deterministic_backoff(9), None);
	}

	#[test]
	fn retryable_cooldown_doubles_from_250ms_and_caps_at_five_seconds() {
		assert_eq!(retryable_cooldown(1), Duration::from_millis(250));
		assert_eq!(retryable_cooldown(2), Duration::from_millis(500));
		assert_eq!(retryable_cooldown(3), Duration::from_secs(1));
		assert_eq!(retryable_cooldown(4), Duration::from_secs(2));
		assert_eq!(retryable_cooldown(5), Duration::from_secs(4));
		// Capped, and stays capped however long the race runs.
		assert_eq!(retryable_cooldown(6), RETRYABLE_COOLDOWN_CAP);
		assert_eq!(retryable_cooldown(1_000), RETRYABLE_COOLDOWN_CAP);
	}

	/// The key must separate work by ORIGIN, not by metric category: nine sites
	/// report as `CorruptSourceFrame`, and pooling them would let one site spend
	/// another's attempt budget - or quarantine a slab that had failed once for each
	/// of three unrelated reasons as though one failure had recurred three times.
	#[test]
	fn the_work_key_separates_sites_that_share_a_metric_category() {
		let slabs = [SlabId::new(7), SlabId::new(3)];
		let duplicate_identity = MaintenanceWorkKey::new(
			OP,
			slabs,
			DeterministicOrigin::LsmRewriteDuplicateRowIdentity,
		)
		.unwrap();
		let inconsistent_frames =
			MaintenanceWorkKey::new(OP, slabs, DeterministicOrigin::LsmInconsistentSourceFrames)
				.unwrap();
		assert_eq!(
			duplicate_identity.reason(),
			inconsistent_frames.reason(),
			"the fixture must use two sites that SHARE a metric category"
		);
		assert_ne!(
			duplicate_identity, inconsistent_frames,
			"but they are different work with independent budgets"
		);

		// Canonical: caller order and duplicates do not create a second identity.
		assert_eq!(
			duplicate_identity,
			MaintenanceWorkKey::new(
				OP,
				[SlabId::new(3), SlabId::new(7), SlabId::new(3)],
				DeterministicOrigin::LsmRewriteDuplicateRowIdentity
			)
			.unwrap()
		);
		assert_eq!(
			duplicate_identity.slabs(),
			&[SlabId::new(3), SlabId::new(7)]
		);
	}

	/// Fail closed. A malformed receipt must never read as "no history", which would
	/// reset the attempt bound on the restart where it matters most.
	#[test]
	fn malformed_receipts_are_refused_rather_than_read_as_absent() {
		let key = MaintenanceWorkKey::new(
			OP,
			[SlabId::new(9), SlabId::new(1), SlabId::new(5)],
			DeterministicOrigin::MaintenanceRewriteWithoutClaim,
		)
		.unwrap();
		assert_eq!(MaintenanceWorkKey::decode(&key.encode()), Ok(key.clone()));

		assert_eq!(
			MaintenanceWorkKey::decode(&[]),
			Err(ReceiptDecodeError::Empty)
		);
		assert_eq!(
			MaintenanceWorkKey::decode(&[0, 1, 0, 11]),
			Err(ReceiptDecodeError::NoSlabs),
			"an origin with no slabs names no work"
		);
		assert_eq!(
			MaintenanceWorkKey::decode(&[0, 1, 255, 255, 0, 0, 0, 0, 0, 0, 0, 1]),
			Err(ReceiptDecodeError::UnknownOrigin(65535)),
			"a receipt from a future binary is refused, not silently dropped"
		);
		assert_eq!(
			MaintenanceWorkKey::decode(&[255, 255, 0, 11, 0, 0, 0, 0, 0, 0, 0, 1]),
			Err(ReceiptDecodeError::UnknownOperation(65535)),
			"so is one from a job this binary does not know"
		);
		assert_eq!(
			MaintenanceWorkKey::decode(&[0, 1, 0]),
			Err(ReceiptDecodeError::Empty),
			"shorter than the operation-and-origin header"
		);
		assert_eq!(
			MaintenanceWorkKey::decode(&[0, 1, 0, 11, 0, 0, 1]),
			Err(ReceiptDecodeError::TruncatedSlabId)
		);
		let mut unsorted = Vec::from([0u8, 1, 0, 11]);
		unsorted.extend_from_slice(&7u64.to_be_bytes());
		unsorted.extend_from_slice(&3u64.to_be_bytes());
		assert_eq!(
			MaintenanceWorkKey::decode(&unsorted),
			Err(ReceiptDecodeError::UnsortedSlabs),
			"a non-canonical key would be a second identity for the same work"
		);
	}

	#[test]
	fn origin_codes_round_trip_and_are_stable() {
		let all = [
			DeterministicOrigin::LsmWriterCannotSplitMultiRowFrame,
			DeterministicOrigin::LsmRewriteProducedNoSurvivors,
			DeterministicOrigin::LsmRewriteFrameNotMultiRow,
			DeterministicOrigin::LsmRewriteDuplicateRowIdentity,
			DeterministicOrigin::LsmRewriteKeptWrongRowCount,
			DeterministicOrigin::LsmSourceFrameMissingPlannedSurvivor,
			DeterministicOrigin::LsmInconsistentSourceFrames,
			DeterministicOrigin::LsmRewrittenFrameCarriesUnplannedRow,
			DeterministicOrigin::LsmMultiRowSourceFrameHasNoEntries,
			DeterministicOrigin::LsmOutputLocationMissingPlannedRow,
			DeterministicOrigin::MaintenanceRewriteWithoutClaim,
			DeterministicOrigin::SidecarDigestMismatch,
			DeterministicOrigin::VisibleDomainInvariance,
		];
		let mut codes = all.iter().map(|origin| origin.code()).collect::<Vec<_>>();
		codes.sort_unstable();
		codes.dedup();
		assert_eq!(codes.len(), all.len(), "every origin has a distinct code");
		for origin in all {
			assert_eq!(DeterministicOrigin::from_code(origin.code()), Some(origin));
		}
		assert_eq!(DeterministicOrigin::from_code(0), None);
	}

	/// Documents the transport hazard the origins must avoid: re-wrapping an error
	/// by its STRING drops the payload and silently reclassifies the failure as
	/// retryable. Any site that does this reintroduces the unbounded loop.
	/// The enumeration test above checks that codes are distinct and round-trip; it
	/// cannot see whether a variant is ever CONSTRUCTED. Two origins were declared
	/// and never wired, and a third was used twice - a bulk edit had matched the
	/// nearest message rather than the right one. This reads the production sources
	/// and requires every origin to be tagged exactly once.
	#[test]
	fn a_failure_schedules_from_when_it_failed_not_when_its_tick_began() {
		let first = DeterministicReceipt::after_failure(None, 1_000_000, "shape").unwrap();
		assert_eq!(first.attempts(), 1);
		assert_eq!(
			first.next_eligible_at_ms(),
			Some(1_005_000),
			"+5s from the FAILURE, not from the tick that began long before"
		);
		assert!(!first.is_eligible_at(1_004_999));
		assert!(first.is_eligible_at(1_005_000));

		let second = DeterministicReceipt::after_failure(Some(&first), 1_005_100, "shape").unwrap();
		assert_eq!(second.attempts(), 2);
		assert_eq!(second.next_eligible_at_ms(), Some(1_035_100), "+30s");

		let third = DeterministicReceipt::after_failure(Some(&second), 1_035_200, "shape").unwrap();
		assert_eq!(third.attempts(), 3);
		assert_eq!(third.next_eligible_at_ms(), None, "the budget is spent");
		assert!(third.is_quarantined());
		assert!(
			!third.is_eligible_at(u64::MAX),
			"no clock value makes a quarantined receipt eligible"
		);

		// Quarantine is absorbing: only the explicit requeue leaves it.
		assert_eq!(
			DeterministicReceipt::after_failure(Some(&third), 1_040_000, "shape"),
			Err(ReceiptDecodeError::QuarantinedReceiptAdvanced)
		);
	}

	/// A backward clock step must never SHORTEN a wait. With absolute deadlines it
	/// cannot: a smaller `now` simply fails the comparison.
	#[test]
	fn a_clock_rollback_extends_the_wait_rather_than_shortening_it() {
		let receipt = DeterministicReceipt::after_failure(None, 1_000_000, "shape").unwrap();
		assert!(receipt.is_eligible_at(1_005_000));
		assert!(
			!receipt.is_eligible_at(900_000),
			"a rolled-back clock is further from the deadline, not past it"
		);
	}

	/// Time arithmetic fails CLOSED. Wrapping or saturating would produce a
	/// deadline that looks like a valid schedule and is not one.
	#[test]
	fn deadline_arithmetic_that_would_overflow_is_refused() {
		assert_eq!(
			DeterministicReceipt::after_failure(None, u64::MAX - 1, "shape"),
			Err(ReceiptDecodeError::TimeOverflow),
			"a near-max failure time cannot silently saturate into an immediate deadline"
		);
	}

	#[test]
	fn receipts_round_trip_and_reject_malformed_values() {
		let first = DeterministicReceipt::after_failure(None, 42, "unsupported shape").unwrap();
		let second = DeterministicReceipt::after_failure(Some(&first), 60, "again").unwrap();
		let third = DeterministicReceipt::after_failure(Some(&second), 90, "and again").unwrap();
		for receipt in [&first, &second, &third] {
			assert_eq!(
				DeterministicReceipt::decode(&receipt.encode()).as_ref(),
				Ok(receipt)
			);
		}
		assert_eq!(first.detail(), "unsupported shape");

		assert_eq!(
			DeterministicReceipt::decode(&[]),
			Err(ReceiptDecodeError::TruncatedReceipt)
		);
		let mut wrong_version = first.encode();
		wrong_version[0] = 9;
		assert_eq!(
			DeterministicReceipt::decode(&wrong_version),
			Err(ReceiptDecodeError::UnknownReceiptVersion(9)),
			"a future value layout is refused, not reinterpreted"
		);
	}

	/// The validation that matters: a structurally intact receipt whose deadline
	/// does not match its attempt count would permit a FOURTH working-set build.
	#[test]
	fn a_deadline_that_contradicts_its_attempt_count_is_refused() {
		let spent = DeterministicReceipt::after_failure(
			Some(
				&DeterministicReceipt::after_failure(
					Some(&DeterministicReceipt::after_failure(None, 1_000, "a").unwrap()),
					2_000,
					"b",
				)
				.unwrap(),
			),
			3_000,
			"c",
		)
		.unwrap();
		assert!(spent.is_quarantined());

		// Forge attempt 3 carrying a deadline: structurally fine, semantically a
		// fourth attempt.
		let mut forged = spent.encode();
		forged[13] = 1;
		forged.splice(14..14, 9_999u64.to_be_bytes());
		assert_eq!(
			DeterministicReceipt::decode(&forged),
			Err(ReceiptDecodeError::DeadlineDoesNotMatchSchedule),
			"a quarantined attempt count may not carry a deadline"
		);

		// And a deadline that is merely LATER than the failure, but not the one the
		// schedule produces, is equally refused.
		let one = DeterministicReceipt::after_failure(None, 1_000, "a").unwrap();
		let mut drifted = one.encode();
		let at = u64::from_be_bytes(drifted[14..22].try_into().unwrap());
		drifted[14..22].copy_from_slice(&(at + 1).to_be_bytes());
		assert_eq!(
			DeterministicReceipt::decode(&drifted),
			Err(ReceiptDecodeError::DeadlineDoesNotMatchSchedule),
			"validated EXACTLY against the schedule, not merely for being later"
		);
	}

	/// ADR 0097 decision 4: v1 receipts pinned to EXPLICIT BYTES.
	///
	/// Every other receipt test runs the schedule function on both sides, so a
	/// change to `DETERMINISTIC_BACKOFF` or to the value layout would keep them all
	/// green while silently redefining what v1 means on disk. This fixture is
	/// independent of both: it is what an already-deployed node has written.
	///
	/// If this test fails, the change is a NEW receipt version with a migration -
	/// not an edit to these bytes.
	#[test]
	fn v1_receipt_bytes_are_pinned() {
		// attempt 1, failed at 1_000_000 ms, eligible again at +5s, detail "shape".
		let scheduled: Vec<u8> = vec![
			0x01, // version
			0x00, 0x00, 0x00, 0x01, // attempts = 1
			0x00, 0x00, 0x00, 0x00, 0x00, 0x0F, 0x42, 0x40, // last_failure_ms = 1_000_000
			0x01, // a deadline is present
			0x00, 0x00, 0x00, 0x00, 0x00, 0x0F, 0x55, 0xC8, // next_eligible = 1_005_000
			0x00, 0x05, // detail length
			b's', b'h', b'a', b'p', b'e',
		];
		let receipt = DeterministicReceipt::after_failure(None, 1_000_000, "shape").unwrap();
		assert_eq!(
			receipt.encode(),
			scheduled,
			"v1 encoding changed; bump RECEIPT_VALUE_VERSION instead of editing this"
		);
		let decoded = DeterministicReceipt::decode(&scheduled).expect("v1 bytes decode");
		assert_eq!(decoded.attempts(), 1);
		assert_eq!(decoded.last_failure_ms(), 1_000_000);
		assert_eq!(decoded.next_eligible_at_ms(), Some(1_005_000));
		assert_eq!(decoded.detail(), "shape");

		// A quarantined receipt: attempt 3, no deadline.
		let quarantined: Vec<u8> = vec![
			0x01, // version
			0x00, 0x00, 0x00, 0x03, // attempts = 3
			0x00, 0x00, 0x00, 0x00, 0x00, 0x1E, 0x84, 0x80, // last_failure_ms = 2_000_000
			0x00, // no deadline: quarantined
			0x00, 0x01, // detail length
			b'x',
		];
		let first = DeterministicReceipt::after_failure(None, 1_000, "x").unwrap();
		let second = DeterministicReceipt::after_failure(Some(&first), 2_000, "x").unwrap();
		let third = DeterministicReceipt::after_failure(Some(&second), 2_000_000, "x").unwrap();
		assert_eq!(
			third.encode(),
			quarantined,
			"v1 quarantine encoding changed; bump the version instead"
		);
		let decoded = DeterministicReceipt::decode(&quarantined).expect("v1 bytes decode");
		assert!(decoded.is_quarantined());
		assert_eq!(decoded.attempts(), 3);
		assert_eq!(decoded.last_failure_ms(), 2_000_000);
	}

	/// The fixture above is only meaningful if the decoder rejects trailing bytes:
	/// otherwise a longer future layout would still decode as v1.
	#[test]
	fn a_v1_receipt_with_trailing_bytes_is_refused() {
		let mut trailing = DeterministicReceipt::after_failure(None, 1_000, "x")
			.unwrap()
			.encode();
		trailing.push(0);
		assert_eq!(
			DeterministicReceipt::decode(&trailing),
			Err(ReceiptDecodeError::TruncatedReceipt),
			"length is exact; a longer value is a different layout"
		);
	}

	#[test]
	fn receipt_detail_is_bounded_and_stays_valid_utf8() {
		let long = "é".repeat(MAX_RECEIPT_DETAIL_BYTES);
		let receipt = DeterministicReceipt::after_failure(None, 1_000, &long).unwrap();
		assert!(receipt.detail().len() <= MAX_RECEIPT_DETAIL_BYTES);
		assert!(
			long.starts_with(receipt.detail()),
			"truncation keeps a prefix, on a char boundary"
		);
		assert_eq!(
			DeterministicReceipt::decode(&receipt.encode())
				.unwrap()
				.detail(),
			receipt.detail()
		);
	}

	#[test]
	fn the_manual_clock_moves_only_when_told() {
		let clock = ManualClock::at(1_000);
		assert_eq!(clock.now_ms(), Some(1_000));
		clock.advance(Duration::from_secs(5));
		assert_eq!(clock.now_ms(), Some(6_000));
		clock.set(1);
		assert_eq!(clock.now_ms(), Some(1), "tests can slam it backward too");
	}

	/// A policy over a REAL store: `Enabled` deliberately cannot exist without one,
	/// so these tests open a temporary index rather than inventing a storeless
	/// variant that production could never reach.
	async fn enabled_policy(clock: Arc<ManualClock>) -> Arc<MaintenancePolicyInner> {
		let path = std::env::temp_dir().join(format!(
			"wendelon-policy-{}-{}.redb",
			std::process::id(),
			uuid::Uuid::new_v4()
		));
		let store = Arc::new(
			crate::storage::index::PersistentLocalIndex::open(path)
				.await
				.expect("temp index opens"),
		);
		MaintenancePolicyInner::enabled(store, BTreeMap::new(), clock)
	}

	#[tokio::test]
	async fn a_reservation_bars_a_second_identical_plan() {
		let clock = Arc::new(ManualClock::at(1_000));
		let policy = enabled_policy(clock).await;
		let slabs = [SlabId::new(3), SlabId::new(1)];

		let first = policy.reserve(OP, &slabs).expect("first admission");
		assert_eq!(
			policy.reserve(OP, &slabs).err(),
			Some(AdmissionRefusal::AlreadyInFlight),
			"a second identical plan cannot enter while the first is in flight"
		);
		// Canonical: a differently-ordered set is the SAME work.
		assert_eq!(
			policy.reserve(OP, &[SlabId::new(1), SlabId::new(3)]).err(),
			Some(AdmissionRefusal::AlreadyInFlight)
		);
		// Disjoint work is unaffected. Finalized, because dropping it unfinalized
		// would disable the policy - which is the invariant, not an inconvenience.
		policy
			.reserve(OP, &[SlabId::new(9)])
			.expect("disjoint work is unaffected")
			.finalize();

		first.finalize();
		policy
			.reserve(OP, &slabs)
			.expect("finalizing releases the reservation")
			.finalize();
		assert!(
			policy.is_enabled(),
			"the test must not end by silently exercising the abandoned-attempt path"
		);
	}

	/// THE fail-closed invariant. An attempt that ends without its outcome reaching
	/// disk cost a full working-set build that nothing counted; readmitting the
	/// same work would restart the loop with the bound none the wiser.
	#[tokio::test]
	async fn dropping_an_unfinalized_reservation_disables_maintenance() {
		let clock = Arc::new(ManualClock::at(1_000));
		let policy = enabled_policy(clock).await;
		let slabs = [SlabId::new(5)];

		let reservation = policy.reserve(OP, &slabs).expect("admission");
		assert!(policy.is_enabled());
		drop(reservation);

		assert!(
			!policy.is_enabled(),
			"an abandoned attempt disables admission rather than quietly releasing"
		);
		assert_eq!(
			policy.diagnostic().as_deref(),
			Some("attempt abandoned before durable outcome")
		);
		assert!(matches!(
			policy.reserve(OP, &slabs),
			Err(AdmissionRefusal::PolicyDisabled(_))
		));
	}

	#[tokio::test]
	async fn finalizing_does_not_disable_and_a_disabled_policy_admits_nothing() {
		let clock = Arc::new(ManualClock::at(1_000));
		let policy = enabled_policy(clock).await;
		policy.reserve(OP, &[SlabId::new(1)]).unwrap().finalize();
		assert!(
			policy.is_enabled(),
			"a finalized attempt leaves policy enabled"
		);

		policy.disable("test");
		assert!(matches!(
			policy.reserve(OP, &[SlabId::new(2)]),
			Err(AdmissionRefusal::PolicyDisabled(_))
		));
		assert!(
			!policy.is_eligible(OP, &[SlabId::new(2)], 1_000),
			"a disabled policy makes nothing eligible"
		);
	}

	#[tokio::test]
	async fn eligibility_does_not_reserve_so_the_picker_can_inspect_candidates() {
		let clock = Arc::new(ManualClock::at(1_000));
		let policy = enabled_policy(clock).await;
		let slabs = [SlabId::new(4)];
		assert!(policy.is_eligible(OP, &slabs, 1_000));
		assert!(
			policy.is_eligible(OP, &slabs, 1_000),
			"asking twice does not consume anything"
		);
		policy
			.reserve(OP, &slabs)
			.expect("the predicate left the work admissible")
			.finalize();
		assert!(
			policy.is_enabled(),
			"the test must not end by silently exercising the abandoned-attempt path"
		);
	}

	/// The aggregation bug this pins: `reserve` once consulted only the FIRST
	/// receipt for a slab set while `is_eligible` consulted all of them, so an
	/// eligible origin could hide a quarantined one and admit work the picker had
	/// already ruled out.
	#[tokio::test]
	async fn a_quarantined_origin_is_not_hidden_by_an_eligible_sibling() {
		let clock = Arc::new(ManualClock::at(10_000_000));
		let slabs = [SlabId::new(2), SlabId::new(7)];

		// Two origins over ONE slab set: one genuinely ELIGIBLE, one quarantined.
		// The eligible one failed long enough ago that its whole backoff has elapsed
		// - a merely-waiting sibling would refuse under the old first-match bug too,
		// and prove nothing.
		let eligible = DeterministicReceipt::after_failure(None, 9_000_000, "eligible").unwrap();
		assert!(
			eligible.is_eligible_at(10_000_000),
			"the premise: this sibling ALONE would admit"
		);
		let spent = {
			let a = DeterministicReceipt::after_failure(None, 1_000, "a").unwrap();
			let b = DeterministicReceipt::after_failure(Some(&a), 2_000, "b").unwrap();
			DeterministicReceipt::after_failure(Some(&b), 3_000, "c").unwrap()
		};
		assert!(spent.is_quarantined());

		let mut receipts = BTreeMap::new();
		// Ordered so the ELIGIBLE-looking one is encountered first.
		receipts.insert(
			MaintenanceWorkKey::new(
				OP,
				slabs,
				DeterministicOrigin::LsmRewriteDuplicateRowIdentity,
			)
			.unwrap(),
			eligible,
		);
		receipts.insert(
			MaintenanceWorkKey::new(OP, slabs, DeterministicOrigin::LsmInconsistentSourceFrames)
				.unwrap(),
			spent,
		);

		let path = std::env::temp_dir().join(format!(
			"wendelon-policy-agg-{}-{}.redb",
			std::process::id(),
			uuid::Uuid::new_v4()
		));
		let store = Arc::new(
			crate::storage::index::PersistentLocalIndex::open(path)
				.await
				.unwrap(),
		);
		let policy = MaintenancePolicyInner::enabled(store, receipts, clock);

		assert!(
			!policy.is_eligible(OP, &slabs, 10_000_000),
			"the predicate sees the quarantined origin"
		);
		assert_eq!(
			policy.reserve(OP, &slabs).err(),
			Some(AdmissionRefusal::Quarantined),
			"and so does admission: quarantine wins over an eligible sibling"
		);
	}

	#[tokio::test]
	async fn a_plan_naming_no_slabs_is_refused() {
		let policy = enabled_policy(Arc::new(ManualClock::at(1_000))).await;
		assert_eq!(
			policy.reserve(OP, &[]).err(),
			Some(AdmissionRefusal::EmptyWorkSet)
		);
		assert!(policy.is_enabled(), "a refusal is not a failure");
	}

	fn temp_index_path(tag: &str) -> std::path::PathBuf {
		std::env::temp_dir().join(format!(
			"wendelon-policy-{tag}-{}-{}.redb",
			std::process::id(),
			uuid::Uuid::new_v4()
		))
	}

	/// Recovery publishes the record whole: a quarantine written before restart is
	/// effective the instant the policy is `Enabled`, and slabs with no receipt
	/// are admissible.
	#[tokio::test]
	async fn recovery_republishes_durable_receipts_as_one_record() {
		let path = temp_index_path("recover");
		let quarantined = [SlabId::new(4), SlabId::new(9)];
		let key = MaintenanceWorkKey::new(
			OP,
			quarantined,
			DeterministicOrigin::LsmInconsistentSourceFrames,
		)
		.unwrap();
		let spent = {
			let a = DeterministicReceipt::after_failure(None, 1_000, "a").unwrap();
			let b = DeterministicReceipt::after_failure(Some(&a), 2_000, "b").unwrap();
			DeterministicReceipt::after_failure(Some(&b), 3_000, "c").unwrap()
		};
		{
			let store = crate::storage::index::PersistentLocalIndex::open(path.clone())
				.await
				.unwrap();
			store
				.put_maintenance_receipt(key.encode(), spent.encode())
				.await
				.unwrap();
		}

		let store = Arc::new(
			crate::storage::index::PersistentLocalIndex::open(path)
				.await
				.unwrap(),
		);
		let policy = MaintenancePolicyInner::load(store, Arc::new(ManualClock::at(u64::MAX))).await;
		assert!(policy.is_enabled(), "{:?}", policy.diagnostic());
		assert!(
			!policy.is_eligible(OP, &quarantined, u64::MAX),
			"the recovered quarantine is effective, at any clock value"
		);
		assert!(
			policy.is_eligible(OP, &[SlabId::new(5)], u64::MAX),
			"work with no receipt is admissible"
		);
	}

	/// The hydration invariant: one undecodable pair among good ones yields NO
	/// policy, not a policy over the good ones. A partial history admits work whose
	/// receipt simply was not loaded, which is the bound failing silently.
	#[tokio::test]
	async fn one_undecodable_receipt_disables_recovery_entirely() {
		let path = temp_index_path("corrupt");
		let good_slabs = [SlabId::new(1)];
		let good_key = MaintenanceWorkKey::new(
			OP,
			good_slabs,
			DeterministicOrigin::LsmInconsistentSourceFrames,
		)
		.unwrap();
		let good = DeterministicReceipt::after_failure(None, 1_000, "good").unwrap();
		// Sorts AFTER the good key, so the good pair is decoded first: the test
		// fails if recovery had published it before reaching the bad one.
		let bad_key = MaintenanceWorkKey::new(
			OP,
			[SlabId::new(2)],
			DeterministicOrigin::LsmInconsistentSourceFrames,
		)
		.unwrap();
		assert!(good_key.encode() < bad_key.encode());
		{
			let store = crate::storage::index::PersistentLocalIndex::open(path.clone())
				.await
				.unwrap();
			store
				.put_maintenance_receipt(good_key.encode(), good.encode())
				.await
				.unwrap();
			store
				.put_maintenance_receipt(bad_key.encode(), vec![0xFF])
				.await
				.unwrap();
		}

		let store = Arc::new(
			crate::storage::index::PersistentLocalIndex::open(path)
				.await
				.unwrap(),
		);
		let policy = MaintenancePolicyInner::load(store, Arc::new(ManualClock::at(1))).await;
		assert!(!policy.is_enabled());
		let diagnostic = policy.diagnostic().unwrap();
		assert!(
			diagnostic.contains("undecodable"),
			"the diagnostic names the cause: {diagnostic}"
		);
		assert!(
			!policy.is_eligible(OP, &good_slabs, u64::MAX)
				&& !policy.is_eligible(OP, &[SlabId::new(3)], u64::MAX),
			"nothing is admissible from a policy that never finished loading"
		);
		assert_eq!(
			policy.reserve(OP, &[SlabId::new(3)]).err(),
			Some(AdmissionRefusal::PolicyDisabled(diagnostic))
		);
	}

	/// The load bounds are exact for the schema, so a stale constant would either
	/// refuse a valid receipt or admit an oversized one. Pin them to the encoders.
	#[test]
	fn load_bounds_track_the_encoders_exactly() {
		let widest_detail = "x".repeat(MAX_RECEIPT_DETAIL_BYTES);
		let widest = DeterministicReceipt::after_failure(None, 1, &widest_detail).unwrap();
		assert_eq!(widest.detail().len(), MAX_RECEIPT_DETAIL_BYTES);
		assert_eq!(widest.encode().len(), MAX_RECEIPT_VALUE_BYTES);
		assert!(
			DeterministicReceipt::after_failure(None, 1, &format!("{widest_detail}y"))
				.unwrap()
				.encode()
				.len() <= MAX_RECEIPT_VALUE_BYTES,
			"detail truncation keeps every value under the bound"
		);

		let widest_key = MaintenanceWorkKey::new(
			OP,
			(1..=MAX_RECEIPT_KEY_SLABS as u64).map(SlabId::new),
			DeterministicOrigin::LsmInconsistentSourceFrames,
		)
		.unwrap();
		assert_eq!(widest_key.encode().len(), MAX_RECEIPT_KEY_BYTES);
		assert_eq!(
			MaintenanceWorkKey::new(
				OP,
				(1..=MAX_RECEIPT_KEY_SLABS as u64 + 1).map(SlabId::new),
				DeterministicOrigin::LsmInconsistentSourceFrames,
			)
			.err(),
			Some(ReceiptDecodeError::TooManySlabs(MAX_RECEIPT_KEY_SLABS + 1)),
			"the executor fails closed at the write, not the next startup at the read"
		);
		let mut too_wide = widest_key.encode();
		too_wide.extend_from_slice(&u64::MAX.to_be_bytes());
		assert_eq!(
			MaintenanceWorkKey::decode(&too_wide).err(),
			Some(ReceiptDecodeError::TooManySlabs(MAX_RECEIPT_KEY_SLABS + 1))
		);
	}

	/// An oversized row fails closed BEFORE it is copied, and the diagnostic that
	/// results is bounded no matter how large the offending row was.
	#[tokio::test]
	async fn an_oversized_receipt_disables_recovery_with_a_bounded_diagnostic() {
		let path = temp_index_path("oversized");
		let key = MaintenanceWorkKey::new(
			OP,
			[SlabId::new(1)],
			DeterministicOrigin::LsmInconsistentSourceFrames,
		)
		.unwrap();
		{
			let store = crate::storage::index::PersistentLocalIndex::open(path.clone())
				.await
				.unwrap();
			// Raw put bypasses the schema: one byte over the exact v1 maximum.
			store
				.put_maintenance_receipt(
					key.encode(),
					vec![RECEIPT_VALUE_VERSION; MAX_RECEIPT_VALUE_BYTES + 1],
				)
				.await
				.unwrap();
		}
		let store = Arc::new(
			crate::storage::index::PersistentLocalIndex::open(path)
				.await
				.unwrap(),
		);
		let policy = MaintenancePolicyInner::load(store, Arc::new(ManualClock::at(1))).await;
		assert!(!policy.is_enabled());
		let diagnostic = policy.diagnostic().unwrap();
		assert!(diagnostic.contains("bound exceeded"), "{diagnostic}");
		assert!(diagnostic.len() <= MAX_DIAGNOSTIC_BYTES, "{diagnostic}");
	}

	/// Diagnostics describe corrupt receipts by summary, never by content: a wide
	/// key names its slab count and a few ids, a raw key its length and a prefix.
	#[tokio::test]
	async fn corrupt_receipt_diagnostics_are_summaries_not_dumps() {
		// A decodable key over many slabs, with an undecodable value.
		let path = temp_index_path("wide-key");
		let wide = MaintenanceWorkKey::new(
			OP,
			(1..=500u64).map(SlabId::new),
			DeterministicOrigin::LsmInconsistentSourceFrames,
		)
		.unwrap();
		{
			let store = crate::storage::index::PersistentLocalIndex::open(path.clone())
				.await
				.unwrap();
			store
				.put_maintenance_receipt(wide.encode(), vec![0xFF])
				.await
				.unwrap();
		}
		let store = Arc::new(
			crate::storage::index::PersistentLocalIndex::open(path)
				.await
				.unwrap(),
		);
		let policy = MaintenancePolicyInner::load(store, Arc::new(ManualClock::at(1))).await;
		let diagnostic = policy.diagnostic().unwrap();
		assert!(
			diagnostic.contains("500 slabs, first [1, 2, 3, 4, ..]"),
			"{diagnostic}"
		);
		assert!(diagnostic.contains("undecodable"), "{diagnostic}");
		assert!(diagnostic.len() <= MAX_DIAGNOSTIC_BYTES, "{diagnostic}");

		// An undecodable raw key, large but under the load bound.
		let path = temp_index_path("raw-key");
		let raw_key = vec![0xAB; 10_000];
		assert!(raw_key.len() <= MAX_RECEIPT_KEY_BYTES);
		{
			let store = crate::storage::index::PersistentLocalIndex::open(path.clone())
				.await
				.unwrap();
			store
				.put_maintenance_receipt(raw_key, vec![0xFF])
				.await
				.unwrap();
		}
		let store = Arc::new(
			crate::storage::index::PersistentLocalIndex::open(path)
				.await
				.unwrap(),
		);
		let policy = MaintenancePolicyInner::load(store, Arc::new(ManualClock::at(1))).await;
		let diagnostic = policy.diagnostic().unwrap();
		assert!(
			diagnostic.contains("10000 bytes, prefix abababab"),
			"{diagnostic}"
		);
		assert!(diagnostic.len() <= MAX_DIAGNOSTIC_BYTES, "{diagnostic}");

		// And the cap itself holds on a char boundary for an arbitrary diagnostic.
		let long = "é".repeat(MAX_DIAGNOSTIC_BYTES);
		let capped = truncate_utf8(&long, MAX_DIAGNOSTIC_BYTES);
		assert!(capped.len() <= MAX_DIAGNOSTIC_BYTES && capped.len() >= MAX_DIAGNOSTIC_BYTES - 1);
		assert!(capped.chars().all(|c| c == 'é'));
	}

	/// Lost history (marker present, table gone) is not empty history.
	#[tokio::test]
	async fn lost_receipt_history_disables_recovery() {
		let path = temp_index_path("lost");
		{
			let store = crate::storage::index::PersistentLocalIndex::open(path.clone())
				.await
				.unwrap();
			assert!(store.receipts_available());
		}
		crate::storage::index::delete_maintenance_receipt_table_for_test(&path);
		let store = Arc::new(
			crate::storage::index::PersistentLocalIndex::open(path)
				.await
				.unwrap(),
		);
		assert!(!store.receipts_available());
		let policy = MaintenancePolicyInner::load(store, Arc::new(ManualClock::at(1))).await;
		assert!(!policy.is_enabled());
		assert!(
			policy.diagnostic().unwrap().contains("lost"),
			"{:?}",
			policy.diagnostic()
		);
	}

	// ------------------------------------------------------------------
	// The executor: reserve, run, record durably, release.
	// ------------------------------------------------------------------

	async fn enabled_policy_with_store(
		clock: Arc<dyn MaintenanceClock>,
	) -> (
		Arc<MaintenancePolicyInner>,
		Arc<crate::storage::index::PersistentLocalIndex>,
	) {
		let store = Arc::new(
			crate::storage::index::PersistentLocalIndex::open(temp_index_path("exec"))
				.await
				.expect("temp index opens"),
		);
		let policy = MaintenancePolicyInner::load(Arc::clone(&store), clock).await;
		assert!(policy.is_enabled(), "{:?}", policy.diagnostic());
		(policy, store)
	}

	/// Like `enabled_policy_with_store`, at a path the test keeps so it can reopen.
	async fn enabled_policy_at(
		path: &std::path::Path,
		clock: Arc<dyn MaintenanceClock>,
	) -> (
		Arc<MaintenancePolicyInner>,
		Arc<crate::storage::index::PersistentLocalIndex>,
	) {
		let store = Arc::new(
			crate::storage::index::PersistentLocalIndex::open(path.to_path_buf())
				.await
				.expect("temp index opens"),
		);
		let policy = MaintenancePolicyInner::load(Arc::clone(&store), clock).await;
		assert!(policy.is_enabled(), "{:?}", policy.diagnostic());
		(policy, store)
	}

	/// A REAL reopen of the index, not a rehydration of the policy from a live
	/// handle: every handle is gone before the file is opened again, so what the new
	/// one reads is what reached the database. Restart survival is the property
	/// decision 4 cares about, and only this exercises it.
	async fn reopen_store(
		store: Arc<crate::storage::index::PersistentLocalIndex>,
		path: &std::path::Path,
	) -> Arc<crate::storage::index::PersistentLocalIndex> {
		let sole = Arc::try_unwrap(store)
			.unwrap_or_else(|_| panic!("a policy still holds the index; drop it before reopening"));
		drop(sole);
		Arc::new(
			crate::storage::index::PersistentLocalIndex::open(path.to_path_buf())
				.await
				.expect("index reopens"),
		)
	}

	const ORIGIN: DeterministicOrigin = DeterministicOrigin::LsmInconsistentSourceFrames;
	const OP: MaintenanceOperation = MaintenanceOperation::LsmCompaction;

	/// Every completion in these tests is a rewrite unless the test says otherwise.
	fn rewrote<T>(_: &T) -> SuccessDisposition {
		SuccessDisposition::Rewrote
	}

	fn det_err(detail: &str) -> Error {
		deterministic_failure(ErrorKind::Other, ORIGIN, detail)
	}

	fn race_err() -> Error {
		Error::other("lost a race")
	}

	async fn fail_deterministically(policy: &Arc<MaintenancePolicyInner>, slabs: &[SlabId]) {
		let attempt = policy
			.execute(OP, slabs, classify_io_error, rewrote, || async {
				Err::<(), _>(det_err("shape"))
			})
			.await;
		assert!(
			matches!(attempt, MaintenanceAttempt::Failed(_)),
			"{attempt:?}"
		);
	}

	fn receipt_for(
		policy: &MaintenancePolicyInner,
		slabs: &[SlabId],
	) -> Option<MaintenanceReceiptView> {
		policy
			.report()
			.receipts
			.into_iter()
			.find(|receipt| receipt.slabs == slabs && receipt.origin == ORIGIN)
	}

	/// Admit-and-release, for asserting eligibility without abandoning a
	/// reservation - `reserve(..).is_ok()` would drop the guard and disable.
	fn assert_admits(policy: &Arc<MaintenancePolicyInner>, slabs: &[SlabId], why: &str) {
		match policy.reserve(OP, slabs) {
			Ok(reservation) => reservation.finalize(),
			Err(refusal) => panic!("{why}: refused with {refusal:?}"),
		}
	}

	#[tokio::test]
	async fn a_deterministic_failure_schedules_from_the_post_outcome_clock() {
		let clock = Arc::new(ManualClock::at(1_000_000));
		let (policy, _store) = enabled_policy_with_store(clock.clone()).await;
		let slabs = [SlabId::new(5), SlabId::new(6)];

		// The build outlasts its own backoff: the tick began at 1,000,000 and the
		// failure lands at 1,010,000. Scheduling from the tick's start would have
		// made this eligible the instant it failed.
		let work_clock = clock.clone();
		let attempt = policy
			.execute(OP, &slabs, classify_io_error, rewrote, || async move {
				work_clock.set(1_010_000);
				Err::<(), _>(det_err("shape"))
			})
			.await;
		assert!(matches!(attempt, MaintenanceAttempt::Failed(_)));

		let receipt = receipt_for(&policy, &slabs).expect("a receipt was written");
		assert_eq!(receipt.attempts, 1);
		assert_eq!(receipt.last_failure_ms, 1_010_000);
		assert_eq!(receipt.next_eligible_at_ms, Some(1_015_000));
		assert_eq!(receipt.detail, "shape");

		clock.set(1_014_999);
		assert_eq!(
			policy.reserve(OP, &slabs).err(),
			Some(AdmissionRefusal::NotYetEligible)
		);
		clock.set(1_015_000);
		assert_admits(&policy, &slabs, "at the deadline");
		assert!(policy.is_enabled());
	}

	#[tokio::test]
	async fn three_deterministic_failures_quarantine_and_only_requeue_clears_it() {
		let clock = Arc::new(ManualClock::at(1_000_000));
		let (policy, store) = enabled_policy_with_store(clock.clone()).await;
		let slabs = [SlabId::new(7)];
		let metrics = crate::metrics::global();
		let before_quarantined =
			metrics.maintenance_attempts(MaintenanceAttemptResult::Quarantined);
		let before_reason = metrics.maintenance_deterministic_failures(ORIGIN.reason());

		for attempt in 1..=3u32 {
			clock.set(u64::from(attempt) * 10_000_000);
			fail_deterministically(&policy, &slabs).await;
			assert_eq!(receipt_for(&policy, &slabs).unwrap().attempts, attempt);
		}
		let report = policy.report();
		assert_eq!(report.quarantined().count(), 1);
		assert!(
			!policy.is_eligible(OP, &slabs, u64::MAX),
			"quarantine is absorbing"
		);
		assert_eq!(
			policy.reserve(OP, &slabs).err(),
			Some(AdmissionRefusal::Quarantined)
		);
		assert_eq!(
			metrics.maintenance_attempts(MaintenanceAttemptResult::Quarantined)
				- before_quarantined,
			1
		);
		assert_eq!(
			metrics.maintenance_deterministic_failures(ORIGIN.reason()) - before_reason,
			3
		);
		assert_eq!(policy.gauge_snapshot_for_test(), (1, 1));

		// Requeue refuses work that is not quarantined ...
		let unreviewed = MaintenanceFingerprint {
			version: MAINTENANCE_FINGERPRINT_VERSION,
			digest: [0; 32],
		};
		assert_eq!(
			policy.requeue(OP, &[SlabId::new(99)], unreviewed).await,
			Err(RequeueRefusal::NotQuarantined)
		);
		// ... and work that is in flight.
		let other = [SlabId::new(8)];
		let held = policy.reserve(OP, &other).unwrap();
		assert_eq!(
			policy.requeue(OP, &other, unreviewed).await,
			Err(RequeueRefusal::InFlight)
		);
		held.finalize();

		// The requeue clears it durably and in memory, with a fresh budget.
		let expected = policy.quarantine_fingerprint_for_test(OP, &slabs).unwrap();
		assert_eq!(policy.requeue(OP, &slabs, expected).await, Ok(1));
		assert!(receipt_for(&policy, &slabs).is_none());
		assert!(
			store
				.load_maintenance_receipts(receipt_load_bounds())
				.await
				.unwrap()
				.is_empty()
		);
		assert_eq!(policy.gauge_snapshot_for_test(), (1, 0));
		assert_admits(&policy, &slabs, "after requeue");
		fail_deterministically(&policy, &slabs).await;
		assert_eq!(receipt_for(&policy, &slabs).unwrap().attempts, 1);
	}

	/// The bound is on working-set BUILDS, not on recorded attempts: a second
	/// identical plan must not enter while the first is paused in flight, or the
	/// second runs regardless of how the first ends.
	#[tokio::test]
	async fn a_second_identical_plan_cannot_enter_while_the_first_is_in_flight() {
		let clock = Arc::new(ManualClock::at(1_000_000));
		let (policy, _store) = enabled_policy_with_store(clock.clone()).await;
		let slabs = vec![SlabId::new(3), SlabId::new(1)];

		let (release, released) = tokio::sync::oneshot::channel::<()>();
		let first = {
			let policy = Arc::clone(&policy);
			let slabs = slabs.clone();
			tokio::spawn(async move {
				policy
					.execute(OP, &slabs, classify_io_error, rewrote, || async move {
						released.await.unwrap();
						Err::<(), _>(det_err("shape"))
					})
					.await
			})
		};
		tokio::task::yield_now().await;
		assert_eq!(
			policy.report().in_flight,
			vec![vec![SlabId::new(1), SlabId::new(3)]]
		);

		// Same work, differently ordered: refused immediately, no build.
		let second: MaintenanceAttempt<(), Error> = policy
			.execute(
				OP,
				&[SlabId::new(1), SlabId::new(3)],
				classify_io_error,
				rewrote,
				|| async { panic!("the second attempt must never run") },
			)
			.await;
		assert!(
			matches!(
				second,
				MaintenanceAttempt::Skipped(AdmissionRefusal::AlreadyInFlight)
			),
			"{second:?}"
		);
		// Disjoint work is unaffected.
		let disjoint = policy
			.execute(
				OP,
				&[SlabId::new(9)],
				classify_io_error,
				rewrote,
				|| async { Ok::<_, Error>(()) },
			)
			.await;
		assert!(matches!(disjoint, MaintenanceAttempt::Completed(())));

		release.send(()).unwrap();
		let first = first.await.unwrap();
		assert!(matches!(first, MaintenanceAttempt::Failed(_)));
		assert!(policy.report().in_flight.is_empty());
		let canonical = [SlabId::new(1), SlabId::new(3)];
		assert_eq!(receipt_for(&policy, &canonical).unwrap().attempts, 1);
		// Now the first's outcome governs the second: backoff, not admission.
		let third: MaintenanceAttempt<(), Error> = policy
			.execute(OP, &slabs, classify_io_error, rewrote, || async {
				panic!("still in backoff")
			})
			.await;
		assert!(
			matches!(
				third,
				MaintenanceAttempt::Skipped(AdmissionRefusal::NotYetEligible)
			),
			"{third:?}"
		);
		assert!(policy.is_enabled());
	}

	#[tokio::test]
	async fn retryable_cooldown_is_ephemeral_and_counts_consecutive_failures_only() {
		let clock = Arc::new(ManualClock::at(1_000_000));
		let (policy, store) = enabled_policy_with_store(clock.clone()).await;
		let slabs = [SlabId::new(4)];
		let retry = |policy: Arc<MaintenancePolicyInner>| async move {
			let attempt = policy
				.execute(OP, &slabs, classify_io_error, rewrote, || async {
					Err::<(), _>(race_err())
				})
				.await;
			assert!(matches!(attempt, MaintenanceAttempt::Failed(_)));
		};

		retry(Arc::clone(&policy)).await;
		let cooldown = policy.report().retryable;
		assert_eq!(cooldown.len(), 1);
		assert_eq!(cooldown[0].consecutive_failures, 1);
		assert_eq!(cooldown[0].next_eligible_at_ms, 1_000_250);
		assert_eq!(
			policy.reserve(OP, &slabs).err(),
			Some(AdmissionRefusal::NotYetEligible)
		);
		clock.set(1_000_250);
		retry(Arc::clone(&policy)).await;
		let cooldown = policy.report().retryable;
		assert_eq!(cooldown[0].consecutive_failures, 2);
		assert_eq!(cooldown[0].next_eligible_at_ms, 1_000_750, "doubling");

		// Ephemeral: nothing about it reaches the store.
		assert!(
			store
				.load_maintenance_receipts(receipt_load_bounds())
				.await
				.unwrap()
				.is_empty()
		);
		let reloaded = MaintenancePolicyInner::load(Arc::clone(&store), clock.clone()).await;
		assert!(reloaded.report().retryable.is_empty());

		// A deterministic failure ends the consecutive run.
		clock.set(1_000_750);
		fail_deterministically(&policy, &slabs).await;
		assert!(policy.report().retryable.is_empty());
		assert_eq!(receipt_for(&policy, &slabs).unwrap().attempts, 1);
		clock.set(2_000_000);
		retry(Arc::clone(&policy)).await;
		assert_eq!(
			policy.report().retryable[0].consecutive_failures,
			1,
			"reset"
		);

		// Success clears both.
		clock.set(3_000_000);
		let ok = policy
			.execute(OP, &slabs, classify_io_error, rewrote, || async {
				Ok::<_, Error>(())
			})
			.await;
		assert!(matches!(ok, MaintenanceAttempt::Completed(())));
		let report = policy.report();
		assert!(report.retryable.is_empty());
		assert!(report.receipts.is_empty());
		assert!(
			store
				.load_maintenance_receipts(receipt_load_bounds())
				.await
				.unwrap()
				.is_empty()
		);
	}

	/// Failure-order 4: a success on the EXACT slab set removes every origin's
	/// receipt for that set, and touches no other set.
	#[tokio::test]
	async fn success_removes_every_origin_receipt_for_the_exact_slab_set_only() {
		let clock = Arc::new(ManualClock::at(1_000_000));
		let (_policy, store) = enabled_policy_with_store(clock.clone()).await;
		let set_a = [SlabId::new(1), SlabId::new(2)];
		let set_b = [SlabId::new(1)];
		let receipt = DeterministicReceipt::after_failure(None, 0, "x").unwrap();
		for (slabs, origin) in [
			(&set_a[..], DeterministicOrigin::LsmInconsistentSourceFrames),
			(
				&set_a[..],
				DeterministicOrigin::LsmRewriteDuplicateRowIdentity,
			),
			(&set_b[..], DeterministicOrigin::LsmInconsistentSourceFrames),
		] {
			let key = MaintenanceWorkKey::new(OP, slabs.iter().copied(), origin).unwrap();
			store
				.put_maintenance_receipt(key.encode(), receipt.encode())
				.await
				.unwrap();
		}
		let policy = MaintenancePolicyInner::load(Arc::clone(&store), clock).await;
		assert_eq!(policy.report().receipts.len(), 3);
		// Every origin's schedule has elapsed; the work is admissible.
		let ok = policy
			.execute(OP, &set_a, classify_io_error, rewrote, || async {
				Ok::<_, Error>(())
			})
			.await;
		assert!(matches!(ok, MaintenanceAttempt::Completed(())));

		let remaining = policy.report().receipts;
		assert_eq!(remaining.len(), 1);
		assert_eq!(remaining[0].slabs, set_b);
		let durable = store
			.load_maintenance_receipts(receipt_load_bounds())
			.await
			.unwrap();
		assert_eq!(durable.len(), 1);
		assert_eq!(
			MaintenanceWorkKey::decode(&durable[0].0).unwrap().slabs(),
			&set_b
		);
		assert!(policy.is_enabled());
	}

	/// Failure-order 2: a receipt that cannot be written admits no further plan.
	#[tokio::test]
	async fn a_receipt_write_failure_admits_no_further_plan() {
		let clock = Arc::new(ManualClock::at(1_000_000));
		let (policy, store) = enabled_policy_with_store(clock.clone()).await;
		let slabs = [SlabId::new(5)];
		store
			.fail_receipt_mutations
			.store(true, std::sync::atomic::Ordering::SeqCst);

		let attempt = policy
			.execute(OP, &slabs, classify_io_error, rewrote, || async {
				Err::<(), _>(det_err("shape"))
			})
			.await;
		assert!(
			matches!(attempt, MaintenanceAttempt::Failed(_)),
			"the work's own outcome is reported"
		);
		assert!(!policy.is_enabled());
		let diagnostic = policy.diagnostic().unwrap();
		assert!(
			diagnostic.contains("could not be persisted"),
			"{diagnostic}"
		);
		assert!(
			policy.report().in_flight.is_empty(),
			"the reservation was released"
		);
		assert!(matches!(
			policy.reserve(OP, &[SlabId::new(6)]).err(),
			Some(AdmissionRefusal::PolicyDisabled(_))
		));
		// Nothing half-written: the store has no receipt and a fresh load is clean.
		store
			.fail_receipt_mutations
			.store(false, std::sync::atomic::Ordering::SeqCst);
		let reloaded = MaintenancePolicyInner::load(store, clock).await;
		assert!(reloaded.is_enabled());
		assert!(reloaded.report().receipts.is_empty());
	}

	/// Failure-order 3: a requeue whose deletion fails leaves the receipt on disk
	/// AND the in-memory quarantine effective.
	#[tokio::test]
	async fn a_requeue_deletion_failure_leaves_the_quarantine_effective() {
		let clock = Arc::new(ManualClock::at(1_000_000));
		let (policy, store) = enabled_policy_with_store(clock.clone()).await;
		let slabs = [SlabId::new(5)];
		for attempt in 1..=3u32 {
			clock.set(u64::from(attempt) * 10_000_000);
			fail_deterministically(&policy, &slabs).await;
		}
		assert_eq!(
			policy.reserve(OP, &slabs).err(),
			Some(AdmissionRefusal::Quarantined)
		);

		store
			.fail_receipt_mutations
			.store(true, std::sync::atomic::Ordering::SeqCst);
		assert!(matches!(
			policy
				.requeue(
					OP,
					&slabs,
					policy.quarantine_fingerprint_for_test(OP, &slabs).unwrap()
				)
				.await,
			Err(RequeueRefusal::StoreFailure(_))
		));
		assert!(!policy.is_enabled());
		assert!(
			!policy.is_eligible(OP, &slabs, u64::MAX),
			"the quarantine remains effective while disabled"
		);
		store
			.fail_receipt_mutations
			.store(false, std::sync::atomic::Ordering::SeqCst);
		let reloaded = MaintenancePolicyInner::load(store, clock).await;
		assert!(reloaded.is_enabled());
		assert_eq!(
			reloaded.report().quarantined().count(),
			1,
			"the receipt is still on disk"
		);
		assert_eq!(
			reloaded.reserve(OP, &slabs).err(),
			Some(AdmissionRefusal::Quarantined)
		);
	}

	/// Failure-order 5: reopen after attempts 1, 2 and 3 preserves the backoff
	/// schedule and the quarantine, each on its own terms. Each reopen is a real reopen of
	/// the index file, with every prior handle dropped.
	#[tokio::test]
	async fn reopen_after_each_attempt_preserves_the_schedule_and_the_quarantine() {
		let clock = Arc::new(ManualClock::at(1_000_000));
		let path = temp_index_path("reopen-each-attempt");
		let (policy, store) = enabled_policy_at(&path, clock.clone()).await;
		let slabs = [SlabId::new(11), SlabId::new(12)];
		let backoff = deterministic_backoff;

		// Attempt 1.
		fail_deterministically(&policy, &slabs).await;
		drop(policy);
		let store = reopen_store(store, &path).await;
		let policy = MaintenancePolicyInner::load(Arc::clone(&store), clock.clone()).await;
		let deadline_1 = 1_000_000 + backoff(1).unwrap().as_millis() as u64;
		clock.set(deadline_1 - 1);
		assert_eq!(
			policy.reserve(OP, &slabs).err(),
			Some(AdmissionRefusal::NotYetEligible)
		);
		clock.set(deadline_1);
		assert_admits(&policy, &slabs, "attempt 1 deadline survives reopen");

		// Attempt 2, from the reopened policy.
		fail_deterministically(&policy, &slabs).await;
		drop(policy);
		let store = reopen_store(store, &path).await;
		let policy = MaintenancePolicyInner::load(Arc::clone(&store), clock.clone()).await;
		assert_eq!(receipt_for(&policy, &slabs).unwrap().attempts, 2);
		let deadline_2 = deadline_1 + backoff(2).unwrap().as_millis() as u64;
		clock.set(deadline_2 - 1);
		assert_eq!(
			policy.reserve(OP, &slabs).err(),
			Some(AdmissionRefusal::NotYetEligible)
		);
		clock.set(deadline_2);
		assert_admits(&policy, &slabs, "attempt 2 deadline survives reopen");

		// Attempt 3 quarantines; the reopened policy holds it at any clock value.
		fail_deterministically(&policy, &slabs).await;
		drop(policy);
		let store = reopen_store(store, &path).await;
		let policy = MaintenancePolicyInner::load(Arc::clone(&store), clock.clone()).await;
		assert_eq!(policy.report().quarantined().count(), 1);
		for at in [0, deadline_2, u64::MAX] {
			clock.set(at);
			assert_eq!(
				policy.reserve(OP, &slabs).err(),
				Some(AdmissionRefusal::Quarantined)
			);
		}
		assert!(policy.is_enabled());
	}

	/// Success cleanup failure: the work was published, so the caller still sees
	/// success; the policy, no longer able to trust its record, disables.
	#[tokio::test]
	async fn success_cleanup_failure_disables_policy_but_reports_success() {
		let clock = Arc::new(ManualClock::at(1_000_000));
		let (policy, store) = enabled_policy_with_store(clock.clone()).await;
		let slabs = [SlabId::new(5)];
		clock.set(1);
		fail_deterministically(&policy, &slabs).await;
		clock.set(1_000_000);
		store
			.fail_receipt_mutations
			.store(true, std::sync::atomic::Ordering::SeqCst);

		let attempt = policy
			.execute(OP, &slabs, classify_io_error, rewrote, || async {
				Ok::<_, Error>(42)
			})
			.await;
		assert!(
			matches!(attempt, MaintenanceAttempt::Completed(42)),
			"{attempt:?}"
		);
		assert!(!policy.is_enabled());
		assert!(policy.diagnostic().unwrap().contains("cleanup"));
		assert!(policy.report().in_flight.is_empty());
		store
			.fail_receipt_mutations
			.store(false, std::sync::atomic::Ordering::SeqCst);
		let reloaded = MaintenancePolicyInner::load(store, clock).await;
		assert_eq!(
			reloaded.report().receipts.len(),
			1,
			"the receipt outlives the failed cleanup"
		);
	}

	struct ClockThatFailsAfter(std::sync::atomic::AtomicU32);

	impl MaintenanceClock for ClockThatFailsAfter {
		fn now_ms(&self) -> Option<u64> {
			let remaining = &self.0;
			if remaining.load(std::sync::atomic::Ordering::SeqCst) == 0 {
				return None;
			}
			remaining.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
			Some(1_000_000)
		}
	}

	#[tokio::test]
	async fn a_clock_failure_disables_admission_at_either_end_of_an_attempt() {
		// Fails at admission.
		let (policy, _store) =
			enabled_policy_with_store(Arc::new(ClockThatFailsAfter(0.into()))).await;
		let attempt: MaintenanceAttempt<(), Error> = policy
			.execute(
				OP,
				&[SlabId::new(1)],
				classify_io_error,
				rewrote,
				|| async { panic!("never admitted") },
			)
			.await;
		assert!(matches!(
			attempt,
			MaintenanceAttempt::Skipped(AdmissionRefusal::ClockUnavailable)
		));
		assert!(!policy.is_enabled());

		// Fails after the outcome: one sample for admission, none for the record.
		let (policy, _store) =
			enabled_policy_with_store(Arc::new(ClockThatFailsAfter(1.into()))).await;
		let attempt = policy
			.execute(
				OP,
				&[SlabId::new(1)],
				classify_io_error,
				rewrote,
				|| async { Err::<(), _>(det_err("shape")) },
			)
			.await;
		assert!(matches!(attempt, MaintenanceAttempt::Failed(_)));
		assert!(!policy.is_enabled());
		assert!(
			policy
				.diagnostic()
				.unwrap()
				.contains("after a failed attempt")
		);
		assert!(policy.report().in_flight.is_empty());
	}

	/// Cancellation between the build and the record is an attempt nothing
	/// counted. The guard's drop disables rather than readmitting.
	#[tokio::test]
	async fn an_abandoned_attempt_disables_admission() {
		let clock = Arc::new(ManualClock::at(1_000_000));
		let (policy, _store) = enabled_policy_with_store(clock).await;
		let task = {
			let policy = Arc::clone(&policy);
			tokio::spawn(async move {
				policy
					.execute(
						OP,
						&[SlabId::new(2)],
						classify_io_error,
						rewrote,
						|| async { std::future::pending::<Result<(), Error>>().await },
					)
					.await
			})
		};
		tokio::task::yield_now().await;
		assert_eq!(policy.report().in_flight.len(), 1);
		task.abort();
		assert!(task.await.unwrap_err().is_cancelled());
		assert!(!policy.is_enabled());
		assert!(policy.diagnostic().unwrap().contains("abandoned"));
		assert!(
			policy.report().in_flight.is_empty(),
			"no phantom reservation survives"
		);
	}

	#[test]
	fn every_origin_is_wired_to_exactly_one_production_site() {
		let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
		let sources = [
			"src/storage/manager/lsm_driver.rs",
			"src/storage/manager/compaction.rs",
		]
		.iter()
		.map(|path| std::fs::read_to_string(manifest_dir.join(path)).unwrap())
		.collect::<Vec<_>>()
		.join("\n");
		// The two invariance origins are produced by classification of typed
		// `CompactionFailure` variants, not by a tagged construction site.
		let classified_not_constructed = [
			"DeterministicOrigin::SidecarDigestMismatch",
			"DeterministicOrigin::VisibleDomainInvariance",
		];
		for (name, expected) in [
			("LsmWriterCannotSplitMultiRowFrame", 1),
			("LsmRewriteProducedNoSurvivors", 1),
			("LsmRewriteFrameNotMultiRow", 1),
			("LsmRewriteDuplicateRowIdentity", 1),
			("LsmRewriteKeptWrongRowCount", 1),
			("LsmSourceFrameMissingPlannedSurvivor", 2),
			("LsmInconsistentSourceFrames", 1),
			("LsmRewrittenFrameCarriesUnplannedRow", 1),
			("LsmMultiRowSourceFrameHasNoEntries", 1),
			("LsmOutputLocationMissingPlannedRow", 1),
			("MaintenanceRewriteWithoutClaim", 1),
		] {
			let needle = format!("DeterministicOrigin::{name}");
			assert!(
				!classified_not_constructed.contains(&needle.as_str()),
				"{name} is classified, not constructed"
			);
			assert_eq!(
				sources.matches(&needle).count(),
				expected,
				"{name} must be tagged at exactly {expected} production site(s)"
			);
		}
	}

	/// A key naming no work is one this type's own decoder refuses; constructing it
	/// would write a receipt that fails closed on the next startup.
	#[test]
	fn an_empty_slab_set_is_not_a_work_key() {
		assert_eq!(
			MaintenanceWorkKey::new(OP, [], DeterministicOrigin::LsmInconsistentSourceFrames),
			Err(ReceiptDecodeError::NoSlabs)
		);
	}

	/// The two invariance failures are distinct defects and must not share an
	/// attempt budget: a sidecar digest mismatch and a visible-domain change have
	/// different causes and different fixes.
	#[test]
	fn the_two_invariance_failures_have_separate_budgets() {
		let sidecar = classify_compaction_failure(&CompactionFailure::SidecarDigestMismatch);
		let visible = classify_compaction_failure(&CompactionFailure::VisibleDigestInvariance);
		assert_ne!(sidecar, visible, "distinct origins");
		assert_eq!(
			sidecar.label(),
			visible.label(),
			"but they report under one metric category"
		);
		let slabs = [SlabId::new(4)];
		assert_ne!(
			MaintenanceWorkKey::new(OP, slabs, DeterministicOrigin::SidecarDigestMismatch).unwrap(),
			MaintenanceWorkKey::new(OP, slabs, DeterministicOrigin::VisibleDomainInvariance)
				.unwrap(),
		);
	}

	/// Classification runs on the failure path of every attempt, so a pathological
	/// `source()` cycle must not hang it.
	#[test]
	fn a_cyclic_error_chain_terminates() {
		#[derive(Debug)]
		struct Loop;
		impl std::fmt::Display for Loop {
			fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
				write!(f, "loop")
			}
		}
		// A self-referential source: `source()` always yields another link.
		impl std::error::Error for Loop {
			fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
				Some(&Loop)
			}
		}
		assert_eq!(
			classify_io_error(&Error::new(ErrorKind::Other, Loop)),
			MaintenanceFailureClass::Retryable,
			"a cycle terminates at the bound rather than hanging"
		);
	}

	#[test]
	fn restringifying_a_failure_loses_its_class() {
		let tagged = deterministic_failure(
			ErrorKind::InvalidData,
			DeterministicOrigin::LsmInconsistentSourceFrames,
			"structural defect",
		);
		assert!(classify_io_error(&tagged).is_deterministic());

		let restringified = Error::new(tagged.kind(), tagged.to_string());
		assert_eq!(
			classify_io_error(&restringified),
			MaintenanceFailureClass::Retryable,
			"a string re-wrap loses the class - origins must pass the error itself"
		);

		// A wrap that preserves the error as a source keeps the class, because
		// classification walks the chain.
		let preserved = deterministic_failure(
			ErrorKind::InvalidData,
			DeterministicOrigin::LsmInconsistentSourceFrames,
			"structural defect",
		);
		let nested = Error::new(ErrorKind::Other, preserved);
		assert_eq!(
			classify_io_error(&nested),
			MaintenanceFailureClass::Deterministic(
				DeterministicOrigin::LsmInconsistentSourceFrames
			),
			"a source-preserving wrap keeps the class"
		);
	}

	#[test]
	fn metric_categories_are_labels_only_and_are_not_persisted() {
		// Derived from the origin on demand; nothing writes a category to disk, so
		// there is no origin/category agreement for a decoder to enforce.
		assert_eq!(
			DeterministicOrigin::LsmWriterCannotSplitMultiRowFrame.reason(),
			DeterministicFailureReason::UnsupportedFrameShape
		);
		assert_eq!(
			DeterministicOrigin::MaintenanceRewriteWithoutClaim.reason(),
			DeterministicFailureReason::OwnershipViolation
		);
		assert_eq!(
			DeterministicOrigin::LsmInconsistentSourceFrames
				.reason()
				.label(),
			"corrupt_source_frame"
		);
		// The encoded key carries the operation code, then the ORIGIN code, and
		// nothing about the category.
		let key = MaintenanceWorkKey::new(
			OP,
			[SlabId::new(1)],
			DeterministicOrigin::LsmInconsistentSourceFrames,
		)
		.unwrap();
		assert_eq!(
			&key.encode()[2..4],
			&DeterministicOrigin::LsmInconsistentSourceFrames
				.code()
				.to_be_bytes()[..]
		);
	}

	/// The discriminating case for the operation in the work identity: an LSM
	/// receipt at attempt 1, a SUCCESSFUL backfill rewrite over the same slab once
	/// the backoff has elapsed, and the next LSM failure must be attempt 2. Under
	/// a slab-set-only identity the backfill's success erased the receipt and the
	/// count restarted at one: unrelated work over the same slabs could spend an
	/// unbounded number of LSM attempts one receipt at a time.
	#[tokio::test]
	async fn another_operations_rewrite_does_not_clear_this_operations_receipt() {
		let clock = Arc::new(ManualClock::at(1_000_000));
		let (policy, store) = enabled_policy_with_store(clock.clone()).await;
		let slabs = [SlabId::new(7)];
		fail_deterministically(&policy, &slabs).await;
		assert_eq!(receipt_for(&policy, &slabs).unwrap().attempts, 1);

		// The LSM backoff has elapsed; a backfill over the same slab rewrites it.
		clock.set(1_005_000);
		let ok = policy
			.execute(
				MaintenanceOperation::SecondaryIndexBackfill,
				&slabs,
				classify_io_error,
				rewrote,
				|| async { Ok::<_, Error>(()) },
			)
			.await;
		assert!(matches!(ok, MaintenanceAttempt::Completed(())));
		let receipt = receipt_for(&policy, &slabs).expect("the LSM receipt stands");
		assert_eq!(receipt.operation, OP);
		assert_eq!(receipt.attempts, 1);
		assert_eq!(
			store
				.load_maintenance_receipts(receipt_load_bounds())
				.await
				.unwrap()
				.len(),
			1,
			"durably, too"
		);

		// The next LSM failure is attempt TWO, not a fresh attempt one.
		clock.set(1_005_001);
		fail_deterministically(&policy, &slabs).await;
		assert_eq!(receipt_for(&policy, &slabs).unwrap().attempts, 2);
		assert!(policy.is_enabled());

		// And the LSM's own rewrite clears it: the identity is the operation, not
		// the slab set alone in the other direction either.
		clock.set(2_000_000);
		let ok = policy
			.execute(OP, &slabs, classify_io_error, rewrote, || async {
				Ok::<_, Error>(())
			})
			.await;
		assert!(matches!(ok, MaintenanceAttempt::Completed(())));
		assert!(receipt_for(&policy, &slabs).is_none());
	}

	/// A completion that rewrote nothing is neither success nor failure: the
	/// reservation is released and every receipt and cooldown stands as it was.
	/// Retention returns `Ok` with no compaction for a slab with nothing to prune;
	/// treating that as a rewrite would clear a receipt on no evidence at all.
	#[tokio::test]
	async fn a_completion_that_rewrote_nothing_records_nothing() {
		let clock = Arc::new(ManualClock::at(1_000_000));
		let (policy, store) = enabled_policy_with_store(clock.clone()).await;
		let metrics = crate::metrics::global();
		let neutral = |_: &()| SuccessDisposition::Neutral;

		// A deterministic receipt on one set, a retryable cooldown on another.
		let receipted = [SlabId::new(8)];
		let cooled = [SlabId::new(9)];
		fail_deterministically(&policy, &receipted).await;
		let raced = policy
			.execute(OP, &cooled, classify_io_error, rewrote, || async {
				Err::<(), _>(race_err())
			})
			.await;
		assert!(matches!(raced, MaintenanceAttempt::Failed(_)));
		assert_eq!(policy.report().retryable[0].consecutive_failures, 1);

		// Both schedules elapse; both attempts complete without rewriting.
		clock.set(1_005_000);
		let before =
			metrics.maintenance_attempts(MaintenanceAttemptResult::CompletedWithoutRewrite);
		for slabs in [&receipted[..], &cooled[..]] {
			let done = policy
				.execute(OP, slabs, classify_io_error, neutral, || async {
					Ok::<_, Error>(())
				})
				.await;
			assert!(matches!(done, MaintenanceAttempt::Completed(())));
		}
		assert!(
			metrics.maintenance_attempts(MaintenanceAttemptResult::CompletedWithoutRewrite)
				>= before + 2,
			"the reporting half sees a neutral completion as its own outcome"
		);

		let report = policy.report();
		assert!(report.enabled);
		assert!(
			report.in_flight.is_empty(),
			"the reservations were released"
		);
		assert_eq!(
			receipt_for(&policy, &receipted).unwrap().attempts,
			1,
			"the receipt stands"
		);
		assert_eq!(
			report.retryable[0].consecutive_failures, 1,
			"and so does the cooldown"
		);
		assert_eq!(
			store
				.load_maintenance_receipts(receipt_load_bounds())
				.await
				.unwrap()
				.len(),
			1
		);

		// ADR 0098 decision 2: what a neutral completion DOES record is a cooldown
		// on the scan itself, beside the receipt and never in place of it.
		assert_eq!(
			policy
				.neutral_cooldowns_for_test()
				.into_iter()
				.map(|(_, slabs, cooldown)| (slabs, cooldown.next_eligible_at_ms))
				.collect::<Vec<_>>(),
			vec![
				(receipted.to_vec(), 1_010_000),
				(cooled.to_vec(), 1_010_000)
			]
		);

		// The next failure, once the scan cooldown has passed, continues the count
		// it found.
		clock.set(1_010_000);
		fail_deterministically(&policy, &receipted).await;
		assert_eq!(receipt_for(&policy, &receipted).unwrap().attempts, 2);
	}

	/// Receipts and cooldowns bar only their own operation: a receipt one job can
	/// never clear must not starve another job of the slabs. Work in flight over
	/// the exact set bars every operation, because the physical claims would
	/// serialize them anyway and the loser would spend a build on a race.
	#[tokio::test]
	async fn admission_is_per_operation_but_in_flight_work_bars_every_operation() {
		let clock = Arc::new(ManualClock::at(1_000_000));
		let (policy, _store) = enabled_policy_with_store(clock.clone()).await;

		// Quarantine the LSM over {3}: three deterministic failures on schedule.
		let quarantined = [SlabId::new(3)];
		for at in [1_000_000, 1_005_000, 1_035_000] {
			clock.set(at);
			fail_deterministically(&policy, &quarantined).await;
		}
		assert_eq!(
			policy.reserve(OP, &quarantined).err(),
			Some(AdmissionRefusal::Quarantined)
		);
		for other in [
			MaintenanceOperation::RetentionPrune,
			MaintenanceOperation::SecondaryIndexBackfill,
			MaintenanceOperation::TombstoneReclaim,
		] {
			match policy.reserve(other, &quarantined) {
				Ok(reservation) => reservation.finalize(),
				Err(refusal) => panic!("{other:?} barred by the LSM quarantine: {refusal:?}"),
			}
		}

		// A waiting LSM receipt over {4} does not bar tombstone reclaim of {4}.
		let waiting = [SlabId::new(4)];
		clock.set(2_000_000);
		fail_deterministically(&policy, &waiting).await;
		assert_eq!(
			policy.reserve(OP, &waiting).err(),
			Some(AdmissionRefusal::NotYetEligible)
		);
		let held = policy
			.reserve(MaintenanceOperation::TombstoneReclaim, &waiting)
			.expect("another operation's schedule is not this one's");

		// While THAT is in flight, nothing else gets the same slabs.
		assert_eq!(
			policy
				.reserve(MaintenanceOperation::RetentionPrune, &waiting)
				.err(),
			Some(AdmissionRefusal::AlreadyInFlight)
		);
		held.finalize();
		assert_admits_op(&policy, MaintenanceOperation::RetentionPrune, &waiting);

		// A retryable cooldown is the racing job's alone.
		let raced = [SlabId::new(5)];
		let lost = policy
			.execute(
				MaintenanceOperation::RetentionPrune,
				&raced,
				classify_io_error,
				rewrote,
				|| async { Err::<(), _>(race_err()) },
			)
			.await;
		assert!(matches!(lost, MaintenanceAttempt::Failed(_)));
		let cooldown = policy.report().retryable;
		assert_eq!(cooldown.len(), 1);
		assert_eq!(cooldown[0].operation, MaintenanceOperation::RetentionPrune);
		assert_eq!(
			policy
				.reserve(MaintenanceOperation::RetentionPrune, &raced)
				.err(),
			Some(AdmissionRefusal::NotYetEligible)
		);
		assert_admits_op(&policy, MaintenanceOperation::TombstoneReclaim, &raced);
		assert!(policy.is_enabled());
	}

	fn assert_admits_op(
		policy: &Arc<MaintenancePolicyInner>,
		operation: MaintenanceOperation,
		slabs: &[SlabId],
	) {
		match policy.reserve(operation, slabs) {
			Ok(reservation) => reservation.finalize(),
			Err(refusal) => panic!("{operation:?} over {slabs:?} refused with {refusal:?}"),
		}
	}

	/// The retryable counter is checked, not saturating: a counter pinned at its
	/// maximum would report a run of races as shorter than it was. Overflow is a
	/// fail-closed arithmetic failure like the deadline ones.
	#[tokio::test]
	async fn a_retryable_counter_that_would_overflow_disables_admission() {
		let clock = Arc::new(ManualClock::at(1_000_000));
		let (policy, _store) = enabled_policy_with_store(clock.clone()).await;
		let slabs = [SlabId::new(6)];
		{
			let mut state = policy.lock();
			let PolicyState::Enabled { tracker, .. } = &mut *state else {
				panic!("freshly enabled");
			};
			tracker.retryable.entry(OP).or_default().insert(
				slabs.to_vec(),
				RetryableCooldown {
					failures: u32::MAX,
					next_eligible_at_ms: 0,
				},
			);
		}
		assert_eq!(policy.report().retryable[0].consecutive_failures, u32::MAX);

		let lost = policy
			.execute(OP, &slabs, classify_io_error, rewrote, || async {
				Err::<(), _>(race_err())
			})
			.await;
		assert!(matches!(lost, MaintenanceAttempt::Failed(_)));
		assert!(!policy.is_enabled());
		assert!(
			policy.diagnostic().unwrap().contains("overflowed"),
			"{:?}",
			policy.diagnostic()
		);
		assert!(matches!(
			policy.reserve(OP, &[SlabId::new(1)]).err(),
			Some(AdmissionRefusal::PolicyDisabled(_))
		));
	}

	/// ADR 0097 decision 4: the v1 key pinned to EXPLICIT BYTES - operation code,
	/// origin code, then slab ids, all big-endian. A change to either code table or
	/// to the layout fails here instead of silently redefining every durable
	/// receipt.
	#[test]
	fn v1_work_key_bytes_are_pinned() {
		let key = MaintenanceWorkKey::new(
			MaintenanceOperation::RetentionPrune,
			[SlabId::new(0x0102), SlabId::new(3)],
			DeterministicOrigin::MaintenanceRewriteWithoutClaim,
		)
		.unwrap();
		let bytes = [
			0x00, 0x02, // operation = retention prune
			0x00, 0x0B, // origin = maintenance rewrite without claim
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, // slab 3
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02, // slab 0x0102
		];
		assert_eq!(key.encode(), bytes);
		assert_eq!(MaintenanceWorkKey::decode(&bytes), Ok(key));
	}

	#[test]
	fn operation_codes_round_trip_and_are_stable() {
		let mut codes = MaintenanceOperation::ALL
			.iter()
			.map(|operation| operation.code())
			.collect::<Vec<_>>();
		codes.sort_unstable();
		codes.dedup();
		assert_eq!(codes.len(), MaintenanceOperation::ALL.len());
		for operation in MaintenanceOperation::ALL {
			assert_eq!(
				MaintenanceOperation::from_code(operation.code()),
				Some(operation)
			);
		}
		assert_eq!(MaintenanceOperation::from_code(0), None);
		// Pinned: these are written into durable receipts.
		assert_eq!(
			MaintenanceOperation::ALL.map(|operation| operation.code()),
			[1, 2, 3, 4]
		);
	}

	/// Every operation the policy knows is one a production job declares at its
	/// `execute` site, and every `execute` site declares exactly one of them. A
	/// variant nothing declares is dead identity; a site declaring none cannot
	/// compile, but one declaring the WRONG job would pool two jobs' budgets.
	#[test]
	fn every_operation_is_declared_at_exactly_one_execute_site() {
		let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
		let sources = [
			"src/storage/manager/lsm_driver.rs",
			"src/storage/manager/retention_reclaim.rs",
			"src/storage/manager/secondary_index_segments.rs",
			"src/storage/manager/compaction.rs",
		]
		.iter()
		.map(|path| std::fs::read_to_string(manifest_dir.join(path)).unwrap())
		.collect::<Vec<_>>()
		.join("\n");
		// Indentation-agnostic: the sites sit at different nesting depths.
		let sources = sources.replace('\t', "");
		let execute_sites = sources.matches(".maintenance_policy\n.execute(").count();
		assert_eq!(
			execute_sites,
			MaintenanceOperation::ALL.len(),
			"one policed job per operation"
		);
		for operation in MaintenanceOperation::ALL {
			let needle =
				format!(".maintenance_policy\n.execute(\nMaintenanceOperation::{operation:?},\n");
			assert_eq!(
				sources.matches(&needle).count(),
				1,
				"{operation:?} must be declared at exactly one execute site"
			);
		}
	}

	/// Requeue is scoped to the operation the report named. Two jobs quarantined
	/// over the same slabs, one requeued: the other's quarantine stands, in memory
	/// and on disk across a real reopen of the index. A slab-set-only requeue would
	/// have let a fixed LSM rewrite clear a retention defect it knows nothing about.
	#[tokio::test]
	async fn requeue_clears_only_the_named_operations_quarantine() {
		let clock = Arc::new(ManualClock::at(1_000_000));
		let path = temp_index_path("requeue-scope");
		let (policy, store) = enabled_policy_at(&path, clock.clone()).await;
		drop(policy);
		let slabs = [SlabId::new(11)];
		let quarantined = {
			let a = DeterministicReceipt::after_failure(None, 1_000, "a").unwrap();
			let b = DeterministicReceipt::after_failure(Some(&a), 2_000, "b").unwrap();
			DeterministicReceipt::after_failure(Some(&b), 3_000, "c").unwrap()
		};
		assert!(quarantined.is_quarantined());
		for operation in [OP, MaintenanceOperation::RetentionPrune] {
			let key = MaintenanceWorkKey::new(operation, slabs, ORIGIN).unwrap();
			store
				.put_maintenance_receipt(key.encode(), quarantined.encode())
				.await
				.unwrap();
		}
		let policy = MaintenancePolicyInner::load(Arc::clone(&store), clock.clone()).await;
		assert_eq!(policy.report().quarantined().count(), 2);

		// An operation with no quarantine over the set is refused, even though
		// another operation's quarantine is right there.
		assert_eq!(
			policy
				.requeue(
					MaintenanceOperation::SecondaryIndexBackfill,
					&slabs,
					MaintenanceFingerprint {
						version: MAINTENANCE_FINGERPRINT_VERSION,
						digest: [0; 32],
					}
				)
				.await,
			Err(RequeueRefusal::NotQuarantined)
		);

		let expected = policy.quarantine_fingerprint_for_test(OP, &slabs).unwrap();
		assert_eq!(policy.requeue(OP, &slabs, expected).await, Ok(1));
		assert_admits_op(&policy, OP, &slabs);
		assert_eq!(
			policy
				.reserve(MaintenanceOperation::RetentionPrune, &slabs)
				.err(),
			Some(AdmissionRefusal::Quarantined),
			"retention's quarantine is not the LSM's to clear"
		);

		// Durably: a real reopen, and the retention quarantine is the one that
		// remains.
		drop(policy);
		let store = reopen_store(store, &path).await;
		let reloaded = MaintenancePolicyInner::load(store, clock).await;
		let report = reloaded.report();
		let remaining = report.quarantined().collect::<Vec<_>>();
		assert_eq!(remaining.len(), 1);
		assert_eq!(remaining[0].operation, MaintenanceOperation::RetentionPrune);
		assert_eq!(remaining[0].slabs, slabs);
		assert_eq!(
			reloaded
				.reserve(MaintenanceOperation::RetentionPrune, &slabs)
				.err(),
			Some(AdmissionRefusal::Quarantined)
		);
		assert_admits_op(&reloaded, OP, &slabs);
	}

	fn quarantined_receipt(detail: &str) -> DeterministicReceipt {
		let first = DeterministicReceipt::after_failure(None, 1_000, detail).unwrap();
		let second = DeterministicReceipt::after_failure(Some(&first), 2_000, detail).unwrap();
		DeterministicReceipt::after_failure(Some(&second), 3_000, detail).unwrap()
	}

	/// ADR 0582: an item is one complete work identity, never one receipt. With a
	/// one-item page, two origins for one operation/slab set must remain together;
	/// otherwise the fingerprint would authorize clearing an origin the page hid.
	#[tokio::test]
	async fn report_paging_never_splits_one_work_identity() {
		let clock = Arc::new(ManualClock::at(10_000));
		let (policy, store) = enabled_policy_with_store(clock).await;
		drop(policy);
		let slabs = [SlabId::new(4), SlabId::new(9)];
		let receipt = quarantined_receipt("shape");
		for origin in [
			DeterministicOrigin::LsmInconsistentSourceFrames,
			DeterministicOrigin::LsmSourceFrameMissingPlannedSurvivor,
		] {
			let key = MaintenanceWorkKey::new(OP, slabs, origin).unwrap();
			store
				.put_maintenance_receipt(key.encode(), receipt.encode())
				.await
				.unwrap();
		}
		let other = MaintenanceWorkKey::new(
			MaintenanceOperation::RetentionPrune,
			[SlabId::new(12)],
			DeterministicOrigin::MaintenanceRewriteWithoutClaim,
		)
		.unwrap();
		store
			.put_maintenance_receipt(other.encode(), receipt.encode())
			.await
			.unwrap();
		let last = MaintenanceWorkKey::new(
			MaintenanceOperation::TombstoneReclaim,
			[SlabId::new(20)],
			DeterministicOrigin::MaintenanceRewriteWithoutClaim,
		)
		.unwrap();
		store
			.put_maintenance_receipt(last.encode(), receipt.encode())
			.await
			.unwrap();
		let policy = MaintenancePolicyInner::load(store, Arc::new(ManualClock::at(10_000))).await;

		let first = policy
			.report_page(None, 1, MAINTENANCE_REPORT_MAX_BYTES)
			.unwrap();
		assert_eq!(first.items.len(), 1);
		let MaintenanceReportItem::QuarantinedWork {
			operation,
			slabs: reported_slabs,
			origins,
			quarantine_fingerprint,
		} = &first.items[0]
		else {
			panic!("the canonical first item is deterministic")
		};
		assert_eq!(*operation, OP);
		assert_eq!(reported_slabs, &slabs);
		assert_eq!(origins.len(), 2, "the work identity was not split");
		assert_eq!(
			quarantine_fingerprint.to_bytes(),
			[
				0x01, 0x40, 0x39, 0x50, 0x04, 0xfd, 0xea, 0x3e, 0xb1, 0xb4, 0xa0, 0xfd, 0x04, 0xd5,
				0x81, 0x16, 0x08, 0x04, 0x3f, 0x5d, 0xe2, 0x76, 0xf9, 0xa1, 0x66, 0xb9, 0x20, 0xb8,
				0xef, 0xac, 0xa1, 0x66, 0x94,
			],
			"fingerprint v1 is deployed schema: change requires a new version"
		);

		let second = policy
			.report_page(
				first.next_cursor.as_deref(),
				1,
				MAINTENANCE_REPORT_MAX_BYTES,
			)
			.unwrap();
		assert_eq!(second.items.len(), 1);
		let second_cursor = second.next_cursor.expect("a third identity remains");

		REPORT_FINGERPRINT_CALLS.with(|calls| calls.set(0));
		REPORT_DETERMINISTIC_VISITS.with(|visits| visits.set(0));
		let third = policy
			.report_page(Some(&second_cursor), 1, MAINTENANCE_REPORT_MAX_BYTES)
			.unwrap();
		assert_eq!(third.items.len(), 1);
		assert!(third.next_cursor.is_none());
		assert_eq!(
			REPORT_FINGERPRINT_CALLS.with(std::cell::Cell::get),
			1,
			"a later page does not hash earlier work or its cursor identity"
		);
		assert_eq!(
			REPORT_DETERMINISTIC_VISITS.with(std::cell::Cell::get),
			2,
			"the map range begins at the cursor identity rather than at the first receipt"
		);
	}

	/// A cursor is a snapshot capability, not a live iterator. A report-visible
	/// reservation invalidates it, and a fresh policy incarnation refuses it even
	/// when the durable receipt generation happens to start at the same number.
	#[tokio::test]
	async fn report_cursor_is_invalidated_by_mutation_and_policy_rehydration() {
		let clock = Arc::new(ManualClock::at(10_000));
		let path = temp_index_path("report-cursor");
		let (policy, store) = enabled_policy_at(&path, clock.clone()).await;
		drop(policy);
		for slab in [1_u64, 2] {
			let key = MaintenanceWorkKey::new(
				OP,
				[SlabId::new(slab)],
				DeterministicOrigin::LsmInconsistentSourceFrames,
			)
			.unwrap();
			store
				.put_maintenance_receipt(key.encode(), quarantined_receipt("shape").encode())
				.await
				.unwrap();
		}
		let policy = MaintenancePolicyInner::load(Arc::clone(&store), clock.clone()).await;
		let page = policy
			.report_page(None, 1, MAINTENANCE_REPORT_MAX_BYTES)
			.unwrap();
		let cursor = page.next_cursor.expect("two identities require a cursor");

		let reservation = policy
			.reserve(MaintenanceOperation::RetentionPrune, &[SlabId::new(99)])
			.unwrap();
		assert_eq!(
			policy.report_page(Some(&cursor), 1, MAINTENANCE_REPORT_MAX_BYTES),
			Err(MaintenanceReportPageError::StaleCursor)
		);
		reservation.finalize();

		let fresh_page = policy
			.report_page(None, 1, MAINTENANCE_REPORT_MAX_BYTES)
			.unwrap();
		let pre_reopen_cursor = fresh_page.next_cursor.unwrap();
		drop(policy);
		let store = reopen_store(store, &path).await;
		let reopened = MaintenancePolicyInner::load(store, clock).await;
		assert_eq!(
			reopened.report_page(Some(&pre_reopen_cursor), 1, MAINTENANCE_REPORT_MAX_BYTES),
			Err(MaintenanceReportPageError::StaleCursor)
		);
	}

	/// ADR 0582 step 4a: accepting the reviewed fingerprint reserves the slab set
	/// before durable deletion. A concurrent attempt is refused before its body;
	/// once deletion and release complete, the next attempt enters and records a
	/// fresh first failure normally.
	#[tokio::test]
	async fn compare_and_clear_reserves_between_validation_and_durable_delete() {
		let clock = Arc::new(ManualClock::at(1_000_000));
		let (policy, _store) = enabled_policy_with_store(clock.clone()).await;
		let slabs = [SlabId::new(71)];
		for attempt in 1..=3_u64 {
			clock.set(attempt * 10_000_000);
			fail_deterministically(&policy, &slabs).await;
		}
		let expected = policy.quarantine_fingerprint_for_test(OP, &slabs).unwrap();
		let prepared = policy
			.prepare_requeue(OP, slabs.to_vec(), expected)
			.expect("the reviewed quarantine is reserved");

		let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
		let body_flag = Arc::clone(&entered);
		let refused = policy
			.execute(OP, &slabs, classify_io_error, rewrote, move || async move {
				body_flag.store(true, std::sync::atomic::Ordering::SeqCst);
				Err::<(), _>(det_err("must not run"))
			})
			.await;
		assert!(matches!(
			refused,
			MaintenanceAttempt::Skipped(AdmissionRefusal::AlreadyInFlight)
		));
		assert!(!entered.load(std::sync::atomic::Ordering::SeqCst));

		assert_eq!(policy.commit_requeue(prepared).await, Ok(1));
		let body_flag = Arc::clone(&entered);
		let attempted = policy
			.execute(OP, &slabs, classify_io_error, rewrote, move || async move {
				body_flag.store(true, std::sync::atomic::Ordering::SeqCst);
				Err::<(), _>(det_err("fresh failure"))
			})
			.await;
		assert!(matches!(attempted, MaintenanceAttempt::Failed(_)));
		assert!(entered.load(std::sync::atomic::Ordering::SeqCst));
		assert_eq!(receipt_for(&policy, &slabs).unwrap().attempts, 1);
	}

	#[tokio::test]
	async fn every_report_visible_policy_mutation_invalidates_a_cursor() {
		let clock = Arc::new(ManualClock::at(10_000));
		let (policy, store) = enabled_policy_with_store(clock.clone()).await;
		drop(policy);
		for slab in [1_u64, 2] {
			let key = MaintenanceWorkKey::new(
				OP,
				[SlabId::new(slab)],
				DeterministicOrigin::LsmInconsistentSourceFrames,
			)
			.unwrap();
			store
				.put_maintenance_receipt(key.encode(), quarantined_receipt("shape").encode())
				.await
				.unwrap();
		}
		let policy = MaintenancePolicyInner::load(store, clock.clone()).await;
		let cursor = || {
			policy
				.report_page(None, 1, MAINTENANCE_REPORT_MAX_BYTES)
				.unwrap()
				.next_cursor
				.expect("the fixture always has a second item")
		};
		let assert_stale = |cursor: &[u8]| {
			assert_eq!(
				policy.report_page(Some(cursor), 1, MAINTENANCE_REPORT_MAX_BYTES),
				Err(MaintenanceReportPageError::StaleCursor)
			);
		};

		let before_retryable = cursor();
		let retryable = policy
			.execute(
				MaintenanceOperation::RetentionPrune,
				&[SlabId::new(40)],
				|_| MaintenanceFailureClass::Retryable,
				rewrote,
				|| async { Err::<(), _>(race_err()) },
			)
			.await;
		assert!(matches!(retryable, MaintenanceAttempt::Failed(_)));
		assert_stale(&before_retryable);

		let before_receipt = cursor();
		fail_deterministically(&policy, &[SlabId::new(41)]).await;
		assert_stale(&before_receipt);

		let before_disable = cursor();
		policy.disable("test policy disable");
		assert_stale(&before_disable);
	}

	#[tokio::test]
	async fn a_recreated_quarantine_makes_the_reviewed_fingerprint_stale() {
		let clock = Arc::new(ManualClock::at(1_000_000));
		let (policy, _store) = enabled_policy_with_store(clock.clone()).await;
		let slabs = [SlabId::new(81)];
		for attempt in 1..=3_u64 {
			clock.set(attempt * 10_000_000);
			fail_deterministically(&policy, &slabs).await;
		}
		let reviewed = policy.quarantine_fingerprint_for_test(OP, &slabs).unwrap();
		assert_eq!(policy.requeue(OP, &slabs, reviewed).await, Ok(1));

		for attempt in 1..=3_u64 {
			clock.set(100_000_000 + attempt * 10_000_000);
			let body = policy
				.execute(OP, &slabs, classify_io_error, rewrote, || async {
					Err::<(), _>(det_err("new decision"))
				})
				.await;
			assert!(matches!(body, MaintenanceAttempt::Failed(_)));
		}
		let current = policy.quarantine_fingerprint_for_test(OP, &slabs).unwrap();
		assert_ne!(current, reviewed);
		assert_eq!(
			policy.requeue(OP, &slabs, reviewed).await,
			Err(RequeueRefusal::StaleReport {
				quarantined_origins: 1
			})
		);
		assert_eq!(
			policy.quarantine_fingerprint_for_test(OP, &slabs),
			Some(current),
			"stale compare-and-clear changed the reviewed state"
		);
	}

	#[test]
	fn unknown_fingerprint_versions_are_refused() {
		let mut bytes = [0_u8; 33];
		bytes[0] = MAINTENANCE_FINGERPRINT_VERSION + 1;
		assert_eq!(
			MaintenanceFingerprint::from_bytes(&bytes),
			Err("maintenance fingerprint version is unsupported")
		);
	}

	fn neutral<T>(_: &T) -> SuccessDisposition {
		SuccessDisposition::Neutral
	}

	fn unclaimed<T>(_: &T) -> SuccessDisposition {
		SuccessDisposition::Unclaimed
	}

	/// ADR 0098 decision 2: a clean scan cools its set for one tick, doubling per
	/// consecutive clean scan to the cap; a rewrite clears it; an unclaimed
	/// completion records nothing.
	#[tokio::test]
	async fn neutral_completion_cools_the_set_down_and_a_rewrite_clears_it() {
		let clock = Arc::new(ManualClock::at(1_000_000));
		let (policy, _store) = enabled_policy_with_store(clock.clone()).await;
		let slabs = [SlabId::new(4)];
		let scan_clean = |policy: Arc<MaintenancePolicyInner>| async move {
			let attempt = policy
				.execute(OP, &slabs, classify_io_error, neutral, || async {
					Ok::<_, Error>(())
				})
				.await;
			assert!(
				matches!(attempt, MaintenanceAttempt::Completed(())),
				"{attempt:?}"
			);
		};

		scan_clean(Arc::clone(&policy)).await;
		assert_eq!(
			policy.neutral_cooldowns_for_test(),
			vec![(
				OP,
				vec![SlabId::new(4)],
				NeutralCooldown {
					completions: 1,
					next_eligible_at_ms: 1_005_000,
				}
			)],
			"one tick of cooldown after the first clean scan"
		);
		assert_eq!(
			policy.reserve(OP, &slabs).err(),
			Some(AdmissionRefusal::NeutralCooldown),
			"the same scan is not repeated inside its cooldown"
		);
		assert_eq!(
			MaintenanceAttemptResult::skipped(&AdmissionRefusal::NeutralCooldown).label(),
			"skipped_neutral_cooldown"
		);
		assert!(
			MaintenanceAttemptResult::ALL
				.contains(&MaintenanceAttemptResult::SkippedNeutralCooldown),
			"the attempt counter renders the neutral skip"
		);
		assert!(
			policy.report().receipts.is_empty() && policy.report().retryable.is_empty(),
			"a neutral cooldown is not a receipt and not a retryable cooldown"
		);
		// Disjoint work is unaffected.
		assert_admits(&policy, &[SlabId::new(9)], "disjoint slab set");

		// Doubling per consecutive clean scan, capped at 300 s.
		let mut expected_wait_ms = 5_000_u64;
		for completions in 2..=8_u32 {
			let deadline = policy.neutral_cooldowns_for_test()[0].2.next_eligible_at_ms;
			clock.set(deadline);
			scan_clean(Arc::clone(&policy)).await;
			expected_wait_ms = (expected_wait_ms * 2).min(300_000);
			let cooldown = policy.neutral_cooldowns_for_test();
			assert_eq!(cooldown.len(), 1);
			assert_eq!(cooldown[0].2.completions, completions);
			assert_eq!(
				cooldown[0].2.next_eligible_at_ms,
				deadline + expected_wait_ms,
				"completion {completions}"
			);
		}
		assert_eq!(neutral_cooldown(7), NEUTRAL_COOLDOWN_CAP);
		assert_eq!(neutral_cooldown(1), NEUTRAL_COOLDOWN_BASE);

		// An unclaimed completion over another set records nothing about it.
		let attempt = policy
			.execute(
				OP,
				&[SlabId::new(9)],
				classify_io_error,
				unclaimed,
				|| async { Ok::<_, Error>(()) },
			)
			.await;
		assert!(matches!(attempt, MaintenanceAttempt::Completed(())));
		assert_eq!(policy.neutral_cooldowns_for_test().len(), 1);
		assert_admits(
			&policy,
			&[SlabId::new(9)],
			"an unclaimed set stays eligible",
		);

		// A rewrite clears the cooldown, and the counter with it.
		let deadline = policy.neutral_cooldowns_for_test()[0].2.next_eligible_at_ms;
		clock.set(deadline);
		let attempt = policy
			.execute(OP, &slabs, classify_io_error, rewrote, || async {
				Ok::<_, Error>(())
			})
			.await;
		assert!(matches!(attempt, MaintenanceAttempt::Completed(())));
		assert!(policy.neutral_cooldowns_for_test().is_empty());
		scan_clean(Arc::clone(&policy)).await;
		assert_eq!(
			policy.neutral_cooldowns_for_test()[0].2.completions,
			1,
			"the run of clean scans restarts after a rewrite"
		);
		assert!(policy.is_enabled());
	}

	/// ADR 0098 decision 2: a failure replaces a neutral cooldown with its own
	/// schedule, and the cooldown table drops expired entries when one is recorded.
	#[tokio::test]
	async fn a_failure_replaces_the_neutral_cooldown_and_expired_entries_are_swept() {
		let clock = Arc::new(ManualClock::at(1_000_000));
		let (policy, _store) = enabled_policy_with_store(clock.clone()).await;
		for slab in [1_u64, 2, 3] {
			let attempt = policy
				.execute(
					OP,
					&[SlabId::new(slab)],
					classify_io_error,
					neutral,
					|| async { Ok::<_, Error>(()) },
				)
				.await;
			assert!(matches!(attempt, MaintenanceAttempt::Completed(())));
		}
		assert_eq!(policy.neutral_cooldowns_for_test().len(), 3);

		clock.set(1_005_000);
		fail_deterministically(&policy, &[SlabId::new(1)]).await;
		let remaining = policy.neutral_cooldowns_for_test();
		assert!(
			remaining
				.iter()
				.all(|(_, slabs, _)| slabs != &[SlabId::new(1)]),
			"the failed set is governed by its receipt now: {remaining:?}"
		);
		assert_eq!(
			policy.reserve(OP, &[SlabId::new(1)]).err(),
			Some(AdmissionRefusal::NotYetEligible),
			"backoff, not a neutral cooldown"
		);

		// Every remaining entry became eligible at 1,005,000 and nobody rescanned
		// it for a whole cap period after that; the next record sweeps them.
		clock.set(1_005_000 + NEUTRAL_COOLDOWN_CAP.as_millis() as u64);
		let attempt = policy
			.execute(
				OP,
				&[SlabId::new(7)],
				classify_io_error,
				neutral,
				|| async { Ok::<_, Error>(()) },
			)
			.await;
		assert!(matches!(attempt, MaintenanceAttempt::Completed(())));
		let after = policy.neutral_cooldowns_for_test();
		assert_eq!(after.len(), 1, "expired entries are gone: {after:?}");
		assert_eq!(after[0].1, vec![SlabId::new(7)]);
	}

	/// ADR 0098 decision 3: a receipt inside its backoff budget reports as
	/// backing off, never as quarantined, and a one-item page walks one work
	/// identity's quarantined and backing-off views without losing either.
	#[tokio::test]
	async fn backing_off_receipts_report_as_backing_off_and_page_after_quarantine() {
		let clock = Arc::new(ManualClock::at(10_000));
		let (policy, store) = enabled_policy_with_store(clock).await;
		drop(policy);
		let slabs = [SlabId::new(4), SlabId::new(9)];
		let backing_off = DeterministicReceipt::after_failure(None, 1_000, "shape").unwrap();
		assert!(backing_off.next_eligible_at_ms().is_some());
		let quarantined = quarantined_receipt("shape");
		// One identity with both views.
		for (origin, receipt) in [
			(
				DeterministicOrigin::LsmInconsistentSourceFrames,
				&quarantined,
			),
			(
				DeterministicOrigin::LsmSourceFrameMissingPlannedSurvivor,
				&backing_off,
			),
		] {
			let key = MaintenanceWorkKey::new(OP, slabs, origin).unwrap();
			store
				.put_maintenance_receipt(key.encode(), receipt.encode())
				.await
				.unwrap();
		}
		// An identity that is only backing off, for an operation that sorts first.
		let only_backing_off = MaintenanceWorkKey::new(
			MaintenanceOperation::RetentionPrune,
			[SlabId::new(12)],
			DeterministicOrigin::MaintenanceRewriteWithoutClaim,
		)
		.unwrap();
		store
			.put_maintenance_receipt(only_backing_off.encode(), backing_off.encode())
			.await
			.unwrap();
		let policy = MaintenancePolicyInner::load(store, Arc::new(ManualClock::at(10_000))).await;

		let whole = policy
			.report_page(None, 10, MAINTENANCE_REPORT_MAX_BYTES)
			.unwrap();
		let describe = |item: &MaintenanceReportItem| match item {
			MaintenanceReportItem::QuarantinedWork {
				operation,
				slabs,
				origins,
				..
			} => format!(
				"quarantined {operation:?} {slabs:?} origins={}",
				origins.len()
			),
			MaintenanceReportItem::BackingOff {
				operation,
				slabs,
				origins,
			} => {
				assert!(
					origins
						.iter()
						.all(|origin| origin.next_eligible_at_ms.is_some())
				);
				format!(
					"backing_off {operation:?} {slabs:?} origins={}",
					origins.len()
				)
			}
			other => format!("{other:?}"),
		};
		let expected = vec![
			format!("quarantined {OP:?} {slabs:?} origins=1"),
			format!("backing_off {OP:?} {slabs:?} origins=1"),
			format!(
				"backing_off {:?} {:?} origins=1",
				MaintenanceOperation::RetentionPrune,
				[SlabId::new(12)]
			),
		];
		assert_eq!(
			whole.items.iter().map(describe).collect::<Vec<_>>(),
			expected,
			"quarantined items first, then every backing-off identity in key order"
		);
		assert!(whole.next_cursor.is_none());

		// One item per page walks the same sequence with no loss and no repeat.
		let mut walked = Vec::new();
		let mut cursor: Option<Vec<u8>> = None;
		loop {
			let page = policy
				.report_page(cursor.as_deref(), 1, MAINTENANCE_REPORT_MAX_BYTES)
				.unwrap();
			assert_eq!(page.items.len(), 1);
			walked.push(describe(&page.items[0]));
			match page.next_cursor {
				Some(next) => cursor = Some(next),
				None => break,
			}
		}
		assert_eq!(walked, expected);
	}
}

/// ADR 0097 decision 4: the identity a failure is tracked against.
///
/// The OPERATION, the canonical sorted touched slabs, and the failure ORIGIN. The
/// slab set alone is too coarse - two unrelated deterministic defects over the
/// same inputs would share one attempt budget, and the first to reach the bound
/// would quarantine work that had only failed once for its own reason, and one
/// job's success would clear another's receipt. `job_id` is too fine: it is
/// derived per pick, so it would reset the count every tick and the bound would
/// never bite, which is the loop this record exists to stop.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct MaintenanceWorkKey {
	operation: MaintenanceOperation,
	slabs: Vec<SlabId>,
	origin: DeterministicOrigin,
}

/// Why a persisted receipt could not be read back.
///
/// Decoding returns this rather than `None` deliberately. A malformed receipt is
/// not "no history": treating it as absent would silently reset the attempt bound
/// on every restart, which is exactly the unbounded loop this record exists to
/// stop, and it would do so in the one situation - a crash cycle - where the bound
/// matters most.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ReceiptDecodeError {
	#[error("maintenance receipt key is empty")]
	Empty,
	#[error("maintenance receipt key names no slabs")]
	NoSlabs,
	#[error("maintenance receipt key has a truncated slab id")]
	TruncatedSlabId,
	#[error("maintenance receipt key has an unknown origin code {0}")]
	UnknownOrigin(u16),
	#[error("maintenance receipt key has an unknown operation code {0}")]
	UnknownOperation(u16),
	#[error("maintenance receipt key slab ids are not strictly sorted")]
	UnsortedSlabs,
	#[error("maintenance receipt value is truncated or malformed")]
	TruncatedReceipt,
	#[error("maintenance receipt attempt count {0} is outside the schedule")]
	AttemptsOutOfRange(u32),
	#[error("maintenance receipt deadline does not match the schedule for its attempt")]
	DeadlineDoesNotMatchSchedule,
	#[error("maintenance receipt time arithmetic overflowed")]
	TimeOverflow,
	#[error("maintenance receipt version {0} is not supported")]
	UnknownReceiptVersion(u8),
	#[error("maintenance receipt detail is {0} bytes, over the bound")]
	DetailTooLong(usize),
	#[error("maintenance receipt detail is not valid UTF-8")]
	DetailNotUtf8,
	#[error("an already-quarantined maintenance receipt cannot record another attempt")]
	QuarantinedReceiptAdvanced,
	#[error("maintenance receipt key names {0} slabs, over the bound")]
	TooManySlabs(usize),
}

impl MaintenanceWorkKey {
	/// Rejects an empty slab set: a key naming no work is one this type's own
	/// decoder refuses, so constructing it would write a receipt that fails closed on
	/// the next startup.
	pub(crate) fn new(
		operation: MaintenanceOperation,
		slabs: impl IntoIterator<Item = SlabId>,
		origin: DeterministicOrigin,
	) -> Result<Self, ReceiptDecodeError> {
		let mut slabs = slabs.into_iter().collect::<Vec<_>>();
		slabs.sort_unstable();
		slabs.dedup();
		if slabs.is_empty() {
			return Err(ReceiptDecodeError::NoSlabs);
		}
		if slabs.len() > MAX_RECEIPT_KEY_SLABS {
			return Err(ReceiptDecodeError::TooManySlabs(slabs.len()));
		}
		Ok(Self {
			operation,
			slabs,
			origin,
		})
	}

	pub(crate) fn operation(&self) -> MaintenanceOperation {
		self.operation
	}

	pub(crate) fn slabs(&self) -> &[SlabId] {
		&self.slabs
	}

	pub(crate) fn origin(&self) -> DeterministicOrigin {
		self.origin
	}

	pub(crate) fn reason(&self) -> DeterministicFailureReason {
		self.origin.reason()
	}

	/// Stable byte encoding: operation code big-endian, origin code big-endian,
	/// then the slab ids big-endian so key order matches slab order.
	pub(crate) fn encode(&self) -> Vec<u8> {
		let mut out = Vec::with_capacity(4 + self.slabs.len() * 8);
		out.extend_from_slice(&self.operation.code().to_be_bytes());
		out.extend_from_slice(&self.origin.code().to_be_bytes());
		for slab in &self.slabs {
			out.extend_from_slice(&slab.get().to_be_bytes());
		}
		out
	}

	/// Fail closed: every rejection is reported so recovery can refuse to run
	/// maintenance rather than quietly forget an attempt budget.
	pub(crate) fn decode(bytes: &[u8]) -> Result<Self, ReceiptDecodeError> {
		if bytes.len() < 4 {
			return Err(ReceiptDecodeError::Empty);
		}
		let (header, rest) = bytes.split_at(4);
		let code = u16::from_be_bytes([header[0], header[1]]);
		let operation = MaintenanceOperation::from_code(code)
			.ok_or(ReceiptDecodeError::UnknownOperation(code))?;
		let code = u16::from_be_bytes([header[2], header[3]]);
		let origin =
			DeterministicOrigin::from_code(code).ok_or(ReceiptDecodeError::UnknownOrigin(code))?;
		if rest.is_empty() {
			return Err(ReceiptDecodeError::NoSlabs);
		}
		if rest.len() % 8 != 0 {
			return Err(ReceiptDecodeError::TruncatedSlabId);
		}
		if rest.len() / 8 > MAX_RECEIPT_KEY_SLABS {
			return Err(ReceiptDecodeError::TooManySlabs(rest.len() / 8));
		}
		let slabs = rest
			.chunks_exact(8)
			.map(|chunk| {
				SlabId::new(u64::from_be_bytes(
					chunk.try_into().expect("chunk is 8 bytes"),
				))
			})
			.collect::<Vec<_>>();
		if slabs.windows(2).any(|pair| pair[0] >= pair[1]) {
			return Err(ReceiptDecodeError::UnsortedSlabs);
		}
		Ok(Self {
			operation,
			slabs,
			origin,
		})
	}
}

/// How long a deterministic failure waits before its next attempt, and when it
/// stops waiting and quarantines instead.
///
/// Three full attempts, not four: each one rebuilds the whole working set, and
/// three recurrences of the same shape-dependent failure over the same inputs is
/// enough evidence. The delays are long relative to the 100 ms picker tick, which
/// is the scheduling RESOLUTION rather than a cap on the backoff.
pub const DETERMINISTIC_BACKOFF: [Duration; 2] = [Duration::from_secs(5), Duration::from_secs(30)];
pub const DETERMINISTIC_ATTEMPTS_BEFORE_QUARANTINE: u32 = 3;

/// A retryable failure is a race or a transient condition: it may well succeed on
/// the next tick, so the cooldown only stops a busy loser from spinning. It never
/// counts toward quarantine.
pub const RETRYABLE_COOLDOWN_BASE: Duration = Duration::from_millis(250);
pub const RETRYABLE_COOLDOWN_CAP: Duration = Duration::from_secs(5);

/// The wait before the next attempt after `failures` consecutive deterministic
/// failures. `None` means the work has exhausted its budget and quarantines.
pub fn deterministic_backoff(failures: u32) -> Option<Duration> {
	if failures >= DETERMINISTIC_ATTEMPTS_BEFORE_QUARANTINE {
		return None;
	}
	DETERMINISTIC_BACKOFF
		.get(failures.saturating_sub(1) as usize)
		.copied()
}

/// Exponential from 250 ms, doubling, capped at 5 s.
pub fn retryable_cooldown(failures: u32) -> Duration {
	let shift = failures.saturating_sub(1).min(16);
	RETRYABLE_COOLDOWN_BASE
		.saturating_mul(1u32 << shift)
		.min(RETRYABLE_COOLDOWN_CAP)
}

/// ADR 0098 decision 2: a scan that found nothing to rewrite is repeated no
/// sooner than one sweeper tick later, and each consecutive clean scan of the
/// same set doubles the wait up to the cap. The base is the sweepers' tick so the
/// first cooldown costs exactly one skipped tick; the cap bounds how long a slab
/// that became reclaimable waits behind its last clean scan.
pub const NEUTRAL_COOLDOWN_BASE: Duration = Duration::from_secs(5);
pub const NEUTRAL_COOLDOWN_CAP: Duration = Duration::from_secs(300);

/// Exponential from 5 s, doubling per consecutive neutral completion, capped at
/// 300 s.
pub fn neutral_cooldown(completions: u32) -> Duration {
	let shift = completions.saturating_sub(1).min(16);
	NEUTRAL_COOLDOWN_BASE
		.saturating_mul(1u32 << shift)
		.min(NEUTRAL_COOLDOWN_CAP)
}

/// ADR 0097 decision 4: the clock admission and recording share.
///
/// Deliberately NOT the tick's `now`. That timestamp is sampled before the whole
/// compaction call, so a working-set build that takes longer than the backoff would
/// consume its own delay and be eligible again the instant it failed - which is the
/// unbounded loop wearing a schedule.
///
/// Fallible on purpose: `SystemClock` reports an unavailable host clock as `0`, and
/// a zero here would mark every failure eligible immediately. A policy that cannot
/// read the time cannot schedule, so it stops admitting instead.
pub(crate) trait MaintenanceClock: Send + Sync {
	fn now_ms(&self) -> Option<u64>;
}

pub(crate) struct SystemMaintenanceClock;

impl MaintenanceClock for SystemMaintenanceClock {
	fn now_ms(&self) -> Option<u64> {
		std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.ok()
			.and_then(|since| u64::try_from(since.as_millis()).ok())
	}
}

/// Test clock: schedule and reopen behaviour is pinned against this rather than
/// against the runtime's scheduler, so the assertions are about the policy.
#[cfg(test)]
pub(crate) struct ManualClock(std::sync::atomic::AtomicU64);

#[cfg(test)]
impl ManualClock {
	pub(crate) fn at(ms: u64) -> Self {
		Self(std::sync::atomic::AtomicU64::new(ms))
	}

	pub(crate) fn advance(&self, by: Duration) {
		self.0
			.fetch_add(by.as_millis() as u64, std::sync::atomic::Ordering::SeqCst);
	}

	pub(crate) fn set(&self, ms: u64) {
		self.0.store(ms, std::sync::atomic::Ordering::SeqCst);
	}
}

#[cfg(test)]
impl MaintenanceClock for ManualClock {
	fn now_ms(&self) -> Option<u64> {
		Some(self.0.load(std::sync::atomic::Ordering::SeqCst))
	}
}

/// Maximum bytes of failure detail carried in a receipt.
///
/// Bounded because this is durable and read on every startup: an unbounded message
/// would let one pathological failure inflate recovery. Metrics never use this -
/// they carry only the bounded category label - but an operator reading the
/// receipt accessor needs to know what actually went wrong without correlating
/// logs that may have rotated.
pub(crate) const MAX_RECEIPT_DETAIL_BYTES: usize = 256;

/// Version byte leading every receipt value.
///
/// The deadline is validated EXACTLY against the schedule, so a change to
/// [`DETERMINISTIC_BACKOFF`] would make existing history undecodable rather than
/// silently reinterpreted. That is deliberate: such a change must bump this
/// version and migrate, not quietly reinterpret what earlier binaries wrote.
pub(crate) const RECEIPT_VALUE_VERSION: u8 = 1;

/// Most slabs one receipt key may name. A plan is a bounded thing; a key past this
/// is not a plan, and refusing it at construction means the executor fails closed
/// at the write rather than the next startup failing closed at the read.
pub(crate) const MAX_RECEIPT_KEY_SLABS: usize = 4096;

/// Exact maximum length of a v1 key: the four-byte header (operation code, then
/// origin code) plus one u64 per slab.
pub(crate) const MAX_RECEIPT_KEY_BYTES: usize = 4 + 8 * MAX_RECEIPT_KEY_SLABS;

/// Exact maximum length of a v1 value: version, attempts, failure time, deadline
/// flag and deadline, detail length, and the detail itself at its bound. Pinned by
/// a test against the encoder, so a layout change cannot leave it stale.
pub(crate) const MAX_RECEIPT_VALUE_BYTES: usize = 1 + 4 + 8 + 1 + 8 + 2 + MAX_RECEIPT_DETAIL_BYTES;

/// Most receipts recovery will read. Real tables hold a handful - one per failed
/// slab set per origin - so a table past this is corrupt or runaway, and either
/// way is history this node should not trust.
pub(crate) const MAX_RECEIPTS_LOADED: usize = 1 << 16;

/// Most bytes recovery will copy out of the table in total, independent of the
/// per-row bounds.
pub(crate) const MAX_RECEIPT_LOAD_BYTES: usize = 64 << 20;

pub(crate) fn receipt_load_bounds() -> crate::storage::index::ReceiptLoadBounds {
	crate::storage::index::ReceiptLoadBounds {
		max_rows: MAX_RECEIPTS_LOADED,
		max_key_bytes: MAX_RECEIPT_KEY_BYTES,
		max_value_bytes: MAX_RECEIPT_VALUE_BYTES,
		max_total_bytes: MAX_RECEIPT_LOAD_BYTES,
	}
}

/// What one work key's deterministic history says.
///
/// Times are ABSOLUTE milliseconds since the epoch, computed from a clock sampled
/// AFTER the outcome, so the wait is measured from when the attempt actually
/// failed rather than from when its tick began.
///
/// A clock that steps BACKWARD conservatively extends the wait, because
/// eligibility is `now >= next_eligible_at_ms` and a smaller `now` cannot satisfy
/// it. A forward jump may shorten it - accepted, because the persisted attempt
/// count still bounds the work regardless of what the clock does.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DeterministicReceipt {
	attempts: u32,
	last_failure_ms: u64,
	/// `None` once the attempt budget is spent: quarantined, and no time makes it
	/// eligible again. Only the explicit requeue clears it.
	next_eligible_at_ms: Option<u64>,
	detail: String,
}

impl DeterministicReceipt {
	pub(crate) fn attempts(&self) -> u32 {
		self.attempts
	}

	#[cfg(test)]
	pub(crate) fn last_failure_ms(&self) -> u64 {
		self.last_failure_ms
	}

	#[cfg(test)]
	pub(crate) fn next_eligible_at_ms(&self) -> Option<u64> {
		self.next_eligible_at_ms
	}

	pub(crate) fn detail(&self) -> &str {
		&self.detail
	}

	pub(crate) fn is_quarantined(&self) -> bool {
		self.next_eligible_at_ms.is_none()
	}

	pub(crate) fn is_eligible_at(&self, now_ms: u64) -> bool {
		match self.next_eligible_at_ms {
			None => false,
			Some(at) => now_ms >= at,
		}
	}

	/// The deadline this schedule requires for `attempts`, or `None` when the
	/// budget is spent. `Err` when the arithmetic would overflow - which must fail
	/// closed rather than wrap into a misleadingly valid schedule.
	fn scheduled_deadline(
		attempts: u32,
		failure_ms: u64,
	) -> Result<Option<u64>, ReceiptDecodeError> {
		match deterministic_backoff(attempts) {
			None => Ok(None),
			Some(wait) => {
				let wait_ms = u64::try_from(wait.as_millis())
					.map_err(|_| ReceiptDecodeError::TimeOverflow)?;
				failure_ms
					.checked_add(wait_ms)
					.map(Some)
					.ok_or(ReceiptDecodeError::TimeOverflow)
			}
		}
	}

	/// Record one more deterministic failure at `failure_ms`.
	///
	/// Refuses to advance an already-quarantined receipt: quarantine is absorbing,
	/// and a transition out of it can only come from the explicit requeue.
	pub(crate) fn after_failure(
		previous: Option<&Self>,
		failure_ms: u64,
		detail: &str,
	) -> Result<Self, ReceiptDecodeError> {
		if previous.is_some_and(|receipt| receipt.is_quarantined()) {
			return Err(ReceiptDecodeError::QuarantinedReceiptAdvanced);
		}
		let attempts = previous
			.map_or(0, |receipt| receipt.attempts)
			.checked_add(1)
			.ok_or(ReceiptDecodeError::TimeOverflow)?;
		if attempts > DETERMINISTIC_ATTEMPTS_BEFORE_QUARANTINE {
			return Err(ReceiptDecodeError::AttemptsOutOfRange(attempts));
		}
		Ok(Self {
			attempts,
			last_failure_ms: failure_ms,
			next_eligible_at_ms: Self::scheduled_deadline(attempts, failure_ms)?,
			detail: truncate_detail(detail),
		})
	}

	pub(crate) fn encode(&self) -> Vec<u8> {
		let detail = self.detail.as_bytes();
		let mut out = Vec::with_capacity(24 + detail.len());
		out.push(RECEIPT_VALUE_VERSION);
		out.extend_from_slice(&self.attempts.to_be_bytes());
		out.extend_from_slice(&self.last_failure_ms.to_be_bytes());
		match self.next_eligible_at_ms {
			Some(at) => {
				out.push(1);
				out.extend_from_slice(&at.to_be_bytes());
			}
			None => out.push(0),
		}
		out.extend_from_slice(&(detail.len() as u16).to_be_bytes());
		out.extend_from_slice(detail);
		out
	}

	/// Decode and VALIDATE. A structurally intact receipt whose deadline does not
	/// match the schedule its attempt count implies is refused: accepting it would
	/// permit a fourth working-set build from a value that merely looked plausible.
	pub(crate) fn decode(bytes: &[u8]) -> Result<Self, ReceiptDecodeError> {
		let (&version, rest) = bytes
			.split_first()
			.ok_or(ReceiptDecodeError::TruncatedReceipt)?;
		if version != RECEIPT_VALUE_VERSION {
			return Err(ReceiptDecodeError::UnknownReceiptVersion(version));
		}
		if rest.len() < 13 {
			return Err(ReceiptDecodeError::TruncatedReceipt);
		}
		let attempts = u32::from_be_bytes(rest[0..4].try_into().expect("4 bytes"));
		let last_failure_ms = u64::from_be_bytes(rest[4..12].try_into().expect("8 bytes"));
		let (next_eligible_at_ms, after_deadline) = match rest[12] {
			0 => (None, 13),
			1 => {
				if rest.len() < 21 {
					return Err(ReceiptDecodeError::TruncatedReceipt);
				}
				(
					Some(u64::from_be_bytes(
						rest[13..21].try_into().expect("8 bytes"),
					)),
					21,
				)
			}
			_ => return Err(ReceiptDecodeError::TruncatedReceipt),
		};
		if attempts == 0 || attempts > DETERMINISTIC_ATTEMPTS_BEFORE_QUARANTINE {
			return Err(ReceiptDecodeError::AttemptsOutOfRange(attempts));
		}
		// EXACT, not merely "later": the deadline must be the one this schedule
		// produces for this attempt count.
		if next_eligible_at_ms != Self::scheduled_deadline(attempts, last_failure_ms)? {
			return Err(ReceiptDecodeError::DeadlineDoesNotMatchSchedule);
		}
		if rest.len() < after_deadline + 2 {
			return Err(ReceiptDecodeError::TruncatedReceipt);
		}
		let detail_len = u16::from_be_bytes(
			rest[after_deadline..after_deadline + 2]
				.try_into()
				.expect("2 bytes"),
		) as usize;
		if detail_len > MAX_RECEIPT_DETAIL_BYTES {
			return Err(ReceiptDecodeError::DetailTooLong(detail_len));
		}
		let detail_start = after_deadline + 2;
		if rest.len() != detail_start + detail_len {
			return Err(ReceiptDecodeError::TruncatedReceipt);
		}
		let detail = std::str::from_utf8(&rest[detail_start..detail_start + detail_len])
			.map_err(|_| ReceiptDecodeError::DetailNotUtf8)?
			.to_owned();
		Ok(Self {
			attempts,
			last_failure_ms,
			next_eligible_at_ms,
			detail,
		})
	}
}

/// Truncate on a char boundary so the stored detail is always valid UTF-8.
fn truncate_detail(detail: &str) -> String {
	truncate_utf8(detail, MAX_RECEIPT_DETAIL_BYTES)
}

fn truncate_utf8(text: &str, max_bytes: usize) -> String {
	if text.len() <= max_bytes {
		return text.to_owned();
	}
	let mut end = max_bytes;
	while end > 0 && !text.is_char_boundary(end) {
		end -= 1;
	}
	text[..end].to_owned()
}

/// Cap on any diagnostic the policy stores or logs. Diagnostics describe raw
/// receipts, which can be arbitrarily large when corrupt, so they are built from
/// bounded summaries and capped again as a whole.
const MAX_DIAGNOSTIC_BYTES: usize = 512;

/// Quarantined receipts logged at hydration. The rest are in the accessor.
const MAX_QUARANTINE_LOG_LINES: usize = 16;

/// Length plus a short hex prefix - never the whole buffer.
fn bytes_summary(bytes: &[u8]) -> String {
	const PREFIX: usize = 16;
	let prefix = bytes
		.iter()
		.take(PREFIX)
		.map(|byte| format!("{byte:02x}"))
		.collect::<String>();
	if bytes.len() > PREFIX {
		format!("{} bytes, prefix {prefix}..", bytes.len())
	} else {
		format!("{} bytes, {prefix}", bytes.len())
	}
}

/// Count plus the first few ids - never every slab of a wide plan.
fn slabs_summary(slabs: &[SlabId]) -> String {
	const SHOWN: usize = 4;
	let shown = slabs
		.iter()
		.take(SHOWN)
		.map(|slab| slab.get().to_string())
		.collect::<Vec<_>>()
		.join(", ");
	if slabs.len() > SHOWN {
		format!("{} slabs, first [{shown}, ..]", slabs.len())
	} else {
		format!("{} slabs, [{shown}]", slabs.len())
	}
}

/// ADR 0097 decision 4: the one piece of state every maintenance decision reads.
///
/// Shared as an `Arc` so the synchronous picker, the reservation guard, the async
/// recorder and the receipt accessor all consult the SAME admission truth. Two
/// copies would let the picker admit work the recorder had already quarantined.
///
/// The mutex is `std::sync::Mutex`, not an async one, for a specific reason: the
/// reservation guard's fail-closed `Drop` has to run in a destructor, where there
/// is no async context to await in. Nothing awaits while holding it - execution
/// and redb I/O both happen with the lock released.
pub(crate) struct MaintenancePolicyInner {
	state: std::sync::Mutex<PolicyState>,
	clock: Arc<dyn MaintenanceClock>,
	report_incarnation: [u8; 16],
	report_generation: AtomicU64,
}

enum PolicyState {
	Enabled {
		store: Arc<PersistentLocalIndex>,
		tracker: MaintenanceTracker,
	},
	/// Serving continues; maintenance does not. Reached when durable history is
	/// unavailable, unreadable, undecodable, cannot be written, or when an attempt
	/// was abandoned before its outcome reached disk.
	Disabled { diagnostic: String },
}

#[derive(Default)]
struct MaintenanceTracker {
	deterministic: BTreeMap<MaintenanceWorkKey, DeterministicReceipt>,
	/// Keyed by operation and canonical slab set: a race has no origin, but it does
	/// have a job, and one job's run of races is not another's.
	retryable: BTreeMap<MaintenanceOperation, BTreeMap<Vec<SlabId>, RetryableCooldown>>,
	/// Canonical slab sets with an attempt in flight. Prevents two concurrent ticks
	/// both passing eligibility and both building a working set - which would make
	/// the three-attempt bound a bound on RECORDED attempts rather than on builds.
	in_flight: BTreeMap<Vec<SlabId>, MaintenanceOperation>,
	/// ADR 0098 decision 2: canonical slab sets this operation scanned and found
	/// clean, and when each may be scanned again. Ephemeral like `retryable`: a
	/// restart forgets it and scans once more. Never evidence about receipts.
	neutral: BTreeMap<MaintenanceOperation, BTreeMap<Vec<SlabId>, NeutralCooldown>>,
}

#[derive(Clone, Copy, Debug)]
struct RetryableCooldown {
	failures: u32,
	next_eligible_at_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct NeutralCooldown {
	pub(crate) completions: u32,
	pub(crate) next_eligible_at_ms: u64,
}

/// Why a plan was not admitted. Distinguished so the executor can report a skip
/// without implying a failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AdmissionRefusal {
	/// Maintenance is disabled entirely.
	PolicyDisabled(String),
	/// This exact work is already being attempted.
	AlreadyInFlight,
	/// Waiting out a cooldown or a backoff.
	NotYetEligible,
	/// ADR 0098 decision 2: the last attempt scanned this set and found nothing;
	/// the scan is not repeated until its neutral cooldown passes.
	NeutralCooldown,
	/// The attempt budget is spent; only an explicit requeue clears it.
	Quarantined,
	/// The clock could not be read, so no schedule can be evaluated.
	ClockUnavailable,
	/// A plan naming no slabs is not work, matching `MaintenanceWorkKey::new`.
	EmptyWorkSet,
}

impl MaintenancePolicyInner {
	pub(crate) fn disabled(diagnostic: impl Into<String>) -> Arc<Self> {
		Self::disabled_with(diagnostic, Arc::new(SystemMaintenanceClock))
	}

	fn disabled_with(diagnostic: impl Into<String>, clock: Arc<dyn MaintenanceClock>) -> Arc<Self> {
		let diagnostic = truncate_utf8(&diagnostic.into(), MAX_DIAGNOSTIC_BYTES);
		tracing::error!(
			diagnostic = %diagnostic,
			"maintenance admission disabled at open; serving continues"
		);
		let policy = Arc::new(Self {
			state: std::sync::Mutex::new(PolicyState::Disabled { diagnostic }),
			clock,
			report_incarnation: *Uuid::new_v4().as_bytes(),
			report_generation: AtomicU64::new(0),
		});
		policy.publish_gauges();
		policy
	}

	/// Recover admission truth from the durable record.
	///
	/// `Enabled` is published only after the WHOLE record has been read and every
	/// pair decoded into a map built off to the side. Receipts are never inserted
	/// into live state one at a time: were initialization ever to run concurrently
	/// with a tick, an incrementally hydrated policy would admit work whose
	/// quarantine simply had not been loaded yet. A partial history is not a
	/// shorter history; it is no history, and no history means no maintenance.
	///
	/// `None` for the index never reaches here: the manager maps it to `disabled`
	/// with its own diagnostic, so the absence of an index is never read as an
	/// empty record.
	pub(crate) async fn load(
		store: Arc<PersistentLocalIndex>,
		clock: Arc<dyn MaintenanceClock>,
	) -> Arc<Self> {
		if !store.receipts_available() {
			return Self::disabled_with(
				"maintenance receipt history was lost: the table is missing from an \
				 initialized index",
				clock,
			);
		}
		// Bounded while copying: a runaway or corrupt table fails closed here,
		// before it can exhaust memory and deny the policy its chance to refuse.
		let pairs = match store.load_maintenance_receipts(receipt_load_bounds()).await {
			Ok(pairs) => pairs,
			Err(error) => {
				return Self::disabled_with(
					format!("maintenance receipts could not be read: {error}"),
					clock,
				);
			}
		};
		let mut recovered = BTreeMap::new();
		for (key, value) in pairs {
			// Diagnostics summarize raw bytes and slab sets; a corrupt receipt can be
			// as large as the load bound allows, and a diagnostic must not be.
			let key = match MaintenanceWorkKey::decode(&key) {
				Ok(key) => key,
				Err(error) => {
					return Self::disabled_with(
						format!(
							"maintenance receipt key ({}) is undecodable: {error}",
							bytes_summary(&key)
						),
						clock,
					);
				}
			};
			let receipt = match DeterministicReceipt::decode(&value) {
				Ok(receipt) => receipt,
				Err(error) => {
					return Self::disabled_with(
						format!(
							"maintenance receipt for {} {:?} over {} (value {}) is undecodable: {error}",
							key.operation().label(),
							key.origin(),
							slabs_summary(key.slabs()),
							bytes_summary(&value)
						),
						clock,
					);
				}
			};
			// The key encoding is injective and redb keys are unique, so a collision
			// here means the decoder is not the inverse of the encoder. Fail closed
			// rather than let one receipt silently overwrite another's budget.
			let summary = format!(
				"{} {:?} over {}",
				key.operation().label(),
				key.origin(),
				slabs_summary(key.slabs())
			);
			if recovered.insert(key, receipt).is_some() {
				return Self::disabled_with(
					format!("two maintenance receipts decode to one key: {summary}"),
					clock,
				);
			}
		}
		let policy = Self::enabled(store, recovered, clock);
		policy.publish_gauges();
		let report = policy.report();
		for receipt in report.quarantined().take(MAX_QUARANTINE_LOG_LINES) {
			tracing::warn!(
				operation = receipt.operation.label(),
				slabs = %slabs_summary(&receipt.slabs),
				origin = ?receipt.origin,
				reason = receipt.reason.label(),
				detail = %receipt.detail,
				"maintenance work is quarantined; requeue clears it"
			);
		}
		tracing::info!(
			receipts = report.receipts.len(),
			quarantined = report.quarantined().count(),
			"maintenance admission enabled from durable receipts"
		);
		policy
	}

	pub(crate) fn enabled(
		store: Arc<PersistentLocalIndex>,
		deterministic: BTreeMap<MaintenanceWorkKey, DeterministicReceipt>,
		clock: Arc<dyn MaintenanceClock>,
	) -> Arc<Self> {
		Arc::new(Self {
			state: std::sync::Mutex::new(PolicyState::Enabled {
				store,
				tracker: MaintenanceTracker {
					deterministic,
					..MaintenanceTracker::default()
				},
			}),
			clock,
			report_incarnation: *Uuid::new_v4().as_bytes(),
			report_generation: AtomicU64::new(0),
		})
	}

	fn bump_report_generation(&self) {
		self.report_generation.fetch_add(1, Ordering::AcqRel);
	}

	/// A poisoned mutex means a panic left this state partially mutated. Recovering
	/// and carrying on could admit work from a half-updated tracker, so poison is
	/// itself a reason to stop admitting.
	fn lock(&self) -> std::sync::MutexGuard<'_, PolicyState> {
		match self.state.lock() {
			Ok(guard) => guard,
			Err(poisoned) => {
				let mut guard = poisoned.into_inner();
				if matches!(*guard, PolicyState::Enabled { .. }) {
					tracing::error!("maintenance policy mutex poisoned; disabling admission");
					*guard = PolicyState::Disabled {
						diagnostic: "policy state mutex poisoned".to_owned(),
					};
					self.bump_report_generation();
				}
				guard
			}
		}
	}

	#[cfg(test)]
	pub(crate) fn is_enabled(&self) -> bool {
		matches!(*self.lock(), PolicyState::Enabled { .. })
	}

	#[cfg(test)]
	pub(crate) fn diagnostic(&self) -> Option<String> {
		match &*self.lock() {
			PolicyState::Disabled { diagnostic } => Some(diagnostic.clone()),
			PolicyState::Enabled { .. } => None,
		}
	}

	/// Fail closed. Every path that cannot prove the durable record is intact ends
	/// here, and nothing re-enables it but a restart or an explicit repair.
	pub(crate) fn disable(&self, diagnostic: impl Into<String>) {
		let diagnostic = diagnostic.into();
		let mut state = self.lock();
		if let PolicyState::Enabled { .. } = &*state {
			tracing::error!(
				diagnostic = %diagnostic,
				"maintenance admission disabled; serving continues"
			);
			*state = PolicyState::Disabled { diagnostic };
			self.bump_report_generation();
			drop(state);
			self.publish_gauges();
		}
	}

	pub(crate) fn now_ms(&self) -> Option<u64> {
		self.clock.now_ms()
	}

	/// Whether this operation over this slab set is admissible AT `now_ms`,
	/// without reserving it.
	///
	/// Used as the picker's eligibility predicate, which may inspect several
	/// candidates before choosing one - so this deliberately does NOT reserve.
	/// Reservation happens once, for the chosen plan, under a re-checked lock.
	pub(crate) fn is_eligible(
		&self,
		operation: MaintenanceOperation,
		slabs: &[SlabId],
		now_ms: u64,
	) -> bool {
		let state = self.lock();
		let PolicyState::Enabled { tracker, .. } = &*state else {
			return false;
		};
		tracker.is_eligible(operation, slabs, now_ms)
	}
}

impl MaintenanceTracker {
	fn canonical(slabs: &[SlabId]) -> Vec<SlabId> {
		let mut canonical = slabs.to_vec();
		canonical.sort_unstable();
		canonical.dedup();
		canonical
	}

	fn is_eligible(&self, operation: MaintenanceOperation, slabs: &[SlabId], now_ms: u64) -> bool {
		self.refusal(operation, &Self::canonical(slabs), now_ms)
			.is_none()
	}

	fn retryable_for(
		&self,
		operation: MaintenanceOperation,
		canonical: &[SlabId],
	) -> Option<&RetryableCooldown> {
		self.retryable.get(&operation)?.get(canonical)
	}

	fn clear_retryable(&mut self, operation: MaintenanceOperation, canonical: &[SlabId]) -> bool {
		let mut removed = false;
		if let Some(per_set) = self.retryable.get_mut(&operation) {
			removed = per_set.remove(canonical).is_some();
			if per_set.is_empty() {
				self.retryable.remove(&operation);
			}
		}
		removed
	}

	/// THE admission decision, shared by the picker's predicate and by `reserve`.
	///
	/// One shared `Arc` shares the data; it does not share the decision. Two copies
	/// of this logic once diverged - `reserve` consulted the first receipt for a
	/// slab set while the predicate consulted all of them - so there is exactly one
	/// copy now. Anything that wants to know whether work is admissible asks here.
	///
	/// Precedence: in flight, then quarantined, then waiting. An attempt in flight
	/// over the exact slab set bars every operation - the physical claims would
	/// serialize them anyway, and the loser would only spend a build on a race.
	/// Receipts and cooldowns are THIS operation's: every deterministic receipt for
	/// the operation over the canonical slab set is consulted, whatever its origin,
	/// because each origin carries its own budget and an eligible one must not hide
	/// a spent one. Another operation's receipts do not bar this one: they record a
	/// failure this job cannot repair and a success it can never clear.
	fn refusal(
		&self,
		operation: MaintenanceOperation,
		canonical: &[SlabId],
		now_ms: u64,
	) -> Option<AdmissionRefusal> {
		if self.in_flight.contains_key(canonical) {
			return Some(AdmissionRefusal::AlreadyInFlight);
		}
		let mut refusal = None;
		for (key, receipt) in &self.deterministic {
			if key.operation() != operation || key.slabs() != canonical {
				continue;
			}
			if receipt.is_quarantined() {
				return Some(AdmissionRefusal::Quarantined);
			}
			if !receipt.is_eligible_at(now_ms) {
				refusal = Some(AdmissionRefusal::NotYetEligible);
			}
		}
		if refusal.is_none()
			&& let Some(cooldown) = self.retryable_for(operation, canonical)
			&& now_ms < cooldown.next_eligible_at_ms
		{
			refusal = Some(AdmissionRefusal::NotYetEligible);
		}
		if refusal.is_none()
			&& let Some(cooldown) = self.neutral_for(operation, canonical)
			&& now_ms < cooldown.next_eligible_at_ms
		{
			refusal = Some(AdmissionRefusal::NeutralCooldown);
		}
		refusal
	}

	fn neutral_for(
		&self,
		operation: MaintenanceOperation,
		canonical: &[SlabId],
	) -> Option<&NeutralCooldown> {
		self.neutral.get(&operation)?.get(canonical)
	}

	fn clear_neutral(&mut self, operation: MaintenanceOperation, canonical: &[SlabId]) -> bool {
		let mut removed = false;
		if let Some(per_set) = self.neutral.get_mut(&operation) {
			removed = per_set.remove(canonical).is_some();
			if per_set.is_empty() {
				self.neutral.remove(&operation);
			}
		}
		removed
	}

	/// Drop every neutral cooldown whose deadline passed a full cap period ago.
	/// Called when one is recorded, so the table is bounded by the sets scanned
	/// within two cap periods rather than by every set ever scanned. The grace
	/// period matters: an entry must outlive its own deadline so that the next
	/// clean scan of a live set - which arrives some ticks after the deadline,
	/// once the pass reaches it - still doubles from it. A set nobody rescanned
	/// for a whole cap period after it became eligible was retired.
	fn sweep_expired_neutral(&mut self, now_ms: u64) {
		let grace_ms = NEUTRAL_COOLDOWN_CAP.as_millis() as u64;
		self.neutral.retain(|_, per_set| {
			per_set.retain(|_, cooldown| {
				now_ms < cooldown.next_eligible_at_ms.saturating_add(grace_ms)
			});
			!per_set.is_empty()
		});
	}
}

/// ADR 0097 decision 4: proof that this attempt owns its work, and that the policy
/// will hear how it ended.
///
/// Owns the shared policy directly rather than borrowing from the manager or the
/// executor future, so its fail-closed `Drop` still runs if either is being torn
/// down mid-attempt - which is exactly when an unrecorded attempt would otherwise
/// slip through.
///
/// Dropping an UNFINALIZED guard disables maintenance. That is deliberate and it
/// is the whole point: a cancellation or panic after the working-set build but
/// before the outcome reached disk leaves an attempt that cost a full build and
/// that nothing counted. Releasing the reservation quietly would readmit the same
/// work immediately, with the bound none the wiser - the unbounded loop again,
/// now invisible to the counter meant to stop it.
pub(crate) struct MaintenanceReservation {
	policy: Arc<MaintenancePolicyInner>,
	operation: MaintenanceOperation,
	slabs: Vec<SlabId>,
	finalized: bool,
}

impl MaintenanceReservation {
	pub(crate) fn operation(&self) -> MaintenanceOperation {
		self.operation
	}

	pub(crate) fn slabs(&self) -> &[SlabId] {
		&self.slabs
	}

	/// Mark the outcome durably recorded. Call only AFTER persistence returns.
	///
	/// The flag is set LAST: if removing the reservation panics or finds the
	/// invariant broken, the guard is still unfinalized and `Drop` fails closed.
	pub(crate) fn finalize(mut self) {
		{
			let mut state = self.policy.lock();
			if let PolicyState::Enabled { tracker, .. } = &mut *state
				&& !tracker.in_flight.remove(&self.slabs).is_some()
			{
				// The reservation this guard represents was not held. Something else
				// released it, which means admission is no longer accounted for.
				tracing::error!(
					slabs = ?self.slabs,
					"finalized a maintenance reservation that was not in flight"
				);
				*state = PolicyState::Disabled {
					diagnostic: "reservation accounting lost".to_owned(),
				};
				self.policy.bump_report_generation();
				return;
			}
			self.policy.bump_report_generation();
		}
		self.finalized = true;
	}
}

impl Drop for MaintenanceReservation {
	fn drop(&mut self) {
		if self.finalized {
			return;
		}
		// Not `disable()` first: the reservation must come out of `in_flight` in the
		// same critical section, or a later re-enable would inherit a phantom
		// reservation for work no longer running.
		let mut state = self.policy.lock();
		if let PolicyState::Enabled { tracker, .. } = &mut *state {
			tracker.in_flight.remove(&self.slabs);
			tracing::error!(
				slabs = ?self.slabs,
				"maintenance attempt abandoned before its outcome reached disk; \
				 disabling admission"
			);
			*state = PolicyState::Disabled {
				diagnostic: "attempt abandoned before durable outcome".to_owned(),
			};
			self.policy.bump_report_generation();
		}
	}
}

impl MaintenancePolicyInner {
	/// Atomically re-check eligibility and reserve the work.
	///
	/// The clock is re-sampled here rather than reusing the value the picker used:
	/// selection may have inspected several candidates, and the deadline boundary
	/// that matters is the one at the moment of admission.
	pub(crate) fn reserve(
		self: &Arc<Self>,
		operation: MaintenanceOperation,
		slabs: &[SlabId],
	) -> Result<MaintenanceReservation, AdmissionRefusal> {
		let Some(now_ms) = self.clock.now_ms() else {
			self.disable("policy clock unavailable at admission");
			return Err(AdmissionRefusal::ClockUnavailable);
		};
		let canonical = MaintenanceTracker::canonical(slabs);
		if canonical.is_empty() {
			return Err(AdmissionRefusal::EmptyWorkSet);
		}
		let mut state = self.lock();
		let PolicyState::Enabled { tracker, .. } = &mut *state else {
			let PolicyState::Disabled { diagnostic } = &*state else {
				unreachable!("state is one of two variants");
			};
			return Err(AdmissionRefusal::PolicyDisabled(diagnostic.clone()));
		};
		// The same decision the picker's predicate made, re-made under the lock at
		// the moment of admission - not a second copy of it.
		if let Some(refusal) = tracker.refusal(operation, &canonical, now_ms) {
			return Err(refusal);
		}
		tracker.in_flight.insert(canonical.clone(), operation);
		self.bump_report_generation();
		drop(state);
		Ok(MaintenanceReservation {
			policy: Arc::clone(self),
			operation,
			slabs: canonical,
			finalized: false,
		})
	}
}

/// How one policed attempt ended, from the caller's point of view.
///
/// `Skipped` is neither success nor failure: the work was not attempted, and the
/// refusal says why. A caller iterating candidates moves on; a tick returns "no
/// work this tick".
#[derive(Debug)]
pub(crate) enum MaintenanceAttempt<T, E> {
	Skipped(AdmissionRefusal),
	Completed(T),
	Failed(E),
}

/// Why a requeue did not clear a quarantine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RequeueRefusal {
	PolicyDisabled(String),
	/// The work is being attempted right now; its outcome will speak for itself.
	InFlight,
	/// Requeue operates only on quarantined work. Waiting work needs no operator.
	NotQuarantined,
	/// The operator reviewed a different receipt set. Nothing was cleared.
	StaleReport {
		quarantined_origins: usize,
	},
	/// The durable receipt could not be removed. The quarantine stays effective,
	/// on disk and in memory, and admission is disabled because the store is not
	/// trustworthy.
	StoreFailure(String),
}

struct PreparedRequeue {
	operation: MaintenanceOperation,
	canonical: Vec<SlabId>,
	keys: Vec<MaintenanceWorkKey>,
	store: Arc<PersistentLocalIndex>,
	reservation: MaintenanceReservation,
}

pub(crate) const MAINTENANCE_FINGERPRINT_VERSION: u8 = 1;
pub(crate) const MAINTENANCE_REPORT_CURSOR_VERSION: u8 = 1;
pub(crate) const MAINTENANCE_REPORT_MAX_ITEMS: usize = 64;
pub(crate) const MAINTENANCE_REPORT_MAX_BYTES: usize = 256 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MaintenanceFingerprint {
	pub(crate) version: u8,
	pub(crate) digest: [u8; 32],
}

impl MaintenanceFingerprint {
	pub(crate) fn to_bytes(self) -> [u8; 33] {
		let mut bytes = [0_u8; 33];
		bytes[0] = self.version;
		bytes[1..].copy_from_slice(&self.digest);
		bytes
	}

	pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, &'static str> {
		if bytes.len() != 33 {
			return Err("maintenance fingerprint must be exactly 33 bytes");
		}
		if bytes[0] != MAINTENANCE_FINGERPRINT_VERSION {
			return Err("maintenance fingerprint version is unsupported");
		}
		let mut digest = [0_u8; 32];
		digest.copy_from_slice(&bytes[1..]);
		Ok(Self {
			version: bytes[0],
			digest,
		})
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MaintenanceOriginView {
	pub(crate) origin: DeterministicOrigin,
	pub(crate) reason: DeterministicFailureReason,
	pub(crate) attempts: u32,
	pub(crate) last_failure_ms: u64,
	pub(crate) next_eligible_at_ms: Option<u64>,
	pub(crate) detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum MaintenanceReportItem {
	QuarantinedWork {
		operation: MaintenanceOperation,
		slabs: Vec<SlabId>,
		origins: Vec<MaintenanceOriginView>,
		quarantine_fingerprint: MaintenanceFingerprint,
	},
	/// ADR 0098 decision 3: deterministic receipts still inside their backoff
	/// budget. Every origin carries a deadline; none is quarantined, so there is
	/// no fingerprint and nothing to requeue.
	BackingOff {
		operation: MaintenanceOperation,
		slabs: Vec<SlabId>,
		origins: Vec<MaintenanceOriginView>,
	},
	RetryableCooldown {
		operation: MaintenanceOperation,
		slabs: Vec<SlabId>,
		consecutive_failures: u32,
		next_eligible_at_ms: u64,
	},
	InFlight {
		operation: MaintenanceOperation,
		slabs: Vec<SlabId>,
	},
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum MaintenanceReportPosition {
	QuarantinedWork(MaintenanceOperation, Vec<SlabId>),
	BackingOff(MaintenanceOperation, Vec<SlabId>),
	Retryable(MaintenanceOperation, Vec<SlabId>),
	InFlight(MaintenanceOperation, Vec<SlabId>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MaintenanceReportPage {
	pub(crate) enabled: bool,
	pub(crate) diagnostic: Option<String>,
	pub(crate) incarnation: [u8; 16],
	pub(crate) generation: u64,
	pub(crate) items: Vec<MaintenanceReportItem>,
	pub(crate) next_cursor: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum MaintenanceReportPageError {
	InvalidCursor(String),
	StaleCursor,
	InvalidBound(String),
}

fn quarantine_fingerprint(
	operation: MaintenanceOperation,
	slabs: &[SlabId],
	origins: &[(DeterministicOrigin, &DeterministicReceipt)],
) -> MaintenanceFingerprint {
	#[cfg(test)]
	REPORT_FINGERPRINT_CALLS.with(|calls| calls.set(calls.get() + 1));
	const DOMAIN: &[u8] = b"nilket:maintenance-quarantine:v1\0";
	let mut canonical =
		Vec::with_capacity(DOMAIN.len() + 2 + 4 + slabs.len() * 8 + 2 + origins.len() * 32);
	canonical.extend_from_slice(DOMAIN);
	canonical.extend_from_slice(&operation.code().to_be_bytes());
	canonical.extend_from_slice(&(slabs.len() as u32).to_be_bytes());
	for slab in slabs {
		canonical.extend_from_slice(&slab.get().to_be_bytes());
	}
	canonical.extend_from_slice(&(origins.len() as u16).to_be_bytes());
	for (origin, receipt) in origins {
		canonical.extend_from_slice(&origin.code().to_be_bytes());
		canonical.extend_from_slice(&receipt.attempts().to_be_bytes());
		canonical.extend_from_slice(&receipt.last_failure_ms.to_be_bytes());
		match receipt.next_eligible_at_ms {
			Some(deadline) => {
				canonical.push(1);
				canonical.extend_from_slice(&deadline.to_be_bytes());
			}
			None => canonical.push(0),
		}
		let detail = receipt.detail().as_bytes();
		canonical.extend_from_slice(&(detail.len() as u16).to_be_bytes());
		canonical.extend_from_slice(detail);
	}
	MaintenanceFingerprint {
		version: MAINTENANCE_FINGERPRINT_VERSION,
		digest: crate::hash::sha256::digest(&canonical),
	}
}

#[cfg(test)]
std::thread_local! {
	static REPORT_FINGERPRINT_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	static REPORT_DETERMINISTIC_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn encode_report_cursor(
	incarnation: [u8; 16],
	generation: u64,
	position: &MaintenanceReportPosition,
) -> Vec<u8> {
	let (kind, operation, slabs) = match position {
		MaintenanceReportPosition::QuarantinedWork(operation, slabs) => {
			(0, operation.code(), slabs)
		}
		MaintenanceReportPosition::Retryable(operation, slabs) => (1, operation.code(), slabs),
		MaintenanceReportPosition::InFlight(operation, slabs) => (2, operation.code(), slabs),
		MaintenanceReportPosition::BackingOff(operation, slabs) => (3, operation.code(), slabs),
	};
	let mut bytes = Vec::with_capacity(32 + slabs.len() * 8);
	bytes.push(MAINTENANCE_REPORT_CURSOR_VERSION);
	bytes.extend_from_slice(&incarnation);
	bytes.extend_from_slice(&generation.to_be_bytes());
	bytes.push(kind);
	bytes.extend_from_slice(&operation.to_be_bytes());
	bytes.extend_from_slice(&(slabs.len() as u32).to_be_bytes());
	for slab in slabs {
		bytes.extend_from_slice(&slab.get().to_be_bytes());
	}
	bytes
}

fn decode_report_cursor(
	bytes: &[u8],
) -> Result<([u8; 16], u64, MaintenanceReportPosition), MaintenanceReportPageError> {
	const HEADER: usize = 1 + 16 + 8 + 1 + 2 + 4;
	if bytes.len() < HEADER {
		return Err(MaintenanceReportPageError::InvalidCursor(
			"maintenance report cursor is truncated".to_owned(),
		));
	}
	if bytes[0] != MAINTENANCE_REPORT_CURSOR_VERSION {
		return Err(MaintenanceReportPageError::InvalidCursor(
			"maintenance report cursor version is unsupported".to_owned(),
		));
	}
	let mut incarnation = [0_u8; 16];
	incarnation.copy_from_slice(&bytes[1..17]);
	let generation = u64::from_be_bytes(bytes[17..25].try_into().expect("fixed slice"));
	let kind = bytes[25];
	let operation_code = u16::from_be_bytes(bytes[26..28].try_into().expect("fixed slice"));
	let slab_count = u32::from_be_bytes(bytes[28..32].try_into().expect("fixed slice")) as usize;
	if slab_count > MAX_RECEIPT_KEY_SLABS || bytes.len() != HEADER + slab_count * 8 {
		return Err(MaintenanceReportPageError::InvalidCursor(
			"maintenance report cursor slab set is malformed".to_owned(),
		));
	}
	let mut slabs = Vec::with_capacity(slab_count);
	for chunk in bytes[HEADER..].chunks_exact(8) {
		slabs.push(SlabId::new(u64::from_be_bytes(
			chunk.try_into().expect("fixed chunk"),
		)));
	}
	if slabs.windows(2).any(|pair| pair[0] >= pair[1]) {
		return Err(MaintenanceReportPageError::InvalidCursor(
			"maintenance report cursor slabs are not canonical".to_owned(),
		));
	}
	let position = match kind {
		0 | 1 => {
			let operation = MaintenanceOperation::from_code(operation_code).ok_or_else(|| {
				MaintenanceReportPageError::InvalidCursor(
					"maintenance report cursor operation is unknown".to_owned(),
				)
			})?;
			if kind == 0 {
				MaintenanceReportPosition::QuarantinedWork(operation, slabs)
			} else {
				MaintenanceReportPosition::Retryable(operation, slabs)
			}
		}
		2 | 3 => {
			let operation = MaintenanceOperation::from_code(operation_code).ok_or_else(|| {
				MaintenanceReportPageError::InvalidCursor(
					"maintenance report cursor operation is unknown".to_owned(),
				)
			})?;
			if kind == 2 {
				MaintenanceReportPosition::InFlight(operation, slabs)
			} else {
				MaintenanceReportPosition::BackingOff(operation, slabs)
			}
		}
		_ => {
			return Err(MaintenanceReportPageError::InvalidCursor(
				"maintenance report cursor kind is unknown".to_owned(),
			));
		}
	};
	Ok((incarnation, generation, position))
}

fn report_item_estimated_bytes(item: &MaintenanceReportItem) -> usize {
	const ITEM_OVERHEAD: usize = 64;
	match item {
		MaintenanceReportItem::QuarantinedWork { slabs, origins, .. }
		| MaintenanceReportItem::BackingOff { slabs, origins, .. } => {
			ITEM_OVERHEAD
				+ slabs.len() * 10
				+ origins
					.iter()
					.map(|origin| 48 + origin.detail.len())
					.sum::<usize>()
		}
		MaintenanceReportItem::RetryableCooldown { slabs, .. }
		| MaintenanceReportItem::InFlight { slabs, .. } => ITEM_OVERHEAD + slabs.len() * 10,
	}
}

fn try_push_report_item(
	after: Option<&MaintenanceReportPosition>,
	max_items: usize,
	max_bytes: usize,
	position: MaintenanceReportPosition,
	item: MaintenanceReportItem,
	items: &mut Vec<MaintenanceReportItem>,
	used_bytes: &mut usize,
	last_position: &mut Option<MaintenanceReportPosition>,
	has_more: &mut bool,
) -> bool {
	if after.is_some_and(|after| position <= *after) {
		return true;
	}
	let item_bytes = report_item_estimated_bytes(&item);
	if items.len() == max_items || used_bytes.saturating_add(item_bytes) > max_bytes {
		*has_more = true;
		return false;
	}
	*used_bytes += item_bytes;
	*last_position = Some(position);
	items.push(item);
	true
}

fn quarantined_report_item(
	operation: MaintenanceOperation,
	slabs: Vec<SlabId>,
	mut origins: Vec<MaintenanceOriginView>,
) -> Option<(MaintenanceReportPosition, MaintenanceReportItem)> {
	origins.retain(|origin| origin.next_eligible_at_ms.is_none());
	if origins.is_empty() {
		return None;
	}
	let quarantined = origins
		.iter()
		.map(|origin| {
			(
				origin.origin,
				DeterministicReceipt {
					attempts: origin.attempts,
					last_failure_ms: origin.last_failure_ms,
					next_eligible_at_ms: origin.next_eligible_at_ms,
					detail: origin.detail.clone(),
				},
			)
		})
		.collect::<Vec<_>>();
	let borrowed = quarantined
		.iter()
		.map(|(origin, receipt)| (*origin, receipt))
		.collect::<Vec<_>>();
	let fingerprint = quarantine_fingerprint(operation, &slabs, &borrowed);
	Some((
		MaintenanceReportPosition::QuarantinedWork(operation, slabs.clone()),
		MaintenanceReportItem::QuarantinedWork {
			operation,
			slabs,
			origins,
			quarantine_fingerprint: fingerprint,
		},
	))
}

/// ADR 0098 decision 3: the origins of one work identity that are backing off,
/// or nothing when every origin is quarantined.
fn backing_off_report_item(
	operation: MaintenanceOperation,
	slabs: Vec<SlabId>,
	mut origins: Vec<MaintenanceOriginView>,
) -> Option<(MaintenanceReportPosition, MaintenanceReportItem)> {
	origins.retain(|origin| origin.next_eligible_at_ms.is_some());
	if origins.is_empty() {
		return None;
	}
	Some((
		MaintenanceReportPosition::BackingOff(operation, slabs.clone()),
		MaintenanceReportItem::BackingOff {
			operation,
			slabs,
			origins,
		},
	))
}

/// Operator-facing view of one receipt: the slab set and the reason, so a
/// quarantine can be understood and requeued (ADR 0097, Risks).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MaintenanceReceiptView {
	pub(crate) operation: MaintenanceOperation,
	pub(crate) slabs: Vec<SlabId>,
	pub(crate) origin: DeterministicOrigin,
	pub(crate) reason: DeterministicFailureReason,
	pub(crate) attempts: u32,
	pub(crate) last_failure_ms: u64,
	/// `None` is quarantine.
	pub(crate) next_eligible_at_ms: Option<u64>,
	pub(crate) detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RetryableCooldownView {
	pub(crate) operation: MaintenanceOperation,
	pub(crate) slabs: Vec<SlabId>,
	pub(crate) consecutive_failures: u32,
	pub(crate) next_eligible_at_ms: u64,
}

/// The reporting half of the policy (ADR 0097, Acceptance): what an operator
/// reads to know why maintenance is or is not running.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MaintenancePolicyReport {
	pub(crate) enabled: bool,
	pub(crate) diagnostic: Option<String>,
	pub(crate) receipts: Vec<MaintenanceReceiptView>,
	pub(crate) retryable: Vec<RetryableCooldownView>,
	pub(crate) in_flight: Vec<Vec<SlabId>>,
}

impl MaintenancePolicyReport {
	pub(crate) fn quarantined(&self) -> impl Iterator<Item = &MaintenanceReceiptView> {
		self.receipts
			.iter()
			.filter(|receipt| receipt.next_eligible_at_ms.is_none())
	}
}

impl MaintenancePolicyInner {
	/// ADR 0098 decision 2: the neutral cooldowns in force, in canonical order.
	#[cfg(test)]
	pub(crate) fn neutral_cooldowns_for_test(
		&self,
	) -> Vec<(MaintenanceOperation, Vec<SlabId>, NeutralCooldown)> {
		let state = self.lock();
		let PolicyState::Enabled { tracker, .. } = &*state else {
			return Vec::new();
		};
		tracker
			.neutral
			.iter()
			.flat_map(|(operation, per_set)| {
				per_set
					.iter()
					.map(|(slabs, cooldown)| (*operation, slabs.clone(), *cooldown))
			})
			.collect()
	}

	#[cfg(test)]
	pub(crate) fn quarantine_fingerprint_for_test(
		&self,
		operation: MaintenanceOperation,
		slabs: &[SlabId],
	) -> Option<MaintenanceFingerprint> {
		let canonical = MaintenanceTracker::canonical(slabs);
		let state = self.lock();
		let PolicyState::Enabled { tracker, .. } = &*state else {
			return None;
		};
		let entries = tracker
			.deterministic
			.iter()
			.filter(|(key, receipt)| {
				key.operation() == operation
					&& key.slabs() == canonical.as_slice()
					&& receipt.is_quarantined()
			})
			.map(|(key, receipt)| (key.origin(), receipt))
			.collect::<Vec<_>>();
		(!entries.is_empty()).then(|| quarantine_fingerprint(operation, &canonical, &entries))
	}

	pub(crate) fn report_page(
		&self,
		cursor: Option<&[u8]>,
		max_items: usize,
		max_bytes: usize,
	) -> Result<MaintenanceReportPage, MaintenanceReportPageError> {
		if max_items == 0 || max_items > MAINTENANCE_REPORT_MAX_ITEMS {
			return Err(MaintenanceReportPageError::InvalidBound(format!(
				"maintenance report item bound must be 1..={MAINTENANCE_REPORT_MAX_ITEMS}"
			)));
		}
		if max_bytes == 0 || max_bytes > MAINTENANCE_REPORT_MAX_BYTES {
			return Err(MaintenanceReportPageError::InvalidBound(format!(
				"maintenance report byte bound must be 1..={MAINTENANCE_REPORT_MAX_BYTES}"
			)));
		}

		let decoded_cursor = cursor.map(decode_report_cursor).transpose()?;
		let state = self.lock();
		let generation = self.report_generation.load(Ordering::Acquire);
		let after = match decoded_cursor {
			Some((incarnation, cursor_generation, position)) => {
				if incarnation != self.report_incarnation || cursor_generation != generation {
					return Err(MaintenanceReportPageError::StaleCursor);
				}
				Some(position)
			}
			None => None,
		};
		let PolicyState::Enabled { tracker, .. } = &*state else {
			let PolicyState::Disabled { diagnostic } = &*state else {
				unreachable!("state is one of two variants");
			};
			return Ok(MaintenanceReportPage {
				enabled: false,
				diagnostic: Some(diagnostic.clone()),
				incarnation: self.report_incarnation,
				generation,
				items: Vec::new(),
				next_cursor: None,
			});
		};

		let mut items = Vec::new();
		let mut used_bytes = 128_usize;
		let mut last_position = None;
		let mut has_more = false;

		let mut group_operation = None;
		let mut group_slabs = Vec::new();
		let mut group_origins = Vec::new();

		// The deterministic map is ordered by operation, slabs, then origin. Resume
		// at the cursor's work identity and discard only that identity's bounded
		// origin suffix BEFORE grouping or hashing it. Later report variants skip
		// the deterministic map entirely. A page therefore does not repeat all
		// receipt hashing from the beginning while holding the admission lock.
		let deterministic: Box<
			dyn Iterator<Item = (&MaintenanceWorkKey, &DeterministicReceipt)> + '_,
		> = match after.as_ref() {
			None => Box::new(tracker.deterministic.iter()),
			Some(MaintenanceReportPosition::QuarantinedWork(operation, slabs)) => {
				let start = MaintenanceWorkKey {
					operation: *operation,
					slabs: slabs.clone(),
					origin: DeterministicOrigin::LsmWriterCannotSplitMultiRowFrame,
				};
				Box::new(tracker.deterministic.range(start..))
			}
			Some(MaintenanceReportPosition::BackingOff(..))
			| Some(MaintenanceReportPosition::Retryable(..))
			| Some(MaintenanceReportPosition::InFlight(..)) => Box::new(std::iter::empty()),
		};
		for (key, receipt) in deterministic {
			#[cfg(test)]
			REPORT_DETERMINISTIC_VISITS.with(|visits| visits.set(visits.get() + 1));
			if matches!(
				after.as_ref(),
				Some(MaintenanceReportPosition::QuarantinedWork(operation, slabs))
					if *operation == key.operation() && slabs.as_slice() == key.slabs()
			) {
				continue;
			}
			let changed = group_operation.is_some_and(|operation| {
				operation != key.operation() || group_slabs.as_slice() != key.slabs()
			});
			if changed {
				let grouped = quarantined_report_item(
					group_operation.expect("a changed group has an operation"),
					std::mem::take(&mut group_slabs),
					std::mem::take(&mut group_origins),
				);
				if let Some((position, item)) = grouped
					&& !try_push_report_item(
						after.as_ref(),
						max_items,
						max_bytes,
						position,
						item,
						&mut items,
						&mut used_bytes,
						&mut last_position,
						&mut has_more,
					) {
					break;
				}
			}
			if has_more {
				break;
			}
			if group_operation != Some(key.operation()) || group_slabs.as_slice() != key.slabs() {
				group_operation = Some(key.operation());
				group_slabs = key.slabs().to_vec();
			}
			group_origins.push(MaintenanceOriginView {
				origin: key.origin(),
				reason: key.reason(),
				attempts: receipt.attempts(),
				last_failure_ms: receipt.last_failure_ms,
				next_eligible_at_ms: receipt.next_eligible_at_ms,
				detail: receipt.detail().to_owned(),
			});
		}
		if !has_more && let Some(operation) = group_operation {
			if let Some((position, item)) =
				quarantined_report_item(operation, group_slabs, group_origins)
			{
				let _ = try_push_report_item(
					after.as_ref(),
					max_items,
					max_bytes,
					position,
					item,
					&mut items,
					&mut used_bytes,
					&mut last_position,
					&mut has_more,
				);
			}
		}

		// ADR 0098 decision 3: the same map, read once more for the receipts still
		// inside their backoff budget. Grouped by work identity like the quarantined
		// pass, positioned after every quarantined item so a page boundary between
		// one identity's two views loses neither. No hashing happens here.
		if !has_more {
			let backing_off: Box<
				dyn Iterator<Item = (&MaintenanceWorkKey, &DeterministicReceipt)> + '_,
			> = match after.as_ref() {
				None | Some(MaintenanceReportPosition::QuarantinedWork(..)) => {
					Box::new(tracker.deterministic.iter())
				}
				Some(MaintenanceReportPosition::BackingOff(operation, slabs)) => {
					let start = MaintenanceWorkKey {
						operation: *operation,
						slabs: slabs.clone(),
						origin: DeterministicOrigin::LsmWriterCannotSplitMultiRowFrame,
					};
					Box::new(tracker.deterministic.range(start..))
				}
				Some(MaintenanceReportPosition::Retryable(..))
				| Some(MaintenanceReportPosition::InFlight(..)) => Box::new(std::iter::empty()),
			};
			let mut group_operation = None;
			let mut group_slabs = Vec::new();
			let mut group_origins = Vec::new();
			for (key, receipt) in backing_off {
				if receipt.next_eligible_at_ms.is_none() {
					continue;
				}
				if matches!(
					after.as_ref(),
					Some(MaintenanceReportPosition::BackingOff(operation, slabs))
						if *operation == key.operation() && slabs.as_slice() == key.slabs()
				) {
					continue;
				}
				let changed = group_operation.is_some_and(|operation| {
					operation != key.operation() || group_slabs.as_slice() != key.slabs()
				});
				if changed {
					let grouped = backing_off_report_item(
						group_operation.expect("a changed group has an operation"),
						std::mem::take(&mut group_slabs),
						std::mem::take(&mut group_origins),
					);
					if let Some((position, item)) = grouped
						&& !try_push_report_item(
							after.as_ref(),
							max_items,
							max_bytes,
							position,
							item,
							&mut items,
							&mut used_bytes,
							&mut last_position,
							&mut has_more,
						) {
						break;
					}
				}
				if group_operation != Some(key.operation()) || group_slabs.as_slice() != key.slabs()
				{
					group_operation = Some(key.operation());
					group_slabs = key.slabs().to_vec();
				}
				group_origins.push(MaintenanceOriginView {
					origin: key.origin(),
					reason: key.reason(),
					attempts: receipt.attempts(),
					last_failure_ms: receipt.last_failure_ms,
					next_eligible_at_ms: receipt.next_eligible_at_ms,
					detail: receipt.detail().to_owned(),
				});
			}
			if !has_more
				&& let Some(operation) = group_operation
				&& let Some((position, item)) =
					backing_off_report_item(operation, group_slabs, group_origins)
			{
				let _ = try_push_report_item(
					after.as_ref(),
					max_items,
					max_bytes,
					position,
					item,
					&mut items,
					&mut used_bytes,
					&mut last_position,
					&mut has_more,
				);
			}
		}

		if !has_more {
			'outer_retryable: for (operation, per_set) in &tracker.retryable {
				for (slabs, cooldown) in per_set {
					let position = MaintenanceReportPosition::Retryable(*operation, slabs.clone());
					let item = MaintenanceReportItem::RetryableCooldown {
						operation: *operation,
						slabs: slabs.clone(),
						consecutive_failures: cooldown.failures,
						next_eligible_at_ms: cooldown.next_eligible_at_ms,
					};
					if !try_push_report_item(
						after.as_ref(),
						max_items,
						max_bytes,
						position,
						item,
						&mut items,
						&mut used_bytes,
						&mut last_position,
						&mut has_more,
					) {
						break 'outer_retryable;
					}
				}
			}
		}
		if !has_more {
			let mut in_flight = tracker
				.in_flight
				.iter()
				.map(|(slabs, operation)| (*operation, slabs))
				.collect::<Vec<_>>();
			in_flight.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(right.1)));
			for (operation, slabs) in in_flight {
				let position = MaintenanceReportPosition::InFlight(operation, slabs.clone());
				let item = MaintenanceReportItem::InFlight {
					operation,
					slabs: slabs.clone(),
				};
				if !try_push_report_item(
					after.as_ref(),
					max_items,
					max_bytes,
					position,
					item,
					&mut items,
					&mut used_bytes,
					&mut last_position,
					&mut has_more,
				) {
					break;
				}
			}
		}

		let next_cursor = if has_more {
			last_position
				.as_ref()
				.map(|position| encode_report_cursor(self.report_incarnation, generation, position))
		} else {
			None
		};
		Ok(MaintenanceReportPage {
			enabled: true,
			diagnostic: None,
			incarnation: self.report_incarnation,
			generation,
			items,
			next_cursor,
		})
	}

	/// ADR 0097 decision 4: the ONE path a maintenance attempt takes.
	///
	/// Pick provisionally elsewhere; here: reserve atomically, execute with no
	/// policy lock held, record the outcome durably, and only then release. The
	/// reservation lives across `work`, so cancelling the future mid-build drops
	/// it unfinalized and disables admission - an attempt nothing recorded must
	/// not be silently readmitted.
	///
	/// `classify` runs on the error; `detail` is its `Display`, bounded by the
	/// receipt. `disposition` runs on the value and says whether the job actually
	/// rewrote the slabs: only a `Rewrote` completion is evidence against the
	/// receipts, and only this operation's receipts at that. A `Completed` value is
	/// returned even when the success bookkeeping fails: the work was published, and
	/// pretending otherwise would be a lie the caller might act on. The policy
	/// disables itself instead.
	pub(crate) async fn execute<T, E, Fut>(
		self: &Arc<Self>,
		operation: MaintenanceOperation,
		slabs: &[SlabId],
		classify: impl FnOnce(&E) -> MaintenanceFailureClass,
		disposition: impl FnOnce(&T) -> SuccessDisposition,
		work: impl FnOnce() -> Fut,
	) -> MaintenanceAttempt<T, E>
	where
		Fut: std::future::Future<Output = Result<T, E>>,
		E: fmt::Display,
	{
		let metrics = crate::metrics::global();
		let reservation = match self.reserve(operation, slabs) {
			Ok(reservation) => reservation,
			Err(refusal) => {
				metrics.observe_maintenance_attempt(MaintenanceAttemptResult::skipped(&refusal));
				return MaintenanceAttempt::Skipped(refusal);
			}
		};
		let result = work().await;
		match &result {
			Ok(value) => match disposition(value) {
				SuccessDisposition::Rewrote => {
					self.record_success(reservation).await;
					metrics.observe_maintenance_attempt(MaintenanceAttemptResult::Ok);
				}
				SuccessDisposition::Neutral => {
					self.record_neutral(reservation);
					metrics.observe_maintenance_attempt(
						MaintenanceAttemptResult::CompletedWithoutRewrite,
					);
				}
				SuccessDisposition::Unclaimed => {
					self.record_unclaimed(reservation);
					metrics.observe_maintenance_attempt(
						MaintenanceAttemptResult::CompletedWithoutRewrite,
					);
				}
			},
			Err(error) => {
				let class = classify(error);
				let detail = error.to_string();
				let quarantined = self.record_failure(reservation, class, &detail).await;
				metrics.observe_maintenance_attempt(match class {
					MaintenanceFailureClass::Retryable => MaintenanceAttemptResult::Retryable,
					MaintenanceFailureClass::Deterministic(origin) => {
						metrics.observe_maintenance_deterministic_failure(origin.reason());
						if quarantined {
							MaintenanceAttemptResult::Quarantined
						} else {
							MaintenanceAttemptResult::Deterministic
						}
					}
				});
			}
		}
		match result {
			Ok(value) => MaintenanceAttempt::Completed(value),
			Err(error) => MaintenanceAttempt::Failed(error),
		}
	}

	/// A completion that never owned its slabs proves nothing. The reservation is
	/// released and every receipt and cooldown stands exactly as it was: the next
	/// attempt is still the next attempt.
	fn record_unclaimed(&self, reservation: MaintenanceReservation) {
		reservation.finalize();
	}

	/// ADR 0098 decision 2: a completion that scanned its slabs and found nothing
	/// to rewrite is not evidence about any receipt - those stand exactly as they
	/// were - but it IS evidence that scanning the same set on the next tick would
	/// find the same nothing. The set cools down for one tick, doubling per
	/// consecutive clean scan up to the cap. The clock is sampled after the scan,
	/// as for failures, so a scan longer than its own cooldown does not consume it.
	/// A clock or arithmetic failure disables admission, as every other
	/// unrecordable outcome does.
	fn record_neutral(&self, reservation: MaintenanceReservation) {
		let operation = reservation.operation();
		let slabs = reservation.slabs().to_vec();
		let Some(now_ms) = self.clock.now_ms() else {
			self.disable("policy clock unavailable after a neutral attempt");
			reservation.finalize();
			return;
		};
		{
			let mut state = self.lock();
			if let PolicyState::Enabled { tracker, .. } = &mut *state {
				let previous = tracker
					.neutral_for(operation, &slabs)
					.map_or(0, |cooldown| cooldown.completions);
				tracker.sweep_expired_neutral(now_ms);
				let next = previous.checked_add(1).and_then(|completions| {
					let cooldown_ms =
						u64::try_from(neutral_cooldown(completions).as_millis()).ok()?;
					let next_eligible_at_ms = now_ms.checked_add(cooldown_ms)?;
					Some(NeutralCooldown {
						completions,
						next_eligible_at_ms,
					})
				});
				match next {
					Some(cooldown) => {
						tracker
							.neutral
							.entry(operation)
							.or_default()
							.insert(slabs, cooldown);
					}
					None => {
						drop(state);
						self.disable(
							"neutral completion counter or cooldown arithmetic overflowed",
						);
					}
				}
			}
		}
		reservation.finalize();
	}

	/// A rewrite clears EVERY origin's receipt for THIS operation over this exact
	/// slab set, and the operation's retryable cooldown. Another operation's receipts
	/// for the same slabs stand: this success is not evidence about that failure.
	/// Durable deletion first; memory after. The reservation is finalized last
	/// either way: a cleanup failure disables admission - the store can no longer
	/// be trusted to hold the record - but the attempt itself is complete and
	/// accounted for.
	async fn record_success(self: &Arc<Self>, reservation: MaintenanceReservation) {
		let operation = reservation.operation();
		let slabs = reservation.slabs().to_vec();
		let (keys, store) = {
			let mut state = self.lock();
			let PolicyState::Enabled { tracker, store } = &mut *state else {
				drop(state);
				reservation.finalize();
				return;
			};
			if tracker.clear_retryable(operation, &slabs) {
				self.bump_report_generation();
			}
			tracker.clear_neutral(operation, &slabs);
			let keys = tracker
				.deterministic
				.keys()
				.filter(|key| key.operation() == operation && key.slabs() == slabs.as_slice())
				.cloned()
				.collect::<Vec<_>>();
			(keys, Arc::clone(store))
		};
		if !keys.is_empty() {
			let encoded = keys.iter().map(MaintenanceWorkKey::encode).collect();
			if let Err(error) = store.delete_maintenance_receipts(encoded).await {
				self.disable(format!(
					"receipt cleanup after a successful attempt failed: {error}"
				));
				reservation.finalize();
				return;
			}
			let mut state = self.lock();
			if let PolicyState::Enabled { tracker, .. } = &mut *state {
				for key in &keys {
					tracker.deterministic.remove(key);
				}
				self.bump_report_generation();
			}
		}
		reservation.finalize();
		self.publish_gauges();
	}

	/// Returns whether the failure quarantined the work.
	///
	/// The clock is sampled HERE, after the outcome, never from the tick's start:
	/// a build that outlasts its own backoff would otherwise be eligible the
	/// instant it failed. Retryable failures touch only ephemeral state.
	/// Deterministic failures clear the retryable counter (it counts CONSECUTIVE
	/// transient failures), compute the next receipt without exposing it, persist
	/// it, install it, and only then finalize. Every clock, arithmetic or store
	/// failure disables admission before finalizing.
	async fn record_failure(
		self: &Arc<Self>,
		reservation: MaintenanceReservation,
		class: MaintenanceFailureClass,
		detail: &str,
	) -> bool {
		let operation = reservation.operation();
		let slabs = reservation.slabs().to_vec();
		let Some(now_ms) = self.clock.now_ms() else {
			self.disable("policy clock unavailable after a failed attempt");
			reservation.finalize();
			return false;
		};
		let quarantined = match class {
			MaintenanceFailureClass::Retryable => {
				let mut state = self.lock();
				if let PolicyState::Enabled { tracker, .. } = &mut *state {
					tracker.clear_neutral(operation, &slabs);
					// Checked, not saturating: a counter pinned at its maximum would
					// report a run of races as shorter than it was, and a cooldown
					// computed from it would be a schedule nothing measured. Overflow
					// is a fail-closed condition like any other arithmetic failure.
					let next = tracker
						.retryable_for(operation, &slabs)
						.map_or(0, |cooldown| cooldown.failures)
						.checked_add(1)
						.and_then(|failures| {
							let cooldown_ms =
								u64::try_from(retryable_cooldown(failures).as_millis()).ok()?;
							let next_eligible_at_ms = now_ms.checked_add(cooldown_ms)?;
							Some(RetryableCooldown {
								failures,
								next_eligible_at_ms,
							})
						});
					match next {
						Some(cooldown) => {
							tracker
								.retryable
								.entry(operation)
								.or_default()
								.insert(slabs, cooldown);
							self.bump_report_generation();
						}
						None => {
							drop(state);
							self.disable(
								"retryable failure counter or cooldown arithmetic overflowed",
							);
						}
					}
				}
				false
			}
			MaintenanceFailureClass::Deterministic(origin) => {
				let key = match MaintenanceWorkKey::new(operation, slabs.iter().copied(), origin) {
					Ok(key) => key,
					Err(error) => {
						self.disable(format!("deterministic failure has no work key: {error}"));
						reservation.finalize();
						return false;
					}
				};
				let (previous, store) = {
					let mut state = self.lock();
					match &mut *state {
						PolicyState::Enabled { tracker, store } => {
							if tracker.clear_retryable(operation, &slabs) {
								self.bump_report_generation();
							}
							tracker.clear_neutral(operation, &slabs);
							(tracker.deterministic.get(&key).cloned(), Arc::clone(store))
						}
						PolicyState::Disabled { .. } => {
							drop(state);
							reservation.finalize();
							return false;
						}
					}
				};
				let receipt =
					match DeterministicReceipt::after_failure(previous.as_ref(), now_ms, detail) {
						Ok(receipt) => receipt,
						Err(error) => {
							self.disable(format!(
								"deterministic receipt could not be computed: {error}"
							));
							reservation.finalize();
							return false;
						}
					};
				if let Err(error) = store
					.put_maintenance_receipt(key.encode(), receipt.encode())
					.await
				{
					self.disable(format!(
						"deterministic receipt could not be persisted: {error}"
					));
					reservation.finalize();
					return false;
				}
				let quarantined = receipt.is_quarantined();
				if quarantined {
					tracing::error!(
						operation = operation.label(),
						slabs = ?slabs,
						origin = ?origin,
						reason = origin.reason().label(),
						detail,
						"maintenance work quarantined after its attempt budget; \
						 requeue clears it"
					);
				}
				let mut state = self.lock();
				if let PolicyState::Enabled { tracker, .. } = &mut *state {
					tracker.deterministic.insert(key, receipt);
					self.bump_report_generation();
				}
				quarantined
			}
		};
		reservation.finalize();
		self.publish_gauges();
		quarantined
	}

	/// Operator requeue: clear the quarantine on ONE operation over one exact slab
	/// set so it re-enters the rotation with a fresh budget for the origins that
	/// were quarantined. Origins merely waiting keep their schedule, and other
	/// operations' quarantines over the same slabs stand: a fixed LSM rewrite is no
	/// evidence about a retention defect, and the operator names the operation the
	/// report showed. Durable removal first; memory after; a store failure leaves
	/// both intact and disables admission.
	pub(crate) async fn requeue(
		self: &Arc<Self>,
		operation: MaintenanceOperation,
		slabs: &[SlabId],
		expected: MaintenanceFingerprint,
	) -> Result<usize, RequeueRefusal> {
		let canonical = MaintenanceTracker::canonical(slabs);
		let prepared = self.prepare_requeue(operation, canonical, expected)?;
		self.commit_requeue(prepared).await
	}

	/// Step 3 of ADR 0582's compare-and-clear sequence. Validation and
	/// reservation are one critical section, so no maintenance body over the slab
	/// set can enter after the reviewed fingerprint is accepted.
	fn prepare_requeue(
		self: &Arc<Self>,
		operation: MaintenanceOperation,
		canonical: Vec<SlabId>,
		expected: MaintenanceFingerprint,
	) -> Result<PreparedRequeue, RequeueRefusal> {
		let (keys, store, reservation) = {
			let mut state = self.lock();
			let PolicyState::Enabled { tracker, store } = &mut *state else {
				let PolicyState::Disabled { diagnostic } = &*state else {
					unreachable!("state is one of two variants");
				};
				return Err(RequeueRefusal::PolicyDisabled(diagnostic.clone()));
			};
			if tracker.in_flight.contains_key(&canonical) {
				return Err(RequeueRefusal::InFlight);
			}
			let entries = tracker
				.deterministic
				.iter()
				.filter(|(key, receipt)| {
					key.operation() == operation
						&& key.slabs() == canonical.as_slice()
						&& receipt.is_quarantined()
				})
				.map(|(key, receipt)| (key.clone(), receipt.clone()))
				.collect::<Vec<_>>();
			if entries.is_empty() {
				return Err(RequeueRefusal::NotQuarantined);
			}
			let fingerprint_entries = entries
				.iter()
				.map(|(key, receipt)| (key.origin(), receipt))
				.collect::<Vec<_>>();
			let current = quarantine_fingerprint(operation, &canonical, &fingerprint_entries);
			if current != expected {
				return Err(RequeueRefusal::StaleReport {
					quarantined_origins: entries.len(),
				});
			}
			let keys = entries.into_iter().map(|(key, _)| key).collect::<Vec<_>>();
			tracker.in_flight.insert(canonical.clone(), operation);
			self.bump_report_generation();
			let reservation = MaintenanceReservation {
				policy: Arc::clone(self),
				operation,
				slabs: canonical.clone(),
				finalized: false,
			};
			(keys, Arc::clone(store), reservation)
		};
		Ok(PreparedRequeue {
			operation,
			canonical,
			keys,
			store,
			reservation,
		})
	}

	/// Steps 4 and 5: delete durably, then clear memory and release.
	async fn commit_requeue(
		self: &Arc<Self>,
		prepared: PreparedRequeue,
	) -> Result<usize, RequeueRefusal> {
		let PreparedRequeue {
			operation,
			canonical,
			keys,
			store,
			reservation,
		} = prepared;
		let encoded = keys.iter().map(MaintenanceWorkKey::encode).collect();
		if let Err(error) = store.delete_maintenance_receipts(encoded).await {
			let diagnostic = format!("requeue could not remove the durable receipt: {error}");
			self.disable(diagnostic.clone());
			reservation.finalize();
			return Err(RequeueRefusal::StoreFailure(diagnostic));
		}
		let mut state = self.lock();
		if let PolicyState::Enabled { tracker, .. } = &mut *state {
			for key in &keys {
				tracker.deterministic.remove(key);
			}
		}
		self.bump_report_generation();
		drop(state);
		reservation.finalize();
		tracing::info!(
			operation = operation.label(),
			slabs = ?canonical,
			origins = keys.len(),
			"maintenance work requeued"
		);
		self.publish_gauges();
		Ok(keys.len())
	}

	/// The reporting half.
	pub(crate) fn report(&self) -> MaintenancePolicyReport {
		match &*self.lock() {
			PolicyState::Disabled { diagnostic } => MaintenancePolicyReport {
				enabled: false,
				diagnostic: Some(diagnostic.clone()),
				receipts: Vec::new(),
				retryable: Vec::new(),
				in_flight: Vec::new(),
			},
			PolicyState::Enabled { tracker, .. } => MaintenancePolicyReport {
				enabled: true,
				diagnostic: None,
				receipts: tracker
					.deterministic
					.iter()
					.map(|(key, receipt)| MaintenanceReceiptView {
						operation: key.operation(),
						slabs: key.slabs().to_vec(),
						origin: key.origin(),
						reason: key.reason(),
						attempts: receipt.attempts(),
						last_failure_ms: receipt.last_failure_ms,
						next_eligible_at_ms: receipt.next_eligible_at_ms,
						detail: receipt.detail().to_owned(),
					})
					.collect(),
				retryable: tracker
					.retryable
					.iter()
					.flat_map(|(operation, per_set)| {
						per_set
							.iter()
							.map(move |(slabs, cooldown)| (*operation, slabs, cooldown))
					})
					.map(|(operation, slabs, cooldown)| RetryableCooldownView {
						operation,
						slabs: slabs.clone(),
						consecutive_failures: cooldown.failures,
						next_eligible_at_ms: cooldown.next_eligible_at_ms,
					})
					.collect(),
				in_flight: tracker.in_flight.keys().cloned().collect(),
			},
		}
	}

	fn gauge_values(&self) -> (bool, u64) {
		match &*self.lock() {
			PolicyState::Disabled { .. } => (false, 0),
			PolicyState::Enabled { tracker, .. } => (
				true,
				tracker
					.deterministic
					.values()
					.filter(|receipt| receipt.is_quarantined())
					.count() as u64,
			),
		}
	}

	fn publish_gauges_locked(&self) -> (u64, u64) {
		let (enabled, quarantined) = self.gauge_values();
		let metrics = crate::metrics::global();
		metrics.set_maintenance_policy_state(enabled, quarantined);
		(
			metrics.maintenance_policy_enabled.get(),
			metrics.maintenance_quarantined_work.get(),
		)
	}

	/// Gauges are set from the state, never incremented alongside it, so they
	/// cannot drift from what the accessor reports.
	pub(crate) fn publish_gauges(&self) {
		#[cfg(test)]
		let _serial = maintenance_gauge_test_lock()
			.lock()
			.expect("maintenance gauge test lock poisoned");
		let _ = self.publish_gauges_locked();
	}

	/// Publish this policy and read the two process-global gauges as one serialized
	/// test operation. Every test-build publisher takes the same lock, so another
	/// concurrently hydrated policy cannot overwrite the values between the set and
	/// the assertions.
	#[cfg(test)]
	pub(crate) fn gauge_snapshot_for_test(&self) -> (u64, u64) {
		let _serial = maintenance_gauge_test_lock()
			.lock()
			.expect("maintenance gauge test lock poisoned");
		self.publish_gauges_locked()
	}
}

#[cfg(test)]
fn maintenance_gauge_test_lock() -> &'static std::sync::Mutex<()> {
	static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
	LOCK.get_or_init(|| std::sync::Mutex::new(()))
}

/// Bounded label for the attempt counter. One value per way an attempt can end,
/// including the ways it can end without starting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MaintenanceAttemptResult {
	Ok,
	/// The job finished without rewriting: nothing to prune, or a candidate
	/// replaced before its claim. Neither evidence for nor against the receipts.
	CompletedWithoutRewrite,
	Retryable,
	Deterministic,
	Quarantined,
	SkippedInFlight,
	SkippedNotYetEligible,
	/// ADR 0098 decision 2: the set's last scan found nothing and its cooldown
	/// has not passed. The reporting half of the neutral cooldown.
	SkippedNeutralCooldown,
	SkippedQuarantined,
	SkippedPolicyDisabled,
	SkippedClockUnavailable,
	SkippedEmptyWorkSet,
}

impl MaintenanceAttemptResult {
	pub const ALL: [Self; 12] = [
		Self::Ok,
		Self::CompletedWithoutRewrite,
		Self::Retryable,
		Self::Deterministic,
		Self::Quarantined,
		Self::SkippedInFlight,
		Self::SkippedNotYetEligible,
		Self::SkippedNeutralCooldown,
		Self::SkippedQuarantined,
		Self::SkippedPolicyDisabled,
		Self::SkippedClockUnavailable,
		Self::SkippedEmptyWorkSet,
	];

	pub const fn label(self) -> &'static str {
		match self {
			Self::Ok => "ok",
			Self::CompletedWithoutRewrite => "completed_without_rewrite",
			Self::Retryable => "retryable_failure",
			Self::Deterministic => "deterministic_failure",
			Self::Quarantined => "quarantined",
			Self::SkippedInFlight => "skipped_in_flight",
			Self::SkippedNotYetEligible => "skipped_not_yet_eligible",
			Self::SkippedNeutralCooldown => "skipped_neutral_cooldown",
			Self::SkippedQuarantined => "skipped_quarantined",
			Self::SkippedPolicyDisabled => "skipped_policy_disabled",
			Self::SkippedClockUnavailable => "skipped_clock_unavailable",
			Self::SkippedEmptyWorkSet => "skipped_empty_work_set",
		}
	}

	pub(crate) fn skipped(refusal: &AdmissionRefusal) -> Self {
		match refusal {
			AdmissionRefusal::PolicyDisabled(_) => Self::SkippedPolicyDisabled,
			AdmissionRefusal::AlreadyInFlight => Self::SkippedInFlight,
			AdmissionRefusal::NotYetEligible => Self::SkippedNotYetEligible,
			AdmissionRefusal::NeutralCooldown => Self::SkippedNeutralCooldown,
			AdmissionRefusal::Quarantined => Self::SkippedQuarantined,
			AdmissionRefusal::ClockUnavailable => Self::SkippedClockUnavailable,
			AdmissionRefusal::EmptyWorkSet => Self::SkippedEmptyWorkSet,
		}
	}
}
