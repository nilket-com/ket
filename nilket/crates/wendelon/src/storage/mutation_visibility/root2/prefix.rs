//! Stage T-S1: shape validation of one bounded batch of the locally committed Control prefix,
//! over values captured by the caller. Pure: no store read, no `CommittedSource2`, no adaptation,
//! no reducer and no authorization. History-shape evidence only, never quorum currentness, a
//! freshness lease, authenticated genesis or a permission.
//!
//! Limits, stated rather than checked: entries below the boundary are not compared against
//! re-reads, so an altered earlier entry is not detected (Raft's committed-prefix immutability and
//! storage integrity are assumed); and `from` is checked against the batch, not authenticated, so
//! covering `0..=captured` across batches is the acquisition's obligation.
use crate::consensus::retention::{ControlRetention, HistoryCheck};
use crate::consensus::{RetentionStatus, WendelonTypeConfig};
use openraft::{Entry, EntryPayload, LogId};
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum PrefixError2 {
	NotFull,
	Latched,
	NotChecked,
	Gap { at: u64 },
	MissingGenesis,
	BeyondCaptured,
	BoundaryMismatch,
	Regressed,
	Overflow,
	Encode,
}

/// Checks, first failure wins: both statuses `Full`; neither latched; neither `NotChecked`; a
/// non-empty batch; genesis when `from == 0`; per entry the expected index, then not beyond
/// `captured`; the boundary's whole identity and payload; then `committed_after`.
pub(in crate::storage::mutation_visibility::root2) fn check_committed_prefix2(
	retention_start: &RetentionStatus,
	captured: LogId<u32>,
	batch: &[Entry<WendelonTypeConfig>],
	from: u64,
	retention_end: &RetentionStatus,
	boundary_after: &Entry<WendelonTypeConfig>,
	committed_after: LogId<u32>,
) -> Result<(), PrefixError2> {
	use PrefixError2 as E;
	let both = [retention_start, retention_end];
	if both.iter().any(|s| s.configured != ControlRetention::Full) {
		return Err(E::NotFull);
	}
	if both.iter().any(|s| s.violation.is_some()) {
		return Err(E::Latched);
	}
	if both.iter().any(|s| s.history == HistoryCheck::NotChecked) {
		return Err(E::NotChecked);
	}
	let Some(first) = batch.first() else {
		return Err(E::Gap { at: from });
	};
	if from == 0
		&& (first.log_id != LogId::default()
			|| !matches!(first.payload, EntryPayload::Membership(_)))
	{
		return Err(E::MissingGenesis);
	}
	let mut expected = from;
	for (i, entry) in batch.iter().enumerate() {
		if entry.log_id.index != expected {
			return Err(E::Gap { at: expected });
		}
		if entry.log_id.index > captured.index {
			return Err(E::BeyondCaptured);
		}
		// The successor is needed only when another entry follows: a final entry at u64::MAX is
		// checkable.
		if i + 1 < batch.len() {
			expected = expected.checked_add(1).ok_or(E::Overflow)?;
		}
	}
	if boundary_after.log_id != captured {
		return Err(E::BoundaryMismatch);
	}
	// Contiguous from `from`, so the boundary is in the batch exactly when its offset is in range.
	// (The loop already established `from <= captured.index`; the subtraction stays checked.)
	let offset = captured
		.index
		.checked_sub(from)
		.and_then(|o| usize::try_from(o).ok());
	if let Some(copy) = offset.and_then(|o| batch.get(o)) {
		if copy.log_id != captured {
			return Err(E::BoundaryMismatch);
		}
		if encode(&copy.payload)? != encode(&boundary_after.payload)? {
			return Err(E::BoundaryMismatch);
		}
	}
	if committed_after.index < captured.index
		|| (committed_after.index == captured.index && committed_after != captured)
	{
		return Err(E::Regressed);
	}
	Ok(())
}

fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, PrefixError2> {
	postcard::to_allocvec(value).map_err(|_| PrefixError2::Encode)
}

#[cfg(test)]
pub(in crate::storage::mutation_visibility::root2) fn encode_for_test<T: Serialize>(
	value: &T,
) -> Result<Vec<u8>, PrefixError2> {
	encode(value)
}
