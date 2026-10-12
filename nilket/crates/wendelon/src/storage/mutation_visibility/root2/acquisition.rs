//! Stage S2a: bounded acquisition of the locally committed Control prefix `0..=c`, with `c`
//! captured from the node's `last_applied` (a committed lower bound, not the tip, not quorum
//! currentness). Each batch is read by an exact bounded range, size-checked, followed by a
//! boundary re-read and a terminal capture of `applied` then `retention`, validated by T-S1, and
//! only then folded. The result is a report, not a capability: nothing here mints, adapts,
//! reduces or authorizes, and the module has no caller.
//!
//! The byte caps are encoded-size rejection thresholds, not a memory bound: a batch is cloned
//! before it is measured, the boundary entry is cloned before it is measured, and T-S1 encodes
//! both boundary payloads in the last batch. The validation cut is the last batch's sequential
//! terminal samples; later changes belong to the caller's readiness checks. A fold that is not a
//! pure, privately owned value fold gets no isolation guarantee from this signature.
use crate::consensus::{RetentionStatus, WendelonTypeConfig};
use crate::storage::mutation_visibility::root2::prefix::{PrefixError2, check_committed_prefix2};
use openraft::storage::RaftLogReader;
use openraft::{Entry, LogId, StorageError};

#[derive(Debug, Clone, Copy)]
pub(in crate::storage::mutation_visibility::root2) struct AcquisitionCaps2 {
	pub max_entries: u64,
	pub max_batch_bytes: u64,
	pub max_total_bytes: u64,
}

pub(in crate::storage::mutation_visibility::root2) trait AcquisitionSources2 {
	fn retention(&self) -> RetentionStatus;
	fn applied(&self) -> Result<Option<LogId<u32>>, SourceClosed2>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct SourceClosed2;

/// A fold's refusal. Carries nothing, so no accumulator or provisional content leaves through it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct FoldError2;

#[derive(Debug)]
pub(in crate::storage::mutation_visibility::root2) enum AcquisitionError2 {
	CapsUnset,
	SourceClosed,
	NoCommittedEntry,
	Overflow,
	Store(StorageError<u32>),
	Coverage { from: u64, end: u64, got: u64 },
	Encode,
	BatchBytes { from: u64 },
	TotalBytes { from: u64 },
	BoundaryBytes { from: u64 },
	BoundaryUnavailable { got: u64 },
	Prefix(PrefixError2),
	Fold(FoldError2),
}

/// What was validated, as of the last terminal samples. A report: not accepted anywhere as input.
#[derive(Debug)]
pub(in crate::storage::mutation_visibility::root2) struct PrefixValidated2 {
	through: LogId<u32>,
	entries: u64,
	encoded_bytes: u64,
	terminal_applied: LogId<u32>,
	terminal_retention: RetentionStatus,
}
impl PrefixValidated2 {
	pub(in crate::storage::mutation_visibility::root2) fn through(&self) -> LogId<u32> {
		self.through
	}
	pub(in crate::storage::mutation_visibility::root2) fn entries(&self) -> u64 {
		self.entries
	}
	pub(in crate::storage::mutation_visibility::root2) fn encoded_bytes(&self) -> u64 {
		self.encoded_bytes
	}
	pub(in crate::storage::mutation_visibility::root2) fn terminal_applied(&self) -> LogId<u32> {
		self.terminal_applied
	}
	pub(in crate::storage::mutation_visibility::root2) fn terminal_retention(
		&self,
	) -> RetentionStatus {
		self.terminal_retention
	}
}

pub(in crate::storage::mutation_visibility::root2) async fn acquire_committed_prefix2<Rd, Acc>(
	reader: &mut Rd,
	sources: &impl AcquisitionSources2,
	caps: AcquisitionCaps2,
	init: impl FnOnce(&Entry<WendelonTypeConfig>) -> Result<Acc, FoldError2>,
	mut fold: impl FnMut(Acc, &[Entry<WendelonTypeConfig>]) -> Result<Acc, FoldError2>,
) -> Result<(Acc, PrefixValidated2), AcquisitionError2>
where
	Rd: RaftLogReader<WendelonTypeConfig>,
{
	use AcquisitionError2 as E;
	if caps.max_entries == 0 || caps.max_batch_bytes == 0 || caps.max_total_bytes == 0 {
		return Err(E::CapsUnset);
	}
	let c = sources
		.applied()
		.map_err(|_| E::SourceClosed)?
		.ok_or(E::NoCommittedEntry)?;
	let count = c.index.checked_add(1).ok_or(E::Overflow)?;
	let start = sources.retention();
	let (mut from, mut total) = (0u64, 0u64);
	let mut init = Some(init);
	let mut acc = None;
	loop {
		let (end, want) = next_range(from, caps.max_entries, c.index)?;
		let batch = reader
			.try_get_log_entries(from..=end)
			.await
			.map_err(E::Store)?;
		let got = u64::try_from(batch.len()).map_err(|_| E::Overflow)?;
		if got != want {
			return Err(E::Coverage { from, end, got });
		}
		let mut batch_bytes = 0;
		for entry in &batch {
			batch_bytes = add_bytes(batch_bytes, encoded_size(entry)?)?;
		}
		if batch_bytes > caps.max_batch_bytes {
			return Err(E::BatchBytes { from });
		}
		let next_total = add_bytes(total, batch_bytes)?;
		if next_total > caps.max_total_bytes {
			return Err(E::TotalBytes { from });
		}
		let mut boundary = reader
			.try_get_log_entries(c.index..=c.index)
			.await
			.map_err(E::Store)?;
		if boundary.len() != 1 {
			let got = u64::try_from(boundary.len()).map_err(|_| E::Overflow)?;
			return Err(E::BoundaryUnavailable { got });
		}
		let Some(boundary) = boundary.pop() else {
			return Err(E::BoundaryUnavailable { got: 0 });
		};
		if add_bytes(batch_bytes, encoded_size(&boundary)?)? > caps.max_batch_bytes {
			return Err(E::BoundaryBytes { from });
		}
		// Terminal samples, after the boundary read has returned: applied first, then retention.
		let c2 = sources
			.applied()
			.map_err(|_| E::SourceClosed)?
			.ok_or(E::Prefix(PrefixError2::Regressed))?;
		let end_status = sources.retention();
		check_committed_prefix2(&start, c, &batch, from, &end_status, &boundary, c2)
			.map_err(E::Prefix)?;
		let current = match acc.take() {
			Some(current) => current,
			None => init.take().expect("initialization occurs once")(&boundary).map_err(E::Fold)?,
		};
		acc = Some(fold(current, &batch).map_err(E::Fold)?);
		total = next_total;
		if end == c.index {
			return Ok((
				acc.expect("the validated batch was folded"),
				PrefixValidated2 {
					through: c,
					entries: count,
					encoded_bytes: total,
					terminal_applied: c2,
					terminal_retention: end_status,
				},
			));
		}
		// end < c.index <= u64::MAX: exact.
		from = end + 1;
	}
}

/// `(end, want)` for the batch starting at `from`: `min()` discards any saturated sum, so `end` is
/// exact, and `end <= c_index` keeps every later successor representable.
fn next_range(from: u64, max_entries: u64, c_index: u64) -> Result<(u64, u64), AcquisitionError2> {
	let span = max_entries
		.checked_sub(1)
		.ok_or(AcquisitionError2::CapsUnset)?;
	let end = from.saturating_add(span).min(c_index);
	let want = end
		.checked_sub(from)
		.and_then(|d| d.checked_add(1))
		.ok_or(AcquisitionError2::Overflow)?;
	Ok((end, want))
}

fn encoded_size(entry: &Entry<WendelonTypeConfig>) -> Result<u64, AcquisitionError2> {
	let size =
		postcard::experimental::serialized_size(entry).map_err(|_| AcquisitionError2::Encode)?;
	u64::try_from(size).map_err(|_| AcquisitionError2::Overflow)
}

fn add_bytes(a: u64, b: u64) -> Result<u64, AcquisitionError2> {
	a.checked_add(b).ok_or(AcquisitionError2::Overflow)
}

#[cfg(test)]
pub(in crate::storage::mutation_visibility::root2) fn next_range_for_test(
	from: u64,
	max_entries: u64,
	c_index: u64,
) -> Result<(u64, u64), AcquisitionError2> {
	next_range(from, max_entries, c_index)
}

#[cfg(test)]
pub(in crate::storage::mutation_visibility::root2) fn add_bytes_for_test(
	a: u64,
	b: u64,
) -> Result<u64, AcquisitionError2> {
	add_bytes(a, b)
}
