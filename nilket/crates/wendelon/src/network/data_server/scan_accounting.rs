//! Per-request accounting for the whole-record scan.
//!
//! ADR 0089 made one physical read and one decode serve every row of a packed
//! record, and ADR 077's root cap is what keeps that machinery from accreting
//! back into the dispatch root. Three things live here because they answer the
//! same question — what did this one scan actually do: the decode audit that
//! makes the amortization checkable, the bounded cache that produces it, and
//! the sampled phase trace that attributes the time the amortization left
//! behind.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;

use crate::schema::SlabId;
use crate::storage::core::RecordLocation;

/// ADR 0089: per-request accounting for the whole-record scan's physical
/// reads. Rows of one packed record share a read and a decode, so the audit
/// counts distinct physical records — the amortization the packing exists to
/// buy — and the cache's high-water mark, so bounded memory is checkable.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ScanDecodeAudit {
	pub(crate) distinct_records_decoded: u64,
	pub(crate) coalesced_hits: u64,
	pub(crate) max_resident_records: u64,
}

// `QueryRequestMeta` is the shared internal scan envelope. Its first padding
// byte already carries the stats-pruning control; this private bit carries a
// verified sampled TableQuery carrier through to the shared scan planner.
pub(crate) const QUERY_REQUEST_FLAG_SAMPLED_POST_PACKING_PHASE_TRACE: u8 = 0b0000_0010;

/// A request-local attribution trace for the post-ADR-0089 scan. The original
/// profile's dominant read/decode phases are now amortized per physical record,
/// so it cannot identify the owner of the remaining per-index-entry budget.
///
/// This is deliberately enabled only by a valid sampled table-query trace
/// carrier. Normal requests pay one disabled branch, not an `Instant` per row.
#[derive(Default)]
pub(crate) struct PostPackingScanPhaseTrace {
	enabled: bool,
	index_entries: Duration,
	stats_prune: Duration,
	projected_read: Duration,
	cache_lookup: Duration,
	physical_read: Duration,
	decode: Duration,
	placement: Duration,
	synthetic_probe: Duration,
	materialize: Duration,
	descriptor_match: Duration,
	pbc: Duration,
	accumulator_push: Duration,
}

#[derive(Clone, Copy)]
pub(crate) enum ScanPhase {
	IndexEntries,
	StatsPrune,
	ProjectedRead,
	CacheLookup,
	PhysicalRead,
	Decode,
	Placement,
	SyntheticProbe,
	Materialize,
	DescriptorMatch,
	Pbc,
	AccumulatorPush,
}

impl PostPackingScanPhaseTrace {
	pub(crate) fn new(enabled: bool) -> Self {
		Self {
			enabled,
			..Self::default()
		}
	}

	pub(crate) fn started(&self) -> Option<Instant> {
		self.enabled.then(Instant::now)
	}

	pub(crate) fn record(&mut self, phase: ScanPhase, started: Option<Instant>) {
		let Some(started) = started else {
			return;
		};
		let elapsed = started.elapsed();
		let target = match phase {
			ScanPhase::IndexEntries => &mut self.index_entries,
			ScanPhase::StatsPrune => &mut self.stats_prune,
			ScanPhase::ProjectedRead => &mut self.projected_read,
			ScanPhase::CacheLookup => &mut self.cache_lookup,
			ScanPhase::PhysicalRead => &mut self.physical_read,
			ScanPhase::Decode => &mut self.decode,
			ScanPhase::Placement => &mut self.placement,
			ScanPhase::SyntheticProbe => &mut self.synthetic_probe,
			ScanPhase::Materialize => &mut self.materialize,
			ScanPhase::DescriptorMatch => &mut self.descriptor_match,
			ScanPhase::Pbc => &mut self.pbc,
			ScanPhase::AccumulatorPush => &mut self.accumulator_push,
		};
		*target = target.saturating_add(elapsed);
	}

	pub(crate) fn emit(&self, rows: u64, scanned_locations: usize) {
		if !self.enabled {
			return;
		}
		tracing::info!(
			rows,
			scanned_locations,
			index_entries_micros = self.index_entries.as_micros(),
			stats_prune_micros = self.stats_prune.as_micros(),
			projected_read_micros = self.projected_read.as_micros(),
			cache_lookup_micros = self.cache_lookup.as_micros(),
			physical_read_micros = self.physical_read.as_micros(),
			decode_micros = self.decode.as_micros(),
			placement_micros = self.placement.as_micros(),
			synthetic_probe_micros = self.synthetic_probe.as_micros(),
			materialize_micros = self.materialize.as_micros(),
			descriptor_match_micros = self.descriptor_match.as_micros(),
			pbc_micros = self.pbc.as_micros(),
			accumulator_push_micros = self.accumulator_push.as_micros(),
			"sampled table query post-packing phase trace"
		);
	}
}

/// ADR 0089: the scan's bounded cache of decoded records. Locality-index
/// entries for one packed record interleave with other records sharing the
/// prefix, so a single-slot cache would thrash; a small bounded set is kept and
/// the oldest entry evicted. A scan's memory must never grow with its size.
pub(crate) const SCAN_DECODED_CACHE_MAX_RECORDS: usize = 8;
pub(crate) const SCAN_DECODED_CACHE_MAX_BYTES: usize = 32 << 20;

pub(crate) struct DecodedRecordCache {
	entries: Vec<((SlabId, u64), Arc<RecordBatch>, usize)>,
	resident_bytes: usize,
	pub(crate) audit: ScanDecodeAudit,
}

impl DecodedRecordCache {
	pub(crate) fn new() -> Self {
		Self {
			entries: Vec::new(),
			resident_bytes: 0,
			audit: ScanDecodeAudit::default(),
		}
	}

	pub(crate) fn get(&mut self, location: RecordLocation) -> Option<Arc<RecordBatch>> {
		let key = (location.slab_id, location.offset);
		let hit = self
			.entries
			.iter()
			.find(|(entry_key, _, _)| *entry_key == key)
			.map(|(_, batch, _)| Arc::clone(batch));
		if hit.is_some() {
			self.audit.coalesced_hits = self.audit.coalesced_hits.saturating_add(1);
		}
		hit
	}

	pub(crate) fn insert(&mut self, location: RecordLocation, batch: Arc<RecordBatch>) {
		self.audit.distinct_records_decoded = self.audit.distinct_records_decoded.saturating_add(1);
		let bytes = batch.get_array_memory_size();
		self.entries
			.push(((location.slab_id, location.offset), batch, bytes));
		self.resident_bytes = self.resident_bytes.saturating_add(bytes);
		while self.entries.len() > SCAN_DECODED_CACHE_MAX_RECORDS
			|| self.resident_bytes > SCAN_DECODED_CACHE_MAX_BYTES
		{
			if self.entries.len() <= 1 {
				break;
			}
			let (_, _, evicted) = self.entries.remove(0);
			self.resident_bytes = self.resident_bytes.saturating_sub(evicted);
		}
		self.audit.max_resident_records = self
			.audit
			.max_resident_records
			.max(self.entries.len() as u64);
	}
}
