use super::spill::PinDecodedPagesOutcome;
use crate::error::ExecutionError;
use serde::Serialize;
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub(in crate::exec) const STREAMED_SPILLED_LEFT_OUTER_PROFILE_ENV: &str =
	"TORON_STREAMED_SPILLED_LEFT_OUTER_PROFILE_PATH";
const PROFILE_SCHEMA_VERSION: u32 = 2;

fn profile_error(message: impl Into<String>) -> ExecutionError {
	ExecutionError::Kernel {
		message: format!(
			"streamed spilled left outer throughput profile: {}",
			message.into()
		),
	}
}

fn add_counter(counter: &mut u64, delta: u64, overflowed: &mut bool) {
	match counter.checked_add(delta) {
		Some(total) => *counter = total,
		None => {
			*counter = u64::MAX;
			*overflowed = true;
		}
	}
}

fn duration_micros(duration: Duration) -> u64 {
	u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

#[derive(Clone, Copy, Debug, Default)]
pub(in crate::exec) struct SpillCacheEvictions {
	pub(in crate::exec) plaintext_entries: u64,
	pub(in crate::exec) plaintext_bytes: u64,
	pub(in crate::exec) decoded_entries: u64,
	pub(in crate::exec) decoded_bytes: u64,
}

impl SpillCacheEvictions {
	pub(in crate::exec) fn add(&mut self, other: Self) {
		self.plaintext_entries = self
			.plaintext_entries
			.saturating_add(other.plaintext_entries);
		self.plaintext_bytes = self.plaintext_bytes.saturating_add(other.plaintext_bytes);
		self.decoded_entries = self.decoded_entries.saturating_add(other.decoded_entries);
		self.decoded_bytes = self.decoded_bytes.saturating_add(other.decoded_bytes);
	}
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::exec) enum ThroughputOperationPhase {
	Preload,
	#[default]
	Probe,
}

#[derive(Debug, Default, Serialize)]
struct OperationPhaseSnapshot {
	plaintext_hits: u64,
	plaintext_misses: u64,
	plaintext_evictions: u64,
	plaintext_evicted_bytes: u64,
	decoded_hits: u64,
	decoded_misses: u64,
	decoded_evictions: u64,
	decoded_evicted_bytes: u64,
	file_read_calls: u64,
	file_read_completions: u64,
	file_read_pending_calls: u64,
	file_read_requested_bytes: u64,
	file_read_returned_bytes: u64,
	file_read_micros: u64,
	page_decodes: u64,
	page_decode_micros: u64,
}

#[derive(Debug, Serialize)]
struct PinReceiptSnapshot {
	memory_cap_bytes: u64,
	retained_cap_bytes: u64,
	resident_baseline_bytes: u64,
	resident_index_bytes: u64,
	writer_peak_bytes: u64,
	page_reserve_bytes: u64,
	loaded_page_estimate_bytes: u64,
	preflight_estimate_bytes: u64,
	decoded_bytes: u64,
	pages_attempted: u64,
	pages_completed: u64,
	outcome: PinDecodedPagesOutcome,
}

#[derive(Debug)]
pub(in crate::exec) struct StreamedSpilledLeftOuterProfile {
	path: PathBuf,
	lookup_calls: u64,
	matched_references: u64,
	probe_chunks: u64,
	probe_rows: u64,
	probe_chunk_unique_page_requests: u64,
	max_unique_pages_per_probe_chunk: u64,
	plaintext_hits: u64,
	plaintext_misses: u64,
	plaintext_promotions_to_decoded: u64,
	plaintext_promoted_bytes: u64,
	plaintext_evictions: u64,
	plaintext_evicted_bytes: u64,
	plaintext_resident_bytes: u64,
	plaintext_peak_resident_bytes: u64,
	decoded_hits: u64,
	decoded_misses: u64,
	decoded_evictions: u64,
	decoded_evicted_bytes: u64,
	decoded_resident_bytes: u64,
	decoded_peak_resident_bytes: u64,
	decoded_page_bytes: BTreeMap<usize, u64>,
	file_read_calls: u64,
	file_read_completions: u64,
	file_read_pending_calls: u64,
	file_read_requested_bytes: u64,
	file_read_returned_bytes: u64,
	file_read_micros: u64,
	page_decodes: u64,
	page_decode_micros: u64,
	materialized_batches: u64,
	materialized_rows: u64,
	materialized_unique_page_requests: u64,
	grouped_fold_batches: u64,
	grouped_fold_rows: u64,
	grouped_fold_micros: u64,
	build_micros: u64,
	probe_micros: u64,
	operation_phase: ThroughputOperationPhase,
	preload_operations: OperationPhaseSnapshot,
	probe_operations: OperationPhaseSnapshot,
	pin_receipt: Option<PinReceiptSnapshot>,
	counter_overflowed: bool,
}

impl StreamedSpilledLeftOuterProfile {
	pub(in crate::exec) fn from_env() -> Result<Option<Self>, ExecutionError> {
		Self::from_lookup(|name| std::env::var_os(name))
	}

	fn from_lookup(
		lookup: impl FnOnce(&str) -> Option<OsString>,
	) -> Result<Option<Self>, ExecutionError> {
		let Some(raw_path) = lookup(STREAMED_SPILLED_LEFT_OUTER_PROFILE_ENV) else {
			return Ok(None);
		};
		if raw_path.is_empty() {
			return Ok(None);
		}
		let path = PathBuf::from(raw_path);
		if path.file_name().is_none() {
			return Err(profile_error("profile path must name a file"));
		}
		Ok(Some(Self {
			path,
			lookup_calls: 0,
			matched_references: 0,
			probe_chunks: 0,
			probe_rows: 0,
			probe_chunk_unique_page_requests: 0,
			max_unique_pages_per_probe_chunk: 0,
			plaintext_hits: 0,
			plaintext_misses: 0,
			plaintext_promotions_to_decoded: 0,
			plaintext_promoted_bytes: 0,
			plaintext_evictions: 0,
			plaintext_evicted_bytes: 0,
			plaintext_resident_bytes: 0,
			plaintext_peak_resident_bytes: 0,
			decoded_hits: 0,
			decoded_misses: 0,
			decoded_evictions: 0,
			decoded_evicted_bytes: 0,
			decoded_resident_bytes: 0,
			decoded_peak_resident_bytes: 0,
			decoded_page_bytes: BTreeMap::new(),
			file_read_calls: 0,
			file_read_completions: 0,
			file_read_pending_calls: 0,
			file_read_requested_bytes: 0,
			file_read_returned_bytes: 0,
			file_read_micros: 0,
			page_decodes: 0,
			page_decode_micros: 0,
			materialized_batches: 0,
			materialized_rows: 0,
			materialized_unique_page_requests: 0,
			grouped_fold_batches: 0,
			grouped_fold_rows: 0,
			grouped_fold_micros: 0,
			build_micros: 0,
			probe_micros: 0,
			operation_phase: ThroughputOperationPhase::Probe,
			preload_operations: OperationPhaseSnapshot::default(),
			probe_operations: OperationPhaseSnapshot::default(),
			pin_receipt: None,
			counter_overflowed: false,
		}))
	}

	#[cfg(test)]
	pub(in crate::exec) fn at_test_path(path: PathBuf) -> Self {
		Self::from_lookup(|_| Some(path.into_os_string()))
			.expect("test throughput profile path")
			.expect("test throughput profile enabled")
	}

	fn record_operation(&mut self, record: impl FnOnce(&mut OperationPhaseSnapshot, &mut bool)) {
		let mut overflowed = false;
		let operations = match self.operation_phase {
			ThroughputOperationPhase::Preload => &mut self.preload_operations,
			ThroughputOperationPhase::Probe => &mut self.probe_operations,
		};
		record(operations, &mut overflowed);
		self.counter_overflowed |= overflowed;
	}

	pub(in crate::exec) fn set_operation_phase(&mut self, phase: ThroughputOperationPhase) {
		self.operation_phase = phase;
	}

	#[allow(clippy::too_many_arguments)]
	pub(in crate::exec) fn record_pin_receipt(
		&mut self,
		memory_cap_bytes: u64,
		retained_cap_bytes: u64,
		resident_baseline_bytes: u64,
		resident_index_bytes: u64,
		writer_peak_bytes: u64,
		page_reserve_bytes: u64,
		loaded_page_estimate_bytes: u64,
		preflight_estimate_bytes: u64,
		decoded_bytes: u64,
		pages_attempted: u64,
		pages_completed: u64,
		outcome: PinDecodedPagesOutcome,
	) {
		self.pin_receipt = Some(PinReceiptSnapshot {
			memory_cap_bytes,
			retained_cap_bytes,
			resident_baseline_bytes,
			resident_index_bytes,
			writer_peak_bytes,
			page_reserve_bytes,
			loaded_page_estimate_bytes,
			preflight_estimate_bytes,
			decoded_bytes,
			pages_attempted,
			pages_completed,
			outcome,
		});
	}

	pub(in crate::exec) fn record_lookup(&mut self, matched_references: usize) {
		add_counter(&mut self.lookup_calls, 1, &mut self.counter_overflowed);
		add_counter(
			&mut self.matched_references,
			u64::try_from(matched_references).unwrap_or(u64::MAX),
			&mut self.counter_overflowed,
		);
	}

	pub(in crate::exec) fn record_probe_chunk(
		&mut self,
		probe_rows: usize,
		unique_page_requests: usize,
	) {
		let probe_rows = u64::try_from(probe_rows).unwrap_or(u64::MAX);
		let unique_page_requests = u64::try_from(unique_page_requests).unwrap_or(u64::MAX);
		add_counter(&mut self.probe_chunks, 1, &mut self.counter_overflowed);
		add_counter(
			&mut self.probe_rows,
			probe_rows,
			&mut self.counter_overflowed,
		);
		add_counter(
			&mut self.probe_chunk_unique_page_requests,
			unique_page_requests,
			&mut self.counter_overflowed,
		);
		self.max_unique_pages_per_probe_chunk = self
			.max_unique_pages_per_probe_chunk
			.max(unique_page_requests);
	}

	pub(in crate::exec) fn record_plaintext_hit(&mut self) {
		add_counter(&mut self.plaintext_hits, 1, &mut self.counter_overflowed);
		self.record_operation(|operations, overflowed| {
			add_counter(&mut operations.plaintext_hits, 1, overflowed);
		});
	}

	pub(in crate::exec) fn record_plaintext_miss(&mut self) {
		add_counter(&mut self.plaintext_misses, 1, &mut self.counter_overflowed);
		self.record_operation(|operations, overflowed| {
			add_counter(&mut operations.plaintext_misses, 1, overflowed);
		});
	}

	pub(in crate::exec) fn record_decoded_hit(&mut self) {
		add_counter(&mut self.decoded_hits, 1, &mut self.counter_overflowed);
		self.record_operation(|operations, overflowed| {
			add_counter(&mut operations.decoded_hits, 1, overflowed);
		});
	}

	pub(in crate::exec) fn record_decoded_miss(&mut self) {
		add_counter(&mut self.decoded_misses, 1, &mut self.counter_overflowed);
		self.record_operation(|operations, overflowed| {
			add_counter(&mut operations.decoded_misses, 1, overflowed);
		});
	}

	pub(in crate::exec) fn record_plaintext_insert(&mut self, bytes: u64) {
		add_counter(
			&mut self.plaintext_resident_bytes,
			bytes,
			&mut self.counter_overflowed,
		);
		self.plaintext_peak_resident_bytes = self
			.plaintext_peak_resident_bytes
			.max(self.plaintext_resident_bytes);
	}

	pub(in crate::exec) fn record_decoded_insert(&mut self, bytes: u64) {
		add_counter(
			&mut self.decoded_resident_bytes,
			bytes,
			&mut self.counter_overflowed,
		);
		self.decoded_peak_resident_bytes = self
			.decoded_peak_resident_bytes
			.max(self.decoded_resident_bytes);
	}

	pub(in crate::exec) fn record_plaintext_promotion(&mut self, bytes: u64) {
		if bytes == 0 {
			return;
		}
		add_counter(
			&mut self.plaintext_promotions_to_decoded,
			1,
			&mut self.counter_overflowed,
		);
		add_counter(
			&mut self.plaintext_promoted_bytes,
			bytes,
			&mut self.counter_overflowed,
		);
		self.plaintext_resident_bytes = self.plaintext_resident_bytes.saturating_sub(bytes);
	}

	pub(in crate::exec) fn record_cache_evictions(&mut self, evictions: SpillCacheEvictions) {
		add_counter(
			&mut self.plaintext_evictions,
			evictions.plaintext_entries,
			&mut self.counter_overflowed,
		);
		add_counter(
			&mut self.plaintext_evicted_bytes,
			evictions.plaintext_bytes,
			&mut self.counter_overflowed,
		);
		add_counter(
			&mut self.decoded_evictions,
			evictions.decoded_entries,
			&mut self.counter_overflowed,
		);
		add_counter(
			&mut self.decoded_evicted_bytes,
			evictions.decoded_bytes,
			&mut self.counter_overflowed,
		);
		self.plaintext_resident_bytes = self
			.plaintext_resident_bytes
			.saturating_sub(evictions.plaintext_bytes);
		self.decoded_resident_bytes = self
			.decoded_resident_bytes
			.saturating_sub(evictions.decoded_bytes);
		self.record_operation(|operations, overflowed| {
			add_counter(
				&mut operations.plaintext_evictions,
				evictions.plaintext_entries,
				overflowed,
			);
			add_counter(
				&mut operations.plaintext_evicted_bytes,
				evictions.plaintext_bytes,
				overflowed,
			);
			add_counter(
				&mut operations.decoded_evictions,
				evictions.decoded_entries,
				overflowed,
			);
			add_counter(
				&mut operations.decoded_evicted_bytes,
				evictions.decoded_bytes,
				overflowed,
			);
		});
	}

	pub(in crate::exec) fn record_file_read(
		&mut self,
		requested_bytes: usize,
		returned_bytes: usize,
		elapsed: Duration,
		returned_pending: bool,
	) {
		add_counter(&mut self.file_read_calls, 1, &mut self.counter_overflowed);
		add_counter(
			&mut self.file_read_completions,
			1,
			&mut self.counter_overflowed,
		);
		if returned_pending {
			add_counter(
				&mut self.file_read_pending_calls,
				1,
				&mut self.counter_overflowed,
			);
		}
		add_counter(
			&mut self.file_read_requested_bytes,
			u64::try_from(requested_bytes).unwrap_or(u64::MAX),
			&mut self.counter_overflowed,
		);
		add_counter(
			&mut self.file_read_returned_bytes,
			u64::try_from(returned_bytes).unwrap_or(u64::MAX),
			&mut self.counter_overflowed,
		);
		add_counter(
			&mut self.file_read_micros,
			duration_micros(elapsed),
			&mut self.counter_overflowed,
		);
		let requested_bytes = u64::try_from(requested_bytes).unwrap_or(u64::MAX);
		let returned_bytes = u64::try_from(returned_bytes).unwrap_or(u64::MAX);
		let elapsed_micros = duration_micros(elapsed);
		self.record_operation(|operations, overflowed| {
			add_counter(&mut operations.file_read_calls, 1, overflowed);
			add_counter(&mut operations.file_read_completions, 1, overflowed);
			if returned_pending {
				add_counter(&mut operations.file_read_pending_calls, 1, overflowed);
			}
			add_counter(
				&mut operations.file_read_requested_bytes,
				requested_bytes,
				overflowed,
			);
			add_counter(
				&mut operations.file_read_returned_bytes,
				returned_bytes,
				overflowed,
			);
			add_counter(&mut operations.file_read_micros, elapsed_micros, overflowed);
		});
	}

	pub(in crate::exec) fn record_page_decode(
		&mut self,
		page_index: usize,
		decoded_bytes: u64,
		elapsed: Duration,
	) {
		add_counter(&mut self.page_decodes, 1, &mut self.counter_overflowed);
		add_counter(
			&mut self.page_decode_micros,
			duration_micros(elapsed),
			&mut self.counter_overflowed,
		);
		if let Some(previous) = self.decoded_page_bytes.insert(page_index, decoded_bytes)
			&& previous != decoded_bytes
		{
			self.counter_overflowed = true;
		}
		let elapsed_micros = duration_micros(elapsed);
		self.record_operation(|operations, overflowed| {
			add_counter(&mut operations.page_decodes, 1, overflowed);
			add_counter(
				&mut operations.page_decode_micros,
				elapsed_micros,
				overflowed,
			);
		});
	}

	pub(in crate::exec) fn record_materialized_batch(
		&mut self,
		rows: usize,
		unique_page_requests: usize,
	) {
		add_counter(
			&mut self.materialized_batches,
			1,
			&mut self.counter_overflowed,
		);
		add_counter(
			&mut self.materialized_rows,
			u64::try_from(rows).unwrap_or(u64::MAX),
			&mut self.counter_overflowed,
		);
		add_counter(
			&mut self.materialized_unique_page_requests,
			u64::try_from(unique_page_requests).unwrap_or(u64::MAX),
			&mut self.counter_overflowed,
		);
	}

	pub(in crate::exec) fn record_grouped_fold(&mut self, rows: usize, elapsed: Duration) {
		add_counter(
			&mut self.grouped_fold_batches,
			1,
			&mut self.counter_overflowed,
		);
		add_counter(
			&mut self.grouped_fold_rows,
			u64::try_from(rows).unwrap_or(u64::MAX),
			&mut self.counter_overflowed,
		);
		add_counter(
			&mut self.grouped_fold_micros,
			duration_micros(elapsed),
			&mut self.counter_overflowed,
		);
	}

	pub(in crate::exec) fn record_phase_micros(&mut self, build_micros: u64, probe_micros: u64) {
		self.build_micros = build_micros;
		self.probe_micros = probe_micros;
	}

	pub(in crate::exec) fn persist(
		&self,
		spill_id: u64,
		spill_bytes: u64,
		page_count: usize,
		input_rows: u64,
		cache_target_bytes: u64,
	) -> Result<(), ExecutionError> {
		let decoded_working_set_bytes = self
			.decoded_page_bytes
			.values()
			.try_fold(0u64, |total, bytes| total.checked_add(*bytes))
			.ok_or_else(|| profile_error("decoded working-set byte total overflow"))?;
		let snapshot = ProfileSnapshot {
			schema_version: PROFILE_SCHEMA_VERSION,
			spill_id,
			spill_bytes,
			page_count,
			input_rows,
			cache_target_bytes,
			decoded_working_set: DecodedWorkingSetSnapshot {
				unique_pages: self.decoded_page_bytes.len(),
				bytes: decoded_working_set_bytes,
				maximum_page_bytes: self.decoded_page_bytes.values().copied().max().unwrap_or(0),
			},
			probe: ProbeSnapshot {
				lookup_calls: self.lookup_calls,
				matched_references: self.matched_references,
				probe_chunks: self.probe_chunks,
				probe_rows: self.probe_rows,
				unique_page_requests: self.probe_chunk_unique_page_requests,
				maximum_unique_pages_per_chunk: self.max_unique_pages_per_probe_chunk,
			},
			plaintext_cache: PlaintextCacheSnapshot {
				hits: self.plaintext_hits,
				misses: self.plaintext_misses,
				promotions_to_decoded: self.plaintext_promotions_to_decoded,
				promoted_bytes: self.plaintext_promoted_bytes,
				evictions: self.plaintext_evictions,
				evicted_bytes: self.plaintext_evicted_bytes,
				final_resident_bytes: self.plaintext_resident_bytes,
				peak_resident_bytes_before_eviction: self.plaintext_peak_resident_bytes,
			},
			decoded_cache: CacheSnapshot {
				hits: self.decoded_hits,
				misses: self.decoded_misses,
				evictions: self.decoded_evictions,
				evicted_bytes: self.decoded_evicted_bytes,
				final_resident_bytes: self.decoded_resident_bytes,
				peak_resident_bytes_before_eviction: self.decoded_peak_resident_bytes,
			},
			file_reads: FileReadSnapshot {
				calls: self.file_read_calls,
				completions: self.file_read_completions,
				pending_calls: self.file_read_pending_calls,
				requested_bytes: self.file_read_requested_bytes,
				returned_bytes: self.file_read_returned_bytes,
				micros: self.file_read_micros,
			},
			decode: DecodeSnapshot {
				page_decodes: self.page_decodes,
				micros: self.page_decode_micros,
			},
			materialization: MaterializationSnapshot {
				batches: self.materialized_batches,
				rows: self.materialized_rows,
				unique_page_requests: self.materialized_unique_page_requests,
			},
			grouped_fold: FoldSnapshot {
				batches: self.grouped_fold_batches,
				rows: self.grouped_fold_rows,
				micros: self.grouped_fold_micros,
			},
			phases: PhaseSnapshot {
				build_micros: self.build_micros,
				probe_micros: self.probe_micros,
			},
			operation_phases: OperationPhasesSnapshot {
				preload: &self.preload_operations,
				probe: &self.probe_operations,
			},
			pin: self.pin_receipt.as_ref(),
			counter_overflowed: self.counter_overflowed,
		};
		write_snapshot(&self.path, &snapshot)
	}
}

#[derive(Serialize)]
struct ProfileSnapshot<'a> {
	schema_version: u32,
	spill_id: u64,
	spill_bytes: u64,
	page_count: usize,
	input_rows: u64,
	cache_target_bytes: u64,
	decoded_working_set: DecodedWorkingSetSnapshot,
	probe: ProbeSnapshot,
	plaintext_cache: PlaintextCacheSnapshot,
	decoded_cache: CacheSnapshot,
	file_reads: FileReadSnapshot,
	decode: DecodeSnapshot,
	materialization: MaterializationSnapshot,
	grouped_fold: FoldSnapshot,
	phases: PhaseSnapshot,
	operation_phases: OperationPhasesSnapshot<'a>,
	pin: Option<&'a PinReceiptSnapshot>,
	counter_overflowed: bool,
}

#[derive(Serialize)]
struct OperationPhasesSnapshot<'a> {
	preload: &'a OperationPhaseSnapshot,
	probe: &'a OperationPhaseSnapshot,
}

#[derive(Serialize)]
struct DecodedWorkingSetSnapshot {
	unique_pages: usize,
	bytes: u64,
	maximum_page_bytes: u64,
}

#[derive(Serialize)]
struct ProbeSnapshot {
	lookup_calls: u64,
	matched_references: u64,
	probe_chunks: u64,
	probe_rows: u64,
	unique_page_requests: u64,
	maximum_unique_pages_per_chunk: u64,
}

#[derive(Serialize)]
struct CacheSnapshot {
	hits: u64,
	misses: u64,
	evictions: u64,
	evicted_bytes: u64,
	final_resident_bytes: u64,
	peak_resident_bytes_before_eviction: u64,
}

#[derive(Serialize)]
struct PlaintextCacheSnapshot {
	hits: u64,
	misses: u64,
	promotions_to_decoded: u64,
	promoted_bytes: u64,
	evictions: u64,
	evicted_bytes: u64,
	final_resident_bytes: u64,
	peak_resident_bytes_before_eviction: u64,
}

#[derive(Serialize)]
struct FileReadSnapshot {
	calls: u64,
	completions: u64,
	pending_calls: u64,
	requested_bytes: u64,
	returned_bytes: u64,
	micros: u64,
}

#[derive(Serialize)]
struct DecodeSnapshot {
	page_decodes: u64,
	micros: u64,
}

#[derive(Serialize)]
struct MaterializationSnapshot {
	batches: u64,
	rows: u64,
	unique_page_requests: u64,
}

#[derive(Serialize)]
struct FoldSnapshot {
	batches: u64,
	rows: u64,
	micros: u64,
}

#[derive(Serialize)]
struct PhaseSnapshot {
	build_micros: u64,
	probe_micros: u64,
}

fn write_snapshot(path: &Path, snapshot: &ProfileSnapshot<'_>) -> Result<(), ExecutionError> {
	if let Some(parent) = path.parent()
		&& !parent.as_os_str().is_empty()
	{
		std::fs::create_dir_all(parent)
			.map_err(|error| profile_error(format!("create profile directory: {error}")))?;
	}
	let bytes = serde_json::to_vec_pretty(snapshot)
		.map_err(|error| profile_error(format!("serialize snapshot: {error}")))?;
	let temporary = path.with_extension("tmp");
	std::fs::write(&temporary, bytes)
		.map_err(|error| profile_error(format!("write temporary snapshot: {error}")))?;
	std::fs::rename(&temporary, path)
		.map_err(|error| profile_error(format!("publish snapshot: {error}")))
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::sync::atomic::{AtomicU64, Ordering};
	use std::time::Duration;

	static NEXT_TEST_PATH: AtomicU64 = AtomicU64::new(1);

	fn test_path(label: &str) -> PathBuf {
		std::env::temp_dir().join(format!(
			"toron_streamed_spilled_profile_{label}_{}_{}.json",
			std::process::id(),
			NEXT_TEST_PATH.fetch_add(1, Ordering::SeqCst)
		))
	}

	#[test]
	fn disabled_profile_constructs_no_state_or_sink() {
		let path = test_path("disabled");
		let profile = StreamedSpilledLeftOuterProfile::from_lookup(|_| None)
			.expect("disabled profile lookup");
		assert!(profile.is_none());
		assert!(!path.exists());
	}

	#[test]
	fn enabled_profile_persists_exact_cache_and_operation_counts() {
		let path = test_path("enabled");
		let mut profile =
			StreamedSpilledLeftOuterProfile::from_lookup(|_| Some(path.as_os_str().to_os_string()))
				.expect("enabled profile lookup")
				.expect("enabled profile");
		profile.record_lookup(2);
		profile.record_probe_chunk(3, 1);
		profile.set_operation_phase(ThroughputOperationPhase::Preload);
		profile.record_decoded_miss();
		profile.record_plaintext_miss();
		profile.record_file_read(64, 64, Duration::from_micros(7), true);
		profile.record_plaintext_insert(48);
		profile.record_page_decode(0, 80, Duration::from_micros(11));
		profile.record_plaintext_promotion(48);
		profile.record_decoded_insert(80);
		profile.record_pin_receipt(
			1024,
			16_384,
			1,
			2,
			3,
			64,
			70,
			140,
			80,
			1,
			1,
			PinDecodedPagesOutcome::Admitted,
		);
		profile.set_operation_phase(ThroughputOperationPhase::Probe);
		profile.record_decoded_hit();
		profile.record_materialized_batch(2, 1);
		profile.record_grouped_fold(2, Duration::from_micros(13));
		profile.record_phase_micros(17, 19);
		profile.persist(1, 64, 1, 3, 128).expect("persist profile");

		let snapshot: serde_json::Value =
			serde_json::from_slice(&std::fs::read(&path).expect("read profile snapshot"))
				.expect("decode profile snapshot");
		assert_eq!(snapshot["probe"]["lookup_calls"], 1);
		assert_eq!(snapshot["probe"]["matched_references"], 2);
		assert_eq!(snapshot["file_reads"]["calls"], 1);
		assert_eq!(snapshot["file_reads"]["pending_calls"], 1);
		assert_eq!(snapshot["decoded_working_set"]["bytes"], 80);
		assert_eq!(snapshot["plaintext_cache"]["final_resident_bytes"], 0);
		assert_eq!(snapshot["plaintext_cache"]["promotions_to_decoded"], 1);
		assert_eq!(snapshot["plaintext_cache"]["evictions"], 0);
		assert_eq!(snapshot["decoded_cache"]["final_resident_bytes"], 80);
		assert_eq!(
			snapshot["operation_phases"]["preload"]["file_read_calls"],
			1
		);
		assert_eq!(snapshot["operation_phases"]["preload"]["page_decodes"], 1);
		assert_eq!(snapshot["operation_phases"]["probe"]["decoded_hits"], 1);
		assert_eq!(snapshot["operation_phases"]["probe"]["file_read_calls"], 0);
		assert_eq!(snapshot["pin"]["retained_cap_bytes"], 16_384);
		assert_eq!(snapshot["pin"]["preflight_estimate_bytes"], 140);
		assert_eq!(snapshot["pin"]["pages_completed"], 1);
		assert_eq!(snapshot["pin"]["outcome"], "admitted");
		assert_eq!(snapshot["materialization"]["rows"], 2);
		assert_eq!(snapshot["grouped_fold"]["micros"], 13);
		assert_eq!(snapshot["counter_overflowed"], false);
		std::fs::remove_file(path).expect("remove profile snapshot");
	}
}
