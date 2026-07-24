use super::residency_profile::{
	GroupedReservoirResidencyProfile, ProfilePhase, writer_set_collection_bytes,
};
use super::stage_attribution;
use super::*;
use crate::exec::runtime_filter::{
	build_runtime_filter_fragment_for_ordinal, descriptor_from_fragment, runtime_filter_key_type,
};
use ahash::RandomState;

const STREAMED_INNER_LOADED_BUILD_ESTIMATE_SAFETY_MULTIPLIER: u64 = 64;
const STREAMED_INNER_BUILD_MAX_PARTITIONS: u32 = 1024;
const EMPTY_NEQ_EXISTENCE_SUMMARY_BUCKET: u32 = u32::MAX;
const NEQ_EXISTENCE_SUMMARY_INITIAL_ENTRY_CAPACITY: usize = 1024;
const NEQ_EXISTENCE_SUMMARY_PRODUCER: &str = "recognized Neq existence summary resident state";
const EMPTY_INT64_PRESENCE_BUCKET: u32 = u32::MAX;
const INT64_PRESENCE_INITIAL_ENTRY_CAPACITY: usize = 1024;
const INT64_PRESENCE_PRODUCER: &str = "membership presence set resident state";
const PARTITION_REPLAY_PRODUCER: &str = "partition-at-a-time replay resident state";
const STREAMED_CORRELATED_SORT_INPUT_PRODUCER: &str = "streamed correlated sort input accumulator";
const LOCAL_RUNTIME_FILTER_ABLATION_ENV: &str = "TORON_ABLATE_LOCAL_RUNTIME_FILTER";
const PHYSICAL_SCAN_BUILD_ENV: &str = "TORON_TPCH_PHYSICAL_SCAN_BUILD";
const RETAIN_FITTING_SCAN_BUILD_ENV: &str = "TORON_TPCH_RETAIN_FITTING_SCAN_BUILD";

fn physical_scan_build_enabled_from_env(value: Option<&str>) -> bool {
	!matches!(value, Some("0"))
}

fn physical_scan_build_enabled() -> bool {
	physical_scan_build_enabled_from_env(std::env::var(PHYSICAL_SCAN_BUILD_ENV).ok().as_deref())
}

fn retain_fitting_scan_build_enabled_from_env(value: Option<&str>) -> bool {
	!matches!(value, Some("0"))
}

fn retain_fitting_scan_build_enabled() -> bool {
	retain_fitting_scan_build_enabled_from_env(
		std::env::var(RETAIN_FITTING_SCAN_BUILD_ENV).ok().as_deref(),
	)
}

fn local_runtime_filter_enabled_from_env(value: Option<&str>) -> bool {
	!matches!(
		value.map(|value| value.trim().to_ascii_lowercase()),
		Some(value) if matches!(value.as_str(), "1" | "true" | "yes" | "on")
	)
}

fn local_runtime_filter_enabled() -> bool {
	local_runtime_filter_enabled_from_env(
		std::env::var(LOCAL_RUNTIME_FILTER_ABLATION_ENV)
			.ok()
			.as_deref(),
	)
}

fn build_local_terminal_runtime_filter(
	executor: &Executor,
	base_batches: &[RecordBatch],
	base_key: Option<crate::ir::ColumnOrdinal>,
	scan: &ExecutionPlan,
	probe_key: Option<crate::ir::ColumnOrdinal>,
	morsel: Option<(MorselId, u32)>,
) -> Option<RuntimeFilterDescriptor> {
	if executor.context.runtime_filter.is_some() {
		return None;
	}
	let base_key = base_key?;
	let probe_key = probe_key?;
	let base_schema = base_batches.first()?.schema();
	let base_field = base_schema.fields().get(base_key.0 as usize)?;
	let scan_schema = match scan {
		ExecutionPlan::Scan { schema, .. } => schema,
		_ => return None,
	};
	let source_probe_key = scan_source_ordinal(scan, probe_key)?;
	// `ExecutionPlan::Scan::schema` is the scan's projected output schema.  The
	// source ordinal is recovered from `projection`, while type eligibility is
	// checked against the projected field that actually carries that source
	// column.  The physical descriptor fingerprint on the request authenticates
	// the source schema at the storage boundary.
	let probe_field = scan_schema.fields().get(probe_key.0 as usize)?;
	let key_type = runtime_filter_key_type(base_field.data_type())?;
	if runtime_filter_key_type(probe_field.data_type())? != key_type {
		return None;
	}
	let fragment =
		build_runtime_filter_fragment_for_ordinal(base_batches, base_key, key_type).ok()??;
	let request = executor.bound_scan_request(scan, morsel)?;
	Some(descriptor_from_fragment(
		&fragment,
		vec![source_probe_key.0],
		request.physical_descriptor_fingerprint,
	))
}

fn scan_source_ordinal(
	scan: &ExecutionPlan,
	projected_ordinal: crate::ir::ColumnOrdinal,
) -> Option<crate::ir::ColumnOrdinal> {
	let ExecutionPlan::Scan { projection, .. } = scan else {
		return None;
	};
	projection
		.as_ref()
		.map_or(Some(projected_ordinal), |columns| {
			columns.get(projected_ordinal.0 as usize).copied()
		})
}

fn local_runtime_filter_shape_is_admitted(
	null_safe_keys: bool,
	key_plan: JoinKeyPlan,
	partition_build_keys: &[crate::ir::ColumnOrdinal],
	probe_keys: &[crate::ir::ColumnOrdinal],
) -> bool {
	!null_safe_keys
		&& matches!(key_plan, JoinKeyPlan::Int32 | JoinKeyPlan::Int64)
		&& partition_build_keys.len() == 1
		&& probe_keys.len() == 1
}

fn account_streamed_correlated_sort_input(
	accumulated_bytes: u64,
	batch: &RecordBatch,
	cap: u64,
) -> Result<u64, ExecutionError> {
	let requested = accumulated_bytes
		.checked_add(record_batch_memory_bytes(batch))
		.unwrap_or(u64::MAX);
	if requested > cap {
		return Err(budget_exhausted(
			BudgetDimension::Memory,
			STREAMED_CORRELATED_SORT_INPUT_PRODUCER,
			requested,
			cap,
		));
	}
	Ok(requested)
}

#[cfg(test)]
static MEMBERSHIP_OUTER_WRITER_CONSTRUCT_COUNT: AtomicU64 = AtomicU64::new(0);

fn record_membership_outer_writer_construct() {
	#[cfg(test)]
	MEMBERSHIP_OUTER_WRITER_CONSTRUCT_COUNT.fetch_add(1, Ordering::SeqCst);
}

struct MembershipScheduleReceipt {
	path: PathBuf,
	rows: Vec<String>,
}

impl MembershipScheduleReceipt {
	fn from_env() -> Option<Self> {
		std::env::var_os("TORON_MEMBERSHIP_SCHEDULE_RECEIPT_PATH").map(|path| Self {
			path: PathBuf::from(path),
			rows: Vec::new(),
		})
	}

	fn append(&mut self, milestone: &'static str, detail: serde_json::Value) {
		self.rows.push(
			serde_json::json!({
				"milestone": milestone,
				"detail": detail,
			})
			.to_string(),
		);
		let temporary = self.path.with_extension("tmp");
		let contents = format!("{}\n", self.rows.join("\n"));
		if std::fs::write(&temporary, contents).is_ok() {
			let _ = std::fs::rename(temporary, &self.path);
		}
	}

	#[cfg(test)]
	fn at_path(path: PathBuf) -> Self {
		Self {
			path,
			rows: Vec::new(),
		}
	}
}

const CHAIN_STAGE_PROFILE_ENV: &str = "TORON_CHAIN_STAGE_PROFILE_PATH";
const CHAIN_STAGE_COUNT: usize = 7;
const CHAIN_DEFERRED_BASE_FLUSH_INTERVAL: u64 = 256;
const CHAIN_DEFERRED_PAGE_FLUSH_INTERVAL: u64 = 1024;

#[derive(Clone, Copy)]
enum ChainStage {
	BaseScan = 0,
	InnerReplay0 = 1,
	InnerReplay1 = 2,
	InnerReplay2 = 3,
	SemiReplay3 = 4,
	AntiReplay4 = 5,
	GroupedFold = 6,
}

impl ChainStage {
	const ALL: [Self; CHAIN_STAGE_COUNT] = [
		Self::BaseScan,
		Self::InnerReplay0,
		Self::InnerReplay1,
		Self::InnerReplay2,
		Self::SemiReplay3,
		Self::AntiReplay4,
		Self::GroupedFold,
	];

	fn name(self) -> &'static str {
		match self {
			Self::BaseScan => "base_scan",
			Self::InnerReplay0 => "inner_replay_0",
			Self::InnerReplay1 => "inner_replay_1",
			Self::InnerReplay2 => "inner_replay_2",
			Self::SemiReplay3 => "semi_replay_3",
			Self::AntiReplay4 => "anti_replay_4",
			Self::GroupedFold => "grouped_fold",
		}
	}
}

#[derive(Clone, Copy, Default)]
struct ChainStageMeasurement {
	micros: u64,
	calls: u64,
	batches: u64,
	input_rows: u64,
	output_rows: u64,
}

struct ChainStageProfiler {
	path: PathBuf,
	temporary: PathBuf,
	started: Instant,
	sequence: u64,
	completed_base_batches: u64,
	raw_base_rows: u64,
	post_filter_base_rows: u64,
	drive_completed: bool,
	terminal_error: Option<String>,
	stages: [ChainStageMeasurement; CHAIN_STAGE_COUNT],
	deferred_partition_count: u64,
	deferred_active_partition: Option<u64>,
	deferred_completed_partitions: u64,
	deferred_probe_rows_buffered: u64,
	deferred_index_builds: u64,
	deferred_build_page_reads: u64,
	deferred_build_decoded_bytes: u64,
	deferred_probe_page_reads: u64,
	deferred_build_spill_bytes: u64,
	deferred_probe_spill_bytes: u64,
	deferred_max_index_bytes: u64,
	deferred_max_pinned_build_bytes: u64,
}

impl ChainStageProfiler {
	fn from_env() -> Option<Self> {
		static PATH: std::sync::OnceLock<Option<PathBuf>> = std::sync::OnceLock::new();
		let path = PATH
			.get_or_init(|| std::env::var_os(CHAIN_STAGE_PROFILE_ENV).map(PathBuf::from))
			.as_ref()?
			.clone();
		Some(Self::at_path(path))
	}

	fn at_path(path: PathBuf) -> Self {
		let temporary = path.with_extension("tmp");
		Self {
			path,
			temporary,
			started: Instant::now(),
			sequence: 0,
			completed_base_batches: 0,
			raw_base_rows: 0,
			post_filter_base_rows: 0,
			drive_completed: false,
			terminal_error: None,
			stages: [ChainStageMeasurement::default(); CHAIN_STAGE_COUNT],
			deferred_partition_count: 0,
			deferred_active_partition: None,
			deferred_completed_partitions: 0,
			deferred_probe_rows_buffered: 0,
			deferred_index_builds: 0,
			deferred_build_page_reads: 0,
			deferred_build_decoded_bytes: 0,
			deferred_probe_page_reads: 0,
			deferred_build_spill_bytes: 0,
			deferred_probe_spill_bytes: 0,
			deferred_max_index_bytes: 0,
			deferred_max_pinned_build_bytes: 0,
		}
	}

	fn begin_deferred_replay(
		&mut self,
		partition_count: usize,
		build_spill_bytes: u64,
	) -> Result<(), ExecutionError> {
		self.deferred_partition_count =
			u64::try_from(partition_count).map_err(|_| ExecutionError::Kernel {
				message: "chain stage deferred partition count exceeds u64".to_string(),
			})?;
		self.deferred_build_spill_bytes = build_spill_bytes;
		Ok(())
	}

	fn set_deferred_probe_spill_bytes(&mut self, probe_spill_bytes: u64) {
		self.deferred_probe_spill_bytes = probe_spill_bytes;
	}

	fn add_deferred_probe_rows(&mut self, rows: u64) -> Result<(), ExecutionError> {
		self.deferred_probe_rows_buffered = self
			.deferred_probe_rows_buffered
			.checked_add(rows)
			.ok_or_else(|| ExecutionError::Kernel {
				message: "chain stage deferred probe row count overflow".to_string(),
			})?;
		Ok(())
	}

	fn begin_deferred_partition(&mut self, partition: usize) -> Result<(), ExecutionError> {
		self.deferred_active_partition =
			Some(
				u64::try_from(partition).map_err(|_| ExecutionError::Kernel {
					message: "chain stage active partition exceeds u64".to_string(),
				})?,
			);
		Ok(())
	}

	fn record_deferred_index(
		&mut self,
		stats: PinnedSpilledInnerBuildStats,
	) -> Result<(), ExecutionError> {
		self.deferred_index_builds =
			self.deferred_index_builds
				.checked_add(1)
				.ok_or_else(|| ExecutionError::Kernel {
					message: "chain stage deferred index build count overflow".to_string(),
				})?;
		self.deferred_build_page_reads = self
			.deferred_build_page_reads
			.checked_add(stats.page_reads)
			.ok_or_else(|| ExecutionError::Kernel {
				message: "chain stage deferred build page read count overflow".to_string(),
			})?;
		self.deferred_build_decoded_bytes = self
			.deferred_build_decoded_bytes
			.checked_add(stats.decoded_bytes)
			.ok_or_else(|| ExecutionError::Kernel {
				message: "chain stage deferred decoded byte count overflow".to_string(),
			})?;
		self.deferred_max_index_bytes = self.deferred_max_index_bytes.max(stats.index_bytes);
		self.deferred_max_pinned_build_bytes = self
			.deferred_max_pinned_build_bytes
			.max(stats.decoded_bytes);
		Ok(())
	}

	fn complete_deferred_probe_page(&mut self) -> Result<(), ExecutionError> {
		self.deferred_probe_page_reads =
			self.deferred_probe_page_reads
				.checked_add(1)
				.ok_or_else(|| ExecutionError::Kernel {
					message: "chain stage deferred probe page read count overflow".to_string(),
				})?;
		Ok(())
	}

	fn deferred_probe_page_flush_due(&self) -> bool {
		self.deferred_probe_page_reads % CHAIN_DEFERRED_PAGE_FLUSH_INTERVAL == 0
	}

	fn complete_deferred_partition(&mut self) -> Result<(), ExecutionError> {
		self.deferred_completed_partitions = self
			.deferred_completed_partitions
			.checked_add(1)
			.ok_or_else(|| ExecutionError::Kernel {
				message: "chain stage deferred completed partition count overflow".to_string(),
			})?;
		self.deferred_active_partition = None;
		Ok(())
	}

	fn add_micros(&mut self, stage: ChainStage, micros: u64) -> Result<(), ExecutionError> {
		let measurement = &mut self.stages[stage as usize];
		measurement.micros =
			measurement
				.micros
				.checked_add(micros)
				.ok_or_else(|| ExecutionError::Kernel {
					message: "chain stage profile duration overflow".to_string(),
				})?;
		Ok(())
	}

	fn add_elapsed(&mut self, stage: ChainStage, started: Instant) -> Result<(), ExecutionError> {
		self.add_micros(stage, duration_micros(started))
	}

	fn add_flow(
		&mut self,
		stage: ChainStage,
		calls: u64,
		batches: u64,
		input_rows: u64,
		output_rows: u64,
	) -> Result<(), ExecutionError> {
		let measurement = &mut self.stages[stage as usize];
		measurement.calls =
			measurement
				.calls
				.checked_add(calls)
				.ok_or_else(|| ExecutionError::Kernel {
					message: "chain stage profile call count overflow".to_string(),
				})?;
		measurement.batches =
			measurement
				.batches
				.checked_add(batches)
				.ok_or_else(|| ExecutionError::Kernel {
					message: "chain stage profile batch count overflow".to_string(),
				})?;
		measurement.input_rows =
			measurement
				.input_rows
				.checked_add(input_rows)
				.ok_or_else(|| ExecutionError::Kernel {
					message: "chain stage profile input row count overflow".to_string(),
				})?;
		measurement.output_rows = measurement
			.output_rows
			.checked_add(output_rows)
			.ok_or_else(|| ExecutionError::Kernel {
				message: "chain stage profile output row count overflow".to_string(),
			})?;
		Ok(())
	}

	fn complete_base_batch(
		&mut self,
		raw_rows: u64,
		post_filter_rows: u64,
	) -> Result<(), ExecutionError> {
		self.completed_base_batches =
			self.completed_base_batches
				.checked_add(1)
				.ok_or_else(|| ExecutionError::Kernel {
					message: "chain stage profile completed batch count overflow".to_string(),
				})?;
		self.raw_base_rows =
			self.raw_base_rows
				.checked_add(raw_rows)
				.ok_or_else(|| ExecutionError::Kernel {
					message: "chain stage profile raw row count overflow".to_string(),
				})?;
		self.post_filter_base_rows = self
			.post_filter_base_rows
			.checked_add(post_filter_rows)
			.ok_or_else(|| ExecutionError::Kernel {
				message: "chain stage profile filtered row count overflow".to_string(),
			})?;
		Ok(())
	}

	fn base_batch_flush_due(&self) -> bool {
		self.deferred_partition_count == 0
			|| self.completed_base_batches % CHAIN_DEFERRED_BASE_FLUSH_INTERVAL == 0
	}

	fn snapshot_value(&self, total_drive_micros: u64) -> Result<serde_json::Value, ExecutionError> {
		let stage_micros = self.stages.iter().try_fold(0u64, |total, stage| {
			total
				.checked_add(stage.micros)
				.ok_or_else(|| ExecutionError::Kernel {
					message: "chain stage profile duration sum overflow".to_string(),
				})
		})?;
		// ADR 507: the in-drive residual comes from the attribution contract, not a hand-rolled
		// subtraction, so this profile's time reconciliation means exactly what every other profile's
		// does and an inverted band is a typed error rather than a silent underflow.
		let unattributed_residual_micros =
			stage_attribution::reconcile_stage_within_drive(stage_micros, total_drive_micros)
				.map_err(|error| ExecutionError::Kernel {
					message: format!("chain stage profile time reconciliation failed: {error}"),
				})?;
		// The replay-1 residency high-waters are a peak domain: the footprint is their max, never
		// their sum. Reconciling through the contract emits that as a checked object beside the raw
		// maxima, so a reader cannot add two peaks into a footprint that never existed at once.
		let residency_peak = stage_attribution::reconcile_peak_domain(
			"replay1_residency_peak",
			&[
				("index", self.deferred_max_index_bytes),
				("pinned_build", self.deferred_max_pinned_build_bytes),
			],
		);
		let stages = ChainStage::ALL
			.iter()
			.map(|stage| {
				let measurement = self.stages[*stage as usize];
				serde_json::json!({
					"stage": stage.name(),
					"micros": measurement.micros,
					"calls": measurement.calls,
					"batches": measurement.batches,
					"input_rows": measurement.input_rows,
					"output_rows": measurement.output_rows,
				})
			})
			.collect::<Vec<_>>();
		Ok(serde_json::json!({
			"sequence": self.sequence,
			"completed_base_batches": self.completed_base_batches,
			"raw_base_rows": self.raw_base_rows,
			"post_filter_base_rows": self.post_filter_base_rows,
			"drive_completed": self.drive_completed,
			"terminal_error": self.terminal_error,
			"total_drive_micros": total_drive_micros,
			"unattributed_residual_micros": unattributed_residual_micros,
			"stages": stages,
			"deferred_replay_1": {
				"partition_count": self.deferred_partition_count,
				"active_partition": self.deferred_active_partition,
				"completed_partitions": self.deferred_completed_partitions,
				"probe_rows_buffered": self.deferred_probe_rows_buffered,
				"index_builds": self.deferred_index_builds,
				"build_page_reads": self.deferred_build_page_reads,
				"build_decoded_bytes": self.deferred_build_decoded_bytes,
				"probe_page_reads": self.deferred_probe_page_reads,
				"build_spill_bytes": self.deferred_build_spill_bytes,
				"probe_spill_bytes": self.deferred_probe_spill_bytes,
				"max_index_bytes": self.deferred_max_index_bytes,
				"max_pinned_build_bytes": self.deferred_max_pinned_build_bytes,
				"residency_peak": residency_peak,
			},
		}))
	}

	fn flush(&mut self) -> Result<(), ExecutionError> {
		let value = self.snapshot_value(duration_micros(self.started))?;
		let contents =
			serde_json::to_vec_pretty(&value).map_err(|error| ExecutionError::Kernel {
				message: format!("chain stage profile serialization failed: {error}"),
			})?;
		std::fs::write(&self.temporary, contents).map_err(|error| ExecutionError::Kernel {
			message: format!("chain stage profile snapshot write failed: {error}"),
		})?;
		std::fs::rename(&self.temporary, &self.path).map_err(|error| ExecutionError::Kernel {
			message: format!("chain stage profile snapshot rename failed: {error}"),
		})?;
		self.sequence = self
			.sequence
			.checked_add(1)
			.ok_or_else(|| ExecutionError::Kernel {
				message: "chain stage profile sequence overflow".to_string(),
			})?;
		Ok(())
	}

	fn mark_terminal_error(&mut self, error: &ExecutionError) {
		self.terminal_error = Some(error.to_string());
	}
}

// Non-semantic diagnostic instrumentation for the fused streaming inner-join + global (ungrouped)
// aggregate path (`try_streaming_collapsing_global_aggregate` → scan-probe chunks). It splits the
// single fused pass into build / probe-scan / probe-key / output-materialize / aggregate-fold and
// always publishes attributed-vs-total so any un-timed span (e.g. the build-side scan branch or
// framework overhead) surfaces as an explicit residual rather than false precision. It writes no
// wire/cluster struct.
//
// The phases straddle three functions in one fused pass (collapsing driver → scan-probe chunks →
// selection materialize/fold), so a local profiler cannot thread through the `visit_batch` closure
// chain. Instead a process-global accumulator is begun/finished by the driver and written by the
// leaf call sites. This is sound for the diagnostic contract only: the accumulator is active for at
// most one fused inner-join-global-aggregate at a time, which holds for the single-query perf child
// the ledger is read from (each ladder rung runs in its own process). Everything is gated on the
// env path: when it is unset `local_join_phase_enabled()` is false, no `Instant` is sampled and no
// lock is taken, so a disabled run is byte-identical and pays no timing cost.
const LOCAL_JOIN_PHASE_PROFILE_ENV: &str = "TORON_LOCAL_JOIN_PHASE_PROFILE_PATH";
const LOCAL_JOIN_PHASE_COUNT: usize = 5;

#[derive(Clone, Copy)]
enum LocalJoinPhase {
	Build = 0,
	ProbeScan = 1,
	ProbeKey = 2,
	OutputMaterialize = 3,
	AggregateFold = 4,
}

impl LocalJoinPhase {
	const ALL: [Self; LOCAL_JOIN_PHASE_COUNT] = [
		Self::Build,
		Self::ProbeScan,
		Self::ProbeKey,
		Self::OutputMaterialize,
		Self::AggregateFold,
	];

	fn name(self) -> &'static str {
		match self {
			Self::Build => "build",
			Self::ProbeScan => "probe_scan",
			Self::ProbeKey => "probe_key",
			Self::OutputMaterialize => "output_materialize",
			Self::AggregateFold => "aggregate_fold",
		}
	}
}

#[derive(Clone, Copy, Default)]
struct LocalJoinPhaseMeasurement {
	micros: u64,
	batches: u64,
	input_rows: u64,
	output_rows: u64,
}

struct LocalJoinPhaseAccumulator {
	started: Instant,
	build_rows: u64,
	probe_rows: u64,
	matched_rows: u64,
	phases: [LocalJoinPhaseMeasurement; LOCAL_JOIN_PHASE_COUNT],
}

static LOCAL_JOIN_PHASE_LEDGER: Mutex<Option<LocalJoinPhaseAccumulator>> = Mutex::new(None);

fn local_join_phase_path() -> Option<&'static PathBuf> {
	static PATH: std::sync::OnceLock<Option<PathBuf>> = std::sync::OnceLock::new();
	PATH.get_or_init(|| std::env::var_os(LOCAL_JOIN_PHASE_PROFILE_ENV).map(PathBuf::from))
		.as_ref()
}

fn local_join_phase_enabled() -> bool {
	local_join_phase_path().is_some()
}

/// Dropping this disarms the accumulator on any path that did not already finish it, writing
/// `drive_completed=false`. This makes the write unconditional across every exit — error, early
/// return, or panic — so a later run can never read a stale file left by an aborted run. On the
/// success path `local_join_phase_finish(true)` has already taken the accumulator, so the guard's
/// disarm finds nothing and is a no-op.
struct LocalJoinPhaseGuard;

impl Drop for LocalJoinPhaseGuard {
	fn drop(&mut self) {
		// Best-effort: Drop cannot surface a Result, and a failed diagnostic write must never mask
		// the real execution outcome.
		let _ = local_join_phase_finish(false);
	}
}

/// Arm the accumulator for one fused inner-join global aggregate and return a guard that guarantees
/// it is disarmed and persisted on every exit. No-op (still returns a guard) unless the env path is
/// set.
fn local_join_phase_begin() -> LocalJoinPhaseGuard {
	if local_join_phase_enabled() {
		if let Ok(mut slot) = LOCAL_JOIN_PHASE_LEDGER.lock() {
			*slot = Some(LocalJoinPhaseAccumulator {
				started: Instant::now(),
				build_rows: 0,
				probe_rows: 0,
				matched_rows: 0,
				phases: [LocalJoinPhaseMeasurement::default(); LOCAL_JOIN_PHASE_COUNT],
			});
		}
	}
	LocalJoinPhaseGuard
}

fn local_join_phase_record_elapsed(phase: LocalJoinPhase, started: Instant) {
	if let Ok(mut slot) = LOCAL_JOIN_PHASE_LEDGER.lock() {
		if let Some(accumulator) = slot.as_mut() {
			let measurement = &mut accumulator.phases[phase as usize];
			measurement.micros = measurement.micros.saturating_add(duration_micros(started));
		}
	}
}

fn local_join_phase_record_flow(
	phase: LocalJoinPhase,
	batches: u64,
	input_rows: u64,
	output_rows: u64,
) {
	if let Ok(mut slot) = LOCAL_JOIN_PHASE_LEDGER.lock() {
		if let Some(accumulator) = slot.as_mut() {
			let measurement = &mut accumulator.phases[phase as usize];
			measurement.batches = measurement.batches.saturating_add(batches);
			measurement.input_rows = measurement.input_rows.saturating_add(input_rows);
			measurement.output_rows = measurement.output_rows.saturating_add(output_rows);
		}
	}
}

fn local_join_phase_set_counts(build_rows: u64, probe_rows: u64, matched_rows: u64) {
	if let Ok(mut slot) = LOCAL_JOIN_PHASE_LEDGER.lock() {
		if let Some(accumulator) = slot.as_mut() {
			accumulator.build_rows = accumulator.build_rows.saturating_add(build_rows);
			accumulator.probe_rows = accumulator.probe_rows.saturating_add(probe_rows);
			accumulator.matched_rows = accumulator.matched_rows.saturating_add(matched_rows);
		}
	}
}

/// Disarm the accumulator and write the ledger. `drive_completed` records whether the fused
/// dispatch actually ran to finalize, so a declined dispatch is never read as a full attribution.
fn local_join_phase_finish(drive_completed: bool) -> Result<(), ExecutionError> {
	if !local_join_phase_enabled() {
		return Ok(());
	}
	let accumulator = match LOCAL_JOIN_PHASE_LEDGER.lock() {
		Ok(mut slot) => slot.take(),
		Err(_) => return Ok(()),
	};
	let Some(accumulator) = accumulator else {
		return Ok(());
	};
	let Some(path) = local_join_phase_path() else {
		return Ok(());
	};
	let total_micros = duration_micros(accumulator.started);
	let attributed_micros = accumulator
		.phases
		.iter()
		.fold(0u64, |total, phase| total.saturating_add(phase.micros));
	// saturating_sub, never checked_sub: the fused sub-intervals sit at or below the drive total,
	// but Instant granularity could nudge the sum over — clamp to zero and always publish both
	// figures so any overlap or un-timed span reads as an honest residual, never false precision.
	let unattributed_residual_micros = total_micros.saturating_sub(attributed_micros);
	let phases = LocalJoinPhase::ALL
		.iter()
		.map(|phase| {
			let measurement = accumulator.phases[*phase as usize];
			serde_json::json!({
				"phase": phase.name(),
				"micros": measurement.micros,
				"batches": measurement.batches,
				"input_rows": measurement.input_rows,
				"output_rows": measurement.output_rows,
			})
		})
		.collect::<Vec<_>>();
	let value = serde_json::json!({
		"suite": "local_join_phase_ledger",
		"drive_completed": drive_completed,
		"build_rows": accumulator.build_rows,
		"probe_rows": accumulator.probe_rows,
		"matched_rows": accumulator.matched_rows,
		"total_execute_micros": total_micros,
		"attributed_micros": attributed_micros,
		"unattributed_residual_micros": unattributed_residual_micros,
		"phases": phases,
	});
	let temporary = path.with_extension("tmp");
	let contents = serde_json::to_vec_pretty(&value).map_err(|error| ExecutionError::Kernel {
		message: format!("local join phase profile serialization failed: {error}"),
	})?;
	std::fs::write(&temporary, contents).map_err(|error| ExecutionError::Kernel {
		message: format!("local join phase profile snapshot write failed: {error}"),
	})?;
	std::fs::rename(&temporary, path).map_err(|error| ExecutionError::Kernel {
		message: format!("local join phase profile snapshot rename failed: {error}"),
	})?;
	Ok(())
}

struct Int64PresenceSet {
	buckets: Vec<u32>,
	entries: Vec<i64>,
	hasher: RandomState,
	construction_peak_bytes: u64,
}

impl Int64PresenceSet {
	fn capacity_targets(row_upper_bound: usize) -> Result<(usize, usize), ExecutionError> {
		let entry_target = row_upper_bound
			.max(INT64_PRESENCE_INITIAL_ENTRY_CAPACITY)
			.checked_next_power_of_two()
			.ok_or_else(|| ExecutionError::Kernel {
				message: "membership presence capacity overflow".to_string(),
			})?;
		if entry_target >= EMPTY_INT64_PRESENCE_BUCKET as usize {
			return Err(ExecutionError::Kernel {
				message: "membership presence set exceeds u32 entry range".to_string(),
			});
		}
		let bucket_target = entry_target
			.checked_mul(2)
			.ok_or_else(|| ExecutionError::Kernel {
				message: "membership presence bucket capacity overflow".to_string(),
			})?;
		Ok((entry_target, bucket_target))
	}

	fn from_batches(
		batches: &[RecordBatch],
		key: ColumnOrdinal,
		memory_cap: u64,
	) -> Result<Option<Self>, ExecutionError> {
		let row_upper_bound = batches.iter().try_fold(0usize, |rows, batch| {
			rows.checked_add(batch.num_rows())
				.ok_or_else(|| ExecutionError::Kernel {
					message: "membership presence row bound overflow".to_string(),
				})
		})?;
		if row_upper_bound == 0 {
			return Ok(None);
		}
		let (entry_target, bucket_target) = Self::capacity_targets(row_upper_bound)?;
		let batch_bytes = batches
			.iter()
			.map(record_batch_memory_bytes)
			.fold(0u64, u64::saturating_add);
		let cap = join_spill_retained_footprint_memory_cap(memory_cap);
		let requested_entry_bytes = summary_bytes::<i64>(entry_target);
		let requested_bucket_bytes = summary_bytes::<u32>(bucket_target);
		let requested_peak = batch_bytes
			.saturating_add(requested_entry_bytes)
			.saturating_add(requested_bucket_bytes);
		Self::guard_construction(requested_peak, cap)?;

		let mut entries = Vec::new();
		entries
			.try_reserve_exact(entry_target)
			.map_err(|error| ExecutionError::Kernel {
				message: format!("membership presence entry allocation failed: {error}"),
			})?;
		let observed_entry_bytes = summary_bytes::<i64>(entries.capacity());
		let observed_entry_requested_bucket_peak = batch_bytes
			.saturating_add(observed_entry_bytes)
			.saturating_add(requested_bucket_bytes);
		Self::guard_construction(observed_entry_requested_bucket_peak, cap)?;

		let mut buckets = Vec::new();
		buckets
			.try_reserve_exact(bucket_target)
			.map_err(|error| ExecutionError::Kernel {
				message: format!("membership presence bucket allocation failed: {error}"),
			})?;
		let observed_bucket_bytes = summary_bytes::<u32>(buckets.capacity());
		let observed_peak = batch_bytes
			.saturating_add(observed_entry_bytes)
			.saturating_add(observed_bucket_bytes);
		Self::guard_construction(observed_peak, cap)?;
		buckets.resize(bucket_target, EMPTY_INT64_PRESENCE_BUCKET);

		let mut set = Self {
			buckets,
			entries,
			hasher: RandomState::new(),
			construction_peak_bytes: requested_peak
				.max(observed_entry_requested_bucket_peak)
				.max(observed_peak),
		};
		for batch in batches {
			let keys = batch
				.columns()
				.get(key.0 as usize)
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "membership presence key is out of range".to_string(),
				})?
				.as_any()
				.downcast_ref::<Int64Array>()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "membership presence key must be Int64".to_string(),
				})?;
			if keys.null_count() != 0 {
				return Err(ExecutionError::InvalidExpression {
					message: "membership presence key must be non-null".to_string(),
				});
			}
			for row in 0..batch.num_rows() {
				set.insert(keys.value(row))?;
			}
		}
		Ok(Some(set))
	}

	fn guard_construction(requested: u64, cap: u64) -> Result<(), ExecutionError> {
		if requested > cap {
			return Err(budget_exhausted(
				BudgetDimension::Memory,
				INT64_PRESENCE_PRODUCER,
				requested,
				cap,
			));
		}
		Ok(())
	}

	fn insert(&mut self, key: i64) -> Result<(), ExecutionError> {
		let bucket = self.find_bucket(key)?;
		if self.buckets[bucket] != EMPTY_INT64_PRESENCE_BUCKET {
			return Ok(());
		}
		let entry = u32::try_from(self.entries.len()).map_err(|_| ExecutionError::Kernel {
			message: "membership presence set exceeds u32 entry range".to_string(),
		})?;
		if entry == EMPTY_INT64_PRESENCE_BUCKET {
			return Err(ExecutionError::Kernel {
				message: "membership presence set exhausted u32 entry domain".to_string(),
			});
		}
		let capacity = self.entries.capacity();
		self.entries.push(key);
		if self.entries.capacity() != capacity {
			return Err(ExecutionError::Kernel {
				message: "membership presence set grew after reservation".to_string(),
			});
		}
		self.buckets[bucket] = entry;
		Ok(())
	}

	fn contains(&self, key: i64) -> Result<bool, ExecutionError> {
		let bucket = self.find_bucket(key)?;
		Ok(self.buckets[bucket] != EMPTY_INT64_PRESENCE_BUCKET)
	}

	fn find_bucket(&self, key: i64) -> Result<usize, ExecutionError> {
		if self.buckets.is_empty() || !self.buckets.len().is_power_of_two() {
			return Err(ExecutionError::Kernel {
				message: "membership presence set has invalid bucket capacity".to_string(),
			});
		}
		let mut bucket = (self.hasher.hash_one(key) as usize) & (self.buckets.len() - 1);
		for _ in 0..self.buckets.len() {
			let entry = self.buckets[bucket];
			if entry == EMPTY_INT64_PRESENCE_BUCKET
				|| self
					.entries
					.get(entry as usize)
					.is_some_and(|candidate| *candidate == key)
			{
				return Ok(bucket);
			}
			bucket = (bucket + 1) & (self.buckets.len() - 1);
		}
		Err(ExecutionError::Kernel {
			message: "membership presence set has no free bucket".to_string(),
		})
	}

	fn accounted_bytes(&self) -> u64 {
		summary_bytes::<u32>(self.buckets.capacity())
			.saturating_add(summary_bytes::<i64>(self.entries.capacity()))
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecognizedNeqExistenceSummaryColumns {
	preserved_value: ColumnOrdinal,
	existence_key: ColumnOrdinal,
	existence_value: ColumnOrdinal,
	key_count_hint: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
struct NeqExistenceSummaryEntry {
	key: i64,
	representative: i64,
}

pub(in crate::exec) struct NeqExistenceSummary {
	buckets: Vec<u32>,
	entries: Vec<NeqExistenceSummaryEntry>,
	multi: Vec<u64>,
	entry_limit: usize,
	hasher: RandomState,
	input_rows: u64,
	multi_keys: u64,
	peak_combined_bytes: u64,
	last_entry: Option<(i64, usize)>,
	#[cfg(test)]
	growth_count: u64,
}

impl NeqExistenceSummary {
	fn new() -> Self {
		Self {
			buckets: Vec::new(),
			entries: Vec::new(),
			multi: Vec::new(),
			entry_limit: 0,
			hasher: RandomState::new(),
			input_rows: 0,
			multi_keys: 0,
			peak_combined_bytes: 0,
			last_entry: None,
			#[cfg(test)]
			growth_count: 0,
		}
	}

	fn insert(
		&mut self,
		key: i64,
		value: i64,
		baseline_bytes: u64,
		cap: u64,
	) -> Result<(), ExecutionError> {
		self.input_rows = self.input_rows.saturating_add(1);
		if let Some((last_key, entry_index)) = self.last_entry
			&& last_key == key
		{
			self.observe_value(entry_index, value);
			return Ok(());
		}
		if let Some(entry_index) = self.lookup_entry_index(key) {
			self.observe_value(entry_index, value);
			self.last_entry = Some((key, entry_index));
			return Ok(());
		}
		if self.entries.len() == self.entry_limit {
			self.grow(baseline_bytes, cap)?;
		}
		let entry_index =
			u32::try_from(self.entries.len()).map_err(|_| ExecutionError::Kernel {
				message: "recognized Neq existence summary exceeds u32 entry range".to_string(),
			})?;
		let bucket = self.find_bucket(key)?;
		let capacity_before = self.entries.capacity();
		self.entries.push(NeqExistenceSummaryEntry {
			key,
			representative: value,
		});
		debug_assert_eq!(self.entries.capacity(), capacity_before);
		self.buckets[bucket] = entry_index;
		self.last_entry = Some((key, entry_index as usize));
		Ok(())
	}

	fn observe_value(&mut self, entry_index: usize, value: i64) {
		let entry = self.entries[entry_index];
		if entry.representative != value && !self.is_multi(entry_index) {
			self.set_multi(entry_index);
			self.multi_keys = self.multi_keys.saturating_add(1);
		}
	}

	fn matches(&self, key: i64, probe_value: i64) -> bool {
		let Some(entry_index) = self.lookup_entry_index(key) else {
			return false;
		};
		self.is_multi(entry_index) || self.entries[entry_index].representative != probe_value
	}

	fn matching_preserved_rows(
		&self,
		preserved_batch: &RecordBatch,
		preserved_by_key: &HashMap<JoinKey, Vec<u32>>,
		preserved_value: ColumnOrdinal,
	) -> Result<Vec<bool>, ExecutionError> {
		let values = preserved_batch
			.columns()
			.get(preserved_value.0 as usize)
			.ok_or_else(|| ExecutionError::InvalidExpression {
				message: "recognized Neq existence summary preserved value is out of range"
					.to_string(),
			})?
			.as_any()
			.downcast_ref::<Int64Array>()
			.ok_or_else(|| ExecutionError::InvalidExpression {
				message: "recognized Neq existence summary preserved value must be Int64"
					.to_string(),
			})?;
		if values.null_count() != 0 {
			return Err(ExecutionError::InvalidExpression {
				message: "recognized Neq existence summary received a null preserved value"
					.to_string(),
			});
		}
		let mut matched = vec![false; preserved_batch.num_rows()];
		for (key, preserved_rows) in preserved_by_key {
			let JoinKey::Int64(key) = key else {
				return Err(ExecutionError::InvalidExpression {
					message: "recognized Neq existence summary received a non-Int64 key"
						.to_string(),
				});
			};
			for preserved_row in preserved_rows {
				let row = *preserved_row as usize;
				if self.matches(*key, values.value(row)) {
					matched[row] = true;
				}
			}
		}
		Ok(matched)
	}

	fn write_batch(
		&mut self,
		batch: &RecordBatch,
		existence_key: ColumnOrdinal,
		existence_value: ColumnOrdinal,
		baseline_bytes: u64,
		cap: u64,
	) -> Result<(), ExecutionError> {
		let keys = batch
			.columns()
			.get(existence_key.0 as usize)
			.ok_or_else(|| ExecutionError::InvalidExpression {
				message: "recognized Neq existence summary build key is out of range".to_string(),
			})?
			.as_any()
			.downcast_ref::<Int64Array>()
			.ok_or_else(|| ExecutionError::InvalidExpression {
				message: "recognized Neq existence summary build key must be Int64".to_string(),
			})?;
		let values = batch
			.columns()
			.get(existence_value.0 as usize)
			.ok_or_else(|| ExecutionError::InvalidExpression {
				message: "recognized Neq existence summary build value is out of range".to_string(),
			})?
			.as_any()
			.downcast_ref::<Int64Array>()
			.ok_or_else(|| ExecutionError::InvalidExpression {
				message: "recognized Neq existence summary build value must be Int64".to_string(),
			})?;
		if keys.null_count() != 0 || values.null_count() != 0 {
			return Err(ExecutionError::InvalidExpression {
				message: "recognized Neq existence summary received a null build key/value"
					.to_string(),
			});
		}
		for row in 0..batch.num_rows() {
			self.insert(keys.value(row), values.value(row), baseline_bytes, cap)?;
		}
		Ok(())
	}

	fn grow(&mut self, baseline_bytes: u64, cap: u64) -> Result<(), ExecutionError> {
		let target = if self.entry_limit == 0 {
			NEQ_EXISTENCE_SUMMARY_INITIAL_ENTRY_CAPACITY
		} else {
			self.entry_limit
				.checked_mul(2)
				.ok_or_else(|| ExecutionError::Kernel {
					message: "recognized Neq existence summary capacity overflow".to_string(),
				})?
		};
		self.resize_capacity(target, baseline_bytes, cap)
	}

	fn reserve_key_count(
		&mut self,
		key_count: usize,
		baseline_bytes: u64,
		cap: u64,
	) -> Result<(), ExecutionError> {
		let target = key_count
			.max(NEQ_EXISTENCE_SUMMARY_INITIAL_ENTRY_CAPACITY)
			.checked_next_power_of_two()
			.ok_or_else(|| ExecutionError::Kernel {
				message: "recognized Neq existence summary capacity overflow".to_string(),
			})?;
		if target > self.entry_limit {
			self.resize_capacity(target, baseline_bytes, cap)?;
		}
		Ok(())
	}

	fn resize_capacity(
		&mut self,
		target: usize,
		baseline_bytes: u64,
		cap: u64,
	) -> Result<(), ExecutionError> {
		if target > u32::MAX as usize {
			return Err(ExecutionError::Kernel {
				message: "recognized Neq existence summary exceeds u32 entry range".to_string(),
			});
		}

		let requested_entry_bytes = summary_bytes::<NeqExistenceSummaryEntry>(target);
		self.guard_allocation(baseline_bytes, requested_entry_bytes, cap)?;
		let mut entries = Vec::new();
		entries
			.try_reserve_exact(target)
			.map_err(|error| ExecutionError::Kernel {
				message: format!(
					"recognized Neq existence summary entry allocation failed: {error}"
				),
			})?;
		let actual_entry_bytes = summary_bytes::<NeqExistenceSummaryEntry>(entries.capacity());
		self.guard_allocation(baseline_bytes, actual_entry_bytes, cap)?;
		entries.extend_from_slice(&self.entries);
		self.observe_peak(baseline_bytes, actual_entry_bytes);
		self.entries = entries;

		let multi_words = target.saturating_add(63) / 64;
		let requested_multi_bytes = summary_bytes::<u64>(multi_words);
		self.guard_allocation(baseline_bytes, requested_multi_bytes, cap)?;
		let mut multi = Vec::new();
		multi
			.try_reserve_exact(multi_words)
			.map_err(|error| ExecutionError::Kernel {
				message: format!(
					"recognized Neq existence summary bitset allocation failed: {error}"
				),
			})?;
		let actual_multi_bytes = summary_bytes::<u64>(multi.capacity());
		self.guard_allocation(baseline_bytes, actual_multi_bytes, cap)?;
		multi.resize(multi_words, 0);
		multi[..self.multi.len()].copy_from_slice(&self.multi);
		self.observe_peak(baseline_bytes, actual_multi_bytes);
		self.multi = multi;

		let bucket_count = target
			.checked_mul(2)
			.ok_or_else(|| ExecutionError::Kernel {
				message: "recognized Neq existence summary bucket capacity overflow".to_string(),
			})?;
		let requested_bucket_bytes = summary_bytes::<u32>(bucket_count);
		self.guard_allocation(baseline_bytes, requested_bucket_bytes, cap)?;
		let mut buckets = Vec::new();
		buckets
			.try_reserve_exact(bucket_count)
			.map_err(|error| ExecutionError::Kernel {
				message: format!(
					"recognized Neq existence summary bucket allocation failed: {error}"
				),
			})?;
		let actual_bucket_bytes = summary_bytes::<u32>(buckets.capacity());
		self.guard_allocation(baseline_bytes, actual_bucket_bytes, cap)?;
		buckets.resize(bucket_count, EMPTY_NEQ_EXISTENCE_SUMMARY_BUCKET);
		for (entry_index, entry) in self.entries.iter().enumerate() {
			let bucket = Self::find_bucket_in(&self.hasher, &buckets, &self.entries, entry.key)?;
			buckets[bucket] = u32::try_from(entry_index).map_err(|_| ExecutionError::Kernel {
				message: "recognized Neq existence summary exceeds u32 entry range".to_string(),
			})?;
		}
		self.observe_peak(baseline_bytes, actual_bucket_bytes);
		self.buckets = buckets;
		self.entry_limit = target;
		#[cfg(test)]
		{
			self.growth_count = self.growth_count.saturating_add(1);
		}
		Ok(())
	}

	fn lookup_entry_index(&self, key: i64) -> Option<usize> {
		if self.buckets.is_empty() {
			return None;
		}
		let bucket = self.find_bucket(key).ok()?;
		let entry_index = self.buckets[bucket];
		(entry_index != EMPTY_NEQ_EXISTENCE_SUMMARY_BUCKET).then_some(entry_index as usize)
	}

	fn find_bucket(&self, key: i64) -> Result<usize, ExecutionError> {
		Self::find_bucket_in(&self.hasher, &self.buckets, &self.entries, key)
	}

	fn find_bucket_in(
		hasher: &RandomState,
		buckets: &[u32],
		entries: &[NeqExistenceSummaryEntry],
		key: i64,
	) -> Result<usize, ExecutionError> {
		if buckets.is_empty() || !buckets.len().is_power_of_two() {
			return Err(ExecutionError::Kernel {
				message: "recognized Neq existence summary has invalid bucket capacity".to_string(),
			});
		}
		let mut bucket = (hasher.hash_one(key) as usize) & (buckets.len() - 1);
		for _ in 0..buckets.len() {
			let entry_index = buckets[bucket];
			if entry_index == EMPTY_NEQ_EXISTENCE_SUMMARY_BUCKET
				|| entries
					.get(entry_index as usize)
					.is_some_and(|entry| entry.key == key)
			{
				return Ok(bucket);
			}
			bucket = (bucket + 1) & (buckets.len() - 1);
		}
		Err(ExecutionError::Kernel {
			message: "recognized Neq existence summary has no free bucket".to_string(),
		})
	}

	fn is_multi(&self, entry_index: usize) -> bool {
		let word = entry_index / 64;
		let bit = entry_index % 64;
		self.multi
			.get(word)
			.is_some_and(|value| value & (1u64 << bit) != 0)
	}

	fn set_multi(&mut self, entry_index: usize) {
		let word = entry_index / 64;
		let bit = entry_index % 64;
		self.multi[word] |= 1u64 << bit;
	}

	fn accounted_bytes(&self) -> u64 {
		summary_bytes::<u32>(self.buckets.capacity())
			.saturating_add(summary_bytes::<NeqExistenceSummaryEntry>(
				self.entries.capacity(),
			))
			.saturating_add(summary_bytes::<u64>(self.multi.capacity()))
	}

	fn guard_allocation(
		&self,
		baseline_bytes: u64,
		allocation_bytes: u64,
		cap: u64,
	) -> Result<(), ExecutionError> {
		let requested = baseline_bytes
			.saturating_add(self.accounted_bytes())
			.saturating_add(allocation_bytes);
		if requested > cap {
			return Err(budget_exhausted(
				BudgetDimension::Memory,
				NEQ_EXISTENCE_SUMMARY_PRODUCER,
				requested,
				cap,
			));
		}
		Ok(())
	}

	fn observe_peak(&mut self, baseline_bytes: u64, allocation_bytes: u64) {
		self.peak_combined_bytes = self.peak_combined_bytes.max(
			baseline_bytes
				.saturating_add(self.accounted_bytes())
				.saturating_add(allocation_bytes),
		);
	}

	#[cfg(test)]
	fn computed_bytes(&self) -> u64 {
		self.accounted_bytes()
	}
}

fn summary_bytes<T>(capacity: usize) -> u64 {
	u64::try_from(capacity.saturating_mul(std::mem::size_of::<T>())).unwrap_or(u64::MAX)
}

fn guard_membership_owner_resident(
	baseline_bytes: u64,
	local_resident_bytes: u64,
	memory_cap: u64,
	producer: &'static str,
) -> Result<(), ExecutionError> {
	let requested = baseline_bytes.saturating_add(local_resident_bytes);
	let cap = join_spill_retained_footprint_memory_cap(memory_cap);
	if requested > cap {
		return Err(budget_exhausted(
			BudgetDimension::Memory,
			producer,
			requested,
			cap,
		));
	}
	Ok(())
}

fn partition_replay_reservoir_bytes(
	partitions: &[Option<SpilledInnerJoinBuild>],
	partition_capacity: usize,
) -> u64 {
	summary_bytes::<Option<SpilledInnerJoinBuild>>(partition_capacity).saturating_add(
		partitions
			.iter()
			.flatten()
			.map(SpilledInnerJoinBuild::owner_retained_resident_bytes)
			.fold(0u64, u64::saturating_add),
	)
}

fn grouped_fold_resident_bytes(
	groups: &BoundGroupedAggregateGroups,
	group_type_capacity: usize,
) -> u64 {
	groups
		.current_resident_bytes()
		.saturating_add(summary_bytes::<DataType>(group_type_capacity))
}

fn record_batches_resident_bytes(batches: &[RecordBatch], capacity: usize) -> u64 {
	summary_bytes::<RecordBatch>(capacity).saturating_add(
		batches
			.iter()
			.map(record_batch_memory_bytes)
			.fold(0u64, u64::saturating_add),
	)
}

#[allow(clippy::too_many_arguments)]
fn guard_partition_replay_owner_resident(
	runtime_baseline_bytes: u64,
	build_reservoir_bytes: u64,
	probe_reservoir_bytes: u64,
	active_build_bytes: u64,
	active_probe_bytes: u64,
	probe_page_bytes: u64,
	output_bytes: u64,
	fold_bytes: u64,
	memory_cap: u64,
) -> Result<(), ExecutionError> {
	let requested = runtime_baseline_bytes
		.saturating_add(build_reservoir_bytes)
		.saturating_add(probe_reservoir_bytes)
		.saturating_add(active_build_bytes)
		.saturating_add(active_probe_bytes)
		.saturating_add(probe_page_bytes)
		.saturating_add(output_bytes)
		.saturating_add(fold_bytes);
	let cap = join_spill_retained_footprint_memory_cap(memory_cap);
	if requested > cap {
		return Err(budget_exhausted_with_detail(
			BudgetDimension::Memory,
			PARTITION_REPLAY_PRODUCER,
			requested,
			cap,
			crate::error::BudgetExhaustedDetail::message(format!(
				"runtime_baseline_bytes={runtime_baseline_bytes}, build_reservoir_bytes={build_reservoir_bytes}, probe_reservoir_bytes={probe_reservoir_bytes}, active_build_bytes={active_build_bytes}, active_probe_bytes={active_probe_bytes}, probe_page_bytes={probe_page_bytes}, output_bytes={output_bytes}, fold_bytes={fold_bytes}"
			)),
		));
	}
	Ok(())
}

#[derive(Clone)]
pub(in crate::exec) struct ReorientedInnerJoinStage<'a> {
	replay_id: JoinReplayId,
	probe_schema: SchemaRef,
	probe_keys: Vec<crate::ir::ColumnOrdinal>,
	build: &'a ExecutionPlan,
	build_keys: Vec<crate::ir::ColumnOrdinal>,
	retained_build_columns: Option<Vec<crate::ir::ColumnOrdinal>>,
	output_schema: SchemaRef,
	strategy: JoinStrategy,
	post_filter: Option<&'a BoundExprProgram>,
	probe_columns_first: bool,
}

pub(in crate::exec) struct ReorientedInnerJoinRuntime<'a> {
	replay_id: JoinReplayId,
	strategy: JoinStrategy,
	build: ReorientedInnerJoinBuild<'a>,
	key_plan: JoinKeyPlan,
	probe_keys: Vec<crate::ir::ColumnOrdinal>,
	output_schema: SchemaRef,
	post_filter: Option<&'a BoundExprProgram>,
	probe_columns_first: bool,
	probe_rows: u64,
	output_rows: u64,
}

enum ReorientedStageScanBuildDecision {
	Stream,
	StreamMaterializedBatches(Vec<RecordBatch>),
	Materialized,
	MaterializedBatches(Vec<RecordBatch>),
}

fn reoriented_stage_scan_build_decision_for_materialized_batches(
	scan_output_bytes: u64,
	build_hash_bytes: u64,
	materialization_cap: u64,
	batches: Vec<RecordBatch>,
) -> ReorientedStageScanBuildDecision {
	// A scan-shaped build can fit as Arrow batches while the hash accumulator would not.
	if scan_output_bytes > materialization_cap {
		ReorientedStageScanBuildDecision::Stream
	} else if build_hash_bytes > materialization_cap {
		ReorientedStageScanBuildDecision::StreamMaterializedBatches(batches)
	} else {
		ReorientedStageScanBuildDecision::MaterializedBatches(batches)
	}
}

pub(in crate::exec) enum ReorientedInnerJoinBuild<'a> {
	InMemory {
		batch: RecordBatch,
		rows: JoinBuildRows,
	},
	Spilled {
		build: SpilledInnerJoinBuild,
		materialized_output_schema: SchemaRef,
		build_column_count: usize,
		projection: Option<ProjectedInnerBuildProjection>,
	},
	PartitionedScan {
		scan_input: StreamingScanProbeInput<'a>,
		morsel: Option<(MorselId, u32)>,
		partition_count: u32,
		group_count: u32,
		partition_build_keys: Vec<crate::ir::ColumnOrdinal>,
		write_build_keys: Vec<crate::ir::ColumnOrdinal>,
		materialized_output_schema: SchemaRef,
		build_column_count: usize,
		projection: Option<ProjectedInnerBuildProjection>,
		estimated_build_bytes: u64,
		observed_build_rows: u64,
		loaded_builds: Vec<Option<LoadedSpilledInnerBuild>>,
		loaded_resident_bytes: u64,
		loaded_build_attempted: bool,
	},
}

#[derive(Clone)]
pub(in crate::exec) struct ProjectedInnerBuildProjection {
	full_build_schema: SchemaRef,
	retained_columns: Vec<crate::ir::ColumnOrdinal>,
}

pub(in crate::exec) struct StreamingSemiAntiStage<'a> {
	pub(in crate::exec) replay_id: JoinReplayId,
	pub(in crate::exec) kind: JoinKind,
	pub(in crate::exec) preserved_keys: Vec<crate::ir::ColumnOrdinal>,
	pub(in crate::exec) existence: &'a ExecutionPlan,
	pub(in crate::exec) existence_keys: Vec<crate::ir::ColumnOrdinal>,
	pub(in crate::exec) match_filter: Option<&'a BoundExprProgram>,
	pub(in crate::exec) output_schema: SchemaRef,
	pub(in crate::exec) strategy: JoinStrategy,
}

#[allow(dead_code)]
pub(in crate::exec) enum StreamingSemiAntiExistence {
	InMemory {
		batch: RecordBatch,
		rows: JoinBuildRows,
	},
	Spilled(SpilledSemiAntiExistenceBuild),
	RecognizedNeqSummary {
		summary: NeqExistenceSummary,
		preserved_value: ColumnOrdinal,
	},
}

pub(in crate::exec) enum StreamingExistenceOp<'a> {
	Filter(&'a BoundExprProgram),
	Project {
		exprs: &'a BoundExprProgram,
		output_schema: SchemaRef,
	},
	Exchange {
		partition_count: u32,
		key: &'a BoundExprProgram,
	},
}

pub(in crate::exec) struct StreamingSemiAntiRuntime<'a> {
	pub(in crate::exec) replay_id: JoinReplayId,
	pub(in crate::exec) kind: JoinKind,
	pub(in crate::exec) strategy: JoinStrategy,
	pub(in crate::exec) existence: StreamingSemiAntiExistence,
	pub(in crate::exec) existence_row_count: u64,
	pub(in crate::exec) key_plan: JoinKeyPlan,
	pub(in crate::exec) preserved_keys: Vec<crate::ir::ColumnOrdinal>,
	pub(in crate::exec) match_filter: Option<&'a BoundExprProgram>,
	pub(in crate::exec) output_schema: SchemaRef,
	pub(in crate::exec) probe_rows: u64,
	pub(in crate::exec) output_rows: u64,
}

pub(in crate::exec) struct StreamingLeftOuterRecombineRuntime<'a> {
	replay_id: JoinReplayId,
	strategy: JoinStrategy,
	scalar_batch: RecordBatch,
	scalar_rows: JoinBuildRows,
	key_plan: JoinKeyPlan,
	preserved_keys: Vec<crate::ir::ColumnOrdinal>,
	match_filter: Option<&'a BoundExprProgram>,
	null_safe_keys: bool,
	output_schema: SchemaRef,
	probe_rows: u64,
	output_rows: u64,
}

struct StreamingLeftOuterGroupedInput<'a> {
	join: &'a ExecutionPlan,
	exchange: Option<(u32, &'a BoundExprProgram)>,
}

enum ReorientedChainOutputSink<'a> {
	GroupedFold {
		groups: &'a mut BoundGroupedAggregateGroups,
		group_types: &'a mut Vec<DataType>,
		outer_predicate: Option<&'a BoundExprProgram>,
		project_exprs: Option<&'a BoundExprProgram>,
		project_output_schema: Option<&'a arrow::datatypes::SchemaRef>,
		group_expr: &'a BoundExprProgram,
		aggr_expr: &'a [BoundAggregateCall],
	},
	#[allow(dead_code)]
	ProjectedSpill {
		spilled: &'a mut SpilledInnerJoinBuild,
		retained_columns: &'a [ColumnOrdinal],
		key_plan: JoinKeyPlan,
		build_keys: &'a [ColumnOrdinal],
		null_safe_keys: bool,
	},
	MembershipFilteredGroupedFold {
		presence: &'a Int64PresenceSet,
		left_key: ColumnOrdinal,
		partition_count: u32,
		exchange_key: &'a BoundExprProgram,
		group_expr: &'a BoundExprProgram,
		aggr_expr: &'a [BoundAggregateCall],
		groups: &'a mut BoundGroupedAggregateGroups,
		group_types: &'a mut Vec<DataType>,
		input_rows: &'a mut u64,
		output_rows: &'a mut u64,
		profile: Option<&'a MembershipChainProfile>,
	},
}

impl StreamingSemiAntiRuntime<'_> {
	fn retained_resident_bytes(&self) -> u64 {
		match &self.existence {
			StreamingSemiAntiExistence::InMemory { batch, rows } => {
				record_batch_memory_bytes(batch).saturating_add(rows.accounted_bytes())
			}
			StreamingSemiAntiExistence::Spilled(_) => 0,
			StreamingSemiAntiExistence::RecognizedNeqSummary { summary, .. } => {
				summary.accounted_bytes()
			}
		}
	}

	pub(in crate::exec) async fn cleanup(self) {
		if let StreamingSemiAntiExistence::Spilled(spilled) = self.existence {
			spilled.cleanup().await;
		}
	}
}

impl ReorientedInnerJoinRuntime<'_> {
	pub(in crate::exec) fn build_row_count(&self) -> u64 {
		match &self.build {
			ReorientedInnerJoinBuild::InMemory { batch, .. } => batch.num_rows() as u64,
			ReorientedInnerJoinBuild::Spilled { build, .. } => build.input_row_count(),
			ReorientedInnerJoinBuild::PartitionedScan {
				observed_build_rows,
				..
			} => *observed_build_rows,
		}
	}

	fn retained_resident_bytes(&self) -> u64 {
		match &self.build {
			ReorientedInnerJoinBuild::InMemory { batch, rows } => {
				record_batch_memory_bytes(batch).saturating_add(rows.accounted_bytes())
			}
			ReorientedInnerJoinBuild::Spilled { build, .. } => build.retained_resident_bytes(),
			ReorientedInnerJoinBuild::PartitionedScan {
				loaded_builds,
				loaded_resident_bytes,
				..
			} => loaded_resident_bytes.saturating_add(summary_bytes::<
				Option<LoadedSpilledInnerBuild>,
			>(loaded_builds.capacity())),
		}
	}

	pub(in crate::exec) async fn cleanup(self) {
		if let ReorientedInnerJoinBuild::Spilled { build, .. } = self.build {
			build.cleanup().await;
		}
	}
}

pub(in crate::exec) struct StreamingSemiAntiChain<'a> {
	base: &'a ExecutionPlan,
	inner_stages: Vec<ReorientedInnerJoinStage<'a>>,
	semi_stage: StreamingSemiAntiStage<'a>,
	anti_stage: StreamingSemiAntiStage<'a>,
}

#[derive(Clone, Copy)]
pub(in crate::exec) struct StreamingScanProbeInput<'a> {
	scan: &'a ExecutionPlan,
	filter_predicate: Option<&'a BoundExprProgram>,
	exchange: Option<StreamingScanProbeExchange<'a>>,
}

#[derive(Clone, Copy)]
pub(in crate::exec) struct StreamingScanProbeExchange<'a> {
	partition_count: u32,
	key: &'a BoundExprProgram,
}

#[derive(Clone, Copy)]
struct StreamingScanJoinPartition<'a> {
	keys: &'a [crate::ir::ColumnOrdinal],
	partition_count: u32,
	partition: u32,
	null_safe_keys: bool,
}

struct LoadedPartitionWork {
	partition: usize,
	build_pages: Vec<RecordBatch>,
	probe_batch: RecordBatch,
	probe_keys: Vec<crate::ir::ColumnOrdinal>,
	write_build_keys: Vec<crate::ir::ColumnOrdinal>,
	key_plan: JoinKeyPlan,
	null_safe_keys: bool,
	materialized_output_schema: SchemaRef,
	output_schema: SchemaRef,
	probe_columns_first: bool,
	build_column_count: usize,
	projection: Option<ProjectedInnerBuildProjection>,
	memory_cap: u64,
	resident_baseline_bytes: u64,
}

struct LoadedPartitionResult {
	partition: usize,
	output: Vec<RecordBatch>,
	probe_rows: u64,
	output_rows: u64,
}

#[derive(Clone)]
struct PartitionAtATimeReplaySpec<'a> {
	scan_input: StreamingScanProbeInput<'a>,
	morsel: Option<(MorselId, u32)>,
	partition_count: u32,
	partition_build_keys: Vec<crate::ir::ColumnOrdinal>,
	write_build_keys: Vec<crate::ir::ColumnOrdinal>,
	materialized_output_schema: SchemaRef,
	build_column_count: usize,
	projection: Option<ProjectedInnerBuildProjection>,
	estimated_build_bytes: u64,
	key_plan: JoinKeyPlan,
	probe_keys: Vec<crate::ir::ColumnOrdinal>,
	output_schema: SchemaRef,
	post_filter: Option<&'a BoundExprProgram>,
	probe_columns_first: bool,
}

struct PartitionAtATimeReplayReservoir {
	build: Vec<Option<SpilledInnerJoinBuild>>,
	probe: Vec<Option<SpilledInnerJoinBuild>>,
	build_rows: u64,
	build_spill_bytes: u64,
	probe_spill_bytes: u64,
}

#[derive(Clone, Copy)]
struct ParallelPartitionPlan {
	k: usize,
	resident_peak: u64,
}

fn partition_streaming_join_batch_with_buckets(
	batch: &RecordBatch,
	buckets: &[Option<u32>],
	partition_count: u32,
) -> Result<Vec<(usize, RecordBatch)>, ExecutionError> {
	if buckets.len() != batch.num_rows() {
		return Err(ExecutionError::InvalidExpression {
			message: format!(
				"partition bucket count {} did not match batch rows {}",
				buckets.len(),
				batch.num_rows()
			),
		});
	}

	let mut builders: Vec<Vec<u32>> = (0..partition_count).map(|_| Vec::new()).collect();
	for (row, row_partition) in buckets.iter().enumerate() {
		let Some(partition) = row_partition else {
			continue;
		};
		let partition = usize::try_from(*partition).map_err(|_| ExecutionError::Kernel {
			message: "join partition bucket did not fit usize".to_string(),
		})?;
		let Some(indices) = builders.get_mut(partition) else {
			return Err(ExecutionError::InvalidExpression {
				message: format!(
					"join partition bucket {} exceeded partition count {}",
					partition, partition_count
				),
			});
		};
		indices.push(join_probe_row_index(row)?);
	}

	let mut partitioned = Vec::new();
	for (partition, indices) in builders.into_iter().enumerate() {
		if indices.is_empty() {
			continue;
		}
		let indices = UInt32Array::from(indices);
		let columns = batch
			.columns()
			.iter()
			.map(|column| take(column, &indices, None))
			.collect::<Result<Vec<_>, _>>()?;
		partitioned.push((partition, RecordBatch::try_new(batch.schema(), columns)?));
	}
	Ok(partitioned)
}

fn partition_streaming_join_batch_active_group_with_buckets(
	batch: &RecordBatch,
	buckets: &[Option<u32>],
	partition_count: u32,
	partition_start: usize,
	partition_end: usize,
) -> Result<Vec<(usize, RecordBatch)>, ExecutionError> {
	if buckets.len() != batch.num_rows() {
		return Err(ExecutionError::InvalidExpression {
			message: format!(
				"partition bucket count {} did not match batch rows {}",
				buckets.len(),
				batch.num_rows()
			),
		});
	}
	let partition_count_usize =
		usize::try_from(partition_count).map_err(|_| ExecutionError::Kernel {
			message: "join partition count did not fit usize".to_string(),
		})?;
	if partition_start > partition_end || partition_end > partition_count_usize {
		return Err(ExecutionError::InvalidExpression {
			message: format!(
				"active join partition range {partition_start}..{partition_end} exceeded partition count {partition_count}"
			),
		});
	}

	let mut builders: Vec<Vec<u32>> = (partition_start..partition_end)
		.map(|_| Vec::new())
		.collect();
	for (row, row_partition) in buckets.iter().enumerate() {
		let Some(partition) = row_partition else {
			continue;
		};
		let partition = usize::try_from(*partition).map_err(|_| ExecutionError::Kernel {
			message: "join partition bucket did not fit usize".to_string(),
		})?;
		if partition >= partition_count_usize {
			return Err(ExecutionError::InvalidExpression {
				message: format!(
					"join partition bucket {} exceeded partition count {}",
					partition, partition_count
				),
			});
		}
		if partition < partition_start || partition >= partition_end {
			continue;
		}
		builders[partition - partition_start].push(join_probe_row_index(row)?);
	}

	let mut partitioned = Vec::new();
	for (offset, indices) in builders.into_iter().enumerate() {
		if indices.is_empty() {
			continue;
		}
		let indices = UInt32Array::from(indices);
		let columns = batch
			.columns()
			.iter()
			.map(|column| take(column, &indices, None))
			.collect::<Result<Vec<_>, _>>()?;
		partitioned.push((
			partition_start + offset,
			RecordBatch::try_new(batch.schema(), columns)?,
		));
	}
	Ok(partitioned)
}

pub(in crate::exec) fn extract_reoriented_inner_chain(
	top: &ExecutionPlan,
) -> Option<(&ExecutionPlan, Vec<ReorientedInnerJoinStage<'_>>)> {
	let mut current = top;
	let mut stages = Vec::new();
	let ExecutionPlan::Join { replay_id, .. } = current else {
		return None;
	};
	if replay_id.0 == 0 || *replay_id == JoinReplayId::SYNTHETIC {
		return None;
	}
	let mut expected = replay_id.0;
	let mut post_filter = None;
	loop {
		let ExecutionPlan::Join {
			replay_id,
			left,
			right,
			kind: JoinKind::Inner,
			left_keys,
			right_keys,
			match_filter: None,
			null_safe_keys: false,
			strategy: JoinStrategy::HashPartitioned,
			output_schema,
			..
		} = current
		else {
			return None;
		};
		if *replay_id != JoinReplayId(expected) {
			return None;
		}
		let (probe, probe_keys, build, build_keys, probe_columns_first, continuation) =
			if let Some(continuation) = reoriented_chain_continuation(left.as_ref(), expected) {
				(
					left.as_ref(),
					left_keys,
					right.as_ref(),
					right_keys,
					true,
					continuation,
				)
			} else if let Some(continuation) =
				reoriented_chain_continuation(right.as_ref(), expected)
			{
				(
					right.as_ref(),
					right_keys,
					left.as_ref(),
					left_keys,
					false,
					continuation,
				)
			} else {
				return None;
			};
		stages.push(ReorientedInnerJoinStage {
			replay_id: *replay_id,
			probe_schema: probe.schema(),
			probe_keys: probe_keys.clone(),
			build,
			build_keys: build_keys.clone(),
			retained_build_columns: None,
			output_schema: output_schema.clone(),
			strategy: JoinStrategy::HashPartitioned,
			post_filter,
			probe_columns_first,
		});
		if expected == 1 {
			stages.reverse();
			return Some((probe, stages));
		}
		current = continuation.plan;
		post_filter = continuation.post_filter;
		expected -= 1;
	}
}

fn collect_aggregate_columns(
	aggr_expr: &[BoundAggregateCall],
	columns: &mut BTreeSet<ColumnOrdinal>,
) {
	for call in aggr_expr {
		collect_program_column_ordinals(&call.args, columns);
	}
}

fn collect_left_side_program_columns(
	program: &BoundExprProgram,
	left_column_count: usize,
	columns: &mut BTreeSet<ColumnOrdinal>,
) {
	let mut referenced = BTreeSet::new();
	collect_program_column_ordinals(program, &mut referenced);
	for column in referenced {
		if (column.0 as usize) < left_column_count {
			columns.insert(column);
		}
	}
}

fn retained_existence_columns_for_semi_anti_stage(
	stage: &StreamingSemiAntiStage<'_>,
) -> Result<Option<Vec<ColumnOrdinal>>, ExecutionError> {
	let existence_schema = stage.existence.schema();
	let existence_width = existence_schema.fields().len();
	let mut columns = BTreeSet::new();
	columns.extend(stage.existence_keys.iter().copied());
	if let Some(match_filter) = stage.match_filter {
		let preserved_width = stage.output_schema.fields().len();
		let mut referenced = BTreeSet::new();
		collect_program_column_ordinals(match_filter, &mut referenced);
		for column in referenced {
			let index = column.0 as usize;
			if index < preserved_width {
				continue;
			}
			let existence_index = index - preserved_width;
			if existence_index >= existence_width {
				return Err(ExecutionError::InvalidExpression {
					message: format!(
						"semi/anti residual existence column {} exceeds existence width {}",
						existence_index, existence_width
					),
				});
			}
			columns.insert(ColumnOrdinal(existence_index as u32));
		}
	}
	let retained = columns.into_iter().collect::<Vec<_>>();
	if retained.is_empty() || retained.len() >= existence_width {
		Ok(None)
	} else {
		Ok(Some(retained))
	}
}

fn recognized_neq_existence_summary_columns(
	stage: &StreamingSemiAntiStage<'_>,
) -> Option<RecognizedNeqExistenceSummaryColumns> {
	let match_filter = stage.match_filter?;
	let root = match_filter.single_root()?;
	let BoundExprNode::Binary {
		op: BoundBinaryOp::Neq,
		left,
		right,
	} = match_filter.node(root)?
	else {
		return None;
	};
	let BoundExprNode::Column(left) = match_filter.node(*left)? else {
		return None;
	};
	let BoundExprNode::Column(right) = match_filter.node(*right)? else {
		return None;
	};
	if match_filter.node_type(root) != Some(&DataType::Boolean) {
		return None;
	}

	let preserved_schema = &stage.output_schema;
	let existence_schema = stage.existence.schema();
	let preserved_width = preserved_schema.fields().len();
	let classify = |column: &crate::ir::ResolvedColumn| {
		let ordinal = column.ordinal.0 as usize;
		if ordinal < preserved_width {
			let field = preserved_schema.fields().get(ordinal)?;
			(column.data_type == *field.data_type() && column.nullable == field.is_nullable())
				.then_some((true, ColumnOrdinal(ordinal as u32)))
		} else {
			let local = ordinal.checked_sub(preserved_width)?;
			let field = existence_schema.fields().get(local)?;
			(column.data_type == *field.data_type() && column.nullable == field.is_nullable())
				.then_some((false, ColumnOrdinal(local as u32)))
		}
	};
	let left = classify(left)?;
	let right = classify(right)?;
	let (preserved_value, existence_value) = match (left, right) {
		((true, preserved), (false, existence)) | ((false, existence), (true, preserved)) => {
			(preserved, existence)
		}
		_ => return None,
	};

	if stage.preserved_keys.len() != 1 || stage.existence_keys.len() != 1 {
		return None;
	}
	let preserved_key = stage.preserved_keys[0];
	let existence_key = stage.existence_keys[0];
	let preserved_key_field = preserved_schema.fields().get(preserved_key.0 as usize)?;
	let existence_key_field = existence_schema.fields().get(existence_key.0 as usize)?;
	let preserved_value_field = preserved_schema.fields().get(preserved_value.0 as usize)?;
	let existence_value_field = existence_schema.fields().get(existence_value.0 as usize)?;
	for field in [
		preserved_key_field,
		existence_key_field,
		preserved_value_field,
		existence_value_field,
	] {
		if field.data_type() != &DataType::Int64 || field.is_nullable() {
			return None;
		}
	}

	Some(RecognizedNeqExistenceSummaryColumns {
		preserved_value,
		existence_key,
		existence_value,
		key_count_hint: streaming_existence_key_count_hint(stage.existence, existence_key),
	})
}

fn retained_left_outer_grouped_columns(
	left_column_count: usize,
	left_keys: &[ColumnOrdinal],
	group_expr: &BoundExprProgram,
	aggr_expr: &[BoundAggregateCall],
	exchange: Option<(u32, &BoundExprProgram)>,
) -> Vec<ColumnOrdinal> {
	let mut columns = BTreeSet::new();
	columns.extend(left_keys.iter().copied());
	collect_left_side_program_columns(group_expr, left_column_count, &mut columns);
	if let Some((_partition_count, exchange_key)) = exchange {
		collect_left_side_program_columns(exchange_key, left_column_count, &mut columns);
	}
	for call in aggr_expr {
		collect_left_side_program_columns(&call.args, left_column_count, &mut columns);
	}
	columns.into_iter().collect()
}

fn mark_reoriented_inner_stage_projections(
	stages: &mut [ReorientedInnerJoinStage<'_>],
	mut required_after: BTreeSet<ColumnOrdinal>,
) -> Result<(), ExecutionError> {
	for stage in stages.iter_mut().rev() {
		let mut required_output = required_after.clone();
		if let Some(predicate) = stage.post_filter {
			collect_program_column_ordinals(predicate, &mut required_output);
		}
		let retained = retained_build_columns_for_reoriented_stage(stage, &required_output)?;
		let build_column_count = stage.build.schema().fields().len();
		stage.retained_build_columns = if retained.len() < build_column_count {
			Some(retained)
		} else {
			None
		};
		required_after = required_probe_columns_for_reoriented_stage(stage, &required_output)?;
	}
	Ok(())
}

fn retained_build_columns_for_reoriented_stage(
	stage: &ReorientedInnerJoinStage<'_>,
	required_output: &BTreeSet<ColumnOrdinal>,
) -> Result<Vec<ColumnOrdinal>, ExecutionError> {
	let probe_column_count = stage.probe_schema.fields().len();
	let build_column_count = stage.build.schema().fields().len();
	let output_column_count = stage.output_schema.fields().len();
	let mut retained = BTreeSet::new();
	retained.extend(stage.build_keys.iter().copied());
	for column in required_output {
		let index = column.0 as usize;
		if index >= output_column_count {
			return Err(ExecutionError::InvalidExpression {
				message: format!(
					"reoriented inner required output column {} exceeds output width {}",
					column.0, output_column_count
				),
			});
		}
		let build_index = if stage.probe_columns_first {
			index.checked_sub(probe_column_count)
		} else if index < build_column_count {
			Some(index)
		} else {
			None
		};
		if let Some(build_index) = build_index {
			if build_index < build_column_count {
				retained.insert(ColumnOrdinal(build_index as u32));
			}
		}
	}
	Ok(retained.into_iter().collect())
}

fn required_probe_columns_for_reoriented_stage(
	stage: &ReorientedInnerJoinStage<'_>,
	required_output: &BTreeSet<ColumnOrdinal>,
) -> Result<BTreeSet<ColumnOrdinal>, ExecutionError> {
	let probe_column_count = stage.probe_schema.fields().len();
	let build_column_count = stage.build.schema().fields().len();
	let output_column_count = stage.output_schema.fields().len();
	let mut required = BTreeSet::new();
	required.extend(stage.probe_keys.iter().copied());
	for column in required_output {
		let index = column.0 as usize;
		if index >= output_column_count {
			return Err(ExecutionError::InvalidExpression {
				message: format!(
					"reoriented inner required output column {} exceeds output width {}",
					column.0, output_column_count
				),
			});
		}
		let probe_index = if stage.probe_columns_first {
			if index < probe_column_count {
				Some(index)
			} else {
				None
			}
		} else {
			index
				.checked_sub(build_column_count)
				.filter(|index| *index < probe_column_count)
		};
		if let Some(probe_index) = probe_index {
			required.insert(ColumnOrdinal(probe_index as u32));
		}
	}
	Ok(required)
}

struct ReorientedChainContinuation<'a> {
	plan: &'a ExecutionPlan,
	post_filter: Option<&'a BoundExprProgram>,
}

fn reoriented_chain_continuation(
	plan: &ExecutionPlan,
	expected: u32,
) -> Option<ReorientedChainContinuation<'_>> {
	let ExecutionPlan::Exchange { input, .. } = plan else {
		return None;
	};
	let (candidate, post_filter) = match input.as_ref() {
		ExecutionPlan::Filter {
			input,
			predicate,
			disposition: FilterDisposition::EvaluateInToron,
		} => (input.as_ref(), Some(predicate)),
		ExecutionPlan::Filter {
			input,
			disposition: FilterDisposition::FullyPushed,
			..
		} => (input.as_ref(), None),
		other => (other, None),
	};
	let ExecutionPlan::Join { replay_id, .. } = candidate else {
		return None;
	};
	if expected == 1 {
		if *replay_id == JoinReplayId(0) {
			Some(ReorientedChainContinuation {
				plan: candidate,
				post_filter,
			})
		} else {
			None
		}
	} else if *replay_id == JoinReplayId(expected - 1) {
		Some(ReorientedChainContinuation {
			plan: candidate,
			post_filter,
		})
	} else {
		None
	}
}

pub(in crate::exec) fn exchange_input(plan: &ExecutionPlan) -> Option<&ExecutionPlan> {
	let ExecutionPlan::Exchange { input, .. } = plan else {
		return None;
	};
	Some(input.as_ref())
}

pub(in crate::exec) fn decompose_streaming_scan_probe_input(
	plan: &ExecutionPlan,
) -> Option<StreamingScanProbeInput<'_>> {
	match plan {
		ExecutionPlan::Scan { .. } => Some(StreamingScanProbeInput {
			scan: plan,
			filter_predicate: None,
			exchange: None,
		}),
		ExecutionPlan::Filter {
			input,
			predicate,
			disposition,
		} => {
			let ExecutionPlan::Scan { .. } = input.as_ref() else {
				return None;
			};
			let filter_predicate = match disposition {
				FilterDisposition::EvaluateInToron => Some(predicate),
				FilterDisposition::FullyPushed => None,
			};
			Some(StreamingScanProbeInput {
				scan: input.as_ref(),
				filter_predicate,
				exchange: None,
			})
		}
		ExecutionPlan::Exchange {
			input,
			partition_count,
			key,
		} => {
			let mut scan_input = decompose_streaming_scan_probe_input(input.as_ref())?;
			if scan_input.exchange.is_some() {
				return None;
			}
			scan_input.exchange = Some(StreamingScanProbeExchange {
				partition_count: *partition_count,
				key,
			});
			Some(scan_input)
		}
		_ => None,
	}
}

fn decompose_streaming_left_outer_grouped_input<'a>(
	input: &'a ExecutionPlan,
	group_expr: &BoundExprProgram,
	aggr_expr: &[BoundAggregateCall],
) -> Result<Option<StreamingLeftOuterGroupedInput<'a>>, ExecutionError> {
	match input {
		ExecutionPlan::Exchange {
			input,
			partition_count,
			key,
		} => {
			if *partition_count == 0 {
				return Err(ExecutionError::InvalidExpression {
					message: "exchange partition_count must be greater than zero".to_string(),
				});
			}
			if key != group_expr && !grouped_aggregate_is_order_insensitive(aggr_expr) {
				return Ok(None);
			}
			if !is_streaming_left_outer_grouped_join(input.as_ref()) {
				return Ok(None);
			}
			Ok(Some(StreamingLeftOuterGroupedInput {
				join: input.as_ref(),
				exchange: Some((*partition_count, key)),
			}))
		}
		_ if is_streaming_left_outer_grouped_join(input) => {
			Ok(Some(StreamingLeftOuterGroupedInput {
				join: input,
				exchange: None,
			}))
		}
		_ => Ok(None),
	}
}

fn is_streaming_left_outer_grouped_join(input: &ExecutionPlan) -> bool {
	let ExecutionPlan::Join {
		right,
		kind: JoinKind::LeftOuter,
		strategy: JoinStrategy::HashPartitioned,
		..
	} = input
	else {
		return false;
	};
	decompose_streaming_scan_probe_input(right).is_some()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReorientedInnerChainGroupedBaseScanProbeMode {
	FilteredJoin0,
	BareJoin0,
}

fn reoriented_inner_chain_grouped_base_scan_probe_mode(
	base: &ExecutionPlan,
) -> Option<ReorientedInnerChainGroupedBaseScanProbeMode> {
	let ExecutionPlan::Exchange {
		input: base_input, ..
	} = base
	else {
		return None;
	};
	let (join0, mode) = match base_input.as_ref() {
		ExecutionPlan::Filter {
			input,
			disposition: FilterDisposition::EvaluateInToron,
			..
		} => (
			input.as_ref(),
			ReorientedInnerChainGroupedBaseScanProbeMode::FilteredJoin0,
		),
		other @ ExecutionPlan::Join { .. } => (
			other,
			ReorientedInnerChainGroupedBaseScanProbeMode::BareJoin0,
		),
		_ => return None,
	};
	let ExecutionPlan::Join {
		replay_id,
		right,
		kind: JoinKind::Inner,
		..
	} = join0
	else {
		return None;
	};
	(*replay_id == JoinReplayId(0)
		&& decompose_streaming_scan_probe_input(right.as_ref()).is_some())
	.then_some(mode)
}

fn reoriented_grouped_required_output_columns(
	project_exprs: Option<&BoundExprProgram>,
	group_expr: &BoundExprProgram,
	aggr_expr: &[BoundAggregateCall],
	outer_predicate: Option<&BoundExprProgram>,
) -> BTreeSet<ColumnOrdinal> {
	let mut required_output = BTreeSet::new();
	if let Some(project_exprs) = project_exprs {
		collect_program_column_ordinals(project_exprs, &mut required_output);
	} else {
		collect_program_column_ordinals(group_expr, &mut required_output);
		collect_aggregate_columns(aggr_expr, &mut required_output);
	}
	if let Some(predicate) = outer_predicate {
		collect_program_column_ordinals(predicate, &mut required_output);
	}
	required_output
}

fn concat_join_schema(left: &SchemaRef, right: &SchemaRef) -> SchemaRef {
	Arc::new(Schema::new(
		left.fields()
			.iter()
			.chain(right.fields().iter())
			.map(|field| field.as_ref().clone())
			.collect::<Vec<_>>(),
	))
}

fn reorder_spilled_inner_probe_before_build(
	batch: RecordBatch,
	build_column_count: usize,
	output_schema: &SchemaRef,
) -> Result<RecordBatch, ExecutionError> {
	let column_count = batch.num_columns();
	if build_column_count > column_count || output_schema.fields().len() != column_count {
		return Err(ExecutionError::InvalidExpression {
			message: format!(
				"streamed spilled inner join column restore expected {} build columns in {} output columns",
				build_column_count, column_count
			),
		});
	}
	let mut columns = Vec::with_capacity(column_count);
	for column in &batch.columns()[build_column_count..] {
		columns.push(column.clone());
	}
	for column in &batch.columns()[..build_column_count] {
		columns.push(column.clone());
	}
	Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
}

#[allow(clippy::too_many_arguments)]
async fn visit_projected_streamed_spilled_inner_join_chunks<F>(
	spilled: &mut SpilledInnerJoinBuild,
	right_batches: &[RecordBatch],
	right_keys: &[crate::ir::ColumnOrdinal],
	key_plan: JoinKeyPlan,
	null_safe_keys: bool,
	full_build_schema: &SchemaRef,
	retained_columns: &[ColumnOrdinal],
	output_schema: &SchemaRef,
	memory_cap: u64,
	grouped_chain_observer: Option<&GroupedChainSpillObserver<'_>>,
	mut visit_batch: F,
) -> Result<(u64, u64), ExecutionError>
where
	F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
{
	let mut probe_rows = 0u64;
	let mut output_rows = 0u64;
	for batch in right_batches {
		for offset in (0..batch.num_rows()).step_by(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let len = CROSS_JOIN_OUTPUT_CHUNK_ROWS.min(batch.num_rows() - offset);
			let probe_chunk = batch.slice(offset, len);
			probe_rows = probe_rows.saturating_add(probe_chunk.num_rows() as u64);
			let lookup_timer = grouped_chain_observer
				.map(|observer| observer.begin(GroupedChainSubLeaf::ReadLookup))
				.transpose()?;
			let right_key_arrays = key_arrays(&probe_chunk, right_keys)?;
			let mut matches = Vec::new();
			for row in 0..probe_chunk.num_rows() {
				let Some(key) = join_key_at(key_plan, &right_key_arrays, row, null_safe_keys)?
				else {
					continue;
				};
				for build in spilled.lookup(&key).await? {
					matches.push(SpilledInnerJoinMatch {
						build,
						probe: join_probe_row_index(row)?,
					});
				}
			}
			if let Some(timer) = lookup_timer {
				timer.finish_with(probe_chunk.num_rows() as u64, 0, 0)?;
			}
			let page_map =
				load_streamed_inner_match_pages(spilled, &matches, grouped_chain_observer).await?;
			for chunk in matches.chunks(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
				let materialize_timer = grouped_chain_observer
					.map(|observer| observer.begin(GroupedChainSubLeaf::ReadMaterialization))
					.transpose()?;
				let batch = materialize_projected_streamed_spilled_inner_matches(
					&page_map,
					&probe_chunk,
					chunk,
					full_build_schema,
					retained_columns,
					output_schema,
					memory_cap,
				)?;
				if let Some(timer) = materialize_timer {
					timer.finish_with(
						batch.num_rows() as u64,
						record_batch_memory_bytes(&batch),
						1,
					)?;
				}
				output_rows = output_rows.saturating_add(batch.num_rows() as u64);
				visit_batch(batch)?;
			}
		}
	}
	Ok((probe_rows, output_rows))
}

pub(super) async fn load_streamed_inner_match_pages(
	spilled: &mut SpilledInnerJoinBuild,
	matches: &[SpilledInnerJoinMatch],
	grouped_chain_observer: Option<&GroupedChainSpillObserver<'_>>,
) -> Result<BTreeMap<usize, RecordBatch>, ExecutionError> {
	let mut page_sources = BTreeMap::new();
	for row in matches {
		page_sources
			.entry(row.build.page_index)
			.or_insert(usize::MAX);
	}
	let mut pages = BTreeMap::new();
	for page_index in page_sources.keys().copied() {
		pages.insert(
			page_index,
			spilled
				.read_batch_page_profiled(page_index, grouped_chain_observer)
				.await?,
		);
	}
	Ok(pages)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn visit_streamed_spilled_inner_join_chunks<F>(
	spilled: &mut SpilledInnerJoinBuild,
	right_batches: &[RecordBatch],
	right_keys: &[crate::ir::ColumnOrdinal],
	key_plan: JoinKeyPlan,
	null_safe_keys: bool,
	output_schema: &arrow::datatypes::SchemaRef,
	memory_cap: u64,
	mut visit_batch: F,
) -> Result<(u64, u64), ExecutionError>
where
	F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
{
	visit_streamed_spilled_inner_join_chunks_profiled(
		spilled,
		right_batches,
		right_keys,
		key_plan,
		null_safe_keys,
		output_schema,
		memory_cap,
		None,
		None,
		&mut visit_batch,
	)
	.await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn visit_streamed_spilled_inner_join_chunks_profiled<F>(
	spilled: &mut SpilledInnerJoinBuild,
	right_batches: &[RecordBatch],
	right_keys: &[crate::ir::ColumnOrdinal],
	key_plan: JoinKeyPlan,
	null_safe_keys: bool,
	output_schema: &arrow::datatypes::SchemaRef,
	memory_cap: u64,
	profile: Option<&GlobalFoldJoinFilterProfile>,
	grouped_chain_observer: Option<&GroupedChainSpillObserver<'_>>,
	mut visit_batch: F,
) -> Result<(u64, u64), ExecutionError>
where
	F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
{
	let mut probe_rows = 0u64;
	let mut output_rows = 0u64;
	let pin_timer = profile
		.map(|profile| profile.begin_leaf(GlobalFoldProfileLeaf::SpillPageRead))
		.transpose()?;
	let pin = spilled
		.try_pin_decoded_pages_profiled(memory_cap, grouped_chain_observer)
		.await;
	if let Some(timer) = pin_timer {
		timer.finish()?;
	}
	let _pin_admitted = pin?.is_admitted();
	for batch in right_batches {
		for offset in (0..batch.num_rows()).step_by(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let len = CROSS_JOIN_OUTPUT_CHUNK_ROWS.min(batch.num_rows() - offset);
			let probe_chunk = batch.slice(offset, len);
			probe_rows = probe_rows.saturating_add(probe_chunk.num_rows() as u64);
			let selection_timer = profile
				.map(|profile| profile.begin_leaf(GlobalFoldProfileLeaf::ProbeLookupSelect))
				.transpose()?;
			let grouped_lookup_timer = grouped_chain_observer
				.map(|observer| observer.begin(GroupedChainSubLeaf::ReadLookup))
				.transpose()?;
			let selection = async {
				let right_key_arrays = key_arrays(&probe_chunk, right_keys)?;
				let mut matches = Vec::new();
				for row in 0..probe_chunk.num_rows() {
					let Some(key) = join_key_at(key_plan, &right_key_arrays, row, null_safe_keys)?
					else {
						continue;
					};
					for build in spilled.lookup(&key).await? {
						matches.push(SpilledInnerJoinMatch {
							build,
							probe: join_probe_row_index(row)?,
						});
					}
				}
				Ok::<Vec<SpilledInnerJoinMatch>, ExecutionError>(matches)
			}
			.await;
			if let Some(timer) = selection_timer {
				timer.finish()?;
			}
			if let Some(timer) = grouped_lookup_timer {
				timer.finish_with(probe_chunk.num_rows() as u64, 0, 0)?;
			}
			let matches = selection?;
			let page_timer = profile
				.map(|profile| profile.begin_leaf(GlobalFoldProfileLeaf::SpillPageRead))
				.transpose()?;
			let page_map =
				load_streamed_inner_match_pages(spilled, &matches, grouped_chain_observer).await;
			if let Some(timer) = page_timer {
				timer.finish()?;
			}
			let page_map = page_map?;
			for chunk in matches.chunks(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
				let materialize_timer = profile
					.map(|profile| profile.begin_leaf(GlobalFoldProfileLeaf::ArrowMaterialize))
					.transpose()?;
				let grouped_materialize_timer = grouped_chain_observer
					.map(|observer| observer.begin(GroupedChainSubLeaf::ReadMaterialization))
					.transpose()?;
				let materialized = materialize_streamed_spilled_inner_matches(
					&page_map,
					&probe_chunk,
					chunk,
					output_schema,
					memory_cap,
				);
				if let Some(timer) = materialize_timer {
					timer.finish()?;
				}
				if let Some(timer) = grouped_materialize_timer {
					timer.finish_with(
						batch.num_rows() as u64,
						record_batch_memory_bytes(&batch),
						1,
					)?;
				}
				let batch = materialized?;
				output_rows = output_rows.saturating_add(batch.num_rows() as u64);
				visit_batch(batch)?;
			}
		}
	}
	Ok((probe_rows, output_rows))
}

fn materialize_projected_streamed_spilled_inner_matches(
	page_map: &BTreeMap<usize, RecordBatch>,
	probe_batch: &RecordBatch,
	matches: &[SpilledInnerJoinMatch],
	full_build_schema: &SchemaRef,
	retained_columns: &[ColumnOrdinal],
	output_schema: &SchemaRef,
	memory_cap: u64,
) -> Result<RecordBatch, ExecutionError> {
	if matches.is_empty() {
		return Ok(RecordBatch::new_empty(output_schema.clone()));
	}

	let materialization_cap = join_output_materialization_memory_cap(memory_cap);
	let mut page_sources = BTreeMap::new();
	for row in matches {
		page_sources
			.entry(row.build.page_index)
			.or_insert(usize::MAX);
	}
	let page_indices = page_sources.keys().copied().collect::<Vec<_>>();
	for (source_index, page_index) in page_indices.iter().copied().enumerate() {
		page_sources.insert(page_index, source_index);
	}
	let mut build_pages = Vec::with_capacity(page_sources.len());
	for page_index in page_indices {
		build_pages.push(page_map.get(&page_index).cloned().ok_or_else(|| {
			ExecutionError::Kernel {
				message: format!(
					"projected streamed inner join missing preloaded spilled page {page_index}"
				),
			}
		})?);
	}

	let retained_positions = retained_columns
		.iter()
		.enumerate()
		.map(|(position, ordinal)| (ordinal.0 as usize, position))
		.collect::<BTreeMap<_, _>>();
	let mut default_columns = BTreeMap::new();
	let mut default_column_bytes = 0u64;
	for (column_index, field) in full_build_schema.fields().iter().enumerate() {
		if retained_positions.contains_key(&column_index) {
			continue;
		}
		let column = default_array_for_unused_column(field.data_type(), matches.len())?;
		default_column_bytes = default_column_bytes
			.saturating_add(u64::try_from(column.get_array_memory_size()).unwrap_or(u64::MAX));
		default_columns.insert(column_index, column);
	}

	let estimated_output_bytes = join_take_index_bytes(matches.len())
		.saturating_add(join_interleave_index_bytes(matches.len()))
		.saturating_add(estimate_spilled_inner_build_selection_bytes(
			&build_pages,
			matches.len(),
		))
		.saturating_add(estimate_selected_batch_bytes(probe_batch, matches.len()))
		.saturating_add(default_column_bytes);
	if estimated_output_bytes > materialization_cap {
		return Err(budget_exhausted(
			BudgetDimension::Memory,
			"projected streamed inner join output materialization",
			estimated_output_bytes,
			materialization_cap,
		));
	}

	let mut build_indices = Vec::with_capacity(matches.len());
	let mut probe_indices = Vec::with_capacity(matches.len());
	for row in matches {
		let source_index =
			*page_sources
				.get(&row.build.page_index)
				.ok_or_else(|| ExecutionError::Kernel {
					message: format!(
						"projected streamed inner join missing spilled page source {}",
						row.build.page_index
					),
				})?;
		build_indices.push((source_index, row.build.row as usize));
		probe_indices.push(Some(row.probe));
	}

	let mut columns =
		Vec::with_capacity(full_build_schema.fields().len() + probe_batch.num_columns());
	for column_index in 0..full_build_schema.fields().len() {
		if let Some(retained_position) = retained_positions.get(&column_index).copied() {
			let source_columns = build_pages
				.iter()
				.map(|batch| batch.column(retained_position).as_ref())
				.collect::<Vec<_>>();
			columns.push(interleave(&source_columns, &build_indices)?);
		} else {
			columns.push(default_columns.remove(&column_index).ok_or_else(|| {
				ExecutionError::Kernel {
					message: format!(
						"projected streamed inner join missing default column {}",
						column_index
					),
				}
			})?);
		}
	}
	let probe_indices = UInt32Array::from(probe_indices);
	for column in probe_batch.columns() {
		columns.push(take(column, &probe_indices, None)?);
	}

	Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
}

fn run_loaded_partition_work(
	work: LoadedPartitionWork,
	membership_profile: Option<(&MembershipChainProfile, MembershipChainPhase)>,
	grouped_chain_observer: Option<&GroupedChainSpillObserver<'_>>,
) -> Result<LoadedPartitionResult, ExecutionError> {
	let LoadedPartitionWork {
		partition,
		build_pages,
		probe_batch,
		probe_keys,
		write_build_keys,
		key_plan,
		null_safe_keys,
		materialized_output_schema,
		output_schema,
		probe_columns_first,
		build_column_count,
		projection,
		memory_cap,
		resident_baseline_bytes,
	} = work;
	let index_timer = membership_profile
		.map(|(profile, phase)| profile.begin_leaf(phase, MembershipChainOperation::IndexBuild))
		.transpose()?;
	let grouped_index_timer = grouped_chain_observer
		.map(|observer| observer.begin(GroupedChainSubLeaf::ReadIndexRebuildCpu))
		.transpose()?;
	let loaded = LoadedSpilledInnerBuild::new(
		build_pages,
		key_plan,
		&write_build_keys,
		null_safe_keys,
		memory_cap,
		resident_baseline_bytes,
	)?;
	if let Some(timer) = index_timer {
		timer.finish_with(loaded.index.len() as u64, loaded.resident_bytes())?;
	}
	if let Some(timer) = grouped_index_timer {
		timer.finish_with(loaded.index.len() as u64, loaded.resident_bytes(), 1)?;
	}
	visit_loaded_partition_work(
		&loaded,
		partition,
		probe_batch,
		&probe_keys,
		key_plan,
		null_safe_keys,
		&materialized_output_schema,
		&output_schema,
		probe_columns_first,
		build_column_count,
		projection.as_ref(),
		memory_cap,
		membership_profile,
		grouped_chain_observer,
	)
}

#[allow(clippy::too_many_arguments)]
fn visit_loaded_partition_work(
	loaded: &LoadedSpilledInnerBuild,
	partition: usize,
	probe_batch: RecordBatch,
	probe_keys: &[crate::ir::ColumnOrdinal],
	key_plan: JoinKeyPlan,
	null_safe_keys: bool,
	materialized_output_schema: &SchemaRef,
	output_schema: &SchemaRef,
	probe_columns_first: bool,
	build_column_count: usize,
	projection: Option<&ProjectedInnerBuildProjection>,
	memory_cap: u64,
	membership_profile: Option<(&MembershipChainProfile, MembershipChainPhase)>,
	grouped_chain_observer: Option<&GroupedChainSpillObserver<'_>>,
) -> Result<LoadedPartitionResult, ExecutionError> {
	let mut output = Vec::new();
	let (probe_rows, output_rows) = if let Some(projection) = projection {
		visit_loaded_projected_streamed_spilled_inner_join_chunks(
			loaded,
			std::slice::from_ref(&probe_batch),
			probe_keys,
			key_plan,
			null_safe_keys,
			&projection.full_build_schema,
			&projection.retained_columns,
			materialized_output_schema,
			memory_cap,
			membership_profile,
			grouped_chain_observer,
			|batch| {
				let materialize_timer = grouped_chain_observer
					.map(|observer| observer.begin(GroupedChainSubLeaf::ReadMaterialization))
					.transpose()?;
				let batch = if probe_columns_first {
					reorder_spilled_inner_probe_before_build(
						batch,
						build_column_count,
						output_schema,
					)?
				} else {
					batch
				};
				if let Some(timer) = materialize_timer {
					timer.finish_with(
						batch.num_rows() as u64,
						record_batch_memory_bytes(&batch),
						1,
					)?;
				}
				if batch.num_rows() > 0 {
					output.push(batch);
				}
				Ok(())
			},
		)?
	} else {
		visit_loaded_streamed_spilled_inner_join_chunks(
			loaded,
			std::slice::from_ref(&probe_batch),
			probe_keys,
			key_plan,
			null_safe_keys,
			materialized_output_schema,
			memory_cap,
			membership_profile,
			grouped_chain_observer,
			|batch| {
				let materialize_timer = grouped_chain_observer
					.map(|observer| observer.begin(GroupedChainSubLeaf::ReadMaterialization))
					.transpose()?;
				let batch = if probe_columns_first {
					reorder_spilled_inner_probe_before_build(
						batch,
						build_column_count,
						output_schema,
					)?
				} else {
					batch
				};
				if let Some(timer) = materialize_timer {
					timer.finish_with(
						batch.num_rows() as u64,
						record_batch_memory_bytes(&batch),
						1,
					)?;
				}
				if batch.num_rows() > 0 {
					output.push(batch);
				}
				Ok(())
			},
		)?
	};
	Ok(LoadedPartitionResult {
		partition,
		output,
		probe_rows,
		output_rows,
	})
}

pub(in crate::exec::join) struct LoadedSpilledInnerBuild {
	pages: Vec<RecordBatch>,
	index: HashMap<JoinKey, Vec<SpilledBatchRowRef>>,
	resident_bytes: u64,
}

impl LoadedSpilledInnerBuild {
	fn new(
		pages: Vec<RecordBatch>,
		key_plan: JoinKeyPlan,
		build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		memory_cap: u64,
		resident_baseline_bytes: u64,
	) -> Result<Self, ExecutionError> {
		let mut index = HashMap::new();
		let mut resident_index_bytes = 0u64;
		let cap = join_spill_retained_footprint_memory_cap(memory_cap);
		let loaded_page_bytes = pages_loaded_bytes(&pages);
		for (page_index, page) in pages.iter().enumerate() {
			let key_arrays = key_arrays(page, build_keys)?;
			for row in 0..page.num_rows() {
				let Some(key) = join_key_at(key_plan, &key_arrays, row, null_safe_keys)? else {
					continue;
				};
				append_loaded_spilled_batch_row_ref(
					&mut index,
					&mut resident_index_bytes,
					key,
					SpilledBatchRowRef {
						page_index,
						row: join_build_row_index(row)?,
					},
				);
			}
			let resident = resident_baseline_bytes
				.saturating_add(resident_index_bytes)
				.saturating_add(loaded_page_bytes)
				.saturating_add(64 * 1024);
			if resident > cap {
				return Err(budget_exhausted(
					BudgetDimension::Memory,
					"parallel streamed inner join loaded partition resident state",
					resident,
					cap,
				));
			}
		}
		let resident_bytes = resident_index_bytes
			.saturating_add(loaded_page_bytes)
			.saturating_add(64 * 1024);
		Ok(Self {
			pages,
			index,
			resident_bytes,
		})
	}

	fn lookup(&self, join_key: &JoinKey) -> &[SpilledBatchRowRef] {
		self.index.get(join_key).map(Vec::as_slice).unwrap_or(&[])
	}

	fn resident_bytes(&self) -> u64 {
		self.resident_bytes
	}
}

fn append_loaded_spilled_batch_row_ref(
	index: &mut HashMap<JoinKey, Vec<SpilledBatchRowRef>>,
	resident_index_bytes: &mut u64,
	key: JoinKey,
	row_ref: SpilledBatchRowRef,
) {
	match index.entry(key) {
		Entry::Occupied(mut entry) => {
			entry.get_mut().push(row_ref);
		}
		Entry::Vacant(entry) => {
			*resident_index_bytes =
				resident_index_bytes.saturating_add(loaded_join_key_index_base_bytes(entry.key()));
			entry.insert(vec![row_ref]);
		}
	}
	*resident_index_bytes =
		resident_index_bytes.saturating_add(std::mem::size_of::<SpilledBatchRowRef>() as u64);
}

fn loaded_join_key_index_base_bytes(key: &JoinKey) -> u64 {
	let heap_bytes = match key {
		JoinKey::Int32(_) => std::mem::size_of::<i32>(),
		JoinKey::Int64(_) => std::mem::size_of::<i64>(),
		JoinKey::Generic(bytes) => bytes.len(),
	};
	u64::try_from(64usize.saturating_add(heap_bytes)).unwrap_or(u64::MAX)
}

fn pages_loaded_bytes(pages: &[RecordBatch]) -> u64 {
	pages
		.iter()
		.map(record_batch_memory_bytes)
		.fold(0u64, u64::saturating_add)
}

#[allow(clippy::too_many_arguments)]
fn visit_loaded_projected_streamed_spilled_inner_join_chunks<F>(
	loaded: &LoadedSpilledInnerBuild,
	right_batches: &[RecordBatch],
	right_keys: &[crate::ir::ColumnOrdinal],
	key_plan: JoinKeyPlan,
	null_safe_keys: bool,
	full_build_schema: &SchemaRef,
	retained_columns: &[ColumnOrdinal],
	output_schema: &SchemaRef,
	memory_cap: u64,
	membership_profile: Option<(&MembershipChainProfile, MembershipChainPhase)>,
	grouped_chain_observer: Option<&GroupedChainSpillObserver<'_>>,
	mut visit_batch: F,
) -> Result<(u64, u64), ExecutionError>
where
	F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
{
	let mut probe_rows = 0u64;
	let mut output_rows = 0u64;
	for batch in right_batches {
		for offset in (0..batch.num_rows()).step_by(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let len = CROSS_JOIN_OUTPUT_CHUNK_ROWS.min(batch.num_rows() - offset);
			let probe_chunk = batch.slice(offset, len);
			probe_rows = probe_rows.saturating_add(probe_chunk.num_rows() as u64);
			let lookup_timer = membership_profile
				.map(|(profile, phase)| {
					profile.begin_leaf(phase, MembershipChainOperation::LookupSelection)
				})
				.transpose()?;
			let grouped_lookup_timer = grouped_chain_observer
				.map(|observer| observer.begin(GroupedChainSubLeaf::ReadLookup))
				.transpose()?;
			let right_key_arrays = key_arrays(&probe_chunk, right_keys)?;
			let mut matches = Vec::new();
			for row in 0..probe_chunk.num_rows() {
				let Some(key) = join_key_at(key_plan, &right_key_arrays, row, null_safe_keys)?
				else {
					continue;
				};
				for build in loaded.lookup(&key) {
					matches.push(SpilledInnerJoinMatch {
						build: *build,
						probe: join_probe_row_index(row)?,
					});
				}
			}
			if let Some(timer) = lookup_timer {
				timer.finish_with(probe_chunk.num_rows() as u64, 0)?;
			}
			if let Some(timer) = grouped_lookup_timer {
				timer.finish_with(probe_chunk.num_rows() as u64, 0, 0)?;
			}
			for chunk in matches.chunks(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
				let materialize_timer = membership_profile
					.map(|(profile, phase)| {
						profile.begin_leaf(phase, MembershipChainOperation::ArrowMaterialize)
					})
					.transpose()?;
				let grouped_materialize_timer = grouped_chain_observer
					.map(|observer| observer.begin(GroupedChainSubLeaf::ReadMaterialization))
					.transpose()?;
				let batch = materialize_loaded_projected_streamed_spilled_inner_matches(
					loaded,
					&probe_chunk,
					chunk,
					full_build_schema,
					retained_columns,
					output_schema,
					memory_cap,
				)?;
				if let Some(timer) = materialize_timer {
					timer
						.finish_with(batch.num_rows() as u64, record_batch_memory_bytes(&batch))?;
				}
				if let Some(timer) = grouped_materialize_timer {
					timer.finish_with(
						batch.num_rows() as u64,
						record_batch_memory_bytes(&batch),
						1,
					)?;
				}
				output_rows = output_rows.saturating_add(batch.num_rows() as u64);
				visit_batch(batch)?;
			}
		}
	}
	Ok((probe_rows, output_rows))
}

#[allow(clippy::too_many_arguments)]
fn visit_loaded_streamed_spilled_inner_join_chunks<F>(
	loaded: &LoadedSpilledInnerBuild,
	right_batches: &[RecordBatch],
	right_keys: &[crate::ir::ColumnOrdinal],
	key_plan: JoinKeyPlan,
	null_safe_keys: bool,
	output_schema: &SchemaRef,
	memory_cap: u64,
	membership_profile: Option<(&MembershipChainProfile, MembershipChainPhase)>,
	grouped_chain_observer: Option<&GroupedChainSpillObserver<'_>>,
	mut visit_batch: F,
) -> Result<(u64, u64), ExecutionError>
where
	F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
{
	let mut probe_rows = 0u64;
	let mut output_rows = 0u64;
	for batch in right_batches {
		for offset in (0..batch.num_rows()).step_by(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let len = CROSS_JOIN_OUTPUT_CHUNK_ROWS.min(batch.num_rows() - offset);
			let probe_chunk = batch.slice(offset, len);
			probe_rows = probe_rows.saturating_add(probe_chunk.num_rows() as u64);
			let lookup_timer = membership_profile
				.map(|(profile, phase)| {
					profile.begin_leaf(phase, MembershipChainOperation::LookupSelection)
				})
				.transpose()?;
			let grouped_lookup_timer = grouped_chain_observer
				.map(|observer| observer.begin(GroupedChainSubLeaf::ReadLookup))
				.transpose()?;
			let right_key_arrays = key_arrays(&probe_chunk, right_keys)?;
			let mut matches = Vec::new();
			for row in 0..probe_chunk.num_rows() {
				let Some(key) = join_key_at(key_plan, &right_key_arrays, row, null_safe_keys)?
				else {
					continue;
				};
				for build in loaded.lookup(&key) {
					matches.push(SpilledInnerJoinMatch {
						build: *build,
						probe: join_probe_row_index(row)?,
					});
				}
			}
			if let Some(timer) = lookup_timer {
				timer.finish_with(probe_chunk.num_rows() as u64, 0)?;
			}
			if let Some(timer) = grouped_lookup_timer {
				timer.finish_with(probe_chunk.num_rows() as u64, 0, 0)?;
			}
			for chunk in matches.chunks(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
				let materialize_timer = membership_profile
					.map(|(profile, phase)| {
						profile.begin_leaf(phase, MembershipChainOperation::ArrowMaterialize)
					})
					.transpose()?;
				let grouped_materialize_timer = grouped_chain_observer
					.map(|observer| observer.begin(GroupedChainSubLeaf::ReadMaterialization))
					.transpose()?;
				let batch = materialize_loaded_streamed_spilled_inner_matches(
					loaded,
					&probe_chunk,
					chunk,
					output_schema,
					memory_cap,
				)?;
				if let Some(timer) = materialize_timer {
					timer
						.finish_with(batch.num_rows() as u64, record_batch_memory_bytes(&batch))?;
				}
				if let Some(timer) = grouped_materialize_timer {
					timer.finish_with(
						batch.num_rows() as u64,
						record_batch_memory_bytes(&batch),
						1,
					)?;
				}
				output_rows = output_rows.saturating_add(batch.num_rows() as u64);
				visit_batch(batch)?;
			}
		}
	}
	Ok((probe_rows, output_rows))
}

fn materialize_loaded_projected_streamed_spilled_inner_matches(
	loaded: &LoadedSpilledInnerBuild,
	probe_batch: &RecordBatch,
	matches: &[SpilledInnerJoinMatch],
	full_build_schema: &SchemaRef,
	retained_columns: &[ColumnOrdinal],
	output_schema: &SchemaRef,
	memory_cap: u64,
) -> Result<RecordBatch, ExecutionError> {
	if matches.is_empty() {
		return Ok(RecordBatch::new_empty(output_schema.clone()));
	}

	let materialization_cap = join_output_materialization_memory_cap(memory_cap);
	let mut page_sources = BTreeMap::new();
	for row in matches {
		page_sources
			.entry(row.build.page_index)
			.or_insert(usize::MAX);
	}
	let page_indices = page_sources.keys().copied().collect::<Vec<_>>();
	for (source_index, page_index) in page_indices.iter().copied().enumerate() {
		page_sources.insert(page_index, source_index);
	}
	let build_pages = page_indices
		.iter()
		.map(|page_index| loaded_page(loaded, *page_index).cloned())
		.collect::<Result<Vec<_>, _>>()?;

	let retained_positions = retained_columns
		.iter()
		.enumerate()
		.map(|(position, ordinal)| (ordinal.0 as usize, position))
		.collect::<BTreeMap<_, _>>();
	let mut default_columns = BTreeMap::new();
	let mut default_column_bytes = 0u64;
	for (column_index, field) in full_build_schema.fields().iter().enumerate() {
		if retained_positions.contains_key(&column_index) {
			continue;
		}
		let column = default_array_for_unused_column(field.data_type(), matches.len())?;
		default_column_bytes = default_column_bytes
			.saturating_add(u64::try_from(column.get_array_memory_size()).unwrap_or(u64::MAX));
		default_columns.insert(column_index, column);
	}

	let estimated_output_bytes = join_take_index_bytes(matches.len())
		.saturating_add(join_interleave_index_bytes(matches.len()))
		.saturating_add(estimate_spilled_inner_build_selection_bytes(
			&build_pages,
			matches.len(),
		))
		.saturating_add(estimate_selected_batch_bytes(probe_batch, matches.len()))
		.saturating_add(default_column_bytes);
	if estimated_output_bytes > materialization_cap {
		return Err(budget_exhausted(
			BudgetDimension::Memory,
			"parallel projected streamed inner join output materialization",
			estimated_output_bytes,
			materialization_cap,
		));
	}

	let mut build_indices = Vec::with_capacity(matches.len());
	let mut probe_indices = Vec::with_capacity(matches.len());
	for row in matches {
		let source_index =
			*page_sources
				.get(&row.build.page_index)
				.ok_or_else(|| ExecutionError::Kernel {
					message: format!(
						"parallel projected streamed inner join missing loaded page source {}",
						row.build.page_index
					),
				})?;
		build_indices.push((source_index, row.build.row as usize));
		probe_indices.push(Some(row.probe));
	}

	let mut columns =
		Vec::with_capacity(full_build_schema.fields().len() + probe_batch.num_columns());
	for column_index in 0..full_build_schema.fields().len() {
		if let Some(retained_position) = retained_positions.get(&column_index).copied() {
			let source_columns = build_pages
				.iter()
				.map(|batch| batch.column(retained_position).as_ref())
				.collect::<Vec<_>>();
			columns.push(interleave(&source_columns, &build_indices)?);
		} else {
			columns.push(default_columns.remove(&column_index).ok_or_else(|| {
				ExecutionError::Kernel {
					message: format!(
						"parallel projected streamed inner join missing default column {}",
						column_index
					),
				}
			})?);
		}
	}
	let probe_indices = UInt32Array::from(probe_indices);
	for column in probe_batch.columns() {
		columns.push(take(column, &probe_indices, None)?);
	}

	Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
}

fn materialize_loaded_streamed_spilled_inner_matches(
	loaded: &LoadedSpilledInnerBuild,
	probe_batch: &RecordBatch,
	matches: &[SpilledInnerJoinMatch],
	output_schema: &SchemaRef,
	memory_cap: u64,
) -> Result<RecordBatch, ExecutionError> {
	if matches.is_empty() {
		return Ok(RecordBatch::new_empty(output_schema.clone()));
	}

	let materialization_cap = join_output_materialization_memory_cap(memory_cap);
	let mut page_sources = BTreeMap::new();
	for row in matches {
		page_sources
			.entry(row.build.page_index)
			.or_insert(usize::MAX);
	}
	let page_indices = page_sources.keys().copied().collect::<Vec<_>>();
	for (source_index, page_index) in page_indices.iter().copied().enumerate() {
		page_sources.insert(page_index, source_index);
	}
	let build_pages = page_indices
		.iter()
		.map(|page_index| loaded_page(loaded, *page_index).cloned())
		.collect::<Result<Vec<_>, _>>()?;

	let estimated_output_bytes = join_take_index_bytes(matches.len())
		.saturating_add(join_interleave_index_bytes(matches.len()))
		.saturating_add(estimate_spilled_inner_build_selection_bytes(
			&build_pages,
			matches.len(),
		))
		.saturating_add(estimate_selected_batch_bytes(probe_batch, matches.len()));
	if estimated_output_bytes > materialization_cap {
		return Err(budget_exhausted(
			BudgetDimension::Memory,
			"parallel streamed inner join output materialization",
			estimated_output_bytes,
			materialization_cap,
		));
	}

	let mut build_indices = Vec::with_capacity(matches.len());
	let mut probe_indices = Vec::with_capacity(matches.len());
	for row in matches {
		let source_index =
			*page_sources
				.get(&row.build.page_index)
				.ok_or_else(|| ExecutionError::Kernel {
					message: format!(
						"parallel streamed inner join missing loaded page source {}",
						row.build.page_index
					),
				})?;
		build_indices.push((source_index, row.build.row as usize));
		probe_indices.push(Some(row.probe));
	}

	let mut columns = Vec::with_capacity(
		build_pages
			.first()
			.map(RecordBatch::num_columns)
			.unwrap_or(0)
			.saturating_add(probe_batch.num_columns()),
	);
	for column_index in 0..build_pages
		.first()
		.map(RecordBatch::num_columns)
		.unwrap_or(0)
	{
		let source_columns = build_pages
			.iter()
			.map(|batch| batch.column(column_index).as_ref())
			.collect::<Vec<_>>();
		columns.push(interleave(&source_columns, &build_indices)?);
	}
	let probe_indices = UInt32Array::from(probe_indices);
	for column in probe_batch.columns() {
		columns.push(take(column, &probe_indices, None)?);
	}

	Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
}

fn loaded_page(
	loaded: &LoadedSpilledInnerBuild,
	page_index: usize,
) -> Result<&RecordBatch, ExecutionError> {
	loaded
		.pages
		.get(page_index)
		.ok_or_else(|| ExecutionError::Kernel {
			message: format!("parallel streamed inner join loaded page {page_index} is missing"),
		})
}

async fn materialize_projected_streamed_spilled_left_outer_matches(
	spilled: &mut SpilledInnerJoinBuild,
	probe_batch: &RecordBatch,
	matches: &[SpilledInnerJoinMatch],
	full_left_schema: &SchemaRef,
	retained_columns: &[ColumnOrdinal],
	output_schema: &SchemaRef,
	memory_cap: u64,
) -> Result<RecordBatch, ExecutionError> {
	if matches.is_empty() {
		return Ok(RecordBatch::new_empty(output_schema.clone()));
	}

	let materialization_cap = join_output_materialization_memory_cap(memory_cap);
	let mut page_sources = BTreeMap::new();
	for row in matches {
		page_sources
			.entry(row.build.page_index)
			.or_insert(usize::MAX);
	}
	let page_indices = page_sources.keys().copied().collect::<Vec<_>>();
	for (source_index, page_index) in page_indices.iter().copied().enumerate() {
		page_sources.insert(page_index, source_index);
	}
	let mut build_pages = Vec::with_capacity(page_sources.len());
	for page_index in page_indices {
		build_pages.push(spilled.read_batch_page(page_index).await?);
	}

	let retained_positions = retained_columns
		.iter()
		.enumerate()
		.map(|(position, ordinal)| (ordinal.0 as usize, position))
		.collect::<BTreeMap<_, _>>();
	let mut default_columns = BTreeMap::new();
	let mut default_column_bytes = 0u64;
	for (column_index, field) in full_left_schema.fields().iter().enumerate() {
		if retained_positions.contains_key(&column_index) {
			continue;
		}
		let column = default_array_for_unused_column(field.data_type(), matches.len())?;
		default_column_bytes = default_column_bytes
			.saturating_add(u64::try_from(column.get_array_memory_size()).unwrap_or(u64::MAX));
		default_columns.insert(column_index, column);
	}

	let estimated_output_bytes = join_take_index_bytes(matches.len())
		.saturating_add(join_interleave_index_bytes(matches.len()))
		.saturating_add(estimate_spilled_inner_build_selection_bytes(
			&build_pages,
			matches.len(),
		))
		.saturating_add(estimate_selected_batch_bytes(probe_batch, matches.len()))
		.saturating_add(default_column_bytes);
	if estimated_output_bytes > materialization_cap {
		return Err(budget_exhausted(
			BudgetDimension::Memory,
			"projected spilled left outer output materialization",
			estimated_output_bytes,
			materialization_cap,
		));
	}

	let mut build_indices = Vec::with_capacity(matches.len());
	let mut probe_indices = Vec::with_capacity(matches.len());
	for row in matches {
		let source_index =
			*page_sources
				.get(&row.build.page_index)
				.ok_or_else(|| ExecutionError::Kernel {
					message: format!(
						"projected spilled left outer missing spilled page source {}",
						row.build.page_index
					),
				})?;
		build_indices.push((source_index, row.build.row as usize));
		probe_indices.push(Some(row.probe));
	}

	let mut columns =
		Vec::with_capacity(full_left_schema.fields().len() + probe_batch.num_columns());
	for column_index in 0..full_left_schema.fields().len() {
		if let Some(retained_position) = retained_positions.get(&column_index).copied() {
			let source_columns = build_pages
				.iter()
				.map(|batch| batch.column(retained_position).as_ref())
				.collect::<Vec<_>>();
			columns.push(interleave(&source_columns, &build_indices)?);
		} else {
			columns.push(default_columns.remove(&column_index).ok_or_else(|| {
				ExecutionError::Kernel {
					message: format!(
						"projected spilled left outer missing default column {}",
						column_index
					),
				}
			})?);
		}
	}
	let probe_indices = UInt32Array::from(probe_indices);
	for column in probe_batch.columns() {
		columns.push(take(column, &probe_indices, None)?);
	}

	let output = RecordBatch::try_new(output_schema.clone(), columns)?;
	spilled.record_materialized_batch(matches.len(), page_sources.len());
	Ok(output)
}

async fn materialize_projected_streamed_spilled_left_outer_unmatched(
	spilled: &mut SpilledInnerJoinBuild,
	refs: &[SpilledBatchRowRef],
	full_left_schema: &SchemaRef,
	retained_columns: &[ColumnOrdinal],
	right_schema: &SchemaRef,
	output_schema: &SchemaRef,
	memory_cap: u64,
) -> Result<RecordBatch, ExecutionError> {
	if refs.is_empty() {
		return Ok(RecordBatch::new_empty(output_schema.clone()));
	}

	let materialization_cap = join_output_materialization_memory_cap(memory_cap);
	let mut page_sources = BTreeMap::new();
	for row in refs {
		page_sources.entry(row.page_index).or_insert(usize::MAX);
	}
	let page_indices = page_sources.keys().copied().collect::<Vec<_>>();
	for (source_index, page_index) in page_indices.iter().copied().enumerate() {
		page_sources.insert(page_index, source_index);
	}
	let mut build_pages = Vec::with_capacity(page_sources.len());
	for page_index in page_indices {
		build_pages.push(spilled.read_batch_page(page_index).await?);
	}

	let retained_positions = retained_columns
		.iter()
		.enumerate()
		.map(|(position, ordinal)| (ordinal.0 as usize, position))
		.collect::<BTreeMap<_, _>>();
	let mut default_columns = BTreeMap::new();
	let mut default_column_bytes = 0u64;
	for (column_index, field) in full_left_schema.fields().iter().enumerate() {
		if retained_positions.contains_key(&column_index) {
			continue;
		}
		let column = default_array_for_unused_column(field.data_type(), refs.len())?;
		default_column_bytes = default_column_bytes
			.saturating_add(u64::try_from(column.get_array_memory_size()).unwrap_or(u64::MAX));
		default_columns.insert(column_index, column);
	}

	let estimated_output_bytes = join_interleave_index_bytes(refs.len())
		.saturating_add(estimate_spilled_inner_build_selection_bytes(
			&build_pages,
			refs.len(),
		))
		.saturating_add(default_column_bytes);
	if estimated_output_bytes > materialization_cap {
		return Err(budget_exhausted(
			BudgetDimension::Memory,
			"projected spilled left outer unmatched materialization",
			estimated_output_bytes,
			materialization_cap,
		));
	}

	let mut build_indices = Vec::with_capacity(refs.len());
	for row in refs {
		let source_index =
			*page_sources
				.get(&row.page_index)
				.ok_or_else(|| ExecutionError::Kernel {
					message: format!(
						"projected spilled left outer missing unmatched page source {}",
						row.page_index
					),
				})?;
		build_indices.push((source_index, row.row as usize));
	}

	let mut columns = Vec::with_capacity(output_schema.fields().len());
	for column_index in 0..full_left_schema.fields().len() {
		if let Some(retained_position) = retained_positions.get(&column_index).copied() {
			let source_columns = build_pages
				.iter()
				.map(|batch| batch.column(retained_position).as_ref())
				.collect::<Vec<_>>();
			columns.push(interleave(&source_columns, &build_indices)?);
		} else {
			columns.push(default_columns.remove(&column_index).ok_or_else(|| {
				ExecutionError::Kernel {
					message: format!(
						"projected spilled left outer missing unmatched default column {}",
						column_index
					),
				}
			})?);
		}
	}
	for field in right_schema.fields() {
		columns.push(arrow::array::new_null_array(field.data_type(), refs.len()));
	}

	let output = RecordBatch::try_new(output_schema.clone(), columns)?;
	spilled.record_materialized_batch(refs.len(), page_sources.len());
	Ok(output)
}

fn predicate_references_only_probe_prefix(
	predicate: &BoundExprProgram,
	probe_columns_first: bool,
	probe_column_count: usize,
) -> bool {
	if !probe_columns_first {
		return false;
	}
	let mut columns = BTreeSet::new();
	collect_program_column_ordinals(predicate, &mut columns);
	columns
		.iter()
		.all(|column| (column.0 as usize) < probe_column_count)
}

fn decompose_join_or_filtered_join(
	plan: &ExecutionPlan,
) -> Option<(&ExecutionPlan, Option<&BoundExprProgram>)> {
	match plan {
		ExecutionPlan::Join {
			kind: JoinKind::Inner,
			..
		} => Some((plan, None)),
		ExecutionPlan::Filter {
			input,
			predicate,
			disposition,
		} => {
			let ExecutionPlan::Join {
				kind: JoinKind::Inner,
				..
			} = input.as_ref()
			else {
				return None;
			};
			let filter_predicate = match disposition {
				FilterDisposition::FullyPushed => None,
				FilterDisposition::EvaluateInToron => Some(predicate),
			};
			Some((input.as_ref(), filter_predicate))
		}
		_ => None,
	}
}

fn limit_streaming_scan_batch(
	batch: RecordBatch,
	remaining: &mut Option<usize>,
) -> Option<RecordBatch> {
	let Some(limit) = remaining.as_mut() else {
		return Some(batch);
	};
	if *limit == 0 {
		return None;
	}
	let batch = if batch.num_rows() > *limit {
		batch.slice(0, *limit)
	} else {
		batch
	};
	*limit = (*limit).saturating_sub(batch.num_rows());
	Some(batch)
}

fn estimate_streaming_scan_build_batch_bytes(
	batch: &RecordBatch,
	retained_columns: Option<&[crate::ir::ColumnOrdinal]>,
	build_keys: &[crate::ir::ColumnOrdinal],
	_null_safe_keys: bool,
) -> Result<u64, ExecutionError> {
	let projected;
	let batch = if let Some(retained_columns) = retained_columns {
		projected = project_batch_columns(batch, retained_columns)?;
		&projected
	} else {
		batch
	};
	let key_arrays = key_arrays(batch, build_keys)?;
	Ok(estimated_build_hash_bytes(&key_arrays, batch.num_rows())
		.max(record_batch_memory_bytes(batch)))
}

fn add_streaming_scan_build_partition_byte_estimates(
	partition_bytes: &mut [u64],
	batch: &RecordBatch,
	retained_columns: Option<&[crate::ir::ColumnOrdinal]>,
	partition_build_keys: &[crate::ir::ColumnOrdinal],
	write_build_keys: &[crate::ir::ColumnOrdinal],
	partition_count: u32,
	null_safe_keys: bool,
) -> Result<(), ExecutionError> {
	let buckets = precompute_join_partition_buckets(
		batch,
		partition_build_keys,
		partition_count,
		null_safe_keys,
		false,
	)?;
	for partition in 0..partition_count {
		let partitioned = select_join_partition_batch_with_buckets(batch, &buckets, partition)?;
		if partitioned.num_rows() == 0 {
			continue;
		}
		let partition_index = usize::try_from(partition).map_err(|_| ExecutionError::Kernel {
			message: "streamed inner build partition exceeds usize".to_string(),
		})?;
		partition_bytes[partition_index] = partition_bytes[partition_index].saturating_add(
			estimate_streaming_scan_build_batch_bytes(
				&partitioned,
				retained_columns,
				write_build_keys,
				null_safe_keys,
			)?,
		);
	}
	Ok(())
}

fn estimate_materialized_inner_build_partition_bytes(
	build_batches: &[RecordBatch],
	build_keys: &[crate::ir::ColumnOrdinal],
	partition_count: u32,
	null_safe_keys: bool,
) -> Result<Vec<u64>, ExecutionError> {
	let partition_count_usize =
		usize::try_from(partition_count).map_err(|_| ExecutionError::Kernel {
			message: "streamed inner build partition count exceeds usize".to_string(),
		})?;
	let mut partition_bytes = vec![0u64; partition_count_usize];
	for batch in build_batches {
		add_streaming_scan_build_partition_byte_estimates(
			&mut partition_bytes,
			batch,
			None,
			build_keys,
			build_keys,
			partition_count,
			null_safe_keys,
		)?;
	}
	Ok(partition_bytes)
}

fn streamed_inner_build_partition_count(
	estimated_build_bytes: u64,
	memory_cap: u64,
	spill_budget: u64,
) -> Result<u32, ExecutionError> {
	let resident_cap = join_spill_retained_footprint_memory_cap(memory_cap);
	let memory_partitions = streamed_spill_partition_count(estimated_build_bytes, resident_cap)?;
	let spill_partitions = partition_count_for_target_bytes(
		estimated_build_bytes,
		streamed_inner_build_writer_target_bytes(spill_budget),
		"streamed inner join build spill partition sizing",
		spill_budget,
	)?;
	Ok(memory_partitions.max(spill_partitions))
}

#[cfg(test)]
fn streamed_inner_build_group_count(
	estimated_build_bytes: u64,
	spill_budget: u64,
	partition_count: u32,
) -> Result<u32, ExecutionError> {
	let partition_count_usize =
		usize::try_from(partition_count).map_err(|_| ExecutionError::Kernel {
			message: "streamed inner build partition count exceeds usize".to_string(),
		})?;
	if partition_count_usize == 0 {
		return Err(ExecutionError::Kernel {
			message: "streamed inner build partition count must be positive".to_string(),
		});
	}
	let per_partition =
		evenly_distributed_partition_bytes(estimated_build_bytes, partition_count_usize)?;
	let (group_count, _) =
		streamed_inner_build_group_count_for_partition_spill_bytes(&per_partition, spill_budget)?;
	Ok(group_count)
}

fn streamed_inner_build_writer_target_bytes(spill_budget: u64) -> u64 {
	spill_budget.saturating_div(4).max(1)
}

fn streamed_inner_build_group_spill_budget(spill_budget: u64) -> u64 {
	spill_budget.saturating_mul(3).saturating_div(4).max(1)
}

#[cfg(test)]
fn streamed_inner_build_group_overhead_reserve_bytes(spill_budget: u64) -> u64 {
	spill_budget.saturating_sub(streamed_inner_build_group_spill_budget(spill_budget))
}

fn ceil_div_u64(numerator: u64, denominator: u64) -> u64 {
	if numerator == 0 {
		return 0;
	}
	let denominator = denominator.max(1);
	numerator.saturating_add(denominator.saturating_sub(1)) / denominator
}

#[cfg(test)]
fn evenly_distributed_partition_bytes(
	total_bytes: u64,
	partition_count: usize,
) -> Result<Vec<u64>, ExecutionError> {
	if partition_count == 0 {
		return Err(ExecutionError::Kernel {
			message: "streamed inner build partition count must be positive".to_string(),
		});
	}
	let partition_count_u64 =
		u64::try_from(partition_count).map_err(|_| ExecutionError::Kernel {
			message: "streamed inner build partition count exceeds u64".to_string(),
		})?;
	let base = total_bytes / partition_count_u64;
	let remainder = total_bytes % partition_count_u64;
	let mut bytes = vec![base; partition_count];
	for slot in bytes.iter_mut().take(
		usize::try_from(remainder)
			.unwrap_or(usize::MAX)
			.min(partition_count),
	) {
		*slot = slot.saturating_add(1);
	}
	Ok(bytes)
}

fn max_partition_group_spill_bytes(
	partition_spill_bytes: &[u64],
	group_count: u32,
) -> Result<u64, ExecutionError> {
	let partition_count =
		u32::try_from(partition_spill_bytes.len()).map_err(|_| ExecutionError::Kernel {
			message: "streamed inner build partition count exceeds u32".to_string(),
		})?;
	if partition_count == 0 {
		return Err(ExecutionError::Kernel {
			message: "streamed inner build partition count must be positive".to_string(),
		});
	}
	let mut max_group = 0u64;
	for group in 0..group_count {
		let range = partition_group_bounds(partition_count, group_count, group)?;
		let group_bytes = partition_spill_bytes[range]
			.iter()
			.fold(0u64, |acc, bytes| acc.saturating_add(*bytes));
		max_group = max_group.max(group_bytes);
	}
	Ok(max_group)
}

fn streamed_inner_build_group_count_for_partition_spill_bytes(
	partition_spill_bytes: &[u64],
	spill_budget: u64,
) -> Result<(u32, u64), ExecutionError> {
	let partition_count =
		u32::try_from(partition_spill_bytes.len()).map_err(|_| ExecutionError::Kernel {
			message: "streamed inner build partition count exceeds u32".to_string(),
		})?;
	if partition_count == 0 {
		return Err(ExecutionError::Kernel {
			message: "streamed inner build partition count must be positive".to_string(),
		});
	}
	let group_spill_budget = streamed_inner_build_group_spill_budget(spill_budget);
	let total_spill = partition_spill_bytes
		.iter()
		.fold(0u64, |acc, bytes| acc.saturating_add(*bytes));
	let target_groups = ceil_div_u64(total_spill, group_spill_budget)
		.max(1)
		.min(u64::from(partition_count));
	let mut group_count = u32::try_from(target_groups).map_err(|_| ExecutionError::Kernel {
		message: "streamed inner build group count exceeds u32".to_string(),
	})?;
	if partition_count > 1 {
		group_count = group_count.max(2).min(partition_count);
	}
	loop {
		let max_group = max_partition_group_spill_bytes(partition_spill_bytes, group_count)?;
		if max_group <= group_spill_budget {
			return Ok((group_count, max_group));
		}
		if group_count >= partition_count {
			return Err(budget_exhausted(
				BudgetDimension::SpillBytes,
				"streamed inner join build partition group sizing",
				max_group,
				group_spill_budget,
			));
		}
		group_count = group_count.saturating_add(1).min(partition_count);
	}
}

fn next_streamed_inner_build_partition_count(partition_count: u32) -> Option<u32> {
	if partition_count >= STREAMED_INNER_BUILD_MAX_PARTITIONS {
		return None;
	}
	Some(
		partition_count
			.saturating_mul(2)
			.max(1)
			.min(STREAMED_INNER_BUILD_MAX_PARTITIONS),
	)
}

fn choose_materialized_inner_build_partitioning(
	estimated_build_bytes: u64,
	memory_cap: u64,
	spill_budget: u64,
	build_batches: &[RecordBatch],
	build_keys: &[crate::ir::ColumnOrdinal],
	null_safe_keys: bool,
) -> Result<(u32, u32, u64), ExecutionError> {
	let mut partition_count =
		streamed_inner_build_partition_count(estimated_build_bytes, memory_cap, spill_budget)?;
	loop {
		let partition_bytes = estimate_materialized_inner_build_partition_bytes(
			build_batches,
			build_keys,
			partition_count,
			null_safe_keys,
		)?;
		match streamed_inner_build_group_count_for_partition_spill_bytes(
			&partition_bytes,
			spill_budget,
		) {
			Ok((group_count, max_group_spill)) => {
				return Ok((partition_count, group_count, max_group_spill));
			}
			Err(error) => {
				let Some(next_partition_count) =
					next_streamed_inner_build_partition_count(partition_count)
				else {
					return Err(error);
				};
				if next_partition_count == partition_count {
					return Err(error);
				}
				partition_count = next_partition_count;
			}
		}
	}
}

fn partition_count_for_target_bytes(
	estimated_bytes: u64,
	target_bytes: u64,
	producer: &'static str,
	available: u64,
) -> Result<u32, ExecutionError> {
	const MAX_PARTITIONS: u64 = 1024;
	let target = target_bytes.max(1);
	let needed = estimated_bytes.saturating_add(target.saturating_sub(1)) / target;
	let needed = needed.max(1);
	let Some(partitions) = needed.checked_next_power_of_two() else {
		return Err(budget_exhausted(
			BudgetDimension::SpillBytes,
			producer,
			estimated_bytes,
			available,
		));
	};
	if partitions > MAX_PARTITIONS {
		return Err(budget_exhausted(
			BudgetDimension::SpillBytes,
			producer,
			estimated_bytes,
			available,
		));
	}
	u32::try_from(partitions).map_err(|_| ExecutionError::Kernel {
		message: "streamed inner build partition count exceeds u32".to_string(),
	})
}

fn partition_group_bounds(
	partition_count: u32,
	group_count: u32,
	group: u32,
) -> Result<std::ops::Range<usize>, ExecutionError> {
	if partition_count == 0 || group_count == 0 || group >= group_count {
		return Err(ExecutionError::Kernel {
			message: "invalid streamed inner build partition group".to_string(),
		});
	}
	let partition_count = usize::try_from(partition_count).map_err(|_| ExecutionError::Kernel {
		message: "streamed inner build partition count exceeds usize".to_string(),
	})?;
	let group_count = usize::try_from(group_count).map_err(|_| ExecutionError::Kernel {
		message: "streamed inner build group count exceeds usize".to_string(),
	})?;
	let group = usize::try_from(group).map_err(|_| ExecutionError::Kernel {
		message: "streamed inner build group index exceeds usize".to_string(),
	})?;
	let start = group.saturating_mul(partition_count) / group_count;
	let end = (group.saturating_add(1)).saturating_mul(partition_count) / group_count;
	Ok(start..end)
}

fn parallel_partition_plan(
	partitioned_builds: &[Option<SpilledInnerJoinBuild>],
	group_probe_batches: &[(usize, RecordBatch)],
	key_plan: JoinKeyPlan,
	configured_parallelism: usize,
	memory_cap: u64,
) -> ParallelPartitionPlan {
	let configured = configured_parallelism.max(1);
	let available = std::thread::available_parallelism()
		.map(std::num::NonZeroUsize::get)
		.unwrap_or(1)
		.max(1);
	let requested = configured.min(available).max(1);
	let resident_peak = group_probe_batches
		.iter()
		.filter_map(|(partition, probe)| {
			let spilled = partitioned_builds.get(*partition)?.as_ref()?;
			Some(estimate_parallel_partition_resident(
				spilled, probe, key_plan,
			))
		})
		.fold(0u64, u64::max);
	let budget = join_spill_retained_footprint_memory_cap(memory_cap).max(1);
	let memory_k = if resident_peak == 0 {
		requested
	} else {
		usize::try_from(budget / resident_peak)
			.unwrap_or(usize::MAX)
			.max(1)
	};
	ParallelPartitionPlan {
		k: requested.min(memory_k).max(1),
		resident_peak,
	}
}

fn estimate_parallel_partition_resident(
	spilled: &SpilledInnerJoinBuild,
	probe: &RecordBatch,
	key_plan: JoinKeyPlan,
) -> u64 {
	let loaded_pages = spilled.loaded_page_bytes_estimate();
	let index =
		estimate_loaded_partition_index_bytes(spilled.input_row_count(), key_plan, loaded_pages);
	loaded_pages
		.saturating_add(index)
		.saturating_add(spilled.writer_peak_bytes())
		.saturating_add(64 * 1024)
		.saturating_add(record_batch_memory_bytes(probe))
}

fn estimate_loaded_partition_index_bytes(
	rows: u64,
	key_plan: JoinKeyPlan,
	loaded_pages_bytes: u64,
) -> u64 {
	if rows == 0 {
		return 0;
	}
	let key_heap_bytes = match key_plan {
		JoinKeyPlan::Int32 => std::mem::size_of::<i32>() as u64,
		JoinKeyPlan::Int64 => std::mem::size_of::<i64>() as u64,
		JoinKeyPlan::Generic => loaded_pages_bytes
			.saturating_add(rows.saturating_sub(1))
			.saturating_div(rows)
			.max(16),
	};
	rows.saturating_mul(
		64u64
			.saturating_add(key_heap_bytes)
			.saturating_add(std::mem::size_of::<SpilledBatchRowRef>() as u64),
	)
}

pub(in crate::exec) fn decompose_streaming_existence_plan(
	plan: &ExecutionPlan,
) -> Option<(&ExecutionPlan, Vec<StreamingExistenceOp<'_>>)> {
	let mut current = plan;
	let mut ops = Vec::new();
	loop {
		match current {
			ExecutionPlan::Exchange {
				input,
				partition_count,
				key,
			} => {
				ops.push(StreamingExistenceOp::Exchange {
					partition_count: *partition_count,
					key,
				});
				current = input.as_ref();
			}
			ExecutionPlan::Filter {
				input,
				predicate,
				disposition,
			} => {
				if *disposition == FilterDisposition::EvaluateInToron {
					ops.push(StreamingExistenceOp::Filter(predicate));
				}
				current = input.as_ref();
			}
			ExecutionPlan::Project {
				input,
				exprs,
				output_schema,
			} => {
				ops.push(StreamingExistenceOp::Project {
					exprs,
					output_schema: output_schema.clone(),
				});
				current = input.as_ref();
			}
			ExecutionPlan::Scan { .. } => {
				ops.reverse();
				return Some((current, ops));
			}
			_ => return None,
		}
	}
}

fn streaming_existence_key_count_hint(
	plan: &ExecutionPlan,
	existence_key: ColumnOrdinal,
) -> Option<usize> {
	let (scan, ops) = decompose_streaming_existence_plan(plan)?;
	let mut scan_output_ordinal = existence_key;
	for op in ops.iter().rev() {
		if let StreamingExistenceOp::Project { exprs, .. } = op {
			let columns = exprs.root_columns()?;
			scan_output_ordinal = columns.get(scan_output_ordinal.0 as usize)?.ordinal;
		}
	}
	let ExecutionPlan::Scan {
		projection,
		column_ndv,
		..
	} = scan
	else {
		return None;
	};
	let descriptor_ordinal = projection
		.as_ref()
		.and_then(|columns| columns.get(scan_output_ordinal.0 as usize).copied())
		.unwrap_or(scan_output_ordinal);
	let count = column_ndv.iter().find_map(|(column, fact)| {
		if *column != descriptor_ordinal {
			return None;
		}
		match fact {
			crate::catalog::DistinctCountFact::Exact(count) => Some(*count),
			crate::catalog::DistinctCountFact::Approx { .. } => None,
		}
	})?;
	usize::try_from(count).ok()
}

pub(in crate::exec) fn extract_streaming_inner_prefix_semi_anti_chain(
	top: &ExecutionPlan,
) -> Option<StreamingSemiAntiChain<'_>> {
	let ExecutionPlan::Join {
		replay_id: anti_replay_id,
		left: anti_left,
		right: anti_right,
		kind: JoinKind::LeftAnti,
		left_keys: anti_left_keys,
		right_keys: anti_right_keys,
		match_filter: Some(anti_filter),
		null_safe_keys: false,
		strategy: JoinStrategy::HashPartitioned,
		output_schema: anti_output_schema,
		..
	} = top
	else {
		return None;
	};
	if *anti_replay_id != JoinReplayId(4) {
		return None;
	}
	let semi = exchange_input(anti_left)?;
	let ExecutionPlan::Join {
		replay_id: semi_replay_id,
		left: semi_left,
		right: semi_right,
		kind: JoinKind::LeftSemi,
		left_keys: semi_left_keys,
		right_keys: semi_right_keys,
		match_filter: Some(semi_filter),
		null_safe_keys: false,
		strategy: JoinStrategy::HashPartitioned,
		output_schema: semi_output_schema,
		..
	} = semi
	else {
		return None;
	};
	if *semi_replay_id != JoinReplayId(3) {
		return None;
	}

	let (join2, join2_post_filter) = decompose_join_or_filtered_join(exchange_input(semi_left)?)?;
	let ExecutionPlan::Join {
		replay_id: join2_replay_id,
		left: join2_left,
		right: join2_right,
		kind: JoinKind::Inner,
		left_keys: join2_left_keys,
		right_keys: join2_right_keys,
		match_filter: None,
		null_safe_keys: false,
		strategy: JoinStrategy::HashPartitioned,
		output_schema: join2_output_schema,
		..
	} = join2
	else {
		return None;
	};
	if *join2_replay_id != JoinReplayId(2) {
		return None;
	}

	let (join1, join1_post_filter) = decompose_join_or_filtered_join(exchange_input(join2_left)?)?;
	let ExecutionPlan::Join {
		replay_id: join1_replay_id,
		left: join1_left,
		right: join1_right,
		kind: JoinKind::Inner,
		left_keys: join1_left_keys,
		right_keys: join1_right_keys,
		match_filter: None,
		null_safe_keys: false,
		strategy: JoinStrategy::HashPartitioned,
		output_schema: join1_output_schema,
		..
	} = join1
	else {
		return None;
	};
	if *join1_replay_id != JoinReplayId(1) {
		return None;
	}

	Some(StreamingSemiAntiChain {
		base: join1_left.as_ref(),
		inner_stages: vec![
			ReorientedInnerJoinStage {
				replay_id: *join1_replay_id,
				probe_schema: join1_left.schema(),
				probe_keys: join1_left_keys.clone(),
				build: join1_right.as_ref(),
				build_keys: join1_right_keys.clone(),
				retained_build_columns: None,
				output_schema: join1_output_schema.clone(),
				strategy: JoinStrategy::HashPartitioned,
				post_filter: join1_post_filter,
				probe_columns_first: true,
			},
			ReorientedInnerJoinStage {
				replay_id: *join2_replay_id,
				probe_schema: join2_left.schema(),
				probe_keys: join2_left_keys.clone(),
				build: join2_right.as_ref(),
				build_keys: join2_right_keys.clone(),
				retained_build_columns: None,
				output_schema: join2_output_schema.clone(),
				strategy: JoinStrategy::HashPartitioned,
				post_filter: join2_post_filter,
				probe_columns_first: true,
			},
		],
		semi_stage: StreamingSemiAntiStage {
			replay_id: *semi_replay_id,
			kind: JoinKind::LeftSemi,
			preserved_keys: semi_left_keys.clone(),
			existence: semi_right.as_ref(),
			existence_keys: semi_right_keys.clone(),
			match_filter: Some(semi_filter),
			output_schema: semi_output_schema.clone(),
			strategy: JoinStrategy::HashPartitioned,
		},
		anti_stage: StreamingSemiAntiStage {
			replay_id: *anti_replay_id,
			kind: JoinKind::LeftAnti,
			preserved_keys: anti_left_keys.clone(),
			existence: anti_right.as_ref(),
			existence_keys: anti_right_keys.clone(),
			match_filter: Some(anti_filter),
			output_schema: anti_output_schema.clone(),
			strategy: JoinStrategy::HashPartitioned,
		},
	})
}
impl Executor {
	pub(in crate::exec) async fn try_streaming_join0_filter(
		&self,
		input: &ExecutionPlan,
		predicate: &BoundExprProgram,
		disposition: FilterDisposition,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
		if disposition == FilterDisposition::FullyPushed {
			return Ok(None);
		}

		let mut output = Vec::new();
		let mut accumulated_bytes = 0u64;
		let accumulator_cap =
			join_output_materialization_memory_cap(self.context.task_budget.memory.get());
		let matched = self
			.try_visit_streaming_join0_filter_chunks(
				input,
				predicate,
				disposition,
				morsel,
				|batch| {
					accumulated_bytes =
						accumulated_bytes.saturating_add(record_batch_memory_bytes(&batch));
					if accumulated_bytes > accumulator_cap {
						return Err(budget_exhausted(
							BudgetDimension::Memory,
							"streamed join filter accumulator",
							accumulated_bytes,
							accumulator_cap,
						));
					}
					if batch.num_rows() > 0 {
						output.push(batch);
					}
					Ok(())
				},
			)
			.await?;
		if matched.is_none() {
			return Ok(None);
		}
		if output.is_empty() {
			output.push(RecordBatch::new_empty(input.schema()));
		}
		Ok(Some(output))
	}

	pub(in crate::exec) async fn try_streaming_join0_filter_exchange(
		&self,
		input: &ExecutionPlan,
		partition_count: u32,
		key: &BoundExprProgram,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
		let ExecutionPlan::Filter {
			input: filter_input,
			predicate,
			disposition,
		} = input
		else {
			return Ok(None);
		};
		if partition_count == 0 {
			return Err(ExecutionError::InvalidExpression {
				message: "exchange partition_count must be greater than zero".to_string(),
			});
		}

		let schema = input.schema();
		let partition_count_usize =
			usize::try_from(partition_count).expect("partition count fits usize");
		let mut partition_batches: Vec<Vec<RecordBatch>> =
			(0..partition_count_usize).map(|_| Vec::new()).collect();
		let accumulator_cap =
			join_output_materialization_memory_cap(self.context.task_budget.memory.get());
		let mut accumulated_bytes = 0u64;
		let matched = self
			.try_visit_streaming_join0_filter_chunks(
				filter_input,
				predicate,
				*disposition,
				morsel,
				|batch| {
					for (partition, partitioned) in
						self.repartition_exchange_batch(&batch, partition_count, key)?
					{
						accumulated_bytes = accumulated_bytes
							.saturating_add(record_batch_memory_bytes(&partitioned));
						if accumulated_bytes > accumulator_cap {
							return Err(budget_exhausted(
								BudgetDimension::Memory,
								"streamed exchange output accumulator",
								accumulated_bytes,
								accumulator_cap,
							));
						}
						partition_batches[partition].push(partitioned);
					}
					Ok(())
				},
			)
			.await?;
		if matched.is_none() {
			return Ok(None);
		}
		Ok(Some(finalize_exchange_partitions(
			schema,
			partition_batches,
		)?))
	}

	pub(in crate::exec) async fn try_streaming_correlated_scalar_left_outer_global_aggregate(
		&self,
		input: &ExecutionPlan,
		aggr_expr: &[BoundAggregateCall],
		output_schema: &arrow::datatypes::SchemaRef,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
		let ExecutionPlan::Filter {
			input: recombine,
			predicate: recombine_filter,
			disposition: FilterDisposition::EvaluateInToron,
		} = input
		else {
			return Ok(None);
		};
		let ExecutionPlan::Join {
			replay_id,
			left,
			right,
			kind: JoinKind::LeftOuter,
			left_keys,
			right_keys,
			match_filter,
			null_safe_keys,
			strategy,
			output_schema: recombine_output_schema,
			..
		} = recombine.as_ref()
		else {
			return Ok(None);
		};
		let ExecutionPlan::Exchange {
			input: preserved_exchange_input,
			partition_count,
			key: preserved_exchange_key,
		} = left.as_ref()
		else {
			return Ok(None);
		};
		let Some((preserved_join, preserved_filter)) =
			decompose_join_or_filtered_join(preserved_exchange_input.as_ref())
		else {
			return Ok(None);
		};
		if *partition_count == 0 {
			return Err(ExecutionError::InvalidExpression {
				message: "exchange partition_count must be greater than zero".to_string(),
			});
		}

		let scalar_batches = self.execute_bound(right, morsel).await?;
		let scalar_batch = concat_or_empty(right.schema(), &scalar_batches)?;
		let scalar_key_arrays = key_arrays(&scalar_batch, right_keys)?;
		let key_plan = JoinKeyPlan::from_build_arrays_and_probe_schema(
			&scalar_key_arrays,
			&left.schema(),
			left_keys,
			*null_safe_keys,
		)?;
		let build_cap =
			join_output_materialization_memory_cap(self.context.task_budget.memory.get());
		let estimated_build_bytes =
			estimated_build_hash_bytes(&scalar_key_arrays, scalar_batch.num_rows());
		if estimated_build_bytes > build_cap {
			return Err(budget_exhausted(
				BudgetDimension::Memory,
				"correlated scalar recombine build accumulator",
				estimated_build_bytes,
				build_cap,
			));
		}
		let Some(scalar_rows) = build_in_memory_join_rows(
			key_plan,
			&scalar_key_arrays,
			scalar_batch.num_rows(),
			build_cap,
			*null_safe_keys,
		)?
		else {
			return Err(budget_exhausted(
				BudgetDimension::Memory,
				"correlated scalar recombine build accumulator",
				estimated_build_bytes,
				build_cap,
			));
		};
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.build_rows = task
				.counters
				.build_rows
				.saturating_add(scalar_batch.num_rows() as u64);
		});
		let mut runtime = StreamingLeftOuterRecombineRuntime {
			replay_id: *replay_id,
			strategy: *strategy,
			scalar_batch,
			scalar_rows,
			key_plan,
			preserved_keys: left_keys.clone(),
			match_filter: match_filter.as_ref(),
			null_safe_keys: *null_safe_keys,
			output_schema: recombine_output_schema.clone(),
			probe_rows: 0,
			output_rows: 0,
		};

		let partition_count_usize =
			usize::try_from(*partition_count).expect("partition count fits usize");
		let mut partition_batches: Vec<Vec<RecordBatch>> =
			(0..partition_count_usize).map(|_| Vec::new()).collect();
		let accumulator_cap =
			join_output_materialization_memory_cap(self.context.task_budget.memory.get());
		let mut accumulated_bytes = 0u64;
		let matched = Box::pin(self.try_visit_streaming_join0_chunks(
			preserved_join,
			preserved_filter,
			morsel,
			None,
			|batch| {
				for (partition, partitioned) in self.repartition_exchange_batch(
					&batch,
					*partition_count,
					preserved_exchange_key,
				)? {
					accumulated_bytes =
						accumulated_bytes.saturating_add(record_batch_memory_bytes(&partitioned));
					if accumulated_bytes > accumulator_cap {
						return Err(budget_exhausted(
							BudgetDimension::Memory,
							"correlated scalar preserved-side exchange accumulator",
							accumulated_bytes,
							accumulator_cap,
						));
					}
					if partitioned.num_rows() > 0 {
						partition_batches[partition].push(partitioned);
					}
				}
				Ok(())
			},
		))
		.await?;
		if matched.is_none() {
			return Ok(None);
		}

		let mut state = init_global_aggregate_state(aggr_expr)?;
		for partition in partition_batches {
			for preserved_batch in partition {
				let recombined_batches =
					self.probe_streaming_left_outer_recombine(&mut runtime, &preserved_batch)?;
				for recombined in recombined_batches {
					let filtered = self.filter_batch_with_bound_predicate(
						recombine_filter,
						recombined,
						"filter predicate",
					)?;
					if filtered.num_rows() > 0 {
						self.fold_global_aggregate_state(&mut state, aggr_expr, &filtered)?;
					}
				}
			}
		}
		record_join_runtime_observation(
			&self.context.task_telemetry,
			&self.context.join_runtime_observation_mode,
			JoinRuntimeObservation {
				replay_id: runtime.replay_id,
				strategy: runtime.strategy,
				build_rows: RowCount::new(runtime.scalar_batch.num_rows() as u64),
				probe_rows: RowCount::new(runtime.probe_rows),
				output_rows: RowCount::new(runtime.output_rows),
			},
		);

		finalize_global_aggregate_state(&state, aggr_expr, output_schema).map(Some)
	}

	pub(in crate::exec) async fn bound_sort(
		&self,
		input: &ExecutionPlan,
		order_by: &[ExecutionSortKey],
		limit: Option<usize>,
		offset: usize,
		output_schema: &arrow::datatypes::SchemaRef,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		if let Some(output) = Box::pin(self.try_streaming_correlated_scalar_left_outer_sort(
			input,
			order_by,
			limit,
			offset,
			output_schema,
			morsel,
		))
		.await?
		{
			return Ok(output);
		}
		let input_batches = self.execute_bound(input, morsel).await?;
		execute_sort(
			input.schema(),
			&input_batches,
			order_by,
			limit,
			offset,
			output_schema,
			self.context.task_budget,
		)
	}

	#[allow(clippy::too_many_arguments)]
	async fn try_streaming_correlated_scalar_left_outer_sort(
		&self,
		input: &ExecutionPlan,
		order_by: &[ExecutionSortKey],
		limit: Option<usize>,
		offset: usize,
		output_schema: &arrow::datatypes::SchemaRef,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
		let ExecutionPlan::Exchange {
			input: project,
			partition_count: 1,
			..
		} = input
		else {
			return Ok(None);
		};
		let ExecutionPlan::Project {
			input: filter,
			exprs: project_exprs,
			output_schema: project_output_schema,
		} = project.as_ref()
		else {
			return Ok(None);
		};
		let ExecutionPlan::Filter {
			input: recombine,
			predicate: recombine_filter,
			disposition: FilterDisposition::EvaluateInToron,
		} = filter.as_ref()
		else {
			return Ok(None);
		};
		let ExecutionPlan::Join {
			replay_id,
			left,
			right,
			kind: JoinKind::LeftOuter,
			left_keys,
			right_keys,
			match_filter,
			null_safe_keys,
			strategy: JoinStrategy::HashPartitioned,
			output_schema: recombine_output_schema,
			..
		} = recombine.as_ref()
		else {
			return Ok(None);
		};
		let ExecutionPlan::Exchange {
			input: preserved_exchange_input,
			partition_count,
			key: preserved_exchange_key,
		} = left.as_ref()
		else {
			return Ok(None);
		};
		if *partition_count == 0
			|| extract_reoriented_inner_chain(preserved_exchange_input.as_ref()).is_none()
		{
			return Ok(None);
		}

		let scalar_batches = self.execute_bound(right, morsel).await?;
		let scalar_batch = concat_or_empty(right.schema(), &scalar_batches)?;
		let scalar_key_arrays = key_arrays(&scalar_batch, right_keys)?;
		let key_plan = JoinKeyPlan::from_build_arrays_and_probe_schema(
			&scalar_key_arrays,
			&left.schema(),
			left_keys,
			*null_safe_keys,
		)?;
		let build_cap =
			join_output_materialization_memory_cap(self.context.task_budget.memory.get());
		let estimated_build_bytes =
			estimated_build_hash_bytes(&scalar_key_arrays, scalar_batch.num_rows());
		if estimated_build_bytes > build_cap {
			return Err(budget_exhausted(
				BudgetDimension::Memory,
				"correlated scalar recombine build accumulator",
				estimated_build_bytes,
				build_cap,
			));
		}
		let Some(scalar_rows) = build_in_memory_join_rows(
			key_plan,
			&scalar_key_arrays,
			scalar_batch.num_rows(),
			build_cap,
			*null_safe_keys,
		)?
		else {
			return Err(budget_exhausted(
				BudgetDimension::Memory,
				"correlated scalar recombine build accumulator",
				estimated_build_bytes,
				build_cap,
			));
		};
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.build_rows = task
				.counters
				.build_rows
				.saturating_add(scalar_batch.num_rows() as u64);
		});
		let mut runtime = StreamingLeftOuterRecombineRuntime {
			replay_id: *replay_id,
			strategy: JoinStrategy::HashPartitioned,
			scalar_batch,
			scalar_rows,
			key_plan,
			preserved_keys: left_keys.clone(),
			match_filter: match_filter.as_ref(),
			null_safe_keys: *null_safe_keys,
			output_schema: recombine_output_schema.clone(),
			probe_rows: 0,
			output_rows: 0,
		};

		let accumulator_cap =
			sort_materialization_memory_cap(self.context.task_budget.memory.get());
		let mut accumulated_bytes = 0u64;
		let mut survivors = Vec::new();
		let matched = self
			.try_visit_streaming_reoriented_inner_chain(
				preserved_exchange_input,
				morsel,
				&mut |batch| {
					for (_, partitioned) in self.repartition_exchange_batch(
						&batch,
						*partition_count,
						preserved_exchange_key,
					)? {
						for recombined in
							self.probe_streaming_left_outer_recombine(&mut runtime, &partitioned)?
						{
							let filtered = self.filter_batch_with_bound_predicate(
								recombine_filter,
								recombined,
								"filter predicate",
							)?;
							if filtered.num_rows() == 0 {
								continue;
							}
							let projected = RecordBatch::try_new(
								project_output_schema.clone(),
								self.evaluate_bound_program(project_exprs, &filtered)?,
							)?;
							accumulated_bytes = account_streamed_correlated_sort_input(
								accumulated_bytes,
								&projected,
								accumulator_cap,
							)?;
							survivors.push(projected);
						}
					}
					Ok(())
				},
			)
			.await?;
		if matched.is_none() {
			return Ok(None);
		}
		record_join_runtime_observation(
			&self.context.task_telemetry,
			&self.context.join_runtime_observation_mode,
			JoinRuntimeObservation {
				replay_id: runtime.replay_id,
				strategy: runtime.strategy,
				build_rows: RowCount::new(runtime.scalar_batch.num_rows() as u64),
				probe_rows: RowCount::new(runtime.probe_rows),
				output_rows: RowCount::new(runtime.output_rows),
			},
		);
		if survivors.is_empty() {
			survivors.push(RecordBatch::new_empty(project_output_schema.clone()));
		}
		execute_sort(
			input.schema(),
			&survivors,
			order_by,
			limit,
			offset,
			output_schema,
			self.context.task_budget,
		)
		.map(Some)
	}

	pub(in crate::exec) async fn try_visit_streaming_join0_filter_chunks<F>(
		&self,
		input: &ExecutionPlan,
		predicate: &BoundExprProgram,
		disposition: FilterDisposition,
		morsel: Option<(MorselId, u32)>,
		visit_filtered_batch: F,
	) -> Result<Option<()>, ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		if disposition == FilterDisposition::FullyPushed {
			return Ok(None);
		}
		Box::pin(self.try_visit_streaming_join0_chunks(
			input,
			Some(predicate),
			morsel,
			None,
			visit_filtered_batch,
		))
		.await
	}

	#[allow(clippy::too_many_arguments)]
	pub(in crate::exec) async fn try_visit_streaming_inner_join_scan_probe_chunks<F>(
		&self,
		build_plan: &ExecutionPlan,
		probe_plan: &ExecutionPlan,
		build_batches: Option<&[RecordBatch]>,
		build_keys: &[crate::ir::ColumnOrdinal],
		probe_keys: &[crate::ir::ColumnOrdinal],
		match_filter: Option<&BoundExprProgram>,
		null_safe_keys: bool,
		replay_id: JoinReplayId,
		strategy: JoinStrategy,
		output_schema: &arrow::datatypes::SchemaRef,
		filter_predicate: Option<&BoundExprProgram>,
		probe_columns_first: bool,
		morsel: Option<(MorselId, u32)>,
		profile: Option<&GlobalFoldJoinFilterProfile>,
		visit_batch: &mut F,
	) -> Result<Option<()>, ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		if probe_columns_first && match_filter.is_some() {
			return Ok(None);
		}
		let Some(scan_input) = decompose_streaming_scan_probe_input(probe_plan) else {
			return Ok(None);
		};
		if self.context.task_budget.memory.get() < 64 * 1024 * 1024 {
			return Ok(None);
		}
		let probe_prefilter_predicate = filter_predicate.filter(|predicate| {
			predicate_references_only_probe_prefix(
				predicate,
				probe_columns_first,
				probe_plan.schema().fields().len(),
			)
		});
		let join_filter_predicate = if probe_prefilter_predicate.is_some() {
			None
		} else {
			filter_predicate
		};
		let retain_fitting_scan_build = retain_fitting_scan_build_enabled();
		let mut resident_scan_build = None;
		let scan_build_estimate = if build_batches.is_none() {
			let scan_input = decompose_streaming_scan_probe_input(build_plan)
				.expect("scan build was established above");
			let (estimated, batches) = if retain_fitting_scan_build {
				self.load_or_estimate_streaming_scan_inner_build(
					scan_input,
					None,
					build_keys,
					null_safe_keys,
					morsel,
					self.context.task_budget.memory.get(),
				)
				.await?
			} else {
				(
					self.estimate_streaming_scan_inner_build_bytes(
						scan_input,
						None,
						build_keys,
						null_safe_keys,
						morsel,
					)
					.await?,
					None,
				)
			};
			resident_scan_build = batches;
			Some(estimated)
		} else {
			None
		};
		if !retain_fitting_scan_build
			&& matches!(
				scan_build_estimate,
				Some(estimated) if estimated <= self.context.task_budget.memory.get()
			) {
			// ADR 493 control: preserve the prior estimate-then-materialize behavior.
			return Ok(None);
		}

		let build_started = Instant::now();
		if let Some(estimated_build_bytes) = scan_build_estimate
			&& resident_scan_build.is_none()
			&& let Some((build_rows, probe_rows, output_rows)) = self
				.try_visit_partitioned_streamed_inner_scan_build_scan_probe_chunks(
					build_plan,
					probe_plan,
					scan_input,
					build_keys,
					probe_keys,
					match_filter,
					null_safe_keys,
					output_schema,
					probe_prefilter_predicate,
					join_filter_predicate,
					probe_columns_first,
					morsel,
					replay_id,
					estimated_build_bytes,
					profile,
					&mut *visit_batch,
				)
				.await?
		{
			record_join_runtime_observation(
				&self.context.task_telemetry,
				&self.context.join_runtime_observation_mode,
				JoinRuntimeObservation {
					replay_id,
					strategy,
					build_rows: RowCount::new(build_rows),
					probe_rows: RowCount::new(probe_rows),
					output_rows: RowCount::new(output_rows),
				},
			);
			return Ok(Some(()));
		}
		if scan_build_estimate.is_some()
			&& resident_scan_build.is_none()
			&& let Some((mut spilled, key_plan)) = self
				.try_create_streamed_spilled_inner_scan_build(
					build_plan,
					probe_plan,
					build_keys,
					probe_keys,
					match_filter,
					null_safe_keys,
					morsel,
				)
				.await?
		{
			let attributed_spill_bytes = spilled.spill_bytes();
			let materialized_output_schema = if probe_columns_first {
				concat_join_schema(&build_plan.schema(), &probe_plan.schema())
			} else {
				output_schema.clone()
			};
			record_task_telemetry(&self.context.task_telemetry, |task| {
				task.phases.build_micros = task
					.phases
					.build_micros
					.saturating_add(duration_micros(build_started));
				task.counters.build_rows = task
					.counters
					.build_rows
					.saturating_add(spilled.input_row_count());
				task.phases.spill_micros = task
					.phases
					.spill_micros
					.saturating_add(duration_micros(build_started));
				task.counters.spill_bytes = task
					.counters
					.spill_bytes
					.saturating_add(spilled.spill_bytes());
			});
			let probe_started = Instant::now();
			let result = self
				.visit_streaming_spilled_inner_scan_probe_chunks(
					&mut spilled,
					scan_input,
					probe_keys,
					key_plan,
					null_safe_keys,
					&materialized_output_schema,
					output_schema,
					probe_prefilter_predicate,
					join_filter_predicate,
					probe_columns_first,
					build_plan.schema().fields().len(),
					morsel,
					profile,
					visit_batch,
				)
				.await;
			let build_rows = spilled.input_row_count();
			record_task_telemetry(&self.context.task_telemetry, |task| {
				task.phases.probe_micros = task
					.phases
					.probe_micros
					.saturating_add(duration_micros(probe_started));
				if let Ok((probe_rows, output_rows)) = &result {
					task.counters.probe_rows = task.counters.probe_rows.saturating_add(*probe_rows);
					task.counters.matched_rows =
						task.counters.matched_rows.saturating_add(*output_rows);
				}
			});
			if let Ok((probe_rows, output_rows)) = &result {
				record_join_runtime_observation(
					&self.context.task_telemetry,
					&self.context.join_runtime_observation_mode,
					JoinRuntimeObservation {
						replay_id,
						strategy,
						build_rows: RowCount::new(build_rows),
						probe_rows: RowCount::new(*probe_rows),
						output_rows: RowCount::new(*output_rows),
					},
				);
			}
			record_join_spill_observation(
				&self.context.task_telemetry,
				replay_id,
				crate::distributed::task::JoinSpillPath::StreamingScanBuild,
				attributed_spill_bytes,
			);
			spilled.cleanup().await;
			result?;
			return Ok(Some(()));
		}

		let build_batches = build_batches.or(resident_scan_build.as_deref());
		let Some(build_batches) = build_batches else {
			return Ok(None);
		};
		if let Some((build_rows, probe_rows, output_rows)) = self
			.try_visit_partitioned_streamed_inner_materialized_build_scan_probe_chunks(
				build_plan,
				probe_plan,
				scan_input,
				build_batches,
				build_keys,
				probe_keys,
				match_filter,
				null_safe_keys,
				output_schema,
				probe_prefilter_predicate,
				join_filter_predicate,
				probe_columns_first,
				morsel,
				&mut *visit_batch,
			)
			.await?
		{
			record_join_runtime_observation(
				&self.context.task_telemetry,
				&self.context.join_runtime_observation_mode,
				JoinRuntimeObservation {
					replay_id,
					strategy,
					build_rows: RowCount::new(build_rows),
					probe_rows: RowCount::new(probe_rows),
					output_rows: RowCount::new(output_rows),
				},
			);
			return Ok(Some(()));
		}
		if let Some((mut spilled, key_plan)) = try_create_streamed_spilled_inner_build(
			build_plan.schema(),
			probe_plan.schema(),
			build_batches,
			JoinKind::Inner,
			build_keys,
			probe_keys,
			match_filter,
			null_safe_keys,
			self.context.task_budget,
			self.context.join_spill_file_ops.clone(),
			self.context.join_spill_dir.clone(),
		)
		.await?
		{
			let attributed_spill_bytes = spilled.spill_bytes();
			let materialized_output_schema = if probe_columns_first {
				concat_join_schema(&build_plan.schema(), &probe_plan.schema())
			} else {
				output_schema.clone()
			};
			record_task_telemetry(&self.context.task_telemetry, |task| {
				task.phases.build_micros = task
					.phases
					.build_micros
					.saturating_add(duration_micros(build_started));
				task.counters.build_rows = task
					.counters
					.build_rows
					.saturating_add(spilled.input_row_count());
				task.phases.spill_micros = task
					.phases
					.spill_micros
					.saturating_add(duration_micros(build_started));
				task.counters.spill_bytes = task
					.counters
					.spill_bytes
					.saturating_add(spilled.spill_bytes());
			});
			let probe_started = Instant::now();
			let result = self
				.visit_streaming_spilled_inner_scan_probe_chunks(
					&mut spilled,
					scan_input,
					probe_keys,
					key_plan,
					null_safe_keys,
					&materialized_output_schema,
					output_schema,
					probe_prefilter_predicate,
					join_filter_predicate,
					probe_columns_first,
					build_plan.schema().fields().len(),
					morsel,
					profile,
					visit_batch,
				)
				.await;
			let build_rows = spilled.input_row_count();
			record_task_telemetry(&self.context.task_telemetry, |task| {
				task.phases.probe_micros = task
					.phases
					.probe_micros
					.saturating_add(duration_micros(probe_started));
				if let Ok((probe_rows, output_rows)) = &result {
					task.counters.probe_rows = task.counters.probe_rows.saturating_add(*probe_rows);
					task.counters.matched_rows =
						task.counters.matched_rows.saturating_add(*output_rows);
				}
			});
			if let Ok((probe_rows, output_rows)) = &result {
				record_join_runtime_observation(
					&self.context.task_telemetry,
					&self.context.join_runtime_observation_mode,
					JoinRuntimeObservation {
						replay_id,
						strategy,
						build_rows: RowCount::new(build_rows),
						probe_rows: RowCount::new(*probe_rows),
						output_rows: RowCount::new(*output_rows),
					},
				);
			}
			record_join_spill_observation(
				&self.context.task_telemetry,
				replay_id,
				crate::distributed::task::JoinSpillPath::StreamedSpilledBuild,
				attributed_spill_bytes,
			);
			spilled.cleanup().await;
			result?;
			return Ok(Some(()));
		}

		let build_batch = concat_or_empty(build_plan.schema(), build_batches)?;
		let build_key_arrays = key_arrays(&build_batch, build_keys)?;
		if estimated_build_hash_bytes(&build_key_arrays, build_batch.num_rows())
			> self.context.task_budget.memory.get()
		{
			return Ok(None);
		}
		let join_key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&build_plan.schema(),
			build_keys,
			&probe_plan.schema(),
			probe_keys,
			null_safe_keys,
		)?;
		let Some(build_rows) = build_in_memory_join_rows(
			join_key_plan,
			&build_key_arrays,
			build_batch.num_rows(),
			self.context.task_budget.memory.get(),
			null_safe_keys,
		)?
		else {
			return Ok(None);
		};
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.phases.build_micros = task
				.phases
				.build_micros
				.saturating_add(duration_micros(build_started));
			task.counters.build_rows = task
				.counters
				.build_rows
				.saturating_add(build_batch.num_rows() as u64);
		});
		if local_join_phase_enabled() {
			// Hash-build only (concat + key_arrays + table). The build-side scan that produced
			// `build_batches` is timed at its `execute_bound` call sites, or falls to the residual
			// on the physical-scan-build branch.
			local_join_phase_record_elapsed(LocalJoinPhase::Build, build_started);
			local_join_phase_record_flow(
				LocalJoinPhase::Build,
				1,
				build_batch.num_rows() as u64,
				0,
			);
			local_join_phase_set_counts(build_batch.num_rows() as u64, 0, 0);
		}

		let probe_started = Instant::now();
		let result = self
			.visit_streaming_in_memory_inner_scan_probe_chunks(
				&build_batch,
				&build_rows,
				join_key_plan,
				scan_input,
				probe_keys,
				match_filter,
				null_safe_keys,
				output_schema,
				probe_prefilter_predicate,
				join_filter_predicate,
				probe_columns_first,
				morsel,
				visit_batch,
			)
			.await;
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.phases.probe_micros = task
				.phases
				.probe_micros
				.saturating_add(duration_micros(probe_started));
			if let Ok((probe_rows, output_rows)) = &result {
				task.counters.probe_rows = task.counters.probe_rows.saturating_add(*probe_rows);
				task.counters.matched_rows =
					task.counters.matched_rows.saturating_add(*output_rows);
			}
		});
		if local_join_phase_enabled() {
			if let Ok((probe_rows, output_rows)) = &result {
				local_join_phase_set_counts(0, *probe_rows, *output_rows);
			}
		}
		let (probe_rows, output_rows) = result?;
		record_join_runtime_observation(
			&self.context.task_telemetry,
			&self.context.join_runtime_observation_mode,
			JoinRuntimeObservation {
				replay_id,
				strategy,
				build_rows: RowCount::new(build_batch.num_rows() as u64),
				probe_rows: RowCount::new(probe_rows),
				output_rows: RowCount::new(output_rows),
			},
		);
		record_join_spill_observation(
			&self.context.task_telemetry,
			replay_id,
			crate::distributed::task::JoinSpillPath::InMemory,
			0,
		);
		Ok(Some(()))
	}

	#[allow(clippy::too_many_arguments)]
	async fn try_visit_partitioned_streamed_inner_scan_build_scan_probe_chunks<F>(
		&self,
		build_plan: &ExecutionPlan,
		probe_plan: &ExecutionPlan,
		probe_scan_input: StreamingScanProbeInput<'_>,
		build_keys: &[crate::ir::ColumnOrdinal],
		probe_keys: &[crate::ir::ColumnOrdinal],
		match_filter: Option<&BoundExprProgram>,
		null_safe_keys: bool,
		output_schema: &arrow::datatypes::SchemaRef,
		probe_prefilter_predicate: Option<&BoundExprProgram>,
		filter_predicate: Option<&BoundExprProgram>,
		probe_columns_first: bool,
		morsel: Option<(MorselId, u32)>,
		replay_id: JoinReplayId,
		estimated_build_bytes: u64,
		profile: Option<&GlobalFoldJoinFilterProfile>,
		visit_batch: &mut F,
	) -> Result<Option<(u64, u64, u64)>, ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		if match_filter.is_some() {
			return Ok(None);
		}
		let Some(build_scan_input) = decompose_streaming_scan_probe_input(build_plan) else {
			return Ok(None);
		};
		if estimated_build_bytes <= self.context.task_budget.memory.get() {
			return Ok(None);
		}
		let partition_timer = profile
			.map(|profile| profile.begin_leaf(GlobalFoldProfileLeaf::BuildSpillWrite))
			.transpose()?;
		let partitioning = self
			.choose_streaming_scan_inner_build_partitioning(
				estimated_build_bytes,
				build_scan_input,
				None,
				build_keys,
				build_keys,
				null_safe_keys,
				morsel,
			)
			.await;
		if let Some(timer) = partition_timer {
			timer.finish()?;
		}
		let (partition_count, group_count, _max_group_spill) = partitioning?;
		if partition_count <= 1 {
			return Ok(None);
		}
		if let Some(profile) = profile {
			profile.record_dispatch(
				"partitioned_streamed_scan_build_scan_probe",
				replay_id,
				probe_columns_first,
				&build_plan.schema(),
				&probe_plan.schema(),
			)?;
		}
		let key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&build_plan.schema(),
			build_keys,
			&probe_plan.schema(),
			probe_keys,
			null_safe_keys,
		)?;
		let use_primitive_resident_index =
			!null_safe_keys && matches!(key_plan, JoinKeyPlan::Int32 | JoinKeyPlan::Int64);
		let materialized_output_schema = if probe_columns_first {
			concat_join_schema(&build_plan.schema(), &probe_plan.schema())
		} else {
			output_schema.clone()
		};

		let group_count = group_count.max(1).min(partition_count);
		let mut total_build_rows = 0u64;
		let mut total_probe_rows = 0u64;
		let mut total_output_rows = 0u64;
		let mut total_spill_bytes = 0u64;
		for group in 0..group_count {
			let group_build_timer = profile
				.map(|profile| profile.begin_leaf(GlobalFoldProfileLeaf::BuildSpillWrite))
				.transpose()?;
			let group_range = partition_group_bounds(partition_count, group_count, group)?;
			let mut partitioned_builds: Vec<Option<SpilledInnerJoinBuild>> =
				(0..partition_count).map(|_| None).collect();
			for partition in group_range.clone() {
				let created = SpilledInnerJoinBuild::create(
					self.context.task_budget,
					self.context.join_spill_file_ops.clone(),
					self.context.join_spill_dir.clone(),
				)
				.await;
				let created = match created {
					Ok(created) => created,
					Err(error) => {
						Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
						return Err(error);
					}
				};
				partitioned_builds[partition] = Some(created);
			}

			let build_started = Instant::now();
			let mut group_spill_bytes = 0u64;
			let build_result = self
				.write_partitioned_streaming_scan_inner_build_batches(
					&mut partitioned_builds,
					&mut group_spill_bytes,
					build_scan_input,
					None,
					build_keys,
					build_keys,
					key_plan,
					null_safe_keys,
					morsel,
					group_range.start,
					group_range.end,
					true,
					None,
					None,
					None,
					None,
					None,
				)
				.await;
			if let Err(error) = build_result {
				Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
				return Err(error);
			}

			let mut group_build_rows = 0u64;
			for partition in group_range.clone() {
				let Some(spilled) = partitioned_builds
					.get_mut(partition)
					.and_then(Option::as_mut)
				else {
					continue;
				};
				if let Err(error) = spilled.finish(self.context.task_budget).await {
					Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
					return Err(error);
				}
				spilled.release_write_scratch();
				group_build_rows = group_build_rows.saturating_add(spilled.input_row_count());
				if spilled.input_row_count() > 0 {
					if use_primitive_resident_index {
						spilled.use_compact_resident_index();
					}
					if let Err(error) = spilled
						.rebuild_index_from_pages(
							key_plan,
							build_keys,
							null_safe_keys,
							self.context.task_budget,
						)
						.await
					{
						Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
						return Err(error);
					}
				}
			}
			total_build_rows = total_build_rows.saturating_add(group_build_rows);
			total_spill_bytes = total_spill_bytes.saturating_add(group_spill_bytes);
			record_task_telemetry(&self.context.task_telemetry, |task| {
				task.phases.build_micros = task
					.phases
					.build_micros
					.saturating_add(duration_micros(build_started));
				task.phases.spill_micros = task
					.phases
					.spill_micros
					.saturating_add(duration_micros(build_started));
				task.counters.build_rows =
					task.counters.build_rows.saturating_add(group_build_rows);
				task.counters.spill_bytes =
					task.counters.spill_bytes.saturating_add(group_spill_bytes);
			});
			if let Some(timer) = group_build_timer {
				timer.finish()?;
			}

			let probe_started = Instant::now();
			let result = self
				.visit_partitioned_streaming_scan_probe_group_chunks(
					&mut partitioned_builds,
					probe_scan_input,
					probe_keys,
					partition_count,
					group_range.start,
					group_range.end,
					key_plan,
					null_safe_keys,
					&materialized_output_schema,
					output_schema,
					probe_prefilter_predicate,
					filter_predicate,
					probe_columns_first,
					build_plan.schema().fields().len(),
					morsel,
					profile,
					visit_batch,
				)
				.await;
			let (probe_rows, output_rows) = match result {
				Ok(result) => result,
				Err(error) => {
					Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
					return Err(error);
				}
			};
			total_probe_rows = total_probe_rows.saturating_add(probe_rows);
			total_output_rows = total_output_rows.saturating_add(output_rows);
			record_task_telemetry(&self.context.task_telemetry, |task| {
				task.phases.probe_micros = task
					.phases
					.probe_micros
					.saturating_add(duration_micros(probe_started));
				task.counters.probe_rows = task.counters.probe_rows.saturating_add(probe_rows);
				task.counters.matched_rows = task.counters.matched_rows.saturating_add(output_rows);
			});
			let cleanup_timer = profile
				.map(|profile| profile.begin_leaf(GlobalFoldProfileLeaf::BuildSpillWrite))
				.transpose()?;
			Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
			if let Some(timer) = cleanup_timer {
				timer.finish()?;
			}
		}
		record_join_spill_observation(
			&self.context.task_telemetry,
			replay_id,
			crate::distributed::task::JoinSpillPath::PartitionedStreamingScanBuild,
			total_spill_bytes,
		);
		Ok(Some((
			total_build_rows,
			total_probe_rows,
			total_output_rows,
		)))
	}

	#[allow(clippy::too_many_arguments)]
	async fn try_visit_partitioned_streamed_inner_materialized_build_probe_chunks<F>(
		&self,
		build_schema: arrow::datatypes::SchemaRef,
		probe_schema: arrow::datatypes::SchemaRef,
		build_batches: &[RecordBatch],
		probe_batches: &[RecordBatch],
		build_keys: &[crate::ir::ColumnOrdinal],
		probe_keys: &[crate::ir::ColumnOrdinal],
		match_filter: Option<&BoundExprProgram>,
		null_safe_keys: bool,
		output_schema: &arrow::datatypes::SchemaRef,
		filter_predicate: Option<&BoundExprProgram>,
		_morsel: Option<(MorselId, u32)>,
		visit_batch: &mut F,
	) -> Result<Option<(u64, u64, u64)>, ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		if match_filter.is_some() {
			return Ok(None);
		}
		let estimated_build_bytes =
			estimated_build_hash_bytes_for_batches(build_batches, build_keys)?;
		if estimated_build_bytes <= self.context.task_budget.memory.get() {
			return Ok(None);
		}
		let (partition_count, group_count, _max_group_spill) =
			choose_materialized_inner_build_partitioning(
				estimated_build_bytes,
				self.context.task_budget.memory.get(),
				self.context.task_budget.spill_bytes.get(),
				build_batches,
				build_keys,
				null_safe_keys,
			)?;
		if partition_count <= 1 {
			return Ok(None);
		}
		let key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&build_schema,
			build_keys,
			&probe_schema,
			probe_keys,
			null_safe_keys,
		)?;
		let build_partition_buckets = build_batches
			.iter()
			.map(|batch| {
				precompute_join_partition_buckets(
					batch,
					build_keys,
					partition_count,
					null_safe_keys,
					false,
				)
			})
			.collect::<Result<Vec<_>, _>>()?;
		let probe_partition_buckets = probe_batches
			.iter()
			.map(|batch| {
				precompute_join_partition_buckets(
					batch,
					probe_keys,
					partition_count,
					null_safe_keys,
					false,
				)
			})
			.collect::<Result<Vec<_>, _>>()?;

		let group_count = group_count.max(1).min(partition_count);
		let group_spill_budget =
			streamed_inner_build_group_spill_budget(self.context.task_budget.spill_bytes.get());
		let mut total_build_rows = 0u64;
		let mut total_probe_rows = 0u64;
		let mut total_output_rows = 0u64;
		for group in 0..group_count {
			let group_range = partition_group_bounds(partition_count, group_count, group)?;
			let mut partitioned_builds: Vec<Option<SpilledInnerJoinBuild>> =
				(0..partition_count).map(|_| None).collect();
			for partition in group_range.clone() {
				let created = SpilledInnerJoinBuild::create(
					self.context.task_budget,
					self.context.join_spill_file_ops.clone(),
					self.context.join_spill_dir.clone(),
				)
				.await;
				let created = match created {
					Ok(created) => created,
					Err(error) => {
						Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
						return Err(error);
					}
				};
				partitioned_builds[partition] = Some(created);
			}

			let build_started = Instant::now();
			let mut group_spill_bytes = 0u64;
			for (batch, buckets) in build_batches.iter().zip(&build_partition_buckets) {
				for partition in group_range.clone() {
					let partition_u32 =
						u32::try_from(partition).map_err(|_| ExecutionError::Kernel {
							message: "streamed inner build partition exceeds u32".to_string(),
						})?;
					let partitioned =
						select_join_partition_batch_with_buckets(batch, buckets, partition_u32)?;
					if partitioned.num_rows() == 0 {
						continue;
					}
					let Some(spilled) = partitioned_builds
						.get_mut(partition)
						.and_then(Option::as_mut)
					else {
						continue;
					};
					let before = spilled.spill_bytes();
					spilled
						.write_batch_chunks_unindexed(
							&partitioned,
							key_plan,
							build_keys,
							null_safe_keys,
							self.context.task_budget,
						)
						.await?;
					group_spill_bytes = group_spill_bytes
						.saturating_add(spilled.spill_bytes().saturating_sub(before));
					if group_spill_bytes > group_spill_budget {
						Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
						return Err(budget_exhausted(
							BudgetDimension::SpillBytes,
							"streamed inner join build partition group spill writer",
							group_spill_bytes,
							group_spill_budget,
						));
					}
				}
			}
			let mut group_build_rows = 0u64;
			for partition in group_range.clone() {
				let Some(spilled) = partitioned_builds
					.get_mut(partition)
					.and_then(Option::as_mut)
				else {
					continue;
				};
				if let Err(error) = spilled.finish(self.context.task_budget).await {
					Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
					return Err(error);
				}
				spilled.release_write_scratch();
				group_build_rows = group_build_rows.saturating_add(spilled.input_row_count());
				if spilled.input_row_count() > 0 {
					if let Err(error) = spilled
						.rebuild_index_from_pages(
							key_plan,
							build_keys,
							null_safe_keys,
							self.context.task_budget,
						)
						.await
					{
						Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
						return Err(error);
					}
				}
			}
			total_build_rows = total_build_rows.saturating_add(group_build_rows);
			record_task_telemetry(&self.context.task_telemetry, |task| {
				task.phases.build_micros = task
					.phases
					.build_micros
					.saturating_add(duration_micros(build_started));
				task.phases.spill_micros = task
					.phases
					.spill_micros
					.saturating_add(duration_micros(build_started));
				task.counters.build_rows =
					task.counters.build_rows.saturating_add(group_build_rows);
				task.counters.spill_bytes =
					task.counters.spill_bytes.saturating_add(group_spill_bytes);
			});

			let probe_started = Instant::now();
			let mut group_probe_rows = 0u64;
			let mut group_output_rows = 0u64;
			for (batch, buckets) in probe_batches.iter().zip(&probe_partition_buckets) {
				for partition in group_range.clone() {
					let partition_u32 =
						u32::try_from(partition).map_err(|_| ExecutionError::Kernel {
							message: "streamed inner probe partition exceeds u32".to_string(),
						})?;
					let partitioned =
						select_join_partition_batch_with_buckets(batch, buckets, partition_u32)?;
					if partitioned.num_rows() == 0 {
						continue;
					}
					let Some(spilled) = partitioned_builds
						.get_mut(partition)
						.and_then(Option::as_mut)
					else {
						continue;
					};
					if spilled.input_row_count() == 0 {
						continue;
					}
					let (probe_rows, output_rows) = self
						.visit_streaming_spilled_inner_probe_batch(
							spilled,
							&partitioned,
							probe_keys,
							key_plan,
							null_safe_keys,
							output_schema,
							output_schema,
							filter_predicate,
							false,
							build_schema.fields().len(),
							None,
							None,
							None,
							visit_batch,
						)
						.await?;
					group_probe_rows = group_probe_rows.saturating_add(probe_rows);
					group_output_rows = group_output_rows.saturating_add(output_rows);
				}
			}
			total_probe_rows = total_probe_rows.saturating_add(group_probe_rows);
			total_output_rows = total_output_rows.saturating_add(group_output_rows);
			record_task_telemetry(&self.context.task_telemetry, |task| {
				task.phases.probe_micros = task
					.phases
					.probe_micros
					.saturating_add(duration_micros(probe_started));
				task.counters.probe_rows =
					task.counters.probe_rows.saturating_add(group_probe_rows);
				task.counters.matched_rows =
					task.counters.matched_rows.saturating_add(group_output_rows);
			});
			Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
		}

		Ok(Some((
			total_build_rows,
			total_probe_rows,
			total_output_rows,
		)))
	}

	#[allow(clippy::too_many_arguments)]
	async fn try_visit_partitioned_streamed_inner_materialized_build_scan_probe_chunks<F>(
		&self,
		build_plan: &ExecutionPlan,
		probe_plan: &ExecutionPlan,
		scan_input: StreamingScanProbeInput<'_>,
		build_batches: &[RecordBatch],
		build_keys: &[crate::ir::ColumnOrdinal],
		probe_keys: &[crate::ir::ColumnOrdinal],
		match_filter: Option<&BoundExprProgram>,
		null_safe_keys: bool,
		output_schema: &arrow::datatypes::SchemaRef,
		probe_prefilter_predicate: Option<&BoundExprProgram>,
		filter_predicate: Option<&BoundExprProgram>,
		probe_columns_first: bool,
		morsel: Option<(MorselId, u32)>,
		visit_batch: &mut F,
	) -> Result<Option<(u64, u64, u64)>, ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		if match_filter.is_some() {
			return Ok(None);
		}
		let estimated_build_bytes =
			estimated_build_hash_bytes_for_batches(build_batches, build_keys)?;
		if estimated_build_bytes <= self.context.task_budget.memory.get() {
			return Ok(None);
		}
		let (partition_count, group_count, _max_group_spill) =
			choose_materialized_inner_build_partitioning(
				estimated_build_bytes,
				self.context.task_budget.memory.get(),
				self.context.task_budget.spill_bytes.get(),
				build_batches,
				build_keys,
				null_safe_keys,
			)?;
		if partition_count <= 1 {
			return Ok(None);
		}
		let key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&build_plan.schema(),
			build_keys,
			&probe_plan.schema(),
			probe_keys,
			null_safe_keys,
		)?;
		let materialized_output_schema = if probe_columns_first {
			concat_join_schema(&build_plan.schema(), &probe_plan.schema())
		} else {
			output_schema.clone()
		};
		let build_partition_buckets = build_batches
			.iter()
			.map(|batch| {
				precompute_join_partition_buckets(
					batch,
					build_keys,
					partition_count,
					null_safe_keys,
					false,
				)
			})
			.collect::<Result<Vec<_>, _>>()?;

		let group_count = group_count.max(1).min(partition_count);
		let group_spill_budget =
			streamed_inner_build_group_spill_budget(self.context.task_budget.spill_bytes.get());
		let mut total_build_rows = 0u64;
		let mut total_probe_rows = 0u64;
		let mut total_output_rows = 0u64;
		for group in 0..group_count {
			let group_range = partition_group_bounds(partition_count, group_count, group)?;
			let mut partitioned_builds: Vec<Option<SpilledInnerJoinBuild>> =
				(0..partition_count).map(|_| None).collect();
			for partition in group_range.clone() {
				let created = SpilledInnerJoinBuild::create(
					self.context.task_budget,
					self.context.join_spill_file_ops.clone(),
					self.context.join_spill_dir.clone(),
				)
				.await;
				let created = match created {
					Ok(created) => created,
					Err(error) => {
						Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
						return Err(error);
					}
				};
				partitioned_builds[partition] = Some(created);
			}

			let build_started = Instant::now();
			let mut group_spill_bytes = 0u64;
			for (batch, buckets) in build_batches.iter().zip(&build_partition_buckets) {
				for partition in group_range.clone() {
					let partition_u32 =
						u32::try_from(partition).map_err(|_| ExecutionError::Kernel {
							message: "streamed inner build partition exceeds u32".to_string(),
						})?;
					let partitioned =
						select_join_partition_batch_with_buckets(batch, buckets, partition_u32)?;
					if partitioned.num_rows() == 0 {
						continue;
					}
					let Some(spilled) = partitioned_builds
						.get_mut(partition)
						.and_then(Option::as_mut)
					else {
						continue;
					};
					let before = spilled.spill_bytes();
					spilled
						.write_batch_chunks_unindexed(
							&partitioned,
							key_plan,
							build_keys,
							null_safe_keys,
							self.context.task_budget,
						)
						.await?;
					group_spill_bytes = group_spill_bytes
						.saturating_add(spilled.spill_bytes().saturating_sub(before));
					if group_spill_bytes > group_spill_budget {
						Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
						return Err(budget_exhausted(
							BudgetDimension::SpillBytes,
							"streamed inner join build partition group spill writer",
							group_spill_bytes,
							group_spill_budget,
						));
					}
				}
			}
			let mut group_build_rows = 0u64;
			for partition in group_range.clone() {
				let Some(spilled) = partitioned_builds
					.get_mut(partition)
					.and_then(Option::as_mut)
				else {
					continue;
				};
				if let Err(error) = spilled.finish(self.context.task_budget).await {
					Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
					return Err(error);
				}
				spilled.release_write_scratch();
				group_build_rows = group_build_rows.saturating_add(spilled.input_row_count());
				if spilled.input_row_count() > 0 {
					if let Err(error) = spilled
						.rebuild_index_from_pages(
							key_plan,
							build_keys,
							null_safe_keys,
							self.context.task_budget,
						)
						.await
					{
						Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
						return Err(error);
					}
				}
			}
			total_build_rows = total_build_rows.saturating_add(group_build_rows);
			record_task_telemetry(&self.context.task_telemetry, |task| {
				task.phases.build_micros = task
					.phases
					.build_micros
					.saturating_add(duration_micros(build_started));
				task.phases.spill_micros = task
					.phases
					.spill_micros
					.saturating_add(duration_micros(build_started));
				task.counters.build_rows =
					task.counters.build_rows.saturating_add(group_build_rows);
				task.counters.spill_bytes =
					task.counters.spill_bytes.saturating_add(group_spill_bytes);
			});

			let probe_started = Instant::now();
			let result = self
				.visit_partitioned_streaming_scan_probe_group_chunks(
					&mut partitioned_builds,
					scan_input,
					probe_keys,
					partition_count,
					group_range.start,
					group_range.end,
					key_plan,
					null_safe_keys,
					&materialized_output_schema,
					output_schema,
					probe_prefilter_predicate,
					filter_predicate,
					probe_columns_first,
					build_plan.schema().fields().len(),
					morsel,
					None,
					visit_batch,
				)
				.await;
			let (probe_rows, output_rows) = match result {
				Ok(result) => result,
				Err(error) => {
					Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
					return Err(error);
				}
			};
			total_probe_rows = total_probe_rows.saturating_add(probe_rows);
			total_output_rows = total_output_rows.saturating_add(output_rows);
			record_task_telemetry(&self.context.task_telemetry, |task| {
				task.phases.probe_micros = task
					.phases
					.probe_micros
					.saturating_add(duration_micros(probe_started));
				task.counters.probe_rows = task.counters.probe_rows.saturating_add(probe_rows);
				task.counters.matched_rows = task.counters.matched_rows.saturating_add(output_rows);
			});
			Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
		}
		Ok(Some((
			total_build_rows,
			total_probe_rows,
			total_output_rows,
		)))
	}

	#[allow(clippy::too_many_arguments)]
	async fn try_create_streamed_spilled_inner_scan_build(
		&self,
		build_plan: &ExecutionPlan,
		probe_plan: &ExecutionPlan,
		build_keys: &[crate::ir::ColumnOrdinal],
		probe_keys: &[crate::ir::ColumnOrdinal],
		match_filter: Option<&BoundExprProgram>,
		null_safe_keys: bool,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<(SpilledInnerJoinBuild, JoinKeyPlan)>, ExecutionError> {
		if match_filter.is_some() {
			return Ok(None);
		}
		let Some(scan_input) = decompose_streaming_scan_probe_input(build_plan) else {
			return Ok(None);
		};
		let key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&build_plan.schema(),
			build_keys,
			&probe_plan.schema(),
			probe_keys,
			null_safe_keys,
		)?;
		let mut spilled = SpilledInnerJoinBuild::create(
			self.context.task_budget,
			self.context.join_spill_file_ops.clone(),
			self.context.join_spill_dir.clone(),
		)
		.await?;
		self.write_streaming_scan_inner_build_batches(
			&mut spilled,
			scan_input,
			None,
			build_keys,
			key_plan,
			null_safe_keys,
			false,
			None,
			morsel,
		)
		.await?;
		spilled.finish(self.context.task_budget).await?;
		Ok(Some((spilled, key_plan)))
	}

	#[allow(clippy::too_many_arguments)]
	async fn write_streaming_scan_inner_build_batches(
		&self,
		spilled: &mut SpilledInnerJoinBuild,
		scan_input: StreamingScanProbeInput<'_>,
		retained_columns: Option<&[crate::ir::ColumnOrdinal]>,
		build_keys: &[crate::ir::ColumnOrdinal],
		key_plan: JoinKeyPlan,
		null_safe_keys: bool,
		retain_unkeyed_rows: bool,
		join_partition: Option<StreamingScanJoinPartition<'_>>,
		morsel: Option<(MorselId, u32)>,
	) -> Result<(), ExecutionError> {
		if let Some(exchange) = scan_input.exchange {
			let Some(req) = self.bound_scan_request(scan_input.scan, morsel) else {
				return Err(ExecutionError::InvalidExpression {
					message: "streaming scan build expected a scan input".to_string(),
				});
			};
			let mut remaining = req.row_limit;
			let span = crate::telemetry::storage_scan_span(&req);
			let mut stream = self
				.storage
				.scan(req)
				.instrument(span)
				.await?
				.into_batches();
			while let Some(batch) = stream.next().await {
				let Some(mut batch) = limit_streaming_scan_batch(batch?, &mut remaining) else {
					break;
				};
				if let Some(predicate) = scan_input.filter_predicate {
					batch = self.filter_batch_with_bound_predicate(
						predicate,
						batch,
						"filter predicate",
					)?;
					if batch.num_rows() == 0 {
						continue;
					}
				}
				for (_, partitioned) in
					self.repartition_exchange_batch(&batch, exchange.partition_count, exchange.key)?
				{
					let partitioned = if let Some(join_partition) = join_partition {
						select_join_partition_batch(
							&partitioned,
							join_partition.keys,
							join_partition.partition_count,
							join_partition.partition,
							join_partition.null_safe_keys,
							false,
						)?
					} else {
						partitioned
					};
					if partitioned.num_rows() == 0 {
						continue;
					}
					self.write_streaming_scan_inner_build_batch(
						spilled,
						&partitioned,
						retained_columns,
						key_plan,
						build_keys,
						null_safe_keys,
						retain_unkeyed_rows,
					)
					.await?;
				}
			}
		} else {
			let Some(req) = self.bound_scan_request(scan_input.scan, morsel) else {
				return Err(ExecutionError::InvalidExpression {
					message: "streaming scan build expected a scan input".to_string(),
				});
			};
			let mut remaining = req.row_limit;
			let span = crate::telemetry::storage_scan_span(&req);
			let mut stream = self
				.storage
				.scan(req)
				.instrument(span)
				.await?
				.into_batches();
			while let Some(batch) = stream.next().await {
				let Some(mut batch) = limit_streaming_scan_batch(batch?, &mut remaining) else {
					break;
				};
				if let Some(predicate) = scan_input.filter_predicate {
					batch = self.filter_batch_with_bound_predicate(
						predicate,
						batch,
						"filter predicate",
					)?;
					if batch.num_rows() == 0 {
						continue;
					}
				}
				let batch = if let Some(join_partition) = join_partition {
					select_join_partition_batch(
						&batch,
						join_partition.keys,
						join_partition.partition_count,
						join_partition.partition,
						join_partition.null_safe_keys,
						false,
					)?
				} else {
					batch
				};
				if batch.num_rows() == 0 {
					continue;
				}
				self.write_streaming_scan_inner_build_batch(
					spilled,
					&batch,
					retained_columns,
					key_plan,
					build_keys,
					null_safe_keys,
					retain_unkeyed_rows,
				)
				.await?;
			}
		}
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	async fn write_partitioned_streaming_scan_inner_build_batches(
		&self,
		partitioned_builds: &mut [Option<SpilledInnerJoinBuild>],
		total_spill_bytes: &mut u64,
		scan_input: StreamingScanProbeInput<'_>,
		retained_columns: Option<&[crate::ir::ColumnOrdinal]>,
		partition_build_keys: &[crate::ir::ColumnOrdinal],
		write_build_keys: &[crate::ir::ColumnOrdinal],
		key_plan: JoinKeyPlan,
		null_safe_keys: bool,
		morsel: Option<(MorselId, u32)>,
		partition_start: usize,
		partition_end: usize,
		enforce_group_spill_budget: bool,
		combined_resident_baseline_bytes: Option<u64>,
		mut residency_profile: Option<&mut GroupedReservoirResidencyProfile>,
		membership_profile: Option<(&MembershipChainProfile, MembershipChainPhase)>,
		grouped_chain_observer: Option<&GroupedChainSpillObserver<'_>>,
		local_runtime_filter: Option<RuntimeFilterDescriptor>,
	) -> Result<(), ExecutionError> {
		let partition_count =
			u32::try_from(partitioned_builds.len()).map_err(|_| ExecutionError::Kernel {
				message: "partitioned streamed inner build count exceeds u32".to_string(),
			})?;
		if let Some(exchange) = scan_input.exchange {
			let Some(mut req) = self.bound_scan_request(scan_input.scan, morsel) else {
				return Err(ExecutionError::InvalidExpression {
					message: "streaming scan build expected a scan input".to_string(),
				});
			};
			if req.runtime_filter.is_none() {
				req.runtime_filter = local_runtime_filter.clone();
			}
			let mut remaining = req.row_limit;
			let span = crate::telemetry::storage_scan_span(&req);
			let mut stream = self
				.storage
				.scan(req)
				.instrument(span)
				.await?
				.into_batches();
			loop {
				let grouped_scan_timer = grouped_chain_observer
					.map(|observer| observer.begin(GroupedChainSubLeaf::WriteScanDecode))
					.transpose()?;
				let scan_timer = membership_profile
					.map(|(profile, phase)| {
						profile.begin_leaf(phase, MembershipChainOperation::StorageScanDecode)
					})
					.transpose()?;
				let next_batch = stream.next().await;
				if let Some(timer) = grouped_scan_timer {
					timer.finish_with(
						next_batch
							.as_ref()
							.and_then(|batch| batch.as_ref().ok())
							.map_or(0, |batch| batch.num_rows() as u64),
						0,
						0,
					)?;
				}
				if let Some(timer) = scan_timer {
					timer.finish_with(
						next_batch
							.as_ref()
							.and_then(|batch| batch.as_ref().ok())
							.map_or(0, |batch| batch.num_rows() as u64),
						0,
					)?;
				}
				let Some(batch) = next_batch else {
					break;
				};
				let Some(mut batch) = limit_streaming_scan_batch(batch?, &mut remaining) else {
					break;
				};
				if let Some(predicate) = scan_input.filter_predicate {
					batch = self.filter_batch_with_bound_predicate(
						predicate,
						batch,
						"filter predicate",
					)?;
					if batch.num_rows() == 0 {
						continue;
					}
				}
				if let Some(profile) = residency_profile.as_deref_mut() {
					profile.before_reservoir_batch(partitioned_builds, &batch)?;
				}
				let write_timer = membership_profile
					.map(|(profile, phase)| {
						profile.begin_leaf(phase, MembershipChainOperation::PartitionSpillWrite)
					})
					.transpose()?;
				let exchange_timer = grouped_chain_observer
					.map(|observer| observer.begin(GroupedChainSubLeaf::WritePartitionRouting))
					.transpose()?;
				let exchange_batches = self.repartition_exchange_batch(
					&batch,
					exchange.partition_count,
					exchange.key,
				)?;
				if let Some(timer) = exchange_timer {
					timer.finish_with(batch.num_rows() as u64, 0, 0)?;
				}
				for (_, partitioned) in exchange_batches {
					if let Some(profile) = residency_profile.as_deref_mut() {
						profile.before_reservoir_batch(partitioned_builds, &partitioned)?;
					}
					self.write_partitioned_streaming_scan_inner_build_batch(
						partitioned_builds,
						total_spill_bytes,
						&partitioned,
						retained_columns,
						partition_build_keys,
						write_build_keys,
						partition_count,
						key_plan,
						null_safe_keys,
						partition_start,
						partition_end,
						enforce_group_spill_budget,
						combined_resident_baseline_bytes,
						grouped_chain_observer,
					)
					.await?;
					if let Some(profile) = residency_profile.as_deref_mut() {
						profile
							.reservoir_batch_complete(partitioned_builds, partitioned.num_rows())?;
					}
				}
				if let Some(timer) = write_timer {
					timer
						.finish_with(batch.num_rows() as u64, record_batch_memory_bytes(&batch))?;
				}
			}
		} else {
			let Some(mut req) = self.bound_scan_request(scan_input.scan, morsel) else {
				return Err(ExecutionError::InvalidExpression {
					message: "streaming scan build expected a scan input".to_string(),
				});
			};
			if req.runtime_filter.is_none() {
				req.runtime_filter = local_runtime_filter;
			}
			let mut remaining = req.row_limit;
			let span = crate::telemetry::storage_scan_span(&req);
			let mut stream = self
				.storage
				.scan(req)
				.instrument(span)
				.await?
				.into_batches();
			loop {
				let grouped_scan_timer = grouped_chain_observer
					.map(|observer| observer.begin(GroupedChainSubLeaf::WriteScanDecode))
					.transpose()?;
				let scan_timer = membership_profile
					.map(|(profile, phase)| {
						profile.begin_leaf(phase, MembershipChainOperation::StorageScanDecode)
					})
					.transpose()?;
				let next_batch = stream.next().await;
				if let Some(timer) = grouped_scan_timer {
					timer.finish_with(
						next_batch
							.as_ref()
							.and_then(|batch| batch.as_ref().ok())
							.map_or(0, |batch| batch.num_rows() as u64),
						0,
						0,
					)?;
				}
				if let Some(timer) = scan_timer {
					timer.finish_with(
						next_batch
							.as_ref()
							.and_then(|batch| batch.as_ref().ok())
							.map_or(0, |batch| batch.num_rows() as u64),
						0,
					)?;
				}
				let Some(batch) = next_batch else {
					break;
				};
				let Some(mut batch) = limit_streaming_scan_batch(batch?, &mut remaining) else {
					break;
				};
				if let Some(predicate) = scan_input.filter_predicate {
					batch = self.filter_batch_with_bound_predicate(
						predicate,
						batch,
						"filter predicate",
					)?;
					if batch.num_rows() == 0 {
						continue;
					}
				}
				if let Some(profile) = residency_profile.as_deref_mut() {
					profile.before_reservoir_batch(partitioned_builds, &batch)?;
				}
				let write_timer = membership_profile
					.map(|(profile, phase)| {
						profile.begin_leaf(phase, MembershipChainOperation::PartitionSpillWrite)
					})
					.transpose()?;
				self.write_partitioned_streaming_scan_inner_build_batch(
					partitioned_builds,
					total_spill_bytes,
					&batch,
					retained_columns,
					partition_build_keys,
					write_build_keys,
					partition_count,
					key_plan,
					null_safe_keys,
					partition_start,
					partition_end,
					enforce_group_spill_budget,
					combined_resident_baseline_bytes,
					grouped_chain_observer,
				)
				.await?;
				if let Some(timer) = write_timer {
					timer
						.finish_with(batch.num_rows() as u64, record_batch_memory_bytes(&batch))?;
				}
				if let Some(profile) = residency_profile.as_deref_mut() {
					profile.reservoir_batch_complete(partitioned_builds, batch.num_rows())?;
				}
			}
		}
		// The unindexed reservoir writer deliberately delays partial pages until
		// the scan is complete. Flush every partition at this existing all-builds
		// barrier so later index rebuilds see only complete, self-describing IPC
		// pages.
		for spilled in partitioned_builds.iter_mut().filter_map(Option::as_mut) {
			let before = spilled.spill_bytes();
			spilled
				.flush_unindexed_page_buffer(
					key_plan,
					write_build_keys,
					null_safe_keys,
					self.context.task_budget,
					grouped_chain_observer,
				)
				.await?;
			*total_spill_bytes =
				total_spill_bytes.saturating_add(spilled.spill_bytes().saturating_sub(before));
		}
		let group_spill_budget =
			streamed_inner_build_group_spill_budget(self.context.task_budget.spill_bytes.get());
		if enforce_group_spill_budget && *total_spill_bytes > group_spill_budget {
			return Err(budget_exhausted(
				BudgetDimension::SpillBytes,
				"partitioned streamed inner join build spill writer",
				*total_spill_bytes,
				group_spill_budget,
			));
		}
		if let Some(baseline_bytes) = combined_resident_baseline_bytes {
			guard_partition_replay_owner_resident(
				baseline_bytes,
				partition_replay_reservoir_bytes(partitioned_builds, partitioned_builds.len()),
				0,
				0,
				0,
				0,
				0,
				0,
				self.context.task_budget.memory.get(),
			)?;
		}
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	async fn write_partitioned_streaming_scan_inner_build_batch(
		&self,
		partitioned_builds: &mut [Option<SpilledInnerJoinBuild>],
		total_spill_bytes: &mut u64,
		batch: &RecordBatch,
		retained_columns: Option<&[crate::ir::ColumnOrdinal]>,
		partition_build_keys: &[crate::ir::ColumnOrdinal],
		write_build_keys: &[crate::ir::ColumnOrdinal],
		partition_count: u32,
		key_plan: JoinKeyPlan,
		null_safe_keys: bool,
		partition_start: usize,
		partition_end: usize,
		enforce_group_spill_budget: bool,
		combined_resident_baseline_bytes: Option<u64>,
		grouped_chain_observer: Option<&GroupedChainSpillObserver<'_>>,
	) -> Result<(), ExecutionError> {
		let routing_timer = grouped_chain_observer
			.map(|observer| observer.begin(GroupedChainSubLeaf::WritePartitionRouting))
			.transpose()?;
		let buckets = precompute_join_partition_buckets(
			batch,
			partition_build_keys,
			partition_count,
			null_safe_keys,
			false,
		)?;
		let partitioned_batches = partition_streaming_join_batch_active_group_with_buckets(
			batch,
			&buckets,
			partition_count,
			partition_start,
			partition_end,
		)?;
		if let Some(timer) = routing_timer {
			timer.finish_with(batch.num_rows() as u64, 0, 0)?;
		}
		for (partition, partitioned) in partitioned_batches {
			let Some(spilled) = partitioned_builds
				.get_mut(partition)
				.and_then(Option::as_mut)
			else {
				continue;
			};
			let before = spilled.spill_bytes();
			if let Some(retained_columns) = retained_columns {
				spilled
					.write_projected_batch_chunks_unindexed_profiled(
						&partitioned,
						retained_columns,
						key_plan,
						write_build_keys,
						null_safe_keys,
						self.context.task_budget,
						grouped_chain_observer,
					)
					.await?;
			} else {
				spilled
					.write_batch_chunks_unindexed_profiled(
						&partitioned,
						key_plan,
						write_build_keys,
						null_safe_keys,
						self.context.task_budget,
						grouped_chain_observer,
					)
					.await?;
			}
			*total_spill_bytes =
				total_spill_bytes.saturating_add(spilled.spill_bytes().saturating_sub(before));
			let group_spill_budget =
				streamed_inner_build_group_spill_budget(self.context.task_budget.spill_bytes.get());
			if enforce_group_spill_budget && *total_spill_bytes > group_spill_budget {
				return Err(budget_exhausted(
					BudgetDimension::SpillBytes,
					"partitioned streamed inner join build spill writer",
					*total_spill_bytes,
					group_spill_budget,
				));
			}
			if let Some(baseline_bytes) = combined_resident_baseline_bytes {
				guard_partition_replay_owner_resident(
					baseline_bytes,
					partition_replay_reservoir_bytes(partitioned_builds, partitioned_builds.len()),
					0,
					0,
					0,
					0,
					0,
					0,
					self.context.task_budget.memory.get(),
				)?;
			}
		}
		Ok(())
	}

	async fn load_or_estimate_streaming_scan_inner_build(
		&self,
		scan_input: StreamingScanProbeInput<'_>,
		retained_columns: Option<&[crate::ir::ColumnOrdinal]>,
		build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		morsel: Option<(MorselId, u32)>,
		memory_cap: u64,
	) -> Result<(u64, Option<Vec<RecordBatch>>), ExecutionError> {
		let Some(req) = self.bound_scan_request(scan_input.scan, morsel) else {
			return Err(ExecutionError::InvalidExpression {
				message: "streaming scan build expected a scan input".to_string(),
			});
		};
		let mut remaining = req.row_limit;
		let span = crate::telemetry::storage_scan_span(&req);
		let mut stream = self
			.storage
			.scan(req)
			.instrument(span)
			.await?
			.into_batches();
		let mut estimated = 0u64;
		let mut resident = Some(Vec::new());
		while let Some(batch) = stream.next().await {
			let Some(mut batch) = limit_streaming_scan_batch(batch?, &mut remaining) else {
				break;
			};
			if let Some(predicate) = scan_input.filter_predicate {
				batch =
					self.filter_batch_with_bound_predicate(predicate, batch, "filter predicate")?;
				if batch.num_rows() == 0 {
					continue;
				}
			}
			let batches = if let Some(exchange) = scan_input.exchange {
				self.repartition_exchange_batch(&batch, exchange.partition_count, exchange.key)?
					.into_iter()
					.map(|(_, partitioned)| partitioned)
					.collect::<Vec<_>>()
			} else {
				vec![batch]
			};
			for batch in batches {
				estimated = estimated.saturating_add(estimate_streaming_scan_build_batch_bytes(
					&batch,
					retained_columns,
					build_keys,
					null_safe_keys,
				)?);
				if estimated > memory_cap {
					resident = None;
				} else if let Some(resident) = resident.as_mut() {
					resident.push(batch);
				}
			}
		}
		Ok((estimated, resident))
	}

	async fn estimate_streaming_scan_inner_build_bytes(
		&self,
		scan_input: StreamingScanProbeInput<'_>,
		retained_columns: Option<&[crate::ir::ColumnOrdinal]>,
		build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		morsel: Option<(MorselId, u32)>,
	) -> Result<u64, ExecutionError> {
		let Some(req) = self.bound_scan_request(scan_input.scan, morsel) else {
			return Err(ExecutionError::InvalidExpression {
				message: "streaming scan build expected a scan input".to_string(),
			});
		};
		let mut remaining = req.row_limit;
		let span = crate::telemetry::storage_scan_span(&req);
		let mut stream = self
			.storage
			.scan(req)
			.instrument(span)
			.await?
			.into_batches();
		let mut estimated = 0u64;
		while let Some(batch) = stream.next().await {
			let Some(mut batch) = limit_streaming_scan_batch(batch?, &mut remaining) else {
				break;
			};
			if let Some(predicate) = scan_input.filter_predicate {
				batch =
					self.filter_batch_with_bound_predicate(predicate, batch, "filter predicate")?;
				if batch.num_rows() == 0 {
					continue;
				}
			}
			if let Some(exchange) = scan_input.exchange {
				for (_, partitioned) in
					self.repartition_exchange_batch(&batch, exchange.partition_count, exchange.key)?
				{
					estimated =
						estimated.saturating_add(estimate_streaming_scan_build_batch_bytes(
							&partitioned,
							retained_columns,
							build_keys,
							null_safe_keys,
						)?);
				}
			} else {
				estimated = estimated.saturating_add(estimate_streaming_scan_build_batch_bytes(
					&batch,
					retained_columns,
					build_keys,
					null_safe_keys,
				)?);
			}
		}
		Ok(estimated)
	}

	async fn estimate_streaming_scan_inner_build_partition_bytes(
		&self,
		scan_input: StreamingScanProbeInput<'_>,
		retained_columns: Option<&[crate::ir::ColumnOrdinal]>,
		partition_build_keys: &[crate::ir::ColumnOrdinal],
		write_build_keys: &[crate::ir::ColumnOrdinal],
		partition_count: u32,
		null_safe_keys: bool,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Vec<u64>, ExecutionError> {
		let partition_count_usize =
			usize::try_from(partition_count).map_err(|_| ExecutionError::Kernel {
				message: "streamed inner build partition count exceeds usize".to_string(),
			})?;
		let Some(req) = self.bound_scan_request(scan_input.scan, morsel) else {
			return Err(ExecutionError::InvalidExpression {
				message: "streaming scan build expected a scan input".to_string(),
			});
		};
		let mut remaining = req.row_limit;
		let span = crate::telemetry::storage_scan_span(&req);
		let mut stream = self
			.storage
			.scan(req)
			.instrument(span)
			.await?
			.into_batches();
		let mut partition_bytes = vec![0u64; partition_count_usize];
		while let Some(batch) = stream.next().await {
			let Some(mut batch) = limit_streaming_scan_batch(batch?, &mut remaining) else {
				break;
			};
			if let Some(predicate) = scan_input.filter_predicate {
				batch =
					self.filter_batch_with_bound_predicate(predicate, batch, "filter predicate")?;
				if batch.num_rows() == 0 {
					continue;
				}
			}
			if let Some(exchange) = scan_input.exchange {
				for (_, partitioned) in
					self.repartition_exchange_batch(&batch, exchange.partition_count, exchange.key)?
				{
					add_streaming_scan_build_partition_byte_estimates(
						&mut partition_bytes,
						&partitioned,
						retained_columns,
						partition_build_keys,
						write_build_keys,
						partition_count,
						null_safe_keys,
					)?;
				}
			} else {
				add_streaming_scan_build_partition_byte_estimates(
					&mut partition_bytes,
					&batch,
					retained_columns,
					partition_build_keys,
					write_build_keys,
					partition_count,
					null_safe_keys,
				)?;
			}
		}
		Ok(partition_bytes)
	}

	#[allow(clippy::too_many_arguments)]
	async fn choose_streaming_scan_inner_build_partitioning(
		&self,
		estimated_build_bytes: u64,
		scan_input: StreamingScanProbeInput<'_>,
		retained_columns: Option<&[crate::ir::ColumnOrdinal]>,
		partition_build_keys: &[crate::ir::ColumnOrdinal],
		write_build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		morsel: Option<(MorselId, u32)>,
	) -> Result<(u32, u32, u64), ExecutionError> {
		let mut partition_count = streamed_inner_build_partition_count(
			estimated_build_bytes,
			self.context.task_budget.memory.get(),
			self.context.task_budget.spill_bytes.get(),
		)?;
		loop {
			let partition_bytes = self
				.estimate_streaming_scan_inner_build_partition_bytes(
					scan_input,
					retained_columns,
					partition_build_keys,
					write_build_keys,
					partition_count,
					null_safe_keys,
					morsel,
				)
				.await?;
			match streamed_inner_build_group_count_for_partition_spill_bytes(
				&partition_bytes,
				self.context.task_budget.spill_bytes.get(),
			) {
				Ok((group_count, max_group_spill)) => {
					return Ok((partition_count, group_count, max_group_spill));
				}
				Err(error) => {
					let Some(next_partition_count) =
						next_streamed_inner_build_partition_count(partition_count)
					else {
						return Err(error);
					};
					if next_partition_count == partition_count {
						return Err(error);
					}
					partition_count = next_partition_count;
				}
			}
		}
	}

	async fn materialize_streaming_scan_output_for_cap(
		&self,
		scan_input: StreamingScanProbeInput<'_>,
		morsel: Option<(MorselId, u32)>,
	) -> Result<(u64, Vec<RecordBatch>), ExecutionError> {
		let Some(req) = self.bound_scan_request(scan_input.scan, morsel) else {
			return Err(ExecutionError::InvalidExpression {
				message: "streaming scan build expected a scan input".to_string(),
			});
		};
		let mut remaining = req.row_limit;
		let span = crate::telemetry::storage_scan_span(&req);
		let mut stream = self
			.storage
			.scan(req)
			.instrument(span)
			.await?
			.into_batches();
		let mut estimated = 0u64;
		let mut batches = Vec::new();
		while let Some(batch) = stream.next().await {
			let Some(mut batch) = limit_streaming_scan_batch(batch?, &mut remaining) else {
				break;
			};
			if let Some(predicate) = scan_input.filter_predicate {
				batch =
					self.filter_batch_with_bound_predicate(predicate, batch, "filter predicate")?;
				if batch.num_rows() == 0 {
					continue;
				}
			}
			estimated = estimated.saturating_add(batch.get_array_memory_size() as u64);
			batches.push(batch);
		}
		Ok((estimated, batches))
	}

	async fn reoriented_inner_stage_scan_build_decision(
		&self,
		stage: &ReorientedInnerJoinStage<'_>,
		morsel: Option<(MorselId, u32)>,
	) -> Result<ReorientedStageScanBuildDecision, ExecutionError> {
		let Some(scan_input) = decompose_streaming_scan_probe_input(stage.build) else {
			return Ok(ReorientedStageScanBuildDecision::Materialized);
		};
		let task_memory_cap = self.context.task_budget.memory.get();
		if task_memory_cap < 64 * 1024 * 1024 {
			return Ok(ReorientedStageScanBuildDecision::Materialized);
		}
		let scan_output_cap = join_output_materialization_memory_cap(task_memory_cap);
		let (estimated, batches) = self
			.materialize_streaming_scan_output_for_cap(scan_input, morsel)
			.await?;
		let estimated_build_hash_bytes =
			estimated_build_hash_bytes_for_batches(&batches, &stage.build_keys)?;
		Ok(
			reoriented_stage_scan_build_decision_for_materialized_batches(
				estimated,
				estimated_build_hash_bytes,
				scan_output_cap,
				batches,
			),
		)
	}

	async fn reoriented_inner_stage_scan_build_exceeds_scan_output_cap(
		&self,
		stage: &ReorientedInnerJoinStage<'_>,
		morsel: Option<(MorselId, u32)>,
	) -> Result<bool, ExecutionError> {
		Ok(matches!(
			self.reoriented_inner_stage_scan_build_decision(stage, morsel)
				.await?,
			ReorientedStageScanBuildDecision::Stream
		))
	}

	fn reoriented_terminal_scan_build_estimated_over_cap(
		&self,
		stage: &ReorientedInnerJoinStage<'_>,
		morsel: Option<(MorselId, u32)>,
	) -> bool {
		if morsel.is_some() || self.context.task_budget.memory.get() < 64 * 1024 * 1024 {
			return false;
		}
		let Some(scan_input) = decompose_streaming_scan_probe_input(stage.build) else {
			return false;
		};
		let ExecutionPlan::Scan {
			estimated_byte_size,
			..
		} = scan_input.scan
		else {
			return false;
		};
		estimated_byte_size
			.map(|bytes| {
				// This shortcut skips the measured scan-build classifier, so it only fires
				// when metadata says the terminal scan is catastrophically beyond the
				// materialization cap. At >=8x cap, the old grouped path needs many
				// scan passes before it can make progress, so one cap-guarded attempt to
				// build the other side is the safer first move. Moderate over-cap scans
				// keep the old measured path because projected metadata can be much
				// larger than the actual filtered/materialized build.
				bytes
					> join_output_materialization_memory_cap(self.context.task_budget.memory.get())
						.saturating_mul(8)
			})
			.unwrap_or(false)
	}

	async fn write_streaming_scan_inner_build_batch(
		&self,
		spilled: &mut SpilledInnerJoinBuild,
		batch: &RecordBatch,
		retained_columns: Option<&[crate::ir::ColumnOrdinal]>,
		key_plan: JoinKeyPlan,
		build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		retain_unkeyed_rows: bool,
	) -> Result<(), ExecutionError> {
		if let Some(retained_columns) = retained_columns {
			if retain_unkeyed_rows {
				spilled
					.write_projected_preserved_batch_chunks(
						batch,
						retained_columns,
						key_plan,
						build_keys,
						null_safe_keys,
						self.context.task_budget,
					)
					.await
			} else {
				spilled
					.write_projected_batch_chunks(
						batch,
						retained_columns,
						key_plan,
						build_keys,
						null_safe_keys,
						self.context.task_budget,
					)
					.await
			}
		} else {
			if retain_unkeyed_rows {
				spilled
					.write_preserved_batch_chunks(
						batch,
						key_plan,
						build_keys,
						null_safe_keys,
						self.context.task_budget,
					)
					.await
			} else {
				spilled
					.write_batch_chunks(
						batch,
						key_plan,
						build_keys,
						null_safe_keys,
						self.context.task_budget,
					)
					.await
			}
		}
	}

	#[allow(clippy::too_many_arguments)]
	async fn visit_streaming_spilled_inner_scan_probe_chunks<F>(
		&self,
		spilled: &mut SpilledInnerJoinBuild,
		scan_input: StreamingScanProbeInput<'_>,
		right_keys: &[crate::ir::ColumnOrdinal],
		key_plan: JoinKeyPlan,
		null_safe_keys: bool,
		materialized_output_schema: &arrow::datatypes::SchemaRef,
		output_schema: &arrow::datatypes::SchemaRef,
		probe_prefilter_predicate: Option<&BoundExprProgram>,
		filter_predicate: Option<&BoundExprProgram>,
		probe_columns_first: bool,
		build_column_count: usize,
		morsel: Option<(MorselId, u32)>,
		profile: Option<&GlobalFoldJoinFilterProfile>,
		visit_batch: &mut F,
	) -> Result<(u64, u64), ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		let mut probe_rows = 0u64;
		let mut output_rows = 0u64;
		if let Some(exchange) = scan_input.exchange {
			let Some(req) = self.bound_scan_request(scan_input.scan, morsel) else {
				return Err(ExecutionError::InvalidExpression {
					message: "streaming scan probe expected a scan input".to_string(),
				});
			};
			let mut remaining = req.row_limit;
			let span = crate::telemetry::storage_scan_span(&req);
			let mut stream = self
				.storage
				.scan(req)
				.instrument(span)
				.await?
				.into_batches();
			loop {
				let scan_timer = profile
					.map(|profile| profile.begin_leaf(GlobalFoldProfileLeaf::ScanPollDecode))
					.transpose()?;
				let next = stream.next().await;
				if let Some(timer) = scan_timer {
					timer.finish()?;
				}
				let Some(batch) = next else {
					break;
				};
				let scan_timer = profile
					.map(|profile| profile.begin_leaf(GlobalFoldProfileLeaf::ScanPollDecode))
					.transpose()?;
				let prepared = (|| {
					let Some(mut batch) = limit_streaming_scan_batch(batch?, &mut remaining) else {
						return Ok(None);
					};
					if let Some(predicate) = scan_input.filter_predicate {
						batch = self.filter_batch_with_bound_predicate(
							predicate,
							batch,
							"filter predicate",
						)?;
					}
					if batch.num_rows() == 0 {
						return Ok(None);
					}
					if let Some(predicate) = probe_prefilter_predicate {
						batch = self.filter_batch_with_bound_predicate(
							predicate,
							batch,
							"probe filter predicate",
						)?;
					}
					Ok::<Option<RecordBatch>, ExecutionError>(
						(batch.num_rows() > 0).then_some(batch),
					)
				})();
				if let Some(timer) = scan_timer {
					timer.finish()?;
				}
				let Some(batch) = prepared? else {
					if remaining == Some(0) {
						break;
					}
					continue;
				};
				let partition_timer = profile
					.map(|profile| profile.begin_leaf(GlobalFoldProfileLeaf::ProbeLookupSelect))
					.transpose()?;
				let partitioned =
					self.repartition_exchange_batch(&batch, exchange.partition_count, exchange.key);
				if let Some(timer) = partition_timer {
					timer.finish()?;
				}
				for (_, partitioned) in partitioned? {
					let (batch_probe_rows, batch_output_rows) = self
						.visit_streaming_spilled_inner_probe_batch(
							spilled,
							&partitioned,
							right_keys,
							key_plan,
							null_safe_keys,
							materialized_output_schema,
							output_schema,
							filter_predicate,
							probe_columns_first,
							build_column_count,
							None,
							profile,
							None,
							&mut *visit_batch,
						)
						.await?;
					probe_rows = probe_rows.saturating_add(batch_probe_rows);
					output_rows = output_rows.saturating_add(batch_output_rows);
				}
			}
		} else {
			let Some(req) = self.bound_scan_request(scan_input.scan, morsel) else {
				return Err(ExecutionError::InvalidExpression {
					message: "streaming scan probe expected a scan input".to_string(),
				});
			};
			let mut remaining = req.row_limit;
			let span = crate::telemetry::storage_scan_span(&req);
			let mut stream = self
				.storage
				.scan(req)
				.instrument(span)
				.await?
				.into_batches();
			while let Some(batch) = stream.next().await {
				let Some(mut batch) = limit_streaming_scan_batch(batch?, &mut remaining) else {
					break;
				};
				if let Some(predicate) = scan_input.filter_predicate {
					batch = self.filter_batch_with_bound_predicate(
						predicate,
						batch,
						"filter predicate",
					)?;
					if batch.num_rows() == 0 {
						continue;
					}
				}
				if let Some(predicate) = probe_prefilter_predicate {
					batch = self.filter_batch_with_bound_predicate(
						predicate,
						batch,
						"probe filter predicate",
					)?;
					if batch.num_rows() == 0 {
						continue;
					}
				}
				let (batch_probe_rows, batch_output_rows) = self
					.visit_streaming_spilled_inner_probe_batch(
						spilled,
						&batch,
						right_keys,
						key_plan,
						null_safe_keys,
						materialized_output_schema,
						output_schema,
						filter_predicate,
						probe_columns_first,
						build_column_count,
						None,
						profile,
						None,
						&mut *visit_batch,
					)
					.await?;
				probe_rows = probe_rows.saturating_add(batch_probe_rows);
				output_rows = output_rows.saturating_add(batch_output_rows);
			}
		}
		Ok((probe_rows, output_rows))
	}

	#[allow(clippy::too_many_arguments)]
	async fn visit_partitioned_streaming_scan_probe_group_chunks<F>(
		&self,
		partitioned_builds: &mut [Option<SpilledInnerJoinBuild>],
		scan_input: StreamingScanProbeInput<'_>,
		right_keys: &[crate::ir::ColumnOrdinal],
		partition_count: u32,
		partition_start: usize,
		partition_end: usize,
		key_plan: JoinKeyPlan,
		null_safe_keys: bool,
		materialized_output_schema: &arrow::datatypes::SchemaRef,
		output_schema: &arrow::datatypes::SchemaRef,
		probe_prefilter_predicate: Option<&BoundExprProgram>,
		filter_predicate: Option<&BoundExprProgram>,
		probe_columns_first: bool,
		build_column_count: usize,
		morsel: Option<(MorselId, u32)>,
		profile: Option<&GlobalFoldJoinFilterProfile>,
		visit_batch: &mut F,
	) -> Result<(u64, u64), ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		let mut probe_rows = 0u64;
		let mut output_rows = 0u64;
		if let Some(exchange) = scan_input.exchange {
			let Some(req) = self.bound_scan_request(scan_input.scan, morsel) else {
				return Err(ExecutionError::InvalidExpression {
					message: "streaming scan probe expected a scan input".to_string(),
				});
			};
			let mut remaining = req.row_limit;
			let span = crate::telemetry::storage_scan_span(&req);
			let mut stream = self
				.storage
				.scan(req)
				.instrument(span)
				.await?
				.into_batches();
			loop {
				let scan_timer = profile
					.map(|profile| profile.begin_leaf(GlobalFoldProfileLeaf::ScanPollDecode))
					.transpose()?;
				let next = stream.next().await;
				if let Some(timer) = scan_timer {
					timer.finish()?;
				}
				let Some(batch) = next else {
					break;
				};
				let scan_timer = profile
					.map(|profile| profile.begin_leaf(GlobalFoldProfileLeaf::ScanPollDecode))
					.transpose()?;
				let prepared = (|| {
					let Some(mut batch) = limit_streaming_scan_batch(batch?, &mut remaining) else {
						return Ok(None);
					};
					if let Some(predicate) = scan_input.filter_predicate {
						batch = self.filter_batch_with_bound_predicate(
							predicate,
							batch,
							"filter predicate",
						)?;
					}
					if batch.num_rows() == 0 {
						return Ok(None);
					}
					if let Some(predicate) = probe_prefilter_predicate {
						batch = self.filter_batch_with_bound_predicate(
							predicate,
							batch,
							"probe filter predicate",
						)?;
					}
					Ok::<Option<RecordBatch>, ExecutionError>(
						(batch.num_rows() > 0).then_some(batch),
					)
				})();
				if let Some(timer) = scan_timer {
					timer.finish()?;
				}
				let Some(batch) = prepared? else {
					if remaining == Some(0) {
						break;
					}
					continue;
				};
				let partition_timer = profile
					.map(|profile| profile.begin_leaf(GlobalFoldProfileLeaf::ProbeLookupSelect))
					.transpose()?;
				let partitioned =
					self.repartition_exchange_batch(&batch, exchange.partition_count, exchange.key);
				if let Some(timer) = partition_timer {
					timer.finish()?;
				}
				for (_, partitioned) in partitioned? {
					let (batch_probe_rows, batch_output_rows) = self
						.visit_partitioned_streaming_scan_probe_batch(
							partitioned_builds,
							&partitioned,
							right_keys,
							partition_count,
							partition_start,
							partition_end,
							key_plan,
							null_safe_keys,
							materialized_output_schema,
							output_schema,
							filter_predicate,
							probe_columns_first,
							build_column_count,
							profile,
							visit_batch,
						)
						.await?;
					probe_rows = probe_rows.saturating_add(batch_probe_rows);
					output_rows = output_rows.saturating_add(batch_output_rows);
				}
			}
		} else {
			let Some(req) = self.bound_scan_request(scan_input.scan, morsel) else {
				return Err(ExecutionError::InvalidExpression {
					message: "streaming scan probe expected a scan input".to_string(),
				});
			};
			let mut remaining = req.row_limit;
			let span = crate::telemetry::storage_scan_span(&req);
			let mut stream = self
				.storage
				.scan(req)
				.instrument(span)
				.await?
				.into_batches();
			loop {
				let scan_timer = profile
					.map(|profile| profile.begin_leaf(GlobalFoldProfileLeaf::ScanPollDecode))
					.transpose()?;
				let next = stream.next().await;
				if let Some(timer) = scan_timer {
					timer.finish()?;
				}
				let Some(batch) = next else {
					break;
				};
				let scan_timer = profile
					.map(|profile| profile.begin_leaf(GlobalFoldProfileLeaf::ScanPollDecode))
					.transpose()?;
				let prepared = (|| {
					let Some(mut batch) = limit_streaming_scan_batch(batch?, &mut remaining) else {
						return Ok(None);
					};
					if let Some(predicate) = scan_input.filter_predicate {
						batch = self.filter_batch_with_bound_predicate(
							predicate,
							batch,
							"filter predicate",
						)?;
					}
					if batch.num_rows() == 0 {
						return Ok(None);
					}
					if let Some(predicate) = probe_prefilter_predicate {
						batch = self.filter_batch_with_bound_predicate(
							predicate,
							batch,
							"probe filter predicate",
						)?;
					}
					Ok::<Option<RecordBatch>, ExecutionError>(
						(batch.num_rows() > 0).then_some(batch),
					)
				})();
				if let Some(timer) = scan_timer {
					timer.finish()?;
				}
				let Some(batch) = prepared? else {
					if remaining == Some(0) {
						break;
					}
					continue;
				};
				let (batch_probe_rows, batch_output_rows) = self
					.visit_partitioned_streaming_scan_probe_batch(
						partitioned_builds,
						&batch,
						right_keys,
						partition_count,
						partition_start,
						partition_end,
						key_plan,
						null_safe_keys,
						materialized_output_schema,
						output_schema,
						filter_predicate,
						probe_columns_first,
						build_column_count,
						profile,
						visit_batch,
					)
					.await?;
				probe_rows = probe_rows.saturating_add(batch_probe_rows);
				output_rows = output_rows.saturating_add(batch_output_rows);
			}
		}
		Ok((probe_rows, output_rows))
	}

	#[allow(clippy::too_many_arguments)]
	async fn visit_partitioned_streaming_scan_probe_batch<F>(
		&self,
		partitioned_builds: &mut [Option<SpilledInnerJoinBuild>],
		batch: &RecordBatch,
		right_keys: &[crate::ir::ColumnOrdinal],
		partition_count: u32,
		partition_start: usize,
		partition_end: usize,
		key_plan: JoinKeyPlan,
		null_safe_keys: bool,
		materialized_output_schema: &arrow::datatypes::SchemaRef,
		output_schema: &arrow::datatypes::SchemaRef,
		filter_predicate: Option<&BoundExprProgram>,
		probe_columns_first: bool,
		build_column_count: usize,
		profile: Option<&GlobalFoldJoinFilterProfile>,
		visit_batch: &mut F,
	) -> Result<(u64, u64), ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		let partition_timer = profile
			.map(|profile| profile.begin_leaf(GlobalFoldProfileLeaf::ProbeLookupSelect))
			.transpose()?;
		let partitioned = (|| {
			let buckets = precompute_join_partition_buckets(
				batch,
				right_keys,
				partition_count,
				null_safe_keys,
				false,
			)?;
			partition_streaming_join_batch_with_buckets(batch, &buckets, partition_count)
		})();
		if let Some(timer) = partition_timer {
			timer.finish()?;
		}
		let mut probe_rows = 0u64;
		let mut output_rows = 0u64;
		for (partition, partitioned) in partitioned? {
			if partition < partition_start || partition >= partition_end {
				continue;
			}
			let Some(spilled) = partitioned_builds
				.get_mut(partition)
				.and_then(Option::as_mut)
			else {
				continue;
			};
			if spilled.input_row_count() == 0 || partitioned.num_rows() == 0 {
				continue;
			}
			let (partition_probe_rows, partition_output_rows) = self
				.visit_streaming_spilled_inner_probe_batch(
					spilled,
					&partitioned,
					right_keys,
					key_plan,
					null_safe_keys,
					materialized_output_schema,
					output_schema,
					filter_predicate,
					probe_columns_first,
					build_column_count,
					None,
					profile,
					None,
					visit_batch,
				)
				.await?;
			probe_rows = probe_rows.saturating_add(partition_probe_rows);
			output_rows = output_rows.saturating_add(partition_output_rows);
		}
		Ok((probe_rows, output_rows))
	}

	#[allow(clippy::too_many_arguments)]
	async fn visit_streaming_spilled_inner_probe_batch<F>(
		&self,
		spilled: &mut SpilledInnerJoinBuild,
		probe_batch: &RecordBatch,
		right_keys: &[crate::ir::ColumnOrdinal],
		key_plan: JoinKeyPlan,
		null_safe_keys: bool,
		materialized_output_schema: &arrow::datatypes::SchemaRef,
		output_schema: &arrow::datatypes::SchemaRef,
		filter_predicate: Option<&BoundExprProgram>,
		probe_columns_first: bool,
		build_column_count: usize,
		projection: Option<&ProjectedInnerBuildProjection>,
		profile: Option<&GlobalFoldJoinFilterProfile>,
		grouped_chain_observer: Option<&GroupedChainSpillObserver<'_>>,
		visit_batch: &mut F,
	) -> Result<(u64, u64), ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		let mut visit_output = |batch: RecordBatch| -> Result<(), ExecutionError> {
			let grouped_materialize_timer = grouped_chain_observer
				.map(|observer| observer.begin(GroupedChainSubLeaf::ReadMaterialization))
				.transpose()?;
			let materialize_timer = profile
				.map(|profile| profile.begin_leaf(GlobalFoldProfileLeaf::ArrowMaterialize))
				.transpose()?;
			let reordered = if probe_columns_first {
				reorder_spilled_inner_probe_before_build(batch, build_column_count, output_schema)
			} else {
				Ok(batch)
			};
			if let Some(timer) = materialize_timer {
				timer.finish()?;
			}
			let batch = reordered?;
			if let Some(timer) = grouped_materialize_timer {
				timer.finish_with(
					batch.num_rows() as u64,
					record_batch_memory_bytes(&batch),
					1,
				)?;
			}
			let predicate_input_rows = batch.num_rows();
			let predicate_timer = profile
				.map(|profile| profile.begin_leaf(GlobalFoldProfileLeaf::PredicateEvalFilter))
				.transpose()?;
			let filtered = if let Some(predicate) = filter_predicate {
				self.filter_batch_with_bound_predicate(predicate, batch, "filter predicate")
			} else {
				Ok(batch)
			};
			if let Some(timer) = predicate_timer {
				timer.finish()?;
			}
			let batch = filtered?;
			if let Some(profile) = profile {
				profile.record_predicate_rows(predicate_input_rows, batch.num_rows())?;
			}
			if batch.num_rows() > 0 {
				visit_batch(batch)?;
			}
			Ok(())
		};
		let pin_timer = profile
			.map(|profile| profile.begin_leaf(GlobalFoldProfileLeaf::SpillPageRead))
			.transpose()?;
		let pin = spilled
			.try_pin_decoded_pages_profiled(
				self.context.task_budget.memory.get(),
				grouped_chain_observer,
			)
			.await;
		if let Some(timer) = pin_timer {
			timer.finish()?;
		}
		let _pin_admitted = pin?.is_admitted();
		if let Some(projection) = projection {
			visit_projected_streamed_spilled_inner_join_chunks(
				spilled,
				std::slice::from_ref(probe_batch),
				right_keys,
				key_plan,
				null_safe_keys,
				&projection.full_build_schema,
				&projection.retained_columns,
				materialized_output_schema,
				self.context.task_budget.memory.get(),
				grouped_chain_observer,
				&mut visit_output,
			)
			.await
		} else {
			visit_streamed_spilled_inner_join_chunks_profiled(
				spilled,
				std::slice::from_ref(probe_batch),
				right_keys,
				key_plan,
				null_safe_keys,
				materialized_output_schema,
				self.context.task_budget.memory.get(),
				profile,
				grouped_chain_observer,
				&mut visit_output,
			)
			.await
		}
	}

	#[allow(clippy::too_many_arguments)]
	async fn visit_streaming_in_memory_inner_scan_probe_chunks<F>(
		&self,
		build_batch: &RecordBatch,
		build_rows: &JoinBuildRows,
		join_key_plan: JoinKeyPlan,
		scan_input: StreamingScanProbeInput<'_>,
		probe_keys: &[crate::ir::ColumnOrdinal],
		match_filter: Option<&BoundExprProgram>,
		null_safe_keys: bool,
		output_schema: &arrow::datatypes::SchemaRef,
		probe_prefilter_predicate: Option<&BoundExprProgram>,
		filter_predicate: Option<&BoundExprProgram>,
		probe_columns_first: bool,
		morsel: Option<(MorselId, u32)>,
		visit_batch: &mut F,
	) -> Result<(u64, u64), ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		let mut probe_rows = 0u64;
		let mut output_rows = 0u64;
		let mut process_probe_batch = |right_batch: RecordBatch| -> Result<(), ExecutionError> {
			probe_rows = probe_rows.saturating_add(right_batch.num_rows() as u64);
			let key_started = local_join_phase_enabled().then(Instant::now);
			let right_key_arrays = key_arrays(&right_batch, probe_keys)?;
			let selection = probe_in_memory_join_rows(
				JoinKind::Inner,
				join_key_plan,
				build_rows,
				build_batch.num_rows(),
				&right_key_arrays,
				right_batch.num_rows(),
				match_filter,
				build_batch,
				&right_batch,
				null_safe_keys,
			)?;
			if let Some(started) = key_started {
				local_join_phase_record_elapsed(LocalJoinPhase::ProbeKey, started);
				local_join_phase_record_flow(
					LocalJoinPhase::ProbeKey,
					1,
					right_batch.num_rows() as u64,
					selection.len() as u64,
				);
			}
			output_rows = output_rows.saturating_add(selection.len() as u64);
			self.visit_reoriented_inner_selection_filtered_chunks(
				build_batch,
				&right_batch,
				&selection,
				output_schema,
				filter_predicate,
				probe_columns_first,
				visit_batch,
			)?;
			Ok(())
		};
		if let Some(exchange) = scan_input.exchange {
			let Some(req) = self.bound_scan_request(scan_input.scan, morsel) else {
				return Err(ExecutionError::InvalidExpression {
					message: "streaming scan probe expected a scan input".to_string(),
				});
			};
			let mut remaining = req.row_limit;
			let span = crate::telemetry::storage_scan_span(&req);
			let mut stream = self
				.storage
				.scan(req)
				.instrument(span)
				.await?
				.into_batches();
			// `loop` (not `while let`) so the probe-scan timer can bracket `stream.next().await`
			// (the decode); break conditions are identical to the prior `while let`.
			loop {
				let scan_started = local_join_phase_enabled().then(Instant::now);
				let Some(batch) = stream.next().await else {
					break;
				};
				let Some(mut right_batch) = limit_streaming_scan_batch(batch?, &mut remaining)
				else {
					break;
				};
				if let Some(predicate) = scan_input.filter_predicate {
					right_batch = self.filter_batch_with_bound_predicate(
						predicate,
						right_batch,
						"filter predicate",
					)?;
					if right_batch.num_rows() == 0 {
						continue;
					}
				}
				if let Some(predicate) = probe_prefilter_predicate {
					right_batch = self.filter_batch_with_bound_predicate(
						predicate,
						right_batch,
						"probe filter predicate",
					)?;
					if right_batch.num_rows() == 0 {
						continue;
					}
				}
				if let Some(started) = scan_started {
					local_join_phase_record_elapsed(LocalJoinPhase::ProbeScan, started);
					local_join_phase_record_flow(
						LocalJoinPhase::ProbeScan,
						1,
						right_batch.num_rows() as u64,
						right_batch.num_rows() as u64,
					);
				}
				for (_, partitioned) in self.repartition_exchange_batch(
					&right_batch,
					exchange.partition_count,
					exchange.key,
				)? {
					process_probe_batch(partitioned)?;
				}
			}
		} else {
			let Some(req) = self.bound_scan_request(scan_input.scan, morsel) else {
				return Err(ExecutionError::InvalidExpression {
					message: "streaming scan probe expected a scan input".to_string(),
				});
			};
			let mut remaining = req.row_limit;
			let span = crate::telemetry::storage_scan_span(&req);
			let mut stream = self
				.storage
				.scan(req)
				.instrument(span)
				.await?
				.into_batches();
			// `loop` (not `while let`) so the probe-scan timer can bracket `stream.next().await`
			// (the decode); break conditions are identical to the prior `while let`.
			loop {
				let scan_started = local_join_phase_enabled().then(Instant::now);
				let Some(batch) = stream.next().await else {
					break;
				};
				let Some(mut right_batch) = limit_streaming_scan_batch(batch?, &mut remaining)
				else {
					break;
				};
				if let Some(predicate) = scan_input.filter_predicate {
					right_batch = self.filter_batch_with_bound_predicate(
						predicate,
						right_batch,
						"filter predicate",
					)?;
					if right_batch.num_rows() == 0 {
						continue;
					}
				}
				if let Some(predicate) = probe_prefilter_predicate {
					right_batch = self.filter_batch_with_bound_predicate(
						predicate,
						right_batch,
						"probe filter predicate",
					)?;
					if right_batch.num_rows() == 0 {
						continue;
					}
				}
				if let Some(started) = scan_started {
					local_join_phase_record_elapsed(LocalJoinPhase::ProbeScan, started);
					local_join_phase_record_flow(
						LocalJoinPhase::ProbeScan,
						1,
						right_batch.num_rows() as u64,
						right_batch.num_rows() as u64,
					);
				}
				process_probe_batch(right_batch)?;
			}
		}
		Ok((probe_rows, output_rows))
	}

	fn visit_reoriented_inner_selection_filtered_chunks<F>(
		&self,
		build_batch: &RecordBatch,
		probe_batch: &RecordBatch,
		selection: &[JoinOutputRow],
		output_schema: &arrow::datatypes::SchemaRef,
		filter_predicate: Option<&BoundExprProgram>,
		probe_columns_first: bool,
		visit_batch: &mut F,
	) -> Result<(), ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		for chunk in selection.chunks(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let materialize_started = local_join_phase_enabled().then(Instant::now);
			let batch = materialize_reoriented_inner_selection(
				probe_batch,
				build_batch,
				chunk,
				output_schema,
				self.context.task_budget.memory.get(),
				probe_columns_first,
			)?;
			let batch = if let Some(predicate) = filter_predicate {
				self.filter_batch_with_bound_predicate(predicate, batch, "filter predicate")?
			} else {
				batch
			};
			if let Some(started) = materialize_started {
				local_join_phase_record_elapsed(LocalJoinPhase::OutputMaterialize, started);
				local_join_phase_record_flow(
					LocalJoinPhase::OutputMaterialize,
					1,
					chunk.len() as u64,
					batch.num_rows() as u64,
				);
			}
			if batch.num_rows() > 0 {
				// For the fused ungrouped-count path `visit_batch` is the global fold; timing it
				// here isolates aggregate-fold from output-materialize (the load-bearing split).
				let fold_started = local_join_phase_enabled().then(Instant::now);
				let fold_rows = batch.num_rows() as u64;
				visit_batch(batch)?;
				if let Some(started) = fold_started {
					local_join_phase_record_elapsed(LocalJoinPhase::AggregateFold, started);
					local_join_phase_record_flow(LocalJoinPhase::AggregateFold, 1, fold_rows, 0);
				}
			}
		}
		Ok(())
	}

	pub(in crate::exec) async fn try_visit_streaming_join0_chunks<F>(
		&self,
		input: &ExecutionPlan,
		filter_predicate: Option<&BoundExprProgram>,
		morsel: Option<(MorselId, u32)>,
		profile: Option<&GlobalFoldJoinFilterProfile>,
		mut visit_batch: F,
	) -> Result<Option<()>, ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		let ExecutionPlan::Join {
			replay_id,
			left,
			right,
			kind: JoinKind::Inner,
			left_keys,
			right_keys,
			match_filter,
			null_safe_keys,
			strategy,
			output_schema,
			..
		} = input
		else {
			return Ok(None);
		};
		if *replay_id != JoinReplayId(0) {
			return Ok(None);
		}

		let left_is_scan_probe = decompose_streaming_scan_probe_input(left).is_some();
		let right_is_scan_probe = decompose_streaming_scan_probe_input(right).is_some();
		let physical_scan_build = physical_scan_build_enabled();
		let mut left_batches = None;
		let mut right_batches = None;
		// Both scan inputs still obey the physical join contract: left builds and
		// right probes. In particular, SwappedRightBuild has already moved the
		// selected smaller build to the left. Trying the symmetric scan route first
		// silently undoes that placement and can spill the larger declared probe.
		if left_is_scan_probe && (!right_is_scan_probe || !physical_scan_build) {
			if right_is_scan_probe
				&& !physical_scan_build
				&& self
					.try_visit_streaming_inner_join_scan_probe_chunks(
						right,
						left,
						None,
						right_keys,
						left_keys,
						match_filter.as_ref(),
						*null_safe_keys,
						*replay_id,
						*strategy,
						output_schema,
						filter_predicate,
						true,
						morsel,
						profile,
						&mut visit_batch,
					)
					.await?
					.is_some()
			{
				return Ok(Some(()));
			}
			let build_scan_started = local_join_phase_enabled().then(Instant::now);
			let build_batches = self.execute_bound(right, morsel).await?;
			if let Some(started) = build_scan_started {
				local_join_phase_record_elapsed(LocalJoinPhase::Build, started);
			}
			if self
				.try_visit_streaming_inner_join_scan_probe_chunks(
					right,
					left,
					Some(&build_batches),
					right_keys,
					left_keys,
					match_filter.as_ref(),
					*null_safe_keys,
					*replay_id,
					*strategy,
					output_schema,
					filter_predicate,
					true,
					morsel,
					profile,
					&mut visit_batch,
				)
				.await?
				.is_some()
			{
				return Ok(Some(()));
			}
			right_batches = Some(build_batches);
		}
		if right_is_scan_probe {
			if left_is_scan_probe
				&& self
					.try_visit_streaming_inner_join_scan_probe_chunks(
						left,
						right,
						None,
						left_keys,
						right_keys,
						match_filter.as_ref(),
						*null_safe_keys,
						*replay_id,
						*strategy,
						output_schema,
						filter_predicate,
						false,
						morsel,
						profile,
						&mut visit_batch,
					)
					.await?
					.is_some()
			{
				return Ok(Some(()));
			}
			let build_scan_started = local_join_phase_enabled().then(Instant::now);
			let build_batches = match left_batches.take() {
				Some(batches) => batches,
				None => self.execute_bound(left, morsel).await?,
			};
			if let Some(started) = build_scan_started {
				local_join_phase_record_elapsed(LocalJoinPhase::Build, started);
			}
			if self
				.try_visit_streaming_inner_join_scan_probe_chunks(
					left,
					right,
					Some(&build_batches),
					left_keys,
					right_keys,
					match_filter.as_ref(),
					*null_safe_keys,
					*replay_id,
					*strategy,
					output_schema,
					filter_predicate,
					false,
					morsel,
					profile,
					&mut visit_batch,
				)
				.await?
				.is_some()
			{
				return Ok(Some(()));
			}
			left_batches = Some(build_batches);
		}
		let left_batches = match left_batches {
			Some(batches) => batches,
			None => self.execute_bound(left, morsel).await?,
		};
		let right_batches = match right_batches {
			Some(batches) => batches,
			None => self.execute_bound(right, morsel).await?,
		};
		let build_started = Instant::now();
		if let Some((build_rows, probe_rows, output_rows)) = self
			.try_visit_partitioned_streamed_inner_materialized_build_probe_chunks(
				left.schema(),
				right.schema(),
				&left_batches,
				&right_batches,
				left_keys,
				right_keys,
				match_filter.as_ref(),
				*null_safe_keys,
				output_schema,
				filter_predicate,
				morsel,
				&mut visit_batch,
			)
			.await?
		{
			record_join_runtime_observation(
				&self.context.task_telemetry,
				&self.context.join_runtime_observation_mode,
				JoinRuntimeObservation {
					replay_id: *replay_id,
					strategy: *strategy,
					build_rows: RowCount::new(build_rows),
					probe_rows: RowCount::new(probe_rows),
					output_rows: RowCount::new(output_rows),
				},
			);
			return Ok(Some(()));
		}
		if let Some((mut spilled, key_plan)) = try_create_streamed_spilled_inner_build(
			left.schema(),
			right.schema(),
			&left_batches,
			JoinKind::Inner,
			left_keys,
			right_keys,
			match_filter.as_ref(),
			*null_safe_keys,
			self.context.task_budget,
			self.context.join_spill_file_ops.clone(),
			self.context.join_spill_dir.clone(),
		)
		.await?
		{
			record_task_telemetry(&self.context.task_telemetry, |task| {
				task.phases.build_micros = task
					.phases
					.build_micros
					.saturating_add(duration_micros(build_started));
				task.counters.build_rows = task
					.counters
					.build_rows
					.saturating_add(spilled.input_row_count());
				task.phases.spill_micros = task
					.phases
					.spill_micros
					.saturating_add(duration_micros(build_started));
				task.counters.spill_bytes = task
					.counters
					.spill_bytes
					.saturating_add(spilled.spill_bytes());
			});
			let probe_started = Instant::now();
			let result = if let Some(predicate) = filter_predicate {
				visit_streamed_spilled_inner_join_filtered_chunks(
					self,
					&mut spilled,
					&right_batches,
					right_keys,
					key_plan,
					*null_safe_keys,
					output_schema,
					predicate,
					self.context.task_budget.memory.get(),
					&mut visit_batch,
				)
				.await
			} else {
				visit_streamed_spilled_inner_join_chunks(
					&mut spilled,
					&right_batches,
					right_keys,
					key_plan,
					*null_safe_keys,
					output_schema,
					self.context.task_budget.memory.get(),
					&mut visit_batch,
				)
				.await
			};
			let build_rows = spilled.input_row_count();
			record_task_telemetry(&self.context.task_telemetry, |task| {
				task.phases.probe_micros = task
					.phases
					.probe_micros
					.saturating_add(duration_micros(probe_started));
				if let Ok((probe_rows, output_rows)) = &result {
					task.counters.probe_rows = task.counters.probe_rows.saturating_add(*probe_rows);
					task.counters.matched_rows =
						task.counters.matched_rows.saturating_add(*output_rows);
				}
			});
			if let Ok((probe_rows, output_rows)) = &result {
				record_join_runtime_observation(
					&self.context.task_telemetry,
					&self.context.join_runtime_observation_mode,
					JoinRuntimeObservation {
						replay_id: *replay_id,
						strategy: *strategy,
						build_rows: RowCount::new(build_rows),
						probe_rows: RowCount::new(*probe_rows),
						output_rows: RowCount::new(*output_rows),
					},
				);
			}
			spilled.cleanup().await;
			result?;
			return Ok(Some(()));
		}

		let left_batch = concat_or_empty(left.schema(), &left_batches)?;
		let right_batch = concat_or_empty(right.schema(), &right_batches)?;
		let left_key_arrays = key_arrays(&left_batch, left_keys)?;
		let right_key_arrays = key_arrays(&right_batch, right_keys)?;
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.build_rows = task
				.counters
				.build_rows
				.saturating_add(left_batch.num_rows() as u64);
			task.counters.probe_rows = task
				.counters
				.probe_rows
				.saturating_add(right_batch.num_rows() as u64);
		});
		if estimated_build_hash_bytes(&left_key_arrays, left_batch.num_rows())
			> self.context.task_budget.memory.get()
		{
			let output_rows = visit_spilled_hash_join_filtered_chunks(
				self,
				JoinKind::Inner,
				&left_batch,
				&right_batch,
				&left_key_arrays,
				&right_key_arrays,
				output_schema,
				filter_predicate,
				match_filter.as_ref(),
				*null_safe_keys,
				self.context.task_budget,
				self.context.task_telemetry.clone(),
				self.context.join_spill_file_ops.clone(),
				self.context.join_spill_dir.clone(),
				&mut visit_batch,
			)
			.await?;
			record_join_runtime_observation(
				&self.context.task_telemetry,
				&self.context.join_runtime_observation_mode,
				JoinRuntimeObservation {
					replay_id: *replay_id,
					strategy: *strategy,
					build_rows: RowCount::new(left_batch.num_rows() as u64),
					probe_rows: RowCount::new(right_batch.num_rows() as u64),
					output_rows: RowCount::new(output_rows),
				},
			);
			return Ok(Some(()));
		}
		let join_key_plan =
			JoinKeyPlan::from_key_arrays(&left_key_arrays, &right_key_arrays, *null_safe_keys);
		let build_started = Instant::now();
		let Some(build_rows) = build_in_memory_join_rows(
			join_key_plan,
			&left_key_arrays,
			left_batch.num_rows(),
			self.context.task_budget.memory.get(),
			*null_safe_keys,
		)?
		else {
			return Ok(None);
		};
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.phases.build_micros = task
				.phases
				.build_micros
				.saturating_add(duration_micros(build_started));
		});
		let probe_started = Instant::now();
		let selection = probe_in_memory_join_rows(
			JoinKind::Inner,
			join_key_plan,
			&build_rows,
			left_batch.num_rows(),
			&right_key_arrays,
			right_batch.num_rows(),
			match_filter.as_ref(),
			&left_batch,
			&right_batch,
			*null_safe_keys,
		)?;
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.phases.probe_micros = task
				.phases
				.probe_micros
				.saturating_add(duration_micros(probe_started));
			task.counters.matched_rows = task
				.counters
				.matched_rows
				.saturating_add(selection.len() as u64);
		});
		visit_join_selection_filtered_chunks(
			self,
			JoinKind::Inner,
			&left_batch,
			&right_batch,
			&selection,
			output_schema,
			filter_predicate,
			self.context.task_budget.memory.get(),
			&mut visit_batch,
		)?;
		record_join_runtime_observation(
			&self.context.task_telemetry,
			&self.context.join_runtime_observation_mode,
			JoinRuntimeObservation {
				replay_id: *replay_id,
				strategy: *strategy,
				build_rows: RowCount::new(left_batch.num_rows() as u64),
				probe_rows: RowCount::new(right_batch.num_rows() as u64),
				output_rows: RowCount::new(selection.len() as u64),
			},
		);
		Ok(Some(()))
	}

	pub(in crate::exec) async fn try_streaming_left_outer_grouped_aggregate(
		&self,
		input: &ExecutionPlan,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		output_schema: &arrow::datatypes::SchemaRef,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
		let Some(streaming_input) =
			decompose_streaming_left_outer_grouped_input(input, group_expr, aggr_expr)?
		else {
			return Ok(None);
		};
		let ExecutionPlan::Join {
			replay_id,
			left,
			right,
			kind: JoinKind::LeftOuter,
			left_keys,
			right_keys,
			match_filter,
			null_safe_keys,
			strategy: JoinStrategy::HashPartitioned,
			output_schema: join_output_schema,
			..
		} = streaming_input.join
		else {
			return Ok(None);
		};
		let Some(scan_input) = decompose_streaming_scan_probe_input(right) else {
			return Ok(None);
		};
		if self.context.task_budget.memory.get() < 64 * 1024 * 1024 {
			return Ok(None);
		}

		if match_filter.is_none() {
			if let Some(left_scan_input) = decompose_streaming_scan_probe_input(left) {
				let retained_columns = retained_left_outer_grouped_columns(
					left.schema().fields().len(),
					left_keys,
					group_expr,
					aggr_expr,
					streaming_input.exchange,
				);
				if !retained_columns.is_empty() {
					let projected_left_keys = remap_column_ordinals(&retained_columns, left_keys)?;
					let projected_left_schema =
						projected_schema_for_columns(&left.schema(), &retained_columns)?;
					let key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
						&projected_left_schema,
						&projected_left_keys,
						&right.schema(),
						right_keys,
						*null_safe_keys,
					)?;
					let mut spilled = SpilledInnerJoinBuild::create(
						self.context.task_budget,
						self.context.join_spill_file_ops.clone(),
						self.context.join_spill_dir.clone(),
					)
					.await?;
					let build_started = Instant::now();
					self.write_streaming_scan_inner_build_batches(
						&mut spilled,
						left_scan_input,
						Some(&retained_columns),
						&projected_left_keys,
						key_plan,
						*null_safe_keys,
						true,
						None,
						morsel,
					)
					.await?;
					spilled.finish(self.context.task_budget).await?;
					let profile_build_micros = spilled
						.throughput_profile_enabled()
						.then(|| duration_micros(build_started));
					let build_rows = spilled.input_row_count();
					record_task_telemetry(&self.context.task_telemetry, |task| {
						task.counters.build_rows =
							task.counters.build_rows.saturating_add(build_rows);
						task.phases.build_micros = task
							.phases
							.build_micros
							.saturating_add(duration_micros(build_started));
						task.phases.spill_micros = task
							.phases
							.spill_micros
							.saturating_add(duration_micros(build_started));
						task.counters.spill_bytes = task
							.counters
							.spill_bytes
							.saturating_add(spilled.spill_bytes());
					});

					let mut groups = BoundGroupedAggregateGroups::new();
					let mut group_types = Vec::new();
					let mut folded_input = false;
					let mut matched_build_rows = spilled.empty_page_match_bitmap();
					let probe_started = Instant::now();
					let _pin_admitted = spilled
						.try_pin_decoded_pages(self.context.task_budget.memory.get())
						.await?
						.is_admitted();
					let (probe_rows, mut output_rows) = self
						.visit_streaming_spilled_left_outer_scan_probe_chunks(
							&mut spilled,
							scan_input,
							left.schema(),
							&retained_columns,
							&mut matched_build_rows,
							key_plan,
							right_keys,
							*null_safe_keys,
							join_output_schema,
							streaming_input.exchange,
							group_expr,
							aggr_expr,
							&mut groups,
							&mut group_types,
							&mut folded_input,
							morsel,
						)
						.await?;
					output_rows = output_rows.saturating_add(
						self.fold_streaming_spilled_left_outer_unmatched_rows(
							&mut spilled,
							&matched_build_rows,
							left.schema(),
							&retained_columns,
							right.schema(),
							join_output_schema,
							streaming_input.exchange,
							group_expr,
							aggr_expr,
							&mut groups,
							&mut group_types,
							&mut folded_input,
						)
						.await?,
					);
					if !folded_input {
						self.fold_bound_grouped_aggregate_batch(
							&mut groups,
							&mut group_types,
							group_expr,
							aggr_expr,
							&RecordBatch::new_empty(join_output_schema.clone()),
						)?;
					}
					let profile_probe_micros = spilled
						.throughput_profile_enabled()
						.then(|| duration_micros(probe_started));
					if let (Some(build_micros), Some(probe_micros)) =
						(profile_build_micros, profile_probe_micros)
					{
						spilled.record_throughput_phase_micros(build_micros, probe_micros);
					}
					record_task_telemetry(&self.context.task_telemetry, |task| {
						task.phases.probe_micros = task
							.phases
							.probe_micros
							.saturating_add(duration_micros(probe_started));
						task.counters.probe_rows =
							task.counters.probe_rows.saturating_add(probe_rows);
						task.counters.matched_rows =
							task.counters.matched_rows.saturating_add(output_rows);
					});
					record_join_runtime_observation(
						&self.context.task_telemetry,
						&self.context.join_runtime_observation_mode,
						JoinRuntimeObservation {
							replay_id: *replay_id,
							strategy: JoinStrategy::HashPartitioned,
							build_rows: RowCount::new(build_rows),
							probe_rows: RowCount::new(probe_rows),
							output_rows: RowCount::new(output_rows),
						},
					);
					let output = self.finalize_bound_grouped_aggregate(
						&mut groups,
						&group_types,
						aggr_expr,
						output_schema,
					);
					let profile_result = spilled.persist_throughput_profile();
					spilled.cleanup().await;
					profile_result?;
					return output.map(Some);
				}
			}
		}

		let left_batches = self.execute_bound(left, morsel).await?;
		let left_batch = concat_or_empty(left.schema(), &left_batches)?;
		let left_key_arrays = key_arrays(&left_batch, left_keys)?;
		if estimated_build_hash_bytes(&left_key_arrays, left_batch.num_rows())
			> self.context.task_budget.memory.get()
		{
			return Ok(None);
		}
		let join_key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&left.schema(),
			left_keys,
			&right.schema(),
			right_keys,
			*null_safe_keys,
		)?;
		let build_started = Instant::now();
		let Some(build_rows) = build_in_memory_join_rows(
			join_key_plan,
			&left_key_arrays,
			left_batch.num_rows(),
			self.context.task_budget.memory.get(),
			*null_safe_keys,
		)?
		else {
			return Ok(None);
		};
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.build_rows = task
				.counters
				.build_rows
				.saturating_add(left_batch.num_rows() as u64);
			task.phases.build_micros = task
				.phases
				.build_micros
				.saturating_add(duration_micros(build_started));
		});

		let mut groups = BoundGroupedAggregateGroups::new();
		let mut group_types = Vec::new();
		let mut folded_input = false;
		let mut matched_build_rows = build_match_bitmap(JoinKind::LeftOuter, left_batch.num_rows())
			.ok_or_else(|| ExecutionError::Kernel {
				message: "left outer streaming expected a build match bitmap".to_string(),
			})?;

		let probe_started = Instant::now();
		let (probe_rows, mut output_rows) = self
			.visit_streaming_left_outer_scan_probe_chunks(
				scan_input,
				&left_batch,
				&build_rows,
				&mut matched_build_rows,
				join_key_plan,
				right_keys,
				match_filter.as_ref(),
				*null_safe_keys,
				join_output_schema,
				streaming_input.exchange,
				group_expr,
				aggr_expr,
				&mut groups,
				&mut group_types,
				&mut folded_input,
				morsel,
			)
			.await?;
		output_rows = output_rows.saturating_add(self.fold_streaming_left_outer_unmatched_rows(
			&left_batch,
			&matched_build_rows,
			right.schema(),
			join_output_schema,
			streaming_input.exchange,
			group_expr,
			aggr_expr,
			&mut groups,
			&mut group_types,
			&mut folded_input,
		)?);
		if !folded_input {
			self.fold_bound_grouped_aggregate_batch(
				&mut groups,
				&mut group_types,
				group_expr,
				aggr_expr,
				&RecordBatch::new_empty(join_output_schema.clone()),
			)?;
		}
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.phases.probe_micros = task
				.phases
				.probe_micros
				.saturating_add(duration_micros(probe_started));
			task.counters.probe_rows = task.counters.probe_rows.saturating_add(probe_rows);
			task.counters.matched_rows = task.counters.matched_rows.saturating_add(output_rows);
		});
		record_join_runtime_observation(
			&self.context.task_telemetry,
			&self.context.join_runtime_observation_mode,
			JoinRuntimeObservation {
				replay_id: *replay_id,
				strategy: JoinStrategy::HashPartitioned,
				build_rows: RowCount::new(left_batch.num_rows() as u64),
				probe_rows: RowCount::new(probe_rows),
				output_rows: RowCount::new(output_rows),
			},
		);

		self.finalize_bound_grouped_aggregate(&mut groups, &group_types, aggr_expr, output_schema)
			.map(Some)
	}

	#[allow(clippy::too_many_arguments)]
	async fn visit_streaming_spilled_left_outer_scan_probe_chunks(
		&self,
		spilled: &mut SpilledInnerJoinBuild,
		scan_input: StreamingScanProbeInput<'_>,
		full_left_schema: SchemaRef,
		retained_columns: &[ColumnOrdinal],
		matched_build_rows: &mut [Vec<u64>],
		join_key_plan: JoinKeyPlan,
		right_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		join_output_schema: &arrow::datatypes::SchemaRef,
		exchange: Option<(u32, &BoundExprProgram)>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BoundGroupedAggregateGroups,
		group_types: &mut Vec<DataType>,
		folded_input: &mut bool,
		morsel: Option<(MorselId, u32)>,
	) -> Result<(u64, u64), ExecutionError> {
		let Some(req) = self.bound_scan_request(scan_input.scan, morsel) else {
			return Err(ExecutionError::InvalidExpression {
				message: "streaming spilled left outer probe expected a scan input".to_string(),
			});
		};
		let mut remaining = req.row_limit;
		let span = crate::telemetry::storage_scan_span(&req);
		let mut stream = self
			.storage
			.scan(req)
			.instrument(span)
			.await?
			.into_batches();
		let mut probe_rows = 0u64;
		let mut output_rows = 0u64;
		while let Some(batch) = stream.next().await {
			let Some(mut right_batch) = limit_streaming_scan_batch(batch?, &mut remaining) else {
				break;
			};
			if let Some(predicate) = scan_input.filter_predicate {
				right_batch = self.filter_batch_with_bound_predicate(
					predicate,
					right_batch,
					"filter predicate",
				)?;
				if right_batch.num_rows() == 0 {
					continue;
				}
			}
			if let Some(scan_exchange) = scan_input.exchange {
				for (_partition, partitioned) in self.repartition_exchange_batch(
					&right_batch,
					scan_exchange.partition_count,
					scan_exchange.key,
				)? {
					let (batch_probe_rows, batch_output_rows) = self
						.visit_streaming_spilled_left_outer_probe_batch(
							spilled,
							&partitioned,
							&full_left_schema,
							retained_columns,
							matched_build_rows,
							join_key_plan,
							right_keys,
							null_safe_keys,
							join_output_schema,
							exchange,
							group_expr,
							aggr_expr,
							groups,
							group_types,
							folded_input,
						)
						.await?;
					probe_rows = probe_rows.saturating_add(batch_probe_rows);
					output_rows = output_rows.saturating_add(batch_output_rows);
				}
			} else {
				let (batch_probe_rows, batch_output_rows) = self
					.visit_streaming_spilled_left_outer_probe_batch(
						spilled,
						&right_batch,
						&full_left_schema,
						retained_columns,
						matched_build_rows,
						join_key_plan,
						right_keys,
						null_safe_keys,
						join_output_schema,
						exchange,
						group_expr,
						aggr_expr,
						groups,
						group_types,
						folded_input,
					)
					.await?;
				probe_rows = probe_rows.saturating_add(batch_probe_rows);
				output_rows = output_rows.saturating_add(batch_output_rows);
			}
		}
		Ok((probe_rows, output_rows))
	}

	#[allow(clippy::too_many_arguments)]
	async fn visit_streaming_spilled_left_outer_probe_batch(
		&self,
		spilled: &mut SpilledInnerJoinBuild,
		right_batch: &RecordBatch,
		full_left_schema: &SchemaRef,
		retained_columns: &[ColumnOrdinal],
		matched_build_rows: &mut [Vec<u64>],
		join_key_plan: JoinKeyPlan,
		right_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		join_output_schema: &arrow::datatypes::SchemaRef,
		exchange: Option<(u32, &BoundExprProgram)>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BoundGroupedAggregateGroups,
		group_types: &mut Vec<DataType>,
		folded_input: &mut bool,
	) -> Result<(u64, u64), ExecutionError> {
		let mut output_rows = 0u64;
		for offset in (0..right_batch.num_rows()).step_by(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let len = CROSS_JOIN_OUTPUT_CHUNK_ROWS.min(right_batch.num_rows() - offset);
			let probe_chunk = right_batch.slice(offset, len);
			let right_key_arrays = key_arrays(&probe_chunk, right_keys)?;
			let mut matches = Vec::new();
			for row in 0..probe_chunk.num_rows() {
				let Some(key) = join_key_at(join_key_plan, &right_key_arrays, row, null_safe_keys)?
				else {
					continue;
				};
				for build in spilled.lookup(&key).await? {
					spilled.mark_page_row_matched(matched_build_rows, build);
					matches.push(SpilledInnerJoinMatch {
						build,
						probe: join_probe_row_index(row)?,
					});
				}
			}
			spilled.record_probe_chunk(probe_chunk.num_rows(), &matches);
			output_rows = output_rows.saturating_add(matches.len() as u64);
			self.fold_projected_spilled_left_outer_join_selection_chunks(
				spilled,
				&probe_chunk,
				&matches,
				full_left_schema,
				retained_columns,
				join_output_schema,
				exchange,
				group_expr,
				aggr_expr,
				groups,
				group_types,
				folded_input,
			)
			.await?;
		}
		Ok((right_batch.num_rows() as u64, output_rows))
	}

	#[allow(clippy::too_many_arguments)]
	async fn fold_streaming_spilled_left_outer_unmatched_rows(
		&self,
		spilled: &mut SpilledInnerJoinBuild,
		matched_build_rows: &[Vec<u64>],
		full_left_schema: SchemaRef,
		retained_columns: &[ColumnOrdinal],
		right_schema: arrow::datatypes::SchemaRef,
		join_output_schema: &arrow::datatypes::SchemaRef,
		exchange: Option<(u32, &BoundExprProgram)>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BoundGroupedAggregateGroups,
		group_types: &mut Vec<DataType>,
		folded_input: &mut bool,
	) -> Result<u64, ExecutionError> {
		let mut unmatched = Vec::new();
		let mut output_rows = 0u64;
		for page_index in 0..spilled.page_count() {
			let rows = spilled.page_row_count(page_index)?;
			for row in 0..rows {
				if spilled.page_row_matched(matched_build_rows, page_index, row) {
					continue;
				}
				unmatched.push(SpilledBatchRowRef {
					page_index,
					row: join_build_row_index(row)?,
				});
				if unmatched.len() == CROSS_JOIN_OUTPUT_CHUNK_ROWS {
					output_rows = output_rows.saturating_add(unmatched.len() as u64);
					let batch = materialize_projected_streamed_spilled_left_outer_unmatched(
						spilled,
						&unmatched,
						&full_left_schema,
						retained_columns,
						&right_schema,
						join_output_schema,
						self.context.task_budget.memory.get(),
					)
					.await?;
					let fold_rows = batch.num_rows();
					let fold_started = spilled.throughput_profile_enabled().then(Instant::now);
					self.fold_left_outer_grouped_batch(
						batch,
						exchange,
						group_expr,
						aggr_expr,
						groups,
						group_types,
						folded_input,
					)?;
					if let Some(fold_started) = fold_started {
						spilled.record_grouped_fold(fold_rows, fold_started.elapsed());
					}
					unmatched.clear();
				}
			}
		}
		if !unmatched.is_empty() {
			output_rows = output_rows.saturating_add(unmatched.len() as u64);
			let batch = materialize_projected_streamed_spilled_left_outer_unmatched(
				spilled,
				&unmatched,
				&full_left_schema,
				retained_columns,
				&right_schema,
				join_output_schema,
				self.context.task_budget.memory.get(),
			)
			.await?;
			let fold_rows = batch.num_rows();
			let fold_started = spilled.throughput_profile_enabled().then(Instant::now);
			self.fold_left_outer_grouped_batch(
				batch,
				exchange,
				group_expr,
				aggr_expr,
				groups,
				group_types,
				folded_input,
			)?;
			if let Some(fold_started) = fold_started {
				spilled.record_grouped_fold(fold_rows, fold_started.elapsed());
			}
		}
		Ok(output_rows)
	}

	#[allow(clippy::too_many_arguments)]
	async fn fold_projected_spilled_left_outer_join_selection_chunks(
		&self,
		spilled: &mut SpilledInnerJoinBuild,
		right_batch: &RecordBatch,
		matches: &[SpilledInnerJoinMatch],
		full_left_schema: &SchemaRef,
		retained_columns: &[ColumnOrdinal],
		join_output_schema: &arrow::datatypes::SchemaRef,
		exchange: Option<(u32, &BoundExprProgram)>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BoundGroupedAggregateGroups,
		group_types: &mut Vec<DataType>,
		folded_input: &mut bool,
	) -> Result<(), ExecutionError> {
		for chunk in matches.chunks(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let batch = materialize_projected_streamed_spilled_left_outer_matches(
				spilled,
				right_batch,
				chunk,
				full_left_schema,
				retained_columns,
				join_output_schema,
				self.context.task_budget.memory.get(),
			)
			.await?;
			let fold_rows = batch.num_rows();
			let fold_started = spilled.throughput_profile_enabled().then(Instant::now);
			self.fold_left_outer_grouped_batch(
				batch,
				exchange,
				group_expr,
				aggr_expr,
				groups,
				group_types,
				folded_input,
			)?;
			if let Some(fold_started) = fold_started {
				spilled.record_grouped_fold(fold_rows, fold_started.elapsed());
			}
		}
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	async fn visit_streaming_left_outer_scan_probe_chunks(
		&self,
		scan_input: StreamingScanProbeInput<'_>,
		left_batch: &RecordBatch,
		build_rows: &JoinBuildRows,
		matched_build_rows: &mut [u64],
		join_key_plan: JoinKeyPlan,
		right_keys: &[crate::ir::ColumnOrdinal],
		match_filter: Option<&BoundExprProgram>,
		null_safe_keys: bool,
		join_output_schema: &arrow::datatypes::SchemaRef,
		exchange: Option<(u32, &BoundExprProgram)>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BoundGroupedAggregateGroups,
		group_types: &mut Vec<DataType>,
		folded_input: &mut bool,
		morsel: Option<(MorselId, u32)>,
	) -> Result<(u64, u64), ExecutionError> {
		let Some(req) = self.bound_scan_request(scan_input.scan, morsel) else {
			return Err(ExecutionError::InvalidExpression {
				message: "streaming left outer probe expected a scan input".to_string(),
			});
		};
		let mut remaining = req.row_limit;
		let span = crate::telemetry::storage_scan_span(&req);
		let mut stream = self
			.storage
			.scan(req)
			.instrument(span)
			.await?
			.into_batches();
		let mut probe_rows = 0u64;
		let mut output_rows = 0u64;
		while let Some(batch) = stream.next().await {
			let Some(mut right_batch) = limit_streaming_scan_batch(batch?, &mut remaining) else {
				break;
			};
			if let Some(predicate) = scan_input.filter_predicate {
				right_batch = self.filter_batch_with_bound_predicate(
					predicate,
					right_batch,
					"filter predicate",
				)?;
				if right_batch.num_rows() == 0 {
					continue;
				}
			}
			if let Some(scan_exchange) = scan_input.exchange {
				for (_partition, partitioned) in self.repartition_exchange_batch(
					&right_batch,
					scan_exchange.partition_count,
					scan_exchange.key,
				)? {
					let (batch_probe_rows, batch_output_rows) = self
						.visit_streaming_left_outer_probe_batch(
							left_batch,
							&partitioned,
							build_rows,
							matched_build_rows,
							join_key_plan,
							right_keys,
							match_filter,
							null_safe_keys,
							join_output_schema,
							exchange,
							group_expr,
							aggr_expr,
							groups,
							group_types,
							folded_input,
						)?;
					probe_rows = probe_rows.saturating_add(batch_probe_rows);
					output_rows = output_rows.saturating_add(batch_output_rows);
				}
			} else {
				let (batch_probe_rows, batch_output_rows) = self
					.visit_streaming_left_outer_probe_batch(
						left_batch,
						&right_batch,
						build_rows,
						matched_build_rows,
						join_key_plan,
						right_keys,
						match_filter,
						null_safe_keys,
						join_output_schema,
						exchange,
						group_expr,
						aggr_expr,
						groups,
						group_types,
						folded_input,
					)?;
				probe_rows = probe_rows.saturating_add(batch_probe_rows);
				output_rows = output_rows.saturating_add(batch_output_rows);
			}
		}
		Ok((probe_rows, output_rows))
	}

	#[allow(clippy::too_many_arguments)]
	fn visit_streaming_left_outer_probe_batch(
		&self,
		left_batch: &RecordBatch,
		right_batch: &RecordBatch,
		build_rows: &JoinBuildRows,
		matched_build_rows: &mut [u64],
		join_key_plan: JoinKeyPlan,
		right_keys: &[crate::ir::ColumnOrdinal],
		match_filter: Option<&BoundExprProgram>,
		null_safe_keys: bool,
		join_output_schema: &arrow::datatypes::SchemaRef,
		exchange: Option<(u32, &BoundExprProgram)>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BoundGroupedAggregateGroups,
		group_types: &mut Vec<DataType>,
		folded_input: &mut bool,
	) -> Result<(u64, u64), ExecutionError> {
		let mut output_rows = 0u64;
		for offset in (0..right_batch.num_rows()).step_by(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let len = CROSS_JOIN_OUTPUT_CHUNK_ROWS.min(right_batch.num_rows() - offset);
			let probe_chunk = right_batch.slice(offset, len);
			let right_key_arrays = key_arrays(&probe_chunk, right_keys)?;
			let mut selection = Vec::new();
			let mut residual = JoinMatchResidual::new(match_filter, left_batch, &probe_chunk);
			match join_key_plan {
				JoinKeyPlan::Int32 => {
					let values = single_join_key_array::<Int32Array>(&right_key_arrays, "Int32")?;
					for row in 0..probe_chunk.num_rows() {
						if values.is_null(row) {
							continue;
						}
						if let Some(matches) = build_rows.get_int32(values.value(row)) {
							append_join_matches_for_kind(
								JoinKind::LeftOuter,
								row,
								matches.rows(),
								Some(matched_build_rows),
								&mut selection,
								&mut residual,
							)?;
						}
					}
				}
				JoinKeyPlan::Int64 => {
					let values = single_join_key_array::<Int64Array>(&right_key_arrays, "Int64")?;
					for row in 0..probe_chunk.num_rows() {
						if values.is_null(row) {
							continue;
						}
						if let Some(matches) = build_rows.get_int64(values.value(row)) {
							append_join_matches_for_kind(
								JoinKind::LeftOuter,
								row,
								matches.rows(),
								Some(matched_build_rows),
								&mut selection,
								&mut residual,
							)?;
						}
					}
				}
				JoinKeyPlan::Generic => {
					for row in 0..probe_chunk.num_rows() {
						let key = if null_safe_keys {
							join_key_bytes_null_safe(&right_key_arrays, row)?
						} else {
							let Some(key) = join_key_bytes(&right_key_arrays, row)? else {
								continue;
							};
							key
						};
						if let Some(matches) = build_rows.get(&JoinKey::Generic(key)) {
							append_join_matches_for_kind(
								JoinKind::LeftOuter,
								row,
								matches.rows(),
								Some(matched_build_rows),
								&mut selection,
								&mut residual,
							)?;
						}
					}
				}
			}
			output_rows = output_rows.saturating_add(selection.len() as u64);
			self.fold_left_outer_join_selection_chunks(
				left_batch,
				&probe_chunk,
				&selection,
				join_output_schema,
				exchange,
				group_expr,
				aggr_expr,
				groups,
				group_types,
				folded_input,
			)?;
		}
		Ok((right_batch.num_rows() as u64, output_rows))
	}

	#[allow(clippy::too_many_arguments)]
	fn fold_streaming_left_outer_unmatched_rows(
		&self,
		left_batch: &RecordBatch,
		matched_build_rows: &[u64],
		right_schema: arrow::datatypes::SchemaRef,
		join_output_schema: &arrow::datatypes::SchemaRef,
		exchange: Option<(u32, &BoundExprProgram)>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BoundGroupedAggregateGroups,
		group_types: &mut Vec<DataType>,
		folded_input: &mut bool,
	) -> Result<u64, ExecutionError> {
		let empty_right = RecordBatch::new_empty(right_schema);
		let mut selection = Vec::new();
		let mut output_rows = 0u64;
		for row in 0..left_batch.num_rows() {
			if build_row_matched(matched_build_rows, row) {
				continue;
			}
			selection.push(JoinOutputRow::LeftFillNull {
				left: join_build_row_index(row)?,
			});
			if selection.len() == CROSS_JOIN_OUTPUT_CHUNK_ROWS {
				output_rows = output_rows.saturating_add(selection.len() as u64);
				self.fold_left_outer_join_selection_chunks(
					left_batch,
					&empty_right,
					&selection,
					join_output_schema,
					exchange,
					group_expr,
					aggr_expr,
					groups,
					group_types,
					folded_input,
				)?;
				selection.clear();
			}
		}
		if !selection.is_empty() {
			output_rows = output_rows.saturating_add(selection.len() as u64);
			self.fold_left_outer_join_selection_chunks(
				left_batch,
				&empty_right,
				&selection,
				join_output_schema,
				exchange,
				group_expr,
				aggr_expr,
				groups,
				group_types,
				folded_input,
			)?;
		}
		Ok(output_rows)
	}

	#[allow(clippy::too_many_arguments)]
	fn fold_left_outer_join_selection_chunks(
		&self,
		left_batch: &RecordBatch,
		right_batch: &RecordBatch,
		selection: &[JoinOutputRow],
		join_output_schema: &arrow::datatypes::SchemaRef,
		exchange: Option<(u32, &BoundExprProgram)>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BoundGroupedAggregateGroups,
		group_types: &mut Vec<DataType>,
		folded_input: &mut bool,
	) -> Result<(), ExecutionError> {
		for chunk in selection.chunks(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let batch = materialize_join_selection(
				JoinKind::LeftOuter,
				left_batch,
				right_batch,
				chunk,
				join_output_schema,
				self.context.task_budget.memory.get(),
			)?;
			self.fold_left_outer_grouped_batch(
				batch,
				exchange,
				group_expr,
				aggr_expr,
				groups,
				group_types,
				folded_input,
			)?;
		}
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	fn fold_left_outer_grouped_batch(
		&self,
		batch: RecordBatch,
		exchange: Option<(u32, &BoundExprProgram)>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BoundGroupedAggregateGroups,
		group_types: &mut Vec<DataType>,
		folded_input: &mut bool,
	) -> Result<(), ExecutionError> {
		if batch.num_rows() == 0 {
			if !*folded_input {
				self.fold_bound_grouped_aggregate_batch(
					groups,
					group_types,
					group_expr,
					aggr_expr,
					&batch,
				)?;
				*folded_input = true;
			}
			return Ok(());
		}
		if let Some((partition_count, exchange_key)) = exchange {
			for (_partition, partitioned) in
				self.repartition_exchange_batch(&batch, partition_count, exchange_key)?
			{
				if partitioned.num_rows() == 0 {
					continue;
				}
				self.fold_bound_grouped_aggregate_batch(
					groups,
					group_types,
					group_expr,
					aggr_expr,
					&partitioned,
				)?;
				*folded_input = true;
			}
		} else {
			self.fold_bound_grouped_aggregate_batch(
				groups,
				group_types,
				group_expr,
				aggr_expr,
				&batch,
			)?;
			*folded_input = true;
		}
		Ok(())
	}

	pub(in crate::exec) async fn try_streaming_collapsing_global_aggregate(
		&self,
		input: &ExecutionPlan,
		aggr_expr: &[BoundAggregateCall],
		output_schema: &arrow::datatypes::SchemaRef,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
		let (join, join_filter_predicate, project, project_filter_predicate) = match input {
			ExecutionPlan::Join {
				kind: JoinKind::Inner,
				..
			} => (input, None, None, None),
			ExecutionPlan::Filter {
				input,
				predicate,
				disposition,
			} => match input.as_ref() {
				ExecutionPlan::Join {
					kind: JoinKind::Inner,
					..
				} => {
					let filter_predicate = match disposition {
						FilterDisposition::FullyPushed => None,
						FilterDisposition::EvaluateInToron => Some(predicate),
					};
					(input.as_ref(), filter_predicate, None, None)
				}
				ExecutionPlan::Project {
					input,
					exprs,
					output_schema,
				} => {
					let Some((join, join_filter_predicate)) =
						decompose_join_or_filtered_join(input.as_ref())
					else {
						return Ok(None);
					};
					let project_filter_predicate = match disposition {
						FilterDisposition::FullyPushed => None,
						FilterDisposition::EvaluateInToron => Some(predicate),
					};
					(
						join,
						join_filter_predicate,
						Some((exprs, output_schema)),
						project_filter_predicate,
					)
				}
				_ => return Ok(None),
			},
			ExecutionPlan::Project {
				input,
				exprs,
				output_schema,
			} => {
				let Some((join, filter_predicate)) =
					decompose_join_or_filtered_join(input.as_ref())
				else {
					return Ok(None);
				};
				(join, filter_predicate, Some((exprs, output_schema)), None)
			}
			_ => return Ok(None),
		};

		let ExecutionPlan::Join {
			replay_id,
			left,
			right,
			kind,
			left_keys,
			right_keys,
			match_filter,
			null_safe_keys,
			strategy,
			output_schema: join_output_schema,
			..
		} = join
		else {
			return Ok(None);
		};

		let mut state = init_global_aggregate_state(aggr_expr)?;
		let profile = if crate::exec::aggregate::global_fold_join_filter_profile_eligible(
			join_filter_predicate,
			project,
			project_filter_predicate,
			aggr_expr,
		) {
			GlobalFoldJoinFilterProfile::from_env(
				join_filter_predicate.expect("eligible profile requires a join filter"),
			)?
		} else {
			None
		};
		let profile_ref = profile.as_deref();
		let root_started = profile_ref
			.map(GlobalFoldJoinFilterProfile::begin_root)
			.transpose()?;
		// Arm the phase ledger for this fused inner-join global aggregate (no-op unless the env path
		// is set). The leaf scan-probe/materialize/fold sites accumulate into it; the guard disarms
		// and persists it on every exit (writing drive_completed=false on any path that does not
		// reach the success finish below).
		let _phase_guard = local_join_phase_begin();
		// ADR 438: the profiled visitor carries several timer-bearing join futures. Keep this parent
		// aggregate future pointer-sized so deep reoriented plan chains do not stack every child state
		// machine inline (the same bounded-future discipline as the boxed profile state itself).
		let visited = Box::pin(self.try_visit_streaming_join0_chunks(
			join,
			join_filter_predicate,
			morsel,
			profile_ref,
			|batch| {
				let batch = if let Some((exprs, project_schema)) = project {
					let cols = self.evaluate_bound_program(exprs, &batch)?;
					RecordBatch::try_new(project_schema.clone(), cols)?
				} else {
					batch
				};
				let batch = if let Some(predicate) = project_filter_predicate {
					self.filter_batch_with_bound_predicate(predicate, batch, "filter predicate")?
				} else {
					batch
				};
				if batch.num_rows() == 0 {
					Ok(())
				} else if let Some(profile) = profile_ref {
					self.fold_global_decimal_sum_state_profiled(
						&mut state, aggr_expr, &batch, profile,
					)
				} else {
					self.fold_global_aggregate_state(&mut state, aggr_expr, &batch)
				}
			},
		))
		.await;
		if let (Some(profile), Some(started)) = (profile_ref, root_started) {
			profile.finish_root(started)?;
		}
		let visited = visited?;
		if visited.is_some() {
			let output = finalize_global_aggregate_state(&state, aggr_expr, output_schema)?;
			if let Some(profile) = profile_ref {
				profile.persist()?;
			}
			local_join_phase_finish(true)?;
			return Ok(Some(output));
		}
		// Every non-success exit (declined dispatch, error via `?`, or panic) is handled by
		// `_phase_guard`, which writes drive_completed=false — so a partial attribution is never
		// mistaken for a full one, and the generic fallback below never inherits a stale ledger.
		if profile.is_some() {
			return Err(ExecutionError::Kernel {
				message:
					"global fold join/filter profile did not reach its confirmed fused dispatch"
						.to_string(),
			});
		}
		if project.is_some() {
			return Ok(None);
		}

		let left_batches = self.execute_bound(left, morsel).await?;
		let right_batches = self.execute_bound(right, morsel).await?;
		execute_hash_join_into_global_aggregate(
			self,
			left.schema(),
			right.schema(),
			&left_batches,
			&right_batches,
			*kind,
			left_keys,
			right_keys,
			match_filter.as_ref(),
			*null_safe_keys,
			*replay_id,
			*strategy,
			join_output_schema,
			join_filter_predicate,
			aggr_expr,
			&mut state,
			self.context.task_budget,
			self.context.task_telemetry.clone(),
			self.context.join_runtime_observation_mode.clone(),
			self.context.join_spill_file_ops.clone(),
			self.context.join_spill_dir.clone(),
		)
		.await?;
		finalize_global_aggregate_state(&state, aggr_expr, output_schema).map(Some)
	}

	#[allow(clippy::too_many_arguments)]
	async fn try_streaming_scan_left_semi_grouped_aggregate(
		&self,
		input: &ExecutionPlan,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		output_schema: &arrow::datatypes::SchemaRef,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
		let ExecutionPlan::Exchange {
			input: aggregate_input,
			partition_count,
			key: exchange_key,
		} = input
		else {
			return Ok(None);
		};
		if exchange_key != group_expr && !grouped_aggregate_is_order_insensitive(aggr_expr) {
			return Ok(None);
		}
		let ExecutionPlan::Filter {
			input: semi_input,
			predicate: post_filter,
			disposition: FilterDisposition::EvaluateInToron,
		} = aggregate_input.as_ref()
		else {
			return Ok(None);
		};
		let semi_input = semi_input.as_ref();
		let ExecutionPlan::Join {
			replay_id,
			left,
			right,
			kind: JoinKind::LeftSemi,
			left_keys,
			right_keys,
			match_filter,
			null_safe_keys: false,
			strategy,
			output_schema: semi_output_schema,
			..
		} = semi_input
		else {
			return Ok(None);
		};
		let Some(scan_input) = decompose_streaming_scan_probe_input(left) else {
			return Ok(None);
		};
		if decompose_streaming_existence_plan(right).is_none() {
			return Ok(None);
		}

		let stage = StreamingSemiAntiStage {
			replay_id: *replay_id,
			kind: JoinKind::LeftSemi,
			preserved_keys: left_keys.clone(),
			existence: right.as_ref(),
			existence_keys: right_keys.clone(),
			match_filter: match_filter.as_ref(),
			output_schema: semi_output_schema.clone(),
			strategy: *strategy,
		};
		let Some(mut runtime) = self
			.prepare_streaming_semi_anti_runtime(stage, morsel)
			.await?
		else {
			return Ok(None);
		};

		let output = async {
			let Some(req) = self.bound_scan_request(scan_input.scan, morsel) else {
				return Ok(None);
			};
			let mut remaining = req.row_limit;
			let span = crate::telemetry::storage_scan_span(&req);
			let mut stream = self
				.storage
				.scan(req)
				.instrument(span)
				.await?
				.into_batches();
			let mut groups = BoundGroupedAggregateGroups::new();
			let mut group_types = Vec::new();
			while let Some(batch) = stream.next().await {
				let Some(mut batch) = limit_streaming_scan_batch(batch?, &mut remaining) else {
					break;
				};
				if let Some(predicate) = scan_input.filter_predicate {
					batch = self.filter_batch_with_bound_predicate(
						predicate,
						batch,
						"filter predicate",
					)?;
					if batch.num_rows() == 0 {
						continue;
					}
				}
				batch =
					self.filter_batch_with_bound_predicate(post_filter, batch, "filter predicate")?;
				if batch.num_rows() == 0 {
					continue;
				}
				let mut semi_batches = self.probe_streaming_semi_anti(&mut runtime, &batch).await?;
				for batch in semi_batches.drain(..) {
					if batch.num_rows() == 0 {
						continue;
					}
					for (_partition, partitioned) in
						self.repartition_exchange_batch(&batch, *partition_count, exchange_key)?
					{
						if partitioned.num_rows() == 0 {
							continue;
						}
						self.fold_bound_grouped_aggregate_batch(
							&mut groups,
							&mut group_types,
							group_expr,
							aggr_expr,
							&partitioned,
						)?;
					}
				}
			}
			record_join_runtime_observation(
				&self.context.task_telemetry,
				&self.context.join_runtime_observation_mode,
				JoinRuntimeObservation {
					replay_id: *replay_id,
					strategy: *strategy,
					build_rows: RowCount::new(runtime.existence_row_count),
					probe_rows: RowCount::new(runtime.probe_rows),
					output_rows: RowCount::new(runtime.output_rows),
				},
			);
			self.finalize_bound_grouped_aggregate(
				&mut groups,
				&group_types,
				aggr_expr,
				output_schema,
			)
			.map(Some)
		}
		.await;
		runtime.cleanup().await;
		output
	}

	#[allow(clippy::too_many_arguments)]
	pub(in crate::exec) async fn try_streaming_projected_left_semi_grouped_aggregate(
		&self,
		input: &ExecutionPlan,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		output_schema: &arrow::datatypes::SchemaRef,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
		if let Some(output) = self
			.try_streaming_scan_left_semi_grouped_aggregate(
				input,
				group_expr,
				aggr_expr,
				output_schema,
				morsel,
			)
			.await?
		{
			return Ok(Some(output));
		}
		let ExecutionPlan::Exchange {
			input: aggregate_input,
			partition_count,
			key: exchange_key,
		} = input
		else {
			return Ok(None);
		};
		if exchange_key != group_expr && !grouped_aggregate_is_order_insensitive(aggr_expr) {
			return Ok(None);
		}
		let ExecutionPlan::Join {
			replay_id,
			left,
			right,
			kind: JoinKind::LeftSemi,
			left_keys,
			right_keys,
			match_filter: None,
			null_safe_keys,
			strategy,
			output_schema: semi_output_schema,
			..
		} = aggregate_input.as_ref()
		else {
			return Ok(None);
		};

		let retained_columns =
			grouped_aggregate_input_columns(group_expr, aggr_expr, exchange_key, left_keys);
		if retained_columns.is_empty()
			|| retained_columns.len() >= semi_output_schema.fields().len()
		{
			return Ok(None);
		}
		let chain_input = exchange_input(left);
		let has_reoriented_chain_top = matches!(
			chain_input,
			Some(ExecutionPlan::Join { replay_id, .. })
				if replay_id.0 > 0 && *replay_id != JoinReplayId::SYNTHETIC
		);
		let chain_candidate = chain_input.and_then(|chain_input| {
			let (base, stages) = extract_reoriented_inner_chain(chain_input)?;
			(stages.len() == 1
				&& stages[0].replay_id == JoinReplayId(1)
				&& self.reoriented_terminal_scan_build_estimated_over_cap(&stages[0], morsel))
			.then_some((base, stages))
		});
		if has_reoriented_chain_top && chain_candidate.is_none() {
			return Ok(None);
		}
		if let Some((base, mut stages)) = chain_candidate {
			if *null_safe_keys || left_keys.len() != 1 || right_keys.len() != 1 {
				return Ok(None);
			}
			let Some(left_field) = semi_output_schema.fields().get(left_keys[0].0 as usize) else {
				return Ok(None);
			};
			let right_schema = right.schema();
			let Some(right_field) = right_schema.fields().get(right_keys[0].0 as usize) else {
				return Ok(None);
			};
			if left_field.data_type() != &DataType::Int64
				|| right_field.data_type() != &DataType::Int64
				|| left_field.is_nullable()
				|| right_field.is_nullable()
			{
				return Ok(None);
			}

			let required_output = retained_columns.iter().copied().collect();
			mark_reoriented_inner_stage_projections(&mut stages, required_output)?;
			let stage = stages.pop().expect("one-stage chain checked");
			let membership_profile = MembershipChainProfile::from_env()?;
			let profile = membership_profile.as_deref();
			let profile_root = profile
				.map(MembershipChainProfile::begin_root)
				.transpose()?;
			let mut receipt = MembershipScheduleReceipt::from_env();
			if let Some(receipt) = &mut receipt {
				receipt.append(
					"selected/committed",
					serde_json::json!({"outer_writer_constructed": false}),
				);
				receipt.append("membership_execute_started", serde_json::json!({}));
			}
			let membership_started = Instant::now();
			let membership_timer = profile
				.map(|profile| {
					profile.begin_leaf(
						MembershipChainPhase::Membership,
						MembershipChainOperation::SubtreeExecute,
					)
				})
				.transpose()?;
			let right_batches = self.execute_bound(right, morsel).await?;
			let membership_rows = right_batches
				.iter()
				.map(|batch| batch.num_rows() as u64)
				.fold(0u64, u64::saturating_add);
			let membership_batch_bytes = right_batches
				.iter()
				.map(record_batch_memory_bytes)
				.fold(0u64, u64::saturating_add);
			if let Some(timer) = membership_timer {
				timer.finish_with(membership_rows, membership_batch_bytes)?;
			}
			if let Some(receipt) = &mut receipt {
				receipt.append(
					"membership_execute_completed",
					serde_json::json!({
						"rows": membership_rows,
						"batch_bytes": membership_batch_bytes,
					}),
				);
			}
			let presence_timer = profile
				.map(|profile| {
					profile.begin_leaf(
						MembershipChainPhase::Membership,
						MembershipChainOperation::PresenceSetBuild,
					)
				})
				.transpose()?;
			let presence_result = Int64PresenceSet::from_batches(
				&right_batches,
				right_keys[0],
				self.context.task_budget.memory.get(),
			)?;
			if let Some(timer) = presence_timer {
				timer.finish_with(
					presence_result
						.as_ref()
						.map_or(0, |presence| presence.entries.len() as u64),
					presence_result
						.as_ref()
						.map_or(0, Int64PresenceSet::accounted_bytes),
				)?;
			}
			let Some(presence) = presence_result else {
				let mut groups = BoundGroupedAggregateGroups::new();
				let mut group_types = Vec::new();
				let empty = RecordBatch::new_empty(semi_output_schema.clone());
				self.fold_bound_grouped_aggregate_batch(
					&mut groups,
					&mut group_types,
					group_expr,
					aggr_expr,
					&empty,
				)?;
				if let Some(receipt) = &mut receipt {
					receipt.append("empty_membership_finalized", serde_json::json!({}));
				}
				record_join_runtime_observation(
					&self.context.task_telemetry,
					&self.context.join_runtime_observation_mode,
					JoinRuntimeObservation {
						replay_id: *replay_id,
						strategy: *strategy,
						build_rows: RowCount::new(0),
						probe_rows: RowCount::new(0),
						output_rows: RowCount::new(0),
					},
				);
				return self
					.finalize_bound_grouped_aggregate(
						&mut groups,
						&group_types,
						aggr_expr,
						output_schema,
					)
					.map(Some);
			};
			let presence_bytes = presence.accounted_bytes();
			if let Some(profile) = profile {
				profile.record_presence_entries(presence.entries.len())?;
			}
			let membership_micros = duration_micros(membership_started);
			if let Some(receipt) = &mut receipt {
				receipt.append(
					"presence_set_completed",
					serde_json::json!({
						"entries": presence.entries.len(),
						"accounted_bytes": presence_bytes,
						"construction_peak_bytes": presence.construction_peak_bytes,
					}),
				);
			}
			drop(right_batches);

			let chain_started = Instant::now();
			if let Some(receipt) = &mut receipt {
				receipt.append("chain_runtime_prepare_started", serde_json::json!({}));
			}
			let prepare_timer = profile
				.map(|profile| {
					profile.begin_leaf(
						MembershipChainPhase::Lifecycle,
						MembershipChainOperation::RuntimePrepare,
					)
				})
				.transpose()?;
			let prepared_runtime = self
				.prepare_reoriented_inner_join_runtime_with_streamed_scan_build_baseline(
					stage.clone(),
					morsel,
					presence_bytes,
				)
				.await?;
			if let Some(timer) = prepare_timer {
				timer.finish()?;
			}
			let Some(mut runtime) = prepared_runtime else {
				return Err(ExecutionError::InvalidExpression {
					message: "committed membership-first chain lost its terminal scan stage"
						.to_string(),
				});
			};
			let mut groups = BoundGroupedAggregateGroups::new();
			let mut group_types = Vec::new();
			let mut chain_input_rows = 0u64;
			let mut matched_rows = 0u64;
			if let Some(receipt) = &mut receipt {
				receipt.append("chain_drive_started", serde_json::json!({}));
			}
			let mut sink = ReorientedChainOutputSink::MembershipFilteredGroupedFold {
				presence: &presence,
				left_key: left_keys[0],
				partition_count: *partition_count,
				exchange_key,
				group_expr,
				aggr_expr,
				groups: &mut groups,
				group_types: &mut group_types,
				input_rows: &mut chain_input_rows,
				output_rows: &mut matched_rows,
				profile,
			};
			let drive_result = self
				.try_streaming_reoriented_inner_chain_scan_probe_base_to_sink(
					base,
					&stage,
					&mut runtime,
					&mut sink,
					morsel,
					presence_bytes,
					profile,
				)
				.await;
			drop(sink);
			if drive_result.as_ref().is_ok_and(|matched| matched.is_some()) {
				if let Some(receipt) = &mut receipt {
					receipt.append(
						"chain_drive_completed",
						serde_json::json!({
							"input_rows": chain_input_rows,
							"matched_rows": matched_rows,
						}),
					);
				}
			}
			record_join_runtime_observation(
				&self.context.task_telemetry,
				&self.context.join_runtime_observation_mode,
				JoinRuntimeObservation {
					replay_id: runtime.replay_id,
					strategy: runtime.strategy,
					build_rows: RowCount::new(runtime.build_row_count()),
					probe_rows: RowCount::new(runtime.probe_rows),
					output_rows: RowCount::new(runtime.output_rows),
				},
			);
			let cleanup_timer = profile
				.map(|profile| {
					profile.begin_leaf(
						MembershipChainPhase::Lifecycle,
						MembershipChainOperation::Cleanup,
					)
				})
				.transpose()?;
			runtime.cleanup().await;
			if let Some(timer) = cleanup_timer {
				timer.finish()?;
			}
			match drive_result {
				Ok(Some(())) => {}
				Ok(None) => {
					return Err(ExecutionError::InvalidExpression {
						message:
							"committed membership-first chain could not stream its terminal stage"
								.to_string(),
					});
				}
				Err(error) => return Err(error),
			}
			record_task_telemetry(&self.context.task_telemetry, |task| {
				task.phases.probe_micros =
					task.phases.probe_micros.saturating_add(membership_micros);
				task.phases.build_micros = task
					.phases
					.build_micros
					.saturating_add(duration_micros(chain_started));
				task.counters.build_rows =
					task.counters.build_rows.saturating_add(chain_input_rows);
				task.counters.probe_rows = task.counters.probe_rows.saturating_add(membership_rows);
				task.counters.matched_rows =
					task.counters.matched_rows.saturating_add(matched_rows);
			});
			record_join_runtime_observation(
				&self.context.task_telemetry,
				&self.context.join_runtime_observation_mode,
				JoinRuntimeObservation {
					replay_id: *replay_id,
					strategy: *strategy,
					build_rows: RowCount::new(chain_input_rows),
					probe_rows: RowCount::new(membership_rows),
					output_rows: RowCount::new(matched_rows),
				},
			);
			let finalize_timer = profile
				.map(|profile| {
					profile.begin_leaf(
						MembershipChainPhase::Lifecycle,
						MembershipChainOperation::Finalize,
					)
				})
				.transpose()?;
			let output = self.finalize_bound_grouped_aggregate(
				&mut groups,
				&group_types,
				aggr_expr,
				output_schema,
			);
			if let Some(timer) = finalize_timer {
				timer.finish()?;
			}
			if let (Ok(output_batches), Some(profile), Some(root)) =
				(&output, profile, profile_root)
			{
				let root_rows = output_batches
					.iter()
					.try_fold(0usize, |rows, batch| rows.checked_add(batch.num_rows()))
					.ok_or_else(|| ExecutionError::Kernel {
						message: "membership-chain root row count overflow".to_string(),
					})?;
				profile.record_root_aggregate_rows(root_rows)?;
				profile.finish_root(root)?;
				profile.persist()?;
			}
			if output.is_ok()
				&& let Some(receipt) = &mut receipt
			{
				receipt.append("finalized", serde_json::json!({}));
			}
			return output.map(Some);
		}

		let projected_left_keys = remap_column_ordinals(&retained_columns, left_keys)?;
		let projected_schema = projected_schema_for_columns(semi_output_schema, &retained_columns)?;
		let key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&projected_schema,
			&projected_left_keys,
			&right.schema(),
			right_keys,
			*null_safe_keys,
		)?;
		let left_batches = self.execute_bound(left, morsel).await?;
		let estimated_build_bytes =
			estimated_build_hash_bytes_for_batches(&left_batches, left_keys)?;
		if estimated_build_bytes <= self.context.task_budget.memory.get() {
			return Ok(None);
		}
		let right_batches = self.execute_bound(right, morsel).await?;
		record_membership_outer_writer_construct();
		let mut spilled = SpilledInnerJoinBuild::create(
			self.context.task_budget,
			self.context.join_spill_file_ops.clone(),
			self.context.join_spill_dir.clone(),
		)
		.await?;
		let build_started = Instant::now();
		let build_result = async {
			for batch in &left_batches {
				spilled
					.write_projected_batch_chunks(
						batch,
						&retained_columns,
						key_plan,
						&projected_left_keys,
						*null_safe_keys,
						self.context.task_budget,
					)
					.await?;
			}
			spilled.finish(self.context.task_budget).await
		}
		.await;
		if let Err(error) = build_result {
			spilled.cleanup().await;
			return Err(error);
		}
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.phases.build_micros = task
				.phases
				.build_micros
				.saturating_add(duration_micros(build_started));
			task.phases.spill_micros = task
				.phases
				.spill_micros
				.saturating_add(duration_micros(build_started));
			task.counters.build_rows = task
				.counters
				.build_rows
				.saturating_add(spilled.input_row_count());
			task.counters.spill_bytes = task
				.counters
				.spill_bytes
				.saturating_add(spilled.spill_bytes());
		});
		let mut groups = BoundGroupedAggregateGroups::new();
		let mut group_types = Vec::new();
		let probe_started = Instant::now();
		let result = self
			.fold_projected_streamed_left_semi_grouped_aggregate(
				&mut spilled,
				&right_batches,
				right_keys,
				key_plan,
				*null_safe_keys,
				semi_output_schema,
				&retained_columns,
				*partition_count,
				exchange_key,
				group_expr,
				aggr_expr,
				&mut groups,
				&mut group_types,
			)
			.await;
		let build_rows = spilled.input_row_count();
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.phases.probe_micros = task
				.phases
				.probe_micros
				.saturating_add(duration_micros(probe_started));
			if let Ok((probe_rows, output_rows)) = &result {
				task.counters.probe_rows = task.counters.probe_rows.saturating_add(*probe_rows);
				task.counters.matched_rows =
					task.counters.matched_rows.saturating_add(*output_rows);
			}
		});
		let output = result.and_then(|(probe_rows, output_rows)| {
			record_join_runtime_observation(
				&self.context.task_telemetry,
				&self.context.join_runtime_observation_mode,
				JoinRuntimeObservation {
					replay_id: *replay_id,
					strategy: *strategy,
					build_rows: RowCount::new(build_rows),
					probe_rows: RowCount::new(probe_rows),
					output_rows: RowCount::new(output_rows),
				},
			);
			self.finalize_bound_grouped_aggregate(
				&mut groups,
				&group_types,
				aggr_expr,
				output_schema,
			)
		});
		spilled.cleanup().await;
		output.map(Some)
	}

	#[allow(clippy::too_many_arguments)]
	pub(in crate::exec) async fn fold_projected_streamed_left_semi_grouped_aggregate(
		&self,
		spilled: &mut SpilledInnerJoinBuild,
		right_batches: &[RecordBatch],
		right_keys: &[crate::ir::ColumnOrdinal],
		key_plan: JoinKeyPlan,
		null_safe_keys: bool,
		semi_output_schema: &arrow::datatypes::SchemaRef,
		retained_columns: &[ColumnOrdinal],
		partition_count: u32,
		exchange_key: &BoundExprProgram,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BoundGroupedAggregateGroups,
		group_types: &mut Vec<DataType>,
	) -> Result<(u64, u64), ExecutionError> {
		let mut matched: BTreeMap<usize, BTreeSet<u32>> = BTreeMap::new();
		let mut probe_rows = 0u64;
		for batch in right_batches {
			for offset in (0..batch.num_rows()).step_by(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
				let len = CROSS_JOIN_OUTPUT_CHUNK_ROWS.min(batch.num_rows() - offset);
				let probe_chunk = batch.slice(offset, len);
				probe_rows = probe_rows.saturating_add(probe_chunk.num_rows() as u64);
				let right_key_arrays = key_arrays(&probe_chunk, right_keys)?;
				for row in 0..probe_chunk.num_rows() {
					let Some(key) = join_key_at(key_plan, &right_key_arrays, row, null_safe_keys)?
					else {
						continue;
					};
					for build in spilled.lookup(&key).await? {
						matched
							.entry(build.page_index)
							.or_default()
							.insert(build.row);
					}
				}
			}
		}

		let mut refs = Vec::new();
		let mut output_rows = 0u64;
		for (page_index, rows) in matched {
			for row in rows {
				refs.push(SpilledBatchRowRef { page_index, row });
				if refs.len() == CROSS_JOIN_OUTPUT_CHUNK_ROWS {
					self.fold_projected_left_semi_refs(
						spilled,
						&refs,
						semi_output_schema,
						retained_columns,
						partition_count,
						exchange_key,
						group_expr,
						aggr_expr,
						groups,
						group_types,
					)
					.await?;
					output_rows = output_rows.saturating_add(refs.len() as u64);
					refs.clear();
				}
			}
		}
		if !refs.is_empty() {
			self.fold_projected_left_semi_refs(
				spilled,
				&refs,
				semi_output_schema,
				retained_columns,
				partition_count,
				exchange_key,
				group_expr,
				aggr_expr,
				groups,
				group_types,
			)
			.await?;
			output_rows = output_rows.saturating_add(refs.len() as u64);
		}
		Ok((probe_rows, output_rows))
	}

	#[allow(clippy::too_many_arguments)]
	pub(in crate::exec) async fn fold_projected_left_semi_refs(
		&self,
		spilled: &mut SpilledInnerJoinBuild,
		refs: &[SpilledBatchRowRef],
		semi_output_schema: &arrow::datatypes::SchemaRef,
		retained_columns: &[ColumnOrdinal],
		partition_count: u32,
		exchange_key: &BoundExprProgram,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BoundGroupedAggregateGroups,
		group_types: &mut Vec<DataType>,
	) -> Result<(), ExecutionError> {
		let batch = materialize_projected_streamed_spilled_build_rows(
			spilled,
			refs,
			semi_output_schema,
			retained_columns,
			self.context.task_budget.memory.get(),
		)
		.await?;
		if batch.num_rows() == 0 {
			return Ok(());
		}
		for (_partition, partitioned) in
			self.repartition_exchange_batch(&batch, partition_count, exchange_key)?
		{
			if partitioned.num_rows() == 0 {
				continue;
			}
			self.fold_bound_grouped_aggregate_batch(
				groups,
				group_types,
				group_expr,
				aggr_expr,
				&partitioned,
			)?;
		}
		Ok(())
	}

	async fn emit_reoriented_chain_output(
		&self,
		sink: &mut ReorientedChainOutputSink<'_>,
		batch: RecordBatch,
	) -> Result<(), ExecutionError> {
		match sink {
			ReorientedChainOutputSink::GroupedFold {
				groups,
				group_types,
				outer_predicate,
				project_exprs,
				project_output_schema,
				group_expr,
				aggr_expr,
			} => self.fold_reoriented_grouped_output_batch(
				groups,
				group_types,
				*outer_predicate,
				*project_exprs,
				*project_output_schema,
				group_expr,
				aggr_expr,
				batch,
				None,
			),
			ReorientedChainOutputSink::ProjectedSpill {
				spilled,
				retained_columns,
				key_plan,
				build_keys,
				null_safe_keys,
			} => {
				spilled
					.write_projected_batch_chunks(
						&batch,
						retained_columns,
						*key_plan,
						build_keys,
						*null_safe_keys,
						self.context.task_budget,
					)
					.await
			}
			ReorientedChainOutputSink::MembershipFilteredGroupedFold {
				presence,
				left_key,
				partition_count,
				exchange_key,
				group_expr,
				aggr_expr,
				groups,
				group_types,
				input_rows,
				output_rows,
				profile,
			} => {
				**input_rows = input_rows.saturating_add(batch.num_rows() as u64);
				let presence_timer = profile
					.map(|profile| {
						profile.begin_leaf(
							MembershipChainPhase::Sink,
							MembershipChainOperation::PresenceFilter,
						)
					})
					.transpose()?;
				let keys = batch
					.columns()
					.get(left_key.0 as usize)
					.ok_or_else(|| ExecutionError::InvalidExpression {
						message: "membership-filtered fold key is out of range".to_string(),
					})?
					.as_any()
					.downcast_ref::<Int64Array>()
					.ok_or_else(|| ExecutionError::InvalidExpression {
						message: "membership-filtered fold key must be Int64".to_string(),
					})?;
				if keys.null_count() != 0 {
					return Err(ExecutionError::InvalidExpression {
						message: "membership-filtered fold key must be non-null".to_string(),
					});
				}
				let mut matches = Vec::with_capacity(batch.num_rows());
				for row in 0..batch.num_rows() {
					matches.push(presence.contains(keys.value(row))?);
				}
				let mask = BooleanArray::from(matches);
				let filtered = filter_record_batch(&batch, &mask)?;
				**output_rows = output_rows.saturating_add(filtered.num_rows() as u64);
				if let Some(profile) = *profile {
					profile.record_sink_rows(batch.num_rows(), filtered.num_rows())?;
				}
				if let Some(timer) = presence_timer {
					timer.finish_with(batch.num_rows() as u64, 0)?;
				}
				if filtered.num_rows() == 0 {
					return Ok(());
				}
				let exchange_timer = profile
					.map(|profile| {
						profile.begin_leaf(
							MembershipChainPhase::Sink,
							MembershipChainOperation::Exchange,
						)
					})
					.transpose()?;
				let partitioned_batches =
					self.repartition_exchange_batch(&filtered, *partition_count, exchange_key)?;
				if let Some(timer) = exchange_timer {
					timer.finish_with(filtered.num_rows() as u64, 0)?;
				}
				for (_partition, partitioned) in partitioned_batches {
					if partitioned.num_rows() == 0 {
						continue;
					}
					let fold_timer = profile
						.map(|profile| {
							profile.begin_leaf(
								MembershipChainPhase::Sink,
								MembershipChainOperation::GroupedFold,
							)
						})
						.transpose()?;
					self.fold_bound_grouped_aggregate_batch(
						groups,
						group_types,
						group_expr,
						aggr_expr,
						&partitioned,
					)?;
					if let Some(timer) = fold_timer {
						timer.finish_with(partitioned.num_rows() as u64, 0)?;
					}
				}
				Ok(())
			}
		}
	}

	pub(in crate::exec) async fn try_streaming_reoriented_inner_chain_grouped_aggregate(
		&self,
		input: &ExecutionPlan,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		output_schema: &arrow::datatypes::SchemaRef,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
		let (aggregate_input, aggregate_input_wrapped) = match input {
			ExecutionPlan::Exchange { input, .. } => (input.as_ref(), true),
			other => (other, false),
		};
		let (project_input, project_exprs, project_output_schema) = match aggregate_input {
			ExecutionPlan::Project {
				input,
				exprs,
				output_schema,
			} => (input.as_ref(), Some(exprs), Some(output_schema)),
			other => (other, None, None),
		};
		let (chain_input, outer_predicate) = match project_input {
			ExecutionPlan::Filter {
				input,
				predicate,
				disposition: FilterDisposition::EvaluateInToron,
			} => (input.as_ref(), Some(predicate)),
			ExecutionPlan::Filter {
				input,
				disposition: FilterDisposition::FullyPushed,
				..
			} => (input.as_ref(), None),
			other => (other, None),
		};
		let Some((base, mut stages)) = extract_reoriented_inner_chain(chain_input) else {
			return Ok(None);
		};
		let required_output = reoriented_grouped_required_output_columns(
			project_exprs,
			group_expr,
			aggr_expr,
			outer_predicate,
		);
		mark_reoriented_inner_stage_projections(&mut stages, required_output)?;
		let grouped_chain_profile = GroupedChainProfile::from_env(stages.len())?;
		let grouped_chain_root = if let Some(profile) = grouped_chain_profile.as_deref() {
			let spill = GroupedChainProfile::task_spill_bytes(&self.context.task_telemetry)?;
			Some(profile.begin_root(spill)?)
		} else {
			None
		};
		let mut runtime_prep_timer = grouped_chain_profile
			.as_deref()
			.map(|profile| profile.begin_leaf(GroupedChainProfileLeaf::RuntimePrepBuild))
			.transpose()?;

		let base_scan_probe_mode = reoriented_inner_chain_grouped_base_scan_probe_mode(base);
		let force_stream_projected_filtered_base = project_exprs.is_some()
			&& matches!(
				base_scan_probe_mode,
				Some(ReorientedInnerChainGroupedBaseScanProbeMode::FilteredJoin0)
			);
		let stage_count = stages.len();
		let mut runtimes = Vec::with_capacity(stage_count);
		let mut deferred_stream_stage = None;
		for (stage_index, stage) in stages.into_iter().enumerate() {
			if stage_index + 1 == stage_count
				&& self.reoriented_terminal_scan_build_estimated_over_cap(&stage, morsel)
			{
				deferred_stream_stage = Some((stage, None, true));
				break;
			}
			let scan_build_decision = if force_stream_projected_filtered_base
				&& matches!(
					base_scan_probe_mode,
					Some(ReorientedInnerChainGroupedBaseScanProbeMode::FilteredJoin0)
				) {
				ReorientedStageScanBuildDecision::Stream
			} else if aggregate_input_wrapped {
				if self
					.reoriented_inner_stage_scan_build_exceeds_scan_output_cap(&stage, morsel)
					.await?
				{
					ReorientedStageScanBuildDecision::Stream
				} else {
					ReorientedStageScanBuildDecision::Materialized
				}
			} else {
				self.reoriented_inner_stage_scan_build_decision(&stage, morsel)
					.await?
			};
			let scan_build_decision = if stage_index + 1 == stage_count {
				match scan_build_decision {
					ReorientedStageScanBuildDecision::Stream => {
						deferred_stream_stage = Some((stage, None, false));
						break;
					}
					ReorientedStageScanBuildDecision::StreamMaterializedBatches(batches) => {
						deferred_stream_stage = Some((stage, Some(batches), false));
						break;
					}
					other => other,
				}
			} else {
				scan_build_decision
			};
			let runtime = match scan_build_decision {
				ReorientedStageScanBuildDecision::Stream
				| ReorientedStageScanBuildDecision::StreamMaterializedBatches(_) => {
					self.prepare_reoriented_inner_join_runtime_with_streamed_scan_build(
						stage, morsel,
					)
					.await?
				}
				ReorientedStageScanBuildDecision::Materialized => {
					self.prepare_reoriented_inner_join_runtime(stage, morsel)
						.await?
				}
				ReorientedStageScanBuildDecision::MaterializedBatches(build_batches) => {
					self.prepare_reoriented_inner_join_runtime_from_build_batches(
						stage,
						build_batches,
					)
					.await?
				}
			};
			let Some(runtime) = runtime else {
				return Ok(None);
			};
			runtimes.push(runtime);
		}

		let mut groups = BoundGroupedAggregateGroups::new();
		let mut group_types = Vec::new();
		let mut matched = None;
		if let Some((stage, scan_probe_batches, needs_decision_fallback)) = deferred_stream_stage {
			if let Some(candidate) =
				self.reoriented_adaptive_partitioned_scan_candidate(&stage, morsel)?
			{
				runtimes.push(candidate);
				matched = self
					.try_streaming_reoriented_inner_chain_adaptive_last_stage_grouped(
						base,
						&mut runtimes,
						outer_predicate,
						project_exprs,
						project_output_schema,
						group_expr,
						aggr_expr,
						&mut groups,
						&mut group_types,
						morsel,
						scan_probe_batches,
					)
					.await?;
				if matched.is_none() {
					runtimes.pop();
				}
			}
			if matched.is_none() {
				let runtime = if needs_decision_fallback {
					let scan_build_decision = if force_stream_projected_filtered_base
						&& matches!(
							base_scan_probe_mode,
							Some(ReorientedInnerChainGroupedBaseScanProbeMode::FilteredJoin0)
						) {
						ReorientedStageScanBuildDecision::Stream
					} else if aggregate_input_wrapped {
						if self
							.reoriented_inner_stage_scan_build_exceeds_scan_output_cap(
								&stage, morsel,
							)
							.await?
						{
							ReorientedStageScanBuildDecision::Stream
						} else {
							ReorientedStageScanBuildDecision::Materialized
						}
					} else {
						self.reoriented_inner_stage_scan_build_decision(&stage, morsel)
							.await?
					};
					match scan_build_decision {
						ReorientedStageScanBuildDecision::Stream
						| ReorientedStageScanBuildDecision::StreamMaterializedBatches(_) => {
							self.prepare_reoriented_inner_join_runtime_with_streamed_scan_build(
								stage, morsel,
							)
							.await?
						}
						ReorientedStageScanBuildDecision::Materialized => {
							self.prepare_reoriented_inner_join_runtime(stage, morsel)
								.await?
						}
						ReorientedStageScanBuildDecision::MaterializedBatches(build_batches) => {
							self.prepare_reoriented_inner_join_runtime_from_build_batches(
								stage,
								build_batches,
							)
							.await?
						}
					}
				} else {
					self.prepare_reoriented_inner_join_runtime_with_streamed_scan_build(
						stage, morsel,
					)
					.await?
				};
				let Some(runtime) = runtime else {
					for runtime in runtimes {
						runtime.cleanup().await;
					}
					return Ok(None);
				};
				runtimes.push(runtime);
			}
		}
		if matched.is_none() {
			matched = self
				.try_streaming_reoriented_inner_chain_scan_probe_base_grouped(
					base,
					&mut runtimes,
					outer_predicate,
					project_exprs,
					project_output_schema,
					group_expr,
					aggr_expr,
					&mut groups,
					&mut group_types,
					morsel,
				)
				.await?;
		}
		if matched.is_none() {
			if let Some(timer) = runtime_prep_timer.take() {
				timer.finish()?;
			}
			if let Some(profile) = grouped_chain_profile.as_deref() {
				profile.record_runtime_prep_end_spill(GroupedChainProfile::task_spill_bytes(
					&self.context.task_telemetry,
				)?)?;
			}
			matched = self
				.try_streaming_reoriented_inner_chain_generic_base_grouped(
					base,
					&mut runtimes,
					outer_predicate,
					project_exprs,
					project_output_schema,
					group_expr,
					aggr_expr,
					&mut groups,
					&mut group_types,
					morsel,
					grouped_chain_profile.as_deref(),
				)
				.await?;
		}
		if runtime_prep_timer.is_some() && grouped_chain_profile.is_some() {
			return Err(ExecutionError::Kernel {
				message: "grouped-chain profile: admitted chain did not commit the generic grouped reservoir path"
					.to_string(),
			});
		}
		if matched.is_none() {
			for runtime in runtimes {
				runtime.cleanup().await;
			}
			return Ok(None);
		}

		for runtime in &runtimes {
			record_join_runtime_observation(
				&self.context.task_telemetry,
				&self.context.join_runtime_observation_mode,
				JoinRuntimeObservation {
					replay_id: runtime.replay_id,
					strategy: runtime.strategy,
					build_rows: RowCount::new(runtime.build_row_count()),
					probe_rows: RowCount::new(runtime.probe_rows),
					output_rows: RowCount::new(runtime.output_rows),
				},
			);
			if let Some(profile) = grouped_chain_profile.as_deref() {
				profile.record_runtime_stage(
					runtime.replay_id,
					runtime.build_row_count(),
					runtime.probe_rows,
					runtime.output_rows,
				)?;
			}
		}

		let finalization_timer = grouped_chain_profile
			.as_deref()
			.map(|profile| profile.begin_leaf(GroupedChainProfileLeaf::Finalization))
			.transpose()?;
		let output = self.finalize_bound_grouped_aggregate(
			&mut groups,
			&group_types,
			aggr_expr,
			output_schema,
		);
		if let Some(timer) = finalization_timer {
			timer.finish()?;
		}
		if let (Some(profile), Ok(output)) = (grouped_chain_profile.as_deref(), output.as_ref()) {
			profile.record_final_group_rows(record_batch_row_count(output))?;
		}
		let cleanup_timer = grouped_chain_profile
			.as_deref()
			.map(|profile| profile.begin_leaf(GroupedChainProfileLeaf::Cleanup))
			.transpose()?;
		for runtime in runtimes {
			runtime.cleanup().await;
		}
		if let Some(timer) = cleanup_timer {
			timer.finish()?;
		}
		if let (Some(profile), Some(root)) = (grouped_chain_profile.as_deref(), grouped_chain_root)
		{
			let spill = GroupedChainProfile::task_spill_bytes(&self.context.task_telemetry)?;
			profile.finish_root(root, spill)?;
			profile.persist()?;
		}
		output.map(Some)
	}

	fn reoriented_adaptive_partitioned_scan_candidate<'a>(
		&self,
		stage: &ReorientedInnerJoinStage<'a>,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<ReorientedInnerJoinRuntime<'a>>, ExecutionError> {
		let Some(scan_input) = decompose_streaming_scan_probe_input(stage.build) else {
			return Ok(None);
		};
		let key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&stage.build.schema(),
			&stage.build_keys,
			&stage.probe_schema,
			&stage.probe_keys,
			false,
		)?;
		let materialized_output_schema = if stage.probe_columns_first {
			concat_join_schema(&stage.build.schema(), &stage.probe_schema)
		} else {
			stage.output_schema.clone()
		};
		Ok(Some(ReorientedInnerJoinRuntime {
			replay_id: stage.replay_id,
			strategy: stage.strategy,
			build: ReorientedInnerJoinBuild::PartitionedScan {
				scan_input,
				morsel,
				partition_count: 1,
				group_count: 1,
				partition_build_keys: stage.build_keys.clone(),
				write_build_keys: stage.build_keys.clone(),
				materialized_output_schema,
				build_column_count: stage.build.schema().fields().len(),
				projection: None,
				estimated_build_bytes: 0,
				observed_build_rows: 0,
				loaded_builds: vec![None],
				loaded_resident_bytes: 0,
				loaded_build_attempted: false,
			},
			key_plan,
			probe_keys: stage.probe_keys.clone(),
			output_schema: stage.output_schema.clone(),
			post_filter: stage.post_filter,
			probe_columns_first: stage.probe_columns_first,
			probe_rows: 0,
			output_rows: 0,
		}))
	}

	#[allow(clippy::too_many_arguments)]
	async fn try_streaming_reoriented_inner_chain_adaptive_last_stage_grouped(
		&self,
		base: &ExecutionPlan,
		inner_runtimes: &mut [ReorientedInnerJoinRuntime<'_>],
		outer_predicate: Option<&BoundExprProgram>,
		project_exprs: Option<&BoundExprProgram>,
		project_output_schema: Option<&arrow::datatypes::SchemaRef>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BoundGroupedAggregateGroups,
		group_types: &mut Vec<DataType>,
		morsel: Option<(MorselId, u32)>,
		scan_probe_batches: Option<Vec<RecordBatch>>,
	) -> Result<Option<()>, ExecutionError> {
		let mut sink = ReorientedChainOutputSink::GroupedFold {
			groups,
			group_types,
			outer_predicate,
			project_exprs,
			project_output_schema,
			group_expr,
			aggr_expr,
		};
		self.try_streaming_reoriented_inner_chain_adaptive_last_stage(
			base,
			inner_runtimes,
			&mut sink,
			morsel,
			scan_probe_batches,
		)
		.await
	}

	async fn try_streaming_reoriented_inner_chain_adaptive_last_stage(
		&self,
		base: &ExecutionPlan,
		inner_runtimes: &mut [ReorientedInnerJoinRuntime<'_>],
		sink: &mut ReorientedChainOutputSink<'_>,
		morsel: Option<(MorselId, u32)>,
		scan_probe_batches: Option<Vec<RecordBatch>>,
	) -> Result<Option<()>, ExecutionError> {
		// The first adaptive cut only handles a terminal over-cap scan-build stage. If
		// later stages still need async probing, buffering the flipped output would
		// cross the output-order/materialization boundary this cut deliberately avoids.
		let Some((runtime, prefix)) = inner_runtimes.split_last_mut() else {
			return Ok(None);
		};
		if !prefix.is_empty() {
			return Ok(None);
		}
		match sink {
			ReorientedChainOutputSink::GroupedFold {
				groups,
				group_types,
				outer_predicate,
				project_exprs,
				project_output_schema,
				group_expr,
				aggr_expr,
			} => {
				let mut visit_output = |batch| {
					self.fold_reoriented_grouped_output_batch(
						groups,
						group_types,
						*outer_predicate,
						*project_exprs,
						*project_output_schema,
						group_expr,
						aggr_expr,
						batch,
						None,
					)
				};
				self.try_visit_reoriented_adaptive_derived_last_stage(
					base,
					runtime,
					morsel,
					scan_probe_batches,
					&mut visit_output,
				)
				.await
			}
			ReorientedChainOutputSink::ProjectedSpill { .. }
			| ReorientedChainOutputSink::MembershipFilteredGroupedFold { .. } => {
				self.try_visit_reoriented_adaptive_derived_last_stage_to_sink(
					base,
					runtime,
					morsel,
					scan_probe_batches,
					sink,
				)
				.await
			}
		}
	}

	async fn try_visit_reoriented_adaptive_derived_last_stage<F>(
		&self,
		base: &ExecutionPlan,
		runtime: &mut ReorientedInnerJoinRuntime<'_>,
		morsel: Option<(MorselId, u32)>,
		scan_probe_batches: Option<Vec<RecordBatch>>,
		visit_batch: &mut F,
	) -> Result<Option<()>, ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		let (
			scan_input,
			scan_probe_keys,
			runtime_probe_keys,
			output_schema,
			post_filter,
			original_probe_columns_first,
		) = match &runtime.build {
			ReorientedInnerJoinBuild::PartitionedScan {
				scan_input,
				partition_build_keys,
				..
			} => (
				*scan_input,
				partition_build_keys.clone(),
				runtime.probe_keys.clone(),
				runtime.output_schema.clone(),
				runtime.post_filter,
				runtime.probe_columns_first,
			),
			_ => return Ok(None),
		};
		let Some(derived_batches) = self
			.materialize_reoriented_inner_chain_base_under_cap(base, morsel)
			.await?
		else {
			return Ok(None);
		};
		if derived_batches.is_empty() {
			return Ok(Some(()));
		}
		let derived_schema = derived_batches[0].schema();
		let derived_batch = concat_or_empty(derived_schema.clone(), &derived_batches)?;
		let derived_key_arrays = key_arrays(&derived_batch, &runtime_probe_keys)?;
		let build_memory_cap =
			join_output_materialization_memory_cap(self.context.task_budget.memory.get());
		let estimated_build_bytes =
			estimated_build_hash_bytes(&derived_key_arrays, derived_batch.num_rows());
		if estimated_build_bytes > build_memory_cap {
			return Ok(None);
		}
		let scan_schema = scan_input.scan.schema();
		let join_key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&derived_schema,
			&runtime_probe_keys,
			&scan_schema,
			&scan_probe_keys,
			false,
		)?;
		let build_started = Instant::now();
		let Some(build_rows) = build_in_memory_join_rows(
			join_key_plan,
			&derived_key_arrays,
			derived_batch.num_rows(),
			build_memory_cap,
			false,
		)?
		else {
			return Ok(None);
		};
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.build_rows = task
				.counters
				.build_rows
				.saturating_add(derived_batch.num_rows() as u64);
			task.phases.build_micros = task
				.phases
				.build_micros
				.saturating_add(duration_micros(build_started));
		});
		if let ReorientedInnerJoinBuild::PartitionedScan {
			observed_build_rows,
			..
		} = &mut runtime.build
		{
			*observed_build_rows = derived_batch.num_rows() as u64;
		}

		let probe_started = Instant::now();
		let mut visit_output = |batch: RecordBatch| -> Result<(), ExecutionError> {
			let batch = if let Some(predicate) = post_filter {
				self.filter_batch_with_bound_predicate(predicate, batch, "filter predicate")?
			} else {
				batch
			};
			if batch.num_rows() > 0 {
				visit_batch(batch)?;
			}
			Ok(())
		};
		let (probe_rows, output_rows) = if let Some(scan_probe_batches) = scan_probe_batches {
			self.visit_reoriented_adaptive_derived_probe_batches(
				&derived_batch,
				&build_rows,
				join_key_plan,
				scan_input,
				&scan_probe_keys,
				&output_schema,
				!original_probe_columns_first,
				&mut visit_output,
				scan_probe_batches,
			)?
		} else {
			self.visit_streaming_in_memory_inner_scan_probe_chunks(
				&derived_batch,
				&build_rows,
				join_key_plan,
				scan_input,
				&scan_probe_keys,
				None,
				false,
				&output_schema,
				None,
				None,
				!original_probe_columns_first,
				morsel,
				&mut visit_output,
			)
			.await?
		};
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.phases.probe_micros = task
				.phases
				.probe_micros
				.saturating_add(duration_micros(probe_started));
		});
		self.record_reoriented_inner_probe(runtime, probe_rows, output_rows);
		Ok(Some(()))
	}

	async fn try_visit_reoriented_adaptive_derived_last_stage_to_sink(
		&self,
		base: &ExecutionPlan,
		runtime: &mut ReorientedInnerJoinRuntime<'_>,
		morsel: Option<(MorselId, u32)>,
		scan_probe_batches: Option<Vec<RecordBatch>>,
		sink: &mut ReorientedChainOutputSink<'_>,
	) -> Result<Option<()>, ExecutionError> {
		let (
			scan_input,
			scan_probe_keys,
			runtime_probe_keys,
			output_schema,
			post_filter,
			original_probe_columns_first,
		) = match &runtime.build {
			ReorientedInnerJoinBuild::PartitionedScan {
				scan_input,
				partition_build_keys,
				..
			} => (
				*scan_input,
				partition_build_keys.clone(),
				runtime.probe_keys.clone(),
				runtime.output_schema.clone(),
				runtime.post_filter,
				runtime.probe_columns_first,
			),
			_ => return Ok(None),
		};
		let Some(derived_batches) = self
			.materialize_reoriented_inner_chain_base_under_cap(base, morsel)
			.await?
		else {
			return Ok(None);
		};
		if derived_batches.is_empty() {
			return Ok(Some(()));
		}
		let derived_schema = derived_batches[0].schema();
		let derived_batch = concat_or_empty(derived_schema.clone(), &derived_batches)?;
		let derived_key_arrays = key_arrays(&derived_batch, &runtime_probe_keys)?;
		let build_memory_cap =
			join_output_materialization_memory_cap(self.context.task_budget.memory.get());
		let estimated_build_bytes =
			estimated_build_hash_bytes(&derived_key_arrays, derived_batch.num_rows());
		if estimated_build_bytes > build_memory_cap {
			return Ok(None);
		}
		let scan_schema = scan_input.scan.schema();
		let join_key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&derived_schema,
			&runtime_probe_keys,
			&scan_schema,
			&scan_probe_keys,
			false,
		)?;
		let build_started = Instant::now();
		let Some(build_rows) = build_in_memory_join_rows(
			join_key_plan,
			&derived_key_arrays,
			derived_batch.num_rows(),
			build_memory_cap,
			false,
		)?
		else {
			return Ok(None);
		};
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.build_rows = task
				.counters
				.build_rows
				.saturating_add(derived_batch.num_rows() as u64);
			task.phases.build_micros = task
				.phases
				.build_micros
				.saturating_add(duration_micros(build_started));
		});
		if let ReorientedInnerJoinBuild::PartitionedScan {
			observed_build_rows,
			..
		} = &mut runtime.build
		{
			*observed_build_rows = derived_batch.num_rows() as u64;
		}

		let probe_started = Instant::now();
		let result = async {
			let mut probe_rows = 0u64;
			let mut output_rows = 0u64;
			if let Some(scan_probe_batches) = scan_probe_batches {
				for probe_batch in scan_probe_batches {
					if let Some(exchange) = scan_input.exchange {
						for (_, partitioned) in self.repartition_exchange_batch(
							&probe_batch,
							exchange.partition_count,
							exchange.key,
						)? {
							let (batch_probe_rows, batch_output_rows) = self
								.visit_reoriented_adaptive_probe_batch_to_sink(
									&derived_batch,
									&build_rows,
									join_key_plan,
									&partitioned,
									&scan_probe_keys,
									&output_schema,
									!original_probe_columns_first,
									post_filter,
									sink,
								)
								.await?;
							probe_rows = probe_rows.saturating_add(batch_probe_rows);
							output_rows = output_rows.saturating_add(batch_output_rows);
						}
					} else {
						let (batch_probe_rows, batch_output_rows) = self
							.visit_reoriented_adaptive_probe_batch_to_sink(
								&derived_batch,
								&build_rows,
								join_key_plan,
								&probe_batch,
								&scan_probe_keys,
								&output_schema,
								!original_probe_columns_first,
								post_filter,
								sink,
							)
							.await?;
						probe_rows = probe_rows.saturating_add(batch_probe_rows);
						output_rows = output_rows.saturating_add(batch_output_rows);
					}
				}
			} else {
				let Some(req) = self.bound_scan_request(scan_input.scan, morsel) else {
					return Err(ExecutionError::InvalidExpression {
						message: "adaptive reoriented scan probe expected a scan input".to_string(),
					});
				};
				let mut remaining = req.row_limit;
				let span = crate::telemetry::storage_scan_span(&req);
				let mut stream = self
					.storage
					.scan(req)
					.instrument(span)
					.await?
					.into_batches();
				while let Some(batch) = stream.next().await {
					let Some(mut probe_batch) = limit_streaming_scan_batch(batch?, &mut remaining)
					else {
						break;
					};
					if let Some(predicate) = scan_input.filter_predicate {
						probe_batch = self.filter_batch_with_bound_predicate(
							predicate,
							probe_batch,
							"filter predicate",
						)?;
						if probe_batch.num_rows() == 0 {
							continue;
						}
					}
					let probe_batches = if let Some(exchange) = scan_input.exchange {
						self.repartition_exchange_batch(
							&probe_batch,
							exchange.partition_count,
							exchange.key,
						)?
						.into_iter()
						.map(|(_, batch)| batch)
						.collect::<Vec<_>>()
					} else {
						vec![probe_batch]
					};
					for probe_batch in probe_batches {
						let (batch_probe_rows, batch_output_rows) = self
							.visit_reoriented_adaptive_probe_batch_to_sink(
								&derived_batch,
								&build_rows,
								join_key_plan,
								&probe_batch,
								&scan_probe_keys,
								&output_schema,
								!original_probe_columns_first,
								post_filter,
								sink,
							)
							.await?;
						probe_rows = probe_rows.saturating_add(batch_probe_rows);
						output_rows = output_rows.saturating_add(batch_output_rows);
					}
				}
			}
			Ok::<(u64, u64), ExecutionError>((probe_rows, output_rows))
		}
		.await;
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.phases.probe_micros = task
				.phases
				.probe_micros
				.saturating_add(duration_micros(probe_started));
		});
		let (probe_rows, output_rows) = result?;
		self.record_reoriented_inner_probe(runtime, probe_rows, output_rows);
		Ok(Some(()))
	}

	#[allow(clippy::too_many_arguments)]
	async fn visit_reoriented_adaptive_probe_batch_to_sink(
		&self,
		build_batch: &RecordBatch,
		build_rows: &JoinBuildRows,
		join_key_plan: JoinKeyPlan,
		probe_batch: &RecordBatch,
		probe_keys: &[ColumnOrdinal],
		output_schema: &SchemaRef,
		probe_columns_first: bool,
		post_filter: Option<&BoundExprProgram>,
		sink: &mut ReorientedChainOutputSink<'_>,
	) -> Result<(u64, u64), ExecutionError> {
		if probe_batch.num_rows() == 0 {
			return Ok((0, 0));
		}
		let probe_key_arrays = key_arrays(probe_batch, probe_keys)?;
		let selection = probe_in_memory_join_rows(
			JoinKind::Inner,
			join_key_plan,
			build_rows,
			build_batch.num_rows(),
			&probe_key_arrays,
			probe_batch.num_rows(),
			None,
			build_batch,
			probe_batch,
			false,
		)?;
		let output_rows = selection.len() as u64;
		for chunk in selection.chunks(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let batch = materialize_reoriented_inner_selection(
				probe_batch,
				build_batch,
				chunk,
				output_schema,
				self.context.task_budget.memory.get(),
				probe_columns_first,
			)?;
			let batch = if let Some(predicate) = post_filter {
				self.filter_batch_with_bound_predicate(predicate, batch, "filter predicate")?
			} else {
				batch
			};
			if batch.num_rows() > 0 {
				self.emit_reoriented_chain_output(sink, batch).await?;
			}
		}
		Ok((probe_batch.num_rows() as u64, output_rows))
	}

	#[allow(clippy::too_many_arguments)]
	fn visit_reoriented_adaptive_derived_probe_batches<F>(
		&self,
		build_batch: &RecordBatch,
		build_rows: &JoinBuildRows,
		join_key_plan: JoinKeyPlan,
		scan_input: StreamingScanProbeInput<'_>,
		probe_keys: &[crate::ir::ColumnOrdinal],
		output_schema: &arrow::datatypes::SchemaRef,
		probe_columns_first: bool,
		visit_batch: &mut F,
		scan_probe_batches: Vec<RecordBatch>,
	) -> Result<(u64, u64), ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		let mut probe_rows = 0u64;
		let mut output_rows = 0u64;
		for probe_batch in scan_probe_batches {
			if let Some(exchange) = scan_input.exchange {
				for (_, partitioned) in self.repartition_exchange_batch(
					&probe_batch,
					exchange.partition_count,
					exchange.key,
				)? {
					let (batch_probe_rows, batch_output_rows) = self
						.visit_reoriented_adaptive_derived_probe_batch(
							build_batch,
							build_rows,
							join_key_plan,
							&partitioned,
							probe_keys,
							output_schema,
							probe_columns_first,
							&mut *visit_batch,
						)?;
					probe_rows = probe_rows.saturating_add(batch_probe_rows);
					output_rows = output_rows.saturating_add(batch_output_rows);
				}
			} else {
				let (batch_probe_rows, batch_output_rows) = self
					.visit_reoriented_adaptive_derived_probe_batch(
						build_batch,
						build_rows,
						join_key_plan,
						&probe_batch,
						probe_keys,
						output_schema,
						probe_columns_first,
						&mut *visit_batch,
					)?;
				probe_rows = probe_rows.saturating_add(batch_probe_rows);
				output_rows = output_rows.saturating_add(batch_output_rows);
			}
		}
		Ok((probe_rows, output_rows))
	}

	#[allow(clippy::too_many_arguments)]
	fn visit_reoriented_adaptive_derived_probe_batch<F>(
		&self,
		build_batch: &RecordBatch,
		build_rows: &JoinBuildRows,
		join_key_plan: JoinKeyPlan,
		probe_batch: &RecordBatch,
		probe_keys: &[crate::ir::ColumnOrdinal],
		output_schema: &arrow::datatypes::SchemaRef,
		probe_columns_first: bool,
		visit_batch: &mut F,
	) -> Result<(u64, u64), ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		if probe_batch.num_rows() == 0 {
			return Ok((0, 0));
		}
		let probe_key_arrays = key_arrays(probe_batch, probe_keys)?;
		let selection = probe_in_memory_join_rows(
			JoinKind::Inner,
			join_key_plan,
			build_rows,
			build_batch.num_rows(),
			&probe_key_arrays,
			probe_batch.num_rows(),
			None,
			build_batch,
			probe_batch,
			false,
		)?;
		let output_rows = selection.len() as u64;
		self.visit_reoriented_inner_selection_filtered_chunks(
			build_batch,
			probe_batch,
			&selection,
			output_schema,
			None,
			probe_columns_first,
			visit_batch,
		)?;
		Ok((probe_batch.num_rows() as u64, output_rows))
	}

	async fn materialize_reoriented_inner_chain_base_under_cap(
		&self,
		base: &ExecutionPlan,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
		let accumulator_cap =
			join_output_materialization_memory_cap(self.context.task_budget.memory.get());
		let mut accumulated_bytes = 0u64;
		let mut batches = Vec::new();
		let matched = self
			.try_visit_streaming_reoriented_inner_chain_base(base, morsel, |batch| {
				accumulated_bytes =
					accumulated_bytes.saturating_add(record_batch_memory_bytes(&batch));
				if accumulated_bytes > accumulator_cap {
					return Err(budget_exhausted(
						BudgetDimension::Memory,
						"adaptive derived-side build accumulator",
						accumulated_bytes,
						accumulator_cap,
					));
				}
				if batch.num_rows() > 0 {
					batches.push(batch);
				}
				Ok(())
			})
			.await;
		match matched {
			Ok(Some(())) => Ok(Some(batches)),
			Ok(None) => Ok(None),
			Err(ExecutionError::BudgetExhausted {
				dimension: BudgetDimension::Memory,
				..
			}) => Ok(None),
			Err(error) => Err(error),
		}
	}

	#[allow(clippy::too_many_arguments)]
	async fn try_streaming_reoriented_inner_chain_scan_probe_base_grouped(
		&self,
		base: &ExecutionPlan,
		inner_runtimes: &mut [ReorientedInnerJoinRuntime<'_>],
		outer_predicate: Option<&BoundExprProgram>,
		project_exprs: Option<&BoundExprProgram>,
		project_output_schema: Option<&arrow::datatypes::SchemaRef>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BoundGroupedAggregateGroups,
		group_types: &mut Vec<DataType>,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<()>, ExecutionError> {
		let mut visit_batch = |batch| {
			self.fold_reoriented_grouped_output_batch(
				groups,
				group_types,
				outer_predicate,
				project_exprs,
				project_output_schema,
				group_expr,
				aggr_expr,
				batch,
				None,
			)
		};
		self.try_visit_streaming_reoriented_inner_chain_scan_probe_base(
			base,
			inner_runtimes,
			morsel,
			&mut visit_batch,
		)
		.await
	}

	async fn try_visit_streaming_reoriented_inner_chain_scan_probe_base<F>(
		&self,
		base: &ExecutionPlan,
		inner_runtimes: &mut [ReorientedInnerJoinRuntime<'_>],
		morsel: Option<(MorselId, u32)>,
		visit_batch: &mut F,
	) -> Result<Option<()>, ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		let ExecutionPlan::Exchange {
			input: base_input,
			partition_count,
			key: base_exchange_key,
		} = base
		else {
			return Ok(None);
		};
		let (join0, join0_filter) = match base_input.as_ref() {
			ExecutionPlan::Filter {
				input,
				predicate,
				disposition: FilterDisposition::EvaluateInToron,
			} => (input.as_ref(), Some(predicate)),
			other => (other, None),
		};
		let ExecutionPlan::Join {
			replay_id,
			left,
			right,
			kind: JoinKind::Inner,
			left_keys,
			right_keys,
			match_filter,
			null_safe_keys,
			strategy,
			output_schema: join0_output_schema,
			..
		} = join0
		else {
			return Ok(None);
		};
		if *replay_id != JoinReplayId(0) {
			return Ok(None);
		}
		let Some(scan_input) = decompose_streaming_scan_probe_input(right.as_ref()) else {
			return Ok(None);
		};

		let left_batches = self.execute_bound(left.as_ref(), morsel).await?;
		let left_batch = concat_or_empty(left.schema(), &left_batches)?;
		let left_key_arrays = key_arrays(&left_batch, left_keys)?;
		if estimated_build_hash_bytes(&left_key_arrays, left_batch.num_rows())
			> self.context.task_budget.memory.get()
		{
			return Ok(None);
		}
		let join_key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&left.schema(),
			left_keys,
			&right.schema(),
			right_keys,
			*null_safe_keys,
		)?;
		let build_started = Instant::now();
		let Some(build_rows) = build_in_memory_join_rows(
			join_key_plan,
			&left_key_arrays,
			left_batch.num_rows(),
			self.context.task_budget.memory.get(),
			*null_safe_keys,
		)?
		else {
			return Ok(None);
		};
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.build_rows = task
				.counters
				.build_rows
				.saturating_add(left_batch.num_rows() as u64);
			task.phases.build_micros = task
				.phases
				.build_micros
				.saturating_add(duration_micros(build_started));
		});
		let Some(req) = self.bound_scan_request(scan_input.scan, morsel) else {
			return Ok(None);
		};
		let mut remaining = req.row_limit;
		let span = crate::telemetry::storage_scan_span(&req);
		let mut stream = self
			.storage
			.scan(req)
			.instrument(span)
			.await?
			.into_batches();
		let probe_started = Instant::now();
		let mut probe_rows = 0u64;
		let mut output_rows = 0u64;
		while let Some(batch) = stream.next().await {
			let Some(mut right_batch) = limit_streaming_scan_batch(batch?, &mut remaining) else {
				break;
			};
			if let Some(predicate) = scan_input.filter_predicate {
				right_batch = self.filter_batch_with_bound_predicate(
					predicate,
					right_batch,
					"filter predicate",
				)?;
				if right_batch.num_rows() == 0 {
					continue;
				}
			}
			probe_rows = probe_rows.saturating_add(right_batch.num_rows() as u64);
			let right_key_arrays = key_arrays(&right_batch, right_keys)?;
			let selection = probe_in_memory_join_rows(
				JoinKind::Inner,
				join_key_plan,
				&build_rows,
				left_batch.num_rows(),
				&right_key_arrays,
				right_batch.num_rows(),
				match_filter.as_ref(),
				&left_batch,
				&right_batch,
				*null_safe_keys,
			)?;
			output_rows = output_rows.saturating_add(selection.len() as u64);

			for chunk in selection.chunks(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
				let batch = materialize_join_selection(
					JoinKind::Inner,
					&left_batch,
					&right_batch,
					chunk,
					join0_output_schema,
					self.context.task_budget.memory.get(),
				)?;
				let batch = if let Some(join0_filter) = join0_filter {
					self.filter_batch_with_bound_predicate(join0_filter, batch, "filter predicate")?
				} else {
					batch
				};
				if batch.num_rows() == 0 {
					continue;
				}
				for (_partition, partitioned) in
					self.repartition_exchange_batch(&batch, *partition_count, base_exchange_key)?
				{
					if partitioned.num_rows() == 0 {
						continue;
					}
					let mut batches = vec![partitioned];
					for runtime in inner_runtimes.iter_mut() {
						let mut next = Vec::new();
						for batch in batches {
							next.extend(
								self.probe_reoriented_inner_join_streaming(runtime, &batch)
									.await?,
							);
						}
						batches = next;
						if let Some(predicate) = runtime.post_filter {
							let mut filtered = Vec::new();
							for batch in batches {
								let batch = self.filter_batch_with_bound_predicate(
									predicate,
									batch,
									"filter predicate",
								)?;
								if batch.num_rows() > 0 {
									filtered.push(batch);
								}
							}
							batches = filtered;
						}
						if batches.is_empty() {
							break;
						}
					}

					for batch in batches {
						visit_batch(batch)?;
					}
				}
			}
		}
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.phases.probe_micros = task
				.phases
				.probe_micros
				.saturating_add(duration_micros(probe_started));
			task.counters.probe_rows = task.counters.probe_rows.saturating_add(probe_rows);
			task.counters.matched_rows = task.counters.matched_rows.saturating_add(output_rows);
		});
		record_join_runtime_observation(
			&self.context.task_telemetry,
			&self.context.join_runtime_observation_mode,
			JoinRuntimeObservation {
				replay_id: *replay_id,
				strategy: *strategy,
				build_rows: RowCount::new(left_batch.num_rows() as u64),
				probe_rows: RowCount::new(probe_rows),
				output_rows: RowCount::new(output_rows),
			},
		);
		Ok(Some(()))
	}

	async fn try_visit_streaming_reoriented_inner_chain<F>(
		&self,
		input: &ExecutionPlan,
		morsel: Option<(MorselId, u32)>,
		visit_batch: &mut F,
	) -> Result<Option<()>, ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		let Some((base, stages)) = extract_reoriented_inner_chain(input) else {
			return Ok(None);
		};
		let mut runtimes: Vec<ReorientedInnerJoinRuntime<'_>> = Vec::with_capacity(stages.len());
		for stage in stages {
			let decision = match self
				.reoriented_inner_stage_scan_build_decision(&stage, morsel)
				.await
			{
				Ok(decision) => decision,
				Err(error) => {
					for runtime in runtimes {
						runtime.cleanup().await;
					}
					return Err(error);
				}
			};
			let runtime = match match decision {
				ReorientedStageScanBuildDecision::Stream => {
					self.prepare_reoriented_inner_join_runtime_with_streamed_scan_build(
						stage, morsel,
					)
					.await
				}
				ReorientedStageScanBuildDecision::StreamMaterializedBatches(build_batches)
				| ReorientedStageScanBuildDecision::MaterializedBatches(build_batches) => {
					self.prepare_reoriented_inner_join_runtime_from_build_batches(
						stage,
						build_batches,
					)
					.await
				}
				ReorientedStageScanBuildDecision::Materialized => {
					self.prepare_reoriented_inner_join_runtime(stage, morsel)
						.await
				}
			} {
				Ok(runtime) => runtime,
				Err(error) => {
					for runtime in runtimes {
						runtime.cleanup().await;
					}
					return Err(error);
				}
			};
			let Some(runtime) = runtime else {
				for runtime in runtimes {
					runtime.cleanup().await;
				}
				return Ok(None);
			};
			runtimes.push(runtime);
		}

		let result = self
			.try_visit_streaming_reoriented_inner_chain_scan_probe_base(
				base,
				&mut runtimes,
				morsel,
				visit_batch,
			)
			.await;
		for runtime in &runtimes {
			record_join_runtime_observation(
				&self.context.task_telemetry,
				&self.context.join_runtime_observation_mode,
				JoinRuntimeObservation {
					replay_id: runtime.replay_id,
					strategy: runtime.strategy,
					build_rows: RowCount::new(runtime.build_row_count()),
					probe_rows: RowCount::new(runtime.probe_rows),
					output_rows: RowCount::new(runtime.output_rows),
				},
			);
		}
		for runtime in runtimes {
			runtime.cleanup().await;
		}
		result
	}

	#[allow(clippy::too_many_arguments)]
	async fn try_streaming_reoriented_inner_chain_scan_probe_base_to_sink<'p>(
		&self,
		base: &ExecutionPlan,
		terminal_stage: &ReorientedInnerJoinStage<'p>,
		runtime: &mut ReorientedInnerJoinRuntime<'p>,
		sink: &mut ReorientedChainOutputSink<'_>,
		morsel: Option<(MorselId, u32)>,
		resident_baseline_bytes: u64,
		profile: Option<&MembershipChainProfile>,
	) -> Result<Option<()>, ExecutionError> {
		let ExecutionPlan::Exchange {
			input: base_input,
			partition_count: base_partition_count,
			key: base_exchange_key,
		} = base
		else {
			return Ok(None);
		};
		let (join0, join0_filter) = match base_input.as_ref() {
			ExecutionPlan::Filter {
				input,
				predicate,
				disposition: FilterDisposition::EvaluateInToron,
			} => (input.as_ref(), Some(predicate)),
			ExecutionPlan::Filter {
				input,
				disposition: FilterDisposition::FullyPushed,
				..
			} => (input.as_ref(), None),
			other => (other, None),
		};
		let ExecutionPlan::Join {
			replay_id,
			left,
			right,
			kind: JoinKind::Inner,
			left_keys,
			right_keys,
			match_filter,
			null_safe_keys,
			strategy,
			output_schema: join0_output_schema,
			..
		} = join0
		else {
			return Ok(None);
		};
		if *replay_id != JoinReplayId(0) || match_filter.is_some() || *null_safe_keys {
			return Ok(None);
		}
		let Some(base_probe_scan_input) = decompose_streaming_scan_probe_input(right.as_ref())
		else {
			return Ok(None);
		};
		if decompose_streaming_scan_probe_input(left.as_ref()).is_none() {
			return Ok(None);
		}
		let base_stage = ReorientedInnerJoinStage {
			replay_id: *replay_id,
			probe_schema: right.schema(),
			probe_keys: right_keys.clone(),
			build: left.as_ref(),
			build_keys: left_keys.clone(),
			retained_build_columns: None,
			output_schema: join0_output_schema.clone(),
			strategy: *strategy,
			post_filter: join0_filter,
			probe_columns_first: false,
		};
		let base_prepare_timer = profile
			.map(|profile| {
				profile.begin_leaf(
					MembershipChainPhase::Lifecycle,
					MembershipChainOperation::RuntimePrepare,
				)
			})
			.transpose()?;
		let prepared_base_runtime = self
			.prepare_reoriented_inner_join_runtime_with_streamed_scan_build_baseline(
				base_stage,
				morsel,
				resident_baseline_bytes,
			)
			.await?;
		if let Some(timer) = base_prepare_timer {
			timer.finish()?;
		}
		let Some(mut base_runtime) = prepared_base_runtime else {
			return Ok(None);
		};
		// A spilled base cannot use the partitioned-scan machinery below, and a
		// partitioned-scan terminal inside the streaming loop would re-scan its
		// build for every probe batch. Re-prepare the terminal in materialized
		// form (InMemory or Spilled — both probe without re-scanning) so the
		// mixed pair streams through the same loop as the spilled terminal.
		if matches!(base_runtime.build, ReorientedInnerJoinBuild::Spilled { .. })
			&& matches!(
				runtime.build,
				ReorientedInnerJoinBuild::PartitionedScan { .. }
			) {
			let reprepared = self
				.prepare_reoriented_inner_join_runtime_with_scan_build(
					terminal_stage.clone(),
					morsel,
					false,
					resident_baseline_bytes,
				)
				.await?;
			let Some(materialized) = reprepared else {
				base_runtime.cleanup().await;
				return Ok(None);
			};
			let stale = std::mem::replace(runtime, materialized);
			stale.cleanup().await;
		}
		if matches!(base_runtime.build, ReorientedInnerJoinBuild::Spilled { .. }) {
			let base_post_filter = base_runtime.post_filter;
			let terminal_post_filter = runtime.post_filter;
			let result = async {
				let Some(req) = self.bound_scan_request(base_probe_scan_input.scan, morsel) else {
					return Err(ExecutionError::InvalidExpression {
						message: "committed reoriented chain lost its base scan".to_string(),
					});
				};
				let mut remaining = req.row_limit;
				let span = crate::telemetry::storage_scan_span(&req);
				let mut stream = self
					.storage
					.scan(req)
					.instrument(span)
					.await?
					.into_batches();
				while let Some(batch) = stream.next().await {
					let Some(mut probe_batch) = limit_streaming_scan_batch(batch?, &mut remaining)
					else {
						break;
					};
					if let Some(predicate) = base_probe_scan_input.filter_predicate {
						probe_batch = self.filter_batch_with_bound_predicate(
							predicate,
							probe_batch,
							"filter predicate",
						)?;
						if probe_batch.num_rows() == 0 {
							continue;
						}
					}
					let mut base_output = self
						.probe_reoriented_inner_join_streaming(&mut base_runtime, &probe_batch)
						.await?;
					if let Some(predicate) = base_post_filter {
						let mut filtered = Vec::new();
						for batch in base_output {
							let batch = self.filter_batch_with_bound_predicate(
								predicate,
								batch,
								"filter predicate",
							)?;
							if batch.num_rows() > 0 {
								filtered.push(batch);
							}
						}
						base_output = filtered;
					}
					for base_batch in base_output {
						for (_partition, partitioned) in self.repartition_exchange_batch(
							&base_batch,
							*base_partition_count,
							base_exchange_key,
						)? {
							let mut output = self
								.probe_reoriented_inner_join_streaming(runtime, &partitioned)
								.await?;
							if let Some(predicate) = terminal_post_filter {
								let mut filtered = Vec::new();
								for batch in output {
									let batch = self.filter_batch_with_bound_predicate(
										predicate,
										batch,
										"filter predicate",
									)?;
									if batch.num_rows() > 0 {
										filtered.push(batch);
									}
								}
								output = filtered;
							}
							for batch in output {
								self.emit_reoriented_chain_output(sink, batch).await?;
							}
						}
					}
				}
				Ok::<(), ExecutionError>(())
			}
			.await;
			if result.is_ok() {
				record_join_runtime_observation(
					&self.context.task_telemetry,
					&self.context.join_runtime_observation_mode,
					JoinRuntimeObservation {
						replay_id: base_runtime.replay_id,
						strategy: base_runtime.strategy,
						build_rows: RowCount::new(base_runtime.build_row_count()),
						probe_rows: RowCount::new(base_runtime.probe_rows),
						output_rows: RowCount::new(base_runtime.output_rows),
					},
				);
			}
			base_runtime.cleanup().await;
			return result.map(|()| Some(()));
		}
		let (
			base_build_scan_input,
			base_build_morsel,
			base_build_partition_count,
			base_build_group_count,
			base_partition_build_keys,
			base_write_build_keys,
			base_key_plan,
			base_materialized_output_schema,
			base_output_schema,
			base_probe_columns_first,
			base_build_column_count,
			base_projection,
			base_probe_keys,
			base_post_filter,
		) = match &base_runtime.build {
			ReorientedInnerJoinBuild::PartitionedScan {
				scan_input,
				morsel: build_morsel,
				partition_count,
				group_count,
				partition_build_keys,
				write_build_keys,
				materialized_output_schema,
				build_column_count,
				projection,
				..
			} if *partition_count > 1 => (
				*scan_input,
				*build_morsel,
				*partition_count,
				*group_count,
				partition_build_keys.clone(),
				write_build_keys.clone(),
				base_runtime.key_plan,
				materialized_output_schema.clone(),
				base_runtime.output_schema.clone(),
				base_runtime.probe_columns_first,
				*build_column_count,
				projection.clone(),
				base_runtime.probe_keys.clone(),
				base_runtime.post_filter,
			),
			_ => {
				base_runtime.cleanup().await;
				return Ok(None);
			}
		};

		let (
			terminal_scan_input,
			build_morsel,
			partition_count,
			group_count,
			partition_build_keys,
			write_build_keys,
			terminal_key_plan,
			materialized_output_schema,
			output_schema,
			probe_columns_first,
			build_column_count,
			projection,
			probe_keys,
			post_filter,
		) = match &runtime.build {
			ReorientedInnerJoinBuild::PartitionedScan {
				scan_input,
				morsel: build_morsel,
				partition_count,
				group_count,
				partition_build_keys,
				write_build_keys,
				materialized_output_schema,
				build_column_count,
				projection,
				..
			} if *partition_count > 1 => (
				*scan_input,
				*build_morsel,
				*partition_count,
				*group_count,
				partition_build_keys.clone(),
				write_build_keys.clone(),
				runtime.key_plan,
				materialized_output_schema.clone(),
				runtime.output_schema.clone(),
				runtime.probe_columns_first,
				*build_column_count,
				projection.clone(),
				runtime.probe_keys.clone(),
				runtime.post_filter,
			),
			_ => {
				base_runtime.cleanup().await;
				return Ok(None);
			}
		};
		if let Some(profile) = profile {
			profile.record_dispatch(
				replay_id.0,
				runtime.replay_id.0,
				base_build_partition_count,
				base_build_group_count,
				partition_count,
				group_count,
				base_probe_columns_first,
				probe_columns_first,
			)?;
		}

		let base_build_result = self
			.build_partitioned_streaming_scan_inner_build_reservoir(
				base_build_scan_input,
				base_build_morsel.or(morsel),
				base_build_partition_count,
				&base_partition_build_keys,
				&base_write_build_keys,
				base_key_plan,
				false,
				base_projection.as_ref(),
				resident_baseline_bytes,
				false,
				None,
				profile.map(|profile| {
					(
						profile,
						MembershipChainPhase::BaseBuild,
						MembershipSpillRole::BaseBuild,
					)
				}),
				None,
				None,
			)
			.await;
		let (mut base_reservoir, base_build_rows) = match base_build_result {
			Ok(Some(result)) => result,
			Ok(None) => {
				base_runtime.cleanup().await;
				return Ok(None);
			}
			Err(error) => {
				base_runtime.cleanup().await;
				return Err(error);
			}
		};
		let base_build_spill_bytes = base_reservoir
			.iter()
			.flatten()
			.try_fold(0u64, |bytes, spilled| {
				bytes.checked_add(spilled.spill_bytes())
			})
			.ok_or_else(|| ExecutionError::Kernel {
				message: "membership-chain base-build spill bytes overflow".to_string(),
			})?;

		let mut base_probe_rows = 0u64;
		let mut base_output_rows = 0u64;
		let partition_count_usize = match usize::try_from(partition_count) {
			Ok(count) => count,
			Err(_) => {
				Self::cleanup_partitioned_streamed_inner_builds(base_reservoir).await;
				base_runtime.cleanup().await;
				return Err(ExecutionError::Kernel {
					message: "reoriented chain partition count exceeds usize".to_string(),
				});
			}
		};
		let intermediate_key_plan = match JoinKeyPlan::from_build_schema_and_probe_schema(
			&base_output_schema,
			&probe_keys,
			&terminal_scan_input.scan.schema(),
			&partition_build_keys,
			false,
		) {
			Ok(plan) => plan,
			Err(error) => {
				Self::cleanup_partitioned_streamed_inner_builds(base_reservoir).await;
				base_runtime.cleanup().await;
				return Err(error);
			}
		};
		guard_membership_owner_resident(
			resident_baseline_bytes,
			summary_bytes::<Option<SpilledInnerJoinBuild>>(partition_count_usize),
			self.context.task_budget.memory.get(),
			"reoriented chain intermediate spill handles",
		)?;
		let mut intermediate_partitions: Vec<Option<SpilledInnerJoinBuild>> =
			(0..partition_count_usize).map(|_| None).collect();
		for slot in &mut intermediate_partitions {
			let created = SpilledInnerJoinBuild::create_with_resident_baseline(
				self.context.task_budget,
				self.context.join_spill_file_ops.clone(),
				self.context.join_spill_dir.clone(),
				resident_baseline_bytes,
			)
			.await;
			match created {
				Ok(created) => *slot = Some(created),
				Err(error) => {
					Self::cleanup_partitioned_streamed_inner_builds(intermediate_partitions).await;
					Self::cleanup_partitioned_streamed_inner_builds(base_reservoir).await;
					base_runtime.cleanup().await;
					return Err(error);
				}
			}
		}
		let base_partition_count_usize = match usize::try_from(base_build_partition_count) {
			Ok(count) => count,
			Err(_) => {
				Self::cleanup_partitioned_streamed_inner_builds(intermediate_partitions).await;
				Self::cleanup_partitioned_streamed_inner_builds(base_reservoir).await;
				base_runtime.cleanup().await;
				return Err(ExecutionError::Kernel {
					message: "reoriented base partition count exceeds usize".to_string(),
				});
			}
		};
		let base_probe_key_plan = match JoinKeyPlan::from_build_schema_and_probe_schema(
			&right.schema(),
			&base_probe_keys,
			&base_build_scan_input.scan.schema(),
			&base_partition_build_keys,
			false,
		) {
			Ok(plan) => plan,
			Err(error) => {
				Self::cleanup_partitioned_streamed_inner_builds(intermediate_partitions).await;
				Self::cleanup_partitioned_streamed_inner_builds(base_reservoir).await;
				base_runtime.cleanup().await;
				return Err(error);
			}
		};
		guard_membership_owner_resident(
			resident_baseline_bytes,
			summary_bytes::<Option<SpilledInnerJoinBuild>>(base_partition_count_usize),
			self.context.task_budget.memory.get(),
			"reoriented chain base probe spill handles",
		)?;
		let mut base_probe_partitions: Vec<Option<SpilledInnerJoinBuild>> =
			(0..base_partition_count_usize).map(|_| None).collect();
		for slot in &mut base_probe_partitions {
			let created = SpilledInnerJoinBuild::create_with_resident_baseline(
				self.context.task_budget,
				self.context.join_spill_file_ops.clone(),
				self.context.join_spill_dir.clone(),
				resident_baseline_bytes,
			)
			.await;
			match created {
				Ok(created) => *slot = Some(created),
				Err(error) => {
					Self::cleanup_partitioned_streamed_inner_builds(base_probe_partitions).await;
					Self::cleanup_partitioned_streamed_inner_builds(intermediate_partitions).await;
					Self::cleanup_partitioned_streamed_inner_builds(base_reservoir).await;
					base_runtime.cleanup().await;
					return Err(error);
				}
			}
		}

		let intermediate_started = Instant::now();
		let mut base_probe_spill_bytes = 0u64;
		let base_result = async {
			let Some(req) = self.bound_scan_request(base_probe_scan_input.scan, morsel) else {
				return Ok::<Option<()>, ExecutionError>(None);
			};
			let mut remaining = req.row_limit;
			let span = crate::telemetry::storage_scan_span(&req);
			let mut stream = self
				.storage
				.scan(req)
				.instrument(span)
				.await?
				.into_batches();
			loop {
				let scan_timer = profile
					.map(|profile| {
						profile.begin_leaf(
							MembershipChainPhase::BaseProbeInput,
							MembershipChainOperation::StorageScanDecode,
						)
					})
					.transpose()?;
				let next_batch = stream.next().await;
				if let Some(timer) = scan_timer {
					timer.finish_with(
						next_batch
							.as_ref()
							.and_then(|batch| batch.as_ref().ok())
							.map_or(0, |batch| batch.num_rows() as u64),
						0,
					)?;
				}
				let Some(batch) = next_batch else {
					break;
				};
				let Some(mut right_batch) = limit_streaming_scan_batch(batch?, &mut remaining)
				else {
					break;
				};
				if let Some(predicate) = base_probe_scan_input.filter_predicate {
					right_batch = self.filter_batch_with_bound_predicate(
						predicate,
						right_batch,
						"filter predicate",
					)?;
					if right_batch.num_rows() == 0 {
						continue;
					}
				}
				let write_timer = profile
					.map(|profile| {
						profile.begin_leaf(
							MembershipChainPhase::BaseProbeInput,
							MembershipChainOperation::PartitionSpillWrite,
						)
					})
					.transpose()?;
				let buckets = precompute_join_partition_buckets(
					&right_batch,
					&base_probe_keys,
					base_build_partition_count,
					false,
					false,
				)?;
				for (partition, batch) in partition_streaming_join_batch_with_buckets(
					&right_batch,
					&buckets,
					base_build_partition_count,
				)? {
					let Some(spilled) = base_probe_partitions
						.get_mut(partition)
						.and_then(Option::as_mut)
					else {
						return Err(ExecutionError::Kernel {
							message: format!(
								"reoriented base partition {partition} is out of range"
							),
						});
					};
					spilled
						.write_batch_chunks_unindexed(
							&batch,
							base_probe_key_plan,
							&base_probe_keys,
							false,
							self.context.task_budget,
						)
						.await?;
				}
				if let Some(timer) = write_timer {
					timer.finish_with(
						right_batch.num_rows() as u64,
						record_batch_memory_bytes(&right_batch),
					)?;
				}
			}

			for spilled in base_probe_partitions.iter_mut().flatten() {
				spilled.finish(self.context.task_budget).await?;
				spilled.release_write_scratch();
				base_probe_spill_bytes =
					base_probe_spill_bytes.saturating_add(spilled.spill_bytes());
			}
			for partition in 0..base_partition_count_usize {
				let Some(mut probe_partition) = base_probe_partitions
					.get_mut(partition)
					.and_then(Option::take)
				else {
					continue;
				};
				let read_timer = profile
					.map(|profile| {
						profile.begin_leaf(
							MembershipChainPhase::BaseJoin,
							MembershipChainOperation::SpillReadDecode,
						)
					})
					.transpose()?;
				let pages_result = probe_partition.read_all_batch_pages().await;
				let pages = match pages_result {
					Ok(pages) => pages,
					Err(error) => {
						probe_partition.cleanup().await;
						return Err(error);
					}
				};
				if let Some(timer) = read_timer {
					timer.finish_with(
						pages.iter().map(|batch| batch.num_rows() as u64).sum(),
						pages.iter().map(record_batch_memory_bytes).sum(),
					)?;
				}
				let concat_timer = profile
					.map(|profile| {
						profile.begin_leaf(
							MembershipChainPhase::BaseJoin,
							MembershipChainOperation::PostReadMaterialize,
						)
					})
					.transpose()?;
				let right_batch_result = concat_or_empty(right.schema(), &pages);
				if let Some(timer) = concat_timer {
					timer.finish()?;
				}
				drop(pages);
				probe_partition.cleanup().await;
				let right_batch = right_batch_result?;
				if right_batch.num_rows() == 0 {
					if let Some(build_partition) =
						base_reservoir.get_mut(partition).and_then(Option::take)
					{
						build_partition.cleanup().await;
					}
					continue;
				}

				let probe_result = self
					.probe_partitioned_streamed_inner_scan_build_reservoir(
						&mut base_reservoir,
						base_build_partition_count,
						base_build_group_count,
						&right_batch,
						&base_probe_keys,
						&base_write_build_keys,
						base_key_plan,
						false,
						&base_materialized_output_schema,
						&base_output_schema,
						base_probe_columns_first,
						base_build_column_count,
						base_projection.as_ref(),
						resident_baseline_bytes,
						profile.map(|profile| (profile, MembershipChainPhase::BaseJoin)),
						None,
					)
					.await;
				let build_partition = base_reservoir.get_mut(partition).and_then(Option::take);
				if let Some(build_partition) = build_partition {
					build_partition.cleanup().await;
				}
				let (mut base_output, probe_rows, output_rows) = probe_result?;
				base_probe_rows = base_probe_rows.saturating_add(probe_rows);
				base_output_rows = base_output_rows.saturating_add(output_rows);
				if let Some(predicate) = base_post_filter {
					let mut filtered = Vec::new();
					for batch in base_output {
						let batch = self.filter_batch_with_bound_predicate(
							predicate,
							batch,
							"filter predicate",
						)?;
						if batch.num_rows() > 0 {
							filtered.push(batch);
						}
					}
					base_output = filtered;
				}

				for base_batch in base_output {
					let intermediate_write_timer = profile
						.map(|profile| {
							profile.begin_leaf(
								MembershipChainPhase::Intermediate,
								MembershipChainOperation::PartitionSpillWrite,
							)
						})
						.transpose()?;
					for (_partition, partitioned) in self.repartition_exchange_batch(
						&base_batch,
						*base_partition_count,
						base_exchange_key,
					)? {
						if partitioned.num_rows() == 0 {
							continue;
						}
						let buckets = precompute_join_partition_buckets(
							&partitioned,
							&probe_keys,
							partition_count,
							false,
							false,
						)?;
						for (partition, batch) in partition_streaming_join_batch_with_buckets(
							&partitioned,
							&buckets,
							partition_count,
						)? {
							let Some(spilled) = intermediate_partitions
								.get_mut(partition)
								.and_then(Option::as_mut)
							else {
								return Err(ExecutionError::Kernel {
									message: format!(
										"reoriented chain partition {partition} is out of range"
									),
								});
							};
							spilled
								.write_batch_chunks_unindexed(
									&batch,
									intermediate_key_plan,
									&probe_keys,
									false,
									self.context.task_budget,
								)
								.await?;
						}
					}
					if let Some(timer) = intermediate_write_timer {
						timer.finish_with(
							base_batch.num_rows() as u64,
							record_batch_memory_bytes(&base_batch),
						)?;
					}
				}
			}
			Ok(Some(()))
		}
		.await;
		Self::cleanup_partitioned_streamed_inner_builds(base_reservoir).await;
		Self::cleanup_partitioned_streamed_inner_builds(base_probe_partitions).await;
		let matched = match base_result {
			Ok(matched) => matched,
			Err(error) => {
				Self::cleanup_partitioned_streamed_inner_builds(intermediate_partitions).await;
				base_runtime.cleanup().await;
				return Err(error);
			}
		};
		if matched.is_none() {
			Self::cleanup_partitioned_streamed_inner_builds(intermediate_partitions).await;
			base_runtime.cleanup().await;
			return Ok(None);
		}
		let finish_result = async {
			for spilled in intermediate_partitions.iter_mut().flatten() {
				spilled.finish(self.context.task_budget).await?;
				spilled.release_write_scratch();
			}
			Ok::<(), ExecutionError>(())
		}
		.await;
		if let Err(error) = finish_result {
			Self::cleanup_partitioned_streamed_inner_builds(intermediate_partitions).await;
			base_runtime.cleanup().await;
			return Err(error);
		}
		let intermediate_spill_bytes = intermediate_partitions
			.iter()
			.flatten()
			.fold(0u64, |bytes, spilled| {
				bytes.saturating_add(spilled.spill_bytes())
			})
			.saturating_add(base_probe_spill_bytes);
		let intermediate_only_spill_bytes = intermediate_spill_bytes
			.checked_sub(base_probe_spill_bytes)
			.ok_or_else(|| ExecutionError::Kernel {
				message: "membership-chain intermediate spill underflow".to_string(),
			})?;
		if let Some(profile) = profile {
			profile
				.record_spill_bytes(MembershipSpillRole::BaseProbeInput, base_probe_spill_bytes)?;
			profile.record_spill_bytes(
				MembershipSpillRole::Intermediate,
				intermediate_only_spill_bytes,
			)?;
		}
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.phases.spill_micros = task
				.phases
				.spill_micros
				.saturating_add(duration_micros(intermediate_started));
			task.counters.spill_bytes = task
				.counters
				.spill_bytes
				.saturating_add(intermediate_spill_bytes);
		});

		if let ReorientedInnerJoinBuild::PartitionedScan {
			observed_build_rows,
			..
		} = &mut base_runtime.build
		{
			*observed_build_rows = observed_build_rows.saturating_add(base_build_rows);
		}
		self.record_reoriented_inner_probe(&mut base_runtime, base_probe_rows, base_output_rows);
		record_join_runtime_observation(
			&self.context.task_telemetry,
			&self.context.join_runtime_observation_mode,
			JoinRuntimeObservation {
				replay_id: *replay_id,
				strategy: *strategy,
				build_rows: RowCount::new(base_runtime.build_row_count()),
				probe_rows: RowCount::new(base_runtime.probe_rows),
				output_rows: RowCount::new(base_runtime.output_rows),
			},
		);
		base_runtime.cleanup().await;

		let terminal_build_started = Instant::now();
		let mut terminal_build_rows = 0u64;
		let mut terminal_spill_bytes = 0u64;
		let mut terminal_probe_rows = 0u64;
		let mut terminal_output_rows = 0u64;
		let terminal_result = async {
			let group_count = group_count.max(1).min(partition_count);
			for group in 0..group_count {
				let group_range = partition_group_bounds(partition_count, group_count, group)?;
				guard_membership_owner_resident(
					resident_baseline_bytes,
					summary_bytes::<Option<SpilledInnerJoinBuild>>(partition_count_usize),
					self.context.task_budget.memory.get(),
					"reoriented chain terminal spill handles",
				)?;
				let mut terminal_partitions: Vec<Option<SpilledInnerJoinBuild>> =
					(0..partition_count_usize).map(|_| None).collect();
				let mut create_error = None;
				for partition in group_range.clone() {
					match SpilledInnerJoinBuild::create_with_resident_baseline(
						self.context.task_budget,
						self.context.join_spill_file_ops.clone(),
						self.context.join_spill_dir.clone(),
						resident_baseline_bytes,
					)
					.await
					{
						Ok(spilled) => terminal_partitions[partition] = Some(spilled),
						Err(error) => {
							create_error = Some(error);
							break;
						}
					}
				}
				if let Some(error) = create_error {
					Self::cleanup_partitioned_streamed_inner_builds(terminal_partitions).await;
					return Err(error);
				}
				let group_result = async {
					let mut group_spill_bytes = 0u64;
					self.write_partitioned_streaming_scan_inner_build_batches(
						&mut terminal_partitions,
						&mut group_spill_bytes,
						terminal_scan_input,
						projection
							.as_ref()
							.map(|projection| projection.retained_columns.as_slice()),
						&partition_build_keys,
						&write_build_keys,
						terminal_key_plan,
						false,
						build_morsel.or(morsel),
						group_range.start,
						group_range.end,
						true,
						None,
						None,
						profile.map(|profile| (profile, MembershipChainPhase::TerminalBuild)),
						None,
						None,
					)
					.await?;
					terminal_spill_bytes = terminal_spill_bytes.saturating_add(group_spill_bytes);
					for spilled in terminal_partitions.iter_mut().flatten() {
						spilled.finish(self.context.task_budget).await?;
						spilled.release_write_scratch();
						terminal_build_rows =
							terminal_build_rows.saturating_add(spilled.input_row_count());
					}

					for partition in group_range.clone() {
						let Some(mut intermediate) = intermediate_partitions
							.get_mut(partition)
							.and_then(Option::take)
						else {
							continue;
						};
						let read_timer = profile
							.map(|profile| {
								profile.begin_leaf(
									MembershipChainPhase::Intermediate,
									MembershipChainOperation::SpillReadDecode,
								)
							})
							.transpose()?;
						let pages_result = intermediate.read_all_batch_pages().await;
						let pages = match pages_result {
							Ok(pages) => pages,
							Err(error) => {
								intermediate.cleanup().await;
								return Err(error);
							}
						};
						if let Some(timer) = read_timer {
							timer.finish_with(
								pages.iter().map(|batch| batch.num_rows() as u64).sum(),
								pages.iter().map(record_batch_memory_bytes).sum(),
							)?;
						}
						let concat_timer = profile
							.map(|profile| {
								profile.begin_leaf(
									MembershipChainPhase::Intermediate,
									MembershipChainOperation::PostReadMaterialize,
								)
							})
							.transpose()?;
						let base_batch_result = concat_or_empty(base_output_schema.clone(), &pages);
						if let Some(timer) = concat_timer {
							timer.finish()?;
						}
						drop(pages);
						intermediate.cleanup().await;
						let base_batch = base_batch_result?;
						if base_batch.num_rows() == 0 {
							continue;
						}

						let probe_result = self
							.probe_partitioned_streamed_inner_scan_build_reservoir(
								&mut terminal_partitions,
								partition_count,
								group_count,
								&base_batch,
								&probe_keys,
								&write_build_keys,
								terminal_key_plan,
								false,
								&materialized_output_schema,
								&output_schema,
								probe_columns_first,
								build_column_count,
								projection.as_ref(),
								resident_baseline_bytes,
								profile
									.map(|profile| (profile, MembershipChainPhase::TerminalJoin)),
								None,
							)
							.await;
						let terminal_partition = terminal_partitions
							.get_mut(partition)
							.and_then(Option::take);
						if let Some(terminal_partition) = terminal_partition {
							terminal_partition.cleanup().await;
						}
						let (mut output, probe_rows, output_rows) = probe_result?;
						terminal_probe_rows = terminal_probe_rows.saturating_add(probe_rows);
						terminal_output_rows = terminal_output_rows.saturating_add(output_rows);
						if let Some(predicate) = post_filter {
							let mut filtered = Vec::new();
							for batch in output {
								let batch = self.filter_batch_with_bound_predicate(
									predicate,
									batch,
									"filter predicate",
								)?;
								if batch.num_rows() > 0 {
									filtered.push(batch);
								}
							}
							output = filtered;
						}
						for batch in output {
							self.emit_reoriented_chain_output(sink, batch).await?;
						}
					}
					Ok::<(), ExecutionError>(())
				}
				.await;
				Self::cleanup_partitioned_streamed_inner_builds(terminal_partitions).await;
				group_result?;
			}
			Ok::<(), ExecutionError>(())
		}
		.await;
		Self::cleanup_partitioned_streamed_inner_builds(intermediate_partitions).await;
		terminal_result?;
		if let Some(profile) = profile {
			profile.record_spill_bytes(MembershipSpillRole::TerminalBuild, terminal_spill_bytes)?;
			let engine_spill_bytes = base_build_spill_bytes
				.checked_add(base_probe_spill_bytes)
				.and_then(|bytes| bytes.checked_add(intermediate_only_spill_bytes))
				.and_then(|bytes| bytes.checked_add(terminal_spill_bytes))
				.ok_or_else(|| ExecutionError::Kernel {
					message: "membership-chain engine spill total overflow".to_string(),
				})?;
			profile.record_engine_spill_bytes(engine_spill_bytes)?;
		}
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.build_rows = task.counters.build_rows.saturating_add(terminal_build_rows);
			task.phases.build_micros = task
				.phases
				.build_micros
				.saturating_add(duration_micros(terminal_build_started));
			task.phases.spill_micros = task
				.phases
				.spill_micros
				.saturating_add(duration_micros(terminal_build_started));
			task.counters.spill_bytes = task
				.counters
				.spill_bytes
				.saturating_add(terminal_spill_bytes);
		});

		if let ReorientedInnerJoinBuild::PartitionedScan {
			observed_build_rows,
			..
		} = &mut runtime.build
		{
			*observed_build_rows = observed_build_rows.saturating_add(terminal_build_rows);
		}
		self.record_reoriented_inner_probe(runtime, terminal_probe_rows, terminal_output_rows);
		Ok(Some(()))
	}

	#[allow(clippy::too_many_arguments)]
	async fn try_streaming_reoriented_inner_chain_generic_base_grouped(
		&self,
		base: &ExecutionPlan,
		inner_runtimes: &mut [ReorientedInnerJoinRuntime<'_>],
		outer_predicate: Option<&BoundExprProgram>,
		project_exprs: Option<&BoundExprProgram>,
		project_output_schema: Option<&arrow::datatypes::SchemaRef>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BoundGroupedAggregateGroups,
		group_types: &mut Vec<DataType>,
		morsel: Option<(MorselId, u32)>,
		grouped_chain_profile: Option<&GroupedChainProfile>,
	) -> Result<Option<()>, ExecutionError> {
		let mut base_batches = Vec::new();
		let mut base_rows = 0u64;
		let mut accumulated_bytes = 0u64;
		let accumulator_cap =
			join_output_materialization_memory_cap(self.context.task_budget.memory.get());
		let base_timer = grouped_chain_profile
			.map(|profile| profile.begin_leaf(GroupedChainProfileLeaf::BaseAccumulation))
			.transpose()?;
		let matched = self
			.try_visit_streaming_reoriented_inner_chain_base(base, morsel, |base_batch| {
				base_rows = base_rows.saturating_add(base_batch.num_rows() as u64);
				accumulated_bytes =
					accumulated_bytes.saturating_add(record_batch_memory_bytes(&base_batch));
				if accumulated_bytes > accumulator_cap {
					return Err(budget_exhausted(
						BudgetDimension::Memory,
						"reoriented inner chain base accumulator",
						accumulated_bytes,
						accumulator_cap,
					));
				}
				if base_batch.num_rows() > 0 {
					base_batches.push(base_batch);
				}
				Ok(())
			})
			.await?;
		if let Some(timer) = base_timer {
			timer.finish_with(base_rows, accumulated_bytes)?;
		}
		if let Some(profile) = grouped_chain_profile {
			profile.record_base(base_batches.len(), base_rows, accumulated_bytes)?;
			profile.record_base_end_spill(GroupedChainProfile::task_spill_bytes(
				&self.context.task_telemetry,
			)?)?;
		}
		if matched.is_none() {
			return Ok(None);
		}
		let (profile_partition_count, profile_group_count) = inner_runtimes
			.first()
			.and_then(|runtime| match &runtime.build {
				ReorientedInnerJoinBuild::PartitionedScan {
					partition_count,
					group_count,
					..
				} => Some((*partition_count, *group_count)),
				_ => None,
			})
			.unwrap_or((0, 0));
		let mut residency_profile = GroupedReservoirResidencyProfile::from_env(
			&base_batches,
			base_batches.capacity(),
			profile_partition_count,
			profile_group_count,
		)?;
		if self
			.try_process_reoriented_inner_chain_grouped_partitioned_scan_reservoir(
				&base_batches,
				inner_runtimes,
				outer_predicate,
				project_exprs,
				project_output_schema,
				group_expr,
				aggr_expr,
				groups,
				group_types,
				morsel,
				residency_profile.as_mut(),
				grouped_chain_profile,
			)
			.await?
		{
			return Ok(Some(()));
		}
		if grouped_chain_profile.is_some() {
			return Err(ExecutionError::Kernel {
				message: "grouped-chain profile: generic grouped path did not commit the partitioned-scan reservoir"
					.to_string(),
			});
		}
		for base_batch in base_batches {
			self.process_reoriented_inner_chain_grouped_base_batch(
				base_batch,
				inner_runtimes,
				outer_predicate,
				project_exprs,
				project_output_schema,
				group_expr,
				aggr_expr,
				groups,
				group_types,
				None,
			)
			.await?;
		}
		Ok(Some(()))
	}

	#[allow(clippy::too_many_arguments)]
	async fn process_reoriented_inner_chain_grouped_base_batch(
		&self,
		base_batch: RecordBatch,
		inner_runtimes: &mut [ReorientedInnerJoinRuntime<'_>],
		outer_predicate: Option<&BoundExprProgram>,
		project_exprs: Option<&BoundExprProgram>,
		project_output_schema: Option<&arrow::datatypes::SchemaRef>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BoundGroupedAggregateGroups,
		group_types: &mut Vec<DataType>,
		grouped_chain_profile: Option<&GroupedChainProfile>,
	) -> Result<(), ExecutionError> {
		self.process_reoriented_inner_chain_grouped_suffix_batch(
			vec![base_batch],
			inner_runtimes,
			outer_predicate,
			project_exprs,
			project_output_schema,
			group_expr,
			aggr_expr,
			groups,
			group_types,
			grouped_chain_profile,
		)
		.await
	}

	#[allow(clippy::too_many_arguments)]
	async fn process_reoriented_inner_chain_grouped_suffix_batch(
		&self,
		mut batches: Vec<RecordBatch>,
		inner_runtimes: &mut [ReorientedInnerJoinRuntime<'_>],
		outer_predicate: Option<&BoundExprProgram>,
		project_exprs: Option<&BoundExprProgram>,
		project_output_schema: Option<&arrow::datatypes::SchemaRef>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BoundGroupedAggregateGroups,
		group_types: &mut Vec<DataType>,
		grouped_chain_profile: Option<&GroupedChainProfile>,
	) -> Result<(), ExecutionError> {
		for runtime in inner_runtimes.iter_mut() {
			let suffix_input_rows = record_batch_row_count(&batches);
			let suffix_timer = grouped_chain_profile
				.map(|profile| profile.begin_leaf(GroupedChainProfileLeaf::SuffixJoin))
				.transpose()?;
			let mut next = Vec::new();
			for batch in batches {
				next.extend(
					self.probe_reoriented_inner_join_streaming(runtime, &batch)
						.await?,
				);
			}
			batches = next;
			if batches.is_empty() {
				if let Some(timer) = suffix_timer {
					timer.finish_with(suffix_input_rows, 0)?;
				}
				break;
			}
			if let Some(predicate) = runtime.post_filter {
				let mut filtered = Vec::new();
				for batch in batches {
					let batch = self.filter_batch_with_bound_predicate(
						predicate,
						batch,
						"filter predicate",
					)?;
					if batch.num_rows() > 0 {
						filtered.push(batch);
					}
				}
				batches = filtered;
			}
			if let Some(timer) = suffix_timer {
				timer.finish_with(suffix_input_rows, 0)?;
			}
		}

		for batch in batches {
			let fold_rows = batch.num_rows() as u64;
			let fold_bytes = record_batch_memory_bytes(&batch);
			let fold_timer = grouped_chain_profile
				.map(|profile| profile.begin_leaf(GroupedChainProfileLeaf::GroupedFold))
				.transpose()?;
			self.fold_reoriented_grouped_output_batch(
				groups,
				group_types,
				outer_predicate,
				project_exprs,
				project_output_schema,
				group_expr,
				aggr_expr,
				batch,
				grouped_chain_profile,
			)?;
			if let Some(timer) = fold_timer {
				timer.finish_with(fold_rows, fold_bytes)?;
			}
		}
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	async fn try_process_reoriented_inner_chain_grouped_partitioned_scan_reservoir(
		&self,
		base_batches: &[RecordBatch],
		inner_runtimes: &mut [ReorientedInnerJoinRuntime<'_>],
		outer_predicate: Option<&BoundExprProgram>,
		project_exprs: Option<&BoundExprProgram>,
		project_output_schema: Option<&arrow::datatypes::SchemaRef>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BoundGroupedAggregateGroups,
		group_types: &mut Vec<DataType>,
		morsel: Option<(MorselId, u32)>,
		mut residency_profile: Option<&mut GroupedReservoirResidencyProfile>,
		grouped_chain_profile: Option<&GroupedChainProfile>,
	) -> Result<bool, ExecutionError> {
		let Some((runtime, suffix_runtimes)) = inner_runtimes.split_first_mut() else {
			if let Some(profile) = residency_profile.as_deref_mut() {
				profile.terminal(&[], "reservoir_path_missing_runtime")?;
			}
			return Ok(false);
		};
		let (
			scan_input,
			build_morsel,
			partition_count,
			group_count,
			partition_build_keys,
			write_build_keys,
			key_plan,
			null_safe_keys,
			materialized_output_schema,
			output_schema,
			probe_columns_first,
			build_column_count,
			projection,
			probe_keys,
			post_filter,
		) = match &runtime.build {
			ReorientedInnerJoinBuild::PartitionedScan {
				scan_input,
				morsel: build_morsel,
				partition_count,
				group_count,
				partition_build_keys,
				write_build_keys,
				materialized_output_schema,
				build_column_count,
				projection,
				..
			} => (
				*scan_input,
				*build_morsel,
				*partition_count,
				*group_count,
				partition_build_keys.clone(),
				write_build_keys.clone(),
				runtime.key_plan,
				false,
				materialized_output_schema.clone(),
				runtime.output_schema.clone(),
				runtime.probe_columns_first,
				*build_column_count,
				projection.clone(),
				runtime.probe_keys.clone(),
				runtime.post_filter,
			),
			_ => {
				if let Some(profile) = residency_profile.as_deref_mut() {
					profile.terminal(&[], "reservoir_path_not_partitioned_scan")?;
				}
				return Ok(false);
			}
		};

		if partition_count <= 1 || group_count <= 1 {
			if let Some(profile) = residency_profile.as_deref_mut() {
				profile.terminal(&[], "reservoir_path_not_admitted")?;
			}
			return Ok(false);
		}
		if let Some(profile) = grouped_chain_profile {
			profile.record_reservoir_dispatch(runtime.replay_id, partition_count, group_count)?;
		}
		let local_runtime_filter = if local_runtime_filter_enabled()
			&& local_runtime_filter_shape_is_admitted(
				null_safe_keys,
				key_plan,
				&partition_build_keys,
				&runtime.probe_keys,
			) {
			build_local_terminal_runtime_filter(
				self,
				base_batches,
				runtime.probe_keys.first().copied(),
				scan_input.scan,
				partition_build_keys.first().copied(),
				build_morsel.or(morsel),
			)
		} else {
			None
		};
		let grouped_chain_observer = grouped_chain_profile.map(GroupedChainProfile::spill_observer);

		let reservoir_write_timer = grouped_chain_profile
			.map(|profile| {
				profile.begin_leaf(GroupedChainProfileLeaf::ReservoirPartitionSpillWrite)
			})
			.transpose()?;
		let build_result = self
			.build_partitioned_streaming_scan_inner_build_reservoir(
				scan_input,
				build_morsel.or(morsel),
				partition_count,
				&partition_build_keys,
				&write_build_keys,
				key_plan,
				null_safe_keys,
				projection.as_ref(),
				0,
				false,
				residency_profile.as_deref_mut(),
				None,
				grouped_chain_observer.as_ref(),
				local_runtime_filter,
			)
			.await;
		let (mut reservoir, build_rows) = match build_result {
			Ok(Some(result)) => result,
			Ok(None) => {
				if let Some(profile) = residency_profile.as_deref_mut() {
					profile.terminal(&[], "reservoir_build_declined")?;
				}
				return Ok(false);
			}
			Err(error) => return Err(error),
		};
		let reservoir_spill_bytes = reservoir.iter().try_fold(0u64, |total, build| {
			total
				.checked_add(build.as_ref().map_or(0, SpilledInnerJoinBuild::spill_bytes))
				.ok_or_else(|| ExecutionError::Kernel {
					message: "grouped-chain profile: reservoir spill total overflow".to_string(),
				})
		})?;
		if let Some(timer) = reservoir_write_timer {
			timer.finish_with(build_rows, reservoir_spill_bytes)?;
		}
		if let Some(profile) = grouped_chain_profile {
			profile.record_reservoir_build_end_spill(GroupedChainProfile::task_spill_bytes(
				&self.context.task_telemetry,
			)?)?;
		}

		let mut total_probe_rows = 0u64;
		let mut total_output_rows = 0u64;
		let result = async {
			let Some(first_base_batch) = base_batches.first() else {
				return Ok::<(), ExecutionError>(());
			};
			if let Some(profile) = residency_profile.as_deref_mut() {
				profile.base_concat_intent(&reservoir)?;
			}
			let base_concat_timer = grouped_chain_profile
				.map(|profile| profile.begin_leaf(GroupedChainProfileLeaf::BaseConcat))
				.transpose()?;
			let base_batch = concat_or_empty(first_base_batch.schema(), base_batches)?;
			if let Some(timer) = base_concat_timer {
				timer.finish_with(
					base_batch.num_rows() as u64,
					record_batch_memory_bytes(&base_batch),
				)?;
			}
			if base_batch.num_rows() == 0 {
				return Ok(());
			}
			if let Some(profile) = residency_profile.as_deref_mut() {
				profile.phase(
					ProfilePhase::ReservoirProbe,
					&reservoir,
					"base_concat_complete",
				)?;
			}
			let reservoir_probe_timer = grouped_chain_profile
				.map(|profile| profile.begin_leaf(GroupedChainProfileLeaf::ReservoirReadProbe))
				.transpose()?;
			let (mut output, probe_rows, output_rows) = self
				.probe_partitioned_streamed_inner_scan_build_reservoir(
					&mut reservoir,
					partition_count,
					group_count,
					&base_batch,
					&probe_keys,
					&write_build_keys,
					key_plan,
					null_safe_keys,
					&materialized_output_schema,
					&output_schema,
					probe_columns_first,
					build_column_count,
					projection.as_ref(),
					0,
					None,
					grouped_chain_observer.as_ref(),
				)
				.await?;
			if let Some(profile) = residency_profile.as_deref_mut() {
				profile.phase(ProfilePhase::Suffix, &reservoir, "reservoir_probe_complete")?;
			}
			total_probe_rows = total_probe_rows.saturating_add(probe_rows);
			total_output_rows = total_output_rows.saturating_add(output_rows);
			if let Some(predicate) = post_filter {
				let mut filtered = Vec::new();
				for batch in output {
					let batch = self.filter_batch_with_bound_predicate(
						predicate,
						batch,
						"filter predicate",
					)?;
					if batch.num_rows() > 0 {
						filtered.push(batch);
					}
				}
				output = filtered;
			}
			if let Some(timer) = reservoir_probe_timer {
				timer.finish_with(probe_rows, output_rows)?;
			}
			if let Some(profile) = grouped_chain_profile {
				profile.record_reservoir_probe_end_spill(GroupedChainProfile::task_spill_bytes(
					&self.context.task_telemetry,
				)?)?;
			}
			self.process_reoriented_inner_chain_grouped_suffix_batch(
				output,
				suffix_runtimes,
				outer_predicate,
				project_exprs,
				project_output_schema,
				group_expr,
				aggr_expr,
				groups,
				group_types,
				grouped_chain_profile,
			)
			.await?;
			if let Some(profile) = grouped_chain_profile {
				profile.record_suffix_fold_end_spill(GroupedChainProfile::task_spill_bytes(
					&self.context.task_telemetry,
				)?)?;
			}
			if let Some(profile) = residency_profile.as_deref_mut() {
				profile.phase(ProfilePhase::Fold, &reservoir, "suffix_and_fold_complete")?;
			}
			Ok::<(), ExecutionError>(())
		}
		.await;
		if let Some(profile) = grouped_chain_profile {
			profile.record_reservoir(
				reservoir_spill_bytes,
				build_rows,
				total_probe_rows,
				total_output_rows,
			)?;
		}
		if let Some(profile) = residency_profile.as_deref_mut() {
			let status = if result.is_ok() {
				"complete"
			} else {
				"execution_error"
			};
			profile.terminal(&reservoir, status)?;
		}
		let cleanup_timer = grouped_chain_profile
			.map(|profile| profile.begin_leaf(GroupedChainProfileLeaf::Cleanup))
			.transpose()?;
		Self::cleanup_partitioned_streamed_inner_builds(reservoir).await;
		if let Some(timer) = cleanup_timer {
			timer.finish()?;
		}
		result?;
		if let ReorientedInnerJoinBuild::PartitionedScan {
			observed_build_rows,
			..
		} = &mut runtime.build
		{
			*observed_build_rows = observed_build_rows.saturating_add(build_rows);
		}
		self.record_reoriented_inner_probe(runtime, total_probe_rows, total_output_rows);
		Ok(true)
	}

	fn fold_reoriented_grouped_output_batch(
		&self,
		groups: &mut BoundGroupedAggregateGroups,
		group_types: &mut Vec<DataType>,
		outer_predicate: Option<&BoundExprProgram>,
		project_exprs: Option<&BoundExprProgram>,
		project_output_schema: Option<&arrow::datatypes::SchemaRef>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		batch: RecordBatch,
		grouped_chain_profile: Option<&GroupedChainProfile>,
	) -> Result<(), ExecutionError> {
		let filtered = if let Some(predicate) = outer_predicate {
			self.filter_batch_with_bound_predicate(predicate, batch, "filter predicate")?
		} else {
			batch
		};
		if filtered.num_rows() == 0 {
			return Ok(());
		}
		let aggregate_input = match (project_exprs, project_output_schema) {
			(Some(project_exprs), Some(project_output_schema)) => RecordBatch::try_new(
				project_output_schema.clone(),
				self.evaluate_bound_program(project_exprs, &filtered)?,
			)?,
			(None, None) => filtered,
			_ => {
				return Err(ExecutionError::InvalidExpression {
					message: "reoriented grouped projection metadata is incomplete".to_string(),
				});
			}
		};
		if let Some(profile) = grouped_chain_profile {
			profile.record_fold_input_rows(aggregate_input.num_rows())?;
		}
		self.fold_bound_grouped_aggregate_batch(
			groups,
			group_types,
			group_expr,
			aggr_expr,
			&aggregate_input,
		)
	}

	pub(in crate::exec) async fn try_streaming_inner_prefix_semi_anti_grouped_aggregate(
		&self,
		input: &ExecutionPlan,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		output_schema: &arrow::datatypes::SchemaRef,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
		let ExecutionPlan::Exchange {
			input: aggregate_input,
			..
		} = input
		else {
			return Ok(None);
		};
		let Some(mut chain) = extract_streaming_inner_prefix_semi_anti_chain(aggregate_input)
		else {
			return Ok(None);
		};
		let mut required_output = BTreeSet::new();
		required_output.extend(chain.semi_stage.preserved_keys.iter().copied());
		required_output.extend(chain.anti_stage.preserved_keys.iter().copied());
		if let Some(match_filter) = chain.semi_stage.match_filter {
			collect_left_side_program_columns(
				match_filter,
				chain.semi_stage.output_schema.fields().len(),
				&mut required_output,
			);
		}
		if let Some(match_filter) = chain.anti_stage.match_filter {
			collect_left_side_program_columns(
				match_filter,
				chain.anti_stage.output_schema.fields().len(),
				&mut required_output,
			);
		}
		collect_program_column_ordinals(group_expr, &mut required_output);
		collect_aggregate_columns(aggr_expr, &mut required_output);
		mark_reoriented_inner_stage_projections(&mut chain.inner_stages, required_output)?;
		let recognized_summaries =
			self.recognized_neq_summary_pair(&chain.semi_stage, &chain.anti_stage, morsel);

		let mut inner_runtimes: Vec<ReorientedInnerJoinRuntime<'_>> =
			Vec::with_capacity(chain.inner_stages.len());
		for stage in chain.inner_stages {
			let prepared = self
				.prepare_reoriented_inner_join_runtime_with_streamed_scan_build(stage, morsel)
				.await;
			let runtime = match prepared {
				Ok(Some(runtime)) => runtime,
				Ok(None) => {
					for runtime in inner_runtimes {
						runtime.cleanup().await;
					}
					return Ok(None);
				}
				Err(error) => {
					for runtime in inner_runtimes {
						runtime.cleanup().await;
					}
					return Err(error);
				}
			};
			inner_runtimes.push(runtime);
		}
		let inner_resident_bytes = inner_runtimes.iter().fold(0u64, |total, runtime| {
			total.saturating_add(runtime.retained_resident_bytes())
		});
		let (mut semi_runtime, mut anti_runtime) = if let Some((semi_columns, anti_columns)) =
			recognized_summaries
		{
			let semi = match self
				.prepare_recognized_neq_semi_anti_runtime(
					chain.semi_stage,
					semi_columns,
					morsel,
					inner_resident_bytes,
				)
				.await
			{
				Ok(runtime) => runtime,
				Err(error) => {
					for runtime in inner_runtimes {
						runtime.cleanup().await;
					}
					return Err(error);
				}
			};
			let anti_baseline = inner_resident_bytes.saturating_add(semi.retained_resident_bytes());
			let anti = match self
				.prepare_recognized_neq_semi_anti_runtime(
					chain.anti_stage,
					anti_columns,
					morsel,
					anti_baseline,
				)
				.await
			{
				Ok(runtime) => runtime,
				Err(error) => {
					semi.cleanup().await;
					for runtime in inner_runtimes {
						runtime.cleanup().await;
					}
					return Err(error);
				}
			};
			(semi, anti)
		} else {
			let semi = match self
				.prepare_streaming_semi_anti_runtime(chain.semi_stage, morsel)
				.await
			{
				Ok(Some(runtime)) => runtime,
				Ok(None) => {
					for runtime in inner_runtimes {
						runtime.cleanup().await;
					}
					return Ok(None);
				}
				Err(error) => {
					for runtime in inner_runtimes {
						runtime.cleanup().await;
					}
					return Err(error);
				}
			};
			let anti = match self
				.prepare_streaming_semi_anti_runtime(chain.anti_stage, morsel)
				.await
			{
				Ok(Some(runtime)) => runtime,
				Ok(None) => {
					semi.cleanup().await;
					for runtime in inner_runtimes {
						runtime.cleanup().await;
					}
					return Ok(None);
				}
				Err(error) => {
					semi.cleanup().await;
					for runtime in inner_runtimes {
						runtime.cleanup().await;
					}
					return Err(error);
				}
			};
			(semi, anti)
		};

		let execution_result = async {
			let mut groups = BoundGroupedAggregateGroups::new();
			let mut group_types = Vec::new();
			if self
				.try_streaming_inner_prefix_semi_anti_scan_probe_base(
					chain.base,
					&mut inner_runtimes,
					&mut semi_runtime,
					&mut anti_runtime,
					group_expr,
					aggr_expr,
					&mut groups,
					&mut group_types,
					morsel,
				)
				.await?
				.is_none()
			{
				let ExecutionPlan::Exchange {
					input: base_input,
					partition_count,
					key: base_exchange_key,
				} = chain.base
				else {
					return Ok(None);
				};
				let Some((join0, join0_filter)) =
					decompose_join_or_filtered_join(base_input.as_ref())
				else {
					return Ok(None);
				};
				let ExecutionPlan::Join {
					replay_id,
					left,
					right,
					kind: JoinKind::Inner,
					left_keys,
					right_keys,
					match_filter,
					null_safe_keys,
					strategy,
					output_schema: join0_output_schema,
					..
				} = join0
				else {
					return Ok(None);
				};
				if *replay_id != JoinReplayId(0) {
					return Ok(None);
				}

				let left_batches = self.execute_bound(left, morsel).await?;
				let right_batches = self.execute_bound(right, morsel).await?;
				let left_batch = concat_or_empty(left.schema(), &left_batches)?;
				let right_batch = concat_or_empty(right.schema(), &right_batches)?;
				let left_key_arrays = key_arrays(&left_batch, left_keys)?;
				let right_key_arrays = key_arrays(&right_batch, right_keys)?;
				if estimated_build_hash_bytes(&left_key_arrays, left_batch.num_rows())
					> self.context.task_budget.memory.get()
				{
					return Ok(None);
				}
				let join_key_plan = JoinKeyPlan::from_key_arrays(
					&left_key_arrays,
					&right_key_arrays,
					*null_safe_keys,
				);
				let build_started = Instant::now();
				let Some(build_rows) = build_in_memory_join_rows(
					join_key_plan,
					&left_key_arrays,
					left_batch.num_rows(),
					self.context.task_budget.memory.get(),
					*null_safe_keys,
				)?
				else {
					return Ok(None);
				};
				record_task_telemetry(&self.context.task_telemetry, |task| {
					task.counters.build_rows = task
						.counters
						.build_rows
						.saturating_add(left_batch.num_rows() as u64);
					task.counters.probe_rows = task
						.counters
						.probe_rows
						.saturating_add(right_batch.num_rows() as u64);
				});
				record_task_telemetry(&self.context.task_telemetry, |task| {
					task.phases.build_micros = task
						.phases
						.build_micros
						.saturating_add(duration_micros(build_started));
				});
				let probe_started = Instant::now();
				let selection = probe_in_memory_join_rows(
					JoinKind::Inner,
					join_key_plan,
					&build_rows,
					left_batch.num_rows(),
					&right_key_arrays,
					right_batch.num_rows(),
					match_filter.as_ref(),
					&left_batch,
					&right_batch,
					*null_safe_keys,
				)?;
				record_task_telemetry(&self.context.task_telemetry, |task| {
					task.phases.probe_micros = task
						.phases
						.probe_micros
						.saturating_add(duration_micros(probe_started));
					task.counters.matched_rows = task
						.counters
						.matched_rows
						.saturating_add(selection.len() as u64);
				});

				for chunk in selection.chunks(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
					let batch = materialize_join_selection(
						JoinKind::Inner,
						&left_batch,
						&right_batch,
						chunk,
						join0_output_schema,
						self.context.task_budget.memory.get(),
					)?;
					let batch = if let Some(join0_filter) = join0_filter {
						self.filter_batch_with_bound_predicate(
							join0_filter,
							batch,
							"filter predicate",
						)?
					} else {
						batch
					};
					if batch.num_rows() == 0 {
						continue;
					}
					for (_partition, partitioned) in self.repartition_exchange_batch(
						&batch,
						*partition_count,
						base_exchange_key,
					)? {
						if partitioned.num_rows() == 0 {
							continue;
						}
						let mut batches = vec![partitioned];
						for runtime in inner_runtimes.iter_mut() {
							let mut next = Vec::new();
							for batch in batches {
								next.extend(
									self.probe_reoriented_inner_join_streaming(runtime, &batch)
										.await?,
								);
							}
							batches = next;
							if let Some(predicate) = runtime.post_filter {
								let mut filtered = Vec::new();
								for batch in batches {
									let batch = self.filter_batch_with_bound_predicate(
										predicate,
										batch,
										"filter predicate",
									)?;
									if batch.num_rows() > 0 {
										filtered.push(batch);
									}
								}
								batches = filtered;
							}
							if batches.is_empty() {
								break;
							}
						}

						let mut semi_batches = Vec::new();
						for batch in batches {
							semi_batches.extend(
								self.probe_streaming_semi_anti(&mut semi_runtime, &batch)
									.await?,
							);
						}
						let mut anti_batches = Vec::new();
						for batch in semi_batches {
							anti_batches.extend(
								self.probe_streaming_semi_anti(&mut anti_runtime, &batch)
									.await?,
							);
						}
						for batch in anti_batches {
							self.fold_bound_grouped_aggregate_batch(
								&mut groups,
								&mut group_types,
								group_expr,
								aggr_expr,
								&batch,
							)?;
						}
					}
				}
				record_join_runtime_observation(
					&self.context.task_telemetry,
					&self.context.join_runtime_observation_mode,
					JoinRuntimeObservation {
						replay_id: *replay_id,
						strategy: *strategy,
						build_rows: RowCount::new(left_batch.num_rows() as u64),
						probe_rows: RowCount::new(right_batch.num_rows() as u64),
						output_rows: RowCount::new(selection.len() as u64),
					},
				);
			}

			for runtime in &inner_runtimes {
				record_join_runtime_observation(
					&self.context.task_telemetry,
					&self.context.join_runtime_observation_mode,
					JoinRuntimeObservation {
						replay_id: runtime.replay_id,
						strategy: runtime.strategy,
						build_rows: RowCount::new(runtime.build_row_count()),
						probe_rows: RowCount::new(runtime.probe_rows),
						output_rows: RowCount::new(runtime.output_rows),
					},
				);
			}
			for runtime in [&semi_runtime, &anti_runtime] {
				record_join_runtime_observation(
					&self.context.task_telemetry,
					&self.context.join_runtime_observation_mode,
					JoinRuntimeObservation {
						replay_id: runtime.replay_id,
						strategy: runtime.strategy,
						build_rows: RowCount::new(runtime.existence_row_count),
						probe_rows: RowCount::new(runtime.probe_rows),
						output_rows: RowCount::new(runtime.output_rows),
					},
				);
			}

			let output = self.finalize_bound_grouped_aggregate(
				&mut groups,
				&group_types,
				aggr_expr,
				output_schema,
			);
			output.map(Some)
		}
		.await;
		for runtime in inner_runtimes {
			runtime.cleanup().await;
		}
		semi_runtime.cleanup().await;
		anti_runtime.cleanup().await;
		execution_result
	}

	fn partition_at_a_time_replay_spec<'a>(
		inner_runtimes: &[ReorientedInnerJoinRuntime<'a>],
		semi_runtime: &StreamingSemiAntiRuntime<'a>,
		anti_runtime: &StreamingSemiAntiRuntime<'a>,
	) -> Option<PartitionAtATimeReplaySpec<'a>> {
		if inner_runtimes.len() != 2
			|| inner_runtimes[0].replay_id != JoinReplayId(1)
			|| inner_runtimes[1].replay_id != JoinReplayId(2)
			|| semi_runtime.replay_id != JoinReplayId(3)
			|| anti_runtime.replay_id != JoinReplayId(4)
			|| !matches!(
				semi_runtime.existence,
				StreamingSemiAntiExistence::RecognizedNeqSummary { .. }
			) || !matches!(
			anti_runtime.existence,
			StreamingSemiAntiExistence::RecognizedNeqSummary { .. }
		) {
			return None;
		}
		let runtime = &inner_runtimes[0];
		let ReorientedInnerJoinBuild::PartitionedScan {
			scan_input,
			morsel,
			partition_count,
			partition_build_keys,
			write_build_keys,
			materialized_output_schema,
			build_column_count,
			projection,
			estimated_build_bytes,
			loaded_builds,
			loaded_build_attempted,
			..
		} = &runtime.build
		else {
			return None;
		};
		if *partition_count <= 1
			|| *loaded_build_attempted
			|| loaded_builds.iter().any(Option::is_some)
			|| !matches!(runtime.key_plan, JoinKeyPlan::Int64)
		{
			return None;
		}
		Some(PartitionAtATimeReplaySpec {
			scan_input: *scan_input,
			morsel: *morsel,
			partition_count: *partition_count,
			partition_build_keys: partition_build_keys.clone(),
			write_build_keys: write_build_keys.clone(),
			materialized_output_schema: materialized_output_schema.clone(),
			build_column_count: *build_column_count,
			projection: projection.clone(),
			estimated_build_bytes: *estimated_build_bytes,
			key_plan: runtime.key_plan,
			probe_keys: runtime.probe_keys.clone(),
			output_schema: runtime.output_schema.clone(),
			post_filter: runtime.post_filter,
			probe_columns_first: runtime.probe_columns_first,
		})
	}

	async fn start_partition_at_a_time_replay_reservoir(
		&self,
		spec: &PartitionAtATimeReplaySpec<'_>,
		morsel: Option<(MorselId, u32)>,
		runtime_baseline_bytes: u64,
	) -> Result<PartitionAtATimeReplayReservoir, ExecutionError> {
		let build_result = self
			.build_partitioned_streaming_scan_inner_build_reservoir(
				spec.scan_input,
				spec.morsel.or(morsel),
				spec.partition_count,
				&spec.partition_build_keys,
				&spec.write_build_keys,
				spec.key_plan,
				false,
				spec.projection.as_ref(),
				runtime_baseline_bytes,
				true,
				None,
				None,
				None,
				None,
			)
			.await?;
		let Some((build, build_rows)) = build_result else {
			return Err(ExecutionError::InvalidExpression {
				message: "committed partition replay build reservoir declined".to_string(),
			});
		};
		let build_spill_bytes = build.iter().flatten().fold(0u64, |bytes, partition| {
			bytes.saturating_add(partition.spill_bytes())
		});
		let partition_count =
			usize::try_from(spec.partition_count).map_err(|_| ExecutionError::Kernel {
				message: "partition-at-a-time replay partition count exceeds usize".to_string(),
			})?;
		let build_resident_bytes = partition_replay_reservoir_bytes(&build, build.capacity());
		let mut probe: Vec<Option<SpilledInnerJoinBuild>> =
			(0..partition_count).map(|_| None).collect();
		for slot in &mut probe {
			let created = SpilledInnerJoinBuild::create_with_resident_baseline(
				self.context.task_budget,
				self.context.join_spill_file_ops.clone(),
				self.context.join_spill_dir.clone(),
				runtime_baseline_bytes.saturating_add(build_resident_bytes),
			)
			.await;
			match created {
				Ok(created) => *slot = Some(created),
				Err(error) => {
					Self::cleanup_partitioned_streamed_inner_builds(probe).await;
					Self::cleanup_partitioned_streamed_inner_builds(build).await;
					return Err(error);
				}
			}
		}
		guard_partition_replay_owner_resident(
			runtime_baseline_bytes,
			build_resident_bytes,
			partition_replay_reservoir_bytes(&probe, probe.capacity()),
			0,
			0,
			0,
			0,
			0,
			self.context.task_budget.memory.get(),
		)?;
		Ok(PartitionAtATimeReplayReservoir {
			build,
			probe,
			build_rows,
			build_spill_bytes,
			probe_spill_bytes: 0,
		})
	}

	async fn buffer_partition_at_a_time_replay_probe(
		&self,
		spec: &PartitionAtATimeReplaySpec<'_>,
		state: &mut PartitionAtATimeReplayReservoir,
		batch: &RecordBatch,
		runtime_baseline_bytes: u64,
	) -> Result<(), ExecutionError> {
		let build_resident_bytes =
			partition_replay_reservoir_bytes(&state.build, state.build.capacity());
		let probe_partition_count = state.probe.len();
		self.write_partitioned_streaming_scan_inner_build_batch(
			&mut state.probe,
			&mut state.probe_spill_bytes,
			batch,
			None,
			&spec.probe_keys,
			&spec.probe_keys,
			spec.partition_count,
			spec.key_plan,
			false,
			0,
			probe_partition_count,
			false,
			Some(runtime_baseline_bytes.saturating_add(build_resident_bytes)),
			None,
		)
		.await?;
		Ok(())
	}

	async fn finish_partition_at_a_time_probe_reservoir(
		&self,
		spec: &PartitionAtATimeReplaySpec<'_>,
		state: &mut PartitionAtATimeReplayReservoir,
		runtime_baseline_bytes: u64,
	) -> Result<(), ExecutionError> {
		for partition_index in 0..state.probe.len() {
			if let Some(partition) = state.probe[partition_index].as_mut() {
				// This is the probe reservoir's all-writes-complete barrier, the analogue of the
				// build side's in `write_partitioned_streaming_scan_inner_build_batches`. Probe
				// batches arrive through the buffering writer, which holds a partial final page
				// back until a caller seals it.
				partition
					.seal_unindexed_reservoir_partition(
						spec.key_plan,
						&spec.probe_keys,
						false,
						self.context.task_budget,
						None,
					)
					.await?;
			}
			guard_partition_replay_owner_resident(
				runtime_baseline_bytes,
				partition_replay_reservoir_bytes(&state.build, state.build.capacity()),
				partition_replay_reservoir_bytes(&state.probe, state.probe.capacity()),
				0,
				0,
				0,
				0,
				0,
				self.context.task_budget.memory.get(),
			)?;
		}
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	async fn drain_partition_at_a_time_replay(
		&self,
		spec: &PartitionAtATimeReplaySpec<'_>,
		state: &mut PartitionAtATimeReplayReservoir,
		runtime: &mut ReorientedInnerJoinRuntime<'_>,
		suffix_runtimes: &mut [ReorientedInnerJoinRuntime<'_>],
		semi_runtime: &mut StreamingSemiAntiRuntime<'_>,
		anti_runtime: &mut StreamingSemiAntiRuntime<'_>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BoundGroupedAggregateGroups,
		group_types: &mut Vec<DataType>,
		stage_profile: &mut Option<ChainStageProfiler>,
		runtime_baseline_bytes: u64,
	) -> Result<(), ExecutionError> {
		let partition_count = state.build.len();
		let mut total_probe_rows = 0u64;
		let mut total_output_rows = 0u64;
		for partition in 0..partition_count {
			if let Some(profile) = stage_profile.as_mut() {
				profile.begin_deferred_partition(partition)?;
				profile.flush()?;
			}
			let mut active_build = state.build.get_mut(partition).and_then(Option::take);
			let mut active_probe = state.probe.get_mut(partition).and_then(Option::take);
			let partition_result = async {
				let (Some(build), Some(probe)) = (active_build.as_mut(), active_probe.as_mut())
				else {
					return Ok::<(), ExecutionError>(());
				};
				if build.input_row_count() == 0 || probe.input_row_count() == 0 {
					return Ok(());
				}
				let build_reservoir_bytes =
					partition_replay_reservoir_bytes(&state.build, state.build.capacity());
				let probe_reservoir_bytes =
					partition_replay_reservoir_bytes(&state.probe, state.probe.capacity());
				let fold_bytes = grouped_fold_resident_bytes(groups, group_types.capacity());
				let active_probe_bytes = probe.owner_retained_resident_bytes();
				let active_build_metadata = build.owner_retained_resident_bytes();
				let active_baseline = runtime_baseline_bytes
					.saturating_add(build_reservoir_bytes)
					.saturating_add(probe_reservoir_bytes)
					.saturating_add(active_probe_bytes)
					.saturating_add(active_build_metadata)
					.saturating_add(fold_bytes);
				let index_started = stage_profile.as_ref().map(|_| Instant::now());
				let stats = build
					.rebuild_compact_index_and_pin_pages(
						spec.key_plan,
						&spec.write_build_keys,
						false,
						self.context.task_budget,
						active_baseline,
					)
					.await?;
				if let (Some(profile), Some(started)) = (stage_profile.as_mut(), index_started) {
					profile.add_elapsed(ChainStage::InnerReplay1, started)?;
					profile.record_deferred_index(stats)?;
					profile.flush()?;
				}
				guard_partition_replay_owner_resident(
					runtime_baseline_bytes,
					build_reservoir_bytes,
					probe_reservoir_bytes,
					build.owner_retained_resident_bytes(),
					probe.owner_retained_resident_bytes(),
					0,
					0,
					fold_bytes,
					self.context.task_budget.memory.get(),
				)?;

				for page_index in 0..probe.page_count() {
					let replay_started = stage_profile.as_ref().map(|_| Instant::now());
					let probe_page = probe.read_batch_page(page_index).await?;
					let probe_page_bytes = record_batch_memory_bytes(&probe_page);
					guard_partition_replay_owner_resident(
						runtime_baseline_bytes,
						build_reservoir_bytes,
						probe_reservoir_bytes,
						build.owner_retained_resident_bytes(),
						probe.owner_retained_resident_bytes(),
						probe_page_bytes,
						0,
						grouped_fold_resident_bytes(groups, group_types.capacity()),
						self.context.task_budget.memory.get(),
					)?;
					let mut batches = Vec::new();
					let (probe_rows, output_rows) = self
						.visit_streaming_spilled_inner_probe_batch(
							build,
							&probe_page,
							&spec.probe_keys,
							spec.key_plan,
							false,
							&spec.materialized_output_schema,
							&spec.output_schema,
							None,
							spec.probe_columns_first,
							spec.build_column_count,
							spec.projection.as_ref(),
							None,
							None,
							&mut |batch| {
								batches.push(batch);
								Ok(())
							},
						)
						.await?;
					total_probe_rows = total_probe_rows.saturating_add(probe_rows);
					total_output_rows = total_output_rows.saturating_add(output_rows);
					if let Some(predicate) = spec.post_filter {
						let mut filtered = Vec::new();
						for batch in batches {
							let batch = self.filter_batch_with_bound_predicate(
								predicate,
								batch,
								"filter predicate",
							)?;
							if batch.num_rows() > 0 {
								filtered.push(batch);
							}
						}
						batches = filtered;
					}
					if let (Some(profile), Some(started)) = (stage_profile.as_mut(), replay_started)
					{
						profile.add_elapsed(ChainStage::InnerReplay1, started)?;
						let replay_output_rows = batches
							.iter()
							.map(|batch| batch.num_rows() as u64)
							.fold(0u64, u64::saturating_add);
						profile.add_flow(
							ChainStage::InnerReplay1,
							0,
							batches.len() as u64,
							0,
							replay_output_rows,
						)?;
					}
					guard_partition_replay_owner_resident(
						runtime_baseline_bytes,
						build_reservoir_bytes,
						probe_reservoir_bytes,
						build.owner_retained_resident_bytes(),
						probe.owner_retained_resident_bytes(),
						probe_page_bytes,
						record_batches_resident_bytes(&batches, batches.capacity()),
						grouped_fold_resident_bytes(groups, group_types.capacity()),
						self.context.task_budget.memory.get(),
					)?;

					for suffix_runtime in suffix_runtimes.iter_mut() {
						let stage = (suffix_runtime.replay_id == JoinReplayId(2))
							.then_some(ChainStage::InnerReplay2);
						let input_rows = batches
							.iter()
							.map(|batch| batch.num_rows() as u64)
							.fold(0u64, u64::saturating_add);
						let input_batches = batches.len() as u64;
						let started = stage
							.filter(|_| stage_profile.is_some())
							.map(|_| Instant::now());
						let mut next = Vec::new();
						for batch in batches {
							next.extend(
								self.probe_reoriented_inner_join_streaming(suffix_runtime, &batch)
									.await?,
							);
						}
						batches = next;
						if let Some(predicate) = suffix_runtime.post_filter {
							let mut filtered = Vec::new();
							for batch in batches {
								let batch = self.filter_batch_with_bound_predicate(
									predicate,
									batch,
									"filter predicate",
								)?;
								if batch.num_rows() > 0 {
									filtered.push(batch);
								}
							}
							batches = filtered;
						}
						if let (Some(profile), Some(stage), Some(started)) =
							(stage_profile.as_mut(), stage, started)
						{
							profile.add_elapsed(stage, started)?;
							let output_rows = batches
								.iter()
								.map(|batch| batch.num_rows() as u64)
								.fold(0u64, u64::saturating_add);
							profile.add_flow(
								stage,
								input_batches,
								batches.len() as u64,
								input_rows,
								output_rows,
							)?;
						}
						guard_partition_replay_owner_resident(
							runtime_baseline_bytes,
							build_reservoir_bytes,
							probe_reservoir_bytes,
							build.owner_retained_resident_bytes(),
							probe.owner_retained_resident_bytes(),
							probe_page_bytes,
							record_batches_resident_bytes(&batches, batches.capacity()),
							grouped_fold_resident_bytes(groups, group_types.capacity()),
							self.context.task_budget.memory.get(),
						)?;
						if batches.is_empty() {
							break;
						}
					}

					let semi_input_rows = batches
						.iter()
						.map(|batch| batch.num_rows() as u64)
						.fold(0u64, u64::saturating_add);
					let semi_input_batches = batches.len() as u64;
					let semi_started = stage_profile.as_ref().map(|_| Instant::now());
					let mut semi_batches = Vec::new();
					for batch in batches {
						semi_batches
							.extend(self.probe_streaming_semi_anti(semi_runtime, &batch).await?);
					}
					if let (Some(profile), Some(started)) = (stage_profile.as_mut(), semi_started) {
						profile.add_elapsed(ChainStage::SemiReplay3, started)?;
						let rows = semi_batches
							.iter()
							.map(|batch| batch.num_rows() as u64)
							.fold(0u64, u64::saturating_add);
						profile.add_flow(
							ChainStage::SemiReplay3,
							semi_input_batches,
							semi_batches.len() as u64,
							semi_input_rows,
							rows,
						)?;
					}
					guard_partition_replay_owner_resident(
						runtime_baseline_bytes,
						build_reservoir_bytes,
						probe_reservoir_bytes,
						build.owner_retained_resident_bytes(),
						probe.owner_retained_resident_bytes(),
						probe_page_bytes,
						record_batches_resident_bytes(&semi_batches, semi_batches.capacity()),
						grouped_fold_resident_bytes(groups, group_types.capacity()),
						self.context.task_budget.memory.get(),
					)?;
					let anti_input_rows = semi_batches
						.iter()
						.map(|batch| batch.num_rows() as u64)
						.fold(0u64, u64::saturating_add);
					let anti_input_batches = semi_batches.len() as u64;
					let anti_started = stage_profile.as_ref().map(|_| Instant::now());
					let mut anti_batches = Vec::new();
					for batch in semi_batches {
						anti_batches
							.extend(self.probe_streaming_semi_anti(anti_runtime, &batch).await?);
					}
					if let (Some(profile), Some(started)) = (stage_profile.as_mut(), anti_started) {
						profile.add_elapsed(ChainStage::AntiReplay4, started)?;
						let rows = anti_batches
							.iter()
							.map(|batch| batch.num_rows() as u64)
							.fold(0u64, u64::saturating_add);
						profile.add_flow(
							ChainStage::AntiReplay4,
							anti_input_batches,
							anti_batches.len() as u64,
							anti_input_rows,
							rows,
						)?;
					}
					guard_partition_replay_owner_resident(
						runtime_baseline_bytes,
						build_reservoir_bytes,
						probe_reservoir_bytes,
						build.owner_retained_resident_bytes(),
						probe.owner_retained_resident_bytes(),
						probe_page_bytes,
						record_batches_resident_bytes(&anti_batches, anti_batches.capacity()),
						grouped_fold_resident_bytes(groups, group_types.capacity()),
						self.context.task_budget.memory.get(),
					)?;
					let non_fold_resident_bytes = runtime_baseline_bytes
						.saturating_add(build_reservoir_bytes)
						.saturating_add(probe_reservoir_bytes)
						.saturating_add(build.owner_retained_resident_bytes())
						.saturating_add(probe.owner_retained_resident_bytes())
						.saturating_add(probe_page_bytes)
						.saturating_add(record_batches_resident_bytes(
							&anti_batches,
							anti_batches.capacity(),
						))
						.saturating_add(summary_bytes::<DataType>(group_types.capacity()));
					let retained_cap = join_spill_retained_footprint_memory_cap(
						self.context.task_budget.memory.get(),
					);
					groups.set_growth_budget_bytes(Some(
						retained_cap.saturating_sub(non_fold_resident_bytes),
					));
					for batch in anti_batches {
						let fold_rows = batch.num_rows() as u64;
						let fold_started = stage_profile.as_ref().map(|_| Instant::now());
						self.fold_bound_grouped_aggregate_batch(
							groups,
							group_types,
							group_expr,
							aggr_expr,
							&batch,
						)?;
						if let (Some(profile), Some(started)) =
							(stage_profile.as_mut(), fold_started)
						{
							profile.add_elapsed(ChainStage::GroupedFold, started)?;
							profile.add_flow(
								ChainStage::GroupedFold,
								1,
								1,
								fold_rows,
								fold_rows,
							)?;
						}
					}
					groups.set_growth_budget_bytes(None);
					guard_partition_replay_owner_resident(
						runtime_baseline_bytes,
						build_reservoir_bytes,
						probe_reservoir_bytes,
						build.owner_retained_resident_bytes(),
						probe.owner_retained_resident_bytes(),
						probe_page_bytes,
						0,
						grouped_fold_resident_bytes(groups, group_types.capacity()),
						self.context.task_budget.memory.get(),
					)?;
					drop(probe_page);
					probe.clear_read_cache();
					if let Some(profile) = stage_profile.as_mut() {
						profile.complete_deferred_probe_page()?;
						if profile.deferred_probe_page_flush_due() {
							profile.flush()?;
						}
					}
				}
				Ok(())
			}
			.await;
			if let Some(build) = active_build {
				build.cleanup().await;
			}
			if let Some(probe) = active_probe {
				probe.cleanup().await;
			}
			partition_result?;
			if let Some(profile) = stage_profile.as_mut() {
				profile.complete_deferred_partition()?;
				profile.flush()?;
			}
		}
		if let ReorientedInnerJoinBuild::PartitionedScan {
			observed_build_rows,
			..
		} = &mut runtime.build
		{
			*observed_build_rows = observed_build_rows.saturating_add(state.build_rows);
		}
		self.record_reoriented_inner_probe(runtime, total_probe_rows, total_output_rows);
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	async fn try_streaming_inner_prefix_semi_anti_scan_probe_base(
		&self,
		base: &ExecutionPlan,
		inner_runtimes: &mut [ReorientedInnerJoinRuntime<'_>],
		semi_runtime: &mut StreamingSemiAntiRuntime<'_>,
		anti_runtime: &mut StreamingSemiAntiRuntime<'_>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BoundGroupedAggregateGroups,
		group_types: &mut Vec<DataType>,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<()>, ExecutionError> {
		let ExecutionPlan::Exchange {
			input: base_input,
			partition_count,
			key: base_exchange_key,
		} = base
		else {
			return Ok(None);
		};
		let Some((join0, join0_filter)) = decompose_join_or_filtered_join(base_input.as_ref())
		else {
			return Ok(None);
		};
		let ExecutionPlan::Join {
			replay_id,
			left,
			right,
			kind: JoinKind::Inner,
			left_keys,
			right_keys,
			match_filter,
			null_safe_keys,
			strategy,
			output_schema: join0_output_schema,
			..
		} = join0
		else {
			return Ok(None);
		};
		if *replay_id != JoinReplayId(0) {
			return Ok(None);
		}
		let Some(scan_input) = decompose_streaming_scan_probe_input(right) else {
			return Ok(None);
		};

		let left_batches = self.execute_bound(left, morsel).await?;
		let left_batch = concat_or_empty(left.schema(), &left_batches)?;
		let left_key_arrays = key_arrays(&left_batch, left_keys)?;
		if estimated_build_hash_bytes(&left_key_arrays, left_batch.num_rows())
			> self.context.task_budget.memory.get()
		{
			return Ok(None);
		}
		let join_key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&left.schema(),
			left_keys,
			&right.schema(),
			right_keys,
			*null_safe_keys,
		)?;
		let build_started = Instant::now();
		let Some(build_rows) = build_in_memory_join_rows(
			join_key_plan,
			&left_key_arrays,
			left_batch.num_rows(),
			self.context.task_budget.memory.get(),
			*null_safe_keys,
		)?
		else {
			return Ok(None);
		};
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.build_rows = task
				.counters
				.build_rows
				.saturating_add(left_batch.num_rows() as u64);
			task.phases.build_micros = task
				.phases
				.build_micros
				.saturating_add(duration_micros(build_started));
		});

		let Some(req) = self.bound_scan_request(scan_input.scan, morsel) else {
			return Ok(None);
		};
		let mut remaining = req.row_limit;
		let span = crate::telemetry::storage_scan_span(&req);
		let runtime_baseline_bytes = inner_runtimes
			.iter()
			.map(ReorientedInnerJoinRuntime::retained_resident_bytes)
			.chain([
				semi_runtime.retained_resident_bytes(),
				anti_runtime.retained_resident_bytes(),
			])
			.fold(0u64, u64::saturating_add);
		let mut partition_replay_spec =
			Self::partition_at_a_time_replay_spec(inner_runtimes, semi_runtime, anti_runtime);
		if let Some(spec) = partition_replay_spec.as_ref() {
			let partition_count = u64::from(spec.partition_count).max(1);
			let estimated_partition_bytes =
				ceil_div_u64(spec.estimated_build_bytes, partition_count);
			let estimated_rows = ceil_div_u64(
				ceil_div_u64(spec.estimated_build_bytes, 120),
				partition_count,
			);
			let estimated_index_bytes = estimate_loaded_partition_index_bytes(
				estimated_rows,
				spec.key_plan,
				estimated_partition_bytes,
			);
			let handles = summary_bytes::<Option<SpilledInnerJoinBuild>>(
				usize::try_from(spec.partition_count).unwrap_or(usize::MAX),
			)
			.saturating_mul(2);
			if guard_partition_replay_owner_resident(
				runtime_baseline_bytes,
				handles,
				0,
				estimated_index_bytes.saturating_add(estimated_partition_bytes),
				0,
				64 * 1024,
				0,
				0,
				self.context.task_budget.memory.get(),
			)
			.is_err()
			{
				partition_replay_spec = None;
			}
		}
		let mut stage_profile = ChainStageProfiler::from_env();
		let mut partition_replay_state: Option<PartitionAtATimeReplayReservoir> = None;
		if let Some(profile) = stage_profile.as_mut() {
			profile.flush()?;
		}
		let scan_started = stage_profile.as_ref().map(|_| Instant::now());
		let scan_result: Result<_, ExecutionError> = async {
			Ok(self
				.storage
				.scan(req)
				.instrument(span)
				.await?
				.into_batches())
		}
		.await;
		if let (Some(profile), Some(started)) = (stage_profile.as_mut(), scan_started) {
			profile.add_elapsed(ChainStage::BaseScan, started)?;
		}
		let mut stream = match scan_result {
			Ok(stream) => stream,
			Err(error) => {
				if let Some(profile) = stage_profile.as_mut() {
					profile.mark_terminal_error(&error);
					let _ = profile.flush();
				}
				return Err(error);
			}
		};
		let probe_started = Instant::now();
		let mut probe_rows = 0u64;
		let mut output_rows = 0u64;
		let drive_result: Result<(), ExecutionError> = async {
			loop {
				let base_started = stage_profile.as_ref().map(|_| Instant::now());
				let next_batch = stream.next().await;
				let Some(batch) = next_batch else {
					if let (Some(profile), Some(started)) = (stage_profile.as_mut(), base_started) {
						profile.add_elapsed(ChainStage::BaseScan, started)?;
					}
					break;
				};
				let base_result: Result<Option<(u64, RecordBatch)>, ExecutionError> = (|| {
					let raw_batch = batch?;
					let raw_rows = raw_batch.num_rows() as u64;
					let Some(mut right_batch) =
						limit_streaming_scan_batch(raw_batch, &mut remaining)
					else {
						return Ok(None);
					};
					if let Some(predicate) = scan_input.filter_predicate {
						right_batch = self.filter_batch_with_bound_predicate(
							predicate,
							right_batch,
							"filter predicate",
						)?;
					}
					Ok(Some((raw_rows, right_batch)))
				})();
				if let (Some(profile), Some(started)) = (stage_profile.as_mut(), base_started) {
					profile.add_elapsed(ChainStage::BaseScan, started)?;
				}
				let Some((raw_rows, right_batch)) = base_result? else {
					break;
				};
				let post_filter_rows = right_batch.num_rows() as u64;
				if let Some(profile) = stage_profile.as_mut() {
					profile.add_flow(ChainStage::BaseScan, 1, 1, raw_rows, post_filter_rows)?;
				}

				if right_batch.num_rows() == 0 {
					if let Some(profile) = stage_profile.as_mut() {
						profile.complete_base_batch(raw_rows, post_filter_rows)?;
						if profile.base_batch_flush_due() {
							profile.flush()?;
						}
					}
					continue;
				}
				probe_rows = probe_rows.saturating_add(right_batch.num_rows() as u64);
				let replay0_started = stage_profile.as_ref().map(|_| Instant::now());
				let selection_result: Result<_, ExecutionError> = (|| {
					let right_key_arrays = key_arrays(&right_batch, right_keys)?;
					probe_in_memory_join_rows(
						JoinKind::Inner,
						join_key_plan,
						&build_rows,
						left_batch.num_rows(),
						&right_key_arrays,
						right_batch.num_rows(),
						match_filter.as_ref(),
						&left_batch,
						&right_batch,
						*null_safe_keys,
					)
				})();
				if let (Some(profile), Some(started)) = (stage_profile.as_mut(), replay0_started) {
					profile.add_elapsed(ChainStage::InnerReplay0, started)?;
				}
				let selection = selection_result?;
				output_rows = output_rows.saturating_add(selection.len() as u64);
				let mut replay0_output_rows = 0u64;
				let mut replay0_output_batches = 0u64;

				for chunk in selection.chunks(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
					let replay0_started = stage_profile.as_ref().map(|_| Instant::now());
					let partitioned_result: Result<_, ExecutionError> = (|| {
						let batch = materialize_join_selection(
							JoinKind::Inner,
							&left_batch,
							&right_batch,
							chunk,
							join0_output_schema,
							self.context.task_budget.memory.get(),
						)?;
						let batch = if let Some(join0_filter) = join0_filter {
							self.filter_batch_with_bound_predicate(
								join0_filter,
								batch,
								"filter predicate",
							)?
						} else {
							batch
						};
						if batch.num_rows() == 0 {
							Ok(Vec::new())
						} else {
							self.repartition_exchange_batch(
								&batch,
								*partition_count,
								base_exchange_key,
							)
						}
					})();
					if let (Some(profile), Some(started)) =
						(stage_profile.as_mut(), replay0_started)
					{
						profile.add_elapsed(ChainStage::InnerReplay0, started)?;
					}
					let partitioned_batches = partitioned_result?;

					for (_partition, partitioned) in partitioned_batches {
						if partitioned.num_rows() == 0 {
							continue;
						}
						replay0_output_rows =
							replay0_output_rows.saturating_add(partitioned.num_rows() as u64);
						replay0_output_batches = replay0_output_batches.saturating_add(1);
						if let Some(spec) = partition_replay_spec.as_ref() {
							let replay1_started = stage_profile.as_ref().map(|_| Instant::now());
							if partition_replay_state.is_none() {
								let state =
									Box::pin(self.start_partition_at_a_time_replay_reservoir(
										spec,
										morsel,
										runtime_baseline_bytes,
									))
									.await?;
								if let Some(profile) = stage_profile.as_mut() {
									profile.begin_deferred_replay(
										state.build.len(),
										state.build_spill_bytes,
									)?;
									profile.flush()?;
								}
								partition_replay_state = Some(state);
							}
							let buffered_rows = partitioned.num_rows() as u64;
							let state = partition_replay_state.as_mut().ok_or_else(|| {
								ExecutionError::InvalidExpression {
									message: "committed partition replay lost its reservoir state"
										.to_string(),
								}
							})?;
							Box::pin(self.buffer_partition_at_a_time_replay_probe(
								spec,
								state,
								&partitioned,
								runtime_baseline_bytes,
							))
							.await?;
							let probe_spill_bytes = state.probe_spill_bytes;
							if let (Some(profile), Some(started)) =
								(stage_profile.as_mut(), replay1_started)
							{
								profile.add_elapsed(ChainStage::InnerReplay1, started)?;
								profile.add_flow(
									ChainStage::InnerReplay1,
									1,
									0,
									buffered_rows,
									0,
								)?;
								profile.add_deferred_probe_rows(buffered_rows)?;
								profile.set_deferred_probe_spill_bytes(probe_spill_bytes);
							}
							continue;
						}
						let mut batches = vec![partitioned];
						for runtime in inner_runtimes.iter_mut() {
							let stage = match runtime.replay_id {
								JoinReplayId(1) => Some(ChainStage::InnerReplay1),
								JoinReplayId(2) => Some(ChainStage::InnerReplay2),
								_ => None,
							};
							let input_rows = batches
								.iter()
								.map(|batch| batch.num_rows() as u64)
								.fold(0u64, u64::saturating_add);
							let input_batches = batches.len() as u64;
							let mut next = Vec::new();
							for batch in batches {
								let started = stage
									.filter(|_| stage_profile.is_some())
									.map(|_| Instant::now());
								let probe_result = self
									.probe_reoriented_inner_join_streaming(runtime, &batch)
									.await;
								if let (Some(profile), Some(stage), Some(started)) =
									(stage_profile.as_mut(), stage, started)
								{
									profile.add_elapsed(stage, started)?;
								}
								next.extend(probe_result?);
							}
							batches = next;
							if let Some(predicate) = runtime.post_filter {
								let started = stage
									.filter(|_| stage_profile.is_some())
									.map(|_| Instant::now());
								let filter_result: Result<_, ExecutionError> = (|| {
									let mut filtered = Vec::new();
									for batch in batches {
										let batch = self.filter_batch_with_bound_predicate(
											predicate,
											batch,
											"filter predicate",
										)?;
										if batch.num_rows() > 0 {
											filtered.push(batch);
										}
									}
									Ok(filtered)
								})();
								if let (Some(profile), Some(stage), Some(started)) =
									(stage_profile.as_mut(), stage, started)
								{
									profile.add_elapsed(stage, started)?;
								}
								batches = filter_result?;
							}
							if let (Some(profile), Some(stage)) = (stage_profile.as_mut(), stage) {
								let stage_output_rows = batches
									.iter()
									.map(|batch| batch.num_rows() as u64)
									.fold(0u64, u64::saturating_add);
								profile.add_flow(
									stage,
									input_batches,
									batches.len() as u64,
									input_rows,
									stage_output_rows,
								)?;
							}
							if batches.is_empty() {
								break;
							}
						}

						let semi_input_rows = batches
							.iter()
							.map(|batch| batch.num_rows() as u64)
							.fold(0u64, u64::saturating_add);
						let semi_input_batches = batches.len() as u64;
						let mut semi_batches = Vec::new();
						for batch in batches {
							let started = stage_profile.as_ref().map(|_| Instant::now());
							let probe_result =
								self.probe_streaming_semi_anti(semi_runtime, &batch).await;
							if let (Some(profile), Some(started)) =
								(stage_profile.as_mut(), started)
							{
								profile.add_elapsed(ChainStage::SemiReplay3, started)?;
							}
							semi_batches.extend(probe_result?);
						}
						if let Some(profile) = stage_profile.as_mut() {
							let semi_output_rows = semi_batches
								.iter()
								.map(|batch| batch.num_rows() as u64)
								.fold(0u64, u64::saturating_add);
							profile.add_flow(
								ChainStage::SemiReplay3,
								semi_input_batches,
								semi_batches.len() as u64,
								semi_input_rows,
								semi_output_rows,
							)?;
						}

						let anti_input_rows = semi_batches
							.iter()
							.map(|batch| batch.num_rows() as u64)
							.fold(0u64, u64::saturating_add);
						let anti_input_batches = semi_batches.len() as u64;
						let mut anti_batches = Vec::new();
						for batch in semi_batches {
							let started = stage_profile.as_ref().map(|_| Instant::now());
							let probe_result =
								self.probe_streaming_semi_anti(anti_runtime, &batch).await;
							if let (Some(profile), Some(started)) =
								(stage_profile.as_mut(), started)
							{
								profile.add_elapsed(ChainStage::AntiReplay4, started)?;
							}
							anti_batches.extend(probe_result?);
						}
						if let Some(profile) = stage_profile.as_mut() {
							let anti_output_rows = anti_batches
								.iter()
								.map(|batch| batch.num_rows() as u64)
								.fold(0u64, u64::saturating_add);
							profile.add_flow(
								ChainStage::AntiReplay4,
								anti_input_batches,
								anti_batches.len() as u64,
								anti_input_rows,
								anti_output_rows,
							)?;
						}

						for batch in anti_batches {
							let fold_rows = batch.num_rows() as u64;
							let started = stage_profile.as_ref().map(|_| Instant::now());
							let fold_result = self.fold_bound_grouped_aggregate_batch(
								groups,
								group_types,
								group_expr,
								aggr_expr,
								&batch,
							);
							if let (Some(profile), Some(started)) =
								(stage_profile.as_mut(), started)
							{
								profile.add_elapsed(ChainStage::GroupedFold, started)?;
							}
							fold_result?;
							if let Some(profile) = stage_profile.as_mut() {
								profile.add_flow(
									ChainStage::GroupedFold,
									1,
									1,
									fold_rows,
									fold_rows,
								)?;
							}
						}
					}
				}
				if let Some(profile) = stage_profile.as_mut() {
					profile.add_flow(
						ChainStage::InnerReplay0,
						1,
						replay0_output_batches,
						post_filter_rows,
						replay0_output_rows,
					)?;
					profile.complete_base_batch(raw_rows, post_filter_rows)?;
					if profile.base_batch_flush_due() {
						profile.flush()?;
					}
				}
			}
			if let Some(mut state) = partition_replay_state.take() {
				let deferred_result = async {
					// Resolved before sealing: the probe reservoir's flush needs the same key plan
					// its writes used.
					let spec = partition_replay_spec.as_ref().ok_or_else(|| {
						ExecutionError::InvalidExpression {
							message: "committed partition replay lost its admitted spec"
								.to_string(),
						}
					})?;
					Box::pin(self.finish_partition_at_a_time_probe_reservoir(
						spec,
						&mut state,
						runtime_baseline_bytes,
					))
					.await?;
					let (runtime, suffix_runtimes) =
						inner_runtimes.split_first_mut().ok_or_else(|| {
							ExecutionError::InvalidExpression {
								message: "committed partition replay lost replay 1 runtime"
									.to_string(),
							}
						})?;
					Box::pin(self.drain_partition_at_a_time_replay(
						spec,
						&mut state,
						runtime,
						suffix_runtimes,
						semi_runtime,
						anti_runtime,
						group_expr,
						aggr_expr,
						groups,
						group_types,
						&mut stage_profile,
						runtime_baseline_bytes,
					))
					.await
				}
				.await;
				let probe_spill_bytes = state.probe_spill_bytes;
				Self::cleanup_partitioned_streamed_inner_builds(state.build).await;
				Self::cleanup_partitioned_streamed_inner_builds(state.probe).await;
				deferred_result?;
				record_task_telemetry(&self.context.task_telemetry, |task| {
					task.counters.spill_bytes =
						task.counters.spill_bytes.saturating_add(probe_spill_bytes);
				});
			}
			Ok(())
		}
		.await;
		if let Err(error) = drive_result {
			if let Some(state) = partition_replay_state.take() {
				Self::cleanup_partitioned_streamed_inner_builds(state.build).await;
				Self::cleanup_partitioned_streamed_inner_builds(state.probe).await;
			}
			if let Some(profile) = stage_profile.as_mut() {
				profile.mark_terminal_error(&error);
				let _ = profile.flush();
			}
			return Err(error);
		}
		if let Some(profile) = stage_profile.as_mut() {
			profile.drive_completed = true;
			profile.flush()?;
		}
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.phases.probe_micros = task
				.phases
				.probe_micros
				.saturating_add(duration_micros(probe_started));
			task.counters.probe_rows = task.counters.probe_rows.saturating_add(probe_rows);
			task.counters.matched_rows = task.counters.matched_rows.saturating_add(output_rows);
		});
		record_join_runtime_observation(
			&self.context.task_telemetry,
			&self.context.join_runtime_observation_mode,
			JoinRuntimeObservation {
				replay_id: *replay_id,
				strategy: *strategy,
				build_rows: RowCount::new(left_batch.num_rows() as u64),
				probe_rows: RowCount::new(probe_rows),
				output_rows: RowCount::new(output_rows),
			},
		);
		Ok(Some(()))
	}

	pub(in crate::exec) async fn prepare_reoriented_inner_join_runtime<'a>(
		&self,
		stage: ReorientedInnerJoinStage<'a>,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<ReorientedInnerJoinRuntime<'a>>, ExecutionError> {
		self.prepare_reoriented_inner_join_runtime_with_scan_build(stage, morsel, false, 0)
			.await
	}

	pub(in crate::exec) async fn prepare_reoriented_inner_join_runtime_with_streamed_scan_build<
		'a,
	>(
		&self,
		stage: ReorientedInnerJoinStage<'a>,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<ReorientedInnerJoinRuntime<'a>>, ExecutionError> {
		self.prepare_reoriented_inner_join_runtime_with_scan_build(stage, morsel, true, 0)
			.await
	}

	async fn prepare_reoriented_inner_join_runtime_with_streamed_scan_build_baseline<'a>(
		&self,
		stage: ReorientedInnerJoinStage<'a>,
		morsel: Option<(MorselId, u32)>,
		resident_baseline_bytes: u64,
	) -> Result<Option<ReorientedInnerJoinRuntime<'a>>, ExecutionError> {
		self.prepare_reoriented_inner_join_runtime_with_scan_build(
			stage,
			morsel,
			true,
			resident_baseline_bytes,
		)
		.await
	}

	async fn prepare_reoriented_inner_join_runtime_with_scan_build<'a>(
		&self,
		stage: ReorientedInnerJoinStage<'a>,
		morsel: Option<(MorselId, u32)>,
		stream_scan_build: bool,
		resident_baseline_bytes: u64,
	) -> Result<Option<ReorientedInnerJoinRuntime<'a>>, ExecutionError> {
		if stream_scan_build && decompose_streaming_scan_probe_input(stage.build).is_some() {
			let scan_input =
				decompose_streaming_scan_probe_input(stage.build).expect("checked scan build");
			let build_schema = stage.build.schema();
			let projected_build = match stage.retained_build_columns.as_deref() {
				Some(retained_columns) if retained_columns.len() < build_schema.fields().len() => {
					let projected_build_keys =
						remap_column_ordinals(retained_columns, &stage.build_keys)?;
					let projected_schema =
						projected_schema_for_columns(&build_schema, retained_columns)?;
					Some((
						retained_columns,
						projected_build_keys,
						projected_schema,
						ProjectedInnerBuildProjection {
							full_build_schema: build_schema.clone(),
							retained_columns: retained_columns.to_vec(),
						},
					))
				}
				_ => None,
			};
			let (key_plan, write_build_keys, retained_columns, projection) = if let Some((
				retained_columns,
				projected_build_keys,
				projected_schema,
				projection,
			)) = &projected_build
			{
				(
					JoinKeyPlan::from_build_schema_and_probe_schema(
						projected_schema,
						projected_build_keys,
						&stage.probe_schema,
						&stage.probe_keys,
						false,
					)?,
					projected_build_keys.as_slice(),
					Some(*retained_columns),
					Some(ProjectedInnerBuildProjection {
						full_build_schema: projection.full_build_schema.clone(),
						retained_columns: projection.retained_columns.clone(),
					}),
				)
			} else {
				(
					JoinKeyPlan::from_build_schema_and_probe_schema(
						&build_schema,
						&stage.build_keys,
						&stage.probe_schema,
						&stage.probe_keys,
						false,
					)?,
					stage.build_keys.as_slice(),
					None,
					None,
				)
			};
			let materialized_output_schema = if stage.probe_columns_first {
				concat_join_schema(&stage.build.schema(), &stage.probe_schema)
			} else {
				stage.output_schema.clone()
			};
			let estimated_build_bytes = self
				.estimate_streaming_scan_inner_build_bytes(
					scan_input,
					retained_columns,
					write_build_keys,
					false,
					morsel,
				)
				.await?;
			let (partition_count, group_count, _max_group_spill) = self
				.choose_streaming_scan_inner_build_partitioning(
					estimated_build_bytes,
					scan_input,
					retained_columns,
					&stage.build_keys,
					write_build_keys,
					false,
					morsel,
				)
				.await?;
			if partition_count > 1 {
				let partition_count_usize =
					usize::try_from(partition_count).map_err(|_| ExecutionError::Kernel {
						message: "streamed inner build partition count exceeds usize".to_string(),
					})?;
				guard_membership_owner_resident(
					resident_baseline_bytes,
					summary_bytes::<Option<LoadedSpilledInnerBuild>>(partition_count_usize),
					self.context.task_budget.memory.get(),
					"reoriented inner partition runtime",
				)?;
				return Ok(Some(ReorientedInnerJoinRuntime {
					replay_id: stage.replay_id,
					strategy: stage.strategy,
					build: ReorientedInnerJoinBuild::PartitionedScan {
						scan_input,
						morsel,
						partition_count,
						group_count,
						partition_build_keys: stage.build_keys.clone(),
						write_build_keys: write_build_keys.to_vec(),
						materialized_output_schema,
						build_column_count: stage.build.schema().fields().len(),
						projection,
						estimated_build_bytes,
						observed_build_rows: 0,
						loaded_builds: (0..partition_count_usize).map(|_| None).collect(),
						loaded_resident_bytes: 0,
						loaded_build_attempted: false,
					},
					key_plan,
					probe_keys: stage.probe_keys,
					output_schema: stage.output_schema,
					post_filter: stage.post_filter,
					probe_columns_first: stage.probe_columns_first,
					probe_rows: 0,
					output_rows: 0,
				}));
			}
			let mut spilled = SpilledInnerJoinBuild::create_with_resident_baseline(
				self.context.task_budget,
				self.context.join_spill_file_ops.clone(),
				self.context.join_spill_dir.clone(),
				resident_baseline_bytes,
			)
			.await?;
			let build_started = Instant::now();
			let build_result = async {
				self.write_streaming_scan_inner_build_batches(
					&mut spilled,
					scan_input,
					retained_columns,
					write_build_keys,
					key_plan,
					false,
					false,
					None,
					morsel,
				)
				.await?;
				spilled.finish(self.context.task_budget).await
			}
			.await;
			if let Err(error) = build_result {
				spilled.cleanup().await;
				return Err(error);
			}
			let build_rows = spilled.input_row_count();
			record_task_telemetry(&self.context.task_telemetry, |task| {
				task.counters.build_rows = task.counters.build_rows.saturating_add(build_rows);
				task.phases.build_micros = task
					.phases
					.build_micros
					.saturating_add(duration_micros(build_started));
				task.phases.spill_micros = task
					.phases
					.spill_micros
					.saturating_add(duration_micros(build_started));
				task.counters.spill_bytes = task
					.counters
					.spill_bytes
					.saturating_add(spilled.spill_bytes());
			});
			return Ok(Some(ReorientedInnerJoinRuntime {
				replay_id: stage.replay_id,
				strategy: stage.strategy,
				build: ReorientedInnerJoinBuild::Spilled {
					build: spilled,
					materialized_output_schema,
					build_column_count: stage.build.schema().fields().len(),
					projection,
				},
				key_plan,
				probe_keys: stage.probe_keys,
				output_schema: stage.output_schema,
				post_filter: stage.post_filter,
				probe_columns_first: stage.probe_columns_first,
				probe_rows: 0,
				output_rows: 0,
			}));
		}

		let build_batches = self.execute_bound(stage.build, morsel).await?;
		self.prepare_reoriented_inner_join_runtime_from_build_batches_with_baseline(
			stage,
			build_batches,
			resident_baseline_bytes,
		)
		.await
	}

	async fn prepare_reoriented_inner_join_runtime_from_build_batches<'a>(
		&self,
		stage: ReorientedInnerJoinStage<'a>,
		build_batches: Vec<RecordBatch>,
	) -> Result<Option<ReorientedInnerJoinRuntime<'a>>, ExecutionError> {
		self.prepare_reoriented_inner_join_runtime_from_build_batches_with_baseline(
			stage,
			build_batches,
			0,
		)
		.await
	}

	async fn prepare_reoriented_inner_join_runtime_from_build_batches_with_baseline<'a>(
		&self,
		stage: ReorientedInnerJoinStage<'a>,
		build_batches: Vec<RecordBatch>,
		resident_baseline_bytes: u64,
	) -> Result<Option<ReorientedInnerJoinRuntime<'a>>, ExecutionError> {
		let build_memory_cap =
			join_output_materialization_memory_cap(self.context.task_budget.memory.get());
		let estimated_build_bytes =
			estimated_build_hash_bytes_for_batches(&build_batches, &stage.build_keys)?;
		let key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&stage.build.schema(),
			&stage.build_keys,
			&stage.probe_schema,
			&stage.probe_keys,
			false,
		)?;
		if estimated_build_bytes > build_memory_cap {
			let mut spilled = SpilledInnerJoinBuild::create_with_resident_baseline(
				self.context.task_budget,
				self.context.join_spill_file_ops.clone(),
				self.context.join_spill_dir.clone(),
				resident_baseline_bytes,
			)
			.await?;
			spilled.use_compact_resident_index();
			let build_started = Instant::now();
			let build_result = async {
				for batch in &build_batches {
					spilled
						.write_batch_chunks(
							batch,
							key_plan,
							&stage.build_keys,
							false,
							self.context.task_budget,
						)
						.await?;
				}
				spilled.finish(self.context.task_budget).await
			}
			.await;
			if let Err(error) = build_result {
				spilled.cleanup().await;
				return Err(error);
			}
			let build_rows = spilled.input_row_count();
			record_task_telemetry(&self.context.task_telemetry, |task| {
				task.counters.build_rows = task.counters.build_rows.saturating_add(build_rows);
				task.phases.build_micros = task
					.phases
					.build_micros
					.saturating_add(duration_micros(build_started));
				task.phases.spill_micros = task
					.phases
					.spill_micros
					.saturating_add(duration_micros(build_started));
				task.counters.spill_bytes = task
					.counters
					.spill_bytes
					.saturating_add(spilled.spill_bytes());
			});
			let materialized_output_schema = if stage.probe_columns_first {
				concat_join_schema(&stage.build.schema(), &stage.probe_schema)
			} else {
				stage.output_schema.clone()
			};
			return Ok(Some(ReorientedInnerJoinRuntime {
				replay_id: stage.replay_id,
				strategy: stage.strategy,
				build: ReorientedInnerJoinBuild::Spilled {
					build: spilled,
					materialized_output_schema,
					build_column_count: stage.build.schema().fields().len(),
					projection: None,
				},
				key_plan,
				probe_keys: stage.probe_keys,
				output_schema: stage.output_schema,
				post_filter: stage.post_filter,
				probe_columns_first: stage.probe_columns_first,
				probe_rows: 0,
				output_rows: 0,
			}));
		}
		guard_membership_owner_resident(
			resident_baseline_bytes,
			estimated_build_bytes,
			self.context.task_budget.memory.get(),
			"reoriented inner in-memory runtime",
		)?;
		let build_batch = concat_or_empty(stage.build.schema(), &build_batches)?;
		let build_key_arrays = key_arrays(&build_batch, &stage.build_keys)?;
		let build_started = Instant::now();
		let Some(build_rows) = build_in_memory_join_rows(
			key_plan,
			&build_key_arrays,
			build_batch.num_rows(),
			build_memory_cap,
			false,
		)?
		else {
			return Err(budget_exhausted(
				BudgetDimension::Memory,
				"reoriented join build accumulator",
				estimated_build_bytes,
				build_memory_cap,
			));
		};
		let build_micros = duration_micros(build_started);
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.phases.build_micros = task.phases.build_micros.saturating_add(build_micros);
			task.counters.build_rows = task
				.counters
				.build_rows
				.saturating_add(build_batch.num_rows() as u64);
		});
		record_join_build_profile(
			&self.context.task_telemetry,
			stage.replay_id,
			stage.strategy,
			key_plan,
			&build_rows,
			build_micros,
		);
		Ok(Some(ReorientedInnerJoinRuntime {
			replay_id: stage.replay_id,
			strategy: stage.strategy,
			build: ReorientedInnerJoinBuild::InMemory {
				batch: build_batch,
				rows: build_rows,
			},
			key_plan,
			probe_keys: stage.probe_keys,
			output_schema: stage.output_schema,
			post_filter: stage.post_filter,
			probe_columns_first: stage.probe_columns_first,
			probe_rows: 0,
			output_rows: 0,
		}))
	}

	pub(in crate::exec) async fn prepare_streaming_semi_anti_runtime<'a>(
		&self,
		stage: StreamingSemiAntiStage<'a>,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<StreamingSemiAntiRuntime<'a>>, ExecutionError> {
		debug_assert!(matches!(
			stage.kind,
			JoinKind::LeftSemi | JoinKind::LeftAnti
		));
		let full_existence_schema = stage.existence.schema();
		let retained_existence_columns = retained_existence_columns_for_semi_anti_stage(&stage)?;
		let (build_schema, write_existence_keys) =
			if let Some(retained_columns) = retained_existence_columns.as_ref() {
				(
					projected_schema_for_columns(&full_existence_schema, retained_columns)?,
					remap_column_ordinals(retained_columns, &stage.existence_keys)?,
				)
			} else {
				(full_existence_schema.clone(), stage.existence_keys.clone())
			};
		let key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&build_schema,
			&write_existence_keys,
			&stage.output_schema,
			&stage.preserved_keys,
			false,
		)?;
		let mut spilled = SpilledSemiAntiExistenceBuild::create(
			self.context.task_budget,
			self.context.join_spill_file_ops.clone(),
			self.context.join_spill_dir.clone(),
		)
		.await?;
		if let Some(retained_columns) = retained_existence_columns.as_ref() {
			spilled.set_projection(full_existence_schema.clone(), retained_columns.clone())?;
		}
		let membership_only = stage.match_filter.is_none();
		let Some(()) = self
			.write_streaming_semi_anti_existence(
				stage.existence,
				&write_existence_keys,
				key_plan,
				retained_existence_columns.as_deref(),
				membership_only,
				morsel,
				&mut spilled,
			)
			.await?
		else {
			spilled.cleanup().await;
			return Ok(None);
		};
		spilled.finish(self.context.task_budget).await?;
		let existence_row_count = spilled.input_row_count();
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.build_rows = task.counters.build_rows.saturating_add(existence_row_count);
			task.counters.spill_bytes = task
				.counters
				.spill_bytes
				.saturating_add(spilled.spill_bytes());
		});
		Ok(Some(StreamingSemiAntiRuntime {
			replay_id: stage.replay_id,
			kind: stage.kind,
			strategy: stage.strategy,
			existence: StreamingSemiAntiExistence::Spilled(spilled),
			existence_row_count,
			key_plan,
			preserved_keys: stage.preserved_keys,
			match_filter: stage.match_filter,
			output_schema: stage.output_schema,
			probe_rows: 0,
			output_rows: 0,
		}))
	}

	fn recognized_neq_summary_scan_is_eligible(
		&self,
		stage: &StreamingSemiAntiStage<'_>,
		morsel: Option<(MorselId, u32)>,
	) -> bool {
		let Some((scan_plan, _)) = decompose_streaming_existence_plan(stage.existence) else {
			return false;
		};
		self.bound_scan_request(scan_plan, morsel).is_some()
	}

	fn recognized_neq_summary_pair(
		&self,
		semi: &StreamingSemiAntiStage<'_>,
		anti: &StreamingSemiAntiStage<'_>,
		morsel: Option<(MorselId, u32)>,
	) -> Option<(
		RecognizedNeqExistenceSummaryColumns,
		RecognizedNeqExistenceSummaryColumns,
	)> {
		let semi_columns = recognized_neq_existence_summary_columns(semi)?;
		let anti_columns = recognized_neq_existence_summary_columns(anti)?;
		(self.recognized_neq_summary_scan_is_eligible(semi, morsel)
			&& self.recognized_neq_summary_scan_is_eligible(anti, morsel))
		.then_some((semi_columns, anti_columns))
	}

	async fn prepare_recognized_neq_semi_anti_runtime<'a>(
		&self,
		stage: StreamingSemiAntiStage<'a>,
		columns: RecognizedNeqExistenceSummaryColumns,
		morsel: Option<(MorselId, u32)>,
		baseline_bytes: u64,
	) -> Result<StreamingSemiAntiRuntime<'a>, ExecutionError> {
		let cap = join_spill_retained_footprint_memory_cap(self.context.task_budget.memory.get());
		let build_started = Instant::now();
		let mut summary = NeqExistenceSummary::new();
		if let Some(key_count_hint) = columns.key_count_hint {
			summary.reserve_key_count(key_count_hint, baseline_bytes, cap)?;
		}
		self.write_streaming_recognized_neq_existence_summary(
			stage.existence,
			columns,
			morsel,
			baseline_bytes,
			cap,
			&mut summary,
		)
		.await?;
		let existence_row_count = summary.input_rows;
		record_existence_summary_profile(
			&self.context.task_telemetry,
			stage.replay_id,
			existence_row_count,
			summary.entries.len() as u64,
			summary.multi_keys,
			summary.entries.capacity(),
			summary.buckets.capacity(),
			summary.accounted_bytes(),
			baseline_bytes,
			summary.peak_combined_bytes,
			columns.key_count_hint,
			duration_micros(build_started),
		);
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.build_rows = task.counters.build_rows.saturating_add(existence_row_count);
		});
		Ok(StreamingSemiAntiRuntime {
			replay_id: stage.replay_id,
			kind: stage.kind,
			strategy: stage.strategy,
			existence: StreamingSemiAntiExistence::RecognizedNeqSummary {
				summary,
				preserved_value: columns.preserved_value,
			},
			existence_row_count,
			key_plan: JoinKeyPlan::Int64,
			preserved_keys: stage.preserved_keys,
			match_filter: stage.match_filter,
			output_schema: stage.output_schema,
			probe_rows: 0,
			output_rows: 0,
		})
	}

	async fn write_streaming_recognized_neq_existence_summary(
		&self,
		plan: &ExecutionPlan,
		columns: RecognizedNeqExistenceSummaryColumns,
		morsel: Option<(MorselId, u32)>,
		baseline_bytes: u64,
		cap: u64,
		summary: &mut NeqExistenceSummary,
	) -> Result<(), ExecutionError> {
		let (scan_plan, ops) = decompose_streaming_existence_plan(plan).ok_or_else(|| {
			ExecutionError::InvalidExpression {
				message: "committed recognized Neq existence summary lost its scan plan"
					.to_string(),
			}
		})?;
		let req = self.bound_scan_request(scan_plan, morsel).ok_or_else(|| {
			ExecutionError::InvalidExpression {
				message: "committed recognized Neq existence summary lost its scan request"
					.to_string(),
			}
		})?;
		let mut remaining = req.row_limit;
		let span = crate::telemetry::storage_scan_span(&req);
		let stream = self.storage.scan(req).instrument(span).await?;
		let mut stream = stream.into_batches();
		while let Some(batch) = stream.next().await {
			let mut batch = batch?;
			if let Some(limit) = remaining.as_mut() {
				if *limit == 0 {
					break;
				}
				if batch.num_rows() > *limit {
					batch = batch.slice(0, *limit);
				}
				*limit = (*limit).saturating_sub(batch.num_rows());
			}
			let mut batches = vec![batch];
			for op in &ops {
				let mut next = Vec::new();
				for batch in batches {
					match op {
						StreamingExistenceOp::Filter(predicate) => {
							let batch = self.filter_batch_with_bound_predicate(
								predicate,
								batch,
								"filter predicate",
							)?;
							if batch.num_rows() > 0 {
								next.push(batch);
							}
						}
						StreamingExistenceOp::Project {
							exprs,
							output_schema,
						} => {
							let columns = self.evaluate_bound_program(exprs, &batch)?;
							next.push(RecordBatch::try_new(output_schema.clone(), columns)?);
						}
						StreamingExistenceOp::Exchange {
							partition_count,
							key,
						} => {
							if *partition_count == 0 {
								return Err(ExecutionError::InvalidExpression {
									message: "exchange partition_count must be greater than zero"
										.to_string(),
								});
							}
							for (_partition, partitioned) in
								self.repartition_exchange_batch(&batch, *partition_count, key)?
							{
								if partitioned.num_rows() > 0 {
									next.push(partitioned);
								}
							}
						}
					}
				}
				batches = next;
				if batches.is_empty() {
					break;
				}
			}
			for batch in batches {
				summary.write_batch(
					&batch,
					columns.existence_key,
					columns.existence_value,
					baseline_bytes,
					cap,
				)?;
			}
		}
		Ok(())
	}

	pub(in crate::exec) async fn write_streaming_semi_anti_existence(
		&self,
		plan: &ExecutionPlan,
		existence_keys: &[crate::ir::ColumnOrdinal],
		key_plan: JoinKeyPlan,
		retained_columns: Option<&[crate::ir::ColumnOrdinal]>,
		membership_only: bool,
		morsel: Option<(MorselId, u32)>,
		spilled: &mut SpilledSemiAntiExistenceBuild,
	) -> Result<Option<()>, ExecutionError> {
		let Some((scan_plan, ops)) = decompose_streaming_existence_plan(plan) else {
			return Ok(None);
		};
		let Some(req) = self.bound_scan_request(scan_plan, morsel) else {
			return Ok(None);
		};
		let mut remaining = req.row_limit;
		let span = crate::telemetry::storage_scan_span(&req);
		let stream = self.storage.scan(req).instrument(span).await?;
		let mut stream = stream.into_batches();
		while let Some(batch) = stream.next().await {
			let mut batch = batch?;
			if let Some(limit) = remaining.as_mut() {
				if *limit == 0 {
					break;
				}
				if batch.num_rows() > *limit {
					batch = batch.slice(0, *limit);
				}
				*limit = (*limit).saturating_sub(batch.num_rows());
			}
			let mut batches = vec![batch];
			for op in &ops {
				let mut next = Vec::new();
				for batch in batches {
					match op {
						StreamingExistenceOp::Filter(predicate) => {
							let batch = self.filter_batch_with_bound_predicate(
								predicate,
								batch,
								"filter predicate",
							)?;
							if batch.num_rows() > 0 {
								next.push(batch);
							}
						}
						StreamingExistenceOp::Project {
							exprs,
							output_schema,
						} => {
							let columns = self.evaluate_bound_program(exprs, &batch)?;
							next.push(RecordBatch::try_new(output_schema.clone(), columns)?);
						}
						StreamingExistenceOp::Exchange {
							partition_count,
							key,
						} => {
							if *partition_count == 0 {
								return Err(ExecutionError::InvalidExpression {
									message: "exchange partition_count must be greater than zero"
										.to_string(),
								});
							}
							for (_partition, partitioned) in
								self.repartition_exchange_batch(&batch, *partition_count, key)?
							{
								if partitioned.num_rows() > 0 {
									next.push(partitioned);
								}
							}
						}
					}
				}
				batches = next;
				if batches.is_empty() {
					break;
				}
			}
			for batch in batches {
				if membership_only {
					if retained_columns.is_some() {
						spilled.write_projected_membership_key_chunks(
							&batch,
							key_plan,
							existence_keys,
							self.context.task_budget,
						)?;
					} else {
						spilled.write_membership_key_chunks(
							&batch,
							key_plan,
							existence_keys,
							self.context.task_budget,
						)?;
					}
				} else if retained_columns.is_some() {
					spilled
						.write_projected_batch_chunks(
							&batch,
							key_plan,
							existence_keys,
							self.context.task_budget,
						)
						.await?;
				} else {
					spilled
						.write_batch_chunks(
							&batch,
							key_plan,
							existence_keys,
							self.context.task_budget,
						)
						.await?;
				}
			}
		}
		Ok(Some(()))
	}

	pub(in crate::exec) async fn try_visit_streaming_reoriented_inner_chain_base<F>(
		&self,
		base: &ExecutionPlan,
		morsel: Option<(MorselId, u32)>,
		mut visit_batch: F,
	) -> Result<Option<()>, ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		let ExecutionPlan::Exchange {
			input,
			partition_count,
			key,
		} = base
		else {
			return Ok(None);
		};
		let (join0, filter_predicate) = match input.as_ref() {
			ExecutionPlan::Filter {
				input,
				predicate,
				disposition: FilterDisposition::EvaluateInToron,
			} => (input.as_ref(), Some(predicate)),
			ExecutionPlan::Filter {
				input,
				disposition: FilterDisposition::FullyPushed,
				..
			} => (input.as_ref(), None),
			other => (other, None),
		};

		let matched = Box::pin(self.try_visit_streaming_join0_chunks(
			join0,
			filter_predicate,
			morsel,
			None,
			|batch| {
				for (_partition, partitioned) in
					self.repartition_exchange_batch(&batch, *partition_count, key)?
				{
					if partitioned.num_rows() > 0 {
						visit_batch(partitioned)?;
					}
				}
				Ok(())
			},
		))
		.await?;
		if matched.is_none() {
			return Ok(None);
		}
		Ok(Some(()))
	}

	pub(in crate::exec) async fn probe_streaming_semi_anti(
		&self,
		runtime: &mut StreamingSemiAntiRuntime<'_>,
		preserved_batch: &RecordBatch,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		if preserved_batch.num_rows() == 0 {
			return Ok(Vec::new());
		}
		let preserved_key_arrays = key_arrays(preserved_batch, &runtime.preserved_keys)?;
		runtime.probe_rows = runtime
			.probe_rows
			.saturating_add(preserved_batch.num_rows() as u64);
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.probe_rows = task
				.counters
				.probe_rows
				.saturating_add(preserved_batch.num_rows() as u64);
		});

		let mut preserved_by_key: HashMap<JoinKey, Vec<u32>> = HashMap::new();
		for row in 0..preserved_batch.num_rows() {
			if let Some(key) = join_key_at(runtime.key_plan, &preserved_key_arrays, row, false)? {
				preserved_by_key
					.entry(key)
					.or_default()
					.push(join_probe_row_index(row)?);
			}
		}
		let matched_rows = match &mut runtime.existence {
			StreamingSemiAntiExistence::InMemory { batch, rows } => {
				matching_preserved_rows_in_memory(
					preserved_batch,
					&preserved_by_key,
					runtime.match_filter,
					batch,
					rows,
				)?
			}
			StreamingSemiAntiExistence::Spilled(spilled) => {
				spilled
					.matching_preserved_rows(
						preserved_batch,
						&preserved_by_key,
						runtime.match_filter,
					)
					.await?
			}
			StreamingSemiAntiExistence::RecognizedNeqSummary {
				summary,
				preserved_value,
			} => summary.matching_preserved_rows(
				preserved_batch,
				&preserved_by_key,
				*preserved_value,
			)?,
		};
		let mut selected = Vec::<u32>::new();
		for row in 0..preserved_batch.num_rows() {
			let matched = matched_rows[row];
			if (runtime.kind == JoinKind::LeftSemi && matched)
				|| (runtime.kind == JoinKind::LeftAnti && !matched)
			{
				selected.push(join_probe_row_index(row)?);
			}
		}
		runtime.output_rows = runtime.output_rows.saturating_add(selected.len() as u64);
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.matched_rows = task
				.counters
				.matched_rows
				.saturating_add(selected.len() as u64);
		});

		let mut output = Vec::new();
		for chunk in selected.chunks(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let batch = materialize_preserved_selection(
				preserved_batch,
				chunk,
				&runtime.output_schema,
				self.context.task_budget.memory.get(),
			)?;
			if batch.num_rows() > 0 {
				output.push(batch);
			}
		}
		Ok(output)
	}

	pub(in crate::exec) fn probe_streaming_left_outer_recombine(
		&self,
		runtime: &mut StreamingLeftOuterRecombineRuntime<'_>,
		preserved_batch: &RecordBatch,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		if preserved_batch.num_rows() == 0 {
			return Ok(Vec::new());
		}
		let preserved_key_arrays = key_arrays(preserved_batch, &runtime.preserved_keys)?;
		runtime.probe_rows = runtime
			.probe_rows
			.saturating_add(preserved_batch.num_rows() as u64);
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.probe_rows = task
				.counters
				.probe_rows
				.saturating_add(preserved_batch.num_rows() as u64);
		});

		let mut selection = Vec::new();
		let mut residual =
			JoinMatchResidual::new(runtime.match_filter, preserved_batch, &runtime.scalar_batch);
		for row in 0..preserved_batch.num_rows() {
			let left = join_build_row_index(row)?;
			let matched = if let Some(key) = join_key_at(
				runtime.key_plan,
				&preserved_key_arrays,
				row,
				runtime.null_safe_keys,
			)? {
				if let Some(matches) = runtime.scalar_rows.get(&key) {
					let mut matched = false;
					for right in matches.rows() {
						if residual.matches(left, right)? {
							matched = true;
							selection.push(JoinOutputRow::Paired { left, right });
						}
					}
					matched
				} else {
					false
				}
			} else {
				false
			};
			if !matched {
				selection.push(JoinOutputRow::LeftFillNull { left });
			}
		}
		runtime.output_rows = runtime.output_rows.saturating_add(selection.len() as u64);
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.matched_rows = task
				.counters
				.matched_rows
				.saturating_add(selection.len() as u64);
		});

		let mut output = Vec::new();
		for chunk in selection.chunks(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let batch = materialize_join_selection(
				JoinKind::LeftOuter,
				preserved_batch,
				&runtime.scalar_batch,
				chunk,
				&runtime.output_schema,
				self.context.task_budget.memory.get(),
			)?;
			if batch.num_rows() > 0 {
				output.push(batch);
			}
		}
		Ok(output)
	}

	pub(in crate::exec) async fn probe_reoriented_inner_join_streaming(
		&self,
		runtime: &mut ReorientedInnerJoinRuntime<'_>,
		probe_batch: &RecordBatch,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		let (output, probe_rows, output_rows) = match &mut runtime.build {
			ReorientedInnerJoinBuild::InMemory { batch, rows } => self
				.probe_reoriented_inner_join_in_memory(
					runtime.key_plan,
					&runtime.probe_keys,
					&runtime.output_schema,
					runtime.probe_columns_first,
					batch,
					rows,
					probe_batch,
				)?,
			ReorientedInnerJoinBuild::Spilled {
				build,
				materialized_output_schema,
				build_column_count,
				projection,
			} => {
				if probe_batch.num_rows() == 0 {
					(Vec::new(), 0, 0)
				} else {
					let mut output = Vec::new();
					let (probe_rows, output_rows) = self
						.visit_streaming_spilled_inner_probe_batch(
							build,
							probe_batch,
							&runtime.probe_keys,
							runtime.key_plan,
							false,
							materialized_output_schema,
							&runtime.output_schema,
							None,
							runtime.probe_columns_first,
							*build_column_count,
							projection.as_ref(),
							None,
							None,
							&mut |batch| {
								output.push(batch);
								Ok(())
							},
						)
						.await?;
					(output, probe_rows, output_rows)
				}
			}
			ReorientedInnerJoinBuild::PartitionedScan {
				scan_input,
				morsel,
				partition_count,
				partition_build_keys,
				write_build_keys,
				materialized_output_schema,
				build_column_count,
				projection,
				estimated_build_bytes,
				observed_build_rows,
				group_count,
				loaded_builds,
				loaded_resident_bytes,
				loaded_build_attempted,
			} => {
				if probe_batch.num_rows() == 0 {
					(Vec::new(), 0, 0)
				} else {
					if !*loaded_build_attempted {
						*loaded_build_attempted = true;
						if let Some((loaded, resident_bytes, build_rows)) = self
							.try_load_resident_partitioned_streamed_inner_scan_build(
								*scan_input,
								*morsel,
								*partition_count,
								*group_count,
								partition_build_keys,
								write_build_keys,
								runtime.key_plan,
								false,
								projection.as_ref(),
								*estimated_build_bytes,
							)
							.await?
						{
							*loaded_builds = loaded;
							*loaded_resident_bytes = resident_bytes;
							*observed_build_rows = observed_build_rows.saturating_add(build_rows);
						}
					}
					if loaded_builds.iter().any(Option::is_some) {
						let (output, probe_rows, output_rows) = self
							.probe_loaded_partitioned_streamed_inner_scan_build(
								loaded_builds,
								*partition_count,
								probe_batch,
								&runtime.probe_keys,
								runtime.key_plan,
								false,
								materialized_output_schema,
								&runtime.output_schema,
								runtime.probe_columns_first,
								*build_column_count,
								projection.as_ref(),
							)?;
						(output, probe_rows, output_rows)
					} else {
						let (output, probe_rows, output_rows, build_rows) = self
							.probe_partitioned_streamed_inner_scan_build(
								*scan_input,
								*morsel,
								*partition_count,
								*group_count,
								partition_build_keys,
								write_build_keys,
								probe_batch,
								&runtime.probe_keys,
								runtime.key_plan,
								false,
								materialized_output_schema,
								&runtime.output_schema,
								runtime.probe_columns_first,
								*build_column_count,
								projection.as_ref(),
							)
							.await?;
						*observed_build_rows = observed_build_rows.saturating_add(build_rows);
						(output, probe_rows, output_rows)
					}
				}
			}
		};
		self.record_reoriented_inner_probe(runtime, probe_rows, output_rows);
		Ok(output)
	}

	#[allow(clippy::too_many_arguments)]
	async fn try_load_resident_partitioned_streamed_inner_scan_build(
		&self,
		scan_input: StreamingScanProbeInput<'_>,
		morsel: Option<(MorselId, u32)>,
		partition_count: u32,
		group_count: u32,
		partition_build_keys: &[crate::ir::ColumnOrdinal],
		write_build_keys: &[crate::ir::ColumnOrdinal],
		key_plan: JoinKeyPlan,
		null_safe_keys: bool,
		projection: Option<&ProjectedInnerBuildProjection>,
		estimated_build_bytes: u64,
	) -> Result<Option<(Vec<Option<LoadedSpilledInnerBuild>>, u64, u64)>, ExecutionError> {
		let partition_count_usize =
			usize::try_from(partition_count).map_err(|_| ExecutionError::Kernel {
				message: "streamed inner build partition count exceeds usize".to_string(),
			})?;
		let group_count = group_count.max(1).min(partition_count);
		let loaded_cap =
			join_spill_retained_footprint_memory_cap(self.context.task_budget.memory.get());
		let estimated_resident_floor = estimated_build_bytes
			.saturating_mul(STREAMED_INNER_LOADED_BUILD_ESTIMATE_SAFETY_MULTIPLIER)
			.saturating_add(64 * 1024);
		if estimated_resident_floor > loaded_cap {
			return Ok(None);
		}
		let mut loaded_builds: Vec<Option<LoadedSpilledInnerBuild>> =
			(0..partition_count_usize).map(|_| None).collect();
		let mut loaded_resident_bytes = 0u64;
		let mut total_build_rows = 0u64;
		let mut total_spill_bytes = 0u64;
		let build_started = Instant::now();

		for group in 0..group_count {
			let group_range = partition_group_bounds(partition_count, group_count, group)?;
			let mut partitioned_builds: Vec<Option<SpilledInnerJoinBuild>> =
				(0..partition_count_usize).map(|_| None).collect();
			for partition in group_range.clone() {
				let created = SpilledInnerJoinBuild::create(
					self.context.task_budget,
					self.context.join_spill_file_ops.clone(),
					self.context.join_spill_dir.clone(),
				)
				.await?;
				partitioned_builds[partition] = Some(created);
			}

			let mut group_spill_bytes = 0u64;
			let build_result = self
				.write_partitioned_streaming_scan_inner_build_batches(
					&mut partitioned_builds,
					&mut group_spill_bytes,
					scan_input,
					projection.map(|projection| projection.retained_columns.as_slice()),
					partition_build_keys,
					write_build_keys,
					key_plan,
					null_safe_keys,
					morsel,
					group_range.start,
					group_range.end,
					true,
					None,
					None,
					None,
					None,
					None,
				)
				.await;
			if let Err(error) = build_result {
				Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
				if matches!(error, ExecutionError::BudgetExhausted { .. }) {
					return Ok(None);
				}
				return Err(error);
			}
			total_spill_bytes = total_spill_bytes.saturating_add(group_spill_bytes);

			for partition in group_range {
				let Some(mut spilled) =
					partitioned_builds.get_mut(partition).and_then(Option::take)
				else {
					continue;
				};
				if let Err(error) = spilled.finish(self.context.task_budget).await {
					spilled.cleanup().await;
					Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
					return Err(error);
				}
				spilled.release_write_scratch();
				if spilled.input_row_count() == 0 {
					spilled.cleanup().await;
					continue;
				}
				let loaded_page_bytes = spilled.loaded_page_bytes_estimate();
				let estimated_index = estimate_loaded_partition_index_bytes(
					spilled.input_row_count(),
					key_plan,
					loaded_page_bytes,
				);
				let estimated_resident = loaded_page_bytes
					.saturating_add(estimated_index)
					.saturating_add(64 * 1024);
				if loaded_resident_bytes.saturating_add(estimated_resident) > loaded_cap {
					spilled.cleanup().await;
					Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
					return Ok(None);
				}
				let build_pages = match spilled.read_all_batch_pages().await {
					Ok(pages) => pages,
					Err(error) => {
						spilled.cleanup().await;
						Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
						return Err(error);
					}
				};
				let build_rows = spilled.input_row_count();
				spilled.cleanup().await;
				let loaded = LoadedSpilledInnerBuild::new(
					build_pages,
					key_plan,
					write_build_keys,
					null_safe_keys,
					self.context.task_budget.memory.get(),
					0,
				)?;
				if loaded_resident_bytes.saturating_add(loaded.resident_bytes()) > loaded_cap {
					Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
					return Ok(None);
				}
				loaded_resident_bytes =
					loaded_resident_bytes.saturating_add(loaded.resident_bytes());
				total_build_rows = total_build_rows.saturating_add(build_rows);
				loaded_builds[partition] = Some(loaded);
			}
			Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
		}

		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.build_rows = task.counters.build_rows.saturating_add(total_build_rows);
			task.phases.build_micros = task
				.phases
				.build_micros
				.saturating_add(duration_micros(build_started));
			task.phases.spill_micros = task
				.phases
				.spill_micros
				.saturating_add(duration_micros(build_started));
			task.counters.spill_bytes = task.counters.spill_bytes.saturating_add(total_spill_bytes);
		});
		Ok(Some((
			loaded_builds,
			loaded_resident_bytes,
			total_build_rows,
		)))
	}

	#[allow(clippy::too_many_arguments)]
	async fn build_partitioned_streaming_scan_inner_build_reservoir(
		&self,
		scan_input: StreamingScanProbeInput<'_>,
		morsel: Option<(MorselId, u32)>,
		partition_count: u32,
		partition_build_keys: &[crate::ir::ColumnOrdinal],
		write_build_keys: &[crate::ir::ColumnOrdinal],
		key_plan: JoinKeyPlan,
		null_safe_keys: bool,
		projection: Option<&ProjectedInnerBuildProjection>,
		resident_baseline_bytes: u64,
		committed_owner_path: bool,
		mut residency_profile: Option<&mut GroupedReservoirResidencyProfile>,
		membership_profile: Option<(
			&MembershipChainProfile,
			MembershipChainPhase,
			MembershipSpillRole,
		)>,
		grouped_chain_observer: Option<&GroupedChainSpillObserver<'_>>,
		local_runtime_filter: Option<RuntimeFilterDescriptor>,
	) -> Result<Option<(Vec<Option<SpilledInnerJoinBuild>>, u64)>, ExecutionError> {
		let partition_count_usize =
			usize::try_from(partition_count).map_err(|_| ExecutionError::Kernel {
				message: "streamed inner build partition count exceeds usize".to_string(),
			})?;
		if let Some(profile) = residency_profile.as_deref_mut() {
			profile.writer_set_intent(writer_set_collection_bytes(partition_count)?)?;
		}
		let writer_setup_timer = grouped_chain_observer
			.map(|observer| observer.begin(GroupedChainSubLeaf::WriterSetup))
			.transpose()?;
		let mut partitioned_builds: Vec<Option<SpilledInnerJoinBuild>> =
			(0..partition_count_usize).map(|_| None).collect();
		for partition in 0..partition_count_usize {
			let created = match SpilledInnerJoinBuild::create_with_resident_baseline(
				self.context.task_budget,
				self.context.join_spill_file_ops.clone(),
				self.context.join_spill_dir.clone(),
				resident_baseline_bytes,
			)
			.await
			{
				Ok(created) => created,
				Err(error) => {
					if let Some(profile) = residency_profile.as_deref_mut() {
						profile.terminal(&partitioned_builds, "writer_creation_error")?;
					}
					Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
					return Err(error);
				}
			};
			partitioned_builds[partition] = Some(created);
		}
		if let Some(profile) = residency_profile.as_deref_mut() {
			profile.writer_set_ready(&partitioned_builds)?;
		}
		if let Some(timer) = writer_setup_timer {
			timer.finish_with(0, 0, partition_count as u64)?;
		}

		let build_started = Instant::now();
		let mut total_spill_bytes = 0u64;
		let build_result = self
			.write_partitioned_streaming_scan_inner_build_batches(
				&mut partitioned_builds,
				&mut total_spill_bytes,
				scan_input,
				projection.map(|projection| projection.retained_columns.as_slice()),
				partition_build_keys,
				write_build_keys,
				key_plan,
				null_safe_keys,
				morsel,
				0,
				partition_count_usize,
				false,
				committed_owner_path.then_some(resident_baseline_bytes),
				residency_profile.as_deref_mut(),
				membership_profile.map(|(profile, phase, _)| (profile, phase)),
				grouped_chain_observer,
				local_runtime_filter,
			)
			.await;
		if let Err(error) = build_result {
			if let Some(profile) = residency_profile.as_deref_mut() {
				profile.terminal(&partitioned_builds, "reservoir_write_error")?;
			}
			Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
			if !committed_owner_path && matches!(error, ExecutionError::BudgetExhausted { .. }) {
				return Ok(None);
			}
			return Err(error);
		}
		if let Some(profile) = residency_profile.as_deref_mut() {
			profile.phase(
				ProfilePhase::ReservoirFinish,
				&partitioned_builds,
				"reservoir_write_complete",
			)?;
		}

		let finish_timer = grouped_chain_observer
			.map(|observer| observer.begin(GroupedChainSubLeaf::WriteFinishFsync))
			.transpose()?;
		let mut total_build_rows = 0u64;
		for partition in 0..partition_count_usize {
			let Some(spilled) = partitioned_builds
				.get_mut(partition)
				.and_then(Option::as_mut)
			else {
				continue;
			};
			if let Err(error) = spilled.finish(self.context.task_budget).await {
				if let Some(profile) = residency_profile.as_deref_mut() {
					profile.terminal(&partitioned_builds, "reservoir_finish_error")?;
				}
				Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
				return Err(error);
			}
			spilled.release_write_scratch();
			total_build_rows = total_build_rows.saturating_add(spilled.input_row_count());
			if spilled.input_row_count() == 0 {
				let Some(empty) = partitioned_builds.get_mut(partition).and_then(Option::take)
				else {
					continue;
				};
				empty.cleanup().await;
			}
		}
		if let Some(profile) = residency_profile.as_deref_mut() {
			profile.phase(
				ProfilePhase::ReservoirFinish,
				&partitioned_builds,
				"reservoir_finish_complete",
			)?;
		}
		if let Some(timer) = finish_timer {
			timer.finish_with(total_build_rows, total_spill_bytes, partition_count as u64)?;
		}

		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.build_rows = task.counters.build_rows.saturating_add(total_build_rows);
			task.phases.build_micros = task
				.phases
				.build_micros
				.saturating_add(duration_micros(build_started));
			task.phases.spill_micros = task
				.phases
				.spill_micros
				.saturating_add(duration_micros(build_started));
			task.counters.spill_bytes = task.counters.spill_bytes.saturating_add(total_spill_bytes);
		});
		if let Some((profile, _, role)) = membership_profile {
			profile.record_spill_bytes(role, total_spill_bytes)?;
		}
		Ok(Some((partitioned_builds, total_build_rows)))
	}

	#[allow(clippy::too_many_arguments)]
	fn probe_loaded_partitioned_streamed_inner_scan_build(
		&self,
		loaded_builds: &[Option<LoadedSpilledInnerBuild>],
		partition_count: u32,
		probe_batch: &RecordBatch,
		probe_keys: &[crate::ir::ColumnOrdinal],
		key_plan: JoinKeyPlan,
		null_safe_keys: bool,
		materialized_output_schema: &SchemaRef,
		output_schema: &SchemaRef,
		probe_columns_first: bool,
		build_column_count: usize,
		projection: Option<&ProjectedInnerBuildProjection>,
	) -> Result<(Vec<RecordBatch>, u64, u64), ExecutionError> {
		let mut output = Vec::new();
		let mut total_probe_rows = 0u64;
		let mut total_output_rows = 0u64;
		let probe_partition_buckets = precompute_join_partition_buckets(
			probe_batch,
			probe_keys,
			partition_count,
			null_safe_keys,
			false,
		)?;
		for (partition, partitioned_probe) in partition_streaming_join_batch_with_buckets(
			probe_batch,
			&probe_partition_buckets,
			partition_count,
		)? {
			let Some(loaded) = loaded_builds.get(partition).and_then(Option::as_ref) else {
				continue;
			};
			let result = visit_loaded_partition_work(
				loaded,
				partition,
				partitioned_probe,
				probe_keys,
				key_plan,
				null_safe_keys,
				materialized_output_schema,
				output_schema,
				probe_columns_first,
				build_column_count,
				projection,
				self.context.task_budget.memory.get(),
				None,
				None,
			)?;
			total_probe_rows = total_probe_rows.saturating_add(result.probe_rows);
			total_output_rows = total_output_rows.saturating_add(result.output_rows);
			output.extend(result.output);
		}
		Ok((output, total_probe_rows, total_output_rows))
	}

	#[allow(clippy::too_many_arguments)]
	async fn probe_partitioned_streamed_inner_scan_build_reservoir(
		&self,
		partitioned_builds: &mut [Option<SpilledInnerJoinBuild>],
		partition_count: u32,
		group_count: u32,
		probe_batch: &RecordBatch,
		probe_keys: &[crate::ir::ColumnOrdinal],
		write_build_keys: &[crate::ir::ColumnOrdinal],
		key_plan: JoinKeyPlan,
		null_safe_keys: bool,
		materialized_output_schema: &SchemaRef,
		output_schema: &SchemaRef,
		probe_columns_first: bool,
		build_column_count: usize,
		projection: Option<&ProjectedInnerBuildProjection>,
		resident_baseline_bytes: u64,
		membership_profile: Option<(&MembershipChainProfile, MembershipChainPhase)>,
		grouped_chain_observer: Option<&GroupedChainSpillObserver<'_>>,
	) -> Result<(Vec<RecordBatch>, u64, u64), ExecutionError> {
		let mut output = Vec::new();
		let mut total_probe_rows = 0u64;
		let mut total_output_rows = 0u64;
		let partition_timer = grouped_chain_observer
			.map(|observer| observer.begin(GroupedChainSubLeaf::ReadProbePartitioning))
			.transpose()?;
		let probe_partition_buckets = precompute_join_partition_buckets(
			probe_batch,
			probe_keys,
			partition_count,
			null_safe_keys,
			false,
		)?;
		let partitioned_probe_batches = partition_streaming_join_batch_with_buckets(
			probe_batch,
			&probe_partition_buckets,
			partition_count,
		)?;
		if let Some(timer) = partition_timer {
			timer.finish_with(probe_batch.num_rows() as u64, 0, 0)?;
		}
		if partitioned_probe_batches.is_empty() {
			return Ok((output, 0, 0));
		}
		let group_count = group_count.max(1).min(partition_count);
		for group in 0..group_count {
			let group_range = partition_group_bounds(partition_count, group_count, group)?;
			for (partition, partitioned_probe) in partitioned_probe_batches
				.iter()
				.filter(|(partition, _)| group_range.contains(partition))
			{
				let Some(spilled) = partitioned_builds
					.get_mut(*partition)
					.and_then(Option::as_mut)
				else {
					continue;
				};
				if spilled.input_row_count() == 0 {
					continue;
				}
				let memory_cap = self.context.task_budget.memory.get();
				let loaded_resident =
					estimate_parallel_partition_resident(spilled, partitioned_probe, key_plan);
				let loaded_cap = join_spill_retained_footprint_memory_cap(memory_cap);
				if resident_baseline_bytes.saturating_add(loaded_resident) <= loaded_cap {
					if let Some(observer) = grouped_chain_observer {
						observer.record_branch(true)?;
					}
					spilled.disable_page_cache();
					let read_timer = membership_profile
						.map(|(profile, phase)| {
							profile.begin_leaf(phase, MembershipChainOperation::SpillReadDecode)
						})
						.transpose()?;
					let build_pages = spilled
						.read_all_batch_pages_profiled(grouped_chain_observer)
						.await?;
					if let Some(timer) = read_timer {
						timer.finish_with(
							build_pages
								.iter()
								.map(|batch| batch.num_rows() as u64)
								.sum(),
							build_pages.iter().map(record_batch_memory_bytes).sum(),
						)?;
					}
					let work = LoadedPartitionWork {
						partition: *partition,
						build_pages,
						probe_batch: partitioned_probe.clone(),
						probe_keys: probe_keys.to_vec(),
						write_build_keys: write_build_keys.to_vec(),
						key_plan,
						null_safe_keys,
						materialized_output_schema: materialized_output_schema.clone(),
						output_schema: output_schema.clone(),
						probe_columns_first,
						build_column_count,
						projection: projection.cloned(),
						memory_cap,
						resident_baseline_bytes,
					};
					let result = run_loaded_partition_work(
						work,
						membership_profile,
						grouped_chain_observer,
					)?;
					total_probe_rows = total_probe_rows.saturating_add(result.probe_rows);
					total_output_rows = total_output_rows.saturating_add(result.output_rows);
					output.extend(result.output);
				} else {
					if let Some(observer) = grouped_chain_observer {
						observer.record_branch(false)?;
					}
					let index_timer = membership_profile
						.map(|(profile, phase)| {
							profile.begin_leaf(phase, MembershipChainOperation::IndexBuild)
						})
						.transpose()?;
					if let Err(error) = spilled
						.rebuild_index_from_pages_profiled(
							key_plan,
							write_build_keys,
							null_safe_keys,
							self.context.task_budget,
							grouped_chain_observer,
						)
						.await
					{
						return Err(error);
					}
					if let Some(timer) = index_timer {
						timer.finish()?;
					}
					let mut partition_output = Vec::new();
					let lookup_timer = membership_profile
						.map(|(profile, phase)| {
							profile.begin_leaf(phase, MembershipChainOperation::LookupSelection)
						})
						.transpose()?;
					let (probe_rows, output_rows) = self
						.visit_streaming_spilled_inner_probe_batch(
							spilled,
							partitioned_probe,
							probe_keys,
							key_plan,
							null_safe_keys,
							materialized_output_schema,
							output_schema,
							None,
							probe_columns_first,
							build_column_count,
							projection,
							None,
							grouped_chain_observer,
							&mut |batch| {
								partition_output.push(batch);
								Ok(())
							},
						)
						.await?;
					if let Some(timer) = lookup_timer {
						timer.finish_with(partitioned_probe.num_rows() as u64, 0)?;
					}
					total_probe_rows = total_probe_rows.saturating_add(probe_rows);
					total_output_rows = total_output_rows.saturating_add(output_rows);
					output.extend(partition_output);
				}
			}
		}
		Ok((output, total_probe_rows, total_output_rows))
	}

	#[allow(clippy::too_many_arguments)]
	async fn probe_partitioned_streamed_inner_scan_build(
		&self,
		scan_input: StreamingScanProbeInput<'_>,
		morsel: Option<(MorselId, u32)>,
		partition_count: u32,
		group_count: u32,
		partition_build_keys: &[crate::ir::ColumnOrdinal],
		write_build_keys: &[crate::ir::ColumnOrdinal],
		probe_batch: &RecordBatch,
		probe_keys: &[crate::ir::ColumnOrdinal],
		key_plan: JoinKeyPlan,
		null_safe_keys: bool,
		materialized_output_schema: &SchemaRef,
		output_schema: &SchemaRef,
		probe_columns_first: bool,
		build_column_count: usize,
		projection: Option<&ProjectedInnerBuildProjection>,
	) -> Result<(Vec<RecordBatch>, u64, u64, u64), ExecutionError> {
		let mut output = Vec::new();
		let mut total_probe_rows = 0u64;
		let mut total_output_rows = 0u64;
		let probe_partition_buckets = precompute_join_partition_buckets(
			probe_batch,
			probe_keys,
			partition_count,
			null_safe_keys,
			false,
		)?;
		let partitioned_probe_batches = partition_streaming_join_batch_with_buckets(
			probe_batch,
			&probe_partition_buckets,
			partition_count,
		)?;
		if partitioned_probe_batches.is_empty() {
			return Ok((output, 0, 0, 0));
		}
		let build_started = Instant::now();
		let mut total_build_rows = 0u64;
		let mut cumulative_spill_bytes = 0u64;
		let group_count = group_count.max(1).min(partition_count);
		for group in 0..group_count {
			let group_range = partition_group_bounds(partition_count, group_count, group)?;
			let group_probe_batches = partitioned_probe_batches
				.iter()
				.filter(|(partition, _)| group_range.contains(partition))
				.map(|(partition, batch)| (*partition, batch.clone()))
				.collect::<Vec<_>>();
			if group_probe_batches.is_empty() {
				continue;
			}

			let mut partitioned_builds: Vec<Option<SpilledInnerJoinBuild>> =
				(0..partition_count).map(|_| None).collect();
			for (partition, _) in &group_probe_batches {
				if *partition >= partitioned_builds.len() {
					Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
					return Err(ExecutionError::Kernel {
						message: format!("active join partition {partition} is out of range"),
					});
				}
				let created = SpilledInnerJoinBuild::create(
					self.context.task_budget,
					self.context.join_spill_file_ops.clone(),
					self.context.join_spill_dir.clone(),
				)
				.await;
				let created = match created {
					Ok(created) => created,
					Err(error) => {
						Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
						return Err(error);
					}
				};
				partitioned_builds[*partition] = Some(created);
			}

			let mut group_spill_bytes = 0u64;
			let build_result = self
				.write_partitioned_streaming_scan_inner_build_batches(
					&mut partitioned_builds,
					&mut group_spill_bytes,
					scan_input,
					projection.map(|projection| projection.retained_columns.as_slice()),
					partition_build_keys,
					write_build_keys,
					key_plan,
					null_safe_keys,
					morsel,
					group_range.start,
					group_range.end,
					true,
					None,
					None,
					None,
					None,
					None,
				)
				.await;
			if let Err(error) = build_result {
				Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
				return Err(error);
			}
			cumulative_spill_bytes = cumulative_spill_bytes.saturating_add(group_spill_bytes);

			for (partition, _) in &group_probe_batches {
				let Some(spilled) = partitioned_builds
					.get_mut(*partition)
					.and_then(Option::as_mut)
				else {
					continue;
				};
				if let Err(error) = spilled.finish(self.context.task_budget).await {
					Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
					return Err(error);
				}
				spilled.release_write_scratch();
				total_build_rows = total_build_rows.saturating_add(spilled.input_row_count());
			}

			let parallel_plan = parallel_partition_plan(
				&partitioned_builds,
				&group_probe_batches,
				key_plan,
				self.context.partition_parallelism,
				self.context.task_budget.memory.get(),
			);
			debug_assert!(parallel_plan.resident_peak > 0 || group_probe_batches.is_empty());
			if parallel_plan.k > 1 {
				let partition_count_usize =
					usize::try_from(partition_count).map_err(|_| ExecutionError::Kernel {
						message: "streamed inner build partition count exceeds usize".to_string(),
					})?;
				let semaphore = Arc::new(tokio::sync::Semaphore::new(parallel_plan.k));
				let mut handles = Vec::new();
				let mut first_error = None;
				for (partition, partitioned_probe) in group_probe_batches {
					let Some(mut spilled) =
						partitioned_builds.get_mut(partition).and_then(Option::take)
					else {
						continue;
					};
					if spilled.input_row_count() == 0 {
						spilled.cleanup().await;
						continue;
					}
					let permit = match semaphore.clone().acquire_owned().await {
						Ok(permit) => permit,
						Err(error) => {
							spilled.cleanup().await;
							first_error.get_or_insert(ExecutionError::Kernel {
								message: format!(
									"parallel streamed inner join semaphore closed: {error}"
								),
							});
							break;
						}
					};
					let build_pages = match spilled.read_all_batch_pages().await {
						Ok(pages) => pages,
						Err(error) => {
							spilled.cleanup().await;
							first_error.get_or_insert(error);
							break;
						}
					};
					spilled.cleanup().await;
					let work = LoadedPartitionWork {
						partition,
						build_pages,
						probe_batch: partitioned_probe,
						probe_keys: probe_keys.to_vec(),
						write_build_keys: write_build_keys.to_vec(),
						key_plan,
						null_safe_keys,
						materialized_output_schema: materialized_output_schema.clone(),
						output_schema: output_schema.clone(),
						probe_columns_first,
						build_column_count,
						projection: projection.cloned(),
						memory_cap: self.context.task_budget.memory.get(),
						resident_baseline_bytes: 0,
					};
					handles.push(tokio::task::spawn_blocking(move || {
						let _permit = permit;
						run_loaded_partition_work(work, None, None)
					}));
				}
				let mut slots: Vec<Option<LoadedPartitionResult>> =
					(0..partition_count_usize).map(|_| None).collect();
				let ordered_output_slot_cap =
					join_output_materialization_memory_cap(self.context.task_budget.memory.get());
				let mut ordered_output_slot_bytes = 0u64;
				for handle in handles {
					let result = match handle.await {
						Ok(Ok(result)) => result,
						Ok(Err(error)) => {
							first_error.get_or_insert(error);
							continue;
						}
						Err(error) => {
							first_error.get_or_insert(ExecutionError::Kernel {
								message: format!(
									"parallel streamed inner join partition task failed: {error}"
								),
							});
							continue;
						}
					};
					if result.partition >= slots.len() {
						first_error.get_or_insert(ExecutionError::Kernel {
							message: format!(
								"parallel streamed inner join partition {} is out of range",
								result.partition
							),
						});
						continue;
					}
					if slots[result.partition].is_some() {
						first_error.get_or_insert(ExecutionError::Kernel {
							message: format!(
								"parallel streamed inner join duplicate partition {}",
								result.partition
							),
						});
						continue;
					}
					let result_output_bytes = result
						.output
						.iter()
						.map(record_batch_memory_bytes)
						.fold(0u64, u64::saturating_add);
					ordered_output_slot_bytes =
						ordered_output_slot_bytes.saturating_add(result_output_bytes);
					if ordered_output_slot_bytes > ordered_output_slot_cap {
						first_error.get_or_insert(budget_exhausted(
							BudgetDimension::Memory,
							"parallel streamed inner join ordered output slots",
							ordered_output_slot_bytes,
							ordered_output_slot_cap,
						));
						continue;
					}
					let partition = result.partition;
					slots[partition] = Some(result);
				}
				if let Some(error) = first_error {
					Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
					return Err(error);
				}
				for result in slots.into_iter().flatten() {
					total_probe_rows = total_probe_rows.saturating_add(result.probe_rows);
					total_output_rows = total_output_rows.saturating_add(result.output_rows);
					output.extend(result.output);
				}
			} else {
				for (partition, partitioned_probe) in group_probe_batches {
					let Some(mut spilled) =
						partitioned_builds.get_mut(partition).and_then(Option::take)
					else {
						continue;
					};
					if spilled.input_row_count() == 0 {
						spilled.cleanup().await;
						continue;
					}
					let memory_cap = self.context.task_budget.memory.get();
					let loaded_resident = estimate_parallel_partition_resident(
						&spilled,
						&partitioned_probe,
						key_plan,
					);
					let loaded_cap = join_spill_retained_footprint_memory_cap(memory_cap);
					if loaded_resident <= loaded_cap {
						let build_pages = match spilled.read_all_batch_pages().await {
							Ok(pages) => pages,
							Err(error) => {
								spilled.cleanup().await;
								Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds)
									.await;
								return Err(error);
							}
						};
						spilled.cleanup().await;
						let work = LoadedPartitionWork {
							partition,
							build_pages,
							probe_batch: partitioned_probe,
							probe_keys: probe_keys.to_vec(),
							write_build_keys: write_build_keys.to_vec(),
							key_plan,
							null_safe_keys,
							materialized_output_schema: materialized_output_schema.clone(),
							output_schema: output_schema.clone(),
							probe_columns_first,
							build_column_count,
							projection: projection.cloned(),
							memory_cap,
							resident_baseline_bytes: 0,
						};
						let result = match run_loaded_partition_work(work, None, None) {
							Ok(result) => result,
							Err(error) => {
								Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds)
									.await;
								return Err(error);
							}
						};
						total_probe_rows = total_probe_rows.saturating_add(result.probe_rows);
						total_output_rows = total_output_rows.saturating_add(result.output_rows);
						output.extend(result.output);
					} else {
						if let Err(error) = spilled
							.rebuild_index_from_pages(
								key_plan,
								write_build_keys,
								null_safe_keys,
								self.context.task_budget,
							)
							.await
						{
							spilled.cleanup().await;
							Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds)
								.await;
							return Err(error);
						}

						let mut partition_output = Vec::new();
						let result = self
							.visit_streaming_spilled_inner_probe_batch(
								&mut spilled,
								&partitioned_probe,
								probe_keys,
								key_plan,
								null_safe_keys,
								materialized_output_schema,
								output_schema,
								None,
								probe_columns_first,
								build_column_count,
								projection,
								None,
								None,
								&mut |batch| {
									partition_output.push(batch);
									Ok(())
								},
							)
							.await;
						spilled.cleanup().await;
						let (probe_rows, output_rows) = match result {
							Ok(result) => result,
							Err(error) => {
								Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds)
									.await;
								return Err(error);
							}
						};
						total_probe_rows = total_probe_rows.saturating_add(probe_rows);
						total_output_rows = total_output_rows.saturating_add(output_rows);
						output.extend(partition_output);
					}
				}
			}
			Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
		}
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.build_rows = task.counters.build_rows.saturating_add(total_build_rows);
			task.phases.build_micros = task
				.phases
				.build_micros
				.saturating_add(duration_micros(build_started));
			task.phases.spill_micros = task
				.phases
				.spill_micros
				.saturating_add(duration_micros(build_started));
			task.counters.spill_bytes = task
				.counters
				.spill_bytes
				.saturating_add(cumulative_spill_bytes);
		});
		Ok((
			output,
			total_probe_rows,
			total_output_rows,
			total_build_rows,
		))
	}

	async fn cleanup_partitioned_streamed_inner_builds(
		partitioned_builds: Vec<Option<SpilledInnerJoinBuild>>,
	) {
		for spilled in partitioned_builds.into_iter().flatten() {
			spilled.cleanup().await;
		}
	}

	fn record_reoriented_inner_probe(
		&self,
		runtime: &mut ReorientedInnerJoinRuntime<'_>,
		probe_rows: u64,
		output_rows: u64,
	) {
		runtime.probe_rows = runtime.probe_rows.saturating_add(probe_rows);
		runtime.output_rows = runtime.output_rows.saturating_add(output_rows);
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.probe_rows = task.counters.probe_rows.saturating_add(probe_rows);
			task.counters.matched_rows = task.counters.matched_rows.saturating_add(output_rows);
		});
	}

	fn probe_reoriented_inner_join_in_memory(
		&self,
		key_plan: JoinKeyPlan,
		probe_keys: &[crate::ir::ColumnOrdinal],
		output_schema: &SchemaRef,
		probe_columns_first: bool,
		build_batch: &RecordBatch,
		build_rows: &JoinBuildRows,
		probe_batch: &RecordBatch,
	) -> Result<(Vec<RecordBatch>, u64, u64), ExecutionError> {
		if probe_batch.num_rows() == 0 {
			return Ok((Vec::new(), 0, 0));
		}
		let probe_key_arrays = key_arrays(probe_batch, probe_keys)?;
		let mut selection = probe_in_memory_join_rows(
			JoinKind::Inner,
			key_plan,
			build_rows,
			build_batch.num_rows(),
			&probe_key_arrays,
			probe_batch.num_rows(),
			None,
			build_batch,
			probe_batch,
			false,
		)?;
		let output_rows = selection.len() as u64;
		let mut output = Vec::new();
		for chunk in selection.chunks_mut(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let batch = materialize_reoriented_inner_selection(
				probe_batch,
				build_batch,
				chunk,
				output_schema,
				self.context.task_budget.memory.get(),
				probe_columns_first,
			)?;
			if batch.num_rows() > 0 {
				output.push(batch);
			}
		}
		Ok((output, probe_batch.num_rows() as u64, output_rows))
	}
}
fn matching_preserved_rows_in_memory(
	preserved_batch: &RecordBatch,
	preserved_by_key: &HashMap<JoinKey, Vec<u32>>,
	match_filter: Option<&BoundExprProgram>,
	existence_batch: &RecordBatch,
	existence_rows: &JoinBuildRows,
) -> Result<Vec<bool>, ExecutionError> {
	let mut matched = vec![false; preserved_batch.num_rows()];
	let mut residual = JoinMatchResidual::new(match_filter, preserved_batch, existence_batch);
	for (key, preserved_rows) in preserved_by_key {
		let Some(matches) = existence_rows.get(key) else {
			continue;
		};
		for preserved_row in preserved_rows {
			if matched[*preserved_row as usize] {
				continue;
			}
			for existence_row in matches.rows() {
				if residual.matches(*preserved_row, existence_row)? {
					matched[*preserved_row as usize] = true;
					break;
				}
			}
		}
	}
	Ok(matched)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::distributed::determinism::{SortPolicy, canonical_arrow_ipc_digest};
	use crate::error::StorageError;
	use crate::ir::{
		BoundExprProgram, BoundExprProgramBuilder, BroadcastReplayEligibility, ColumnName,
		ColumnOrdinal, QueryProfileHint, ResolvedColumn, TableId,
	};
	use crate::storage::{ScanRequest, ScanStream, StorageScan};
	use arrow::array::{ArrayRef, Decimal128Array, Float64Array, Int64Array, StringArray};
	use arrow::datatypes::{DataType, Field, Schema};
	use std::collections::HashMap;
	use std::sync::{Arc, Mutex};
	use wendelon::schema::{
		LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, SchemaEvolutionPolicy,
		TableObjectId, TableSchemaDescriptor,
	};

	static NEXT_CHAIN_SEMI_TEST_DIR: std::sync::atomic::AtomicU64 =
		std::sync::atomic::AtomicU64::new(1);

	struct RecordingMultiBatchTableStorageScan {
		batches: HashMap<TableId, Vec<RecordBatch>>,
		requested_tables: Arc<Mutex<Vec<TableId>>>,
	}

	#[async_trait::async_trait]
	impl StorageScan for RecordingMultiBatchTableStorageScan {
		async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
			self.requested_tables
				.lock()
				.expect("requested tables lock")
				.push(request.table_id);
			let batches = self.batches.get(&request.table_id).cloned().ok_or(
				StorageError::TableNotFound {
					table: request.table_id,
				},
			)?;
			let schema = batches
				.first()
				.map(RecordBatch::schema)
				.unwrap_or_else(|| Arc::new(Schema::empty()));
			Ok(ScanStream::from_batches(
				schema,
				request.session_id,
				request.query_id,
				batches,
			))
		}
	}

	struct CountingFileOps {
		inner: BlockingFileOps,
		read_plain_calls: std::sync::atomic::AtomicU64,
	}

	impl CountingFileOps {
		fn new() -> Self {
			Self {
				inner: BlockingFileOps,
				read_plain_calls: std::sync::atomic::AtomicU64::new(0),
			}
		}

		fn read_plain_calls(&self) -> u64 {
			self.read_plain_calls
				.load(std::sync::atomic::Ordering::SeqCst)
		}
	}

	#[test]
	fn reoriented_scan_build_decision_checks_hash_accumulator_bytes() {
		let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
		match reoriented_stage_scan_build_decision_for_materialized_batches(
			512,
			1025,
			1024,
			vec![batch.clone()],
		) {
			ReorientedStageScanBuildDecision::StreamMaterializedBatches(batches) => {
				assert_eq!(batches.len(), 1)
			}
			_ => panic!("build hash over cap should stream while preserving measured batches"),
		}
		assert!(matches!(
			reoriented_stage_scan_build_decision_for_materialized_batches(
				1025,
				512,
				1024,
				vec![batch.clone()]
			),
			ReorientedStageScanBuildDecision::Stream
		));
		let decision = reoriented_stage_scan_build_decision_for_materialized_batches(
			512,
			1024,
			1024,
			vec![batch],
		);
		match decision {
			ReorientedStageScanBuildDecision::MaterializedBatches(batches) => {
				assert_eq!(batches.len(), 1)
			}
			_ => panic!("build should stay materialized when scan and hash estimates fit"),
		}
	}

	#[test]
	fn streamed_correlated_sort_input_budget_is_named_and_exact() {
		let schema = Arc::new(Schema::new(vec![Field::new(
			"value",
			DataType::Int64,
			false,
		)]));
		let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))])
			.expect("sort input batch");
		let batch_bytes = record_batch_memory_bytes(&batch);
		assert_eq!(
			account_streamed_correlated_sort_input(7, &batch, 7 + batch_bytes)
				.expect("exact cap fits"),
			7 + batch_bytes
		);
		let error = account_streamed_correlated_sort_input(7, &batch, 6 + batch_bytes)
			.expect_err("one byte over cap must fail");
		assert!(matches!(
			error,
			ExecutionError::BudgetExhausted {
				dimension: BudgetDimension::Memory,
				producer: STREAMED_CORRELATED_SORT_INPUT_PRODUCER,
				..
			}
		));
	}

	#[async_trait::async_trait]
	impl AsyncFileOps for CountingFileOps {
		async fn open(
			&self,
			path: &std::path::Path,
			flags: OpenFlags,
		) -> std::io::Result<FileHandle> {
			self.inner.open(path, flags).await
		}

		async fn read_plain_at(
			&self,
			handle: &FileHandle,
			offset: u64,
			len: usize,
		) -> std::io::Result<Vec<u8>> {
			self.read_plain_calls
				.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
			self.inner.read_plain_at(handle, offset, len).await
		}

		async fn write_plain_at(
			&self,
			handle: &FileHandle,
			offset: u64,
			bytes: Vec<u8>,
		) -> std::io::Result<usize> {
			self.inner.write_plain_at(handle, offset, bytes).await
		}

		async fn read_at(
			&self,
			handle: &FileHandle,
			offset: u64,
			buf: wendelon::storage::AlignedBuf,
		) -> std::io::Result<(usize, wendelon::storage::AlignedBuf)> {
			self.inner.read_at(handle, offset, buf).await
		}

		async fn write_at(
			&self,
			handle: &FileHandle,
			offset: u64,
			buf: wendelon::storage::AlignedBuf,
		) -> std::io::Result<(usize, wendelon::storage::AlignedBuf)> {
			self.inner.write_at(handle, offset, buf).await
		}

		async fn fsync(&self, handle: &FileHandle) -> std::io::Result<()> {
			self.inner.fsync(handle).await
		}

		async fn fdatasync(&self, handle: &FileHandle) -> std::io::Result<()> {
			self.inner.fdatasync(handle).await
		}

		async fn read_all(&self, path: &std::path::Path) -> std::io::Result<Vec<u8>> {
			self.inner.read_all(path).await
		}

		async fn write_all(&self, path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
			self.inner.write_all(path, bytes).await
		}

		async fn file_len(&self, path: &std::path::Path) -> std::io::Result<Option<u64>> {
			self.inner.file_len(path).await
		}

		async fn set_len(&self, handle: &FileHandle, len: u64) -> std::io::Result<()> {
			self.inner.set_len(handle, len).await
		}

		async fn create_dir_all(&self, path: &std::path::Path) -> std::io::Result<()> {
			self.inner.create_dir_all(path).await
		}

		async fn read_dir(
			&self,
			path: &std::path::Path,
		) -> std::io::Result<Vec<wendelon::storage::DirectoryEntry>> {
			self.inner.read_dir(path).await
		}

		async fn fsync_dir(&self, path: &std::path::Path) -> std::io::Result<()> {
			self.inner.fsync_dir(path).await
		}

		async fn rename(
			&self,
			from: &std::path::Path,
			to: &std::path::Path,
		) -> std::io::Result<()> {
			self.inner.rename(from, to).await
		}

		async fn unlink(&self, path: &std::path::Path) -> std::io::Result<()> {
			self.inner.unlink(path).await
		}
	}

	fn test_scan_plan_with_first_field_key(
		table_id: TableId,
		schema: Arc<Schema>,
		object_byte: u8,
	) -> ExecutionPlan {
		let primary_key = schema.field(0).name().to_string();
		let descriptor = TableSchemaDescriptor::new(
			TableObjectId::new([object_byte; 16]),
			1,
			"default",
			"join_test",
			schema.as_ref(),
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single(primary_key)),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicy::AdditiveOnly,
		)
		.expect("descriptor");
		ExecutionPlan::Scan {
			local_table_id: table_id,
			table: TableObjectId::new([object_byte; 16]),
			fingerprint: descriptor.physical_descriptor_fingerprint,
			schema,
			projection: None,
			pbc_program: None,
			row_identity: crate::storage::RowIdentityProjection::None,
			row_limit: None,
			profile_hint: QueryProfileHint::Scan,
			estimated_byte_size: None,
			primary_key_columns: vec![ColumnOrdinal(0)],
			column_ndv: Vec::new(),
			column_range: Vec::new(),
			column_range_distributions: Vec::new(),
			column_heavy_hitters: Vec::new(),
			column_group_top_k: Default::default(),
			column_group_ndv: Default::default(),
		}
	}

	fn concat_test_schema(left: &SchemaRef, right: &SchemaRef) -> SchemaRef {
		Arc::new(Schema::new(
			left.fields()
				.iter()
				.chain(right.fields().iter())
				.map(|field| field.as_ref().clone())
				.collect::<Vec<_>>(),
		))
	}

	fn inner_join_plan(
		left: ExecutionPlan,
		right: ExecutionPlan,
		left_keys: Vec<ColumnOrdinal>,
		right_keys: Vec<ColumnOrdinal>,
		output_schema: SchemaRef,
	) -> ExecutionPlan {
		ExecutionPlan::Join {
			replay_id: JoinReplayId(0),
			replay_eligibility: BroadcastReplayEligibility::Terminal,
			left: Box::new(left),
			right: Box::new(right),
			kind: JoinKind::Inner,
			left_keys,
			right_keys,
			match_filter: None,
			null_safe_keys: false,
			strategy: JoinStrategy::HashPartitioned,
			output_schema,
		}
	}

	fn left_semi_join_plan(
		left: ExecutionPlan,
		right: ExecutionPlan,
		left_keys: Vec<ColumnOrdinal>,
		right_keys: Vec<ColumnOrdinal>,
		output_schema: SchemaRef,
	) -> ExecutionPlan {
		ExecutionPlan::Join {
			replay_id: JoinReplayId::SYNTHETIC,
			replay_eligibility: BroadcastReplayEligibility::Terminal,
			left: Box::new(left),
			right: Box::new(right),
			kind: JoinKind::LeftSemi,
			left_keys,
			right_keys,
			match_filter: None,
			null_safe_keys: false,
			strategy: JoinStrategy::HashPartitioned,
			output_schema,
		}
	}

	fn exchange_plan(
		input: ExecutionPlan,
		key: BoundExprProgram,
		partition_count: u32,
	) -> ExecutionPlan {
		ExecutionPlan::Exchange {
			input: Box::new(input),
			partition_count,
			key,
		}
	}

	fn terminal_correlated_sort_fixture(
		chain_replay_id: JoinReplayId,
		top_partition_count: u32,
	) -> (Executor, ExecutionPlan, Arc<Mutex<Vec<TableId>>>, PathBuf) {
		let supplier_schema = Arc::new(Schema::new(vec![
			Field::new("s_suppkey", DataType::Int64, false),
			Field::new("s_name", DataType::Utf8, false),
		]));
		let partsupp_schema = Arc::new(Schema::new(vec![
			Field::new("ps_partkey", DataType::Int64, false),
			Field::new("ps_suppkey", DataType::Int64, false),
			Field::new("ps_supplycost", DataType::Int64, false),
		]));
		let part_schema = Arc::new(Schema::new(vec![
			Field::new("p_partkey", DataType::Int64, false),
			Field::new("p_mfgr", DataType::Utf8, false),
		]));
		let scalar_schema = Arc::new(Schema::new(vec![
			Field::new("key_0", DataType::Int64, false),
			Field::new("value", DataType::Int64, false),
		]));
		let supplier = RecordBatch::try_new(
			supplier_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![10, 20, 30])),
				Arc::new(StringArray::from(vec!["alpha", "beta", "gamma"])),
			],
		)
		.expect("supplier batch");
		let partsupp_a = RecordBatch::try_new(
			partsupp_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1, 1])),
				Arc::new(Int64Array::from(vec![10, 20])),
				Arc::new(Int64Array::from(vec![5, 5])),
			],
		)
		.expect("partsupp batch a");
		let partsupp_b = RecordBatch::try_new(
			partsupp_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1, 2])),
				Arc::new(Int64Array::from(vec![30, 20])),
				Arc::new(Int64Array::from(vec![7, 3])),
			],
		)
		.expect("partsupp batch b");
		let part = RecordBatch::try_new(
			part_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1, 2])),
				Arc::new(StringArray::from(vec!["mfgr-1", "mfgr-2"])),
			],
		)
		.expect("part batch");
		let scalar = RecordBatch::try_new(
			scalar_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1, 2])),
				Arc::new(Int64Array::from(vec![5, 3])),
			],
		)
		.expect("scalar batch");
		let requested_tables = Arc::new(Mutex::new(Vec::new()));
		let storage = Arc::new(RecordingMultiBatchTableStorageScan {
			batches: HashMap::from([
				(TableId(80), vec![supplier]),
				(TableId(81), vec![partsupp_a, partsupp_b]),
				(TableId(82), vec![part]),
				(TableId(83), vec![scalar]),
			]),
			requested_tables: requested_tables.clone(),
		});
		let spill_dir = std::env::temp_dir().join(format!(
			"toron_terminal_correlated_sort_test_{}_{}",
			std::process::id(),
			NEXT_CHAIN_SEMI_TEST_DIR.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
		));
		let executor = Executor::with_context(
			storage,
			ExecutionContext::local_test()
				.with_task_budget(high_test_budget())
				.with_join_spill_store(Arc::new(BlockingFileOps), spill_dir.clone()),
		);

		let supplier = exchange_plan(
			test_scan_plan_with_first_field_key(TableId(80), supplier_schema.clone(), 80),
			bound_column_root("s_suppkey", ColumnOrdinal(0), DataType::Int64, false),
			4,
		);
		let partsupp = exchange_plan(
			test_scan_plan_with_first_field_key(TableId(81), partsupp_schema.clone(), 81),
			bound_column_root("ps_suppkey", ColumnOrdinal(1), DataType::Int64, false),
			4,
		);
		let join0_schema = concat_test_schema(&supplier_schema, &partsupp_schema);
		let join0 = inner_join_plan(
			supplier,
			partsupp,
			vec![ColumnOrdinal(0)],
			vec![ColumnOrdinal(1)],
			join0_schema.clone(),
		);
		let base = exchange_plan(
			join0,
			bound_column_root("ps_partkey", ColumnOrdinal(2), DataType::Int64, false),
			4,
		);
		let part = exchange_plan(
			test_scan_plan_with_first_field_key(TableId(82), part_schema.clone(), 82),
			bound_column_root("p_partkey", ColumnOrdinal(0), DataType::Int64, false),
			4,
		);
		let chain_schema = concat_test_schema(&part_schema, &join0_schema);
		let chain = ExecutionPlan::Join {
			replay_id: chain_replay_id,
			replay_eligibility: BroadcastReplayEligibility::Replayable,
			left: Box::new(part),
			right: Box::new(base),
			kind: JoinKind::Inner,
			left_keys: vec![ColumnOrdinal(0)],
			right_keys: vec![ColumnOrdinal(2)],
			match_filter: None,
			null_safe_keys: false,
			strategy: JoinStrategy::HashPartitioned,
			output_schema: chain_schema.clone(),
		};
		let preserved = exchange_plan(
			chain,
			bound_column_root("p_partkey", ColumnOrdinal(0), DataType::Int64, false),
			4,
		);
		let scalar = test_scan_plan_with_first_field_key(TableId(83), scalar_schema.clone(), 83);
		let recombine_schema = concat_test_schema(&chain_schema, &scalar_schema);
		let recombine = ExecutionPlan::Join {
			replay_id: JoinReplayId(2),
			replay_eligibility: BroadcastReplayEligibility::Terminal,
			left: Box::new(preserved),
			right: Box::new(scalar),
			kind: JoinKind::LeftOuter,
			left_keys: vec![ColumnOrdinal(0)],
			right_keys: vec![ColumnOrdinal(0)],
			match_filter: None,
			null_safe_keys: false,
			strategy: JoinStrategy::HashPartitioned,
			output_schema: recombine_schema,
		};
		let filtered = ExecutionPlan::Filter {
			input: Box::new(recombine),
			predicate: binary_residual_program(
				BoundBinaryOp::Eq,
				ColumnOrdinal(6),
				DataType::Int64,
				false,
				ColumnOrdinal(8),
				DataType::Int64,
				false,
			),
			disposition: FilterDisposition::EvaluateInToron,
		};
		let project_schema = Arc::new(Schema::new(vec![
			Field::new("s_name", DataType::Utf8, false),
			Field::new("p_partkey", DataType::Int64, false),
			Field::new("ps_supplycost", DataType::Int64, false),
		]));
		let mut project_builder = BoundExprProgramBuilder::new();
		for (name, ordinal, data_type) in [
			("s_name", ColumnOrdinal(3), DataType::Utf8),
			("p_partkey", ColumnOrdinal(0), DataType::Int64),
			("ps_supplycost", ColumnOrdinal(6), DataType::Int64),
		] {
			let column = project_builder.append_column(ResolvedColumn {
				name: ColumnName(name.to_string()),
				ordinal,
				data_type,
				nullable: false,
				pbc_eligible: false,
			});
			project_builder.push_root(column);
		}
		let projected = ExecutionPlan::Project {
			input: Box::new(filtered),
			exprs: project_builder.build(),
			output_schema: project_schema.clone(),
		};
		let sort_input = exchange_plan(
			projected,
			bound_column_root("p_partkey", ColumnOrdinal(1), DataType::Int64, false),
			top_partition_count,
		);
		let sort = ExecutionPlan::Sort {
			input: Box::new(sort_input),
			order_by: vec![
				ExecutionSortKey {
					ordinal: ColumnOrdinal(2),
					descending: false,
					nulls_first: false,
				},
				ExecutionSortKey {
					ordinal: ColumnOrdinal(0),
					descending: false,
					nulls_first: false,
				},
			],
			limit: None,
			offset: 0,
			output_schema: project_schema,
		};
		(executor, sort, requested_tables, spill_dir)
	}

	#[tokio::test]
	async fn terminal_correlated_sort_streams_early_join_and_preserves_all_min_ties() {
		let (executor, plan, requested, spill_dir) =
			terminal_correlated_sort_fixture(JoinReplayId(1), 1);
		let ExecutionPlan::Sort {
			input,
			order_by,
			limit,
			offset,
			output_schema,
		} = &plan
		else {
			panic!("sort fixture")
		};
		let streamed = executor
			.try_streaming_correlated_scalar_left_outer_sort(
				input,
				order_by,
				*limit,
				*offset,
				output_schema,
				None,
			)
			.await
			.expect("streamed terminal correlated sort")
			.expect("exact shape must be admitted");

		let (baseline_executor, baseline_plan, _, baseline_spill_dir) =
			terminal_correlated_sort_fixture(JoinReplayId(2), 1);
		let materialized = baseline_executor
			.execute_bound(&baseline_plan, None)
			.await
			.expect("shape-fenced materialized sort");
		assert_record_batches_equal(&streamed, &materialized);
		let output = arrow::compute::concat_batches(&streamed[0].schema(), streamed.iter())
			.expect("streamed concat");
		assert_eq!(int64_column_values(&output, 1), vec![2, 1, 1]);
		assert_eq!(int64_column_values(&output, 2), vec![3, 5, 5]);
		assert_eq!(
			output
				.column(0)
				.as_any()
				.downcast_ref::<StringArray>()
				.expect("name column")
				.iter()
				.collect::<Vec<_>>(),
			vec![Some("beta"), Some("alpha"), Some("beta")]
		);
		let requested = requested.lock().expect("requested tables");
		for table in [TableId(80), TableId(81), TableId(82), TableId(83)] {
			assert_eq!(
				requested
					.iter()
					.filter(|requested| **requested == table)
					.count(),
				1,
				"streamed path must scan table {table:?} exactly once"
			);
		}
		drop(requested);
		let _ = std::fs::remove_dir_all(spill_dir);
		let _ = std::fs::remove_dir_all(baseline_spill_dir);
	}

	#[tokio::test]
	async fn terminal_correlated_sort_shape_fence_is_pre_io() {
		for (replay_id, top_partition_count) in [(JoinReplayId(1), 2), (JoinReplayId(2), 1)] {
			let (executor, plan, requested, spill_dir) =
				terminal_correlated_sort_fixture(replay_id, top_partition_count);
			let ExecutionPlan::Sort {
				input,
				order_by,
				limit,
				offset,
				output_schema,
			} = &plan
			else {
				panic!("sort fixture")
			};
			let output = executor
				.try_streaming_correlated_scalar_left_outer_sort(
					input,
					order_by,
					*limit,
					*offset,
					output_schema,
					None,
				)
				.await
				.expect("shape fence");
			assert!(output.is_none());
			assert!(requested.lock().expect("requested tables").is_empty());
			let _ = std::fs::remove_dir_all(spill_dir);
		}
	}

	#[allow(clippy::type_complexity)]
	fn reoriented_chain_under_semi_fixture(
		budget: TaskBudget,
		terminal_estimated_bytes: Option<u64>,
		chain_replay_id: JoinReplayId,
		membership_keys: Option<Vec<i64>>,
	) -> (
		Executor,
		ExecutionPlan,
		ExecutionPlan,
		BoundExprProgram,
		Vec<BoundAggregateCall>,
		SchemaRef,
		Arc<Mutex<Vec<TableId>>>,
	) {
		let customer_schema = Arc::new(Schema::new(vec![
			Field::new("cust_key", DataType::Int64, false),
			Field::new("customer_name", DataType::Utf8, false),
		]));
		let orders_schema = Arc::new(Schema::new(vec![
			Field::new("order_custkey", DataType::Int64, false),
			Field::new("order_key", DataType::Int64, false),
			Field::new("order_amount", DataType::Int64, false),
		]));
		let line_schema = Arc::new(Schema::new(vec![
			Field::new("line_orderkey", DataType::Int64, false),
			Field::new("quantity", DataType::Int64, false),
		]));
		let membership_schema = Arc::new(Schema::new(vec![Field::new(
			"member_orderkey",
			DataType::Int64,
			false,
		)]));
		let join0_schema = concat_test_schema(&customer_schema, &orders_schema);
		let chain_schema = concat_test_schema(&join0_schema, &line_schema);
		let customer = RecordBatch::try_new(
			customer_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1, 2])),
				Arc::new(StringArray::from(vec!["alice", "bob"])),
			],
		)
		.expect("customer batch");
		let orders = RecordBatch::try_new(
			orders_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1, 1, 2, 3])),
				Arc::new(Int64Array::from(vec![10, 11, 20, 30])),
				Arc::new(Int64Array::from(vec![100, 110, 200, 300])),
			],
		)
		.expect("orders batch");
		let line = RecordBatch::try_new(
			line_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![10, 10, 11, 20, 99])),
				Arc::new(Int64Array::from(vec![5, 7, 3, 4, 9])),
			],
		)
		.expect("line batch");
		let requested_tables = Arc::new(Mutex::new(Vec::new()));
		let mut batches = HashMap::from([
			(TableId(60), vec![customer]),
			(TableId(61), vec![orders]),
			(TableId(62), vec![line]),
		]);
		if let Some(membership_keys) = membership_keys {
			let membership = RecordBatch::try_new(
				membership_schema.clone(),
				vec![Arc::new(Int64Array::from(membership_keys))],
			)
			.expect("membership batch");
			batches.insert(TableId(63), vec![membership]);
		}
		let storage = Arc::new(RecordingMultiBatchTableStorageScan {
			batches,
			requested_tables: requested_tables.clone(),
		});
		let spill_dir = std::env::temp_dir().join(format!(
			"toron_chain_semi_test_{}_{}",
			std::process::id(),
			NEXT_CHAIN_SEMI_TEST_DIR.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
		));
		let executor = Executor::with_context(
			storage,
			ExecutionContext::local_test()
				.with_task_budget(budget)
				.with_join_spill_store(Arc::new(BlockingFileOps), spill_dir),
		);

		let customer = exchange_plan(
			test_scan_plan_with_first_field_key(TableId(60), customer_schema, 60),
			bound_column_root("cust_key", ColumnOrdinal(0), DataType::Int64, false),
			4,
		);
		let orders = exchange_plan(
			test_scan_plan_with_first_field_key(TableId(61), orders_schema, 61),
			bound_column_root("order_custkey", ColumnOrdinal(0), DataType::Int64, false),
			4,
		);
		let join0 = inner_join_plan(
			customer,
			orders,
			vec![ColumnOrdinal(0)],
			vec![ColumnOrdinal(0)],
			join0_schema.clone(),
		);
		let base = exchange_plan(
			join0,
			bound_column_root("order_key", ColumnOrdinal(3), DataType::Int64, false),
			4,
		);
		let mut line_scan = test_scan_plan_with_first_field_key(TableId(62), line_schema, 62);
		if let ExecutionPlan::Scan {
			estimated_byte_size,
			..
		} = &mut line_scan
		{
			*estimated_byte_size = terminal_estimated_bytes;
		}
		let line = exchange_plan(
			line_scan,
			bound_column_root("line_orderkey", ColumnOrdinal(0), DataType::Int64, false),
			4,
		);
		let chain = ExecutionPlan::Join {
			replay_id: chain_replay_id,
			replay_eligibility: BroadcastReplayEligibility::Replayable,
			left: Box::new(base),
			right: Box::new(line),
			kind: JoinKind::Inner,
			left_keys: vec![ColumnOrdinal(3)],
			right_keys: vec![ColumnOrdinal(0)],
			match_filter: None,
			null_safe_keys: false,
			strategy: JoinStrategy::HashPartitioned,
			output_schema: chain_schema.clone(),
		};
		let preserved = exchange_plan(
			chain,
			bound_column_root("order_key", ColumnOrdinal(3), DataType::Int64, false),
			4,
		);
		let membership = test_scan_plan_with_first_field_key(TableId(63), membership_schema, 63);
		let semi = left_semi_join_plan(
			preserved,
			membership,
			vec![ColumnOrdinal(3)],
			vec![ColumnOrdinal(0)],
			chain_schema,
		);
		let group_expr =
			bound_column_root("customer_name", ColumnOrdinal(1), DataType::Utf8, false);
		let aggr_expr = vec![BoundAggregateCall {
			function: AggregateFunction::Sum,
			args: bound_column_root("quantity", ColumnOrdinal(6), DataType::Int64, false),
		}];
		let output_schema = Arc::new(Schema::new(vec![
			Field::new("customer_name", DataType::Utf8, false),
			Field::new("sum_quantity", DataType::Int64, true),
		]));
		let aggregate_input = exchange_plan(semi, group_expr.clone(), 4);
		let aggregate = ExecutionPlan::Aggregate {
			input: Box::new(aggregate_input.clone()),
			group_expr: group_expr.clone(),
			aggr_expr: aggr_expr.clone(),
			output_schema: output_schema.clone(),
		};
		(
			executor,
			aggregate_input,
			aggregate,
			group_expr,
			aggr_expr,
			output_schema,
			requested_tables,
		)
	}

	#[tokio::test]
	async fn reoriented_chain_under_semi_is_directly_admitted_and_matches_materialized() {
		MEMBERSHIP_OUTER_WRITER_CONSTRUCT_COUNT.store(0, Ordering::SeqCst);
		let budget = TaskBudget {
			memory: ByteCount::new(64 * 1024 * 1024),
			spill_bytes: ByteCount::new(256 * 1024 * 1024),
			..TaskBudget::from_query_budget(QueryBudget::test_default())
		};
		let metadata = Some(16 * 1024 * 1024 * 1024);
		let (executor, input, _, group_expr, aggr_expr, output_schema, requested) =
			reoriented_chain_under_semi_fixture(
				budget,
				metadata,
				JoinReplayId(1),
				Some(vec![10, 10, 20, 999]),
			);
		let streamed_spill_dir = executor.context.join_spill_dir.clone();
		let streamed = executor
			.try_streaming_projected_left_semi_grouped_aggregate(
				&input,
				&group_expr,
				&aggr_expr,
				&output_schema,
				None,
			)
			.await
			.expect("direct recognizer")
			.expect("exact chain must be admitted");
		assert_eq!(
			MEMBERSHIP_OUTER_WRITER_CONSTRUCT_COUNT.load(Ordering::SeqCst),
			0,
			"membership-first composition must not construct the outer projected writer"
		);

		let (baseline_executor, _, baseline_plan, _, _, _, _) = reoriented_chain_under_semi_fixture(
			high_test_budget(),
			None,
			JoinReplayId(2),
			Some(vec![10, 10, 20, 999]),
		);
		let baseline_spill_dir = baseline_executor.context.join_spill_dir.clone();
		let materialized = baseline_executor
			.execute_bound(&baseline_plan, None)
			.await
			.expect("shape-fenced materialized baseline");
		assert_eq!(
			canonical_arrow_ipc_digest(&streamed, SortPolicy::FullRowLex).expect("streamed digest"),
			canonical_arrow_ipc_digest(&materialized, SortPolicy::FullRowLex)
				.expect("materialized digest")
		);

		let requested = requested.lock().expect("requested tables");
		for (table, expected) in [
			(TableId(60), 3),
			(TableId(61), 1),
			(TableId(62), 3),
			(TableId(63), 1),
		] {
			assert_eq!(
				requested
					.iter()
					.filter(|requested| **requested == table)
					.count(),
				expected,
				"unexpected scan count for table {table:?}"
			);
		}
		drop(requested);
		let _ = std::fs::remove_dir_all(streamed_spill_dir);
		let _ = std::fs::remove_dir_all(baseline_spill_dir);
	}

	#[tokio::test]
	async fn reoriented_chain_under_semi_metadata_and_shape_fences_are_pre_io() {
		for (metadata, replay_id) in [
			(None, JoinReplayId(1)),
			(Some(16 * 1024 * 1024 * 1024), JoinReplayId(2)),
		] {
			let (executor, input, _, group_expr, aggr_expr, output_schema, requested) =
				reoriented_chain_under_semi_fixture(
					high_test_budget(),
					metadata,
					replay_id,
					Some(vec![10, 10, 20, 999]),
				);
			let spill_dir = executor.context.join_spill_dir.clone();
			let output = executor
				.try_streaming_projected_left_semi_grouped_aggregate(
					&input,
					&group_expr,
					&aggr_expr,
					&output_schema,
					None,
				)
				.await
				.expect("fenced recognizer");
			assert!(output.is_none());
			assert!(
				requested.lock().expect("requested tables").is_empty(),
				"failed chain fences must not consume a scan"
			);
			let _ = std::fs::remove_dir_all(spill_dir);
		}

		let (executor, mut input, _, group_expr, aggr_expr, output_schema, requested) =
			reoriented_chain_under_semi_fixture(
				high_test_budget(),
				Some(32 * 1024 * 1024 * 1024),
				JoinReplayId(1),
				Some(vec![10, 10, 20, 999]),
			);
		let spill_dir = executor.context.join_spill_dir.clone();
		let extra_schema = Arc::new(Schema::new(vec![Field::new(
			"extra_orderkey",
			DataType::Int64,
			false,
		)]));
		let extra_scan = test_scan_plan_with_first_field_key(TableId(64), extra_schema.clone(), 64);
		let ExecutionPlan::Exchange { input: semi, .. } = &mut input else {
			panic!("aggregate input exchange")
		};
		let ExecutionPlan::Join {
			left: preserved, ..
		} = semi.as_mut()
		else {
			panic!("semi join")
		};
		let ExecutionPlan::Exchange { input: chain, .. } = preserved.as_mut() else {
			panic!("preserved exchange")
		};
		let original = std::mem::replace(chain, Box::new(extra_scan.clone()));
		let longer_schema = concat_test_schema(&original.schema(), &extra_schema);
		*chain = Box::new(ExecutionPlan::Join {
			replay_id: JoinReplayId(2),
			replay_eligibility: BroadcastReplayEligibility::Replayable,
			left: original,
			right: Box::new(extra_scan),
			kind: JoinKind::Inner,
			left_keys: vec![ColumnOrdinal(3)],
			right_keys: vec![ColumnOrdinal(0)],
			match_filter: None,
			null_safe_keys: false,
			strategy: JoinStrategy::HashPartitioned,
			output_schema: longer_schema,
		});
		let output = executor
			.try_streaming_projected_left_semi_grouped_aggregate(
				&input,
				&group_expr,
				&aggr_expr,
				&output_schema,
				None,
			)
			.await
			.expect("longer-chain fence");
		assert!(output.is_none());
		assert!(
			requested.lock().expect("requested tables").is_empty(),
			"longer chain fence must not consume a scan"
		);
		let _ = std::fs::remove_dir_all(spill_dir);

		for fence in ["null_safe", "composite", "nullable", "non_primitive"] {
			let (executor, mut input, _, group_expr, aggr_expr, output_schema, requested) =
				reoriented_chain_under_semi_fixture(
					TaskBudget {
						memory: ByteCount::new(64 * 1024 * 1024),
						spill_bytes: ByteCount::new(256 * 1024 * 1024),
						..TaskBudget::from_query_budget(QueryBudget::test_default())
					},
					Some(16 * 1024 * 1024 * 1024),
					JoinReplayId(1),
					Some(vec![10, 20]),
				);
			let spill_dir = executor.context.join_spill_dir.clone();
			let ExecutionPlan::Exchange { input: semi, .. } = &mut input else {
				panic!("aggregate input exchange")
			};
			let ExecutionPlan::Join {
				left_keys,
				right_keys,
				null_safe_keys,
				output_schema: semi_schema,
				..
			} = semi.as_mut()
			else {
				panic!("semi join")
			};
			match fence {
				"null_safe" => *null_safe_keys = true,
				"composite" => {
					left_keys.push(left_keys[0]);
					right_keys.push(right_keys[0]);
				}
				"nullable" | "non_primitive" => {
					let mut fields = semi_schema
						.fields()
						.iter()
						.map(|field| field.as_ref().clone())
						.collect::<Vec<_>>();
					let key_index = left_keys[0].0 as usize;
					fields[key_index] = if fence == "nullable" {
						Field::new(fields[key_index].name(), DataType::Int64, true)
					} else {
						Field::new(fields[key_index].name(), DataType::Utf8, false)
					};
					*semi_schema = Arc::new(Schema::new(fields));
				}
				_ => unreachable!(),
			}
			let admitted = executor
				.try_streaming_projected_left_semi_grouped_aggregate(
					&input,
					&group_expr,
					&aggr_expr,
					&output_schema,
					None,
				)
				.await
				.expect("direct-filter fence");
			assert!(admitted.is_none(), "{fence} must decline");
			assert!(
				requested.lock().expect("requested tables").is_empty(),
				"{fence} must decline before I/O"
			);
			let _ = std::fs::remove_dir_all(spill_dir);
		}
	}

	#[tokio::test]
	async fn reoriented_chain_under_semi_cleans_spill_after_membership_error() {
		let budget = TaskBudget {
			memory: ByteCount::new(64 * 1024 * 1024),
			spill_bytes: ByteCount::new(256 * 1024 * 1024),
			..TaskBudget::from_query_budget(QueryBudget::test_default())
		};
		let (executor, input, _, group_expr, aggr_expr, output_schema, requested) =
			reoriented_chain_under_semi_fixture(
				budget,
				Some(16 * 1024 * 1024 * 1024),
				JoinReplayId(1),
				None,
			);
		let spill_dir = executor.context.join_spill_dir.clone();
		let error = executor
			.try_streaming_projected_left_semi_grouped_aggregate(
				&input,
				&group_expr,
				&aggr_expr,
				&output_schema,
				None,
			)
			.await
			.expect_err("missing membership table must fail before the preserved chain starts");
		assert!(
			matches!(
				error,
				ExecutionError::Storage(StorageError::TableNotFound { .. })
			),
			"unexpected membership error: {error:?}"
		);
		assert!(
			matches!(
				requested.lock().expect("requested tables").as_slice(),
				[TableId(63)]
			),
			"membership-first failure must not open a preserved-side scan"
		);
		let remaining = std::fs::read_dir(&spill_dir)
			.map(|entries| entries.count())
			.unwrap_or(0);
		assert_eq!(remaining, 0, "membership error leaked spill scratch");
		let _ = std::fs::remove_dir_all(spill_dir);
	}

	#[tokio::test]
	async fn reoriented_chain_under_semi_empty_membership_skips_preserved_chain() {
		MEMBERSHIP_OUTER_WRITER_CONSTRUCT_COUNT.store(0, Ordering::SeqCst);
		let budget = TaskBudget {
			memory: ByteCount::new(64 * 1024 * 1024),
			spill_bytes: ByteCount::new(256 * 1024 * 1024),
			..TaskBudget::from_query_budget(QueryBudget::test_default())
		};
		let (executor, input, _, group_expr, aggr_expr, output_schema, requested) =
			reoriented_chain_under_semi_fixture(
				budget,
				Some(16 * 1024 * 1024 * 1024),
				JoinReplayId(1),
				Some(Vec::new()),
			);
		let spill_dir = executor.context.join_spill_dir.clone();
		let output = executor
			.try_streaming_projected_left_semi_grouped_aggregate(
				&input,
				&group_expr,
				&aggr_expr,
				&output_schema,
				None,
			)
			.await
			.expect("empty membership execution")
			.expect("exact chain remains directly admitted");
		assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
		assert_eq!(
			requested.lock().expect("requested tables").as_slice(),
			[TableId(63)],
			"empty membership must skip every preserved-side scan"
		);
		assert_eq!(
			MEMBERSHIP_OUTER_WRITER_CONSTRUCT_COUNT.load(Ordering::SeqCst),
			0
		);
		let _ = std::fs::remove_dir_all(spill_dir);
	}

	#[test]
	fn int64_presence_set_is_one_shot_duplicate_idempotent_and_full_domain_safe() {
		assert_eq!(std::mem::size_of::<i64>(), 8);
		assert_eq!(std::mem::size_of::<u32>(), 4);
		let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
		let batch = RecordBatch::try_new(
			schema,
			vec![Arc::new(Int64Array::from(vec![i64::MIN, 10, 10, i64::MAX]))],
		)
		.expect("presence batch");
		let set = Int64PresenceSet::from_batches(&[batch], ColumnOrdinal(0), 1024 * 1024)
			.expect("presence build")
			.expect("nonempty set");
		assert_eq!(set.entries.len(), 3);
		assert!(set.entries.capacity() >= 1024);
		assert_eq!(set.buckets.len(), 2048);
		assert!(set.buckets.capacity() >= 2048);
		assert_eq!(
			set.accounted_bytes(),
			summary_bytes::<i64>(set.entries.capacity())
				+ summary_bytes::<u32>(set.buckets.capacity())
		);
		assert!(set.contains(i64::MIN).expect("minimum lookup"));
		assert!(set.contains(10).expect("duplicate lookup"));
		assert!(set.contains(i64::MAX).expect("maximum lookup"));
		assert!(!set.contains(11).expect("absent lookup"));
		assert!(matches!(
			Int64PresenceSet::capacity_targets(u32::MAX as usize),
			Err(ExecutionError::Kernel { .. })
		));
	}

	#[test]
	fn membership_owner_baseline_changes_only_resident_guard_bytes() {
		let estimated_build_bytes = 16 * 1024 * 1024 * 1024;
		let memory_cap = 64 * 1024 * 1024;
		let spill_budget = 8 * 1024 * 1024 * 1024;
		let decision_without_baseline =
			streamed_inner_build_partition_count(estimated_build_bytes, memory_cap, spill_budget)
				.expect("partition decision");
		let decision_with_owner_baseline =
			streamed_inner_build_partition_count(estimated_build_bytes, memory_cap, spill_budget)
				.expect("same partition decision");
		assert_eq!(decision_without_baseline, decision_with_owner_baseline);

		let guard_memory = 1024;
		let local = join_spill_retained_footprint_memory_cap(guard_memory);
		guard_membership_owner_resident(0, local, guard_memory, "baseline test")
			.expect("local state exactly at cap");
		let error = guard_membership_owner_resident(1, local, guard_memory, "baseline test")
			.expect_err("baseline adds exactly to requested resident bytes");
		match error {
			ExecutionError::BudgetExhausted {
				requested,
				available,
				..
			} => {
				assert_eq!(requested.get(), local + 1);
				assert_eq!(available.get(), local);
			}
			other => panic!("expected resident guard crossing, got {other:?}"),
		}
	}

	#[test]
	fn int64_presence_set_construction_guard_counts_live_batches() {
		let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
		let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))])
			.expect("presence batch");
		let error = match Int64PresenceSet::from_batches(&[batch], ColumnOrdinal(0), 1024) {
			Err(error) => error,
			Ok(_) => panic!("set allocation fits alone but the live batch must cross the guard"),
		};
		assert!(matches!(
			error,
			ExecutionError::BudgetExhausted {
				producer: INT64_PRESENCE_PRODUCER,
				..
			}
		));
		assert!(
			Int64PresenceSet::from_batches(&[], ColumnOrdinal(0), 0)
				.expect("empty set")
				.is_none(),
			"empty membership must allocate nothing"
		);
	}

	#[test]
	fn membership_schedule_receipt_survives_a_post_commit_stop() {
		let directory = std::env::temp_dir().join(format!(
			"toron_membership_receipt_test_{}_{}",
			std::process::id(),
			NEXT_CHAIN_SEMI_TEST_DIR.fetch_add(1, Ordering::SeqCst)
		));
		std::fs::create_dir_all(&directory).expect("receipt directory");
		let path = directory.join("receipt.jsonl");
		{
			let mut receipt = MembershipScheduleReceipt::at_path(path.clone());
			receipt.append(
				"selected/committed",
				serde_json::json!({"outer_writer_constructed": false}),
			);
			receipt.append("membership_execute_started", serde_json::json!({}));
			// Dropping here models termination after commit and before the subtree returns.
		}
		let rows = std::fs::read_to_string(&path).expect("durable receipt");
		let parsed = rows
			.lines()
			.map(|line| serde_json::from_str::<serde_json::Value>(line).expect("receipt row"))
			.collect::<Vec<_>>();
		assert_eq!(parsed.len(), 2);
		assert_eq!(parsed[0]["milestone"], "selected/committed");
		assert_eq!(parsed[0]["detail"]["outer_writer_constructed"], false);
		assert_eq!(parsed[1]["milestone"], "membership_execute_started");
		let _ = std::fs::remove_dir_all(directory);
	}

	#[test]
	fn reoriented_inner_chain_grouped_base_classifies_filtered_and_bare_scan_probe() {
		let left_schema = Arc::new(Schema::new(vec![Field::new(
			"left_key",
			DataType::Int64,
			false,
		)]));
		let right_schema = Arc::new(Schema::new(vec![Field::new(
			"right_key",
			DataType::Int64,
			false,
		)]));
		let left = exchange_plan(
			test_scan_plan_with_first_field_key(TableId(1), left_schema.clone(), 1),
			bound_column_root("left_key", ColumnOrdinal(0), DataType::Int64, false),
			4,
		);
		let right = exchange_plan(
			test_scan_plan_with_first_field_key(TableId(2), right_schema.clone(), 2),
			bound_column_root("right_key", ColumnOrdinal(0), DataType::Int64, false),
			4,
		);
		let join = inner_join_plan(
			left,
			right,
			vec![ColumnOrdinal(0)],
			vec![ColumnOrdinal(0)],
			concat_test_schema(&left_schema, &right_schema),
		);
		let base_key = bound_column_root("left_key", ColumnOrdinal(0), DataType::Int64, false);
		let bare_base = exchange_plan(join.clone(), base_key.clone(), 4);
		assert!(matches!(
			reoriented_inner_chain_grouped_base_scan_probe_mode(&bare_base),
			Some(ReorientedInnerChainGroupedBaseScanProbeMode::BareJoin0)
		));

		let filtered_base = exchange_plan(
			ExecutionPlan::Filter {
				input: Box::new(join),
				predicate: less_than_filter_program(ColumnOrdinal(0), 10),
				disposition: FilterDisposition::EvaluateInToron,
			},
			base_key,
			4,
		);
		assert!(matches!(
			reoriented_inner_chain_grouped_base_scan_probe_mode(&filtered_base),
			Some(ReorientedInnerChainGroupedBaseScanProbeMode::FilteredJoin0)
		));
	}

	#[test]
	fn local_terminal_runtime_filter_preserves_source_ordinal_and_context_filter() {
		let schema = Arc::new(Schema::new(vec![
			Field::new("join_key", DataType::Int64, false),
			Field::new("sibling_key", DataType::Int64, false),
		]));
		let base = RecordBatch::try_new(
			schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![10, 20, 30])) as ArrayRef,
				Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
			],
		)
		.expect("base batch");
		let scan = test_scan_plan_with_first_field_key(TableId(47), schema, 47);
		let executor = Executor::with_context(
			Arc::new(RecordingMultiBatchTableStorageScan {
				batches: HashMap::new(),
				requested_tables: Arc::new(Mutex::new(Vec::new())),
			}),
			ExecutionContext::local_test(),
		);
		let descriptor = build_local_terminal_runtime_filter(
			&executor,
			std::slice::from_ref(&base),
			Some(ColumnOrdinal(0)),
			&scan,
			Some(ColumnOrdinal(0)),
			None,
		)
		.expect("supported local filter");
		assert_eq!(descriptor.probe_key_columns, vec![0]);

		let with_context_filter = Executor::with_context(
			Arc::new(RecordingMultiBatchTableStorageScan {
				batches: HashMap::new(),
				requested_tables: Arc::new(Mutex::new(Vec::new())),
			}),
			ExecutionContext::local_test().with_runtime_filter(Some(descriptor)),
		);
		assert!(
			build_local_terminal_runtime_filter(
				&with_context_filter,
				std::slice::from_ref(&base),
				Some(ColumnOrdinal(0)),
				&scan,
				Some(ColumnOrdinal(0)),
				None,
			)
			.is_none()
		);
	}

	#[test]
	fn adr446_null_safe_and_unsupported_shapes_decline() {
		assert!(!local_runtime_filter_shape_is_admitted(
			true,
			JoinKeyPlan::Int64,
			&[ColumnOrdinal(0)],
			&[ColumnOrdinal(0)],
		));
		assert!(!local_runtime_filter_shape_is_admitted(
			false,
			JoinKeyPlan::Generic,
			&[ColumnOrdinal(0)],
			&[ColumnOrdinal(0)],
		));
		assert!(!local_runtime_filter_shape_is_admitted(
			false,
			JoinKeyPlan::Int64,
			&[ColumnOrdinal(0), ColumnOrdinal(1)],
			&[ColumnOrdinal(0)],
		));
	}

	#[test]
	fn adr448_kill_switch_defaults_to_filter_on() {
		assert!(local_runtime_filter_enabled_from_env(None));
		assert!(local_runtime_filter_enabled_from_env(Some("0")));
		assert!(!local_runtime_filter_enabled_from_env(Some("1")));
		assert!(!local_runtime_filter_enabled_from_env(Some("true")));
		assert!(!local_runtime_filter_enabled_from_env(Some(" on ")));
	}

	#[test]
	fn adr492_physical_scan_build_ablation_defaults_on() {
		assert!(physical_scan_build_enabled_from_env(None));
		assert!(physical_scan_build_enabled_from_env(Some("1")));
		assert!(!physical_scan_build_enabled_from_env(Some("0")));
	}

	#[test]
	fn adr493_retain_fitting_scan_build_ablation_defaults_on() {
		assert!(retain_fitting_scan_build_enabled_from_env(None));
		assert!(retain_fitting_scan_build_enabled_from_env(Some("1")));
		assert!(!retain_fitting_scan_build_enabled_from_env(Some("0")));
	}

	#[test]
	fn adr446_descriptor_uses_source_ordinal_after_projection_reordering() {
		let base_schema = Arc::new(Schema::new(vec![
			Field::new("l_orderkey", DataType::Int64, false),
			Field::new("l_suppkey", DataType::Int64, false),
		]));
		let base = RecordBatch::try_new(
			base_schema,
			vec![
				Arc::new(Int64Array::from(vec![10, 20, 30])) as ArrayRef,
				Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
			],
		)
		.expect("base batch");
		let reordered_schema = Arc::new(Schema::new(vec![
			Field::new("l_suppkey", DataType::Int64, false),
			Field::new("l_orderkey", DataType::Int64, false),
		]));
		let scan = test_scan_plan_with_first_field_key(TableId(48), reordered_schema, 48);
		let executor = Executor::with_context(
			Arc::new(RecordingMultiBatchTableStorageScan {
				batches: HashMap::new(),
				requested_tables: Arc::new(Mutex::new(Vec::new())),
			}),
			ExecutionContext::local_test(),
		);
		let descriptor = build_local_terminal_runtime_filter(
			&executor,
			std::slice::from_ref(&base),
			Some(ColumnOrdinal(0)),
			&scan,
			Some(ColumnOrdinal(1)),
			None,
		)
		.expect("source-ordinal filter");
		assert_eq!(descriptor.probe_key_columns, vec![1]);
	}

	#[test]
	fn adr446_descriptor_maps_projected_key_to_source_ordinal() {
		let base_schema = Arc::new(Schema::new(vec![Field::new(
			"o_orderkey",
			DataType::Int64,
			false,
		)]));
		let base = RecordBatch::try_new(
			base_schema,
			vec![Arc::new(Int64Array::from(vec![10, 20, 30])) as ArrayRef],
		)
		.expect("base batch");
		let scan_schema = Arc::new(Schema::new(
			(0..6)
				.map(|ordinal| {
					Field::new(
						match ordinal {
							4 => "l_orderkey",
							5 => "l_suppkey",
							_ => "lineitem_payload",
						},
						DataType::Int64,
						false,
					)
				})
				.collect::<Vec<_>>(),
		));
		let mut scan = test_scan_plan_with_first_field_key(TableId(49), scan_schema, 49);
		let ExecutionPlan::Scan { projection, .. } = &mut scan else {
			panic!("expected scan plan");
		};
		*projection = Some(vec![ColumnOrdinal(0), ColumnOrdinal(1), ColumnOrdinal(4)]);
		let executor = Executor::with_context(
			Arc::new(RecordingMultiBatchTableStorageScan {
				batches: HashMap::new(),
				requested_tables: Arc::new(Mutex::new(Vec::new())),
			}),
			ExecutionContext::local_test(),
		);
		let descriptor = build_local_terminal_runtime_filter(
			&executor,
			std::slice::from_ref(&base),
			Some(ColumnOrdinal(0)),
			&scan,
			Some(ColumnOrdinal(2)),
			None,
		)
		.expect("projected source-ordinal filter");
		assert_eq!(descriptor.probe_key_columns, vec![4]);
	}

	#[tokio::test]
	async fn partition_reservoir_scans_source_once_and_matches_group_rescan_path() {
		let build_schema = Arc::new(Schema::new(vec![
			Field::new("build_key", DataType::Int64, false),
			Field::new("build_payload", DataType::Int64, false),
		]));
		let probe_schema = Arc::new(Schema::new(vec![
			Field::new("probe_key", DataType::Int64, false),
			Field::new("probe_tag", DataType::Int64, false),
		]));
		let materialized_output_schema = concat_test_schema(&build_schema, &probe_schema);
		let output_schema = concat_test_schema(&probe_schema, &build_schema);
		let build_rows = 64usize;
		let build_batch = RecordBatch::try_new(
			build_schema.clone(),
			vec![
				Arc::new(Int64Array::from_iter_values(
					0..i64::try_from(build_rows).expect("build rows fit"),
				)),
				Arc::new(Int64Array::from_iter_values(
					(0..i64::try_from(build_rows).expect("build rows fit")).map(|row| row * 10),
				)),
			],
		)
		.expect("build batch");
		let probe_batch = RecordBatch::try_new(
			probe_schema.clone(),
			vec![
				Arc::new(Int64Array::from_iter_values(
					(0..i64::try_from(build_rows).expect("build rows fit")).rev(),
				)),
				Arc::new(Int64Array::from_iter_values(
					(0..i64::try_from(build_rows).expect("build rows fit"))
						.rev()
						.map(|row| row * 100),
				)),
			],
		)
		.expect("probe batch");
		let build_scan = test_scan_plan_with_first_field_key(TableId(40), build_schema.clone(), 40);
		let build_exchange = exchange_plan(
			build_scan,
			bound_column_root("build_key", ColumnOrdinal(0), DataType::Int64, false),
			8,
		);
		let scan_input =
			decompose_streaming_scan_probe_input(&build_exchange).expect("build scan input");
		let budget = TaskBudget {
			memory: ByteCount::new(64 * 1024),
			spill_bytes: ByteCount::new(64 * 1024 * 1024),
			..TaskBudget::from_query_budget(QueryBudget::test_default())
		};
		let build_keys = vec![ColumnOrdinal(0)];
		let probe_keys = vec![ColumnOrdinal(0)];
		let make_executor = |requested_tables: Arc<Mutex<Vec<TableId>>>| {
			Executor::with_context(
				Arc::new(RecordingMultiBatchTableStorageScan {
					batches: HashMap::from([(TableId(40), vec![build_batch.clone()])]),
					requested_tables,
				}),
				ExecutionContext::local_test().with_task_budget(budget),
			)
		};

		let reservoir_requested_tables = Arc::new(Mutex::new(Vec::new()));
		let reservoir_executor = make_executor(reservoir_requested_tables.clone());
		let (mut reservoir, reservoir_build_rows) = reservoir_executor
			.build_partitioned_streaming_scan_inner_build_reservoir(
				scan_input,
				None,
				8,
				&build_keys,
				&build_keys,
				JoinKeyPlan::Int64,
				false,
				None,
				0,
				false,
				None,
				None,
				None,
				None,
			)
			.await
			.expect("reservoir build")
			.expect("reservoir should fit");
		assert_eq!(
			reservoir_build_rows, build_rows as u64,
			"reservoir should write every build row exactly once"
		);
		assert_eq!(
			reservoir_requested_tables
				.lock()
				.expect("reservoir requested tables")
				.iter()
				.filter(|table| **table == TableId(40))
				.count(),
			1,
			"reservoir build must scan the source once, not once per partition group"
		);
		let reservoir_probe = reservoir_executor
			.probe_partitioned_streamed_inner_scan_build_reservoir(
				&mut reservoir,
				8,
				4,
				&probe_batch,
				&probe_keys,
				&build_keys,
				JoinKeyPlan::Int64,
				false,
				&materialized_output_schema,
				&output_schema,
				true,
				build_schema.fields().len(),
				None,
				0,
				None,
				None,
			)
			.await;
		Executor::cleanup_partitioned_streamed_inner_builds(reservoir).await;
		let (reservoir_output, reservoir_probe_rows, reservoir_output_rows) =
			reservoir_probe.expect("reservoir probe");

		let profile_path = chain_stage_profile_test_path("reservoir_enabled_parity");
		let _ = std::fs::remove_file(&profile_path);
		let enabled_requested_tables = Arc::new(Mutex::new(Vec::new()));
		let enabled_executor = make_executor(enabled_requested_tables.clone());
		let profile_base = Vec::<RecordBatch>::new();
		let mut residency_profile = GroupedReservoirResidencyProfile::at_path_for_test(
			profile_path.clone(),
			&profile_base,
			profile_base.capacity(),
			8,
			4,
		)
		.expect("enabled residency profile");
		let (mut enabled_reservoir, enabled_build_rows) = enabled_executor
			.build_partitioned_streaming_scan_inner_build_reservoir(
				scan_input,
				None,
				8,
				&build_keys,
				&build_keys,
				JoinKeyPlan::Int64,
				false,
				None,
				0,
				false,
				Some(&mut residency_profile),
				None,
				None,
				None,
			)
			.await
			.expect("enabled reservoir build")
			.expect("enabled reservoir should fit");
		let enabled_probe = enabled_executor
			.probe_partitioned_streamed_inner_scan_build_reservoir(
				&mut enabled_reservoir,
				8,
				4,
				&probe_batch,
				&probe_keys,
				&build_keys,
				JoinKeyPlan::Int64,
				false,
				&materialized_output_schema,
				&output_schema,
				true,
				build_schema.fields().len(),
				None,
				0,
				None,
				None,
			)
			.await
			.expect("enabled reservoir probe");
		Executor::cleanup_partitioned_streamed_inner_builds(enabled_reservoir).await;
		assert_eq!(enabled_build_rows, reservoir_build_rows);
		assert_eq!(enabled_probe.1, reservoir_probe_rows);
		assert_eq!(enabled_probe.2, reservoir_output_rows);
		assert_eq!(
			canonical_arrow_ipc_digest(&enabled_probe.0, SortPolicy::None)
				.expect("enabled reservoir digest"),
			canonical_arrow_ipc_digest(&reservoir_output, SortPolicy::None)
				.expect("disabled reservoir digest"),
			"enabled profiling must not change reservoir output"
		);
		assert_eq!(
			enabled_requested_tables
				.lock()
				.expect("enabled requested tables")
				.as_slice(),
			reservoir_requested_tables
				.lock()
				.expect("disabled requested tables")
				.as_slice(),
			"enabled profiling must not change scan telemetry"
		);
		let profile_snapshot: serde_json::Value = serde_json::from_slice(
			&std::fs::read(&profile_path).expect("enabled profile snapshot"),
		)
		.expect("parse enabled profile snapshot");
		assert_eq!(profile_snapshot["schema_version"], 1);
		assert_eq!(profile_snapshot["phase"], "reservoir_finish");
		assert_eq!(profile_snapshot["engine"]["reservoir"]["writer_count"], 8);
		assert!(
			profile_snapshot["engine"]["reservoir"]["component_summaries"]["resident_index_bytes"]
				["aggregate_bytes"]["value"]
				.as_u64()
				.is_some()
		);
		let _ = std::fs::remove_file(profile_path);

		let group_requested_tables = Arc::new(Mutex::new(Vec::new()));
		let group_executor = make_executor(group_requested_tables.clone());
		let (group_output, group_probe_rows, group_output_rows, _) = group_executor
			.probe_partitioned_streamed_inner_scan_build(
				scan_input,
				None,
				8,
				4,
				&build_keys,
				&build_keys,
				&probe_batch,
				&probe_keys,
				JoinKeyPlan::Int64,
				false,
				&materialized_output_schema,
				&output_schema,
				true,
				build_schema.fields().len(),
				None,
			)
			.await
			.expect("group re-scan probe");
		let group_scan_count = group_requested_tables
			.lock()
			.expect("group requested tables")
			.iter()
			.filter(|table| **table == TableId(40))
			.count();
		assert!(
			group_scan_count > 1,
			"fixture must exercise the old repeated-scan comparison path"
		);

		assert_eq!(reservoir_probe_rows, group_probe_rows);
		assert_eq!(reservoir_output_rows, group_output_rows);
		assert_eq!(
			canonical_arrow_ipc_digest(&reservoir_output, SortPolicy::None)
				.expect("reservoir digest"),
			canonical_arrow_ipc_digest(&group_output, SortPolicy::None)
				.expect("group re-scan digest"),
			"one-pass reservoir output must match the old per-group scan path row-for-row"
		);
	}

	#[tokio::test]
	async fn committed_partition_reservoir_budget_failure_is_loud_and_cleans_scratch() {
		let schema = Arc::new(Schema::new(vec![Field::new(
			"build_key",
			DataType::Int64,
			false,
		)]));
		let batch = RecordBatch::try_new(
			schema.clone(),
			vec![Arc::new(Int64Array::from_iter_values(0..4096))],
		)
		.expect("build batch");
		let scan = test_scan_plan_with_first_field_key(TableId(41), schema.clone(), 41);
		let exchange = exchange_plan(
			scan,
			bound_column_root("build_key", ColumnOrdinal(0), DataType::Int64, false),
			4,
		);
		let scan_input =
			decompose_streaming_scan_probe_input(&exchange).expect("streaming scan input");
		let requested = Arc::new(Mutex::new(Vec::new()));
		let budget = TaskBudget {
			memory: ByteCount::new(64 * 1024),
			spill_bytes: ByteCount::new(64 * 1024 * 1024),
			..TaskBudget::from_query_budget(QueryBudget::test_default())
		};
		let spill_dir = chain_stage_profile_test_path("committed_cleanup").with_extension("spill");
		let _ = std::fs::remove_dir_all(&spill_dir);
		let executor = Executor::with_context(
			Arc::new(RecordingMultiBatchTableStorageScan {
				batches: HashMap::from([(TableId(41), vec![batch])]),
				requested_tables: requested.clone(),
			}),
			ExecutionContext::local_test()
				.with_task_budget(budget)
				.with_join_spill_store(Arc::new(BlockingFileOps), spill_dir.clone()),
		);
		let result = executor
			.build_partitioned_streaming_scan_inner_build_reservoir(
				scan_input,
				None,
				4,
				&[ColumnOrdinal(0)],
				&[ColumnOrdinal(0)],
				JoinKeyPlan::Int64,
				false,
				None,
				join_spill_retained_footprint_memory_cap(budget.memory.get()),
				true,
				None,
				None,
				None,
				None,
			)
			.await;
		let error = match result {
			Err(error) => error,
			Ok(_) => panic!("committed reservoir must not turn a late failure into None"),
		};
		assert!(matches!(
			error,
			ExecutionError::BudgetExhausted {
				dimension: BudgetDimension::Memory,
				..
			}
		));
		assert_eq!(
			requested.lock().expect("requested tables").as_slice(),
			&[TableId(41)],
			"failure must occur after the committed source scan starts"
		);
		let remaining = std::fs::read_dir(&spill_dir)
			.map(|entries| entries.count())
			.unwrap_or(0);
		assert_eq!(remaining, 0, "committed reservoir failure leaked scratch");
		let _ = std::fs::remove_dir_all(spill_dir);
	}

	#[tokio::test]
	async fn reoriented_inner_chain_generic_base_consumes_streamed_stage_runtime() {
		let dim0_schema = Arc::new(Schema::new(vec![
			Field::new("d0_key", DataType::Int64, false),
			Field::new("d0_label", DataType::Utf8, false),
		]));
		let dim1_schema = Arc::new(Schema::new(vec![
			Field::new("d1_key", DataType::Int64, false),
			Field::new("d1_label", DataType::Utf8, false),
		]));
		let fact_schema = Arc::new(Schema::new(vec![
			Field::new("d0_key", DataType::Int64, false),
			Field::new("d1_key", DataType::Int64, false),
			Field::new("amount", DataType::Int64, false),
		]));
		let dim0_batch = RecordBatch::try_new(
			dim0_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![0, 1])),
				Arc::new(StringArray::from(vec!["A", "B"])),
			],
		)
		.expect("dim0 batch");
		let fact_batch = RecordBatch::try_new(
			fact_schema.clone(),
			vec![
				Arc::new(Int64Array::from(
					(0..32)
						.map(|row| if row % 2 == 0 { 0 } else { 1 })
						.collect::<Vec<_>>(),
				)),
				Arc::new(Int64Array::from(
					(0..32)
						.map(|row| i64::try_from(row % 4).expect("d1 key fits"))
						.collect::<Vec<_>>(),
				)),
				Arc::new(Int64Array::from(
					(0..32)
						.map(|row| i64::try_from(row + 1).expect("amount fits"))
						.collect::<Vec<_>>(),
				)),
			],
		)
		.expect("fact batch");
		let dim1_rows = 512usize;
		let dim1_labels = (0..dim1_rows)
			.map(|row| format!("label-{row}-{}", "x".repeat(192)))
			.collect::<Vec<_>>();
		let dim1_batch = RecordBatch::try_new(
			dim1_schema.clone(),
			vec![
				Arc::new(Int64Array::from(
					(0..dim1_rows)
						.map(|row| i64::try_from(row).expect("d1 key fits"))
						.collect::<Vec<_>>(),
				)),
				Arc::new(StringArray::from(dim1_labels)),
			],
		)
		.expect("dim1 batch");

		let build_plan = |budget, wrap_aggregate_input: bool| {
			let requested_tables = Arc::new(Mutex::new(Vec::new()));
			let storage = Arc::new(RecordingMultiBatchTableStorageScan {
				batches: HashMap::from([
					(TableId(1), vec![dim0_batch.clone()]),
					(TableId(2), vec![fact_batch.clone()]),
					(TableId(3), vec![dim1_batch.clone()]),
				]),
				requested_tables: requested_tables.clone(),
			});
			let executor = Executor::with_context(
				storage,
				ExecutionContext::local_test().with_task_budget(budget),
			);
			let dim0 = exchange_plan(
				test_scan_plan_with_first_field_key(TableId(1), dim0_schema.clone(), 1),
				bound_column_root("d0_key", ColumnOrdinal(0), DataType::Int64, false),
				4,
			);
			let fact = exchange_plan(
				test_scan_plan_with_first_field_key(TableId(2), fact_schema.clone(), 2),
				bound_column_root("d0_key", ColumnOrdinal(0), DataType::Int64, false),
				4,
			);
			let join0_schema = concat_test_schema(&dim0_schema, &fact_schema);
			let join0 = inner_join_plan(
				dim0,
				fact,
				vec![ColumnOrdinal(0)],
				vec![ColumnOrdinal(0)],
				join0_schema.clone(),
			);
			let base = exchange_plan(
				ExecutionPlan::Filter {
					input: Box::new(join0),
					predicate: less_than_filter_program(ColumnOrdinal(0), 10),
					disposition: FilterDisposition::FullyPushed,
				},
				bound_column_root("d1_key", ColumnOrdinal(3), DataType::Int64, false),
				4,
			);
			assert!(
				reoriented_inner_chain_grouped_base_scan_probe_mode(&base).is_none(),
				"fixture must force the generic base traversal, not the specialized base path"
			);

			let dim1 = exchange_plan(
				test_scan_plan_with_first_field_key(TableId(3), dim1_schema.clone(), 3),
				bound_column_root("d1_key", ColumnOrdinal(0), DataType::Int64, false),
				4,
			);
			let join1_schema = concat_test_schema(&dim1_schema, &join0_schema);
			let join1 = ExecutionPlan::Join {
				replay_id: JoinReplayId(1),
				replay_eligibility: BroadcastReplayEligibility::Terminal,
				left: Box::new(dim1),
				right: Box::new(base),
				kind: JoinKind::Inner,
				left_keys: vec![ColumnOrdinal(0)],
				right_keys: vec![ColumnOrdinal(3)],
				match_filter: None,
				null_safe_keys: false,
				strategy: JoinStrategy::HashPartitioned,
				output_schema: join1_schema,
			};
			let group_expr = bound_column_root("d1_label", ColumnOrdinal(1), DataType::Utf8, false);
			let aggr_expr = vec![BoundAggregateCall {
				function: AggregateFunction::Sum,
				args: bound_column_root("amount", ColumnOrdinal(6), DataType::Int64, false),
			}];
			let output_schema = Arc::new(Schema::new(vec![
				Field::new("d1_label", DataType::Utf8, false),
				Field::new("sum_amount", DataType::Int64, true),
			]));
			let aggregate_input = if wrap_aggregate_input {
				exchange_plan(join1, group_expr.clone(), 4)
			} else {
				join1
			};
			let plan = ExecutionPlan::Aggregate {
				input: Box::new(aggregate_input),
				group_expr,
				aggr_expr,
				output_schema,
			};
			(executor, plan, requested_tables)
		};

		let low_budget = TaskBudget {
			memory: ByteCount::new(64 * 1024),
			spill_bytes: ByteCount::new(256 * 1024 * 1024),
			..TaskBudget::from_query_budget(QueryBudget::test_default())
		};
		for (wrap_aggregate_input, shape) in [
			(true, "exchange-wrapped aggregate input"),
			(false, "bare aggregate input"),
		] {
			let (streaming_executor, streaming_plan, streaming_requested_tables) =
				build_plan(low_budget, wrap_aggregate_input);
			let streamed = streaming_executor
				.execute_bound(&streaming_plan, None)
				.await
				.expect("generic base should consume streamed stage runtime");
			let dim1_scan_count = streaming_requested_tables
				.lock()
				.expect("requested tables")
				.iter()
				.filter(|table| **table == TableId(3))
				.count();
			assert_eq!(
				dim1_scan_count, 1,
				"streamed generic base should build the partition reservoir from the scan once for {shape}"
			);

			let (materialized_executor, materialized_plan, _) =
				build_plan(high_test_budget(), wrap_aggregate_input);
			let materialized = materialized_executor
				.execute_bound(&materialized_plan, None)
				.await
				.expect("materialized baseline");

			assert_eq!(
				canonical_arrow_ipc_digest(&streamed, SortPolicy::FullRowLex)
					.expect("streamed digest"),
				canonical_arrow_ipc_digest(&materialized, SortPolicy::FullRowLex)
					.expect("materialized digest"),
				"generic base traversal must feed non-in-memory stage runtimes through the streaming probe for {shape}"
			);
		}
	}

	#[test]
	fn reoriented_grouped_required_columns_cover_identity_and_projected_inputs() {
		let group_expr = bound_column_root("group_key", ColumnOrdinal(2), DataType::Int64, false);
		let aggr_arg = bound_column_root("amount", ColumnOrdinal(5), DataType::Int64, true);
		let aggr_expr = vec![BoundAggregateCall {
			function: AggregateFunction::Sum,
			args: aggr_arg,
		}];
		let outer_predicate = less_than_filter_program(ColumnOrdinal(7), 10);

		let identity_required = reoriented_grouped_required_output_columns(
			None,
			&group_expr,
			&aggr_expr,
			Some(&outer_predicate),
		);
		assert_eq!(
			identity_required,
			BTreeSet::from([ColumnOrdinal(2), ColumnOrdinal(5), ColumnOrdinal(7)])
		);

		let mut builder = BoundExprProgramBuilder::new();
		let projected_a = builder.append_column(ResolvedColumn {
			name: ColumnName("projected_a".to_string()),
			ordinal: ColumnOrdinal(1),
			data_type: DataType::Int64,
			nullable: false,
			pbc_eligible: false,
		});
		let projected_b = builder.append_column(ResolvedColumn {
			name: ColumnName("projected_b".to_string()),
			ordinal: ColumnOrdinal(4),
			data_type: DataType::Int64,
			nullable: false,
			pbc_eligible: false,
		});
		builder.push_root(projected_a);
		builder.push_root(projected_b);
		let project_exprs = builder.build();

		let projected_required = reoriented_grouped_required_output_columns(
			Some(&project_exprs),
			&group_expr,
			&aggr_expr,
			None,
		);
		assert_eq!(
			projected_required,
			BTreeSet::from([ColumnOrdinal(1), ColumnOrdinal(4)])
		);
	}

	fn less_than_filter_program(ordinal: ColumnOrdinal, upper: i64) -> BoundExprProgram {
		let mut builder = BoundExprProgramBuilder::new();
		let value = builder.append_column(ResolvedColumn {
			name: ColumnName("value".to_string()),
			ordinal,
			data_type: DataType::Int64,
			nullable: false,
			pbc_eligible: false,
		});
		let upper = builder.append_literal(ScalarValue::Int64(Some(upper)));
		let predicate = builder.append_binary(BoundBinaryOp::Lt, value, upper, DataType::Boolean);
		builder.push_root(predicate);
		builder.build()
	}

	fn high_test_budget() -> TaskBudget {
		TaskBudget {
			memory: ByteCount::new(128 * 1024 * 1024),
			spill_bytes: ByteCount::new(256 * 1024 * 1024),
			..TaskBudget::from_query_budget(QueryBudget::test_default())
		}
	}

	fn partition_parallel_batch(
		keys: &[i64],
		payload_len: usize,
		schema: SchemaRef,
	) -> RecordBatch {
		let payload = "p".repeat(payload_len);
		RecordBatch::try_new(
			schema,
			vec![
				Arc::new(Int64Array::from(keys.to_vec())),
				Arc::new(StringArray::from(vec![payload.as_str(); keys.len()])),
			],
		)
		.expect("partition parallel batch")
	}

	#[test]
	fn active_group_partitioning_matches_all_partitions_filtered() {
		let schema = Arc::new(Schema::new(vec![
			Field::new("k", DataType::Int64, false),
			Field::new("payload", DataType::Utf8, false),
		]));
		let batch = RecordBatch::try_new(
			schema,
			vec![
				Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50, 60, 70, 80])) as ArrayRef,
				Arc::new(StringArray::from(vec![
					"p0", "p2a", "p3a", "skip", "p1", "p2b", "p4", "p3b",
				])) as ArrayRef,
			],
		)
		.expect("partition test batch");
		let buckets = vec![
			Some(0),
			Some(2),
			Some(3),
			None,
			Some(1),
			Some(2),
			Some(4),
			Some(3),
		];
		let all = partition_streaming_join_batch_with_buckets(&batch, &buckets, 5)
			.expect("all partitions");
		let expected = all
			.into_iter()
			.filter(|(partition, _)| (2..4).contains(partition))
			.collect::<Vec<_>>();
		let active =
			partition_streaming_join_batch_active_group_with_buckets(&batch, &buckets, 5, 2, 4)
				.expect("active partitions");

		assert_eq!(
			active
				.iter()
				.map(|(partition, _)| *partition)
				.collect::<Vec<_>>(),
			vec![2, 3]
		);
		assert_eq!(
			active
				.iter()
				.map(|(partition, _)| *partition)
				.collect::<Vec<_>>(),
			expected
				.iter()
				.map(|(partition, _)| *partition)
				.collect::<Vec<_>>()
		);
		for ((active_partition, active_batch), (expected_partition, expected_batch)) in
			active.iter().zip(expected.iter())
		{
			assert_eq!(active_partition, expected_partition);
			assert_record_batches_equal(&[active_batch.clone()], &[expected_batch.clone()]);
		}

		let empty =
			partition_streaming_join_batch_active_group_with_buckets(&batch, &buckets, 5, 1, 1)
				.expect("empty active group");
		assert!(empty.is_empty());
	}

	#[test]
	fn loaded_partition_work_is_send_static() {
		fn assert_send_static<T: Send + 'static>() {}
		assert_send_static::<LoadedPartitionWork>();
	}

	#[tokio::test]
	async fn parallel_partition_plan_uses_active_group_max_not_first_partition() {
		let budget = high_test_budget();
		let spill_dir = std::env::temp_dir().join(format!(
			"toron_parallel_partition_skew_{}",
			std::process::id()
		));
		let _ = std::fs::remove_dir_all(&spill_dir);
		std::fs::create_dir_all(&spill_dir).expect("spill dir");
		let schema = Arc::new(Schema::new(vec![
			Field::new("k", DataType::Int64, false),
			Field::new("payload", DataType::Utf8, false),
		]));
		let mut small =
			SpilledInnerJoinBuild::create(budget, Arc::new(BlockingFileOps), spill_dir.clone())
				.await
				.expect("small spill");
		let mut large =
			SpilledInnerJoinBuild::create(budget, Arc::new(BlockingFileOps), spill_dir.clone())
				.await
				.expect("large spill");
		let keys = [ColumnOrdinal(0)];
		small
			.write_batch_chunks_unindexed(
				&partition_parallel_batch(&[1], 1, schema.clone()),
				JoinKeyPlan::Int64,
				&keys,
				false,
				budget,
			)
			.await
			.expect("small write");
		large
			.write_batch_chunks_unindexed(
				&partition_parallel_batch(&(0..256).collect::<Vec<_>>(), 2048, schema.clone()),
				JoinKeyPlan::Int64,
				&keys,
				false,
				budget,
			)
			.await
			.expect("large write");
		let probe = partition_parallel_batch(&[1], 1, schema);
		let small_est = estimate_parallel_partition_resident(&small, &probe, JoinKeyPlan::Int64);
		let large_est = estimate_parallel_partition_resident(&large, &probe, JoinKeyPlan::Int64);
		assert!(
			large_est > small_est,
			"fixture must put the larger partition after the first"
		);

		let builds = vec![Some(small), Some(large)];
		let plan = parallel_partition_plan(
			&builds,
			&[(0, probe.clone()), (1, probe)],
			JoinKeyPlan::Int64,
			8,
			budget.memory.get(),
		);
		assert_eq!(
			plan.resident_peak, large_est,
			"K must be calibrated from the active-group maximum, not the first partition"
		);
		assert!(plan.k >= 1);

		for spilled in builds.into_iter().flatten() {
			spilled.cleanup().await;
		}
		let _ = std::fs::remove_dir_all(spill_dir);
	}

	#[tokio::test]
	async fn parallel_partitioned_scan_build_matches_sequential_ordered_emit() {
		let build_schema = Arc::new(Schema::new(vec![
			Field::new("build_key", DataType::Int64, false),
			Field::new("build_payload", DataType::Utf8, false),
		]));
		let probe_schema = Arc::new(Schema::new(vec![
			Field::new("probe_key", DataType::Int64, false),
			Field::new("probe_payload", DataType::Utf8, false),
		]));
		let build_batch =
			partition_parallel_batch(&[1, 2, 3, 4, 5, 6, 7, 8], 8, build_schema.clone());
		let probe_batch =
			partition_parallel_batch(&[8, 1, 6, 3, 2, 7, 4, 5], 4, probe_schema.clone());
		let storage = Arc::new(RecordingMultiBatchTableStorageScan {
			batches: HashMap::from([(TableId(9), vec![build_batch])]),
			requested_tables: Arc::new(Mutex::new(Vec::new())),
		});
		let scan = test_scan_plan_with_first_field_key(TableId(9), build_schema.clone(), 9);
		let materialized_output_schema = concat_test_schema(&build_schema, &probe_schema);
		let output_schema = concat_test_schema(&probe_schema, &build_schema);
		let budget = high_test_budget();
		let sequential = Executor::with_context(
			storage.clone(),
			ExecutionContext::local_test()
				.with_task_budget(budget)
				.with_partition_parallelism(1),
		);
		let parallel = Executor::with_context(
			storage,
			ExecutionContext::local_test()
				.with_task_budget(budget)
				.with_partition_parallelism(4),
		);
		let scan_input = StreamingScanProbeInput {
			scan: &scan,
			filter_predicate: None,
			exchange: None,
		};

		let (sequential_output, sequential_probe_rows, sequential_output_rows, _) = sequential
			.probe_partitioned_streamed_inner_scan_build(
				scan_input,
				None,
				4,
				1,
				&[ColumnOrdinal(0)],
				&[ColumnOrdinal(0)],
				&probe_batch,
				&[ColumnOrdinal(0)],
				JoinKeyPlan::Int64,
				false,
				&materialized_output_schema,
				&output_schema,
				true,
				build_schema.fields().len(),
				None,
			)
			.await
			.expect("sequential partitioned probe");
		let (parallel_output, parallel_probe_rows, parallel_output_rows, _) = parallel
			.probe_partitioned_streamed_inner_scan_build(
				scan_input,
				None,
				4,
				1,
				&[ColumnOrdinal(0)],
				&[ColumnOrdinal(0)],
				&probe_batch,
				&[ColumnOrdinal(0)],
				JoinKeyPlan::Int64,
				false,
				&materialized_output_schema,
				&output_schema,
				true,
				build_schema.fields().len(),
				None,
			)
			.await
			.expect("parallel partitioned probe");

		assert_eq!(parallel_probe_rows, sequential_probe_rows);
		assert_eq!(parallel_output_rows, sequential_output_rows);
		assert_record_batches_equal(&parallel_output, &sequential_output);
		assert_eq!(
			canonical_arrow_ipc_digest(&parallel_output, SortPolicy::None)
				.expect("parallel digest"),
			canonical_arrow_ipc_digest(&sequential_output, SortPolicy::None)
				.expect("sequential digest")
		);
	}

	#[tokio::test]
	async fn sequential_partitioned_scan_build_loads_spill_pages_once_for_repeated_matches() {
		let build_schema = Arc::new(Schema::new(vec![
			Field::new("build_key", DataType::Int64, false),
			Field::new("build_payload", DataType::Utf8, false),
		]));
		let probe_schema = Arc::new(Schema::new(vec![
			Field::new("probe_key", DataType::Int64, false),
			Field::new("probe_payload", DataType::Utf8, false),
		]));
		let build_rows = CROSS_JOIN_OUTPUT_CHUNK_ROWS * 2;
		let build_keys = vec![1; build_rows];
		let build_batch = partition_parallel_batch(&build_keys, 1, build_schema.clone());
		let probe_batch = partition_parallel_batch(&[1, 1, 1, 1], 1, probe_schema.clone());
		let storage = Arc::new(RecordingMultiBatchTableStorageScan {
			batches: HashMap::from([(TableId(10), vec![build_batch])]),
			requested_tables: Arc::new(Mutex::new(Vec::new())),
		});
		let spill_dir = std::env::temp_dir().join(format!(
			"toron_loaded_partition_once_{}",
			std::process::id()
		));
		let _ = std::fs::remove_dir_all(&spill_dir);
		let file_ops = Arc::new(CountingFileOps::new());
		let executor = Executor::with_context(
			storage,
			ExecutionContext::local_test()
				.with_task_budget(high_test_budget())
				.with_partition_parallelism(1)
				.with_join_spill_store(file_ops.clone(), spill_dir.clone()),
		);
		let scan = test_scan_plan_with_first_field_key(TableId(10), build_schema.clone(), 10);
		let materialized_output_schema = concat_test_schema(&build_schema, &probe_schema);
		let output_schema = concat_test_schema(&probe_schema, &build_schema);

		let (output, probe_rows, output_rows, observed_build_rows) = executor
			.probe_partitioned_streamed_inner_scan_build(
				StreamingScanProbeInput {
					scan: &scan,
					filter_predicate: None,
					exchange: None,
				},
				None,
				1,
				1,
				&[ColumnOrdinal(0)],
				&[ColumnOrdinal(0)],
				&probe_batch,
				&[ColumnOrdinal(0)],
				JoinKeyPlan::Int64,
				false,
				&materialized_output_schema,
				&output_schema,
				true,
				build_schema.fields().len(),
				None,
			)
			.await
			.expect("sequential loaded partition probe");

		assert_eq!(observed_build_rows, build_rows as u64);
		assert_eq!(probe_rows, probe_batch.num_rows() as u64);
		assert_eq!(
			output_rows,
			(build_rows as u64).saturating_mul(probe_batch.num_rows() as u64)
		);
		assert_eq!(
			output.iter().map(RecordBatch::num_rows).sum::<usize>(),
			output_rows as usize
		);
		assert_eq!(
			file_ops.read_plain_calls(),
			2,
			"the resident-safe sequential branch must decode each spilled build page once, not once per output chunk"
		);
		let _ = std::fs::remove_dir_all(spill_dir);
	}

	fn filtered_scan_left_semi_grouped_aggregate_fixture(
		rows: usize,
		payload_len: usize,
		budget: TaskBudget,
	) -> (Executor, ExecutionPlan) {
		let left_schema = Arc::new(Schema::new(vec![
			Field::new("pk", DataType::Int64, false),
			Field::new("order_key", DataType::Int64, false),
			Field::new("quantity", DataType::Decimal128(20, 2), false),
			Field::new("payload", DataType::Utf8, false),
		]));
		let right_schema = Arc::new(Schema::new(vec![
			Field::new("pk", DataType::Int64, false),
			Field::new("order_key", DataType::Int64, false),
		]));
		let payload = "x".repeat(payload_len);
		let left_keys = (0..rows)
			.map(|value| i64::try_from(value).expect("row fits i64"))
			.collect::<Vec<_>>();
		let quantity = Decimal128Array::from(
			(0..rows)
				.map(|row| Some(((row % 11) as i128 + 1) * 100))
				.collect::<Vec<_>>(),
		)
		.with_precision_and_scale(20, 2)
		.expect("quantity decimal");
		let left_batch = RecordBatch::try_new(
			left_schema.clone(),
			vec![
				Arc::new(Int64Array::from(left_keys.clone())),
				Arc::new(Int64Array::from(left_keys.clone())),
				Arc::new(quantity),
				Arc::new(StringArray::from(vec![payload.as_str(); rows])),
			],
		)
		.expect("filtered scan preserved batch");
		let right_keys = (0..rows)
			.flat_map(|value| {
				let value = i64::try_from(value).expect("row fits i64");
				[value, value]
			})
			.collect::<Vec<_>>();
		let right_batch = RecordBatch::try_new(
			right_schema.clone(),
			vec![
				Arc::new(Int64Array::from(
					(0..right_keys.len())
						.map(|value| i64::try_from(value).expect("row fits i64"))
						.collect::<Vec<_>>(),
				)),
				Arc::new(Int64Array::from(right_keys)),
			],
		)
		.expect("filtered scan existence batch");
		let storage = Arc::new(RecordingMultiBatchTableStorageScan {
			batches: HashMap::from([
				(TableId(1), vec![left_batch]),
				(TableId(2), vec![right_batch]),
			]),
			requested_tables: Arc::new(Mutex::new(Vec::new())),
		});
		let executor = Executor::with_context(
			storage,
			ExecutionContext::local_test().with_task_budget(budget),
		);
		let semi = left_semi_join_plan(
			test_scan_plan_with_first_field_key(TableId(1), left_schema.clone(), 1),
			test_scan_plan_with_first_field_key(TableId(2), right_schema, 2),
			vec![ColumnOrdinal(1)],
			vec![ColumnOrdinal(1)],
			left_schema,
		);
		let filtered = ExecutionPlan::Filter {
			input: Box::new(semi),
			predicate: less_than_filter_program(ColumnOrdinal(0), 4),
			disposition: FilterDisposition::EvaluateInToron,
		};
		let group_expr = bound_column_root("order_key", ColumnOrdinal(1), DataType::Int64, false);
		let aggr_expr = vec![BoundAggregateCall {
			function: AggregateFunction::Sum,
			args: bound_column_root(
				"quantity",
				ColumnOrdinal(2),
				DataType::Decimal128(20, 2),
				false,
			),
		}];
		let output_schema = Arc::new(Schema::new(vec![
			Field::new("order_key", DataType::Int64, false),
			Field::new("sum_quantity", DataType::Decimal128(20, 2), true),
		]));
		let plan = ExecutionPlan::Aggregate {
			input: Box::new(exchange_plan(filtered, group_expr.clone(), 1)),
			group_expr,
			aggr_expr,
			output_schema,
		};
		(executor, plan)
	}

	#[tokio::test]
	async fn adr493_streaming_inner_join_scans_fitting_physical_build_once() {
		let left_schema = Arc::new(Schema::new(vec![
			Field::new("k", DataType::Int64, false),
			Field::new("left_value", DataType::Int64, false),
		]));
		let right_schema = Arc::new(Schema::new(vec![
			Field::new("k", DataType::Int64, false),
			Field::new("right_value", DataType::Int64, false),
		]));
		let left_batch = RecordBatch::try_new(
			left_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1, 2])),
				Arc::new(Int64Array::from(vec![10, 20])),
			],
		)
		.expect("left batch");
		let right_batch_one = RecordBatch::try_new(
			right_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1])),
				Arc::new(Int64Array::from(vec![100])),
			],
		)
		.expect("right batch one");
		let right_batch_two = RecordBatch::try_new(
			right_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![2])),
				Arc::new(Int64Array::from(vec![200])),
			],
		)
		.expect("right batch two");
		let requested_tables = Arc::new(Mutex::new(Vec::new()));
		let storage = Arc::new(RecordingMultiBatchTableStorageScan {
			batches: HashMap::from([
				(TableId(1), vec![left_batch]),
				(TableId(2), vec![right_batch_one, right_batch_two]),
			]),
			requested_tables: requested_tables.clone(),
		});
		let telemetry = Arc::new(Mutex::new(TaskTelemetry::default()));
		let executor = Executor::with_context(
			storage,
			ExecutionContext::local_test()
				.with_task_budget(TaskBudget {
					memory: ByteCount::new(256 * 1024 * 1024),
					spill_bytes: ByteCount::new(256 * 1024 * 1024),
					..TaskBudget::from_query_budget(QueryBudget::test_default())
				})
				.with_task_telemetry(telemetry.clone()),
		);
		let join = inner_join_plan(
			test_scan_plan_with_first_field_key(TableId(1), left_schema.clone(), 1),
			test_scan_plan_with_first_field_key(TableId(2), right_schema.clone(), 2),
			vec![ColumnOrdinal(0)],
			vec![ColumnOrdinal(0)],
			concat_test_schema(&left_schema, &right_schema),
		);
		let mut visited = Vec::new();

		let matched = executor
			.try_visit_streaming_join0_chunks(&join, None, None, None, |batch| {
				visited.push(batch);
				Ok(())
			})
			.await
			.expect("stream scan probe");

		assert!(matched.is_some());
		assert_eq!(
			visited.len(),
			2,
			"the physical right probe should stream its two scan batches"
		);
		let joined =
			concat_or_empty(join.schema(), &visited).expect("joined scan-probe output batches");
		assert_eq!(joined.num_rows(), 2);
		let left_values = joined
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("left value");
		let right_values = joined
			.column(3)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("right value");
		assert_eq!(left_values.values(), &[10, 20]);
		assert_eq!(right_values.values(), &[100, 200]);
		assert_eq!(
			requested_tables
				.lock()
				.expect("requested tables")
				.as_slice(),
			&[TableId(1), TableId(2)]
		);
		let telemetry = telemetry.lock().expect("telemetry");
		assert_eq!(telemetry.counters.spill_bytes, 0);
		assert_eq!(
			telemetry.join_spill_observations,
			vec![crate::distributed::task::JoinSpillObservation {
				replay_id: JoinReplayId(0),
				path: crate::distributed::task::JoinSpillPath::InMemory,
				spill_bytes: 0,
			}]
		);
	}

	#[tokio::test]
	async fn scan_filtered_left_semi_grouped_aggregate_bypasses_scan_output_accumulator() {
		let rows = 512usize;
		let low_budget = TaskBudget {
			memory: ByteCount::new(64 * 1024),
			spill_bytes: ByteCount::new(64 * 1024 * 1024),
			..TaskBudget::from_query_budget(QueryBudget::test_default())
		};
		let (stream_executor, plan) =
			filtered_scan_left_semi_grouped_aggregate_fixture(rows, 4096, low_budget);
		let streamed = stream_executor
			.execute_bound(&plan, None)
			.await
			.expect("streamed scan left-semi grouped aggregate");

		let (baseline_executor, baseline_plan) =
			filtered_scan_left_semi_grouped_aggregate_fixture(rows, 4096, high_test_budget());
		let materialized = baseline_executor
			.execute_bound(&baseline_plan, None)
			.await
			.expect("materialized scan left-semi grouped aggregate");

		assert_eq!(
			canonical_arrow_ipc_digest(&streamed, SortPolicy::None).expect("streamed digest"),
			canonical_arrow_ipc_digest(&materialized, SortPolicy::None)
				.expect("materialized digest"),
			"streamed scan left-semi grouped aggregate must match materialized output"
		);
	}

	#[tokio::test]
	async fn adr493_scan_build_accumulator_drops_batches_above_memory_cap() {
		let schema = Arc::new(Schema::new(vec![
			Field::new("k", DataType::Int64, false),
			Field::new("value", DataType::Int64, false),
		]));
		let batch = RecordBatch::try_new(
			schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1, 2])),
				Arc::new(Int64Array::from(vec![10, 20])),
			],
		)
		.expect("build batch");
		let requested_tables = Arc::new(Mutex::new(Vec::new()));
		let executor = Executor::new(Arc::new(RecordingMultiBatchTableStorageScan {
			batches: HashMap::from([(TableId(1), vec![batch])]),
			requested_tables: requested_tables.clone(),
		}));
		let plan = test_scan_plan_with_first_field_key(TableId(1), schema, 1);
		let scan_input = decompose_streaming_scan_probe_input(&plan).expect("scan input");

		let (estimated, resident) = executor
			.load_or_estimate_streaming_scan_inner_build(
				scan_input,
				None,
				&[ColumnOrdinal(0)],
				false,
				None,
				1,
			)
			.await
			.expect("scan build estimate");

		assert!(estimated > 1);
		assert!(resident.is_none());
		assert_eq!(
			requested_tables
				.lock()
				.expect("requested tables")
				.as_slice(),
			&[TableId(1)]
		);
	}

	#[tokio::test]
	async fn streaming_inner_join_left_scan_probe_restores_original_column_order() {
		let left_schema = Arc::new(Schema::new(vec![
			Field::new("k", DataType::Int64, false),
			Field::new("left_value", DataType::Int64, false),
		]));
		let right_schema = Arc::new(Schema::new(vec![
			Field::new("k", DataType::Int64, false),
			Field::new("right_value", DataType::Int64, false),
		]));
		let left_batch_one = RecordBatch::try_new(
			left_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1])),
				Arc::new(Int64Array::from(vec![10])),
			],
		)
		.expect("left batch one");
		let left_batch_two = RecordBatch::try_new(
			left_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![2])),
				Arc::new(Int64Array::from(vec![20])),
			],
		)
		.expect("left batch two");
		let right_batch = RecordBatch::try_new(
			right_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1, 2])),
				Arc::new(Int64Array::from(vec![100, 200])),
			],
		)
		.expect("right batch");
		let requested_tables = Arc::new(Mutex::new(Vec::new()));
		let storage = Arc::new(RecordingMultiBatchTableStorageScan {
			batches: HashMap::from([
				(TableId(1), vec![left_batch_one, left_batch_two]),
				(TableId(2), vec![right_batch]),
			]),
			requested_tables: requested_tables.clone(),
		});
		let executor = Executor::with_context(
			storage,
			ExecutionContext::local_test().with_task_budget(TaskBudget {
				memory: ByteCount::new(256 * 1024 * 1024),
				spill_bytes: ByteCount::new(256 * 1024 * 1024),
				..TaskBudget::from_query_budget(QueryBudget::test_default())
			}),
		);
		let join = inner_join_plan(
			test_scan_plan_with_first_field_key(TableId(1), left_schema.clone(), 1),
			test_scan_plan_with_first_field_key(TableId(2), right_schema.clone(), 2),
			vec![ColumnOrdinal(0)],
			vec![ColumnOrdinal(0)],
			concat_test_schema(&left_schema, &right_schema),
		);
		let mut visited = Vec::new();

		let matched = executor
			.try_visit_streaming_join0_chunks(&join, None, None, None, |batch| {
				visited.push(batch);
				Ok(())
			})
			.await
			.expect("stream left scan probe");

		assert!(matched.is_some());
		assert_eq!(
			visited.len(),
			1,
			"the physical right probe should stream after building the left side"
		);
		let joined =
			concat_or_empty(join.schema(), &visited).expect("joined scan-probe output batches");
		assert_eq!(joined.schema(), join.schema());
		let left_values = joined
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("left value");
		let right_values = joined
			.column(3)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("right value");
		assert_eq!(left_values.values(), &[10, 20]);
		assert_eq!(right_values.values(), &[100, 200]);
		assert_eq!(
			requested_tables
				.lock()
				.expect("requested tables")
				.as_slice(),
			&[TableId(1), TableId(2)]
		);
	}

	#[tokio::test]
	async fn reoriented_inner_runtime_streams_scan_build_batches_and_preserves_order() {
		for value_kind in [StreamingValueKind::Float64, StreamingValueKind::Decimal] {
			let value_type = value_kind.data_type();
			let build_schema = Arc::new(Schema::new(vec![
				Field::new("build_key", DataType::Int64, false),
				Field::new("build_value", value_type.clone(), true),
			]));
			let probe_schema = Arc::new(Schema::new(vec![
				Field::new("probe_key", DataType::Int64, false),
				Field::new("probe_tag", DataType::Int64, false),
			]));
			let output_schema = concat_test_schema(&probe_schema, &build_schema);
			let build_a = RecordBatch::try_new(
				build_schema.clone(),
				vec![
					Arc::new(Int64Array::from(vec![1, 2])),
					value_kind.values(&[100, 200]),
				],
			)
			.expect("build batch a");
			let build_b = RecordBatch::try_new(
				build_schema.clone(),
				vec![
					Arc::new(Int64Array::from(vec![3])),
					value_kind.values(&[300]),
				],
			)
			.expect("build batch b");
			let probe = RecordBatch::try_new(
				probe_schema.clone(),
				vec![
					Arc::new(Int64Array::from(vec![3, 1, 2])),
					Arc::new(Int64Array::from(vec![30, 10, 20])),
				],
			)
			.expect("probe batch");
			let requested_tables = Arc::new(Mutex::new(Vec::new()));
			let storage = Arc::new(RecordingMultiBatchTableStorageScan {
				batches: HashMap::from([(TableId(3), vec![build_a, build_b])]),
				requested_tables: requested_tables.clone(),
			});
			let executor = Executor::with_context(
				storage,
				ExecutionContext::local_test().with_task_budget(TaskBudget {
					memory: ByteCount::new(256 * 1024 * 1024),
					spill_bytes: ByteCount::new(256 * 1024 * 1024),
					..TaskBudget::from_query_budget(QueryBudget::test_default())
				}),
			);
			let scan = test_scan_plan_with_first_field_key(TableId(3), build_schema.clone(), 3);
			let build = ExecutionPlan::Exchange {
				input: Box::new(scan),
				partition_count: 1,
				key: bound_column_root("build_key", ColumnOrdinal(0), DataType::Int64, false),
			};
			let mut streamed_runtime = executor
				.prepare_reoriented_inner_join_runtime_with_streamed_scan_build(
					reoriented_scan_build_stage(
						&build,
						probe_schema.clone(),
						output_schema.clone(),
					),
					None,
				)
				.await
				.expect("streamed runtime")
				.expect("scan build should stream");
			let ReorientedInnerJoinBuild::Spilled { build: spilled, .. } = &streamed_runtime.build
			else {
				panic!("scan build side must use the streamed-spilled build");
			};
			assert_eq!(
				spilled.input_batches_written(),
				2,
				"scan build batches must be spilled one at a time, not concat_or_empty'd"
			);
			let mut materialized_runtime = executor
				.prepare_reoriented_inner_join_runtime(
					reoriented_scan_build_stage(
						&build,
						probe_schema.clone(),
						output_schema.clone(),
					),
					None,
				)
				.await
				.expect("materialized runtime")
				.expect("materialized build should prepare");

			let streamed = executor
				.probe_reoriented_inner_join_streaming(&mut streamed_runtime, &probe)
				.await
				.expect("streamed probe");
			let materialized = executor
				.probe_reoriented_inner_join_streaming(&mut materialized_runtime, &probe)
				.await
				.expect("materialized probe");
			assert_record_batches_equal(&streamed, &materialized);
			assert_eq!(
				streamed.first().expect("streamed output").schema().as_ref(),
				output_schema.as_ref(),
				"streamed spilled build must restore probe-first column order"
			);
			assert_eq!(
				requested_tables
					.lock()
					.expect("requested tables")
					.as_slice(),
				&[TableId(3), TableId(3), TableId(3), TableId(3)],
				"streamed scan build reads once for total estimation, once for per-partition spill sizing, once for the live group, and once for the materialized comparison path"
			);

			streamed_runtime.cleanup().await;
			materialized_runtime.cleanup().await;
		}
	}

	#[tokio::test]
	async fn reoriented_materialized_build_over_cap_spills_compact_from_first_batch() {
		let build_schema = Arc::new(Schema::new(vec![
			Field::new("build_key", DataType::Int64, false),
			Field::new("build_value", DataType::Int64, false),
		]));
		let probe_schema = Arc::new(Schema::new(vec![
			Field::new("probe_key", DataType::Int64, false),
			Field::new("probe_tag", DataType::Int64, false),
		]));
		let output_schema = concat_test_schema(&probe_schema, &build_schema);
		let first_rows = 5_000;
		let second_rows = 5_000;
		let mut first_keys = vec![1; first_rows];
		first_keys[first_rows - 1] = 2;
		let mut second_keys = vec![1; second_rows];
		second_keys[second_rows - 1] = 2;
		let build_a = RecordBatch::try_new(
			build_schema.clone(),
			vec![
				Arc::new(Int64Array::from(first_keys)),
				Arc::new(Int64Array::from_iter_values(0..first_rows as i64)),
			],
		)
		.expect("build batch a");
		let build_b = RecordBatch::try_new(
			build_schema.clone(),
			vec![
				Arc::new(Int64Array::from(second_keys)),
				Arc::new(Int64Array::from_iter_values(
					first_rows as i64..(first_rows + second_rows) as i64,
				)),
			],
		)
		.expect("build batch b");
		let probe = RecordBatch::try_new(
			probe_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![2])),
				Arc::new(Int64Array::from(vec![10])),
			],
		)
		.expect("probe batch");
		let build = test_scan_plan_with_first_field_key(TableId(30), build_schema.clone(), 30);
		let storage = Arc::new(RecordingMultiBatchTableStorageScan {
			batches: HashMap::new(),
			requested_tables: Arc::new(Mutex::new(Vec::new())),
		});
		let low_budget_executor = Executor::with_context(
			storage.clone(),
			ExecutionContext::local_test().with_task_budget(TaskBudget {
				memory: ByteCount::new(64 * 1024),
				spill_bytes: ByteCount::new(64 * 1024 * 1024),
				..TaskBudget::from_query_budget(QueryBudget::test_default())
			}),
		);
		let mut spilled_runtime = low_budget_executor
			.prepare_reoriented_inner_join_runtime_from_build_batches(
				reoriented_scan_build_stage(&build, probe_schema.clone(), output_schema.clone()),
				vec![build_a.clone(), build_b.clone()],
			)
			.await
			.expect("spilled materialized runtime")
			.expect("runtime");
		let ReorientedInnerJoinBuild::Spilled { build: spilled, .. } = &spilled_runtime.build
		else {
			panic!("over-cap materialized build must spill instead of erroring");
		};
		assert_eq!(
			spilled.input_batches_written(),
			2,
			"already-materialized build batches must be spilled batch-by-batch, not concat-first"
		);
		assert!(
			spilled.uses_compact_int64_index(),
			"the reoriented materialized spill path must select the compact Int64 index"
		);
		assert_eq!(
			spilled.legacy_index_insert_count(),
			0,
			"the compact path must be selected before the first build-row append"
		);
		assert_eq!(
			spilled.resident_index_bytes(),
			spilled.computed_index_bytes()
		);
		assert_eq!(spilled.input_row_count(), (first_rows + second_rows) as u64);

		let high_budget_executor = Executor::with_context(
			storage,
			ExecutionContext::local_test().with_task_budget(high_test_budget()),
		);
		let mut in_memory_runtime = high_budget_executor
			.prepare_reoriented_inner_join_runtime_from_build_batches(
				reoriented_scan_build_stage(&build, probe_schema.clone(), output_schema.clone()),
				vec![build_a, build_b],
			)
			.await
			.expect("in-memory materialized runtime")
			.expect("runtime");
		assert!(
			matches!(
				in_memory_runtime.build,
				ReorientedInnerJoinBuild::InMemory { .. }
			),
			"high-budget comparison path should still use the in-memory accumulator"
		);

		let spilled_output = low_budget_executor
			.probe_reoriented_inner_join_streaming(&mut spilled_runtime, &probe)
			.await
			.expect("spilled probe");
		let in_memory_output = high_budget_executor
			.probe_reoriented_inner_join_streaming(&mut in_memory_runtime, &probe)
			.await
			.expect("in-memory probe");
		assert_record_batches_equal(&spilled_output, &in_memory_output);
		assert_eq!(
			spilled_output
				.iter()
				.map(RecordBatch::num_rows)
				.sum::<usize>(),
			2
		);
		assert_eq!(
			spilled_output
				.first()
				.expect("spilled output")
				.schema()
				.as_ref(),
			output_schema.as_ref(),
			"spilled materialized build must restore probe-first column order"
		);

		spilled_runtime.cleanup().await;
		in_memory_runtime.cleanup().await;
	}

	#[tokio::test]
	async fn projected_reoriented_inner_runtime_remaps_keys_and_restores_observable_schema() {
		for value_kind in [StreamingValueKind::Float64, StreamingValueKind::Decimal] {
			let value_type = value_kind.data_type();
			let build_schema = Arc::new(Schema::new(vec![
				Field::new("build_key", DataType::Int64, false),
				Field::new("build_value", value_type.clone(), true),
				Field::new("dead_payload", DataType::Utf8, false),
			]));
			let probe_schema = Arc::new(Schema::new(vec![
				Field::new("probe_key", DataType::Int64, false),
				Field::new("probe_tag", DataType::Int64, false),
			]));
			let output_schema = concat_test_schema(&probe_schema, &build_schema);
			let build_a = RecordBatch::try_new(
				build_schema.clone(),
				vec![
					Arc::new(Int64Array::from(vec![1, 2])),
					value_kind.values(&[100, 200]),
					Arc::new(StringArray::from(vec!["wide-a", "wide-b"])),
				],
			)
			.expect("projected build batch a");
			let build_b = RecordBatch::try_new(
				build_schema.clone(),
				vec![
					Arc::new(Int64Array::from(vec![3])),
					value_kind.values(&[300]),
					Arc::new(StringArray::from(vec!["wide-c"])),
				],
			)
			.expect("projected build batch b");
			let probe = RecordBatch::try_new(
				probe_schema.clone(),
				vec![
					Arc::new(Int64Array::from(vec![3, 1, 2])),
					Arc::new(Int64Array::from(vec![30, 10, 20])),
				],
			)
			.expect("probe batch");
			let requested_tables = Arc::new(Mutex::new(Vec::new()));
			let storage = Arc::new(RecordingMultiBatchTableStorageScan {
				batches: HashMap::from([(TableId(4), vec![build_a, build_b])]),
				requested_tables,
			});
			let executor = Executor::with_context(
				storage,
				ExecutionContext::local_test().with_task_budget(TaskBudget {
					memory: ByteCount::new(256 * 1024 * 1024),
					spill_bytes: ByteCount::new(256 * 1024 * 1024),
					..TaskBudget::from_query_budget(QueryBudget::test_default())
				}),
			);
			let scan = test_scan_plan_with_first_field_key(TableId(4), build_schema.clone(), 4);
			let build = ExecutionPlan::Exchange {
				input: Box::new(scan),
				partition_count: 1,
				key: bound_column_root("build_key", ColumnOrdinal(0), DataType::Int64, false),
			};
			let mut projected_stage =
				reoriented_scan_build_stage(&build, probe_schema.clone(), output_schema.clone());
			projected_stage.retained_build_columns = Some(vec![ColumnOrdinal(0), ColumnOrdinal(1)]);
			let mut projected_runtime = executor
				.prepare_reoriented_inner_join_runtime_with_streamed_scan_build(
					projected_stage,
					None,
				)
				.await
				.expect("projected streamed runtime")
				.expect("projected scan build should stream");
			{
				let ReorientedInnerJoinBuild::Spilled {
					build: spilled,
					projection,
					..
				} = &mut projected_runtime.build
				else {
					panic!("projected scan build must use the streamed-spilled build");
				};
				assert!(projection.is_some(), "projection metadata must be carried");
				assert_eq!(spilled.input_batches_written(), 2);
				let page = spilled.read_batch_page(0).await.expect("projected page");
				assert_eq!(
					page.num_columns(),
					2,
					"projected reoriented build pages should omit dead payload columns"
				);
			}

			let mut materialized_runtime = executor
				.prepare_reoriented_inner_join_runtime(
					reoriented_scan_build_stage(
						&build,
						probe_schema.clone(),
						output_schema.clone(),
					),
					None,
				)
				.await
				.expect("materialized runtime")
				.expect("materialized build should prepare");

			let projected = executor
				.probe_reoriented_inner_join_streaming(&mut projected_runtime, &probe)
				.await
				.expect("projected streamed probe");
			let materialized = executor
				.probe_reoriented_inner_join_streaming(&mut materialized_runtime, &probe)
				.await
				.expect("materialized probe");
			let projected_batch =
				concat_or_empty(output_schema.clone(), &projected).expect("projected concat");
			let materialized_batch =
				concat_or_empty(output_schema.clone(), &materialized).expect("materialized concat");
			assert_eq!(projected_batch.schema(), output_schema);
			assert_eq!(projected_batch.num_rows(), materialized_batch.num_rows());
			for column in 0..4 {
				assert_eq!(
					format!("{:?}", projected_batch.column(column)),
					format!("{:?}", materialized_batch.column(column)),
					"retained output column {column} differs"
				);
			}
			let dead_payload = projected_batch
				.column(4)
				.as_any()
				.downcast_ref::<StringArray>()
				.expect("dead payload defaults");
			assert!(
				(0..dead_payload.len()).all(|row| dead_payload.value(row).is_empty()),
				"dead payload is restored only as sparse ordinal-compatible defaults"
			);

			projected_runtime.cleanup().await;
			materialized_runtime.cleanup().await;
		}
	}

	#[test]
	fn streamed_inner_build_partition_count_uses_effective_resident_cap() {
		let memory_cap = 64 * 1024 * 1024;
		let estimated_build_bytes = memory_cap * 2;
		let spill_budget = 512 * 1024 * 1024;
		let raw_cap_partitions =
			streamed_spill_partition_count(estimated_build_bytes, memory_cap).unwrap();
		let retained_cap_partitions =
			streamed_inner_build_partition_count(estimated_build_bytes, memory_cap, spill_budget)
				.unwrap();

		assert!(
			retained_cap_partitions < raw_cap_partitions,
			"streamed inner build partitioning must size against the effective retained-footprint cap, not the raw task-memory input"
		);
		assert_eq!(retained_cap_partitions, 1);
	}

	#[test]
	fn streamed_inner_build_partitioning_uses_spill_budget_and_ordered_groups() {
		let memory_cap = 4_u64 * 1024 * 1024 * 1024;
		let spill_budget = 512_u64 * 1024 * 1024;
		let estimated_build_bytes = spill_budget + 1;

		let partition_count =
			streamed_inner_build_partition_count(estimated_build_bytes, memory_cap, spill_budget)
				.expect("partition count");
		let group_count =
			streamed_inner_build_group_count(estimated_build_bytes, spill_budget, partition_count)
				.expect("group count");

		assert_eq!(
			partition_count, 8,
			"spill budget should raise P even when the memory cap alone would not"
		);
		assert_eq!(
			group_count, 2,
			"live spill grouping should bound aggregate live spill without scanning once per writer partition"
		);
		assert_eq!(
			partition_group_bounds(partition_count, group_count, 0).expect("group 0"),
			0..4
		);
		assert_eq!(
			partition_group_bounds(partition_count, group_count, 1).expect("group 1"),
			4..8
		);
	}

	#[test]
	fn streamed_inner_build_group_count_uses_max_group_spill_not_average() {
		let spill_budget = 1024;
		let group_spill_budget = streamed_inner_build_group_spill_budget(spill_budget);
		let overhead_reserve = streamed_inner_build_group_overhead_reserve_bytes(spill_budget);
		assert_eq!(group_spill_budget, 768);
		assert_eq!(overhead_reserve, 256);

		let partition_bytes = vec![400, 400, 10, 10];
		assert!(
			partition_bytes.iter().sum::<u64>() / 2 <= group_spill_budget,
			"average two-group sizing would look safe"
		);
		assert!(
			max_partition_group_spill_bytes(&partition_bytes, 2).expect("two groups")
				> group_spill_budget,
			"but the max active group is not safe"
		);

		let (group_count, max_group_spill) =
			streamed_inner_build_group_count_for_partition_spill_bytes(
				&partition_bytes,
				spill_budget,
			)
			.expect("max-bounded group count");
		assert_eq!(group_count, 3);
		assert_eq!(max_group_spill, 400);
		assert!(max_group_spill <= group_spill_budget);
	}

	#[test]
	fn streamed_inner_build_group_count_names_skew_floor_when_single_partition_exceeds_budget() {
		let spill_budget = 1024;
		let group_spill_budget = streamed_inner_build_group_spill_budget(spill_budget);
		let partition_bytes = vec![group_spill_budget + 1, 1, 1, 1];
		let error = streamed_inner_build_group_count_for_partition_spill_bytes(
			&partition_bytes,
			spill_budget,
		)
		.expect_err("hot partition must not be claimed bounded");

		match error {
			ExecutionError::BudgetExhausted {
				dimension,
				producer,
				requested,
				available,
				..
			} => {
				assert_eq!(dimension, BudgetDimension::SpillBytes);
				assert_eq!(producer, "streamed inner join build partition group sizing");
				assert_eq!(requested.get(), group_spill_budget + 1);
				assert_eq!(available.get(), group_spill_budget);
			}
			other => panic!("expected budget exhausted skew floor, got {other:?}"),
		}
	}

	#[tokio::test]
	async fn partitioned_reoriented_inner_runtime_matches_single_spill_projected_path() {
		for value_kind in [StreamingValueKind::Float64, StreamingValueKind::Decimal] {
			let rows = 8_192usize;
			let value_type = value_kind.data_type();
			let build_schema = Arc::new(Schema::new(vec![
				Field::new("dead_payload", DataType::Utf8, false),
				Field::new("build_key", DataType::Int64, false),
				Field::new("build_value", value_type.clone(), true),
			]));
			let probe_schema = Arc::new(Schema::new(vec![
				Field::new("probe_key", DataType::Int64, false),
				Field::new("probe_tag", DataType::Int64, false),
			]));
			let output_schema = concat_test_schema(&probe_schema, &build_schema);
			let build_keys = (0..rows)
				.map(|row| i64::try_from(row).expect("build key fits"))
				.collect::<Vec<_>>();
			let build_values = (0..rows)
				.map(|row| i128::try_from(row * 10).expect("value fits"))
				.collect::<Vec<_>>();
			let dead_payload = (0..rows)
				.map(|row| format!("dead-{row}"))
				.collect::<Vec<_>>();
			let build_batch = RecordBatch::try_new(
				build_schema.clone(),
				vec![
					Arc::new(StringArray::from(dead_payload)),
					Arc::new(Int64Array::from(build_keys)),
					value_kind.values(&build_values),
				],
			)
			.expect("partitioned build batch");
			let build_batches = vec![
				build_batch.slice(0, rows / 2),
				build_batch.slice(rows / 2, rows / 2),
			];
			let probe_rows = 128usize;
			let probe_keys = (0..probe_rows)
				.rev()
				.map(|row| i64::try_from(row).expect("probe key fits"))
				.collect::<Vec<_>>();
			let probe_tags = (0..probe_rows)
				.rev()
				.map(|row| i64::try_from(row * 3).expect("probe tag fits"))
				.collect::<Vec<_>>();
			let probe = RecordBatch::try_new(
				probe_schema.clone(),
				vec![
					Arc::new(Int64Array::from(probe_keys)),
					Arc::new(Int64Array::from(probe_tags)),
				],
			)
			.expect("partitioned probe batch");
			let scan = test_scan_plan_with_first_field_key(TableId(5), build_schema.clone(), 5);
			let build = ExecutionPlan::Exchange {
				input: Box::new(scan),
				partition_count: 1,
				key: bound_column_root("build_key", ColumnOrdinal(1), DataType::Int64, false),
			};
			let mut stage =
				reoriented_scan_build_stage(&build, probe_schema.clone(), output_schema.clone());
			stage.build_keys = vec![ColumnOrdinal(1)];
			stage.retained_build_columns = Some(vec![ColumnOrdinal(1), ColumnOrdinal(2)]);

			let partitioned_requested_tables = Arc::new(Mutex::new(Vec::new()));
			let partitioned_executor = Executor::with_context(
				Arc::new(RecordingMultiBatchTableStorageScan {
					batches: HashMap::from([(TableId(5), build_batches.clone())]),
					requested_tables: partitioned_requested_tables.clone(),
				}),
				ExecutionContext::local_test().with_task_budget(TaskBudget {
					memory: ByteCount::new(64 * 1024),
					spill_bytes: ByteCount::new(64 * 1024 * 1024),
					..TaskBudget::from_query_budget(QueryBudget::test_default())
				}),
			);
			let mut partitioned_runtime = partitioned_executor
				.prepare_reoriented_inner_join_runtime_with_streamed_scan_build(stage, None)
				.await
				.expect("partitioned runtime")
				.expect("partitioned scan build should prepare");
			let (partition_count, group_count) = match &partitioned_runtime.build {
				ReorientedInnerJoinBuild::PartitionedScan {
					group_count,
					partition_count,
					..
				} => (*partition_count, *group_count),
				_ => panic!("low-budget scan build should use the partitioned runtime"),
			};
			assert!(
				partition_count > 1,
				"test fixture must force more than one active build partition"
			);
			assert!(
				group_count >= 1,
				"partitioned runtime must record at least one live partition group"
			);

			let mut single_stage =
				reoriented_scan_build_stage(&build, probe_schema.clone(), output_schema.clone());
			single_stage.build_keys = vec![ColumnOrdinal(1)];
			single_stage.retained_build_columns = Some(vec![ColumnOrdinal(1), ColumnOrdinal(2)]);
			let single_executor = Executor::with_context(
				Arc::new(RecordingMultiBatchTableStorageScan {
					batches: HashMap::from([(TableId(5), build_batches)]),
					requested_tables: Arc::new(Mutex::new(Vec::new())),
				}),
				ExecutionContext::local_test().with_task_budget(TaskBudget {
					memory: ByteCount::new(256 * 1024 * 1024),
					spill_bytes: ByteCount::new(64 * 1024 * 1024),
					..TaskBudget::from_query_budget(QueryBudget::test_default())
				}),
			);
			let mut single_runtime = single_executor
				.prepare_reoriented_inner_join_runtime_with_streamed_scan_build(single_stage, None)
				.await
				.expect("single-spill runtime")
				.expect("single-spill scan build should prepare");
			let ReorientedInnerJoinBuild::Spilled { .. } = &single_runtime.build else {
				panic!("high-budget scan build should stay on the single-spill runtime");
			};

			let partitioned = partitioned_executor
				.probe_reoriented_inner_join_streaming(&mut partitioned_runtime, &probe)
				.await
				.expect("partitioned probe");
			let single = single_executor
				.probe_reoriented_inner_join_streaming(&mut single_runtime, &probe)
				.await
				.expect("single-spill probe");
			assert_eq!(
				partitioned.first().expect("partitioned output").schema(),
				single.first().expect("single output").schema()
			);
			assert_eq!(
				canonical_arrow_ipc_digest(&partitioned, SortPolicy::FullRowLex)
					.expect("partitioned digest"),
				canonical_arrow_ipc_digest(&single, SortPolicy::FullRowLex)
					.expect("single-spill digest"),
				"partitioned streamed inner build must preserve the same rows and sparse schema restoration as the single-spill projected path"
			);
			let expected_scan_count =
				2 + usize::try_from(group_count).expect("group count fits usize");
			let expected_tables = vec![TableId(5); expected_scan_count];
			assert_eq!(
				partitioned_requested_tables
					.lock()
					.expect("partitioned requested tables")
					.as_slice(),
				expected_tables.as_slice(),
				"partitioned scan build should scan once for total estimation, once for per-partition group sizing, and once per live partition group, not once per writer partition"
			);

			partitioned_runtime.cleanup().await;
			single_runtime.cleanup().await;
		}
	}

	#[tokio::test]
	async fn adaptive_derived_last_stage_build_scans_leaf_once_and_restores_order() {
		let customer_schema = Arc::new(Schema::new(vec![
			Field::new("cust_key", DataType::Int64, false),
			Field::new("cust_tag", DataType::Int64, false),
		]));
		let orders_schema = Arc::new(Schema::new(vec![
			Field::new("order_custkey", DataType::Int64, false),
			Field::new("order_key", DataType::Int64, false),
		]));
		let line_schema = Arc::new(Schema::new(vec![
			Field::new("line_orderkey", DataType::Int64, false),
			Field::new("line_value", DataType::Int64, false),
		]));
		let base_schema = concat_test_schema(&customer_schema, &orders_schema);
		let output_schema = concat_test_schema(&base_schema, &line_schema);
		let customer = RecordBatch::try_new(
			customer_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1, 2])),
				Arc::new(Int64Array::from(vec![10, 20])),
			],
		)
		.expect("customer batch");
		let orders = RecordBatch::try_new(
			orders_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1, 2, 3])),
				Arc::new(Int64Array::from(vec![100, 200, 300])),
			],
		)
		.expect("orders batch");
		let lineitem = RecordBatch::try_new(
			line_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![200, 100, 999])),
				Arc::new(Int64Array::from(vec![2_000, 1_000, 9_990])),
			],
		)
		.expect("lineitem batch");
		let requested_tables = Arc::new(Mutex::new(Vec::new()));
		let executor = Executor::with_context(
			Arc::new(RecordingMultiBatchTableStorageScan {
				batches: HashMap::from([
					(TableId(20), vec![customer]),
					(TableId(21), vec![orders]),
					(TableId(22), vec![lineitem]),
				]),
				requested_tables: requested_tables.clone(),
			}),
			ExecutionContext::local_test().with_task_budget(high_test_budget()),
		);

		let customer_scan =
			test_scan_plan_with_first_field_key(TableId(20), customer_schema.clone(), 20);
		let orders_scan = test_scan_plan_with_first_field_key(TableId(21), orders_schema, 21);
		let base_join = inner_join_plan(
			customer_scan,
			orders_scan,
			vec![ColumnOrdinal(0)],
			vec![ColumnOrdinal(0)],
			base_schema.clone(),
		);
		let base = exchange_plan(
			base_join,
			bound_column_root("order_key", ColumnOrdinal(3), DataType::Int64, false),
			1,
		);
		let line_scan = test_scan_plan_with_first_field_key(TableId(22), line_schema.clone(), 22);
		let line_exchange = exchange_plan(
			line_scan,
			bound_column_root("line_orderkey", ColumnOrdinal(0), DataType::Int64, false),
			1,
		);
		let scan_input =
			decompose_streaming_scan_probe_input(&line_exchange).expect("line scan input");
		let mut runtime = ReorientedInnerJoinRuntime {
			replay_id: JoinReplayId(1),
			strategy: JoinStrategy::HashPartitioned,
			build: ReorientedInnerJoinBuild::PartitionedScan {
				scan_input,
				morsel: None,
				partition_count: 4,
				group_count: 4,
				partition_build_keys: vec![ColumnOrdinal(0)],
				write_build_keys: vec![ColumnOrdinal(0)],
				materialized_output_schema: concat_test_schema(&line_schema, &base_schema),
				build_column_count: line_schema.fields().len(),
				projection: None,
				estimated_build_bytes: u64::MAX,
				observed_build_rows: 0,
				loaded_builds: (0..4).map(|_| None).collect(),
				loaded_resident_bytes: 0,
				loaded_build_attempted: false,
			},
			key_plan: JoinKeyPlan::Int64,
			probe_keys: vec![ColumnOrdinal(3)],
			output_schema: output_schema.clone(),
			post_filter: None,
			probe_columns_first: true,
			probe_rows: 0,
			output_rows: 0,
		};

		let mut output = Vec::new();
		let matched = executor
			.try_visit_reoriented_adaptive_derived_last_stage(
				&base,
				&mut runtime,
				None,
				None,
				&mut |batch| {
					output.push(batch);
					Ok(())
				},
			)
			.await
			.expect("adaptive derived build");
		assert!(matched.is_some(), "fitting derived side should flip");
		assert_eq!(runtime.build_row_count(), 2);
		assert_eq!(runtime.probe_rows, 3);
		assert_eq!(runtime.output_rows, 2);
		let requested = requested_tables.lock().expect("requested tables");
		assert_eq!(
			requested
				.iter()
				.filter(|table| **table == TableId(22))
				.count(),
			1,
			"adaptive flip must scan the terminal leaf side exactly once"
		);
		let output = arrow::compute::concat_batches(&output_schema, output.iter())
			.expect("adaptive output concat");
		assert_eq!(int64_column_values(&output, 0), vec![2, 1]);
		assert_eq!(int64_column_values(&output, 3), vec![200, 100]);
		assert_eq!(int64_column_values(&output, 5), vec![2_000, 1_000]);
	}

	#[tokio::test]
	async fn adaptive_derived_last_stage_declines_when_measured_build_exceeds_cap() {
		let customer_schema = Arc::new(Schema::new(vec![Field::new(
			"cust_key",
			DataType::Int64,
			false,
		)]));
		let orders_schema = Arc::new(Schema::new(vec![
			Field::new("order_custkey", DataType::Int64, false),
			Field::new("order_key", DataType::Int64, false),
		]));
		let line_schema = Arc::new(Schema::new(vec![Field::new(
			"line_orderkey",
			DataType::Int64,
			false,
		)]));
		let base_schema = concat_test_schema(&customer_schema, &orders_schema);
		let output_schema = concat_test_schema(&base_schema, &line_schema);
		let rows = 8_192usize;
		let customer = RecordBatch::try_new(
			customer_schema.clone(),
			vec![Arc::new(Int64Array::from(vec![1]))],
		)
		.expect("customer batch");
		let orders = RecordBatch::try_new(
			orders_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1; rows])),
				Arc::new(Int64Array::from_iter_values(
					0..i64::try_from(rows).expect("rows fit"),
				)),
			],
		)
		.expect("orders batch");
		let lineitem = RecordBatch::try_new(
			line_schema.clone(),
			vec![Arc::new(Int64Array::from(vec![0]))],
		)
		.expect("lineitem batch");
		let requested_tables = Arc::new(Mutex::new(Vec::new()));
		let executor = Executor::with_context(
			Arc::new(RecordingMultiBatchTableStorageScan {
				batches: HashMap::from([
					(TableId(23), vec![customer]),
					(TableId(24), vec![orders]),
					(TableId(25), vec![lineitem]),
				]),
				requested_tables: requested_tables.clone(),
			}),
			ExecutionContext::local_test().with_task_budget(TaskBudget {
				memory: ByteCount::new(32 * 1024),
				spill_bytes: ByteCount::new(64 * 1024 * 1024),
				..TaskBudget::from_query_budget(QueryBudget::test_default())
			}),
		);
		let customer_scan =
			test_scan_plan_with_first_field_key(TableId(23), customer_schema.clone(), 23);
		let orders_scan = test_scan_plan_with_first_field_key(TableId(24), orders_schema, 24);
		let base_join = inner_join_plan(
			customer_scan,
			orders_scan,
			vec![ColumnOrdinal(0)],
			vec![ColumnOrdinal(0)],
			base_schema.clone(),
		);
		let base = exchange_plan(
			base_join,
			bound_column_root("order_key", ColumnOrdinal(2), DataType::Int64, false),
			1,
		);
		let line_scan = test_scan_plan_with_first_field_key(TableId(25), line_schema.clone(), 25);
		let line_exchange = exchange_plan(
			line_scan,
			bound_column_root("line_orderkey", ColumnOrdinal(0), DataType::Int64, false),
			1,
		);
		let scan_input =
			decompose_streaming_scan_probe_input(&line_exchange).expect("line scan input");
		let mut runtime = ReorientedInnerJoinRuntime {
			replay_id: JoinReplayId(1),
			strategy: JoinStrategy::HashPartitioned,
			build: ReorientedInnerJoinBuild::PartitionedScan {
				scan_input,
				morsel: None,
				partition_count: 4,
				group_count: 4,
				partition_build_keys: vec![ColumnOrdinal(0)],
				write_build_keys: vec![ColumnOrdinal(0)],
				materialized_output_schema: concat_test_schema(&line_schema, &base_schema),
				build_column_count: line_schema.fields().len(),
				projection: None,
				estimated_build_bytes: u64::MAX,
				observed_build_rows: 0,
				loaded_builds: (0..4).map(|_| None).collect(),
				loaded_resident_bytes: 0,
				loaded_build_attempted: false,
			},
			key_plan: JoinKeyPlan::Int64,
			probe_keys: vec![ColumnOrdinal(2)],
			output_schema,
			post_filter: None,
			probe_columns_first: true,
			probe_rows: 0,
			output_rows: 0,
		};
		let matched = executor
			.try_visit_reoriented_adaptive_derived_last_stage(
				&base,
				&mut runtime,
				None,
				None,
				&mut |_batch| Ok(()),
			)
			.await
			.expect("adaptive derived build");
		assert!(
			matched.is_none(),
			"oversized measured derived side must decline the adaptive flip"
		);
		assert_eq!(
			requested_tables
				.lock()
				.expect("requested tables")
				.iter()
				.filter(|table| **table == TableId(25))
				.count(),
			0,
			"declined flip must not scan the terminal leaf side"
		);
	}

	#[tokio::test]
	async fn projected_semi_anti_existence_build_keys_only_matches_unprojected() {
		let existence_schema = Arc::new(Schema::new(vec![
			Field::new("order_key", DataType::Int64, false),
			Field::new("dead_payload", DataType::Utf8, false),
			Field::new("dead_value", DataType::Int64, false),
		]));
		let preserved_schema = Arc::new(Schema::new(vec![Field::new(
			"order_key",
			DataType::Int64,
			false,
		)]));
		let existence = RecordBatch::try_new(
			existence_schema,
			vec![
				Arc::new(Int64Array::from(vec![1, 3])),
				Arc::new(StringArray::from(vec!["unused-a", "unused-b"])),
				Arc::new(Int64Array::from(vec![10, 30])),
			],
		)
		.expect("existence batch");
		let preserved = RecordBatch::try_new(
			preserved_schema.clone(),
			vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
		)
		.expect("preserved batch");
		let preserved_by_key = HashMap::from([
			(JoinKey::Int64(1), vec![0]),
			(JoinKey::Int64(2), vec![1]),
			(JoinKey::Int64(3), vec![2]),
		]);
		let keys = vec![ColumnOrdinal(0)];
		let mut projected = projected_existence_spill(
			existence.clone(),
			preserved_schema.clone(),
			&keys,
			vec![ColumnOrdinal(0)],
		)
		.await;
		let page = projected.read_batch_page(0).await.expect("projected page");
		assert_eq!(
			page.num_columns(),
			1,
			"keys-only semi existence spill should not write dead payload columns"
		);
		let mut unprojected = unprojected_existence_spill(existence, preserved_schema, &keys).await;

		let projected_matches = projected
			.matching_preserved_rows(&preserved, &preserved_by_key, None)
			.await
			.expect("projected keys-only matches");
		let unprojected_matches = unprojected
			.matching_preserved_rows(&preserved, &preserved_by_key, None)
			.await
			.expect("unprojected keys-only matches");

		assert_eq!(projected_matches, unprojected_matches);
		assert_eq!(projected_matches, vec![true, false, true]);
		projected.cleanup().await;
		unprojected.cleanup().await;
	}

	#[tokio::test]
	async fn membership_only_projected_semi_anti_existence_writes_no_pages() {
		let existence_schema = Arc::new(Schema::new(vec![
			Field::new("order_key", DataType::Int64, false),
			Field::new("dead_payload", DataType::Utf8, false),
		]));
		let preserved_schema = Arc::new(Schema::new(vec![Field::new(
			"order_key",
			DataType::Int64,
			false,
		)]));
		let existence = RecordBatch::try_new(
			existence_schema,
			vec![
				Arc::new(Int64Array::from(vec![1, 1, 3])),
				Arc::new(StringArray::from(vec!["unused-a", "unused-b", "unused-c"])),
			],
		)
		.expect("existence batch");
		let preserved = RecordBatch::try_new(
			preserved_schema.clone(),
			vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
		)
		.expect("preserved batch");
		let preserved_by_key = HashMap::from([
			(JoinKey::Int64(1), vec![0]),
			(JoinKey::Int64(2), vec![1]),
			(JoinKey::Int64(3), vec![2]),
		]);
		let keys = vec![ColumnOrdinal(0)];
		let mut projected = projected_membership_existence_spill(
			existence.clone(),
			preserved_schema.clone(),
			&keys,
			vec![ColumnOrdinal(0)],
		)
		.await;
		assert_eq!(
			projected.page_count(),
			0,
			"membership-only semi/anti existence should answer from the key index"
		);
		assert_eq!(
			projected.spill_bytes(),
			0,
			"membership-only semi/anti existence should not write page payloads"
		);
		let mut unprojected = unprojected_existence_spill(existence, preserved_schema, &keys).await;

		let projected_matches = projected
			.matching_preserved_rows(&preserved, &preserved_by_key, None)
			.await
			.expect("projected membership matches");
		let unprojected_matches = unprojected
			.matching_preserved_rows(&preserved, &preserved_by_key, None)
			.await
			.expect("unprojected membership matches");

		assert_eq!(projected_matches, unprojected_matches);
		assert_eq!(projected_matches, vec![true, false, true]);
		projected.cleanup().await;
		unprojected.cleanup().await;
	}

	#[tokio::test]
	async fn projected_semi_anti_existence_build_retains_residual_columns() {
		let existence_schema = Arc::new(Schema::new(vec![
			Field::new("order_key", DataType::Int64, false),
			Field::new("supp_key", DataType::Int64, false),
			Field::new("dead_payload", DataType::Utf8, false),
		]));
		let preserved_schema = Arc::new(Schema::new(vec![
			Field::new("order_key", DataType::Int64, false),
			Field::new("supp_key", DataType::Int64, false),
		]));
		let existence = RecordBatch::try_new(
			existence_schema,
			vec![
				Arc::new(Int64Array::from(vec![10, 10, 20])),
				Arc::new(Int64Array::from(vec![1, 2, 1])),
				Arc::new(StringArray::from(vec!["dead-a", "dead-b", "dead-c"])),
			],
		)
		.expect("existence batch");
		let preserved = RecordBatch::try_new(
			preserved_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![10, 10, 20, 30])),
				Arc::new(Int64Array::from(vec![1, 2, 1, 9])),
			],
		)
		.expect("preserved batch");
		let preserved_by_key = HashMap::from([
			(JoinKey::Int64(10), vec![0, 1]),
			(JoinKey::Int64(20), vec![2]),
			(JoinKey::Int64(30), vec![3]),
		]);
		let keys = vec![ColumnOrdinal(0)];
		let mut projected = projected_existence_spill(
			existence.clone(),
			preserved_schema.clone(),
			&keys,
			vec![ColumnOrdinal(0), ColumnOrdinal(1)],
		)
		.await;
		let page = projected.read_batch_page(0).await.expect("projected page");
		assert_eq!(
			page.num_columns(),
			2,
			"residual semi/anti existence spill must keep key plus residual payload only"
		);
		let mut unprojected = unprojected_existence_spill(existence, preserved_schema, &keys).await;
		let residual = not_equal_residual_program(
			ColumnOrdinal(1),
			ColumnOrdinal(3),
			"preserved_supp",
			"existence_supp",
		);

		let projected_matches = projected
			.matching_preserved_rows(&preserved, &preserved_by_key, Some(&residual))
			.await
			.expect("projected residual matches");
		let unprojected_matches = unprojected
			.matching_preserved_rows(&preserved, &preserved_by_key, Some(&residual))
			.await
			.expect("unprojected residual matches");

		assert_eq!(projected_matches, unprojected_matches);
		assert_eq!(projected_matches, vec![true, true, false, false]);
		projected.cleanup().await;
		unprojected.cleanup().await;
	}

	#[derive(Clone, Copy)]
	enum StreamingValueKind {
		Float64,
		Decimal,
	}

	impl StreamingValueKind {
		fn data_type(self) -> DataType {
			match self {
				Self::Float64 => DataType::Float64,
				Self::Decimal => DataType::Decimal128(20, 2),
			}
		}

		fn values(self, cents: &[i128]) -> ArrayRef {
			match self {
				Self::Float64 => Arc::new(Float64Array::from(
					cents
						.iter()
						.map(|value| Some(*value as f64 / 100.0))
						.collect::<Vec<_>>(),
				)),
				Self::Decimal => Arc::new(
					Decimal128Array::from(
						cents.iter().map(|value| Some(*value)).collect::<Vec<_>>(),
					)
					.with_precision_and_scale(20, 2)
					.expect("decimal build values"),
				),
			}
		}
	}

	fn bound_column_root(
		name: &str,
		ordinal: ColumnOrdinal,
		data_type: DataType,
		nullable: bool,
	) -> BoundExprProgram {
		let mut builder = BoundExprProgramBuilder::new();
		let column = builder.append_column(ResolvedColumn {
			name: ColumnName(name.to_string()),
			ordinal,
			data_type,
			nullable,
			pbc_eligible: false,
		});
		builder.push_root(column);
		builder.build()
	}

	fn not_equal_residual_program(
		left_ordinal: ColumnOrdinal,
		right_ordinal: ColumnOrdinal,
		left_name: &str,
		right_name: &str,
	) -> BoundExprProgram {
		let mut builder = BoundExprProgramBuilder::new();
		let left = builder.append_column(ResolvedColumn {
			name: ColumnName(left_name.to_string()),
			ordinal: left_ordinal,
			data_type: DataType::Int64,
			nullable: false,
			pbc_eligible: false,
		});
		let right = builder.append_column(ResolvedColumn {
			name: ColumnName(right_name.to_string()),
			ordinal: right_ordinal,
			data_type: DataType::Int64,
			nullable: false,
			pbc_eligible: false,
		});
		let predicate = builder.append_binary(BoundBinaryOp::Neq, left, right, DataType::Boolean);
		builder.push_root(predicate);
		builder.build()
	}

	fn binary_residual_program(
		op: BoundBinaryOp,
		left_ordinal: ColumnOrdinal,
		left_type: DataType,
		left_nullable: bool,
		right_ordinal: ColumnOrdinal,
		right_type: DataType,
		right_nullable: bool,
	) -> BoundExprProgram {
		let mut builder = BoundExprProgramBuilder::new();
		let left = builder.append_column(ResolvedColumn {
			name: ColumnName("left".to_string()),
			ordinal: left_ordinal,
			data_type: left_type,
			nullable: left_nullable,
			pbc_eligible: false,
		});
		let right = builder.append_column(ResolvedColumn {
			name: ColumnName("right".to_string()),
			ordinal: right_ordinal,
			data_type: right_type,
			nullable: right_nullable,
			pbc_eligible: false,
		});
		let predicate = builder.append_binary(op, left, right, DataType::Boolean);
		builder.push_root(predicate);
		builder.build()
	}

	fn recognized_summary_stage<'a>(
		kind: JoinKind,
		replay_id: u32,
		existence: &'a ExecutionPlan,
		match_filter: &'a BoundExprProgram,
		preserved_schema: SchemaRef,
	) -> StreamingSemiAntiStage<'a> {
		StreamingSemiAntiStage {
			replay_id: JoinReplayId(replay_id),
			kind,
			preserved_keys: vec![ColumnOrdinal(0)],
			existence,
			existence_keys: vec![ColumnOrdinal(0)],
			match_filter: Some(match_filter),
			output_schema: preserved_schema,
			strategy: JoinStrategy::HashPartitioned,
		}
	}

	fn recognized_summary_stage_with_filter<'a>(
		kind: JoinKind,
		replay_id: u32,
		existence: &'a ExecutionPlan,
		match_filter: Option<&'a BoundExprProgram>,
		preserved_schema: SchemaRef,
	) -> StreamingSemiAntiStage<'a> {
		StreamingSemiAntiStage {
			replay_id: JoinReplayId(replay_id),
			kind,
			preserved_keys: vec![ColumnOrdinal(0)],
			existence,
			existence_keys: vec![ColumnOrdinal(0)],
			match_filter,
			output_schema: preserved_schema,
			strategy: JoinStrategy::HashPartitioned,
		}
	}

	#[test]
	fn recognized_neq_summary_structure_and_growth_are_exact() {
		assert_eq!(std::mem::size_of::<NeqExistenceSummaryEntry>(), 16);
		let mut summary = NeqExistenceSummary::new();
		let cap = 1024 * 1024;
		for key in 0..1_025i64 {
			summary
				.insert(key, key.saturating_add(10), 0, cap)
				.expect("summary insert");
		}
		summary.insert(10, 10, 0, cap).expect("different value");
		summary
			.insert(10, 10, 0, cap)
			.expect("repeated different value");
		summary
			.insert(i64::MIN, i64::MAX, 0, cap)
			.expect("full-domain insert");

		assert_eq!(summary.entry_limit, 2_048);
		assert_eq!(summary.buckets.len(), 4_096);
		assert_eq!(summary.multi.len(), 32);
		assert_eq!(summary.accounted_bytes(), summary.computed_bytes());
		assert_eq!(summary.growth_count, 2);
		assert!(summary.matches(10, 20));
		assert!(summary.matches(10, 10));
		assert!(summary.matches(i64::MIN, 0));
		assert!(!summary.matches(9_999, 0));
	}

	#[test]
	fn recognized_neq_summary_growth_guard_counts_the_live_baseline() {
		let mut summary = NeqExistenceSummary::new();
		let initial_entries =
			summary_bytes::<NeqExistenceSummaryEntry>(NEQ_EXISTENCE_SUMMARY_INITIAL_ENTRY_CAPACITY);
		let baseline = 64u64 * 1024;
		let cap = baseline.saturating_add(initial_entries).saturating_sub(1);
		let error = summary
			.insert(1, 2, baseline, cap)
			.expect_err("combined baseline must arm the growth guard");
		assert!(matches!(
			error,
			ExecutionError::BudgetExhausted {
				dimension: BudgetDimension::Memory,
				producer: NEQ_EXISTENCE_SUMMARY_PRODUCER,
				..
			}
		));
		assert!(summary.entries.is_empty());
		assert!(summary.buckets.is_empty());
	}

	#[test]
	fn recognized_neq_summary_pair_uses_one_combined_guard() {
		let cap = 32 * 1024;
		let mut first = NeqExistenceSummary::new();
		first
			.insert(1, 10, 0, cap)
			.expect("one summary fits in isolation");
		let first_bytes = first.accounted_bytes();
		let mut isolated_second = NeqExistenceSummary::new();
		isolated_second
			.insert(2, 20, 0, cap)
			.expect("second summary also fits in isolation");
		let mut combined_second = NeqExistenceSummary::new();
		let error = combined_second
			.insert(2, 20, first_bytes, cap)
			.expect_err("the live first summary must arm the combined guard");
		assert!(matches!(
			error,
			ExecutionError::BudgetExhausted {
				producer: NEQ_EXISTENCE_SUMMARY_PRODUCER,
				..
			}
		));
	}

	#[test]
	fn recognized_neq_summary_uses_scan_ndv_as_a_checked_capacity_hint() {
		use crate::catalog::DistinctCountFact;

		let schema = Arc::new(Schema::new(vec![
			Field::new("orderkey", DataType::Int64, false),
			Field::new("suppkey", DataType::Int64, false),
		]));
		let mut existence = test_scan_plan_with_first_field_key(TableId(77), schema.clone(), 77);
		let ExecutionPlan::Scan { column_ndv, .. } = &mut existence else {
			panic!("scan fixture");
		};
		column_ndv.push((ColumnOrdinal(0), DistinctCountFact::Exact(1_500)));
		let residual = not_equal_residual_program(
			ColumnOrdinal(1),
			ColumnOrdinal(3),
			"preserved",
			"existence",
		);
		let stage = recognized_summary_stage(JoinKind::LeftSemi, 3, &existence, &residual, schema);
		let columns = recognized_neq_existence_summary_columns(&stage).expect("recognized shape");
		assert_eq!(columns.key_count_hint, Some(1_500));
		let mut summary = NeqExistenceSummary::new();
		summary
			.reserve_key_count(1_500, 0, 1024 * 1024)
			.expect("hinted reservation");
		let growth_count = summary.growth_count;
		for key in 0..1_500i64 {
			summary
				.insert(key, key, 0, 1024 * 1024)
				.expect("hinted insert");
		}
		assert_eq!(summary.entry_limit, 2_048);
		assert_eq!(summary.growth_count, growth_count);

		let schema = Arc::new(Schema::new(vec![
			Field::new("orderkey", DataType::Int64, false),
			Field::new("suppkey", DataType::Int64, false),
		]));
		let mut approximate = test_scan_plan_with_first_field_key(TableId(79), schema.clone(), 79);
		let ExecutionPlan::Scan { column_ndv, .. } = &mut approximate else {
			panic!("scan fixture");
		};
		column_ndv.push((
			ColumnOrdinal(0),
			DistinctCountFact::Approx {
				estimate: 1_500,
				rel_err_ppm: 10_000,
			},
		));
		let approximate_stage =
			recognized_summary_stage(JoinKind::LeftSemi, 3, &approximate, &residual, schema);
		assert_eq!(
			recognized_neq_existence_summary_columns(&approximate_stage)
				.expect("recognized approximate stage")
				.key_count_hint,
			None,
			"approximate NDV is evidence, not hard preallocation authority"
		);
	}

	#[test]
	fn recognized_neq_summary_pair_is_atomic_and_pre_io() {
		let preserved_schema = Arc::new(Schema::new(vec![
			Field::new("orderkey", DataType::Int64, false),
			Field::new("suppkey", DataType::Int64, false),
		]));
		let existence_schema = preserved_schema.clone();
		let semi_plan =
			test_scan_plan_with_first_field_key(TableId(71), existence_schema.clone(), 71);
		let anti_plan =
			test_scan_plan_with_first_field_key(TableId(72), existence_schema.clone(), 72);
		let semi_filter = not_equal_residual_program(
			ColumnOrdinal(1),
			ColumnOrdinal(3),
			"preserved",
			"existence",
		);
		let anti_filter = not_equal_residual_program(
			ColumnOrdinal(3),
			ColumnOrdinal(1),
			"existence",
			"preserved",
		);
		let rejected_filter = binary_residual_program(
			BoundBinaryOp::Eq,
			ColumnOrdinal(1),
			DataType::Int64,
			false,
			ColumnOrdinal(3),
			DataType::Int64,
			false,
		);
		let requested_tables = Arc::new(Mutex::new(Vec::new()));
		let executor = Executor::with_context(
			Arc::new(RecordingMultiBatchTableStorageScan {
				batches: HashMap::new(),
				requested_tables: requested_tables.clone(),
			}),
			ExecutionContext::local_test(),
		);
		let semi = recognized_summary_stage(
			JoinKind::LeftSemi,
			3,
			&semi_plan,
			&semi_filter,
			preserved_schema.clone(),
		);
		let anti = recognized_summary_stage(
			JoinKind::LeftAnti,
			4,
			&anti_plan,
			&anti_filter,
			preserved_schema.clone(),
		);
		let selected = executor
			.recognized_neq_summary_pair(&semi, &anti, None)
			.expect("both operand orders must be admitted");
		assert_eq!(selected.0.preserved_value, ColumnOrdinal(1));
		assert_eq!(selected.0.existence_value, ColumnOrdinal(1));
		assert_eq!(selected.1.preserved_value, ColumnOrdinal(1));
		assert_eq!(selected.1.existence_value, ColumnOrdinal(1));

		let rejected = recognized_summary_stage(
			JoinKind::LeftAnti,
			4,
			&anti_plan,
			&rejected_filter,
			preserved_schema,
		);
		assert!(
			executor
				.recognized_neq_summary_pair(&semi, &rejected, None)
				.is_none(),
			"one rejected stage must reject the pair"
		);
		assert!(
			requested_tables
				.lock()
				.expect("requested tables")
				.is_empty(),
			"pair selection must not open either scan"
		);
	}

	#[test]
	fn recognized_neq_summary_reject_matrix_is_structure_first() {
		let int64_schema = Arc::new(Schema::new(vec![
			Field::new("orderkey", DataType::Int64, false),
			Field::new("suppkey", DataType::Int64, false),
		]));
		let existence = test_scan_plan_with_first_field_key(TableId(73), int64_schema.clone(), 73);
		let same_side = binary_residual_program(
			BoundBinaryOp::Neq,
			ColumnOrdinal(0),
			DataType::Int64,
			false,
			ColumnOrdinal(1),
			DataType::Int64,
			false,
		);
		let out_of_range = binary_residual_program(
			BoundBinaryOp::Neq,
			ColumnOrdinal(1),
			DataType::Int64,
			false,
			ColumnOrdinal(99),
			DataType::Int64,
			false,
		);
		let mut cast_builder = BoundExprProgramBuilder::new();
		let preserved = cast_builder.append_column(ResolvedColumn {
			name: ColumnName("preserved".to_string()),
			ordinal: ColumnOrdinal(1),
			data_type: DataType::Int64,
			nullable: false,
			pbc_eligible: false,
		});
		let preserved = cast_builder.append_cast(preserved, DataType::Int64, DataType::Int64);
		let existence_value = cast_builder.append_column(ResolvedColumn {
			name: ColumnName("existence".to_string()),
			ordinal: ColumnOrdinal(3),
			data_type: DataType::Int64,
			nullable: false,
			pbc_eligible: false,
		});
		let cast_root = cast_builder.append_binary(
			BoundBinaryOp::Neq,
			preserved,
			existence_value,
			DataType::Boolean,
		);
		cast_builder.push_root(cast_root);
		let cast_child = cast_builder.build();
		let mut multi_root_builder = BoundExprProgramBuilder::new();
		let a = multi_root_builder.append_literal(ScalarValue::Boolean(Some(true)));
		let b = multi_root_builder.append_literal(ScalarValue::Boolean(Some(false)));
		multi_root_builder.push_root(a);
		multi_root_builder.push_root(b);
		let multi_root = multi_root_builder.build();

		for residual in [&same_side, &out_of_range, &cast_child, &multi_root] {
			let stage = recognized_summary_stage(
				JoinKind::LeftSemi,
				3,
				&existence,
				residual,
				int64_schema.clone(),
			);
			assert!(recognized_neq_existence_summary_columns(&stage).is_none());
		}

		let nullable_preserved_schema = Arc::new(Schema::new(vec![
			Field::new("orderkey", DataType::Int64, false),
			Field::new("suppkey", DataType::Int64, true),
		]));
		let nullable_preserved = binary_residual_program(
			BoundBinaryOp::Neq,
			ColumnOrdinal(1),
			DataType::Int64,
			true,
			ColumnOrdinal(3),
			DataType::Int64,
			false,
		);
		let stage = recognized_summary_stage(
			JoinKind::LeftSemi,
			3,
			&existence,
			&nullable_preserved,
			nullable_preserved_schema,
		);
		assert!(recognized_neq_existence_summary_columns(&stage).is_none());

		let nullable_existence_schema = Arc::new(Schema::new(vec![
			Field::new("orderkey", DataType::Int64, false),
			Field::new("suppkey", DataType::Int64, true),
		]));
		let nullable_existence_plan =
			test_scan_plan_with_first_field_key(TableId(74), nullable_existence_schema, 74);
		let nullable_existence = binary_residual_program(
			BoundBinaryOp::Neq,
			ColumnOrdinal(1),
			DataType::Int64,
			false,
			ColumnOrdinal(3),
			DataType::Int64,
			true,
		);
		let stage = recognized_summary_stage(
			JoinKind::LeftSemi,
			3,
			&nullable_existence_plan,
			&nullable_existence,
			int64_schema.clone(),
		);
		assert!(recognized_neq_existence_summary_columns(&stage).is_none());

		let int32_key_schema = Arc::new(Schema::new(vec![
			Field::new("orderkey", DataType::Int32, false),
			Field::new("suppkey", DataType::Int64, false),
		]));
		let int32_key_plan =
			test_scan_plan_with_first_field_key(TableId(75), int32_key_schema.clone(), 75);
		let valid_shape = not_equal_residual_program(
			ColumnOrdinal(1),
			ColumnOrdinal(3),
			"preserved",
			"existence",
		);
		let stage = recognized_summary_stage(
			JoinKind::LeftSemi,
			3,
			&int32_key_plan,
			&valid_shape,
			int32_key_schema,
		);
		assert!(recognized_neq_existence_summary_columns(&stage).is_none());

		let float_value_schema = Arc::new(Schema::new(vec![
			Field::new("orderkey", DataType::Int64, false),
			Field::new("suppkey", DataType::Float64, false),
		]));
		let float_value_plan =
			test_scan_plan_with_first_field_key(TableId(76), float_value_schema.clone(), 76);
		let float_value = binary_residual_program(
			BoundBinaryOp::Neq,
			ColumnOrdinal(1),
			DataType::Float64,
			false,
			ColumnOrdinal(3),
			DataType::Float64,
			false,
		);
		let stage = recognized_summary_stage(
			JoinKind::LeftSemi,
			3,
			&float_value_plan,
			&float_value,
			float_value_schema,
		);
		assert!(recognized_neq_existence_summary_columns(&stage).is_none());

		let membership_only = recognized_summary_stage_with_filter(
			JoinKind::LeftSemi,
			3,
			&existence,
			None,
			int64_schema,
		);
		assert!(recognized_neq_existence_summary_columns(&membership_only).is_none());
	}

	#[tokio::test]
	async fn recognized_neq_summary_matches_residual_twin_for_semi_and_anti() {
		let preserved_schema = Arc::new(Schema::new(vec![
			Field::new("orderkey", DataType::Int64, false),
			Field::new("suppkey", DataType::Int64, false),
			Field::new("tag", DataType::Int64, false),
		]));
		let existence_schema = Arc::new(Schema::new(vec![
			Field::new("orderkey", DataType::Int64, false),
			Field::new("suppkey", DataType::Int64, false),
		]));
		let preserved = RecordBatch::try_new(
			preserved_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1, 1, 2, 3, 4])),
				Arc::new(Int64Array::from(vec![10, 11, 20, 30, 41])),
				Arc::new(Int64Array::from(vec![100, 101, 200, 300, 400])),
			],
		)
		.expect("preserved batch");
		let existence = RecordBatch::try_new(
			existence_schema,
			vec![
				Arc::new(Int64Array::from(vec![1, 1, 2, 2, 4])),
				Arc::new(Int64Array::from(vec![10, 10, 20, 30, 40])),
			],
		)
		.expect("existence batch");
		let residual = not_equal_residual_program(
			ColumnOrdinal(1),
			ColumnOrdinal(4),
			"preserved_suppkey",
			"existence_suppkey",
		);
		let executor = Executor::new(Arc::new(RecordingMultiBatchTableStorageScan {
			batches: HashMap::new(),
			requested_tables: Arc::new(Mutex::new(Vec::new())),
		}));

		for kind in [JoinKind::LeftSemi, JoinKind::LeftAnti] {
			let key_arrays = key_arrays(&existence, &[ColumnOrdinal(0)]).expect("existence keys");
			let rows = build_in_memory_join_rows(
				JoinKeyPlan::Int64,
				&key_arrays,
				existence.num_rows(),
				u64::MAX,
				false,
			)
			.expect("build rows")
			.expect("in-memory rows");
			let mut twin = StreamingSemiAntiRuntime {
				replay_id: JoinReplayId::SYNTHETIC,
				kind,
				strategy: JoinStrategy::HashPartitioned,
				existence: StreamingSemiAntiExistence::InMemory {
					batch: existence.clone(),
					rows,
				},
				existence_row_count: existence.num_rows() as u64,
				key_plan: JoinKeyPlan::Int64,
				preserved_keys: vec![ColumnOrdinal(0)],
				match_filter: Some(&residual),
				output_schema: preserved_schema.clone(),
				probe_rows: 0,
				output_rows: 0,
			};
			let mut summary = NeqExistenceSummary::new();
			summary
				.write_batch(
					&existence,
					ColumnOrdinal(0),
					ColumnOrdinal(1),
					0,
					1024 * 1024,
				)
				.expect("summary build");
			let mut summarized = StreamingSemiAntiRuntime {
				replay_id: JoinReplayId::SYNTHETIC,
				kind,
				strategy: JoinStrategy::HashPartitioned,
				existence: StreamingSemiAntiExistence::RecognizedNeqSummary {
					summary,
					preserved_value: ColumnOrdinal(1),
				},
				existence_row_count: existence.num_rows() as u64,
				key_plan: JoinKeyPlan::Int64,
				preserved_keys: vec![ColumnOrdinal(0)],
				match_filter: Some(&residual),
				output_schema: preserved_schema.clone(),
				probe_rows: 0,
				output_rows: 0,
			};

			let expected = executor
				.probe_streaming_semi_anti(&mut twin, &preserved)
				.await
				.expect("residual twin probe");
			let actual = executor
				.probe_streaming_semi_anti(&mut summarized, &preserved)
				.await
				.expect("summary probe");
			assert_record_batches_equal(&actual, &expected);
			assert!(matches!(
				summarized.existence,
				StreamingSemiAntiExistence::RecognizedNeqSummary { .. }
			));
		}
	}

	#[tokio::test]
	async fn recognized_neq_summary_prepare_is_page_free_and_profiled() {
		let preserved_schema = Arc::new(Schema::new(vec![
			Field::new("orderkey", DataType::Int64, false),
			Field::new("suppkey", DataType::Int64, false),
		]));
		let existence_batch = RecordBatch::try_new(
			preserved_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1, 1, 2])),
				Arc::new(Int64Array::from(vec![10, 20, 30])),
			],
		)
		.expect("existence batch");
		let existence_plan =
			test_scan_plan_with_first_field_key(TableId(78), preserved_schema.clone(), 78);
		let residual = not_equal_residual_program(
			ColumnOrdinal(1),
			ColumnOrdinal(3),
			"preserved",
			"existence",
		);
		let stage = recognized_summary_stage(
			JoinKind::LeftSemi,
			3,
			&existence_plan,
			&residual,
			preserved_schema,
		);
		let columns = recognized_neq_existence_summary_columns(&stage).expect("recognized stage");
		let telemetry = Arc::new(Mutex::new(TaskTelemetry::default()));
		let requested_tables = Arc::new(Mutex::new(Vec::new()));
		let executor = Executor::with_context(
			Arc::new(RecordingMultiBatchTableStorageScan {
				batches: HashMap::from([(TableId(78), vec![existence_batch])]),
				requested_tables: requested_tables.clone(),
			}),
			ExecutionContext::local_test()
				.with_task_budget(high_test_budget())
				.with_task_telemetry(telemetry.clone()),
		);
		let runtime = executor
			.prepare_recognized_neq_semi_anti_runtime(stage, columns, None, 0)
			.await
			.expect("summary runtime");
		assert!(matches!(
			runtime.existence,
			StreamingSemiAntiExistence::RecognizedNeqSummary { .. }
		));
		assert_eq!(
			requested_tables
				.lock()
				.expect("requested tables")
				.as_slice(),
			&[TableId(78)]
		);
		let telemetry = telemetry.lock().expect("telemetry");
		let [profile] = telemetry.existence_summary_profiles.as_slice() else {
			panic!("one summary profile");
		};
		assert_eq!(profile.replay_id, JoinReplayId(3));
		assert_eq!(profile.build_rows.get(), 3);
		assert_eq!(profile.unique_keys.get(), 2);
		assert_eq!(profile.multi_keys.get(), 1);
		assert_eq!(profile.page_reads, 0);
		assert_eq!(profile.accounted_bytes, 24_704);
	}

	async fn projected_existence_spill(
		existence: RecordBatch,
		preserved_schema: SchemaRef,
		existence_keys: &[ColumnOrdinal],
		retained_columns: Vec<ColumnOrdinal>,
	) -> SpilledSemiAntiExistenceBuild {
		let context = ExecutionContext::local_test().with_task_budget(high_test_budget());
		let projected_schema = projected_schema_for_columns(&existence.schema(), &retained_columns)
			.expect("projected existence schema");
		let projected_keys =
			remap_column_ordinals(&retained_columns, existence_keys).expect("projected keys");
		let key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&projected_schema,
			&projected_keys,
			&preserved_schema,
			existence_keys,
			false,
		)
		.expect("projected key plan");
		let mut spilled = SpilledSemiAntiExistenceBuild::create(
			context.task_budget,
			context.join_spill_file_ops.clone(),
			context.join_spill_dir.clone(),
		)
		.await
		.expect("projected existence spill");
		spilled
			.set_projection(existence.schema(), retained_columns)
			.expect("set projection");
		spilled
			.write_projected_batch_chunks(
				&existence,
				key_plan,
				&projected_keys,
				context.task_budget,
			)
			.await
			.expect("write projected existence");
		spilled
			.finish(context.task_budget)
			.await
			.expect("finish spill");
		spilled
	}

	async fn projected_membership_existence_spill(
		existence: RecordBatch,
		preserved_schema: SchemaRef,
		existence_keys: &[ColumnOrdinal],
		retained_columns: Vec<ColumnOrdinal>,
	) -> SpilledSemiAntiExistenceBuild {
		let context = ExecutionContext::local_test().with_task_budget(high_test_budget());
		let projected_schema = projected_schema_for_columns(&existence.schema(), &retained_columns)
			.expect("projected existence schema");
		let projected_keys =
			remap_column_ordinals(&retained_columns, existence_keys).expect("projected keys");
		let key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&projected_schema,
			&projected_keys,
			&preserved_schema,
			existence_keys,
			false,
		)
		.expect("projected membership key plan");
		let mut spilled = SpilledSemiAntiExistenceBuild::create(
			context.task_budget,
			context.join_spill_file_ops.clone(),
			context.join_spill_dir.clone(),
		)
		.await
		.expect("projected membership existence spill");
		spilled
			.set_projection(existence.schema(), retained_columns)
			.expect("set projection");
		spilled
			.write_projected_membership_key_chunks(
				&existence,
				key_plan,
				&projected_keys,
				context.task_budget,
			)
			.expect("write projected membership existence");
		spilled
			.finish(context.task_budget)
			.await
			.expect("finish spill");
		spilled
	}

	async fn unprojected_existence_spill(
		existence: RecordBatch,
		preserved_schema: SchemaRef,
		existence_keys: &[ColumnOrdinal],
	) -> SpilledSemiAntiExistenceBuild {
		let context = ExecutionContext::local_test().with_task_budget(high_test_budget());
		let key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&existence.schema(),
			existence_keys,
			&preserved_schema,
			existence_keys,
			false,
		)
		.expect("unprojected key plan");
		let mut spilled = SpilledSemiAntiExistenceBuild::create(
			context.task_budget,
			context.join_spill_file_ops.clone(),
			context.join_spill_dir.clone(),
		)
		.await
		.expect("unprojected existence spill");
		spilled
			.write_batch_chunks(&existence, key_plan, existence_keys, context.task_budget)
			.await
			.expect("write unprojected existence");
		spilled
			.finish(context.task_budget)
			.await
			.expect("finish spill");
		spilled
	}

	fn reoriented_scan_build_stage(
		build: &ExecutionPlan,
		probe_schema: SchemaRef,
		output_schema: SchemaRef,
	) -> ReorientedInnerJoinStage<'_> {
		ReorientedInnerJoinStage {
			replay_id: JoinReplayId(1),
			probe_schema,
			probe_keys: vec![ColumnOrdinal(0)],
			build,
			build_keys: vec![ColumnOrdinal(0)],
			retained_build_columns: None,
			output_schema,
			strategy: JoinStrategy::HashPartitioned,
			post_filter: None,
			probe_columns_first: true,
		}
	}

	fn assert_record_batches_equal(left: &[RecordBatch], right: &[RecordBatch]) {
		let left_schema = left.first().expect("left batches").schema();
		let right_schema = right.first().expect("right batches").schema();
		assert_eq!(left_schema, right_schema);
		let left_batch =
			arrow::compute::concat_batches(&left_schema, left.iter()).expect("left concat");
		let right_batch =
			arrow::compute::concat_batches(&right_schema, right.iter()).expect("right concat");
		assert_eq!(left_batch.num_rows(), right_batch.num_rows());
		assert_eq!(left_batch.num_columns(), right_batch.num_columns());
		for column in 0..left_batch.num_columns() {
			assert_eq!(
				format!("{:?}", left_batch.column(column)),
				format!("{:?}", right_batch.column(column)),
				"column {column} differs"
			);
		}
	}

	fn int64_column_values(batch: &RecordBatch, column: usize) -> Vec<i64> {
		batch
			.column(column)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("int64 column")
			.iter()
			.map(|value| value.expect("non-null int64"))
			.collect()
	}

	fn chain_stage_profile_test_path(label: &str) -> PathBuf {
		static NEXT: AtomicU64 = AtomicU64::new(0);
		std::env::temp_dir().join(format!(
			"toron_chain_stage_profile_{}_{}_{}.json",
			std::process::id(),
			NEXT.fetch_add(1, Ordering::SeqCst),
			label,
		))
	}

	#[test]
	fn chain_stage_profile_snapshot_has_exact_exclusive_accounting() {
		let path = chain_stage_profile_test_path("accounting");
		let temporary = path.with_extension("tmp");
		let _ = std::fs::remove_file(&path);
		let _ = std::fs::remove_file(&temporary);
		let mut profile = ChainStageProfiler::at_path(path.clone());
		profile
			.add_micros(ChainStage::BaseScan, 5)
			.expect("base timing");
		profile
			.add_micros(ChainStage::InnerReplay0, 7)
			.expect("replay timing");
		profile
			.add_flow(ChainStage::BaseScan, 1, 1, 10, 8)
			.expect("base flow");
		profile.complete_base_batch(10, 8).expect("completed batch");

		let snapshot = profile.snapshot_value(20).expect("snapshot");
		assert_eq!(snapshot["sequence"], 0);
		assert_eq!(snapshot["completed_base_batches"], 1);
		assert_eq!(snapshot["raw_base_rows"], 10);
		assert_eq!(snapshot["post_filter_base_rows"], 8);
		assert_eq!(snapshot["total_drive_micros"], 20);
		assert_eq!(snapshot["unattributed_residual_micros"], 8);
		assert_eq!(
			snapshot["stages"].as_array().expect("stages").len(),
			CHAIN_STAGE_COUNT
		);
		let sum = snapshot["stages"]
			.as_array()
			.expect("stages")
			.iter()
			.map(|stage| stage["micros"].as_u64().expect("stage micros"))
			.sum::<u64>();
		assert_eq!(
			sum + snapshot["unattributed_residual_micros"]
				.as_u64()
				.expect("residual"),
			20
		);

		// ADR 507: the inverted-band rejection now flows through the attribution contract, so the
		// message is the contract's typed one. The invariant it guards is unchanged — stages that
		// exceed the drive total are refused loudly, never clamped.
		let error = profile
			.snapshot_value(11)
			.expect_err("stage timers greater than total must fail");
		assert!(
			error.to_string().contains("time reconciliation failed")
				&& error.to_string().contains("time band inverted"),
			"unexpected error: {error}"
		);
		let _ = std::fs::remove_file(path);
		let _ = std::fs::remove_file(temporary);
	}

	#[test]
	fn deferred_replay_profile_keeps_logical_rows_and_durable_page_progress() {
		let path = chain_stage_profile_test_path("deferred_progress");
		let temporary = path.with_extension("tmp");
		let _ = std::fs::remove_file(&path);
		let _ = std::fs::remove_file(&temporary);
		let mut profile = ChainStageProfiler::at_path(path.clone());
		profile
			.begin_deferred_replay(4, 1234)
			.expect("begin deferred replay");
		for _ in 0..CHAIN_DEFERRED_BASE_FLUSH_INTERVAL - 1 {
			profile.complete_base_batch(1, 1).expect("base batch");
		}
		assert!(!profile.base_batch_flush_due());
		profile.complete_base_batch(1, 1).expect("base batch");
		assert!(profile.base_batch_flush_due());
		profile
			.add_flow(ChainStage::InnerReplay1, 2, 0, 11, 0)
			.expect("buffer logical input");
		profile
			.add_deferred_probe_rows(11)
			.expect("buffer progress");
		profile.set_deferred_probe_spill_bytes(5678);
		profile
			.begin_deferred_partition(2)
			.expect("begin partition");
		profile
			.record_deferred_index(PinnedSpilledInnerBuildStats {
				page_reads: 3,
				decoded_bytes: 400,
				index_bytes: 500,
				rows: 7,
			})
			.expect("record index");
		for _ in 0..CHAIN_DEFERRED_PAGE_FLUSH_INTERVAL {
			profile
				.complete_deferred_probe_page()
				.expect("record probe page");
		}
		assert!(profile.deferred_probe_page_flush_due());
		profile
			.add_flow(ChainStage::InnerReplay1, 0, 3, 0, 7)
			.expect("record logical output");
		profile.flush().expect("flush partial drain");
		let snapshot: serde_json::Value =
			serde_json::from_slice(&std::fs::read(&path).expect("read deferred snapshot"))
				.expect("parse deferred snapshot");
		assert_eq!(snapshot["drive_completed"], false);
		assert_eq!(snapshot["deferred_replay_1"]["partition_count"], 4);
		assert_eq!(snapshot["deferred_replay_1"]["active_partition"], 2);
		assert_eq!(snapshot["deferred_replay_1"]["index_builds"], 1);
		assert_eq!(snapshot["deferred_replay_1"]["build_page_reads"], 3);
		assert_eq!(
			snapshot["deferred_replay_1"]["probe_page_reads"],
			CHAIN_DEFERRED_PAGE_FLUSH_INTERVAL
		);
		assert_eq!(snapshot["deferred_replay_1"]["max_index_bytes"], 500);
		let replay = snapshot["stages"]
			.as_array()
			.expect("stages")
			.iter()
			.find(|stage| stage["stage"] == "inner_replay_1")
			.expect("replay stage");
		assert_eq!(replay["input_rows"], 11);
		assert_eq!(replay["output_rows"], 7);
		let _ = std::fs::remove_file(path);
		let _ = std::fs::remove_file(temporary);
	}

	#[test]
	fn partition_replay_owner_guard_names_every_live_component() {
		let memory_cap = 1024;
		let retained_cap = join_spill_retained_footprint_memory_cap(memory_cap);
		guard_partition_replay_owner_resident(
			1,
			2,
			3,
			4,
			5,
			6,
			7,
			retained_cap.saturating_sub(28),
			memory_cap,
		)
		.expect("exact retained cap fits");
		let error = guard_partition_replay_owner_resident(
			2,
			2,
			3,
			4,
			5,
			6,
			7,
			retained_cap.saturating_sub(28),
			memory_cap,
		)
		.expect_err("later owner baseline must arm the guard");
		assert!(matches!(
			error,
			ExecutionError::BudgetExhausted {
				dimension: BudgetDimension::Memory,
				producer: PARTITION_REPLAY_PRODUCER,
				..
			}
		));
		let detail = error.to_string();
		for component in [
			"runtime_baseline_bytes",
			"build_reservoir_bytes",
			"probe_reservoir_bytes",
			"active_build_bytes",
			"active_probe_bytes",
			"probe_page_bytes",
			"output_bytes",
			"fold_bytes",
		] {
			assert!(detail.contains(component), "missing {component}: {detail}");
		}
	}

	#[test]
	fn partition_replay_drain_is_page_streamed_by_construction() {
		let source = include_str!("streaming.rs");
		let start = source
			.find("async fn drain_partition_at_a_time_replay(")
			.expect("partition replay drain");
		let tail = &source[start..];
		let end = tail
			.find("async fn try_streaming_inner_prefix_semi_anti_scan_probe_base(")
			.expect("next owner function");
		let drain = &tail[..end];
		assert!(drain.contains("for page_index in 0..probe.page_count()"));
		assert!(drain.contains("probe.read_batch_page(page_index).await?"));
		assert!(!drain.contains("read_all_batch_pages"));
		assert!(!drain.contains("concat_or_empty"));
	}

	#[test]
	fn chain_stage_profile_atomic_snapshot_preserves_last_complete_value() {
		let path = chain_stage_profile_test_path("atomic");
		let temporary = path.with_extension("tmp");
		let _ = std::fs::remove_file(&path);
		let _ = std::fs::remove_file(&temporary);
		let mut profile = ChainStageProfiler::at_path(path.clone());
		profile.flush().expect("start snapshot");
		let start: serde_json::Value =
			serde_json::from_slice(&std::fs::read(&path).expect("read start snapshot"))
				.expect("parse start snapshot");
		assert_eq!(start["sequence"], 0);
		assert_eq!(start["completed_base_batches"], 0);
		assert_eq!(start["drive_completed"], false);

		profile
			.add_flow(ChainStage::BaseScan, 1, 1, 4, 3)
			.expect("base flow");
		profile.complete_base_batch(4, 3).expect("completed batch");
		profile.flush().expect("batch snapshot");
		let completed_bytes = std::fs::read(&path).expect("read batch snapshot");
		let completed: serde_json::Value =
			serde_json::from_slice(&completed_bytes).expect("parse batch snapshot");
		assert_eq!(completed["sequence"], 1);
		assert_eq!(completed["completed_base_batches"], 1);

		std::fs::write(&temporary, b"{interrupted").expect("write interrupted temporary");
		assert_eq!(
			std::fs::read(&path).expect("read preserved snapshot"),
			completed_bytes,
			"an interrupted temporary write must not corrupt the last renamed snapshot"
		);
		let _ = std::fs::remove_file(path);
		let _ = std::fs::remove_file(temporary);
	}

	#[test]
	fn chain_stage_profile_snapshot_failures_are_labeled_and_loud() {
		let parent = chain_stage_profile_test_path("missing_parent").with_extension("dir");
		let _ = std::fs::remove_dir_all(&parent);
		let mut profile = ChainStageProfiler::at_path(parent.join("profile.json"));
		let error = profile.flush().expect_err("missing parent must fail");
		assert!(
			error
				.to_string()
				.contains("chain stage profile snapshot write failed")
		);
		assert!(!parent.exists());

		std::fs::create_dir(&parent).expect("create profile parent");
		let target = parent.join("profile.json");
		std::fs::create_dir(&target).expect("create directory at target path");
		let mut profile = ChainStageProfiler::at_path(target);
		let error = profile
			.flush()
			.expect_err("directory target must fail rename");
		assert!(
			error
				.to_string()
				.contains("chain stage profile snapshot rename failed")
		);
		let _ = std::fs::remove_dir_all(parent);
	}
}
