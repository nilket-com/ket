use super::{SpilledInnerJoinBuild, SpilledInnerJoinResidencySnapshot, record_batch_memory_bytes};
use crate::error::ExecutionError;
use arrow::record_batch::RecordBatch;
use serde::Serialize;
use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Instant;

pub(in crate::exec) const GROUPED_RESERVOIR_RESIDENCY_PROFILE_ENV: &str =
	"TORON_GROUPED_RESERVOIR_RESIDENCY_PROFILE_PATH";
pub(in crate::exec) const GROUPED_RESERVOIR_PROGRESS_BATCHES: u64 = 64;
pub(in crate::exec) const GROUPED_RESERVOIR_INTENT_BYTES: u64 = 16 * 1024 * 1024;
const GROUPED_RESERVOIR_PROFILE_SCHEMA_VERSION: u32 = 1;

fn profile_error(message: impl Into<String>) -> ExecutionError {
	ExecutionError::Kernel {
		message: format!("grouped reservoir residency profile: {}", message.into()),
	}
}

fn checked_sum(label: &'static str, values: impl IntoIterator<Item = u64>) -> CheckedBytes {
	match values
		.into_iter()
		.try_fold(0u64, |total, value| total.checked_add(value))
	{
		Some(value) => CheckedBytes {
			value: Some(value),
			error: None,
		},
		None => CheckedBytes {
			value: None,
			error: Some(format!("{label} overflow")),
		},
	}
}

fn checked_capacity_bytes<T>(capacity: usize, label: &'static str) -> CheckedBytes {
	let value = capacity
		.checked_mul(std::mem::size_of::<T>())
		.and_then(|bytes| u64::try_from(bytes).ok());
	CheckedBytes {
		value,
		error: value.is_none().then(|| format!("{label} overflow")),
	}
}

#[derive(Clone, Debug, Serialize)]
pub(in crate::exec) struct CheckedBytes {
	value: Option<u64>,
	error: Option<String>,
}

impl CheckedBytes {
	fn required(&self, label: &'static str) -> Result<u64, ExecutionError> {
		self.value.ok_or_else(|| {
			profile_error(
				self.error
					.clone()
					.unwrap_or_else(|| format!("{label} unavailable")),
			)
		})
	}
}

#[derive(Clone, Debug, Serialize)]
struct BaseOwnership {
	batch_count: usize,
	row_count: CheckedBytes,
	batch_buffer_accounted_bytes: CheckedBytes,
	collection_capacity_bytes: CheckedBytes,
	current_resident_bytes: CheckedBytes,
	overlap_label: &'static str,
}

impl BaseOwnership {
	fn from_batches(batches: &[RecordBatch], capacity: usize) -> Self {
		let row_count = checked_sum(
			"base row count",
			batches
				.iter()
				.filter_map(|batch| u64::try_from(batch.num_rows()).ok()),
		);
		let row_conversion_failed = batches
			.iter()
			.any(|batch| u64::try_from(batch.num_rows()).is_err());
		let row_count = if row_conversion_failed {
			CheckedBytes {
				value: None,
				error: Some("base row count conversion overflow".to_string()),
			}
		} else {
			row_count
		};
		let batch_buffer_accounted_bytes = checked_sum(
			"base batch buffer accounting",
			batches.iter().map(record_batch_memory_bytes),
		);
		let collection_capacity_bytes =
			checked_capacity_bytes::<RecordBatch>(capacity, "base collection capacity");
		let current_resident_bytes = match (
			batch_buffer_accounted_bytes.value,
			collection_capacity_bytes.value,
		) {
			(Some(buffers), Some(collection)) => {
				checked_sum("base current-resident accounting", [buffers, collection])
			}
			_ => CheckedBytes {
				value: None,
				error: Some("base component unavailable".to_string()),
			},
		};
		Self {
			batch_count: batches.len(),
			row_count,
			batch_buffer_accounted_bytes,
			collection_capacity_bytes,
			current_resident_bytes,
			overlap_label: "arrow_buffer_identity_not_deduplicated",
		}
	}

	#[cfg(test)]
	fn empty() -> Self {
		Self {
			batch_count: 0,
			row_count: checked_sum("base row count", []),
			batch_buffer_accounted_bytes: checked_sum("base buffers", []),
			collection_capacity_bytes: checked_sum("base collection", []),
			current_resident_bytes: checked_sum("base resident", []),
			overlap_label: "arrow_buffer_identity_not_deduplicated",
		}
	}
}

#[derive(Clone, Debug, Serialize)]
struct WriterComponentSummary {
	aggregate_bytes: CheckedBytes,
	maximum_writer_bytes: Option<u64>,
	unavailable_writers: usize,
}

fn summarize_writer_component(
	label: &'static str,
	values: impl IntoIterator<Item = Option<u64>>,
) -> WriterComponentSummary {
	let values = values.into_iter().collect::<Vec<_>>();
	let unavailable_writers = values.iter().filter(|value| value.is_none()).count();
	let available = values.iter().filter_map(|value| *value).collect::<Vec<_>>();
	let aggregate_bytes = if unavailable_writers == 0 {
		checked_sum(label, available.iter().copied())
	} else {
		CheckedBytes {
			value: None,
			error: Some(format!(
				"{label} unavailable for {unavailable_writers} writer(s)"
			)),
		}
	};
	WriterComponentSummary {
		aggregate_bytes,
		maximum_writer_bytes: available.into_iter().max(),
		unavailable_writers,
	}
}

#[derive(Clone, Debug, Serialize)]
struct WriterAggregate {
	writer_count: usize,
	writer_collection_capacity_bytes: CheckedBytes,
	per_writer: Vec<SpilledInnerJoinResidencySnapshot>,
	component_summaries: BTreeMap<&'static str, WriterComponentSummary>,
	aggregate_current_resident_bytes: CheckedBytes,
	maximum_writer_current_resident_bytes: Option<u64>,
	aggregate_guard_accounted_bytes: CheckedBytes,
	maximum_writer_guard_accounted_bytes: Option<u64>,
	aggregate_input_rows: CheckedBytes,
	spill_disk_bytes: CheckedBytes,
	open_file_handles: CheckedBytes,
	overlap_label: &'static str,
}

impl WriterAggregate {
	fn from_builds(builds: &[Option<SpilledInnerJoinBuild>], collection_capacity: usize) -> Self {
		let snapshots = builds
			.iter()
			.flatten()
			.map(SpilledInnerJoinBuild::residency_snapshot)
			.collect::<Vec<_>>();
		Self::from_snapshots(
			snapshots,
			checked_capacity_bytes::<Option<SpilledInnerJoinBuild>>(
				collection_capacity,
				"writer collection capacity",
			),
		)
	}

	fn from_snapshots(
		per_writer: Vec<SpilledInnerJoinResidencySnapshot>,
		writer_collection_capacity_bytes: CheckedBytes,
	) -> Self {
		let current_values = per_writer
			.iter()
			.map(|writer| writer.current_resident_bytes)
			.collect::<Option<Vec<_>>>();
		let guard_values = per_writer
			.iter()
			.map(|writer| writer.guard_accounted_bytes)
			.collect::<Option<Vec<_>>>();
		let aggregate_current_resident_bytes = match (
			current_values.as_ref(),
			writer_collection_capacity_bytes.value,
		) {
			(Some(values), Some(collection)) => checked_sum(
				"writer aggregate current-resident accounting",
				values.iter().copied().chain([collection]),
			),
			_ => CheckedBytes {
				value: None,
				error: Some("writer current-resident component unavailable".to_string()),
			},
		};
		let aggregate_guard_accounted_bytes = match guard_values.as_ref() {
			Some(values) => checked_sum(
				"writer aggregate guard-accounted accounting",
				values.iter().copied(),
			),
			None => CheckedBytes {
				value: None,
				error: Some("writer guard-accounted component unavailable".to_string()),
			},
		};
		let maximum_writer_current_resident_bytes =
			current_values.and_then(|values| values.into_iter().max());
		let maximum_writer_guard_accounted_bytes =
			guard_values.and_then(|values| values.into_iter().max());
		let aggregate_input_rows = checked_sum(
			"writer aggregate input rows",
			per_writer.iter().map(|writer| writer.input_rows),
		);
		let spill_disk_bytes = checked_sum(
			"writer aggregate spill disk bytes",
			per_writer.iter().map(|writer| writer.spill_disk_bytes),
		);
		let open_file_handles = checked_sum(
			"writer aggregate file handles",
			per_writer.iter().map(|writer| writer.open_file_handles),
		);
		let mut component_summaries = BTreeMap::new();
		macro_rules! component {
			($name:literal, $field:ident) => {
				component_summaries.insert(
					$name,
					summarize_writer_component(
						$name,
						per_writer.iter().map(|writer| Some(writer.$field)),
					),
				);
			};
		}
		macro_rules! optional_component {
			($name:literal, $field:ident) => {
				component_summaries.insert(
					$name,
					summarize_writer_component(
						$name,
						per_writer.iter().map(|writer| writer.$field),
					),
				);
			};
		}
		component!(
			"resident_baseline_guard_bytes",
			resident_baseline_guard_bytes
		);
		component!("resident_index_bytes", resident_index_bytes);
		component!("writer_scratch_peak_bytes", writer_scratch_peak_bytes);
		component!("page_reserve_bytes", page_reserve_bytes);
		component!("cache_payload_bytes", cache_payload_bytes);
		component!("pinned_decoded_bytes", pinned_decoded_bytes);
		optional_component!("page_metadata_bytes", page_metadata_bytes);
		optional_component!("cache_metadata_bytes", cache_metadata_bytes);
		optional_component!("pinned_handle_capacity_bytes", pinned_handle_capacity_bytes);
		optional_component!("write_scratch_capacity_bytes", write_scratch_capacity_bytes);
		optional_component!("retained_legacy_index_bytes", retained_legacy_index_bytes);
		optional_component!("path_capacity_bytes", path_capacity_bytes);
		Self {
			writer_count: per_writer.len(),
			writer_collection_capacity_bytes,
			per_writer,
			component_summaries,
			aggregate_current_resident_bytes,
			maximum_writer_current_resident_bytes,
			aggregate_guard_accounted_bytes,
			maximum_writer_guard_accounted_bytes,
			aggregate_input_rows,
			spill_disk_bytes,
			open_file_handles,
			overlap_label: "guard_accounting_is_reported_but_not_added_to_current_ownership",
		}
	}
}

#[derive(Clone, Debug, Serialize)]
struct Measurement {
	value: Option<u64>,
	error: Option<String>,
}

impl Measurement {
	fn value(value: u64) -> Self {
		Self {
			value: Some(value),
			error: None,
		}
	}

	fn unavailable(error: impl Into<String>) -> Self {
		Self {
			value: None,
			error: Some(error.into()),
		}
	}
}

#[derive(Clone, Debug, Serialize)]
struct CgroupMemoryStat {
	anon: Measurement,
	file: Measurement,
	file_dirty: Measurement,
	file_writeback: Measurement,
	shmem: Measurement,
	kernel: Measurement,
	slab: Measurement,
	unknown: BTreeMap<String, u64>,
	invalid_lines: Vec<String>,
}

impl CgroupMemoryStat {
	fn unavailable(error: impl Into<String>) -> Self {
		let error = error.into();
		Self {
			anon: Measurement::unavailable(error.clone()),
			file: Measurement::unavailable(error.clone()),
			file_dirty: Measurement::unavailable(error.clone()),
			file_writeback: Measurement::unavailable(error.clone()),
			shmem: Measurement::unavailable(error.clone()),
			kernel: Measurement::unavailable(error.clone()),
			slab: Measurement::unavailable(error),
			unknown: BTreeMap::new(),
			invalid_lines: Vec::new(),
		}
	}
}

fn parse_memory_stat(contents: &str) -> CgroupMemoryStat {
	let mut known = BTreeMap::new();
	let mut unknown = BTreeMap::new();
	let mut invalid_lines = Vec::new();
	for line in contents.lines() {
		let mut fields = line.split_whitespace();
		let Some(key) = fields.next() else {
			continue;
		};
		let Some(raw_value) = fields.next() else {
			invalid_lines.push(line.to_string());
			continue;
		};
		if fields.next().is_some() {
			invalid_lines.push(line.to_string());
			continue;
		}
		let Ok(value) = raw_value.parse::<u64>() else {
			invalid_lines.push(line.to_string());
			continue;
		};
		if matches!(
			key,
			"anon" | "file" | "file_dirty" | "file_writeback" | "shmem" | "kernel" | "slab"
		) {
			known.insert(key.to_string(), value);
		} else {
			unknown.insert(key.to_string(), value);
		}
	}
	let measurement = |key: &'static str| {
		known.get(key).copied().map_or_else(
			|| Measurement::unavailable(format!("missing memory.stat key {key}")),
			Measurement::value,
		)
	};
	CgroupMemoryStat {
		anon: measurement("anon"),
		file: measurement("file"),
		file_dirty: measurement("file_dirty"),
		file_writeback: measurement("file_writeback"),
		shmem: measurement("shmem"),
		kernel: measurement("kernel"),
		slab: measurement("slab"),
		unknown,
		invalid_lines,
	}
}

fn parse_process_rss(contents: &str) -> Measurement {
	let Some(line) = contents.lines().find(|line| line.starts_with("VmRSS:")) else {
		return Measurement::unavailable("missing VmRSS in /proc/self/status");
	};
	let mut fields = line.split_whitespace();
	let _label = fields.next();
	let Some(raw_kib) = fields.next() else {
		return Measurement::unavailable("missing VmRSS value");
	};
	let Ok(kib) = raw_kib.parse::<u64>() else {
		return Measurement::unavailable("invalid VmRSS value");
	};
	if fields.next() != Some("kB") || fields.next().is_some() {
		return Measurement::unavailable("unexpected VmRSS unit");
	}
	match kib.checked_mul(1024) {
		Some(bytes) => Measurement::value(bytes),
		None => Measurement::unavailable("VmRSS byte conversion overflow"),
	}
}

#[derive(Clone, Debug, Serialize)]
struct OsLedger {
	process_rss_bytes: Measurement,
	cgroup_v2_path: Option<String>,
	cgroup_path_error: Option<String>,
	memory_current_bytes: Measurement,
	memory_stat: CgroupMemoryStat,
	non_additive_label: &'static str,
}

trait OsSampler: Send {
	fn sample(&mut self) -> OsLedger;
}

struct ProcCgroupSampler {
	cgroup_dir: Result<PathBuf, String>,
}

impl ProcCgroupSampler {
	fn discover() -> Self {
		let cgroup_dir = std::fs::read_to_string("/proc/self/cgroup")
			.map_err(|error| format!("read /proc/self/cgroup failed: {error}"))
			.and_then(|contents| {
				contents
					.lines()
					.find_map(|line| line.strip_prefix("0::"))
					.map(|relative| {
						Path::new("/sys/fs/cgroup").join(relative.trim_start_matches('/'))
					})
					.ok_or_else(|| "cgroup-v2 entry missing from /proc/self/cgroup".to_string())
			});
		Self { cgroup_dir }
	}
}

impl OsSampler for ProcCgroupSampler {
	fn sample(&mut self) -> OsLedger {
		let process_rss_bytes = std::fs::read_to_string("/proc/self/status").map_or_else(
			|error| Measurement::unavailable(format!("read /proc/self/status failed: {error}")),
			|contents| parse_process_rss(&contents),
		);
		match &self.cgroup_dir {
			Ok(directory) => {
				let memory_current_bytes =
					std::fs::read_to_string(directory.join("memory.current")).map_or_else(
						|error| {
							Measurement::unavailable(format!("read memory.current failed: {error}"))
						},
						|contents| match contents.trim().parse::<u64>() {
							Ok(value) => Measurement::value(value),
							Err(error) => Measurement::unavailable(format!(
								"parse memory.current failed: {error}"
							)),
						},
					);
				let memory_stat = std::fs::read_to_string(directory.join("memory.stat"))
					.map_or_else(
						|error| {
							CgroupMemoryStat::unavailable(format!(
								"read memory.stat failed: {error}"
							))
						},
						|contents| parse_memory_stat(&contents),
					);
				OsLedger {
					process_rss_bytes,
					cgroup_v2_path: Some(directory.display().to_string()),
					cgroup_path_error: None,
					memory_current_bytes,
					memory_stat,
					non_additive_label: "os_cgroup_values_are_not_added_to_engine_ownership",
				}
			}
			Err(error) => OsLedger {
				process_rss_bytes,
				cgroup_v2_path: None,
				cgroup_path_error: Some(error.clone()),
				memory_current_bytes: Measurement::unavailable(error.clone()),
				memory_stat: CgroupMemoryStat::unavailable(error.clone()),
				non_additive_label: "os_cgroup_values_are_not_added_to_engine_ownership",
			},
		}
	}
}

trait SnapshotSink: Send {
	fn write_snapshot(&mut self, contents: &[u8]) -> io::Result<()>;
	fn resident_bytes(&self) -> Option<u64>;
}

struct AtomicFileSnapshotSink {
	path: PathBuf,
	temporary: PathBuf,
}

impl AtomicFileSnapshotSink {
	fn new(path: PathBuf) -> Self {
		let mut temporary_name = path
			.file_name()
			.map(|name| name.to_os_string())
			.unwrap_or_else(|| "residency-profile".into());
		temporary_name.push(".tmp");
		let temporary = path.with_file_name(temporary_name);
		Self { path, temporary }
	}
}

impl SnapshotSink for AtomicFileSnapshotSink {
	fn write_snapshot(&mut self, contents: &[u8]) -> io::Result<()> {
		std::fs::write(&self.temporary, contents)?;
		std::fs::rename(&self.temporary, &self.path)?;
		Ok(())
	}

	fn resident_bytes(&self) -> Option<u64> {
		u64::try_from(self.path.capacity()).ok().and_then(|path| {
			u64::try_from(self.temporary.capacity())
				.ok()
				.and_then(|temporary| path.checked_add(temporary))
		})
	}
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum ProfileEvent {
	Start,
	Progress,
	Intent,
	Phase,
	Terminal,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::exec) enum ProfilePhase {
	BaseAccumulation,
	WriterSetCreation,
	ReservoirWrite,
	ReservoirFinish,
	BaseConcat,
	ReservoirProbe,
	Suffix,
	Fold,
	Terminal,
}

#[derive(Clone, Debug, Serialize)]
struct Progress {
	completed_build_batches: u64,
	completed_build_rows: u64,
	active_partition: Option<usize>,
}

#[derive(Clone, Debug, Serialize)]
struct Intent {
	owner: &'static str,
	current_engine_resident_bytes: u64,
	projected_delta_bytes: u64,
	projected_engine_resident_bytes: u64,
	projection_kind: &'static str,
}

#[derive(Clone, Debug, Serialize)]
struct DurableBoundary {
	sequence: u64,
	event: ProfileEvent,
	phase: ProfilePhase,
	completed_build_batches: u64,
	completed_build_rows: u64,
}

#[derive(Debug, Serialize)]
struct EngineLedger<'a> {
	base: &'a BaseOwnership,
	reservoir: &'a WriterAggregate,
	receipt_owned_bytes: CheckedBytes,
	receipt_serialization_transient_bytes: CheckedBytes,
	current_resident_bytes: CheckedBytes,
	spill_disk_bytes: &'a CheckedBytes,
	overlap_label: &'static str,
	unattributed_label: &'static str,
}

#[derive(Debug, Serialize)]
struct CrossLedgerComparison {
	cgroup_anon_minus_engine_current_bytes: CheckedBytes,
	label: &'static str,
}

fn compare_cgroup_anon_to_engine(
	engine_current: &CheckedBytes,
	os_cgroup: &OsLedger,
) -> CrossLedgerComparison {
	let residual = match (
		engine_current.value,
		os_cgroup.memory_stat.anon.value,
	) {
		(Some(engine), Some(anon)) => match anon.checked_sub(engine) {
			Some(value) => CheckedBytes {
				value: Some(value),
				error: None,
			},
			None => CheckedBytes {
				value: None,
				error: Some(
					"engine current ownership exceeds cgroup anon; overlap or sampling scope prevents a residual"
						.to_string(),
				),
			},
		},
		_ => CheckedBytes {
			value: None,
			error: Some(
				"engine current ownership or cgroup anon unavailable for residual comparison"
					.to_string(),
			),
		},
	};
	CrossLedgerComparison {
		cgroup_anon_minus_engine_current_bytes: residual,
		label: "diagnostic_subtraction_only; cgroup_anon_and_engine_ownership_remain_non_additive",
	}
}

#[derive(Debug, Serialize)]
struct ProfileSnapshot<'a> {
	schema_version: u32,
	sequence: u64,
	elapsed_ms: u64,
	event: ProfileEvent,
	phase: ProfilePhase,
	partition_count: u32,
	group_count: u32,
	progress_cadence_batches: u64,
	intent_threshold_bytes: u64,
	progress: Progress,
	intent: Option<Intent>,
	last_completed_boundary: Option<&'a DurableBoundary>,
	engine: EngineLedger<'a>,
	admission_os_cgroup: OsLedger,
	os_cgroup: OsLedger,
	cross_ledger_comparison: CrossLedgerComparison,
	status: Option<&'a str>,
}

pub(in crate::exec) struct GroupedReservoirResidencyProfile {
	sink: Box<dyn SnapshotSink>,
	sampler: Box<dyn OsSampler>,
	base: BaseOwnership,
	started: Instant,
	sequence: u64,
	partition_count: u32,
	group_count: u32,
	completed_build_batches: u64,
	completed_build_rows: u64,
	writer_collection_capacity: usize,
	last_durable_engine_high_water: u64,
	last_completed_boundary: Option<DurableBoundary>,
	admission_os_cgroup: Option<OsLedger>,
}

impl GroupedReservoirResidencyProfile {
	pub(in crate::exec) fn from_env(
		base_batches: &[RecordBatch],
		base_capacity: usize,
		partition_count: u32,
		group_count: u32,
	) -> Result<Option<Self>, ExecutionError> {
		Self::from_optional_path(
			std::env::var_os(GROUPED_RESERVOIR_RESIDENCY_PROFILE_ENV).map(PathBuf::from),
			base_batches,
			base_capacity,
			partition_count,
			group_count,
		)
	}

	fn from_optional_path(
		path: Option<PathBuf>,
		base_batches: &[RecordBatch],
		base_capacity: usize,
		partition_count: u32,
		group_count: u32,
	) -> Result<Option<Self>, ExecutionError> {
		let Some(path) = path else {
			return Ok(None);
		};
		let mut profile = Self::new(
			Box::new(AtomicFileSnapshotSink::new(path)),
			Box::new(ProcCgroupSampler::discover()),
			BaseOwnership::from_batches(base_batches, base_capacity),
			partition_count,
			group_count,
		);
		profile.emit(
			ProfileEvent::Start,
			ProfilePhase::BaseAccumulation,
			&[],
			None,
			Some("base_accumulation_complete"),
		)?;
		Ok(Some(profile))
	}

	#[cfg(test)]
	pub(in crate::exec) fn at_path_for_test(
		path: PathBuf,
		base_batches: &[RecordBatch],
		base_capacity: usize,
		partition_count: u32,
		group_count: u32,
	) -> Result<Self, ExecutionError> {
		Self::from_optional_path(
			Some(path),
			base_batches,
			base_capacity,
			partition_count,
			group_count,
		)?
		.ok_or_else(|| profile_error("test path unexpectedly disabled"))
	}

	fn new(
		sink: Box<dyn SnapshotSink>,
		sampler: Box<dyn OsSampler>,
		base: BaseOwnership,
		partition_count: u32,
		group_count: u32,
	) -> Self {
		Self {
			sink,
			sampler,
			base,
			started: Instant::now(),
			sequence: 0,
			partition_count,
			group_count,
			completed_build_batches: 0,
			completed_build_rows: 0,
			writer_collection_capacity: 0,
			last_durable_engine_high_water: 0,
			last_completed_boundary: None,
			admission_os_cgroup: None,
		}
	}

	fn receipt_owned_bytes(&self) -> CheckedBytes {
		let inline = u64::try_from(std::mem::size_of::<Self>()).ok();
		match (inline, self.sink.resident_bytes()) {
			(Some(inline), Some(sink)) => checked_sum("receipt owned accounting", [inline, sink]),
			_ => CheckedBytes {
				value: None,
				error: Some("receipt resident-size conversion overflow".to_string()),
			},
		}
	}

	fn engine_current_total(&self, writers: &WriterAggregate) -> Result<u64, ExecutionError> {
		checked_sum(
			"engine current-resident accounting",
			[
				self.base
					.current_resident_bytes
					.required("base current resident")?,
				writers
					.aggregate_current_resident_bytes
					.required("writer aggregate current resident")?,
				self.receipt_owned_bytes().required("receipt owned bytes")?,
			],
		)
		.required("engine current resident")
	}

	fn emit(
		&mut self,
		event: ProfileEvent,
		phase: ProfilePhase,
		builds: &[Option<SpilledInnerJoinBuild>],
		intent: Option<Intent>,
		status: Option<&str>,
	) -> Result<(), ExecutionError> {
		let writers = WriterAggregate::from_builds(builds, self.writer_collection_capacity);
		let receipt_owned_bytes = self.receipt_owned_bytes();
		let current_resident_bytes = match (
			self.base.current_resident_bytes.value,
			writers.aggregate_current_resident_bytes.value,
			receipt_owned_bytes.value,
		) {
			(Some(base), Some(writers), Some(receipt)) => checked_sum(
				"engine current-resident accounting",
				[base, writers, receipt],
			),
			_ => CheckedBytes {
				value: None,
				error: Some("engine ownership component unavailable".to_string()),
			},
		};
		let engine_total = current_resident_bytes.value;
		let os_cgroup = self.sampler.sample();
		if self.admission_os_cgroup.is_none() {
			self.admission_os_cgroup = Some(os_cgroup.clone());
		}
		let admission_os_cgroup = self
			.admission_os_cgroup
			.as_ref()
			.expect("admission sample is set above")
			.clone();
		let cross_ledger_comparison =
			compare_cgroup_anon_to_engine(&current_resident_bytes, &os_cgroup);
		let elapsed_ms = u64::try_from(self.started.elapsed().as_millis())
			.map_err(|_| profile_error("snapshot elapsed milliseconds overflow"))?;
		let snapshot = ProfileSnapshot {
			schema_version: GROUPED_RESERVOIR_PROFILE_SCHEMA_VERSION,
			sequence: self.sequence,
			elapsed_ms,
			event,
			phase,
			partition_count: self.partition_count,
			group_count: self.group_count,
			progress_cadence_batches: GROUPED_RESERVOIR_PROGRESS_BATCHES,
			intent_threshold_bytes: GROUPED_RESERVOIR_INTENT_BYTES,
			progress: Progress {
				completed_build_batches: self.completed_build_batches,
				completed_build_rows: self.completed_build_rows,
				active_partition: None,
			},
			intent,
			last_completed_boundary: self.last_completed_boundary.as_ref(),
			engine: EngineLedger {
				base: &self.base,
				reservoir: &writers,
				receipt_owned_bytes,
				receipt_serialization_transient_bytes: CheckedBytes {
					value: None,
					error: Some(
						"allocated after ownership sampling; snapshot file size bounds serialized payload"
							.to_string(),
					),
				},
				current_resident_bytes,
				spill_disk_bytes: &writers.spill_disk_bytes,
				overlap_label: "same-ledger_components_only; shared_arrow_buffers_not_deduplicated",
				unattributed_label:
					"allocator_runtime_residual_reported_in_cross_ledger_comparison",
			},
			admission_os_cgroup,
			os_cgroup,
			cross_ledger_comparison,
			status,
		};
		let contents = serde_json::to_vec(&snapshot)
			.map_err(|error| profile_error(format!("serialize snapshot failed: {error}")))?;
		self.sink
			.write_snapshot(&contents)
			.map_err(|error| profile_error(format!("write snapshot failed: {error}")))?;
		if let Some(total) = engine_total {
			self.last_durable_engine_high_water = self.last_durable_engine_high_water.max(total);
		}
		if !matches!(event, ProfileEvent::Intent) {
			self.last_completed_boundary = Some(DurableBoundary {
				sequence: self.sequence,
				event,
				phase,
				completed_build_batches: self.completed_build_batches,
				completed_build_rows: self.completed_build_rows,
			});
		}
		self.sequence = self
			.sequence
			.checked_add(1)
			.ok_or_else(|| profile_error("snapshot sequence overflow"))?;
		Ok(())
	}

	pub(in crate::exec) fn writer_set_intent(
		&mut self,
		projected_delta_bytes: u64,
	) -> Result<(), ExecutionError> {
		let empty = WriterAggregate::from_builds(&[], 0);
		let current = self.engine_current_total(&empty)?;
		let projected = current
			.checked_add(projected_delta_bytes)
			.ok_or_else(|| profile_error("writer-set projected total overflow"))?;
		self.emit(
			ProfileEvent::Intent,
			ProfilePhase::WriterSetCreation,
			&[],
			Some(Intent {
				owner: "grouped_reservoir_writer_set",
				current_engine_resident_bytes: current,
				projected_delta_bytes,
				projected_engine_resident_bytes: projected,
				projection_kind: "exact_collection_capacity; writer_internal_allocations_unavailable",
			}),
			None,
		)
	}

	pub(in crate::exec) fn writer_set_ready(
		&mut self,
		builds: &Vec<Option<SpilledInnerJoinBuild>>,
	) -> Result<(), ExecutionError> {
		self.writer_collection_capacity = builds.capacity();
		self.emit(
			ProfileEvent::Phase,
			ProfilePhase::ReservoirWrite,
			builds,
			None,
			Some("writer_set_ready"),
		)
	}

	pub(in crate::exec) fn before_reservoir_batch(
		&mut self,
		builds: &[Option<SpilledInnerJoinBuild>],
		batch: &RecordBatch,
	) -> Result<(), ExecutionError> {
		let rows = u64::try_from(batch.num_rows())
			.map_err(|_| profile_error("reservoir batch row count conversion overflow"))?;
		let row_growth = rows
			.checked_mul(96)
			.ok_or_else(|| profile_error("reservoir batch index estimate overflow"))?;
		let projected_delta = record_batch_memory_bytes(batch)
			.checked_add(row_growth)
			.ok_or_else(|| profile_error("reservoir batch growth estimate overflow"))?;
		self.before_projected_reservoir_growth(builds, projected_delta)
	}

	fn before_projected_reservoir_growth(
		&mut self,
		builds: &[Option<SpilledInnerJoinBuild>],
		projected_delta: u64,
	) -> Result<(), ExecutionError> {
		let writers = WriterAggregate::from_builds(builds, self.writer_collection_capacity);
		let current = self.engine_current_total(&writers)?;
		let projected = current
			.checked_add(projected_delta)
			.ok_or_else(|| profile_error("reservoir batch projected total overflow"))?;
		let intent_floor = self
			.last_durable_engine_high_water
			.checked_add(GROUPED_RESERVOIR_INTENT_BYTES)
			.ok_or_else(|| profile_error("intent high-water threshold overflow"))?;
		if projected >= intent_floor {
			self.emit(
				ProfileEvent::Intent,
				ProfilePhase::ReservoirWrite,
				builds,
				Some(Intent {
					owner: "grouped_reservoir_batch_write",
					current_engine_resident_bytes: current,
					projected_delta_bytes: projected_delta,
					projected_engine_resident_bytes: projected,
					projection_kind: "conservative_batch_buffers_plus_index_row_estimate",
				}),
				None,
			)?;
			self.last_durable_engine_high_water = projected;
		}
		Ok(())
	}

	pub(in crate::exec) fn reservoir_batch_complete(
		&mut self,
		builds: &[Option<SpilledInnerJoinBuild>],
		rows: usize,
	) -> Result<(), ExecutionError> {
		self.completed_build_batches = self
			.completed_build_batches
			.checked_add(1)
			.ok_or_else(|| profile_error("completed build batch count overflow"))?;
		self.completed_build_rows = self
			.completed_build_rows
			.checked_add(
				u64::try_from(rows)
					.map_err(|_| profile_error("completed build row count conversion overflow"))?,
			)
			.ok_or_else(|| profile_error("completed build row count overflow"))?;
		if self.completed_build_batches % GROUPED_RESERVOIR_PROGRESS_BATCHES == 0 {
			self.emit(
				ProfileEvent::Progress,
				ProfilePhase::ReservoirWrite,
				builds,
				None,
				None,
			)?;
		}
		Ok(())
	}

	pub(in crate::exec) fn phase(
		&mut self,
		phase: ProfilePhase,
		builds: &[Option<SpilledInnerJoinBuild>],
		status: &'static str,
	) -> Result<(), ExecutionError> {
		self.emit(ProfileEvent::Phase, phase, builds, None, Some(status))
	}

	pub(in crate::exec) fn base_concat_intent(
		&mut self,
		builds: &[Option<SpilledInnerJoinBuild>],
	) -> Result<(), ExecutionError> {
		let projected_delta_bytes = self
			.base
			.batch_buffer_accounted_bytes
			.required("base concat buffer accounting")?;
		let writers = WriterAggregate::from_builds(builds, self.writer_collection_capacity);
		let current = self.engine_current_total(&writers)?;
		let projected = current
			.checked_add(projected_delta_bytes)
			.ok_or_else(|| profile_error("base concat projected total overflow"))?;
		self.emit(
			ProfileEvent::Intent,
			ProfilePhase::BaseConcat,
			builds,
			Some(Intent {
				owner: "grouped_reservoir_base_concat",
				current_engine_resident_bytes: current,
				projected_delta_bytes,
				projected_engine_resident_bytes: projected,
				projection_kind: "accounted_base_buffers; concat_allocator_overhead_unavailable",
			}),
			None,
		)
	}

	pub(in crate::exec) fn terminal(
		&mut self,
		builds: &[Option<SpilledInnerJoinBuild>],
		status: &str,
	) -> Result<(), ExecutionError> {
		self.emit(
			ProfileEvent::Terminal,
			ProfilePhase::Terminal,
			builds,
			None,
			Some(status),
		)
	}
}

pub(in crate::exec) fn writer_set_collection_bytes(
	partition_count: u32,
) -> Result<u64, ExecutionError> {
	let count = usize::try_from(partition_count)
		.map_err(|_| profile_error("partition count conversion overflow"))?;
	checked_capacity_bytes::<Option<SpilledInnerJoinBuild>>(count, "writer-set collection")
		.required("writer-set collection")
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::sync::{Arc, Mutex};

	#[derive(Clone, Default)]
	struct SharedSink {
		writes: Arc<Mutex<Vec<Vec<u8>>>>,
		fail: bool,
	}

	impl SnapshotSink for SharedSink {
		fn write_snapshot(&mut self, contents: &[u8]) -> io::Result<()> {
			if self.fail {
				return Err(io::Error::other("injected snapshot failure"));
			}
			self.writes
				.lock()
				.expect("sink lock")
				.push(contents.to_vec());
			Ok(())
		}

		fn resident_bytes(&self) -> Option<u64> {
			Some(0)
		}
	}

	struct FixedSampler;

	impl OsSampler for FixedSampler {
		fn sample(&mut self) -> OsLedger {
			OsLedger {
				process_rss_bytes: Measurement::value(10),
				cgroup_v2_path: Some("/test".to_string()),
				cgroup_path_error: None,
				memory_current_bytes: Measurement::value(20),
				memory_stat: parse_memory_stat(
					"anon 1000000\nfile 2\nfile_dirty 3\nfile_writeback 4\nshmem 5\nkernel 6\nslab 7\n",
				),
				non_additive_label: "os_cgroup_values_are_not_added_to_engine_ownership",
			}
		}
	}

	fn profile(sink: SharedSink) -> GroupedReservoirResidencyProfile {
		GroupedReservoirResidencyProfile::new(
			Box::new(sink),
			Box::new(FixedSampler),
			BaseOwnership::empty(),
			4,
			2,
		)
	}

	fn last_snapshot(sink: &SharedSink) -> serde_json::Value {
		let writes = sink.writes.lock().expect("sink lock");
		serde_json::from_slice(writes.last().expect("snapshot write")).expect("snapshot json")
	}

	#[test]
	fn memory_stat_parser_marks_missing_invalid_and_unknown_fields() {
		let parsed = parse_memory_stat(
			"anon 11\nfile 12\nfile_dirty nope\nfile_writeback 14\nshmem 15\nkernel 16\nslab 17\nfuture_counter 99\nbroken\n",
		);
		assert_eq!(parsed.anon.value, Some(11));
		assert_eq!(parsed.file.value, Some(12));
		assert_eq!(parsed.file_dirty.value, None);
		assert!(
			parsed
				.file_dirty
				.error
				.as_deref()
				.is_some_and(|error| error.contains("missing"))
		);
		assert_eq!(parsed.unknown.get("future_counter"), Some(&99));
		assert_eq!(parsed.invalid_lines, ["file_dirty nope", "broken"]);
	}

	#[test]
	fn process_rss_parser_requires_bytes_convertible_kib() {
		assert_eq!(
			parse_process_rss("Name:\ttoron\nVmRSS:\t42 kB\n").value,
			Some(42 * 1024)
		);
		assert!(parse_process_rss("Name:\ttoron\n").value.is_none());
		assert!(parse_process_rss("VmRSS: 42 pages\n").value.is_none());
	}

	#[test]
	fn checked_accounting_never_saturates() {
		let overflow = checked_sum("test component", [u64::MAX, 1]);
		assert_eq!(overflow.value, None);
		assert_eq!(overflow.error.as_deref(), Some("test component overflow"));
	}

	#[test]
	fn disabled_profile_constructs_no_state_or_sink() {
		let profile = GroupedReservoirResidencyProfile::from_optional_path(None, &[], 0, 4, 2)
			.expect("disabled profile");
		assert!(profile.is_none());
	}

	#[test]
	fn all_writer_snapshots_contribute_to_aggregate_and_maximum() {
		let writer = |current, guard, spill| SpilledInnerJoinResidencySnapshot {
			resident_baseline_guard_bytes: 0,
			resident_index_bytes: current,
			writer_scratch_peak_bytes: guard,
			page_reserve_bytes: 0,
			cache_payload_bytes: 0,
			cache_cap_bytes: 0,
			cache_peak_bytes: 0,
			pinned_decoded_bytes: 0,
			page_metadata_bytes: Some(0),
			cache_metadata_bytes: Some(0),
			pinned_handle_capacity_bytes: Some(0),
			write_scratch_capacity_bytes: Some(0),
			retained_legacy_index_bytes: Some(0),
			path_capacity_bytes: Some(0),
			open_file_handles: 1,
			input_rows: 1,
			spill_disk_bytes: spill,
			current_resident_bytes: Some(current),
			current_resident_error: None,
			guard_accounted_bytes: Some(guard),
			guard_accounted_error: None,
			overlap_label: "test",
		};
		let aggregate = WriterAggregate::from_snapshots(
			vec![
				writer(10, 30, 100),
				writer(20, 40, 200),
				writer(30, 50, 300),
			],
			CheckedBytes {
				value: Some(7),
				error: None,
			},
		);
		assert_eq!(aggregate.writer_count, 3);
		assert_eq!(aggregate.aggregate_current_resident_bytes.value, Some(67));
		assert_eq!(aggregate.maximum_writer_current_resident_bytes, Some(30));
		assert_eq!(aggregate.aggregate_guard_accounted_bytes.value, Some(120));
		assert_eq!(aggregate.spill_disk_bytes.value, Some(600));
		assert_eq!(aggregate.open_file_handles.value, Some(3));
		let index = aggregate
			.component_summaries
			.get("resident_index_bytes")
			.expect("index summary");
		assert_eq!(index.aggregate_bytes.value, Some(60));
		assert_eq!(index.maximum_writer_bytes, Some(30));
	}

	#[test]
	fn allocation_intent_is_durable_before_the_projected_growth() {
		let sink = SharedSink::default();
		let mut profile = profile(sink.clone());
		profile
			.phase(ProfilePhase::ReservoirWrite, &[], "prior_boundary")
			.expect("prior snapshot");
		profile
			.before_projected_reservoir_growth(&[], GROUPED_RESERVOIR_INTENT_BYTES)
			.expect("intent snapshot");
		let snapshot = last_snapshot(&sink);
		assert_eq!(snapshot["event"], "intent");
		assert_eq!(snapshot["phase"], "reservoir_write");
		assert_eq!(snapshot["intent"]["owner"], "grouped_reservoir_batch_write");
		assert_eq!(snapshot["last_completed_boundary"]["sequence"], 0);
		assert_eq!(snapshot["last_completed_boundary"]["event"], "phase");
	}

	#[test]
	fn progress_cadence_never_exceeds_sixty_four_completed_batches() {
		let sink = SharedSink::default();
		let mut profile = profile(sink.clone());
		for _ in 0..GROUPED_RESERVOIR_PROGRESS_BATCHES - 1 {
			profile
				.reservoir_batch_complete(&[], 1)
				.expect("complete batch");
		}
		assert!(sink.writes.lock().expect("sink lock").is_empty());
		profile
			.reservoir_batch_complete(&[], 1)
			.expect("cadence snapshot");
		let snapshot = last_snapshot(&sink);
		assert_eq!(snapshot["event"], "progress");
		assert_eq!(snapshot["progress"]["completed_build_batches"], 64);
	}

	#[test]
	fn every_named_phase_transition_is_flushed() {
		let sink = SharedSink::default();
		let mut profile = profile(sink.clone());
		for phase in [
			ProfilePhase::WriterSetCreation,
			ProfilePhase::ReservoirWrite,
			ProfilePhase::ReservoirFinish,
			ProfilePhase::BaseConcat,
			ProfilePhase::ReservoirProbe,
			ProfilePhase::Suffix,
			ProfilePhase::Fold,
		] {
			profile
				.phase(phase, &[], "transition")
				.expect("phase snapshot");
		}
		profile
			.terminal(&[], "complete")
			.expect("terminal snapshot");
		assert_eq!(sink.writes.lock().expect("sink lock").len(), 8);
		assert_eq!(last_snapshot(&sink)["event"], "terminal");
	}

	#[test]
	fn engine_and_os_ledgers_are_separate_and_not_cross_summed() {
		let sink = SharedSink::default();
		let mut profile = profile(sink.clone());
		profile
			.phase(ProfilePhase::ReservoirWrite, &[], "test")
			.expect("phase snapshot");
		let snapshot = last_snapshot(&sink);
		assert_eq!(
			snapshot["engine"]["current_resident_bytes"]["value"]
				.as_u64()
				.is_some(),
			true
		);
		assert_eq!(snapshot["os_cgroup"]["process_rss_bytes"]["value"], 10);
		assert_eq!(snapshot["os_cgroup"]["memory_current_bytes"]["value"], 20);
		assert_eq!(
			snapshot["admission_os_cgroup"]["memory_current_bytes"]["value"],
			20
		);
		let engine = snapshot["engine"]["current_resident_bytes"]["value"]
			.as_u64()
			.expect("engine current");
		let residual =
			snapshot["cross_ledger_comparison"]["cgroup_anon_minus_engine_current_bytes"]["value"]
				.as_u64()
				.expect("cross-ledger residual");
		assert_eq!(engine + residual, 1_000_000);
		assert!(
			snapshot["engine"]["unattributed_label"]
				.as_str()
				.expect("label")
				.contains("cross_ledger_comparison")
		);
	}

	#[test]
	fn enabled_snapshot_failures_are_loud() {
		let sink = SharedSink {
			fail: true,
			..SharedSink::default()
		};
		let mut profile = profile(sink);
		let error = profile
			.phase(ProfilePhase::ReservoirWrite, &[], "test")
			.expect_err("injected sink failure must surface");
		assert!(error.to_string().contains("write snapshot failed"));
	}

	#[test]
	fn interrupted_temporary_write_preserves_prior_parseable_snapshot() {
		let directory = std::env::temp_dir().join(format!(
			"toron_grouped_reservoir_atomic_{}_{}",
			std::process::id(),
			std::thread::current().name().unwrap_or("test")
		));
		let _ = std::fs::remove_dir_all(&directory);
		std::fs::create_dir_all(&directory).expect("test directory");
		let path = directory.join("profile.json");
		let mut sink = AtomicFileSnapshotSink::new(path.clone());
		sink.write_snapshot(br#"{"sequence":1}"#)
			.expect("prior snapshot");
		std::fs::write(&sink.temporary, b"{").expect("interrupted temp write");
		let prior: serde_json::Value =
			serde_json::from_slice(&std::fs::read(&path).expect("prior file"))
				.expect("prior remains parseable");
		assert_eq!(prior["sequence"], 1);
		let _ = std::fs::remove_dir_all(directory);
	}
}
