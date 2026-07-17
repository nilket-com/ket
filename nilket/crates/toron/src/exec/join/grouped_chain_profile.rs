use crate::distributed::task::TaskTelemetry;
use crate::error::ExecutionError;
use crate::ir::JoinReplayId;
use serde::Serialize;
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub(in crate::exec) const GROUPED_CHAIN_PROFILE_ENV: &str = "TORON_GROUPED_CHAIN_PROFILE_DIR";
const PROFILE_SCHEMA_VERSION: u32 = 2;
const LEAF_COUNT: usize = 9;
const SUB_LEAF_COUNT: usize = 15;
static NEXT_PROFILE_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::exec) enum GroupedChainProfileLeaf {
	RuntimePrepBuild = 0,
	BaseAccumulation = 1,
	ReservoirPartitionSpillWrite = 2,
	BaseConcat = 3,
	ReservoirReadProbe = 4,
	SuffixJoin = 5,
	GroupedFold = 6,
	Finalization = 7,
	Cleanup = 8,
}

impl GroupedChainProfileLeaf {
	const ALL: [Self; LEAF_COUNT] = [
		Self::RuntimePrepBuild,
		Self::BaseAccumulation,
		Self::ReservoirPartitionSpillWrite,
		Self::BaseConcat,
		Self::ReservoirReadProbe,
		Self::SuffixJoin,
		Self::GroupedFold,
		Self::Finalization,
		Self::Cleanup,
	];

	fn index(self) -> usize {
		self as usize
	}

	fn name(self) -> &'static str {
		match self {
			Self::RuntimePrepBuild => "runtime_prep_build",
			Self::BaseAccumulation => "base_accumulation",
			Self::ReservoirPartitionSpillWrite => "reservoir_partition_spill_write",
			Self::BaseConcat => "base_concat",
			Self::ReservoirReadProbe => "reservoir_read_probe",
			Self::SuffixJoin => "suffix_join",
			Self::GroupedFold => "grouped_fold",
			Self::Finalization => "finalization",
			Self::Cleanup => "cleanup",
		}
	}
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::exec) enum GroupedChainSubLeaf {
	WriterSetup = 0,
	WriteScanDecode = 1,
	WritePartitionRouting = 2,
	WriteIpcEncode = 3,
	WriteEncryption = 4,
	WritePhysicalWrite = 5,
	WriteFinishFsync = 6,
	ReadProbePartitioning = 7,
	ReadPhysicalRead = 8,
	ReadDecrypt = 9,
	ReadIpcDecode = 10,
	ReadCacheLru = 11,
	ReadIndexRebuildCpu = 12,
	ReadLookup = 13,
	ReadMaterialization = 14,
}

impl GroupedChainSubLeaf {
	const ALL: [Self; SUB_LEAF_COUNT] = [
		Self::WriterSetup,
		Self::WriteScanDecode,
		Self::WritePartitionRouting,
		Self::WriteIpcEncode,
		Self::WriteEncryption,
		Self::WritePhysicalWrite,
		Self::WriteFinishFsync,
		Self::ReadProbePartitioning,
		Self::ReadPhysicalRead,
		Self::ReadDecrypt,
		Self::ReadIpcDecode,
		Self::ReadCacheLru,
		Self::ReadIndexRebuildCpu,
		Self::ReadLookup,
		Self::ReadMaterialization,
	];

	fn index(self) -> usize {
		self as usize
	}

	fn parent(self) -> GroupedChainProfileLeaf {
		match self {
			Self::WriterSetup
			| Self::WriteScanDecode
			| Self::WritePartitionRouting
			| Self::WriteIpcEncode
			| Self::WriteEncryption
			| Self::WritePhysicalWrite
			| Self::WriteFinishFsync => GroupedChainProfileLeaf::ReservoirPartitionSpillWrite,
			Self::ReadProbePartitioning
			| Self::ReadPhysicalRead
			| Self::ReadDecrypt
			| Self::ReadIpcDecode
			| Self::ReadCacheLru
			| Self::ReadIndexRebuildCpu
			| Self::ReadLookup
			| Self::ReadMaterialization => GroupedChainProfileLeaf::ReservoirReadProbe,
		}
	}

	fn name(self) -> &'static str {
		match self {
			Self::WriterSetup => "writer_setup",
			Self::WriteScanDecode => "write_scan_decode",
			Self::WritePartitionRouting => "write_partition_routing",
			Self::WriteIpcEncode => "write_ipc_encode",
			Self::WriteEncryption => "write_encryption",
			Self::WritePhysicalWrite => "write_physical_write",
			Self::WriteFinishFsync => "write_finish_fsync",
			Self::ReadProbePartitioning => "read_probe_partitioning",
			Self::ReadPhysicalRead => "read_physical_read",
			Self::ReadDecrypt => "read_decrypt",
			Self::ReadIpcDecode => "read_ipc_decode",
			Self::ReadCacheLru => "read_cache_lru",
			Self::ReadIndexRebuildCpu => "read_index_rebuild_cpu",
			Self::ReadLookup => "read_lookup",
			Self::ReadMaterialization => "read_materialization",
		}
	}
}

#[derive(Debug, Serialize)]
struct LeafSnapshot {
	name: &'static str,
	micros: u64,
	calls: u64,
	rows: u64,
	bytes: u64,
	share: f64,
}

#[derive(Debug, Serialize)]
struct SubLeafSnapshot {
	name: &'static str,
	parent: &'static str,
	micros: u64,
	calls: u64,
	rows: u64,
	bytes: u64,
	pages: u64,
	share_of_root: f64,
	share_of_parent: f64,
}

#[derive(Debug, Serialize)]
struct ParentRefinementSnapshot {
	parent: &'static str,
	measured_micros: u64,
	sub_leaf_micros: u64,
	gap_micros: u64,
	gap_ratio: f64,
	gap_within_ceiling: bool,
}

#[derive(Debug, Default, Serialize)]
struct BranchSnapshot {
	resident_partitions: u64,
	streamed_partitions: u64,
	pin_admitted: u64,
	pin_rejected_preflight: u64,
	pin_rejected_mid_decode: u64,
}

#[derive(Debug, Serialize)]
struct SpillIoSnapshot {
	unique_ciphertext_pages: u64,
	unique_ciphertext_bytes: u64,
	physical_write_calls: u64,
	physical_write_bytes: u64,
	physical_read_calls: u64,
	physical_read_bytes: u64,
	plaintext_cache_hits: u64,
	plaintext_cache_misses: u64,
	decoded_cache_hits: u64,
	decoded_cache_misses: u64,
	decode_count: u64,
	read_amplification: f64,
	inventory_reconciled: bool,
}

#[derive(Debug, Clone, Serialize)]
struct RuntimeStageSnapshot {
	replay_id: u32,
	build_rows: u64,
	probe_rows: u64,
	output_rows: u64,
}

#[derive(Debug, Serialize)]
struct GroupedChainSnapshot {
	schema_version: u32,
	profile_id: u64,
	path: &'static str,
	structural_stage_count: u64,
	reservoir_replay_id: u32,
	partition_count: u32,
	group_count: u32,
	root_span_micros: u64,
	leaves: Vec<LeafSnapshot>,
	sub_leaves: Vec<SubLeafSnapshot>,
	parent_refinements: Vec<ParentRefinementSnapshot>,
	in_root_residual_micros: u64,
	in_root_residual_ratio: f64,
	in_root_residual_within_ceiling: bool,
	task_spill_at_root_start: u64,
	task_spill_at_runtime_prep_end: u64,
	task_spill_at_base_end: u64,
	task_spill_at_reservoir_build_end: u64,
	task_spill_at_reservoir_probe_end: u64,
	task_spill_at_suffix_fold_end: u64,
	task_spill_at_root_end: u64,
	task_spill_delta: u64,
	runtime_build_spill_bytes: u64,
	reservoir_spill_bytes: u64,
	spill_roles_reconciled: bool,
	base_batches: u64,
	base_rows: u64,
	base_bytes: u64,
	reservoir_build_rows: u64,
	reservoir_probe_rows: u64,
	reservoir_output_rows: u64,
	fold_input_rows: u64,
	final_group_rows: u64,
	runtime_stages: Vec<RuntimeStageSnapshot>,
	branches: BranchSnapshot,
	spill_io: SpillIoSnapshot,
}

#[derive(Debug, Default)]
struct ProfileState {
	active_leaf: Option<GroupedChainProfileLeaf>,
	active_sub_leaf: Option<GroupedChainSubLeaf>,
	root_started: bool,
	root_span_micros: Option<u64>,
	leaf_micros: [u64; LEAF_COUNT],
	leaf_calls: [u64; LEAF_COUNT],
	leaf_rows: [u64; LEAF_COUNT],
	leaf_bytes: [u64; LEAF_COUNT],
	sub_leaf_nanos: [u128; SUB_LEAF_COUNT],
	sub_leaf_calls: [u64; SUB_LEAF_COUNT],
	sub_leaf_rows: [u64; SUB_LEAF_COUNT],
	sub_leaf_bytes: [u64; SUB_LEAF_COUNT],
	sub_leaf_pages: [u64; SUB_LEAF_COUNT],
	task_spill_at_root_start: Option<u64>,
	task_spill_at_runtime_prep_end: Option<u64>,
	task_spill_at_base_end: Option<u64>,
	task_spill_at_reservoir_build_end: Option<u64>,
	task_spill_at_reservoir_probe_end: Option<u64>,
	task_spill_at_suffix_fold_end: Option<u64>,
	task_spill_at_root_end: Option<u64>,
	reservoir_spill_bytes: Option<u64>,
	reservoir_replay_id: Option<u32>,
	partition_count: Option<u32>,
	group_count: Option<u32>,
	base_batches: u64,
	base_rows: u64,
	base_bytes: u64,
	reservoir_build_rows: Option<u64>,
	reservoir_probe_rows: Option<u64>,
	reservoir_output_rows: Option<u64>,
	fold_input_rows: u64,
	final_group_rows: Option<u64>,
	runtime_stages: Vec<RuntimeStageSnapshot>,
	page_inventory: BTreeMap<(u64, u64), u64>,
	physical_write_calls: u64,
	physical_write_bytes: u64,
	physical_read_calls: u64,
	physical_read_bytes: u64,
	plaintext_cache_hits: u64,
	plaintext_cache_misses: u64,
	decoded_cache_hits: u64,
	decoded_cache_misses: u64,
	decode_count: u64,
	resident_partitions: u64,
	streamed_partitions: u64,
	pin_admitted: u64,
	pin_rejected_preflight: u64,
	pin_rejected_mid_decode: u64,
}

#[derive(Debug)]
pub(in crate::exec) struct GroupedChainProfile {
	directory: PathBuf,
	profile_id: u64,
	structural_stage_count: u64,
	state: Mutex<ProfileState>,
}

pub(in crate::exec) struct GroupedChainProfileTimer<'a> {
	profile: &'a GroupedChainProfile,
	leaf: GroupedChainProfileLeaf,
	started: Instant,
	finished: bool,
}

#[derive(Clone, Copy)]
pub(in crate::exec) struct GroupedChainSpillObserver<'a> {
	profile: &'a GroupedChainProfile,
}

pub(in crate::exec) struct GroupedChainSubLeafTimer<'a> {
	profile: &'a GroupedChainProfile,
	leaf: GroupedChainSubLeaf,
	started: Instant,
	finished: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::exec) enum GroupedChainPinOutcome {
	Admitted,
	RejectedPreflight,
	RejectedMidDecode,
}

impl GroupedChainProfile {
	pub(in crate::exec) fn from_env(
		structural_stage_count: usize,
	) -> Result<Option<Box<Self>>, ExecutionError> {
		Self::from_lookup(structural_stage_count, |name| std::env::var_os(name))
			.map(|profile| profile.map(Box::new))
	}

	fn from_lookup(
		structural_stage_count: usize,
		lookup: impl FnOnce(&str) -> Option<OsString>,
	) -> Result<Option<Self>, ExecutionError> {
		let Some(raw) = lookup(GROUPED_CHAIN_PROFILE_ENV) else {
			return Ok(None);
		};
		if raw.is_empty() {
			return Ok(None);
		}
		Ok(Some(Self {
			directory: PathBuf::from(raw),
			profile_id: NEXT_PROFILE_ID.fetch_add(1, Ordering::Relaxed),
			structural_stage_count: checked_usize(
				structural_stage_count,
				"structural stage count",
			)?,
			state: Mutex::new(ProfileState::default()),
		}))
	}

	pub(in crate::exec) fn task_spill_bytes(
		telemetry: &Option<Arc<Mutex<TaskTelemetry>>>,
	) -> Result<u64, ExecutionError> {
		let telemetry = telemetry
			.as_ref()
			.ok_or_else(|| profile_error("task telemetry is required while profiling"))?;
		telemetry
			.lock()
			.map(|telemetry| telemetry.counters.spill_bytes)
			.map_err(|_| profile_error("task telemetry mutex poisoned"))
	}

	pub(in crate::exec) fn begin_root(
		&self,
		task_spill_at_root_start: u64,
	) -> Result<Instant, ExecutionError> {
		let mut state = self.lock_state()?;
		if state.root_started || state.root_span_micros.is_some() {
			return Err(profile_error("root span started more than once"));
		}
		state.root_started = true;
		state.task_spill_at_root_start = Some(task_spill_at_root_start);
		Ok(Instant::now())
	}

	pub(in crate::exec) fn finish_root(
		&self,
		started: Instant,
		task_spill_at_root_end: u64,
	) -> Result<(), ExecutionError> {
		let elapsed = checked_duration_micros(started.elapsed())?;
		let mut state = self.lock_state()?;
		if !state.root_started || state.root_span_micros.is_some() {
			return Err(profile_error("root span finished without one active root"));
		}
		if state.active_leaf.is_some() || state.active_sub_leaf.is_some() {
			return Err(profile_error(
				"root span finished while an exclusive leaf or sub-leaf was active",
			));
		}
		state.root_span_micros = Some(elapsed);
		state.task_spill_at_root_end = Some(task_spill_at_root_end);
		Ok(())
	}

	pub(in crate::exec) fn begin_leaf(
		&self,
		leaf: GroupedChainProfileLeaf,
	) -> Result<GroupedChainProfileTimer<'_>, ExecutionError> {
		let mut state = self.lock_state()?;
		if !state.root_started || state.root_span_micros.is_some() {
			return Err(profile_error(format!(
				"exclusive leaf {} started outside the root span",
				leaf.name()
			)));
		}
		if let Some(active) = state.active_leaf {
			return Err(profile_error(format!(
				"exclusive leaf {} overlapped active leaf {}",
				leaf.name(),
				active.name()
			)));
		}
		state.active_leaf = Some(leaf);
		drop(state);
		Ok(GroupedChainProfileTimer {
			profile: self,
			leaf,
			started: Instant::now(),
			finished: false,
		})
	}

	pub(in crate::exec) fn spill_observer(&self) -> GroupedChainSpillObserver<'_> {
		GroupedChainSpillObserver { profile: self }
	}

	fn begin_sub_leaf(
		&self,
		leaf: GroupedChainSubLeaf,
	) -> Result<GroupedChainSubLeafTimer<'_>, ExecutionError> {
		let started = Instant::now();
		let mut state = self.lock_state()?;
		if state.active_leaf != Some(leaf.parent()) {
			return Err(profile_error(format!(
				"sub-leaf {} started outside parent {}",
				leaf.name(),
				leaf.parent().name()
			)));
		}
		if let Some(active) = state.active_sub_leaf {
			return Err(profile_error(format!(
				"sub-leaf {} overlapped active sub-leaf {}",
				leaf.name(),
				active.name()
			)));
		}
		state.active_sub_leaf = Some(leaf);
		drop(state);
		Ok(GroupedChainSubLeafTimer {
			profile: self,
			leaf,
			started,
			finished: false,
		})
	}

	fn record_written_page(
		&self,
		spill_id: u64,
		page_index: usize,
		ciphertext_len: usize,
	) -> Result<(), ExecutionError> {
		let mut state = self.lock_state()?;
		let page_index = checked_usize(page_index, "spill page index")?;
		let ciphertext_len = checked_usize(ciphertext_len, "spill ciphertext length")?;
		if state
			.page_inventory
			.insert((spill_id, page_index), ciphertext_len)
			.is_some()
		{
			return Err(profile_error(format!(
				"duplicate ciphertext page inventory entry ({spill_id}, {page_index})"
			)));
		}
		Ok(())
	}

	fn record_physical_write(&self, bytes: usize) -> Result<(), ExecutionError> {
		let mut state = self.lock_state()?;
		checked_add(&mut state.physical_write_calls, 1, "physical write calls")?;
		checked_add(
			&mut state.physical_write_bytes,
			checked_usize(bytes, "physical write bytes")?,
			"physical write bytes",
		)
	}

	fn record_physical_read(&self, bytes: usize) -> Result<(), ExecutionError> {
		let mut state = self.lock_state()?;
		checked_add(&mut state.physical_read_calls, 1, "physical read calls")?;
		checked_add(
			&mut state.physical_read_bytes,
			checked_usize(bytes, "physical read bytes")?,
			"physical read bytes",
		)
	}

	fn record_cache_event(&self, plaintext: bool, hit: bool) -> Result<(), ExecutionError> {
		let mut state = self.lock_state()?;
		let (counter, label) = match (plaintext, hit) {
			(true, true) => (&mut state.plaintext_cache_hits, "plaintext cache hits"),
			(true, false) => (&mut state.plaintext_cache_misses, "plaintext cache misses"),
			(false, true) => (&mut state.decoded_cache_hits, "decoded cache hits"),
			(false, false) => (&mut state.decoded_cache_misses, "decoded cache misses"),
		};
		checked_add(counter, 1, label)
	}

	fn record_decode(&self) -> Result<(), ExecutionError> {
		let mut state = self.lock_state()?;
		checked_add(&mut state.decode_count, 1, "decode count")
	}

	fn record_branch(&self, resident: bool) -> Result<(), ExecutionError> {
		let mut state = self.lock_state()?;
		if resident {
			checked_add(
				&mut state.resident_partitions,
				1,
				"resident branch partitions",
			)
		} else {
			checked_add(
				&mut state.streamed_partitions,
				1,
				"streamed branch partitions",
			)
		}
	}

	fn record_pin_outcome(&self, outcome: GroupedChainPinOutcome) -> Result<(), ExecutionError> {
		let mut state = self.lock_state()?;
		let (counter, label) = match outcome {
			GroupedChainPinOutcome::Admitted => (&mut state.pin_admitted, "pin admitted"),
			GroupedChainPinOutcome::RejectedPreflight => {
				(&mut state.pin_rejected_preflight, "pin rejected preflight")
			}
			GroupedChainPinOutcome::RejectedMidDecode => (
				&mut state.pin_rejected_mid_decode,
				"pin rejected mid-decode",
			),
		};
		checked_add(counter, 1, label)
	}

	pub(in crate::exec) fn record_runtime_prep_end_spill(
		&self,
		bytes: u64,
	) -> Result<(), ExecutionError> {
		let mut state = self.lock_state()?;
		if state
			.task_spill_at_runtime_prep_end
			.replace(bytes)
			.is_some()
		{
			return Err(profile_error(
				"runtime-preparation spill boundary recorded more than once",
			));
		}
		Ok(())
	}

	pub(in crate::exec) fn record_base_end_spill(&self, bytes: u64) -> Result<(), ExecutionError> {
		let mut state = self.lock_state()?;
		if state.task_spill_at_base_end.replace(bytes).is_some() {
			return Err(profile_error(
				"base-accumulation spill boundary recorded more than once",
			));
		}
		Ok(())
	}

	pub(in crate::exec) fn record_reservoir_build_end_spill(
		&self,
		bytes: u64,
	) -> Result<(), ExecutionError> {
		let mut state = self.lock_state()?;
		if state
			.task_spill_at_reservoir_build_end
			.replace(bytes)
			.is_some()
		{
			return Err(profile_error(
				"reservoir-build spill boundary recorded more than once",
			));
		}
		Ok(())
	}

	pub(in crate::exec) fn record_reservoir_probe_end_spill(
		&self,
		bytes: u64,
	) -> Result<(), ExecutionError> {
		let mut state = self.lock_state()?;
		if state
			.task_spill_at_reservoir_probe_end
			.replace(bytes)
			.is_some()
		{
			return Err(profile_error(
				"reservoir-probe spill boundary recorded more than once",
			));
		}
		Ok(())
	}

	pub(in crate::exec) fn record_suffix_fold_end_spill(
		&self,
		bytes: u64,
	) -> Result<(), ExecutionError> {
		let mut state = self.lock_state()?;
		if state.task_spill_at_suffix_fold_end.replace(bytes).is_some() {
			return Err(profile_error(
				"suffix/fold spill boundary recorded more than once",
			));
		}
		Ok(())
	}

	pub(in crate::exec) fn record_reservoir_dispatch(
		&self,
		replay_id: JoinReplayId,
		partition_count: u32,
		group_count: u32,
	) -> Result<(), ExecutionError> {
		let mut state = self.lock_state()?;
		if state.reservoir_replay_id.replace(replay_id.0).is_some()
			|| state.partition_count.replace(partition_count).is_some()
			|| state.group_count.replace(group_count).is_some()
		{
			return Err(profile_error("reservoir dispatch recorded more than once"));
		}
		Ok(())
	}

	pub(in crate::exec) fn record_base(
		&self,
		batches: usize,
		rows: u64,
		bytes: u64,
	) -> Result<(), ExecutionError> {
		let mut state = self.lock_state()?;
		state.base_batches = checked_usize(batches, "base batch count")?;
		state.base_rows = rows;
		state.base_bytes = bytes;
		Ok(())
	}

	pub(in crate::exec) fn record_reservoir(
		&self,
		spill_bytes: u64,
		build_rows: u64,
		probe_rows: u64,
		output_rows: u64,
	) -> Result<(), ExecutionError> {
		let mut state = self.lock_state()?;
		if state.reservoir_spill_bytes.replace(spill_bytes).is_some()
			|| state.reservoir_build_rows.replace(build_rows).is_some()
			|| state.reservoir_probe_rows.replace(probe_rows).is_some()
			|| state.reservoir_output_rows.replace(output_rows).is_some()
		{
			return Err(profile_error("reservoir counters recorded more than once"));
		}
		Ok(())
	}

	pub(in crate::exec) fn record_fold_input_rows(
		&self,
		rows: usize,
	) -> Result<(), ExecutionError> {
		let mut state = self.lock_state()?;
		checked_add(
			&mut state.fold_input_rows,
			checked_usize(rows, "fold input rows")?,
			"fold input rows",
		)
	}

	pub(in crate::exec) fn record_final_group_rows(&self, rows: u64) -> Result<(), ExecutionError> {
		let mut state = self.lock_state()?;
		if state.final_group_rows.replace(rows).is_some() {
			return Err(profile_error("final group rows recorded more than once"));
		}
		Ok(())
	}

	pub(in crate::exec) fn record_runtime_stage(
		&self,
		replay_id: JoinReplayId,
		build_rows: u64,
		probe_rows: u64,
		output_rows: u64,
	) -> Result<(), ExecutionError> {
		let mut state = self.lock_state()?;
		if state
			.runtime_stages
			.iter()
			.any(|stage| stage.replay_id == replay_id.0)
		{
			return Err(profile_error(format!(
				"runtime stage {} recorded more than once",
				replay_id.0
			)));
		}
		state.runtime_stages.push(RuntimeStageSnapshot {
			replay_id: replay_id.0,
			build_rows,
			probe_rows,
			output_rows,
		});
		Ok(())
	}

	pub(in crate::exec) fn persist(&self) -> Result<(), ExecutionError> {
		let state = self.lock_state()?;
		if state.active_leaf.is_some() || state.active_sub_leaf.is_some() {
			return Err(profile_error(
				"cannot persist while an exclusive leaf or sub-leaf is active",
			));
		}
		let root_span_micros = required(state.root_span_micros, "root span")?;
		let task_spill_at_root_start =
			required(state.task_spill_at_root_start, "task spill at root start")?;
		let task_spill_at_runtime_prep_end = required(
			state.task_spill_at_runtime_prep_end,
			"task spill at runtime-preparation end",
		)?;
		let task_spill_at_base_end =
			required(state.task_spill_at_base_end, "task spill at base end")?;
		let task_spill_at_reservoir_build_end = required(
			state.task_spill_at_reservoir_build_end,
			"task spill at reservoir-build end",
		)?;
		let task_spill_at_reservoir_probe_end = required(
			state.task_spill_at_reservoir_probe_end,
			"task spill at reservoir-probe end",
		)?;
		let task_spill_at_suffix_fold_end = required(
			state.task_spill_at_suffix_fold_end,
			"task spill at suffix/fold end",
		)?;
		let task_spill_at_root_end =
			required(state.task_spill_at_root_end, "task spill at root end")?;
		let reservoir_spill_bytes = required(state.reservoir_spill_bytes, "reservoir spill bytes")?;
		let runtime_build_spill_bytes = task_spill_at_base_end
			.checked_sub(task_spill_at_root_start)
			.ok_or_else(|| profile_error("task spill at base end preceded root start"))?;
		let task_spill_delta = task_spill_at_root_end
			.checked_sub(task_spill_at_root_start)
			.ok_or_else(|| profile_error("task spill at root end preceded root start"))?;
		let spill_role_total = reservoir_spill_bytes
			.checked_add(runtime_build_spill_bytes)
			.ok_or_else(|| profile_error("spill-role total overflow"))?;
		if spill_role_total != task_spill_delta {
			return Err(profile_error(format!(
				"spill-role reconciliation failed: runtime_build={runtime_build_spill_bytes} reservoir={reservoir_spill_bytes} roles={spill_role_total} task_delta={task_spill_delta} checkpoints=root:{task_spill_at_root_start},runtime_prep:{task_spill_at_runtime_prep_end},base:{task_spill_at_base_end},reservoir_build:{task_spill_at_reservoir_build_end},reservoir_probe:{task_spill_at_reservoir_probe_end},suffix_fold:{task_spill_at_suffix_fold_end},root_end:{task_spill_at_root_end}"
			)));
		}
		let expected_after_reservoir = task_spill_at_base_end
			.checked_add(reservoir_spill_bytes)
			.ok_or_else(|| profile_error("base plus reservoir spill overflow"))?;
		if task_spill_at_runtime_prep_end > task_spill_at_base_end
			|| task_spill_at_reservoir_build_end != expected_after_reservoir
			|| task_spill_at_reservoir_probe_end != expected_after_reservoir
			|| task_spill_at_suffix_fold_end != expected_after_reservoir
			|| task_spill_at_root_end != expected_after_reservoir
		{
			return Err(profile_error(format!(
				"spill-boundary reconciliation failed: expected_after_reservoir={expected_after_reservoir} checkpoints=root:{task_spill_at_root_start},runtime_prep:{task_spill_at_runtime_prep_end},base:{task_spill_at_base_end},reservoir_build:{task_spill_at_reservoir_build_end},reservoir_probe:{task_spill_at_reservoir_probe_end},suffix_fold:{task_spill_at_suffix_fold_end},root_end:{task_spill_at_root_end}"
			)));
		}

		let leaf_total = checked_sum(&state.leaf_micros, "exclusive leaf micros")?;
		let residual = root_span_micros.checked_sub(leaf_total).ok_or_else(|| {
			profile_error(format!(
				"exclusive leaves ({leaf_total}) exceeded root span ({root_span_micros})"
			))
		})?;
		let residual_ratio = ratio(residual, root_span_micros);
		let leaves = GroupedChainProfileLeaf::ALL
			.into_iter()
			.map(|leaf| LeafSnapshot {
				name: leaf.name(),
				micros: state.leaf_micros[leaf.index()],
				calls: state.leaf_calls[leaf.index()],
				rows: state.leaf_rows[leaf.index()],
				bytes: state.leaf_bytes[leaf.index()],
				share: ratio(state.leaf_micros[leaf.index()], root_span_micros),
			})
			.collect();
		let mut sub_leaf_micros_by_index = [0u64; SUB_LEAF_COUNT];
		for leaf in GroupedChainSubLeaf::ALL {
			sub_leaf_micros_by_index[leaf.index()] =
				checked_nanos_to_micros(state.sub_leaf_nanos[leaf.index()], "sub-leaf duration")?;
		}
		let parent_refinement = |parent: GroupedChainProfileLeaf| {
			let measured = state.leaf_micros[parent.index()];
			let sub_leaf_micros = GroupedChainSubLeaf::ALL
				.into_iter()
				.filter(|leaf| leaf.parent() == parent)
				.try_fold(0u64, |total, leaf| {
					total
						.checked_add(sub_leaf_micros_by_index[leaf.index()])
						.ok_or_else(|| profile_error("sub-leaf micros overflow"))
				})?;
			let gap = measured.checked_sub(sub_leaf_micros).ok_or_else(|| {
				profile_error(format!(
					"sub-leaves ({sub_leaf_micros}) exceeded parent {} ({measured})",
					parent.name()
				))
			})?;
			let gap_ratio = ratio(gap, measured);
			Ok::<ParentRefinementSnapshot, ExecutionError>(ParentRefinementSnapshot {
				parent: parent.name(),
				measured_micros: measured,
				sub_leaf_micros,
				gap_micros: gap,
				gap_ratio,
				gap_within_ceiling: gap_ratio <= 0.10,
			})
		};
		let parent_refinements = vec![
			parent_refinement(GroupedChainProfileLeaf::ReservoirPartitionSpillWrite)?,
			parent_refinement(GroupedChainProfileLeaf::ReservoirReadProbe)?,
		];
		let sub_leaves = GroupedChainSubLeaf::ALL
			.into_iter()
			.map(|leaf| {
				let parent = leaf.parent();
				SubLeafSnapshot {
					name: leaf.name(),
					parent: parent.name(),
					micros: sub_leaf_micros_by_index[leaf.index()],
					calls: state.sub_leaf_calls[leaf.index()],
					rows: state.sub_leaf_rows[leaf.index()],
					bytes: state.sub_leaf_bytes[leaf.index()],
					pages: state.sub_leaf_pages[leaf.index()],
					share_of_root: ratio(sub_leaf_micros_by_index[leaf.index()], root_span_micros),
					share_of_parent: ratio(
						sub_leaf_micros_by_index[leaf.index()],
						state.leaf_micros[parent.index()],
					),
				}
			})
			.collect();
		let unique_ciphertext_pages = checked_usize(state.page_inventory.len(), "page inventory")?;
		let unique_ciphertext_bytes =
			state
				.page_inventory
				.values()
				.try_fold(0u64, |total, bytes| {
					total
						.checked_add(*bytes)
						.ok_or_else(|| profile_error("page inventory byte total overflow"))
				})?;
		if unique_ciphertext_bytes != reservoir_spill_bytes {
			return Err(profile_error(format!(
				"ciphertext page inventory mismatch: inventory={unique_ciphertext_bytes} reservoir={reservoir_spill_bytes}"
			)));
		}
		if state.physical_write_bytes != reservoir_spill_bytes {
			return Err(profile_error(format!(
				"physical write byte mismatch: writes={} reservoir={reservoir_spill_bytes}",
				state.physical_write_bytes
			)));
		}
		let snapshot = GroupedChainSnapshot {
			schema_version: PROFILE_SCHEMA_VERSION,
			profile_id: self.profile_id,
			path: "reoriented_inner_chain_grouped_partitioned_scan_reservoir",
			structural_stage_count: self.structural_stage_count,
			reservoir_replay_id: required(state.reservoir_replay_id, "reservoir replay id")?,
			partition_count: required(state.partition_count, "partition count")?,
			group_count: required(state.group_count, "group count")?,
			root_span_micros,
			leaves,
			sub_leaves,
			parent_refinements,
			in_root_residual_micros: residual,
			in_root_residual_ratio: residual_ratio,
			in_root_residual_within_ceiling: residual_ratio <= 0.10,
			task_spill_at_root_start,
			task_spill_at_runtime_prep_end,
			task_spill_at_base_end,
			task_spill_at_reservoir_build_end,
			task_spill_at_reservoir_probe_end,
			task_spill_at_suffix_fold_end,
			task_spill_at_root_end,
			task_spill_delta,
			runtime_build_spill_bytes,
			reservoir_spill_bytes,
			spill_roles_reconciled: true,
			base_batches: state.base_batches,
			base_rows: state.base_rows,
			base_bytes: state.base_bytes,
			reservoir_build_rows: required(state.reservoir_build_rows, "reservoir build rows")?,
			reservoir_probe_rows: required(state.reservoir_probe_rows, "reservoir probe rows")?,
			reservoir_output_rows: required(state.reservoir_output_rows, "reservoir output rows")?,
			fold_input_rows: state.fold_input_rows,
			final_group_rows: required(state.final_group_rows, "final group rows")?,
			runtime_stages: state.runtime_stages.clone(),
			branches: BranchSnapshot {
				resident_partitions: state.resident_partitions,
				streamed_partitions: state.streamed_partitions,
				pin_admitted: state.pin_admitted,
				pin_rejected_preflight: state.pin_rejected_preflight,
				pin_rejected_mid_decode: state.pin_rejected_mid_decode,
			},
			spill_io: SpillIoSnapshot {
				unique_ciphertext_pages,
				unique_ciphertext_bytes,
				physical_write_calls: state.physical_write_calls,
				physical_write_bytes: state.physical_write_bytes,
				physical_read_calls: state.physical_read_calls,
				physical_read_bytes: state.physical_read_bytes,
				plaintext_cache_hits: state.plaintext_cache_hits,
				plaintext_cache_misses: state.plaintext_cache_misses,
				decoded_cache_hits: state.decoded_cache_hits,
				decoded_cache_misses: state.decoded_cache_misses,
				decode_count: state.decode_count,
				read_amplification: ratio(state.physical_read_bytes, unique_ciphertext_bytes),
				inventory_reconciled: true,
			},
		};
		drop(state);
		write_snapshot(&self.directory, self.profile_id, &snapshot)
	}

	fn finish_leaf(
		&self,
		leaf: GroupedChainProfileLeaf,
		elapsed: Duration,
		rows: u64,
		bytes: u64,
	) -> Result<(), ExecutionError> {
		let elapsed = checked_duration_micros(elapsed)?;
		let mut state = self.lock_state()?;
		if state.active_leaf != Some(leaf) {
			return Err(profile_error(format!(
				"exclusive leaf {} finished while {:?} was active",
				leaf.name(),
				state.active_leaf
			)));
		}
		if state.active_sub_leaf.is_some() {
			return Err(profile_error(format!(
				"parent leaf {} finished while sub-leaf {:?} was active",
				leaf.name(),
				state.active_sub_leaf
			)));
		}
		let index = leaf.index();
		checked_add(&mut state.leaf_micros[index], elapsed, "leaf micros")?;
		checked_add(&mut state.leaf_calls[index], 1, "leaf calls")?;
		checked_add(&mut state.leaf_rows[index], rows, "leaf rows")?;
		checked_add(&mut state.leaf_bytes[index], bytes, "leaf bytes")?;
		state.active_leaf = None;
		Ok(())
	}

	fn finish_sub_leaf(
		&self,
		leaf: GroupedChainSubLeaf,
		started: Instant,
		rows: u64,
		bytes: u64,
		pages: u64,
	) -> Result<(), ExecutionError> {
		let mut state = self.lock_state()?;
		if state.active_leaf != Some(leaf.parent()) || state.active_sub_leaf != Some(leaf) {
			return Err(profile_error(format!(
				"sub-leaf {} finished outside parent {} or while {:?} was active",
				leaf.name(),
				leaf.parent().name(),
				state.active_sub_leaf
			)));
		}
		let index = leaf.index();
		checked_add(&mut state.sub_leaf_calls[index], 1, "sub-leaf calls")?;
		checked_add(&mut state.sub_leaf_rows[index], rows, "sub-leaf rows")?;
		checked_add(&mut state.sub_leaf_bytes[index], bytes, "sub-leaf bytes")?;
		checked_add(&mut state.sub_leaf_pages[index], pages, "sub-leaf pages")?;
		let elapsed_nanos = started.elapsed().as_nanos();
		state.sub_leaf_nanos[index] = state.sub_leaf_nanos[index]
			.checked_add(elapsed_nanos)
			.ok_or_else(|| profile_error("sub-leaf nanoseconds overflow"))?;
		state.active_sub_leaf = None;
		Ok(())
	}

	fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, ProfileState>, ExecutionError> {
		self.state
			.lock()
			.map_err(|_| profile_error("profile state mutex poisoned"))
	}

	#[cfg(test)]
	fn at_test_directory(
		directory: PathBuf,
		structural_stage_count: usize,
	) -> Result<Self, ExecutionError> {
		Self::from_lookup(structural_stage_count, |_| Some(directory.into_os_string()))?
			.ok_or_else(|| profile_error("test profile unexpectedly disabled"))
	}
}

impl GroupedChainProfileTimer<'_> {
	pub(in crate::exec) fn finish(self) -> Result<(), ExecutionError> {
		self.finish_with(0, 0)
	}

	pub(in crate::exec) fn finish_with(
		mut self,
		rows: u64,
		bytes: u64,
	) -> Result<(), ExecutionError> {
		self.profile
			.finish_leaf(self.leaf, self.started.elapsed(), rows, bytes)?;
		self.finished = true;
		Ok(())
	}
}

impl GroupedChainSpillObserver<'_> {
	pub(in crate::exec) fn begin(
		&self,
		leaf: GroupedChainSubLeaf,
	) -> Result<GroupedChainSubLeafTimer<'_>, ExecutionError> {
		self.profile.begin_sub_leaf(leaf)
	}

	pub(in crate::exec) fn record_written_page(
		&self,
		spill_id: u64,
		page_index: usize,
		ciphertext_len: usize,
	) -> Result<(), ExecutionError> {
		self.profile
			.record_written_page(spill_id, page_index, ciphertext_len)
	}

	pub(in crate::exec) fn record_physical_write(
		&self,
		bytes: usize,
	) -> Result<(), ExecutionError> {
		self.profile.record_physical_write(bytes)
	}

	pub(in crate::exec) fn record_physical_read(&self, bytes: usize) -> Result<(), ExecutionError> {
		self.profile.record_physical_read(bytes)
	}

	pub(in crate::exec) fn record_plaintext_cache(&self, hit: bool) -> Result<(), ExecutionError> {
		self.profile.record_cache_event(true, hit)
	}

	pub(in crate::exec) fn record_decoded_cache(&self, hit: bool) -> Result<(), ExecutionError> {
		self.profile.record_cache_event(false, hit)
	}

	pub(in crate::exec) fn record_decode(&self) -> Result<(), ExecutionError> {
		self.profile.record_decode()
	}

	pub(in crate::exec) fn record_branch(&self, resident: bool) -> Result<(), ExecutionError> {
		self.profile.record_branch(resident)
	}

	pub(in crate::exec) fn record_pin_outcome(
		&self,
		outcome: GroupedChainPinOutcome,
	) -> Result<(), ExecutionError> {
		self.profile.record_pin_outcome(outcome)
	}
}

impl GroupedChainSubLeafTimer<'_> {
	pub(in crate::exec) fn finish(self) -> Result<(), ExecutionError> {
		self.finish_with(0, 0, 0)
	}

	pub(in crate::exec) fn finish_with(
		mut self,
		rows: u64,
		bytes: u64,
		pages: u64,
	) -> Result<(), ExecutionError> {
		self.profile
			.finish_sub_leaf(self.leaf, self.started, rows, bytes, pages)?;
		self.finished = true;
		Ok(())
	}
}

impl Drop for GroupedChainProfileTimer<'_> {
	fn drop(&mut self) {
		if self.finished {
			return;
		}
		if let Ok(mut state) = self.profile.state.lock()
			&& state.active_leaf == Some(self.leaf)
		{
			state.active_sub_leaf = None;
			state.active_leaf = None;
		}
	}
}

impl Drop for GroupedChainSubLeafTimer<'_> {
	fn drop(&mut self) {
		if self.finished {
			return;
		}
		if let Ok(mut state) = self.profile.state.lock()
			&& state.active_sub_leaf == Some(self.leaf)
		{
			state.active_sub_leaf = None;
		}
	}
}

fn checked_add(counter: &mut u64, delta: u64, label: &str) -> Result<(), ExecutionError> {
	*counter = counter
		.checked_add(delta)
		.ok_or_else(|| profile_error(format!("{label} overflow")))?;
	Ok(())
}

fn checked_usize(value: usize, label: &str) -> Result<u64, ExecutionError> {
	u64::try_from(value).map_err(|_| profile_error(format!("{label} exceeds u64")))
}

fn checked_duration_micros(duration: Duration) -> Result<u64, ExecutionError> {
	u64::try_from(duration.as_micros())
		.map_err(|_| profile_error("profile duration exceeds u64 microseconds"))
}

fn checked_nanos_to_micros(nanos: u128, label: &str) -> Result<u64, ExecutionError> {
	u64::try_from(nanos / 1_000).map_err(|_| profile_error(format!("{label} exceeds u64 micros")))
}

fn checked_sum(values: &[u64], label: &str) -> Result<u64, ExecutionError> {
	values.iter().try_fold(0u64, |total, value| {
		total
			.checked_add(*value)
			.ok_or_else(|| profile_error(format!("{label} overflow")))
	})
}

fn required<T: Copy>(value: Option<T>, label: &str) -> Result<T, ExecutionError> {
	value.ok_or_else(|| profile_error(format!("missing {label}")))
}

fn ratio(value: u64, root: u64) -> f64 {
	if root == 0 {
		0.0
	} else {
		value as f64 / root as f64
	}
}

fn write_snapshot(
	directory: &Path,
	profile_id: u64,
	snapshot: &GroupedChainSnapshot,
) -> Result<(), ExecutionError> {
	std::fs::create_dir_all(directory)
		.map_err(|error| profile_error(format!("create profile directory: {error}")))?;
	let bytes = serde_json::to_vec_pretty(snapshot)
		.map_err(|error| profile_error(format!("serialize snapshot: {error}")))?;
	let final_path = directory.join(format!("grouped_chain_{profile_id:04}.json"));
	let temporary = directory.join(format!("grouped_chain_{profile_id:04}.tmp"));
	std::fs::write(&temporary, bytes)
		.map_err(|error| profile_error(format!("write temporary snapshot: {error}")))?;
	std::fs::rename(&temporary, final_path)
		.map_err(|error| profile_error(format!("publish snapshot: {error}")))
}

fn profile_error(message: impl Into<String>) -> ExecutionError {
	ExecutionError::Kernel {
		message: format!("grouped-chain profile: {}", message.into()),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn test_directory(label: &str) -> PathBuf {
		std::env::temp_dir().join(format!(
			"toron_grouped_chain_{label}_{}_{}",
			std::process::id(),
			NEXT_PROFILE_ID.fetch_add(1, Ordering::Relaxed)
		))
	}

	fn complete_profile(profile: &GroupedChainProfile) {
		let root = profile.begin_root(0).expect("begin root");
		for leaf in GroupedChainProfileLeaf::ALL {
			let parent = profile.begin_leaf(leaf).expect("begin leaf");
			for sub_leaf in GroupedChainSubLeaf::ALL
				.into_iter()
				.filter(|sub_leaf| sub_leaf.parent() == leaf)
			{
				profile
					.begin_sub_leaf(sub_leaf)
					.expect("begin sub-leaf")
					.finish_with(1, 2, 1)
					.expect("finish sub-leaf");
			}
			parent.finish_with(1, 2).expect("finish leaf");
		}
		let observer = profile.spill_observer();
		observer
			.record_written_page(7, 0, 20)
			.expect("written page");
		observer.record_physical_write(20).expect("write bytes");
		observer.record_physical_read(40).expect("read bytes");
		observer.record_plaintext_cache(false).expect("plain miss");
		observer.record_decoded_cache(false).expect("decoded miss");
		observer.record_decode().expect("decode");
		observer.record_branch(false).expect("streamed branch");
		observer
			.record_pin_outcome(GroupedChainPinOutcome::RejectedPreflight)
			.expect("pin outcome");
		profile
			.record_runtime_prep_end_spill(0)
			.expect("runtime-preparation spill boundary");
		profile
			.record_base_end_spill(10)
			.expect("base spill boundary");
		profile
			.record_reservoir_build_end_spill(30)
			.expect("reservoir-build spill boundary");
		profile
			.record_reservoir_probe_end_spill(30)
			.expect("reservoir-probe spill boundary");
		profile
			.record_suffix_fold_end_spill(30)
			.expect("suffix/fold spill boundary");
		profile
			.record_reservoir_dispatch(JoinReplayId(1), 8, 2)
			.expect("dispatch");
		profile.record_base(1, 3, 24).expect("base");
		profile.record_reservoir(20, 4, 3, 2).expect("reservoir");
		profile.record_fold_input_rows(2).expect("fold rows");
		profile.record_final_group_rows(1).expect("final rows");
		profile
			.record_runtime_stage(JoinReplayId(1), 4, 3, 2)
			.expect("runtime stage");
		profile.finish_root(root, 30).expect("finish root");
	}

	#[test]
	fn disabled_profile_constructs_no_state_or_sink() {
		assert!(
			GroupedChainProfile::from_lookup(2, |_| None)
				.expect("missing lookup")
				.is_none()
		);
		assert!(
			GroupedChainProfile::from_lookup(2, |_| Some(OsString::new()))
				.expect("empty lookup")
				.is_none()
		);
	}

	#[test]
	fn exact_leaf_spill_and_row_reconciliation_publishes_atomically() {
		let directory = test_directory("exact");
		let profile =
			GroupedChainProfile::at_test_directory(directory.clone(), 1).expect("test profile");
		complete_profile(&profile);
		profile.persist().expect("persist profile");
		let path = directory.join(format!("grouped_chain_{:04}.json", profile.profile_id));
		let value: serde_json::Value =
			serde_json::from_slice(&std::fs::read(path).expect("read snapshot"))
				.expect("parse snapshot");
		let object = value.as_object().expect("snapshot object");
		for schema_one_field in [
			"schema_version",
			"profile_id",
			"path",
			"structural_stage_count",
			"reservoir_replay_id",
			"partition_count",
			"group_count",
			"root_span_micros",
			"leaves",
			"in_root_residual_micros",
			"in_root_residual_ratio",
			"in_root_residual_within_ceiling",
			"task_spill_at_root_start",
			"task_spill_at_runtime_prep_end",
			"task_spill_at_base_end",
			"task_spill_at_reservoir_build_end",
			"task_spill_at_reservoir_probe_end",
			"task_spill_at_suffix_fold_end",
			"task_spill_at_root_end",
			"task_spill_delta",
			"runtime_build_spill_bytes",
			"reservoir_spill_bytes",
			"spill_roles_reconciled",
			"base_batches",
			"base_rows",
			"base_bytes",
			"reservoir_build_rows",
			"reservoir_probe_rows",
			"reservoir_output_rows",
			"fold_input_rows",
			"final_group_rows",
			"runtime_stages",
		] {
			assert!(
				object.contains_key(schema_one_field),
				"schema-2 snapshot dropped schema-1 field {schema_one_field}"
			);
		}
		assert!(object.contains_key("sub_leaves") && object.contains_key("spill_io"));
		assert_eq!(value["schema_version"], PROFILE_SCHEMA_VERSION);
		assert_eq!(value["task_spill_delta"], 30);
		assert_eq!(value["runtime_build_spill_bytes"], 10);
		assert_eq!(value["reservoir_spill_bytes"], 20);
		assert_eq!(value["base_rows"], 3);
		assert_eq!(value["reservoir_build_rows"], 4);
		assert_eq!(value["fold_input_rows"], 2);
		assert_eq!(value["final_group_rows"], 1);
		assert_eq!(value["leaves"].as_array().expect("leaves").len(), 9);
		assert_eq!(
			value["sub_leaves"].as_array().expect("sub leaves").len(),
			15
		);
		assert_eq!(
			value["parent_refinements"]
				.as_array()
				.expect("parent refinements")
				.len(),
			2
		);
		assert_eq!(value["spill_io"]["unique_ciphertext_bytes"], 20);
		assert_eq!(value["spill_io"]["physical_read_bytes"], 40);
		assert_eq!(value["spill_io"]["read_amplification"], 2.0);
		assert_eq!(value["branches"]["streamed_partitions"], 1);
		assert!(
			!directory
				.join(format!("grouped_chain_{:04}.tmp", profile.profile_id))
				.exists()
		);
		std::fs::remove_dir_all(directory).expect("remove test directory");
	}

	#[test]
	fn overlap_outside_root_and_spill_mismatch_fail_loudly() {
		let directory = test_directory("overlap");
		let profile =
			GroupedChainProfile::at_test_directory(directory.clone(), 1).expect("test profile");
		let outside = profile
			.begin_leaf(GroupedChainProfileLeaf::BaseAccumulation)
			.err()
			.expect("outside-root leaf must fail");
		assert!(outside.to_string().contains("outside the root span"));
		let root = profile.begin_root(0).expect("begin root");
		let leaf = profile
			.begin_leaf(GroupedChainProfileLeaf::RuntimePrepBuild)
			.expect("first leaf");
		let overlap = profile
			.begin_leaf(GroupedChainProfileLeaf::BaseAccumulation)
			.err()
			.expect("overlap must fail");
		assert!(overlap.to_string().contains("overlapped active leaf"));
		leaf.finish().expect("finish first leaf");
		let write = profile
			.begin_leaf(GroupedChainProfileLeaf::ReservoirPartitionSpillWrite)
			.expect("write parent");
		let observer = profile.spill_observer();
		let sub_leaf = observer
			.begin(GroupedChainSubLeaf::WriteIpcEncode)
			.expect("write sub-leaf");
		let sub_overlap = observer
			.begin(GroupedChainSubLeaf::WriteEncryption)
			.err()
			.expect("sub-leaf overlap must fail");
		assert!(
			sub_overlap
				.to_string()
				.contains("overlapped active sub-leaf")
		);
		sub_leaf.finish().expect("finish sub-leaf");
		let wrong_parent = observer
			.begin(GroupedChainSubLeaf::ReadIndexRebuildCpu)
			.err()
			.expect("read CPU timer must not run under write parent");
		assert!(wrong_parent.to_string().contains("started outside parent"));
		write.finish().expect("finish write parent");
		let outside_sub = observer
			.begin(GroupedChainSubLeaf::ReadLookup)
			.err()
			.expect("outside-parent sub-leaf must fail");
		assert!(outside_sub.to_string().contains("started outside parent"));
		profile.finish_root(root, 0).expect("finish root");
		std::fs::remove_dir_all(directory).ok();

		let directory = test_directory("nested_read_io");
		let profile =
			GroupedChainProfile::at_test_directory(directory.clone(), 1).expect("test profile");
		let root = profile.begin_root(0).expect("begin root");
		let read = profile
			.begin_leaf(GroupedChainProfileLeaf::ReservoirReadProbe)
			.expect("read parent");
		let observer = profile.spill_observer();
		let index_cpu = observer
			.begin(GroupedChainSubLeaf::ReadIndexRebuildCpu)
			.expect("index CPU");
		let nested_io = observer
			.begin(GroupedChainSubLeaf::ReadPhysicalRead)
			.err()
			.expect("I/O nested under index CPU must fail");
		assert!(nested_io.to_string().contains("overlapped active sub-leaf"));
		index_cpu.finish().expect("finish index CPU");
		let physical_read = observer
			.begin(GroupedChainSubLeaf::ReadPhysicalRead)
			.expect("physical read after pausing CPU");
		physical_read.finish().expect("finish physical read");
		read.finish().expect("finish read parent");
		profile.finish_root(root, 0).expect("finish root");
		std::fs::remove_dir_all(directory).ok();

		let directory = test_directory("duplicate_page");
		let profile =
			GroupedChainProfile::at_test_directory(directory.clone(), 1).expect("test profile");
		let observer = profile.spill_observer();
		observer
			.record_written_page(7, 3, 64)
			.expect("first page identity");
		let duplicate = observer
			.record_written_page(7, 3, 64)
			.expect_err("duplicate page identity must fail");
		assert!(duplicate.to_string().contains("duplicate ciphertext page"));
		std::fs::remove_dir_all(directory).ok();

		let directory = test_directory("spill_mismatch");
		let profile =
			GroupedChainProfile::at_test_directory(directory.clone(), 1).expect("test profile");
		complete_profile(&profile);
		{
			let mut state = profile.lock_state().expect("profile state");
			state.task_spill_at_root_end = Some(31);
		}
		let error = profile.persist().expect_err("spill mismatch must fail");
		assert!(
			error
				.to_string()
				.contains("spill-role reconciliation failed")
		);
		std::fs::remove_dir_all(directory).ok();

		let directory = test_directory("boundary_mismatch");
		let profile =
			GroupedChainProfile::at_test_directory(directory.clone(), 1).expect("test profile");
		complete_profile(&profile);
		{
			let mut state = profile.lock_state().expect("profile state");
			state.task_spill_at_reservoir_probe_end = Some(29);
		}
		let error = profile
			.persist()
			.expect_err("spill boundary mismatch must fail");
		assert!(
			error
				.to_string()
				.contains("spill-boundary reconciliation failed")
		);
		std::fs::remove_dir_all(directory).ok();
	}

	#[test]
	fn snapshot_failures_are_loud() {
		let parent = test_directory("not_directory");
		std::fs::write(&parent, b"not a directory").expect("write blocker");
		let profile =
			GroupedChainProfile::at_test_directory(parent.join("child"), 1).expect("test profile");
		complete_profile(&profile);
		let error = profile.persist().expect_err("publication must fail");
		assert!(error.to_string().contains("create profile directory"));
		std::fs::remove_file(parent).expect("remove blocker");
	}
}
