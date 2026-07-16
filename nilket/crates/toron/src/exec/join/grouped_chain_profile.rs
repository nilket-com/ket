use crate::distributed::task::TaskTelemetry;
use crate::error::ExecutionError;
use crate::ir::JoinReplayId;
use serde::Serialize;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub(in crate::exec) const GROUPED_CHAIN_PROFILE_ENV: &str = "TORON_GROUPED_CHAIN_PROFILE_DIR";
const PROFILE_SCHEMA_VERSION: u32 = 1;
const LEAF_COUNT: usize = 9;
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

#[derive(Debug, Serialize)]
struct LeafSnapshot {
	name: &'static str,
	micros: u64,
	calls: u64,
	rows: u64,
	bytes: u64,
	share: f64,
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
}

#[derive(Debug, Default)]
struct ProfileState {
	active_leaf: Option<GroupedChainProfileLeaf>,
	root_started: bool,
	root_span_micros: Option<u64>,
	leaf_micros: [u64; LEAF_COUNT],
	leaf_calls: [u64; LEAF_COUNT],
	leaf_rows: [u64; LEAF_COUNT],
	leaf_bytes: [u64; LEAF_COUNT],
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
		if state.active_leaf.is_some() {
			return Err(profile_error(
				"root span finished while an exclusive leaf was active",
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
		if state.active_leaf.is_some() {
			return Err(profile_error(
				"cannot persist while an exclusive leaf is active",
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
		let index = leaf.index();
		checked_add(&mut state.leaf_micros[index], elapsed, "leaf micros")?;
		checked_add(&mut state.leaf_calls[index], 1, "leaf calls")?;
		checked_add(&mut state.leaf_rows[index], rows, "leaf rows")?;
		checked_add(&mut state.leaf_bytes[index], bytes, "leaf bytes")?;
		state.active_leaf = None;
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

impl Drop for GroupedChainProfileTimer<'_> {
	fn drop(&mut self) {
		if self.finished {
			return;
		}
		if let Ok(mut state) = self.profile.state.lock()
			&& state.active_leaf == Some(self.leaf)
		{
			state.active_leaf = None;
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
			profile
				.begin_leaf(leaf)
				.expect("begin leaf")
				.finish_with(1, 2)
				.expect("finish leaf");
		}
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
		assert_eq!(value["schema_version"], PROFILE_SCHEMA_VERSION);
		assert_eq!(value["task_spill_delta"], 30);
		assert_eq!(value["runtime_build_spill_bytes"], 10);
		assert_eq!(value["reservoir_spill_bytes"], 20);
		assert_eq!(value["base_rows"], 3);
		assert_eq!(value["reservoir_build_rows"], 4);
		assert_eq!(value["fold_input_rows"], 2);
		assert_eq!(value["final_group_rows"], 1);
		assert_eq!(value["leaves"].as_array().expect("leaves").len(), 9);
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
		profile.finish_root(root, 0).expect("finish root");
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
