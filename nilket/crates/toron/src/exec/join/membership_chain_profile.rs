use crate::error::ExecutionError;
use serde::Serialize;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

pub(in crate::exec) const MEMBERSHIP_CHAIN_PROFILE_ENV: &str = "TORON_MEMBERSHIP_CHAIN_PROFILE_DIR";
const PROFILE_SCHEMA_VERSION: u32 = 1;
const PHASE_COUNT: usize = 9;
const OPERATION_COUNT: usize = 15;
const LEAF_COUNT: usize = PHASE_COUNT * OPERATION_COUNT;
const DOMAIN_COUNT: usize = 6;
const SPILL_ROLE_COUNT: usize = 4;
static NEXT_PROFILE_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::exec) enum MembershipChainPhase {
	Membership = 0,
	Lifecycle = 1,
	BaseBuild = 2,
	BaseProbeInput = 3,
	BaseJoin = 4,
	Intermediate = 5,
	TerminalBuild = 6,
	TerminalJoin = 7,
	Sink = 8,
}

impl MembershipChainPhase {
	const ALL: [Self; PHASE_COUNT] = [
		Self::Membership,
		Self::Lifecycle,
		Self::BaseBuild,
		Self::BaseProbeInput,
		Self::BaseJoin,
		Self::Intermediate,
		Self::TerminalBuild,
		Self::TerminalJoin,
		Self::Sink,
	];

	fn name(self) -> &'static str {
		match self {
			Self::Membership => "membership",
			Self::Lifecycle => "lifecycle",
			Self::BaseBuild => "base_build",
			Self::BaseProbeInput => "base_probe_input",
			Self::BaseJoin => "base_join",
			Self::Intermediate => "intermediate",
			Self::TerminalBuild => "terminal_build",
			Self::TerminalJoin => "terminal_join",
			Self::Sink => "sink",
		}
	}
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::exec) enum MembershipChainOperation {
	SubtreeExecute = 0,
	PresenceSetBuild = 1,
	RuntimePrepare = 2,
	StorageScanDecode = 3,
	PartitionSpillWrite = 4,
	SpillReadDecode = 5,
	IndexBuild = 6,
	LookupSelection = 7,
	ArrowMaterialize = 8,
	PostReadMaterialize = 9,
	PresenceFilter = 10,
	Exchange = 11,
	GroupedFold = 12,
	Finalize = 13,
	Cleanup = 14,
}

impl MembershipChainOperation {
	const ALL: [Self; OPERATION_COUNT] = [
		Self::SubtreeExecute,
		Self::PresenceSetBuild,
		Self::RuntimePrepare,
		Self::StorageScanDecode,
		Self::PartitionSpillWrite,
		Self::SpillReadDecode,
		Self::IndexBuild,
		Self::LookupSelection,
		Self::ArrowMaterialize,
		Self::PostReadMaterialize,
		Self::PresenceFilter,
		Self::Exchange,
		Self::GroupedFold,
		Self::Finalize,
		Self::Cleanup,
	];

	fn name(self) -> &'static str {
		match self {
			Self::SubtreeExecute => "subtree_execute",
			Self::PresenceSetBuild => "presence_set_build",
			Self::RuntimePrepare => "runtime_prepare",
			Self::StorageScanDecode => "storage_scan_decode",
			Self::PartitionSpillWrite => "partition_spill_write",
			Self::SpillReadDecode => "spill_read_decode",
			Self::IndexBuild => "index_build",
			Self::LookupSelection => "lookup_selection",
			Self::ArrowMaterialize => "arrow_materialize",
			Self::PostReadMaterialize => "post_read_materialize",
			Self::PresenceFilter => "presence_filter",
			Self::Exchange => "exchange",
			Self::GroupedFold => "grouped_fold",
			Self::Finalize => "finalize",
			Self::Cleanup => "cleanup",
		}
	}
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::exec) enum MembershipSpillRole {
	BaseBuild = 0,
	BaseProbeInput = 1,
	Intermediate = 2,
	TerminalBuild = 3,
}

impl MembershipSpillRole {
	const ALL: [Self; SPILL_ROLE_COUNT] = [
		Self::BaseBuild,
		Self::BaseProbeInput,
		Self::Intermediate,
		Self::TerminalBuild,
	];

	fn name(self) -> &'static str {
		match self {
			Self::BaseBuild => "base_build",
			Self::BaseProbeInput => "base_probe_input",
			Self::Intermediate => "intermediate",
			Self::TerminalBuild => "terminal_build",
		}
	}
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ActiveLeaf {
	phase: MembershipChainPhase,
	operation: MembershipChainOperation,
}

impl ActiveLeaf {
	fn index(self) -> usize {
		(self.phase as usize) * OPERATION_COUNT + self.operation as usize
	}

	fn label(self) -> String {
		format!("{}.{}", self.phase.name(), self.operation.name())
	}
}

#[derive(Debug)]
struct ProfileState {
	root_started: bool,
	root_span_micros: Option<u64>,
	active_leaf: Option<ActiveLeaf>,
	leaf_micros: [u64; LEAF_COUNT],
	leaf_calls: [u64; LEAF_COUNT],
	leaf_rows: [u64; LEAF_COUNT],
	leaf_bytes: [u64; LEAF_COUNT],
	spill_bytes: [u64; SPILL_ROLE_COUNT],
	engine_spill_bytes: Option<u64>,
	presence_entries: u64,
	sink_input_rows: u64,
	sink_matching_rows: u64,
	root_aggregate_rows: u64,
	base_replay_id: Option<u32>,
	terminal_replay_id: Option<u32>,
	base_partition_count: Option<u32>,
	base_group_count: Option<u32>,
	terminal_partition_count: Option<u32>,
	terminal_group_count: Option<u32>,
	base_probe_columns_first: Option<bool>,
	terminal_probe_columns_first: Option<bool>,
}

impl Default for ProfileState {
	fn default() -> Self {
		Self {
			root_started: false,
			root_span_micros: None,
			active_leaf: None,
			leaf_micros: [0; LEAF_COUNT],
			leaf_calls: [0; LEAF_COUNT],
			leaf_rows: [0; LEAF_COUNT],
			leaf_bytes: [0; LEAF_COUNT],
			spill_bytes: [0; SPILL_ROLE_COUNT],
			engine_spill_bytes: None,
			presence_entries: 0,
			sink_input_rows: 0,
			sink_matching_rows: 0,
			root_aggregate_rows: 0,
			base_replay_id: None,
			terminal_replay_id: None,
			base_partition_count: None,
			base_group_count: None,
			terminal_partition_count: None,
			terminal_group_count: None,
			base_probe_columns_first: None,
			terminal_probe_columns_first: None,
		}
	}
}

#[derive(Debug)]
pub(in crate::exec) struct MembershipChainProfile {
	directory: PathBuf,
	profile_id: u64,
	state: Mutex<ProfileState>,
}

#[derive(Debug)]
pub(in crate::exec) struct MembershipChainTimer<'a> {
	profile: &'a MembershipChainProfile,
	leaf: ActiveLeaf,
	started: Instant,
	finished: bool,
}

#[derive(Debug, Serialize)]
struct LeafSnapshot {
	phase: &'static str,
	operation: &'static str,
	micros: u64,
	calls: u64,
	rows: u64,
	bytes: u64,
}

#[derive(Debug, Serialize)]
struct DomainSnapshot {
	name: &'static str,
	micros: u64,
	share: f64,
}

#[derive(Debug, Serialize)]
struct SpillSnapshot {
	role: &'static str,
	bytes: u64,
}

#[derive(Debug, Serialize)]
struct MembershipChainSnapshot {
	schema_version: u32,
	profile_id: u64,
	root_span_micros: u64,
	exclusive_leaf_micros: u64,
	in_root_residual_micros: u64,
	in_root_residual_ratio: f64,
	in_root_residual_within_ceiling: bool,
	leaves: Vec<LeafSnapshot>,
	domains: Vec<DomainSnapshot>,
	spill_roles: Vec<SpillSnapshot>,
	spill_role_total_bytes: u64,
	engine_spill_bytes: u64,
	presence_entries: u64,
	sink_input_rows: u64,
	sink_matching_rows: u64,
	root_aggregate_rows: u64,
	base_replay_id: u32,
	terminal_replay_id: u32,
	base_partition_count: u32,
	base_group_count: u32,
	terminal_partition_count: u32,
	terminal_group_count: u32,
	base_probe_columns_first: bool,
	terminal_probe_columns_first: bool,
}

impl MembershipChainProfile {
	pub(in crate::exec) fn from_env() -> Result<Option<Box<Self>>, ExecutionError> {
		Self::from_lookup(|name| std::env::var_os(name)).map(|value| value.map(Box::new))
	}

	fn from_lookup(
		lookup: impl FnOnce(&str) -> Option<OsString>,
	) -> Result<Option<Self>, ExecutionError> {
		let Some(raw) = lookup(MEMBERSHIP_CHAIN_PROFILE_ENV) else {
			return Ok(None);
		};
		if raw.is_empty() {
			return Ok(None);
		}
		Ok(Some(Self {
			directory: PathBuf::from(raw),
			profile_id: NEXT_PROFILE_ID.fetch_add(1, Ordering::Relaxed),
			state: Mutex::new(ProfileState::default()),
		}))
	}

	pub(in crate::exec) fn begin_root(&self) -> Result<Instant, ExecutionError> {
		let mut state = self.lock_state()?;
		if state.root_started || state.root_span_micros.is_some() {
			return Err(profile_error("root span started more than once"));
		}
		state.root_started = true;
		Ok(Instant::now())
	}

	pub(in crate::exec) fn finish_root(&self, started: Instant) -> Result<(), ExecutionError> {
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
		Ok(())
	}

	pub(in crate::exec) fn begin_leaf(
		&self,
		phase: MembershipChainPhase,
		operation: MembershipChainOperation,
	) -> Result<MembershipChainTimer<'_>, ExecutionError> {
		let leaf = ActiveLeaf { phase, operation };
		let mut state = self.lock_state()?;
		if !state.root_started || state.root_span_micros.is_some() {
			return Err(profile_error(format!(
				"exclusive leaf {} started outside the root span",
				leaf.label()
			)));
		}
		if let Some(active) = state.active_leaf {
			return Err(profile_error(format!(
				"exclusive leaf {} overlapped active leaf {}",
				leaf.label(),
				active.label()
			)));
		}
		state.active_leaf = Some(leaf);
		drop(state);
		Ok(MembershipChainTimer {
			profile: self,
			leaf,
			started: Instant::now(),
			finished: false,
		})
	}

	#[allow(clippy::too_many_arguments)]
	pub(in crate::exec) fn record_dispatch(
		&self,
		base_replay_id: u32,
		terminal_replay_id: u32,
		base_partition_count: u32,
		base_group_count: u32,
		terminal_partition_count: u32,
		terminal_group_count: u32,
		base_probe_columns_first: bool,
		terminal_probe_columns_first: bool,
	) -> Result<(), ExecutionError> {
		let mut state = self.lock_state()?;
		if state.base_replay_id.is_some() {
			return Err(profile_error(
				"membership-chain dispatch recorded more than once",
			));
		}
		state.base_replay_id = Some(base_replay_id);
		state.terminal_replay_id = Some(terminal_replay_id);
		state.base_partition_count = Some(base_partition_count);
		state.base_group_count = Some(base_group_count);
		state.terminal_partition_count = Some(terminal_partition_count);
		state.terminal_group_count = Some(terminal_group_count);
		state.base_probe_columns_first = Some(base_probe_columns_first);
		state.terminal_probe_columns_first = Some(terminal_probe_columns_first);
		Ok(())
	}

	pub(in crate::exec) fn record_presence_entries(
		&self,
		rows: usize,
	) -> Result<(), ExecutionError> {
		let mut state = self.lock_state()?;
		state.presence_entries = checked_usize(rows, "presence entries")?;
		Ok(())
	}

	pub(in crate::exec) fn record_sink_rows(
		&self,
		input_rows: usize,
		matching_rows: usize,
	) -> Result<(), ExecutionError> {
		let mut state = self.lock_state()?;
		checked_add_usize(&mut state.sink_input_rows, input_rows, "sink input rows")?;
		checked_add_usize(
			&mut state.sink_matching_rows,
			matching_rows,
			"sink matching rows",
		)
	}

	pub(in crate::exec) fn record_root_aggregate_rows(
		&self,
		rows: usize,
	) -> Result<(), ExecutionError> {
		let mut state = self.lock_state()?;
		state.root_aggregate_rows = checked_usize(rows, "root aggregate rows")?;
		Ok(())
	}

	pub(in crate::exec) fn record_spill_bytes(
		&self,
		role: MembershipSpillRole,
		bytes: u64,
	) -> Result<(), ExecutionError> {
		let mut state = self.lock_state()?;
		let counter = &mut state.spill_bytes[role as usize];
		*counter = counter
			.checked_add(bytes)
			.ok_or_else(|| profile_error(format!("{} spill bytes overflow", role.name())))?;
		Ok(())
	}

	pub(in crate::exec) fn record_engine_spill_bytes(
		&self,
		bytes: u64,
	) -> Result<(), ExecutionError> {
		let mut state = self.lock_state()?;
		if state.engine_spill_bytes.replace(bytes).is_some() {
			return Err(profile_error("engine spill total recorded more than once"));
		}
		Ok(())
	}

	pub(in crate::exec) fn persist(&self) -> Result<(), ExecutionError> {
		let state = self.lock_state()?;
		if state.active_leaf.is_some() {
			return Err(profile_error(
				"cannot persist while an exclusive leaf is active",
			));
		}
		let root = state
			.root_span_micros
			.ok_or_else(|| profile_error("cannot persist before root completion"))?;
		let leaf_total = checked_sum(&state.leaf_micros, "exclusive leaf micros")?;
		let residual = root.checked_sub(leaf_total).ok_or_else(|| {
			profile_error(format!(
				"exclusive leaves ({leaf_total}) exceeded root span ({root})"
			))
		})?;
		let residual_ratio = ratio(residual, root);
		let engine_spill_bytes = state
			.engine_spill_bytes
			.ok_or_else(|| profile_error("engine spill total was not recorded"))?;
		let spill_role_total = checked_sum(&state.spill_bytes, "spill-role bytes")?;
		if spill_role_total != engine_spill_bytes {
			return Err(profile_error(format!(
				"spill-role reconciliation failed: roles={spill_role_total} engine={engine_spill_bytes}"
			)));
		}
		if state.presence_entries != state.root_aggregate_rows {
			return Err(profile_error(format!(
				"presence/root reconciliation failed: presence={} root={}",
				state.presence_entries, state.root_aggregate_rows
			)));
		}
		if state.sink_matching_rows > state.sink_input_rows {
			return Err(profile_error(format!(
				"sink matches {} exceeded input {}",
				state.sink_matching_rows, state.sink_input_rows
			)));
		}

		let mut leaves = Vec::with_capacity(LEAF_COUNT);
		for phase in MembershipChainPhase::ALL {
			for operation in MembershipChainOperation::ALL {
				let leaf = ActiveLeaf { phase, operation };
				leaves.push(LeafSnapshot {
					phase: phase.name(),
					operation: operation.name(),
					micros: state.leaf_micros[leaf.index()],
					calls: state.leaf_calls[leaf.index()],
					rows: state.leaf_rows[leaf.index()],
					bytes: state.leaf_bytes[leaf.index()],
				});
			}
		}
		let domain_micros = rollup_domains(&state.leaf_micros)?;
		let domain_names = [
			"membership",
			"scan_spill",
			"index_probe",
			"materialization",
			"sink_fold",
			"lifecycle",
		];
		let domains = domain_names
			.into_iter()
			.enumerate()
			.map(|(index, name)| DomainSnapshot {
				name,
				micros: domain_micros[index],
				share: ratio(domain_micros[index], root),
			})
			.collect();
		let spill_roles = MembershipSpillRole::ALL
			.into_iter()
			.map(|role| SpillSnapshot {
				role: role.name(),
				bytes: state.spill_bytes[role as usize],
			})
			.collect();
		let required = |value: Option<u32>, label: &str| {
			value.ok_or_else(|| profile_error(format!("{label} was not recorded")))
		};
		let required_bool = |value: Option<bool>, label: &str| {
			value.ok_or_else(|| profile_error(format!("{label} was not recorded")))
		};
		let snapshot = MembershipChainSnapshot {
			schema_version: PROFILE_SCHEMA_VERSION,
			profile_id: self.profile_id,
			root_span_micros: root,
			exclusive_leaf_micros: leaf_total,
			in_root_residual_micros: residual,
			in_root_residual_ratio: residual_ratio,
			in_root_residual_within_ceiling: residual_ratio <= 0.10,
			leaves,
			domains,
			spill_roles,
			spill_role_total_bytes: spill_role_total,
			engine_spill_bytes,
			presence_entries: state.presence_entries,
			sink_input_rows: state.sink_input_rows,
			sink_matching_rows: state.sink_matching_rows,
			root_aggregate_rows: state.root_aggregate_rows,
			base_replay_id: required(state.base_replay_id, "base replay id")?,
			terminal_replay_id: required(state.terminal_replay_id, "terminal replay id")?,
			base_partition_count: required(state.base_partition_count, "base partition count")?,
			base_group_count: required(state.base_group_count, "base group count")?,
			terminal_partition_count: required(
				state.terminal_partition_count,
				"terminal partition count",
			)?,
			terminal_group_count: required(state.terminal_group_count, "terminal group count")?,
			base_probe_columns_first: required_bool(
				state.base_probe_columns_first,
				"base probe orientation",
			)?,
			terminal_probe_columns_first: required_bool(
				state.terminal_probe_columns_first,
				"terminal probe orientation",
			)?,
		};
		drop(state);
		write_snapshot(&self.directory, self.profile_id, &snapshot)
	}

	fn finish_leaf(
		&self,
		leaf: ActiveLeaf,
		elapsed: Duration,
		rows: u64,
		bytes: u64,
	) -> Result<(), ExecutionError> {
		let elapsed = checked_duration_micros(elapsed)?;
		let mut state = self.lock_state()?;
		if state.active_leaf != Some(leaf) {
			return Err(profile_error(format!(
				"exclusive leaf {} finished while {:?} was active",
				leaf.label(),
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
	fn at_test_directory(directory: PathBuf) -> Result<Self, ExecutionError> {
		Self::from_lookup(|_| Some(directory.into_os_string()))?
			.ok_or_else(|| profile_error("test profile unexpectedly disabled"))
	}
}

impl MembershipChainTimer<'_> {
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

impl Drop for MembershipChainTimer<'_> {
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

fn rollup_domains(leaves: &[u64; LEAF_COUNT]) -> Result<[u64; DOMAIN_COUNT], ExecutionError> {
	let mut domains = [0u64; DOMAIN_COUNT];
	for phase in MembershipChainPhase::ALL {
		for operation in MembershipChainOperation::ALL {
			let leaf = ActiveLeaf { phase, operation };
			let domain = match operation {
				MembershipChainOperation::SubtreeExecute
				| MembershipChainOperation::PresenceSetBuild => 0,
				MembershipChainOperation::StorageScanDecode
				| MembershipChainOperation::PartitionSpillWrite
				| MembershipChainOperation::SpillReadDecode => 1,
				MembershipChainOperation::IndexBuild
				| MembershipChainOperation::LookupSelection => 2,
				MembershipChainOperation::ArrowMaterialize
				| MembershipChainOperation::PostReadMaterialize => 3,
				MembershipChainOperation::PresenceFilter
				| MembershipChainOperation::Exchange
				| MembershipChainOperation::GroupedFold => 4,
				MembershipChainOperation::RuntimePrepare
				| MembershipChainOperation::Finalize
				| MembershipChainOperation::Cleanup => 5,
			};
			checked_add(&mut domains[domain], leaves[leaf.index()], "domain micros")?;
		}
	}
	Ok(domains)
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

fn checked_add_usize(counter: &mut u64, value: usize, label: &str) -> Result<(), ExecutionError> {
	checked_add(counter, checked_usize(value, label)?, label)
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
	snapshot: &MembershipChainSnapshot,
) -> Result<(), ExecutionError> {
	std::fs::create_dir_all(directory)
		.map_err(|error| profile_error(format!("create profile directory: {error}")))?;
	let bytes = serde_json::to_vec_pretty(snapshot)
		.map_err(|error| profile_error(format!("serialize snapshot: {error}")))?;
	let final_path = directory.join(format!("membership_chain_{profile_id:04}.json"));
	let temporary = directory.join(format!("membership_chain_{profile_id:04}.tmp"));
	std::fs::write(&temporary, bytes)
		.map_err(|error| profile_error(format!("write temporary snapshot: {error}")))?;
	std::fs::rename(&temporary, final_path)
		.map_err(|error| profile_error(format!("publish snapshot: {error}")))
}

fn profile_error(message: impl Into<String>) -> ExecutionError {
	ExecutionError::Kernel {
		message: format!("membership-chain profile: {}", message.into()),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn test_directory(label: &str) -> PathBuf {
		std::env::temp_dir().join(format!(
			"toron_membership_chain_{label}_{}_{}",
			std::process::id(),
			NEXT_PROFILE_ID.fetch_add(1, Ordering::Relaxed)
		))
	}

	fn complete_profile(profile: &MembershipChainProfile) {
		let root = profile.begin_root().expect("begin root");
		profile
			.record_dispatch(0, 1, 8, 2, 8, 2, false, true)
			.expect("dispatch");
		profile.record_presence_entries(2).expect("presence");
		let leaf = profile
			.begin_leaf(
				MembershipChainPhase::Membership,
				MembershipChainOperation::SubtreeExecute,
			)
			.expect("begin leaf");
		leaf.finish_with(2, 16).expect("finish leaf");
		profile.record_sink_rows(3, 1).expect("sink rows");
		profile.record_root_aggregate_rows(2).expect("root rows");
		profile
			.record_spill_bytes(MembershipSpillRole::BaseBuild, 10)
			.expect("base spill");
		profile
			.record_spill_bytes(MembershipSpillRole::BaseProbeInput, 20)
			.expect("probe spill");
		profile
			.record_spill_bytes(MembershipSpillRole::Intermediate, 30)
			.expect("intermediate spill");
		profile
			.record_spill_bytes(MembershipSpillRole::TerminalBuild, 40)
			.expect("terminal spill");
		profile
			.record_engine_spill_bytes(100)
			.expect("engine spill");
		profile.finish_root(root).expect("finish root");
	}

	#[test]
	fn disabled_profile_constructs_no_state_or_sink() {
		assert!(
			MembershipChainProfile::from_lookup(|_| None)
				.expect("missing lookup")
				.is_none()
		);
		assert!(
			MembershipChainProfile::from_lookup(|_| Some(OsString::new()))
				.expect("empty lookup")
				.is_none()
		);
	}

	#[test]
	fn exact_matrix_reconciliation_and_atomic_publication() {
		let directory = test_directory("exact");
		let profile =
			MembershipChainProfile::at_test_directory(directory.clone()).expect("test profile");
		complete_profile(&profile);
		profile.persist().expect("persist profile");
		let path = directory.join(format!("membership_chain_{:04}.json", profile.profile_id));
		let value: serde_json::Value =
			serde_json::from_slice(&std::fs::read(&path).expect("read snapshot"))
				.expect("parse snapshot");
		assert_eq!(value["schema_version"], PROFILE_SCHEMA_VERSION);
		assert_eq!(value["spill_role_total_bytes"], 100);
		assert_eq!(value["presence_entries"], 2);
		assert_eq!(value["root_aggregate_rows"], 2);
		assert!(
			!directory
				.join(format!("membership_chain_{:04}.tmp", profile.profile_id))
				.exists()
		);
		std::fs::remove_dir_all(directory).expect("remove test directory");
	}

	#[test]
	fn overlapping_leaf_and_spill_mismatch_fail_loudly() {
		let directory = test_directory("failures");
		let profile =
			MembershipChainProfile::at_test_directory(directory.clone()).expect("test profile");
		let root = profile.begin_root().expect("begin root");
		let leaf = profile
			.begin_leaf(
				MembershipChainPhase::BaseJoin,
				MembershipChainOperation::LookupSelection,
			)
			.expect("first leaf");
		let overlap = profile
			.begin_leaf(
				MembershipChainPhase::BaseJoin,
				MembershipChainOperation::ArrowMaterialize,
			)
			.expect_err("overlap must fail");
		assert!(overlap.to_string().contains("overlapped active leaf"));
		leaf.finish().expect("finish first leaf");
		profile.finish_root(root).expect("finish root");
		std::fs::remove_dir_all(directory).ok();

		let directory = test_directory("spill_mismatch");
		let profile =
			MembershipChainProfile::at_test_directory(directory.clone()).expect("test profile");
		let outside = profile
			.begin_leaf(
				MembershipChainPhase::BaseBuild,
				MembershipChainOperation::StorageScanDecode,
			)
			.err()
			.expect("leaf outside root must fail");
		assert!(outside.to_string().contains("outside the root span"));
		let root = profile.begin_root().expect("begin mismatch root");
		profile
			.record_dispatch(0, 1, 2, 2, 64, 22, false, true)
			.expect("dispatch");
		profile.record_presence_entries(1).expect("presence");
		profile.record_root_aggregate_rows(1).expect("root rows");
		profile
			.record_spill_bytes(MembershipSpillRole::BaseBuild, 10)
			.expect("role spill");
		profile.record_engine_spill_bytes(11).expect("engine spill");
		profile.finish_root(root).expect("finish mismatch root");
		let mismatch = profile.persist().expect_err("spill mismatch must fail");
		assert!(
			mismatch
				.to_string()
				.contains("spill-role reconciliation failed")
		);
		std::fs::remove_dir_all(directory).ok();
	}

	#[test]
	fn snapshot_failures_are_loud() {
		let parent = test_directory("not_directory");
		std::fs::write(&parent, b"not a directory").expect("write blocker");
		let profile =
			MembershipChainProfile::at_test_directory(parent.join("child")).expect("test profile");
		complete_profile(&profile);
		let error = profile.persist().expect_err("publication must fail");
		assert!(error.to_string().contains("create profile directory"));
		std::fs::remove_file(parent).expect("remove blocker");
	}
}
