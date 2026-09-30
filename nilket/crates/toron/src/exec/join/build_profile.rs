use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

use crate::distributed::task::{
	ExistenceSummaryProfile, JoinBuildProfile, RowCount, TaskTelemetry,
};
use crate::exec::join::{BuildRowList, JoinBuildRows, JoinKeyPlan};
use crate::ir::{JoinReplayId, JoinStrategy};

fn join_build_profile_enabled() -> bool {
	static ENABLED: OnceLock<bool> = OnceLock::new();
	*ENABLED.get_or_init(|| {
		std::env::var_os("TORON_JOIN_BUILD_PROFILE").is_some()
			|| std::env::var_os("TORON_JOIN_BUILD_PROFILE_PATH").is_some()
	})
}

fn join_build_profile_path() -> Option<&'static PathBuf> {
	static PATH: OnceLock<Option<PathBuf>> = OnceLock::new();
	PATH.get_or_init(|| std::env::var_os("TORON_JOIN_BUILD_PROFILE_PATH").map(PathBuf::from))
		.as_ref()
}

fn join_build_profile_file_lock() -> &'static Mutex<()> {
	static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
	LOCK.get_or_init(|| Mutex::new(()))
}

fn existence_summary_profile_path() -> Option<&'static PathBuf> {
	static PATH: OnceLock<Option<PathBuf>> = OnceLock::new();
	PATH.get_or_init(|| std::env::var_os("TORON_EXISTENCE_SUMMARY_PROFILE_PATH").map(PathBuf::from))
		.as_ref()
}

fn existence_summary_profile_file_lock() -> &'static Mutex<()> {
	static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
	LOCK.get_or_init(|| Mutex::new(()))
}

fn record_existence_summary_profile_line(profile: &ExistenceSummaryProfile) {
	let Some(path) = existence_summary_profile_path() else {
		return;
	};
	let Ok(_guard) = existence_summary_profile_file_lock().lock() else {
		return;
	};
	let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) else {
		return;
	};
	if serde_json::to_writer(&mut file, profile).is_ok() {
		let _ = file.write_all(b"\n");
	}
}

fn record_join_build_profile_line(profile: &JoinBuildProfile) {
	let Some(path) = join_build_profile_path() else {
		return;
	};
	let Ok(_guard) = join_build_profile_file_lock().lock() else {
		return;
	};
	let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) else {
		return;
	};
	if serde_json::to_writer(&mut file, profile).is_ok() {
		let _ = file.write_all(b"\n");
	}
}

pub(in crate::exec) fn record_join_build_profile(
	telemetry: &Option<Arc<Mutex<TaskTelemetry>>>,
	replay_id: JoinReplayId,
	strategy: JoinStrategy,
	key_plan: JoinKeyPlan,
	build_rows: &JoinBuildRows,
	build_micros: u64,
) {
	if !join_build_profile_enabled() {
		return;
	}
	let mut duplicate_keys = 0u64;
	let mut overflow_rows = 0u64;
	let mut max_rows_per_key = 0u64;
	let mut overflow_heap_capacity_bytes = 0u64;
	let mut total_build_rows = 0u64;
	for rows in build_rows.values() {
		let row_count = rows.len() as u64;
		total_build_rows = total_build_rows.saturating_add(row_count);
		max_rows_per_key = max_rows_per_key.max(row_count);
		if row_count > 1 {
			duplicate_keys = duplicate_keys.saturating_add(1);
			overflow_rows = overflow_rows.saturating_add(row_count.saturating_sub(1));
		}
		overflow_heap_capacity_bytes = overflow_heap_capacity_bytes.saturating_add(
			u64::try_from(rows.heap_capacity_rows())
				.unwrap_or(u64::MAX)
				.saturating_mul(std::mem::size_of::<u32>() as u64),
		);
	}
	let profile = JoinBuildProfile {
		replay_id,
		strategy,
		key_plan: key_plan.label().to_string(),
		build_rows: RowCount::new(total_build_rows),
		unique_keys: RowCount::new(build_rows.len() as u64),
		duplicate_keys: RowCount::new(duplicate_keys),
		overflow_rows: RowCount::new(overflow_rows),
		max_rows_per_key: RowCount::new(max_rows_per_key),
		row_list_value_bytes: (build_rows.len() as u64)
			.saturating_mul(std::mem::size_of::<BuildRowList>() as u64),
		overflow_heap_capacity_bytes,
		build_micros,
		representation: build_rows.representation().to_string(),
		inline_eligible: build_rows.inline_eligible(),
	};
	record_join_build_profile_line(&profile);
	super::record_task_telemetry(telemetry, |task| {
		task.join_build_profiles.push(profile);
	});
}

#[allow(clippy::too_many_arguments)]
pub(in crate::exec) fn record_existence_summary_profile(
	telemetry: &Option<Arc<Mutex<TaskTelemetry>>>,
	replay_id: JoinReplayId,
	build_rows: u64,
	unique_keys: u64,
	multi_keys: u64,
	entry_capacity: usize,
	bucket_capacity: usize,
	accounted_bytes: u64,
	baseline_bytes: u64,
	peak_combined_bytes: u64,
	key_count_hint: Option<usize>,
	build_micros: u64,
) {
	let profile = ExistenceSummaryProfile {
		replay_id,
		build_rows: RowCount::new(build_rows),
		unique_keys: RowCount::new(unique_keys),
		multi_keys: RowCount::new(multi_keys),
		entry_capacity: u64::try_from(entry_capacity).unwrap_or(u64::MAX),
		bucket_capacity: u64::try_from(bucket_capacity).unwrap_or(u64::MAX),
		accounted_bytes,
		baseline_bytes,
		peak_combined_bytes,
		key_count_hint: key_count_hint.map(|value| u64::try_from(value).unwrap_or(u64::MAX)),
		page_reads: 0,
		build_micros,
	};
	record_existence_summary_profile_line(&profile);
	super::record_task_telemetry(telemetry, |task| {
		task.existence_summary_profiles.push(profile);
	});
}
