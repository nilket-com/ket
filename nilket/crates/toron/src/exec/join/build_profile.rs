use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use crate::distributed::task::{JoinBuildProfile, RowCount, TaskTelemetry};
use crate::exec::join::{BuildRowList, JoinKey, JoinKeyPlan};
use crate::ir::{JoinReplayId, JoinStrategy};

fn join_build_profile_enabled() -> bool {
	static ENABLED: OnceLock<bool> = OnceLock::new();
	*ENABLED.get_or_init(|| std::env::var_os("TORON_JOIN_BUILD_PROFILE").is_some())
}

pub(in crate::exec) fn record_join_build_profile(
	telemetry: &Option<Arc<Mutex<TaskTelemetry>>>,
	replay_id: JoinReplayId,
	strategy: JoinStrategy,
	key_plan: JoinKeyPlan,
	build_rows: &HashMap<JoinKey, BuildRowList>,
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
	};
	super::record_task_telemetry(telemetry, |task| {
		task.join_build_profiles.push(profile);
	});
}
