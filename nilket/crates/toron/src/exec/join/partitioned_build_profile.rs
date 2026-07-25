use super::{
	JOIN_SPILL_RETAINED_FOOTPRINT_MEMORY_CAP_MULTIPLIER, SpilledInnerJoinResidencySnapshot,
	join_spill_retained_footprint_memory_cap,
};
use crate::error::ExecutionError;
use std::ops::Range;

/// Invocation-local proof that the final indexes of one active partition group can overlap the
/// writer high-water without changing the production memory coordinate.
///
/// The profile is constructed only when node attribution is active. Snapshots are taken from the
/// production owners; no row-count or key-width estimate enters the verdict.
#[derive(Debug)]
pub(super) struct PartitionedBuildResidencyProfile {
	partition_count: u32,
	group_count: u32,
	memory_per_task_bytes: u64,
	spill_bytes_reserved: u64,
	retained_footprint_cap_bytes: u64,
	executor_task_resident_baseline_bytes: u64,
	groups: Vec<serde_json::Value>,
	minimum_headroom_bytes: Option<u64>,
	minimum_task_budget_headroom_bytes: Option<u64>,
}

impl PartitionedBuildResidencyProfile {
	pub(super) fn new(
		partition_count: u32,
		group_count: u32,
		memory_per_task_bytes: u64,
		spill_bytes_reserved: u64,
		executor_task_resident_baseline_bytes: u64,
	) -> Result<Self, ExecutionError> {
		let checked_cap = memory_per_task_bytes
			.checked_mul(JOIN_SPILL_RETAINED_FOOTPRINT_MEMORY_CAP_MULTIPLIER)
			.ok_or_else(|| profile_error("retained-footprint cap overflowed"))?;
		let production_cap = join_spill_retained_footprint_memory_cap(memory_per_task_bytes);
		if checked_cap != production_cap {
			return Err(profile_error(
				"checked retained-footprint cap disagrees with production accounting",
			));
		}
		Ok(Self {
			partition_count,
			group_count,
			memory_per_task_bytes,
			spill_bytes_reserved,
			retained_footprint_cap_bytes: checked_cap,
			executor_task_resident_baseline_bytes,
			groups: Vec::new(),
			minimum_headroom_bytes: None,
			minimum_task_budget_headroom_bytes: None,
		})
	}

	pub(super) fn record_group(
		&mut self,
		group: u32,
		range: Range<usize>,
		writer_collection_capacity_bytes: u64,
		before_release: Vec<(usize, SpilledInnerJoinResidencySnapshot)>,
		after_rebuild: Vec<(usize, SpilledInnerJoinResidencySnapshot)>,
	) -> Result<(), ExecutionError> {
		if range.start > range.end || range.end > self.partition_count as usize {
			return Err(profile_error(
				"partition group range is outside partition count",
			));
		}
		let expected = range.end - range.start;
		if before_release.len() != expected || after_rebuild.len() != expected {
			return Err(profile_error(
				"partition group snapshots omit an active partition",
			));
		}

		let mut per_partition = Vec::with_capacity(expected);
		let mut final_index_bytes = 0u64;
		let mut summed_writer_peak_bytes = 0u64;
		let mut summed_writer_capacity_bytes = 0u64;
		let mut summed_page_reserve_bytes = 0u64;
		let mut summed_cache_cap_bytes = 0u64;
		let mut summed_cache_current_bytes = 0u64;
		let mut summed_cache_peak_bytes = 0u64;
		let mut summed_metadata_bytes = 0u64;
		let mut production_guard_after_rebuild_bytes = writer_collection_capacity_bytes;
		let mut production_guard_candidate_overlap_bytes = writer_collection_capacity_bytes;
		let mut complete_owner_after_rebuild_bytes = writer_collection_capacity_bytes;
		let mut complete_owner_candidate_overlap_bytes = writer_collection_capacity_bytes;

		for partition in range.clone() {
			let before = snapshot_for(partition, &before_release)?;
			let after = snapshot_for(partition, &after_rebuild)?;
			final_index_bytes = add(
				final_index_bytes,
				after.resident_index_bytes,
				"final resident index sum",
			)?;
			summed_writer_peak_bytes = add(
				summed_writer_peak_bytes,
				before.writer_scratch_peak_bytes,
				"writer high-water sum",
			)?;
			summed_writer_capacity_bytes = add(
				summed_writer_capacity_bytes,
				required(
					before.write_scratch_capacity_bytes,
					"writer scratch capacity",
				)?,
				"writer scratch capacity sum",
			)?;
			summed_page_reserve_bytes = add(
				summed_page_reserve_bytes,
				before.page_reserve_bytes,
				"page reserve sum",
			)?;
			summed_cache_cap_bytes = add(
				summed_cache_cap_bytes,
				after.cache_cap_bytes,
				"cache cap sum",
			)?;
			summed_cache_current_bytes = add(
				summed_cache_current_bytes,
				after.cache_payload_bytes,
				"cache current sum",
			)?;
			summed_cache_peak_bytes = add(
				summed_cache_peak_bytes,
				after.cache_peak_bytes,
				"cache peak sum",
			)?;
			let metadata = metadata_bytes(after)?;
			summed_metadata_bytes = add(summed_metadata_bytes, metadata, "metadata sum")?;

			let guard_after = required(after.guard_accounted_bytes, "guard-accounted bytes")?;
			production_guard_after_rebuild_bytes = add(
				production_guard_after_rebuild_bytes,
				guard_after,
				"production guard after rebuild",
			)?;
			production_guard_candidate_overlap_bytes = add(
				production_guard_candidate_overlap_bytes,
				add(
					guard_after,
					before.writer_scratch_peak_bytes,
					"per-partition production overlap",
				)?,
				"production candidate overlap",
			)?;

			let owner_after = required(after.current_resident_bytes, "current resident ownership")?;
			complete_owner_after_rebuild_bytes = add(
				complete_owner_after_rebuild_bytes,
				add(
					add(
						owner_after,
						after.resident_baseline_guard_bytes,
						"owner plus resident baseline",
					)?,
					after.page_reserve_bytes,
					"owner plus page reserve",
				)?,
				"complete owner after rebuild",
			)?;
			let candidate_partition = add(
				add(
					add(
						owner_after,
						before.resident_baseline_guard_bytes,
						"candidate owner plus resident baseline",
					)?,
					before.page_reserve_bytes,
					"candidate owner plus page reserve",
				)?,
				before.writer_scratch_peak_bytes,
				"candidate owner plus writer high-water",
			)?;
			complete_owner_candidate_overlap_bytes = add(
				complete_owner_candidate_overlap_bytes,
				candidate_partition,
				"complete owner candidate overlap",
			)?;

			per_partition.push(serde_json::json!({
				"partition": partition,
				"before_scratch_release": before,
				"after_index_rebuild": after,
				"candidate_guard_overlap_bytes": guard_after
					.checked_add(before.writer_scratch_peak_bytes),
				"candidate_complete_owner_overlap_bytes": candidate_partition,
			}));
		}

		let headroom = self
			.retained_footprint_cap_bytes
			.checked_sub(complete_owner_candidate_overlap_bytes);
		let task_budget_headroom = self
			.memory_per_task_bytes
			.checked_sub(complete_owner_candidate_overlap_bytes);
		if let Some(headroom) = headroom {
			self.minimum_headroom_bytes = Some(
				self.minimum_headroom_bytes
					.map_or(headroom, |current| current.min(headroom)),
			);
		}
		if let Some(headroom) = task_budget_headroom {
			self.minimum_task_budget_headroom_bytes = Some(
				self.minimum_task_budget_headroom_bytes
					.map_or(headroom, |current| current.min(headroom)),
			);
		}
		self.groups.push(serde_json::json!({
			"group": group,
			"partition_range": {
				"start": range.start,
				"end": range.end,
			},
			"active_partition_count": expected,
			"writer_collection_capacity_bytes": writer_collection_capacity_bytes,
			"final_summed_resident_index_bytes": final_index_bytes,
			"summed_writer_scratch_peak_bytes": summed_writer_peak_bytes,
			"summed_writer_scratch_capacity_bytes": summed_writer_capacity_bytes,
			"summed_page_reserve_bytes": summed_page_reserve_bytes,
			"summed_cache_cap_bytes": summed_cache_cap_bytes,
			"summed_cache_current_bytes": summed_cache_current_bytes,
			"summed_cache_peak_bytes": summed_cache_peak_bytes,
			"summed_metadata_bytes": summed_metadata_bytes,
			"production_guard_after_rebuild_bytes": production_guard_after_rebuild_bytes,
			"production_guard_candidate_overlap_bytes":
				production_guard_candidate_overlap_bytes,
			"complete_owner_after_rebuild_bytes": complete_owner_after_rebuild_bytes,
			"complete_owner_candidate_overlap_bytes":
				complete_owner_candidate_overlap_bytes,
			"headroom_bytes": headroom,
			"task_budget_headroom_bytes": task_budget_headroom,
			"fits_retained_footprint_cap": headroom.is_some(),
			"fits_task_memory_budget": task_budget_headroom.is_some(),
			"per_partition": per_partition,
		}));
		Ok(())
	}

	pub(super) fn json(&self) -> serde_json::Value {
		let all_groups_recorded = self.groups.len() == self.group_count as usize;
		let all_groups_fit = all_groups_recorded
			&& self.groups.iter().all(|group| {
				group["fits_retained_footprint_cap"]
					.as_bool()
					.unwrap_or(false)
					&& group["fits_task_memory_budget"].as_bool().unwrap_or(false)
			});
		serde_json::json!({
			"accounting": "production_owner_components_with_index_writer_overlap",
			"partition_count": self.partition_count,
			"group_count": self.group_count,
			"groups_recorded": self.groups.len(),
			"all_groups_recorded": all_groups_recorded,
			"memory_per_task_bytes": self.memory_per_task_bytes,
			"spill_bytes_reserved": self.spill_bytes_reserved,
			"retained_footprint_multiplier":
				JOIN_SPILL_RETAINED_FOOTPRINT_MEMORY_CAP_MULTIPLIER,
			"retained_footprint_cap_bytes": self.retained_footprint_cap_bytes,
			"executor_task_resident_baseline_bytes":
				self.executor_task_resident_baseline_bytes,
			"minimum_headroom_bytes": self.minimum_headroom_bytes,
			"minimum_task_budget_headroom_bytes":
				self.minimum_task_budget_headroom_bytes,
			"passed": all_groups_fit,
			"groups": self.groups,
		})
	}
}

fn snapshot_for(
	partition: usize,
	snapshots: &[(usize, SpilledInnerJoinResidencySnapshot)],
) -> Result<&SpilledInnerJoinResidencySnapshot, ExecutionError> {
	snapshots
		.iter()
		.find_map(|(id, snapshot)| (*id == partition).then_some(snapshot))
		.ok_or_else(|| profile_error("partition identity is missing from residency snapshot"))
}

fn metadata_bytes(snapshot: &SpilledInnerJoinResidencySnapshot) -> Result<u64, ExecutionError> {
	[
		snapshot.page_metadata_bytes,
		snapshot.cache_metadata_bytes,
		snapshot.pinned_handle_capacity_bytes,
		snapshot.retained_legacy_index_bytes,
		snapshot.path_capacity_bytes,
	]
	.into_iter()
	.try_fold(0u64, |total, value| {
		add(
			total,
			required(value, "resident metadata component")?,
			"metadata",
		)
	})
}

fn required(value: Option<u64>, label: &'static str) -> Result<u64, ExecutionError> {
	value.ok_or_else(|| profile_error(label))
}

fn add(left: u64, right: u64, label: &'static str) -> Result<u64, ExecutionError> {
	left.checked_add(right).ok_or_else(|| profile_error(label))
}

fn profile_error(message: &'static str) -> ExecutionError {
	ExecutionError::Kernel {
		message: format!("partitioned build residency profile: {message}"),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn snapshot(index: u64, writer: u64, cache: u64) -> SpilledInnerJoinResidencySnapshot {
		let current = index + cache + 31;
		SpilledInnerJoinResidencySnapshot {
			resident_baseline_guard_bytes: 3,
			resident_index_bytes: index,
			writer_scratch_peak_bytes: writer,
			page_reserve_bytes: 5,
			cache_payload_bytes: cache,
			cache_cap_bytes: 8,
			cache_peak_bytes: cache,
			pinned_decoded_bytes: 0,
			page_metadata_bytes: Some(7),
			cache_metadata_bytes: Some(11),
			pinned_handle_capacity_bytes: Some(0),
			write_scratch_capacity_bytes: Some(writer),
			retained_legacy_index_bytes: Some(0),
			path_capacity_bytes: Some(13),
			open_file_handles: 1,
			input_rows: 1,
			spill_disk_bytes: 17,
			current_resident_bytes: Some(current),
			current_resident_error: None,
			guard_accounted_bytes: Some(3 + index + writer + 5 + cache),
			guard_accounted_error: None,
			overlap_label: "test",
		}
	}

	#[test]
	fn group_sums_each_partition_instead_of_pairing_independent_maxima() {
		let mut profile =
			PartitionedBuildResidencyProfile::new(2, 1, 1_000, 100, 0).expect("profile");
		profile
			.record_group(
				0,
				0..2,
				0,
				vec![(0, snapshot(1, 40, 7)), (1, snapshot(1, 2, 7))],
				vec![(0, snapshot(3, 0, 7)), (1, snapshot(50, 0, 7))],
			)
			.expect("group");
		let value = profile.json();
		assert_eq!(value["groups"][0]["final_summed_resident_index_bytes"], 53);
		assert_eq!(value["groups"][0]["summed_writer_scratch_peak_bytes"], 42);
		assert_eq!(
			value["groups"][0]["summed_cache_current_bytes"], 14,
			"two per-object caches may exceed either object's 8-byte cap"
		);
		assert_eq!(value["passed"], true);
	}

	#[test]
	fn empty_partitions_are_required_and_missing_components_fail_closed() {
		let mut empty = snapshot(0, 0, 0);
		empty.input_rows = 0;
		let mut profile =
			PartitionedBuildResidencyProfile::new(2, 1, 100, 100, 0).expect("profile");
		profile
			.record_group(
				0,
				0..2,
				0,
				vec![(0, empty.clone()), (1, snapshot(1, 1, 0))],
				vec![(0, empty.clone()), (1, snapshot(1, 0, 0))],
			)
			.expect("empty partition is still present");

		let mut missing = empty;
		missing.page_metadata_bytes = None;
		let error = profile
			.record_group(0, 0..1, 0, vec![(0, missing.clone())], vec![(0, missing)])
			.expect_err("missing ownership component");
		assert!(error.to_string().contains("resident metadata component"));
	}

	#[test]
	fn overlap_can_fail_when_the_released_state_fits() {
		let mut profile = PartitionedBuildResidencyProfile::new(1, 1, 10, 10, 0).expect("profile");
		profile
			.record_group(
				0,
				0..1,
				0,
				vec![(0, snapshot(0, 100, 0))],
				vec![(0, snapshot(40, 0, 0))],
			)
			.expect("arithmetic");
		let group = &profile.json()["groups"][0];
		assert!(
			group["complete_owner_after_rebuild_bytes"]
				.as_u64()
				.expect("after")
				< 160
		);
		assert_eq!(group["fits_retained_footprint_cap"], false);
		assert_eq!(profile.json()["passed"], false);
	}

	#[test]
	fn overflow_and_partition_omission_fail_closed() {
		assert!(
			PartitionedBuildResidencyProfile::new(1, 1, u64::MAX, 1, 0)
				.expect_err("cap overflow")
				.to_string()
				.contains("overflowed")
		);
		let mut profile =
			PartitionedBuildResidencyProfile::new(2, 1, 100, 100, 0).expect("profile");
		assert!(
			profile
				.record_group(0, 0..2, 0, vec![], vec![])
				.expect_err("omitted partitions")
				.to_string()
				.contains("omit")
		);
	}
}
