//! ADR 583: replayable, sealable Toron/DuckDB performance boards.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use crate::ALL_QUERY_IDS;

pub const PERF_BOARD_SUITE: &str = "tpch_perf_board";
pub const PERF_BOARD_ARTIFACT: &str = "perf_board.jsonl";
pub const PERF_BOARD_EVIDENCE: &str = "perf_board.jsonl.evidence";
pub const PERF_BOARD_RUNS: usize = 5;
pub const PERF_BOARD_WARMUP: usize = 1;
pub const PERF_BOARD_MEMORY_BYTES: u64 = 2 * 1024 * 1024 * 1024;
pub const PERF_BOARD_SPILL_BYTES: u64 = 2 * 1024 * 1024 * 1024;
pub const PERF_BOARD_ENGINES: [&str; 2] = ["toron", "duckdb"];

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PerfBoardDecision {
	pub candidate_authority: bool,
	pub reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PerfBoardComparison {
	pub comparable: bool,
	pub reasons: Vec<String>,
	pub baseline_source_tree_id: Option<String>,
	pub candidate_source_tree_id: Option<String>,
	pub baseline_harness_sha256: Option<String>,
	pub candidate_harness_sha256: Option<String>,
	pub baseline_duckdb_sha256: Option<String>,
	pub candidate_duckdb_sha256: Option<String>,
}

impl PerfBoardDecision {
	fn from_reasons(mut reasons: Vec<String>) -> Self {
		reasons.sort();
		reasons.dedup();
		Self {
			candidate_authority: reasons.is_empty(),
			reasons,
		}
	}
}

/// Replay the complete board. The caller may put the resulting decision in the header only after
/// this function has inspected the exact rows that will be published.
pub fn evaluate_perf_board(
	header: &serde_json::Value,
	samples: &[serde_json::Value],
	summaries: &[serde_json::Value],
) -> PerfBoardDecision {
	let mut reasons = Vec::new();
	for reason in header
		.get("content_identity_reasons")
		.and_then(serde_json::Value::as_array)
		.into_iter()
		.flatten()
		.filter_map(serde_json::Value::as_str)
	{
		reasons.push(reason.to_string());
	}
	require_header_string(header, "suite", PERF_BOARD_SUITE, &mut reasons);
	require_header_string(header, "mode", "strict", &mut reasons);
	require_header_string(header, "data_source", "parquet", &mut reasons);
	require_header_string(header, "profile", "release", &mut reasons);
	require_header_string(header, "dataset_verification", "full_hash", &mut reasons);
	require_header_string(
		header,
		"memory_per_task_provenance",
		"explicit",
		&mut reasons,
	);
	require_header_string(
		header,
		"spill_bytes_reserved_provenance",
		"explicit",
		&mut reasons,
	);
	require_header_string(
		header,
		"partition_work_concurrency_requested_provenance",
		"explicit",
		&mut reasons,
	);
	require_header_string(header, "duckdb_threads", "1", &mut reasons);
	require_header_string(
		header,
		"duckdb_threads_provenance",
		"explicit",
		&mut reasons,
	);
	// ADR 603: the anchor runs DuckDB under its own default memory limit; a capped artifact is a
	// labelled panel, never the anchor.
	require_header_string(
		header,
		"duckdb_memory_limit_provenance",
		"default",
		&mut reasons,
	);
	if header.get("duckdb_memory_limit") != Some(&serde_json::Value::Null) {
		reasons.push("coordinate_mismatch:duckdb_memory_limit:expected_null".to_string());
	}
	// ADR 591: an explicit affinity, honoured by the header's own thread; rows are bound to it
	// below, so a row cannot vouch for itself with a matching pair of foreign values.
	require_header_string(
		header,
		"cpu_affinity_requested_provenance",
		"explicit",
		&mut reasons,
	);
	let affinity_requested = header
		.get("cpu_affinity_requested")
		.and_then(serde_json::Value::as_str)
		.filter(|list| !list.is_empty());
	if affinity_requested.is_none() {
		reasons.push("coordinate_missing:cpu_affinity_requested".to_string());
	} else if header.get("cpu_affinity_observed") != header.get("cpu_affinity_requested") {
		reasons.push("cpu_affinity_mismatch:header:observed".to_string());
	}
	require_header_u64(header, "runs", PERF_BOARD_RUNS as u64, &mut reasons);
	require_header_u64(header, "warmup", PERF_BOARD_WARMUP as u64, &mut reasons);
	require_header_u64(
		header,
		"memory_per_task",
		PERF_BOARD_MEMORY_BYTES,
		&mut reasons,
	);
	require_header_u64(
		header,
		"spill_bytes_reserved",
		PERF_BOARD_SPILL_BYTES,
		&mut reasons,
	);
	require_header_u64(
		header,
		"partition_work_concurrency_requested",
		1,
		&mut reasons,
	);
	if header
		.get("scale_factor")
		.and_then(serde_json::Value::as_f64)
		!= Some(8.0)
	{
		reasons.push("coordinate_mismatch:scale_factor:expected_8".to_string());
	}
	if header.get("queries") != Some(&serde_json::json!(ALL_QUERY_IDS)) {
		reasons.push("coordinate_mismatch:queries:expected_q01_through_q22".to_string());
	}
	if header.get("engines") != Some(&serde_json::json!(PERF_BOARD_ENGINES)) {
		reasons.push("coordinate_mismatch:engines:expected_toron_then_duckdb".to_string());
	}
	for field in [
		"dataset_manifest_fingerprint",
		"duckdb_executable_sha256",
		"duckdb_version",
	] {
		if header
			.get(field)
			.and_then(serde_json::Value::as_str)
			.is_none_or(str::is_empty)
		{
			reasons.push(format!("coordinate_missing:{field}"));
		}
	}
	if !header
		.get("duckdb_version")
		.and_then(serde_json::Value::as_str)
		.is_some_and(|version| version.contains("v1.5.4") || version.starts_with("1.5.4"))
	{
		reasons.push("coordinate_mismatch:duckdb_version:expected_1.5.4".to_string());
	}
	for field in [
		"diagnostics_disabled",
		"host_start_quiescent",
		"host_end_quiescent",
		"join_spill_scratch_empty_before",
		"join_spill_scratch_empty_after",
		"output_directory_fresh",
	] {
		if header.get(field).and_then(serde_json::Value::as_bool) != Some(true) {
			reasons.push(format!("authority_condition_false:{field}"));
		}
	}

	let expected_samples =
		ALL_QUERY_IDS.len() * PERF_BOARD_ENGINES.len() * (PERF_BOARD_RUNS + PERF_BOARD_WARMUP);
	if samples.len() != expected_samples {
		reasons.push(format!(
			"sample_count_mismatch:expected_{expected_samples}:observed_{}",
			samples.len()
		));
	}
	let expected_fingerprint = header
		.get("dataset_manifest_fingerprint")
		.and_then(serde_json::Value::as_str);
	let expected_duckdb_hash = header
		.get("duckdb_executable_sha256")
		.and_then(serde_json::Value::as_str);
	let expected_identity = header.get("evidence_identity");
	if expected_identity
		.and_then(|identity| identity.get("duckdb_sha256"))
		.and_then(serde_json::Value::as_str)
		!= expected_duckdb_hash
	{
		reasons.push("duckdb_executable_disagrees_with_build_manifest".to_string());
	}
	let mut seen = BTreeSet::new();
	let mut expected_order = Vec::with_capacity(expected_samples);
	for query in ALL_QUERY_IDS {
		for engine in PERF_BOARD_ENGINES {
			expected_order.push((query, engine, true, 0u64));
			for run in 0..PERF_BOARD_RUNS as u64 {
				expected_order.push((query, engine, false, run));
			}
		}
	}
	for row in samples {
		let Some(query) = row.get("query").and_then(serde_json::Value::as_str) else {
			continue;
		};
		let Some(engine) = row.get("engine").and_then(serde_json::Value::as_str) else {
			continue;
		};
		let Some(warmup) = row.get("warmup").and_then(serde_json::Value::as_bool) else {
			continue;
		};
		let Some(run) = row.get("run_index").and_then(serde_json::Value::as_u64) else {
			continue;
		};
		if !seen.insert((query.to_string(), engine.to_string(), warmup, run)) {
			reasons.push(format!("duplicate_sample:{query}:{engine}:{warmup}:{run}"));
		}
	}
	for (query, engine, warmup, run) in &expected_order {
		if !seen.contains(&(query.to_string(), engine.to_string(), *warmup, *run)) {
			reasons.push(format!(
				"missing_sample:{query}:{engine}:{}:{run}",
				if *warmup { "warmup" } else { "measured" }
			));
		}
	}
	for (position, expected) in expected_order.iter().enumerate() {
		let Some(row) = samples.get(position) else {
			reasons.push(format!(
				"missing_sample:{}:{}:{}:{}",
				expected.0,
				expected.1,
				if expected.2 { "warmup" } else { "measured" },
				expected.3
			));
			continue;
		};
		let observed = (
			row.get("query").and_then(serde_json::Value::as_str),
			row.get("engine").and_then(serde_json::Value::as_str),
			row.get("warmup").and_then(serde_json::Value::as_bool),
			row.get("run_index").and_then(serde_json::Value::as_u64),
		);
		if observed
			!= (
				Some(expected.0),
				Some(expected.1),
				Some(expected.2),
				Some(expected.3),
			) {
			reasons.push(format!("sample_order_mismatch:position_{position}"));
		}
		if row.get("status").and_then(serde_json::Value::as_str) != Some("pass") {
			reasons.push(format!("sample_not_passing:{}:{}", expected.0, expected.1));
		}
		let sample_flags = row
			.get("flags")
			.and_then(serde_json::Value::as_array)
			.into_iter()
			.flatten()
			.filter_map(serde_json::Value::as_str)
			.collect::<Vec<_>>();
		for flag in ["row_count_mismatch"] {
			if sample_flags.contains(&flag) {
				reasons.push(format!(
					"sample_invalid:{}:{}:{flag}",
					expected.0, expected.1
				));
			}
		}
		if row.get("evidence_identity") != expected_identity {
			reasons.push(format!(
				"row_harness_identity_mismatch:{}:{}",
				expected.0, expected.1
			));
		}
		for (field, expected_value) in [
			("scale_factor", header.get("scale_factor")),
			("mode", header.get("mode")),
			("data_source", header.get("data_source")),
			("profile", header.get("profile")),
			(
				"dataset_manifest_fingerprint",
				header.get("dataset_manifest_fingerprint"),
			),
			("memory_per_task", header.get("memory_per_task")),
			(
				"memory_per_task_provenance",
				header.get("memory_per_task_provenance"),
			),
			("spill_bytes_reserved", header.get("spill_bytes_reserved")),
			(
				"spill_bytes_reserved_provenance",
				header.get("spill_bytes_reserved_provenance"),
			),
			(
				"partition_work_concurrency_requested",
				header.get("partition_work_concurrency_requested"),
			),
			(
				"partition_work_concurrency_requested_provenance",
				header.get("partition_work_concurrency_requested_provenance"),
			),
		] {
			if expected_value.is_none() || row.get(field) != expected_value {
				reasons.push(format!(
					"row_coordinate_mismatch:{}:{}:{field}",
					expected.0, expected.1
				));
			}
		}
		if row
			.get("dataset_verification")
			.and_then(serde_json::Value::as_str)
			!= Some("full_hash")
		{
			reasons.push(format!(
				"row_dataset_not_full_hash:{}:{}",
				expected.0, expected.1
			));
		}
		let row_requested = row
			.get("cpu_affinity_requested")
			.and_then(serde_json::Value::as_str);
		if row_requested.is_none() || row_requested != affinity_requested {
			reasons.push(format!(
				"cpu_affinity_mismatch:{}:{}:requested",
				expected.0, expected.1
			));
		}
		if row.get("cpu_affinity_observed") != row.get("cpu_affinity_requested") {
			reasons.push(format!(
				"cpu_affinity_mismatch:{}:{}:observed",
				expected.0, expected.1
			));
		}
		if row.get("cpu_affinity_requested_provenance")
			!= header.get("cpu_affinity_requested_provenance")
		{
			reasons.push(format!(
				"cpu_affinity_mismatch:{}:{}:provenance",
				expected.0, expected.1
			));
		}
		if expected.1 == "duckdb" {
			if row
				.get("duckdb_threads")
				.and_then(serde_json::Value::as_str)
				!= Some("1")
			{
				reasons.push(format!("duckdb_thread_pin_mismatch:{}", expected.0));
			}
			if row
				.get("duckdb_executable_sha256")
				.and_then(serde_json::Value::as_str)
				!= expected_duckdb_hash
			{
				reasons.push(format!("duckdb_executable_mismatch:{}", expected.0));
			}
			if row.get("duckdb_version") != header.get("duckdb_version") {
				reasons.push(format!("duckdb_version_mismatch:{}", expected.0));
			}
			// ADR 603: a DuckDB row is bound to the header's memory coordinate.
			for field in ["duckdb_memory_limit", "duckdb_memory_limit_provenance"] {
				if header.get(field).is_none() || row.get(field) != header.get(field) {
					reasons.push(format!(
						"row_coordinate_mismatch:{}:duckdb:{field}",
						expected.0
					));
				}
			}
		} else if let Some(observations) = row
			.get("partition_work_concurrency_observations")
			.and_then(serde_json::Value::as_array)
		{
			if observations.iter().any(|observation| {
				observation
					.get("planned_k")
					.and_then(serde_json::Value::as_u64)
					.is_some_and(|value| value > 1)
			}) {
				reasons.push(format!(
					"partition_work_concurrency_exceeded:{}",
					expected.0
				));
			}
		}
		if expected_fingerprint.is_none() {
			reasons.push("coordinate_missing:dataset_manifest_fingerprint".to_string());
		}
	}

	let mut summaries_by_key = BTreeMap::new();
	for row in summaries {
		let Some(engine) = row.get("engine").and_then(serde_json::Value::as_str) else {
			reasons.push("summary_missing:engine".to_string());
			continue;
		};
		let Some(query) = row.get("query").and_then(serde_json::Value::as_str) else {
			reasons.push("summary_missing:query".to_string());
			continue;
		};
		if summaries_by_key
			.insert((query.to_string(), engine.to_string()), row)
			.is_some()
		{
			reasons.push(format!("duplicate_summary:{query}:{engine}"));
		}
	}
	for query in ALL_QUERY_IDS {
		for engine in PERF_BOARD_ENGINES {
			let expected_position = ALL_QUERY_IDS
				.iter()
				.position(|candidate| *candidate == query)
				.expect("query is canonical")
				* PERF_BOARD_ENGINES.len()
				+ PERF_BOARD_ENGINES
					.iter()
					.position(|candidate| *candidate == engine)
					.expect("engine is canonical");
			if summaries.get(expected_position).is_none_or(|row| {
				row.get("query").and_then(serde_json::Value::as_str) != Some(query)
					|| row.get("engine").and_then(serde_json::Value::as_str) != Some(engine)
			}) {
				reasons.push(format!(
					"summary_order_mismatch:position_{expected_position}"
				));
			}
			let Some(row) = summaries_by_key.get(&(query.to_string(), engine.to_string())) else {
				reasons.push(format!("missing_summary:{query}:{engine}"));
				continue;
			};
			if row.get("status").and_then(serde_json::Value::as_str) != Some("pass") {
				reasons.push(format!("cell_not_passing:{query}:{engine}"));
			}
			let flags = row
				.get("flags")
				.and_then(serde_json::Value::as_array)
				.into_iter()
				.flatten()
				.filter_map(serde_json::Value::as_str)
				.collect::<Vec<_>>();
			if flags.contains(&"unstable") {
				reasons.push(format!("cell_unstable:{query}:{engine}"));
			}
			for flag in [
				"dataset_verification_changed",
				"debug_build",
				"row_count_mismatch",
			] {
				if flags.contains(&flag) {
					reasons.push(format!("cell_invalid:{query}:{engine}:{flag}"));
				}
			}
			if row.get("runs").and_then(serde_json::Value::as_u64) != Some(PERF_BOARD_RUNS as u64)
				|| row.get("warmup").and_then(serde_json::Value::as_u64)
					!= Some(PERF_BOARD_WARMUP as u64)
			{
				reasons.push(format!("cell_sample_contract_mismatch:{query}:{engine}"));
			}
		}
	}

	PerfBoardDecision::from_reasons(reasons)
}

fn require_header_string(
	header: &serde_json::Value,
	field: &str,
	expected: &str,
	reasons: &mut Vec<String>,
) {
	if header.get(field).and_then(serde_json::Value::as_str) != Some(expected) {
		reasons.push(format!("coordinate_mismatch:{field}:expected_{expected}"));
	}
}

fn require_header_u64(
	header: &serde_json::Value,
	field: &str,
	expected: u64,
	reasons: &mut Vec<String>,
) {
	if header.get(field).and_then(serde_json::Value::as_u64) != Some(expected) {
		reasons.push(format!("coordinate_mismatch:{field}:expected_{expected}"));
	}
}

/// Compare only the coordinate. Toron and harness identities are reported, not equated; DuckDB is
/// the fixed reference and therefore must retain the same staged hash.
pub fn compare_perf_board_headers(
	baseline: &serde_json::Value,
	candidate: &serde_json::Value,
) -> PerfBoardComparison {
	let mut reasons = Vec::new();
	for field in [
		"suite",
		"scale_factor",
		"mode",
		"data_source",
		"profile",
		"queries",
		"engines",
		"dataset_verification",
		"dataset_manifest_fingerprint",
		"runs",
		"warmup",
		"memory_per_task",
		"memory_per_task_provenance",
		"spill_bytes_reserved",
		"spill_bytes_reserved_provenance",
		"partition_work_concurrency_requested",
		"partition_work_concurrency_requested_provenance",
		"duckdb_threads",
		"duckdb_threads_provenance",
		"duckdb_version",
		"cpu_affinity_requested",
		"cpu_affinity_requested_provenance",
		"duckdb_memory_limit",
		"duckdb_memory_limit_provenance",
	] {
		if baseline.get(field) != candidate.get(field) {
			reasons.push(format!("board_coordinate_mismatch:{field}"));
		}
	}
	// ADR 591: a board from before the affinity coordinate has no field at all; it is named as
	// absent rather than folded into a mismatch, since nothing was requested on it.
	for (side, header) in [("baseline", baseline), ("candidate", candidate)] {
		if header
			.get("cpu_affinity_requested")
			.and_then(serde_json::Value::as_str)
			.is_none_or(str::is_empty)
		{
			reasons.push(format!(
				"board_coordinate_absent:cpu_affinity_requested:{side}"
			));
		}
		// ADR 603: a board from before the DuckDB memory coordinate carries neither field.
		for field in ["duckdb_memory_limit", "duckdb_memory_limit_provenance"] {
			if header.get(field).is_none() {
				reasons.push(format!("board_coordinate_absent:{field}:{side}"));
			}
		}
	}
	if baseline.get("duckdb_executable_sha256") != candidate.get("duckdb_executable_sha256") {
		reasons.push(format!(
			"duckdb_reference_mismatch:baseline={}:candidate={}",
			baseline
				.get("duckdb_executable_sha256")
				.unwrap_or(&serde_json::Value::Null),
			candidate
				.get("duckdb_executable_sha256")
				.unwrap_or(&serde_json::Value::Null)
		));
	}
	let decision = PerfBoardDecision::from_reasons(reasons);
	let identity_field = |header: &serde_json::Value, field: &str| {
		header
			.get("evidence_identity")
			.and_then(|identity| identity.get(field))
			.and_then(serde_json::Value::as_str)
			.map(str::to_string)
	};
	PerfBoardComparison {
		comparable: decision.candidate_authority,
		reasons: decision.reasons,
		baseline_source_tree_id: identity_field(baseline, "source_tree_id"),
		candidate_source_tree_id: identity_field(candidate, "source_tree_id"),
		baseline_harness_sha256: identity_field(baseline, "harness_sha256"),
		candidate_harness_sha256: identity_field(candidate, "harness_sha256"),
		baseline_duckdb_sha256: baseline
			.get("duckdb_executable_sha256")
			.and_then(serde_json::Value::as_str)
			.map(str::to_string),
		candidate_duckdb_sha256: candidate
			.get("duckdb_executable_sha256")
			.and_then(serde_json::Value::as_str)
			.map(str::to_string),
	}
}

/// ADR 603: this process's own `ru_maxrss` in KiB, read just before a child is spawned. A child's
/// `ru_maxrss` can carry the forked parent's high-water across `exec`, so the child's reading is
/// labelled against this value rather than taken as its own peak.
pub fn self_peak_rss_kib() -> Option<u64> {
	#[cfg(unix)]
	{
		let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
		let read = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
		if read != 0 {
			return None;
		}
		let usage = unsafe { usage.assume_init() };
		Some(usage.ru_maxrss.max(0) as u64)
	}
	#[cfg(not(unix))]
	{
		None
	}
}

/// ADR 603: `above_parent_high_water` when the child's reading exceeds the parent's high-water at
/// spawn, the best available reading of the child's own peak; `ambiguous` otherwise, including
/// equal, below, and unknown. An ambiguous value is never presented as the cell's consumption.
pub fn peak_rss_label(child_kib: Option<u64>, parent_kib: Option<u64>) -> &'static str {
	match (child_kib, parent_kib) {
		(Some(child), Some(parent)) if child > parent => "above_parent_high_water",
		_ => "ambiguous",
	}
}

/// ADR 603: a Toron cell's spill status over its samples. `NoSpill` only when every sample reads
/// `spill_bytes = 0` and every join-spill observation present reports zero bytes and no spilled
/// path; an empty observation list is evidence of nothing either way (ADR 591's q21 spilled with
/// an empty list; its q11, q14, and q19 carried zero-byte `in_memory` observations).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CellSpillStatus {
	NoSpill,
	Spilling,
	/// A sample without a numeric `spill_bytes`, or no samples: the status cannot be read.
	Unreadable,
}

pub fn cell_spill_status(samples: &[&serde_json::Value]) -> CellSpillStatus {
	if samples.is_empty() {
		return CellSpillStatus::Unreadable;
	}
	let mut spilled = false;
	for sample in samples {
		let Some(bytes) = sample
			.get("spill_bytes")
			.and_then(serde_json::Value::as_u64)
		else {
			return CellSpillStatus::Unreadable;
		};
		spilled |= bytes != 0;
		for observation in sample
			.get("join_spill_observations")
			.and_then(serde_json::Value::as_array)
			.into_iter()
			.flatten()
		{
			let observed_bytes = observation
				.get("spill_bytes")
				.and_then(serde_json::Value::as_u64);
			let path = observation.get("path").and_then(serde_json::Value::as_str);
			match (observed_bytes, path) {
				(Some(0), Some("in_memory")) => {}
				(None, _) | (_, None) => return CellSpillStatus::Unreadable,
				_ => spilled = true,
			}
		}
	}
	if spilled {
		CellSpillStatus::Spilling
	} else {
		CellSpillStatus::NoSpill
	}
}

/// The median `execute_ms` of a cell's measured, passing samples.
fn cell_median_execute_ms(samples: &[serde_json::Value], query: &str, engine: &str) -> Option<f64> {
	let mut values = samples
		.iter()
		.filter(|row| {
			row.get("query").and_then(serde_json::Value::as_str) == Some(query)
				&& row.get("engine").and_then(serde_json::Value::as_str) == Some(engine)
				&& row.get("warmup").and_then(serde_json::Value::as_bool) == Some(false)
				&& row.get("status").and_then(serde_json::Value::as_str) == Some("pass")
		})
		.filter_map(|row| row.get("execute_ms").and_then(serde_json::Value::as_f64))
		.collect::<Vec<_>>();
	if values.is_empty() {
		return None;
	}
	values.sort_by(f64::total_cmp);
	let middle = values.len() / 2;
	Some(if values.len() % 2 == 1 {
		values[middle]
	} else {
		(values[middle - 1] + values[middle]) / 2.0
	})
}

fn measured_samples<'a>(
	samples: &'a [serde_json::Value],
	query: &str,
	engine: &str,
) -> Vec<&'a serde_json::Value> {
	samples
		.iter()
		.filter(|row| {
			row.get("query").and_then(serde_json::Value::as_str) == Some(query)
				&& row.get("engine").and_then(serde_json::Value::as_str) == Some(engine)
				&& row.get("warmup").and_then(serde_json::Value::as_bool) == Some(false)
		})
		.collect()
}

/// ADR 603: the anchor and the two memory panels side by side, per cell and in aggregate. Panel A
/// is DuckDB under a nominally matched cap; Panel B is Toron at a larger budget, whose no-spill
/// status is read per cell and never inferred. Panel B's full aggregate always covers every cell,
/// with spilling or unreadable cells named; a no-spill subset aggregate states its coverage.
/// ADR 603: a sweep cell's readings and its result checked against the timed cell's digests.
/// `timed_digests_vary` is an identity limitation of the timed rows themselves, reported as such;
/// row counts are never taken as identity.
fn sweep_cell(
	sweep: &[serde_json::Value],
	timed: &[serde_json::Value],
	query: &str,
) -> serde_json::Value {
	let Some(cell) = sweep.iter().find(|row| {
		row.get("kind").and_then(serde_json::Value::as_str) == Some("duckdb_profile_sweep_cell")
			&& row.get("query").and_then(serde_json::Value::as_str) == Some(query)
	}) else {
		return serde_json::json!({"result_identity": "sweep_absent"});
	};
	let timed_digests = measured_samples(timed, query, "duckdb")
		.iter()
		.filter_map(|row| {
			row.get("duckdb_result_sha256")
				.and_then(serde_json::Value::as_str)
		})
		.collect::<BTreeSet<_>>();
	let sweep_digest = cell
		.get("duckdb_result_sha256")
		.and_then(serde_json::Value::as_str);
	let identity = match (sweep_digest, timed_digests.len()) {
		(None, _) => "sweep_digest_absent",
		(_, 0) => "timed_digest_absent",
		(Some(digest), 1) if timed_digests.contains(digest) => "matches_timed",
		(Some(_), 1) => "differs_from_timed",
		(Some(_), _) => "timed_digests_vary",
	};
	serde_json::json!({
		"sweep_run_id": cell.get("sweep_run_id"),
		"peak_temp_dir_bytes": cell.get("peak_temp_dir_bytes"),
		"peak_buffer_bytes": cell.get("peak_buffer_bytes"),
		"result_identity": identity,
		"sweep_digest_among_timed": sweep_digest.is_some_and(|digest| timed_digests.contains(digest)),
	})
}

pub fn memory_panels_summary(
	anchor: &[serde_json::Value],
	panel_a: &[serde_json::Value],
	panel_b: &[serde_json::Value],
	sweep_anchor: &[serde_json::Value],
	sweep_panel_a: &[serde_json::Value],
) -> serde_json::Value {
	let ratio = |toron: Option<f64>, duckdb: Option<f64>| match (toron, duckdb) {
		(Some(toron), Some(duckdb)) if duckdb > 0.0 => Some(toron / duckdb),
		_ => None,
	};
	let mut cells = Vec::new();
	let mut sums = [0.0f64; 4];
	let mut complete = true;
	let mut subset = (0.0f64, 0.0f64, Vec::new());
	let mut panel_b_not_no_spill = Vec::new();
	for query in ALL_QUERY_IDS {
		let anchor_toron = cell_median_execute_ms(anchor, query, "toron");
		let anchor_duckdb = cell_median_execute_ms(anchor, query, "duckdb");
		let capped_duckdb = cell_median_execute_ms(panel_a, query, "duckdb");
		let unspilled_toron = cell_median_execute_ms(panel_b, query, "toron");
		let panel_b_status = cell_spill_status(&measured_samples(panel_b, query, "toron"));
		let anchor_status = cell_spill_status(&measured_samples(anchor, query, "toron"));
		match [anchor_toron, anchor_duckdb, capped_duckdb, unspilled_toron] {
			[Some(a), Some(b), Some(c), Some(d)] => {
				for (sum, value) in sums.iter_mut().zip([a, b, c, d]) {
					*sum += value;
				}
				if panel_b_status == CellSpillStatus::NoSpill {
					subset.0 += d;
					subset.1 += b;
					subset.2.push(query);
				}
			}
			_ => complete = false,
		}
		if panel_b_status != CellSpillStatus::NoSpill {
			panel_b_not_no_spill
				.push(serde_json::json!({"query": query, "status": panel_b_status}));
		}
		cells.push(serde_json::json!({
			"query": query,
			"anchor_toron_ms": anchor_toron,
			"anchor_duckdb_ms": anchor_duckdb,
			"anchor_ratio": ratio(anchor_toron, anchor_duckdb),
			"anchor_gap_ms": anchor_toron.zip(anchor_duckdb).map(|(t, d)| t - d),
			"anchor_toron_spill": anchor_status,
			"panel_a_duckdb_ms": capped_duckdb,
			"panel_a_ratio_anchor_toron": ratio(anchor_toron, capped_duckdb),
			"panel_a_over_anchor_duckdb": ratio(capped_duckdb, anchor_duckdb),
			"panel_b_toron_ms": unspilled_toron,
			"panel_b_toron_spill": panel_b_status,
			"panel_b_ratio_anchor_duckdb": ratio(unspilled_toron, anchor_duckdb),
			"panel_b_over_anchor_toron": ratio(unspilled_toron, anchor_toron),
			"anchor_duckdb_sweep": sweep_cell(sweep_anchor, anchor, query),
			"panel_a_duckdb_sweep": sweep_cell(sweep_panel_a, panel_a, query),
		}));
	}
	let [anchor_toron, anchor_duckdb, capped_duckdb, unspilled_toron] = sums;
	// An incomplete summary publishes no aggregate: a partial sum is not the 22-cell total.
	let aggregate = |value: f64| complete.then_some(value);
	serde_json::json!({
		"cells": cells,
		"complete": complete,
		"anchor": {
			"toron_sum_ms": aggregate(anchor_toron),
			"duckdb_sum_ms": aggregate(anchor_duckdb),
			"ratio_of_sums": aggregate(anchor_toron / anchor_duckdb),
		},
		"panel_a": {
			"label": "DuckDB under a nominally matched cap: process-wide against Toron's per-task budget",
			"duckdb_sum_ms": aggregate(capped_duckdb),
			"ratio_of_sums_anchor_toron": aggregate(anchor_toron / capped_duckdb),
		},
		"panel_b": {
			"label": "Toron at a larger budget; no-spill read per cell, never from the budget",
			"toron_sum_ms": aggregate(unspilled_toron),
			"ratio_of_sums_anchor_duckdb": aggregate(unspilled_toron / anchor_duckdb),
			"cells_not_no_spill": panel_b_not_no_spill,
			"no_spill_subset": {
				"cells": subset.2.len(),
				"of": ALL_QUERY_IDS.len(),
				"queries": subset.2,
				"toron_sum_ms": aggregate(subset.0),
				"anchor_duckdb_sum_ms": aggregate(subset.1),
				"ratio_of_sums": if complete && subset.1 > 0.0 {
					Some(subset.0 / subset.1)
				} else {
					None
				},
			},
		},
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	fn fixture() -> (
		serde_json::Value,
		Vec<serde_json::Value>,
		Vec<serde_json::Value>,
	) {
		let header = serde_json::json!({
			"suite": PERF_BOARD_SUITE,
			"evidence_identity": {"harness_sha256": "h", "duckdb_sha256": "duckdb"},
			"content_identity_reasons": [],
			"scale_factor": 8.0,
			"mode": "strict",
			"data_source": "parquet",
			"profile": "release",
			"runs": PERF_BOARD_RUNS,
			"warmup": PERF_BOARD_WARMUP,
			"queries": ALL_QUERY_IDS,
			"engines": PERF_BOARD_ENGINES,
			"dataset_verification": "full_hash",
			"dataset_manifest_fingerprint": "dataset",
			"memory_per_task": PERF_BOARD_MEMORY_BYTES,
			"memory_per_task_provenance": "explicit",
			"spill_bytes_reserved": PERF_BOARD_SPILL_BYTES,
			"spill_bytes_reserved_provenance": "explicit",
			"partition_work_concurrency_requested": 1,
			"partition_work_concurrency_requested_provenance": "explicit",
			"duckdb_threads": "1",
			"duckdb_threads_provenance": "explicit",
			"duckdb_version": "v1.5.4",
			"duckdb_executable_sha256": "duckdb",
			"duckdb_memory_limit": null,
			"duckdb_memory_limit_provenance": "default",
			"cpu_affinity_requested": "0-15",
			"cpu_affinity_requested_provenance": "explicit",
			"cpu_affinity_observed": "0-15",
			"diagnostics_disabled": true,
			"host_start_quiescent": true,
			"host_end_quiescent": true,
			"join_spill_scratch_empty_before": true,
			"join_spill_scratch_empty_after": true,
			"output_directory_fresh": true,
		});
		let mut samples = Vec::new();
		let mut summaries = Vec::new();
		for query in ALL_QUERY_IDS {
			for engine in PERF_BOARD_ENGINES {
				for (warmup, run_index) in std::iter::once((true, 0u64))
					.chain((0..PERF_BOARD_RUNS as u64).map(|run| (false, run)))
				{
					samples.push(serde_json::json!({
						"query": query, "engine": engine, "warmup": warmup,
						"run_index": run_index, "status": "pass", "scale_factor": 8.0,
						"mode": "strict", "data_source": "parquet", "profile": "release",
						"dataset_verification": "full_hash",
						"dataset_manifest_fingerprint": "dataset",
						"memory_per_task": PERF_BOARD_MEMORY_BYTES,
						"memory_per_task_provenance": "explicit",
						"spill_bytes_reserved": PERF_BOARD_SPILL_BYTES,
						"spill_bytes_reserved_provenance": "explicit",
						"partition_work_concurrency_requested": 1,
						"partition_work_concurrency_requested_provenance": "explicit",
						"partition_work_concurrency_observations": [],
						"cpu_affinity_requested": "0-15",
						"cpu_affinity_requested_provenance": "explicit",
						"cpu_affinity_observed": "0-15",
						"evidence_identity": header["evidence_identity"],
						"duckdb_threads": (engine == "duckdb").then_some("1"),
						"duckdb_executable_sha256": (engine == "duckdb").then_some("duckdb"),
						"duckdb_version": (engine == "duckdb").then_some("v1.5.4"),
						"duckdb_memory_limit": null,
						"duckdb_memory_limit_provenance": (engine == "duckdb").then_some("default"),
					}));
				}
				summaries.push(serde_json::json!({
					"query": query, "engine": engine, "status": "pass", "flags": [],
					"runs": PERF_BOARD_RUNS, "warmup": PERF_BOARD_WARMUP,
				}));
			}
		}
		(header, samples, summaries)
	}

	#[test]
	fn complete_board_is_a_candidate() {
		let (header, samples, summaries) = fixture();
		let decision = evaluate_perf_board(&header, &samples, &summaries);
		assert!(decision.candidate_authority, "{:?}", decision.reasons);
	}

	#[test]
	fn dropping_one_duckdb_sample_names_the_exact_missing_sample() {
		let (header, mut samples, summaries) = fixture();
		let position = samples
			.iter()
			.position(|row| {
				row["query"] == "q07"
					&& row["engine"] == "duckdb"
					&& row["warmup"] == false
					&& row["run_index"] == 3
			})
			.unwrap();
		samples.remove(position);
		let decision = evaluate_perf_board(&header, &samples, &summaries);
		assert!(!decision.candidate_authority);
		assert!(
			decision
				.reasons
				.iter()
				.any(|reason| { reason == "missing_sample:q07:duckdb:measured:3" })
		);
	}

	#[test]
	fn pin_and_observed_concurrency_are_both_enforced() {
		let (mut header, mut samples, summaries) = fixture();
		header["partition_work_concurrency_requested"] = serde_json::Value::Null;
		let decision = evaluate_perf_board(&header, &samples, &summaries);
		assert!(
			decision
				.reasons
				.iter()
				.any(|reason| reason.contains("partition_work_concurrency_requested"))
		);
		header["partition_work_concurrency_requested"] = serde_json::json!(2);
		let decision = evaluate_perf_board(&header, &samples, &summaries);
		assert!(
			decision
				.reasons
				.iter()
				.any(|reason| reason.contains("partition_work_concurrency_requested"))
		);
		header["partition_work_concurrency_requested"] = serde_json::json!(1);
		samples[0]["partition_work_concurrency_observations"] =
			serde_json::json!([{"planned_k": 2}]);
		let decision = evaluate_perf_board(&header, &samples, &summaries);
		assert!(
			decision
				.reasons
				.iter()
				.any(|reason| reason == "partition_work_concurrency_exceeded:q01")
		);
	}

	#[test]
	fn a_raw_correctness_failure_cannot_be_hidden_by_a_clean_summary() {
		let (header, mut samples, summaries) = fixture();
		samples[0]["flags"] = serde_json::json!(["row_count_mismatch"]);
		let decision = evaluate_perf_board(&header, &samples, &summaries);
		assert!(
			decision
				.reasons
				.iter()
				.any(|reason| reason == "sample_invalid:q01:toron:row_count_mismatch"),
			"{:?}",
			decision.reasons
		);
	}

	#[test]
	fn cross_engine_type_presentation_stays_disclosure_not_a_board_failure() {
		let (header, mut samples, mut summaries) = fixture();
		samples[0]["flags"] = serde_json::json!(["schema_mismatch"]);
		summaries[0]["flags"] = serde_json::json!(["schema_mismatch"]);
		let decision = evaluate_perf_board(&header, &samples, &summaries);
		assert!(decision.candidate_authority, "{:?}", decision.reasons);
	}

	#[test]
	fn toron_identity_may_change_but_duckdb_reference_may_not() {
		let (mut baseline, _, _) = fixture();
		let mut candidate = baseline.clone();
		baseline["evidence_identity"]["harness_sha256"] = serde_json::json!("old");
		candidate["evidence_identity"]["harness_sha256"] = serde_json::json!("new");
		let comparison = compare_perf_board_headers(&baseline, &candidate);
		assert!(comparison.comparable);
		assert_eq!(comparison.baseline_harness_sha256.as_deref(), Some("old"));
		assert_eq!(comparison.candidate_harness_sha256.as_deref(), Some("new"));
		candidate["duckdb_executable_sha256"] = serde_json::json!("different");
		let decision = compare_perf_board_headers(&baseline, &candidate);
		assert!(
			decision
				.reasons
				.iter()
				.any(|reason| reason.contains("duckdb_reference_mismatch"))
		);
	}

	#[test]
	fn every_coordinate_difference_is_named_and_toron_identity_is_not_one() {
		let (baseline, _, _) = fixture();
		for field in [
			"suite",
			"scale_factor",
			"mode",
			"data_source",
			"profile",
			"queries",
			"engines",
			"dataset_verification",
			"dataset_manifest_fingerprint",
			"runs",
			"warmup",
			"memory_per_task",
			"memory_per_task_provenance",
			"spill_bytes_reserved",
			"spill_bytes_reserved_provenance",
			"partition_work_concurrency_requested",
			"partition_work_concurrency_requested_provenance",
			"duckdb_threads",
			"duckdb_threads_provenance",
			"duckdb_version",
			"cpu_affinity_requested",
			"cpu_affinity_requested_provenance",
		] {
			let mut candidate = baseline.clone();
			candidate[field] = serde_json::json!("different");
			let decision = compare_perf_board_headers(&baseline, &candidate);
			assert!(
				decision
					.reasons
					.iter()
					.any(|reason| reason == &format!("board_coordinate_mismatch:{field}")),
				"{field}: {:?}",
				decision.reasons
			);
		}
	}

	#[test]
	fn absent_content_identity_is_a_named_candidate_refusal() {
		let (mut header, samples, summaries) = fixture();
		header["content_identity_reasons"] =
			serde_json::json!(["content_identity_manifest_absent"]);
		let decision = evaluate_perf_board(&header, &samples, &summaries);
		assert!(!decision.candidate_authority);
		assert!(
			decision
				.reasons
				.iter()
				.any(|reason| reason == "content_identity_manifest_absent")
		);
	}

	#[test]
	fn adr591_affinity_must_be_explicit_and_honoured_by_the_header() {
		let (mut header, samples, summaries) = fixture();
		header["cpu_affinity_requested_provenance"] = serde_json::json!("default");
		let decision = evaluate_perf_board(&header, &samples, &summaries);
		assert!(decision.reasons.iter().any(|reason| {
			reason == "coordinate_mismatch:cpu_affinity_requested_provenance:expected_explicit"
		}));
		let (mut header, samples, summaries) = fixture();
		header["cpu_affinity_observed"] = serde_json::json!("0-31");
		let decision = evaluate_perf_board(&header, &samples, &summaries);
		assert!(
			decision
				.reasons
				.iter()
				.any(|reason| reason == "cpu_affinity_mismatch:header:observed"),
			"{:?}",
			decision.reasons
		);
		let (mut header, samples, summaries) = fixture();
		header
			.as_object_mut()
			.unwrap()
			.retain(|key, _| !key.starts_with("cpu_affinity"));
		let decision = evaluate_perf_board(&header, &samples, &summaries);
		for expected in [
			"coordinate_mismatch:cpu_affinity_requested_provenance:expected_explicit",
			"coordinate_missing:cpu_affinity_requested",
		] {
			assert!(
				decision.reasons.iter().any(|reason| reason == expected),
				"{expected}: {:?}",
				decision.reasons
			);
		}
	}

	#[test]
	fn adr591_every_row_is_bound_to_the_header_request_not_only_to_itself() {
		// One row observed on another mask: named by its observed field.
		let (header, mut samples, summaries) = fixture();
		let target = samples
			.iter()
			.position(|row| {
				row["query"] == "q05" && row["engine"] == "duckdb" && row["warmup"] == false
			})
			.unwrap();
		samples[target]["cpu_affinity_observed"] = serde_json::json!("0-31");
		let decision = evaluate_perf_board(&header, &samples, &summaries);
		assert!(!decision.candidate_authority);
		assert_eq!(
			decision
				.reasons
				.iter()
				.filter(|reason| reason.starts_with("cpu_affinity_mismatch"))
				.collect::<Vec<_>>(),
			vec!["cpu_affinity_mismatch:q05:duckdb:observed"]
		);
		// One row with BOTH fields moved to another mask agrees with itself and is still refused,
		// named by its requested field, because the request is bound to the header's.
		let (header, mut samples, summaries) = fixture();
		samples[target]["cpu_affinity_requested"] = serde_json::json!("16-31");
		samples[target]["cpu_affinity_observed"] = serde_json::json!("16-31");
		let decision = evaluate_perf_board(&header, &samples, &summaries);
		assert!(!decision.candidate_authority);
		assert_eq!(
			decision
				.reasons
				.iter()
				.filter(|reason| reason.starts_with("cpu_affinity_mismatch"))
				.collect::<Vec<_>>(),
			vec!["cpu_affinity_mismatch:q05:duckdb:requested"]
		);
		// One row whose provenance alone is `default`: its masks agree with the header, and the
		// row is still refused, because the row's own claim of how the mask arose must match.
		let (header, mut samples, summaries) = fixture();
		samples[target]["cpu_affinity_requested_provenance"] = serde_json::json!("default");
		let decision = evaluate_perf_board(&header, &samples, &summaries);
		assert!(!decision.candidate_authority);
		assert_eq!(
			decision
				.reasons
				.iter()
				.filter(|reason| reason.starts_with("cpu_affinity_mismatch"))
				.collect::<Vec<_>>(),
			vec!["cpu_affinity_mismatch:q05:duckdb:provenance"]
		);
		// One row without a provenance field at all.
		let (header, mut samples, summaries) = fixture();
		samples[target]
			.as_object_mut()
			.unwrap()
			.remove("cpu_affinity_requested_provenance");
		let decision = evaluate_perf_board(&header, &samples, &summaries);
		assert_eq!(
			decision
				.reasons
				.iter()
				.filter(|reason| reason.starts_with("cpu_affinity_mismatch"))
				.collect::<Vec<_>>(),
			vec!["cpu_affinity_mismatch:q05:duckdb:provenance"]
		);
		// A row without any of the fields is refused on every count.
		let (header, mut samples, summaries) = fixture();
		samples[target]
			.as_object_mut()
			.unwrap()
			.retain(|key, _| !key.starts_with("cpu_affinity"));
		let decision = evaluate_perf_board(&header, &samples, &summaries);
		for expected in [
			"cpu_affinity_mismatch:q05:duckdb:requested",
			"cpu_affinity_mismatch:q05:duckdb:provenance",
		] {
			assert!(
				decision.reasons.iter().any(|reason| reason == expected),
				"{expected}: {:?}",
				decision.reasons
			);
		}
	}

	#[test]
	fn adr591_boards_compare_only_on_equal_affinity_and_an_absent_one_is_named() {
		let (baseline, _, _) = fixture();
		let mut candidate = baseline.clone();
		candidate["cpu_affinity_requested"] = serde_json::json!("0-7");
		candidate["cpu_affinity_observed"] = serde_json::json!("0-7");
		let comparison = compare_perf_board_headers(&baseline, &candidate);
		assert!(!comparison.comparable);
		assert!(
			comparison
				.reasons
				.iter()
				.any(|reason| reason == "board_coordinate_mismatch:cpu_affinity_requested")
		);
		let mut old_board = baseline.clone();
		old_board
			.as_object_mut()
			.unwrap()
			.retain(|key, _| !key.starts_with("cpu_affinity"));
		let comparison = compare_perf_board_headers(&old_board, &baseline);
		assert!(!comparison.comparable);
		assert!(
			comparison
				.reasons
				.iter()
				.any(|reason| reason == "board_coordinate_absent:cpu_affinity_requested:baseline"),
			"{:?}",
			comparison.reasons
		);
		let comparison = compare_perf_board_headers(&baseline, &baseline);
		assert!(comparison.comparable, "{:?}", comparison.reasons);
	}

	#[test]
	fn a_capped_duckdb_artifact_cannot_seal_as_the_anchor() {
		let (mut header, mut samples, summaries) = fixture();
		header["duckdb_memory_limit"] = serde_json::json!("2GiB");
		header["duckdb_memory_limit_provenance"] = serde_json::json!("explicit");
		for row in samples.iter_mut().filter(|row| row["engine"] == "duckdb") {
			row["duckdb_memory_limit"] = serde_json::json!("2GiB");
			row["duckdb_memory_limit_provenance"] = serde_json::json!("explicit");
		}
		let decision = evaluate_perf_board(&header, &samples, &summaries);
		assert!(!decision.candidate_authority);
		assert!(
			decision.reasons.iter().any(|reason| reason
				== "coordinate_mismatch:duckdb_memory_limit_provenance:expected_default"),
			"{:?}",
			decision.reasons
		);
		assert!(
			decision
				.reasons
				.iter()
				.any(|reason| reason == "coordinate_mismatch:duckdb_memory_limit:expected_null"),
			"{:?}",
			decision.reasons
		);
		// A default provenance with the limit field missing is not the anchor either.
		let (mut header, samples, summaries) = fixture();
		header
			.as_object_mut()
			.unwrap()
			.remove("duckdb_memory_limit");
		let decision = evaluate_perf_board(&header, &samples, &summaries);
		assert!(!decision.candidate_authority);
		assert!(
			decision
				.reasons
				.iter()
				.any(|reason| reason == "coordinate_mismatch:duckdb_memory_limit:expected_null")
		);
	}

	#[test]
	fn a_board_without_the_memory_coordinate_is_named_absent() {
		let (header, _, _) = fixture();
		let mut older = header.clone();
		let object = older.as_object_mut().unwrap();
		object.remove("duckdb_memory_limit");
		object.remove("duckdb_memory_limit_provenance");
		let comparison = compare_perf_board_headers(&older, &header);
		assert!(!comparison.comparable);
		for field in ["duckdb_memory_limit", "duckdb_memory_limit_provenance"] {
			assert!(
				comparison
					.reasons
					.iter()
					.any(|reason| reason == &format!("board_coordinate_absent:{field}:baseline")),
				"{field}: {:?}",
				comparison.reasons
			);
		}
	}

	#[test]
	fn a_duckdb_row_is_bound_to_the_headers_memory_coordinate() {
		for field in ["duckdb_memory_limit", "duckdb_memory_limit_provenance"] {
			let (header, mut samples, summaries) = fixture();
			let row = samples
				.iter_mut()
				.find(|row| row["engine"] == "duckdb" && row["query"] == "q05")
				.unwrap();
			row[field] = serde_json::json!("2GiB");
			let decision = evaluate_perf_board(&header, &samples, &summaries);
			assert!(!decision.candidate_authority, "{field}");
			assert!(
				decision
					.reasons
					.iter()
					.any(|reason| reason == &format!("row_coordinate_mismatch:q05:duckdb:{field}")),
				"{field}: {:?}",
				decision.reasons
			);
		}
		let (header, mut samples, summaries) = fixture();
		samples
			.iter_mut()
			.find(|row| row["engine"] == "duckdb" && row["query"] == "q05")
			.unwrap()
			.as_object_mut()
			.unwrap()
			.remove("duckdb_memory_limit_provenance");
		let decision = evaluate_perf_board(&header, &samples, &summaries);
		assert!(
			decision.reasons.iter().any(|reason| reason
				== "row_coordinate_mismatch:q05:duckdb:duckdb_memory_limit_provenance")
		);
	}

	#[test]
	fn peak_rss_is_labelled_against_the_parents_high_water() {
		assert_eq!(
			peak_rss_label(Some(900), Some(633)),
			"above_parent_high_water"
		);
		assert_eq!(peak_rss_label(Some(633), Some(633)), "ambiguous", "equal");
		assert_eq!(peak_rss_label(Some(500), Some(633)), "ambiguous", "below");
		assert_eq!(peak_rss_label(None, Some(633)), "ambiguous");
		assert_eq!(peak_rss_label(Some(900), None), "ambiguous");
		assert!(self_peak_rss_kib().is_some_and(|kib| kib > 0));
	}

	#[test]
	fn a_cell_is_no_spill_only_on_zero_bytes_and_no_spilled_path() {
		let in_memory = serde_json::json!({"spill_bytes": 0, "join_spill_observations": [
			{"replay_id": 3, "path": "in_memory", "spill_bytes": 0}]});
		let empty = serde_json::json!({"spill_bytes": 0, "join_spill_observations": []});
		let absent = serde_json::json!({"spill_bytes": 0});
		assert_eq!(
			cell_spill_status(&[&in_memory, &empty, &absent]),
			CellSpillStatus::NoSpill
		);
		// ADR 591's q21: bytes spilled, an empty list.
		let spilled_empty_list =
			serde_json::json!({"spill_bytes": 27_242_808, "join_spill_observations": []});
		assert_eq!(
			cell_spill_status(&[&in_memory, &spilled_empty_list]),
			CellSpillStatus::Spilling
		);
		// An observation that reports spilled bytes, and one on a spilled path at zero bytes.
		let observed_bytes = serde_json::json!({"spill_bytes": 0, "join_spill_observations": [
			{"replay_id": 1, "path": "streaming_scan_build", "spill_bytes": 4096}]});
		assert_eq!(
			cell_spill_status(&[&observed_bytes]),
			CellSpillStatus::Spilling
		);
		let spilled_path = serde_json::json!({"spill_bytes": 0, "join_spill_observations": [
			{"replay_id": 1, "path": "streamed_spilled_build", "spill_bytes": 0}]});
		assert_eq!(
			cell_spill_status(&[&spilled_path]),
			CellSpillStatus::Spilling
		);
		let in_memory_with_bytes = serde_json::json!({"spill_bytes": 0, "join_spill_observations": [
			{"replay_id": 1, "path": "in_memory", "spill_bytes": 8}]});
		assert_eq!(
			cell_spill_status(&[&in_memory_with_bytes]),
			CellSpillStatus::Spilling
		);
		// Unreadable: no samples, a missing count, an observation without its fields.
		assert_eq!(cell_spill_status(&[]), CellSpillStatus::Unreadable);
		assert_eq!(
			cell_spill_status(&[&serde_json::json!({"spill_bytes": null})]),
			CellSpillStatus::Unreadable
		);
		let malformed = serde_json::json!({"spill_bytes": 0, "join_spill_observations": [
			{"replay_id": 1}]});
		assert_eq!(
			cell_spill_status(&[&malformed]),
			CellSpillStatus::Unreadable
		);
	}

	fn panel_rows(engine: &str, ms: f64, spill_bytes: u64) -> Vec<serde_json::Value> {
		ALL_QUERY_IDS
			.iter()
			.flat_map(|query| {
				(0..3).map(move |run| {
					serde_json::json!({"query": query, "engine": engine, "warmup": false,
						"status": "pass", "run_index": run, "execute_ms": ms + run as f64,
						"spill_bytes": spill_bytes, "join_spill_observations": []})
				})
			})
			.collect()
	}

	#[test]
	fn the_panels_report_full_aggregates_and_a_stated_no_spill_subset() {
		let mut anchor = panel_rows("toron", 400.0, 0);
		anchor.extend(panel_rows("duckdb", 100.0, 0));
		let panel_a = panel_rows("duckdb", 110.0, 0);
		let mut panel_b = panel_rows("toron", 300.0, 0);
		// One Panel B cell still spills: it stays in the full aggregate and leaves the subset.
		for row in panel_b.iter_mut().filter(|row| row["query"] == "q21") {
			row["spill_bytes"] = serde_json::json!(4096);
		}
		let summary = memory_panels_summary(&anchor, &panel_a, &panel_b, &[], &[]);
		assert_eq!(summary["complete"], true);
		let n = ALL_QUERY_IDS.len() as f64;
		assert_eq!(summary["anchor"]["toron_sum_ms"], 401.0 * n);
		assert_eq!(summary["anchor"]["ratio_of_sums"], 401.0 / 101.0);
		assert_eq!(summary["panel_a"]["duckdb_sum_ms"], 111.0 * n);
		assert_eq!(
			summary["panel_b"]["toron_sum_ms"],
			301.0 * n,
			"the spilling cell stays in"
		);
		assert_eq!(summary["panel_b"]["cells_not_no_spill"][0]["query"], "q21");
		assert_eq!(
			summary["panel_b"]["cells_not_no_spill"][0]["status"],
			"spilling"
		);
		let subset = &summary["panel_b"]["no_spill_subset"];
		assert_eq!(subset["cells"], ALL_QUERY_IDS.len() - 1);
		assert_eq!(subset["of"], ALL_QUERY_IDS.len());
		assert!(
			!subset["queries"]
				.as_array()
				.unwrap()
				.iter()
				.any(|q| q == "q21")
		);
		assert_eq!(subset["toron_sum_ms"], 301.0 * (n - 1.0));
		let q21 = summary["cells"]
			.as_array()
			.unwrap()
			.iter()
			.find(|cell| cell["query"] == "q21")
			.unwrap();
		assert_eq!(q21["panel_b_toron_spill"], "spilling");
		assert_eq!(q21["anchor_gap_ms"], 300.0);
		assert_eq!(
			q21["anchor_duckdb_sweep"]["result_identity"],
			"sweep_absent"
		);
		// One missing cell: the summary is incomplete and publishes no aggregate.
		let summary = memory_panels_summary(&anchor, &panel_a[3..], &panel_b, &[], &[]);
		assert_eq!(summary["complete"], false);
		for aggregate in [
			&summary["anchor"]["toron_sum_ms"],
			&summary["anchor"]["ratio_of_sums"],
			&summary["panel_a"]["duckdb_sum_ms"],
			&summary["panel_a"]["ratio_of_sums_anchor_toron"],
			&summary["panel_b"]["toron_sum_ms"],
			&summary["panel_b"]["ratio_of_sums_anchor_duckdb"],
			&summary["panel_b"]["no_spill_subset"]["toron_sum_ms"],
			&summary["panel_b"]["no_spill_subset"]["ratio_of_sums"],
		] {
			assert_eq!(aggregate, &serde_json::Value::Null);
		}
	}

	#[test]
	fn a_sweep_result_is_checked_against_the_timed_digests() {
		let timed = |digests: &[&str]| -> Vec<serde_json::Value> {
			digests
				.iter()
				.enumerate()
				.map(|(run, digest)| {
					serde_json::json!({"query": "q05", "engine": "duckdb", "warmup": false,
						"status": "pass", "run_index": run, "execute_ms": 100.0,
						"duckdb_result_sha256": digest})
				})
				.collect()
		};
		let sweep = |digest: Option<&str>| {
			vec![
				serde_json::json!({"kind": "duckdb_profile_sweep_cell", "query": "q05",
				"sweep_run_id": "s", "peak_temp_dir_bytes": 0, "peak_buffer_bytes": 9,
				"duckdb_result_sha256": digest}),
			]
		};
		let identity = |sweep_rows: &[serde_json::Value], timed_rows: &[serde_json::Value]| {
			sweep_cell(sweep_rows, timed_rows, "q05")["result_identity"].clone()
		};
		assert_eq!(
			identity(&sweep(Some("a")), &timed(&["a", "a"])),
			"matches_timed"
		);
		assert_eq!(
			identity(&sweep(Some("b")), &timed(&["a", "a"])),
			"differs_from_timed"
		);
		assert_eq!(
			identity(&sweep(Some("a")), &timed(&["a", "b"])),
			"timed_digests_vary"
		);
		assert_eq!(
			identity(&sweep(None), &timed(&["a"])),
			"sweep_digest_absent"
		);
		assert_eq!(
			identity(&sweep(Some("a")), &timed(&[])),
			"timed_digest_absent"
		);
		assert_eq!(identity(&[], &timed(&["a"])), "sweep_absent");
		assert_eq!(
			sweep_cell(&sweep(Some("a")), &timed(&["a"]), "q05")["peak_buffer_bytes"],
			9
		);
	}
}
