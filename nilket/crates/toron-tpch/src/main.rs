#![recursion_limit = "256"]

//! ADR 261: TPC-H Reachability Harness CLI — `status` / `prepare` / `run`, manual arg parsing (no clap,
//! matching Toron's no-extra-framework style). An unknown command/arg is a *harness* error (exit 2),
//! distinct from a query `blocked`/`fail` (which is structured telemetry).

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::time::Instant;

use toron_tpch::evidence::{
	BUILD_MANIFEST_ENV, BuildManifest, runtime_identity, runtime_identity_gate,
	runtime_identity_json, verify_staged_executable,
};
use toron_tpch::perf_board::{
	PERF_BOARD_ARTIFACT, PERF_BOARD_ENGINES, PERF_BOARD_EVIDENCE, PERF_BOARD_SUITE,
	evaluate_perf_board,
};
use toron_tpch::telemetry::{
	Attempt, CorrectnessAttempt, ExecutionCensusRow, FragmentCensusRow, PerfAttempt,
	PerfPhaseTimings, Phase, Status,
};
use toron_tpch::{
	ALL_QUERY_IDS, CACHE_ROOT, PRIMITIVE_LADDER_IDS, Q17BoundaryCensusOutcome,
	check_strict_query_with_data_source, correctness_matrix, export_perf_query_parquet,
	export_persisted_perf_query_parquet, gauge_filtered_build_side_swap, gauge_hash_smaller_build,
	gauge_membership_first, gauge_memory_per_task, gauge_outer_count_preaggregation,
	gauge_partition_work_concurrency_requested, gauge_scalar_agg_input_reduction,
	gauge_semi_filtered_reachable_keys, gauge_side_local_filter_routing,
	gauge_spill_bytes_reserved, generate_dev_lineitem, join_diagnostics_disabled,
	load_persisted_tpch_dataset, matrix, node_attribution_outer_reconciliation,
	normalize_duckdb_type, observe_host_quiescence, paired_ablation_dataset_fingerprint,
	parse_scale_factor, prepare_persisted_tpch_dataset, primitive_ladder_intent,
	q17_correctness_scale, run_dev_q1, run_dev_q02, run_dev_q3, run_dev_q04, run_dev_q05,
	run_dev_q6, run_dev_q07, run_dev_q08, run_dev_q09, run_dev_q10, run_dev_q11, run_dev_q12,
	run_dev_q13, run_dev_q14, run_dev_q15, run_dev_q16, run_dev_q17, run_dev_q18, run_dev_q19,
	run_dev_q20, run_dev_q21, run_dev_q22, run_execution_census_query, run_fragment_census_query,
	run_perf_query_with_data_source, run_placement_ab, run_q17_boundary_census_with_data_source,
	run_rune_perf_query_with_data_source, run_rune_q03_profile_loop_with_data_source,
	run_strict_q1, run_strict_q02, run_strict_q3, run_strict_q04, run_strict_q05, run_strict_q6,
	run_strict_q07, run_strict_q08, run_strict_q09, run_strict_q10, run_strict_q11, run_strict_q12,
	run_strict_q13, run_strict_q14, run_strict_q15, run_strict_q16, run_strict_q17, run_strict_q18,
	run_strict_q19, run_strict_q20, run_strict_q21, run_strict_q22,
	rune_q03_isolation_proof_with_data_source, schema_fingerprint_from_duckdb_fields,
	stable_fingerprint, status_rows, strict_lineitem_row_count, take_parquet_scan_ledger,
	validate_query_scale,
};

/// The default scale factor when `--sf` is omitted (a tiny reachability scale).
const DEFAULT_SF: f64 = 0.01;

// Deep multi-join binds (q10+) exceed a default worker stack, and the timing atlas walks
// every cell in one process. The runtime is built explicitly so the worker stacks are
// large enough for every command; `#[tokio::main]` cannot express this.
fn main() -> ExitCode {
	// ADR 591: affinity is per thread on Linux and inherited by threads and children created
	// afterwards, so it is applied here, before the runtime's workers exist.
	if let Err(error) = toron_tpch::affinity::apply_from_environment() {
		eprintln!("{error}");
		return ExitCode::from(2);
	}
	tokio::runtime::Builder::new_multi_thread()
		.worker_threads(4)
		.thread_stack_size(64 * 1024 * 1024)
		.enable_all()
		.build()
		.expect("tokio runtime")
		.block_on(run())
}

async fn run() -> ExitCode {
	let args: Vec<String> = std::env::args().skip(1).collect();
	let Some(command) = args.first().map(String::as_str) else {
		return usage();
	};
	match command {
		"status" => {
			for row in status_rows() {
				println!("{row}");
			}
			ExitCode::SUCCESS
		}
		"prepare" => {
			let sf = match resolved_sf(&args) {
				Ok(sf) => sf,
				Err(message) => return harness_error(&message),
			};
			let data_source = flag_value(&args, "--data-source").unwrap_or("memory");
			if matches!(data_source, "parquet" | "persisted") {
				let mode = flag_value(&args, "--mode").unwrap_or("strict");
				let data_dir = data_dir_from_args(&args);
				let dataset = match prepare_persisted_tpch_dataset(mode, sf, &data_dir) {
					Ok(dataset) => dataset,
					Err(message) => return harness_error(&message),
				};
				println!(
					"prepared persisted tpch mode={mode} sf={sf} root={} manifest={} tables={}",
					dataset.root_dir.display(),
					dataset.manifest_path.display(),
					dataset.tables.len()
				);
				return ExitCode::SUCCESS;
			}
			let batch = generate_dev_lineitem(sf);
			let _ = std::fs::create_dir_all(CACHE_ROOT);
			println!(
				"prepared dev lineitem rows={} sf={sf} cache_root={CACHE_ROOT}",
				batch.num_rows()
			);
			ExitCode::SUCCESS
		}
		"matrix" => {
			for row in matrix() {
				println!("{row}");
			}
			ExitCode::SUCCESS
		}
		"run" => run_command(&args).await,
		"perf" => perf_command(&args).await,
		"perf-run-one" => perf_run_one_command(&args).await,
		"perf-board-replay" => perf_board_replay_command(&args),
		"adr592-revalidate" => adr592_revalidate_command(&args),
		"adr593-journal-recheck" => adr593_journal_recheck_command(&args),
		"adr584-q21-attribution" => adr584_q21_attribution_command(&args),
		"adr585-q21-decomposition" => adr585_q21_decomposition_command(&args),
		"adr594-q21-inner-1" => adr594_q21_inner_1_command(&args),
		"adr595-q21-inner-1" => adr595_q21_inner_1_command(&args),
		"adr598-gate-probe" => adr598_gate_probe_command(&args),
		"adr598-disposition" => adr598_disposition_command(&args),
		"adr597-q21-exchange-skip" => adr597_q21_exchange_skip_command(&args),
		// ADR 603: profiled DuckDB children, one per cell, after a timed artifact is complete.
		"duckdb-profile-sweep" => duckdb_profile_sweep_command(&args),
		"memory-panels-summary" => memory_panels_summary_command(&args),
		// ADR 604: Toron's plan and costs beside DuckDB's profiled operator tree, one cell.
		"structural-census" => structural_census_command(&args).await,
		// ADR 605: scan work per table on both engines, and the local bridge's ablation effect.
		"scan-work-census" => scan_work_census_command(&args),
		"adr597-reevaluate" => adr597_reevaluate_command(&args),
		"adr596-q21-build-write" => adr596_q21_build_write_command(&args),
		"adr596-reevaluate" => adr596_reevaluate_command(&args),
		"adr595-reevaluate" => adr595_reevaluate_command(&args),
		"adr594-reevaluate" => adr594_reevaluate_command(&args),
		"adr586-q21-semi-write" => adr586_q21_semi_write_command(&args),
		"adr587-q21-insert" => adr587_q21_insert_command(&args),
		"adr588-q21-second-walk" => adr588_q21_second_walk_command(&args),
		"adr589-q21-order-aware" => adr589_q21_order_aware_command(&args),
		"adr587-selfcheck-spin" => adr587_selfcheck_spin_command(),
		"adr449-gate-zero" => adr449_gate_zero_command(&args),
		"adr450-scale-ladder" => adr450_scale_ladder_command(&args),
		"adr448-capability" => {
			println!("adr448-harness-capability-v1");
			ExitCode::SUCCESS
		}
		"ladder" => ladder_command(&args).await,
		"fragment-census" => fragment_census_command(&args).await,
		"execution-census" => execution_census_command(&args).await,
		// ADR 581: produces a sealable measurement; only explicit seal-evidence grants authority.
		"timing-atlas" => timing_atlas_command(&args).await,
		// ADR 555: paired fallback-placement comparison over the same distributed workers.
		"placement-ab" => placement_ab_command(&args).await,
		"q17-census" => q17_census_command(&args).await,
		"q03-plan-proof" => q03_plan_proof_command(&args).await,
		"polars-q03-value-gate" => polars_q03_value_gate_command(&args),
		// ADR 273: the correctness rail — answer-check a query against an independent oracle.
		"correctness" => correctness_command(&args).await,
		"correctness-matrix" => {
			for row in correctness_matrix() {
				println!("{row}");
			}
			ExitCode::SUCCESS
		}
		_ => usage(),
	}
}

/// ADR 538: the matched-envelope timing atlas. Explicit-only — no other command produces an
/// authoritative artifact, and this one refuses to start on a non-quiescent host or an
/// existing destination. Protocol constants are compiled in and take no override.
async fn timing_atlas_command(args: &[String]) -> ExitCode {
	let scale_factor = match resolved_sf(args) {
		Ok(scale_factor) => scale_factor,
		Err(message) => return harness_error(&message),
	};
	let mode = flag_value(args, "--mode").unwrap_or("strict");
	let data_root = data_dir_from_args(args);
	let output = PathBuf::from(
		flag_value(args, "--output").unwrap_or("target/tpch/timing_atlas/timing_atlas.jsonl"),
	);
	for rejected in ["--pairs", "--warmups", "--runs", "--query"] {
		if flag_value(args, rejected).is_some() {
			return harness_error(&format!(
				"timing atlas takes no {rejected}: ADR 538 fixes the protocol and the cohort \
				 before measurement"
			));
		}
	}

	match toron_tpch::run_timing_atlas(mode, scale_factor, &data_root, &output).await {
		Ok(path) => {
			println!(
				"tpch_timing_atlas published={} final_authority=false pending=seal-evidence",
				path.display()
			);
			ExitCode::SUCCESS
		}
		Err(message) => harness_error(&message),
	}
}

/// ADR 555: compare controlled affinity-only serialization with load-observing placement.
/// This is deliberately distinct from `timing-atlas`: placement policy is its independent
/// variable, and a failed gate publishes a sealed withheld artifact instead of a verdict.
async fn placement_ab_command(args: &[String]) -> ExitCode {
	let scale_factor = match resolved_sf(args) {
		Ok(scale_factor) => scale_factor,
		Err(message) => return harness_error(&message),
	};
	let mode = flag_value(args, "--mode").unwrap_or("strict");
	let data_root = data_dir_from_args(args);
	let output = PathBuf::from(
		flag_value(args, "--output").unwrap_or("target/tpch/placement_ab/placement_ab.jsonl"),
	);
	for rejected in ["--pairs", "--warmups", "--runs", "--query"] {
		if flag_value(args, rejected).is_some() {
			return harness_error(&format!(
				"placement A/B takes no {rejected}: ADR 555 fixes the paired cohort and order before measurement"
			));
		}
	}
	match run_placement_ab(mode, scale_factor, &data_root, &output).await {
		Ok(run) => {
			println!(
				"tpch_placement_ab published={} final_authority={} evidence_candidate_authority={} withheld_reasons={:?}",
				run.path.display(),
				run.paired_timing_authority,
				run.evidence_candidate_authority,
				run.withheld_reasons,
			);
			ExitCode::SUCCESS
		}
		Err(message) => harness_error(&message),
	}
}

/// ADR 529: execute through the real coordinator/worker/TCP substrate and compare total output
/// identity with the local oracle. Honest capability walls are successful census outcomes; only a
/// harness failure prevents a row and exits nonzero.
async fn execution_census_command(args: &[String]) -> ExitCode {
	let scale_factor = match resolved_sf(args) {
		Ok(scale_factor) => scale_factor,
		Err(message) => return harness_error(&message),
	};
	let mode = flag_value(args, "--mode").unwrap_or("strict");
	let data_source = flag_value(args, "--data-source").unwrap_or("persisted");
	let data_root = data_dir_from_args(args);
	let output_dir =
		PathBuf::from(flag_value(args, "--output-dir").unwrap_or("target/tpch/execution_census"));
	let queries: Vec<&str> = match flag_value(args, "--query") {
		None | Some("all") => ALL_QUERY_IDS.to_vec(),
		Some(query) => vec![query],
	};
	let mut rows = Vec::new();
	for query in queries {
		let row =
			match run_execution_census_query(query, mode, scale_factor, data_source, &data_root)
				.await
			{
				Ok(row) => row,
				Err(message) => return harness_error(&message),
			};
		println!("{}", row.human());
		rows.push(row);
	}
	if let Err(error) = std::fs::create_dir_all(&output_dir) {
		return harness_error(&format!(
			"execution-census output dir {}: {error}",
			output_dir.display()
		));
	}
	let jsonl_path = output_dir.join("execution_census.jsonl");
	let jsonl = rows
		.iter()
		.map(ExecutionCensusRow::jsonl)
		.collect::<Vec<_>>()
		.join("\n");
	if let Err(error) = std::fs::write(&jsonl_path, jsonl + "\n") {
		return harness_error(&format!(
			"execution-census write {}: {error}",
			jsonl_path.display()
		));
	}
	let mut histogram: BTreeMap<String, usize> = BTreeMap::new();
	for row in &rows {
		let key = match &row.reason {
			Some(reason) => format!("{}:{reason}", row.classification),
			None => row.classification.clone(),
		};
		*histogram.entry(key).or_insert(0) += 1;
	}
	for (key, count) in &histogram {
		println!("execution_census_histogram {key}={count}");
	}
	let identical = rows
		.iter()
		.filter(|row| row.classification == "executes_identical")
		.count();
	let divergent = rows
		.iter()
		.filter(|row| row.classification == "executes_divergent")
		.count();
	println!(
		"execution_census_summary cells={} executes_identical={identical} executes_divergent={divergent} rows={}",
		rows.len(),
		jsonl_path.display()
	);
	ExitCode::SUCCESS
}

/// ADR 528: the fragment-capability census — bind q01-q22 under the distributed posture, ask
/// the fragmenter, and classify every cell from typed receipts. Plan-shape only: no workers,
/// no execution, no timing authority. The command exits nonzero only when a cell produced no
/// definitive classification (a harness error), never because the fragmenter said no — the
/// reject IS the finding.
async fn fragment_census_command(args: &[String]) -> ExitCode {
	let scale_factor = match resolved_sf(args) {
		Ok(scale_factor) => scale_factor,
		Err(message) => return harness_error(&message),
	};
	let mode = flag_value(args, "--mode").unwrap_or("strict");
	let data_source = flag_value(args, "--data-source").unwrap_or("memory");
	let data_root = data_dir_from_args(args);
	let output_dir =
		PathBuf::from(flag_value(args, "--output-dir").unwrap_or("target/tpch/fragment_census"));
	let queries: Vec<&str> = match flag_value(args, "--query") {
		None | Some("all") => ALL_QUERY_IDS.to_vec(),
		Some(query) => vec![query],
	};
	let mut rows = Vec::new();
	for query in queries {
		let row =
			match run_fragment_census_query(query, mode, scale_factor, data_source, &data_root)
				.await
			{
				Ok(row) => row,
				Err(message) => return harness_error(&message),
			};
		println!("{}", row.human());
		rows.push(row);
	}
	if let Err(error) = std::fs::create_dir_all(&output_dir) {
		return harness_error(&format!(
			"fragment-census output dir {}: {error}",
			output_dir.display()
		));
	}
	let jsonl_path = output_dir.join("fragment_census.jsonl");
	let jsonl = rows
		.iter()
		.map(FragmentCensusRow::jsonl)
		.collect::<Vec<_>>()
		.join("\n");
	if let Err(error) = std::fs::write(&jsonl_path, jsonl + "\n") {
		return harness_error(&format!(
			"fragment-census write {}: {error}",
			jsonl_path.display()
		));
	}
	let mut histogram: BTreeMap<String, usize> = BTreeMap::new();
	for row in &rows {
		let key = match &row.reason {
			Some(reason) => format!("{}:{reason}", row.classification),
			None => row.classification.clone(),
		};
		*histogram.entry(key).or_insert(0) += 1;
	}
	for (key, count) in &histogram {
		println!("fragment_census_histogram {key}={count}");
	}
	let parallel = rows
		.iter()
		.filter(|row| row.classification == "fragments_parallel")
		.count();
	println!(
		"fragment_census_summary cells={} fragments_parallel={parallel} rows={}",
		rows.len(),
		jsonl_path.display()
	);
	ExitCode::SUCCESS
}

async fn q17_census_command(args: &[String]) -> ExitCode {
	let scale_factor = match resolved_sf(args) {
		Ok(scale_factor) => scale_factor,
		Err(message) => return harness_error(&message),
	};
	let mode = flag_value(args, "--mode").unwrap_or("strict");
	if mode != "strict" {
		return harness_error("q17-census supports only --mode strict");
	}
	let data_source = flag_value(args, "--data-source").unwrap_or("memory");
	let data_root = data_dir_from_args(args);
	let census =
		match run_q17_boundary_census_with_data_source(scale_factor, data_source, &data_root).await
		{
			Ok(census) => census,
			Err(message) => return harness_error(&message),
		};
	let (rational, rounded) = census.receipt.totals();
	println!(
		"q17_boundary_census sf={scale_factor} outcome={} common_exact={} eligible_parts={} aggregate_parts={} selected_rows={} membership_difference_rows={}",
		census.outcome.as_str(),
		census.common_comparison_error.is_none(),
		census.eligible_parts,
		census.toron.part_aggregates.len(),
		census.toron.selected_rows.len(),
		census.receipt.membership_difference_rows,
	);
	println!(
		"q17_boundary_totals rational_below={} rational_equal={} rational_above={} rounded_below={} rounded_equal={} rounded_above={}",
		rational.below, rational.equal, rational.above, rounded.below, rounded.equal, rounded.above,
	);
	println!(
		"q17_boundary_fingerprints toron_aggregates={} oracle_aggregates={} toron_selected={} oracle_selected={}",
		census.toron_fingerprints.part_aggregates,
		census.oracle_fingerprints.part_aggregates,
		census.toron_fingerprints.selected_rows,
		census.oracle_fingerprints.selected_rows,
	);
	if let Some(error) = census.common_comparison_error {
		eprintln!("q17_boundary_difference {error}");
	}
	if census.outcome == Q17BoundaryCensusOutcome::Clean {
		ExitCode::SUCCESS
	} else {
		ExitCode::FAILURE
	}
}

async fn perf_run_one_command(args: &[String]) -> ExitCode {
	let (memory_per_task, memory_provenance) = match gauge_memory_per_task() {
		Ok(budget) => budget,
		Err(message) => return harness_error(&message),
	};
	let (spill_bytes_reserved, spill_provenance) = match gauge_spill_bytes_reserved() {
		Ok(budget) => budget,
		Err(message) => return harness_error(&message),
	};
	let (hash_smaller_build, hash_smaller_build_provenance) = match gauge_hash_smaller_build() {
		Ok(setting) => setting,
		Err(message) => return harness_error(&message),
	};
	let (filtered_build_side_swap, filtered_build_side_swap_provenance) =
		match gauge_filtered_build_side_swap() {
			Ok(setting) => setting,
			Err(message) => return harness_error(&message),
		};
	let (scalar_agg_input_reduction, scalar_agg_input_reduction_provenance) =
		match gauge_scalar_agg_input_reduction() {
			Ok(setting) => setting,
			Err(message) => return harness_error(&message),
		};
	let (membership_first, membership_first_provenance) = match gauge_membership_first() {
		Ok(setting) => setting,
		Err(message) => return harness_error(&message),
	};
	let (side_local_routing, side_local_routing_provenance) =
		match gauge_side_local_filter_routing() {
			Ok(setting) => setting,
			Err(message) => return harness_error(&message),
		};
	let (semi_filtered_reachable_keys, semi_filtered_reachable_keys_provenance) =
		match gauge_semi_filtered_reachable_keys() {
			Ok(setting) => setting,
			Err(message) => return harness_error(&message),
		};
	let (outer_count_preaggregation, outer_count_preaggregation_provenance) =
		match gauge_outer_count_preaggregation() {
			Ok(setting) => setting,
			Err(message) => return harness_error(&message),
		};
	let query = flag_value(args, "--query").unwrap_or("q06");
	let mode = flag_value(args, "--mode").unwrap_or("strict");
	let data_source = flag_value(args, "--data-source").unwrap_or("memory");
	let data_dir = data_dir_from_args(args);
	let explicit_scale = match explicit_sf(args) {
		Ok(explicit) => explicit,
		Err(message) => return harness_error(&message),
	};
	let sf = explicit_scale.unwrap_or(1.0);
	let run_index = match parse_u32_flag(args, "--run-index", 0) {
		Ok(run_index) => run_index,
		Err(message) => return harness_error(&message),
	};
	let warmup = args.iter().any(|arg| arg == "--warmup-run");
	let engine = match parse_perf_engine(flag_value(args, "--engine").unwrap_or("toron")) {
		Ok(engine) => engine,
		Err(message) => return harness_error(&message),
	};
	let partition_work_concurrency = if engine == PerfEngine::Toron {
		match gauge_partition_work_concurrency_requested() {
			Ok(setting) => Some(setting),
			Err(message) => return harness_error(&message),
		}
	} else {
		None
	};
	if engine == PerfEngine::Rune {
		if let Some(raw_iterations) = std::env::var_os("TORON_TPCH_PROFILE_LOOP") {
			let iterations = match raw_iterations.to_string_lossy().parse::<usize>() {
				Ok(iterations) if iterations > 0 => iterations,
				Ok(_) => return harness_error("TORON_TPCH_PROFILE_LOOP must be > 0"),
				Err(error) => {
					return harness_error(&format!(
						"TORON_TPCH_PROFILE_LOOP must be a positive integer: {error}"
					));
				}
			};
			if query != "q03" && query != "q3" {
				return harness_error("TORON_TPCH_PROFILE_LOOP supports only --query q03");
			}
			let summary = match run_rune_q03_profile_loop_with_data_source(
				mode,
				sf,
				data_source,
				&data_dir,
				iterations,
			)
			.await
			{
				Ok(summary) => summary,
				Err(message) => return harness_error(&message),
			};
			println!(
				"{}",
				serde_json::json!({
					"engine": "rune",
					"query": "q03",
					"scale_factor": sf,
					"mode": mode,
					"profile_loop_iterations": summary.iterations,
					"prepare_ms": summary.prepare_ms,
					"warmup_execute_ms": summary.warmup_execute_ms,
					"loop_execute_ms": summary.loop_execute_ms,
					"iteration_execute_ms": summary.iteration_execute_ms,
					"plan_fingerprint": summary.plan_fingerprint,
					"spill_bytes_max": summary.spill_bytes_max,
					"output_row_count": summary.output_row_count,
					"output_schema_fingerprint": summary.output_schema_fingerprint,
					"join_build_profiles": summary.join_build_profiles,
					"parquet_scan_ledger": summary.parquet_scan_ledger,
					"materialized_path": summary.spill_bytes_max == 0,
					"debug_build": cfg!(debug_assertions),
				})
			);
			return ExitCode::SUCCESS;
		}
	}
	// Clear any prior phase ledger before the run so the row can only ever attach THIS run's file
	// (or none). The engine's scope guard always rewrites it when the fused path is armed, but an
	// unarmed shape writes nothing — without this removal that would attach a stale prior run's
	// identity-bearing ledger.
	if let Some(path) = std::env::var_os("TORON_LOCAL_JOIN_PHASE_PROFILE_PATH") {
		match std::fs::remove_file(&path) {
			Ok(()) => {}
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
			Err(error) => {
				return harness_error(&format!("clear local join phase ledger {path:?}: {error}"));
			}
		}
	}
	if let Some(path) = std::env::var_os("TORON_LOCAL_GROUPED_EXCHANGE_RECEIPT_PATH") {
		match std::fs::remove_file(&path) {
			Ok(()) => {}
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
			Err(error) => {
				return harness_error(&format!("clear local grouped exchange receipt: {error}"));
			}
		}
	}
	// Same removal for the node-attribution profile, and for the same reason with an extra edge: the
	// profiler's write is best-effort (its errors are swallowed so a diagnostic failure cannot mask a
	// query result), so a run that failed to emit would otherwise attach the previous run's profile.
	// A stale same-query profile would even reconcile inside the boundary — a silent false pass.
	if let Some(path) = std::env::var_os("TORON_NODE_ATTRIBUTION_PROFILE_PATH") {
		match std::fs::remove_file(&path) {
			Ok(()) => {}
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
			Err(error) => {
				return harness_error(&format!("clear node attribution profile {path:?}: {error}"));
			}
		}
	}
	if let Some(path) = std::env::var_os("TORON_CHAIN_STAGE_PROFILE_PATH") {
		match std::fs::remove_file(&path) {
			Ok(()) => {}
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
			Err(error) => {
				return harness_error(&format!("clear chain-stage profile {path:?}: {error}"));
			}
		}
	}
	// ADR 593: every perf child brackets its execute with host readings and puts them on the row.
	toron_tpch::host_readings::enable();
	let attempt_result = match engine {
		PerfEngine::Toron => {
			run_perf_query_with_data_source(
				query,
				mode,
				sf,
				run_index,
				warmup,
				data_source,
				&data_dir,
			)
			.await
		}
		PerfEngine::Rune => {
			run_rune_perf_query_with_data_source(
				query,
				mode,
				sf,
				run_index,
				warmup,
				data_source,
				&data_dir,
			)
			.await
		}
		PerfEngine::DuckDb => {
			Err("perf-run-one does not run duckdb; use perf --engines duckdb".to_string())
		}
		PerfEngine::PolarsSql => run_polars_perf_query_with_data_source(
			query,
			mode,
			sf,
			run_index,
			warmup,
			data_source,
			&data_dir,
		),
	};
	let attempt = match attempt_result {
		Ok(attempt) => attempt,
		Err(message) => return harness_error(&message),
	};
	let mut row: serde_json::Value = match serde_json::from_str(&attempt.jsonl()) {
		Ok(row) => row,
		Err(error) => return harness_error(&format!("perf row is not JSON: {error}")),
	};
	row["memory_per_task"] = serde_json::json!(memory_per_task);
	row["memory_per_task_provenance"] = serde_json::json!(memory_provenance);
	row["spill_bytes_reserved"] = serde_json::json!(spill_bytes_reserved);
	row["spill_bytes_reserved_provenance"] = serde_json::json!(spill_provenance);
	row["host_readings"] = match toron_tpch::host_readings::take_last() {
		Some(pair) => pair.json_value(),
		None => serde_json::Value::Null,
	};
	row["hash_smaller_build"] = serde_json::json!(hash_smaller_build);
	row["hash_smaller_build_provenance"] = serde_json::json!(hash_smaller_build_provenance);
	row["filtered_build_side_swap"] = serde_json::json!(filtered_build_side_swap);
	row["filtered_build_side_swap_provenance"] =
		serde_json::json!(filtered_build_side_swap_provenance);
	row["scalar_aggregate_input_reduction"] = serde_json::json!(scalar_agg_input_reduction);
	row["scalar_aggregate_input_reduction_provenance"] =
		serde_json::json!(scalar_agg_input_reduction_provenance);
	row["membership_first_composition"] = serde_json::json!(membership_first);
	row["membership_first_composition_provenance"] = serde_json::json!(membership_first_provenance);
	row["side_local_filter_routing"] = serde_json::json!(side_local_routing);
	row["side_local_filter_routing_provenance"] = serde_json::json!(side_local_routing_provenance);
	// The data source this child was asked to read, so a paired driver can bind every arm to one.
	row["data_source"] = serde_json::json!(data_source);
	row["semi_filtered_reachable_keys"] = serde_json::json!(semi_filtered_reachable_keys);
	row["semi_filtered_reachable_keys_provenance"] =
		serde_json::json!(semi_filtered_reachable_keys_provenance);
	row["outer_count_preaggregation"] = serde_json::json!(outer_count_preaggregation);
	row["outer_count_preaggregation_provenance"] =
		serde_json::json!(outer_count_preaggregation_provenance);
	add_partition_work_concurrency_fields(&mut row, partition_work_concurrency);
	if std::env::var_os("TORON_TPCH_PARQUET_SCAN_LEDGER").is_some() {
		let ledger = take_parquet_scan_ledger();
		let batches = ledger.iter().flat_map(|scan| scan.batches.iter());
		let (batches, rows, decode_micros, raw_bytes) = batches.fold(
			(0u64, 0u64, 0u64, 0u64),
			|(batches, rows, decode_micros, raw_bytes), batch| {
				(
					batches.saturating_add(1),
					rows.saturating_add(u64::try_from(batch.raw_rows).unwrap_or(u64::MAX)),
					decode_micros.saturating_add(batch.parquet_decode_micros),
					raw_bytes.saturating_add(u64::try_from(batch.raw_bytes).unwrap_or(u64::MAX)),
				)
			},
		);
		row["parquet_scan_receipt"] = serde_json::json!({
			"scans": ledger.len(), "batches": batches, "rows": rows,
			"decode_micros": decode_micros, "raw_bytes": raw_bytes,
		});
	}
	// Diagnostic: when TORON_LOCAL_JOIN_PHASE_PROFILE_PATH is set the engine writes one phase
	// ledger for the local in-memory inner-join drive loop. Fold it into this row so the buckets
	// travel alongside the identity/receipt fields (build_commit, plan_fingerprint, digest,
	// output_row_count, local_join_admissions, spill_bytes, join_spill_observations) already here.
	if let Some(path) = std::env::var_os("TORON_LOCAL_JOIN_PHASE_PROFILE_PATH") {
		match std::fs::read_to_string(&path) {
			Ok(contents) => match serde_json::from_str::<serde_json::Value>(&contents) {
				Ok(ledger) => row["local_join_phase_ledger"] = ledger,
				Err(error) => {
					row["local_join_phase_ledger_error"] =
						serde_json::json!(format!("parse: {error}"));
				}
			},
			// The path was cleared before the run; an absent file simply means this query shape did
			// not arm the ledger — not an error. Only surface genuine read failures on a real file.
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
			Err(error) => {
				row["local_join_phase_ledger_error"] = serde_json::json!(format!("read: {error}"));
			}
		}
	}
	if let Some(path) = std::env::var_os("TORON_LOCAL_GROUPED_EXCHANGE_RECEIPT_PATH") {
		match std::fs::read_to_string(&path) {
			Ok(contents) => match serde_json::from_str::<serde_json::Value>(&contents) {
				Ok(receipt) => row["local_grouped_exchange_receipt"] = receipt,
				Err(error) => {
					row["local_grouped_exchange_receipt_error"] =
						serde_json::json!(format!("parse: {error}"))
				}
			},
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
			Err(error) => {
				row["local_grouped_exchange_receipt_error"] =
					serde_json::json!(format!("read: {error}"))
			}
		}
	}
	// ADR 507: join the node-attribution profile to this perf row. The profiler emitted the in-tree
	// reconciliation; here — the one place holding both the profile and execute_ms — the outer band
	// (root span ⊆ execute_ms) is reconciled against its declared boundary and folded in beside the
	// profile, so the row carries a complete two-band attribution.
	if let Some(path) = std::env::var_os("TORON_NODE_ATTRIBUTION_PROFILE_PATH") {
		match std::fs::read_to_string(&path) {
			Ok(contents) => match serde_json::from_str::<serde_json::Value>(&contents) {
				Ok(profile) => {
					// NaN, not 0.0, when the row carries no execute_ms: the reconciliation must fail
					// closed naming its real cause, not default to a zero that reads as an inverted band.
					let execute_ms = row
						.get("execute_ms")
						.and_then(serde_json::Value::as_f64)
						.unwrap_or(f64::NAN);
					row["node_attribution_outer_reconciliation"] =
						node_attribution_outer_reconciliation(&profile, execute_ms);
					row["node_attribution_profile_receipt_path"] =
						serde_json::json!(Path::new(&path).display().to_string());
					row["node_attribution"] = profile;
				}
				Err(error) => {
					row["node_attribution_error"] = serde_json::json!(format!("parse: {error}"));
				}
			},
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
			Err(error) => {
				row["node_attribution_error"] = serde_json::json!(format!("read: {error}"));
			}
		}
	}
	if let Some(path) = std::env::var_os("TORON_CHAIN_STAGE_PROFILE_PATH") {
		if let Err(message) = attach_chain_stage_profile(&mut row, Path::new(&path)) {
			return harness_error(&message);
		}
	}
	if toron::exec::join_replay_input_diagnostic_enabled()
		&& let Err(message) = validate_targeted_local_join_diagnostic(&row)
	{
		return harness_error(&message);
	}
	row["diagnostics_disabled"] = serde_json::json!(join_diagnostics_disabled());
	println!("{row}");
	ExitCode::SUCCESS
}

fn attach_chain_stage_profile(row: &mut serde_json::Value, path: &Path) -> Result<(), String> {
	let contents = std::fs::read_to_string(path)
		.map_err(|error| format!("read chain-stage profile {}: {error}", path.display()))?;
	let mut complete = Vec::new();
	for (line_index, line) in contents.lines().enumerate() {
		if line.trim().is_empty() {
			continue;
		}
		let value: serde_json::Value = serde_json::from_str(line).map_err(|error| {
			format!(
				"parse chain-stage profile {} line {}: {error}",
				path.display(),
				line_index + 1
			)
		})?;
		if value
			.get("drive_completed")
			.and_then(serde_json::Value::as_bool)
			== Some(true)
		{
			complete.push(value);
		}
	}
	let query_ids = complete
		.iter()
		.filter_map(|value| value.get("query_id").and_then(serde_json::Value::as_str))
		.collect::<std::collections::BTreeSet<_>>();
	if query_ids.len() != 1 {
		return Err(format!(
			"chain-stage profile {} must contain one completed query identity, found {}",
			path.display(),
			query_ids.len()
		));
	}
	let final_snapshot = complete
		.into_iter()
		.max_by_key(|value| {
			value
				.get("sequence")
				.and_then(serde_json::Value::as_u64)
				.unwrap_or(0)
		})
		.ok_or_else(|| {
			format!(
				"chain-stage profile {} has no completed snapshot",
				path.display()
			)
		})?;
	row["chain_stage_profile_receipt_path"] = serde_json::json!(path.display().to_string());
	row["chain_stage_profile"] = final_snapshot;
	Ok(())
}

fn validate_targeted_local_join_diagnostic(row: &serde_json::Value) -> Result<(), String> {
	let stripped = row
		.get("local_join_admissions")
		.and_then(serde_json::Value::as_array)
		.ok_or_else(|| {
			"join replay diagnostic requires the ordinary local-admission receipts".to_string()
		})?;
	let targeted = row
		.get("targeted_local_join_admissions")
		.and_then(serde_json::Value::as_array)
		.ok_or_else(|| {
			"join replay diagnostic refuses stripped local-admission receipts".to_string()
		})?;
	if stripped.len() != targeted.len() {
		return Err(format!(
			"join replay diagnostic receipt count mismatch: stripped={} targeted={}",
			stripped.len(),
			targeted.len()
		));
	}
	let mut replay_ids = BTreeSet::new();
	for (position, (plain, receipt)) in stripped.iter().zip(targeted).enumerate() {
		let replay_id = receipt
			.get("replay_id")
			.and_then(serde_json::Value::as_u64)
			.ok_or_else(|| {
				format!("targeted local-admission receipt {position} lacks replay_id")
			})?;
		if !replay_ids.insert(replay_id) {
			return Err(format!(
				"targeted local-admission receipt {position} repeats replay {replay_id}"
			));
		}
		let verdict = receipt
			.get("verdict")
			.and_then(serde_json::Value::as_str)
			.ok_or_else(|| format!("targeted local-admission receipt {position} lacks verdict"))?;
		if plain.as_str() != Some(verdict) {
			return Err(format!(
				"targeted local-admission receipt {position} verdict disagrees with stripped receipt"
			));
		}
	}
	let ledger = row
		.get("join_input_replay_ledger")
		.and_then(serde_json::Value::as_array)
		.ok_or_else(|| "join replay diagnostic lacks its per-replay input ledger".to_string())?;
	let recorded_concats = ledger.iter().try_fold(0u64, |total, replay| {
		["left", "right"]
			.into_iter()
			.try_fold(total, |total, side| {
				let concats = replay
					.get(side)
					.and_then(|value| value.get("shape"))
					.and_then(|value| value.get("concats"))
					.and_then(serde_json::Value::as_u64)
					.ok_or_else(|| format!("join replay diagnostic lacks {side} concat count"))?;
				total
					.checked_add(concats)
					.ok_or_else(|| "join replay diagnostic concat count overflow".to_string())
			})
	})?;
	let total_concats = row
		.get("join_input_total_concats")
		.and_then(serde_json::Value::as_u64)
		.ok_or_else(|| "join replay diagnostic lacks the independent concat total".to_string())?;
	if recorded_concats != total_concats {
		return Err(format!(
			"join replay diagnostic is incomplete: replay ledger records {recorded_concats} concats but shared primitive observed {total_concats}"
		));
	}
	Ok(())
}

const ADR584_PAIRS: usize = 5;
const ADR584_BASELINE_EXECUTABLE: &str =
	"b436a843b107b2f385d5e7c2486c41b1d1f89acfdb4af75f8a4793c823a05ee4";
const ADR584_STAGE_NAMES: [&str; 7] = [
	"base_scan",
	"inner_replay_0",
	"inner_replay_1",
	"inner_replay_2",
	"semi_replay_3",
	"anti_replay_4",
	"grouped_fold",
];

fn adr584_q21_attribution_command(args: &[String]) -> ExitCode {
	match run_adr584_q21_attribution(args) {
		Ok(summary) => {
			println!("{summary}");
			ExitCode::SUCCESS
		}
		Err(message) => harness_error(&message),
	}
}

fn run_adr584_q21_attribution(args: &[String]) -> Result<serde_json::Value, String> {
	let mode = flag_value(args, "--mode").unwrap_or("strict");
	let sf = resolved_sf(args)?;
	let data_source = flag_value(args, "--data-source").unwrap_or("parquet");
	let data_dir = data_dir_from_args(args);
	let output_dir = PathBuf::from(
		flag_value(args, "--output-dir").unwrap_or("target/tpch/adr584-q21-attribution"),
	);
	if mode != "strict" || sf != 8.0 || !matches!(data_source, "parquet" | "persisted") {
		return Err(format!(
			"ADR 584 requires --mode strict --sf 8 --data-source parquet; got mode={mode:?} sf={sf} data_source={data_source:?}"
		));
	}
	if output_dir.exists() {
		return Err(format!(
			"ADR 584 requires a fresh output directory; {} already exists",
			output_dir.display()
		));
	}
	for name in [
		"TORON_CHAIN_STAGE_PROFILE_PATH",
		toron::exec::JOIN_REPLAY_INPUT_DIAGNOSTIC_ENV,
	] {
		if std::env::var_os(name).is_some() {
			return Err(format!(
				"ADR 584 parent environment must not set {name}; the driver owns both arms"
			));
		}
	}
	let budget = GaugeBudget::from_environment()?;
	if budget.memory_per_task != 2_147_483_648
		|| budget.memory_per_task_provenance != "explicit"
		|| budget.spill_bytes_reserved != 2_147_483_648
		|| budget.spill_bytes_reserved_provenance != "explicit"
	{
		return Err("ADR 584 requires explicit 2 GiB task memory and spill reserve".to_string());
	}
	let concurrency = gauge_partition_work_concurrency_requested()?;
	if concurrency != (1, "explicit") {
		return Err(format!(
			"ADR 584 requires explicit partition parallelism 1, got {concurrency:?}"
		));
	}
	let identity = runtime_identity()?.ok_or_else(|| {
		"ADR 584 must run through toron-bench build/run with a staged executable".to_string()
	})?;
	if identity.manifest.recipe.profile != "release" {
		return Err(format!(
			"ADR 584 requires a release build, got {:?}",
			identity.manifest.recipe.profile
		));
	}
	let host_start = observe_host_quiescence();
	if !host_start.quiescent() {
		return Err("ADR 584 requires the local deployment and SQL port quiescent".to_string());
	}
	let scratch = std::env::var_os("TORON_JOIN_SPILL_DIR")
		.filter(|value| !value.is_empty())
		.map(PathBuf::from)
		.unwrap_or_else(|| std::env::temp_dir().join("toron_join_spill"));
	if !directory_is_empty(&scratch)? {
		return Err(format!(
			"ADR 584 spill scratch {} is not empty before the run",
			scratch.display()
		));
	}
	let dataset = load_persisted_tpch_dataset(mode, sf, &data_dir)?;
	if dataset.verification.as_str() != "full_hash" {
		return Err(format!(
			"ADR 584 requires full-hash dataset verification, got {}",
			dataset.verification.as_str()
		));
	}
	// ADR 592: the coordinate is fixed, and bound to the sealed baseline when one is named,
	// before the first child; every child, the warmup included, is validated as it returns.
	let coordinate = adr592_driver_coordinate(
		"ADR 584",
		args,
		&identity,
		&dataset.manifest_fingerprint,
		data_source,
	)?;
	let journal = prepare_perf_sample_journal(&output_dir)?;
	std::fs::create_dir_all(output_dir.join("profiles"))
		.map_err(|error| format!("create ADR 584 profile directory: {error}"))?;

	let (warmup, rows) = adr592_run_paired_children(
		("adr584_arm", "adr584_pair", "adr584_pair_position"),
		ADR584_PAIRS,
		&coordinate,
		&journal,
		|run_index, warmup, on| {
			run_adr584_child(
				run_index,
				warmup,
				on,
				mode,
				sf,
				data_source,
				&data_dir,
				&output_dir,
			)
		},
	)?;
	let host_end = observe_host_quiescence();
	if !host_end.quiescent() || !directory_is_empty(&scratch)? {
		return Err(
			"ADR 584 lost host quiescence or left spill scratch after measurement".to_string(),
		);
	}
	let mut summary = evaluate_adr584_pairs(&rows)?;
	summary["kind"] = serde_json::json!("adr584_q21_attribution");
	summary["sealed"] = serde_json::json!(false);
	summary["candidate_authority"] = serde_json::json!(false);
	summary["sealed_baseline_executable_sha256"] = serde_json::json!(ADR584_BASELINE_EXECUTABLE);
	let off_median = summary
		.get("off_execute_median_ms")
		.and_then(serde_json::Value::as_f64);
	adr592_stamp_summary(&mut summary, &coordinate, off_median);
	summary["instrumented_evidence_identity"] = identity.json_value();
	summary["dataset_manifest_fingerprint"] = serde_json::json!(dataset.manifest_fingerprint);
	summary["memory_per_task"] = serde_json::json!(budget.memory_per_task);
	summary["spill_bytes_reserved"] = serde_json::json!(budget.spill_bytes_reserved);
	summary["partition_work_concurrency_requested"] = serde_json::json!(concurrency.0);
	summary["warmup_execute_ms"] = warmup.get("execute_ms").cloned().unwrap_or_default();
	write_json_atomically(&output_dir.join("summary.json"), &summary)?;
	Ok(summary)
}

#[allow(clippy::too_many_arguments)]
fn run_adr584_child(
	run_index: u32,
	warmup: bool,
	on: bool,
	mode: &str,
	sf: f64,
	data_source: &str,
	data_dir: &Path,
	output_dir: &Path,
) -> Result<serde_json::Value, String> {
	let mut args = vec![
		"perf-run-one".to_string(),
		"--engine".to_string(),
		"toron".to_string(),
		"--query".to_string(),
		"q21".to_string(),
		"--mode".to_string(),
		mode.to_string(),
		"--sf".to_string(),
		sf.to_string(),
		"--run-index".to_string(),
		run_index.to_string(),
		"--data-source".to_string(),
		data_source.to_string(),
		"--data-dir".to_string(),
		data_dir.display().to_string(),
	];
	if warmup {
		args.push("--warmup-run".to_string());
	}
	let profile = output_dir
		.join("profiles")
		.join(format!("pair-{run_index:02}-on.jsonl"));
	let profile_text = profile
		.to_str()
		.ok_or_else(|| "ADR 584 profile path is not UTF-8".to_string())?;
	let row = if on {
		run_perf_child_args_with_envs(
			&args,
			&[
				("TORON_CHAIN_STAGE_PROFILE_PATH", profile_text),
				(toron::exec::JOIN_REPLAY_INPUT_DIAGNOSTIC_ENV, "1"),
			],
		)?
	} else {
		run_perf_child_args(&args)?
	};
	// ADR 592: a non-passing row is returned as it is; the shared validator refuses it and the
	// driver journals the refusal beside the row before stopping.
	Ok(row)
}

fn required_f64(row: &serde_json::Value, field: &str) -> Result<f64, String> {
	row.get(field)
		.and_then(serde_json::Value::as_f64)
		.filter(|value| value.is_finite())
		.ok_or_else(|| format!("ADR 584 row lacks finite {field}"))
}

fn required_u64_value(row: &serde_json::Value, field: &str) -> Result<u64, String> {
	row.get(field)
		.and_then(serde_json::Value::as_u64)
		.ok_or_else(|| format!("ADR 584 receipt lacks integer {field}"))
}

fn evaluate_adr584_pairs(rows: &[serde_json::Value]) -> Result<serde_json::Value, String> {
	if rows.len() != ADR584_PAIRS * 2 {
		return Err(format!(
			"ADR 584 requires {} measured rows, found {}",
			ADR584_PAIRS * 2,
			rows.len()
		));
	}
	let first = rows
		.first()
		.ok_or_else(|| "ADR 584 has no rows".to_string())?;
	for row in rows {
		let identity = toron_tpch::evidence::compare_row_identities(first, row);
		if !identity.would_admit {
			return Err(format!(
				"ADR 584 arms differ in identity: {:?}",
				identity.reasons
			));
		}
		if row.get("query").and_then(serde_json::Value::as_str) != Some("q21")
			|| row.get("status").and_then(serde_json::Value::as_str) != Some("pass")
		{
			return Err("ADR 584 accepts only passing q21 rows".to_string());
		}
	}
	let mut pair_records = Vec::new();
	let mut movements = Vec::new();
	let mut off_execute = Vec::new();
	let mut profile_receipt_paths = BTreeSet::new();
	let mut term_samples = ADR584_STAGE_NAMES
		.iter()
		.chain(["in_drive_residual", "outside_drive"].iter())
		.map(|name| ((*name).to_string(), Vec::<f64>::new()))
		.collect::<BTreeMap<_, _>>();
	for pair in 0..ADR584_PAIRS {
		let pair_rows = rows
			.iter()
			.filter(|row| {
				row.get("adr584_pair").and_then(serde_json::Value::as_u64) == Some(pair as u64)
			})
			.collect::<Vec<_>>();
		let off = pair_rows
			.iter()
			.find(|row| row.get("adr584_arm").and_then(serde_json::Value::as_str) == Some("off"))
			.ok_or_else(|| format!("ADR 584 pair {pair} lacks off arm"))?;
		let on = pair_rows
			.iter()
			.find(|row| row.get("adr584_arm").and_then(serde_json::Value::as_str) == Some("on"))
			.ok_or_else(|| format!("ADR 584 pair {pair} lacks on arm"))?;
		let expected_order = if pair % 2 == 0 {
			[("off", 0), ("on", 1)]
		} else {
			[("on", 0), ("off", 1)]
		};
		for (arm, position) in expected_order {
			let observed = pair_rows
				.iter()
				.find(|row| row.get("adr584_arm").and_then(serde_json::Value::as_str) == Some(arm));
			if observed
				.and_then(|row| row.get("adr584_pair_position"))
				.and_then(serde_json::Value::as_u64)
				!= Some(position)
			{
				return Err(format!(
					"ADR 584 pair {pair} does not use its declared alternating order"
				));
			}
		}
		if off
			.get("diagnostics_disabled")
			.and_then(serde_json::Value::as_bool)
			!= Some(true)
			|| off.get("chain_stage_profile").is_some()
			|| off.get("targeted_local_join_admissions").is_some()
			|| off.get("join_input_replay_ledger").is_some()
			|| off.get("join_input_total_concats").is_some()
		{
			return Err(format!(
				"ADR 584 pair {pair} off arm is not diagnostic-free"
			));
		}
		if on
			.get("diagnostics_disabled")
			.and_then(serde_json::Value::as_bool)
			!= Some(false)
		{
			return Err(format!(
				"ADR 584 pair {pair} on arm did not void diagnostic authority"
			));
		}
		let execute_off_ms = required_f64(off, "execute_ms")?;
		let execute_on_ms = required_f64(on, "execute_ms")?;
		if execute_off_ms <= 0.0 || execute_on_ms <= 0.0 {
			return Err(format!("ADR 584 pair {pair} has non-positive execute time"));
		}
		let movement = (execute_on_ms - execute_off_ms) / execute_off_ms;
		movements.push(movement.abs());
		off_execute.push(execute_off_ms);
		let profile = on
			.get("chain_stage_profile")
			.ok_or_else(|| format!("ADR 584 pair {pair} lacks chain profile"))?;
		let profile_receipt_path = on
			.get("chain_stage_profile_receipt_path")
			.and_then(serde_json::Value::as_str)
			.ok_or_else(|| format!("ADR 584 pair {pair} lacks its profile receipt path"))?;
		if !profile_receipt_paths.insert(profile_receipt_path.to_string()) {
			return Err(format!(
				"ADR 584 pair {pair} reuses profile receipt path {profile_receipt_path:?}"
			));
		}
		let total_drive = required_u64_value(profile, "total_drive_micros")?;
		let residual = required_u64_value(profile, "unattributed_residual_micros")?;
		let mut stage_micros = BTreeMap::new();
		for stage in profile
			.get("stages")
			.and_then(serde_json::Value::as_array)
			.ok_or_else(|| format!("ADR 584 pair {pair} lacks stages"))?
		{
			let name = stage
				.get("stage")
				.and_then(serde_json::Value::as_str)
				.ok_or_else(|| format!("ADR 584 pair {pair} has unnamed stage"))?;
			stage_micros.insert(name.to_string(), required_u64_value(stage, "micros")?);
		}
		let stage_sum = ADR584_STAGE_NAMES.iter().try_fold(0u64, |sum, name| {
			stage_micros
				.get(*name)
				.copied()
				.ok_or_else(|| format!("ADR 584 pair {pair} lacks stage {name}"))
				.and_then(|value| {
					sum.checked_add(value)
						.ok_or_else(|| "ADR 584 stage sum overflow".to_string())
				})
		})?;
		if stage_sum.checked_add(residual) != Some(total_drive) {
			return Err(format!(
				"ADR 584 pair {pair} does not reconcile: stages={stage_sum} residual={residual} drive={total_drive}"
			));
		}
		let execute_on_us = execute_on_ms * 1000.0;
		let outside_drive = execute_on_us - total_drive as f64;
		let outside_drive_non_negative = outside_drive >= 0.0;
		let overhead = (execute_on_ms - execute_off_ms).max(0.0) * 1000.0;
		let residual_bound = overhead + 0.05 * total_drive as f64;
		let complete = residual as f64 <= residual_bound;
		for name in ADR584_STAGE_NAMES {
			let micros = stage_micros[name] as f64;
			term_samples
				.get_mut(name)
				.expect("stage universe was initialized")
				.push(micros / execute_on_us * execute_off_ms);
		}
		term_samples
			.get_mut("in_drive_residual")
			.expect("residual term")
			.push(residual as f64 / execute_on_us * execute_off_ms);
		term_samples
			.get_mut("outside_drive")
			.expect("outside term")
			.push(outside_drive / execute_on_us * execute_off_ms);
		let targeted = on
			.get("targeted_local_join_admissions")
			.and_then(serde_json::Value::as_array)
			.ok_or_else(|| format!("ADR 584 pair {pair} lacks replay-identified admissions"))?;
		if targeted.len() != 3 {
			return Err(format!(
				"ADR 584 pair {pair} expected three replay-identified admissions, found {}",
				targeted.len()
			));
		}
		let replay_ledger = on
			.get("join_input_replay_ledger")
			.and_then(serde_json::Value::as_array)
			.ok_or_else(|| format!("ADR 584 pair {pair} lacks replay ledger"))?;
		let total_concats = required_u64_value(on, "join_input_total_concats")?;
		pair_records.push(serde_json::json!({
			"pair": pair,
			"execute_off_ms": execute_off_ms,
			"execute_on_ms": execute_on_ms,
			"movement": movement,
			"total_drive_micros": total_drive,
			"stage_micros": stage_micros,
			"in_drive_residual_micros": residual,
			"residual_bound_micros": residual_bound,
			"profile_complete": complete && outside_drive_non_negative,
			"residual_complete": complete,
			"outside_drive_non_negative": outside_drive_non_negative,
			"outside_drive_micros": outside_drive,
			"targeted_local_join_admissions": targeted,
			"join_input_replay_ledger": replay_ledger,
			"join_input_total_concats": total_concats,
			"profile_receipt_path": profile_receipt_path,
		}));
	}
	let median_abs_movement = median(&mut movements);
	let perturbation_accepted = median_abs_movement <= 0.25;
	let all_profiles_complete = pair_records
		.iter()
		.all(|pair| pair["profile_complete"] == true);
	let off_min = off_execute.iter().copied().fold(f64::INFINITY, f64::min);
	let off_max = off_execute
		.iter()
		.copied()
		.fold(f64::NEG_INFINITY, f64::max);
	let off_range = off_max - off_min;
	let off_median = median(&mut off_execute);
	let owner_floor = 500.0f64.max(2.0 * off_range);
	let mut medians = term_samples
		.into_iter()
		.map(|(name, mut values)| (name, median(&mut values)))
		.collect::<Vec<_>>();
	medians.sort_by(|left, right| right.1.total_cmp(&left.1));
	let candidates = medians
		.iter()
		.filter(|(_, value)| *value > owner_floor)
		.map(|(name, value)| {
			serde_json::json!({
				"term": name,
				"median_scaled_ms": value,
				"share_of_off_execute": value / off_median,
			})
		})
		.collect::<Vec<_>>();
	let instrument_valid = pair_records
		.iter()
		.all(|pair| pair["outside_drive_non_negative"] == true);
	let owner = if !instrument_valid {
		serde_json::json!({"status": "withheld_instrument_defect"})
	} else if !perturbation_accepted {
		serde_json::json!({"status": "withheld_perturbation_rejected"})
	} else if !all_profiles_complete {
		serde_json::json!({"status": "withheld_incomplete_profiles"})
	} else {
		match (medians.first(), medians.get(1)) {
			(Some((name, value)), Some((_, second)))
				if *value > owner_floor && value - second > off_range =>
			{
				serde_json::json!({
					"status": "resolved",
					"term": name,
					"median_scaled_ms": value,
					"share_of_off_execute": value / off_median,
				})
			}
			(Some((name, value)), _) if *value > owner_floor => serde_json::json!({
				"status": "unresolved_tie_within_off_range",
				"leading_term": name,
				"median_scaled_ms": value,
				"share_of_off_execute": value / off_median,
			}),
			_ => serde_json::json!({"status": "no_term_clears_floor"}),
		}
	};
	Ok(serde_json::json!({
		"pair_count": ADR584_PAIRS,
		"pairs": pair_records,
		"median_absolute_movement": median_abs_movement,
		"perturbation_gate": if perturbation_accepted { "accepted" } else { "rejected" },
		"all_profiles_complete": all_profiles_complete,
		"instrument_valid": instrument_valid,
		"off_execute_median_ms": off_median,
		"off_execute_range_ms": off_range,
		"off_execute_relative_spread": off_range / off_median,
		"owner_floor_ms": owner_floor,
		"term_medians_scaled_ms": medians.into_iter().map(|(term, value)| serde_json::json!({"term": term, "median_scaled_ms": value})).collect::<Vec<_>>(),
		"candidate_terms": candidates,
		"owner": owner,
	}))
}

const ADR585_PAIRS: usize = 5;
const ADR585_SEALED_BASELINE_EXECUTABLE: &str =
	"b436a843b107b2f385d5e7c2486c41b1d1f89acfdb4af75f8a4793c823a05ee4";
const ADR585_ADR584_EXECUTABLE: &str =
	"a173e8252a3f37839166a98adad8005f22d590f858121c1fb6ff3f3f6a58ec3f";
/// ADR 585's thirteen terms plus ADR 589's two finish terms, zero on a build that finishes
/// nothing after its last batch and candidates like any other when it does not.
const ADR585_TERM_FIELDS: [(&str, &str); 15] = [
	("runtime_prep_inner_1", "runtime_prep_inner_1_micros"),
	("runtime_prep_inner_2", "runtime_prep_inner_2_micros"),
	("runtime_prep_semi_read", "runtime_prep_semi_read_micros"),
	("runtime_prep_semi_write", "runtime_prep_semi_write_micros"),
	(
		"runtime_prep_semi_finish",
		"runtime_prep_semi_finish_micros",
	),
	("runtime_prep_anti_read", "runtime_prep_anti_read_micros"),
	("runtime_prep_anti_write", "runtime_prep_anti_write_micros"),
	(
		"runtime_prep_anti_finish",
		"runtime_prep_anti_finish_micros",
	),
	("runtime_prep_residual", "runtime_prep_residual_micros"),
	("base_prep", "base_prep_inclusive_micros"),
	("finalize", "finalize_inclusive_micros"),
	("cleanup", "cleanup_inclusive_micros"),
	("outside_handler_span", "outside_handler_span_micros"),
	("lifecycle_residual", "lifecycle_residual_micros"),
	("outer_residual", "outer_residual_micros"),
];

const ADR586_PHASE0_THRESHOLD: f64 = 0.10;
const ADR586_ADR585_EXECUTABLE: &str =
	"2119272652c4c81f9e2ee9b45bda6236a59cf452ad2c3d1e37b4d5ce4485b444";
const ADR586_ADR585_REFERENCE_OFF_MEDIAN_MS: f64 = 13_958.6;
const ADR586_ADR585_REFERENCE_OFF_RANGE_MS: f64 = 247.4;
/// ADR 585's accepted thirteen-term medians, carried for the side-by-side consistency statement.
const ADR586_ADR585_REFERENCE_MEDIANS: [(&str, f64); 13] = [
	("runtime_prep_semi_write", 2_323.1),
	("runtime_prep_anti_write", 1_954.0),
	("runtime_prep_inner_1", 1_810.2),
	("cleanup", 586.8),
	("runtime_prep_anti_read", 528.6),
	("runtime_prep_semi_read", 386.2),
	("runtime_prep_residual", 17.6),
	("base_prep", 2.4),
	("runtime_prep_inner_2", 1.4),
	("lifecycle_residual", 0.8),
	("finalize", 0.6),
	("outside_handler_span", 0.4),
	("outer_residual", 0.2),
];

const ADR587_TAGS: LifecyclePairTags = LifecyclePairTags {
	label: "ADR 587",
	arm: "adr587_arm",
	pair: "adr587_pair",
	position: "adr587_pair_position",
};
const ADR587_ADR586_EXECUTABLE: &str =
	"03daafed26923778cc7011512d18bc0d30a43ac19056de004f1a3611ab571ace";
const ADR587_ADR586_REFERENCE_OFF_MEDIAN_MS: f64 = 13_128.5;
const ADR587_ADR586_REFERENCE_OFF_RANGE_MS: f64 = 172.2;
/// ADR 586's accepted semi-write medians, carried for the cross-profile consistency statement.
const ADR587_ADR586_REFERENCE_SEMI_WRITE_MEDIANS: [(&str, f64); 5] = [
	("semi_write_insert", 1_610.7),
	("semi_write_repartition", 636.0),
	("semi_write_residual", 3.3),
	("semi_write_filter", 0.0),
	("semi_write_key_program", 0.0),
];
const ADR587_PERF_FREQUENCY_HZ: u32 = 10_000;
const ADR587_MAX_PARANOID: i64 = 2;

/// The sealed ADR 583 q21 Toron digest, byte for byte; ADR 586's and ADR 587's rows carried it.
const ADR588_SEALED_Q21_DIGEST: &str = "ArrowIpcDigest([253, 211, 102, 221, 152, 120, 53, 233, 227, 131, 103, 248, 201, 39, 69, 110, 137, 182, 55, 137, 86, 129, 82, 251, 87, 153, 11, 247, 152, 140, 107, 240])";
const ADR588_ADR587_EXECUTABLE: &str =
	"3f3a5e2c786d18a8142f4cb8477fd1360566b62bbb23e65eb5fd177620e59c36";

/// ADR 589 phase 2 reuses ADR 588's paired-executable driver unchanged, under its own kind.
fn adr589_q21_order_aware_command(args: &[String]) -> ExitCode {
	match run_paired_executables(args, "adr589_q21_order_aware_build", "ADR 589") {
		Ok(summary) => {
			println!("{summary}");
			ExitCode::SUCCESS
		}
		Err(message) => harness_error(&message),
	}
}

fn adr588_q21_second_walk_command(args: &[String]) -> ExitCode {
	match run_adr588_q21_second_walk(args) {
		Ok(summary) => {
			println!("{summary}");
			ExitCode::SUCCESS
		}
		Err(message) => harness_error(&message),
	}
}

fn adr587_q21_insert_command(args: &[String]) -> ExitCode {
	match run_adr587_q21_insert(args) {
		Ok(summary) => {
			println!("{summary}");
			ExitCode::SUCCESS
		}
		Err(message) => harness_error(&message),
	}
}

/// A one-second busy loop for the profiler self-check: the harness sampling itself, so a
/// recording that loses samples or produces none is found before any query runs.
fn adr587_selfcheck_spin_command() -> ExitCode {
	let started = Instant::now();
	let mut acc = 0u64;
	while started.elapsed() < std::time::Duration::from_secs(1) {
		acc = acc
			.wrapping_mul(6_364_136_223_846_793_005)
			.wrapping_add(1_442_695_040_888_963_407);
	}
	if acc == 0 {
		eprintln!("spin");
	}
	ExitCode::SUCCESS
}

fn adr586_q21_semi_write_command(args: &[String]) -> ExitCode {
	match run_adr586_q21_semi_write(args) {
		Ok(summary) => {
			println!("{summary}");
			ExitCode::SUCCESS
		}
		Err(message) => harness_error(&message),
	}
}

fn adr585_q21_decomposition_command(args: &[String]) -> ExitCode {
	match run_adr585_q21_decomposition(args) {
		Ok(summary) => {
			println!("{summary}");
			ExitCode::SUCCESS
		}
		Err(message) => harness_error(&message),
	}
}

/// One lifecycle-decomposition driver shared by ADR 585 and ADR 594 (ADR 593's scope, gate,
/// stamps, journal collection, and variability cutoff included): the label, tags, kind,
/// evaluator, and summary stamps are the spec's; everything else is one path.
struct LifecycleDriverSpec {
	label: &'static str,
	default_output_dir: &'static str,
	tags: (&'static str, &'static str, &'static str),
	kind: &'static str,
	evaluate: fn(&[serde_json::Value]) -> Result<serde_json::Value, String>,
	stamp: fn(&mut serde_json::Value),
	/// ADR 595: the driver requires the separated-launch environment, refuses a differing boot
	/// id or a late start, collects the journal from the launch instant, and withholds the
	/// owner unless both the separation interval and the measurement window are verified.
	separated_launch: bool,
	/// ADR 597: what the two arms are — one executable profiled off/on, or two executables
	/// (control and treatment) both unprofiled — and the summary field the cutoff and the
	/// launch precedence act on (`owner` for an attribution, `verdict` for a retention).
	arms: LifecycleArms,
	outcome_field: &'static str,
}

/// ADR 597: the arms of a lifecycle driver.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LifecycleArms {
	/// One executable, the on arm profiled (ADR 585's pattern); one warmup, five pairs.
	ProfilingOnOff,
	/// Two executables from one recipe, both unprofiled (ADR 588's pattern); one warmup per
	/// executable, control first, then five alternating pairs — twelve rows.
	TwoExecutables,
}

fn adr585_stamp(summary: &mut serde_json::Value) {
	summary["sealed_baseline_executable_sha256"] =
		serde_json::json!(ADR585_SEALED_BASELINE_EXECUTABLE);
	summary["adr584_instrumented_executable_sha256"] = serde_json::json!(ADR585_ADR584_EXECUTABLE);
}

const ADR585_DRIVER: LifecycleDriverSpec = LifecycleDriverSpec {
	label: "ADR 585",
	default_output_dir: "target/tpch/adr585-q21-decomposition",
	tags: ("adr585_arm", "adr585_pair", "adr585_pair_position"),
	kind: "adr585_q21_outside_drive_decomposition",
	evaluate: evaluate_adr585_pairs,
	stamp: adr585_stamp,
	separated_launch: false,
	arms: LifecycleArms::ProfilingOnOff,
	outcome_field: "owner",
};

fn run_adr585_q21_decomposition(args: &[String]) -> Result<serde_json::Value, String> {
	run_lifecycle_decomposition_driver(args, &ADR585_DRIVER)
}

/// ADR 598: the driver's host probes — the scope and the gate — as functions, so a refusal
/// at either can be exercised by a fixture; production passes the real ones.
struct LifecycleHostProbes {
	scope: fn() -> Result<toron_tpch::host_readings::DriverScope, String>,
	/// ADR 599: the launch environment's admission (ADR 595), so a fixture can exercise the
	/// separated path's order — admission, then the wait, then the gate — and its refusal.
	launch: fn(&str, &toron_tpch::host_readings::DriverScope) -> Result<Adr595Launch, String>,
	gate: fn() -> Adr593GateOutcome,
	/// ADR 599: the settle wait's sleep, so a fixture can record the order without waiting.
	sleep: fn(std::time::Duration),
}

const LIFECYCLE_HOST_PROBES: LifecycleHostProbes = LifecycleHostProbes {
	scope: toron_tpch::host_readings::observe_driver_scope,
	launch: adr595_launch_from_environment,
	gate: adr593_start_gate,
	sleep: std::thread::sleep,
};

/// ADR 599: the declared gate procedure's setting — how long the driver waits, doing nothing
/// of its own, between the launch environment's admission and the gate's reading. Default 0
/// is today's placement; a malformed, negative, valueless, or over-limit value is a refusal,
/// never a default.
const ADR599_GATE_SETTLE_MAX_SECONDS: u64 = 600;

fn adr599_parse_gate_settle(args: &[String]) -> Result<u64, String> {
	let Some(position) = args.iter().position(|arg| arg == "--gate-settle-seconds") else {
		return Ok(0);
	};
	let Some(raw) = args.get(position + 1) else {
		return Err("--gate-settle-seconds is missing its value".to_string());
	};
	if raw.starts_with("--") {
		return Err("--gate-settle-seconds is missing its value".to_string());
	}
	let seconds = raw
		.parse::<i128>()
		.map_err(|_| format!("--gate-settle-seconds {raw:?} is not a whole number of seconds"))?;
	if seconds < 0 {
		return Err(format!("--gate-settle-seconds {raw:?} is negative"));
	}
	if seconds > i128::from(ADR599_GATE_SETTLE_MAX_SECONDS) {
		return Err(format!(
			"--gate-settle-seconds {raw:?} is over the {ADR599_GATE_SETTLE_MAX_SECONDS} s limit"
		));
	}
	Ok(seconds as u64)
}

/// ADR 599: what the driver records about the wait and the gate, filled in as they happen so
/// a refusal at any point carries exactly what had been done.
#[derive(Default)]
struct LifecycleGateTrace {
	/// The requested setting as parsed, or the raw text when it was refused.
	requested: serde_json::Value,
	/// The wait as performed: monotonic start and end, elapsed; absent until it runs.
	settle: Option<serde_json::Value>,
	gate: Option<serde_json::Value>,
}

impl LifecycleGateTrace {
	fn settle_value(&self) -> serde_json::Value {
		match &self.settle {
			Some(settle) => {
				let mut value = settle.clone();
				value["requested_seconds"] = self.requested.clone();
				value
			}
			None => serde_json::json!({
				"requested_seconds": self.requested,
				"wait_performed": false,
			}),
		}
	}
}

fn run_lifecycle_decomposition_driver(
	args: &[String],
	spec: &LifecycleDriverSpec,
) -> Result<serde_json::Value, String> {
	run_lifecycle_decomposition_driver_with(args, spec, &LIFECYCLE_HOST_PROBES)
}

fn run_lifecycle_decomposition_driver_with(
	args: &[String],
	spec: &LifecycleDriverSpec,
	probes: &LifecycleHostProbes,
) -> Result<serde_json::Value, String> {
	let label = spec.label;
	// ADR 598: the driver's first act is a reading, before the scope and the launch
	// environment are read; the launcher's verification stamps are read beside it.
	let at_entry = adr598_take_reading();
	let launcher_verification = adr598_launcher_verification_from_environment();
	// ADR 598: the output directory's freshness is decided before anything else can refuse,
	// and the directory is created here, so a refusal has a place of its own to be written
	// that held no prior evidence; an existing directory is refused with nothing written.
	let output_dir =
		PathBuf::from(flag_value(args, "--output-dir").unwrap_or(spec.default_output_dir));
	if output_dir.exists() {
		return Err(format!(
			"{label} requires a fresh output directory; {} already exists",
			output_dir.display()
		));
	}
	std::fs::create_dir_all(&output_dir)
		.map_err(|error| format!("create {}: {error}", output_dir.display()))?;
	let stamps = Adr598DriverStamps {
		at_entry,
		launcher_verification,
	};
	// The wait's and the gate's evidence, filled in as each happens, so a refusal at any point
	// carries the requested setting, whether the wait ran, and the gate's reading if read.
	let mut trace = LifecycleGateTrace::default();
	let result =
		run_lifecycle_decomposition_driver_in(args, spec, &output_dir, &stamps, probes, &mut trace);
	if let Err(error) = &result {
		adr598_write_refusal(&output_dir, label, error, &stamps, &trace);
	}
	result
}

/// ADR 598: what the driver stamps on its summary and on a refusal — the reading it took
/// first and the launcher's verification as the environment carried it.
struct Adr598DriverStamps {
	at_entry: toron_tpch::host_readings::HostReading,
	launcher_verification: serde_json::Value,
}

fn adr598_take_reading() -> toron_tpch::host_readings::HostReading {
	toron_tpch::host_readings::take_reading(
		&toron_tpch::host_readings::HostReadingRoots::system(),
		&toron_tpch::affinity::current_affinity().unwrap_or_default(),
	)
}

/// The launcher's verification stamps: `recorded` (all four present and parsed), `absent`
/// (none present: not launched through `toron-bench run`, or by a `run` older than ADR 598 —
/// absence is not zero work), or `malformed` (some present, or unparsable), the raw values
/// beside it in the last two cases.
fn adr598_launcher_verification_from_environment() -> serde_json::Value {
	const NAMES: [&str; 4] = [
		"TORON_BENCH_VERIFY_BYTES",
		"TORON_BENCH_VERIFY_MICROS",
		"TORON_BENCH_VERIFY_UNIX_MS_END",
		"TORON_BENCH_LAUNCHER_CPU_MASK",
	];
	let raw: Vec<Option<String>> = NAMES.iter().map(|name| std::env::var(name).ok()).collect();
	adr598_launcher_verification(&raw)
}

fn adr598_launcher_verification(raw: &[Option<String>]) -> serde_json::Value {
	let names = [
		"TORON_BENCH_VERIFY_BYTES",
		"TORON_BENCH_VERIFY_MICROS",
		"TORON_BENCH_VERIFY_UNIX_MS_END",
		"TORON_BENCH_LAUNCHER_CPU_MASK",
	];
	let raw_json = serde_json::json!(
		names
			.iter()
			.zip(raw)
			.map(|(name, value)| (name.to_string(), serde_json::json!(value)))
			.collect::<serde_json::Map<_, _>>()
	);
	if raw.iter().all(Option::is_none) {
		return serde_json::json!({"state": "absent"});
	}
	let parsed = (|| {
		let bytes = raw[0].as_deref()?.parse::<u64>().ok()?;
		let micros = raw[1].as_deref()?.parse::<u64>().ok()?;
		let end = raw[2].as_deref()?.parse::<f64>().ok()?;
		let mask = raw[3].as_deref()?;
		let cpus = toron_tpch::affinity::parse_cpulist(mask).ok()?;
		if !end.is_finite() || cpus.is_empty() {
			return None;
		}
		Some(serde_json::json!({
			"state": "recorded",
			"verify_bytes": bytes,
			"verify_micros": micros,
			"verify_end_unix_ms": end,
			"launcher_cpu_mask": toron_tpch::affinity::format_cpulist(&cpus),
		}))
	})();
	parsed.unwrap_or_else(|| serde_json::json!({"state": "malformed", "raw": raw_json}))
}

/// The delay from the launcher's hash to the gate's read, when both are recorded.
fn adr598_hash_end_to_gate_ms(
	launcher_verification: &serde_json::Value,
	at_gate_unix_ms: f64,
) -> serde_json::Value {
	match launcher_verification
		.get("verify_end_unix_ms")
		.and_then(serde_json::Value::as_f64)
	{
		Some(end) => serde_json::json!(at_gate_unix_ms - end),
		None => serde_json::Value::Null,
	}
}

/// A refusal's record, written only into the directory this driver created: the entry
/// reading, the launcher's verification, and — when the gate was read — the gate's reading,
/// load, and its distance from the launcher's hash; a refusal before the gate says so.
fn adr598_write_refusal(
	output_dir: &Path,
	label: &str,
	error: &str,
	stamps: &Adr598DriverStamps,
	trace: &LifecycleGateTrace,
) {
	let gate = trace.gate.as_ref();
	let refusal = serde_json::json!({
		"label": label,
		"refusal": error,
		"refused_unix_ms": adr593_unix_ms(),
		"readings": adr598_readings_value(&stamps.at_entry, gate),
		"launcher_verification": stamps.launcher_verification,
		"start_gate": gate.cloned().unwrap_or(serde_json::Value::Null),
		"gate_evidence": if gate.is_some() { "recorded" } else { "absent: refused before the gate was read" },
		"hash_end_to_gate_ms": adr598_hash_end_to_gate_from_gate(&stamps.launcher_verification, gate),
		"gate_settle": trace.settle_value(),
	});
	if let Err(write_error) = write_json_atomically(&output_dir.join("refusal.json"), &refusal) {
		eprintln!("{label}: refusal.json could not be written: {write_error}");
	}
}

/// The `readings` value shared by the summary and a refusal: `at_gate` is the gate's own
/// reading when the gate was read, else explicitly `null`.
fn adr598_readings_value(
	at_entry: &toron_tpch::host_readings::HostReading,
	gate: Option<&serde_json::Value>,
) -> serde_json::Value {
	serde_json::json!({
		"at_entry": at_entry,
		"at_gate": gate.and_then(|gate| gate.get("reading")).cloned().unwrap_or(serde_json::Value::Null),
		"load_1m_at_gate": gate.and_then(|gate| gate.get("load_1m")).cloned().unwrap_or(serde_json::Value::Null),
	})
}

fn adr598_hash_end_to_gate_from_gate(
	launcher_verification: &serde_json::Value,
	gate: Option<&serde_json::Value>,
) -> serde_json::Value {
	match gate
		.and_then(|gate| gate.pointer("/reading/unix_ms_first"))
		.and_then(serde_json::Value::as_f64)
	{
		Some(at_gate) => adr598_hash_end_to_gate_ms(launcher_verification, at_gate),
		None => serde_json::Value::Null,
	}
}

fn run_lifecycle_decomposition_driver_in(
	args: &[String],
	spec: &LifecycleDriverSpec,
	output_dir: &Path,
	stamps: &Adr598DriverStamps,
	probes: &LifecycleHostProbes,
	trace: &mut LifecycleGateTrace,
) -> Result<serde_json::Value, String> {
	let label = spec.label;
	let output_dir = output_dir.to_path_buf();
	// ADR 599: the gate procedure's setting, parsed before anything else can refuse, refused
	// rather than defaulted when it is malformed.
	let settle_seconds = match adr599_parse_gate_settle(args) {
		Ok(seconds) => {
			trace.requested = serde_json::json!(seconds);
			seconds
		}
		Err(error) => {
			trace.requested = serde_json::json!(flag_value(args, "--gate-settle-seconds"));
			return Err(format!("{label}: {error}"));
		}
	};
	// ADR 593: the driver runs as a transient service outside any login session's scope, and
	// starts only through the gate, read before the driver does any work of its own (the
	// dataset's full-hash verification alone warms a P-core by ten degrees or more).
	let driver_scope = (probes.scope)()?;
	// ADR 595: the launch's identity and instants, checked before the gate is read.
	let launch = if spec.separated_launch {
		Some((probes.launch)(label, &driver_scope)?)
	} else {
		None
	};
	// ADR 599: the wait, after the launch environment's admission and before the gate's
	// reading, doing nothing of the driver's own; its start, end, and elapsed are recorded on
	// the monotonic clock so a skipped or cut-short wait is visible.
	let wait_started = Instant::now();
	let wait_started_ns = toron_tpch::host_readings::monotonic_ns();
	if settle_seconds > 0 {
		(probes.sleep)(std::time::Duration::from_secs(settle_seconds));
	}
	let wait_elapsed = wait_started.elapsed();
	// Zero requested is no wait: recorded as not performed, with its (zero) elapsed.
	trace.settle = Some(serde_json::json!({
		"wait_performed": settle_seconds > 0,
		"started_monotonic_ns": wait_started_ns,
		"ended_monotonic_ns": toron_tpch::host_readings::monotonic_ns(),
		"elapsed_ms": wait_elapsed.as_secs_f64() * 1000.0,
	}));
	// The gate's reading is the first act after the wait ends.
	let gate_outcome = (probes.gate)();
	trace.gate = Some(gate_outcome.gate.clone());
	if let Some(refusal) = gate_outcome.refusal {
		return Err(refusal);
	}
	let start_gate = gate_outcome.gate;
	let mode = flag_value(args, "--mode").unwrap_or("strict");
	let sf = resolved_sf(args)?;
	let data_source = flag_value(args, "--data-source").unwrap_or("parquet");
	let data_dir = data_dir_from_args(args);
	if mode != "strict" || sf != 8.0 || !matches!(data_source, "parquet" | "persisted") {
		return Err(format!(
			"{label} requires --mode strict --sf 8 --data-source parquet; got mode={mode:?} sf={sf} data_source={data_source:?}"
		));
	}
	if std::env::var_os("TORON_NODE_ATTRIBUTION_PROFILE_PATH").is_some() {
		return Err(
			"{label} parent environment must not set TORON_NODE_ATTRIBUTION_PROFILE_PATH"
				.to_string(),
		);
	}
	let budget = GaugeBudget::from_environment()?;
	if budget.memory_per_task != 2_147_483_648
		|| budget.memory_per_task_provenance != "explicit"
		|| budget.spill_bytes_reserved != 2_147_483_648
		|| budget.spill_bytes_reserved_provenance != "explicit"
	{
		return Err(format!(
			"{label} requires explicit 2 GiB task memory and spill reserve"
		));
	}
	let concurrency = gauge_partition_work_concurrency_requested()?;
	if concurrency != (1, "explicit") {
		return Err(format!(
			"{label} requires explicit partition parallelism 1, got {concurrency:?}"
		));
	}
	let identity = runtime_identity()?.ok_or_else(|| {
		format!("{label} must run through toron-bench build/run with a staged executable")
	})?;
	if identity.manifest.recipe.profile != "release" {
		return Err(format!(
			"{label} requires a release build, got {:?}",
			identity.manifest.recipe.profile
		));
	}
	let host_start = observe_host_quiescence();
	if !host_start.quiescent() {
		return Err(format!(
			"{label} requires the local deployment and SQL port quiescent"
		));
	}
	let scratch = std::env::var_os("TORON_JOIN_SPILL_DIR")
		.filter(|value| !value.is_empty())
		.map(PathBuf::from)
		.unwrap_or_else(|| std::env::temp_dir().join("toron_join_spill"));
	if !directory_is_empty(&scratch)? {
		return Err(format!(
			"{label} spill scratch {} is not empty before the run",
			scratch.display()
		));
	}
	let dataset = load_persisted_tpch_dataset(mode, sf, &data_dir)?;
	if dataset.verification.as_str() != "full_hash" {
		return Err(format!(
			"{label} requires full-hash dataset verification, got {}",
			dataset.verification.as_str()
		));
	}
	// ADR 592: as in the ADR 584 driver, the coordinate is fixed and bound before the first
	// child and every child is validated as it returns.
	let coordinate = adr592_driver_coordinate(
		label,
		args,
		&identity,
		&dataset.manifest_fingerprint,
		data_source,
	)?;
	let journal = prepare_perf_sample_journal(&output_dir)?;
	std::fs::create_dir_all(output_dir.join("profiles"))
		.map_err(|error| format!("create {label} profile directory: {error}"))?;

	// ADR 597: two executables, both unprofiled, or one executable profiled off/on.
	let two_executables = match spec.arms {
		LifecycleArms::TwoExecutables => Some(adr597_load_arms(label, args, &identity)?),
		LifecycleArms::ProfilingOnOff => None,
	};
	let (warmups, rows) = match &two_executables {
		Some(arms) => adr597_run_children(
			spec.tags,
			ADR585_PAIRS,
			&coordinate,
			arms,
			&journal,
			|run_index, warmup, arm| {
				run_adr597_child(
					arm,
					run_index,
					warmup,
					&coordinate.expectation.query,
					mode,
					sf,
					data_source,
					&data_dir,
				)
			},
		)?,
		None => {
			let (warmup, rows) = adr592_run_paired_children(
				spec.tags,
				ADR585_PAIRS,
				&coordinate,
				&journal,
				|run_index, warmup, on| {
					run_adr585_child(
						run_index,
						warmup,
						on,
						mode,
						sf,
						data_source,
						&data_dir,
						&output_dir,
					)
				},
			)?;
			(vec![warmup], rows)
		}
	};
	let host_end = observe_host_quiescence();
	if !host_end.quiescent() || !directory_is_empty(&scratch)? {
		return Err(format!(
			"{label} lost host quiescence or left spill scratch after measurement"
		));
	}
	// ADR 593: the journal is collected once, after every child and a settle, then joined.
	let all_rows: Vec<&serde_json::Value> = warmups.iter().chain(rows.iter()).collect();
	let first_launch = all_rows
		.iter()
		.filter_map(|row| {
			row.get("driver_launch_unix_ms")
				.and_then(serde_json::Value::as_f64)
		})
		.fold(f64::INFINITY, f64::min);
	let last_return = all_rows
		.iter()
		.filter_map(|row| {
			row.get("driver_return_unix_ms")
				.and_then(serde_json::Value::as_f64)
		})
		.fold(f64::NEG_INFINITY, f64::max);
	std::thread::sleep(std::time::Duration::from_secs(2));
	// ADR 595: the collection reaches back to the launch instant so the launcher's logout, in
	// the delay before the first child, is inside it.
	let collection_since = match &launch {
		Some(launch) => launch.launch_unix_ms - 1000.0,
		None => first_launch - 1000.0,
	};
	let collection =
		toron_tpch::host_readings::collect_journal(collection_since, last_return + 1000.0);
	let host_journal = adr593_join_journal_and_gaps(&all_rows, &collection);
	let mut summary = (spec.evaluate)(&rows)?;
	adr593_apply_variability_bound_on(&mut summary, spec.outcome_field)?;
	if let Some(launch) = &launch {
		let verdicts = adr595_launch_verdicts(launch, &collection, first_launch, last_return);
		adr595_apply_launch_precedence_on(&mut summary, &verdicts, spec.outcome_field);
		summary["separated_launch"] = serde_json::json!(launch);
		summary["launch_separation"] = verdicts.separation;
		summary["measurement_window"] = verdicts.window;
	}
	if let Some(arms) = &two_executables {
		summary["identities"] = serde_json::json!({
			"control": arms.control.manifest.json_value_for_executable(&arms.control.manifest.toron),
			"treatment": arms.treatment.manifest.json_value_for_executable(&arms.treatment.manifest.toron),
			"control_manifest": arms.control.manifest_path.display().to_string(),
			"treatment_manifest": arms.treatment.manifest_path.display().to_string(),
		});
	}
	summary["driver_scope"] = serde_json::json!(driver_scope);
	// ADR 598: the gate's reading is `at_gate`, the one reading the gate was judged on.
	summary["readings"] = adr598_readings_value(&stamps.at_entry, Some(&start_gate));
	summary["launcher_verification"] = stamps.launcher_verification.clone();
	summary["hash_end_to_gate_ms"] =
		adr598_hash_end_to_gate_from_gate(&stamps.launcher_verification, Some(&start_gate));
	summary["gate_settle"] = trace.settle_value();
	summary["start_gate"] = start_gate;
	summary["host_journal"] = host_journal;
	summary["kind"] = serde_json::json!(spec.kind);
	summary["sealed"] = serde_json::json!(false);
	summary["candidate_authority"] = serde_json::json!(false);
	(spec.stamp)(&mut summary);
	let off_median = summary
		.get("off_execute_median_ms")
		.and_then(serde_json::Value::as_f64);
	adr592_stamp_summary(&mut summary, &coordinate, off_median);
	summary["instrumented_evidence_identity"] = identity.json_value();
	summary["dataset_manifest_fingerprint"] = serde_json::json!(dataset.manifest_fingerprint);
	summary["memory_per_task"] = serde_json::json!(budget.memory_per_task);
	summary["spill_bytes_reserved"] = serde_json::json!(budget.spill_bytes_reserved);
	summary["partition_work_concurrency_requested"] = serde_json::json!(concurrency.0);
	summary["warmup_execute_ms"] = warmups
		.first()
		.and_then(|warmup| warmup.get("execute_ms"))
		.cloned()
		.unwrap_or_default();
	if warmups.len() > 1 {
		summary["warmups_execute_ms"] = serde_json::json!(
			warmups
				.iter()
				.map(|warmup| warmup.get("execute_ms").cloned().unwrap_or_default())
				.collect::<Vec<_>>()
		);
	}
	write_json_atomically(&output_dir.join("summary.json"), &summary)?;
	Ok(summary)
}

// ---------------------------------------------------------------------------------------------
// ADR 598: the paired gate probe — does the launcher's hash lift the sensor at the gate's read?
// ---------------------------------------------------------------------------------------------

/// The fixed recovery schedule, nominal seconds after the work's end.
const ADR598_SCHEDULE_S: [u64; 10] = [0, 1, 2, 3, 5, 10, 20, 30, 45, 60];
const ADR598_BASELINE_READINGS: usize = 10;
const ADR598_GAP_S: u64 = 60;
const ADR598_PAIRS: usize = 8;
const ADR598_CALIBRATION_TRIALS: usize = 3;
/// A scheduled reading more than this past its nominal offset is `late`.
const ADR598_LATE_MS: f64 = 500.0;
/// Recovery: three consecutive readings within this of the baseline's median.
const ADR598_RECOVERY_BAND_MILLIC: i64 = 1_000;
/// The decision: this many positive pairs of eight, and this median adjusted excess.
const ADR598_REQUIRED_POSITIVE_PAIRS: usize = 6;
const ADR598_REQUIRED_MEDIAN_EXCESS_MILLIC: f64 = 2_000.0;
const ADR598_SETTLE_CAP_S: u64 = 30;

/// One reading of the probe: its nominal offset (seconds from the work's end; negative for the
/// baseline), its actual offset in milliseconds, the package temperature, and its state.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Adr598Reading {
	nominal_s: i64,
	actual_offset_ms: f64,
	package_temp_millic: Option<i64>,
	late: bool,
	missing: bool,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Adr598Trial {
	/// `calibration`, `hash`, or `no_hash`.
	kind: String,
	index: usize,
	baseline: Vec<Adr598Reading>,
	work_kind: String,
	work_mask: String,
	work_micros: u64,
	pre_gate_micros: u64,
	at_gate_delay: Adr598Reading,
	schedule: Vec<Adr598Reading>,
	started_unix_ms: f64,
}

/// What a trial's readings say, derived once from the readings alone.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
struct Adr598Derived {
	baseline_median_millic: Option<i64>,
	peak_millic: Option<i64>,
	peak_offset_ms: Option<f64>,
	/// The actual offset of the third of three consecutive scheduled readings after the peak
	/// within the band of the baseline's median; `None` is `unresolved`.
	recovery_ms: Option<f64>,
	unresolved_reason: Option<String>,
	valid: bool,
	invalid_reason: Option<String>,
}

fn adr598_median_i64(values: &mut [i64]) -> Option<i64> {
	if values.is_empty() {
		return None;
	}
	values.sort_unstable();
	let n = values.len();
	Some(if n % 2 == 1 {
		values[n / 2]
	} else {
		(values[n / 2 - 1] + values[n / 2]) / 2
	})
}

fn adr598_derive(trial: &Adr598Trial) -> Adr598Derived {
	let mut baseline: Vec<i64> = trial
		.baseline
		.iter()
		.filter_map(|reading| reading.package_temp_millic)
		.collect();
	let baseline_median = if trial.baseline.len() == ADR598_BASELINE_READINGS
		&& baseline.len() == ADR598_BASELINE_READINGS
	{
		adr598_median_i64(&mut baseline)
	} else {
		None
	};
	let mut invalid_reason = None;
	if baseline_median.is_none() {
		invalid_reason = Some("baseline is not ten readings".to_string());
	} else if trial.at_gate_delay.missing || trial.at_gate_delay.package_temp_millic.is_none() {
		invalid_reason = Some("at_gate_delay reading is missing".to_string());
	} else if trial.schedule.len() != ADR598_SCHEDULE_S.len() {
		invalid_reason = Some("schedule is not ten readings".to_string());
	} else if let Some(reading) = trial
		.schedule
		.iter()
		.find(|reading| reading.missing || reading.package_temp_millic.is_none())
	{
		invalid_reason = Some(format!(
			"scheduled reading +{} s is missing",
			reading.nominal_s
		));
	} else if let Some(reading) = trial.schedule.iter().find(|reading| reading.late) {
		invalid_reason = Some(format!(
			"scheduled reading +{} s is late",
			reading.nominal_s
		));
	}
	let (peak_millic, peak_offset_ms, peak_index) = trial
		.schedule
		.iter()
		.enumerate()
		.filter_map(|(index, reading)| {
			reading
				.package_temp_millic
				.map(|temp| (temp, reading.actual_offset_ms, index))
		})
		.max_by(|a, b| a.0.cmp(&b.0).then(b.2.cmp(&a.2)))
		.map(|(temp, offset, index)| (Some(temp), Some(offset), Some(index)))
		.unwrap_or((None, None, None));
	let mut recovery_ms = None;
	let mut unresolved_reason = None;
	match (baseline_median, peak_index, &invalid_reason) {
		(Some(median), Some(peak_index), None) => {
			let within = |reading: &Adr598Reading| {
				reading
					.package_temp_millic
					.is_some_and(|temp| (temp - median).abs() <= ADR598_RECOVERY_BAND_MILLIC)
			};
			let after = &trial.schedule[peak_index + 1..];
			let found = after
				.windows(3)
				.find(|window| window.iter().all(|reading| within(reading)))
				.map(|window| window[2].actual_offset_ms);
			match found {
				Some(offset) => recovery_ms = Some(offset),
				None => {
					unresolved_reason = Some(
						"no three consecutive readings within the band after the peak by +60 s"
							.to_string(),
					)
				}
			}
		}
		(_, _, Some(reason)) => unresolved_reason = Some(reason.clone()),
		_ => unresolved_reason = Some("no peak".to_string()),
	}
	Adr598Derived {
		baseline_median_millic: baseline_median,
		peak_millic,
		peak_offset_ms,
		recovery_ms,
		unresolved_reason,
		valid: invalid_reason.is_none(),
		invalid_reason,
	}
}

/// One pair's record: both trials' derived values, the raw temperatures at every point, and
/// the baseline-adjusted excesses, the primary quantity.
fn adr598_pair(pair: usize, hash: &Adr598Trial, no_hash: &Adr598Trial) -> serde_json::Value {
	let h = adr598_derive(hash);
	let n = adr598_derive(no_hash);
	let valid = h.valid && n.valid;
	let adjusted = |hash_temp: Option<i64>, no_hash_temp: Option<i64>| -> Option<i64> {
		Some((hash_temp? - h.baseline_median_millic?) - (no_hash_temp? - n.baseline_median_millic?))
	};
	let excess_at_gate = adjusted(
		hash.at_gate_delay.package_temp_millic,
		no_hash.at_gate_delay.package_temp_millic,
	);
	let excess_at_peak = adjusted(h.peak_millic, n.peak_millic);
	let excess_by_offset: Vec<serde_json::Value> = ADR598_SCHEDULE_S
		.iter()
		.enumerate()
		.map(|(index, nominal)| {
			let hash_temp = hash.schedule.get(index).and_then(|r| r.package_temp_millic);
			let no_hash_temp = no_hash
				.schedule
				.get(index)
				.and_then(|r| r.package_temp_millic);
			serde_json::json!({
				"nominal_s": nominal,
				"hash_millic": hash_temp,
				"no_hash_millic": no_hash_temp,
				"adjusted_excess_millic": adjusted(hash_temp, no_hash_temp),
			})
		})
		.collect();
	serde_json::json!({
		"pair": pair,
		"valid": valid,
		"invalid_reason": if valid { serde_json::Value::Null } else {
			serde_json::json!(format!(
				"hash: {}; no_hash: {}",
				h.invalid_reason.clone().unwrap_or_else(|| "valid".to_string()),
				n.invalid_reason.clone().unwrap_or_else(|| "valid".to_string())
			))
		},
		"hash": {"index": hash.index, "derived": h, "at_gate_delay": hash.at_gate_delay, "work_micros": hash.work_micros, "work_mask": hash.work_mask, "pre_gate_micros": hash.pre_gate_micros},
		"no_hash": {"index": no_hash.index, "derived": n, "at_gate_delay": no_hash.at_gate_delay, "work_micros": no_hash.work_micros, "work_mask": no_hash.work_mask, "pre_gate_micros": no_hash.pre_gate_micros},
		"excess_at_gate_millic": excess_at_gate,
		"excess_at_peak_millic": excess_at_peak,
		"excess_by_offset": excess_by_offset,
	})
}

/// The run's disposition from the report's state and the completion hook's marker: a report
/// that says `complete` is complete; one that says `failed` failed; one still `running` was
/// stopped at the unit's runtime limit only when the marker's `SERVICE_RESULT` says
/// `timeout`, and is otherwise `incomplete_unknown` — never assumed.
fn adr598_run_disposition(
	report: &serde_json::Value,
	completion_marker: Option<&str>,
) -> &'static str {
	let complete = report.get("complete") == Some(&serde_json::Value::Bool(true));
	match report.get("state").and_then(serde_json::Value::as_str) {
		Some("complete") => "complete",
		// A report from before the state field carries `complete` alone.
		_ if complete => "complete",
		Some("failed") => "failed",
		_ => {
			let result = completion_marker.and_then(|marker| {
				marker
					.split_whitespace()
					.find_map(|field| field.strip_prefix("result="))
			});
			match result {
				Some("timeout") => "stopped_at_runtime_limit",
				_ => "incomplete_unknown",
			}
		}
	}
}

/// The decision over the pairs: complete evidence (eight valid pairs, the run not stopped),
/// then the counts and the median adjusted excess; the settle interval, if established, from
/// the hash trials' recoveries aggregated explicitly.
fn adr598_decide(pairs: &[serde_json::Value], complete: bool) -> serde_json::Value {
	let valid: Vec<&serde_json::Value> = pairs.iter().filter(|p| p["valid"] == true).collect();
	let positives = valid
		.iter()
		.filter(|p| p["excess_at_gate_millic"].as_i64().is_some_and(|e| e > 0))
		.count();
	let mut excesses: Vec<i64> = valid
		.iter()
		.filter_map(|p| p["excess_at_gate_millic"].as_i64())
		.collect();
	let median_excess = adr598_median_i64(&mut excesses);
	let recoveries: Vec<serde_json::Value> = valid
		.iter()
		.map(|p| p["hash"]["derived"]["recovery_ms"].clone())
		.collect();
	let unresolved = recoveries.iter().filter(|r| r.is_null()).count();
	let mut resolved: Vec<i64> = recoveries
		.iter()
		.filter_map(|r| r.as_f64())
		.map(|ms| ms.round() as i64)
		.collect();
	let median_recovery_ms = if unresolved == 0 && !resolved.is_empty() {
		adr598_median_i64(&mut resolved)
	} else {
		None
	};
	let status = if !complete {
		"undecided_incomplete"
	} else if valid.len() != ADR598_PAIRS {
		"undecided_incomplete_pairs"
	} else if positives >= ADR598_REQUIRED_POSITIVE_PAIRS
		&& median_excess.is_some_and(|m| m as f64 >= ADR598_REQUIRED_MEDIAN_EXCESS_MILLIC)
	{
		"established"
	} else {
		"not_established"
	};
	let settle_seconds = if status == "established" {
		median_recovery_ms.and_then(|ms| {
			let seconds = (ms as f64 / 1000.0 / 5.0).ceil() as u64 * 5;
			(seconds <= ADR598_SETTLE_CAP_S).then_some(seconds)
		})
	} else {
		None
	};
	serde_json::json!({
		"status": status,
		"scope": "this probe's measured temperature effect at the gate's read; not the cause of ADR 597's refusals",
		"valid_pairs": valid.len(),
		"positive_pairs": positives,
		"required_positive_pairs": ADR598_REQUIRED_POSITIVE_PAIRS,
		"median_excess_at_gate_millic": median_excess,
		"required_median_excess_millic": ADR598_REQUIRED_MEDIAN_EXCESS_MILLIC,
		"hash_recoveries_ms": recoveries,
		"unresolved_recoveries": unresolved,
		"median_recovery_ms": median_recovery_ms,
		"median_recovery": if unresolved > 0 { serde_json::json!("unresolved") } else { serde_json::json!(median_recovery_ms) },
		"settle_seconds": settle_seconds,
		"settle_note": "an operational choice for the successor attempt, declared in its plan; not a guarantee",
	})
}

/// The probe's host: the launcher's mask, the driver's mask, the manifest to hash, and the
/// scope for the pre-gate operations.
struct Adr598Host {
	launcher_mask: BTreeSet<usize>,
	driver_mask: BTreeSet<usize>,
	manifest: BuildManifest,
	scope: toron_tpch::host_readings::DriverScope,
}

fn adr598_host(label: &str) -> Result<Adr598Host, String> {
	let launcher_mask = std::env::var("TORON_BENCH_LAUNCHER_CPU_MASK")
		.map_err(|_| format!("{label} requires TORON_BENCH_LAUNCHER_CPU_MASK from toron-bench run: the launcher's mask is read back, never inferred"))
		.and_then(|text| toron_tpch::affinity::parse_cpulist(&text))?;
	let driver_mask = toron_tpch::affinity::current_affinity()?;
	let manifest_path = std::env::var(BUILD_MANIFEST_ENV)
		.map_err(|_| format!("{label} must run through toron-bench run with a build manifest"))?;
	let manifest = BuildManifest::read(Path::new(&manifest_path))?;
	let scope = toron_tpch::host_readings::observe_driver_scope()?;
	Ok(Adr598Host {
		launcher_mask,
		driver_mask,
		manifest,
		scope,
	})
}

/// The hash exactly as `toron-bench run` performs it: the same function on the same files.
fn adr598_hash_as_launcher(manifest: &BuildManifest) -> Result<u64, String> {
	let mut bytes = 0u64;
	let root = |identity: &toron_tpch::evidence::ExecutableIdentity| {
		identity
			.staged_path
			.parent()
			.map(Path::to_path_buf)
			.ok_or_else(|| "staged executable has no parent".to_string())
	};
	toron_tpch::evidence::verify_staged_executable(&manifest.harness, &root(&manifest.harness)?)?;
	bytes += std::fs::metadata(&manifest.harness.staged_path)
		.map(|m| m.len())
		.unwrap_or(0);
	if manifest.toron != manifest.harness {
		toron_tpch::evidence::verify_staged_executable(&manifest.toron, &root(&manifest.toron)?)?;
		bytes += std::fs::metadata(&manifest.toron.staged_path)
			.map(|m| m.len())
			.unwrap_or(0);
	}
	if let Some(duckdb) = &manifest.duckdb {
		toron_tpch::evidence::verify_staged_executable(duckdb, &root(duckdb)?)?;
		bytes += std::fs::metadata(&duckdb.staged_path)
			.map(|m| m.len())
			.unwrap_or(0);
	}
	Ok(bytes)
}

/// A signed offset in milliseconds from `origin` to `instant`.
fn adr598_offset_ms(instant: Instant, origin: Instant) -> f64 {
	if instant >= origin {
		instant.duration_since(origin).as_secs_f64() * 1000.0
	} else {
		-(origin.duration_since(instant).as_secs_f64() * 1000.0)
	}
}

/// One reading, with the instant it was actually taken at; the offset is against the work's
/// end when that is known, and against nothing (NaN, to be converted) when it is not yet.
fn adr598_reading_now(nominal_s: i64, work_end: Option<Instant>) -> (Adr598Reading, Instant) {
	let taken = Instant::now();
	let reading = adr598_take_reading();
	let actual_offset_ms = work_end.map_or(f64::NAN, |end| adr598_offset_ms(taken, end));
	let late = work_end.is_some()
		&& nominal_s >= 0
		&& actual_offset_ms - nominal_s as f64 * 1000.0 > ADR598_LATE_MS;
	(
		Adr598Reading {
			nominal_s,
			actual_offset_ms,
			missing: reading.package_temp_millic.is_none(),
			package_temp_millic: reading.package_temp_millic,
			late,
		},
		taken,
	)
}

fn adr598_reading_at(nominal_s: i64, work_end: Option<Instant>) -> Adr598Reading {
	adr598_reading_now(nominal_s, work_end).0
}

/// The baseline's actual offsets from the work's end, from the instants each reading was
/// actually taken at — never from the nominal schedule.
fn adr598_baseline_offsets(taken: &[Instant], work_end: Instant) -> Vec<f64> {
	taken
		.iter()
		.map(|instant| adr598_offset_ms(*instant, work_end))
		.collect()
}

fn adr598_sleep_until(instant: Instant) {
	let now = Instant::now();
	if instant > now {
		std::thread::sleep(instant - now);
	}
}

/// One trial: the gap, the baseline, the work under the launcher's mask (a hash, or a sleep of
/// the control duration — the same affinity transitions either way), the driver's
/// observational pre-gate operations, the dedicated gate reading, then the fixed schedule.
fn adr598_run_trial(
	host: &Adr598Host,
	kind: &str,
	index: usize,
	control_micros: Option<u64>,
	gap: bool,
) -> Result<Adr598Trial, String> {
	if gap {
		std::thread::sleep(std::time::Duration::from_secs(ADR598_GAP_S));
	}
	let started_unix_ms = adr593_unix_ms();
	let started = Instant::now();
	let mut baseline = Vec::with_capacity(ADR598_BASELINE_READINGS);
	let mut baseline_taken = Vec::with_capacity(ADR598_BASELINE_READINGS);
	for tick in 0..ADR598_BASELINE_READINGS {
		adr598_sleep_until(started + std::time::Duration::from_secs(tick as u64));
		let nominal = tick as i64 - ADR598_BASELINE_READINGS as i64;
		let (reading, taken) = adr598_reading_now(nominal, None);
		baseline.push(reading);
		baseline_taken.push(taken);
	}
	adr598_sleep_until(started + std::time::Duration::from_secs(ADR598_BASELINE_READINGS as u64));
	// The same affinity transitions on both kinds of trial.
	toron_tpch::affinity::apply_affinity(&host.launcher_mask)?;
	let work_mask = toron_tpch::affinity::format_cpulist(
		&toron_tpch::affinity::current_affinity().unwrap_or_default(),
	);
	let work_started = Instant::now();
	let work_kind = if kind == "no_hash" {
		let micros =
			control_micros.ok_or_else(|| "no-hash trial without a control duration".to_string())?;
		std::thread::sleep(std::time::Duration::from_micros(micros));
		"sleep".to_string()
	} else {
		adr598_hash_as_launcher(&host.manifest)?;
		"hash".to_string()
	};
	let work_end = Instant::now();
	let work_micros =
		u64::try_from(work_end.duration_since(work_started).as_micros()).unwrap_or(u64::MAX);
	toron_tpch::affinity::apply_affinity(&host.driver_mask)?;
	// The driver's observational pre-gate operations, then the gate's own reading.
	let pre_gate_started = Instant::now();
	let _scope = toron_tpch::host_readings::observe_driver_scope()?;
	let _ = adr595_read_timer_last_trigger(&host.scope);
	let pre_gate_micros = u64::try_from(pre_gate_started.elapsed().as_micros()).unwrap_or(u64::MAX);
	let at_gate_delay = adr598_reading_at(0, Some(work_end));
	let mut schedule = Vec::with_capacity(ADR598_SCHEDULE_S.len());
	for nominal in ADR598_SCHEDULE_S {
		adr598_sleep_until(work_end + std::time::Duration::from_secs(nominal));
		schedule.push(adr598_reading_at(nominal as i64, Some(work_end)));
	}
	// The baseline's actual offsets from the work's end, from the instants the readings were
	// actually taken at, now that the end is known.
	for (reading, offset) in baseline
		.iter_mut()
		.zip(adr598_baseline_offsets(&baseline_taken, work_end))
	{
		reading.actual_offset_ms = offset;
	}
	Ok(Adr598Trial {
		kind: kind.to_string(),
		index,
		baseline,
		work_kind,
		work_mask,
		work_micros,
		pre_gate_micros,
		at_gate_delay,
		schedule,
		started_unix_ms,
	})
}

/// ADR 598: the retained report's disposition from the completion evidence, and its decision
/// recomputed from the retained pairs.
fn adr598_disposition_command(args: &[String]) -> ExitCode {
	let result = (|| -> Result<serde_json::Value, String> {
		let report_path = flag_value(args, "--report")
			.ok_or_else(|| "adr598-disposition requires --report PATH".to_string())?;
		let report: serde_json::Value = serde_json::from_str(
			&std::fs::read_to_string(report_path)
				.map_err(|error| format!("read {report_path}: {error}"))?,
		)
		.map_err(|error| format!("parse {report_path}: {error}"))?;
		let marker = match flag_value(args, "--completion-marker") {
			Some(path) => Some(
				std::fs::read_to_string(path).map_err(|error| format!("read {path}: {error}"))?,
			),
			None => None,
		};
		let pairs = report
			.get("pairs")
			.and_then(serde_json::Value::as_array)
			.cloned()
			.unwrap_or_default();
		let complete = report.get("complete") == Some(&serde_json::Value::Bool(true));
		Ok(serde_json::json!({
			"report": report_path,
			"disposition": adr598_run_disposition(&report, marker.as_deref()),
			"decision": adr598_decide(&pairs, complete),
			"retained_decision": report.get("decision").cloned(),
			"reevaluating_evidence_identity": runtime_identity_json(),
		}))
	})();
	match result {
		Ok(value) => {
			println!("{value}");
			ExitCode::SUCCESS
		}
		Err(error) => {
			eprintln!("{error}");
			ExitCode::from(2)
		}
	}
}

fn adr598_gate_probe_command(args: &[String]) -> ExitCode {
	match adr598_gate_probe(args) {
		Ok(report) => {
			println!("{report}");
			ExitCode::SUCCESS
		}
		Err(message) => harness_error(&message),
	}
}

/// The probe: calibration, then eight alternating pairs, the report rewritten after every
/// trial so a stop at the unit's runtime limit leaves what completed, marked incomplete.
fn adr598_gate_probe(args: &[String]) -> Result<serde_json::Value, String> {
	let label = "ADR 598";
	let at_entry = adr598_take_reading();
	let launcher_verification = adr598_launcher_verification_from_environment();
	let output_dir =
		PathBuf::from(flag_value(args, "--output-dir").unwrap_or("target/tpch/adr598-gate-probe"));
	if output_dir.exists() {
		return Err(format!(
			"{label} requires a fresh output directory; {} already exists",
			output_dir.display()
		));
	}
	std::fs::create_dir_all(&output_dir)
		.map_err(|error| format!("create {}: {error}", output_dir.display()))?;
	let stamps = Adr598DriverStamps {
		at_entry,
		launcher_verification,
	};
	let result = adr598_gate_probe_in(label, &output_dir, &stamps);
	if let Err(error) = &result {
		adr598_write_refusal(
			&output_dir,
			label,
			error,
			&stamps,
			&LifecycleGateTrace::default(),
		);
	}
	result
}

fn adr598_gate_probe_in(
	label: &str,
	output_dir: &Path,
	stamps: &Adr598DriverStamps,
) -> Result<serde_json::Value, String> {
	let host = adr598_host(label)?;
	let launch = adr595_launch_from_environment(label, &host.scope)?;
	let identity = runtime_identity()?.ok_or_else(|| {
		format!("{label} must run through toron-bench run with a staged executable")
	})?;
	let mut calibration: Vec<Adr598Trial> = Vec::new();
	let mut trials: Vec<Adr598Trial> = Vec::new();
	let mut control_micros: Option<u64> = None;
	let write = |calibration: &[Adr598Trial],
	             trials: &[Adr598Trial],
	             control_micros: Option<u64>,
	             state: &str,
	             error: Option<&str>|
	 -> Result<serde_json::Value, String> {
		let complete = state == "complete";
		let pairs: Vec<serde_json::Value> = trials
			.chunks(2)
			.enumerate()
			.filter(|(_, chunk)| chunk.len() == 2)
			.map(|(pair, chunk)| {
				let (hash, no_hash) = if chunk[0].kind == "hash" {
					(&chunk[0], &chunk[1])
				} else {
					(&chunk[1], &chunk[0])
				};
				adr598_pair(pair, hash, no_hash)
			})
			.collect();
		let report = serde_json::json!({
			"kind": "adr598_gate_probe",
			"complete": complete,
			// The run's own state; whether an incomplete run was stopped at the unit's runtime
			// limit is not the probe's to say — `adr598_run_disposition` reads it from the
			// completion evidence.
			"state": state,
			"error": error,
			"decision": adr598_decide(&pairs, complete),
			"pairs": pairs,
			"trials": trials,
			"calibration": {
				"trials": calibration,
				"hash_micros": calibration.iter().map(|t| t.work_micros).collect::<Vec<_>>(),
				"control_micros": control_micros,
			},
			"schedule_s": ADR598_SCHEDULE_S,
			"gap_s": ADR598_GAP_S,
			"launcher_cpu_mask": toron_tpch::affinity::format_cpulist(&host.launcher_mask),
			"driver_cpu_mask": toron_tpch::affinity::format_cpulist(&host.driver_mask),
			"readings": {"at_entry": stamps.at_entry},
			"launcher_verification": stamps.launcher_verification,
			"separated_launch": launch,
			"driver_scope": host.scope,
			"evidence_identity": identity.json_value(),
			"sealed": false,
			"candidate_authority": false,
		});
		write_json_atomically(&output_dir.join("probe.json"), &report)?;
		Ok(report)
	};
	let run = |calibration: &mut Vec<Adr598Trial>,
	           trials: &mut Vec<Adr598Trial>,
	           control_micros: &mut Option<u64>|
	 -> Result<(), String> {
		for index in 0..ADR598_CALIBRATION_TRIALS {
			let trial = adr598_run_trial(&host, "calibration", index, None, index > 0)?;
			calibration.push(trial);
			write(calibration, trials, *control_micros, "running", None)?;
		}
		let mut hash_micros: Vec<i64> = calibration
			.iter()
			.map(|trial| trial.work_micros as i64)
			.collect();
		*control_micros = adr598_median_i64(&mut hash_micros).map(|micros| micros as u64);
		for pair in 0..ADR598_PAIRS {
			let order: [&str; 2] = if pair % 2 == 0 {
				["hash", "no_hash"]
			} else {
				["no_hash", "hash"]
			};
			for kind in order {
				let index = trials.len();
				let trial = adr598_run_trial(&host, kind, index, *control_micros, true)?;
				trials.push(trial);
				write(calibration, trials, *control_micros, "running", None)?;
			}
		}
		Ok(())
	};
	match run(&mut calibration, &mut trials, &mut control_micros) {
		Ok(()) => write(&calibration, &trials, control_micros, "complete", None),
		Err(error) => {
			// An ordinary failure: the report says so, with what completed, and the error.
			let _ = write(
				&calibration,
				&trials,
				control_micros,
				"failed",
				Some(&error),
			);
			Err(error)
		}
	}
}

// ---------------------------------------------------------------------------------------------
// ADR 597: two executables under the lifecycle driver, twelve rows, ADR 588's retention rule.
// ---------------------------------------------------------------------------------------------

/// The two arms of a paired-executable run: loaded, hash-verified, gated as ADR 588 gates them.
struct Adr597Arms {
	control: Adr588Arm,
	treatment: Adr588Arm,
	control_expectation: Adr592ChildExpectation,
	treatment_expectation: Adr592ChildExpectation,
}

fn adr597_load_arms(
	label: &'static str,
	args: &[String],
	identity: &toron_tpch::evidence::RuntimeIdentity,
) -> Result<Adr597Arms, String> {
	let control_manifest = flag_value(args, "--control-manifest")
		.ok_or_else(|| format!("{label} requires --control-manifest"))?;
	let treatment_manifest_path = std::env::var_os(BUILD_MANIFEST_ENV)
		.map(PathBuf::from)
		.ok_or_else(|| format!("{label} lacks the treatment manifest path"))?;
	let treatment = Adr588Arm::load("treatment", &treatment_manifest_path)?;
	let control = Adr588Arm::load("control", Path::new(control_manifest))?;
	if treatment.manifest.toron.sha256 != identity.executable.sha256 {
		return Err(format!(
			"{label} must be launched from the treatment executable"
		));
	}
	adr588_manifest_gate(&control.manifest, &treatment.manifest)?;
	let expectation = |arm: &Adr588Arm| Adr592ChildExpectation {
		label,
		query: String::new(),
		instrumented: arm.manifest.clone(),
		instrumented_manifest_path: arm.manifest_path.display().to_string(),
		dataset_manifest_fingerprint: String::new(),
		data_source: String::new(),
		digest: String::new(),
		cpu_affinity_requested: String::new(),
	};
	Ok(Adr597Arms {
		control_expectation: expectation(&control),
		treatment_expectation: expectation(&treatment),
		control,
		treatment,
	})
}

/// One child from the arm's executable, unprofiled.
fn run_adr597_child(
	arm: &Adr588Arm,
	run_index: u32,
	warmup: bool,
	query: &str,
	mode: &str,
	sf: f64,
	data_source: &str,
	data_dir: &Path,
) -> Result<serde_json::Value, String> {
	let mut args = vec![
		"perf-run-one".to_string(),
		"--engine".to_string(),
		"toron".to_string(),
		"--query".to_string(),
		query.to_string(),
		"--mode".to_string(),
		mode.to_string(),
		"--sf".to_string(),
		sf.to_string(),
		"--run-index".to_string(),
		run_index.to_string(),
		"--data-source".to_string(),
		data_source.to_string(),
		"--data-dir".to_string(),
		data_dir.display().to_string(),
	];
	if warmup {
		args.push("--warmup-run".to_string());
	}
	// The arm's own manifest and executable in the child's environment, as ADR 588's child
	// carries them, so the row's evidence identity is the arm's and not the launcher's.
	let envs = arm.envs();
	let env_refs = envs
		.iter()
		.map(|(name, value)| (name.as_str(), value.as_str()))
		.collect::<Vec<_>>();
	run_perf_child_with_executable(&arm.executable, &args, &env_refs, None)
}

/// The arm's expectation for a row: the driver's coordinate (dataset, source, digest,
/// affinity) with the arm's executable and manifest in place of the launching one.
fn adr597_arm_expectation(
	coordinate: &Adr592DriverCoordinate,
	arm: &Adr592ChildExpectation,
) -> Adr592ChildExpectation {
	Adr592ChildExpectation {
		label: coordinate.expectation.label,
		query: coordinate.expectation.query.clone(),
		instrumented: arm.instrumented.clone(),
		instrumented_manifest_path: arm.instrumented_manifest_path.clone(),
		dataset_manifest_fingerprint: coordinate.expectation.dataset_manifest_fingerprint.clone(),
		data_source: coordinate.expectation.data_source.clone(),
		digest: coordinate.expectation.digest.clone(),
		cpu_affinity_requested: coordinate.expectation.cpu_affinity_requested.clone(),
	}
}

/// ADR 597's twelve rows: the control's warmup, the treatment's warmup, then five pairs
/// alternating control-treatment, treatment-control. Every row, the warmups included, is
/// stamped with its arm's executable and validated as it returns; a failing row is journaled
/// with its refusal and the run stops before another child is launched.
fn adr597_run_children(
	tags: (&str, &str, &str),
	pairs: usize,
	coordinate: &Adr592DriverCoordinate,
	arms: &Adr597Arms,
	journal: &PerfSampleJournal,
	mut run_child: impl FnMut(u32, bool, &Adr588Arm) -> Result<serde_json::Value, String>,
) -> Result<(Vec<serde_json::Value>, Vec<serde_json::Value>), String> {
	let (arm_tag, pair_tag, position_tag) = tags;
	let admit = |row: &mut serde_json::Value,
	             arm: &Adr588Arm,
	             expectation: &Adr592ChildExpectation|
	 -> Result<(), String> {
		let expected = adr597_arm_expectation(coordinate, expectation);
		row["adr592_launched_manifest"] = serde_json::json!(expected.instrumented_manifest_path);
		row["adr592_launched_executable_sha256"] = serde_json::json!(arm.manifest.toron.sha256);
		if let Err(error) = adr592_validate_child_row(row, &expected, true) {
			row["adr592_refusal"] = serde_json::json!(error);
			journal.append(row)?;
			return Err(format!("{error}; run stopped, row retained"));
		}
		Ok(())
	};
	let mut warmups = Vec::with_capacity(2);
	for (arm, expectation, name) in [
		(&arms.control, &arms.control_expectation, "control"),
		(&arms.treatment, &arms.treatment_expectation, "treatment"),
	] {
		let launched = adr593_unix_ms();
		let mut warmup = run_child(0, true, arm)?;
		warmup["driver_launch_unix_ms"] = serde_json::json!(launched);
		warmup["driver_return_unix_ms"] = serde_json::json!(adr593_unix_ms());
		warmup[arm_tag] = serde_json::json!(name);
		warmup[pair_tag] = serde_json::Value::Null;
		warmup["adr597_phase"] = serde_json::json!("warmup");
		warmup["warmup"] = serde_json::json!(true);
		admit(&mut warmup, arm, expectation)?;
		journal.append(&warmup)?;
		warmups.push(warmup);
	}
	let mut rows = Vec::with_capacity(pairs * 2);
	for pair in 0..pairs {
		let order: [(&Adr588Arm, &Adr592ChildExpectation, &str); 2] = if pair % 2 == 0 {
			[
				(&arms.control, &arms.control_expectation, "control"),
				(&arms.treatment, &arms.treatment_expectation, "treatment"),
			]
		} else {
			[
				(&arms.treatment, &arms.treatment_expectation, "treatment"),
				(&arms.control, &arms.control_expectation, "control"),
			]
		};
		for (position, (arm, expectation, name)) in order.into_iter().enumerate() {
			let launched = adr593_unix_ms();
			let mut row = run_child(pair as u32, false, arm)?;
			row["driver_launch_unix_ms"] = serde_json::json!(launched);
			row["driver_return_unix_ms"] = serde_json::json!(adr593_unix_ms());
			row[arm_tag] = serde_json::json!(name);
			row[pair_tag] = serde_json::json!(pair);
			row[position_tag] = serde_json::json!(position);
			row["adr597_phase"] = serde_json::json!("pairs");
			admit(&mut row, arm, expectation)?;
			journal.append(&row)?;
			rows.push(row);
		}
	}
	Ok((warmups, rows))
}

const ADR597_TAGS: LifecyclePairTags = LifecyclePairTags {
	label: "ADR 597",
	arm: "adr597_arm",
	pair: "adr597_pair",
	position: "adr597_pair_position",
};

/// ADR 596's off median as context for the control arm, never netted.
const ADR597_ADR596_REFERENCE_OFF_MEDIAN_MS: f64 = 8986.78692;

fn adr597_stamp(summary: &mut serde_json::Value) {
	summary["adr596_reference_off_execute_median_ms"] =
		serde_json::json!(ADR597_ADR596_REFERENCE_OFF_MEDIAN_MS);
	if let Some(control) = summary
		.get("control_execute_median_ms")
		.and_then(serde_json::Value::as_f64)
	{
		summary["control_distance_from_adr596_ms"] =
			serde_json::json!(control - ADR597_ADR596_REFERENCE_OFF_MEDIAN_MS);
	}
}

const ADR597_DRIVER: LifecycleDriverSpec = LifecycleDriverSpec {
	label: "ADR 597",
	default_output_dir: "target/tpch/adr597-q21-exchange-skip",
	tags: ("adr597_arm", "adr597_pair", "adr597_pair_position"),
	kind: "adr597_q21_exchange_skip_paired_cut_separated_launch",
	evaluate: evaluate_adr597_pairs,
	stamp: adr597_stamp,
	separated_launch: true,
	arms: LifecycleArms::TwoExecutables,
	outcome_field: "verdict",
};

fn adr597_q21_exchange_skip_command(args: &[String]) -> ExitCode {
	match run_lifecycle_decomposition_driver(args, &ADR597_DRIVER) {
		Ok(summary) => {
			println!("{summary}");
			ExitCode::SUCCESS
		}
		Err(message) => harness_error(&message),
	}
}

/// ADR 588's pairs evaluation under ADR 597's tags: per pair the control and treatment
/// execute times and the gain, the control median and range, the bar `2 × range_control`,
/// every-pair positivity, and the predeclared verdict. The control's median and range are
/// also published under the lifecycle names (`off_execute_*`) the cutoff and the stamp read.
fn evaluate_adr597_pairs(rows: &[serde_json::Value]) -> Result<serde_json::Value, String> {
	let tags = &ADR597_TAGS;
	if rows.len() != ADR585_PAIRS * 2 {
		return Err(format!(
			"ADR 597 requires {} pair rows, found {}",
			ADR585_PAIRS * 2,
			rows.len()
		));
	}
	let mut pair_records = Vec::new();
	let mut gains = Vec::new();
	let mut controls = Vec::new();
	let mut treatments = Vec::new();
	for pair in 0..ADR585_PAIRS {
		let pair_rows = rows
			.iter()
			.filter(|row| {
				row.get(tags.pair).and_then(serde_json::Value::as_u64) == Some(pair as u64)
			})
			.collect::<Vec<_>>();
		let find = |arm: &str| {
			pair_rows
				.iter()
				.find(|row| row.get(tags.arm).and_then(serde_json::Value::as_str) == Some(arm))
				.copied()
				.ok_or_else(|| format!("ADR 597 pair {pair} lacks {arm} arm"))
		};
		let control_row = find("control")?;
		let treatment_row = find("treatment")?;
		let expected_order = if pair % 2 == 0 {
			[("control", 0), ("treatment", 1)]
		} else {
			[("treatment", 0), ("control", 1)]
		};
		for (arm, position) in expected_order {
			if find(arm)?
				.get(tags.position)
				.and_then(serde_json::Value::as_u64)
				!= Some(position)
			{
				return Err(format!(
					"ADR 597 pair {pair} does not use its declared alternating order"
				));
			}
		}
		for row in [control_row, treatment_row] {
			if row.get("status").and_then(serde_json::Value::as_str) != Some("pass")
				|| row.get("warmup") == Some(&serde_json::Value::Bool(true))
			{
				return Err(format!(
					"ADR 597 pair {pair} carries a non-passing or warmup row"
				));
			}
		}
		let control_ms = required_f64(control_row, "execute_ms")?;
		let treatment_ms = required_f64(treatment_row, "execute_ms")?;
		if control_ms <= 0.0 || treatment_ms <= 0.0 {
			return Err(format!("ADR 597 pair {pair} has non-positive execute time"));
		}
		let gain = control_ms - treatment_ms;
		gains.push(gain);
		controls.push(control_ms);
		treatments.push(treatment_ms);
		pair_records.push(serde_json::json!({
			"pair": pair,
			"execute_control_ms": control_ms,
			"execute_treatment_ms": treatment_ms,
			"gain_ms": gain,
			"gain_fraction": gain / control_ms,
			"control_digest": control_row.get("digest"),
			"treatment_digest": treatment_row.get("digest"),
		}));
	}
	let control_min = controls.iter().copied().fold(f64::INFINITY, f64::min);
	let control_max = controls.iter().copied().fold(f64::NEG_INFINITY, f64::max);
	let control_range = control_max - control_min;
	let control_median = median(&mut controls.clone());
	let treatment_median = median(&mut treatments.clone());
	let gain_median = median(&mut gains.clone());
	let verdict = adr588_verdict(&gains, control_range);
	Ok(serde_json::json!({
		"pair_count": ADR585_PAIRS,
		"pairs": pair_records,
		"control_execute_median_ms": control_median,
		"control_execute_range_ms": control_range,
		"treatment_execute_median_ms": treatment_median,
		"off_execute_median_ms": control_median,
		"off_execute_range_ms": control_range,
		"on_execute_median_ms": treatment_median,
		"gain_median_ms": gain_median,
		"gain_median_fraction_of_control": gain_median / control_median,
		"required_gain_ms": 2.0 * control_range,
		"every_pair_positive": gains.iter().all(|gain| *gain > 0.0),
		"verdict": verdict,
	}))
}

/// ADR 597: the twelve-row re-evaluation over a retained journal, with the receipt-free
/// evaluator, the cutoff, and ADR 595's verdicts on the `verdict` field.
fn adr597_reevaluate_command(args: &[String]) -> ExitCode {
	match adr595_reevaluate_with_contract(
		args,
		"adr597-reevaluate",
		evaluate_adr597_pairs,
		&adr597_row_contract(),
		"verdict",
	) {
		Ok(summary) => {
			println!("{summary}");
			ExitCode::SUCCESS
		}
		Err(error) => {
			eprintln!("{error}");
			ExitCode::from(2)
		}
	}
}

// ---------------------------------------------------------------------------------------------
// ADR 595: the separated launch, verified from the driver's own journal collection.
// ---------------------------------------------------------------------------------------------

/// Late-start tolerance: the driver's start against the timer's trigger, and the trigger
/// against the launch instant plus the delay.
const ADR595_LATE_START_TOLERANCE_MS: f64 = 30_000.0;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct Adr595Launch {
	launcher_session_id: String,
	boot_id: String,
	attempt_token: String,
	launch_unix_ms: f64,
	delay_seconds: f64,
	timer_unit: Option<String>,
	timer_last_trigger_unix_ms: Option<f64>,
	driver_start_unix_ms: f64,
}

fn adr595_env(name: &str, label: &str) -> Result<String, String> {
	std::env::var(name)
		.ok()
		.filter(|value| !value.trim().is_empty())
		.ok_or_else(|| format!("{label} requires {name} from the separated launch"))
}

/// Read the launch environment, bind it to this boot, read the timer's actual trigger, and
/// refuse a late start. Every instant is recorded on the summary whatever the verdict.
fn adr595_launch_from_environment(
	label: &str,
	scope: &toron_tpch::host_readings::DriverScope,
) -> Result<Adr595Launch, String> {
	let launcher_session_id = adr595_env("ADR595_LAUNCHER_SESSION_ID", label)?;
	let boot_id = adr595_env("ADR595_BOOT_ID", label)?;
	let attempt_token = adr595_env("ADR595_ATTEMPT_TOKEN", label)?;
	let launch_unix_ms = adr595_env("ADR595_LAUNCH_UNIX_MS", label)?
		.parse::<f64>()
		.map_err(|error| format!("{label} ADR595_LAUNCH_UNIX_MS: {error}"))?;
	let delay_seconds = adr595_env("ADR595_DELAY_SECONDS", label)?
		.parse::<f64>()
		.map_err(|error| format!("{label} ADR595_DELAY_SECONDS: {error}"))?;
	let host_boot_id = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
		.map(|text| text.trim().to_string())
		.map_err(|error| format!("{label} boot id: {error}"))?;
	if host_boot_id != boot_id {
		return Err(format!(
			"{label} launched on boot {boot_id} but runs on boot {host_boot_id}"
		));
	}
	let (timer_unit, timer_last_trigger_unix_ms) = adr595_read_timer_last_trigger(scope);
	let driver_start_unix_ms = adr593_unix_ms();
	let launch = Adr595Launch {
		launcher_session_id,
		boot_id,
		attempt_token,
		launch_unix_ms,
		delay_seconds,
		timer_unit,
		timer_last_trigger_unix_ms,
		driver_start_unix_ms,
	};
	adr595_check_late_start(label, &launch)?;
	Ok(launch)
}

/// The observational read of the launch environment: the driver's timer unit and its last
/// trigger, through `systemctl --user show`. ADR 598's probe repeats this read per trial; the
/// admission check around it is the driver's alone.
fn adr595_read_timer_last_trigger(
	scope: &toron_tpch::host_readings::DriverScope,
) -> (Option<String>, Option<f64>) {
	let timer_unit = scope
		.unit
		.as_ref()
		.and_then(|unit| unit.strip_suffix(".service"))
		.map(|stem| format!("{stem}.timer"));
	let timer_last_trigger_unix_ms = timer_unit.as_ref().and_then(|timer| {
		let output = std::process::Command::new("systemctl")
			.args(["--user", "show", timer, "-p", "LastTriggerUSec", "--value"])
			.output()
			.ok()?;
		let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
		adr595_parse_systemd_timestamp(&text)
	});
	(timer_unit, timer_last_trigger_unix_ms)
}

/// `systemctl show` prints a timer's trigger as a local timestamp (`Tue 2026-09-16 07:25:08 CDT`);
/// parsed through `date`, which knows the zone the manager printed.
fn adr595_parse_systemd_timestamp(text: &str) -> Option<f64> {
	if text.is_empty() || text == "n/a" {
		return None;
	}
	let output = std::process::Command::new("date")
		.args(["-d", text, "+%s%N"])
		.output()
		.ok()?;
	let nanos = String::from_utf8_lossy(&output.stdout)
		.trim()
		.parse::<u128>()
		.ok()?;
	Some(nanos as f64 / 1_000_000.0)
}

/// A late timer trigger or a late driver start is a finding, not a longer window.
fn adr595_check_late_start(label: &str, launch: &Adr595Launch) -> Result<(), String> {
	let Some(trigger) = launch.timer_last_trigger_unix_ms else {
		return Err(format!(
			"{label} cannot read the timer's last trigger ({:?}); the late-start check needs it",
			launch.timer_unit
		));
	};
	let expected = launch.launch_unix_ms + launch.delay_seconds * 1000.0;
	if trigger - expected > ADR595_LATE_START_TOLERANCE_MS {
		return Err(format!(
			"{label} timer triggered {:.0} ms after the launch instant plus the delay",
			trigger - expected
		));
	}
	if launch.driver_start_unix_ms - trigger > ADR595_LATE_START_TOLERANCE_MS {
		return Err(format!(
			"{label} driver started {:.0} ms after the timer's trigger",
			launch.driver_start_unix_ms - trigger
		));
	}
	Ok(())
}

/// A boot id is 32 hex digits, spelled by the kernel as 8-4-4-4-12 with dashes and by the
/// journal without; anything else is malformed, not "close enough".
fn adr595_parse_boot_id(text: &str) -> Option<String> {
	let trimmed = text.trim();
	let dashed = trimmed.len() == 36
		&& trimmed.char_indices().all(|(i, c)| {
			if matches!(i, 8 | 13 | 18 | 23) {
				c == '-'
			} else {
				c.is_ascii_hexdigit()
			}
		});
	let plain = trimmed.len() == 32 && trimmed.chars().all(|c| c.is_ascii_hexdigit());
	if !dashed && !plain {
		return None;
	}
	Some(
		trimmed
			.chars()
			.filter(|c| *c != '-')
			.map(|c| c.to_ascii_lowercase())
			.collect(),
	)
}

struct Adr595Verdicts {
	separation: serde_json::Value,
	window: serde_json::Value,
}

fn adr595_is_session_source(entry: &toron_tpch::host_readings::JournalEntry) -> bool {
	let source = entry.source.as_deref().unwrap_or("");
	source == "ssh.service"
		|| source == "sshd.service"
		|| source == "sshd"
		|| source == "sshd-session"
		|| source == "systemd-logind.service"
		|| (source.starts_with("session-") && source.ends_with(".scope"))
		|| (source == "init.scope" && entry.message.contains("session-"))
}

/// The two verdicts from the driver's own collection: separation (the launcher's logout, on
/// this boot, before the first child) and cleanliness (no session entry in the window), each
/// `verified` only under a `collected` collection whose bounds cover the interval.
fn adr595_launch_verdicts(
	launch: &Adr595Launch,
	collection: &toron_tpch::host_readings::JournalCollection,
	first_launch_unix_ms: f64,
	last_return_unix_ms: f64,
) -> Adr595Verdicts {
	use toron_tpch::host_readings::JournalState;
	let collected = collection.state == JournalState::Collected;
	let state = serde_json::json!(collection.state);
	let covers = |since: f64, until: f64| {
		collected && collection.since_unix_ms <= since && collection.until_unix_ms >= until
	};
	// Separation: [launch − 1 s, first launch).
	let separation_since = launch.launch_unix_ms - 1000.0;
	let logout_needle = format!("Session {} logged out", launch.launcher_session_id);
	// The launcher's boot id must be well formed; a malformed one verifies nothing.
	let launch_boot = adr595_parse_boot_id(&launch.boot_id);
	// The logout is evidence only as logind's own entry, on this boot, with a boot id present.
	let logouts: Vec<f64> = collection
		.entries
		.iter()
		.filter(|entry| {
			entry.message.contains(&logout_needle)
				&& entry.source.as_deref() == Some("systemd-logind.service")
				&& match (
					&launch_boot,
					entry.boot_id.as_deref().and_then(adr595_parse_boot_id),
				) {
					(Some(launch_boot), Some(entry_boot)) => &entry_boot == launch_boot,
					_ => false,
				}
		})
		.map(|entry| entry.realtime_us as f64 / 1000.0)
		.collect();
	let separation_covered = covers(separation_since, first_launch_unix_ms);
	let inside_interval = |ms: f64| ms >= separation_since && ms < first_launch_unix_ms;
	let inside_bounds = |ms: f64| ms >= collection.since_unix_ms && ms <= collection.until_unix_ms;
	let separation = if launch_boot.is_none() {
		serde_json::json!({
			"verdict": "unknown",
			"reason": format!("the launcher's boot id {:?} is malformed", launch.boot_id),
			"collection_state": state,
			"first_child_launch_unix_ms": first_launch_unix_ms,
		})
	} else if !separation_covered {
		serde_json::json!({
			"verdict": "unknown",
			"reason": "the collection does not cover the separation interval completely",
			"collection_state": state,
			"launcher_logout_unix_ms": logouts.first(),
			"first_child_launch_unix_ms": first_launch_unix_ms,
		})
	} else if let Some(logout_ms) = logouts
		.iter()
		.copied()
		.find(|ms| inside_interval(*ms) && inside_bounds(*ms))
	{
		serde_json::json!({
			"verdict": "verified",
			"collection_state": state,
			"launcher_logout_unix_ms": logout_ms,
			"first_child_launch_unix_ms": first_launch_unix_ms,
			"margin_ms": first_launch_unix_ms - logout_ms,
		})
	} else if let Some(logout_ms) = logouts.first().copied() {
		serde_json::json!({
			"verdict": "failed",
			"reason": if logout_ms < separation_since {
				"the launcher's logout precedes the separation interval: it is not this launch's"
			} else {
				"the launcher's logout is inside or after the measurement window"
			},
			"collection_state": state,
			"launcher_logout_unix_ms": logout_ms,
			"first_child_launch_unix_ms": first_launch_unix_ms,
		})
	} else {
		serde_json::json!({
			"verdict": "failed",
			"reason": format!(
				"no logind '{logout_needle}' entry on boot {} inside the collection",
				launch.boot_id
			),
			"collection_state": state,
			"launcher_logout_unix_ms": serde_json::Value::Null,
			"first_child_launch_unix_ms": first_launch_unix_ms,
		})
	};
	// Cleanliness: [first launch − 1 s, last return + 1 s].
	let window_since = first_launch_unix_ms - 1000.0;
	let window_until = last_return_unix_ms + 1000.0;
	let events: Vec<serde_json::Value> = collection
		.entries
		.iter()
		.filter(|entry| {
			let ms = entry.realtime_us as f64 / 1000.0;
			ms >= window_since && ms <= window_until && adr595_is_session_source(entry)
		})
		.map(|entry| serde_json::json!(entry))
		.collect();
	let window = if !covers(window_since, window_until) {
		serde_json::json!({
			"verdict": "unknown",
			"reason": "the collection does not cover the measurement window completely",
			"collection_state": state,
			"session_events": events,
		})
	} else if events.is_empty() {
		serde_json::json!({
			"verdict": "verified",
			"collection_state": state,
			"session_events": [],
		})
	} else {
		serde_json::json!({
			"verdict": "failed",
			"reason": format!("{} session entries inside the measurement window", events.len()),
			"collection_state": state,
			"session_events": events,
		})
	};
	Adr595Verdicts { separation, window }
}

/// Precedence: a separation or window verdict other than `verified` withholds the owner with
/// the named status; the tables, facts, readings, and cutoff verdict stay as evidence.
#[cfg(test)]
fn adr595_apply_launch_precedence(summary: &mut serde_json::Value, verdicts: &Adr595Verdicts) {
	adr595_apply_launch_precedence_on(summary, verdicts, "owner")
}

fn adr595_apply_launch_precedence_on(
	summary: &mut serde_json::Value,
	verdicts: &Adr595Verdicts,
	field: &str,
) {
	let status = |verdict: &serde_json::Value, prefix: &str| -> Option<String> {
		match verdict.get("verdict").and_then(serde_json::Value::as_str) {
			Some("verified") => None,
			Some(other) => Some(format!("{prefix}_{other}")),
			None => Some(format!("{prefix}_unknown")),
		}
	};
	let withheld = status(&verdicts.separation, "withheld_launch_separation")
		.or_else(|| status(&verdicts.window, "withheld_window_not_clean"));
	if let Some(status) = withheld {
		let previous = summary[field].take();
		summary[field] = serde_json::json!({
			"status": status,
			"rule_outcome_withheld": previous,
		});
	}
}

/// ADR 595: recompute the launch verdicts and the precedence from a retained summary's own
/// collection and launch record, and re-run the evaluator and the cutoff over the retained
/// rows, so a corrected verdict can be applied to retained evidence without another run.
fn adr595_reevaluate_command(args: &[String]) -> ExitCode {
	match adr595_reevaluate(args, "adr595-reevaluate", evaluate_adr594_pairs) {
		Ok(summary) => {
			println!("{summary}");
			ExitCode::SUCCESS
		}
		Err(error) => {
			eprintln!("{error}");
			ExitCode::from(2)
		}
	}
}

/// ADR 596: the same re-evaluation over a retained ADR 596 journal, through the seventh-band
/// evaluator, with every retained on-row receipt re-run through the band's current rules.
fn adr596_reevaluate_command(args: &[String]) -> ExitCode {
	let result = adr595_reevaluate(args, "adr596-reevaluate", evaluate_adr596_pairs).and_then(
		|mut summary| {
			let samples_path = flag_value(args, "--samples").unwrap_or_default();
			let rows: Vec<serde_json::Value> = std::fs::read_to_string(samples_path)
				.map_err(|error| format!("read {samples_path}: {error}"))?
				.lines()
				.filter(|line| !line.trim().is_empty())
				.map(|line| {
					serde_json::from_str(line)
						.map_err(|error| format!("parse {samples_path}: {error}"))
				})
				.collect::<Result<_, _>>()?;
			summary["receipt_revalidation"] = adr596_revalidate_receipts(&rows)?;
			Ok(summary)
		},
	);
	match result {
		Ok(summary) => {
			println!("{summary}");
			ExitCode::SUCCESS
		}
		Err(error) => {
			eprintln!("{error}");
			ExitCode::from(2)
		}
	}
}

/// Re-run an evaluator, the cutoff, and ADR 595's launch verdicts over a retained summary and
/// journal, so a corrected rule can be applied to retained rows without another run.
fn adr595_reevaluate(
	args: &[String],
	verb: &str,
	evaluate: fn(&[serde_json::Value]) -> Result<serde_json::Value, String>,
) -> Result<serde_json::Value, String> {
	adr595_reevaluate_with_contract(args, verb, evaluate, &adr595_row_contract(), "owner")
}

/// One expected row of a retained journal: its arm (when the contract names arms), whether
/// it is a warmup, and its pair and position.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ExpectedRow {
	arm_tag: &'static str,
	arm: Option<&'static str>,
	warmup: bool,
	pair: Option<u64>,
	position: Option<u64>,
}

/// ADR 595's eleven rows: one warmup, then ten pair rows; the arms, pairs, and positions are
/// not named (the retained journals predate the contract's checks and are read as they were,
/// the stamps and the warmup flag alone).
fn adr595_row_contract() -> Vec<ExpectedRow> {
	let mut rows = vec![ExpectedRow {
		arm_tag: "adr594_arm",
		arm: None,
		warmup: true,
		pair: None,
		position: None,
	}];
	for _ in 0..ADR585_PAIRS * 2 {
		rows.push(ExpectedRow {
			arm_tag: "adr594_arm",
			arm: None,
			warmup: false,
			pair: None,
			position: None,
		});
	}
	rows
}

/// ADR 597's twelve rows: the control's warmup, the treatment's warmup, then five pairs
/// alternating control-treatment, treatment-control.
fn adr597_row_contract() -> Vec<ExpectedRow> {
	let mut rows = vec![
		ExpectedRow {
			arm_tag: "adr597_arm",
			arm: Some("control"),
			warmup: true,
			pair: None,
			position: None,
		},
		ExpectedRow {
			arm_tag: "adr597_arm",
			arm: Some("treatment"),
			warmup: true,
			pair: None,
			position: None,
		},
	];
	for pair in 0..ADR585_PAIRS as u64 {
		let order = if pair % 2 == 0 {
			["control", "treatment"]
		} else {
			["treatment", "control"]
		};
		for (position, arm) in order.into_iter().enumerate() {
			rows.push(ExpectedRow {
				arm_tag: "adr597_arm",
				arm: Some(arm),
				warmup: false,
				pair: Some(pair),
				position: Some(position as u64),
			});
		}
	}
	rows
}

fn adr595_reevaluate_with_contract(
	args: &[String],
	verb: &str,
	evaluate: fn(&[serde_json::Value]) -> Result<serde_json::Value, String>,
	contract: &[ExpectedRow],
	outcome_field: &str,
) -> Result<serde_json::Value, String> {
	let summary_path =
		flag_value(args, "--summary").ok_or_else(|| format!("{verb} requires --summary PATH"))?;
	let samples_path =
		flag_value(args, "--samples").ok_or_else(|| format!("{verb} requires --samples PATH"))?;
	let read =
		|path: &str| std::fs::read_to_string(path).map_err(|error| format!("read {path}: {error}"));
	let retained: serde_json::Value = serde_json::from_str(&read(summary_path)?)
		.map_err(|error| format!("parse {summary_path}: {error}"))?;
	let rows: Vec<serde_json::Value> = read(samples_path)?
		.lines()
		.filter(|line| !line.trim().is_empty())
		.map(|line| {
			serde_json::from_str(line).map_err(|error| format!("parse {samples_path}: {error}"))
		})
		.collect::<Result<_, _>>()?;
	let launch: Adr595Launch = serde_json::from_value(retained["separated_launch"].clone())
		.map_err(|error| format!("retained summary lacks a separated launch record: {error}"))?;
	let collection: toron_tpch::host_readings::JournalCollection =
		serde_json::from_value(retained["host_journal"]["collection"].clone())
			.map_err(|error| format!("retained summary lacks a journal collection: {error}"))?;
	let (first_launch, last_return) = lifecycle_window_from_rows(&rows, contract)?;
	let pair_rows: Vec<serde_json::Value> = rows
		.iter()
		.filter(|row| row.get("warmup") != Some(&serde_json::Value::Bool(true)))
		.cloned()
		.collect();
	let mut summary = evaluate(&pair_rows)?;
	adr593_apply_variability_bound_on(&mut summary, outcome_field)?;
	let verdicts = adr595_launch_verdicts(&launch, &collection, first_launch, last_return);
	adr595_apply_launch_precedence_on(&mut summary, &verdicts, outcome_field);
	summary["separated_launch"] = serde_json::json!(launch);
	summary["launch_separation"] = verdicts.separation;
	summary["measurement_window"] = verdicts.window;
	summary["retained_owner"] = retained[outcome_field].clone();
	summary["retained_launch_separation"] = retained["launch_separation"].clone();
	summary["retained_measurement_window"] = retained["measurement_window"].clone();
	summary["reevaluated_summary"] = serde_json::json!(summary_path);
	summary["reevaluating_evidence_identity"] = runtime_identity_json();
	Ok(summary)
}

/// The measurement window from retained rows: every one of the eleven rows (the warmup
/// included) must carry a finite launch and return with launch < return, and the rows must
/// not overlap; a missing or malformed stamp is refused rather than narrowing the window.
#[cfg(test)]
fn adr595_window_from_rows(rows: &[serde_json::Value]) -> Result<(f64, f64), String> {
	lifecycle_window_from_rows(rows, &adr595_row_contract())
}

/// The measurement window from retained rows under a row contract: exactly the contract's
/// rows in its order (arm, warmup, pair, position where the contract names them), every row
/// with a finite launch and return, launch before return, no overlap; a missing or malformed
/// stamp on any row — the first and last included — is refused rather than narrowed around.
fn lifecycle_window_from_rows(
	rows: &[serde_json::Value],
	contract: &[ExpectedRow],
) -> Result<(f64, f64), String> {
	if rows.len() != contract.len() {
		return Err(format!(
			"ADR 595 re-evaluation requires {} rows, found {}",
			contract.len(),
			rows.len()
		));
	}
	for (index, (row, expected)) in rows.iter().zip(contract).enumerate() {
		let warmup = row.get("warmup") == Some(&serde_json::Value::Bool(true));
		if warmup != expected.warmup {
			return Err(format!(
				"ADR 595 row {index} is {} where the contract expects {}",
				if warmup { "a warmup" } else { "a pair row" },
				if expected.warmup {
					"a warmup"
				} else {
					"a pair row"
				}
			));
		}
		if let Some(arm) = expected.arm
			&& row
				.get(expected.arm_tag)
				.and_then(serde_json::Value::as_str)
				!= Some(arm)
		{
			return Err(format!(
				"ADR 595 row {index} carries arm {:?} where the contract expects {arm:?}",
				row.get(expected.arm_tag)
			));
		}
		let pair_tag = expected.arm_tag.replace("_arm", "_pair");
		let position_tag = expected.arm_tag.replace("_arm", "_pair_position");
		if expected.pair.is_some()
			&& (row.get(&pair_tag).and_then(serde_json::Value::as_u64) != expected.pair
				|| row.get(&position_tag).and_then(serde_json::Value::as_u64) != expected.position)
		{
			return Err(format!(
				"ADR 595 row {index} is not pair {:?} position {:?}",
				expected.pair, expected.position
			));
		}
	}
	let mut intervals = Vec::with_capacity(rows.len());
	for (index, row) in rows.iter().enumerate() {
		let launch = row
			.get("driver_launch_unix_ms")
			.and_then(serde_json::Value::as_f64)
			.filter(|value| value.is_finite());
		let returned = row
			.get("driver_return_unix_ms")
			.and_then(serde_json::Value::as_f64)
			.filter(|value| value.is_finite());
		match (launch, returned) {
			(Some(launch), Some(returned)) if launch < returned => {
				intervals.push((launch, returned))
			}
			(Some(_), Some(_)) => {
				return Err(format!("ADR 595 row {index} returns before it launches"));
			}
			_ => {
				return Err(format!(
					"ADR 595 row {index} lacks a finite driver_launch_unix_ms or driver_return_unix_ms"
				));
			}
		}
	}
	let mut ordered = intervals.clone();
	ordered.sort_by(|a, b| a.0.total_cmp(&b.0));
	if ordered.windows(2).any(|pair| pair[1].0 < pair[0].1) {
		return Err("ADR 595 child intervals overlap".to_string());
	}
	Ok((ordered[0].0, ordered.last().expect("contract rows").1))
}

fn adr595_stamp(summary: &mut serde_json::Value) {
	adr594_stamp(summary);
	summary["adr594_reference_steady_pair_band_ms"] = serde_json::json!({
		"inner_1_sizing_scan": 200.0,
		"inner_1_partition_sizing": 266.0,
		"inner_1_build_write": 880.0,
		"runtime_prep_inner_1": 1345.0,
	});
}

const ADR595_DRIVER: LifecycleDriverSpec = LifecycleDriverSpec {
	label: "ADR 595",
	default_output_dir: "target/tpch/adr595-q21-inner-1",
	tags: ("adr594_arm", "adr594_pair", "adr594_pair_position"),
	kind: "adr595_q21_inner_1_decomposition_separated_launch",
	evaluate: evaluate_adr594_pairs,
	stamp: adr595_stamp,
	separated_launch: true,
	arms: LifecycleArms::ProfilingOnOff,
	outcome_field: "owner",
};

fn adr595_q21_inner_1_command(args: &[String]) -> ExitCode {
	match run_lifecycle_decomposition_driver(args, &ADR595_DRIVER) {
		Ok(summary) => {
			println!("{summary}");
			ExitCode::SUCCESS
		}
		Err(message) => harness_error(&message),
	}
}

// ---------------------------------------------------------------------------------------------
// ADR 596: the spilled build write decomposed through the seventh band.
// ---------------------------------------------------------------------------------------------

const ADR596_TAGS: LifecyclePairTags = LifecyclePairTags {
	label: "ADR 596",
	arm: "adr596_arm",
	pair: "adr596_pair",
	position: "adr596_pair_position",
};

/// The children of `inner_1_build_write` as the receipt names them: the writer's eleven and, on
/// a receipt of the `index_build` facts vintage, the finish-built index's reserve and
/// finalization.
/// `scan_open` and `scan_poll` are read from the receipt and reported beside each other; the
/// rule sees their sum as `build_write_scan_wait` and never ranks the sum beside its parts.
const ADR596_CHILDREN: [&str; 13] = [
	"build_write_create",
	"build_write_scan_open",
	"build_write_scan_poll",
	"build_write_filter",
	"build_write_repartition",
	"build_write_project",
	"build_write_key_extract",
	"build_write_encode",
	"build_write_page_write",
	"build_write_index",
	"build_write_finish",
	"build_write_reserve",
	"build_write_finish_index",
];

/// The two children a receipt carries only under the `index_build` facts vintage; an earlier
/// receipt neither carries them nor is asked for them, and they enter no band.
const FINISH_BUILT_CHILDREN: [&str; 2] = ["build_write_reserve", "build_write_finish_index"];

/// The rule's terms: the scan wait, the other children, and the residual; the two ADR 601
/// terms rank only when a receipt carried them.
const ADR596_RULE_TERMS: [&str; 13] = [
	"build_write_create",
	"build_write_scan_wait",
	"build_write_filter",
	"build_write_repartition",
	"build_write_project",
	"build_write_key_extract",
	"build_write_encode",
	"build_write_page_write",
	"build_write_index",
	"build_write_finish",
	"build_write_reserve",
	"build_write_finish_index",
	"build_write_residual",
];

const ADR596_FACTS: [&str; 37] = [
	"build_write_facts_vintage",
	"build_write_exchange_handling",
	"build_write_pass_through_requested",
	"build_write_pass_through_refusal",
	"build_write_scratch_plaintext_capacity_peak",
	"build_write_scratch_refs_capacity_peak",
	"build_write_writer_peak_bytes",
	"build_write_resident_state_peak_bytes",
	"build_write_scratch_releases",
	"build_write_filter_present",
	"build_write_exchange",
	"build_write_exchange_partitions",
	"build_write_projected",
	"build_write_termination",
	"build_write_index_mode",
	"build_write_index_variant",
	"build_write_index_build",
	"build_write_index_expected_rows",
	"build_write_index_capacity_growths",
	"build_write_finish_index_entries",
	"build_write_finish_index_duplicates",
	"build_write_finish_index_buckets",
	"build_write_unsupported",
	"build_write_batches_read",
	"build_write_batches_limit_discarded",
	"build_write_batches_admitted",
	"build_write_batches_filtered_empty",
	"build_write_batches_written",
	"build_write_rows_read",
	"build_write_rows_kept",
	"build_write_chunks",
	"build_write_chunks_unkeyed",
	"build_write_pages",
	"build_write_plaintext_bytes",
	"build_write_ciphertext_bytes",
	"build_write_index_entries",
	"build_write_index_compactions",
];

/// Historical readings of the term this record decomposes: ADR 595's off median and its
/// `inner_1_build_write`, context beside the new reading and never netted against it.
const ADR596_ADR595_REFERENCE_OFF_MEDIAN_MS: f64 = 9005.848683;
const ADR596_ADR595_REFERENCE_BUILD_WRITE_MS: f64 = 876.3799093750869;

fn adr596_stamp(summary: &mut serde_json::Value) {
	adr595_stamp(summary);
	summary["adr595_reference_off_execute_median_ms"] =
		serde_json::json!(ADR596_ADR595_REFERENCE_OFF_MEDIAN_MS);
	summary["adr595_reference_inner_1_build_write_ms"] =
		serde_json::json!(ADR596_ADR595_REFERENCE_BUILD_WRITE_MS);
	if let Some(off) = summary
		.get("off_execute_median_ms")
		.and_then(serde_json::Value::as_f64)
	{
		summary["off_arm_distance_from_adr595_ms"] =
			serde_json::json!(off - ADR596_ADR595_REFERENCE_OFF_MEDIAN_MS);
	}
}

const ADR596_DRIVER: LifecycleDriverSpec = LifecycleDriverSpec {
	label: "ADR 596",
	default_output_dir: "target/tpch/adr596-q21-build-write",
	tags: ("adr596_arm", "adr596_pair", "adr596_pair_position"),
	kind: "adr596_q21_build_write_decomposition_separated_launch",
	evaluate: evaluate_adr596_pairs,
	stamp: adr596_stamp,
	separated_launch: true,
	arms: LifecycleArms::ProfilingOnOff,
	outcome_field: "owner",
};

fn adr596_q21_build_write_command(args: &[String]) -> ExitCode {
	match run_lifecycle_decomposition_driver(args, &ADR596_DRIVER) {
		Ok(summary) => {
			println!("{summary}");
			ExitCode::SUCCESS
		}
		Err(message) => harness_error(&message),
	}
}

/// The seventh-band layer over ADR 594's sixth-band evaluation (run first, under this record's
/// tags): per pair the eleven children, the residual and its bound, the path facts, the shape,
/// and the consistency; then ADR 594's rule over the eleven rule terms. The partitioned branch,
/// an unsupported branch, or an unsupported writer shape on any pair withholds the owner as an
/// unsupported path; an inconsistent or absent band withholds it as an incomplete instrument.
fn evaluate_adr596_pairs(rows: &[serde_json::Value]) -> Result<serde_json::Value, String> {
	let tags = &ADR596_TAGS;
	let mut base = evaluate_adr594_pairs_tagged(rows, tags)?;
	let mut pair_bands = Vec::new();
	let mut samples: BTreeMap<String, Vec<f64>> = BTreeMap::new();
	let mut all_residuals_complete = true;
	let mut all_facts_consistent = true;
	let mut unsupported_pairs = Vec::new();
	let mut incompleteness = Vec::new();
	for pair in 0..ADR585_PAIRS {
		let pair_rows = rows
			.iter()
			.filter(|row| {
				row.get(tags.pair).and_then(serde_json::Value::as_u64) == Some(pair as u64)
			})
			.collect::<Vec<_>>();
		let off = pair_rows
			.iter()
			.find(|row| row.get(tags.arm).and_then(serde_json::Value::as_str) == Some("off"))
			.ok_or_else(|| format!("ADR 596 pair {pair} lacks off arm"))?;
		let on = pair_rows
			.iter()
			.find(|row| row.get(tags.arm).and_then(serde_json::Value::as_str) == Some("on"))
			.ok_or_else(|| format!("ADR 596 pair {pair} lacks on arm"))?;
		let execute_off_ms = required_f64(off, "execute_ms")?;
		let execute_on_ms = required_f64(on, "execute_ms")?;
		let execute_on_micros = (execute_on_ms * 1000.0).round() as u64;
		let overhead = (execute_on_ms - execute_off_ms).max(0.0) * 1000.0;
		let (_, receipt) = lifecycle_target_receipt(on, pair, tags.label)?;
		let branch = receipt
			.get("inner_1_branch")
			.and_then(serde_json::Value::as_str)
			.map(str::to_string);
		let mut band = serde_json::json!({"pair": pair, "inner_1_branch": branch});
		match branch.as_deref() {
			Some("spilled") => {}
			Some(other) => {
				// The partitioned branch and the non-streaming fallback carry no build write to
				// decompose; the band is not read through.
				band["build_write_shape"] = serde_json::json!("unsupported");
				band["build_write_unsupported"] = serde_json::json!(format!("branch {other}"));
				unsupported_pairs.push(pair);
				pair_bands.push(band);
				continue;
			}
			None => {
				all_facts_consistent = false;
				incompleteness.push(format!("pair {pair}: the sixth band is absent"));
				pair_bands.push(band);
				continue;
			}
		}
		let build_write = receipt_u64(receipt, "inner_1_build_write_micros")?;
		band["inner_1_build_write_micros"] = serde_json::json!(build_write);
		let shape = receipt
			.get("build_write_shape")
			.and_then(serde_json::Value::as_str)
			.map(str::to_string);
		let Some(shape) = shape else {
			all_facts_consistent = false;
			incompleteness.push(format!("pair {pair}: the seventh band is absent"));
			pair_bands.push(band);
			continue;
		};
		for fact in ADR596_FACTS {
			band[fact] = receipt.get(fact).cloned().unwrap_or_default();
		}
		band["build_write_shape"] = serde_json::json!(shape);
		if shape != "supported" {
			unsupported_pairs.push(pair);
		}
		let scale = |micros: u64| micros as f64 / execute_on_micros as f64 * execute_off_ms;
		let mut children_total = 0u64;
		let mut scan_wait = 0u64;
		// The receipt names its form when its facts carry `index_build`; that fact, not the
		// vintage label, is what says the two finish-built terms are on it.
		let finish_built_vintage = receipt
			.get("build_write_index_build")
			.is_some_and(|value| !value.is_null());
		for child in ADR596_CHILDREN {
			if !finish_built_vintage && FINISH_BUILT_CHILDREN.contains(&child) {
				continue;
			}
			let micros = receipt_u64(receipt, &format!("{child}_micros"))?;
			let calls = receipt_u64(receipt, &format!("{child}_calls"))?;
			children_total = children_total
				.checked_add(micros)
				.ok_or_else(|| format!("ADR 596 pair {pair} child sum overflowed"))?;
			band[format!("{child}_micros")] = serde_json::json!(micros);
			band[format!("{child}_calls")] = serde_json::json!(calls);
			if matches!(child, "build_write_scan_open" | "build_write_scan_poll") {
				scan_wait = scan_wait.saturating_add(micros);
			}
			samples
				.entry(child.to_string())
				.or_default()
				.push(scale(micros));
		}
		band["build_write_scan_wait_micros"] = serde_json::json!(scan_wait);
		samples
			.entry("build_write_scan_wait".to_string())
			.or_default()
			.push(scale(scan_wait));
		let residual = receipt_u64(receipt, "build_write_residual_micros")?;
		if children_total.checked_add(residual) != Some(build_write) {
			return Err(format!(
				"ADR 596 pair {pair} build_write algebra does not reconcile"
			));
		}
		samples
			.entry("build_write_residual".to_string())
			.or_default()
			.push(scale(residual));
		let residual_bound = overhead + 0.05 * build_write as f64;
		let residual_complete = residual as f64 <= residual_bound;
		all_residuals_complete &= residual_complete;
		let consistent =
			receipt.get("build_write_facts_consistent") == Some(&serde_json::json!(true));
		if !consistent && shape == "supported" {
			all_facts_consistent = false;
			incompleteness.push(format!(
				"pair {pair}: {}",
				receipt
					.get("build_write_inconsistency")
					.and_then(serde_json::Value::as_str)
					.unwrap_or("facts inconsistent")
			));
		}
		band["build_write_residual_micros"] = serde_json::json!(residual);
		band["build_write_residual_bound_micros"] = serde_json::json!(residual_bound);
		band["build_write_residual_complete"] = serde_json::json!(residual_complete);
		band["build_write_facts_consistent"] = serde_json::json!(consistent);
		band["build_write_inconsistency"] = receipt
			.get("build_write_inconsistency")
			.cloned()
			.unwrap_or_default();
		pair_bands.push(band);
	}
	let off_range = required_f64(&base, "off_execute_range_ms")?;
	let off_median = required_f64(&base, "off_execute_median_ms")?;
	let owner_floor = required_f64(&base, "owner_floor_ms")?;
	let perturbation_accepted = base["perturbation_gate"] == "accepted";
	let base_profiles_complete = base["all_profiles_complete"] == true;
	let median_of = |term: &str| {
		let mut values = samples.get(term).cloned().unwrap_or_default();
		median(&mut values)
	};
	let mut medians = ADR596_RULE_TERMS
		.iter()
		.filter(|term| samples.contains_key(**term))
		.map(|term| (term.to_string(), median_of(term)))
		.collect::<Vec<_>>();
	medians.sort_by(|left, right| right.1.total_cmp(&left.1));
	// The sixth band's own preconditions: its facts consistent and its branch spilled on every
	// pair, checked above per pair; its rule's outcome is carried as context.
	let sixth_band_consistent = base["all_inner_1_facts_consistent"] == true;
	let sixth_band_residuals_complete = base["all_inner_1_residuals_complete"] == true;
	if !sixth_band_consistent {
		all_facts_consistent = false;
		incompleteness.push("the sixth band is inconsistent".to_string());
	}
	let owner = if !unsupported_pairs.is_empty() {
		serde_json::json!({
			"status": "withheld_unsupported_path",
			"pairs": unsupported_pairs,
		})
	} else if !all_facts_consistent {
		serde_json::json!({
			"status": "withheld_instrument_incomplete",
			"incompleteness": incompleteness,
		})
	} else {
		adr594_owner(
			&medians,
			owner_floor,
			off_range,
			off_median,
			perturbation_accepted,
			base_profiles_complete && sixth_band_residuals_complete && all_residuals_complete,
		)
	};
	let pricing = adr596_removable_pricing(&owner, owner_floor);
	base["adr596_pair_bands"] = serde_json::json!(pair_bands);
	base["all_build_write_residuals_complete"] = serde_json::json!(all_residuals_complete);
	base["all_build_write_facts_consistent"] = serde_json::json!(all_facts_consistent);
	base["build_write_terms_scaled_ms"] = serde_json::json!(
		medians
			.iter()
			.map(|(term, value)| {
				serde_json::json!({
					"term": term,
					"median_scaled_ms": value,
					"share_of_off_execute": value / off_median,
					"share_of_build_write": if let Some(build_write) = medians_build_write(&base) { value / build_write } else { f64::NAN },
					"clears_floor": *value > owner_floor,
				})
			})
			.collect::<Vec<_>>()
	);
	// The scan wait's two components, beside the rule's table and never ranked in it.
	base["build_write_scan_wait_components_scaled_ms"] = serde_json::json!({
		"build_write_scan_open": median_of("build_write_scan_open"),
		"build_write_scan_poll": median_of("build_write_scan_poll"),
		"build_write_scan_wait": median_of("build_write_scan_wait"),
	});
	base["adr594_four_term_owner"] = base["owner"].clone();
	base["owner"] = owner;
	base["owner_removable_pricing"] = pricing;
	Ok(base)
}

/// ADR 596: every retained on-row receipt re-run through the seventh band's current rules,
/// as `toron::exec::node_attribution::revalidate_build_write_band` applies them to the terms
/// and facts the profile stores. Two passes per pair: the receipt as retained, and — only when
/// the retained facts predate a fact the rules now require and the writer's termination is
/// `stream_end` — a second pass with `batches_limit_discarded = 0` and `batches_admitted =
/// batches_read`, which the measured code's path implies (the discard counter is incremented
/// only on the branch that sets `row_limit`); the derivation is named, never silent.
fn adr596_revalidate_receipts(rows: &[serde_json::Value]) -> Result<serde_json::Value, String> {
	let tags = &ADR596_TAGS;
	let mut pairs = Vec::new();
	let mut all_consistent_as_retained = true;
	let mut all_consistent_with_derivation = true;
	for row in rows {
		if row.get(tags.arm).and_then(serde_json::Value::as_str) != Some("on")
			|| row.get("warmup") == Some(&serde_json::Value::Bool(true))
		{
			continue;
		}
		let pair = row
			.get(tags.pair)
			.and_then(serde_json::Value::as_u64)
			.ok_or_else(|| "ADR 596 on row lacks a pair".to_string())?;
		let (profile, retained) = lifecycle_target_receipt(row, pair as usize, tags.label)?;
		let invocation = profile["aggregate_invocations"]
			.as_array()
			.and_then(|invocations| {
				invocations.iter().find(|invocation| {
					invocation.get("inclusive_lifecycle_receipt") == Some(retained)
				})
			})
			.ok_or_else(|| format!("ADR 596 pair {pair} target invocation not found"))?;
		let as_retained = toron::exec::node_attribution::revalidate_build_write_band(invocation);
		let retained_consistent =
			retained.get("build_write_facts_consistent") == Some(&serde_json::json!(true));
		let revalidated_consistent =
			as_retained.get("build_write_facts_consistent") == Some(&serde_json::json!(true));
		all_consistent_as_retained &= revalidated_consistent;
		// Fields the retained receipt carries that the re-run band disagrees on.
		let differences: Vec<String> = as_retained
			.as_object()
			.into_iter()
			.flat_map(|band| band.iter())
			.filter(|(name, value)| {
				retained
					.get(name.as_str())
					.is_some_and(|kept| kept != *value)
			})
			.map(|(name, value)| {
				format!(
					"{name}: retained {} vs revalidated {value}",
					retained[name.as_str()]
				)
			})
			.collect();
		let facts = invocation
			.get("diagnostics")
			.and_then(serde_json::Value::as_array)
			.and_then(|entries| {
				entries.iter().find(|entry| {
					entry.get("name") == Some(&serde_json::json!("build_write_facts"))
				})
			})
			.and_then(|entry| entry.get("value").cloned())
			.unwrap_or_default();
		let predating: Vec<&str> = ["batches_limit_discarded", "batches_admitted"]
			.into_iter()
			.filter(|name| facts.get(name).is_none())
			.collect();
		let with_derivation = if !predating.is_empty()
			&& facts.get("termination") == Some(&serde_json::json!("stream_end"))
		{
			let mut derived = invocation.clone();
			if let Some(entry) = derived
				.get_mut("diagnostics")
				.and_then(serde_json::Value::as_array_mut)
				.and_then(|entries| {
					entries.iter_mut().find(|entry| {
						entry.get("name") == Some(&serde_json::json!("build_write_facts"))
					})
				}) {
				// Only a missing field is derived; a recorded value is kept and validated as
				// recorded, so a receipt that recorded a discard is never rewritten clean.
				if facts.get("batches_limit_discarded").is_none() {
					entry["value"]["batches_limit_discarded"] = serde_json::json!(0);
				}
				if facts.get("batches_admitted").is_none() {
					entry["value"]["batches_admitted"] = facts["batches_read"].clone();
				}
			}
			let band = toron::exec::node_attribution::revalidate_build_write_band(&derived);
			all_consistent_with_derivation &=
				band.get("build_write_facts_consistent") == Some(&serde_json::json!(true));
			Some(serde_json::json!({
				"derivation": "stream_end implies batches_limit_discarded = 0 and batches_admitted = batches_read on the measured code path; applied to the missing fields only",
				"derived_fields": predating,
				"build_write_facts_consistent": band.get("build_write_facts_consistent").cloned(),
				"build_write_inconsistency": band.get("build_write_inconsistency").cloned(),
				"build_write_shape": band.get("build_write_shape").cloned(),
			}))
		} else {
			all_consistent_with_derivation &= revalidated_consistent;
			None
		};
		pairs.push(serde_json::json!({
			"pair": pair,
			"retained_facts_consistent": retained_consistent,
			"revalidated_facts_consistent": revalidated_consistent,
			"revalidated_inconsistency": as_retained.get("build_write_inconsistency").cloned(),
			"revalidated_shape": as_retained.get("build_write_shape").cloned(),
			"revalidated_residual_micros": as_retained.get("build_write_residual_micros").cloned(),
			"differences_from_retained": differences,
			"facts_predating_the_rules": predating,
			"with_derived_limit_facts": with_derivation,
		}));
	}
	Ok(serde_json::json!({
		"pairs": pairs,
		"all_facts_consistent_as_retained": all_consistent_as_retained,
		"all_facts_consistent_with_derivation": all_consistent_with_derivation,
	}))
}

/// The sixth band's `inner_1_build_write` median-scaled ms from the base evaluation, the
/// denominator for each child's share of the term it decomposes.
fn medians_build_write(base: &serde_json::Value) -> Option<f64> {
	base.get("inner_1_terms_scaled_ms")?
		.as_array()?
		.iter()
		.find(|term| term.get("term") == Some(&serde_json::json!("inner_1_build_write")))?
		.get("median_scaled_ms")?
		.as_f64()
}

/// ADR 596: what a mechanism would stop doing in the owning child, priced gross as the record
/// wrote it. A gross figure is the work the candidate would not repeat, not a saving: a carried
/// or resident input still has to be allocated, accounted against the memory budget, and paid
/// for on the drive side, and only a paired net-gain measurement under its own record says
/// whether any of it is a gain. No cut opens under this record.
fn adr596_removable_pricing(owner: &serde_json::Value, owner_floor: f64) -> serde_json::Value {
	if owner["status"] != "resolved" {
		return serde_json::json!({"status": "no_owner"});
	}
	let term = owner["term"].as_str().unwrap_or_default();
	let median = owner["median_scaled_ms"].as_f64().unwrap_or(f64::NAN);
	let gross = |candidate: &str, net: &str| {
		serde_json::json!({
			"status": "gross_candidate_work",
			"removable_subterm": term,
			"median_scaled_ms": median,
			"clears_floor": median > owner_floor,
			"candidate": candidate,
			"net_improvement": net,
		})
	};
	match term {
		"build_write_scan_wait" => gross(
			"a carried or cached input: the polls the sizing scan and the partition-sizing pass already paid for would not be repeated",
			"unpriced: a carried input is allocated and accounted against the memory budget; successor measures paired off/on with the cut beside value parity and peak-memory accounting",
		),
		"build_write_encode" | "build_write_page_write" => gross(
			"a resident build under the budget: the pages would not be encoded and written",
			"unpriced: a resident build is allocated and accounted against the memory budget and read differently on the drive side; successor measures paired off/on with the cut beside value parity and peak-memory accounting",
		),
		"build_write_index" => gross(
			"a once-built index (ADR 589's pattern): the per-page inserts would be replaced by one build",
			"unpriced: the once-built index still sorts or hashes every entry; successor measures paired off/on with the cut beside value parity",
		),
		"build_write_residual" => serde_json::json!({
			"status": "not_removable",
			"successor": "the residual is not a mechanism; deepen the instrument",
		}),
		_ => serde_json::json!({
			"status": "unpriced",
			"removable_subterm": term,
			"median_scaled_ms": median,
			"successor": "name the subterm a mechanism would stop doing, then price it gross under its own record",
		}),
	}
}

// ---------------------------------------------------------------------------------------------
// ADR 594: replay 1's preparation decomposed through the sixth band.
// ---------------------------------------------------------------------------------------------

const ADR594_TAGS: LifecyclePairTags = LifecyclePairTags {
	label: "ADR 594",
	arm: "adr594_arm",
	pair: "adr594_pair",
	position: "adr594_pair_position",
};

const ADR594_CHILDREN: [&str; 3] = [
	"inner_1_sizing_scan",
	"inner_1_partition_sizing",
	"inner_1_build_write",
];

/// Historical readings of the term this record decomposes: context beside the new reading,
/// never netted against it. ADR 593's off median and its `runtime_prep_inner_1`; ADR 591's
/// sealed q21 median is bound by content through `--sealed-manifest`.
const ADR594_ADR593_REFERENCE_OFF_MEDIAN_MS: f64 = 9064.673146;
const ADR594_ADR593_REFERENCE_INNER_1_MS: f64 = 1345.0028301315272;

fn adr594_stamp(summary: &mut serde_json::Value) {
	summary["adr593_reference_off_execute_median_ms"] =
		serde_json::json!(ADR594_ADR593_REFERENCE_OFF_MEDIAN_MS);
	summary["adr593_reference_runtime_prep_inner_1_ms"] =
		serde_json::json!(ADR594_ADR593_REFERENCE_INNER_1_MS);
	if let Some(off) = summary
		.get("off_execute_median_ms")
		.and_then(serde_json::Value::as_f64)
	{
		summary["off_arm_distance_from_adr593_ms"] =
			serde_json::json!(off - ADR594_ADR593_REFERENCE_OFF_MEDIAN_MS);
	}
}

const ADR594_DRIVER: LifecycleDriverSpec = LifecycleDriverSpec {
	label: "ADR 594",
	default_output_dir: "target/tpch/adr594-q21-inner-1",
	tags: ("adr594_arm", "adr594_pair", "adr594_pair_position"),
	kind: "adr594_q21_inner_1_decomposition",
	evaluate: evaluate_adr594_pairs,
	stamp: adr594_stamp,
	separated_launch: false,
	arms: LifecycleArms::ProfilingOnOff,
	outcome_field: "owner",
};

fn adr594_q21_inner_1_command(args: &[String]) -> ExitCode {
	match run_lifecycle_decomposition_driver(args, &ADR594_DRIVER) {
		Ok(summary) => {
			println!("{summary}");
			ExitCode::SUCCESS
		}
		Err(message) => harness_error(&message),
	}
}

/// The sixth-band layer over the shared lifecycle evaluation: per pair the four terms, the
/// facts, the identity, the consistency, and the residual bound; then the four-term rule with
/// its named outcomes. An unsupported branch on any pair withholds the owner as an
/// unsupported path; an inconsistent band withholds it as an incomplete instrument.
fn evaluate_adr594_pairs(rows: &[serde_json::Value]) -> Result<serde_json::Value, String> {
	evaluate_adr594_pairs_tagged(rows, &ADR594_TAGS)
}

/// The sixth-band evaluation under a successor record's tags (ADR 596 runs it first).
fn evaluate_adr594_pairs_tagged(
	rows: &[serde_json::Value],
	tags: &LifecyclePairTags,
) -> Result<serde_json::Value, String> {
	let mut base = evaluate_lifecycle_pairs(rows, tags)?;
	let mut pair_bands = Vec::new();
	let mut samples: BTreeMap<String, Vec<f64>> = BTreeMap::new();
	let mut all_residuals_complete = true;
	let mut all_facts_consistent = true;
	let mut unsupported_pairs = Vec::new();
	let mut incompleteness = Vec::new();
	for pair in 0..ADR585_PAIRS {
		let pair_rows = rows
			.iter()
			.filter(|row| {
				row.get(tags.pair).and_then(serde_json::Value::as_u64) == Some(pair as u64)
			})
			.collect::<Vec<_>>();
		let off = pair_rows
			.iter()
			.find(|row| row.get(tags.arm).and_then(serde_json::Value::as_str) == Some("off"))
			.ok_or_else(|| format!("ADR 594 pair {pair} lacks off arm"))?;
		let on = pair_rows
			.iter()
			.find(|row| row.get(tags.arm).and_then(serde_json::Value::as_str) == Some("on"))
			.ok_or_else(|| format!("ADR 594 pair {pair} lacks on arm"))?;
		let execute_off_ms = required_f64(off, "execute_ms")?;
		let execute_on_ms = required_f64(on, "execute_ms")?;
		let execute_on_micros = (execute_on_ms * 1000.0).round() as u64;
		let overhead = (execute_on_ms - execute_off_ms).max(0.0) * 1000.0;
		let (_, receipt) = lifecycle_target_receipt(on, pair, tags.label)?;
		let inner_1 = receipt_u64(receipt, "runtime_prep_inner_1_micros")?;
		let mut band = serde_json::json!({"pair": pair, "runtime_prep_inner_1_micros": inner_1});
		let branch = receipt
			.get("inner_1_branch")
			.and_then(serde_json::Value::as_str)
			.map(str::to_string);
		band["inner_1_branch"] = serde_json::json!(branch);
		match branch.as_deref() {
			None => {
				all_facts_consistent = false;
				incompleteness.push(format!("pair {pair}: the band is absent"));
				pair_bands.push(band);
				continue;
			}
			Some("unsupported") => {
				unsupported_pairs.push(pair);
				pair_bands.push(band);
				continue;
			}
			Some(_) => {}
		}
		let mut children_total = 0u64;
		for child in ADR594_CHILDREN {
			let micros = receipt_u64(receipt, &format!("{child}_micros"))?;
			let calls = receipt_u64(receipt, &format!("{child}_calls"))?;
			children_total = children_total
				.checked_add(micros)
				.ok_or_else(|| format!("ADR 594 pair {pair} child sum overflowed"))?;
			band[format!("{child}_micros")] = serde_json::json!(micros);
			band[format!("{child}_calls")] = serde_json::json!(calls);
			samples
				.entry(child.to_string())
				.or_default()
				.push(micros as f64 / execute_on_micros as f64 * execute_off_ms);
		}
		let residual = receipt_u64(receipt, "inner_1_residual_micros")?;
		if children_total.checked_add(residual) != Some(inner_1) {
			return Err(format!(
				"ADR 594 pair {pair} inner_1 algebra does not reconcile"
			));
		}
		samples
			.entry("inner_1_residual".to_string())
			.or_default()
			.push(residual as f64 / execute_on_micros as f64 * execute_off_ms);
		let residual_bound = overhead + 0.05 * inner_1 as f64;
		let residual_complete = residual as f64 <= residual_bound;
		all_residuals_complete &= residual_complete;
		let consistent = receipt.get("inner_1_facts_consistent") == Some(&serde_json::json!(true));
		if !consistent {
			all_facts_consistent = false;
			incompleteness.push(format!(
				"pair {pair}: {}",
				receipt
					.get("inner_1_inconsistency")
					.and_then(serde_json::Value::as_str)
					.unwrap_or("facts inconsistent")
			));
		}
		for fact in [
			"inner_1_partition_count",
			"inner_1_group_count",
			"inner_1_partition_sizing_passes",
			"inner_1_estimated_build_bytes",
			"inner_1_sizing_rows",
			"inner_1_build_rows",
			"inner_1_build_spill_bytes",
			"inner_1_lookups_counted",
			"inner_1_lookup_hits",
			"inner_1_lookup_misses",
			"inner_1_lookup_form",
			"inner_1_lookup_inline_hits",
		] {
			band[fact] = receipt.get(fact).cloned().unwrap_or_default();
		}
		band["inner_1_residual_micros"] = serde_json::json!(residual);
		band["inner_1_residual_bound_micros"] = serde_json::json!(residual_bound);
		band["inner_1_residual_complete"] = serde_json::json!(residual_complete);
		band["inner_1_facts_consistent"] = serde_json::json!(consistent);
		pair_bands.push(band);
	}
	let off_range = required_f64(&base, "off_execute_range_ms")?;
	let off_median = required_f64(&base, "off_execute_median_ms")?;
	let owner_floor = required_f64(&base, "owner_floor_ms")?;
	let perturbation_accepted = base["perturbation_gate"] == "accepted";
	let base_profiles_complete = base["all_profiles_complete"] == true;
	let mut medians = ADR594_CHILDREN
		.iter()
		.map(|child| child.to_string())
		.chain(std::iter::once("inner_1_residual".to_string()))
		.map(|term| {
			let mut values = samples.get(&term).cloned().unwrap_or_default();
			(term, median(&mut values))
		})
		.collect::<Vec<_>>();
	medians.sort_by(|left, right| right.1.total_cmp(&left.1));
	let owner = if !unsupported_pairs.is_empty() {
		serde_json::json!({
			"status": "withheld_unsupported_path",
			"pairs": unsupported_pairs,
		})
	} else if !all_facts_consistent {
		serde_json::json!({
			"status": "withheld_instrument_incomplete",
			"incompleteness": incompleteness,
		})
	} else {
		adr594_owner(
			&medians,
			owner_floor,
			off_range,
			off_median,
			perturbation_accepted,
			base_profiles_complete && all_residuals_complete,
		)
	};
	let branches: BTreeSet<String> = pair_bands
		.iter()
		.filter_map(|band| {
			band.get("inner_1_branch")
				.and_then(serde_json::Value::as_str)
		})
		.map(str::to_string)
		.collect();
	base["adr594_pair_bands"] = serde_json::json!(pair_bands);
	base["all_inner_1_residuals_complete"] = serde_json::json!(all_residuals_complete);
	base["all_inner_1_facts_consistent"] = serde_json::json!(all_facts_consistent);
	base["inner_1_branches"] = serde_json::json!(branches);
	base["inner_1_terms_scaled_ms"] = serde_json::json!(
		medians
			.iter()
			.map(|(term, value)| {
				serde_json::json!({
					"term": term,
					"median_scaled_ms": value,
					"share_of_off_execute": value / off_median,
					"clears_floor": *value > owner_floor,
				})
			})
			.collect::<Vec<_>>()
	);
	base["adr585_fifteen_term_owner"] = base["owner"].clone();
	base["owner"] = owner;
	Ok(base)
}

/// ADR 594's rule: ADR 586's outcomes, except that a residual that leads is an incomplete
/// instrument at any size, not only above the floor. The children are supposed to name the
/// term; a residual ahead of every child says they do not, whatever the floor.
fn adr594_owner(
	medians: &[(String, f64)],
	owner_floor: f64,
	off_range: f64,
	off_median: f64,
	perturbation_accepted: bool,
	profiles_complete: bool,
) -> serde_json::Value {
	if perturbation_accepted
		&& profiles_complete
		&& let Some((name, value)) = medians.first()
		&& name.ends_with("_residual")
	{
		return serde_json::json!({
			"status": "withheld_instrument_incomplete",
			"leading_residual": name,
			"median_scaled_ms": value,
			"reason": "a residual leads the children at any size",
		});
	}
	adr586_owner(
		medians,
		owner_floor,
		off_range,
		off_median,
		perturbation_accepted,
		profiles_complete,
	)
}

/// ADR 594: re-run the evaluator and the cutoff over a retained journal, so a tightened rule
/// can be applied to retained rows without another run.
fn adr594_reevaluate_command(args: &[String]) -> ExitCode {
	let Some(samples) = flag_value(args, "--samples") else {
		eprintln!("adr594-reevaluate requires --samples PATH");
		return ExitCode::from(2);
	};
	let rows: Result<Vec<serde_json::Value>, String> = std::fs::read_to_string(samples)
		.map_err(|error| format!("read {samples}: {error}"))
		.and_then(|text| {
			text.lines()
				.filter(|line| !line.trim().is_empty())
				.map(|line| {
					serde_json::from_str(line).map_err(|error| format!("parse {samples}: {error}"))
				})
				.collect()
		});
	let summary = rows.and_then(|rows| {
		// The evaluator sees the pair rows only, as the driver hands them to it; the warmup is
		// journaled but never evaluated.
		let rows: Vec<serde_json::Value> = rows
			.into_iter()
			.filter(|row| row.get("warmup") != Some(&serde_json::Value::Bool(true)))
			.collect();
		let mut summary = evaluate_adr594_pairs(&rows)?;
		adr593_apply_variability_bound(&mut summary)?;
		summary["reevaluated_samples"] = serde_json::json!(samples);
		summary["reevaluating_evidence_identity"] = runtime_identity_json();
		Ok(summary)
	});
	match summary {
		Ok(summary) => {
			println!("{summary}");
			ExitCode::SUCCESS
		}
		Err(error) => {
			eprintln!("{error}");
			ExitCode::from(2)
		}
	}
}

fn run_adr586_q21_semi_write(args: &[String]) -> Result<serde_json::Value, String> {
	let mode = flag_value(args, "--mode").unwrap_or("strict");
	let sf = resolved_sf(args)?;
	let data_source = flag_value(args, "--data-source").unwrap_or("parquet");
	let data_dir = data_dir_from_args(args);
	let output_dir = PathBuf::from(
		flag_value(args, "--output-dir").unwrap_or("target/tpch/adr586-q21-semi-write"),
	);
	if mode != "strict" || sf != 8.0 || !matches!(data_source, "parquet" | "persisted") {
		return Err(format!(
			"ADR 586 requires --mode strict --sf 8 --data-source parquet; got mode={mode:?} sf={sf} data_source={data_source:?}"
		));
	}
	if output_dir.exists() {
		return Err(format!(
			"ADR 586 requires a fresh output directory; {} already exists",
			output_dir.display()
		));
	}
	if std::env::var_os("TORON_NODE_ATTRIBUTION_PROFILE_PATH").is_some() {
		return Err(
			"ADR 586 parent environment must not set TORON_NODE_ATTRIBUTION_PROFILE_PATH"
				.to_string(),
		);
	}
	let reference_off_median_ms = flag_value(args, "--reference-off-median-ms")
		.ok_or_else(|| {
			"ADR 586 requires --reference-off-median-ms, the ADR 585 unprofiled off median"
				.to_string()
		})?
		.parse::<f64>()
		.ok()
		.filter(|value| value.is_finite() && *value > 0.0)
		.ok_or_else(|| "ADR 586 reference off median must be a positive number".to_string())?;
	let budget = GaugeBudget::from_environment()?;
	if budget.memory_per_task != 2_147_483_648
		|| budget.memory_per_task_provenance != "explicit"
		|| budget.spill_bytes_reserved != 2_147_483_648
		|| budget.spill_bytes_reserved_provenance != "explicit"
	{
		return Err("ADR 586 requires explicit 2 GiB task memory and spill reserve".to_string());
	}
	let concurrency = gauge_partition_work_concurrency_requested()?;
	if concurrency != (1, "explicit") {
		return Err(format!(
			"ADR 586 requires explicit partition parallelism 1, got {concurrency:?}"
		));
	}
	let identity = runtime_identity()?.ok_or_else(|| {
		"ADR 586 must run through toron-bench build/run with a staged executable".to_string()
	})?;
	if identity.manifest.recipe.profile != "release" {
		return Err(format!(
			"ADR 586 requires a release build, got {:?}",
			identity.manifest.recipe.profile
		));
	}
	let host_start = observe_host_quiescence();
	if !host_start.quiescent() {
		return Err("ADR 586 requires the local deployment and SQL port quiescent".to_string());
	}
	let scratch = std::env::var_os("TORON_JOIN_SPILL_DIR")
		.filter(|value| !value.is_empty())
		.map(PathBuf::from)
		.unwrap_or_else(|| std::env::temp_dir().join("toron_join_spill"));
	if !directory_is_empty(&scratch)? {
		return Err(format!(
			"ADR 586 spill scratch {} is not empty before the run",
			scratch.display()
		));
	}
	let dataset = load_persisted_tpch_dataset(mode, sf, &data_dir)?;
	if dataset.verification.as_str() != "full_hash" {
		return Err(format!(
			"ADR 586 requires full-hash dataset verification, got {}",
			dataset.verification.as_str()
		));
	}
	let journal = prepare_perf_sample_journal(&output_dir)?;
	std::fs::create_dir_all(output_dir.join("profiles"))
		.map_err(|error| format!("create ADR 586 profile directory: {error}"))?;

	// Phase 0: one unprofiled warmup, one unprofiled sample, and the tripwire against the reference.
	// The sample enters no statistic; a tripped phase 0 ends the run with the finding on disk.
	let mut phase0_warmup = run_adr586_child(
		0,
		true,
		false,
		mode,
		sf,
		data_source,
		&data_dir,
		&output_dir,
	)?;
	phase0_warmup["adr586_phase"] = serde_json::json!("host_check_warmup");
	phase0_warmup["adr586_arm"] = serde_json::json!("off");
	phase0_warmup["adr586_pair"] = serde_json::Value::Null;
	journal.append(&phase0_warmup)?;
	let mut phase0_sample = run_adr586_child(
		1,
		false,
		false,
		mode,
		sf,
		data_source,
		&data_dir,
		&output_dir,
	)?;
	let phase0_execute_ms = required_f64(&phase0_sample, "execute_ms")?;
	let tripwire = adr586_phase0_tripwire(phase0_execute_ms, reference_off_median_ms);
	phase0_sample["adr586_phase"] = serde_json::json!("host_check");
	phase0_sample["adr586_arm"] = serde_json::json!("off");
	phase0_sample["adr586_pair"] = serde_json::Value::Null;
	phase0_sample["adr586_phase0_tripwire"] = tripwire.clone();
	journal.append(&phase0_sample)?;
	let phase0 = serde_json::json!({
		"warmup_execute_ms": phase0_warmup.get("execute_ms").cloned().unwrap_or_default(),
		"sample_execute_ms": phase0_execute_ms,
		"reference_off_median_ms": reference_off_median_ms,
		"tripwire": tripwire,
	});
	if phase0["tripwire"]["tripped"] == true {
		let summary = serde_json::json!({
			"kind": "adr586_q21_semi_write_decomposition",
			"outcome": "phase0_tripped",
			"sealed": false,
			"candidate_authority": false,
			"phase0": phase0,
			"instrumented_evidence_identity": identity.json_value(),
			"dataset_manifest_fingerprint": dataset.manifest_fingerprint,
		});
		write_json_atomically(&output_dir.join("summary.json"), &summary)?;
		return Err(format!(
			"ADR 586 phase 0 tripped: sample {phase0_execute_ms:.1} ms is {:+.2}% from the reference {reference_off_median_ms:.1} ms (threshold {:.0}%); pairs not run, finding retained at {}",
			phase0["tripwire"]["movement"].as_f64().unwrap_or(f64::NAN) * 100.0,
			ADR586_PHASE0_THRESHOLD * 100.0,
			output_dir.display()
		));
	}

	let mut warmup = run_adr586_child(
		2,
		true,
		false,
		mode,
		sf,
		data_source,
		&data_dir,
		&output_dir,
	)?;
	warmup["adr586_phase"] = serde_json::json!("pairs_warmup");
	warmup["adr586_arm"] = serde_json::json!("off");
	warmup["adr586_pair"] = serde_json::Value::Null;
	journal.append(&warmup)?;

	let mut rows = Vec::with_capacity(ADR585_PAIRS * 2);
	for pair in 0..ADR585_PAIRS {
		let order = if pair % 2 == 0 {
			[false, true]
		} else {
			[true, false]
		};
		for (position, on) in order.into_iter().enumerate() {
			let mut row = run_adr586_child(
				pair as u32,
				false,
				on,
				mode,
				sf,
				data_source,
				&data_dir,
				&output_dir,
			)?;
			row["adr586_phase"] = serde_json::json!("pairs");
			row["adr586_arm"] = serde_json::json!(if on { "on" } else { "off" });
			row["adr586_pair"] = serde_json::json!(pair);
			row["adr586_pair_position"] = serde_json::json!(position);
			journal.append(&row)?;
			rows.push(row);
		}
	}
	let host_end = observe_host_quiescence();
	if !host_end.quiescent() || !directory_is_empty(&scratch)? {
		return Err(
			"ADR 586 lost host quiescence or left spill scratch after measurement".to_string(),
		);
	}
	let mut summary = evaluate_adr586_pairs(&rows)?;
	summary["kind"] = serde_json::json!("adr586_q21_semi_write_decomposition");
	summary["outcome"] = serde_json::json!("pairs_measured");
	summary["sealed"] = serde_json::json!(false);
	summary["candidate_authority"] = serde_json::json!(false);
	summary["phase0"] = phase0;
	summary["sealed_baseline_executable_sha256"] =
		serde_json::json!(ADR585_SEALED_BASELINE_EXECUTABLE);
	summary["adr585_instrumented_executable_sha256"] = serde_json::json!(ADR586_ADR585_EXECUTABLE);
	summary["instrumented_evidence_identity"] = identity.json_value();
	summary["dataset_manifest_fingerprint"] = serde_json::json!(dataset.manifest_fingerprint);
	summary["memory_per_task"] = serde_json::json!(budget.memory_per_task);
	summary["spill_bytes_reserved"] = serde_json::json!(budget.spill_bytes_reserved);
	summary["partition_work_concurrency_requested"] = serde_json::json!(concurrency.0);
	summary["warmup_execute_ms"] = warmup.get("execute_ms").cloned().unwrap_or_default();
	write_json_atomically(&output_dir.join("summary.json"), &summary)?;
	Ok(summary)
}

fn run_adr587_q21_insert(args: &[String]) -> Result<serde_json::Value, String> {
	let mode = flag_value(args, "--mode").unwrap_or("strict");
	let sf = resolved_sf(args)?;
	let data_source = flag_value(args, "--data-source").unwrap_or("parquet");
	let data_dir = data_dir_from_args(args);
	let output_dir =
		PathBuf::from(flag_value(args, "--output-dir").unwrap_or("target/tpch/adr587-q21-insert"));
	if mode != "strict" || sf != 8.0 || !matches!(data_source, "parquet" | "persisted") {
		return Err(format!(
			"ADR 587 requires --mode strict --sf 8 --data-source parquet; got mode={mode:?} sf={sf} data_source={data_source:?}"
		));
	}
	if output_dir.exists() {
		return Err(format!(
			"ADR 587 requires a fresh output directory; {} already exists",
			output_dir.display()
		));
	}
	if std::env::var_os("TORON_NODE_ATTRIBUTION_PROFILE_PATH").is_some() {
		return Err(
			"ADR 587 parent environment must not set TORON_NODE_ATTRIBUTION_PROFILE_PATH"
				.to_string(),
		);
	}
	let reference_off_median_ms = flag_value(args, "--reference-off-median-ms")
		.ok_or_else(|| {
			"ADR 587 requires --reference-off-median-ms, the ADR 586 unprofiled off median"
				.to_string()
		})?
		.parse::<f64>()
		.ok()
		.filter(|value| value.is_finite() && *value > 0.0)
		.ok_or_else(|| "ADR 587 reference off median must be a positive number".to_string())?;
	let budget = GaugeBudget::from_environment()?;
	if budget.memory_per_task != 2_147_483_648
		|| budget.memory_per_task_provenance != "explicit"
		|| budget.spill_bytes_reserved != 2_147_483_648
		|| budget.spill_bytes_reserved_provenance != "explicit"
	{
		return Err("ADR 587 requires explicit 2 GiB task memory and spill reserve".to_string());
	}
	let concurrency = gauge_partition_work_concurrency_requested()?;
	if concurrency != (1, "explicit") {
		return Err(format!(
			"ADR 587 requires explicit partition parallelism 1, got {concurrency:?}"
		));
	}
	let identity = runtime_identity()?.ok_or_else(|| {
		"ADR 587 must run through toron-bench build/run with a staged executable".to_string()
	})?;
	if identity.manifest.recipe.profile != "profiling" {
		return Err(format!(
			"ADR 587 requires the symbol-bearing profiling build profile, got {:?}",
			identity.manifest.recipe.profile
		));
	}
	let host_start = observe_host_quiescence();
	if !host_start.quiescent() {
		return Err("ADR 587 requires the local deployment and SQL port quiescent".to_string());
	}
	let scratch = std::env::var_os("TORON_JOIN_SPILL_DIR")
		.filter(|value| !value.is_empty())
		.map(PathBuf::from)
		.unwrap_or_else(|| std::env::temp_dir().join("toron_join_spill"));
	if !directory_is_empty(&scratch)? {
		return Err(format!(
			"ADR 587 spill scratch {} is not empty before the run",
			scratch.display()
		));
	}
	let dataset = load_persisted_tpch_dataset(mode, sf, &data_dir)?;
	if dataset.verification.as_str() != "full_hash" {
		return Err(format!(
			"ADR 587 requires full-hash dataset verification, got {}",
			dataset.verification.as_str()
		));
	}
	let journal = prepare_perf_sample_journal(&output_dir)?;
	std::fs::create_dir_all(output_dir.join("profiles"))
		.map_err(|error| format!("create ADR 587 profile directory: {error}"))?;

	// The profiler gates: present, permitted, and able to sample this harness without loss.
	let perf = adr587_perf_gate(&output_dir)?;

	let mut phase0_warmup = run_adr587_child(
		0,
		true,
		false,
		mode,
		sf,
		data_source,
		&data_dir,
		&output_dir,
	)?;
	phase0_warmup["adr587_phase"] = serde_json::json!("host_check_warmup");
	phase0_warmup["adr587_arm"] = serde_json::json!("off");
	phase0_warmup["adr587_pair"] = serde_json::Value::Null;
	journal.append(&phase0_warmup)?;
	let mut phase0_sample = run_adr587_child(
		1,
		false,
		false,
		mode,
		sf,
		data_source,
		&data_dir,
		&output_dir,
	)?;
	let phase0_execute_ms = required_f64(&phase0_sample, "execute_ms")?;
	let tripwire = adr586_phase0_tripwire(phase0_execute_ms, reference_off_median_ms);
	phase0_sample["adr587_phase"] = serde_json::json!("host_check");
	phase0_sample["adr587_arm"] = serde_json::json!("off");
	phase0_sample["adr587_pair"] = serde_json::Value::Null;
	phase0_sample["adr587_phase0_tripwire"] = tripwire.clone();
	journal.append(&phase0_sample)?;
	let phase0 = serde_json::json!({
		"warmup_execute_ms": phase0_warmup.get("execute_ms").cloned().unwrap_or_default(),
		"sample_execute_ms": phase0_execute_ms,
		"reference_off_median_ms": reference_off_median_ms,
		"tripwire": tripwire,
	});
	if phase0["tripwire"]["tripped"] == true {
		let summary = serde_json::json!({
			"kind": "adr587_q21_insert_decomposition",
			"outcome": "phase0_tripped",
			"sealed": false,
			"candidate_authority": false,
			"phase0": phase0,
			"perf": perf,
			"instrumented_evidence_identity": identity.json_value(),
			"dataset_manifest_fingerprint": dataset.manifest_fingerprint,
		});
		write_json_atomically(&output_dir.join("summary.json"), &summary)?;
		return Err(format!(
			"ADR 587 phase 0 tripped: sample {phase0_execute_ms:.1} ms is {:+.2}% from the reference {reference_off_median_ms:.1} ms (threshold {:.0}%); pairs not run, finding retained at {}",
			phase0["tripwire"]["movement"].as_f64().unwrap_or(f64::NAN) * 100.0,
			ADR586_PHASE0_THRESHOLD * 100.0,
			output_dir.display()
		));
	}

	let mut warmup = run_adr587_child(
		2,
		true,
		false,
		mode,
		sf,
		data_source,
		&data_dir,
		&output_dir,
	)?;
	warmup["adr587_phase"] = serde_json::json!("pairs_warmup");
	warmup["adr587_arm"] = serde_json::json!("off");
	warmup["adr587_pair"] = serde_json::Value::Null;
	journal.append(&warmup)?;

	let mut rows = Vec::with_capacity(ADR585_PAIRS * 2);
	for pair in 0..ADR585_PAIRS {
		let order = if pair % 2 == 0 {
			[false, true]
		} else {
			[true, false]
		};
		for (position, on) in order.into_iter().enumerate() {
			let mut row = run_adr587_child(
				pair as u32,
				false,
				on,
				mode,
				sf,
				data_source,
				&data_dir,
				&output_dir,
			)?;
			row["adr587_phase"] = serde_json::json!("pairs");
			row["adr587_arm"] = serde_json::json!(if on { "on" } else { "off" });
			row["adr587_pair"] = serde_json::json!(pair);
			row["adr587_pair_position"] = serde_json::json!(position);
			if on {
				// Bin the profiler's samples against this arm's windows now, so the bins ride
				// the journal row and the evaluator needs nothing outside the rows.
				let perf_data = adr587_perf_data_path(&output_dir, pair as u32);
				let (_, receipt) = lifecycle_target_receipt(&row, pair, "ADR 587")?;
				let windows = adr587_window_sets_from_row(&row)?;
				let bins = adr587_bin_perf_data(&perf_data, &windows, receipt)?;
				write_json_atomically(
					&output_dir
						.join("profiles")
						.join(format!("pair-{pair:02}.bins.json")),
					&bins,
				)?;
				row["adr587_bins"] = bins;
			}
			journal.append(&row)?;
			rows.push(row);
		}
	}
	let host_end = observe_host_quiescence();
	if !host_end.quiescent() || !directory_is_empty(&scratch)? {
		return Err(
			"ADR 587 lost host quiescence or left spill scratch after measurement".to_string(),
		);
	}
	let mut summary = evaluate_adr587_pairs(&rows)?;
	summary["kind"] = serde_json::json!("adr587_q21_insert_decomposition");
	summary["outcome"] = serde_json::json!("pairs_measured");
	summary["sealed"] = serde_json::json!(false);
	summary["candidate_authority"] = serde_json::json!(false);
	summary["phase0"] = phase0;
	summary["perf"] = perf;
	summary["sealed_baseline_executable_sha256"] =
		serde_json::json!(ADR585_SEALED_BASELINE_EXECUTABLE);
	summary["adr585_instrumented_executable_sha256"] = serde_json::json!(ADR586_ADR585_EXECUTABLE);
	summary["adr586_instrumented_executable_sha256"] = serde_json::json!(ADR587_ADR586_EXECUTABLE);
	summary["instrumented_evidence_identity"] = identity.json_value();
	summary["build_profile"] = serde_json::json!(identity.manifest.recipe.profile);
	summary["dataset_manifest_fingerprint"] = serde_json::json!(dataset.manifest_fingerprint);
	summary["memory_per_task"] = serde_json::json!(budget.memory_per_task);
	summary["spill_bytes_reserved"] = serde_json::json!(budget.spill_bytes_reserved);
	summary["partition_work_concurrency_requested"] = serde_json::json!(concurrency.0);
	summary["warmup_execute_ms"] = warmup.get("execute_ms").cloned().unwrap_or_default();
	write_json_atomically(&output_dir.join("summary.json"), &summary)?;
	Ok(summary)
}

fn adr587_perf_data_path(output_dir: &Path, pair: u32) -> PathBuf {
	output_dir
		.join("profiles")
		.join(format!("pair-{pair:02}.perf.data"))
}

fn adr587_perf_record_args(frequency_hz: u32, output: &Path) -> Vec<String> {
	vec![
		"perf".to_string(),
		"record".to_string(),
		"-e".to_string(),
		"task-clock:u".to_string(),
		"-F".to_string(),
		frequency_hz.to_string(),
		"-k".to_string(),
		"CLOCK_MONOTONIC".to_string(),
		"-o".to_string(),
		output.display().to_string(),
		"--".to_string(),
	]
}

/// The profiler gates: `perf` on the path, `perf_event_paranoid` at most 2, and a one-second
/// recording of this harness spinning that loses no samples and produces some. Records the
/// `perf` version and the sampling command the pairs will use.
fn adr587_perf_gate(output_dir: &Path) -> Result<serde_json::Value, String> {
	let version = Command::new("perf")
		.arg("version")
		.output()
		.map_err(|error| format!("ADR 587 requires perf on the path: {error}"))?;
	let version = String::from_utf8_lossy(&version.stdout).trim().to_string();
	let paranoid = std::fs::read_to_string("/proc/sys/kernel/perf_event_paranoid")
		.map_err(|error| format!("ADR 587 cannot read perf_event_paranoid: {error}"))?
		.trim()
		.parse::<i64>()
		.map_err(|error| format!("ADR 587 cannot parse perf_event_paranoid: {error}"))?;
	if paranoid > ADR587_MAX_PARANOID {
		return Err(format!(
			"ADR 587 requires kernel.perf_event_paranoid <= {ADR587_MAX_PARANOID}, got {paranoid}"
		));
	}
	let selfcheck = output_dir.join("profiles").join("selfcheck.perf.data");
	let exe = std::env::current_exe().map_err(|error| format!("current exe: {error}"))?;
	let mut args = adr587_perf_record_args(ADR587_PERF_FREQUENCY_HZ, &selfcheck);
	args.push(exe.display().to_string());
	args.push("adr587-selfcheck-spin".to_string());
	let status = Command::new(&args[0])
		.args(&args[1..])
		.stdout(Stdio::null())
		.stderr(Stdio::null())
		.status()
		.map_err(|error| format!("ADR 587 perf self-check failed to start: {error}"))?;
	if !status.success() {
		return Err(format!(
			"ADR 587 perf self-check recording failed: {status}"
		));
	}
	let header = adr587_perf_header(&selfcheck)?;
	if header.lost_samples != 0 || header.samples == 0 {
		return Err(format!(
			"ADR 587 perf self-check lost {} samples of {}",
			header.lost_samples, header.samples
		));
	}
	Ok(serde_json::json!({
		"version": version,
		"perf_event_paranoid": paranoid,
		"frequency_hz": ADR587_PERF_FREQUENCY_HZ,
		"record_args": adr587_perf_record_args(ADR587_PERF_FREQUENCY_HZ, Path::new("<profiles/pair-NN.perf.data>")),
		"script_fields": "--ns -F pid,tid,time,period,ip,sym,srcline",
		"selfcheck_samples": header.samples,
		"selfcheck_lost_samples": header.lost_samples,
	}))
}

struct Adr587PerfHeader {
	samples: u64,
	lost_samples: u64,
}

/// `perf report` prints "# Total Lost Samples: N" and "# Samples: N of event" in its header;
/// both are required, and a header without them is refused rather than read as zero.
fn adr587_perf_header(path: &Path) -> Result<Adr587PerfHeader, String> {
	let output = Command::new("perf")
		.args(["report", "-i"])
		.arg(path)
		.args(["--stdio", "--sort", "comm", "--percent-limit", "100"])
		.output()
		.map_err(|error| format!("ADR 587 perf report failed to start: {error}"))?;
	let text = String::from_utf8_lossy(&output.stdout).to_string();
	adr587_parse_perf_header(&text)
}

fn adr587_parse_perf_header(text: &str) -> Result<Adr587PerfHeader, String> {
	let mut samples = None;
	let mut lost = None;
	for line in text.lines() {
		let line = line.trim();
		if let Some(rest) = line.strip_prefix("# Total Lost Samples:") {
			lost = rest.trim().parse::<u64>().ok();
		} else if let Some(rest) = line.strip_prefix("# Samples:") {
			let count = rest.trim().split_whitespace().next().unwrap_or_default();
			samples = adr587_parse_perf_count(count);
		}
	}
	match (samples, lost) {
		(Some(samples), Some(lost_samples)) => Ok(Adr587PerfHeader {
			samples,
			lost_samples,
		}),
		_ => Err("ADR 587 perf report header lacks the sample or lost-sample count".to_string()),
	}
}

/// perf abbreviates sample counts ("34K", "1M"); the abbreviation is a floor, which is enough
/// for the gates that read it (zero, or at least some).
fn adr587_parse_perf_count(text: &str) -> Option<u64> {
	let (digits, scale) = match text.chars().last() {
		Some('K') => (&text[..text.len() - 1], 1_000u64),
		Some('M') => (&text[..text.len() - 1], 1_000_000u64),
		Some('G') => (&text[..text.len() - 1], 1_000_000_000u64),
		_ => (text, 1u64),
	};
	digits.parse::<u64>().ok().map(|value| value * scale)
}

/// One profiler sample as `perf script --ns -F pid,tid,time,period,ip,sym,srcline` prints it:
/// a header line and a source-line line.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Adr587Sample {
	pid: u64,
	tid: u64,
	time_ns: u64,
	period: u64,
	symbol: String,
	source_file: String,
	line: Option<u32>,
}

fn adr587_parse_perf_script(text: &str) -> Result<Vec<Adr587Sample>, String> {
	let mut samples = Vec::new();
	let mut lines = text.lines().peekable();
	while let Some(line) = lines.next() {
		let header = line.trim();
		if header.is_empty() {
			continue;
		}
		let mut fields = header.split_whitespace();
		let ids = fields
			.next()
			.ok_or_else(|| format!("perf script sample lacks pid/tid: {header:?}"))?;
		let (pid, tid) = ids
			.split_once('/')
			.ok_or_else(|| format!("perf script pid/tid is not pid/tid: {ids:?}"))?;
		let time = fields
			.next()
			.ok_or_else(|| format!("perf script sample lacks time: {header:?}"))?
			.trim_end_matches(':');
		let (secs, nanos) = time
			.split_once('.')
			.ok_or_else(|| format!("perf script time lacks nanoseconds: {time:?}"))?;
		if nanos.len() != 9 {
			return Err(format!("perf script time is not in --ns form: {time:?}"));
		}
		let time_ns = secs
			.parse::<u64>()
			.ok()
			.zip(nanos.parse::<u64>().ok())
			.map(|(s, n)| s * 1_000_000_000 + n)
			.ok_or_else(|| format!("perf script time is not numeric: {time:?}"))?;
		let period = fields
			.next()
			.and_then(|value| value.parse::<u64>().ok())
			.ok_or_else(|| format!("perf script sample lacks period: {header:?}"))?;
		let _ip = fields
			.next()
			.ok_or_else(|| format!("perf script sample lacks ip: {header:?}"))?;
		let symbol = fields.collect::<Vec<_>>().join(" ");
		let (source_file, line_number) = match lines.peek() {
			Some(next) if next.starts_with(' ') || next.starts_with('\t') => {
				let source = lines.next().unwrap_or_default().trim();
				match source.rsplit_once(':') {
					Some((file, number)) => {
						let file = file.rsplit('/').next().unwrap_or(file).to_string();
						(file, number.parse::<u32>().ok())
					}
					None => (source.to_string(), None),
				}
			}
			_ => (String::new(), None),
		};
		samples.push(Adr587Sample {
			pid: pid
				.parse()
				.map_err(|_| format!("perf script pid is not numeric: {pid:?}"))?,
			tid: tid
				.parse()
				.map_err(|_| format!("perf script tid is not numeric: {tid:?}"))?,
			time_ns,
			period,
			symbol,
			source_file,
			line: line_number,
		});
	}
	Ok(samples)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Adr587Window {
	start_ns: u64,
	end_ns: u64,
	tid: u64,
}

/// One summary's windows as the engine published them: rows windows (ordered, disjoint) and
/// growth windows nested inside them, all on the recorded PID.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Adr587WindowSet {
	summary: &'static str,
	pid: u64,
	rows: Vec<Adr587Window>,
	growth: Vec<Adr587Window>,
}

impl Adr587WindowSet {
	fn from_diagnostic(summary: &'static str, value: &serde_json::Value) -> Result<Self, String> {
		let pid = value
			.get("pid")
			.and_then(serde_json::Value::as_u64)
			.ok_or_else(|| format!("{summary} insert windows lack pid"))?;
		let read = |key: &str| -> Result<Vec<Adr587Window>, String> {
			let mut windows = value
				.get(key)
				.and_then(serde_json::Value::as_array)
				.ok_or_else(|| format!("{summary} insert windows lack {key}"))?
				.iter()
				.map(|entry| {
					let field = |index: usize| {
						entry
							.get(index)
							.and_then(serde_json::Value::as_u64)
							.ok_or_else(|| format!("{summary} {key} entry lacks field {index}"))
					};
					Ok(Adr587Window {
						start_ns: field(0)?,
						end_ns: field(1)?,
						tid: field(2)?,
					})
				})
				.collect::<Result<Vec<_>, String>>()?;
			windows.sort_by_key(|window| window.start_ns);
			for pair in windows.windows(2) {
				if pair[0].end_ns > pair[1].start_ns {
					return Err(format!("{summary} {key} overlap"));
				}
			}
			Ok(windows)
		};
		let rows_text = value
			.get("rows_windows")
			.and_then(serde_json::Value::as_str)
			.ok_or_else(|| format!("{summary} insert windows lack the rows_windows string"))?;
		let rows = adr587_decode_rows_windows(rows_text)
			.map_err(|error| format!("{summary} rows windows: {error}"))?;
		if let Some(count) = value
			.get("rows_window_count")
			.and_then(serde_json::Value::as_u64)
			&& count != rows.len() as u64
		{
			return Err(format!(
				"{summary} rows window count {count} disagrees with {} decoded windows",
				rows.len()
			));
		}
		for pair in rows.windows(2) {
			if pair[0].end_ns > pair[1].start_ns {
				return Err(format!("{summary} rows_windows overlap"));
			}
		}
		Ok(Self {
			summary,
			pid,
			rows,
			growth: read("growth_windows")?,
		})
	}

	fn tids(&self) -> Vec<u64> {
		let mut tids = self
			.rows
			.iter()
			.map(|window| window.tid)
			.collect::<Vec<_>>();
		tids.sort_unstable();
		tids.dedup();
		tids
	}

	/// Half-open containment on the window's own thread. Windows are sorted and disjoint, so a
	/// binary search on the start finds the only candidate.
	fn find(windows: &[Adr587Window], sample: &Adr587Sample) -> bool {
		let index = windows.partition_point(|window| window.start_ns <= sample.time_ns);
		index
			.checked_sub(1)
			.and_then(|index| windows.get(index))
			.is_some_and(|window| {
				window.tid == sample.tid
					&& sample.time_ns >= window.start_ns
					&& sample.time_ns < window.end_ns
			})
	}
}

/// The exact inverse of the engine's `encode_rows_windows`: `<tid>@<start_ns>:<gap>+<duration>;...`
/// with `|` between per-thread segments, gaps measured from the previous window's end.
fn adr587_decode_rows_windows(text: &str) -> Result<Vec<Adr587Window>, String> {
	let mut windows = Vec::new();
	if text.is_empty() {
		return Ok(windows);
	}
	for segment in text.split('|') {
		let (head, body) = segment
			.split_once(':')
			.ok_or_else(|| "segment lacks ':'".to_string())?;
		let (tid, start) = head
			.split_once('@')
			.ok_or_else(|| "segment lacks '@'".to_string())?;
		let tid = tid
			.parse::<u64>()
			.map_err(|_| "tid is not numeric".to_string())?;
		let mut cursor = start
			.parse::<u64>()
			.map_err(|_| "start is not numeric".to_string())?;
		for item in body.split(';') {
			let (gap, duration) = item
				.split_once('+')
				.ok_or_else(|| "item lacks '+'".to_string())?;
			let gap = gap
				.parse::<u64>()
				.map_err(|_| "gap is not numeric".to_string())?;
			let duration = duration
				.parse::<u64>()
				.map_err(|_| "duration is not numeric".to_string())?;
			let start_ns = cursor
				.checked_add(gap)
				.ok_or_else(|| "overflow".to_string())?;
			let end_ns = start_ns
				.checked_add(duration)
				.ok_or_else(|| "overflow".to_string())?;
			windows.push(Adr587Window {
				start_ns,
				end_ns,
				tid,
			});
			cursor = end_ns;
		}
	}
	Ok(windows)
}

fn adr587_window_sets_from_row(row: &serde_json::Value) -> Result<[Adr587WindowSet; 2], String> {
	let invocation = row
		.get("node_attribution")
		.and_then(|profile| profile.get("aggregate_invocations"))
		.and_then(serde_json::Value::as_array)
		.and_then(|invocations| {
			invocations.iter().find(|invocation| {
				invocation.get("route").and_then(serde_json::Value::as_str)
					== Some("inner_prefix_semi_anti_grouped")
			})
		})
		.ok_or_else(|| "ADR 587 row lacks the target invocation".to_string())?;
	let diagnostic = |name: &str| {
		invocation
			.get("diagnostics")
			.and_then(serde_json::Value::as_array)
			.and_then(|entries| {
				entries.iter().find(|entry| {
					entry.get("name").and_then(serde_json::Value::as_str) == Some(name)
				})
			})
			.and_then(|entry| entry.get("value"))
			.ok_or_else(|| format!("ADR 587 row lacks diagnostic {name}"))
	};
	Ok([
		Adr587WindowSet::from_diagnostic("semi", diagnostic("semi_insert_windows")?)?,
		Adr587WindowSet::from_diagnostic("anti", diagnostic("anti_insert_windows")?)?,
	])
}

/// The source-line classes, fixed from the measured tree: inclusive line ranges in
/// `toron/src/exec/join/streaming.rs`, each anchored on a function whose name the pinning test
/// checks at the range's first line. Growth is chosen by window before any of these apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Adr587LineRange {
	class: &'static str,
	start: u32,
	end: u32,
	anchor: &'static str,
}

const ADR587_SOURCE_FILE: &str = "streaming.rs";
const ADR587_LINE_CLASSES: [Adr587LineRange; 17] = [
	// ADR 589: construction, entry room, and reservation of the order-aware build.
	Adr587LineRange {
		class: "ordered",
		start: 1705,
		end: 1800,
		anchor: "fn new_ordered() -> Self {",
	},
	// ADR 589: the order-aware row path, fast and fallback, and the entry push.
	Adr587LineRange {
		class: "ordered",
		start: 1801,
		end: 1979,
		anchor: "fn insert_ordered<const INSTRUMENTED: bool>(",
	},
	// ADR 589: the once-built index (its own windows; classed here for samples outside them).
	Adr587LineRange {
		class: "finish",
		start: 1980,
		end: 2100,
		anchor: "fn finish_index(",
	},
	// insert_with: entry, row count, last-key compare, cache-hit branch, the ordered route.
	Adr587LineRange {
		class: "shared",
		start: 2125,
		end: 2159,
		anchor: "fn insert_with<const INSTRUMENTED: bool>(",
	},
	// insert_with: the lookup counters, the lookup, and the found path.
	Adr587LineRange {
		class: "hash_and_probe",
		start: 2160,
		end: 2182,
		anchor: "if INSTRUMENTED {",
	},
	// insert_with: the capacity check that precedes growth (growth itself is a window).
	Adr587LineRange {
		class: "hash_and_probe",
		start: 2183,
		end: 2194,
		anchor: "let grew = self.entries.len() == self.entry_limit;",
	},
	// insert_with: the second walk and the append.
	Adr587LineRange {
		class: "hash_and_probe",
		start: 2195,
		end: 2239,
		anchor: "let entry_index =",
	},
	Adr587LineRange {
		class: "shared",
		start: 2240,
		end: 2249,
		anchor: "fn observe_value(&mut self, entry_index: usize, value: i64) {",
	},
	// write_batch_with: the column checks, the row loop, and the ADR 589 census around it.
	Adr587LineRange {
		class: "shared",
		start: 2340,
		end: 2464,
		anchor: "fn write_batch_with<const INSTRUMENTED: bool>(",
	},
	Adr587LineRange {
		class: "growth_other",
		start: 2465,
		end: 2505,
		anchor: "fn grow(&mut self, baseline_bytes: u64, cap: u64) -> Result<(), ExecutionError> {",
	},
	Adr587LineRange {
		class: "growth_other",
		start: 2506,
		end: 2598,
		anchor: "fn resize_capacity(",
	},
	Adr587LineRange {
		class: "rehash",
		start: 2599,
		end: 2605,
		anchor: "for (entry_index, entry) in self.entries.iter().enumerate() {",
	},
	Adr587LineRange {
		class: "growth_other",
		start: 2606,
		end: 2620,
		anchor: "",
	},
	Adr587LineRange {
		class: "hash_and_probe",
		start: 2621,
		end: 2651,
		anchor: "fn lookup_entry_index(&self, key: i64) -> Option<usize> {",
	},
	Adr587LineRange {
		class: "hash_and_probe",
		start: 2652,
		end: 2710,
		anchor: "fn find_bucket(&self, key: i64) -> Result<usize, ExecutionError> {",
	},
	Adr587LineRange {
		class: "shared",
		start: 2711,
		end: 2720,
		anchor: "fn is_multi(&self, entry_index: usize) -> bool {",
	},
	Adr587LineRange {
		class: "shared",
		start: 2721,
		end: 2728,
		anchor: "fn set_multi(&mut self, entry_index: usize) {",
	},
];
/// Lines inside `resize_capacity` that copy or initialise the grown arrays.
const ADR587_COPY_LINES: [u32; 4] = [2539, 2565, 2567, 2597];

fn adr587_is_copy_symbol(symbol: &str) -> bool {
	[
		"__memmove",
		"__memcpy",
		"__memset",
		"memcpy",
		"memmove",
		"memset",
	]
	.iter()
	.any(|prefix| symbol.starts_with(prefix))
}

/// Class of one sample already known to lie inside a rows window, outside every growth window.
fn adr587_rows_class(sample: &Adr587Sample) -> &'static str {
	if sample.symbol.contains("find_bucket_in") {
		return "hash_and_probe";
	}
	if sample.source_file != ADR587_SOURCE_FILE {
		return "other";
	}
	let Some(line) = sample.line else {
		return "other";
	};
	ADR587_LINE_CLASSES
		.iter()
		.find(|range| line >= range.start && line <= range.end)
		.map(|range| match range.class {
			"shared" | "hash_and_probe" | "ordered" | "finish" => range.class,
			_ => "other",
		})
		.unwrap_or("other")
}

/// Class of one sample already known to lie inside a growth window.
fn adr587_growth_class(sample: &Adr587Sample) -> &'static str {
	if adr587_is_copy_symbol(&sample.symbol) {
		return "copy";
	}
	if sample.symbol.contains("find_bucket_in") {
		return "rehash";
	}
	if sample.source_file == ADR587_SOURCE_FILE
		&& let Some(line) = sample.line
	{
		if ADR587_COPY_LINES.contains(&line) {
			return "copy";
		}
		if let Some(range) = ADR587_LINE_CLASSES
			.iter()
			.find(|range| line >= range.start && line <= range.end)
			&& range.class == "rehash"
		{
			return "rehash";
		}
	}
	"growth_other"
}

/// ADR 589 adds `ordered` (the order-aware row path) and `finish` (index-build lines sampled
/// outside their windows) as rows classes; a hashed build has zero samples in both.
const ADR587_ROWS_CLASSES: [&str; 5] = ["hash_and_probe", "ordered", "finish", "shared", "other"];
const ADR587_GROWTH_CLASSES: [&str; 3] = ["copy", "rehash", "growth_other"];

#[derive(Default, Debug, Clone)]
struct Adr587ClassBin {
	samples: u64,
	period_ns: u64,
}

/// Bin parsed samples against one summary's windows: exclusive growth first, then rows.
fn adr587_bin_summary(samples: &[Adr587Sample], windows: &Adr587WindowSet) -> serde_json::Value {
	let mut classes: BTreeMap<&'static str, Adr587ClassBin> = BTreeMap::new();
	for class in ADR587_ROWS_CLASSES
		.iter()
		.chain(ADR587_GROWTH_CLASSES.iter())
	{
		classes.insert(class, Adr587ClassBin::default());
	}
	let tids = windows.tids();
	let mut on_thread_outside = Adr587ClassBin::default();
	let mut other_threads = Adr587ClassBin::default();
	let mut growth_total = Adr587ClassBin::default();
	let mut rows_total = Adr587ClassBin::default();
	for sample in samples {
		if sample.pid != windows.pid {
			continue;
		}
		if !tids.contains(&sample.tid) {
			other_threads.samples += 1;
			other_threads.period_ns += sample.period;
			continue;
		}
		let class = if Adr587WindowSet::find(&windows.growth, sample) {
			growth_total.samples += 1;
			growth_total.period_ns += sample.period;
			rows_total.samples += 1;
			rows_total.period_ns += sample.period;
			adr587_growth_class(sample)
		} else if Adr587WindowSet::find(&windows.rows, sample) {
			rows_total.samples += 1;
			rows_total.period_ns += sample.period;
			adr587_rows_class(sample)
		} else {
			on_thread_outside.samples += 1;
			on_thread_outside.period_ns += sample.period;
			continue;
		};
		let bin = classes.entry(class).or_default();
		bin.samples += 1;
		bin.period_ns += sample.period;
	}
	let bin_value = |bin: &Adr587ClassBin| serde_json::json!({"samples": bin.samples, "period_ns": bin.period_ns});
	// Every thread the windows name must appear in the profile; a summary spanning two threads
	// with one of them unsampled would otherwise pass with half its windows empty.
	let sampled_tids = samples
		.iter()
		.filter(|sample| sample.pid == windows.pid)
		.map(|sample| sample.tid)
		.collect::<BTreeSet<_>>();
	let missing_tids = tids
		.iter()
		.copied()
		.filter(|tid| !sampled_tids.contains(tid))
		.collect::<Vec<_>>();
	serde_json::json!({
		"summary": windows.summary,
		"pid": windows.pid,
		"tids": tids,
		"missing_tids": missing_tids,
		"thread_present": !tids.is_empty() && missing_tids.is_empty(),
		"rows_windows": windows.rows.len(),
		"growth_windows": windows.growth.len(),
		"rows_inclusive": bin_value(&rows_total),
		"growth": bin_value(&growth_total),
		"classes": classes.iter().map(|(class, bin)| (class.to_string(), bin_value(bin))).collect::<serde_json::Map<_, _>>(),
		"on_thread_outside_windows": bin_value(&on_thread_outside),
		"other_threads": bin_value(&other_threads),
	})
}

fn adr587_bin_samples(
	samples: &[Adr587Sample],
	windows: &[Adr587WindowSet; 2],
	receipt: &serde_json::Value,
	header: &Adr587PerfHeader,
) -> Result<serde_json::Value, String> {
	let mut summaries = serde_json::Map::new();
	for set in windows {
		let mut bin = adr587_bin_summary(samples, set);
		let rows_micros = receipt_u64(receipt, &format!("{}_insert_rows_micros", set.summary))?;
		let grow_micros = receipt_u64(receipt, &format!("{}_insert_grow_micros", set.summary))?;
		let sampled = bin["rows_inclusive"]["period_ns"]
			.as_u64()
			.unwrap_or_default();
		let wall_ns = rows_micros.saturating_mul(1_000);
		bin["ledger_rows_micros"] = serde_json::json!(rows_micros);
		bin["ledger_grow_micros"] = serde_json::json!(grow_micros);
		bin["estimated_coverage_ratio"] = serde_json::json!(if wall_ns == 0 {
			0.0
		} else {
			sampled as f64 / wall_ns as f64
		});
		bin["unexplained_discrepancy_ns"] = serde_json::json!(wall_ns as i128 - sampled as i128);
		summaries.insert(set.summary.to_string(), bin);
	}
	Ok(serde_json::json!({
		"total_samples": header.samples,
		"lost_samples": header.lost_samples,
		"parsed_samples": samples.len(),
		"line_classes": ADR587_LINE_CLASSES.iter().map(|range| serde_json::json!({"class": range.class, "start": range.start, "end": range.end})).collect::<Vec<_>>(),
		"summaries": summaries,
	}))
}

fn adr587_bin_perf_data(
	perf_data: &Path,
	windows: &[Adr587WindowSet; 2],
	receipt: &serde_json::Value,
) -> Result<serde_json::Value, String> {
	let header = adr587_perf_header(perf_data)?;
	if header.lost_samples != 0 {
		return Err(format!(
			"ADR 587 profile {} lost {} samples",
			perf_data.display(),
			header.lost_samples
		));
	}
	let output = Command::new("perf")
		.args(["script", "-i"])
		.arg(perf_data)
		.args(["--ns", "-F", "pid,tid,time,period,ip,sym,srcline"])
		.output()
		.map_err(|error| format!("ADR 587 perf script failed to start: {error}"))?;
	if !output.status.success() {
		return Err(format!(
			"ADR 587 perf script failed: {}",
			String::from_utf8_lossy(&output.stderr)
		));
	}
	let samples = adr587_parse_perf_script(&String::from_utf8_lossy(&output.stdout))?;
	adr587_bin_samples(&samples, windows, receipt, &header)
}

/// One staged executable the ADR 588 driver launches: its manifest path, the manifest, and
/// the executable's staged path, verified against the manifest before any child runs.
struct Adr588Arm {
	name: &'static str,
	manifest_path: PathBuf,
	manifest: BuildManifest,
	executable: PathBuf,
}

impl Adr588Arm {
	fn load(name: &'static str, manifest_path: &Path) -> Result<Self, String> {
		let manifest_path = manifest_path
			.canonicalize()
			.map_err(|error| format!("ADR 588 {name} manifest: {error}"))?;
		let manifest = BuildManifest::read(&manifest_path)?;
		let staging_root = manifest
			.toron
			.staged_path
			.parent()
			.ok_or_else(|| format!("ADR 588 {name} staged path has no parent"))?
			.to_path_buf();
		verify_staged_executable(&manifest.toron, &staging_root)
			.map_err(|error| format!("ADR 588 {name} executable: {error}"))?;
		if manifest.toron.sha256 != manifest.harness.sha256 {
			return Err(format!(
				"ADR 588 {name} manifest stages a harness different from its toron executable"
			));
		}
		Ok(Self {
			name,
			manifest_path,
			executable: manifest.toron.staged_path.clone(),
			manifest,
		})
	}

	fn envs(&self) -> Vec<(String, String)> {
		vec![
			(
				BUILD_MANIFEST_ENV.to_string(),
				self.manifest_path.display().to_string(),
			),
			(
				"TORON_TPCH_TORON_BIN".to_string(),
				self.executable.display().to_string(),
			),
		]
	}
}

/// The cross-executable identity gate on the two manifests: everything but the source tree
/// and the executable must agree, and those must differ.
fn adr588_manifest_gate(control: &BuildManifest, treatment: &BuildManifest) -> Result<(), String> {
	if control.recipe_digest != treatment.recipe_digest {
		return Err(format!(
			"ADR 588 recipes differ: control {} treatment {} (profile {}/{}, rustflags {}/{}, lock {}/{})",
			control.recipe_digest,
			treatment.recipe_digest,
			control.recipe.profile,
			treatment.recipe.profile,
			control.recipe.rustflags_sha256,
			treatment.recipe.rustflags_sha256,
			control.recipe.cargo_lock_sha256,
			treatment.recipe.cargo_lock_sha256
		));
	}
	if control.recipe.profile != "release" {
		return Err(format!(
			"ADR 588 requires the release recipe, got {:?}",
			control.recipe.profile
		));
	}
	if control.source_tree_id == treatment.source_tree_id {
		return Err("ADR 588 control and treatment are the same source tree".to_string());
	}
	if control.toron.sha256 == treatment.toron.sha256 {
		return Err("ADR 588 control and treatment are the same executable".to_string());
	}
	if !control.untracked_build_inputs.is_empty() || !treatment.untracked_build_inputs.is_empty() {
		return Err("ADR 588 refuses untracked build inputs on either arm".to_string());
	}
	Ok(())
}

fn run_adr588_q21_second_walk(args: &[String]) -> Result<serde_json::Value, String> {
	run_paired_executables(args, "adr588_q21_second_walk", "ADR 588")
}

/// The paired-executable measurement ADR 588 defined: control and treatment on one recipe,
/// phase 0 on the control, warmups, five alternating unprofiled pairs, one validator on every
/// child, and the predeclared retention rule. `kind` names the record in the summary; the
/// refusals keep ADR 588's wording, since the contract is ADR 588's.
fn run_paired_executables(
	args: &[String],
	kind: &str,
	label: &str,
) -> Result<serde_json::Value, String> {
	let mode = flag_value(args, "--mode").unwrap_or("strict");
	let sf = resolved_sf(args)?;
	let data_source = flag_value(args, "--data-source").unwrap_or("parquet");
	let data_dir = data_dir_from_args(args);
	let output_dir =
		PathBuf::from(flag_value(args, "--output-dir").unwrap_or("target/tpch/paired-executables"));
	if mode != "strict" || sf != 8.0 || !matches!(data_source, "parquet" | "persisted") {
		return Err(format!(
			"ADR 588 requires --mode strict --sf 8 --data-source parquet; got mode={mode:?} sf={sf} data_source={data_source:?}"
		));
	}
	if output_dir.exists() {
		return Err(format!(
			"ADR 588 requires a fresh output directory; {} already exists",
			output_dir.display()
		));
	}
	for name in [
		"TORON_NODE_ATTRIBUTION_PROFILE_PATH",
		"TORON_CHAIN_STAGE_PROFILE_PATH",
		"TORON_EXISTENCE_SUMMARY_PROFILE_PATH",
	] {
		if std::env::var_os(name).is_some() {
			return Err(format!("ADR 588 parent environment must not set {name}"));
		}
	}
	let reference_off_median_ms = flag_value(args, "--reference-off-median-ms")
		.ok_or_else(|| {
			"ADR 588 requires --reference-off-median-ms, the ADR 586 unprofiled off median"
				.to_string()
		})?
		.parse::<f64>()
		.ok()
		.filter(|value| value.is_finite() && *value > 0.0)
		.ok_or_else(|| "ADR 588 reference off median must be a positive number".to_string())?;
	let control_manifest = flag_value(args, "--control-manifest")
		.ok_or_else(|| "ADR 588 requires --control-manifest".to_string())?;
	let budget = GaugeBudget::from_environment()?;
	if budget.memory_per_task != 2_147_483_648
		|| budget.memory_per_task_provenance != "explicit"
		|| budget.spill_bytes_reserved != 2_147_483_648
		|| budget.spill_bytes_reserved_provenance != "explicit"
	{
		return Err("ADR 588 requires explicit 2 GiB task memory and spill reserve".to_string());
	}
	let concurrency = gauge_partition_work_concurrency_requested()?;
	if concurrency != (1, "explicit") {
		return Err(format!(
			"ADR 588 requires explicit partition parallelism 1, got {concurrency:?}"
		));
	}
	let identity = runtime_identity()?.ok_or_else(|| {
		"ADR 588 must run through toron-bench run with the treatment's staged executable"
			.to_string()
	})?;
	let treatment_manifest_path = std::env::var_os(BUILD_MANIFEST_ENV)
		.map(PathBuf::from)
		.ok_or_else(|| "ADR 588 lacks the treatment manifest path".to_string())?;
	let treatment = Adr588Arm::load("treatment", &treatment_manifest_path)?;
	let control = Adr588Arm::load("control", Path::new(control_manifest))?;
	if treatment.manifest.toron.sha256 != identity.executable.sha256 {
		return Err("ADR 588 must be launched from the treatment executable".to_string());
	}
	adr588_manifest_gate(&control.manifest, &treatment.manifest)?;
	let host_start = observe_host_quiescence();
	if !host_start.quiescent() {
		return Err("ADR 588 requires the local deployment and SQL port quiescent".to_string());
	}
	let scratch = std::env::var_os("TORON_JOIN_SPILL_DIR")
		.filter(|value| !value.is_empty())
		.map(PathBuf::from)
		.unwrap_or_else(|| std::env::temp_dir().join("toron_join_spill"));
	if !directory_is_empty(&scratch)? {
		return Err(format!(
			"ADR 588 spill scratch {} is not empty before the run",
			scratch.display()
		));
	}
	let dataset = load_persisted_tpch_dataset(mode, sf, &data_dir)?;
	if dataset.verification.as_str() != "full_hash" {
		return Err(format!(
			"ADR 588 requires full-hash dataset verification, got {}",
			dataset.verification.as_str()
		));
	}
	let journal = prepare_perf_sample_journal(&output_dir)?;
	let expected = Adr588Expected {
		control: Adr588ArmExpectation {
			name: "control",
			manifest: control.manifest.clone(),
			manifest_path: control.manifest_path.display().to_string(),
		},
		treatment: Adr588ArmExpectation {
			name: "treatment",
			manifest: treatment.manifest.clone(),
			manifest_path: treatment.manifest_path.display().to_string(),
		},
	};
	let expected_dataset = Adr588Dataset {
		manifest_fingerprint: dataset.manifest_fingerprint.clone(),
		data_source: data_source.to_string(),
	};

	// Every child, phase 0 and warmups included, is validated the moment it returns: a row that
	// fails parity or identity stops the run there, as the record promises, and is journaled.
	let mut run_index = 0u32;
	let mut child = |arm: &Adr588Arm, warmup: bool| -> Result<serde_json::Value, String> {
		let index = run_index;
		run_index += 1;
		let mut row = run_adr588_child(arm, index, warmup, mode, sf, data_source, &data_dir)?;
		row["adr588_arm"] = serde_json::json!(arm.name);
		row["adr588_launched_manifest"] =
			serde_json::json!(arm.manifest_path.display().to_string());
		row["adr588_launched_executable_sha256"] = serde_json::json!(arm.manifest.toron.sha256);
		let expectation = if arm.name == "control" {
			&expected.control
		} else {
			&expected.treatment
		};
		if let Err(error) = adr588_validate_row(&row, expectation, &expected_dataset) {
			journal.append(&row)?;
			return Err(format!("{error}; run stopped, row retained"));
		}
		Ok(row)
	};

	// Phase 0 on the control: one unprofiled warmup, one sample, the tripwire.
	let mut phase0_warmup = child(&control, true)?;
	phase0_warmup["adr588_phase"] = serde_json::json!("host_check_warmup");
	journal.append(&phase0_warmup)?;
	let mut phase0_sample = child(&control, false)?;
	let phase0_execute_ms = required_f64(&phase0_sample, "execute_ms")?;
	let tripwire = adr586_phase0_tripwire(phase0_execute_ms, reference_off_median_ms);
	phase0_sample["adr588_phase"] = serde_json::json!("host_check");
	phase0_sample["adr588_phase0_tripwire"] = tripwire.clone();
	journal.append(&phase0_sample)?;
	let phase0 = serde_json::json!({
		"warmup_execute_ms": phase0_warmup.get("execute_ms").cloned().unwrap_or_default(),
		"sample_execute_ms": phase0_execute_ms,
		"reference_off_median_ms": reference_off_median_ms,
		"tripwire": tripwire,
	});
	let identities = serde_json::json!({
		"control": control.manifest.json_value_for_executable(&control.manifest.toron),
		"treatment": treatment.manifest.json_value_for_executable(&treatment.manifest.toron),
		"control_manifest": control.manifest_path.display().to_string(),
		"treatment_manifest": treatment.manifest_path.display().to_string(),
	});
	if phase0["tripwire"]["tripped"] == true {
		let summary = serde_json::json!({
			"kind": kind,
			"outcome": "phase0_tripped",
			"sealed": false,
			"candidate_authority": false,
			"phase0": phase0,
			"identities": identities,
			"dataset_manifest_fingerprint": dataset.manifest_fingerprint,
		});
		write_json_atomically(&output_dir.join("summary.json"), &summary)?;
		return Err(format!(
			"ADR 588 phase 0 tripped: sample {phase0_execute_ms:.1} ms is {:+.2}% from the reference {reference_off_median_ms:.1} ms (threshold {:.0}%); pairs not run, finding retained at {}",
			phase0["tripwire"]["movement"].as_f64().unwrap_or(f64::NAN) * 100.0,
			ADR586_PHASE0_THRESHOLD * 100.0,
			output_dir.display()
		));
	}

	// One unprofiled warmup on each executable, control first.
	let mut warmups = Vec::new();
	for arm in [&control, &treatment] {
		let mut warmup = child(arm, true)?;
		warmup["adr588_phase"] = serde_json::json!("pairs_warmup");
		journal.append(&warmup)?;
		warmups.push(warmup);
	}

	let mut rows = Vec::with_capacity(ADR585_PAIRS * 2);
	for pair in 0..ADR585_PAIRS {
		// Alternating: control-treatment, treatment-control, ...
		let order: [&Adr588Arm; 2] = if pair % 2 == 0 {
			[&control, &treatment]
		} else {
			[&treatment, &control]
		};
		for (position, arm) in order.into_iter().enumerate() {
			let mut row = child(arm, false)?;
			row["adr588_phase"] = serde_json::json!("pairs");
			row["adr588_pair"] = serde_json::json!(pair);
			row["adr588_pair_position"] = serde_json::json!(position);
			journal.append(&row)?;
			rows.push(row);
		}
	}
	let host_end = observe_host_quiescence();
	if !host_end.quiescent() || !directory_is_empty(&scratch)? {
		return Err(
			"ADR 588 lost host quiescence or left spill scratch after measurement".to_string(),
		);
	}
	let mut summary = evaluate_adr588_pairs(&rows, &expected, &expected_dataset)?;
	summary["kind"] = serde_json::json!(kind);
	summary["label"] = serde_json::json!(label);
	summary["outcome"] = serde_json::json!("pairs_measured");
	summary["sealed"] = serde_json::json!(false);
	summary["candidate_authority"] = serde_json::json!(false);
	summary["phase0"] = phase0;
	summary["identities"] = identities;
	summary["sealed_baseline_executable_sha256"] =
		serde_json::json!(ADR585_SEALED_BASELINE_EXECUTABLE);
	summary["adr586_instrumented_executable_sha256"] = serde_json::json!(ADR587_ADR586_EXECUTABLE);
	summary["adr587_instrumented_executable_sha256"] = serde_json::json!(ADR588_ADR587_EXECUTABLE);
	summary["dataset_manifest_fingerprint"] = serde_json::json!(dataset.manifest_fingerprint);
	summary["memory_per_task"] = serde_json::json!(budget.memory_per_task);
	summary["spill_bytes_reserved"] = serde_json::json!(budget.spill_bytes_reserved);
	summary["partition_work_concurrency_requested"] = serde_json::json!(concurrency.0);
	summary["warmup_execute_ms"] = serde_json::json!(
		warmups
			.iter()
			.map(|row| row.get("execute_ms").cloned().unwrap_or_default())
			.collect::<Vec<_>>()
	);
	write_json_atomically(&output_dir.join("summary.json"), &summary)?;
	Ok(summary)
}

/// What every row of one arm must report: the arm's manifest, the path it was launched with,
/// and the executable it ran.
struct Adr588ArmExpectation {
	name: &'static str,
	manifest: BuildManifest,
	manifest_path: String,
}

struct Adr588Expected {
	control: Adr588ArmExpectation,
	treatment: Adr588ArmExpectation,
}

/// The dataset the driver verified before any child ran; every row must name the same one.
struct Adr588Dataset {
	manifest_fingerprint: String,
	data_source: String,
}

/// The one validator every ADR 588 row passes through, phase 0 and warmups included, at the
/// moment the child returns and again in the evaluator: a passing q21 Toron row at the sealed
/// coordinate, unprofiled, carrying the pinned digest byte for byte, reporting the identity of
/// its arm's manifest, launched with that manifest and executable, over the verified dataset.
// ---------------------------------------------------------------------------------------------
// ADR 592: the sealed baseline bound by its contents, and one child validator for the
// attribution drivers.
// ---------------------------------------------------------------------------------------------

/// What a driver binds itself to when `--sealed-manifest` names a sealed board: not the
/// manifest's labels but the artifact they name, re-hashed, and the values read out of it.
#[derive(Debug, Clone, PartialEq)]
struct Adr592SealedBaseline {
	manifest_path: String,
	source_tree_id: String,
	executable_sha256: String,
	duckdb_sha256: Option<String>,
	artifact_path: String,
	artifact_sha256: String,
	dataset_manifest_fingerprint: String,
	cpu_affinity_requested: String,
	/// ADR 606: the query whose sealed cell this is. The two fields below keep their q21 names
	/// from ADR 592 (retained receipts carry them); for another query they hold that query's cell.
	query: String,
	q21_toron_execute_ms_median: f64,
	q21_toron_digest: String,
}

impl Adr592SealedBaseline {
	fn json_value(&self) -> serde_json::Value {
		serde_json::json!({
			"manifest_path": self.manifest_path,
			"source_tree_id": self.source_tree_id,
			"executable_sha256": self.executable_sha256,
			"duckdb_sha256": self.duckdb_sha256,
			"artifact_path": self.artifact_path,
			"artifact_sha256": self.artifact_sha256,
			"dataset_manifest_fingerprint": self.dataset_manifest_fingerprint,
			"cpu_affinity_requested": self.cpu_affinity_requested,
			"query": self.query,
			"q21_toron_execute_ms_median": self.q21_toron_execute_ms_median,
			"q21_toron_digest": self.q21_toron_digest,
		})
	}
}

/// Read and verify the sealed manifest at `path`: sealed with final authority, its artifact
/// present and hashing to what the manifest claims, and carrying a passing q21 Toron cell.
fn adr592_load_sealed_baseline(path: &Path, query: &str) -> Result<Adr592SealedBaseline, String> {
	let manifest = toron_tpch::evidence::RunEvidenceManifest::read(path)
		.map_err(|error| format!("ADR 592 sealed manifest {}: {error}", path.display()))?;
	let artifact_sha256 = toron_tpch::evidence::sha256_file(&manifest.artifact_path)
		.map_err(|error| format!("ADR 592 sealed artifact: {error}"))?;
	let (header, _samples, summaries) = read_perf_board_artifact(&manifest.artifact_path)
		.map_err(|error| format!("ADR 592 sealed artifact: {error}"))?;
	adr592_bind_sealed_baseline(
		&path.display().to_string(),
		&manifest,
		&artifact_sha256,
		&header,
		&summaries,
		query,
	)
}

fn adr592_bind_sealed_baseline(
	manifest_path: &str,
	manifest: &toron_tpch::evidence::RunEvidenceManifest,
	artifact_sha256_observed: &str,
	header: &serde_json::Value,
	summaries: &[serde_json::Value],
	query: &str,
) -> Result<Adr592SealedBaseline, String> {
	if manifest.seal_state != toron_tpch::evidence::SealState::Sealed || !manifest.final_authority {
		return Err(format!(
			"ADR 592 baseline {manifest_path} is not a sealed manifest with final authority (seal_state={:?}, final_authority={})",
			manifest.seal_state, manifest.final_authority
		));
	}
	if artifact_sha256_observed != manifest.artifact_sha256 {
		return Err(format!(
			"ADR 592 baseline artifact {} hashes to {artifact_sha256_observed}, manifest claims {}",
			manifest.artifact_path.display(),
			manifest.artifact_sha256
		));
	}
	let header_text = |field: &str| {
		header
			.get(field)
			.and_then(serde_json::Value::as_str)
			.filter(|value| !value.is_empty())
			.map(str::to_string)
			.ok_or_else(|| format!("ADR 592 baseline artifact header lacks {field}"))
	};
	if header.get("candidate_authority") != Some(&serde_json::Value::Bool(true)) {
		return Err(
			"ADR 592 baseline artifact header does not claim candidate authority".to_string(),
		);
	}
	// The artifact must be this record's coordinate, not merely a sealed board: a valid board
	// under other budgets or another scale would otherwise become the reference.
	for (field, expected) in [
		("mode", serde_json::json!("strict")),
		("data_source", serde_json::json!("parquet")),
		("scale_factor", serde_json::json!(8.0)),
		("memory_per_task", serde_json::json!(2_147_483_648u64)),
		("memory_per_task_provenance", serde_json::json!("explicit")),
		("spill_bytes_reserved", serde_json::json!(2_147_483_648u64)),
		(
			"spill_bytes_reserved_provenance",
			serde_json::json!("explicit"),
		),
		("partition_work_concurrency_requested", serde_json::json!(1)),
		(
			"partition_work_concurrency_requested_provenance",
			serde_json::json!("explicit"),
		),
	] {
		if header.get(field) != Some(&expected) {
			return Err(format!(
				"ADR 592 baseline artifact {field} is {:?}, this record's coordinate is {expected}",
				header.get(field).cloned().unwrap_or_default()
			));
		}
	}
	// The artifact's own identity must be the manifest's, since the manifest's labels are what
	// the summary reports.
	let header_identity = |field: &str| {
		header
			.pointer(&format!("/evidence_identity/{field}"))
			.and_then(serde_json::Value::as_str)
	};
	if header_identity("executable_sha256") != Some(manifest.build.toron.sha256.as_str())
		|| header_identity("source_tree_id") != Some(manifest.build.source_tree_id.as_str())
	{
		return Err(format!(
			"ADR 592 baseline artifact reports identity {} but the manifest names executable {} on tree {}",
			header.get("evidence_identity").cloned().unwrap_or_default(),
			manifest.build.toron.sha256,
			manifest.build.source_tree_id
		));
	}
	let dataset_manifest_fingerprint = header_text("dataset_manifest_fingerprint")?;
	let cpu_affinity_requested = header_text("cpu_affinity_requested")?;
	if header_text("cpu_affinity_requested_provenance")? != "explicit" {
		return Err("ADR 592 baseline artifact affinity provenance is not explicit".to_string());
	}
	let cell = summaries
		.iter()
		.find(|row| {
			row.get("engine").and_then(serde_json::Value::as_str) == Some("toron")
				&& row.get("query").and_then(serde_json::Value::as_str) == Some(query)
		})
		.ok_or_else(|| format!("ADR 592 baseline artifact has no {query} Toron cell"))?;
	if cell.get("status").and_then(serde_json::Value::as_str) != Some("pass") {
		return Err(format!("ADR 592 baseline {query} Toron cell did not pass"));
	}
	let q21_toron_execute_ms_median = cell
		.get("execute_ms_median")
		.and_then(serde_json::Value::as_f64)
		.filter(|value| value.is_finite() && *value > 0.0)
		.ok_or_else(|| format!("ADR 592 baseline {query} Toron cell lacks a finite median"))?;
	let q21_toron_digest = cell
		.get("digest")
		.and_then(serde_json::Value::as_str)
		.filter(|value| !value.is_empty())
		.map(str::to_string)
		.ok_or_else(|| format!("ADR 592 baseline {query} Toron cell lacks a digest"))?;
	Ok(Adr592SealedBaseline {
		query: query.to_string(),
		manifest_path: manifest_path.to_string(),
		source_tree_id: manifest.build.source_tree_id.clone(),
		executable_sha256: manifest.build.toron.sha256.clone(),
		duckdb_sha256: manifest
			.build
			.duckdb
			.as_ref()
			.map(|duckdb| duckdb.sha256.clone()),
		artifact_path: manifest.artifact_path.display().to_string(),
		artifact_sha256: manifest.artifact_sha256.clone(),
		dataset_manifest_fingerprint,
		cpu_affinity_requested,
		q21_toron_execute_ms_median,
		q21_toron_digest,
	})
}

/// The coordinate a driver's children must reproduce, fixed before the first child runs.
#[derive(Debug, Clone, PartialEq)]
struct Adr592ChildExpectation {
	label: &'static str,
	/// ADR 606: the query every child row must be; q21 unless the driver was given `--query`.
	query: String,
	/// The current instrumented executable's manifest: every child must have run from it.
	instrumented: BuildManifest,
	instrumented_manifest_path: String,
	dataset_manifest_fingerprint: String,
	data_source: String,
	digest: String,
	cpu_affinity_requested: String,
}

/// Bind the driver's own coordinate to the verified baseline: the process affinity must be
/// explicit and the baseline's, and the loaded dataset must be the baseline's.
fn adr592_bind_driver_coordinate(
	label: &str,
	baseline: &Adr592SealedBaseline,
	affinity: &toron_tpch::affinity::AffinityCoordinate,
	dataset_manifest_fingerprint: &str,
) -> Result<(), String> {
	match (affinity.provenance, affinity.requested.as_deref()) {
		("explicit", Some(requested)) if requested == baseline.cpu_affinity_requested => {}
		("explicit", Some(requested)) => {
			return Err(format!(
				"{label} runs under affinity {requested}, the sealed baseline was measured under {}",
				baseline.cpu_affinity_requested
			));
		}
		_ => {
			return Err(format!(
				"{label} requires an explicit CPU affinity coordinate ({}); found provenance {:?}",
				toron_tpch::affinity::CPU_AFFINITY_ENV,
				affinity.provenance
			));
		}
	}
	if dataset_manifest_fingerprint != baseline.dataset_manifest_fingerprint {
		return Err(format!(
			"{label} loaded dataset {dataset_manifest_fingerprint}, the sealed baseline ran over {}",
			baseline.dataset_manifest_fingerprint
		));
	}
	Ok(())
}

/// One validator for every child of the ADR 584 and ADR 585 drivers, warmup included, applied
/// the moment the child returns. `diagnostics_disabled` is what the arm expects: the off arm
/// runs with every diagnostic disabled, the on arm with them enabled.
fn adr592_validate_child_row(
	row: &serde_json::Value,
	expected: &Adr592ChildExpectation,
	diagnostics_disabled: bool,
) -> Result<(), String> {
	let label = expected.label;
	let text = |field: &str| row.get(field).and_then(serde_json::Value::as_str);
	if text("status") != Some("pass") {
		return Err(format!("{label} child row did not pass: {row}"));
	}
	if text("query") != Some(expected.query.as_str()) || text("engine") != Some("toron") {
		return Err(format!(
			"{label} child row is not a {} Toron row",
			expected.query
		));
	}
	if text("mode") != Some("strict")
		|| row.get("scale_factor").and_then(serde_json::Value::as_f64) != Some(8.0)
		|| row
			.get("memory_per_task")
			.and_then(serde_json::Value::as_u64)
			!= Some(2_147_483_648)
		|| row
			.get("spill_bytes_reserved")
			.and_then(serde_json::Value::as_u64)
			!= Some(2_147_483_648)
		|| row
			.get("partition_work_concurrency_requested")
			.and_then(serde_json::Value::as_u64)
			!= Some(1)
	{
		return Err(format!(
			"{label} child row is not at the sealed coordinate (mode/sf/memory/spill/parallelism)"
		));
	}
	if row.get("diagnostics_disabled") != Some(&serde_json::Value::Bool(diagnostics_disabled)) {
		return Err(format!(
			"{label} child row reports diagnostics_disabled {:?}, the arm expects {diagnostics_disabled}",
			row.get("diagnostics_disabled")
		));
	}
	for (field, value) in [
		(
			"cpu_affinity_requested",
			expected.cpu_affinity_requested.as_str(),
		),
		("cpu_affinity_requested_provenance", "explicit"),
		(
			"cpu_affinity_observed",
			expected.cpu_affinity_requested.as_str(),
		),
	] {
		if text(field) != Some(value) {
			return Err(format!(
				"{label} child row {field} is {:?}, expected {value:?}",
				text(field)
			));
		}
	}
	match text("digest") {
		Some(digest) if digest == expected.digest => {}
		Some(digest) => {
			return Err(format!(
				"{label} digest parity failed on a child row: {digest}"
			));
		}
		None => return Err(format!("{label} child row lacks a digest")),
	}
	if text("dataset_verification") != Some("full_hash") {
		return Err(format!(
			"{label} child row was not run over a fully verified dataset"
		));
	}
	if text("dataset_manifest_fingerprint") != Some(expected.dataset_manifest_fingerprint.as_str())
	{
		return Err(format!(
			"{label} child row names dataset {:?}, expected {}",
			text("dataset_manifest_fingerprint"),
			expected.dataset_manifest_fingerprint
		));
	}
	if text("data_source") != Some(expected.data_source.as_str()) {
		return Err(format!(
			"{label} child row names data source {:?}, expected {}",
			text("data_source"),
			expected.data_source
		));
	}
	let identity = |field: &str| {
		row.pointer(&format!("/evidence_identity/{field}"))
			.and_then(serde_json::Value::as_str)
	};
	if identity("executable_sha256") != Some(expected.instrumented.toron.sha256.as_str())
		|| identity("source_tree_id") != Some(expected.instrumented.source_tree_id.as_str())
		|| identity("recipe_sha256") != Some(expected.instrumented.recipe_digest.as_str())
	{
		return Err(format!(
			"{label} child row reports an identity that is not the instrumented manifest's: {}",
			row.get("evidence_identity").cloned().unwrap_or_default()
		));
	}
	if text("adr592_launched_manifest") != Some(expected.instrumented_manifest_path.as_str()) {
		return Err(format!(
			"{label} child row was launched with manifest {:?}, expected {}",
			text("adr592_launched_manifest"),
			expected.instrumented_manifest_path
		));
	}
	if text("adr592_launched_executable_sha256")
		!= Some(expected.instrumented.toron.sha256.as_str())
	{
		return Err(format!(
			"{label} child row was launched from an executable other than the instrumented one"
		));
	}
	Ok(())
}

/// The coordinate both drivers fix before their first child: the sealed baseline when
/// `--sealed-manifest` is given (required by ADR 592), else ADR 584's slim constant and ADR
/// 588's digest, the pre-592 default. Either way the affinity must be explicit.
struct Adr592DriverCoordinate {
	baseline: Option<Adr592SealedBaseline>,
	expectation: Adr592ChildExpectation,
	launched_manifest: String,
}

fn adr592_driver_coordinate(
	label: &'static str,
	args: &[String],
	identity: &toron_tpch::evidence::RuntimeIdentity,
	dataset_manifest_fingerprint: &str,
	data_source: &str,
) -> Result<Adr592DriverCoordinate, String> {
	let affinity = toron_tpch::affinity::affinity_coordinate();
	let launched_manifest = std::env::var(toron_tpch::evidence::BUILD_MANIFEST_ENV)
		.map_err(|_| format!("{label} must run through toron-bench run with a build manifest"))?;
	// ADR 606: the query the pair runs, q21 unless named. Another query needs a sealed board
	// whose cell it is, since the pre-592 fallback digest is q21's.
	let query = flag_value(args, "--query").unwrap_or("q21");
	if query != "q21" && flag_value(args, "--sealed-manifest").is_none() {
		return Err(format!(
			"{label} --query {query} requires --sealed-manifest with a sealed {query} cell"
		));
	}
	let baseline = match flag_value(args, "--sealed-manifest") {
		Some(path) => {
			let baseline = adr592_load_sealed_baseline(Path::new(path), query)?;
			adr592_bind_driver_coordinate(
				label,
				&baseline,
				&affinity,
				dataset_manifest_fingerprint,
			)?;
			Some(baseline)
		}
		None => {
			if affinity.provenance != "explicit" || affinity.requested.is_none() {
				return Err(format!(
					"{label} requires an explicit CPU affinity coordinate ({}); found provenance {:?}",
					toron_tpch::affinity::CPU_AFFINITY_ENV,
					affinity.provenance
				));
			}
			None
		}
	};
	let digest = baseline
		.as_ref()
		.map(|baseline| baseline.q21_toron_digest.clone())
		.unwrap_or_else(|| ADR588_SEALED_Q21_DIGEST.to_string());
	Ok(Adr592DriverCoordinate {
		expectation: Adr592ChildExpectation {
			label,
			query: query.to_string(),
			instrumented: identity.manifest.clone(),
			instrumented_manifest_path: launched_manifest.clone(),
			dataset_manifest_fingerprint: dataset_manifest_fingerprint.to_string(),
			data_source: data_source.to_string(),
			digest,
			cpu_affinity_requested: affinity.requested.clone().unwrap_or_default(),
		},
		baseline,
		launched_manifest,
	})
}

/// Stamp the launch identity on a returned child row and validate it; a failing row is
/// journaled and the error stops the driver before another child is launched.
fn adr592_admit_child_row(
	row: &mut serde_json::Value,
	coordinate: &Adr592DriverCoordinate,
	diagnostics_disabled: bool,
	journal: &PerfSampleJournal,
) -> Result<(), String> {
	row["adr592_launched_manifest"] = serde_json::json!(coordinate.launched_manifest);
	row["adr592_launched_executable_sha256"] =
		serde_json::json!(coordinate.expectation.instrumented.toron.sha256);
	if let Err(error) =
		adr592_validate_child_row(row, &coordinate.expectation, diagnostics_disabled)
	{
		row["adr592_refusal"] = serde_json::json!(error);
		journal.append(row)?;
		return Err(format!("{error}; run stopped, row retained"));
	}
	Ok(())
}

fn adr593_unix_ms() -> f64 {
	std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.map(|d| d.as_secs_f64() * 1000.0)
		.unwrap_or(f64::NAN)
}

/// ADR 593's predeclared operational cutoff on the off arm's range, as a fraction of its median.
const ADR593_VARIABILITY_BOUND: f64 = 0.04;
/// ADR 593's start gate: package temperature at or below this, load below the other.
const ADR593_START_GATE_TEMP_MILLIC: i64 = 45_000;
const ADR593_START_GATE_LOAD: f64 = 0.3;

/// Apply the cutoff to an evaluated lifecycle summary: over the bound, the record closes on the
/// variability finding and no term is the owner; the leading term stays named as such.
fn adr593_apply_variability_bound(summary: &mut serde_json::Value) -> Result<(), String> {
	adr593_apply_variability_bound_on(summary, "owner")
}

/// The cutoff on the named outcome field (`owner` for an attribution, `verdict` for a
/// retention): over the bound the outcome becomes `closed_on_variability` with the rule's
/// outcome kept beside it.
fn adr593_apply_variability_bound_on(
	summary: &mut serde_json::Value,
	field: &str,
) -> Result<(), String> {
	let range = required_f64(summary, "off_execute_range_ms")?;
	let median = required_f64(summary, "off_execute_median_ms")?;
	let ratio = range / median;
	let exceeded = ratio > ADR593_VARIABILITY_BOUND;
	summary["variability"] = serde_json::json!({
		"range_off_ms": range,
		"median_off_ms": median,
		"bound": ADR593_VARIABILITY_BOUND,
		"ratio": ratio,
		"verdict": if exceeded { "exceeded" } else { "within_bound" },
	});
	if exceeded {
		let previous = summary[field].take();
		let leading = previous
			.get("leading_term")
			.cloned()
			.or_else(|| previous.get("term").cloned())
			.unwrap_or_default();
		let leading_ms = previous
			.get("median_scaled_ms")
			.cloned()
			.unwrap_or_default();
		let mut closed = serde_json::json!({
			"status": "closed_on_variability",
			"leading_term": leading,
			"median_scaled_ms": leading_ms,
			"reason": format!(
				"off range {range:.1} ms is {ratio:.4} of the off median, above the predeclared bound {ADR593_VARIABILITY_BOUND}"
			),
		});
		if field != "owner" {
			closed["rule_outcome_withheld"] = previous;
		}
		summary[field] = closed;
	}
	Ok(())
}

/// ADR 593's start gate, read from the host at driver start: a temperature that cannot be read
/// does not satisfy it.
/// ADR 598: the gate's outcome carries its structured reading whether it admits or refuses,
/// so a refusal keeps the evidence it was judged on.
struct Adr593GateOutcome {
	gate: serde_json::Value,
	refusal: Option<String>,
}

fn adr593_start_gate() -> Adr593GateOutcome {
	let reading = adr598_take_reading();
	let load = std::fs::read_to_string("/proc/loadavg")
		.ok()
		.and_then(|text| {
			text.split_whitespace()
				.next()
				.and_then(|v| v.parse::<f64>().ok())
		});
	adr593_judge_gate(reading, load)
}

fn adr593_judge_gate(
	reading: toron_tpch::host_readings::HostReading,
	load: Option<f64>,
) -> Adr593GateOutcome {
	let refusal = match (reading.package_temp_millic, load) {
		(None, _) => Some(format!(
			"ADR 593 start gate needs a package temperature and none could be read: {:?}",
			reading.missing
		)),
		(Some(temp), _) if temp > ADR593_START_GATE_TEMP_MILLIC => Some(format!(
			"ADR 593 start gate: package temperature {temp} millic is above {ADR593_START_GATE_TEMP_MILLIC}"
		)),
		(_, None) => {
			Some("ADR 593 start gate needs a load average and none could be read".to_string())
		}
		(_, Some(load)) if load >= ADR593_START_GATE_LOAD => Some(format!(
			"ADR 593 start gate: load {load} is not below {ADR593_START_GATE_LOAD}"
		)),
		_ => None,
	};
	let gate = serde_json::json!({
		"package_temp_millic": reading.package_temp_millic,
		"load_1m": load,
		"temp_bound_millic": ADR593_START_GATE_TEMP_MILLIC,
		"load_bound": ADR593_START_GATE_LOAD,
		"admitted": refusal.is_none(),
		"refusal": refusal,
		"reading": reading,
	});
	Adr593GateOutcome { gate, refusal }
}

/// ADR 593: re-collect a retained summary's journal window with the current collector and
/// re-classify every child's coverage, so a collection made under an earlier collector can be
/// checked without another performance run. The journal itself must still hold the window.
fn adr593_journal_recheck_command(args: &[String]) -> ExitCode {
	match adr593_journal_recheck(args) {
		Ok(report) => {
			println!("{report}");
			ExitCode::SUCCESS
		}
		Err(error) => {
			eprintln!("{error}");
			ExitCode::from(2)
		}
	}
}

fn adr593_journal_recheck(args: &[String]) -> Result<serde_json::Value, String> {
	let summary_path = flag_value(args, "--summary")
		.ok_or_else(|| "adr593-journal-recheck requires --summary PATH".to_string())?;
	let samples_path = flag_value(args, "--samples")
		.ok_or_else(|| "adr593-journal-recheck requires --samples PATH".to_string())?;
	let read =
		|path: &str| std::fs::read_to_string(path).map_err(|error| format!("read {path}: {error}"));
	let summary: serde_json::Value = serde_json::from_str(&read(summary_path)?)
		.map_err(|error| format!("parse {summary_path}: {error}"))?;
	let rows: Vec<serde_json::Value> = read(samples_path)?
		.lines()
		.filter(|line| !line.trim().is_empty())
		.map(|line| {
			serde_json::from_str(line).map_err(|error| format!("parse {samples_path}: {error}"))
		})
		.collect::<Result<_, _>>()?;
	let retained = &summary["host_journal"]["collection"];
	let (Some(since), Some(until)) = (
		retained
			.get("since_unix_ms")
			.and_then(serde_json::Value::as_f64),
		retained
			.get("until_unix_ms")
			.and_then(serde_json::Value::as_f64),
	) else {
		return Err("retained summary has no journal collection window".to_string());
	};
	let collection = toron_tpch::host_readings::collect_journal(since, until);
	let row_refs: Vec<&serde_json::Value> = rows.iter().collect();
	let joined = adr593_join_journal_and_gaps(&row_refs, &collection);
	let keys = |entries: &[serde_json::Value]| -> BTreeSet<(u64, String)> {
		entries
			.iter()
			.filter_map(|entry| {
				Some((
					entry.get("realtime_us")?.as_u64()?,
					entry.get("message")?.as_str()?.to_string(),
				))
			})
			.collect()
	};
	let retained_entries = retained
		.get("entries")
		.and_then(serde_json::Value::as_array)
		.cloned()
		.unwrap_or_default();
	let retained_keys = keys(&retained_entries);
	let current_keys: BTreeSet<(u64, String)> = collection
		.entries
		.iter()
		.map(|entry| (entry.realtime_us, entry.message.clone()))
		.collect();
	Ok(serde_json::json!({
		"summary": summary_path,
		"retained_state": retained.get("state"),
		"retained_entries": retained_entries.len(),
		"recheck_state": collection.state,
		"recheck_entries": collection.entries.len(),
		"recheck_malformed_records": collection.malformed_records,
		"recheck_bytes_read": collection.bytes_read,
		"entries_agree": retained_keys == current_keys,
		"only_in_retained": retained_keys.difference(&current_keys).count(),
		"only_in_recheck": current_keys.difference(&retained_keys).count(),
		"per_child_coverage": joined["per_child"].as_array().map(|children| {
			children
				.iter()
				.map(|child| {
					serde_json::json!({
						"run_index": child["run_index"], "warmup": child["warmup"],
						"coverage": child["journal_coverage"], "count": child["journal_count"],
					})
				})
				.collect::<Vec<_>>()
		}),
		"rechecking_evidence_identity": runtime_identity_json(),
	}))
}

/// Join the run's single journal collection to every row's window and summarize the gaps.
fn adr593_join_journal_and_gaps(
	rows: &[&serde_json::Value],
	collection: &toron_tpch::host_readings::JournalCollection,
) -> serde_json::Value {
	let mut per_child = Vec::new();
	let mut previous_return: Option<f64> = None;
	for row in rows {
		let launch = row
			.get("driver_launch_unix_ms")
			.and_then(serde_json::Value::as_f64);
		let returned = row
			.get("driver_return_unix_ms")
			.and_then(serde_json::Value::as_f64);
		// "Inside the requested bounds" and "completely covered" are different claims: a
		// truncated collection covers a window only partially and an unavailable or degraded one
		// not at all, whatever the bounds say. Counts follow the coverage: exact, partial (a lower
		// bound), or unknown.
		let (joined, inside_bounds) = match (launch, returned) {
			(Some(launch), Some(returned)) => {
				let since = launch - 1000.0;
				let until = returned + 1000.0;
				let inside = since >= collection.since_unix_ms && until <= collection.until_unix_ms;
				(
					toron_tpch::host_readings::join_window(collection, since, until),
					inside,
				)
			}
			_ => (Vec::new(), false),
		};
		use toron_tpch::host_readings::JournalState;
		let coverage = match &collection.state {
			_ if !inside_bounds => "outside_collection_bounds",
			JournalState::Collected => "complete",
			JournalState::Truncated(_) => "partial",
			JournalState::Degraded(_) | JournalState::Unavailable(_) => "unknown",
		};
		let journal_count = match coverage {
			"complete" => serde_json::json!(joined.len()),
			"partial" => serde_json::json!({"at_least": joined.len()}),
			_ => serde_json::Value::Null,
		};
		per_child.push(serde_json::json!({
			"run_index": row.get("run_index"),
			"warmup": row.get("warmup"),
			"launch_unix_ms": launch,
			"return_unix_ms": returned,
			"gap_since_previous_return_ms": match (previous_return, launch) {
				(Some(prev), Some(launch)) => serde_json::json!(launch - prev),
				_ => serde_json::Value::Null,
			},
			"journal_window_inside_collection_bounds": inside_bounds,
			"journal_coverage": coverage,
			"journal_count": journal_count,
			"journal_entries": joined,
		}));
		previous_return = returned;
	}
	serde_json::json!({
		"collection": collection,
		"per_child": per_child,
	})
}

/// The paired-children loop both drivers share: one unprofiled warmup, then `pairs` adjacent
/// pairs alternating off-on and on-off, every child (the warmup included) admitted by the
/// validator the moment it returns and journaled, a refused child stopping the loop before
/// another is launched. `run_child(run_index, warmup, on)` is the launcher, injectable so the
/// stop-on-refusal contract is testable without a child process.
fn adr592_run_paired_children(
	tags: (&str, &str, &str),
	pairs: usize,
	coordinate: &Adr592DriverCoordinate,
	journal: &PerfSampleJournal,
	mut run_child: impl FnMut(u32, bool, bool) -> Result<serde_json::Value, String>,
) -> Result<(serde_json::Value, Vec<serde_json::Value>), String> {
	let (arm_tag, pair_tag, position_tag) = tags;
	// ADR 593: launch and return stamps on every row, so inter-child gaps are derivable and the
	// journal, collected once afterwards, can be joined to each child's window.
	let launched = adr593_unix_ms();
	let mut warmup = run_child(0, true, false)?;
	warmup["driver_launch_unix_ms"] = serde_json::json!(launched);
	warmup["driver_return_unix_ms"] = serde_json::json!(adr593_unix_ms());
	warmup[arm_tag] = serde_json::json!("off");
	warmup[pair_tag] = serde_json::Value::Null;
	adr592_admit_child_row(&mut warmup, coordinate, true, journal)?;
	journal.append(&warmup)?;
	let mut rows = Vec::with_capacity(pairs * 2);
	for pair in 0..pairs {
		let order = if pair % 2 == 0 {
			[false, true]
		} else {
			[true, false]
		};
		for (position, on) in order.into_iter().enumerate() {
			let launched = adr593_unix_ms();
			let mut row = run_child(pair as u32, false, on)?;
			row["driver_launch_unix_ms"] = serde_json::json!(launched);
			row["driver_return_unix_ms"] = serde_json::json!(adr593_unix_ms());
			row[arm_tag] = serde_json::json!(if on { "on" } else { "off" });
			row[pair_tag] = serde_json::json!(pair);
			row[position_tag] = serde_json::json!(position);
			adr592_admit_child_row(&mut row, coordinate, !on, journal)?;
			journal.append(&row)?;
			rows.push(row);
		}
	}
	Ok((warmup, rows))
}

/// ADR 592: re-run the child validator over a retained driver journal under the gate this
/// executable carries, bound to a sealed baseline and to the instrumented manifest the journal
/// was launched from. A tightened validator can thereby re-examine retained rows without
/// another performance run; every row's verdict is printed, none is hidden.
fn adr592_revalidate_command(args: &[String]) -> ExitCode {
	match adr592_revalidate(args) {
		Ok((report, all_admitted)) => {
			println!("{report}");
			if all_admitted {
				ExitCode::SUCCESS
			} else {
				ExitCode::from(1)
			}
		}
		Err(error) => {
			eprintln!("{error}");
			ExitCode::from(2)
		}
	}
}

fn adr592_revalidate(args: &[String]) -> Result<(serde_json::Value, bool), String> {
	let samples = flag_value(args, "--samples")
		.ok_or_else(|| "adr592-revalidate requires --samples PATH".to_string())?;
	let sealed = flag_value(args, "--sealed-manifest")
		.ok_or_else(|| "adr592-revalidate requires --sealed-manifest PATH".to_string())?;
	let instrumented_path = flag_value(args, "--instrumented-manifest")
		.ok_or_else(|| "adr592-revalidate requires --instrumented-manifest PATH".to_string())?;
	let arm_tag = flag_value(args, "--arm-tag")
		.ok_or_else(|| "adr592-revalidate requires --arm-tag adr584_arm|adr585_arm".to_string())?;
	let launched_manifest = flag_value(args, "--launched-manifest").unwrap_or(instrumented_path);
	let baseline = adr592_load_sealed_baseline(Path::new(sealed), "q21")?;
	let instrumented = BuildManifest::read(Path::new(instrumented_path))?;
	let text =
		std::fs::read_to_string(samples).map_err(|error| format!("read {samples}: {error}"))?;
	let rows = text
		.lines()
		.filter(|line| !line.trim().is_empty())
		.map(|line| {
			serde_json::from_str::<serde_json::Value>(line)
				.map_err(|error| format!("parse {samples}: {error}"))
		})
		.collect::<Result<Vec<_>, _>>()?;
	let first_affinity = rows
		.first()
		.and_then(|row| row.get("cpu_affinity_requested"))
		.and_then(serde_json::Value::as_str)
		.unwrap_or_default()
		.to_string();
	let affinity = toron_tpch::affinity::AffinityCoordinate {
		requested: Some(first_affinity),
		provenance: "explicit",
	};
	adr592_bind_driver_coordinate(
		"adr592-revalidate",
		&baseline,
		&affinity,
		rows.first()
			.and_then(|row| row.get("dataset_manifest_fingerprint"))
			.and_then(serde_json::Value::as_str)
			.unwrap_or_default(),
	)?;
	let expected = Adr592ChildExpectation {
		label: "adr592-revalidate",
		query: baseline.query.clone(),
		instrumented: instrumented.clone(),
		instrumented_manifest_path: launched_manifest.to_string(),
		dataset_manifest_fingerprint: baseline.dataset_manifest_fingerprint.clone(),
		data_source: "parquet".to_string(),
		digest: baseline.q21_toron_digest.clone(),
		cpu_affinity_requested: baseline.cpu_affinity_requested.clone(),
	};
	let mut verdicts = Vec::new();
	let mut all_admitted = true;
	for (index, row) in rows.iter().enumerate() {
		let arm = row.get(arm_tag).and_then(serde_json::Value::as_str);
		let diagnostics_disabled = match arm {
			Some("off") => true,
			Some("on") => false,
			other => {
				all_admitted = false;
				verdicts.push(serde_json::json!({
					"row": index, "arm": other, "admitted": false,
					"refusal": format!("row lacks a recognised {arm_tag}"),
				}));
				continue;
			}
		};
		let mut row = row.clone();
		// Rows journaled by a driver already carry the launch stamps; a row from before the
		// stamps existed is refused on them, which is the point.
		if row.get("adr592_launched_manifest").is_none() {
			row["adr592_launched_manifest"] = serde_json::Value::Null;
		}
		match adr592_validate_child_row(&row, &expected, diagnostics_disabled) {
			Ok(()) => {
				verdicts.push(serde_json::json!({"row": index, "arm": arm, "admitted": true}))
			}
			Err(refusal) => {
				all_admitted = false;
				verdicts.push(serde_json::json!({
					"row": index, "arm": arm, "admitted": false, "refusal": refusal,
				}));
			}
		}
	}
	Ok((
		serde_json::json!({
			"samples": samples,
			"rows": rows.len(),
			"all_admitted": all_admitted,
			"sealed_baseline": baseline.json_value(),
			"instrumented_executable_sha256": instrumented.toron.sha256,
			"instrumented_source_tree_id": instrumented.source_tree_id,
			"verdicts": verdicts,
			"revalidating_evidence_identity": runtime_identity_json(),
		}),
		all_admitted,
	))
}

/// The baseline fields every driver summary carries under ADR 592.
fn adr592_stamp_summary(
	summary: &mut serde_json::Value,
	coordinate: &Adr592DriverCoordinate,
	off_execute_ms_median: Option<f64>,
) {
	summary["cpu_affinity_requested"] =
		serde_json::json!(coordinate.expectation.cpu_affinity_requested);
	summary["adr592_launched_manifest"] = serde_json::json!(coordinate.launched_manifest);
	match &coordinate.baseline {
		Some(baseline) => {
			summary["sealed_baseline"] = baseline.json_value();
			summary["sealed_baseline_executable_sha256"] =
				serde_json::json!(baseline.executable_sha256);
			if let Some(off) = off_execute_ms_median {
				let distance = off - baseline.q21_toron_execute_ms_median;
				summary["off_arm_distance_from_sealed_ms"] = serde_json::json!(distance);
				summary["off_arm_distance_from_sealed_fraction"] =
					serde_json::json!(distance / baseline.q21_toron_execute_ms_median);
			}
		}
		None => summary["sealed_baseline"] = serde_json::Value::Null,
	}
}

fn adr588_validate_row(
	row: &serde_json::Value,
	arm: &Adr588ArmExpectation,
	dataset: &Adr588Dataset,
) -> Result<(), String> {
	let name = arm.name;
	let text = |field: &str| row.get(field).and_then(serde_json::Value::as_str);
	if text("status") != Some("pass") {
		return Err(format!("ADR 588 {name} row did not pass: {row}"));
	}
	if text("query") != Some("q21") || text("engine") != Some("toron") {
		return Err(format!("ADR 588 {name} row is not a q21 Toron row"));
	}
	if text("mode") != Some("strict")
		|| row.get("scale_factor").and_then(serde_json::Value::as_f64) != Some(8.0)
		|| row
			.get("memory_per_task")
			.and_then(serde_json::Value::as_u64)
			!= Some(2_147_483_648)
		|| row
			.get("spill_bytes_reserved")
			.and_then(serde_json::Value::as_u64)
			!= Some(2_147_483_648)
		|| row
			.get("partition_work_concurrency_requested")
			.and_then(serde_json::Value::as_u64)
			!= Some(1)
	{
		return Err(format!(
			"ADR 588 {name} row is not at the sealed coordinate (mode/sf/memory/spill/parallelism)"
		));
	}
	if row.get("diagnostics_disabled") != Some(&serde_json::Value::Bool(true)) {
		return Err(format!(
			"ADR 588 {name} row does not have diagnostics disabled"
		));
	}
	match text("digest") {
		Some(digest) if digest == ADR588_SEALED_Q21_DIGEST => {}
		Some(digest) => {
			return Err(format!(
				"ADR 588 digest parity failed on a {name} row: {digest}"
			));
		}
		None => return Err(format!("ADR 588 {name} row lacks a digest")),
	}
	if text("dataset_verification") != Some("full_hash") {
		return Err(format!(
			"ADR 588 {name} row was not run over a fully verified dataset"
		));
	}
	if text("dataset_manifest_fingerprint") != Some(dataset.manifest_fingerprint.as_str()) {
		return Err(format!(
			"ADR 588 {name} row names dataset {:?}, expected {}",
			text("dataset_manifest_fingerprint"),
			dataset.manifest_fingerprint
		));
	}
	if text("data_source") != Some(dataset.data_source.as_str()) {
		return Err(format!(
			"ADR 588 {name} row names data source {:?}, expected {}",
			text("data_source"),
			dataset.data_source
		));
	}
	let identity = |field: &str| {
		row.pointer(&format!("/evidence_identity/{field}"))
			.and_then(serde_json::Value::as_str)
	};
	if identity("executable_sha256") != Some(arm.manifest.toron.sha256.as_str())
		|| identity("source_tree_id") != Some(arm.manifest.source_tree_id.as_str())
		|| identity("recipe_sha256") != Some(arm.manifest.recipe_digest.as_str())
	{
		return Err(format!(
			"ADR 588 {name} row reports an identity that is not its manifest's: {}",
			row.get("evidence_identity").cloned().unwrap_or_default()
		));
	}
	if text("adr588_launched_manifest") != Some(arm.manifest_path.as_str()) {
		return Err(format!(
			"ADR 588 {name} row was launched with manifest {:?}, expected {}",
			text("adr588_launched_manifest"),
			arm.manifest_path
		));
	}
	if text("adr588_launched_executable_sha256") != Some(arm.manifest.toron.sha256.as_str()) {
		return Err(format!(
			"ADR 588 {name} row was launched from the other arm's executable"
		));
	}
	Ok(())
}

fn run_adr588_child(
	arm: &Adr588Arm,
	run_index: u32,
	warmup: bool,
	mode: &str,
	sf: f64,
	data_source: &str,
	data_dir: &Path,
) -> Result<serde_json::Value, String> {
	let mut args = vec![
		"perf-run-one".to_string(),
		"--engine".to_string(),
		"toron".to_string(),
		"--query".to_string(),
		"q21".to_string(),
		"--mode".to_string(),
		mode.to_string(),
		"--sf".to_string(),
		sf.to_string(),
		"--run-index".to_string(),
		run_index.to_string(),
		"--data-source".to_string(),
		data_source.to_string(),
		"--data-dir".to_string(),
		data_dir.display().to_string(),
	];
	if warmup {
		args.push("--warmup-run".to_string());
	}
	let envs = arm.envs();
	let env_refs = envs
		.iter()
		.map(|(key, value)| (key.as_str(), value.as_str()))
		.collect::<Vec<_>>();
	let row = run_perf_child_with_executable(&arm.executable, &args, &env_refs, None)?;
	if row.get("status").and_then(serde_json::Value::as_str) != Some("pass") {
		return Err(format!("ADR 588 {} child did not pass: {row}", arm.name));
	}
	Ok(row)
}

/// The row-level identity gate, digest parity, and the predeclared acceptance rule. The rule is
/// fixed: retained only if the median gain exceeds twice the control range and every pair's
/// gain is strictly positive; a zero pair fails exactly as a negative one.
fn evaluate_adr588_pairs(
	rows: &[serde_json::Value],
	expected: &Adr588Expected,
	expected_dataset: &Adr588Dataset,
) -> Result<serde_json::Value, String> {
	if rows.len() != ADR585_PAIRS * 2 {
		return Err(format!(
			"ADR 588 requires {} measured rows, found {}",
			ADR585_PAIRS * 2,
			rows.len()
		));
	}
	adr588_manifest_gate(&expected.control.manifest, &expected.treatment.manifest)?;
	let mut executables = BTreeSet::new();
	for row in rows {
		let arm = row
			.get("adr588_arm")
			.and_then(serde_json::Value::as_str)
			.ok_or_else(|| "ADR 588 row lacks its arm".to_string())?;
		let expected = match arm {
			"control" => &expected.control,
			"treatment" => &expected.treatment,
			other => return Err(format!("ADR 588 row names unknown arm {other:?}")),
		};
		adr588_validate_row(row, expected, &expected_dataset)?;
		executables.insert(expected.manifest.toron.sha256.clone());
	}
	if executables.len() != 2 {
		return Err(format!(
			"ADR 588 requires exactly two executables across the rows, found {}",
			executables.len()
		));
	}
	let mut pair_records = Vec::new();
	let mut gains = Vec::new();
	let mut controls = Vec::new();
	let mut treatments = Vec::new();
	for pair in 0..ADR585_PAIRS {
		let pair_rows = rows
			.iter()
			.filter(|row| {
				row.get("adr588_pair").and_then(serde_json::Value::as_u64) == Some(pair as u64)
			})
			.collect::<Vec<_>>();
		let find = |arm: &str| {
			pair_rows
				.iter()
				.find(|row| row.get("adr588_arm").and_then(serde_json::Value::as_str) == Some(arm))
				.copied()
				.ok_or_else(|| format!("ADR 588 pair {pair} lacks {arm} arm"))
		};
		let control_row = find("control")?;
		let treatment_row = find("treatment")?;
		let expected_order = if pair % 2 == 0 {
			[("control", 0), ("treatment", 1)]
		} else {
			[("treatment", 0), ("control", 1)]
		};
		for (arm, position) in expected_order {
			if find(arm)?
				.get("adr588_pair_position")
				.and_then(serde_json::Value::as_u64)
				!= Some(position)
			{
				return Err(format!(
					"ADR 588 pair {pair} does not use its declared alternating order"
				));
			}
		}
		let control_ms = required_f64(control_row, "execute_ms")?;
		let treatment_ms = required_f64(treatment_row, "execute_ms")?;
		if control_ms <= 0.0 || treatment_ms <= 0.0 {
			return Err(format!("ADR 588 pair {pair} has non-positive execute time"));
		}
		let gain = control_ms - treatment_ms;
		gains.push(gain);
		controls.push(control_ms);
		treatments.push(treatment_ms);
		pair_records.push(serde_json::json!({
			"pair": pair,
			"execute_control_ms": control_ms,
			"execute_treatment_ms": treatment_ms,
			"gain_ms": gain,
			"gain_fraction": gain / control_ms,
			"digest": control_row.get("digest"),
		}));
	}
	let control_min = controls.iter().copied().fold(f64::INFINITY, f64::min);
	let control_max = controls.iter().copied().fold(f64::NEG_INFINITY, f64::max);
	let control_range = control_max - control_min;
	let control_median = median(&mut controls.clone());
	let treatment_median = median(&mut treatments.clone());
	let gain_median = median(&mut gains.clone());
	let verdict = adr588_verdict(&gains, control_range);
	Ok(serde_json::json!({
		"pair_count": ADR585_PAIRS,
		"pairs": pair_records,
		"control_execute_median_ms": control_median,
		"control_execute_range_ms": control_range,
		"treatment_execute_median_ms": treatment_median,
		"gain_median_ms": gain_median,
		"gain_median_fraction_of_control": gain_median / control_median,
		"required_gain_ms": 2.0 * control_range,
		"every_pair_positive": gains.iter().all(|gain| *gain > 0.0),
		"adr586_reference_off_execute_median_ms": ADR587_ADR586_REFERENCE_OFF_MEDIAN_MS,
		"digest": ADR588_SEALED_Q21_DIGEST,
		"verdict": verdict,
	}))
}

/// The predeclared retention rule. Fixed before the run; not revisited after it.
fn adr588_verdict(gains: &[f64], control_range: f64) -> serde_json::Value {
	let mut sorted = gains.to_vec();
	let gain_median = median(&mut sorted);
	let required = 2.0 * control_range;
	let every_pair_positive = !gains.is_empty() && gains.iter().all(|gain| *gain > 0.0);
	let clears = gain_median > required;
	let status = if clears && every_pair_positive {
		"retained"
	} else {
		"not_established"
	};
	let reason = match (clears, every_pair_positive) {
		(true, true) => {
			"median gain exceeds twice the control range and every pair is strictly positive"
		}
		(false, true) => "median gain does not exceed twice the control range",
		(true, false) => "a pair has zero or negative gain",
		(false, false) => {
			"median gain does not exceed twice the control range and a pair has zero or negative gain"
		}
	};
	serde_json::json!({
		"status": status,
		"reason": reason,
		"gain_median_ms": gain_median,
		"required_gain_ms": required,
		"every_pair_positive": every_pair_positive,
		"consequence": if status == "retained" { "cut retained" } else { "cut reverted in the same commit; candidate closed" },
	})
}

#[allow(clippy::too_many_arguments)]
fn run_adr587_child(
	run_index: u32,
	warmup: bool,
	on: bool,
	mode: &str,
	sf: f64,
	data_source: &str,
	data_dir: &Path,
	output_dir: &Path,
) -> Result<serde_json::Value, String> {
	let mut args = vec![
		"perf-run-one".to_string(),
		"--engine".to_string(),
		"toron".to_string(),
		"--query".to_string(),
		"q21".to_string(),
		"--mode".to_string(),
		mode.to_string(),
		"--sf".to_string(),
		sf.to_string(),
		"--run-index".to_string(),
		run_index.to_string(),
		"--data-source".to_string(),
		data_source.to_string(),
		"--data-dir".to_string(),
		data_dir.display().to_string(),
	];
	if warmup {
		args.push("--warmup-run".to_string());
	}
	let profile = output_dir
		.join("profiles")
		.join(format!("pair-{run_index:02}-on.json"));
	let profile_text = profile
		.to_str()
		.ok_or_else(|| "ADR 587 profile path is not UTF-8".to_string())?;
	let row = if on {
		let wrapper = adr587_perf_record_args(
			ADR587_PERF_FREQUENCY_HZ,
			&adr587_perf_data_path(output_dir, run_index),
		);
		run_perf_child_args_with_envs_wrapped(
			&args,
			&[("TORON_NODE_ATTRIBUTION_PROFILE_PATH", profile_text)],
			Some(&wrapper),
		)?
	} else {
		run_perf_child_args(&args)?
	};
	if row.get("status").and_then(serde_json::Value::as_str) != Some("pass") {
		return Err(format!("ADR 587 child did not pass: {row}"));
	}
	Ok(row)
}

const ADR587_EXACT_TERMS: [&str; 3] = [
	"insert_grow",
	"insert_rows_excluding_grow",
	"insert_residual",
];

/// ADR 587's layer over ADR 586's: the fifth band per summary, its algebra and bound, the
/// three-term owner rule over the exact wall terms, the sampled classes reported as shares
/// with a consistent-or-unresolved ranking, coverage as an estimate, and gross pricing.
fn evaluate_adr587_pairs(rows: &[serde_json::Value]) -> Result<serde_json::Value, String> {
	let mut base = evaluate_adr586_layer(rows, &ADR587_TAGS)?;
	let mut pair_bands = Vec::new();
	let mut samples: BTreeMap<String, Vec<f64>> = BTreeMap::new();
	let mut all_insert_residuals_complete = true;
	let mut class_shares: BTreeMap<String, Vec<f64>> = BTreeMap::new();
	let mut class_periods: BTreeMap<String, u64> = BTreeMap::new();
	let mut rankings: BTreeMap<String, Vec<Vec<(String, u64)>>> = BTreeMap::new();
	let mut coverage: BTreeMap<String, Vec<f64>> = BTreeMap::new();
	for pair in 0..ADR585_PAIRS {
		let pair_rows = rows
			.iter()
			.filter(|row| {
				row.get("adr587_pair").and_then(serde_json::Value::as_u64) == Some(pair as u64)
			})
			.collect::<Vec<_>>();
		let off = pair_rows
			.iter()
			.find(|row| row.get("adr587_arm").and_then(serde_json::Value::as_str) == Some("off"))
			.ok_or_else(|| format!("ADR 587 pair {pair} lacks off arm"))?;
		let on = pair_rows
			.iter()
			.find(|row| row.get("adr587_arm").and_then(serde_json::Value::as_str) == Some("on"))
			.ok_or_else(|| format!("ADR 587 pair {pair} lacks on arm"))?;
		let execute_off_ms = required_f64(off, "execute_ms")?;
		let execute_on_ms = required_f64(on, "execute_ms")?;
		let execute_on_micros = (execute_on_ms * 1000.0).round() as u64;
		let overhead = (execute_on_ms - execute_off_ms).max(0.0) * 1000.0;
		let (_, receipt) = lifecycle_target_receipt(on, pair, "ADR 587")?;
		let bins = on
			.get("adr587_bins")
			.ok_or_else(|| format!("ADR 587 pair {pair} on arm lacks profiler bins"))?;
		if bins.get("lost_samples").and_then(serde_json::Value::as_u64) != Some(0) {
			return Err(format!(
				"ADR 587 pair {pair} profile lost samples or lacks the count"
			));
		}
		let mut band = serde_json::json!({"pair": pair});
		for summary in ["semi", "anti"] {
			let insert = receipt_u64(receipt, &format!("{summary}_write_insert_micros"))?;
			let rows_micros = receipt_u64(receipt, &format!("{summary}_insert_rows_micros"))?;
			let grow = receipt_u64(receipt, &format!("{summary}_insert_grow_micros"))?;
			let excluding = receipt_u64(
				receipt,
				&format!("{summary}_insert_rows_excluding_grow_micros"),
			)?;
			let residual = receipt_u64(receipt, &format!("{summary}_insert_residual_micros"))?;
			let grow_events = receipt_u64(receipt, &format!("{summary}_insert_grow_events"))?;
			if rows_micros.checked_add(residual) != Some(insert)
				|| grow.checked_add(excluding) != Some(rows_micros)
			{
				return Err(format!(
					"ADR 587 pair {pair} {summary} insert algebra does not reconcile"
				));
			}
			let residual_bound = overhead + 0.05 * insert as f64;
			let residual_complete = residual as f64 <= residual_bound;
			all_insert_residuals_complete &= residual_complete;
			for (term, micros) in [
				("insert_grow", grow),
				("insert_rows_excluding_grow", excluding),
				("insert_residual", residual),
			] {
				samples
					.entry(format!("{summary}_{term}"))
					.or_default()
					.push(micros as f64 / execute_on_micros as f64 * execute_off_ms);
			}
			let bin = bins
				.pointer(&format!("/summaries/{summary}"))
				.ok_or_else(|| format!("ADR 587 pair {pair} bins lack {summary}"))?;
			if bin.get("thread_present") != Some(&serde_json::Value::Bool(true)) {
				return Err(format!(
					"ADR 587 pair {pair} {summary} window thread is absent from the profile (missing TIDs {})",
					bin.get("missing_tids").cloned().unwrap_or_default()
				));
			}
			let rows_sampled = bin["rows_inclusive"]["period_ns"]
				.as_u64()
				.unwrap_or_default();
			let growth_sampled = bin["growth"]["period_ns"].as_u64().unwrap_or_default();
			let excluding_sampled = rows_sampled.saturating_sub(growth_sampled);
			let mut ranking = Vec::new();
			for class in ADR587_ROWS_CLASSES {
				let period = bin["classes"][class]["period_ns"]
					.as_u64()
					.unwrap_or_default();
				*class_periods
					.entry(format!("{summary}_{class}"))
					.or_default() += period;
				let share = if excluding_sampled == 0 {
					0.0
				} else {
					period as f64 / excluding_sampled as f64
				};
				class_shares
					.entry(format!("{summary}_{class}"))
					.or_default()
					.push(share);
				ranking.push((class.to_string(), period));
			}
			ranking.sort_by(|left, right| right.1.cmp(&left.1));
			rankings
				.entry(summary.to_string())
				.or_default()
				.push(ranking);
			for class in ADR587_GROWTH_CLASSES {
				let period = bin["classes"][class]["period_ns"]
					.as_u64()
					.unwrap_or_default();
				*class_periods
					.entry(format!("{summary}_{class}"))
					.or_default() += period;
				let share = if growth_sampled == 0 {
					0.0
				} else {
					period as f64 / growth_sampled as f64
				};
				class_shares
					.entry(format!("{summary}_{class}"))
					.or_default()
					.push(share);
			}
			let ratio = required_f64(bin, "estimated_coverage_ratio")?;
			coverage.entry(summary.to_string()).or_default().push(ratio);
			band[format!("{summary}_write_insert_micros")] = serde_json::json!(insert);
			band[format!("{summary}_insert_rows_micros")] = serde_json::json!(rows_micros);
			band[format!("{summary}_insert_grow_micros")] = serde_json::json!(grow);
			band[format!("{summary}_insert_grow_events")] = serde_json::json!(grow_events);
			band[format!("{summary}_insert_rows_excluding_grow_micros")] =
				serde_json::json!(excluding);
			band[format!("{summary}_insert_residual_micros")] = serde_json::json!(residual);
			band[format!("{summary}_insert_residual_bound_micros")] =
				serde_json::json!(residual_bound);
			band[format!("{summary}_insert_residual_complete")] =
				serde_json::json!(residual_complete);
			band[format!("{summary}_insert_finish_micros")] = serde_json::json!(receipt_u64(
				receipt,
				&format!("{summary}_insert_finish_micros")
			)?);
			band[format!("{summary}_insert_total_micros")] = serde_json::json!(receipt_u64(
				receipt,
				&format!("{summary}_insert_total_micros")
			)?);
			band[format!("{summary}_sampled")] = bin.clone();
			band[format!("{summary}_walks")] = adr587_walks_value(on, summary)?;
			band[format!("{summary}_order_census")] = adr589_census_value(on, summary)?;
		}
		pair_bands.push(band);
	}
	let off_range = required_f64(&base, "off_execute_range_ms")?;
	let off_median = required_f64(&base, "off_execute_median_ms")?;
	let owner_floor = required_f64(&base, "owner_floor_ms")?;
	let perturbation_accepted = base["perturbation_gate"] == "accepted";
	let base_profiles_complete =
		base["all_profiles_complete"] == true && base["all_write_residuals_complete"] == true;
	let medians_for = |summary: &str| {
		let mut medians = ADR587_EXACT_TERMS
			.iter()
			.map(|term| {
				let name = format!("{summary}_{term}");
				let mut values = samples.get(&name).cloned().unwrap_or_default();
				(name, median(&mut values))
			})
			.collect::<Vec<_>>();
		medians.sort_by(|left, right| right.1.total_cmp(&left.1));
		medians
	};
	let semi_medians = medians_for("semi");
	let anti_medians = medians_for("anti");
	let owner = adr586_owner(
		&semi_medians,
		owner_floor,
		off_range,
		off_median,
		perturbation_accepted,
		base_profiles_complete && all_insert_residuals_complete,
	);
	let pricing = adr587_removable_pricing(&owner, owner_floor);
	let terms_value = |medians: &[(String, f64)]| {
		medians
			.iter()
			.map(|(term, value)| {
				serde_json::json!({
					"term": term,
					"median_scaled_ms": value,
					"share_of_off_execute": value / off_median,
					"clears_floor": *value > owner_floor,
				})
			})
			.collect::<Vec<_>>()
	};
	let classes_value = |summary: &str, classes: &[&str]| {
		classes
			.iter()
			.map(|class| {
				let name = format!("{summary}_{class}");
				let shares = class_shares.get(&name).cloned().unwrap_or_default();
				let period = class_periods.get(&name).copied().unwrap_or_default();
				let total: u64 = classes
					.iter()
					.map(|other| {
						class_periods
							.get(&format!("{summary}_{other}"))
							.copied()
							.unwrap_or_default()
					})
					.sum();
				let share_all_pairs = if total == 0 {
					0.0
				} else {
					period as f64 / total as f64
				};
				serde_json::json!({
					"class": class,
					"period_ns_all_pairs": period,
					"share_per_pair": shares,
					"share_all_pairs": share_all_pairs,
				})
			})
			.collect::<Vec<_>>()
	};
	let ranking_value = |summary: &str| {
		adr587_ranking_value(rankings.get(summary).map(Vec::as_slice).unwrap_or(&[]))
	};
	let coverage_value = |summary: &str| {
		let mut values = coverage.get(summary).cloned().unwrap_or_default();
		serde_json::json!({
			"estimated_coverage_ratio_per_pair": values.clone(),
			"estimated_coverage_ratio_median": median(&mut values),
			"reading": "estimate: periods straddle window edges, sampling has phase, the kernel may throttle; the discrepancy is unexplained, not attributed",
		})
	};
	base["adr587_pair_bands"] = serde_json::json!(pair_bands);
	base["all_insert_residuals_complete"] = serde_json::json!(all_insert_residuals_complete);
	base["semi_insert_terms_scaled_ms"] = serde_json::json!(terms_value(&semi_medians));
	base["anti_insert_terms_scaled_ms"] = serde_json::json!(terms_value(&anti_medians));
	base["semi_rows_classes"] = serde_json::json!(classes_value("semi", &ADR587_ROWS_CLASSES));
	base["semi_growth_classes"] = serde_json::json!(classes_value("semi", &ADR587_GROWTH_CLASSES));
	base["anti_rows_classes"] = serde_json::json!(classes_value("anti", &ADR587_ROWS_CLASSES));
	base["anti_growth_classes"] = serde_json::json!(classes_value("anti", &ADR587_GROWTH_CLASSES));
	base["semi_rows_ranking"] = ranking_value("semi");
	base["anti_rows_ranking"] = ranking_value("anti");
	base["semi_coverage"] = coverage_value("semi");
	base["anti_coverage"] = coverage_value("anti");
	base["adr586_five_term_owner"] = base["owner"].clone();
	base["adr586_reference_semi_write_medians_scaled_ms"] = serde_json::json!(
		ADR587_ADR586_REFERENCE_SEMI_WRITE_MEDIANS
			.iter()
			.map(|(term, value)| serde_json::json!({"term": term, "median_scaled_ms": value}))
			.collect::<Vec<_>>()
	);
	base["adr586_reference_off_execute_median_ms"] =
		serde_json::json!(ADR587_ADR586_REFERENCE_OFF_MEDIAN_MS);
	base["adr586_reference_off_execute_range_ms"] =
		serde_json::json!(ADR587_ADR586_REFERENCE_OFF_RANGE_MS);
	base["owner"] = owner;
	base["owner_removable_pricing"] = pricing;
	base["adr589_opening_rule"] = adr589_opening_rule(&pair_bands);
	Ok(base)
}

/// The walk denominators from one on-arm row's counters, both stated: the two-walk build's
/// `2 * appended + lookup_found - empty_table_lookups` and the single-walk build's
/// `lookups - empty_table_lookups + growth_reprobes`. A reader picks the one that matches the
/// executable's insert path; the receipt never picks for it, and `probe_steps` is reported per
/// walk under each.
fn adr587_walks_value(row: &serde_json::Value, summary: &str) -> Result<serde_json::Value, String> {
	let counters = row
		.get("node_attribution")
		.and_then(|profile| profile.get("aggregate_invocations"))
		.and_then(serde_json::Value::as_array)
		.and_then(|invocations| {
			invocations.iter().find(|invocation| {
				invocation.get("route").and_then(serde_json::Value::as_str)
					== Some("inner_prefix_semi_anti_grouped")
			})
		})
		.and_then(|invocation| invocation.get("diagnostics"))
		.and_then(serde_json::Value::as_array)
		.and_then(|entries| {
			entries.iter().find(|entry| {
				entry.get("name").and_then(serde_json::Value::as_str)
					== Some(&format!("{summary}_insert_windows"))
			})
		})
		.and_then(|entry| entry.pointer("/value/counters"))
		.ok_or_else(|| format!("ADR 587 row lacks {summary} insert counters"))?;
	let count = |name: &str| receipt_u64(counters, name);
	let lookups = count("lookups")?;
	let found = count("lookup_found")?;
	let appended = count("appended")?;
	let steps = count("probe_steps")?;
	let empty = counters
		.get("empty_table_lookups")
		.and_then(serde_json::Value::as_u64)
		.unwrap_or(0);
	let reprobes = counters
		.get("growth_reprobes")
		.and_then(serde_json::Value::as_u64)
		.unwrap_or(0);
	let two_walk = (2 * appended + found).saturating_sub(empty);
	let single_walk = lookups.saturating_sub(empty) + reprobes;
	let per = |walks: u64| {
		if walks == 0 {
			0.0
		} else {
			steps as f64 / walks as f64
		}
	};
	Ok(serde_json::json!({
		"lookups": lookups,
		"lookup_found": found,
		"appended": appended,
		"probe_steps": steps,
		"empty_table_lookups": empty,
		"growth_reprobes": reprobes,
		"two_walk_denominator": two_walk,
		"probe_steps_per_two_walk": per(two_walk),
		"single_walk_denominator": single_walk,
		"probe_steps_per_single_walk": per(single_walk),
	}))
}

/// The consistent-or-unresolved ranking of the row-loop classes across pairs. A pair with no
/// sampled period in any class, or a tie at the top, has no ranking; declaration order must
/// never be read as one. A ranking is consistent only when every pair has a strict leader and
/// the same order.
fn adr587_ranking_value(orders: &[Vec<(String, u64)>]) -> serde_json::Value {
	let order_names = orders
		.iter()
		.map(|order| {
			order
				.iter()
				.map(|(class, _)| class.clone())
				.collect::<Vec<_>>()
		})
		.collect::<Vec<_>>();
	let mut status = "consistent_observation";
	if orders.is_empty() {
		status = "unresolved_no_pairs";
	}
	for order in orders {
		let top = order.first().map(|(_, period)| *period).unwrap_or(0);
		let second = order.get(1).map(|(_, period)| *period).unwrap_or(0);
		if top == 0 {
			status = "unresolved_no_samples";
			break;
		}
		if top == second {
			status = "unresolved_tie_at_the_top";
			break;
		}
	}
	if status == "consistent_observation" {
		if !order_names.windows(2).all(|pair| pair[0] == pair[1]) {
			status = "unresolved_ranking_changes_between_pairs";
		} else if order_names
			.first()
			.and_then(|order| order.first())
			.map(String::as_str)
			== Some("other")
		{
			status = "unresolved_other_leads";
		}
	}
	let leader = if status == "consistent_observation" {
		serde_json::json!(order_names[0][0])
	} else {
		serde_json::Value::Null
	};
	serde_json::json!({
		"status": status,
		"order_per_pair": order_names,
		"period_ns_per_pair": orders.iter().map(|order| order.iter().map(|(_, period)| *period).collect::<Vec<_>>()).collect::<Vec<_>>(),
		"leading_class": leader,
		"significance": "none claimed: frequency-mode periods vary, samples are correlated in time, skid is systematic",
	})
}

/// ADR 589's ordering census for one summary of one on-arm row, read from the windows
/// diagnostic: per-partition counts and running maxima, the straddle attribution of found
/// lookups, and the consumed-batch summary. Absent on executables older than the census.
fn adr589_census_value(
	row: &serde_json::Value,
	summary: &str,
) -> Result<serde_json::Value, String> {
	let diagnostic = row
		.get("node_attribution")
		.and_then(|profile| profile.get("aggregate_invocations"))
		.and_then(serde_json::Value::as_array)
		.and_then(|invocations| {
			invocations.iter().find(|invocation| {
				invocation.get("route").and_then(serde_json::Value::as_str)
					== Some("inner_prefix_semi_anti_grouped")
			})
		})
		.and_then(|invocation| invocation.get("diagnostics"))
		.and_then(serde_json::Value::as_array)
		.and_then(|entries| {
			entries.iter().find(|entry| {
				entry.get("name").and_then(serde_json::Value::as_str)
					== Some(&format!("{summary}_insert_windows"))
			})
		})
		.and_then(|entry| entry.get("value"))
		.ok_or_else(|| format!("ADR 589 row lacks the {summary} insert windows"))?;
	let census = diagnostic
		.get("order_census")
		.ok_or_else(|| format!("ADR 589 row lacks the {summary} order census"))?;
	let counters = diagnostic
		.get("counters")
		.ok_or_else(|| format!("ADR 589 {summary} windows lack counters"))?;
	let partitions = census
		.get("partitions")
		.and_then(serde_json::Value::as_array)
		.ok_or_else(|| format!("ADR 589 {summary} census lacks partitions"))?;
	if partitions.is_empty() {
		return Err(format!("ADR 589 {summary} census has no partitions"));
	}
	let sum = |field: &str| -> Result<u64, String> {
		partitions.iter().try_fold(0u64, |total, partition| {
			receipt_u64(partition, field).map(|value| total + value)
		})
	};
	let below_max_per_partition = partitions
		.iter()
		.map(|partition| receipt_u64(partition, "below_max"))
		.collect::<Result<Vec<_>, _>>()?;
	// Coverage: the consumed-batch record must name exactly the census's partitions, and the
	// batch totals must agree with each other and with the rows windows.
	let consumed_text = census
		.get("consumed")
		.and_then(serde_json::Value::as_str)
		.ok_or_else(|| format!("ADR 589 {summary} census lacks the consumed string"))?;
	let consumed = adr589_decode_consumed(consumed_text)
		.map_err(|error| format!("ADR 589 {summary} consumed batches: {error}"))?;
	let consumed_batches = receipt_u64(census, "consumed_batches")?;
	if consumed.len() as u64 != consumed_batches {
		return Err(format!(
			"ADR 589 {summary} consumed string decodes to {} batches, census says {consumed_batches}",
			consumed.len()
		));
	}
	let mut seen = vec![0u64; partitions.len()];
	for batch in &consumed {
		let Some(count) = seen.get_mut(batch.partition) else {
			return Err(format!(
				"ADR 589 {summary} consumed a batch on partition {} the census does not cover",
				batch.partition
			));
		};
		*count += 1;
	}
	for (partition, count) in seen.iter().enumerate() {
		let census_batches = receipt_u64(&partitions[partition], "batches")?;
		if *count != census_batches {
			return Err(format!(
				"ADR 589 {summary} partition {partition} has {census_batches} census batches but {count} consumed"
			));
		}
		if *count == 0 {
			return Err(format!(
				"ADR 589 {summary} partition {partition} consumed no batches"
			));
		}
	}
	let rows_windows = receipt_u64(diagnostic, "rows_window_count")?;
	let batches = sum("batches")?;
	if batches != consumed_batches || rows_windows != consumed_batches {
		return Err(format!(
			"ADR 589 {summary} batch totals disagree: census {batches}, consumed {consumed_batches}, rows windows {rows_windows}"
		));
	}
	let decoded_restarts = consumed.iter().filter(|batch| batch.restart).count() as u64;
	let restarts = sum("restarts")?;
	if decoded_restarts != restarts {
		return Err(format!(
			"ADR 589 {summary} restarts disagree: census {restarts}, consumed {decoded_restarts}"
		));
	}
	// Accounting: every non-cache-hit row was classified exactly once, and every found lookup
	// was attributed exactly once.
	let above = sum("above_max")?;
	let equal = sum("equal_max")?;
	let below = sum("below_max")?;
	let lookups = receipt_u64(counters, "lookups")?;
	if above + equal + below != lookups {
		return Err(format!(
			"ADR 589 {summary} classes {above} + {equal} + {below} do not sum to lookups {lookups}"
		));
	}
	let found_at_straddle = receipt_u64(census, "found_at_straddle")?;
	let found_elsewhere = receipt_u64(census, "found_elsewhere")?;
	let lookup_found = receipt_u64(counters, "lookup_found")?;
	if found_at_straddle + found_elsewhere != lookup_found {
		return Err(format!(
			"ADR 589 {summary} found {found_at_straddle} + {found_elsewhere} do not sum to lookup_found {lookup_found}"
		));
	}
	let boundary_straddles = receipt_u64(census, "boundary_straddles")?;
	if found_at_straddle > boundary_straddles {
		return Err(format!(
			"ADR 589 {summary} found_at_straddle {found_at_straddle} exceeds boundary_straddles {boundary_straddles}"
		));
	}
	Ok(serde_json::json!({
		"partitions": partitions,
		"partition_count": partitions.len(),
		"above_max": above,
		"equal_max": equal,
		"below_max": below,
		"below_max_per_partition": below_max_per_partition,
		"batches": batches,
		"restarts": restarts,
		"boundary_straddles": boundary_straddles,
		"found_at_straddle": found_at_straddle,
		"found_elsewhere": found_elsewhere,
		"lookups": lookups,
		"lookup_found": lookup_found,
		"consumed_batches": consumed_batches,
		"consumed_batches_per_partition": seen,
		"global_restarts": adr589_global_restarts(&consumed),
	}))
}

/// One decoded consumed batch, as the engine recorded it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Adr589ConsumedBatch {
	partition: usize,
	first_key: i64,
	last_key: i64,
	restart: bool,
}

/// The exact inverse of the engine's `encode_consumed`: `p:first:last:r;...`, `first` a delta
/// from the previous batch's last key on `p` (`=` prefix for an absolute first), `last` a delta
/// from `first`, `r` `!` on a restart.
fn adr589_decode_consumed(text: &str) -> Result<Vec<Adr589ConsumedBatch>, String> {
	let mut consumed = Vec::new();
	if text.is_empty() {
		return Ok(consumed);
	}
	let mut previous_last: Vec<Option<i64>> = Vec::new();
	for item in text.split(';') {
		let mut parts = item.split(':');
		let partition = parts
			.next()
			.and_then(|value| value.parse::<usize>().ok())
			.ok_or_else(|| "batch lacks a partition".to_string())?;
		let first = parts
			.next()
			.ok_or_else(|| "batch lacks a first key".to_string())?;
		let last_delta = parts
			.next()
			.and_then(|value| value.parse::<i64>().ok())
			.ok_or_else(|| "batch lacks a last key".to_string())?;
		let restart = parts.next() == Some("!");
		if previous_last.len() <= partition {
			previous_last.resize(partition + 1, None);
		}
		let first_key = match first.strip_prefix('=') {
			Some(absolute) => absolute
				.parse::<i64>()
				.map_err(|_| "first key is not numeric".to_string())?,
			None => {
				let delta = first
					.parse::<i64>()
					.map_err(|_| "first delta is not numeric".to_string())?;
				previous_last[partition]
					.ok_or_else(|| "delta without a previous batch on the partition".to_string())?
					.wrapping_add(delta)
			}
		};
		let last_key = first_key.wrapping_add(last_delta);
		previous_last[partition] = Some(last_key);
		consumed.push(Adr589ConsumedBatch {
			partition,
			first_key,
			last_key,
			restart,
		});
	}
	Ok(consumed)
}

/// How many consumed batches opened below the last key of the batch consumed just before
/// them, on any partition: the restarts of the stream as a whole, as opposed to within one
/// partition.
fn adr589_global_restarts(consumed: &[Adr589ConsumedBatch]) -> u64 {
	consumed
		.windows(2)
		.filter(|pair| pair[1].first_key < pair[0].last_key)
		.count() as u64
}

/// ADR 589's opening rule, as the record states it: phase 2 opens only if, on both summaries
/// and in every pair, `below_max` is zero on every partition and `found_elsewhere` is zero.
/// Anything else, including a missing census, closes phase 2.
fn adr589_opening_rule(pair_bands: &[serde_json::Value]) -> serde_json::Value {
	let mut violations = Vec::new();
	let mut checked = 0usize;
	for band in pair_bands {
		let pair = band.get("pair").cloned().unwrap_or_default();
		for summary in ["semi", "anti"] {
			let Some(census) = band.get(format!("{summary}_order_census")) else {
				violations.push(
					serde_json::json!({"pair": pair, "summary": summary, "reason": "census missing"}),
				);
				continue;
			};
			checked += 1;
			let below = census
				.get("below_max_per_partition")
				.and_then(serde_json::Value::as_array)
				.cloned()
				.unwrap_or_default();
			if below
				.iter()
				.any(|value| value.as_u64().unwrap_or(u64::MAX) != 0)
			{
				violations.push(serde_json::json!({"pair": pair, "summary": summary, "reason": "below_max non-zero", "below_max_per_partition": below}));
			}
			let elsewhere = census
				.get("found_elsewhere")
				.and_then(serde_json::Value::as_u64)
				.unwrap_or(u64::MAX);
			if elsewhere != 0 {
				violations.push(serde_json::json!({"pair": pair, "summary": summary, "reason": "found_elsewhere non-zero", "found_elsewhere": elsewhere}));
			}
			let found = census
				.get("found_at_straddle")
				.and_then(serde_json::Value::as_u64)
				.unwrap_or(0);
			let straddles = census
				.get("boundary_straddles")
				.and_then(serde_json::Value::as_u64)
				.unwrap_or(0);
			if found > straddles {
				violations.push(serde_json::json!({"pair": pair, "summary": summary, "reason": "found_at_straddle exceeds boundary_straddles"}));
			}
		}
	}
	let opens = violations.is_empty() && checked == pair_bands.len() * 2 && checked > 0;
	serde_json::json!({
		"phase_2": if opens { "opens" } else { "closed" },
		"rule": "below_max == 0 on every partition and found_elsewhere == 0, on both summaries, in every pair",
		"censuses_checked": checked,
		"violations": violations,
	})
}

/// Growth is gross removable work (a reservation still allocates, initialises, and first-touches
/// the final table); the row loop is not removable as such; the residual is not a mechanism.
fn adr587_removable_pricing(owner: &serde_json::Value, owner_floor: f64) -> serde_json::Value {
	if owner["status"] != "resolved" {
		return serde_json::json!({"status": "no_owner"});
	}
	let term = owner["term"].as_str().unwrap_or_default();
	let median = owner["median_scaled_ms"].as_f64().unwrap_or(f64::NAN);
	match term {
		"semi_insert_grow" => serde_json::json!({
			"status": "gross_removable",
			"removable_subterm": term,
			"median_scaled_ms": median,
			"clears_floor": median > owner_floor,
			"net_improvement": "unpriced: a reservation still allocates, initialises and first-touches the final table; successor measures paired off/on with the cut beside value parity and peak-memory accounting",
		}),
		"semi_insert_rows_excluding_grow" => serde_json::json!({
			"status": "not_removable",
			"successor": "chosen from the counts and sampled classes: the second walk on a miss, an order-aware build, or the hash and load factor",
		}),
		_ => serde_json::json!({
			"status": "not_a_mechanism",
			"successor": "an incomplete instrument is not an owner",
		}),
	}
}

#[allow(clippy::too_many_arguments)]
fn run_adr586_child(
	run_index: u32,
	warmup: bool,
	on: bool,
	mode: &str,
	sf: f64,
	data_source: &str,
	data_dir: &Path,
	output_dir: &Path,
) -> Result<serde_json::Value, String> {
	let mut args = vec![
		"perf-run-one".to_string(),
		"--engine".to_string(),
		"toron".to_string(),
		"--query".to_string(),
		"q21".to_string(),
		"--mode".to_string(),
		mode.to_string(),
		"--sf".to_string(),
		sf.to_string(),
		"--run-index".to_string(),
		run_index.to_string(),
		"--data-source".to_string(),
		data_source.to_string(),
		"--data-dir".to_string(),
		data_dir.display().to_string(),
	];
	if warmup {
		args.push("--warmup-run".to_string());
	}
	let profile = output_dir
		.join("profiles")
		.join(format!("pair-{run_index:02}-on.json"));
	let profile_text = profile
		.to_str()
		.ok_or_else(|| "ADR 586 profile path is not UTF-8".to_string())?;
	let row = if on {
		run_perf_child_args_with_envs(
			&args,
			&[("TORON_NODE_ATTRIBUTION_PROFILE_PATH", profile_text)],
		)?
	} else {
		run_perf_child_args(&args)?
	};
	if row.get("status").and_then(serde_json::Value::as_str) != Some("pass") {
		return Err(format!("ADR 586 child did not pass: {row}"));
	}
	Ok(row)
}

/// The phase-0 tripwire: relative movement of one unprofiled sample from the reference off
/// median, tripped beyond the predeclared threshold. Coarse on purpose: one sample can catch a
/// gross host shift and cannot establish drift, which the pairs alone measure.
fn adr586_phase0_tripwire(sample_ms: f64, reference_ms: f64) -> serde_json::Value {
	let movement = (sample_ms - reference_ms) / reference_ms;
	serde_json::json!({
		"sample_execute_ms": sample_ms,
		"reference_off_median_ms": reference_ms,
		"movement": movement,
		"threshold": ADR586_PHASE0_THRESHOLD,
		"tripped": movement.abs() > ADR586_PHASE0_THRESHOLD,
	})
}

const ADR586_WRITE_CHILDREN: [&str; 4] = ["filter", "key_program", "repartition", "insert"];

/// ADR 586's layer over the shared three-band evaluation: the fourth band per summary, its
/// algebra and completeness bound, the five-term owner rule over the semi write with its three
/// outcomes, the anti write reported beside it, and the owning child's removable pricing.
fn evaluate_adr586_pairs(rows: &[serde_json::Value]) -> Result<serde_json::Value, String> {
	evaluate_adr586_layer(rows, &ADR586_TAGS)
}

/// ADR 586's fourth-band layer over the shared three-band evaluation, reused by ADR 587 under
/// its own tags: the write children, their algebra and bounds, the five-term rule, the pricing.
fn evaluate_adr586_layer(
	rows: &[serde_json::Value],
	tags: &LifecyclePairTags,
) -> Result<serde_json::Value, String> {
	let mut base = evaluate_lifecycle_pairs(rows, tags)?;
	let mut pair_bands = Vec::new();
	let mut samples: BTreeMap<String, Vec<f64>> = BTreeMap::new();
	let mut all_write_residuals_complete = true;
	for pair in 0..ADR585_PAIRS {
		let pair_rows = rows
			.iter()
			.filter(|row| {
				row.get(tags.pair).and_then(serde_json::Value::as_u64) == Some(pair as u64)
			})
			.collect::<Vec<_>>();
		let off = pair_rows
			.iter()
			.find(|row| row.get(tags.arm).and_then(serde_json::Value::as_str) == Some("off"))
			.ok_or_else(|| format!("ADR 586 pair {pair} lacks off arm"))?;
		let on = pair_rows
			.iter()
			.find(|row| row.get(tags.arm).and_then(serde_json::Value::as_str) == Some("on"))
			.ok_or_else(|| format!("ADR 586 pair {pair} lacks on arm"))?;
		let execute_off_ms = required_f64(off, "execute_ms")?;
		let execute_on_ms = required_f64(on, "execute_ms")?;
		let execute_on_micros = (execute_on_ms * 1000.0).round() as u64;
		let overhead = (execute_on_ms - execute_off_ms).max(0.0) * 1000.0;
		let (_, receipt) = lifecycle_target_receipt(on, pair, tags.label)?;
		let mut band = serde_json::json!({"pair": pair});
		for prefix in ["semi_write", "anti_write"] {
			let write = receipt_u64(receipt, &format!("runtime_prep_{prefix}_micros"))?;
			let mut children_total = 0u64;
			for child in ADR586_WRITE_CHILDREN {
				let micros = receipt_u64(receipt, &format!("{prefix}_{child}_micros"))?;
				let passes = receipt_u64(receipt, &format!("{prefix}_{child}_passes"))?;
				children_total = children_total
					.checked_add(micros)
					.ok_or_else(|| format!("ADR 586 pair {pair} {prefix} child sum overflowed"))?;
				band[format!("{prefix}_{child}_micros")] = serde_json::json!(micros);
				band[format!("{prefix}_{child}_passes")] = serde_json::json!(passes);
				samples
					.entry(format!("{prefix}_{child}"))
					.or_default()
					.push(micros as f64 / execute_on_micros as f64 * execute_off_ms);
			}
			let residual = receipt_u64(receipt, &format!("{prefix}_residual_micros"))?;
			if children_total.checked_add(residual) != Some(write) {
				return Err(format!(
					"ADR 586 pair {pair} {prefix} algebra does not reconcile"
				));
			}
			let ops = receipt
				.get(format!("{prefix}_ops"))
				.and_then(serde_json::Value::as_array)
				.ok_or_else(|| format!("ADR 586 pair {pair} lacks {prefix} op list"))?;
			let residual_bound = overhead + 0.05 * write as f64;
			let residual_complete = residual as f64 <= residual_bound;
			all_write_residuals_complete &= residual_complete;
			samples
				.entry(format!("{prefix}_residual"))
				.or_default()
				.push(residual as f64 / execute_on_micros as f64 * execute_off_ms);
			band[format!("{prefix}_micros")] = serde_json::json!(write);
			band[format!("{prefix}_residual_micros")] = serde_json::json!(residual);
			band[format!("{prefix}_residual_bound_micros")] = serde_json::json!(residual_bound);
			band[format!("{prefix}_residual_complete")] = serde_json::json!(residual_complete);
			band[format!("{prefix}_ops")] = serde_json::json!(ops);
		}
		pair_bands.push(band);
	}
	let off_range = required_f64(&base, "off_execute_range_ms")?;
	let off_median = required_f64(&base, "off_execute_median_ms")?;
	let owner_floor = required_f64(&base, "owner_floor_ms")?;
	let perturbation_accepted = base["perturbation_gate"] == "accepted";
	let base_profiles_complete = base["all_profiles_complete"] == true;
	let medians_for = |prefix: &str| {
		let mut medians = ADR586_WRITE_CHILDREN
			.iter()
			.map(|child| format!("{prefix}_{child}"))
			.chain(std::iter::once(format!("{prefix}_residual")))
			.map(|term| {
				let mut values = samples.get(&term).cloned().unwrap_or_default();
				(term, median(&mut values))
			})
			.collect::<Vec<_>>();
		medians.sort_by(|left, right| right.1.total_cmp(&left.1));
		medians
	};
	let semi_medians = medians_for("semi_write");
	let anti_medians = medians_for("anti_write");
	let owner = adr586_owner(
		&semi_medians,
		owner_floor,
		off_range,
		off_median,
		perturbation_accepted,
		base_profiles_complete && all_write_residuals_complete,
	);
	let pricing = adr586_removable_pricing(&owner, owner_floor);
	let terms_value = |medians: &[(String, f64)]| {
		medians
			.iter()
			.map(|(term, value)| {
				serde_json::json!({
					"term": term,
					"median_scaled_ms": value,
					"share_of_off_execute": value / off_median,
					"clears_floor": *value > owner_floor,
				})
			})
			.collect::<Vec<_>>()
	};
	base["adr586_pair_bands"] = serde_json::json!(pair_bands);
	base["all_write_residuals_complete"] = serde_json::json!(all_write_residuals_complete);
	base["semi_write_terms_scaled_ms"] = serde_json::json!(terms_value(&semi_medians));
	base["anti_write_terms_scaled_ms"] = serde_json::json!(terms_value(&anti_medians));
	base["adr585_thirteen_term_owner"] = base["owner"].clone();
	base["adr585_reference_medians_scaled_ms"] = serde_json::json!(
		ADR586_ADR585_REFERENCE_MEDIANS
			.iter()
			.map(|(term, value)| serde_json::json!({"term": term, "median_scaled_ms": value}))
			.collect::<Vec<_>>()
	);
	base["adr585_reference_off_execute_median_ms"] =
		serde_json::json!(ADR586_ADR585_REFERENCE_OFF_MEDIAN_MS);
	base["adr585_reference_off_execute_range_ms"] =
		serde_json::json!(ADR586_ADR585_REFERENCE_OFF_RANGE_MS);
	base["owner"] = owner;
	base["owner_removable_pricing"] = pricing;
	Ok(base)
}

/// The five-term owner rule with its three outcomes. A leading residual is an incomplete
/// instrument; a leader clearing the floor by more than the off range is the owner; a leader
/// clearing the floor within the off range of the runner-up is an unresolved tie; and no term
/// clearing the floor is the below-floor outcome, which opens no mechanism.
fn adr586_owner(
	medians: &[(String, f64)],
	owner_floor: f64,
	off_range: f64,
	off_median: f64,
	perturbation_accepted: bool,
	profiles_complete: bool,
) -> serde_json::Value {
	if !perturbation_accepted {
		return serde_json::json!({"status": "withheld_perturbation_rejected"});
	}
	if !profiles_complete {
		return serde_json::json!({"status": "withheld_incomplete_profiles"});
	}
	match (medians.first(), medians.get(1)) {
		(Some((name, value)), _) if name.ends_with("_residual") && *value > owner_floor => {
			serde_json::json!({
				"status": "withheld_instrument_incomplete",
				"leading_residual": name,
				"median_scaled_ms": value,
			})
		}
		(Some((name, value)), Some((_, second)))
			if *value > owner_floor && value - second > off_range =>
		{
			serde_json::json!({
				"status": "resolved",
				"term": name,
				"median_scaled_ms": value,
				"share_of_off_execute": value / off_median,
			})
		}
		(Some((name, value)), Some((second_name, second))) if *value > owner_floor => {
			serde_json::json!({
				"status": "unresolved_tie_within_off_range",
				"leading_term": name,
				"median_scaled_ms": value,
				"runner_up": second_name,
				"runner_up_median_scaled_ms": second,
			})
		}
		(Some((name, value)), _) => serde_json::json!({
			"status": "below_floor_no_mechanism",
			"leading_term": name,
			"median_scaled_ms": value,
			"owner_floor_ms": owner_floor,
		}),
		(None, _) => serde_json::json!({"status": "withheld_no_terms"}),
	}
}

/// What a mechanism would remove from the owning child, priced as the record wrote it: the
/// repartition child is gross removable work (the writer discards its partition index but its
/// order feeds the insert's consecutive-key cache, so net is unpriced here); the insert child is
/// not itself removable; filter and key program are unpriced pending the successor.
fn adr586_removable_pricing(owner: &serde_json::Value, owner_floor: f64) -> serde_json::Value {
	if owner["status"] != "resolved" {
		return serde_json::json!({"status": "no_owner"});
	}
	let term = owner["term"].as_str().unwrap_or_default();
	let median = owner["median_scaled_ms"].as_f64().unwrap_or(f64::NAN);
	match term {
		"semi_write_repartition" => serde_json::json!({
			"status": "gross_removable",
			"removable_subterm": term,
			"median_scaled_ms": median,
			"clears_floor": median > owner_floor,
			"net_improvement": "unpriced: successor measures paired off/on with the cut beside value parity",
		}),
		"semi_write_insert" => serde_json::json!({
			"status": "not_removable",
			"successor": "split insert into cache hits, hash and probe, growth",
		}),
		_ => serde_json::json!({
			"status": "unpriced",
			"successor": "name the subterm redundant with scan or summary work",
		}),
	}
}

#[allow(clippy::too_many_arguments)]
fn run_adr585_child(
	run_index: u32,
	warmup: bool,
	on: bool,
	mode: &str,
	sf: f64,
	data_source: &str,
	data_dir: &Path,
	output_dir: &Path,
) -> Result<serde_json::Value, String> {
	let mut args = vec![
		"perf-run-one".to_string(),
		"--engine".to_string(),
		"toron".to_string(),
		"--query".to_string(),
		"q21".to_string(),
		"--mode".to_string(),
		mode.to_string(),
		"--sf".to_string(),
		sf.to_string(),
		"--run-index".to_string(),
		run_index.to_string(),
		"--data-source".to_string(),
		data_source.to_string(),
		"--data-dir".to_string(),
		data_dir.display().to_string(),
	];
	if warmup {
		args.push("--warmup-run".to_string());
	}
	let profile = output_dir
		.join("profiles")
		.join(format!("pair-{run_index:02}-on.json"));
	let profile_text = profile
		.to_str()
		.ok_or_else(|| "ADR 585 profile path is not UTF-8".to_string())?;
	let row = if on {
		run_perf_child_args_with_envs(
			&args,
			&[("TORON_NODE_ATTRIBUTION_PROFILE_PATH", profile_text)],
		)?
	} else {
		run_perf_child_args(&args)?
	};
	// ADR 592: a non-passing row is returned as it is; the shared validator refuses it and the
	// driver journals the refusal beside the row before stopping.
	Ok(row)
}

fn receipt_u64(value: &serde_json::Value, field: &str) -> Result<u64, String> {
	value
		.get(field)
		.and_then(serde_json::Value::as_u64)
		.ok_or_else(|| format!("lifecycle receipt lacks integer {field}"))
}

fn lifecycle_target_receipt<'a>(
	row: &'a serde_json::Value,
	pair: usize,
	label: &str,
) -> Result<(&'a serde_json::Value, &'a serde_json::Value), String> {
	let profile = row
		.get("node_attribution")
		.ok_or_else(|| format!("{label} pair {pair} lacks node attribution"))?;
	let matches = profile
		.get("aggregate_invocations")
		.and_then(serde_json::Value::as_array)
		.ok_or_else(|| format!("{label} pair {pair} lacks aggregate invocations"))?
		.iter()
		.filter(|invocation| {
			invocation.get("route").and_then(serde_json::Value::as_str)
				== Some("inner_prefix_semi_anti_grouped")
		})
		.collect::<Vec<_>>();
	if matches.len() != 1 {
		return Err(format!(
			"{label} pair {pair} requires exactly one inner_prefix_semi_anti_grouped receipt, found {}",
			matches.len()
		));
	}
	let invocation = matches[0];
	if invocation
		.pointer("/bridge/passed")
		.and_then(serde_json::Value::as_bool)
		!= Some(true)
		|| invocation
			.pointer("/stages/passed")
			.and_then(serde_json::Value::as_bool)
			!= Some(true)
	{
		return Err(format!(
			"{label} pair {pair} target invocation failed ADR 507's exclusive reconciliation"
		));
	}
	let receipt = invocation
		.get("inclusive_lifecycle_receipt")
		.filter(|value| !value.is_null())
		.ok_or_else(|| format!("{label} pair {pair} target invocation lacks lifecycle receipt"))?;
	if receipt.get("status").and_then(serde_json::Value::as_str) != Some("reconciled")
		|| receipt.get("passed").and_then(serde_json::Value::as_bool) != Some(true)
	{
		return Err(format!(
			"{label} pair {pair} lifecycle receipt is not reconciled: {receipt}"
		));
	}
	if receipt
		.get("invocation_id")
		.and_then(serde_json::Value::as_u64)
		!= invocation
			.get("invocation_id")
			.and_then(serde_json::Value::as_u64)
	{
		return Err(format!(
			"{label} pair {pair} lifecycle receipt is not bound to its invocation"
		));
	}
	Ok((profile, receipt))
}

fn evaluate_adr585_pairs(rows: &[serde_json::Value]) -> Result<serde_json::Value, String> {
	evaluate_lifecycle_pairs(rows, &ADR585_TAGS)
}

/// The row tags one paired lifecycle measurement stamps on its journal rows. ADR 585 and ADR 586
/// share the three-band reconciliation and the thirteen-term rule; only the tags and the label in
/// their refusals differ.
struct LifecyclePairTags {
	label: &'static str,
	arm: &'static str,
	pair: &'static str,
	position: &'static str,
}

const ADR585_TAGS: LifecyclePairTags = LifecyclePairTags {
	label: "ADR 585",
	arm: "adr585_arm",
	pair: "adr585_pair",
	position: "adr585_pair_position",
};

const ADR586_TAGS: LifecyclePairTags = LifecyclePairTags {
	label: "ADR 586",
	arm: "adr586_arm",
	pair: "adr586_pair",
	position: "adr586_pair_position",
};

fn evaluate_lifecycle_pairs(
	rows: &[serde_json::Value],
	tags: &LifecyclePairTags,
) -> Result<serde_json::Value, String> {
	let label = tags.label;
	if rows.len() != ADR585_PAIRS * 2 {
		return Err(format!(
			"{label} requires {} measured rows, found {}",
			ADR585_PAIRS * 2,
			rows.len()
		));
	}
	let first = rows.first().ok_or_else(|| format!("{label} has no rows"))?;
	for row in rows {
		let identity = toron_tpch::evidence::compare_row_identities(first, row);
		if !identity.would_admit {
			return Err(format!(
				"{label} arms differ in identity: {:?}",
				identity.reasons
			));
		}
		if row.get("query").and_then(serde_json::Value::as_str) != Some("q21")
			|| row.get("status").and_then(serde_json::Value::as_str) != Some("pass")
		{
			return Err(format!("{label} accepts only passing q21 rows"));
		}
	}
	let mut movements = Vec::new();
	let mut off_execute = Vec::new();
	let mut on_execute = Vec::new();
	let mut profile_paths = BTreeSet::new();
	let mut pair_records = Vec::new();
	let mut term_samples = ADR585_TERM_FIELDS
		.iter()
		.map(|(name, _)| ((*name).to_string(), Vec::<f64>::new()))
		.collect::<BTreeMap<_, _>>();
	let mut drive_samples = Vec::new();
	let mut non_drive_samples = Vec::new();
	let mut removable_samples = Vec::new();

	for pair in 0..ADR585_PAIRS {
		let pair_rows = rows
			.iter()
			.filter(|row| {
				row.get(tags.pair).and_then(serde_json::Value::as_u64) == Some(pair as u64)
			})
			.collect::<Vec<_>>();
		let off = pair_rows
			.iter()
			.find(|row| row.get(tags.arm).and_then(serde_json::Value::as_str) == Some("off"))
			.ok_or_else(|| format!("{label} pair {pair} lacks off arm"))?;
		let on = pair_rows
			.iter()
			.find(|row| row.get(tags.arm).and_then(serde_json::Value::as_str) == Some("on"))
			.ok_or_else(|| format!("{label} pair {pair} lacks on arm"))?;
		let expected_order = if pair % 2 == 0 {
			[("off", 0), ("on", 1)]
		} else {
			[("on", 0), ("off", 1)]
		};
		for (arm, position) in expected_order {
			let observed = pair_rows
				.iter()
				.find(|row| row.get(tags.arm).and_then(serde_json::Value::as_str) == Some(arm));
			if observed
				.and_then(|row| row.get(tags.position))
				.and_then(serde_json::Value::as_u64)
				!= Some(position)
			{
				return Err(format!(
					"{label} pair {pair} does not use its declared alternating order"
				));
			}
		}
		if off
			.get("diagnostics_disabled")
			.and_then(serde_json::Value::as_bool)
			!= Some(true)
			|| off.get("node_attribution").is_some()
			|| off.get("node_attribution_profile_receipt_path").is_some()
		{
			return Err(format!(
				"{label} pair {pair} off arm is not diagnostic-free"
			));
		}
		if on
			.get("diagnostics_disabled")
			.and_then(serde_json::Value::as_bool)
			!= Some(false)
		{
			return Err(format!(
				"{label} pair {pair} on arm did not void diagnostic authority"
			));
		}
		let path = on
			.get("node_attribution_profile_receipt_path")
			.and_then(serde_json::Value::as_str)
			.ok_or_else(|| format!("{label} pair {pair} lacks node-profile receipt path"))?;
		if !profile_paths.insert(path.to_string()) {
			return Err(format!(
				"{label} pair {pair} reuses node-profile receipt path {path:?}"
			));
		}
		let execute_off_ms = required_f64(off, "execute_ms")?;
		let execute_on_ms = required_f64(on, "execute_ms")?;
		if execute_off_ms <= 0.0 || execute_on_ms <= 0.0 {
			return Err(format!("{label} pair {pair} has non-positive execute time"));
		}
		let execute_on_micros = (execute_on_ms * 1000.0).round() as u64;
		let movement = (execute_on_ms - execute_off_ms) / execute_off_ms;
		movements.push(movement.abs());
		off_execute.push(execute_off_ms);
		on_execute.push(execute_on_ms);
		let (profile, receipt) = lifecycle_target_receipt(on, pair, label)?;
		let root_span = receipt_u64(profile, "execute_bound_span_micros")?;
		let outer = on
			.get("node_attribution_outer_reconciliation")
			.ok_or_else(|| format!("{label} pair {pair} lacks outer reconciliation"))?;
		let outer_residual = receipt_u64(outer, "outer_residual_micros")?;
		let outer_complete = outer
			.get("within_boundary")
			.and_then(serde_json::Value::as_bool)
			== Some(true);
		if root_span.checked_add(outer_residual) != Some(execute_on_micros) {
			return Err(format!(
				"{label} pair {pair} outer algebra does not reconcile"
			));
		}
		let handler = receipt_u64(receipt, "handler_inclusive_micros")?;
		let outside_handler = receipt_u64(receipt, "outside_handler_span_micros")?;
		if handler.checked_add(outside_handler) != Some(root_span) {
			return Err(format!(
				"{label} pair {pair} handler algebra does not reconcile"
			));
		}
		let runtime = receipt_u64(receipt, "runtime_prep_inclusive_micros")?;
		let runtime_residual = receipt_u64(receipt, "runtime_prep_residual_micros")?;
		for field in [
			"runtime_prep_semi_read_micros",
			"runtime_prep_semi_write_micros",
			"runtime_prep_anti_read_micros",
			"runtime_prep_anti_write_micros",
		] {
			if receipt_u64(receipt, field)? == 0 {
				return Err(format!(
					"{label} pair {pair} requires non-zero multi-batch summary term {field}"
				));
			}
		}
		let runtime_children = [
			"runtime_prep_inner_1_micros",
			"runtime_prep_inner_2_micros",
			"runtime_prep_semi_read_micros",
			"runtime_prep_semi_write_micros",
			"runtime_prep_semi_finish_micros",
			"runtime_prep_anti_read_micros",
			"runtime_prep_anti_write_micros",
			"runtime_prep_anti_finish_micros",
		]
		.into_iter()
		.try_fold(0u64, |total, field| {
			receipt_u64(receipt, field).and_then(|value| {
				total
					.checked_add(value)
					.ok_or_else(|| format!("{label} runtime child sum overflowed"))
			})
		})?;
		if runtime_children.checked_add(runtime_residual) != Some(runtime) {
			return Err(format!(
				"{label} pair {pair} runtime-prep algebra does not reconcile"
			));
		}
		let base = receipt_u64(receipt, "base_prep_inclusive_micros")?;
		let drive = receipt_u64(receipt, "drive_inclusive_micros")?;
		let finalize = receipt_u64(receipt, "finalize_inclusive_micros")?;
		let cleanup = receipt_u64(receipt, "cleanup_inclusive_micros")?;
		let lifecycle_residual = receipt_u64(receipt, "lifecycle_residual_micros")?;
		let lifecycle_sum = [runtime, base, drive, finalize, cleanup, lifecycle_residual]
			.into_iter()
			.try_fold(0u64, |total, value| total.checked_add(value))
			.ok_or_else(|| format!("{label} lifecycle sum overflowed"))?;
		if lifecycle_sum != handler {
			return Err(format!(
				"{label} pair {pair} lifecycle algebra does not reconcile"
			));
		}
		let overhead = (execute_on_ms - execute_off_ms).max(0.0) * 1000.0;
		let residual_bound = overhead + 0.05 * handler as f64;
		let residuals_complete = runtime_residual as f64 <= residual_bound
			&& lifecycle_residual as f64 <= residual_bound;
		let profile_complete = outer_complete && residuals_complete;
		for (name, field) in ADR585_TERM_FIELDS {
			let micros = if field == "outer_residual_micros" {
				outer_residual
			} else {
				receipt_u64(receipt, field)?
			};
			term_samples
				.get_mut(name)
				.expect("term universe initialized")
				.push(micros as f64 / execute_on_micros as f64 * execute_off_ms);
		}
		drive_samples.push(drive as f64 / execute_on_micros as f64 * execute_off_ms);
		non_drive_samples
			.push((execute_on_micros - drive) as f64 / execute_on_micros as f64 * execute_off_ms);
		let removable = receipt_u64(receipt, "runtime_prep_semi_read_micros")?
			.min(receipt_u64(receipt, "runtime_prep_anti_read_micros")?);
		removable_samples.push(removable as f64 / execute_on_micros as f64 * execute_off_ms);
		pair_records.push(serde_json::json!({
			"pair": pair,
			"execute_off_ms": execute_off_ms,
			"execute_on_ms": execute_on_ms,
			"movement": movement,
			"overhead_micros": overhead,
			"root_span_micros": root_span,
			"outer_residual_micros": outer_residual,
			"handler_inclusive_micros": handler,
			"outside_handler_span_micros": outside_handler,
			"runtime_prep_inclusive_micros": runtime,
			"runtime_prep_inner_1_micros": receipt_u64(receipt, "runtime_prep_inner_1_micros")?,
			"runtime_prep_inner_2_micros": receipt_u64(receipt, "runtime_prep_inner_2_micros")?,
			"runtime_prep_semi_read_micros": receipt_u64(receipt, "runtime_prep_semi_read_micros")?,
			"runtime_prep_semi_write_micros": receipt_u64(receipt, "runtime_prep_semi_write_micros")?,
			"runtime_prep_anti_read_micros": receipt_u64(receipt, "runtime_prep_anti_read_micros")?,
			"runtime_prep_anti_write_micros": receipt_u64(receipt, "runtime_prep_anti_write_micros")?,
			"runtime_prep_semi_finish_micros": receipt_u64(receipt, "runtime_prep_semi_finish_micros")?,
			"runtime_prep_anti_finish_micros": receipt_u64(receipt, "runtime_prep_anti_finish_micros")?,
			"runtime_prep_residual_micros": runtime_residual,
			"base_prep_inclusive_micros": base,
			"drive_inclusive_micros": drive,
			"finalize_inclusive_micros": finalize,
			"cleanup_inclusive_micros": cleanup,
			"lifecycle_residual_micros": lifecycle_residual,
			"residual_bound_micros": residual_bound,
			"profile_complete": profile_complete,
			"profile_receipt_path": path,
		}));
	}
	let median_abs_movement = median(&mut movements);
	let perturbation_accepted = median_abs_movement <= 0.25;
	let all_profiles_complete = pair_records
		.iter()
		.all(|pair| pair["profile_complete"] == true);
	let off_min = off_execute.iter().copied().fold(f64::INFINITY, f64::min);
	let off_max = off_execute
		.iter()
		.copied()
		.fold(f64::NEG_INFINITY, f64::max);
	let off_range = off_max - off_min;
	let off_median = median(&mut off_execute);
	let on_median = median(&mut on_execute);
	let owner_floor = 500.0f64.max(2.0 * off_range);
	let mut medians = term_samples
		.into_iter()
		.map(|(name, mut values)| (name, median(&mut values)))
		.collect::<Vec<_>>();
	medians.sort_by(|left, right| right.1.total_cmp(&left.1));
	let candidates = medians
		.iter()
		.filter(|(_, value)| *value > owner_floor)
		.map(|(name, value)| {
			serde_json::json!({
				"term": name,
				"median_scaled_ms": value,
				"share_of_off_execute": value / off_median,
			})
		})
		.collect::<Vec<_>>();
	let residual_name = |name: &str| name.contains("residual");
	let owner = if !perturbation_accepted {
		serde_json::json!({"status": "withheld_perturbation_rejected"})
	} else if !all_profiles_complete {
		serde_json::json!({"status": "withheld_incomplete_profiles"})
	} else {
		match (medians.first(), medians.get(1)) {
			(Some((name, value)), _) if residual_name(name) && *value > owner_floor => {
				serde_json::json!({
					"status": "withheld_instrument_incomplete",
					"leading_residual": name,
					"median_scaled_ms": value,
				})
			}
			(Some((name, value)), Some((_, second)))
				if *value > owner_floor && value - second > off_range =>
			{
				serde_json::json!({
					"status": "resolved",
					"term": name,
					"median_scaled_ms": value,
					"share_of_off_execute": value / off_median,
				})
			}
			(Some((name, value)), _) if *value > owner_floor => serde_json::json!({
				"status": "unresolved_tie_within_off_range",
				"leading_term": name,
				"median_scaled_ms": value,
			}),
			_ => serde_json::json!({"status": "no_term_clears_floor"}),
		}
	};
	let removable_median = median(&mut removable_samples);
	Ok(serde_json::json!({
		"pair_count": ADR585_PAIRS,
		"pairs": pair_records,
		"median_absolute_movement": median_abs_movement,
		"perturbation_gate": if perturbation_accepted { "accepted" } else { "rejected" },
		"all_profiles_complete": all_profiles_complete,
		"off_execute_median_ms": off_median,
		"on_execute_median_ms": on_median,
		"off_execute_range_ms": off_range,
		"owner_floor_ms": owner_floor,
		"term_medians_scaled_ms": medians.into_iter().map(|(term, value)| serde_json::json!({"term": term, "median_scaled_ms": value})).collect::<Vec<_>>(),
		"candidate_terms": candidates,
		"drive_inclusive_median_scaled_ms": median(&mut drive_samples),
		"non_drive_median_scaled_ms": median(&mut non_drive_samples),
		"shared_source_removable_read_median_scaled_ms": removable_median,
		"shared_source_candidate": if removable_median > owner_floor { "opens" } else { "closed_below_floor" },
		"owner": owner,
	}))
}

fn add_partition_work_concurrency_fields(
	row: &mut serde_json::Value,
	setting: Option<(u64, &'static str)>,
) {
	row["partition_work_concurrency_requested"] = serde_json::json!(setting.map(|value| value.0));
	row["partition_work_concurrency_requested_provenance"] =
		serde_json::json!(setting.map(|value| value.1));
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PerfEngine {
	Toron,
	Rune,
	DuckDb,
	PolarsSql,
}

impl PerfEngine {
	fn as_str(self) -> &'static str {
		match self {
			PerfEngine::Toron => "toron",
			PerfEngine::Rune => "rune",
			PerfEngine::DuckDb => "duckdb",
			PerfEngine::PolarsSql => "polars_sql",
		}
	}
}

fn parse_perf_engine(raw: &str) -> Result<PerfEngine, String> {
	match raw.trim() {
		"toron" => Ok(PerfEngine::Toron),
		"rune" => Ok(PerfEngine::Rune),
		"duckdb" => Ok(PerfEngine::DuckDb),
		"polars_sql" => Ok(PerfEngine::PolarsSql),
		other => Err(format!("unsupported perf engine {other:?}")),
	}
}

fn parse_perf_engines(raw: &str) -> Result<Vec<PerfEngine>, String> {
	let mut engines = Vec::new();
	for item in raw.split(',') {
		let engine = match item.trim() {
			"" => continue,
			"toron" => PerfEngine::Toron,
			"rune" => PerfEngine::Rune,
			"duckdb" => PerfEngine::DuckDb,
			"polars_sql" => PerfEngine::PolarsSql,
			"all" => {
				push_perf_engine(&mut engines, PerfEngine::Toron);
				push_perf_engine(&mut engines, PerfEngine::DuckDb);
				continue;
			}
			other => {
				return Err(format!(
					"perf --engines contains unsupported engine {other:?}"
				));
			}
		};
		push_perf_engine(&mut engines, engine);
	}
	if engines.is_empty() {
		return Err("perf --engines must include at least one engine".to_string());
	}
	Ok(engines)
}

fn push_perf_engine(engines: &mut Vec<PerfEngine>, engine: PerfEngine) {
	if !engines.contains(&engine) {
		engines.push(engine);
	}
}

fn data_dir_from_args(args: &[String]) -> PathBuf {
	flag_value(args, "--data-dir")
		.map(PathBuf::from)
		.unwrap_or_else(|| PathBuf::from(CACHE_ROOT).join("data"))
}

async fn q03_plan_proof_command(args: &[String]) -> ExitCode {
	let mode = flag_value(args, "--mode").unwrap_or("strict");
	let data_source = flag_value(args, "--data-source").unwrap_or("memory");
	let data_dir = data_dir_from_args(args);
	let sf = match explicit_sf(args) {
		Ok(explicit) => explicit.unwrap_or(DEFAULT_SF),
		Err(message) => return harness_error(&message),
	};
	let proof =
		match rune_q03_isolation_proof_with_data_source(mode, sf, data_source, &data_dir).await {
			Ok(proof) => proof,
			Err(message) => return harness_error(&message),
		};
	let row = serde_json::json!({
		"suite": "tpch_q03_plan_proof",
		"mode": mode,
		"scale_factor": sf,
		"data_source": proof.data_source,
		"manifest_fingerprint": proof.manifest_fingerprint,
		"sql_plan_fingerprint": proof.sql_plan_fingerprint,
		"rune_plan_fingerprint": proof.rune_plan_fingerprint,
		"plans_differ": proof.plans_differ(),
		"sql_compact_shape": proof.sql_compact_shape,
		"rune_compact_shape": proof.rune_compact_shape,
		"sql_plan_shape": proof.sql_plan_shape,
		"rune_plan_shape": proof.rune_plan_shape,
		"optimizer_table_facts": proof.optimizer_table_facts,
		"optimizer_evidence_gap": proof.optimizer_evidence_gap,
		"sql_join_decisions": proof.sql_join_decisions,
		"rune_join_decisions": proof.rune_join_decisions,
		"sql_join_order_edges": proof.sql_join_order_edges,
		"rune_join_order_edges": proof.rune_join_order_edges,
	});
	println!("{row}");
	ExitCode::SUCCESS
}

async fn perf_command(args: &[String]) -> ExitCode {
	let query = flag_value(args, "--query").unwrap_or("all");
	let mode = flag_value(args, "--mode").unwrap_or("strict");
	let data_source = flag_value(args, "--data-source").unwrap_or("memory");
	let data_dir = data_dir_from_args(args);
	let explicit_scale = match explicit_sf(args) {
		Ok(explicit) => explicit,
		Err(message) => return harness_error(&message),
	};
	let runs = match parse_usize_flag(args, "--runs", 7) {
		Ok(runs) if runs > 0 => runs,
		Ok(_) => return harness_error("--runs must be greater than zero"),
		Err(message) => return harness_error(&message),
	};
	let warmup = match parse_usize_flag(args, "--warmup", 1) {
		Ok(warmup) => warmup,
		Err(message) => return harness_error(&message),
	};
	let output_dir = PathBuf::from(flag_value(args, "--output-dir").unwrap_or("target/tpch/perf"));
	let sf = match if flag_present(args, "--paired-ablation") {
		selected_scale_or_default(explicit_scale, &output_dir)
	} else {
		Ok(explicit_scale.unwrap_or(1.0))
	} {
		Ok(sf) => sf,
		Err(message) => return harness_error(&message),
	};
	let baseline_path = flag_value(args, "--baseline")
		.map(PathBuf::from)
		.unwrap_or_else(|| output_dir.join(default_baseline_name(sf, mode)));
	let engines = match parse_perf_engines(flag_value(args, "--engines").unwrap_or("toron")) {
		Ok(engines) => engines,
		Err(message) => return harness_error(&message),
	};
	let queries = match perf_query_list(query) {
		Ok(queries) => queries,
		Err(message) => return harness_error(&message),
	};
	// ADR 603: a named DuckDB memory limit that cannot be honoured is refused before any query.
	if let Err(message) = toron_tpch::duckdb_memory::memory_limit_coordinate() {
		return harness_error(&message);
	}
	let board_context =
		match PerfBoardRunContext::prepare(&output_dir, mode, sf, data_source, &data_dir, &engines)
		{
			Ok(context) => context,
			Err(message) => return harness_error(&message),
		};
	let paired_switch = paired_switch_from_args(args);
	let paired_gate = match paired_gate_from_args(args) {
		Ok(gate) => gate,
		Err(message) => return harness_error(&message),
	};
	let paired_materiality_floor_ms =
		match optional_positive_f64_flag(args, "--paired-materiality-floor-ms") {
			Ok(value) => value,
			Err(message) => return harness_error(&message),
		};
	if flag_present(args, "--paired-ablation")
		&& paired_gate == PairedGate::ShapePreservingSpillIndex
		&& paired_materiality_floor_ms.is_none()
	{
		return harness_error(
			"--paired-gate shape-preserving-spill-index requires --paired-materiality-floor-ms",
		);
	}
	if flag_present(args, "--paired-ablation")
		&& paired_gate == PairedGate::ShapePreservingSpillIndex
	{
		if queries != vec!["q17"]
			|| mode != "strict"
			|| sf != 8.0
			|| !matches!(data_source, "parquet" | "persisted")
			|| paired_switch.env != PARTITIONED_SCAN_BUILD_INDEX_AT_FLUSH_ENV
			|| paired_switch.on_value != "1"
			|| paired_switch.off_value != "0"
			|| paired_materiality_floor_ms != Some(3_554.0)
			|| runs != 10
		{
			return harness_error(
				"shape-preserving-spill-index requires q17 strict sf=8 persisted data, 10 pairs, \
				 TORON_PARTITIONED_SCAN_BUILD_INDEX_AT_FLUSH=1/0, and the fixed 3554 ms floor",
			);
		}
		let (memory_per_task, memory_provenance) = match gauge_memory_per_task() {
			Ok(value) => value,
			Err(message) => return harness_error(&message),
		};
		let (spill_bytes_reserved, spill_provenance) = match gauge_spill_bytes_reserved() {
			Ok(value) => value,
			Err(message) => return harness_error(&message),
		};
		if memory_per_task != 2_147_483_648
			|| spill_bytes_reserved != 2_147_483_648
			|| memory_provenance != "explicit"
			|| spill_provenance != "explicit"
		{
			return harness_error(
				"shape-preserving-spill-index requires explicit 2 GiB memory and spill budgets",
			);
		}
	}
	// Existing paired rails define `--runs` as child rows. ADR 510's named gate defines it as
	// adjacent pairs so the approved `--runs 10` command durably emits 20 measured children.
	let measured_runs = if flag_present(args, "--paired-ablation")
		&& paired_gate == PairedGate::ShapePreservingSpillIndex
	{
		runs.saturating_mul(2)
	} else {
		runs
	};
	let gate_zero = if flag_present(args, "--paired-ablation") {
		let default_switch = paired_switch.env == PAIRED_DEFAULT_ENV;
		if default_switch && queries != vec!["q05"] {
			return harness_error("--paired-ablation currently requires --query q05");
		}
		if !default_switch && queries.len() != 1 {
			return harness_error("--paired-ablation with --paired-env requires a single query");
		}
		// The gate's whole claim is that both arms saw identical bytes, so it admits only a full-hash
		// load. Under the skip env the load still succeeds; its fingerprint just proves manifest
		// self-consistency, which is why the admission asks the dataset what was actually proved.
		let dataset_fingerprint = match load_persisted_tpch_dataset(mode, sf, &data_dir)
			.map_err(|message| {
				format!(
					"paired ablation requires a full-hash verified persisted dataset: {message}"
				)
			})
			.and_then(|dataset| paired_ablation_dataset_fingerprint(&dataset))
		{
			Ok(fingerprint) => fingerprint,
			Err(message) => return harness_error(&message),
		};
		let gate = if default_switch {
			run_adr449_gate_zero(sf, mode, data_source, &data_dir, &output_dir)
		} else {
			match paired_gate {
				PairedGate::PlanDistinct => run_adr469_plan_gate(
					queries[0],
					mode,
					sf,
					data_source,
					&data_dir,
					&paired_switch,
				),
				PairedGate::ShapePreservingSpillIndex => run_shape_preserving_spill_index_gate(
					queries[0],
					mode,
					sf,
					data_source,
					&data_dir,
					&output_dir,
					&paired_switch,
				),
				PairedGate::RouteDistinct => run_route_distinct_gate(
					queries[0],
					mode,
					sf,
					data_source,
					&data_dir,
					&output_dir,
					&paired_switch,
				),
			}
		};
		match gate {
			Ok(mut record) if record["passed"] == true => {
				record["dataset_fingerprint"] = serde_json::json!(dataset_fingerprint);
				Some(record)
			}
			Ok(record) => {
				return harness_error(&format!("paired ablation gate failed: {record}"));
			}
			Err(message) => return harness_error(&message),
		}
	} else {
		None
	};
	let sample_journal = if flag_present(args, "--paired-ablation") {
		if measured_runs % 2 != 0 {
			return harness_error("--paired-ablation requires an even --runs count");
		}
		let gate_fingerprint = gate_zero
			.as_ref()
			.and_then(|record| record.get("dataset_fingerprint"))
			.and_then(serde_json::Value::as_str)
			.unwrap_or_default()
			.to_string();
		let budget = match GaugeBudget::from_environment() {
			Ok(budget) => budget,
			Err(message) => return harness_error(&message),
		};
		match prepare_paired_ablation_journal(
			&output_dir,
			query,
			mode,
			sf,
			data_source,
			&data_dir,
			measured_runs,
			warmup,
			&gate_fingerprint,
			&paired_switch,
			paired_gate,
			paired_materiality_floor_ms,
			budget,
		) {
			Ok(journal) => Some(journal),
			Err(message) => return harness_error(&message),
		}
	} else {
		// Un-paired runs get the same durability without the paired manifest's config lock. They
		// start clean rather than resuming: replaying rows across runs is only safe behind the lock
		// that gate zero establishes, and a silently-mixed board is worse than a re-run.
		match prepare_perf_sample_journal(&output_dir) {
			Ok(journal) => Some(journal),
			Err(message) => return harness_error(&message),
		}
	};
	let mut raw_rows = sample_journal
		.as_ref()
		.map(|journal| journal.rows.clone())
		.unwrap_or_default();
	for query_id in queries {
		// A query that cannot run at this scale is out of scope for the board, not a failure of it:
		// q11's HAVING threshold is scale-specific, so unsupported scales are recorded as cells
		// and keep going — aborting here would discard every query already measured.
		if let Err(reason) = validate_query_scale(query_id, sf) {
			for engine in &engines {
				raw_rows.push(perf_stub_row(PerfStubRow {
					engine: engine.as_str(),
					query: query_id,
					mode,
					scale_factor: sf,
					status: Status::NotAttempted,
					reason: "unsupported_scale",
					message: Some(reason.clone()),
				}));
			}
			continue;
		}

		// Each cell's children are run inside a labelled block so a child that dies is recorded as a
		// failed row and the sweep continues. The exit code below still reflects it, so a contained
		// failure is an honest red cell rather than a silently-missing one.
		let cell_outcome: Result<Vec<serde_json::Value>, String> = 'cell: {
			let mut cell_rows: Vec<serde_json::Value> = Vec::new();
			for engine in &engines {
				match engine {
					PerfEngine::Toron => {
						if flag_present(args, "--paired-ablation") {
							if let Err(message) = require_adr448_harness_capability() {
								break 'cell Err(message);
							}
						}
						for warmup_index in 0..warmup {
							if sample_journal.as_ref().is_some_and(|journal| {
								journal.contains(
									query_id,
									engine.as_str(),
									warmup_index as u32,
									true,
								)
							}) {
								continue;
							}
							match run_perf_child_with_ablation(
								PerfEngine::Toron,
								query_id,
								mode,
								sf,
								warmup_index as u32,
								true,
								data_source,
								&data_dir,
								if flag_present(args, "--paired-ablation") {
									Some((&paired_switch, false))
								} else {
									None
								},
							) {
								Ok(row) => {
									if let Some(journal) = &sample_journal {
										if let Err(message) = journal.append(&row) {
											break 'cell Err(message);
										}
									}
									cell_rows.push(row);
								}
								Err(message) => break 'cell Err(message),
							}
						}
						for run_index in 0..measured_runs {
							if sample_journal.as_ref().is_some_and(|journal| {
								journal.contains(query_id, engine.as_str(), run_index as u32, false)
							}) {
								continue;
							}
							match run_perf_child_with_ablation(
								PerfEngine::Toron,
								query_id,
								mode,
								sf,
								run_index as u32,
								false,
								data_source,
								&data_dir,
								if flag_present(args, "--paired-ablation") {
									Some((&paired_switch, paired_ablation_arm(run_index as u32)))
								} else {
									None
								},
							) {
								Ok(row) => {
									if let Some(journal) = &sample_journal {
										if let Err(message) = journal.append(&row) {
											break 'cell Err(message);
										}
									}
									cell_rows.push(row);
								}
								Err(message) => break 'cell Err(message),
							}
						}
					}
					PerfEngine::Rune => {
						for warmup_index in 0..warmup {
							match run_perf_child(
								PerfEngine::Rune,
								query_id,
								mode,
								sf,
								warmup_index as u32,
								true,
								data_source,
								&data_dir,
							) {
								Ok(row) => {
									if let Err(message) =
										journal_and_collect(&sample_journal, &mut cell_rows, row)
									{
										break 'cell Err(message);
									}
								}
								Err(message) => break 'cell Err(message),
							}
						}
						for run_index in 0..measured_runs {
							match run_perf_child(
								PerfEngine::Rune,
								query_id,
								mode,
								sf,
								run_index as u32,
								false,
								data_source,
								&data_dir,
							) {
								Ok(row) => {
									if let Err(message) =
										journal_and_collect(&sample_journal, &mut cell_rows, row)
									{
										break 'cell Err(message);
									}
								}
								Err(message) => break 'cell Err(message),
							}
						}
					}
					PerfEngine::DuckDb => {
						match run_duckdb_perf_rows(
							query_id,
							mode,
							sf,
							measured_runs,
							warmup,
							&output_dir,
							data_source,
							&data_dir,
						) {
							Ok(rows) => {
								for row in rows {
									if let Err(message) =
										journal_and_collect(&sample_journal, &mut cell_rows, row)
									{
										break 'cell Err(message);
									}
								}
							}
							Err(message) => break 'cell Err(message),
						}
					}
					PerfEngine::PolarsSql => {
						for warmup_index in 0..warmup {
							match run_perf_child(
								PerfEngine::PolarsSql,
								query_id,
								mode,
								sf,
								warmup_index as u32,
								true,
								data_source,
								&data_dir,
							) {
								Ok(row) => {
									if let Err(message) =
										journal_and_collect(&sample_journal, &mut cell_rows, row)
									{
										break 'cell Err(message);
									}
								}
								Err(message) => break 'cell Err(message),
							}
						}
						for run_index in 0..measured_runs {
							match run_perf_child(
								PerfEngine::PolarsSql,
								query_id,
								mode,
								sf,
								run_index as u32,
								false,
								data_source,
								&data_dir,
							) {
								Ok(row) => {
									if let Err(message) =
										journal_and_collect(&sample_journal, &mut cell_rows, row)
									{
										break 'cell Err(message);
									}
								}
								Err(message) => break 'cell Err(message),
							}
						}
					}
				}
			}
			Ok(cell_rows)
		};
		match cell_outcome {
			Ok(rows) => raw_rows.extend(rows),
			Err(message) => {
				eprintln!("perf cell {query_id} failed: {message}");
				for engine in &engines {
					raw_rows.push(perf_stub_row(PerfStubRow {
						engine: engine.as_str(),
						query: query_id,
						mode,
						scale_factor: sf,
						status: Status::Fail,
						reason: "perf_child_failed",
						message: Some(message.clone()),
					}));
				}
			}
		}
	}

	apply_duckdb_output_sanity_flags(&mut raw_rows);
	board_context.annotate_samples(&mut raw_rows);
	let baseline_rows = read_baseline_rows(&baseline_path);
	let summary_rows = summarize_perf_rows(&raw_rows, measured_runs, warmup, &baseline_rows);
	let summary_rows = if let Some(gate_zero) = gate_zero {
		match attach_paired_ablation_receipt(
			summary_rows,
			measured_runs,
			gate_zero,
			paired_materiality_floor_ms,
		) {
			Ok(rows) => rows,
			Err(message) => return harness_error(&message),
		}
	} else {
		summary_rows
	};
	if let Err(error) = write_perf_outputs(&output_dir, &baseline_path, &raw_rows, &summary_rows) {
		return harness_error(&format!("write perf output: {error}"));
	}
	if let Err(error) = board_context.finish(&output_dir, &raw_rows, &summary_rows) {
		return harness_error(&format!("write perf board evidence: {error}"));
	}

	for row in &summary_rows {
		print_perf_summary(row);
	}
	// Containing a failed cell keeps the rest of the board; it must not also make the board look
	// clean. `not_attempted` (out-of-scope at this scale) is not a failure — `fail` is.
	let failed = summary_rows
		.iter()
		.filter(|row| row.get("status").and_then(serde_json::Value::as_str) == Some("fail"))
		.filter_map(|row| row.get("query").and_then(serde_json::Value::as_str))
		.collect::<Vec<_>>();
	if failed.is_empty() {
		ExitCode::SUCCESS
	} else {
		eprintln!("perf completed with failed cells: {}", failed.join(", "));
		ExitCode::FAILURE
	}
}

#[derive(Debug)]
struct PerfBoardRunContext {
	mode: String,
	scale_factor: f64,
	data_source: String,
	output_directory_fresh: bool,
	host_start: toron_tpch::HostQuiescence,
	join_spill_scratch: PathBuf,
	join_spill_scratch_empty_before: bool,
	dataset_verification: Option<String>,
	dataset_manifest_fingerprint: Option<String>,
	budget: GaugeBudget,
	partition_work_concurrency_requested: (u64, &'static str),
	duckdb_version: Option<String>,
	duckdb_executable_sha256: Option<String>,
}

impl PerfBoardRunContext {
	fn prepare(
		output_dir: &Path,
		mode: &str,
		sf: f64,
		data_source: &str,
		data_dir: &Path,
		engines: &[PerfEngine],
	) -> Result<Self, String> {
		let output_directory_fresh = !output_dir.exists();
		let host_start = observe_host_quiescence();
		let join_spill_scratch = std::env::var_os("TORON_JOIN_SPILL_DIR")
			.filter(|value| !value.is_empty())
			.map(PathBuf::from)
			.unwrap_or_else(|| std::env::temp_dir().join("toron_join_spill"));
		let join_spill_scratch_empty_before = directory_is_empty(&join_spill_scratch)?;
		let budget = GaugeBudget::from_environment()?;
		let partition_work_concurrency_requested = gauge_partition_work_concurrency_requested()?;
		let (dataset_verification, dataset_manifest_fingerprint) =
			if matches!(data_source, "parquet" | "persisted") {
				let dataset = load_persisted_tpch_dataset(mode, sf, data_dir)?;
				(
					Some(dataset.verification.as_str().to_string()),
					Some(dataset.manifest_fingerprint),
				)
			} else {
				(None, None)
			};
		let (duckdb_version, duckdb_executable_sha256) = if engines.contains(&PerfEngine::DuckDb) {
			require_one_thread_duckdb_reference(engines)?;
			let identity = runtime_identity()?.ok_or_else(|| {
				"DuckDB perf requires toron-bench run and a staged DuckDB executable".to_string()
			})?;
			let duckdb = identity.manifest.duckdb.as_ref().ok_or_else(|| {
				"DuckDB perf requires --duckdb-bin in the toron-bench build manifest".to_string()
			})?;
			let configured = PathBuf::from(duckdb_binary())
				.canonicalize()
				.map_err(|error| format!("canonicalize staged DuckDB executable: {error}"))?;
			let staged = duckdb
				.staged_path
				.canonicalize()
				.map_err(|error| format!("canonicalize manifest DuckDB executable: {error}"))?;
			if configured != staged {
				return Err(format!(
					"DuckDB perf executable is not the staged manifest executable: configured={} staged={}",
					configured.display(),
					staged.display()
				));
			}
			let version = duckdb_cli_version(
				staged
					.to_str()
					.ok_or_else(|| "staged DuckDB path is not UTF-8".to_string())?,
			)?
			.ok_or_else(|| "staged DuckDB executable is absent".to_string())?;
			if !(version.contains("v1.5.4") || version.starts_with("1.5.4")) {
				return Err(format!(
					"ADR 583 baseline requires DuckDB 1.5.4; staged executable reports {version:?}"
				));
			}
			(Some(version), Some(duckdb.sha256.clone()))
		} else {
			(None, None)
		};
		Ok(Self {
			mode: mode.to_string(),
			scale_factor: sf,
			data_source: data_source.to_string(),
			output_directory_fresh,
			host_start,
			join_spill_scratch,
			join_spill_scratch_empty_before,
			dataset_verification,
			dataset_manifest_fingerprint,
			budget,
			partition_work_concurrency_requested,
			duckdb_version,
			duckdb_executable_sha256,
		})
	}

	fn annotate_samples(&self, rows: &mut [serde_json::Value]) {
		for row in rows {
			row["data_source"] = serde_json::json!(self.data_source);
			row["dataset_manifest_fingerprint"] =
				serde_json::json!(self.dataset_manifest_fingerprint);
			row["memory_per_task"] = serde_json::json!(self.budget.memory_per_task);
			row["memory_per_task_provenance"] =
				serde_json::json!(self.budget.memory_per_task_provenance);
			row["spill_bytes_reserved"] = serde_json::json!(self.budget.spill_bytes_reserved);
			row["spill_bytes_reserved_provenance"] =
				serde_json::json!(self.budget.spill_bytes_reserved_provenance);
			row["partition_work_concurrency_requested"] =
				serde_json::json!(self.partition_work_concurrency_requested.0);
			row["partition_work_concurrency_requested_provenance"] =
				serde_json::json!(self.partition_work_concurrency_requested.1);
			if row.get("engine").and_then(serde_json::Value::as_str) == Some("duckdb") {
				row["duckdb_threads"] = serde_json::json!(duckdb_thread_setting());
				row["duckdb_threads_provenance"] =
					serde_json::json!(reference_thread_pin_provenance(&duckdb_thread_setting()));
				let (limit, provenance) = duckdb_memory_limit_fields();
				row["duckdb_memory_limit"] = limit;
				row["duckdb_memory_limit_provenance"] = provenance;
				row["duckdb_executable_sha256"] = serde_json::json!(self.duckdb_executable_sha256);
			}
		}
	}

	fn finish(
		&self,
		output_dir: &Path,
		samples: &[serde_json::Value],
		summaries: &[serde_json::Value],
	) -> Result<(), String> {
		let host_end = observe_host_quiescence();
		let join_spill_scratch_empty_after = directory_is_empty(&self.join_spill_scratch)?;
		let identity_gate = runtime_identity_gate();
		let affinity = toron_tpch::affinity::affinity_coordinate();
		let mut observed_queries = Vec::<String>::new();
		let mut observed_engines = Vec::<String>::new();
		for row in samples {
			if let Some(query) = row.get("query").and_then(serde_json::Value::as_str)
				&& !observed_queries.iter().any(|known| known == query)
			{
				observed_queries.push(query.to_string());
			}
			if let Some(engine) = row.get("engine").and_then(serde_json::Value::as_str)
				&& !observed_engines.iter().any(|known| known == engine)
			{
				observed_engines.push(engine.to_string());
			}
		}
		let mut header = serde_json::json!({
			"kind": "artifact_header",
			"suite": PERF_BOARD_SUITE,
			"evidence_identity": runtime_identity_json(),
			"content_identity_reasons": identity_gate.reasons,
			"scale_factor": self.scale_factor,
			"mode": self.mode,
			"data_source": self.data_source,
			"profile": samples.first().and_then(|row| row.get("profile")).cloned(),
			"runs": summaries.first().and_then(|row| row.get("runs")).cloned(),
			"warmup": summaries.first().and_then(|row| row.get("warmup")).cloned(),
			"queries": observed_queries,
			"engines": observed_engines,
			"dataset_verification": self.dataset_verification,
			"dataset_manifest_fingerprint": self.dataset_manifest_fingerprint,
			"memory_per_task": self.budget.memory_per_task,
			"memory_per_task_provenance": self.budget.memory_per_task_provenance,
			"spill_bytes_reserved": self.budget.spill_bytes_reserved,
			"spill_bytes_reserved_provenance": self.budget.spill_bytes_reserved_provenance,
			"partition_work_concurrency_requested": self.partition_work_concurrency_requested.0,
			"partition_work_concurrency_requested_provenance": self.partition_work_concurrency_requested.1,
			"duckdb_threads": duckdb_thread_setting(),
			"duckdb_threads_provenance": reference_thread_pin_provenance(&duckdb_thread_setting()),
			"duckdb_memory_limit": duckdb_memory_limit_fields().0,
			"duckdb_memory_limit_provenance": duckdb_memory_limit_fields().1,
			"duckdb_version": self.duckdb_version,
			"duckdb_executable_sha256": self.duckdb_executable_sha256,
			"cpu_affinity_requested": affinity.requested,
			"cpu_affinity_requested_provenance": affinity.provenance,
			"cpu_affinity_observed": toron_tpch::affinity::observed_cpulist(),
			"diagnostics_disabled": join_diagnostics_disabled(),
			"host_start_service_inactive": self.host_start.service_inactive,
			"host_start_sql_port_free": self.host_start.sql_port_free,
			"host_start_quiescent": self.host_start.quiescent(),
			"host_end_service_inactive": host_end.service_inactive,
			"host_end_sql_port_free": host_end.sql_port_free,
			"host_end_quiescent": host_end.quiescent(),
			"join_spill_scratch": self.join_spill_scratch,
			"join_spill_scratch_empty_before": self.join_spill_scratch_empty_before,
			"join_spill_scratch_empty_after": join_spill_scratch_empty_after,
			"output_directory_fresh": self.output_directory_fresh,
		});
		let ordered_summaries = ordered_perf_summaries(summaries);
		let decision = evaluate_perf_board(&header, samples, &ordered_summaries);
		header["candidate_authority"] = serde_json::json!(decision.candidate_authority);
		header["authority_reasons"] = serde_json::json!(decision.reasons);
		let artifact = output_dir.join(PERF_BOARD_ARTIFACT);
		write_perf_board_artifact(&artifact, &header, samples, &ordered_summaries)?;
		let (replayed_header, replayed_samples, replayed_summaries) =
			read_perf_board_artifact(&artifact)?;
		let replayed =
			evaluate_perf_board(&replayed_header, &replayed_samples, &replayed_summaries);
		if replayed != decision
			|| replayed_header
				.get("candidate_authority")
				.and_then(serde_json::Value::as_bool)
				!= Some(replayed.candidate_authority)
			|| replayed_header.get("authority_reasons")
				!= Some(&serde_json::json!(replayed.reasons))
		{
			return Err(
				"published perf board does not replay to its claimed authority".to_string(),
			);
		}
		match toron_tpch::evidence::write_runtime_evidence_manifest(
			&artifact,
			PERF_BOARD_SUITE,
			replayed.candidate_authority,
		)? {
			Some(path) => {
				if path.file_name().and_then(|name| name.to_str()) != Some(PERF_BOARD_EVIDENCE) {
					return Err(format!(
						"unexpected perf board evidence path {}",
						path.display()
					));
				}
			}
			None if replayed.candidate_authority => {
				return Err("candidate board has no runtime build manifest".to_string());
			}
			None => {}
		}
		Ok(())
	}
}

fn ordered_perf_summaries(summaries: &[serde_json::Value]) -> Vec<serde_json::Value> {
	let mut by_key = BTreeMap::new();
	for summary in summaries {
		if let (Some(query), Some(engine)) = (
			summary.get("query").and_then(serde_json::Value::as_str),
			summary.get("engine").and_then(serde_json::Value::as_str),
		) {
			by_key.insert((query.to_string(), engine.to_string()), summary.clone());
		}
	}
	let mut ordered = Vec::with_capacity(summaries.len());
	for query in ALL_QUERY_IDS {
		for engine in PERF_BOARD_ENGINES {
			if let Some(summary) = by_key.remove(&(query.to_string(), engine.to_string())) {
				ordered.push(summary);
			}
		}
	}
	ordered.extend(by_key.into_values());
	ordered
}

/// ADR 591: re-evaluate a published board under the gate this executable carries and say whether
/// the artifact's own claim still holds. A later, tighter gate can thereby verify an earlier
/// board's retained rows without rerunning them; a disagreement is printed, never hidden.
fn perf_board_replay_command(args: &[String]) -> ExitCode {
	let Some(artifact) = flag_value(args, "--artifact") else {
		eprintln!("perf-board-replay requires --artifact PATH");
		return ExitCode::from(2);
	};
	match perf_board_replay(&PathBuf::from(artifact)) {
		Ok((report, agrees)) => {
			println!("{report}");
			if agrees {
				ExitCode::SUCCESS
			} else {
				ExitCode::from(1)
			}
		}
		Err(error) => {
			eprintln!("{error}");
			ExitCode::from(2)
		}
	}
}

fn perf_board_replay(artifact: &Path) -> Result<(serde_json::Value, bool), String> {
	let (header, samples, summaries) = read_perf_board_artifact(artifact)?;
	let decision = evaluate_perf_board(&header, &samples, &ordered_perf_summaries(&summaries));
	let claimed_authority = header
		.get("candidate_authority")
		.and_then(serde_json::Value::as_bool);
	let claimed_reasons = header.get("authority_reasons").cloned();
	let agrees = claimed_authority == Some(decision.candidate_authority)
		&& claimed_reasons == Some(serde_json::json!(decision.reasons));
	Ok((
		serde_json::json!({
			"artifact": artifact.display().to_string(),
			"samples": samples.len(),
			"summaries": summaries.len(),
			"replayed_candidate_authority": decision.candidate_authority,
			"replayed_reasons": decision.reasons,
			"claimed_candidate_authority": claimed_authority,
			"claimed_reasons": claimed_reasons,
			"claim_agrees_with_replay": agrees,
			"replaying_evidence_identity": runtime_identity_json(),
		}),
		agrees,
	))
}

fn read_perf_board_artifact(
	path: &Path,
) -> Result<
	(
		serde_json::Value,
		Vec<serde_json::Value>,
		Vec<serde_json::Value>,
	),
	String,
> {
	let body = std::fs::read_to_string(path)
		.map_err(|error| format!("read {}: {error}", path.display()))?;
	let mut header = None;
	let mut samples = Vec::new();
	let mut summaries = Vec::new();
	for (line_number, line) in body.lines().enumerate() {
		let row: serde_json::Value = serde_json::from_str(line).map_err(|error| {
			format!("parse {} line {}: {error}", path.display(), line_number + 1)
		})?;
		match row.get("kind").and_then(serde_json::Value::as_str) {
			Some("artifact_header") if header.is_none() && line_number == 0 => header = Some(row),
			Some("sample") => samples.push(row),
			Some("cell_summary") => summaries.push(row),
			Some("artifact_header") => {
				return Err("perf board carries more than one or a misplaced header".to_string());
			}
			Some(kind) => return Err(format!("unknown perf board row kind {kind:?}")),
			None => return Err("perf board row is missing kind".to_string()),
		}
	}
	Ok((
		header.ok_or_else(|| "perf board is missing its header".to_string())?,
		samples,
		summaries,
	))
}

fn directory_is_empty(path: &Path) -> Result<bool, String> {
	match std::fs::read_dir(path) {
		Ok(mut entries) => Ok(entries.next().is_none()),
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
		Err(error) => Err(format!("inspect {}: {error}", path.display())),
	}
}

fn write_perf_board_artifact(
	path: &Path,
	header: &serde_json::Value,
	samples: &[serde_json::Value],
	summaries: &[serde_json::Value],
) -> Result<(), String> {
	if path.exists() {
		return Err(format!(
			"perf board artifact already exists at {}",
			path.display()
		));
	}
	let partial = PathBuf::from(format!("{}.partial", path.display()));
	if partial.exists() {
		return Err(format!(
			"partial perf board artifact already exists at {}",
			partial.display()
		));
	}
	let mut file = OpenOptions::new()
		.create_new(true)
		.write(true)
		.open(&partial)
		.map_err(|error| format!("create {}: {error}", partial.display()))?;
	let mut append = |row: &serde_json::Value| -> Result<(), String> {
		serde_json::to_writer(&mut file, row)
			.map_err(|error| format!("encode perf board row: {error}"))?;
		file.write_all(b"\n")
			.map_err(|error| format!("write perf board row: {error}"))
	};
	append(header)?;
	for sample in samples {
		let mut row = sample.clone();
		row["kind"] = serde_json::json!("sample");
		append(&row)?;
	}
	let mut by_key = BTreeMap::new();
	for summary in summaries {
		if let (Some(query), Some(engine)) = (
			summary.get("query").and_then(serde_json::Value::as_str),
			summary.get("engine").and_then(serde_json::Value::as_str),
		) {
			by_key.insert((query.to_string(), engine.to_string()), summary);
		}
	}
	for query in ALL_QUERY_IDS {
		for engine in PERF_BOARD_ENGINES {
			if let Some(summary) = by_key.get(&(query.to_string(), engine.to_string())) {
				let mut row = (*summary).clone();
				row["kind"] = serde_json::json!("cell_summary");
				append(&row)?;
			}
		}
	}
	file.sync_all()
		.map_err(|error| format!("sync {}: {error}", partial.display()))?;
	drop(file);
	std::fs::rename(&partial, path).map_err(|error| format!("publish {}: {error}", path.display()))
}

fn adr449_gate_zero_command(args: &[String]) -> ExitCode {
	let explicit_scale = match explicit_sf(args) {
		Ok(value) => value,
		Err(message) => return harness_error(&message),
	};
	let mode = flag_value(args, "--mode").unwrap_or("strict");
	let data_source = flag_value(args, "--data-source").unwrap_or("parquet");
	let data_dir = data_dir_from_args(args);
	let output_dir = PathBuf::from(flag_value(args, "--output-dir").unwrap_or("target/tpch/perf"));
	let scale_factor = match selected_scale_or_default(explicit_scale, &output_dir) {
		Ok(value) => value,
		Err(message) => return harness_error(&message),
	};
	match run_adr449_gate_zero(scale_factor, mode, data_source, &data_dir, &output_dir) {
		Ok(record) if record["passed"] == true => ExitCode::SUCCESS,
		Ok(record) => {
			eprintln!("ADR 449 gate zero failed: {record}");
			ExitCode::from(1)
		}
		Err(message) => harness_error(&message),
	}
}

fn selected_scale_or_default(explicit: Option<f64>, output_dir: &Path) -> Result<f64, String> {
	if let Some(scale) = explicit {
		return Ok(scale);
	}
	let selected = output_dir.join("adr450_selected_scale.json");
	if !selected.exists() {
		return Ok(1.0);
	}
	let bytes = std::fs::read(&selected)
		.map_err(|error| format!("read selected ADR 450 scale: {error}"))?;
	let value: serde_json::Value = serde_json::from_slice(&bytes)
		.map_err(|error| format!("parse selected ADR 450 scale: {error}"))?;
	value
		.get("selected_scale")
		.and_then(serde_json::Value::as_f64)
		.ok_or_else(|| format!("selected ADR 450 scale missing from {}", selected.display()))
}

const ADR450_GUARD_A: &str = "admitted chain did not commit the generic grouped reservoir path";
const ADR450_GUARD_B: &str = "generic grouped path did not commit the partitioned-scan reservoir";
const ADR450_NO_CHAIN_PROFILE: &str =
	"no grouped-chain surface entered (passed with zero profiles)";

#[derive(Debug, Clone, PartialEq)]
enum Adr450RungOutcome {
	Committed {
		reservoir_build_rows: u64,
		execute_ms: f64,
	},
	NonCommitting {
		diagnostic: &'static str,
		execute_ms: f64,
	},
}

fn adr450_classify_rung(
	row: &serde_json::Value,
	profile_dir: &Path,
) -> Result<Adr450RungOutcome, String> {
	let execute_ms = row
		.get("execute_ms")
		.and_then(serde_json::Value::as_f64)
		.ok_or("scale rung row lacks execute_ms")?;
	let status = row
		.get("status")
		.and_then(serde_json::Value::as_str)
		.unwrap_or("fail");
	if status == "pass" {
		// A passing child with an empty profile dir never entered the admitted
		// grouped-chain surface at all (no commit guard can fire outside it) —
		// a legitimate non-commit, distinct from Guards A/B. Ambiguity (two or
		// more profiles) still fails inside profiled_reservoir_build_rows.
		if count_grouped_chain_profiles(profile_dir)? == 0 {
			return Ok(Adr450RungOutcome::NonCommitting {
				diagnostic: ADR450_NO_CHAIN_PROFILE,
				execute_ms,
			});
		}
		return Ok(Adr450RungOutcome::Committed {
			reservoir_build_rows: profiled_reservoir_build_rows(profile_dir)?,
			execute_ms,
		});
	}
	let diagnostic = row
		.get("diagnostic_message")
		.and_then(serde_json::Value::as_str)
		.unwrap_or("");
	if diagnostic.contains(ADR450_GUARD_A) {
		return Ok(Adr450RungOutcome::NonCommitting {
			diagnostic: ADR450_GUARD_A,
			execute_ms,
		});
	}
	if diagnostic.contains(ADR450_GUARD_B) {
		return Ok(Adr450RungOutcome::NonCommitting {
			diagnostic: ADR450_GUARD_B,
			execute_ms,
		});
	}
	Err(format!("ADR 450 scale rung hard failure: {row}"))
}

fn adr450_scale_rungs(args: &[String]) -> Result<Vec<f64>, String> {
	if let Some(raw) = flag_value(args, "--scales") {
		let mut values = raw
			.split(',')
			.map(parse_scale_factor)
			.collect::<Result<Vec<_>, _>>()?;
		values.sort_by(f64::total_cmp);
		values.dedup_by(|left, right| left == right);
		if values.is_empty() {
			return Err("--scales must contain at least one scale".to_string());
		}
		return Ok(values);
	}
	Ok((1..=10).map(f64::from).collect())
}

#[derive(Debug, Clone, Copy)]
struct GaugeBudget {
	memory_per_task: u64,
	memory_per_task_provenance: &'static str,
	spill_bytes_reserved: u64,
	spill_bytes_reserved_provenance: &'static str,
}

impl GaugeBudget {
	fn from_environment() -> Result<Self, String> {
		let (memory_per_task, memory_per_task_provenance) = gauge_memory_per_task()?;
		let (spill_bytes_reserved, spill_bytes_reserved_provenance) = gauge_spill_bytes_reserved()?;
		Ok(Self {
			memory_per_task,
			memory_per_task_provenance,
			spill_bytes_reserved,
			spill_bytes_reserved_provenance,
		})
	}

	#[cfg(test)]
	const fn defaults() -> Self {
		Self {
			memory_per_task: 64 * 1024 * 1024,
			memory_per_task_provenance: "default",
			spill_bytes_reserved: 512 * 1024 * 1024,
			spill_bytes_reserved_provenance: "default",
		}
	}
}

fn adr450_scale_ladder_command(args: &[String]) -> ExitCode {
	let query = flag_value(args, "--query").unwrap_or("q05");
	if !ALL_QUERY_IDS.contains(&query) {
		return harness_error(&format!("unknown ladder query {query:?}"));
	}
	let mode = flag_value(args, "--mode").unwrap_or("strict");
	let data_source = flag_value(args, "--data-source").unwrap_or("parquet");
	let data_dir = data_dir_from_args(args);
	let output_dir = PathBuf::from(flag_value(args, "--output-dir").unwrap_or("target/tpch/perf"));
	let scales = match adr450_scale_rungs(args) {
		Ok(scales) => scales,
		Err(message) => return harness_error(&message),
	};
	let budget = match GaugeBudget::from_environment() {
		Ok(budget) => budget,
		Err(message) => return harness_error(&message),
	};
	match run_adr450_scale_ladder(
		query,
		&scales,
		mode,
		data_source,
		&data_dir,
		&output_dir,
		budget,
	) {
		Ok(record) => {
			println!("{record}");
			ExitCode::SUCCESS
		}
		Err(message) => harness_error(&message),
	}
}

fn run_adr450_scale_ladder(
	query: &str,
	scales: &[f64],
	mode: &str,
	data_source: &str,
	data_dir: &Path,
	output_dir: &Path,
	budget: GaugeBudget,
) -> Result<serde_json::Value, String> {
	if data_source != "parquet" && data_source != "persisted" {
		return Err("ADR 450 requires --data-source parquet".to_string());
	}
	let GaugeBudget {
		memory_per_task,
		memory_per_task_provenance: memory_provenance,
		spill_bytes_reserved,
		spill_bytes_reserved_provenance: spill_provenance,
	} = budget;
	// q05 keeps the original artifact names: ADR 449 reads its fixture scale
	// from `adr450_selected_scale.json` and that contract must not move.
	let suffix = if query == "q05" {
		String::new()
	} else {
		format!("_{query}")
	};
	let ladder_dir = output_dir.join(format!("adr450_scale_ladder{suffix}"));
	if ladder_dir.exists() {
		std::fs::remove_dir_all(&ladder_dir)
			.map_err(|error| format!("clear ladder directory: {error}"))?;
	}
	std::fs::create_dir_all(&ladder_dir)
		.map_err(|error| format!("create ladder directory: {error}"))?;
	let mut rungs = Vec::new();
	let write_ladder_record = |record: &serde_json::Value| -> Result<(), String> {
		std::fs::write(
			output_dir.join(format!("adr450_scale_ladder{suffix}.json")),
			serde_json::to_vec_pretty(record).map_err(|error| error.to_string())?,
		)
		.map_err(|error| format!("write ladder record: {error}"))
	};
	for scale in scales {
		let rung_dir = ladder_dir.join(format!("sf_{scale}"));
		let profile_dir = rung_dir.join("profile");
		if rung_dir.exists() {
			return Err(format!(
				"ladder rung directory already exists: {}",
				rung_dir.display()
			));
		}
		std::fs::create_dir_all(&profile_dir)
			.map_err(|error| format!("create rung profile directory: {error}"))?;
		let spill_dir = Path::new("target/tpch/join_spill");
		if spill_dir.exists() {
			std::fs::remove_dir_all(spill_dir)
				.map_err(|error| format!("clear rung spill directory: {error}"))?;
		}
		let dataset = prepare_persisted_tpch_dataset(mode, *scale, data_dir)
			.map_err(|error| format!("prepare sf={scale} dataset: {error}"))?;
		let row = run_perf_child_with_envs(
			query,
			mode,
			*scale,
			0,
			data_source,
			data_dir,
			&[
				("TORON_ABLATE_LOCAL_RUNTIME_FILTER", "0"),
				(
					"TORON_GROUPED_CHAIN_PROFILE_DIR",
					profile_dir.to_str().ok_or("invalid profile path")?,
				),
			],
		)?;
		let outcome = match adr450_classify_rung(&row, &profile_dir) {
			Ok(outcome) => outcome,
			Err(error) => {
				let record = serde_json::json!({
					"suite": "adr450_scale_ladder",
					"memory_per_task": memory_per_task,
					"memory_per_task_provenance": memory_provenance,
				"spill_bytes_reserved": spill_bytes_reserved,
				"spill_bytes_reserved_provenance": spill_provenance,
					"outcome": "hard_error",
					"error": error,
					"rungs": rungs,
					"failed_scale": scale,
				});
				write_ladder_record(&record)?;
				return Err(format!(
					"ADR 450 scale ladder hard failure at sf={scale}: {record}"
				));
			}
		};
		let (outcome_name, diagnostic, reservoir_build_rows, execute_ms) = match &outcome {
			Adr450RungOutcome::Committed {
				reservoir_build_rows,
				execute_ms,
			} => (
				"committed",
				serde_json::Value::Null,
				Some(*reservoir_build_rows),
				*execute_ms,
			),
			Adr450RungOutcome::NonCommitting {
				diagnostic,
				execute_ms,
			} => (
				"non_committing",
				serde_json::json!(diagnostic),
				None,
				*execute_ms,
			),
		};
		let record = serde_json::json!({
			"query": query,
			"scale": scale,
			"outcome": outcome_name,
			"diagnostic": diagnostic,
			"execute_ms": execute_ms,
			"reservoir_build_rows": reservoir_build_rows,
			"manifest_fingerprint": dataset.manifest_fingerprint,
			"trusted": *scale == 10.0 && outcome_name == "committed",
		});
		rungs.push(record);
		if outcome_name == "committed" {
			let selected = serde_json::json!({
				"suite": "adr450_scale_ladder",
				"query": query,
				"memory_per_task": memory_per_task,
				"memory_per_task_provenance": memory_provenance,
			"spill_bytes_reserved": spill_bytes_reserved,
			"spill_bytes_reserved_provenance": spill_provenance,
				"selected_scale": scale,
				"rungs": rungs,
				"outcome": "committed",
			});
			write_ladder_record(&selected)?;
			std::fs::write(
				output_dir.join(format!("adr450_selected_scale{suffix}.json")),
				serde_json::to_vec_pretty(&selected).map_err(|error| error.to_string())?,
			)
			.map_err(|error| format!("write selected scale: {error}"))?;
			return Ok(selected);
		}
	}
	let no_commit = serde_json::json!({
		"suite": "adr450_scale_ladder",
		"query": query,
		"memory_per_task": memory_per_task,
		"memory_per_task_provenance": memory_provenance,
	"spill_bytes_reserved": spill_bytes_reserved,
	"spill_bytes_reserved_provenance": spill_provenance,
		"outcome": "no_commit",
		"selected_scale": null,
		"rungs": rungs,
	});
	write_ladder_record(&no_commit)?;
	Ok(no_commit)
}

/// Answer-check a query against its independent oracle and record a correctness row. Unsupported queries are a
/// usage error (exit 2, no row), not fake telemetry.
async fn correctness_command(args: &[String]) -> ExitCode {
	let query = flag_value(args, "--query").unwrap_or("q06");
	let data_source = flag_value(args, "--data-source").unwrap_or("memory");
	let data_dir = data_dir_from_args(args);
	let explicit = match explicit_sf(args) {
		Ok(explicit) => explicit,
		Err(message) => return harness_error(&message),
	};
	let sf = if matches!(query, "q17") {
		match q17_correctness_sf(explicit) {
			Ok(sf) => sf,
			Err(message) => return harness_error(&message),
		}
	} else {
		explicit.unwrap_or(DEFAULT_SF)
	};
	let attempt = match check_strict_query_with_data_source(query, sf, data_source, &data_dir).await
	{
		Ok(attempt) => attempt,
		Err(message) => return harness_error(&message),
	};
	emit_correctness(&attempt);
	if attempt.is_process_success() {
		ExitCode::SUCCESS
	} else {
		ExitCode::FAILURE
	}
}

fn q17_correctness_sf(explicit: Option<f64>) -> Result<f64, String> {
	q17_correctness_scale(explicit)
}

fn emit_correctness(attempt: &CorrectnessAttempt) {
	println!("{}", attempt.human());
	println!("{}", attempt.jsonl());
	let _ = std::fs::create_dir_all(CACHE_ROOT);
	let path = format!("{CACHE_ROOT}/correctness.jsonl");
	let existing = std::fs::read_to_string(&path).unwrap_or_default();
	let _ = std::fs::write(&path, format!("{existing}{}\n", attempt.jsonl()));
}

async fn run_command(args: &[String]) -> ExitCode {
	let query = flag_value(args, "--query").unwrap_or("q06");
	let mode = flag_value(args, "--mode").unwrap_or("dev");
	let explicit = match explicit_sf(args) {
		Ok(explicit) => explicit,
		Err(message) => return harness_error(&message),
	};
	let sf = explicit.unwrap_or(DEFAULT_SF);
	let allow_blocked = args.iter().any(|arg| arg == "--allow-blocked");

	let strict = mode == "strict";
	let attempt = match query {
		"q06" | "q6" => {
			if strict {
				run_strict_q6(sf).await
			} else {
				run_dev_q6(sf).await
			}
		}
		// Single-table reachability attempt over the lineitem fixture.
		"q01" | "q1" => {
			if strict {
				run_strict_q1(sf).await
			} else {
				run_dev_q1(sf).await
			}
		}
		// Three-table reachability attempt over customer/orders/lineitem.
		"q03" | "q3" => {
			if strict {
				run_strict_q3(sf).await
			} else {
				run_dev_q3(sf).await
			}
		}
		// Five-table query with a multi-table-inner correlated scalar subquery.
		"q02" | "q2" => {
			if strict {
				run_strict_q02(sf).await
			} else {
				run_dev_q02(sf).await
			}
		}
		// Four-table reachability attempt over customer/orders/lineitem/nation.
		"q10" => {
			if strict {
				run_strict_q10(sf).await
			} else {
				run_dev_q10(sf).await
			}
		}
		// Six-table reachability attempt over customer/orders/lineitem/supplier/nation/region.
		"q05" | "q5" => {
			if strict {
				run_strict_q05(sf).await
			} else {
				run_dev_q05(sf).await
			}
		}
		// Volume-shipping reachability attempt: q05's table family without region, with nation self-joined.
		"q07" | "q7" => {
			if strict {
				run_strict_q07(sf).await
			} else {
				run_dev_q07(sf).await
			}
		}
		// National-market-share reachability attempt: q05's table family plus part, with a nation self-join.
		"q08" | "q8" => {
			if strict {
				run_strict_q08(sf).await
			} else {
				run_dev_q08(sf).await
			}
		}
		// Product-profit reachability attempt over part/supplier/lineitem/partsupp/orders/nation.
		"q09" | "q9" => {
			if strict {
				run_strict_q09(sf).await
			} else {
				run_dev_q09(sf).await
			}
		}
		// Correlated-subquery reachability attempt: orders + an EXISTS over lineitem.
		"q04" | "q4" => {
			if strict {
				run_strict_q04(sf).await
			} else {
				run_dev_q04(sf).await
			}
		}
		// Shipping-mode/order-priority reachability attempt over orders + lineitem.
		"q12" => {
			if strict {
				run_strict_q12(sf).await
			} else {
				run_dev_q12(sf).await
			}
		}
		// Part-supplier value reachability attempt over partsupp/supplier/nation.
		"q11" => {
			let result = if strict {
				run_strict_q11(sf).await
			} else {
				run_dev_q11(sf).await
			};
			match result {
				Ok(attempt) => attempt,
				Err(message) => return harness_error(&message),
			}
		}
		// Customer-distribution reachability attempt over customer + orders.
		"q13" => {
			if strict {
				run_strict_q13(sf).await
			} else {
				run_dev_q13(sf).await
			}
		}
		// Two-table decimal-ratio reachability attempt.
		"q14" => {
			if strict {
				run_strict_q14(sf).await
			} else {
				run_dev_q14(sf).await
			}
		}
		// Top-supplier reachability attempt: CTE used as a joined source and scalar-subquery source.
		"q15" => {
			if strict {
				run_strict_q15(sf).await
			} else {
				run_dev_q15(sf).await
			}
		}
		// Parts/supplier relationship reachability attempt over part + partsupp + supplier.
		"q16" => {
			if strict {
				run_strict_q16(sf).await
			} else {
				run_dev_q16(sf).await
			}
		}
		// Correlated-scalar-subquery reachability attempt: part + a scalar subquery over lineitem.
		"q17" => {
			if strict {
				run_strict_q17(sf).await
			} else {
				run_dev_q17(sf).await
			}
		}
		// Large-volume-customer reachability attempt: grouped IN subquery over customer/orders/lineitem.
		"q18" => {
			if strict {
				run_strict_q18(sf).await
			} else {
				run_dev_q18(sf).await
			}
		}
		// Discounted-revenue reachability attempt: OR-of-AND predicates over lineitem + part.
		"q19" => {
			if strict {
				run_strict_q19(sf).await
			} else {
				run_dev_q19(sf).await
			}
		}
		// Nested membership with a correlated scalar inside the membership body.
		"q20" => {
			if strict {
				run_strict_q20(sf).await
			} else {
				run_dev_q20(sf).await
			}
		}
		// Supplier-wait reachability attempt: correlated EXISTS/NOT EXISTS over lineitem self-aliases.
		"q21" => {
			if strict {
				run_strict_q21(sf).await
			} else {
				run_dev_q21(sf).await
			}
		}
		// Global-sales-opportunity reachability attempt: customer + orders with SQL SUBSTRING.
		"q22" => {
			if strict {
				run_strict_q22(sf).await
			} else {
				run_dev_q22(sf).await
			}
		}
		_ => Attempt {
			query: query.to_string(),
			scale_factor: sf,
			mode: mode.to_string(),
			phase: Phase::Plan,
			status: Status::NotAttempted,
			reason: Some("query_not_implemented".to_string()),
			elapsed_ms: 0.0,
			digest: None,
			diagnostic_message: None,
		},
	};

	emit(&attempt);
	if attempt.is_process_success(allow_blocked) {
		ExitCode::SUCCESS
	} else {
		ExitCode::FAILURE
	}
}

fn emit(attempt: &Attempt) {
	println!("{}", attempt.human());
	println!("{}", attempt.jsonl());
	// The only on-disk write: append the structured row under the cache root (never a tracked path).
	let _ = std::fs::create_dir_all(CACHE_ROOT);
	if let Ok(existing) = std::fs::read_to_string(format!("{CACHE_ROOT}/reachability.jsonl")) {
		let _ = std::fs::write(
			format!("{CACHE_ROOT}/reachability.jsonl"),
			format!("{existing}{}\n", attempt.jsonl()),
		);
	} else {
		let _ = std::fs::write(
			format!("{CACHE_ROOT}/reachability.jsonl"),
			format!("{}\n", attempt.jsonl()),
		);
	}
}

fn perf_query_list(query: &str) -> Result<Vec<&'static str>, String> {
	if query == "all" {
		return Ok(ALL_QUERY_IDS.to_vec());
	}
	let normalized = match query {
		"q1" => "q01",
		"q2" => "q02",
		"q3" => "q03",
		"q4" => "q04",
		"q5" => "q05",
		"q6" => "q06",
		"q7" => "q07",
		"q8" => "q08",
		"q9" => "q09",
		"q01" => "q01",
		"q02" => "q02",
		"q03" => "q03",
		"q04" => "q04",
		"q05" => "q05",
		"q06" => "q06",
		"q07" => "q07",
		"q08" => "q08",
		"q09" => "q09",
		"q10" => "q10",
		"q11" => "q11",
		"q12" => "q12",
		"q13" => "q13",
		"q14" => "q14",
		"q15" => "q15",
		"q16" => "q16",
		"q17" => "q17",
		"q18" => "q18",
		"q19" => "q19",
		"q20" => "q20",
		"q21" => "q21",
		"q22" => "q22",
		// ADR 473: the primitive ladder rides the same perf rail as the board.
		"m00" => "m00",
		"m01" => "m01",
		"m02" => "m02",
		"m03" => "m03",
		"m04" => "m04",
		"m05" => "m05",
		"m06" => "m06",
		"m07" => "m07",
		"m08" => "m08",
		"m09" => "m09",
		"m10" => "m10",
		other => return Err(format!("perf --query {other} is not supported")),
	};
	Ok(vec![normalized])
}

fn run_perf_child(
	engine: PerfEngine,
	query: &str,
	mode: &str,
	sf: f64,
	run_index: u32,
	warmup: bool,
	data_source: &str,
	data_dir: &Path,
) -> Result<serde_json::Value, String> {
	run_perf_child_with_ablation(
		engine,
		query,
		mode,
		sf,
		run_index,
		warmup,
		data_source,
		data_dir,
		None,
	)
}

fn run_perf_child_with_ablation(
	engine: PerfEngine,
	query: &str,
	mode: &str,
	sf: f64,
	run_index: u32,
	warmup: bool,
	data_source: &str,
	data_dir: &Path,
	ablate_filter: Option<(&PairedSwitch, bool)>,
) -> Result<serde_json::Value, String> {
	let mut args = vec![
		"perf-run-one".to_string(),
		"--engine".to_string(),
		engine.as_str().to_string(),
		"--query".to_string(),
		query.to_string(),
		"--mode".to_string(),
		mode.to_string(),
		"--sf".to_string(),
		sf.to_string(),
		"--run-index".to_string(),
		run_index.to_string(),
		"--data-source".to_string(),
		data_source.to_string(),
		"--data-dir".to_string(),
		data_dir.display().to_string(),
	];
	if warmup {
		args.push("--warmup-run".to_string());
	}
	let mut row = match ablate_filter {
		Some((switch, off_arm)) => run_perf_child_args_with_envs(
			&args,
			&[(
				switch.env.as_str(),
				if off_arm {
					switch.off_value.as_str()
				} else {
					switch.on_value.as_str()
				},
			)],
		)?,
		None => run_perf_child_args(&args)?,
	};
	if let Some((_, off_arm)) = ablate_filter {
		row["ablation_arm"] = serde_json::json!(if off_arm { "off" } else { "on" });
	}
	Ok(row)
}

fn run_perf_child_with_envs(
	query: &str,
	mode: &str,
	sf: f64,
	run_index: u32,
	data_source: &str,
	data_dir: &Path,
	envs: &[(&str, &str)],
) -> Result<serde_json::Value, String> {
	let args = vec![
		"perf-run-one".to_string(),
		"--engine".to_string(),
		"toron".to_string(),
		"--query".to_string(),
		query.to_string(),
		"--mode".to_string(),
		mode.to_string(),
		"--sf".to_string(),
		sf.to_string(),
		"--run-index".to_string(),
		run_index.to_string(),
		"--data-source".to_string(),
		data_source.to_string(),
		"--data-dir".to_string(),
		data_dir.display().to_string(),
	];
	run_perf_child_args_with_envs(&args, envs)
}

fn q05_reservoir_full_input_cardinality(scale_factor: f64) -> u64 {
	strict_lineitem_row_count(scale_factor)
}

fn adr449_gate_zero_passes(on_rows: u64, off_rows: u64, expected_off: u64) -> bool {
	off_rows == expected_off && on_rows < off_rows
}

fn profile_files(directory: &Path) -> Result<Vec<PathBuf>, String> {
	let mut files = std::fs::read_dir(directory)
		.map_err(|error| format!("read profile directory {}: {error}", directory.display()))?
		.filter_map(|entry| entry.ok().map(|entry| entry.path()))
		.filter(|path| {
			path.file_name()
				.and_then(|name| name.to_str())
				.map(|name| name.starts_with("grouped_chain_") && name.ends_with(".json"))
				.unwrap_or(false)
		})
		.collect::<Vec<_>>();
	files.sort();
	Ok(files)
}

fn count_grouped_chain_profiles(directory: &Path) -> Result<usize, String> {
	Ok(profile_files(directory)?.len())
}

fn profiled_reservoir_build_rows(directory: &Path) -> Result<u64, String> {
	let files = profile_files(directory)?;
	if files.len() != 1 {
		return Err(format!(
			"expected exactly one grouped-chain profile in {}, found {}",
			directory.display(),
			files.len()
		));
	}
	let bytes = std::fs::read(&files[0])
		.map_err(|error| format!("read profile {}: {error}", files[0].display()))?;
	let value: serde_json::Value = serde_json::from_slice(&bytes)
		.map_err(|error| format!("parse profile {}: {error}", files[0].display()))?;
	value
		.get("reservoir_build_rows")
		.and_then(serde_json::Value::as_u64)
		.ok_or_else(|| format!("profile {} lacks reservoir_build_rows", files[0].display()))
}

fn run_adr449_gate_zero(
	scale_factor: f64,
	mode: &str,
	data_source: &str,
	data_dir: &Path,
	output_dir: &Path,
) -> Result<serde_json::Value, String> {
	if data_source != "parquet" && data_source != "persisted" {
		return Err("ADR 449 gate zero requires --data-source parquet".to_string());
	}
	let gate_dir = output_dir.join("adr449_gate_zero");
	if gate_dir.exists() {
		std::fs::remove_dir_all(&gate_dir)
			.map_err(|error| format!("clear gate-zero directory: {error}"))?;
	}
	let on_dir = gate_dir.join("on");
	let off_dir = gate_dir.join("off");
	std::fs::create_dir_all(&on_dir)
		.map_err(|error| format!("create ON profile directory: {error}"))?;
	std::fs::create_dir_all(&off_dir)
		.map_err(|error| format!("create OFF profile directory: {error}"))?;
	let on_row = run_perf_child_with_envs(
		"q05",
		mode,
		scale_factor,
		0,
		data_source,
		data_dir,
		&[
			("TORON_ABLATE_LOCAL_RUNTIME_FILTER", "0"),
			(
				"TORON_GROUPED_CHAIN_PROFILE_DIR",
				on_dir.to_str().ok_or("invalid ON profile path")?,
			),
		],
	)?;
	if on_row.get("status").and_then(serde_json::Value::as_str) != Some("pass") {
		return Err(format!("ADR 449 ON gate-zero child did not pass: {on_row}"));
	}
	let off_row = run_perf_child_with_envs(
		"q05",
		mode,
		scale_factor,
		1,
		data_source,
		data_dir,
		&[
			("TORON_ABLATE_LOCAL_RUNTIME_FILTER", "1"),
			(
				"TORON_GROUPED_CHAIN_PROFILE_DIR",
				off_dir.to_str().ok_or("invalid OFF profile path")?,
			),
		],
	)?;
	if off_row.get("status").and_then(serde_json::Value::as_str) != Some("pass") {
		return Err(format!(
			"ADR 449 OFF gate-zero child did not pass: {off_row}"
		));
	}
	let on_rows = profiled_reservoir_build_rows(&on_dir)?;
	let off_rows = profiled_reservoir_build_rows(&off_dir)?;
	let expected_off = q05_reservoir_full_input_cardinality(scale_factor);
	let passed = adr449_gate_zero_passes(on_rows, off_rows, expected_off);
	let record = serde_json::json!({
		"suite": "adr449_gate_zero",
		"query": "q05",
		"scale_factor": scale_factor,
		"on_rows": on_rows,
		"off_rows": off_rows,
		"expected_off": expected_off,
		"off_matches_expected": off_rows == expected_off,
		"on_strictly_less": on_rows < off_rows,
		"on_digest": on_row.get("digest"),
		"off_digest": off_row.get("digest"),
		"passed": passed,
	});
	println!("{record}");
	Ok(record)
}

fn attach_paired_ablation_receipt(
	mut summaries: Vec<serde_json::Value>,
	runs: usize,
	gate_zero: serde_json::Value,
	fixed_materiality_floor_ms: Option<f64>,
) -> Result<Vec<serde_json::Value>, String> {
	if !gate_zero
		.get("passed")
		.and_then(serde_json::Value::as_bool)
		.unwrap_or(false)
	{
		return Err("paired receipt requires a passed gate-zero record".to_string());
	}
	if runs < 6 {
		return Err("paired ablation requires at least six measured runs".to_string());
	}
	for summary in &mut summaries {
		let samples = summary
			.get("execute_ms_samples")
			.and_then(serde_json::Value::as_array)
			.ok_or("paired summary lacks execute_ms_samples")?;
		let diffs = paired_ablation_differences(samples)?;
		let shift = paired_median_shift(&diffs)?;
		let mut off_values = samples
			.iter()
			.filter(|sample| {
				sample
					.get("ablation_arm")
					.and_then(serde_json::Value::as_str)
					== Some("off")
			})
			.filter_map(|sample| sample.get("execute_ms").and_then(serde_json::Value::as_f64))
			.collect::<Vec<_>>();
		if off_values.is_empty() {
			return Err("paired summary lacks off-arm samples for the baseline median".to_string());
		}
		// The materiality baseline is the OFF/control arm's median, not the pooled
		// cohort median: the pool mixes both arms and shrinks the floor by the very
		// effect under test.
		let baseline_off_median = median(&mut off_values);
		let materiality_floor_ms = fixed_materiality_floor_ms
			.unwrap_or_else(|| MATERIALITY_FLOOR * baseline_off_median.abs());
		summary["gate_zero"] = gate_zero.clone();
		summary["paired_ablation"] = serde_json::json!({
			"differences_off_minus_on_ms": diffs,
			"point_ms": shift.point,
			"lower_ms": shift.lower,
			"upper_ms": shift.upper,
			"pairs": shift.pairs,
			"baseline_off_median_ms": baseline_off_median,
			"materiality_floor_ms": materiality_floor_ms,
			"materiality_policy": if fixed_materiality_floor_ms.is_some() {
				"fixed_predeclared"
			} else {
				"cohort_relative_10_percent"
			},
			"material": shift.point >= materiality_floor_ms,
			"improving_interval_excludes_zero": shift.lower > 0.0,
			"activate": shift.lower > 0.0 && shift.point >= materiality_floor_ms,
		});
	}
	Ok(summaries)
}

fn paired_ablation_differences(samples: &[serde_json::Value]) -> Result<Vec<f64>, String> {
	let mut by_pair = BTreeMap::<u64, (Option<f64>, Option<f64>)>::new();
	for sample in samples {
		let index = sample
			.get("run_index")
			.and_then(serde_json::Value::as_u64)
			.ok_or("sample lacks run_index")?;
		let arm = sample
			.get("ablation_arm")
			.and_then(serde_json::Value::as_str)
			.ok_or("sample lacks ablation_arm")?;
		let value = sample
			.get("execute_ms")
			.and_then(serde_json::Value::as_f64)
			.ok_or("sample lacks execute_ms")?;
		if (index % 2 == 0) != (arm == "on") {
			return Err(format!("ablation arm parity mismatch at run_index {index}"));
		}
		let entry = by_pair.entry(index / 2).or_insert((None, None));
		match arm {
			"on" => {
				if entry.0.replace(value).is_some() {
					return Err(format!("duplicate ablation sample at run_index {index}"));
				}
			}
			"off" => {
				if entry.1.replace(value).is_some() {
					return Err(format!("duplicate ablation sample at run_index {index}"));
				}
			}
			_ => return Err(format!("unknown ablation arm {arm:?}")),
		}
	}
	if by_pair.is_empty() {
		return Err("no paired samples".to_string());
	}
	let expected = 0..(by_pair.len() as u64);
	if by_pair.keys().copied().ne(expected) {
		return Err("paired run indices are not contiguous".to_string());
	}
	// Pair entries are validated above; each index must contain both arms.
	by_pair
		.into_values()
		.map(|(on, off)| Ok(off.ok_or("missing OFF sample")? - on.ok_or("missing ON sample")?))
		.collect()
}

fn run_duckdb_perf_rows(
	query: &str,
	mode: &str,
	sf: f64,
	runs: usize,
	warmup: usize,
	_output_dir: &Path,
	data_source: &str,
	data_dir: &Path,
) -> Result<Vec<serde_json::Value>, String> {
	let duckdb_bin = duckdb_binary();
	let Some(duckdb_version) = duckdb_cli_version(&duckdb_bin)? else {
		let mut rows = Vec::new();
		for warmup_index in 0..warmup {
			rows.push(duckdb_not_attempted_row(
				query,
				mode,
				sf,
				warmup_index as u32,
				true,
				"duckdb_absent",
				None,
			));
		}
		for run_index in 0..runs {
			rows.push(duckdb_not_attempted_row(
				query,
				mode,
				sf,
				run_index as u32,
				false,
				"duckdb_absent",
				None,
			));
		}
		return Ok(rows);
	};

	let export_root = PathBuf::from(CACHE_ROOT).join("parquet");
	let export = match data_source {
		"memory" => export_perf_query_parquet(query, mode, sf, &export_root)?,
		"parquet" | "persisted" => export_persisted_perf_query_parquet(query, mode, sf, data_dir)?,
		other => return Err(format!("perf --data-source {other:?} is not supported")),
	};
	duckdb_type_parity_gate(&duckdb_bin, &export)?;
	let output_schema_fingerprint = duckdb_output_schema_fingerprint(&duckdb_bin, &export)?;
	let plan_fingerprint = duckdb_plan_fingerprint(&export);

	let mut rows = Vec::new();
	for warmup_index in 0..warmup {
		rows.push(run_duckdb_perf_child(
			&duckdb_bin,
			&duckdb_version,
			&export,
			&plan_fingerprint,
			&output_schema_fingerprint,
			warmup_index as u32,
			true,
		)?);
	}
	for run_index in 0..runs {
		rows.push(run_duckdb_perf_child(
			&duckdb_bin,
			&duckdb_version,
			&export,
			&plan_fingerprint,
			&output_schema_fingerprint,
			run_index as u32,
			false,
		)?);
	}
	Ok(rows)
}

fn duckdb_binary() -> String {
	std::env::var("TORON_TPCH_DUCKDB_BIN").unwrap_or_else(|_| "duckdb".to_string())
}

fn toron_binary() -> Result<std::path::PathBuf, String> {
	let current = std::env::current_exe().map_err(|error| format!("current exe: {error}"))?;
	Ok(toron_binary_from_override(
		std::env::var_os("TORON_TPCH_TORON_BIN"),
		current,
	))
}

fn toron_binary_from_override(
	candidate: Option<std::ffi::OsString>,
	current: std::path::PathBuf,
) -> std::path::PathBuf {
	candidate.map(Into::into).unwrap_or(current)
}

const ADR448_HARNESS_CAPABILITY: &str = "adr448-harness-capability-v1";

fn require_adr448_harness_capability() -> Result<(), String> {
	let binary = toron_binary()?;
	let output = Command::new(&binary)
		.arg("adr448-capability")
		.output()
		.map_err(|error| format!("probe ADR 448 harness capability: {error}"))?;
	let stdout = String::from_utf8_lossy(&output.stdout);
	if output.status.success() && stdout.trim() == ADR448_HARNESS_CAPABILITY {
		Ok(())
	} else {
		Err(format!(
			"comparison binary {} lacks ADR 448 harness capability",
			binary.display()
		))
	}
}

fn duckdb_cli_version(duckdb_bin: &str) -> Result<Option<String>, String> {
	match Command::new(duckdb_bin).arg("--version").output() {
		Ok(output) if output.status.success() => {
			let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
			let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
			Ok(Some(if stdout.is_empty() { stderr } else { stdout }))
		}
		Ok(output) => Err(format!(
			"duckdb --version failed status={} stderr={:?}",
			output.status,
			String::from_utf8_lossy(&output.stderr)
		)),
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
		Err(error) => Err(format!("spawn duckdb --version: {error}")),
	}
}

fn run_polars_perf_query_with_data_source(
	query: &str,
	mode: &str,
	sf: f64,
	run_index: u32,
	warmup: bool,
	data_source: &str,
	data_dir: &Path,
) -> Result<PerfAttempt, String> {
	let export_root = PathBuf::from(CACHE_ROOT).join("parquet");
	let export = match data_source {
		"memory" => export_perf_query_parquet(query, mode, sf, &export_root)?,
		"parquet" | "persisted" => export_persisted_perf_query_parquet(query, mode, sf, data_dir)?,
		other => return Err(format!("perf --data-source {other:?} is not supported")),
	};
	Ok(run_polars_perf_export(&export, run_index, warmup))
}

fn polars_q03_value_gate_command(args: &[String]) -> ExitCode {
	let sf = match explicit_sf(args) {
		Ok(explicit) => explicit.unwrap_or(1.0),
		Err(message) => return harness_error(&message),
	};
	let mode = flag_value(args, "--mode").unwrap_or("strict");
	let data_source = flag_value(args, "--data-source").unwrap_or("parquet");
	let data_dir = data_dir_from_args(args);
	let row = match run_polars_q03_value_gate(sf, mode, data_source, &data_dir) {
		Ok(row) => row,
		Err(message) => return harness_error(&message),
	};
	println!("{row}");
	ExitCode::SUCCESS
}

#[cfg(not(feature = "polars-sql-reference"))]
fn run_polars_q03_value_gate(
	_scale_factor: f64,
	_mode: &str,
	_data_source: &str,
	_data_dir: &Path,
) -> Result<serde_json::Value, String> {
	Err("polars-q03-value-gate requires --features polars-sql-reference".to_string())
}

#[cfg(feature = "polars-sql-reference")]
fn run_polars_q03_value_gate(
	scale_factor: f64,
	mode: &str,
	data_source: &str,
	data_dir: &Path,
) -> Result<serde_json::Value, String> {
	if mode != "strict" {
		return Err(format!(
			"polars-q03-value-gate supports only --mode strict; got {mode:?}"
		));
	}
	let export_root = PathBuf::from(CACHE_ROOT).join("parquet");
	let export = match data_source {
		"memory" => export_perf_query_parquet("q03", mode, scale_factor, &export_root)?,
		"parquet" | "persisted" => {
			export_persisted_perf_query_parquet("q03", mode, scale_factor, data_dir)?
		}
		other => return Err(format!("--data-source {other:?} is not supported")),
	};
	let frame = collect_polars_sql_frame(&export)?;
	let oracle = toron_tpch::strict_q3_oracle(scale_factor);
	if oracle.selected_rows < 1 || oracle.groups.len() <= 1 {
		return Err(format!(
			"oracle non-vacuous guard failed: selected_rows={} groups={}",
			oracle.selected_rows,
			oracle.groups.len()
		));
	}
	let stats = compare_polars_q03_frame(&frame, &oracle.groups)?;
	Ok(serde_json::json!({
		"status": "pass",
		"engine": "polars_sql",
		"engine_version": polars::VERSION,
		"query": "q03",
		"scale_factor": scale_factor,
		"mode": mode,
		"data_source": data_source,
		"rows": stats.rows,
		"max_revenue_diff_raw_scale4": stats.max_revenue_diff_raw,
		"revenue_tolerance_raw_scale4": POLARS_Q03_REVENUE_TOLERANCE_RAW,
		"plan_fingerprint": polars_plan_fingerprint(&export),
		"schema_fingerprint": stable_fingerprint(&polars_schema_signature(&frame)),
		"value_gate": "oracle_decimal_tolerance_and_q03_order",
	}))
}

fn duckdb_not_attempted_row(
	query: &str,
	mode: &str,
	sf: f64,
	run_index: u32,
	warmup: bool,
	reason: &str,
	duckdb_version: Option<String>,
) -> serde_json::Value {
	PerfAttempt {
		engine: "duckdb".to_string(),
		query: query.to_string(),
		scale_factor: sf,
		mode: mode.to_string(),
		run_index,
		warmup,
		phase: Phase::Generate,
		status: Status::NotAttempted,
		reason: Some(reason.to_string()),
		diagnostic_message: None,
		timings: PerfPhaseTimings::default(),
		parent_wall_ms: None,
		peak_rss_kib: None,
		spill_bytes: 0,
		plan_fingerprint: None,
		digest: None,
		output_row_count: None,
		output_schema_fingerprint: None,
		engine_version: duckdb_version.clone(),
		duckdb_version,
		debug_build: false,
		local_join_admissions: None,
		targeted_local_join_admissions: None,
		join_input_replay_ledger: None,
		join_input_total_concats: None,
		join_spill_observations: None,
		partition_work_concurrency_observations: None,
		dataset_verification: None,
		dataset_manifest_fingerprint: None,
	}
	.json_value()
}

#[cfg(test)]
fn polars_not_attempted_row(
	query: &str,
	mode: &str,
	sf: f64,
	run_index: u32,
	warmup: bool,
	reason: &str,
	engine_version: Option<String>,
) -> serde_json::Value {
	PerfAttempt {
		engine: "polars_sql".to_string(),
		query: query.to_string(),
		scale_factor: sf,
		mode: mode.to_string(),
		run_index,
		warmup,
		phase: Phase::Generate,
		status: Status::NotAttempted,
		reason: Some(reason.to_string()),
		diagnostic_message: None,
		timings: PerfPhaseTimings::default(),
		parent_wall_ms: None,
		peak_rss_kib: None,
		spill_bytes: 0,
		plan_fingerprint: None,
		digest: None,
		output_row_count: None,
		output_schema_fingerprint: None,
		engine_version,
		duckdb_version: None,
		debug_build: false,
		local_join_admissions: None,
		targeted_local_join_admissions: None,
		join_input_replay_ledger: None,
		join_input_total_concats: None,
		join_spill_observations: None,
		partition_work_concurrency_observations: None,
		dataset_verification: None,
		dataset_manifest_fingerprint: None,
	}
	.json_value()
}

/// Push a measured row into the cell and fsync it to the journal first, so the row survives a
/// process that dies before the sweep can write its artifacts.
fn journal_and_collect(
	journal: &Option<PerfSampleJournal>,
	cell_rows: &mut Vec<serde_json::Value>,
	row: serde_json::Value,
) -> Result<(), String> {
	if let Some(journal) = journal {
		journal.append(&row)?;
	}
	cell_rows.push(row);
	Ok(())
}

/// A cell that produced no measurement, recorded as a row rather than aborting the sweep.
///
/// `--query all` has to survive both a query that cannot run at this scale and one whose child
/// died. Discarding every other cell's work to report one bad cell is the opposite of what a board
/// is for, and it is what pushes measurement back out into shell loops. The status carries the
/// distinction: `not_attempted` for out-of-scope, `fail` for a cell that should have run and did not.
struct PerfStubRow<'a> {
	engine: &'a str,
	query: &'a str,
	mode: &'a str,
	scale_factor: f64,
	status: Status,
	reason: &'a str,
	message: Option<String>,
}

fn perf_stub_row(stub: PerfStubRow<'_>) -> serde_json::Value {
	PerfAttempt {
		engine: stub.engine.to_string(),
		query: stub.query.to_string(),
		scale_factor: stub.scale_factor,
		mode: stub.mode.to_string(),
		run_index: 0,
		warmup: false,
		phase: Phase::Generate,
		status: stub.status,
		reason: Some(stub.reason.to_string()),
		diagnostic_message: stub.message,
		timings: PerfPhaseTimings::default(),
		parent_wall_ms: None,
		peak_rss_kib: None,
		spill_bytes: 0,
		plan_fingerprint: None,
		digest: None,
		output_row_count: None,
		output_schema_fingerprint: None,
		engine_version: None,
		duckdb_version: None,
		debug_build: cfg!(debug_assertions),
		local_join_admissions: None,
		targeted_local_join_admissions: None,
		join_input_replay_ledger: None,
		join_input_total_concats: None,
		join_spill_observations: None,
		partition_work_concurrency_observations: None,
		dataset_verification: None,
		dataset_manifest_fingerprint: None,
	}
	.json_value()
}

/// ADR 473: engine thread count is a first-class ladder condition — a comparison
/// that does not record it is not a roofline.
fn duckdb_thread_setting() -> Option<String> {
	reference_thread_setting(std::env::var("TORON_TPCH_DUCKDB_THREADS"))
}

/// ADR 603: the DuckDB memory-limit coordinate as the header and DuckDB rows record it: the
/// normalized request (null when none) and its provenance. A malformed request was refused at
/// start, so an unreadable coordinate here records nothing rather than a default.
fn duckdb_memory_limit_fields() -> (serde_json::Value, serde_json::Value) {
	match toron_tpch::duckdb_memory::memory_limit_coordinate() {
		Ok(coordinate) => (
			serde_json::json!(coordinate.requested.map(|limit| limit.normalized)),
			serde_json::json!(coordinate.provenance),
		),
		Err(_) => (serde_json::Value::Null, serde_json::Value::Null),
	}
}

fn polars_max_threads_setting() -> Option<String> {
	reference_thread_setting(std::env::var("POLARS_MAX_THREADS"))
}

fn reference_thread_setting(raw: Result<String, std::env::VarError>) -> Option<String> {
	raw.ok().filter(|value| !value.is_empty())
}

/// Observational record of the reference-engine thread pins a perf row ran
/// under. An unset pin means the engine chose its own machine-dependent
/// parallelism; the row says so instead of leaving the condition unverifiable.
fn reference_thread_pin_provenance(setting: &Option<String>) -> &'static str {
	if setting.is_some() {
		"explicit"
	} else {
		"engine_default"
	}
}

/// A primitive-ladder row compares Toron's one-task coordinate against a one-thread reference.
/// Do not silently fall back to DuckDB's machine-dependent default parallelism: the recorded
/// provenance must be the literal one-thread setting before a ladder can run its DuckDB arm.
fn require_one_thread_duckdb_reference(engines: &[PerfEngine]) -> Result<(), String> {
	if !engines.contains(&PerfEngine::DuckDb) {
		return Ok(());
	}
	require_one_thread_duckdb_setting(duckdb_thread_setting().as_deref())
}

fn require_one_thread_duckdb_setting(setting: Option<&str>) -> Result<(), String> {
	match setting {
		Some("1") => Ok(()),
		Some(value) => Err(format!(
			"primitive ladder requires TORON_TPCH_DUCKDB_THREADS=1 for its DuckDB reference; found {value:?}"
		)),
		None => Err(
			"primitive ladder requires TORON_TPCH_DUCKDB_THREADS=1 for its DuckDB reference; it is unset"
				.to_string(),
		),
	}
}

fn duckdb_setup_sql(export: &toron_tpch::TpchParquetExport) -> String {
	let threads = duckdb_thread_setting()
		.map(|threads| format!("SET threads={threads};\n"))
		.unwrap_or_default();
	// ADR 603: an explicit memory limit follows the thread pin; absent, DuckDB keeps its default.
	let memory_limit = toron_tpch::duckdb_memory::memory_limit_coordinate()
		.ok()
		.and_then(|coordinate| coordinate.requested)
		.map(|limit| format!("SET memory_limit='{}';\n", limit.normalized))
		.unwrap_or_default();
	let views = export
		.tables
		.iter()
		.map(|table| {
			format!(
				"CREATE OR REPLACE VIEW {} AS SELECT * FROM read_parquet({});",
				table.name,
				sql_string_literal(&table.path.display().to_string())
			)
		})
		.collect::<Vec<_>>()
		.join("\n");
	format!("{threads}{memory_limit}{views}")
}

fn duckdb_query_statement(sql: &str) -> String {
	format!("{};", sql.trim().trim_end_matches(';'))
}

fn sql_string_literal(value: &str) -> String {
	format!("'{}'", value.replace('\'', "''"))
}

fn duckdb_plan_fingerprint(export: &toron_tpch::TpchParquetExport) -> String {
	stable_fingerprint(&format!(
		"duckdb_sql={};manifest={}",
		export.sql, export.manifest_fingerprint
	))
}

fn polars_plan_fingerprint(export: &toron_tpch::TpchParquetExport) -> String {
	let sql = polars_sql_text(export);
	stable_fingerprint(&format!(
		"polars_sql={};manifest={}",
		sql, export.manifest_fingerprint
	))
}

const POLARS_Q03_SQL: &str = "SELECT l_orderkey, \
	SUM(l_extendedprice * (1 - l_discount)) AS revenue, \
	o_orderdate, o_shippriority \
	FROM customer \
	JOIN orders ON c_custkey = o_custkey \
	JOIN lineitem ON l_orderkey = o_orderkey \
	WHERE c_mktsegment = 'BUILDING' \
	  AND o_orderdate < DATE '1995-03-15' \
	  AND l_shipdate > DATE '1995-03-15' \
	GROUP BY l_orderkey, o_orderdate, o_shippriority \
	ORDER BY revenue DESC, o_orderdate";

const POLARS_Q14_SQL: &str = "SELECT 100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%' \
		THEN l_extendedprice * (1 - l_discount) ELSE 0 END) \
	/ SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue \
	FROM lineitem \
	JOIN part ON l_partkey = p_partkey \
	WHERE l_shipdate >= DATE '1995-09-01' AND l_shipdate < DATE '1995-10-01'";

const POLARS_Q19_SQL: &str = "SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue \
	FROM lineitem \
	JOIN part ON p_partkey = l_partkey \
	WHERE (p_brand = 'Brand#12' \
	    AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') \
	    AND l_quantity >= 1 \
	    AND l_quantity <= 11 \
	    AND p_size BETWEEN 1 AND 5 \
	    AND l_shipmode IN ('AIR', 'AIR REG') \
	    AND l_shipinstruct = 'DELIVER IN PERSON') \
	   OR (p_brand = 'Brand#23' \
	    AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') \
	    AND l_quantity >= 10 \
	    AND l_quantity <= 20 \
	    AND p_size BETWEEN 1 AND 10 \
	    AND l_shipmode IN ('AIR', 'AIR REG') \
	    AND l_shipinstruct = 'DELIVER IN PERSON') \
	   OR (p_brand = 'Brand#34' \
	    AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') \
	    AND l_quantity >= 20 \
	    AND l_quantity <= 30 \
	    AND p_size BETWEEN 1 AND 15 \
	    AND l_shipmode IN ('AIR', 'AIR REG') \
	    AND l_shipinstruct = 'DELIVER IN PERSON')";

fn polars_sql_text(export: &toron_tpch::TpchParquetExport) -> Cow<'_, str> {
	match export.query_id.as_str() {
		"q03" => Cow::Borrowed(POLARS_Q03_SQL),
		"q14" => Cow::Borrowed(POLARS_Q14_SQL),
		"q19" => Cow::Borrowed(POLARS_Q19_SQL),
		_ => Cow::Borrowed(&export.sql),
	}
}

#[cfg(not(feature = "polars-sql-reference"))]
fn run_polars_perf_export(
	export: &toron_tpch::TpchParquetExport,
	run_index: u32,
	warmup: bool,
) -> PerfAttempt {
	PerfAttempt {
		engine: "polars_sql".to_string(),
		query: export.query_id.clone(),
		scale_factor: export.scale_factor,
		mode: export.mode.clone(),
		run_index,
		warmup,
		phase: Phase::Generate,
		status: Status::NotAttempted,
		reason: Some("polars_absent".to_string()),
		diagnostic_message: Some(
			"toron-tpch was built without --features polars-sql-reference".to_string(),
		),
		timings: PerfPhaseTimings::default(),
		parent_wall_ms: None,
		peak_rss_kib: None,
		spill_bytes: 0,
		plan_fingerprint: Some(polars_plan_fingerprint(export)),
		digest: None,
		output_row_count: None,
		output_schema_fingerprint: None,
		engine_version: None,
		duckdb_version: None,
		debug_build: cfg!(debug_assertions),
		local_join_admissions: None,
		targeted_local_join_admissions: None,
		join_input_replay_ledger: None,
		join_input_total_concats: None,
		join_spill_observations: None,
		partition_work_concurrency_observations: None,
		dataset_verification: None,
		dataset_manifest_fingerprint: None,
	}
}

#[cfg(feature = "polars-sql-reference")]
#[derive(Debug, Clone)]
struct PolarsReferenceError {
	reason: &'static str,
	message: String,
}

#[cfg(feature = "polars-sql-reference")]
impl PolarsReferenceError {
	fn new(reason: &'static str, message: impl Into<String>) -> Self {
		Self {
			reason,
			message: message.into(),
		}
	}
}

#[cfg(feature = "polars-sql-reference")]
fn run_polars_perf_export(
	export: &toron_tpch::TpchParquetExport,
	run_index: u32,
	warmup: bool,
) -> PerfAttempt {
	use polars_sql::SQLContext;

	let plan_fingerprint = polars_plan_fingerprint(export);
	let engine_version = Some(polars::VERSION.to_string());
	let mut context = SQLContext::new();
	if let Err(error) = register_polars_reference_tables(export, &mut context) {
		return polars_fail_attempt(
			export,
			&plan_fingerprint,
			run_index,
			warmup,
			error.reason,
			&error.message,
			engine_version,
		);
	}

	let polars_sql = polars_sql_text(export);
	let lazy = match execute_polars_sql(&mut context, polars_sql.as_ref()) {
		Ok(lazy) => lazy,
		Err(message) if polars_error_is_known_unsupported(&message) => {
			return polars_unsupported_attempt(
				export,
				&plan_fingerprint,
				run_index,
				warmup,
				&message,
				engine_version,
			);
		}
		Err(message) => {
			return polars_fail_attempt(
				export,
				&plan_fingerprint,
				run_index,
				warmup,
				"polars_execute_failed",
				&message,
				engine_version,
			);
		}
	};
	let start = Instant::now();
	let frame = match collect_polars_lazy_frame(lazy) {
		Ok(frame) => frame,
		Err(message) if polars_error_is_known_unsupported(&message) => {
			return polars_unsupported_attempt(
				export,
				&plan_fingerprint,
				run_index,
				warmup,
				&message,
				engine_version,
			);
		}
		Err(message) => {
			return polars_fail_attempt(
				export,
				&plan_fingerprint,
				run_index,
				warmup,
				"polars_collect_failed",
				&message,
				engine_version,
			);
		}
	};
	let execute_ms = start.elapsed().as_secs_f64() * 1000.0;
	let mut timings = PerfPhaseTimings::default();
	timings.execute_ms = execute_ms;
	timings.total_ms = execute_ms;
	PerfAttempt {
		engine: "polars_sql".to_string(),
		query: export.query_id.clone(),
		scale_factor: export.scale_factor,
		mode: export.mode.clone(),
		run_index,
		warmup,
		phase: Phase::Execute,
		status: Status::Pass,
		reason: None,
		diagnostic_message: None,
		timings,
		parent_wall_ms: None,
		peak_rss_kib: None,
		spill_bytes: 0,
		plan_fingerprint: Some(plan_fingerprint),
		digest: None,
		output_row_count: Some(frame.height() as u64),
		output_schema_fingerprint: Some(stable_fingerprint(&polars_schema_signature(&frame))),
		engine_version,
		duckdb_version: None,
		debug_build: cfg!(debug_assertions),
		local_join_admissions: None,
		targeted_local_join_admissions: None,
		join_input_replay_ledger: None,
		join_input_total_concats: None,
		join_spill_observations: None,
		partition_work_concurrency_observations: None,
		dataset_verification: export.dataset_verification,
		dataset_manifest_fingerprint: export
			.dataset_verification
			.map(|_| export.manifest_fingerprint.clone()),
	}
}

#[cfg(feature = "polars-sql-reference")]
fn polars_schema_signature(frame: &polars::prelude::DataFrame) -> String {
	frame
		.schema()
		.iter()
		.map(|(name, dtype)| format!("{name}:{dtype:?}"))
		.collect::<Vec<_>>()
		.join("|")
}

#[cfg(feature = "polars-sql-reference")]
fn collect_polars_sql_frame(
	export: &toron_tpch::TpchParquetExport,
) -> Result<polars::prelude::DataFrame, String> {
	use polars_sql::SQLContext;

	let mut context = SQLContext::new();
	register_polars_reference_tables(export, &mut context).map_err(|error| error.message)?;
	let polars_sql = polars_sql_text(export);
	let lazy = execute_polars_sql(&mut context, polars_sql.as_ref())?;
	collect_polars_lazy_frame(lazy).map_err(|error| format!("polars collect failed: {error}"))
}

#[cfg(feature = "polars-sql-reference")]
fn register_polars_reference_tables(
	export: &toron_tpch::TpchParquetExport,
	context: &mut polars_sql::SQLContext,
) -> Result<(), PolarsReferenceError> {
	use polars::prelude::*;

	for table in &export.tables {
		let Some(path) = table.path.to_str() else {
			return Err(PolarsReferenceError::new(
				"polars_invalid_path",
				format!("non-utf8 parquet path {}", table.path.display()),
			));
		};
		let lazy = LazyFrame::scan_parquet(path.into(), Default::default()).map_err(|error| {
			PolarsReferenceError::new(
				"polars_scan_failed",
				format!(
					"polars scan failed for {} ({}): {error:?}",
					table.name,
					table.path.display()
				),
			)
		})?;
		context.register(&table.name, lazy);
	}
	Ok(())
}

#[cfg(feature = "polars-sql-reference")]
fn execute_polars_sql(
	context: &mut polars_sql::SQLContext,
	sql: &str,
) -> Result<polars::prelude::LazyFrame, String> {
	match catch_polars_unwind("polars SQL execute", || context.execute(sql)) {
		Ok(Ok(lazy)) => Ok(lazy),
		Ok(Err(error)) => Err(format!("polars SQL execute failed: {error:?}")),
		Err(message) => Err(message),
	}
}

#[cfg(feature = "polars-sql-reference")]
fn collect_polars_lazy_frame(
	lazy: polars::prelude::LazyFrame,
) -> Result<polars::prelude::DataFrame, String> {
	match catch_polars_unwind("polars collect", || lazy.collect()) {
		Ok(Ok(frame)) => Ok(frame),
		Ok(Err(error)) => Err(format!("{error:?}")),
		Err(message) => Err(message),
	}
}

#[cfg(feature = "polars-sql-reference")]
static POLARS_PANIC_HOOK_LOCK: std::sync::OnceLock<std::sync::Mutex<()>> =
	std::sync::OnceLock::new();

#[cfg(feature = "polars-sql-reference")]
fn catch_polars_unwind<T, F>(label: &str, f: F) -> Result<T, String>
where
	F: FnOnce() -> T,
{
	let hook_lock = POLARS_PANIC_HOOK_LOCK
		.get_or_init(|| std::sync::Mutex::new(()))
		.lock()
		.unwrap_or_else(|poisoned| poisoned.into_inner());
	let current_thread = std::thread::current().id();
	let previous_hook = std::sync::Arc::new(std::panic::take_hook());
	let previous_hook_for_delegate = std::sync::Arc::clone(&previous_hook);
	std::panic::set_hook(Box::new(move |info| {
		if std::thread::current().id() != current_thread {
			(previous_hook_for_delegate.as_ref())(info);
		}
	}));
	let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
	let installed_hook = std::panic::take_hook();
	drop(installed_hook);
	let previous_hook = std::sync::Arc::try_unwrap(previous_hook).unwrap_or_else(|previous_hook| {
		Box::new(move |info| {
			(previous_hook.as_ref())(info);
		})
	});
	std::panic::set_hook(previous_hook);
	drop(hook_lock);
	match result {
		Ok(value) => Ok(value),
		Err(payload) => Err(format!(
			"{label} panic: {}",
			polars_panic_payload_message(&payload)
		)),
	}
}

#[cfg(feature = "polars-sql-reference")]
fn polars_panic_payload_message(payload: &(dyn std::any::Any + Send)) -> String {
	if let Some(message) = payload.downcast_ref::<&str>() {
		(*message).to_string()
	} else if let Some(message) = payload.downcast_ref::<String>() {
		message.clone()
	} else {
		"non-string panic payload".to_string()
	}
}

#[cfg(feature = "polars-sql-reference")]
const POLARS_Q03_REVENUE_TOLERANCE_RAW: i128 = 1_000;

#[cfg(feature = "polars-sql-reference")]
struct PolarsQ03ValueGateStats {
	rows: usize,
	max_revenue_diff_raw: i128,
}

#[cfg(feature = "polars-sql-reference")]
fn compare_polars_q03_frame(
	frame: &polars::prelude::DataFrame,
	oracle: &BTreeMap<i64, toron_tpch::Q3Group>,
) -> Result<PolarsQ03ValueGateStats, String> {
	let orderkeys = frame
		.column("l_orderkey")
		.map_err(|error| format!("missing l_orderkey: {error:?}"))?
		.i64()
		.map_err(|error| format!("l_orderkey not Int64: {error:?}"))?;
	let revenues = frame
		.column("revenue")
		.map_err(|error| format!("missing revenue: {error:?}"))?;
	let orderdates = frame
		.column("o_orderdate")
		.map_err(|error| format!("missing o_orderdate: {error:?}"))?;
	let shippriorities = frame
		.column("o_shippriority")
		.map_err(|error| format!("missing o_shippriority: {error:?}"))?
		.i32()
		.map_err(|error| format!("o_shippriority not Int32: {error:?}"))?;

	let mut emitted = Vec::with_capacity(frame.height());
	for i in 0..frame.height() {
		let orderkey = orderkeys
			.get(i)
			.ok_or_else(|| format!("NULL l_orderkey at row {i}"))?;
		let revenue = polars_revenue_raw_scale4(revenues, i)?;
		let orderdate = polars_date32_value(orderdates, i)?;
		let shippriority = shippriorities
			.get(i)
			.ok_or_else(|| format!("NULL o_shippriority at row {i}"))?;
		emitted.push((orderkey, revenue, orderdate, shippriority));
	}

	if emitted.len() != oracle.len() {
		return Err(format!(
			"row count mismatch: oracle has {} groups, polars_sql has {} rows",
			oracle.len(),
			emitted.len()
		));
	}
	let mut seen = BTreeMap::new();
	let mut max_revenue_diff_raw = 0i128;
	for &(orderkey, revenue, orderdate, shippriority) in &emitted {
		if seen
			.insert(orderkey, (revenue, orderdate, shippriority))
			.is_some()
		{
			return Err(format!("duplicate l_orderkey {orderkey}"));
		}
	}
	for (orderkey, group) in oracle {
		let Some(&(revenue, orderdate, shippriority)) = seen.get(orderkey) else {
			return Err(format!(
				"oracle l_orderkey {orderkey} missing from polars_sql"
			));
		};
		let diff = (revenue - group.revenue).abs();
		max_revenue_diff_raw = max_revenue_diff_raw.max(diff);
		if diff > POLARS_Q03_REVENUE_TOLERANCE_RAW {
			return Err(format!(
				"l_orderkey {orderkey}: revenue mismatch beyond tolerance: expected {} got {revenue} diff {diff} tolerance {}",
				group.revenue, POLARS_Q03_REVENUE_TOLERANCE_RAW
			));
		}
		if orderdate != group.orderdate {
			return Err(format!(
				"l_orderkey {orderkey}: o_orderdate mismatch: expected {}, got {orderdate}",
				group.orderdate
			));
		}
		if shippriority != group.shippriority {
			return Err(format!(
				"l_orderkey {orderkey}: o_shippriority mismatch: expected {}, got {shippriority}",
				group.shippriority
			));
		}
	}

	for pair in emitted.windows(2) {
		let (_, rev_a, date_a, _) = pair[0];
		let (_, rev_b, date_b, _) = pair[1];
		if rev_a + POLARS_Q03_REVENUE_TOLERANCE_RAW < rev_b {
			return Err(format!(
				"ORDER BY revenue DESC violated: {rev_a} emitted before {rev_b}"
			));
		}
		if rev_a == rev_b && date_a > date_b {
			return Err(format!(
				"ORDER BY o_orderdate ASC violated within equal revenue: {date_a} emitted before {date_b}"
			));
		}
	}

	Ok(PolarsQ03ValueGateStats {
		rows: emitted.len(),
		max_revenue_diff_raw,
	})
}

#[cfg(feature = "polars-sql-reference")]
fn polars_date32_value(column: &polars::prelude::Column, row: usize) -> Result<i32, String> {
	use polars::prelude::DataType;

	match column.dtype() {
		DataType::Date => column
			.date()
			.map_err(|error| format!("o_orderdate not Date: {error:?}"))?
			.physical()
			.get(row)
			.ok_or_else(|| format!("NULL o_orderdate at row {row}")),
		DataType::Int32 => column
			.i32()
			.map_err(|error| format!("o_orderdate not Int32: {error:?}"))?
			.get(row)
			.ok_or_else(|| format!("NULL o_orderdate at row {row}")),
		other => Err(format!("o_orderdate has unsupported dtype {other:?}")),
	}
}

#[cfg(feature = "polars-sql-reference")]
fn polars_revenue_raw_scale4(column: &polars::prelude::Column, row: usize) -> Result<i128, String> {
	use polars::prelude::DataType;

	match column.dtype() {
		DataType::Decimal(_, scale) => {
			let raw = column
				.decimal()
				.map_err(|error| format!("revenue not Decimal: {error:?}"))?
				.physical()
				.get(row)
				.ok_or_else(|| format!("NULL revenue at row {row}"))?;
			Ok(normalize_decimal_raw_to_scale4(raw, *scale))
		}
		DataType::Float64 => {
			let value = column
				.f64()
				.map_err(|error| format!("revenue not Float64: {error:?}"))?
				.get(row)
				.ok_or_else(|| format!("NULL revenue at row {row}"))?;
			Ok((value * 10_000.0).round() as i128)
		}
		DataType::Float32 => {
			let value = column
				.f32()
				.map_err(|error| format!("revenue not Float32: {error:?}"))?
				.get(row)
				.ok_or_else(|| format!("NULL revenue at row {row}"))?;
			Ok((f64::from(value) * 10_000.0).round() as i128)
		}
		other => Err(format!("revenue has unsupported dtype {other:?}")),
	}
}

#[cfg(feature = "polars-sql-reference")]
fn normalize_decimal_raw_to_scale4(value: i128, scale: usize) -> i128 {
	match scale.cmp(&4) {
		std::cmp::Ordering::Equal => value,
		std::cmp::Ordering::Less => value * pow10_i128(4 - scale),
		std::cmp::Ordering::Greater => {
			let divisor = pow10_i128(scale - 4);
			let half = divisor / 2;
			if value >= 0 {
				(value + half) / divisor
			} else {
				(value - half) / divisor
			}
		}
	}
}

#[cfg(feature = "polars-sql-reference")]
fn pow10_i128(exp: usize) -> i128 {
	(0..exp).fold(1i128, |acc, _| acc * 10)
}

#[cfg(any(feature = "polars-sql-reference", test))]
fn polars_unsupported_attempt(
	export: &toron_tpch::TpchParquetExport,
	plan_fingerprint: &str,
	run_index: u32,
	warmup: bool,
	message: &str,
	engine_version: Option<String>,
) -> PerfAttempt {
	let error_class = classify_polars_error(message);
	let reason = format!("polars_sql_unsupported_{error_class}");
	PerfAttempt {
		engine: "polars_sql".to_string(),
		query: export.query_id.clone(),
		scale_factor: export.scale_factor,
		mode: export.mode.clone(),
		run_index,
		warmup,
		phase: Phase::Execute,
		status: Status::NotAttempted,
		reason: Some(reason),
		diagnostic_message: Some(message.to_string()),
		timings: PerfPhaseTimings::default(),
		parent_wall_ms: None,
		peak_rss_kib: None,
		spill_bytes: 0,
		plan_fingerprint: Some(plan_fingerprint.to_string()),
		digest: None,
		output_row_count: None,
		output_schema_fingerprint: None,
		engine_version,
		duckdb_version: None,
		debug_build: cfg!(debug_assertions),
		local_join_admissions: None,
		targeted_local_join_admissions: None,
		join_input_replay_ledger: None,
		join_input_total_concats: None,
		join_spill_observations: None,
		partition_work_concurrency_observations: None,
		dataset_verification: export.dataset_verification,
		dataset_manifest_fingerprint: export
			.dataset_verification
			.map(|_| export.manifest_fingerprint.clone()),
	}
}

#[cfg(any(feature = "polars-sql-reference", test))]
fn polars_error_is_known_unsupported(message: &str) -> bool {
	matches!(
		classify_polars_error(message),
		"correlated_subquery"
			| "interval_syntax"
			| "join_constraint"
			| "from_clause_dialect"
			| "index_limit"
	)
}

#[cfg(feature = "polars-sql-reference")]
fn polars_fail_attempt(
	export: &toron_tpch::TpchParquetExport,
	plan_fingerprint: &str,
	run_index: u32,
	warmup: bool,
	reason: &str,
	message: &str,
	engine_version: Option<String>,
) -> PerfAttempt {
	PerfAttempt {
		engine: "polars_sql".to_string(),
		query: export.query_id.clone(),
		scale_factor: export.scale_factor,
		mode: export.mode.clone(),
		run_index,
		warmup,
		phase: Phase::Execute,
		status: Status::Fail,
		reason: Some(reason.to_string()),
		diagnostic_message: Some(message.to_string()),
		timings: PerfPhaseTimings::default(),
		parent_wall_ms: None,
		peak_rss_kib: None,
		spill_bytes: 0,
		plan_fingerprint: Some(plan_fingerprint.to_string()),
		digest: None,
		output_row_count: None,
		output_schema_fingerprint: None,
		engine_version,
		duckdb_version: None,
		debug_build: cfg!(debug_assertions),
		local_join_admissions: None,
		join_spill_observations: None,
		partition_work_concurrency_observations: None,
		dataset_verification: export.dataset_verification,
		dataset_manifest_fingerprint: export
			.dataset_verification
			.map(|_| export.manifest_fingerprint.clone()),
	}
}

#[cfg(any(feature = "polars-sql-reference", test))]
fn classify_polars_error(message: &str) -> &'static str {
	let lower = message.to_ascii_lowercase();
	if lower.contains("panic") || lower.contains("panicked") {
		return "other";
	}
	// Polars hedges its capability messages — "not *currently* supported" does not contain the
	// substring "not supported", which is why whole classes of dialect gap were landing as engine
	// failures rather than as cells the peer cannot express.
	let explicitly_unsupported = lower.contains("not supported")
		|| lower.contains("unsupported")
		|| lower.contains("not currently supported");
	if explicitly_unsupported
		&& (lower.contains("subquer") || lower.contains("correlated") || lower.contains("exists"))
	{
		"correlated_subquery"
	} else if explicitly_unsupported && lower.contains("interval") {
		"interval_syntax"
	} else if explicitly_unsupported
		&& (lower.contains("not like")
			|| lower.contains("join constraint")
			|| lower.contains("non-equi join"))
		// Stated as a positive limit rather than a negation, so it needs its own pattern.
		|| lower.contains("only equi-join constraints")
	{
		"join_constraint"
	} else if explicitly_unsupported && lower.contains("multiple tables in from clause") {
		// The comma-join form the TPC-H texts use. A dialect gap in the peer, not a failed run.
		"from_clause_dialect"
	} else if lower.contains("2^32") || lower.contains("2**32") || lower.contains("index limit") {
		"index_limit"
	} else {
		"other"
	}
}

fn duckdb_type_parity_gate(
	duckdb_bin: &str,
	export: &toron_tpch::TpchParquetExport,
) -> Result<(), String> {
	for table in &export.tables {
		let script = format!(
			".mode json\nDESCRIBE SELECT * FROM read_parquet({});\nSELECT COUNT(*) AS row_count FROM read_parquet({});\n",
			sql_string_literal(&table.path.display().to_string()),
			sql_string_literal(&table.path.display().to_string())
		);
		let output = run_duckdb_script(duckdb_bin, &script)
			.map_err(|error| format!("DuckDB type gate for {}: {error}", table.name))?;
		if !output.status_success {
			return Err(format!(
				"DuckDB type gate for {} failed: stderr={:?}",
				table.name, output.stderr
			));
		}
		let arrays = extract_json_arrays(&output.stdout);
		let describe = arrays
			.first()
			.ok_or_else(|| format!("DuckDB DESCRIBE for {} emitted no JSON", table.name))?;
		let count = arrays
			.get(1)
			.ok_or_else(|| format!("DuckDB COUNT for {} emitted no JSON", table.name))?;
		let observed_schema = duckdb_describe_schema_fingerprint(describe)?;
		let observed_count = duckdb_count_result(count)?;
		if observed_count != table.row_count {
			return Err(format!(
				"DuckDB row-count mismatch for {}: manifest={} duckdb={observed_count}",
				table.name, table.row_count
			));
		}
		if observed_schema != table.schema_fingerprint {
			return Err(format!(
				"DuckDB schema mismatch for {}: manifest={} duckdb={observed_schema}",
				table.name, table.schema_fingerprint
			));
		}
	}
	Ok(())
}

fn duckdb_output_schema_fingerprint(
	duckdb_bin: &str,
	export: &toron_tpch::TpchParquetExport,
) -> Result<String, String> {
	let script = format!(
		".mode json\n{}\nDESCRIBE SELECT * FROM ({}) AS toron_tpch_output;\n",
		duckdb_setup_sql(export),
		export.sql
	);
	let output = run_duckdb_script(duckdb_bin, &script)
		.map_err(|error| format!("DuckDB output schema: {error}"))?;
	if !output.status_success {
		return Err(format!(
			"DuckDB output schema failed: stderr={:?}",
			output.stderr
		));
	}
	let arrays = extract_json_arrays(&output.stdout);
	let describe = arrays
		.first()
		.ok_or_else(|| "DuckDB output schema emitted no JSON".to_string())?;
	duckdb_describe_schema_fingerprint(describe)
}

fn run_duckdb_perf_child(
	duckdb_bin: &str,
	duckdb_version: &str,
	export: &toron_tpch::TpchParquetExport,
	plan_fingerprint: &str,
	output_schema_fingerprint: &str,
	run_index: u32,
	warmup: bool,
) -> Result<serde_json::Value, String> {
	// ADR 603: under an explicit memory limit each child gets its own fresh temp directory, so no
	// cell can see another's files; the read-back runs before the timer, outside the timed query.
	let memory = toron_tpch::duckdb_memory::memory_limit_coordinate()?;
	let temp_directory = match &memory.requested {
		Some(_) => Some(fresh_duckdb_temp_directory()?),
		None => None,
	};
	let script = format!(
		".mode json\n{}{}\n{DUCKDB_MEMORY_LIMIT_READ_BACK}\n.timer on\n{}\n",
		duckdb_setup_sql(export),
		temp_directory
			.as_deref()
			.map(duckdb_temp_directory_sql)
			.unwrap_or_default(),
		duckdb_query_statement(&export.sql)
	);
	let output = run_duckdb_script(duckdb_bin, &script);
	if let Some(directory) = &temp_directory {
		let _ = std::fs::remove_dir_all(directory);
	}
	let output = output?;
	let arrays = extract_json_arrays(&output.stdout);
	let read_back = duckdb_memory_limit_read_back(&arrays);
	// A successful child always read the setting back, under the default limit too: the anchor
	// reports the default DuckDB actually applied.
	let read_back_mismatch = match (&memory.requested, &read_back) {
		(Some(requested), Some(observed)) => {
			!toron_tpch::duckdb_memory::read_back_agrees(requested, observed)
		}
		(_, None) => output.status_success,
		(None, Some(_)) => false,
	};
	let annotate = |mut row: serde_json::Value| {
		row["duckdb_memory_limit_observed"] = serde_json::json!(read_back);
		row["duckdb_memory_limit_observed_bytes_display_derived"] = serde_json::json!(
			read_back
				.as_deref()
				.and_then(toron_tpch::duckdb_memory::parse_display)
		);
		row["duckdb_temp_directory_isolated"] = serde_json::json!(temp_directory.is_some());
		row["parent_peak_rss_kib_at_spawn"] =
			serde_json::json!(output.parent_peak_rss_kib_at_spawn);
		row["peak_rss_label"] = serde_json::json!(toron_tpch::perf_board::peak_rss_label(
			output.peak_rss_kib,
			output.parent_peak_rss_kib_at_spawn
		));
		row
	};
	let failure = if !output.status_success {
		Some(("duckdb_query_failed", format!("stderr={:?}", output.stderr)))
	} else if read_back_mismatch {
		Some((
			"duckdb_memory_limit_read_back_mismatch",
			format!(
				"requested={:?} expected_display={:?} observed={read_back:?}",
				memory.requested.as_ref().map(|limit| &limit.normalized),
				memory
					.requested
					.as_ref()
					.map(|limit| toron_tpch::duckdb_memory::duckdb_display(limit.bytes)),
			),
		))
	} else {
		None
	};
	if let Some((reason, message)) = failure {
		let mut timings = PerfPhaseTimings::default();
		timings.total_ms = output.parent_wall_ms;
		return Ok(annotate(
			PerfAttempt {
				engine: "duckdb".to_string(),
				query: export.query_id.clone(),
				scale_factor: export.scale_factor,
				mode: export.mode.clone(),
				run_index,
				warmup,
				phase: Phase::Execute,
				status: Status::Fail,
				reason: Some(reason.to_string()),
				diagnostic_message: Some(message),
				timings,
				parent_wall_ms: Some(output.parent_wall_ms),
				peak_rss_kib: output.peak_rss_kib,
				spill_bytes: 0,
				plan_fingerprint: Some(plan_fingerprint.to_string()),
				digest: None,
				output_row_count: None,
				output_schema_fingerprint: Some(output_schema_fingerprint.to_string()),
				engine_version: Some(duckdb_version.to_string()),
				duckdb_version: Some(duckdb_version.to_string()),
				debug_build: false,
				local_join_admissions: None,
				targeted_local_join_admissions: None,
				join_input_replay_ledger: None,
				join_input_total_concats: None,
				join_spill_observations: None,
				partition_work_concurrency_observations: None,
				dataset_verification: export.dataset_verification,
				dataset_manifest_fingerprint: export
					.dataset_verification
					.map(|_| export.manifest_fingerprint.clone()),
			}
			.json_value(),
		));
	}
	let combined = format!("{}\n{}", output.stdout, output.stderr);
	let execute_ms = parse_duckdb_timer_ms(&combined)
		.ok_or_else(|| format!("DuckDB timer output missing from {:?}", combined))?;
	let result = duckdb_query_result(&arrays)
		.ok_or_else(|| "DuckDB query emitted no JSON result".to_string())?;
	let row_count = result
		.as_array()
		.map(|rows| rows.len() as u64)
		.ok_or_else(|| "DuckDB query JSON result was not an array".to_string())?;
	// ADR 603: the captured result's digest, taken after the child has exited, so the timed
	// interval is untouched; the diagnostic sweep's result is checked against it.
	let result_sha256 = duckdb_result_sha256(result)?;
	let mut timings = PerfPhaseTimings::default();
	timings.execute_ms = execute_ms;
	timings.total_ms = execute_ms;
	let annotate = |row: serde_json::Value| {
		let mut row = annotate(row);
		row["duckdb_result_sha256"] = serde_json::json!(result_sha256);
		row
	};
	Ok(annotate(
		PerfAttempt {
			engine: "duckdb".to_string(),
			query: export.query_id.clone(),
			scale_factor: export.scale_factor,
			mode: export.mode.clone(),
			run_index,
			warmup,
			phase: Phase::Execute,
			status: Status::Pass,
			reason: None,
			diagnostic_message: None,
			timings,
			parent_wall_ms: Some(output.parent_wall_ms),
			peak_rss_kib: output.peak_rss_kib,
			spill_bytes: 0,
			plan_fingerprint: Some(plan_fingerprint.to_string()),
			digest: None,
			output_row_count: Some(row_count),
			output_schema_fingerprint: Some(output_schema_fingerprint.to_string()),
			engine_version: Some(duckdb_version.to_string()),
			duckdb_version: Some(duckdb_version.to_string()),
			debug_build: false,
			local_join_admissions: None,
			targeted_local_join_admissions: None,
			join_input_replay_ledger: None,
			join_input_total_concats: None,
			join_spill_observations: None,
			partition_work_concurrency_observations: None,
			dataset_verification: export.dataset_verification,
			dataset_manifest_fingerprint: export
				.dataset_verification
				.map(|_| export.manifest_fingerprint.clone()),
		}
		.json_value(),
	))
}

/// ADR 603: the DuckDB diagnostic sweep. One profiled DuckDB child per cell, run only after a
/// timed artifact is complete: same staged executable, query, data, thread pin, and memory
/// setting, a fresh temp directory and profile file per child. Its readings are diagnostic
/// observations under the sweep's own run identity, never facts about the timed runs.
fn duckdb_profile_sweep_command(args: &[String]) -> ExitCode {
	match duckdb_profile_sweep(args) {
		Ok(()) => ExitCode::SUCCESS,
		Err(message) => harness_error(&message),
	}
}

fn duckdb_profile_sweep(args: &[String]) -> Result<(), String> {
	let query = flag_value(args, "--query").unwrap_or("all");
	let mode = flag_value(args, "--mode").unwrap_or("strict");
	let data_source = flag_value(args, "--data-source").unwrap_or("parquet");
	if !matches!(data_source, "parquet" | "persisted") {
		return Err(format!(
			"duckdb-profile-sweep --data-source {data_source:?}: persisted parquet only"
		));
	}
	let data_dir = data_dir_from_args(args);
	let sf = explicit_sf(args)?.ok_or_else(|| "duckdb-profile-sweep requires --sf".to_string())?;
	let output_dir = PathBuf::from(
		flag_value(args, "--output-dir")
			.ok_or_else(|| "duckdb-profile-sweep requires --output-dir".to_string())?,
	);
	if output_dir.exists() {
		return Err(format!(
			"duckdb-profile-sweep output directory {} must be fresh",
			output_dir.display()
		));
	}
	let memory = toron_tpch::duckdb_memory::memory_limit_coordinate()?;
	let queries = perf_query_list(query)?;
	let duckdb_bin = duckdb_binary();
	let duckdb_version = duckdb_cli_version(&duckdb_bin)?
		.ok_or_else(|| format!("DuckDB executable {duckdb_bin:?} is absent"))?;
	let duckdb_sha256 = toron_tpch::evidence::sha256_file(Path::new(&duckdb_bin))?;
	let profiles = output_dir.join("profiles");
	std::fs::create_dir_all(&profiles)
		.map_err(|error| format!("create {}: {error}", profiles.display()))?;
	let sweep_run_id = format!(
		"duckdb-profile-sweep-{}-{}",
		std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.map(|elapsed| elapsed.as_millis())
			.unwrap_or(0),
		std::process::id()
	);
	let (memory_limit, memory_limit_provenance) = duckdb_memory_limit_fields();
	let mut lines = vec![serde_json::json!({
		"kind": "duckdb_profile_sweep_header",
		"sweep_run_id": sweep_run_id,
		"label": "diagnostic observations of each cell's query at this setting; not timed-run facts",
		"scale_factor": sf,
		"mode": mode,
		"data_source": data_source,
		"queries": queries,
		"duckdb_version": duckdb_version,
		"duckdb_executable_sha256": duckdb_sha256,
		"duckdb_threads": duckdb_thread_setting(),
		"duckdb_threads_provenance": reference_thread_pin_provenance(&duckdb_thread_setting()),
		"duckdb_memory_limit": memory_limit,
		"duckdb_memory_limit_provenance": memory_limit_provenance,
		"cpu_affinity_requested": toron_tpch::affinity::affinity_coordinate().requested,
		"cpu_affinity_observed": toron_tpch::affinity::observed_cpulist(),
		"profile_settings": toron_tpch::duckdb_memory::PROFILE_SETTINGS,
	})];
	for query_id in &queries {
		let export = export_persisted_perf_query_parquet(query_id, mode, sf, &data_dir)?;
		let temp_directory = fresh_duckdb_temp_directory()?;
		let profile_path = profiles.join(format!("{query_id}.json"));
		let script = format!(
			".mode json\n{}{}{DUCKDB_MEMORY_LIMIT_READ_BACK}\nSET custom_profiling_settings={};\n\
			 SET enable_profiling='json';\nSET profiling_output={};\n{}\n",
			duckdb_setup_sql(&export),
			duckdb_temp_directory_sql(&temp_directory),
			sql_string_literal(toron_tpch::duckdb_memory::PROFILE_SETTINGS),
			sql_string_literal(&profile_path.display().to_string()),
			duckdb_query_statement(&export.sql)
		);
		let output = run_duckdb_script(&duckdb_bin, &script);
		let _ = std::fs::remove_dir_all(&temp_directory);
		let output = output?;
		if !output.status_success {
			return Err(format!(
				"duckdb-profile-sweep {query_id}: DuckDB failed: {:?}",
				output.stderr
			));
		}
		let arrays = extract_json_arrays(&output.stdout);
		let read_back = duckdb_memory_limit_read_back(&arrays);
		if let Some(requested) = &memory.requested
			&& !read_back.as_deref().is_some_and(|observed| {
				toron_tpch::duckdb_memory::read_back_agrees(requested, observed)
			}) {
			return Err(format!(
				"duckdb-profile-sweep {query_id}: memory_limit read-back {read_back:?} is not {:?}",
				toron_tpch::duckdb_memory::duckdb_display(requested.bytes)
			));
		}
		let result = duckdb_query_result(&arrays)
			.ok_or_else(|| format!("duckdb-profile-sweep {query_id}: no JSON result"))?;
		let row_count = result
			.as_array()
			.map(|rows| rows.len() as u64)
			.ok_or_else(|| format!("duckdb-profile-sweep {query_id}: result is not an array"))?;
		let result_sha256 = duckdb_result_sha256(result)
			.map_err(|error| format!("duckdb-profile-sweep {query_id}: {error}"))?;
		let profile_text = std::fs::read_to_string(&profile_path)
			.map_err(|error| format!("read {}: {error}", profile_path.display()))?;
		let profile: serde_json::Value = serde_json::from_str(&profile_text)
			.map_err(|error| format!("parse {}: {error}", profile_path.display()))?;
		let readings = toron_tpch::duckdb_memory::parse_profile(&profile)
			.map_err(|error| format!("duckdb-profile-sweep {query_id}: {error}"))?;
		lines.push(serde_json::json!({
			"kind": "duckdb_profile_sweep_cell",
			"sweep_run_id": sweep_run_id,
			"query": query_id,
			"duckdb_memory_limit_observed": read_back,
			"duckdb_temp_directory_isolated": true,
			"peak_temp_dir_bytes": readings.peak_temp_dir_bytes,
			"peak_buffer_bytes": readings.peak_buffer_bytes,
			"profile_latency_seconds": readings.latency_seconds,
			"output_row_count": row_count,
			"duckdb_result_sha256": result_sha256,
			"profile_sha256": toron_tpch::evidence::sha256_bytes(profile_text.as_bytes()),
			"dataset_manifest_fingerprint": export
				.dataset_verification
				.map(|_| export.manifest_fingerprint.clone()),
		}));
	}
	let artifact = output_dir.join("duckdb_profile_sweep.jsonl");
	let text = lines
		.iter()
		.map(|line| line.to_string() + "\n")
		.collect::<String>();
	std::fs::write(&artifact, text)
		.map_err(|error| format!("write {}: {error}", artifact.display()))?;
	println!("{}", artifact.display());
	Ok(())
}

/// ADR 604: one cell's structural census at the sealed anchor's coordinate. Every execution is
/// named: the plan (no execution), T1 (Toron `EXPLAIN ANALYZE`), T2 (Toron, profiled with node
/// attribution and the scan ledger), D0 (DuckDB plain `EXPLAIN`, no execution), and D1 (DuckDB,
/// profiled with operator metrics). All are diagnostic; nothing is compared with a board median.
async fn structural_census_command(args: &[String]) -> ExitCode {
	match structural_census(args).await {
		Ok(path) => {
			println!("{}", path.display());
			ExitCode::SUCCESS
		}
		Err(message) => harness_error(&message),
	}
}

/// ADR 605: scan work on both engines for one cell, at the sealed anchor's coordinate. T2 is a
/// profiled Toron execution with the scan progress file, node attribution, and the reservoir
/// residency profile; T3 is the same with the local runtime-filter bridge ablated; D2 is DuckDB
/// with `OPERATOR_ROWS_SCANNED`. All diagnostic; nothing is compared with a board median.
fn scan_work_census_command(args: &[String]) -> ExitCode {
	match scan_work_census(args) {
		Ok(path) => {
			println!("{}", path.display());
			ExitCode::SUCCESS
		}
		Err(message) => harness_error(&message),
	}
}

fn scan_work_census(args: &[String]) -> Result<PathBuf, String> {
	let CensusPrelude {
		query_id,
		mode,
		data_source,
		data_dir,
		sf,
		output_dir,
		anchor_path,
		anchor_sha256,
		anchor_rows,
		coordinate,
		export,
		duckdb_bin,
	} = census_prelude("scan-work-census", args)?;
	let anchor_cell = |engine: &str| -> Vec<&serde_json::Value> {
		anchor_rows
			.iter()
			.filter(|row| {
				row.get("query").and_then(serde_json::Value::as_str) == Some(query_id)
					&& row.get("engine").and_then(serde_json::Value::as_str) == Some(engine)
					&& row.get("warmup").and_then(serde_json::Value::as_bool) == Some(false)
			})
			.collect()
	};
	let distinct = |rows: &[&serde_json::Value], field: &str| -> BTreeSet<String> {
		rows.iter()
			.filter_map(|row| row.get(field))
			.map(|value| {
				value
					.as_str()
					.map_or_else(|| value.to_string(), str::to_string)
			})
			.collect()
	};
	let anchor_toron = anchor_cell("toron");
	let anchor_duckdb = anchor_cell("duckdb");
	let anchor_toron_digests = distinct(&anchor_toron, "digest");
	let anchor_toron_fingerprints = distinct(&anchor_toron, "plan_fingerprint");
	let anchor_duckdb_digests = distinct(&anchor_duckdb, "duckdb_result_sha256");
	let anchor_memory_observed = distinct(&anchor_duckdb, "duckdb_memory_limit_observed");

	// T2 and T3: the same profiled Toron execution, with and without the local bridge. Each carries
	// the progress stream, the ledger (to reconcile the stream with), node attribution, and the
	// reservoir residency profile.
	let toron_run = |label: &str, ablate: bool| -> Result<serde_json::Value, String> {
		let progress = output_dir.join(format!("{label}_scan_progress.jsonl"));
		let attribution = output_dir.join(format!("{label}_node_attribution.json"));
		let residency = output_dir.join(format!("{label}_reservoir_residency.json"));
		let (progress_text, attribution_text, residency_text) = (
			progress.display().to_string(),
			attribution.display().to_string(),
			residency.display().to_string(),
		);
		let mut envs = vec![
			("TORON_TPCH_PARQUET_SCAN_LEDGER", "1"),
			(
				"TORON_TPCH_PARQUET_SCAN_PROGRESS_PATH",
				progress_text.as_str(),
			),
			(
				"TORON_NODE_ATTRIBUTION_PROFILE_PATH",
				attribution_text.as_str(),
			),
			(
				"TORON_GROUPED_RESERVOIR_RESIDENCY_PROFILE_PATH",
				residency_text.as_str(),
			),
		];
		if ablate {
			envs.push(("TORON_ABLATE_LOCAL_RUNTIME_FILTER", "1"));
		}
		let row = run_perf_child_with_envs(query_id, &mode, sf, 0, &data_source, &data_dir, &envs)?;
		// The progress stream, folded fail-closed and reconciled with the ledger's receipt. A
		// failure is kept and named, and the run's scan work is withheld.
		let progress_fold = std::fs::read_to_string(&progress)
			.map_err(|error| format!("read {}: {error}", progress.display()))
			.and_then(|jsonl| toron_tpch::census::fold_scan_progress(&jsonl))
			.and_then(|scans| {
				toron_tpch::census::reconcile_progress_with_receipt(
					&scans,
					row.get("parquet_scan_receipt"),
				)
				.map(|()| scans)
			});
		let (scans, scan_work_error) = match progress_fold {
			Ok(scans) => (Some(scans), None),
			Err(error) => (None, Some(error)),
		};
		let tables = scans.as_deref().map(toron_tpch::census::toron_table_work);
		let residency_profile = match std::fs::read_to_string(&residency) {
			Ok(text) => serde_json::from_str::<serde_json::Value>(&text)
				.unwrap_or_else(|error| serde_json::json!({"unparsed": error.to_string()})),
			Err(_) => {
				serde_json::json!({"absent": "no residency profile was written for this run"})
			}
		};
		let digest = row.get("digest").map(|value| {
			value
				.as_str()
				.map_or_else(|| value.to_string(), str::to_string)
		});
		Ok(serde_json::json!({
			"execution": if ablate {
				"Toron perf-run-one child, TORON_ABLATE_LOCAL_RUNTIME_FILTER=1"
			} else {
				"Toron perf-run-one child, local runtime-filter bridge as by default"
			},
			"status": row.get("status"),
			"digest": digest,
			"digest_matches_anchor": digest.as_ref().is_some_and(|digest| {
				anchor_toron_digests.len() == 1 && anchor_toron_digests.contains(digest)
			}),
			"plan_fingerprint": row.get("plan_fingerprint"),
			"route": row
				.get("node_attribution")
				.and_then(|profile| profile.get("aggregate_invocations"))
				.and_then(serde_json::Value::as_array)
				.map(|invocations| {
					invocations
						.iter()
						.filter_map(|invocation| invocation.get("route"))
						.cloned()
						.collect::<Vec<_>>()
				}),
			"node_attribution_present": row
				.get("node_attribution")
				.is_some_and(serde_json::Value::is_object),
			"parquet_scan_receipt": row.get("parquet_scan_receipt"),
			"scan_work_valid": scan_work_error.is_none(),
			"scan_work_error": scan_work_error,
			"scans": scans,
			"tables": tables,
			"reservoir_residency": residency_profile,
		}))
	};
	let t2 = toron_run("t2", false)?;
	let t3 = toron_run("t3", true)?;

	// D2: DuckDB with the scan-work counter.
	let temp_directory = fresh_duckdb_temp_directory()?;
	let d2_profile_path = output_dir.join("d2_profile.json");
	let d2 = run_duckdb_script(
		&duckdb_bin,
		&format!(
			".mode json\n{}{}{DUCKDB_MEMORY_LIMIT_READ_BACK}\nSET custom_profiling_settings={};\n\
			 SET enable_profiling='json';\nSET profiling_output={};\n{}\n",
			duckdb_setup_sql(&export),
			duckdb_temp_directory_sql(&temp_directory),
			sql_string_literal(toron_tpch::census::DUCKDB_SCAN_WORK_PROFILE_SETTINGS),
			sql_string_literal(&d2_profile_path.display().to_string()),
			duckdb_query_statement(&export.sql)
		),
	);
	let _ = std::fs::remove_dir_all(&temp_directory);
	let d2 = d2?;
	if !d2.status_success {
		return Err(format!(
			"scan-work-census {query_id}: D2 failed: {:?}",
			d2.stderr
		));
	}
	let d2_arrays = extract_json_arrays(&d2.stdout);
	let d2_result = duckdb_query_result(&d2_arrays)
		.ok_or_else(|| format!("scan-work-census {query_id}: D2 returned no result"))?;
	let d2_digest = duckdb_result_sha256(d2_result)?;
	let d2_profile = toron_tpch::census::parse_duckdb_operator_profile(
		&serde_json::from_str(
			&std::fs::read_to_string(&d2_profile_path)
				.map_err(|error| format!("read {}: {error}", d2_profile_path.display()))?,
		)
		.map_err(|error| format!("parse {}: {error}", d2_profile_path.display()))?,
	)?;
	let (duckdb_tables, duckdb_unpaired, d2_scan_work_error) =
		match toron_tpch::census::duckdb_table_work(&d2_profile) {
			Ok((tables, unpaired)) => (tables, unpaired, None),
			Err(error) => (BTreeMap::new(), Vec::new(), Some(error)),
		};
	let d2_memory_observed = duckdb_memory_limit_read_back(&d2_arrays);

	// Identity, the per-table comparison, and the ablation reading.
	let run_tables =
		|run: &serde_json::Value| -> BTreeMap<String, toron_tpch::census::TableScanWork> {
			serde_json::from_value(run["tables"].clone()).unwrap_or_default()
		};
	let t2_tables = run_tables(&t2);
	let t3_tables = run_tables(&t3);
	let fingerprint =
		|run: &serde_json::Value| run["plan_fingerprint"].as_str().map(str::to_string);
	let t2_fingerprint = fingerprint(&t2);
	let fingerprints_agree = t2_fingerprint.is_some()
		&& t2_fingerprint == fingerprint(&t3)
		&& anchor_toron_fingerprints.len() == 1
		&& anchor_toron_fingerprints.contains(t2_fingerprint.as_deref().unwrap_or_default());
	let run_ok = |run: &serde_json::Value| {
		run["status"].as_str() == Some("pass")
			&& run["digest_matches_anchor"] == true
			&& run["scan_work_valid"] == true
			&& run["node_attribution_present"] == true
	};
	let t2_ok = run_ok(&t2);
	let t3_ok = run_ok(&t3);
	let d2_digest_matches =
		anchor_duckdb_digests.len() == 1 && anchor_duckdb_digests.contains(&d2_digest);
	let d2_memory_matches = d2_memory_observed.as_ref().is_some_and(|observed| {
		anchor_memory_observed.len() == 1 && anchor_memory_observed.contains(observed)
	});
	let d2_ok = d2_digest_matches && d2_memory_matches && d2_scan_work_error.is_none();
	let mut excluded = Vec::new();
	for (failed, reason) in [
		(!t2_ok, "t2_failed_digest_scan_work_or_attribution"),
		(!t3_ok, "t3_failed_digest_scan_work_or_attribution"),
		(!d2_digest_matches, "d2_digest_differs_from_anchor_duckdb"),
		(
			!d2_memory_matches,
			"d2_memory_limit_read_back_differs_from_anchor",
		),
		(d2_scan_work_error.is_some(), "d2_scan_work_refused"),
	] {
		if failed {
			excluded.push(reason);
		}
	}
	let ablation = toron_tpch::census::ablation_readings(
		t2_ok && t3_ok && fingerprints_agree,
		&t2_tables,
		&t3_tables,
	);
	let tables = t2_tables
		.keys()
		.chain(duckdb_tables.keys())
		.cloned()
		.collect::<BTreeSet<_>>()
		.into_iter()
		.map(|table| {
			let toron = t2_tables.get(&table);
			let duckdb = duckdb_tables.get(&table);
			let (reading, visited_ratio) =
				toron_tpch::census::visited_comparison(t2_ok && d2_ok, toron, duckdb);
			let fraction = |work: Option<&toron_tpch::census::TableScanWork>| {
				work.and_then(|work| {
					(work.rows_visited > 0)
						.then(|| work.rows_emitted as f64 / work.rows_visited as f64)
				})
			};
			serde_json::json!({
				"table": table,
				"toron_t2": toron,
				"toron_t3": t3_tables.get(&table),
				"duckdb_d2": duckdb,
				"visited_ratio_toron_over_duckdb": visited_ratio,
				"visited_reading": reading,
				"toron_emitted_fraction": fraction(toron),
				"duckdb_emitted_fraction": fraction(duckdb),
				"ablation": ablation.get(&table),
			})
		})
		.collect::<Vec<_>>();
	let artifact = serde_json::json!({
		"kind": "scan_work_census_cell",
		"query": query_id,
		"coordinate": coordinate,
		"sealed_anchor": {"path": anchor_path, "sha256": anchor_sha256},
		// The visited-rows comparison needs T2 and D2; the planned cell is complete only with T3.
		"scan_comparison_valid": t2_ok && d2_ok,
		"cell_complete": excluded.is_empty(),
		"excluded_reasons": excluded,
		"d2_scan_work_error": d2_scan_work_error,
		"ablation_identity": {
			"t2_ok": t2_ok,
			"t3_ok": t3_ok,
			"fingerprints_agree_t2_t3_anchor": fingerprints_agree,
		},
		"measures": {
			"rows_visited": "Toron raw_rows (decoded parquet batches before Toron's row mask) / \
				DuckDB OPERATOR_ROWS_SCANNED: the closest visited-row counters, compared \
				operationally; their accounting is not proven identical",
			"rows_emitted": "Toron output_rows / DuckDB OPERATOR_CARDINALITY: each engine's own \
				reduction only, never cross-engine scan work",
			"decoded_bytes": "Toron only (decoded Arrow bytes, not file bytes)",
			"file_bytes_per_scan": "unavailable on both engines",
			"duckdb_total_bytes_read": "query-level only",
			"progress_end_rule": "the lazy scan path emits no scan_end; a scan without one is \
				marked end_not_emitted_by_lazy_path, and completeness rests on reconciling the \
				progress fold with the ledger's receipt, both describing the batches consumed",
		},
		"tables": tables,
		"duckdb_unpaired_scans": duckdb_unpaired,
		"duckdb_total_bytes_read": d2_profile.total_bytes_read,
		"toron": {"t2": t2, "t3": t3},
		"duckdb": {
			"d2_result_sha256": d2_digest,
			"d2_memory_limit_observed": d2_memory_observed,
			"d2": d2_profile,
		},
	});
	let path = output_dir.join("scan_work_census.json");
	std::fs::write(
		&path,
		serde_json::to_string_pretty(&artifact).map_err(|error| error.to_string())?,
	)
	.map_err(|error| format!("write {}: {error}", path.display()))?;
	Ok(path)
}

/// ADR 604/605: what every census command establishes before it executes anything: one query,
/// a fresh output directory, the sealed anchor bound by its SHA-256, and the selected coordinate
/// checked field by field against the anchor's header.
struct CensusPrelude {
	query_id: &'static str,
	mode: String,
	data_source: String,
	data_dir: PathBuf,
	sf: f64,
	output_dir: PathBuf,
	anchor_path: String,
	anchor_sha256: String,
	anchor_rows: Vec<serde_json::Value>,
	coordinate: serde_json::Value,
	export: toron_tpch::TpchParquetExport,
	duckdb_bin: String,
}

fn census_prelude(command: &str, args: &[String]) -> Result<CensusPrelude, String> {
	let query_id = perf_query_list(
		flag_value(args, "--query").ok_or_else(|| format!("{command} requires --query"))?,
	)?;
	let [query_id] = query_id.as_slice() else {
		return Err(format!("{command} takes one query"));
	};
	let query_id = *query_id;
	let mode = flag_value(args, "--mode").unwrap_or("strict");
	let data_source = flag_value(args, "--data-source").unwrap_or("parquet");
	if !matches!(data_source, "parquet" | "persisted") {
		return Err(format!("{command}: persisted parquet only"));
	}
	let data_dir = data_dir_from_args(args);
	let sf = explicit_sf(args)?.ok_or_else(|| format!("{command} requires --sf"))?;
	let output_dir = PathBuf::from(
		flag_value(args, "--output-dir")
			.ok_or_else(|| format!("{command} requires --output-dir"))?,
	);
	let anchor_path = flag_value(args, "--anchor")
		.ok_or_else(|| format!("{command} requires --anchor <sealed perf_board.jsonl>"))?;
	// The anchor is bound to the sealed artifact by content, before any of it is used.
	let anchor_sha256_expected = flag_value(args, "--anchor-sha256").ok_or_else(|| {
		format!("{command} requires --anchor-sha256 <the sealed artifact's sha256>")
	})?;
	let anchor_sha256 = toron_tpch::evidence::sha256_file(Path::new(anchor_path))?;
	if anchor_sha256 != anchor_sha256_expected {
		return Err(format!(
			"{command}: anchor {anchor_path} has sha256 {anchor_sha256}, not the sealed \
			 {anchor_sha256_expected}"
		));
	}
	if output_dir.exists() {
		return Err(format!(
			"{command} output {} must be fresh",
			output_dir.display()
		));
	}
	std::fs::create_dir_all(&output_dir)
		.map_err(|error| format!("create {}: {error}", output_dir.display()))?;
	let anchor_rows = std::fs::read_to_string(anchor_path)
		.map_err(|error| format!("read {anchor_path}: {error}"))?
		.lines()
		.filter(|line| !line.trim().is_empty())
		.map(|line| serde_json::from_str::<serde_json::Value>(line).map_err(|e| e.to_string()))
		.collect::<Result<Vec<_>, _>>()?;
	let sealed_header = anchor_rows
		.first()
		.filter(|row| {
			row.get("kind").and_then(serde_json::Value::as_str) == Some("artifact_header")
		})
		.ok_or_else(|| format!("{anchor_path} has no artifact header"))?;

	// The census's own coordinate, checked against the sealed header before anything runs. The
	// dataset fingerprint is the persisted dataset's, as the board header records it.
	let export = export_persisted_perf_query_parquet(query_id, mode, sf, &data_dir)?;
	let dataset_manifest_fingerprint =
		load_persisted_tpch_dataset(mode, sf, &data_dir)?.manifest_fingerprint;
	let (memory_per_task, memory_provenance) = gauge_memory_per_task()?;
	let (spill_bytes_reserved, spill_provenance) = gauge_spill_bytes_reserved()?;
	let (concurrency, concurrency_provenance) = gauge_partition_work_concurrency_requested()?;
	let (duckdb_memory_limit, duckdb_memory_limit_provenance) = duckdb_memory_limit_fields();
	let duckdb_bin = duckdb_binary();
	let coordinate = serde_json::json!({
		"scale_factor": sf,
		"mode": mode,
		"data_source": data_source,
		"dataset_manifest_fingerprint": dataset_manifest_fingerprint,
		"memory_per_task": memory_per_task,
		"memory_per_task_provenance": memory_provenance,
		"spill_bytes_reserved": spill_bytes_reserved,
		"spill_bytes_reserved_provenance": spill_provenance,
		"partition_work_concurrency_requested": concurrency,
		"partition_work_concurrency_requested_provenance": concurrency_provenance,
		"duckdb_threads": duckdb_thread_setting(),
		"duckdb_threads_provenance": reference_thread_pin_provenance(&duckdb_thread_setting()),
		"duckdb_memory_limit": duckdb_memory_limit,
		"duckdb_memory_limit_provenance": duckdb_memory_limit_provenance,
		"duckdb_executable_sha256": toron_tpch::evidence::sha256_file(Path::new(&duckdb_bin))?,
		"cpu_affinity_requested": toron_tpch::affinity::affinity_coordinate().requested,
		"cpu_affinity_observed": toron_tpch::affinity::observed_cpulist(),
		"dataset_verification": export
			.dataset_verification
			.map(toron_tpch::telemetry::DatasetVerification::as_str),
	});
	let mismatches = toron_tpch::census::coordinate_mismatches(&coordinate, sealed_header);
	if !mismatches.is_empty() {
		return Err(format!(
			"{command} {query_id}: coordinate differs from the sealed header: {}",
			mismatches.join(", ")
		));
	}

	Ok(CensusPrelude {
		query_id,
		mode: mode.to_string(),
		data_source: data_source.to_string(),
		data_dir,
		sf,
		output_dir,
		anchor_path: anchor_path.to_string(),
		anchor_sha256,
		anchor_rows,
		coordinate,
		export,
		duckdb_bin,
	})
}

async fn structural_census(args: &[String]) -> Result<PathBuf, String> {
	let CensusPrelude {
		query_id,
		mode,
		data_source,
		data_dir,
		sf,
		output_dir,
		anchor_path,
		anchor_sha256,
		anchor_rows,
		coordinate,
		export,
		duckdb_bin,
	} = census_prelude("structural-census", args)?;
	let (mode, data_source, anchor_path) =
		(mode.as_str(), data_source.as_str(), anchor_path.as_str());

	// The plan (no execution) and T1.
	let toron_plan =
		toron_tpch::structural_census_toron_plan(query_id, mode, sf, &data_dir).await?;

	// T2: one profiled Toron execution with node attribution and the scan ledger.
	let profile_path = output_dir.join("t2_node_attribution.json");
	let profile_text = profile_path.display().to_string();
	let t2 = run_perf_child_with_envs(
		query_id,
		mode,
		sf,
		0,
		data_source,
		&data_dir,
		&[
			("TORON_NODE_ATTRIBUTION_PROFILE_PATH", profile_text.as_str()),
			("TORON_TPCH_PARQUET_SCAN_LEDGER", "1"),
		],
	)?;

	// D0 and D1.
	let duckdb_version = duckdb_cli_version(&duckdb_bin)?
		.ok_or_else(|| format!("DuckDB executable {duckdb_bin:?} is absent"))?;
	let d0 = run_duckdb_script(
		&duckdb_bin,
		&format!(
			".mode json\n{}EXPLAIN (FORMAT JSON) {}\n",
			duckdb_setup_sql(&export),
			duckdb_query_statement(&export.sql)
		),
	)?;
	let d0_names = toron_tpch::census::duckdb_explain_names(&d0.stdout);
	let temp_directory = fresh_duckdb_temp_directory()?;
	let d1_profile_path = output_dir.join("d1_profile.json");
	let d1 = run_duckdb_script(
		&duckdb_bin,
		&format!(
			".mode json\n{}{}{DUCKDB_MEMORY_LIMIT_READ_BACK}\nSET custom_profiling_settings={};\n\
			 SET enable_profiling='json';\nSET profiling_output={};\n{}\n",
			duckdb_setup_sql(&export),
			duckdb_temp_directory_sql(&temp_directory),
			sql_string_literal(toron_tpch::census::DUCKDB_OPERATOR_PROFILE_SETTINGS),
			sql_string_literal(&d1_profile_path.display().to_string()),
			duckdb_query_statement(&export.sql)
		),
	);
	let _ = std::fs::remove_dir_all(&temp_directory);
	let d1 = d1?;
	if !d1.status_success {
		return Err(format!(
			"structural-census {query_id}: D1 failed: {:?}",
			d1.stderr
		));
	}
	let d1_arrays = extract_json_arrays(&d1.stdout);
	let d1_result = duckdb_query_result(&d1_arrays)
		.ok_or_else(|| format!("structural-census {query_id}: D1 returned no result"))?;
	let d1_digest = duckdb_result_sha256(d1_result)?;
	let d1_row_count = d1_result.as_array().map(Vec::len);
	let d1_profile: serde_json::Value = serde_json::from_str(
		&std::fs::read_to_string(&d1_profile_path)
			.map_err(|error| format!("read {}: {error}", d1_profile_path.display()))?,
	)
	.map_err(|error| format!("parse {}: {error}", d1_profile_path.display()))?;
	let d1_operators = toron_tpch::census::parse_duckdb_operator_profile(&d1_profile)?;
	let d0_correspondence =
		toron_tpch::census::duckdb_plan_correspondence(&d0_names, &d1_operators);

	// Identity and plan correspondence against the sealed anchor's timed rows.
	let anchor_cell = |engine: &str| -> Vec<&serde_json::Value> {
		anchor_rows
			.iter()
			.filter(|row| {
				row.get("query").and_then(serde_json::Value::as_str) == Some(query_id)
					&& row.get("engine").and_then(serde_json::Value::as_str) == Some(engine)
					&& row.get("warmup").and_then(serde_json::Value::as_bool) == Some(false)
			})
			.collect()
	};
	let distinct = |rows: &[&serde_json::Value], field: &str| -> BTreeSet<String> {
		rows.iter()
			.filter_map(|row| row.get(field))
			.map(|value| {
				value
					.as_str()
					.map_or_else(|| value.to_string(), str::to_string)
			})
			.collect()
	};
	let anchor_toron = anchor_cell("toron");
	let anchor_duckdb = anchor_cell("duckdb");
	let anchor_toron_digests = distinct(&anchor_toron, "digest");
	let anchor_toron_fingerprints = distinct(&anchor_toron, "plan_fingerprint");
	let anchor_duckdb_digests = distinct(&anchor_duckdb, "duckdb_result_sha256");
	let t2_digest = t2.get("digest").map(|value| {
		value
			.as_str()
			.map_or_else(|| value.to_string(), str::to_string)
	});
	let t2_fingerprint = t2
		.get("plan_fingerprint")
		.and_then(serde_json::Value::as_str);
	let binder_fingerprint = toron_plan["plan"]["plan_fingerprint"].as_str();
	let toron_correspondence = toron_tpch::census::toron_plan_correspondence(
		binder_fingerprint,
		t2_fingerprint,
		&anchor_toron_fingerprints,
	);
	let t1_rows_emitted = toron_plan["t1"]["analyze_rows"]
		.as_array()
		.map_or(0, Vec::len);
	let toron_joins = toron_tpch::census::toron_join_table(
		toron_plan["plan"]["join_order_edges_estimated"]
			.as_str()
			.unwrap_or_default(),
		t1_rows_emitted,
	);
	let t2_digest_matches = t2_digest.as_ref().is_some_and(|digest| {
		anchor_toron_digests.len() == 1 && anchor_toron_digests.contains(digest)
	});
	let d1_digest_matches =
		anchor_duckdb_digests.len() == 1 && anchor_duckdb_digests.contains(&d1_digest);
	// The default memory limit D1 ran under, against the one the anchor's timed DuckDB rows read.
	let d1_memory_observed = duckdb_memory_limit_read_back(&d1_arrays);
	let anchor_memory_observed = distinct(&anchor_duckdb, "duckdb_memory_limit_observed");
	let d1_memory_matches = d1_memory_observed.as_ref().is_some_and(|observed| {
		anchor_memory_observed.len() == 1 && anchor_memory_observed.contains(observed)
	});
	let t2_passed = t2.get("status").and_then(serde_json::Value::as_str) == Some("pass");
	let excluded = toron_tpch::census::census_exclusions(
		&t2,
		t2_digest_matches,
		d1_digest_matches,
		d1_memory_matches,
	);
	let identity = serde_json::json!({
		"t2_passed": t2_passed,
		"t2_digest_matches_anchor_toron": t2_digest_matches,
		"d1_digest_matches_anchor_duckdb": d1_digest_matches,
		"d1_memory_limit_observed": d1_memory_observed,
		"anchor_duckdb_memory_limit_observed": anchor_memory_observed,
		"d1_memory_limit_matches_anchor": d1_memory_matches,
		"anchor_toron_digests": anchor_toron_digests,
		"anchor_duckdb_digests": anchor_duckdb_digests,
		"cross_engine_equivalence": "not proved by this record",
		"toron_output_row_count": t2.get("output_row_count"),
		"duckdb_output_row_count": d1_row_count,
	});
	let correspondence = serde_json::json!({
		"toron": toron_correspondence,
		"toron_note": "explain-binder fingerprint against T2 and the anchor; T1 returns no plan of its own",
		"binder_fingerprint": binder_fingerprint,
		"t2_fingerprint": t2_fingerprint,
		"anchor_toron_fingerprints": anchor_toron_fingerprints,
		"duckdb": d0_correspondence,
		"duckdb_note": "D0 against D1 by (depth, operator name): a shape-name match, weaker than \
			exact plan correspondence; D1 takes precedence",
	});
	let artifact = serde_json::json!({
		"kind": "structural_census_cell",
		"query": query_id,
		"coordinate": coordinate,
		"sealed_anchor": {"path": anchor_path, "sha256": anchor_sha256},
		"comparable_for_classification": excluded.is_empty(),
		"excluded_reasons": excluded,
		"executions": {
			"plan": "explain binder, no execution",
			"t1": "Toron EXPLAIN ANALYZE, local QueryCoordinator, result discarded",
			"t2": "Toron perf-run-one child, node attribution and parquet scan ledger",
			"d0": "DuckDB EXPLAIN (FORMAT JSON), no execution",
			"d1": "DuckDB profiled execution, operator metrics",
		},
		"profiling_caveat": "every execution here is diagnostic and profiled; absolute times carry \
			profiling overhead and are not comparable with board medians",
		"correspondence": correspondence,
		"identity": identity,
		"toron": {
			"joins": toron_joins,
			"plan": toron_plan["plan"],
			"t1": toron_plan["t1"],
			"t2": t2,
		},
		"duckdb": {
			"version": duckdb_version,
			"d0_names": d0_names.as_ref().ok(),
			"d0_raw": d0.stdout,
			"d1_result_sha256": d1_digest,
			"d1": d1_operators,
		},
	});
	let path = output_dir.join("structural_census.json");
	std::fs::write(
		&path,
		serde_json::to_string_pretty(&artifact).map_err(|error| error.to_string())?,
	)
	.map_err(|error| format!("write {}: {error}", path.display()))?;
	Ok(path)
}

/// ADR 603: the anchor and both memory panels compared, from their `samples.jsonl` files.
fn memory_panels_summary_command(args: &[String]) -> ExitCode {
	let read = |flag: &str| -> Result<Vec<serde_json::Value>, String> {
		let path = flag_value(args, flag)
			.ok_or_else(|| format!("memory-panels-summary requires {flag} <jsonl>"))?;
		std::fs::read_to_string(path)
			.map_err(|error| format!("read {path}: {error}"))?
			.lines()
			.filter(|line| !line.trim().is_empty())
			.map(|line| {
				serde_json::from_str(line).map_err(|error| format!("parse {path}: {error}"))
			})
			.collect()
	};
	let inputs = [
		"--anchor",
		"--panel-a",
		"--panel-b",
		"--sweep-anchor",
		"--sweep-panel-a",
	]
	.map(read);
	match inputs {
		[
			Ok(anchor),
			Ok(panel_a),
			Ok(panel_b),
			Ok(sweep_anchor),
			Ok(sweep_panel_a),
		] => {
			println!(
				"{}",
				toron_tpch::perf_board::memory_panels_summary(
					&anchor,
					&panel_a,
					&panel_b,
					&sweep_anchor,
					&sweep_panel_a
				)
			);
			ExitCode::SUCCESS
		}
		inputs => harness_error(
			&inputs
				.into_iter()
				.filter_map(Result::err)
				.collect::<Vec<_>>()
				.join("; "),
		),
	}
}

/// ADR 603: one digest path for a DuckDB result, shared by timed children and the sweep: the
/// parsed JSON result re-serialized with `serde_json::to_vec`, then SHA-256.
fn duckdb_result_sha256(result: &serde_json::Value) -> Result<String, String> {
	serde_json::to_vec(result)
		.map(|bytes| toron_tpch::evidence::sha256_bytes(&bytes))
		.map_err(|error| format!("serialize DuckDB result: {error}"))
}

/// ADR 603: the read-back statement; its one-row JSON array precedes the query's result.
const DUCKDB_MEMORY_LIMIT_READ_BACK: &str =
	"SELECT current_setting('memory_limit') AS duckdb_memory_limit_observed;";

/// The read-back's value: the first JSON array whose first row names the read-back column.
fn duckdb_memory_limit_read_back(arrays: &[serde_json::Value]) -> Option<String> {
	arrays.iter().find_map(|array| {
		array
			.as_array()?
			.first()?
			.get("duckdb_memory_limit_observed")?
			.as_str()
			.map(str::to_string)
	})
}

/// The query's result: the last JSON array, provided it is not the read-back's.
fn duckdb_query_result(arrays: &[serde_json::Value]) -> Option<&serde_json::Value> {
	let last = arrays.last()?;
	let is_read_back = last
		.as_array()
		.and_then(|rows| rows.first())
		.is_some_and(|row| row.get("duckdb_memory_limit_observed").is_some());
	(!is_read_back).then_some(last)
}

/// ADR 603: a fresh, empty directory for one DuckDB child's temporary files.
fn fresh_duckdb_temp_directory() -> Result<PathBuf, String> {
	static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
	let directory = std::env::temp_dir().join(format!(
		"toron-duckdb-{}-{}",
		std::process::id(),
		NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
	));
	if directory.exists() {
		return Err(format!(
			"DuckDB temp directory {} already exists",
			directory.display()
		));
	}
	std::fs::create_dir_all(&directory).map_err(|error| {
		format!(
			"create DuckDB temp directory {}: {error}",
			directory.display()
		)
	})?;
	Ok(directory)
}

fn duckdb_temp_directory_sql(directory: &Path) -> String {
	format!(
		"SET temp_directory={};\n",
		sql_string_literal(&directory.display().to_string())
	)
}

#[derive(Debug)]
struct DuckDbCommandOutput {
	stdout: String,
	stderr: String,
	parent_wall_ms: f64,
	peak_rss_kib: Option<u64>,
	/// ADR 603: the harness's own `ru_maxrss` read just before the child was spawned.
	parent_peak_rss_kib_at_spawn: Option<u64>,
	status_success: bool,
}

#[cfg(unix)]
fn run_duckdb_script(duckdb_bin: &str, script: &str) -> Result<DuckDbCommandOutput, String> {
	let parent_peak_rss_kib_at_spawn = toron_tpch::perf_board::self_peak_rss_kib();
	let start = Instant::now();
	let mut child = Command::new(duckdb_bin)
		.arg(":memory:")
		.stdin(Stdio::piped())
		.stdout(Stdio::piped())
		.stderr(Stdio::piped())
		.spawn()
		.map_err(|error| format!("spawn duckdb: {error}"))?;
	child
		.stdin
		.take()
		.ok_or_else(|| "duckdb stdin was not piped".to_string())?
		.write_all(script.as_bytes())
		.map_err(|error| format!("write duckdb script: {error}"))?;
	let pid = child.id() as libc::pid_t;
	let mut stdout = child
		.stdout
		.take()
		.ok_or_else(|| "duckdb stdout was not piped".to_string())?;
	let mut stderr = child
		.stderr
		.take()
		.ok_or_else(|| "duckdb stderr was not piped".to_string())?;
	let stdout_reader = std::thread::spawn(move || {
		let mut output = String::new();
		stdout.read_to_string(&mut output).map(|_| output)
	});
	let stderr_reader = std::thread::spawn(move || {
		let mut output = String::new();
		stderr.read_to_string(&mut output).map(|_| output)
	});
	let mut status: libc::c_int = 0;
	let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
	let waited = unsafe { libc::wait4(pid, &mut status, 0, usage.as_mut_ptr()) };
	if waited < 0 {
		return Err(format!("wait4 duckdb: {}", std::io::Error::last_os_error()));
	}
	let usage = unsafe { usage.assume_init() };
	let stdout = stdout_reader
		.join()
		.map_err(|_| "duckdb stdout reader panicked".to_string())?
		.map_err(|error| format!("read duckdb stdout: {error}"))?;
	let stderr = stderr_reader
		.join()
		.map_err(|_| "duckdb stderr reader panicked".to_string())?
		.map_err(|error| format!("read duckdb stderr: {error}"))?;
	Ok(DuckDbCommandOutput {
		stdout,
		stderr,
		parent_wall_ms: start.elapsed().as_secs_f64() * 1000.0,
		peak_rss_kib: Some(usage.ru_maxrss.max(0) as u64),
		parent_peak_rss_kib_at_spawn,
		status_success: libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
	})
}

#[cfg(not(unix))]
fn run_duckdb_script(duckdb_bin: &str, script: &str) -> Result<DuckDbCommandOutput, String> {
	let start = Instant::now();
	let mut child = Command::new(duckdb_bin)
		.arg(":memory:")
		.stdin(Stdio::piped())
		.stdout(Stdio::piped())
		.stderr(Stdio::piped())
		.spawn()
		.map_err(|error| format!("spawn duckdb: {error}"))?;
	child
		.stdin
		.take()
		.ok_or_else(|| "duckdb stdin was not piped".to_string())?
		.write_all(script.as_bytes())
		.map_err(|error| format!("write duckdb script: {error}"))?;
	let output = child
		.wait_with_output()
		.map_err(|error| format!("wait duckdb: {error}"))?;
	Ok(DuckDbCommandOutput {
		stdout: String::from_utf8_lossy(&output.stdout).to_string(),
		stderr: String::from_utf8_lossy(&output.stderr).to_string(),
		parent_wall_ms: start.elapsed().as_secs_f64() * 1000.0,
		peak_rss_kib: None,
		parent_peak_rss_kib_at_spawn: None,
		status_success: output.status.success(),
	})
}

fn duckdb_describe_schema_fingerprint(value: &serde_json::Value) -> Result<String, String> {
	let rows = value
		.as_array()
		.ok_or_else(|| "DuckDB DESCRIBE JSON was not an array".to_string())?;
	let mut fields = Vec::new();
	for row in rows {
		let object = row
			.as_object()
			.ok_or_else(|| "DuckDB DESCRIBE row was not an object".to_string())?;
		let name = object
			.get("column_name")
			.or_else(|| object.get("COLUMN_NAME"))
			.and_then(serde_json::Value::as_str)
			.ok_or_else(|| format!("DuckDB DESCRIBE row missing column_name: {row}"))?;
		let data_type = object
			.get("column_type")
			.or_else(|| object.get("COLUMN_TYPE"))
			.and_then(serde_json::Value::as_str)
			.ok_or_else(|| format!("DuckDB DESCRIBE row missing column_type: {row}"))?;
		fields.push((name.to_string(), normalize_duckdb_type(data_type)));
	}
	Ok(schema_fingerprint_from_duckdb_fields(&fields))
}

fn duckdb_count_result(value: &serde_json::Value) -> Result<u64, String> {
	let rows = value
		.as_array()
		.ok_or_else(|| "DuckDB COUNT JSON was not an array".to_string())?;
	let row = rows
		.first()
		.and_then(serde_json::Value::as_object)
		.ok_or_else(|| "DuckDB COUNT JSON had no object row".to_string())?;
	row.values()
		.find_map(serde_json::Value::as_u64)
		.ok_or_else(|| format!("DuckDB COUNT row had no u64 value: {value}"))
}

fn parse_duckdb_timer_ms(output: &str) -> Option<f64> {
	for line in output.lines().rev() {
		if !line.to_ascii_lowercase().contains("run time") {
			continue;
		}
		if let Some(real) = line.split("real").nth(1) {
			if let Some(value) = real
				.split_whitespace()
				.find_map(|token| token.trim().parse::<f64>().ok())
			{
				return Some(value * 1000.0);
			}
		}
	}
	None
}

fn extract_json_arrays(input: &str) -> Vec<serde_json::Value> {
	let mut arrays = Vec::new();
	let mut start = None;
	let mut depth = 0_i32;
	let mut in_string = false;
	let mut escaped = false;
	for (index, byte) in input.bytes().enumerate() {
		if let Some(start_index) = start {
			if in_string {
				if escaped {
					escaped = false;
				} else if byte == b'\\' {
					escaped = true;
				} else if byte == b'"' {
					in_string = false;
				}
				continue;
			}
			match byte {
				b'"' => in_string = true,
				b'[' => depth += 1,
				b']' => {
					depth -= 1;
					if depth == 0 {
						if let Ok(value) = serde_json::from_str(&input[start_index..=index]) {
							arrays.push(value);
						}
						start = None;
					}
				}
				_ => {}
			}
		} else if byte == b'[' {
			start = Some(index);
			depth = 1;
			in_string = false;
			escaped = false;
		}
	}
	arrays
}

fn apply_duckdb_output_sanity_flags(raw_rows: &mut [serde_json::Value]) {
	let mut toron_outputs: BTreeMap<String, (u64, String)> = BTreeMap::new();
	for row in raw_rows.iter() {
		if row.get("engine").and_then(serde_json::Value::as_str) != Some("toron") {
			continue;
		}
		if row
			.get("warmup")
			.and_then(serde_json::Value::as_bool)
			.unwrap_or(false)
		{
			continue;
		}
		if row.get("status").and_then(serde_json::Value::as_str) != Some("pass") {
			continue;
		}
		let Some(key) = perf_output_compare_key(row) else {
			continue;
		};
		let Some(row_count) = row
			.get("output_row_count")
			.and_then(serde_json::Value::as_u64)
		else {
			continue;
		};
		let Some(schema) = row
			.get("output_schema_fingerprint")
			.and_then(serde_json::Value::as_str)
		else {
			continue;
		};
		toron_outputs
			.entry(key)
			.or_insert_with(|| (row_count, schema.to_string()));
	}

	for row in raw_rows.iter_mut() {
		if row.get("engine").and_then(serde_json::Value::as_str) != Some("duckdb") {
			continue;
		}
		if row.get("status").and_then(serde_json::Value::as_str) != Some("pass") {
			continue;
		}
		let Some(key) = perf_output_compare_key(row) else {
			continue;
		};
		let Some((toron_count, toron_schema)) = toron_outputs.get(&key) else {
			continue;
		};
		let duckdb_count = row
			.get("output_row_count")
			.and_then(serde_json::Value::as_u64);
		let duckdb_schema = row
			.get("output_schema_fingerprint")
			.and_then(serde_json::Value::as_str);
		let schema_matches = duckdb_schema == Some(toron_schema.as_str());
		if duckdb_count != Some(*toron_count) {
			push_row_flag(row, "row_count_mismatch");
		}
		if !schema_matches {
			push_row_flag(row, "schema_mismatch");
		}
	}
}

fn perf_output_compare_key(row: &serde_json::Value) -> Option<String> {
	let query = row.get("query").and_then(serde_json::Value::as_str)?;
	let mode = row.get("mode").and_then(serde_json::Value::as_str)?;
	let sf = row
		.get("scale_factor")
		.and_then(serde_json::Value::as_f64)?;
	Some(format!("{query}\t{mode}\t{sf}"))
}

fn push_row_flag(row: &mut serde_json::Value, flag: &str) {
	let Some(object) = row.as_object_mut() else {
		return;
	};
	let entry = object
		.entry("flags".to_string())
		.or_insert_with(|| serde_json::Value::Array(Vec::new()));
	let Some(flags) = entry.as_array_mut() else {
		return;
	};
	if !flags.iter().any(|existing| existing.as_str() == Some(flag)) {
		flags.push(serde_json::json!(flag));
	}
}

fn structured_perf_failure(row: &serde_json::Value) -> bool {
	matches!(
		row.get("status").and_then(serde_json::Value::as_str),
		Some("fail" | "blocked" | "not_attempted")
	)
}

#[cfg(unix)]
fn run_perf_child_args(args: &[String]) -> Result<serde_json::Value, String> {
	run_perf_child_args_with_envs(args, &[])
}

#[cfg(unix)]
fn run_perf_child_args_with_envs(
	args: &[String],
	envs: &[(&str, &str)],
) -> Result<serde_json::Value, String> {
	run_perf_child_args_with_envs_wrapped(args, envs, None)
}

/// Run the perf child, optionally under a wrapper command (`perf record ... --`) that receives
/// the child's executable and arguments after its own. The wrapper's exit status is the
/// child's, its stderr is its own, and the child's JSON row still arrives on stdout.
#[cfg(unix)]
fn run_perf_child_args_with_envs_wrapped(
	args: &[String],
	envs: &[(&str, &str)],
	wrapper: Option<&[String]>,
) -> Result<serde_json::Value, String> {
	run_perf_child_with_executable(&toron_binary()?, args, envs, wrapper)
}

/// Run the perf child from an explicit executable. The default runner resolves the executable
/// from this process's own environment, which names the executable the harness was launched
/// from; a driver that compares two staged executables must name the arm's executable here,
/// or every arm silently runs the launcher's binary.
#[cfg(unix)]
fn run_perf_child_with_executable(
	exe: &Path,
	args: &[String],
	envs: &[(&str, &str)],
	wrapper: Option<&[String]>,
) -> Result<serde_json::Value, String> {
	let parent_peak_rss_kib = toron_tpch::perf_board::self_peak_rss_kib();
	let start = Instant::now();
	let mut command = match wrapper {
		Some(wrapper) if !wrapper.is_empty() => {
			let mut command = Command::new(&wrapper[0]);
			command.args(&wrapper[1..]);
			command.arg(exe);
			command
		}
		_ => Command::new(exe),
	};
	command.args(args);
	for (key, value) in envs {
		command.env(key, value);
	}
	let mut child = command
		.stdout(Stdio::piped())
		.stderr(Stdio::piped())
		.spawn()
		.map_err(|error| format!("spawn perf child: {error}"))?;
	let pid = child.id() as libc::pid_t;
	let mut stdout = child
		.stdout
		.take()
		.ok_or_else(|| "perf child stdout was not piped".to_string())?;
	let mut stderr = child
		.stderr
		.take()
		.ok_or_else(|| "perf child stderr was not piped".to_string())?;
	let stdout_reader = std::thread::spawn(move || {
		let mut output = String::new();
		stdout.read_to_string(&mut output).map(|_| output)
	});
	let stderr_reader = std::thread::spawn(move || {
		let mut output = String::new();
		stderr.read_to_string(&mut output).map(|_| output)
	});
	let mut status: libc::c_int = 0;
	let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
	let waited = unsafe { libc::wait4(pid, &mut status, 0, usage.as_mut_ptr()) };
	if waited < 0 {
		return Err(format!(
			"wait4 perf child: {}",
			std::io::Error::last_os_error()
		));
	}
	let usage = unsafe { usage.assume_init() };
	let parent_wall_ms = start.elapsed().as_secs_f64() * 1000.0;
	let stdout = stdout_reader
		.join()
		.map_err(|_| "perf child stdout reader panicked".to_string())?
		.map_err(|error| format!("read perf child stdout: {error}"))?;
	let stderr = stderr_reader
		.join()
		.map_err(|_| "perf child stderr reader panicked".to_string())?
		.map_err(|error| format!("read perf child stderr: {error}"))?;
	let stdout_trimmed = stdout.trim();
	let mut row: serde_json::Value = serde_json::from_str(stdout_trimmed).map_err(|error| {
		format!(
			"perf child emitted invalid JSON (status={status}, stderr={stderr:?}, stdout={stdout_trimmed:?}): {error}"
		)
	})?;
	if (!libc::WIFEXITED(status) || libc::WEXITSTATUS(status) != 0)
		&& !structured_perf_failure(&row)
	{
		return Err(format!(
			"perf child failed status={status} stderr={stderr:?} stdout={stdout_trimmed:?}"
		));
	}
	let object = row
		.as_object_mut()
		.ok_or_else(|| "perf child JSON row was not an object".to_string())?;
	object.insert(
		"parent_wall_ms".to_string(),
		serde_json::json!(parent_wall_ms),
	);
	let peak_rss_kib = usage.ru_maxrss.max(0) as u64;
	object.insert("peak_rss_kib".to_string(), serde_json::json!(peak_rss_kib));
	// ADR 603: the harness's own high-water at spawn, and the child's reading labelled against it.
	object.insert(
		"parent_peak_rss_kib_at_spawn".to_string(),
		serde_json::json!(parent_peak_rss_kib),
	);
	object.insert(
		"peak_rss_label".to_string(),
		serde_json::json!(toron_tpch::perf_board::peak_rss_label(
			Some(peak_rss_kib),
			parent_peak_rss_kib
		)),
	);
	Ok(row)
}

#[cfg(not(unix))]
fn run_perf_child_args(args: &[String]) -> Result<serde_json::Value, String> {
	run_perf_child_args_with_envs(args, &[])
}

#[cfg(not(unix))]
fn run_perf_child_args_with_envs(
	args: &[String],
	envs: &[(&str, &str)],
) -> Result<serde_json::Value, String> {
	let exe = toron_binary()?;
	let start = Instant::now();
	let mut command = Command::new(exe);
	command.args(args);
	for (key, value) in envs {
		command.env(key, value);
	}
	let output = command
		.output()
		.map_err(|error| format!("spawn perf child: {error}"))?;
	let stdout = String::from_utf8_lossy(&output.stdout);
	let stderr = String::from_utf8_lossy(&output.stderr);
	let stdout_trimmed = stdout.trim();
	let mut row: serde_json::Value = serde_json::from_str(stdout_trimmed).map_err(|error| {
		format!(
			"perf child emitted invalid JSON (status={}, stderr={stderr:?}, stdout={stdout_trimmed:?}): {error}",
			output.status
		)
	})?;
	if !output.status.success() && !structured_perf_failure(&row) {
		return Err(format!(
			"perf child failed status={} stderr={stderr:?} stdout={stdout_trimmed:?}",
			output.status
		));
	}
	let object = row
		.as_object_mut()
		.ok_or_else(|| "perf child JSON row was not an object".to_string())?;
	object.insert(
		"parent_wall_ms".to_string(),
		serde_json::json!(start.elapsed().as_secs_f64() * 1000.0),
	);
	object.insert("peak_rss_kib".to_string(), serde_json::Value::Null);
	Ok(row)
}

fn summarize_perf_rows(
	raw_rows: &[serde_json::Value],
	runs: usize,
	warmup: usize,
	baseline_rows: &BTreeMap<String, serde_json::Value>,
) -> Vec<serde_json::Value> {
	let mut by_key: BTreeMap<String, Vec<&serde_json::Value>> = BTreeMap::new();
	for row in raw_rows {
		if row
			.get("warmup")
			.and_then(serde_json::Value::as_bool)
			.unwrap_or(false)
		{
			continue;
		}
		if let (Some(engine), Some(query)) = (
			row.get("engine").and_then(serde_json::Value::as_str),
			row.get("query").and_then(serde_json::Value::as_str),
		) {
			by_key
				.entry(perf_summary_key(engine, query))
				.or_default()
				.push(row);
		}
	}
	by_key
		.into_iter()
		.map(|(key, rows)| summarize_query_rows(rows, runs, warmup, baseline_rows.get(&key)))
		.collect()
}

fn summarize_query_rows(
	rows: Vec<&serde_json::Value>,
	runs: usize,
	warmup: usize,
	baseline: Option<&serde_json::Value>,
) -> serde_json::Value {
	let mut execute_ms = rows
		.iter()
		.filter_map(|row| row.get("execute_ms").and_then(serde_json::Value::as_f64))
		.collect::<Vec<_>>();
	let execute_min = execute_ms.iter().copied().reduce(f64::min).unwrap_or(0.0);
	let execute_max = execute_ms.iter().copied().reduce(f64::max).unwrap_or(0.0);
	let execute_median = median(&mut execute_ms);
	let spread_ratio = if execute_median > 0.0 {
		(execute_max - execute_min) / execute_median
	} else {
		0.0
	};
	let raw_execute_samples = rows
		.iter()
		.filter_map(|row| {
			let mut sample = serde_json::json!({
				"run_index": row.get("run_index")?.as_u64()?,
				"execute_ms": row.get("execute_ms")?.as_f64()?,
			});
			if let Some(arm) = row.get("ablation_arm").and_then(serde_json::Value::as_str) {
				sample["ablation_arm"] = serde_json::json!(arm);
			}
			Some(sample)
		})
		.collect::<Vec<_>>();
	let peak_rss_kib_max = rows
		.iter()
		.filter_map(|row| row.get("peak_rss_kib").and_then(serde_json::Value::as_u64))
		.max()
		.unwrap_or(0);
	let spill_bytes_max = rows
		.iter()
		.filter_map(|row| row.get("spill_bytes").and_then(serde_json::Value::as_u64))
		.max()
		.unwrap_or(0);
	let first = rows.first().copied();
	let engine = first
		.and_then(|row| row.get("engine"))
		.and_then(serde_json::Value::as_str)
		.unwrap_or("unknown");
	let query = first
		.and_then(|row| row.get("query"))
		.and_then(serde_json::Value::as_str)
		.unwrap_or("unknown");
	let plan_fingerprint = first
		.and_then(|row| row.get("plan_fingerprint"))
		.and_then(serde_json::Value::as_str)
		.unwrap_or("none");
	let digest = first
		.and_then(|row| row.get("digest"))
		.and_then(serde_json::Value::as_str)
		.unwrap_or("none");
	let status = if rows.iter().all(|row| {
		row.get("status")
			.and_then(serde_json::Value::as_str)
			.unwrap_or("fail")
			== "pass"
	}) {
		"pass"
	} else if rows.iter().all(|row| {
		row.get("status")
			.and_then(serde_json::Value::as_str)
			.unwrap_or("fail")
			== "not_attempted"
	}) {
		"not_attempted"
	} else {
		"fail"
	};
	let output_row_count = first
		.and_then(|row| row.get("output_row_count"))
		.and_then(serde_json::Value::as_u64);
	let output_schema_fingerprint = first
		.and_then(|row| row.get("output_schema_fingerprint"))
		.and_then(serde_json::Value::as_str)
		.unwrap_or("none");
	let engine_version = first
		.and_then(|row| row.get("engine_version"))
		.and_then(serde_json::Value::as_str);
	let duckdb_version = first
		.and_then(|row| row.get("duckdb_version"))
		.and_then(serde_json::Value::as_str);
	let duckdb_executable_sha256 = first
		.and_then(|row| row.get("duckdb_executable_sha256"))
		.and_then(serde_json::Value::as_str);
	let dataset_manifest_fingerprint = first
		.and_then(|row| row.get("dataset_manifest_fingerprint"))
		.and_then(serde_json::Value::as_str);
	let scale_factor = first
		.and_then(|row| row.get("scale_factor"))
		.and_then(serde_json::Value::as_f64)
		.unwrap_or(0.0);
	let mode = first
		.and_then(|row| row.get("mode"))
		.and_then(serde_json::Value::as_str)
		.unwrap_or("unknown");
	let profile = first
		.and_then(|row| row.get("profile"))
		.and_then(serde_json::Value::as_str)
		.unwrap_or("unknown");
	let memory_per_task = first
		.and_then(|row| row.get("memory_per_task"))
		.and_then(serde_json::Value::as_u64);
	let memory_per_task_provenance = first
		.and_then(|row| row.get("memory_per_task_provenance"))
		.and_then(serde_json::Value::as_str)
		.unwrap_or("unrecorded");
	let spill_bytes_reserved = first
		.and_then(|row| row.get("spill_bytes_reserved"))
		.and_then(serde_json::Value::as_u64);
	let spill_bytes_reserved_provenance = first
		.and_then(|row| row.get("spill_bytes_reserved_provenance"))
		.and_then(serde_json::Value::as_str)
		.unwrap_or("unrecorded");
	let partition_work_concurrency_requested = first
		.and_then(|row| row.get("partition_work_concurrency_requested"))
		.and_then(serde_json::Value::as_u64);
	let partition_work_concurrency_requested_provenance = first
		.and_then(|row| row.get("partition_work_concurrency_requested_provenance"))
		.and_then(serde_json::Value::as_str);
	let partition_work_concurrency_observations = first
		.and_then(|row| row.get("partition_work_concurrency_observations"))
		.cloned()
		.unwrap_or(serde_json::Value::Null);
	let (dataset_verification, dataset_verification_changed) = summary_dataset_verification(&rows);
	let mut flags = Vec::new();
	const STABILITY_THRESHOLD: f64 = 0.20;
	const REGRESSION_THRESHOLD: f64 = 0.10;
	if spread_ratio > STABILITY_THRESHOLD {
		push_summary_flag(&mut flags, "unstable");
	}
	if dataset_verification_changed {
		push_summary_flag(&mut flags, "dataset_verification_changed");
	}
	if profile != "release" {
		push_summary_flag(&mut flags, "debug_build");
	}
	for row in &rows {
		if let Some(row_flags) = row.get("flags").and_then(serde_json::Value::as_array) {
			for flag in row_flags {
				if let Some(flag) = flag.as_str() {
					push_summary_flag(&mut flags, flag);
				}
			}
		}
	}
	let mut baseline_execute_ms_median = None;
	if let Some(baseline) = baseline {
		let baseline_plan = baseline
			.get("plan_fingerprint")
			.and_then(serde_json::Value::as_str)
			.unwrap_or("none");
		if baseline_plan != plan_fingerprint {
			push_summary_flag(&mut flags, "plan_changed");
		} else if let Some(baseline_median) = baseline
			.get("execute_ms_median")
			.and_then(serde_json::Value::as_f64)
		{
			baseline_execute_ms_median = Some(baseline_median);
			let baseline_spread = baseline
				.get("execute_ms_spread_ratio")
				.and_then(serde_json::Value::as_f64)
				.unwrap_or(0.0);
			let regression_alarm_band =
				REGRESSION_THRESHOLD.max(2.0 * spread_ratio.max(baseline_spread));
			if baseline_median > 0.0
				&& (execute_median - baseline_median) / baseline_median > regression_alarm_band
			{
				push_summary_flag(&mut flags, "slower_than_baseline");
			}
		}
	}

	serde_json::json!({
		"suite": "tpch_perf_summary",
		"engine": engine,
		"query": query,
		"scale_factor": scale_factor,
		"mode": mode,
		"profile": profile,
		"memory_per_task": memory_per_task,
		"memory_per_task_provenance": memory_per_task_provenance,
		"spill_bytes_reserved": spill_bytes_reserved,
		"spill_bytes_reserved_provenance": spill_bytes_reserved_provenance,
		"partition_work_concurrency_requested": partition_work_concurrency_requested,
		"partition_work_concurrency_requested_provenance": partition_work_concurrency_requested_provenance,
		"partition_work_concurrency_observations": partition_work_concurrency_observations,
		"status": status,
		"runs": runs,
		"warmup": warmup,
		"execute_ms_median": execute_median,
		"execute_ms_min": execute_min,
		"execute_ms_max": execute_max,
		"execute_ms_spread_ratio": spread_ratio,
		"execute_ms_samples": raw_execute_samples,
		"peak_rss_kib_max": peak_rss_kib_max,
		"spill_bytes_max": spill_bytes_max,
		"spill_scope": "whole_query_total",
		"plan_fingerprint": plan_fingerprint,
		"digest": digest,
		"output_row_count": output_row_count,
		"output_schema_fingerprint": output_schema_fingerprint,
		"engine_version": engine_version,
		"duckdb_version": duckdb_version,
		"duckdb_executable_sha256": duckdb_executable_sha256,
		"duckdb_threads": duckdb_thread_setting(),
		"duckdb_threads_provenance": reference_thread_pin_provenance(&duckdb_thread_setting()),
		"polars_max_threads": polars_max_threads_setting(),
		"polars_max_threads_provenance": reference_thread_pin_provenance(&polars_max_threads_setting()),
		"dataset_verification": dataset_verification,
		"dataset_manifest_fingerprint": dataset_manifest_fingerprint,
		"build_commit": rows.last().and_then(|row| row.get("build_commit")).cloned(),
		"binary_modified_unix": rows
			.last()
			.and_then(|row| row.get("binary_modified_unix"))
			.cloned(),
		"local_join_admissions": rows
			.last()
			.and_then(|row| row.get("local_join_admissions"))
			.cloned(),
		"baseline_execute_ms_median": baseline_execute_ms_median,
		"flags": flags,
	})
}

/// The verification the whole summary cell rests on, and whether the measured rows disagreed.
///
/// Unlike the other provenance fields here, this one is not taken from the first row. A summary is
/// one median over several runs, so a cell that mixed a full-hash run with a manifest-only one has no
/// single chain of custody to report — taking either would overstate one of them. Disagreement
/// therefore yields `null` plus a flag rather than a plausible-looking value. Agreement on *absent*
/// (in-memory runs, which load no dataset) is agreement, not disagreement.
fn summary_dataset_verification(rows: &[&serde_json::Value]) -> (serde_json::Value, bool) {
	let mut observed = rows.iter().map(|row| {
		row.get("dataset_verification")
			.unwrap_or(&serde_json::Value::Null)
	});
	let Some(first) = observed.next() else {
		return (serde_json::Value::Null, false);
	};
	if observed.all(|value| value == first) {
		(first.clone(), false)
	} else {
		(serde_json::Value::Null, true)
	}
}

fn perf_summary_key(engine: &str, query: &str) -> String {
	format!("{engine}\t{query}")
}

fn push_summary_flag<'a>(flags: &mut Vec<&'a str>, flag: &'a str) {
	if !flags.contains(&flag) {
		flags.push(flag);
	}
}

fn median(values: &mut [f64]) -> f64 {
	if values.is_empty() {
		return 0.0;
	}
	values.sort_by(|left, right| left.total_cmp(right));
	let mid = values.len() / 2;
	if values.len() % 2 == 0 {
		(values[mid - 1] + values[mid]) / 2.0
	} else {
		values[mid]
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillScope {
	WholeQuery,
	GroupedRoot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScopedSpill {
	pub bytes: u64,
	pub scope: SpillScope,
}

pub fn spill_ratio(numerator: ScopedSpill, denominator: ScopedSpill) -> Result<f64, String> {
	if numerator.scope != denominator.scope {
		return Err("spill ratio requires matching scopes".to_string());
	}
	if denominator.bytes == 0 {
		return Err("spill ratio denominator is zero".to_string());
	}
	Ok(numerator.bytes as f64 / denominator.bytes as f64)
}

pub const MATERIALITY_FLOOR: f64 = 0.10;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PairedShift {
	pub point: f64,
	pub lower: f64,
	pub upper: f64,
	pub pairs: usize,
}

/// Deterministic Hodges–Lehmann paired shift with an exact two-sided
/// Wilcoxon signed-rank interval. Zero differences are omitted from the rank
/// calculation; fewer than six non-zero pairs cannot reach alpha=.05.
pub fn paired_median_shift(diffs: &[f64]) -> Result<PairedShift, String> {
	if diffs.iter().any(|value| !value.is_finite()) {
		return Err("paired differences must be finite".to_string());
	}
	let nonzero = diffs
		.iter()
		.copied()
		.filter(|value| *value != 0.0)
		.collect::<Vec<_>>();
	if nonzero.len() < 6 {
		return Err("at least six non-zero paired differences are required".to_string());
	}
	if nonzero.len() > 20 {
		return Err("at most twenty non-zero paired differences are supported".to_string());
	}
	let mut walsh = Vec::with_capacity(nonzero.len() * (nonzero.len() + 1) / 2);
	for (index, left) in nonzero.iter().enumerate() {
		for right in nonzero.iter().skip(index) {
			walsh.push((left + right) / 2.0);
		}
	}
	walsh.sort_by(f64::total_cmp);
	let point = walsh[walsh.len() / 2];
	let n = nonzero.len();
	let total_rank_sum = n * (n + 1) / 2;
	let mut counts = vec![0_u64; total_rank_sum + 1];
	for mask in 0_u64..(1_u64 << n) {
		let mut sum = 0usize;
		for rank in 0..n {
			if mask & (1_u64 << rank) != 0 {
				sum += rank + 1;
			}
		}
		counts[sum] += 1;
	}
	let total = 1_u64 << n;
	let mut lower_tail = 0_u64;
	let mut critical = 0usize;
	for (rank, count) in counts.iter().enumerate() {
		lower_tail += count;
		if (lower_tail * 2) as f64 / total as f64 <= 0.05 {
			critical = rank;
		}
	}
	let lower_index = critical.saturating_sub(1).min(walsh.len() - 1);
	let upper_index = walsh.len().saturating_sub(critical + 1);
	Ok(PairedShift {
		point,
		lower: walsh[lower_index],
		upper: walsh[upper_index],
		pairs: n,
	})
}

pub fn material_effect(point: f64, baseline_median: f64) -> bool {
	point.abs() >= MATERIALITY_FLOOR * baseline_median.abs()
}

fn read_baseline_rows(path: &Path) -> BTreeMap<String, serde_json::Value> {
	let Ok(contents) = std::fs::read_to_string(path) else {
		return BTreeMap::new();
	};
	contents
		.lines()
		.filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
		.filter_map(|row| {
			let engine = row
				.get("engine")
				.and_then(serde_json::Value::as_str)
				.unwrap_or("toron");
			row.get("query")
				.and_then(serde_json::Value::as_str)
				.map(|query| (perf_summary_key(engine, query), row.clone()))
		})
		.collect()
}

fn write_perf_outputs(
	output_dir: &Path,
	baseline_path: &Path,
	raw_rows: &[serde_json::Value],
	summary_rows: &[serde_json::Value],
) -> std::io::Result<()> {
	std::fs::create_dir_all(output_dir)?;
	let raw = raw_rows
		.iter()
		.map(serde_json::Value::to_string)
		.collect::<Vec<_>>()
		.join("\n");
	let summary = summary_rows
		.iter()
		.map(serde_json::Value::to_string)
		.collect::<Vec<_>>()
		.join("\n");
	std::fs::write(output_dir.join("latest_raw.jsonl"), format!("{raw}\n"))?;
	std::fs::write(
		output_dir.join("latest_summary.jsonl"),
		format!("{summary}\n"),
	)?;
	if !baseline_path.exists() {
		if perf_baseline_is_recordable(summary_rows) {
			if let Some(parent) = baseline_path.parent() {
				std::fs::create_dir_all(parent)?;
			}
			std::fs::write(baseline_path, format!("{summary}\n"))?;
		} else {
			eprintln!(
				"perf baseline not recorded: summary rows must be release, passing, stable, and \
				 agree on dataset verification"
			);
		}
	}
	Ok(())
}

const ADR449_PAIRED_JOURNAL: &str = "adr449_paired_samples.jsonl";
const ADR449_PAIRED_MANIFEST: &str = "adr449_paired_run.json";
const PERF_SAMPLE_JOURNAL: &str = "samples.jsonl";

/// Every measured row, fsynced as it is produced.
///
/// The summary and raw artifacts are only written once the whole sweep finishes, so without this a
/// run that dies partway through leaves nothing at all behind — hours of measurement held in memory
/// and lost. ADR 449 built this durability for the paired-ablation rail; it is not paired-specific,
/// and every perf run gets it.
#[derive(Debug, Clone)]
struct PerfSampleJournal {
	path: PathBuf,
	rows: Vec<serde_json::Value>,
}

/// A fresh journal for an ordinary perf run. Truncated up front so its rows always describe this
/// run: resume belongs to the paired rail, which locks the config it would be resuming into.
fn prepare_perf_sample_journal(output_dir: &Path) -> Result<PerfSampleJournal, String> {
	std::fs::create_dir_all(output_dir)
		.map_err(|error| format!("create {}: {error}", output_dir.display()))?;
	let path = output_dir.join(PERF_SAMPLE_JOURNAL);
	std::fs::write(&path, "")
		.map_err(|error| format!("truncate perf sample journal {}: {error}", path.display()))?;
	Ok(PerfSampleJournal {
		path,
		rows: Vec::new(),
	})
}

impl PerfSampleJournal {
	fn contains(&self, query: &str, engine: &str, run_index: u32, warmup: bool) -> bool {
		self.rows.iter().any(|row| {
			row.get("query").and_then(serde_json::Value::as_str) == Some(query)
				&& row.get("engine").and_then(serde_json::Value::as_str) == Some(engine)
				&& row.get("run_index").and_then(serde_json::Value::as_u64)
					== Some(u64::from(run_index))
				&& row.get("warmup").and_then(serde_json::Value::as_bool) == Some(warmup)
		})
	}

	fn append(&self, row: &serde_json::Value) -> Result<(), String> {
		let mut file = OpenOptions::new()
			.create(true)
			.append(true)
			.open(&self.path)
			.map_err(|error| {
				format!(
					"open paired-ablation journal {}: {error}",
					self.path.display()
				)
			})?;
		serde_json::to_writer(&mut file, row)
			.map_err(|error| format!("encode paired-ablation sample: {error}"))?;
		file.write_all(b"\n")
			.map_err(|error| format!("append paired-ablation sample: {error}"))?;
		file.sync_data()
			.map_err(|error| format!("sync paired-ablation sample: {error}"))
	}
}

fn prepare_paired_ablation_journal(
	output_dir: &Path,
	query: &str,
	mode: &str,
	sf: f64,
	data_source: &str,
	data_dir: &Path,
	runs: usize,
	warmup: usize,
	dataset_fingerprint: &str,
	switch: &PairedSwitch,
	paired_gate: PairedGate,
	fixed_materiality_floor_ms: Option<f64>,
	budget: GaugeBudget,
) -> Result<PerfSampleJournal, String> {
	if dataset_fingerprint.is_empty() {
		return Err("paired-ablation manifest requires a dataset fingerprint".to_string());
	}
	let memory_per_task = budget.memory_per_task;
	let spill_bytes_reserved = budget.spill_bytes_reserved;
	std::fs::create_dir_all(output_dir)
		.map_err(|error| format!("create paired-ablation output directory: {error}"))?;
	let journal_path = output_dir.join(ADR449_PAIRED_JOURNAL);
	let manifest_path = output_dir.join(ADR449_PAIRED_MANIFEST);
	// The fingerprint arrives from the gate-zero record of THIS launch, so a
	// resume only proceeds when the stored manifest matches the dataset the
	// live gate zero just revalidated. A pre-fingerprint manifest can never
	// match and is refused, not grandfathered.
	let expected = serde_json::json!({
		"suite": "adr449_paired_ablation",
		"query": query,
		"mode": mode,
		"scale_factor": sf,
		"data_source": data_source,
		"data_dir": data_dir.display().to_string(),
		"runs": runs,
		"warmup": warmup,
		"dataset_fingerprint": dataset_fingerprint,
		// A cohort's budget regime is part of its identity (ADR 454/455): a
		// resume across different budget coordinates would mix regimes exactly
		// like a resume across datasets.
		"memory_per_task": memory_per_task,
		"spill_bytes_reserved": spill_bytes_reserved,
		"switch_env": switch.env,
		"switch_on_value": switch.on_value,
		"switch_off_value": switch.off_value,
		"paired_gate": paired_gate.as_str(),
		"fixed_materiality_floor_ms": fixed_materiality_floor_ms,
	});
	if manifest_path.exists() {
		let bytes = std::fs::read(&manifest_path)
			.map_err(|error| format!("read paired-ablation manifest: {error}"))?;
		let observed: serde_json::Value = serde_json::from_slice(&bytes)
			.map_err(|error| format!("parse paired-ablation manifest: {error}"))?;
		if observed != expected {
			return Err(format!(
				"paired-ablation manifest mismatch at {}; use a fresh output directory rather than mixing cohorts",
				manifest_path.display()
			));
		}
	} else {
		if journal_path.exists() {
			return Err(format!(
				"paired-ablation journal {} exists without its manifest; refuse to resume ambiguous samples",
				journal_path.display()
			));
		}
		write_json_atomically(&manifest_path, &expected)?;
	}
	let rows = if journal_path.exists() {
		let body = std::fs::read_to_string(&journal_path)
			.map_err(|error| format!("read paired-ablation journal: {error}"))?;
		body.lines()
			.enumerate()
			.map(|(line_number, line)| {
				serde_json::from_str(line).map_err(|error| {
					format!(
						"parse paired-ablation journal {} line {}: {error}",
						journal_path.display(),
						line_number + 1
					)
				})
			})
			.collect::<Result<Vec<_>, _>>()?
	} else {
		Vec::new()
	};
	Ok(PerfSampleJournal {
		path: journal_path,
		rows,
	})
}

fn write_json_atomically(path: &Path, value: &serde_json::Value) -> Result<(), String> {
	let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
	let body = serde_json::to_string_pretty(value)
		.map_err(|error| format!("encode {}: {error}", path.display()))?;
	std::fs::write(&temporary, format!("{body}\n"))
		.map_err(|error| format!("write {}: {error}", temporary.display()))?;
	std::fs::rename(&temporary, path).map_err(|error| {
		format!(
			"rename {} to {}: {error}",
			temporary.display(),
			path.display()
		)
	})
}

fn perf_baseline_is_recordable(summary_rows: &[serde_json::Value]) -> bool {
	!summary_rows.is_empty()
		&& summary_rows.iter().all(|row| {
			row.get("status").and_then(serde_json::Value::as_str) == Some("pass")
				&& row.get("profile").and_then(serde_json::Value::as_str) == Some("release")
				&& !row
					.get("flags")
					.and_then(serde_json::Value::as_array)
					.into_iter()
					.flatten()
					.filter_map(serde_json::Value::as_str)
					// A baseline is what later runs are measured against, so it may not rest on a
					// cell whose dataset identity moved between runs.
					.any(|flag| {
						flag == "debug_build"
							|| flag == "unstable" || flag == "dataset_verification_changed"
					})
		})
}

fn print_perf_summary(row: &serde_json::Value) {
	let engine = row
		.get("engine")
		.and_then(serde_json::Value::as_str)
		.unwrap_or("unknown");
	let query = row
		.get("query")
		.and_then(serde_json::Value::as_str)
		.unwrap_or("unknown");
	let status = row
		.get("status")
		.and_then(serde_json::Value::as_str)
		.unwrap_or("unknown");
	let median = row
		.get("execute_ms_median")
		.and_then(serde_json::Value::as_f64)
		.unwrap_or(0.0);
	let min = row
		.get("execute_ms_min")
		.and_then(serde_json::Value::as_f64)
		.unwrap_or(0.0);
	let spread = row
		.get("execute_ms_spread_ratio")
		.and_then(serde_json::Value::as_f64)
		.unwrap_or(0.0);
	let rss = row
		.get("peak_rss_kib_max")
		.and_then(serde_json::Value::as_u64)
		.unwrap_or(0);
	let spill = row
		.get("spill_bytes_max")
		.and_then(serde_json::Value::as_u64)
		.unwrap_or(0);
	let flags = row
		.get("flags")
		.and_then(serde_json::Value::as_array)
		.map(|flags| {
			flags
				.iter()
				.filter_map(serde_json::Value::as_str)
				.collect::<Vec<_>>()
				.join(",")
		})
		.unwrap_or_default();
	println!(
		"tpch_perf engine={engine} query={query} status={status} execute_ms_median={median:.1} execute_ms_min={min:.1} spread={spread:.3} peak_rss_kib={rss} spill_bytes={spill} flags={}",
		if flags.is_empty() { "none" } else { &flags }
	);
}

fn default_baseline_name(sf: f64, mode: &str) -> String {
	format!(
		"baseline_sf{}_{}.jsonl",
		sf.to_string().replace('.', "_"),
		mode
	)
}

fn flag_value<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
	args.iter()
		.position(|arg| arg == flag)
		.and_then(|index| args.get(index + 1))
		.map(String::as_str)
}

fn flag_present(args: &[String], flag: &str) -> bool {
	args.iter().any(|arg| arg == flag)
}

#[derive(Debug, Clone, PartialEq)]
struct PairedSwitch {
	env: String,
	on_value: String,
	off_value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PairedGate {
	PlanDistinct,
	ShapePreservingSpillIndex,
	RouteDistinct,
}

impl PairedGate {
	fn as_str(self) -> &'static str {
		match self {
			Self::PlanDistinct => "plan-distinct",
			Self::ShapePreservingSpillIndex => "shape-preserving-spill-index",
			Self::RouteDistinct => "route-distinct",
		}
	}
}

const PAIRED_DEFAULT_ENV: &str = "TORON_ABLATE_LOCAL_RUNTIME_FILTER";
const PARTITIONED_SCAN_BUILD_INDEX_AT_FLUSH_ENV: &str =
	"TORON_PARTITIONED_SCAN_BUILD_INDEX_AT_FLUSH";

fn paired_gate_from_args(args: &[String]) -> Result<PairedGate, String> {
	match flag_value(args, "--paired-gate").unwrap_or("plan-distinct") {
		"plan-distinct" => Ok(PairedGate::PlanDistinct),
		"shape-preserving-spill-index" => Ok(PairedGate::ShapePreservingSpillIndex),
		"route-distinct" => Ok(PairedGate::RouteDistinct),
		other => Err(format!("unsupported --paired-gate {other:?}")),
	}
}

fn paired_switch_from_args(args: &[String]) -> PairedSwitch {
	PairedSwitch {
		env: flag_value(args, "--paired-env")
			.unwrap_or(PAIRED_DEFAULT_ENV)
			.to_string(),
		// The filter's ON arm disables ablation, hence the inverted defaults.
		on_value: flag_value(args, "--paired-on").unwrap_or("0").to_string(),
		off_value: flag_value(args, "--paired-off").unwrap_or("1").to_string(),
	}
}

/// ADR 469: the plan-distinctness gate — a non-reservoir switch's analog of gate
/// zero. Arms must differ in plan fingerprint (the switch does something) and
/// agree in output digest (it changes shape, never answers).
fn adr469_plan_gate_passes(on_row: &serde_json::Value, off_row: &serde_json::Value) -> bool {
	let field = |row: &serde_json::Value, key: &str| {
		row.get(key)
			.and_then(serde_json::Value::as_str)
			.map(ToOwned::to_owned)
	};
	on_row.get("status").and_then(serde_json::Value::as_str) == Some("pass")
		&& off_row.get("status").and_then(serde_json::Value::as_str) == Some("pass")
		&& field(on_row, "plan_fingerprint").is_some()
		&& field(on_row, "plan_fingerprint") != field(off_row, "plan_fingerprint")
		&& field(on_row, "digest").is_some()
		&& field(on_row, "digest") == field(off_row, "digest")
}

fn run_adr469_plan_gate(
	query: &str,
	mode: &str,
	sf: f64,
	data_source: &str,
	data_dir: &Path,
	switch: &PairedSwitch,
) -> Result<serde_json::Value, String> {
	let arm = |value: &str| {
		run_perf_child_with_envs(
			query,
			mode,
			sf,
			0,
			data_source,
			data_dir,
			&[(switch.env.as_str(), value)],
		)
	};
	let on_row = arm(&switch.on_value)?;
	let off_row = arm(&switch.off_value)?;
	let passed = adr469_plan_gate_passes(&on_row, &off_row);
	Ok(serde_json::json!({
		"suite": "adr469_plan_gate",
		"switch_env": switch.env,
		"on_value": switch.on_value,
		"off_value": switch.off_value,
		"on_plan": on_row.get("plan_fingerprint"),
		"off_plan": off_row.get("plan_fingerprint"),
		"digests_match": on_row.get("digest") == off_row.get("digest"),
		"passed": passed,
	}))
}

fn invocation_diagnostic<'a>(
	row: &'a serde_json::Value,
	name: &str,
) -> Result<&'a serde_json::Value, String> {
	let invocations = row
		.pointer("/node_attribution/aggregate_invocations")
		.and_then(serde_json::Value::as_array)
		.ok_or_else(|| "shape gate row lacks node-attribution invocations".to_string())?;
	let matches = invocations
		.iter()
		.flat_map(|invocation| {
			invocation
				.get("diagnostics")
				.and_then(serde_json::Value::as_array)
				.into_iter()
				.flatten()
		})
		.filter(|diagnostic| {
			diagnostic.get("name").and_then(serde_json::Value::as_str) == Some(name)
		})
		.collect::<Vec<_>>();
	if matches.len() != 1 {
		return Err(format!(
			"shape gate expected exactly one {name} diagnostic, found {}",
			matches.len()
		));
	}
	matches[0]
		.get("value")
		.ok_or_else(|| format!("shape gate {name} diagnostic lacks value"))
}

fn required_u64(value: &serde_json::Value, field: &str) -> Result<u64, String> {
	value
		.get(field)
		.and_then(serde_json::Value::as_u64)
		.ok_or_else(|| format!("shape gate field {field} is missing or nonnumeric"))
}

fn spill_subleaf_rows(row: &serde_json::Value, stage: &str) -> Result<u64, String> {
	let invocations = row
		.pointer("/node_attribution/aggregate_invocations")
		.and_then(serde_json::Value::as_array)
		.ok_or_else(|| "shape gate row lacks node-attribution invocations".to_string())?;
	let matches = invocations
		.iter()
		.flat_map(|invocation| {
			invocation
				.get("spill_io")
				.and_then(serde_json::Value::as_array)
				.into_iter()
				.flatten()
		})
		.flat_map(|parent| {
			parent
				.get("subleaves")
				.and_then(serde_json::Value::as_array)
				.into_iter()
				.flatten()
		})
		.filter(|leaf| leaf.get("stage").and_then(serde_json::Value::as_str) == Some(stage))
		.collect::<Vec<_>>();
	if matches.len() != 1 {
		return Err(format!(
			"shape gate expected exactly one {stage} spill-I/O leaf, found {}",
			matches.len()
		));
	}
	matches[0]
		.get("rows")
		.and_then(serde_json::Value::as_u64)
		.ok_or_else(|| format!("shape gate spill-I/O leaf {stage} has nonnumeric rows"))
}

fn shape_preserving_spill_index_gate_record(
	on_row: &serde_json::Value,
	off_row: &serde_json::Value,
) -> Result<serde_json::Value, String> {
	for (label, row) in [("candidate", on_row), ("control", off_row)] {
		if row.get("status").and_then(serde_json::Value::as_str) != Some("pass") {
			return Err(format!("shape gate {label} row did not pass"));
		}
		if row
			.get("dataset_verification")
			.and_then(serde_json::Value::as_str)
			!= Some("full_hash")
		{
			return Err(format!("shape gate {label} row was not full-hash verified"));
		}
	}
	let content_identity = toron_tpch::evidence::compare_row_identities(on_row, off_row);
	if !content_identity.would_admit {
		return Err(format!(
			"shape gate content identity refused: {:?}",
			content_identity.reasons
		));
	}
	for field in [
		"dataset_verification",
		"plan_fingerprint",
		"digest",
		"output_schema_fingerprint",
		"output_row_count",
		"join_spill_observations",
		"spill_bytes",
		"memory_per_task",
		"memory_per_task_provenance",
		"spill_bytes_reserved",
		"spill_bytes_reserved_provenance",
	] {
		let on = on_row
			.get(field)
			.ok_or_else(|| format!("shape gate candidate row lacks {field}"))?;
		let off = off_row
			.get(field)
			.ok_or_else(|| format!("shape gate control row lacks {field}"))?;
		if on != off {
			return Err(format!("shape gate arms disagree on {field}"));
		}
	}
	if on_row
		.get("memory_per_task_provenance")
		.and_then(serde_json::Value::as_str)
		!= Some("explicit")
		|| on_row
			.get("spill_bytes_reserved_provenance")
			.and_then(serde_json::Value::as_str)
			!= Some("explicit")
	{
		return Err("shape gate requires explicit memory and spill budgets".to_string());
	}
	let candidate = invocation_diagnostic(on_row, "partitioned_scan_build_index_lifecycle")?;
	let control = invocation_diagnostic(off_row, "partitioned_scan_build_index_lifecycle")?;
	let candidate_residency = invocation_diagnostic(on_row, "partitioned_build_residency")?;
	let control_residency = invocation_diagnostic(off_row, "partitioned_build_residency")?;
	for field in [
		"partition_count",
		"group_count",
		"page_count",
		"page_rows",
		"page_ciphertext_bytes",
		"spill_bytes",
	] {
		if candidate.get(field) != control.get(field) {
			return Err(format!("shape gate lifecycle arms disagree on {field}"));
		}
	}
	let partition_count = required_u64(candidate, "partition_count")?;
	let group_count = required_u64(candidate, "group_count")?;
	let page_count = required_u64(candidate, "page_count")?;
	if partition_count != 16 || group_count != 4 || page_count == 0 {
		return Err(format!(
			"shape gate expected q17's 16 partitions / 4 groups / nonempty pages, got {partition_count}/{group_count}/{page_count}"
		));
	}
	if candidate.get("mode").and_then(serde_json::Value::as_str) != Some("index_at_flush")
		|| control.get("mode").and_then(serde_json::Value::as_str) != Some("rebuild_after_write")
	{
		return Err("shape gate arms do not report opposite lifecycle modes".to_string());
	}
	let candidate_indexed = required_u64(candidate, "indexed_flush_pages")?;
	let candidate_guard_checks = required_u64(candidate, "indexed_flush_guard_checks")?;
	let candidate_rebuilds = required_u64(candidate, "index_rebuild_calls")?;
	let candidate_reads = required_u64(candidate, "build_physical_read_pages")?;
	let control_indexed = required_u64(control, "indexed_flush_pages")?;
	let control_rebuilds = required_u64(control, "index_rebuild_calls")?;
	let control_reads = required_u64(control, "build_physical_read_pages")?;
	if candidate_indexed != page_count
		|| candidate_guard_checks != candidate_indexed
		|| candidate
			.get("live_guard_checked_after_every_indexed_flush")
			.and_then(serde_json::Value::as_bool)
			!= Some(true)
		|| candidate_rebuilds != 0
		|| candidate_reads != 0
		|| control_indexed != 0
		|| control_rebuilds == 0
		|| control_reads == 0
	{
		return Err("shape gate lifecycle counters are missing, inverted, or same-arm".to_string());
	}
	const Q17_BUILD_ROWS: u64 = 47_989_007;
	for (label, row) in [("candidate", on_row), ("control", off_row)] {
		let exchange = spill_subleaf_rows(row, "write_exchange_repartition")?;
		let active_group = spill_subleaf_rows(row, "write_active_group_partitioning")?;
		let page_key = spill_subleaf_rows(row, "write_page_key_extraction")?;
		if exchange != 4 * Q17_BUILD_ROWS
			|| active_group != 4 * Q17_BUILD_ROWS
			|| page_key != Q17_BUILD_ROWS
		{
			return Err(format!(
				"shape gate {label} write multiplicities are not exactly 4x + 4x + 1x"
			));
		}
	}
	for (label, residency) in [
		("candidate", candidate_residency),
		("control", control_residency),
	] {
		if residency.get("passed").and_then(serde_json::Value::as_bool) != Some(true)
			|| required_u64(residency, "partition_count")? != partition_count
			|| required_u64(residency, "group_count")? != group_count
		{
			return Err(format!(
				"shape gate {label} residency receipt is incomplete or failed"
			));
		}
	}
	Ok(serde_json::json!({
		"suite": "shape_preserving_spill_index_gate",
		"content_identity_gate": content_identity,
		"candidate_mode": candidate["mode"],
		"control_mode": control["mode"],
		"partition_count": partition_count,
		"group_count": group_count,
		"page_count": page_count,
		"spill_bytes": candidate["spill_bytes"],
		"candidate_indexed_flush_pages": candidate_indexed,
		"candidate_indexed_flush_guard_checks": candidate_guard_checks,
		"candidate_rebuild_calls": candidate_rebuilds,
		"candidate_build_physical_read_pages": candidate_reads,
		"control_indexed_flush_pages": control_indexed,
		"control_rebuild_calls": control_rebuilds,
		"control_build_physical_read_pages": control_reads,
		"write_row_multiplicities": {
			"exchange": 4 * Q17_BUILD_ROWS,
			"active_group": 4 * Q17_BUILD_ROWS,
			"page_key": Q17_BUILD_ROWS,
		},
		"passed": true,
	}))
}

fn run_shape_preserving_spill_index_gate(
	query: &str,
	mode: &str,
	sf: f64,
	data_source: &str,
	data_dir: &Path,
	output_dir: &Path,
	switch: &PairedSwitch,
) -> Result<serde_json::Value, String> {
	let gate_dir = output_dir.join("shape_preserving_spill_index_gate");
	std::fs::create_dir_all(&gate_dir)
		.map_err(|error| format!("create shape-preserving gate directory: {error}"))?;
	let run_arm = |arm: &str, value: &str| {
		let spill_dir = PathBuf::from(CACHE_ROOT).join("join_spill");
		if spill_dir.exists() {
			std::fs::remove_dir_all(&spill_dir)
				.map_err(|error| format!("clear shape-gate join spill scratch: {error}"))?;
		}
		let profile_path = gate_dir.join(format!("{arm}_node_attribution.json"));
		let profile = profile_path.display().to_string();
		let row = run_perf_child_with_envs(
			query,
			mode,
			sf,
			0,
			data_source,
			data_dir,
			&[
				(switch.env.as_str(), value),
				("TORON_PARTITIONED_BUILD_SPILL_PROFILE", "1"),
				("TORON_NODE_ATTRIBUTION_PROFILE_PATH", profile.as_str()),
			],
		)?;
		if spill_dir.exists()
			&& std::fs::read_dir(&spill_dir)
				.map_err(|error| format!("inspect shape-gate join spill scratch: {error}"))?
				.next()
				.is_some()
		{
			return Err(format!(
				"shape-gate {arm} child abandoned join spill state in {}",
				spill_dir.display()
			));
		}
		Ok(row)
	};
	let on_row = run_arm("candidate", &switch.on_value)?;
	let off_row = run_arm("control", &switch.off_value)?;
	match shape_preserving_spill_index_gate_record(&on_row, &off_row) {
		Ok(mut record) => {
			record["switch_env"] = serde_json::json!(switch.env);
			record["on_value"] = serde_json::json!(switch.on_value);
			record["off_value"] = serde_json::json!(switch.off_value);
			record["join_spill_scratch_empty"] = serde_json::json!(true);
			Ok(record)
		}
		Err(reason) => Ok(serde_json::json!({
			"suite": "shape_preserving_spill_index_gate",
			"switch_env": switch.env,
			"on_value": switch.on_value,
			"off_value": switch.off_value,
			"reason": reason,
			"passed": false,
		})),
	}
}

/// ADR 514: the plan-identical route ablation gate. Both arms must bind the
/// same plan and produce the same result facts; the route facts must be
/// opposite — the control on the deferred reservoir lifecycle
/// (partition_count >= 2, probe reservoir spill positive) and the candidate on
/// the direct lifecycle (no deferred block, reservoir zero by construction).
/// Missing, stale, or same-arm evidence fails closed.
fn run_route_distinct_gate(
	query: &str,
	mode: &str,
	sf: f64,
	data_source: &str,
	data_dir: &Path,
	output_dir: &Path,
	switch: &PairedSwitch,
) -> Result<serde_json::Value, String> {
	let gate_dir = output_dir.join("route_distinct_gate");
	std::fs::create_dir_all(&gate_dir)
		.map_err(|error| format!("create route-distinct gate directory: {error}"))?;
	let run_arm = |arm: &str, value: &str| {
		let spill_dir = PathBuf::from(CACHE_ROOT).join("join_spill");
		if spill_dir.exists() {
			std::fs::remove_dir_all(&spill_dir)
				.map_err(|error| format!("clear route-gate join spill scratch: {error}"))?;
		}
		let profile_path = gate_dir.join(format!("{arm}_chain_stage.json"));
		let profile = profile_path.display().to_string();
		let row = run_perf_child_with_envs(
			query,
			mode,
			sf,
			0,
			data_source,
			data_dir,
			&[
				(switch.env.as_str(), value),
				("TORON_CHAIN_STAGE_PROFILE_PATH", profile.as_str()),
			],
		)?;
		if spill_dir.exists()
			&& std::fs::read_dir(&spill_dir)
				.map_err(|error| format!("inspect route-gate join spill scratch: {error}"))?
				.next()
				.is_some()
		{
			return Err(format!(
				"route-gate {arm} child abandoned join spill state in {}",
				spill_dir.display()
			));
		}
		let profile_value: serde_json::Value =
			serde_json::from_str(&std::fs::read_to_string(&profile_path).map_err(|error| {
				format!("route-gate {arm} chain stage profile missing: {error}")
			})?)
			.map_err(|error| format!("route-gate {arm} chain stage profile parse: {error}"))?;
		Ok((row, profile_value))
	};
	let (on_row, on_profile) = run_arm("candidate", &switch.on_value)?;
	let (off_row, off_profile) = run_arm("control", &switch.off_value)?;
	match route_distinct_gate_record(&on_row, &off_row, &on_profile, &off_profile) {
		Ok(mut record) => {
			record["switch_env"] = serde_json::json!(switch.env);
			record["on_value"] = serde_json::json!(switch.on_value);
			record["off_value"] = serde_json::json!(switch.off_value);
			record["join_spill_scratch_empty"] = serde_json::json!(true);
			Ok(record)
		}
		Err(reason) => Ok(serde_json::json!({
			"suite": "route_distinct_gate",
			"switch_env": switch.env,
			"on_value": switch.on_value,
			"off_value": switch.off_value,
			"reason": reason,
			"passed": false,
		})),
	}
}

fn route_distinct_gate_record(
	on_row: &serde_json::Value,
	off_row: &serde_json::Value,
	on_profile: &serde_json::Value,
	off_profile: &serde_json::Value,
) -> Result<serde_json::Value, String> {
	let row_str = |row: &serde_json::Value, key: &str| -> Result<String, String> {
		row.get(key)
			.and_then(serde_json::Value::as_str)
			.map(ToOwned::to_owned)
			.ok_or_else(|| format!("route gate row lacks {key}"))
	};
	let row_u64 = |row: &serde_json::Value, key: &str| -> Result<u64, String> {
		row.get(key)
			.and_then(serde_json::Value::as_u64)
			.ok_or_else(|| format!("route gate row lacks numeric {key}"))
	};
	for (arm, row) in [("candidate", on_row), ("control", off_row)] {
		if row_str(row, "status")? != "pass" {
			return Err(format!("route gate {arm} child did not pass"));
		}
		if row_str(row, "dataset_verification")? != "full_hash" {
			return Err(format!("route gate {arm} child is not full-hash verified"));
		}
		if row.get("debug_build").and_then(serde_json::Value::as_bool) != Some(false) {
			return Err(format!("route gate {arm} child is not a release build"));
		}
	}
	for key in ["plan_fingerprint", "digest", "output_schema_fingerprint"] {
		if row_str(on_row, key)? != row_str(off_row, key)? {
			return Err(format!("route gate arms disagree on {key}"));
		}
	}
	let content_identity = toron_tpch::evidence::compare_row_identities(on_row, off_row);
	if !content_identity.would_admit {
		return Err(format!(
			"route gate content identity refused: {:?}",
			content_identity.reasons
		));
	}
	if row_u64(on_row, "output_row_count")? != row_u64(off_row, "output_row_count")? {
		return Err("route gate arms disagree on output_row_count".to_string());
	}
	let off_deferred = off_profile
		.get("deferred_replay_1")
		.ok_or("route gate control lacks the deferred reservoir block")?;
	let off_partitions = off_deferred
		.get("partition_count")
		.and_then(serde_json::Value::as_u64)
		.ok_or("route gate control lacks a numeric partition_count")?;
	let off_probe_spill = off_deferred
		.get("probe_spill_bytes")
		.and_then(serde_json::Value::as_u64)
		.ok_or("route gate control lacks numeric probe_spill_bytes")?;
	if off_partitions < 2 {
		return Err(format!(
			"route gate control must partition (partition_count {off_partitions})"
		));
	}
	if off_probe_spill == 0 {
		return Err("route gate control must spill the probe reservoir".to_string());
	}
	// The chain-stage profiler emits the deferred block unconditionally; the
	// candidate's must be inert — zero partitions completed, zero rows
	// buffered, zero reservoir spill. Any live fact means the deferred
	// lifecycle ran and the gate fails closed.
	if let Some(on_deferred) = on_profile.get("deferred_replay_1") {
		for key in [
			"completed_partitions",
			"probe_rows_buffered",
			"probe_spill_bytes",
			"build_spill_bytes",
			"index_builds",
		] {
			let value = on_deferred
				.get(key)
				.and_then(serde_json::Value::as_u64)
				.ok_or_else(|| {
					format!("route gate candidate deferred block lacks numeric {key}")
				})?;
			if value != 0 {
				return Err(format!(
					"route gate candidate still ran the deferred reservoir lifecycle ({key} = {value})"
				));
			}
		}
	}
	Ok(serde_json::json!({
		"suite": "route_distinct_gate",
		"content_identity_gate": content_identity,
		"plan_fingerprint": row_str(on_row, "plan_fingerprint")?,
		"digest_equal": true,
		"control_partition_count": off_partitions,
		"control_probe_spill_bytes": off_probe_spill,
		"candidate_deferred_block": false,
		"control_spill_bytes": off_row.get("spill_bytes").cloned().unwrap_or(serde_json::json!(null)),
		"candidate_spill_bytes": on_row.get("spill_bytes").cloned().unwrap_or(serde_json::json!(null)),
		"dataset_fingerprint": on_row.get("dataset_verification").cloned().unwrap_or(serde_json::json!(null)),
		"passed": true,
	}))
}

fn paired_ablation_arm(run_index: u32) -> bool {
	run_index % 2 == 1
}

fn parse_usize_flag(args: &[String], flag: &str, default: usize) -> Result<usize, String> {
	match flag_value(args, flag) {
		None => Ok(default),
		Some(raw) => raw
			.parse::<usize>()
			.map_err(|_| format!("{flag} must be a non-negative integer (got {raw:?})")),
	}
}

fn parse_u32_flag(args: &[String], flag: &str, default: u32) -> Result<u32, String> {
	match flag_value(args, flag) {
		None => Ok(default),
		Some(raw) => raw
			.parse::<u32>()
			.map_err(|_| format!("{flag} must be a non-negative integer (got {raw:?})")),
	}
}

fn optional_positive_f64_flag(args: &[String], flag: &str) -> Result<Option<f64>, String> {
	let Some(raw) = flag_value(args, flag) else {
		return Ok(None);
	};
	let value = raw
		.parse::<f64>()
		.map_err(|_| format!("{flag} must be a positive finite number (got {raw:?})"))?;
	if !value.is_finite() || value <= 0.0 {
		return Err(format!(
			"{flag} must be a positive finite number (got {raw:?})"
		));
	}
	Ok(Some(value))
}

/// The explicitly-passed, validated `--sf`. `Ok(None)` means the flag was absent (use the default);
/// `Ok(Some(sf))` is a finite, positive scale; `Err` is a malformed / zero / negative / NaN / infinite
/// value — a usage error the caller turns into [`harness_error`]. See [`parse_scale_factor`].
fn explicit_sf(args: &[String]) -> Result<Option<f64>, String> {
	match flag_value(args, "--sf") {
		None => Ok(None),
		Some(raw) => parse_scale_factor(raw).map(Some),
	}
}

/// The resolved scale: a validated `--sf` if present, else [`DEFAULT_SF`].
fn resolved_sf(args: &[String]) -> Result<f64, String> {
	explicit_sf(args).map(|explicit| explicit.unwrap_or(DEFAULT_SF))
}

/// Print a usage error and exit 2 — a *harness* error (bad arg), distinct from a query `blocked`/`fail`
/// (ADR 261). No telemetry row is emitted.
fn harness_error(message: &str) -> ExitCode {
	eprintln!("{message}");
	ExitCode::from(2)
}

fn usage() -> ExitCode {
	eprintln!(
		"usage: toron-tpch <status|prepare|run|matrix|correctness|correctness-matrix|perf|perf-board-replay|adr592-revalidate|adr593-journal-recheck|adr584-q21-attribution|adr585-q21-decomposition|adr594-q21-inner-1|adr595-q21-inner-1|adr596-q21-build-write|adr596-reevaluate|adr597-q21-exchange-skip|adr597-reevaluate|adr598-gate-probe|adr598-disposition|adr595-reevaluate|adr594-reevaluate|adr586-q21-semi-write|adr587-q21-insert|adr588-q21-second-walk|adr589-q21-order-aware|adr449-gate-zero|q03-plan-proof|q17-census|fragment-census|execution-census|timing-atlas|placement-ab> [--query q06|all] [--mode dev|strict] [--sf 0.01] [--allow-blocked] [--runs 7] [--warmup 1] [--paired-ablation] [--engines toron|rune|duckdb|polars_sql|toron,duckdb] [--data-source memory|parquet] [--data-dir target/tpch/data] [--output-dir target/tpch/perf]"
	);
	ExitCode::from(2)
}

#[cfg(test)]
mod tests {
	#[test]
	fn adr448_driver_alternates_arms_per_child() {
		assert_eq!(
			(0..6).map(paired_ablation_arm).collect::<Vec<_>>(),
			vec![false, true, false, true, false, true]
		);
	}

	#[test]
	fn adr449_pairing_is_by_run_index_and_uses_off_minus_on() {
		let samples = vec![
			serde_json::json!({"run_index": 1, "ablation_arm": "off", "execute_ms": 14.0}),
			serde_json::json!({"run_index": 0, "ablation_arm": "on", "execute_ms": 10.0}),
			serde_json::json!({"run_index": 3, "ablation_arm": "off", "execute_ms": 24.0}),
			serde_json::json!({"run_index": 2, "ablation_arm": "on", "execute_ms": 20.0}),
		];
		assert_eq!(
			paired_ablation_differences(&samples).unwrap(),
			vec![4.0, 4.0]
		);
	}

	#[test]
	fn adr449_pairing_rejects_missing_or_misaligned_samples() {
		let dropped = vec![
			serde_json::json!({"run_index": 0, "ablation_arm": "on", "execute_ms": 10.0}),
			serde_json::json!({"run_index": 1, "ablation_arm": "off", "execute_ms": 14.0}),
			serde_json::json!({"run_index": 2, "ablation_arm": "on", "execute_ms": 20.0}),
		];
		assert!(paired_ablation_differences(&dropped).is_err());
		let wrong_arm = vec![
			serde_json::json!({"run_index": 0, "ablation_arm": "off", "execute_ms": 10.0}),
			serde_json::json!({"run_index": 1, "ablation_arm": "off", "execute_ms": 14.0}),
		];
		assert!(paired_ablation_differences(&wrong_arm).is_err());
	}

	fn test_paired_switch() -> PairedSwitch {
		PairedSwitch {
			env: PAIRED_DEFAULT_ENV.to_string(),
			on_value: "0".to_string(),
			off_value: "1".to_string(),
		}
	}

	#[test]
	fn adr469_plan_gate_requires_distinct_plans_and_equal_digests() {
		let row = |plan: &str, digest: &str, status: &str| serde_json::json!({"plan_fingerprint": plan, "digest": digest, "status": status});
		// The gate's whole point: arms must differ in plan and agree in answer.
		assert!(adr469_plan_gate_passes(
			&row("aaaa", "d1", "pass"),
			&row("bbbb", "d1", "pass")
		));
		// Same plan = the switch did nothing at this cell: refuse.
		assert!(!adr469_plan_gate_passes(
			&row("aaaa", "d1", "pass"),
			&row("aaaa", "d1", "pass")
		));
		// Digest mismatch = a correctness defect, never a cohort: refuse.
		assert!(!adr469_plan_gate_passes(
			&row("aaaa", "d1", "pass"),
			&row("bbbb", "d2", "pass")
		));
		assert!(!adr469_plan_gate_passes(
			&row("aaaa", "d1", "fail"),
			&row("bbbb", "d1", "pass")
		));
	}

	fn shape_gate_row(candidate: bool) -> serde_json::Value {
		let page_count = 6_059u64;
		let mode = if candidate {
			"index_at_flush"
		} else {
			"rebuild_after_write"
		};
		let lifecycle = serde_json::json!({
			"mode": mode,
			"partition_count": 16,
			"group_count": 4,
			"indexed_flush_pages": if candidate { page_count } else { 0 },
			"indexed_flush_guard_checks": if candidate { page_count } else { 0 },
			"live_guard_checked_after_every_indexed_flush": candidate,
			"index_rebuild_calls": if candidate { 0 } else { 16 },
			"build_physical_read_pages": if candidate { 0 } else { page_count },
			"page_count": page_count,
			"page_rows": [8192, 7],
			"page_ciphertext_bytes": [65536, 128],
			"spill_bytes": 622_544_968u64,
		});
		let residency = serde_json::json!({
			"partition_count": 16,
			"group_count": 4,
			"passed": true,
		});
		serde_json::json!({
			"status": "pass",
			"dataset_verification": "full_hash",
			"build_commit": "seal",
			"evidence_identity": {
				"executable_sha256": "11".repeat(32),
				"harness_sha256": "11".repeat(32),
				"toron_sha256": "11".repeat(32),
				"recipe_sha256": "22".repeat(32),
				"source_tree_id": "3".repeat(40),
				"authority_issues": [],
			},
			"plan_fingerprint": "plan",
			"digest": "digest",
			"output_schema_fingerprint": "schema",
			"output_row_count": 1,
			"join_spill_observations": [{"path": "partitioned_streaming_scan_build"}],
			"spill_bytes": 622_544_968u64,
			"memory_per_task": 2_147_483_648u64,
			"memory_per_task_provenance": "explicit",
			"spill_bytes_reserved": 2_147_483_648u64,
			"spill_bytes_reserved_provenance": "explicit",
			"node_attribution": {
				"aggregate_invocations": [{
					"diagnostics": [
						{"name": "partitioned_scan_build_index_lifecycle", "value": lifecycle},
						{"name": "partitioned_build_residency", "value": residency},
					],
					"spill_io": [{
						"subleaves": [
							{"stage": "write_exchange_repartition", "rows": 191_956_028u64},
							{"stage": "write_active_group_partitioning", "rows": 191_956_028u64},
							{"stage": "write_page_key_extraction", "rows": 47_989_007u64},
						],
					}],
				}],
			},
		})
	}

	#[test]
	fn shape_preserving_gate_requires_equal_shape_and_opposite_lifecycle() {
		let candidate = shape_gate_row(true);
		let control = shape_gate_row(false);
		let gate = shape_preserving_spill_index_gate_record(&candidate, &control).unwrap();
		assert_eq!(gate["passed"], true);
		assert_eq!(gate["content_identity_gate"]["would_admit"], true);
		let mut dirty_navigation_only = candidate.clone();
		dirty_navigation_only["build_commit"] = serde_json::json!("different-dirty");
		assert!(
			shape_preserving_spill_index_gate_record(&dirty_navigation_only, &control).is_ok(),
			"the commit hint and dirty spelling are not authority inputs"
		);
		let mut different_recipe = candidate.clone();
		different_recipe["evidence_identity"]["recipe_sha256"] = serde_json::json!("44".repeat(32));
		assert!(
			shape_preserving_spill_index_gate_record(&different_recipe, &control).is_err(),
			"one commit built with two recipes is not one comparable cohort"
		);
		assert!(
			shape_preserving_spill_index_gate_record(&candidate, &candidate).is_err(),
			"two candidate arms must not pass"
		);
		let mut wrong_geometry = control.clone();
		wrong_geometry
			.pointer_mut("/node_attribution/aggregate_invocations/0/diagnostics/0/value/page_rows")
			.expect("page rows")
			.clone_from(&serde_json::json!([8192, 8]));
		assert!(shape_preserving_spill_index_gate_record(&candidate, &wrong_geometry).is_err());
		let mut missing_counter = candidate.clone();
		missing_counter
			.pointer_mut(
				"/node_attribution/aggregate_invocations/0/diagnostics/0/value/indexed_flush_pages",
			)
			.expect("counter")
			.clone_from(&serde_json::Value::Null);
		assert!(shape_preserving_spill_index_gate_record(&missing_counter, &control).is_err());
		let mut skipped_guard = candidate.clone();
		skipped_guard
			.pointer_mut(
				"/node_attribution/aggregate_invocations/0/diagnostics/0/value/indexed_flush_guard_checks",
			)
			.expect("guard count")
			.clone_from(&serde_json::json!(6_058));
		assert!(shape_preserving_spill_index_gate_record(&skipped_guard, &control).is_err());
	}

	#[test]
	fn adr449_sample_journal_is_durable_and_resume_config_locked() {
		let directory =
			std::env::temp_dir().join(format!("toron-adr449-journal-{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&directory);
		let data_dir = Path::new("/tmp/adr449-data");
		let budget = GaugeBudget::defaults();
		let journal = prepare_paired_ablation_journal(
			&directory,
			"q05",
			"strict",
			4.0,
			"parquet",
			data_dir,
			20,
			1,
			"fnv1a64:cohort",
			&test_paired_switch(),
			PairedGate::PlanDistinct,
			None,
			budget,
		)
		.unwrap();
		journal
			.append(&serde_json::json!({
				"engine": "toron",
				"query": "q05",
				"run_index": 0,
				"warmup": false,
				"ablation_arm": "on",
				"execute_ms": 100.0,
			}))
			.unwrap();
		let resumed = prepare_paired_ablation_journal(
			&directory,
			"q05",
			"strict",
			4.0,
			"parquet",
			data_dir,
			20,
			1,
			"fnv1a64:cohort",
			&test_paired_switch(),
			PairedGate::PlanDistinct,
			None,
			budget,
		)
		.unwrap();
		assert_eq!(resumed.rows.len(), 1);
		assert!(resumed.contains("q05", "toron", 0, false));
		assert!(
			prepare_paired_ablation_journal(
				&directory,
				"q05",
				"strict",
				2.0,
				"parquet",
				data_dir,
				20,
				1,
				"fnv1a64:cohort",
				&test_paired_switch(),
				PairedGate::PlanDistinct,
				None,
				budget,
			)
			.is_err()
		);
		// ADR 456: a resume across different budget coordinates is a regime mix
		// and must refuse exactly like a scale or dataset mismatch.
		let different_budget = GaugeBudget {
			memory_per_task: 128 * 1024 * 1024,
			memory_per_task_provenance: "explicit",
			..budget
		};
		assert!(
			prepare_paired_ablation_journal(
				&directory,
				"q05",
				"strict",
				4.0,
				"parquet",
				data_dir,
				20,
				1,
				"fnv1a64:cohort",
				&test_paired_switch(),
				PairedGate::PlanDistinct,
				None,
				different_budget,
			)
			.is_err()
		);
		// ADR 452: a regenerated dataset at the same path must refuse to resume,
		// and an empty fingerprint (no verified dataset) must never launch.
		assert!(
			prepare_paired_ablation_journal(
				&directory,
				"q05",
				"strict",
				4.0,
				"parquet",
				data_dir,
				20,
				1,
				"fnv1a64:regenerated",
				&test_paired_switch(),
				PairedGate::PlanDistinct,
				None,
				budget,
			)
			.is_err()
		);
		assert!(
			prepare_paired_ablation_journal(
				&directory,
				"q05",
				"strict",
				4.0,
				"parquet",
				data_dir,
				20,
				1,
				"",
				&test_paired_switch(),
				PairedGate::PlanDistinct,
				None,
				budget,
			)
			.is_err()
		);
		std::fs::remove_dir_all(directory).unwrap();

		let shape_directory =
			std::env::temp_dir().join(format!("toron-adr510-journal-{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&shape_directory);
		let shape_switch = PairedSwitch {
			env: PARTITIONED_SCAN_BUILD_INDEX_AT_FLUSH_ENV.to_string(),
			on_value: "1".to_string(),
			off_value: "0".to_string(),
		};
		prepare_paired_ablation_journal(
			&shape_directory,
			"q17",
			"strict",
			8.0,
			"parquet",
			data_dir,
			20,
			1,
			"fnv1a64:shape",
			&shape_switch,
			PairedGate::ShapePreservingSpillIndex,
			Some(3_554.0),
			budget,
		)
		.unwrap();
		assert!(
			prepare_paired_ablation_journal(
				&shape_directory,
				"q17",
				"strict",
				8.0,
				"parquet",
				data_dir,
				20,
				1,
				"fnv1a64:shape",
				&shape_switch,
				PairedGate::PlanDistinct,
				None,
				budget,
			)
			.is_err(),
			"resume must reject a paired-gate mismatch"
		);
		assert!(
			prepare_paired_ablation_journal(
				&shape_directory,
				"q17",
				"strict",
				8.0,
				"parquet",
				data_dir,
				20,
				1,
				"fnv1a64:shape",
				&shape_switch,
				PairedGate::ShapePreservingSpillIndex,
				Some(3_555.0),
				budget,
			)
			.is_err(),
			"resume must reject a fixed-floor mismatch"
		);
		std::fs::remove_dir_all(shape_directory).unwrap();
	}

	#[test]
	fn adr451_materiality_baseline_is_off_arm_median_not_pooled() {
		// HL shift 108 sits between 10% of the pooled median (105.4) and 10% of
		// the OFF median (110.8): material under a pooled baseline, immaterial
		// under the OFF/control baseline. Fails if the pooled rule returns.
		let samples = (0..12u64)
			.map(|index| {
				let on = index % 2 == 0;
				serde_json::json!({
					"run_index": index,
					"ablation_arm": if on { "on" } else { "off" },
					"execute_ms": if on { 1000.0 } else { 1108.0 },
				})
			})
			.collect::<Vec<_>>();
		let summary = serde_json::json!({
			"execute_ms_samples": samples,
			"execute_ms_median": 1054.0,
		});
		let receipts = attach_paired_ablation_receipt(
			vec![summary],
			12,
			serde_json::json!({"passed": true}),
			None,
		)
		.unwrap();
		let paired = &receipts[0]["paired_ablation"];
		assert_eq!(paired["point_ms"].as_f64().unwrap(), 108.0);
		assert_eq!(paired["baseline_off_median_ms"].as_f64().unwrap(), 1108.0);
		assert!((paired["materiality_floor_ms"].as_f64().unwrap() - 110.8).abs() < 1e-9);
		assert_eq!(paired["material"].as_bool().unwrap(), false);
		assert!(
			material_effect(108.0, 1054.0),
			"fixture must discriminate: the pooled rule would call this material"
		);
	}

	#[test]
	fn adr510_fixed_materiality_floor_is_not_recomputed_from_the_cohort() {
		let samples = (0..20u64)
			.map(|index| {
				let on = index % 2 == 0;
				serde_json::json!({
					"run_index": index,
					"ablation_arm": if on { "on" } else { "off" },
					"execute_ms": if on { 31_000.0 } else { 34_500.0 },
				})
			})
			.collect::<Vec<_>>();
		let summary = serde_json::json!({"execute_ms_samples": samples});
		let receipt = attach_paired_ablation_receipt(
			vec![summary],
			20,
			serde_json::json!({"passed": true}),
			Some(3_554.0),
		)
		.expect("fixed-floor receipt");
		let paired = &receipt[0]["paired_ablation"];
		assert_eq!(paired["materiality_floor_ms"], 3_554.0);
		assert_eq!(paired["materiality_policy"], "fixed_predeclared");
		assert_eq!(paired["material"], false);
	}

	#[test]
	fn adr449_gate_zero_expectation_is_oracle_derived() {
		let small = q05_reservoir_full_input_cardinality(0.01);
		let sf1 = q05_reservoir_full_input_cardinality(1.0);
		let sf10 = q05_reservoir_full_input_cardinality(10.0);
		assert_eq!(
			small,
			toron_tpch::generate_strict_lineitem(0.01).num_rows() as u64,
			"the streaming cardinality oracle matches the materialized strict batch"
		);
		assert!(small < sf1 && sf1 < sf10);
		assert_ne!(small, 59_986_052);
		assert_ne!(sf1, 59_986_052);
		assert_eq!(sf10, 59_986_052);
		assert!(adr449_gate_zero_passes(sf1 - 1, sf1, sf1));
		assert!(!adr449_gate_zero_passes(sf1, sf1, sf1));
	}

	#[test]
	fn adr449_gate_zero_rejects_multiple_profiles() {
		let directory =
			std::env::temp_dir().join(format!("toron-adr449-profile-test-{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&directory);
		std::fs::create_dir_all(&directory).unwrap();
		std::fs::write(
			directory.join("grouped_chain_0000.json"),
			r#"{"reservoir_build_rows": 1}"#,
		)
		.unwrap();
		std::fs::write(
			directory.join("grouped_chain_0001.json"),
			r#"{"reservoir_build_rows": 2}"#,
		)
		.unwrap();
		assert!(profiled_reservoir_build_rows(&directory).is_err());
		std::fs::remove_dir_all(directory).unwrap();
	}

	#[test]
	fn adr450_classifier_guard_b_is_non_committing_not_hard_error() {
		let row = serde_json::json!({
			"status": "fail",
			"diagnostic_message": "Kernel { message: \"generic grouped path did not commit the partitioned-scan reservoir\" }",
			"execute_ms": 12.0,
		});
		assert!(matches!(
			adr450_classify_rung(&row, Path::new("/nonexistent")),
			Ok(Adr450RungOutcome::NonCommitting {
				diagnostic: ADR450_GUARD_B,
				..
			})
		));
	}

	#[test]
	fn adr455_ladder_records_carry_budget_provenance() {
		let directory =
			std::env::temp_dir().join(format!("toron-adr455-ladder-{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&directory);
		let record = run_adr450_scale_ladder(
			"q05",
			&[],
			"strict",
			"parquet",
			Path::new("/tmp/adr455-data"),
			&directory,
			GaugeBudget::defaults(),
		)
		.unwrap();
		assert_eq!(record["outcome"], "no_commit");
		assert_eq!(
			record["memory_per_task"].as_u64(),
			Some(64 * 1024 * 1024),
			"ladder record must carry the budget it ran under"
		);
		assert_eq!(record["memory_per_task_provenance"], "default");
		std::fs::remove_dir_all(directory).unwrap();
	}

	#[test]
	fn adr454_gauge_budget_defaults_are_explicit_inputs_to_consumers() {
		let budget = GaugeBudget::defaults();
		assert_eq!(budget.memory_per_task, 64 * 1024 * 1024);
		assert_eq!(budget.memory_per_task_provenance, "default");
		assert_eq!(budget.spill_bytes_reserved, 512 * 1024 * 1024);
		assert_eq!(budget.spill_bytes_reserved_provenance, "default");
	}

	#[test]
	fn adr527_rows_name_requested_partition_work_concurrency() {
		let mut toron_row = serde_json::json!({});
		add_partition_work_concurrency_fields(&mut toron_row, Some((8, "explicit")));
		assert_eq!(toron_row["partition_work_concurrency_requested"], 8);
		assert_eq!(
			toron_row["partition_work_concurrency_requested_provenance"],
			"explicit"
		);

		let mut peer_row = serde_json::json!({});
		add_partition_work_concurrency_fields(&mut peer_row, None);
		assert!(peer_row["partition_work_concurrency_requested"].is_null());
		assert!(peer_row["partition_work_concurrency_requested_provenance"].is_null());
	}

	#[test]
	fn adr452_classifier_pass_with_zero_profiles_is_non_committing() {
		// A passing child that never entered the grouped-chain surface emits no
		// profile; that is a climb signal, not a hard error. Fails under the
		// pre-452 classifier, which demanded exactly one profile on pass.
		let directory =
			std::env::temp_dir().join(format!("toron-adr452-empty-profile-{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&directory);
		std::fs::create_dir_all(&directory).unwrap();
		let row = serde_json::json!({"status": "pass", "execute_ms": 12.0});
		assert!(matches!(
			adr450_classify_rung(&row, &directory),
			Ok(Adr450RungOutcome::NonCommitting {
				diagnostic: ADR450_NO_CHAIN_PROFILE,
				..
			})
		));
		std::fs::remove_dir_all(directory).unwrap();
	}

	#[test]
	fn adr450_classifier_unknown_failure_is_hard_error() {
		let row = serde_json::json!({
			"status": "fail",
			"diagnostic_message": "Kernel { message: \"unexpected failure\" }",
			"execute_ms": 12.0,
		});
		assert!(adr450_classify_rung(&row, Path::new("/nonexistent")).is_err());
	}
	use super::*;
	use toron_tpch::Q17_CORRECTNESS_SCALE;

	#[test]
	fn q17_correctness_scale_policy_rejects_masked_empty_scale() {
		assert_eq!(q17_correctness_sf(None).unwrap(), Q17_CORRECTNESS_SCALE);
		assert_eq!(
			q17_correctness_sf(Some(Q17_CORRECTNESS_SCALE)).unwrap(),
			Q17_CORRECTNESS_SCALE
		);
		assert_eq!(q17_correctness_sf(Some(10.0)).unwrap(), 10.0);
		assert!(q17_correctness_sf(Some(0.01)).is_err());
		assert!(q17_correctness_sf(Some(1.0)).is_err());
	}

	#[test]
	fn perf_query_list_normalizes_all_and_short_aliases() {
		assert_eq!(perf_query_list("all").unwrap(), ALL_QUERY_IDS.to_vec());
		assert_eq!(perf_query_list("q6").unwrap(), vec!["q06"]);
		assert_eq!(perf_query_list("q21").unwrap(), vec!["q21"]);
		assert!(perf_query_list("q23").is_err());
	}

	#[test]
	fn adr584_profile_receipt_path_selects_the_final_completed_snapshot() {
		let directory =
			std::env::temp_dir().join(format!("toron-adr584-chain-profile-{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&directory);
		std::fs::create_dir_all(&directory).unwrap();
		let path = directory.join("pair-03-on.jsonl");
		std::fs::write(
			&path,
			concat!(
				r#"{"query_id":"q","sequence":1,"drive_completed":false}"#,
				"\n",
				r#"{"query_id":"q","sequence":2,"drive_completed":true,"total_drive_micros":10}"#,
				"\n",
				r#"{"query_id":"q","sequence":3,"drive_completed":true,"total_drive_micros":20}"#,
				"\n"
			),
		)
		.unwrap();
		let mut row = serde_json::json!({});
		attach_chain_stage_profile(&mut row, &path).expect("attach final receipt");
		assert_eq!(
			row["chain_stage_profile_receipt_path"],
			path.display().to_string()
		);
		assert_eq!(row["chain_stage_profile"]["sequence"], 3);
		assert_eq!(row["chain_stage_profile"]["total_drive_micros"], 20);
		std::fs::remove_dir_all(directory).unwrap();
	}

	#[test]
	fn adr584_join_diagnostic_refuses_receipts_with_the_target_stripped() {
		let stripped = serde_json::json!({
			"local_join_admissions": ["declined(no_build_row_bound)"],
			"join_input_replay_ledger": [],
			"join_input_total_concats": 0,
		});
		assert!(
			validate_targeted_local_join_diagnostic(&stripped)
				.expect_err("a stripped receipt is not attribution evidence")
				.contains("refuses stripped")
		);

		let targeted = serde_json::json!({
			"local_join_admissions": ["declined(no_build_row_bound)"],
			"targeted_local_join_admissions": [{
				"replay_id": 2,
				"verdict": "declined(no_build_row_bound)",
			}],
			"join_input_replay_ledger": [],
			"join_input_total_concats": 0,
		});
		validate_targeted_local_join_diagnostic(&targeted)
			.expect("replay identity and verdict are bound in one receipt");

		let incomplete = serde_json::json!({
			"local_join_admissions": ["declined(no_build_row_bound)"],
			"targeted_local_join_admissions": [{
				"replay_id": 2,
				"verdict": "declined(no_build_row_bound)",
			}],
			"join_input_replay_ledger": [],
			"join_input_total_concats": 1,
		});
		assert!(
			validate_targeted_local_join_diagnostic(&incomplete)
				.expect_err("an unowned concat must invalidate the ledger")
				.contains("incomplete")
		);
	}

	fn adr584_fixture_row(pair: usize, on: bool) -> serde_json::Value {
		let off_ms = 2_000.0 + pair as f64 * 10.0;
		let execute_ms = if on { off_ms + 100.0 } else { off_ms };
		let identity = serde_json::json!({
			"executable_sha256": "1".repeat(64),
			"recipe_sha256": "2".repeat(64),
			"source_tree_id": "3".repeat(40),
			"harness_sha256": "4".repeat(64),
			"toron_sha256": "5".repeat(64),
			"authority_issues": [],
		});
		let mut row = serde_json::json!({
			"query": "q21",
			"status": "pass",
			"execute_ms": execute_ms,
			"adr584_pair": pair,
			"adr584_arm": if on { "on" } else { "off" },
			"adr584_pair_position": if pair % 2 == 0 { if on { 1 } else { 0 } } else if on { 0 } else { 1 },
			"diagnostics_disabled": !on,
			"evidence_identity": identity,
		});
		if on {
			row["chain_stage_profile_receipt_path"] =
				serde_json::json!(format!("pair-{pair:02}-on.jsonl"));
			row["chain_stage_profile"] = serde_json::json!({
				"total_drive_micros": 1_000_000,
				"unattributed_residual_micros": 10_000,
				"stages": [
					{"stage": "base_scan", "micros": 90_000},
					{"stage": "inner_replay_0", "micros": 150_000},
					{"stage": "inner_replay_1", "micros": 250_000},
					{"stage": "inner_replay_2", "micros": 200_000},
					{"stage": "semi_replay_3", "micros": 100_000},
					{"stage": "anti_replay_4", "micros": 100_000},
					{"stage": "grouped_fold", "micros": 100_000}
				]
			});
			row["targeted_local_join_admissions"] = serde_json::json!([
				{"replay_id": 0, "verdict": "admitted(estimate=1,budget=2)"},
				{"replay_id": 1, "verdict": "declined(no_build_row_bound)"},
				{"replay_id": 2, "verdict": "declined(no_build_row_bound)"}
			]);
			row["join_input_replay_ledger"] = serde_json::json!([]);
			row["join_input_total_concats"] = serde_json::json!(0);
		}
		row
	}

	#[test]
	fn adr584_owner_rule_includes_outside_drive_and_uses_the_off_range() {
		let mut rows = (0..ADR584_PAIRS)
			.flat_map(|pair| {
				[
					adr584_fixture_row(pair, false),
					adr584_fixture_row(pair, true),
				]
			})
			.collect::<Vec<_>>();
		for row in &mut rows {
			let pair = row["adr584_pair"].as_f64().unwrap();
			let off_ms = 8_000.0 + pair * 1_000.0;
			row["execute_ms"] = serde_json::json!(if row["adr584_arm"] == "on" {
				off_ms + 100.0
			} else {
				off_ms
			});
		}
		let summary = evaluate_adr584_pairs(&rows).expect("complete five-pair fixture");
		assert_eq!(summary["perturbation_gate"], "accepted");
		assert_eq!(summary["all_profiles_complete"], true);
		assert_eq!(summary["off_execute_range_ms"], 4_000.0);
		assert_eq!(summary["owner_floor_ms"], 8_000.0);
		assert_eq!(summary["owner"]["status"], "resolved");
		assert_eq!(summary["owner"]["term"], "outside_drive");
	}

	#[test]
	fn adr584_owner_rule_reports_a_tie_within_the_off_range() {
		let mut rows = (0..ADR584_PAIRS)
			.flat_map(|pair| {
				[
					adr584_fixture_row(pair, false),
					adr584_fixture_row(pair, true),
				]
			})
			.collect::<Vec<_>>();
		for row in &mut rows {
			if row["adr584_arm"] == "on" {
				row["chain_stage_profile"] = serde_json::json!({
					"total_drive_micros": 1_400_000,
					"unattributed_residual_micros": 10_000,
					"stages": [
						{"stage": "base_scan", "micros": 100_000},
						{"stage": "inner_replay_0", "micros": 200_000},
						{"stage": "inner_replay_1", "micros": 690_000},
						{"stage": "inner_replay_2", "micros": 100_000},
						{"stage": "semi_replay_3", "micros": 100_000},
						{"stage": "anti_replay_4", "micros": 100_000},
						{"stage": "grouped_fold", "micros": 100_000}
					]
				});
			}
		}
		let summary = evaluate_adr584_pairs(&rows).expect("complete tied fixture");
		assert_eq!(summary["owner_floor_ms"], 500.0);
		assert_eq!(
			summary["owner"]["status"],
			"unresolved_tie_within_off_range"
		);
	}

	#[test]
	fn adr584_negative_outside_drive_is_reported_as_an_instrument_defect() {
		let mut rows = (0..ADR584_PAIRS)
			.flat_map(|pair| {
				[
					adr584_fixture_row(pair, false),
					adr584_fixture_row(pair, true),
				]
			})
			.collect::<Vec<_>>();
		rows[1]["execute_ms"] = serde_json::json!(900.0);
		let summary = evaluate_adr584_pairs(&rows).expect("instrument defects remain reportable");
		assert_eq!(summary["instrument_valid"], false);
		assert_eq!(summary["pairs"][0]["outside_drive_non_negative"], false);
		assert_eq!(summary["owner"]["status"], "withheld_instrument_defect");
	}

	#[test]
	fn adr584_perturbation_gate_is_the_median_absolute_pair_movement() {
		let mut rows = (0..ADR584_PAIRS)
			.flat_map(|pair| {
				[
					adr584_fixture_row(pair, false),
					adr584_fixture_row(pair, true),
				]
			})
			.collect::<Vec<_>>();
		for row in &mut rows {
			if row["adr584_arm"] == "on" {
				let off_ms = 2_000.0 + row["adr584_pair"].as_f64().unwrap() * 10.0;
				row["execute_ms"] = serde_json::json!(off_ms * 1.30);
			}
		}
		let summary = evaluate_adr584_pairs(&rows).expect("rejected profiles are reported");
		assert_eq!(summary["perturbation_gate"], "rejected");
		assert!(summary["median_absolute_movement"].as_f64().unwrap() > 0.25);
		assert_eq!(
			summary["owner"]["status"], "withheld_perturbation_rejected",
			"a rejected instrument must not yield an attribution owner"
		);
	}

	#[test]
	fn adr584_profile_receipt_paths_are_unique_per_pair() {
		let mut rows = (0..ADR584_PAIRS)
			.flat_map(|pair| {
				[
					adr584_fixture_row(pair, false),
					adr584_fixture_row(pair, true),
				]
			})
			.collect::<Vec<_>>();
		rows[3]["chain_stage_profile_receipt_path"] =
			rows[1]["chain_stage_profile_receipt_path"].clone();
		assert!(
			evaluate_adr584_pairs(&rows)
				.expect_err("two profiled children cannot share one receipt")
				.contains("reuses profile receipt path")
		);
	}

	#[test]
	fn adr584_requires_all_three_target_bearing_admissions() {
		let mut rows = (0..ADR584_PAIRS)
			.flat_map(|pair| {
				[
					adr584_fixture_row(pair, false),
					adr584_fixture_row(pair, true),
				]
			})
			.collect::<Vec<_>>();
		rows[1]["targeted_local_join_admissions"] = serde_json::json!([]);
		assert!(
			evaluate_adr584_pairs(&rows)
				.expect_err("a stripped or partial admission set is not evidence")
				.contains("expected three")
		);
	}

	fn adr585_fixture_row(pair: usize, on: bool) -> serde_json::Value {
		let off_ms = 10_000.0 + pair as f64 * 100.0;
		let execute_ms = if on { off_ms + 100.0 } else { off_ms };
		let identity = serde_json::json!({
			"executable_sha256": "1".repeat(64),
			"recipe_sha256": "2".repeat(64),
			"source_tree_id": "3".repeat(40),
			"harness_sha256": "4".repeat(64),
			"toron_sha256": "5".repeat(64),
			"authority_issues": [],
		});
		let mut row = serde_json::json!({
			"query": "q21",
			"status": "pass",
			"execute_ms": execute_ms,
			"adr585_pair": pair,
			"adr585_arm": if on { "on" } else { "off" },
			"adr585_pair_position": if pair % 2 == 0 { if on { 1 } else { 0 } } else if on { 0 } else { 1 },
			"diagnostics_disabled": !on,
			"evidence_identity": identity,
		});
		if on {
			let execute_micros = (execute_ms * 1000.0) as u64;
			let root = execute_micros - 1_000;
			let handler = 8_400_000u64;
			row["node_attribution_profile_receipt_path"] =
				serde_json::json!(format!("pair-{pair:02}-on.json"));
			row["node_attribution_outer_reconciliation"] = serde_json::json!({
				"execute_bound_span_micros": root,
				"execute_micros": execute_micros,
				"outer_residual_micros": 1_000,
				"within_boundary": true,
			});
			row["node_attribution"] = serde_json::json!({
				"execute_bound_span_micros": root,
				"aggregate_invocations": [{
					"invocation_id": 17,
					"route": "inner_prefix_semi_anti_grouped",
					"bridge": {"passed": true},
					"stages": {"passed": true},
					"inclusive_lifecycle_receipt": {
						"status": "reconciled",
						"passed": true,
						"invocation_id": 17,
						"handler_inclusive_micros": handler,
						"runtime_prep_inclusive_micros": 6_000_000,
						"runtime_prep_inner_1_micros": 3_000_000,
						"runtime_prep_inner_2_micros": 1_600_000,
						"runtime_prep_semi_read_micros": 300_000,
						"runtime_prep_semi_write_micros": 200_000,
						"runtime_prep_anti_read_micros": 400_000,
						"runtime_prep_anti_write_micros": 300_000,
						"runtime_prep_semi_finish_micros": 0,
						"runtime_prep_anti_finish_micros": 0,
						"runtime_prep_residual_micros": 200_000,
						"base_prep_inclusive_micros": 500_000,
						"drive_inclusive_micros": 1_500_000,
						"finalize_inclusive_micros": 100_000,
						"cleanup_inclusive_micros": 100_000,
						"lifecycle_residual_micros": 200_000,
						"outside_handler_span_micros": root - handler,
					}
				}]
			});
		}
		row
	}

	fn reconcile_adr585_fixture(row: &mut serde_json::Value) {
		let root = row["node_attribution"]["execute_bound_span_micros"]
			.as_u64()
			.unwrap();
		let receipt = row
			.pointer_mut("/node_attribution/aggregate_invocations/0/inclusive_lifecycle_receipt")
			.expect("fixture receipt");
		let children = [
			"runtime_prep_inner_1_micros",
			"runtime_prep_inner_2_micros",
			"runtime_prep_semi_read_micros",
			"runtime_prep_semi_write_micros",
			"runtime_prep_semi_finish_micros",
			"runtime_prep_anti_read_micros",
			"runtime_prep_anti_write_micros",
			"runtime_prep_anti_finish_micros",
		]
		.into_iter()
		.map(|field| receipt[field].as_u64().expect("fixture micros"))
		.sum::<u64>();
		let runtime = children + receipt["runtime_prep_residual_micros"].as_u64().unwrap();
		receipt["runtime_prep_inclusive_micros"] = serde_json::json!(runtime);
		let handler = runtime
			+ receipt["base_prep_inclusive_micros"].as_u64().unwrap()
			+ receipt["drive_inclusive_micros"].as_u64().unwrap()
			+ receipt["finalize_inclusive_micros"].as_u64().unwrap()
			+ receipt["cleanup_inclusive_micros"].as_u64().unwrap()
			+ receipt["lifecycle_residual_micros"].as_u64().unwrap();
		receipt["handler_inclusive_micros"] = serde_json::json!(handler);
		receipt["outside_handler_span_micros"] = serde_json::json!(root - handler);
	}

	/// An ADR 586 row: the ADR 585 fixture under 586's tags, with a 2.4 s semi write and a 1.8 s
	/// anti write each carrying the fourth band. `semi`/`anti` are the four children in micros;
	/// the residual is whatever the write term leaves.
	fn adr586_fixture_row(
		pair: usize,
		on: bool,
		semi: [u64; 4],
		anti: [u64; 4],
	) -> serde_json::Value {
		let mut row = adr585_fixture_row(pair, on);
		for (from, to) in [
			("adr585_pair", "adr586_pair"),
			("adr585_arm", "adr586_arm"),
			("adr585_pair_position", "adr586_pair_position"),
		] {
			let value = row[from].take();
			row[to] = value;
			row.as_object_mut().unwrap().remove(from);
		}
		if on {
			let receipt = row
				.pointer_mut(
					"/node_attribution/aggregate_invocations/0/inclusive_lifecycle_receipt",
				)
				.expect("fixture receipt");
			receipt["runtime_prep_inner_1_micros"] = serde_json::json!(1_000_000);
			receipt["runtime_prep_inner_2_micros"] = serde_json::json!(200_000);
			receipt["runtime_prep_semi_write_micros"] = serde_json::json!(2_400_000);
			receipt["runtime_prep_anti_write_micros"] = serde_json::json!(1_800_000);
			for (prefix, children, ops) in [
				(
					"semi_write",
					semi,
					serde_json::json!(["project", "exchange"]),
				),
				(
					"anti_write",
					anti,
					serde_json::json!(["filter", "project", "exchange"]),
				),
			] {
				let write = receipt[format!("runtime_prep_{prefix}_micros")]
					.as_u64()
					.unwrap();
				for (child, micros) in ADR586_WRITE_CHILDREN.iter().zip(children) {
					receipt[format!("{prefix}_{child}_micros")] = serde_json::json!(micros);
					receipt[format!("{prefix}_{child}_passes")] =
						serde_json::json!(if micros == 0 { 0 } else { 23_000 });
				}
				receipt[format!("{prefix}_residual_micros")] =
					serde_json::json!(write - children.iter().sum::<u64>());
				receipt[format!("{prefix}_ops")] = ops;
			}
			reconcile_adr585_fixture(&mut row);
		}
		row
	}

	// ADR 594: an on row whose receipt carries the sixth band. `micros` are the three children;
	// the residual is whatever of inner_1 (1,000,000 µs in the ADR 585 fixture) they leave.
	fn adr594_fixture_row(
		pair: usize,
		on: bool,
		micros: [u64; 3],
		facts: serde_json::Value,
	) -> serde_json::Value {
		adr594_fixture_row_with_term(pair, on, micros, facts, 1_700_000)
	}

	fn adr594_fixture_row_with_term(
		pair: usize,
		on: bool,
		micros: [u64; 3],
		facts: serde_json::Value,
		inner_1: u64,
	) -> serde_json::Value {
		let mut row = adr585_fixture_row(pair, on);
		for (from, to) in [
			("adr585_pair", "adr594_pair"),
			("adr585_arm", "adr594_arm"),
			("adr585_pair_position", "adr594_pair_position"),
		] {
			let value = row[from].take();
			row[to] = value;
			row.as_object_mut().unwrap().remove(from);
		}
		if on {
			let receipt = row
				.pointer_mut(
					"/node_attribution/aggregate_invocations/0/inclusive_lifecycle_receipt",
				)
				.expect("fixture receipt");
			// A 1,700,000 µs term (the ADR 585 fixture's own is 3,000,000; the reconcile below
			// rebalances the band): the residual bound is 185,000 µs and the floor 800 ms.
			receipt["runtime_prep_inner_1_micros"] = serde_json::json!(inner_1);
			if let Some(branch) = facts.get("branch") {
				receipt["inner_1_branch"] = branch.clone();
			}
			if facts.get("branch") != Some(&serde_json::json!("unsupported"))
				&& facts.get("branch").is_some()
			{
				for (child, value) in ADR594_CHILDREN.iter().zip(micros) {
					receipt[format!("{child}_micros")] = serde_json::json!(value);
					receipt[format!("{child}_calls")] =
						serde_json::json!(if *child == "inner_1_build_write" && value == 0 {
							0
						} else {
							1
						});
				}
				receipt["inner_1_residual_micros"] =
					serde_json::json!(inner_1 - micros.iter().sum::<u64>());
				for (name, value) in facts.as_object().unwrap() {
					if name != "branch" {
						receipt[format!("inner_1_{name}")] = value.clone();
					}
				}
				receipt["inner_1_facts_consistent"] = facts
					.get("consistent")
					.cloned()
					.unwrap_or(serde_json::json!(true));
				receipt["inner_1_inconsistency"] = facts
					.get("inconsistency")
					.cloned()
					.unwrap_or(serde_json::Value::Null);
			}
			reconcile_adr585_fixture(&mut row);
		}
		row
	}

	fn adr594_rows(micros: [u64; 3], facts: serde_json::Value) -> Vec<serde_json::Value> {
		(0..ADR585_PAIRS)
			.flat_map(|pair| {
				[
					adr594_fixture_row(pair, false, micros, facts.clone()),
					adr594_fixture_row(pair, true, micros, facts.clone()),
				]
			})
			.collect()
	}

	fn adr594_partitioned_facts() -> serde_json::Value {
		serde_json::json!({
			"branch": "partitioned", "partition_count": 4, "group_count": 2,
			"partition_sizing_passes": 2, "estimated_build_bytes": 300_000_000,
			"sizing_rows": 7_300_000, "build_rows": null, "build_spill_bytes": null,
		})
	}

	// The fixture's off arms span 400 ms, so the floor is 800 ms and a lead must exceed 400 ms.
	#[test]
	fn adr594_the_four_term_rule_returns_each_named_outcome() {
		// Resolved: the sizing scan leads by more than the range.
		let rows = adr594_rows([1_300_000, 280_000, 0], adr594_partitioned_facts());
		let summary = evaluate_adr594_pairs(&rows).expect("complete fixture");
		assert_eq!(
			summary["owner"]["status"], "resolved",
			"{}",
			summary["owner"]
		);
		assert_eq!(
			summary["owner"]["status"],
			"resolved",
			"{} / complete={} residuals={} facts={} pair0={}",
			summary["owner"],
			summary["all_profiles_complete"],
			summary["all_inner_1_residuals_complete"],
			summary["all_inner_1_facts_consistent"],
			summary["pairs"][0]
		);
		assert_eq!(
			summary["owner"]["status"], "resolved",
			"{}",
			summary["owner"]
		);
		assert_eq!(summary["owner"]["term"], "inner_1_sizing_scan");
		assert_eq!(
			summary["inner_1_branches"],
			serde_json::json!(["partitioned"])
		);
		assert_eq!(summary["all_inner_1_facts_consistent"], true);
		let band = &summary["adr594_pair_bands"][0];
		assert_eq!(band["inner_1_build_write_calls"], 0);
		assert_eq!(band["inner_1_residual_micros"], 120_000);
		assert_eq!(band["inner_1_partition_sizing_passes"], 2);
		let terms = summary["inner_1_terms_scaled_ms"].as_array().unwrap();
		assert_eq!(terms.len(), 4, "three children and the residual");
		// Unresolved tie: two terms clear the floor inside the range of each other.
		let rows = adr594_rows([850_000, 730_000, 0], adr594_partitioned_facts());
		let summary = evaluate_adr594_pairs(&rows).unwrap();
		assert_eq!(
			summary["owner"]["status"],
			"unresolved_tie_within_off_range"
		);
		// Below floor: no term clears 800 ms of a 1 s term scaled by ~1.
		let rows = adr594_rows([790_000, 790_000, 0], adr594_partitioned_facts());
		let summary = evaluate_adr594_pairs(&rows).unwrap();
		assert_eq!(summary["owner"]["status"], "below_floor_no_mechanism");
		// Incomplete: a residual that leads.
		let rows = adr594_rows([100_000, 100_000, 0], adr594_partitioned_facts());
		let summary = evaluate_adr594_pairs(&rows).unwrap();
		assert_eq!(summary["owner"]["status"], "withheld_incomplete_profiles");
		assert_eq!(summary["all_inner_1_residuals_complete"], false);
		assert_eq!(
			summary["adr594_pair_bands"][0]["inner_1_residual_complete"],
			false
		);
		// Incomplete: inconsistent facts, even though the residual bound passes.
		let mut facts = adr594_partitioned_facts();
		facts["consistent"] = serde_json::json!(false);
		facts["inconsistency"] = serde_json::json!("partitioned branch with a build-write call");
		let rows = adr594_rows([1_300_000, 280_000, 0], facts);
		let summary = evaluate_adr594_pairs(&rows).unwrap();
		assert_eq!(summary["owner"]["status"], "withheld_instrument_incomplete");
		assert!(
			summary["owner"]["incompleteness"][0]
				.as_str()
				.unwrap()
				.contains("build-write call")
		);
		// Unsupported path: the fallback ran on a pair.
		let rows = adr594_rows([0, 0, 0], serde_json::json!({"branch": "unsupported"}));
		let summary = evaluate_adr594_pairs(&rows).unwrap();
		assert_eq!(summary["owner"]["status"], "withheld_unsupported_path");
		// Absent band on a non-zero term: incomplete, the pair named.
		let rows = adr594_rows([0, 0, 0], serde_json::json!({}));
		let summary = evaluate_adr594_pairs(&rows).unwrap();
		assert_eq!(summary["owner"]["status"], "withheld_instrument_incomplete");
		assert!(
			summary["owner"]["incompleteness"][0]
				.as_str()
				.unwrap()
				.contains("band is absent")
		);
		// Closed on variability, applied after the rule as ADR 593 fixed.
		let rows = adr594_rows([1_300_000, 280_000, 0], adr594_partitioned_facts());
		let mut summary = evaluate_adr594_pairs(&rows).unwrap();
		summary["off_execute_range_ms"] = serde_json::json!(700.0);
		adr593_apply_variability_bound(&mut summary).unwrap();
		assert_eq!(summary["owner"]["status"], "closed_on_variability");
		assert_eq!(summary["owner"]["leading_term"], "inner_1_sizing_scan");
	}

	// A residual that leads is incomplete even below the floor and inside its own bound: a
	// 300,000 µs term whose children are 95,000 each leaves a 110,000 µs residual, under the
	// 115,000 µs bound and under the 800 ms floor, and still the children have not named it.
	#[test]
	fn adr594_a_leading_residual_is_incomplete_at_any_size() {
		let rows: Vec<serde_json::Value> = (0..ADR585_PAIRS)
			.flat_map(|pair| {
				[
					adr594_fixture_row_with_term(
						pair,
						false,
						[95_000, 95_000, 0],
						adr594_partitioned_facts(),
						300_000,
					),
					adr594_fixture_row_with_term(
						pair,
						true,
						[95_000, 95_000, 0],
						adr594_partitioned_facts(),
						300_000,
					),
				]
			})
			.collect();
		let summary = evaluate_adr594_pairs(&rows).unwrap();
		assert_eq!(
			summary["all_inner_1_residuals_complete"], true,
			"inside its bound"
		);
		assert_eq!(
			summary["owner"]["status"], "withheld_instrument_incomplete",
			"{}",
			summary["owner"]
		);
		assert_eq!(summary["owner"]["leading_residual"], "inner_1_residual");
		assert!(summary["owner"]["median_scaled_ms"].as_f64().unwrap() < 500.0);
	}

	use toron_tpch::host_readings::{JournalCollection, JournalEntry, JournalState};

	fn adr595_launch() -> Adr595Launch {
		Adr595Launch {
			launcher_session_id: "2300".to_string(),
			boot_id: "882b9409-f9d4-4b4e-b2ca-bffc1de1f891".to_string(),
			attempt_token: "t0k3n".to_string(),
			launch_unix_ms: 100_000.0,
			delay_seconds: 120.0,
			timer_unit: Some("adr595-inner-1.timer".to_string()),
			timer_last_trigger_unix_ms: Some(220_000.0),
			driver_start_unix_ms: 220_500.0,
		}
	}

	fn adr595_entry(ms: f64, source: &str, message: &str, boot: &str) -> JournalEntry {
		JournalEntry {
			realtime_us: (ms * 1000.0) as u64,
			monotonic_us: None,
			boot_id: Some(boot.to_string()),
			source: Some(source.to_string()),
			message: message.to_string(),
		}
	}

	fn adr595_collection(
		state: JournalState,
		since: f64,
		until: f64,
		entries: Vec<JournalEntry>,
	) -> JournalCollection {
		JournalCollection {
			since_unix_ms: since,
			until_unix_ms: until,
			state,
			entries,
			malformed_records: 0,
			bytes_read: 0,
			entry_cap: 2000,
			byte_cap: 1 << 24,
			duration_bound_ms: 30_000,
			command: String::new(),
		}
	}

	// ADR 595: separation is verified only from a collected collection covering the interval,
	// with the launcher's logout on this boot before the first child; every other case is
	// failed or unknown, and a logout outside the collected bounds is unknown, not verified.
	#[test]
	fn adr595_separation_verdicts_cover_every_placement_and_collection_state() {
		let launch = adr595_launch();
		let first = 221_000.0;
		let last = 360_000.0;
		let logout = |ms: f64| {
			adr595_entry(
				ms,
				"systemd-logind.service",
				"Session 2300 logged out. Waiting for processes to exit.",
				"882b9409f9d44b4eb2cabffc1de1f891",
			)
		};
		let verdict = |collection: &JournalCollection| {
			adr595_launch_verdicts(&launch, collection, first, last).separation["verdict"].clone()
		};
		let full = |entries: Vec<JournalEntry>| {
			adr595_collection(JournalState::Collected, 99_000.0, 361_000.0, entries)
		};
		assert_eq!(verdict(&full(vec![logout(100_500.0)])), "verified");
		assert_eq!(
			verdict(&full(vec![logout(250_000.0)])),
			"failed",
			"inside the window"
		);
		assert_eq!(
			verdict(&full(vec![logout(370_000.0)])),
			"failed",
			"after the window"
		);
		assert_eq!(
			verdict(&full(vec![])),
			"failed",
			"absent from a collected collection"
		);
		// The entry must be logind's own: the same text from another source is not evidence.
		let sshd_text = adr595_entry(
			100_500.0,
			"ssh.service",
			"Session 2300 logged out. Waiting for processes to exit.",
			"882b9409f9d44b4eb2cabffc1de1f891",
		);
		assert_eq!(verdict(&full(vec![sshd_text])), "failed", "wrong source");
		// An entry without a boot id is not evidence either.
		let mut no_boot = logout(100_500.0);
		no_boot.boot_id = None;
		assert_eq!(verdict(&full(vec![no_boot])), "failed", "missing boot id");
		// A logout before the separation interval is another launch's, not this one's.
		assert_eq!(
			verdict(&full(vec![logout(50_000.0)])),
			"failed",
			"too early"
		);
		// A malformed launcher boot id verifies nothing.
		let mut malformed = adr595_launch();
		malformed.boot_id = "not-a-boot-id".to_string();
		let verdicts =
			adr595_launch_verdicts(&malformed, &full(vec![logout(100_500.0)]), first, last);
		assert_eq!(verdicts.separation["verdict"], "unknown");
		assert!(
			verdicts.separation["reason"]
				.as_str()
				.unwrap()
				.contains("malformed")
		);
		// The kernel's dashed spelling and the journal's undashed one are the same boot.
		let mut dashed = adr595_launch();
		dashed.boot_id = "882b9409-f9d4-4b4e-b2ca-bffc1de1f891".to_string();
		let undashed = adr595_entry(
			100_500.0,
			"systemd-logind.service",
			"Session 2300 logged out. Waiting for processes to exit.",
			"882b9409f9d44b4eb2cabffc1de1f891",
		);
		let verdicts = adr595_launch_verdicts(&dashed, &full(vec![undashed]), first, last);
		assert_eq!(
			verdicts.separation["verdict"], "verified",
			"{}",
			verdicts.separation
		);
		// The logout entry from another boot does not count.
		let other_boot = adr595_entry(
			100_500.0,
			"systemd-logind.service",
			"Session 2300 logged out. Waiting for processes to exit.",
			"11111111222233334444555555555555",
		);
		assert_eq!(verdict(&full(vec![other_boot])), "failed");
		// Bounds that start after the launch instant leave the logout outside them: unknown.
		let late_bounds = adr595_collection(
			JournalState::Collected,
			220_000.0,
			361_000.0,
			vec![logout(100_500.0)],
		);
		assert_eq!(verdict(&late_bounds), "unknown");
		for state in [
			JournalState::Truncated("entry cap".to_string()),
			JournalState::Degraded("1 malformed".to_string()),
			JournalState::Unavailable("journalctl exited 1".to_string()),
		] {
			let collection =
				adr595_collection(state.clone(), 99_000.0, 361_000.0, vec![logout(100_500.0)]);
			assert_eq!(verdict(&collection), "unknown", "{state:?}");
		}
	}

	#[test]
	fn adr595_window_verdicts_name_session_events_and_need_complete_coverage() {
		let launch = adr595_launch();
		let first = 221_000.0;
		let last = 360_000.0;
		let logout = adr595_entry(
			100_500.0,
			"systemd-logind.service",
			"Session 2300 logged out.",
			"882b9409f9d44b4eb2cabffc1de1f891",
		);
		let clean = adr595_collection(
			JournalState::Collected,
			99_000.0,
			361_000.0,
			vec![
				logout.clone(),
				adr595_entry(
					300_000.0,
					"kernel",
					"[UFW BLOCK] ...",
					"882b9409f9d44b4eb2cabffc1de1f891",
				),
			],
		);
		let verdicts = adr595_launch_verdicts(&launch, &clean, first, last);
		assert_eq!(verdicts.window["verdict"], "verified");
		assert_eq!(
			verdicts.window["session_events"].as_array().unwrap().len(),
			0
		);
		let churn = adr595_collection(
			JournalState::Collected,
			99_000.0,
			361_000.0,
			vec![
				logout.clone(),
				adr595_entry(
					300_000.0,
					"ssh.service",
					"Accepted publickey for me from 192.168.0.5",
					"882b9409f9d44b4eb2cabffc1de1f891",
				),
			],
		);
		let verdicts = adr595_launch_verdicts(&launch, &churn, first, last);
		assert_eq!(verdicts.window["verdict"], "failed");
		assert_eq!(
			verdicts.window["session_events"].as_array().unwrap().len(),
			1
		);
		for state in [
			JournalState::Truncated("entry cap".to_string()),
			JournalState::Degraded("1 malformed".to_string()),
			JournalState::Unavailable("journalctl exited 1".to_string()),
		] {
			let collection =
				adr595_collection(state.clone(), 99_000.0, 361_000.0, vec![logout.clone()]);
			let verdicts = adr595_launch_verdicts(&launch, &collection, first, last);
			assert_eq!(verdicts.window["verdict"], "unknown", "{state:?}");
		}
		// Bounds ending before the last return: unknown even with no events.
		let short = adr595_collection(JournalState::Collected, 99_000.0, 350_000.0, vec![logout]);
		let verdicts = adr595_launch_verdicts(&launch, &short, first, last);
		assert_eq!(verdicts.window["verdict"], "unknown");
	}

	// ADR 595: precedence withholds the owner with the named status and keeps the rule's
	// outcome beside it; a verified pair leaves the owner alone.
	#[test]
	fn adr595_precedence_withholds_the_owner_unless_both_intervals_are_verified() {
		let resolved =
			|| serde_json::json!({"owner": {"status": "resolved", "term": "inner_1_build_write"}});
		let v = |sep: &str, win: &str| Adr595Verdicts {
			separation: serde_json::json!({"verdict": sep}),
			window: serde_json::json!({"verdict": win}),
		};
		let mut summary = resolved();
		adr595_apply_launch_precedence(&mut summary, &v("verified", "verified"));
		assert_eq!(summary["owner"]["status"], "resolved");
		for (sep, win, expected) in [
			("failed", "verified", "withheld_launch_separation_failed"),
			("unknown", "verified", "withheld_launch_separation_unknown"),
			("verified", "failed", "withheld_window_not_clean_failed"),
			("verified", "unknown", "withheld_window_not_clean_unknown"),
			("failed", "failed", "withheld_launch_separation_failed"),
		] {
			let mut summary = resolved();
			adr595_apply_launch_precedence(&mut summary, &v(sep, win));
			assert_eq!(summary["owner"]["status"], expected, "{sep}/{win}");
			assert_eq!(
				summary["owner"]["rule_outcome_withheld"]["term"],
				"inner_1_build_write"
			);
		}
	}

	// ADR 595: a late trigger or a late driver start is refused; a missing trigger is refused.
	#[test]
	fn adr595_late_starts_are_refused_and_the_boot_is_bound() {
		let launch = adr595_launch();
		adr595_check_late_start("ADR 595", &launch).expect("on time");
		let mut late_trigger = adr595_launch();
		late_trigger.timer_last_trigger_unix_ms = Some(260_000.0);
		let error = adr595_check_late_start("ADR 595", &late_trigger).unwrap_err();
		assert!(error.contains("timer triggered"), "{error}");
		let mut late_driver = adr595_launch();
		late_driver.driver_start_unix_ms = 260_000.0;
		let error = adr595_check_late_start("ADR 595", &late_driver).unwrap_err();
		assert!(error.contains("driver started"), "{error}");
		let mut no_trigger = adr595_launch();
		no_trigger.timer_last_trigger_unix_ms = None;
		assert!(
			adr595_check_late_start("ADR 595", &no_trigger)
				.unwrap_err()
				.contains("last trigger")
		);
		assert_eq!(adr595_parse_systemd_timestamp("n/a"), None);
		assert_eq!(adr595_parse_systemd_timestamp(""), None);
		let parsed = adr595_parse_systemd_timestamp("@1789561508").unwrap();
		assert!((parsed - 1_789_561_508_000.0).abs() < 1.0, "{parsed}");
	}

	// ADR 595: the workstation's wait accepts only this attempt's token; a probe, a stale
	// record, and a foreign token are ignored, and the deadline ends the wait with nothing.
	#[test]
	fn adr595_the_completion_wait_accepts_only_this_attempts_token() {
		use std::net::UdpSocket;
		let script = concat!(
			env!("CARGO_MANIFEST_DIR"),
			"/scripts/adr595_wait_completion.py"
		);
		let probe = UdpSocket::bind("127.0.0.1:0").unwrap();
		let port = probe.local_addr().unwrap().port();
		drop(probe);
		let deadline = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.unwrap()
			.as_millis() as f64
			+ 8_000.0;
		let mut child = std::process::Command::new("python3")
			.args([
				script,
				"--bind",
				"127.0.0.1",
				"--port",
				&port.to_string(),
				"--token",
				"attempt-42",
				"--deadline-unix-ms",
				&format!("{deadline}"),
			])
			.stdout(std::process::Stdio::piped())
			.stderr(std::process::Stdio::piped())
			.spawn()
			.expect("python3 runs the wait");
		std::thread::sleep(std::time::Duration::from_millis(400));
		let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
		let target = format!("127.0.0.1:{port}");
		sender
			.send_to(b"adr595 token=probe result=probe\n", &target)
			.unwrap();
		sender
			.send_to(
				b"adr595 token=attempt-41 result=success exit_code=exited\n",
				&target,
			)
			.unwrap();
		std::thread::sleep(std::time::Duration::from_millis(200));
		assert!(
			child.try_wait().unwrap().is_none(),
			"foreign tokens do not end the wait"
		);
		sender
			.send_to(
				b"adr595 token=attempt-42 result=success exit_code=exited exit_status=0\n",
				&target,
			)
			.unwrap();
		let output = child.wait_with_output().unwrap();
		assert!(output.status.success(), "{:?}", output);
		let stdout = String::from_utf8_lossy(&output.stdout);
		assert!(
			stdout.contains("token=attempt-42") && stdout.contains("received_unix_ms="),
			"{stdout}"
		);
		let stderr = String::from_utf8_lossy(&output.stderr);
		assert_eq!(stderr.matches("ignored datagram").count(), 2, "{stderr}");
		// The deadline: no datagram, exit 1, nothing on stdout.
		let probe = UdpSocket::bind("127.0.0.1:0").unwrap();
		let port = probe.local_addr().unwrap().port();
		drop(probe);
		let soon = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.unwrap()
			.as_millis() as f64
			+ 600.0;
		let output = std::process::Command::new("python3")
			.args([
				script,
				"--bind",
				"127.0.0.1",
				"--port",
				&port.to_string(),
				"--token",
				"attempt-43",
				"--deadline-unix-ms",
				&format!("{soon}"),
			])
			.output()
			.unwrap();
		assert_eq!(output.status.code(), Some(1));
		assert!(output.stdout.is_empty());
	}

	// ADR 595: the re-evaluation's window comes from all eleven rows' stamps, ordered and
	// complete; a missing first launch or last return is refused, never a narrower window.
	#[test]
	fn adr595_the_reevaluation_window_requires_every_stamp() {
		let rows = |n: usize| -> Vec<serde_json::Value> {
			(0..n)
				.map(|i| {
					serde_json::json!({
						"warmup": i == 0,
						"driver_launch_unix_ms": 1000.0 + i as f64 * 100.0,
						"driver_return_unix_ms": 1050.0 + i as f64 * 100.0,
					})
				})
				.collect()
		};
		assert_eq!(
			adr595_window_from_rows(&rows(11)).unwrap(),
			(1000.0, 2050.0)
		);
		assert!(
			adr595_window_from_rows(&rows(10))
				.unwrap_err()
				.contains("requires 11 rows")
		);
		let mut missing_first = rows(11);
		missing_first[0]
			.as_object_mut()
			.unwrap()
			.remove("driver_launch_unix_ms");
		assert!(
			adr595_window_from_rows(&missing_first)
				.unwrap_err()
				.contains("row 0 lacks")
		);
		let mut missing_last = rows(11);
		missing_last[10]["driver_return_unix_ms"] = serde_json::Value::Null;
		assert!(
			adr595_window_from_rows(&missing_last)
				.unwrap_err()
				.contains("row 10 lacks")
		);
		let mut inverted = rows(11);
		inverted[3]["driver_return_unix_ms"] = serde_json::json!(1200.0);
		assert!(
			adr595_window_from_rows(&inverted)
				.unwrap_err()
				.contains("returns before")
		);
		let mut overlapping = rows(11);
		overlapping[4]["driver_launch_unix_ms"] = serde_json::json!(1340.0);
		assert!(
			adr595_window_from_rows(&overlapping)
				.unwrap_err()
				.contains("overlap")
		);
		assert_eq!(
			adr595_parse_boot_id("882b9409-f9d4-4b4e-b2ca-bffc1de1f891").as_deref(),
			Some("882b9409f9d44b4eb2cabffc1de1f891")
		);
		assert_eq!(
			adr595_parse_boot_id("882B9409F9D44B4EB2CABFFC1DE1F891").as_deref(),
			Some("882b9409f9d44b4eb2cabffc1de1f891")
		);
		for bad in [
			"",
			"882b9409",
			"882b9409-f9d4-4b4e-b2ca-bffc1de1f89",
			"zz2b9409-f9d4-4b4e-b2ca-bffc1de1f891",
			"882b9409f9d44b4eb2cabffc1de1f891x",
		] {
			assert_eq!(adr595_parse_boot_id(bad), None, "{bad:?}");
		}
	}

	// The spilled branch reads through the band too, with its completed write.
	#[test]
	fn adr594_the_spilled_branch_names_the_build_write() {
		let facts = serde_json::json!({
			"branch": "spilled", "partition_count": 1, "group_count": 1,
			"partition_sizing_passes": 1, "estimated_build_bytes": 100_000_000,
			"sizing_rows": 7_300_000, "build_rows": 7_300_000, "build_spill_bytes": 27_242_808,
		});
		let rows = adr594_rows([200_000, 200_000, 1_180_000], facts);
		let summary = evaluate_adr594_pairs(&rows).unwrap();
		assert_eq!(summary["owner"]["status"], "resolved");
		assert_eq!(summary["owner"]["term"], "inner_1_build_write");
		assert_eq!(summary["inner_1_branches"], serde_json::json!(["spilled"]));
		assert_eq!(
			summary["adr594_pair_bands"][0]["inner_1_build_rows"],
			7_300_000
		);
	}

	// -----------------------------------------------------------------------------------------
	// ADR 596 fixtures: the seventh band over the sixth band's spilled branch.
	// -----------------------------------------------------------------------------------------

	fn adr596_spilled_facts() -> serde_json::Value {
		serde_json::json!({
			"branch": "spilled", "partition_count": 1, "group_count": 1,
			"partition_sizing_passes": 1, "estimated_build_bytes": 100_000_000,
			"sizing_rows": 7_300_000, "build_rows": 5_845_938, "build_spill_bytes": 27_242_808,
		})
	}

	/// The writer's facts for a projected, filtered, unexchanged write to the stream's end.
	fn adr596_write_facts() -> serde_json::Value {
		serde_json::json!({
			"filter_present": true, "exchange": false, "exchange_partitions": null,
			"projected": true, "termination": "stream_end", "index_mode": "compact",
			"index_variant": "int64",
			"unsupported": null, "batches_read": 120, "batches_limit_discarded": 0,
			"batches_admitted": 120, "batches_filtered_empty": 0,
			"batches_written": 120, "rows_read": 12_000_000, "rows_kept": 5_845_938,
			"chunks": 720, "chunks_unkeyed": 0, "pages": 720, "plaintext_bytes": 60_000_000,
			"ciphertext_bytes": 27_242_808, "index_entries": 5_845_938, "index_compactions": 0,
			"shape": "supported", "consistent": true, "inconsistency": null,
		})
	}

	/// One ADR 596 row: ADR 594's spilled band (sizing 200 ms, partition sizing 200 ms, build
	/// write 1,180 ms of a 1,700 ms term) with the eleven children of the build write and the
	/// writer's facts on the receipt, under this record's tags.
	fn adr596_fixture_row(
		pair: usize,
		on: bool,
		children: [u64; 11],
		write_facts: serde_json::Value,
		inner_1_facts: serde_json::Value,
	) -> serde_json::Value {
		let mut row = adr594_fixture_row_with_term(
			pair,
			on,
			[200_000, 200_000, 1_180_000],
			inner_1_facts.clone(),
			1_700_000,
		);
		for (from, to) in [
			("adr594_pair", "adr596_pair"),
			("adr594_arm", "adr596_arm"),
			("adr594_pair_position", "adr596_pair_position"),
		] {
			let value = row[from].take();
			row[to] = value;
			row.as_object_mut().unwrap().remove(from);
		}
		if on && inner_1_facts.get("branch") == Some(&serde_json::json!("spilled")) {
			let receipt = row
				.pointer_mut(
					"/node_attribution/aggregate_invocations/0/inclusive_lifecycle_receipt",
				)
				.expect("fixture receipt");
			// Calls as the ledger would count them on the facts' path.
			let fact = |name: &str| write_facts[name].as_u64().unwrap_or(0);
			let flag = |name: &str| write_facts[name].as_bool().unwrap_or(false);
			let admitted = fact("batches_admitted");
			let calls = |child: &str| match child {
				"build_write_create" | "build_write_scan_open" | "build_write_finish" => 1,
				"build_write_scan_poll" => {
					if write_facts["termination"] == "row_limit" {
						fact("batches_read")
					} else {
						fact("batches_read") + 1
					}
				}
				"build_write_filter" => {
					if flag("filter_present") {
						admitted
					} else {
						0
					}
				}
				"build_write_repartition" => {
					if flag("exchange") {
						admitted - fact("batches_filtered_empty")
					} else {
						0
					}
				}
				"build_write_project" => {
					if flag("projected") {
						fact("chunks")
					} else {
						0
					}
				}
				"build_write_key_extract" => fact("chunks"),
				_ => fact("pages"),
			};
			for (child, value) in ADR596_CHILDREN.iter().zip(children) {
				receipt[format!("{child}_micros")] = serde_json::json!(value);
				receipt[format!("{child}_calls")] = serde_json::json!(calls(child));
			}
			receipt["build_write_residual_micros"] =
				serde_json::json!(1_180_000 - children.iter().sum::<u64>());
			for (name, value) in write_facts.as_object().unwrap() {
				match name.as_str() {
					"shape" => receipt["build_write_shape"] = value.clone(),
					"consistent" => receipt["build_write_facts_consistent"] = value.clone(),
					"inconsistency" => receipt["build_write_inconsistency"] = value.clone(),
					_ => receipt[format!("build_write_{name}")] = value.clone(),
				}
			}
		}
		row
	}

	fn adr596_rows(children: [u64; 11], write_facts: serde_json::Value) -> Vec<serde_json::Value> {
		adr596_rows_on(children, write_facts, adr596_spilled_facts())
	}

	fn adr596_rows_on(
		children: [u64; 11],
		write_facts: serde_json::Value,
		inner_1_facts: serde_json::Value,
	) -> Vec<serde_json::Value> {
		(0..ADR585_PAIRS)
			.flat_map(|pair| {
				[
					adr596_fixture_row(
						pair,
						false,
						children,
						write_facts.clone(),
						inner_1_facts.clone(),
					),
					adr596_fixture_row(
						pair,
						true,
						children,
						write_facts.clone(),
						inner_1_facts.clone(),
					),
				]
			})
			.collect()
	}

	/// Children in receipt order: create, scan_open, scan_poll, filter, repartition, project,
	/// key_extract, encode, page_write, index, finish.
	const ADR596_BELOW_FLOOR: [u64; 11] = [
		1_000, 20_000, 300_000, 100_000, 0, 60_000, 90_000, 150_000, 250_000, 150_000, 5_000,
	];

	/// ADR 600: the seventh band carries the writer's index variant beside its mode, and the
	/// sixth band carries replay 1's lookup counts as the receipt names them (absent stays
	/// null, never zero).
	#[test]
	fn adr600_the_bands_carry_the_index_variant_and_the_lookup_counts() {
		let children = [
			1_000, 20_000, 30_000, 20_000, 0, 10_000, 20_000, 40_000, 950_000, 60_000, 5_000,
		];
		let mut inner_1_facts = adr596_spilled_facts();
		inner_1_facts["lookups_counted"] = serde_json::json!(true);
		inner_1_facts["lookup_hits"] = serde_json::json!(400);
		inner_1_facts["lookup_misses"] = serde_json::json!(12);
		inner_1_facts["lookup_form"] = serde_json::json!("inline");
		inner_1_facts["lookup_inline_hits"] = serde_json::json!(390);
		let summary = evaluate_adr596_pairs(&adr596_rows_on(
			children,
			adr596_write_facts(),
			inner_1_facts,
		))
		.unwrap();
		let band = &summary["adr596_pair_bands"][0];
		assert_eq!(band["build_write_index_mode"], "compact");
		assert_eq!(band["build_write_index_variant"], "int64");
		let sixth = &summary["adr594_pair_bands"][0];
		assert_eq!(sixth["inner_1_lookups_counted"], true);
		assert_eq!(sixth["inner_1_lookup_hits"], 400);
		assert_eq!(sixth["inner_1_lookup_misses"], 12);
		// ADR 602: the lookup form and the inline hits ride the same band.
		assert_eq!(sixth["inner_1_lookup_form"], "inline");
		assert_eq!(sixth["inner_1_lookup_inline_hits"], 390);

		let summary = evaluate_adr596_pairs(&adr596_rows(children, adr596_write_facts())).unwrap();
		let sixth = &summary["adr594_pair_bands"][0];
		assert_eq!(sixth["inner_1_lookups_counted"], serde_json::Value::Null);
		assert_eq!(sixth["inner_1_lookup_hits"], serde_json::Value::Null);
		assert_eq!(sixth["inner_1_lookup_form"], serde_json::Value::Null);
		assert_eq!(sixth["inner_1_lookup_inline_hits"], serde_json::Value::Null);
	}

	/// ADR 601: a receipt of the `index_build` facts vintage carries the reserve and finalization terms
	/// into the band, the rule, and the residual's algebra; an earlier vintage is neither
	/// asked for them nor shows them, and the rule ranks eleven terms as before.
	#[test]
	fn adr601_the_bands_carry_the_finish_built_terms_by_vintage() {
		let children = [
			1_000, 20_000, 30_000, 20_000, 0, 10_000, 20_000, 40_000, 700_000, 60_000, 5_000,
		];
		let mut write_facts = adr596_write_facts();
		write_facts["index_build"] = serde_json::json!("finish");
		write_facts["index_expected_rows"] = serde_json::json!(5_845_938);
		write_facts["index_capacity_growths"] = serde_json::json!(0);
		write_facts["finish_index_entries"] = serde_json::json!(5_845_938);
		write_facts["finish_index_duplicates"] = serde_json::json!(0);
		write_facts["finish_index_buckets"] = serde_json::json!(16_777_216);
		let mut rows = adr596_rows(children, write_facts);
		let eleven: u64 = children.iter().sum();
		for row in &mut rows {
			let Some(receipt) = row.pointer_mut(
				"/node_attribution/aggregate_invocations/0/inclusive_lifecycle_receipt",
			) else {
				continue;
			};
			if receipt.get("build_write_facts_vintage").is_none()
				&& receipt.get("build_write_index_build").is_none()
			{
				continue;
			}
			receipt["build_write_facts_vintage"] = serde_json::json!("index_build");
			receipt["build_write_index_build"] = serde_json::json!("finish");
			receipt["build_write_reserve_micros"] = serde_json::json!(2_000);
			receipt["build_write_reserve_calls"] = serde_json::json!(1);
			receipt["build_write_finish_index_micros"] = serde_json::json!(250_000);
			receipt["build_write_finish_index_calls"] = serde_json::json!(1);
			receipt["build_write_residual_micros"] =
				serde_json::json!(1_180_000 - eleven - 2_000 - 250_000);
		}
		let summary = evaluate_adr596_pairs(&rows).unwrap();
		assert_eq!(
			summary["all_build_write_facts_consistent"], true,
			"{}",
			summary["adr596_pair_bands"][0]
		);
		let band = &summary["adr596_pair_bands"][0];
		assert_eq!(band["build_write_reserve_micros"], 2_000);
		assert_eq!(band["build_write_finish_index_micros"], 250_000);
		assert_eq!(band["build_write_finish_index_calls"], 1);
		assert_eq!(band["build_write_index_build"], "finish");
		assert_eq!(band["build_write_finish_index_entries"], 5_845_938);
		assert_eq!(band["build_write_finish_index_buckets"], 16_777_216);
		assert_eq!(
			band["build_write_residual_micros"],
			1_180_000 - eleven - 252_000
		);
		let terms = summary["build_write_terms_scaled_ms"].as_array().unwrap();
		assert_eq!(terms.len(), 13, "twelve rule children and the residual");
		let names: Vec<&str> = terms.iter().map(|t| t["term"].as_str().unwrap()).collect();
		assert!(
			names.contains(&"build_write_finish_index") && names.contains(&"build_write_reserve")
		);
		assert_eq!(names[0], "build_write_page_write");
		assert_eq!(names[1], "build_write_finish_index");

		// An earlier vintage: no such terms in the band, eleven rule terms.
		let summary = evaluate_adr596_pairs(&adr596_rows(children, adr596_write_facts())).unwrap();
		let band = &summary["adr596_pair_bands"][0];
		assert!(band.get("build_write_reserve_micros").is_none());
		assert_eq!(
			summary["build_write_terms_scaled_ms"]
				.as_array()
				.unwrap()
				.len(),
			11
		);
	}

	// The fixture's off arms span 400 ms, so the floor is 800 ms and a lead must exceed 400 ms.
	// The rule ranks the scan wait once (open plus poll), never its components beside it.
	#[test]
	fn adr596_the_eleven_term_rule_returns_each_named_outcome() {
		// Resolved: the page write leads by more than the range and clears the floor.
		let children = [
			1_000, 20_000, 30_000, 20_000, 0, 10_000, 20_000, 40_000, 950_000, 60_000, 5_000,
		];
		let summary = evaluate_adr596_pairs(&adr596_rows(children, adr596_write_facts())).unwrap();
		assert_eq!(
			summary["owner"]["status"], "resolved",
			"{}",
			summary["owner"]
		);
		assert_eq!(summary["owner"]["term"], "build_write_page_write");
		assert_eq!(
			summary["adr594_four_term_owner"]["term"],
			"inner_1_build_write"
		);
		assert_eq!(summary["all_build_write_facts_consistent"], true);
		assert_eq!(summary["all_build_write_residuals_complete"], true);
		let band = &summary["adr596_pair_bands"][0];
		assert_eq!(band["build_write_scan_wait_micros"], 50_000);
		assert_eq!(band["build_write_residual_micros"], 24_000);
		assert_eq!(band["build_write_pages"], 720);
		assert_eq!(band["build_write_termination"], "stream_end");
		assert_eq!(band["build_write_repartition_calls"], 0);
		let terms = summary["build_write_terms_scaled_ms"].as_array().unwrap();
		assert_eq!(
			terms.len(),
			11,
			"ten rule children (scan wait once) and the residual"
		);
		let names: Vec<&str> = terms.iter().map(|t| t["term"].as_str().unwrap()).collect();
		assert!(names.contains(&"build_write_scan_wait"));
		assert!(
			!names.contains(&"build_write_scan_open") && !names.contains(&"build_write_scan_poll")
		);
		assert_eq!(names[0], "build_write_page_write");
		assert!(terms[0]["share_of_build_write"].as_f64().unwrap() > 0.8);
		let components = &summary["build_write_scan_wait_components_scaled_ms"];
		assert!(components["build_write_scan_open"].as_f64().unwrap() > 0.0);
		assert!(components["build_write_scan_poll"].as_f64().unwrap() > 0.0);
		let pricing = &summary["owner_removable_pricing"];
		assert_eq!(pricing["status"], "gross_candidate_work");
		assert_eq!(pricing["clears_floor"], true);
		assert!(
			pricing["candidate"]
				.as_str()
				.unwrap()
				.contains("resident build")
		);
		assert!(
			pricing["net_improvement"]
				.as_str()
				.unwrap()
				.contains("unpriced")
		);

		// Resolved on the scan wait: open and poll sum past the floor though neither alone does.
		let children = [
			1_000, 400_000, 600_000, 20_000, 0, 10_000, 20_000, 40_000, 30_000, 20_000, 5_000,
		];
		let summary = evaluate_adr596_pairs(&adr596_rows(children, adr596_write_facts())).unwrap();
		assert_eq!(
			summary["owner"]["status"], "resolved",
			"{}",
			summary["owner"]
		);
		assert_eq!(summary["owner"]["term"], "build_write_scan_wait");
		assert!(
			summary["owner_removable_pricing"]["candidate"]
				.as_str()
				.unwrap()
				.contains("carried")
		);

		// Below floor: no child above 800 ms; the finding is the shape, not a failure.
		let summary =
			evaluate_adr596_pairs(&adr596_rows(ADR596_BELOW_FLOOR, adr596_write_facts())).unwrap();
		assert_eq!(
			summary["owner"]["status"], "below_floor_no_mechanism",
			"{}",
			summary["owner"]
		);
		assert_eq!(summary["owner"]["leading_term"], "build_write_scan_wait");
		assert_eq!(summary["owner_removable_pricing"]["status"], "no_owner");

		// Two children splitting the term evenly: neither clears the floor, so the outcome is
		// below floor, not a tie (a tie needs two terms above 800 ms inside a 1,180 ms term,
		// which this fixture cannot hold; the tie outcome is ADR 586's, tested there).
		let children = [
			1_000, 20_000, 30_000, 20_000, 0, 10_000, 20_000, 40_000, 500_000, 500_000, 5_000,
		];
		let summary = evaluate_adr596_pairs(&adr596_rows(children, adr596_write_facts())).unwrap();
		assert_eq!(summary["owner"]["status"], "below_floor_no_mechanism");
	}

	// A leading residual is an incomplete instrument at any size; inconsistent facts, an
	// absent band, the partitioned branch, and an unsupported writer shape each withhold.
	#[test]
	fn adr596_withholds_on_a_leading_residual_inconsistent_facts_and_unsupported_shapes() {
		// The children sum to 1,030 ms of the 1,180 ms term, none above 145 ms: the residual
		// (150 ms) is inside its bound (overhead 100 ms + 5% of the term) yet leads.
		let children = [
			5_000, 5_000, 140_000, 140_000, 0, 130_000, 145_000, 145_000, 145_000, 145_000, 30_000,
		];
		let summary = evaluate_adr596_pairs(&adr596_rows(children, adr596_write_facts())).unwrap();
		assert_eq!(summary["all_build_write_residuals_complete"], true);
		assert_eq!(
			summary["owner"]["status"], "withheld_instrument_incomplete",
			"{}",
			summary["owner"]
		);
		assert_eq!(summary["owner"]["leading_residual"], "build_write_residual");

		// A residual past its bound: incomplete profiles, whatever leads.
		let children = [
			1_000, 20_000, 30_000, 20_000, 0, 10_000, 20_000, 40_000, 150_000, 84_000, 5_000,
		];
		let summary = evaluate_adr596_pairs(&adr596_rows(children, adr596_write_facts())).unwrap();
		assert_eq!(
			summary["owner"]["status"], "withheld_incomplete_profiles",
			"{}",
			summary["owner"]
		);
		assert_eq!(summary["all_build_write_residuals_complete"], false);

		// Inconsistent facts on one pair.
		let mut facts = adr596_write_facts();
		facts["consistent"] = serde_json::json!(false);
		facts["inconsistency"] =
			serde_json::json!("pages disagree with chunks less chunks_unkeyed");
		let summary = evaluate_adr596_pairs(&adr596_rows(ADR596_BELOW_FLOOR, facts)).unwrap();
		assert_eq!(summary["owner"]["status"], "withheld_instrument_incomplete");
		assert!(
			summary["owner"]["incompleteness"][0]
				.as_str()
				.unwrap()
				.contains("pages disagree")
		);

		// The seventh band absent while the sixth is present: incomplete.
		let rows = adr596_rows(ADR596_BELOW_FLOOR, adr596_write_facts())
			.into_iter()
			.map(|mut row| {
				if let Some(receipt) = row.pointer_mut(
					"/node_attribution/aggregate_invocations/0/inclusive_lifecycle_receipt",
				) {
					let object = receipt.as_object_mut().unwrap();
					object.retain(|key, _| !key.starts_with("build_write_"));
				}
				row
			})
			.collect::<Vec<_>>();
		let summary = evaluate_adr596_pairs(&rows).unwrap();
		assert_eq!(summary["owner"]["status"], "withheld_instrument_incomplete");
		assert!(
			summary["owner"]["incompleteness"][0]
				.as_str()
				.unwrap()
				.contains("seventh band is absent")
		);

		// The partitioned branch: no build write to decompose.
		let summary = evaluate_adr596_pairs(&adr596_rows_on(
			[0; 11],
			adr596_write_facts(),
			adr594_partitioned_facts(),
		))
		.unwrap();
		assert_eq!(
			summary["owner"]["status"], "withheld_unsupported_path",
			"{}",
			summary["owner"]
		);
		assert_eq!(
			summary["adr596_pair_bands"][0]["build_write_unsupported"],
			"branch partitioned"
		);

		// An unsupported writer shape, reported by the receipt.
		let mut facts = adr596_write_facts();
		facts["shape"] = serde_json::json!("unsupported");
		facts["unsupported"] = serde_json::json!("preserved_rows");
		facts["consistent"] = serde_json::json!(false);
		facts["inconsistency"] = serde_json::json!("unsupported shape: preserved_rows");
		let summary = evaluate_adr596_pairs(&adr596_rows(ADR596_BELOW_FLOOR, facts)).unwrap();
		assert_eq!(summary["owner"]["status"], "withheld_unsupported_path");
		assert_eq!(
			summary["owner"]["pairs"],
			serde_json::json!([0, 1, 2, 3, 4])
		);

		// ADR 595's precedence still applies over this record's outcome.
		let summary =
			evaluate_adr596_pairs(&adr596_rows(ADR596_BELOW_FLOOR, adr596_write_facts())).unwrap();
		let mut withheld = summary.clone();
		adr595_apply_launch_precedence(
			&mut withheld,
			&Adr595Verdicts {
				separation: serde_json::json!({"verdict": "unknown"}),
				window: serde_json::json!({"verdict": "verified"}),
			},
		);
		assert_eq!(
			withheld["owner"]["status"],
			"withheld_launch_separation_unknown"
		);
		assert_eq!(
			withheld["owner"]["rule_outcome_withheld"]["status"],
			"below_floor_no_mechanism"
		);
	}

	/// The fixture rows with the writer's facts also stored as the profile's diagnostic, as a
	/// real profile carries them; `strip` removes facts from the diagnostic to stand in for a
	/// receipt recorded before a rule required them.
	fn adr596_rows_with_diagnostics(strip: &[&str]) -> Vec<serde_json::Value> {
		adr596_rows(ADR596_BELOW_FLOOR, adr596_write_facts())
			.into_iter()
			.map(|mut row| {
				if let Some(invocation) =
					row.pointer_mut("/node_attribution/aggregate_invocations/0")
				{
					let mut facts = adr596_write_facts();
					for name in ["shape", "consistent", "inconsistency"] {
						facts.as_object_mut().unwrap().remove(name);
					}
					for name in strip {
						facts.as_object_mut().unwrap().remove(*name);
					}
					invocation["diagnostics"] =
						serde_json::json!([{"name": "build_write_facts", "value": facts}]);
				}
				row
			})
			.collect()
	}

	// ADR 596: the retained receipts re-run through the band's current rules, as retained and,
	// where the retained facts predate a rule, with the stream-end derivation named.
	#[test]
	fn adr596_receipt_revalidation_reruns_the_band_and_names_a_derivation() {
		let report = adr596_revalidate_receipts(&adr596_rows_with_diagnostics(&[])).unwrap();
		assert_eq!(report["all_facts_consistent_as_retained"], true, "{report}");
		assert_eq!(report["all_facts_consistent_with_derivation"], true);
		let pairs = report["pairs"].as_array().unwrap();
		assert_eq!(pairs.len(), ADR585_PAIRS);
		assert_eq!(pairs[0]["revalidated_facts_consistent"], true);
		assert_eq!(pairs[0]["differences_from_retained"], serde_json::json!([]));
		assert_eq!(pairs[0]["facts_predating_the_rules"], serde_json::json!([]));
		assert_eq!(
			pairs[0]["with_derived_limit_facts"],
			serde_json::Value::Null
		);
		assert_eq!(
			pairs[0]["revalidated_residual_micros"],
			1_180_000 - ADR596_BELOW_FLOOR.iter().sum::<u64>()
		);

		// Facts recorded before the discarded-batch rule: inconsistent as retained, named, and
		// consistent under the derivation the measured path implies.
		let report = adr596_revalidate_receipts(&adr596_rows_with_diagnostics(&[
			"batches_limit_discarded",
			"batches_admitted",
		]))
		.unwrap();
		assert_eq!(report["all_facts_consistent_as_retained"], false);
		assert_eq!(
			report["all_facts_consistent_with_derivation"], true,
			"{report}"
		);
		let pair = &report["pairs"][0];
		assert_eq!(pair["revalidated_facts_consistent"], false);
		assert!(
			pair["revalidated_inconsistency"]
				.as_str()
				.unwrap()
				.contains("fact batches_limit_discarded is missing")
		);
		assert_eq!(
			pair["facts_predating_the_rules"],
			serde_json::json!(["batches_limit_discarded", "batches_admitted"])
		);
		assert_eq!(
			pair["with_derived_limit_facts"]["build_write_facts_consistent"],
			true
		);
		assert_eq!(
			pair["with_derived_limit_facts"]["derived_fields"],
			serde_json::json!(["batches_limit_discarded", "batches_admitted"])
		);
		assert!(
			pair["differences_from_retained"]
				.as_array()
				.unwrap()
				.iter()
				.any(|d| d
					.as_str()
					.unwrap()
					.starts_with("build_write_facts_consistent"))
		);

		// Partially missing facts: only the missing field is derived, and a recorded value that
		// contradicts the stream end survives into the rules and fails them.
		let with_recorded = |kept: &str, value: u64, strip: &str| {
			let mut rows = adr596_rows_with_diagnostics(&[strip]);
			for row in &mut rows {
				if let Some(facts) =
					row.pointer_mut("/node_attribution/aggregate_invocations/0/diagnostics/0/value")
				{
					facts[kept] = serde_json::json!(value);
				}
			}
			rows
		};
		// A recorded discard of one with the admitted count missing: the derivation supplies
		// admitted = read, the recorded discard stays, and the counts disagree.
		let report = adr596_revalidate_receipts(&with_recorded(
			"batches_limit_discarded",
			1,
			"batches_admitted",
		))
		.unwrap();
		assert_eq!(
			report["all_facts_consistent_with_derivation"], false,
			"{report}"
		);
		let pair = &report["pairs"][0];
		assert_eq!(
			pair["facts_predating_the_rules"],
			serde_json::json!(["batches_admitted"])
		);
		assert_eq!(
			pair["with_derived_limit_facts"]["derived_fields"],
			serde_json::json!(["batches_admitted"])
		);
		assert_eq!(
			pair["with_derived_limit_facts"]["build_write_facts_consistent"],
			false
		);
		assert!(
			pair["with_derived_limit_facts"]["build_write_inconsistency"]
				.as_str()
				.unwrap()
				.contains("batches_admitted disagree with batches_read less discarded"),
			"{pair}"
		);
		// A recorded admitted count short of the read count with the discard missing: the
		// derivation supplies discarded = 0, the recorded 119 stays, and the counts disagree.
		let report = adr596_revalidate_receipts(&with_recorded(
			"batches_admitted",
			119,
			"batches_limit_discarded",
		))
		.unwrap();
		assert_eq!(
			report["all_facts_consistent_with_derivation"], false,
			"{report}"
		);
		let pair = &report["pairs"][0];
		assert_eq!(
			pair["facts_predating_the_rules"],
			serde_json::json!(["batches_limit_discarded"])
		);
		assert_eq!(
			pair["with_derived_limit_facts"]["derived_fields"],
			serde_json::json!(["batches_limit_discarded"])
		);
		assert!(
			pair["with_derived_limit_facts"]["build_write_inconsistency"]
				.as_str()
				.unwrap()
				.contains("batches_admitted disagree with batches_read less discarded"),
			"{pair}"
		);
		// A recorded admitted count equal to the read count with the discard missing: the one
		// field is derived and the pair is consistent.
		let report = adr596_revalidate_receipts(&with_recorded(
			"batches_admitted",
			120,
			"batches_limit_discarded",
		))
		.unwrap();
		assert_eq!(
			report["all_facts_consistent_with_derivation"], true,
			"{report}"
		);
		assert_eq!(
			report["pairs"][0]["with_derived_limit_facts"]["derived_fields"],
			serde_json::json!(["batches_limit_discarded"])
		);
	}

	// -----------------------------------------------------------------------------------------
	// ADR 597: the paired-executable evaluation, the retention rule on the verdict field, the
	// cutoff and the precedence on that field, and the twelve-row contract.
	// -----------------------------------------------------------------------------------------

	/// Twelve rows as the driver journals them: control warmup, treatment warmup, then pairs
	/// alternating control-treatment / treatment-control, with the given per-pair gains over a
	/// control that spans `control_spread` ms.
	fn adr597_rows(gains: [f64; 5], control_spread: f64) -> Vec<serde_json::Value> {
		let row =
			|arm: &str, warmup: bool, pair: Option<usize>, position: Option<usize>, ms: f64| {
				let mut row = serde_json::json!({
					"query": "q21",
					"status": "pass",
					"execute_ms": ms,
					"adr597_arm": arm,
					"adr597_phase": if warmup { "warmup" } else { "pairs" },
					"adr597_pair": pair,
					"adr597_pair_position": position,
					"diagnostics_disabled": true,
					"digest": ADR588_SEALED_Q21_DIGEST,
					"driver_launch_unix_ms": 0.0,
					"driver_return_unix_ms": 0.0,
				});
				if warmup {
					row["warmup"] = serde_json::json!(true);
				}
				row
			};
		let mut rows = vec![
			row("control", true, None, None, 9_100.0),
			row("treatment", true, None, None, 9_050.0),
		];
		let mut stamp = 1_000.0;
		for (pair, gain) in gains.into_iter().enumerate() {
			let control_ms = 9_000.0 + pair as f64 * control_spread / 4.0;
			let treatment_ms = control_ms - gain;
			let (first, second) = if pair % 2 == 0 {
				(("control", control_ms), ("treatment", treatment_ms))
			} else {
				(("treatment", treatment_ms), ("control", control_ms))
			};
			for (position, (arm, ms)) in [first, second].into_iter().enumerate() {
				rows.push(row(arm, false, Some(pair), Some(position), ms));
			}
		}
		for row in &mut rows {
			row["driver_launch_unix_ms"] = serde_json::json!(stamp);
			row["driver_return_unix_ms"] = serde_json::json!(stamp + 50.0);
			stamp += 100.0;
		}
		rows
	}

	fn adr597_pair_rows(gains: [f64; 5], control_spread: f64) -> Vec<serde_json::Value> {
		adr597_rows(gains, control_spread)
			.into_iter()
			.filter(|row| row.get("warmup") != Some(&serde_json::Value::Bool(true)))
			.collect()
	}

	// The retention rule is ADR 588's, fixed: retained only above 2 × the control range with
	// every pair strictly positive; a zero pair or a median under the bar is not established.
	#[test]
	fn adr597_the_retention_rule_is_adr588s_on_the_verdict_field() {
		// Control spread 40 ms → bar 80 ms; gains median 120: retained.
		let summary =
			evaluate_adr597_pairs(&adr597_pair_rows([100.0, 130.0, 120.0, 110.0, 140.0], 40.0))
				.unwrap();
		assert_eq!(
			summary["verdict"]["status"], "retained",
			"{}",
			summary["verdict"]
		);
		assert_eq!(summary["required_gain_ms"], 80.0);
		assert_eq!(summary["gain_median_ms"], 120.0);
		assert_eq!(summary["every_pair_positive"], true);
		assert_eq!(summary["control_execute_range_ms"], 40.0);
		assert_eq!(
			summary["off_execute_range_ms"], 40.0,
			"the control range under the lifecycle name"
		);
		assert_eq!(summary["pairs"][1]["execute_treatment_ms"], 9_010.0 - 130.0);
		// A zero pair: not established, whatever the median.
		let summary =
			evaluate_adr597_pairs(&adr597_pair_rows([100.0, 130.0, 0.0, 110.0, 140.0], 40.0))
				.unwrap();
		assert_eq!(summary["verdict"]["status"], "not_established");
		assert_eq!(summary["every_pair_positive"], false);
		// A median under the bar: not established.
		let summary =
			evaluate_adr597_pairs(&adr597_pair_rows([60.0, 70.0, 65.0, 62.0, 75.0], 40.0)).unwrap();
		assert_eq!(summary["verdict"]["status"], "not_established");
		assert_eq!(summary["every_pair_positive"], true);
		// A wrong alternating order is refused.
		let mut rows = adr597_pair_rows([100.0, 130.0, 120.0, 110.0, 140.0], 40.0);
		rows[0]["adr597_pair_position"] = serde_json::json!(1);
		rows[1]["adr597_pair_position"] = serde_json::json!(0);
		assert!(
			evaluate_adr597_pairs(&rows)
				.unwrap_err()
				.contains("alternating order")
		);
		// A warmup among the pair rows is refused.
		let mut rows = adr597_pair_rows([100.0, 130.0, 120.0, 110.0, 140.0], 40.0);
		rows[2]["warmup"] = serde_json::json!(true);
		assert!(evaluate_adr597_pairs(&rows).unwrap_err().contains("warmup"));

		// The cutoff on the verdict field: a control range over 4% closes on variability with
		// the rule's outcome kept beside it.
		let mut summary = evaluate_adr597_pairs(&adr597_pair_rows(
			[1_000.0, 1_100.0, 1_050.0, 1_020.0, 1_080.0],
			400.0,
		))
		.unwrap();
		assert_eq!(summary["verdict"]["status"], "retained");
		adr593_apply_variability_bound_on(&mut summary, "verdict").unwrap();
		assert_eq!(
			summary["verdict"]["status"], "closed_on_variability",
			"{}",
			summary["verdict"]
		);
		assert_eq!(
			summary["verdict"]["rule_outcome_withheld"]["status"],
			"retained"
		);
		assert_eq!(summary["variability"]["verdict"], "exceeded");
		// The precedence on the verdict field.
		let mut summary =
			evaluate_adr597_pairs(&adr597_pair_rows([100.0, 130.0, 120.0, 110.0, 140.0], 40.0))
				.unwrap();
		adr595_apply_launch_precedence_on(
			&mut summary,
			&Adr595Verdicts {
				separation: serde_json::json!({"verdict": "failed"}),
				window: serde_json::json!({"verdict": "verified"}),
			},
			"verdict",
		);
		assert_eq!(
			summary["verdict"]["status"],
			"withheld_launch_separation_failed"
		);
		assert_eq!(
			summary["verdict"]["rule_outcome_withheld"]["status"],
			"retained"
		);
	}

	// The twelve-row contract: arms and order, warmups, pairs and positions, every stamp.
	#[test]
	fn adr597_the_twelve_row_contract_refuses_each_departure() {
		let rows = adr597_rows([100.0; 5], 40.0);
		assert_eq!(
			lifecycle_window_from_rows(&rows, &adr597_row_contract()).unwrap(),
			(1_000.0, 2_150.0)
		);
		let contract = adr597_row_contract();
		assert!(
			lifecycle_window_from_rows(&rows[..11], &contract)
				.unwrap_err()
				.contains("requires 12 rows")
		);
		let mut swapped_warmups = rows.clone();
		swapped_warmups.swap(0, 1);
		assert!(
			lifecycle_window_from_rows(&swapped_warmups, &contract)
				.unwrap_err()
				.contains("carries arm")
		);
		let mut wrong_position = rows.clone();
		wrong_position[2]["adr597_pair_position"] = serde_json::json!(1);
		assert!(
			lifecycle_window_from_rows(&wrong_position, &contract)
				.unwrap_err()
				.contains("is not pair")
		);
		let mut warmup_late = rows.clone();
		warmup_late[5]["warmup"] = serde_json::json!(true);
		assert!(
			lifecycle_window_from_rows(&warmup_late, &contract)
				.unwrap_err()
				.contains("is a warmup where")
		);
		let mut missing_first = rows.clone();
		missing_first[0]
			.as_object_mut()
			.unwrap()
			.remove("driver_launch_unix_ms");
		assert!(
			lifecycle_window_from_rows(&missing_first, &contract)
				.unwrap_err()
				.contains("row 0 lacks")
		);
		let mut missing_last = rows.clone();
		missing_last[11]["driver_return_unix_ms"] = serde_json::Value::Null;
		assert!(
			lifecycle_window_from_rows(&missing_last, &contract)
				.unwrap_err()
				.contains("row 11 lacks")
		);
	}

	// -----------------------------------------------------------------------------------------
	// ADR 598: the launcher's stamps, the refusal file, and the probe's derivation and decision.
	// -----------------------------------------------------------------------------------------

	fn adr598_synthetic_reading(nominal_s: i64, temp: Option<i64>, late: bool) -> Adr598Reading {
		Adr598Reading {
			nominal_s,
			actual_offset_ms: nominal_s as f64 * 1000.0 + if late { 800.0 } else { 20.0 },
			package_temp_millic: temp,
			late,
			missing: temp.is_none(),
		}
	}

	/// A trial with a flat baseline, a gate reading, and ten scheduled temperatures.
	fn adr598_synthetic_trial(
		kind: &str,
		index: usize,
		baseline_millic: i64,
		gate_millic: i64,
		schedule_millic: [i64; 10],
	) -> Adr598Trial {
		Adr598Trial {
			kind: kind.to_string(),
			index,
			baseline: (0..10)
				.map(|tick| adr598_synthetic_reading(tick - 10, Some(baseline_millic), false))
				.collect(),
			work_kind: if kind == "no_hash" { "sleep" } else { "hash" }.to_string(),
			work_mask: "0-31".to_string(),
			work_micros: 60_000,
			pre_gate_micros: 30_000,
			at_gate_delay: Adr598Reading {
				nominal_s: 0,
				actual_offset_ms: 31.0,
				package_temp_millic: Some(gate_millic),
				late: false,
				missing: false,
			},
			schedule: ADR598_SCHEDULE_S
				.iter()
				.zip(schedule_millic)
				.map(|(nominal, temp)| adr598_synthetic_reading(*nominal as i64, Some(temp), false))
				.collect(),
			started_unix_ms: 1.0,
		}
	}

	/// A hash trial that peaks 4 °C over a 42 °C baseline at +1 s and recovers by +10 s, and a
	/// no-hash trial that stays flat.
	fn adr598_hash_trial(index: usize) -> Adr598Trial {
		adr598_synthetic_trial(
			"hash",
			index,
			42_000,
			45_000,
			[
				45_000, 46_000, 45_000, 44_000, 43_000, 42_000, 42_000, 42_000, 42_000, 42_000,
			],
		)
	}

	fn adr598_no_hash_trial(index: usize) -> Adr598Trial {
		adr598_synthetic_trial("no_hash", index, 42_000, 42_000, [42_000; 10])
	}

	fn adr598_pairs(count: usize) -> Vec<serde_json::Value> {
		(0..count)
			.map(|pair| {
				adr598_pair(
					pair,
					&adr598_hash_trial(pair * 2),
					&adr598_no_hash_trial(pair * 2 + 1),
				)
			})
			.collect()
	}

	#[test]
	fn adr598_the_launcher_verification_is_recorded_absent_or_malformed() {
		let recorded = adr598_launcher_verification(&[
			Some("99336904".to_string()),
			Some("61234".to_string()),
			Some("1789588860000.500".to_string()),
			Some("0-31".to_string()),
		]);
		assert_eq!(recorded["state"], "recorded");
		assert_eq!(recorded["verify_bytes"], 99_336_904);
		assert_eq!(recorded["launcher_cpu_mask"], "0-31");
		assert_eq!(
			adr598_hash_end_to_gate_ms(&recorded, 1789588860020.5),
			serde_json::json!(20.0)
		);
		let absent = adr598_launcher_verification(&[None, None, None, None]);
		assert_eq!(absent["state"], "absent");
		assert_eq!(
			adr598_hash_end_to_gate_ms(&absent, 1.0),
			serde_json::Value::Null
		);
		// Some present: malformed, never zero work; unparsable likewise; an empty mask likewise.
		for raw in [
			vec![Some("1".to_string()), None, None, None],
			vec![
				Some("x".to_string()),
				Some("1".to_string()),
				Some("1.0".to_string()),
				Some("0-3".to_string()),
			],
			vec![
				Some("1".to_string()),
				Some("1".to_string()),
				Some("1.0".to_string()),
				Some("".to_string()),
			],
		] {
			let malformed = adr598_launcher_verification(&raw);
			assert_eq!(malformed["state"], "malformed", "{raw:?}");
			assert!(malformed["raw"].is_object());
			assert!(malformed.get("verify_bytes").is_none());
		}
	}

	fn adr598_test_scope() -> Result<toron_tpch::host_readings::DriverScope, String> {
		Ok(toron_tpch::host_readings::DriverScope {
			cgroup: "/user.slice/user-1002.slice/user@1002.service/app.slice/adr598-test.service"
				.to_string(),
			unit: Some("adr598-test.service".to_string()),
			unix_session_id: 1,
			audit_session_id: None,
			unit_settings: None,
			unit_settings_unavailable: None,
		})
	}

	fn adr598_test_reading(
		package_temp_millic: Option<i64>,
	) -> toron_tpch::host_readings::HostReading {
		toron_tpch::host_readings::HostReading {
			unix_ms_first: 1789588860020.5,
			monotonic_ns_first: 1,
			unix_ms_last: 1789588860020.9,
			monotonic_ns_last: 2,
			pid: 1,
			tid: 1,
			package_temp_millic,
			cpu_freq_khz: None,
			ctxt_switches: None,
			elapsed_us: 400,
			missing: Vec::new(),
		}
	}

	fn adr598_gate_at_47() -> Adr593GateOutcome {
		adr593_judge_gate(adr598_test_reading(Some(47_000)), Some(0.02))
	}

	fn adr598_scope_refused() -> Result<toron_tpch::host_readings::DriverScope, String> {
		Err("driver scope is a login session's (session-1.scope)".to_string())
	}

	// ADR 598: a temperature refusal keeps the gate's reading, its load, and its distance from
	// the launcher's hash on the refusal file; a refusal before the gate says the gate was not
	// read, with the gate fields explicitly null.
	#[test]
	fn adr598_a_gate_refusal_keeps_the_gate_reading_and_an_earlier_refusal_says_it_was_not_read() {
		let dir = std::env::temp_dir().join(format!("adr598-gate-refusal-{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&dir);
		let args = vec!["--output-dir".to_string(), dir.display().to_string()];
		let probes = LifecycleHostProbes {
			scope: adr598_test_scope,
			launch: adr595_launch_from_environment,
			gate: adr598_gate_at_47,
			sleep: std::thread::sleep,
		};
		let error =
			run_lifecycle_decomposition_driver_with(&args, &ADR594_DRIVER, &probes).unwrap_err();
		assert!(error.contains("47000 millic is above 45000"), "{error}");
		let refusal: serde_json::Value =
			serde_json::from_str(&std::fs::read_to_string(dir.join("refusal.json")).unwrap())
				.unwrap();
		assert_eq!(refusal["refusal"], error);
		assert_eq!(refusal["gate_evidence"], "recorded");
		assert_eq!(
			refusal["readings"]["at_gate"]["package_temp_millic"],
			47_000
		);
		assert_eq!(refusal["readings"]["load_1m_at_gate"], 0.02);
		assert_eq!(refusal["start_gate"]["admitted"], false);
		assert_eq!(refusal["start_gate"]["refusal"], error);
		assert_eq!(refusal["start_gate"]["package_temp_millic"], 47_000);
		// Not launched through toron-bench run here: the launcher's verification is absent and
		// the distance is null, never zero.
		assert_eq!(refusal["launcher_verification"]["state"], "absent");
		assert_eq!(refusal["hash_end_to_gate_ms"], serde_json::Value::Null);
		std::fs::remove_dir_all(&dir).unwrap();

		let dir = std::env::temp_dir().join(format!("adr598-scope-refusal-{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&dir);
		let args = vec!["--output-dir".to_string(), dir.display().to_string()];
		let probes = LifecycleHostProbes {
			scope: adr598_scope_refused,
			launch: adr595_launch_from_environment,
			gate: adr598_gate_at_47,
			sleep: std::thread::sleep,
		};
		let error =
			run_lifecycle_decomposition_driver_with(&args, &ADR594_DRIVER, &probes).unwrap_err();
		assert!(error.contains("login session"), "{error}");
		let refusal: serde_json::Value =
			serde_json::from_str(&std::fs::read_to_string(dir.join("refusal.json")).unwrap())
				.unwrap();
		assert_eq!(
			refusal["gate_evidence"],
			"absent: refused before the gate was read"
		);
		assert_eq!(refusal["readings"]["at_gate"], serde_json::Value::Null);
		assert_eq!(
			refusal["readings"]["load_1m_at_gate"],
			serde_json::Value::Null
		);
		assert_eq!(refusal["start_gate"], serde_json::Value::Null);
		assert_eq!(refusal["hash_end_to_gate_ms"], serde_json::Value::Null);
		assert!(
			refusal["readings"]["at_entry"]["unix_ms_first"]
				.as_f64()
				.unwrap() > 0.0
		);
		std::fs::remove_dir_all(&dir).unwrap();

		// The distance from the hash, when the launcher's stamps are recorded.
		let recorded = adr598_launcher_verification(&[
			Some("99388424".to_string()),
			Some("45785".to_string()),
			Some("1789588860000.000".to_string()),
			Some("0-31".to_string()),
		]);
		let gate = adr598_gate_at_47().gate;
		assert_eq!(
			adr598_hash_end_to_gate_from_gate(&recorded, Some(&gate)),
			serde_json::json!(20.5)
		);
		assert_eq!(
			adr598_hash_end_to_gate_from_gate(&recorded, None),
			serde_json::Value::Null
		);
	}

	// ADR 598: the baseline's actual offsets come from the instants the readings were taken at;
	// a delayed read shows its delay, and is not reconstructed from the nominal schedule.
	#[test]
	fn adr598_baseline_offsets_come_from_the_actual_instants() {
		let origin = Instant::now();
		let second = std::time::Duration::from_secs(1);
		let taken = vec![
			origin,
			origin + second,
			origin + 2 * second + std::time::Duration::from_millis(750),
		];
		let work_end = origin + 10 * second;
		let offsets = adr598_baseline_offsets(&taken, work_end);
		assert!((offsets[0] + 10_000.0).abs() < 1e-6, "{offsets:?}");
		assert!((offsets[1] + 9_000.0).abs() < 1e-6, "{offsets:?}");
		assert!(
			(offsets[2] + 7_250.0).abs() < 1e-6,
			"the delayed read keeps its delay: {offsets:?}"
		);
		assert_eq!(adr598_offset_ms(work_end, origin), 10_000.0);
	}

	// ADR 598: an incomplete report is `running` or `failed`; a stop at the unit's runtime
	// limit is asserted only from the completion marker's result.
	#[test]
	fn adr598_the_disposition_is_read_from_the_state_and_the_completion_evidence() {
		let running = serde_json::json!({"state": "running", "complete": false});
		let failed =
			serde_json::json!({"state": "failed", "complete": false, "error": "hash: open failed"});
		let complete = serde_json::json!({"state": "complete", "complete": true});
		let timeout = "adr595 token=t result=timeout exit_code=killed exit_status=TERM unix_ms=1";
		let success = "adr595 token=t result=success exit_code=exited exit_status=0 unix_ms=1";
		assert_eq!(
			adr598_run_disposition(&running, Some(timeout)),
			"stopped_at_runtime_limit"
		);
		assert_eq!(
			adr598_run_disposition(&running, Some(success)),
			"incomplete_unknown"
		);
		assert_eq!(adr598_run_disposition(&running, None), "incomplete_unknown");
		assert_eq!(adr598_run_disposition(&failed, Some(timeout)), "failed");
		assert_eq!(adr598_run_disposition(&complete, None), "complete");
		// A report from before the state field: complete when it says so, else incomplete and
		// never runtime-limited on its own.
		let stateless = serde_json::json!({"complete": false});
		assert_eq!(
			adr598_run_disposition(&stateless, None),
			"incomplete_unknown"
		);
		assert_eq!(
			adr598_run_disposition(&serde_json::json!({"complete": true}), Some(timeout)),
			"complete"
		);
	}

	// -----------------------------------------------------------------------------------------
	// ADR 599: the declared gate procedure — the wait between the launch admission and the gate.
	// -----------------------------------------------------------------------------------------

	/// The order of the driver's pre-gate acts, recorded by injected probes.
	static ADR599_SEQUENCE: std::sync::Mutex<Vec<&'static str>> = std::sync::Mutex::new(Vec::new());

	fn adr599_scope_recording() -> Result<toron_tpch::host_readings::DriverScope, String> {
		ADR599_SEQUENCE.lock().unwrap().push("scope");
		adr598_test_scope()
	}

	fn adr599_sleep_recording(duration: std::time::Duration) {
		ADR599_SEQUENCE
			.lock()
			.unwrap()
			.push(if duration.as_secs() == 5 {
				"wait_5s"
			} else {
				"wait"
			});
		// A real but short sleep, so the measured elapsed is visibly the wait and nothing else.
		std::thread::sleep(std::time::Duration::from_millis(20));
	}

	fn adr599_launch_recording_admit(
		_label: &str,
		_scope: &toron_tpch::host_readings::DriverScope,
	) -> Result<Adr595Launch, String> {
		ADR599_SEQUENCE.lock().unwrap().push("launch_admission");
		Ok(adr595_launch())
	}

	fn adr599_launch_recording_refuse(
		_label: &str,
		_scope: &toron_tpch::host_readings::DriverScope,
	) -> Result<Adr595Launch, String> {
		ADR599_SEQUENCE.lock().unwrap().push("launch_admission");
		Err("ADR 599 launched on boot a but runs on boot b".to_string())
	}

	fn adr599_gate_recording_admit() -> Adr593GateOutcome {
		ADR599_SEQUENCE.lock().unwrap().push("gate");
		adr593_judge_gate(adr598_test_reading(Some(41_000)), Some(0.01))
	}

	fn adr599_gate_recording_refuse() -> Adr593GateOutcome {
		ADR599_SEQUENCE.lock().unwrap().push("gate");
		adr593_judge_gate(adr598_test_reading(Some(47_000)), Some(0.01))
	}

	fn adr599_refusal_in(dir: &Path) -> serde_json::Value {
		serde_json::from_str(&std::fs::read_to_string(dir.join("refusal.json")).unwrap()).unwrap()
	}

	#[test]
	fn adr599_the_flag_is_parsed_strictly_and_never_defaults_on_a_bad_value() {
		let args = |values: &[&str]| values.iter().map(|v| v.to_string()).collect::<Vec<_>>();
		assert_eq!(
			adr599_parse_gate_settle(&args(&["--mode", "strict"])).unwrap(),
			0
		);
		assert_eq!(
			adr599_parse_gate_settle(&args(&["--gate-settle-seconds", "5"])).unwrap(),
			5
		);
		assert_eq!(
			adr599_parse_gate_settle(&args(&["--gate-settle-seconds", "600"])).unwrap(),
			600
		);
		for (values, needle) in [
			(vec!["--gate-settle-seconds"], "missing its value"),
			(vec!["--gate-settle-seconds", "--mode"], "missing its value"),
			(vec!["--gate-settle-seconds", "five"], "not a whole number"),
			(vec!["--gate-settle-seconds", "5.0"], "not a whole number"),
			(vec!["--gate-settle-seconds", "-5"], "is negative"),
			(vec!["--gate-settle-seconds", "601"], "over the 600 s limit"),
			(
				vec![
					"--gate-settle-seconds",
					"99999999999999999999999999999999999999999",
				],
				"not a whole number",
			),
		] {
			let error = adr599_parse_gate_settle(&args(&values)).unwrap_err();
			assert!(error.contains(needle), "{values:?}: {error}");
		}
	}

	// ADR 599: launch admission, then the wait, then the gate — the gate's reading the first
	// act after the wait; the wait recorded as performed with its measured duration; the
	// default performing no wait; a bad value refused before the wait with the setting, the
	// wait not performed, and the gate evidence absent on the refusal file.
	#[test]
	fn adr599_the_wait_sits_between_the_launch_admission_and_the_gate_and_is_recorded() {
		ADR599_SEQUENCE.lock().unwrap().clear();
		let dir = std::env::temp_dir().join(format!("adr599-order-{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&dir);
		let probes = LifecycleHostProbes {
			scope: adr599_scope_recording,
			launch: adr599_launch_recording_admit,
			gate: adr599_gate_recording_refuse,
			sleep: adr599_sleep_recording,
		};
		// A refusing gate after a 5 s setting: the sequence is scope, wait, gate; the refusal
		// carries the performed wait beside the gate's reading.
		let args = vec![
			"--output-dir".to_string(),
			dir.display().to_string(),
			"--gate-settle-seconds".to_string(),
			"5".to_string(),
		];
		let error =
			run_lifecycle_decomposition_driver_with(&args, &ADR596_DRIVER, &probes).unwrap_err();
		assert!(error.contains("47000 millic"), "{error}");
		assert_eq!(
			*ADR599_SEQUENCE.lock().unwrap(),
			vec!["scope", "launch_admission", "wait_5s", "gate"],
			"the separated path: scope, the launch environment's admission, the wait, the gate"
		);
		let refusal = adr599_refusal_in(&dir);
		assert_eq!(refusal["gate_settle"]["requested_seconds"], 5);
		assert_eq!(refusal["gate_settle"]["wait_performed"], true);
		let elapsed = refusal["gate_settle"]["elapsed_ms"].as_f64().unwrap();
		assert!((20.0..1_000.0).contains(&elapsed), "{elapsed}");
		assert!(
			refusal["gate_settle"]["ended_monotonic_ns"]
				.as_u64()
				.unwrap() >= refusal["gate_settle"]["started_monotonic_ns"]
				.as_u64()
				.unwrap()
		);
		assert_eq!(refusal["gate_evidence"], "recorded");
		assert_eq!(
			refusal["readings"]["at_gate"]["package_temp_millic"],
			47_000
		);
		std::fs::remove_dir_all(&dir).unwrap();

		// The default: no wait is performed and recorded as none, the gate is read at once.
		ADR599_SEQUENCE.lock().unwrap().clear();
		let args = vec!["--output-dir".to_string(), dir.display().to_string()];
		let error =
			run_lifecycle_decomposition_driver_with(&args, &ADR596_DRIVER, &probes).unwrap_err();
		assert!(error.contains("47000 millic"), "{error}");
		assert_eq!(
			*ADR599_SEQUENCE.lock().unwrap(),
			vec!["scope", "launch_admission", "gate"]
		);
		let refusal = adr599_refusal_in(&dir);
		assert_eq!(refusal["gate_settle"]["requested_seconds"], 0);
		assert_eq!(refusal["gate_settle"]["wait_performed"], false);
		assert!(refusal["gate_settle"]["elapsed_ms"].as_f64().unwrap() < 20.0);
		std::fs::remove_dir_all(&dir).unwrap();

		// A failed launch admission: neither the wait nor the gate runs; the refusal records
		// the setting, the wait not performed, and the gate evidence absent.
		ADR599_SEQUENCE.lock().unwrap().clear();
		let probes = LifecycleHostProbes {
			scope: adr599_scope_recording,
			launch: adr599_launch_recording_refuse,
			gate: adr599_gate_recording_admit,
			sleep: adr599_sleep_recording,
		};
		let args = vec![
			"--output-dir".to_string(),
			dir.display().to_string(),
			"--gate-settle-seconds".to_string(),
			"5".to_string(),
		];
		let error =
			run_lifecycle_decomposition_driver_with(&args, &ADR596_DRIVER, &probes).unwrap_err();
		assert!(error.contains("runs on boot"), "{error}");
		assert_eq!(
			*ADR599_SEQUENCE.lock().unwrap(),
			vec!["scope", "launch_admission"],
			"no wait and no gate after a failed launch admission"
		);
		let refusal = adr599_refusal_in(&dir);
		assert_eq!(refusal["gate_settle"]["requested_seconds"], 5);
		assert_eq!(refusal["gate_settle"]["wait_performed"], false);
		assert_eq!(
			refusal["gate_evidence"],
			"absent: refused before the gate was read"
		);
		std::fs::remove_dir_all(&dir).unwrap();

		// A bad value: refused before the scope, the wait, or the gate; the refusal records
		// the raw setting, the wait not performed, and absent gate evidence.
		ADR599_SEQUENCE.lock().unwrap().clear();
		let args = vec![
			"--output-dir".to_string(),
			dir.display().to_string(),
			"--gate-settle-seconds".to_string(),
			"-5".to_string(),
		];
		let error =
			run_lifecycle_decomposition_driver_with(&args, &ADR596_DRIVER, &probes).unwrap_err();
		assert!(error.contains("is negative"), "{error}");
		assert!(ADR599_SEQUENCE.lock().unwrap().is_empty());
		let refusal = adr599_refusal_in(&dir);
		assert_eq!(refusal["gate_settle"]["requested_seconds"], "-5");
		assert_eq!(refusal["gate_settle"]["wait_performed"], false);
		assert_eq!(
			refusal["gate_evidence"],
			"absent: refused before the gate was read"
		);
		std::fs::remove_dir_all(&dir).unwrap();

		// A refusal before the wait (the scope): the setting recorded, the wait not performed.
		ADR599_SEQUENCE.lock().unwrap().clear();
		let probes = LifecycleHostProbes {
			scope: adr598_scope_refused,
			launch: adr599_launch_recording_admit,
			gate: adr599_gate_recording_admit,
			sleep: adr599_sleep_recording,
		};
		let args = vec![
			"--output-dir".to_string(),
			dir.display().to_string(),
			"--gate-settle-seconds".to_string(),
			"5".to_string(),
		];
		let error =
			run_lifecycle_decomposition_driver_with(&args, &ADR596_DRIVER, &probes).unwrap_err();
		assert!(error.contains("login session"), "{error}");
		assert!(ADR599_SEQUENCE.lock().unwrap().is_empty());
		let refusal = adr599_refusal_in(&dir);
		assert_eq!(refusal["gate_settle"]["requested_seconds"], 5);
		assert_eq!(refusal["gate_settle"]["wait_performed"], false);
		assert_eq!(
			refusal["gate_evidence"],
			"absent: refused before the gate was read"
		);
		std::fs::remove_dir_all(&dir).unwrap();
	}

	#[test]
	fn adr598_a_refusal_is_written_only_into_a_directory_the_driver_created() {
		let dir = std::env::temp_dir().join(format!("adr598-refusal-{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&dir);
		let args = vec![
			"--output-dir".to_string(),
			dir.display().to_string(),
			"--mode".to_string(),
			"strict".to_string(),
		];
		// A fresh directory: the driver creates it, refuses (under cargo test the scope is a
		// login session's, which ADR 593 refuses), and writes the refusal beside its readings.
		let error = run_lifecycle_decomposition_driver(&args, &ADR596_DRIVER).unwrap_err();
		let refusal: serde_json::Value =
			serde_json::from_str(&std::fs::read_to_string(dir.join("refusal.json")).unwrap())
				.unwrap();
		assert_eq!(refusal["refusal"], error);
		assert!(
			refusal["readings"]["at_entry"]["unix_ms_first"]
				.as_f64()
				.unwrap() > 0.0
		);
		assert_eq!(refusal["launcher_verification"]["state"], "absent");
		// The directory now exists: refused before anything is written, the file untouched.
		let before = std::fs::read_to_string(dir.join("refusal.json")).unwrap();
		let error = run_lifecycle_decomposition_driver(&args, &ADR596_DRIVER).unwrap_err();
		assert!(error.contains("fresh output directory"), "{error}");
		assert_eq!(
			std::fs::read_to_string(dir.join("refusal.json")).unwrap(),
			before
		);
		std::fs::remove_dir_all(&dir).unwrap();
	}

	#[test]
	fn adr598_a_trial_derives_its_peak_and_its_sustained_recovery_or_says_why_not() {
		let derived = adr598_derive(&adr598_hash_trial(0));
		assert!(derived.valid);
		assert_eq!(derived.baseline_median_millic, Some(42_000));
		assert_eq!(derived.peak_millic, Some(46_000));
		assert_eq!(derived.peak_offset_ms, Some(1_020.0));
		// After the peak at +1 s: +2 (45) out, +3 (44) out, +5 (43) in, +10, +20 in: the third
		// confirming reading is +20 s.
		assert_eq!(derived.recovery_ms, Some(20_020.0));
		// No recovery by +60 s: unresolved, the reason named.
		let hot = adr598_synthetic_trial("hash", 0, 42_000, 46_000, [46_000; 10]);
		let derived = adr598_derive(&hot);
		assert!(derived.valid);
		assert_eq!(derived.recovery_ms, None);
		assert!(derived.unresolved_reason.unwrap().contains("by +60 s"));
		// A late reading invalidates the trial; so does a missing one; so does a short baseline.
		let mut late = adr598_hash_trial(0);
		late.schedule[4] = adr598_synthetic_reading(5, Some(43_000), true);
		let derived = adr598_derive(&late);
		assert!(!derived.valid);
		assert!(derived.invalid_reason.unwrap().contains("+5 s is late"));
		let mut missing = adr598_hash_trial(0);
		missing.schedule[7] = adr598_synthetic_reading(30, None, false);
		let derived = adr598_derive(&missing);
		assert!(derived.invalid_reason.unwrap().contains("+30 s is missing"));
		let mut short = adr598_hash_trial(0);
		short.baseline.pop();
		assert!(
			adr598_derive(&short)
				.invalid_reason
				.unwrap()
				.contains("ten readings")
		);
		let mut no_gate = adr598_hash_trial(0);
		no_gate.at_gate_delay.package_temp_millic = None;
		no_gate.at_gate_delay.missing = true;
		assert!(
			adr598_derive(&no_gate)
				.invalid_reason
				.unwrap()
				.contains("at_gate_delay")
		);
	}

	#[test]
	fn adr598_a_pair_reports_the_baseline_adjusted_excess_beside_the_raw_temperatures() {
		// Hash trial starts 1 °C warmer: raw gate difference 4 °C, adjusted 3 °C.
		let hash = adr598_synthetic_trial(
			"hash",
			0,
			43_000,
			46_000,
			[
				46_000, 46_000, 45_000, 44_000, 43_000, 43_000, 43_000, 43_000, 43_000, 43_000,
			],
		);
		let no_hash = adr598_no_hash_trial(1);
		let pair = adr598_pair(0, &hash, &no_hash);
		assert_eq!(pair["valid"], true);
		assert_eq!(pair["excess_at_gate_millic"], 3_000);
		assert_eq!(pair["hash"]["at_gate_delay"]["package_temp_millic"], 46_000);
		assert_eq!(
			pair["no_hash"]["at_gate_delay"]["package_temp_millic"],
			42_000
		);
		assert_eq!(pair["excess_by_offset"][0]["hash_millic"], 46_000);
		assert_eq!(pair["excess_by_offset"][0]["adjusted_excess_millic"], 3_000);
		assert_eq!(pair["excess_by_offset"][9]["adjusted_excess_millic"], 0);
		// An invalid trial makes the pair invalid, reason named, excess null.
		let mut late = no_hash.clone();
		late.schedule[0] = adr598_synthetic_reading(0, Some(42_000), true);
		let pair = adr598_pair(0, &hash, &late);
		assert_eq!(pair["valid"], false);
		assert!(
			pair["invalid_reason"]
				.as_str()
				.unwrap()
				.contains("no_hash: scheduled reading +0 s is late")
		);
	}

	#[test]
	fn adr598_the_decision_needs_eight_valid_pairs_and_names_the_settle_or_none() {
		let decision = adr598_decide(&adr598_pairs(8), true);
		assert_eq!(decision["status"], "established", "{decision}");
		assert_eq!(decision["positive_pairs"], 8);
		assert_eq!(decision["median_excess_at_gate_millic"], 3_000);
		assert_eq!(decision["unresolved_recoveries"], 0);
		assert_eq!(decision["median_recovery_ms"], 20_020);
		assert_eq!(
			decision["settle_seconds"], 25,
			"20.02 s rounded up to the next 5 s"
		);
		// Seven valid pairs: undecided; a stopped run: undecided, whatever the pairs.
		assert_eq!(
			adr598_decide(&adr598_pairs(7), true)["status"],
			"undecided_incomplete_pairs"
		);
		assert_eq!(
			adr598_decide(&adr598_pairs(8), false)["status"],
			"undecided_incomplete"
		);
		assert_eq!(
			adr598_decide(&adr598_pairs(8), false)["settle_seconds"],
			serde_json::Value::Null
		);
		// Five positive pairs of eight: not established.
		let mut pairs = adr598_pairs(8);
		for pair in pairs.iter_mut().take(3) {
			pair["excess_at_gate_millic"] = serde_json::json!(-500);
		}
		let decision = adr598_decide(&pairs, true);
		assert_eq!(decision["status"], "not_established");
		assert_eq!(decision["positive_pairs"], 5);
		// Six positive but a median under 2 °C: not established.
		let mut pairs = adr598_pairs(8);
		for pair in pairs.iter_mut() {
			pair["excess_at_gate_millic"] = serde_json::json!(1_500);
		}
		pairs[0]["excess_at_gate_millic"] = serde_json::json!(-100);
		pairs[1]["excess_at_gate_millic"] = serde_json::json!(-100);
		let decision = adr598_decide(&pairs, true);
		assert_eq!(decision["status"], "not_established");
		assert_eq!(decision["positive_pairs"], 6);
		// An unresolved hash recovery: the median is unresolved, the resolved values listed,
		// and no settle even though established.
		let mut pairs = adr598_pairs(8);
		pairs[3]["hash"]["derived"]["recovery_ms"] = serde_json::Value::Null;
		let decision = adr598_decide(&pairs, true);
		assert_eq!(decision["status"], "established");
		assert_eq!(decision["unresolved_recoveries"], 1);
		assert_eq!(decision["median_recovery"], "unresolved");
		assert_eq!(decision["hash_recoveries_ms"].as_array().unwrap().len(), 8);
		assert_eq!(decision["settle_seconds"], serde_json::Value::Null);
		// A recovery past 30 s: established, but no settle.
		let mut pairs = adr598_pairs(8);
		for pair in pairs.iter_mut() {
			pair["hash"]["derived"]["recovery_ms"] = serde_json::json!(45_020.0);
		}
		let decision = adr598_decide(&pairs, true);
		assert_eq!(decision["status"], "established");
		assert_eq!(decision["settle_seconds"], serde_json::Value::Null);
		// Exactly 30 s rounds to 30 and is admitted.
		for pair in pairs.iter_mut() {
			pair["hash"]["derived"]["recovery_ms"] = serde_json::json!(26_000.0);
		}
		assert_eq!(adr598_decide(&pairs, true)["settle_seconds"], 30);
	}

	fn adr586_rows(semi: [u64; 4], anti: [u64; 4]) -> Vec<serde_json::Value> {
		(0..ADR585_PAIRS)
			.flat_map(|pair| {
				[
					adr586_fixture_row(pair, false, semi, anti),
					adr586_fixture_row(pair, true, semi, anti),
				]
			})
			.collect()
	}

	// The fixture's off arms run 10,000..10,400 ms, so the off range is 400 ms and the owner
	// floor is max(500, 800) = 800 ms; a lead must exceed 400 ms to resolve.
	const ADR586_FIXTURE_FLOOR: f64 = 800.0;

	#[test]
	fn adr586_phase0_tripwire_trips_only_beyond_ten_percent_on_either_side() {
		let reference = 13_958.6;
		for (factor, tripped) in [
			(1.09, false),
			(1.11, true),
			(0.91, false),
			(0.89, true),
			(1.0, false),
		] {
			let value = adr586_phase0_tripwire(reference * factor, reference);
			assert_eq!(value["tripped"], tripped, "factor {factor}");
			assert!((value["movement"].as_f64().unwrap() - (factor - 1.0)).abs() < 1e-9);
			assert_eq!(value["threshold"], ADR586_PHASE0_THRESHOLD);
		}
	}

	#[test]
	fn adr586_resolves_a_write_child_owner_and_prices_repartition_as_gross() {
		let rows = adr586_rows(
			[0, 100_000, 1_500_000, 700_000],
			[200_000, 100_000, 900_000, 500_000],
		);
		let summary = evaluate_adr586_pairs(&rows).expect("complete five-pair fixture");
		assert_eq!(summary["owner_floor_ms"], ADR586_FIXTURE_FLOOR);
		assert_eq!(summary["owner"]["status"], "resolved");
		assert_eq!(summary["owner"]["term"], "semi_write_repartition");
		assert_eq!(
			summary["owner_removable_pricing"]["status"],
			"gross_removable"
		);
		assert_eq!(summary["owner_removable_pricing"]["clears_floor"], true);
		let semi = summary["semi_write_terms_scaled_ms"].as_array().unwrap();
		assert_eq!(semi.len(), 5, "four children and the residual");
		assert_eq!(semi[0]["term"], "semi_write_repartition");
		let anti = summary["anti_write_terms_scaled_ms"].as_array().unwrap();
		assert_eq!(anti[0]["term"], "anti_write_repartition");
		assert_eq!(
			summary["adr585_thirteen_term_owner"]["term"],
			"runtime_prep_semi_write"
		);
		let band = &summary["adr586_pair_bands"][0];
		assert_eq!(band["semi_write_filter_passes"], 0);
		assert_eq!(
			band["semi_write_ops"],
			serde_json::json!(["project", "exchange"])
		);
		assert_eq!(band["semi_write_residual_micros"], 100_000);
		assert_eq!(band["semi_write_residual_complete"], true);
	}

	#[test]
	fn adr586_reports_a_tie_within_the_off_range() {
		let rows = adr586_rows(
			[0, 100_000, 1_200_000, 1_000_000],
			[200_000, 100_000, 900_000, 500_000],
		);
		let summary = evaluate_adr586_pairs(&rows).expect("tie fixture");
		assert_eq!(
			summary["owner"]["status"],
			"unresolved_tie_within_off_range"
		);
		assert_eq!(summary["owner"]["leading_term"], "semi_write_repartition");
		assert_eq!(summary["owner"]["runner_up"], "semi_write_insert");
		assert_eq!(summary["owner_removable_pricing"]["status"], "no_owner");
	}

	#[test]
	fn adr586_reports_below_floor_with_no_mechanism_when_no_child_clears() {
		let rows = adr586_rows(
			[0, 760_000, 750_000, 740_000],
			[200_000, 100_000, 900_000, 500_000],
		);
		let summary = evaluate_adr586_pairs(&rows).expect("below-floor fixture");
		assert_eq!(summary["owner"]["status"], "below_floor_no_mechanism");
		assert_eq!(summary["owner"]["leading_term"], "semi_write_key_program");
		assert_eq!(summary["owner_removable_pricing"]["status"], "no_owner");
		assert!(
			summary["semi_write_terms_scaled_ms"]
				.as_array()
				.unwrap()
				.iter()
				.all(|term| term["clears_floor"] == false)
		);
	}

	#[test]
	fn adr586_insert_owner_is_not_removable_and_other_owners_are_unpriced() {
		let rows = adr586_rows(
			[0, 100_000, 500_000, 1_700_000],
			[200_000, 100_000, 900_000, 500_000],
		);
		let summary = evaluate_adr586_pairs(&rows).expect("insert owner fixture");
		assert_eq!(summary["owner"]["term"], "semi_write_insert");
		assert_eq!(
			summary["owner_removable_pricing"]["status"],
			"not_removable"
		);
		let rows = adr586_rows(
			[0, 1_700_000, 500_000, 100_000],
			[200_000, 100_000, 900_000, 500_000],
		);
		let summary = evaluate_adr586_pairs(&rows).expect("key program owner fixture");
		assert_eq!(summary["owner"]["term"], "semi_write_key_program");
		assert_eq!(summary["owner_removable_pricing"]["status"], "unpriced");
	}

	#[test]
	fn adr586_an_incomplete_write_residual_is_reported_and_withholds_the_owner() {
		// 400 ms of write residual exceeds overhead (100 ms) + 5% of a 2.4 s write (120 ms).
		let mut rows = adr586_rows(
			[0, 100_000, 1_500_000, 700_000],
			[200_000, 100_000, 900_000, 500_000],
		);
		let receipt = rows[1]
			.pointer_mut("/node_attribution/aggregate_invocations/0/inclusive_lifecycle_receipt")
			.unwrap();
		receipt["semi_write_insert_micros"] = serde_json::json!(400_000);
		receipt["semi_write_residual_micros"] = serde_json::json!(400_000);
		let summary = evaluate_adr586_pairs(&rows).expect("incomplete samples remain reportable");
		assert_eq!(summary["all_write_residuals_complete"], false);
		assert_eq!(
			summary["adr586_pair_bands"][0]["semi_write_residual_complete"],
			false
		);
		assert_eq!(summary["owner"]["status"], "withheld_incomplete_profiles");
	}

	// The residual-leader branch is unreachable while the 5% bound holds, so it is pinned at the
	// rule itself rather than through a fixture.
	#[test]
	fn adr586_owner_rule_names_a_leading_residual_as_an_incomplete_instrument() {
		let medians = vec![
			("semi_write_residual".to_string(), 1_000.0),
			("semi_write_insert".to_string(), 900.0),
		];
		let owner = adr586_owner(&medians, 800.0, 400.0, 10_000.0, true, true);
		assert_eq!(owner["status"], "withheld_instrument_incomplete");
		assert_eq!(owner["leading_residual"], "semi_write_residual");
		let owner = adr586_owner(&medians, 800.0, 400.0, 10_000.0, false, true);
		assert_eq!(owner["status"], "withheld_perturbation_rejected");
	}

	#[test]
	fn adr586_refuses_a_receipt_missing_pass_counts_op_lists_or_its_algebra() {
		let semi = [0, 100_000, 1_500_000, 700_000];
		let anti = [200_000, 100_000, 900_000, 500_000];
		for (field, expected) in [
			(
				"semi_write_repartition_passes",
				"lacks integer semi_write_repartition_passes",
			),
			("anti_write_ops", "lacks anti_write op list"),
		] {
			let mut rows = adr586_rows(semi, anti);
			let receipt = rows[1]
				.pointer_mut(
					"/node_attribution/aggregate_invocations/0/inclusive_lifecycle_receipt",
				)
				.unwrap();
			receipt.as_object_mut().unwrap().remove(field);
			let error = evaluate_adr586_pairs(&rows).expect_err("missing field refused");
			assert!(error.contains(expected), "{error}");
		}
		let mut rows = adr586_rows(semi, anti);
		let receipt = rows[1]
			.pointer_mut("/node_attribution/aggregate_invocations/0/inclusive_lifecycle_receipt")
			.unwrap();
		receipt["semi_write_residual_micros"] = serde_json::json!(99);
		let error = evaluate_adr586_pairs(&rows).expect_err("algebra refused");
		assert!(
			error.contains("semi_write algebra does not reconcile"),
			"{error}"
		);
	}

	/// The engine source the line classes are pinned to. Test-only: the measured tree and the
	/// class table must agree, and this is what makes a shifted line a failing test rather than
	/// a silently misbinned profile.
	const ADR587_STREAMING_SOURCE: &str = include_str!("../../toron/src/exec/join/streaming.rs");

	#[test]
	fn adr587_line_classes_match_the_measured_tree() {
		let lines = ADR587_STREAMING_SOURCE.lines().collect::<Vec<_>>();
		let mut previous_end = 0u32;
		for range in ADR587_LINE_CLASSES {
			assert!(
				range.start > previous_end,
				"{:?} overlaps or is unsorted",
				range
			);
			assert!(range.end >= range.start, "{:?} is empty", range);
			let first = lines[(range.start - 1) as usize].trim();
			assert!(
				first.starts_with(range.anchor),
				"line {} is {first:?}, expected anchor {:?}",
				range.start,
				range.anchor
			);
			previous_end = range.end;
		}
		for (line, expected) in ADR587_COPY_LINES.iter().zip([
			"entries.extend_from_slice(&self.entries);",
			"multi.resize(multi_words, 0);",
			"multi[..self.multi.len()].copy_from_slice(&self.multi);",
			"buckets.resize(bucket_count, EMPTY_NEQ_EXISTENCE_SUMMARY_BUCKET);",
		]) {
			assert_eq!(
				lines[(*line - 1) as usize].trim(),
				expected,
				"copy line {line}"
			);
		}
		// The rehash range is exactly the loop that re-walks every entry, and the growth ranges
		// end where the function does: the next non-blank line opens the lookup.
		let rehash = ADR587_LINE_CLASSES
			.iter()
			.find(|range| range.class == "rehash")
			.unwrap();
		assert_eq!(lines[(rehash.end - 1) as usize].trim(), "}");
		assert!(lines[(rehash.end - 2) as usize].trim().ends_with("})?;"));
		let last_growth = ADR587_LINE_CLASSES
			.iter()
			.filter(|range| range.class == "growth_other")
			.last()
			.unwrap();
		let after = lines[last_growth.end as usize..]
			.iter()
			.find(|line| !line.trim().is_empty())
			.unwrap()
			.trim();
		assert!(after.starts_with("fn lookup_entry_index("), "{after:?}");
	}

	#[test]
	fn adr587_perf_script_parser_reads_two_line_samples_in_ns_form() {
		let text = "\
 707479/707479  187967.804049874:     100000      5b0e59d75732 toron::exec::join::streaming::NeqExistenceSummary::find_bucket_in
  /home/x/streaming.rs:2055
 707479/707480  187967.804149627:      99000      72e98caa8a0f [unknown]
  [vdso][a0f]
 707479/707479  187967.804249544:     100000      5b0e59d75767 write_batch+0x2f44 (inlined)
  streaming.rs:1800
";
		let samples = adr587_parse_perf_script(text).expect("parse");
		assert_eq!(samples.len(), 3);
		assert_eq!(samples[0].pid, 707479);
		assert_eq!(samples[0].tid, 707479);
		assert_eq!(samples[0].time_ns, 187_967_804_049_874);
		assert_eq!(samples[0].period, 100_000);
		assert_eq!(samples[0].source_file, "streaming.rs");
		assert_eq!(samples[0].line, Some(2055));
		assert!(samples[0].symbol.ends_with("find_bucket_in"));
		assert_eq!(samples[1].tid, 707480);
		assert_eq!(samples[1].line, None);
		assert_eq!(samples[2].line, Some(1800));
		let error =
			adr587_parse_perf_script(" 1/1  187967.804049:  100000  5b0e59d75732 sym\n  a.rs:1\n")
				.expect_err("microsecond time refused");
		assert!(error.contains("--ns"), "{error}");
	}

	#[test]
	fn adr587_perf_header_parser_requires_both_counts() {
		let header = adr587_parse_perf_header(
			"# Total Lost Samples: 0\n#\n# Samples: 34K of event 'task-clock:u'\n",
		)
		.unwrap();
		assert_eq!(header.samples, 34_000);
		assert_eq!(header.lost_samples, 0);
		let header =
			adr587_parse_perf_header("# Total Lost Samples: 12\n# Samples: 512  of event 'x'\n")
				.unwrap();
		assert_eq!((header.samples, header.lost_samples), (512, 12));
		assert!(adr587_parse_perf_header("# Samples: 3 of event\n").is_err());
	}

	fn adr587_sample(tid: u64, time_ns: u64, symbol: &str, line: Option<u32>) -> Adr587Sample {
		Adr587Sample {
			pid: 42,
			tid,
			time_ns,
			period: 100,
			symbol: symbol.to_string(),
			source_file: "streaming.rs".to_string(),
			line,
		}
	}

	fn adr587_test_windows() -> Adr587WindowSet {
		Adr587WindowSet {
			summary: "semi",
			pid: 42,
			rows: vec![
				Adr587Window {
					start_ns: 1_000,
					end_ns: 2_000,
					tid: 7,
				},
				Adr587Window {
					start_ns: 3_000,
					end_ns: 4_000,
					tid: 7,
				},
			],
			growth: vec![Adr587Window {
				start_ns: 1_200,
				end_ns: 1_400,
				tid: 7,
			}],
		}
	}

	// Half-open edges, thread binding, nested precedence, class placement, and exact identities.
	#[test]
	fn adr587_binner_places_samples_by_thread_window_and_class() {
		let windows = adr587_test_windows();
		let samples = vec![
			adr587_sample(7, 1_000, "poll", Some(2_132)), // start edge: inside, shared
			adr587_sample(7, 2_000, "poll", Some(2_132)), // end edge: outside
			adr587_sample(9, 1_500, "poll", Some(2_132)), // other thread: outside every window
			adr587_sample(7, 1_250, "poll", Some(2_132)), // inside growth: growth, not shared
			adr587_sample(7, 1_300, "__memmove_avx_unaligned_erms", None), // growth copy
			adr587_sample(7, 1_350, "NeqExistenceSummary::find_bucket_in", Some(2_212)), // rehash
			adr587_sample(7, 1_500, "NeqExistenceSummary::find_bucket_in", Some(2_212)), // rows: probe
			adr587_sample(7, 1_600, "poll", Some(2_212)), // append lines: probe
			adr587_sample(7, 1_700, "poll", Some(2_262)), // matches(): unclassified line: other
			adr587_sample(7, 1_800, "poll", None),        // unresolved line: other
			adr587_sample(7, 3_500, "poll", Some(2_244)), // observe_value: shared
			adr587_sample(7, 2_500, "poll", Some(2_132)), // between windows: on-thread outside
		];
		// The fixture lines must sit in the classes the comments claim, under the current table.
		let class_of = |line: u32| {
			ADR587_LINE_CLASSES
				.iter()
				.find(|range| line >= range.start && line <= range.end)
				.map(|range| range.class)
		};
		assert_eq!(class_of(2_132), Some("shared"));
		assert_eq!(class_of(2_212), Some("hash_and_probe"));
		assert_eq!(class_of(2_244), Some("shared"));
		assert_eq!(class_of(2_262), None, "matches() is unclassified");
		assert_eq!(class_of(2_212), Some("hash_and_probe"));
		let bin = adr587_bin_summary(&samples, &windows);
		assert_eq!(bin["thread_present"], true);
		assert_eq!(bin["tids"], serde_json::json!([7]));
		assert_eq!(bin["rows_inclusive"]["samples"], 9);
		assert_eq!(bin["growth"]["samples"], 3);
		assert_eq!(bin["classes"]["shared"]["samples"], 2);
		assert_eq!(bin["classes"]["hash_and_probe"]["samples"], 2);
		assert_eq!(bin["classes"]["other"]["samples"], 2);
		assert_eq!(bin["classes"]["copy"]["samples"], 1);
		assert_eq!(bin["classes"]["rehash"]["samples"], 1);
		assert_eq!(bin["classes"]["growth_other"]["samples"], 1);
		assert_eq!(
			bin["on_thread_outside_windows"]["samples"], 2,
			"end edge and the gap"
		);
		assert_eq!(bin["other_threads"]["samples"], 1);
		// rows_inclusive = rows_excluding_growth + growth, exactly, in periods.
		let rows = bin["rows_inclusive"]["period_ns"].as_u64().unwrap();
		let growth = bin["growth"]["period_ns"].as_u64().unwrap();
		let excluding: u64 = ADR587_ROWS_CLASSES
			.iter()
			.map(|c| bin["classes"][*c]["period_ns"].as_u64().unwrap())
			.sum();
		let growth_classes: u64 = ADR587_GROWTH_CLASSES
			.iter()
			.map(|c| bin["classes"][*c]["period_ns"].as_u64().unwrap())
			.sum();
		assert_eq!(rows, excluding + growth);
		assert_eq!(growth, growth_classes);
	}

	#[test]
	fn adr587_rows_windows_decoder_matches_the_engine_encoding() {
		let windows =
			adr587_decode_rows_windows("7@1000:0+1000;0+500;6500+1|8@10000:0+2000;10+10").unwrap();
		assert_eq!(
			windows,
			vec![
				Adr587Window {
					start_ns: 1_000,
					end_ns: 2_000,
					tid: 7
				},
				Adr587Window {
					start_ns: 2_000,
					end_ns: 2_500,
					tid: 7
				},
				Adr587Window {
					start_ns: 9_000,
					end_ns: 9_001,
					tid: 7
				},
				Adr587Window {
					start_ns: 10_000,
					end_ns: 12_000,
					tid: 8
				},
				Adr587Window {
					start_ns: 12_010,
					end_ns: 12_020,
					tid: 8
				},
			]
		);
		assert!(adr587_decode_rows_windows("").unwrap().is_empty());
		assert!(adr587_decode_rows_windows("7@1000:0-5").is_err());
		let set = Adr587WindowSet::from_diagnostic(
			"semi",
			&serde_json::json!({"pid": 1, "rows_windows": "7@1000:0+10;0+10", "rows_window_count": 2, "growth_windows": [[1002, 1004, 7, 1, 1024]]}),
		)
		.unwrap();
		assert_eq!(set.rows.len(), 2);
		assert_eq!(set.growth[0].tid, 7);
		let error = Adr587WindowSet::from_diagnostic(
			"semi",
			&serde_json::json!({"pid": 1, "rows_windows": "7@1000:0+10", "rows_window_count": 2, "growth_windows": []}),
		)
		.unwrap_err();
		assert!(error.contains("disagrees"), "{error}");
	}

	// A summary whose windows span two threads needs both sampled; one absent is reported by id.
	#[test]
	fn adr587_binner_requires_every_window_thread_to_be_sampled() {
		let mut windows = adr587_test_windows();
		windows.rows.push(Adr587Window {
			start_ns: 5_000,
			end_ns: 6_000,
			tid: 8,
		});
		let samples = vec![
			adr587_sample(7, 1_500, "poll", Some(2_132)),
			adr587_sample(7, 3_500, "poll", Some(2_132)),
		];
		let bin = adr587_bin_summary(&samples, &windows);
		assert_eq!(bin["tids"], serde_json::json!([7, 8]));
		assert_eq!(bin["thread_present"], false);
		assert_eq!(bin["missing_tids"], serde_json::json!([8]));
		let samples = vec![
			adr587_sample(7, 1_500, "poll", Some(2_132)),
			adr587_sample(8, 5_500, "poll", Some(2_132)),
		];
		let bin = adr587_bin_summary(&samples, &windows);
		assert_eq!(bin["thread_present"], true);
		assert_eq!(bin["missing_tids"], serde_json::json!([]));
		assert_eq!(bin["rows_inclusive"]["samples"], 2);
	}

	// Empty bins and ties carry no ranking: declaration order is never a leader.
	#[test]
	fn adr587_ranking_is_unresolved_for_empty_bins_and_ties() {
		let order = |values: [u64; 3]| {
			let mut order = vec![
				("hash_and_probe".to_string(), values[0]),
				("shared".to_string(), values[1]),
				("other".to_string(), values[2]),
			];
			order.sort_by(|left, right| right.1.cmp(&left.1));
			order
		};
		let zero = vec![order([0, 0, 0]); 5];
		assert_eq!(
			adr587_ranking_value(&zero)["status"],
			"unresolved_no_samples"
		);
		assert!(adr587_ranking_value(&zero)["leading_class"].is_null());
		let mut tied = vec![order([500, 100, 20]); 5];
		tied[3] = order([500, 500, 20]);
		assert_eq!(
			adr587_ranking_value(&tied)["status"],
			"unresolved_tie_at_the_top"
		);
		let mut one_empty = vec![order([500, 100, 20]); 5];
		one_empty[4] = order([0, 0, 0]);
		assert_eq!(
			adr587_ranking_value(&one_empty)["status"],
			"unresolved_no_samples"
		);
		let consistent = vec![order([500, 100, 20]); 5];
		let value = adr587_ranking_value(&consistent);
		assert_eq!(value["status"], "consistent_observation");
		assert_eq!(value["leading_class"], "hash_and_probe");
		assert_eq!(
			value["period_ns_per_pair"][0],
			serde_json::json!([500, 100, 20])
		);
		assert_eq!(adr587_ranking_value(&[])["status"], "unresolved_no_pairs");
	}

	#[test]
	fn adr587_binner_reports_an_absent_thread() {
		let windows = adr587_test_windows();
		let samples = vec![adr587_sample(9, 1_500, "poll", Some(2_132))];
		let bin = adr587_bin_summary(&samples, &windows);
		assert_eq!(bin["thread_present"], false);
		assert_eq!(bin["rows_inclusive"]["samples"], 0);
	}

	/// An ADR 587 row: the ADR 586 fixture under 587's tags with the fifth band and profiler
	/// bins. `grow`/`excluding` are the semi insert's exact terms in micros; the residual is
	/// whatever the 1,700,000 µs insert child leaves. `rows_class_periods` are the semi's
	/// sampled periods for hash_and_probe, shared, other.
	fn adr587_fixture_row(
		pair: usize,
		on: bool,
		grow: u64,
		excluding: u64,
		rows_class_periods: [u64; 3],
	) -> serde_json::Value {
		let mut row = adr586_fixture_row(
			pair,
			on,
			[0, 0, 500_000, 1_700_000],
			[200_000, 0, 400_000, 1_100_000],
		);
		for (from, to) in [
			("adr586_pair", "adr587_pair"),
			("adr586_arm", "adr587_arm"),
			("adr586_pair_position", "adr587_pair_position"),
		] {
			let value = row[from].take();
			row[to] = value;
			row.as_object_mut().unwrap().remove(from);
		}
		if on {
			let receipt = row
				.pointer_mut(
					"/node_attribution/aggregate_invocations/0/inclusive_lifecycle_receipt",
				)
				.expect("fixture receipt");
			for (summary, insert, grow, excluding) in [
				("semi", 1_700_000u64, grow, excluding),
				("anti", 1_100_000u64, 100_000, 950_000),
			] {
				let rows = grow + excluding;
				receipt[format!("{summary}_insert_rows_micros")] = serde_json::json!(rows);
				receipt[format!("{summary}_insert_rows_windows")] = serde_json::json!(5_859);
				receipt[format!("{summary}_insert_grow_micros")] = serde_json::json!(grow);
				receipt[format!("{summary}_insert_grow_events")] = serde_json::json!(14);
				receipt[format!("{summary}_insert_rows_excluding_grow_micros")] =
					serde_json::json!(excluding);
				receipt[format!("{summary}_insert_residual_micros")] =
					serde_json::json!(insert - rows);
				receipt[format!("{summary}_insert_finish_micros")] = serde_json::json!(0);
				receipt[format!("{summary}_insert_total_micros")] = serde_json::json!(insert);
			}
			let mut summaries = serde_json::Map::new();
			for (summary, grow, periods) in [
				("semi", grow, rows_class_periods),
				("anti", 100_000u64, [500_000u64, 300_000, 50_000]),
			] {
				let growth_ns = grow * 1_000;
				let rows_ns = periods.iter().sum::<u64>() + growth_ns;
				summaries.insert(
					summary.to_string(),
					serde_json::json!({
						"summary": summary,
						"pid": 42,
						"tids": [7],
						"thread_present": true,
						"rows_inclusive": {"samples": rows_ns / 100_000, "period_ns": rows_ns},
						"growth": {"samples": growth_ns / 100_000, "period_ns": growth_ns},
						"classes": {
							"hash_and_probe": {"samples": periods[0] / 100_000, "period_ns": periods[0]},
							"shared": {"samples": periods[1] / 100_000, "period_ns": periods[1]},
							"other": {"samples": periods[2] / 100_000, "period_ns": periods[2]},
							"copy": {"samples": 1, "period_ns": growth_ns / 2},
							"rehash": {"samples": 1, "period_ns": growth_ns / 2},
							"growth_other": {"samples": 0, "period_ns": 0},
						},
						"on_thread_outside_windows": {"samples": 10, "period_ns": 1_000_000},
						"other_threads": {"samples": 0, "period_ns": 0},
						"estimated_coverage_ratio": 0.97,
						"unexplained_discrepancy_ns": 30_000,
					}),
				);
			}
			row["adr587_bins"] = serde_json::json!({
				"total_samples": 130_000,
				"lost_samples": 0,
				"parsed_samples": 130_000,
				"summaries": summaries,
			});
			// The windows diagnostics the walk denominators are read from.
			let counters = serde_json::json!({
				"cache_hits": 35_984_637u64, "lookups": 12_004_370u64, "lookup_found": 4_370,
				"appended": 12_000_000, "probe_steps": 17_864_353, "empty_table_lookups": 1,
				"growth_reprobes": 15, "grow_events": 15,
			});
			let census = serde_json::json!({
				"partitions": [
					{"partition": 0, "max": 5_999_000, "above_max": 3_000_000, "equal_max": 1_100, "below_max": 0, "batches": 5_859, "restarts": 0},
					{"partition": 1, "max": 5_999_500, "above_max": 3_000_000, "equal_max": 1_090, "below_max": 0, "batches": 5_859, "restarts": 0},
					{"partition": 2, "max": 5_999_900, "above_max": 3_000_000, "equal_max": 1_080, "below_max": 0, "batches": 5_859, "restarts": 0},
					{"partition": 3, "max": 5_999_990, "above_max": 3_000_000, "equal_max": 1_100, "below_max": 0, "batches": 5_859, "restarts": 0},
				],
				"boundary_straddles": 4_370, "found_at_straddle": 4_370, "found_elsewhere": 0,
				"consumed_batches": 23_436, "consumed": adr589_fixture_consumed(),
			});
			row["node_attribution"]["aggregate_invocations"][0]["diagnostics"] = serde_json::json!([
				{"name": "semi_insert_windows", "value": {"pid": 42, "tids": [7], "rows_windows": "", "rows_window_count": 23_436, "growth_windows": [], "counters": counters, "order_census": census}},
				{"name": "anti_insert_windows", "value": {"pid": 42, "tids": [7], "rows_windows": "", "rows_window_count": 23_436, "growth_windows": [], "counters": counters, "order_census": census}},
			]);
		}
		row
	}

	fn adr587_rows(grow: u64, excluding: u64, periods: [u64; 3]) -> Vec<serde_json::Value> {
		(0..ADR585_PAIRS)
			.flat_map(|pair| {
				[
					adr587_fixture_row(pair, false, grow, excluding, periods),
					adr587_fixture_row(pair, true, grow, excluding, periods),
				]
			})
			.collect()
	}

	// Fixture off arms run 10,000..10,400 ms: off range 400, floor 800, a lead must beat 400.
	#[test]
	fn adr587_growth_owner_is_priced_gross_and_the_row_ranking_is_an_observation() {
		let rows = adr587_rows(1_100_000, 550_000, [400_000_000, 100_000_000, 20_000_000]);
		let summary = evaluate_adr587_pairs(&rows).expect("complete fixture");
		assert_eq!(summary["owner_floor_ms"], 800.0);
		assert_eq!(summary["owner"]["status"], "resolved");
		assert_eq!(summary["owner"]["term"], "semi_insert_grow");
		assert_eq!(
			summary["owner_removable_pricing"]["status"],
			"gross_removable"
		);
		assert_eq!(
			summary["semi_rows_ranking"]["status"],
			"consistent_observation"
		);
		assert_eq!(
			summary["semi_rows_ranking"]["leading_class"],
			"hash_and_probe"
		);
		let classes = summary["semi_rows_classes"].as_array().unwrap();
		assert_eq!(classes[0]["class"], "hash_and_probe");
		assert!((classes[0]["share_all_pairs"].as_f64().unwrap() - 400.0 / 520.0).abs() < 1e-9);
		assert_eq!(
			summary["adr586_five_term_owner"]["term"],
			"semi_write_insert"
		);
		let band = &summary["adr587_pair_bands"][0];
		assert_eq!(band["semi_insert_residual_micros"], 50_000);
		assert_eq!(band["semi_insert_residual_complete"], true);
		assert_eq!(band["semi_insert_grow_events"], 14);
		assert!(
			(summary["semi_coverage"]["estimated_coverage_ratio_median"]
				.as_f64()
				.unwrap() - 0.97)
				.abs() < 1e-9
		);
	}

	#[test]
	fn adr587_row_loop_owner_is_not_removable() {
		let rows = adr587_rows(200_000, 1_450_000, [400_000_000, 100_000_000, 20_000_000]);
		let summary = evaluate_adr587_pairs(&rows).expect("row loop fixture");
		assert_eq!(summary["owner"]["term"], "semi_insert_rows_excluding_grow");
		assert_eq!(
			summary["owner_removable_pricing"]["status"],
			"not_removable"
		);
	}

	#[test]
	fn adr587_reports_ties_and_below_floor() {
		let rows = adr587_rows(850_000, 800_000, [400_000_000, 100_000_000, 20_000_000]);
		let summary = evaluate_adr587_pairs(&rows).expect("tie fixture");
		assert_eq!(
			summary["owner"]["status"],
			"unresolved_tie_within_off_range"
		);
		let rows = adr587_rows(780_000, 780_000, [400_000_000, 100_000_000, 20_000_000]);
		let summary = evaluate_adr587_pairs(&rows).expect("below-floor fixture");
		assert_eq!(summary["owner"]["status"], "below_floor_no_mechanism");
		assert_eq!(summary["owner_removable_pricing"]["status"], "no_owner");
	}

	#[test]
	fn adr587_ranking_is_unresolved_when_it_changes_or_other_leads() {
		let mut rows = adr587_rows(1_100_000, 550_000, [400_000_000, 100_000_000, 20_000_000]);
		let bins = rows[1]
			.pointer_mut("/adr587_bins/summaries/semi/classes")
			.unwrap();
		bins["shared"]["period_ns"] = serde_json::json!(450_000_000);
		let summary = evaluate_adr587_pairs(&rows).expect("changed ranking");
		assert_eq!(
			summary["semi_rows_ranking"]["status"],
			"unresolved_ranking_changes_between_pairs"
		);
		assert!(summary["semi_rows_ranking"]["leading_class"].is_null());
		// The owner rule is unaffected: it reads wall terms only.
		assert_eq!(summary["owner"]["term"], "semi_insert_grow");

		let rows = adr587_rows(1_100_000, 550_000, [100_000_000, 100_000_000, 400_000_000]);
		let summary = evaluate_adr587_pairs(&rows).expect("other leads");
		assert_eq!(
			summary["semi_rows_ranking"]["status"],
			"unresolved_other_leads"
		);
	}

	#[test]
	fn adr587_refuses_lost_samples_absent_threads_and_broken_algebra() {
		let mut rows = adr587_rows(1_100_000, 550_000, [400_000_000, 100_000_000, 20_000_000]);
		rows[1]["adr587_bins"]["lost_samples"] = serde_json::json!(3);
		assert!(
			evaluate_adr587_pairs(&rows)
				.unwrap_err()
				.contains("lost samples")
		);

		let mut rows = adr587_rows(1_100_000, 550_000, [400_000_000, 100_000_000, 20_000_000]);
		rows[1]["adr587_bins"]["summaries"]["anti"]["thread_present"] = serde_json::json!(false);
		rows[1]["adr587_bins"]["summaries"]["anti"]["missing_tids"] = serde_json::json!([9]);
		let error = evaluate_adr587_pairs(&rows).unwrap_err();
		assert!(
			error.contains("window thread is absent") && error.contains("[9]"),
			"{error}"
		);

		// Five all-zero profiles are unresolved, never a leader in declaration order.
		let mut rows = adr587_rows(1_100_000, 550_000, [0, 0, 0]);
		for row in rows.iter_mut().filter(|row| row["adr587_arm"] == "on") {
			row["adr587_bins"]["summaries"]["semi"]["rows_inclusive"]["period_ns"] =
				serde_json::json!(1_100_000_000u64);
		}
		let summary = evaluate_adr587_pairs(&rows).expect("empty bins remain reportable");
		assert_eq!(
			summary["semi_rows_ranking"]["status"],
			"unresolved_no_samples"
		);
		assert!(summary["semi_rows_ranking"]["leading_class"].is_null());

		let mut rows = adr587_rows(1_100_000, 550_000, [400_000_000, 100_000_000, 20_000_000]);
		let receipt = rows[1]
			.pointer_mut("/node_attribution/aggregate_invocations/0/inclusive_lifecycle_receipt")
			.unwrap();
		receipt["semi_insert_grow_micros"] = serde_json::json!(1);
		assert!(
			evaluate_adr587_pairs(&rows)
				.unwrap_err()
				.contains("insert algebra does not reconcile")
		);
	}

	#[test]
	fn adr587_an_incomplete_insert_residual_withholds_the_owner() {
		// A 300 ms insert residual exceeds overhead (100 ms) + 5% of a 1.7 s insert (85 ms).
		let rows = adr587_rows(1_100_000, 300_000, [400_000_000, 100_000_000, 20_000_000]);
		let summary = evaluate_adr587_pairs(&rows).expect("incomplete samples remain reportable");
		assert_eq!(summary["all_insert_residuals_complete"], false);
		assert_eq!(summary["owner"]["status"], "withheld_incomplete_profiles");
	}

	fn adr588_manifest(tree: &str, exe: &str, profile: &str, lock: &str) -> BuildManifest {
		use toron_tpch::evidence::{BuildRecipe, ExecutableIdentity};
		let recipe = BuildRecipe::new(
			profile,
			Vec::<String>::new(),
			"x86_64-unknown-linux-gnu",
			"rustc 1.95.0",
			"a".repeat(64),
			lock.repeat(64),
		)
		.unwrap();
		let identity = ExecutableIdentity {
			sha256: exe.repeat(64),
			staged_path: PathBuf::from(format!("/staged/{}", exe.repeat(64))),
		};
		BuildManifest::new(
			tree.repeat(40),
			tree.repeat(40),
			recipe,
			identity.clone(),
			identity,
			None,
			Vec::new(),
		)
		.unwrap()
	}

	#[test]
	fn adr588_manifest_gate_admits_only_the_intended_differences() {
		let control = adr588_manifest("1", "c", "release", "b");
		let treatment = adr588_manifest("2", "d", "release", "b");
		adr588_manifest_gate(&control, &treatment).expect("differ by tree and executable only");
		let moved_lock = adr588_manifest("2", "d", "release", "e");
		assert!(
			adr588_manifest_gate(&control, &moved_lock)
				.unwrap_err()
				.contains("recipes differ")
		);
		let profiling = adr588_manifest("1", "c", "profiling", "b");
		let profiling_treatment = adr588_manifest("2", "d", "profiling", "b");
		assert!(
			adr588_manifest_gate(&profiling, &profiling_treatment)
				.unwrap_err()
				.contains("requires the release recipe")
		);
		let same_tree = adr588_manifest("1", "d", "release", "b");
		assert!(
			adr588_manifest_gate(&control, &same_tree)
				.unwrap_err()
				.contains("same source tree")
		);
		let same_exe = adr588_manifest("2", "c", "release", "b");
		assert!(
			adr588_manifest_gate(&control, &same_exe)
				.unwrap_err()
				.contains("same executable")
		);
	}

	fn adr588_fixture_row(
		pair: usize,
		arm: &str,
		manifest: &BuildManifest,
		execute_ms: f64,
	) -> serde_json::Value {
		let position = match (pair % 2 == 0, arm) {
			(true, "control") | (false, "treatment") => 0,
			_ => 1,
		};
		serde_json::json!({
			"query": "q21",
			"engine": "toron",
			"status": "pass",
			"mode": "strict",
			"scale_factor": 8.0,
			"memory_per_task": 2_147_483_648u64,
			"spill_bytes_reserved": 2_147_483_648u64,
			"partition_work_concurrency_requested": 1,
			"diagnostics_disabled": true,
			"digest": ADR588_SEALED_Q21_DIGEST,
			"dataset_verification": "full_hash",
			"dataset_manifest_fingerprint": "fnv1a64:0fc5c06d19c5ed23",
			"data_source": "parquet",
			"execute_ms": execute_ms,
			"adr588_arm": arm,
			"adr588_pair": pair,
			"adr588_pair_position": position,
			"adr588_launched_manifest": format!("/manifests/{arm}.manifest"),
			"adr588_launched_executable_sha256": manifest.toron.sha256,
			"evidence_identity": {
				"executable_sha256": manifest.toron.sha256,
				"source_tree_id": manifest.source_tree_id,
				"recipe_sha256": manifest.recipe_digest,
			},
		})
	}

	fn adr588_expected(control: &BuildManifest, treatment: &BuildManifest) -> Adr588Expected {
		Adr588Expected {
			control: Adr588ArmExpectation {
				name: "control",
				manifest: control.clone(),
				manifest_path: "/manifests/control.manifest".to_string(),
			},
			treatment: Adr588ArmExpectation {
				name: "treatment",
				manifest: treatment.clone(),
				manifest_path: "/manifests/treatment.manifest".to_string(),
			},
		}
	}

	fn adr588_dataset() -> Adr588Dataset {
		Adr588Dataset {
			manifest_fingerprint: "fnv1a64:0fc5c06d19c5ed23".to_string(),
			data_source: "parquet".to_string(),
		}
	}

	/// Control at 13,000..13,400 (range 400, required gain 800); treatment = control - gain[i].
	fn adr588_rows(gains: [f64; 5]) -> (Vec<serde_json::Value>, Adr588Expected) {
		let control = adr588_manifest("1", "c", "release", "b");
		let treatment = adr588_manifest("2", "d", "release", "b");
		let rows = (0..ADR585_PAIRS)
			.flat_map(|pair| {
				let control_ms = 13_000.0 + pair as f64 * 100.0;
				[
					adr588_fixture_row(pair, "control", &control, control_ms),
					adr588_fixture_row(pair, "treatment", &treatment, control_ms - gains[pair]),
				]
			})
			.collect();
		(rows, adr588_expected(&control, &treatment))
	}

	#[test]
	fn adr588_retains_only_an_established_gain() {
		let (rows, expected) = adr588_rows([900.0, 950.0, 1_000.0, 880.0, 1_100.0]);
		let summary = evaluate_adr588_pairs(&rows, &expected, &adr588_dataset()).expect("fixture");
		assert_eq!(summary["control_execute_range_ms"], 400.0);
		assert_eq!(summary["required_gain_ms"], 800.0);
		assert_eq!(summary["verdict"]["status"], "retained");
		assert_eq!(summary["gain_median_ms"], 950.0);

		// A positive median with one negative pair fails.
		let (rows, expected) = adr588_rows([900.0, 950.0, 1_000.0, -5.0, 1_100.0]);
		let summary = evaluate_adr588_pairs(&rows, &expected, &adr588_dataset()).expect("fixture");
		assert_eq!(summary["verdict"]["status"], "not_established");
		assert_eq!(summary["verdict"]["every_pair_positive"], false);

		// A positive median with one zero pair fails exactly the same way.
		let (rows, expected) = adr588_rows([900.0, 950.0, 1_000.0, 0.0, 1_100.0]);
		let summary = evaluate_adr588_pairs(&rows, &expected, &adr588_dataset()).expect("fixture");
		assert_eq!(summary["verdict"]["status"], "not_established");
		assert_eq!(
			summary["verdict"]["reason"],
			"a pair has zero or negative gain"
		);

		// A median inside twice the control range fails even with every pair positive.
		let (rows, expected) = adr588_rows([700.0, 750.0, 790.0, 600.0, 810.0]);
		let summary = evaluate_adr588_pairs(&rows, &expected, &adr588_dataset()).expect("fixture");
		assert_eq!(summary["verdict"]["status"], "not_established");
		assert_eq!(summary["verdict"]["every_pair_positive"], true);
		assert_eq!(
			summary["verdict"]["reason"],
			"median gain does not exceed twice the control range"
		);
	}

	#[test]
	fn adr588_refuses_digest_identity_and_setting_mismatches() {
		let (mut rows, expected) = adr588_rows([900.0; 5]);
		rows[3]["digest"] = serde_json::json!("ArrowIpcDigest([0])");
		assert!(
			evaluate_adr588_pairs(&rows, &expected, &adr588_dataset())
				.unwrap_err()
				.contains("digest parity failed on a treatment row")
		);

		let (mut rows, expected) = adr588_rows([900.0; 5]);
		rows[2]["evidence_identity"]["executable_sha256"] = serde_json::json!("d".repeat(64));
		assert!(
			evaluate_adr588_pairs(&rows, &expected, &adr588_dataset())
				.unwrap_err()
				.contains("control row reports an identity that is not its manifest's")
		);

		let (mut rows, expected) = adr588_rows([900.0; 5]);
		rows[2]["adr588_launched_executable_sha256"] = serde_json::json!("d".repeat(64));
		assert!(
			evaluate_adr588_pairs(&rows, &expected, &adr588_dataset())
				.unwrap_err()
				.contains("other arm's executable")
		);

		let (mut rows, expected) = adr588_rows([900.0; 5]);
		rows[5]["memory_per_task"] = serde_json::json!(1_073_741_824u64);
		assert!(
			evaluate_adr588_pairs(&rows, &expected, &adr588_dataset())
				.unwrap_err()
				.contains("sealed coordinate")
		);

		let (mut rows, expected) = adr588_rows([900.0; 5]);
		rows[1]["diagnostics_disabled"] = serde_json::json!(false);
		assert!(
			evaluate_adr588_pairs(&rows, &expected, &adr588_dataset())
				.unwrap_err()
				.contains("diagnostics disabled")
		);

		let (rows, expected) = adr588_rows([900.0; 5]);
		let same = Adr588Expected {
			control: Adr588ArmExpectation {
				name: "control",
				manifest: expected.control.manifest.clone(),
				manifest_path: expected.control.manifest_path.clone(),
			},
			treatment: Adr588ArmExpectation {
				name: "treatment",
				manifest: expected.control.manifest.clone(),
				manifest_path: expected.treatment.manifest_path.clone(),
			},
		};
		assert!(
			evaluate_adr588_pairs(&rows, &same, &adr588_dataset())
				.unwrap_err()
				.contains("same source tree")
		);
	}

	// The one validator runs on every row, so a phase-0 or warmup row that fails parity or
	// identity is refused the moment it returns, not only when the pairs are evaluated.
	#[test]
	fn adr588_validator_refuses_any_row_on_parity_identity_launch_or_dataset() {
		let (rows, expected) = adr588_rows([900.0; 5]);
		let dataset = adr588_dataset();
		let good = rows[0].clone();
		adr588_validate_row(&good, &expected.control, &dataset).expect("a control row passes");
		let cases: Vec<(&str, Box<dyn Fn(&mut serde_json::Value)>, &str)> = vec![
			(
				"digest",
				Box::new(|row| row["digest"] = serde_json::json!("ArrowIpcDigest([0])")),
				"digest parity failed on a control row",
			),
			(
				"warmup row without a pair still fails parity",
				Box::new(|row| {
					row["digest"] = serde_json::json!("ArrowIpcDigest([1])");
					row.as_object_mut().unwrap().remove("adr588_pair");
					row["adr588_phase"] = serde_json::json!("pairs_warmup");
				}),
				"digest parity failed on a control row",
			),
			(
				"swapped manifest",
				Box::new(|row| {
					row["adr588_launched_manifest"] =
						serde_json::json!("/manifests/treatment.manifest")
				}),
				"launched with manifest",
			),
			(
				"swapped executable",
				Box::new(|row| {
					row["adr588_launched_executable_sha256"] = serde_json::json!("d".repeat(64))
				}),
				"other arm's executable",
			),
			(
				"identity",
				Box::new(|row| {
					row["evidence_identity"]["source_tree_id"] = serde_json::json!("9".repeat(40))
				}),
				"not its manifest's",
			),
			(
				"dataset fingerprint",
				Box::new(|row| {
					row["dataset_manifest_fingerprint"] = serde_json::json!("fnv1a64:deadbeef")
				}),
				"names dataset",
			),
			(
				"dataset verification",
				Box::new(|row| row["dataset_verification"] = serde_json::json!("skipped")),
				"fully verified dataset",
			),
			(
				"data source",
				Box::new(|row| row["data_source"] = serde_json::json!("memory")),
				"names data source",
			),
			(
				"diagnostics",
				Box::new(|row| row["diagnostics_disabled"] = serde_json::json!(false)),
				"diagnostics disabled",
			),
			(
				"coordinate",
				Box::new(|row| row["memory_per_task"] = serde_json::json!(1_073_741_824u64)),
				"sealed coordinate",
			),
		];
		for (label, mutate, expected_error) in cases {
			let mut row = good.clone();
			mutate(&mut row);
			let error = adr588_validate_row(&row, &expected.control, &dataset).expect_err(label);
			assert!(error.contains(expected_error), "{label}: {error}");
		}
		// A treatment row validated against the control expectation is refused on identity.
		let error = adr588_validate_row(&rows[1], &expected.control, &dataset).unwrap_err();
		assert!(error.contains("not its manifest's"), "{error}");
	}

	/// 23,436 consumed batches cycling partitions 0..3, 5,859 per partition, keys rising on
	/// each partition with no restarts within one: consistent with the fixture census.
	fn adr589_fixture_consumed() -> String {
		let mut text = String::new();
		let mut previous_last = [None::<i64>; 4];
		for batch in 0..23_436usize {
			// Source batch n spans keys [10n, 10n + 9]; each partition's slice of it opens at
			// 10n + p and ends at 10n + 5 + p, so partitions 1..3 open below partition 0's end.
			let partition = batch % 4;
			let source = (batch / 4) as i64;
			let first = source * 10 + partition as i64;
			let last = first + 5;
			if !text.is_empty() {
				text.push(';');
			}
			match previous_last[partition] {
				Some(previous) => text.push_str(&format!(
					"{partition}:{}:{}:",
					first - previous,
					last - first
				)),
				None => text.push_str(&format!("{partition}:={first}:{}:", last - first)),
			}
			previous_last[partition] = Some(last);
		}
		text
	}

	fn adr589_census_mut(row: &mut serde_json::Value, index: usize) -> &mut serde_json::Value {
		&mut row["node_attribution"]["aggregate_invocations"][0]["diagnostics"][index]["value"]
	}

	// The census evaluator refuses incomplete or inconsistent evidence: no partitions, a
	// partition dropped from the census while the consumed batches name it, batch totals that
	// disagree with the rows windows, classes that do not sum to the lookups, and found
	// attributions that do not sum to the found lookups.
	#[test]
	fn adr589_census_refuses_empty_dropped_and_inconsistent_partitions() {
		let good = adr587_fixture_row(0, true, 1_100_000, 550_000, [1, 1, 1]);
		let census = adr589_census_value(&good, "semi").expect("a consistent census");
		assert_eq!(census["partition_count"], 4);
		assert_eq!(
			census["consumed_batches_per_partition"],
			serde_json::json!([5_859, 5_859, 5_859, 5_859])
		);
		assert_eq!(
			census["global_restarts"], 17_577,
			"three of every four transitions"
		);

		let mut row = good.clone();
		adr589_census_mut(&mut row, 0)["order_census"]["partitions"] = serde_json::json!([]);
		assert!(
			adr589_census_value(&row, "semi")
				.unwrap_err()
				.contains("has no partitions")
		);

		let mut row = good.clone();
		adr589_census_mut(&mut row, 0)["order_census"]["partitions"]
			.as_array_mut()
			.unwrap()
			.pop();
		assert!(
			adr589_census_value(&row, "semi")
				.unwrap_err()
				.contains("partition 3 the census does not cover")
		);

		let mut row = good.clone();
		adr589_census_mut(&mut row, 0)["rows_window_count"] = serde_json::json!(23_435);
		assert!(
			adr589_census_value(&row, "semi")
				.unwrap_err()
				.contains("batch totals disagree")
		);

		let mut row = good.clone();
		adr589_census_mut(&mut row, 0)["order_census"]["partitions"][1]["above_max"] =
			serde_json::json!(2_999_999);
		assert!(
			adr589_census_value(&row, "semi")
				.unwrap_err()
				.contains("do not sum to lookups")
		);

		let mut row = good.clone();
		adr589_census_mut(&mut row, 0)["order_census"]["found_elsewhere"] = serde_json::json!(1);
		assert!(
			adr589_census_value(&row, "semi")
				.unwrap_err()
				.contains("do not sum to lookup_found")
		);

		let mut row = good.clone();
		adr589_census_mut(&mut row, 0)["order_census"]["partitions"][2]["batches"] =
			serde_json::json!(5_858);
		assert!(
			adr589_census_value(&row, "semi")
				.unwrap_err()
				.contains("partition 2 has 5858 census batches but 5859 consumed")
		);

		let mut row = good.clone();
		adr589_census_mut(&mut row, 0)["order_census"]["consumed"] =
			serde_json::json!("0:=1:0:;1:=2:0:");
		assert!(
			adr589_census_value(&row, "semi")
				.unwrap_err()
				.contains("decodes to 2 batches")
		);
	}

	// ADR 589's opening rule: opens only on zero below_max everywhere and zero found_elsewhere
	// on both summaries in every pair; one violation anywhere, or a missing census, closes it.
	#[test]
	fn adr589_opening_rule_opens_only_on_a_clean_census_everywhere() {
		let rows = adr587_rows(1_100_000, 550_000, [400_000_000, 100_000_000, 20_000_000]);
		let summary = evaluate_adr587_pairs(&rows).expect("fixture");
		assert_eq!(summary["adr589_opening_rule"]["phase_2"], "opens");
		assert_eq!(summary["adr589_opening_rule"]["censuses_checked"], 10);
		let census = &summary["adr587_pair_bands"][0]["semi_order_census"];
		assert_eq!(census["below_max"], 0);
		assert_eq!(census["found_at_straddle"], 4_370);
		assert_eq!(census["partition_count"], 4);

		let mut rows = adr587_rows(1_100_000, 550_000, [400_000_000, 100_000_000, 20_000_000]);
		{
			let partition = &mut rows[7]["node_attribution"]["aggregate_invocations"][0]["diagnostics"]
				[1]["value"]["order_census"]["partitions"][2];
			partition["below_max"] = serde_json::json!(1);
			partition["above_max"] = serde_json::json!(2_999_999);
		}
		let summary = evaluate_adr587_pairs(&rows).expect("fixture");
		assert_eq!(summary["adr589_opening_rule"]["phase_2"], "closed");
		let violations = summary["adr589_opening_rule"]["violations"]
			.as_array()
			.unwrap();
		assert_eq!(violations.len(), 1);
		assert_eq!(violations[0]["summary"], "anti");
		assert_eq!(violations[0]["reason"], "below_max non-zero");

		let mut rows = adr587_rows(1_100_000, 550_000, [400_000_000, 100_000_000, 20_000_000]);
		{
			let census = &mut rows[1]["node_attribution"]["aggregate_invocations"][0]["diagnostics"]
				[0]["value"]["order_census"];
			census["found_elsewhere"] = serde_json::json!(3);
			census["found_at_straddle"] = serde_json::json!(4_367);
		}
		let summary = evaluate_adr587_pairs(&rows).expect("fixture");
		assert_eq!(summary["adr589_opening_rule"]["phase_2"], "closed");
		assert_eq!(
			summary["adr589_opening_rule"]["violations"][0]["reason"],
			"found_elsewhere non-zero"
		);

		let mut rows = adr587_rows(1_100_000, 550_000, [400_000_000, 100_000_000, 20_000_000]);
		let value =
			rows[1]["node_attribution"]["aggregate_invocations"][0]["diagnostics"][0]["value"]
				.as_object_mut()
				.unwrap();
		value.remove("order_census");
		let error = evaluate_adr587_pairs(&rows).unwrap_err();
		assert!(error.contains("lacks the semi order census"), "{error}");
	}

	#[test]
	fn adr587_walk_denominators_are_both_stated_and_kept_apart() {
		let row = adr587_fixture_row(0, true, 1_100_000, 550_000, [1, 1, 1]);
		let walks = adr587_walks_value(&row, "semi").expect("walks");
		// 2 * 12,000,000 + 4,370 - 1 and 12,004,370 - 1 + 15: two ratios, never one.
		assert_eq!(walks["two_walk_denominator"], 24_004_369u64);
		assert_eq!(walks["single_walk_denominator"], 12_004_384u64);
		assert!((walks["probe_steps_per_two_walk"].as_f64().unwrap() - 0.744).abs() < 1e-3);
		assert!((walks["probe_steps_per_single_walk"].as_f64().unwrap() - 1.488).abs() < 1e-3);
		let summary = evaluate_adr587_pairs(&adr587_rows(
			1_100_000,
			550_000,
			[400_000_000, 100_000_000, 20_000_000],
		))
		.expect("fixture");
		assert_eq!(
			summary["adr587_pair_bands"][0]["semi_walks"]["growth_reprobes"],
			15
		);
	}

	#[test]
	fn adr585_owner_rule_uses_all_thirteen_terms_and_the_off_range() {
		let rows = (0..ADR585_PAIRS)
			.flat_map(|pair| {
				[
					adr585_fixture_row(pair, false),
					adr585_fixture_row(pair, true),
				]
			})
			.collect::<Vec<_>>();
		let summary = evaluate_adr585_pairs(&rows).expect("complete five-pair fixture");
		assert_eq!(summary["perturbation_gate"], "accepted");
		assert_eq!(summary["all_profiles_complete"], true);
		assert_eq!(summary["owner_floor_ms"], 800.0);
		// ADR 585's thirteen terms plus ADR 589's two finish terms, zero on this fixture.
		assert_eq!(
			summary["term_medians_scaled_ms"].as_array().unwrap().len(),
			15
		);
		assert_eq!(summary["owner"]["status"], "resolved");
		assert_eq!(summary["owner"]["term"], "runtime_prep_inner_1");
	}

	#[test]
	fn adr585_requires_exactly_one_invocation_bound_target_receipt() {
		let mut rows = (0..ADR585_PAIRS)
			.flat_map(|pair| {
				[
					adr585_fixture_row(pair, false),
					adr585_fixture_row(pair, true),
				]
			})
			.collect::<Vec<_>>();
		let duplicate = rows[1]["node_attribution"]["aggregate_invocations"][0].clone();
		rows[1]["node_attribution"]["aggregate_invocations"]
			.as_array_mut()
			.unwrap()
			.push(duplicate);
		assert!(
			evaluate_adr585_pairs(&rows)
				.expect_err("two target receipts are an instrument defect")
				.contains("exactly one")
		);
	}

	#[test]
	fn adr585_node_profile_paths_are_unique_per_on_arm() {
		let mut rows = (0..ADR585_PAIRS)
			.flat_map(|pair| {
				[
					adr585_fixture_row(pair, false),
					adr585_fixture_row(pair, true),
				]
			})
			.collect::<Vec<_>>();
		rows[3]["node_attribution_profile_receipt_path"] =
			rows[1]["node_attribution_profile_receipt_path"].clone();
		assert!(
			evaluate_adr585_pairs(&rows)
				.expect_err("profile reuse must be refused")
				.contains("reuses node-profile receipt path")
		);
	}

	#[test]
	fn adr585_shared_source_floor_uses_the_pairwise_minimum_not_minimum_of_medians() {
		let mut rows = (0..ADR585_PAIRS)
			.flat_map(|pair| {
				[
					adr585_fixture_row(pair, false),
					adr585_fixture_row(pair, true),
				]
			})
			.collect::<Vec<_>>();
		for pair in 0..ADR585_PAIRS {
			let on = &mut rows[pair * 2 + 1];
			let receipt = on
				.pointer_mut(
					"/node_attribution/aggregate_invocations/0/inclusive_lifecycle_receipt",
				)
				.unwrap();
			receipt["runtime_prep_semi_read_micros"] =
				serde_json::json!(if pair < 3 { 1_000_000 } else { 100_000 });
			receipt["runtime_prep_anti_read_micros"] =
				serde_json::json!(if pair >= 2 { 1_000_000 } else { 100_000 });
			reconcile_adr585_fixture(on);
		}
		let summary = evaluate_adr585_pairs(&rows).expect("reconciled alias fixture");
		let terms = summary["term_medians_scaled_ms"].as_array().unwrap();
		let median_for = |name: &str| {
			terms.iter().find(|term| term["term"] == name).unwrap()["median_scaled_ms"]
				.as_f64()
				.unwrap()
		};
		assert!(median_for("runtime_prep_semi_read") > 800.0);
		assert!(median_for("runtime_prep_anti_read") > 800.0);
		assert_eq!(summary["shared_source_candidate"], "closed_below_floor");
		assert!(
			summary["shared_source_removable_read_median_scaled_ms"]
				.as_f64()
				.unwrap() < 800.0
		);
	}

	#[test]
	fn adr585_incomplete_residual_is_reported_and_withholds_the_owner() {
		let mut rows = (0..ADR585_PAIRS)
			.flat_map(|pair| {
				[
					adr585_fixture_row(pair, false),
					adr585_fixture_row(pair, true),
				]
			})
			.collect::<Vec<_>>();
		let receipt = rows[1]
			.pointer_mut("/node_attribution/aggregate_invocations/0/inclusive_lifecycle_receipt")
			.unwrap();
		receipt["runtime_prep_residual_micros"] = serde_json::json!(1_000_000);
		receipt["runtime_prep_inclusive_micros"] = serde_json::json!(6_800_000);
		reconcile_adr585_fixture(&mut rows[1]);
		let summary = evaluate_adr585_pairs(&rows).expect("incomplete samples remain reportable");
		assert_eq!(summary["pairs"][0]["profile_complete"], false);
		assert_eq!(summary["owner"]["status"], "withheld_incomplete_profiles");
	}

	#[test]
	fn adr585_refuses_a_zero_summary_half() {
		let mut rows = (0..ADR585_PAIRS)
			.flat_map(|pair| {
				[
					adr585_fixture_row(pair, false),
					adr585_fixture_row(pair, true),
				]
			})
			.collect::<Vec<_>>();
		rows[1].pointer_mut(
			"/node_attribution/aggregate_invocations/0/inclusive_lifecycle_receipt/runtime_prep_semi_read_micros",
		).unwrap().clone_from(&serde_json::json!(0));
		reconcile_adr585_fixture(&mut rows[1]);
		assert!(
			evaluate_adr585_pairs(&rows)
				.expect_err("an unobserved summary half is not a complete instrument")
				.contains("non-zero")
		);
	}

	#[test]
	fn perf_engines_accept_rune_without_changing_all() {
		assert_eq!(
			parse_perf_engines("toron,rune,polars_sql,duckdb").unwrap(),
			vec![
				PerfEngine::Toron,
				PerfEngine::Rune,
				PerfEngine::PolarsSql,
				PerfEngine::DuckDb
			]
		);
		assert_eq!(
			parse_perf_engines("all").unwrap(),
			vec![PerfEngine::Toron, PerfEngine::DuckDb],
			"all stays all-query safe; rune and polars_sql are explicit probes"
		);
	}

	#[test]
	fn perf_summary_flags_instability_plan_change_and_debug_build() {
		let rows = vec![
			perf_row("q06", 100.0, "plan-a", false),
			perf_row("q06", 180.0, "plan-a", false),
			perf_row("q06", 220.0, "plan-a", false),
		];
		let baseline = serde_json::json!({
			"query": "q06",
			"execute_ms_median": 90.0,
			"execute_ms_spread_ratio": 0.01,
			"plan_fingerprint": "plan-b",
		});
		let mut baseline_rows = BTreeMap::new();
		baseline_rows.insert(perf_summary_key("toron", "q06"), baseline);

		let summary = summarize_perf_rows(&rows, 3, 1, &baseline_rows);
		assert_eq!(summary.len(), 1);
		let flags = summary[0]["flags"]
			.as_array()
			.expect("flags")
			.iter()
			.map(|flag| flag.as_str().expect("flag").to_string())
			.collect::<Vec<_>>();
		assert!(flags.contains(&"unstable".to_string()));
		assert!(flags.contains(&"plan_changed".to_string()));
		assert!(flags.contains(&"debug_build".to_string()));
		assert_eq!(summary[0]["execute_ms_median"], 180.0);
	}

	#[test]
	fn perf_summary_flags_same_plan_slowdown_beyond_noise_band() {
		let rows = vec![
			perf_row("q06", 130.0, "plan-a", true),
			perf_row("q06", 132.0, "plan-a", true),
			perf_row("q06", 134.0, "plan-a", true),
		];
		let baseline = serde_json::json!({
			"query": "q06",
			"execute_ms_median": 100.0,
			"execute_ms_spread_ratio": 0.01,
			"plan_fingerprint": "plan-a",
		});
		let mut baseline_rows = BTreeMap::new();
		baseline_rows.insert(perf_summary_key("toron", "q06"), baseline);

		let summary = summarize_perf_rows(&rows, 3, 0, &baseline_rows);
		let flags = summary[0]["flags"]
			.as_array()
			.expect("flags")
			.iter()
			.map(|flag| flag.as_str().expect("flag").to_string())
			.collect::<Vec<_>>();
		assert!(flags.contains(&"slower_than_baseline".to_string()));
		assert!(!flags.contains(&"plan_changed".to_string()));
	}

	#[test]
	fn adr447_perf_summary_retains_raw_per_run_samples() {
		let rows = (0..7)
			.map(|index| perf_row("q06", 100.0 + f64::from(index), "plan-a", true))
			.collect::<Vec<_>>();
		let summary = summarize_perf_rows(&rows, 7, 1, &BTreeMap::new());
		let samples = summary[0]["execute_ms_samples"]
			.as_array()
			.expect("raw samples");
		assert_eq!(samples.len(), 7);
		assert_eq!(samples[0]["run_index"], 0);
		assert_eq!(samples[6]["execute_ms"], 106.0);
		assert_eq!(summary[0]["execute_ms_min"], 100.0);
		assert_eq!(summary[0]["execute_ms_max"], 106.0);
		assert_eq!(summary[0]["spill_scope"], "whole_query_total");
	}

	#[test]
	fn adr447_estimator_detects_a_known_median_difference() {
		let shift =
			paired_median_shift(&[12.0, 11.5, 12.5, 11.8, 12.2, 12.1, 11.9, 12.4, 11.7, 12.3])
				.expect("estimator");
		assert!(shift.lower > 0.0 && shift.upper > 0.0);
		assert!((shift.point - 12.0).abs() < 0.5);
		assert!(material_effect(shift.point, 10.0));
	}

	#[test]
	fn adr447_estimator_rejects_a_null_difference() {
		let shift =
			paired_median_shift(&[-1.0, 1.0, -0.5, 0.5, -0.25, 0.25, -0.1, 0.1, -0.05, 0.05])
				.expect("estimator");
		assert!(shift.lower <= 0.0 && shift.upper >= 0.0);
	}

	#[test]
	fn adr447_estimator_separates_detectable_from_material() {
		let shift = paired_median_shift(&[2.0, 2.1, 1.9, 2.05, 1.95, 2.02, 1.98, 2.03, 1.97, 2.01])
			.expect("estimator");
		assert!(shift.lower > 0.0 && shift.upper > 0.0);
		assert!(!material_effect(shift.point, 100.0));
	}

	#[test]
	fn adr447_estimator_is_not_range_based() {
		let concentrated = paired_median_shift(&[1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 3.0])
			.expect("concentrated estimator");
		let dispersed = paired_median_shift(&[0.5, 0.6, 0.7, 0.8, 1.2, 1.3, 1.4, 1.5, 1.8, 2.5])
			.expect("dispersed estimator");
		assert_ne!(
			(concentrated.lower, concentrated.upper),
			(dispersed.lower, dispersed.upper)
		);
	}

	#[test]
	fn adr448_ratio_across_mismatched_scopes_is_refused() {
		let result = spill_ratio(
			ScopedSpill {
				bytes: 10,
				scope: SpillScope::GroupedRoot,
			},
			ScopedSpill {
				bytes: 100,
				scope: SpillScope::WholeQuery,
			},
		);
		assert!(result.is_err());
	}

	#[test]
	fn adr447_estimator_is_deterministic_and_rejects_short_samples() {
		let diffs = [12.0, 11.5, 12.5, 11.8, 12.2, 12.1, 11.9, 12.4, 11.7, 12.3];
		assert_eq!(paired_median_shift(&diffs), paired_median_shift(&diffs));
		assert!(paired_median_shift(&[1.0, 2.0, 3.0, 4.0, 5.0]).is_err());
	}

	#[test]
	fn adr447_perf_child_binary_defaults_and_honors_override() {
		let current = std::path::PathBuf::from("current-toron-tpch");
		assert_eq!(toron_binary_from_override(None, current.clone()), current);
		assert_eq!(
			toron_binary_from_override(Some("comparison-toron-tpch".into()), current),
			std::path::PathBuf::from("comparison-toron-tpch")
		);
	}

	#[test]
	fn perf_baseline_requires_release_passing_stable_rows() {
		let release_pass = perf_summary_row("pass", "release", &[]);
		assert!(perf_baseline_is_recordable(std::slice::from_ref(
			&release_pass
		)));
		assert!(!perf_baseline_is_recordable(&[perf_summary_row(
			"pass",
			"debug",
			&["debug_build"]
		)]));
		assert!(!perf_baseline_is_recordable(&[perf_summary_row(
			"fail",
			"release",
			&[]
		)]));
		assert!(!perf_baseline_is_recordable(&[perf_summary_row(
			"pass",
			"release",
			&["unstable"]
		)]));
	}

	#[test]
	fn perf_summary_keeps_engines_separate_for_same_query() {
		let rows = vec![
			perf_row_for_engine("toron", "q06", 100.0, "toron-plan", true),
			perf_row_for_engine("duckdb", "q06", 80.0, "duckdb-plan", true),
		];
		let summary = summarize_perf_rows(&rows, 1, 0, &BTreeMap::new());
		assert_eq!(summary.len(), 2);
		assert!(summary.iter().any(|row| {
			row["engine"] == "toron" && row["query"] == "q06" && row["execute_ms_median"] == 100.0
		}));
		assert!(summary.iter().any(|row| {
			row["engine"] == "duckdb" && row["query"] == "q06" && row["execute_ms_median"] == 80.0
		}));
	}

	/// A board must survive its worst cell. q11's threshold constant is scale-specific, so at any
	/// other scale it is out of scope — `not_attempted`, not a failure, and above all not a reason to
	/// discard every query already measured.
	#[test]
	fn a_scale_unsupported_query_is_out_of_scope_not_a_board_failure() {
		assert!(validate_query_scale("q11", 3.0).is_err());
		assert!(validate_query_scale("q11", 8.0).is_ok());
		assert!(validate_query_scale("q11", 10.0).is_ok());
		assert!(
			validate_query_scale("q06", 8.0).is_ok(),
			"only the scale-pinned query is constrained"
		);

		let stub = perf_stub_row(PerfStubRow {
			engine: "toron",
			query: "q11",
			mode: "strict",
			scale_factor: 3.0,
			status: Status::NotAttempted,
			reason: "unsupported_scale",
			message: Some("q11 requires an exactly supported scale".to_string()),
		});
		assert_eq!(stub["status"], "not_attempted");
		assert_eq!(stub["query"], "q11");

		// `not_attempted` keeps the board green; a cell that should have run and did not does not.
		let summary = summarize_perf_rows(&[stub], 1, 0, &BTreeMap::new());
		assert_eq!(summary[0]["status"], "not_attempted");

		let failed = perf_stub_row(PerfStubRow {
			engine: "toron",
			query: "q05",
			mode: "strict",
			scale_factor: 8.0,
			status: Status::Fail,
			reason: "perf_child_failed",
			message: Some("child died".to_string()),
		});
		let summary = summarize_perf_rows(&[failed], 1, 0, &BTreeMap::new());
		assert_eq!(
			summary[0]["status"], "fail",
			"a contained failure stays visible as a red cell"
		);
	}

	/// The artifacts are written once, at the end. The journal is what makes a run that dies partway
	/// through leave its measurements behind instead of nothing at all.
	#[test]
	fn measured_rows_are_durable_before_the_sweep_finishes() {
		let dir = std::env::temp_dir().join(format!("toron_tpch_journal_{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&dir);
		let journal = prepare_perf_sample_journal(&dir).expect("journal");
		let mut cell_rows = Vec::new();
		let journal = Some(journal);

		journal_and_collect(
			&journal,
			&mut cell_rows,
			perf_row("q06", 100.0, "plan-a", true),
		)
		.expect("journal the row");

		// Read from disk, not from `cell_rows`: the point is that nothing is held only in memory.
		let on_disk = std::fs::read_to_string(dir.join(PERF_SAMPLE_JOURNAL)).expect("read journal");
		let persisted: Vec<serde_json::Value> = on_disk
			.lines()
			.map(|line| serde_json::from_str(line).expect("journal line is JSON"))
			.collect();
		assert_eq!(persisted.len(), 1, "the row is on disk before the run ends");
		assert_eq!(persisted[0]["query"], "q06");

		// A fresh journal for the same directory starts clean rather than replaying a prior run.
		let reopened = prepare_perf_sample_journal(&dir).expect("reopen");
		assert!(reopened.rows.is_empty());
		assert!(
			std::fs::read_to_string(dir.join(PERF_SAMPLE_JOURNAL))
				.expect("read")
				.is_empty()
		);
		let _ = std::fs::remove_dir_all(&dir);
	}

	/// `latest_summary.jsonl` and the recorded baseline are what a board is read from, so the
	/// verification has to survive the reduction from raw rows — otherwise only `latest_raw.jsonl`
	/// is auditable and the artifact everyone actually reads is not.
	#[test]
	fn summary_carries_dataset_verification_when_measured_rows_agree() {
		for verification in ["full_hash", "manifest_only"] {
			let rows = vec![
				perf_row_with_verification(0, Some(verification)),
				perf_row_with_verification(1, Some(verification)),
			];
			let summary = summarize_perf_rows(&rows, 2, 0, &BTreeMap::new());
			assert_eq!(summary.len(), 1);
			assert_eq!(summary[0]["dataset_verification"], verification);
			assert!(!summary_flags(&summary[0]).contains(&"dataset_verification_changed"));
			assert!(
				perf_baseline_is_recordable(&summary),
				"{verification} agreement must stay recordable"
			);
		}
	}

	/// Agreement on *absent* is agreement: in-memory runs load no dataset, so a null cell is honest
	/// rather than a disagreement, and it must not block a baseline.
	#[test]
	fn summary_reports_null_when_no_row_claims_verification() {
		let rows = vec![
			perf_row_with_verification(0, None),
			perf_row_with_verification(1, None),
		];
		let summary = summarize_perf_rows(&rows, 2, 0, &BTreeMap::new());
		assert_eq!(summary[0]["dataset_verification"], serde_json::Value::Null);
		assert!(!summary_flags(&summary[0]).contains(&"dataset_verification_changed"));
		assert!(perf_baseline_is_recordable(&summary));
	}

	#[test]
	fn a_cell_whose_verification_moved_reports_null_flags_it_and_blocks_the_baseline() {
		// full_hash vs manifest_only: one median over runs that did not share a chain of custody.
		let mixed_modes = vec![
			perf_row_with_verification(0, Some("full_hash")),
			perf_row_with_verification(1, Some("manifest_only")),
		];
		let summary = summarize_perf_rows(&mixed_modes, 2, 0, &BTreeMap::new());
		assert_eq!(
			summary[0]["dataset_verification"],
			serde_json::Value::Null,
			"neither arm may be reported as if it covered the whole cell"
		);
		assert!(summary_flags(&summary[0]).contains(&"dataset_verification_changed"));
		assert!(
			!perf_baseline_is_recordable(&summary),
			"a baseline may not rest on a cell whose dataset identity moved"
		);

		// A row that claims nothing mixed with one that does is equally a disagreement.
		let mixed_presence = vec![
			perf_row_with_verification(0, Some("full_hash")),
			perf_row_with_verification(1, None),
		];
		let summary = summarize_perf_rows(&mixed_presence, 2, 0, &BTreeMap::new());
		assert_eq!(summary[0]["dataset_verification"], serde_json::Value::Null);
		assert!(summary_flags(&summary[0]).contains(&"dataset_verification_changed"));
		assert!(!perf_baseline_is_recordable(&summary));
	}

	#[test]
	fn duckdb_absent_rows_summarize_as_not_attempted() {
		let rows = vec![duckdb_not_attempted_row(
			"q06",
			"strict",
			1.0,
			0,
			false,
			"duckdb_absent",
			None,
		)];
		let summary = summarize_perf_rows(&rows, 1, 0, &BTreeMap::new());
		assert_eq!(summary.len(), 1);
		assert_eq!(summary[0]["engine"], "duckdb");
		assert_eq!(summary[0]["status"], "not_attempted");
	}

	#[test]
	fn polars_absent_rows_summarize_as_not_attempted() {
		let rows = vec![polars_not_attempted_row(
			"q03",
			"strict",
			1.0,
			0,
			false,
			"polars_absent",
			None,
		)];
		let summary = summarize_perf_rows(&rows, 1, 0, &BTreeMap::new());
		assert_eq!(summary.len(), 1);
		assert_eq!(summary[0]["engine"], "polars_sql");
		assert_eq!(summary[0]["status"], "not_attempted");
		assert_eq!(summary[0]["engine_version"], serde_json::Value::Null);
	}

	#[test]
	fn polars_unsupported_attempt_maps_to_existing_not_attempted_status() {
		let export = fake_polars_export();
		let attempt = polars_unsupported_attempt(
			&export,
			"polars-plan",
			0,
			false,
			"correlated subqueries are not supported",
			Some("0.54.4".to_string()),
		);
		let row = attempt.json_value();
		assert_eq!(row["engine"], "polars_sql");
		assert_eq!(row["status"], "not_attempted");
		assert_eq!(row["reason"], "polars_sql_unsupported_correlated_subquery");
		assert_eq!(row["engine_version"], "0.54.4");
		assert_eq!(row["duckdb_version"], serde_json::Value::Null);
		// ADR 591: every perf row carries the affinity coordinate; with nothing applied in this
		// test process the request is null and default, and the observed mask is still read.
		assert_eq!(row["cpu_affinity_requested"], serde_json::Value::Null);
		assert_eq!(row["cpu_affinity_requested_provenance"], "default");
		assert!(
			row["cpu_affinity_observed"]
				.as_str()
				.is_some_and(|list| { toron_tpch::affinity::parse_cpulist(list).is_ok() })
		);

		let attempt = polars_unsupported_attempt(
			&export,
			"polars-plan",
			0,
			false,
			"query would exceed the Polars 2^32 index limit",
			Some("0.54.4".to_string()),
		);
		assert_eq!(
			attempt.json_value()["reason"],
			"polars_sql_unsupported_index_limit"
		);
		assert_eq!(
			classify_polars_error(
				"panicked at /home/me/.cargo/registry/src/index.crates.io/polars/src/lib.rs:1: unrelated"
			),
			"other"
		);
		assert_eq!(
			classify_polars_error("polars collect panic: outer join payload exploded"),
			"other"
		);
		assert_eq!(
			classify_polars_error(
				"polars collect panic: get_streaming_executor_builder() failed (hint: missing feature new-streaming?)"
			),
			"other"
		);
		assert_eq!(
			classify_polars_error("planning failed: outer relation join interval exists"),
			"other"
		);

		// A control engine is only usable if a dialect gap reads as "cannot express this", not as a
		// red cell. These are the verbatim Polars 0.54 messages behind 14 of the 22 TPC-H texts;
		// both hedge ("not currently supported") or state the limit positively, so neither was
		// recognised and every board run reported them as failures.
		assert_eq!(
			classify_polars_error(
				"polars SQL execute failed: SQLInterface(ErrString(\"multiple tables in FROM clause are not currently supported\"))"
			),
			"from_clause_dialect"
		);
		assert_eq!(
			classify_polars_error(
				"polars SQL execute failed: SQLInterface(ErrString(\"only equi-join constraints are currently supported; found e\"))"
			),
			"join_constraint"
		);
		assert!(polars_error_is_known_unsupported(
			"multiple tables in FROM clause are not currently supported"
		));
		assert!(polars_error_is_known_unsupported(
			"only equi-join constraints are currently supported"
		));

		// The widened phrase must not swallow a genuine execution failure.
		assert_eq!(
			classify_polars_error("polars collect failed: out of memory"),
			"other"
		);
		assert!(!polars_error_is_known_unsupported(
			"polars collect failed: out of memory"
		));
	}

	#[cfg(all(feature = "polars-sql-reference", unix))]
	#[test]
	fn polars_register_reports_non_utf8_path_as_invalid_path() {
		use std::ffi::OsString;
		use std::os::unix::ffi::OsStringExt;

		let mut export = fake_polars_export();
		export.tables.push(toron_tpch::TpchParquetTableExport {
			name: "bad_path".to_string(),
			path: PathBuf::from(OsString::from_vec(vec![0xff])),
			row_count: 0,
			schema_fingerprint: "schema".to_string(),
			schema_signature: "schema".to_string(),
			file_hash: "hash".to_string(),
		});
		let mut context = polars_sql::SQLContext::new();
		let error = register_polars_reference_tables(&export, &mut context)
			.expect_err("non-UTF8 path should fail before scan");

		assert_eq!(error.reason, "polars_invalid_path");
		assert!(error.message.contains("non-utf8 parquet path"));
	}

	#[test]
	fn polars_sql_reference_uses_explicit_join_dialect_for_q03() {
		let export = fake_polars_export();
		let polars_sql = polars_sql_text(&export);
		assert_ne!(polars_sql.as_ref(), export.sql);
		assert!(polars_sql.contains("JOIN orders ON c_custkey = o_custkey"));
		assert!(polars_sql.contains("JOIN lineitem ON l_orderkey = o_orderkey"));
	}

	#[test]
	fn duckdb_version_is_mirrored_to_generic_engine_version() {
		let row = duckdb_not_attempted_row(
			"q06",
			"strict",
			1.0,
			0,
			false,
			"duckdb_absent",
			Some("DuckDB v1.5.4".to_string()),
		);
		assert_eq!(row["engine_version"], "DuckDB v1.5.4");
		assert_eq!(row["duckdb_version"], "DuckDB v1.5.4");
	}

	#[test]
	fn primitive_ladder_requires_explicit_one_thread_duckdb() {
		assert!(require_one_thread_duckdb_setting(Some("1")).is_ok());
		assert!(
			require_one_thread_duckdb_setting(None)
				.expect_err("unset DuckDB thread count must reject")
				.contains("TORON_TPCH_DUCKDB_THREADS=1")
		);
		assert!(
			require_one_thread_duckdb_setting(Some("16"))
				.expect_err("parallel DuckDB thread count must reject")
				.contains("found \"16\"")
		);
	}

	#[test]
	fn perf_summary_deduplicates_row_flags() {
		let mut first = perf_row_for_engine("duckdb", "q06", 80.0, "duckdb-plan", true);
		first["flags"] = serde_json::json!(["schema_mismatch"]);
		let mut second = perf_row_for_engine("duckdb", "q06", 82.0, "duckdb-plan", true);
		second["flags"] = serde_json::json!(["schema_mismatch"]);

		let summary = summarize_perf_rows(&[first, second], 2, 0, &BTreeMap::new());
		let flags = summary[0]["flags"].as_array().expect("flags");
		assert_eq!(flags.len(), 1);
		assert_eq!(flags[0], "schema_mismatch");
	}

	#[test]
	fn duckdb_schema_fingerprint_normalizes_describe_types() {
		let describe = serde_json::json!([
			{"column_name": "revenue", "column_type": "DECIMAL(32, 4)"},
			{"column_name": "shipdate", "column_type": "DATE"},
			{"column_name": "name", "column_type": "VARCHAR"}
		]);
		let observed = duckdb_describe_schema_fingerprint(&describe).unwrap();
		let expected = schema_fingerprint_from_duckdb_fields(&[
			("revenue".to_string(), "DECIMAL(32,4)".to_string()),
			("shipdate".to_string(), "DATE".to_string()),
			("name".to_string(), "VARCHAR".to_string()),
		]);
		assert_eq!(observed, expected);
	}

	fn fake_polars_export() -> toron_tpch::TpchParquetExport {
		toron_tpch::TpchParquetExport {
			query_id: "q03".to_string(),
			mode: "strict".to_string(),
			scale_factor: 1.0,
			sql: "select 1".to_string(),
			root_dir: PathBuf::from("target/tpch/data/strict/sf1"),
			manifest_path: PathBuf::from("target/tpch/data/strict/sf1/manifest.json"),
			manifest_fingerprint: "manifest".to_string(),
			tables: Vec::new(),
			dataset_verification: None,
		}
	}

	fn perf_row(
		query: &str,
		execute_ms: f64,
		plan_fingerprint: &str,
		release: bool,
	) -> serde_json::Value {
		perf_row_for_engine("toron", query, execute_ms, plan_fingerprint, release)
	}

	fn perf_row_for_engine(
		engine: &str,
		query: &str,
		execute_ms: f64,
		plan_fingerprint: &str,
		release: bool,
	) -> serde_json::Value {
		serde_json::json!({
			"suite": "tpch_perf",
			"engine": engine,
			"query": query,
			"scale_factor": 1.0,
			"mode": "strict",
			"run_index": 0,
			"warmup": false,
			"profile": if release { "release" } else { "debug" },
			"phase": "execute",
			"status": "pass",
			"execute_ms": execute_ms,
			"peak_rss_kib": 1000,
			"spill_bytes": 0,
			"plan_fingerprint": plan_fingerprint,
			"digest": "digest",
			"output_row_count": 1,
			"output_schema_fingerprint": "schema",
		})
	}

	/// One measured toron q06 row, differing only in run index and its verification claim.
	fn perf_row_with_verification(run_index: u32, verification: Option<&str>) -> serde_json::Value {
		let mut row = perf_row("q06", 100.0, "plan-a", true);
		row["run_index"] = serde_json::json!(run_index);
		row["dataset_verification"] = match verification {
			Some(value) => serde_json::json!(value),
			None => serde_json::Value::Null,
		};
		row
	}

	fn summary_flags(row: &serde_json::Value) -> Vec<&str> {
		row.get("flags")
			.and_then(serde_json::Value::as_array)
			.into_iter()
			.flatten()
			.filter_map(serde_json::Value::as_str)
			.collect()
	}

	fn perf_summary_row(status: &str, profile: &str, flags: &[&str]) -> serde_json::Value {
		serde_json::json!({
			"suite": "tpch_perf_summary",
			"engine": "toron",
			"query": "q06",
			"scale_factor": 1.0,
			"mode": "strict",
			"profile": profile,
			"status": status,
			"execute_ms_median": 100.0,
			"execute_ms_min": 100.0,
			"execute_ms_max": 100.0,
			"execute_ms_spread_ratio": 0.0,
			"peak_rss_kib_max": 1000,
			"spill_bytes_max": 0,
			"plan_fingerprint": "plan-a",
			"digest": "digest",
			"output_row_count": 1,
			"output_schema_fingerprint": "schema",
			"flags": flags,
		})
	}
}
#[cfg(test)]
mod adr469_tests {
	#[test]
	fn adr469_hash_smaller_build_knob_contract() {
		assert!(toron::binder::PlannerConfig::default().hash_smaller_build_placement);
	}
}
#[cfg(test)]
mod adr511_tests {
	#[test]
	fn adr511_filtered_build_side_swap_knob_contract() {
		assert!(toron::binder::PlannerConfig::default().filtered_build_side_swap);
	}
}
#[cfg(test)]
mod adr512_tests {
	#[test]
	fn adr512_scalar_agg_input_reduction_knob_contract() {
		assert!(toron::binder::PlannerConfig::default().scalar_aggregate_input_reduction);
	}
}
#[cfg(test)]
mod adr515_tests {
	#[test]
	fn adr515_membership_first_knob_contract() {
		assert!(toron::binder::PlannerConfig::default().membership_first_composition);
	}
}
#[cfg(test)]
mod adr517_tests {
	#[test]
	fn adr517_side_local_filter_routing_knob_contract() {
		assert!(toron::binder::PlannerConfig::default().side_local_filter_routing);
	}
}
#[cfg(test)]
mod adr518_tests {
	use super::*;

	#[test]
	fn reference_thread_pins_are_observational() {
		let absent = reference_thread_setting(Err(std::env::VarError::NotPresent));
		let explicit = reference_thread_setting(Ok("1".to_string()));
		let empty = reference_thread_setting(Ok(String::new()));
		assert_eq!(absent, None);
		assert_eq!(empty, None);
		assert_eq!(explicit.as_deref(), Some("1"));
		assert_eq!(reference_thread_pin_provenance(&absent), "engine_default");
		assert_eq!(reference_thread_pin_provenance(&explicit), "explicit");
	}

	#[test]
	fn adr518_semi_filtered_reachable_keys_knob_contract() {
		assert!(toron::binder::PlannerConfig::default().semi_filtered_reachable_keys);
	}
}

#[cfg(test)]
mod adr521_tests {
	#[test]
	fn adr521_outer_count_preaggregation_knob_contract() {
		assert!(toron::binder::PlannerConfig::default().outer_count_preaggregation);
	}
}

/// ADR 473: the primitive ladder — run the cost primitives across engines and
/// record every condition that makes the comparison meaningful. Emits one JSONL
/// row per (engine, primitive) plus a rendered ratio table. Conditions recorded:
/// data source and path, engine version, thread setting, budget coordinates,
/// rows/groups/output rows, spill bytes, and per-run samples.
async fn ladder_command(args: &[String]) -> ExitCode {
	let mode = flag_value(args, "--mode").unwrap_or("strict");
	let data_source = flag_value(args, "--data-source").unwrap_or("parquet");
	let data_dir = data_dir_from_args(args);
	let sf = match explicit_sf(args) {
		Ok(explicit) => explicit.unwrap_or(1.0),
		Err(message) => return harness_error(&message),
	};
	let runs = match parse_usize_flag(args, "--runs", 5) {
		Ok(runs) if runs > 0 => runs,
		Ok(_) => return harness_error("--runs must be greater than zero"),
		Err(message) => return harness_error(&message),
	};
	let warmup = match parse_usize_flag(args, "--warmup", 1) {
		Ok(warmup) => warmup,
		Err(message) => return harness_error(&message),
	};
	let engines = match parse_perf_engines(flag_value(args, "--engines").unwrap_or("toron,duckdb"))
	{
		Ok(engines) => engines,
		Err(message) => return harness_error(&message),
	};
	if let Err(message) = require_one_thread_duckdb_reference(&engines) {
		return harness_error(&message);
	}
	let output_dir =
		PathBuf::from(flag_value(args, "--output-dir").unwrap_or("target/tpch/ladder"));
	if let Err(error) = std::fs::create_dir_all(&output_dir) {
		return harness_error(&format!("create ladder output directory: {error}"));
	}
	let (memory_per_task, memory_provenance) = match gauge_memory_per_task() {
		Ok(value) => value,
		Err(message) => return harness_error(&message),
	};
	let mut rows = Vec::new();
	let mut medians: BTreeMap<(String, String), f64> = BTreeMap::new();
	for primitive in PRIMITIVE_LADDER_IDS {
		for engine in &engines {
			let mut samples = Vec::new();
			let mut last: Option<serde_json::Value> = None;
			let mut failure = None;
			// DuckDB has no perf-run-one child; it runs through its own rail,
			// which already produces one row per run over the same parquet views.
			if *engine == PerfEngine::DuckDb {
				match run_duckdb_perf_rows(
					primitive,
					mode,
					sf,
					runs,
					warmup,
					&output_dir,
					data_source,
					&data_dir,
				) {
					Ok(duck_rows) => {
						for row in &duck_rows {
							if row.get("warmup").and_then(serde_json::Value::as_bool) == Some(true)
							{
								continue;
							}
							if row.get("status").and_then(serde_json::Value::as_str) != Some("pass")
							{
								failure = Some(row.to_string());
								break;
							}
							if let Some(value) =
								row.get("execute_ms").and_then(serde_json::Value::as_f64)
							{
								samples.push(value);
							}
							last = Some(row.clone());
						}
					}
					Err(message) => failure = Some(message),
				}
			}
			for index in 0..(warmup + runs) {
				if *engine == PerfEngine::DuckDb {
					break;
				}
				let warm = index < warmup;
				match run_perf_child(
					*engine,
					primitive,
					mode,
					sf,
					index as u32,
					warm,
					data_source,
					&data_dir,
				) {
					Ok(row) => {
						if row.get("status").and_then(serde_json::Value::as_str) != Some("pass") {
							failure = Some(row.to_string());
							break;
						}
						if !warm {
							if let Some(value) =
								row.get("execute_ms").and_then(serde_json::Value::as_f64)
							{
								samples.push(value);
							}
						}
						last = Some(row);
					}
					Err(message) => {
						failure = Some(message);
						break;
					}
				}
			}
			let mut sorted = samples.clone();
			let median = if sorted.is_empty() {
				None
			} else {
				Some(median(&mut sorted))
			};
			if let (Some(median), None) = (median, failure.as_ref()) {
				medians.insert((primitive.to_string(), engine.as_str().to_string()), median);
			}
			let row = serde_json::json!({
				"suite": "adr473_primitive_ladder",
				"primitive": primitive,
				"isolates": primitive_ladder_intent(primitive),
				"engine": engine.as_str(),
				"engine_version": last.as_ref().and_then(|row| row.get("engine_version").cloned()),
				"duckdb_threads": duckdb_thread_setting(),
				"polars_max_threads": std::env::var("POLARS_MAX_THREADS").ok(),
				"mode": mode,
				"scale_factor": sf,
				"data_source": data_source,
				"data_dir": data_dir.display().to_string(),
				"memory_per_task": memory_per_task,
				"memory_per_task_provenance": memory_provenance,
				"runs": runs,
				"warmup": warmup,
				"execute_ms_samples": samples,
				"execute_ms_median": median,
				"output_row_count": last.as_ref().and_then(|row| row.get("output_row_count").cloned()),
				"digest": last.as_ref().and_then(|row| row.get("digest").cloned()),
				"spill_bytes": last.as_ref().and_then(|row| row.get("spill_bytes").cloned()),
				"peak_rss_kib": last.as_ref().and_then(|row| row.get("peak_rss_kib").cloned()),
				"plan_fingerprint": last.as_ref().and_then(|row| row.get("plan_fingerprint").cloned()),
				"failure": failure,
			});
			println!("{row}");
			rows.push(row);
		}
	}
	let artifact = output_dir.join("ladder.jsonl");
	let body = rows
		.iter()
		.map(ToString::to_string)
		.collect::<Vec<_>>()
		.join("\n");
	if let Err(error) = std::fs::write(&artifact, format!("{body}\n")) {
		return harness_error(&format!("write ladder artifact: {error}"));
	}
	// The ratio table: every engine against the best observed reference for that
	// primitive. Digest equality across engines is asserted where all engines
	// produced one — a ratio between different answers is meaningless.
	for primitive in PRIMITIVE_LADDER_IDS {
		let mut cells = Vec::new();
		for engine in &engines {
			if let Some(value) = medians.get(&(primitive.to_string(), engine.as_str().to_string()))
			{
				cells.push((engine.as_str(), *value));
			}
		}
		let Some(best) = cells
			.iter()
			.map(|(_, value)| *value)
			.fold(None::<f64>, |acc, value| {
				Some(acc.map_or(value, |current: f64| current.min(value)))
			})
		else {
			continue;
		};
		let rendered = cells
			.iter()
			.map(|(engine, value)| format!("{engine}={value:.1}ms({:.1}x)", value / best))
			.collect::<Vec<_>>()
			.join(" ");
		println!(
			"ladder_ratio primitive={primitive} best={best:.1}ms {rendered} isolates=\"{}\"",
			primitive_ladder_intent(primitive)
		);
	}
	ExitCode::SUCCESS
}

#[cfg(test)]
mod adr591_tests {
	use super::*;

	#[test]
	fn adr591_a_replay_reports_a_claim_the_current_gate_no_longer_supports() {
		let dir = std::env::temp_dir().join(format!("adr591-replay-{}", std::process::id()));
		std::fs::create_dir_all(&dir).unwrap();
		let artifact = dir.join(PERF_BOARD_ARTIFACT);
		// A header claiming authority over an empty board: the gate refuses it on sample count
		// and coordinate grounds, so the claim disagrees with the replay and the reasons are named.
		let header = serde_json::json!({
			"kind": "artifact_header",
			"suite": PERF_BOARD_SUITE,
			"candidate_authority": true,
			"authority_reasons": [],
		});
		write_perf_board_artifact(&artifact, &header, &[], &[]).unwrap();
		let (report, agrees) = perf_board_replay(&artifact).unwrap();
		assert!(!agrees);
		assert_eq!(report["claimed_candidate_authority"], true);
		assert_eq!(report["replayed_candidate_authority"], false);
		assert!(
			report["replayed_reasons"]
				.as_array()
				.unwrap()
				.iter()
				.any(|reason| reason
					== "coordinate_mismatch:cpu_affinity_requested_provenance:expected_explicit")
		);
		assert!(perf_board_replay(&dir.join("absent.jsonl")).is_err());
		std::fs::remove_dir_all(&dir).unwrap();
	}
}

#[cfg(test)]
mod adr592_tests {
	use super::*;
	use toron_tpch::affinity::AffinityCoordinate;
	use toron_tpch::evidence::{BuildRecipe, ExecutableIdentity, RunEvidenceManifest, SealState};

	fn manifest(tree: &str, exe: &str) -> BuildManifest {
		let recipe = BuildRecipe::new(
			"release",
			Vec::<String>::new(),
			"x86_64-unknown-linux-gnu",
			"rustc 1.95.0",
			"a".repeat(64),
			"b".repeat(64),
		)
		.unwrap();
		let identity = ExecutableIdentity {
			sha256: exe.repeat(64),
			staged_path: PathBuf::from(format!("/staged/{}", exe.repeat(64))),
		};
		BuildManifest::new(
			tree.repeat(40),
			tree.repeat(40),
			recipe,
			identity.clone(),
			identity,
			Some(ExecutableIdentity {
				sha256: "d".repeat(64),
				staged_path: PathBuf::from("/staged/duckdb"),
			}),
			Vec::new(),
		)
		.unwrap()
	}

	fn sealed_manifest() -> RunEvidenceManifest {
		RunEvidenceManifest {
			build: manifest("9", "5"),
			suite: PERF_BOARD_SUITE.to_string(),
			artifact_path: PathBuf::from("/boards/perf_board.jsonl"),
			artifact_sha256: "f".repeat(64),
			candidate_authority: true,
			seal_state: SealState::Sealed,
			final_authority: true,
			archived_harness: None,
			archived_toron: None,
			archived_duckdb: None,
			source_tree_tag: Some("evidence/tag".to_string()),
		}
	}

	fn sealed_header() -> serde_json::Value {
		let build = manifest("9", "5");
		serde_json::json!({
			"kind": "artifact_header",
			"candidate_authority": true,
			"mode": "strict",
			"data_source": "parquet",
			"scale_factor": 8.0,
			"memory_per_task": 2_147_483_648u64,
			"memory_per_task_provenance": "explicit",
			"spill_bytes_reserved": 2_147_483_648u64,
			"spill_bytes_reserved_provenance": "explicit",
			"partition_work_concurrency_requested": 1,
			"partition_work_concurrency_requested_provenance": "explicit",
			"dataset_manifest_fingerprint": "fnv1a64:0fc5c06d19c5ed23",
			"cpu_affinity_requested": "0-15",
			"cpu_affinity_requested_provenance": "explicit",
			"cpu_affinity_observed": "0-15",
			"evidence_identity": {
				"executable_sha256": build.toron.sha256,
				"source_tree_id": build.source_tree_id,
			},
		})
	}

	fn sealed_summaries() -> Vec<serde_json::Value> {
		vec![
			serde_json::json!({"engine": "duckdb", "query": "q21", "status": "pass",
				"execute_ms_median": 2210.0, "digest": null}),
			serde_json::json!({"engine": "toron", "query": "q21", "status": "pass",
				"execute_ms_median": 9126.9, "digest": ADR588_SEALED_Q21_DIGEST}),
		]
	}

	fn bind(
		manifest: &RunEvidenceManifest,
		observed: &str,
		header: &serde_json::Value,
		summaries: &[serde_json::Value],
	) -> Result<Adr592SealedBaseline, String> {
		adr592_bind_sealed_baseline(
			"/boards/sealed",
			manifest,
			observed,
			header,
			summaries,
			"q21",
		)
	}

	#[test]
	fn adr592_the_baseline_is_bound_by_content_and_every_mismatch_is_named() {
		let manifest = sealed_manifest();
		let hash = "f".repeat(64);
		let baseline = bind(&manifest, &hash, &sealed_header(), &sealed_summaries()).unwrap();
		assert_eq!(baseline.q21_toron_execute_ms_median, 9126.9);
		assert_eq!(baseline.q21_toron_digest, ADR588_SEALED_Q21_DIGEST);
		assert_eq!(baseline.cpu_affinity_requested, "0-15");
		assert_eq!(
			baseline.dataset_manifest_fingerprint,
			"fnv1a64:0fc5c06d19c5ed23"
		);
		assert_eq!(
			baseline.duckdb_sha256.as_deref(),
			Some("d".repeat(64).as_str())
		);
		assert_eq!(baseline.source_tree_id, "9".repeat(40));

		// The artifact's bytes are not what the manifest claims.
		let error = bind(
			&manifest,
			&"e".repeat(64),
			&sealed_header(),
			&sealed_summaries(),
		)
		.unwrap_err();
		assert!(
			error.contains("hashes to") && error.contains(&"e".repeat(64)),
			"{error}"
		);

		// Unsealed, or sealed without final authority.
		let mut unsealed = sealed_manifest();
		unsealed.seal_state = SealState::Unsealed;
		assert!(
			bind(&unsealed, &hash, &sealed_header(), &sealed_summaries())
				.unwrap_err()
				.contains("not a sealed manifest")
		);
		let mut not_final = sealed_manifest();
		not_final.final_authority = false;
		assert!(
			bind(&not_final, &hash, &sealed_header(), &sealed_summaries())
				.unwrap_err()
				.contains("final_authority=false")
		);

		// The artifact lacks a passing q21 Toron cell, or the cell lacks its digest.
		let only_duckdb = sealed_summaries()[..1].to_vec();
		assert!(
			bind(&manifest, &hash, &sealed_header(), &only_duckdb)
				.unwrap_err()
				.contains("no q21 Toron cell")
		);
		let mut failing = sealed_summaries();
		failing[1]["status"] = serde_json::json!("fail");
		assert!(
			bind(&manifest, &hash, &sealed_header(), &failing)
				.unwrap_err()
				.contains("did not pass")
		);
		let mut undigested = sealed_summaries();
		undigested[1]["digest"] = serde_json::Value::Null;
		assert!(
			bind(&manifest, &hash, &sealed_header(), &undigested)
				.unwrap_err()
				.contains("lacks a digest")
		);

		// The header lacks the affinity coordinate or claims it by default.
		let mut no_affinity = sealed_header();
		no_affinity["cpu_affinity_requested"] = serde_json::Value::Null;
		assert!(
			bind(&manifest, &hash, &no_affinity, &sealed_summaries())
				.unwrap_err()
				.contains("lacks cpu_affinity_requested")
		);
		let mut default_affinity = sealed_header();
		default_affinity["cpu_affinity_requested_provenance"] = serde_json::json!("default");
		assert!(
			bind(&manifest, &hash, &default_affinity, &sealed_summaries())
				.unwrap_err()
				.contains("not explicit")
		);
		let mut unauthoritative = sealed_header();
		unauthoritative["candidate_authority"] = serde_json::json!(false);
		assert!(
			bind(&manifest, &hash, &unauthoritative, &sealed_summaries())
				.unwrap_err()
				.contains("candidate authority")
		);

		// A sealed board at another coordinate is not this record's reference: every
		// coordinate field is checked, one at a time.
		for (field, other) in [
			("mode", serde_json::json!("dev")),
			("data_source", serde_json::json!("memory")),
			("scale_factor", serde_json::json!(10.0)),
			("memory_per_task", serde_json::json!(1_073_741_824u64)),
			("memory_per_task_provenance", serde_json::json!("default")),
			("spill_bytes_reserved", serde_json::json!(1_073_741_824u64)),
			(
				"spill_bytes_reserved_provenance",
				serde_json::json!("default"),
			),
			("partition_work_concurrency_requested", serde_json::json!(2)),
			(
				"partition_work_concurrency_requested_provenance",
				serde_json::json!("default"),
			),
		] {
			let mut header = sealed_header();
			header[field] = other;
			let error = bind(&manifest, &hash, &header, &sealed_summaries()).unwrap_err();
			assert!(
				error.contains(&format!("artifact {field} is")),
				"{field}: {error}"
			);
			let mut header = sealed_header();
			header.as_object_mut().unwrap().remove(field);
			let error = bind(&manifest, &hash, &header, &sealed_summaries()).unwrap_err();
			assert!(
				error.contains(&format!("artifact {field} is")),
				"{field} absent: {error}"
			);
		}

		// The artifact's own identity must be the manifest's.
		let mut other_exe = sealed_header();
		other_exe["evidence_identity"]["executable_sha256"] = serde_json::json!("6".repeat(64));
		let error = bind(&manifest, &hash, &other_exe, &sealed_summaries()).unwrap_err();
		assert!(
			error.contains("but the manifest names executable"),
			"{error}"
		);
		let mut other_tree = sealed_header();
		other_tree["evidence_identity"]["source_tree_id"] = serde_json::json!("8".repeat(40));
		assert!(
			bind(&manifest, &hash, &other_tree, &sealed_summaries())
				.unwrap_err()
				.contains("but the manifest names")
		);
		let mut no_identity = sealed_header();
		no_identity
			.as_object_mut()
			.unwrap()
			.remove("evidence_identity");
		assert!(
			bind(&manifest, &hash, &no_identity, &sealed_summaries())
				.unwrap_err()
				.contains("but the manifest names")
		);
	}

	fn driver_coordinate(expected: &Adr592ChildExpectation) -> Adr592DriverCoordinate {
		Adr592DriverCoordinate {
			baseline: None,
			expectation: expected.clone(),
			launched_manifest: expected.instrumented_manifest_path.clone(),
		}
	}

	fn journal_in_temp(name: &str) -> (PathBuf, PerfSampleJournal) {
		let dir = std::env::temp_dir().join(format!("adr592-{name}-{}", std::process::id()));
		std::fs::create_dir_all(&dir).unwrap();
		(dir.clone(), prepare_perf_sample_journal(&dir).unwrap())
	}

	fn journaled(dir: &Path) -> Vec<serde_json::Value> {
		std::fs::read_to_string(dir.join(PERF_SAMPLE_JOURNAL))
			.unwrap()
			.lines()
			.map(|line| serde_json::from_str(line).unwrap())
			.collect()
	}

	// A child that fails is not an error the driver swallows before validation: the row reaches
	// the validator, is journaled with its refusal, and no further child is launched.
	#[test]
	fn adr592_a_failed_warmup_is_journaled_with_its_refusal_and_stops_the_driver() {
		let expected = expectation();
		let coordinate = driver_coordinate(&expected);
		let (dir, journal) = journal_in_temp("failed-warmup");
		let mut launches = 0;
		let error = adr592_run_paired_children(
			("adr584_arm", "adr584_pair", "adr584_pair_position"),
			5,
			&coordinate,
			&journal,
			|_, _, _| {
				launches += 1;
				let mut row = child_row(&expected, true);
				row["status"] = serde_json::json!("fail");
				row["reason"] = serde_json::json!("out_of_memory");
				Ok(row)
			},
		)
		.unwrap_err();
		assert!(
			error.contains("did not pass") && error.contains("row retained"),
			"{error}"
		);
		assert_eq!(launches, 1, "no child after the refused warmup");
		let rows = journaled(&dir);
		assert_eq!(rows.len(), 1);
		assert_eq!(rows[0]["status"], "fail");
		assert_eq!(rows[0]["adr584_arm"], "off");
		assert!(
			rows[0]["adr592_refusal"]
				.as_str()
				.unwrap()
				.contains("did not pass")
		);
		assert_eq!(
			rows[0]["adr592_launched_executable_sha256"],
			expected.instrumented.toron.sha256
		);
		std::fs::remove_dir_all(&dir).unwrap();
	}

	#[test]
	fn adr592_a_refused_pair_row_stops_after_the_rows_before_it() {
		let expected = expectation();
		let coordinate = driver_coordinate(&expected);
		let (dir, journal) = journal_in_temp("refused-pair");
		let mut launches = 0;
		let error = adr592_run_paired_children(
			("adr585_arm", "adr585_pair", "adr585_pair_position"),
			5,
			&coordinate,
			&journal,
			|run_index, warmup, on| {
				launches += 1;
				let mut row = child_row(&expected, !on);
				// Pair 1's first child (the on arm) comes back observed on another mask.
				if !warmup && run_index == 1 && on {
					row["cpu_affinity_observed"] = serde_json::json!("0-31");
				}
				Ok(row)
			},
		)
		.unwrap_err();
		assert!(error.contains("cpu_affinity_observed is"), "{error}");
		assert_eq!(launches, 4, "warmup, pair 0 off, pair 0 on, pair 1 on");
		let rows = journaled(&dir);
		assert_eq!(rows.len(), 4);
		assert!(
			rows[..3]
				.iter()
				.all(|row| row.get("adr592_refusal").is_none())
		);
		assert_eq!(rows[3]["adr585_pair"], 1);
		assert_eq!(rows[3]["adr585_arm"], "on");
		assert!(
			rows[3]["adr592_refusal"]
				.as_str()
				.unwrap()
				.contains("cpu_affinity_observed")
		);
		std::fs::remove_dir_all(&dir).unwrap();
	}

	fn adr597_arms() -> Adr597Arms {
		let arm = |name: &'static str, tree: &str, exe: &str| {
			let manifest = manifest(tree, exe);
			let manifest_path = PathBuf::from(format!("/manifests/{name}.manifest"));
			let expectation = Adr592ChildExpectation {
				label: "ADR 597",
				query: "q21".to_string(),
				instrumented: manifest.clone(),
				instrumented_manifest_path: manifest_path.display().to_string(),
				dataset_manifest_fingerprint: String::new(),
				data_source: String::new(),
				digest: String::new(),
				cpu_affinity_requested: String::new(),
			};
			(
				Adr588Arm {
					name,
					manifest_path,
					executable: PathBuf::from(format!("/staged/{name}")),
					manifest,
				},
				expectation,
			)
		};
		let (control, control_expectation) = arm("control", "3", "7");
		let (treatment, treatment_expectation) = arm("treatment", "4", "8");
		Adr597Arms {
			control,
			treatment,
			control_expectation,
			treatment_expectation,
		}
	}

	/// A passing row from the arm named by its executable.
	fn adr597_child_row(
		coordinate: &Adr592DriverCoordinate,
		arms: &Adr597Arms,
		arm: &Adr588Arm,
	) -> serde_json::Value {
		let expectation = if arm.name == "control" {
			&arms.control_expectation
		} else {
			&arms.treatment_expectation
		};
		child_row(&adr597_arm_expectation(coordinate, expectation), true)
	}

	// ADR 597: the twelve-row contract — the control's warmup, the treatment's warmup, then five
	// alternating pairs — every row stamped with its arm's executable and validated as it
	// returns; a failing warmup on either arm stops the run with the refusal journaled.
	#[test]
	fn adr597_a_complete_run_journals_twelve_admitted_rows_in_order() {
		let expected = expectation();
		let coordinate = driver_coordinate(&expected);
		let arms = adr597_arms();
		let (dir, journal) = journal_in_temp("adr597-complete");
		let mut launched = Vec::new();
		let (warmups, rows) = adr597_run_children(
			("adr597_arm", "adr597_pair", "adr597_pair_position"),
			5,
			&coordinate,
			&arms,
			&journal,
			|_, warmup, arm| {
				launched.push((arm.name, warmup));
				Ok(adr597_child_row(&coordinate, &arms, arm))
			},
		)
		.unwrap();
		assert_eq!(warmups.len(), 2);
		assert_eq!(rows.len(), 10);
		assert_eq!(
			launched[..2],
			[("control", true), ("treatment", true)],
			"the control's warmup first, then the treatment's"
		);
		let arms_in_order: Vec<_> = rows
			.iter()
			.map(|row| row["adr597_arm"].as_str().unwrap())
			.collect();
		assert_eq!(
			arms_in_order,
			[
				"control",
				"treatment",
				"treatment",
				"control",
				"control",
				"treatment",
				"treatment",
				"control",
				"control",
				"treatment"
			]
		);
		for row in warmups.iter().chain(rows.iter()) {
			let arm = row["adr597_arm"].as_str().unwrap();
			let expected_exe = if arm == "control" {
				&arms.control.manifest.toron.sha256
			} else {
				&arms.treatment.manifest.toron.sha256
			};
			assert_eq!(
				&row["adr592_launched_executable_sha256"], expected_exe,
				"{row}"
			);
			assert!(row["driver_launch_unix_ms"].as_f64().unwrap() > 0.0);
			assert!(row["driver_return_unix_ms"].as_f64().unwrap() > 0.0);
			assert!(row.get("adr592_refusal").is_none());
		}
		assert_eq!(warmups[0]["adr597_phase"], "warmup");
		assert_eq!(rows[0]["adr597_phase"], "pairs");
		assert_eq!(rows[0]["adr597_pair_position"], 0);
		assert_eq!(rows[3]["adr597_pair"], 1);
		assert_eq!(rows[3]["adr597_pair_position"], 1);
		let journaled_rows = journaled(&dir);
		assert_eq!(journaled_rows.len(), 12);
		// The retained journal satisfies the re-evaluation's twelve-row contract.
		lifecycle_window_from_rows(&journaled_rows, &adr597_row_contract()).unwrap();
		std::fs::remove_dir_all(&dir).unwrap();
	}

	#[test]
	fn adr597_a_failing_warmup_on_either_arm_stops_the_run_with_its_refusal_journaled() {
		let expected = expectation();
		let coordinate = driver_coordinate(&expected);
		let arms = adr597_arms();
		for (failing, expected_launches) in [("control", 1), ("treatment", 2)] {
			let (dir, journal) = journal_in_temp(&format!("adr597-failing-{failing}"));
			let mut launches = 0;
			let error = adr597_run_children(
				("adr597_arm", "adr597_pair", "adr597_pair_position"),
				5,
				&coordinate,
				&arms,
				&journal,
				|_, _, arm| {
					launches += 1;
					let mut row = adr597_child_row(&coordinate, &arms, arm);
					if arm.name == failing {
						row["status"] = serde_json::json!("fail");
						row["reason"] = serde_json::json!("out_of_memory");
					}
					Ok(row)
				},
			)
			.unwrap_err();
			assert!(
				error.contains("did not pass") && error.contains("row retained"),
				"{failing}: {error}"
			);
			assert_eq!(launches, expected_launches, "{failing}");
			let rows = journaled(&dir);
			assert_eq!(rows.len(), expected_launches);
			let last = rows.last().unwrap();
			assert_eq!(last["adr597_arm"], failing);
			assert_eq!(last["warmup"], true);
			assert!(
				last["adr592_refusal"]
					.as_str()
					.unwrap()
					.contains("did not pass")
			);
			std::fs::remove_dir_all(&dir).unwrap();
		}
		// A pair row from the wrong executable is refused where it returns.
		let (dir, journal) = journal_in_temp("adr597-wrong-exe");
		let error = adr597_run_children(
			("adr597_arm", "adr597_pair", "adr597_pair_position"),
			5,
			&coordinate,
			&arms,
			&journal,
			|run_index, warmup, arm| {
				let mut row = adr597_child_row(&coordinate, &arms, arm);
				if !warmup && run_index == 1 && arm.name == "treatment" {
					row["evidence_identity"]["executable_sha256"] =
						serde_json::json!(arms.control.manifest.toron.sha256);
				}
				Ok(row)
			},
		)
		.unwrap_err();
		assert!(error.contains("row retained"), "{error}");
		let rows = journaled(&dir);
		assert_eq!(
			rows.len(),
			5,
			"two warmups, pair 0, then pair 1's first row refused"
		);
		assert_eq!(rows[4]["adr597_arm"], "treatment");
		assert!(rows[4].get("adr592_refusal").is_some());
		std::fs::remove_dir_all(&dir).unwrap();
	}

	#[test]
	fn adr592_a_complete_run_journals_eleven_admitted_rows_in_order() {
		let expected = expectation();
		let coordinate = driver_coordinate(&expected);
		let (dir, journal) = journal_in_temp("complete");
		let (warmup, rows) = adr592_run_paired_children(
			("adr584_arm", "adr584_pair", "adr584_pair_position"),
			5,
			&coordinate,
			&journal,
			|_, _, on| Ok(child_row(&expected, !on)),
		)
		.unwrap();
		assert_eq!(warmup["adr584_arm"], "off");
		assert_eq!(rows.len(), 10);
		let arms: Vec<_> = rows
			.iter()
			.map(|row| row["adr584_arm"].as_str().unwrap())
			.collect();
		assert_eq!(
			arms,
			[
				"off", "on", "on", "off", "off", "on", "on", "off", "off", "on"
			]
		);
		assert_eq!(journaled(&dir).len(), 11);
		std::fs::remove_dir_all(&dir).unwrap();
	}

	#[test]
	fn adr592_the_driver_coordinate_must_match_the_baseline_affinity_and_dataset() {
		let baseline = bind(
			&sealed_manifest(),
			&"f".repeat(64),
			&sealed_header(),
			&sealed_summaries(),
		)
		.unwrap();
		let explicit = AffinityCoordinate {
			requested: Some("0-15".to_string()),
			provenance: "explicit",
		};
		adr592_bind_driver_coordinate("ADR 584", &baseline, &explicit, "fnv1a64:0fc5c06d19c5ed23")
			.unwrap();
		let other = AffinityCoordinate {
			requested: Some("0-7".to_string()),
			provenance: "explicit",
		};
		let error =
			adr592_bind_driver_coordinate("ADR 584", &baseline, &other, "fnv1a64:0fc5c06d19c5ed23")
				.unwrap_err();
		assert!(
			error.contains("affinity 0-7") && error.contains("under 0-15"),
			"{error}"
		);
		let default = AffinityCoordinate {
			requested: None,
			provenance: "default",
		};
		let error = adr592_bind_driver_coordinate(
			"ADR 584",
			&baseline,
			&default,
			"fnv1a64:0fc5c06d19c5ed23",
		)
		.unwrap_err();
		assert!(error.contains("explicit CPU affinity"), "{error}");
		let error =
			adr592_bind_driver_coordinate("ADR 585", &baseline, &explicit, "fnv1a64:57c8f4f5")
				.unwrap_err();
		assert!(
			error.contains("loaded dataset fnv1a64:57c8f4f5") && error.contains("0fc5c06d19c5ed23"),
			"{error}"
		);
	}

	fn expectation() -> Adr592ChildExpectation {
		Adr592ChildExpectation {
			label: "ADR 584",
			query: "q21".to_string(),
			instrumented: manifest("3", "7"),
			instrumented_manifest_path: "/manifests/instrumented.manifest".to_string(),
			dataset_manifest_fingerprint: "fnv1a64:0fc5c06d19c5ed23".to_string(),
			data_source: "parquet".to_string(),
			digest: ADR588_SEALED_Q21_DIGEST.to_string(),
			cpu_affinity_requested: "0-15".to_string(),
		}
	}

	fn child_row(
		expected: &Adr592ChildExpectation,
		diagnostics_disabled: bool,
	) -> serde_json::Value {
		serde_json::json!({
			"query": "q21",
			"engine": "toron",
			"status": "pass",
			"mode": "strict",
			"scale_factor": 8.0,
			"memory_per_task": 2_147_483_648u64,
			"spill_bytes_reserved": 2_147_483_648u64,
			"partition_work_concurrency_requested": 1,
			"diagnostics_disabled": diagnostics_disabled,
			"digest": ADR588_SEALED_Q21_DIGEST,
			"dataset_verification": "full_hash",
			"dataset_manifest_fingerprint": "fnv1a64:0fc5c06d19c5ed23",
			"data_source": "parquet",
			"cpu_affinity_requested": "0-15",
			"cpu_affinity_requested_provenance": "explicit",
			"cpu_affinity_observed": "0-15",
			"execute_ms": 9000.0,
			"adr592_launched_manifest": expected.instrumented_manifest_path,
			"adr592_launched_executable_sha256": expected.instrumented.toron.sha256,
			"evidence_identity": {
				"executable_sha256": expected.instrumented.toron.sha256,
				"source_tree_id": expected.instrumented.source_tree_id,
				"recipe_sha256": expected.instrumented.recipe_digest,
			},
		})
	}

	// ADR 606: `--query` names which sealed cell binds, and the validator holds every child
	// to that query, so a q05 pair can neither bind q21's cell nor accept a q21 row.
	#[test]
	fn adr606_the_named_query_binds_its_own_cell_and_refuses_other_rows() {
		let manifest = sealed_manifest();
		let hash = "f".repeat(64);
		let mut summaries = sealed_summaries();
		summaries.push(
			serde_json::json!({"engine": "toron", "query": "q05", "status": "pass",
			"execute_ms_median": 4321.5, "digest": "q05-digest"}),
		);
		let baseline = adr592_bind_sealed_baseline(
			"/boards/sealed",
			&manifest,
			&hash,
			&sealed_header(),
			&summaries,
			"q05",
		)
		.unwrap();
		assert_eq!(baseline.query, "q05");
		assert_eq!(baseline.q21_toron_execute_ms_median, 4321.5);
		assert_eq!(baseline.q21_toron_digest, "q05-digest");
		assert_eq!(baseline.json_value()["query"], "q05");
		let error = adr592_bind_sealed_baseline(
			"/boards/sealed",
			&manifest,
			&hash,
			&sealed_header(),
			&sealed_summaries(),
			"q05",
		)
		.unwrap_err();
		assert!(error.contains("no q05 Toron cell"), "{error}");

		let mut expected = expectation();
		expected.query = "q05".to_string();
		let error =
			adr592_validate_child_row(&child_row(&expected, true), &expected, true).unwrap_err();
		assert!(error.contains("is not a q05 Toron row"), "{error}");
		let mut row = child_row(&expected, true);
		row["query"] = serde_json::json!("q05");
		adr592_validate_child_row(&row, &expected, true).expect("a q05 row passes");
	}

	// The one validator runs on every child, the warmup included, and each refusal changes
	// exactly one thing on an otherwise admissible row.
	#[test]
	fn adr592_the_child_validator_refuses_each_defect_by_name() {
		let expected = expectation();
		adr592_validate_child_row(&child_row(&expected, true), &expected, true)
			.expect("an off row passes");
		adr592_validate_child_row(&child_row(&expected, false), &expected, false)
			.expect("an on row passes");
		let cases: Vec<(&str, Box<dyn Fn(&mut serde_json::Value)>, &str)> = vec![
			(
				"status",
				Box::new(|row| row["status"] = serde_json::json!("fail")),
				"did not pass",
			),
			(
				"query",
				Box::new(|row| row["query"] = serde_json::json!("q09")),
				"not a q21 Toron row",
			),
			(
				"engine",
				Box::new(|row| row["engine"] = serde_json::json!("duckdb")),
				"not a q21 Toron row",
			),
			(
				"memory",
				Box::new(|row| row["memory_per_task"] = serde_json::json!(1_073_741_824u64)),
				"sealed coordinate",
			),
			(
				"parallelism",
				Box::new(|row| row["partition_work_concurrency_requested"] = serde_json::json!(2)),
				"sealed coordinate",
			),
			(
				"diagnostics on the off arm",
				Box::new(|row| row["diagnostics_disabled"] = serde_json::json!(false)),
				"diagnostics_disabled",
			),
			(
				"affinity requested",
				Box::new(|row| row["cpu_affinity_requested"] = serde_json::json!("0-31")),
				"cpu_affinity_requested is",
			),
			(
				"affinity provenance",
				Box::new(|row| {
					row["cpu_affinity_requested_provenance"] = serde_json::json!("default")
				}),
				"cpu_affinity_requested_provenance is",
			),
			(
				"affinity observed",
				Box::new(|row| row["cpu_affinity_observed"] = serde_json::json!("0-31")),
				"cpu_affinity_observed is",
			),
			(
				"affinity absent",
				Box::new(|row| {
					row.as_object_mut()
						.unwrap()
						.retain(|key, _| !key.starts_with("cpu_affinity"));
				}),
				"cpu_affinity_requested is None",
			),
			(
				"digest other",
				Box::new(|row| row["digest"] = serde_json::json!("ArrowIpcDigest([0])")),
				"digest parity failed",
			),
			(
				"digest absent",
				Box::new(|row| row["digest"] = serde_json::Value::Null),
				"lacks a digest",
			),
			(
				"verification",
				Box::new(|row| row["dataset_verification"] = serde_json::json!("declared")),
				"fully verified",
			),
			(
				"fingerprint",
				Box::new(|row| {
					row["dataset_manifest_fingerprint"] = serde_json::json!("fnv1a64:57c8f4f5")
				}),
				"names dataset",
			),
			(
				"data source",
				Box::new(|row| row["data_source"] = serde_json::json!("memory")),
				"data source",
			),
			(
				"identity",
				Box::new(|row| {
					row["evidence_identity"]["executable_sha256"] =
						serde_json::json!("5".repeat(64))
				}),
				"not the instrumented manifest's",
			),
			(
				"tree",
				Box::new(|row| {
					row["evidence_identity"]["source_tree_id"] = serde_json::json!("9".repeat(40))
				}),
				"not the instrumented manifest's",
			),
			(
				"launched manifest",
				Box::new(|row| {
					row["adr592_launched_manifest"] =
						serde_json::json!("/manifests/sealed.manifest")
				}),
				"launched with manifest",
			),
			(
				"launched executable",
				Box::new(|row| {
					row["adr592_launched_executable_sha256"] = serde_json::json!("5".repeat(64))
				}),
				"executable other than the instrumented",
			),
		];
		for (label, mutate, needle) in cases {
			let mut row = child_row(&expected, true);
			mutate(&mut row);
			let error = adr592_validate_child_row(&row, &expected, true).expect_err(label);
			assert!(error.contains(needle), "{label}: {error}");
		}
		// An on row that ran with diagnostics disabled is refused too: the arm expects them on.
		let error =
			adr592_validate_child_row(&child_row(&expected, true), &expected, false).unwrap_err();
		assert!(error.contains("the arm expects false"), "{error}");
	}
}

#[cfg(test)]
mod adr593_tests {
	use super::*;
	use toron_tpch::host_readings::{JournalCollection, JournalEntry, JournalState};

	fn lifecycle_summary(range: f64, median: f64) -> serde_json::Value {
		serde_json::json!({
			"off_execute_range_ms": range,
			"off_execute_median_ms": median,
			"owner": {"status": "unresolved_tie_within_off_range", "leading_term": "runtime_prep_inner_1", "median_scaled_ms": 1334.5},
		})
	}

	#[test]
	fn adr593_the_bound_closes_on_variability_above_four_percent_and_not_below() {
		let mut over = lifecycle_summary(609.0, 9023.8);
		adr593_apply_variability_bound(&mut over).unwrap();
		assert_eq!(over["variability"]["verdict"], "exceeded");
		assert_eq!(over["owner"]["status"], "closed_on_variability");
		assert_eq!(over["owner"]["leading_term"], "runtime_prep_inner_1");
		assert!(over["owner"].get("term").is_none());
		let mut under = lifecycle_summary(300.0, 9023.8);
		adr593_apply_variability_bound(&mut under).unwrap();
		assert_eq!(under["variability"]["verdict"], "within_bound");
		assert_eq!(under["owner"]["status"], "unresolved_tie_within_off_range");
		let mut at = lifecycle_summary(0.04 * 9000.0, 9000.0);
		adr593_apply_variability_bound(&mut at).unwrap();
		assert_eq!(
			at["variability"]["verdict"], "within_bound",
			"the bound is inclusive"
		);
	}

	#[test]
	fn adr593_the_journal_is_joined_per_child_and_gaps_come_from_the_stamps() {
		let entry = |ms: f64, msg: &str| JournalEntry {
			realtime_us: (ms * 1000.0) as u64,
			monotonic_us: None,
			boot_id: Some("b".to_string()),
			source: Some("anacron.service".to_string()),
			message: msg.to_string(),
		};
		let collection = JournalCollection {
			since_unix_ms: 9_000.0,
			until_unix_ms: 40_000.0,
			state: JournalState::Collected,
			entries: vec![
				entry(10_500.0, "inside the warmup"),
				entry(25_000.0, "between children"),
				entry(31_000.0, "inside pair 0 on"),
			],
			malformed_records: 0,
			bytes_read: 0,
			entry_cap: 2000,
			byte_cap: 1 << 24,
			duration_bound_ms: 30_000,
			command: String::new(),
		};
		let warmup = serde_json::json!({"run_index": 0, "warmup": true, "driver_launch_unix_ms": 10_000.0, "driver_return_unix_ms": 20_000.0});
		let off = serde_json::json!({"run_index": 0, "warmup": false, "driver_launch_unix_ms": 20_500.0, "driver_return_unix_ms": 23_000.0});
		let on = serde_json::json!({"run_index": 0, "warmup": false, "driver_launch_unix_ms": 30_000.0, "driver_return_unix_ms": 35_000.0});
		let joined = adr593_join_journal_and_gaps(&[&warmup, &off, &on], &collection);
		let per_child = joined["per_child"].as_array().unwrap();
		assert_eq!(per_child[0]["journal_count"], 1);
		assert_eq!(per_child[1]["journal_count"], 0);
		assert_eq!(per_child[2]["journal_count"], 1);
		assert_eq!(per_child[1]["gap_since_previous_return_ms"], 500.0);
		assert_eq!(per_child[2]["gap_since_previous_return_ms"], 7000.0);
		assert_eq!(
			per_child[0]["gap_since_previous_return_ms"],
			serde_json::Value::Null
		);
		assert_eq!(
			per_child[0]["journal_window_inside_collection_bounds"],
			true
		);
		assert_eq!(per_child[0]["journal_coverage"], "complete");
		// A window outside the collection's bounds is said so, not silently empty.
		let late = serde_json::json!({"run_index": 1, "warmup": false, "driver_launch_unix_ms": 39_500.0, "driver_return_unix_ms": 41_000.0});
		let joined = adr593_join_journal_and_gaps(&[&late], &collection);
		assert_eq!(
			joined["per_child"][0]["journal_window_inside_collection_bounds"],
			false
		);
		assert_eq!(
			joined["per_child"][0]["journal_coverage"],
			"outside_collection_bounds"
		);
		assert_eq!(
			joined["per_child"][0]["journal_count"],
			serde_json::Value::Null
		);
	}

	// Inside the requested bounds is not coverage: a truncated collection covers a window only
	// partially and its count is a lower bound; an unavailable or degraded one covers nothing and
	// its count is unknown, however many entries happen to lie in the window.
	#[test]
	fn adr593_truncated_and_unavailable_collections_never_report_complete_coverage() {
		let entry = JournalEntry {
			realtime_us: 10_500_000,
			monotonic_us: None,
			boot_id: None,
			source: Some("x".to_string()),
			message: "m".to_string(),
		};
		let warmup = serde_json::json!({"run_index": 0, "warmup": true, "driver_launch_unix_ms": 10_000.0, "driver_return_unix_ms": 20_000.0});
		let base = JournalCollection {
			since_unix_ms: 9_000.0,
			until_unix_ms: 40_000.0,
			state: JournalState::Collected,
			entries: vec![entry],
			malformed_records: 0,
			bytes_read: 100,
			entry_cap: 2000,
			byte_cap: 1 << 24,
			duration_bound_ms: 30_000,
			command: String::new(),
		};
		let truncated = JournalCollection {
			state: JournalState::Truncated("entry cap 2000 hit; 2000 entries kept".to_string()),
			..base.clone()
		};
		let child = &adr593_join_journal_and_gaps(&[&warmup], &truncated)["per_child"][0];
		assert_eq!(child["journal_window_inside_collection_bounds"], true);
		assert_eq!(child["journal_coverage"], "partial");
		assert_eq!(child["journal_count"]["at_least"], 1);
		for state in [
			JournalState::Unavailable("journalctl exited 1".to_string()),
			JournalState::Degraded("3 malformed records skipped".to_string()),
		] {
			let collection = JournalCollection {
				state,
				..base.clone()
			};
			let child = &adr593_join_journal_and_gaps(&[&warmup], &collection)["per_child"][0];
			assert_eq!(child["journal_window_inside_collection_bounds"], true);
			assert_eq!(child["journal_coverage"], "unknown");
			assert_eq!(child["journal_count"], serde_json::Value::Null);
			assert_eq!(
				child["journal_entries"].as_array().unwrap().len(),
				1,
				"entries still shown"
			);
		}
		let complete = &adr593_join_journal_and_gaps(&[&warmup], &base)["per_child"][0];
		assert_eq!(complete["journal_coverage"], "complete");
		assert_eq!(complete["journal_count"], 1);
	}
}
