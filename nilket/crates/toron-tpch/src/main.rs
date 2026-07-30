//! ADR 261: TPC-H Reachability Harness CLI — `status` / `prepare` / `run`, manual arg parsing (no clap,
//! matching Toron's no-extra-framework style). An unknown command/arg is a *harness* error (exit 2),
//! distinct from a query `blocked`/`fail` (which is structured telemetry).

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::time::Instant;

use toron_tpch::telemetry::{
	Attempt, CorrectnessAttempt, PerfAttempt, PerfPhaseTimings, Phase, Status,
};
use toron_tpch::{
	ALL_QUERY_IDS, CACHE_ROOT, PRIMITIVE_LADDER_IDS, Q17BoundaryCensusOutcome,
	check_strict_query_with_data_source, correctness_matrix, export_perf_query_parquet,
	export_persisted_perf_query_parquet, gauge_filtered_build_side_swap, gauge_hash_smaller_build,
	gauge_membership_first, gauge_memory_per_task, gauge_scalar_agg_input_reduction,
	gauge_side_local_filter_routing, gauge_spill_bytes_reserved, generate_dev_lineitem,
	load_persisted_tpch_dataset, matrix, node_attribution_outer_reconciliation,
	normalize_duckdb_type, paired_ablation_dataset_fingerprint, parse_scale_factor,
	prepare_persisted_tpch_dataset, primitive_ladder_intent, q17_correctness_scale, run_dev_q1,
	run_dev_q02, run_dev_q3, run_dev_q04, run_dev_q05, run_dev_q6, run_dev_q07, run_dev_q08,
	run_dev_q09, run_dev_q10, run_dev_q11, run_dev_q12, run_dev_q13, run_dev_q14, run_dev_q15,
	run_dev_q16, run_dev_q17, run_dev_q18, run_dev_q19, run_dev_q20, run_dev_q21, run_dev_q22,
	run_perf_query_with_data_source, run_q17_boundary_census_with_data_source,
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

#[tokio::main]
async fn main() -> ExitCode {
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
		"adr449-gate-zero" => adr449_gate_zero_command(&args),
		"adr450-scale-ladder" => adr450_scale_ladder_command(&args),
		"adr448-capability" => {
			println!("adr448-harness-capability-v1");
			ExitCode::SUCCESS
		}
		"ladder" => ladder_command(&args).await,
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
	println!("{row}");
	ExitCode::SUCCESS
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
	match run_adr450_scale_ladder(query, &scales, mode, data_source, &data_dir, &output_dir) {
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
) -> Result<serde_json::Value, String> {
	if data_source != "parquet" && data_source != "persisted" {
		return Err("ADR 450 requires --data-source parquet".to_string());
	}
	let (memory_per_task, memory_provenance) = gauge_memory_per_task()?;
	let (spill_bytes_reserved, spill_provenance) = gauge_spill_bytes_reserved()?;
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
		join_spill_observations: None,
		dataset_verification: None,
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
		join_spill_observations: None,
		dataset_verification: None,
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
		join_spill_observations: None,
		dataset_verification: None,
	}
	.json_value()
}

/// ADR 473: engine thread count is a first-class ladder condition — a comparison
/// that does not record it is not a roofline.
fn duckdb_thread_setting() -> Option<String> {
	std::env::var("TORON_TPCH_DUCKDB_THREADS")
		.ok()
		.filter(|value| !value.is_empty())
}

/// A primitive-ladder row compares Toron's one-task coordinate against a one-thread reference.
/// Do not silently fall back to DuckDB's machine-dependent default parallelism: the recorded
/// provenance must be the literal single-thread setting before a ladder can run its DuckDB arm.
fn require_single_thread_duckdb_reference(engines: &[PerfEngine]) -> Result<(), String> {
	if !engines.contains(&PerfEngine::DuckDb) {
		return Ok(());
	}
	require_single_thread_duckdb_setting(duckdb_thread_setting().as_deref())
}

fn require_single_thread_duckdb_setting(setting: Option<&str>) -> Result<(), String> {
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
	format!("{threads}{views}")
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
		join_spill_observations: None,
		dataset_verification: None,
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
		join_spill_observations: None,
		dataset_verification: export.dataset_verification,
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
		join_spill_observations: None,
		dataset_verification: export.dataset_verification,
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
		dataset_verification: export.dataset_verification,
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
	let script = format!(
		".mode json\n{}\n.timer on\n{}\n",
		duckdb_setup_sql(export),
		duckdb_query_statement(&export.sql)
	);
	let output = run_duckdb_script(duckdb_bin, &script)?;
	if !output.status_success {
		let mut timings = PerfPhaseTimings::default();
		timings.total_ms = output.parent_wall_ms;
		return Ok(PerfAttempt {
			engine: "duckdb".to_string(),
			query: export.query_id.clone(),
			scale_factor: export.scale_factor,
			mode: export.mode.clone(),
			run_index,
			warmup,
			phase: Phase::Execute,
			status: Status::Fail,
			reason: Some("duckdb_query_failed".to_string()),
			diagnostic_message: Some(format!("stderr={:?}", output.stderr)),
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
			join_spill_observations: None,
			dataset_verification: export.dataset_verification,
		}
		.json_value());
	}
	let combined = format!("{}\n{}", output.stdout, output.stderr);
	let execute_ms = parse_duckdb_timer_ms(&combined)
		.ok_or_else(|| format!("DuckDB timer output missing from {:?}", combined))?;
	let arrays = extract_json_arrays(&output.stdout);
	let result = arrays
		.last()
		.ok_or_else(|| "DuckDB query emitted no JSON result".to_string())?;
	let row_count = result
		.as_array()
		.map(|rows| rows.len() as u64)
		.ok_or_else(|| "DuckDB query JSON result was not an array".to_string())?;
	let mut timings = PerfPhaseTimings::default();
	timings.execute_ms = execute_ms;
	timings.total_ms = execute_ms;
	Ok(PerfAttempt {
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
		join_spill_observations: None,
		dataset_verification: export.dataset_verification,
	}
	.json_value())
}

#[derive(Debug)]
struct DuckDbCommandOutput {
	stdout: String,
	stderr: String,
	parent_wall_ms: f64,
	peak_rss_kib: Option<u64>,
	status_success: bool,
}

#[cfg(unix)]
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
	let exe = toron_binary()?;
	let start = Instant::now();
	let mut command = Command::new(exe);
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
	object.insert(
		"peak_rss_kib".to_string(),
		serde_json::json!(usage.ru_maxrss.max(0) as u64),
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
		"dataset_verification": dataset_verification,
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
) -> Result<PerfSampleJournal, String> {
	if dataset_fingerprint.is_empty() {
		return Err("paired-ablation manifest requires a dataset fingerprint".to_string());
	}
	let (memory_per_task, _) = gauge_memory_per_task()?;
	let (spill_bytes_reserved, _) = gauge_spill_bytes_reserved()?;
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
	let build_commit = on_row
		.get("build_commit")
		.and_then(serde_json::Value::as_str)
		.ok_or_else(|| "shape gate candidate row lacks a string build_commit".to_string())?;
	if build_commit.contains("dirty") || build_commit.is_empty() {
		return Err("shape gate requires a clean sealed build identity".to_string());
	}
	for field in [
		"build_commit",
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
	for key in [
		"plan_fingerprint",
		"digest",
		"output_schema_fingerprint",
		"build_commit",
	] {
		if row_str(on_row, key)? != row_str(off_row, key)? {
			return Err(format!("route gate arms disagree on {key}"));
		}
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
		"usage: toron-tpch <status|prepare|run|matrix|correctness|correctness-matrix|perf|adr449-gate-zero|q03-plan-proof|q17-census> [--query q06|all] [--mode dev|strict] [--sf 0.01] [--allow-blocked] [--runs 7] [--warmup 1] [--paired-ablation] [--engines toron|rune|duckdb|polars_sql|toron,duckdb] [--data-source memory|parquet] [--data-dir target/tpch/data] [--output-dir target/tpch/perf]"
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
		assert_eq!(
			shape_preserving_spill_index_gate_record(&candidate, &control).unwrap()["passed"],
			true
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
			)
			.is_err()
		);
		// ADR 456: a resume across different budget coordinates is a regime mix
		// and must refuse exactly like a scale or dataset mismatch.
		// SAFETY: gate suites run single-threaded; no concurrent env readers.
		unsafe { std::env::set_var("TORON_TPCH_MEMORY_PER_TASK", "134217728") };
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
			)
			.is_err()
		);
		unsafe { std::env::remove_var("TORON_TPCH_MEMORY_PER_TASK") };
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
	fn adr454_gauge_budget_defaults_and_overrides_with_provenance() {
		// SAFETY: gate suites run single-threaded (--test-threads=1); no other
		// thread reads the environment while this test mutates it.
		unsafe { std::env::remove_var("TORON_TPCH_MEMORY_PER_TASK") };
		assert_eq!(
			gauge_memory_per_task().unwrap(),
			(64 * 1024 * 1024, "default")
		);
		unsafe { std::env::set_var("TORON_TPCH_MEMORY_PER_TASK", "268435456") };
		assert_eq!(gauge_memory_per_task().unwrap(), (268435456, "explicit"));
		unsafe { std::env::set_var("TORON_TPCH_MEMORY_PER_TASK", "0") };
		assert!(gauge_memory_per_task().is_err());
		unsafe { std::env::set_var("TORON_TPCH_MEMORY_PER_TASK", "lots") };
		assert!(gauge_memory_per_task().is_err());
		unsafe { std::env::remove_var("TORON_TPCH_MEMORY_PER_TASK") };
		unsafe { std::env::remove_var("TORON_TPCH_SPILL_BYTES_RESERVED") };
		assert_eq!(
			gauge_spill_bytes_reserved().unwrap(),
			(512 * 1024 * 1024, "default")
		);
		unsafe { std::env::set_var("TORON_TPCH_SPILL_BYTES_RESERVED", "2147483648") };
		assert_eq!(
			gauge_spill_bytes_reserved().unwrap(),
			(2147483648, "explicit")
		);
		unsafe { std::env::set_var("TORON_TPCH_SPILL_BYTES_RESERVED", "0") };
		assert!(gauge_spill_bytes_reserved().is_err());
		unsafe { std::env::remove_var("TORON_TPCH_SPILL_BYTES_RESERVED") };
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
	fn primitive_ladder_requires_explicit_single_thread_duckdb() {
		assert!(require_single_thread_duckdb_setting(Some("1")).is_ok());
		assert!(
			require_single_thread_duckdb_setting(None)
				.expect_err("unset DuckDB thread count must reject")
				.contains("TORON_TPCH_DUCKDB_THREADS=1")
		);
		assert!(
			require_single_thread_duckdb_setting(Some("16"))
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
	use super::*;

	#[test]
	fn adr469_hash_smaller_build_knob_contract() {
		// SAFETY: gate suites run single-threaded; no concurrent env readers.
		unsafe { std::env::remove_var("TORON_TPCH_HASH_SMALLER_BUILD") };
		assert_eq!(gauge_hash_smaller_build().unwrap(), (true, "default"));
		unsafe { std::env::set_var("TORON_TPCH_HASH_SMALLER_BUILD", "0") };
		assert_eq!(gauge_hash_smaller_build().unwrap(), (false, "explicit"));
		unsafe { std::env::set_var("TORON_TPCH_HASH_SMALLER_BUILD", "1") };
		assert_eq!(gauge_hash_smaller_build().unwrap(), (true, "explicit"));
		unsafe { std::env::set_var("TORON_TPCH_HASH_SMALLER_BUILD", "yes") };
		assert!(gauge_hash_smaller_build().is_err());
		unsafe { std::env::remove_var("TORON_TPCH_HASH_SMALLER_BUILD") };
	}
}
#[cfg(test)]
mod adr511_tests {
	use super::*;

	#[test]
	fn adr511_filtered_build_side_swap_knob_contract() {
		// SAFETY: gate suites run single-threaded; no concurrent env readers.
		unsafe { std::env::remove_var("TORON_TPCH_FILTERED_BUILD_SIDE_SWAP") };
		assert_eq!(gauge_filtered_build_side_swap().unwrap(), (true, "default"));
		unsafe { std::env::set_var("TORON_TPCH_FILTERED_BUILD_SIDE_SWAP", "1") };
		assert_eq!(
			gauge_filtered_build_side_swap().unwrap(),
			(true, "explicit")
		);
		unsafe { std::env::set_var("TORON_TPCH_FILTERED_BUILD_SIDE_SWAP", "0") };
		assert_eq!(
			gauge_filtered_build_side_swap().unwrap(),
			(false, "explicit")
		);
		unsafe { std::env::set_var("TORON_TPCH_FILTERED_BUILD_SIDE_SWAP", "yes") };
		assert!(gauge_filtered_build_side_swap().is_err());
		unsafe { std::env::remove_var("TORON_TPCH_FILTERED_BUILD_SIDE_SWAP") };
	}
}
#[cfg(test)]
mod adr512_tests {
	use super::*;

	#[test]
	fn adr512_scalar_agg_input_reduction_knob_contract() {
		// SAFETY: gate suites run single-threaded; no concurrent env readers.
		unsafe { std::env::remove_var("TORON_TPCH_SCALAR_AGG_INPUT_REDUCTION") };
		assert_eq!(
			gauge_scalar_agg_input_reduction().unwrap(),
			(true, "default")
		);
		unsafe { std::env::set_var("TORON_TPCH_SCALAR_AGG_INPUT_REDUCTION", "1") };
		assert_eq!(
			gauge_scalar_agg_input_reduction().unwrap(),
			(true, "explicit")
		);
		unsafe { std::env::set_var("TORON_TPCH_SCALAR_AGG_INPUT_REDUCTION", "0") };
		assert_eq!(
			gauge_scalar_agg_input_reduction().unwrap(),
			(false, "explicit")
		);
		unsafe { std::env::set_var("TORON_TPCH_SCALAR_AGG_INPUT_REDUCTION", "yes") };
		assert!(gauge_scalar_agg_input_reduction().is_err());
		unsafe { std::env::remove_var("TORON_TPCH_SCALAR_AGG_INPUT_REDUCTION") };
	}
}
#[cfg(test)]
mod adr515_tests {
	use super::*;

	#[test]
	fn adr515_membership_first_knob_contract() {
		// SAFETY: gate suites run single-threaded; no concurrent env readers.
		unsafe { std::env::remove_var("TORON_TPCH_MEMBERSHIP_FIRST") };
		assert_eq!(gauge_membership_first().unwrap(), (true, "default"));
		unsafe { std::env::set_var("TORON_TPCH_MEMBERSHIP_FIRST", "1") };
		assert_eq!(gauge_membership_first().unwrap(), (true, "explicit"));
		unsafe { std::env::set_var("TORON_TPCH_MEMBERSHIP_FIRST", "0") };
		assert_eq!(gauge_membership_first().unwrap(), (false, "explicit"));
		unsafe { std::env::set_var("TORON_TPCH_MEMBERSHIP_FIRST", "yes") };
		assert!(gauge_membership_first().is_err());
		unsafe { std::env::remove_var("TORON_TPCH_MEMBERSHIP_FIRST") };
	}
}
#[cfg(test)]
mod adr517_tests {
	use super::*;

	#[test]
	fn adr517_side_local_filter_routing_knob_contract() {
		// SAFETY: gate suites run single-threaded; no concurrent env readers.
		unsafe { std::env::remove_var("TORON_TPCH_SIDE_LOCAL_FILTER_ROUTING") };
		assert_eq!(
			gauge_side_local_filter_routing().unwrap(),
			(false, "default")
		);
		unsafe { std::env::set_var("TORON_TPCH_SIDE_LOCAL_FILTER_ROUTING", "1") };
		assert_eq!(
			gauge_side_local_filter_routing().unwrap(),
			(true, "explicit")
		);
		unsafe { std::env::set_var("TORON_TPCH_SIDE_LOCAL_FILTER_ROUTING", "0") };
		assert_eq!(
			gauge_side_local_filter_routing().unwrap(),
			(false, "explicit")
		);
		unsafe { std::env::set_var("TORON_TPCH_SIDE_LOCAL_FILTER_ROUTING", "yes") };
		assert!(gauge_side_local_filter_routing().is_err());
		unsafe { std::env::remove_var("TORON_TPCH_SIDE_LOCAL_FILTER_ROUTING") };
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
	if let Err(message) = require_single_thread_duckdb_reference(&engines) {
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
