//! ADR 261: TPC-H Reachability Harness CLI — `status` / `prepare` / `run`, manual arg parsing (no clap,
//! matching Toron's no-extra-framework style). An unknown command/arg is a *harness* error (exit 2),
//! distinct from a query `blocked`/`fail` (which is structured telemetry).

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::time::Instant;

use toron_tpch::telemetry::{
	Attempt, CorrectnessAttempt, PerfAttempt, PerfPhaseTimings, Phase, Status,
};
use toron_tpch::{
	ALL_QUERY_IDS, CACHE_ROOT, Q17_CORRECTNESS_SCALE, check_strict_query_with_data_source,
	correctness_matrix, export_perf_query_parquet, export_persisted_perf_query_parquet,
	generate_dev_lineitem, matrix, normalize_duckdb_type, parse_scale_factor,
	prepare_persisted_tpch_dataset, run_dev_q1, run_dev_q02, run_dev_q3, run_dev_q04, run_dev_q05,
	run_dev_q6, run_dev_q07, run_dev_q08, run_dev_q09, run_dev_q10, run_dev_q11, run_dev_q12,
	run_dev_q13, run_dev_q14, run_dev_q15, run_dev_q16, run_dev_q17, run_dev_q18, run_dev_q19,
	run_dev_q20, run_dev_q21, run_dev_q22, run_perf_query_with_data_source,
	run_rune_perf_query_with_data_source, run_strict_q1, run_strict_q02, run_strict_q3,
	run_strict_q04, run_strict_q05, run_strict_q6, run_strict_q07, run_strict_q08, run_strict_q09,
	run_strict_q10, run_strict_q11, run_strict_q12, run_strict_q13, run_strict_q14, run_strict_q15,
	run_strict_q16, run_strict_q17, run_strict_q18, run_strict_q19, run_strict_q20, run_strict_q21,
	run_strict_q22, schema_fingerprint_from_duckdb_fields, stable_fingerprint, status_rows,
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

async fn perf_run_one_command(args: &[String]) -> ExitCode {
	let query = flag_value(args, "--query").unwrap_or("q06");
	let mode = flag_value(args, "--mode").unwrap_or("strict");
	let data_source = flag_value(args, "--data-source").unwrap_or("memory");
	let data_dir = data_dir_from_args(args);
	let sf = match explicit_sf(args) {
		Ok(explicit) => explicit.unwrap_or(1.0),
		Err(message) => return harness_error(&message),
	};
	let run_index = match parse_u32_flag(args, "--run-index", 0) {
		Ok(run_index) => run_index,
		Err(message) => return harness_error(&message),
	};
	let warmup = args.iter().any(|arg| arg == "--warmup-run");
	let engine = match parse_perf_engine(flag_value(args, "--engine").unwrap_or("toron")) {
		Ok(engine) => engine,
		Err(message) => return harness_error(&message),
	};
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
	println!("{}", attempt.jsonl());
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

async fn perf_command(args: &[String]) -> ExitCode {
	let query = flag_value(args, "--query").unwrap_or("all");
	let mode = flag_value(args, "--mode").unwrap_or("strict");
	let data_source = flag_value(args, "--data-source").unwrap_or("memory");
	let data_dir = data_dir_from_args(args);
	let sf = match explicit_sf(args) {
		Ok(explicit) => explicit.unwrap_or(1.0),
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
	let mut raw_rows = Vec::new();
	for query_id in queries {
		for engine in &engines {
			match engine {
				PerfEngine::Toron => {
					for warmup_index in 0..warmup {
						match run_perf_child(
							PerfEngine::Toron,
							query_id,
							mode,
							sf,
							warmup_index as u32,
							true,
							data_source,
							&data_dir,
						) {
							Ok(row) => raw_rows.push(row),
							Err(message) => return harness_error(&message),
						}
					}
					for run_index in 0..runs {
						match run_perf_child(
							PerfEngine::Toron,
							query_id,
							mode,
							sf,
							run_index as u32,
							false,
							data_source,
							&data_dir,
						) {
							Ok(row) => raw_rows.push(row),
							Err(message) => return harness_error(&message),
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
							Ok(row) => raw_rows.push(row),
							Err(message) => return harness_error(&message),
						}
					}
					for run_index in 0..runs {
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
							Ok(row) => raw_rows.push(row),
							Err(message) => return harness_error(&message),
						}
					}
				}
				PerfEngine::DuckDb => {
					match run_duckdb_perf_rows(
						query_id,
						mode,
						sf,
						runs,
						warmup,
						&output_dir,
						data_source,
						&data_dir,
					) {
						Ok(rows) => raw_rows.extend(rows),
						Err(message) => return harness_error(&message),
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
							Ok(row) => raw_rows.push(row),
							Err(message) => return harness_error(&message),
						}
					}
					for run_index in 0..runs {
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
							Ok(row) => raw_rows.push(row),
							Err(message) => return harness_error(&message),
						}
					}
				}
			}
		}
	}

	apply_duckdb_output_sanity_flags(&mut raw_rows);
	let baseline_rows = read_baseline_rows(&baseline_path);
	let summary_rows = summarize_perf_rows(&raw_rows, runs, warmup, &baseline_rows);
	if let Err(error) = write_perf_outputs(&output_dir, &baseline_path, &raw_rows, &summary_rows) {
		return harness_error(&format!("write perf output: {error}"));
	}

	for row in &summary_rows {
		print_perf_summary(row);
	}
	ExitCode::SUCCESS
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
	match explicit {
		None => Ok(Q17_CORRECTNESS_SCALE),
		Some(sf) if (sf - Q17_CORRECTNESS_SCALE).abs() <= f64::EPSILON => Ok(sf),
		Some(sf) => Err(format!(
			"q17_sf10_oracle_policy_gap: q17 correctness requires --sf {Q17_CORRECTNESS_SCALE}; got {sf}"
		)),
	}
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
			if strict {
				run_strict_q11(sf).await
			} else {
				run_dev_q11(sf).await
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
	run_perf_child_args(&args)
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
	}
	.json_value()
}

fn duckdb_setup_sql(export: &toron_tpch::TpchParquetExport) -> String {
	export
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
		.join("\n")
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
	}
}

#[cfg(feature = "polars-sql-reference")]
fn run_polars_perf_export(
	export: &toron_tpch::TpchParquetExport,
	run_index: u32,
	warmup: bool,
) -> PerfAttempt {
	use polars::prelude::*;
	use polars_sql::SQLContext;

	let plan_fingerprint = polars_plan_fingerprint(export);
	let engine_version = Some(polars::VERSION.to_string());
	let mut context = SQLContext::new();
	for table in &export.tables {
		let Some(path) = table.path.to_str() else {
			return polars_fail_attempt(
				export,
				&plan_fingerprint,
				run_index,
				warmup,
				"polars_invalid_path",
				&format!("non-utf8 parquet path {}", table.path.display()),
				engine_version,
			);
		};
		let lazy = match LazyFrame::scan_parquet(path.into(), Default::default()) {
			Ok(lazy) => lazy,
			Err(error) => {
				return polars_fail_attempt(
					export,
					&plan_fingerprint,
					run_index,
					warmup,
					"polars_scan_failed",
					&format!("{error:?}"),
					engine_version,
				);
			}
		};
		context.register(&table.name, lazy);
	}

	let polars_sql = polars_sql_text(export);
	let lazy = match context.execute(polars_sql.as_ref()) {
		Ok(lazy) => lazy,
		Err(error) => {
			return polars_unsupported_attempt(
				export,
				&plan_fingerprint,
				run_index,
				warmup,
				&format!("{error:?}"),
				engine_version,
			);
		}
	};
	let start = Instant::now();
	let frame = match lazy.collect() {
		Ok(frame) => frame,
		Err(error) => {
			return polars_unsupported_attempt(
				export,
				&plan_fingerprint,
				run_index,
				warmup,
				&format!("{error:?}"),
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
	}
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
	}
}

#[cfg(any(feature = "polars-sql-reference", test))]
fn classify_polars_error(message: &str) -> &'static str {
	let lower = message.to_ascii_lowercase();
	if lower.contains("subquer")
		|| lower.contains("correlated")
		|| lower.contains("exists")
		|| lower.contains("outer")
	{
		"correlated_subquery"
	} else if lower.contains("interval") {
		"interval_syntax"
	} else if lower.contains("not like") || lower.contains(" join") || lower.contains("join ") {
		"join_constraint"
	} else if lower.contains("2^32") || lower.contains("2**32") || lower.contains("index") {
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

#[cfg(unix)]
fn run_perf_child_args(args: &[String]) -> Result<serde_json::Value, String> {
	let exe = std::env::current_exe().map_err(|error| format!("current exe: {error}"))?;
	let start = Instant::now();
	let mut child = Command::new(exe)
		.args(args)
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
	if !libc::WIFEXITED(status) || libc::WEXITSTATUS(status) != 0 {
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
	let exe = std::env::current_exe().map_err(|error| format!("current exe: {error}"))?;
	let start = Instant::now();
	let output = Command::new(exe)
		.args(args)
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
	if !output.status.success() {
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
	let mut flags = Vec::new();
	const STABILITY_THRESHOLD: f64 = 0.20;
	const REGRESSION_THRESHOLD: f64 = 0.10;
	if spread_ratio > STABILITY_THRESHOLD {
		push_summary_flag(&mut flags, "unstable");
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
			let band = REGRESSION_THRESHOLD.max(2.0 * spread_ratio.max(baseline_spread));
			if baseline_median > 0.0 && (execute_median - baseline_median) / baseline_median > band
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
		"status": status,
		"runs": runs,
		"warmup": warmup,
		"execute_ms_median": execute_median,
		"execute_ms_min": execute_min,
		"execute_ms_max": execute_max,
		"execute_ms_spread_ratio": spread_ratio,
		"peak_rss_kib_max": peak_rss_kib_max,
		"spill_bytes_max": spill_bytes_max,
		"plan_fingerprint": plan_fingerprint,
		"digest": digest,
		"output_row_count": output_row_count,
		"output_schema_fingerprint": output_schema_fingerprint,
		"engine_version": engine_version,
		"duckdb_version": duckdb_version,
		"baseline_execute_ms_median": baseline_execute_ms_median,
		"flags": flags,
	})
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
				"perf baseline not recorded: summary rows must be release, passing, and stable"
			);
		}
	}
	Ok(())
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
					.any(|flag| flag == "debug_build" || flag == "unstable")
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
		"usage: toron-tpch <status|prepare|run|matrix|correctness|correctness-matrix|perf> [--query q06|all] [--mode dev|strict] [--sf 0.01] [--allow-blocked] [--runs 7] [--warmup 1] [--engines toron|rune|duckdb|polars_sql|toron,duckdb] [--data-source memory|parquet] [--data-dir target/tpch/data] [--output-dir target/tpch/perf]"
	);
	ExitCode::from(2)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn q17_correctness_scale_policy_rejects_masked_empty_scale() {
		assert_eq!(q17_correctness_sf(None).unwrap(), Q17_CORRECTNESS_SCALE);
		assert_eq!(
			q17_correctness_sf(Some(Q17_CORRECTNESS_SCALE)).unwrap(),
			Q17_CORRECTNESS_SCALE
		);
		assert!(q17_correctness_sf(Some(0.01)).is_err());
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
