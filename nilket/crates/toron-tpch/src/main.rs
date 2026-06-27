//! ADR 261: TPC-H Reachability Harness CLI — `status` / `prepare` / `run`, manual arg parsing (no clap,
//! matching Toron's no-extra-framework style). An unknown command/arg is a *harness* error (exit 2),
//! distinct from a query `blocked`/`fail` (which is structured telemetry).

use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::time::Instant;

use toron_tpch::telemetry::{Attempt, CorrectnessAttempt, Phase, Status};
use toron_tpch::{
	ALL_QUERY_IDS, CACHE_ROOT, Q17_CORRECTNESS_SCALE, check_strict_q1, check_strict_q02,
	check_strict_q3, check_strict_q04, check_strict_q5, check_strict_q6, check_strict_q07,
	check_strict_q08, check_strict_q09, check_strict_q10, check_strict_q11, check_strict_q12,
	check_strict_q13, check_strict_q14, check_strict_q15, check_strict_q16, check_strict_q17,
	check_strict_q18, check_strict_q19, check_strict_q20, check_strict_q21, check_strict_q22,
	correctness_matrix, generate_dev_lineitem, matrix, parse_scale_factor, run_dev_q1, run_dev_q02,
	run_dev_q3, run_dev_q04, run_dev_q05, run_dev_q6, run_dev_q07, run_dev_q08, run_dev_q09,
	run_dev_q10, run_dev_q11, run_dev_q12, run_dev_q13, run_dev_q14, run_dev_q15, run_dev_q16,
	run_dev_q17, run_dev_q18, run_dev_q19, run_dev_q20, run_dev_q21, run_dev_q22, run_perf_query,
	run_strict_q1, run_strict_q02, run_strict_q3, run_strict_q04, run_strict_q05, run_strict_q6,
	run_strict_q07, run_strict_q08, run_strict_q09, run_strict_q10, run_strict_q11, run_strict_q12,
	run_strict_q13, run_strict_q14, run_strict_q15, run_strict_q16, run_strict_q17, run_strict_q18,
	run_strict_q19, run_strict_q20, run_strict_q21, run_strict_q22, status_rows,
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
	let sf = match explicit_sf(args) {
		Ok(explicit) => explicit.unwrap_or(1.0),
		Err(message) => return harness_error(&message),
	};
	let run_index = match parse_u32_flag(args, "--run-index", 0) {
		Ok(run_index) => run_index,
		Err(message) => return harness_error(&message),
	};
	let warmup = args.iter().any(|arg| arg == "--warmup-run");
	let attempt = match run_perf_query(query, mode, sf, run_index, warmup).await {
		Ok(attempt) => attempt,
		Err(message) => return harness_error(&message),
	};
	println!("{}", attempt.jsonl());
	ExitCode::SUCCESS
}

async fn perf_command(args: &[String]) -> ExitCode {
	let query = flag_value(args, "--query").unwrap_or("all");
	let mode = flag_value(args, "--mode").unwrap_or("strict");
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
	let queries = match perf_query_list(query) {
		Ok(queries) => queries,
		Err(message) => return harness_error(&message),
	};
	let mut raw_rows = Vec::new();
	for query_id in queries {
		for warmup_index in 0..warmup {
			match run_perf_child(query_id, mode, sf, warmup_index as u32, true) {
				Ok(row) => raw_rows.push(row),
				Err(message) => return harness_error(&message),
			}
		}
		for run_index in 0..runs {
			match run_perf_child(query_id, mode, sf, run_index as u32, false) {
				Ok(row) => raw_rows.push(row),
				Err(message) => return harness_error(&message),
			}
		}
	}

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
	let explicit = match explicit_sf(args) {
		Ok(explicit) => explicit,
		Err(message) => return harness_error(&message),
	};
	let sf = explicit.unwrap_or(DEFAULT_SF);
	let attempt = match query {
		"q17" => {
			if let Err(message) = q17_correctness_sf(explicit) {
				return harness_error(&message);
			}
			check_strict_q17().await
		}
		"q06" | "q6" => check_strict_q6(sf).await,
		"q01" | "q1" => check_strict_q1(sf).await,
		"q02" | "q2" => check_strict_q02(sf).await,
		"q03" | "q3" => check_strict_q3(sf).await,
		"q04" | "q4" => check_strict_q04(sf).await,
		"q05" | "q5" => check_strict_q5(sf).await,
		"q07" | "q7" => check_strict_q07(sf).await,
		"q08" | "q8" => check_strict_q08(sf).await,
		"q09" | "q9" => check_strict_q09(sf).await,
		"q10" => check_strict_q10(sf).await,
		"q11" => check_strict_q11(sf).await,
		"q12" => check_strict_q12(sf).await,
		"q13" => check_strict_q13(sf).await,
		"q14" => check_strict_q14(sf).await,
		"q15" => check_strict_q15(sf).await,
		"q18" => check_strict_q18(sf).await,
		"q19" => check_strict_q19(sf).await,
		"q21" => check_strict_q21(sf).await,
		"q16" => check_strict_q16(sf).await,
		"q20" => check_strict_q20(sf).await,
		"q22" => check_strict_q22(sf).await,
		other => return harness_error(&format!("correctness --query {other} is not supported")),
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
			"q17 correctness requires --sf {Q17_CORRECTNESS_SCALE}; got {sf}"
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
	query: &str,
	mode: &str,
	sf: f64,
	run_index: u32,
	warmup: bool,
) -> Result<serde_json::Value, String> {
	let mut args = vec![
		"perf-run-one".to_string(),
		"--query".to_string(),
		query.to_string(),
		"--mode".to_string(),
		mode.to_string(),
		"--sf".to_string(),
		sf.to_string(),
		"--run-index".to_string(),
		run_index.to_string(),
	];
	if warmup {
		args.push("--warmup-run".to_string());
	}
	run_perf_child_args(&args)
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
	let mut by_query: BTreeMap<String, Vec<&serde_json::Value>> = BTreeMap::new();
	for row in raw_rows {
		if row
			.get("warmup")
			.and_then(serde_json::Value::as_bool)
			.unwrap_or(false)
		{
			continue;
		}
		if let Some(query) = row.get("query").and_then(serde_json::Value::as_str) {
			by_query.entry(query.to_string()).or_default().push(row);
		}
	}
	by_query
		.into_iter()
		.map(|(query, rows)| {
			summarize_query_rows(&query, rows, runs, warmup, baseline_rows.get(&query))
		})
		.collect()
}

fn summarize_query_rows(
	query: &str,
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
	} else {
		"fail"
	};
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
		flags.push("unstable");
	}
	if profile != "release" {
		flags.push("debug_build");
	}
	let mut baseline_execute_ms_median = None;
	if let Some(baseline) = baseline {
		let baseline_plan = baseline
			.get("plan_fingerprint")
			.and_then(serde_json::Value::as_str)
			.unwrap_or("none");
		if baseline_plan != plan_fingerprint {
			flags.push("plan_changed");
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
				flags.push("slower_than_baseline");
			}
		}
	}

	serde_json::json!({
		"suite": "tpch_perf_summary",
		"engine": "toron",
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
		"baseline_execute_ms_median": baseline_execute_ms_median,
		"flags": flags,
	})
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
			row.get("query")
				.and_then(serde_json::Value::as_str)
				.map(|query| (query.to_string(), row.clone()))
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
		"tpch_perf query={query} status={status} execute_ms_median={median:.1} execute_ms_min={min:.1} spread={spread:.3} peak_rss_kib={rss} spill_bytes={spill} flags={}",
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
		"usage: toron-tpch <status|prepare|run|matrix|correctness|correctness-matrix|perf> [--query q06|all] [--mode dev|strict] [--sf 0.01] [--allow-blocked] [--runs 7] [--warmup 1] [--output-dir target/tpch/perf]"
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
		baseline_rows.insert("q06".to_string(), baseline);

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
		baseline_rows.insert("q06".to_string(), baseline);

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

	fn perf_row(
		query: &str,
		execute_ms: f64,
		plan_fingerprint: &str,
		release: bool,
	) -> serde_json::Value {
		serde_json::json!({
			"suite": "tpch_perf",
			"engine": "toron",
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
			"flags": flags,
		})
	}
}
