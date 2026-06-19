//! ADR 261: TPC-H Reachability Harness CLI — `status` / `prepare` / `run`, manual arg parsing (no clap,
//! matching Toron's no-extra-framework style). An unknown command/arg is a *harness* error (exit 2),
//! distinct from a query `blocked`/`fail` (which is structured telemetry).

use std::process::ExitCode;

use toron_tpch::telemetry::{Attempt, Phase, Status};
use toron_tpch::{
	CACHE_ROOT, generate_dev_lineitem, matrix, parse_scale_factor, resolve_q14_scale, run_dev_q1,
	run_dev_q6, run_dev_q14, run_strict_q1, run_strict_q6, run_strict_q14, status_rows,
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
		_ => usage(),
	}
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
		// ADR 266 adds Q1; the other 20 are inventoried but not yet attempted.
		"q01" | "q1" => {
			if strict {
				run_strict_q1(sf).await
			} else {
				run_dev_q1(sf).await
			}
		}
		// ADR 269 adds q14 (the first multi-table query) as a reachability attempt. It rides a broadcast
		// cartesian until comma-join recognition (ADR 270), so it is scale-capped: default to the gauge
		// scale and refuse a larger explicit --sf (a harness error, exit 2) rather than hang.
		"q14" => match resolve_q14_scale(explicit) {
			Ok(sf) if strict => run_strict_q14(sf).await,
			Ok(sf) => run_dev_q14(sf).await,
			Err(message) => return harness_error(&message),
		},
		_ => Attempt {
			query: query.to_string(),
			scale_factor: sf,
			mode: mode.to_string(),
			phase: Phase::Plan,
			status: Status::NotAttempted,
			reason: Some("query_not_implemented".to_string()),
			elapsed_ms: 0.0,
			digest: None,
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

fn flag_value<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
	args.iter()
		.position(|arg| arg == flag)
		.and_then(|index| args.get(index + 1))
		.map(String::as_str)
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
		"usage: toron-tpch <status|prepare|run|matrix> [--query q06] [--mode dev|strict] [--sf 0.01] [--allow-blocked]"
	);
	ExitCode::from(2)
}
