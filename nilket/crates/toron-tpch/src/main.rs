//! ADR 261: TPC-H Reachability Harness CLI — `status` / `prepare` / `run`, manual arg parsing (no clap,
//! matching Toron's no-extra-framework style). An unknown command/arg is a *harness* error (exit 2),
//! distinct from a query `blocked`/`fail` (which is structured telemetry).

use std::process::ExitCode;

use toron_tpch::telemetry::{Attempt, CorrectnessAttempt, Phase, Status};
use toron_tpch::{
	CACHE_ROOT, Q17_CORRECTNESS_SCALE, check_strict_q1, check_strict_q02, check_strict_q3,
	check_strict_q04, check_strict_q5, check_strict_q6, check_strict_q07, check_strict_q08,
	check_strict_q09, check_strict_q10, check_strict_q12, check_strict_q14, check_strict_q17,
	check_strict_q20, check_strict_q22, correctness_matrix, generate_dev_lineitem, matrix,
	parse_scale_factor, run_dev_q1, run_dev_q02, run_dev_q3, run_dev_q04, run_dev_q05, run_dev_q6,
	run_dev_q07, run_dev_q08, run_dev_q09, run_dev_q10, run_dev_q12, run_dev_q13, run_dev_q14,
	run_dev_q17, run_dev_q20, run_dev_q22, run_strict_q1, run_strict_q02, run_strict_q3,
	run_strict_q04, run_strict_q05, run_strict_q6, run_strict_q07, run_strict_q08, run_strict_q09,
	run_strict_q10, run_strict_q12, run_strict_q13, run_strict_q14, run_strict_q17, run_strict_q20,
	run_strict_q22, status_rows,
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
		"q12" => check_strict_q12(sf).await,
		"q14" => check_strict_q14(sf).await,
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
		// Correlated-scalar-subquery reachability attempt: part + a scalar subquery over lineitem.
		"q17" => {
			if strict {
				run_strict_q17(sf).await
			} else {
				run_dev_q17(sf).await
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
		"usage: toron-tpch <status|prepare|run|matrix|correctness|correctness-matrix> [--query q06] [--mode dev|strict] [--sf 0.01] [--allow-blocked]"
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
}
