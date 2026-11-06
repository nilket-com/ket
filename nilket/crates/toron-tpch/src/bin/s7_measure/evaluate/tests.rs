//! Evaluator controls over small hand-written fixtures. Each fixture is one
//! valid fix-arm block (a PASS); every test changes exactly one thing and
//! asserts the verdict and the reason it names.

use super::*;
use crate::drive::BatchRecord;

const S: u64 = 1_000_000_000;
const MSEC: u64 = 1_000_000;
const T0: u64 = 1_000 * S;
const W_START: u64 = T0;
const W_STOP: u64 = T0 + 50 * S + 100 * MSEC;
const DRAINED: u64 = T0 + 50 * S + 500 * MSEC;
const R_REQ: u64 = T0 + 51 * S;
const PRE: u64 = R_REQ + 2 * MSEC;
const R_SEEN: u64 = R_REQ + 10 * MSEC;
const MS: u64 = T0 + 52 * S;
const DEADLINE: u64 = MS + 700 * S;
const F_REQ: u64 = DEADLINE + MSEC;
const ME: u64 = F_REQ + 2 * MSEC;
const P99: &str = "at every measured capture: lineages over cap/4 * 100 <= lineages";
/// A tick well inside the measured window.
const MIDDLE_TICK: u64 = 40;
const PID: u64 = 4242;
const START: u64 = 777;
const SHA: &str = "manifest-sha";
const BLOCK: &str = "20261102-b1-fix-TEST-abcd";

fn manifest() -> Manifest {
	Manifest {
		format_version: 1,
		label: "TEST".into(),
		scale_factor: 1.0,
		target_rows_per_sec: 2500,
		batch_rows: 4096,
		max_in_flight: 4,
		warmup_secs: 50,
		measured_secs: 700,
		tenant_id: 1,
		node_id: 1,
		slab_max_bytes: 4 << 20,
		sweeper_scan_interval_ms: 1000,
		sweeper_min_slab_bytes: 0,
		cluster_retention_window_secs: 15,
		tick_secs: 10,
		tick_late_tolerance_ms: 2000,
		tick_missing_tolerance: 0,
		ack_timeout_secs: 30,
		drain_timeout_secs: 120,
		node_ready_timeout_secs: 180,
		diagnostic_quiesce_secs: 0,
		setup_timeout_secs: 120,
		dataset_allowance_secs: 900,
		node_shutdown_timeout_secs: 120,
		phase_timing_allowance_ms: 2000,
		rate_window_secs: 60,
		rate_window_step_secs: 10,
		rate_floor_fraction: 0.9,
		rss_window_secs: 300,
		rss_window_min_samples: 29,
		rss_ratio_limit: 1.10,
		lineage_quarter_cap: 262_144,
	}
}

/// The counters and instrument of one scrape.
#[derive(Clone)]
struct Values {
	collision: u64,
	redirect_skips: u64,
	tiered: u64,
	pruned: u64,
	peak: u64,
	headroom: u64,
	limit: u64,
	/// (cumulative at le=262144, cumulative at +Inf)
	census: (u64, u64),
	/// Base: no §7 counters and no instrument.
	base: bool,
}

const BEFORE: Values = Values {
	collision: 0,
	redirect_skips: 0,
	tiered: 10,
	pruned: 100,
	peak: 1_000,
	headroom: 9_000,
	limit: 10_000,
	census: (40, 40),
	base: false,
};

fn after() -> Values {
	Values {
		tiered: 80,
		pruned: 9_000,
		census: (99, 100),
		..BEFORE
	}
}

/// A hand-written scrape: the families the evaluator binds, as the node
/// renders them.
fn prom(v: &Values) -> String {
	let mut out = format!(
		"# HELP wendelon_storage_tiered_slabs_total Sealed slabs tiered from L1 to L2.\n\
		 # TYPE wendelon_storage_tiered_slabs_total counter\n\
		 wendelon_storage_tiered_slabs_total {tiered}\n\
		 # TYPE wendelon_storage_table_retention_l2_rows_pruned_total counter\n\
		 wendelon_storage_table_retention_l2_rows_pruned_total{{kind=\"data\"}} {pruned}\n\
		 wendelon_storage_table_retention_l2_rows_pruned_total{{kind=\"tombstone\"}} 0\n\
		 # TYPE wendelon_storage_table_retention_l2_bytes_reclaimed_total counter\n\
		 wendelon_storage_table_retention_l2_bytes_reclaimed_total {bytes}\n",
		tiered = v.tiered,
		pruned = v.pruned,
		bytes = v.pruned * 10,
	);
	if v.base {
		return out;
	}
	out.push_str(&format!(
		"# TYPE wendelon_tier_rewrite_refusals_total counter\n\
		 wendelon_tier_rewrite_refusals_total{{reason=\"layout_collision\"}} {collision}\n\
		 wendelon_tier_rewrite_refusals_total{{reason=\"lineage_history_cap\"}} 0\n\
		 wendelon_tier_rewrite_refusals_total{{reason=\"node_history_budget\"}} 0\n\
		 wendelon_tier_rewrite_refusals_total{{reason=\"stale_plan\"}} 0\n\
		 wendelon_tier_rewrite_refusals_total{{reason=\"slab_retired\"}} 0\n\
		 # TYPE wendelon_tier_publication_fenced_total counter\n\
		 wendelon_tier_publication_fenced_total{{reason=\"no_budget\"}} 0\n\
		 wendelon_tier_publication_fenced_total{{reason=\"no_scratch\"}} 0\n\
		 wendelon_tier_publication_fenced_total{{reason=\"l2_unavailable\"}} 0\n\
		 # TYPE wendelon_relocation_redirects_skipped_total counter\n\
		 wendelon_relocation_redirects_skipped_total {skips}\n\
		 # TYPE wendelon_test_tier_lineage_entries histogram\n\
		 wendelon_test_tier_lineage_entries_bucket{{le=\"0\"}} 0\n\
		 wendelon_test_tier_lineage_entries_bucket{{le=\"2\"}} {small}\n\
		 wendelon_test_tier_lineage_entries_bucket{{le=\"262144\"}} {at}\n\
		 wendelon_test_tier_lineage_entries_bucket{{le=\"2097152\"}} {inf}\n\
		 wendelon_test_tier_lineage_entries_bucket{{le=\"+Inf\"}} {inf}\n\
		 wendelon_test_tier_lineage_entries_count {inf}\n\
		 # TYPE wendelon_test_tier_lineages_over_quarter_cap gauge\n\
		 wendelon_test_tier_lineages_over_quarter_cap {over}\n\
		 # TYPE wendelon_test_tier_budget_block_peak_bytes gauge\n\
		 wendelon_test_tier_budget_block_peak_bytes {peak}\n\
		 # TYPE wendelon_test_tier_budget_block_min_headroom_bytes gauge\n\
		 wendelon_test_tier_budget_block_min_headroom_bytes {head}\n\
		 # TYPE wendelon_test_tier_budget_limit_bytes gauge\n\
		 wendelon_test_tier_budget_limit_bytes {limit}\n",
		collision = v.collision,
		skips = v.redirect_skips,
		at = v.census.0,
		small = v.census.0.min(1),
		inf = v.census.1,
		over = v.census.1 - v.census.0,
		peak = v.peak,
		head = v.headroom,
		limit = v.limit,
	));
	out
}

/// One block's records, built valid; tests mutate them before evaluating.
struct Built {
	lines: Vec<Value>,
	driver: Value,
	process: Value,
	batches: Vec<BatchRecord>,
}

struct Fixture {
	arm: &'static str,
	pre: Values,
	post: Values,
	fin: Values,
	rss: fn(u64) -> u64,
	drop_tick: Option<u64>,
	pid_change_at_tick: Option<u64>,
	/// (tick index, scrape) replacing that tick's scrape.
	tick_override: Option<(u64, Values)>,
	batch_error: bool,
	/// A raw text replacement in the final scrape.
	final_edit: Option<(&'static str, &'static str)>,
	mutate: fn(&mut Built),
}

fn nothing(_: &mut Built) {}

impl Fixture {
	fn fix() -> Self {
		Self {
			arm: "fix",
			pre: BEFORE,
			post: BEFORE,
			fin: after(),
			rss: |_| 100_000,
			drop_tick: None,
			pid_change_at_tick: None,
			tick_override: None,
			batch_error: false,
			final_edit: None,
			mutate: nothing,
		}
	}

	fn build(&self) -> Built {
		let base = self.arm == "base";
		let mark = |mut v: Values| {
			v.base |= base;
			v
		};
		let mut lines = vec![
			json!({"kind": "start", "arm": self.arm, "label": "TEST", "t_mono_ns": T0 - S,
				"block_id": BLOCK, "manifest_sha256": SHA}),
			json!({"kind": "ready", "t_mono_ns": T0}),
		];
		let mut reset_written = false;
		for i in 0..=75u64 {
			let t = T0 + i * 10 * S;
			if !reset_written && t > PRE {
				lines.push(json!({
					"kind": "reset_ack", "seq": 1, "t_signal_ns": R_REQ + MSEC, "t_pre_ns": PRE,
					"pre_scrape": prom(&mark(self.pre.clone())),
					"t_reset_ns": PRE + 1, "t_post_ns": PRE + 2,
					"reset_done": if base { json!("N/A") } else { json!(true) },
					"post_scrape": prom(&mark(self.post.clone())), "t_ack_ns": PRE + 3,
				}));
				reset_written = true;
			}
			if self.drop_tick == Some(i) {
				continue;
			}
			let values = match &self.tick_override {
				Some((at, v)) if *at == i => v.clone(),
				_ if t > PRE => self.post.clone(),
				_ => self.pre.clone(),
			};
			let mut tick = json!({
				"kind": "tick", "index": i, "t_sched_ns": t, "t_mono_ns": t + 1_000_000,
				"rss_kib": (self.rss)(t), "metrics": prom(&mark(values)),
			});
			if self.pid_change_at_tick == Some(i) {
				tick["pid"] = json!(PID + 1);
			}
			lines.push(tick);
		}
		let mut final_text = prom(&mark(self.fin.clone()));
		if let Some((from, to)) = self.final_edit {
			assert!(
				final_text.contains(from),
				"fixture edit {from:?} must apply"
			);
			final_text = final_text.replace(from, to);
		}
		lines.push(json!({
			"kind": "final", "seq": 2, "t_signal_ns": F_REQ + MSEC / 2, "t_mono_ns": ME,
			"rss_kib": (self.rss)(ME), "metrics": final_text, "t_ack_ns": ME + 1,
		}));
		lines
			.push(json!({"kind": "shutdown_begin", "t_mono_ns": ME + 20 * S, "reason": "SIGTERM"}));
		lines.push(
			json!({"kind": "shutdown_done", "t_mono_ns": ME + 21 * S, "ok": true, "error": null}),
		);
		for l in &mut lines {
			l.as_object_mut()
				.unwrap()
				.entry("pid")
				.or_insert(json!(PID));
			l["pid_start_time"] = json!(START);
		}

		let interval = manifest().batch_interval_ns();
		let mut batches = Vec::new();
		let mut admitted = BTreeMap::new();
		let mut index = 0;
		for (phase, from, to) in [("warmup", W_START, W_STOP - 2 * S), ("measured", MS, ME)] {
			let mut slot = 0;
			while from + slot * interval < to {
				let admit = from + slot * interval;
				let bad = self.batch_error && index == 7;
				batches.push(BatchRecord {
					index,
					phase: phase.into(),
					slot,
					rows: 4096,
					t_sched_ns: admit,
					t_admit_ns: admit,
					t_ack_ns: admit + S / 5,
					ok: !bad,
					duplicate: false,
					error: bad.then(|| "write failed".into()),
					in_flight_at_admit: 1,
					completed: true,
				});
				*admitted.entry(phase.to_string()).or_insert(0u64) += 1;
				index += 1;
				slot += 1;
			}
		}
		let driver = json!({
			"arm": self.arm,
			"block_id": BLOCK,
			"manifest_sha256": SHA,
			"aborted": null,
			"node": {"pid": PID, "pid_start_time": START},
			"phases": {
				"warmup": {"t_start_ns": W_START, "t_admission_stopped_ns": W_STOP, "t_drained_ns": DRAINED},
				"reset": {"seq": 1, "t_request_ns": R_REQ, "t_ack_seen_ns": R_SEEN},
				"measured": {"t_start_ns": MS, "t_deadline_ns": DEADLINE},
				"final": {"seq": 2, "t_request_ns": F_REQ, "t_ack_seen_ns": ME + 10 * MSEC,
					"in_flight_at_request": 1, "in_flight_at_ack": 1},
				"join": {"t_joined_ns": ME + S},
			},
			"totals": {"admitted": admitted, "dataset_exhausted_at_ns": null},
		});
		let process = json!({
			"block_id": BLOCK, "manifest_sha256": SHA,
			"drive_exit_code": 0, "node_exit_code": 0, "node_stop": "sigterm",
			"node_pid": PID, "node_pid_start_time": START,
		});
		Built {
			lines,
			driver,
			process,
			batches,
		}
	}

	fn run(&self) -> Value {
		let mut b = self.build();
		(self.mutate)(&mut b);
		let jsonl: String = b.lines.iter().map(|l| format!("{l}\n")).collect();
		let batches: String = b
			.batches
			.iter()
			.map(|r| format!("{}\n", serde_json::to_string(r).unwrap()))
			.collect();
		evaluate(&Inputs {
			arm: self.arm,
			manifest: &manifest(),
			manifest_sha256: SHA,
			block_id: BLOCK,
			observations: &jsonl,
			driver: &b.driver,
			batches: &batches,
			process: &b.process,
		})
	}
}

fn verdict(v: &Value) -> &str {
	v["verdict"].as_str().unwrap()
}

fn reasons(v: &Value) -> String {
	v["invalid_reasons"].to_string()
}

fn assert_invalid(f: Fixture, needle: &str) {
	let v = f.run();
	assert_eq!(verdict(&v), "INVALID", "{v:#}");
	assert!(
		reasons(&v).contains(needle),
		"expected {needle:?} in {}",
		reasons(&v)
	);
}

#[test]
fn a_clean_fix_block_passes() {
	let v = Fixture::fix().run();
	assert_eq!(verdict(&v), "PASS", "{v:#}");
	assert_eq!(v["evidence"], false, "a TEST label is never evidence");
	assert_eq!(
		v["predicates"][P99]["value"]["breaching_captures"],
		json!(0),
		"1 of 100 above cap/4 is exactly the bound"
	);
	assert_eq!(v["detail"]["census"]["final_over"], json!(1));
	assert_eq!(v["detail"]["census"]["final_count"], json!(100));
}

#[test]
fn a_layout_collision_delta_fails() {
	let mut f = Fixture::fix();
	f.fin.collision = 1;
	let v = f.run();
	assert_eq!(verdict(&v), "FAIL", "{v:#}");
	assert!(
		v["failed_predicates"]
			.to_string()
			.contains("layout_collision")
	);
}

#[test]
fn a_redirect_skip_fails() {
	let mut f = Fixture::fix();
	f.fin.redirect_skips = 1;
	let v = f.run();
	assert_eq!(verdict(&v), "FAIL", "{v:#}");
	assert!(
		v["failed_predicates"]
			.to_string()
			.contains("redirects_skipped")
	);
}

#[test]
fn a_peak_at_the_limit_fails() {
	let mut f = Fixture::fix();
	f.fin.peak = f.fin.limit;
	let v = f.run();
	assert_eq!(verdict(&v), "FAIL", "{v:#}");
	assert_eq!(v["failed_predicates"], json!(["block peak < limit"]));
}

#[test]
fn p99_fails_by_exact_count() {
	let mut f = Fixture::fix();
	f.fin.census = (98, 100);
	let v = f.run();
	assert_eq!(verdict(&v), "FAIL", "2 of 100 above cap/4: {v:#}");
	assert_eq!(v["failed_predicates"], json!([P99]));
}

#[test]
fn an_rss_ratio_at_the_limit_fails() {
	let mut f = Fixture::fix();
	f.rss = |t| if t > ME - 300 * S { 110_000 } else { 100_000 };
	let v = f.run();
	assert_eq!(verdict(&v), "FAIL", "{v:#}");
	assert_eq!(
		v["failed_predicates"],
		json!(["rss median last/first window < limit"])
	);
}

#[test]
fn a_missing_tick_is_invalid() {
	let mut f = Fixture::fix();
	f.drop_tick = Some(30);
	let v = f.run();
	assert_eq!(verdict(&v), "INVALID");
	assert!(reasons(&v).contains("missing ticks"), "{v:#}");
}

#[test]
fn a_pid_change_is_invalid() {
	let mut f = Fixture::fix();
	f.pid_change_at_tick = Some(40);
	let v = f.run();
	assert_eq!(verdict(&v), "INVALID");
	assert!(
		reasons(&v).contains("has another or no process identity"),
		"{v:#}"
	);
}

#[test]
fn a_counter_reset_is_invalid() {
	let mut f = Fixture::fix();
	f.tick_override = Some((
		40,
		Values {
			tiered: 0,
			..BEFORE
		},
	));
	let v = f.run();
	assert_eq!(verdict(&v), "INVALID");
	assert!(
		reasons(&v).contains("wendelon_storage_tiered_slabs_total decreased"),
		"{v:#}"
	);
}

#[test]
fn an_idle_block_is_invalid_not_pass() {
	let mut f = Fixture::fix();
	f.fin.tiered = f.pre.tiered;
	let v = f.run();
	assert_eq!(verdict(&v), "INVALID");
	assert!(reasons(&v).contains("no first tiering"), "{v:#}");

	let mut f = Fixture::fix();
	f.fin.pruned = f.pre.pruned;
	let v = f.run();
	assert_eq!(verdict(&v), "INVALID");
	assert!(reasons(&v).contains("no L2 retention pruned rows"), "{v:#}");
}

/// The amendment of 2026-11-06, which replaces the frozen rule's `an_empty_census_is_invalid`:
/// an empty final capture alone is no longer INVALID, and it is reported.
#[test]
fn an_empty_final_capture_alone_is_valid_and_disclosed() {
	let mut f = Fixture::fix();
	f.fin.census = (0, 0);
	let v = f.run();
	assert_eq!(verdict(&v), "PASS", "{v:#}");
	assert_eq!(v["detail"]["census"]["final_count"], json!(0));
	let captures = v["detail"]["census"]["captures"].as_u64().unwrap();
	assert_eq!(
		v["detail"]["census"]["nonempty_captures"].as_u64().unwrap(),
		captures - 1,
		"occupancy is disclosed"
	);
}

/// What the frozen rule's empty case still means: no capture in the window had a lineage.
#[test]
fn no_lineage_at_any_measured_capture_is_invalid() {
	let mut f = Fixture::fix();
	f.post.census = (0, 0);
	f.fin.census = (0, 0);
	assert_invalid(f, "no measured capture has a lineage");
}

/// A capture before the measured window is warm-up: it neither fails nor occupies the census.
#[test]
fn a_breach_before_the_measured_window_is_not_counted() {
	let mut f = Fixture::fix();
	f.pre.census = (0, 100);
	let v = f.run();
	assert_eq!(verdict(&v), "PASS", "{v:#}");
}

/// The final capture no longer speaks for the window: a breach anywhere in it fails.
#[test]
fn a_breach_at_a_middle_capture_fails() {
	let mut f = Fixture::fix();
	f.tick_override = Some((
		MIDDLE_TICK,
		Values {
			census: (98, 100),
			..f.post.clone()
		},
	));
	let v = f.run();
	assert_eq!(verdict(&v), "FAIL", "{v:#}");
	assert_eq!(v["failed_predicates"], json!([P99]));
	assert_eq!(v["detail"]["census"]["breaching_captures"], json!(1));
}

/// A malformed census at any measured capture is refused, not skipped.
#[test]
fn a_malformed_middle_capture_is_invalid() {
	let mut f = Fixture::fix();
	f.mutate = |b| {
		let tick = b
			.lines
			.iter_mut()
			.find(|l| l["kind"] == "tick" && l["index"] == json!(MIDDLE_TICK))
			.unwrap();
		let text = tick["metrics"].as_str().unwrap().replace(
			"wendelon_test_tier_lineages_over_quarter_cap 0",
			"wendelon_test_tier_lineages_over_quarter_cap 1",
		);
		tick["metrics"] = json!(text);
	};
	assert_invalid(f, "derived over-count 0 != gauge 1");
}

#[test]
fn a_workload_error_is_invalid() {
	let mut f = Fixture::fix();
	f.batch_error = true;
	let v = f.run();
	assert_eq!(verdict(&v), "INVALID");
	assert!(reasons(&v).contains("1 workload errors"), "{v:#}");
}

#[test]
fn base_reports_absent_fields_as_na_and_never_passes() {
	let mut f = Fixture::fix();
	f.arm = "base";
	let v = f.run();
	assert_eq!(verdict(&v), "NO_FAILURE_ON_PRESENT_FIELDS", "{v:#}");
	for (name, p) in v["predicates"].as_object().unwrap() {
		if name.starts_with("rss") {
			assert_eq!(p["verdict"], "pass", "{name}: base RSS is a present field");
		} else {
			assert_eq!(p["verdict"], "n/a", "{name}: absent on base is n/a");
			assert!(p["value"].is_null(), "{name}: n/a is never a 0");
		}
	}
	// Its present-field failures are preserved.
	f.rss = |t| if t > ME - 300 * S { 120_000 } else { 100_000 };
	assert_eq!(verdict(&f.run()), "FAIL");
}

#[test]
fn fix_metrics_missing_on_the_fix_arm_is_invalid() {
	let mut f = Fixture::fix();
	f.pre.base = true;
	let v = f.run();
	assert_eq!(verdict(&v), "INVALID");
	assert!(reasons(&v).contains("fix metric"), "{v:#}");
}

/// An event between the pre-reset scrape and the measured start counts: the
/// baseline is the PRE-reset scrape, not the post-reset one.
#[test]
fn deltas_use_the_pre_reset_baseline() {
	let mut f = Fixture::fix();
	f.post.collision = 1;
	f.fin.collision = 1;
	let v = f.run();
	assert_eq!(verdict(&v), "FAIL", "{v:#}");
	assert_eq!(
		v["predicates"]["delta wendelon_tier_rewrite_refusals_total{reason=\"layout_collision\"} == 0"]
			["value"],
		json!(1)
	);
}

// --- Item 1: process and collection outcomes govern the verdict -----------

#[test]
fn a_node_killed_after_a_valid_final_capture_is_invalid() {
	let mut f = Fixture::fix();
	f.mutate = |b| {
		b.process["node_stop"] = json!("sigkill-after-120s");
		b.process["node_exit_code"] = json!(137);
		b.lines.retain(|l| l["kind"] != "shutdown_done");
	};
	let v = f.run();
	assert_eq!(verdict(&v), "INVALID", "{v:#}");
	let r = reasons(&v);
	assert!(
		r.contains("node exited 137") && r.contains("sigkill"),
		"{r}"
	);
	assert!(r.contains("terminal evidence"), "{r}");
}

#[test]
fn a_failed_shutdown_after_a_valid_final_capture_is_invalid() {
	let mut f = Fixture::fix();
	f.mutate = |b| {
		let done = b.lines.last_mut().unwrap();
		done["ok"] = json!(false);
		done["error"] = json!("storage drain timed out");
	};
	assert_invalid(f, "node shutdown failed");
}

#[test]
fn a_missing_shutdown_done_is_invalid() {
	let mut f = Fixture::fix();
	f.mutate = |b| b.lines.retain(|l| l["kind"] != "shutdown_done");
	assert_invalid(f, "terminal evidence");
}

#[test]
fn a_driver_failure_after_a_valid_final_capture_is_invalid() {
	let mut f = Fixture::fix();
	f.mutate = |b| b.process["drive_exit_code"] = json!(1);
	assert_invalid(f, "driver exited 1");
}

#[test]
fn missing_process_outcomes_are_invalid() {
	let mut f = Fixture::fix();
	f.mutate = |b| b.process = json!({"unreadable": "no such file"});
	assert_invalid(f, "process/drive_exit_code: missing");
}

// --- Item 2: dropped work is never acknowledged ---------------------------

#[test]
fn an_incomplete_batch_is_invalid() {
	let mut f = Fixture::fix();
	f.mutate = |b| {
		let last = b.batches.last_mut().unwrap();
		last.completed = false;
		last.ok = false;
	};
	assert_invalid(f, "1 batches never completed");
}

#[test]
fn batch_records_missing_against_the_admitted_count_are_invalid() {
	let mut f = Fixture::fix();
	f.mutate = |b| {
		b.batches.pop();
	};
	assert_invalid(f, "recorded");
}

// --- Item 3: declared durations, ordering and typed identity --------------

#[test]
fn a_shortened_measured_interval_is_invalid() {
	let mut f = Fixture::fix();
	// Everything else valid; the final arrives 100 s before the declared
	// 700 s deadline (the driver's own deadline field says so too).
	f.mutate = |b| b.driver["phases"]["measured"]["t_deadline_ns"] = json!(MS + 600 * S);
	assert_invalid(f, "measured deadline");
	let mut f = Fixture::fix();
	f.mutate = |b| b.driver["phases"]["final"]["t_request_ns"] = json!(DEADLINE - 100 * S);
	assert_invalid(f, "final requested");
}

#[test]
fn a_shortened_warmup_is_invalid() {
	let mut f = Fixture::fix();
	f.mutate = |b| b.driver["phases"]["warmup"]["t_admission_stopped_ns"] = json!(W_START + 30 * S);
	assert_invalid(f, "warm-up lasted 30000 ms");
}

#[test]
fn an_overlong_final_beyond_the_allowance_is_invalid() {
	let mut f = Fixture::fix();
	f.mutate = |b| b.driver["phases"]["final"]["t_request_ns"] = json!(DEADLINE + 3 * S);
	assert_invalid(f, "final requested 3000 ms");
}

#[test]
fn an_ack_before_its_request_is_invalid() {
	let mut f = Fixture::fix();
	f.mutate = |b| b.driver["phases"]["reset"]["t_request_ns"] = json!(PRE + 5);
	assert_invalid(f, "timestamps out of order: reset request <= node signal");
}

#[test]
fn a_resume_before_the_reset_ack_is_invalid() {
	let mut f = Fixture::fix();
	f.mutate = |b| b.driver["phases"]["reset"]["t_ack_seen_ns"] = json!(MS + 1);
	assert_invalid(f, "reset ack seen <= measured start");
}

#[test]
fn omitted_identity_fields_never_compare_equal() {
	// Null == Null: the node start record and the driver both lose the pid.
	let mut f = Fixture::fix();
	f.mutate = |b| {
		b.lines[0].as_object_mut().unwrap().remove("pid");
		b.driver["node"].as_object_mut().unwrap().remove("pid");
	};
	assert_invalid(f, "node start/pid: missing");
}

#[test]
fn omitted_correlation_fields_never_compare_equal() {
	let mut f = Fixture::fix();
	f.mutate = |b| {
		for l in &mut b.lines {
			if l["kind"] == "reset_ack" {
				l.as_object_mut().unwrap().remove("seq");
			}
		}
		b.driver["phases"]["reset"]
			.as_object_mut()
			.unwrap()
			.remove("seq");
	};
	assert_invalid(f, "reset_ack/seq: missing");
}

#[test]
fn records_from_another_block_or_manifest_are_invalid() {
	let mut f = Fixture::fix();
	f.mutate = |b| b.driver["block_id"] = json!("20261102-b2-fix-TEST-ffff");
	assert_invalid(f, "driver block");
	let mut f = Fixture::fix();
	f.mutate = |b| b.lines[0]["manifest_sha256"] = json!("other");
	assert_invalid(f, "node start manifest other");
	let mut f = Fixture::fix();
	f.mutate = |b| {
		b.process.as_object_mut().unwrap().remove("manifest_sha256");
	};
	assert_invalid(f, "process/manifest_sha256: missing");
}

// --- Item 4: semantic metric validation ----------------------------------

#[test]
fn a_nan_refusal_counter_is_invalid_not_pass() {
	let mut f = Fixture::fix();
	f.final_edit = Some((
		"wendelon_tier_rewrite_refusals_total{reason=\"layout_collision\"} 0",
		"wendelon_tier_rewrite_refusals_total{reason=\"layout_collision\"} NaN",
	));
	let v = f.run();
	assert_eq!(verdict(&v), "INVALID", "{v:#}");
	assert_eq!(
		v["predicates"]["delta wendelon_tier_rewrite_refusals_total{reason=\"layout_collision\"} == 0"]
			["verdict"],
		"invalid",
		"never pass"
	);
}

#[test]
fn a_fractional_or_infinite_counter_is_invalid() {
	let mut f = Fixture::fix();
	f.final_edit = Some((
		"wendelon_relocation_redirects_skipped_total 0",
		"wendelon_relocation_redirects_skipped_total 0.5",
	));
	assert_invalid(f, "not integral");
	let mut f = Fixture::fix();
	f.final_edit = Some((
		"wendelon_storage_tiered_slabs_total 80",
		"wendelon_storage_tiered_slabs_total +Inf",
	));
	assert_invalid(f, "not finite");
}

#[test]
fn an_invalid_census_is_invalid() {
	// Cumulative counts that fall: 101 at le=262144 but 100 at +Inf. The
	// derived over-count would be negative; it must never pass p99.
	let mut f = Fixture::fix();
	f.final_edit = Some((
		"wendelon_test_tier_lineage_entries_bucket{le=\"262144\"} 99",
		"wendelon_test_tier_lineage_entries_bucket{le=\"262144\"} 101",
	));
	let v = f.run();
	assert_eq!(verdict(&v), "INVALID", "{v:#}");
	assert!(
		reasons(&v).contains("lineage census invalid"),
		"{}",
		reasons(&v)
	);
	assert_eq!(v["predicates"][P99]["verdict"], "invalid");
}

#[test]
fn a_census_disagreeing_with_its_gauge_or_count_is_invalid() {
	let mut f = Fixture::fix();
	f.final_edit = Some((
		"wendelon_test_tier_lineages_over_quarter_cap 1",
		"wendelon_test_tier_lineages_over_quarter_cap 0",
	));
	assert_invalid(f, "derived over-count 1 != gauge 0");
	let mut f = Fixture::fix();
	f.final_edit = Some((
		"wendelon_test_tier_lineage_entries_count 100",
		"wendelon_test_tier_lineage_entries_count 100.5",
	));
	assert_invalid(f, "not integral");
}

#[test]
fn a_bad_rss_value_is_invalid() {
	let mut f = Fixture::fix();
	f.rss = |t| if t == T0 + 300 * S { 0 } else { 100_000 };
	assert_invalid(f, "RSS missing, zero");
	let mut f = Fixture::fix();
	f.mutate = |b| {
		for l in &mut b.lines {
			if l["index"] == 31 {
				l["rss_kib"] = json!(-5);
			}
		}
	};
	assert_invalid(f, "tick 31 RSS");
}

#[test]
fn a_nan_counter_in_a_tick_is_invalid() {
	let mut f = Fixture::fix();
	f.mutate = |b| {
		for l in &mut b.lines {
			if l["index"] == 20 {
				let t = l["metrics"].as_str().unwrap().replace(
					"wendelon_storage_tiered_slabs_total 10",
					"wendelon_storage_tiered_slabs_total NaN",
				);
				l["metrics"] = json!(t);
			}
		}
	};
	assert_invalid(f, "counter wendelon_storage_tiered_slabs_total is NaN");
}
