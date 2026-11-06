//! `s7-measure evaluate`: the predeclared predicates over one sealed block.
//!
//! Validity comes first: any INVALID reason makes the block INVALID (never
//! PASS and never a value of 0). Then, for the fix arm, any failed predicate
//! is a FAIL. The base arm is evaluated over the same fields where 9e7e3ad1
//! has them; the rest are `n/a`, and its failures are preserved, but it is
//! never reported as PASS.
//!
//! Validity covers the whole block, not only its scrapes:
//! - process outcomes (run.sh's `process.json`): both processes exited 0 and
//!   the node stopped on SIGTERM, with one successful `shutdown_done` after
//!   the final capture;
//! - identity: every record is bound to this block id and manifest SHA-256,
//!   and every identity, sequence and timestamp field is a present unsigned
//!   integer (a missing field never equals another missing field);
//! - timing: the warm-up lasted its declared duration (within the manifest
//!   allowance), the final was requested at the shared measured deadline
//!   (within the allowance), and each request precedes its acknowledgement
//!   within `ack_timeout_secs`;
//! - quantities: every acceptance counter, count, byte value and histogram
//!   bucket is finite, nonnegative and integral, and arithmetic is checked.
//!
//! Deltas are final minus the PRE-reset scrape of the reset acknowledgement,
//! so an event between the warm-up drain and the measured start still counts.

use crate::drive::BatchRecord;
use crate::manifest::Manifest;
use crate::prom::Scrape;
use serde_json::{Value, json};
use std::collections::BTreeMap;

/// Fix-arm FAIL on any positive delta (ADR 0692 §7 refusal and fence paths).
pub const FAIL_ON_DELTA: &[&str] = &[
	"wendelon_tier_rewrite_refusals_total{reason=\"layout_collision\"}",
	"wendelon_tier_rewrite_refusals_total{reason=\"node_history_budget\"}",
	"wendelon_tier_rewrite_refusals_total{reason=\"lineage_history_cap\"}",
	"wendelon_tier_rewrite_refusals_total{reason=\"stale_plan\"}",
	"wendelon_tier_publication_fenced_total{reason=\"no_budget\"}",
	"wendelon_tier_publication_fenced_total{reason=\"no_scratch\"}",
	"wendelon_tier_publication_fenced_total{reason=\"l2_unavailable\"}",
	// Any skip fails (Codex r2 Q3); its cause is unknown from this counter alone.
	"wendelon_relocation_redirects_skipped_total",
];

/// Reported deltas, not predicates.
pub const REPORTED_DELTAS: &[&str] = &[
	"wendelon_tier_rewrite_refusals_total{reason=\"slab_retired\"}",
	"wendelon_tier_history_reservations_total",
	"wendelon_tier_observer_publications_total",
	"wendelon_storage_tiering_failures_total",
	"wendelon_storage_tiering_pass_errors_total",
	"wendelon_storage_table_retention_l2_bytes_reclaimed_total",
];

/// Required positive measured activity (present on both commits): first
/// tierings, and rows pruned by L2 retention (pruned rows, not a count of
/// rewrites: no rewrite-count counter exists).
pub const FIRST_TIERINGS: &str = "wendelon_storage_tiered_slabs_total";
pub const RETENTION_L2_PRUNED_ROWS: &[&str] = &[
	"wendelon_storage_table_retention_l2_rows_pruned_total{kind=\"data\"}",
	"wendelon_storage_table_retention_l2_rows_pruned_total{kind=\"tombstone\"}",
];

pub const PEAK: &str = "wendelon_test_tier_budget_block_peak_bytes";
pub const MIN_HEADROOM: &str = "wendelon_test_tier_budget_block_min_headroom_bytes";
pub const LIMIT: &str = "wendelon_test_tier_budget_limit_bytes";
pub const LINEAGE: &str = "wendelon_test_tier_lineage_entries";
pub const LINEAGE_COUNT: &str = "wendelon_test_tier_lineage_entries_count";
pub const OVER_QUARTER: &str = "wendelon_test_tier_lineages_over_quarter_cap";

/// Everything one block evaluation reads.
pub struct Inputs<'a> {
	pub arm: &'a str,
	pub manifest: &'a Manifest,
	/// SHA-256 of the manifest file the evaluator itself loaded.
	pub manifest_sha256: &'a str,
	/// The block identity from `block-id.json`.
	pub block_id: &'a str,
	pub observations: &'a str,
	pub driver: &'a Value,
	pub batches: &'a str,
	/// run.sh's process outcomes.
	pub process: &'a Value,
}

#[derive(Default)]
struct Outcome {
	invalid: Vec<String>,
	fail: Vec<String>,
	predicates: BTreeMap<String, Value>,
}

impl Outcome {
	fn invalid(&mut self, reason: impl Into<String>) {
		self.invalid.push(reason.into());
	}
	fn predicate(&mut self, name: &str, value: Value, verdict: &str) {
		if verdict == "fail" {
			self.fail.push(name.to_string());
		}
		self.predicates
			.insert(name.into(), json!({"value": value, "verdict": verdict}));
	}
	/// A present unsigned integer at `pointer`, or INVALID.
	fn u(&mut self, v: &Value, pointer: &str, what: &str) -> Option<u64> {
		let got = v.pointer(pointer).and_then(Value::as_u64);
		if got.is_none() {
			self.invalid(format!(
				"{what}{pointer}: missing or not an unsigned integer"
			));
		}
		got
	}
	/// A present string at `pointer`, or INVALID.
	fn s<'v>(&mut self, v: &'v Value, pointer: &str, what: &str) -> Option<&'v str> {
		let got = v.pointer(pointer).and_then(Value::as_str);
		if got.is_none() {
			self.invalid(format!("{what}{pointer}: missing or not a string"));
		}
		got
	}
	/// `a <= b`, both present, or INVALID naming the step.
	fn ordered(&mut self, a: Option<u64>, b: Option<u64>, step: &str) {
		if let (Some(a), Some(b)) = (a, b)
			&& a > b
		{
			self.invalid(format!("timestamps out of order: {step}"));
		}
	}
}

fn median(mut v: Vec<f64>) -> f64 {
	v.sort_by(f64::total_cmp);
	let n = v.len();
	if n % 2 == 1 {
		v[n / 2]
	} else {
		(v[n / 2 - 1] + v[n / 2]) / 2.0
	}
}

const MS: u64 = 1_000_000;
const S: u64 = 1_000_000_000;

pub fn evaluate(inp: &Inputs<'_>) -> Value {
	let (arm, m) = (inp.arm, inp.manifest);
	let fix = arm == "fix";
	let mut o = Outcome::default();

	// --- Records ---------------------------------------------------------
	let mut lines = Vec::new();
	for (n, line) in inp.observations.lines().enumerate() {
		match serde_json::from_str::<Value>(line) {
			Ok(v) if v.is_object() && v["kind"].is_string() => lines.push(v),
			_ => o.invalid(format!("malformed observation line {}", n + 1)),
		}
	}
	let mut batches = Vec::new();
	for (n, line) in inp.batches.lines().enumerate() {
		match serde_json::from_str::<BatchRecord>(line) {
			Ok(b) => batches.push(b),
			Err(_) => o.invalid(format!("malformed driver batch line {}", n + 1)),
		}
	}
	let driver = inp.driver;
	if !driver["aborted"].is_null() {
		o.invalid(format!("driver aborted: {}", driver["aborted"]));
	}

	// --- Bindings: one block, one manifest, one arm ------------------------
	let Some(start) = lines.first().filter(|l| l["kind"] == "start").cloned() else {
		o.invalid("first observation is not the start record");
		return finish(inp, o, json!({}));
	};
	for (what, v) in [
		("driver", driver),
		("node start", &start),
		("process", inp.process),
	] {
		if let Some(sha) = o.s(v, "/manifest_sha256", what)
			&& sha != inp.manifest_sha256
		{
			o.invalid(format!(
				"{what} manifest {sha} != evaluated manifest {}",
				inp.manifest_sha256
			));
		}
		if let Some(id) = o.s(v, "/block_id", what)
			&& id != inp.block_id
		{
			o.invalid(format!("{what} block {id} != this block {}", inp.block_id));
		}
	}
	for (what, v) in [("driver", driver), ("node start", &start)] {
		if let Some(a) = o.s(v, "/arm", what)
			&& a != arm
		{
			o.invalid(format!("{what} arm {a} != {arm}"));
		}
	}

	// --- Process outcomes -------------------------------------------------
	let p = inp.process;
	if let Some(rc) = o.u(p, "/drive_exit_code", "process")
		&& rc != 0
	{
		o.invalid(format!("driver exited {rc}"));
	}
	if let Some(rc) = o.u(p, "/node_exit_code", "process")
		&& rc != 0
	{
		o.invalid(format!("node exited {rc}"));
	}
	if let Some(stop) = o.s(p, "/node_stop", "process")
		&& stop != "sigterm"
	{
		o.invalid(format!(
			"node stop was {stop}, not an orderly SIGTERM shutdown"
		));
	}

	// --- Identity ---------------------------------------------------------
	let pid = o.u(&start, "/pid", "node start");
	let start_time = o.u(&start, "/pid_start_time", "node start");
	let identities = [
		("driver", driver, "/node/pid", "/node/pid_start_time"),
		("process", p, "/node_pid", "/node_pid_start_time"),
	];
	for (what, v, pp, sp) in identities {
		let (a, b) = (o.u(v, pp, what), o.u(v, sp, what));
		if a.is_some() && pid.is_some() && (a, b) != (pid, start_time) {
			o.invalid(format!(
				"{what} node identity differs from the observed process"
			));
		}
	}
	for (n, l) in lines.iter().enumerate() {
		let (a, b) = (l["pid"].as_u64(), l["pid_start_time"].as_u64());
		if a.is_none() || b.is_none() || (a, b) != (pid, start_time) {
			o.invalid(format!(
				"observation {} has another or no process identity",
				n + 1
			));
		}
		match l["kind"].as_str() {
			Some("fatal") => o.invalid(format!("node fatal: {}", l["error"])),
			Some("control_error") => o.invalid(format!("control error: {}", l["reason"])),
			_ => {}
		}
	}

	// --- Phase acknowledgements -----------------------------------------
	let at = |kind: &str| -> Vec<usize> {
		lines
			.iter()
			.enumerate()
			.filter(|(_, l)| l["kind"] == kind)
			.map(|(i, _)| i)
			.collect()
	};
	let (resets, finals, diags) = (at("reset_ack"), at("final"), at("diagnostic"));
	let (sd_begin, sd_done) = (at("shutdown_begin"), at("shutdown_done"));
	if resets.len() != 1 {
		o.invalid(format!(
			"{} reset acknowledgements (expected 1)",
			resets.len()
		));
	}
	if finals.len() != 1 {
		o.invalid(format!("{} final captures (expected 1)", finals.len()));
	}
	let (Some(&reset_at), Some(&final_at)) = (resets.first(), finals.first()) else {
		return finish(inp, o, json!({}));
	};
	let (reset, fin) = (&lines[reset_at], &lines[final_at]);
	let phases = &driver["phases"];
	for (what, node_v, driver_ptr, expect) in [
		("reset_ack", reset, "/phases/reset/seq", 1u64),
		("final", fin, "/phases/final/seq", 2),
	] {
		let (a, b) = (o.u(node_v, "/seq", what), o.u(driver, driver_ptr, "driver"));
		if a.is_some() && b.is_some() && (a != Some(expect) || b != Some(expect)) {
			o.invalid(format!(
				"{what} seq node {a:?} / driver {b:?}, expected {expect}"
			));
		}
	}
	if reset_at > final_at {
		o.invalid("final capture precedes the reset acknowledgement");
	}
	if m.diagnostic_quiesce_secs > 0 && diags.len() != 1 {
		o.invalid(format!("{} diagnostic captures (expected 1)", diags.len()));
	}
	// Terminal evidence: exactly one orderly shutdown, after every capture,
	// completing successfully, and nothing after it.
	let last_capture = diags
		.iter()
		.copied()
		.max()
		.unwrap_or(final_at)
		.max(final_at);
	match (sd_begin.as_slice(), sd_done.as_slice()) {
		([b], [d]) if *b > last_capture && *d > *b && *d == lines.len() - 1 => {
			if lines[*d]["ok"] != Value::Bool(true) {
				o.invalid(format!("node shutdown failed: {}", lines[*d]["error"]));
			}
		}
		([], _) | (_, []) => o.invalid("missing shutdown_begin/shutdown_done terminal evidence"),
		_ => o.invalid("shutdown records duplicated, misplaced or not last"),
	}
	match (fix, &reset["reset_done"]) {
		(true, Value::Bool(true)) => {}
		(true, other) => o.invalid(format!("fix instrument reset not done: {other}")),
		(false, _) => {}
	}

	// --- Phase timing ------------------------------------------------------
	let allow = m.phase_timing_allowance_ms * MS;
	let ack_bound = m.ack_timeout_secs * S;
	let w_start = o.u(driver, "/phases/warmup/t_start_ns", "driver");
	let w_stop = o.u(driver, "/phases/warmup/t_admission_stopped_ns", "driver");
	let drained = o.u(driver, "/phases/warmup/t_drained_ns", "driver");
	if let (Some(a), Some(b)) = (w_start, w_stop) {
		let span = b.saturating_sub(a);
		let declared = m.warmup_secs * S;
		if b < a || span < declared || span > declared + allow {
			o.invalid(format!(
				"warm-up lasted {} ms, declared {} ms (+{} ms allowance)",
				span / MS,
				declared / MS,
				allow / MS
			));
		}
	}
	o.ordered(w_stop, drained, "warm-up stop <= drained");
	let r_req = o.u(driver, "/phases/reset/t_request_ns", "driver");
	let r_seen = o.u(driver, "/phases/reset/t_ack_seen_ns", "driver");
	let r_sig = o.u(reset, "/t_signal_ns", "reset_ack");
	let t_pre = o.u(reset, "/t_pre_ns", "reset_ack");
	let r_reset = o.u(reset, "/t_reset_ns", "reset_ack");
	let r_post = o.u(reset, "/t_post_ns", "reset_ack");
	let r_ack = o.u(reset, "/t_ack_ns", "reset_ack");
	let chain = [
		(drained, r_req, "drained <= reset request"),
		(r_req, r_sig, "reset request <= node signal"),
		(r_sig, t_pre, "signal <= pre-reset scrape"),
		(t_pre, r_reset, "pre-reset scrape <= reset"),
		(r_reset, r_post, "reset <= post-reset scrape"),
		(r_post, r_ack, "post-reset scrape <= node ack"),
		(r_ack, r_seen, "node ack <= driver saw ack"),
	];
	for (a, b, step) in chain {
		o.ordered(a, b, step);
	}
	if let (Some(a), Some(b)) = (r_req, r_seen)
		&& b.saturating_sub(a) > ack_bound
	{
		o.invalid("reset acknowledged later than ack_timeout_secs");
	}
	let ms = o.u(driver, "/phases/measured/t_start_ns", "driver");
	let deadline = o.u(driver, "/phases/measured/t_deadline_ns", "driver");
	o.ordered(r_seen, ms, "reset ack seen <= measured start (resume)");
	if let (Some(ms), Some(d)) = (ms, deadline)
		&& ms.checked_add(m.measured_secs * S) != Some(d)
	{
		o.invalid(format!(
			"measured deadline is {} ms after its start, declared {} s",
			d.saturating_sub(ms) / MS,
			m.measured_secs
		));
	}
	let f_req = o.u(driver, "/phases/final/t_request_ns", "driver");
	let f_seen = o.u(driver, "/phases/final/t_ack_seen_ns", "driver");
	let f_sig = o.u(fin, "/t_signal_ns", "final");
	let f_cap = o.u(fin, "/t_mono_ns", "final");
	let f_ack = o.u(fin, "/t_ack_ns", "final");
	if let (Some(d), Some(f)) = (deadline, f_req)
		&& (f < d || f - d > allow)
	{
		o.invalid(format!(
			"final requested {} ms from the measured deadline (allowance {} ms, never early)",
			(f as i128 - d as i128) / MS as i128,
			allow / MS
		));
	}
	for (a, b, step) in [
		(f_req, f_sig, "final request <= node signal"),
		(f_sig, f_cap, "signal <= final capture"),
		(f_cap, f_ack, "final capture <= node ack"),
		(f_ack, f_seen, "node ack <= driver saw ack"),
	] {
		o.ordered(a, b, step);
	}
	if let (Some(a), Some(b)) = (f_req, f_seen)
		&& b.saturating_sub(a) > ack_bound
	{
		o.invalid("final acknowledged later than ack_timeout_secs");
	}
	let joined = o.u(driver, "/phases/join/t_joined_ns", "driver");
	o.ordered(f_seen, joined, "final ack seen <= join");
	let (Some(ms), Some(me)) = (ms, f_cap) else {
		o.invalid("measured interval bounds unusable");
		return finish(inp, o, json!({}));
	};

	// --- Ticks -----------------------------------------------------------
	let tick_ns = m.tick_secs * S;
	let tol_ns = m.tick_late_tolerance_ms * MS;
	let mut ticks: Vec<(u64, u64)> = Vec::new(); // (t_mono, rss)
	let mut missing = 0u64;
	let mut expect = 0u64;
	for l in lines.iter().filter(|l| l["kind"] == "tick") {
		let (Some(i), Some(s), Some(a)) = (
			l["index"].as_u64(),
			l["t_sched_ns"].as_u64(),
			l["t_mono_ns"].as_u64(),
		) else {
			o.invalid("tick without index/schedule/time");
			continue;
		};
		if i < expect {
			o.invalid(format!("tick index {i} repeated or out of order"));
		}
		missing += i.saturating_sub(expect);
		expect = i + 1;
		if a > s.saturating_add(tol_ns) {
			o.invalid(format!("tick {i} late by {} ms", (a - s) / MS));
		}
		match l["rss_kib"].as_u64() {
			Some(r) if r > 0 => ticks.push((a, r)),
			_ => o.invalid(format!("tick {i} RSS missing, zero or not an integer")),
		}
		if !l["metrics"].is_string() {
			o.invalid(format!("tick {i} without metrics"));
		}
	}
	if missing > m.tick_missing_tolerance {
		o.invalid(format!("{missing} missing ticks"));
	}
	match ticks.iter().map(|t| t.0).filter(|t| *t <= me).max() {
		Some(t) if me - t <= tick_ns + tol_ns => {}
		_ => o.invalid("no tick within one interval of the final capture"),
	}
	if ticks.iter().map(|t| t.0).min().is_none_or(|t| t > ms) {
		o.invalid("ticks do not cover the measured interval from its start");
	}

	// --- Scrapes and counter monotonicity -------------------------------
	let mut scrapes: Vec<(String, Scrape)> = Vec::new();
	for l in &lines {
		let fields: &[&str] = match l["kind"].as_str() {
			Some("tick" | "final" | "diagnostic") => &["metrics"],
			Some("reset_ack") => &["pre_scrape", "post_scrape"],
			_ => &[],
		};
		for f in fields {
			let label = format!("{}#{}:{f}", l["kind"].as_str().unwrap_or(""), l["seq"]);
			match l[*f].as_str().map(Scrape::parse) {
				Some(Ok(s)) => scrapes.push((label, s)),
				Some(Err(e)) => o.invalid(format!("malformed scrape {label}: {e}")),
				None => o.invalid(format!("missing scrape {label}")),
			}
		}
	}
	for (label, s) in &scrapes {
		for (series, v) in &s.samples {
			if s.is_monotone(series) && !(v.is_finite() && *v >= 0.0) {
				o.invalid(format!("counter {series} is {v} at {label}"));
			}
		}
	}
	for pair in scrapes.windows(2) {
		let ((_, a), (lb, b)) = (&pair[0], &pair[1]);
		for (series, va) in &a.samples {
			if !a.is_monotone(series) {
				continue;
			}
			match b.get(series) {
				None => o.invalid(format!("counter {series} vanished at {lb}")),
				Some(vb) if vb < *va => o.invalid(format!("counter {series} decreased at {lb}")),
				_ => {}
			}
		}
	}
	let parse = |v: &Value| v.as_str().and_then(|t| Scrape::parse(t).ok());
	let (Some(base), Some(last)) = (parse(&reset["pre_scrape"]), parse(&fin["metrics"])) else {
		o.invalid("baseline or final scrape unusable");
		return finish(inp, o, json!({}));
	};
	// A validated delta: Ok(None) when the series is absent from either
	// scrape; Err when a value is not a quantity or the delta is negative.
	let delta = |s: &str| -> Result<Option<u64>, String> {
		match (base.quantity(s)?, last.quantity(s)?) {
			(Some(a), Some(b)) => b
				.checked_sub(a)
				.map(Some)
				.ok_or_else(|| format!("{s} fell from {a} to {b}")),
			_ => Ok(None),
		}
	};

	// --- Refusal and fence deltas ----------------------------------------
	for s in FAIL_ON_DELTA {
		let name = format!("delta {s} == 0");
		match delta(s) {
			Ok(Some(d)) => o.predicate(&name, json!(d), if d > 0 { "fail" } else { "pass" }),
			Ok(None) if fix => {
				o.invalid(format!("fix metric {s} missing"));
				o.predicate(&name, Value::Null, "invalid");
			}
			Ok(None) => o.predicate(&name, Value::Null, "n/a"),
			Err(e) => {
				o.invalid(format!("acceptance counter invalid: {e}"));
				o.predicate(&name, Value::Null, "invalid");
			}
		}
	}
	let mut reported = BTreeMap::new();
	for s in REPORTED_DELTAS {
		let v = match delta(s) {
			Ok(v) => json!(v),
			Err(e) => json!({"invalid": e}),
		};
		reported.insert(s.to_string(), v);
	}

	// --- Activity --------------------------------------------------------
	let tierings = delta(FIRST_TIERINGS);
	let mut pruned: Result<Option<u64>, String> = Ok(Some(0));
	for s in RETENTION_L2_PRUNED_ROWS {
		pruned = match (pruned, delta(s)) {
			(Ok(Some(acc)), Ok(Some(d))) => acc
				.checked_add(d)
				.map(Some)
				.ok_or_else(|| "pruned rows overflow".to_string()),
			(Err(e), _) | (_, Err(e)) => Err(e),
			_ => Ok(None),
		};
	}
	match &tierings {
		Ok(Some(d)) if *d > 0 => {}
		Ok(Some(_)) => o.invalid("no first tiering in the measured interval"),
		Ok(None) => o.invalid(format!("{FIRST_TIERINGS} missing")),
		Err(e) => o.invalid(format!("activity counter invalid: {e}")),
	}
	match &pruned {
		Ok(Some(d)) if *d > 0 => {}
		Ok(Some(_)) => o.invalid("no L2 retention pruned rows in the measured interval"),
		Ok(None) => o.invalid("L2 retention pruned-row counters missing"),
		Err(e) => o.invalid(format!("activity counter invalid: {e}")),
	}

	// --- Budget extrema and lineage census at the final capture ----------
	let gauges = (
		last.quantity(PEAK),
		last.quantity(MIN_HEADROOM),
		last.quantity(LIMIT),
	);
	match gauges {
		(Ok(Some(peak)), Ok(Some(head)), Ok(Some(limit))) => {
			o.predicate(
				"block peak < limit",
				json!({"peak": peak, "limit": limit}),
				if peak < limit { "pass" } else { "fail" },
			);
			o.predicate(
				"block min headroom > 0",
				json!(head),
				if head > 0 { "pass" } else { "fail" },
			);
		}
		(Ok(None), Ok(None), Ok(None)) if !fix => {
			o.predicate("block peak < limit", Value::Null, "n/a");
			o.predicate("block min headroom > 0", Value::Null, "n/a");
		}
		(a, b, c) => {
			o.invalid(format!(
				"budget instrument missing or invalid at final: {:?}",
				[a, b, c]
					.iter()
					.filter_map(|r| r.clone().err())
					.collect::<Vec<_>>()
			));
			o.predicate("block peak < limit", Value::Null, "invalid");
			o.predicate("block min headroom > 0", Value::Null, "invalid");
		}
	}
	const P99: &str = "lineages over cap/4 * 100 <= lineages";
	let census = (|| -> Result<Option<Value>, String> {
		let buckets = last.buckets(LINEAGE)?;
		let (count, over_gauge) = (last.quantity(LINEAGE_COUNT)?, last.quantity(OVER_QUARTER)?);
		if buckets.is_empty() && count.is_none() && over_gauge.is_none() {
			return Ok(None);
		}
		let (Some(count), Some(over_gauge)) = (count, over_gauge) else {
			return Err("lineage census incomplete".into());
		};
		let quarter = m.lineage_quarter_cap as f64;
		let at = buckets
			.iter()
			.find(|(le, _)| *le == quarter)
			.map(|b| b.1)
			.ok_or("lineage histogram lacks the cap/4 bucket")?;
		let inf = buckets.last().map(|b| b.1).ok_or("no +Inf bucket")?;
		if inf != count {
			return Err(format!("lineage +Inf bucket {inf} != count {count}"));
		}
		let over = inf
			.checked_sub(at)
			.ok_or_else(|| format!("negative over-count ({inf} - {at})"))?;
		if over != over_gauge {
			return Err(format!("derived over-count {over} != gauge {over_gauge}"));
		}
		if count == 0 {
			return Err("empty final lineage population".into());
		}
		let lhs = over.checked_mul(100).ok_or("over-count overflow")?;
		Ok(Some(
			json!({"over": over, "count": count, "pass": lhs <= count}),
		))
	})();
	let census = match census {
		Ok(Some(c)) => {
			let pass = c["pass"] == true;
			o.predicate(
				P99,
				json!({"over": c["over"], "count": c["count"]}),
				if pass { "pass" } else { "fail" },
			);
			c
		}
		Ok(None) if !fix => {
			o.predicate(P99, Value::Null, "n/a");
			Value::Null
		}
		Ok(None) => {
			o.invalid("fix lineage census missing at final");
			o.predicate(P99, Value::Null, "invalid");
			Value::Null
		}
		Err(e) => {
			o.invalid(format!("lineage census invalid: {e}"));
			o.predicate(P99, Value::Null, "invalid");
			Value::Null
		}
	};

	// --- RSS -------------------------------------------------------------
	let w = m.rss_window_secs * S;
	let rss_in = |lo: u64, hi: u64| -> Vec<f64> {
		ticks
			.iter()
			.filter(|(t, _)| *t >= lo && *t <= hi)
			.map(|(_, r)| *r as f64)
			.collect()
	};
	let (first, lastw) = (rss_in(ms, ms + w), rss_in(me.saturating_sub(w), me));
	let rss = if me.saturating_sub(ms) < 2 * w {
		o.invalid("measured interval shorter than two RSS windows");
		Value::Null
	} else if first.len() < m.rss_window_min_samples || lastw.len() < m.rss_window_min_samples {
		o.invalid(format!(
			"incomplete RSS window ({} / {} samples, need {})",
			first.len(),
			lastw.len(),
			m.rss_window_min_samples
		));
		Value::Null
	} else {
		let (a, b) = (median(first.clone()), median(lastw.clone()));
		let ratio = b / a;
		o.predicate(
			"rss median last/first window < limit",
			json!(ratio),
			if ratio < m.rss_ratio_limit {
				"pass"
			} else {
				"fail"
			},
		);
		json!({"first_median_kib": a, "last_median_kib": b, "ratio": ratio,
			"first_samples": first.len(), "last_samples": lastw.len()})
	};

	// --- Workload --------------------------------------------------------
	let incomplete = batches.iter().filter(|b| !b.completed).count();
	if incomplete > 0 {
		o.invalid(format!("{incomplete} batches never completed"));
	}
	let errors = batches.iter().filter(|b| !b.ok).count();
	if errors > 0 {
		o.invalid(format!("{errors} workload errors"));
	}
	let admitted: Option<u64> = driver["totals"]["admitted"]
		.as_object()
		.and_then(|a| a.values().map(Value::as_u64).sum());
	if admitted != Some(batches.len() as u64) {
		o.invalid(format!(
			"driver admitted {admitted:?} batches but recorded {}",
			batches.len()
		));
	}
	if let Some(at) = driver["totals"]["dataset_exhausted_at_ns"].as_u64()
		&& at <= me
	{
		o.invalid("dataset exhausted before the final capture");
	}
	let interval = m.batch_interval_ns();
	if !batches.iter().any(|b| {
		b.phase == "measured"
			&& b.t_admit_ns.saturating_add(2 * interval) >= me
			&& b.t_admit_ns <= me
	}) {
		o.invalid("no admission within two intervals of the final capture: load not active");
	}
	let window = m.rate_window_secs * S;
	let step = m.rate_window_step_secs * S;
	let floor = m.rate_floor_fraction * m.target_rows_per_sec as f64;
	let acks: Vec<(u64, u64)> = batches
		.iter()
		.filter(|b| b.ok && b.completed && b.phase == "measured")
		.map(|b| (b.t_ack_ns, b.rows))
		.collect();
	let mut min_rate: Option<f64> = None;
	let mut s = ms;
	while s + window <= me {
		let rows: u64 = acks
			.iter()
			.filter(|(t, _)| *t >= s && *t < s + window)
			.map(|(_, r)| r)
			.sum();
		let rate = rows as f64 / m.rate_window_secs as f64;
		min_rate = Some(min_rate.map_or(rate, |v: f64| v.min(rate)));
		s += step;
	}
	match min_rate {
		None => o.invalid("measured interval shorter than one rate window"),
		Some(r) if r < floor => o.invalid(format!(
			"sustained rate {r:.0} rows/s below {floor:.0} over a {} s window",
			m.rate_window_secs
		)),
		_ => {}
	}
	let measured_rows: u64 = acks
		.iter()
		.filter(|(t, _)| *t >= ms && *t <= me)
		.map(|(_, r)| r)
		.sum();
	let workload = json!({
		"errors": errors,
		"incomplete": incomplete,
		"measured_acked_rows": measured_rows,
		"measured_rate_rows_per_sec": measured_rows as f64 / ((me - ms).max(1) as f64 / 1e9),
		"min_window_rate_rows_per_sec": min_rate,
		"rate_floor_rows_per_sec": floor,
		"in_flight_at_final_request": phases["final"]["in_flight_at_request"],
		"in_flight_at_final_ack": phases["final"]["in_flight_at_ack"],
		"dataset": driver["dataset"],
	});
	let ok_or = |r: &Result<Option<u64>, String>| match r {
		Ok(v) => json!(v),
		Err(e) => json!({"invalid": e}),
	};
	let detail = json!({
		"baseline": "reset_ack.pre_scrape",
		"measured_start_ns": ms,
		"final_ns": me,
		"reported_deltas": reported,
		"activity": {
			"first_tierings": ok_or(&tierings),
			"retention_l2_pruned_rows": ok_or(&pruned),
		},
		"census": census,
		"rss": rss,
		"workload": workload,
		"process": inp.process,
		"ticks": ticks.len(),
		"scrapes": scrapes.len(),
	});
	finish(inp, o, detail)
}

fn finish(inp: &Inputs<'_>, o: Outcome, detail: Value) -> Value {
	let verdict = if !o.invalid.is_empty() {
		"INVALID"
	} else if !o.fail.is_empty() {
		"FAIL"
	} else if inp.arm == "fix" {
		"PASS"
	} else {
		"NO_FAILURE_ON_PRESENT_FIELDS"
	};
	json!({
		"arm": inp.arm,
		"label": inp.manifest.label,
		"evidence": inp.manifest.label == "CAMPAIGN",
		"block_id": inp.block_id,
		"manifest_sha256": inp.manifest_sha256,
		"verdict": verdict,
		"invalid_reasons": o.invalid,
		"failed_predicates": o.fail,
		"predicates": o.predicates,
		"detail": detail,
	})
}

#[cfg(test)]
mod tests;
