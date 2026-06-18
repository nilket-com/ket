//! ADR 261: the phase/status telemetry contract and the load-bearing honesty rule.
//!
//! `blocked` is permitted (and exits 0 under `--allow-blocked`) ONLY for a reason on the recognized
//! allowlist. Any error whose reason is not recognized — and any panic or malformed row — is a `fail`,
//! even under `--allow-blocked`. This is what stops "blocked" from silently absorbing real regressions.

/// The stage a query attempt reached. `Fragment` is reserved for later *distributed* reachability; ADR
/// 261 is single-node only and never emits it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
	Generate,
	Load,
	Plan,
	Bind,
	Execute,
	Compare,
	/// Reserved for distributed reachability (unused in ADR 261).
	Fragment,
}

impl Phase {
	pub fn as_str(self) -> &'static str {
		match self {
			Phase::Generate => "generate",
			Phase::Load => "load",
			Phase::Plan => "plan",
			Phase::Bind => "bind",
			Phase::Execute => "execute",
			Phase::Compare => "compare",
			Phase::Fragment => "fragment",
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
	Pass,
	Blocked,
	Fail,
	NotAttempted,
}

impl Status {
	pub fn as_str(self) -> &'static str {
		match self {
			Status::Pass => "pass",
			Status::Blocked => "blocked",
			Status::Fail => "fail",
			Status::NotAttempted => "not_attempted",
		}
	}
}

/// The allowlist of recognized blocker reasons. A `blocked` row may only carry one of these; anything
/// else is a `fail`. Keep this list and [`status_for_reason`] together so the architecture guard can
/// scan one function.
pub const RECOGNIZED_BLOCKERS: &[&str] = &[
	"strict_inputs_required",
	"unsupported_decimal_ddl",
	"unsupported_decimal_scalar",
	"unsupported_decimal_arrow",
	"unsupported_date_interval",
	"unsupported_query_surface",
];

/// The honesty rule: a recognized reason is a `blocked`; an unrecognized one is a `fail`.
pub fn status_for_reason(reason: &str) -> Status {
	if RECOGNIZED_BLOCKERS.contains(&reason) {
		Status::Blocked
	} else {
		Status::Fail
	}
}

/// Map an engine error message to a *candidate* blocker reason. A message that matches no known gap
/// returns `"unrecognized"`, which [`status_for_reason`] classifies as `fail` — never `blocked`.
pub fn reason_for_error(message: &str) -> &'static str {
	let lowered = message.to_ascii_lowercase();
	if lowered.contains("decimal") && lowered.contains("create table") {
		"unsupported_decimal_ddl"
	} else if lowered.contains("decimal") && lowered.contains("comparison operands") {
		// A Decimal column compared against a non-Decimal literal: Toron has no Decimal `ScalarValue`,
		// so decimal literals lower to `Float64`/integer and the comparison fails type unification. This
		// is the measured strict-Q6 bind gap. Keep the marker specific (`comparison operands`) so an
		// unrelated Decimal message that merely mentions "type" is NOT overclassified here.
		"unsupported_decimal_scalar"
	} else if lowered.contains("decimal") {
		// Reserved for a future Decimal carrier/load gap (Arrow Decimal data the engine can't carry).
		"unsupported_decimal_arrow"
	} else if lowered.contains("interval") {
		"unsupported_date_interval"
	} else if lowered.contains("not supported") || lowered.contains("unsupported") {
		"unsupported_query_surface"
	} else {
		"unrecognized"
	}
}

/// One query attempt's structured outcome — emitted as one human line and one JSONL line.
#[derive(Debug, Clone)]
pub struct Attempt {
	pub query: String,
	pub scale_factor: f64,
	pub mode: String,
	pub phase: Phase,
	pub status: Status,
	pub reason: Option<String>,
	pub elapsed_ms: f64,
	pub digest: Option<String>,
}

impl Attempt {
	pub fn human(&self) -> String {
		format!(
			"tpch_reachability query={} sf={} mode={} phase={} status={} reason={} elapsed_ms={:.1} digest={}",
			self.query,
			self.scale_factor,
			self.mode,
			self.phase.as_str(),
			self.status.as_str(),
			self.reason.as_deref().unwrap_or("none"),
			self.elapsed_ms,
			self.digest.as_deref().unwrap_or("none"),
		)
	}

	pub fn jsonl(&self) -> String {
		serde_json::json!({
			"suite": "tpch",
			"query": self.query,
			"scale_factor": self.scale_factor,
			"mode": self.mode,
			"phase": self.phase.as_str(),
			"status": self.status.as_str(),
			"reason": self.reason,
			"elapsed_ms": self.elapsed_ms,
			"digest": self.digest,
		})
		.to_string()
	}

	/// Exit-code policy: `pass` / `not_attempted` always succeed; a recognized `blocked` succeeds only
	/// under `--allow-blocked`; `fail` (including an unrecognized "blocker") never succeeds.
	pub fn is_process_success(&self, allow_blocked: bool) -> bool {
		match self.status {
			Status::Pass | Status::NotAttempted => true,
			Status::Blocked => allow_blocked,
			Status::Fail => false,
		}
	}
}
