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
	"unsupported_decimal_arithmetic",
	"unsupported_decimal_avg",
	"unsupported_decimal_case",
	"unsupported_decimal_group_key",
	"unsupported_decimal_arrow",
	"unsupported_numeric_comparison_alignment",
	"unsupported_date_interval",
	"ambiguous_aggregate_projection",
	"unsupported_correlated_exists",
	"unsupported_derived_multi_source",
	"unsupported_substring",
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
	} else if lowered.contains("comparison operands have different types")
		&& lowered.contains("int32")
		&& (lowered.contains("float64") || lowered.contains("decimal128"))
	{
		// ADR 285: q20's `ps_availqty > 0.5 * SUM(l_quantity)` compares an Int32 column against a
		// non-literal Float64/Decimal scalar expression. Literal coercion and Decimal-vs-Decimal alignment do
		// not cover this general numeric comparison alignment.
		"unsupported_numeric_comparison_alignment"
	} else if lowered.contains("decimal") && lowered.contains("comparison operands") {
		// A Decimal column compared against a non-Decimal literal: Toron has no Decimal `ScalarValue`,
		// so decimal literals lower to `Float64`/integer and the comparison fails type unification. This
		// is the measured strict-Q6 bind gap. Keep the marker specific (`comparison operands`) so an
		// unrelated Decimal message that merely mentions "type" is NOT overclassified here.
		"unsupported_decimal_scalar"
	} else if lowered.contains("decimal") && lowered.contains("arithmetic operands") {
		// ADR 266: a Decimal operand in arithmetic the binder can't type (mixed-scale `+`/`-`, or `÷`).
		// q01's first measured wall (`1 - l_discount`) before ADR 266 lands its same-(p,s) `+`/`-` arm.
		// Kept specific (`arithmetic operands`) so it does not collide with the comparison/scalar gap.
		"unsupported_decimal_arithmetic"
	} else if lowered.contains("decimal") && lowered.contains("avg expects numeric input") {
		// ADR 268 implements AVG(Decimal) (widened `Decimal128(p+4,s+4)` finalize). This now fires only for
		// a Decimal whose `+4` widening can't fit Decimal128's precision ceiling — a genuine edge gap, kept
		// a recognized blocker rather than a silent fail.
		"unsupported_decimal_avg"
	} else if lowered.contains("decimal")
		&& lowered.contains("case result branches cannot be unified")
	{
		// ADR 269: q14's `CASE WHEN … THEN <Decimal> ELSE 0 END` — the Decimal `THEN` branch won't unify
		// with the integer `ELSE 0` literal. q14's measured first wall once honest optimizer facts admit
		// the comma-join. Kept specific (`case result branches cannot be unified`) so it does not fall to
		// the generic Decimal-carrier bucket below.
		"unsupported_decimal_case"
	} else if (lowered.contains("exchange partition key type")
		|| lowered.contains("unsupported group by type"))
		&& lowered.contains("decimal")
	{
		// ADR 274: q10's `GROUP BY c_acctbal` over a `Decimal128` key. The *measured* first wall is the
		// group-by repartition exchange — `"unsupported exchange partition key type: Decimal128(15, 2)"` —
		// which rejects the Decimal partition key before `group_value` (which also lacks a Decimal128 arm) is
		// reached. Both are the same Decimal-group-key gap; names ADR 275 (Decimal group-key support). Kept
		// specific so it does not fall to the generic Decimal-carrier bucket below.
		"unsupported_decimal_group_key"
	} else if lowered.contains("decimal") {
		// Reserved for a future Decimal carrier/load gap (Arrow Decimal data the engine can't carry).
		"unsupported_decimal_arrow"
	} else if lowered.contains("interval") {
		"unsupported_date_interval"
	} else if lowered.contains("ambiguous aggregate projection") {
		// ADR 266: q01's measured first wall — a projection with multiple same-function aggregates (e.g.
		// two SUMs) the aggregate-projection accounting can't disambiguate. The genuine q01 prerequisite
		// (ADR 267), distinct from the broad query-surface bucket. Checked before the generic fallback.
		"ambiguous_aggregate_projection"
	} else if lowered.contains("exists column reference") {
		// ADR 278: q04's correlated EXISTS uses an UNQUALIFIED reference (`l_orderkey = o_orderkey`); the
		// qualifier-driven `classify_exists_conjunct` can't resolve which side each column is on and rejects
		// it at plan ("unsupported EXISTS column reference"). q04's measured first wall — the
		// correlated-subquery class; names the decorrelation-generality ADR. Kept specific so it does not fall
		// to the generic query-surface bucket below.
		"unsupported_correlated_exists"
	} else if lowered.contains("derived table subquery must have exactly one from source") {
		// ADR 293/294: this was q07's raw derived comma-list wall before ADR 294 relaxed the derived-table
		// validator. Keep the classifier precise for old rows and targeted regression probes.
		"unsupported_derived_multi_source"
	} else if lowered.contains("unsupported expression") && lowered.contains("substring") {
		// ADR 304's q22 row used this wall before ADR 305 added SUBSTRING support; keep it precise for old
		// telemetry and targeted regression probes.
		"unsupported_substring"
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

/// ADR 273: one correctness-check outcome — a rail distinct from [`Attempt`]. The human line is
/// `tpch_correctness` (NOT `tpch_reachability`), so it cannot be confused with a reachability row, and it
/// carries no digest (correctness compares the raw Decimal value, not the digest). `phase=compare
/// status=pass|fail` is the answer check; a query that fails to plan/bind/execute carries that deepest phase
/// with a reachability-style reason.
#[derive(Debug, Clone)]
pub struct CorrectnessAttempt {
	pub query: String,
	pub scale_factor: f64,
	pub mode: String,
	pub phase: Phase,
	pub status: Status,
	pub reason: Option<String>,
	pub elapsed_ms: f64,
}

impl CorrectnessAttempt {
	pub fn human(&self) -> String {
		format!(
			"tpch_correctness query={} sf={} mode={} phase={} status={} reason={} elapsed_ms={:.1}",
			self.query,
			self.scale_factor,
			self.mode,
			self.phase.as_str(),
			self.status.as_str(),
			self.reason.as_deref().unwrap_or("none"),
			self.elapsed_ms,
		)
	}

	pub fn jsonl(&self) -> String {
		serde_json::json!({
			"suite": "tpch_correctness",
			"query": self.query,
			"scale_factor": self.scale_factor,
			"mode": self.mode,
			"phase": self.phase.as_str(),
			"status": self.status.as_str(),
			"reason": self.reason,
			"elapsed_ms": self.elapsed_ms,
		})
		.to_string()
	}

	/// A correctness check succeeds only when it reached `compare` and matched (`Pass`). A mismatch
	/// (`Fail`) or a query that couldn't be executed is a process failure — there is no `--allow-blocked`
	/// escape on the correctness rail.
	pub fn is_process_success(&self) -> bool {
		self.status == Status::Pass
	}
}
