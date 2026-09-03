//! ADR 261: the phase/status telemetry contract and the load-bearing honesty rule.
//!
//! `blocked` is permitted (and exits 0 under `--allow-blocked`) ONLY for a reason on the recognized
//! allowlist. Any error whose reason is not recognized — and any panic or malformed row — is a `fail`,
//! even under `--allow-blocked`. This is what stops "blocked" from silently absorbing real regressions.

/// The stage a query attempt reached. `Fragment` was reserved at ADR 261 for distributed
/// reachability; ADR 528/529's explicit census rails are its sole producers — no default rail
/// emits it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
	Generate,
	Load,
	Plan,
	Bind,
	Execute,
	Compare,
	/// Emitted only by the ADR 528/529 census rails (plan-shape capability, no default execution).
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
	"unsupported_non_equi_exists_correlation",
	"unsupported_derived_multi_source",
	"unsupported_non_equi_join_on",
	"unsupported_cross_join",
	"unsupported_having_scalar_subquery",
	"unsupported_substring",
	"unsupported_query_surface",
];

/// The honesty rule: only allowlisted blocker reasons are `blocked`.
/// Any other reason, including named non-blockers and `unrecognized`, is a `fail`.
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
	if lowered.contains("budgetexhausted") && lowered.contains("dimension: memory") {
		// ADR 332: q04/q12/q20 at sf=1 reach execute, then exhaust the per-task memory budget. This is
		// a named scale/cost-bound wall, not an unsupported SQL surface, so it intentionally stays out
		// of RECOGNIZED_BLOCKERS and remains fail(memory_budget_exhausted@execute).
		"memory_budget_exhausted"
	} else if lowered.contains("budgetexhausted") && lowered.contains("dimension: spillbytes") {
		// ADR 347: streamed spill can make memory walls observable as spill-volume walls. This is still
		// a scale/cost-bound fail, not an unsupported SQL blocker.
		"spill_budget_exhausted"
	} else if lowered.contains("decimal") && lowered.contains("create table") {
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
	} else if lowered.contains("unsupported non-equi exists correlation") {
		// ADR 323 keeps non-equi-only EXISTS correlation rejected. A non-equi residual can be evaluated only
		// inside an already-keyed semi/anti probe; without at least one equality key it would be a cartesian
		// correlated search.
		"unsupported_non_equi_exists_correlation"
	} else if lowered.contains("derived table subquery must have exactly one from source") {
		// ADR 293/294: this was q07's raw derived comma-list wall before ADR 294 relaxed the derived-table
		// validator. Keep the classifier precise for old rows and targeted regression probes.
		"unsupported_derived_multi_source"
	} else if lowered.contains("join on only supports equality predicates") {
		// ADR 307's q13 row used this wall before ADR 308 added side-local ON residual pushdown. Keep
		// the classifier precise for old telemetry and targeted regression probes.
		"unsupported_non_equi_join_on"
	} else if lowered.contains("cross join requires a broadcast-eligible build side") {
		// A keyless CROSS JOIN is only admitted when one side can be broadcast. Keep this narrower than the
		// generic unsupported-query bucket so the gauge reports cartesian/keying walls honestly.
		"unsupported_cross_join"
	} else if lowered.contains("scalar subqueries in group by/having") {
		// ADR 314: q11's HAVING threshold is an uncorrelated aggregate scalar subquery. The frontend keeps
		// scalar subqueries out of GROUP BY/HAVING, so q11 walls at plan before its joins or Decimal arithmetic.
		"unsupported_having_scalar_subquery"
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
	pub diagnostic_message: Option<String>,
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
		let mut row = serde_json::json!({
			"suite": "tpch",
			"query": self.query,
			"scale_factor": self.scale_factor,
			"mode": self.mode,
			"phase": self.phase.as_str(),
			"status": self.status.as_str(),
			"reason": self.reason,
			"elapsed_ms": self.elapsed_ms,
			"digest": self.digest,
		});
		if let Some(message) = &self.diagnostic_message {
			row.as_object_mut()
				.expect("attempt JSON row is an object")
				.insert("diagnostic_message".to_string(), serde_json::json!(message));
		}
		row.to_string()
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

/// Discrete timings for the performance rail. `execute_ms` is the primary performance metric; the legacy
/// reachability `elapsed_ms` remains a broad wall-clock telemetry field.
#[derive(Debug, Clone, Copy, Default)]
pub struct PerfPhaseTimings {
	pub load_ms: f64,
	pub plan_ms: f64,
	pub bind_ms: f64,
	pub execute_ms: f64,
	pub digest_ms: f64,
	pub total_ms: f64,
}

/// ADR 584's opt-in local-admission receipt. Ordinary rows retain the compact
/// verdict-only list; diagnostic rows add the replay identity derived from the
/// optimizer trace target, so bind order is never mistaken for identity.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct TargetedLocalJoinAdmission {
	pub replay_id: u32,
	pub verdict: String,
}

impl PerfPhaseTimings {
	pub fn json_value(self) -> serde_json::Value {
		serde_json::json!({
			"load_ms": self.load_ms,
			"plan_ms": self.plan_ms,
			"bind_ms": self.bind_ms,
			"execute_ms": self.execute_ms,
			"digest_ms": self.digest_ms,
			"total_ms": self.total_ms,
		})
	}
}

/// How a persisted dataset was reconciled against its manifest before it backed a run.
///
/// The distinction is a chain-of-custody one and belongs on the row: `FullHash` re-read every table
/// file and compared its byte hash to the manifest, so the bytes that were measured are the bytes the
/// manifest names. `ManifestOnly` checked that the manifest is internally consistent with its own
/// recorded hashes and that the paths exist — it can not detect a file whose contents changed
/// underneath it. Without this field a reader cannot tell the two apart, and a `manifest_only` row
/// silently carries a weaker claim than a `full_hash` one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatasetVerification {
	FullHash,
	ManifestOnly,
}

impl DatasetVerification {
	pub fn as_str(self) -> &'static str {
		match self {
			DatasetVerification::FullHash => "full_hash",
			DatasetVerification::ManifestOnly => "manifest_only",
		}
	}
}

/// One performance-run record. The parent process augments child records with RSS and parent-observed wall
/// time before writing raw JSONL and summary rows.
#[derive(Debug, Clone)]
pub struct PerfAttempt {
	pub engine: String,
	pub query: String,
	pub scale_factor: f64,
	pub mode: String,
	pub run_index: u32,
	pub warmup: bool,
	pub phase: Phase,
	pub status: Status,
	pub reason: Option<String>,
	pub diagnostic_message: Option<String>,
	pub timings: PerfPhaseTimings,
	pub parent_wall_ms: Option<f64>,
	pub peak_rss_kib: Option<u64>,
	pub spill_bytes: u64,
	pub plan_fingerprint: Option<String>,
	pub digest: Option<String>,
	pub output_row_count: Option<u64>,
	pub output_schema_fingerprint: Option<String>,
	pub engine_version: Option<String>,
	pub duckdb_version: Option<String>,
	pub debug_build: bool,
	/// ADR 490: one compact admission receipt per binder-physicalized hash join,
	/// in bind order — toron rows only.
	pub local_join_admissions: Option<Vec<String>>,
	/// ADR 584: present only under the replay-input diagnostic. Every compact
	/// verdict above must have one trace-target-derived sibling here.
	pub targeted_local_join_admissions: Option<Vec<TargetedLocalJoinAdmission>>,
	/// ADR 584: per-replay, per-side input materialization and batch histograms.
	/// Empty ordinary task telemetry is surfaced as `None`, never as observed zero.
	pub join_input_replay_ledger: Option<Vec<toron::distributed::task::JoinInputReplayLedger>>,
	/// ADR 584: independent count at the shared concat primitive. The diagnostic
	/// is complete only when its replay-ledger concat counts sum to this value.
	pub join_input_total_concats: Option<u64>,
	/// Runtime hash-join path and attributed spill bytes, keyed by replay id.
	pub join_spill_observations: Option<Vec<toron::distributed::task::JoinSpillObservation>>,
	/// ADR 527: route-local requested/effective partition-work facts. An empty Toron vector means
	/// the query never reached the sole executor consumer; peer rows carry `None`.
	pub partition_work_concurrency_observations:
		Option<Vec<toron::distributed::task::PartitionWorkConcurrencyObservation>>,
	/// How the persisted dataset behind this row was verified. `None` means no persisted dataset
	/// backed the run — an in-memory fixture, or a peer engine row the harness did not load data for.
	pub dataset_verification: Option<DatasetVerification>,
}

/// Navigation hint supplied by ADR 581's runtime manifest. Retained for old artifact readers;
/// authority gates do not consult it.
pub fn build_commit() -> String {
	crate::evidence::runtime_commit_hint()
}

/// Modification time (unix seconds) of the executable actually running — the
/// runtime half of build identity: it distinguishes a stale on-disk binary from
/// the code the row claims to measure.
pub fn binary_modified_unix() -> Option<u64> {
	let exe = std::env::current_exe().ok()?;
	let modified = std::fs::metadata(exe).ok()?.modified().ok()?;
	modified
		.duration_since(std::time::UNIX_EPOCH)
		.ok()
		.map(|d| d.as_secs())
}

impl PerfAttempt {
	pub fn json_value(&self) -> serde_json::Value {
		let mut row = serde_json::json!({
			"suite": "tpch_perf",
			"engine": self.engine,
			"query": self.query,
			"scale_factor": self.scale_factor,
			"mode": self.mode,
			"run_index": self.run_index,
			"warmup": self.warmup,
			"profile": if self.debug_build { "debug" } else { "release" },
			"debug_build": self.debug_build,
			"phase": self.phase.as_str(),
			"status": self.status.as_str(),
			"reason": self.reason,
			"parent_wall_ms": self.parent_wall_ms,
			"peak_rss_kib": self.peak_rss_kib,
			"spill_bytes": self.spill_bytes,
			"plan_fingerprint": self.plan_fingerprint,
			"digest": self.digest,
			"output_row_count": self.output_row_count,
			"output_schema_fingerprint": self.output_schema_fingerprint,
			"engine_version": self.engine_version,
			"duckdb_version": self.duckdb_version,
			"build_commit": build_commit(),
			"evidence_identity": crate::evidence::runtime_identity_json(),
			"binary_modified_unix": binary_modified_unix(),
			"local_join_admissions": self.local_join_admissions,
			"join_spill_observations": self.join_spill_observations,
			"partition_work_concurrency_observations": self.partition_work_concurrency_observations,
			"dataset_verification": self.dataset_verification.map(DatasetVerification::as_str),
		});
		let object = row.as_object_mut().expect("perf JSON row is an object");
		let timings = self
			.timings
			.json_value()
			.as_object()
			.expect("timings JSON is an object")
			.clone();
		for (key, value) in timings {
			object.insert(key, value);
		}
		if let Some(receipts) = &self.targeted_local_join_admissions {
			object.insert(
				"targeted_local_join_admissions".to_string(),
				serde_json::json!(receipts),
			);
		}
		if let Some(ledger) = &self.join_input_replay_ledger {
			object.insert(
				"join_input_replay_ledger".to_string(),
				serde_json::json!(ledger),
			);
		}
		if let Some(total) = self.join_input_total_concats {
			object.insert(
				"join_input_total_concats".to_string(),
				serde_json::json!(total),
			);
		}
		if let Some(message) = &self.diagnostic_message {
			object.insert("diagnostic_message".to_string(), serde_json::json!(message));
		}
		row
	}

	pub fn jsonl(&self) -> String {
		self.json_value().to_string()
	}
}

/// ADR 528: one fragment-capability census outcome — plan-shape only, no execution, and no
/// timing authority (`elapsed_ms` is diagnostic). `classification` is the predeclared
/// vocabulary: `bind_rejected` (any pre-fragment wall; `phase` carries the exact stage),
/// `fragment_rejected` (`reason` carries the typed `FragmentError` slug), or
/// `fragments_sequential` / `fragments_parallel` (stage/width facts present). A fragmented
/// classification proves plan admission only, never executability.
#[derive(Debug, Clone)]
pub struct FragmentCensusRow {
	pub query: String,
	pub scale_factor: f64,
	pub mode: String,
	pub phase: Phase,
	pub classification: String,
	pub reason: Option<String>,
	pub plan_fingerprint: Option<String>,
	pub stage_count: Option<usize>,
	pub stage_task_counts: Option<Vec<usize>>,
	pub max_stage_width: Option<usize>,
	pub single_task_producer_stages: Option<usize>,
	pub live_eligible_consumer_tasks: Option<usize>,
	pub elapsed_ms: f64,
	pub diagnostic_message: Option<String>,
}

impl FragmentCensusRow {
	pub fn human(&self) -> String {
		format!(
			"tpch_fragment_census query={} sf={} mode={} phase={} classification={} reason={} stages={} widths={} max_width={} single_task_producers={}",
			self.query,
			self.scale_factor,
			self.mode,
			self.phase.as_str(),
			self.classification,
			self.reason.as_deref().unwrap_or("none"),
			self.stage_count
				.map_or("none".to_string(), |count| count.to_string()),
			self.stage_task_counts.as_ref().map_or_else(
				|| "none".to_string(),
				|counts| {
					counts
						.iter()
						.map(usize::to_string)
						.collect::<Vec<_>>()
						.join(",")
				}
			),
			self.max_stage_width
				.map_or("none".to_string(), |width| width.to_string()),
			self.single_task_producer_stages
				.map_or("none".to_string(), |count| count.to_string()),
		)
	}

	pub fn jsonl(&self) -> String {
		let mut row = serde_json::json!({
			"suite": "tpch_fragment_census",
			"query": self.query,
			"scale_factor": self.scale_factor,
			"mode": self.mode,
			"phase": self.phase.as_str(),
			"classification": self.classification,
			"reason": self.reason,
			"plan_fingerprint": self.plan_fingerprint,
			"stage_count": self.stage_count,
			"stage_task_counts": self.stage_task_counts,
			"max_stage_width": self.max_stage_width,
			"single_task_producer_stages": self.single_task_producer_stages,
			"live_eligible_consumer_tasks": self.live_eligible_consumer_tasks,
			"elapsed_ms": self.elapsed_ms,
			"build_commit": build_commit(),
			"evidence_identity": crate::evidence::runtime_identity_json(),
			"binary_modified_unix": binary_modified_unix(),
		});
		if let Some(message) = &self.diagnostic_message {
			row.as_object_mut()
				.expect("fragment census JSON row is an object")
				.insert("diagnostic_message".to_string(), serde_json::json!(message));
		}
		row.to_string()
	}
}

/// ADR 529: one execution-reachability result. Identity, rather than elapsed time, is the
/// authority. The complete distributed envelope travels as the typed `QueryBudget`; the explicit
/// task-memory envelope is separate because `memory_per_task * admitted_width` bounds concurrent
/// task allowances, not shuffle-store, transport, result, spill, or broadcast residency.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ExecutionCensusBackpressureReceipt {
	pub worker_id: toron::distributed::identity::WorkerId,
	pub observation: toron::distributed::partition_store::LivePartitionBackpressureObservation,
}

#[derive(Debug, Clone)]
pub struct ExecutionCensusRow {
	pub query: String,
	pub scale_factor: f64,
	pub mode: String,
	pub phase: Phase,
	pub classification: String,
	pub reason: Option<String>,
	pub local_plan_fingerprint: Option<String>,
	pub distributed_plan_fingerprint: Option<String>,
	pub local_digest: Option<String>,
	pub distributed_digest: Option<String>,
	pub local_row_count: Option<u64>,
	pub distributed_row_count: Option<u64>,
	pub local_schema_fingerprint: Option<String>,
	pub distributed_schema_fingerprint: Option<String>,
	pub stage_count: Option<usize>,
	pub stage_task_counts: Option<Vec<usize>>,
	pub live_eligible_consumer_tasks: Option<usize>,
	/// ADR 536 Phase 0, observe-only: the live publication structure behind this row.
	pub stage_upstreams: Option<Vec<(u32, Vec<u32>)>>,
	pub live_producer_stages: Option<usize>,
	pub multi_upstream_live_eligible_consumer_tasks: Option<usize>,
	pub live_residency_requirement: Option<usize>,
	pub admission_max_in_flight_tasks: Option<usize>,
	pub residency_exceeds_admission: Option<bool>,
	pub admitted_width: Option<usize>,
	pub task_memory_envelope_bytes: u64,
	pub local_memory_bytes: u64,
	pub local_spill_bytes: u64,
	pub distributed_budget: toron::distributed::budget::QueryBudget,
	pub worker_count: usize,
	pub transport: &'static str,
	pub partition_store: &'static str,
	pub memory_pool_retained_buffers: usize,
	pub memory_pool_max_buffer_capacity_bytes: usize,
	pub distributed_attempts: usize,
	pub transport_retry: bool,
	pub live_backpressure_receipts: Vec<ExecutionCensusBackpressureReceipt>,
	pub dataset_verification: Option<DatasetVerification>,
	pub local_elapsed_ms: Option<f64>,
	pub distributed_elapsed_ms: Option<f64>,
	pub total_elapsed_ms: f64,
	pub diagnostic_message: Option<String>,
}

impl ExecutionCensusRow {
	pub fn human(&self) -> String {
		format!(
			"tpch_execution_census query={} sf={} mode={} phase={} classification={} reason={} width={} memory_per_task={} max_in_flight={} transport={} store={} identity={}",
			self.query,
			self.scale_factor,
			self.mode,
			self.phase.as_str(),
			self.classification,
			self.reason.as_deref().unwrap_or("none"),
			self.admitted_width
				.map_or("none".to_string(), |width| width.to_string()),
			self.distributed_budget.memory_per_task.get(),
			self.distributed_budget
				.max_in_flight_tasks
				.map_or("none".to_string(), |count| count.to_string()),
			self.transport,
			self.partition_store,
			match (&self.local_digest, &self.distributed_digest) {
				(Some(local), Some(distributed)) if local == distributed => "equal",
				(Some(_), Some(_)) => "different",
				_ => "incomplete",
			},
		)
	}

	pub fn jsonl(&self) -> String {
		let mut row = serde_json::json!({
			"suite": "tpch_execution_census",
			"query": self.query,
			"scale_factor": self.scale_factor,
			"mode": self.mode,
			"phase": self.phase.as_str(),
			"classification": self.classification,
			"reason": self.reason,
			"local_plan_fingerprint": self.local_plan_fingerprint,
			"distributed_plan_fingerprint": self.distributed_plan_fingerprint,
			"local_digest": self.local_digest,
			"distributed_digest": self.distributed_digest,
			"local_row_count": self.local_row_count,
			"distributed_row_count": self.distributed_row_count,
			"local_schema_fingerprint": self.local_schema_fingerprint,
			"distributed_schema_fingerprint": self.distributed_schema_fingerprint,
			"stage_count": self.stage_count,
			"stage_task_counts": self.stage_task_counts,
			"live_eligible_consumer_tasks": self.live_eligible_consumer_tasks,
			"admitted_width": self.admitted_width,
			"task_memory_envelope_bytes": self.task_memory_envelope_bytes,
			"local_memory_bytes": self.local_memory_bytes,
			"local_spill_bytes": self.local_spill_bytes,
			"distributed_budget": self.distributed_budget,
			"worker_count": self.worker_count,
			"transport": self.transport,
			"partition_store": self.partition_store,
			"memory_pool_retained_buffers": self.memory_pool_retained_buffers,
			"memory_pool_max_buffer_capacity_bytes": self.memory_pool_max_buffer_capacity_bytes,
			"distributed_attempts": self.distributed_attempts,
			"transport_retry": self.transport_retry,
			"live_backpressure_receipts": self.live_backpressure_receipts,
			"dataset_verification": self.dataset_verification.map(DatasetVerification::as_str),
			"local_elapsed_ms": self.local_elapsed_ms,
			"distributed_elapsed_ms": self.distributed_elapsed_ms,
			"total_elapsed_ms": self.total_elapsed_ms,
			"timing_authority": false,
			"build_commit": build_commit(),
			"evidence_identity": crate::evidence::runtime_identity_json(),
			"binary_modified_unix": binary_modified_unix(),
		});
		if let Some(message) = &self.diagnostic_message {
			row.as_object_mut()
				.expect("execution census JSON row is an object")
				.insert("diagnostic_message".to_string(), serde_json::json!(message));
		}
		// ADR 536 Phase 0, observe-only. Inserted rather than declared in the `json!`
		// literal above, which is already at the macro's expansion depth.
		{
			let object = row
				.as_object_mut()
				.expect("execution census JSON row is an object");
			object.insert(
				"stage_upstreams".to_string(),
				serde_json::json!(self.stage_upstreams),
			);
			object.insert(
				"live_producer_stages".to_string(),
				serde_json::json!(self.live_producer_stages),
			);
			object.insert(
				"multi_upstream_live_eligible_consumer_tasks".to_string(),
				serde_json::json!(self.multi_upstream_live_eligible_consumer_tasks),
			);
			object.insert(
				"live_residency_requirement".to_string(),
				serde_json::json!(self.live_residency_requirement),
			);
			object.insert(
				"admission_max_in_flight_tasks".to_string(),
				serde_json::json!(self.admission_max_in_flight_tasks),
			);
			object.insert(
				"residency_exceeds_admission".to_string(),
				serde_json::json!(self.residency_exceeds_admission),
			);
		}
		row.to_string()
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
