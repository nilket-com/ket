# Impl Plan - ADR 187: Optimizer Decision Accrual

Pairs with `plans/adr/0187_optimizer_decision_accrual.md`.

Accrue the optimizer's join-order decisions across the workload into a coordinator-scoped,
config-threaded `OptimizerDecisionLog`, recorded always-on by the binder at the 186 commit
points (committed-only, no scratch over-count) and surfaced as a queryable
`information_schema.optimizer_decisions` report. Decisions-only, side-effect-only, in-memory; no
executor / `fragment.rs` / storage / Wendelon change.

## Anchors

| Concern | Anchor |
|---|---|
| Commit / decline points (record here) | `try_bind_star_order_actuation` (`binder/join_order.rs`): decline returns `Ok(None)`; `Actuated(real_root)` + `branch_receipts` |
| Level kind | `SnowflakeStarTree::has_branch_dimensions()` (`binder/join_order.rs`) |
| Outcome | `StarOrderAction` (Reordered/KeptAlreadyOptimal/KeptInsufficientMargin) / `StarOrderDeclineReason` |
| Config thread | `PlannerConfig` (borrowed into `Binder { catalog, config }` and ~12 coordinator `bind` sites) |
| Coordinator holder | `QueryCoordinator::new` (`coordinator.rs`) constructs `self.config` |
| Queryable report path | `bind_information_schema_relation` (`binder.rs:656`); `SystemTableKind::InformationSchema*`; `INFORMATION_SCHEMA_*` consts (`binder.rs:83+`) |
| Tests | `binder/tests.rs`; coordinator tests; `tests/architecture/` (new `optimizer_accrual.rs`) |

## Phase 1 - The decision log (`binder/decision_log.rs`)

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum OptimizerDecisionKind {
    StarCandidate,
    FlatStar,
    SnowflakeRoot,
    SnowflakeBranch,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum OptimizerDecisionOutcome {
    Reordered, KeptOptimal, KeptThinMargin, Declined(StarOrderDeclineReason),
}

const DEFAULT_OPTIMIZER_DECISION_LOG_MAX_KEYS: usize = 1024;

#[derive(Debug)]
pub(crate) struct OptimizerDecisionLog {
    counts: Mutex<BTreeMap<(OptimizerDecisionKind, OptimizerDecisionOutcome), u64>>,
    overflow_count: AtomicU64,
    max_keys: usize,
}

impl Default for OptimizerDecisionLog {
    fn default() -> Self {
        Self::with_max_keys(DEFAULT_OPTIMIZER_DECISION_LOG_MAX_KEYS)
    }
}

impl OptimizerDecisionLog {
    pub(crate) fn with_max_keys(max_keys: usize) -> Self { /* initialize empty bounded log */ }
    pub(crate) fn record(&self, kind: OptimizerDecisionKind, outcome: OptimizerDecisionOutcome) {
        // If the key exists, increment it. If the key cap is exhausted, increment overflow_count
        // instead of growing unboundedly.
    }
    pub(crate) fn snapshot(&self) -> Vec<OptimizerDecisionRow> {
        // sorted by (kind, outcome) for determinism, plus one overflow row when non-zero
    }
}
```

Shared as `Arc<OptimizerDecisionLog>`. `Default` yields an empty throwaway log for
standalone/test configs with the default key cap. Keep the cap small enough to test overflow
directly, but large enough that ordinary sessions do not immediately collapse into overflow.

## Phase 2 - Thread via `PlannerConfig`

- Add `pub(crate) decision_log: Arc<OptimizerDecisionLog>` to `PlannerConfig` with a `Default`.
- `QueryCoordinator::new` builds one `Arc<OptimizerDecisionLog>` into `self.config.decision_log`;
  the ~12 `bind(plan, catalog, &self.config)` sites are unchanged.
- **Run `cargo check -p toron --all-targets`** — the new field breaks literal
  `PlannerConfig { .. }` constructions in benches/examples (E0063 passes `cargo test`). Add
  `..Default::default()` or the field to each.

## Phase 3 - Committed-only recording (`binder/join_order.rs`)

Map outcomes once:

```rust
fn decision_outcome(action: StarOrderAction) -> OptimizerDecisionOutcome { /* Reordered/KeptOptimal/KeptThinMargin */ }
fn root_kind(tree: &SnowflakeStarTree) -> OptimizerDecisionKind {
    if tree.has_branch_dimensions() { SnowflakeRoot } else { FlatStar }
}
```

In `try_bind_star_order_actuation`:

- **Pre-tree declines** (`MissingRelationLabel`, `ProbeNotScanLinear`, tree-classification
  failure before `SnowflakeStarTree` exists): record `Declined(reason)` under `StarCandidate`.
- **Decline returns** (`MissingEstimate` and scratch `Declined(reason)` → `Ok(None)`): once, under
  the same `!force_hash_root` guard that wraps `record_star_order_decline`,
  `config.decision_log.record(root_kind(&tree), Declined(reason))`.
- **Commit** (after `LocalStarActuationAttempt::Actuated(real_root)`):
  - `config.decision_log.record(root_kind(&tree), decision_outcome(real_root.action))`;
  - for each committed `branch_receipt` whose outcome is `Recognized(evidence)`:
    `config.decision_log.record(SnowflakeBranch, decision_outcome(evidence.action))`.
- The scratch/preflight `bind_local_star_actuation`/`bind_snowflake_root_dimensions` calls record
  nothing. Recording is independent of `render_trace`.

"Committed" = the real (non-scratch) pass within a bind, not cross-query dedup. A broadcast-cap
force-hash replay re-binds, so a replayed query accrues both its pre-cap and forced-hash real
decisions; replay dedup is a noted minor follow-on.

## Phase 4 - The queryable report (`binder.rs`)

- Add `SystemTableKind::InformationSchemaOptimizerDecisions`, the
  `information_schema.optimizer_decisions` name const + table const, and the schema
  (`decision_kind` Utf8, `outcome` Utf8, `decline_reason` Utf8-nullable, `count` Int64).
- Extend `bind_information_schema_relation`: read `config.decision_log.snapshot()` → one row per
  `(decision_kind, outcome, decline_reason, count)` into a `SystemTable` plan, mirroring the other
  information-schema tables.
- Include one deterministic overflow row when `overflow_count > 0`, for example
  `decision_kind='overflow'`, `outcome='overflow'`, `decline_reason=NULL`, `count=<overflow>`.
- Reject DDL/writes against it like the other information-schema relations.

## Phase 5 - Behavioral tests (`binder/tests.rs`, coordinator tests)

- `optimizer_decisions_accrue_across_queries` - one coordinator binds a reordered star, a
  kept-declared star, a declined snowflake; the report returns summed counts per
  `(decision_kind, outcome)`.
- `optimizer_decision_accrual_is_always_on` - bind a reordering star via normal `bind` (no
  EXPLAIN); the `(FlatStar, Reordered)` count increments.
- `optimizer_decision_accrual_is_side_effect_only` - a **non-report** star query's bound
  `ExecutionPlan` byte-identical with a real vs empty log; results unchanged.
- `optimizer_decisions_record_committed_not_scratch` - a bushy snowflake commit records one
  `SnowflakeRoot` + one `SnowflakeBranch` (not scratch); a mixed-root snowflake records exactly
  one `Declined` root entry and no branch entry.
- `optimizer_decisions_report_aggregates_by_kind_and_outcome` - repeated identical queries bump
  one row's count.
- `optimizer_decisions_records_decline_reasons` - mixed/broadcast-root snowflake →
  `Declined(mixed_strategy_star)` row.
- `optimizer_decisions_records_unclassified_candidate_declines` - a pre-tree decline records as
  `StarCandidate`, not `FlatStar` or `SnowflakeRoot`.
- `optimizer_decisions_records_per_level_for_recursive_snowflake` - bushy snowflake → a
  `SnowflakeRoot` and a `SnowflakeBranch` entry.
- `optimizer_decisions_log_is_bounded` - force the cap with distinct keys and assert a stable
  overflow row.
- `rune_queries_optimizer_decisions` - Rune `SELECT`s the report.

## Phase 6 - Architecture guards (`tests/architecture/optimizer_accrual.rs`)

- `adr187_decision_log_is_coordinator_scoped_shared` - `QueryCoordinator` holds the
  `Arc<OptimizerDecisionLog>`; it is in `PlannerConfig`, not per-query Binder state.
- `adr187_accrual_recording_is_always_on_not_explain_gated` - the `record(` calls are not guarded
  by `render_trace.is_some()`.
- `adr187_accrual_records_committed_not_scratch` - `record(` is at the commit / decline-return
  paths, not inside the scratch `bind_local_star_actuation`/`bind_snowflake_root_dimensions` calls.
- `adr187_accrual_is_side_effect_only` - `decision_log` is recorded + snapshot-read only; no
  planning decision matches it as an input.
- `adr187_report_reuses_information_schema_path` - the report binds via
  `bind_information_schema_relation`; no new render/system-table machinery.
- `adr187_decisions_only_no_runtime_capture` - the log has no `JoinRuntimeObservation`/output-row
  field.
- `adr187_decision_log_is_bounded` - the log has `max_keys` and an overflow counter/row; it is not
  an unbounded workload sink.
- `adr187_no_executor_fragment_storage_or_wendelon_change` - marker sweep: `OptimizerDecisionLog`
  absent from `exec.rs`, `fragment.rs`, `pbc.rs`, `storage.rs`, `distributed/`, Wendelon.
- `adr187_no_optimizer_shell_or_planproperties_contract` - 177's no-shell invariant holds.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets        # PlannerConfig new-field breadth
scripts/cargo-cgroup test -p toron --lib optimizer_decision
scripts/cargo-cgroup test -p toron --lib star_order      # 177-181 plans byte-identical
scripts/cargo-cgroup test -p toron --lib snowflake hash_snowflake   # 182-186 unchanged
scripts/cargo-cgroup test -p toron --test architecture_test adr187
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --lib
git diff --check
```

## Risks

- **`PlannerConfig` field breadth.** New field breaks literal `PlannerConfig { .. }` in
  benches/examples (passes `cargo test`, fails `--all-targets`). Default it; run `--all-targets`.
  (Memory: verify-all-targets-on-a-new-IR-field.)
- **Committed-only.** Record at commit/decline returns, never the scratch preflight, or
  commit-or-discard over-counts. Pin with `optimizer_decisions_record_committed_not_scratch`.
- **Side-effect-only.** The log never feeds planning; recording is a pure side-write, report bind
  read-only. Pin with `optimizer_decision_accrual_is_side_effect_only` + the guard.
- **Always-on, not EXPLAIN-gated.** Record independent of `render_trace`. Pin with
  `optimizer_decision_accrual_is_always_on`.
- **Concurrency.** Concurrent coordinator binds write the shared `Mutex`; keep the record call
  lock-local; never hold the log lock across a bind/await.
- **Config smell.** Mutable handle in `PlannerConfig` is pragmatic; documented as a sink handle,
  guarded side-effect-only. Dedicated context rejected (more churn).
- **Determinism.** `snapshot()` sorts by `(kind, outcome, reason)`; the report is stable.
- **Unclassified declines.** Some declines occur before the snowflake tree exists. Record those as
  `StarCandidate` so the report does not invent a flat/root classification the binder did not
  prove.
- **Unbounded telemetry.** Long-lived coordinators can see many distinct keys. Cap aggregate keys
  and expose overflow deterministically.

## Deferred

Always-on measured/runtime verdict accrual; persistence; margin self-tuning; scan/filter/strategy
accrual; broadcast cliff; mixed-strategy cost model; galaxy/bridge.
