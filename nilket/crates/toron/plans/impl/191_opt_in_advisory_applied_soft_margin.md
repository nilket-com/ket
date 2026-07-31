# Impl Plan - ADR 191: Opt-In Advisory-Applied Soft Margin

Pairs with `plans/adr/191_opt_in_advisory_applied_soft_margin.md`.

Add a default-off planner application mode. In `Manual`, the ADR 190 configured
`join_order_soft_margin_step` remains the effective step. In `ApplyAdvisory`, the effective step is
the ADR 189 `recommend_soft_margin(...).recommended_step` computed from the configured base and the
current verdict snapshot. Resolve once at bind entry, thread the resolved step through existing
join-order seams, and append report columns that show the applied margin.

## Anchors

| Concern | Anchor |
|---|---|
| Config surface | `PlannerConfig` in `crates/toron/src/binder.rs` |
| Existing step type | `SoftMarginLadderStep` in `crates/toron/src/binder/join_order.rs` |
| Existing policy | `recommend_soft_margin(snapshot, current_step) -> MarginAdvisory` |
| Existing advisory report | `information_schema_optimizer_margin_advisory_schema` and `_rows` in `binder/explain.rs` |
| Binder entry points | `bind_with_trace`, `bind_with_explain_trace` in `binder.rs` |
| Star actuation reads | `self.config.join_order_soft_margin_step.margin()` in `binder/join_order.rs` |
| Observed EXPLAIN reads | `analyze_snowflake(..., config.join_order_soft_margin_step)` and `analyze(...)` |
| 190 guards to retarget | `tests/architecture/optimizer_accrual.rs` |
| Behavior tests | `crates/toron/src/binder/tests.rs` |

## Phase 1 - Typed application mode

In `binder.rs`, add the config enum near `PlannerConfig`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinOrderSoftMarginApplication {
    Manual,
    ApplyAdvisory,
}
```

Add to `PlannerConfig`:

```rust
pub join_order_soft_margin_application: JoinOrderSoftMarginApplication,
```

Default it to `JoinOrderSoftMarginApplication::Manual`.

Do not add `join_order_margin_auto_tune: bool`; the architecture guard should reject that shape.

## Phase 2 - Expose advisory steps

In `binder/join_order.rs`, extend `MarginAdvisory`:

```rust
pub(crate) current_step: SoftMarginLadderStep,
pub(crate) recommended_step: SoftMarginLadderStep,
```

Set them in `recommend_soft_margin`:

```rust
MarginAdvisory {
    current_step: current,
    recommended_step,
    current: current.margin(),
    recommended: recommended_step.margin(),
    ...
}
```

Keep the existing `current` and `recommended` fields so current report rendering and tests can be
retargeted with minimal churn.

## Phase 3 - Effective margin resolver

Add a pure helper, preferably in `binder/join_order.rs` where the margin policy lives:

```rust
pub(crate) fn effective_join_order_soft_margin_step(
    config: &PlannerConfig,
) -> SoftMarginLadderStep {
    match config.join_order_soft_margin_application {
        JoinOrderSoftMarginApplication::Manual => config.join_order_soft_margin_step,
        JoinOrderSoftMarginApplication::ApplyAdvisory => recommend_soft_margin(
            &config.decision_log.verdict_snapshot(),
            config.join_order_soft_margin_step,
        )
        .recommended_step,
    }
}
```

Properties to preserve:

- reads the verdict snapshot only under `ApplyAdvisory`;
- uses the configured base step as the advisory input;
- returns exactly `recommended_step`;
- does not mutate config or the decision log;
- contains no loop or repeated ladder walk.

## Phase 4 - Resolve once per bind

Add an effective-step field to `Binder`:

```rust
struct Binder<'a> {
    catalog: &'a dyn Catalog,
    config: &'a PlannerConfig,
    join_order_soft_margin_step: SoftMarginLadderStep,
}
```

Add a constructor to avoid updating every call site with a raw struct literal:

```rust
impl<'a> Binder<'a> {
    fn new(catalog: &'a dyn Catalog, config: &'a PlannerConfig) -> Self {
        Self {
            catalog,
            config,
            join_order_soft_margin_step: effective_join_order_soft_margin_step(config),
        }
    }
}
```

Then update `bind_with_trace`, `bind_with_explain_trace`, and test helpers/direct tests to
construct with `Binder::new(catalog, config)`.

For `bind_with_explain_trace`, copy the resolved step before moving the binder into the async bind:

```rust
let binder = Binder::new(catalog, config);
let join_order_soft_margin_step = binder.join_order_soft_margin_step;
let plan = binder.bind_plan_with_trace(...).await?;
...
analyze_snowflake(..., join_order_soft_margin_step)
analyze(..., join_order_soft_margin_step)
```

This keeps observed star/snowflake receipts on the same effective step used by actuation earlier
in the bind.

## Phase 5 - Swap join-order call sites

In `binder/join_order.rs`, replace the two star actuation reads:

```rust
self.config.join_order_soft_margin_step.margin()
```

with:

```rust
self.join_order_soft_margin_step.margin()
```

In `bind_with_explain_trace`, pass the copied `join_order_soft_margin_step` into
`analyze_snowflake` and `analyze`.

Do not call `effective_join_order_soft_margin_step` inside low-level star/snowflake helpers. The
resolver belongs at bind entry.

## Phase 6 - Advisory report surface

Add a small report struct/helper so the table does not recompute from the applied step:

```rust
pub(crate) struct MarginAdvisoryReport {
    pub(crate) advisory: MarginAdvisory,
    pub(crate) application: JoinOrderSoftMarginApplication,
    pub(crate) applied_step: SoftMarginLadderStep,
}

pub(crate) fn margin_advisory_report(config: &PlannerConfig) -> MarginAdvisoryReport {
    let advisory = recommend_soft_margin(
        &config.decision_log.verdict_snapshot(),
        config.join_order_soft_margin_step,
    );
    let applied_step = match config.join_order_soft_margin_application {
        JoinOrderSoftMarginApplication::Manual => config.join_order_soft_margin_step,
        JoinOrderSoftMarginApplication::ApplyAdvisory => advisory.recommended_step,
    };
    MarginAdvisoryReport {
        advisory,
        application: config.join_order_soft_margin_application,
        applied_step,
    }
}
```

Use that helper in `bind_information_schema_relation`.

Append fields to `information_schema_optimizer_margin_advisory_schema`:

```rust
Field::new("application_mode", DataType::Utf8, false),
Field::new("applied_margin", DataType::Utf8, false),
```

Append row values:

```rust
ScalarValue::Utf8(Some(join_order_soft_margin_application_label(report.application).to_string())),
ScalarValue::Utf8(Some(report.applied_step.margin().label())),
```

Append at the tail so existing `current_margin`, `recommended_margin`, and `recommendation` column
indexes remain stable.

## Phase 7 - Behavioral tests

Add or retarget tests in `binder/tests.rs`.

Core helpers:

- extend `margin_config(step)` with default `Manual`;
- add `auto_applied_margin_config(base_step, verdicts)` or a builder that shares an
  `Arc<OptimizerDecisionLog>`;
- reuse existing `record_verdicts`, `optimizer_margin_advisory_rows`, and the thin soft star
  fixtures around `(1_000, 850)` or `(1_100, 1_000)`.

Required tests:

- `auto_applied_margin_uses_advisory_recommendation` - base `Baseline`, seeded `MissedWin`
  verdicts over the min sample, ApplyAdvisory. Assert the borderline star reorders and the receipt
  contains `soft_margin=9/10`. Bind the same query under Manual and assert it keeps or differs from
  the auto plan as expected.
- `manual_application_mode_is_190_byte_identical` - with the same seeded verdicts, Manual uses the
  configured base. Compare the bound plan/debug receipt with the existing ADR 190 baseline path.
- `advisory_report_does_not_double_step_auto_application` - base `Baseline`, loosen verdicts,
  ApplyAdvisory. Assert advisory columns: `current_margin=4/5`, `recommended_margin=9/10`,
  `recommendation=loosen`, `application_mode=apply_advisory`, `applied_margin=9/10`.
- `auto_application_insufficient_evidence_uses_configured_margin` - below min sample, applied
  margin equals configured base.
- `auto_application_hysteresis_keep_uses_configured_margin` - balanced or low-net verdicts inside
  hysteresis, applied equals configured base.
- `auto_application_tightens_on_overeager` - base `Baseline`, `OverEager` verdicts, applied
  `Tight` / `7/10`.
- `auto_application_is_bounded_to_one_step` - extreme MissedWin from `Baseline` applies `Loose`,
  not `Loosest`; extreme OverEager applies `Tight`, not `Tightest`.
- `auto_application_does_not_mutate_config_or_log` - call the resolver/report path and assert the
  config field remains the base step, verdict snapshot counts are unchanged, and no margin state is
  written. For a full bind, allow ordinary optimizer decision accrual while still asserting no
  verdict or margin-state write.
- `observed_snowflake_uses_auto_applied_margin` - EXPLAIN-only snowflake observation renders
  `soft_margin=9/10` under ApplyAdvisory with a loosen recommendation.
- `rune_respects_auto_applied_margin` - Rune-authored equivalent plan sees the same effective
  margin and report columns.

Keep the 189 advisory tests green by retargeting them to the new `current_step` and
`recommended_step` fields where useful.

## Phase 8 - Architecture guards

Update `tests/architecture/optimizer_accrual.rs`.

Retarget ADR 190 guards:

- `adr190_observed_paths_receive_configured_margin` becomes a manual/default guard: observed paths
  receive the resolved effective step, and default Manual makes it the configured step.
- `adr190_advisory_current_is_configured_margin` remains true for the advisory input. Assert the
  report helper calls `recommend_soft_margin(..., config.join_order_soft_margin_step)` rather than
  from the applied step.
- `adr190_operator_applied_not_auto` becomes the ADR 191 invariant: no reassignment to
  `join_order_soft_margin_step`, and the only derived path is the opt-in resolver.

Add ADR 191 guards:

- `adr191_margin_application_mode_is_typed` - enum exists; `PlannerConfig` has
  `join_order_soft_margin_application: JoinOrderSoftMarginApplication`; default is `Manual`; no
  `join_order_margin_auto_tune` marker exists.
- `adr191_effective_margin_resolved_once_at_bind_entry` - `Binder` stores
  `join_order_soft_margin_step`; `Binder::new` calls `effective_join_order_soft_margin_step`; star
  actuation reads `self.join_order_soft_margin_step`; low-level helpers do not read
  `verdict_snapshot()`.
- `adr191_auto_apply_reuses_189_policy_only` - resolver block contains `recommend_soft_margin` and
  `.recommended_step`, has no policy constants of its own, and no loop over ladder steps.
- `adr191_report_preserves_configured_recommended_and_applied_margins` - schema contains existing
  columns plus tail columns `application_mode` and `applied_margin`; report helper feeds the
  configured step into `recommend_soft_margin`.
- `adr191_retargets_190_operator_applied_not_auto` - source scan for
  `.join_order_soft_margin_step =` remains empty; `decision_log.rs` still does not mention the
  margin step or application mode.
- `adr191_default_manual_preserves_190` - `PlannerConfig::default` sets Manual and the baseline
  step constant.
- `adr191_no_executor_fragmenter_storage_or_wendelon_change` - markers
  `JoinOrderSoftMarginApplication`, `effective_join_order_soft_margin_step`, and `applied_margin`
  are absent from executor, PBC, storage, distributed fragment/scheduler, and Wendelon.
- `adr191_no_optimizer_shell_or_planproperties_contract` - reuse the no-shell sweep for
  `Optimizer`, `Memo`, `Cascades`, and `PlanProperties`.

## Verification

Run targeted gates first, then the broader net:

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib auto_applied_margin_uses_advisory_recommendation
scripts/cargo-cgroup test -p toron --lib manual_application_mode_is_190_byte_identical
scripts/cargo-cgroup test -p toron --lib advisory_report_does_not_double_step_auto_application
scripts/cargo-cgroup test -p toron --lib auto_application
scripts/cargo-cgroup test -p toron --lib observed_snowflake_uses_auto_applied_margin
scripts/cargo-cgroup test -p toron --lib rune_respects_auto_applied_margin
scripts/cargo-cgroup test -p toron --lib margin_advisory
scripts/cargo-cgroup test -p toron --lib operator_margin
scripts/cargo-cgroup test -p toron --lib star_order
scripts/cargo-cgroup test -p toron --lib snowflake
scripts/cargo-cgroup test -p toron --test explain_analyze_test
scripts/cargo-cgroup test -p toron --test architecture_test adr190
scripts/cargo-cgroup test -p toron --test architecture_test adr191
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --lib
git diff --check
```

Use one cargo test filter per command; do not rely on multi-filter invocations.

## Risks

- **Double-step reporting.** Avoid recomputing the advisory from the already-applied margin. Pin
  with `advisory_report_does_not_double_step_auto_application`.
- **Repeated live snapshot reads.** Resolve once at bind entry and store the effective step in
  `Binder`. Pin with source guards around `Binder::new` and low-level join-order helpers.
- **Default behavior drift.** Manual must be the default and must preserve ADR 190 plans. Pin with
  the byte-identical manual gate and ADR 177-190 regressions.
- **Config literal breadth.** Adding a `PlannerConfig` field touches many tests. Use
  `Binder::new` and `..Default::default()`/existing helpers where possible; run `--all-targets`.
- **Operator visibility.** Do not hide the configured base when auto application is enabled.
  Append report columns instead of replacing semantics.

## Deferred

Broadcast-snowflake execution baseline; mixed-strategy cost model; galaxy/bridge topology;
multi-step advisory convergence; tuned-step persistence; per-kind margin policies.
