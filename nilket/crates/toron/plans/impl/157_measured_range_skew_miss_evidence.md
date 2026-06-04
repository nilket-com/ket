# Impl Plan — ADR 157: Measured Range-Skew Miss Evidence

Pairs with `plans/adr/157_measured_range_skew_miss_evidence.md`. **Toron-local, no behavior
change** — a deterministic range-skew fixture, the three-state gates, the inert quantile
contract, retarget guards. Mirrors ADR 154's evidence structure. No range-selectivity /
chooser / replay / Wendelon / public EXPLAIN change. All anchors verified on `main`.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Uniform range estimate | `binder.rs` `filter_range_selectivity_estimated_row_count:4215`, `estimated_rows_for_range:4559`, `estimated_rows_for_integral_range:4587` (`non_null_count × overlap/domain`); `range_scalar_for_fact` |
| Range fact | `RangeFact { arrow_type_tag, min, max, non_null_count }`; rides `Scan { column_range: Vec<(ColumnOrdinal, RangeFact)> }` (`ir/execution.rs:240`); reader `column_range_for_input:4488` |
| Estimate channel | `facts.estimated_row_count` |
| 154 infra (reuse) | `lib.rs::tests::skew_evidence` fixture + `..._and_replay_signal` runner (`.replayed`); `RangeFact` seeding pattern beside `column_ndv` |
| Broadcast/replay | `JoinStrategyDecisionReason::BroadcastEstimated:466`; `BudgetDimension::BroadcastReplication`; `broadcast_cap_replay_trigger` |
| EXPLAIN fence | `optimizer_explain_schema:5680` (stays join-decision-only; `adr133`/`adr154` guards) |
| Test home | `binder::tests` ((a) gates) + `lib.rs::tests::skew_evidence` ((b)/(c)) + new `tests/architecture/range_skew.rs` (guards) |

## Phase 1 — Deterministic clustered-range fixture (direct `LogicalPlan`)

Helper: build-side `customers` dim, `amount: Int64`,
- seeded `RangeFact { arrow_type_tag: Int64, min=0, max=10_000, non_null_count=10_000 }` on
  `Scan.column_range` (mirror 154's `column_ndv` seeding);
- **actual** clustered:
  ```rust
  amount(i) = if i < 9_000 {
      i % 101
  } else if i == 9_000 {
      10_000
  } else {
      101 + ((i - 9_001) % 9_899)
  }
  ```
  (exactly 9_000 in `[0,100]`, every tail row outside the filter, and at least one row at
  10_000 to make the envelope honest) — **RNG-free**, exact `CLUSTER_ROWS = 9_000`;
- filtered build via **direct `LogicalPlan` `Filter(Scan, amount BETWEEN 0 AND 100)`** (not SQL
  `WHERE` — pushdown-fragility trap).
Reuse 154's `skewed_*` catalog/runner helpers, parameterized for a `RangeFact` + range predicate.

## Phase 2 — (a) raw-miss gate `range_skew_filter_records_min_max_uniform_miss`

Bind `Filter(dim, amount BETWEEN 0 AND 100)`; assert `scoped.facts.estimated_row_count ==
RowCount::new(101)` (`ceil(10_000 × 101 / 10_001)`, exact from the seeded RangeFact); assert
`CLUSTER_ROWS (9_000) / 101 ≥ THRESHOLD` (e.g. ≥25×). Sibling
`range_skew_measurement_does_not_change_estimated_rows_for_range` (the 144/147 path unchanged).

## Phase 3 — (b) consequence gate `range_skew_miss_triggers_broadcast_replay`

Direct `fact ⋈ Filter(dim, amount BETWEEN 0 AND 100)`, auto strategy; the planner threshold
accepts the 101-row estimated bytes, while runtime `QueryBudget` broadcast cap sits **below**
the actual 9,000-row build bytes:
- assert decision receipt `BroadcastEstimated`;
- run via the 154 replay-signal runner → `replayed == true`, `BudgetExhausted(BroadcastReplication)`,
  digest matches forced-hash. Reuse `lib.rs::skew_evidence`; keep 154's gates green.

## Phase 4 — (c) honest gate `range_skew_miss_within_headroom_is_absorbed_no_replay`

Same skew, default cap **above** the materialized build (within ~64× headroom): raw miss present, decision
`BroadcastEstimated`, `replayed == false`. Pins the miss matters only at the boundary.

## Phase 5 — Quantile fact contract (inert, the forward deliverable)

Doc-commented / ADR-text only (no live fact, no Wendelon — that's 158):
```rust
/// ADR 157 evidence deliverable; the fact ADR 158 must produce. NOT wired in 157.
/// Refines IN-ENVELOPE selectivity; min/max stays the envelope/zero-out fact.
struct ColumnRangeDistributionFact {
    column_ordinal: u16, arrow_type_tag: u8, non_null_count: u64,
    family: Kll, payload: RangeDistributionPayload, // KLL params+rank_error_ppm | equi-depth buckets
}
```
Numeric/Date32/Timestamp; all-contributors coverage (142/156); estimate-channel only; **not** a
`HeavyHitterFact` top-k. `KllSketch` (aggregate state today) is a different contract.

## Phase 6 — Guards (new tests/architecture/range_skew.rs, adr157_*)

- `adr157_range_selectivity_is_min_max_uniform_no_quantile_fact` — range still routes through
  `estimated_rows_for_range`/`estimated_rows_for_integral_range`; no quantile/KLL fact consulted
  (the 158 retarget anchor).
- `adr157_no_live_range_distribution_optimizer_fact_or_producer` — the inert stub is ADR-text, not
  a fact path; no `ColumnRangeDistributionFact`/quantile producer wired (Toron or Wendelon).
- `adr157_public_explain_surface_stays_closed` — `optimizer_explain_schema` unchanged (brainstorm
  010: gauges are Rune-future, not SQL EXPLAIN).
- `adr157_min_max_envelope_pruning_unchanged` + estimate-channel/replay fences.
- **Land in `range_skew.rs`**, not `optimizer.rs` (over the adr077 ceiling).

## Verification

```sh
scripts/cargo-cgroup test -p toron range_skew
scripts/cargo-cgroup test -p toron range_skew_filter_records_min_max_uniform_miss
scripts/cargo-cgroup test -p toron range_skew_miss_triggers_broadcast_replay
scripts/cargo-cgroup test -p toron range_skew_miss_within_headroom_is_absorbed_no_replay
scripts/cargo-cgroup test -p toron --test architecture_test adr157
scripts/cargo-cgroup test -p toron --test architecture_test   # FULL binary (review-checklist; expect only the 2 known pre-existing reds)
scripts/cargo-cgroup test -p toron --test architecture_test adr144   # range selectivity unchanged
git diff --check
```

## Risks

- **Accidentally "fixing" range skew** — no new selectivity branch; the uniform estimate must stay
  visibly wrong (the observational gate pins it).
- **Make-work smell** — Phase 3/4 assert *decisions* (`BroadcastEstimated`, replay-or-not), not just
  a ratio.
- **Non-determinism** — RNG-free fixture so estimate/actual/replay are exact in CI.
- **SQL-shape trap** — direct `LogicalPlan` for the build-side range filter.
- **min/max mislabeled "wrong"** — the gate must assert envelope pruning still correct; the miss is
  in-envelope uniformity only.
- **Contract drift into top-k** — `ColumnRangeDistributionFact` is a quantile/distribution, not a
  `HeavyHitterFact`; keep the fence.

## Deferred (named)

The KLL/quantile range-distribution producer + consumer (ADR 158, builds to the Phase-5 contract);
Date32/Timestamp/float range-skew headlines (Int64 is v1); cold-region over-estimate →
silently-missed-broadcast (no runtime signal); multi-column/correlated range skew; the future Rune
optimizer cockpit (brainstorm 010); the adr077/adr031 hygiene (separate commit + full-architecture-
binary review checklist).
