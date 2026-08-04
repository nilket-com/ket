# Impl Plan — ADR 154: Measured Skew Miss Evidence

Pairs with `plans/adr/0154_measured_skew_miss_evidence.md`. **Toron-local evidence cut,
no behavior change** — deterministic fixtures, raw-miss + replay + absorbed gates, an
inert fact-shape contract, retarget guards, optional bench knob. It does **not** alter
selectivity, the chooser, replay, Wendelon, or the public EXPLAIN surface. All anchors
verified on `main`.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Uniform equality estimate | `binder.rs` `filter_equality_ndv_estimated_row_count:3941`, `predicate_node_equality_ndv_estimated_row_count:3954`, `equality_ndv_estimated_row_count:3998` |
| Uniform IN estimate | `in_list_ndv_estimated_row_count:4041`, `InListLiteralKey`, `estimated_rows_for_ndv:4683` |
| Estimate channel | `PlanOptimizerFacts::estimated_row_count`, `with_estimated_row_count` (asserted `:10534+`) |
| NDV seeding | `ExecutionPlan::Scan { column_ndv }` (`:4147`); seed pattern at `:10566` (`in_list_filtered_dim_with_wendelon_ndv_*`) |
| Broadcast choice | `JoinStrategyDecisionReason::BroadcastEstimated` (`:465`); `build_byte_candidate` |
| Replay path | `coordinator.rs broadcast_cap_replay_trigger:1350`, `force_hash_join_ids` (`binder.rs:78`), `BudgetDimension::BroadcastReplication` (`distributed/budget.rs:63`) |
| Existing replay tests (extend) | `lib.rs` `ndv_underestimate_over_cap_replays_as_hash:961`, `estimated_broadcast_over_cap_replays_as_hash_and_matches_digest:941` |
| Public EXPLAIN fence | `optimizer_explain_schema:5622` (must stay join-decision-only) |
| Bench | `benches/distributed_inner_equi_join.rs` `BenchConfig{build_rows, probe_key_domain}` |
| Test home | new `crates/toron/tests/skew_evidence.rs` (or `lib.rs` test mod beside distributed fixtures) + `tests/architecture/optimizer.rs` guards |

## Phase 1 — Deterministic hot-key fixture (direct `LogicalPlan`)

Test-only helpers (where distributed fixtures already live, `lib.rs`):
- `skewed_dim(base_rows N, ndv D, hot_key, hot_fraction f) -> (LogicalPlan, MockCatalog
  with column_ndv=D + row_upper_bound=N, hot_value, actual_hot_rows = floor(N·f))`;
- deterministic assignment: `key(i) = if i < actual_hot_rows { hot } else { tail(i % (D-1)) }`
  — **RNG-free**, exact `actual_hot_rows`;
- build the filtered build side with a **direct `LogicalPlan`** (`Filter(Scan)` on the
  build), *not* SQL `WHERE` over a join (avoids the 143-pushdown-fragility trap, D4).
Pick `N,D,H` so `ceil(N/D)` is tiny and `H = N·f ≫` it (e.g. N=10_000, D=100 → est 100;
f=0.5 → actual 5_000; 50× miss).

## Phase 2 — (a) raw-miss gate `skewed_hot_key_filter_records_uniform_ndv_miss`

- Bind `skewed_dim(...).filter(key == hot)`; read `scoped.facts.estimated_row_count`
  (the `:10534+` pattern) → assert `== ceil(N/D)`;
- assert `actual_hot_rows / estimate ≥ THRESHOLD` (a ratio, not bare inequality).
- Sibling `uniform_ndv_hot_key_in_list_miss_is_measured`: `key IN ('hot','tail_0')` →
  `== 2·ceil(N/D)` (148 distinct-literal handling) vs actual ≫.
- `skew_miss_measurement_does_not_change_estimated_row_count`: the bound facts match
  current 141/148 expectations — observational, not a correction.

## Phase 3 — (b) consequence gate `uniform_ndv_hot_key_miss_triggers_broadcast_replay`

Direct `fact ⋈ Filter(dim, key==hot)`, auto strategy, skewed catalog; runtime
`QueryBudget` broadcast cap **below** the actual filtered build but above `ceil(N/D)`:
1. assert the decision receipt is `BroadcastEstimated`;
2. run; assert success **via** replay — `BudgetExhausted(BroadcastReplication)` →
   `broadcast_cap_replay_trigger` → forced-hash digest match (139/140 contract). If the
   helper exposes scheduler telemetry, also assert `retry_count > 0`; else digest+success
   is the replay proof. Reuse the `lib.rs:941/961` harness; **keep** those tests.

## Phase 4 — (honest) `uniform_ndv_miss_within_headroom_is_absorbed_no_replay`

Same skew, cap sized **above** the actual build (miss within ~64× headroom): assert raw
miss still present, decision still `BroadcastEstimated`, execution completes **without**
replay. Pins that the miss matters only at the boundary (D6). Postpone only if the helper
can't expose the no-replay signal — do **not** weaken Phase 3 to ease this.

## Phase 5 — The fact-shape contract (inert, the forward deliverable)

A documentation-only, **not-wired** pseudo-Rust spec (reviewable, inert — no producer
exists; no live fact path change):
```rust
/// ADR 154 evidence deliverable; ADR 155 must produce a real fact like this.
/// NOT wired in 154. Tracked hot literal → estimate `frequency`; untracked →
/// uniform tail (base_rows − Σ frequency)/(D − k). Coverage follows 142.
pub struct HeavyHitterFact { column_ordinal: ColumnOrdinal, entries: Vec<HeavyHitterEntry>,
    exactness: FactExactness, coverage: FactCoverage }
struct HeavyHitterEntry { value_key: ScalarLiteralKey, frequency: u64 }
```
`ScalarLiteralKey` means a future serializable key with the same canonicalization semantics
as ADR 148's binder-local `InListLiteralKey`; it is not the private helper itself.
Placement: the promoted ADR/impl text, or a non-fact-path documentation module if the
implementation needs an in-repo anchor. **Do not** add it to the live fact path (that's 155).

## Phase 6 — Optional bench skew mode (secondary)

Extend `BenchConfig` with `hot_key_fraction` (+ a `BenchStrategy::Auto` lowering to
`JoinStrategyIntent::Auto`, a build-filter mode); seed row/NDV facts in `MockCatalog` when
skew is on; print estimate-vs-actual + over-cap incidence. Magnitude data for 155 sizing.
**Not** the gate (bench can't use crate-private trace; the checked-in tests are the proof).

## Phase 7 — Architecture guards (tests/architecture/optimizer.rs, adr154_*)

- `adr154_uniform_ndv_math_stays_current_behavior` — equality/`IN` still route through
  `estimated_rows_for_ndv`; no heavy-hitter/count-min branch consulted.
- `adr154_no_live_heavy_hitter_or_count_min_producer` — no `CountMin`/`HeavyHitter`/
  `TopK`/`frequency` producer/wire/schema/fact-path markers added; ADR/impl documentation
  and a non-fact-path documentation module remain allowed.
- `adr154_no_public_filter_explain_surface` — `optimizer_explain_schema` unchanged.
- `adr154_estimate_channel_and_replay_fences_untouched` — `with_estimated_row_count`
  estimate-only; `build_byte_candidate` prefers `byte_upper_bound`;
  `broadcast_cap_replay_trigger` keys only on `BroadcastReplication`.

## Verification

```sh
scripts/cargo-cgroup test -p toron skew_evidence
scripts/cargo-cgroup test -p toron skewed_hot_key_filter_records_uniform_ndv_miss
scripts/cargo-cgroup test -p toron uniform_ndv_hot_key_miss_triggers_broadcast_replay
scripts/cargo-cgroup test -p toron uniform_ndv_miss_within_headroom_is_absorbed_no_replay
scripts/cargo-cgroup test -p toron ndv_underestimate_over_cap_replays_as_hash   # still green
scripts/cargo-cgroup test -p toron --test architecture_test adr154
scripts/cargo-cgroup test -p toron --test architecture_test adr141
scripts/cargo-cgroup test -p toron --test architecture_test adr148
scripts/cargo-cgroup check -p toron --all-targets
git diff --check
```

## Risks

- **Accidentally fixing skew instead of measuring it** — no new planner branch/producer;
  the estimate must stay visibly wrong for the fixture.
- **Make-work smell** — Phase 3/4 assert *decisions* (BroadcastEstimated, replay-or-not),
  not just a row ratio; the evidence is the decision-boundary behavior.
- **SQL-shape trap** — SQL `WHERE` over a join may not land on the build side; the
  headline uses a direct `LogicalPlan` (D4).
- **Non-determinism** — RNG-free fixtures so estimate/actual/replay are exact in CI.
- **Replay false-proof** — pair success-under-tiny-cap with digest equality (and retry
  count if cheap) so the test proves *replay*, not mere execution.
- **Scope creep** — `HeavyHitterFact` stays an inert doc-stub; no Wendelon, no live fact,
  no public EXPLAIN expansion in 154.

## Deferred (named)

The heavy-hitter/top-k (or Count-Min) producer + consumer (ADR 155, builds to the Phase-5
contract); cold-key over-estimate → silently-missed-broadcast measurement (no runtime
signal); equality/`IN` selectivity correction; range/KLL skew; `CONVERT`/units.
