# ADR 252 — IGNORE / RESPECT NULLS for Value Window Functions

## Status

Proposed.

## Context

The value window functions (ADR 246 — LAG/LEAD/FIRST_VALUE/LAST_VALUE/NTH_VALUE) navigate to a row
and return its value, counting **physical** rows. SQL's `IGNORE NULLS` modifier changes the
navigation to skip NULL values: `LAG(x, 1) IGNORE NULLS` returns `x` at the nearest *prior row whose
`x` is non-NULL*; `LAST_VALUE(x) IGNORE NULLS` returns the last non-NULL `x` in the frame. `RESPECT
NULLS` is the default — today's behavior. Toron parses the modifier (sqlparser
`null_treatment: Option<NullTreatment>`) but rejects it (`sql_planner.rs:3977/4098`,
"IGNORE/RESPECT NULLS is not supported"). This is the substantive remaining window feature.

## Decision

Carry the null treatment as **`ignore_nulls: bool`** on the window call — `WindowFunctionCall`
(logical) and `BoundWindowCall` (execution) — defaulting `false` (RESPECT = today's semantics, a
no-op). It is a call-level modifier alongside `frame`. Scope to **value functions only**: IGNORE /
RESPECT NULLS on ranking / NTILE / aggregate windows stays rejected (`RESPECT NULLS` on a value call
is accepted as the no-op default).

### Lowering (both frontends)

- **SQL**: `extract_value_function` stops rejecting `null_treatment` — maps `IgnoreNulls → true`,
  `RespectNulls → false`. The generic `extract_window_call` keeps rejecting `null_treatment` for
  ranking / aggregate.
- **Rune**: a `WindowFunctionCall::ignore_nulls()` method (sets the flag), registered in the module —
  parallels `.frame()` / `.named()` / `.with_default()`. Errors on a non-value call (Rune-primary
  parity).

### Executor — the load-bearing part

`execute_window_value` (ADR 246) computes a per-row source `targets: Vec<Option<usize>>`. It gains an
`ignore_nulls` parameter and skips NULLs in `sorted_value` when navigating:

- **LAG / LEAD**: instead of `row ∓ offset` physical rows, step toward the partition edge counting
  only **non-NULL** rows, stopping at the `offset`-th; out of range → the default (`Lag.default` /
  NULL). E.g. `LAG(v, 1) IGNORE NULLS` over `[10, NULL, 20]` at row 3 = `10` (the NULL at row 2 is
  skipped), where RESPECT NULLS would give `NULL`.
- **FIRST_VALUE / NTH_VALUE**: scan **forward** from the frame slice start, returning the first /
  `n`-th non-NULL row. **LAST_VALUE**: scan **backward** from the slice end, returning the first
  non-NULL.

This replaces the 246 O(1) positional pick with an **O(frame)** sparse scan when `ignore_nulls` — the
necessary, bounded cost of null-skipping (the partition/frame already bounds it). Row clamping stays
partition-local; the target is `None` (→ NULL) when no non-NULL row qualifies. The value column is the
already-sorted `sorted_value`, so `sorted_value.is_null(i)` is the skip test. `RESPECT`
(`ignore_nulls == false`) keeps the O(1) physical path byte-for-byte.

### Wire / goldens

`BoundWindowCall` is in the execution plan, so a field append shifts its postcard preimage
(positional encoding) — *if* a pinned golden vector contains a Window node. ADR 242 (created
`BoundWindowCall`) and 246 (added the `Value` variant) passed `--lib` without a re-pin, suggesting
window plans aren't in the golden set; but a field append is more invasive than a variant add.
**Verify**: `--lib` is the signal — re-pin the affected window-plan vectors in this cut if it flags
them (`#[serde(default)]` does not stabilize postcard).

## Consequences

- Real analytic null-skipping (`LAG ... IGNORE NULLS`, `LAST_VALUE ... IGNORE NULLS`) on both
  frontends.
- No new node, no fragmenter change — `ignore_nulls` rides the existing value-call path; distributed
  execution is unchanged structurally (one new cross-producer gate documents it).
- Ranking / aggregate / NTILE windows are unaffected (they reject the modifier as before).

## Discriminators (each fails on `main` — IGNORE NULLS rejects today)

- **`lag_ignore_nulls_skips_null_rows`** — over a value column with interleaved NULLs,
  `LAG(v, 1) IGNORE NULLS` returns the nearest prior non-NULL, not the physical prior row.
- **`last_value_ignore_nulls_skips_trailing_nulls`** — `LAST_VALUE(v) IGNORE NULLS` over a frame
  ending in NULLs returns the last non-NULL, differing from `RESPECT NULLS` (which returns NULL).
- **`respect_nulls_is_default_unchanged`** — `RESPECT NULLS` and the no-modifier form produce
  byte-identical plans + results to ADR 246 (the default is a true no-op).
- **`ignore_nulls_rejected_on_non_value_functions`** — `RANK() IGNORE NULLS`, `SUM(x) IGNORE NULLS`,
  `NTILE(2) IGNORE NULLS` reject.
- **`rune_value_ignore_nulls_matches_sql`** — Rune `lag(col(v), 1).ignore_nulls()` matches the SQL
  `LAG(v, 1) IGNORE NULLS` result (frontend parity).
- **`ignore_nulls_distributed_gate_is_value_discriminating`** — an IGNORE-NULLS value function whose
  skipped NULL lives on a co-located scan-shared row (236/238 cross-producer cardinal).

## Architecture guards

- `adr252_window_call_carries_ignore_nulls` — `WindowFunctionCall` + `BoundWindowCall` carry
  `ignore_nulls`.
- `adr252_value_executor_skips_nulls_under_ignore` — `execute_window_value` consults `ignore_nulls`
  and skips `sorted_value.is_null(..)` rows in target computation.
- `adr252_ignore_nulls_scoped_to_value_functions` — the lowering accepts IGNORE/RESPECT only on value
  calls; ranking / aggregate / NTILE reject it.
- `adr252_rune_value_ignore_nulls_method` — the Rune `ignore_nulls` method is registered, value-only.
- `adr252_named_window_spec_merge_stays_deferred` — the inheritance/extension rejects (`sql_planner.rs`
  3496/3926) are untouched; this cut is null-skipping only, the spec-merge boundary stays for 253
  (folded from the council).

## Deferred

- **IGNORE NULLS on aggregates** (e.g. `ARRAY_AGG`) — niche, separate semantics; not value navigation.
- **EXCLUDE** frame clause — sqlparser 0.62 cannot represent it (`// TBD: EXCLUDE`); externally
  blocked.

## Forward arc

252 IGNORE/RESPECT NULLS (this) → 253 named-window spec-merge (inheritance + extension) → window
surface complete (EXCLUDE deferred on the sqlparser external blocker).
