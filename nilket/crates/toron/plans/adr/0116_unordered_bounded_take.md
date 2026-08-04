# ADR 116: Unordered Bounded Take (cheap bare `LIMIT`)

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini).** **Toron relational operator + execution
+ a narrow cross-ring table-query-meta widening (Wendelon-honored `row_limit`).** **Predecessors:** 010/015
(deterministic-ordering doctrine this deliberately opts out of), 059/060 (downstream cancellation), 115 (ordered
top-k — the *other* `LIMIT` branch; left the `unordered-LIMIT-is-future-Take` guard to cash in). **Migration:**
`plans/impl/0116_unordered_bounded_take.md`.

## Doctrine — a distinct operator: cheapest rows, not a weaker sort

`SELECT * FROM huge_table LIMIT 3` with **no `ORDER BY`** = "return any 3 qualifying rows; choose the cheapest
correct path." On a 100-billion-row table the fast path is one bounded Wendelon request; if that source produces
`n` rows it answers directly and stops — no global gather, no full scan, no proof about the rest. If it underfills,
the cardinality rule below still applies. This is **Toron's reason for existence** (smart-storage pushdown) and a
**deliberate opt-out of the 010/015 deterministic-ordering doctrine**: no order, stability, fairness, repeatability,
randomness, or row-identity guarantee.

It is **not** ordered top-k with no keys — it must **not** route through `Sort`/the ordered-barrier path (115
owns `ORDER BY … LIMIT`). But it keeps the one contract bare `LIMIT` carries:

> **Cardinality:** `rows_returned = min(limit, relation_cardinality)`. Once `n` rows are produced, no proof about
> the rest is needed; before `n`, the engine must prove exhaustion or keep searching. Returning fewer than `n`
> while qualifying rows exist elsewhere is a *different* operator (a future Rune `take_available(n)`), not SQL
> `LIMIT n`.

## Why Wendelon must honor the bound in V1 (grounded)

`WendelonStorageScan::scan` (`storage.rs:853`) awaits `table_query_with_runtime_filter[_and_attempt_hook](...)`,
which **`collect_all()`s** the `StreamingQueryResult`, then wraps the already-materialized batches in
`ScanStream::from_batches(...)`; Wendelon `handle_table_query` plans, concatenates `plan.batches`, and sends one
`TABLE_QUERY_RESP`. So by the time Toron holds a `ScanStream`, **Wendelon has already done the full scan** —
executor `Take` only trims already-materialized batches, and cancelling the stream saves nothing on today's path.
The cheap goal therefore **requires Wendelon to receive and honor the bound**; executor early-stop is the
correctness *floor* beneath it. (Claude's iter-1 "Fork A cancellation stops the scan" was wrong on this grounding;
resolved to B.)

## Three enforcement layers

- **(A) Semantic operator** — `LogicalPlan::Take { input, limit }` + `ExecutionPlan::Take { input, limit,
  output_schema }`: schema-preserving, **not** `Sort`, not a global sort barrier, not sampling, not deterministic
  row-selection.
- **(B) Executor early-stop (correctness floor)** — stop at `n`, slice the final batch, `LIMIT 0` avoids child
  execution, drop/cancel upstream once satisfied. Coordinator-local (the ScanStream-collection guard). Correct
  for every backend (in-memory, residual-filter shapes, non-scan inputs, future true-streaming) — but on today's
  collect-all Wendelon path it only trims, so it is **not** the cheap mechanism.
- **(C) Storage pushdown (cheap path)** — `ScanRequest.row_limit`, honored by Wendelon (post-PBC early-return).

## Decisions (promotion-locked)

**D1 — `Take` operator.** `LogicalPlan::Take` + `ExecutionPlan::Take`, schema-preserving, distinct from `Sort`,
not counted as a global sort barrier (115's `global_sort_barrier_count`). Arena/walker sweep for the new IR variant.

**D2 — SQL surface.** Bare `LIMIT n` (no `ORDER BY`) lowers to `Take` (lift the `LIMIT`-without-`ORDER BY` reject,
`sql_planner.rs:2481`, for that case only). `ORDER BY … LIMIT` stays 115 `Sort`. Unordered `OFFSET` (and `LIMIT n
OFFSET m` without `ORDER BY`) stays **rejected**. `LIMIT 0` in scope.

**D3 — Cardinality contract** `min(limit, cardinality)` — not best-effort; continue to more sources if one yields
`< n` and exhaustion is unproven.

**D4 — Executor early-stop (B, correctness floor):** bounded materialization, slice final batch, `LIMIT 0` fast
path (schema + zero rows, no child exec), cancel/drop upstream after `n`. Coordinator-local.

**D5 — Storage pushdown (C), pushed only when safe.** "Safe" = `row_limit` bounds rows *after* all
storage-visible filtering/projection (post-PBC = post-storage): `Take(Scan)`, `Take(Project(Scan))`,
`Take(Filter(fully-pushed-PBC, Scan))` → set `Scan.row_limit`. `Take(Filter(residual, Scan))` → **unsafe** (the
residual isn't storage-visible; a storage bound would count pre-residual rows the filter discards → under-count)
→ leave `row_limit = None`, executor `Take` above the filter. **Row-identity append scans** do **not** push
`row_limit` in V1 unless explicitly implemented/tested (executor `Take` still slices appended rows).

**D6 — Cross-ring chain (B in V1, narrow).** `ScanRequest.row_limit: Option<usize>` →
`wendelon_client::TableQueryOptions.row_limit` → an **explicit** field in the fixed `TableQueryRequestMeta` (`0` =
absent; **not** in `_pad`/`_trace_pad` — `_pad[0]` already carries the stats-pruning flag and the fixed meta is
guard-pinned, so widen it intentionally + add a guard) → server decode/validate → Wendelon planner post-PBC
early-return. **Narrow:** no new message type, sidecar frame, compat path, new result protocol, ordered-top-k /
aggregate-pushdown change, or true Wendelon→Toron streaming — a fixed-meta widening + a planner early return.

**D7 — Wendelon `row_limit` is a success bound, not a budget.** Apply PBC predicate/projection first, runtime
filter before the bound, count **post-PBC output rows**, slice the final overlarge batch, set
`QueryResponseMeta.rows` to the bounded count, stop scanning/planning once satisfied, return **`OK`** (not
`QUERY_BUDGET_EXCEEDED`). Do **not** implement by lowering `QueryExecutionBudget.max_rows` — budgets apply
independently (a limit above budget may still be budget-rejected; the bound itself is not an error).
**Secondary-index** table-query plans must honor the same post-PBC bound, or bypass the index path to the bounded
full-scan as a V1 fallback (must not ignore the bound).

**D8 — `LIMIT 0` / wire-`0` trap.** Because `0` means *absent* on the wire, `LIMIT 0` is short-circuited **before
issuing a Wendelon request** (return the schema + zero rows). If `ScanRequest.row_limit = Some(0)` reaches a local
backend it defensively returns zero rows; it is **never** encoded as a Wendelon row-limit (`0`).

**D9 — Rune `.take(n)` (honest naming).** `RuneDataFrame::take(n)` for unordered bounded take; Rune `.limit(n)`
stays the **ordered** top-k spelling attached to `.sort` (115) — do **not** overload. No unordered Rune `.offset`.

**D10 — Fragmenter.** Best: **progressive single-source-first** — one bounded scan request (`row_limit = n`), no
global fanout/gather/sort/exchange when one source can satisfy `n`. Acceptable V1 fallback: existing distributed
fanout + per-source row limits + a coordinator `Take`. **Not acceptable:** returning `< n` while qualifying rows
exist elsewhere (D3). `Take` over an aggregate/sort/join/residual **barrier** applies after the barrier unless a
later optimizer proves a safe rewrite.

## Behavior gates

**Toron:** `bare_limit_returns_n_rows_subset` (assert `count==n` + `rows ⊆ t`, **not** identity),
`bare_limit_over_small_relation_returns_all`, `bare_limit_zero_returns_empty_with_schema`,
`where_then_bare_limit_bounds_matches`, `residual_filter_limit_not_pushed_to_storage`,
`fully_pushed_filter_limit_can_push_to_storage`, `ordered_limit_still_deterministic` (115 regression),
`rune_take_executes`, `rune_limit_without_sort_still_rejects`, `unordered_offset_rejects`.
**Wendelon:** `table_query_row_limit_bounds_post_pbc_rows`, `table_query_row_limit_slices_final_batch`,
`table_query_row_limit_is_not_budget_exceeded`, `table_query_row_limit_zero_absent_on_wire`,
`table_query_options_default_row_limit_absent`, `table_query_row_limit_not_hidden_in_pad`,
`secondary_index_plan_honors_or_falls_back_on_row_limit`.
**Distributed:** `bare_limit_can_use_single_bounded_scan_when_source_satisfies_limit`,
`bare_limit_does_not_return_too_few_when_first_source_underfills` (may be a unit/architecture pin if full
progressive scheduling exceeds the cut — but the semantic prohibition must be pinned somewhere).

## Architecture guards

`adr116_bare_limit_lowers_to_take_not_sort` (cash in 115's future-Take guard), `adr116_take_is_distinct_ir_operator`,
`adr116_take_is_schema_preserving`, `adr116_take_is_not_global_sort_barrier`, `adr116_ordered_limit_still_uses_sort`,
`adr116_scan_request_has_row_limit`, `adr116_row_limit_not_pushed_below_residual_filter`,
`adr116_row_identity_append_does_not_push_row_limit_without_guard`, `adr116_table_query_options_has_row_limit`,
`adr116_table_query_meta_has_explicit_row_limit_not_pad_payload`, `adr116_wendelon_row_limit_is_success_bound_not_budget`,
`adr116_rune_take_is_unordered_surface`, `adr116_rune_limit_without_sort_still_rejects`,
`adr116_unordered_offset_rejected`, `adr116_bounded_take_early_stop_is_coordinator_local`.

## Council history

Topic operator-locked. **Genuine 3-of-3:** Claude seed + Codex iter-0 full design (the `Take` operator, three
layers, Rune `.take` honest naming, `min(limit,cardinality)` contract, the residual-filter pushdown-safety rule);
Gemini's iter-1 was a **stale 114 mis-file**, re-drafted at iter-2 onto the converged shape. **Cross-ring resolved
→ B (Wendelon honors `row_limit`)**, operator-steered, correcting a Claude grounding miss: I'd claimed Fork A's
stream-cancel stops Wendelon mid-scan, but `scan` `collect_all()`s before `from_batches`, so A only trims dead
batches. **Codex iter-2 sharpened the V1 edges** (now locked): the `LIMIT 0`/wire-`0` short-circuit (D8), the
success-bound-not-budget rule (D7), secondary-index honoring/fallback, the row-identity-append carve-out (D5), and
the fragmenter fallback ladder (D10).

## What to do next

Multiple ordered top-k barriers (115's recursive-fragmenter follow-on); **true Wendelon→Toron streaming scans**
(would make executor cancellation bound remote cost too, and relax the collect-all barrier); a future Rune
`take_available(n)` (best-effort sub-cardinality — explicitly *not* this); unordered `OFFSET` only if a real need
appears; logical-type lifecycle.

## Promotion criteria

Bare SQL `LIMIT n` and Rune `.take(n)` lower to a distinct `Take` (not `Sort`), returning `min(n, cardinality)`
rows with no order/identity guarantee; unordered `OFFSET` rejected; ordered `ORDER BY … LIMIT` (115) unaffected;
executor `Take` enforces the bound (coordinator-local), `LIMIT 0` free; `ScanRequest.row_limit` set only on safe
post-PBC shapes (never below a residual filter or row-identity-append); **Wendelon receives an explicit
table-query `row_limit` (fixed-meta field, not `_pad`) and stops after `n` post-PBC rows, returning `OK` not
budget-exceedance**; secondary-index plans honor or fall back; distributed execution preserves `min(limit,
cardinality)`; the `adr116_*` guards + the cashed-115 guard hold; Toron + Wendelon + distributed gates green.
Migration: `plans/impl/0116_unordered_bounded_take.md`.
