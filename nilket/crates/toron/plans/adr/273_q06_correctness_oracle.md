# ADR 273 — strict q06 correctness oracle (the first answer-checked cell)

## Status
Proposed.

## Context
The reachability gauge now has four `pass@execute` cells (q01/q03/q06/q14, both modes). But `pass@execute`
proves a query *shape reaches the executor* — **not that a result cell is correct**. The operator's steer:
the board is credible enough to deserve a second axis — at least one *answer-checked* cell — before expanding
reachability further. This is a **local correctness rail**, not a benchmark certification: same generated
fixture, same strict query, an independent Rust recomputation, and a comparison step.

**q06 is the right first oracle** because it is the only already-green strict query that is **exactly
`i128`-comparable**: `SUM(l_extendedprice * l_discount)` with both operands `Decimal128(15,2)` types under ADR
263 to `Decimal128(31,4)` (the raw i128 product, no rescale), and `SUM` carries that scale-4 accumulator. So
the expected answer is exactly `Σ (l_extendedprice_raw × l_discount_raw)` — **no division, no ordering, no
rounding tolerance**. q14's `÷` (rounding) and q01/q03's grouped/ordered rows each add a comparison-policy
question; they are good later oracles, q06 is the first with none.

## Decision
Add a strict-only q06 correctness oracle as a **separate rail** from reachability (operator-steered, with the
reachability-only q10 menu deferred to the next reach ADR).

1. **Separate the two axes.** Correctness rows go to `target/tpch/correctness.jsonl` and render through a
   distinct `correctness-matrix`; the human line is `tpch_correctness` (not `tpch_reachability`); a new
   `phase=compare` follows execute. **`reachability.jsonl` and its matrix are untouched** — reachability
   answers "did it run," correctness answers "did the result match an independent oracle." `q06 strict` stays
   `pass@execute` on the reachability matrix.
2. **Strict only.** The first correctness cell is `q06 strict`; dev mode is a `Float64` proxy (reachability
   only, not exact-comparable).
3. **Independent oracle.** Compute the expected value by traversing `tpchgen::generators::LineItemGenerator`
   **directly** — not Toron's loaded `RecordBatch`, not the executor output, not official answer files (that
   would share too much path, or pull in certification scope). The oracle sums
   `l_extendedprice.into_inner() as i128 * l_discount.into_inner() as i128` over rows matching the **same**
   strict-q06 filters, with the raw thresholds pinned (verified against `tpchgen`'s scale-2 `TPCHDecimal`):
   - `l_discount`: raw `into_inner()` in `5..=7` (i.e. `0.05..=0.07`; **not** `500..=700`).
   - `l_quantity`: `(l_quantity as i128 * 100) < 2400` (i.e. `< 24`; the whole-number quantity is stored ×100).
   - `l_shipdate`: `8766 <= to_unix_epoch() < 9131` (i.e. `[1994-01-01, 1995-01-01)` as `Date32` day counts).
4. **Exact raw comparison.** Compare the executor output's raw `Decimal128` value, its type (`Decimal128(31,4)`),
   and its nullness — **not** the canonical digest and **not** a formatted string (so a future Decimal-widening
   change can't silently pass). Equality is byte-exact (q06 has no rounding).
5. **Honest comparison telemetry.** A mismatch is `fail@compare` — there is **no "wrong answer" recognized
   blocker**. A plan/bind/execute failure stays a normal reachability-style error at its deepest phase.
6. **Non-vacuous oracle.** At the default correctness scale (`--sf 0.01`) the oracle must select **≥ 1 row**;
   a zero-row oracle could turn a threshold bug into a false fail, or worse a vacuous `0 == 0` pass. The cell
   proves both the answer *and* that the oracle actually selected rows.

## Watch-items (operator) → guards
- **The oracle's own correctness is the risk.** The discount threshold is `5..=7`, not `500..=700` — an
  iter-0 draft had the 100× error, which would have made the first cell a false `fail@compare`. Pin the raw
  thresholds and assert a non-vacuous matched-row count (a regression against the bug re-creeping).
- **Independence.** The oracle traverses the generator, not Toron's Arrow batch or executor output — else it
  would share Toron's path and prove nothing.
- **Separation.** Correctness must not write or mutate `reachability.jsonl`; the reachability matrix is
  unchanged.
- **Scale drift.** Pin the raw value and `Decimal128(31,4)` type; never compare via formatted strings.
- **No certification overclaim.** This is a local strict-query oracle against an independent recomputation, not
  an official TPC-H result.

## Acceptance
- `correctness --query q06 --sf 0.01` emits `tpch_correctness query=q06 … phase=compare status=pass`, and
  `correctness-matrix` renders `q06 strict=pass@compare` with q01/q03/q14 `unchecked`.
- `matrix` is unchanged: q01/q03/q06/q14 still `pass@execute`.
- Tests prove: Toron's q06 output equals the `LineItemGenerator`-traversal oracle; the oracle's matched-row
  count is non-zero at the default correctness scale; the discount bound is raw `5..=7`; the output type is
  `Decimal128(31,4)` compared by raw i128; an injected mismatch is `fail@compare` (not `blocked`); a
  correctness JSONL row does not affect `matrix_from_log`.
- ADR 261 CLI hygiene preserved: malformed / non-positive `--sf` is exit 2 with no row.

## Non-goals
No TPC-H certification language; no DBGEN/QGEN vendoring or answer-set files; no q01/q03/q14 oracles; no dev
correctness cell; no distributed correctness; no change to query semantics or Decimal kernels unless the oracle
catches a real mismatch (then widen to the full ADR 264 seal gate).

## Forward arc
q10 returns as the next *reachability* ADR. Correctness follow-ons proceed simplest-first: q01 (grouped output)
→ q03 (multi-table grouped) → q14 (once ratio/rounding comparison policy is explicit). Cadence: a couple of
reach cells, then another correctness cell — keep both axes honest.

## Why toron-home
The `toron-tpch` gauge and Toron's strict Decimal execution result. No cross-ring surface.
