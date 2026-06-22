# ADR 308 — ON-clause residual pushdown (single-input residuals)

## Status
Proposed.

## Context

ADR 307's q13 measured `blocked(unsupported_non_equi_join_on@plan)` — `lower_join_on` extracts equality conjuncts
only and rejects the `o_comment NOT LIKE '%special%requests%'` residual. q13's residual is **right-side-only**, so
for a `LEFT OUTER` join it is SQL-equivalent to pre-filtering the right input — no in-join residual machinery
needed. This ADR delivers **single-input ON-residual pushdown**; the general theta-residual (cross-side /
preserved-side / FULL OUTER) is deferred to a future ADR at a genuine mechanism boundary.

(Topic was framed as "non-equi outer-join ON predicate"; the accurate, complete-for-its-class name is ON-residual
**pushdown** — the theta-residual is a distinct mechanism, not part of this cut.)

## Decision — classify the ON residual; push the pushable cases; reject the rest

`lower_join_on` collects the non-equality ON conjuncts into a transient logical `on_filter` (instead of rejecting).
The binder classifies + rewrites in the catalog-aware logical-normalization pass beside
`recognize_cross_equi_joins` (`binder/outer_join_demotion.rs`), then normal binding produces the existing key-only
join:
- **`LeftOuter` + residual referencing only the right (null-supplying) input** → `LEFT OUTER JOIN (right WHERE
  residual) ON keys`. **q13's case.**
- **`Inner` + single-input residual** → push into that input (existing WHERE side-local pushdown).
- **`RightOuter` + left-(null-supplying)-only residual** → push into the left input (symmetric).
- **Cross-side residual, preserved-side-only residual, or FULL OUTER residual** → **reject precisely** (these
  require the theta-residual `Join.filter` + exec match — a deferred future ADR).
- **Residual with no equality key** → **reject precisely**. This cut supports equi-join keys plus pushable
  residuals, not pure theta joins.

**`ExecutionPlan::Join` stays key-only** — no exec match-filter, no broadcast-swap residual, **no fragmenter
residual**. After the logical rewrite, the distributed path sees an ordinary `Filter` below a key-only join, so it
works for free. This is why the cut is small.

### The load-bearing correctness — the push preserves null-extension
`LEFT OUTER JOIN orders ON (c=o AND right_pred(o))` ≡ `LEFT OUTER JOIN (orders WHERE right_pred) ON (c=o)`: the
matched set `{O : c=o ∧ right_pred(O)}` is identical, and a left row with zero qualifying right rows null-extends
in **both** (so `COUNT(o_orderkey)=0` is preserved). The textbook "predicate on the null-supplying side pushes to
that side" equivalence. Only push a residual referencing **solely** the null-supplying input; a preserved-side or
cross-side residual is NOT pushable.

### Fixture — add `orders.o_comment` in this cut (ADR 307 correction)
ADR 307 deferred `o_comment` (the wall was at lowering, pre-column-resolution). ADR 308 **clears that wall**, so
the binder now resolves `o_comment` — add it (Utf8, from `OrderGenerator`) to the **shared** orders fixture here,
else q13's next row is a false `UnknownColumn(o_comment)` gap, not the real engine wall. Verify no regression:
all existing orders-backed rows (q03/q04/q05/q10/q12/q22) + both matrices + any orders golden
(postcard-preimage caution).

## Scope / boundaries
- Single-input pushable ON residuals with at least one equality key (LeftOuter-right / Inner-side-local /
  RightOuter-left). Cross-side / preserved-side / FULL-OUTER / pure-theta residuals reject precisely. No
  `ExecutionPlan::Join` residual; no exec / swap / fragmenter change. Add `o_comment`.

## Acceptance
- A `LEFT OUTER` with a right-only ON residual rewrites + executes with **correct null-extension** (the
  residual-fail→null-extend test, `c_count=0` preserved); an `INNER` single-side residual pushes; a **cross-side /
  preserved-side residual rejects precisely** (the deferred theta boundary). Discriminating: rejects pre-cut at
  sql_planner:6110.
- **q13 advances past `unsupported_non_equi_join_on`** — verify-first re-measure (with `o_comment` present): the
  next wall (count-of-counts → ADR 309), or further. `pass@execute` not required.
- Full `-p toron --no-fail-fast` + `--all-targets` + `architecture_test` green; the `o_comment` add leaves
  the existing orders-backed rows (q03/q04/q05/q10/q12/q22) + the 15/15 boards unchanged.

## Risks
- **Null-extension equivalence** — the pushability classifier is the load-bearing logic; pin both the push (a
  residual-failing match still null-extends) and the reject (preserved-side/cross-side not pushed).
- **Shared `o_comment` add** — full suite + goldens are the net.
- **Don't over-push/over-reject** — Inner pushes any single-side residual; OUTER only the null-supplying side.
- **Don't accidentally admit pure theta joins** — this ADR still needs equality keys; no-key residual joins belong
  to the deferred full theta-residual mechanism.

## Forward arc
q13 re-measures (count-of-counts `GROUP BY <agg-alias>` → ADR 309), then the multi-wildcard `NOT LIKE` /
`COUNT`-non-null, then the q13 correctness oracle. A **future ADR = theta-residual** (`Join.filter` + exec match)
for cross-side / preserved-side / FULL OUTER residuals, **if** a query nominates it. q19 (disjunctive) remains a
machete.

## Why toron-home
A `crates/toron` cut across lowering + the catalog-aware binder normalizer — a join-semantics capability via
logical filter placement before the normal key-only join bind. No `ExecutionPlan::Join` / exec / fragmenter
change. q13's row advances toward its next wall in `toron-tpch`. No Wendelon / cross-ring surface.

## Council provenance
3/3. **Codex's grounding led**, Claude conceded from the iter-0 full-theta-residual: q13's right-side-only residual
**pushes into the right input** (SQL-equivalent for LEFT OUTER), so `ExecutionPlan::Join` stays key-only and the
distributed path works for free — collapsing "the biggest cut in the arc" into a binder pushdown. Folded:
single-input pushdown (push pushable, reject theta); **`o_comment` added in this cut** (Codex's correction, else a
false fixture gap); the null-extension equivalence pinned; the theta-residual deferred at a genuine mechanism
boundary. Renamed to the accurate, complete-for-its-class name.
