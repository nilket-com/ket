# ADR 324 — q11 correctness oracle

## Status
Proposed.

## Context

First correctness cut after full reach (22/22). q11 is the query ADR 321 flagged: its green reach cell is **not**
a correctness signal. The HAVING threshold `… > (SELECT SUM(ps_supplycost * ps_availqty) * 0.0001 …)` hits a
literal-scale bug in strict mode, so a faithful oracle **exposes** it. This ADR is the **oracle** — it does not fix
the bug (that would smuggle a literal-representation change in under a coercion name); it proves the bug, names it,
and hands the fix to a follow-on.

## The bug (grounded)
A numeric literal lowers through `parse::<f64>()` → `ScalarValue::Float64`, so `0.0001` is an f64 before any
coercion — the text scale is gone at parse. In **strict** mode `coerce_literal_value` → `decimal_literal_from_f64`
(binder.rs:5748) then rounds it to the operand's scale: `round(0.0001 * 10^2) = 0` → **`0.00`**. The threshold
collapses to `SUM * 0.00 = 0`, so HAVING becomes `value > 0` and q11 returns every positive German part group.
(Dev keeps Float64 `0.0001`, so dev is correct — but the canonical rail is strict-only; see below.)

## Decision — land the q11 oracle, honest, with the strict red expected and asserted

### D1 — the oracle (independent generator traversal, correct TPC-H semantics)
- Traverse `partsupp ⋈ supplier ⋈ nation` filtered to `n_name = 'GERMANY'`
  (`ps_suppkey = s_suppkey`, `s_nationkey = n_nationkey`).
- Per `ps_partkey`: `value = SUM(ps_supplycost * ps_availqty)`, mirroring the engine's **legitimate** Decimal
  precision (the per-product `Decimal128(26,2)` + sum precision-fit) — so the *only* intended mismatch is the
  literal, not rounding.
- Global `threshold = SUM(ps_supplycost * ps_availqty) * 0.0001` — **the real 0.0001**, not the engine's `0.00`.
- Emit parts with `value > threshold`, `ORDER BY value DESC`. Assert schema first; check nullness on every Arrow
  slot.

### D2 — the contract (the expected outcome on the current engine)
- **Canonical correctness row is strict-only.** q11 strict = **`fail@compare`** against the current engine, with a
  **precise compare reason `decimal_literal_scale`** — the row names exactly why. This is not a reachability
  blocker and must not be added to the recognized-blocker allowlist.
- **The suite stays green because the strict red is *expected and asserted*, not ignored.** The test asserts "q11
  strict mismatches the oracle, reason `decimal_literal_scale`" — it passes by *confirming the expected red*. (No
  `#[ignore]`; an ignored test would hide the bug instead of pinning it.)
- **Dev is a diagnostic/test control, not a matrix cell.** The oracle may confirm dev matches (Float64 `0.0001` is
  correct) as a control that isolates the bug to the strict literal path, but the canonical correctness row stays
  strict and honestly red. Do **not** claim "q11 dev = pass@compare" in the matrix (the rail is strict-only).

### D3 — non-vacuity (mandatory)
The oracle must prove the threshold is **selective at sf=0.01** — assert it emits a **proper subset** of the German
parts (more than zero, fewer than all). Otherwise it fake-passes like the earlier empty-result traps: if the
threshold filtered nothing (or everything), the oracle would agree with the buggy `value > 0` engine for the wrong
reason. The selectivity assertion is what makes the strict red *meaningful*.

## Scope / boundaries
- **In scope:** the q11 oracle; the strict `fail@compare` + `decimal_literal_scale` reason (asserted); the
  selectivity assertion; dev as a control.
- **Not in scope (follow-on):** the fix. **ADR 325 — exact numeric/decimal literal preservation through
  lowering/binding** (carry a decimal literal's scale from parse as a scale-bearing value, so `0.0001` survives as
  `Decimal128(…,4)` and strict q11 flips to `pass@compare`). Do not smuggle a literal-representation change into
  this oracle ADR under a coercion name.
- **Not in scope:** adding `decimal_literal_scale` to `RECOGNIZED_BLOCKERS` or `reason_for_error`. The q11 row is
  a correctness `fail@compare`, not a blocked reachability/query-surface row.

## Acceptance
- The q11 oracle lands; **q11 strict = `fail@compare` with reason `decimal_literal_scale`**. In
  `correctness-matrix` this renders as `q11 strict=fail(decimal_literal_scale@compare)`. Tests assert that expected
  outcome, so the suite stays **green** (the red is pinned, not hidden).
- **Non-vacuity:** the oracle asserts the threshold selects a proper subset at sf=0.01 (not empty, not all).
- The oracle mirrors the engine's legitimate Decimal precision (no false-fail on rounding); schema + per-slot
  nullness checked.
- Full `-p toron --no-fail-fast` + `-p toron-tpch` green; reach unchanged (22/22); other correctness cells
  unchanged.

## Risks
- **Don't smuggle the fix** — this is the oracle; the literal-representation fix is ADR 325.
- **Fake-pass via vacuity** — the selectivity assertion is mandatory (the empty/all-result trap).
- **Mirror legitimate precision only** — the product/sum Decimal precision must match the engine so the sole
  intended mismatch is the literal scale, not rounding.
- **Strict-only rail** — keep the canonical row strict; dev is a control, not a cell.

## Forward arc
ADR 325 (exact numeric/decimal literal preservation) flips q11 strict `fail@compare → pass@compare`. Then the rest
of the correctness backlog: q15/q18/q19/q21 oracles (the newly-reached queries).

## Why toron-home
The oracle + the expected-red assertion live in `toron-tpch`; the precise reason `decimal_literal_scale` is emitted
by the q11 correctness comparator, not by reachability error classification. No engine change here (the fix is ADR
325). No Wendelon / cross-ring surface.

## Council provenance
Operator + Codex + Claude. Codex grounded — and Claude verified (`decimal_literal_from_f64` takes `Option<f64>`;
numeric literals lower via `parse::<f64>()` → `Float64`, so the scale is lost at parse) — that a binder-coercion
"natural scale" fix is not implementable; Claude conceded Option A. The useful correction: **don't smuggle a
literal-representation fix into an oracle ADR**. Operator's contract folded: strict `fail@compare` with reason
`decimal_literal_scale`; the suite green by *asserting* the expected red (not ignoring it); non-vacuous selectivity
at sf=0.01; the rail is **strict-only** (dev is a control, not a matrix cell); the fix is follow-on ADR 325.
