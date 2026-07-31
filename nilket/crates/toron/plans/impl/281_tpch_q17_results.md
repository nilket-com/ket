# ADR 281 — Results: tpch.q17 reachability (the correlated-scalar unqualified-outer-ref wall)

## Measured row
```
q17 dev=fail(unrecognized@bind)  strict=fail(unrecognized@bind)
reachability matrix otherwise unchanged: q01/q03/q04/q05/q06/q10/q14 dev+strict pass@execute
correctness matrix unchanged: q01/q03/q06 strict pass@compare
```
An **honest red row** — exactly the wall the gauge was built to surface.

## The measured wall (raw, captured by a temporary bind-error probe; probe removed)
```
UnknownColumn { column: "p_partkey" }   @ bind   (identical in dev and strict)
```
This **resolves the operator's fork to option (1)**: the binder resolves the scalar subquery's **unqualified
outer reference** `p_partkey` (in `... WHERE l_partkey = p_partkey`) against the *inner* `lineitem` scope,
doesn't find it there, and errors `UnknownColumn`. The correlation is never recognized — so the wrapped-
aggregate boundary (fork option 2, `0.2 * AVG(...)`) sits *behind* this wall and is not yet reached. This is the
scalar-subquery analogue of q04's unqualified correlated-EXISTS wall (ADR 279), one phase later (bind, not
plan): EXISTS lowering at least produced a node; the scalar subquery binds its inner relation and the outer ref
falls through as an unknown column.

## Why `fail`, not `blocked` (the honesty call)
The engine's error is a **generic** `UnknownColumn` — it carries no marker that says "unsupported correlated
scalar." Allowlisting `UnknownColumn` as a recognized blocker would mis-classify a real column-resolution bug
(or a typo) in any other query as an expected wall, masking regressions. So per the gauge's honesty rule
(a recognized reason is `blocked`; an unrecognized one is `fail`), q17 is an honest `fail(unrecognized@bind)`,
not a manufactured `blocked`. `reason_for_error` is left unchanged — no over-broad classifier added. The wall is
documented here and pinned by a test; it is intentional, not a regression.

## No strict literal split needed
Both modes wall identically at the unqualified outer ref — the bind fails before the `7.0` / `0.2` literal
typing (the dev/strict axis) is ever exercised. So `STRICT_Q17_SQL` stays identical to `Q17_DEV_SQL`; no harness
deviation was introduced. (If a future cut resolves the outer ref, the literal typing may surface as the next
strict wall — measured then, not now.)

## What was built (`crates/toron-tpch`, harness-only — no engine change)
- `part` fixture: appended `p_brand` + `p_container` (Utf8) for q17's `= 'Brand#23'` / `= 'MED BOX'` filters
  (append-only; q14's `p_partkey`/`p_type` positions unchanged). `lineitem` already exposed
  `l_quantity`/`l_extendedprice`/`l_partkey` — unchanged.
- `Q17_DEV_SQL`/`STRICT_Q17_SQL` (tpchgen-exact, unqualified `l_partkey = p_partkey` kept — not pre-qualified,
  not pre-decorrelated), `run_dev_q17`/`run_strict_q17`, CLI `run --query q17`, `status_rows` → q17
  `implemented`.

## Tests
- `q17_fails_at_bind_on_unqualified_correlated_scalar` — pins q17 `fail`/`Bind`/`unrecognized` in both modes +
  q17 `implemented`. (Bare `#[tokio::test]`; walls in <2 ms at bind.)
- The seven reach cells + q06/q01/q03 correctness tests stay green (the `part` appends are transparent).

## Gate (harness-only)
fmt --check · `check -p toron-tpch --all-targets` · `test -p toron-tpch` (harness binary 19) · `run --query q17
--mode dev|strict` → measured `fail@bind` · `matrix` shows q17 `fail(unrecognized@bind)` + the seven reach cells
unchanged · `correctness-matrix` unchanged · `git diff --check` clean. No `-p toron` change.

## Forward arc
The measured wall names the next **engine** cut: **correlated-scalar-subquery outer-ref resolution** — resolve
an unqualified outer reference inside a scalar subquery into the correlation (the scalar analogue of ADR 279's
EXISTS resolution), which carries the full `-p toron` gate. Behind it, the wrapped-aggregate projection
(`0.2 * AVG(...)`) beyond ADR 260's bare-aggregate case is the *next* wall that cut will likely measure. q05
correctness stays available on the rail, independent of this.
