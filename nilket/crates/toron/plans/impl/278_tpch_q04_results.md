# ADR 278 — Results: tpch.q04 (measured row — first correlated-subquery wall)

## Measured row
```
q04 dev=blocked(unsupported_correlated_exists@plan)  strict=blocked(unsupported_correlated_exists@plan)
Reachability matrix (unchanged): q01/q03/q05/q06/q10/q14 dev+strict pass@execute
Correctness (unchanged): q06 + q01 strict=pass@compare
```

q04 — the gauge's first correlated-subquery query — produces its first **honest red row**: a recognized,
specific blocker at plan, naming the next ADR. (An honest red beats a lying green; this is the gauge working.)

## The measured first wall is the unqualified correlated EXISTS (the predicted wall, confirmed)
Of q04's two new classes, the **correlated EXISTS** walls first, at **plan**. The raw frontend error:
```
unsupported frontend construct: unsupported EXISTS column reference
```
q04's `EXISTS (… l_orderkey = o_orderkey …)` uses an **unqualified** correlated reference. Toron's
correlated-EXISTS decorrelation classifies the correlated pair **by column qualifier**
(`classify_exists_conjunct` over the outer/inner qualifier sets, every decorrelation test qualified), so it
cannot resolve which side `l_orderkey`/`o_orderkey` belong to and rejects the reference at plan. Classified
precisely as `unsupported_correlated_exists` (a new recognized blocker, placed before the generic
`unsupported_query_surface` bucket), it names the **decorrelation-generality** ADR: resolve unqualified
correlated refs in EXISTS by scope (the distinct `l_`/`o_` names are unambiguous against the inner/outer
relations).

The **date-interval** class is *not* q04's wall: the EXISTS is hit first, and `DATE '1993-07-01' + INTERVAL
'3' MONTH` is literal + literal (constant-foldable at bind), so it does not exercise the runtime
`column ± interval` kernel anyway. The official interval was kept (operator-steered) — measuring the real
shape, not a literalized one — and the gauge honestly reports the first wall the binder hits.

## What was built (harness only)
- **Fixtures (append-only, no new table):** `o_orderpriority` (Utf8) on orders; `l_commitdate` +
  `l_receiptdate` (`Date32`) on both lineitem fixtures. Existing queries reference none of them, so the six
  reach cells + the q06/q01 correctness cells are unchanged (verified: harness 18, correctness 11 green).
- **SQL** (`Q4_DEV_SQL` / `STRICT_Q4_SQL`): tpchgen-exact Q4, `:1` → `'1993-07-01'`, **official `+ INTERVAL '3'
  MONTH`**, correlated EXISTS, no LIMIT.
- **Wiring:** `run_dev_q04` / `run_strict_q04`; `q04_tables` = `lineitem` + `orders` (q04's FROM is orders, the
  EXISTS subquery is over lineitem); CLI `run --query q04`; `status_rows` → q04 `implemented`.
- **Telemetry:** `unsupported_correlated_exists` recognized blocker + classifier matching the EXISTS
  column-reference message (ADR 262 precise-match) — the only telemetry change; no engine feature.

## Tests
- `harness.rs::q04_blocks_at_plan_on_unqualified_correlated_exists` — dev + strict
  `blocked(unsupported_correlated_exists@plan)`, q04 `implemented`. A bare `#[tokio::test]` (q04 fails at plan
  in <1 ms — no deep execution future, no large-stack wrapper needed).
- `harness.rs::correlated_exists_error_classifies_precisely` — the EXISTS column-reference message →
  `unsupported_correlated_exists`, not the generic bucket.
- q01/q03/q05/q06/q10/q14 reachability + the q06/q01 correctness tests stay green (the appends are transparent).

## Gate (harness-only)
fmt clean · `check -p toron-tpch --all-targets` clean · `test -p toron-tpch` green (harness 18, correctness 11,
probes) · live `matrix` shows q04 `blocked(unsupported_correlated_exists@plan)`, `correctness-matrix` still
q01 + q06 `pass@compare` · `git diff --check` clean. No `-p toron` engine change.

## Forward arc
q04's measured wall names the **decorrelation-generality** ADR (the engine cut: resolve unqualified correlated
refs in EXISTS by scope). After that, q04 re-measures — likely advancing to bind/execute (or surfacing the next
correlated wall), and the runtime date-interval kernel (a `column ± interval` query) remains a separate
deferred class, as do the harder correlated forms (q17/q20 scalar/`IN`, q22). A q03/q05 correctness oracle
stays available on the correctness rail.
