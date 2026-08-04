# ADR 269 — Results: tpch.q14 (first measured row)

## Measured row

```
q14 dev=pass@execute  strict=blocked(unsupported_decimal_case@bind)
q01 dev=pass@execute  strict=pass@execute   (unchanged)
q06 dev=pass@execute  strict=pass@execute   (unchanged)
```

q14 — the first multi-table query — is now in the gauge. dev q14 (Float64) reaches execute end-to-end;
strict q14 walls at **bind** on the CASE Decimal-vs-integer gap. No engine feature was added; this is a
reachability attempt and the measured row names ADR 270.

## The measurement vindicated the no-facts caveat

The iter-1 probe (mock fixture, **no optimizer facts**) measured q14's first wall as the comma-join
**CROSS JOIN** — `FROM a, b WHERE a.k = b.k` lowers to a keyless cartesian the fragmenter rejects. The
operator's review caught that row counts are **not** a `TableSchemaDescriptor` field; they are catalog-side
optimizer facts. Seeding honest facts changed the measured wall:

- `run_query_with_tables` seeds each table's `TableOptimizerFacts` (real `row_upper_bound` /
  `byte_upper_bound` from the fixture batch) via `MockCatalog::seed_optimizer_facts` after registering the
  descriptor.
- With honest facts the tiny `part` is **broadcast-eligible**, so the comma equi-join is *admitted* (a
  broadcast cross-join + residual equi-filter — semantically the inner join), and q14 no longer walls there.
- So the cross-join was a **fixture artifact**, not q14's real wall. q14's genuine first wall is the next
  one down: `CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice*(1-l_discount) ELSE 0 END` — the Decimal
  `THEN` branch won't unify with the integer `ELSE 0` (`"CASE result branches cannot be unified:
  Decimal128(32,4) and Int32"`).

Both walls are pinned, side by side, in `q14_probe.rs` — `q14_without_optimizer_facts_walls_at_cross_join`
(the no-facts artifact) and `q14_with_seeded_facts_walls_at_decimal_case` (the real, seeded-facts wall) —
so the "facts matter" reasoning is a regression, not a comment.

## What was built (harness only)

- **Multi-table harness** (`lib.rs`): `HarnessTable { id, name, descriptor, batch, facts }` +
  `run_query_with_tables` (registers every descriptor, seeds its optimizer facts, assembles `HarnessScan`,
  then plan→bind→execute→digest). `run_query` is now a one-table wrapper (default facts) — q01/q06 unchanged.
- **q14 SQL** (`Q14_DEV_SQL` / `STRICT_Q14_SQL`): the official comma-join shape with literal date bounds,
  same text both modes. Not rewritten to explicit `JOIN`, CASE, or `/` — the failure is the evidence.
- **Fixtures**: `l_partkey` **appended** to the dev/strict `lineitem` schemas + generators (so q01/q06
  column positions and the q06 digest don't move); new `part` `(p_partkey, p_type)` generated from
  `tpchgen::PartGenerator`. `facts_for_batch` derives honest row/byte upper bounds.
- **Wiring**: `run_dev_q14` / `run_strict_q14`, CLI `run --query q14 --mode dev|strict` (scale-capped via
  `resolve_q14_scale` — see the caveat below), `status_rows` → q14 `implemented` (static capability only;
  the measured row lives in the JSONL/matrix).
- **Telemetry**: new `unsupported_decimal_case` recognized blocker, classified *before* the generic
  `unsupported_decimal_arrow` bucket and kept specific to `"case result branches cannot be unified"` (ADR
  262 precise-match discipline). Without it the wall mis-classified as the generic Decimal-carrier reason —
  a lying row the operator's "classify fairly" principle forbids.

## Honest caveat: dev q14 passes via a broadcast cartesian — and is scale-capped
dev q14's comma-join is admitted as a broadcast cross-join + equi-filter (the equality is not lifted into a
join *key*). At the gauge's tiny scale that is a correct, fast reachability pass; at production scale the
cartesian would be a scalability problem. Lifting `Filter(a.k=b.k)` over `CrossJoin` into a hash-join key is
a real follow-on (perf, and the canonical comma-join → inner-join recognition), but it is **not** a
reachability gate here — dev q14 already reaches execute, and strict q14's reachability wall is the CASE gap.

Because that cartesian is O(lineitem × part), q14 is tractable **only at the gauge scale**: at the CLI's
default `--sf 0.01` it is ~100M rows and never returns. So q14 is **scale-capped** until ADR 270 lifts the
join key. `Q14_GAUGE_SCALE = 0.0001` and `resolve_q14_scale` (lib) encode the cap: the `run --query q14` CLI
path **defaults** to the gauge scale (so the no-`--sf` gate command runs and matches the measured row) and
**refuses** a larger explicit `--sf` with a harness error (exit 2) rather than hang. The refusal is a *usage
constraint, not a query wall* — it never becomes a telemetry row (consistent with ADR 261's "unknown arg =
harness error, not a blocked/fail"). This closes the one way the documented gate could hang.

## Tests
- `harness.rs::q14_dev_executes_and_strict_blocks_at_decimal_case` — dev pass@execute (+ digest), strict
  `blocked(unsupported_decimal_case@bind)`, q14 `implemented` in the inventory.
- `harness.rs::q14_scale_is_capped_at_the_gauge_scale` — `resolve_q14_scale` defaults to the gauge scale,
  honors a scale at the cap, and refuses a larger one (so the CLI cannot hang on the cartesian).
- `q14_probe.rs` — the two-wall grounding (no-facts cross-join vs seeded-facts CASE).
- q01/q06 harness tests, the architecture guard (`run_query(` + `fn run_strict_q6`), and all toron-tpch
  tests stay green; appending `l_partkey` did not move q01/q06.

## Forward arc (named by the measured row)
- **ADR 270 — CASE Decimal-vs-integer unification**: q14's measured first wall. Coerce an integer branch
  literal (`ELSE 0`) to the Decimal `THEN` branch type (the literal-coercion instinct ADR 266 used).
- **Then the rest of q14's decimal stack**: extend ADR 266's Decimal-literal coercion to `*`
  (`100.00 * decimal`), then scalar `Decimal ÷ Decimal` (reusing 268's i128 half-away-from-zero divide).
- **Separately (perf, not a reachability gate)**: comma-join `Filter(equi)` over `CrossJoin` → inner
  hash-join, so multi-table queries don't ride a cartesian at scale.

## Gate
fmt + `git diff --check` clean · `check -p toron-tpch --all-targets` clean · `test -p toron-tpch` green
(harness incl. q14, q14_probe, decimal_probe, architecture) · live matrix shows the measured q14 row.
