# ADR 392 - substrate-ceiling measurement at completing scale (q03 sf=1)

## Status
Proposed.

## Context

391 built the Rune perf instrument but couldn't measure the substrate at sf=10: SQL q03 is censored as time/no-row,
while the Rune-authored q03 shape fails early at `scan output accumulator`; sf=0.01 is fixed-overhead noise. **392
measures the substrate ceiling at a completing scale:** q03 three-way (SQL / Rune / DuckDB) at **sf=1 persisted
Parquet**, reusing the 391 instrument with full perf-rail measurement discipline. **Diagnostic-only, not a fix.**
Headline: **`Rune / DuckDB`** - is the shared Toron substrate 5x, 50x, or 500x off DuckDB when the hand-authored shape
completes? That number decides the next arc.

## Decision

### Scale: sf=1 primary, sf=3 escalation only
**sf=1** is the headline - large enough that DuckDB q03 is above the single-digit-ms noise floor, cheap enough to run
with repeats. **sf=3 is an escalation, not the headline** - run it **only if** the sf=1 ratio is unstable (spread too
wide) or obviously fixed-overhead dominated.

### Measurement quality (356 perf-rail discipline)
- **sf=1, persisted Parquet, release, fixed 4 GB, `--runs 7 --warmup 1`.**
- **Ratios use medians**, not single runs (`Rune_median / DuckDB_median`, etc.).
- **The stability gate applies before interpreting ratios** - if a row's spread exceeds the noise band, it is unstable;
  do not report a ratio as trustworthy, escalate to sf=3 or more runs.
- **Persisted data even at sf=1** (not the in-memory fixture) - match the sf=10 path + DuckDB-on-the-same-files parity.

### Correctness gate (before any ratio)
**SQL and Rune q03 must pass the q03 oracle/order comparator at sf=1** (same answer including ORDER BY) before any ratio
is interpreted. SQL's perf row is reach/timing only, so SQL correctness must be checked by the persisted correctness
command. Rune's perf row is acceptable only if it reaches `phase=compare status=pass` through the ADR 391 Rune oracle
gate. No faster-because-wrong.

### Branches
- **Rune completes, `Rune/DuckDB` large:** **symbol-retaining profile** the Rune path (381 method) and **name the
  substrate stack** (Arrow kernel / materialize / spill / hash).
- **Rune fails at sf=1** (reach wall): **verdict = reach-blocked, not substrate-measured** - q03 reach must be fixed
  before the ceiling can be measured (next arc reach-first).
- **sf=1 unstable / fixed-overhead-dominated:** escalate to sf=3.

## Acceptance (diagnostic-only)
- **The sf=1 persisted three-way table** with **raw timings AND ratios** (medians):

| engine | status | execute_ms (median, min, spread) | ratio_vs_duckdb (median) | correctness | producer / profile |
| --- | --- | --- | ---: | --- | --- |
| sql | | | | oracle/order | |
| rune | | | | oracle/order | |
| duckdb | | | 1.0 | reference | |

- **Correctness gate passed** - SQL and Rune oracle/order at sf=1 - **before** ratios are interpreted.
- **Stability gate passed** (runs=7, warmup=1, median, spread within band); else sf=3 escalation recorded.
- **Headline `Rune/DuckDB` (median)** + secondary `SQL/Rune`, `SQL/DuckDB`.
- **Ratios live in the results artifact, not as fake perf rows** - raw `engine="rune"`/`toron`/`duckdb` perf rows are
  real; ratios are computed in the doc.
- **If Rune slow:** symbol-retaining profile + named substrate stack. **If Rune fails at sf=1:** reach-blocked verdict.
- **The named next-arc decision** (reach-first / substrate precision-strikes / planner-shape) justified by the number.
- **No engine change** (harness/measurement-only); `crates/toron/src` unchanged; `--no-fail-fast` unchanged; de-branded;
  no qNN; large `.perf.data` uncommitted.

## Guardrails
- **sf=1 primary, sf=3 escalation only** (on instability/fixed-overhead), not the headline.
- **Medians + stability gate before ratios** - 356 discipline; no single-run ratios; unstable -> escalate, don't report.
- **Persisted data even at sf=1** - match sf=10 + DuckDB parity.
- **Correctness gate before ratios** - oracle/order, no faster-because-wrong.
- **Ratios in the artifact, real raw perf rows** - no fabricated ratio rows.
- **Diagnostic-only; honest branches (slow->profile, fail->reach-first); no engine change; --no-fail-fast; no qNN.**

## Risks
- **sf=1 fixed-overhead-dominated** - the stability gate + sf=3 escalation catch it.
- **Unstable spread** - runs=7/warmup=1 + the gate; escalate, don't trust a noisy ratio.
- **Rune fails at sf=1** - honest reach-blocked verdict (not a substrate number).
- **Faster-because-wrong** - oracle/order gate.
- **Harness leak into engine** - `crates/toron/src` unchanged; split an ADR if a fix tempts.

## Forward Arc
392's `Rune/DuckDB` (median, stable) decides the next arc:
- **Substrate close (<~10x):** sf=10 gap is mostly **reach** (the scan-output executor term) -> reach-first (continue
  367-390-style), then re-measure substrate at sf=10.
- **Substrate far (>~50x):** **materialize-each is the ceiling** -> execution precision-strikes (385-style) on the named
  substrate stack, or the honest vectorized/pipelined question.
- **`SQL/Rune` large:** a **planner/join-order** lever in parallel.
- **Rune fails at sf=1:** reach-first is forced.

## Grounding evidence (file:line)
391 instrument: `run_rune_perf_query_with_data_source`, `--engines rune`; isolation proof + oracle/order; sf=0.01
SQL/Rune=2.19x; sf=10 Rune fails `scan output accumulator` 744ms, SQL censored, DuckDB 1.03s. 356 perf rail (runs/warmup/
median/stability gate). 360 persisted surface (`prepare --sf 1 --data-source parquet`). 381 symbol-retaining profile.
q03 = customer/orders/lineitem + group.
