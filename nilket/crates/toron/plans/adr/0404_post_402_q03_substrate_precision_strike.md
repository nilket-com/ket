# ADR 404 - post-402 q03 substrate precision strike

## Context

402 closed q03's SQL/Rune *logical-shape* gap: the default SQL path and the Rune path now share plan fingerprint
`666e0ac028036f70` and run at ~398 ms each (402 results: SQL 398.532 ms, Rune 399.133 ms). That makes this the cleanest
substrate-measurement point in the whole 391-402 arc: **same logical plan, yet Toron ~398 ms vs Polars ~102 ms (~3.7x)
vs DuckDB ~40 ms (~10x)**. Any remaining time is pure substrate, and it is now visible on the SQL path a real user runs -
not only on the hand-authored Rune shape.

399 explicitly teed up this cut in its forward: re-profile, **name the Toron-owned producer behind the residual frame
first, then strike that producer or seal as a diagnostic.** 399 struck only the hash *function* (SipHash -> `ahash`) and
left named residuals: **hash-table entry/probe work** (`hashbrown::rustc_entry` still hot), **Parquet RLE/dictionary
decode**, and **copy/materialization**. 400/401/402 detoured to reach and shape; 404 returns to the substrate now that
the measurement is clean.

**Headline metric: Toron SQL / Polars, with Rune as the same-shape control** (both must move together and stay
shape-equivalent).

## Decision

### Phase 0 - symbol-gated post-402 profile + cause ledger (before any code)
1. **Fresh four-way baseline** before code (`toron,rune,polars_sql,duckdb`, sf=1, runs=7). This establishes the
   current post-402 Toron SQL / Polars ratio on the same machine/profile that will be used for the post-cut row. Do not
   bank a speedup from inherited 401/402 numbers alone.
2. **Profile** the post-402 q03 sf=1 **materialized path** (`spill=0`, fingerprint `666e0ac028036f70`) with a
   symbol-gated release build (`CARGO_PROFILE_RELEASE_DEBUG=true`, `CARGO_PROFILE_RELEASE_STRIP=false`). Use the existing
   Rune-q03 profile loop (`TORON_TPCH_PROFILE_LOOP` + `TORON_JOIN_BUILD_PROFILE`, `perf-run-one --engine rune`); this is
   valid because SQL == Rune post-402.
3. **Cause ledger:** classify the dominant residual as one of:
   - hash-table entry/probe;
   - Parquet / RLE / dictionary decode;
   - copy / materialization;
   - **diffuse / no narrow producer.**
   Attribute the top frames to a **named Toron-owned producer**, not to a library primitive.

Record the profile + ledger classification in the results doc **before** code.

### The cut - precision strike OR stop-line
- **Precision strike:** cut **only** if there is a named Toron-owned, **behavior-neutral** producer behind the dominant
  frame - strike that producer. **No generic "optimize hashbrown / memmove" strike**: a library-primitive frame with no
  owning Toron producer is not a valid target (399's discipline: no copy primitive struck without an owning producer).
- **Stop-line:** if the residual is **diffuse**, or requires **vectorized / pipelined execution** (a mechanism, not a
  producer), seal a diagnostic naming the residual and feed the big-rock execution-model thread. No forced strike.

## Acceptance
- **Phase 0 recorded:** the fresh pre-cut four-way baseline, the symbol-gated post-402 profile (materialized path,
  `spill=0`, fingerprint `666e0ac028036f70`) + the cause-ledger classification, before code.
- **Either** a behavior-neutral precision strike on a named Toron-owned producer **or** a sealed diagnostic (diffuse /
  needs-vectorization) feeding the big rock.
- **Behavior-neutral:** q03 plan fingerprint stays `666e0ac028036f70`; correctness `pass@compare`; output rows `11620`;
  `spill=0`. Substrate only - no plan/shape/output/spill drift.
- **Four-way scorecard after the strike** (`toron,rune,polars_sql,duckdb`, sf=1, runs=7) proving **SQL and Rune move
  together and stay shape-equivalent (SQL/Rune ~1.00x)** - the strike is a substrate change, not a shape change. Compare
  against the fresh Phase 0 four-way baseline.
- **Primary:** **Toron SQL / Polars** improves beyond the runs=7 noise band (headline), **or** the residual is
  explicitly named (stop-line). Rune moves with SQL (same substrate); a speedup on only one path is a red flag.
- **No sibling regression** - q04/q19 shape + correctness unchanged.
- **No reach claim / no reach regression** - this is a sf=1 substrate cut, not a heavy-cell fix. A cheap sf=10 signal may
  be recorded for the over-time curve, but it is not the primary metric and a reach improvement is not required.
- **Full engine gate:** `--no-fail-fast` both crates, with `toron-tpch --features polars-sql-reference`;
  de-branded; **no committed `.perf.data`**.

## Guardrails
- **Named Toron-owned producer only** - no generic library-primitive strike; name the producer behind the frame before
  cutting (395-399 cause-ledger discipline).
- **Behavior-neutral** - fingerprint / rows / spill unchanged; substrate, not plan or reach.
- **Headline = Toron SQL / Polars; Rune = same-shape control** - both move together, SQL/Rune stays ~1.00x.
- **Diffuse or needs-vectorization -> stop-line** - seal a diagnostic and feed the big rock; do not force a strike.
- **Profile the timed path** - the post-402 materialized path (`spill=0`), not a memory-spec helper.

## Risks
- **Diffuse residual / no narrow producer** -> stop-line; a legitimate outcome that feeds the big rock, not a failure.
- **Needs vectorized / pipelined execution** -> stop-line; that is the execution-model change, not a precision strike.
- **Profile moves but median does not** (as in 399's modest -3.7%) -> banked only if Toron/Polars beats the noise band;
  otherwise the residual is named, not banked.
- **Behavior drift** (fingerprint / rows / spill change) -> rejected; substrate strikes must be behavior-neutral.
- **One-path speedup** (SQL moves, Rune does not, or vice versa) -> red flag; both share the substrate and must move
  together.
- **Missing pre-cut baseline** -> no banked Toron/Polars speedup; inherited rows are context, not the acceptance metric.
- **Sibling regression** -> q04/q19 gate.

## Forward Arc
A landed strike moves the four-way scorecard, adds an article datapoint, and names the next residual for a follow-on
strike - until the tail goes diffuse. A diagnostic names the residual (likely a vectorized probe or a decode pipeline)
as a concrete target for the big-rock execution-model / streaming-completion thread, alongside the sf=10 time cells and
q08's post-403 execution speed. 404 is the last well-lit sf=1 substrate cut before that slower heavy-cell work.
Article-grade throughout.

## Grounding evidence (file:line)
Post-402 q03 SQL == Rune fingerprint `666e0ac028036f70`, ~398 ms both (`plans/impl/0402_grouped_aggregate_exchange_placement_results.md`:
SQL 398.532, Rune 399.133); Polars ~102 ms, DuckDB ~40 ms peer references. 399 substrate strike (SipHash -> `ahash`) +
residual ledger (hash-table probe, Parquet RLE/dict decode, copy/materialization) and the re-profile hand-off:
`plans/impl/0399_post_397_q03_substrate_strike_results.md` (cause ledger, Forward). Profile harness: `TORON_TPCH_PROFILE_LOOP`
+ `TORON_JOIN_BUILD_PROFILE`, `perf-run-one --engine rune --query q03`; symbol-gated build
`CARGO_PROFILE_RELEASE_DEBUG=true CARGO_PROFILE_RELEASE_STRIP=false`. 399 stop-line discipline: no generic memmove strike
without an owning producer.
