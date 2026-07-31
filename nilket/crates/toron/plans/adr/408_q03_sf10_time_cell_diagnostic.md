# ADR 408 - q03 sf=10 time-cell diagnostic (superlinear blowup)

## Context

The sf=1 q03 arc is closed: shape (401/402, SQL == Rune `666e0ac028036f70`), substrate mechanism (405 primitive table,
-13%), and two honest diagnostics (404 precision-strike stop-line, 407 build-only radix non-bank). 404 and 407 both
concluded the remaining levers are **execution-model-sized**. 408 enters that thread through its sharpest, most
decision-making signal.

**The signal:** q03 runs **~373 ms at sf=1** but **times out >360 s at sf=10** - a **>100x superlinear blowup** (linear
scaling predicts ~4 s). And it is **CPU-bound, not memory-throttled**: the 399 Rune probe recorded 5 m 57 s CPU in
6 m 00 s wall, and the 400 Toron probe recorded 5 m 55 s CPU in 6 m 00 s wall at the 4 GB scope. The engine computes
the entire window - so something *structural* changes at sf=10 (a path flip, a re-scan loop, a materialization blowup),
not "the same work, slower." q05/q10/q12 share the signature (>5 min, no row, 1.4-2.9 GiB peaks; 398 board), so q03's
cause likely generalizes.

**Peer context (article-grade):** DuckDB completes q03 sf=10 in **241 ms** - faster than Toron's sf=1. Polars **OOMs**
under 4 GiB at sf=10. Toron completing the cell at all would beat the peer on reach.

**Why q03 as the entry:** richest instrumentation in the repo (plan-proof with `--sf`, profile loop, join-build
telemetry, **scan ledger** - a re-scan loop shows up directly as "lineitem scanned N times"), and it continues the
article thread (q03: 2028 ms -> 373 ms at sf=1; this is the sf=10 chapter). 403 set the precedent: sf=10 walls can fall
to bounded routing fixes once diagnosed (q08 wall -> pass in one fenced cut).

## Decision

### Phase 0 - attribute the six minutes (diagnostic-first, before any fix)
0. **Clean rebuild first.** `target/release/toron-tpch` is currently built from 407's **rejected radix candidate** -
   rebuild release from clean source before any probe (the 407 results doc records this hazard).
1. **Fresh live re-probe** post-405/406/407 under the fixed 398 scope (`MemoryMax=4G`, `MemoryHigh=4G`,
   `RuntimeMaxSec=360s`) - never trust the cached board; 405 moved the substrate. **Toron SQL is the primary cell**
   because the board failure is the default SQL path; Rune is the same-executor control. Record wall, CPU, peak RSS,
   and whether a row lands for Toron first, then Rune as the isolation/control row.
2. **Plan-shape-at-scale proof.** Dump the q03 **sf=10** SQL-vs-Rune bound plan (`q03-plan-proof --sf 10`) and diff
   both against each other and against the sf=1 closure (`666e0ac028036f70`). The binder's join-strategy/exchange
   decisions consume row/byte facts, which differ at sf=10 - **a plan-level flip at scale (SQL/Rune divergence,
   strategy change, exchange return, collapse not firing) could alone explain superlinearity** and would redirect the
   fix to the binder, not execution.
3. **Bounded-window attribution** of the timeout run: sampled `perf` (delayed start, within the bounded window), spill
   counters, and timeout-safe scan/join progress telemetry. The existing `TORON_TPCH_PARQUET_SCAN_LEDGER` and
   `TORON_JOIN_BUILD_PROFILE` are useful on completing/profile-loop rows, but they are in-memory and may print nothing
   when a 360 s timeout kills the process. If the normal rail cannot flush progress before termination, add a
   temporary/off-by-default diagnostic hook that writes scan counts and join-build profile rows incrementally (or keep
   it only if it is cleanly reusable). A re-scan loop must be proved by durable counts, not inferred from a missing
   JSON row.
4. **Classify the blowup:**
   - **(a) partition/re-scan loop** - over-cap routing into a superlinear re-scan path (403's `PartitionedScan`
     family re-executing huge inputs per partition);
   - **(b) materialization blowup** - a materialized intermediate (derived join output, join selection vector) growing
     superlinearly or being rebuilt repeatedly;
   - **(c) plan/exchange/aggregate term** - a scale-dependent SQL/Rune shape divergence, exchange/aggregate path, or
     402 collapse not firing at sf=10 co-location facts;
   - **(d) true execution-model floor** - linear-but-huge materialized execution; no bounded fix exists.

Record probe results, the sf=10 plan proof, the attribution evidence, and the classification **before** any fix.

### The fork - bounded fix OR design spec
- **Bounded/fenceable routing or planner fix** (classes (a)/(c), sometimes (b)): implement **only** if Phase 0 names a
  specific mis-routing/mis-shape with a generic, fenceable fix (403-style). No q03/table/query branch; the fence is a
  plan-shape/size property.
- **Streaming-completion design spec** (class (d), or unfenceable (b)): seal Phase 0 as **the streaming-completion
  design spec with hard evidence** - where the time goes, what must stream, which operators hold the floor. That
  spec-with-evidence is the deliverable, and the big-rock ADR builds on it.

### Stop-lines
- **No q03-shaped hack** - a fix must be generic and fenced; else the diagnostic is the outcome.
- **No cap bump** - completing by raising `MemoryMax`/task budget or the 360 s window is out of bounds.

## Acceptance
- **Phase 0 recorded:** clean-rebuild note, fresh live Toron primary probe row plus Rune control/isolation row,
  **sf=10 SQL-vs-Rune plan proof and sf=1 comparison** (fingerprint/shape diff or match), bounded-window attribution
  (perf frames, timeout-safe telemetry/scan counts, spill), and the classification - before any fix.
- **Either** a bounded generic fix: q03 sf=10 moves **off** time/no-row - it **completes** under the 398 scope with
  persisted correctness `pass@compare`, **or advances to a named + reproduced next wall** (reach-board honesty; a
  silent same-wall re-fail is not banked). **Or** the sealed streaming-completion design spec (named floor + evidence).
- **sf=1 unregressed:** the q03 sf=1 four-way stays within its band (fingerprint `666e0ac028036f70`); the won ground is
  a control, not collateral.
- **Controls:** q08/q19/q04 sf=10 stay green (no regression).
- **Generalization signal:** if a fix lands, re-probe q05/q10/q12 sf=10 cheaply and record any movement (they share the
  signature; movement is evidence the cut is generic, not banked acceptance).
- **Full engine gate** (if code changes): `--no-fail-fast` both crates; de-branded; **no committed `.perf.data`**.
- **Scope honesty:** one cell's cause named (and fixed if fenceable); **not** a claim that all >360 s cells complete;
  under (d) the big rock explicitly remains, now with a spec.

## Guardrails
- **Diagnostic-first** - no fix before the classification; the fork is decided by Phase 0 evidence.
- **Clean binary** - rebuild before probing (407's rejected-candidate binary is on disk).
- **Plan-shape check before execution blame** - the sf=10 SQL/Rune bound plans may differ from each other and from
  sf=1; verify before attributing to the executor.
- **Timeout-safe diagnostics** - do not rely on in-memory ledgers that only print after successful completion. If the
  run times out before JSON is emitted, add or use an off-by-default progress hook before classifying the blowup.
- **Bounded window discipline** - all probes under the fixed scope; no cap bump; timeout runs are attributed, not
  extended.
- **Generic or diagnostic** - no q03 branch; unfenceable cause -> the spec is the deliverable.
- **Won ground is a control** - sf=1 q03 four-way + q08/q19/q04 must not regress.

## Risks
- **Cause is the execution model itself (d)** -> the spec-with-evidence is the accepted outcome, not a failure.
- **Plan flips at sf=10** -> Phase 0 step 2 catches it; the fix may be binder-side (cheaper), not execution.
- **Ledger does not flush on timeout** -> add a temporary/off-by-default progress hook; missing ledger output is not
  evidence of no re-scan loop.
- **Multiple compounding causes** -> attribute shares within the window; fix the dominant one only if fenceable; name
  the rest.
- **Probe cost** (each timeout run burns 6 min) -> bound the runs; lean on the ledger/telemetry (cheap) before
  repeated perf passes.
- **A fix helps q03 but not siblings** -> record the q05/q10/q12 re-probe honestly; generalization is evidence, not
  assumed.
- **Regression on won ground** -> sf=1 four-way + controls + full gate.

## Forward Arc
Under a bounded fix, the reach board moves (potentially q05/q10/q12 with it) and the next heavy cell continues the
thread. Under the design spec, the streaming-completion big rock finally has evidence-grade scoping - which operators
hold the floor, what must stream - and 407's build+probe/output-aware partitioning folds into that design. q08
execution speed remains a follow-on. Article-grade throughout: this is the "why does a 373 ms query take six minutes
at 10x data" chapter, with DuckDB at 241 ms and Polars OOM as the peer frame.

## Grounding evidence (file:line)
q03 sf=1: 373.105 ms four-way, fp `666e0ac028036f70` (404 results baseline). q03 sf=10: Rune >360 s timeout, CPU
5m57s / wall 6m00s, peak 4G (`plans/impl/399_post_397_q03_substrate_strike_results.md` sf=10 reach signal); Toron
>360 s timeout, CPU 5m55s / wall 6m00s (`plans/impl/400_sf10_post_399_reach_recheck_results.md`). 398 board: q03
5m31s/2.7 GiB, q05 5m27s/2.9 GiB, q10 5m48s/2.7 GiB, q12 5m50s/1.4 GiB, all no-row
(`plans/impl/398_sf10_post_397_mini_atlas_results.md`); DuckDB q03 sf=10 241 ms, Polars OOM under 4 GiB (same board).
Instrumentation: `q03-plan-proof` (persisted proof with `--sf`), `TORON_TPCH_PROFILE_LOOP`, `TORON_JOIN_BUILD_PROFILE`,
`TORON_TPCH_PARQUET_SCAN_LEDGER` (404 results used all three on completing/profile-loop rows), spill counters (perf rail
rows). 403 precedent: q08 sf=10 wall -> pass via a fenced routing fix
(`plans/impl/403_q08_reoriented_build_accumulator_streaming_results.md`). Stale-binary hazard: 407 results, final state
note. perf enabled (`perf_event_paranoid=1`).
