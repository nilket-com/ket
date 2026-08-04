# ADR 359 - the TPC-H SF=10 atlas (live, fixed-cap, measure + classify, with DuckDB)

## Status
Proposed.

## Context

The high-signal next step on the scale arc. Re-measure **all 22 queries live at sf=10** under the 356 perf rail with
the 358 DuckDB comparison, and **classify the walls** - the first real test of the streaming substrate's
constant-memory claim at 10x. **Measure and classify only; no fixes** - sf=10 is a new landscape, and the deliverable
is the wall atlas (producers / bytes / reasons + the Toron-vs-DuckDB board), not a mixed probe+patch.

## Decision

### Fixed 4 GB cap (the constant-memory test)
Run sf=10 under the **same fixed 4 GB cap as sf=1**. That is the only clean test of "constant memory": if a cell needs
more, **the atlas says so** (a named wall) rather than moving the bar. **No cap bump** to manufacture green.

**Cap discipline:** do not use the `affinity` backend for the canonical sf=10 evidence. Affinity is acceptable for a
DuckDB path smoke only; it has no memory ceiling. The canonical pilot/full run must execute the already-built release
binary in a real memory-capped scope (systemd/nerdctl/direct cgroup) with a documented 4 GB runtime ceiling. Build first;
do not classify a release rebuild under the measurement cap.

### Measure + classify only (no fixes in 359)
Per cell, live: reach (pass@execute via the rail) + perf (execute_ms) + correctness (pass@compare where reachable) +
the DuckDB comparison. Each red is **classified**: kind (memory / time / spill-volume / join-orientation /
exchange-or-materialization / correctness drift) + **named producer + bytes** (332). The sf=10 walls become *subsequent*
gated ADRs; 359 does not fix them.

### Live, never cached (the load-bearing discipline)
`matrix`/`correctness-matrix` are sf-agnostic cache renders and would report stale green. **Every sf=10 cell is a live
run** - the rail (fresh-process, release) for reach+perf, live correctness for the answer. No cached-matrix row counts
as an sf=10 result.

### DuckDB included now (the 358 frame)
Same artifact frame: **Toron absolute + Toron regression bands + DuckDB reference**. DuckDB lifts out of the sf=1
noise floor at sf=10, so the comparison is meaningful here. **If DuckDB gets noisy, record the flag - don't drop it**
(the schema/row sanity + stability flags ride along).

### The sf=10 pilot gate (Phase 0, not a separate ADR)
Before launching all 22: run a **small sf=10 pilot - q06 / q14 / q21** (a light scan-agg, a streamed comma-join, a
semi/anti spill) - to verify **runtime, resource shape, output volume**, and the DuckDB-side Parquet export/parity at
sf=10 size. The pilot starts with `--runs 1 --warmup 0`; only after it confirms the shape is sane do we choose the full
run's `N/K` (up to the normal `7/1` if affordable) and run the board.

## Phase 0 - pilot + run design
1. **Data:** generate sf=10 tables (10x); gen excluded from execute_ms; confirm the 358 Parquet export handles sf=10
   volume for the DuckDB side.
2. **Pilot (q06/q14/q21):** run three one-query perf invocations (the harness supports one query or `all`, not a
   comma-list today): `q06`, `q14`, `q21`, `--sf 10 --mode strict --runs 1 --warmup 0 --engines toron,duckdb`
   (release, fixed 4 GB runtime cap). Check: wall-clock per query, peak RSS vs the 4 GB cap, output row volume, DuckDB
   export size/parity, stability spread. Choose the full run's `N/K` from this evidence; do not change the cap.
3. **Full board:** the same fixed-cap command over `--query all`; live correctness for reachable cells.
4. **Classification taxonomy:** per query - reach/wall(kind, producer, bytes); correctness; Toron execute_ms vs DuckDB;
   stability; constant-memory-held? (vs the sf=1 streamed cells).

## Acceptance (an honest sf=10 landscape on one artifact)
- **Pilot first** (q06/q14/q21) confirms runtime/resource/output-volume shape before the full run.
- **All 22 measured live at sf=10** (rail reach + execute_ms; live correctness where the query's correctness command
  supports sf=10; DuckDB comparison) - no cached-matrix rows. If a correctness oracle rejects sf=10 (known example:
  q17's current correctness scale policy), record `no_sf10_oracle` rather than manufacturing a compare result.
- **Every wall classified** - kind + named producer + bytes (332); the streaming substrate's **constant-memory verdict**
  recorded (which cells hold at 10x under 4 GB, which break and why - the upstream child materialization / exchange the
  prime suspects from ADR 349).
- **The Toron-vs-DuckDB sf=10 board** (absolute + bands + DuckDB; flags recorded, not dropped).
- **Honest reds** - walls named, not greened; **no cap bump, no fixes** in 359.
- **No engine behavior change** - measurement only; the sf=1 board unchanged.

## Guardrails
- **Fixed 4 GB** - constant-memory tested, not granted; over-cap cells are named walls.
- **Live, not cached** - the rail + live correctness; never the matrix for an sf=10 claim.
- **Measure + classify only** - no fixes; sf=10 walls are subsequent gated ADRs.
- **Named walls** - 332 producer + bytes + kind; no silent OOM, no "it's slow" without a number.
- **DuckDB included, flags recorded** - noisy/schema-mismatch rows are kept + flagged, not dropped.
- **Pilot gate** - q06/q14/q21 first; verify shape before the full 22.
- **Honest reds > lying green.**

## Risks
- **Resource surprise on all-22** - the pilot gate de-risks it (verify shape first).
- **Cap-bump temptation** - rejected; over-cap is a named wall, the atlas's honest output.
- **Cached-matrix contamination** - explicitly live-only; the lesson from ADR 356.
- **DuckDB noise at sf=10** - recorded as flags; the comparison is reported with caveats, not dropped.
- **Scope creep into fixes** - measure+classify only; the first wall fix is its own ADR.

## Forward arc
359 (the sf=10 atlas) names the sf=10 walls -> each becomes a targeted, rail-gated ADR (likely first: the upstream
child materialization / exchange streaming - the data-proportional executor-model terms ADR 349 deferred), measured
against the DuckDB reference -> sf=100 / Rune-dataframe TPC-H later. Plus the cross-engine digest oracle + the
matrix-hygiene fix. No central roadmap.

## Why toron-home
The gauge harness (`crates/toron-tpch`) measuring `crates/toron` at sf=10. No Wendelon / cross-ring surface; no engine
behavior change.

## Council provenance
Operator + Claude. The sf=10 atlas under the 356 rail + 358 DuckDB comparison. Operator's steer: **fixed 4 GB cap**
(the only clean constant-memory test - a cell needing more is a named wall, not a moved bar); **measure + classify
only** (the wall atlas with producers/bytes/reasons, no fixes in 359); **DuckDB included now** (Toron absolute + bands +
DuckDB reference; record noise flags, don't drop). Operator's added gate: a **small sf=10 pilot (q06/q14/q21) in
Phase 0** to verify runtime/resource/output-volume shape before the full 22. Live-only (never the cached matrix - the
ADR 356 lesson).
