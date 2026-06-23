# ADR 330 - scale-atlas probe (sf=0.1, sf=1)

## Status
Proposed.

## Context

The board is closed at sf=0.01 (reach 22 / correctness 22) - but sf=0.01 is a **correctness/reach proxy, not a
scale test.** The distributed fragmenter, exchanges, cost-bound/memory-streaming (ADR 118), and spill are almost
entirely unexercised. This ADR opens the **scale arc** the way the failure atlas opened the reach arc: a **probe
that measures, not a cut that fixes.** It reuses the existing scale-aware surfaces (`run --sf`, `correctness --query
--sf`), classifies each query's first scale wall, and **names the next arc from measured evidence.** No engine
change - measure-don't-predict.

## Decision - run the gauge at sf=0.1 and sf=1, classify each query's first scale wall

### D1 - scale points
- **sf=0.1** (~10x, ~100 MB) - the cheap pass; catches gross walls first.
- **sf=1** (~100x, ~1 GB lineitem ~ 6M rows) - the foundation test: where memory bounds, distributed
  fragmentation, and spill actually bite.
- Stop here. sf=10+ is a *later* arc, only if sf=1 holds.

### D2 - the probe surface (the scale-aware commands, not correctness-matrix)
- **Reach-at-scale:** `run --query qNN --mode strict --sf {0.1,1}` for **all 22** queries - the scale-aware reach
  surface (`matrix` aggregates the logged rows). This is the primary probe.
- **Correctness-at-scale:** per-query `correctness --query qNN --sf {0.1,1}` **only where the oracle is
  scale-stable** (generator-traversal oracles that recompute at the given sf). Flag the fixed-scale ones explicitly
  - q17 is pinned at `--sf 0.02` (reach-only at scale); any other oracle that isn't scale-general is reach-only and
  named as such in the atlas.
- **Not** `correctness-matrix`: it reads the `correctness.jsonl` log and is **not scale-partitioned**, so it cannot
  produce a per-SF correctness atlas.

### D3 - the classification (per query x scale point), concrete thresholds
A "scale atlas": `(query, sf)` -> one of, by **fixed rules** (no soft post-hoc judgment):
- **green** - finishes within both bounds; `pass@execute` (reach) and, where scale-stable, `pass@compare`.
- **memory** - the process hits the cgroup memory ceiling or is OOM-killed.
- **time** - wall clock exceeds the fixed per-query cap at that scale.
- **distributed-correctness** - the query finishes within bounds but a scale-stable correctness probe at that same sf
  now `fail@compare`, or the same-sf digest is unstable across equivalent runs/partitions (a partition/exchange bug,
  or a precision/overflow that only manifests at scale - e.g. a SUM exceeding `Decimal128(38, …)` at sf=1).

**Concrete caps (tunable, set here so classification is mechanical):**
- sf=0.1: memory ceiling ~1 GB, wall-clock cap ~1 min.
- sf=1: memory ceiling ~4 GB, wall-clock cap ~5 min.
- Rationale: a cost-bound-streamed query (ADR 118 keeps memory roughly constant, not table-proportional) fits well
  under the ceiling; a cartesian or unbounded plan blows past one cap and is classified + killed, never left to
  hang.

### D4 - methodology (the cost discipline)
- **Run the built binary directly** at default scale, not a `cargo-cgroup` rebuild (a throttled rebuild *looks* like
  a runtime hang - the ADR 282 lesson). Build once, then `target/debug/toron-tpch run/correctness --sf N` under the
  cgroup memory + timeout caps.
- **Cap each query** with the cgroup memory ceiling + the wall-clock cap above, so a cartesian-at-scale is
  *classified* (memory/time) and killed, not allowed to hang the session.
- **Keyed-plan assertions still hold at scale** - a query green at sf=1 must still be keyed; a fake-green cartesian
  that fit at sf=0.1 will OOM or time out at sf=1 (that's the signal, not noise).

## Acceptance - the atlas is the deliverable
- A **scale atlas** produced: 22 queries x {sf=0.1, sf=1}, each cell classified (green / memory / time /
  distributed-correctness) by the fixed rules, with the precise reason. Honest - greens *and* walls.
- Reach probed via `run --sf` for all 22; correctness probed via `correctness --query --sf` only where scale-stable,
  with the reach-only queries (q17, any non-scale-general oracle) flagged.
- Measured on the built binary under the memory + timeout caps; reproducible (a recorded command sequence).
- **No engine change.** The atlas *names the next arc*: board mostly holds at sf=1 -> logical-completeness arc;
  walls -> scale-cuts arc (cost-bound/distributed/spill), each a scoped ADR keyed to a measured cell.
- The reach/correctness boards at sf=0.01 are unchanged (this adds a dimension, doesn't touch existing cells).

## Risks
- **Cost** - sf=1 is expensive; the per-query memory ceiling + timeout are mandatory; build once and run the binary;
  don't sweep blind.
- **Probe discipline** - measurement only; resist fixing a wall mid-probe (that's the next arc, scoped from the
  atlas). An honest `memory@sf=1` cell is the deliverable, not a red to rush-fix.
- **Scale-stable oracles only** - classify correctness only where the oracle recomputes at the given sf; flag
  fixed-scale (q17) and any non-general oracle as reach-only.
- **Threshold honesty** - the caps are fixed up front (D3); a cell is classified by rule, not by after-the-fact
  judgment.

## Forward arc
The scale atlas turns the strategic fork (SF vs logical completeness) into a **measured decision**: the sf=1 wall
pattern picks the next arc. Green-dominant -> logical-completeness arc (the deferred follow-ons + a breadth gauge).
Wall-dominant -> a scale-cuts arc (cost-bound/distributed/spill), each ADR keyed to an atlas cell - the
failure-atlas method, second time around.

## Why toron-home
The probe + classification live in `toron-tpch` (the existing `run`/`correctness` scale-aware surfaces + a wall
classifier). The scale behavior under test is `crates/toron` (fragmenter/exchanges/cost-bound), but this ADR only
*measures* it. No Wendelon / cross-ring surface.

## Council provenance
Operator + Codex + Claude (3/3: probe SF first). The structural argument: logical completeness is additive +
bounded by known gaps; SF is a foundation check with the measuring surface already in hand. Codex's corrections
folded: phrase the probe around the **scale-aware `run --sf` / `correctness --query --sf`** surfaces (not
`correctness-matrix`, which reads a non-scale-partitioned log); use correctness **only where the oracle is
scale-stable**, flagged per query; and set **concrete memory/time/distributed-correctness thresholds** up front so
the atlas stays mechanical. The measure-don't-predict discipline that ran the reach+correctness arcs, applied to the
strategic fork.
