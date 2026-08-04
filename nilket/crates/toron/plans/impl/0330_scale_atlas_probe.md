# ADR 330 - Implementation plan: scale-atlas probe (sf=0.1, sf=1)

Pairs with `plans/adr/0330_scale_atlas_probe.md`. A **measurement probe** in `toron-tpch` - no engine cut. Produces
the scale atlas (22 queries x {sf=0.1, sf=1}, classified) that names the next arc. Built binary, memory + timeout
capped, mechanical classification.

## Phase 0 - build once, set the caps
- Build the binary once: `scripts/cargo-cgroup build -p toron-tpch` -> run `target/debug/toron-tpch ...` directly
  (do not `cargo-cgroup run` per query - a throttled rebuild looks like a runtime hang, ADR 282).
- Caps (fixed up front, classification is mechanical):
  - sf=0.1: memory ~1 GB, wall-clock ~1 min.
  - sf=1: memory ~4 GB, wall-clock ~5 min.
- Identify the scale-stable correctness oracles (generator-traversal, recompute at the given sf) vs the fixed-scale
  ones (q17 @ 0.02, and any other non-general oracle) - the latter are reach-only at scale.

## D1 - reach-at-scale (all 22)
- For each q01..q22, `target/debug/toron-tpch run --query qNN --mode strict --sf {0.1,1}` under the cgroup memory
  ceiling + a wall-clock timeout for that scale.
- Classify the cell by rule:
  - finishes within both caps + `status=pass` -> **green (reach)**.
  - `phase=bind status=blocked` -> **bind-fail**.
  - `phase=execute status=fail` -> **execute-fail**.
  - OOM / memory-ceiling hit -> **memory**.
  - wall-clock cap exceeded (killed) -> **time**.
- Record the measured RSS + wall-clock per cell (the evidence), plus the keyed-plan status where the harness asserts
  it (a green-at-sf=1 must still be keyed).

## D2 - correctness-at-scale (scale-stable oracles only)
- For each scale-stable query: `target/debug/toron-tpch correctness --query qNN --sf {0.1,1}` under the same caps.
  - `phase=compare status=pass` within caps -> **green (correctness)**.
  - finishes but `fail@compare` or same-sf digest instability -> **distributed-correctness**.
  - OOM / timeout -> **memory** / **time** (same rules).
- Flag the reach-only queries (q17, any non-scale-general oracle) explicitly in the atlas - not silently skipped.

## D3 - the atlas (the deliverable)
- Assemble the `(query, sf)` -> {green / bind-fail / execute-fail / memory / time / distributed-correctness}
  matrix with per-cell evidence (RSS, wall-clock, reason). Record it in the results doc. Honest - greens and walls
  both.
- Optional (only if cheap): a `scale-matrix` harness helper that drives the sweep + prints the classified atlas;
  otherwise a recorded command sequence + the captured table is sufficient (this is a probe, not a permanent gate).

## Tests
- This is a probe, not a feature - no new behavioral assertions on the engine. If a `scale-matrix` helper is added,
  a unit test that it classifies a known (capped) outcome correctly. The existing reach/correctness tests at sf=0.01
  stay green (unchanged).

## Phase 3 - read the atlas, name the next arc
- Summarize the sf=1 wall pattern: green-dominant -> the next arc is logical completeness; wall-dominant -> the next
  arc is scale-cuts (cost-bound/distributed/spill), each ADR keyed to a measured atlas cell.
- Do **not** fix any wall in this ADR.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
# the probe itself (built binary, capped) - recorded, not a CI gate:
scripts/cargo-cgroup build -p toron-tpch
#   target/debug/toron-tpch run --query qNN --mode strict --sf {0.1,1}   (x22, capped)
#   target/debug/toron-tpch correctness --query qNN --sf {0.1,1}         (scale-stable only, capped)
git diff --check
```

## Results doc
`crates/toron/plans/impl/0330_scale_atlas_probe_results.md`: the scale atlas (22 x {0.1,1}, classified) with per-cell
RSS + wall-clock + reason; the scale-stable vs reach-only oracle split; the sf=1 wall-pattern summary; and the named
next arc (logical-completeness vs scale-cuts) with the evidence behind the call. The sf=0.01 boards unchanged.

## Risk / blast radius
- **Cost** - sf=1 is expensive; caps mandatory; built binary; no blind sweep.
- **Probe discipline** - measure only; no mid-probe fixes; honest cells.
- **Scale-stable correctness** - only where the oracle recomputes at sf; flag the rest reach-only.
- **No engine change** - the existing sf=0.01 boards stay green.

## Seal
`impl: adr 330 scale atlas probe`. Do not commit wip drafts.
