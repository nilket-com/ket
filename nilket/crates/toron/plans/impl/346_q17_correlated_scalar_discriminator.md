# ADR 346 - Implementation plan: q17 the discriminator (correlated-scalar / LeftOuter recombine)

Pairs with `plans/adr/346_q17_correlated_scalar_discriminator.md`. A `crates/toron` (exec.rs) cut driven by a strict
Phase 0: measure q17's wall, assess toolkit coverage, and record the **discriminator verdict** (incremental toolkit
reuse vs a distinct correlated-scalar/LeftOuter substrate class). Wall-mover-or-green; the verdict is the primary
deliverable; **structural-only never justifies a green-target.** q17 is reach-only at `sf=1`: its correctness oracle
is pinned to `Q17_CORRECTNESS_SCALE = 0.02`, and `--sf 0.01` is an intentional masked-empty rejection.

## Phase 0 - the wall-boundary table (strict three-tier provenance)
- Build the table; **separate the tiers, never conflate:**

  | boundary | tier | source_of_measurement | bytes | toolkit_coverage | verdict |

  - **measured first wall at sf=1** - where q17 dies now (sf=1 trace, actual bytes);
  - **reachable measured downstream walls** - boundaries reached only after the first wall is removed (this ADR's later
    phase or a synthetic isolated-operator probe; actual bytes);
  - **structural-only predicted walls** - past the unreached frontier (plan-shape only, **bytes = n/a**).
- **The rule:** a structural-only boundary (e.g. the LeftOuter recombine, if q17 dies before it) sizes/names the next
  mechanism but **cannot justify a green-target**, and is **never** given measured bytes (the 344/345 rule).
- Ground which boundary the measured first wall is: inner `Join(0)` output, the correlated-scalar grouped aggregate
  (AVG-per-part over 6M lineitem), or the LeftOuter recombine.

## The gate - toolkit coverage of the MEASURED wall
- inner `Join(0)` -> reuse 343 (reorient) / 345 (streamed-spilled build).
- correlated-scalar grouped aggregate -> extend 338's fold to a **grouped** aggregate-over-big-input (part-keyed).
- LeftOuter recombine -> likely the **distinct** piece (asymmetric); if it is the measured wall, name the new substrate
  class rather than forcing a toolkit fit.

## D1 - the cut (per the measured wall)
- If the measured wall is toolkit-covered: apply the matching primitive (343/345/grouped-338) for q17's matched shape;
  target green or a reachable measured downstream wall.
- If the measured wall is the LeftOuter/correlated-scalar recombine (not toolkit-covered): this ADR's deliverable is the
  **named wall + the distinct mechanism named** (a streamed/spilled LeftOuter recombine, scoped for the next ADR), not
  a forced green.
- Any residual build/spill over budget -> a named producer (332), not a silent OOM.

## D2 - correctness of the correlated scalar (load-bearing)
- The AVG-per-part recombine must be exact regardless of streaming/spilling: per-part average computed over all
  lineitem rows for that part, recombined via LeftOuter (preserved outer rows, null-fill for non-matches). A
  streamed/spilled scalar must not drop or mis-key any part's average. The q17 oracle (fixed sf=0.02 `pass@compare`)
  is the gate.

## Tests (`toron` lib + `toron-tpch`)
- **Correlated-scalar correctness:** a synthetic correlated AVG-per-key + LeftOuter recombine -> streamed/spilled
  result equals the materialized result (per-key average exact, null-fill correct); q17 `pass@compare` at sf=0.02.
- **Provenance discipline (if applicable):** the wall-boundary table's structural-only rows carry no bytes; any green
  claim is backed by a measured (not structural) toolkit-covered wall.
- **Toolkit-reuse / shape fence:** if a 343/345/338 primitive is applied, its existing paths stay unchanged; non-matching
  shapes fall back.
- **No silent wall:** an over-budget residual is named (332).
- default-scale reach unchanged; q17 fixed-scale correctness unchanged.

## Phase 3 - re-measure (release, capped, sf=1)
```
scripts/cargo-cgroup build -p toron-tpch --release
# q17 @ sf=1 (4G): green (measured wall toolkit-covered), OR a named wall (332: producer + bytes) with the distinct
#   mechanism named. Record the three-tier wall table; do not give structural-only boundaries measured bytes.
```
- Record q17's measured first wall, any reachable measured downstream wall, the structural-only predicted walls, and
  the discriminator verdict. Update the ADR 330 atlas q17 cell.
- **Green-cell regression:** q01/q03/q05/q06/q10/q13/q16/q22 + q07/q14/q19/q21 stay green, unchanged plans.
- q12/q18/q04/q20 unchanged.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q17 --sf 0.02   # AVG-per-part exact (fixed q17 oracle scale)
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix     # 22/22 unchanged; q17 uses its fixed oracle scale
scripts/cargo-cgroup run -p toron-tpch -- matrix
scripts/cargo-cgroup run -p toron-tpch --release -- run --query q17 --mode strict --sf 1
scripts/cargo-cgroup run -p toron-tpch --release -- matrix --query q17 --sf 1
git diff --check
#   release/cgroup q17 sf=1: green or named wall + distinct-mechanism verdict; green cells unchanged
```

## Results doc
`crates/toron/plans/impl/346_q17_correlated_scalar_discriminator_results.md`: the three-tier wall-boundary table
(measured first / reachable measured downstream / structural-only predicted, with provenance); the toolkit-coverage of
the measured wall; the discriminator verdict (incremental vs distinct, with evidence); q17 sf=1 before/after (green or
named wall via 332); the correlated-scalar correctness result; sf=0.02 q17 pass@compare; the green-cell regression
check; the ADR 330 atlas q17 update.

## Risk / blast radius
- **Strict provenance** - the three-tier table; structural-only never green, never measured bytes (the LeftOuter trap).
- **Scalar correctness** - the AVG-per-part recombine exact under stream/spill (the q17 oracle gate).
- **Verdict by measurement** - incremental vs distinct decided by the measured wall + toolkit-coverage, not the
  structural prediction.
- **Scope** - q17 only; reuse 343/345/338 where measured; the LeftOuter/correlated-scalar new mechanism (if needed) is
  named, scoped to the next ADR; q02 noted not folded.
- **Core naming** - no `q17`/TPC-H query-number symbols in `crates/toron/src`; implementation helpers are shape-named
  (correlated-scalar / grouped fold / LeftOuter recombine). Query-number names stay in `toron-tpch` and plan/results
  docs.

## Seal
`impl: adr 346 q17 correlated scalar discriminator`. Do not commit wip drafts.
