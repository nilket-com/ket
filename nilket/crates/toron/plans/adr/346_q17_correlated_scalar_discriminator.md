# ADR 346 - q17 the discriminator (correlated-scalar / LeftOuter recombine: incremental toolkit vs distinct mechanism)

## Status
Proposed.

## Context

The "one more scale cut, then reassess" (operator). q07/q14/q19/q21 are green via the post-345 streaming toolkit
(collapse, exchange, reorient, semi/anti spill). q17 is the **discriminator**: it is a third shape - a
**correlated-scalar (AVG-per-part) + LeftOuter recombine** - so whether the toolkit covers it tells us if the remaining
memory walls (q12/q18/q04/q20) are **incremental** reuse or **hide a distinct substrate class**. Wall-mover-or-green,
but the **primary deliverable is the discriminator verdict**, kept honest by strict provenance.

### Grounded (the code)
- q17: `SUM(l_extendedprice) / 7.0 FROM lineitem, part WHERE p_partkey=l_partkey AND p_brand AND p_container AND
  l_quantity < (SELECT 0.2 * AVG(l_quantity) FROM lineitem WHERE l_partkey = p_partkey)` (lib.rs:131).
- q17 is **reach-only at sf=1**: its correctness oracle is deliberately pinned to the first non-empty correctness
  scale (`Q17_CORRECTNESS_SCALE = 0.02`). Do not use `--sf 0.01` as the q17 oracle gate; that scale is intentionally
  rejected by the harness as masked-empty.
- Shape: inner `Join(0)` (lineitem x part) + a **correlated-scalar** AVG-per-part (grouped aggregate over
  lineitem-by-partkey) resolved into a **LeftOuter recombine** (`DeferredScalarSubquery` -> join, exec.rs:721 / ADR 282)
  -> `Filter (l_quantity < 0.2*avg)` -> outer **global** `SUM/7.0`.
- 337/342: q17's first failure is in the correlated-scalar branch before the left-outer recombine; lineitem is
  build-side at `Join(0)` (spilled). 336: q17 wall = ADR 335 output guard ~1.65 GB. 338's fold did **not** green q17
  (its global aggregate sits above LeftOuter + Exchange + correlated-scalar, not a direct segment).

## Decision - measure the wall (strict provenance), assess toolkit coverage, name the verdict

### Phase 0 - the wall-boundary table, three provenance tiers (operator)
Separate, never conflate:
1. **measured first wall at sf=1** - where q17 actually dies now (actual bytes);
2. **reachable measured downstream walls** - boundaries reached only after the first wall is removed, then measured
   (actual bytes, this ADR's later phases or a synthetic isolated-operator probe);
3. **structural-only predicted walls** - boundaries past the unreached frontier (plan-shape only, **no bytes**).

| boundary | tier | source_of_measurement | bytes | toolkit_coverage | verdict |

**The rule:** a **structural-only** boundary (e.g. the LeftOuter recombine, if q17 dies *before* it) **sizes/names the
next mechanism but cannot justify a green-target.** A green-target requires the **measured** wall to be toolkit-covered.
Never present a structural-only LeftOuter byte as measured.

### Toolkit coverage (per measured boundary)
- inner `Join(0)` -> 343 (reorient) / 345 (streamed-spilled build);
- correlated-scalar grouped aggregate -> extend 338's fold to a **grouped** aggregate-over-big-input (part-keyed);
- **LeftOuter recombine -> likely the distinct piece** (asymmetric: not freely reorientable like inner, not an
  existence set like semi/anti) - if measured as the wall, name it as a new substrate class.

### The discriminator verdict (primary deliverable)
- **Incremental** - the measured wall is toolkit-covered (+ maybe a grouped fold) and q17 greens or advances to a
  reachable measured downstream wall the toolkit also covers.
- **Distinct** - the measured wall (or a reachable measured downstream wall) is the LeftOuter/correlated-scalar
  recombine, not toolkit-covered -> name the new substrate class for the next cut.

## Acceptance (honest under all three outcomes)
- **q17 sf=1: green** (measured wall toolkit-covered), **OR** a **named wall** (332, measured producer + bytes) that the
  toolkit doesn't cover, **with the distinct mechanism named.**
- **The wall-boundary table** with the three provenance tiers; **structural-only never justifies green** and never
  carries measured bytes (the 344/345 rule).
- **The discriminator verdict recorded** (incremental vs distinct) with per-boundary toolkit-coverage evidence - the
  input to the post-q17 continue-vs-pivot reassessment.
- **Correctness:** the correlated-scalar AVG-per-part is exact; q17 `pass@compare` at `sf=0.02` (the fixed q17 oracle
  scale; the `7.0`/`0.2` literal-typing harness note, ADR 281); default correctness matrix remains 22/22.
- **No silent wall; green cells + q07/q14/q19/q21 unchanged; bounded; no cost-model change; no TPC-H query-number
  identifiers in `crates/toron/src`.**

## Guardrails
- **Strict three-tier provenance** - measured-first / reachable-measured-downstream / structural-only-predicted;
  structural-only sizes the next mechanism, never a green-target, never measured bytes.
- **Scalar correctness** - a streamed/spilled AVG-per-part must not drop or mis-key the per-part average (the q17
  oracle is the gate).
- **LeftOuter semantics** - if the recombine streams, preserved-side + null-fill stay correct (no semantic flip).
- **No silent wall / green cells unchanged / bounded** - the standard arc guards.
- **Shape-named core** - implementation helpers in `crates/toron/src` are named by operator shape
  (correlated-scalar / grouped fold / LeftOuter recombine), not `q17` or TPC-H. Query-number naming belongs only in
  `toron-tpch` and plan/results docs.

## Risks
- **Structural-as-measured (the LeftOuter trap)** - if q17 dies before the recombine, the LeftOuter is structural-only;
  the three-tier table + the green-target rule guard against claiming it measured or green.
- **"It's just q14/q19"** - it is not; q17's global aggregate sits above LeftOuter + correlated-scalar, so 338's direct
  segment does not match - the measured wall must be grounded, not assumed.
- **Verdict honesty** - the discriminator is decided by the **measured** wall + toolkit-coverage, not by the
  structural prediction.

## Forward arc
- Incremental verdict -> q17 green (or a toolkit-covered advance); the remaining q12/q18/q04/q20 are likely incremental;
  continue is cheap.
- Distinct verdict -> the LeftOuter/correlated-scalar recombine is a new substrate class; its named cost is the input
  to continue-vs-logical-completeness-pivot. q02 (the other correlated-scalar) would share the mechanism.

## Why toron-home
Engine cut in `crates/toron` (exec.rs correlated-scalar / LeftOuter recombine + the 343/345/338 reuse); driven via
`toron-tpch` reach at sf=1 plus q17 correctness at its fixed `sf=0.02` oracle scale. No Wendelon / cross-ring surface.

## Council provenance
Operator + Codex + Claude. q17 as the discriminator (incremental toolkit vs distinct correlated-scalar/LeftOuter
class), not just "next green." Operator's tightening: Phase 0's wall-boundary table is **strict three-tier provenance**
(measured first wall / reachable measured downstream / structural-only predicted); a **structural-only LeftOuter byte
cannot justify a green-target** and is never presented as measured (the 344/345 rule). The ADR succeeds either way: q17
green if the measured wall is toolkit-covered, or the next distinct mechanism named if it advances to the LeftOuter
recombine.
