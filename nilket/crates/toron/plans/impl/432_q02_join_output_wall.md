# ADR 432 - Implementation plan: q02 join-output wall — attribute, then move

Pairs with `plans/adr/432_q02_join_output_wall.md`. **Diagnosis-first**: capture and attribute the
1.738 GB sf=10 join-output wall before any streaming code, then land the shape-fenced wall-mover that
Phase 0 selects. Candidate board move 21/22 → 22/22; the named wall or a persisted sf=10
`pass@compare` decides the claim. **No one-cut green commitment.**

## Phase 0 - capture, attribute, conditional probe (removed before seal)

1. **Capture** the exact bound q02 plan and matching fingerprint from the failing **persisted sf=10**
   invocation (ADR 417 artifact discipline). Retain the plan + fingerprint as evidence under the ADR
   432 evidence directory (mirroring ADR 430's evidence/scratch split).
2. **Attribute** the 1.738 GB via an **env-gated sf=10 diagnostic** (e.g.
   `TORON_Q02_JOIN_OUTPUT_ATTRIBUTION_PATH`): at the `materialize_join_selection` boundary, when the
   requested bytes exceed the cap, persist a one-run receipt naming the producing join
   identity/kind, parent path, inputs (left/right batch shapes), selection rows, and byte
   decomposition. This is a **removed-before-seal diagnostic**, not a permanent enrichment of the
   generic join error.
3. **Conditional probe (sf=10).** *Only if* the receipt shows the producing join's
   `ps_supplycost = MIN(...)` filter is adjacent/reachable, add an env-gated chunk probe measuring
   raw-versus-post-filter survivor bytes at **sf=10** (sf=1 is calibration only). If an earlier join
   owns the wall, do not run the probe — post-`MIN` selectivity cannot select the mechanism.
4. Record the Phase-0 result (plan, attribution receipt, survivor selectivity) before writing any
   wall-mover code. The named producer selects mechanism (a), (b), or (c).

## Phase 1 - the shape-fenced wall-mover (Phase-0-selected)

**Conditional lean — (a) terminal-sort-input recombine+filter**, taken *only if* Phase 0 confirms the
terminal correlated-scalar `Sort <- [Project] <- Filter <- LeftOuter` shape:

1. Recognize the terminal-`Sort` consumer variant of the correlated-scalar recombine and route it
   through `try_streaming_correlated_scalar_left_outer_global_aggregate`'s primitive
   (`streaming.rs:3873`) extended to feed a Sort input rather than a global aggregate: chunk-recombine
   the LeftOuter output, apply `ps_supplycost = MIN(...)` per chunk, accumulate **only projected
   survivors** (ADR 340 accumulator), then hand the survivor stream to the existing Sort.
2. **Order:** the survivor stream carries selection-order parity within chunks; final order is
   established by the existing Sort with the **exact q02 comparator** (`s_acctbal DESC, n_name,
   s_name, p_partkey`) and **all MIN-cost ties preserved**.
3. **Budget:** the survivor/sort-input accumulator is budget-accounted; if survivors overflow the
   Sort, emit a named `memory_budget_exhausted@execute` (a distinct sort-materialization wall) — no
   silent unaccounted wall.
4. Shape-fence the recognition so only the q02-class terminal correlated-scalar shape is affected;
   unrelated plans keep their current path.

**If Phase 0 selects (b)** — survivors exceed the Sort budget → spill the survivor/sort-input stream
or externalize the Sort, named honestly as new (not `SpilledInnerJoinBuild`, which spills build
ownership). **If (c)** — an earlier join owns the wall → stream/spill that named boundary; q02's
terminal shape is not the mechanism. Either path still targets the named wall and records the next.

## Tests

- Phase-0 attribution receipt: an env-gated unit/integration check that the sf=10 diagnostic names
  the producing join and byte decomposition (kept as a named regression per the verify-first-probe
  doctrine if it proves a load-bearing plan claim; the ad-hoc probe is removed).
- Shape-recognition: a plan-level test that the q02 terminal correlated-scalar shape routes to the
  streaming consumer and an off-shape plan does not (a discriminating test that would fail under the
  rejected non-fenced alternative).
- Order + ties: a fixture with MIN-cost ties confirms all tied suppliers survive and the final order
  matches the exact q02 comparator.
- Budget: a survivor set exceeding the Sort budget emits the named `memory_budget_exhausted@execute`,
  not a silent wall.
- Correctness: q02 small-scale `pass@compare` throughout; persisted sf=10 `pass@compare` whenever
  execution reaches output.
- Existing streaming, join-spill, correlated-scalar, architecture, Toron, and TPC-H suites remain
  green.

## Phase 2 - live proof and board decision

Build the release harness; run q02 sf=10 persisted strict under the unchanged 4 GiB / 600-second
envelope from fresh `target/tpch/join_spill` scratch:

- `perf-run-one` must remove the 1.738 GB materialization (attribution receipt shows the wall moved)
  and either complete or name the next wall with producer + bytes;
- `correctness` must independently reach `pass@compare` at sf=10 if execution reaches output.

Move the board to 22/22 only when both the bounded performance run and the persisted correctness run
are green. Otherwise keep 21/22 and name the residual (e.g. a sort-materialization wall) from the live
evidence. Remove the Phase-0 diagnostic and probe before seal.

## Verification

```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast
git diff --check
git ls-files '*.perf.data'
```

## Results document

`plans/impl/432_q02_join_output_wall_results.md` records the Phase-0 captured plan + fingerprint, the
sf=10 attribution receipt (producing join, inputs, selection rows, byte decomposition), the
survivor-selectivity probe, the selected mechanism, the wall-mover outcome (wall removed + next wall
named or sf=10 `pass@compare`), order/ties proof, budget honesty, focused controls, scratch cleanup,
full gates, and board state.

## Seal

Plan dual: `plan: adr 432 q02 join output wall`. Implementation seal:
`impl: adr 432 q02 join output wall`. Do not commit `.perf.data`.
