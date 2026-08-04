# ADR 347 - Implementation plan: finish sf=1 q12/q18 (regular-inner streamed-spill build)

Pairs with `plans/adr/0347_finish_sf1_retained_input_q12_q18.md`. A `crates/toron` (exec.rs) cut: stream q12/q18's
**regular-inner** spilled-build input into spill (rows + index), chunk any measured-large probe input, drop each input
batch, and **retrieve build columns on a probe match** for materialization. Reuse 336/345. **Gated by Phase 0's
regular-inner streamed-spill seam.** No cap-bump.

## Phase 0 - the regular-inner streamed-spill seam (go/no-go)
- **Prove the retained terms:** confirm q12/q18's sf=1 wall is the 336 retained-spill guard on **regular inner**
  spilled-join inputs (~1.14/1.42 GB), measured (sf=1 trace). Record build-side input bytes, probe-side input bytes,
  build-map bytes, and total retained footprint separately; do not collapse both inputs into "the build input."
- **Ground the seam (the fork):**
  - Does `SpilledJoinBuild` (rows-in-pages + key index) **retrieve build-row columns on a probe match**
    so a regular inner join can materialize matched rows **without** the in-memory build batch? If yes -> **streamable,
    incremental**.
  - Or does the inner probe/materialization (`materialize_join_selection` / the build-side column access) **depend on
    the retained in-memory build batch**? If yes -> **name it in the results doc**; the cut adds a spill-retrieval
    materialization path (not a bare 345 reuse).
- Record the verdict (streamable vs dependent) before D1.

## D1 - the streamed-spilled regular-inner build (per the seam verdict)
- For the matched spilled-inner-build shape: pull the build input batches one at a time; write/index each into
  `SpilledJoinBuild` (rows + key index); **drop each input batch** (never `concat_or_empty` the full build input).
- Pull the probe side in bounded chunks when Phase 0 shows it contributes materially to the retained footprint; do not
  replace one full retained input with another.
- On a probe match, retrieve the matched build row's columns from the spill page; materialize the inner output rows
  (both sides) in the **preserved observable order**.
- Resident = the 336-budgeted spill state + one build input batch + one probe chunk; an over-budget spill resident is a
  named producer (332), not a silent OOM.
- If Phase 0 said "dependent": implement the spill-retrieval materialization path here (scoped), still no retained full
  input.

## Tests (`toron` lib + `toron-tpch`)
- **No-concat proof:** the regular-inner build input is streamed/indexed/dropped, never `concat_or_empty`'d (the 345
  gate-1 proof, applied to the inner build); if Phase 0 marks the probe side as contributing, a test also proves the
  probe is consumed in chunks rather than concat'd whole.
- **Spill column-retrieval correctness:** a synthetic spilled regular inner join -> the materialized output (both
  sides' columns, matched rows, preserved order) equals the in-memory build result; Float64/Decimal + order parity.
- **Spill-resident budget:** an over-budget streamed-spilled inner build emits the named diagnostic, not a silent OOM.
- **Shape fence + 345/336 reuse:** non-matching shapes unchanged; the 345 existence-build + 336 paths intact.
- sf=0.01 reach/correctness unchanged.

## Phase 3 - re-measure (release, capped, sf=1)
```
scripts/cargo-cgroup build -p toron-tpch --release
# q12, q18 @ sf=1 (4G): green (pass@execute + pass@compare) via the streamed-spilled regular-inner build, OR a named
#   next wall (332: producer + bytes). No cap-bump; the wall is cleared by bounding the input, not raising a budget.
```
- q12/q18 green or the named next wall via 332; update the ADR 330 atlas q12/q18 cells.
- **Green-cell regression:** q01/q03/q05/q06/q10/q13/q16/q22 + q07/q14/q19/q21/q17 stay green, unchanged plans.
- q04/q20 unchanged (ADR 348).

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
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q12 --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q18 --sf 0.01
# If q12/q18 green at sf=1, run scale correctness for each greened query:
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q12 --sf 1
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q18 --sf 1
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix     # 22/22 unchanged
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
#   release/cgroup q12/q18 sf=1 -> green or named next wall; green cells unchanged; no cap-bump
```

## Results doc
`crates/toron/plans/impl/0347_finish_sf1_retained_input_q12_q18_results.md`: the Phase-0 seam verdict (streamable vs
dependent, with the exact retained terms split by build input / probe input / build map); the streamed-spilled
regular-inner build + column retrieval; q12/q18 sf=1 before/after (retained-spill wall -> green or named next wall via
332); the no-concat proof for each contributing side; the column-retrieval correctness result; sf=0.01 q12/q18
pass@compare; sf=1 pass@compare for any greened q12/q18 cell; the green-cell regression check; the ADR 330 atlas
q12/q18 update.

## Risk / blast radius
- **Seam grounded first** - streamable vs retained-batch-dependent; if dependent, the spill-retrieval materialization
  path is named + scoped, not assumed.
- **Column retrieval** - the spilled inner build returns exact matched columns + order (sf=0.01 oracle gate).
- **No cap-bump** - bound the input by streaming; never raise a budget to mask the wall (sf=10-durability).
- **Scope** - q12/q18 only; 345/336 reuse; q04/q20 index is ADR 348; no cost-model change.

## Seal
`impl: adr 347 finish sf1 retained input q12 q18`. Do not commit wip drafts.
