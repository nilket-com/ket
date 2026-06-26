# ADR 349 - Implementation plan: bound the spill-writer resident path (q04/q20)

Pairs with `plans/adr/349_bound_spill_writer_resident_path.md`. A `crates/toron` (exec.rs) cut: convert
the old `execute_hash_join` / `execute_spilled_hash_join` build-everything-then-spill route into a
**grace/partitioned** spill that bounds the **join-local resident path** - concat'd input copy, pre-spill
`build_rows` map, and page index - all per-partition. Completes sf=1 (9/9), with an sf=10-durable join-local spill
path. **No cap-bump, no index-only fix.**

## Phase 0 - measure all resident terms + choose the mechanism
- **Map the exact q04/q20 spill call sites and consumers.** Record whether the wall is reached through
  `execute_hash_join`, `execute_hash_join_into_global_aggregate`, `visit_spilled_hash_join_filtered_chunks`, or another
  old spill entry. The implementation seam must sit before the current `concat_or_empty` + full `build_rows`
  construction for the targeted route.
- **Measure (env-gated, removed before seal)** for q04/q20 at sf=1: upstream child `Vec<RecordBatch>` bytes,
  join-local concat bytes, pre-spill `build_rows: HashMap<Vec<u8>, Vec<u32>>` bytes, page-index bytes, writer peak.
  Confirm which terms are data-proportional and which are in this ADR's join-local scope (the named index wall ~121/72
  MB + the unnamed build-map + concat terms).
- **Consumer/output gate:** record whether each targeted route can consume partition output incrementally. A
  partitioned writer that later re-accumulates all partition outputs into one selection/Vec has merely moved the wall;
  if that happens, the producer must be named via ADR 332 and sf=1 is not complete.
- **Choose:** grace/partitioned spill (bounds all three join-local terms) vs spilled-index-plus-streamed-build (only if
  it also bounds the build map + join-local input). Record what it touches in `execute_hash_join` and the old spill
  helpers.
- **sf=10-durability proof plan:** a synthetic isolated-operator at sf=10-scale build measuring **all in-scope**
  resident terms (must stay bounded, not ~10x), or a bounded-by-design argument (per-partition resident <= a fixed
  fraction). Cover the build map + join-local concat/input term, not just the index. Keep upstream child materialization
  labeled separately if it remains out of scope.

## D1 - grace/partitioned spilled join (bound the join-local resident path)
- Partition both build and probe sides by key hash into P partitions (P chosen so each partition's input + build map +
  index fits the budget; P grows with data so the per-partition resident stays bounded).
- Per partition: build the (bounded) build map, spill/probe, materialize matches; drop the partition before the next.
- Engage before the current full `concat_or_empty` + full `build_rows` path; a helper implemented only inside
  `SpilledJoinBuild::write_inner` is insufficient because the full map/input already exist by then.
- Never hold the full join-local concat input, the full `build_rows` map, or the full page index resident at once.
- Do not accumulate all partition outputs into an unbounded final selection/Vec. Either fold/visit partition output
  through the existing consumer or budget and name the output accumulator as the next wall.
- If the spilled-index alternative is chosen: it must also stream the build input and bound the pre-spill map (an
  index-only change is rejected by the seam check).

## Tests (`toron` lib + `toron-tpch`)
- **Resident-path bounded:** a synthetic spilled join larger than one partition -> the in-scope resident terms
  (join-local input + build map + index) stay within the budget across partitions (assert no full-build resident); the
  named producers stay named.
- **Pre-concat seam:** a regression test proves the partitioned route avoids the old full `concat_or_empty` + full
  `build_rows` construction for the targeted spill entry, not just the final `SpilledJoinBuild` index.
- **Consumer boundary:** a multi-partition fixture proves partition output is folded/visited incrementally, or else the
  output accumulator is budgeted and named. No silent all-partition collection.
- **Correctness:** the partitioned/grace join returns the same matches + order as the single-partition path
  (Float64/Decimal + order parity); q04/q20 `pass@compare` at sf=0.01.
- **sf=10-durability:** the synthetic isolated-operator at sf=10-scale keeps all in-scope resident terms bounded (not
  ~10x) - the durable-vs-deferred proof.
- **No cap-bump / no index-only:** the resident budget is unchanged; a test that the build map + input are bounded
  (not just the index).
- sf=0.01 reach/correctness unchanged.

## Phase 3 - re-measure (release, capped, sf=1) + the durability proof
```
scripts/cargo-cgroup build -p toron-tpch --release
scripts/cargo-cgroup run -p toron-tpch --release -- run --query q04 --mode strict --sf 1
scripts/cargo-cgroup run -p toron-tpch --release -- run --query q20 --mode strict --sf 1
scripts/cargo-cgroup run -p toron-tpch --release -- correctness --query q04 --sf 1
scripts/cargo-cgroup run -p toron-tpch --release -- correctness --query q20 --sf 1
# plus the synthetic isolated-operator at sf=10-scale build: all in-scope resident terms bounded (the durability proof)
```
- q04/q20 green at sf=1 -> **sf=1 complete (9/9)**; the sf=10-durability proof recorded (all in-scope resident terms
  bounded). Update the ADR 330 atlas q04/q20 cells.
- **Green-cell regression:** q01/q03/q05/q06/q10/q13/q16/q22 + q07/q12/q14/q17/q18/q19/q21 stay green, unchanged plans.

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
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q04 --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q20 --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix     # 22/22 unchanged
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
#   release/cgroup q04/q20 sf=1 -> green; sf=10-scale synthetic -> all in-scope resident terms bounded; budget unchanged
```

## Results doc
`crates/toron/plans/impl/349_bound_spill_writer_resident_path_results.md`: the Phase-0 resident-term breakdown (input /
upstream child batches / join-local concat / build map / index, with provenance for which terms are in 349 scope); the
grace/partitioned (or bounded) mechanism; q04/q20 sf=1 before/after (green via the bounded path); the sf=10-durability
proof (all in-scope terms bounded at sf=10-scale, not ~10x); the correctness result; sf=0.01 q04/q20 pass@compare;
confirmation the budget is unchanged + no index-only fix; the green-cell regression; the ADR 330 atlas q04/q20 update;
**sf=1 complete (9/9)**.

## Risk / blast radius
- **Whole join-local resident path** - bound concat/input + build map + index, not just the named index (the Codex
  catch); an index-only fix is rejected.
- **Seam placement** - the route must engage before full concat/full build-map construction. A `write_inner`-only
  patch is not durable.
- **Consumer boundary** - no unbounded all-partition output accumulator.
- **sf=10-durability proven** - synthetic-at-sf=10 covering all in-scope terms, not structural.
- **Correctness** - partitioned/grace join returns the same matches + order (sf=0.01 oracle gate).
- **No cap-bump** - budget unchanged.
- **Scope** - q04/q20's spilled-join path -> grace/partitioned; the general substrate for sf=10 spilled builds; no
  cost-model change.

## Seal
`impl: adr 349 bound spill writer resident path`. Do not commit wip drafts.
