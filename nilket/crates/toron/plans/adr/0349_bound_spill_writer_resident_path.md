# ADR 349 - bound the spill-writer resident path (q04/q20) - the sf=1 closer, sf=10-durable

## Status
Proposed.

## Context

The last sf=1 memory-family cut: **q04/q20 - bound the spill-writer resident *path*, not raise the cap.** This
**completes sf=1 (9/9 memory cells green)** and is the **first true data-proportional spill cut** - the sf=10-durability
discipline's load-bearing test. The honesty gate (your point #1): a cap-alignment, *or an index-only fix*, would green
sf=1 but re-wall at sf=10.

## Grounded (336 + the code) - the resident path has MULTIPLE data-proportional terms (Codex)
q04/q20's **named** wall is the spill-writer resident check (`index_bytes + writer_peak + page` vs raw 64 MB; q04
~121 MB, q20 ~72 MB). But that index is **not the only** data-proportional term on the path. The old spill route in
`execute_hash_join` / `execute_spilled_hash_join`:
1. already has materialized child batches from `execute_bound`;
2. creates **join-local concat batches** (`left_batch` / `right_batch`) before entering `execute_spilled_hash_join`;
3. builds a **full `build_rows: HashMap<Vec<u8>, Vec<u32>>`** by iterating all build rows **before**
   `SpilledJoinBuild::write` (334's trace: q18's pre-spill map reached 1.5M keys before write_inner);
4. then the spill writer's **page index** (`HashMap<Vec<u8>, Vec<usize>>`; `join_spill_index_bytes`
   data-proportional) - the named wall.
The join-local concat copy, pre-spill build map, and page index all grow with the build. So **bounding only the page
index green-lights sf=1 while leaving the pre-spill build-map (and join-local concat) crack at sf=10.** The
cap-alignment trap (resident check -> 16x) defers the same way.

Important provenance: the upstream child `Vec<RecordBatch>` materialization is a separate executor-model term. 349's
durability claim is about the **join-local spill resident path** unless Phase 0 proves the implementation also streams
the child boundary. Do not blur those terms in the results: "input bounded" must say whether it means the join-local
concat copy or the upstream child batches.

## Decision direction - bound the join-local resident path (grace/partitioned preferred)
The durable fix keeps every **join-local** resident term bounded as the build grows - concat/input copy, pre-spill
build map, and page index.
- **Grace / partitioned hash spill (preferred, Codex):** partition **both** sides by key hash; process
  partition-by-partition; only the active partition's join-local input + build map + index is resident (all bounded;
  more partitions at scale). This must engage **before** the current full `concat_or_empty` + full `build_rows` path,
  or it has not bounded the input/build-map terms. Bounds **all join-local spill terms** - the standard sf=10-durable
  spill substrate.
- **Spilled index (acceptable only if it also bounds the build map + join-local input):** an on-disk index alone is **not
  sufficient** - it leaves the pre-spill `build_rows` map and concat'd input resident. Reject unless paired with a
  mechanism that also streams those.

## Phase 0
1. **Map the exact q04/q20 call sites and consumer boundaries.** The implementation seam must be before the current
   `concat_or_empty` + full `build_rows` construction in the old spill path (or must explicitly classify the remaining
   concat/build-map term as a named next wall). Also record whether the downstream consumer folds/filters partition
   output incrementally or would re-accumulate all partition outputs.
2. **Measure all resident terms** for q04/q20: upstream child batches, join-local concat bytes, pre-spill
   `build_rows` map bytes, page-index bytes, writer peak. Confirm which terms are data-proportional and which are in
   349 scope (the named index wall + the join-local build-map/input terms).
3. **Choose the mechanism:** grace/partitioned (bounds all join-local terms) vs spilled-index-plus-streamed-build (only
   if it also bounds the build map + join-local input). Record what it touches in `execute_hash_join` and the old
   `execute_spilled_hash_join` family.
4. **sf=10-durability proof plan:** prove **every in-scope resident term** stays bounded as data grows - a **synthetic
   isolated-operator at sf=10-scale build** (measure all terms; they must stay bounded, not ~10x), or a
   bounded-by-design argument (per-partition resident <= a fixed fraction). Structural-only is not enough (344/345/346
   rule). The proof must cover the build map + join-local input, not just the index.

## Acceptance (sf=1 closer + sf=10-durable for the whole path, no cap-bump)
- **q04/q20 sf=1: green** (`pass@execute` + `pass@compare`) via the bounded resident path, under the **unchanged**
  budget - **completes sf=1 (9/9).**
- **sf=10-durability proven for all in-scope resident terms** (join-local input + build map + index bounded as the
  build grows; synthetic at sf=10 or bounded-by-design) - **not** a cap-alignment and **not** an index-only fix that
  leaves the build-map crack. Any upstream child-materialization term remains separately labeled, not hidden inside the
  join-local proof.
- **No cap-bump / no cap-alignment**; no index-only fix presented as durable.
- **Correctness:** the partitioned/grace (or bounded) join returns the same matches; q04/q20 `pass@compare` at
  sf=0.01; full `correctness-matrix` 22/22.
- **Green cells unchanged** (q07/q12/q14/q17/q18/q19/q21 + always-green); de-branded; bounded; no cost-model change.

## Guardrails
- **Bound the path, not the cap** - every in-scope data-proportional resident term (join-local input, build map, index)
  stays bounded as data grows; a fix that only bounds the named index (or raises/aligns the cap) is rejected (the
  no-deferred-crack discipline).
- **sf=10-durability proven, not asserted** - synthetic-at-sf=10 covering all in-scope terms, or bounded-by-design.
- **Correctness** - grace/partitioned (or bounded) join returns the same matches (sf=0.01 oracle gate).
- **No silent wall** - residuals named (332); if partitioned spill removes the resident-path wall but exposes an
  output/consumer accumulator, that producer and byte count are recorded instead of calling the path complete;
  de-branded.

## Why this is the sf=1 closer + the sf=10 substrate down-payment
q04/q20 is the only remaining sf=1 memory cell (-> 9/9), and grace/partitioned spill is the **general sf=10 substrate**
for every spilled build (its resident is bounded by partitioning, not by data size). Doing it durably here pays down
sf=10 broadly; doing it index-only or cap-aligned would finish sf=1 while leaving the data-proportional crack the
operator explicitly wants to avoid. The cut is larger than a local index tweak precisely because the durable target is
the join-local resident path, not the final index alone.

## Forward arc
349 completes sf=1 (9/9, durably, join-local resident path bounded) -> **step 2: the perf/regression rail** -> step 3: the
sf=10 atlas (where the grace/partitioned substrate is exercised broadly) -> step 4: Rune-dataframe TPC-H. Each a
Toron-side ADR; no central roadmap.

## Why toron-home
Engine cut in `crates/toron` (exec.rs old spill route before/through `execute_spilled_hash_join` -> grace/partitioned,
bounding join-local concat/input + build map + index); driven via `toron-tpch` reach + correctness at sf=1 / sf=0.01.
No Wendelon / cross-ring surface.

## Council provenance
Operator + Codex + Claude. q04/q20 the sf=1 closer; reject cap-alignment. Codex's tightening folded: the target is the
**spill-writer resident path** (join-local concat input + pre-spill `build_rows` map + page index - all
data-proportional), not only the final index; an index-only fix greens sf=1 but leaves the pre-spill build-map crack at
sf=10. **Grace/partitioned spill is preferred** (bounds all three join-local terms); a spilled index is acceptable only
if it also bounds the build map + join-local input. The sf=10-durability proof must cover every in-scope resident term,
not just the index.
