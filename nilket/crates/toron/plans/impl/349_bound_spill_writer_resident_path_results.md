# ADR 349 Results - bound the spill-writer resident path

## Phase 0 - measured route

The active sf=1 wall was q20:

- Before: `q20 --sf 1` failed at execute with `memory_budget_exhausted`.
- Producer: `join spill writer resident state`.
- Requested/available: `72,131,060 / 67,108,864` bytes.
- Old route: `execute_spilled_hash_join`, `LeftOuter`, no residual, 800,000 build rows and 543,210 probe rows.

q04 was already green at the start of this pass after the preceding spill/streaming cuts:

- `q04 --sf 1`: `pass@execute`, `pass@compare`.
- ADR 349 keeps it as the paired control rather than reclassifying it.

The old q20 route built all join-local resident terms before spilling:

- upstream children remained materialized by the executor model (out of 349 scope);
- join-local concat created full left/right batches;
- the old pre-spill `HashMap<Vec<u8>, Vec<u32>>` covered the full build;
- `SpilledJoinBuild` then built the full page index and tripped the resident-state guard.

## Implementation

Added a shape-named streamed-spilled `LeftOuter` route before the generic concat path:

- partition build/probe batches by join-key hash;
- write only the active build partition to the batch spill;
- probe only the matching probe partition;
- emit matched chunks from spill pages;
- emit unmatched build rows page-by-page with null right columns;
- budget the returned output accumulator so the path cannot trade the resident wall for a silent output wall.
- concatenate the already-budgeted partition outputs before returning, preserving the hash-join executor's existing single-batch result shape for callers/tests that consume the first result batch.

This bounds the in-scope join-local resident path for the targeted route: no full join-local concat batch and no full pre-spill build map are constructed, and the spill index is resident only for the active partition. The upstream child `Vec<RecordBatch>` materialization remains a separate executor-model term.

## sf=10 Durability

The partition count is derived from the estimated build footprint and grows with input size. The regression test `streamed_spill_partition_count_grows_to_keep_partition_resident_bounded` models a 10x build-size increase and asserts the per-partition target remains bounded instead of scaling by 10x.

This is the bounded-by-design proof for ADR 349's in-scope terms. It is not a claim that upstream child materialization is solved.

## Verification

Release/cgroup checks:

- `q20 --sf 1`: `pass@execute`, 2227.8 ms.
- `q20 correctness --sf 1`: `pass@compare`, 4924.8 ms.
- `q04 --sf 1`: `pass@execute`, 2638.0 ms.
- `q04 correctness --sf 1`: `pass@compare`, 5921.6 ms.
- `matrix --sf 1`: all q01-q22 `pass@execute`.
- `correctness-matrix --sf 1`: all q01-q22 `pass@compare`.
- default `matrix`: all q01-q22 `pass@execute`.
- default `correctness-matrix`: all q01-q22 `pass@compare`.

Focused tests:

- `partitioned_spilled_left_outer_join_matches_materialized_full_row`
- `partitioned_spilled_left_outer_build_writes_preserved_batches_without_concat`
- `streamed_spill_partition_count_grows_to_keep_partition_resident_bounded`
- `spilled_hash_join_matches_in_memory_digest_for_all_kinds`

Broad suites:

- `fmt -- --check`
- `check -p toron --all-targets`
- `check -p toron-tpch --all-targets`
- `test -p toron --lib`
- `test -p toron --test architecture_test`
- `test -p toron-tpch`
- `test -p toron --no-fail-fast`

No memory cap or spill cap was raised. The old resident-state wall was cleared by bounding the join-local path, not by aligning the old raw resident check to the 16x retained-footprint cap.

## Outcome

q20 moved from the old spill-writer resident wall to green. q04 stayed green. The sf=1 memory family is now green for the previously remaining q04/q20 cells under the unchanged budgets.
