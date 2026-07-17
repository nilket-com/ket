# ADR 445 results — resident-drain spill-cache bypass

## Disposition

Implemented and verified as a shape-fenced dead-work removal. The bypass is
active only after the grouped-reservoir resident-arm admission; the streamed
arm retains its cache. No speed or materiality claim is made.

## Implementation

`SpilledInnerJoinBuild::disable_page_cache` clears the cap, cache, LRU, and
accounted cache bytes. The grouped-reservoir resident arm calls it immediately
after `record_branch(true)` and before `read_all_batch_pages_profiled`. The
streamed arm and all other cache callers are unchanged.

## Verification

- Release binary rebuilt from the implementation tree.
- Strict q05 sf=10 under `MemoryMax=4G`, `TasksMax=512`, fresh
  `target/tpch/join_spill`: `pass@compare`, `elapsed_ms=213241.7`.
- The 213,241.7 ms single run is 9.0% above ADR 444's 195,629.1 ms reference;
  it is inside the pre-existing 23.86% q05 noise band and is not a regression
  or speed claim. The specified randomized five-child before/after corroboration
  was not produced for this cut.
- A profiler-enabled strict sf=10 q05 pass also succeeded:
  `elapsed_ms=204657.5`.
- Profiler receipt: resident partitions `64`, streamed partitions `0`, pin
  outcomes all zero; `read_index_rebuild_cpu` calls `64`.
- Unique ciphertext pages and physical reads both `3,683,390`; bytes both
  `5,270,315,856`; read amplification `1.0`; plaintext and decoded cache hits
  `0`; misses each `3,683,390`; decodes `3,683,390`.
- `read_cache_lru` fell to `1,298,976` µs, with no cache population or
  eviction. The remaining time is cache/accounting lookup overhead; the
  linear LRU-retain work is absent. The 444 baseline’s corresponding measured
  cost was ~21.78 s.
- Output status, plan fingerprint, schema, rows, and spill accounting remained
  successful; scratch was fresh and cleaned after the run.

## Rails

The streamed cache reuse test remains green, and the resident bypass test
asserts that disabling the cache clears prior state and prevents repopulation.
The architecture guard checks that the setter occurs after the resident-arm
fence and before the resident drain, and is absent from the streamed arm.

No materiality band was rewritten. The result is accepted on the structural
single-touch proof and streamed-path preservation, not on the observed wall
time.

## Gates

Format, diff check, toron library tests (1317/0), architecture suite (2088/0),
toron-tpch tests, Polars-reference tests, and the full toron no-fail-fast suite
completed green.
