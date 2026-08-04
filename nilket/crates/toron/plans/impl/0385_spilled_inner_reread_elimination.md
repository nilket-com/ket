# ADR 385 - Implementation plan: streamed spilled-inner re-read elimination (q19)

Pairs with `plans/adr/0385_spilled_inner_reread_elimination.md`. Widen page de-dup scope from output-chunk to work-unit:
a per-work-unit page access plan that loads each unique page once and materializes matches in output order. **Not** a
bigger cache. q19 canary; q03/q07 controls; no cap bump; order+digest parity.

## Phase 0 - map the loop + size the preload (before code)
- Map q19's materialization: the caller of `materialize_streamed_spilled_inner_matches`, the
  `CROSS_JOIN_OUTPUT_CHUNK_ROWS` chunk size, the match-ref structure, the **unique pages per work unit**, the
  **decoded page bytes**, and **why the 8 MiB LRU misses**. `cache_hits=0` is the symptom; Phase 0 must distinguish
  per-page-too-large, decoded-page-too-large, cache-entry insertion mechanics, eviction distance, and access-order
  issues.
- Produce a cache-miss table: plaintext bytes/page, decoded bytes/page, `JOIN_SPILL_CACHE_TARGET_BYTES`, whether
  `insert_cache_entry`/`insert_decoded_cache_entry` accepts the page, eviction distance, and observed hit/miss counts.
- **Size the preload:** unique-pages-per-work-unit x decoded-bytes (q19: 3,908 pages) vs a bounded reservation within
  the existing budget. Decide preload vs page-grouped/windowed fallback.
- **Size output-order restoration:** refs, output ordinals, scatter/take indices, default columns, and output batches
  must stay within the existing materialization cap. If restoring order for the full work unit is too large, shrink the
  work unit/window or name the output wall.
- Controls: q03 (moderate), q07 (unique-once) via the same counter.

## D1 - bounded per-work-unit page access plan
- Collect the work unit's needed refs as `(page_index, build_row, output_ordinal)`.
- **Load/decode each unique page once** (preload into a bounded per-work-unit map, or via the existing decode path with
  the de-dup widened to the work unit), within the budgeted reservation.
- **Materialize in original output order:** scatter each match's build columns to its `output_ordinal` so output order
  + schema are identical to the per-chunk path.
- **Fallback (preload too large):** page-grouped materialization - process one page's matches at a time, scatter to
  output order; if full-work-unit scatter is too large, process bounded ordinal windows and name any residual wall.
- No global LRU cap bump. Any local reservation is charged to the existing memory budget. Topology-named; no qNN.

## Tests / safety net
- **Order + digest parity:** the page-access-plan output digest == the per-chunk path (strict + dev), output order +
  schema identical, on a synthetic spilled-inner case with repeated pages across chunks.
- **Re-read elimination:** with the 384 counter (temporary or kept off-by-default), reads-per-hot-page collapse toward
  unique-once for the work unit (assert max-reads-per-page drops). Remove temporary instrumentation before seal.
- **q07 not regressed:** the unique-once path stays 1x (no new reads / no preload on non-pathological shapes).
- **Budget:** decoded-page preload and output-order restoration bounded; `spill_bytes`/`writer_peak`/resident-guard
  unchanged-or-better; no cap bump.
- **q19/q03 correctness:** strict `pass@compare` sf=0.01/sf=1.

## Verification (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast            # REQUIRED (engine cut)
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q19 --sf 0.01   # strict pass@compare
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q03 --sf 0.01
# re-profile/re-count (384 method) q19 sf=10: reads-per-hot-page collapse (1,273x -> ~1x); q03 benefit; q07 1x;
#   persisted sf=10 correctness if q19 advances/greens; if advances, name the next stack
git diff --check
#   page-access-plan order+digest parity; reads-per-page collapse; q07 not regressed; budget no-regress; no cap bump;
#   --no-fail-fast green; no qNN
```

## Results doc
`crates/toron/plans/impl/0385_spilled_inner_reread_elimination_results.md`:
- Phase-0 loop map + preload sizing (unique pages/work-unit, decoded bytes, why the LRU missed);
- cache-miss table (`insert_cache_entry`, decoded cache, eviction/access-order verdict);
- the page-access-plan (preload or page-grouped/windowed fallback) + order/digest parity;
- decoded-page and output-restoration budget accounting;
- **q19 reads-per-hot-page before/after** (384 counter): 1,273x -> measured; q19 wall before/after;
- q03 benefit (moderate control) + q07 not-regressed (unique-once);
- budget no-regress + no cap bump; if q19 advances, the named next stack;
- confirmation `--no-fail-fast` green, no qNN, no committed perf.data.

## Risk / blast radius
- **Preload too large** -> page-grouped/windowed fallback; no cap bump.
- **Order-restoration buffer too large** -> shrink the work unit/window or name the output wall.
- **Order drift** -> output-ordinal scatter + digest parity.
- **q07 regression** -> unique-once control stays 1x.
- **q19 advances** -> named next stack, honest.
- **Regression** -> `--no-fail-fast`.

## Seal
`impl: adr 385 spilled inner reread elimination`. Do not commit wip drafts or large `.perf.data`.
