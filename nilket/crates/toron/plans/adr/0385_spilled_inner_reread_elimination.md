# ADR 385 - streamed spilled-inner re-read elimination (q19 canary)

## Context

ADR 384 **proved** q19 is a repeated spill-page-read pathology: **4,095,000 physical reads over 3,908 unique pages, max
1,273 reads/page, `cache_hits=0`** (q07 control: 688,500/688,500, unique-once; q03: moderate 99x). Code-grounding
sharpens it: `materialize_streamed_spilled_inner_matches` **already de-dupes pages within one output chunk**, and
`read_batch_page` **already has an 8 MiB LRU decoded cache** (`JOIN_SPILL_CACHE_TARGET_BYTES`, spill.rs:5). So **385 is
NOT "add a cache" or "make it bigger"**. q19 re-reads the same page set across repeated materialization chunks/work
units, and the 384 counter observed `cache_hits=0`. Phase 0 must prove **why** those hits are zero (per-page too large
for the cache, decoded page bytes exceeding cap, eviction between chunks, cache-entry insertion mechanics, or a
different access-order issue) before choosing preload vs page-grouping. The fix **widens page de-dup scope from chunk
to a bounded work unit**. q19 canary; q03 moderate-repeat control; q07 unique-once negative control. **No cap bump.**

## Decision

### Phase 0 - map the materialization loop (before code)
- Map q19's exact loop: the **caller**, `CROSS_JOIN_OUTPUT_CHUNK_ROWS` chunk size, the match batches, the **unique page
  set per candidate work unit**, the **decoded page bytes**, and the cache-miss reason.
- Produce a cache-miss table: page plaintext bytes, decoded bytes, cache cap, whether `insert_cache_entry` succeeds,
  whether `insert_decoded_cache_entry` succeeds, eviction distance, and observed hit/miss counts. `cache_hits=0` is
  evidence of failure, not the reason.
- **Measure whether q19's 3,908-page hot set fits a bounded local preload budget** (unique pages x decoded bytes vs a
  principled reservation within the existing budget). If the full hot set does not fit, size smaller work units.
- **Budget the output-order restoration too:** collecting refs, ordinals, scatter indices, default columns, and any
  materialized output must remain within the existing output/materialization caps. A read-saving plan that creates a
  new output-resident wall is not a green.
- Controls: q03 (moderate-repeat) + q07 (unique-once), same read-count artifact.

### Preferred lever - per-work-unit page access plan
Build a **bounded per-work-unit page access plan**: collect the needed `(page, row, original-output-ordinal)` refs across
the work unit, **load/decode each unique page once**, then **materialize matches in original output order** (scatter each
match to its output ordinal). The work unit is chosen by Phase 0 from the actual caller shape and must fit the decoded
page reservation plus output-order restoration budget. This collapses physical-reads-per-page toward **unique-once per
work unit** without turning the fix into a hidden materialization wall.

- **If the preload is too large** for a bounded reservation: fall back to **page-grouped materialization** (process one
  page's matches at a time) **with scatter/order restoration**. If full-work-unit scatter is too large, use bounded
  ordinal windows and name any remaining wall rather than buffering the whole result.
- **Do NOT raise a global cache cap.** A local reservation is acceptable only if it is charged to the existing memory
  budget and bounded by measured decoded-page/output-restoration bytes. The lever is scope widening, not a bigger LRU.

## Acceptance
- **q19 physical-reads-per-hot-page collapse materially** (re-profile with the 384 per-page counter), ideally toward
  **unique-once for the measured work unit** (1,273x -> ~1x).
- **q19 correctness preserved** (strict `pass@compare` sf=0.01/sf=1; persisted sf=10 if it advances/greens).
- **q03 measured for benefit** (moderate-repeat control - reads-per-page before/after); **q07 not regressed**
  (unique-once stays 1x; no new reads).
- **Output order + digest parity:** the page-access-plan/scatter materializes in **identical output order**; digest ==
  the per-chunk path (strict + dev).
- **Budget semantics unchanged; no cap bump** (the preload is a bounded reservation within budget, or page-grouped
  fallback); decoded-page resident bytes and output-restoration bytes are measured; `spill_bytes`/`writer_peak`/
  resident-guard unchanged-or-better.
- **`--no-fail-fast` required**; de-branded; prior greens unchanged.
- **Re-profile names the next stack** if q19 advances rather than greens (332).

## Guardrails
- **Cache diagnosis before mechanism** - `cache_hits=0` is the symptom; Phase 0 must state why the existing 8 MiB cache
  misses before implementing preload/page-grouping.
- **Scope widening, not a bigger cache** - per-work-unit page de-dup; no global LRU cap bump.
- **Order + digest parity is the correctness gate** - load pages out-of-order, materialize in output order via the
  original ordinal; digest identical.
- **Bounded preload or page-grouped fallback** - decoded pages plus output-order restoration must fit a principled
  reservation; else page-grouped/windowed with scatter. No cap bump.
- **q19 canary; q03/q07 controls; no q07 regression; --no-fail-fast; de-branded; no qNN.**

## Risks
- **Preload too large** -> page-grouped/windowed fallback; never a cap bump.
- **Order restoration too large** -> shrink the work unit/window or name an output-materialization wall; do not trade
  repeated reads for an unbounded scatter buffer.
- **Order drift from out-of-order page loads** -> the output-ordinal scatter + digest parity gate it.
- **q07 regression** -> the unique-once control must stay 1x (no new reads on the non-pathological path).
- **q19 advances not greens** -> honest; re-profile names the next stack (the join/OR-branch or decode floor).
- **Regression** -> `--no-fail-fast`.

## Forward arc
385 (q19 re-read eliminated; q03 measured) -> if q19 advances, the named next stack -> rest of the time cluster (q17/
q21) -> memory-tail (q02/q08/q09/q18/q22) -> parallelism engagement-policy follow-up. 384 proved the waste directly;
385 removes it (scope widening), not distributes (parallelism) or hides it (bigger cache).

## Grounding evidence (file:line)
384: q19 4.095M reads / 3,908 unique / max 1,273, `cache_hits=0`; q07 control 1:1; q03 moderate 99x. within-chunk
de-dup: `materialize_streamed_spilled_inner_matches`. 8 MiB LRU: `JOIN_SPILL_CACHE_TARGET_BYTES` spill.rs:5,
`SpilledInnerJoinBuild::read_batch_page` and cache helpers in `spill.rs`. chunking: `CROSS_JOIN_OUTPUT_CHUNK_ROWS`.
q19 DuckDB=161ms.
