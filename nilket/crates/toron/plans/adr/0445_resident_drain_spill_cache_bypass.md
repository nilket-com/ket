# ADR 445 - resident-drain spill-cache bypass

Disable the join-spill page cache on the admitted resident arm, where the drain is
structurally single-touch and the cache's output is unreachable. Shape-fenced; the
streamed arm is untouched. No speed claim.

## Context

The sf=10 strict board is closed 22/22 (ADR 432). q05 is the worst retained cell —
177,026.577 ms against DuckDB's 654.000 ms, 270.68x
(`impl/0356_article_perf_baseline_post432_results.md:67,100`).

ADRs 443 and 444 are both honest reds. 444's G4 stands: no sub-leaf clears the 0.2386
envelope, and the refined cost is diffuse — the top five sub-leaves are 0.155 / 0.152 /
0.136 / 0.120 / 0.119, summing to 0.68 of root with no hot spot. Leaf-taxonomy refinement
on this axis is spent.

**This cut is not a q05 mechanism and makes no speed claim.** Every q05 mechanism must
clear a 23.86% materiality band derived from q05's own 0.1193 wall spread (`adr/443:12`),
and nothing currently on the board can. This cut does not inherit that gate because it
asserts no speedup: it removes a code path whose output is structurally unreachable on the
arm it runs on. That distinction is the design, not a loophole — a sub-band delta reported
as a win would smuggle the rejected ruler back in.

### The resident-arm single-touch drain

Scope every claim here to the resident arm. The page cache remains correct and useful in
general.

The arm fence already exists at `exec/join/streaming.rs:15690-15693`:

```rust
let loaded_resident = estimate_parallel_partition_resident(spilled, partitioned_probe, key_plan);
let loaded_cap = join_spill_retained_footprint_memory_cap(memory_cap);
if resident_baseline_bytes.saturating_add(loaded_resident) <= loaded_cap {
    observer.record_branch(true)?;    // RESIDENT — partition fits; drained once
} else {
    observer.record_branch(false)?;   // STREAMED — re-reads pages per match
}
```

**Resident arm** (`:15693-15736`): calls `read_all_batch_pages_profiled` once (`:15703`),
scanning `for page_index in 0..self.pages.len()` (`spill.rs:1964`) — each page read
exactly once. The resulting `Vec<RecordBatch>` is **moved** into `LoadedPartitionWork`
(`:15711`), and `run_loaded_partition_work(work, membership_profile,
grouped_chain_observer)` (`:15730`) **does not receive `spilled`**. The resident consumer
cannot re-read a page because it does not hold the build — enforced by the type signature,
not by convention. Its index rebuild runs from the in-memory pages (`:2505`), not disk.

**Streamed arm** (`:15737+`): `rebuild_index_from_pages_profiled` (`:15748`), then
`load_streamed_inner_match_pages` (`:2209`) re-reads pages per match set. This is where the
cache earns its keep and is not touched here.

Build ownership does not cross arms: `partitioned_builds` (`:15680`) are created per group
and consumed within it, so a build disabled on the resident arm is never handed to a
streamed arm.

Measurement corroborates the structure rather than standing in for it. The 444 cohort took
the resident arm 64 times and streamed 0 times, recorded `physical_read_calls = 3,683,390 =
unique_ciphertext_pages` exactly with **0 cache hits** (`impl/444_..._results.md:69-77`),
and logged `read_index_rebuild_cpu` at exactly 64 calls in all five children — matching
resident partitions one-for-one, pinning every rebuild to the in-memory resident site.

### What the dead work costs on that arm

`read_cache_lru` is 21.78 s = 11.9% of root. The cost is algorithmic (`spill.rs:2488`):

```rust
pub(in crate::exec) fn touch_cache_entry(&mut self, page_index: usize) {
    self.lru.retain(|cached| *cached != page_index);   // O(n) scan of the whole LRU
    self.lru.push_back(page_index);
}
```

`lru` is a `VecDeque<usize>`; `insert_cache_entry` calls it on every insert (`:2510`), as
does `insert_decoded_cache_entry` (`:2558`).

The arithmetic closes independently. `JOIN_SPILL_CACHE_TARGET_BYTES = 8 MiB` (`spill.rs:6`);
mean page = 5,270,315,856 / 3,683,390 = 1,431 bytes; steady-state LRU ≈ 5,863 entries.
3,683,390 inserts x 5,863 ≈ 2.16e10 comparisons ≈ 21.6 s at ~1 ns/element, against 21.78 s
measured — within 1%. A second instrument agrees: ADR 443's perf child put
`SpilledInnerJoinBuild::touch_cache_entry` at 12.46% (`impl/443_..._results.md:230-240`)
against the ledger's 11.9%.

## Decision — disable the page cache on the admitted resident arm, reusing the existing cap-zero guard

On the resident arm only, the partition's build has its page cache disabled before the drain
scan. **No new machinery**: the suppression path exists and is already exercised.

- `insert_cache_entry` early-returns on `cache_cap == 0` (`spill.rs:2495`)
- `insert_decoded_cache_entry` early-returns on `cache_cap == 0` (`spill.rs:2528`)

With nothing inserted, the plaintext lookup in `read_page_profiled` misses on an empty map,
`touch_cache_entry` is never reached (its only callers are the insert paths and the hit
path), and `evict_join_spill_cache` no-ops because `cache_bytes` stays 0. The O(n) retain,
the eviction bookkeeping, the `Vec<u8>` insert traffic, and the `record_batch_memory_bytes`
walk fall away together.

All three `insert_cache_entry` call sites (`spill.rs:774`, `:2474`, `:3323`) are read-path;
the cache is never populated during the write phase, so the bypass cannot alter write-side
behavior.

**Rejected — generic O(1) LRU rewrite.** Correct in the abstract; named-deferred, not
adopted. It changes behavior on the streamed arm, where the cache is live and where no
defect is evidenced. Bundling it would place an unmeasured hit-path change inside a cut
justified purely by dead-work removal.

**Rejected — delete the cache.** The streamed arm needs it. The fence was wrong, not the
cache.

## Acceptance

**The accepted result is a structural cache bypass on the resident-arm single-touch drain
plus preservation of the streamed path — not a material speedup.**

- Strict q05 sf=10 `pass@compare`: unchanged status, plan fingerprint, digest, schema, row
  count, and spill tuple against the pre-cut tree.
- 4 GiB / no-swap survival, scratch empty after the scope.
- The bypass is reached only on the admitted resident arm; streamed-arm cache
  insert/hit/evict behavior is byte-for-byte unchanged.
- A discriminating streamed-arm test proves the cache still populates and still hits — it
  must fail if the bypass is applied unconditionally.
- A resident-arm test proves zero cache inserts, zero `touch_cache_entry` calls, and one
  physical read per page across the drain.
- No materiality claim and no band rewrite. The 23.86% band and 444's G4 red stand as
  written.
- The before/after wall is corroboration only — evidence the removed work was real. It is
  not a gate and not a win. **A null wall result does not invalidate the cut**, because the
  justification is structural. The receipt is unprofiled, with scope order randomized or
  interleaved and recorded.

## Guardrails and risks

- **The fence is the whole cut.** An architecture guard asserts the bypass occurs on the
  resident arm and that the streamed arm retains an enabled cache — ordering and
  arm-identity, not co-presence, in the shape of 439's fence guard (`adr/439:44-46`).
- **Fence drift** (439's named risk, `:90`): the resident test is a memory-fit estimate. If
  a later cut moves partitions across that fence, the bypass follows the arm, not the query.
  That is the intent; the guard pins it.
- **The 8 MiB cap is not the bug and is not retuned here.** Raising it lengthens the LRU
  scan rather than shortening it. Cap tuning is budget work, out of scope.
- **Scope the language.** Say "resident-arm single-touch drain". The cache can and does hit
  on the streamed arm.
- **Order randomization stays.** Any prospective write-path measurement on the 444 substrate
  must randomize or interleave scope order and record it (`impl/444_..._results.md:63-67`) —
  retained as cheap insurance, downgraded from "drift established": ADRs 443 and 441 do not
  reproduce the ordering, and the pattern was identified post-hoc.
- **444's instrument is untouched.** Schema 2 remains the diagnosis substrate; this cut
  neither widens nor reinterprets it.

## Forward arc

**ADR 446 candidate — spill geometry.** Toron spills 5,353,985,328 bytes where DuckDB spills
0. The only axis with a 270x ceiling; 443's root boundary includes it while 444's sub-leaf
taxonomy cannot see it. Needs the disk-envelope authorization ADR 442 declined to touch
(`adr/442:12-14`).

**Characterize the q05 wall.** The write-variance claim does not replicate across cohorts
(443 spread 0.2882 → 444 spread 0.0869, same query and harness), and 443's five write values
read as two clusters (70.1/70.8/75.1 against 85.3/91.7) rather than one noisy distribution.
If the wall is bimodal, the 23.86% band is mis-specified and the 441-444 arc is mis-gated.
n=5 cannot settle it. This would also define "bimodality", named in two forward arcs
(`adr/443:132-133`, `adr/444:163`) with no defining paragraph in the corpus.

## Deferred (named)

- Generic O(1) LRU rewrite — contingent on streamed-arm evidence of a defect.
- Spill-cache cap retune — budget work, not this cut.
- q17 remeasurement — parallel, temporally isolated chore.
- q18 strict one-pass and bimodality characterization; q21 residual; q19 re-diagnosis +
  438-profiler widening; q13 fresh diagnosis; Cut A (439 mirror).
