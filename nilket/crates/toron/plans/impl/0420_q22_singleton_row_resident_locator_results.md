# ADR 420 results - q22 singleton-row resident locator

## Verdict

ADR 420 makes q22 green at sf=10 without changing the join-spill page format, spill volume,
resident accounting, budget, or guard. The all-singleton Int64 build now stores build-row
ordinals in the existing compact locator values. A temporary real-shape profile recorded exactly
zero probe-side page reads, cache accesses, decryptions, and decoded bytes. After all profiling
hooks were removed and the release binary rebuilt, q22 completed reach in 3,282.0 ms and strict
`pass@compare` in 10,047.2 ms.

The board moves from **15/22 to 16/22 green**. This closes q22's page-probe amplification wall at
sf=10. The O(unique keys) resident-locator asymptote remains unchanged and is not hidden by this
result.

Artifacts are under `target/tpch/adr420/`.

## Phase 0 - current-HEAD census and baseline

The temporary pre-cut release profile re-proved the full eligibility seam before production code
changed:

| fact | observed |
| --- | ---: |
| key plan | Int64 |
| unique keys | 1,500,000 |
| singleton keys | 1,500,000 |
| multi-row keys | 0 |
| maximum build-row ordinal | 1,499,999 |
| compact locator bytes | 50,331,648 |
| writer peak | 65,524 |
| page reserve | 65,536 |
| spill pages | 481 |
| spill bytes | 31,509,620 |
| cache cap | 8,388,608 |

The stop-line did not fire: the real q22 writer is exactly all-singleton. The three production
writers still derive every stored u32 from `0..left_batch.num_rows()` and retain that same batch
by reference through their probe. The returned u32 continues through
`append_join_matches_for_kind`, including residual evaluation and build-match bitmap completion.
No detached-row writer exists.

The behavior-neutral baseline reached the registered 600-second boundary with
`Result=timeout` / `ExecMainStatus=15`. Its last completed ledger row recorded:

```text
read_page_calls=13,600,000
cache_hits=3,625,115       (26.655%)
cache_misses=9,974,885     (73.345%)
decrypted_bytes=653,338,337,780 (608.469 GiB)
```

This is direct evidence for the cut: q22 had still not completed all 15M probes after decrypting
more than 608 GiB. A page-local search alone would not remove the dominant miss/decrypt term.
The temporary baseline instrumentation was removed before the production edit.

## Implementation

`JoinSpillPageLocatorStorage` now expresses value meaning as variants:

```text
Encoded
Int32Pages / Int64Pages
Int32Rows  / Int64Rows
```

Selection happens once, before locator construction. Primitive plans whose build-map values are
all length one select Rows; any multi-row primitive build selects Pages for the whole locator;
Generic, composite, and null-safe plans select Encoded. There is no runtime mode bool and no
per-entry tag.

The compact entry remains `{ key, u32 value }`: Int32 is exactly 8 bytes and Int64 exactly 16
bytes. Bucket and entry capacity accounting is unchanged. At each existing page flush, Rows
variants insert `rows[0]`; Pages variants insert the page ordinal. Pages are still written with
the same entries, encryption, assignment, and order, so spill telemetry and cleanup semantics are
unchanged.

`SpilledJoinBuild::lookup` now returns `NoMatch | OneRow(u32) | Rows(Vec<u32>)`. Its
`IntoIterator` uses a concrete iterator enum (`NoMatch`, `option::IntoIter`, or
`vec::IntoIter`), not `Box<dyn Iterator>`. The row path therefore allocates neither a singleton
vector nor an iterator object. All three production probe loops continue to consume
`matches.into_iter()`.

## q22 mechanism proof and final outcome

The temporary post-cut real-q22 profile recorded:

```text
locator_mode=int64_rows
index_bytes=50,331,648
writer_peak=65,524
page_reserve=65,536
pages=481
spill_bytes=31,509,620
read_page_calls=0
cache_hits=0
cache_misses=0
decrypted_bytes=0
```

That instrumented run reached output in 3,341.0 ms. Every temporary profile symbol, environment
gate, and log was then removed, and the release harness was rebuilt from the final source.

The hook-free result is:

| proof | result |
| --- | --- |
| persisted reach | pass, 7 rows, execute 3,282.0 ms |
| strict correctness | `pass@compare`, 10,047.2 ms |
| plan fingerprint | `ffd45ab430a864ab` (unchanged) |
| output digest | `2e036f87904c9049bab31137c4a2bbf16c9915c33fbc4cf580c07c540b029ffc` |
| output schema | `fnv1a64:75e687d83a43ecf6` |
| spill bytes | 31,509,620 (unchanged) |
| reach max RSS | 676,552 KiB |
| correctness max RSS | 689,956 KiB |

This is a green result, not a named-wall substitution.

## Discriminating locks

| lock | contract |
| --- | --- |
| `join_spill_singleton_row_mode_eliminates_page_reads` | actual writer selects Int64Rows before insertion, legacy insertion count is zero, present/missing lookup is scalar, multiple pages are still written, and the production probe completes with `page_read_count == 0` |
| `join_spill_compact_locator_matches_legacy_across_pages` | a mixed primitive build selects Int64Pages, performs page reads, preserves duplicate-row order, and matches the encoded twin across page boundaries and an oversized entry |
| `join_spill_singleton_left_anti_residual_matches_encoded` | row mode and encoded page mode have identical page lengths/spill bytes and identical LeftAnti output under a discriminating residual; row mode reads zero pages while encoded mode reads pages |
| `compact_join_spill_page_locator_fences_and_accounts` | Int32/Int64 layouts, row/page variants, equal capacity accounting, concrete scalar/many iterators, direct-reuse negative arithmetic, fallback fences, malformed keys, and page-index truncation |
| retained ADR 419 wall/cleanup locks | the resident guard and producer remain armed; write, guard, malformed-key, and probe cleanup leave scratch empty |

Focused results: join-spill 8/8; spill 50/50; join 205/205; streaming 46/46.

## Do-not-disturb walls

Both excluded drivers reproduce exactly on the hook-free final binary:

| query | producer | requested | available | fingerprint |
| --- | --- | ---: | ---: | --- |
| q21 | `semi/anti existence-set spill resident state` | 1,073,742,536 | 1,073,741,824 | `e2e25953a6a3fefd` |
| q18 | `streamed inner join build spill writer` | 536,899,968 | 536,870,912 | `d8b60e3a03bfaf79` |

q21 remains a duplicate-heavy existence-index representation problem. q18 remains a SpillBytes
quota/volume problem. Neither contract changed.

## Spill-bearing controls

All ten controls pass persisted sf=10 strict correctness and reproduce their exact forensic spill
signatures:

| query | reach execute_ms | spill bytes | correctness elapsed_ms |
| --- | ---: | ---: | ---: |
| q03 | 16,217.2 | 233,338,304 | 23,126.8 |
| q07 | 14,337.0 | 134,732,480 | 32,478.0 |
| q09 | 32,943.9 | 108,992,256 | 43,407.9 |
| q10 | 12,722.2 | 215,703,848 | 22,834.3 |
| q12 | 6,391.8 | 159,510,976 | 18,199.2 |
| q13 | 144,010.4 | 9,152,992 | 145,593.9 |
| q14 | 4,770.8 | 36,464,000 | 12,586.4 |
| q16 | 4,394.5 | 60,078,400 | 10,139.5 |
| q19 | 87,384.1 | 51,471,552 | 109,291.1 |
| q20 | 23,118.2 | 256,104,392 | 32,699.1 |

q20's reach max RSS was 3,358,200 KiB under the unchanged 4 GiB cgroup boundary. Timings are
single observations, not speed claims.

## Verification and hygiene

- `scripts/cargo-cgroup fmt -p toron -- --check` passed.
- `scripts/cargo-cgroup check -p toron --all-targets` passed.
- full Toron library: 1,239 passed, 0 failed.
- full architecture suite: 2,079 passed, 0 failed.
- `scripts/cargo-cgroup test -p toron --no-fail-fast`: every test binary passed with zero
  failures and no skip list.
- `scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast`:
  every test binary passed with zero failures.
- `git diff --check` passed; no tracked `.perf.data`; spill scratch is empty.
- No query/ADR branding, debug output, temporary profile hooks, or boxed iterator remains in
  engine source.

No cap, multiplier, guard term, producer, page format, build-map representation, spill telemetry,
or cleanup contract changed.

## Forward

The locator is still O(unique keys) resident and retains ADR 419's approximate sf=15 capacity
ceiling. Page-write elision for fully row-resident builds is a separate telemetry/cleanup
optimization, not part of this seal. Per-key row/page specialization remains deferred until a
mixed-build driver exists.

q21's measured 15M-key / 59,986,052-ref shape remains the next representation candidate, with
CSR-at-finish, page-local residual filtering, and sharded/deferred indexing as genuinely distinct
contracts. q18, q02, q05, and the q11/q17 correctness-policy threads remain named.
