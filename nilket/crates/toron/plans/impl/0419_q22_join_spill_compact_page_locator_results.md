# ADR 419 results - q22 join-spill compact page locator

## Verdict

ADR 419 removes q22's `join spill writer resident state` wall under the unchanged 64 MiB task
budget. The legacy page index requested 121,631,060 bytes; the completed compact Int64 locator is
50,331,648 bytes, with the same 65,524-byte writer peak and 65,536-byte page reserve. The guarded
resident total is therefore 50,462,708 bytes, leaving 16,646,156 bytes of task-budget headroom.

q22 does **not** become green in this cut. It advances to a separately named wall:
**join spill page-probe amplification**. The hook-free final sf=10 run remains bounded in memory
but reaches the registered 600-second boundary while the probe repeatedly loads and linearly scans
candidate spill pages. `systemd` records `Result=timeout`, `ExecMainStatus=15`, and a
1,837,846,528-byte memory peak; no Toron memory/spill diagnostic reappears and scratch is clean.

The sf=10 board therefore remains **15/22 green**. This is an honest wall move, not a q22 green
claim. q22's independent strict oracle still passes at sf=0.01 in 761.4 ms.

Artifacts are under `target/tpch/adr419/`.

## Phase 0

### Baseline reproduction

The fresh hook-free baseline reproduced ADR 415 byte-for-byte:

| status | producer | requested | available | execute_ms | fingerprint |
| --- | --- | ---: | ---: | ---: | --- |
| fail | `join spill writer resident state` | 121,631,060 | 67,108,864 | 2,168.897 | `ffd45ab430a864ab` |

The retained decomposition is exact:

```text
1,500,000 unique Int64 keys x (64 + 9 encoded-key bytes + 8 page-ref bytes)
                                                        = 121,500,000
writer peak                                             =      65,524
page reserve                                            =      65,536
total                                                   = 121,631,060
```

The writer emits 481 pages and never splits a build-map entry. Each key therefore has one page
locator. The shared blast radius is the three materialized spilled-hash consumers
(`execute_spilled_hash_join`, its global-aggregate sibling, and its filtered-chunk sibling).
q04/q12/q20/q18 historically reached this writer; current specialized paths moved those cells away,
and q22 is the live board driver. The ten spill-bearing greens remain the behavior net.

### Representation selection

A temporary release-only fixture used the proposed production entry layouts with 1,500,000 keys
and the same 15,000,000-probe present/missing sequence. The temporary candidates and hooks were
removed before the final build.

| candidate | resident bytes | build_us | probe_us | decision |
| --- | ---: | ---: | ---: | --- |
| open addressed (`u32` buckets + scalar page entries) | 50,331,648 | 39,476 | 417,216 | selected |
| sorted vector (`{ i64, u32 }`) | 24,000,000 | 44,616 | 1,752,435 | rejected: 4.20x probe regression |

The sorted vector has more headroom but fails the ADR's lookup-cost fence. Open addressing is still
a 2.41x component reduction from the 121,500,000-byte legacy index and clears sf=10 with material
headroom.

Direct reuse of ADR 414's row-ref entry remains a pinned negative:

```text
4,194,304 u32 buckets + 2,097,152 x 24-byte ref-list entries
  = 16,777,216 + 50,331,648
  = 67,108,864 bytes
```

That consumes the entire task budget before writer/page terms and cannot solve q22.

### q21 sibling classification

The q21 wall remains a crossing, not a completed size. Its retained scan ledger reaches about
31.6M rows when the legacy existence index first crosses 1 GiB. The actual sf=10 Parquet
distribution is:

| lineitems per order | orders |
| ---: | ---: |
| 1 | 2,143,922 |
| 2 | 2,145,125 |
| 3 | 2,141,281 |
| 4 | 2,144,098 |
| 5 | 2,141,972 |
| 6 | 2,143,429 |
| 7 | 2,140,173 |

That is 59,986,052 row refs over exactly 15,000,000 keys. The completed legacy index projects to
2,039,776,832 bytes (`15M x 72 + 59,986,052 x 16`). Reusing ADR 414's compact duplicate-list
shape still projects to 1,342,403,544 bytes: its primitive table/base is 536,870,912 bytes and
duplicate overflow vectors add 805,532,632 bytes. The scan projects to 58,581 input batches and
about 234,324 non-empty four-way exchange page writes.

This confirms q21 needs a different row/page/deferred-index contract. It was correctly excluded
from ADR 419.

q18 remains orthogonal: `BudgetDimension::SpillBytes`, not resident index memory.

## Implementation

`SpilledJoinBuild` now owns a `JoinSpillPageLocator` with three storage variants:

| key plan | locator |
| --- | --- |
| single non-null-safe Int32 | open-addressed primitive key -> u32 page |
| single non-null-safe Int64 | open-addressed primitive key -> u32 page |
| Generic, composite, or null-safe | existing encoded `HashMap<Vec<u8>, Vec<usize>>` fallback |

The primitive locator uses `ahash::RandomState`, `u32` bucket slots, and scalar entries. Int32
entries are 8 bytes; Int64 entries are 16 bytes. Accounting reads bucket and entry **capacities**,
not lengths. The q22 production build observed 4,194,304 bucket slots and 2,097,152 entry slots,
exactly matching the 50,331,648-byte computed footprint.

All three legacy spilled-hash helpers derive `JoinKeyPlan` through the existing
`JoinKeyPlan::from_key_arrays` seam before building the encoded map and pass the plan into the
writer. Primitive keys are decoded only after plan selection through checked tag-and-length
decoders. Wrong tags/lengths fail loudly; eligibility is never inferred from encoded byte length.

The upstream build map, page encoding, encryption, page assignment, build-row order, budget,
writer/page terms, producer string, and guard are unchanged. Primitive lookup returns a scalar page
without allocating a one-element vector; the original encoded key is still passed to page decoding.

## q22 outcome and next wall

The temporary completion profile on the real sf=10 writer recorded:

```text
kind=int64 entries=1500000 entry_capacity=2097152 bucket_capacity=4194304
index_bytes=50331648 writer_peak=65524 page_reserve=65536
pages=481 spill_bytes=31509620
```

The hook was then removed and the release binary rebuilt. The hook-free run independently removed
the resident wall and reached the same bounded CPU path for 600 seconds.

The next wall is byte-bounded by the unchanged page layout. q22 has 1,500,000 one-row build-map
entries. Each encoded page entry is 21 bytes and a page begins with a four-byte count:

```text
480 full pages x (4 + 3,120 x 21) = 31,451,520 plaintext bytes
1 final page  x (4 + 2,400 x 21) =     50,404 plaintext bytes
plaintext total                         = 31,501,924
AES tag bytes (481 x 16)                =      7,696
spill bytes                             = 31,509,620
```

Every one of the 15,000,000 orders probes resolves one page and
`decode_join_spill_page_for_key` linearly scans that full page. Even if every probe hit the smallest
50,404-byte page, the decoder would inspect at least **756,060,000,000 bytes (704.1 GiB)**. Assuming
customer keys are uniformly distributed across the observed pages, the expected scan volume is
about 982,388,482,328 bytes (914.9 GiB).
Cache hits still clone and scan the page, so they do not remove the decode-volume term.

The named follow-on is a page-local mini-index, key-clustered/sorted spill pages, or a deferred/
sharded locator that avoids linear page scans. Changing that probe contract is outside this
resident-locator cut.

## Discriminating locks

| test | contract |
| --- | --- |
| `compact_join_spill_page_locator_fences_and_accounts` | Int32/Int64 entry sizes, direct-reuse negative arithmetic, capacity accounting before/after shrink, Generic/null-safe/composite fences, malformed keys, and u32 truncation |
| `join_spill_compact_locator_matches_legacy_across_pages` | actual writer uses one page per key across multiple pages and an oversized single-entry page; present/missing lookup and duplicate build-row order match legacy |
| `join_spill_compact_locator_clears_legacy_resident_wall` | compact clears a measured low cap where legacy fails; the compact path still fails below its true resident size with the unchanged producer; all error scratch is removed |
| `spilled_hash_join_helper_selects_compact_locator` | the real primitive spilled-hash helper clears a compact-only cap; passing Generic would fail the test |
| `compact_join_spill_malformed_key_cleans_scratch` | checked decoder failures are loud; both a partial write and a post-write probe error leave scratch empty after cleanup |

Focused results: 5 passed, 0 failed.

## Driver controls

Both do-not-disturb walls reproduce exactly on the final binary:

| query | producer | requested | available | fingerprint |
| --- | --- | ---: | ---: | --- |
| q21 | `semi/anti existence-set spill resident state` | 1,073,742,536 | 1,073,741,824 | `e2e25953a6a3fefd` |
| q18 | `streamed inner join build spill writer` | 536,899,968 | 536,870,912 | `d8b60e3a03bfaf79` |

All spill-bearing controls pass persisted sf=10 correctness and retain their exact forensic spill
signatures:

| query | reach execute_ms | spill bytes | correctness elapsed_ms |
| --- | ---: | ---: | ---: |
| q03 | 15,461.5 | 233,338,304 | 25,931.6 |
| q07 | 13,875.4 | 134,732,480 | 37,371.1 |
| q09 | 31,286.9 | 108,992,256 | 49,821.7 |
| q10 | 11,758.6 | 215,703,848 | 25,568.7 |
| q12 | 5,902.1 | 159,510,976 | 20,365.2 |
| q13 | 128,377.2 | 9,152,992 | 177,314.9 |
| q14 | 4,546.9 | 36,464,000 | 13,387.5 |
| q16 | 4,107.3 | 60,078,400 | 10,785.4 |
| q19 | 90,779.2 | 51,471,552 | 116,827.3 |
| q20 | 23,285.4 | 256,104,392 | 32,062.8 |

These are single-session reach/correctness observations, not speed claims.

## Verification and hygiene

- `scripts/cargo-cgroup fmt -p toron -- --check` passed.
- `scripts/cargo-cgroup check -p toron --all-targets` passed.
- full Toron library: 1,237 passed, 0 failed.
- full architecture suite: 2,079 passed, 0 failed.
- `scripts/cargo-cgroup test -p toron --no-fail-fast`: every test binary passed with zero
  failures.
- `scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast`:
  every test binary passed with zero failures.
- `git diff --check` passed; no tracked `.perf.data`; spill scratch is empty.
- No query-number/ADR branding or temporary profile hooks remain in engine source.

No cap, multiplier, guard term, producer, page format, or typed build-map representation changed.

## Forward

q22's resident locator is compact and truthful but remains O(unique keys): the selected layout
re-walls around sf=15 when capacities double. More immediately, q22 exposes the page-probe
amplification contract above. The next q22 cut should make a candidate-page lookup sublinear within
the page; the eventual unbounded-key answer remains a spilled/sharded/deferred index.

q21's measured duplicate-heavy shape is the opening evidence for its separate representation ADR.
q18 retains its spill-volume/quota decision and unreached membership wall. q02 remains the
join-output C-spec driver, q05 remains diagnosis-first, and q11/q17 remain correctness-policy
threads.
