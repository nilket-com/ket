# ADR 434 results - decoded-page pin for projected streamed-spilled LeftOuter grouped shape

## Verdict

ADR 434 is green. The projected streamed-spilled LeftOuter grouped branch now attempts the existing
guarded decoded-page pin once at its parent probe boundary. q13 is admitted by the measured full
retained-footprint guard, and the ADR 433 probe-time eviction/reread loop is gone:

- probe-phase file reads, page decodes, cache misses, and cache evictions are all **zero**;
- total file reads and decodes fall from 4,823,849 to 5,860, exactly one preload per spill page;
- read amplification falls from 822.99x to 1.00x;
- voluntary context switches fall from 9,660,675 to 23,523, a **99.76% reduction**;
- the unprofiled ADR 356-shaped median falls from 134,484.731 ms to **28,288.448 ms**, a
  **4.754x speedup / 78.97% reduction**, with 0.00465 spread;
- strict persisted sf=10 remains `pass@compare` under 4 GiB / no swap.

The board remains 22/22. The next measured q13 wall is the grouped fold: the diagnostic ledger now
records 11,883.068 ms there, 42.85% of the 27,730.537 ms probe phase. The general O(1) LRU rewrite
remains deferred because the pinned probe no longer touches the eviction cache.

## Conditions and provenance

| item | value |
|---|---|
| ADR 433 implementation baseline | `146d1ba` |
| committed ADR 434 plan | `ab77464` |
| measurement date | 2026-07-13 CDT |
| measured release binary SHA-256 | `e19aa7dd3a3fb90f42d7337f56ee85197f82a843612e139bc1d79b61e3bc1cf2` |
| final post-gate release binary SHA-256 | identical: `e19aa7dd3a3fb90f42d7337f56ee85197f82a843612e139bc1d79b61e3bc1cf2` |
| data | persisted strict sf=10 Parquet at `target/tpch/data` |
| throughput scope | `MemoryHigh=infinity`, `MemoryMax=infinity`, `AllowedCPUs=0-1`, `CPUQuota=150%`, `TasksMax=512`, `RuntimeMaxSec=3600` |
| survival scope | `MemoryHigh=4G`, `MemoryMax=4G`, no swap, `TasksMax=512`, `RuntimeMaxSec=600` |
| host | 12th Gen Intel Core i9-12900K; CPUs 0-1 are SMT siblings on P-core 0; 61 GiB RAM; no swap |
| evidence root | `target/tpch/adr434/` |
| join-spill scratch | `target/tpch/join_spill`; empty before and after every accepted child |

The diagnostic ledger timing is not used as a speed result. The speed claim comes only from the
unprofiled 7+1 rail under the same compute profile as the sealed ADR 356 baseline.

## Mechanism

`try_pin_decoded_pages` now returns the typed outcome
`PinDecodedPagesOutcome::{Admitted, RejectedPreflight, RejectedMidDecode}`. It retains the prior
repeat-call behavior through `is_admitted()`, while preserving the exact cache fallback on either
rejection.

The call is made once after `probe_started` and before
`visit_streaming_spilled_left_outer_scan_probe_chunks`, inside the existing
`match_filter.is_none()` projected grouped branch. An architecture gate locks that source fence and
the parent-boundary ordering; non-grouped and match-filter shapes do not gain the call.

The existing off-by-default ADR 433 profile schema advances to version 2 and adds:

- typed pin outcome and full admission inputs;
- separate `preload` and `probe` cache/read/decode operation counters;
- no new unboxed state in `SpilledInnerJoinBuild`.

When the profile environment variable is absent, the existing `Option<Box<_>>` remains `None`: no
receipt, counter state, sink, or diagnostic timing is constructed.

## Pin admission receipt

The live q13 receipt is internally reconciled:

| field | bytes / count |
|---|---:|
| task memory cap | 67,108,864 |
| retained-footprint cap (16x) | 1,073,741,824 |
| resident baseline | 0 |
| resident index | 132,000,000 |
| writer peak | 1,736 |
| page reserve | 65,536 |
| loaded-page estimate | 9,059,232 |
| preflight estimate | 141,126,504 |
| actual decoded bytes | 12,562,560 |
| pages attempted / completed | 5,860 / 5,860 |
| outcome | `admitted` |

The preflight components sum exactly to 141,126,504 bytes. Replacing the loaded-page estimate with
the actual decoded set gives 144,629,832 retained guard bytes, 13.47% of the retained cap. Admission
is therefore established from the full receipt rather than inferred from serialized spill size.

## Counter collapse

The diagnostic child retained fingerprint `ac3784c060c3c226`, the ADR 433 digest,
schema `fnv1a64:06933e705bc02cb3`, 46 rows, and 9,152,992 spill bytes.

| operation | ADR 433 before | ADR 434 after | result |
|---|---:|---:|---|
| file reads / page decodes | 4,823,849 | 5,860 | 823.18x fewer |
| bytes read | 7,532,826,200 | 9,152,992 | 822.99x -> 1.00x amplification |
| pending file reads | 4,823,757 | 5,860 | preload only |
| decoded evictions, total | 4,819,936 | 1,947 | preload-only cache churn |
| decoded evictions, probe | not split | **0** | target loop removed |
| file reads / decodes, probe | not split | **0 / 0** | target loop removed |
| voluntary context switches | 9,660,675 | 23,523 | 410.69x fewer |

The 1,947 remaining decoded evictions occur while the one-time preload passes through the normal
8 MiB cache. On admission the method clears cache/LRU and publishes all decoded pages; the subsequent
14,837,583 probe lookups and 13,627,398 materialization page requests perform no cache or file work.
This is the proof-of-fix requested by ADR 433.

## Unprofiled ADR 356 before/after

One separate warmup completed at 28,189.037 ms. All seven measured children passed and agreed on
fingerprint, digest, schema, 46 rows, and 9,152,992 spill bytes:

| run | execute ms | max RSS KiB | voluntary switches |
|---:|---:|---:|---:|
| 0 | 28,288.448 | 956,592 | 23,482 |
| 1 | 28,342.476 | 956,288 | 23,464 |
| 2 | 28,277.304 | 956,000 | 23,459 |
| 3 | 28,210.956 | 955,624 | 23,487 |
| 4 | 28,308.193 | 956,132 | 23,456 |
| 5 | 28,297.541 | 956,212 | 23,482 |
| 6 | 28,235.545 | 955,172 | 23,458 |

| statistic | ADR 356 before | ADR 434 after |
|---|---:|---:|
| minimum | 133,222.889 ms | 28,210.956 ms |
| median | 134,484.731 ms | **28,288.448 ms** |
| maximum | 138,617.240 ms | 28,342.476 ms |
| spread `(max-min)/median` | 0.0401 | **0.00465** |
| Toron / sealed DuckDB q13 median | 97.31x | 20.47x |

The applicable band is `max(0.10, 2 * max(0.0401, 0.00465)) = 0.10`. The 78.97% median reduction
exceeds that band by a wide margin. No retry was needed.

## Correctness and survival

The independent persisted strict sf=10 correctness command completed under the 4 GiB/no-swap scope:

```text
tpch_correctness query=q13 sf=10 mode=strict phase=compare status=pass reason=none elapsed_ms=41088.0
```

It recorded 1,023,940 KiB maximum RSS, 24,650 voluntary context switches, no swap, exit status 0,
and clean join-spill scratch. This revalidates q13's cell under the defended survival envelope; no
full-board redux is claimed.

## Tests and guardrails

- admitted fixture: one decode per page, pinned probe reads perform no further decode, and the real
  serialized receipt has preload reads bounded by page count with zero probe reads/evictions;
- preflight rejection: typed `RejectedPreflight`, zero attempted/completed pages, no pinned state,
  value-identical cached fallback, and serialized receipt;
- mid-decode rejection: typed `RejectedMidDecode`, attempted/completed progress retained, no pinned
  state, value-identical cached fallback, and serialized receipt;
- disabled profile: no state and no sink;
- architecture fence: exactly one pin call inside the projected grouped LeftOuter recognizer, ordered
  after `probe_started` and before its streamed-spilled probe helper;
- deep-recursion reproducer: passes; the change adds no unboxed state to the widely constructed join
  build.

## Verification

- `scripts/cargo-cgroup fmt -- --check` - pass;
- `scripts/cargo-cgroup check -p toron --all-targets` and
  `scripts/cargo-cgroup check -p toron-tpch --all-targets` - pass;
- `scripts/cargo-cgroup test -p toron --lib` - 1,290 passed, 0 failed;
- `scripts/cargo-cgroup test -p toron --test architecture_test` - 2,080 passed, 0 failed;
- `scripts/cargo-cgroup test -p toron --no-fail-fast` - every unit, integration, and doc-test binary
  green;
- `scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast` - every
  unit, integration, and doc-test binary green; the two pre-existing fixture ignores remain labeled;
- `git diff --check` - clean;
- `git ls-files '*.perf.data'` - empty;
- `exec.rs` remains 7,499 lines.

The final post-gate release rebuild is byte-identical to the binary used for every live receipt.

The implementation and this results document remain uncommitted for independent review.
