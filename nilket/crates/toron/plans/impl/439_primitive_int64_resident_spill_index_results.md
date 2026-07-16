# ADR 439 results - primitive resident spill index on the partitioned scan-build path

## Verdict

ADR 439 is **green**. The surgical cut enables the existing compact resident index immediately before
`rebuild_index_from_pages` on the partitioned streamed scan-build path, and only for non-null-safe
single-column `Int32`/`Int64` plans. Generic and null-safe plans retain the legacy index.

The strict sf=10 q19 wall moves from the ADR 356 baseline of **83,450.371 ms** to **71,987.808 ms**:

- **13.736% lower execute wall**, or **1.159x faster**;
- current spread **0.01190**, below the 0.20 stability ceiling;
- dynamic materiality band
  `max(0.10, 2 * max(0.0556, 0.01190)) = 0.1112`;
- the measured win clears that band by **0.02616** (2.616 percentage points);
- no retry was required;
- the q19 gap to the retained 717 ms DuckDB baseline moves from **116.39x** to **100.40x**.

All seven measured rows preserve the ADR 438 fingerprint, digest, schema, one-row output, and
**51,471,552** spill bytes. The corroborative ADR 438 ledger reread also moves the named producer in
both required dimensions: median D1 time falls **12.71%**, and median `probe_lookup_select` time falls
**17.59%**, while both normalized shares fall. This is wall removal, not leaf relabeling.

One persisted-Parquet strict sf=10 q19 pass@compare is green under 4 GiB/no-swap. The board remains
22/22; this cut does not claim a full-board redux.

## Conditions and provenance

| item | value |
|---|---|
| committed ADR 439 plan | `d290abd61e2f40ed7595b64023a35fa03d353716` |
| implementation baseline | ADR 438 seal `e6074ee` |
| measurement date | 2026-07-15 CDT |
| release binary | `target/adr439/release/toron-tpch` |
| binary SHA-256 | `a69f84cac11ebedf944eebd553f6a4e948fc5bb7a26befed1203832c5e521d16` |
| sf=10 manifest SHA-256 | `c04dd8dc81df705578abda1839f7e2f8ca39d2e6b4c43bcfef2aed5f77c9aa8f` |
| data | persisted strict sf=10 Parquet under `target/tpch/data` |
| throughput / ledger scope | `AllowedCPUs=0-1`, `CPUQuota=150%`, `MemoryHigh=infinity`, `MemoryMax=infinity`, `TasksMax=512`, `RuntimeMaxSec=3600` |
| survival scope | the same CPU placement, `MemoryHigh=4G`, `MemoryMax=4G`, `MemorySwapMax=0`, `TasksMax=512`, `RuntimeMaxSec=600` |
| host | 12th Gen Intel Core i9-12900K; 16 cores / 24 threads; CPUs 0-1 are SMT siblings on P-core 0; 61 GiB RAM; no swap |
| kernel | Linux `7.0.0-27-generic` |
| evidence roots | `target/tpch/adr439/perf/attempt1`, `target/tpch/adr439/ledger/child1..5`, `target/tpch/adr439/survival` |

The release binary was built once after the three focused tests and the architecture fence were green,
then reused for every accepted live receipt. Join-spill scratch was empty before and after the
throughput cohort, every ledger child, and the survival scope.

## Implementation

The production change is five lines in the existing owner
`try_visit_partitioned_streamed_inner_scan_build_scan_probe_chunks`:

1. derive a loop-invariant eligibility flag from the already-bound key plan:
   `!null_safe_keys && matches!(key_plan, JoinKeyPlan::Int32 | JoinKeyPlan::Int64)`;
2. after unindexed writes, `finish`, and write-scratch release, call
   `use_compact_resident_index()` immediately before `rebuild_index_from_pages` when eligible.

No spill-index implementation, lookup accessor, residency formula, or async state changes. The call
selects already-landed machinery that the sibling reorient path uses.

Tests lock the behavior at three levels:

- the production write-unindexed / finish / opt-in / rebuild sequence builds the primitive `Int64`
  index over two partitions, leaves the legacy index empty, preserves exact lookup multisets, and
  reconciles resident accounting;
- generic and honestly-derived nullable/null-safe plans stay on the legacy index, including an actual
  null-key lookup;
- a tight task budget fails at the rebuild site with typed memory `BudgetExhausted` and the existing
  `streamed inner join build spill resident state` producer.

The architecture test slices the live owner and order-checks the non-null-safe primitive fence, guarded
opt-in, and adjacent rebuild. It also rejects query branding.

## Unprofiled speed rail

One discarded warmup and seven measured fresh children ran under the ADR 356 compute profile.

| run | execute ms | peak RSS KiB | spill bytes |
|---:|---:|---:|---:|
| 0 | 71,689.115 | 118,884 | 51,471,552 |
| 1 | 71,987.808 | 119,484 | 51,471,552 |
| 2 | 71,838.253 | 119,552 | 51,471,552 |
| 3 | 72,094.754 | 119,412 | 51,471,552 |
| 4 | 72,077.506 | 119,340 | 51,471,552 |
| 5 | 72,545.701 | 118,944 | 51,471,552 |
| 6 | 71,854.668 | 119,220 | 51,471,552 |

| metric | ADR 356 before | ADR 439 after | change |
|---|---:|---:|---:|
| minimum execute ms | 80,839.702 | 71,689.115 | -11.32% |
| median execute ms | 83,450.371 | 71,987.808 | **-13.736%** |
| maximum execute ms | 85,482.071 | 72,545.701 | -15.13% |
| spread `(max-min)/median` | 0.0556 | 0.01190 | stable |
| maximum peak RSS KiB | 295,932 | 119,552 | -59.60% |
| Toron / DuckDB | 116.39x | 100.40x | narrower |

All seven rows are `pass/release` and invariant on:

- plan fingerprint `65c3c2c0b0a99063`;
- Arrow IPC digest
  `ArrowIpcDigest([253, 54, 72, 128, 190, 45, 198, 7, 153, 127, 37, 175, 196, 190, 44, 105, 166, 96, 62, 18, 87, 70, 149, 246, 201, 35, 150, 91, 14, 116, 137, 213])`;
- schema fingerprint `fnv1a64:677ce2d00a9afbc2`;
- one output row and 51,471,552 spill bytes.

## ADR 438 ledger corroboration

Five fresh profiler-enabled, external-perf-disabled children ran in separate scopes. Raw timings are
diagnostic and are not cross-compared with the unprofiled speed cohort.

| child | root us | D1 us | probe lookup us | D1/root | probe/root | residual/root |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 72,060,172 | 40,591,659 | 23,086,280 | 0.563302 | 0.320375 | 0.054188 |
| 2 | 72,719,461 | 40,938,637 | 23,297,966 | 0.562967 | 0.320381 | 0.054034 |
| 3 | 81,087,585 | 45,273,471 | 25,853,855 | 0.558328 | 0.318839 | 0.051277 |
| 4 | 81,857,866 | 45,714,495 | 26,094,198 | 0.558462 | 0.318774 | 0.050773 |
| 5 | 79,141,951 | 44,522,198 | 25,542,477 | 0.562561 | 0.322743 | 0.051823 |

| corroborative metric | ADR 438 median | ADR 439 median | change |
|---|---:|---:|---:|
| D1 absolute us | 51,005,848 | 44,522,198 | **-12.71%** |
| D1/root | 0.581713 | 0.562561 | **-0.01915** |
| `probe_lookup_select` absolute us | 30,994,398 | 25,542,477 | **-17.59%** |
| `probe_lookup_select`/root | 0.353798 | 0.320375 | **-0.03342** |

Every child retains:

- exact `M = 59,986,052` predicate-input rows and `S = 1,134` predicate-output rows;
- `S = 1,134` SUM arguments and numeric-fold rows;
- an in-root residual below 10%;
- the same fingerprint, digest, schema, one row, and spill bytes as the unprofiled cohort.

The independent oracle identity remains `171 + 382 + 581 = 1,134`.

## 4 GiB/no-swap survival rail

One persisted-Parquet strict sf=10 q19 correctness scope completed:

| item | value |
|---|---:|
| status | `pass@compare` |
| correctness elapsed | 100,166.877 ms |
| wall | 1:40.22 |
| peak RSS | 839,592 KiB |
| swaps | 0 |
| exit status | 0 |
| scratch after exit | empty |

Correctness elapsed includes the correctness harness and is not an execute-wall performance sample.
The exact output identity is independently pinned by pass@compare and by the seven invariant
persisted-Parquet perf rows above.

## Verification

The final uncommitted implementation passes:

- `scripts/cargo-cgroup fmt -p toron -- --check`;
- `scripts/cargo-cgroup test -p toron --lib`: **1,307 passed, 0 failed**;
- `scripts/cargo-cgroup test -p toron --test architecture_test`: **2,083 passed, 0 failed**;
- `scripts/cargo-cgroup test -p toron --no-fail-fast`: every unit, integration, architecture,
  and doc-test binary completed with zero failures;
- `scripts/cargo-cgroup test -p toron-tpch`: zero failures;
- `scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast`:
  zero failures;
- `git diff --check`: clean;
- `exec.rs`: 7,499 lines; `exec/join.rs`: 3,713 lines;
- no tracked `.perf.data` file; join-spill scratch empty.

## Seal state

The committed plan remains HEAD. The implementation and this results receipt remain uncommitted for
independent review. Board status remains 22/22.
