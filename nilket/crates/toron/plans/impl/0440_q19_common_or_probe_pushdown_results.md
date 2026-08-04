# ADR 440 results - factor common OR conjuncts into the probe child

## Verdict

ADR 440 is **green**. The binder now factors deterministic, pushdown-safe, side-local conjuncts that
are structurally common to every arm of an ADR 319 admitted OR, then routes them through the existing
cross-child filter seam. No executor code changed.

The strict sf=10 q19 execute wall moves from ADR 439's **71,987.808 ms** to **12,839.121 ms**:

- **82.165% lower execute wall**, or **5.607x faster**;
- current spread **0.01846**, below the 0.20 stability ceiling;
- dynamic materiality band
  `max(0.10, 2 * max(0.011899, 0.018457)) = 0.10`;
- the measured win clears that band by **0.72165** (72.165 percentage points);
- no retry was required;
- the q19 gap to the retained 717 ms DuckDB baseline moves from **100.40x** after ADR 439 to
  **17.91x**.

The exact mechanism receipt holds in every one of five diagnostic children:

- the join-filter input moves from **59,986,052** rows to exactly **2,141,904** rows, the predeclared
  common-ship-filter count (**3.57067% retained; 96.42933% reduction**);
- the residual still selects **1,134** rows, and SUM arguments and numeric-fold rows are both exactly
  **1,134**;
- the independent oracle identity remains `171 + 382 + 581 = 1,134`;
- the residual program falls from 107 physical nodes to 68, and the probe projection no longer carries
  `l_shipmode` or `l_shipinstruct` above the scan filter.

All seven unprofiled rows preserve the exact Arrow digest, schema, one-row result, revenue value, and
**51,471,552** spill bytes. One persisted-Parquet strict sf=10 q19 `pass@compare` is green under
4 GiB/no-swap. Board status remains 22/22; this cut does not claim a full-board redux.

## Conditions and provenance

| item | value |
|---|---|
| committed ADR 440 plan | `e396efa130741ec94aaa49f427d6b9b1e0a8a7a2` |
| implementation baseline | ADR 439 seal `e571ed3` |
| measurement date | 2026-07-15 CDT |
| release binary | `target/adr440/release/toron-tpch` |
| binary SHA-256 | `da549e76dfebfdbb51ebdea4de106b31796a6525583f6b8fb2ec99f8a4f777f1` |
| sf=10 manifest SHA-256 | `c04dd8dc81df705578abda1839f7e2f8ca39d2e6b4c43bcfef2aed5f77c9aa8f` |
| data | persisted strict sf=10 Parquet under `target/tpch/data` |
| throughput / ledger scope | `AllowedCPUs=0-1`, `CPUQuota=150%`, `MemoryHigh=infinity`, `MemoryMax=infinity`, `TasksMax=512`, `RuntimeMaxSec=3600` |
| survival scope | same CPU placement, `MemoryHigh=4G`, `MemoryMax=4G`, `MemorySwapMax=0`, `TasksMax=512`, `RuntimeMaxSec=600` |
| host | 12th Gen Intel Core i9-12900K; 16 cores / 24 threads; CPUs 0-1 are SMT siblings on P-core 0; 61 GiB RAM; no swap |
| kernel | Linux `7.0.0-27-generic` |
| accepted evidence roots | `target/tpch/adr440/perf/final`, `target/tpch/adr440/ledger/final/child1..5`, `target/tpch/adr440/survival/final` |

The release binary was rebuilt after the final harness-shape assertion changed, then reused for every
accepted live receipt. Earlier smoke and pre-relink runs remain discarded calibration; no number from
them is mixed into the accepted tables. Join-spill scratch was empty before and after the throughput
cohort, every ledger child, and the survival scope.

## Implementation

`CommonOrExtraction` replaces the old `(keys, residual)` tuple and owns four aligned products:

- the common bridging keys ADR 319 already lifted;
- common left-side predicates;
- common right-side predicates;
- the reduced residual OR, or `None` when any arm reduces to true after factoring.

Extraction remains fenced to ORs with a common bridging key in every arm. After removing one occurrence
of each common key, it intersects remaining conjuncts structurally across all arms. A candidate moves
only when it is:

- equal through the typed `expr_structural_eq` visitor (float bits, timestamp unit, Decimal
  precision/scale, cast type, and every expression field);
- deterministic;
- accepted by `guard_filter_pushdown_safe`;
- local to exactly one join child.

One structural occurrence is removed from every arm. Distinct duplicate candidates are extracted only
once. If any reduced arm is empty, the residual OR is omitted; otherwise the reduced arms are rebuilt.
The existing `push_into_cross_child` route produces `Filter<Scan>` (possibly under `Exchange`), which
the existing streaming scan-probe decomposition already consumes. `exec.rs` and `exec/join.rs` remain
unchanged at 7,499 and 3,713 lines.

The TPC-H keyed-q19 harness assertion now pins the new physical shape rather than the stale pre-440
field list: one-key Inner join, ship columns absent from its output, and both ship predicates present in
a direct scan-child filter.

## Correctness and structural gates

The focused ADR 440 tests cover:

- common side-local predicate placement at the scan boundary;
- a conjunct missing from one arm staying above the join;
- nullable SQL three-valued equivalence against explicitly factored SQL;
- reordered conjuncts and one-occurrence duplicate removal;
- empty reduced arm removing the residual OR;
- an error-prone arithmetic predicate rejected by the pushdown-safety guard;
- structural inequality for different float bits, timestamp units, Decimal scales, and cast types;
- architecture ownership, common-key fencing, typed structural equality, both safety gates, and no
  query branding in the generic binder helper.

The cross-join fixture now honors pushed PBC predicates before projection, matching the production
TPC-H storage boundary. Without that correction, a `FullyPushed` child filter made the execution gate
vacuous because the old fixture returned unfiltered rows.

## Unprofiled speed rail

One discarded warmup and seven measured fresh children ran under the ADR 356 compute profile.

| run | execute ms | peak RSS KiB | spill bytes |
|---:|---:|---:|---:|
| 0 | 12,712.652 | 130,388 | 51,471,552 |
| 1 | 12,862.353 | 118,040 | 51,471,552 |
| 2 | 12,910.256 | 128,692 | 51,471,552 |
| 3 | 12,839.121 | 129,684 | 51,471,552 |
| 4 | 12,673.285 | 119,264 | 51,471,552 |
| 5 | 12,843.109 | 118,736 | 51,471,552 |
| 6 | 12,741.642 | 118,716 | 51,471,552 |

| metric | ADR 439 before | ADR 440 after | change |
|---|---:|---:|---:|
| minimum execute ms | 71,689.115 | 12,673.285 | -82.32% |
| median execute ms | 71,987.808 | 12,839.121 | **-82.165%** |
| maximum execute ms | 72,545.701 | 12,910.256 | -82.20% |
| spread `(max-min)/median` | 0.01190 | 0.01846 | stable |
| Toron / DuckDB | 100.40x | 17.91x | narrower |

All seven measured rows are `pass/release` and invariant on:

- new plan fingerprint `9a60798bff80eafc` (the pre-440 fingerprint was `65c3c2c0b0a99063`);
- Arrow IPC digest
  `ArrowIpcDigest([253, 54, 72, 128, 190, 45, 198, 7, 153, 127, 37, 175, 196, 190, 44, 105, 166, 96, 62, 18, 87, 70, 149, 246, 201, 35, 150, 91, 14, 116, 137, 213])`;
- schema fingerprint `fnv1a64:677ce2d00a9afbc2`;
- one output row and 51,471,552 spill bytes.

## Exact M to S and producer corroboration

Five fresh profiler-enabled children ran separately from the unprofiled cohort. Profiling time is
diagnostic and is not cross-compared with the speed rail.

| child | root us | D1 us | lookup us | D1/root | lookup/root | D2 us | M | S |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 12,894,457 | 1,602,119 | 861,096 | 0.12425 | 0.06678 | 3,167,743 | 2,141,904 | 1,134 |
| 2 | 12,842,385 | 1,596,567 | 857,133 | 0.12432 | 0.06674 | 3,126,429 | 2,141,904 | 1,134 |
| 3 | 12,970,779 | 1,621,787 | 866,867 | 0.12503 | 0.06683 | 3,180,883 | 2,141,904 | 1,134 |
| 4 | 12,941,450 | 1,596,384 | 854,450 | 0.12335 | 0.06602 | 3,168,031 | 2,141,904 | 1,134 |
| 5 | 12,857,481 | 1,589,171 | 845,123 | 0.12360 | 0.06573 | 3,150,185 | 2,141,904 | 1,134 |

| corroborative metric | ADR 439 median | ADR 440 median | change |
|---|---:|---:|---:|
| D1 absolute us | 44,522,198 | 1,596,567 | **-96.414%** |
| D1/root | 0.562561 | 0.124249 | **-0.43831** |
| `probe_lookup_select` absolute us | 25,542,477 | 857,133 | **-96.644%** |
| `probe_lookup_select`/root | 0.320375 | 0.066743 | **-0.25363** |

Every child also records SUM arguments and numeric-fold rows at 1,134, the new fingerprint, and the
same digest/schema/row/spill identity. D1 is now below D2 (median 1.597 s vs. 3.168 s), satisfying the
predeclared condition for the residual-program follow-on. D4 is the largest overall domain at a
6.591 s median, led by 5.028 s of scan/decode work, so the next cut must not call D2 the total wall
without a fresh ranking.

The reused ADR 438 profiler's coverage flag is honestly red after this large wall removal:
in-root residual is 11.82%-11.94% in the five children, above its historical 10% diagnosis ceiling.
That ceiling was not an ADR 440 mechanism gate, and it does not alter the exact M-to-S identity or the
exclusive D1/lookup reductions. It does mean a future diagnosis must widen the leaf coverage before
using this profiler to name the whole remaining root.

## 4 GiB/no-swap survival rail

One persisted-Parquet strict sf=10 q19 correctness scope completed:

| item | value |
|---|---:|
| status | `pass@compare` |
| correctness elapsed | 30,009.029 ms |
| wall | 0:30.04 |
| peak RSS | 840,184 KiB |
| swaps | 0 |
| exit status | 0 |
| scratch after exit | empty |

Correctness elapsed includes the independent oracle and is not an execute-wall performance sample.

## Failure and retry disclosure

Two development checks improved the final gates:

1. The first nullable execution assertion exposed that `CrossFixtureScan` ignored pushed PBC. The
   fixture was corrected to execute predicate bytecode before projection, matching the real storage
   contract; the 3VL test then passed with discriminating expected rows.
2. The first full TPC-H gate rejected the stale q19 plan-shape helper because it still required
   `l_shipinstruct` above the join. The helper was strengthened to require the new scan-filter shape.
   Its focused test passed before the full gates were rerun.

The initial release measurement preceded that harness-only final-tree change. It was discarded, the
binary was rebuilt, and the full speed/ledger/survival set above was rerun. No selective performance
retry was used.

## Verification

The final uncommitted implementation passes:

- `scripts/cargo-cgroup fmt -p toron -- --check`;
- `scripts/cargo-cgroup test -p toron --lib`: **1,308 passed, 0 failed**;
- `scripts/cargo-cgroup test -p toron --test architecture_test`: **2,084 passed, 0 failed**;
- `scripts/cargo-cgroup test -p toron --no-fail-fast`: every unit, integration, architecture, and
  doc-test binary completed with zero failures;
- `scripts/cargo-cgroup test -p toron-tpch`: zero failures;
- `scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast`: zero
  failures;
- `git diff --check`: clean;
- `exec.rs`: 7,499 lines; `exec/join.rs`: 3,713 lines;
- no tracked `.perf.data` file; join-spill scratch empty.

## Seal state

The committed plan remains HEAD. The implementation and this results receipt remain uncommitted for
independent review. Board status remains 22/22.
