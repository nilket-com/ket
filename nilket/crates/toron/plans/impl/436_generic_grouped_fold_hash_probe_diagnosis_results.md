# ADR 436 results - generic grouped-fold hash-probe throughput diagnosis

## Verdict

ADR 436 is **green as a diagnosis**: it conclusively names q13's next throughput producer. Its literal
ultra-rare-tail stability subrail remains **red** in this record and is not rewritten as a pass.

The stable finding is that equality dominates `find_bucket_in`:

- `find_bucket_in` remains the largest Toron userspace frame at **21.28%-22.92%** of cpu-core cycles
  in all five fresh children;
- local annotation attributes **71.97%-74.32%** of that frame to key equality in all five children,
  while the bucket-load instruction accounts for 21.44%-25.59% and hash computation is residual;
- the required ledger reconciles exactly to **18,933,731** hash calls in every child;
- successful matching equality is the largest equality class: exactly **13,837,604** invocations per
  child, or **63.11%-63.22%** of all equality invocations; collision mismatches are
  8,048,901-8,088,126, or 36.78%-36.89%;
- probe geometry is ordinary and stable through p99.9: occupied lookup p99/p99.9 is **6/11**, vacant
  lookup and placement is **8/14**, and rehash is **3/5-6**; average probes are 1.425-1.427 and the
  final occupied-cluster maximum is 26-28.

This selects the **matching-equality / key-representation branch** as the evidence-backed ADR 437
candidate: a cheaper canonical/compact key representation or group ID. Hash-quality work is not the
leading cut, and a fingerprint pre-check would not remove successful matching equality. Upstream key
construction remains separate and is not claimed fixed.

The committed stability rule also tests the predeclared `>8`, `>16`, `>32`, and `>64` tail rates with
`(max-min)/median <= 0.25`. Three ultra-rare rates fail that rule: occupied `>32`, vacant/placement
`>32`, and rehash `>16`. These bins contain only 0-12 qualifying operations per 1.5M-13.8M calls in a
child; p99, p99.9, the material `>8`/`>16` rates, collision share, average probes, and final cluster
geometry remain stable. The result is therefore an honest **named acceptance residual**, not a reason
to change thresholds after observing the data.

The promotion review concludes that this residual is a counting-noise regime rather than evidence
against the producer diagnosis. At expected counts near 5 and 12, Poisson relative standard deviation
is approximately 0.45 and 0.29 respectively, already above the fixed 0.25 spread gate before any real
seed variation. The diagnosis is banked on its independently strict and stable reconciliation,
identity, p99/p99.9, dominant-category, overflow, and parity rails. The failed rates stay red.

## Conditions and provenance

| item | value |
|---|---|
| ADR 435 implementation baseline | `9b8a0c2` |
| committed ADR 436 plan | `941c967` |
| measurement date | 2026-07-14 CDT |
| accepted symbol-rich release binary SHA-256 | `86952d8cdc4e1be4311920fd721113e74ab7fabb105e49d53e6ac1c25e0e062d` |
| accepted binary | `target/tpch/adr436/symbol-target/release/toron-tpch` (`not stripped`, debug info present) |
| sf=10 manifest SHA-256 | `c04dd8dc81df705578abda1839f7e2f8ca39d2e6b4c43bcfef2aed5f77c9aa8f` |
| data | persisted strict sf=10 Parquet under `target/tpch/data` |
| scope | `MemoryHigh=infinity`, `MemoryMax=infinity`, `AllowedCPUs=0-1`, `CPUQuota=150%`, `TasksMax=512`, `RuntimeMaxSec=3600` |
| host | 12th Gen Intel Core i9-12900K; 16 cores / 24 threads; CPUs 0-1 are SMT siblings on P-core 0; 61 GiB RAM; no swap |
| kernel / perf / governor | Linux `7.0.0-27-generic`; perf `7.0.6`; powersave; `perf_event_paranoid=1`; `kptr_restrict=1` |
| evidence root | `target/tpch/adr436/evidence/` |
| histogram | exact bins `0..=1023`, one overflow bucket, widen trigger at overflow `>= 0.1%` |
| predeclared tail thresholds | `>8`, `>16`, `>32`, `>64` probes |

All ten accepted children use the same binary SHA. Every child passed with fingerprint
`ac3784c060c3c226`, the sealed Arrow IPC digest, schema `fnv1a64:06933e705bc02cb3`, 46 rows, and
9,152,992 spill bytes. Join-spill scratch was empty before and after every scope.

Profiling timing is diagnostic overhead and is not a speed result. The ADR 356 baseline remains
unchanged.

## Implementation

ADR 435's boxed, off-by-default `GenericGroupedFoldProfile` now owns the ADR 436 ledger. No second
profiling surface was introduced. When `TORON_GENERIC_GROUPED_FOLD_PROFILE_DIR` is absent there is no
profile allocation, sink, or clock.

When enabled, schema version 2 records:

- four phases: primary occupied, primary vacant, vacant placement, and rehash;
- hash calls, bucket visits, equality invocations, matching equality, collision mismatch, and an exact
  probe histogram per phase;
- the same phase matrix per growth epoch;
- the final occupied-cluster run-length histogram;
- the existing upstream key-allocation count separately from `find_bucket_in`;
- one atomic temporary-then-rename JSON receipt per finalized fold, with publication failures returned
  as `ExecutionError`.

The ledger derives equality identities from the actual lookup outcome. An occupied lookup performs one
matching equality and `probes-1` collision mismatches. A vacant lookup performs `probes-1` mismatch
equalities because the terminal empty bucket performs no equality. Rehash and placement are therefore
auditable without per-row timers or stored samples.

The enabled/disabled unit fixture preserves exact sorted key/state output and all existing residency
telemetry. The five disabled perf children and five enabled ledger children additionally preserve the
live q13 fingerprint/digest/schema/rows/spill tuple.

## External perf - sampled category stability

Five fresh disabled-profile children ran under:

```text
perf record --freq 99 --call-graph dwarf,16384
```

Each report has zero lost samples and approximately 2K cpu-core samples. Kernel symbols remain
restricted by the host policy; userspace Toron symbols and inline chains are resolved.

| child | `find_bucket_in` / cpu-core cycles | equality / local frame samples | bucket load / local frame samples | process-wide `_int_malloc` |
|---:|---:|---:|---:|---:|
| 1 | 21.28% | 72.52% | 24.83% | 7.15% |
| 2 | 22.91% | 72.83% | 24.21% | 6.68% |
| 3 | 22.86% | 74.16% | 21.44% | 6.18% |
| 4 | 22.92% | 71.97% | 25.59% | 6.36% |
| 5 | 22.50% | 74.32% | 22.55% | 6.28% |

The `find_bucket_in` cycle-share spread is 0.0717 and equality is the dominant local category in all
five children. The process-wide allocator share is an upper bound for upstream key construction—not
an attribution to it—and is materially smaller than `find_bucket_in` in every child.

The local annotation is preferred over incomplete default callgraph rendering: every child resolves
the same machine code, and it separates the candidate-key length/value comparisons from the bucket
load and inline hash region directly.

## Required ledger - exact operation reconciliation

The q13 inner fold is selected by its 15,337,604 input rows and 1,500,000 groups. Its 46-group outer
fold is emitted as a second receipt and never pooled.

Every child independently satisfies:

| domain | hash calls | matching equalities | identity |
|---|---:|---:|---|
| primary occupied | 13,837,604 | 13,837,604 | exactly one match per occupied lookup |
| primary vacant | 1,499,999 | 0 | first primary lookup uses the empty-table shortcut |
| vacant placement | 1,500,000 | 0 | equality invocations = bucket visits - calls |
| rehash | 2,096,128 | 0 | equality invocations = bucket visits - calls |
| **total** | **18,933,731** | **13,837,604** | total calls = total hash computations |

For every phase and child, collision mismatches equal equality invocations minus matching equalities.
No counter overflowed.

| child | total bucket visits | probes / call | collision mismatches | matching share of equalities | final cluster max | growth epochs | histogram overflow |
|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 26,982,632 | 1.425109 | 8,048,901 | 63.2244% | 26 | 12 | 0 / no widen |
| 2 | 27,021,857 | 1.427181 | 8,088,126 | 63.1113% | 28 | 12 | 0 / no widen |
| 3 | 26,993,311 | 1.425673 | 8,059,580 | 63.1935% | 27 | 12 | 0 / no widen |
| 4 | 27,014,635 | 1.426799 | 8,080,904 | 63.1321% | 26 | 12 | 0 / no widen |
| 5 | 27,015,374 | 1.426838 | 8,081,643 | 63.1299% | 28 | 12 | 0 / no widen |

The per-growth-epoch receipts cover entry limits 1,024 through 2,097,152 and bucket counts 2,048
through 4,194,304. The exact epoch histograms are retained in every child receipt. In child 1, average
probes by epoch range from 1.3384 to 1.6424; the largest per-epoch observed probe is 45. Combined with
the 26-28 final cluster maximum and stable p99.9, this does not support poor hash quality as the primary
producer.

## Probe distribution and stability gate

| phase | calls / child | p99 values | p99.9 values | max range | p99 spread | p99.9 spread |
|---|---:|---|---|---|---:|---:|
| primary occupied | 13,837,604 | 6 / 6 / 6 / 6 / 6 | 11 / 11 / 11 / 11 / 11 | 31-40 | 0 | 0 |
| primary vacant | 1,499,999 | 8 / 8 / 8 / 8 / 8 | 14 / 14 / 14 / 14 / 14 | 39-46 | 0 | 0 |
| vacant placement | 1,500,000 | 8 / 8 / 8 / 8 / 8 | 14 / 14 / 14 / 14 / 14 | 39-46 | 0 | 0 |
| rehash | 2,096,128 | 3 / 3 / 3 / 3 / 3 | 6 / 5 / 6 / 6 / 6 | 14-19 | 0 | 0.1667 |

`max` is reported but never gated. All p99 and p99.9 spreads pass `<= 0.25`.

The normalized tail-rate gate is mixed:

| phase / rate | five values, per million calls | spread | gate |
|---|---|---:|---|
| occupied `>8` | 2892.047 / 2894.504 / 2889.012 / 2951.378 / 2923.555 | 0.0215 | pass |
| occupied `>16` | 150.676 / 130.514 / 145.618 / 134.127 / 137.741 | 0.1464 | pass |
| occupied `>32` | 1.445 / 0.939 / 0.361 / 0 / 1.373 | **1.5385** | **fail** |
| vacant `>8` | 8117.339 / 8086.672 / 8020.005 / 8108.672 / 8097.339 | 0.0120 | pass |
| vacant `>16` | 534.667 / 520.667 / 506.000 / 528.667 / 506.667 | 0.0551 | pass |
| vacant `>32` | 6.667 / 7.333 / 4.667 / 4.000 / 8.000 | **0.6000** | **fail** |
| placement `>8` | 8116.667 / 8086.667 / 8020.000 / 8108.000 / 8097.333 | 0.0119 | pass |
| placement `>16` | 534.667 / 520.667 / 506.000 / 528.667 / 506.667 | 0.0551 | pass |
| placement `>32` | 6.667 / 7.333 / 4.667 / 4.000 / 8.000 | **0.6000** | **fail** |
| rehash `>8` | 56.771 / 71.083 / 62.019 / 60.588 / 66.313 | 0.2308 | pass |
| rehash `>16` | 0 / 0 / 0.477 / 0 / 0 | **median zero, not all zero** | **fail** |

All `>64` rates are zero in all five children. Occupied/vacant/placement `>64`, rehash `>32`, and
rehash `>64` therefore pass the median-zero rule. Histogram overflow is zero everywhere, so p99.9 is
measured rather than bounded and no wider-bin rerun is required.

The failed rates correspond to 0-20 occupied operations, 6-12 vacant/placement operations, and 0-1
rehash operations per child. They establish that the extreme tail is seed-sensitive under the locked
relative-spread rule; they do not overturn the stable dominant-cost attribution.

## Rejected exploratory evidence

The first release build requested debug info but inherited workspace `strip = "symbols"`. Five ledger
and five perf children were collected before report generation exposed that the binary was stripped.
Those artifacts are retained under `ledger-stripped/` and `perf-stripped/` but rejected:

- the perf set cannot prove cycle categories because Toron symbols are absent;
- the ledger set is internally valid but belongs to a different binary SHA and is unnecessary once the
  accepted one-binary set exists.

The accepted binary was rebuilt explicitly with `CARGO_PROFILE_RELEASE_STRIP=none`; both accepted rails
were then rerun from scratch on that one SHA. No rejected number is used above.

## Acceptance status and forward arc

| acceptance item | status |
|---|---|
| exact 18,933,731 operation reconciliation | pass, 5/5 |
| matching/collision identities | pass, every phase and child |
| exact p99/p99.9 with overflow below 0.1% | pass, zero overflow |
| same dominant perf category | pass, equality 5/5 |
| p99/p99.9 spread <= 0.25 | pass |
| all predeclared tail-rate spreads <= 0.25 | **fail on ultra-rare tails** |
| enabled/disabled result and telemetry parity | pass |
| no engine mechanism or speed claim | pass |

The producer diagnosis is banked: successful matching equality over the generic `Vec<GroupValue>` key
is the dominant stable cost, while collision geometry does not justify a hash-quality cut. ADR 437 is
therefore justified as a compact/canonical key or group-ID representation cut.

ADR 436 does not change its threshold or backfill the failed tail rates as passes. It seals because its
diagnosis deliverable is achieved robustly while the mis-specified subrail remains visible. The rule is
fixed prospectively for ADR 437 and later diagnostics: a relative-spread gate applies only to bins above
a **predeclared minimum-event floor**. Below that floor, the exact counts and maxima remain reportable,
and stability is required from a predeclared coarser rate instead. The event floor and coarser rate must
be reviewed before measurement; neither is retroactively applied to ADR 436.

## Verification

- `scripts/cargo-cgroup fmt -- --check` - pass;
- `scripts/cargo-cgroup check -p toron --all-targets` - pass;
- `scripts/cargo-cgroup test -p toron --lib` - 1,298 passed / 0 failed;
- `scripts/cargo-cgroup test -p toron --test architecture_test` - 2,081 passed / 0 failed;
- `scripts/cargo-cgroup test -p toron --no-fail-fast` - all unit, integration, architecture, and doc
  targets pass with zero failures;
- `scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast` - all
  targets pass with zero failures (the two pre-existing explicitly ignored oracle-baseline tests remain
  ignored);
- focused group-table ledger/parity/overflow/loud-publication/growth-guard tests - pass;
- `exec.rs` remains 7,499 lines; `git diff --check` is clean; no `.perf.data` is tracked.
