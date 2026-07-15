# ADR 437 results - compact fixed-width group key rejected and removed

## Verdict

ADR 437 is **rejected and removed**.

The first implementation was correctness-green and reduced its inclusive lookup/equality cycle
frame, but it missed the predeclared 10% end-to-end bar, increased residency, and retained a valid
red tail-stability gate. Review selected one bounded revision: make the 16-byte encoding preserve
`GroupValue::Ord`, retain it as the **sole** compact key, and finalize directly from sorted compact
keys. If that revision still missed any hard bar, the committed plan required removal rather than a
third optimization layer.

The sole-key revision fixed the residency objection decisively. Its schema-v4 q13 inner-fold receipt
records:

- 15,337,604 compact lookups and row encodes;
- zero generic lookups, full-key materializations, or retained full keys;
- 1,500,000 retained compact keys and 1,500,000 direct-finalize rows;
- current ownership **388,663,536 bytes**, below ADR 435's 597,440,688-byte full-key baseline;
- projected rehash peak **352,321,968 bytes**, below ADR 435's 511,705,264-byte baseline;
- exact q13 fingerprint, digest, schema, 46 rows, and 9,152,992 spill bytes.

It still failed the locked speed bar. The complete unprofiled 1-warmup + 7-measured rail is invariant
and stable, but its median is **22,155.228 ms** versus ADR 435's 22,257.249 ms: only **1.0046x / 0.46%**
faster, with spread 0.03061. The required improvement was greater than 10%. This is not a marginal
interpretation: the full sample missed the threshold by more than an order of magnitude.

That failed hard bar terminated the cut. The five-child schema-v4 tail/cycle rail and affected-set
rerun were not spent, because they could not rescue a mechanism already required to be removed. The
first implementation's event-floored occupied `>16` red remains preserved below; it is not silently
overwritten by an unrun revision rail.

The compact route, its schema-v4 profiler changes, and its architecture gate were then removed. The
tracked engine and test tree is byte-identical to the sealed ADR 435/436 implementation; only the
ADR 437 plan commit and this rejected-results artifact remain. Board status therefore stays 22/22
on the previously sealed implementation. No full-board redux or article-baseline update is claimed.

## Sole-key revision receipt

| item | value |
|---|---|
| replacement plan commit | `70048d8` |
| implementation baseline restored after rejection | `bed0308` |
| symbol-rich revision binary SHA-256 | `d8c77d08fee67a7801889fef60c0cafcc07823f506594f28e0f4ec304f9574d3` |
| revision build ID | `daba1c7c651e34e215f216f65bcaae60f78a94c8` |
| revision binary | `target/tpch/adr437/order-key-target/release/toron-tpch` |
| schema-v4 receipt | `target/tpch/adr437/evidence/revision/ledger-smoke/` |
| repeated-run receipt | `target/tpch/adr437/evidence/revision/throughput/` |

All seven measured rows agree on fingerprint `ac3784c060c3c226`, the sealed Arrow IPC digest,
schema `fnv1a64:06933e705bc02cb3`, 46 rows, and 9,152,992 spill bytes.

| measured run | execute ms | max RSS KiB |
|---:|---:|---:|
| 1 | 21,926.901 | 645,844 |
| 2 | 21,964.946 | 645,720 |
| 3 | 21,919.956 | 645,712 |
| 4 | 22,155.228 | 645,560 |
| 5 | 22,324.525 | 645,824 |
| 6 | 22,484.474 | 645,996 |
| 7 | 22,598.066 | 646,248 |

| statistic | ADR 435 before | sole-key revision |
|---|---:|---:|
| minimum | 22,061.912 ms | 21,919.956 ms |
| median | 22,257.249 ms | **22,155.228 ms** |
| maximum | 22,445.267 ms | 22,598.066 ms |
| spread `(max-min)/median` | 0.01722 | **0.03061** |
| median reduction | - | **0.46% (fails >10%)** |

The one earlier unprofiled smoke at 20,453.322 ms is disclosed but excluded from the 7-row statistic;
it is not substituted for the slower completed median. The schema-v4 profiled smoke at 20,554.546 ms
is diagnostic and likewise excluded from the speed claim.

## Removal verification

After the speed hard bar rejected the revision, the compact-key route, schema-v4 profiling changes,
and architecture gate were removed before these gates ran:

- `git diff --name-only -- crates/toron/src crates/toron/tests` - empty; the tracked source and test
  tree is exactly the committed ADR 435/436 implementation;
- `cargo fmt --all -- --check` - pass;
- `cargo check -p toron --all-targets` - pass;
- `cargo test -p toron --lib` - 1,298 passed / 0 failed;
- `cargo test -p toron --test architecture_test` - 2,081 passed / 0 failed;
- `cargo test --workspace --no-fail-fast -- --test-threads=1` - pass, including doc tests;
- `cargo test -p toron-tpch --features polars-sql-reference --no-fail-fast -- --test-threads=1` -
  311 passed / 0 failed / 2 ignored;
- `git diff --check` - clean;
- no `.perf.data` file is tracked.

An initial post-removal command used `--all-targets`, which also executes this workspace's long-running
benchmark binaries. All test targets reached before the benchmark section were green; the benchmark
execution was stopped and is not represented as a completed gate. The canonical workspace command
above excludes benchmark executables and completed with exit status 0.

## Rejected first implementation evidence

## Conditions and provenance

| item | value |
|---|---|
| ADR 436 implementation baseline | `bed0308` |
| original first-implementation plan commit (later reset) | `40547db` |
| replacement sole-key/removal plan commit | `70048d8` |
| measurement date | 2026-07-15 CDT |
| accepted symbol-rich release binary SHA-256 | `27e03c950cd7fc40203d495fcd4a89c3bcb08e5d0330d11ee7b7430e76159ca1` |
| build ID | `90923e2391c42de2ea2938b357e265ae4644de1c` |
| accepted binary | `target/tpch/adr437/symbol-target/release/toron-tpch` (`not stripped`, `.debug_info` and `.symtab` present) |
| sf=10 manifest SHA-256 | `c04dd8dc81df705578abda1839f7e2f8ca39d2e6b4c43bcfef2aed5f77c9aa8f` |
| data | persisted strict sf=10 Parquet under `target/tpch/data` |
| throughput / ledger / affected scope | `MemoryHigh=infinity`, `MemoryMax=infinity`, `AllowedCPUs=0-1`, `CPUQuota=150%`, `TasksMax=512`, `RuntimeMaxSec=3600` |
| survival scope | `MemoryHigh=4G`, `MemoryMax=4G`, `MemorySwapMax=0`, same CPU/task profile, `RuntimeMaxSec=600` |
| host | 12th Gen Intel Core i9-12900K; 16 cores / 24 threads; CPUs 0-1 are SMT siblings on P-core 0; 61 GiB RAM; no swap |
| kernel / perf / governor | Linux `7.0.0-27-generic`; perf `7.0.6`; powersave; `perf_event_paranoid=1` for accepted profiles; `kptr_restrict=1` |
| evidence root | `target/tpch/adr437/evidence/` |
| join-spill scratch | `target/tpch/join_spill`; empty after every accepted scope, with a bounded post-exit drain for asynchronous file teardown |

All accepted live rows use the same binary SHA. The q13 rows agree on fingerprint
`ac3784c060c3c226`, the sealed Arrow IPC digest, schema `fnv1a64:06933e705bc02cb3`, 46 rows, and
9,152,992 spill bytes.

Profiling receipts and one-shot affected-set timings are diagnostic. The speed comparison uses only
the unprofiled 7+1 rail.

## Implementation

`BoundGroupedAggregateGroups` now configures a lookup layout once from the evaluated Arrow group
types. An admitted column contributes one null marker plus a fixed-width canonical payload. The
layout is admitted only when the complete encoding fits 16 bytes. Each row is encoded into a fully
zeroed `[u8; 16]`.

The admitted path stores a compact key beside each retained full key and aggregate-state vector.
An occupied probe updates the existing states without constructing a `Vec<GroupValue>`. A vacant
probe guards the complete pending entry and any rehash peak before invoking the one-shot full-key and
state constructors. The full key is retained exactly once per distinct group. Rehash uses the compact
keys; finalization still sorts the retained `(full_key, states)` pairs by `GroupValue::Ord`.

The generic `Vec<GroupValue>` lookup remains the exact fallback for UTF-8, Decimal128 (17 bytes with
its null marker), over-width keys, and unsupported layouts. A batch whose group types differ from the
configured layout fails loudly rather than silently switching representations.

### Encoding and mutation rails

Focused tests establish:

- Eq-faithful admitted encodings for signed integer extrema, date/timestamp layouts, multi-column
  boundaries, null canonicalization, and zeroed unused bytes;
- the same float canonicalization as `GroupValue` for signed zero and NaN payloads;
- exact Decimal128 and UTF-8 fallback behavior;
- occupied compact probes do not materialize a full key, while a vacant probe materializes exactly
  one;
- a rejected compact growth guards before either lazy constructor and leaves buckets, compact keys,
  full keys, states, and current ownership unchanged;
- ADR 435's paired full-key finalization remains unchanged.

The architecture test requires the explicit 16-byte layout, compact entry seam, and aggregate-route
call site, and rejects query-number branding.

## Schema-v3 q13 ledger

ADR 436's off-by-default profile is versioned to schema 3. It now distinguishes compact lookups,
generic lookups, row encodes, and full-key materializations; the four-phase probe ledger and its
reconciliation identities are retained. When the environment variable is absent, no profile sink or
state is constructed.

The inner 15,337,604-row / 1,500,000-group fold is selected independently from q13's 46-group outer
fold. All five fresh children report:

- schema version 3;
- 15,337,604 compact lookups, zero generic lookups, and 15,337,604 row encodes;
- exactly 1,500,000 full-key materializations;
- 13,837,604 occupied plus 1,500,000 vacant lookups;
- the unchanged exact 18,933,731 four-phase hash-call reconciliation;
- zero counter or histogram overflow.

| child | hash probes | max probe | fold ms | current / peak bytes | projected growth peak | finalize scratch |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 21,953,072 | 40 | 6,142.517 | 630,995,176 | 562,037,288 | 48,000,000 |
| 2 | 21,961,167 | 45 | 6,131.415 | 630,995,176 | 562,037,288 | 48,000,000 |
| 3 | 21,975,997 | 41 | 6,144.230 | 630,995,176 | 562,037,288 | 48,000,000 |
| 4 | 21,963,153 | 35 | 6,166.536 | 630,995,176 | 562,037,288 | 48,000,000 |
| 5 | 21,953,882 | 43 | 6,159.792 | 630,995,176 | 562,037,288 | 48,000,000 |

The key-materialization reduction is real but not free. Against ADR 435, final current ownership rises
from 597,440,688 to 630,995,176 bytes (**+33,554,488 bytes**), and maximum projected growth peak rises
from 511,705,264 to 562,037,288 bytes (**+50,332,024 bytes**). Finalize scratch remains 48,000,000
bytes. These three residency domains are recorded separately and are not cross-summed as simultaneous
maxima.

## Probe distribution and prospective event-floor gate

The exact histograms have bins `0..=1023` plus one overflow bucket. Overflow is zero in all phases and
children, so p99.9 is measured rather than bounded.

| phase | calls / child | p99 | p99.9 | max range |
|---|---:|---|---|---|
| primary occupied | 13,837,604 | 6 / 6 / 6 / 6 / 6 | 11 / 11 / 11 / 11 / 11 | 35-43 |
| primary vacant | 1,499,999 | 8 / 8 / 8 / 8 / 8 | 14 / 14 / 14 / 14 / 14 | 35-45 |
| vacant placement | 1,500,000 | 8 / 8 / 8 / 8 / 8 | 14 / 14 / 14 / 14 / 14 | 35-45 |
| rehash | 2,096,128 | 3 / 3 / 3 / 3 / 3 | 5 / 6 / 6 / 6 / 6 | 13-18 |

The predeclared rule starts at each requested threshold (`>64`, `>32`, `>16`, `>8`) and walks toward
a coarser threshold until every child has at least 100 raw events. The resulting unique gates are:

| phase / selected rate | five raw event counts | spread | gate |
|---|---|---:|---|
| occupied `>16` | 1,589 / 1,658 / 2,068 / 1,764 / 1,959 | **0.27154** | **fail** |
| occupied `>8` | 40,595 / 39,981 / 40,803 / 40,876 / 40,219 | 0.02205 | pass |
| vacant `>16` | 726 / 773 / 792 / 706 / 762 | 0.11286 | pass |
| vacant `>8` | 11,931 / 11,990 / 12,060 / 12,039 / 12,134 | 0.01686 | pass |
| placement `>16` | 726 / 773 / 791 / 706 / 762 | 0.11155 | pass |
| placement `>8` | 11,931 / 11,989 / 12,059 / 12,039 / 12,134 | 0.01686 | pass |
| rehash `>8` | 122 / 123 / 134 / 140 / 137 | 0.13433 | pass |

The occupied `>16` failure is retained red. Each child has far more than the 100-event floor, so the
rule is statistically applicable; the threshold is not loosened after observing the data. p99,
p99.9, total probes, and all other selected rates remain stable, but they do not overwrite this
committed subrail.

## Inclusive lookup/equality cycle-frame proof

The operator restored `kernel.perf_event_paranoid=1` before the accepted profiles. Five fresh q13
children then ran the accepted symbol-rich binary under:

```text
perf record --freq 99 --call-graph dwarf,16384 --event cpu_core/cycles/P
```

The generic-fold ledger was disabled. Every child passed with the exact fingerprint, digest, schema,
46 rows, and 9,152,992 spill bytes. Each profile has approximately 2.46K cpu-core samples and zero
lost samples. Kernel frames remain unresolved under `kptr_restrict=1`; Toron userspace symbols resolve
and are the only frames used here.

Before sampling, the comparison set was locked as the grouped-table lookup root plus its inlined hash
and key-equality work and any resolved out-of-line key-equality children. Fold updates, key
construction/allocation, finalization, and unrelated hash tables are excluded. In both binaries,
hashing and equality resolve inline inside the table's lookup machine-code owner, so the union is
reported by `find_bucket_in` before and `find_compact_bucket_in` after. This is a semantic frame-set
comparison across two implementations, not credit for the old symbol disappearing.

| child | old share | old event cycles | old lookup cycles | new share | new event cycles | new lookup cycles | reduction |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 21.28% | 131.649B | 28.015B | 15.32% | 123.792B | 18.965B | 32.30% |
| 2 | 22.91% | 131.521B | 30.131B | 14.02% | 124.435B | 17.446B | 42.10% |
| 3 | 22.86% | 128.661B | 29.412B | 14.02% | 123.832B | 17.361B | 40.97% |
| 4 | 22.92% | 129.068B | 29.582B | 14.85% | 124.522B | 18.491B | 37.49% |
| 5 | 22.50% | 129.383B | 29.111B | 14.06% | 123.346B | 17.342B | 40.43% |

The old and new share ranges do not overlap. The median share falls from 22.86% to 14.06%, a 38.50%
reduction; multiplying each share by its profile's approximate event count gives median lookup
contributions of 29.412B and 17.446B cycles, a 40.69% reduction. The new share spread is 0.09246.

Child 1 annotation covers 376 local samples. The bucket-index load is 39.62% of the local frame and
the hottest fixed-key comparison load is 53.74%; hash setup is in the residual low-single-digit
region. Equality remains the largest local operation, but its global lookup/equality frame is
materially smaller in all five children. Profiled execute times are diagnostic and are not reused as
the speed result.

## Unprofiled ADR 356-shaped q13 run

The parent command ran one warmup and seven fresh measured children. All seven measured rows agree on
the exact fingerprint/digest/schema/rows/spill tuple.

| measured run | execute ms | max RSS KiB |
|---:|---:|---:|
| 1 | 21,446.553 | 1,005,136 |
| 2 | 21,345.469 | 1,004,960 |
| 3 | 21,452.478 | 1,005,136 |
| 4 | 21,423.237 | 1,004,968 |
| 5 | 21,436.259 | 1,004,900 |
| 6 | 21,476.476 | 1,005,152 |
| 7 | 21,371.319 | 1,004,916 |

| statistic | ADR 435 before | ADR 437 after |
|---|---:|---:|
| minimum | 22,061.912 ms | 21,345.469 ms |
| median | 22,257.249 ms | **21,436.259 ms** |
| maximum | 22,445.267 ms | 21,476.476 ms |
| spread `(max-min)/median` | 0.01722 | **0.00611** |
| Toron / sealed DuckDB q13 median | 16.11x | 15.51x (context only) |

The applicable band is `max(0.10, 2 * max(0.01722, 0.00611)) = 0.10`. The median reduction is
3.69%, below the band. The sample is stable and directionally faster, but it does not establish a
material throughput win and does not replace the sealed ADR 356 article baseline.

## Empirical route census and affected correctness

All 19 ADR 436 generic-fold cells ran once in fresh processes with cell-local schema-v3 receipt
directories. Receipt counters, not SQL inspection, select the compact subset. Every route run matches
its ADR 435 fingerprint, digest, schema, row count, and spill bytes exactly.

| cell | fold route(s) | persisted compare in this cut |
|---|---|---|
| q01 | generic | not re-run |
| q02 | compact | pass |
| q03 | generic | not re-run |
| q04 | generic | not re-run |
| q05 | generic | not re-run |
| q07 | generic | not re-run |
| q08 | compact | pass |
| q09 | generic | not re-run |
| q10 | generic | not re-run |
| q11 | compact | pass |
| q12 | generic | not re-run |
| q13 | compact + compact | pass |
| q15 | compact + compact | pass |
| q16 | generic + generic | not re-run |
| q17 | compact | pass |
| q18 | generic | not re-run |
| q20 | generic | not re-run |
| q21 | generic | not re-run |
| q22 | generic | not re-run |

The compact set is therefore **6/6 pass@compare**. Generic routes are unchanged fallback controls;
they are not silently promoted from reach rows to new correctness rows. The three ADR 435 bypass
controls `{q06, q14, q19}` are outside this census. No full-board redux is claimed.

## q13 survival rail

The independent persisted strict q13 command passes under 4 GiB / no swap:

```text
tpch_correctness query=q13 sf=10 mode=strict phase=compare status=pass reason=none elapsed_ms=32542.3
```

It records 1,009,320 KiB maximum RSS, 23,457 voluntary context switches, 3,079 involuntary context
switches, zero major faults, zero filesystem-input blocks, zero swaps, exit status 0, and clean spill
scratch.

## Verification and disclosed rejected attempts

Accepted source gates:

- `cargo fmt --check` - pass;
- `cargo check --all-targets` - pass;
- `cargo test -p toron --lib` - 1,303 passed / 0 failed;
- `cargo test -p toron --test architecture_test` - 2,082 passed / 0 failed;
- the compact encoding/materialization/growth tests and deep-recursion guard - pass;
- serial whole-workspace `cargo test --workspace --all-targets --no-fail-fast` - pass;
- `cargo test -p toron-tpch --features polars-sql-reference --no-fail-fast` - 311 passed /
  0 failed / 2 ignored;
- `git diff --check` - clean;
- `exec.rs` remains 7,499 lines;
- no `.perf.data` file is tracked.

Four operational failures are excluded rather than hidden:

1. Two release-build attempts did not produce an acceptable measurement binary: one inherited symbol
   stripping; another was killed by the build helper's 8 GiB `MemoryHigh`. A one-job cached link
   without the artificial build cap produced the accepted symbol-rich binary above. No rejected
   binary contributes a live number.
2. The first whole-workspace attempt exhausted disk while rebuilding a generated 651 GiB
   `target/debug`. Only generated `target/debug` was removed. The canonical one-job serial rerun then
   passed. The accepted release binary and evidence stayed byte-identical.
3. In an earlier parallel workspace run, Wendelon's
   `dispatcher_backpressures_cross_tenant_read_when_actor_limit_is_saturated` observed a shared
   metric from a neighboring test. The exact test passed alone, the full Wendelon library passed
   serially (1,063/0), and the final serial whole-workspace run passed. The parallel row is rejected
   as cross-test interference.
4. The first perf permission probe found `kernel.perf_event_paranoid=4` and failed before q13
   launched. Its zero-byte placeholders were removed. After the operator restored the reviewed value
   of 1, all five accepted profiles were collected from scratch; no blocked-attempt artifact or number
   enters the comparison.

## First-implementation acceptance status and steer

| acceptance item | status |
|---|---|
| Eq-faithful compact encoding and exact fallback | pass |
| lazy full-key materialization and pre-allocation mutation guard | pass |
| schema-v3 five-child reconciliation | pass, 5/5 |
| inclusive lookup/equality sampled cost falls | pass, 5/5; median contribution -40.69% |
| mechanical tail-event-floor stability | **fail: occupied `>16` spread 0.27154** |
| unprofiled q13 sample is invariant and stable | pass |
| material q13 speedup beyond the 10% band | **not established: 3.69%** |
| empirical compact subset pass@compare | pass, 6/6 |
| q13 4 GiB/no-swap survival | pass |
| complete source/test/hygiene gates | pass |

The first implementation established that compact lookup/equality cost fell materially and
consistently, but did not justify landing the mechanism. Review therefore selected the one sole-key
revision recorded at the top of this document. That revision fixed ownership but again missed the
end-to-end bar, triggering the predeclared removal rule. ADR 437 moves no threshold, erases no red
tail, and promotes no sub-band timing shift into an article speed claim.
