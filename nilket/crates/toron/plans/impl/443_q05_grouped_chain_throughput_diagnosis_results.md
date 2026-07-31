# ADR 443 results - q05 grouped-chain throughput diagnosis is honestly red

## Verdict

ADR 443 does **not** meet its load-bearing acceptance gate. The required five-child ledger does not
name the same dominant leaf in all five children:

- `reservoir_read_probe` leads children 1, 3, and 4;
- `reservoir_partition_spill_write` leads children 2 and 5.

The implementation is sound and the measurement is strongly informative, but the promoted
leaf-only discriminator does not permit a write/read pair to substitute for one stable leaf. No ADR
444 mechanism is selected and no speed claim is made. The off-by-default instrument, independent
oracle census, and narrower reservoir-pair finding are retained as an **honest-red diagnosis
substrate**; sealing them does not turn the failed discriminator green.

The evidence does establish a narrower, reusable finding:

- reservoir write plus read/probe owns **86.64%-88.08%** of the full grouped-chain root in every
  child;
- read/probe is stable in absolute time (**77.76-78.55 s**, spread **0.01018**) and has the larger
  median (**77.98 s**);
- write is the cohort's variance source (**70.10-91.73 s**, spread **0.28821**); removing write from
  each root leaves **100.56-101.42 s**, spread **0.00850**;
- the result tuple, independent row-flow census, and two-domain spill reconciliation are exact in all
  five children.

That is enough to reject every non-reservoir branch of ADR 443's mechanism menu. It is not enough to
choose between a write-side reservoir cut and a read-side/index cut under the predeclared rule. ADR
444 is therefore a **refinement diagnosis** that separates the reservoir-write variability and the
stable read/probe wall before proposing a mechanism.

The strict board remains **22/22**.

## Conditions and provenance

| item | value |
|---|---|
| sealed engine baseline | ADR 442 rejection on the ADR 441 engine; plan parent `f0b7678` |
| committed ADR 443 plan | `448fd35` |
| measurement date | 2026-07-16 CDT |
| accepted ordinary release binary SHA-256 | `f4982f4a7ab74cbecdbf1fdf37f9996fb9c11f41f87489b19fdd1109ae4ca9eb` |
| qualitative symbol-rich binary SHA-256 | `615f5ea3d3b2523463849dd86b6aca3484ceaaaad091a90e25a144f04bb26fd1` |
| sf=10 manifest SHA-256 | `c04dd8dc81df705578abda1839f7e2f8ca39d2e6b4c43bcfef2aed5f77c9aa8f` |
| data | persisted strict sf=10 Parquet under `target/tpch/data` |
| throughput scope | `MemoryHigh=infinity`, `MemoryMax=infinity`, `AllowedCPUs=0-1`, `CPUQuota=150%`, `TasksMax=512`, `RuntimeMaxSec=3600` |
| survival scope | `MemoryHigh=4G`, `MemoryMax=4G`, no swap, `TasksMax=512`, `RuntimeMaxSec=600` |
| host | Intel i9-12900K, 16 cores / 24 threads, 61 GiB RAM, no swap |
| kernel / governor | Linux `7.0.0-27-generic`; powersave |
| perf policy | `perf_event_paranoid=1`; `kptr_restrict=1` |
| new receipt surface | `TORON_GROUPED_CHAIN_PROFILE_DIR` |
| preserved ADR 429 surface | `TORON_GROUPED_RESERVOIR_RESIDENCY_PROFILE_PATH`, schema 1 |
| evidence root | ignored `target/tpch/adr443/` |

The five accepted ledger children use the ordinary binary SHA. The external perf child uses the
separately identified symbol-rich binary and is qualitative only. No timing or profile from one
binary is pooled with the other.

## Implementation

The new off-by-default `GroupedChainProfile` is constructed only after the top-level grouped-chain
shape is admitted. It is held as `Option<Box<_>>`; when the environment variable is absent or empty,
there is no profiler state, sink, or clock.

The root starts after structural admission in
`try_streaming_reoriented_inner_chain_grouped_aggregate` and ends after grouped finalization and
runtime cleanup. Nine mutually exclusive leaves partition it:

1. runtime preparation/build;
2. base accumulation;
3. reservoir partition/spill write;
4. base concat;
5. reservoir read/probe;
6. suffix join;
7. grouped fold;
8. finalization;
9. cleanup.

The profiler rejects nested/outside-root timers, records its residual separately, reconciles spill
at explicit root/stage boundaries, and atomically publishes one JSON receipt through a temporary file
and rename. Publication and reconciliation failures propagate as execution errors.

The ADR 429 residency profiler was not changed. Architecture tests pin its environment variable and
schema-1 surface while also checking the new profiler's post-fence construction, root/leaf hooks,
atomic publication, query-neutral naming, and absence of query branding.

## Phase 0 - path and boundary mapping

The accepted current-binary Phase-0 child passed with:

- plan fingerprint `757d7c625669b21a`;
- Arrow IPC digest
  `ArrowIpcDigest([91, 92, 150, 245, 67, 32, 163, 17, 163, 188, 220, 35, 218, 160, 201, 31, 203, 54, 179, 238, 52, 151, 29, 167, 39, 211, 38, 101, 64, 253, 52, 158])`;
- schema fingerprint `fnv1a64:3f31e3a6be8148ff`;
- 5 output rows;
- 5,353,985,328 task spill bytes;
- 196,678.677 ms execute time.

It confirmed the live four-stage chain, reservoir replay 1, 64 partitions, and 22 groups.

Two earlier Phase-0 instrumentation attempts failed loudly rather than emitting a false receipt. Both
reported the same missing **83,669,472-byte** spill role. Their artifacts are retained under
`phase0-pre-widen/` and `phase0-pre-base-boundary/`. The failures forced the physical-boundary map to
locate those bytes in the base visitor rather than in pre-base runtime preparation. The accepted
ledger therefore measures `task_spill_at_base_end - task_spill_at_root_start` as the earlier
runtime/base role; it does not preserve the plan's unverified assumption about which side of that
boundary owns the bytes.

## Independent q05 stage census

`strict_q5_oracle` now carries checked, generator-derived stage counters. The explicit ignored sf=10
evidence test ran independently and pinned:

| oracle counter | rows |
|---|---:|
| dated orders | 2,275,919 |
| all lineitems | 59,986,052 |
| dated-order lineitems | 9,103,367 |
| suppliers | 100,000 |
| same-nation rows | 364,380 |
| nations | 25 |
| ASIA regions | 1 |
| ASIA selected rows | 72,985 |
| final groups | 5 |

The ordinary small-scale oracle test also locks the monotone census boundaries, checked increments,
ASIA selected-row equality, and 25-nation / one-ASIA-region invariants. The explicit sf=10 oracle run
completed in 34.41 s.

## Five-child ledger cohort

Every child passed with the exact Phase-0 fingerprint, digest, schema, 5 rows, and 5,353,985,328 spill
bytes. Scratch was empty after every scope.

### Wall distribution

| child | execute ms | root ms | max RSS KiB | dominant leaf |
|---:|---:|---:|---:|---|
| 1 | 175,861.548 | 175,861.395 | 3,052,336 | read/probe |
| 2 | 192,678.882 | 192,678.729 | 3,051,560 | write |
| 3 | 171,976.421 | 171,976.268 | 3,051,776 | read/probe |
| 4 | 170,663.956 | 170,663.802 | 3,051,856 | read/probe |
| 5 | 186,727.252 | 186,727.096 | 3,051,924 | write |

Root min/median/max is **170,663.802 / 175,861.395 / 192,678.729 ms**, spread **0.12518**. The
outside-root remainder is approximately 0.15 ms per child; the top-level root explains the execute
wall rather than orphaning preparation or accumulation.

### All nine leaf shares

Percentages are normalized by each child's root. `resid` is the derived in-root residual, not a
leaf.

| child | prep | base | write | concat | read/probe | suffix | fold | finalize | cleanup | resid |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 7.9578% | 3.4271% | 42.6793% | 0.0085% | **44.3142%** | 0.8487% | 0.0072% | 0.000005% | 0.7441% | 0.0131% |
| 2 | 7.2818% | 3.1519% | **47.6101%** | 0.0078% | 40.4701% | 0.7685% | 0.0067% | 0.000005% | 0.6916% | 0.0114% |
| 3 | 8.1480% | 3.5330% | 41.1549% | 0.0092% | **45.5063%** | 0.8662% | 0.0077% | 0.000006% | 0.7614% | 0.0132% |
| 4 | 8.1550% | 3.5264% | 41.0764% | 0.0090% | **45.5613%** | 0.8569% | 0.0076% | 0.000005% | 0.7941% | 0.0135% |
| 5 | 7.4951% | 3.2329% | **45.6856%** | 0.0083% | 42.0669% | 0.7944% | 0.0067% | 0.000005% | 0.6979% | 0.0122% |

Every residual is far below the fixed 10% ceiling. The leaf table covers the root, but the bold
entries show the acceptance failure directly: the leading leaf is not stable 5/5.

### Absolute leaf stability and variance attribution

| leaf | median micros | `(max-min)/median` |
|---|---:|---:|
| runtime preparation/build | 13,995,448 | 0.00807 |
| base accumulation | 6,036,706 | 0.00957 |
| reservoir partition/spill write | 75,056,387 | **0.28821** |
| base concat | 15,318 | 0.05712 |
| reservoir read/probe | 77,977,353 | 0.01018 |
| suffix join | 1,483,270 | 0.02031 |
| grouped fold | 12,838 | 0.04915 |
| finalization | 9 | 0.22222 |
| cleanup | 1,309,351 | 0.03969 |

The finalization spread is a 9-11 microsecond noise-floor fact and is not a gating tail. The material
variance belongs to the write leaf. Its normalized-share spread is **0.15309**, and read/probe's is
**0.11489**; both are below 0.25 individually, but neither can be called the cohort's dominant leaf
because their rank flips. Their combined share is stable: 86.99%, 88.08%, 86.66%, 86.64%, and
87.75% (median 86.99%, spread 0.01658).

This pair is reported only as evidence. It does not retroactively add a domain rollup or change the
promoted leaf-only gate.

## Exact spill reconciliation

All five children report the same boundaries and roles:

| quantity | bytes |
|---|---:|
| task spill at root start | 0 |
| task spill at runtime-prep end | 0 |
| task spill at base end | 83,669,472 |
| task spill at reservoir-build end | 5,353,985,328 |
| task spill at reservoir-probe end | 5,353,985,328 |
| task spill at suffix/fold end | 5,353,985,328 |
| task spill at root end | 5,353,985,328 |
| runtime/base spill role | 83,669,472 |
| reservoir spill role | 5,270,315,856 |
| role sum / task delta | **5,353,985,328** |

The root starts at zero, no spill grows after the reservoir build, and
`83,669,472 + 5,270,315,856 = 5,353,985,328` exactly in every child. The independent reservoir-writer
sum also equals 5,270,315,856.

## Exact engine-to-oracle row flow

All five children agree with the independent census:

| stage | build/input rows | probe rows | output rows |
|---|---:|---:|---:|
| base accumulation | 2,275,919 | - | - |
| replay 1 / reservoir | 59,986,052 | 2,275,919 | 9,103,367 |
| replay 2 | 100,000 | 9,103,367 | 364,380 |
| replay 3 | 25 | 364,380 | 364,380 |
| replay 4 | 1 | 364,380 | 72,985 |
| grouped fold | 72,985 | - | 5 groups |

The row identity is therefore independently anchored rather than inferred from the engine receipt.

## Qualitative external perf

One fresh symbol-rich child ran under the same CPU scope, outside every ledger/stability gate. It
passed with the invariant result tuple in 196,435.819 ms and recorded zero lost samples.

The cpu-core flat report's largest resolved userspace frames were:

| frame | cpu-core cycles |
|---|---:|
| `_int_malloc` | 21.40% |
| `SpilledInnerJoinBuild::touch_cache_entry` | 12.46% |
| `memset` | 7.55% |
| `stable_file_hash` | 2.31% |
| Arrow IPC record-batch decode | 2.19% |
| LZ4 compression | 1.80% |
| `memmove` | 1.75% |
| `partition_bucket_with_scratch` | 1.54% |

These frames corroborate a spill write/read reservoir wall, but they do not resolve one leaf over the
other. The external sample therefore does not repair the failed ledger discriminator.

## Off-by-default parity and ADR 429 coexistence

An sf=4 disabled-profile run and an sf=4 run with both the ADR 429 and ADR 443 profilers enabled were
byte-identical on the result tuple:

- fingerprint `757d7c625669b21a`;
- Arrow digest
  `ArrowIpcDigest([135, 244, 202, 169, 46, 252, 198, 233, 86, 83, 202, 78, 204, 152, 249, 49, 224, 210, 252, 52, 166, 74, 0, 132, 194, 2, 220, 143, 117, 72, 151, 117])`;
- schema `fnv1a64:3f31e3a6be8148ff`;
- 5 rows;
- 1,289,941,048 spill bytes.

The disabled run took 35,402.842 ms; the both-enabled run took 36,052.453 ms. Timing is not a parity
claim. The both-enabled run emitted a complete ADR 429 schema-1 residency snapshot and the new
grouped-chain receipt; scratch was empty afterward.

## Four-GiB survival control

The persisted strict sf=10 q05 correctness run passed under 4 GiB/no-swap:

- `pass@compare`, 201,425.8 ms;
- maximum RSS 3,359,120 KiB;
- cgroup memory peak exactly 4 GiB, swap 0;
- profiler receipt published;
- join-spill scratch empty after the scope.

ADR 430's survival contract therefore remains green with profiling enabled.

## Verification

- `cargo fmt --all -- --check` - pass;
- `scripts/cargo-cgroup fmt -p toron -- --check` - pass;
- `scripts/cargo-cgroup check -p toron --all-targets` - pass;
- `scripts/cargo-cgroup test -p toron --lib` - **1,316 passed / 0 failed**;
- `scripts/cargo-cgroup test -p toron --test architecture_test` - **2,086 passed / 0 failed**;
- `scripts/cargo-cgroup test -p toron --no-fail-fast` - all targets pass with zero failures;
- `scripts/cargo-cgroup test -p toron-tpch` - all targets pass with zero failures;
- `scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast` - all
  targets pass with zero failures;
- focused profiler matrix/parity/overlap/spill/boundary/atomic-publication tests - pass;
- focused oracle and explicit ignored sf=10 census tests - pass;
- `exec.rs` remains 7,499 lines; join root is 3,721 lines (four module-wiring lines over the 3,717
  baseline); `git diff --check` is clean; no `.perf.data` is tracked.

## Acceptance, disposition, and forward arc

| acceptance item | status |
|---|---|
| current live owner/path and result tuple | pass |
| nine exclusive leaves and residual <= 10% | pass, 5/5 |
| same dominant leaf in all five children | **fail, read/probe 3 and write 2** |
| dominant leaf normalized-share spread <= 0.25 | not satisfiable as a single-leaf cohort gate; each candidate passes separately |
| delta-form two-domain spill reconciliation | pass, exact 5/5 |
| independent intermediate/final row identities | pass, exact 5/5 |
| qualitative perf corroboration | pass, reservoir work corroborated but not leaf-resolving |
| off-by-default parity and ADR 429 coexistence | pass |
| strict sf=10 4 GiB/no-swap survival | pass |
| no mechanism or speed claim | pass |

This red differs from ADR 436's ultra-rare-tail residual: the failed row here is the primary mechanism
discriminator, not a statistically ill-posed secondary rail. It is not eligible for a post-hoc pair
rollup or a threshold rewrite.

The authorized disposition is to **retain and seal** the query-neutral profiler and independent
oracle census because this thread continues and the next diagnosis needs the same substrate. This is
not the disposition used for rejected mechanisms in ADRs 437 and 442: those state machines had no
forward implementation value after missing their materiality gates, whereas ADR 443's instrument is
off by default, parity-locked, and directly required to refine the unresolved wall.

ADR 443 therefore closes **red**, banking only:

- the profiler and oracle instrument;
- exact spill and row-flow reconciliation;
- rejection of every non-reservoir discriminator branch, because the write/read pair owns
  86.64%-88.08% of the root 5/5;
- localization of q05's wall variance to reservoir write.

ADR 444 is predeclared as another diagnosis, not a mechanism. Before measurement it must:

1. define finer mutually exclusive leaves inside the two co-dominant leaves and declare an
   adjacency/tie rule capable of resolving near peers without retroactively merging them;
2. split reservoir write into at least routing, encoding/compression/encryption, file-write/wait, and
   cache/accounting work so the 0.28821 variance has a named owner;
3. refine the stable approximately 78-second read/probe wall into mechanism-selecting leaves, because
   a stable producer near 44% of root is the most plausible cut able to clear q05's at-least-23.86%
   materiality band;
4. retain ADR 443's exact result, spill, row-flow, parity, and 4 GiB survival rails.

No mechanism may be inferred from the merged reservoir-pair share alone.
