# ADR 441 results - q18 membership-chain throughput diagnosis

## Verdict

ADR 441 is **conclusive**. The current strict sf=10 membership-chain root is dominated by the
**scan/spill domain**, and the stable dominant exclusive leaf is
**`terminal_build.partition_spill_write`**:

- scan/spill accounts for **78.55%-82.26%** of the root in the five accepted children;
- terminal-build partition/spill write alone accounts for **39.80%-41.94%**;
- the terminal build scans and repartitions **1,319,693,144 rows** across 22 groups—exactly
  **59,986,052 x 22**, because the full terminal-build input is revisited for every group;
- the corresponding child-1 storage scan is 6.74 seconds and partition/spill write is 92.73 seconds;
- the terminal join then spends 73.22 seconds reading the resulting pages;
- all six domain-share spreads and the dominant-leaf spread are below the predeclared 0.25 ceiling;
- the in-root residual is **4.18%-7.85%**, below the fixed 10% ceiling in every child.

The diagnosis rejects the tempting membership-pushdown premise. Membership lookup/filter is only
about 0.13% of the root, despite the 59,986,052-to-4,368 row reduction. Placement was not cost.

ADR 442 receives one mechanism: **build and partition the terminal-build reservoir once, then reuse
that terminal-build spill across the 22 terminal groups instead of rescanning and repartitioning the
59,986,052-row terminal-build input for each group**. The mechanism is selected by the measured
`(scan_spill, terminal_build, partition_spill_write)` triple. It does not bundle membership movement,
probe-index work, or a cap increase.

ADR 441 changes no execution mechanism and makes no speed claim. The strict board remains 22/22.

## Conditions and provenance

| item | value |
|---|---|
| sealed engine baseline | ADR 440 implementation `d223ae0` |
| committed ADR 441 plan | `3548468` |
| measurement date | 2026-07-16 CDT |
| Phase-0 release binary SHA-256 | `da549e76dfebfdbb51ebdea4de106b31796a6525583f6b8fb2ec99f8a4f777f1` |
| accepted ledger release binary SHA-256 | `2f1fdd3a4fe756a2f2003c188f5d0967d9b4ee7a5f03dbc880a3f58cbbaf4cab` |
| symbol-rich perf binary SHA-256 | `411dae24ed2ff06aeec2c5405678ccc634e26572f34d0be1ae0930d572afbfb4` |
| sf=10 manifest SHA-256 | `c04dd8dc81df705578abda1839f7e2f8ca39d2e6b4c43bcfef2aed5f77c9aa8f` |
| data | persisted strict sf=10 Parquet under `target/tpch/data` |
| throughput scope | `MemoryHigh=infinity`, `MemoryMax=infinity`, `AllowedCPUs=0-1`, `CPUQuota=150%`, `TasksMax=512`, `RuntimeMaxSec=3600` |
| survival scope | 4 GiB/no-swap, `TasksMax=512`, `RuntimeMaxSec=600` |
| host | 12th Gen Intel Core i9-12900K; 16 cores / 24 threads; CPUs 0-1 are SMT siblings on P-core 0; 61 GiB RAM; no swap |
| kernel / perf / governor | Linux `7.0.0-27-generic`; perf `7.0.6`; powersave; `perf_event_paranoid=1` |
| profiler env var | `TORON_MEMBERSHIP_CHAIN_PROFILE_DIR` |
| accepted ledger roots | `target/tpch/adr441/cohort/child1..child5` |
| qualitative perf root | `target/tpch/adr441/perf` |

Every accepted child uses the same ordinary release binary and passes with:

- plan fingerprint `d8b60e3a03bfaf79`;
- Arrow IPC digest
  `ArrowIpcDigest([73, 213, 103, 195, 117, 240, 147, 86, 208, 176, 255, 142, 61, 197, 201, 110, 167, 202, 161, 192, 127, 89, 16, 105, 113, 1, 240, 35, 50, 13, 112, 216])`;
- schema fingerprint `fnv1a64:6bcce34314d4472c`;
- 100 external rows;
- 4,380,793,312 spill bytes.

Join-spill scratch was fresh before and empty after every accepted child. Profiled elapsed values are
diagnostic and are not substituted into the retained ADR 356 throughput baseline.

## Phase 0 - current live path

The plan-only release child completed in 217,780.023 ms execute time (3:41.89 wall), with 1,256,916
KiB peak RSS, no swap, the invariant result tuple above, and the historical schedule receipt:

```text
membership rows=624 bytes=5376
presence entries=624 bytes=16384
chain input rows=59986052
membership matches=4368
finalized=true
```

The completing owner is
`try_streaming_projected_left_semi_grouped_aggregate` -> committed membership-first branch ->
`try_streaming_reoriented_inner_chain_scan_probe_base_to_sink` -> grouped finalization. The accepted
ledger records this geometry in all five children:

| stage | replay ID | partitions | groups | probe columns first |
|---|---:|---:|---:|---|
| base | 0 | 2 | 2 | false |
| terminal | 1 | 64 | 22 | true |

The outer Sort/Limit remains outside the ledger root.

## Implementation

The new query-neutral `MembershipChainProfile` lives in
`exec/join/membership_chain_profile.rs`. It is constructed only after the membership-first shape has
been selected and committed. Missing or empty configuration returns `None`, with no state, directory,
sink, or diagnostic clock.

Schema version 1 owns:

- nine phases by fifteen operations in a fixed phase/operation matrix;
- one release-checked active exclusive leaf, rejecting overlap, nesting, or an outside-root leaf;
- checked root, leaf-sum, residual, row, byte, and spill-role arithmetic;
- six reporting domains without cross-summing storage counters and elapsed time;
- base and terminal replay/partition/group orientation;
- atomic temporary-write/rename publication with propagated create, serialize, write, and rename
  failures.

The root begins after the live shape fences at the selected/committed point and ends after grouped
finalization. `execute_ms - root` is reported separately. Existing task telemetry and the legacy
membership schedule receipt are unchanged.

The profiler is threaded only through the admitted membership-chain helpers. Storage scan, partition
spill write, spill read/decode, index build, lookup/selection, Arrow materialization, presence filter,
exchange, grouped fold, lifecycle, and cleanup use disjoint batch/helper boundaries. Release behavior
rejects timer overlap rather than silently double-counting it.

The independent q18 oracle now counts lineitems per order group and checked-adds those rows only when
the group passes `SUM(l_quantity) > 300`. Dense/frugal parity and threshold/boundary tests cover the
new counter; the value comparator is unchanged.

The semantic-module allowlist includes the new file. `exec.rs` remains 7,499 lines and the join root
remains 3,717 lines.

## Independent row-flow receipt

The explicit persisted sf=10 generator-oracle run independently reports:

```text
qualifying_orders=624
selected_lineitem_rows=59986052
qualifying_lineitem_rows=4368
```

Every accepted engine child then satisfies the full independent identity:

| independent/harness quantity | engine quantity | rows | status |
|---|---|---:|---|
| qualifying orders | presence entries | 624 | exact, 5/5 |
| selected lineitems | sink input | 59,986,052 | exact, 5/5 |
| qualifying lineitems | sink matches | 4,368 | exact, 5/5 |
| qualifying orders | root aggregate | 624 | exact, 5/5 |
| persisted final result | post-Sort/Limit | 100 | exact, 5/5 |

This is the load-bearing guard against treating engine instrumentation as its own oracle.

## Five-child exclusive ledger

The five accepted children are fresh processes under the ADR 356 compute profile. `outside-root` is
`execute_ms * 1000 - root_span_micros`; it is not folded into the in-root residual.

| child | execute ms | root us | residual | scan/spill | dominant leaf share | outside-root us | peak RSS KiB |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 221,090.767 | 221,090,444 | 0.041806 | 0.822612 | 0.419443 | 323.037 | 1,226,720 |
| 2 | 216,251.775 | 216,251,317 | 0.042933 | 0.820285 | 0.417016 | 457.792 | 1,255,744 |
| 3 | 225,461.835 | 225,461,536 | 0.078475 | 0.785454 | 0.397994 | 299.037 | 1,251,372 |
| 4 | 223,135.424 | 223,135,100 | 0.072005 | 0.795884 | 0.404369 | 323.834 | 1,255,692 |
| 5 | 214,548.536 | 214,548,207 | 0.041776 | 0.819770 | 0.417365 | 329.304 | 1,223,388 |

The same dominant domain (`scan_spill`) and same dominant leaf
(`terminal_build.partition_spill_write`) appear in all five children.

| normalized quantity | minimum | median | maximum | spread `(max-min)/median` | gate |
|---|---:|---:|---:|---:|---|
| membership | 0.014623 | 0.015114 | 0.015606 | 0.065012 | pass |
| scan/spill | 0.785454 | 0.819770 | 0.822612 | 0.045328 | pass |
| index/probe | 0.044324 | 0.045698 | 0.047143 | 0.061671 | pass |
| materialization | 0.031343 | 0.032223 | 0.032410 | 0.033126 | pass |
| sink/fold | 0.001303 | 0.001352 | 0.001408 | 0.078000 | pass |
| lifecycle | 0.040468 | 0.041337 | 0.042387 | 0.046422 | pass |
| dominant leaf | 0.397994 | 0.417016 | 0.419443 | 0.051435 | pass |

Every spread is below 0.25. Every residual is below 0.10.

### Child-1 phase/operation attribution

The live nonzero leaves show why the selected mechanism is terminal-build reuse rather than membership
movement:

| phase / operation | us | calls | rows |
|---|---:|---:|---:|
| membership subtree execute | 3,341,476 | 1 | 624 |
| lifecycle runtime prepare | 9,057,322 | 2 | 0 |
| base build scan + spill write | 203,061 | 2,931 | 3,000,000 |
| base probe scan + spill write | 1,512,406 | 29,299 | 30,000,000 |
| base join read + index + lookup | 4,941,841 | 1,838 | 33,000,000 |
| base join Arrow/post-read materialize | 2,799,502 | 1,834 | 15,000,000 |
| intermediate spill write + read | 6,510,450 | 1,896 | 30,000,000 |
| terminal build storage scan | 6,737,818 | 1,288,804 | 1,319,693,144 |
| **terminal build partition/spill write** | **92,734,863** | **1,288,782** | **1,319,693,144** |
| terminal join spill read/decode | 73,224,400 | 64 | 59,986,052 |
| terminal join index + lookup | 6,161,291 | 1,920 | 30,000,000 |
| terminal join Arrow materialize | 4,198,351 | 8,239 | 59,986,052 |
| sink presence + exchange + fold | 298,282 | 9,451 | 59,994,788 |

The grouped rows in aggregate labels above are operation-counter sums, not claims of unique logical
rows. The exact ledger matrix remains the authoritative receipt.

## Spill-role reconciliation

The role values are invariant in all five children:

| spill owner | bytes |
|---|---:|
| base build | 23,185,696 |
| base probe input | 313,900,784 |
| intermediate | 754,561,664 |
| terminal build | 3,289,145,168 |
| **checked total** | **4,380,793,312** |
| engine telemetry | 4,380,793,312 |

The role sum is exact in every child. No fifth spill owner was hidden or forced into an existing role.

## Qualitative external perf child

One separate ledger-disabled symbol-rich child ran with:

```text
perf record -e cpu_core/cycles/P --freq 99 --call-graph dwarf,16384
```

It completed with the invariant result tuple, 21,163 samples, and zero lost samples. Leading flat
frames were:

| frame | self cycles |
|---|---:|
| `SpilledInnerJoinBuild::touch_cache_entry` | 18.38% |
| `partition_bucket_if_non_null_with_scratch` | 7.63% |
| `partition_bucket_with_scratch` | 6.99% |
| `encode_partition_key_value` | 4.11% |
| allocator `malloc` | 3.09% |
| `memset` | 2.80% |
| `visit_loaded_partition_work` | 2.63% |
| `repartition_exchange_batch` | 2.08% |

This qualitatively corroborates partition/write and spill-cache bookkeeping. It does not identify the
exclusive phase by itself, is not part of the five-child stability gate, and its 231,052.385 ms
profiled execute time is not compared with the unprofiled baseline. Kernel symbol visibility was
restricted; no kernel-frame conclusion is claimed.

## Disabled parity and 4 GiB survival

The final source-matching release binary is byte-identical to the five-child binary. A separate
profile-disabled child records the same fingerprint, digest, schema, 100 rows, 4,380,793,312 spill
bytes, and ordinary execution telemetry as the enabled children; no profile directory or sink is
constructed. Join-spill scratch is empty afterward.

The persisted strict sf=10 correctness control also passes under 4 GiB/no-swap:

```text
tpch_correctness query=q18 sf=10 mode=strict phase=compare status=pass elapsed_ms=223556.7
```

It records 1,231,652 KiB peak RSS, no swap, exact row/spill reconciliation, and clean scratch. This is
one q18 survival control, not a full-board redux.

## Tests and hygiene

- formatter and `git diff --check`: clean;
- focused profiler locks: disabled/empty construction, exact matrix and residual, overlap/outside-root
  rejection, checked spill mismatch, atomic publication, and loud snapshot failure;
- independent q18 oracle tests: dense/frugal parity and boundary/counter cases green; explicit sf=10
  receipt reproduces 624 / 59,986,052 / 4,368;
- deep-recursion reproducer
  `reoriented_inner_chain_grouped_aggregate_matches_materialized_float64_and_decimal_order`: pass;
- `scripts/cargo-cgroup test -p toron --lib`: 1,312 passed, 0 failed;
- `scripts/cargo-cgroup test -p toron --test architecture_test`: 2,085 passed, 0 failed;
- `scripts/cargo-cgroup test -p toron --no-fail-fast`: every unit, integration, and doc-test binary
  green;
- `scripts/cargo-cgroup test -p toron-tpch`: every binary green (44 lib tests, 176 correctness tests,
  and the existing labeled ignores);
- `scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast`: every
  binary green;
- architecture guard locks post-fence construction, the exclusive phase/operation surface, atomic
  publication, ADR 176 allowlisting, and absence of query branding;
- `exec.rs` remains 7,499 lines; join root remains 3,717 lines;
- `git ls-files '*.perf.data'` is empty; the 335 MiB diagnostic perf artifact remains under ignored
  `target/` only;
- join-spill scratch is empty.

## Discarded attempt

The first symbol-rich release link was run with an inappropriate 8 GiB build-time `MemoryHigh` and was
terminated by systemd-oomd after 12:42 at an 8.2 GiB peak. It produced no usable binary or measurement.
The link was rerun with non-limiting build memory; only that completed symbol-rich binary contributes
to the qualitative perf receipt. No failed or pre-final evidence is mixed into the accepted cohort.

An initial five-child ledger cohort used ordinary release hash `f55785f5...`. A post-cohort oracle-test
strengthening changed the source hash; after the production helper was restored, the final release
rebuild still produced hash `2f1fdd3...`. The implementation plan requires exact final-binary
provenance, so the whole five-child ledger cohort, disabled child, and 4 GiB control were rerun. Only
the `2f1fdd3...` receipts under `cohort/`, `disabled/`, and `survival/` are accepted. The superseded
receipts remain under `cohort-pre-final/` and `survival-pre-final/` for audit but contribute no number
to this document. The independently rebuilt symbol-rich binary remained byte-identical at
`411dae24...`, validating its existing perf receipt.

## Acceptance and forward arc

| acceptance item | status |
|---|---|
| live owner, replay geometry, fingerprint, digest, schema, rows, spill | pass |
| exclusive root arithmetic; residual <= 10% | pass, 5/5 |
| independent row-flow identities | pass, 5/5 |
| exact four-role spill sum | pass, 5/5 |
| same dominant domain and leaf | pass, scan/spill / terminal-build partition-spill write, 5/5 |
| every domain and dominant-leaf spread <= 0.25 | pass |
| enabled/disabled parity | pass |
| 4 GiB/no-swap q18 correctness | pass |
| no mechanism, cap change, query branch, or speed claim | pass |

ADR 442 owns the one-pass, reusable terminal-build spill mechanism. q05 remains the next large-wall
diagnosis. q17 remeasurement remains a separate evidence chore. The implementation and this results
document remain uncommitted for independent review.
