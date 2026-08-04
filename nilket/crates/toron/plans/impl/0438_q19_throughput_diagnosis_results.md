# ADR 438 results - q19 keyed-join / OR-residual / Decimal-SUM throughput diagnosis

## Verdict

ADR 438 is **conclusive as a producer diagnosis and banked by promotion review with one honest-red
acceptance subrail preserved**. The strict sf=10 fused root is dominated by **D1 join probe and output
materialization**, and D1 is in turn dominated by the **`probe_lookup_select`** exclusive leaf in all
five ledger children:

- D1 accounts for **58.12%-58.24%** of the root; `probe_lookup_select` alone accounts for
  **35.32%-35.41%**;
- D2 predicate evaluation is the stable second domain at **19.51%-19.66%**;
- D4 scan/decode/build/spill is **17.31%-17.54%**;
- D3 Decimal expression plus numeric fold is only **0.00315%-0.00377%**;
- the in-root residual is **4.77%-4.81%** in every child, below the predeclared 10% ceiling;
- all normalized-share spreads are below 0.25, and the dominant domain and leaf agree 5/5.

The independent perf cohort supports the same general wall but **fails the literal same-unique-top
category 5/5 rule**. `hashbrown::map::HashMap<K,V,S,A>::get` leads in children 1 and 5; memmove leads in
children 2-4. Their shares are individually stable—generic lookup **13.70%-14.97%**, memmove
**13.56%-14.19%**—but their ordering is not. `materialize_streamed_spilled_inner_matches` is a separate
**7.36%-7.73%** self-frame. All five reports have zero lost samples. No post-hoc combined category and
no replacement cohort is used to turn that red rail green.

The evidence-backed ADR 439 candidate is therefore a **shape-fenced primitive-key spill-index
extension**:
enable the already-landed primitive `Int64` resident index before `rebuild_index_from_pages` on the
partitioned streamed scan-build path when the join key plan is a single non-null-safe `Int64`. Today
that path reconstructs each group into the generic `HashMap<JoinKey, Vec<SpilledBatchRowRef>>`, despite
the primitive index already existing for other spilled-inner paths. This is one mechanism for the
measured `(D1, probe_lookup_select)` pair; it is not q19-branded and does not bundle predicate factoring
or Arrow materialization work.

D2 remains a separately named follow-on if the index extension moves the first wall. The bound residual
has 107 physical nodes and contains three distinct `l_shipmode` nodes and three distinct
`l_shipinstruct` nodes, each referenced once. The common conjuncts are therefore physically repeated
three times, not CSE-shared. That structural repetition is real, but it is not the dominant measured
leaf in this cut.

ADR 438 changes no execution mechanism and makes no speed claim. The ADR 356 q19 baseline remains
83.45 seconds / 116x DuckDB.

## Conditions and provenance

| item | value |
|---|---|
| sealed engine-source baseline | ADR 436 implementation `bed0308` (ADR 437 was rejected and removed) |
| committed ADR 438 plan | `015293c` |
| measurement date | 2026-07-15 CDT |
| accepted symbol-rich release binary SHA-256 | `4b75e4c84bcf8dc952accf7bbf87af3429aa8af7391de04841dc526a6f367408` |
| accepted binary | `target/adr438-profile/release/toron-tpch` (`not stripped`, `.debug_info` and `.symtab` present) |
| sf=10 manifest SHA-256 | `c04dd8dc81df705578abda1839f7e2f8ca39d2e6b4c43bcfef2aed5f77c9aa8f` |
| data | persisted strict sf=10 Parquet under `target/tpch/data` |
| scope | `MemoryHigh=infinity`, `MemoryMax=infinity`, `AllowedCPUs=0-1`, `CPUQuota=150%`, `TasksMax=512`, `RuntimeMaxSec=3600` |
| host | 12th Gen Intel Core i9-12900K; 16 cores / 24 threads; CPUs 0-1 are SMT siblings on P-core 0; 61 GiB RAM; no swap |
| kernel / perf / governor | Linux `7.0.0-27-generic`; perf `7.0.6`; powersave; `perf_event_paranoid=1`; `kptr_restrict=1` |
| accepted evidence roots | `target/tpch/adr438/evidence/perf/child1..5`; `target/tpch/adr438/evidence/ledger/child1..5` |
| profiler env var | `TORON_GLOBAL_FOLD_JOIN_FILTER_PROFILE_DIR` |

Every accepted child uses the same binary SHA and passes with:

- plan fingerprint `65c3c2c0b0a99063`;
- Arrow IPC digest
  `ArrowIpcDigest([253, 54, 72, 128, 190, 45, 198, 7, 153, 127, 37, 175, 196, 190, 44, 105, 166, 96, 62, 18, 87, 70, 149, 246, 201, 35, 150, 91, 14, 116, 137, 213])`;
- schema fingerprint `fnv1a64:677ce2d00a9afbc2`;
- one output row;
- 51,471,552 spill bytes.

Join-spill scratch was empty before and after every accepted scope. Cohort A ran with the ledger absent;
Cohort B ran with perf absent. Their elapsed values are deliberately not compared.

## Implementation

The new `GlobalFoldJoinFilterProfile` is boxed and constructed only when the env var is present and
non-empty. When disabled there is no profiler state, sink, or diagnostic clock. The implementation is
query-neutral and attached only after the live shape passes the global-Decimal-SUM / join-filter fence.

Schema version 1 records:

- the live path, replay ID, probe/build orientation, and field names;
- one entry-to-return root span;
- the eight mutually exclusive leaf spans and checked D1-D4 roll-ups;
- the checked in-root residual and its 10% status;
- predicate input/output, SUM argument, and numeric-fold row counts;
- the residual program's physical-node census and per-node reference multiplicity.

The timer state rejects overlap in release diagnostics. In particular, page reads are D4 and
`arrow_materialize` begins only after pages are available, so D1 and D4 are not cross-summed. Snapshot
publication is atomic temporary-then-rename, and I/O failures propagate as `ExecutionError`.

The initial timer-bearing future enlarged a recursive async body enough to overflow the existing
deep-recursion fixture. The four production calls to `try_visit_streaming_join0_chunks` are now boxed at
their branch boundaries, restoring pointer-sized recursive futures without adding per-batch boxing.
The profiled spilled-inner visitor also lives in `join/streaming.rs`, its semantic owner, rather than
consuming the bounded join root. The join root is 3,713 lines and the `exec.rs` ratchet remains 7,499.
The exact deep-recursion reproducer and the full library suite pass.

## Phase 0 - live dispatch and structural census

The completing branch is:

```text
path=partitioned_streamed_scan_build_scan_probe
replay_id=0
build=part (p_partkey, p_brand, p_container, p_size)
probe=lineitem (l_quantity, l_extendedprice, l_discount, l_partkey,
                l_shipmode, l_shipinstruct)
probe_columns_first=true
```

The path uses six build/spill group operations in every ledger child. Each group creates partitioned
`SpilledInnerJoinBuild` owners, finishes them, and calls `rebuild_index_from_pages`. Unlike other
spilled-inner call sites, this branch does not call `use_compact_resident_index` before that rebuild, so
its single `Int64` key uses the generic `HashMap<JoinKey, ...>` lookup seen by perf.

The residual program contains **107 physical nodes**. `evaluate_bound_program_without_storage`
evaluates those nodes once each per batch. The relevant common-column nodes are:

| column | physical node indices | reference multiplicity |
|---|---|---|
| `l_shipmode` | 26, 61, 97 | 1 each |
| `l_shipinstruct` | 31, 66, 102 | 1 each |

The three OR arms therefore repeat both common predicates physically. No partkey equality remains in
the residual; ADR 319's key extraction is preserved.

## Cohort A - external perf category stability

Five fresh ledger-disabled children ran with:

```text
perf record -e cpu_core/cycles/P --freq 99 --call-graph dwarf,16384
```

The explicit P-core event is required on this hybrid host; the default event produced an unusably small
Atom-event sample in the exploratory smoke run.

| child | memmove self | generic `HashMap::get` self | spilled-inner materialize self | unique top | lost samples |
|---:|---:|---:|---:|---|---:|
| 1 | 13.56% | 14.97% | 7.36% | map lookup | 0 |
| 2 | 14.19% | 13.70% | 7.62% | memmove | 0 |
| 3 | 14.06% | 13.94% | 7.73% | memmove | 0 |
| 4 | 13.87% | 13.76% | 7.50% | memmove | 0 |
| 5 | 13.92% | 14.30% | 7.60% | map lookup | 0 |

| sampled category | minimum | median | maximum | spread `(max-min)/median` | gate |
|---|---:|---:|---:|---:|---|
| memmove | 13.56% | 13.92% | 14.19% | 0.04526 | share stable; top identity mixed |
| generic `HashMap::get` | 13.70% | 13.94% | 14.97% | 0.09110 | share stable; top identity mixed |

Both leading shares satisfy the numeric spread threshold, but the unique top identity is mixed 3/2 and
therefore fails the committed 5/5 categorical rule. Some memmove call chains resolve to D1 partition or
Arrow movement while others resolve to D4 dictionary decode, so the document does not create a post-hoc
combined D1 category. The generic map frame is still a resolved subcost of the independently dominant
probe leaf and grounds the mechanism candidate.

## Cohort B - required exclusive-span ledger

Raw timings are diagnostic and remain inside the ledger cohort:

| child | root us | D1 us | D2 us | D3 us | D4 us | residual us | outside-root us |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 87,538,606 | 50,917,753 | 17,115,915 | 3,299 | 15,286,852 | 4,214,787 | 154.088 |
| 2 | 87,184,680 | 50,667,846 | 17,089,758 | 2,742 | 15,230,160 | 4,194,174 | 174.380 |
| 3 | 88,630,270 | 51,557,404 | 17,291,241 | 3,297 | 15,550,089 | 4,228,239 | 171.720 |
| 4 | 88,028,087 | 51,223,204 | 17,308,011 | 3,157 | 15,255,710 | 4,238,005 | 206.187 |
| 5 | 87,579,190 | 51,005,848 | 17,206,211 | 2,916 | 15,157,511 | 4,206,704 | 175.925 |

`outside-root` is `execute_ms * 1000 - root_span_micros`. It is reported separately and is never
folded into the in-root residual.

| normalized quantity | minimum | median | maximum | spread | gate |
|---|---:|---:|---:|---:|---|
| D1 | 0.581155 | 0.581713 | 0.582397 | 0.00213 | pass |
| D2 | 0.195094 | 0.196018 | 0.196619 | 0.00778 | pass |
| D3 | 0.0000315 | 0.0000359 | 0.0000377 | 0.17387 | pass |
| D4 | 0.173072 | 0.174630 | 0.175449 | 0.01361 | pass |
| dominant leaf `probe_lookup_select` | 0.353235 | 0.353798 | 0.354097 | 0.00244 | pass |
| `arrow_materialize` | 0.227357 | 0.228407 | 0.228661 | 0.00571 | pass |
| in-root residual | 0.047706 | 0.048107 | 0.048148 | 0.00917 | pass; all < 0.10 |

D1 and `probe_lookup_select` are the dominant pair in all five children. Calls are also exact and
stable in all five:

| leaf | calls / child |
|---|---:|
| probe lookup / selection | 820,134 |
| Arrow materialization | 468,648 |
| predicate evaluation / filter | 234,324 |
| Decimal expression | 1,133 |
| numeric fold | 1,133 |
| scan polling / decode | 234,326 |
| spill-page read / decode | 702,972 |
| build / index / spill encode-write | 6 |

The accepted design is the widened second instrumentation pass. The first pass left 14.7% of the root
unassigned, so it was rejected under the predeclared rule; scan preparation and partition-routing spans
were added, and the ceiling was not moved.

## M to S reconciliation and independent oracle

Every ledger child reports the exact same flow:

| quantity | rows |
|---|---:|
| predicate input / join matches, M | 59,986,052 |
| predicate output, S | 1,134 |
| SUM argument rows | 1,134 |
| numeric fold rows | 1,134 |

Only about 0.00189% of join matches survive the residual. The independent generator oracle reports:

```text
Q19Oracle {
    revenue: Some(301044380911),
    precision: 32,
    scale: 4,
    qualifying_rows: 1134,
    branch_hits: [171, 382, 581],
}
```

Thus engine `S == oracle.qualifying_rows == 1,134`, while the independent oracle identity
`171 + 382 + 581 == 1,134` also holds. The engine does not collect query-specific branch counters.

## Rejected and replaced evidence

No rejected number contributes to the verdict:

- the first instrumentation pass had a 14.7% residual and was widened rather than accepted;
- `perf-pre-box/` and `ledger-pre-box/` belong to the pre-final source before the recursive-future boxing
  correction and were rerun from scratch;
- `perf-pre-carve/` and `ledger-pre-carve/` belong to the source before the streaming-owner carve that
  restored the join-root ratchet; despite matching conclusions, they are superseded by the accepted
  final-source cohorts;
- the pre-carve perf rail's first child lost 52 samples and is retained under
  `perf-pre-carve/child1-lost/`; no final-source child lost samples;
- `perf_smoke/` used the default hybrid event and collected only about 20 Atom samples; the accepted
  rail uses explicit `cpu_core/cycles/P`;
- earlier ordinary-release phase-zero receipts were superseded by the final symbol-rich binary and are
  not pooled with either accepted cohort.

## Acceptance and forward arc

| acceptance item | status |
|---|---|
| live branch/orientation/replay/fingerprint reconfirmed | pass |
| eight exclusive leaves, no overlap | pass |
| in-root residual <= 10% | pass, 5/5 (4.77%-4.81%) |
| exact M to S and oracle reconciliation | pass, 5/5 |
| physical-node/reference-multiplicity census | pass |
| same unique dominant perf category | **fail: memmove 3/5, map lookup 2/5** |
| same dominant ledger domain and leaf | pass, D1 / probe lookup-selection 5/5 |
| D1-D4 and dominant-leaf spread <= 0.25 | pass |
| fingerprint/digest/schema/rows/spill invariance | pass, 10/10 accepted children |
| disjoint cohorts; elapsed never cross-compared | pass |
| no mechanism or speed claim in ADR 438 | pass |

The diagnosis deliverable is robust on the required ledger: D1 / probe lookup-selection is 5/5,
reconciles exactly, and has low spread. The external perf rail does not provide one unique 5/5 top
self-frame, however, so this document leaves that acceptance item red rather than redefining the
category or rerunning selectively. Promotion review banks the diagnosis because symbol-level memmove
crosses D1 and D4 while the required exclusive-span ledger resolves their temporal ownership; the red
row remains part of the record, and ADR 438 does not rewrite its predeclared rule.

ADR 439 receives exactly one first mechanism: extend the existing primitive `Int64` resident
spill index to the partitioned streamed scan-build rebuild path. Acceptance for that cut must prove the
path selects the primitive index, preserve generic/null-safe fallback behavior and residency accounting,
and return any speed claim to an unprofiled ADR 356-shaped rail. That rail must demonstrate that the
primitive index shrinks the wall rather than merely relabeling the leaf. The D1 probe leaf and generic
`HashMap::get` frame are the before receipts.

If that mechanism moves D1 below D2, the independently measured next candidate is residual-program
factoring of the physically triplicated common ship predicates. Decimal arithmetic is not justified as
a current target.

## Verification

The final source-matching seal gate is green:

- `scripts/cargo-cgroup fmt -p toron -- --check`;
- `scripts/cargo-cgroup test -p toron --lib`: **1,305 passed, 0 failed**;
- `scripts/cargo-cgroup test -p toron --test architecture_test`: **2,082 passed, 0 failed**;
- `scripts/cargo-cgroup test -p toron --no-fail-fast`: every unit, integration,
  architecture, and doc-test target completed with **0 failures**;
- `scripts/cargo-cgroup test -p toron-tpch`: every target completed with **0 failures**;
- `scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast`:
  every target completed with **0 failures** (library **44 passed / 1 pre-existing ignored**, CLI
  **15/0**, correctness **176 passed / 1 pre-existing ignored**, and all remaining integration and
  doc-test targets green);
- the six focused profiler tests, the profiled/unprofiled Decimal-SUM parity test, and the exact
  deep-recursion stack-overflow reproducer all pass;
- `exec.rs` remains **7,499 lines** and the bounded join root is **3,713 lines**;
- `git diff --check` is clean, the canonical documents contain no private links, and no `.perf.data`
  file is tracked.

All accepted measurements above were produced by the final symbol-rich binary with SHA-256
`4b75e4c84bcf8dc952accf7bbf87af3429aa8af7391de04841dc526a6f367408`; no pre-box or pre-carve
receipt is pooled into the verdict.
