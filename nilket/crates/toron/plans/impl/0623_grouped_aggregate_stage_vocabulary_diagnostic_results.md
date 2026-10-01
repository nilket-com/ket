# ADR 623 results - stage vocabulary for the primitive scan-grouped and fallback grouped folds

**Closed LIMITED.** The instrument landed. Instrumentation compatibility
held on all 22 cells: outputs, plans, routes and statuses are unchanged
apart from the declared new contracts.

q18's two aggregate invocations:
- **The inner aggregate** takes the primitive Int64 SUM path, and its
  breakdown is published. Of its 1,873.3 ms handler, the `fold` is
  **1,617.9 ms** over 5,859 batches and `stream_pull` is 241.2 ms.
- **The outer aggregate** runs on the fallback route `bound_grouped_fold`.
  **Its breakdown is withheld** under the publication rule: its bridge
  fails in all five runs. The fallback handler itself takes 0.358–0.375
  ms, but the Aggregate node's exclusive time is 1,635.6–1,672.1 ms. So
  about 1.64 s of that node's exclusive time lies **outside the fold
  handler**, as dispatch residual. This record does not identify where.

A diagnostic: no token, no pair, no cut, no saving projected.


## Identity

| identity | value |
|---|---|
| instrumented source | branch `adr623-instrument` `c0d23ca5`, tree `d3baf29d9a30b8194e8c39070edc786513b7d1dc`; strix checkout at the same head, clean |
| instrumented executable | `552a0c930aeba92494b23585424569382dfd0a8046ab6a9ab1141b09b2c439b9`, manifest `adr623-instrumented.manifest` (`fd4b3fb84ceea51c4e3ff0bdf0b7e07a863e56adebbca7fbd2d41f9758db2ac4`) |
| base executable | ADR 622's `18df5747a7dc247cc0caeb5db2e84cd05440d2375f1254697f29c35246cd7ee5`, manifest `adr622-measured.manifest` (`0431d89871b6…`) |
| anchor | ADR 622's board `54e99bdf3f63608630fbdfd7b7bca1c8d0144ed4a025ede41e5d38e28753c0e9`, hash-checked by the reader before any cell is read |
| ladder (bigfoot, `--no-fail-fast`) | fmt clean; toron 46 suites, 5,535 passed (5,533 plus the 2 new fixtures), 0 failed, 5 ignored; toron-tpch 674 passed, 0 failed, 6 ignored; release build 0 warnings |

**The implementation, as reviewed:**
- **The primitive SUM and COUNT paths** each declare a single-phase
  vocabulary (`scan_grouped_primitive_sum` and
  `scan_grouped_primitive_count`). Required stages: `scan_open`,
  `stream_pull`, `input_ops`, `fold`, `finalize`. The boundary is
  500 ms, and the stages are batch-level. Every stage closes before its
  `?`, so error ordering is unchanged.
- **The executor fallback** calls `attributed_bound_grouped_aggregate`.
  It opens the handler span, declares `bound_grouped_fold` (`fold`,
  `finalize`, 500 ms) and records that route.
- **One body.** The original body is shared as
  `staged_bound_grouped_aggregate`, and `bound_grouped_aggregate` is a
  `#[cfg(test)]` delegate (the plan's implementation adjustment). ADR
  435's fold and finalize site counts are unchanged.
- **Size bound.** `exec.rs` stays at 7,593 lines.

## Procedure

One separated launch, ADR 595's: unit `adr623-diagnostic`, token
`adr623-57bd3d8bae67b9ad`, a 120 s delay and `RuntimeMaxSec` 5400.
- **Before the launch:** the scripts, `executables.json`, both manifests
  and both executables were staged and hash-verified on strix, with the
  readings kept in `staging_verification.txt`.
- **At the launch** (unix ms 1790821600769): load 0.12, 39 °C, scratch
  empty, no output directory.
- **Completion** at unix ms 1790821996902, `exit_status=0`.
- **After:** 0 leftover processes and 0 scratch entries.

There was no SSH to strix between the launch and the datagram. The
timed child's recorded environment holds only the coordinate and the
ADR 595 variables. Each run added only
`TORON_NODE_ATTRIBUTION_PROFILE_PATH`. The run log has all 49 blocks,
each rc 0 and isolated, ending "diagnostic complete".

## Instrumentation compatibility (the 22-cell sweep)

`adr623_read.py`: **SWEEP: COMPATIBLE.** Per cell, both executables'
rows were checked: pass, `full_hash`, the dataset fingerprint, the
coordinate, and ADR 622's sealed digest and plan fingerprint.

Both profiles are numerically valid:
- the node exclusives sum to the attributed total;
- the in-tree residual, interleaving and mismatches are zero.

Every invocation's accounting identities hold in both arms. Node
topology and invocation counts are identical. Every invocation outside
the three new contracts is identical in order: route, bridge,
vocabulary, statuses, required set, boundary, stage sequence and calls,
and refinement identities.

Newly instrumented invocations:

| cell | invocation | route / vocabulary | status | stages passed | bridge passed |
|---|---|---|---|---|---|
| q02 | 0 | `bound_grouped_fold` | reconciled | true | true |
| q10 | 0 | `bound_grouped_fold` | reconciled | true | true |
| q11 | 0 | `bound_grouped_fold` | reconciled | true | true |
| q12 | 0 | `bound_grouped_fold` | reconciled | true | true |
| q13 | 1 | `bound_grouped_fold` | reconciled | true | true |
| q16 | 0 and 1 | `bound_grouped_fold` | reconciled | true | true |
| q17 | 0 | `bound_grouped_fold` | reconciled | true | true |
| q18 | 0 | `scan_grouped` / `scan_grouped_primitive_sum` | reconciled | true | true |
| q18 | 1 | `bound_grouped_fold` | reconciled | true | **false** |
| q20 | 0 | `bound_grouped_fold` | reconciled | true | true |
| q22 | 1 | `bound_grouped_fold` | reconciled | true | true |

No invocation took the primitive COUNT path. This is a compatibility
result on observed outputs, plans, routes and statuses. It is not proof
of full behaviour neutrality, and unarmed overhead is unmeasured.

## q18: five armed runs

Every run passed its gate:
- ADR 622's sealed q18 digest and plan fingerprint;
- `full_hash`, the coordinate, and the instrumented executable.

The five runs agree on node topology, routes, vocabularies, stage
sequences with calls, and refinement identities.

Execute times, armed: 6,023.8, 6,018.5, 6,093.1, 6,047.7 and 6,027.0 ms,
**median 6,027.0**. ADR 622's board median is 6,022.0 ms. The +5.0 ms is
an observed difference, not apportioned between the instrument and run
variation.

Peak RSS is **unavailable**: the rows come from a direct `perf-run-one`
child, with no parent-harness label. It is not inferred.

**Invocation 0: the inner aggregate, primitive Int64 SUM.** Published:
the stages, bridge and identities passed in all five runs. Median [min,
max] ms:

| measure | ms |
|---|---|
| handler (= node exclusive) | 1,873.3 [1,871.8, 1,886.3] |
| `scan_open` (1 call) | 1.5 [1.4, 1.5] |
| `stream_pull` (5,860 calls) | 241.2 [233.1, 241.9] |
| `input_ops` (5,859 calls) | 0.0 [0.0, 0.0] |
| **`fold`** (5,859 calls) | **1,617.9 [1,617.5, 1,640.1]** |
| `finalize` (1 call) | 2.2 [1.9, 2.5] |
| stage residual | 9.8 [9.5, 9.8] |
| bridge (dispatch) residual | 0.0 [0.0, 0.0] |

Reading these:
- `stream_pull` includes the parquet decode.
- `fold` is the per-batch downcast plus the per-row `groups.insert` into
  the primitive Int64 table.
- Component medians need not sum to the median total.

**Invocation 1: the outer aggregate, `bound_grouped_fold`. Breakdown
withheld.** Its stages reconcile (status reconciled, passed), but its
**bridge fails in every run**:

| run | handler exclusive µs | node exclusive µs | dispatch residual µs | boundary µs |
|---|---|---|---|---|
| 1 | 371 | 1,641,319 | 1,640,948 | 10,000 |
| 2 | 358 | 1,635,580 | 1,635,222 | 10,000 |
| 3 | 363 | 1,672,059 | 1,671,696 | 10,000 |
| 4 | 375 | 1,644,679 | 1,644,304 | 10,000 |
| 5 | 370 | 1,644,859 | 1,644,489 | 10,000 |

So in these five runs, the fallback fold and finalize themselves are
under 0.4 ms (358–375 µs). The Aggregate node's 1.64 s exclusive time is
not in them. It lies in the
node outside the handler span: either the dispatch attempts that run
before the fallback, or other work inside the node that is not a child
`execute_bound` frame. This record does not measure which. The figure
is named as the reason for withholding, and it is not a stage
attribution.

These new runs show why ADR 614's route-labelled exclusive time
(`uncovered_generic_fold`, 1,645 ms) must not be read as measured
fallback-fold time. That older run's own split remains unmeasured. The
work outside the handler (the losing dispatch attempts, or other inline
non-child work) is unidentified, and no saving is established.

**Node kinds** (exclusive ms, five runs, context only, unattributed):

| node kind | ms |
|---|---|
| aggregate | 3,516.7 [3,509.1, 3,558.4] |
| exchange | 1,139.9 [1,136.3, 1,162.3] |
| join | 847.9 [846.0, 857.3] |
| scan | 380.5 [379.8, 383.1] |
| filter | 143.0 [141.8, 145.2] |

DuckDB's `HASH_GROUP_BY` (1,050 ms, ADR 614's profiling) is historical
context, not a like-for-like timing.

## Provenance

All sources are on strix under `~/work/ket/nilket/target/tpch/`.

| file | sha256 |
|---|---|
| `adr623-scripts/adr623_read.py` | `31f2407c5818d058c56598355022fc0c5f80f6e91cd4adf4b7b23a3e43c39a19` |
| `adr623-scripts/adr623_read_test.py` | `7c53ed2a245e83bcff8e976d9d89191d35715b89a5d9065adc329d76028dd195` |
| `adr623-scripts/adr623_run.sh` | `7237d6fc98ad2102e0982a7da475c0a737b6e2e55d0362c687886e9706055651` |
| `adr623-scripts/adr623_common.sh` | `48749da2bb1ab128b71ff4639b216d07abd6a35da7105c76bb4ff7990bdc151c` |
| `adr623-scripts/executables.json` | `9349a557b13e4a1cd19ff344c08e678c61630d0f47191a12dc063f1660132ecd` |
| `adr623-strix-host-record/launch.sh` / `readout.sh` / `snapshot.sh` / `env_run.sh` | `e2b325b9…` / `018cdc31…` / `d3e362c9…` / `97972a34…` |
| `adr623-strix-host-record/staging_verification.txt` | `46d7f17f06c8297ac34e6b8f7abf5986c46687d7d3c277a797f2b34363908e81` |
| `adr623-strix-host-record/readout.txt` (the read-out) | `dae01a4ffaaf206c495f56cb37d67bceb204cbee76ec57fbc0875c32d8cbb092` |
| `adr623-diagnostic/run.log` | `14b5d9802e3f7c5ea5c8a61f51c29d2c13655cac30b73ddfd839c2dd7ae54763` |
| `adr623-diagnostic/host.txt` | `d6bfd2e65d080643d06f0aac1135c8ddd03113bf0f50ea8767ab0e29bee73e7e` |
| `adr623-strix-host-record/diagnostic/host_config_before.txt` / `host_after.txt` / `launch_env.txt` | `ff51d9d6…` / `bce40f30…` / `25b326f2…` |

**Source inputs.** The 98 JSON sources that establish the figures are
listed with their SHA-256s in
`adr623-strix-host-record/source_inputs.sha256` (sha256
`ee27e477ae4148fb7850a29c1eef8ea5d3eb6ec0e6bf9c7f3ab596d1b3a7ef8c`). The
listing was generated read-only after the run. It covers:
- `adr623-diagnostic/sweep/{base,instr}/qNN_{row,node}.json` (88 files);
- `adr623-diagnostic/runs/run_N_{row,node}.json` (10 files).

The bridge table above was read from `runs/run_N_node.json`
(invocation 1's `bridge`).

**Reader corrections before the launch** (reviewed). The first reader,
kept as `adr623_read-v0.py` in the scratch tooling, checked only boolean
stamps. Codex's controls showed that a negative stage, a broken
residual, a reordered run and a duplicated stage were all accepted. The
final reader:
- recomputes every accounting identity;
- checks the boundary verdicts against the profiler's exact comparisons;
- preserves stage order, calls and identities;
- binds the anchor and both executables.

Before the launch it was calibrated on 75 real invocations from ADR
615's and ADR 621's profiles, refusing 0.

## Consequence

- **q18's inner aggregate:** its time is the primitive SUM `fold`,
  1.62 s over about 48 M `lineitem` rows. The pull and decode are a
  further 0.24 s.
- **q18's outer aggregate,** in these five runs: about 1.64 s sits in
  the Aggregate node outside the fold handler, and the fold itself is
  under 0.4 ms. The next record would attribute that residual before any
  q18 cut is considered. It would locate the exact attempted-handler and
  work boundaries, and keep child time excluded.
- Exchange (1.14 s) and join (0.85 s) remain node-level.
- No saving is projected.
