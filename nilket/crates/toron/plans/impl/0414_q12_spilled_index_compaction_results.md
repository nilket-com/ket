# ADR 414 - q12 spilled index compaction results

## Summary

ADR 414 greens q12 by replacing the streamed-spilled build's resident row index with a compact primitive-key index on
the hot primitive-key paths. The cap, task budget, cgroup scope, and spill budget are unchanged.

Before 414, q12 failed at the write-time resident-state guard:

| query | status | producer | requested | available |
|---|---|---|---:|---:|
| q12 sf=10 | fail | streamed inner join build spill resident state | 1,074,377,480 | 1,073,741,824 |

After 414, q12 passes persisted correctness:

| query | status | execute_ms | spill_bytes | max_rss |
|---|---|---:|---:|---:|
| q12 sf=10 | pass | 6,174.895 | 159,510,976 | 1,428,728 KiB |

The implementation opts the existing streamed-spilled build helpers into the compact index eagerly. That avoids first
building the old resident hash index and then rescuing it near the cap.

## Headroom Proof

The q12 wall was only 65,656 bytes over the retained cap, so a "barely green" result would not satisfy the ADR. A
temporary same-build q12 index profile measured the compact index directly:

```json
{"event":"spill_index_finish","spill_id":1,"index_kind":"int64","rows":15000000,"input_rows":15000000,"page_count":1832,"resident_index_bytes":536870912,"writer_peak_bytes":87432,"spill_bytes":159510976,"entry_count":15000000,"bucket_count":33554432,"entry_capacity":16777216}
```

This is a 2.001x reduction from the prior resident-index payload:

| representation | resident_index_bytes | reduction |
|---|---:|---:|
| old hash index | 1,074,377,480 | baseline |
| compact int64 index | 536,870,912 | 2.001x |

The temporary profile hook was removed before sealing. The retained tests cover the permanent contract: lookup order,
accounting fidelity, fallback fencing, bounds checks, and guard armedness.

## Implementation

The old write-time index was `HashMap<JoinKey, Vec<SpilledBatchRowRef>>`. The new representation is `SpilledBatchIndex`:

| key plan | representation |
|---|---|
| `Int32`, non-null-safe | primitive open-addressed compact index |
| `Int64`, non-null-safe | primitive open-addressed compact index |
| `Generic` or null-safe | hash fallback |

Primitive entries store raw keys and compact row references:

| field | representation |
|---|---|
| key | `i32` / `i64`, no `JoinKey` enum on the hot path |
| row reference | `CompactSpilledBatchRowRef { page_index: u32, row: u32 }` |
| duplicates | first ref inline, heap overflow only for duplicate refs |
| hash seed | private `ahash::RandomState`; output order remains lookup-driven |

The compact path is enabled at the two streamed-spilled build seams:

| helper | effect |
|---|---|
| `try_execute_streamed_spilled_left_semi_join` | compact resident index for semi/existence-style spilled build |
| `try_create_streamed_spilled_inner_build` | compact resident index for streamed spilled inner build |

Loaded decoded-page indexes and the existence-set sibling were not folded into 414. q12's wall was the write-time
`SpilledInnerJoinBuild` index; those siblings remain named follow-ons if a future profile shows them hot.

## Tests

New and updated tests lock the behavior that mattered for this cut:

| test | contract |
|---|---|
| `incremental_inner_resident_index_tracks_new_and_duplicate_refs` | compact lookup preserves duplicate ref order and accounting matches computed bytes |
| `inner_spill_generic_and_null_safe_keep_hash_index_fence` | generic and null-safe builds keep the hash fallback |
| `inner_spill_default_resident_index_keeps_int64_on_hash_path` | default spill builds are unchanged unless a helper opts in |
| `inner_spill_compact_rescue_keeps_resident_guard_armed` | compaction can reduce a near-cap index, but a genuinely over-cap state still fails |
| `compact_spilled_batch_ref_rejects_page_index_truncation` | page indexes outside `u32` range fail instead of truncating |

The architecture guard also remains clean: no engine source uses the banned `packed` terminology.

## Measurements

### q12 Driver

Artifacts:

- `target/tpch/adr414/q12_index_profile_temp.jsonl`
- `target/tpch/adr414/final_scoped/q12_sf10_perf.out`
- `target/tpch/adr414/final_scoped/q12_sf10_correctness.out`

| row | status | execute_ms | correctness | spill_bytes | resident_index_bytes |
|---|---|---:|---|---:|---:|
| pre-414 | fail | ~8s wall | n/a | 0 | 1,074,377,480 |
| compact profile | diagnostic | n/a | n/a | 159,510,976 | 536,870,912 |
| final q12 perf | pass | 6,174.895 | digest row matches | 159,510,976 | under cap |
| final q12 correctness | pass | 18,535.3 wall | pass@compare | n/a | under cap |

### sf=10 Controls

Artifacts:

- `target/tpch/adr414/controls_eager/*`
- `target/tpch/adr414/final_scoped/q05_sf10_correctness_after_spill_cleanup.{out,err}`
- `target/tpch/adr414/final_scoped/q05_sf10_perf_diagnostic.{out,err}`
- `target/tpch/adr414/head_compare/q05_sf10_correctness.{out,err}`
- `target/tpch/adr414/head_compare/q05_sf10_correctness_no_high.{out,err}`

| query | status | elapsed_ms | note |
|---|---:|---:|---|
| q03 | pass | 22,871.065 | retained green |
| q04 | pass | 14,826.035 | retained green |
| q08 | pass | 28,819.645 | retained green |
| q10 | pass | 112,904.293 | retained green |
| q12 | pass | 6,302.988 | driver |
| q19 | pass | 98,340.247 | retained green |
| q05 | inconclusive | OOM in this session | see caveat |

q05 could not be re-proven green in this session. A clean detached HEAD worktree at the ADR 414 plan commit
OOM-killed under the same 4G scope (`43.689s`, 4G peak), and also OOM-killed with `MemoryHigh` omitted (`42.452s`,
4G peak). The current worktree first hit `Disk quota exceeded` because `/tmp/toron_join_spill` held 21 GiB of stale
spill files from killed probes; after clearing only that scratch directory, the current worktree also OOM-killed under
the fixed 4G scope (`40.160s`, 4G peak). This is not banked as a q05 regression or a q05 pass; it is a baseline
reproducibility gap for the closing live sweep.

### q03 sf=1 Four-Way

Artifact:

- `target/tpch/perf_adr414_q03_sf1_eager_control/latest_summary.jsonl`

| engine | status | median_ms | spread | fingerprint | flags |
|---|---|---:|---:|---|---|
| toron | pass | 360.713 | 0.045 | `666e0ac028036f70` | none |
| rune | pass | 368.774 | 0.037 | `666e0ac028036f70` | none |
| polars_sql | pass | 100.624 | 0.141 | `fnv1a64:a3b5c15847833ac3` | none |
| duckdb | pass | 38.000 | 0.263 | `fnv1a64:73b142150e59739f` | `unstable,schema_mismatch` |

## Scope Honesty

414 is a resident-index representation cut, not a cap change and not a new spilled-index sharding mechanism. It
retires the last known enum-key/SipHash-style resident index on the streamed-spilled build write path. If a future
query exceeds the retained cap even with the compact representation, the next mechanism is the sharded/deferred index
named in the ADR, not another formula adjustment.

The full 22-query sf=10 live sweep remains the closing artifact. It should re-prove q05 and state the final board from
fresh measurements rather than carrying this session's q05 caveat forward by implication.
