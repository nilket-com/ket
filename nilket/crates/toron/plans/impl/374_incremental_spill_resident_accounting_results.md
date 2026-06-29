# ADR 374 results - incremental spill resident accounting

ADR 374 replaced the hot spill resident-byte re-folds with maintained O(1) counters. It is a behavior-preserving perf
cut: no budget cap, producer name, or guard condition changed.

## Phase 0 census

Hot indexes and mutation sites:

| build | index | mutation sites | counter action |
| --- | --- | --- | --- |
| `SpilledInnerJoinBuild` | `HashMap<JoinKey, Vec<SpilledBatchRowRef>>` | `write_page_batch` appends a stored build-row ref for each keyed row | new key adds `64 + join_key_heap_bytes(key)`; each stored ref adds `size_of::<SpilledBatchRowRef>()` |
| `SpilledSemiAntiExistenceBuild` membership-only path | `HashMap<JoinKey, Vec<SpilledExistenceRowRef>>` | `index_membership_keys` inserts an empty ref vector for each distinct membership key | new key adds `64 + join_key_heap_bytes(key)`; duplicate membership keys add zero resident bytes |
| `SpilledSemiAntiExistenceBuild` payload path | same | `write_page_batch` appends a stored existence-row ref for each keyed row | new key adds `64 + join_key_heap_bytes(key)`; each stored ref adds `size_of::<SpilledExistenceRowRef>()` |

No clear/remove/repartition path reuses these indexes. New spill-build values start with a zero counter.

The third fold, `join_spill_index_bytes`, is out of scope for ADR 374. It is cold post-write accounting in
`SpilledJoinBuild::write_inner` for the older page-indexed spill writer, not a per-batch resident guard in the sampled
hot path.

## Implementation

- Added `resident_index_bytes` to `SpilledInnerJoinBuild` and `SpilledSemiAntiExistenceBuild`.
- Routed every censused mutation through counter-aware helpers:
  - `append_spilled_batch_row_ref`
  - `index_membership_key`
  - `append_spilled_existence_row_ref`
- Changed the two hot `guard_resident_state` paths to read the maintained counter.
- Kept the old fold functions as `#[cfg(test)]` oracles for equivalence tests.

## Tests

The new tests mutate through real write/index APIs, not direct map construction:

- `incremental_inner_resident_index_tracks_new_and_duplicate_refs`
- `incremental_existence_membership_duplicates_add_no_bytes`
- `incremental_existence_payload_index_tracks_projected_writes`
- `incremental_resident_guard_decisions_match_fold`
- `incremental_resident_counters_start_fresh_for_new_spills`

They cover new-key base cost, duplicate stored-ref cost, duplicate membership-only zero cost, multi-batch equality,
under/equal/over guard equivalence, and fresh counter state for new spill builds.

## Re-profile

Method matched ADR 373: release binary with debug symbols, fixed 4 GB `systemd-run --scope`, `perf record -F 99
--call-graph dwarf`, collapsed/rendered with Inferno. Artifacts remain uncommitted under `target/tpch/profiles/`.

| query | ADR 373 dominant stack | ADR 374 resident-accounting share | ADR 374 outcome / next stack |
| --- | ---: | ---: | --- |
| q04 | 97.60% existence resident accounting | 0.00% | `pass@execute`, `pass@compare` at sf=10; whole-window profile now shows harness file hashing plus real engine hash/index and allocation work |
| q07 | 98.40% inner resident accounting | 0.00% | still timeout-sampled; next stack is allocation/memcopy/free pressure (84.31%), with scan/spill I/O at 8.35% |
| q17 | 84.02% inner resident accounting | 0.00% | still timeout-sampled; next stacks are grouped aggregate fold/finalize (56.53%) and spill read/decrypt/decompress I/O (21.46%) |
| q21 | 98.11% existence resident accounting | 0.00% | no longer a time wall in the sample window; fails quickly at the same named resident-state memory guard, now 712 bytes over cap |

Post-change artifacts:

| profile | artifact |
| --- | --- |
| q04 | `target/tpch/profiles/adr374_q04.svg` |
| q07 | `target/tpch/profiles/adr374_q07.svg` |
| q17 | `target/tpch/profiles/adr374_q17.svg` |
| q21 | `target/tpch/profiles/adr374_q21.svg` |

q04 now completes at sf=10:

```text
tpch_perf engine=toron query=q04 status=pass execute_ms_median=14094.2 peak_rss_kib=1231792 spill_bytes=0 flags=none
tpch_correctness query=q04 sf=10 mode=strict phase=compare status=pass reason=none
```

q21 now reaches the real remaining budget wall quickly:

```text
BudgetExhausted { dimension: Memory, producer: "semi/anti existence-set spill resident state",
  requested: ByteCount(1073742536), available: ByteCount(1073741824) }
```

## Verification

All required gates passed:

```bash
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
systemd-run ... target/release/toron-tpch correctness --query q04 --sf 10 --mode strict --data-source parquet
git diff --check
```

No cap was raised. Budget producer names are unchanged. No query-number references were added under `crates/toron/src`.

## Forward

ADR 374 removed the self-inflicted O(index) resident-byte scan. The next evidence-backed perf cuts are no longer the
resident guards:

- q07: allocation/memcopy pressure in the streamed inner-chain path.
- q17: grouped aggregate fold/finalize plus spill read/decrypt/decompress.
- q21: first clear the remaining 712-byte resident-state budget wall; then re-profile if it becomes time-bound again.
- q04: now a correctness-proven sf=10 green; remaining speed gap is hash/indexing/allocation plus the separate harness
  `stable_file_hash` overhead outside `execute_ms`.
