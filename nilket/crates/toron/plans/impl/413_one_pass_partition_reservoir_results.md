# ADR 413 - Results: one-pass partition reservoir

ADR 413 shipped the q05 capstone reservoir for the generic reoriented grouped path. It does not change the task
memory cap, cgroup scope, `memory_per_task`, sort cap, or spill retained-footprint cap.

## Phase 0

Live q05 sf=10, persisted parquet, fixed 4 GiB scope, pre-413 source:

- Status: no JSON row; `systemd` stopped the run at `RuntimeMaxSec=360s`.
- Journal: `Failed with result 'timeout'`, `5min 8.973s CPU time over 6min 145ms wall clock time`, `4G memory peak`.
- Scan hook: lineitem `34` scan ids, `1,973,109` batch events, about `127.2 GiB` raw bytes.

The diagnostic trace identified the q05 seam:

- grouped generic reoriented base path, not the specialized scan-probe base path;
- runtime `JoinReplayId(1)` is the `lineitem` `PartitionedScan`;
- `partition_count=64`, `group_count=22`;
- estimated lineitem build hash bytes: `7,198,326,240`;
- the generic base side was already materialized under cap, but split into `23,440` small batches.

The first correct insight was not enough by itself: writing the lineitem reservoir once but probing it once per base
batch still timed out. The banked cut concatenates the already-materialized base side once, then probes the reservoir
once by partition/group.

Live q12 sf=10, same pre-413 source:

```text
status=fail
reason=memory_budget_exhausted
producer="streamed inner join build spill resident state"
requested=1,074,443,016
available=1,073,741,824
detail="site=write, resident_index_bytes=1074377480, writer_peak_bytes=0, page_reserve_bytes=65536, pinned_decoded_bytes=0"
execute_ms=10688.537623
spill_bytes=0
```

q12 was classified as genuine resident-index pressure, not a writer-scratch or pinning mismatch.

## Implementation

The shipped cut is narrow:

- In the generic reoriented grouped path, if the first runtime is a multi-group `PartitionedScan`, write all build
  partitions into `SpilledInnerJoinBuild` reservoirs in one source pass.
- The old active-group writer keeps its group spill-budget guard. The reservoir writer disables only the
  active-group-total guard because that guard is a per-group slicing check, used to size and bound one active group
  at a time. It is not meaningful during a single all-partition reservoir write. Each individual
  `SpilledInnerJoinBuild` still has its own spill-budget and resident-state guards, and the task/cgroup caps are
  unchanged.
- Concatenate the already-materialized base batches once, then probe the reservoir by partition/group.
- Feed the resulting batches through the existing suffix runtimes and grouped aggregate fold.
- Clean up all reservoir spill files after success or error.

The intended trade is disk for scans: peak spill disk now holds the full reservoir build (`5.27 GiB` in the q05 run)
instead of roughly one active group at a time, eliminating the `127.2 GiB` repeated read loop.

The existing fallback path remains if the reservoir build itself declines.

## q05 Result

Final trace-free release binary, persisted sf=10, fixed 4 GiB scope:

```json
{"query":"q05","status":"pass","execute_ms":190034.273454,"output_row_count":5,"spill_bytes":5353985328,"plan_fingerprint":"757d7c625669b21a"}
```

`/usr/bin/time`:

```text
Elapsed wall: 3:16.29
User time: 154.97s
System time: 38.34s
Max RSS: 3,051,064 KiB
Exit status: 0
```

Mechanism proof from the final scan progress hook:

| table | raw rows | raw bytes |
|---|---:|---:|
| lineitem | 239,944,208 | 11,607,359,488 |
| orders | 60,000,000 | 1,216,921,728 |
| customer | 4,500,000 | 72,851,520 |
| supplier | 200,000 | 3,248,896 |
| nation | 50 | 2,144 |
| region | 10 | 688 |

The progress hook counts the 4-way exchange scan legs, so `lineitem` appears as about 4x the 59,986,052-row source.
The earlier diagnostic trace recorded the reservoir build directly:

```text
build_rows=59,986,052
partition_count=64
spill_bytes=5,270,315,856
```

That is the intended collapse: q05 moved from repeated `lineitem` re-scans (`~127.2 GiB` in the bounded pre-run) to a
single exchange-driven reservoir build (`11.6 GiB` hook volume, `5.27 GiB` written reservoir), then bounded spill-backed
probe work.

Persisted correctness:

```text
tpch_correctness query=q05 sf=10 mode=strict phase=compare status=pass reason=none elapsed_ms=228680.2
systemd: success, runtime=3min 49.134s, Memory peak=4G
```

## q12 Result

q12 remains the same named resident-index wall:

```json
{"query":"q12","status":"fail","reason":"memory_budget_exhausted","execute_ms":7576.756709,"spill_bytes":0,
"diagnostic_message":"BudgetExhausted { dimension: Memory, producer: \"streamed inner join build spill resident state\", requested: ByteCount(1074443016), available: ByteCount(1073741824), detail: Message(\"site=write, resident_index_bytes=1074377480, writer_peak_bytes=0, page_reserve_bytes=65536, pinned_decoded_bytes=0\") }"}
```

ADR 413 did not solve q12. The next q12 cut is still resident-index policy / partition-index sharding.
It is not covered by the q05 reservoir seam: q12 fails at `site=write` while building the spilled build's resident row
index (`spill_bytes=0`), before any grouped runtime/reservoir probe exists. The next lever is index sharding at that
write/index seam, not the grouped re-scan hoist.

## Controls

sf=10 control perf rows, same final release binary and fixed 4 GiB scope:

| query | status | execute_ms | output rows | spill_bytes | fingerprint |
|---|---:|---:|---:|---:|---|
| q03 | pass | 15,608.295 | 114,003 | 233,338,304 | `666e0ac028036f70` |
| q04 | pass | 4,887.229 | 5 | 0 | `61ccf3a19b2a9dd9` |
| q08 | pass | 18,854.443 | 2 | 0 | `cb686ae2c1202c97` |
| q10 | pass | 105,432.635 | 381,105 | 215,703,848 | `4e9ea5a292a03a47` |
| q19 | pass | 85,859.095 | 1 | 51,471,552 | `65c3c2c0b0a99063` |

q03 sf=1 persisted shape/substrate control, `runs=3`, `warmup=1`:

| engine | status | median_ms | spread | fingerprint | flags |
|---|---:|---:|---:|---|---|
| toron | pass | 358.044 | 0.004 | `666e0ac028036f70` | none |
| rune | pass | 362.536 | 0.037 | `666e0ac028036f70` | none |
| duckdb | pass | 37.000 | 0.324 | `fnv1a64:73b142150e59739f` | `unstable,schema_mismatch` |

DuckDB is context only for this short sf=1 row because the denominator is unstable.

## Verification

All verification ran after `cargo fmt --all`; the wrapper fmt check also passed:

```text
scripts/cargo-cgroup fmt -- --check
CARGO_CGROUP_JOBS=4 scripts/cargo-cgroup check -p toron --all-targets
CARGO_CGROUP_JOBS=12 scripts/cargo-cgroup test -p toron partition_reservoir_scans_source_once_and_matches_group_rescan_path --lib
CARGO_CGROUP_JOBS=12 scripts/cargo-cgroup test -p toron reoriented_inner_chain_generic_base_consumes_streamed_stage_runtime --lib
CARGO_CGROUP_JOBS=4 scripts/cargo-cgroup test -p toron --no-fail-fast
CARGO_CGROUP_JOBS=4 scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast
CARGO_CGROUP_JOBS=4 scripts/cargo-cgroup test -p toron --test architecture_test --no-fail-fast
git diff --check
git ls-files '*.perf.data'
```

The full `toron` gate passed after rerunning with `CARGO_CGROUP_JOBS=4`; an earlier 12-job compile exited during
compilation under the cgroup, matching the known local build-pressure pattern. Architecture stayed at `2079 passed /
0 failed`. `git ls-files '*.perf.data'` returned no tracked perf data.

## Rejected Attempts

The earlier non-banking attempts are preserved under `target/tpch/`:

```text
target/tpch/adr413_rejected_one_pass_reservoir.patch
target/tpch/adr413_failed_one_pass_reservoir_attempts.patch
target/tpch/adr413_rejected_generic_scan_probe_reservoir.patch
target/tpch/adr413_rejected_partition_major_grouped_reservoir.patch
```

The most important rejected result was the cached-partition bug: a path completed q05 in 347.7s but was wrong because
once any loaded partition existed, the runtime skipped uncached spilled partitions. The persisted correctness rail caught
the undercount:

```text
tpch_correctness query=q05 sf=10 mode=strict phase=compare status=fail
reason=n_name "CHINA": revenue mismatch: expected 5353508299282, got 282685133977
```

The shipped implementation avoids that class by not mixing a partial loaded cache with uncached partitions; it probes the
one-pass reservoir through the existing partition/group probe semantics.

## Verdict

ADR 413 greens q05 and closes the last repeated-scan time pathology on the board.

Board after 413:

| cell | state | next lever |
|---|---|---|
| q03 | pass | speed only |
| q05 | pass | speed only |
| q10 | pass | speed only |
| q12 | resident-index wall | spill resident index sharding / policy |
| q04/q08/q19 | pass controls | speed only |

q12 remains the only named sf=10 reach wall from this family.
