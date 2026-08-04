# ADR 357 Results - q08/q09 reoriented-inner-chain generalization

## Implementation

ADR 356's perf rail exposed live sf=1 failures in q08/q09. ADR 357 owns the engine behavior fix split out of that
measurement commit.

The reoriented-inner-chain streaming helper is now generalized by topology:
- it infers a consecutive inner chain down to base `Join(0)` instead of hard-coding one replay-id sequence;
- it follows the growing streamed chain on either the left or right side of each join;
- it supports grouped aggregate inputs shaped as `Project(Filter(Join))` and `Project(Join)`;
- it supports filtered and bare base `Join(0)`;
- it restores bound-plan output column order when the streamed probe side is the original right side.

Core executor symbols remain shape-named. TPC-H query numbers appear only in docs/tests that are explicitly TPC-H
scoped.

## Evidence

Pre-fix, live release q08/q09 at sf=1 failed with:

```text
producer: "join output materialization"
requested: ByteCount(1975900039)
available: ByteCount(1073741824)
reason: memory_budget_exhausted
```

After the fix:
- `run --query q08 --mode strict --sf 1` passed.
- `run --query q09 --mode strict --sf 1` passed.
- one-pass release/cgroup `perf --sf 1 --mode strict --runs 1 --warmup 0 --query all` passed 22/22.
- full release/cgroup `perf --sf 1 --mode strict --runs 7 --warmup 1 --query all` passed 22/22; q10 was unstable in
  the all-query run and was rerun per ADR 356's stability policy.
- stable baseline artifact: `target/tpch/perf-sf1-baseline-adr356/baseline_sf1_strict.jsonl`.

## Stable Baseline

The stable baseline has 22 pass rows and no flags. q10 uses the stable rerun row; all other rows come from the full
all-query 7 measured / 1 warmup run.

| query | execute_ms_median | spread | peak_rss_kib_max | spill_bytes_max |
|---|---:|---:|---:|---:|
| q01 | 2210.9 | 0.054 | 2242964 | 0 |
| q02 | 1977.0 | 0.027 | 1030920 | 64102560 |
| q03 | 1349.6 | 0.049 | 3380008 | 0 |
| q04 | 2690.5 | 0.076 | 2524640 | 76307904 |
| q05 | 3018.4 | 0.069 | 3570252 | 159296600 |
| q06 | 179.8 | 0.098 | 2242896 | 0 |
| q07 | 3129.7 | 0.073 | 3410932 | 0 |
| q08 | 3536.2 | 0.035 | 3481708 | 0 |
| q09 | 4044.2 | 0.009 | 3537448 | 0 |
| q10 | 1352.1 | 0.015 | 3132908 | 0 |
| q11 | 2591.7 | 0.027 | 758568 | 72288000 |
| q12 | 4071.2 | 0.013 | 2600232 | 76307904 |
| q13 | 1421.9 | 0.020 | 1417288 | 0 |
| q14 | 104806.0 | 0.021 | 2437820 | 330887144 |
| q15 | 225.1 | 0.184 | 2245108 | 0 |
| q16 | 580.7 | 0.053 | 486252 | 11222496 |
| q17 | 91663.4 | 0.007 | 2465704 | 330887144 |
| q18 | 13689.2 | 0.005 | 6122476 | 331918336 |
| q19 | 90024.4 | 0.008 | 2437848 | 330887144 |
| q20 | 2121.4 | 0.012 | 2305740 | 30178816 |
| q21 | 16688.5 | 0.008 | 3800244 | 524651776 |
| q22 | 245.6 | 0.033 | 923644 | 0 |

## Verification

All commands were run through `scripts/cargo-cgroup`.

- `fmt -- --check`
- `check -p toron --all-targets`
- `check -p toron-tpch --all-targets`
- `test -p toron --lib` - 1161 passed
- `test -p toron --test architecture_test` - 2079 passed
- `test -p toron --no-fail-fast` - passed all toron targets
- `test -p toron-tpch` - passed all toron-tpch targets
- `test -p toron-tpch adr331_q08_scale_probe` - q02/q05/q08/q09 scale probes passed
- live release `run --query q08 --mode strict --sf 1` - passed
- live release `run --query q09 --mode strict --sf 1` - passed
- one-pass release/cgroup `perf --sf 1 --mode strict --runs 1 --warmup 0 --query all` - 22/22 passed
- full release/cgroup `perf --sf 1 --mode strict --runs 7 --warmup 1 --query all` - 22/22 passed; q10 unstable flag observed
- release/cgroup `perf --sf 1 --mode strict --runs 7 --warmup 1 --query q10` - pass, no flags
- `run -p toron-tpch -- correctness-matrix` - 22/22 `pass@compare`
- `run -p toron-tpch -- matrix` - 22/22 strict/dev `pass@execute`
- `git diff --check`

## Follow-up Regression Coverage

Post-review, the generalized topology gained focused regression tests so the q08/q09-shaped path is not protected only
by live TPC-H runs:

- `reoriented_inner_chain_handles_right_growing_project_join_shape` - compares streamed vs materialized execution for a
  multi-join right-growing `Project(Join)` shape in both Float64 and Decimal modes, including output column-order
  restoration.
- `reoriented_inner_chain_right_growing_shape_removes_materialized_wall` - proves the synthetic right-growing shape
  would hit `join output materialization` without replay ids, while the generalized streaming path runs under the same
  budget.
- Verified with `scripts/cargo-cgroup test -p toron --test reoriented_inner_chain_streaming_test`.

## Carry-Forward

The sf=1 all-query baseline now has live release evidence. The cache-rendered matrix commands should still be treated
as inventory views until a live-rerun mode is added or scale claims are routed through the perf rail.
