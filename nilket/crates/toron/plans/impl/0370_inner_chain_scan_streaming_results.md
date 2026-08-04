# ADR 370 results - inner-chain scan streaming

## Summary

ADR 370 removed the measured q07 scan-output miss without broadening the q08/q09 claim.

The Phase 0 diagnosis was not a recognizer miss. The reoriented inner-chain recognizer matched q07 (`stages=4`), but the grouped path prepared downstream stage runtimes through the ordinary build path. That materialized a downstream scan build side before the streaming base could consume it.

Measured producer before the fix:

```text
producer: scan output accumulator
requested: 1,073,762,000
available: 1,073,741,824
plan_fingerprint: 7e6d143c9ef91ee3
materialized scan: TableId(3), projection ordinals 0..7
```

The fix is shape-fenced:

- grouped reoriented chains whose base is `Exchange(Filter(Join(0)))` with a streaming scan probe prepare downstream scan-build runtimes through the streamed scan-build path;
- the base streams the scan probe directly and uses the async downstream probe, so the stage build scan does not enter `materialize_scan_stream`;
- other grouped reoriented-chain shapes keep the existing in-memory runtime preparation and synchronous visitor, preserving the right-growing/q08-q09 topology behavior.

## Live sf=10 outcome

q07 advanced, but did not green. The previous `scan output accumulator` row is gone: a fixed-cap systemd probe under persisted sf=10 produced no memory row and no perf row before the bounded run exited. This is recorded as a no-row/time concern, not a pass claim.

q08 and q09 do not share the q07 miss. They remain at their existing scan-output accumulator walls:

| query | status | execute_ms | peak_rss_kib | producer | requested | available | plan_fingerprint |
| --- | --- | ---: | ---: | --- | ---: | ---: | --- |
| q08 | fail | 1,073.2 | 1,521,964 | scan output accumulator | 1,073,762,000 | 1,073,741,824 | bbbb7f86c01174df |
| q09 | fail | 4,859.3 | 3,110,776 | scan output accumulator | 1,073,762,000 | 1,073,741,824 | 27cd42c5ae8887c2 |

Coverage claim: q07 only, advanced-not-green. q08/q09 remain a separate scan-output miss.

## Tests

```text
scripts/cargo-cgroup fmt -- --check
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup check -p toron --all-targets
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup test -p toron reoriented_inner_chain_removes_intermediate_build_accumulator -- --nocapture
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup test -p toron --test reoriented_inner_chain_streaming_test -- --nocapture
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup test -p toron --test architecture_test
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup test -p toron --no-fail-fast
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup test -p toron-tpch
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup build -p toron-tpch --release
```

Results:

- `toron --no-fail-fast`: green.
- `toron-tpch`: green.
- architecture guard: green, including exec decomposition and no-qNN core checks.
- focused ADR 370 accumulator test: green.
- right-growing reoriented-chain regression test: green.

## Guardrails

- No cap bump.
- Temporary ADR tracing removed.
- No qNN references in core executor modules.
- No q08/q09 overclaim.
- No persisted sf=10 correctness claim for q07, because q07 did not produce a pass row.

## Forward

The q07 scan-output miss is no longer the current blocker; q07 now belongs with the time/no-row family until a row-producing wall is available. q08/q09 remain scan-output reds with a different miss and should be diagnosed separately before any coverage claim.
