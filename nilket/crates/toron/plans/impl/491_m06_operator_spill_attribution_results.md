# ADR 491 implementation receipt — m06 spill attributed

The residual `88,559,536` whole-query spill bytes on m06 are entirely owned by join
replay 0's `partitioned_streaming_scan_build` path. They are not scan or aggregate
spill, and they are not evidence that ADR 490's local admission declined.

## Live receipt

One fresh release parent/child row at the settled ADR 490 coordinate (2 GiB task,
2 GiB spill reserve, sf=8, strict parquet) recorded:

| plan | admission | runtime spill owner | attributed / task spill | execute | peak RSS |
|---|---|---|---:|---:|---:|
| `01b38b33108cf032` | `admitted(estimate=1440000000,budget=2147483648)` | replay 0 `partitioned_streaming_scan_build` | `88,559,536 / 88,559,536` B | 15,101.1 ms | 477,240 KiB |

The standing digest and one-row output passed. The row is preserved under
`target/tpch/adr491_spill_attribution3/` and identifies the binary as
`ab1359a1268c-dirty`, as expected for this instrumentation cut.

## Finding

All production writes to the task spill counter are join-owned. The live receipt
narrows this query further: the fused global-aggregate visitor selects
`try_visit_partitioned_streamed_inner_scan_build_scan_probe_chunks`. That path derives
its own streaming-scan build estimate and, when the estimate exceeds task memory,
partitions and spills the scan build internally. Removing binder exchanges therefore
eliminated exchange work and delivered ADR 490's 27.6% win, but did not change this
separate fused-executor dispatch.

This also exposes the next policy seam: binder admission uses the shared conservative
row/key estimate (`1.44 GB`), while the fused scan path independently estimates the
same build above the 2 GiB task budget. The next optimization must reconcile those
two estimates or add a budget-proven resident streaming-scan build. It must not simply
ignore the fused path's residency guard.

## Shipped rail

`TaskTelemetry.join_spill_observations` carries typed, replay-keyed runtime path and
spill-byte observations. The perf rail serializes them alongside the task total, so
future work can prove both ownership and byte reconciliation. Legacy serialized task
telemetry remains readable through the field's default.
