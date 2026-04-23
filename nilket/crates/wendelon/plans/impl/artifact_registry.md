# Artifact Registry

Date: 2026-04-22
Author Role: Lead Architect
Primary Owner: `WS3 / D`
Supporting Owner: `WS0 / A`

## Purpose

This file is the durable index for implementation evidence.

It is intentionally simple:

- artifact name
- owning workstream
- canonical path
- gate
- current state

State vocabulary:

- `present`: file exists and is the current canonical location
- `reserved`: path is the agreed future location, but the artifact is not written yet

## Registry

| Artifact | Owner | Canonical Path | Gate | State | Notes |
|---|---|---|---|---|---|
| B transition table | `B` | `plans/impl/workstream_B_transition_table.md` | `M1` | `present` | Write-outcome state model |
| B fault-injection report | `B` | `plans/impl/workstream_B_fault_injection_report.md` | `M1` | `present` | Current injected-failure evidence |
| B replay/idempotency note | `B` | `plans/impl/workstream_B_replay_idempotency_note.md` | `M1` | `present` | Duplicate/restart semantics |
| B operator notes | `B` | `plans/impl/workstream_B_operator_notes.md` | `M1` | `present` | Fenced/degraded operator behavior |
| B write-path benchmark delta | `B` + `D` | `plans/impl/workstream_B_benchmark_delta.md` | `M1` | `reserved` | Harness exists; report does not |
| C admission snapshot note | `C` + `D` | `plans/impl/workstream_C_admission_snapshot_note.md` | `M2` | `reserved` | Canonical note for C1 hot-path state publication |
| C execution-topology note | `C` + `D` | `plans/impl/workstream_C_execution_topology_note.md` | `M2` | `reserved` | WAL/slab rings plus isolated blocking executors |
| C overload-limit matrix | `C` + `D` | `plans/impl/workstream_C_overload_limit_matrix.md` | `M2` | `reserved` | Listener, per-IP, per-connection, and post-auth bounds |
| C benchmark report | `C` + `D` | `plans/impl/workstream_C_benchmark_report.md` | `M2` | `reserved` | Expected to include shared vs split `io_topology` numbers |
| C panic/poison disposition note | `C` + `D` | `plans/impl/workstream_C_panic_poison_note.md` | `M2` | `reserved` | Touched-path panic removals and poison recovery contract |
| D metric catalog | `D` | `plans/impl/workstream_D_metric_catalog.md` | `M1` | `present` | D1 |
| D trace inventory | `D` | `plans/impl/workstream_D_trace_inventory.md` | `M1/M2` | `present` | D2 first-pass live span inventory |
| D chaos matrix | `D` | `plans/impl/workstream_D_chaos_matrix.md` | `M1/M2` | `present` | D3 first-pass matrix and evidence index |
| D simulator registry | `D` | `plans/impl/workstream_D_simulator_registry.md` | `M1/M2` | `present` | D4 complete named scenario registry |
| D property suite note | `D` | `plans/impl/workstream_D_property_suite.md` | `M1/M2` | `present` | D5 minimum property suite |
| D soak specification | `D` | `plans/impl/workstream_D_soak_spec.md` | `M2` | `present` | D6 workload shape and promotion rules |
| D soak report | `D` | `plans/impl/workstream_D_soak_report.md` | `M2/M3` | `present` | D6 report template; measured evidence still pending |
| D dashboard inventory | `D` | `plans/impl/workstream_D_dashboard_inventory.md` | `M2/M3` | `reserved` | Can stay thin at first |
| D alert inventory | `D` | `plans/impl/workstream_D_alert_inventory.md` | `M2/M3` | `reserved` | Can stay thin at first |

## Registry Rules

- If an artifact matters to a gate, it needs a canonical path before the work is “almost done.”
- New evidence should reuse an existing reserved path instead of inventing a new filename late.
- If a reserved artifact becomes obviously unnecessary, delete the row rather than leaving dead placeholders around.
- This registry is allowed to stay small. It should track gate evidence, not every planning note in the repo.
