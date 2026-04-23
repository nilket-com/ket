# Implementation Scorecard

Date: 2026-04-22
Author Role: Lead Architect
Primary Owner: `WS3 / D`
Supporting Owner: `WS0 / A`

## Purpose

This is the minimum scorecard needed to keep implementation evidence honest.

It exists to answer three questions quickly:

1. what artifact is required for a gate
2. where that artifact lives
3. whether it is actually done

Status vocabulary:

- `done`: artifact exists and is usable now
- `partial`: artifact exists but is not yet enough to satisfy the gate
- `open`: artifact does not exist or is not trustworthy yet

## Gate Rows

| Gate | Row | Status | Owner | Artifact / Source | Notes |
|---|---|---|---|---|---|
| `M0 Foundation` | ADR wave accepted/amended through `034` | `done` | `A` | `plans/adr/003,009,011,018,020,021,025,027,029,030-034` | Current architecture baseline |
| `M0 Foundation` | Design-lock register | `done` | `A` | `plans/gap/design_lock_20260422.md` | Program-of-record narrow decisions |
| `M0 Foundation` | Implementation roadmap | `done` | `A` | `plans/impl/roadmap.md` | Current milestone/dependency map |
| `M0 Foundation` | Artifact registry | `done` | `D` | `plans/impl/artifact_registry.md` | Minimal `A-lite` support for `D` |
| `M0 Foundation` | Scorecard | `done` | `D` | `plans/impl/scorecard.md` | This file |
| `M1 Correctness Locked` | B transition table | `done` | `B` | `plans/impl/workstream_B_transition_table.md` | |
| `M1 Correctness Locked` | B fault-injection report | `done` | `B` | `plans/impl/workstream_B_fault_injection_report.md` | |
| `M1 Correctness Locked` | B replay/idempotency note | `done` | `B` | `plans/impl/workstream_B_replay_idempotency_note.md` | |
| `M1 Correctness Locked` | B operator note | `done` | `B` | `plans/impl/workstream_B_operator_notes.md` | |
| `M1 Correctness Locked` | B write-path benchmark delta | `open` | `B` + `D` | reserved in `artifact_registry.md` | Harness exists; measured delta report does not |
| `M1 Correctness Locked` | D metric catalog | `done` | `D` | `plans/impl/workstream_D_metric_catalog.md` | D1 output |
| `M1 Correctness Locked` | D trace inventory | `done` | `D` | `plans/impl/workstream_D_trace_inventory.md` | D2 first-pass live span inventory is on disk |
| `M1 Correctness Locked` | D chaos matrix | `done` | `D` | `plans/impl/workstream_D_chaos_matrix.md` | Matrix is on disk and every planned first-pass row is now live, including isolated-leader partition-plus-restart |
| `M1 Correctness Locked` | D simulator registry | `done` | `D` | `plans/impl/workstream_D_simulator_registry.md` | D4 registry is complete and every planned simulator row is now live |
| `M1 Correctness Locked` | D property suite | `done` | `D` | `plans/impl/workstream_D_property_suite.md`, `tests/property_suite.rs` | D5 minimum property suite is live in ordinary CI |
| `M2 Scale & Survive` | C admission snapshot note | `open` | `C` + `D` | reserved in `artifact_registry.md` | Code landed in `C1`; canonical note does not exist yet |
| `M2 Scale & Survive` | C execution-topology note | `open` | `C` + `D` | reserved in `artifact_registry.md` | WAL/slab split is live; note is still missing |
| `M2 Scale & Survive` | C overload-limit matrix | `open` | `C` + `D` | reserved in `artifact_registry.md` | Limits and tests exist; operator-facing matrix does not |
| `M2 Scale & Survive` | C panic/poison disposition note | `open` | `C` + `D` | reserved in `artifact_registry.md` | C6 closed code paths; evidence note is still missing |
| `M2 Scale & Survive` | C overload-limit implementation | `done` | `C` | code + tests under `src/network/server.rs` | Listener and post-auth bounds are live |
| `M2 Scale & Survive` | C execution-topology benchmark harness | `done` | `C` | `benches/io_topology.rs` | Shared vs split modes both compile |
| `M2 Scale & Survive` | C benchmark numbers on real hardware | `open` | `C` + `D` | reserved in `artifact_registry.md` | Sandbox compile only, no authoritative numbers |
| `M2 Scale & Survive` | D critical-path metric ownership | `done` | `D` | `plans/impl/workstream_D_metric_catalog.md` | B/WAL, migration-proxy, and `io_uring` rows are live |
| `M2 Scale & Survive` | D trace inventory | `done` | `D` | `plans/impl/workstream_D_trace_inventory.md` | Request-entry and pending-reconcile span inventory is on disk |
| `M2 Scale & Survive` | D soak spec | `done` | `D` | `plans/impl/workstream_D_soak_spec.md` | D6 workload shape and promotion rule are fixed |
| `M2 Scale & Survive` | D soak report | `partial` | `D` | `plans/impl/workstream_D_soak_report.md` | Template is present; authoritative measured run is still pending |
| `M3 Customer Pilot Ready` | Audit/trace correlation inventory | `open` | `D` + `G` | reserved in `artifact_registry.md` | Depends on D2 and G |
| `M4 Regulated Pilot Authorization` | Hostile-failure evidence package | `open` | `D` | reserved in `artifact_registry.md` | Depends on D3-D6 |

## Immediate D Priorities

These are the next scorecard rows expected to move:

1. `C` admission snapshot note, execution-topology note, and overload-limit matrix from `open` to `done`
2. `C` benchmark numbers on real hardware from `open` to `done`
3. `B` write-path benchmark delta from `open` to `done`
4. `D` turn the now-live metric families into dashboard and alert inventory instead of adding more placeholder series
5. `D` execute the commodity-profile soak and replace the D6 template with measured evidence

## Rules

- A workstream is not “complete” if its promised gate artifact is still `open`.
- Bench harnesses do not count as benchmark evidence.
- Passing tests do not replace an artifact that the roadmap explicitly requires.
- If a row changes state, the artifact path must already be stable enough for someone else to find it without asking.
