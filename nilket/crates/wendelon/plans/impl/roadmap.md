# Wendelon Implementation Roadmap

Date: 2026-04-22
Author Role: Lead Architect
Sources:
- `plans/gap/execution_20260422.md`
- `plans/gap/design_lock_20260422.md`
- `plans/adr/003_stellarator_bifurcated_writes.md`
- `plans/adr/009_io_uring_threading_model.md`
- `plans/adr/011_auto_adaptive_transport.md`
- `plans/adr/018_dual_ca_mtls_identity_authorization.md`
- `plans/adr/020_l2_envelope_encryption_and_dek_lifecycle.md`
- `plans/adr/021_tenant_aware_architecture_day_one.md`
- `plans/adr/025_audit_event_model_and_tamper_evident_hash_chain.md`
- `plans/adr/027_observability_posture.md`
- `plans/adr/029_identity_vs_locality.md`
- `plans/adr/030_deployment_profiles_and_support_envelope.md`
- `plans/adr/031_production_key_custody_provider_stack.md`
- `plans/adr/032_schema_evolution_and_control_ring_contract.md`
- `plans/adr/033_backup_restore_and_node_replacement_architecture.md`
- `plans/adr/034_toron_wendelon_integration_contract.md`
- `plans/gap/codex_workstream_A_20260422.md`
- `plans/gap/codex_workstream_B_20260422.md`
- `plans/gap/codex_workstream_C_20260422.md`
- `plans/gap/codex_workstream_D_20260422.md`
- `plans/gap/codex_workstream_E_20260422.md`
- `plans/gap/codex_workstream_F_20260422.md`
- `plans/gap/codex_workstream_G_20260422.md`
- `plans/gap/codex_workstream_H_20260422.md`
- `plans/gap/codex_workstream_I_20260422.md`
- `plans/gap/codex_workstream_J_20260422.md`

## Purpose

This document is the bridge between:

- the merged execution program in `plans/gap/execution_20260422.md`
- the accepted/proposed ADR set in `plans/adr/`
- the implementation workstream plans that will live in `plans/impl/workstream_{A-J}.md`

It is intentionally thinner than the execution plan.

This file does **not** replace the program plan. It does three narrower jobs:

1. turns the execution gates into implementation milestones
2. maps workstreams `A-J` onto those milestones
3. defines the dependency and artifact contract for the implementation docs

Rule: the implementation docs should not re-litigate architecture that is already locked in ADRs. They should describe how we land it.

## Implementation Objective

Move Wendelon from architecture/program planning into a staged implementation program that:

- preserves the Fortune 500 end-state
- keeps commodity hardware as the default deployment profile
- supports better hardware on day 1 by explicit configuration
- closes correctness and recovery gaps before throughput theater
- yields a clean per-workstream implementation backlog instead of ten disconnected mini-plans

## Milestones

The implementation roadmap uses six milestones. They are a direct implementation view of the execution-plan gates.

| Milestone | Gate Alignment | What Must Be True | Primary Workstreams |
|---|---|---|---|
| `M0 Foundation` | `Foundation` | baselines captured, scorecard live, ADR-backed design lock in place, calendar-dominant external work started | `A`, `D`, `J` |
| `M1 Correctness Locked` | `Correctness Locked` | write/finalization semantics are crash-correct, hot-path state model is explicit, admission-path correctness is stable | `B`, `C`, `D` |
| `M2 Scale & Survive` | `Scale & Survive` | WAL/slab execution split exists, overload behavior is bounded, docs and claims are truth-matched to code | `C`, `D` |
| `M3 Customer Pilot Ready` | `Procurement Ready` | production custody, role split, audit baseline, restore/replacement path, and customer-honest runbooks exist | `E`, `F`, `G`, `H` |
| `M4 Regulated Pilot Authorization` | `Pilot Authorization` | Toron boundary is documented and tested, regulated evidence minimums exist, non-author restore drill passes | `H`, `I`, `J` |
| `M5 Regulated GA` | `Regulated GA` | compliance, FIPS, supply chain, and advanced recovery features have the evidence package to support multi-customer regulated GA | `J` plus residual work from `E`, `G`, `H`, `I` |

## Workstream Map

The implementation workstreams preserve the existing `A-J` lettering so the planning lineage remains stable.

| Letter | Execution WS | Domain | Primary Milestones |
|---|---|---|---|
| `A` | `WS0` | Governance, ADR closure, calendar dependencies | `M0-M5` |
| `B` | `WS1` | Storage and Stellarator correctness | `M0-M2` |
| `C` | `WS2` | Transport, admission control, runtime stability | `M1-M2` |
| `D` | `WS3` | Observability, validation, and SLO evidence | `M0-M4` |
| `E` | `WS4` | Crypto custody and key lifecycle | `M0-M4` |
| `F` | `WS5` | Identity, RBAC, authorization, governance | `M0-M4` |
| `G` | `WS6` | Audit, evidence, retention | `M0-M4` |
| `H` | `WS7` | Node replacement, DR, operator runbooks | `M1-M4` |
| `I` | `WS8` | Toron and ecosystem integration | `M2-M4` |
| `J` | `WS9` | FIPS, supply chain, release compliance | `M0-M5` |

## Dependency Rules

These are the minimum dependency rules the implementation docs must obey.

1. **`A` gates semantic drift.**
   No workstream may land customer-visible semantics, compliance claims, or operator contracts that contradict the ADR/design-lock set maintained by `A`.

2. **`B` gates truth on write correctness.**
   No performance claim, operator automation claim, or customer pilot claim outruns `B`. If the write/finalization path is not crash-correct, the rest is decoration.

3. **`C` may run in parallel with `B`, but not ahead of it semantically.**
   Runtime refactors may proceed while `B` lands, but `C` must not redefine write semantics by accident. `B` owns the correctness contract; `C` owns the execution shape.

4. **`D` shadows `B` and `C`.**
   Critical-path runtime changes are not complete until the corresponding metrics, traces, chaos coverage, and benchmark deltas land.

5. **`E` and `F` converge at `M3`.**
   Custody and identity may be implemented in parallel, but the customer-pilot bar requires them to compose cleanly: provider choice, cert lifecycle, revocation, and operator authorization must read as one system.

6. **`G` depends on `F` for actor meaning and on `D` for correlation context.**
   Audit cannot stabilize until identity semantics and at least the minimum correlation fields are real.

7. **`H` depends on `B`, `E`, `F`, and `G`.**
   Recovery and node replacement are not credible unless write semantics, custody, auth, and audit are already coherent.

8. **`I` depends on `F`, `G`, and `D`, and is anchored by ADR `034`.**
   Toron integration is not just a client API. It depends on identity handoff, trace propagation, quota/error semantics, and audit attribution.

9. **`J` starts early but closes late.**
   CMVP, FIPS-boundary, SBOM, and release-compliance work should start in `M0`, but they cannot close until transport, custody, identity, and audit surfaces stop moving under them.

## ADR Anchors By Workstream

Each `plans/impl/workstream_*.md` file should cite its load-bearing ADRs directly.

| Workstream | Required ADR Anchors |
|---|---|
| `A` | `003`, `009`, `011`, `018`, `025`, `030`, `031`, `032`, `033`, `034` |
| `B` | `003`, `015`, `016`, `021` |
| `C` | `009`, `011`, `027`, `030` |
| `D` | `027`, `025`, `028` |
| `E` | `020`, `031`, `026` |
| `F` | `018`, `021`, `031`, `034` |
| `G` | `025`, `018`, `027` |
| `H` | `001`, `033`, `025`, `030` |
| `I` | `034`, `018`, `021`, `027`, `032` |
| `J` | `019`, `020`, `026`, `031` |

## Milestone Exit Artifacts

The workstream implementation docs should treat these artifacts as required outputs, not vague aspirations.

### `M0 Foundation`

Required artifacts:

- benchmark baseline set
- CI scorecard definition
- design-lock log
- accepted/amended ADR wave
- external kickoff records for PEN, CMVP, and custody/provider dependencies

### `M1 Correctness Locked`

Required artifacts:

- write-outcome state model in code
- crash/failure injection report
- admission-path benchmark before/after
- panic/poison inventory disposition
- evidence that no stranded quorum-durable writes remain in the injected-failure matrix

### `M2 Scale & Survive`

Required artifacts:

- WAL/slab execution-topology implementation note
- overload/admission-control limits and tests
- metrics/traces for runtime bottlenecks
- benchmark deltas on commodity and premium-supported profiles
- docs truth-up for transport and storage claims

### `M3 Customer Pilot Ready`

Required artifacts:

- production custody provider path
- role split and revocation model
- authoritative audit-evidence story
- `audit verify` or equivalent validation tooling
- cluster backup/restore and node-replacement runbooks
- non-author execution of at least one restore/replacement drill

### `M4 Regulated Pilot Authorization`

Required artifacts:

- Toron/Wendelon integration runbook and contract tests
- regulated-pilot evidence minimums
- security review package
- runbook-only teardown-and-restore drill
- red-team or equivalent hostile-review closure record

### `M5 Regulated GA`

Required artifacts:

- CMVP status/evidence package
- SBOM and signed release artifacts
- supply-chain attestation set
- regulated compliance pack
- advanced recovery/compliance items closed or formally scoped by product tier

## Workstream Document Contract

Each file in `plans/impl/workstream_{A-J}.md` should implement the same shape.

Required sections:

1. `Objective`
2. `Why Now`
3. `ADR Anchors`
4. `Scope`
5. `Out Of Scope`
6. `Dependencies`
7. `Entry Criteria`
8. `Implementation Slices`
9. `Tests And Benchmarks`
10. `Exit Criteria`
11. `Artifacts`
12. `Deferred Follow-Ons`

Implementation rule:

- these documents are for landing work, not re-arguing architecture
- each implementation slice should be small enough to become a real PR series
- hot-path slices must name the metrics and tests that ship with them
- any slice that changes a customer or operator contract must point back to its ADR anchor

## Authoring Order

The implementation docs should be written in this order:

1. `workstream_A.md`
2. `workstream_B.md`
3. `workstream_C.md`
4. `workstream_D.md`
5. `workstream_E.md`
6. `workstream_F.md`
7. `workstream_G.md`
8. `workstream_H.md`
9. `workstream_I.md`
10. `workstream_J.md`

Reason:

- `A` freezes the control surface
- `B` and `C` are the code-critical path
- `D` turns those into evidence
- `E` through `H` make the pilot operationally honest
- `I` and `J` close the regulated boundary

## Immediate Next Step

Use this roadmap as the index and write:

- `plans/impl/workstream_A.md`
- `plans/impl/workstream_B.md`
- `plans/impl/workstream_C.md`
- `plans/impl/workstream_D.md`
- `plans/impl/workstream_E.md`
- `plans/impl/workstream_F.md`
- `plans/impl/workstream_G.md`
- `plans/impl/workstream_H.md`
- `plans/impl/workstream_I.md`
- `plans/impl/workstream_J.md`

The workstream docs should inherit the domain boundaries from the gap-planning set, but translate them into implementation slices, code areas, tests, and gate artifacts.
