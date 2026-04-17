# Codex Workstream G: Operations, Observability, And Pilot Readiness

Date: 2026-04-17

Derived from:
- `plans/gap/codex_execution_20260417.md`

## Objective

Make the product operable by an SRE or operator who did not build it, and close the minimum operational surface needed for a controlled production pilot.

This workstream takes the project from "engine plus tests" to "system that can be observed, controlled, deployed, and supported."

## In Scope

- Metrics hardening.
  - histograms
  - dimensional discipline
  - secure exposure
- Distributed tracing / OTEL.
- Health, readiness, and drain surfaces.
- Operator CLI and core admin workflows.
  - backup
  - restore
  - cluster status
  - leadership transfer
  - drain
- Backup/restore maturity and runbook closure.
- Runtime tuning defaults and documented operational rationale.
- Deployment, upgrade, rollback, and operational procedures.

## Out Of Scope For This Pass

- Full enterprise compliance program artifacts outside the product/runtime surface.
- Full multi-tenant operations model.
- Exhaustive tooling for every future operator workflow.

## Primary Deliverables

- Operators can observe cluster health and request flow.
- Operators can perform core lifecycle actions without writing custom Rust.
- Pilot deployment has enough documentation and procedures to be supportable.

## Key Interfaces And Dependencies

- Depends on Workstream A because observability and operator control must attach to the real daemon path.
- Depends on Workstream C for secure exposure of operational surfaces.
- Depends on Workstreams D through F for enough system maturity to define meaningful operational workflows.
- Serves as the final bridge into pilot execution.

## First-Pass Sequencing

1. Define minimum pilot-operability surface.
2. Land metrics/tracing baseline hardening.
3. Land health/readiness/drain surfaces.
4. Land operator CLI for the highest-value workflows.
5. Close backup/restore operational usability.
6. Write runbooks and rollout procedures.

## Validation Shape

- Operators can inspect cluster status through supported surfaces.
- Core admin workflows can be executed through supported tooling.
- Deployment and rollback procedures are documented and testable.
- Observability surface is sufficient to diagnose common bring-up and recovery problems.

## Exit Criteria

- Someone other than the implementation team can observe and operate the system using supported interfaces.
- Pilot deployment has documented procedures, instrumentation, and control surfaces.

## Open Questions

- Which operator CLI commands are required for the first pilot, and which can wait?
- How much OTEL/tracing depth is required before pilot readiness versus later operational maturity?
- Should secure metrics exposure be solved here directly, or partially in Workstream C with final hardening here?
