# Workstream A: Governance, ADR Closure, And Program Control

Date: 2026-04-22
Author Role: Lead Architect
Roadmap Source: `plans/impl/roadmap.md`
Execution Source: `plans/gap/execution_20260422.md` `WS0`

## Objective

Freeze the control surface for implementation so the rest of the program can land against stable semantics, stable claims, and visible gates.

## Why Now

If this workstream is weak, every other workstream pays for it twice:

- once when code lands against unresolved assumptions
- again when docs, tests, or customer claims have to be rewritten later

This is not paperwork. It is the implementation control plane for the program.

## ADR Anchors

- ADR `003`
- ADR `009`
- ADR `011`
- ADR `018`
- ADR `025`
- ADR `030`
- ADR `031`
- ADR `032`
- ADR `033`
- ADR `034`

## Scope

- ADR status normalization and acceptance tracking
- design-lock maintenance
- milestone/gate scoreboard
- benchmark and validation artifact registry
- external dependency tracking:
  - IANA PEN
  - CMVP/lab
  - custody-provider onboarding
- claim-discipline enforcement across docs, comments, and release notes

## Out Of Scope

- runtime refactors in storage or transport
- KMS provider implementation
- operator or DR implementation
- Toron integration code

## Dependencies

- none upstream

This workstream is upstream of all other implementation workstreams.

## Entry Criteria

- `plans/impl/roadmap.md` exists and is the implementation index
- the curated ADR backlog exists in `plans/gap/design_lock_20260422.md`
- the amended/new ADR wave through `034` is on disk
- the merged execution plan remains the program-of-record document

## Implementation Slices

### A1. ADR Status Matrix

Create a living status matrix that maps:

- ADR id
- current status
- owning workstream
- blocking milestone
- last review date

This should be machine-readable enough to drive a CI or dashboard view later.

### A2. Design-Lock Register

Extend the current design-lock file into a maintained register with:

- decision
- owner
- date locked
- unblock target
- downstream files/workstreams affected

Design-lock is for narrow decisions that do not deserve a new ADR but do gate implementation.

### A3. Claim-Discipline Pass

Introduce a standing pass over:

- ADR text
- operator docs
- roadmap text
- code comments
- release-facing notes

No document should claim a transport, custody, recovery, or compliance posture that the implementation does not actually support.

### A4. Program Scorecard

Define the scorecard rows for:

- milestone status
- benchmark deltas
- chaos status
- simulator status
- audit/tooling readiness
- runbook drill status

The point is not pretty reporting. The point is visible failure.

### A5. Baseline Artifact Registry

Record where the following artifacts live and who owns them:

- benchmark baselines
- overload reports
- fault-injection reports
- restore drill reports
- compliance evidence indexes

If an artifact matters to a gate, it needs a durable path and an owner.

### A6. External Dependency Tracker

Track calendar-dominant dependencies explicitly:

- PEN filing and assignment
- CMVP/lab contact state
- provider onboarding state for custody hardening

The workstream does not execute those external processes alone, but it does own visibility and unblock tracking.

## Tests And Benchmarks

- link-check or reference-check for roadmap and ADR anchors
- CI smoke that the scorecard inputs resolve to real files or artifact locations
- scheduled review cadence:
  - weekly ADR/design-lock review
  - milestone readiness review before each milestone transition

## Exit Criteria

- every load-bearing ADR is either `Accepted`, still explicitly `Proposed` with a named owner, or deliberately deferred
- design-lock decisions that block implementation have owners and target milestones
- scorecard rows exist for every milestone gate
- artifact locations are defined for all gate evidence
- external calendar dependencies are tracked with current status and next action

## Artifacts

- ADR status matrix
- updated design-lock register
- milestone scorecard definition
- artifact registry
- external dependency tracker

## Deferred Follow-Ons

- automate ADR/status extraction into CI
- build a richer dashboard once the artifact set stabilizes
- add release-branch claim checks when there is a formal release process
