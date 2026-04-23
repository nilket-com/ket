# Workstream H Drill Report

Date: 2026-04-23
Author Role: Lead Architect
Workstream: `H6`

## Purpose

Record the operational drill evidence for the H recovery contracts.

This file is intentionally honest about the current state:

- the runbooks exist
- the fallback replacement harness exists
- the non-author drill gate is still pending real execution evidence

## Current Status

| Scenario | Runbook / Contract | Status | Notes |
|---|---|---|---|
| Node replacement | `plans/ops/runbooks/node_replacement.md` | `ready` | Supported contract exists; live proof is in automated multinode test coverage |
| Cluster restore | `plans/ops/runbooks/cluster_restore.md` | `ready` | Restore helper and readiness gate are live |
| Pending-finalization backlog distress | `plans/ops/runbooks/pending_finalization_backlog_distress.md` | `ready` | Metrics and trace seams are live |
| Audit sink/archive distress | `plans/ops/runbooks/audit_sink_archive_distress.md` | `ready` | Audit lag and sink-failure signals are live |
| Non-author replacement or restore drill | this file | `pending` | Required by H6 exit criteria; not satisfied by author-only sandbox work |

## What Counts As A Successful Drill

For H6, a successful drill must:

1. be executed by someone other than the author of the runbook
2. use the published runbook and current recovery contract rather than ad hoc
   tribal knowledge
3. produce timestamps for start, key milestones, and completion
4. record whether membership, readiness, and catch-up verification all passed
5. record any runbook ambiguity or missing preconditions discovered during the
   drill

## Required Evidence Bundle

Each drill entry should attach or cite:

- cluster size and topology
- trigger condition
- operator executing the drill
- runbook used
- timestamps:
  - incident declared
  - replacement or restore started
  - membership converged
  - readiness returned
  - catch-up verified
- result:
  - success
  - partial
  - failure
- follow-up defects or doc changes

## Drill Log

| Date | Scenario | Executor | Result | Duration | Evidence Path | Notes |
|---|---|---|---|---|---|---|
| pending | non-author restore or replacement drill | pending | pending | pending | pending | H6 gate still open until this row is replaced with real execution evidence |

## Current Non-Substitutes

The following do not close the H6 drill gate by themselves:

- automated tests
- author-only tabletop review
- no-op harness dry runs
- implementation notes without timestamped execution evidence

They are useful support evidence, but the exit criterion is still
non-author execution.
