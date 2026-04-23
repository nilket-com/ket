# Workstream H First-Party Operator Direction

Date: 2026-04-23
Author Role: Lead Architect
Workstream: `H5`

## Purpose

Preserve the path to a first-party operator without letting "operator" become
an excuse to blur the system boundary.

This note freezes what the future Wendelon-owned operator is allowed to own and
what it must explicitly not absorb.

## Scope Boundary

The first-party operator is **storage-tier-only**.

It may own:

- failed-member replacement orchestration
- backup-trigger orchestration
- restore orchestration
- drill execution and evidence collection
- readiness / membership / catch-up verification loops around those workflows

It does not own:

- customer application orchestration
- tenant workload placement outside Wendelon's own storage-tier nodes
- generic cluster lifecycle for unrelated services
- IdP, cert-service, or human-authentication policy
- customer workload failover semantics above the storage tier

That boundary is the important H5 decision.

## Replacement Ownership

For the H3 remove-and-replace contract, a future first-party operator owns the
workflow choreography around the already-defined Wendelon primitives.

Operator-owned responsibilities:

- notice a replacement trigger from an approved source
- select the failed member and the fresh replacement identity
- invoke failed-member removal
- select the backup artifact to seed from when warm-start is desired
- provision the replacement host / pod / VM
- start the replacement node with the intended join configuration
- wait for membership convergence, readiness, and catch-up
- emit the execution record for the replacement

Wendelon-owned responsibilities:

- backup artifact semantics
- restore semantics
- readiness semantics
- membership semantics
- authorization and audit semantics
- data-path correctness

The operator is therefore a coordinator around Wendelon authority, not a second
source of truth.

## Restore And Drill Integration

The first-party operator should compose directly with the existing H surfaces:

- H2 restore:
  - `WendelonNode::restore_from_cluster_backup(...)`
- H3 replacement:
  - `WendelonNode::remove_member_for_replacement(...)`
  - `WendelonNode::restore_replacement_from_cluster_backup(...)`
- H4 fallback automation:
  - `scripts/recovery/run_node_replacement_contract.sh`

That means H4 is not throwaway work. It is the behavioral contract the future
operator must subsume and eventually replace with a Wendelon-owned controller.

The operator must also integrate with H6 drills:

- scheduled replacement drills
- scheduled restore drills
- evidence capture for pass / fail / timeout
- report emission suitable for roadmap gate review

## Deployment Direction

The preferred future shape is:

- a thin Wendelon-owned controller
- narrow storage-tier CRDs or equivalent job specs
- explicit adapters for the host environment
  - e.g. Kubernetes, VM automation, or bare-metal hooks

The controller should stay intentionally thin:

- read cluster health and replacement inputs
- invoke Wendelon admin surfaces
- invoke host provisioning hooks
- verify outcomes
- record evidence

It should not become a generic platform.

## Identity And Authorization Expectations

The operator acts through the F/G identity and audit boundaries, not around
them.

That means a first-party operator must use:

- the existing serious operator roles
- ordinary administrative authorization
- ordinary audit emission
- the same replacement and restore verification gates visible to human operators

There is no hidden privileged bypass for the controller.

## Success Criteria For Later Implementation

When the first-party operator actually lands, this note expects it to prove:

- it stays within the storage-tier-only scope
- it performs the same H3 replacement contract as the runbook
- it performs the same H4 success checks as the fallback harness
- it integrates with H6 drills rather than inventing a separate drill story
- it produces auditable execution records suitable for regulated review

## Evidence

- `plans/impl/workstream_H_node_replacement_runbook.md`
- `plans/impl/workstream_H_fallback_automation_contract.md`
- `plans/ops/backup_dr.md`
