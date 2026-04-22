# Workstream H: Node Replacement, DR, And Operator Runbooks

Date: 2026-04-22
Author Role: Lead Architect
Source: `plans/gap/execution_20260422.md` WS7

## Mandate

Close the gap between mathematical healing and real operational healing. This workstream is where Wendelon stops assuming that external humans will fill in the hard parts at 3 AM.

## Architectural Position

The Level-2 healing gap is real. Eviction without replacement is a durability tax, not a full recovery story.

My position is:

- for any serious pilot, a first-party Kubernetes Operator is the preferred answer
- for any system claiming five-nines availability, a first-party operator is effectively mandatory
- fallback automation can exist, but it must be explicit, timed, and testable
- restore procedures must be cluster-scoped, not node-image folklore
- runbooks are release artifacts, not internal notes

## Scope

- node replacement contract
- first-party operator or validated fallback
- cluster-scoped backup and restore choreography
- remote-tier validation during restore
- backup encryption
- recovery drill procedure
- operator runbooks
- PITR direction

## Non-Goals

- Toron compute integration
- CMVP lab lifecycle
- low-level runtime hot-path changes

## Work Packages

### H1. Replacement Contract

Choose and document:

- preferred: first-party Kubernetes Operator
- fallback: external automation contract with:
  - trigger source
  - identity bootstrap
  - replacement SLA
  - verification steps

I do not consider "wire your own autoscaler somehow" to be an acceptable pilot contract.

### H2. First-Party Operator

Ship the operator if we are targeting serious shared-cluster pilots.

Requirements:

- consumes cluster events/metrics
- replaces nodes predictably
- joins with the right identity/bootstrap flow
- exposes replacement timing

Target bar:

- replacement and return to full replication factor within a bounded SLA
- my default target is five minutes or less in the reference deployment

### H3. Cluster-Scoped Backup

Move from node-image thinking to:

- consistent-cut cluster backup
- stable manifesting
- explicit remote-tier assumptions
- logical separation between live L2 data and backup storage

### H4. Restore And Validation

Add:

- restore choreography
- remote-tier validation
- backup encryption handling
- post-restore correctness checks
- execution by people who did not author the feature or the runbook

### H5. Runbooks

Minimum runbooks:

- node replacement
- pending-finalization backlog/replay failure
- audit sink/archive failure
- cluster restore

The runbooks must be written for operators who are not the feature authors.

### H6. PITR Direction

Define whether PITR is:

- required for regulated pilot
- or explicitly deferred to regulated GA

## Deliverables

- node replacement contract
- first-party operator or validated fallback procedure
- cluster backup/restore design and tooling
- recovery drill plan
- required runbooks
- PITR scope note
- runbook-only teardown-and-restore validation artifact

## Exit Criteria

- node replacement path exists and is timed
- cluster restore is documented and testable
- DR drill has defined success criteria
- runbooks are versioned with the codebase
- restore/replacement can be executed by non-authors
- reference replacement SLA is demonstrated in a chaos or drill environment

## Recommended Sequencing

1. replacement contract
2. operator or fallback automation
3. cluster-scoped backup design
4. restore validation flow
5. runbooks
6. PITR scope decision

## Risks

- operator project turns into a second platform instead of a focused control loop
- restore choreography is written but never truly exercised
- runbooks look complete but still require author tribal knowledge
- teams try to reuse the live L2 bucket as backup storage and quietly collapse the failure domains

## Opinionated Constraints

- Do not call the system self-healing if it only evicts.
- Do not publish a DR story that is just "restore the node image and hope."
- Do not let the same engineers who wrote the runbooks be the only people who can execute them.
- Do not collapse backup storage into the same failure domain as the live remote tier unless we are prepared to explain that to customers in plain language.

## Open Questions

- Is the first-party Kubernetes Operator mandatory for Gate 3, or can a tightly specified external automation contract satisfy the first customer-facing pilot?
- Do we require cluster-wide restore tooling before customer-facing pilot, or can the first pilot use a documented manual sequence?
- Is PITR a Gate 4 requirement or a Gate 5 requirement?
- Who owns ongoing runbook quality once the feature teams hand off to operations?
- Must the backup manifest live in storage that is physically separate from the live L2 tier for regulated pilots?
