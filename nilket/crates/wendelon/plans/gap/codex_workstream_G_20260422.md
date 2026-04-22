# Workstream G: Audit, Evidence, And Retention

Date: 2026-04-22
Author Role: Lead Architect
Source: `plans/gap/execution_20260422.md` WS6

## Mandate

Make the audit story coherent enough that operators, auditors, and the product itself agree on what counts as evidence.

## Architectural Position

We must choose one evidence story and then build around it. Right now the system has an authoritative local chain and optional off-box distribution paths. That is fine, but it must be stated and implemented cleanly.

My position is:

- if Kafka is marketed as a serious compliance path, it must carry chain-replay fields
- for the first serious pilot, local NDJSON plus durable off-box archival is still the cleanest authoritative evidence story
- if local NDJSON is the true evidence archive, then we must stop speaking about off-box streams as if they are equivalent
- data-access audit is not optional for enterprise posture
- retention defaults must match enterprise reality, not engineering-log intuition

## Scope

- data-access audit events
- authoritative evidence-path decision
- Kafka canonicalization if Kafka is in-scope as evidence
- WORM/object-lock archive support
- enterprise retention defaults
- audit verification tooling
- trace-to-audit correlation

## Non-Goals

- cert lifecycle and role split
- CMVP and supply-chain work
- general DR/runbook work outside audit evidence

## Required Decision

Pick one pilot evidence posture:

1. local NDJSON + off-box archival is the evidence source of truth
2. Kafka is elevated into an evidence-grade stream by carrying chain fields and verification semantics

My bias: for the first serious pilot, keep local NDJSON as the authority and build reliable archival around it. Promote Kafka only if we are willing to carry the full chain semantics there.

## Work Packages

### G1. Data-Access Audit

Add `data.read`, `data.write`, and any additional high-value access events.

Requirements:

- clear tenant attribution
- clear actor attribution
- sampling policy explicit and documented

Recommended default policy:

- writes: `100%`
- reads for regulated tenants: `100%`
- reads for non-regulated tenants: sampled, but explicit
- cross-tenant-principal reads: `100%`
- range-query starts: `100%`

### G2. Evidence Path Decision And Packaging

Document:

- what is authoritative
- what is distribution-only
- what operators must retain

This should appear in both code comments and operator docs.

### G3. Kafka Canonicalization

If Kafka is part of the pilot evidence story:

- include chain fields
- include schema/version fields needed for replay
- make verification possible off-box

If not, say so clearly and stop implying equivalence.

### G4. WORM Support

Add object-lock/WORM archival path for long-term retention and external evidence durability.

### G5. Retention Defaults

Move retention from pilot-friendly to enterprise-safe defaults, with tiered policy controls where needed.

My bias is to move straight to a seven-year default for regulated tiers rather than pretending ninety days is a neutral starting point.

### G6. Verification Tooling

Ship a verification/attestation tool that can:

- replay local chains
- validate continuity
- compare chain heads
- produce operator/auditor consumable output

### G7. Trace Correlation

Where execution context carries trace identity, audit events should preserve enough correlation data that operators can move from "who touched data" to "what path did that operation take."

## Deliverables

- expanded audit event schema
- evidence-path design note
- Kafka payload decision and implementation
- WORM archive support
- updated retention defaults and docs
- audit verification tool
- trace-correlation policy and implementation

## Exit Criteria

- the audit evidence story is explicit and non-contradictory
- data-access events exist with real attribution
- operators can verify chain continuity without ad hoc scripts
- retention posture is enterprise-safe for the intended pilot class
- audit records can be correlated back to trace context where present

## Recommended Sequencing

1. data-access schema
2. evidence-path decision
3. canonical Kafka or explicit non-equivalence statement
4. WORM/archive support
5. retention defaults
6. verification tooling
7. trace correlation

## Risks

- event volume explodes without enough filtering or storage planning
- Kafka path remains half-authoritative and confuses customers
- retention promises are made before archive durability is proven
- trace correlation overpromises if the surrounding observability data expires much earlier than the audit trail

## Opinionated Constraints

- Do not market Kafka as evidence unless it actually carries evidence.
- Do not add data-access audit events and then hide them behind vague sampling defaults.
- Do not leave auditors dependent on local ad hoc parsing and tribal knowledge.
- Do not keep a ninety-day default and call the result enterprise-ready.

## Open Questions

- For the first customer-facing pilot, is local NDJSON plus WORM archive enough, or do we need evidence-grade Kafka immediately?
- What is the default sampling posture for read events in a regulated pilot?
- Do we want syslog to remain intentionally thin, or should it also gain chain metadata?
- How do we present verification results so operators can actually use them under pressure?
- Do we standardize a seven-year default now, or only for regulated tenant/compliance tiers?
