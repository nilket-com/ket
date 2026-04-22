# ADR 025: Audit Event Model And Tamper-Evident Hash Chain

## Status
Proposed (amended 2026-04-22)

## Context
Wendelon needs an audit system that is:

- structured
- attributable
- tamper-evident
- usable on day 1

The planning cycle closed several architectural questions that the earlier draft left too open:

- the baseline authoritative audit story
- whether Kafka is required or optional
- how local materialization relates to authoritative storage
- whether data-access audit events belong in the same canonical schema

Audit is not observability. Audit exists to reconstruct who did what, against which tenant or cluster surface, under what authority.

## Decision

1. **Every audit record is a structured event from a canonical schema.**
   Free-form log lines are not audit records.

2. **The baseline sink is local NDJSON with tamper-evident chaining.**
   Every node maintains a local append-only NDJSON audit materialization with a per-node hash chain. This baseline exists in every serious deployment.

3. **Archival shipping is additive.**
   Off-box archival or forwarding may be added, but the baseline architecture does not require Kafka. Kafka remains an optional shipping path, not the mandatory default.

4. **Audit distinguishes authority from materialization.**
   An audit record identifies whether its authoritative durable source is:
   - the Raft log
   - or node-local audit storage

   The local NDJSON chain always exists as a materialization, but not every event's authority is the local file.

5. **State-machine mutations are authoritative in Raft.**
   Tenant lifecycle, membership changes, key events, and other Raft-committed state transitions must have an authoritative audit record in the replicated control path. Local files materialize those events per node.

6. **Non-state-machine security and access events are authoritative locally.**
   Handshake failures, denials, local authz failures, and similar node-local events are authoritative in the node-local audit store and must be fail-closed if they cannot be durably recorded.

7. **Data-access audit events are part of the canonical model.**
   The schema must cover at least:
   - `data.read`
   - `data.write`
   - `data.range_query`
   - the relevant completion and denial forms of those events

8. **Audit correlation fields are allowed but subordinate.**
   Trace IDs or similar correlation fields may be carried when available, but they are not the source of truth for audit identity or ordering.

## Rationale

- **Local NDJSON is the practical baseline.** It is cheap, inspectable, and works on commodity deployments without introducing a Kafka tax.
- **Authority vs materialization is the honest model.** It avoids pretending that local append and Raft commit are one atomic durability domain.
- **Kafka as optional keeps the product honest.** Some deployments want it; many do not. The architecture should not force it as the baseline.
- **Data access belongs in the same audit model.** Splitting "admin audit" and "data access audit" into separate schemas would create duplication and ambiguity.

## Consequences (The "Negatives")

- **Audit schema ownership is real work.** Every new auditable operation becomes a typed schema addition rather than a casual log statement.
- **Fail-closed local audit events are operationally sharp.** If the authoritative local audit store is unavailable, certain local-security events must block rather than go unaudited.
- **Per-node chains still need archival handling.** Long-term retention and WORM behavior are additional layers on top of the baseline, not magically solved by the local file alone.
- **Rejected alternative: Kafka as the only serious audit path.** Rejected because it imposes the wrong baseline dependency.
- **Rejected alternative: local files only with no authority distinction.** Rejected because it falsely collapses Raft-committed and node-local events into one durability story.
- **Rejected alternative: free-form audit logging.** Rejected because it cannot support stable verification and external evidence handling.

## Open Questions

- **Canonical encoding choice and versioning details.** The schema must stay versionable without breaking verification tooling.
- **Signed-root details.** Whether audit roots use the node identity key or a dedicated signing key remains a follow-up security choice.
- **Retention policy specifics.** Exact day counts, WORM modes, and compliance-tier defaults are policy-layer decisions, not blockers to the event model itself.

## References
- ADR 018: Dual-CA mTLS Identity And OID-Bound Authorization
- ADR 021: Tenant-Aware Architecture from Day One
- ADR 027: Observability Posture
