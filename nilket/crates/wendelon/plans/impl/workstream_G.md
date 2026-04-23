# Workstream G: Audit, Evidence, And Retention

Date: 2026-04-22
Author Role: Lead Architect
Roadmap Source: `plans/impl/roadmap.md`
Execution Source: `plans/gap/execution_20260422.md` `WS6`

## Objective

Make the audit story authoritative, verifiable, and operationally honest enough that customers, operators, and the product all agree on what counts as evidence.

## Why Now

Audit is a boundary, not a logging feature.

If the system does not clearly state:

- which store is authoritative for which event
- which path is materialization only
- which events are fail-closed locally

then every future security or compliance conversation starts from confusion.

## ADR Anchors

- ADR `025`
- ADR `018`
- ADR `027`

## Scope

- canonical audit schema
- authority versus materialization model
- data-access audit events
- local NDJSON chain posture
- archive or WORM path
- verification tooling
- trace correlation fields where appropriate
- optional Kafka canonicalization if it is later used for evidence

## Out Of Scope

- cert lifecycle implementation
- FIPS artifact packaging
- broad SIEM productization beyond the minimum evidence path

## Dependencies

- `A` for the authoritative evidence decision and status tracking
- `F` for principal and role meaning
- `D` for correlation fields and observability alignment
- `E` where key events require audit coverage

## Entry Criteria

- ADR `025` is the authority for event-model semantics
- pilot evidence posture is chosen:
  - local NDJSON plus archival remains authoritative baseline
  - Kafka is optional unless explicitly promoted with full semantics

## Implementation Slices

### G1. Canonical Event Schema

Implement the typed schema for:

- admin/control events
- local authz or handshake failures
- data-access events

Free-form logs are not audit records.

Status: complete.
Artifact: `plans/impl/workstream_G_canonical_schema_reference.md`

### G2. Authority Versus Materialization

Encode the model explicitly:

- Raft-authoritative events for state-machine mutations
- local-authoritative events for node-local security and access failures
- NDJSON materialization on every serious deployment

This should be visible in code and docs.

Status: complete.
Artifact: `plans/impl/workstream_G_evidence_path_note.md`

### G3. Data-Access Event Coverage

Add at least:

- `data.read`
- `data.write`
- `data.range_query`
- denial or completion forms where appropriate

Sampling policy should be explicit rather than implied.

Status: complete.
Artifact: `plans/impl/workstream_G_sampling_policy_note.md`

### G4. Verification Tooling

Build `audit verify` or an equivalent tool that can:

- verify chain continuity
- verify replay-forward where applicable
- report missing or malformed evidence

Status: complete.
Artifact: `plans/impl/workstream_G_audit_verify_usage.md`

### G5. Archive And WORM Layer

Add the baseline archive path on top of local authority:

- off-box retention
- optional object-lock or WORM support
- documented operator retention duties

Status: complete.
Artifact: `plans/impl/workstream_G_archive_retention_guide.md`

### G6. Optional Kafka Evidence Path

Do not make Kafka the baseline.

If Kafka is later used as an evidence-grade stream, it must carry the canonical chain or authority fields needed for replay and verification.

Status: complete.
Artifact: `plans/impl/workstream_G_kafka_evidence_path_note.md`

## Tests And Benchmarks

- canonical serialization tests
- chain-continuity verification tests
- local-authority fail-closed tests
- replay-forward tests for Raft-authoritative events
- data-access event coverage tests
- archive rotation and retention tests
- verification-tool integration tests

## Exit Criteria

- the authoritative evidence story is explicit and implemented
- local NDJSON chain exists and is verifiable
- data-access audit coverage exists for the minimum serious set
- verification tooling can validate the baseline evidence path
- archive expectations are documented for operators

## Artifacts

- canonical audit schema reference
- evidence-path note
- sampling-policy note
- `audit verify` usage guide
- retention and archive guide
- Kafka evidence-path note

## Deferred Follow-Ons

- Kafka evidence-grade mode if later justified
- broader SIEM packaging
- longer-horizon retention profiles per regulated tier
- more advanced attestation packaging for external auditors
