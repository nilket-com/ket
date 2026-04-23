# Workstream I: Toron And Ecosystem Integration

Date: 2026-04-22
Author Role: Lead Architect
Roadmap Source: `plans/impl/roadmap.md`
Execution Source: `plans/gap/execution_20260422.md` `WS8`

## Objective

Define and implement the first serious compute-storage boundary so Toron can integrate as a real customer of Wendelon rather than a hand-waved future neighbor.

## Why Now

If the Toron boundary is left vague:

- schema ownership drifts
- identity handoff drifts
- quota behavior drifts
- trace and audit correlation drift

and both systems pay the cost later.

## ADR Anchors

- ADR `034`
- ADR `018`
- ADR `021`
- ADR `027`
- ADR `032`

## Scope

- generic compute/Wendelon boundary contract, with Toron as the first intended consumer
- `query_session` identity handoff
- schema intent versus durable schema sequencing
- quota and error semantics
- trace-context propagation
- integration runbook
- reference contract tests
- minimal ecosystem guidance for drivers and ingest paths

## Out Of Scope

- building a Wendelon-native SQL front end
- a full marketplace of connectors
- productizing every downstream BI surface

## Dependencies

- `F` for identity and `query_session` semantics
- `G` for audit attribution
- `D` for trace propagation and evidence
- `C` where transport semantics affect response trailers or context carriage

## Entry Criteria

- ADR `034` is the integration authority
- ADR `032` is stable enough to freeze `SchemaUpdate` shape
- the minimum `query_session` issuance model exists

## Implementation Slices

### I1. Boundary Contract

Write the boundary contract in implementation terms:

- who authenticates whom
- what credential crosses the boundary
- what errors are storage-authored
- what metadata returns to compute
- what the versioning rules are

The Wendelon API vocabulary should remain generic compute-client language rather than Toron-specific names.

### I2. `query_session` Handoff

Implement the short-lived storage-facing identity bridge for user-originated query activity.

This should be:

- tenant-bound
- purpose-specific
- short-lived
- not a general application identity

### I3. Schema Contract

Freeze the shape of the durable schema envelope between compute intent and storage sequencing.

Compute clients, including Toron, may originate intent. Wendelon owns durable ordering.

### I4. Quota And Error Semantics

Define and implement:

- structured quota-failure codes
- success-path quota metadata where cheap enough
- what a compute client is allowed to cache versus what must be propagated as storage truth

### I5. Trace And Audit Correlation

Make cross-system propagation explicit:

- W3C trace context on the transport hop
- stable actor/tenant attribution through to audit

### I6. Integration Runbook And Contract Tests

Publish the operator-facing integration document and back it with a small contract-test fleet:

- identity propagation
- schema transitions
- quota failures
- trace continuity
- audit attribution

### I7. Reference Patterns

Add minimal guidance for:

- JDBC/ODBC expectations
- SSO or SAML handoff shape
- ingest/ELT reference posture

This is a reference layer, not a full connector product line.

## Tests And Benchmarks

- `tests/compute_integration_contract.rs`
- contract tests for `query_session` issuance and use
- schema-transition tests across the boundary
- quota-failure propagation tests
- trace-context continuity tests
- audit attribution tests
- end-to-end integration tests with a mock compute-client harness

## Exit Criteria

- the compute/Wendelon boundary is versioned and documented with Toron as the first intended consumer
- `query_session` handoff is real and tested
- schema ownership no longer depends on informal agreement
- quota, trace, and audit semantics survive the boundary coherently
- the integration runbook exists and the contract-test fleet passes

## Artifacts

- compute/Wendelon boundary spec: `plans/impl/workstream_I_boundary_contract.md`
- integration runbook: `plans/impl/workstream_I_integration_runbook.md`
- contract-test inventory: `plans/impl/workstream_I_contract_test_inventory.md`
- driver and identity-mapping note: `plans/impl/workstream_I_driver_identity_mapping_note.md`
- schema-envelope note: `plans/impl/workstream_I_schema_envelope_note.md`
- Wendelon code surface: `src/integration/compute.rs`
- Wendelon client surface: `crates/wendelon-client/src/lib.rs`

## Deferred Follow-Ons

- broader connector ecosystem commitments
- richer BI or downstream analytics packaging
- more advanced workload pushdown negotiation once the base contract is stable
