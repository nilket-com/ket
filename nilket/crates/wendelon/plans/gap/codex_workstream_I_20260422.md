# Workstream I: Toron And Ecosystem Integration

Date: 2026-04-22
Author Role: Lead Architect
Source: `plans/gap/execution_20260422.md` WS8

## Mandate

Define the compute/storage boundary well enough that Wendelon is not a high-performance island with no credible enterprise operating model around it.

## Architectural Position

For internal engineering pilots, a storage-only story is tolerable. For regulated enterprise pilots, it is not.

My position is:

- Toron should be treated as Wendelon's most important customer, not as a side integration
- the Toron integration boundary must be documented before regulated pilot
- identity, quota, audit, and trace semantics must cross the compute/storage boundary coherently
- connector work should start as documentation and reference patterns before it becomes a full product line

## Scope

- Toron integration contract
- SQL/JDBC/ODBC/SSO/SAML operational mapping
- trace and observability stitching
- quota/audit visibility from the compute tier
- reference ELT/ingest patterns

## Non-Goals

- building a Wendelon-native SQL layer
- owning the entire BI/connector ecosystem
- broad marketplace integration work beyond reference patterns

## Work Packages

### I1. Boundary Contract

Write the real compute/storage contract:

- who authenticates whom
- where tenant identity lives
- how quotas are surfaced
- how audit attribution flows
- how tracing context crosses layers
- how Toron responds to Wendelon backpressure without turning storage saturation into client chaos

### I2. Toron Runbook

Publish `ops/toron_integration.md` with:

- deployment topology
- identity mapping
- operational checks
- failure domains
- handoff between compute and storage teams

### I3. End-To-End Test Fleet

Create a test environment that actually exercises:

- SQL request path
- compute/storage auth propagation
- Arrow-over-QUIC path
- audit/tracing correlation
- mock IdP or equivalent enterprise auth source

### I4. Driver And Identity Mapping

At minimum define:

- JDBC/ODBC expectations
- SSO/SAML handoff to short-lived Wendelon storage authorization credentials
- who owns the mapping logic

Preferred shape:

- Toron handles enterprise auth
- Toron exchanges or derives short-lived storage credentials for the executing identity
- Wendelon enforces storage semantics without pretending to be the enterprise IdP

### I5. Ingest And ELT Patterns

Document and test reference ingestion paths for:

- Fivetran-like destinations
- Airbyte-like destinations
- dbt-oriented warehouse workflows

I do not require productionized first-party connectors at the first milestone, but I do require an honest documented ingest contract.

## Deliverables

- compute/storage contract note
- `ops/toron_integration.md`
- end-to-end integration test fleet
- driver/identity compatibility matrix
- ELT/ingest reference patterns
- backpressure and identity handoff contract

## Exit Criteria

- the Toron/Wendelon boundary is documented and versioned
- end-to-end tests prove identity, quota, and trace propagation
- regulated-pilot operators can answer "how does SQL auth map to storage auth?" without inventing the answer
- backpressure behavior across the boundary is tested rather than assumed

## Recommended Sequencing

1. boundary contract
2. Toron runbook
3. end-to-end test fleet
4. driver/identity matrix
5. ELT/ingest patterns

## Risks

- Toron and Wendelon evolve incompatible assumptions about identity or observability
- integration doc becomes aspirational and not tied to tests
- connector scope balloons and delays the core contract work
- the team solves the auth story abstractly without ever proving the IdP handoff in an end-to-end test

## Opinionated Constraints

- Do not promise enterprise pilot readiness while the compute/storage auth story is still hand-wavy.
- Do not let connector ambition block the core Toron contract.
- Do not ship a runbook that has never been exercised end to end.
- Do not let Wendelon pretend to be the enterprise identity source when Toron already owns that contract.

## Open Questions

- Which team owns the authoritative identity mapping from SSO/SAML into storage authorization?
- How much of the JDBC/ODBC compatibility matrix must be proven before regulated pilot versus documented as roadmap?
- Are reference ingest patterns enough for Gate 4, or do we need at least one maintained connector implementation?
- What is the minimum trace context that must survive the Toron to Wendelon hop?
- Do we require a concrete SAML-to-short-lived-storage-credential exchange for Gate 4, or is a narrower mock-IdP proof enough at first?
