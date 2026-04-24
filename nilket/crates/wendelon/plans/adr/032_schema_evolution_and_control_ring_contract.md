# ADR 032: Schema Evolution And Control-Ring Contract

## Status
Proposed

## Context
The code review and planning cycle surfaced a structural gap: Wendelon has customer-facing control-plane behavior around schema and cluster metadata, but some of the relevant types and paths still look placeholder-shaped. That is dangerous because schema and control-ring behavior are exactly the kinds of contracts that become very expensive to retrofit after pilot users exist.

Three facts now need to be locked together:

- Wendelon is not a SQL parser and should not pretend to be one
- schema changes still require a durable, ordered, versioned state transition inside Wendelon
- the control ring must have one authoritative membership and epoch source, not hidden parallel truths

This ADR exists to freeze the Wendelon side of that contract before more implementation lands.

## Decision

1. **Schema changes are durable control-ring proposals, not side effects.**
   Wendelon will represent schema evolution as typed, versioned control-ring proposals rather than out-of-band local mutations or free-form SQL strings.

2. **`SchemaUpdate` is the canonical durable schema-change envelope.**
   The exact wire and state-machine shape may evolve additively, but the architectural contract is fixed:
   - a schema change has an explicit tenant and target object
   - it has an expected prior version or generation
   - it has a requested next version or state
   - it is ordered by the control ring
   - it is idempotent at the proposal boundary

3. **Schema intent and schema durability are separate concerns.**
   External systems such as Toron may own:
   - SQL parsing
   - semantic planning
   - higher-level user intent

   Wendelon owns:
   - durable sequencing of schema transitions
   - authoritative current schema state needed by storage and routing
   - replay and restart behavior of schema transitions

4. **Customer-facing control-ring contracts must be real or absent.**
   Placeholder structs or placeholder control paths are not acceptable in customer-facing behavior. A path must be either:
   - implemented enough to be truthful
   - or disabled and omitted from support claims

5. **The control ring has one authoritative membership and epoch source.**
   There must not be a hidden second topology, second epoch authority, or implicit coordination/data split in customer-facing behavior.

## Rationale

- **Schema state is durable state.** Even if Wendelon does not interpret SQL, it still must durably sequence the schema transitions that affect storage and query execution.
- **Typed proposals are safer than ad hoc strings.** They are easier to version, validate, replay, and audit.
- **This protects the Toron boundary.** Toron can evolve as the compute/intent layer while Wendelon remains the durable sequencing authority for the storage side.
- **Placeholder control paths are architecture debt, not harmless scaffolding.** They create false confidence about what the control ring really guarantees.
- **One membership and epoch truth is load-bearing.** Hidden parallel topology truths are how distributed systems acquire impossible-to-debug behavior.

## Consequences (The "Negatives")

- **The schema envelope becomes a versioned product contract.** Changes must be additive or explicitly migrated.
- **Some implementation flexibility is intentionally lost.** That is the point; schema and control-ring behavior should not drift casually.
- **Upstream systems must speak typed intent rather than hoping Wendelon will infer meaning from opaque requests.**
- **Rejected alternative: let schema remain an implementation detail until later.** Rejected because later is exactly when it becomes expensive to change.
- **Rejected alternative: treat SQL strings as the durable schema-change artifact.** Rejected because that pushes parsing and semantic ambiguity into the wrong boundary.
- **Rejected alternative: keep placeholder control surfaces visible and clean them up post-pilot.** Rejected because customer-facing placeholders are documentation debt with failure semantics attached.

## References
- ADR 002: The Unified Hash Ring Topology
- ADR 007: Raft Implementation Strategy
- ADR 021: Tenant-Aware Architecture from Day One
