# ADR 034: Toron-Wendelon Integration Contract

## Status
Proposed

## Context
Wendelon is the storage ring. Toron is the compute ring. That sounds clean until the first real boundary decisions show up:

- who owns user/session identity at the storage RPC boundary
- who owns schema intent vs durable schema state
- how quota results surface back to compute clients
- what trace context must survive the hop
- what kind of compute pushdown is even allowed

If these are left as ad hoc API details, the two systems will drift into incompatible assumptions. This ADR fixes the first durable contract between the compute and storage rings.

## Decision

1. **Toron owns compute intent; Wendelon owns storage durability.**
   Toron owns:
   - SQL parsing
   - semantic planning
   - user-facing session lifecycle
   - compute orchestration

   Wendelon owns:
   - durable storage semantics
   - replicated sequencing of storage-visible schema transitions
   - authoritative storage quota decisions
   - storage-side authorization enforcement

2. **User-originated storage access crosses the boundary as a short-lived `query_session` identity.**
   Toron is responsible for initiating the exchange from user/session identity into the short-lived Wendelon-facing `query_session` credential defined by ADR `018`. That credential is:
   - tenant-bound
   - purpose-specific to Wendelon RPCs
   - short-lived by design
   - not a substitute for a general tenant-application PKI

3. **Schema intent and schema durability are separate by contract.**
   Toron may originate schema intent, but Wendelon is the durable sequencing authority for `SchemaUpdate` and any other storage-visible schema transition. Toron does not get to mutate durable schema state out of band.

4. **Trace context propagation is required.**
   The Toron→Wendelon hop must preserve W3C trace context so a single user request can be traced across compute and storage components. Trace correlation is required transport behavior, not an optional client feature.

5. **Quota results are storage-authored.**
   Wendelon is the source of truth for storage quota outcomes. Toron may cache or present them, but it must propagate:
   - quota failures as structured, storage-authored failures
   - and any supported usage metadata carried back on successful responses, including response trailers where the transport supports them cheaply

6. **Compute pushdown is constrained and versioned.**
   The integration contract does not permit arbitrary code execution in Wendelon. Pushdown must use a constrained, versioned compute AST or operator set that Wendelon can validate and execute safely.

7. **The boundary is a jointly versioned contract.**
   Changes to identity handoff, `SchemaUpdate` payload shape, quota/error semantics, or pushdown operator compatibility require coordinated versioning between Toron and Wendelon. The systems are allowed to evolve independently, but not by silently changing this boundary.

## Rationale

- **This prevents boundary confusion.** Compute orchestration and durable storage sequencing are different jobs and should stay that way.
- **Short-lived query-session identity is the right trust bridge.** It lets Toron remain the user/session front door without making Wendelon depend on Toron's internal session model.
- **Schema durability must not be a side effect of query planning.** Storage-visible schema changes need ordered, replayable sequencing.
- **Trace and quota context are operationally essential.** Without them, the boundary is opaque to operators and users alike.
- **Constrained pushdown preserves the storage-layer mandate.** Wendelon stays a fast storage engine rather than becoming a general compute sandbox.
- **Joint versioning prevents silent drift.** The boundary should feel like a product contract, not an informal implementation detail.

## Consequences (The "Negatives")

- **The two systems now share a real versioned contract.** That increases coordination cost, but it is cheaper than silent drift.
- **Toron must participate in storage-identity discipline.** It cannot treat Wendelon as a dumb blob sink with hidden side effects.
- **Some future feature requests will be rejected at the boundary.** Arbitrary in-storage compute and out-of-band schema mutation are examples.
- **Quota UX becomes a cross-system obligation.** Toron cannot suppress or reinterpret storage-authored quota outcomes without breaking the contract.
- **Rejected alternative: let Toron own durable schema state.** Rejected because schema durability belongs with the storage system that must replay and enforce it.
- **Rejected alternative: let Wendelon parse and own SQL semantics.** Rejected because that collapses the compute/storage separation in the wrong direction.
- **Rejected alternative: use opaque internal tokens with no storage-facing session credential.** Rejected because it weakens the trust and audit boundary at the storage RPC surface.

## References
- ADR 005: Decoupling Compute via Pushdown Bytecode (PBC)
- ADR 018: Dual-CA mTLS Identity And OID-Bound Authorization
- ADR 021: Tenant-Aware Architecture from Day One
- ADR 032: Schema Evolution And Control-Ring Contract
