# ADR 036: WPBC as the Toron-to-Wendelon Pushdown Contract

## Status
Accepted

## Context
Wendelon is the active storage tier. Toron is the compute tier. The two systems co-evolve, but they must not collapse into one engine.

The user-facing language hierarchy is:

- Rune as the primary programmable interface
- SQL as the compatibility interface
- `toron-ir` as the durable planner and optimizer contract
- WPBC as the constrained storage-side physical pushdown target

This distinction matters. If Wendelon starts accepting Rune, SQL fragments, or arbitrary UDFs, it becomes a compute sandbox with storage attached. If Toron cannot express rich pushdown intent, Wendelon becomes a passive byte store and Toron pays unnecessary network and memory costs.

This ADR refines ADR 034's "compute pushdown is constrained and versioned" principle into an explicit contract before vectorized WPBC (ADR 035) grows new operators.

## Decision
WPBC is a versioned physical expression format emitted by Toron for storage-local work. It is not a user language, not a SQL dialect, and not a Rune bytecode format.

1. **Toron owns language lowering.**
   Rune code, SQL text, visual query builders, and future native APIs all lower into typed `toron-ir`. Toron's optimizer decides which expression fragments are targetable to Wendelon. There is no direct Rune-to-WPBC compiler path inside Wendelon.

2. **Wendelon owns validation and execution safety.**
   Wendelon treats WPBC as untrusted input. It validates bytecode length, stack depth, opcode families, column indices, supported Arrow types, and version/capability compatibility before execution.

3. **Pushdown targetability is a first-class planner property.**
   Toron tracks whether each expression node, not merely each whole query, is:
   - `WendelonExact`: can execute in WPBC with exact semantics
   - `WendelonApprox`: can execute as an approximate or prefilter operation, such as future vector ANN candidate selection
   - `ToronOnly`: must execute in Toron
   - `Rejected`: not valid under the session, tenant, schema, or safety policy

4. **WPBC V1 remains predicate plus projection.**
   The vectorized workstream (ADR 035) does not add joins, windows, global sorting, cross-batch state, arbitrary UDFs, or full aggregations. Those stay in Toron unless a later ADR defines a bounded partial-aggregation extension.

5. **Rune-specific behavior cannot leak into Wendelon.**
   Rune is the preferred authoring surface, but Wendelon never depends on Rune ASTs, Rune VM behavior, or Rune package versions. Rune standard-library functions that are pushdown-capable must lower to typed `toron-ir` functions, then to WPBC opcodes or a rejected targetability result.

6. **SQL-specific behavior cannot define WPBC semantics.**
   SQL compatibility is important, but WPBC semantics are specified in Wendelon terms: Arrow data types, three-valued logic where accepted, deterministic comparison behavior, and explicit typed identity/locality operations. SQL parser quirks do not become storage semantics.

7. **WPBC capability negotiation becomes mandatory before new opcodes.**
   The current opcode set remains implicit for Wendelon V1. Any new operator family after vectorized parity is gated by a WPBC version or capability bit surfaced through the Toron-Wendelon query-session handshake from ADR 034. A standalone capabilities RPC is not introduced until the handshake shape proves insufficient.

8. **Saved work persists above WPBC.**
   Saved Rune notebooks, SQL dashboards, and visual query graphs persist as source plus `toron-ir` or a versioned Toron artifact. Compiled WPBC is an execution artifact regenerated for the current Wendelon topology, schema, and capabilities. WPBC is a wire format, not a storage format.

9. **Pushdown rejections use stable reason codes.**
   Wendelon may include detailed diagnostic context in logs/traces, but the Toron-facing contract uses enumerated reason codes such as `WPBC_REJECT_UNSUPPORTED_OPCODE`, `WPBC_REJECT_TYPE_UNSUPPORTED`, `WPBC_REJECT_VERSION_INCOMPATIBLE`, `WPBC_REJECT_PROGRAM_TOO_LARGE`, and `WPBC_REJECT_STACK_DEPTH`. Toron maps those to user-facing diagnostics. No free-form error strings cross the boundary.

10. **Partial aggregates are not WPBC V2 by default.**
    Predicate/projection bytecode and partial aggregation state machines have different memory, safety, and numerical-stability concerns. The default position is a sibling pushdown protocol for partial aggregates unless a later ADR proves an opcode extension is cleaner.

11. **Targetability diagnostics belong on Toron expression nodes.**
    The `WendelonExact`/`WendelonApprox`/`ToronOnly`/`Rejected(reason_code)` label is carried per `toron-ir` expression node, not per whole query. Useful plans routinely split: a pushable filter leaf may sit under a Toron-only window, join, or final aggregate. Query-level labels would throw away that optimization space.

## Rationale
The main product bet is not "SQL but faster." The bet is a data stack where a serious programmable environment can lower rich analytical work into a distributed Arrow engine, while storage-local fragments execute at memory bandwidth beside the data.

That requires a narrow waist. `toron-ir` is the semantic waist above the optimizer. WPBC is the physical waist at the storage boundary. Mixing those layers would make every future feature harder to reason about.

Keeping WPBC V1 to predicate and projection also protects implementation momentum. Vectorized execution (ADR 035) is enough of a change without simultaneously adding stateful aggregates, vector search, JSON, or UDFs.

Per-node targetability matters because useful plans often split: a pushable filter leaf may sit under a Toron-only window, join, or final aggregate. Query-level labels would throw away that optimization space.

## Consequences (The "Negatives")
Toron has to do real compiler work. It cannot pass SQL strings or Rune snippets to Wendelon and hope storage figures it out.

Some attractive features will be delayed. Rune UDF pushdown, partial aggregation, vector distance kernels, sketches, and time-series operators all need explicit targetability rules and separate acceptance tests.

The same logical expression may have different execution placements depending on Wendelon version, Arrow type, tenant policy, and deployment profile. Toron's planner must model that instead of hardcoding a single pushdown path.

Wendelon error messages must be good enough for Toron to explain pushdown rejections without exposing storage internals directly to end users. Stable reason codes reduce leakage risk but make the enum part of the compatibility contract.

The contract creates a compiler obligation on Toron: every candidate pushdown needs an explicit targetability reason. That is more work than opportunistic string translation, but it prevents silent semantic drift between Rune, SQL, and storage execution.

## Deferred Decisions
Bounded implementation-time decisions against this fixed contract. Each becomes its own ADR or implementation note when the first consumer forces the answer.

1. **Exact reason-code set.** The final enum is selected during implementation so it matches the real validation failures without becoming a free-form error channel.
2. **Capability payload shape.** The smallest useful handshake field is chosen when vectorized WPBC has its first capability-negotiation consumer: single `wpbc_version`, opcode bitset, Arrow type bitset, or a compact struct.
3. **Approximate pushdown policy.** `WendelonApprox` exists for future vector and sketch work; its planner and user-visible semantics need a separate ADR before it affects execution.

## References
- ADR 005: Decoupling Compute via Pushdown Bytecode (WPBC)
- ADR 034: Toron-Wendelon Integration Contract — this ADR refines 034's compute-pushdown principle.
- ADR 035: Vectorized WPBC Executor — the executor workstream this contract gates.
- `plans/schema/compute_ast.md` — WPBC V1 spec.
