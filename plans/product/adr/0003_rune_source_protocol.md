# ADR 0003: Rune source is the local relation protocol

Status: accepted 2026-08-01. This record corrects one false premise found
during review of `plans/product/impl/0003_rune_relation_first_contact.md` before
implementation began. The Rust source is the software specification: Toron
already contains a native, lazy Rune dataframe frontend with a broad relation
algebra and an established sandbox. Recreating that algebra in Polariton only
to print SQL would be a second dialect, not a useful stage-zero adapter.

This record supersedes PRODUCT-0002 section 6 only where it selects a
disposable SQL-text adapter as the first local-Toron lowering. PRODUCT-0002's
neutral, versioned relation expression remains the federation destination for
external sources and execution peers. Its lifetime, provenance, workspace,
capability, dependency, and promotion decisions remain in force.

## Context

`nilket/crates/toron/src/rune_engine.rs` already implements `RuneDataFrame`,
`ToronContext::table`, typed expressions, projection, filtering, aggregation,
sorting, limits, joins, windows, and other lazy operations. `RuneEngine`
compiles sandboxed Rune source and returns Toron's private `LogicalPlan`.
Thousands of Toron tests exercise this frontend.

The public `toron-client` cannot invoke it. It sends SQL statements and
collects Arrow IPC result batches into a bounded `QueryResult`; the current
client ceiling is 128 MiB. It does not expose a streaming result API. Toron's
daemon currently collapses Rune compile, runtime, and sandbox failures into a
generic SQL planning error.

The abandoned first-contact plan proposed a product-side relation tree,
SQL printer, and cross-language source map. That machinery would be larger
than the public protocol needed to reach the real frontend, would duplicate
relation semantics, and would make the language Toron already understands an
implementation detail of a second language.

## Decision

### 1. Local Toron accepts Rune source through its public client boundary

The query transport gains an `ExecuteRune` request. It carries Rune source to
Toron, where the existing `RuneEngine` compiles it to a private `LogicalPlan`.
The plan then enters Toron's existing planning and execution pipeline. Query
identity, cancellation, result batches, completion, and transport limits are
reused.

`ExecuteRune` uses the same authenticated transport and authorization boundary
as SQL execution; it creates no weaker or parallel ingress path.

No `LogicalPlan`, Rune VM value, or other server-internal representation
crosses the process boundary. The existing transport's historical SQL names
do not require a broad rename in this change; a naming migration needs its own
evidence and plan.

The first public implementation remains bounded collection. Rune execution
inherits the client's 128 MiB result ceiling and must report that bound
honestly. This record does not call the result path streaming and does not
authorize a new streaming protocol.

### 2. The public Rune contract has three independent versions

An `ExecuteRune` request carries three explicit compatibility coordinates:

1. a **Rune request-protocol version** for the request and structured response
   contract inside the existing query frame;
2. a **Rune language-surface version** for the modules, dataframe verbs,
   entrypoint shape, and values available to programs; and
3. a **Rune sandbox-contract version** for capability policy, budgets,
   violation categories, and their public interpretation.

Each unsupported coordinate receives a typed skew refusal naming the axis,
requested version, and supported versions. The outer query frame retains its
existing compatibility rules; the Rune request version is independently
decodeable so Rune compatibility does not depend on inferring meaning from a
transport failure.

The server selects and enforces sandbox budgets and capabilities. A client may
name the contract it understands; it may not use version negotiation to
loosen the server's policy.

Existing postcard enum discriminants are append-only. New request, response,
and error variants are appended, and guards prove that historical encodings
remain stable.

### 3. Rune failures cross the wire as structured Rune failures

The public protocol distinguishes compile, runtime, sandbox, version-skew,
and execution failures. Rune sandbox violations are not flattened into
`PlanningError` or prose. A stable public wire type carries the violation kind
and its relevant bounded fields; Toron's internal `RuneSandboxViolation` maps
exhaustively to it but is not serialized directly as a server-internal enum.

Compile and runtime diagnostics carry source spans when Rune provides them.
Messages remain useful when no span is available, but consumers must not
string-match error prose to reconstruct categories or locations.

This converts Toron's Rune surface from an internal convenience into a public
compatibility promise. Changes to registered modules, verb behavior,
entrypoint requirements, or sandbox interpretation advance the appropriate
version or remain compatible under its stated rules.

### 4. Polariton owns only the terminal view envelope

Polariton may structurally recognize a terminal table or plot call, retain
the view intent, and replace that terminal call with its relation argument so
the resulting Rune program returns a dataframe to Toron. This envelope layer
owns presentation intent such as geometry and field roles. It implements no
relation verbs, type inference, planning, optimization, or SQL lowering.

Because the wrapper and user program are the same language, the envelope
records an arithmetic source-offset map. Server compile and runtime spans map
back to the original Rune buffer; wrapper-only failures attach honestly to
the terminal view. Unsupported relation operations are now typed server-side
compile or sandbox refusals rather than pre-network client-tree refusals. That
is a deliberate trade: one authoritative frontend replaces earlier local
failure, while diagnostics remain Rune-native by mechanism.

The envelope is the smallest disposable component. It is isolated in one
module, and an architecture guard with a synthetic positive control prevents
relation interpretation from growing outside Toron or entering through the
view layer by degrees.

### 5. The neutral expression still owns federation's future

Rune source over the wire is the native local-Toron path. It is not the
forever interchange for Postgres, BigQuery, SQLite, Parquet, or other peers.
PRODUCT-0002's neutral versioned relation expression remains the destination
for capability negotiation, safe pushdown, provenance birth certificates,
and backend-specific lowering.

That future expression must not expose Toron's private planner and must not
force other systems to embed Toron's Rune runtime. This record merely removes
SQL as an unnecessary intermediate between the product language and the
local engine that already implements it.

## Rejected

- **A product-side relation algebra that prints SQL:** duplicates the native
  frontend and creates a second dialect whose disposable implementation is
  larger than the protocol it avoids.
- **Serializing `LogicalPlan`:** turns private optimizer and execution details
  into a cross-process compatibility surface.
- **SQL source as Rune's local semantic IR:** preserves SQL's limitations
  beneath the product language and weakens Rune-native diagnostics.
- **Rune source as the universal federation IR:** couples every source and
  execution peer to Toron's language runtime and postpones the neutral
  expression rather than replacing its need.
- **Generic planning errors for Rune failures:** loses sandbox authority,
  diagnostic locations, and compatibility-skew information.
- **Client-selected sandbox policy:** lets a presentation client negotiate
  away a server security boundary.
- **Claiming the current result path streams:** hides the client's 128 MiB
  bounded collection behavior and designs first contact against fiction.

## Consequences and sequencing

- No production implementation proceeds from the uncorrected first-contact
  plan. That plan is revised only after this record is accepted.
- The revised first contact reuses Toron's existing Rune algebra, adds the
  versioned public request and typed errors, and confines Polariton to the
  terminal envelope and view execution.
- Its acceptance corpus describes a **bounded table view**, not a streamed
  one, and cites the inherited 128 MiB ceiling.
- The workspace noun, generation-stamped newest-wins view refresh, plot-sink
  discipline, inherited plot bounds, hard lifetime exclusions, and diary
  measurements from the original first-contact plan survive the re-plumbing.
- The neutral relation expression, streaming results, durable definitions,
  materialization, pinning, PlotSpec, and the workspace artifact store remain
  separate future decisions.
- Audit and telemetry identify Rune as the frontend; Rune work is never
  recorded as SQL merely because it shares the existing query transport.
- Implementation guards include append-only wire fixtures, typed skew and
  sandbox positive controls, exhaustive internal-to-public error mapping,
  source-offset fixtures, and a synthetic envelope-containment violation.
