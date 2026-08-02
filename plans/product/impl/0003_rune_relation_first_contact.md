# Rune relation first contact

Revision of 2026-08-01, superseding this plan's original text (committed
9c4a7e39) after source review exposed two false premises, both mine:

1. the original specified a product-side relation expression tree and SQL
   printer, unaware that Toron already owns a native, lazy
   `RuneDataFrame → LogicalPlan` frontend (`nilket/crates/toron/src/rune_engine.rs`)
   with the full verb set and a large test corpus — the "disposable adapter"
   would have been a second Rune dialect larger than the protocol it avoided;
2. the original described results as "streamed" when `toron-client::execute`
   is bounded collection with a 128 MiB ceiling.

PRODUCT-0003 records the correction: Rune source crosses the public client
boundary; Toron's native frontend is the relation algebra; Polariton owns only
a terminal view envelope. This revision retargets the slice accordingly. The
product-facing contract of the original — the `workspace` noun, lazy relations,
terminal views, supersession, exclusions, corpus, and measurements — survives.

Scope: the smallest end-to-end thread through PRODUCT-0002's destination
posture — read-only Rune relations over the default local workspace, compiled
and planned by Toron's existing Rune frontend via PRODUCT-0003's `ExecuteRune`,
rendered by the existing table and plot surfaces. Every lifetime axis at its
default pole: session-scoped definitions, non-resident data, live bindings.
Deliverable is one `impl:` commit reviewed against this plan, corpus status
flips backed by evidence, and a measurement record. Supersedes
`plans/wip/rune-data-products.md` by reference.

The experiment's question is unchanged and narrow: **does a relation naturally
become a table and a plot here?**

## The semantic slice

```text
r = workspace.table("test2")
        .project(...)
        .filter(...)
        .aggregate(...)
        .sort(...)
        .limit(n)

table(r)
plot.bar(r, "x", "y")
plot.scatter(r, "x", "y", "category")   # category optional
```

- Relation values are Toron's native lazy Rune dataframes, composed in the
  user's buffer and evaluated server-side. Polariton implements **zero**
  relation semantics: no verbs, no type inference, no planning, no lowering.
- **`workspace` is the capability noun** (PRODUCT-0002 §1). The entrypoint
  receives it as a parameter bound server-side to the local catalog; Rune
  programs never name an engine brand. Explicit external sources are a later
  spelling.
- When the local workspace is unavailable, the view request returns a typed
  refusal that says how to start or attach (`nilket`), with zero network
  retries and no silent auto-start (availability stage 1).

## Execution at the view boundary

Each terminal view request:

1. **envelope-transforms** the buffer: structurally recognize the terminal
   `table(...)` / `plot.bar(...)` / `plot.scatter(...)` call, retain the view
   intent (geometry, field roles), replace the terminal call with its relation
   argument, and wrap the program in the entrypoint shape Toron's language
   surface requires — recording an arithmetic source-offset map;
2. **generation-stamps** the request;
3. sends the transformed source via **`ExecuteRune`** (PRODUCT-0003 §1–2:
   three version coordinates, same authenticated transport and authorization
   boundary as SQL, no parallel ingress);
4. Toron's existing sandboxed `RuneEngine` compiles to its private
   `LogicalPlan`; the existing planning, execution, cancellation, and result
   pipeline runs; results collect under the inherited **128 MiB ceiling**,
   reported honestly when exceeded;
5. **supersedes stale requests newest-first** (the established coalescing
   contract: newest per relation/view, stale results rejected by generation,
   at most one unavoidable execution in flight);
6. feeds the existing table or plot surface unchanged.

A buffer without a recognizable terminal relation view continues through
Polariton's existing local Rune execution path; this slice does not annex or
regress ordinary Rune programs. A malformed recognized view call receives a
typed product-side refusal with guidance. Only recognized terminal relation
views cross the new remote route.

## The protocol slice (per PRODUCT-0003, cited not restated)

This implementation adds to Toron and `toron-client`:

- the versioned `ExecuteRune` request and structured response inside the
  existing query frame (§1–2);
- typed compile / runtime / sandbox / version-skew / execution failure
  categories, with the public sandbox-violation wire type and its exhaustive
  mapping from the internal enum (§3);
- span-carrying diagnostics when Rune provides them; no string matching
  anywhere (§3);
- append-only postcard discriminants with historical-encoding guards (§2);
- audit and telemetry attribution of Rune executions as Rune, never as SQL
  merely for sharing the transport (§ consequences).

No `LogicalPlan` or other server-internal representation crosses the wire.
No SQL-name migration of the existing transport rides this pair.

## Diagnostics are Rune-native by mechanism

Server compile and runtime spans map through the envelope's arithmetic offset
map back to the user's buffer. Wrapper-only failures attach honestly to the
terminal view expression. Unsupported relation operations surface as typed
server-side compile or sandbox refusals — the deliberate trade PRODUCT-0003
§4 records: one authoritative frontend replaces pre-network client refusal,
while the primary error still names the user's Rune operation and location.

## The plot sink is an acceptance sink, not PlotSpec v0

Unchanged from the original: geometry `bar` | `scatter`, `x` and `y` fields,
one optional categorical grouping field, nothing else. Internal type named
and documented as disposable; dies at the plotting ADR. Inherited plot-kernel
bounds apply unchanged.

## Inherited bounds are inherited

No new bounding machinery. The Rune route may not bypass or enlarge the
existing SQL-table and plot-kernel limits, and it inherits `toron-client`'s
128 MiB bounded collection — the felt behavior of these bounds is recorded as
evidence for `PlotExecutionPolicy` and the future streaming decision, not
laundered into guarantees. This plan does not authorize a streaming protocol.

## Hard exclusions

Unchanged: read-only and volatile end to end. No `save`, `cache`,
`materialize`, `pin`, `export`; no durable `PlotSpec` or artifact store; no
mutation or DDL API; no `collect` (deferred by design); no auto-start or
lifecycle change; additionally no transport naming migration and no streaming
protocol (0003's non-authorizations).

## Corpus v0.2

`plans/product/corpus/rune_workspace_corpus.md` continues as the PRODUCT-0002
§9 instrument. This revision corrects its view wording to **bounded table
view** citing the 128 MiB ceiling; rows and statuses are otherwise unchanged.
The implementation flips only proven rows, each with cited evidence.

## Measurements — the deliverable's spine

Unchanged, with one adjustment (4):

1. which Rune spellings felt natural during real work, and which fought;
2. whether the same relation flowed naturally to both table and plot views;
3. how often a relation edit caused obsolete Toron work (supersession
   counters provide this for free);
4. whether server-side refusals — compile, sandbox, capability — surfaced in
   Rune terms at the right buffer location, and how the loss of pre-network
   refusal felt in practice;
5. whether plotting wanted a relation, a finite frame, or an aggregate
   product;
6. whether reopening a plot demanded revision semantics immediately;
7. whether `workspace.table` felt invisible enough to deserve default status.

## Tests

1. **Envelope units:** each terminal view form recognized; substitution
   produces the expected program; offset-map arithmetic verified against
   fixtures; a no-view buffer remains outside the relation envelope while a
   malformed recognized view refuses product-side with guidance.
2. **Protocol:** `ExecuteRune` round-trips; each of the three version axes
   produces a typed skew refusal naming axis and versions (positive control
   per axis); historical request encodings decode unchanged (append-only
   fixtures); a sandbox-violating program crosses the wire as the typed
   public category; a compile error carries a span that maps to the user's
   buffer; an oversized result reports the 128 MiB bound honestly.
3. **Supersession:** burst of relation edits against a held in-flight view
   executes only the newest request, proven by work log, not timing.
4. **Unavailable workspace:** typed refusal with attach guidance, zero
   network activity.
5. **End to end, fixture-fidelity rule:** against a real local deployment
   fixture (real `toron-client`, real Toron/Wendelon, never an in-memory
   stand-in), `workspace.table` through every slice verb renders the bounded
   table view and both plot geometries from the same relation.
6. **Attribution:** a Rune execution is recorded in audit/telemetry as Rune,
   not SQL.
7. **Containment guards:** an architecture test with a synthetic positive
   control proves the envelope module is the only site of view
   transformation and that no relation interpretation exists product-side;
   existing libaryon/baryon denylists cited, not duplicated.

## Non-goals

- The neutral versioned relation expression and the shared analytics crate
  (federation's future, per 0003 §5).
- Streaming results or any new result protocol.
- The plotting contract, PlotSpec, themes, or execution policy.
- Any durability verb, the artifact store, or corpus rows beyond the slice.
- External source spellings and federation.
- Transport naming migration.

## Review checklist

- [ ] Polariton contains zero relation semantics; the envelope is the only
      transformation site, containment-guarded with a positive control.
- [ ] `ExecuteRune` carries three version coordinates; skew, compile,
      runtime, and sandbox failures are typed, span-mapped, and never
      string-matched; wire encodings are append-only with guards.
- [ ] Same authenticated transport and authorization as SQL; no parallel
      ingress.
- [ ] Views are generation-stamped with newest-first supersession, proven by
      work log.
- [ ] Bounded collection inherited and reported honestly; no streaming
      claims anywhere in code, corpus, or UI copy.
- [ ] Plot sink unchanged from original discipline; corpus v0.2 wording
      corrected; only proven rows flipped, with evidence.
- [ ] The seven measurements recorded in implementation evidence and diary
      hooks.
- [ ] Full validation green from the final tree; live deployment untouched.
