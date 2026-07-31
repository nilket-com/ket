# Toron first contact in Polariton

Scope: **experiential evidence, not a production connector.** Toron
becomes an *explicitly selected* connection in Polariton (ADR 0005's
ungated mode — promotion to default remains behind the six gates,
untouched). The pair exists to close a specific gap: four months and
thousands of test runs without one real use. Deliverable is one `impl:`
commit reviewed against this plan, followed by a findings record.

## The benchmark

Not TPC-H. Verbatim from the converged design:

> Can Joel select a real query in Polariton, run it against Toron,
> inspect the table, make a plot, cancel a mistake, and want to do it
> again?

**The user drives the first-contact session.** Codex builds the adapter;
the first real query is Joel's. Agents verify the acceptance path with
fixtures; the experiential benchmark is not ours to run.

There is also an implementation gate: **Codex must make a plot from a real
Toron query before handoff.** The automated first-contact fixture starts or
targets a real Toron SQL endpoint, executes through this adapter, crosses the
quarantined compatibility conversion, renders through an existing Polariton
plot kernel, and proves the exported image/vector is non-empty. A mock protocol
server is not sufficient for this gate.

## Acceptance path (brutally concrete, in order)

1. Connect Polariton to an **already-running Toron deployment** (whatever
   `nilket`'s own docs say a running deployment is — this pair does not
   manage Toron's lifecycle).
2. Select SQL in the editor and execute it — the founding gesture, now
   pointed at the founding ambition.
3. Stream or bound returned rows through **today's** adapter
   compatibility layer.
4. Display the result table.
5. Feed that result through **one existing plot path** with zero
   Toron-specific UI — proving the adapter boundary, not decorating it.
6. Exercise **cancellation** of a long/mistaken query, and an **honest
   error** for unsupported SQL. (Toron's own reachability record —
   ~15% SQL surface — guarantees step 6 happens organically; that is a
   feature of first contact, not a failure of it.)

## Mechanism constraints

- **Feature-gated**: a `toron` cargo feature on polariton, off by default
  in ordinary builds — connectors are optional per ADR 0005. Cargo resolves
  optional path dependencies even while their feature is disabled, so a path
  dependency into `../nilket` would violate the requirement to build without
  the sibling checkout. The dependency-boundary guards are unaffected
  (polariton is deliberately unguarded).
- **Client reuse over reinvention**: depend on Toron's existing SQL
  client library if its crate boundaries allow; speak its wire protocol
  through a thin client only if they don't. **Verified during plan review:**
  the reusable protocol types are public, but connection/result collection is
  private inside `toron::cli`; there is no client crate. This pair therefore
  uses a feature-gated thin client with a live-endpoint compatibility test and
  records the missing client boundary as a finding. No Toron source checkout is
  required to compile Polariton.
- **The temporary conversion gets a name and a death warrant**: exactly
  one module (e.g. `adapters/toron/polars_compat.rs`) converts Toron
  results to the current Polars-shaped result boundary, with a doc
  comment stating it exists to die in the Arrow-seam migration (ADR
  0005) and that Toron will be that migration's **first native citizen**.
  No conversion logic outside this module — isolation is what prevents
  the stopgap from quietly becoming the seam.
- Capability honesty from day one: the adapter reports what it supports
  through whatever the current adapter surface can express; where the
  surface cannot express a needed capability (streaming, cancellation,
  pushdown), *that inexpressibility is a finding*, not something to
  extend the closed surface for in this pair.

## The findings record (the pair's second deliverable)

`crates/libaryon/plans/impl/toron-first-contact-findings.md`, co-authored
after the session, structured as:

- startup friction (from zero to connected, honestly timed);
- protocol/client gaps (what the client library couldn't do);
- cancellation behavior (real, cosmetic, or absent);
- result-shape pain (where rows fought the compatibility layer);
- capability needs (what the Arrow seam and capability queries must
  express — **this section feeds ADR 0006 and the seam design
  directly**);
- unsupported-SQL experience (was refusal honest and legible?);
- **the headline, answered by the user alone**: did you want to do it
  again?

## Sequencing (revised, serial, per convergence)

1. This pair. 2. The findings record. 3. **ADR 0006 informed by the
findings.** 4. Theme boundary → PlotSpec/resolver + fixtures → Rune
surface (two registers: matplotlib-energy one-liners constructing the
same explicit PlotSpec as the composable grammar — ergonomics above,
grammar below, no hidden global state) → kernel migration. 5. The shared
Arrow seam, Toron migrating first.

## Non-goals

- Embedded `ToronScratch` (behind the six promotion gates, all of them).
- Arrow-seam architecture (the compat module is quarantine, not design).
- Default/scratch changes, connection-manager redesign, Wendelon
  lifecycle management, performance measurement of any kind.
- Polishing past the acceptance path — the pair ends when the six steps
  work, not when the connector is proud.

## Review checklist

- [ ] Feature-gated; builds without `../nilket` present when the feature
      is off; boundary guards untouched and green.
- [ ] All six acceptance steps demonstrated by fixture/test where
      automatable (connect/execute/bound/cancel/error) — the plot step
      with zero Toron-specific code in the plot path.
- [ ] Codex's live-endpoint acceptance produces a non-empty plot export from
      rows returned by Toron itself; endpoint/protocol provenance is recorded.
- [ ] Conversion confined to the one named compat module with its death
      warrant; nothing Toron-shaped leaks into shared result types.
- [ ] Cancellation genuinely interrupts (or its absence is recorded as a
      finding, with the UI provably staying responsive regardless).
- [ ] Findings record exists with all seven sections; capability-needs
      section explicitly addressed to ADR 0006 and the seam design.
- [ ] The user made first contact before the pair closed.
