# Rune data products

Agreed 2026-07-23 (Joel, Sol, Claude). Extends Rune plotting from notebook
cells to plain buffers and, eventually, to live database relations. Companion
to [notebook-north-star.md](notebook-north-star.md), which remains the
established execution model; this plan owns everything that happens after a
plot names data it does not yet hold.

Implementation status (2026-07-23): steps 1–6 are complete. Deferred
relations, adapter planning, cost gates, cancellation, and provenance display
remain in steps 7–8.

## The target

> A plain SQL buffer beside a plain Rune buffer. The SQL run publishes a named
> result; the Rune buffer plots it into a native plot pane. Later, the same
> Rune API plots a live relation — even an enormous one — because the host
> plans the query, not the script.

Two scope pins, deliberately:

- **Names are explicit and unambiguous.** `workspace.result("joel")` reads a
  published materialized result; `db.table("joel")` names a live relation.
  There is no shared namespace and no fallback between them. Bare-string sugar
  (`viz.plot("joel")`) waits until one namespace clearly wins in practice,
  and may never arrive.
- **The Rune VM never touches money or bulk.** Scripts emit declarative
  requests; all database work — planning, dry-run estimates, fetching,
  cancellation — happens in the host after the VM returns. A deferred
  relation and a render-ready frame are different Rust types, so treating an
  unresolved 20 TiB relation as an in-memory frame is a compile error in host
  code, not a runtime discipline.

## Vocabulary

The word "effect" is retired entirely; `PlotEffect` splits into staged types
and nothing new reuses the old name:

```
RuneExecution
  ├── display: Option<String>      // trailing-expression text for Results
  └── requests: Vec<PlotRequest>   // declarative, possibly unresolved

PlotRequest → PlotPlanner → ResolvedPlot → PlotKernel
```

`PlotRequest.source` is an enum: `Materialized` (owned `Arc<DataFrame>`
snapshot) or `Relation` (connection identity + qualified name + projection +
policy). Only `Materialized` exists until step 7.

## Execution surfaces

Both Rune surfaces share one lower-level executor:

```
execute(source, capabilities) -> Result<RuneExecution, String>
```

The wrappers differ only in capabilities and presentation:

- **Plain buffer** — session workspace of published results, active database
  capability, trailing-expression display. The workspace is an immutable
  dispatch-time snapshot: the `HashMap<String, Arc<DataFrame>>` is cloned
  *before* `spawn_blocking`. Rune never receives editor state, a registry
  handle, or a database connection.
- **Notebook cell** — named notebook snapshots (unchanged), the same database
  capability if desired, notebook output target, normally unit return.

### Output routing

The two channels are independent; `.show()?` never depends on the function's
return value. (This preserves a current invariant: the queue is already a
side channel separate from the VM return.)

- The trailing expression continues to appear in Results.
- Every `.show()?` emits a `PlotRequest`.
- From a plain buffer, the first request opens or replaces a standard plot
  pane — the same reuse-identity-with-fresh-revision convention `begin_query`
  uses so panes replace instead of multiplying. More than one request is
  initially rejected with a precise message; separate panes may come later.
- From a notebook cell, the fence's `output=inline|pane|none` remains
  authoritative.

So this is unambiguous: the plot goes to a plot surface, `"plotted joel"`
goes to Results.

## Named results: `-- @result joel`

A plain SQL buffer publishes its result under an explicit annotation:

```sql
-- @result joel
select 1 as x, 2 as y
union all
select 3, 4
```

Pinned semantics:

- The name binds **exactly the tabular result displayed by that buffer
  execution** — the final result set under the adapter's multi-statement
  semantics. It does not annotate individual statements.
- Starting a new execution that publishes `joel` immediately retires the
  previous binding, matching notebook-cell stale-data prevention.
- Collisions are **latest-dispatch-wins**, not latest-completion-wins. A
  revision is assigned when execution starts; an older slow query cannot
  overwrite a newer publication merely because it finishes later. This is
  deliberately stronger than last-writer-wins because it stays deterministic
  under concurrent execution.
- Failure, or completion without a tabular final result, leaves the name
  unbound rather than restoring stale data.
- Provenance records the source buffer identity, execution revision,
  connection identity, completion time, row/column count, and whether the
  data is materialized or planned. Plot surfaces display it.

## Live relations and the 20 TiB table

`db.table("project.dataset.events")` records a deferred relation. Deferred
sources require explicit projection — `.scatter("x", "y")` — because column
inference today works only after the factory owns a frame, and projecting two
columns out of a wide table is most of the cost win.

Every relation-backed request carries an execution policy, **default
`Auto`** — omitting a policy must never mean "unbounded", and safety belongs
to the host planner, not to the user typing a token. `Auto` selects among:

- Direct fetch when the source is already small.
- Backend sampling (`TABLESAMPLE` where supported) when statistically useful.
- Pixel-aware 2D aggregation for large relations.
- Refusal with an estimated-cost message when no safe plan exists.

Explicit overrides: `.sample(100_000)`, `.density(800, 500)`,
`.limit(50_000)`.

Large-data scatter resolves to an honest **density representation** —
`x_bin`, `y_bin`, `count` — never aggregated pixels pretending to be sampled
points. The UI may keep scatter styling while reporting truthfully:

> Density plot: 184,231 occupied bins from an estimated 2.1B rows; BigQuery
> processed 640 GiB.

Where the backend supports dry runs, estimated bytes/cost are checked before
launching the query, and long queries are cancellable. These are launch gates
for live-table plotting, not niceties.

The deferral also dissolves an async mismatch: adapters are async while Rune
runs inside `spawn_blocking`. Since the VM only records a name, all awaiting
happens after the VM returns, in host territory that already knows how.

## Where this lives

Polariton, not libaryon. `host_event.rs` codifies that libaryon events cannot
carry a DataFrame, texture, or connection; adapters, execution results, and
plot kernels are all Polariton today. If Baryon later needs the same
machinery, extract a separate headless crate (e.g. `ket-data-products`) for
source specifications, query policies, provenance, and adapter planning —
when earned, not now.

## Sequence

1. **Decide and document plain-buffer output routing.** Done above; this step
   exists because it is a product decision, not plumbing, and everything else
   leans on it.
2. **Introduce `RuneExecution` and the shared capability-based executor**
   without changing visible behavior.
3. **Add `PlotSource::{Materialized, Relation}`**, constructing only
   `Materialized`. Sequenced with step 2 rather than after step 4: the
   registry's consumer signature is easier to get right once the request type
   already distinguishes materialized from deferred.
4. **Session result registry and `-- @result joel`**, with the revision and
   provenance semantics pinned above.
5. **Pass `workspace` and `viz` to plain Rune buffers** and route their
   requests to a plot pane. This is the screenshot working.
6. **Explicit `.scatter("x", "y")`.**
7. **Deferred `db.table(...)`** with `Auto` policy by default.
8. **Adapter planning, dry-run gates, cancellation, provenance display, and
   density pushdown** — before live-table plotting is presented as suitable
   for arbitrary BigQuery tables.

Steps 1–5 deliver the split-pane scenario with only materialized data;
nothing in 1–6 blocks on the planner work in 7–8.

## Rejected alternatives

- **Snapshot-then-table namespace fallback** for `viz.plot("joel")`.
  Rerunning or renaming a SQL result would silently change which dataset a
  plot reads without any change to the Rune source. Two explicit constructors
  cost one word and eliminate the ambiguity class.
- **Inferring result names from `CREATE TABLE joel`.** DDL names database
  objects; result names identify returned data. Conflating them makes
  multi-statement SQL and non-returning DDL surprisingly complicated.
- **Mandatory `.auto()` call.** Always-the-same-token ceremony gets
  cargo-culted, and forgetting it becomes an error case to design anyway.
  `Auto` as the stored default keeps the safety property in the planner.
- **Latest-completion-wins collisions.** Nondeterministic under concurrent
  execution; a slow stale query could clobber a fresh publication.
- **Putting resolution in libaryon now.** Violates the documented host-event
  boundary; extraction into a shared headless crate waits until Baryon
  actually needs it.
- **Extending `notebook-north-star.md`.** Its notebook milestones are largely
  shipped; this plan has a different center of gravity and links back instead.
