# ADR 0171: a local Rune source runs in the desktop

Status: proposed 2026-09-24. Application lane. This record is written after
the fact: the behavior it describes was implemented first, and its
decisions took shape during that implementation and its review, slice by
slice, without a record. It records them as implemented, in the commit that
follows this one; the implementation and its verification are in
`plans/product/impl/0171_a_local_rune_source_runs_in_the_desktop_evidence.md`.
It adds a desktop execution mode beside ADR 0003's relation execution,
narrows one inference of ADR 0161, and extends ADRs 0166 and 0169.

ADR 0161 says Rune relations execute inside Toron, which remains true, and
concludes that a Rune showcase therefore needs a workspace. That inference
does not hold for a source whose data is not in Toron. A dashboard over a
public web service (the first one fetches Treasury yields from FRED) has no
table to query, and routing its HTTP through the database daemon would
expand the database's execution surface; this change keeps HTTP in the
desktop. Such a source wants to fetch, shape
rows and plot in one program, on the desktop that shows it.

## Decision

### 1. A source that declares `main(workspace, viz)` is a local source

When a Rune source buffer is run (the Run action or `run_source`), its
parsed text decides how it runs:

- a top-level `fn main` with two parameters makes it a **local source**,
  run in the desktop, in-process, on rnx's embedding runtime;
- a top-level `main` of any other arity is refused by name;
- a source with no top-level `main` takes the path it took before: with
  the `toron` feature, the view compiler, relation execution in Toron and
  the presentation evaluator; without it, the plain host path;
- a source that does not parse takes that same existing path, which owns
  its diagnostics; where that path is unavailable (no Toron connection),
  `run_source` refuses it with the parse error rather than a connection
  refusal.

The mode is read from the parsed items, never inferred from a failed view
compilation. Notebook cells are unaffected. ADR 0161's inference is
narrowed accordingly: a source that queries relations needs a workspace; a
local source needs neither a workspace nor a connection.

### 2. A local source is a trusted local program

A local source runs with the authority of the desktop process and its user.
Its context is rnx's whole `server-runtime` embedding context (rnx record
0056): Rune's default modules and rnx's `json`, `io`, `process`, `fs`,
`path`, `time`, `text`, `env` and `http` batteries. `process::exit` is
refused, `env::args()` is empty, and HTTP follows rnx's defaults (a 30 s
deadline and an 8 MiB body limit per request). Beside them it has
Polariton's `workspace` and `viz` and the presentation vocabulary (§4).

This is deliberate and matches what the product asked for: rnx's batteries
in Polariton, not an HTTP-only facility. It is not Toron's sandbox (ADR 0003
§2): there is no capability policy, no versioned sandbox contract, and a
local source can read and write files, start processes, read the
environment and reach any host. Each run has an instruction budget of 500
million. A rerun of the source or closing it signals cancellation and
revokes the run's pending HTTP requests; an edit to the source does not
interrupt a run, but invalidates its publication, and its late results are
discarded. Those are resource and lifetime controls. They bound interpreted work and retire the run's own operations;
they do not restrict I/O and do not guarantee that a blocking native call or
a started subprocess stops promptly.

Opening a source does not run it. In the window a local source runs only
when a person runs it or an agent sends `run_source`. `polariton drive`
executes the source because its invocation says to: in its default
`on-open` mode, or at `run_source` in `by-command` mode (ADR 0169 §2).

A narrower capability set for local sources would be a separate decision
and a separate rnx record, not a reading of this one.

### 3. rnx is a pinned dependency

Polariton depends on rnx's embedding API at a pinned git revision in ket's
`[workspace.dependencies]`: `https://github.com/nilket-com/rnx`, rev
`b08645d89146a16c9b41a6b4fddd9307aa23b04b` (rnx record 0107's
implementation, over record 0056's `server-runtime` API), with default
features off so rnx's allocator accounting installs no global allocator.
rnx changes reach ket only by moving that pin, which is a ket change with
its own validation.

### 4. One presentation vocabulary

A local source declares panes with the vocabulary a view source uses,
`frame.views([view(name, figure([...]))])`, on frames built from its rows by
`viz.frame(name, rows)` or `viz.frame_typed(name, rows, schema)`. The
presentation handlers, their validation, the registry's argument-origin
metadata and the resolver that turns a frame and a view intent into a pane
are the ones view sources use. The earlier chained settings on local plots
were removed; `viz.plot(...).show()` remains for notebooks, buffers and
compatibility.

The vocabulary is evaluated at run time, in the program's own control flow,
not before it. Static pre-evaluation assumes the Toron split: presentation
evaluated in its own VM before any schema exists, one value per statically
found slot, and no computed binding shared between the relation and
presentation programs. A local source
computes presentation from what it fetched (a subtitle naming the latest
close), builds figures in helpers and views in loops; evaluating that ahead
of the program would change its order or fetch twice.

Before compilation, presentation calls are instrumented with their original
source spans. A free constructor call is the built-in only where no binding
of its name is visible under Rune's scoping; a presentation method is taken
only when its receiver is a presentation value, and any other receiver
makes its ordinary call; syntax the walk does not recognise is refused
rather than guessed. View names are any nonempty string value, unique
within a run.

### 5. A run publishes as one batch

A run's panes are built completely, every kernel included, before any pane
changes, and take the order in which the source produced them. Panes a
successful run no longer produces are orphaned under PRODUCT-0015's
retention: kept and marked until reset, and still reported, with that
status. A failed or cancelled run publishes nothing: the previous panes keep
their plot content, though their status may become stale. A run whose
source was edited or closed after it started publishes nothing either.

### 6. Agent surface (ADRs 0166 and 0169)

- `run_source` is admitted without a Toron connection for a local source.
  It completes once every figure the run committed is exported, matched by
  panel and plot incarnation, so completion never rests on an earlier run's
  figures. Retained earlier panes may still appear in the receipt, each
  with its status.
- Every figure in `describe` and in receipts reports its pane status:
  `fresh`, `running`, `stale` or `orphaned`.
- `publish` leaves orphaned figures out of the package, for every kind of
  source. Stale and running figures keep their existing eligibility.
- Opening a Rune source for `drive` completes once its text is projected,
  since `run_source` reads it.
- A local view is addressable by its view name, taken from the identity its
  panel is registered under when there is no source attachment.

## This cut

§§1–6 as implemented, the Treasury exhibit
`uat/treasury_rates.rn`, and three fixes the exhibit exposed: a null value
in a line series panicked the kernel; a Date x axis was labelled as
milliseconds; `drive` could run a source before its text was projected.

## Acceptance

1. `polariton drive --execute by-command uat/treasury_rates.rn` with
   `[run_source, describe, publish]` and no Toron connection applies all
   three, with four fresh named figures from live FRED data and a package
   whose data matches an independent download of the same series.
2. Sources without a top-level `main` classify and run as before; every
   `uat/*.rn` exhibit except `treasury_rates.rn` takes the existing path.
3. Tests cover cancellation on rerun and on closing the source (the request
   is released), output discarded after an edit during the fetch and before
   the batch commits, batch replacement with orphaning and a failed batch
   keeping prior panes, `run_source` reporting only the new run's
   incarnations, presentation errors located at the original line and
   column, constructor resolution for every binding form, and source order
   kept across `show()` and `views()`.

## Limits and follow-up

- Plot settings of a local source are read-only in the pane, which says
  why. Writing literal settings back into the source is a follow-up; this
  is not parity with view sources, whose settings write back.
- Presentation calls inside macros, template strings included, are not
  recognised, and table views are refused in a local source.
- `frame.views` needs Polariton's `toron` build feature, not a connection,
  because the shared presentation modules are gated on it today.
- `drive` in `on-open` mode still waits for a local workspace before it
  runs any Rune source, a local one included; `--execute by-command` runs a
  local source without one.
