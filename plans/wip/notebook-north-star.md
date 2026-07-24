# Notebook north star

Agreed 2026-07-20 (Joel, Codex, Claude). The multi-cycle target that gives the
Baryon workspace a document-centered reason to exist. Companion to the shipped
host-pane seam; supersedes the deleted `notebook-with-panes.md` planning doc for
forward direction (its decisions live in git history and commit messages).

## The target

> One Markdown source pane beside one native Polariton preview pane.
> The preview renders prose, highlighted SQL, that SQL's result table, and a plot.

Two scope pins, deliberately:

- **Preview *pair*, not a single editable inline surface.** Source pane beside
  preview pane reuses the `MarkdownPreviewSnapshot` precedent and needs zero
  changes to Baryon's text model. Inline outputs between source lines — row-
  quantized foreign blocks reserving space in the editor's cell grid — are the
  deepest engine surgery and stay out of scope until earned.
- **Existing plot path, not a new plot model.** SQL works, the table widget
  works, and `PlotComposition { layers }` already drives the renderer. Rune
  authors that existing composition; it does not invent a Markdown plotting
  DSL or a second rendering path.

```
Markdown source  ⇄  rich notebook preview
       SQL fence       table + plot output
```

Baryon stays the honest naked-source editor; Polariton owns the candy-coated
preview. Both are views of one authoritative source document. Baryon's
split/tab/maximize gives the "fold back the covers" interaction for free.

## Status

- **Milestone 1 — shipped.** The Markdown source/preview pair projects fenced
  SQL highlighting from libaryon; Polariton only resolves its categories through
  the projected palette.
- **Milestone 2 — shipped.** Fences have implicit session-local identities and
  optional explicit `{#name}` identities.
- **Milestone 3 — shipped for tabular output.** `Ctrl+Enter` runs the SQL
  fence at the cursor and a native table, error, or completion message can
  render beneath that fence in the preview. Cell metadata belongs to the
  Markdown source projection, so execution remains available after the
  preview is closed.
- **Milestone 4 — Rune notebook dispatch shipped.** `Ctrl+Enter` now runs a
  `rune` fence too. Explicitly named SQL results are retained as owned
  snapshots even with `output=none`; `workspace.cell("name")` resolves one and
  `viz.plot(...).scatter().line().show()?` builds the existing plot kernel off
  the UI thread. An inline Rune cell receives the resulting native plot.
- **Milestone 5 — inline plot surface shipped.** Each inline plot has local
  state keyed by its Markdown cell identity, renders native hover feedback,
  and uses a plot-sized document block whose width follows the preview. The
  existing canvas has no zoom gesture yet, so zoom remains future work rather
  than a falsely advertised property of this surface.

## Cell presentation contract

Execution and presentation are separate. Fence metadata says where an executed
cell's output appears and whether the preview shows its source; it must not
turn Markdown into a general styling language.

Fence attributes are comma-delimited for scanability. Whitespace around commas
is optional, and the existing explicit identity remains one attribute among
the rest:

````markdown
```sql {#sample, output=inline, code=show}
select x, y, z from test;
```
````

Initial bounded vocabulary:

- `output=inline` — render the cell's table/error/message immediately beneath
  the fenced block in the Markdown preview. This is the notebook default.
- `output=pane` — send output to a native Baryon workspace panel. An implicit
  cell reuses the transient `Results` panel; an explicit identity can later
  own a stable result panel.
- `output=none` — execute but show only command/status feedback; useful for
  schema and setup cells.
- `code=show|hide` — controls only the rich preview. The Markdown source pane
  always retains editable SQL.

Code framing is a renderer/theme default, not a required annotation on every
cell. A future Rune configuration setting should choose the default (for
example `markdown.code_frame = subtle`); a bounded `frame=none|subtle|card`
attribute may override it for an exceptional cell. Result sizing is a separate
future concern, not an effect of code framing.

`Shift+Enter` remains the ad-hoc selection/current-line runner and uses the
transient Results pane. `Ctrl+Enter` is the notebook-cell action and obeys the
cell presentation contract. This keeps shortcut semantics separate from a
document's declared layout.

### Next cut

Extend the plot surface only when there is a concrete gesture to support. The
current canvas exposes hover and double-click messages; preview hover is now
stateful, while a real zoom/pan model has not been designed or implemented.
Keep `output=pane` and inline plots on the same kernel/composition path as new
interaction capabilities arrive.

## Sequence

1. **Outputless pair.** Markdown source + preview with highlighted SQL fences.
   Requires the injection slice and preview-span projection; no execution, no
   identity. **This is the cheapest thing that could invalidate the direction**
   — it answers "does the source/preview pair feel right" in days, before any
   execution/identity investment. First milestone.
2. **Cell identity model.** Structured fence parsing from day one so
   `sql {#trips}` never breaks the parser. Two tiers:
   - *Implicit* — a fence's occurrence/range keys its output within a session.
     Free, no annotation; all step 3 needs.
   - *Explicit* — `{#trips}` required only when a cell is named
     (`workspace.cell("trips")`) or its association must survive edits above it.
     Earned by step 4, not required before.
3. **Execute SQL cells in the preview.** Shipped for table output; the cell
   presentation contract above is the remaining routing refinement. Large
   table/plot can still detach into a Baryon HostPanel (the shipped seam).
4. **Inline Rune plot cells — three earned cuts.** Markdown names cells and
   declares placement; Rune builds the existing `PlotComposition { layers }`.
   Do not invent a Markdown plotting DSL.

   1. *Rune capability cut — shipped.* Embed a Polariton-owned Rune runtime with a
      deliberately small deferred capability: `workspace.cell("trips")`
      reads an explicitly named SQL-cell result and `viz` builds/publishes a
      `PlotComposition`. It returns an owned data snapshot / queued effect;
      it never borrows `AppState` across the VM boundary. Unit-test a layered
      scatter-plus-line composition without Markdown or Iced.
   2. *Notebook dispatch cut — shipped.* Generalize `Ctrl+Enter` from `sql` to `sql` and
      `rune` fences. A Rune cell's own `output=inline|pane|none` controls the
      plot it publishes. Its source may look like:

      ````markdown
      ```sql {#trips, output=none}
      select x, y from trips_source;
      ```

      ```rune {#trips-plot, output=inline, code=hide}
      viz.plot(workspace.cell("trips")).scatter().line().show();
      ```
      ````

      The existing `NotebookCellOutput` gains a native plot variant and the
      preview renders the existing plot kernel beneath the Rune fence.
   3. *Plot-surface cut — shipped for hover and sizing.* Inline plots retain
      per-cell hover state and use responsive width with a plot-sized document
      block. Future zoom/pan must begin with a real canvas gesture, not an
      empty promise; `output=pane` already publishes through the dashboard
      host panel. Dialog edits come only after this surface works, and use
      managed Rune source edits rather than bidirectional general-program
      synchronization.
5. **Direct manipulation as managed source edits.** The dialog edits *recognized*
   Rune calls via the `:init` writer's machinery (`update_init_file` /
   `config_call_key`); hand-authored or too-complex Rune stays authoritative and
   the dialog steps back to read-only for it — the same managed-calls /
   unmanaged-program boundary that already ships for `init.rn`.

## Why this is the right target

Smallest artifact that forces the whole model to exist end to end — nothing
stubbable — while keeping the hardest Baryon surgery (inline blocks) and the
largest unstarted cut (Rune runtime) off the critical path. It also drags cell
identity to the front where it belongs: three roads (output association,
preview→source editing, Rune referencing a cell) converge on it, so it is
earned rather than deferred.
