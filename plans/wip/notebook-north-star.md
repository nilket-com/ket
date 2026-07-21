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
- **Existing plot path, not Rune-authored plots, for v1.** SQL works, the table
  widget works, `PlotComposition { layers }` already drives the renderer. A
  preview that runs a SQL fence and renders table + existing-machinery plot is
  reachable now. Rune plots are a later enrichment of a working surface, not a
  prerequisite.

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
- **Milestone 3 — partly shipped.** `Ctrl+Enter` runs the SQL fence at the
  cursor and a native table, error, or completion message can render beneath
  that fence in the preview. The existing plot path has not yet been attached
  inline.

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

Project structured presentation metadata from the existing fence parser into
`MarkdownCellSnapshot`, then make `Ctrl+Enter` and the native preview honor
`output` and `code`. Preserve today's behavior as the default. Do not add
arbitrary CSS-like attributes or inline plots in this cut; the resulting
output-routing seam is what gives plots an unambiguous home next.

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
4. **Inline existing plots, then Rune plot cells.** First attach the existing
   `PlotComposition` path to the chosen output target. Then Rune builds the
   same `PlotComposition { layers }`;
   layered scatter-plus-line becomes ordered layers, not dialog state. Pulls in
   the Polariton Rune runtime (deferred-handle + backing-object registry
   patterns, per `baryon-rune-config`), and requires explicit cell ids.
5. **Direct manipulation as managed source edits.** The dialog edits *recognized*
   Rune calls via the `:initw` writer's machinery (`update_init_file` /
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
