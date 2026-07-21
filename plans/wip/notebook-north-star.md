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
3. **Execute SQL cells in the preview.** Table + existing plot path only. Output
   renders beneath its cell in the preview. Large table/plot can still detach
   into a Baryon HostPanel (the shipped seam).
4. **Rune plot cells.** Rune builds the existing `PlotComposition { layers }`;
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
