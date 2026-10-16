# ADR 0007: bound long lines in render projection

Status: accepted 2026-10-16, converged by the KET council (user, Claude, Codex). Implementation plan:
`plans/impl/0007-long-line-projection.md`.

## Context

A render projection takes whole leaves until it has collected enough newlines. On a line longer
than one 1 MiB cold extent, three defects compound:

- a line spanning unloaded leaves is projected from its last leaf, so a newline-free file opens on
  its final extent instead of its head;
- with that fixed, the projection walks a newline-free line to the end of the file and the renderer
  hydrates every leaf it returns (a 1 GiB sparse file produced no frame in 45 s);
- every frame publishes whole leaves: up to 1 MiB of one line for a pane 100 columns wide, hashed,
  cloned and walked by both frontends at scan-driven frame rates.

The diagnostic that found them is in `plans/impl/startup-latency.md` section 10.

## Decision

1. A line can begin at a leaf's end only after a newline inside that leaf. The seek never skips a
   leaf for line 0 of that leaf.
2. Render queries take an explicit discovery policy and cap each line at one cold extent of bytes,
   counted from the line's start, in both wrap modes. A capped line resumes at the next line only
   when the root's line metrics are exact; otherwise the projection ends at the capped line. A line
   boundary is never inferred from an extent boundary. Non-render callers keep unbounded discovery.
3. With wrap off, the published text of each line is clipped at the pane edge plus one display cell,
   at grapheme boundaries, keeping absolute byte positions. Frontends position text by each token's
   absolute start, not by contiguity.

## Rationale

The cap has to apply before hydration: a clip after the query is too late to stop the walk. One
extent is the unit the engine already reads and caches, so ordinary text is covered on the next frame
once that extent is resident. Resuming only from exact metrics keeps lines below a capped one honest
while the background scan runs. Clipping at the edge matches what a pane can show: there is no
horizontal scroll.

## Consequences

- The cap is a byte bound on discovery, not a cell bound. A display cell longer than one extent
  (unbounded combining or ZWJ sequences) is published as a bounded prefix.
- A line beginning inside an unloaded leaf is counted from that leaf's end, so discovery takes at most
  the cap plus that one leaf: two extents.
- With wrap on, a line longer than one extent is published as its first extent, and wrap layout still
  falls back to one row when the line is not resident. Correct wrapped rendering of such lines needs a
  row/window protocol; it is deferred.
- Lines below a capped line appear only once the root's line metrics are complete.
- Tests that searched an unwrapped pane for text past its edge have to widen the pane.
