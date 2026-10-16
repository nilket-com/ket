# ADR 0007 implementation findings: long-line projection

Companion to `0007-long-line-projection.md`. Records where the `impl: adr 0007` commit departs from the
plan and what it measured.

## Delivered

- `uast/projection.rs`:
  - the phase 1 seek guard (`target_line_in_node > 0`);
  - crate-private `LineDiscovery` and `UastRegistry::query_viewport_with_line_discovery`;
  - the per-line discovery cap in phase 2, with resumption gated on exact root metrics;
  - `clip_frontend_text_tokens` and `CLIP_LOOKAROUND_COLUMNS`.

  The `UastProjection::query_viewport` trait method and its non-render callers are unchanged.
- `engine/render.rs`: both render queries use `PerLineBytes(COLD_INGEST_EXTENT_SIZE)`, and the visible
  tokens are clipped after coalescing when wrap is off.
- `baryon/src/ui/draw.rs`: the pane byte cursor starts at each text token's `absolute_start_byte`.
- Tests:
  - seek, discovery (unbounded, capped with exact metrics, capped with incomplete metrics) and clip unit
    tests;
  - `tests/long_line_projection_test.rs`, which covers a 1.5 MiB newline-free line, a long line between
    short ones, and a 1 GiB sparse newline-free file in both wrap modes. With discovery forced
    unbounded, the 1 GiB case times out after 30 s, so the test guards the cap.

## Departures from the plan

1. **An unloaded leaf with newlines counts its line from the leaf's end, not its start.** The plan
   counted from the start, which over-counts. Because the cap equals the extent size, that false-capped
   every ordinary multi-line view at the first boundary past an unloaded leaf. Counting from the end
   bounds discovery at the cap plus the one leaf where the line began, at most two extents. The cap
   stays a hard bound, though not exactly one extent.
2. **The clip tracks newline counts incrementally.** The first version counted the newlines before
   each kept range from the token's start. On a hydrated 1 MiB CSV leaf of about 9,500 rows, that
   quadratic scan made each frame slower than the 2 s refresh timeout. The small unit fixtures did not
   show this; `deep_short_row_csv_keeps_visible_coloring_after_context_gap` caught it.
3. **One existing test widens its screen.** `generation_ping_pong_parks_roots_after_the_first_ingest`
   searched an unwrapped inspector pane's token text for a 130-column header line. Panes now publish
   only to their edge, so the test sends `Resize(400, 60)` before opening the inspector. The header's
   content is unchanged.

## Findings

1. **Every earlier sparse measurement showed the file's last extent.** That includes the 1 MiB token
   in `startup-latency.md` section 10. The seek bug (plan 1.1) skipped every unloaded leaf of line 0.
   The probe's "first text" was the final extent, not the `HEAD` marker.
2. **The bound applies with wrap off only.** With wrap on, a line longer than the cap is still
   published as its first extent (1 MiB per frame). Wrap layout still falls back to one row when the
   line is not resident.
3. **Clipping happens after the query.** It bounds the published `Viewport`, queued memory, hashing
   and frontend work. Render still copies the discovered leaf, at most one extent per line, before
   cutting it.
4. **Frame rate during the scan depends on the mode:** about 82 frames/s with wrap off and about 24/s
   with wrap on (64 GiB, 15 s hold). Process CPU is about 15 s in every mode, dominated by the
   background scan, so per-frame render cost could not be separated this way. The follow-up on frames
   for an unchanged projection should measure render time inside the engine.

## Measurements (development machine, release, 64 GiB newline-free sparse file, 100x30, 15 s hold)

| Mode | Probe | Frames | Text per frame | Peak RSS |
| --- | --- | --- | --- | --- |
| wrap off | not draining | 1,230 queued | 83 B | 64 MB |
| wrap off | draining | 1,225 | 83 B | 58 MB |
| wrap on | not draining | 357 queued | 1 MiB | 425 MB |
| wrap on | draining | 367 | 1 MiB | 62 MB |

Before this change, the non-draining wrap-on run (the default) reached about 954 MB anonymous memory
in 15 s. Both earlier wrap modes published the last extent. 83 B is an 82-column text area plus the
lookaround cell.

## Pre-existing, not touched

`architecture_test::product_0059_store_stays_capability_closed_and_later_consumers_are_explicit` fails
at HEAD: `polariton/src/mcp/mod.rs` (from `impl: adr 0173`) holds discovery the test expects in
`main.rs`.
