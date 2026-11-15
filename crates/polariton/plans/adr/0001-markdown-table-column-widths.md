# ADR 0001: size Markdown table columns to their content in the preview

Status: proposed 2026-11-15. Subjects: `plan: adr 0001 polariton …`, `impl: adr 0001 polariton …`
(first ADR of this directory; product ADR 0737). Uses libaryon ADR 0012's shared allocation policy,
`table_content_widths`, unchanged.

## Context

Polariton's Markdown preview gives every table cell `Length::Fill`, so every column is equally wide
whatever it holds. In the operator's `Atomic op | Returns | Does` table the two short columns wrap
while the long `Does` column has room to spare. libaryon 0012 now chooses content-aware widths in
characters for any head; Baryon draws them on a monospace grid. Polariton draws proportional text,
so it needs its own conversion from pixels to characters and back, and evidence with real shaping.

Facts this relies on:
- Body text is 17 px. Prose uses `iced::Font::DEFAULT`, which the host resolves, so its advances are
  unknown; inline code uses the bundled DejaVu Sans Mono.
- The app already loads the bundled "Polariton Plot Sans" (a renamed Noto Sans Regular), and
  `plot/text_estimate.rs` holds conservative per-character budgets for it, censused with iced's own
  shaping and rounded up to 0.05 em (non-ASCII: 1 em).
- Each table cell has 8 px of horizontal padding on each side; the preview is a non-scrolling column
  of the visible blocks.

## Decision

1. **Tables use known faces.** Prose in table cells renders in Polariton Plot Sans; inline code stays
   DejaVu Sans Mono. Prose outside tables is unchanged. The bundled family has only a Regular face:
   bold and italic in a table are whatever iced resolves for that family, and are budgeted like
   regular text (a stated limit, checked by the real-shaping test below).
2. **A cell's estimated width** is the sum of its spans: prose by the censused class budgets, code at
   0.61 em per character (DejaVu Sans Mono's advance is 0.602 em). The estimate is an upper bound
   only where it was measured: printable ASCII in Polariton Plot Sans Regular under iced's basic
   shaping, and printable ASCII in DejaVu Sans Mono. Elsewhere it is best effort: non-ASCII text
   (the class table's 1 em allowance for host fallback glyphs, not a measured bound), bold and
   italic (which the bundled family does not contain) and any glyph a face lacks.
3. **The pane width** comes from iced's `responsive` around the table (width `Fill`, height
   `Shrink`), so the widths are recomputed whenever the preview is resized.
4. **Pixels to characters.** With `n` columns, the text space is `T = max(W - 16n, 0)` px. The
   table's advance is its total estimated width over its total characters (a table with no
   characters uses 0.55 em); the text space in characters is `T` divided by that advance, rounded
   down, and `table_content_widths` chooses `n` character widths from it. Padding is subtracted
   exactly once; there is no other gap.
5. **Characters back to pixels.** A table-wide average does not bound per-column sums (many narrow
   `iiii` rows beside one wide `WWWW` row), so the reservation is checked before it is made. Let
   the candidates be the columns the choice keeps whole (character width reaches the longest cell),
   `R` the sum of their widest cell estimates, and `m` the number of other columns:
   - **all whole and `R <= T`:** each column gets its widest estimate plus an even share of `T - R`;
   - **some whole and `R + m * 17 <= T`** (one em left for each other column): the whole columns get
     their widest estimates, the others share `T - R` in proportion to their character widths;
   - **otherwise (including all whole with `R > T`):** no column is reserved; every column gets `T`
     in proportion to its character width.
   Shares round down and the remainder goes to the widest receiving column; no share is negative.
   The columns reserved by the first two branches are the preserved columns.
   Each column's container is `Length::Fixed(text px + 16)`. Every row uses the same vector, and a
   missing cell is an empty container of its column's width, so columns stay aligned.
6. **Wrapping.** Table cells wrap by word, and break a word only when it is wider than its column
   (`Wrapping::WordOrGlyph`), as Baryon does, so a giant token breaks instead of spilling into its
   neighbour.

## Rationale

The allocation policy is shared; the physical widths are not, because proportional text fits
characters differently. Pinning both faces turns the conversion into a bound for the measured
alphabet and shaping, rather than a guess, and real-shaping tests check that bound, which
arithmetic alone cannot. Measuring with the renderer
at view time would be exact but needs the renderer in `view`; the bound is enough to keep a column
whole when the choice keeps it whole.

## Consequences

- Pure conversion tests: the text widths sum to `T` exactly; preserved columns get at least their
  widest estimate; no width is negative; one vector of `n` widths serves every row, ragged rows
  included; zero and tiny widths and an all-empty table (no characters) give finite widths; the
  adversarial `iiii` rows beside a `WWWW` row takes the fallback branch instead of overspending.
- Real-shaping tests with iced's rich-text paragraph layout (no GPU): the cell's own spans, faces,
  styles and 17 px size, the shaping and wrapping the surface uses:
  - the operator's table at a documented pane width keeps `Atomic op` and `Returns` on one line
    while `Does` wraps;
  - at every width from 300 to 1600 px in 50 px steps, every printable-ASCII regular cell of a
    preserved column shapes to one line;
  - an inline-code cell, a bold header and an unbreakable token are included; the bold header is
    reported, not asserted, being outside the measured bound.
- A layout test lays out the real table element (the `responsive` hierarchy) with a headless
  tiny-skia renderer at narrow, medium and wide limits: finite non-zero height, the same cell
  x-offsets in every row, and new widths after a resize.
- Exact per-line equality with Baryon is not promised.

Scope: Polariton's Markdown table surface. Prose outside tables, other fonts, a width cache and
column alignment from the delimiter row are out of scope.
