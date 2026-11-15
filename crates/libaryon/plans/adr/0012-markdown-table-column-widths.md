# ADR 0012: size Markdown table columns to their content in the engine

Status: proposed 2026-11-15. Subjects: `plan: adr 0012 libaryon …`, `impl: adr 0012 libaryon …` (ADR
0012 also exists in product, toron and wendelon; product ADR 0737). Follows the preview wrap fix
(a8ff28cba), which made Baryon's preview wrap tables at all. Polariton adopts the same choice in its
own ADR (`crates/polariton/plans/adr/0001`), which needs exact pixel geometry, a pinned font and a
layout check of its own.

## Context

Both heads draw the same table model, `MarkdownBlock::Table` from `svp::markdown_blocks`, and
neither sizes its columns by what they hold.

- Baryon gives each column its longest cell capped at half the table, spreads any surplus evenly and
  shares a deficit in proportion to demand above a four-character minimum. Short columns
  (identifiers, statuses, dates, numbers) wrap while a long prose column keeps spare room, and every
  column narrows together as the pane narrows.
- Polariton gives every cell `Length::Fill`: all columns are equal, whatever they hold.

The operator's example is a three-column table, `Atomic op | Returns | Does`, whose two short
columns read best whole while only the long `Does` column wraps. At the operator's pane width
(about 94 columns) Baryon gave `Returns` 24 characters for a 25-character cell, so
`Ok(result) or Err(reason)` wrapped. The operator wants the improvement in Baryon and Polariton, so
the choice must live where both can call it.

## Decision

1. **The width choice is headless and lives in libaryon.** A new module
   `svp::markdown_table_layout` owns, in character units:
   - `table_content_widths(rows, content_chars) -> widths`: the choice for a given text space with
     no gaps, for a head that spaces columns itself (Polariton's cell padding);
   - `table_column_widths(rows, available_chars) -> (widths, gutter)`: Baryon's character-grid
     policy, which takes the gutter from the space (step 2) and calls `table_content_widths`;
   - the inline word model the choice scores with: `inline_words`, `wrap_inline_words` (lines of
     words, a word longer than a line broken on character boundaries) and `inline_line_count` (the
     same breaks, allocating nothing).
   No pixels, fonts or toolkit types cross into libaryon. The functions move from Baryon unchanged
   in behaviour; Baryon's tests of them move with them.
2. **Everything fits.** When every column's longest cell fits, each column gets it and the surplus is
   spread evenly (ties to the larger demand, then the earlier column). The gutter is 2 characters,
   1 or 0 when the pane cannot hold `columns + 2 * (columns - 1)` or `columns + (columns - 1)`.
3. **Candidates.** Otherwise, per column: the longest cell, the longest visible token (a run with no
   whitespace, counted across inline style boundaries) and the volume (all characters), collected
   in one pass. Keep the `k` shortest columns whole, for every `k` that leaves the rest one
   character each, and fit the open columns around them twice: protecting their tokens, and by
   volume alone.
4. **Fit.** A greedy fill from one character per column with a fixed priority: up to four
   characters, then (when protecting) up to the column's longest token capped at 40, then where one
   more character saves the most estimated lines, `volume / (width * (width + 1))`, never past the
   longest cell. The priority does not depend on the space, so the fit for one more character is
   the fit for this one plus a character.
5. **Score.** Per row: its wrapped height, plus one charge for the row's worst blemish, the larger of
   the extra lines of a wrapped cell in a short column (longest cell at most half the table's
   longest cell) and the breaks forced inside a cell's tokens. The lowest score wins; among equals,
   more columns whole, then tokens protected.
6. **Baryon** converts its pane to characters with its measured monospace advance, as now, and
   draws exactly the lines `wrap_inline_words` returns; it measures a row with `inline_line_count`,
   so measured and drawn heights stay equal. The same widths serve wrap on and wrap off (wrap off
   clips).
Token preservation is a scored preference, not a promise: an identifier breaks when keeping it whole
would starve a prose column into a much taller table. The blemish charge is a readability
heuristic, not a measure: one badly split cell and several cost a row the same, the short-column
class follows the table's longest cell, and the forced-break count is a proxy (a token made of
styled fragments can draw on more rows than it counts).

## Rationale

- One choice in the engine is what lets the heads share a policy; a second copy in Polariton would
  drift. Shared policy is not shared physical widths: Polariton's proportional text fits characters
  differently, which its own ADR budgets.
- Pane-width-dependent thresholds were removed because each was a cliff: an equal-share cap or a
  minimum of `min(4, content / columns)` can hand every column the same width and starve the column
  that holds most of the text (a 10-column table at 68 columns: 161 lines, against 75 at 67).
## Consequences

- `short_columns_stay_whole_and_the_long_prose_column_wraps`: the operator's table keeps
  `[22, 25, rest]` from 88 columns up while `Does` still wraps; once all content fits, the surplus is
  distributed as in step 2. At every width from 60 to 200 it is never more than one line taller
  than with the previous allocator.
- Token tests: kept whole when its neighbours do not need the room; broken when keeping it whole
  would double the table. A giant token breaks rather than starving its neighbour. Tiny panes keep
  every column one character wide; gutters shrink before columns.
- `inline_line_count_matches_the_wrapped_lines`, and the corpus probe asserts the same on every cell
  of every table at nine widths.
- `a_wider_gutter_can_cost_more_on_a_tiny_pane`: for one candidate, more content space never narrows
  a column, so the chosen score cannot rise with the content space; but the gutter grows with the
  pane (0, 1, then 2 characters) below `columns + 2 * (columns - 1)` and takes content space away.
  The score is monotone in the pane width only above that threshold.
- `table_width_corpus_probe` (ignored; run by hand with `BARYON_TABLE_CORPUS` listing files) scores
  every table in the tracked Markdown against the previous allocator, kept in the tests as its
  baseline. 1,888 tables in 849 files, none unreadable:

  | width | height | short-cell wraps | token breaks | clipped chars (wrap off) | tables taller (worst) |
  |---|---|---|---|---|---|
  | 60 | 46,788 → 39,548 | 8,569 → 7,288 | 17,593 → 14,474 | 621,620 → 595,494 | 143 (+11) |
  | 80 | 33,885 → 29,849 | 4,615 → 2,874 | 7,686 → 5,688 | 482,584 → 452,179 | 114 (+13) |
  | 100 | 27,247 → 24,629 | 2,414 → 810 | 3,776 → 2,084 | 376,700 → 346,209 | 80 (+9) |
  | 140 | 20,693 → 19,345 | 706 → 78 | 1,027 → 347 | 236,003 → 207,600 | 52 (+7) |

  Over every one-column widening step from 60 to 140 (151,040 steps), the chosen score never rose
  (the previous allocator's rose on 762); height rose on 1,220 steps (previous: 391), each buying
  back a blemish, the worst by 25 lines (previous: 27).
- Cost, release build, allocation alone: per table p50 about 10 µs, p95 about 45 µs, max about
  0.17 ms; the most expensive document about 0.37 ms (previous allocator about 0.03 ms).
  `table_width_stress_probe` (ignored) measures 2 to 5 ms for tables of 50 to 200 rows, 8 to 48
  columns and 120 to 400-character cells. Baryon allocates once in measure and once in draw per
  render. No cache is added; reusing widths between calls is the first step if needed.
- `table_content_widths` equals `table_column_widths`' widths for the same content space, by
  construction and by a test across the corpus widths.
Scope: libaryon's new module and Baryon's preview renderer. Polariton (its ADR 0001), column
alignment from the delimiter row and a width cache are out of scope.
