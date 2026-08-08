# ADR 0048: shared-edge axis text

Status: proposed 2026-08-08. This record decides one thing: in a multi-cell
grid, axis text renders once per shared edge — tick labels in figure gutters
beside the edge cells, axis titles once per side — and every cell keeps a
congruent inner plot area. It is the record PRODUCT-0041 §7 named when it
deferred "suppressing interior labels on shared edges" as "a visible design
choice for a later record, not an implementation judgment call." It adds no
Rune surface and no authored control.

## Context

The exhibits accumulated before the record, as usual. Every faceted figure
shipped since PRODUCT-0042 prints its y-axis title once per row and its
x-axis title once per column — `revenue` four times down the left of the
`test16.rn` window, `region` four times along its bottom — plus four copies
of every tick label column between them. At the facets meeting this was
named directly: interior-chrome suppression "is the specific thing that
makes seaborn's pairplot look clean."

The principle is already in the records. PRODUCT-0044 §1: chrome earns its
place by informing. PRODUCT-0044 §4, on the same figure's legends: "N copies
assert nothing that one does not, in N times the chrome." Under
PRODUCT-0041 §3's accepted clause every cell shares both domains, so every
interior label is a character-for-character repetition of the edge label —
the axis version of four "Value" boxes.

Two constraints bound the design, and both come from 0041:

- **Comparability is the grid's reason to exist** (0041 §6). Any layout that
  reclaims label space must not give cells different inner sizes — the same
  shared domain over different pixel extents is a per-cell scale change, a
  silent lie worse than the repetition being removed.
- **Sharing is what makes the text redundant.** The suppression rule must be
  keyed to the thing that makes labels identical — the domain group — so that
  when a later record relaxes §3, differing-domain cells get their text back
  by rule rather than by remembering.

## Decision

### 1. Axis text renders once per shared edge

> A cell renders y-axis tick labels iff no cell occupies the position to its
> left with the same y domain group. A cell renders x-axis tick labels iff
> no cell occupies the position below it with the same x domain group.

Under the accepted one-group clause this resolves to: the leftmost column
carries y text, and each column's bottom-most cell carries x text — which
handles the partial last row correctly by construction (in a five-cell 3×2
grid, the top-right cell has no cell below it and keeps its x labels).

The rule consumes `DomainGroupId` — its second consumer, after domain
resolution itself. When free scales or pair grids someday relax §3, a
neighbor with a different group no longer shadows the text, and every cell
with its own domain labels itself, with no new decision required. Stated
now, gated on the constructible case.

### 2. Text moves to gutters, and every cell's inner area is congruent

Suppressed text does not leave dead margins behind. The grid computes its
label space **once**: a left gutter sized for y tick labels and title, a
bottom gutter for x, with the cells tiled in the remaining area. Edge cells'
tick labels render in the gutters, aligned to their cells.

The existing per-cell top reservation remains because it belongs to the
non-deduplicable panel label, and the right breathing room remains part of
each cell's frame. Only the repeated left and bottom reservations are
reclaimed. Both retained reservations are identical in every slot, so they
preserve congruence rather than weakening it; this is also why the expected
top-edge stop condition below does not fire in this cut.

One placement follows from occupancy rather than from the gutters: a cell
whose below-slot is empty — the columns that outlast a partial final row —
draws its x text **into that empty slot**, directly beneath itself, not a
full row-height away at the figure's bottom edge. The slot is unoccupied, so
the text costs nothing, and congruence is untouched.

**The load-bearing invariant: every cell's inner plot area is congruent** —
identical width and height — so the shared domain maps to identical pixel
extents in every cell and the tick lattice lands on identical per-cell
positions. Reclaiming space by giving interior cells larger plots than edge
cells is the one tempting implementation this record exists to forbid.

A one-cell grid has only edge cells and one gutter pair, and must reduce
exactly to today's single-plot layout — the ungridded parity oracle
(PRODUCT-0042 gate 10's baseline) is unaffected byte-for-byte.

### 3. Structure stays per cell; only text dedups

Axis lines, tick marks, major and minor gridlines, and panel labels remain
per cell, unchanged. A frame around each panel is structure that guides the
eye across the grid; text is information, and only information that repeats
is removed. Chrome roles are consumed exactly as PRODUCT-0031 defines them,
wherever the text now draws.

### 4. Axis titles render once per side

The y title renders once, centered on the tiled extent's height; the x title
once, centered on its width — where a single plot already puts them. A title
per edge cell would dedup N² to N and stop; the title names the shared
axis, and there is one.

### 5. The change is the default, deliberate and named

This visibly changes every existing multi-cell figure. Preserving the
repetition as the default would keep chrome the records' own principle calls
uninformative, so the change ships as the default with no authored toggle —
the PRODUCT-0021 posture: when the honest presentation and the legacy pixels
disagree, the pixels change and the record says so loudly. A later record
may add an authored control if a real buffer wants the repetition back; none
is anticipated, and the set grows when a use exists (0011 §1).

One-cell grids and ungridded figures are pixel-identical to today.

### 6. What this record does not authorize

No Rune surface, member, or modifier. No free-scales or pair-grid semantics
— the group-keyed rule anticipates them; it does not admit them. No
noncongruent cell layouts. No legend, panel-label, placement, or identity
change. No change to any single-plot rendering, tick generation, chrome
role, or kernel.

## Acceptance gates

1. A one-cell grid and every ungridded figure render **byte-identically** to
   today, against the existing banked baseline.
2. In a 2×2 facet grid, exactly the left column renders y tick labels and
   exactly the bottom row renders x tick labels; interior cell text is
   absent from the SVG, not merely occluded.
3. A five-cell 3×2 grid renders x tick labels on the two bottom-row cells
   **and** the third-column top cell — the partial-row rule, by occupancy —
   and the third column's labels sit in the empty slot directly beneath
   their cell, asserted by vertical proximity, not merely presence.
4. Every cell's inner plot area is congruent: identical width and height
   across all cells, asserted numerically, and the rendered per-cell tick
   positions are identical (the PRODUCT-0042 gate-4 assertion, surviving the
   new layout).
5. The y title renders exactly once, centered on the tiled height; the x
   title exactly once, centered on the tiled width.
6. Axis lines, tick marks, gridlines, and panel labels render per cell,
   unchanged in count and color roles.
7. The tiled area strictly grows: cells in the 2×2 fixture have larger inner
   areas than under the per-cell-margin layout they replace, with the
   freed space coming only from the deduplicated gutters.
8. Suppression is behavioral over the group comparison: the predicate,
   called directly with two synthetic cells differing in y group, grants
   **both** their y text — the PRODUCT-0039 synthetic-palette move, reaching
   a branch 0041 §4 makes unconstructible from a whole grid. A column-zero
   special case cannot pass this gate.
9. No hover, legend, placement, identity, or lifecycle behavior changes;
   `test15.rn` and `test16.rn` render with single-edge axis text and are the
   visual evidence.

## Stop condition

> This record may add the gutter layout, the group-keyed suppression rule,
> single-side titles, and the congruence assertions. If the implementation
> cannot keep every cell's inner area congruent without a per-cell layout
> vocabulary, requires a Rune surface, a free-scales decision, a change to
> tick generation or any chrome role, or touches legends, panel labels,
> placement, or identity, the work stops and the requirement returns as its
> own record.

The expected trigger is the top edge: panel labels occupy per-cell top space
that is not deduplicable (every label differs), and if reconciling label
space with the gutter model forces unequal cell heights, that is the halt —
congruence outranks tightness.

## Rejected

- **Suppressing text while keeping its reserved space:** removes the noise
  but not the cost; the exhibits complain about both, and dead margins in
  every interior cell would make the change look like a bug rather than a
  cleanup.
- **Reclaiming space per cell (noncongruent inner areas):** the worst
  outcome available — a silent per-cell pixel-scale change under one shared
  domain, breaking the comparability the grid exists to provide.
- **Suppressing tick marks with the labels:** marks are structure; a
  frame-and-tick skeleton on every panel is what lets the eye carry the
  edge labels across the row.
- **A title per edge cell:** dedups N² to N and stops; one shared axis has
  one name.
- **An authored toggle in this cut:** no buffer has asked for the
  repetition; the set grows when a use exists.
- **Keying suppression to grid position instead of domain group:** works
  today, silently wrong the day §3 relaxes — the difference between a rule
  and a coincidence, which is 0037 §4's lesson one more time.

## Consequences and sequencing

- Faceted figures stop spending a third of their area saying the same words
  sixteen times; the pairplot-clean look the facets meeting named arrives as
  a default rather than an option.
- `DomainGroupId` acquires its second consumer, and the free-scales future
  inherits correct labeling automatically: text returns exactly where
  domains diverge.
- **This record makes the legend overlay worse, and that is named here in
  advance**: gate 7 grows the tiled area while PRODUCT-0044 §4 still places
  the legend at figure bounds, so more cell content lands beneath it. The
  first post-0048 screenshot showing that is the deferred placement record's
  docket growing — its fourth exhibit — not a regression of this record.
- The legend placement/visibility record (next in the agreed sequence, with
  the `Presence::Hide` repair riding along) inherits tighter grids and a
  four-exhibit overlay docket.
- The gutter model gives any future outer-edge chrome — a shared color bar,
  a figure-level size legend — a place to land that already exists.
- This is the first substrate cut in the arc that does not pay to bank a new
  parity oracle. Gate 1 consumes `grid.rs`'s existing
  `bank_unfaceted_one_cell_parity_oracle`, landed by PRODUCT-0042 before that
  record routed ordinary figures through the grid. The discipline compounds:
  0048 changes every multi-cell layout while inheriting a safety net built
  two cuts earlier.
