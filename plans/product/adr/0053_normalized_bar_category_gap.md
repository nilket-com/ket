# ADR 0053: normalized bar category gap

Status: proposed 2026-08-08. This record adds an opt-in normalized reserve
between categorical bar clusters. The default changes no pixels.

## Context

PRODUCT-0021 defines `bar_width` as occupancy of one subgroup slot. The full
category band is divided equally among resolved series, and each bar's unused
slot width is split around that bar. Consequently the visible gap between two
series in one category equals the gap between the last series of one category
and the first series of the next. A grouped bar chart can narrow every bar, but
cannot distinguish its category clusters geometrically.

The missing quantity is not generic "clustering." It is the part of a category
band reserved before subgroup slots are allocated. `group` already means the
series/color channel throughout the bar substrate, so `group_gap` would name
the opposite boundary. `category_gap` uses the established outer-domain word.

Axis chrome is an independent consumer. `CoordinateTransformer::categorical`
places labels and grid structure at the center of the original ungapped band.
Any reserve that left-aligns the smaller cluster would shift only the bars and
silently separate them from their label and gridline. The transformer's own
center is therefore the geometry oracle, not a duplicate band calculation in
the test.

## Decision

### 1. Category gap is a normalized band reserve

```rune
bar("region", "revenue", "product")
    .bar_width(0.9)
    .category_gap(0.2)
```

`CategoryGap` is a private validated finite fraction in `[0.0, 1.0)`. Zero is
allowed and is the library default; one is refused because it leaves no cluster
or subgroup slot. Negative values, values at or above one, NaN, and infinities
are unrepresentable.

The value is the fraction of each original category band reserved outside the
cluster. It is normalized rather than pixel-valued so authored intent scales
with the band under resizing and export.

### 2. The cluster stays centered on the transformer's category position

For band width `B`, category gap `g`, series count `n`, and bar width `f`:

```text
cluster = B × (1 − g)
inset   = (B − cluster) / 2
slot    = cluster / n
padding = slot × (1 − f) / 2

bar origin = category origin + inset + series index × slot + padding
bar extent = slot × f
```

The horizontal orientation mirrors the same equation along y while preserving
its existing reversed series order. Splitting the reserve equally gives half
the category reserve at each plot edge and keeps every cluster centroid equal
to `transform.categorical(category_index, …)` on its categorical axis.

The visible whitespace is consequently:

```text
within-category gap  = slot × (1 − f)
between-category gap = B × g + within-category gap
```

`category_gap` is the additional separation unique to category boundaries;
clustering is its visual consequence. With `bar_width(1)`, within-category
whitespace is zero and the new reserve is isolated exactly.

### 3. Width and category gap compose without changing either meaning

`bar_width` retains PRODUCT-0021's subgroup-slot occupancy semantics. The two
fractions apply in sequence to different allocations; neither supersedes or
renormalizes the other.

For one resolved series the distinction is intentionally redundant but defined:
`bar_width(f).category_gap(g)` produces the exact same rectangle as
`bar_width(f × (1 − g)).category_gap(0)`. For multiple series the settings are
not redundant because only category gap adds separation at the outer boundary.

### 4. Existing cascade and register rules apply

`category_gap: Override<CategoryGap>` joins `LayerSettings` and PRODUCT-0007's
five-tier, right-biased cascade. It is a member of the bar register under
PRODUCT-0046 §1, so every spellable bar encoding admits it in either modifier
order. It refuses on line and scatter, duplicates and invalid values refuse the
complete destination, and bar remains unavailable inside `layers([...])`.

This record covers the Rune two-field and three-field `bar` forms backed by
`BarPlotKernel`, in both orientations where the substrate is constructed.
Dashboard-only stacked-bar kernels do not share this register or geometry and
remain unchanged.

### 5. One rectangle still owns paint and hit

PRODUCT-0021's `bar_rectangles` remains the sole owner of visible geometry.
Painting and hover consume the same rectangles, so the category reserve is not
interactive blank space. Prepared values, domains, labels, category centers,
series identity, legend content, color assignments, and hover prose do not
change.

## Acceptance gates

1. `CategoryGap::DEFAULT` is `0.0`; finite values from zero through values less
   than one are representable, while one, larger values, negatives, NaN, and
   infinities refuse.
2. All five cascade tiers select right-biased values without multiplication,
   and layer-local values remain independent between bar sinks.
3. At `category_gap(0)`, default and authored bar widths render byte-identically
   to their existing vertical and horizontal baselines. No image or SVG oracle
   is refreshed.
4. In both orientations and with multiple series, each cluster centroid equals
   the categorical coordinate returned by `transform.categorical` for that
   category. The gate consumes the transformer answer rather than recomputing
   its band center.
5. With `bar_width(1)`, adjacent series touch and the edge-to-edge distance
   between categories is exactly `band × category_gap`. Half that reserve
   appears at both plot edges.
6. For multiple series, the measured within-category and between-category gaps
   satisfy the equations in §2. Resizing preserves their normalized ratios.
7. A one-series bar with `(f, g)` has exactly the same rectangle as `(f ×
   (1 − g), 0)`, asserted by equality rather than tolerance.
8. Paint and hit consume the same centered rectangles in both orientations; a
   cursor in category reserve misses while a cursor in visible fill hits with
   unchanged hover prose.
9. `.category_gap(number_literal)` succeeds on two- and three-field bars alone,
   in a figure, and in either order with `.bar_width(...)`. Wrong arity,
   duplicate, invalid value, cross-register use, and layered-bar use refuse at
   the existing attributed boundaries.
10. Prepared data, domains, axis positions, labels, legend content, color
    identity, placement, facet congruence, and dashboard stacked bars pass
    unchanged.

## Stop condition

> This record may add only `CategoryGap`, its `Override` slot and bar-register
> member, the centered reserve in shared bar rectangle geometry, and the named
> gates. If implementation requires moving categorical transform positions,
> changing `bar_width`, adding absolute units, altering prepared data or guides,
> widening `layers`, changing stacked bars, or changing default pixels, the
> work stops and that requirement returns as its own record.

## Rejected

- **`group_gap`:** `group` names the series/color channel, so the spelling
  suggests space between Atlas and Beacon rather than between regions.
- **A generic clustering factor:** exposes a derived visual ratio whose meaning
  changes with series count and bar width instead of the stable category
  reserve that causes it.
- **Left-align the reduced cluster:** shifts bars away from independently drawn
  categorical labels and grid structure.
- **Absolute pixels:** changes its meaning under resize, export, and DPI.
- **Make a small nonzero value the default:** the current appearance is
  accepted; a default change needs its own visual exhibit and premise ledger.
- **Apply it to stacked bars opportunistically:** widens a Rune bar-register
  record into a separate dashboard kernel with independently hidden geometry.

## Consequences

- Authors can make grouped bars read as category clusters without narrowing
  every inter-series relationship equally.
- Omission preserves today's exact geometry and pixels.
- Category labels, grid structure, and bars retain one shared positional truth
  even though chrome and marks consume it through independent code paths.
- Single-series redundancy is explicit rather than a later surprise; the
  distinction becomes observable exactly when a category contains multiple
  series.
