# ADR 0065: shared categorical grid domains

Status: proposed 2026-08-09. This record repairs PRODUCT-0041's categorical
grid implementation so cells in one accepted domain group share the complete
axis domain: the ordered categorical sequence and the numeric range paired
with it. It adds no surface and changes only faceted figures whose cells
currently derive unequal categorical layouts.

## Context

PRODUCT-0041 §2 says cells naming the same axis group resolve one domain,
PRODUCT-0041 §6 makes that sharing the basis of visual comparability, and
PRODUCT-0048 makes congruent cells the gutter model's load-bearing invariant.
The implementation proves that contract only for `PlotLayout::Cartesian`.

`shared_layout` (`plot/grid.rs:888`) unions Cartesian ranges and returns the
first layout unchanged for every other variant. `merge_layout` (`:920`) applies
only a shared Cartesian layout and otherwise returns each local layout. A
faceted `CategoricalX` or `CategoricalY` kernel therefore keeps both parts of
its local domain:

- a numeric range derived from only that partition's values;
- a category sequence derived from only the categories present in that
  partition.

This record calls such a grid **ragged**: its partitions do not contain the
same category set, the same numeric extent, or both. Raggedness describes the
data slices, not the rendered geometry—the whole point of sharing is that the
rendered cells remain congruent despite it.

The defect was reachable when PRODUCT-0041 shipped, but its fixtures could not
distinguish the two states. Every live faceted exhibit, including `test14.rn`
and `test16.rn`, authors numeric bounds, making local numeric ranges equal even
when sharing is broken. Their data also gives every partition every category.
This is the symmetric-heatmap and integer-edge MSAA trap again: the probe passes
under both the promised implementation and the defect.

PRODUCT-0064 exposed the numeric half with an unbounded stacked facet whose
local sums produce `(0, 9.9)` and `(0, 18.7)`. Repairing it inside that record
would move existing grouped-facet pixels outside PRODUCT-0064's enumerated
stacked repairs, so its stop condition correctly halted. Reading the same two
functions exposes the categorical half; repairing only numeric ranges would
leave congruence broken in the identical seam.

Stage 0's first correspondence probe exposed a second implementation premise.
Several categorical kernels address prepared payloads by their **local
ordinal** instead of resolving the prepared category name through the
transform's domain. After sharing the chrome domain, a local `[X,Z]` payload
would therefore draw `Z` in shared slot 1, under the `Y` label, rather than
leaving slot 1 empty and drawing `Z` in slot 2. Sharing only the `PlotLayout`
would turn a scale defect into a label-to-mark defect.

## Decision

### 1. A categorical grid shares the complete domain

For the accepted one-group value facet, every `CategoricalX` cell receives:

```text
categories = shared resolved x display domain
y_range    = union of every cell's local y range
```

Every `CategoricalY` cell receives the mirror:

```text
categories = shared resolved y display domain
x_range    = union of every cell's local x range
```

The merge happens once at the grid composite boundary and the resulting layout
feeds chrome, transforms, marks, hit testing, and hover together. Kernels do not
gain facet awareness.

This makes explicit what PRODUCT-0041's phrase “one shared domain” already
requires for a categorical scale. The record corrects its implementation and
supersedes the implementation evidence claiming gate 3 was fully discharged;
it does not retract PRODUCT-0041's accepted sharing rule.

### 2. Category identity comes from the resolved body, not a cell union

The ordered category sequence is read from the shared
`ResolvedPlotSpec` channel domain carried by every cell body. It is not
reconstructed by concatenating local kernel layouts.

Cell-order union is observably wrong for interleaved input. Given rows
`A:X, B:Y, A:Z`, facet order is `A, B`; concatenating local domains yields
`X, Z, Y`, while the resolved display domain is `X, Y, Z`. PRODUCT-0041 §5
already makes that display domain authoritative and re-derived after relation
sorting. Sharing must preserve it exactly, including categories absent from the
first or any other partition.

No new domain representation is introduced. `GridCell.body` already points to
the common resolved specification required by PRODUCT-0041 §3.

### 3. Numeric extent comes from the rendered cell layouts

The numeric half is the finite union of local layout ranges, using the same
minimum-lower/maximum-upper rule as Cartesian grids. This is necessary because
kernel-derived geometry can change the value domain after channel resolution:
a stacked bar's extent is a per-category sum, not the raw value channel's
domain.

Authored bounds already reach each local layout and consequently union to the
same authored pair. Such figures remain byte-identical. Unbounded facets with
unequal local extents are the intended pixel change.

### 4. Prepared categories address shared slots by identity

`CoordinateTransformer` resolves a prepared category name to its index in the
complete categorical domain. Bar, stacked-bar, box, and violin paint and hover
paths use that semantic lookup before asking for a categorical coordinate.
Scatter already follows this rule: `prepare_scatter_data_resolved` maps each
categorical value through the resolved channel domain.

This is not facet-aware kernel policy. It is the ordinary renderer invariant
that a category's identity, rather than its position in a local data slice,
selects its transform slot. On an ungridded plot the prepared and resolved
orders are equal, so the change is byte-identical. On a ragged facet it leaves
the absent category's band empty while keeping every surviving mark under its
own label. A renderer skips a prepared category absent from the transform
domain rather than substituting another slot.

### 5. Other layout families do not move

`Cartesian` retains its existing union. `CategoricalXY` carries no numeric
range and is already fully transformer-derived, so heatmaps are unchanged.
`Parallel` and `Radial` remain unchanged. One-cell grids and ordinary plots are
byte-identical.

The record does not add free scales, differing domain groups, per-cell bodies,
or a category-union policy. Those remain refused or out of scope under
PRODUCT-0041.

## Implementation sequence

1. Bank the current explicit-bounds facet exhibits and one-cell parity before
   changing layout sharing.
2. Add deliberately ragged, unbounded `CategoricalX` and `CategoricalY`
   fixtures. Assert their pre-merge local domains differ so neither gate can
   pass vacuously.
3. Source ordered categories from the common resolved body, union the numeric
   ranges, and apply both halves in `merge_layout`.
4. Make categorical renderers address transform slots by category identity;
   prove the existing scatter path already does so.
5. Run the categorical correspondence, unchanged-family, baseline, and full
   workspace gates. Record every premise update by its one allowed difference
   class.
6. Resume PRODUCT-0064 at its facet gate.

## Acceptance gates

1. **The fixtures discriminate.** Before merge, both the vertical and
   horizontal fixtures contain at least two unequal local numeric ranges and a
   partition missing at least one category. Removing the shared merge makes
   the positive assertions fail; an explicit-bounds/every-category control is
   named as non-discriminating and cannot discharge the gate.
2. **Categorical X shares both halves.** Every merged `CategoricalX` layout has
   the exact resolved x display-domain sequence and the unioned y range. An
   interleaved `A:X, B:Y, A:Z` fixture pins `X,Y,Z`, specifically rejecting
   facet-order concatenation `X,Z,Y`.
3. **Categorical Y is symmetric.** Every merged `CategoricalY` layout has the
   exact resolved y display-domain sequence and unioned x range under an
   independently ragged fixture.
4. **Labels, marks, and values correspond.** In both orientations, a unique
   category payload renders in the band carrying that category's label, at the
   same normalized position in every cell. Separate presence or order
   assertions do not discharge the joint correspondence.
5. **Congruence is numeric.** Equal category indices and equal numeric values
   map to identical cell-relative pixel positions in every cell, including a
   category absent from one partition's data. Major and minor numeric tick
   positions are identical across cells.
6. **Authored bounds remain authoritative.** Unequal local data under one
   authored numeric bound pair yields that exact pair in every cell; the
   pre-correction `test14.rn` and `test16.rn` figure oracles remain
   byte-identical.
7. **Other families do not move.** Existing Cartesian sharing remains green;
   `CategoricalXY`, `Parallel`, and `Radial` layouts reproduce their prior
   behavior; a one-cell grid remains byte-identical to its ordinary plot.
8. **The correction reaches existing categorical renderers.** Grouped vertical
   and horizontal bars with no authored bounds share their numeric and category
   domains. Categorical scatter, box, and violin fixtures consume the same
   shared merge without kernel-specific branches. Stacked bars are a consumer,
   not the mechanism's special case.
9. **No representation or surface grows.** `PlotGrid`, `GridCell`,
   `DomainGroupId`, and Rune syntax are unchanged. Differing groups, per-cell
   bodies, and free scales retain PRODUCT-0041's refusals.
10. **Pixel movement is bounded and disclosed.** Existing premise updates are
    enumerated as unbounded categorical facets whose cells previously used
    unequal local category or numeric domains. No explicit-bounds facet,
    ungridded plot, table, line, or non-grid baseline changes.
11. **The workspace is green.** Polariton, its architecture/UI suites,
    libaryon, and the opt-in PRODUCT-0057 render-evidence target pass with no
    newly ignored test.

## Consequences

Faceted categorical plots become comparable in the way PRODUCT-0041 already
promised: missing categories leave an empty but congruent band, and a narrower
partition no longer expands its marks relative to its siblings. Figures that
authored bounds and contained complete category crosses look identical, which
explains why contact did not expose this defect earlier.

The correction also closes PRODUCT-0064's halt. Its stacked facet then inherits
the same shared mechanism as grouped bars, rather than gaining a renderer-local
exception.

## Rejected

- **Share only the numeric range.** Leaves missing categories as unequal band
  counts and violates congruence in the same functions.
- **Union local category lists in cell order.** Reorders interleaved display
  domains and makes facet arrangement an accidental ordering authority.
- **Keep ragged categories as free space optimization.** Changes the pixel
  scale per cell under one domain group; free scales remain representable and
  refused, not inferred from missing data.
- **Repair stacked facets only.** Makes a new renderer compensate for a grid
  defect and leaves every existing categorical kernel inconsistent.
- **Require authored bounds.** Turns a masking fixture condition into language
  policy and leaves categories unsolved.

## Stop condition

> This record may source the categorical sequence from the common resolved
> body, union the numeric range of `CategoricalX` and `CategoricalY` cell
> layouts, apply the complete shared layouts in the grid composite, make
> categorical renderers address transform slots by semantic category identity,
> refresh
> only the enumerated unbounded-ragged facet premises, and add the named tests.
> If implementation requires a new grid/domain representation, kernel-specific
> facet logic, a Rune change, free-scale admission, category synthesis outside
> the resolved display domain, or any pixel change outside unbounded categorical
> facets whose local domains differ, work stops and the requirement returns to
> PRODUCT-0041.
