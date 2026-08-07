# ADR 0043: the legend content model

Status: proposed 2026-08-07. This record decides one thing: that legend content
comes from a closed, typed model through a single generator, with colors taken
from resolved artifacts rather than sampled at render time. It adds **no Rune
surface** and decides **no guide policy** — merging, deduplication,
one-legend-versus-N, panel-label formatting, and placement are PRODUCT-0044,
which depends on this record.

## Context

**Guide and interaction are different stages, and this record unbundles them
first.** PRODUCT-0007 §5's pipeline is `Facet → Layer → Encoding → Scale →
Guide`; interaction is not in it. The two were bundled only because
`PlotKernel` requires `draw_legend` and `hover` to be total — an implementation
constraint, not a semantic one — and PRODUCT-0013 §3's bridge covers both in
one paragraph for that reason alone. This record and PRODUCT-0044 are Guide.
Hover ranking, wording, and cross-cell preference are a separate later record,
and the hover half of the 0013 bridge stands unchanged until it exists.

**The Stage-0 census.** Legend drawing today is nineteen independent
`PlotKernel::draw_legend` implementations plus a trait default:

- **twelve** carry a byte-duplicated layout body — the same `item_height =
  25.0`, `col_width = 150.0`, `rect_size = 15.0`, alpha-0.6 backing, copied
  per kernel (`bar.rs:231` is representative);
- **at least eight distinct content vocabularies** feed them: `group_names`
  (bar, stacked bar), `categories` (pie, violin, boxplot, stacked area,
  radial dial), `stages` (funnel), `category_names` (radar, parallel),
  `max_val`/`max_count` gradient ramps (heatmap, histogram, hexbin), and
  bubble's three-part `color_label`/`color_range`/`size_label` dual;
- **the color authority is stale inside them**: bar's legend samples the
  renderer-time `ColorTheme` ramp at `t = i/(n-1)` — the defect class
  PRODUCT-0029 §6 retired for band fills, still live in the legend. The first
  faceted figure showed it on screen: teal swatches over yellow bars;
- the PRODUCT-0013 §3 legend bridge — the arc's oldest named debt — is **one**
  of the nineteen sites. The bulk predates the arc, from PRODUCT-0007-era
  kernel ownership of all four stages;
- composition multiplies the drawing: the layered composite delegates to its
  last layer, the grid composite draws one legend **per cell**, and the
  factory export paths call `draw_legend` at four more sites.

This is PRODUCT-0037's sixteen-tick-loops problem, squared: content and layout
are each derived independently per kernel, and a third axis — color — has
drifted from its authority. Under any later guide policy the nineteen sites
would agree only by coincidence, and the first disagreement would be a legend
describing marks that are not on the canvas.

The census also records three existing mark-sample glyphs: a filled rectangle
for categorical entries, a stroked line for line series, and a filled point for
scatter series. Their shapes are content facts, not a later visual preference;
flattening them during this extraction would make the legend say less about the
mark than it does today.

## Decision

### 1. Legend content is a closed, typed value

```text
LegendContent  = NonEmpty<LegendSection>
LegendSection  = Entries(NonEmpty<LegendEntry>)
               | Gradient { label, low, high, low_color, high_color }
               | SizeSample { label, low, high }

LegendEntry    { label, swatch: resolved RGBA, sample }
sample         = Swatch | LineSample | PointSample
```

The three section kinds are the census's finding, not an invention: discrete
entries (fourteen kernels), gradient ramps (three), and bubble's dual — which
becomes ordinary as a two-section content rather than a special layout. A
kernel with nothing to say produces no content; absence is `Option`, not an
empty section, following the `NonEmpty` house rule (PRODUCT-0013 §1).

A section carries every value it prints, and nothing it doesn't. `Gradient`'s
`low`/`high` are the printed endpoint values — heatmap's low is the literal
`0.0` it prints today — and `SizeSample`'s `low`/`high` are the size range its
samples interpolate (`bubble.rs`: three samples over `size_range`). The sample
count and the 2→30 radius interpolation are layout policy and stay out of the
content. The boundary test is exact: content that omitted the range would
force the layout to invent values, and layout inventing content is the defect
this model exists to prevent.

`sample` is supplied by the kernel with the same fact that supplies the entry:
categorical marks use `Swatch`, line series use `LineSample`, and scatter series
use `PointSample`. The generator reproduces those three existing glyphs; it
does not invent a universal rectangle and thereby erase mark identity.

The representation is complete by construction: no string-keyed properties, no
optional swatch, no theme name, no callback into a kernel. A `LegendContent`
is ready to lay out without consulting the kernel again.

### 2. One generator; no kernel derives or draws a legend

Kernels supply **facts** through one new method — their content, with resolved
colors — and lose their drawing bodies:

```text
PlotKernel::legend_content(&self, settings) -> Option<LegendContent>
```

A single layout renderer draws any `LegendContent` against the PRODUCT-0031
chrome roles — `legend_background`, `legend_text`, `legend_swatch_fill`,
`legend_swatch_border` — and the existing `legend_x`/`legend_y`/
`max_legend_rows` settings.

**`draw_legend` is removed from `PlotKernel` entirely.** Its eight call sites
— `common.rs:984`, `layered.rs:211`, `scatter_line.rs:81`, `grid.rs:630`, and
four factory export paths — migrate to the generator rendering
`legend_content`. This is deliberately stronger than keeping the signature
for compatibility: Rust cannot seal a provided method, so a retained
`draw_legend` would leave the no-kernel-drawing rule a lint any future kernel
author could trip. With `legend_content` the only legend method and layout a
generator function, a kernel **cannot** emit a legend primitive, because it
is never handed a backend for one — the house rule of `NonEmpty`,
`LayerMark`, and `Table::new`'s required skin, applied to the thing this
record exists to guarantee. PRODUCT-0037 §4's rule, made structural rather
than asserted.

The twelve duplicated layout bodies, both free helper functions, and
`scatter_line.rs:81`'s delegation to scatter — a second, older legend bridge
that predates PRODUCT-0013's — do not survive this record.

### 3. Colors come from the resolved artifact, and the change is deliberate

A `LegendEntry` swatch is the exact resolved color of the thing it names —
band-fill assignments, categorical color assignments, series colors from
`ResolvedPlotSpec` — never a renderer-time `ColorTheme` sample.

This visibly changes pixels: the two-field bar's legend swatch stops being the
ramp's teal and becomes the resolved color it describes. That is deliberate,
on PRODUCT-0021 §1's precedent: preserving the legacy sample would make the
new artifact-sourced contract permanently dishonest, exactly as preserving the
0.72 bar default would have falsified the normalized width scale. The parity
oracle in §6 carries this as its one named, accepted difference.

What a two-field bar's legend *says* — one synthetic entry, one entry per
band, or nothing — is content **policy** and is not changed here: the section
carries today's entries with corrected swatches, and PRODUCT-0044 decides the
rest.

### 4. Composition consumes content and preserves today's shape

- The layered composite's legend is its **last authored layer's content** —
  PRODUCT-0013 §3's delegation preserved at the content level, still marked
  provisional, replaced by PRODUCT-0044's merging decision rather than here.
- The grid composite renders **one legend per cell from that cell's kernel
  content** — today's N-per-cell behavior, preserved. One-versus-N is
  PRODUCT-0044's question, and it now has a single seam to decide it at.
- The factory export paths render through the same generator.

### 5. Hover is untouched

No hover body, ranking, wording, or bridge test changes. The unbundling in
this record's context is a scope statement, not an interaction decision.

### 6. The parity oracle is banked first

Before any kernel is migrated, a recording backend banks the current legend
output of **all nineteen implementations** over representative prepared data,
including the gradient and dual forms. After migration the outputs are equal,
with exactly one class of named difference: swatch colors that previously came
from renderer-time sampling now equal the resolved artifact colors they
describe. Every other byte — geometry, order, text, backing, borders — is
parity, checked by sentinel-normalizing the fills in both corpora and
asserting exact equality of the remainder (gate 4's structured comparison).
The line and scatter census fixtures use an unordered legacy partition
iterator; their entry names are canonicalized only for comparison, while each
path/text pairing, position, and glyph remains exact.

### 7. What this record does not authorize

No Rune surface. No merging, deduplication, or reconciliation of entries. No
change to legend count per figure or per cell. No placement, wording, or
panel-label change. No hover or interaction decision. No new chrome role, no
`PlotChrome` change, no theme or palette change. No change to any mark's
rendering, prepared data, identity, placement, or lifecycle.

## Acceptance gates

1. The pre-migration oracle over all nineteen `draw_legend` implementations is
   banked in a commit that precedes any kernel migration, and the evidence
   cites both commits — a historical condition, per the PRODUCT-0042 gate-10
   precedent.
2. `LegendContent` is closed and complete: three section kinds, non-empty by
   construction, resolved RGBA swatches, and one of the three closed sample
   kinds (`Swatch`, `LineSample`, `PointSample`) on every discrete entry; no
   optional or string-keyed member.
3. No kernel can emit a legend primitive **by construction**: `draw_legend`
   no longer exists on `PlotKernel`, `legend_content` is the only legend
   method, and no kernel is handed a backend for legend drawing. The twelve
   duplicated layout bodies, both free helpers, and `scatter_line`'s older
   delegation are gone.
4. Post-migration output equals the oracle under a **structured comparison**:
   every swatch, gradient, and sample fill is normalized to a sentinel color
   in both corpora and the results are byte-equal; separately, each live
   swatch equals the resolved artifact color of the thing it labels. The one
   accepted difference class is machine-checked, not eyeballed — this is the
   arc's one deliberately non-byte-exact parity, so it is the one whose
   comparison must be structured. Where the legacy partition iterator has no
   ordering contract, line/scatter entry names are canonicalized only; their
   path/text geometry and glyph pairing remain compared exactly.
5. The two-field band-filled bar's legend swatch equals its resolved band
   color — the teal-over-yellow screenshot case, retired and asserted.
6. Gradient (heatmap, histogram, hexbin) and dual (bubble) legends reproduce
   their current geometry through `Gradient` and two-section content: the
   printed endpoint values come from the section's `low`/`high`, bubble's
   three size samples interpolate `SizeSample`'s range under the layout's
   2→30 radius policy, and the size sample consumes `legend_swatch_fill` per
   PRODUCT-0031 §1. Discrete entries preserve the existing filled-rectangle,
   stroked-line, and filled-point sample geometries according to `sample`.
7. The layered composite's legend equals its last layer's content, marked
   bridge coverage; the grid composite still renders one legend per cell.
8. Hover output is unchanged across every kernel, layered, and grid path.
9. Legend chrome roles and `legend_x`/`legend_y`/`max_legend_rows` behave
   exactly as before; the eight former `draw_legend` call sites render
   through the generator with no other behavioral change.
10. No Rune grammar, mark rendering, prepared data, identity, placement,
    group, or lifecycle behavior changes.

## Stop condition

> This record may add `LegendContent`, the `legend_content` method, the single
> layout renderer, artifact-sourced swatch resolution, the migration of all
> nineteen kernels, and the banked oracle. If the implementation requires a
> content policy decision — what a legend says, merging, one-versus-N — a
> hover or interaction change, a Rune surface, a new chrome role, or a change
> to any mark's rendering, the work stops and the requirement returns as its
> own record.

The expected trigger is a kernel whose current legend cannot be expressed in
the three section kinds without deciding what it *should* say. That is
PRODUCT-0044 arriving early, and it must arrive as a record.

## Rejected

- **Deciding guide policy while unifying the mechanism:** the 0031 lesson —
  role extraction and redesign are different records, and bundling them makes
  a defect in either a defect in both.
- **Keeping per-kernel layout with shared constants:** constants drift is how
  the twelve copies happened; the duplication is the defect, not its values.
- **Preserving renderer-sampled swatch colors for parity:** makes the
  artifact-sourced contract permanently dishonest (PRODUCT-0021 §1) and
  re-ships the teal-over-yellow lie under a new mechanism.
- **Replacing every entry with a uniform rectangle:** loses whether an entry
  describes a line or a point. Sample policy is Guide semantics under
  PRODUCT-0019 §5 and belongs in PRODUCT-0044, not as an accidental change in
  this mechanism extraction.
- **A fourth section kind "custom" with a drawing callback:** reopens
  per-kernel legend drawing through the back door and makes the closed model
  a suggestion.
- **Keeping `draw_legend` on the trait for consumer compatibility:** eight
  call sites is a small edit, and an unsealable provided method leaves the
  no-kernel-drawing guarantee a lint rather than an impossibility.
- **Fixing one-legend-versus-N here:** a policy decision with layout-state
  and identity questions of its own, owed to PRODUCT-0044 with this record's
  single seam to decide it at.
- **Bundling the hover bridge replacement:** interaction is not in
  PRODUCT-0007 §5's pipeline and its record does not exist yet.

## Consequences and sequencing

- Nineteen legend derivations become one generator and one layout; a later
  policy decision changes one place and every kernel, layer, grid cell, and
  export path follows by construction.
- The legend stops lying about color: swatches equal the resolved colors of
  the marks they describe, on the same authority chain as the marks.
- PRODUCT-0044 becomes small and purely semantic: merging, deduplication,
  one-versus-N per grid, panel-label formatting, and placement, decided over
  `LegendContent` values rather than nineteen drawing bodies — with the
  faceted screenshot's 4× "Value" as its opening exhibit.
- The interaction record, whenever it arrives, inherits an unbundled hover
  bridge and no legend entanglement.
- The PRODUCT-0013 §3 legend bridge survives one more record, narrowed to a
  content-level delegation with a single replacement point.
