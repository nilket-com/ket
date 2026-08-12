# ADR 0080: the mark style tier and the histogram boundary

Status: proposed 2026-08-11. This record establishes a dedicated skin-resolved
authority for the parts of a mark the author did not encode — `fill`,
`decoration`, `label_text`, and `separator` — migrates the existing skin-blind
consumers onto it with pixels unchanged under the default path, and then adds
the histogram interval boundary that PRODUCT-0079's stop condition 2 asked for.
It supersedes
PRODUCT-0079 §2's *placement* of `mark_fill`; every substantive property that
section decided is preserved.

## Context

PRODUCT-0079 §2 put `mark_fill` in `PlotChrome` and argued the choice explicitly:
extending chrome past furniture was preferable to "minting a fourth projection
for one colour," which "would duplicate chrome's resolution and contrast
machinery." Stop condition 2 named the condition under which that reasoning
expires — a second mark role turning the one-off into a general tier.

It expired on the first exhibit. PRODUCT-0079's stop condition 2 fired because
`demo_sales` produced `[10,20) = 25` and `[20,30) = 25`, rendering as one flat
plateau from 10 to 30 with no interior edge: two adjacent bins indistinguishable
from a single bin of the same count. The premise "one colour" is now false —
`fill`, a boundary, and the eight existing decoration sites are a tier — so the
placement decision is revisited on its own stated terms rather than overturned.

### What already exists, and what it actually means

`ResolvedPlotSettings::mark_decoration_color` (`plot/common.rs:422`, default
`Color::WHITE` at `:487`) is consumed at eight sites across five kernels. It is
not skin-resolved: authored `skin()` prepares only `PlotChrome`
(`gui/state.rs:972`).

**One of the five is reachable.** `PlotType::StackedArea` has live producers
including `raw_mark(PlotType::StackedArea, "area", …)`
(`rune_presentation.rs:466`). `BoxPlot`, `Violin`, `Candlestick`, and `Funnel`
have no producer outside `PlotType::ALL` and match arms — the same unreachable
state PRODUCT-0078 found for histogram. This distinction governs what banking
can mean, and §6 states it rather than implying five equal parity subjects.

**The field already carries two unrelated meanings.** Seven sites stroke
structure; one paints text:

```rust
backend.fill_text(Text {
    content: format!("{:.0}", val),
    color: settings.mark_decoration_color,   // funnel.rs:80 — in-mark label
    size: Pixels(16.0),
```

A tier of `fill` and `decoration` alone would have to file funnel's value labels
under decoration, which is precisely the one-field-two-meanings error
PRODUCT-0079 §2 used to reject `mark_decoration_color` as a fill source. That is
the third role; §2 records why a fourth is also required.

## Decision

### 1. A dedicated tier, not a wider chrome

`PlotMarkStyle` becomes a fourth projection of the theme authority, sibling to
plot chrome, table skins, and categorical data palettes. `mark_fill` moves out of
`PlotChrome`, which returns to being furniture exactly as it was before
PRODUCT-0079.

PRODUCT-0079 §2's reasoning is not contradicted; its premise is spent. A fourth
projection carrying one value would have duplicated machinery for no gain. A
fourth projection carrying four roles and absorbing eight scattered call sites
is the thing that stops the duplication.

The corrected boundary statement: **chrome is furniture; mark style is
everything the author did not encode about a mark.** PRODUCT-0079's "chrome is
everything the author did not encode" was accurate only while one fill sat in
the wrong struct.

### 2. Four roles

| role | paints | resolves to |
|---|---|---|
| `fill` | the body of a mark — a histogram bar, a box body | ink |
| `decoration` | structure over or around the body — medians, whiskers, caps, wicks, boundaries | ink |
| `label_text` | text inside a mark — funnel's per-stage values | ink |
| `separator` | an edge *between* adjacent filled bodies | **paper** |

`label_text` is separated from `decoration` on meaning, not on styling. Text and
strokes answer to different typographic and contrast rules even when they happen
to share a colour today, and merging them would make the eventual authored-label
record start by unpicking a conflation this record could have avoided.

**`separator` exists because an earlier draft of this record was self-defeating.**
It gave the histogram boundary to `decoration`, which resolves to ink — the same
ink that fills the bar. An ink stroke on an ink body is invisible, so the record
would have emitted a boundary operation, passed a gate asserting that operation,
and left the `[10,30)` plateau exactly as it was. The defect it exists to close
would have survived it.

The distinction is not that the two have opposite requirements — both may need to
contrast against a fill, and saying otherwise would overstate it. It is three
narrower facts:

- **Migrated `decoration` must stay ink** to hold §5's byte parity, and its
  backdrop across the eight existing sites is an *arbitrary* fill or the canvas,
  depending on the kernel. Nothing about it is answerable in general.
- **The histogram's fill is this tier's own ink**, known rather than arbitrary,
  so an ink separator on it is invisible — not by bad luck but by identity.
- **`separator` therefore takes the tier's paper counterpart**, which makes its
  contrast against *this tier's* fill a measurable pair, and that is exactly what
  §3 can then claim.

`separator` is not a colour that beats ink by construction. Authored anchors may
set paper and ink deliberately close, and the record does not prevent it: only
the **default** pair is required to reach `3:1`, and an authored low-contrast
pair renders exactly as authored, as everywhere else in this arc.

It also satisfies stop condition 2's rationale rather than evading it: the fourth
role arrives with a consumer already in hand — §8's boundary — not in advance of
one.

### 3. What contrast is claimed, and what deliberately is not

`fill` keeps PRODUCT-0079 gate 14's obligation unchanged: the Polariton-chosen
default measures at least `3:1` against the canvas, recorded as an executable
named result, while an authored low-contrast fill renders exactly as authored. A
mark body sits on the canvas, so a canvas-relative assay is the right question
for it.

`separator` carries the **same** policy against `fill` rather than against the
canvas, because a separator's whole job is to be visible against the two bodies
it divides. Polariton's default pair must measure at least `3:1`; an authored
skin producing a low-contrast pair renders exactly as authored.

This **is** a new measured pair — the record does not pretend otherwise — but a
bounded one: it reuses the existing `contrast(..)` helper against a backdrop this
tier itself resolves. That is exactly why it is answerable when
`decoration`-on-arbitrary-fill is not.

`decoration` and `label_text` receive **no contrast claim in this record**, and
this is a decision rather than an omission. Their backdrop is the mark's own
fill in some kernels and the canvas in others, so no fixed colour can guarantee
a ratio against an arbitrary resolved fill. Asserting a canvas-relative number
for them would be measuring the wrong pair and reporting it as a guarantee.

Neither role has an assay today — `mark_decoration_color` was never in
`visible_contrast_checks` — so nothing is lost. **No *general* backdrop-aware
machinery is built here**: the one added pair measures against a fill this tier
resolves, not against an arbitrary one, and nothing generalizes it. The trigger
is named in the stop condition: the first record that makes in-mark labels or
decoration authorable must decide and test contrast against the *resolved fill*
in the general case, and that record owns the machinery this one declines.

### 4. Resolution mirrors chrome exactly

`PlotMarkStyle` resolves through the same anchors as `PlotChrome`, with the same
three entry points, so a skin reaches marks by the path it already reaches
furniture:

| entry point | `fill`, `decoration`, `label_text` | `separator` |
|---|---|---|
| anchor projection | `PlotRgba::opaque_rgb(anchors.ink)` | `PlotRgba::opaque_rgb(anchors.paper)` |
| `legacy(r, g, b)` | `PlotRgba::opaque(1.0, 1.0, 1.0)` | `PlotRgba::opaque(r, g, b)` — the same background the constructor already builds |
| `legacy_from_iced(background, decoration)` | `PlotRgba::from_iced(decoration)` | `PlotRgba::from_iced(background)` |

All four roles are fully opaque with **no alpha search** — the gridline's
quietest-passing-alpha loop is furniture logic and does not apply to marks.

Both legacy constructors are listed for the reason PRODUCT-0079 established at
its own cost: an exhaustive struct literal makes an omission a compile error, so
the risk is not an unnoticed default but an **unconsidered** one, and this record
decides the value rather than whoever clears the error.

**One projection installs chrome and mark style together.** Today
`resolve_rune_plot_skin` (`gui/state.rs:972`) installs `authored_settings.chrome`
alone. A second independent install point would let `skin(...)` update furniture
while marks kept a stale tier — a figure whose gridlines obeyed the new skin and
whose bars did not. The resolved skin therefore yields `{ chrome, mark_style }`
as one value installed in one step, and gate 8 exercises the **real Rune head
path** rather than synthetic settings, because atomicity is a property of the
wiring and synthetic construction cannot observe it.

### 5. The default path preserves every pixel

`mark_decoration_color` defaults to `Color::WHITE`; `PlotChrome::legacy`'s
decoration is `PlotRgba::opaque(1.0, 1.0, 1.0)`. §4's defaults are therefore the
colours all eight sites already receive, and the migration is byte-identical
wherever no skin is authored.

Funnel's label in particular migrates **byte-for-byte** into a semantically
correct role. Moving it to `label_text` renames what it is; it does not claim it
became readable. Any readability guarantee awaits §3's trigger.

### 6. What banking covers, precisely

The oracle is banked before any migration, per PRODUCT-0042 and PRODUCT-0064.
Its scope is stated honestly rather than uniformly:

- **`StackedArea` is the one reachable subject.** Its boundary stroke is live
  user-visible output reachable through the `area` word, and default-path parity
  for it is a real protection.
- **The other four bank as a refactor net.** With no producer, boxplot, violin,
  candlestick, and funnel have no reachable pixels to protect; banking their
  kernel output catches a migration slip but guards no user's figure. Claiming
  otherwise would repeat the overstatement PRODUCT-0078 avoided by saying plainly
  that an unreachable kernel has no parity subject.

### 7. Under an authored skin, marks change — deliberately

Once resolved, an authored skin reaches mark fills, decorations, and in-mark
labels for the first time. Stacked-area boundaries under a non-default skin will
change colour. That is the purpose of the record, not a regression, and it
follows PRODUCT-0021 §1's posture on deliberate visible change. Parity is claimed
for the default path only, and gate 3 asserts the authored path *does* differ, so
the change is proven rather than assumed.

### 8. The histogram gains an interval boundary

Each histogram bar is stroked with **`separator`** — not `decoration`, for the
reason §2 records — at width `1.0`, matching the width existing structural
outlines already use. Adjacent bins of equal count acquire a visible interior
edge, so the `[10,30)` plateau reads as the two bins it is.

The geometry is pinned rather than left to implementation: a closed rectangle
path per bin at the bar's own bounds, stroke width `1.0`, no join or cap
variation. Two adjacent bins therefore each stroke the shared edge, which is
correct — the edge belongs to both — and is what makes an interior boundary
appear without special-casing adjacency.

The boundary is added **after** the migration lands, so the tier is proven by
existing consumers before a new one depends on it. PRODUCT-0079 §3's rejection of
a literal stands: this is a resolved role, not a reinstated `rgba(0,0,0,0.2)`.

Hover is not an alternative and was never a candidate. Since PRODUCT-0075 made
export work, figures travel as standalone AVIFs, and a boundary a recipient
cannot see is a boundary that does not exist for them — the same argument that
refused range-only whiskers.

### 9. What this record does not decide

The legacy `ColorTheme` ramp stays. Seven kernels still sample
`color_theme.get_color` for mark bodies — radar, radial dial, heatmap, parallel,
candlestick, fill-between, and boxplot — and CI-03 remains independently scoped.
This record migrates the *decoration* consumers, not every ramp consumer.

**`boxplot.rs:50` is named for its successor**: the box body fills from
`settings.color_theme.get_color(t)`, the identical defect PRODUCT-0079 removed
from histogram, currently inert only because nothing can author a box plot. The
box-plot surface record repairs it by consuming this tier's `fill` — which is why
this record precedes it in the sequence.

`funnel.rs:71`'s hardcoded `a: 0.2` over the decoration colour is decoration
*styling*, not text authority, and is out of scope here.

No authored spelling: no Rune word reaches `PlotMarkStyle`. Authored mark colour
remains the palette's business per PRODUCT-0030 §5.

## Implementation sequence

**Stage 0 — bank.** Capture all five kernels' default-path output, with
`StackedArea` distinguished as the reachable subject.

**Stage 1 — the tier.** `PlotMarkStyle`, its four roles, the anchor projection,
both legacy constructors, and skin resolution — with no consumer migrated.

**Stage 2 — migrate.** All eight sites, plus `mark_fill` out of `PlotChrome`.
Default-path output byte-identical to Stage 0.

**Stage 3 — the boundary.** Histogram strokes `separator`; the exhibit is
`test25.rn`, whose `[10,20)`/`[20,30)` pair is already the discriminating case.

## Acceptance gates

1. **Default-path parity.** All five kernels' default-path output is
   byte-identical to the Stage 0 bank after migration, `StackedArea` asserted by
   name as the reachable subject.
2. **`PlotChrome` is furniture again.** `mark_fill` is absent from `PlotChrome`,
   and `mark_decoration_color` is absent from `ResolvedPlotSettings`; no consumer
   reads either.
3. **The authored path differs.** The same figure under two skins emits two
   different fills *and* two different decorations, proving resolution rather
   than a relocated constant — the discriminating half of §7.
4. **All four roles resolve to their exact mapped values.** Each of `fill`,
   `decoration`, `label_text`, and `separator` is produced by the anchor
   projection and by both legacy constructors — twelve assertions against §4's
   table; a role missing from one constructor is the divergence §4 exists to
   prevent.

   The fixtures use **deliberately distinct** inputs — anchors whose paper and
   ink differ, and `legacy_from_iced(background, decoration)` with unequal
   arguments — so that `separator` is observably the paper counterpart rather
   than coincidentally equal to `fill`. `separator != fill` is asserted **of
   those fixtures only**, never as an invariant: degenerate anchors, or
   `legacy_from_iced` called with equal arguments, may legitimately make the two
   identical, and a blanket inequality would fail against correct code.
5. **Funnel's label is byte-identical and correctly homed.** The emitted text
   colour is unchanged from the bank, and it is read from `label_text`, not
   `decoration`.
6. **Contrast scope stays bounded.** Separator-versus-fill *is* new contrast
   machinery, so the gate asserts the boundary rather than denying any addition.
   Four outcomes:
   - `fill` against canvas — the PRODUCT-0079 gate 14 result, retained unchanged;
   - `separator` against `fill` — a new default result, present and `>= 3:1`;
   - `decoration` and `label_text` — **no assay of any kind**;
   - an authored low-contrast value in either measured pair renders exactly,
     keeping both assays observable rather than admitting.
7. **The plateau is gone in pixels, not only in operations.** With
   `[10,20) = 25` and `[20,30) = 25`, an emitted-stroke assertion proves only that
   an operation exists — the assertion gap that let the first draft of this record
   claim a boundary drawn in the fill's own colour. By PRODUCT-0057's classifier
   "the interior edge is visible" is a **backend gap**: whether ink separates two
   abutting bodies depends on realized rasterization, which the operation list
   never carries. The gate therefore drives the production histogram renderer
   with the **exact five-bin relation `test25.rn` produces** — the relation, not
   the file: executing a `.rn` buffer inside the automated probe would make this
   a live-Toron gate, which it is not meant to be. `uat/test25.rn` remains the
   live exhibit.

   It asserts `separator`-coloured ink within a **narrow band around the
   transformed position of `x = 20`**, not an exact pixel-column transition:
   rasterization and antialiasing legitimately spread a 1px stroke across
   neighbouring columns, and a single-column assertion would fail against correct
   rendering. The **pre-boundary capture of the identical relation is the
   negative control** — it must show no separator ink in that band, or the probe
   is not detecting what it claims. The operation-level assertion is retained
   beside it as anti-vacuity proof the stroke was emitted at all.
8. **The boundary is resolved, not literal, and the skin installs atomically.**
   `rgba(0, 0, 0, 0.2)` is absent from the tree, and the emitted stroke colour
   equals the resolved `separator` value; under a second skin it differs. The
   atomic half covers **both real install points**, since either alone would let
   one head keep default mark style while its furniture re-skins:
   - the authored Rune path, `resolve_rune_plot_skin` (`gui/state.rs:972`);
   - the application-theme path, `apply_layer` (`plot/theme.rs:334`), which the
     cascade drives for the application, authored, and pane tiers (`:326-329`).

   Each is asserted by observing a furniture colour and a mark colour change
   **together** in one install. Synthetic settings cannot witness this, because
   they bypass the very points where the two could diverge.
9. **Marks are fully opaque from the projection.** All four roles have
   `alpha == 1.0` after anchor projection, for anchors that drive `gridline`
   through several alpha steps — so the gate distinguishes "marks were left
   opaque" from "these anchors happened to need no search". Asserting the loop
   does not run would be untestable; asserting the value it would have changed
   is not.

## Stop condition

1. **Mark style becomes directly authorable, or a readability guarantee is
   claimed for `decoration` or `label_text`.** Note the trigger is *not* "a skin
   reaches decoration" — this record makes `area(...).skin(...)` do exactly that,
   so that wording would fire on the record's own landing. It fires instead when
   a spelling addresses a mark-style role directly, or when any claim is made
   about `decoration`/`label_text` legibility. Either owns the backdrop-aware
   machinery §3 declines; shipping a spelling without it would assert a
   readability property this record explicitly did not establish.
2. **A fifth mark role is wanted.** Four roles are justified by eight existing
   sites and one new boundary, each arriving with a consumer in hand. A fifth
   arriving without one is the accumulation this tier exists to replace.
3. **Default-path parity cannot be achieved for `StackedArea`.** The one
   reachable subject changing pixels under the default skin means the tier's
   defaults are not the colours the sites were already receiving, and §5's
   premise is wrong.

## Supersession

Supersedes PRODUCT-0079 §2's placement of `mark_fill` in `PlotChrome`, on the
condition that section's own stop condition 2 named. Retains every substantive
property it decided: skin-resolved, fully opaque, no alpha search, canvas-relative
`3:1` for the Polariton-chosen default, authored low-contrast renders unchanged.
Closes PRODUCT-0079 stop condition 2. Amends no colour record; PRODUCT-0030 §5's
palette-consumer enumeration is untouched.

## Rejected

**Two roles, folding funnel's labels into `decoration`.** Attractive because it
matches the stated "at least fill and decoration" and adds nothing speculative.
Rejected because it is the same one-field-two-meanings conflation PRODUCT-0079 §2
used to reject `mark_decoration_color` as a fill source, and the eventual
authored-label record would have to unpick it first.

**Give `label_text` a `4.5:1` guarantee now.** Attractive because text has a
standard target and the assay helper already exists. Rejected because in-mark
text sits on the resolved fill, not the canvas, so a fixed colour cannot
guarantee any ratio against an arbitrary fill — the assertion would measure the
wrong pair and report it as safety.

**Keep the tier inside `PlotChrome`.** Attractive as the smaller diff and it
preserves PRODUCT-0079's placement. Rejected because chrome would then mean two
things, and the projection that resolves furniture would be the projection that
resolves data — the distinction stop condition 2 was written to protect.

**Add the histogram boundary without the tier.** Attractive as the direct fix for
the plateau, and it is one stroke. Rejected because it either reinstates a
literal — the thing PRODUCT-0079 §3 removed — or invents a second one-off role
beside `mark_fill`, which is exactly what stop condition 2 refused.

**Retire the `ColorTheme` ramp in the same cut.** Attractive because seven
kernels sample it and the tier is the natural replacement. Rejected as CI-03's
scope: it needs a parity census across kernels this record does not touch, and
bundling it would put a migration with real reachable pixels behind one with
almost none.

## Consequences

Marks acquire a theme authority for the first time. After this record a skin
reaches furniture through chrome and reaches the **migrated solid-role**
consumers through mark style — the eight sites plus the histogram.

It does not reach everything Polariton draws, and the record must not be read as
claiming so. Seven kernels still take mark bodies from `color_theme.get_color`
(§9), and that residue is CI-03's, unchanged in size by this record. The colours
outside a skin's reach are therefore the ramp consumers *and* the palette-encoded
ones, not the palette alone.

Stacked-area boundaries change under authored skins. That is the first
user-visible consequence of the tier and the only reachable one, since the other
four migrating kernels have no producer.

The box-plot surface inherits a resolved authority for boxes, medians, whiskers,
caps, and outliers, and inherits `boxplot.rs:50` as a named repair rather than a
discovery. That is the whole reason this record precedes it: the histogram nearly
shipped its fill through `ColorTheme` because no alternative existed, and the box
plot has five times the decoration surface to get wrong.
