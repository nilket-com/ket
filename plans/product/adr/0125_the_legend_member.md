# ADR 0125: the legend member

Status: proposed 2026-08-17. PRODUCT-0124 separates facet-header typography
from legend typography. That removes the final blocker PRODUCT-0113 §8 named:
the legend can now own placement, visibility, and text size without moving a
facet row.

~~~rune
legend().at(0.2, 0.8).font_size(16)
legend().hide()
~~~

This consolidates the two global legend words and exposes the final role from
the original typography ask. It follows PRODUCT-0123's rule: **settings live on
the member that owns them.**

## Context

### PRODUCT-0050 split one member into two global words

The current surface is `legend_at(0.2, 0.8)` for `Presence::Show` at an
overlay fraction, and `hide_legend()` for `Presence::Hide`. Both lower to one
`Presence<LegendPosition>` field and mutually exclude each other only after the
complete figure is assembled. Neither owns the still-unspelled text size,
although `legend.rs` uses that value for glyphs and the conservative
legend-gutter width estimate.

There are no checked-in UAT call sites for either word. PRODUCT-0050's
behaviour remains load-bearing: fallback puts generated content in a right
gutter, `at` overlays it at unbounded finite fractions, and `hide` returns full
content bounds.

### PRODUCT-0124 makes legend size an honest role

Before PRODUCT-0124, raw `legend_size` also drove facet headers through
`(legend_size + 2).max(10)`. A legend spelling could not honestly mean legend
text alone. The split introduces a distinct facet-label role, leaving legend
text and its own gutter estimate as the only consumers of `LegendTextSize`.

The prerequisite is strict: this record must not begin until PRODUCT-0124 is
implemented. Otherwise a public legend font size would preserve the hidden
facet coupling it claims to remove.

### Bare `legend()` has no present-tense meaning

Absent legend settings resolve to `Fallback`, the renderer's right-gutter
placement. `legend().at(...)` authors `Show`; `legend().hide()` authors `Hide`.
There is no authored “show the fallback” presence state. A bare `legend()`
would write nothing and render exactly like an absent member, so it refuses at
its own span under PRODUCT-0122 §3.

If a future presence model gives a bare legend an actual effect, that is a
separate legend-policy decision, not a reason to retain an inert handle now.

## Decision

### 1. `legend()` is the sole legend receiver

~~~rune
legend().at(0.2, 0.8) // overlay position
legend().hide()       // visibility
legend().font_size(16) // text and its own width estimate
~~~

`at(x, y)` accepts two finite numeric fractions without range clamping, exactly
as `legend_at` did. `hide()` takes no arguments. `font_size(n)` takes one
bounded logical-pixel value, inclusive `6.0 ..= 96.0`.

`font_size` is the right name because the legend has exactly **one** text-size
role: the existing setting drives entry labels, gradient endpoint labels, and
gradient titles together. A receiver that is text itself, such as `title()`,
also has one size trivially. `x_axis()` differs because it contains two
independently-sized text roles, title and ticks, so it must name which one with
`label_size` or `tick_size`.

If legend text later splits into independently-sized roles, `font_size` must
split with it — for example, to `entry_size` and `title_size` — for the same
reason axis typography is qualified. This record names the current one-size
condition rather than treating the container as text.

### 2. Visibility and typography compose fieldwise

`at` and `hide` are alternative values of one visibility/placement property; a
second one refuses at its own modifier span, whether chained or authored on a
separate `legend()` element. `font_size` is independent and composes with
either form:

~~~rune
legend().at(0.2, 0.8),
legend().font_size(16),
~~~

is equivalent to the chained spelling. A hidden legend may retain an authored
font size. The size is dormant while hidden, just as it is when legend policy
emits no content; hiding does not silently erase typography.

### 3. Legend typography is typed and facet-independent

`LegendTextSize` is a `6.0 ..= 96.0` newtype through the three carriers:
`PlotTypography`, a partial `Override<LegendTextSize>` in `PlotSettingsLayer`,
and `ResolvedPlotSettings`. Its default is 14.0, the existing legend value.
The partial slot wins over the atomic carrier within one cascade tier.

Legend text and every existing size-dependent PRODUCT-0093 width estimate
consume this one resolved value. PRODUCT-0124's `FacetLabelTextSize` is neither
read nor written here: a legend font change may adjust its own glyphs and the
gutter geometry that already depends on text size, but never a facet header or
facet row.

### 4. Retire the two old words without aliases

`legend_at` and `hide_legend` become unknown Rune spellings. There are no
fixture or external compatibility call sites. An alias would leave two public
producers of the same visibility setting.

PRODUCT-0050's resolved `Presence` semantics remain current. Only the producer
and attribution move: the second `.at(...)` or `.hide()` reports at the
modifier that made the member ambiguous.

### 5. Default rendering is byte-identical

At `LegendTextSize::DEFAULT == 14`, fallback-gutter width, overlay placement,
legend glyphs, and facet geometry match the banked PRODUCT-0124 state exactly.
Figures without legend content and figures without facets remain unchanged.

A nondefault `.font_size(n)` deliberately changes legend glyph size and only
the existing gutter geometry that already estimates text width. Fixed legend
geometry stays fixed. The record adds no text measurement.

## Implementation sequence

**Stage 0 — bank PRODUCT-0050.** Capture fallback-gutter, overlay, and hidden
legends at default size, including one faceted figure to prove the 0124 split.

**Stage 1 — typed legend carrier.** Add `LegendTextSize`, the partial slot,
five-tier resolution, and typed reads at every existing legend text/estimate
consumer. Defaults must match Stage 0 byte for byte.

**Stage 2 — member and composition.** Add `legend()` with `at`, `hide`, and
`font_size`; preserve axis/title font routing and add local refusals.

**Stage 3 — retire and exhibit.** Delete the two words and add a compact UAT
reference showing a chained overlay and separate composing legend members.

## Acceptance gates

1. **Default parity.** Banked fallback, overlay, and hidden legends preserve
   content bounds, origins, text operations, and pixels. A faceted figure also
   preserves its header row. This catches a 14-px default drift or residual
   legend-to-facet read.
2. **PRODUCT-0050 survives.** `legend().at(1.5, -0.5)` lowers to the same
   unbounded finite `Presence::Show`; `legend().hide()` lowers to `Hide`,
   returns full content bounds, and draws no legend.
3. **Size drives all and only the legend geometry that names it.** At two
   authored sizes, every legend text operation uses the resolved size and each
   existing size-dependent width estimate recomputes from it; fixed legend
   geometry stays fixed. Plot marks, facet row, and facet header do not move.
   Draw-only sizing and a residual facet coupling both fail.
4. **The split holds in both directions.** A nondefault atomic legend type
   value changes legend geometry only; a nondefault atomic facet-label value
   changes facet geometry only. One direction alone would pass a one-way link.
5. **Typed five-tier resolution.** `LegendTextSize` is present in atomic,
   partial, and resolved carriers; library, application, authored, layer, and
   pane values resolve in order, with the partial slot winning in one tier.
6. **Fieldwise composition.** Chained `.at(...).font_size(...)` and separate
   disjoint `legend()` elements lower identically. A duplicate `at`, duplicate
   `hide`, duplicate `font_size`, or an `at`/`hide` pair — including across
   elements — refuses at the second modifier span.
7. **Diagnostics are local.** Bare `legend()` refuses at its member span.
   Invalid `at`, nonempty `hide`, and out-of-range `font_size` refuse at their
   modifier spans. `title("T").at(...)` and `skin("greys_rs2").hide()` do the
   same.
8. **One spelling remains.** `legend_at(...)` and `hide_legend()` are unknown,
   with no aliases, compiler rewrite entries, or retained VM producers.
9. **No measurement authority.** `PlotBackend` gains no text-extent method.
   PRODUCT-0093's declared estimator remains the one legend-width authority.
10. **A reference exhibit proves composition.** Its workbook compiles and
    shows chained and separate legal forms; expected failures remain unit tests.

## Stop conditions

> If PRODUCT-0124 has not landed first, stop. A legend size must not still move
> facet headers.

> If making bare `legend()` meaningful needs a new authored presence state,
> stop. That changes fallback/hidden semantics and is a policy record.

> If a font-size change needs realized text extents rather than PRODUCT-0093's
> declared estimator, stop. That fires the backend-measurement deferral.

> If a compatibility user of either retired word appears, stop and record an
> explicit migration policy instead of retaining aliases by accident.

## Supersession and inheritance

This record supersedes PRODUCT-0050's public `legend_at` and `hide_legend`
spellings and their figure-scope mutual-exclusion diagnostic. It preserves the
resolved `Presence` semantics while moving attribution to the second modifier.

It completes PRODUCT-0113 §8's planned legend consolidation, depends on
PRODUCT-0124's facet/legend split, and inherits PRODUCT-0123's member ownership
and fieldwise composition, PRODUCT-0112's partial-override pattern, and
PRODUCT-0093's estimator unchanged.

## Rejected

**Keep aliases.** Rejected because no fixture or external user needs them and
two producers would make member ownership optional.

**Add top-level `legend_size(n)`.** Rejected because `legend()` owns placement
and visibility; a global word would spend namespace despite a real receiver.

**Make bare `legend()` force visibility.** Rejected because no authored
fallback/show-default state exists. Inventing one would change presence policy
only to make a receiver look symmetrical.

**Make `hide()` erase `font_size`.** Rejected because visibility and typography
are independent settings. A dormant size is ordinary with hidden or no-content
legends; one property must not silently mutate another.

## Consequences

The original seven-role typography ask is complete: legend text becomes its
seventh authorable canvas role. Facet headers remain the separately owned eighth
role PRODUCT-0124 surfaced.

Rune loses two global legend words and gains one owner-shaped member. The
remaining plot work is policy rather than a blocked surface: interior facet
spill and long-label thinning, abbreviation, or elision.
