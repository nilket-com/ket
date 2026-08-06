# ADR 0031: resolved plot artifact chrome

Status: accepted 2026-08-06. This record decides one thing: a rendered plot
receives one complete `PlotChrome` value instead of using
`ResolvedPlotSettings.background_color` and `decoration_color` for unrelated
artifact roles. The default reproduces current rendering exactly. This record
adds no theme-name projection, Rune `skin(...)` member, palette behavior, guide
policy, mark styling, or editor-chrome coupling; PRODUCT-0032 owns the first
authored plot skin.

## Context

The flattened renderer model has only two plot-artifact colors:
`background_color` and `decoration_color` (`plot/common.rs:231-232`). The latter
currently supplies title and subtitle text, axis labels, tick labels, gridlines,
axis and tick strokes, legend text, gradient-legend borders, and size-legend
symbols. Kernel code also reads it for box-plot outlines, violin outlines and
medians, candlestick open/close ticks, and funnel borders and in-mark values.
Those last uses are mark styling, not chrome. One variable currently crosses the
boundary this record needs to make structural.

`background_color` likewise serves several jobs. It paints the canvas and masks
the margins, supplies translucent legend backing, clears scatter raster pixels,
draws axis halos, and separates pie wedges. Some of those are compositing uses of
the artifact background rather than independently authored colors. They may
continue using the resolved canvas background, but no mark may receive a new
semantic fill, stroke, or label color from a plot skin.

There is already a four-field `PlotChromeSpec` in `plot/theme.rs`, but the
flattening path consumes only `background` and `label`; its `axis` and `gridline`
values do not reach rendering (`theme.rs:266-270`). It is therefore not a
resolved renderer contract and cannot serve as the new value by being renamed.
The plot settings pane also exposes the old pair as interactive controls and
uses the decoration color on controls outside the canvas. That compatibility
surface must not become an accidental consumer of authored figure skins.

PRODUCT-0023 deliberately deferred non-text checks and said that a later record
must use WCAG's 3:1 non-text criterion rather than the 4.5:1 text criterion for
a hairline. Plot chrome is that later case. It also exposes a real compatibility
tension: today's white gridline is painted at alpha `0.2` over an effectively
black background. Its visible contrast is about `1.66:1`. An extraction cannot
both preserve that output exactly and claim the default passes 3:1.

## Decision

### 1. `PlotChrome` is one closed resolved value

`PlotChrome` has exactly these semantic roles:

```text
canvas_background
title_text
subtitle_text
axis_label_text
tick_label_text
axis_line
gridline
legend_background
legend_text
legend_swatch_fill
legend_swatch_border
```

Each role is a Polariton-owned RGBA value. Alpha is part of the resolved color;
the renderer does not secretly reduce a role's opacity after validation. Where
an existing semantic treatment applies additional hierarchy—such as a
kernel-specific legend backing or a secondary radial/parallel tick—the treatment
remains explicit at the call site and every actual effective alpha is included
in visible-role assays. The representation contains no string-keyed property
map, optional role, theme name, palette, `UiPalette`, or renderer fallback.

This refines PRODUCT-0007 §10's four-role theme sketch into the renderer's
closed vocabulary. It does not weaken that record's ownership rule: interface
colors may project into plot chrome, while plot data ramps never move into
`UiPalette` and interface colors never become data encoding.

`legend_swatch_fill` covers neutral guide samples such as bubble-size circles.
It does not replace categorical swatch fills or continuous ramps, which remain
data colors. `legend_swatch_border` covers the non-data outline around a guide
sample or color bar. The distinction lets chrome style a guide's framing without
claiming ownership of the encoded color it presents.

### 2. The default is an exact compatibility extraction

`PlotChrome::DEFAULT` maps every role to the exact color and effective alpha
used by `ResolvedPlotSettings::default()` today. Application-theme flattening,
including `PlotThemeSpec::DEFAULT_DARK`, receives an equally exact full-role
projection of what it renders today. Roles that currently share
`decoration_color` remain equal in those compatibility values; the role split
does not redesign the picture.

A recording `PlotBackend` captures ordered fill, stroke, text, raster-clear, and
image operations for a representative kernel/layout matrix before extraction.
After extraction the same matrix is byte-for-byte equal for both the library
default and the production application default. Comparing struct fields alone
does not discharge parity because opacity is currently introduced at use sites.

The dead `PlotChromeSpec.axis` and `.gridline` declarations are not treated as
rendering precedent. Preserving values that never reached a pixel would change
the output while calling it compatibility. The implementation may retire the
old four-field shape after its actual two-field projection has been banked.

### 3. Text and non-text use different visible thresholds

Contrast uses libaryon's PRODUCT-0024 relative-luminance primitives. Every alpha
role is first source-over composited onto the exact chrome background used by
the renderer; no check compares an uncomposited declaration or quantizes a
fractional visible color through `u8`.

The following text roles require **4.5:1 or greater** against their chrome
surface:

1. `title_text` against `canvas_background`;
2. `subtitle_text` against `canvas_background`;
3. `axis_label_text` against `canvas_background`;
4. `tick_label_text` against `canvas_background`; and
5. `legend_text` against `legend_background` composited over
   `canvas_background`.

The following non-text roles require **3:1 or greater**:

1. `axis_line` against `canvas_background`;
2. `gridline` against `canvas_background`; and
3. `legend_swatch_border` against the visible legend background at its outer
   edge.

Major and minor grids share one color role; width, dash pattern, and visibility
remain guide settings. Axis and tick strokes share `axis_line`. The thresholds
classify semantic roles, not individual draw calls.

No contrast gate is invented for `legend_swatch_fill`. A neutral size sample is
framed by its checked border, while a categorical or continuous swatch remains
data encoding and is outside `PlotChrome`.

### 4. The parity default is measured, not falsely certified

`PlotChrome::DEFAULT` is the pre-extraction compatibility baseline, not an
authored production skin. Its text, axes, and legend borders pass the applicable
checks, but its post-composite `gridline` is about `1.66:1` and therefore fails
the 3:1 criterion.

That fact is retained as an executable named result. The default is not placed
in a collection described as “all validated built-ins,” and the validator is
not weakened, skipped by role, or fed opaque white instead of the visible
20%-alpha line. Raising the default grid opacity would be a deliberate visual
change and needs its own record.

PRODUCT-0032 may rely on a harder rule: every successfully theme-projected plot
skin passes all eight checks in §3. Thus new authored behavior does not inherit
the compatibility exception. Omitted `skin(...)` continues selecting the exact
legacy path rather than silently opting into a redesign.

### 5. Chrome and mark styling separate by construction

The plot canvas and guides consume `PlotChrome`; mark renderers do not receive a
skin or theme expression. Existing uses of `decoration_color` that style a
box-plot outline or median, violin outline or median, candlestick tick, funnel
border, or funnel value label move to an explicitly non-chrome compatibility
setting. Their current values and output remain exact in this cut.

The canvas background may still be used as a clear or knockout color for scatter
rasterization, axis halos, and pie separators. That is compositing the mark
against its resolved artifact background, not assigning the mark a semantic
color. A non-default chrome assay may change those background-colored knockout
pixels but must leave every data-derived or mark-compatibility fill, stroke, and
label value unchanged.

This boundary is normative:

```text
PlotChrome -> canvas, axes, labels, grid, guide framing
plot palette / resolved spec / mark compatibility -> marks and encoded swatches
```

### 6. Chrome ends at the plot artifact boundary

`PlotChrome` covers pixels drawn inside the plot canvas, including guides drawn
on that canvas. It does not color the host-panel title, tab, focus border,
toolbar, settings drawer, text inputs, hover/status rail, split divider, editor,
or dashboard. Those remain application chrome.

The existing plot settings pane may retain its two compatibility controls. A
background edit expands to the background-family roles and a decoration edit
expands to the legacy decoration-family roles exactly as today; the surrounding
settings UI keeps its own application color. These controls are pane overrides,
not a second named-skin grammar, and this record neither redesigns nor removes
them.

### 7. Skin consumption changes no guide behavior

Legend background, text, neutral sample fill, and sample border are chrome
roles. `PlotChrome` decides only their colors. It does not decide whether a
legend exists, which entries it contains, their order, labels, grouping,
deduplication, placement, or merging.

In particular, PRODUCT-0013 §3's layered-plot legend delegation remains the
same provisional bridge: the last authored layer supplies the temporary legend.
Coloring that output does not accept the bridge as Guide semantics and adds no
acceptance gate for its contents.

### 8. Chrome is presentation, not artifact identity or geometry

Changing a resolved `PlotChrome` changes only the eleven roles in §1 and the
background-compositing pixels named in §5. It changes no relation, channel,
domain, palette position, exact data color, mark geometry, scale, tick value,
guide entry, layout measurement, hit test, hover wording, panel identity,
placement, or render ordering.

No `PlotChrome`, skin name, or role crosses Toron or libaryon protocol. The
resolved settings/artifact retained by a completed plot carries the chrome with
its kernel settings so replacement and stale retention cannot mix a new skin
with old plot content.

## Acceptance gates

1. `PlotChrome` is a closed complete value with exactly the eleven roles in §1;
   no role is optional or string-keyed, and declared alpha remains observable.
2. A pre-extraction recording-backend oracle covers every current
   `background_color` and `decoration_color` semantic family. The library and
   production application defaults produce the exact same ordered operations
   after extraction, including effective alpha and raster background bytes.
3. Renderer chrome reads only the resolved `PlotChrome`; there is no fallback to
   `background_color`, `decoration_color`, `PlotChromeSpec`, `UiPalette`, or a
   renderer default.
4. The five text checks in §3 use 4.5:1 and the three non-text checks use 3:1,
   after declared/use-site alpha compositing over the exact chrome background
   and without `u8` quantization.
5. The compatibility oracle records the default gridline at approximately
   `1.66:1` and classifies only that applicable role as below threshold. No test
   describes the parity default as a validated authored skin.
6. A sentinel chrome changes every one of the eleven declared role consumers in
   the recording matrix while leaving operation order, geometry, text content,
   tick values, guide entries, and interaction unchanged.
7. Box-plot, violin, candlestick, and funnel mark operations retain their exact
   pre-extraction colors under a chrome-only sentinel. Categorical/continuous
   swatch fills and all resolved mark colors likewise remain exact.
8. Scatter raster clearing, axis halos, and pie separators follow
   `canvas_background` only as the explicit compositing cases in §5; no other
   mark fill, stroke, or label reads `PlotChrome`.
9. Legend chrome consumes the legend roles, but legend presence, entries,
   ordering, labels, placement, grouping, and PRODUCT-0013's provisional
   last-layer bridge are unchanged.
10. The plot settings pane's existing background/decoration edits retain their
    current visible behavior and do not make canvas skin roles color any host or
    settings UI outside the artifact bounds.
11. Plot replacement and failed-rerun assays retain kernel/settings/chrome
    together, with no mixed old-content/new-chrome state and no identity,
    placement, focus, tab, or dashboard change.
12. Rune syntax and normal form, libaryon themes and palettes, table skins,
    categorical allocation, data ramps, guide policy, execution, transport,
    configuration, and durability remain unchanged.

## Stop condition

> This record may add the closed `PlotChrome` and RGBA value, extract exact
> library/application defaults, add visible contrast assays, migrate renderer
> chrome consumers, isolate existing mark-decoration compatibility uses, and
> preserve the two plot-pane color controls as grouped pane overrides. If
> implementation requires a theme-name projection, Rune syntax or envelope
> work, changing the default gridline appearance, recoloring any semantic mark
> or data swatch, changing guide contents or layered legend delegation, editor
> chrome, libaryon, palette allocation, execution, transport, identity,
> placement, dashboard, configuration, or durability work, it stops.

## Rejected

- **Rename `decoration_color`:** preserves one variable doing unrelated jobs and
  gives a later skin no closed vocabulary to validate.
- **Use the existing `PlotChromeSpec` as resolved chrome:** two of its four
  fields currently do not render, it lacks the accepted semantic roles, and
  silently activating them breaks parity.
- **Raise grid opacity during extraction:** makes the default pass 3:1 by hiding
  a visual redesign in a substrate refactor.
- **Validate opaque white before the renderer applies 0.2 alpha:** measures a
  color the reader never sees and contradicts PRODUCT-0023's compositing rule.
- **Apply 4.5:1 to every line:** treats hairlines and component boundaries as
  text instead of using WCAG's 3:1 non-text criterion.
- **Let skin color box plots, violins, candlesticks, or funnel labels:** crosses
  from artifact chrome into mark styling and makes `skin` a data treatment.
- **Validate guide borders against data swatch fills:** makes admissibility
  depend on a palette/skin pairing and violates the chrome-against-chrome rope.
- **Color the plot settings UI from `PlotChrome`:** lets an authored artifact
  setting escape into application chrome.
- **Decide legend entries while migrating their colors:** converts a role
  extraction into the Guide record PRODUCT-0013 explicitly deferred.

## Consequences

- Plot chrome becomes a portable, inspectable renderer value rather than two
  ambiguous fields.
- The default remains a trustworthy pixel oracle, including an honestly named
  gridline contrast debt.
- New named skins can be held to text and non-text thresholds without applying
  either threshold to data colors.
- Mark styling, data palettes, plot chrome, and editor chrome have enforceable
  ownership boundaries before `skin("nord")` is admitted.
