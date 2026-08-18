# ADR 0130: the temporal `tick_every` surface

Status: proposed 2026-08-17. PRODUCT-0128 preserves a temporal axis's exact
domain and civil frame. PRODUCT-0129 provides a shared fixed/calendar interval
carrier and a selector that is already proved through grid lines, labels, band
requirements, capacity, and `f64` projection. This record exposes that proved
policy to Rune.

## Decision

### 1. `tick_every` is the temporal sibling of numeric `tick_step`

```rune
x_axis().tick_every("1 day")
x_axis().tick_every("1 month")
y_axis().tick_every("3 hours")
```

The modifier parses exactly one shared `TemporalInterval` literal, then asks
PRODUCT-0129's positive-placement constructor to validate it as a stride. The
shared grammar therefore continues to recognize signed and zero deltas for
Toron; this surface refuses them at the `tick_every(...)` argument because a
tick stride must be strictly positive. It is spelled on the axis member that
owns tick selection, following PRODUCT-0123. `tick_step` remains numeric-only
and keeps its PRODUCT-0038 refusal on temporal axes.

### 2. Admission uses the selected axis's resolved temporal domain

After relation execution and schema resolution, `tick_every` validates against
the §3 matrix from PRODUCT-0129. A non-temporal axis, fixed interval on Date,
calendar interval on Time, and a temporal interval on a radial/parallel figure
all refuse at the modifier argument span. The error names the interval kind and
the resolved axis type; no renderer-side fallback decides applicability.

### 3. One axis receives one placement

`tick_every`, `tick_step`, and a second `tick_every` are mutually exclusive,
including across separate `x_axis()`/`y_axis()` members. The second modifier
refuses at its own span. `minor_ticks` with `tick_every` likewise refuses at
the minor modifier span, preserving PRODUCT-0129's deliberate absence of
calendar interpolation.

### 4. The default is unchanged and the explicit feature owns its movement

Absent `tick_every`, every existing figure retains its current Count or numeric
Step placement. An authored interval changes only the selected major ticks,
their grid lines, and the bottom/left band derived from their exact labels. It
does not change mark coordinates, figure clipping, endpoint policy, or text
placement. PRODUCT-0114's reserve-space rule and PRODUCT-0119's labels-sit-at-
ticks rule remain intact.

### 5. A temporal UAT exhibit is the acceptance figure

The record adds a semantic temporal line workbook and seed table with both a
zoned datetime and a Date field. It displays fixed daily/hourly and calendar
monthly/yearly axes across DST and month-end boundaries, and keeps a Count pane
as the control. The exhibit is not a time-bucketing demo: relations provide
typed temporal values; the figure demonstrates the tick policy.

## Gates

1. **Surface delivery.** The compiler recognizes `tick_every`; presentation
   lowers it once into the shared temporal placement; its source span survives
   into relation-result validation.
2. **Shared literal parity plus stride rule.** `tick_every` uses the shared
   grammar from PRODUCT-0129, with no local parsing or unit conversion; positive
   literals select ticks, while the grammar's valid zero and negative deltas
   refuse only at conversion to a temporal placement.
3. **Applicability and attribution.** Every matrix refusal from §2 is driven
   through the ordinary Rune view outcome and points at the interval argument.
4. **Placement exclusivity.** Duplicate temporal, numeric-plus-temporal, and
   separate-member conflicts refuse at the second modifier; non-conflicting
   axis properties still compose.
5. **Minor refusal.** `minor_ticks` after `tick_every` refuses at
   `.minor_ticks(...)`, while numeric Step's established minors remain.
6. **Fixed delivery.** `tick_every("1 day")` on a zoned datetime renders the
   exact fixed selector's ticks, labels, grid lines, and band requirement across
   DST.
7. **Calendar delivery.** `tick_every("1 month")` on Date and zoned Datetime
   renders month-start ticks in their proper civil frame and never uses a fixed
   duration approximation.
8. **Default parity.** Existing UAT and oracle output is byte-identical when
   `tick_every` is absent, including the zoned Count baseline banked by 0128.
9. **Acceptance exhibit.** The semantic temporal workbook visibly distinguishes
   elapsed daily/hourly ticks from calendar month/year ticks, with labels clear
   of the derived band and no new clipping.

## Stop conditions

1. If an implementation has to reimplement parsing, interval arithmetic, or a
   selector that PRODUCT-0129 already owns, stop. That is a second authority.
2. If source attribution cannot survive from `tick_every` to post-relation
   applicability/capacity validation, stop rather than reporting an error
   against the whole figure.
3. If the first exhibit needs tick thinning, abbreviated temporal labels, or
   a space-adaptive policy to be legible, stop. Those are legibility decisions,
   not permission to alter interval selection here.

## Supersession

PRODUCT-0038's numeric `tick_step` surface is not weakened or renamed. This
record adds its temporal sibling precisely because a numeric `f64` step cannot
represent either fixed elapsed time or a calendar period. PRODUCT-0129 remains
the sole authority for temporal selection; this record supplies Rune spelling,
admission, and proof of delivery only.

## Consequences

The public change is deliberately small: one named modifier with an interval
literal. The prior two records carry the complexity that makes the spelling
truthful. A reader can see `"1 day"` and `"1 month"` differ in the source, and
the plotted boundaries, not merely in an implementation comment.
