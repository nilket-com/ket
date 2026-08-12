# Plot catalog restoration — dependency plan

Status: planning, 2026-08-12. Not a decision record. This holds the dependency
graph for restoring the dashboard-era plot catalog into Rune, so the records it
spawns are sequenced by what actually blocks what rather than by which mark is
asked for first.

Eight marks remain unauthorable: **pie, violin, radar, radial dial, parallel
coordinates, funnel, heatmap, bubble**. Treating them as eight independent
surface records would re-litigate the same colour question eight times and would
let three substrate defects ship as spellings.

## Census — verified, not inferred

Every row below was read from source on 2026-08-12.

| mark | kernel aggregates? | colour today | concealed substrate decision |
|---|---|---|---|
| **pie** | **yes** — `.select([col(val_col).sum()])` (`pie.rs:191`) | ramp-only | owns aggregation Toron can now supply |
| **violin** | **yes** — `.group_by().agg()` (`violin.rs:279-280`) **+ KDE** | ramp-only | heaviest statistics in any kernel |
| **radar** | no (`.unique()` domain only, `:228`) | ramp-only | variable-length dimension list |
| **parallel** | no (`:147`) | ramp-only | variable-length dimension list |
| **radial dial** | no | ramp-only | **silent first-row-per-category** — filters, then `.get(0)` (`radial_dial.rs:192`) |
| **funnel** | no | ramp-only | row order and stage monotonicity unstated |
| **heatmap** | no (`.unique()` ×2) | **quantitative** ramp + gradient legend (`:36-37`, `:97`) | **silent duplicate-cell overwrite** — `values[xi][yi] = val` (`:103`); only `max_val` retained (`:117`), so signed/diverging data cannot be represented |
| **bubble** | no | **quantitative** ramp + gradient legend (`bubble.rs:70`, `:101-102`) | size-scale contract |

**None of the eight has any resolved-colour support** — `resolved_color` appears
in none of them.

## Shared prerequisites

| prerequisite | marks blocked |
|---|---|
| **A. Categorical colour identity + persistence** | pie, violin, radar, radial dial, parallel, funnel |
| **B. Continuous colour authority** — three records, see below (CI-02) | heatmap, bubble |
| **C. Relation-side aggregation** | pie, violin |
| **D. Variadic dimension fields** | radar, parallel |
| **E. Explicit row / cardinality rules** | radial dial, funnel, heatmap |
| **F. Size-scale contract** | bubble |

**A and B are different problems and must not be merged.** Heatmap and bubble
encode a *quantitative* value as colour with a gradient legend; they need a
sequential/diverging scale authority and allocate **no** categorical
`ColorIdentity`. The other six encode categories and do.

### B splits three ways — Stage 0, 2026-08-12

Tracing the ramp inputs and the guide contract found that heatmap and bubble are
**not** one problem, and that the existing gradient legend is already wrong.

**The gradient legend misrepresents every multi-stop ramp, today.**
`LegendSection::Gradient` carries exactly two colour stops
(`plot/legend.rs:38-44`), and the swatch is painted by
`lerp(*low_color, *high_color, t)` (`:587`) — a straight two-stop interpolation
— while the marks sample the full ramp. Under viridis the marks run purple →
blue → teal → green → yellow while the legend runs purple → yellow through
browns the data never uses. A reader matching a mid-range cell against the
legend lands in the wrong place. This also violates PRODUCT-0043's own rule that
a section carries **every value it prints**: this one prints colours it never
names. It is a shipped misrepresentation, not a missing feature, and it must be
repaired before any new palette can claim honest guidance.

**Their domains differ.** Bubble already normalizes correctly — `color_range:
(f64, f64)` (`bubble.rs:22`) with `t = (val − min) / (max − min)` and a
zero-width guard — so signed and diverging data work. Heatmap is single-ended:
only `max_val` is retained (`heatmap.rs:117`), `t = val / max_val` (`:36-37`)
sends negatives out of range, `max_val <= 0` collapses every cell to `t = 0`,
and its legend asserts `low: 0.0` and a literal `label: "Value"` regardless of
the data. **Bubble is therefore the parity subject** for the guide repair;
heatmap owes its own substrate decision.

Hence three records rather than one:

- **B1 — honest gradient content and rendering.** The guide model carries
  **ordered positioned stops**, not endpoint colours, so nonlinear and diverging
  ramps stay representable. Bank the current viridis midpoint mismatch first; it
  is a free banked red state, since the defect is live.
- **B2 — heatmap substrate.** A two-ended signed domain and an explicit
  duplicate-cell authority, replacing the silent `values[xi][yi] = val`
  overwrite (`heatmap.rs:103`).
- **B3 — sequential/diverging palette authoring** for heatmap and bubble,
  closing CI-02 over a guide that no longer lies.

### Why A forces a persistence decision

`DurableLayer::from_runtime` (`session_persistence.rs:699`) maps `PlotType` to an
`AuthoredFamily` of exactly `Scatter | Bar | Line | Area` and **refuses**
everything else. Histogram and box plot never reached it because they allocate no
colour identity — the reason PRODUCT-0079 §7 and PRODUCT-0082 §8 could claim "no
schema change."

Six categorical marks break that. They will allocate identity, they will reach
that mapping, and it will refuse. Stage 0 for PRODUCT-0086 found that the choice
is not limited to adding six closed `AuthoredFamily` variants or mapping several
marks onto existing families. Family remains a semantic part of lineage, while
a **v3** schema can carry a bounded opaque authored-family identifier and
forward-preserve a future valid identifier without interpreting it. That pays
the schema migration once without collapsing unrelated marks. PRODUCT-0064
avoided the migration once by keeping stacked bars in the `Bar` family; six
unrelated marks cannot use that member-versus-family argument.

Note also `resolve_explicit_band_fills` (`plot/spec/mod.rs:2658`) matches only
`Bar | StackedBar`, so none of these marks can obtain colour through the existing
band-fill path.

## Sequence

1. **Extensible categorical family identity + v3 persistence — LANDED as
   PRODUCT-0086** (`ce40c5f5`/`45913cc4`). PRODUCT-0086
   keeps family in the lineage key but makes its durable identifier bounded and
   forward-preservable. It removes the shared persistence block; each of the six
   mark records still earns its own identifier and colour consumer.
2. **Honest multi-stop gradient content and rendering (B1).** Leads, because it
   repairs a shipped misrepresentation and nothing downstream can claim honest
   guidance until it lands. Parity subject: bubble.
3. **Heatmap substrate (B2).** Two-ended signed domain and duplicate-cell
   authority.
4. **Sequential/diverging palette authoring (B3).** Closes **CI-02**, whose
   if-asked trigger is heatmap and bubble arriving together. Touches **CI-03**,
   since a real sequential authority is what finally displaces the legacy ramp.
5. **Funnel** — the smallest categorical consumer, *but only after its row-order
   and stage-monotonicity contract is explicit*.
6. **Radial dial** — needs prerequisite E; its silent first-row selection is the
   PRODUCT-0078 defect class again.
7. **Pie** — after removing its aggregation authority, which Toron can now supply
   as pre-aggregated category/value rows.
8. **Radar + parallel together** over one dimension-list substrate. They are the
   only two needing D, and solving it twice would produce two vocabularies.
9. **Bubble + heatmap surfaces** over the authority from steps 2–4.
10. **Violin last**, with the KDE moved out of the renderer. By the box-plot
    precedent this is likely a Toron capability question rather than a Polariton
    one.

## What this plan does not decide

Nothing. Each numbered step is its own record, drafted and reviewed on its own
evidence. This file exists so the sequence is chosen once, in view of the whole
graph, rather than rediscovered at each mark.

## Standing hazards from the arc

- **A mark that migrates onto one colour authority makes colours that used to
  differ by accident coincide by construction** — PRODUCT-0047 and PRODUCT-0083
  both cost a record to that. Every step here migrates a mark's colour.
- **Silent row-selection is invisible until a fixture has duplicates.** Radial
  dial and heatmap both have it now; the `demo_sales` single-quarter episode is
  the precedent for what a degenerate fixture hides.
- **A substrate defect shipped as a spelling is two records, not one.**
  Histogram and box plot each needed a substrate cut first; pie, violin, radial
  dial, and heatmap are the same shape.
