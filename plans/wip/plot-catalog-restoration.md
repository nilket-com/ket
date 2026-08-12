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
| **B. Continuous colour-scale authoring** (CI-02) | heatmap, bubble |
| **C. Relation-side aggregation** | pie, violin |
| **D. Variadic dimension fields** | radar, parallel |
| **E. Explicit row / cardinality rules** | radial dial, funnel, heatmap |
| **F. Size-scale contract** | bubble |

**A and B are different problems and must not be merged.** Heatmap and bubble
encode a *quantitative* value as colour with a gradient legend; they need a
sequential/diverging scale authority and allocate **no** categorical
`ColorIdentity`. The other six encode categories and do.

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

1. **Extensible categorical family identity + v3 persistence.** PRODUCT-0086
   keeps family in the lineage key but makes its durable identifier bounded and
   forward-preservable. It removes the shared persistence block; each of the six
   mark records still earns its own identifier and colour consumer.
2. **Continuous sequential/diverging scale authority.** Closes **CI-02**, whose
   if-asked trigger is heatmap and bubble arriving together. Touches **CI-03**,
   since a real sequential authority is what finally displaces the legacy ramp.
3. **Funnel** — the smallest categorical consumer, *but only after its row-order
   and stage-monotonicity contract is explicit*.
4. **Radial dial** — needs prerequisite E; its silent first-row selection is the
   PRODUCT-0078 defect class again.
5. **Pie** — after removing its aggregation authority, which Toron can now supply
   as pre-aggregated category/value rows.
6. **Radar + parallel together** over one dimension-list substrate. They are the
   only two needing D, and solving it twice would produce two vocabularies.
7. **Bubble + heatmap** over the continuous authority from step 2. Heatmap
   additionally owes a duplicate-cell rule and a signed/diverging domain.
8. **Violin last**, with the KDE moved out of the renderer. By the box-plot
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
