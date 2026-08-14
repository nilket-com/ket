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

Every row below was read from source on 2026-08-12, **before ADRs 0087 and 0088
landed**. It is the initial census and is not updated as records land; the two
heatmap findings it names were paid by 0088 and the gradient-legend finding by
0087. Rows are kept as the evidence that chose the sequence.

| mark | kernel aggregates? | colour at census | concealed substrate decision |
|---|---|---|---|
| **pie** | **yes** — `.select([col(val_col).sum()])` (`pie.rs:191`) | ramp-only | owns aggregation Toron can now supply |
| **violin** | **yes** — `.group_by().agg()` (`violin.rs:279-280`) **+ KDE** | ramp-only | heaviest statistics in any kernel |
| **radar** | no (`.unique()` domain only, `:228`) | ramp-only | variable-length dimension list |
| **parallel** | no (`:147`) | ramp-only | variable-length dimension list |
| **radial dial** | no | ramp-only | **silent first-row-per-category** — filters, then `.get(0)` (`radial_dial.rs:192`) |
| **funnel** | no | ramp-only | row order and stage monotonicity unstated |
| **heatmap** | no (`.unique()` ×2) | **quantitative** ramp + gradient legend | **silent duplicate-cell overwrite** — `values[xi][yi] = val`; only `max_val` retained, so signed/diverging data cannot be represented — *both paid by 0088* |
| **bubble** | no | **quantitative** ramp + gradient legend (`bubble.rs:70`, `:101`) | size-scale contract — *and, unfound here, the fabricated null values that became B3* |

**None of the eight has any resolved-colour support** — `resolved_color` appears
in none of them.

## Shared prerequisites

| prerequisite | marks blocked |
|---|---|
| **A. Categorical colour identity + persistence** | pie, violin, radar, radial dial, parallel, funnel |
| **B. Continuous colour authority** — four records, see below; the heatmap and bubble surfaces then CI-02 follow them | heatmap, bubble, hexbin |
| **C. Relation-side aggregation** | pie, violin |
| **D. Variadic dimension fields** | radar, parallel |
| **E. Explicit row / cardinality rules** | radial dial, funnel, heatmap |
| **F. Size-scale contract** | bubble |

**A and B are different problems and must not be merged.** Heatmap and bubble
encode a *quantitative* value as colour with a gradient legend; they need a
sequential/diverging scale authority and allocate **no** categorical
`ColorIdentity`. The other six encode categories and do.

### B splits four ways — initial census, 2026-08-12

> **Historical.** This section records the survey that split B, taken **before**
> ADRs 0087 and 0088 landed. Its findings are stated in the past tense and its
> code citations describe the pre-0087/pre-0088 source; they are kept because
> they are why the split happened, not as a description of current code. For
> today's behaviour read 0087 and 0088. B3 was added later that day while
> drafting 0089 — see below.

Tracing the ramp inputs and the guide contract found that heatmap and bubble were
**not** one problem, and that the gradient legend was already wrong.

**The gradient legend misrepresented every multi-stop ramp.**
`LegendSection::Gradient` carried exactly two colour stops, and the swatch was
painted by a straight `lerp(low_color, high_color, t)` while the marks sampled
the full ramp. Under viridis the marks ran purple → blue → teal → green → yellow
while the legend ran purple → yellow through browns the data never used. A reader
matching a mid-range cell against the legend landed in the wrong place. It also
violated PRODUCT-0043's rule that a section carries **every value it prints**:
that one printed colours it never named. A shipped misrepresentation rather than
a missing feature, and it had to be repaired before any new palette could claim
honest guidance. **Repaired by PRODUCT-0087**, which replaced the two-stop model
with ordered positioned stops.

**Their domains differed.** Bubble already normalized over a two-ended range —
`color_range: (f64, f64)` (`bubble.rs:22`, still current) with
`t = (val − min) / (max − min)` and a zero-width guard — so signed data worked.
Heatmap was single-ended: only `max_val` was retained, `t = val / max_val` sent
negatives out of range, `max_val <= 0` collapsed every cell to `t = 0`, and its
legend asserted `low: 0.0` and a literal `label: "Value"` regardless of the data.
**Bubble was therefore the parity subject** for the guide repair; heatmap owed
its own substrate decision, **paid by PRODUCT-0088**.

What the census did **not** find, because a colour survey had no reason to look
at preparation: bubble fabricates values for absent and null inputs. That is B3,
and 0089's signature change is what surfaced it.

Hence four records rather than one — B3 was added 2026-08-12 while drafting
ADR 0089, and CI-02 moved out to a fifth:

- **B1 — honest gradient content and rendering.** The guide model carries
  **ordered positioned stops**, not endpoint colours, so nonlinear and diverging
  ramps stay representable. Bank the current viridis midpoint mismatch first; it
  is a free banked red state, since the defect is live. — **ADR 0087, landed**
- **B2 — heatmap substrate.** A two-ended signed domain and an explicit
  duplicate-cell authority, replacing the silent `values[xi][yi] = val`
  overwrite (`heatmap.rs:103`). — **ADR 0088, landed**
- **B3 — bubble substrate.** Null disposition and finite-value validation for a
  preparation that is infallible and fabricates: an absent colour column becomes
  `vec![0.0f64; height]` (`bubble.rs:213`) and every field is taken with
  `unwrap_or(0.0)` (`bubble.rs:263-266`), so **a null observation renders as a real
  low-end value**. Unplanned; surfaced by 0089's §8 signature change, which
  breaks bubble's `gradient_section` call at `bubble.rs:101` and so forces
  bubble's migration whether or not its preparation is ready. — **ADR 0090,
  landed**
- **B4 — the continuous colour substrate.** One `ContinuousColorScale` owning
  domain, job, resolved ramp, and a provenance-bearing anchor; the colour job
  carried through the cascade; piecewise normalization about the anchor.
  Migrates all three producers. — **ADR 0089, landed**

**CI-02 follows the heatmap and bubble surfaces**, not B4 directly — see steps
6–10. 0089 is explicitly substrate-only, adding no Rune spelling, so CI-02 stays
open past the whole B track; and the authoring words need a mark that can be
authored before they mean anything.

**CI-02 also shrinks.** It is labelled "sequential / diverging / **status**
palette authoring," but status has no consumer — no kernel requests
`ColorJob::Status`, and `spec/mod.rs:2591` already routes it to
`ScaleFamily::CategoricalColor`. Step 10 closes CI-02 for continuous colour and
leaves a status residue whose trigger is the first real status consumer. A status
word today would be unobservable and its gates vacuous.

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
2. **Honest multi-stop gradient content and rendering (B1) — LANDED as
   PRODUCT-0087.** Led, because it repaired a shipped misrepresentation and
   nothing downstream could claim honest guidance until it landed.
3. **Heatmap substrate (B2) — LANDED as PRODUCT-0088.** Two-ended signed domain
   and duplicate-cell authority.
4. **Bubble substrate (B3) — LANDED as PRODUCT-0090.** Null disposition and
   finite-value validation. It had to precede B4: 0089 §8 breaks
   `bubble.rs:101`, so bubble migrates
   regardless, and a bubble that still fabricates `0.0` for nulls cannot uphold
   0089's non-finite invariant. Re-bank bubble after it, since this record may
   legitimately move null-bearing figures.
5. **Continuous colour substrate (B4) — LANDED as PRODUCT-0089.** Migrates
   heatmap, bubble, and hexbin together. Touches **CI-03**, since a real
   continuous authority is what finally displaces the legacy ramp. Leaves
   **CI-02** open.
6. **Heatmap surface, sequential-only — LANDED as PRODUCT-0092.** The first Rune
   constructor for a quantitative mark. Ships with no job word: colour is
   `{ Sequential, None }`, exactly as 0089 leaves it. Implemented with **bespoke**
   `HeatmapFields` / `HeatmapFieldOrigins`, all gates green, before step 8
   generalizes them.
7. **Legend geometry from content — LANDED as PRODUCT-0093.** The gradient guide
   reserves a hard-coded 60 px (`legend.rs:323`), so `-30.0` clips. Not a catalog
   step but a blocker for one: 0092's signed domain exposed it, and step 9's
   bubble legend uses the *other* geometry path, which carries the same defect.
   Replaces both hard-coded authorities with one conservative content-aware
   estimate. Repairs a violation of PRODUCT-0087's own rule — a section carries
   every value it prints, and a clipped endpoint does not.
8. **Mark-field transport — LANDED as PRODUCT-0094.** A refactor of the working box-plot and heatmap
   carriers into dedicated variants — **not** a generic field map:

   ```text
   SpannedMarkFields          MarkFields            (AuthoredPlotSpec, no spans)
   ├── BoxPlot { fields, origins }   ├── BoxPlot(BoxPlotFields)
   ├── Heatmap { fields, origins }   ├── Heatmap(HeatmapFields)
   └── Bubble  { fields, origins }   └── Bubble(BubbleFields)
   ```

   Fields and origins are **coupled at the Rune boundary** so mismatched variants
   cannot be zipped into `None`. One exhaustive `all()` authority feeds
   projection, which is the point: `project_named_channels` (`request.rs:247-253`)
   currently grows one `if let Some(..)` block per mark, and 0092's review caught
   a missing block that would have panicked the factory on the first successful
   authored heatmap. An exhaustive match makes that omission unrepresentable.

   Bubble's variant may be **test-only** until step 9. That is substrate staging,
   not an inert public spelling — no Rune word is added, so nothing unobservable
   is authored.
9. **Bubble surface, sequential-only — LANDED as PRODUCT-0095.** The first new consumer of step 8's
   transport. Five roles against heatmap's three, with an **optional label** whose
   absent-versus-selected-null semantics 0090 §9 already decided — and whose
   variable arity is why `all()` returns a slice rather than a fixed array.
10. **Continuous colour authoring — LANDED as PRODUCT-0091.** Slot words
    `sequential_palette` / `diverging_palette`, the stable 37-token ramp-name
    grammar they create, the mark-level diverging job word, and a dedicated
    modifier-provenance carrier from authoring through planning. Closes
    **CI-02** for continuous colour; **status splits out** as a residue triggered
    by its first real consumer.

    The reconciliation after steps 6–9 rejected two superficially convenient
    reuses. `FieldAttribution::Role` describes schema-bound fields and would have
    produced the false phrase "anchor field"; `MarkFields` carries field identity
    and participates in figure equality, so continuous mapping intent does not
    belong there. Field-span retention was established practice, but carrying a
    **modifier** span to planning was still this record's new join.

   **Steps 6 and 7 are prerequisites, not conveniences.** Heatmap, bubble, and
   hexbin have no Rune constructor at all (`rune_presentation.rs:384-408` lists
   `__bar`, `__line`, `__scatter`, `__area`, `__histogram`, `__box`), so words
   added before those surfaces exist would be observationally inert — the same
   defect that disqualifies a status word. An earlier draft of this plan placed
   authoring at step 6 and the surfaces at step 11.
11. **Funnel substrate — LANDED as PRODUCT-0096.** Row order, stage uniqueness, value
    monotonicity, and removal of the injectable `total_max`. Five silent
    fabrications become refusals — including a dropped null value that renames
    every stage after it.
12. **Faceted hover coordinate seam — LANDED as PRODUCT-0097.** `grid.rs:819-820`
    builds the transformer with the cell's **absolute** bounds and passes a
    **plot-local** cursor. The survey settled the contract: absolute cursor with
    absolute bounds, matching the non-faceted editor path. Exactly two kernels
    expose the mismatch: funnel (`funnel.rs:168-170` after PRODUCT-0096) checks
    bounds containment, while radial dial (`radial_dial.rs:84-93`) derives its
    ring hit geometry from those bounds. Repairs the grid seam centrally with
    **no kernel-local compensation**, a typed widget-space hit-geometry helper,
    and selected first/non-origin probes for both marks. Sequenced after step 11
    so it has a validated funnel kernel as a parity subject.
13. **Funnel surface — LANDED as PRODUCT-0098.** `funnel("stage", "value")`, no implicit-stage form. A
    `Funnel` variant in the 0094 transport, nominal/quantitative binding with
    field-and-row diagnostics, stage colours shared by geometry and legend, and
    the mark-style tier for fills, separators, silhouette, and labels.

    **The first production exercise of PRODUCT-0086.** Funnel is categorical, so
    it allocates a `ColorIdentity` and reaches `AuthoredFamilyId::parse`
    (`session_persistence.rs:714`) and `FamilyAdmissionPolicy::admits` (`:247`) —
    which heatmap and bubble both avoided. Persistence is a first-of-kind
    decision, not plumbing: catalogue the funnel token, prove `ExtensibleFamilies`
    admits and round-trips it, prove the earlier closed policies refuse it,
    persist and restore category assignments, prove a reordered result under one
    lineage keeps its stage colours, and prove reattachment restores the same
    allocation. Prerequisite **A** is finally consumed, and the five remaining
    categorical marks inherit whatever this establishes.
14. **Radial dial substrate — LANDED as PRODUCT-0099.** Consumes prerequisite **E**.
    The census named its silent first-row selection; the survey found that is the
    fourth of **eight** fabrications, and `radial_dial.rs:189` is worse — a failed
    category filter falls back to the whole frame, so every dial can show a value
    belonging to no category it names. Also removes the implicit `1.0` maximum,
    which contradicts the resolver's existing `number(1, "maximum")?`.
15. **Radial dial surface — LANDED as PRODUCT-0100.** Second consumer of
    PRODUCT-0086: one family token, one conversion arm.
16. **`PathBuilder` arc contract — LANDED as PRODUCT-0101.** Not a catalog step
    but a blocker for one. `arc_to` never said whether it connects or starts a new
    sub-path, and both adapters guessed "new" — so radial dial's two-arc annuli
    rendered as chord-closed arcs and pie's centre move was dead, leaving slices
    as chord-bounded segments. Found by screenshot *after* visual acceptance had
    been given. Pins the contract at the seam, adds a path-**topology** gate
    (every existing assertion checks arc operands, none checks how arcs join),
    and adds a production-canvas oracle.
17. **Pie substrate — LANDED as PRODUCT-0102.** Consumes prerequisite **C**, which
    Stage 0 found needs only an exhibit and a diagnostic: Toron already has
    `.aggregate([col(...)], [sum(col(...))])`. Removes the mark's `.sum()` —
    a *correct* answer to one of five defensible questions — plus eleven silent
    dispositions, four disagreeing truths, four measured hover red states, and a
    doubled paint origin.
18. **Pie surface — LANDED as PRODUCT-0103.** PRODUCT-0086's third categorical
    consumer: persistence is one token and one arm as claimed, but colour costs
    **two** `authored_categories` enumerations. Grouped-only authoring; pays
    0102's `.aggregate(...)` exhibit. That exhibit found a relation-compiler
    defect: sibling raw and derived executions retained each other's definitions
    and replayed a linear relation initializer, so both panels failed until each
    execution retained only its own dependency branch.
19. **Multivariate row substrate — LANDED as PRODUCT-0104.** Radar and parallel declare
    the **same six prepared fields with the same names and types**, prepared by
    near-copy functions — one substrate wearing two names. Shares the data
    (ordered dimensions, validated rows, optional category, observed extrema),
    while each kernel keeps its own display-range rule and geometry. Removes
    `row_categories`/`num_categories` and with them an index fallback that
    silently draws a row as category zero.

    **Does not consume prerequisite D** — the first step in the arc where the
    substrate does not. Variadic dimension authoring is the surfaces'.
20. **Variadic multivariate surface — LANDED as PRODUCT-0105.** Consumes **D**. One
    record, two words: `radar(...)`/`parallel(...)` purely variadic with a shared
    `.category(...)` modifier. D turned out to be four coupled boundaries, not one
    arity check — the rewriter's fixed-arity origin injection, a `3n + 2`
    constructor, PRODUCT-0094's **seven-slot** `all()` array (a ceiling once a
    mark is unbounded), and `MultivariateFields` sitting in the wrong layer.
    Also repairs a live defect: the automatic resolver admits one-dimension
    radars and parallels, which paint nothing.
21. **Violin last**, with the KDE moved out of the renderer. By the box-plot
    precedent this is likely a Toron capability question rather than a Polariton
    one.

**Hexbin is not in this sequence.** It consumes the 0089 substrate but stays
producerless and blocked on viewport-aware planning, and it can never diverge
(`0..max_count`, counts non-negative by construction), so it takes no job word
whenever its surface does arrive.

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
- **An abstraction needs a green baseline under it.** Step 7 generalizes box
  plot's and heatmap's field carriers, and it is deliberately sequenced *after*
  0092 ships bespoke carriers with every gate passing. Rewriting 0092 before its
  gates have ever run would leave the abstraction with nothing trustworthy to
  preserve — the same mistake oracle banking exists to prevent, one level up.
  A refactor's parity subjects must have been green at least once.
- **Substrate staging is not an inert spelling.** Step 7 may land a `Bubble`
  variant that only tests construct, and that does *not* violate the hazard below:
  no Rune word is added, so nothing unobservable is authored. The hazard is about
  **public spellings**, not about types awaiting their consumer.
- **A spelling needs a consumer before it needs a word, and the check is
  symmetric.** The first 0091 draft rejected a status word for being
  observationally inert, then proposed three words for marks with no Rune
  constructor at all — the identical defect, in the same record, one section
  apart. Before adding any word, ask what fixture distinguishes the figure with it
  from the figure without it; if the answer needs a surface that does not exist
  yet, the word is premature. Six marks in this plan still have no constructor.
- **A refused compatibility path is a survey instrument.** B3 exists only because
  0089 declined to keep a decomposed `gradient_section`, which forced an
  enumeration of its call sites and turned up a bubble preparation that renders
  nulls as real values. Expect each remaining step's signature changes to find
  work that has nothing to do with that step's subject — and budget for it rather
  than folding it into the record that found it.
- **A cross-seam exhibit must execute the compiled composition, not prove its
  halves separately.** PRODUCT-0103 had a Toron gate proving `.aggregate(...)`
  and a compiler gate proving two executions existed; both passed while the
  compiler emitted the aggregate twice in one branch and leaked it into the raw
  branch. Only the live two-panel exhibit exercised the composition and exposed
  the linear-handle replay.
- **A substrate defect shipped as a spelling is two records, not one.**
  Histogram and box plot each needed a substrate cut first; pie, violin, radial
  dial, and heatmap are the same shape.
