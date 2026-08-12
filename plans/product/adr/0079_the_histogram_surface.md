# ADR 0079: the histogram surface

Status: proposed 2026-08-11. This record makes the histogram authorable in Rune
over the interval substrate PRODUCT-0078 established. It decides, in this order
and deliberately: that count is encoded once and never as colour; where the one
remaining fill comes from; that there is no legend; and only then the spelling.
The ordering is load-bearing — the colour decision determines whether the mark
allocates durable colour identity, and therefore whether a shipped persistence
schema must change.

## Context

PRODUCT-0078 replaced the dense bin-count model with validated explicit
intervals and removed the second binning authority. It deliberately deferred
three things to this record: the Rune spelling and column mapping; PRODUCT-0064's
standing obligation that a newly authorable mark prove its renderer consumes
resolved assignments; and repair of two theme-blind decorations.

### The two decorations, and what they actually depend on

The paint path ramps bars by count — `t = count / max_count` through
`settings.color_theme.get_color(t)` — and strokes each bar with a literal
`Color::from_rgba(0.0, 0.0, 0.0, 0.2)`. Neither consults a skin, a chrome role,
or a palette.

Removing count-as-colour is necessary but **not sufficient** to stop the legacy
sampling. The single-series idiom already in the codebase routes through the same
catalog:

```rust
fn series_color(series: &ScatterSeries, settings: &ResolvedPlotSettings) -> Color {
    series.resolved_color
        .unwrap_or_else(|| settings.color_theme.get_color(series.color_t))
}                                              // plot/kernels/scatter.rs:449-453
```

So a histogram that merely stops ramping still needs a defined fill, and
inheriting the existing idiom would re-acquire the `ColorTheme` dependency
through a different door. §2 exists because of this.

### Why the colour decision must precede the spelling

`DurableLayer::from_runtime` (`session_persistence.rs:699`) maps a layer's
`PlotType` to an `AuthoredFamily` of exactly `Scatter | Bar | Line | Area`, and
**refuses anything else**: "plot type {other} has no durable authored family".
A fifth variant would be a schema change to `polariton_session` v2, a shipped
format holding real user data — the exact cost PRODUCT-0064 avoided by keeping
stacked bars in the `Bar` family.

That mapping is reached from exactly two call sites, both inside
`ColourIdentityDto::from_runtime` (`:628`, `:640`), iterating colour
**assignments** and allocation **spaces**. A layer that allocates no colour
identity never reaches it.

Two independent guards already keep a histogram out of that path.
`resolve_explicit_band_fills` (`plot/spec/mod.rs:2545`) returns empty unless the
kernel fields are `Bar` or `StackedBar` (`:2553-2555`) **and** the category
channel carries a `ScaleDomain::Categorical` (`:2569`). A histogram is neither:
its kernel fields are its own, and its axis is continuous intervals. It also
takes no Color channel, per §1.

**Therefore a histogram allocates no colour identity, never reaches
`DurableLayer::from_runtime`, and `polariton_session` stays at v2 with no new
`AuthoredFamily` variant.** This is a consequence of the colour decision, not an
independent choice, which is why deciding colour first is not merely tidy.

## Decision

### 1. Count is encoded once, by height; count-as-colour is removed

The ramp and the `max_count` normalization are deleted from the paint path. A
bar's height is the count; nothing else encodes it.

Encoding one quantity twice is not redundancy that costs nothing — it produces a
figure that *appears* to carry two variables and invites a reader to look for the
second. The count gradient legend published alongside it made that reading
explicit, announcing a colour scale whose only information was already on the
value axis.

This also discharges PRODUCT-0064's obligation in the strongest available form.
That rule requires each newly authorable mark to prove its renderer consumes
resolved assignments; a histogram consumes none because it has none, and §2
makes the absence structural rather than incidental.

### 2. The fill is a new skin-resolved mark role, and that is a boundary crossing

A histogram bar is filled from a single role resolved through the active skin. It
does **not** call `color_theme.get_color`, and it does **not** consume the figure
palette.

**The role does not exist and this record adds it.** `PlotChrome` today holds
eleven roles and every one is furniture — `canvas_background`, `title_text`,
`subtitle_text`, `axis_label_text`, `tick_label_text`, `axis_line`, `gridline`,
and four legend roles (`plot/chrome.rs:93-103`). Nothing there colours a mark.

**A third path exists and is rejected on its merits, not overlooked.**
`ResolvedPlotSettings::mark_decoration_color` (`plot/common.rs:422`, defaulting
to `Color::WHITE` at `:487`) is a real non-furniture mark tier, consumed by box,
violin, candlestick, funnel, and stacked area, and described by PRODUCT-0068
(`0068_gridlines_beneath_marks.md:79`).
An earlier draft of this section claimed no such home existed; that was wrong.
It is rejected for two specific reasons rather than by omission:

- **It means mark *structure*, not mark *substance*** — medians, wicks, whiskers,
  boundaries. A histogram's fill is the datum's body, and overloading the
  decoration tier would make one field mean two unrelated things across six
  kernels.
- **It is not skin-resolved.** Authored `skin()` prepares only `PlotChrome`
  (`gui/state.rs:972`), so a fill sourced from `mark_decoration_color` would not
  change with the skin and would fail gate 3 by construction.

The remaining choice is between extending chrome past furniture and minting a
fourth theme projection for one colour. This record extends chrome, because
chrome is already the skin-resolved, contrast-obligated projection for what
*Polariton* chooses rather than what the author encodes — and an unencoded fill
is precisely that. A fourth projection carrying a single value would duplicate
chrome's resolution and contrast machinery to avoid admitting the boundary moved.

The crossing is named rather than slipped in: after this record, "chrome is
everything except the marks" is no longer true, and the accurate statement is
"chrome is everything the author did not encode."

**The role is pinned completely**, so no attachment point is left to
implementation discretion:

| aspect | decision |
|---|---|
| name | `mark_fill` |
| `legacy(r, g, b)` (`chrome.rs:109`) | `mark_fill: decoration`, i.e. `PlotRgba::opaque(1.0, 1.0, 1.0)` — the same opaque white the constructor already gives `title_text`, `axis_line`, and the other decoration roles |
| `PlotChrome::DEFAULT` | inherited from `legacy(0.001, 0.001, 0.001)`: opaque white on a near-black canvas |
| `PlotChrome::APPLICATION_DEFAULT` | inherited from `legacy(0.0, 0.0, 0.0)`: opaque white on black |
| `legacy_from_iced(background, decoration)` (`chrome.rs:128`) | `mark_fill: decoration`, i.e. `PlotRgba::from_iced(decoration)` — the second, separate constructor, used by application-theme resolution. Its exhaustive struct literal means adding the role breaks compilation rather than falling back silently, so the risk is not an unnoticed default but an unconsidered one: it is listed so that **this record** decides the application-theme value, rather than whoever clears the compile error |
| `project_anchors` (`chrome.rs:281`) | `mark_fill: ink`, i.e. `PlotRgba::opaque_rgb(anchors.ink)` — **fully opaque, with no alpha search**. The gridline loop that walks `[0.35 … 1.0]` seeking the quietest passing alpha does not apply: a gridline is quiet furniture behind the data, a fill *is* the data, and there is no quieter-is-better direction to search |
| `visible_contrast_checks` (`chrome.rs:168`) | array widens from 8 to **9**, `mark_fill` measured against the canvas at the non-text `3:1` target |

Nothing above is left as "a suitable value": each entry names the exact
constructor expression, because a colour and an alpha chosen at implementation
are precisely what this record exists to stop being chosen there.

**Contrast is observable, not an admission gate.** PRODUCT-0034 and
PRODUCT-0035 settled that the assay reports facts and never refuses, and this
record does not reopen it. Precisely: the *Polariton-chosen* `DEFAULT` must
measure at least `3:1` against the default canvas, recorded as an executable
named result (gate 14); an *authored* skin producing a low-contrast `mark_fill`
still renders exactly as authored, because valid-but-ugly renders. The
obligation binds what Polariton picks, not what the author picks.

Rejecting the palette is deliberate and narrow: PRODUCT-0030 §5 enumerates
palette consumers as an authored categorical Color channel plus two-field bar
band fills. Adding a third would amend that record to give one mark a colour it
does not vary, and would make a histogram look like a palette participant when
it has no categories to distinguish.

### 3. No outline ships in this cut

`rgba(0, 0, 0, 0.2)` at width `0.5` is removed and **nothing replaces it**. A
20%-black literal is invisible on a dark canvas and an unauthored border on a
light one, so it is not a neutral default — it is a light-canvas assumption
compiled in.

An earlier draft said "a chrome role or nothing," which is the same incomplete
contract PRODUCT-0078 had to correct in "remove or refuse": two outcomes and an
implementer left to choose. The outcome is pinned to **no stroke operation
emitted**, and gate 8 asserts that single result.

The risk this accepts is stated rather than hedged: contiguous bins of equal
height become one visual plateau with no interior edge, and interval boundaries
become unreadable in that region. If the Stage 3 exhibit shows that loses
necessary information, **stop condition 2 fires** and mark decoration gets its
own decision — which is the correct route for adding a second mark role anyway,
rather than smuggling one in beside `mark_fill`.

### 4. No legend

A histogram publishes no legend. With count on the value axis and no colour
encoding, a legend has nothing to disambiguate, which is precisely
PRODUCT-0044 §1's omission rule for synthetic single-entry sections. It is also
the treatment PRODUCT-0044 §5 already gives band-fill bars, on the same
reasoning: the axis is the guide.

This is inherited policy, not a new rule. The gradient legend disappears as a
consequence of §1 rather than by separate decision.

### 5. The spelling names three fields

```rune
histogram(bin_start, bin_end, count)
```

Three explicit quantitative fields, matching PRODUCT-0078's `HistogramBin`
exactly. There is no one-field form: `histogram(value)` would have to bin, and
binning is the authority PRODUCT-0078 removed.

Applicability, refused at resolution time with span attribution per
PRODUCT-0007 §2's two-phase rule:

| field | requirement |
|---|---|
| `bin_start`, `bin_end` | quantitative |
| `count` | integer dtype; a float column refuses, naming the column and its dtype |

`count` requires an integer column rather than accepting a float and checking
integrality per row, so the refusal is a **schema-wide** fact read from the
returned schema before any row is ingested — one statement about the column,
rather than a verdict that depends on which rows happened to come back.

**Dtype is necessary and not sufficient, so there is a second, value phase.**
A signed integer column satisfies the schema check and can still hold a negative
count, and any column can hold nulls. Without this phase stated, stop condition 4
would fire on the first signed column — the check would be *guaranteed*
inadequate rather than merely at risk. The two phases are therefore:

Both phases run **after** Toron executes — PRODUCT-0007's channel and domain
resolution read the *returned* schema, not an ahead-of-time one — so neither is
"before execution":

| phase | refuses | reading |
|---|---|---|
| channel resolution | a float or non-integer `count` | the returned schema, before row ingestion |
| value | a negative `count`, naming the row index and the value | the returned rows |
| value | a null in `bin_start`, `bin_end`, or `count` | the returned rows |

The distinction that survives is not early-versus-late but **schema-wide versus
per-row**: a dtype refusal condemns the column and can be reported once, while a
value refusal must name the row it found.

A null **refuses**; it is never treated as a dropped bin, because silently
discarding a row would let the rendered distribution disagree with the relation
that produced it — the same misreport PRODUCT-0078 §3 refuses for synthesized
bins, arriving from the opposite direction.

Conversion from the column's integer representation to `u64` is **checked**,
never wrapping. No refusal is written for "too large for `u64`": the supported
integer dtypes top out at `u64`, so no representable value exceeds it, and
signed overflow is already the negative case above. What the check earns instead
is exactness — `i64::MAX` and `u64::MAX` arrive as themselves rather than
saturating or truncating. Both value refusals attribute to the mark's span and
name the offending row.

### 6. Ordering is the relation's job, and an unordered relation refuses

`HistogramBins::new` refuses unordered or overlapping input. The surface does not
sort to satisfy it. Sorting is relation-stage work — the rule PRODUCT-0056
established and PRODUCT-0064 §8 restated — so an author writes
`.sort([col("bin_start").asc()])` in the relation.

The substrate's refusal is surfaced to the author at the mark's span, naming the
failing invariant, the offending index, and the relation-side remedy. A
constructor error that reaches the author as an internal message would waste the
one thing PRODUCT-0078's typed errors were built for.

### 7. The modifier register is empty in this cut

PRODUCT-0046 attaches modifier registers to the mark, so this record must say
what a histogram's register contains: **nothing**.

`bar_width` and `category_gap` are the obvious candidates by analogy and both are
meaningless here — width is `end - start`, read from the data, and there are no
categories to gap. Stating the register as empty prevents them being added by
resemblance to `bar`, which is how a mark acquires settings that silently
contradict its own geometry.

### 8. What this record does not decide

No horizontal orientation. PRODUCT-0055 resolves bar orientation from the channel
pair, but a histogram's three fields have no ambiguous pair to resolve, so a
horizontal form needs its own decision and has no requester.

No layering: `histogram` does not enter `layers([...])`, inheriting
PRODUCT-0013 §2's exclusion for the same unresolved scale-composition reason.

No faceting change — a histogram in a facet grid is governed by PRODUCT-0041 and
PRODUCT-0065 without amendment.

No colour authoring for the mark: `palette()` does not apply to it, per §2, and
a future request for authored histogram fill is a new decision rather than a
widening.

CI-03's `ColorTheme` catalog retirement is untouched and stays independently
scoped. §2 removes this mark's dependence on the catalog; it does not retire the
catalog, and this record must not be cited as progress toward that.

## Implementation sequence

**Stage 1 — colour and decoration.** Remove the ramp, the `max_count`
normalization, the gradient legend, and the literal outline; introduce the fill
role. Gated before any Rune word exists, so the mark is theme-correct before it
is authorable.

**Stage 2 — resolution and spelling.** Reintroduce resolved kernel fields for
the three columns and the `histogram` word, replacing PRODUCT-0078 Stage 3's
typed refusal at `plot/spec/mod.rs:1960`.

**Stage 3 — the exhibit.** A `uat` buffer over `demo_sales` bins produced by
Toron's `FLOOR` bucketing, including a deliberate gap so the sparse contract is
visible on screen.

## Acceptance gates

1. **No `ColorTheme` sampling.** The histogram paint and legend paths contain no
   call to `color_theme.get_color`, asserted structurally; and the emitted fill
   for a fixture equals the resolved chrome role's colour, asserted by value.
   The second half matters because absence of a call does not prove the right
   colour arrived.
2. **Fill does not vary with count.** Two bins with different counts emit the
   **same** fill colour. This is the discriminating gate for §1: under the old
   ramp they differed, and any residual count-driven tint fails here.
3. **Fill follows the skin.** The same figure under two skins emits two different
   fills, proving the role is resolved rather than constant.
4. **No palette consumption.** A figure authoring `palette(...)` around a
   histogram emits the identical fill as one without it, and the resolved spec
   carries no band-fill assignment.
5. **No colour identity is allocated, asserted at resolution.** Allocation
   happens during resolution, not rendering, so a post-render check would be
   asking after the fact. The gate takes a known `ColorIdentity` snapshot,
   resolves a histogram against it, and asserts the returned snapshot is
   **unchanged** — no assignment, no allocation space — so
   `DurableLayer::from_runtime` is never reached. This is the gate that holds
   the Context's no-version-change claim.
6. **A session round-trips without a histogram spelling.** A persisted payload
   contains no kernel and no plot type; it contains the panel meaning and the
   colour relation, and restoration yields an unresolved panel until Rune
   reruns. The gate therefore asserts five things, which together prove v2
   suffices without implying the plot is serialized:
   - the payload is schema v2;
   - the panel mapping is present;
   - the colour relation is empty, and no plot-type or `AuthoredFamily`
     spelling appears **in the `polariton_session` owner payload**. The
     assertion is scoped to that owner deliberately: PRODUCT-0060 persists the
     Rune document, which of course contains the literal `histogram(...)` source
     the author wrote, so "nowhere in the session" would be false by
     construction;
   - the restored panel is **unresolved**;
   - rerunning the buffer rejoins that same panel.
7. **No legend.** `legend_content` returns `None` for every histogram, including
   one with positive counts — asserted as absence, following PRODUCT-0078
   gate 11's precedent that "does not panic" admits too much.
8. **No outline at all.** The literal `rgba(0, 0, 0, 0.2)` is absent from the
   tree **and** the histogram emits no stroke operation for any fixture,
   including contiguous equal-height bins. One pinned outcome, asserted
   positively as absence rather than as "not the old literal".
9. **The word resolves.** `histogram(bin_start, bin_end, count)` compiles and
   produces the interval sequence the substrate validated, asserted through the
   production compile path against a real `uat` buffer as PRODUCT-0076 gate did.
10. **Each schema failure refuses with its span.** A non-quantitative
    `bin_start`, a float `count`, and a missing column each refuse at the mark's
    span, naming the column and its dtype. Three cases, three distinct messages.
11. **Each value failure refuses, and the valid twin renders.** A signed
    integer `count` column holding `-1`, and a null in each of `bin_start`,
    `bin_end`, and `count`, refuse at the mark's span naming the row index and
    the value. Each case is paired with the same relation made valid, which must
    render — the negative half is what proves the refusal is reachable and not
    simply a column the mark rejects wholesale. A signed column of non-negative
    counts must render, or channel resolution has silently become a signedness
    ban. Checked conversion is proven by exactness rather than by an
    unreachable overflow: a signed `i64::MAX` count and an unsigned `u64::MAX`
    count each render with that exact stored count, neither saturated nor
    truncated.
12. **An unordered relation refuses at the author's span.** The relation sorted
    descending refuses with a message naming the ordering invariant, the index,
    and the relation-side remedy; the same relation sorted ascending renders. The
    negative half is what proves the diagnostic is reachable rather than dead.
13. **The register is empty.** `.bar_width(...)` and `.category_gap(...)` on a
    histogram refuse as inapplicable, per PRODUCT-0016's rule that a setting
    which cannot apply is never silently dropped.
14. **Contrast is measured, and only binds Polariton's own pick.** Two halves,
    and the second is what keeps the assay observable rather than admitting:
    - `PlotChrome::DEFAULT`'s `mark_fill` **asserts** at least `3:1` against
      `canvas_background`, recorded as an executable named result — a number in
      the tree, not a claim — and `mark_fill` appears in the widened
      nine-element `visible_contrast_checks`.
    - An **authored** skin whose projected `mark_fill` measures below `3:1`
      **renders unchanged**, asserted by emitting that exact low-contrast colour.
      Without this half the gate would quietly convert PRODUCT-0034's assay into
      the admission gate that record refused.

## Stop condition

Halt and report rather than widening if any of these fire:

1. **The new mark role cannot be resolved through the skin.** §2 adds the first
   non-furniture role to `PlotChrome`; if the skin projection turns out unable to
   supply it — so the only remaining paths are the palette (amending
   PRODUCT-0030 §5) or the legacy catalog (entangling CI-03) — then the boundary
   crossing bought nothing and the colour decision reopens before any spelling
   ships.
2. **A second mark role is wanted immediately.** One unencoded fill is a
   boundary crossing this record can justify on its own exhibit. If §3's outline
   plus a future mark stroke or opacity turn it into a general mark-colour tier,
   that is a chrome-scope record, not an accumulation of one-off roles.
3. **A histogram acquires a colour assignment.** If any path allocates identity
   for this mark, the Context's no-version-change claim fails and the `AuthoredFamily` question
   returns as a real decision rather than an avoided one.
4. **The two-phase split loses its footing at either end.** It rests on the
   returned schema carrying an integer dtype for `count`, and on row ingestion
   retaining an attributable row index. If the returned schema does not preserve
   integer dtype, the schema-wide phase has nothing to read and every count
   check collapses into the per-row phase. If ingestion cannot attribute a row,
   the value phase can refuse but cannot say where — and a refusal that names no
   row is not the diagnostic §5 promised. Either end failing means the split
   needs restating for this mark rather than patching in place.

## Supersession

Supersedes PRODUCT-0078 Stage 3's typed refusal, which existed precisely to be
replaced here. Amends no colour record: PRODUCT-0030 §5's palette-consumer
enumeration is left intact by §2's refusal to join it. Discharges
PRODUCT-0064's resolved-assignment obligation for this mark.

## Rejected

**Keep count-as-colour as an optional modifier.** Attractive because the ramp
already exists and some readers like the density cue. Rejected because it
reintroduces every consequence this record's ordering exists to avoid — palette
or catalog dependence, a gradient legend, and a colour identity that would drag
the persistence schema into the decision — in exchange for re-encoding a variable
already on the axis.

**Fill from palette position 0.** Attractive because the palette is the
language's colour authority and this needs no new role. Rejected because it makes
a mark with no categories a palette consumer, amending PRODUCT-0030 §5 so one
mark can take a colour it never varies, and it invites the reasonable next
question of why position 0 rather than an authored position — a question with no
good answer for a single-series mark.

**Keep the literal outline until a skin defines one.** Attractive as the smaller
change and it preserves the current look on a light canvas. Rejected because
"preserves the current look" is exactly the light-canvas assumption §3 names; a
literal that is invisible under the default dark skin is not a default, it is an
untested branch.

**One-field `histogram(value)` that bins.** Attractive as the spelling every
other tool has and the one an author will try first. Rejected because it
reinstates the second binning authority PRODUCT-0078 removed, one record later,
and the ergonomic gap belongs to the relation surface — `FLOOR` bucketing is now
a verified database capability, and a Rune helper over it is a relation-side
convenience, not a mark.

**Sort the relation to satisfy the substrate.** Attractive because it makes
`histogram` work on any relation. Rejected because a mark that sorts is a mark
that aggregates by a shorter route, and PRODUCT-0056 put ordering in the relation
pipeline for reasons that have not changed.

## Consequences

The histogram becomes the first authorable mark that allocates **no durable
colour identity** — no palette, no catalog, no assignments — and therefore the
first with an **empty durable colour footprint**. `polariton_session` stays at
v2.

Both qualifications are load-bearing. The mark plainly does consume a colour
authority: `mark_fill`, the skin role §2 adds. And its durable footprint is not
empty, because its panel meaning persists like any other panel's — what is empty
is the colour relation. Saying "consumes no colour authority" or "empty durable
footprint" would contradict §2 and gate 6 respectively, and would be the kind of
summary sentence a later reader cites in preference to the sections it
summarizes.

It is also the first mark whose data must be aggregated upstream to exist,
making Toron's `FLOOR` bucketing a visible part of the authoring story rather
than an implementation detail. Authors will meet the sparse-bin contract on
screen the first time a range has no observations.

Two things are handed on. A horizontal histogram has no spelling and no
requester. And the ergonomic gap §5 declines — that authors must write bucketing
SQL before they can draw a distribution — is the strongest available argument for
the relation-side binning helper, which now has a concrete motivating case
instead of a hypothetical one.
