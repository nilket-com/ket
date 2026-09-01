# ADR 0148: the candlestick surface

Status: accepted 2026-09-01. PRODUCT-0147 rebuilt the candlestick
substrate — validated OHLC preparation with typed roles, structural
up/down/doji geometry under one resolved colour, honest widths and
temporal x, sound column hits — and nothing can author one. This record
adds the Rune spelling:

```text
candlestick(x, open, high, low, close)
```

a free mark constructor at fixed arity five, spelled in the one order
every OHLC reader already knows. It is
five-station work over a finished substrate: constructor, field
transport, projection, compatibility, diagnostics, and live exhibit. It
decides no preparation, geometry, direction, colour, width, or hit
semantics, because PRODUCT-0147 already did — and it is the **final
record of the PRODUCT-0075 restoration list**: when it lands, every
kernel that record preserved has its Rune spelling.

It supersedes, precisely and only, the surface-absence half of
PRODUCT-0147 gate 16, enforced inside
`product_0147_repairs_automatic_candlestick_without_granting_a_surface`
(`architecture_test.rs`) as four absence assertions: no registry row, no
`MarkFields` variant, no `AuthoredPlotSpec::candlestick` constructor, and
no `SpannedMarkFields` variant. Those four flip from absence to presence
under this record's gates. The same test's automatic-resolution
assertions — `number(0, "x")`, the by-name x exclusion, the OHLC
positional order, the deleted categorical arm — survive verbatim, as does
every other 0147 assertion. `ALL_MARKS` membership is not among the
flipped claims and arrives as new 0148 coverage.

## Context

### The substrate hands this record everything it needs

Preparation already validates in the exact role order this surface spells
— x, open, high, low, close — carries a typed
`X`/`Open`/`High`/`Low`/`Close` role on every error with structural
attribution pinned (ordering → `X`, lower bound → `Low`, upper bound →
`High`), and is wired through `validate_prepared_contract` on the branch
the authored path inherits (0147 §1). Direction, widths, strokes,
temporal identity, hit semantics, and the no-legend decision are all
settled and gated. The surface is additive at every layer, and
`plot/kernels/candlestick.rs` is not touched.

### The closed template is fixed-arity transport, with box as the width precedent

`pie` and `hexbin` establish fixed-arity field-struct transport;
`fill_between` established the two-station diagnostics over substrate
role enums. Candlestick composes them at arity five: seventeen VM
argument values (five fields, five injected argument-origin pairs, one
injected call-origin pair), a `CandlestickFields` carrier of five
required strings, and the twelfth `MarkFields` variant. Five positionals
are comfortably inside established width: `box(...)` already takes seven
positional strings and lowers to twenty-three VM values
(`presentation_registry.rs:241`, `rune_presentation.rs:818`), so the
arity is precedented, not novel — and OHLC order is a universal
convention besides (§Rejected covers the alternatives).

### The four enumeration sites, checked for the last time

All four per-plot-type sites are live for candlestick, confirmed by the
census: both `display_label` matches end in `unreachable!` with the
`Single` arm as the live authored path; `into_layer`'s `Err(self)`
enumeration lacks `Candlestick`, so `layers([candlestick(..)])` panics
today; `decode_layers`' refusal-label match falls to its generic
`"mark"`; and the relation-sugar refusal list omits the name. The
`StyledSingle` arm remains a defensive direct-construction completion —
no authored modifier produces a styled candlestick, for the same
modifier-admission reasons as hexbin and fill-between. All four are
enumerated obligations with gates.

### The settings cut follows the pinned substrate

Candlestick encodes no data colour — direction is geometry — so it joins
`ALL_MARKS` and nothing else. `BAR_MARKS` is deliberately refused even
though candles look bar-adjacent: PRODUCT-0147 §5 pins the width as a
law over the minimum positive delta with a plot-space single-row
fallback, so granting `BarWidth` or `CategoryGap` applicability would
create settings the kernel cannot honour — the silently-inapplicable
class. `STROKE_MARKS` is refused on the same logic against §3's pinned
wick and outline widths. Any authored width or stroke spelling must
amend 0147's pins first, as its own record.

### The exhibit carries both live-contact lessons from birth

The fill-between arc taught two things this net-new exhibit inherits as
requirements, not lessons to relearn: Toron table scans carry no
row-order contract, so the exhibit **authors its sort**; and an exhibit
gate's render half is discharged only by **driving the live application**
— compilation and fixtures alone do not discharge it (0146 §8's
amendment, now in the gate from day one).

## Decision

### 1. One free constructor, fixed arity five

The registry gains one row in `CALLABLES`' mark block:

```text
callable!("candlestick", Free, 5..=5, Text, Candlestick, FREE, NONE,
    "Construct a candlestick mark.")
```

plus `HandlerId::Candlestick`. Exactly five field strings, in OHLC order
after x. No optional position, no volume argument, no series channel, no
colour, width, or stroke spelling. The `doc` string is PRODUCT-0141
inventory.

### 2. The handler is the fixed-arity shape at width five

`raw_candlestick` follows the pie/hexbin shape: seventeen VM argument
values (5 fields + 5×2 injected argument origins + 2 injected call
origins — only the origin values are injected). Wrong count refuses
"candlestick(...) takes x, open, high, low, and close strings" at the
call origin; a non-string field refuses in the same class. Argument
origins decode into `CandlestickFieldOrigins { x, open, high, low,
close }` — all five required, no `Option`. The sink is a
`StyledRunePlotSink` over a `DisposableRunePlotSink` with
`PlotComposition::single(PlotType::Candlestick)` and
`mark_fields: Some(SpannedMarkFields::Candlestick { fields, origins })`.
`HandlerId::Candlestick => raw_candlestick` joins the dispatch; the
`FREE` route needs no registration-loop change.

The view compiler adds `"candlestick" => Some(&[5])` to the
origin-injection arity table, `"candlestick"` to the relation-sugar
refusal list ("candlestick is a sink inside `.views([...])`;
`relation.candlestick(...)` sugar is not available"), and
`HandlerId::Candlestick` to `produced_receiver`'s Mark arm.

### 3. Field transport is the twelfth-variant pattern

`CandlestickFields { x, open, high, low, close }` joins
`plot/spec/mod.rs`; `MarkFields::Candlestick` and
`SpannedMarkFields::Candlestick { fields, origins }` join their enums
with the flat `to_mark_fields` arm and the per-variant accessor.
`AuthoredPlotSpec::candlestick` attaches the fields with the established
construction and a `candlestick_fields()` accessor; the exhaustive
`authored_rune_plot_spec` match in `gui/state.rs` gains its arm,
compiler-carried. Static mark-field bindings (PRODUCT-0058) apply to all
five positions; bound and inline programs produce whole-struct-equal
resolved specs.

### 4. Channel resolution honours the substrate's admissions

`resolve_candlestick_channels` dispatches beside the other field-struct
arms and emits:

```text
channels: [x, y]
kernel_fields: ResolvedKernelFields::Candlestick { x, open, high, low, close }
color_job: None
categorical_limit: 0
stack_normalization: Absolute
```

`semantic_is_compatible` gains candlestick arms mirroring the substrate:
`Channel::X` admits `Quantitative` or `Temporal` (the surface projection
of 0147's honest-gaps admission), `Channel::Y` admits `Quantitative`
only. X and open resolve through `resolve_required`; high, low, and
close are hand-checked as quantitative schema columns with role
attribution, so a temporal or non-quantitative column in **any** of the
four y-side roles refuses at authored resolution with a span on the
authored field — resolution is the author's first contact, the
substrate's typed refusals the backstop, per the 0144 §7 two-station
discipline. The automatic resolver is untouched: 0147 §9's repair stands
and remains test substrate.

### 5. Colour, layers, and labels stay closed

`mark_diverging`'s admission list is unchanged — `diverging()` on a
candlestick refuses with the existing heatmap-and-bubble text.
`PlotType::Candlestick` joins `into_layer`'s `Err(self)` enumeration so
`layers([candlestick(..)])` produces the authored not-a-layer-mark
refusal instead of the current panic, and `decode_layers`' refusal-label
match gains `PlotType::Candlestick => "candlestick"` so that refusal
names the mark rather than its generic fallback. `LayerMark` remains
`Line | Scatter`; stacked/percent modifiers refuse as for every non-bar
mark. `PlotType::Candlestick => "plot.candlestick"` joins the `Single`
`display_label` match — the live path — and the `StyledSingle` match as
the defensive completion, exercised by direct construction. The
composition description follows the existing `Display`
("Candlestick Plot").

### 6. Diagnostics ride the typed roles at both stations

- **Resolution-time** `ChannelError`s map by attribution:
  `Channel::X` → `origins.x`, `Channel::Y` → `origins.open`,
  `Role("high")` → `origins.high`, `Role("low")` → `origins.low`,
  `Role("close")` → `origins.close`.
- **Preparation-time** faults arrive as the substrate's typed roles —
  `X`/`Open`/`High`/`Low`/`Close` — and map onto the corresponding
  origins, all five present by fixed arity. Row-level faults and the
  structural refusals reach preparation from live authored programs;
  column-level variants are intercepted by resolution and stay the
  backstop, verified by direct call. Structural attribution follows the
  substrate's pins: an ordering fault lands on the x span, a lower-bound
  containment fault on the low span, an upper-bound fault on the high
  span; row numbers remain in the diagnostic text. The mapper totalises
  to the call origin on anything it does not recognise, and no
  field-name string comparison decides attribution.

Diagnostic reconstruction needs only the frame and the authored fields —
`prepare_candlestick_data` accepts no source token, and the
substrate-untouched boundary forbids adding one. The retained-token
planning contract is inherited from 0147 §1, not restated as a
diagnostic obligation. The triple-site output wiring follows the
established pattern; a `candlestick_failure_span` sits beside its
siblings.

### 7. Settings, persistence, and the inspector

`PlotType::Candlestick` joins `ALL_MARKS` and nothing else; its
inspector rows equal hexbin's and fill-between's exactly — the universal
set with both axis-bound members functional, and none of the colour-job,
point, stroke, bar, or stack words. No `presentation_family` row (no
orientation variant). No `AuthoredFamilyId`: direction is a derived
predicate on close versus open, the hexbin rejection transfers, and the
authored spelling extends the no-identity proof 0147 seeded through the
automatic fixture; a checkpointed session containing a candlestick panel
reopens.

### 8. The live exhibit closes the restoration on screen

`uat/candlestick_probe.sql` seeds, idempotently, a zoned-timestamp OHLC
relation whose rows span a weekend gap and include at least one rising,
one falling, and one doji row — so the direction trichotomy, the honest
gap, and the minimum-delta width law are all visible in one figure —
plus a deliberately degenerate companion violating containment at a
named row (`high` below the close), so the structural refusal and its
`High`-span, row-carrying attribution are observable. `uat/test43.rn`
authors both, **with the sort authored** per the fill-between lesson:

```rune
let sessions = workspace
    .table("candlestick_probe")
    .sort([asc(col("stamp"))]);
sessions.views([
    view("ohlc-sessions", figure([
        title("Candlestick · sessions with an honest weekend"),
        candlestick("stamp", "open", "high", "low", "close"),
    ])),
]);

let broken = workspace
    .table("candlestick_probe_torn")
    .sort([asc(col("stamp"))]);
broken.views([
    view("containment-refusal", figure([
        title("Candlestick · torn containment refuses"),
        candlestick("stamp", "open", "high", "low", "close"),
    ])),
]);
```

The refusal lands on the `"high"` span with its row in the text while
the healthy view renders. The render half of the exhibit gate is
discharged only by a live application drive — a screenshot showing
hollow, filled, and doji candles over the gap — never by compilation
alone. `uat/README.md` gains the seed/run block;
`crates/polariton/README.md` gains the candlestick shipped section and
retires the stale "Scatter / Line / Bar plots" backlog line
(`README.md:327`) — the README carries no candlestick backlog entry to
move, only that leftover, and this record removes it while closing the
inventory.

### 9. What this record does not do

No relation-shell sugar (refuses by name, §2). No `LayerMark` variant —
the authored refusal is the boundary's voice; DB-12 keeps the widening.
No volume argument or second y axis, no ordinal session-compression, no
authored width, wick, outline, or gap spelling (each pinned by 0147, each
reopening there), no per-direction colour word, no series channel, no
arity variants. No change to `plot/kernels/candlestick.rs`: the substrate
is accepted, and a surface that cannot land without touching it halts.
No automatic-resolution change.

## Implementation sequence

**Stage 0 — bank the refusals and panics being replaced.** The
authored-channel fall-through text for a named-channel candlestick spec,
the generic relation-method failure for `relation.candlestick(...)`, and
the `into_layer` `unreachable!` for a styled candlestick sink.

**Stage 1 — lowering surface.** Registry row, `HandlerId`,
`raw_candlestick`, dispatch, origin arity, sugar-refusal entry,
`produced_receiver`. The four 0147 absence assertions flip in the same
commit as the row that falsifies them.

**Stage 2 — transport and resolution.** Fields and both enum variants,
`AuthoredPlotSpec::candlestick`, the `gui/state.rs` arm, both
display-label arms, the `into_layer` membership and `decode_layers`
label, the resolver and both `semantic_is_compatible` arms. Whole-struct
bound-versus-inline equality.

**Stage 3 — diagnostics and applicability.** `candlestick_failure_span`
and the triple-site wiring over the typed roles; `ALL_MARKS` membership;
inspector and colour-word gates.

**Stage 4 — proof and exhibit.** Authored persistence proofs, probe SQL,
`test43.rn`, both READMEs, the live application drive, full battery.

## Acceptance gates

1. **The registry row is the fixed-arity shape.**
   `callable!("candlestick", Free, 5..=5, Text, Candlestick, FREE, NONE,
   ..)` exists; census tests pass; the PRODUCT-0141 inventory gate is
   untouched.
2. **The rewrite is exact.** The five-argument call lowers to seventeen
   values with five argument spans and one call span, mirroring the
   banked expectation.
3. **Arity and type refuse at the call origin.** Four and six arguments
   each refuse with the stated text; a non-string field refuses in the
   same class.
4. **Relation sugar refuses by name.** `relation.candlestick(..)`
   answers the sink-inside-views refusal, not a generic relation-method
   error.
5. **Resolution honours the admissions.** Quantitative and temporal x
   both resolve; nominal x refuses; a temporal or non-quantitative
   column in each of open, high, low, and close refuses at authored
   resolution with the span on the authored field — resolution, not the
   substrate, is the author's first contact.
6. **No colour job, no layer panic, and the refusal names the mark.**
   The resolved spec carries `color_job: None`; `diverging()` refuses
   with the existing text and the admission list is asserted unchanged;
   `layers([candlestick(..), line(..)])` produces the authored refusal
   whose text names `candlestick`; the banked panic negative control
   fails against the repaired enumeration.
7. **Labels are honest in both sinks.** A plain authored candlestick
   (the live `Single` path) produces `display_label ==
   "plot.candlestick"` and the "Candlestick Plot" description; the
   `StyledSingle` arm is exercised by a directly constructed defensive
   fixture. Neither `unreachable!` is hit.
8. **Bindings are inert to meaning.** Bound versus inline fields yield
   whole-struct-equal resolved specs across all five positions.
9. **Failure spans are role-attributed at both stations.** Resolution
   `ChannelError`s and reachable preparation faults land on the x, open,
   high, low, or close span per their typed role; structural faults
   follow the substrate's pins — ordering to x, lower bound to low,
   upper bound to high — with rows in the diagnostic text; the mapper
   totalises rather than unwraps; no field-name string comparison
   decides attribution; diagnostic reconstruction takes no token.
10. **The inspector shows the universal row set.** Candlestick's
    inspector rows equal hexbin's; no colour-job, point, stroke, bar, or
    stack words; both axis-bound members are functional.
11. **Persistence allocates nothing.** The authored spelling leaves
    colour identity untouched; `AuthoredFamilyId::ALL` remains ten; a
    checkpointed session containing a candlestick panel reopens.
12. **The 0147 inversions are precise.** The four absence assertions
    flip to presence — registry row found, `MarkFields` variant present,
    `AuthoredPlotSpec::candlestick` present, `SpannedMarkFields` variant
    present — while the same test's automatic-resolution assertions and
    every other 0147 assertion survive verbatim; `ALL_MARKS` membership
    is asserted as new 0148 coverage, not as an inversion.
13. **The substrate is untouched.** `plot/kernels/candlestick.rs` is
    byte-identical to its PRODUCT-0147 landing.
14. **The exhibit is live, and live means driven.** The probe seeds
    idempotently; `test43.rn` renders rising, falling, and doji candles
    over a visible weekend gap through the live application — a
    screenshot, not compilation, discharges the render half — and the
    torn companion refuses on the high span with its row while the
    healthy view renders; both READMEs carry their blocks, the
    candlestick shipped section exists, and the stale
    "Scatter / Line / Bar plots" backlog line is retired.
15. **Full battery.** Polariton unit, architecture, trybuild, Rune
    compiler, GUI, and workspace checks pass with no new ignored or
    tolerated failure.

## Guardrails and stop conditions

- If the surface cannot land without editing
  `plot/kernels/candlestick.rs`, halt: substrate repair belongs to a
  0147 amendment, not a surface commit.
- If a structural preparation fault arrives carrying a role the mapper
  does not recognise, totalise to the call origin and file the finding
  against 0147's attribution pins; do not invent a mapping.
- If the live drive cannot be performed, the exhibit gate stays
  undischarged and the record stays short of accepted evidence —
  compilation is not a substitute, per the 0146 amendment.
- If volume, ordinal compression, authored widths or strokes,
  per-direction colour, or a series channel is wanted, each reopens its
  named boundary as a separate record.
- If the persistence no-identity proof fails for the authored spelling,
  halt: a premise defect, not something to accommodate.

## Rejected

**Variable arity or an optional volume argument.** OHLC is irreducible —
no four-field candle exists — and volume is a second y authority the
layout cannot hold; reserving grammar for it would repeat the mistake
the weight-argument rejection avoided in 0144.

**Name-matching or labelled arguments.** No mark in the language matches
fields by name, and OHLC positional order is a universal convention
older than the terminals it renders on; a labelled form would be the
first of its kind for the narrowest possible gain.

**Relation-shell sugar.** The per-mark judgment from 0144 §2 transfers:
the list is closed, the continuous-and-prepared precedent holds, and
refusing by name keeps failures attributed.

**`BAR_MARKS` or `STROKE_MARKS` membership.** Every width and stroke in
the mark is pinned by 0147; an applicable-looking setting the renderer
ignores is the silently-dropped class the doctrine forbids. The
follow-on that wants authored control amends the pins first.

**A per-direction colour word.** 0147 rejected colour-encoded direction
permanently for the substrate; an authored colour treatment layered on
top would reintroduce through the surface what the substrate deliberately
removed, and belongs — if ever — to its own record against 0147's
reasoning.

## Consequences

The PRODUCT-0075 restoration list closes. Thirteen kernels survived the
dashboard's retirement as substrate; with this record, all thirteen are
authorable — hexbin and fill-between and candlestick through their
substrate-and-surface pairs, everything else through the arcs that came
before — each behind the validation boundary its own records define, and
none without a spelling. The legacy-ramp compatibility fallbacks that
CI-03 deliberately preserved remain in several kernels (radar, parallel,
radial dial, pie, violin, funnel among them); what this arc retired is
narrower and stated exactly: no *newly surfaced* renderer reached
authorship while sampling the ramp — the 0064 obligation was discharged
at each of the three widenings, not across the catalog. The five-station
pattern, the two-station diagnostics, the enumeration-trap checklist,
the live-drive exhibit standard, and the typed-role span mapping are now
settled machinery that the next mark — whenever the language wants one —
inherits whole.

What remains open around the plotting language is recorded elsewhere:
the deferred Toron hexbin executor under 0143 §11, the retroactive
test41 live drive, and the named follow-ons each record in this arc
refused into its own future — ordinal compression, volume, authored
geometry, layer widenings. None of them blocks an author, today, from
saying every plot the product knows how to draw.
