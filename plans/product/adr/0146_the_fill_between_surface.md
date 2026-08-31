# ADR 0146: the fill-between surface

Status: accepted 2026-08-31. PRODUCT-0145 rebuilt the fill-between substrate
— fallible role-attributed preparation, typed temporal x, resolved opaque
colour, honest hits and axes, one kernel where there were two — and nothing
can author one. This record adds the Rune spelling:

```text
fill_between(x, lower, upper)
fill_between(x, lower, upper, middle)
```

a free mark constructor with trailing optionality: the first three roles
keep their positions across both arities, and the fourth adds the middle
series the kernel strokes over the band. It is five-station work over a
substrate with nothing left to repair — constructor, field transport,
projection, compatibility, diagnostics, and live exhibit — and it decides
no preparation, geometry, ordering, colour, or legend semantics, because
PRODUCT-0145 already did.

It supersedes, precisely and only, the surface-absence half of PRODUCT-0145
gate 16, enforced inside
`product_0145_keeps_fill_between_surface_absent_and_resolution_non_reusing`
(`architecture_test.rs:5582`) as four absence assertions: no registry row,
no `ALL_MARKS` membership, no `MarkFields` variant, and no
`AuthoredPlotSpec::fill_between`. Those four flip from absence to presence
under this record's gates; `SpannedMarkFields` presence is not among them
and arrives as new 0146 transport coverage instead. The same test's
non-reuse assertions over automatic resolution survive verbatim, as does
every other 0145 assertion — the validated single authority, the dead ramp
sampling, the retired composite, the surviving scatter-line pairs.

## Context

### The substrate hands this record everything it needs

Preparation already validates in the exact role order this surface spells —
x, lower, upper, trailing optional middle — carries a typed
`X`/`Lower`/`Upper`/`Middle` role on every error precisely so this record
can map faults onto authored spans by attribution, and is wired through
`validate_prepared_contract` on a branch the authored path inherits rather
than adds (0145 §1). The kernel decides band-only versus with-middle by
`Option`, pins band-only axis identity, admits temporal x on typed values,
and declares no legend. The surface is therefore additive at every layer,
and the kernel file is not touched.

### The closed template is bubble's arity with the eleventh-variant transport

`bubble` (4..=5) and `radial_dial` (2..=3) establish variable registry
arity and the handler shape that decodes two stack layouts; `pie` and
`hexbin` establish the field-struct transport. `fill_between` composes
them: `3..=4` arity, a `FillBetweenFields` carrier whose `middle` is
`Option<String>` — trailing optionality chosen by review so the first three
roles never shift between arities — and the five stations 0144 §Context
names. Nothing here is novel; every decision cites its template.

### The known panic-and-degradation sites, checked again

The hexbin arc established that enumerated per-plot-type sites degrade or
panic when a new plot type arrives; four are live for fill-between:

- both `display_label` matches in `RuneViewIntent::presentation` end in
  `unreachable!`, and the `Single` arm is the live authored path — the
  first plain authored fill-between panics without a
  `PlotType::FillBetween => "plot.fill_between"` arm; the `StyledSingle`
  arm is the defensive direct-construction completion, unreachable from
  authored marks for the same reason as hexbin (every modified-setting
  modifier refuses it; `diverging()` refuses without setting the flag);
- `StyledRunePlotSink::into_layer`'s non-layer `Err(self)` enumeration —
  verified after 0145's composite retirement — does **not** list
  `FillBetween`, so `layers([fill_between(..)])` panics today instead of
  refusing; fill-between joins the enumeration beside hexbin;
- even with `into_layer` repaired, `decode_layers`' refusal-label match
  (`rune_presentation.rs:3597`) lacks a `FillBetween` arm, so the layer
  refusal would name the mark only as its generic fallback — the refusal
  must say `fill_between`, requiring
  `PlotType::FillBetween => "fill_between"` in that match;
- absent from the relation-sugar refusal list, `relation.fill_between(..)`
  falls through to the generic relation-method arm and fails without
  attribution.

All four are enumerated obligations of this record with gates.

### The settings cut is hexbin's, for hexbin's reason

Fill-between encodes no categorical or continuous data colour: the band and
stroke are mark chrome from the 0080 tier. It therefore joins `ALL_MARKS`
and nothing else — not `HEATMAP` (no colour-job words, and `mark_diverging`
stays heatmap-and-bubble), not `POINT_MARKS`/`BAR_MARKS`/`STACK_MARKS`, and
deliberately not `STROKE_MARKS`: PRODUCT-0145 §3 pinned the middle stroke
at 2.5 pixels, so granting `StrokeWidth` applicability would create a
setting the kernel cannot honour — the silently-inapplicable class the
doctrine forbids. A stroke-width spelling is a named follow-on that must
amend 0145's pin first.

## Decision

### 1. One free constructor, variable arity, closed roles

The registry gains one row in `CALLABLES`' mark block:

```text
callable!("fill_between", Free, 3..=4, Text, FillBetween, FREE, NONE,
    "Construct a fill-between mark.")
```

plus `HandlerId::FillBetween`. Three arguments author a band; four author a
band with a middle stroke. The first three positions are x, lower, upper in
both arities; the fourth is always middle. There is no other optional
position, no series channel, no colour argument, no stroke-width or
baseline spelling. The `doc` string is PRODUCT-0141 inventory.

### 2. The handler decodes two stack layouts, like bubble

`raw_fill_between` follows `raw_bubble`'s variable-arity shape: eleven VM
argument values for the three-argument form (3 fields + 3×2 injected
argument origins + 2 injected call origins) and fourteen for the
four-argument form — only the origin values are injected. Wrong
count refuses "fill_between(...) takes x, lower, upper, and optional
middle strings" at the call origin; a non-string field refuses in the same
class. Argument origins decode into
`FillBetweenFieldOrigins { x, lower, upper, middle: Option<Range<u32>> }` —
present exactly when the fourth argument is. The sink is a
`StyledRunePlotSink` over a `DisposableRunePlotSink` with
`PlotComposition::single(PlotType::FillBetween)` and
`mark_fields: Some(SpannedMarkFields::FillBetween { fields, origins })`.
`HandlerId::FillBetween => raw_fill_between` joins the dispatch; the
`FREE` route needs no registration-loop change.

The view compiler adds `"fill_between" => Some(&[3, 4])` to the
origin-injection arity table, `"fill_between"` to the relation-sugar
refusal list ("fill_between is a sink inside `.views([...])`;
`relation.fill_between(...)` sugar is not available" — the closed-list,
heatmap-and-bubble precedent, per-mark as 0144 §2 established), and
`HandlerId::FillBetween` to `produced_receiver`'s Mark arm.

### 3. Field transport is the eleventh-variant pattern

`FillBetweenFields { x, lower, upper, middle: Option<String> }` joins
`plot/spec/mod.rs`; `MarkFields::FillBetween` and
`SpannedMarkFields::FillBetween { fields, origins }` join their enums with
the flat `to_mark_fields` arm and the per-variant accessor.
`AuthoredPlotSpec::fill_between` attaches the fields with
`hexbin`-pattern construction and a `fill_between_fields()` accessor; the
exhaustive `authored_rune_plot_spec` match in `gui/state.rs` gains its
arm, compiler-carried.

Static mark-field bindings (PRODUCT-0058) apply to all four positions
through the existing lowering; bound and inline programs produce
whole-struct-equal resolved specs, in both arities.

### 4. Channel resolution honours the substrate's admissions

`resolve_fill_between_channels` dispatches beside the other field-struct
arms and emits:

```text
channels: [x, y]
kernel_fields: ResolvedKernelFields::FillBetween { x, lower, upper, middle }
color_job: None
categorical_limit: 0
stack_normalization: Absolute
```

`semantic_is_compatible` gains fill-between arms mirroring the substrate:
`Channel::X` admits `Quantitative` or `Temporal` — the surface projection
of 0145 §4's typed temporal admission — and `Channel::Y` admits
`Quantitative` only. X and the lower field resolve through
`resolve_required` on those arms; upper and, when present, middle are
hand-checked as quantitative schema columns in the heatmap style, with
their refusals attributed by role so the diagnostic lands on the authored
field (§6). A temporal lower, upper, or middle therefore refuses at
authored resolution with a span, before the substrate's own typed-role
refusal — resolution is the author's first contact, the substrate the
backstop, the 0144 §7 two-station discipline.

The automatic resolver is untouched: it remains test substrate under 0145
§9's no-reuse rule, and this record changes what authors can say, not what
tests construct.

### 5. Colour, layers, and labels stay closed

`mark_diverging`'s admission list is unchanged — `diverging()` on a
fill-between refuses with the existing heatmap-and-bubble text. No colour
job appears in the resolved spec. `PlotType::FillBetween` joins
`into_layer`'s `Err(self)` enumeration so `layers([fill_between(..)])`
produces the authored not-a-layer-mark refusal instead of the current
panic, and `decode_layers`' refusal-label match gains
`PlotType::FillBetween => "fill_between"` so that refusal names the mark
rather than falling to the generic label; `LayerMark` itself remains
`Line | Scatter`, and stacked/percent
modifiers refuse as they do for every non-bar mark.
`PlotType::FillBetween => "plot.fill_between"` joins both `display_label`
matches — `Single` as the live path, `StyledSingle` as the defensive
direct-construction completion. The composition description follows
`PlotType::FillBetween`'s existing `Display`.

### 6. Diagnostics ride the typed roles the substrate built for this

Two stations, both attribution-mapped, no string comparison:

- **Resolution-time** `ChannelError`s — missing, unsupported,
  non-quantitative, temporal — map onto `origins.x`, `origins.lower`,
  `origins.upper`, or `origins.middle` by the role the resolver attributes.
- **Preparation-time** faults arrive as the substrate's typed-role errors.
  Only row-level faults (null, non-finite, inexact) and the structural
  refusals (strict order, band order, containment) can reach preparation
  from a live authored program — column-level variants are intercepted by
  resolution and remain the backstop, verified by direct call. A
  `fill_between_failure_span` maps `X`/`Lower`/`Upper`/`Middle` onto the
  corresponding origins; a `Middle`-role fault can only arise from a
  four-argument authoring, where `origins.middle` is present by
  construction — the mapper still totalises to the call origin rather than
  unwrapping. Structural refusals attribute to the x span for ordering and
  to the offending role's argument span for band order and containment;
  the row number remains in the diagnostic text, as the substrate's error
  variants carry it.

Diagnostic reconstruction needs only the frame and the authored fields —
the substrate's preparation accepts no source token
(`fill_between.rs:431`), and gate 13 forbids changing that. The
retained-token planning contract is already inherited from 0145 §1 and is
not restated as a diagnostic obligation. The triple-site output wiring
follows the established pattern.

### 7. Settings, persistence, and the inspector

`PlotType::FillBetween` joins `ALL_MARKS` and nothing else; its inspector
rows equal hexbin's exactly — the universal set, no colour-job words, no
geometry words — and both quantitative axis-bound members apply. No
`presentation_family` row (no orientation variant). No
`AuthoredFamilyId`, no `ALL` growth, no schema change: the mark allocates
no categorical colour identity, the hexbin rejection is the standing
precedent, and the checkpoint/reopen and no-identity proofs extend to the
authored spelling.

### 8. The live exhibit is the temporal band the substrate earned

`uat/fill_between_probe.sql` seeds, idempotently, a forecast-shaped
relation with a zoned timestamp x and quantitative lower/upper/middle —
exercising the temporal admission that distinguishes this surface from
hexbin's — plus a deliberately degenerate companion whose band crosses
(`lower > upper` at a named row), so the structural refusal and its row
attribution are observable. `uat/test42.rn` authors both arities and the
refusal. Both relations sort explicitly because Toron table scans carry no
row-order contract and PRODUCT-0145 deliberately leaves ordering authored:

**Amended at implementation after live contact.** The first checked-in exhibit
used bare `workspace.table(...)` relations. Its compiler gate passed and its
in-memory crossing fixture passed, but a live Toron scan returned the healthy
forecast rows out of timestamp order; both healthy panels therefore refused
strict ordering, exactly as the substrate requires. The defect was in the
exhibit and in gate 14's evidence, not in temporal projection or the mark. The
exhibit now authors ascending `stamp` and `x` sorts, the source gate pins both,
and a live app drive—not compilation alone—is required to claim the render
half of gate 14. Sorting the crossed companion also ensures row 2 demonstrates
the intended band crossing instead of whichever ordering fault an unordered
scan happens to expose.

```rune
let forecast = workspace
    .table("fill_between_probe")
    .sort([asc(col("stamp"))]);
forecast.views([
    view("band-only", figure([
        title("Fill-between · forecast band"),
        fill_between("stamp", "low", "high"),
    ])),
    view("band-with-middle", figure([
        title("Fill-between · band and expected path"),
        fill_between("stamp", "low", "high", "expected"),
    ])),
]);

let crossed = workspace
    .table("fill_between_probe_crossed")
    .sort([asc(col("x"))]);
crossed.views([
    view("crossed-refusal", figure([
        title("Fill-between · crossed band refuses"),
        fill_between("x", "low", "high"),
    ])),
]);
```

The refusal lands with the substrate's row attribution while both healthy
views render. `uat/README.md` gains the seed/run block;
`crates/polariton/README.md` moves fill-between from backlog to the
shipped inventory. `test21.rn` remains untouched.

### 9. What this record does not do

No relation-shell sugar (refuses by name, §2). No `LayerMark` variant —
the authored refusal in §5 is the boundary's honest voice, and DB-12 keeps
the widening decision. No stroke-width, series, colour, baseline, stacked,
percent, or smoothing spelling — each is a named boundary, and
stroke-width additionally requires amending 0145 §3's pinned width first.
No change to `plot/kernels/fill_between.rs`: the substrate is accepted,
and a surface that cannot land without touching it halts. No temporal
lower/upper/middle, ever, per the substrate. No automatic-resolution
change.

## Implementation sequence

**Stage 0 — bank the refusals and panics being replaced.** The
authored-channel fall-through text for a named-channel fill-between spec,
the generic relation-method failure for `relation.fill_between(...)`, and
the `into_layer` `unreachable!` for a styled fill-between sink.

**Stage 1 — lowering surface.** Registry row, `HandlerId`, `raw_fill_between`
over both stack layouts, dispatch, origin arities, sugar-refusal entry,
`produced_receiver`. The 0145 surface-absence assertions flip in the same
commit as the row that falsifies them.

**Stage 2 — transport and resolution.** Fields and both enum variants,
`AuthoredPlotSpec::fill_between`, the `gui/state.rs` arm, both
display-label arms, the `into_layer` membership, the resolver and both
`semantic_is_compatible` arms. Whole-struct bound-versus-inline equality
in both arities.

**Stage 3 — diagnostics and applicability.** `fill_between_failure_span`
and the triple-site wiring over the typed roles; `ALL_MARKS` membership;
inspector and colour-word gates.

**Stage 4 — proof and exhibit.** Authored persistence proofs, probe SQL,
`test42.rn`, both READMEs, full battery.

## Acceptance gates

1. **The registry row is the variable-arity shape.**
   `callable!("fill_between", Free, 3..=4, Text, FillBetween, FREE, NONE,
   ..)` exists; census tests pass; the PRODUCT-0141 inventory gate is
   untouched.
2. **Both rewrites are exact.** The three-argument call lowers to eleven
   values and the four-argument call to fourteen, each with per-argument
   spans and one call span, mirroring the banked expectations.
3. **Arity and type refuse at the call origin.** Two and five arguments
   each refuse with the stated text; a non-string field refuses in the
   same class.
4. **Relation sugar refuses by name.** `relation.fill_between(..)` answers
   the sink-inside-views refusal, not a generic relation-method error.
5. **Resolution honours the admissions.** Quantitative x and temporal x
   both resolve; nominal x refuses; temporal or non-quantitative lower,
   upper, and middle each refuse at authored resolution with the refusal
   span on the authored field, in both arities — resolution, not the
   substrate, is the author's first contact.
6. **No colour job, no layer panic, and the refusal names the mark.** The
   resolved spec carries `color_job: None`; `diverging()` refuses with the
   existing text and the admission list is asserted unchanged;
   `layers([fill_between(..), line(..)])` produces the authored refusal
   whose text names `fill_between` — not the generic fallback label — and
   the banked panic negative control fails against the repaired
   enumeration.
7. **Labels are honest in both sinks.** A plain authored fill-between (the
   live `Single` path) produces `display_label == "plot.fill_between"`
   and the display description; the `StyledSingle` arm is exercised by a
   directly constructed fixture as the defensive completion.
8. **Bindings are inert to meaning.** Bound versus inline fields yield
   whole-struct-equal resolved specs in the three- and four-argument
   forms.
9. **Failure spans are role-attributed at both stations.** Resolution
   `ChannelError`s and reachable preparation faults (row-level and
   structural) land on the x, lower, upper, or middle span per their
   typed role, including a `Middle` fault landing on the fourth
   argument's span; structural refusals carry their row number in the
   diagnostic text; the mapper totalises rather than unwraps; no
   field-name string comparison decides attribution.
10. **The inspector shows the hexbin row set.** Fill-between's inspector
    rows equal hexbin's; no colour-job, point, stroke, bar, or stack
    words; both axis-bound members are functional.
11. **Persistence allocates nothing.** The authored spelling leaves colour
    identity untouched; `AuthoredFamilyId::ALL` remains ten; a
    checkpointed session containing a fill-between panel reopens.
12. **The 0145 inversions are precise.** The four absence assertions flip
    to presence — registry row found, `ALL_MARKS` membership present,
    `MarkFields` variant present, `AuthoredPlotSpec::fill_between` present
    — while the same test's non-reuse assertions and every other 0145
    assertion survive verbatim. `SpannedMarkFields::FillBetween` presence
    is asserted as new 0146 transport coverage, not as an inversion.
13. **The substrate is untouched.** `plot/kernels/fill_between.rs` is
    byte-identical to its PRODUCT-0145 landing.
14. **The exhibit is live.** The probe seeds idempotently; `test42.rn`
    explicitly sorts the forecast by `stamp` and the crossed companion by `x`,
    renders the band-only and with-middle views over temporal x, and the
    crossed companion refuses at row 2 while the healthy views render; both
    READMEs carry the authored-order requirement. Compilation and an in-memory
    fixture do not discharge the render claim: the checked-in exhibit is driven
    against live Toron data and visually verified in the app.
15. **Full battery.** Polariton unit, architecture, trybuild, Rune
    compiler, GUI, and workspace checks pass with no new ignored or
    tolerated failure.

## Guardrails and stop conditions

- If the surface cannot land without editing
  `plot/kernels/fill_between.rs`, halt: substrate repair belongs to a 0145
  amendment, not a surface commit.
- If a `Middle`-role fault can be produced from a three-argument authoring
  — a path with no middle span to attribute — halt: that contradicts the
  substrate's role contract and is a 0145 finding.
- If stroke-width, series, baseline, or layer admission is wanted, each
  reopens its named boundary as a separate record; stroke-width
  additionally reopens 0145 §3's width pin.
- If the persistence no-identity proof fails for the authored spelling,
  halt: a premise defect, not something to accommodate.

## Rejected

**`fill_between(x, middle, lower, upper)` — middle in second position.**
Rejected at census review: shifting lower and upper between arities would
make the optional argument reorder the required ones. Trailing optionality
keeps the first three roles stable and matches the bubble and radial-dial
shape of "the extra thing comes last."

**Two constructors (`fill_between` and `fill_between_with_middle`).** Two
words for one mark family splits the registry inventory, the diagnostics,
and the exhibit for no expressive gain; variable arity is established and
cheaper everywhere.

**A `.middle("field")` modifier.** Modifiers in this language adjust
presentation of an authored form; a fourth data channel is authored form.
Smuggling a channel through the modifier grammar would be the first of its
kind and would detach the middle's span from the argument origins the
diagnostics rely on.

**Relation-shell sugar.** The per-mark judgment from 0144 §2 transfers
unchanged: the list is closed, heatmap and bubble are the precedent, and
refusing by name keeps failures attributed.

**`STROKE_MARKS` membership.** The kernel's stroke width is pinned by 0145
§3; an applicable-looking setting the renderer ignores is the
silently-dropped class the doctrine forbids. The follow-on that wants
authored stroke width must amend the pin first.

**Temporal lower/upper/middle because temporal x is admitted.** The
substrate refuses them with typed roles, and the y axis is pinned Linear;
a time-valued band is a different mark with its own axis identity
questions.

## Consequences

The PRODUCT-0075 restoration list closes to one: candlestick is the last
kernel without a Rune spelling, and its arc — temporal admission plus the
remaining CI-03-class colour repair — inherits both the 0145 substrate
shape and this record's surface shape as settled precedent.

Authors gain bands: confidence intervals, forecast envelopes, and min-max
ranges over quantitative or temporal x, with an optional expected path,
under the same refusal discipline as every other mark. The eleventh
`MarkFields` family exercises every exhaustive transport match, so most of
this record's obligations are compiler-carried; the deliberate exclusions
are each pinned by a gate that names the record owning its boundary.
