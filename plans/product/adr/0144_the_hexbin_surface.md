# ADR 0144: the hexbin surface

Status: accepted 2026-08-30. PRODUCT-0143 made hexbin geometrically correct and
viewport-responsive; nothing can author one. This record adds the Rune
spelling: `hexbin(x, y)`, a free mark constructor over two quantitative
observation columns, rendered as sequential count colour through the
viewport-prepared substrate. It is the inheritance record 0143's Consequences
promised — constructor, field transport, projection, compatibility,
diagnostics, and live exhibit — and it decides no geometry, no aggregation
ownership, and no colour semantics, because those records already exist.

It supersedes, precisely and only, the surface-absence half of PRODUCT-0143:
gate 26 (no Rune surface), enforced today as five boolean absence assertions
carrying three claims — no registry row, no `MarkFields` variant, no
`SpannedMarkFields` variant — inside `product_0143_keeps_the_old_authority_
and_premature_surfaces_absent` (`architecture_test.rs`). Those three claims
flip from absence to presence under this record's own gates. Every other 0143
assertion — the dead `0.02`, the deleted `prepare_hexbin_data`, the
artifact-only renderer, the closed count guide — survives unchanged.

## Context

### The substrate is complete; the authoring path refuses by construction

`ResolvedKernelFields::Hexbin { x, y }` exists and drives a correct kernel,
but only automatic resolution produces it (`plot/spec/mod.rs:3566-3569`), and
the authored fall-through hard-refuses: any named channel on a plot type
without a resolver arm answers "{plot_type} does not yet expose authored
channel roles" (`plot/spec/mod.rs:2249-2256`). `semantic_is_compatible` has no
`PlotType::Hexbin` arm, so it answers `false` for every channel. The surface
work is therefore additive at every layer: nothing must be repaired, and the
kernel pair (`plot/kernels/hexbin.rs`, `hexbin_viewport.rs`) is not touched.

### The closed template is pie's shape with bubble's channels

Nine mark families already carry typed field structs through the same five
stations: registry row → raw handler → origin injection → spanned envelope →
authored spec → channel resolver. `pie(2 args)` is the arity template: an
eight-slot injected stack (two field strings, two per-argument origin pairs,
one call-origin pair), `PieFieldOrigins { value, category }`, and a
`SpannedMarkFields` variant. `bubble` is the channel template: quantitative x
and y through `resolve_required`, with `(PlotType::Bubble, Channel::X |
Channel::Y) => semantic == SemanticType::Quantitative`
(`plot/spec/mod.rs:3850`). Hexbin composes the two and adds nothing novel.

### Colour applicability is decided twice, and both deciders already exist

PRODUCT-0091 §8 forbids hexbin a colour-job word. The prohibition is enforced
on two independent sides today:

- Rune-side, `mark_diverging` admits only `PlotType::Heatmap | PlotType::
  Bubble` (`rune_presentation.rs:3117`), refusing "diverging colour is
  available on heatmap and bubble marks";
- resolver-side, only the heatmap and bubble resolvers emit `color_job:
  Some(..)`; every other resolver emits `None`.

Planning validation additionally hard-codes `ContinuousScaleIntent::
SEQUENTIAL` for hexbin (`plot/request.rs:180-192`) where heatmap honours
`spec.continuous_intent`. That asymmetry is PRODUCT-0091 §8 in code, not an
inconsistency: hexbin's colour domain is a non-negative count and has no
diverging reading. This record states it so nobody later "fixes" it.

### Three latent panic-or-degradation sites sit on the authored path

`RuneViewIntent::presentation` derives `display_label` from
`primary_plot_type()` in two matches (`rune_relation_envelope.rs:250-271` for
`Single`, `:272-284` for `StyledSingle`), each ending in `unreachable!`. An
unmodified `RuneMark` finalizes to `RunePlotBody::Single`, so the `Single`
match is the live authored path and panics on the first plain hexbin. The
`StyledSingle` match is unreachable from authored hexbin today — every
modifier that sets `mark.modified` refuses hexbin, and `diverging()` refuses
without setting it — so its arm is a defensive completion exercised by
direct construction, not a live path.

`StyledRunePlotSink::into_layer` (`rune_relation_envelope.rs:153`) is the
same class one stage later: its non-layer arm enumerates eleven plot types
returning `Err(self)` — the authored "not a layer mark" refusal — and sends
everything else to `unreachable!`. `layers([hexbin(..)])` therefore panics
today instead of refusing. Hexbin must join the `Err(self)` enumeration.

Finally, `rune_view_compiler.rs`: a constructor absent from the
relation-sugar refusal list at `:837-846` falls through to the generic
relation-method arm and fails with an unattributed runtime error instead of
an authored refusal. All three are enumerated-scope obligations of this
record, not incidental fixes.

### Persistence needs nothing, and that is provable, not assumed

Colour lineage is categorical-only. `ColorLayerIdentity::from_authored_layer`
converts exactly the ten `AuthoredFamilyId` families and `unreachable!`s on
everything else (`plot/spec/mod.rs:1401-1425`); heatmap and bubble live on
that arm today, and `plot/request.rs:848-881` already banks the proof that
planning a hexbin allocates no categorical identity. `DurableLayer::
from_runtime` never sees a `PlotType`. Making hexbin authorable therefore
requires no `AuthoredFamilyId` variant, no `ALL` growth, and no durable-schema
change — the shipped-format hazard PRODUCT-0064 documented does not arise.

## Decision

### 1. One free constructor, two quantitative fields, closed arity

The registry gains one row, in `CALLABLES`' mark block:

```text
callable!("hexbin", Free, 2..=2, Text, Hexbin, FREE, NONE,
    "Construct a hexbin mark.")
```

plus `HandlerId::Hexbin` beside `Heatmap`. `hexbin(x, y)` takes exactly two
field strings naming quantitative observation columns; there is no optional
third argument, no weight channel, no radius, orientation, or origin spelling,
and no colour argument. The registry row's `doc` string is inventory under
PRODUCT-0141, not a comment.

### 2. The handler is pie's decoding shape

`raw_hexbin` mirrors `raw_pie`: eight injected stack slots (2 fields + 2×2
argument origins + 2 call origins), the shared `raw_origin` prologue, a
wrong-count refusal "hexbin(...) takes x and y strings" and a non-string
refusal at the call origin, `decode_span(2)`/`decode_span(4)` for
`HexbinFieldOrigins { x, y }` (the `PieFieldOrigins` shape), and a
`StyledRunePlotSink` over a `DisposableRunePlotSink` with `composition:
PlotComposition::single(PlotType::Hexbin)`, mirrored `x`/`y`, and
`mark_fields: Some(SpannedMarkFields::Hexbin { fields, origins })`.
`HandlerId::Hexbin => raw_hexbin` joins the dispatch; the registration loop
needs no change for a `FREE` route.

The view compiler adds `"hexbin" => Some(&[2])` to the origin-injection arity
table, `"hexbin"` to the relation-sugar refusal list ("hexbin is a sink inside
`.views([...])`; `relation.hexbin(...)` sugar is not available"), and
`HandlerId::Hexbin` to `produced_receiver`'s Mark arm so method-chain
provenance attributes correctly. Hexbin does **not** join `mark_constructor`:
that list is deliberately closed and arity-gated at its one call site, and
both continuous-colour marks (heatmap, bubble) sit in the refusal list today
— hexbin follows their precedent. Histogram and violin do hold sugar, so this
is a per-mark ergonomics judgment, not a category rule; granting hexbin sugar
would be its own decision against that list's history.

### 3. Field transport is the tenth-variant pattern, exhaustively enforced

`HexbinFields { x, y }` joins `plot/spec/mod.rs`; `MarkFields::Hexbin` and
`SpannedMarkFields::Hexbin { fields, origins }` join their enums with the flat
`to_mark_fields` arm and a per-variant accessor. `AuthoredPlotSpec::hexbin`
builds on `Self::xy(PlotComposition::single(PlotType::Hexbin), ..)` and
attaches the fields, with a `hexbin_fields()` accessor. The
`authored_rune_plot_spec` match in `gui/state.rs` gains
`MarkFields::Hexbin(fields) => AuthoredPlotSpec::hexbin(fields)` — the match
is exhaustive, so the compiler carries this obligation.

Static mark-field bindings (PRODUCT-0058) apply to both positions through the
existing lowering path: a bound x or y resolves to its concrete string before
dispatch, and the bound and inline programs produce whole-struct-equal
resolved specs.

### 4. Channel resolution mirrors bubble and refuses temporal by name

`resolve_hexbin_channels` dispatches from an arm beside heatmap's
(`plot_type == PlotType::Hexbin && spec.hexbin_fields().is_some()`), resolves
X and Y through `resolve_required`, and binds:

```text
channels: [x, y]
kernel_fields: ResolvedKernelFields::Hexbin { x, y }
color_job: None
categorical_limit: 0
stack_normalization: Absolute
```

`semantic_is_compatible` gains `(PlotType::Hexbin, Channel::X | Channel::Y)
=> semantic == SemanticType::Quantitative` — bubble's row. Temporal columns
therefore refuse at authored resolution with channel and field attribution,
**before** the substrate's own `TemporalColumn` refusal; the substrate check
remains the backstop, not the author's first contact. Nominal and every other
non-quantitative semantic refuse on the same arm. `default_scale` already
answers `Linear` for quantitative channels; no edit.

The automatic resolver is not widened, narrowed, or touched: it remains
direct-test substrate, and its temporal-with-numeric grouping stays inside
0143 §2's deferral. This record changes what authors can say, not what tests
can construct.

### 5. Colour stays sequential count, gated on both existing sides

`mark_diverging`'s admission list is **unchanged** — hexbin refuses with the
existing "available on heatmap and bubble" text at the modifier's origin.
`resolve_hexbin_channels` emits `color_job: None`. Planning keeps the
hard-coded `ContinuousScaleIntent::SEQUENTIAL`. The sequential-palette
setting applies (it is an `ALL_MARKS` row) and selects the ramp the count
scale samples; that is ramp selection, not a job word, and is the intended
authored control over hexbin colour.

### 6. Both display-label matches gain honest arms

`PlotType::Hexbin => "plot.hexbin"` joins the `Single` `display_label` match
— the live path for authored hexbin — and the `StyledSingle` match as a
defensive completion: no authored modifier can currently produce a styled
hexbin, so that arm is reachable only by direct construction. The composition
description follows `PlotType::Hexbin`'s existing `Display` ("Hexbin Plot").
Label and description agree here — hexbin has no orientation or stacking
resolution — so no 0055-style divergence pin is needed; a plain assertion on
both strings suffices.

### 7. Two failure stations, both channel-attributed, one token

Authored hexbin faults surface at two different stations, and the diagnostic
contract must name both:

- **Resolution-time.** Missing columns, columns without a supported semantic
  type, non-quantitative columns, and temporal columns all refuse inside
  `resolve_hexbin_channels` as `ChannelError`s, before any preparation runs.
  Their spans map from the error's channel attribution onto `origins.x` /
  `origins.y` — the violin attribution-match style, not heatmap's
  `error.field ==` string comparison, so two same-named columns cannot
  mis-attribute.
- **Preparation-time.** Only row-level faults — `NullValue`,
  `NonFiniteValue`, `InexactValue` — can reach `prepare_hexbin_observations`
  through a live authored program, because every column-level
  `HexbinPreparationError` variant (`MissingColumn`, `TemporalColumn`,
  `UnsupportedColumn`) is intercepted by resolution first and remains the
  substrate's backstop. A `hexbin_failure_span` beside `heatmap_failure_span`
  in `gui/state.rs` maps the reachable variants by their carried
  `HexbinChannel` onto the same origins.

Diagnostic preparation runs against the retained result's own
`artifact_token()` — the token planning validated with — never a freshly
minted one, so the observations that produce a diagnostic are the same
publication identity the kernel would consume. The per-mark wiring
(accessor, output-span closure, `.or(..)` chain) follows the established
triple-site pattern.

### 8. Settings applicability is one membership, in one constant

`PlotType::Hexbin` joins `ALL_MARKS` and joins nothing else. The inspector
then shows hexbin exactly heatmap's rows minus `Diverging` and `DivergingAt`:
titles and sizes, palettes, skin, hover size, facet chrome, padding, axis
labels/bounds/ticks, and legend placement — and none of `PointSize`,
`StrokeWidth`, `BarWidth`, `CategoryGap`, `Stacked`, `Percent`. Hexbin has no
orientation variant, so `presentation_family` needs no row. No hexbin-specific
`SettingSpec` is added.

### 9. Persistence is proven inert, not assumed inert

No `AuthoredFamilyId` variant, no `ALL` count change, no schema bump. Authored
hexbin sessions checkpoint and reopen through the existing path because they
allocate no categorical colour identity; the gate extends the banked
`plot/request.rs` no-identity proof from the automatic fixture to the authored
spelling. `ColorLayerIdentity::from_authored_layer`'s final `unreachable!` arm
continues to cover `Hexbin`, deliberately, beside heatmap and bubble.

### 10. The live exhibit pairs a probe seed with a new fixture

`uat/hexbin_probe.sql` seeds an idempotent numeric observation table
(`DROP TABLE IF EXISTS` first) dense enough that at least one cell holds
multiple observations and `max_count < N` — making the reservation-versus-
display distinction observable on screen — plus a deliberately degenerate
companion table carrying a temporal column, so the authored temporal refusal
is observable, following `heat_probe_raw`'s precedent. `uat/test41.rn`
authors the exhibit:

```rune
let points = workspace.table("hexbin_probe");
points.views([
    view("hexbin-density", figure([
        title("Hexbin · viewport-prepared density"),
        hexbin("x", "y"),
    ])),
]);

let stamped = workspace.table("hexbin_probe_temporal");
stamped.views([
    view("hexbin-temporal-refusal", figure([
        title("Hexbin · temporal x refuses"),
        hexbin("stamp", "y"),
    ])),
]);
```

The second program invokes the temporal companion and is the live exhibit of
the authored temporal refusal: the diagnostic lands on the `"stamp"` span,
and the healthy view is unaffected.

`uat/README.md` gains the two-line seed/run block. `test21.rn` is compiled
into the binary and pinned; it is not touched. `crates/polariton/README.md`'s
stale backlog line for hexbin moves to the shipped inventory.

### 11. What this record does not do

No relation-shell sugar (`relation.hexbin(..)` refuses by name, §2). No
`LayerMark` variant — `layers([...])` remains `Line | Scatter`, and deferred-
ledger DB-12 keeps that decision — but the refusal must be authored, not a
panic: `PlotType::Hexbin` joins `StyledRunePlotSink::into_layer`'s
`Err(self)` enumeration beside heatmap and bubble, so `layers([hexbin(..)])`
refuses the way every other non-layer mark refuses. No authored radius, orientation, origin,
weight, normalization, or selection polygon — 0143 §12's list stands. No
temporal axes: this surface admits `SemanticType::Quantitative` only, and
temporal hexbin remains the typed-projection follow-on 0143 §2 names. No
colour-job word, per PRODUCT-0091 §8, permanently. No change to
`plot/kernels/hexbin.rs` or `hexbin_viewport.rs`: if the surface cannot land
without touching the substrate pair, that is a halt, not a widening.

## Implementation sequence

**Stage 0 — bank the refusals and panics being replaced.** Bank the
authored-channel fall-through text for a named-channel hexbin spec, the
generic relation-method failure for `relation.hexbin(...)`, and the
`into_layer` `unreachable!` panic for a styled hexbin sink. Negative
controls for the gates that invert them.

**Stage 1 — lowering surface.** Registry row, `HandlerId::Hexbin`,
`raw_hexbin`, dispatch arm, origin-injection arity, sugar-refusal entry,
`produced_receiver` membership. Rewrite and arity-refusal tests. The 0143
negative architecture assertions flip in the same commit as the row that
falsifies them.

**Stage 2 — transport and resolution.** `HexbinFields`, both enum variants,
`AuthoredPlotSpec::hexbin`, the `gui/state.rs` spec arm, both display-label
arms, the `into_layer` `Err(self)` membership, `resolve_hexbin_channels`,
the `semantic_is_compatible` row. Whole-struct bound-versus-inline equality.

**Stage 3 — diagnostics and applicability.** `hexbin_failure_span` and the
triple-site output wiring; `ALL_MARKS` membership; inspector-row and
colour-word gates.

**Stage 4 — proof and exhibit.** Authored persistence no-identity proof,
probe SQL, `test41.rn`, both READMEs, full battery.

## Acceptance gates

1. **The registry row is the pie shape.** `callable!("hexbin", Free, 2..=2,
   Text, Hexbin, FREE, NONE, ..)` exists; duplicate-route and settings-
   projection census tests pass; the PRODUCT-0141 inventory gate is
   untouched.
2. **Rewrite is exact.** `hexbin("x","y")` lowers to
   `__hexbin("x","y", s0,e0, s1,e1, os,oe)` — eight values — mirroring the
   banked heatmap expectation.
3. **Arity and type refuse at the call origin.** Wrong argument count refuses
   "hexbin(...) takes x and y strings"; a non-string field refuses in the
   same class; both carry the call origin.
4. **Relation sugar refuses by name.** `relation.hexbin("x","y")` answers the
   sink-inside-views refusal, not a generic relation-method error.
5. **Quantitative resolves; everything else refuses with spans.** Two numeric
   columns resolve to `ResolvedKernelFields::Hexbin`. A nominal column
   refuses on its channel. `Date`, naive `Datetime`, zoned `Datetime`, and
   `Time` each refuse at authored resolution, in both channels, with the
   refusal span landing on
   the authored field that caused it — the substrate's `TemporalColumn`
   refusal is not the author's first contact.
6. **No colour job, on both sides.** The resolved spec carries `color_job:
   None`; `hexbin(..).diverging()` refuses with the existing heatmap-and-
   bubble text; the `mark_diverging` admission list is asserted unchanged;
   planning retains hard-coded `SEQUENTIAL` for hexbin with the heatmap
   asymmetry stated in the test's reason message.
7. **Labels are honest in both sinks.** A plain authored hexbin (the live
   `Single` path) produces `display_label == "plot.hexbin"` and the
   "Hexbin Plot" description; the `StyledSingle` arm is exercised by a
   directly constructed styled fixture — a defensive completion, since no
   authored modifier can currently produce a styled hexbin. Neither
   `unreachable!` is hit.
8. **Bindings are inert to meaning.** Binding x and y once versus inline
   literals yields whole-struct-equal resolved specs (the 0058 assertion
   shape).
9. **Failure spans are channel-attributed at both stations.** Every
   resolution-time `ChannelError` (missing, unsupported, non-quantitative,
   temporal) and every preparation-time fault reachable through a live
   authored program (`NullValue`, `NonFiniteValue`, `InexactValue`) lands its
   diagnostic on the x span for X-channel faults and the y span for
   Y-channel faults, including when both columns share a name; no string
   comparison against field names decides attribution. The three
   column-level preparation variants are verified as the backstop by direct
   preparer call, not claimed reachable from authored programs. Diagnostic
   preparation is asserted to receive the retained `artifact_token()`.
10. **The inspector shows the derived row set.** Hexbin's inspector rows
    equal heatmap's minus `Diverging`/`DivergingAt`; `PointSize`,
    `StrokeWidth`, `BarWidth`, `CategoryGap`, `Stacked`, and `Percent` are
    absent; axis bounds are present and functional on both axes.
11. **Persistence allocates nothing.** An authored hexbin plan leaves colour
    identity untouched (the banked no-identity proof, extended to the
    authored spelling); `AuthoredFamilyId::ALL` remains ten; a checkpointed
    session containing a hexbin panel reopens.
12. **The 0143 inversions are precise.** The three absence claims (five
    boolean assertions) flip to presence — registry row found, `MarkFields`
    and `SpannedMarkFields` contain the variant — while every other
    assertion in that test (no `0.02`, no `prepare_hexbin_data`, factory
    routes through `prepare_hexbin_observations`) survives verbatim.
13. **The substrate pair is untouched.** `plot/kernels/hexbin.rs` and
    `plot/kernels/hexbin_viewport.rs` are byte-identical to their PRODUCT-0143
    landing.
14. **No layer widening, and no layer panic.** `LayerMark` remains
    `Line | Scatter` with no hexbin variant, and `layers([hexbin("x","y"),
    line(..)])` produces the authored not-a-layer-mark refusal — the
    `Err(self)` path — not an `unreachable!` panic. The pre-repair negative
    control banks the panic.
15. **The exhibit is live.** `hexbin_probe.sql` seeds idempotently;
    `test41.rn` renders a hexbin figure whose densest cell exceeds one and
    whose `max_count < N`; the degenerate companion demonstrates the authored
    temporal refusal; both READMEs carry their blocks.
16. **Full battery.** Polariton unit, architecture, trybuild, Rune compiler,
    GUI, and workspace checks pass with no new ignored or tolerated failure.

## Guardrails and stop conditions

- If the surface cannot land without editing `hexbin.rs` or
  `hexbin_viewport.rs`, halt: the substrate is accepted and its repair
  belongs to a 0143 amendment, not a surface commit.
- If authored temporal refusal cannot be produced without touching the
  automatic resolver's temporal grouping, halt; that grouping is 0143 §2's
  deferred territory.
- If any consumer turns out to depend on the authored-channel fall-through
  refusal text for hexbin, that is its own finding, not a premise update.
- If `relation.hexbin(...)` sugar, a weight channel, a radius spelling, or a
  `LayerMark` variant is wanted, each reopens its named boundary as a
  separate record.
- If the persistence no-identity proof fails — an authored hexbin allocating
  categorical colour identity — halt: that is a defect in this record's
  premise, not something to accommodate.

## Rejected

**`relation.hexbin(x, y)` static-shell sugar.** Mechanically admissible under
`mark_constructor`'s two-or-three-argument rule, but that list is
deliberately closed, and both continuous-colour marks — heatmap and bubble —
sit in the refusal list, which is the precedent hexbin follows. Histogram
and violin holding sugar makes this a per-mark ergonomics judgment, not a
category rule; refusing by name keeps the failure attributed, and joining
the list is a separate record if wanted.

**A weight argument (`hexbin(x, y, w)`).** Weighted counts change the
preparation request and the conservation oracle (0143 §12 names them);
admitting the arity now would reserve grammar for semantics no substrate
defines.

**A colour-job word, or honouring `continuous_intent` in planning.**
PRODUCT-0091 §8 decided this before the surface existed and 0143 gate 27
re-asserted it. Counts are non-negative; a diverging reading has no anchor.

**Admitting temporal columns because the automatic resolver groups them with
numerics.** The automatic path is test substrate; the authored path is the
product. 0143 §2 already names temporal hexbin as a typed-projection
follow-on, and `SemanticType::Quantitative` is the correct authored cut.

**String-matched failure spans (the heatmap pattern).** The substrate's error
type already carries the channel; matching on it is strictly stronger and
survives two columns sharing a name. Heatmap's own migration to
attribution-matching is possible later cleanup, not this record's scope.

**An `AuthoredFamilyId::HEXBIN`.** Colour lineage is categorical; hexbin's
colour is a derived count. Allocating a family would grow a shipped durable
vocabulary for a mark that never consumes it.

## Consequences

The thirteen-kernel restoration list from PRODUCT-0075 closes to two:
candlestick and fill-between remain the only kernels without a Rune spelling,
and neither carries a geometry blocker — they are futures of this record's
five-station pattern, not of 0143's substrate work.

Authors gain density plots over raw observations without writing an
aggregation they cannot know: the first mark in the language whose
preparation is system-synthesized under 0143's narrow exception. The
`hexbin(x, y)` spelling stays honest across the deferred-source future — when
a Toron executor lands under 0143 §11, no authored program changes.

The surface adds the tenth `MarkFields` family and exercises every
exhaustive match that transports one, which is why most of this record's
obligations are compiler-carried. The deliberate exclusions — no job word, no
sugar, no layers, no temporal — are each pinned by a gate, so pressure on any
of them surfaces as a red assertion pointing at the record that owns the
boundary.
