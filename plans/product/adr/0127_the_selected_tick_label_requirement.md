# ADR 0127: the selected tick-label requirement

Status: proposed 2026-08-17. PRODUCT-0114 derived a Cartesian axis band from
*branch-boundary candidates* over the domain rather than from the labels the
axis will actually draw. PRODUCT-0126 then made the left band a sequence, so
that requirement stopped merely reserving space and began placing the y-axis
title. A conservative number that no figure inks now moves visible ink.

This record replaces the candidate set with the **selected label set**: the
labels `TickPlacement` will choose from the same domain, known before layout,
identical to what the draw path emits, and still budgeted by the declared
face-bound advance. No measurement capability is added.

## Context

### The capture

A plain `(0, 100)` linear y axis, headless wgpu at 900x480, default theme,
`Revenue` as the y label. Realized ink columns measured from the render, not
from the operation stream:

| | pre-0126 | post-0126 |
|---|---|---|
| reserved left band | 105 px | 117 px |
| widest tick ink | 12..45 px from plot edge | 12..45 px |
| title ink | 90..104 px | 102..116 px |
| tick-ink-to-title gap | 45 px | 57 px |

The estimate says the tick run reaches 91 px. Its drawn run spans about 34–35
px. **PRODUCT-0126 contributed 12 px of the surplus; the other 46 px predates
it**, and had been buying an over-wide band since PRODUCT-0114. What 0126
changed is not the size of the error but its consequence: a reservation became
a placement.

### The surplus is one phantom label

`formatted_label_candidates` (`common.rs:385`) seeds every domain with the
branch boundaries of `format_label`, including `+/-0.000_999`. Any domain
spanning that value inherits the candidate, and `format_label` renders it
`1.0e-3` -- six characters, 81 px at the default 18 px tick size. A `(0, 100)`
axis draws `0`, `12.5`, ... `100`; its widest real label is four characters.
**The candidate that sizes the band is wider than any label the axis selects.**

This is not estimator conservatism doing its job. Conservatism is a margin over
a real quantity; this is a margin over a label that will never exist.

### Selection was never part of the cycle PRODUCT-0114 broke

PRODUCT-0114 refused to read "the realized tick set" because
`extent -> band -> ticks -> extent` is a real cycle. That refusal was correct
for tick **extents** and unnecessary for tick **values**.

`TickPlacement::positions(range)` (`common.rs:112`) is a pure function of the
domain and the policy: `Count(n)` interpolates the domain, `Step(s)` walks it.
Neither variant reads a pixel, a band, a plot area, or a transform. The tick
values a figure will draw are therefore knowable *before* any layout, with no
fixpoint and no second pass.

The distinction this record turns on, and which PRODUCT-0114 did not draw:
**realized tick extents reopen the cycle; selected tick values never entered
it.**

### Stage 0 proved both contracts

Run 2026-08-17 against the shipped tree; the full survey with citations is the
record's Stage 0 input.

**Every estimator consumer shares its domain, axis type, and placement with the
site that later draws the labels.** The production consumers are exactly two --
`estimated_x_width` at `common.rs:1541` and `estimated_y_width` at `:1595`,
both inside `VerticalGuideBands` holding `settings`. Formatted y-tick labels
are emitted at exactly two sites, `draw_cartesian_axes` (`:2337`) and
`draw_categorical_axes` (`:2531`), both from
`major_tick_positions(settings.y_tick_placement, y_range)`. Domain and axis
type destructure from the *same* `PlotLayout` value the requirement read --
single plots at `:1860`/`:1874`, grids at `grid.rs:811`/`:825` indexing the
same `layouts` vector handed to `across_layouts` (`grid.rs:671`, `:770`). Grid
placement is figure-wide: one `resolved` cloned into every cell helper
(`grid.rs:694`, `:795`). No label is drawn from `minor_tick_positions`; minor
loops stroke only.

**Today's policies are geometry-independent.** `TickPlacement` has exactly
`Count` and `Step`. There is no space-adaptive variant, so no present policy can
reintroduce the cycle.

### Temporal `Step` is refused before layout

PRODUCT-0038 already refuses `x_axis().tick_step(...)` and
`y_axis().tick_step(...)` unless the corresponding resolved scale is
`Linear`. Temporal columns resolve to `ScaleFamily::Time`, so a Rune figure
cannot reach a temporal `Step` placement. The refusal is source-attributed at
the modifier span in `gui/state.rs`.

`TickPlacement::positions_for_axis` still contains a redundant, unused
`UnsupportedAxis` guard. Its direct unit-level result is not evidence of a
production authoring path: the admission layer rejects the form before any
layout or tick generation. The `Step` rows below therefore bank the pure
generator's behavior only; they do not describe renderable Rune figures.

### The banked matrix

Widths in px at the default 18 px tick size. `current` is
`formatted_label_candidates`; `selected` is `positions(domain)` mapped through
`format_label`, under the same declared budget.

| case | current | selected | delta | widest selected label |
|---|---|---|---|---|
| zero-based linear `(0,100)` | 81.0 | 54.0 | -27.0 | `12.5` |
| negative-crossing `(-50,50)` | 94.5 | 67.5 | -27.0 | `-37.5` |
| scientific `(0,5e6)` | 108.0 | 81.0 | -27.0 | `625000` |
| step placement `(0,100)`/25 | 81.0 | 40.5 | -40.5 | `100` |
| percent `(0,1)` | 94.5 | 54.0 | -40.5 | `0.1%` |
| categorical y | 108.0 | unchanged | 0.0 | exact strings already |
| faceted union | 108.0 | 81.0 | -27.0 | max over cells |
| **step over capacity** | 81.0 | **0.0** | -81.0 | no ticks drawn |
| **step, valid empty** `(1,24)`/25 | 27.0 | **0.0** | -27.0 | no ticks drawn |

Every `AxisType` temporal variant, banked separately per §5:

| case | current | selected | delta | widest selected label |
|---|---|---|---|---|
| date `(20000,20010)` | 135.0 | 135.0 | 0.0 | `2024-10-04` |
| date spanning zero | 148.5 | 148.5 | 0.0 | `-0768-02-05` |
| datetime ms | 256.5 | 256.5 | 0.0 | `2023-11-14 22:13:20` |
| datetime ms at epoch | 256.5 | 256.5 | 0.0 | `1970-01-01 00:00:00` |
| datetime us | 256.5 | 256.5 | 0.0 | `1970-01-01 00:00:00` |
| datetime ns | 256.5 | 256.5 | 0.0 | `1970-01-01 00:00:00` |
| time | 162.0 | 162.0 | 0.0 | `00:00:00.000` |

The pure generator under unsupported temporal `Step`, where the equality does
**not** hold (§5):

| case | current | selected | delta | widest selected label |
|---|---|---|---|---|
| date step 3 `(20000,20010)` | 135.0 | 135.0 | 0.0 | `2024-10-05` |
| date step 700k, wide domain | 148.5 | **135.0** | -13.5 | `0053-06-19` |
| datetime ms step, in range | 256.5 | 256.5 | 0.0 | `1970-01-01 00:00:00` |
| datetime ms past chrono range | 283.5 | **270.0** | -13.5 | `300000000000000000.0` |
| time step | 162.0 | 162.0 | 0.0 | `00:00:00.000` |

`selected <= current` in every case. The change never reserves less than the
emitted labels need, so it can clip nothing that renders today.

## Decision

### 1. The Formatted requirement carries its placement and is the selected label set

The carrier changes shape. `AxisBandRequirement::Formatted` becomes

```text
Formatted { domain, axis_type, placement }
```

and resolves its width from the labels the axis will draw:

```text
selected = placement.positions(domain)
width    = max over selected of budget(format_label(value, axis_type))
```

`formatted_label_candidates` is deleted, not narrowed. Branch-boundary sampling
had no consumer other than this requirement, and a narrower sampling would
still be a guess where an exact answer is available.

**The placement is threaded at layout construction, not supplied at estimation
time.** `BandRequirements::from_layout` and `across_layouts` take the resolved
settings and pair each axis with the policy that will select *its* labels --
`x_tick_placement` into the x arm, `y_tick_placement` into the y arm. Three
production call sites (`common.rs:1780`, `:1827`, `:2143`) and five grid sites
(`grid.rs:671`, `:770`, `:940`, `:1748`, `:2840`) already hold resolved
settings at the point they build requirements, so the threading adds an
argument rather than a lookup.

Supplying the placement later, at `estimated_y_width`, would compute the same
number today and is rejected: the guides hold one figure-wide `settings`, so a
per-requirement policy would have to be applied uniformly to a *vector* of
requirements gathered from different layouts. That is correct only while every
cell shares a placement. Carrying the policy beside the domain it selects over
makes the pairing structural, in the house form -- an invalid pairing becomes
unrepresentable rather than merely unproduced. It is redundant today and is the
reason a future per-cell placement cannot silently mis-size a band.

### 2. Selection is pre-layout, and that is a property, not a convenience

The requirement may consult `TickPlacement` precisely because
`positions(domain)` is pure over the domain and the policy. No band, plot area,
transform, or realized geometry may enter the computation of a requirement.
This is PRODUCT-0114's cycle prohibition restated at its true boundary rather
than relaxed.

### 3. An empty selected set is a zero requirement

The rule is over **emptiness**, not over the reason for it. A selection reaches
the draw path empty by two distinct routes, and both emit no major tick labels:

- **Refusal.** The `Step` arm returns `Err(Capacity)` above
  `MAX_GENERATED_TICK_POSITIONS` (`common.rs:127`), and `major_tick_positions`
  resolves it with `unwrap_or_default()` (`common.rs:1712`) -- the single point
  every draw site funnels through. Banked reservation today: 81.0 px for zero
  labels.
- **Valid emptiness.** The `Step` arm also returns `Ok(Vec::new())` when no
  multiple of the step lies inside the domain (`common.rs:124`) -- for example
  step 25 over `(1, 24)`. This is an ordinary success, not an error, and it is
  the more likely of the two in practice. Banked reservation today: 27.0 px for
  zero labels.

An empty selected set therefore yields a **zero** label requirement, and the
band falls to its tick-offset floor. Under PRODUCT-0126's left sequence no tick-
derived clearance is spent either: the geometry follows the rule already written
for a hidden title -- *no reservation, and no clearance, for ink that is never
emitted*.

Keying on `Err(Capacity)` alone would fix the rarer route and leave the common
one, which is how the phantom candidate survived PRODUCT-0114: a rule stated
over a cause rather than over the observable condition.

This is a decision rather than an inherited side effect, because it is a visible
geometry move on figures that render today. The alternative -- keeping a
reservation for labels the draw path provably refuses to emit -- is the exact
defect this record exists to remove, and it would be incoherent to remove it for
`1.0e-3` while retaining it for an empty selection.

### 4. The declared advance remains the only budget

Widths are still `estimate_formatted_label_width` and the PRODUCT-0117 ASCII
classes over the label strings. `PlotBackend` gains no glyph-extent API and no
text measurement of any kind. PRODUCT-0093 §7 is preserved.

Its character changes usefully. Under selected labels the requirement covers
exactly the strings that will be drawn, so **the declared advance becomes the
sole source of conservatism**, named in one place instead of compounding with a
phantom candidate. Measured against the default `Count(8)` capture: `12.5` and
`87.5` ink about 34–35 px against a 54 px budget.

### 5. The Categories arm is unchanged; temporal parity holds under the admitted `Count` policy

`AxisBandRequirement::Categories` already carries the exact drawn strings and is
untouched.

Temporal parity is claimed **per `AxisType` variant, each with its own banked
row, under the only temporal placement Rune admits: `Count`**. Under `Count`, `Date`,
`Datetime(Milliseconds)`, `Datetime(Microseconds)`, `Datetime(Nanoseconds)`, and
`Time` each reach an identical width by both routes, because `Count` includes
both domain endpoints -- the same values the old candidate set was built from.
The negative-year case is the one that could have broken this and does not: a
Date domain spanning `-999_999` yields `"-0768-02-05"` from *both* routes.

The pure `TickPlacement::positions` function can produce a different result for
temporal `Step`: selected values need not include either endpoint, so the
requirement can shrink where the endpoints were the widest labels. The two
banked generator cases do exactly that: a Date domain `(-900_000, 900_000)` at
step 700_000 goes 148.5 -> 135.0, and a `Datetime(Milliseconds)` domain whose
endpoint exceeds chrono's range, so that `format_label` falls back to `{:.1}`,
goes 283.5 -> 270.0. Those are lower-level generator facts, not an exception to
the production parity claim: PRODUCT-0038 refuses temporal `Step` before a
figure can draw either set.

The safety direction is unaffected in every temporal case, because the old
candidate set always contained both domain endpoints while every selected value
lies between them, and the temporal formatters reach their widest output at the
extremes. `selected <= current` therefore still holds, which is what gate 2
protects.

`Percent` is not temporal and does move (94.5 -> 54.0); it appears in the main
matrix.

### 6. Grids take the union of selected sets once

PRODUCT-0048's congruence rule and PRODUCT-0126 §5 are unchanged. The grid
folds the maximum selected width across its layouts once before cells divide.
Because placement is figure-wide, every cell's selection uses the policy its own
labels will use.

### 7. A space-adaptive policy may never feed the band

If a future `TickPlacement` variant selects ticks from available pixels -- a
nice-ticks rule, a thinning rule, an overlap-avoidance rule -- it reintroduces
`extent -> band -> ticks -> extent`. Such a policy **may not** be consulted by a
band requirement. The band must fall back to a conservative pre-layout
requirement over the domain, and the record adding that policy owns the choice
of what that requirement is.

This clause is the load-bearing half of §2. Without it, §1 reads as permission
to consult the tick policy generally, and the next tick feature silently
restores the cycle.

### 8. PRODUCT-0126's sequence is inherited unchanged

The left edge remains a sequence, `Y_AXIS_TITLE_GAP` remains 6.0, and
`YAxisGuideLayout` remains the one authority for both the reservation and the
title placement. This record changes only the number the sequence consumes. The
`(0, 100)` axis returns to an 85 px title offset and a 105 px band because its
requirement shrinks below `y_label_padding`, not because any rule was relaxed.

## Implementation sequence

**Stage 0 -- contracts and matrix.** Complete, 2026-08-17. Both contracts hold;
the sixteen banked rows and the before/after capture are this record's input.
Re-run rather than trusted if the tree has moved.

**Stage 1 -- the carrier.** Add `placement` to
`AxisBandRequirement::Formatted` and thread resolved settings through
`from_layout` and `across_layouts`. No width changes in this stage; the banked
geometry is byte-identical throughout, which is what makes Stage 2's deltas
attributable.

**Stage 2 -- selected labels.** Replace the Formatted arm's width source,
delete `formatted_label_candidates`, and bank the matrix as assertions. The
empty-selection cases are *not* changed here; they must stay observable as a
separate delta.

**Stage 3 -- the empty-selection zero.** Make an empty selected set a zero
requirement, by both routes, and bank the resulting geometry including the
absent tick-derived clearance.

Two implementation notes, neither a decision. The requirement now allocates a
positions vector and formats one string per tick, where it previously formatted
about nine candidates; the draw path already does exactly this work with the
same inputs, so the cost is a duplicate rather than a new one, but the band is
computed more than once per frame and Stage 2 should confirm that is still
acceptable. And the capacity guard exists only on the `Step` arm -- `Count`
bounds its output by the authored interval count, which production pins at
`TickPlacement::DEFAULT` = `Count(8)` since PRODUCT-0075 removed the control
that authored counts. That is a constrained-production caveat, not a proof: if
`Count` becomes authorable again it needs a capacity decision of its own, and
this record does not make one for it.

## Acceptance gates

1. **The requirement is the emitted label set.** For each Formatted matrix
   case, the label strings the requirement measures equal the `Text` contents
   the draw path emits for that axis, compared against the **operation stream**
   -- never by recomputing the strings from the same call the requirement made.
   A gate that recomputes both sides proves only that one function is
   deterministic.
2. **No case under-reserves.** For every matrix case, the widest emitted tick
   label's budgeted width is `<=` the reserved `tick_end` minus the tick
   offset. This is the gate that would catch a selection narrower than reality;
   it must be evaluated per case, not in aggregate.
3. **The matrix is banked with its direction.** Every row asserts
   `selected <= current`, and each strictly-improving row asserts its exact
   delta. A row that merely "changed" passes nothing.
4. **Each temporal variant is byte-identical under `Count`, and categorical is
   byte-identical outright.** `Date`, `Datetime` at all three `TimeUnit`s,
   `Time`, and the Categories arm each get their **own** banked row and
   operation trace, including the negative-year date. A single Date fixture
   standing for the others fails this gate: the claim is per variant because the
   formatting is per variant.
   The generator test additionally banks temporal `Step`'s non-zero deltas --
   148.5 -> 135.0 for the wide Date domain, 283.5 -> 270.0 past chrono's range
   -- as lower-level behavior only. PRODUCT-0038 refuses those spellings before
   a figure reaches the generator, so they do not qualify the render parity
   claim.
5. **An empty selection yields zero by both routes.** Both a capacity refusal
   and a valid `Ok(empty)` step reserve a zero label requirement, and the band
   equals its tick-offset floor. Each route gets its own case. The gate must
   **first assert the pre-change reservation was nonzero** -- 81.0 px banked for
   the refusal, 27.0 px for the valid-empty step -- so each zero is a
   discriminating result and not an oracle reading zero in both states.
6. **An empty selection spends no tick-derived clearance.** With the y title
   visible and the selection empty by either route, `title_x` falls to
   `y_label_padding` and no clearance is added, matching PRODUCT-0126's
   hidden-title rule. An unconditional gap fails.
7. **The draw path is untouched by the emptiness decision.** The same figure
   emits the same zero tick-label operations before and after. If honoring §3
   required changing what is drawn, the record has exceeded its scope.
8. **No text measurement capability.** `PlotBackend` gains no glyph-extent API;
   the declared advance and ASCII classes remain the only width inputs.
   PRODUCT-0093 §7 holds.
9. **The carrier holds the placement, and requirements consult no geometry.**
   Two structural assertions over the production signatures. `Formatted` carries
   `{ domain, axis_type, placement }`, so a requirement cannot exist without the
   policy that selects its labels -- a variant lacking the field fails. And the
   width computation takes only those three plus a font size, and nothing
   carrying a band, plot area, or transform.
10. **PRODUCT-0126's sequence still sequences.** With a tick run wider than
    `y_label_padding`, `title_x` is still `tick_end + Y_AXIS_TITLE_GAP` and the
    reservation and placement still read one `YAxisGuideLayout`.
11. **The capture returns.** The banked `(0, 100)` figure renders a 105 px band
    with the title at 85 px, and its realized tick ink is unmoved at 12..45 px
    from the plot edge. Ink position is a PRODUCT-0057 backend-gap property and
    is verified against a render, not the operation stream.
12. **Ticks remain tick-relative.** A narrower requirement moves the plot edge
    only. Every tick label keeps its transform-derived origin relative to its
    tick, satisfying PRODUCT-0116 in the form this record strengthens rather
    than weakens.
13. **Grid cells remain congruent.** A faceted fixture with unequal domains
    takes one maximum selected requirement before tiling; cell widths and the
    shared title placement stay equal.

## Stop conditions

> If any estimator consumer is found whose domain, axis type, or placement
> differs from the site that draws its labels, stop. §1 is unsound for that
> consumer and the divergence is the finding.

> If any case's selected set is narrower than the labels actually emitted, stop.
> That is under-reservation, and it clips ink rather than merely tightening a
> band.

> If honoring the empty-selection zero requires the draw path to emit different
> operations, stop. This record changes a reservation, never what is drawn.

> If threading `placement` into the requirement forces a decision about
> per-cell or per-axis placement divergence, stop. Today one resolved settings
> value serves the whole figure; making that untrue is a layout-authority
> decision this record does not own.

> If a tightened requirement needs realized glyph extents to stay safe, stop.
> That is PRODUCT-0093 §7's backend-measurement decision and belongs to its own
> record.

> If a placement policy is proposed that reads available space, stop. §7 binds
> the band, but the policy itself needs a record that decides the cycle.

## Supersession

Supersedes PRODUCT-0114's branch-boundary candidate set for the Formatted arm,
and with it the claim that a band requirement cannot consult tick selection.
That claim was right about tick *extents* and overbroad about tick *values*;
the cycle it protected is preserved by §2 and §7 rather than abandoned.

PRODUCT-0126's left sequence, PRODUCT-0116's estimate invariant, PRODUCT-0093
§7's measurement deferral, PRODUCT-0117's ASCII budget, PRODUCT-0048's grid
congruence, and PRODUCT-0119's clipping policy all remain in force.

## Rejected

**Measure realized glyph extents.** It would give the exact answer and is the
obvious fix. It is also PRODUCT-0093 §7's deferred decision, it reopens the
layout cycle for the compounding N-label class, and this record does not need
it: the selected labels are already exact, and only their widths are budgeted.

**Read the realized tick set at draw time.** Attractive because the draw path
plainly knows its own labels. It inverts the dependency -- the band would then
depend on geometry that depends on the band -- which is precisely the cycle
PRODUCT-0114 broke. Selection is consulted *before* layout or not at all.

**Keep the candidates and shrink `Y_AXIS_TITLE_GAP`.** It treats the visible
symptom on one edge. The band stays over-wide, the x edge keeps the same
surplus, and the gap would be tuned against a fixture rather than derived.

**Sample the domain more finely, or drop only the `0.000_999` boundary.** It
removes the one candidate the capture happened to expose while keeping a
sampling strategy that is wrong in principle. The next domain would find the
next phantom.

**Two-pass layout, or a fixpoint over band and ticks.** It would make realized
extents usable. It is unnecessary given that `positions` is pure, and it buys
that generality with exactly the cycle the arc has twice decided not to have.

**Leave the capacity reservation alone as out of scope.** Tempting, since no
capture exposed it. But it reserves space for ink the draw path provably
refuses to emit -- the same defect as `1.0e-3`, found by the same survey. Fixing
one and inheriting the other would leave the rule stated in a form the
implementation contradicts.

## Consequences

Most numeric axes get narrower left bands and, under PRODUCT-0126, titles that
sit where their labels are. The `(0, 100)` capture is the acceptance exhibit:
117 px back to 105 px, the title back to 85 px, the tick ink unmoved.

The arc's estimator now has one source of conservatism instead of two. Before
this record, a band carried both a declared advance margin and an unbounded
error from sampling labels no axis would select; only the first was ever
written down. After it, the requirement is exact and the budget is the whole
margin -- which is also what makes a future measurement record easy to scope,
should PRODUCT-0093 §7 ever come due.

The durable finding is narrower than "the estimate was too conservative." A
prohibition written for one quantity had been applied to a different one that
merely shared its name. Tick extents and tick values are both "the realized
ticks" in English and are opposites with respect to the cycle. This is the
second time in three records that closing a class required separating two
things the earlier record had reasonably treated as one.
