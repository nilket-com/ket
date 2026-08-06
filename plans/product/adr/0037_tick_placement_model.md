# ADR 0037: the tick placement model

Status: accepted 2026-08-06. This record decides one thing: that major tick
positions come from a closed placement model through a single generator, and that
the model admits an authored step. It adds **no Rune surface**; the
`x_tick_step`/`y_tick_step` spelling is PRODUCT-0038, which depends on this
record.

## Context

Tick spacing today is a count, and the count is not what an author holds fixed.
`PlotSettingsLayer` carries `x_ticks`/`y_ticks` inside `Override<…> layer.guides`
(`theme.rs:335-342`), defaulting to `8` (`common.rs:322`). The renderer divides
the resolved range into that many **intervals**, so eight yields nine positions:
over `y_bounds(0, 7)` the step is `7/8 = 0.875` and the labels read
0, 0.9, 1.8, 2.6, 3.5, 4.4, 5.2, 6.1, 7.

Count is fragile against the edit an author makes next. Changing the domain to
`0..10` turns a count of 7 into a step of 1.43, while a step of 1 stays 1. The
invariant an author holds is the gap; the count is derived from it.

Two substrate facts make this a record rather than a spelling.

**`y_ticks` means intervals, not ticks.** Exposing that name would bake an
ambiguity into the surface permanently.

**There is no generator.** Positions are recomputed inline at **sixteen** loops
in `common.rs` — nine major loops of the form `for i in 0..=ticks { let t = i as
f64 / ticks as f64 }` and seven minor loops of the form `for i in 0..ticks`, at
lines 998, 1020, 1042, 1058, 1109, 1131, 1150, 1178, 1248, 1267, 1308, 1407,
1430, 1475, 1636, and 1658. Labels, major ticks, major gridlines, and minor
gridlines each derive their own positions. That is harmless while every position
is the same uniform fraction of the domain and becomes a correctness problem the
moment placement can be something else.

## Decision

### 1. Placement is a closed choice

```text
TickPlacement = Count(NonZeroU32) | Step(f64)
```

`Step` carries a finite, strictly positive value. Zero, negative, and non-finite
steps are unrepresentable after construction, following the house rule of
PRODUCT-0013's `NonEmpty` and `LayerMark`: an invalid state excluded by the
representation is not deferred to a check.

The set is closed. Nice-number rounding, log placement, and data-driven
placement are not variants of it and are not authorized here.

### 2. `Count(8)` remains the default, and the parity oracle

Absent any authored placement, every axis resolves to `Count(8)` and renders
byte-identically to today. The existing output is the baseline; a recorded
position oracle over all sixteen sites is banked before any refactor.

This record therefore changes no pixel until PRODUCT-0038 gives an author a way
to select `Step`.

### 3. `Step` is a zero-anchored lattice

```text
positions(step, min, max) = { k · step : k ∈ ℤ, min ≤ k · step ≤ max }
```

Domain endpoints are not required to be ticks.

```text
0.0 .. 7.0,  step 1  ->  0, 1, 2, 3, 4, 5, 6, 7
1.4 .. 8.6,  step 1  ->  2, 3, 4, 5, 6, 7, 8
0.2 .. 7.2,  step 1  ->  1, 2, 3, 4, 5, 6, 7
-3.5 .. 2.5, step 1  ->  -3, -2, -1, 0, 1, 2
```

Zero is chosen over the domain minimum for a reason beyond appearance:
**a zero-anchored lattice does not move when the domain moves.** Widening
`0..7` to `0..10` keeps every existing tick and adds three; anchoring at the
minimum would shift all of them on every bounds edit. That is the same stability
argument that made a palette *position* the identity in PRODUCT-0029 §1 rather
than a color.

A `Step` placement is defined for every resolved domain, bounded or data-derived.
It does not require authored bounds, and requiring them would refuse a defined
request — the posture PRODUCT-0034 through PRODUCT-0036 removed.

### 4. One generator; no site derives positions

A single position generator answers, for one axis, one resolved domain, and one
`TickPlacement`, the ordered major positions. Every one of the sixteen sites
consumes it. **No call site may compute a tick position.**

This is the substrate's whole value. Four consumer classes currently agree only
because sixteen independent call sites each compute the same fraction; under
`Step` they would agree only by coincidence, and the first disagreement would be
a label sitting where no gridline is drawn.

### 5. Minor ticks subdivide the major lattice

Minor ticks subdivide each **major interval produced by the generator**, not a
fraction of the domain.

Under `Count` this is identical to today's behavior. Under `Step` it is the only
definition that keeps labels, major ticks, major gridlines, and minor gridlines
consistent, because the major interval is no longer a fixed fraction of the
range.

Authored minor spacing is not part of this record.

### 6. Capacity bounds work, not aesthetics

A `Step` small enough relative to its domain generates an unbounded number of
positions: `1e-9` over a domain of 7 is seven billion. That does not render
badly; it does not render.

An axis therefore declares a maximum generated position count, and exceeding it
is an attributed refusal naming the axis, the step, the resolved domain, and the
limit. No truncation, no silent dropping, no partial lattice.

This does not reopen PRODUCT-0036 §6, and the distinction is worth stating
exactly:

> A value threshold gatekeeps what an author may look at. A capacity limit bounds
> work that would otherwise not terminate.

PRODUCT-0036 prohibits an ε on the *span* because `1e-300` bounds are extreme but
constant-cost. A dense step is different in kind, because the cost is
proportional to a number the author chose. The shape is PRODUCT-0007 §8's, which
already refuses at declared categorical capacity with an attributed
`DomainError` rather than inventing identities.

### 7. `Step` applies to linear numeric axes

`Count` continues to apply everywhere it applies today.

`Step` refuses on:

- a categorical or band axis, which has no numeric lattice;
- a temporal axis — `AxisType::Date`, `Datetime`, and `Time` — because a step of
  `1` has no defined unit there, and choosing one is a temporal-scale decision;
- any axis whose scale is not linear numeric.

Each refusal is attributed and never silent, per PRODUCT-0036 §7: a placement
that cannot apply must say so rather than disappear.

### 8. No Rune surface

Nothing here is reachable from a buffer. `TickPlacement::Step` has no production
caller until PRODUCT-0038, which is the fourth substrate cut in this arc to ship
test-only — after PRODUCT-0013, PRODUCT-0024, and PRODUCT-0028 — and the cost is
the same accepted one: the surface is judged on lowering, this record on
placement.

## Acceptance gates

1. With `Count(8)` everywhere, a recorded position oracle over all sixteen sites
   is byte-identical to a pre-refactor baseline banked before any change.
2. No call site computes a tick position; every one consumes the generator.
   Asserted structurally, not by inspection.
3. `Step(1.0)` yields exactly the four lattices in §3, including the negative
   domain and the two cases whose endpoints are not ticks.
4. Widening a domain from `0..7` to `0..10` under `Step(1.0)` preserves every
   existing position and adds only new ones.
5. Minor ticks subdivide the generator's major intervals under both placements;
   labels, major ticks, major gridlines, and minor gridlines observe identical
   major positions in a single assay.
6. A step exceeding declared capacity refuses with an attributed diagnostic
   naming axis, step, domain, and limit. No truncated lattice is produced and no
   unbounded loop runs.
7. `Step` refuses on categorical, band, and each temporal axis type, attributed
   and never silently ignored.
8. Zero, negative, and non-finite steps are unrepresentable after construction.
9. No Rune grammar, kernel geometry, cascade algebra, mark, palette, skin,
   bounds, identity, placement, or lifecycle behavior changes.

## Rejected

- **Exposing the count as the authored knob:** `y_ticks` means intervals, so the
  name is already ambiguous, and a count silently changes meaning whenever the
  domain changes. The author holds the gap fixed, not the number of gaps.
- **Anchoring the lattice at the domain minimum:** every tick moves on every
  bounds edit, and the positions become a function of the data rather than of the
  step.
- **Nice-number rounding:** invents a step the author did not request. It belongs
  to a later record about automatic placement, where the author has asked for
  automatic behavior.
- **A minimum-step threshold:** PRODUCT-0036 §6 prohibits an ε gatekeeping a
  defined value. §6 bounds generated count instead, which is a different quantity
  and a different justification.
- **Truncating or thinning a dense lattice:** renders something other than what
  was requested, without saying so — the one failure the rope rule does not
  cover.
- **Deriving minor ticks from a domain fraction under `Step`:** labels and grids
  would disagree, which is the exact defect §4 exists to prevent.
- **A step on temporal axes:** `1` means nothing without a unit, and choosing
  days, seconds, or milliseconds is a scale decision this record does not make.

## Consequences and sequencing

- Sixteen independent derivations become one generator, and four consumer
  classes that agreed by coincidence begin agreeing by construction.
- `Count` survives as the compatibility default rather than as a second authored
  concept, so today's rendering is preserved without exposing an ambiguous name.
- PRODUCT-0038 becomes small: two figure elements and a lowering onto a proved
  placement model, with the anchor, capacity, and applicability rules already
  decided here.
- Authored minor spacing, nice-number automatic placement, log axes, and temporal
  steps each remain unaddressed and each extends this closed model rather than
  replacing it.
