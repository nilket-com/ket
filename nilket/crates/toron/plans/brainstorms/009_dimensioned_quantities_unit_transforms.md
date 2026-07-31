# Brainstorm 009: Dimensioned Quantities & Unit Coordinate Transforms

**Status:** Exploratory / Future Side Quest

Seeded by an operator riff off ADR 151 (second/minute/hour) — "imagine
`cast(x as Mile) as Kilometer` and it just does the math; this is more general
than time, think numpy/sympy units." Captured as a council seed, not a promotion.
Lineage: brainstorm 007 (nominal type identity) → ADRs **087–092** (the nominal
identity spine) → this, which adds a **dimension** axis to that same spine.

## The Premise

A *unit of measure* is a **dimensioned quantity**: a magnitude plus a coordinate
(the unit) within a **dimension** (Length, Mass, Time, Temperature, Volume). The
quantity is the invariant; the unit is the basis. `1 mile = 1.609 km = 63360 in`
are three coordinates of one distance. Converting between them is a **coordinate
transformation on the invariant** — it changes the *number* while preserving the
*quantity*.

The feature is worth building only if it buys **dimensional safety** — the engine
*rejects* `miles → liters` and `miles + fahrenheit` at bind time. Otherwise it's
just multiplying by a constant you could do yourself. The safety is the product.

## The decided fork: CONVERT and CAST stay apart (Representation Purity)

Three-way convergence (Claude + Gemini + operator, the operator landing it with
"I don't hate `CONVERT(CAST(raw AS Mile), Kilometer)` — it's far more
semantically clear"):

- **`CAST` = nominal-identity ascription.** Already what ADR 092 (`sql_logical_cast`)
  + `bind_logical_ascription` (`binder.rs:7196`) do: attach a logical identity to
  a value; reconcile-or-**reject**, never silent-relabel (the
  [[logical_identity_receipt_doctrine]]). `CAST(customer_id AS ProductId)` rejects;
  `CAST(100.0 AS Celsius)` ascribes-from-naked (`LogicalTypeId::NAKED`, `ir.rs:22`).
- **`CONVERT` = value transform within a dimension.** Net-new (grounded: no
  `CONVERT` exists today). `CONVERT(celsius_expr, Fahrenheit)` checks both are
  dimensioned and **share a dimension**, then desugars to arithmetic in the IR.

**Why they must stay apart — the code already decided it.** CAST's invariant is
"value unchanged, identity reconciled." Mile→Km *changes the value*. Overloading
CAST to multiply by 1.609 would break the receipt doctrine *twice*: a silent
relabel **plus** a silent value mutation — exactly the "structural oops" the
nominal spine (087–092) exists to kill. The operator's own first sketch used CAST
for two different things — `cast(x as Mile)` (label) and `as Kilometer`
(convert) — and that overload is the smell. Two operators, two jobs.

## The substrate: extend ADR 090, not invent a new layer

A unit is *almost exactly* today's `LogicalTypeSpec { id, name, physical: DataType,
refinement }` (`ir.rs:34`) — `CustomerId`-over-`Int64` is the same shape as
`Mile`-over-`Float64`. You already have the **type half**. Extend the persisted
`LogicalTypeDefinition` substrate (ADR 090) with two fields:

- `dimension: DimensionId` — the physical quantity (Length, Time, Temperature…).
- a **transform to the dimension's canonical base** — `scale` (and, for affine
  dimensions, `offset`): `Mile → ×1609.344 → metre`.

Then `CONVERT(x, from→to)` = `from_base(to) ∘ to_base(from)`, **legal only within
a dimension**. Cross-dimension is a bind error (`DimensionMismatch`) *before the
query runs*. That single rule is the whole safety story.

## The hard part neither thread has surfaced: affine + point-vs-delta

Gemini's draft models scale + offset and desugars to `Add(Mul(src, a), b)`. That
is **necessary but not sufficient**, and getting it wrong silently corrupts
temperature. Two coupled facts:

1. **Affine, not linear.** Length/Mass/Volume/Time-duration convert by `×k`.
   **Temperature** converts by `×k + b` (F→C = `(F−32)·5/9`). Affine.
2. **Point vs delta — the load-bearing distinction.** In an affine dimension a
   *point* and a *difference* convert by **different rules**:
   - a temperature **point** (an absolute reading) converts **with** the offset;
   - a temperature **delta** (a difference) converts with the **scale only** — a
     change of 9°F = a change of 5°C, but 9°F-as-a-reading ≠ 5°C.
   And the algebra is affine-space algebra: `point − point = delta`,
   `point + delta = point`, `delta + delta = delta`, and **`point + point` is
   meaningless** (100°C + 100°C is not 200°C of anything). So a naive
   `Add(Mul(src,a),b)` applied to a temperature *difference*, or any `+` of two
   absolute temperatures, is **wrong** — and wrong *silently*.

**This is the same structure as the temporal arc.** A `Timestamp` (ADR 150) is an
affine **point** in time; the 151-deferred **`Interval`** scalar is its **delta**.
"Instant vs civil-date" (150/151) and "point vs delta" are the same two-category
model. So *the units system and the temporal arc are one mechanism* — `Interval`
is literally "the delta of the Time dimension," and temperature is another
instance of the same affine point/delta pair. The operator's "more general than
time" intuition is exactly right: **time is one affine dimension; build the
general affine-quantity model and time falls out as an instance.**

**Design consequence:** a quantity needs a **point/delta tag** (or two logical
families per affine dimension), and `CONVERT` must know which it's transforming.
A pragmatic v1: support **linear** dimensions fully (length/mass/volume —
offset-free, `point`≈`delta`, addition always meaningful), and either defer affine
dimensions or admit temperature **point-only** with arithmetic on absolute
temperatures rejected until the point/delta model lands. Don't ship `offset`
without the point/delta tag — that's the trap.

## Operation taxonomy — don't conflate `AT TIME ZONE` with `CONVERT` (Codex)

The "time is one affine dimension" mapping is right at the *type-category* level
(point/delta), but the *operations* must not be mapped 1:1 — there are **three**
distinct ops, and ADR 151's `AT TIME ZONE` is **not** the `CONVERT` analogue:

1. **Re-lens a point** (`AT TIME ZONE`): pick the frame to *read* an affine point
   through. **Canonical coordinate unchanged, zero arithmetic** — 151 preserves
   the UTC epoch ticks and only sets the zone the civil readers use. It's really a
   *frame argument* to EXTRACT/`date_trunc`, not a value transform. (It exists as
   a standalone node only to stamp render metadata.)
2. **Convert a coordinate** (`CONVERT`): recompute the number into a new unit
   within the dimension, preserving the invariant quantity, **carrying the target
   unit identity forward** (so chained `CONVERT` and the self-documenting story
   work — see OQ4; Claude + Codex lean *yes, carry forward*). Mile→Km, 300K→°C.
3. **Extract a civil field** (`EXTRACT`/`date_trunc`): read a scalar out of a
   point through a frame — dimension-*reducing*.

The loop this closes: the **Time dimension's actual `CONVERT` is duration unit
conversion** — `145 minutes → hours`, the operator's *original* ADR-151 spark
that started this whole thread. That op is on the **delta** (a duration), and it's
**linear/offset-free** — which is exactly why `Interval`-as-the-Time-delta is the
cleanest first instance of the general mechanism: it's `CONVERT` on familiar,
offset-free ground, with `AT TIME ZONE` (re-lens) and `EXTRACT` (civil read) kept
visibly separate. Time isn't special because it has `AT TIME ZONE`; it's special
because its canonical coordinate (epoch) is frame-independent, so re-lensing a
*point* is free where for temperature "read 300K in Celsius" is already a
`CONVERT`.

## The live question: implicit unit coercion? — **No.**

Gemini's open question: should `fuel_liters + fuel_gallons` auto-convert (coerce
the "smaller unit to the larger")? **Recommend: reject it, demand explicit
`CONVERT`.** Reasons:

- **"Smaller to larger" is an arbitrary, surprising winner.** Is the result liters
  or gallons? Silently picking one is magic-without-a-receipt — the same
  silent-mutation class the nominal spine kills, one level up (value instead of
  label).
- **Dimensional safety is sharper when it's loud.** `miles + km` → a helpful bind
  error ("same dimension, different units — `CONVERT` one first") teaches the
  user and keeps the result type unambiguous. `miles + fahrenheit` → hard
  `DimensionMismatch`. Both reject; neither guesses.
- It matches what the operator already endorsed: the explicit
  `CONVERT(CAST(...))` form is "far more semantically clear." The slight verbosity
  *is* the honesty.

So the rule set: same-unit `+` ok; **same-dimension different-unit `+` →
bind error demanding explicit `CONVERT`**; cross-dimension `+` → `DimensionMismatch`.
The engine is a *checker*, not a guesser. (`SELECT a + b AS total_liters` becomes
`SELECT CONVERT(a, L) + CONVERT(b, L)` — explicit, unambiguous result unit.)

## Rune two steps ahead

[[rune_is_primary_interface]]. SQL's `CAST(x AS Mile)` overload is the "made-up
bs"; the honest split is Rune-led:

- **Rune leads** with first-class dimensioned values + bind-time dimensional
  checks: `x.miles().to(KM)`, `t.celsius().to(FAHRENHEIT)`,
  `(a.liters() + b.liters())`. Prescriptive, power-user, safe — the dimension/
  point-delta algebra is natural in Rune's value model.
- **SQL gets the honest scrap:** `CONVERT(expr, Unit)` — net-new, **never** a CAST
  overload. `CAST` stays nominal ascription (092).
- Build the **dimension registry + affine transform + point/delta algebra +
  bind-time dimensional check** frontend-agnostically (the `LogicalTypeRegistry`
  extended per ADR 090); both frontends target the same IR desugar. (Frontend-
  agnostic-mechanism bit us on ADR 134 when skipped.)

## Scope spectrum (this is big — name the sizes)

- **Expression-only (Toron-local, Rune-led):** construct/ascribe a unit in an
  expression, `CONVERT` within a dimension, read a number. New: dimension model,
  affine+point/delta transforms, bind dimensional check, `CONVERT` surface. A
  multi-iter ADR, not a one-cut — but no cross-ring work.
- **Schema-carried units (the operator's "stored as f32 in Gallons" examples):**
  columns *declare* a unit in DDL (091 already does logical-type DDL columns) and
  Wendelon schema carries it; `CONVERT` checks against the **stored** unit.
  Cross-ring (Wendelon schema metadata) — a multi-ADR arc, the maximal version.

## Fenced rabbit hole: derived dimensions

`miles / hours = mph` — multiplying/dividing *dimensions* to synthesize new ones
— is full dimensional analysis (the sympy/Pint deep end). **v1 must be conversion
*within fixed dimensions only*, not dimensional arithmetic.** Derived dimensions
are a named, much-later follow-on.

## Smallest grounded first step (if the council walks this way)

The **151-deferred `Interval` scalar, reframed as "the delta half of the Time
dimension."** It's the smallest real instance of the affine point/delta mechanism,
already on the roadmap, on a dimension (time) just built in 150/151, and it forces
the point/delta model on familiar ground *before* committing to the full unit
registry. "Rune two steps ahead" as a grounded step, not a moonshot.

## Strategic placement

This is a **type-system / frontend-breadth** direction — a *different axis* from
the 141→151 smart-storage-pushdown spine ("Toron's reason for existence is
pushdown"). It's legitimate and it rhymes with the temporal arc beautifully, but
it should be a **deliberate council pivot**, not a silent swap for the next CBO
cut. If pursued, the affine point/delta model is the conceptual core to nail
first — likely via the `Interval` step above.

## Open questions for the council

1. **Point/delta encoding:** a tag on the quantity, or two logical families per
   affine dimension? (Affects the IR and how `CONVERT`/`+`/`−` dispatch.)
2. **Where the unit lives:** value-level quantity vs column-level declared unit
   (091/090 lean column-level — lighter at runtime, matches "stored in gallons,"
   but cross-ring for the schema half).
3. **Canonical bases:** SI throughout? How are user-defined units/dimensions
   registered (the durable `LogicalTypeRegistry` path) and validated for a
   consistent base?
4. **`CONVERT` result identity:** does `CONVERT(x, Km)` carry the `Kilometer`
   logical identity forward (so downstream stays dimensioned), or erase to naked
   `Float64`? (Carrying it forward is what makes chained `CONVERT` and the
   self-documenting story work.)
5. **Affine in v1:** temperature point-only (reject absolute-temp arithmetic), or
   full point/delta from the start?
