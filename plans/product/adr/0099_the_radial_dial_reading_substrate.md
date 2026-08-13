# ADR 0099: the radial dial reading substrate

Status: proposed 2026-08-13. Radial dial's preparation fabricates at eight
levels: a failed category filter falls back to the entire frame, a per-category
value is taken by `.get(0)`, nulls and missing columns become `0.0`, an absent
maximum silently becomes `1.0`, a failed `.unique()` and a failed `.sort()` each
fall back to the raw column, an empty relation invents a `"No Data"` dial, and a
missing category column invents `"All Data"`. This record makes preparation
fallible, refuses what it cannot honestly read, and replaces three parallel
vectors with one validated reading collection.

It is step 14 of `plans/wip/plot-catalog-restoration.md` and consumes
prerequisite **E**. A surface record follows.

## Context

### The named defect is the fourth of eight

`radial_dial.rs:189-195`:

```rust
let filtered = filtered_res.unwrap_or_else(|_| df.clone());          // :189
let val = filtered.column(val_col).ok()
    .and_then(|c| c.cast(&DataType::Float64).ok())
    .and_then(|c| c.f64().ok().map(|s| s.get(0).unwrap_or(0.0)))     // :194
    .unwrap_or(0.0);                                                  // :195
```

`.get(0)` is the silent first-row-per-category selection the census named. It is
not the worst line.

**`:189` is.** A *failed filter* falls back to the whole unfiltered frame, so
`.get(0)` then returns whatever row is first overall. Every category can render
one value belonging to none of them — first-row-of-everything wearing a
per-category loop.

`:194`'s `unwrap_or(0.0)` turns a null into a real zero; `:195`'s turns a missing
or uncastable column into one. The maximum repeats all four with `1.0` defaults
(`:198-205`).

### Two more fabrications sit above it

An empty frame or empty value column returns a complete invented figure —
`categories: ["No Data"], values: [0.0], max_values: [1.0]` (`:135-140`). A
missing or unreadable category column invents `"All Data"` (`:143-147`,
`:167-170`), so a mistyped field name renders one confident dial.

### The absent-maximum default contradicts a contract that already exists

`max_val_col.is_empty()` yields `max_v = 1.0` (`:196-197`), silently
reinterpreting every value as a fraction of one.

But the resolver already requires it: `maximum: number(1, "maximum")?`
(`spec/mod.rs:2756`), with `RadialDial { label: Option<String>, value: String,
maximum: String }` (`:1296-1300`) modelling **`label` as the only optional role**.
The empty-string branch is therefore unreachable from resolution and is not
public semantics.

### The prepared shape is funnel's, before PRODUCT-0096

Three parallel `Vec`s — `categories`, `values`, `max_values` (`:123-127`) — with
no structural guarantee of alignment. That is the shape 0096 replaced, and the
same latent misalignment applies.

### Ordering is lexical, and its derivation fabricates twice

`.unique().unwrap_or_else(|_| c.clone()).sort(...).unwrap_or_else(|_| c.clone())`
(`:151-154`). Unlike funnel, radial dial does not preserve relation order — and
**each step silently falls back to the raw column on failure**, so a failed
de-duplication yields a category list with repeats and a failed sort yields
relation order while the mark still claims lexical ordering. These are the
seventh and eighth fabrications, which the census omitted.

## Decision

### 1. Duplicate category rows refuse, equal values included

A category appearing in two rows **refuses**. Radial dial does not aggregate and
does not choose a representative.

The rule is **key uniqueness, not value disagreement**, exactly as PRODUCT-0088
§1 decided for heatmap cells and PRODUCT-0096 §2 for funnel stages: equal-valued
duplicates refuse too, because the rule is about the shape of the relation rather
than the luck of its contents, so the same query succeeds or fails independently
of which rows it happens to return.

Taking the first row is what `.get(0)` does today, and it makes the figure depend
on row order in a way no reader can see.

### 2. The maximum is required

The implicit `1.0` (`:196-197`) is removed. A missing, null, uncastable,
non-finite, or **non-positive** maximum refuses.

This restores what `spec/mod.rs:2756` already requires. A zero or negative
maximum has no arc to scale against, so it is refused with the same standing as
a missing one rather than clamped.

### 3. Values are finite, non-negative, and at most the maximum

`value <= maximum` is a **substrate** rule, not a rendering detail: the dial's
arc is drawn as a fraction of its declared scale, so a value above the maximum
overruns the geometry and asserts a reading the scale does not contain.

Refusing here rather than clamping follows PRODUCT-0096 §4's reasoning — a clamp
prints a full dial that was never measured.

### 4. Lexical category ordering is kept, deliberately

`.unique().sort()` (`:151-154`) stays. Radial dial's categories are a
**categorical domain**, not a sequence: nothing about a dial asserts that one
category precedes another, so deterministic lexical order is a presentation
choice rather than a claim about the data.

**This differs from PRODUCT-0096 §1 on purpose.** A funnel's vertical order *is*
its meaning, so the relation owns it and the mark must not sort. A radial dial's
order carries no meaning, so a stable order beats an inherited one. The two marks
differ because their semantics differ, and stating that here prevents a later
"consistency" pass from aligning them.

Authored ordering is a later feature with its own trigger, not an omission.

### 5. Ungrouped mode is representable, not inferred

Ungrouped is a **shape in the types**, not a sentinel value:

```rust
prepare_radial_dial_data(category_field: Option<&str>, ...)
struct RadialDialReading { category: Option<String>, value: f64, maximum: f64 }
```

`category_field: None` — the resolver selected no category channel — is a valid
**ungrouped dial**, and renders only if the relation contains **exactly one row**.
Two or more rows refuse, for §1's reason: there is no key and no rule for
choosing among them.

`category: None` stays **semantic absence** all the way through the substrate.
A presentation label such as `"All Data"` is added by the **surface**, never by
preparation — the substrate must not invent an identity that no column supplied.

**`UngroupedMultipleRows` attributes to the mark origin, not to a field.** Every
other refusal in §7 names a field, and this one cannot: in ungrouped mode there is
no category field to name. Promising "field + row" here would be a diagnostic
contract the shape makes impossible.

**A named-but-missing category column refuses.** Today both cases produce
`"All Data"` (`:143-147`, `:167-170`), making a mistyped field name
indistinguishable from a deliberate ungrouped dial. The distinction is exactly
PRODUCT-0090 §9's and PRODUCT-0095 §2's: an encoding the resolver did not select
may earn a presentation default; a selected encoding that cannot be read is
malformed.

### 6. An empty relation is an empty result

Zero rows with all required columns present yields an **empty collection, no
dial, and no legend**. The `"No Data"` invention (`:135-140`) is removed.

An empty relation **missing a required column still refuses**, as PRODUCT-0092
gate 13 and PRODUCT-0096 §6 both established.

### 7. One validated reading collection

```rust
prepare_radial_dial_data(...) -> Result<RadialDialPreparedData, RadialDialPreparationError>
RadialDialReadings::from_rows(fields, rows) -> Result<RadialDialReadings, RadialDialReadingsError>
```

Two layers for PRODUCT-0090 §5's reason: column and cast faults arise while
producing rows. The preparation error wraps the readings error unchanged, and
`from_rows` takes field names so its errors can name channel, field, and 1-based
row.

`RadialDialReadings` is **opaque**, replacing the three parallel vectors with one
collection of `{ category, value, maximum }` readings — per PRODUCT-0090 §6, with
a trybuild compile-failure case and a valid construction twin.

**The per-category filter loop is deleted.** Rows are read once, in order, and
grouped by construction; there is no second pass that can fail and fall back to
the whole frame (`:189`).

**Refusal precedence**, three phases, one diagnostic per malformed relation:

| phase | order | error | attribution |
|---|---:|---|---|
| preparation | 1 | `category` column named but missing | channel + field |
| preparation | 2 | `category` column unsupported nominal dtype | channel + field |
| preparation | 3 | `value` column missing | channel + field |
| preparation | 4 | `value` column not quantitative | channel + field |
| preparation | 5 | `maximum` column missing | channel + field |
| preparation | 6 | `maximum` column not quantitative | channel + field |
| row-local | 7 | `category` null | field + row |
| row-local | 8 | `value` null, non-finite, inexact, or negative | field + row |
| row-local | 9 | `maximum` null, non-finite, inexact, or non-positive | field + row |
| row-local | 10 | `value` exceeds `maximum` | the **`value`** field + row |
| cross-row | 11 | duplicate category | the **category**, and both rows |
| cross-row | 12 | ungrouped mode with more than one row | the **mark origin** and the second row |

Preparation is **field-first** — all of `category`'s faults, then `value`'s, then
`maximum`'s — per PRODUCT-0096 §9's reasoning, so the reported column does not
depend on which fault kind exists elsewhere.

**The row-local phase is row-first, then field-ordered within a row:**
`category` → `value` → `maximum`, with `category` **skipped entirely in ungrouped
mode**. A row whose value and maximum both fail therefore reports the **value** —
an earlier draft grouped them as "null value or maximum" and decided nothing.
Row 10 is ordered last within the row because it is the only rule that reads two
fields, and it names `value` because the value is what exceeded a maximum the
author declared.

Then the cross-row phase.

Conversion to `f64` uses the shared `exact_f64` (`plot/exact_numeric.rs:9`),
which PRODUCT-0096 §8 extracted from PRODUCT-0082's box-plot implementation —
not a second exactness rule.

### 8. What this record does not do

No Rune word — that is the surface record. No colour or authored family. No
legend. No hover-seam work: PRODUCT-0097 already repaired radial dial's faceted
hover, and this record must not disturb it. No authored ordering (§4). No
aggregation.

Radial dial remains producerless.

## Implementation sequence

**Stage 0 — bank.** Every current radial dial figure, as a **refactor oracle,
not user pixels** — radial dial has no production producer. The six fabricating
fixtures are banked as **negative controls**: their current renders are the red
states this record converts into refusals.

**Stage 1 — the collection.** `RadialDialReadings`, its error, §7's precedence.

**Stage 2 — the boundary.** Planning validates; the factory re-prepares and
`expect`s.

**Stage 3 — the kernel.** Consumes the collection; the filter loop, `.get(0)`,
every `unwrap_or`, and both invented figures are deleted — not guarded.

**Stage 4 — re-bank.** The post-0099 parity bank the surface record consumes.

## Acceptance gates

1. **The per-category filter is gone; rows are zipped once.** No
   `DataFrame::filter`, no per-category loop, and no whole-frame fallback remains
   in preparation; readings are zipped directly from the three columns in relation
   order.

   The assertion is about **those constructs**, not about reading the frame — an
   earlier draft asserted that "no code path reads an unfiltered frame," which
   forbids the replacement, since zipping the columns reads exactly that frame
   once. Structural either way, because `:189`'s fabrication produces a figure in
   which every dial renders a real value belonging to a category it does not name,
   and no value-level gate catches output that looks entirely plausible.
2. **Duplicate categories refuse, equal values included.** Two cases, per §1: a
   category with differing values, and one with identical values. The second is
   the discriminating half — a rule keyed on disagreement passes the first alone.
3. **A missing maximum refuses.** The `1.0` default is gone; a relation without a
   maximum column produces a diagnostic, not a figure scaled to one.
4. **A non-positive maximum refuses.** Zero and negative, two cases, per §2.
5. **A value exceeding its maximum refuses**, naming the row — not clamped to a
   full arc, per §3.
6. **Negative and non-finite values refuse**, each naming its row; **a large
   finite value constructs**, so the gate cannot pass by rejecting everything
   unusual.
7. **Nulls refuse per field, separately.** A null `category`, a null `value`, and
   a null `maximum` each refuse naming their own field — three cases, since one
   combined case passes an implementation that checks a single column.
8. **Non-finite maxima refuse.** `NaN`, `INFINITY`, `NEG_INFINITY` in `maximum`,
   matching gate 6's coverage of `value`. The maximum was the field with an
   implicit default, so it is the one most likely to keep a lenient path.
9. **Missing and incompatible columns refuse, per field.** Six cases: each of
   `category`, `value`, `maximum` absent, and each present with an incompatible
   dtype.
10. **Category dtype admission is a matrix.** `String`, `Categorical`/`Enum`, and
    `Boolean` each construct; a **numeric** category refuses. Four cases, so the
    refusal cannot be satisfied by rejecting every non-`String` column.
11. **Conversion is exact and shared, on both quantitative fields.** PRODUCT-0082's trio — `2^53` accepted,
   `2^53 + 1` refused, `2^53 + 2` accepted — routed through `exact_f64`, with no
   second exactness rule.
12. **Category ordering stays lexical.** A relation whose categories arrive in
   non-alphabetical order renders alphabetically, and the order is stable across
   runs. Per §4, and asserted so a later consistency pass with funnel fails here.
13. **Ungrouped mode requires exactly one row.** `label: None` with one row
   renders a single dial labelled `"All Data"`; with two rows it **refuses**,
   naming the second. Paired, since the label alone would pass a one-row gate.
14. **A named-but-missing category column refuses.** It does **not** fall back to
    `"All Data"` — the discriminating pair with gate 9, since today both produce
    the same figure.
15. **Emptiness never short-circuits schema validation.** An empty relation with
    every required field present yields no dial, no legend, and no `"No Data"`
    invention. An empty relation missing **each** required field refuses in turn —
    `category`, `value`, and `maximum` in grouped mode; `value` and `maximum` in
    ungrouped. Five cases, because "missing a column" as a single case leaves an
    early-return implementation free to validate one field and skip the rest.
16. **Precedence is deterministic across all three phases.** Four pairs: a missing
    column with a null row reports the column; a missing `category` with a
    non-quantitative `value` reports **`category`** (field-first); a row with a
    null value and an excessive value reports the **null**; a relation with a
    duplicate and an out-of-range value reports the **value** (row-local before
    cross-row).
17. **The collection cannot be built by literal.** Trybuild compile-failure case
    paired with a valid `from_rows` twin.
18. **The parallel vectors are gone.** `categories`, `values`, and `max_values`
    no longer exist as independent fields; readings are one collection.
    Structural, per §7.
19. **Valid figures are unmoved.** Every banked well-formed radial dial is
    byte-identical to Stage 0.
20. **Faceted hover still works.** PRODUCT-0097's non-origin gates still pass —
    this record changes preparation, not hit geometry.
21. **All eight fabrications are gone, not guarded.** `unwrap_or`, `.get(0)`,
    `unwrap_or_else(|_| df.clone())`, **`.unique().unwrap_or_else(...)`**,
    **`.sort(...).unwrap_or_else(...)`**, `"No Data"`, and `"All Data"` do not
    appear in preparation — the last only because §5 moves it to the surface. The
    two `unwrap_or_else` fallbacks on `.unique()` and `.sort()` (`:151-154`) are
    named explicitly because the original census omitted them. Structural, since a
    guarded fallback passes every value-level gate above.

## Stop conditions

> If removing the per-category filter loop changes any valid figure's values,
> stop. §7 replaces a repeated filter with a single ordered read; a value change
> means the two disagree on some relation, and that disagreement is a finding to
> record before it is erased.

> If refusing `value > maximum` breaks a shipped fixture that is not demonstrably
> malformed, stop. That fixture is evidence the mark is being read as an
> unbounded gauge, which is a surface question about what a radial dial means.

> If the ungrouped single-row rule cannot distinguish `label: None` from a
> named-but-missing column, stop. §5's whole content is that distinction, and
> without it the record has reproduced the defect it removes.

> If PRODUCT-0097's non-origin hover gates fail after this record, stop. Hit
> geometry is not this record's subject, and a hover change means preparation and
> geometry are coupled somewhere neither record accounted for.

## Supersession

Supersedes no record. Extends the fabricated-value class PRODUCT-0088 §3 named,
at its fourth site, and the silent-row-selection class the census attributed to
PRODUCT-0078. Adopts PRODUCT-0090 §5's two-layer shape and §6's opacity,
PRODUCT-0088 §1's key-uniqueness reasoning, PRODUCT-0096 §9's field-first
precedence, and the shared exactness authority at `plot/exact_numeric.rs:9`. Consumes prerequisite **E**.
Preserves PRODUCT-0097's hover repair untouched.

## Rejected

**Aggregate duplicate category rows, or take a documented representative.**
Attractive because real relations contain repeats and refusing is inconvenient.
Rejected because marks never aggregate — 0056, 0064 §8, 0069 §2, 0072, 0078,
0081, 0088, 0096 — and a documented "first row" rule is the current defect with a
comment attached.

**Keep an implicit maximum of `1.0` when no column is bound.** Attractive because
it makes a two-field radial dial possible. Rejected because `spec/mod.rs:2756`
already requires the maximum, so the branch is unreachable from resolution — and
because reinterpreting counts as fractions of one is a semantic decision
disguised as a fallback.

**Clamp values above the maximum instead of refusing.** Attractive because it
always renders. Rejected because a clamped dial reads as a full one, asserting a
measurement at the scale's limit that was never taken.

**Preserve relation order for categories, as funnel does.** Attractive for
consistency across two marks landing in the same arc. Rejected because their
semantics differ: a funnel's order is its meaning, a radial dial's is
presentation. Consistency between marks that mean different things is the wrong
axis to optimize.

**Keep `"No Data"`, and `"All Data"` as a fallback for an unreadable category
column.** Attractive because a blank panel looks broken and a labelled dial looks
finished. Rejected because `"No Data"` asserts a reading that was never measured,
and `"All Data"` on a *missing* column makes a mistyped field name
indistinguishable from a deliberate ungrouped dial.

Note this rejects the **fallback**, not the label: §5 keeps `"All Data"` for
genuine ungrouped mode, added by the surface over a `category: None` the
substrate carried honestly. The distinction is which layer invents it and on what
evidence.

## Consequences

Radial dial preparation becomes fallible, and six fabrications become
diagnostics. The one that matters most is invisible in every current figure: a
failed filter substituting the whole frame means each dial can display a value
belonging to no category it names, and nothing in the rendering says so.

Three parallel vectors become one collection, removing the alignment hazard 0096
removed for funnel. **Radial dial is not the last mark carrying that shape** — an
earlier draft claimed it was. Pie (`pie.rs:113`) and radar, with five parallel
vectors (`radar.rs:185-189`), still carry them, as do parallel coordinates and
violin.

Prerequisite **E** is consumed **for its three listed marks** — radial dial,
funnel, and heatmap — which now answer the cardinality question identically.

What the remaining marks inherit is narrower than the same uniqueness rule, and
saying so matters: pie aggregates by design and radar is a matrix, so neither has
a one-row-per-key contract to adopt. They inherit the **broader** rule — that a
mark does not silently select a representative row or silently aggregate — and
each must still decide what its own cardinality means.

Two marks in one arc deliberately disagree about ordering. §4 records why, so
that a future pass aligning them has to argue with the reason rather than notice
the difference.
