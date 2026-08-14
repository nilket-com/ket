# ADR 0102: the pie slice substrate

Status: proposed 2026-08-13. Pie aggregates its own input, fabricates or falls
back silently at eleven points, holds four fields where two are derived, paints at a doubled origin, and has
a hover that panics on a one-slice figure and misattributes a two-slice one. This
record moves aggregation to the relation, replaces the four vectors with one
validated collection, and gives paint and hover one geometry formula.

It is step 17 of `plans/wip/plot-catalog-restoration.md` and consumes prerequisite
**C** — which turns out to need a diagnostic and an exhibit, not new engine
capability.

## Context

### Aggregation is a useful feature at the wrong layer

Every prior fabrication in this arc produced something nobody asked for: a null
becoming `0.0`, a first row standing for a category, a chord where a sector was
meant. Pie's `.select([col(val_col).sum()])` (`pie.rs:191`) produces a **correct
number**. The objection cannot be that it is wrong.

The objection is that it is **only sometimes what was meant**. Summation is right
when values are additive and the author intended a total. Count, average,
maximum, and already-aggregated values are equally legitimate pie inputs, and for
every one of them the mark's silent `.sum()` answers a different question than
the one asked. The mark cannot know which, because nothing in the relation says.

So the standing rule that marks never aggregate — 0056, 0064 §8, 0069 §2, 0072,
0078, 0081, 0088, 0096, 0099 — holds here for a reason those records did not
need. In each of them aggregation was **incidental**. In pie it is the
conventional workflow, and the rule costs something real: pie will refuse input
it currently accepts and renders plausibly.

It is still the right answer, because the alternative is a mark that picks one of
five defensible aggregations and never says so.

### Prerequisite C is not a blocker

Toron already provides the workflow:

```text
.aggregate([col("category")], [sum(col("value"))])
```

Both are **production** registrations in the Toron tree:
`aggregate` at `nilket/crates/toron/src/rune_engine.rs:1599`, `sum` at `:1521`,
alongside `project_named`, `distinct`, and `sort`. An earlier draft cited
`rune_relation_envelope.rs:4041`, which is a **test-only vocabulary assay** — the
citation was wrong even though the conclusion was right.

**C therefore needs a diagnostic and an exhibit, not new engine capability.**

**This record can supply the diagnostic but not the exhibit.** Pie is
producerless, so there is no authored pie to demonstrate the workflow with; the
live exhibit is **owed by the surface record**. 0102 gates the expression
end-to-end instead — see gate 2.

### What `.sum()` actually does

- **Grouped input**: one filter-and-sum per category. `[A:2, A:3, B:5]` becomes
  `[A:5, B:5]`, independent of row order.
- **Ungrouped input**: no aggregation at all; each row becomes a slice.

So the same word means two different things depending on whether a category
channel is bound.

**Percentage is not a second aggregation.** `total_sum` is the denominator over
accepted slices, and belongs inside the validated substrate rather than being
recomputed.

### Eleven silent dispositions in preparation

Eleven dispositions, in two kinds.

**Six data fabrications**: empty input invents `"No Data": 1.0`; a missing
category field becomes `Row 1, Row 2, …`; a missing value field becomes zeroes; a
grouped null value **disappears from the sum**; an ungrouped null becomes zero;
and a null category becomes a literal-looking `"null"` slice **and loses its
value**.

**Five failed-operation fallbacks**: uniqueness, sorting, filtering, casting, and
collection each fall back silently.

Separately, numeric casting bypasses the shared exactness authority
(`plot/exact_numeric.rs:9`), and negative, non-finite, and all-zero inputs
produce invalid or structurally short angle vectors.

### Four truths that can disagree

`categories`, `values`, `cumulative_angles`, `total_sum` (`pie.rs:113-116`) —
**four independent fields**, of which `categories` and `values` are primary and
`total_sum` and `cumulative_angles` are **derived from them**. So this is the
parallel-vector shape 0096 and 0099 each removed, plus something neither had: two
derived truths stored beside the data they derive from, free to disagree with it
and with each other.

### Hover is independently broken, with measured red states

- a **one-slice pie panics** on an out-of-bounds category index;
- in a two-equal-slice pie, the half drawn as **A reports B**;
- the hover radius accepts points **outside** the rendered pie;
- it compares normalized `0…1` angles against stored **degrees**, and uses the
  wrong boundary index.

An earlier draft also listed "the painted centre has no hover" as a defect. It is
not one: **the exact centre is not attributable.** Every sector owns that point,
its angle is undefined, and `atan2(0, 0)` would select a slice by accident.
Returning `None` there is correct.

### Paint doubles a non-zero origin

Pie uses `bounds.center()` *after* the backend has already translated to that
bounds origin. Funnel and radial dial paint in local width/height coordinates and
are correct. This is a third coordinate defect in the arc, distinct from
PRODUCT-0097's hover seam and PRODUCT-0101's path contract.

## Decision

### 1. Aggregation moves to the relation

`.select([col(val_col).sum()])` is removed. Pie consumes one row per slice.

The diagnostic for a duplicated category **names the remedy**:
`.aggregate([col(...)], [sum(col(...))])`. PRODUCT-0088 §1 established that a
refusal saying only "duplicate" points nowhere; here the destination exists and
must be printed.

### 2. Category keys are unique; duplicates refuse

Including **equal-valued** duplicates, on PRODUCT-0088 §1's key-uniqueness
grounds: the rule is about the relation's shape, not the luck of its contents, so
the same query succeeds or fails independently of which rows it returns.

**Uniqueness applies only when a category channel is selected.** Ungrouped pie has
no key, so §3a's multiple rows are valid and undergo no duplicate check.

### 3. Ungrouped pie is representable, and its rows are not keys

`category: Option<String>` on `PieSlice`, carried honestly through the collection:

- **category channel selected, category null** → **refuses** (a null *value* is
  §5's, not this section's);
- **category channel not selected** → `None`, semantic absence, and **every row is
  a valid slice** — no key, therefore no duplicate check;
- synthetic `Row {i}` labels, if retained at all, belong at **presentation**, as
  PRODUCT-0099 §5 and PRODUCT-0100 §7 settled for radial dial's `"All Data"`.

An earlier draft left this undecided and its §9 said "both columns present,"
which contradicts an optional category channel.

### 4. Relation order is preserved; the mark does not sort

Pie renders slices in relation order. Lexical sorting inside the mark is removed,
because it **destroys authored ordering** — a clockwise sequence is a design
decision, and Toron's `.sort(...)` is where an author expresses it.

**This is the arc's third ordering answer, and they are not inconsistent.**
Funnel preserves relation order because its vertical sequence is its meaning
(0096 §1). Radial dial sorts lexically because its ring order carries none
(0099 §4). Pie joins funnel: clockwise order is meaningful. The trichotomy is
recorded here so a later consistency pass has to argue with three stated reasons
rather than notice three different behaviours.

### 5. Values are non-null, finite, exact, and non-negative

Each refuses on its own terms, attributed to the offending row. Conversion uses
the shared `exact_f64` (`plot/exact_numeric.rs:9`), which pie currently bypasses.

A null **category** refuses; the literal string `"null"` is a valid category, and
the two must be distinguishable — the discriminating pair PRODUCT-0096 §2
established.

### 6. Individual zeroes are valid; the total is finite and positive

A zero-valued slice is a real measurement. It is **accepted, emits no sector**,
and **remains in the legend** — the category was measured, and omitting it from
the legend would hide a result the relation contains. **Hover never attributes to
it**, because a zero-sweep sector occupies no angle and any point that "hit" it
would belong to a neighbour.

**Emptiness is decided before the total is.** An **empty** collection is accepted
and validated no further — there is no total to judge, and §10's empty result is
not a degenerate figure but the absence of one. Total validation applies to
**every non-empty collection**.

**A non-empty all-zero total refuses**: every angle would be undefined, and the
figure has no geometry rather than a degenerate one.

**A non-finite total refuses**, even when every input is finite. Finite values can
overflow their sum, and a total of `inf` yields no usable denominator — so the
total is validated in its own right rather than inferred from its inputs having
been checked.

**Boundaries are radians**, and the final boundary is **exactly `TAU`**. Pinning
the representation is what §8's two consumers need in order to agree, and the
exact terminal value is what prevents an accumulated rounding gap from leaving a
sliver unattributable at the twelve o'clock seam. Degrees are rejected: the
existing hover already compares normalized values against stored degrees, and one
representation is the repair.

### 7. One `PieSlices` authority owning slices, total, and boundaries

```rust
prepare_pie_data(...) -> Result<PiePreparedData, PiePreparationError>
PieSlices::from_rows(fields, rows) -> Result<PieSlices, PieSlicesError>
```

Opaque, per PRODUCT-0090 §6, with a trybuild compile-failure case and a valid
construction twin. It owns the slices, the total, **and** the boundary angles
**together** — the four truths become one, and the derived quantities cannot
disagree with the values they derive from.

Two layers for PRODUCT-0090 §5's reason, with precedence pinned in four ordered phases:

| phase | order | error |
|---|---:|---|
| preparation | 1 | `category` field missing (when selected) |
| preparation | 2 | `category` field not a supported nominal dtype |
| preparation | 3 | `value` field missing |
| preparation | 4 | `value` field not quantitative |
| row-local | 5 | null `category` |
| row-local | 6 | null, non-finite, inexact, or negative `value` |
| cross-row | 7 | duplicate `category` (grouped only) |
| cross-row | 8 | total non-finite, or zero — **non-empty collections only** |

**Missing before uncastable, within each field; `category` before `value`.**
Preparation is field-first per PRODUCT-0096 §9. Row-local checks run in producer
order, `category` before `value` within a row. Duplicates are cross-row, and the
**total is validated last** — it is derived from every accepted row, so it cannot
be judged until they all are, and an **empty** collection skips it entirely.

`category` admits the supported nominal dtypes and refuses numeric; `value`
admits the quantitative dtypes. Conversion is `exact_f64`.

### 8. Paint and hover reconstruct one geometry formula

One **formula**, reconstructed equivalently by both — not one allocation.

Paint and hover are separate calls in different coordinate spaces, so "computed
once and read by both" is not implementable literally, exactly as PRODUCT-0088 §2
and PRODUCT-0089 §4 each found. The contract is:

One constructor taking **size and `PieSlices`**:

- **centre and radius derive from size** — the plot's local width and height;
- **boundaries come from the validated collection**, not from size — they are a
  function of the values, and `PieSlices` already owns them per §7;
- **paint consumes the result directly**;
- **hover translates the widget cursor into local space first**, then hit-tests
  against an equivalently reconstructed instance.

An earlier draft said boundaries derived from width and height, which is the
pre-amendment design: geometry and data were still entangled there.

The four hover defects are consequences of that authority's absence: the panic is
an index into a vector the geometry did not agree with, the A-reports-B
misattribution is the wrong boundary index, the degrees-versus-normalized
comparison is two representations of one quantity, and the radius disagreement is
paint and hover deriving it separately.

Fixing them individually would leave the divergence that produced all four.

### 9. Paint uses local coordinates

Pie paints in local width/height coordinates, as funnel and radial dial do,
rather than `bounds.center()` after the backend has translated. The doubled
origin is removed at its source.

### 10. Empty input is an empty result

Zero rows with every **required** field present yields **no marks and no
legend** — `value` always, and `category` only when the channel is selected, per
§3. The `"No Data": 1.0` invention is removed. An empty relation missing a
required field still refuses, per PRODUCT-0092 gate 13.

### 11. What this record does not do

No Rune word — that is the surface record, PRODUCT-0086's third categorical
consumer. No colour or authored family. No new engine capability: §"Prerequisite
C" establishes that Toron already has it. No changes to `arc_to` (0101).

Pie remains producerless.

## Implementation sequence

**Stage 0 — complete**, against baseline `87ecbdfe`. Findings in Context. Bank
every current pie figure and the measured hover red states as **negative
controls**.

**Stage 1 — the collection.** `PieSlices`, its error, §6's precedence.

**Stage 2 — the boundary.** Planning validates; the factory reconstructs.

**Stage 3 — the kernel.** Consumes the collection; `.sum()`, the lexical sort,
every fallback, and the four vectors are deleted — not guarded.

**Stage 4 — geometry.** §§8–9: one reconstructed formula, local coordinates.

**Stage 5 — re-bank.** The post-0102 parity bank the surface record consumes.

## Acceptance gates

1. **Duplicate categories refuse, equal values included, naming the remedy.** The
   diagnostic contains `.aggregate(`. Three parts: differing values refuse,
   identical values refuse, and the message names the destination.
2. **The named remedy actually runs, and the seam is named.** Polariton depends
   on `toron-client` only (`Cargo.toml:63`); the engine registrations live in the
   separate `nilket` workspace, so this cannot be an ordinary Polariton unit test
   without adding the wrong dependency. It is therefore **two gates across the
   seam**:

   - a **Toron-workspace gate** executing
     `.aggregate([col("category")], [sum(col("value"))])` against `[A:2, A:3, B:5]`
     through the real registrations (`rune_engine.rs:1599`, `:1521`) and asserting
     `[A:5, B:5]`;
   - a **Polariton gate** consuming that exact shape and rendering it.

   A live cross-process gate is the acceptable alternative, named as such. **A
   source-vocabulary assay is not** — that is what produced the earlier draft's
   wrong citation, and it would prove only that a string appears.
3. **The unaggregated form now refuses.** `[A:2, A:3, B:5]` refuses where it
   previously rendered `[A:5, B:5]`. Paired with gate 2: together they show the
   workflow moved rather than vanished.
4. **Relation order is preserved.** Categories in non-alphabetical order render
   clockwise in **relation** order. Against the banked lexical rendering, which
   is the red state — and asserted structurally, since a fixture can coincide.
5. **Ungrouped rows are slices, not keys.** With no category channel selected,
   repeated identical rows all render and **no duplicate check applies**; with the
   channel selected, they refuse. The discriminating pair for §3.
6. **Category dtype admission is a pair.** `String`, `Boolean`, `Categorical`, and
   `Enum` each construct; a **numeric** category refuses. Five cases, so the
   refusal cannot be satisfied by rejecting every non-`String` column — §7's
   admission rule is prose until this gate exists.
7. **Nulls refuse per field, and `"null"` does not.** Null category, null value,
   and the literal string — three cases, the last being the discriminating one.
8. **Negative, non-finite, and inexact values refuse**, each naming its row, with
   a large finite value constructing so the gate cannot pass by refusing
   everything unusual.
9. **A finite input whose sum overflows refuses.** Two near-`f64::MAX` slices sum
   to `inf` and refuse on the **total**, not on either row. Paired with the large-
   finite twin above, since a per-row check passes both rows and still produces an
   unusable denominator.
10. **Exactness is shared.** `2^53` accepted, `2^53 + 1` refused, `2^53 + 2`
   accepted, routed through `exact_f64` — pie currently bypasses it.
11. **A zero slice is accepted, draws nothing, stays in the legend, and never
   receives hover.** Four parts, per §6 — a gate asserting only acceptance would
   pass an implementation that gives it a sliver.
12. **An all-zero total refuses**, and the final boundary of any accepted pie is
   **exactly `TAU`**.
13. **Empty input succeeds before the total is judged.** No marks, no legend, and
    **no total-validation refusal** — §6 accepts an empty collection outright.
    Without this, an all-zero-total rule applied unconditionally would refuse
    every empty pie. Also, paired with an empty relation
   missing a column, which refuses.
14. **A one-slice pie hovers without panicking.** The banked red state is a panic.
15. **A two-equal-slice pie attributes correctly.** The half drawn as A reports
    **A**. Banked red state: it reports B.
16. **Hover rejects points outside the pie; a near-centre point attributes to its
    unique sector; the exact centre returns `None`.** Three parts. The exact
    centre is **not** attributable — every sector owns it and `atan2(0, 0)` is
    undefined — so `None` is correct there, and an earlier draft wrongly listed it
    as a defect.
17. **Paint and hover reconstruct equivalent geometry from one constructor.** Not
    "computed once" — they are separate calls in different spaces, per §8. The
    gate asserts both paths obtain equal centre, radius, and boundaries from the
    same `(size, PieSlices)` constructor. Gates **14–16** are its consequences,
    and fixing those individually would pass them while leaving the divergence.
18. **Angles are compared in one representation.** No normalized value is
    compared against a degree value anywhere in pie.
19. **A non-zero plot origin paints correctly.** Pie in a faceted non-origin cell
    renders in the right place — the doubled-origin red state — and matches
    funnel's and radial dial's convention.
20. **The four truths are one.** `categories`, `values`, `cumulative_angles`, and
    `total_sum` no longer exist as independent fields.
21. **The collection cannot be built by literal.** Trybuild compile-failure with a
    valid twin.
22. **Precedence is deterministic across all four phases.** Preparation →
    row-local → duplicate → total. Three discriminating pairs: a missing field
    with a null row reports the **field**; a duplicate with an invalid value
    reports the **value** (row-local before cross-row); and — the phase an earlier
    draft omitted — a **duplicate category whose accepted rows also overflow the
    total** reports the **duplicate**, proving duplicate detection precedes total
    validation.
23. **All eleven dispositions are gone, and so are the constructs.** Absent from
    preparation: `"No Data"`, `Row {i}`, and every `unwrap_or`/`unwrap_or_else`
    fallback — **and** the operations themselves: `.sum()`, `.unique()`,
    `.sort()`, the lazy `filter`/`select`, and the per-category loop. Forbidding
    only the fallback calls would permit the same pipeline with `?` propagation,
    which keeps the filter-and-sum-per-category shape this record removes.
24. **Parity is claimed only where nothing else changed it.** A banked pie whose
    input is unique, positive, **already lexically ordered**, and rendered **at
    the origin** is byte-identical to the PRODUCT-0101 baseline.

    **Explicitly excepted:** non-lexical relations, which must change under §4,
    and non-origin figures, which must change under §9 — gates 4 and 19 assert
    those changes. An unrestricted parity claim would be false, and a vague
    exception would let unrelated drift through.

## Stop conditions

> If removing `.sum()` leaves no authored path to a summed pie, stop. §"Prerequisite
> C" claims Toron's `.aggregate(...)` is that path; if it cannot express the
> grouping pie needs, C is a blocker after all and this record has taken a
> capability decision without the evidence.

> If any of the four hover defects survives §8's single formula, stop. They were
> diagnosed as consequences of the divergence; one surviving means at least one is
> an independent defect, and patching it here would hide that. The exact centre is
> not among them — returning `None` there is correct, not a defect.

> If pie's painted geometry changes for **gate 24's parity subject** — unique,
> positive, already lexically ordered, rendered at the origin — stop.
> §9 fixes an origin that is only doubled when non-zero, so a figure at the origin
> must be byte-identical to the PRODUCT-0101 baseline.

> If preserving relation order breaks a shipped fixture, stop. Pie has sorted
> lexically for its whole life, so a fixture may depend on it — and whether that
> dependence is authored intent or accident is a finding, not a fixture to edit.

## Supersession

Supersedes no record. Consumes prerequisite **C** by establishing it needs no
engine work. Extends the fabricated-value class PRODUCT-0088 §3 named, at its
fifth site. Adopts PRODUCT-0090 §5's two layers and §6's opacity, PRODUCT-0088
§1's key uniqueness, PRODUCT-0096 §9's field-first precedence, and the shared
exactness authority. Repairs a coordinate defect distinct from PRODUCT-0097's and
PRODUCT-0101's. Blocks the pie surface.

## Rejected

**Keep `.sum()` because pie is conventionally a mark over aggregated data.**
Attractive, and the strongest case any mark in this arc has had for aggregating —
users hand pie transactions and expect slices. Rejected because summation is one
of at least five defensible aggregations, and the mark cannot know which was
meant. A correct answer to an unasked question is still an answer nobody asked
for.

**Keep `.sum()` but require an authored opt-in.** Attractive because it preserves
the workflow and makes the choice explicit. Rejected because the opt-in would
have to name an aggregation, at which point it is a relation operation spelled in
the mark — and Toron already spells it, better, where the data is.

**Sort slices by descending value.** Attractive because most pie charts do, and it
would make small slices adjacent. Rejected for §3's reason: clockwise order is a
design decision, and a mark that reorders silently takes it from the author.

**Fix the four hover defects individually.** Attractive because each has a
measured red state and a small fix. Rejected because all four are symptoms of
paint and hover deriving geometry separately; fixing them leaves the cause and
the next divergence.

**Treat an all-zero total as a degenerate figure with equal slices.** Attractive
because it renders something. Rejected because equal slices assert proportions
the data does not contain — the same objection as PRODUCT-0096 §5's refusal to
print `0.0%` where a conversion is undefined.

**Fold the hover and paint repairs into the surface record.** Attractive because
this record is already large. Rejected because the surface would then inherit an
unmeasured baseline: PRODUCT-0098's and PRODUCT-0100's surfaces both succeeded
because their substrate was green first.

## Consequences

Pie stops answering a question nobody asked. Relations that render today will
refuse, and the refusal names `.aggregate(...)` — so the workflow moves rather
than disappearing, which is the difference between removing a feature and
relocating one.

Prerequisite **C** turns out to be an exhibit and a diagnostic. That is the
cheapest any prerequisite in this arc has resolved, and it is worth noting why:
Toron gained the capability independently, and the plan had listed C as blocking
since before that was true.

Four truths become one, at the fifth site. The pattern is now so consistent that
its absence would be the surprise — and pie is the first where one of the truths
was *derived geometry* rather than data, which is why paint and hover could
disagree about a figure they were both drawing.

Three marks now answer the ordering question three ways, and §3 records all three
reasons together. That is the shape a catalog should have: not one rule applied
uniformly, but a stated reason per mark that a later reader can argue with.
