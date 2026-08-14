# ADR 0105: the variadic multivariate surface

Status: proposed 2026-08-13. Radar and parallel have a shared substrate
(PRODUCT-0104), a transport (PRODUCT-0094), and no way to author them — and the
spelling they need is one no existing word has: a **variadic** dimension list.
This record adds `radar(...)` and `parallel(...)` with a shared `.category(...)`
modifier, replaces the transport's seven-slot field array with a concrete
iterator, and gives the dimension-count minimums one authority used by **both**
the authored and automatic origins.

It is step 20 of `plans/wip/plot-catalog-restoration.md` and consumes prerequisite
**D**.

## Context

### D is four coupled boundaries, not one arity check

Every existing mark word carries a **fixed arity**: `box` at 23 values, heatmap
11, bubble 14 or 17, funnel 8, radial dial 8 or 11, pie 8 — each *fields + two
injected span values per field + the mark origin*. Bubble and radial dial handle
two arities by accepting two literals.

A variadic word is `3n + 2`, and that touches four places:

1. the **rewriter** injects field origins only for fixed arities;
2. the **raw constructor** must validate a computed length rather than a literal;
3. **`MarkFields::all()` is capped at seven slots**, so an eight-dimension figure
   cannot reach projection at all;
4. **`MultivariateFields` lives in the kernel substrate**, so the surface must
   move that authority into `plot/spec` rather than create a second carrier.

### The seven-slot cap is PRODUCT-0094's, and it was correct when written

0094 §3 pinned `[Option<&str>; 7]` with "seven slots because box plot is the
widest mark." That was true, and it is now a ceiling.

The pattern is worth naming: a bound derived from current consumers reads as a
fact until a new consumer arrives — the same shape as PRODUCT-0100's
one-token-one-arm claim, which needed scoping to persistence for the same reason.

### `MultivariateFields` is in the wrong layer

Third instance of the inversion PRODUCT-0094 §4 fixed for `BubbleFieldNames` and
PRODUCT-0095 §5 for `BubblePreparationChannel`: a field-name carrier living in
`plot/kernels` while `plot/spec` needs it.

### The automatic resolver admits one dimension

It permits a single dimension for both marks, which paints nothing: a radar needs
three vertices for a polygon and parallel needs two axes for a line between them.
That defect exists **today, in the automatic path**, and a surface record that
only validated authored figures would knowingly ship it.

### Grouping is ambiguous under a scalar variadic spelling

`radar("a", "b", "c", "group")` cannot be disambiguated at runtime: nothing
distinguishes a final dimension from a category without inferring roles from
schema types, which is the silent-selection class PRODUCT-0078 established and
PRODUCT-0095 §2 refused when it declined `Auto` for bubble's label.

## Decision

### 1. Two words, one shared modifier

```text
radar("a", "b", "c").category("group")
parallel("a", "b", "c").category("group")
```

The constructor is **purely variadic** — dimensions only. The `.category(...)`
modifier carries the category field **and its span** explicitly.

The modifier is **accepted once**, and **only by radar and parallel**. A second
`.category(...)` on one mark refuses at the second spelling's span, per
PRODUCT-0091 §7's rule.

**It must not set the `LayerSettings` routing flag.** That is PRODUCT-0091's
implementation defect exactly — `.diverging*()` set `mark.modified = true`, whose
meaning is "this mark needs the `LayerSettings` factory," and routed two marks
into a factory that could not construct them. Every automated gate passed and the
figures refused. A modifier that carries mark semantics must not assert a
settings dependency it does not have.

### 2. Scalar variadic arguments, not a list literal

`radar(["a", "b", "c"], "group")` would keep arity fixed at two, match Rune's
`.aggregate([col(...)], [sum(...)])` idiom, and remove §1's ambiguity outright.

It is rejected because **the rewriter injects origins per top-level argument**.
Spans would have to reach *inside* a list expression — a deeper AST-origin
mechanism than D needs — and without them PRODUCT-0104 §11's precedence table
cannot name the offending **dimension**, only the list.

Scalar arguments preserve the existing per-argument span machinery. The modifier
makes the otherwise ambiguous role explicit. Attribution is what decides this,
not aesthetics.

### 3. The rewriter learns `3n + 2`

Origin injection generalizes from a table of fixed arities to a computed one. The
raw constructor validates `values.len()` against `3n + 2` for the observed `n`,
refusing anything else with the expected relation rather than a literal.

### 4. `MarkFields::all()` returns a concrete `MarkFieldIter<'_>`

Borrowing the selected variant and advancing by index. Fixed and multivariate
variants share **one return type** with no `Vec` and no `Box<dyn Iterator>`.

**Re-derived, not inherited.** PRODUCT-0094 §3 rejected `Vec` because fixed-arity
marks would allocate to accommodate bubble's variability. That reasoning does not
transfer unexamined: variable arity requires **owned storage for the fields**,
which the carrier already has, but it does not require allocating **again** merely
to iterate them. An index-advancing borrow is the shape that satisfies both.

### 5. `MultivariateFields` moves to `plot/spec`

Shared `MultivariateFields { dimensions: Vec<String>, category: Option<String> }`,
with **distinct `Radar` and `Parallel` transport variants** carrying it — the same
split PRODUCT-0104 §1 made for data and geometry, one layer up: one carrier, two
marks, because their *fields* are identical and their *semantics* are not.

`plot/kernels` imports it back, which is the correct direction.

### 6. One dimension-count authority, used by both origins

**Radar requires ≥ 3 dimensions; parallel requires ≥ 2.**

These are **geometry-admission rules**, and they live here rather than in
PRODUCT-0104: putting them in the shared row collection would require passing a
mark or minimum flag into the substrate, which violates 0104 §1's data-shared,
geometry-separate decision.

**One authority, two consumers:** authored `radar`/`parallel` validation **and the
existing automatic resolver**. That repairs the one-dimension automatic defect
rather than knowingly shipping it, without making the substrate understand either
geometry.

### 7. Dimensions are quantitative, the category is nominal

Every dimension resolves as Quantitative with a **per-field span**; the modifier's
category resolves as Nominal with its own. PRODUCT-0104 §11's refusals map to the
dimension whose cell failed, by span.

Both marks gain `semantic_is_compatible` arms, load-bearing as in PRODUCT-0092
§3.

### 8. A duplicate dimension refuses at its second span

`radar("a", "a", "b")` **refuses**, naming the **second** occurrence.

This must be decided rather than left open, because
`MultivariateRowsError` identifies a dimension by **field name** only: with two
`"a"` arguments there is no way to select which span a downstream refusal belongs
to, and the diagnostic would point at an arbitrary one. Refusing at the surface
keeps every later error unambiguous by construction.

The second occurrence is named for PRODUCT-0091 §7's reason: it is the one the
author added, and pointing at the first tells them to delete the argument they
meant to keep.

### 9. Colour: complete assignments, positional fallback, both enumerations

Three cases, not two — because §1 permits an authored figure with **no**
`.category(...)`:

| origin | colour |
|---|---|
| **grouped authored** | complete `CategoryColorAssignment`s, **one shared accessor** for geometry and legend |
| **ungrouped authored** | **one positional slot**, no durable identity |
| automatic | positional ramp, no durable identity, **not** label-keyed |

An earlier draft said "authored figures get complete assignments," which
overstates: an ungrouped authored figure has **no category field and no
categorical domain** to allocate identity from. It gets one slot, and `"All Data"`
stays presentation-only per PRODUCT-0104 §8.

**Both `authored_categories` enumerations** (`spec/mod.rs:2566`, `:2652`) gain
both marks — the two sites PRODUCT-0103 §5 found, updated together, because
updating one produces a mark that allocates identity on one path and falls back on
the other and **both render plausibly**.

### 10. Persistence: two tokens, two arms

`AuthoredFamilyId::RADAR` and `::PARALLEL`, `ALL` to **nine**, one
`from_authored_layer` arm each, both `unreachable!` arms kept distinct per
PRODUCT-0098 §5. Closed policies refuse; the extensible policy round-trips.

### 11. PRODUCT-0104's three orderings are preserved

Authored **dimension** order exactly, **category** order lexical, **global row**
order relational. The surface adds the authored dimension order; it changes none
of the three.

### 12. What this record does not do

No geometry: radial closure and polyline layout stay where PRODUCT-0104 left
them, and each kernel keeps deriving its own display range. No changes to
`MultivariateRows` or its refusals. No violin.

## Implementation sequence

**Stage 0 — bank.** Every radar and parallel figure after PRODUCT-0104, plus a
session round-trip with no such mark in it.

**Stage 1 — transport.** §4's iterator and §5's move, with no new word.

**Stage 2 — the decoder.** §§2–3: variadic arity and origin injection.

**Stage 3 — words, modifier, resolution.** §§1, 6, 7.

**Stage 4 — colour and family.** §§8–9.

**Stage 5 — the exhibits.** `radar_probe.sql` / `parallel_probe.sql`, `test33.rn`.

## Acceptance gates

1. **Both words render at their minimum and above.** `radar` with three
   dimensions and `parallel` with two, plus a wider case for each.
2. **`MarkFieldIter` handles every existing variant unchanged.** Box plot,
   heatmap, bubble at both arities, funnel, radial dial at both, and pie all
   project identically to Stage 0 — §4 changes the return type, not any existing
   mark's fields.
3. **A multivariate figure wider than seven projects.** Eight dimensions reach
   projection and render. Against the banked cap: PRODUCT-0094's
   `[Option<&str>; 7]` makes this impossible today, so it fails before §4.
4. **`all()` allocates nothing.** No `Vec`, no `Box<dyn Iterator>`, for fixed or
   multivariate variants — structural, since §4's whole content is the return
   type.
5. **Arity is validated as `3n + 2`.** Three dimensions accept at 11 values, four
   at 14; `3n + 1` and `3n + 3` refuse, naming the relation rather than a literal.
6. **Every dimension carries its own span.** A bad third dimension names the third
   argument, not the whole call — the property §2 chose scalar arguments to
   preserve.
7. **A duplicate dimension refuses at its second occurrence.** `radar("a", "a", "b")`
   refuses naming the **second** `"a"`, per §8 — and the valid twin
   `radar("a", "b", "c")` constructs. Without the refusal, a later
   `MultivariateRowsError` naming the field `"a"` could not choose between two
   spans.
8. **`.category(...)` carries the modifier-call span.** A category refusal names
   the `.category("group")` call, which is what `raw_mark_modifier` retains today
   (`rune_presentation.rs:2020`). An earlier draft promised the **argument**
   span; modifiers do not carry per-argument origins, and funding that injection
   is a rewriter change this record does not need — the modifier call is one
   token long in practice and points the author at the right place.
9. **`.category(...)` is accepted once, and refused elsewhere.** Three parts: a
   second on one mark refuses at the **second** spelling's span; **every other
   mark refuses it at the modifier span** — associated methods are registered on
   `RuneMark` generally, so it is *callable* and must *refuse*, not be absent; and
   the radar/parallel whitelist is pinned structurally.

   An earlier draft said the modifier was "unavailable on every other mark,"
   which the registration model does not permit.
10. **`.category(...)` does not set the `LayerSettings` routing flag.** Structural,
   and asserted directly: PRODUCT-0091's identical defect passed every automated
   gate while both live figures refused, so a behavioural gate is not sufficient
   evidence here.
11. **Both marks compile as ordinary `Single` bodies.** The discriminating
    assertion PRODUCT-0091's repair added, applied before the defect can recur.
12. **Minimums refuse on the authored origin.** Radar at two dimensions and
    parallel at one each refuse; radar at exactly three and parallel at exactly
    two each render. Minimum-minus-one and exact-minimum, both marks — four cases.
13. **Minimums refuse on the automatic origin too.** The same four cases through
    automatic resolution. This is the currently-shipping defect, and a gate
    covering only the authored path would leave it.
14. **One count authority serves both origins.** Structural: authored validation
    and the automatic resolver call the same check, so the two cannot drift.
15. **Dimensions accept quantitative and refuse categorical; the category accepts
    nominal and refuses numeric.** Both halves each.
16. **`MultivariateFields` lives in `plot/spec`**, with `plot/kernels` importing
    it — structural, per §5.
17. **Colour has three origins, gated separately.** Grouped authored: every
    category resolves to a complete assignment and **never** reaches the fallback,
    with geometry and legend sharing one accessor. **Ungrouped authored**: exactly
    **one positional slot**, no durable identity, and `"All Data"` appears nowhere
    in the collection. Automatic: positional ramp. Three cases, because §9's
    middle one exists only since §1 permits an authored figure with no
    `.category(...)` — and a two-case gate would silently route it to whichever
    neighbour it resembles.
18. **Both `authored_categories` enumerations name both marks.** Four structural
    assertions. Updating one site alone renders plausibly, so no behavioural gate
    separates them.
19. **`ALL` has nine members**, with `RADAR` and `PARALLEL` present; one
    conversion arm each; both `unreachable!` arms intact.
20. **The extensible policy round-trips both; closed policies refuse both.** Six
    cases.
21. **PRODUCT-0104's three orderings hold from Rune.** Authored dimension order
    renders in argument order; categories lexical; rows in global relation order,
    asserted with an **interleaved** fixture per 0104 gate 6.
22. **Existing documents are undisturbed**, and every banked well-formed radar and
    parallel figure is byte-identical.
23. **Parallel hovers in a non-origin facet cell; radar's `None` is banked.**
    Radar's `hover` returns `None` unconditionally (`radar.rs:204`), and §12
    forbids new geometry — so radar has no hover to place, and asserting one would
    require funding it here. Parallel's non-origin hover is gated; radar's `None`
    is banked as current behaviour, and radar hover is a separate record's if it
    is ever wanted.

## Stop conditions

> If teaching the rewriter `3n + 2` requires per-element origins inside a list or
> any other AST-origin mechanism, stop. §2's whole justification is that scalar
> arguments need only a computed count; needing more means the list form was not
> actually the more expensive option and the choice should be re-made.

> If `MarkFieldIter` cannot unify fixed and multivariate variants without `Box` or
> `Vec`, stop and record the cost rather than absorbing it. §4 re-derived
> PRODUCT-0094 §3's rejection deliberately; if the re-derivation is wrong, that is
> a finding about the transport rather than a detail of this record.

> If the dimension-count authority cannot serve the automatic resolver without
> reaching into PRODUCT-0104's collection, stop. §6 keeps minimums out of the
> substrate precisely so 0104's geometry-separate decision survives.

> If a third mark needs `MultivariateFields`, stop and widen. Two marks justify one
> carrier; three would mean the shape is general and the name should not be
> radar-and-parallel specific — the same condition PRODUCT-0104 set for its
> collection.

## Supersession

Supersedes no record. Consumes prerequisite **D**. Amends PRODUCT-0094 §3's
seven-slot array, which was correct for the marks that existed and is a ceiling
for this one. Repairs a live one-dimension defect in the automatic resolver (§6).
Consumes PRODUCT-0104 unchanged. Extends `AuthoredFamilyId::ALL` from seven to
nine.

## Rejected

**A list literal — `radar(["a", "b", "c"], "group")`.** Attractive because arity
stays fixed, it matches Rune's `.aggregate([col(...)], …)` idiom, and it removes
the grouping ambiguity outright. Rejected because the rewriter injects origins per
**top-level argument**: spans would have to reach inside a list expression, and
without them PRODUCT-0104 §11 can name only the list rather than the offending
dimension. Attribution decides it.

**A trailing optional category — `radar("a", "b", "c", "group")`.** Attractive
because it needs no modifier. Rejected because nothing distinguishes a final
dimension from a category without inferring roles from schema types — the
silent-selection class PRODUCT-0078 established and PRODUCT-0095 §2 refused.

**Two words per mark, grouped and ungrouped.** Attractive because it removes the
modifier. Rejected for PRODUCT-0095 §1's reason: the category is an optional
*argument*, not a different mark, and four words would double every downstream
case. Note the pair would **not** buy fixed arity — both words stay variadic in
their dimensions — so the only thing it trades away is the modifier.

**Put the minimums in PRODUCT-0104.** Attractive because they are validation and
0104 owns validation. Rejected because they are **geometry admission** — a radar
needs three vertices and parallel needs two axes — and the substrate would need a
mark or minimum flag to know which, violating 0104 §1's data-shared,
geometry-separate split.

**Enforce minimums only for authored figures.** Attractive because the surface
owns authoring and the automatic defect predates this record. Rejected because it
would knowingly ship a mark that paints nothing, and §6's single authority costs
nothing extra to point at both origins.

**Keep `[Option<&str>; 7]` and cap dimensions at seven.** Attractive because it
changes no transport code. Rejected because the cap is an artifact of box plot's
width, not a property of any mark — a seven-dimension limit on parallel
coordinates would be a product decision made by an array literal.

## Consequences

Radar and parallel become authorable, and the arc's last prerequisite is consumed.
Every mark in the catalog then has a spelling except violin.

PRODUCT-0094's field array becomes an iterator, and the reason is worth keeping:
the array was sized from the widest mark that existed, which made it a fact until
it became a ceiling. Bounds derived from current consumers do that.

A live defect is repaired on the way past. The automatic resolver has admitted
one-dimension radars and parallels for as long as it has existed, and they paint
nothing — found not by looking for it, but because authoring the same marks forced
a minimum to be stated, and a minimum stated for one origin is conspicuous when
the other has none.

One record, two words: shared decoder, iterator, carrier, count authority,
modifier, projection, diagnostics, and colour plumbing — separate minimums,
families, and renderer semantics. That split is the same one PRODUCT-0104 made for
data and geometry, and it holds for the same reason: these marks differ in what
they *mean*, not in what they *need*.
