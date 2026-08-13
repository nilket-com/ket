# ADR 0094: the mark-field transport

Status: proposed 2026-08-12. Box plot and heatmap each carry a bespoke pair of
field and origin types through seven production files, and every consumer that
handles marks with extra fields grows one `if let Some(..)` arm per mark. This
record replaces the pairs with two sum types of **dedicated variants** — not a
generic field map — so that adding a mark forces every consumer to acknowledge
it.

It is step 8 of `plans/wip/plot-catalog-restoration.md`, deliberately sequenced
**after** PRODUCT-0092 shipped bespoke `HeatmapFields` with every gate green, so
the refactor has two live parity subjects rather than one working example and one
design.

## Context

### Why now, and not at two examples

PRODUCT-0092 §2 deferred this explicitly: "Whether a third such pair justifies
generalizing is a decision for whichever record adds it, with three examples in
hand instead of two." Bubble is the third, and its shape is what makes the design
answerable rather than guessed — see §3.

### What exists, surveyed 2026-08-12

**Seven production files.** `plot/spec/mod.rs`, `rune_presentation.rs`,
`rune_relation_envelope.rs`, `gui/state.rs`, `plot/request.rs`,
`plot/kernels/boxplot.rs`, and **`plot/factory.rs`**.
`rune_view_compiler.rs`'s references are tests only (`:1731-1861`).

**`factory.rs:230` reconstructs `BoxPlotFields` field-by-field** from resolved
kernel fields — a post-resolution consumer, downstream of the authored spec
rather than beside it. It is the only site that rebuilds a carrier rather than
carrying one, and it sits inside an already-exhaustive `ResolvedKernelFields`
match — which is why §8 leaves it alone.

**Four independent `Option` fields on one sink.** `box_plot_fields`,
`box_plot_origins`, `heatmap_fields`, `heatmap_origins`
(`rune_relation_envelope.rs:290-295`). Nothing prevents a `Some` fields with a
`None` origins, or a box-plot fields beside heatmap origins.

**Projection grows an arm per mark.** `project_named_channels`
(`request.rs:247-253`) has one `if let Some(box_fields) = spec.box_plot_fields()`
block, and 0092 added a second. Its review caught a *missing* one: the heatmap
value column would have been projected away and panicked the factory on the first
successful authored figure.

**Origins are deliberately excluded from figure equality.** The sink's manual
`PartialEq` (`:303-313`) compares `box_plot_fields` and `heatmap_fields` and not
their origins, documented at `:300-302`:

> Authored location is diagnostic provenance, not plot semantics. Keeping it out
> of equality preserves the canonical-spelling equivalences established by
> PRODUCT-0008 while still carrying it to PRODUCT-0079's post-query errors.

**Origins carry derived accessors.** `HeatmapFieldOrigins::keys()` (`:324`)
returns the combined x/y span that PRODUCT-0092 §4 requires for `DuplicateCell`
attribution.

**Arity already varies, and is about to vary more.** `BoxPlotFields::all()` is
`[&str; 7]` (`spec/mod.rs:354`), heatmap's is `[&str; 3]`. Bubble has five roles
with an **optional** label (`bubble_observations.rs:23-28` names only the four
numeric ones), so its arity is 4 or 5.

## Decision

### 1. Two sum types, split by whether spans are present

```rust
// Semantic. Carried on AuthoredPlotSpec. Derives PartialEq.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MarkFields {
    BoxPlot(BoxPlotFields),
    Heatmap(HeatmapFields),
    Bubble(BubbleFields),
}

// Spanned. Carried at the Rune boundary. Does NOT derive PartialEq.
#[derive(Clone, Debug)]
pub enum SpannedMarkFields {
    BoxPlot { fields: BoxPlotFields, origins: BoxPlotFieldOrigins },
    Heatmap { fields: HeatmapFields, origins: HeatmapFieldOrigins },
    Bubble  { fields: BubbleFields,  origins: BubbleFieldOrigins  },
}
```

The existing per-mark field and origin structs are **kept as-is** and become
variant payloads. This is a transport change, not a re-modelling of what each
mark's fields mean.

### 2. Dedicated variants, not a generic field map

A `HashMap<Role, String>` or a `Vec<(&str, Range<u32>)>` would make every
consumer's exhaustiveness check disappear, which is the property this record
exists to gain. It would also flatten §6's variant-specific accessors and lose
the per-mark guarantees the substrate records established — 0090's
`BubbleFieldNames` means "these four are required," and a map cannot say that.

### 3. `all()` is one allocation-free iterator, because bubble's arity is variable

One exhaustive `all()` authority feeds projection, **allocation-free**:

```rust
impl MarkFields {
    pub fn all(&self) -> impl Iterator<Item = &str> {
        let fields: [Option<&str>; 7] = match self { /* exhaustive */ };
        fields.into_iter().flatten()
    }
}
```

Seven slots because box plot is the widest mark; `flatten()` drops the unused
ones. This covers 3, 4, 5, and 7 fields on the stack.

Not `Vec<&str>`. An earlier draft left the return type open between `Vec` and a
slice, which contradicted the fourth stop condition: a `Vec` would make the two
fixed-arity marks allocate on every projection to accommodate bubble's variable
one, and the stop condition exists precisely to prevent that cost arriving as a
side effect.

Bubble is why the signature cannot be a fixed array — its label is optional, so
its projection list is **4 or 5** entries. Having the third example is what
settles the shape rather than guessing it from two fixed-arity marks.

### 4. `BubbleFields` wraps `BubbleFieldNames`; it does not replace it

```rust
pub struct BubbleFields {
    numeric: BubbleFieldNames,   // PRODUCT-0090, unchanged
    label: Option<String>,
}
```

PRODUCT-0090 already owns `BubbleFieldNames` for the four numeric bindings
(`bubble_observations.rs:23-28`), paired with `BubbleNumericField` for error
attribution. 0094 needs a fifth, optional, field.

**Wrapping, for two reasons.** `BubbleFieldNames`' invariant is "these four are
required" — folding an `Option<String>` into it would destroy that, and it is the
guarantee 0090's refusals rest on. And replacing it would leave `x`, `y`, `size`,
`color` spelled in two types that must agree, which is the drift this record
exists to remove rather than create.

**`BubbleFieldNames` and `BubbleNumericField` move to `plot/spec`**, joining
`BoxPlotFields` and `HeatmapFields` (`spec/mod.rs:343,368`). They currently live
in `plot/kernels/bubble_observations.rs`, and leaving them there would make
`plot/spec` depend on `plot/kernels` — a dependency inversion, since kernels
consume the spec layer and not the reverse. `bubble_observations.rs` imports them
back, which is the correct direction.

**Ownership across the two types**, since the halves already live apart: the
field structs are in `plot/spec` and the origin structs are in
`rune_relation_envelope.rs` (`:318,331`). So `MarkFields` — semantic, field-only
— belongs in `plot/spec` beside its payloads, and `SpannedMarkFields` belongs in
`rune_relation_envelope.rs`, which already imports `crate::plot::spec::HeatmapFields`
and can therefore see both halves. The Rune boundary is also where spans are
meaningful, so the placement follows the meaning rather than only the imports.

### 5. Fields and origins are coupled at the Rune boundary

`SpannedMarkFields` pairs them inside each variant, so a `Some` fields with a
`None` origins is unrepresentable, and so is a box-plot fields beside heatmap
origins. Today four independent `Option`s permit both.

### 6. Equality lives on `MarkFields`, and provenance stays out of identity

- **`MarkFields` derives `PartialEq`.** It is the semantic half; it is what
  figure identity means.
- **`SpannedMarkFields` does not derive `PartialEq`.** Deriving it would drag
  origins into equality.
- **The sink's manual `PartialEq` compares only the `MarkFields` projection**, and
  the comment at `rune_relation_envelope.rs:300-302` moves with it, so the reason
  survives the refactor.

This is the hazard that makes the record non-mechanical. §5's coupling is right,
but coupled values invite a derive, and a derive here would silently break
**PRODUCT-0008's canonical-spelling equivalence** — two differently-spelled but
semantically identical figures would stop comparing equal. That is not a compile
error: it surfaces as panel rejoining and checkpoint dedup quietly changing
behaviour, the worst failure shape available to a refactor billed as mechanical.

**Variant-specific diagnostic accessors stay on `SpannedMarkFields`** — heatmap's
combined key span (`HeatmapFieldOrigins::keys()`, `:324`) among them. Coupling
without flattening: the spans keep their structure, and none of it reaches
identity.

**The comparison borrows; it does not clone.** "Compare the `MarkFields`
projection" must not mean building an owned `MarkFields` from a
`SpannedMarkFields` to compare it — that would clone every field `String` on
every equality check, and sink equality runs on panel rejoining and checkpoint
dedup. Either a borrowed projection returning references, or an explicit variant
match comparing field by field. Excluding origins must not make identity
allocate.

### 7. Bubble's variant may be test-only

`MarkFields::Bubble` and `SpannedMarkFields::Bubble` may land with no production
constructor until the bubble surface at step 9.

That is **substrate staging, not an inert public spelling.** No Rune word is
added, so nothing unobservable becomes authorable — the distinction recorded in
`plot-catalog-restoration.md`'s standing hazards. The variant exists so that
step 9 adds a constructor rather than a transport.

### 8. `factory.rs` is banked unchanged, not migrated

`factory.rs:230` rebuilds `BoxPlotFields` field-by-field from resolved kernel
fields. It stays exactly as it is, and is counted as the seventh production site
so the census is honest about it.

An earlier draft migrated it, reasoning that a bare `BoxPlotFields` construction
outside the sum type preserves a second authority. That was wrong on both counts.
The mixed-mark boundary there is **already exhaustive** — it is a match on
`ResolvedKernelFields`, so a new mark already forces an arm. Inside the `BoxPlot`
arm the mark is known, and constructing the specific type is *more* precise than
constructing a sum.

Migrating it would be actively worse: `prepare_box_plot_data` would receive a
`MarkFields` and have to unwrap the `BoxPlot` variant, introducing a
wrong-variant branch into a function that can only ever be called for box plots.
That trades a real guarantee for an unreachable error case.

The rule this record follows: the sum type belongs where marks are **mixed**, and
the specific type belongs where the mark is already **known**.

### 9. What this record does not do

No behaviour change of any kind — see the first stop condition. No new marks, no
Rune words, no change to any mark's field semantics or validation. No change to
what participates in equality (§6 preserves it exactly). No generic map (§2). No
touching `x`, `y`, `category`, or `interval_end`: the generic slots stay as they
are, and whether they should also move is a separate question this record does
not open.

## Implementation sequence

**Stage 0 — bank.** Every box-plot and heatmap figure, both parity subjects
green as of PRODUCT-0092 and PRODUCT-0093. Plus the sink-equality behaviour: pairs
that compare equal today must compare equal after.

**Stage 1 — the types.** `MarkFields`, `SpannedMarkFields`, and `BubbleFields` /
`BubbleFieldOrigins`, with no consumer migrated.

**Stage 2 — the boundary.** `rune_presentation.rs` constructs
`SpannedMarkFields`; the sink carries it; §5's manual `PartialEq`.

**Stage 3 — the consumers.** `spec/mod.rs`, `request.rs`, `gui/state.rs`,
`boxplot.rs` migrate to `MarkFields`. The four `Option` fields are removed, not
deprecated.

**Stage 4 — the exhaustive `all()`.** Projection consumes it; the per-mark
`if let Some(..)` arms in `project_named_channels` are deleted.

## Acceptance gates

1. **Every banked figure is byte-identical.** All box-plot and heatmap figures
   match Stage 0 exactly, **including those routed through `factory.rs:230`**,
   which §8 leaves unchanged. This record changes no pixel, and the gate is the
   record's primary claim.
2. **Differently-originated sinks still compare equal.** Two sinks with identical
   `MarkFields` and *different* origins compare **equal** — PRODUCT-0008's
   canonical-spelling equivalence, which a `#[derive(PartialEq)]` on
   `SpannedMarkFields` would break.
3. **Semantically different sinks compare unequal.** The valid twin: two sinks
   whose `MarkFields` differ compare **unequal**. Without it, gate 2 is satisfied
   by an equality that always returns true.
4. **`SpannedMarkFields` does not implement `PartialEq`.** Asserted structurally,
   so §6 cannot erode into a derive added for convenience.
5. **Mismatched variants are unrepresentable.** No construction pairs one mark's
   fields with another's origins, and none carries fields without origins —
   a type-level fact, replacing the four independent `Option`s.
6. **Projection is exhaustive.** The exhaustive match lives inside
   `MarkFields::all()`; `project_named_channels` only iterates its result and
   contains no per-mark match or `if let`. Adding a variant therefore fails to
   compile until `all()` gains its arm. The compile-failure half is the point:
   it converts 0092's near-miss into an impossibility.
7. **`all()` handles variable arity.** Bubble with a selected label yields 5
   fields; without, 4. Both, since a fixed-arity implementation passes the first.
8. **Every mark's fields survive projection.** For each variant, the projected
   frame contains exactly that variant's fields. Three cases, mirroring 0092
   gate 14, which caught the omission this record makes impossible.
9. **Variant-specific accessors survive.** `HeatmapFieldOrigins::keys()` is still
   reachable from `SpannedMarkFields` and still yields the combined x/y span, so
   PRODUCT-0092 §4's `DuplicateCell` attribution is unchanged.
10. **Diagnostics are unchanged.** Every box-plot and heatmap binding and
    preparation refusal produces the same message and the same span as before —
    asserted against the banked diagnostics, since `gui/state.rs` carries ten
    references and is the easiest place for attribution to shift unnoticed.
11. **The four `Option` fields are gone.** `box_plot_fields`, `box_plot_origins`,
    `heatmap_fields`, and `heatmap_origins` no longer exist on the sink. A
    structural check, since leaving them alongside the sum type would create the
    second authority this record removes.
12. **`all()` allocates nothing.** Projection over box plot, heatmap, and bubble
    performs no heap allocation for the field list — the `[Option<&str>; 7]` stays
    on the stack. Asserted for the fixed-arity marks especially, since §3 rejected
    `Vec` precisely so they would not pay for bubble's variability.
13. **Sink equality performs no allocation.** Comparing two sinks clones no
    `String` and allocates nothing — §6's borrowed comparison, asserted, because
    the obvious implementation of "compare the projection" builds an owned value
    and would pass gates 2 and 3 while allocating on every panel rejoin.
14. **`BubbleFields` has one copy of the numeric names.** `x`, `y`, `size`, and
    `color` are reachable only through the wrapped `BubbleFieldNames`; no second
    spelling of them exists on `BubbleFields`. Structural, per §4.
15. **Bubble's variant constructs.** `MarkFields::Bubble` and its spanned
    counterpart are constructible and round-trip through `all()`, from tests, with
    **no production constructor and no Rune word** — §7's staging, asserted in
    both directions.

## Stop conditions

> If any banked figure moves, stop. This record's entire warrant is that it
> changes nothing observable; a moved pixel means a semantic change is riding
> along and belongs in its own record.

> If preserving sink equality requires comparing any **origin or provenance**
> field, stop. §6's split exists because provenance must not reach
> identity, and an equality that needs provenance from the spanned half means
> the semantic half is missing something — which is a modelling finding, not a
> transport one.

> If a consumer cannot be made exhaustive without also changing what it does with
> `x`, `y`, `category`, or `interval_end`, stop. §9 excludes the generic slots
> deliberately; a refactor that reaches them has become the transport redesign
> 0092 declined to start.

> If bubble's variable arity cannot be expressed without making the fixed-arity
> marks allocate, stop and record the cost. It is probably acceptable, but it
> should be a decision rather than a side effect discovered in a profile.

## Supersession

Supersedes no record. Discharges PRODUCT-0092 §2's deferred question with the
third example in hand. Preserves PRODUCT-0008's canonical-spelling equivalence
(§6) and PRODUCT-0092 §4's diagnostic attribution (§6, gate 9) unchanged.
Precedes the bubble surface, which becomes its first new consumer.

## Rejected

**A generic field map — `HashMap<Role, String>` or `Vec<(&str, Range<u32>)>`.**
Attractive because it needs no variant per mark and never has to change again.
Rejected because it deletes the exhaustiveness that is the whole point: a new mark
would compile against every consumer while being handled by none, which is
0092's near-miss made permanent. It also flattens variant-specific accessors like
`HeatmapFieldOrigins::keys()` and discards 0090's "these four are required."

**Keep the four `Option` fields and add a helper that validates their pairing.**
Attractive as a smaller diff. Rejected because a runtime check that fields and
origins agree is a gate standing in for a decision — the coupling belongs in the
type, where the invalid state cannot be built.

**Derive `PartialEq` on `SpannedMarkFields` and exclude origins by making the
origin types compare equal to everything.** Attractive because it keeps the derive.
Rejected because an `Eq` impl that ignores its own contents is a trap for every
future reader, and it would make origin comparison silently useless everywhere,
not just in sink equality.

**One type carrying `Option<Origins>` instead of two types.** Attractive because
it halves the type count. Rejected because it reintroduces the representable
mismatch §4 removes — the `None` case would mean "semantic only," and nothing
would stop a Rune-boundary value from carrying it.

**Migrate `factory.rs` to the sum type for uniformity.** Attractive because a
bare `BoxPlotFields` construction outside `MarkFields` looks like a second
authority. Rejected because its enclosing `ResolvedKernelFields` match is already
exhaustive, so the guarantee is present without the sum, and because
`prepare_box_plot_data` would then have to unwrap a variant it can only ever
receive one value of — trading a real guarantee for an unreachable error branch.

**Fold this into the bubble surface record.** Attractive because bubble is the
motivating third example. Rejected because a surface record performing a
seven-file transport refactor is the shape 0091 was corrected for, in reverse,
and because the refactor needs both existing marks green as parity subjects —
which is why it is sequenced after 0092 rather than before.

**Migrate `x`, `y`, `category`, and `interval_end` at the same time.** Attractive
because those generic slots are visibly too weak, which is why the bespoke
carriers exist at all. Rejected as unscoped: every mark uses them, so it is a
change of a different magnitude with no parity subject prepared.

## Consequences

Adding a mark with extra fields stops being an exercise in remembering six
consumers. The compiler names them, and 0092's near-miss — a value column
projected away, discovered in review rather than by a test — becomes
unrepresentable.

The refactor is deliberately behaviour-preserving, and its most valuable gate is
the one asserting nothing changed. That is only credible because both parity
subjects were green first: 0092 shipped bespoke carriers with every gate passing
and 0093 repaired the guide defect that shipping exposed. Generalizing before
either would have left the abstraction with nothing trustworthy to preserve.

One hazard is worth carrying forward. The coupling in §4 is correct and it invites
a derive that would break figure identity in a way no test failure would obviously
explain — panel rejoining and checkpoint dedup changing quietly. §6 and gates 2–4
exist for that single line of code.
