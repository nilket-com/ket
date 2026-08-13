# ADR 0090: the bubble observation substrate

Status: proposed 2026-08-12. Bubble's preparation is infallible and fabricates
numbers for every kind of missing input: a null in any of its four required
numeric channels becomes a real observation, a failed cast fabricates an entire
column, and the domains are derived from the source columns rather than from the
points, so a fabricated value can land outside the range that colours it. This
record makes bubble preparation fallible, refuses missingness instead of
inventing it, and derives every domain from the validated rows.

It is **B3** in `plans/wip/plot-catalog-restoration.md`, and a **prerequisite of
PRODUCT-0089**: 0089 §8 changes `gradient_section` to take a
`&ContinuousColorScale`, which breaks bubble's call site at `bubble.rs:101` and
so forces bubble's migration whether or not its preparation is ready.

## Context

### The fabricated-zero defect, at its second site

PRODUCT-0088 §3 named this defect and paid it for heatmap: `p_val.get(i)
.unwrap_or(0.0)` turned a null value into a real `0.0` observation, and 0088
replaced it with an absent-cell state. Bubble carries the same defect through a
different door, and it is not fixable the same way — heatmap has an explicit
absent-cell state to fall back to, and **bubble does not**. A bubble figure is a
list of observations; there is no empty slot to leave.

### What exists, read 2026-08-12

**Four numeric channels, all required, all fabricating.** `ResolvedKernelFields::
Bubble` binds `x: number(0, "x")?`, `y: number(1, "y")?`, `size: number(2,
"size")?`, `color: number(3, "color")?` (`spec/mod.rs:2281-2284`) — every one
required. Point construction then supplies a default for each anyway:

```rust
x:         x_f64.get(i).unwrap_or(0.0),      // bubble.rs:263
y:         y_f64.get(i).unwrap_or(0.0),      // :264
size_val:  size_f64.get(i).unwrap_or(1.0),   // :265
color_val: color_f64.get(i).unwrap_or(0.0),  // :266
```

Three different defaults across four channels, and **none of them has a
rationale**. `size`'s `1.0` looks like it protects against an invisible
zero-radius mark, but it does not: raw size is normalized to `t_size` against
`size_range` before it reaches geometry, and the radius carries a floor —
`2.0 + t_size * 28.0` (`bubble.rs:128`) — so the smallest bubble is 2.0 regardless
of the raw value. `1.0` is an arbitrary nominal default. The other three are the
additive identity, reached for because it was there.

**The domains are derived from the columns, not the points.** `x_range`,
`y_range`, `size_range`, and `color_range` come from `.min()`/`.max()` over the
polars columns (`bubble.rs:231-237`), and those skip nulls. So the domain
**excludes** nulls while point construction **fabricates** them, and a fabricated
value can fall outside its own derived domain: a colour column of `10…50` with
one null yields `color_range = (10, 50)` and a point at `color_val = 0.0`.

That is the mechanism behind PRODUCT-0089 gate 16's warning that bubble would
"pass while lying" — `0.0` is finite, so 0089's non-finite assertion never fires,
and its out-of-domain clamp turns the missing observation into **exactly the
first ramp colour**, indistinguishable from a genuine minimum.

**A failed cast fabricates the whole column.** Every column access falls back to
`Series::new_empty` on a missing column or failed cast (`bubble.rs:196-222`), so
every `.get(i)` returns `None` and *every* point takes the channel default — all
bubbles at `x = 0`, or an entire figure at one colour, with no diagnostic. The
same shape appears in the domain guards: an all-null column yields `(0.0, 1.0)`.

**The label is a fifth channel with a fifth failure mode.** `label:
optional_category(0)` (`spec/mod.rs:2285`) is the only optional binding. Absent,
it generates `Point {i}` placeholders. Present but null, it reaches
`v.to_string().replace("\"", "")` (`bubble.rs:251`) and a polars null stringifies
to the literal text **`null`**.

## Decision

### 1. The invariant: absence is represented or refused, never fabricated as a number

This is the general rule, and it is stated as prose and gates rather than as a
shared type. Heatmap cells, bubble observations, and future hex bins have
genuinely different structural notions of absence — heatmap has an absent cell,
bubble has no slot to leave empty, hexbin's absence is an empty bin that already
means zero — so a shared `Missing<T>` enum would force one structure onto three
shapes. What transfers is the invariant, not the representation.

Bubble's expression of it is §2: refusal, because bubble has no representable
absent state and inventing one is a surface decision.

### 2. Null in any required numeric channel refuses

A null in `x`, `y`, `size`, or `color` **refuses**, attributed to the **channel**,
its **field name**, and a **1-based row number**.

Not an argument span. Bubble is producerless, and automatic resolution keeps only
field names — `Bubble { x: String, y: String, size: String, color: String, label:
Option<String> }` (`spec/mod.rs:1110-1116`) — with no per-channel origin to point
at. Span plumbing belongs to the future Rune surface; a preparation record that
claimed spans would be describing a carrier that does not exist.

The channels are declared required at `spec/mod.rs:2281-2284`. Supplying a
default for a required channel is the resolved contract disagreeing with itself,
and the disagreement is invisible: the figure renders, so nothing signals that a
row was invented.

**This record does not decide that a null colour means "unmapped."** There is no
unmapped role and no optional numeric channel to attach one to. The distinction
worth preserving is between an **encoding the resolver did not select** — which
may eventually earn a default, an unmapped swatch, or a legend entry — and **a
null inside a selected required encoding**, which is malformed data. Only the
second exists today, and only the second is decided here.

### 3. Non-finite values refuse under the same attribution

`NaN`, `INFINITY`, and `NEG_INFINITY` in any of the four channels refuse, with
the same channel, field, and row attribution as §2.

This is what PRODUCT-0089 §5 requires of its producers, and the reason 0089
places domain and intent validation in planning: a non-finite value reaching
`normalize` is a defect in this crate rather than bad user data. §2 and §3
together are what make that claim true for bubble.

### 4. Missing columns and failed casts refuse; no empty-column substitution

A named column that is absent from the relation, or present but not castable to
`f64`, **refuses** — attributed to the channel and field name, with no row,
because the fault is the relation's shape rather than any row's content. This
covers the four numeric channels and, for the missing case only, a **selected**
label channel; §9 states why.

`Series::new_empty` substitution is removed. A type error that silently produces
a full-height column of fabricated defaults is the same defect as §2 at
relation scale, and it is worse for being harder to notice: one wrong point may
be spotted, a whole figure of them looks deliberate.

### 5. Two layers, because column faults and row faults happen at different times

A single `from_rows` cannot own §4: missing and uncastable columns are discovered
**while producing rows**, so by the time rows exist those faults have already been
survived or fabricated past. Hence two layers with disjoint subjects:

```rust
prepare_bubble_data(...) -> Result<BubblePreparedData, BubblePreparationError>
BubbleObservations::from_rows(fields, rows) -> Result<BubbleObservations, BubbleObservationsError>
```

- **`prepare_bubble_data`** owns column access and casting — §4's faults — and
  nothing else. It replaces the `Series::new_empty` substitutions
  (`bubble.rs:196-222`).
- **`BubbleObservations::from_rows`** owns null and non-finite row validation
  (§§2–3) and domain construction (§7). It takes **neutral rows** with optional
  numeric channels, so the fallbacks have nowhere left to live, and derives source
  ordinals from row order rather than accepting them, so errors can attribute
  rows.
- **`from_rows` takes the field names too.** Rows alone carry values, not the
  names those values were bound from, so a `from_rows` given only rows could name
  a channel but not a field — and §2 requires both. The field-name carrier is what
  lets `BubbleObservationsError` be complete on its own.
- **`BubblePreparationError` wraps `BubbleObservationsError` unchanged**, in one
  variant, rather than restating its cases or re-attributing them. Two error types
  that both enumerate null and non-finite rules are two authorities, and they
  drift; a wrapper that re-decorates is the same problem wearing a delegation.

**Planning** calls `prepare_bubble_data` and returns the diagnostic; the factory
re-prepares the same approved input and `expect`s, per 0088's
validation-then-reconstruction shape. There is no prepared-observations carrier.
Planning never calls `from_rows` directly — only preparation does.

**The refusals are enumerated so one malformed relation always produces one
diagnostic:**

| layer | order | error | attribution |
|---|---:|---|---|
| preparation | 1 | missing column | **channel** + **field name** |
| preparation | 2 | uncastable column | **channel** + **field name** |
| observations | 3 | null in channel *c* | **channel** + **field** + **row number** |
| observations | 4 | non-finite value in channel *c* | **channel** + **field** + **row number** |

**Precedence is per field, not per fault kind.** Preparation walks the bindings in
order — `x`, `y`, `size`, `color`, then `label` if selected — and within each field
checks *missing* before *uncastable*. So an uncastable `x` with a missing `y`
reports **`x`**: the first binding with any fault wins.

The alternative — all missing columns before all uncastable ones — was rejected
because it makes the diagnostic depend on which *kind* of fault happens to exist
elsewhere in the relation. Fixing `y` would then change the error reported for
`x`, and a person repairing a schema one column at a time would see the
diagnostic jump backwards.

Only after every binding passes are rows scanned, in producer order; within a row,
channels in binding order; within a channel, null before non-finite. Row numbers
are **1-based**, since they appear in diagnostics a person reads.

Unlike PRODUCT-0088 there is **no cross-row phase**: bubble has no key, so no
duplicate is possible and every fault is a fact about one row or one column. The
two-phase precedence 0088 needed for determinism is not needed here, and adding
it would imply a cross-row rule that does not exist.

### 6. The validated collection is opaque

`BubbleObservations` exposes no public fields and no struct literal: its
observations and its four domains are **one authority**, and §7's guarantee that
no observation falls outside its own domain holds only if both are set together.
Public fields would let a caller inject points, or ranges, that disagree.

Accessors are read-only. Following PRODUCT-0088's precedent, a **trybuild
compile-failure** test pins that a struct literal does not compile, paired with a
**valid-construction twin** that does — since a trybuild case asserting failure
passes trivially if the type stops existing.

### 7. Domains derive only from the validated rows

`x_range`, `y_range`, `size_range`, and `color_range` are computed from the
constructed observations, not from the source columns. Because §§2–4 leave no
observation with a null or non-finite value, the domain and the points are
derived from the same set by construction — closing the mismatch that let a
fabricated value fall outside its own domain.

This also removes the `.min().unwrap_or(0.0)` / `.max().unwrap_or(1.0)` guards:
with no nulls to skip, the only case they covered is emptiness, which §8 states
directly.

### 8. Empty input is a valid empty result, with stated ranges

A **schema-valid** empty relation — every required column present and castable,
zero rows — is not an error. It yields an empty observation set, and:

- `x_range` and `y_range` fall back to **`(0.0, 1.0)`**, stated here rather than
  emerging from an `unwrap_or`, because the axes need a range to draw;
- `size_range` and `color_range` are **`(0.0, 0.0)`**, not `(0.0, 1.0)`. Neither
  is an axis range; neither needs a drawable extent. A `(0.0, 1.0)` size range is
  an invented one, exactly as a `(0.0, 1.0)` colour range is;
- **`legend_content()` returns `None`.**

The legend is the substantive change. An earlier draft omitted only the gradient,
which would have left `LegendSection::SizeSample` — built unconditionally at
`bubble.rs:96-100` before the gradient is even considered — printing `low` and
`high` for data that does not exist. Killing the gradient while a size guide still
quotes numbers is worse than leaving both, because it looks deliberate. Both go.

An empty relation **missing a required column still refuses** under §4. Emptiness
is not a licence to skip schema validation, and a producer returning zero rows
because a column is absent is a different fact from one returning zero rows
because nothing matched.

This is PRODUCT-0087's discipline applied to a second guide type: a guide must
not print values nothing exhibits.

### 9. The label channel is decided, not left accidental

`label` becomes `Option<String>` per observation:

- **`label: None`** — the resolver selected no label channel — keeps today's
  generated `Point {i}` placeholder. A positional placeholder is a legitimate
  default for an unselected encoding, and this is the **only** case that may
  generate one;
- **`label: Some(field)`, column absent from the relation** — **refuses** under
  §4, attributed to the label channel and that field name. A selected encoding
  whose column does not exist is the same relation-shape fault as a missing `x`;
  falling back to `Point {i}` here would silently downgrade a broken binding into
  an unselected one, and the figure would render as though the author had never
  asked for labels;
- **`label: Some(field)`, value null** — `None`, meaning this observation has no
  label. Not the string `null` (`bubble.rs:251`), and not a `Point {i}`
  placeholder, which would misrepresent a null as unlabelled-by-choice.

Label is a category, not a number, so §4's *uncastable* case does not apply to it;
only the missing-column case does.

Label does not refuse, and that is deliberate: §1's invariant is that missingness
is never fabricated **as a number**, and a label is not one. A missing label has
a faithful representation — no label — which is exactly the "represented" branch
of §1. The four numeric channels refuse only because they have no such
representation.

**The hover consequence is decided here, not deferred.** Hover formats the label
unconditionally as its first line (`bubble.rs:130-134`), so `Option<String>`
breaks that call site immediately — the same lesson as 0089 and `bubble.rs:101`.
A `None` label **omits the first hover line** and retains all four quantitative
lines. It does not render an empty line, and it does not fall back to `Point {i}`,
which would reintroduce §9's distinction at the presentation layer after removing
it from the data.

That is the minimum needed to keep this record's own implementation compiling.
Richer treatment — an explicit "no label" affordance, hover suppression, label
styling — remains the interaction record's.

### 10. What this record does not do

No colour scale — that is PRODUCT-0089, which depends on this record. No Rune
spelling, no optional numeric channels, no default size, no unmapped-colour
swatch: those are the bubble surface record, and §2 explains why they cannot be
anticipated here. No size-scale contract (prerequisite **F**). No aggregation —
bubble does not aggregate today and does not start.

Bubble remains producerless.

## Implementation sequence

**Stage 0 — bank.** Bubble is producerless, so there is **no reachable user
figure** to protect and the bank is test-oracle parity, not a promise to anyone's
screen. Bank the **direct kernel operations**: `plot()`'s draw list and `hover()`'s
strings, over well-formed fixtures.

The null-bearing, failed-cast, and missing-column fixtures are banked too, but as
**negative controls** — their banked renders are the red states this record
converts into diagnostics, so the diff shows fabricated pixels becoming refusals
rather than the change being asserted only in prose. They are evidence of the
defect, not a baseline to preserve.

**Stage 1 — the collection.** `BubbleObservations`, `BubbleObservationsError`,
and §5's enumerated refusals, with no consumer migrated.

**Stage 2 — the boundary.** Planning calls **`prepare_bubble_data`** and returns
diagnostics; the factory re-prepares and `expect`s. `from_rows` is called only by
preparation, never by planning directly.

**Stage 3 — the kernel.** Bubble consumes the validated collection; the four
`unwrap_or` fallbacks (`bubble.rs:263-266`) and the `Series::new_empty`
substitutions (`:196-222`) are deleted, not guarded.

**Stage 4 — domains and label.** §7's derivation from observations, §8's empty
case, §9's `Option<String>`.

**Stage 5 — re-bank.** The post-B3 parity bank that PRODUCT-0089 Stage 0
consumes.

## Acceptance gates

1. **A null in each required channel refuses, four separate cases.** One row with
   a null `x`, one with a null `y`, one with a null `size`, one with a null
   `color` each produce a refusal naming **that channel** and **that row**. Four
   cases, not one: an implementation checking only the first bound channel passes
   a single-case gate.
2. **The refusal names the right row, 1-based, and the field.** A relation whose
   **fourth** row carries the null produces a diagnostic naming row **4** — not
   `3`, catching an off-by-one from a 0-based ordinal — and naming the bound field
   name, not just the channel. A gate using a single-row relation distinguishes
   neither, since row 1 and index 0 differ by exactly the error being checked.
3. **Non-finite values refuse in each channel.** `NaN`, `INFINITY`, and
   `NEG_INFINITY` each refuse with channel and row attribution.
4. **A finite twin does not refuse.** A far-out-of-range but finite value — `1e300`
   in `color` — constructs successfully. Without this, an implementation
   rejecting everything unusual passes gate 3.
5. **A missing column refuses without a row.** A relation lacking the bound
   `color` column refuses naming the channel, and the diagnostic carries **no**
   row, since no row is at fault.
6. **An uncastable column refuses rather than emptying.** A `color` column of
   non-numeric strings refuses; it does not produce a figure. The banked Stage 0
   render of that fixture is the red state — today it draws every bubble at one
   colour with no diagnostic at all.
7. **Precedence is deterministic across both layers, and per field.** Three pairs,
   per §5's ordering: a relation with a missing column **and** a null row reports
   the **column**; a row with both a null `x` and a non-finite `y` reports the
   **null `x`**; and — the cross-column pair — an **uncastable `x` with a missing
   `y` reports `x`**, not `y`. The third is what distinguishes per-field ordering
   from fault-kind ordering, which would report `y` because missing sorts before
   uncastable.
8. **The preparation error wraps rather than restates.** A null-row refusal
   surfaced through `prepare_bubble_data` carries the *same*
   `BubbleObservationsError` value that `from_rows` returns directly — asserted by
   equality, not by matching message text, since two independently formatted
   messages can agree today and drift tomorrow.
9. **The collection cannot be built by literal.** A trybuild case pins that a
   `BubbleObservations` struct literal fails to compile, paired with a valid
   `from_rows` construction that succeeds. Both halves: a failure case alone
   passes if the type is deleted.
10. **The domain derives from observations, and equals the source domain when no
    row is refused.** For a clean relation, all four ranges are byte-identical to
    Stage 0 — proving §7 changed where domains come from without changing what
    they are on valid input.
11. **No observation falls outside its own domain.** For every constructed
    collection, each observation's four values lie within the corresponding range.
    This is the invariant PRODUCT-0089 gate 16 depends on; today a null colour
    with a `10…50` domain violates it.
12. **A schema-valid empty relation succeeds with the stated ranges and no
    legend.** Zero rows with all four columns present yields an empty collection,
    `x_range == y_range == (0.0, 1.0)`, `size_range == color_range == (0.0, 0.0)`,
    and **`legend_content()` returns `None`** — the whole legend, not just the
    gradient. Against Stage 0, where an empty figure draws both a gradient and a
    `SizeSample` quoting `0.0`–`1.0`.
13. **An empty relation missing a required column still refuses.** Zero rows and
    an absent `color` column produces §4's refusal, not gate 12's empty success.
    The discriminating pair: without it, an implementation that short-circuits on
    `height == 0` before validating schema passes gate 12 and hides a real fault.
14. **An unselected label encoding still generates placeholders.** With
    `label: None`, observations carry `Point 0`, `Point 1`, … exactly as banked.
    Paired with gate 15, since a placeholder is correct in exactly one of the two
    label-absent situations.
15. **A selected label column that is absent refuses.** With `label:
    Some("caption")` and no `caption` column, preparation refuses naming the label
    channel and the field `caption` — it does **not** fall back to `Point {i}`.
    Without this, a broken binding is indistinguishable from an unselected one and
    the figure renders as though labels were never requested.
16. **A null label is `None`, not text.** With a label channel selected and a null
    value, the observation's label is `None` — not `Some("null")`
    (`bubble.rs:251`) and not `Some("Point 3")`. Three-way, since both wrong
    answers are things the current code or a careless fix would produce.
17. **A null label omits one hover line and keeps four.** Hover over such an
    observation returns exactly the four quantitative lines, with no leading
    blank line and no `Point {i}` substitute. Asserted by line count **and**
    content, since a blank first line passes a count-only check.
18. **Well-formed figures are unmoved.** Every banked well-formed bubble figure —
    no null, no non-finite value, no cast or column fault — has a byte-identical
    draw list and hover strings to Stage 0. The negative controls are expected to
    change, and gates 1–15 say how.
19. **The fallbacks are gone, not guarded.** `unwrap_or` does not appear in
    bubble's observation construction and `Series::new_empty` does not appear in
    its column access. A signature-level check, because a guarded fallback passes
    every value-level gate above while leaving the defect one refactor away.

## Stop conditions

> If refusing a null in a required channel breaks a shipped fixture that is not
> demonstrably malformed, stop. That fixture is evidence the channel is not
> really required, which is a resolved-contract decision (`spec/mod.rs:2281-2284`)
> and not one to settle inside a preparation record.

> If planning cannot obtain the rows needed to validate before the factory runs,
> stop. PRODUCT-0088 established this boundary for heatmap; if bubble's binding
> path cannot reach the same shape, the difference is a factory decision and
> deserves its own record rather than an `expect` in a render path.

> If the four channels turn out to need different null policies from one another,
> stop and say why in prose before encoding it. §2's uniform refusal rests on all
> four being required by the same contract; a per-channel policy means the
> contract is not uniform, and that is a surface question.

> If `Option<String>` for the label forces a presentation decision **beyond** §9's
> hover rule — a new affordance, a styling choice, a legend entry — stop. §9
> decides exactly one thing, the omitted hover line, because `bubble.rs:130-134`
> will not compile otherwise. Anything past that is the interaction record's, and
> a preparation record that answers it has grown a second subject.

## Supersession

Supersedes no record. **Blocks PRODUCT-0089**, which cannot be started until this
lands. Extends the defect class PRODUCT-0088 §3 named, at its second site.
Adopts 0088 §2's validation-then-reconstruction shape and its row-attribution
vocabulary. Touches prerequisite **F** (size-scale contract) only by making
`size` non-null; the scale itself is untouched.

## Rejected

**Share a `Missing<T>` representation across heatmap, bubble, and hexbin.**
Attractive because the invariant is genuinely common. Rejected because the three
marks' notions of absence are structurally different — heatmap has an absent
cell, bubble has no slot to leave empty, hexbin's empty bin already means zero —
so one type would force a single structure onto three shapes. The invariant
transfers as prose and gates; the representation does not.

**Give a null colour an "unmapped" swatch.** Attractive because it preserves the
row and is what a mature plotting surface offers. Rejected because no unmapped
role exists, and inventing one here would decide — before any optional numeric
channel exists — that a null in a *required* encoding deserves the same treatment
as an encoding the author declined to bind. Those are different facts.

**Keep the fallbacks but validate afterwards.** Attractive as a smaller diff.
Rejected because a fabricated value is indistinguishable from a real one once
constructed: validating after the fact cannot tell a null-turned-`0.0` from a
genuine `0.0`. Gate 19 exists to keep this from returning as a guard.

**Default `size` to `1.0` and refuse only the other three.** Attractive because
the size default looks principled. Rejected because it is not: normalization and
the `2.0` radius floor (`bubble.rs:128`) already guarantee a visible mark, so
`1.0` protects nothing and is simply an arbitrary nominal value — the same
fabrication as the other three, wearing a plausible motive. A default size
belongs to the surface record that makes `size` optional.

**Refuse a null label too.** Attractive for uniformity with §§2–3. Rejected
because §1's invariant is that missingness is never fabricated as a **number**,
and a label has a faithful absent representation. Refusing it would discard a
usable observation over a cosmetic field.

**Let empty input keep its gradient legend.** Attractive because it is current
behaviour and changing it moves a banked pixel. Rejected because it is
PRODUCT-0087's defect in miniature — a guide printing values the data never
exhibits — and 0087 is a landed record this would contradict.

## Consequences

Bubble preparation becomes fallible, and four kinds of silence become
diagnostics: null observations, non-finite values, missing columns, and failed
casts. Some relations that render today will refuse, which is the point — each
one is currently producing a figure containing values that were never measured.

PRODUCT-0089 is unblocked, and its gate 16 becomes true rather than aspirational:
with no fabricated finite values, "non-finite refuses" and "out-of-domain clamps"
are no longer a pair a null can slip between.

The domain mismatch closes. Deriving ranges from validated observations rather
than from source columns means the set that defines the domain and the set that
is coloured by it are the same set, by construction.

The fabricated-zero defect that PRODUCT-0088 §3 named is now paid at both known
sites. Whether a third exists is not established here; the census in
`plot-catalog-restoration.md` surveyed colour, not preparation, which is how
bubble's went unfound until a signature change forced someone to read it.
