# ADR 0098: the funnel surface

Status: proposed 2026-08-13. Funnel has a validated stage substrate
(PRODUCT-0096), a repaired faceted hover seam (PRODUCT-0097), a transport
(PRODUCT-0094), and no way to author it. This record adds
`funnel("stage", "value")` and makes funnel **the first categorical mark to
allocate a durable colour identity** — the first production exercise of
PRODUCT-0086's extensible-family design, whose live path today ends in an
`unreachable!`.

It is step 13 of `plans/wip/plot-catalog-restoration.md`. Prerequisite **A** is
consumed here, and the five remaining categorical marks inherit whatever this
establishes.

## Context

### PRODUCT-0086's first live path panics

`ColorLayerIdentity::from_authored_layer` (`spec/mod.rs:874-885`) maps five plot
types and closes with:

```rust
other => unreachable!("plot type {other} has no live authored categorical-family conversion")
```

The comment is accurate today only because no unmapped mark allocates a
`ColorIdentity`. Funnel makes it false. That arm has been silently guaranteeing
what no gate asserts.

Its neighbour is a *different* kind of `unreachable!`: stacked bar's cites
PRODUCT-0064's requirement that stacked marks retain authored `Bar` identity.
One arm means "impossible by design," the other means "not yet built," and they
must not blur.

### The admission machinery is ready

`AuthoredFamilyId::ALL` is `[SCATTER, BAR, LINE, AREA]` (`spec/mod.rs:844`).
`FamilyAdmissionPolicy::for_schema` (`session_persistence.rs:236-244`) routes
schema v1 → `InitialClosedFamilies` (scatter, bar, line), v2 →
`AreaClosedFamilies` (+ area), and **v3..=SCHEMA_VERSION →
`ExtensibleFamilies`**, which admits any parsed family.

So the current schema already accepts a funnel token. Until now that has been
exercised only by a synthetic `"future_family"` (`:902`).

### Funnel colours by index, not identity

`t = i as f32 / (num_stages - 1) as f32`, `settings.color_theme.get_color(t)`
(`funnel.rs:118-123`). Colour is a function of ordinal position, so reordering
the relation recolours every stage.

### Bar shows the established sharing shape

`group_colors: Vec<CategoryColorAssignment>` on prepared data, recorded at
`bar.rs:18-21` as "PRODUCT-0029 §1: the renderer consumes only resolved colors."
Paint (`:235`) and `legend_content` (`:296`) both call `self.group_color(i)`,
with a positional ramp fallback when nothing was assigned.

### No compatibility arm

`semantic_is_compatible` has no `PlotType::Funnel` entry, so its `_ => false`
fallback refuses every authored funnel channel — the gap heatmap and bubble each
had.

## Decision

### 1. `funnel("stage", "value")` — two fields, eight values

Following `raw_box`'s layout: 2 field strings, 2 injected `(start, end)` span
pairs, and the mark origin.

**No implicit-stage form.** A one-argument funnel would have to invent stage
names, which is the `format!("Stage {}", i)` fabrication PRODUCT-0096 §8 removed.

### 2. A `Funnel` variant in the transport

`MarkFields::Funnel(FunnelFields)` and `SpannedMarkFields::Funnel { fields,
origins }`, per PRODUCT-0094 §1. `FunnelFields { stage, value }`, two fields
through the exhaustive `all()` — **no new projection arm**, which is the test of
whether 0094 earned its record for a second time.

### 3. Nominal stage, quantitative value

```rust
(PlotType::Funnel, Channel::X | Channel::Y) => semantic == SemanticType::Nominal,
```

for the stage channel, with `value` binding through the per-role loop as
Quantitative. Load-bearing as in PRODUCT-0092 §3: without it the `_ => false`
fallback refuses everything.

PRODUCT-0096 §8 already fixed dtype admission at the substrate — nominal stage,
quantitative value, numeric stage refuses — so this arm states at binding what
preparation already enforces, and the two must agree.

### 4. Diagnostics carry field and row

`FieldAttribution::Role` (`spec/mod.rs:1491`, PRODUCT-0095 §4) for the two roles,
with all PRODUCT-0096 §9 refusals mapped to argument spans:

| refusal | span |
|---|---|
| null / duplicate stage | the `stage` argument |
| null, non-finite, negative, inexact value | the `value` argument |
| first value not positive | the `value` argument |
| rising value | the `value` argument, naming both rows |

Host mapping is funded here, as PRODUCT-0092 §4 funded heatmap's — an unmapped
refusal falls back to a whole-figure span and the table becomes decoration.

### 5. `AuthoredFamilyId::FUNNEL`, and only `PlotType::Funnel` maps to it

- `AuthoredFamilyId::FUNNEL` is added and included in `ALL`, taking it to five.
- `from_authored_layer` gains **exactly one** arm: `PlotType::Funnel =>
  AuthoredFamilyId::FUNNEL`.

**The two `unreachable!` arms stay distinct.** Stacked bar's keeps its
PRODUCT-0064 message — stacked marks deliberately retain authored `Bar` identity,
which is a decision, not a gap. The general arm keeps its "no live authored
categorical-family conversion" message and simply covers a narrower remainder.

Collapsing them would lose the distinction between a mark that *must not* have
its own family and one that does not have one *yet*, and the next categorical
mark would not know which it was reading.

### 6. Authored stages get complete assignments; fallback is not label-keyed

Two paths, and the boundary between them is the record's most consequential
decision.

**Authored funnel:** each stage label resolves to a complete
`CategoryColorAssignment`. Paint and legend consume **one shared accessor**, as
bar does at `:235` and `:296`. **No authored stage may reach the fallback.**

**Unassigned legacy or automatic funnel:** the positional ramp fallback
(`funnel.rs:118-123`) is retained for compatibility, and carries **no durable
identity guarantee**.

**The fallback is not made label-keyed.** That was the tempting symmetry — it
would make reordering stable everywhere — and it is rejected because a
label-keyed fallback is a **second colour allocator beside PRODUCT-0086**,
deriving durable-looking identity from a path that allocates nothing. Two
allocators disagreeing about what a stage's colour is would be the same
two-truths defect this arc has removed five times, in the one place where the
symptom is invisible: both would produce plausible colours.

Reorder stability is therefore a property of **assignment**, not of colouring.

### 7. The mark-style tier carries the geometry

| element | tier role |
|---|---|
| stage fills | resolved stage colour (§6) |
| internal stage boundaries | `separator` |
| outer silhouette | `fill_detail` |
| value labels | `label_text` (already used, `funnel.rs:146`) |

`separator` for internal boundaries and `fill_detail` for the silhouette follow
PRODUCT-0080's role separation: an internal division between two filled stages is
paper-side, while the outline of the whole shape is ink-side. The distinction is
the one PRODUCT-0081 got wrong first — a boundary drawn in ink on an ink fill is
invisible.

### 8. Surface positions

| position | verdict |
|---|---|
| `relation.views([funnel(...)])` | **accept** |
| `view("name", funnel(...))` | **accept** |
| `figure([...])`, named or bare | **accept** |
| `layers([...])` | **refuse** |
| legacy `plot.funnel(...)` | **refuse** |
| `relation.funnel(...)` | **refuse** |

Matching PRODUCT-0092 §5 and PRODUCT-0095 §9.

### 9. What this record does not do

No colour authoring — stage colours are allocated, not spelled; a `palette(...)`
interaction is PRODUCT-0030's and is not extended here. No aggregation
(prerequisite **C**). No changes to `FunnelStages` or its refusals (0096). No
hover-seam work (0097). No new projection arm (§2).

## Implementation sequence

**Prerequisite —** PRODUCT-0096 and PRODUCT-0097, both landed.

**Stage 0 — bank.** Every funnel figure after 0096 and 0097, plus the current
session schema round-trip for a figure with no funnel in it, so §5's additions
can be shown not to disturb existing documents.

**Stage 1 — compatibility, transport, and the word.** §§1–3.

**Stage 2 — the family.** §5: the token, `ALL`, the single mapping arm.

**Stage 3 — identity and colour.** §6's assignment path and shared accessor.

**Stage 4 — diagnostics and style.** §§4 and 7.

**Stage 5 — the exhibit.** `funnel_probe.sql`, `test30.rn`, README entry.

## Acceptance gates

1. **The word renders a figure.** `funnel("stage", "value")` over a valid
   relation produces stages, labels, and a legend.
2. **Stage accepts nominal and refuses numeric**; **value accepts quantitative and
   refuses string**. Both halves each, per §3.
3. **Binding and preparation agree on dtype.** A column combination admitted at
   binding is admitted by `FunnelStages::from_rows`, and one refused at binding
   never reaches it. §3's two authorities must not diverge.
4. **Every 0096 refusal carries its argument span.** The six rows of §4's table,
   each asserted against the span it names — and the rising-value case naming
   both rows.
5. **Host diagnostics are mapped, not fallback.** Each refusal reaching
   `gui/state.rs` produces its own span rather than a whole-figure span.
6. **Projection carries both fields with no new arm.** Through 0094's exhaustive
   `all()`; a bespoke funnel branch in `project_named_channels` fails this.
7. **`AuthoredFamilyId::FUNNEL` exists and is catalogued.** `ALL` has five
   members and contains it.
8. **`ExtensibleFamilies` admits and round-trips it.** A funnel figure
   checkpoints and restores under the current schema with its family intact.
9. **The closed policies refuse it.** `InitialClosedFamilies` and
   `AreaClosedFamilies` both refuse the funnel token. Two cases, paired with gate
   8 — this is the whole point of 0086's bounded design, and gate 8 alone would
   pass a policy that admits everything unconditionally.
10. **`from_authored_layer` maps only `PlotType::Funnel`.** No other previously
    unreachable type gains a family, and the general `unreachable!` still covers
    the remainder. Structural, per §5.
11. **The two `unreachable!` arms remain distinct.** Stacked bar's retains its
    PRODUCT-0064 message. A refactor that merged them fails, because the two
    conditions mean different things.
12. **Authored stages never reach the fallback.** Every stage in an authored
    funnel resolves through a `CategoryColorAssignment`; the positional ramp is
    unreachable from the authored path. Structural, per §6.
13. **Paint and legend use one accessor.** The colour a stage is filled with
    equals the colour of its legend entry, for every stage — asserted through the
    shared accessor, not by comparing two computed values.
14. **Reordering preserves stage colours, under resolved assignments.** The
    discriminating fixture: stages with an **equal-valued plateau**, so both
    orderings satisfy PRODUCT-0096 §4's non-increasing rule. Reordering the
    relation changes geometry order and **not** the colour attached to each stage
    label. Arbitrary reordering would trip 0096's rising-value refusal and test
    only the refusal path.
15. **The fallback is not label-keyed.** An unassigned funnel still colours by
    position — asserted, so §6's boundary cannot erode into a second allocator by
    someone making the two paths "consistent."
16. **Assignments persist and restore.** Checkpointing and reloading a funnel
    figure restores the same stage-to-colour assignments.
17. **Reattachment restores the same allocation.** Rerunning the document rejoins
    the same panel with the same identity allocation, not a fresh one.
18. **Existing documents are undisturbed.** A session with no funnel round-trips
    byte-identically after §5's additions — the bank from Stage 0.
19. **Style roles are separated.** Internal boundaries use `separator`, the outer
    silhouette uses `fill_detail`, values use `label_text`. Asserted by resolved
    colour per element, since a figure where all three coincide would pass a
    weaker check.
20. **Funnel hovers in a non-origin facet cell.** PRODUCT-0097's repair holds for
    an *authored* funnel, which did not exist when it landed.
21. **Surface positions match §8 exactly.** Six cases.
22. **No implicit-stage form exists.** A one-argument `funnel(...)` refuses,
    naming the expected arity — §1's fabrication boundary.

## Stop conditions

> If a funnel stage can obtain a durable-looking colour without a
> `CategoryColorAssignment`, stop. §6's boundary is the whole decision, and a
> second allocator is invisible in every rendered figure — both paths produce
> plausible colours.

> If mapping `PlotType::Funnel` requires touching the stacked-bar `unreachable!`,
> stop. §5 keeps two different conditions distinct; a change that merges them has
> reinterpreted PRODUCT-0064.

> If the reorder gate cannot be written without triggering PRODUCT-0096's
> rising-value refusal, stop and reconsider the fixture rather than relaxing the
> substrate. The plateau exists precisely so both orderings are valid.

> If persisting a funnel family requires a schema bump, stop. PRODUCT-0086 built
> v3 to carry a bounded opaque identifier without one; needing a bump means the
> extensible design did not work, which is a finding about 0086 rather than a
> step in this record.

## Supersession

Supersedes no record. Consumes PRODUCT-0096's `FunnelStages`, PRODUCT-0097's
hover seam, and PRODUCT-0094's transport unchanged. **First production exercise
of PRODUCT-0086**, extending `AuthoredFamilyId::ALL` from four members to five.
Consumes prerequisite **A**. Leaves prerequisite **C** open.

## Rejected

**Make the positional fallback label-keyed.** Attractive because it would make
reordering stable for every funnel, authored or not, and reads as the consistent
choice. Rejected because it is a second colour allocator beside PRODUCT-0086,
producing durable-looking identity from a path that allocates none. Its failure
mode is invisible: both allocators produce plausible colours, and a disagreement
shows only as a figure that recoloured for no stated reason.

**Remove the positional fallback entirely.** Attractive because it would leave one
path. Rejected because unassigned legacy and automatic funnels exist and must
keep rendering; removing it converts a compatibility path into a refusal for
figures this record does not otherwise touch.

**Collapse the two `unreachable!` arms.** Attractive because after §5 they look
like the same case. Rejected because stacked bar's is a decision — PRODUCT-0064
requires stacked marks to keep authored `Bar` identity — and the general arm is
an absence. The next categorical mark needs to know which it is reading.

**Add every remaining categorical mark's family token now.** Attractive because
`ALL` is being touched anyway and five more tokens is a small diff. Rejected
because a token with no mark behind it is unobservable and its gates vacuous —
the same reason PRODUCT-0091 §10 refused to spell status.

**Draw internal stage boundaries with `fill_detail`.** Attractive because the
silhouette and the internal divisions are both outlines. Rejected because an
internal boundary separates two filled stages and is paper-side, while the
silhouette bounds the whole shape and is ink-side — the distinction PRODUCT-0081
got wrong first, where a boundary drawn in ink on an ink fill is invisible.

**Allow a one-argument `funnel(...)` with generated stage names.** Attractive
because it makes the smallest possible funnel authorable. Rejected because
generated names are the `Stage {i}` fabrication PRODUCT-0096 §8 removed, and
reintroducing it at the surface would undo a substrate decision one record later.

## Consequences

Funnel becomes authorable, and PRODUCT-0086's extensible-family design gets its
first production consumer. The `unreachable!` that has been guaranteeing no
unmapped mark reached persistence stops guaranteeing it for funnel and keeps
guaranteeing it for everything else — which is the narrowest possible way to open
that door.

Prerequisite **A** is consumed. Five categorical marks remain, and each will now
add one token and one mapping arm rather than rediscovering the path; whether
0086's design holds is answered here for all of them.

The colour boundary is the decision most likely to be misread later. Reorder
stability belongs to assignment, not to colouring, and the fallback stays
positional on purpose. Gate 15 exists because the natural instinct on reading
this code is to make the two paths consistent — and consistency is precisely the
defect.
