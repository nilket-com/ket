# ADR 0107: the violin substrate

Status: proposed 2026-08-15. Toron ADR 552 landed relation-side kernel density,
so violin — step 22 of `plans/wip/plot-catalog-restoration.md`, and the last
unauthorable mark — can finally stop computing statistics in the renderer. This
record settles what ket does with the delivered relation: which fields the
substrate prepares, where the median comes from, and who owns the conversion
from density to pixels.

The catalog plan predicted step 22 would be "likely a Toron capability question
rather than a Polariton one". It was. What remains is a ket question, and it is
larger than a deletion.

## Context

### What Toron delivered, surveyed rather than assumed

The capability is **Rune-only**. `kernel_density_default(group, value)` evaluates
128 points; `kernel_density(group, value, points)` takes an authored count.
Toron's SQL planner gained only plan-traversal arms for the new node, so there is
no SQL spelling and no second door.

The output is `group | evaluation_value | density`. `group` keeps the authored
expression's type and is nullable; both floats are non-null. The grid is one
exact finite union extent shared across groups, and rows arrive **key-sorted,
not source-ordered**. Null values are ignored, a group with fewer than two finite
values or no spread emits **no rows**, and a group holding a non-finite value
emits NaN densities.

Two properties matter more than they look. `kernel_density` returns a
`RuneDataFrame`, and the join family — `join`, `inner_join`, `auto_join`,
`broadcast_join`, the outer and semi and anti variants — is registered on that
same type. A density relation is therefore joinable **as a relation**, which is
what makes everything in §2 possible. Had it returned a terminal handle, the
median could not have stayed relation-side at all. The request document warned
that ket has three times read a proposed shape as a delivered one; this record
is written against the delivered one.

### What the renderer does today

`violin.rs` computes its own KDE (`compute_kde`), takes its median from a Polars
`col(val_col).median()` inside the kernel, and carries `medians: Vec<f64>` in its
prepared data. The density it renders is **normalized to each violin's own peak**:

```rust
let max_d = density.iter().cloned().fold(f32::MIN, f32::max);
if max_d > 0.0 {
    for d in density.iter_mut() { *d /= max_d; }
}
```

scaled by `width_scale = band_width * 0.4`. Every violin in a figure therefore
reaches exactly the same maximum width, whatever its group size or concentration.

### The gap this record exists to close

ADR 552 provides neither a median nor renderer-ready widths, so deleting
`compute_kde` is necessary and nowhere near sufficient. Worse, ADR 552 chose a
**shared** evaluation extent on the stated reasoning that "a violin plot exists to
compare categories, and per-group extents make the comparison misleading by
giving each violin its own axis" — while the renderer then discards cross-group
comparison on the width axis by normalizing each violin independently. Moving the
kernel density relation-side while leaving that untouched would honour 552's
reasoning on one axis and contradict it on the other.

Per-violin peak normalization is not the fabrication the `0.03` bandwidth was. It
is a legitimate and common convention. The defect is that it was never *chosen* —
it sits inside a helper that also computed the statistic, so no one had to decide
it. This record's job is to make it a decision.

## Decision

### 1. The substrate prepares four fields and validates them as delivered

`group_label`, `evaluation_value`, `density`, `median`. Types follow the
delivered schema: the group column keeps its authored type and nullability, the
floats are non-null. The substrate validates rows the way PRODUCT-0104 validates
multivariate rows, and the kernel renders what it is given.

`compute_kde` and the `0.03` bandwidth leave the kernel entirely. No statistic is
computed in `violin.rs` after this record.

### 2. The median is authored relation-side in one invocation; ket enforces coherence

The median is `approx_quantile(value, 0.5)` grouped by the same key, joined to
the density relation on that key. That composition is **authored Rune**, and its
canonical spelling is carried by the violin exhibit — the way PRODUCT-0103's pie
exhibit carries `.aggregate([col("category")], [sum(col("value"))])` in
`uat/test32.rn`.

**Polariton does not synthesize relation code.** It compiles author-written Rune
and refuses what arrives wrong; `pie_slices.rs` names the spelling the author
should have used rather than writing it for them. Giving ket a query-synthesis
path to own this composition would invent a capability the frontend has
deliberately never had, and would cut against Rune being the primary interface.

**Why one invocation.** ADR 547 established that grouped approximate quantiles
are stable *within a single execution*, with variation between executions
governed by the declared rank bound. Two invocations can therefore return a
median that disagrees with the shape drawn around it — a stroke sitting visibly
off-centre in its own violin. ADR 547 met the same hazard and pinned the same
answer: the values used to derive the geometry are the values that appear in the
rows.

**The boundary, stated plainly, because it is easy to overclaim:**

- **Same-invocation composition is an authored contract**, locked by the
  canonical exhibit and a source-level control over it. Nothing at runtime
  establishes it.
- **Ket validates delivered-row coherence and refuses violations.** A grid that
  differs between groups, or two medians inside one group, are structural
  defects and are declined.
- **Runtime proof of one execution would require provenance from Toron**, which
  ADR 552 does not provide and this record does not add.

Coherence is not provenance. Two independent executions over unchanged input can
agree on an extent and yield one median per group, and ket would accept them —
correctly, since nothing in the delivered rows distinguishes that case. The
structural guards catch incoherent composition, not a second execution that
happened to agree.

**This trades an exact median for an approximate one.** Today's Polars `.median()`
is exact; `approx_quantile` carries ADR 546's rank-error bound. That is the same
trade ADR 547 accepted for the box plot's quartiles, and it is the price of the
statistic being computed where the data lives rather than after it arrives. It is
stated here rather than discovered later.

### 3. Ket owns visual width normalization, and its default preserves comparison

Converting a density to a pixel half-width is scaling and geometry — extents,
band width, aspect. It is rendering, it stays in ket, and it must not travel back
into the relation. ADR 552 emits unnormalized densities precisely so this
decision belongs here.

**The default normalizes across the figure, not per violin.** One scale derived
from the maximum density across every rendered group, so a group's width is
comparable to its neighbour's — the same comparability ADR 552 bought on the
value axis.

Per-violin peak normalization is **supported by this substrate and has no surface
spelling**. It is implemented and controlled here, because equal-width violins
are a real and sometimes wanted convention, but no authored token exposes it —
PRODUCT-0108 §6 declines to add one until there is demonstrated demand. "Chosen"
means chosen by a caller of this substrate, not by an author of Rune.

The invariant, whichever mode is selected: normalization scales a group's curve,
never reshapes it. Any operation that changes the *relative* densities within one
group is a statistic wearing a renderer's clothes, and belongs in a Toron record.

### 4. A null group label is refused; an absent group stays absent

ADR 552 leaves the group column nullable. A row with no category identity cannot
be placed on a categorical axis, so the substrate **refuses** it, consistent with
PRODUCT-0104's refusal of invalid rows rather than silent dropping.

A group that ADR 552 omitted — fewer than two finite values, or no spread — is
**absent, and absent is not flat**. That record made absence distinguishable from
zero deliberately and instructed consumers not to conflate them. The substrate
must not synthesize a zero-width violin for a missing group, and must not
silently renumber the categorical axis to hide the gap.

### 5. A non-finite group is refused per group, and the refusal is surfaced

ADR 552 emits NaN densities for a group carrying a non-finite input, following
the sticky-NaN semantics of ADR 022. Ket's marks refuse non-finite values, so
that group is declined as a whole rather than rendered from its finite parts.

**The refusal is scoped to the group, never to the figure.** One dirty group does
not condemn its neighbours: the remaining violins still render, and the figure
stays useful. Nor is the dirty group dropped silently — it is retained in
`refused_groups()` and surfaced to the caller, so a declined group can always be
told apart from one that was never present and from one ADR 552 omitted as
degenerate.

That distinction is the whole point. A group excluded for bad input and a
category with genuinely zero density would otherwise render identically — as
nothing — and ADR 552 went to the trouble of making absence distinguishable from
zero precisely so a consumer never has to guess which it is looking at.

### 6. Key-sorted delivery is a dependency, not an observation

Rows arrive sorted by key. The substrate establishes its category order
**explicitly** and never infers it from row order. A renderer that walks rows
assuming authored order would look correct on any fixture whose categories happen
to sort that way and drift on real data — a defect that passes every test written
without knowing to look for it.

### 7. The authored point count is validated before the Rune call

`kernel_density` refuses a count below two with a Rune-level `VmResult::panic`.
The substrate validates the authored count itself and refuses it in ket's own
vocabulary, so an author sees a typed refusal rather than a VM panic surfacing
through the engine boundary.

## Implementation sequence

1. **Substrate and prepared fields** — §1, §4, §5, §6, §7. Consumes the delivered
   relation, validates it, and carries the four fields.
2. **Canonical authored composition and its exhibit** — §2. The Rune spelling
   that produces density and median in one invocation, checked in as the violin
   exhibit, with a source-level control asserting the exhibit carries it. No
   relation code is synthesized in polariton. The substrate's coherence guards
   are exercised against rows shaped like that exhibit's output.
3. **Relation-backed rendering path, beside the raw one** — a kernel that
   consumes [`ViolinDensity`] and implements §3's normalization, added alongside
   today's path rather than replacing it. Nothing is deleted here and
   `ResolvedKernelFields::Violin` is untouched, so the existing mark keeps
   rendering while the new path earns its controls.
4. **The surface, the switch, and the deletions — owned by PRODUCT-0108.**

   This step was drafted as "move the authored spelling onto four fields". There
   is no spelling to move: `violin` is in neither the Rune constructor list, the
   arity table, nor the sink gate. What exists is
   `ResolvedKernelFields::Violin { category, value }`, fed by **automatic
   resolution**, and it is the only path that constructs violin fields at all.

   A removal-only cut is therefore impossible. Replacing those two fields with
   four relation fields, with no constructor to produce them, would leave nothing
   able to construct the kernel's input and would break automatic resolution in
   the same stroke. The surface is the precondition for the switch, not an
   addition on top of it.

   Every sibling mark already splits this way — 0102 then 0103 for pie, 0104 then
   0105 for radar and parallel — so the surface gets its own record and the
   implementation lands atomically inside it. PRODUCT-0108 owns the constructor,
   the resolver, the fate of automatic violin, the switch, and the deletions.

## Acceptance gates

- **The canonical composition is in the exhibit.** A source-level control asserts
  the checked-in violin exhibit composes density and median in one invocation.
  This is the only gate that speaks to provenance, and it speaks about the
  authored source rather than about delivered rows.
- **Incoherent delivery is refused.** Groups disagreeing about the grid, or a
  group carrying two medians, are declined. These gate structural coherence and
  are not evidence of a single execution.
- **Cross-group width comparison survives by default.** Two groups with
  materially different peak density render at materially different widths. This
  control fails against today's per-violin divide, and it is this record's point
  on the rendering side.
- **Per-violin normalization is reachable and does the opposite.** Authored
  explicitly, the same two groups render at equal maximum width. A mode that
  cannot be told apart from the default is not a choice.
- **An absent group is absent.** A degenerate group produces no violin, asserted
  as absence, and the categorical axis does not silently close the gap.
- **A null group label is refused**, not rendered under a placeholder category.
- **A non-finite group is declined whole**, with its neighbours unaffected.
- **Category order does not follow row order.** A fixture whose authored order
  differs from key order renders in the substrate's declared order.
- **No statistic remains in the kernel.** An architecture guard: `violin.rs`
  contains no kernel-density computation, no bandwidth constant, and no median
  aggregate.

## Stop conditions

> If preserving comparable widths requires the relation to carry a normalization
> factor, stop. §3 claims normalization is purely rendering; needing a
> relation-side scalar means the split is wrong and the boundary belongs in a
> Toron record rather than being quietly crossed here.

> If the median cannot be composed without re-authoring it per surface, stop.
> §2's claim that the substrate can own the composition rests on
> `kernel_density` returning a joinable `RuneDataFrame`. If some surface needs
> its own spelling, that is a finding about the Rune surface, not a licence to
> scatter the statistic.

> If the approximate median proves visibly off-centre against the rendered
> density at ordinary group sizes, stop. §2 accepts ADR 546's rank bound on the
> reasoning that a smoothing-scale error never moves a rendered value; a stroke
> that visibly disagrees with its own violin falsifies that reasoning and is a
> Toron question.

> If moving normalization changes a shipped exhibit's appearance in a way no gate
> catches, stop and add the gate before landing. §3 knowingly changes how every
> existing violin looks; that change must be asserted, not discovered.

## Supersession

This record supersedes nothing. It consumes Toron ADR 552 and depends on ADR 546
for the quantile it composes and ADR 547 for the single-execution stability
argument in §2.

## Rejected

**Keeping the KDE in the renderer.** The standing rule is that marks render and
relations compute; the density capability now exists, and the request document
refused this escape before Toron built anything.

**A stepped histogram instead of a violin.** Expressible today with bucket
arithmetic, and refused in the request document as a different mark rather than a
restored one.

**Normalizing per violin by default.** Not because it is wrong — it is a real
convention — but because ADR 552 bought cross-group comparability on the value
axis for a stated reason, and silently discarding it on the width axis would make
the arc self-contradictory. It stays available, authored.

**Composing the median in each surface.** Rejected on ADR 552's own guardrail:
one decided statistic, one place, so a later record replaces one expression.

## Consequences

The median becomes approximate where it was exact. Accepted knowingly, on ADR
547's precedent, and bounded by ADR 546.

Existing violin figures change appearance under the new default, because widths
that were all equal become proportional. That is the intended correction and it
is visible, so it needs an asserted gate rather than a release note.

Shared normalization means one very concentrated group can make its neighbours
look flat. That is information rather than a defect — it is the comparison the
mark exists to support — but it is a real trade, and an author who wants the old
appearance now has to ask for it.

A degenerate group renders as nothing at all. Consumers reading a violin plot as
"one violin per category" will find categories with no violin, which is the
honest outcome and the one ADR 552 chose when it made absence distinguishable
from zero.
