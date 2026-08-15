# ADR 0108: the violin surface

Status: implemented 2026-08-15. Violin now has its validated density substrate
(PRODUCT-0107), a figure-wide width normalization with its controls, and the
four-field `violin(...)` surface over the canonical composition in `uat/test34.rn`.
Automatic and raw violin refuse by decision, while the in-kernel statistics are
gone in the same cut because they could not be separated from the surface switch.

It completes step 22 of `plans/wip/plot-catalog-restoration.md`, the last
unauthorable mark in the catalog.

## Context

### Why this could not be a removal-only cut

PRODUCT-0107's sequence assumed its step 4 would *move* an authored spelling onto
four fields. There is no spelling to move: `violin` appears in neither the Rune
constructor list, the arity table, nor the sink gate. What exists instead is
`ResolvedKernelFields::Violin { category, value }`, fed by **automatic
resolution** from a schema — and that is the only path in the product that
constructs violin fields at all.

So replacing those two fields with four relation fields, without a constructor
first, would leave nothing able to produce them and would break automatic violin
resolution in the same stroke. The surface is not an addition on top of the
switch; it is the precondition for it. This record therefore owns both, and the
implementation lands atomically.

### What is already decided elsewhere

Toron ADR 552 owns the density and its bandwidth. PRODUCT-0107 owns the prepared
fields, the per-group refusal policy, the declared category order, and the
figure-wide normalization the kernel will consume. `uat/test34.rn` carries the
canonical one-invocation composition and is gated at source level. None of that
is reopened here.

## Decision

### 1. `violin("group", "evaluation_value", "density", "median")` — four fields, all required

The mark consumes a prepared density relation, so its arguments name the columns
of that relation rather than raw observations. There is no two-argument form and
no optional argument: every prepared field is load-bearing, and a violin missing
its median has no stroke while one missing its densities has no shape.

### 2. The group accepts text and boolean keys; other delivered types are refused by name

Toron accepts a wider group expression than ket can place on a categorical axis.
`is_supported_density_group_type` admits `Boolean`, `Int32`, `Int64`, `Float32`,
`Float64`, `Date32`, `Utf8`, and UTC-zoned `Timestamp`. Ket's `semantic_type`
maps `Utf8` and `Boolean` to **Nominal**, every numeric type to **Quantitative**,
and `Date`/`Datetime`/`Time` to **Temporal**.

So a Nominal requirement accepts two of Toron's eight and refuses six. That
narrowing is **chosen and gated here**, not left to fall out of a channel rule.

**Why refuse rather than widen now.** PRODUCT-0107 keys groups by their rendered
label and declares category order lexically over those labels. Lexical order is
correct for text and boolean keys and wrong for the rest: `"10"` sorts before
`"9"`, and any non-ISO rendering of a date orders by its spelling rather than its
instant. Accepting a numeric or temporal group without typed ordering would put
violins in a meaningless sequence — a defect that looks like working software on
any fixture whose labels happen to sort correctly.

The refusal therefore names the accepted set and says the group must arrive as a
text or boolean key. **Typed group ordering is the named follow-on** that widens
this; it belongs in the substrate, because the ordering it needs is
PRODUCT-0107's, not this record's.

### 3. Automatic and raw violin are refused, and the refusal names the composition

**This is the substantive decision.** Automatic resolution binds a nominal and a
quantitative column found in a schema. It cannot produce a violin any more,
because the density it would need **does not exist in the data**: ADR 552 computes
it, from a bandwidth derived per group. Two raw columns cannot fabricate that
relation, and a surface that quietly accepted them would have to compute the
density somewhere — which is the renderer-side statistic this whole arc removed.

**These are two refusals, not one, and they cannot share an attribution.**

`violin("category", "value")` is an **arity error**. There is no density argument
to point at — that is the whole complaint — so it is attributed to the call and
names the four arguments the mark expects. A record that promised an argument
span here would be promising to indict an argument the author did not write.

**Automatic resolution has no authored call at all.** Nothing was spelled, so
there is no span, no argument, and no token to underline. It can only produce a
**mark-level refusal** carried as the figure's status: violin cannot be resolved
from a schema, and the diagnostic names the composition rather than a field.

Both diagnostics name the spelling the author needs — the way `pie_slices.rs`
directs an author to `.aggregate(...)` rather than aggregating for them. Here
that is `.kernel_density(...)` joined to a grouped `approx_quantile`, which is
exactly what `uat/test34.rn` demonstrates.

**This breaks automatic violin resolution, and legacy figures that relied on it
stop rendering.** That is a real cost and it is chosen. Pie could keep automatic
resolution because a pie's fields exist in the data; violin's do not. A mark
whose defining statistic is computed by the engine cannot be resolved from a
schema, and pretending otherwise would mean keeping a second, hidden KDE alive to
serve the automatic path — the precise outcome PRODUCT-0107 exists to prevent.

### 4. A `Violin` variant in the transport

`MarkFields::Violin(ViolinFields)` and its spanned counterpart, per PRODUCT-0094
§1. `ViolinFields { group, evaluation_value, density, median }`, four fields
through the exhaustive `all()`.

### 5. A dedicated violin resolver

Group **Nominal**; `evaluation_value`, `density`, and `median` **Quantitative**,
with PRODUCT-0107's refusals mapped to argument spans:

| refusal | span |
|---|---|
| null group | the `group` argument |
| null, inexact, or non-finite evaluation point | the `evaluation_value` argument |
| null, inexact, or non-finite density | the `density` argument |
| null, inexact, or non-finite median, or two medians in one group | the `median` argument |
| groups disagreeing about the grid | the `evaluation_value` argument |
| a group column that is numeric or temporal (§2) | the `group` argument |

A group declined for non-finite input is **not** a refusal of the figure
(PRODUCT-0107 §5). It is reported and the remaining violins render.

The two refusals in §3 sit outside this table on purpose. An arity error has no
field to attribute and is carried by the call; an automatic-resolution refusal
has no authored call at all and is carried at mark level.

### 6. Per-violin normalization is not exposed in this cut

PRODUCT-0107 §3 keeps per-violin width normalization supported by the substrate,
which implements and controls it. This record does **not** give it
a token. Following ADR 552's treatment of kernel choice, a parameter with no
demonstrated demand is not a surface; the mode exists, is controlled, and gains a
spelling when someone needs one.

The figure-wide default ships, and it changes how every existing violin looks —
see Consequences.

### 7. The raw statistics leave in this cut

`compute_kde`, the `0.03` bandwidth, and the in-kernel Polars median are deleted
as the kernel switches onto `ViolinDensity`. PRODUCT-0107's
`#![allow(dead_code)]` comes off in the same commit; if it survives this cut, the
migration did not happen.

## Implementation sequence

One atomic cut. The ordering below is how it is written, not how it lands:

1. `ViolinFields`, the `MarkFields::Violin` variant, and the authored constructor
   with its arity and sink-gate entries.
2. The violin resolver and its span-mapped refusals, including §2's refusal of
   the raw two-column shape.
3. `ResolvedKernelFields::Violin` onto four fields, and the factory building a
   `ViolinDensity`.
4. The kernel geometry onto curves and normalized widths; the deletions of §7.
5. Fixtures that carry the old prepared shape — the style oracle and the grid
   test — follow.
6. The violin panel joins `uat/test34.rn`, rendered against a disposable profile.

## Acceptance gates

- **`violin("group", "evaluation_value", "density", "median")` renders**, and any
  other arity refuses naming the expected one.
- **`violin("category", "value")` refuses as an arity error**, carried by the
  call, naming the four expected arguments. The gate asserts the attribution is
  the call and **not** an argument span, since there is no density argument to
  indict.
- **Automatic resolution no longer produces a violin**, and refuses at **mark
  level** with no argument span at all. Asserted as refusal, not as absence of a
  test, and asserted to carry no span — a gate that demanded one would be
  demanding something this path cannot possess.
- Both refusals name the density composition rather than describing a type error.
  These fail against today's automatic resolution, which accepts a raw two-column
  shape, and they are this record's point.
- **A numeric or temporal group column refuses at the `group` argument**, naming
  the accepted text-and-boolean set. One control per refused type — `Int64`,
  `Float64`, `Date32`, and UTC `Timestamp` — because a single case would leave the
  other three to be discovered by an author.
- **A `Utf8` group and a `Boolean` group both render**, so §2's accepted set is
  asserted rather than assumed from one example.
- Each PRODUCT-0107 refusal reaches its mapped argument span.
- **A declined group leaves its neighbours rendering**, per PRODUCT-0107 §5.
- **No statistic remains in the kernel**: an architecture guard finds no
  kernel-density computation, no bandwidth constant, and no median aggregate in
  `violin.rs`.
- The exhibit's violin panel renders against a freshly seeded disposable profile,
  and the `lonely` group has no violin.

## Stop conditions

> If refusing automatic violin breaks a shipped exhibit other than by removing a
> violin, stop. §2 expects legacy violin figures to stop rendering; anything else
> breaking means automatic resolution was load-bearing somewhere this record did
> not survey.

> If the resolver cannot map a PRODUCT-0107 refusal to an argument span, stop.
> An unmappable refusal means the substrate is rejecting something the surface
> cannot describe, which is a substrate question rather than a diagnostic one.

> If no authored spelling can present a date or numeric group as a text key,
> stop. §2's refusal assumes the author has a way to comply; a refusal with no
> available remedy is a wall rather than a diagnostic, and would mean typed group
> ordering is a prerequisite rather than a follow-on.

> If the kernel needs a statistic after the deletions in §7, stop. The prepared
> fields were chosen to make the kernel a renderer; needing one back means
> PRODUCT-0107 §1 chose the wrong four.

## Supersession

This record supersedes nothing. It completes PRODUCT-0107, whose sequence
assumed a surface that did not exist, and it consumes Toron ADR 552.

## Rejected

**A two-argument `violin("category", "value")` that composes the density.** It
would require the frontend to synthesize relation code, which polariton has
deliberately never done, and would put ADR 552's bandwidth decision behind a
spelling that hides it.

**Keeping automatic violin alive on a second KDE.** The only way to serve
automatic resolution is to keep computing density in the renderer for that path.
That is the defect this arc removed, and preserving it for compatibility would
mean the removal never happened.

**Exposing per-violin normalization now.** Deferred per §5; the mode exists and
is controlled, and a token with no demonstrated demand is not a surface.

## Consequences

Every existing violin changes appearance: widths that were all equal become
proportional under the figure-wide default. Intended, and gated.

Legacy figures that resolved a violin automatically stop rendering and refuse
instead. The refusal names the composition, so the path forward is in the error
rather than in this document — but it is a break, and it is the price of the
mark's defining statistic living where the data does.

Violin becomes the only mark in the catalog that cannot be resolved from a
schema. That asymmetry is honest: it is the only mark whose geometry is computed
by the engine rather than read from columns.
