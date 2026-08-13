# ADR 0092: the heatmap surface

Status: proposed 2026-08-12. Heatmap has a substrate (PRODUCT-0088), a colour
scale (PRODUCT-0089), and no way to author it: the Rune constructor registry holds
`__bar`, `__line`, `__scatter`, `__area`, `__histogram`, `__box`
(`rune_presentation.rs:384-408`) and no quantitative mark. This record adds
`heatmap(x, y, value)`, **sequential-only**, with dedicated field and span
carriers, the schema-compatibility entry the authored path needs, and a
pre-aggregated live exhibit.

It is step 6 of `plans/wip/plot-catalog-restoration.md` and a **prerequisite of
PRODUCT-0091**, whose words are inert without a mark that can be authored.

## Context

### What exists, surveyed 2026-08-12

**No authored path.** `spec/mod.rs:1537` returns "`{plot_type}` does not yet
expose authored channel roles" for heatmap, and the automatic path pins
`color_job: None` (`:1540`).

**The substrate is ready.** `HeatmapCells` provides `value(x, y) -> Option<f64>`,
`domain()`, `x_categories()`, `y_categories()` (`heatmap_cells.rs:127-143`), and
`from_rows` refuses duplicate `(x, y)` pairs, null keys, and non-finite values.
The kernel requests `ContinuousScaleIntent::SEQUENTIAL` (`heatmap.rs:158`) and
resolves colour through one `continuous_scale` helper.

**Preparation still `expect`s.** `prepare_heatmap_data` (`heatmap.rs:134-147`)
asserts column presence, castability to `Float64`, and the `f64()` view. These
hold today because automatic resolution *selects* fields by dtype class:
`number(index, role)` and `category(index, role)` (`spec/mod.rs:2213-2232`) take
the *n*-th numeric or nominal schema field, so the field exists and has the right
type by construction.

**The authored path validates differently.** `resolve_required`
(`spec/mod.rs:2342`) checks presence (`:2361`), that the dtype has a supported
semantic type (`:2366`), and semantic compatibility (`:2378`) — but honours an
authored `Override::Set(declared)` semantic type (`:2373`) in place of the
inferred one.

**`semantic_is_compatible` has no heatmap arm**, and its fallback is `_ => false`
(`spec/mod.rs:2523`), so every authored heatmap channel currently refuses.

**Boolean is Nominal, not numeric.** `semantic_type` maps `is_numeric()` to
Quantitative and `Boolean` to Nominal (`:2480-2487`).

**Box plot is the precedent for everything structural here.** `raw_box` takes 23
values — 7 field strings, 7 injected `(start, end)` span pairs, and the mark
origin — retained as `BoxPlotFieldOrigins` into `DisposableRunePlotSink`
(`rune_presentation.rs:483-556`), and binds its five non-channel roles through a
separate presence-and-dtype loop (`spec/mod.rs:1795-1813`).

**No quantitative mark reaches persistence.** `session_persistence.rs` names only
`Bar | HorizontalBar | Line | Scatter | StackedArea`; `Histogram` and `BoxPlot`
are absent, the precedent PRODUCT-0079 §7 and PRODUCT-0082 §8 relied on.

## Decision

### 1. `heatmap(x, y, value)` — three fields, eleven values

Following `raw_box`'s layout: 3 field strings, 3 injected `(start, end)` span
pairs, and the mark origin.

```text
heatmap("region", "quarter", "margin")
```

`x` and `y` are the two category axes; `value` is the quantitative observation.
No optional channels, no modifiers — the job word arrives in PRODUCT-0091.

### 2. Dedicated `HeatmapFields` and `HeatmapFieldOrigins`

```rust
pub struct HeatmapFields       { pub x: String, pub y: String, pub value: String }
pub struct HeatmapFieldOrigins { pub x: Range<u32>, pub y: Range<u32>, pub value: Range<u32> }

impl HeatmapFields {
    pub fn all(&self) -> [&str; 3];   // x, y, value — the projection contract
}
```

Carried on `DisposableRunePlotSink` beside `box_plot_fields` / `box_plot_origins`.

**`all()` exists because the value column would otherwise be projected away.**
`project_named_channels` (`request.rs:223-245`) projects `x`, `y`, optional
`color`, the histogram end, and the box fields — and nothing else. Planning would
validate against the full frame, projection would drop `value`, and the factory's
`df.column(val_col).expect("resolved heatmap value field is present")`
(`heatmap.rs:140`) would panic on **the first successful authored heatmap**, not
on an edge case.

The complete plumbing path is therefore enumerated, and each hop is this record's
responsibility:

| hop | what carries `value` |
|---|---|
| authored spec | `HeatmapFields` on the sink |
| resolver | the per-role binding of §3 |
| **projector** | `HeatmapFields::all()` in `project_named_channels` |
| prepared validation | `HeatmapCells::from_rows` (0088, unchanged) |
| factory | reconstruction from the projected frame |
| host diagnostics | §4's mapping |

A record that adds a field carrier without adding the projection hop produces a
surface that validates and then panics.

**Not generalized.** The sink's generic `x`, `y`, `category`, `interval_end` slots
cannot express two categories plus a quantitative observation, and they are
already too weak for box plot — which is why `box_plot_fields` exists. Replacing
them with a general carrier would turn a surface record into a transport
redesign, and heatmap and box plot occupy different semantic roles: box plot's
extra fields are five summary statistics of one distribution, heatmap's is a
single observation per cell.

Whether a third such pair justifies generalizing is a decision for whichever
record adds it, with three examples in hand instead of two.

### 3. Heatmap's compatibility entry: categorical axes, quantitative value

`semantic_is_compatible` gains:

```rust
(PlotType::Heatmap, Channel::X | Channel::Y) => semantic == SemanticType::Nominal,
```

and `value` binds through a per-role loop modelled on box plot's
(`spec/mod.rs:1795-1813`), requiring `SemanticType::Quantitative`.

**Nominal only, not `Nominal | Ordinal`.** An earlier draft copied box plot's
`Channel::X` arm. Three reasons against it, and the third is decisive:

- no current dtype infers `Ordinal` (`semantic_type`, `:2480-2490`);
- Rune cannot author a semantic override, so nothing could declare it (§6);
- **`HeatmapCells` sorts categories lexically** — `.collect::<BTreeSet<_>>()`
  (`heatmap_cells.rs:78`) — so accepting `Ordinal` would claim an ordering the
  artifact does not carry. The axis would render alphabetically while the type
  said otherwise.

A future ordinal heatmap must supply an explicit order; it is not reachable by
widening this arm.

**Both axes default to `ScaleFamily::Band`.** `default_scale` selects `Band` only
for `Bar | BoxPlot` and gives everything else nominal `Point`
(`spec/mod.rs:2534-2539`), so heatmap would receive `Point` — dimensionless
category positions. Heatmap cells occupy **bands**: they have width and height,
and `categorical_2d` already returns `(center, bw, bh)`. Heatmap joins the `Band`
arm.

This entry is **load-bearing, not incidental**: without it the `_ => false`
fallback refuses every authored heatmap channel, so there is no working surface.
An implementation adding `_ => true` instead would produce a surface with no
validation at all.

**Boolean refuses as a value.** `semantic_type` maps `Boolean` to Nominal
(`:2487`), so a boolean value column refuses at binding rather than casting to
0/1. That is the right answer — a heatmap of `true`/`false` is a two-category
figure, not a quantitative one — and it is the surprising one, so it is gated.

### 4. Both binding paths attribute identically

`x` and `y` bind through `resolve_required`; `value` binds through the per-role
loop. Two code paths, and they must produce **equivalent attribution**.

**`value` attributes to `Channel::Color`.** `ChannelError` requires a `Channel`,
so "not `Channel::Y`" is not a contract — an earlier draft said only that. Box
plot's loop uses `Channel::Y` for all five roles (`spec/mod.rs:1797,1809`), which
is serviceable when they are summaries of the y axis; heatmap's `y` is a category
axis and `value` is not a y-quantity, so inheriting it would name the wrong
channel.

`Channel::Color` is **for diagnostic attribution only**. `value` still binds
through the per-role quantitative check, *not* through
`(_, Channel::Color) => true` (`:2522`), which accepts every semantic type — see
Rejected.

**Preparation-error spans.** PRODUCT-0088's refusals are discovered after binding,
so each maps to the argument that caused it:

| `HeatmapCellsError` | span |
|---|---|
| `NonFiniteValue` | the `value` argument |
| `NullX` | the `x` argument |
| `NullY` | the `y` argument |
| `DuplicateCell` | the combined `x`/`y` argument span |

`DuplicateCell` takes the combined span because uniqueness is a property of the
key pair; naming either argument alone would suggest the wrong one is at fault.

**The host mapping is funded here.** `gui/state.rs:1520` extracts box-specific
diagnostics and `:1818` carries the fallback-span logic. Heatmap needs the
equivalent path built and gated, not assumed — a refusal that reaches the host
without a mapping falls back to a whole-figure span and the table above becomes
decoration.

A reader must not be able to tell which internal path rejected their field.

### 5. Surface positions, enumerated

Registration scope is pinned rather than left to implementation, matching box
plot's table:

| position | verdict |
|---|---|
| `relation.views([heatmap(...)])` | **accept** |
| `view("name", heatmap(...))` | **accept** |
| `figure([...])`, named or bare | **accept** |
| `layers([...])` | **refuse** |
| legacy `plot.heatmap(...)` | **refuse** |
| `relation.heatmap(...)` | **refuse** |

`layers([...])` refuses because a heatmap occupies the whole categorical grid;
composing it with another mark has no defined meaning until a record decides one.
The two legacy positions refuse because they are not the authoring surface this
arc is building toward, and admitting them would create a second entry point to
retire later.

### 6. The `expect`s stay, and the invariant is narrow

`prepare_heatmap_data`'s `expect`s are **unreachable from the Rune surface**:
§3's entry plus `resolve_required`'s presence and dtype checks reject every
authored input that could reach them.

**This is not a claim of universal resolver safety.** `Override::Set(declared)`
(`spec/mod.rs:2373`) lets a directly-constructed `AuthoredPlotSpec` declare a
String column Quantitative, pass compatibility, and reach the cast `expect`. Rune
never sets that field — it appears nowhere in `rune_presentation.rs` — so the
hole is real and out of reach, which is a different thing from closed.

The invariant is therefore gated as stated: **generated heatmap bindings leave
semantic overrides as `Inherit`**, exercised across integer, float, string,
boolean, missing-field, and unsupported-dtype fixtures. Fallible preparation is
**not** a prerequisite of this record; it becomes one the moment Rune can author
a semantic type, which is the fourth stop condition.

### 7. Sequential-only, and PRODUCT-0091's tripwire stays intact

The mark requests `ContinuousScaleIntent::SEQUENTIAL` and gains no job word.
`architecture_test.rs:464-465` — no production `ColorJob::Diverging`, no
production `ContinuousAnchor::Authored` — is **unchanged** by this record and
must still pass.

That assertion is what keeps the sequencing honest: an implementation that
reached for a diverging word while building this surface fails the build rather
than quietly landing PRODUCT-0091's subject inside a surface record.

### 8. The live exhibit is pre-aggregated, and proves three things

A new `uat/heat_probe.sql` and `uat/test27.rn`, following `box_probe.sql` /
`test26.rn`.

The fixture is **pre-aggregated in SQL** — one row per observed `(x, y)` cell.
That is the honest current workflow: Toron owns aggregation, and heatmap consumes
cells. It also means the surface is usable before prerequisite **C**
(relation-side aggregation), which becomes an ergonomics gap rather than a
correctness one.

The exhibit must carry:

- **exactly one row per observed cell**, so 0088 §1's duplicate refusal is not
  triggered by the happy path;
- **one deliberately absent cross-product cell**, so 0088's absent-cell state has
  a live demonstration rather than only a unit test — for a visual product that
  is most of the evidence;
- **signed, non-degenerate values**, so the domain is two-ended and the eventual
  0091 diverging exhibit has something to anchor;
- **a companion raw relation with duplicates that refuses**, proving Polariton
  did not silently aggregate.

The last is the discriminating one. Without it the exhibit shows a heatmap that
works and cannot distinguish "the fixture was pre-aggregated" from "Polariton
aggregated it for us."

### 9. No persistence footprint, no colour identity

Heatmap allocates no `ColorIdentity` — a continuous scale allocates none — so it
never reaches `AuthoredFamilyId::parse` or the durable-layer family mapping.
`session_persistence.rs` names only `Bar | HorizontalBar | Line | Scatter |
StackedArea`, and this record adds nothing to it.

Same claim PRODUCT-0079 §7 and PRODUCT-0082 §8 made, and gated the same way
rather than asserted.

### 10. What this record does not do

No job word, no anchor, no slot palettes — PRODUCT-0091. No bubble surface — step
7, its own record. No hexbin. No relation-side aggregation (**C**). No changes to
`HeatmapCells` or its refusals (0088). No fallible preparation (§6). No generic
field carrier (§2). No `semantic_type` authoring — which §6 depends on remaining
true.

## Implementation sequence

**Stage 0 — bank.** Every current heatmap figure, all reached through automatic
resolution.

**This is a refactor oracle, not user pixels.** Heatmap has no production
producer — the remaining `PlotType::Heatmap` references are the catalog and
tests — so nothing banked here is reachable by a user today. The bank exists so
that adding the authored path beside the automatic one can be shown to change
neither, which is the same distinction PRODUCT-0079's Stage 0 drew for histogram.
Calling it parity would overstate what is being protected.

**Stage 1 — the compatibility entry and the value role.** §3, with no Rune word
yet, so the authored path becomes testable before it becomes reachable.

**Stage 2 — the carriers.** `HeatmapFields`, `HeatmapFieldOrigins`, and the sink
fields.

**Stage 3 — the word.** `heatmap(x, y, value)` and its span injection.

**Stage 4 — the exhibit.** `heat_probe.sql`, `test27.rn`, README entry.

## Acceptance gates

1. **The word renders a figure.** `heatmap("region", "quarter", "margin")` over the
   pre-aggregated fixture produces cells, axes, and a gradient guide.
2. **Automatic heatmaps are unmoved.** Every banked automatically-resolved heatmap
   figure is byte-identical to Stage 0.
3. **X and Y accept categorical and refuse quantitative.** A nominal column binds
   as `x`; a numeric column named as `x` refuses, naming the field and its
   semantic type. Both halves, since `_ => false` passes the refusal alone.
4. **Ordinal is refused on the axes.** A channel declared `Ordinal` refuses, per
   §3 — unreachable from Rune today, so asserted at the resolver. It guards the
   lexical `BTreeSet` ordering (`heatmap_cells.rs:78`) from a future widening that
   would claim an order the artifact does not carry.
5. **Both axes resolve to `ScaleFamily::Band`.** Not `Point`. `default_scale`
   (`spec/mod.rs:2534-2539`) gives nominal `Point` to everything but `Bar` and
   `BoxPlot`, so this fails before §3's entry lands and pins the cells' width and
   height.
6. **Value accepts quantitative and refuses string.** An integer or float column
   binds; a String column named as `value` refuses.
7. **Boolean refuses as a value.** A `Boolean` column named as `value` refuses
   rather than casting to 0/1 — the surprising case, per §3.
8. **Integer and float both bind and agree.** `i64` and `f64` columns of equal
   values produce byte-identical figures, so `is_numeric()`'s breadth is exercised
   rather than assumed from one type.
9. **A missing field refuses at binding.** A field absent from the schema returns
   `resolve_required`'s presence error and **no** `prepare_heatmap_data` call
   occurs. The paired half: refusal alone passes an implementation that refuses
   and then panics.
10. **An unsupported dtype refuses.** A `List` or `Struct` column named as any of
   the three returns `semantic_type`'s "no supported semantic type" error.
11. **The Rune surface emits `Inherit` semantic overrides.** Every
    `AuthoredPlotSpec` the heatmap word produces carries
    `semantic_type == Override::Inherit` on all three fields — asserted on the
    **emitted spec**, across integer, float, string, boolean, missing-field, and
    unsupported-dtype fixtures. The invalid fixtures never produce a successful
    binding, so the assertion is about what the surface *emits*, not what
    resolution *accepts*. This is §6's narrow invariant, and it makes the
    `expect`s unreachable from Rune without claiming they are unreachable.
12. **Attribution is equivalent across both paths.** A bad `x` (channel path) and a
    bad `value` (per-role path) produce diagnostics of the same shape, each naming
    its own field and span. Specifically, `value`'s diagnostic does **not** name
    `Channel::Y`, which box plot's loop would do by inheritance.
13. **Field spans point at the argument.** A refusal for `value` carries a span
    covering the `value` argument, not the whole `heatmap(...)` call.
14. **The projected frame carries exactly the three roles.** After
    `project_named_channels`, the frame contains `x`, `y`, and `value` — and the
    figure renders. Today `value` is dropped (`request.rs:223-245`) and the
    factory panics, so this gate fails before `HeatmapFields::all()` lands. It is
    the happy path, not an edge case.
15. **Preparation refusals carry the right span.** Per §4's table:
    `NonFiniteValue` → the `value` argument; `NullX` → `x`; `NullY` → `y`;
    `DuplicateCell` → the combined `x`/`y` span. Four cases, each asserted against
    the span it names rather than against "some span."
16. **The host maps heatmap diagnostics, not the fallback.** Each §4 refusal
    reaching `gui/state.rs` produces its own span, not the whole-figure fallback
    (`:1818`). Without this the table in §4 is decoration: an unmapped refusal
    still surfaces, just uselessly.
17. **Surface positions match §5 exactly.** `relation.views([heatmap(...)])`,
    `view("name", heatmap(...))`, and `figure([...])` named and bare all accept;
    `layers([...])`, legacy `plot.heatmap(...)`, and `relation.heatmap(...)` all
    refuse. Six cases, since accepting-everything and refusing-everything each
    pass a partial gate.
18. **The intent is sequential.** An authored heatmap yields
    `{ Sequential, None }`, and `architecture_test.rs:464-465` still passes
    unchanged.
19. **Duplicate cells refuse through the authored path.** The companion raw
    relation produces 0088 §1's duplicate refusal, naming the key and both rows,
    with no figure rendered.
20. **The absent cell is absent.** The exhibit's missing `(x, y)` pair draws no
    rectangle — asserted as an omission in the draw list, against a neighbouring
    populated cell, so the gate distinguishes "absent" from "drawn in the
    background colour."
21. **The domain is two-ended.** The exhibit's signed values produce a domain with
    `low < 0 < high`, so the eventual PRODUCT-0091 diverging exhibit has an anchor
    to move.
22. **The full persistence contract holds.** Authoring, checkpointing, and
    reloading a heatmap figure leaves the **wire schema unchanged**, retains panel
    mapping, produces an **empty colour footprint**, and carries **no heatmap or
    family spelling in the owner payload**; the restored panel comes back
    unresolved, and rerunning rejoins the **same** panel. The absence assertion is
    scoped to the owner payload — the persisted Rune document legitimately
    contains `heatmap(...)`, and a document-wide assertion would fail for the
    wrong reason.
23. **No categorical identity is allocated.** Resolving an authored heatmap against
    a known `ColorIdentity` snapshot returns it unchanged.

## Stop conditions

> If `semantic_is_compatible`'s heatmap entry cannot be added without changing
> another mark's behaviour, stop. The entry is additive by construction — the
> fallback is `_ => false` — so a change elsewhere means the match is being
> restructured, which is a resolver record.

> If the exhibit cannot show an absent cell without also showing a duplicate
> refusal in the same figure, stop and split the fixtures. §8 needs both facts
> demonstrated and neither obscured; one figure that refuses proves nothing about
> the other.

> If heatmap's `value` role cannot attribute independently of `Channel::Y`, stop.
> §4's requirement is that a reader cannot tell which internal path rejected their
> field, and inheriting box plot's attribution defeats it at the first diagnostic.

> If Rune gains the ability to author semantic types — here or in any concurrent
> record — stop. §6's invariant collapses, the `expect`s become reachable from
> authored input, and fallible heatmap preparation becomes a prerequisite rather
> than a later cleanup.

## Supersession

Supersedes no record. Consumes PRODUCT-0088's `HeatmapCells` and PRODUCT-0089's
scale unchanged. **Blocks PRODUCT-0091**, which needs an authorable quantitative
mark. Leaves `architecture_test.rs:464-465` intact for 0091 to retire. Adds
nothing to persistence and touches no ledger item.

## Rejected

**Generalize the sink's field carriers now.** Attractive because heatmap is the
second mark to need one and the generic `x`/`y`/`category`/`interval_end` slots
are visibly too weak. Rejected because heatmap and box plot carry different
semantic roles — five summary statistics of one distribution versus one
observation per cell — and a general carrier designed from two examples would be
decided by whichever fit worse. It is a transport decision, not a surface one.

**Make preparation fallible as part of this record.** Attractive because
`prepare_heatmap_data` still `expect`s and PRODUCT-0090 paid exactly this for
bubble. Rejected because the binding matrix shows the `expect`s unreachable from
Rune, so the repair would be unmotivated here — and its gates would have no
authored fixture that reaches them. Recorded instead as §6's narrow invariant and
the fourth stop condition.

**Accept `Boolean` as a value by casting to 0/1.** Attractive because it renders
something and users may expect it. Rejected because a two-valued field is a
category, and a heatmap of it should be authored as such; silently numericizing
it would make the colour scale describe a distinction it cannot represent.

**Aggregate duplicates in the kernel rather than refusing.** Attractive because
the first authored heatmap over raw data will refuse. Rejected because
PRODUCT-0088 §1 decided key uniqueness deliberately and marks do not aggregate —
0056, 0064 §8, 0069 §2, 0072, 0078, 0081, 0088. §8's fixture answers the
ergonomics instead.

**Ship the exhibit without the duplicate-relation companion.** Attractive because
it adds a second fixture for a figure that refuses. Rejected because a working
heatmap over a pre-aggregated relation cannot distinguish "the fixture was
pre-aggregated" from "Polariton aggregated it," and the second is exactly what
0088 forbids.

**Bind `value` through `Channel::Color` instead of a per-role loop.** Attractive
because colour is what `value` drives and `(_, Channel::Color) => true` already
exists (`spec/mod.rs:2522`). Rejected because that arm accepts *every* semantic
type, so a String value column would bind and reach the cast `expect` — turning
§6's narrow invariant false in the same record that states it.

## Consequences

The first quantitative mark becomes authorable, and PRODUCT-0091 gains the
consumer without which its words would be inert.

`prepare_heatmap_data` keeps its `expect`s, and the reason is now written down:
they are unreachable from Rune because binding validates presence and dtype, not
because the function is safe. That distinction matters because the gap is real —
a directly-constructed `AuthoredPlotSpec` can declare a String column
Quantitative via `Override::Set` (`spec/mod.rs:2373`), pass compatibility, and
panic in the cast. Nothing in Rune can do it today, and the fourth stop condition
is what keeps that true.

The exhibit makes 0088's absent-cell state visible for the first time. It also
makes the aggregation boundary explicit rather than implied: the pre-aggregated
fixture shows the intended workflow, and its duplicate-bearing companion shows
what Polariton refuses to do on the author's behalf.

Two marks now carry bespoke field and span carriers. A third will make the case
for generalizing, and this record deliberately leaves that case to be made rather
than pre-empting it from two examples.
