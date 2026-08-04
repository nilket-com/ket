# ADR 0014: the layers surface

Status: accepted 2026-08-03. This record decides one thing: the Rune spelling
that reaches PRODUCT-0013's layered specification, and the lowering that gets it
there. It invents no semantics. Every question about what a layered plot *means*
— compatibility, domain resolution, painting, the bridge — was decided by
PRODUCT-0013 and is inherited unchanged.

## Context

PRODUCT-0013 landed ordered shared-Cartesian layers and closed eight gates, and
deliberately shipped with no production caller: nothing in a Rune buffer can
construct a `LayeredPlotSpec`, so the substrate is exercised only by its own
tests. That was the agreed cost of splitting the arc. This record pays it back.

Everything the spelling needs already exists and is proved:

- PRODUCT-0007 §5 names the grammar stage `Layer`;
- PRODUCT-0008 §3 fixes contextual recognition — sink constructors mean sinks
  only as arguments to `.views([...])`, and recognition inside a proven list is
  atomic;
- PRODUCT-0010 §3 gives views optional names and one identity each, and §4 keeps
  head realizations out of the normal form;
- PRODUCT-0012 makes `line` a sink, so the two marks a layer may carry are both
  already spellable;
- PRODUCT-0013 supplies `LayeredPlotSpec`, `LayerSpec::xy`, and `LayerMark`, and
  fixes the compatibility rules the surface must respect rather than restate.

What remains is one constructor and one lowering.

## Decision

### 1. `layers([...])` is a sink

```rune
ket.table("test1").sort([col("x").asc()]).views([
    table(),
    layers([
        line("x", "y"),
        scatter("x", "y"),
    ]),
])
```

`layers(...)` is recognized only as an element of a proven `.views([...])` list,
under PRODUCT-0008 §3's contextual recognition and its atomicity rule. It takes
exactly one literal list.

An empty list refuses at its own span — but **not** for PRODUCT-0008 §3's reason.
`layers([])` is syntactically one sink, so the artifact count is unambiguously
one. It refuses because PRODUCT-0013 defines a plot as structurally non-empty:
`NonEmpty` has no value to receive, and there is no `LayeredPlotSpec` an empty
list could become.

### 2. Position determines role

> A mark constructor denotes a **view** in `.views([...])` position and a
> **layer** in `layers([...])` position.

This is PRODUCT-0008 §3's rule applied once more rather than a new one. A view
gets a panel, an identity, and a place in presentation order. A layer gets none of
those: it is one mark inside one artifact.

The same text therefore means two different things in two positions, which is the
existing design working as intended — `scatter("x","y")` is already ordinary Rune
in every position except a proven `.views` list, and this adds a second proven
position rather than a second meaning for an unproven one.

### 3. Depth is exactly one

`layers([...])` inside `layers([...])` refuses at the inner span.

PRODUCT-0008 §5 keeps nesting out of the view-set representation so that order is
a property of the form rather than a rule someone maintains. A layer list is a
different axis and does not reopen that question, but only while its depth is
fixed. At depth one, presentation order and paint order remain two flat sequences.

### 4. A layer is `line` or `scatter`, with x and y only

`table()` and `bar(...)` refuse inside `layers([...])`, each at its own span.

Neither refusal is a surface preference. `table()` carries no marks and has
nothing to paint into a shared domain. `bar(...)` has categorical x, and
PRODUCT-0013's accepted capability is one shared **Cartesian** domain; admitting
it would require the scale composition that record defers.

`scatter("x", "y", "cat")` — legal in `.views([...])` position since
PRODUCT-0008 — **refuses inside `layers([...])`**. This is the one place where
position changes which arities are accepted, and it is stated explicitly because
it will otherwise read as a bug.

The boundary between this refusal and PRODUCT-0013's must stay exact, or §6's
prohibition on restating compatibility is violated the moment this section is
implemented:

- **The surface grammar admits two-field layer constructors only.** A
  three-argument `scatter` inside `layers([...])` is a **syntax** refusal, decided
  by this record, at the sink span. The envelope is rejecting an arity, not
  evaluating a color channel.
- **Resolved field and scale compatibility belongs to PRODUCT-0013 alone.**
  Identical x/y field identities, scale families, and the `Absent` color rule are
  checked there. The envelope never restates them and holds no opinion about
  scales.
- **PRODUCT-0013's color refusal is not made redundant by this grammar.** It
  protects every caller of `LayeredPlotSpec`, and the envelope is only one of
  them.

### 5. One sink, one panel, one identity

A `layers([...])` sink is one view. It resolves to one `HostPanelId`, takes one
position in presentation order, and may be named exactly as any other sink:

```rune
view("test1-trend", layers([line("x", "y"), scatter("x", "y")]))
```

The name wraps the whole composition, per PRODUCT-0010 §3. Layers are not
addressable, receive no names, and get no panels. A record that later made layers
individually addressable would be making them views again, which is what
`.views([...])` already spells.

Editing the layer list — adding a mark, reordering, removing one — retains the
view's identity and destination, exactly as PRODUCT-0010 §4 already promises for a
view whose sink form changes.

### 6. Lowering is total and adds nothing

```text
layers([line(x, y), scatter(x, y)])
  -> LayeredPlotSpec { layers: NonEmpty[
       LayerSpec::xy(LayerMark::Line,    x, y),
       LayerSpec::xy(LayerMark::Scatter, x, y),
     ] }
```

Authored list order becomes `LayeredPlotSpec` order, which PRODUCT-0013 §4 makes
paint order. The surface performs no other work: it does not resolve channels,
compute a domain, choose renderers, or check compatibility. Compatibility failures
surface as PRODUCT-0013 refusals attributed to the sink's span, not as a second
copy of the rules living in the envelope.

A one-element list is legal and lowers to a one-layer `LayeredPlotSpec`, which
PRODUCT-0013 §7 routes through the existing single-kernel path. `layers([line(x,
y)])` and `line(x, y)` therefore paint identically.

They do **not** inherently differ in identity. PRODUCT-0010 §3 makes identity
named or positional and §4 makes mark form explicitly non-identifying, so
replacing one spelling with the other in the same named or positional slot is the
same view, keeping its panel. Two such sinks written side by side differ only
because they occupy two positions in presentation order.

### 7. Content kind and display label separate, through an exact presentation seam

PRODUCT-0013 §6 decided that routing asks `Table | Plot` and description comes
from the ordered composition, and assigned the refactor to this record because
`RuneViewForm` lives in the envelope.

That refactor is not cosmetic, because **the head's plot seam still requires the
representation PRODUCT-0013 bypassed**. `RuneRelationSinkOutput::Plot` carries a
`ResolvedPlot`; `set_rune_view_plot` takes a `PlotComposition`
(`editor.rs:907`); and `host_view` derives its title from
`state.composition.title()` (`plot_pane/view.rs:130`). A layered sink has no
`PlotComposition`, and manufacturing one would reintroduce precisely the
normalization and permission table PRODUCT-0013 declined to build on — the layered
path would be laundered back through the model it exists to replace.

This record therefore authorizes exactly one seam:

```text
ViewPresentation { content_kind: Table | Plot, display_label: String }
```

A host plot panel must be completable from `display_label` and a kernel, with no
`PlotComposition`. Routing stops consulting mark identity.

The dashboard is untouched. It keeps `PlotComposition`, `apply_plot_replacing`,
and every other composition-owning behavior it has today; this seam changes only
how a *view* reaches a *host panel*. **If a layered view cannot be completed into
a host panel without changing dashboard behavior, §10's halt fires.** The seam is
authorized; a dashboard change is not.

Existing single-form labels are unchanged — a `line` view still titles as it does
today, so no panel a user recognizes is renamed by this record. A layered view's
label is `LayeredPlotSpec::title()`, which for `[line, scatter]` is exactly
`"Line + Scatter"`.

### 8. Cross-origin layering is unrepresentable

A `layers([...])` sink appears inside one `.views([...])` list, whose receiver has
already fixed one relation origin. Its layers therefore share one canonical origin
and one stamped result by construction, and no refusal is needed.

Layering across origins — one view subscribing to several frames, with its own
cross-origin failure, revision, and domain-reconciliation semantics — is a
different design and a different record. It is named here only so that its absence
is a decision rather than an oversight.

### 9. Acceptance gates

1. `.views([layers([line("x","y"), scatter("x","y")])])` lowers to a two-layer
   `LayeredPlotSpec` in authored order, and reordering the list reorders the
   specification.
2. A `layers([...])` sink produces exactly one panel and one view identity; no
   layer receives either.
3. `layers([])` refuses at its own span, and `layers([layers([...])])` refuses at
   the inner span.
4. `table()` and `bar("c","v")` each refuse inside `layers([...])`, naming their
   spans.
5. `scatter("x","y","cat")` refuses inside `layers([...])` while the identical
   call in `.views([...])` position remains legal. Both halves are asserted
   together; the refusal alone would not prove that position is what changed.
6. **The compatibility handoff.** Layers naming differing x or y fields lower
   *successfully* through the envelope and then fail through PRODUCT-0013, which
   names the offending layer, with the error attributed to the outer sink span.
   This is the gate that proves §6's delegation is real rather than described:
   an envelope that had quietly learned the compatibility rule would refuse
   earlier and pass everything else.
7. `layers(...)` outside a proven `.views([...])` list is ordinary Rune, retained
   and unrefused, including as a bare statement, in a binding, and as an argument
   to an unrelated call.
8. Wrong outer arity and a non-literal-list argument each refuse at the outer
   span; malformed `line`/`scatter` arguments and unknown inner constructors
   refuse atomically, with no partial lowering and no dispatch.
9. `view("n", layers([...]))` names the composition; a rerun reuses its
   `HostPanelId`, and editing the layer list retains identity and destination.
10. Replacing a bare sink with its one-layer `layers([...])` form **in the same
    named or positional slot** retains the view's identity and destination and
    renders identically. Identity does not depend on which spelling was used.
11. Every existing sink in `.views([...])` position behaves exactly as
    PRODUCT-0012 and PRODUCT-0010 leave it, asserted behaviorally.
12. The real head completes a layered view into an ordinary plot host panel with
    no `PlotComposition` constructed on that path, leaving `DashboardState`
    untouched.
13. A layered view's display label is exactly `"Line + Scatter"` for
    `[line, scatter]`, and no single-form label changes.

### 9a. An implementation constraint, not a gate

Existing envelope **behavior** must not change. That is gate 11, and it is
behavioral.

"No existing envelope test changes" is not a gate. §7's representation refactor
may legitimately require type-oriented assertions to be rewritten — a test naming
`RuneViewForm` where routing now asks for a content kind is a citation to update,
not a behavior that regressed. The distinction is PRODUCT-0008's implementation
plan made once already: a guard that fires because the work is doing something
forbidden is a halt; a guard that fires because its premise was superseded is a
citation to update.

### 10. The stop condition

> This record may add the `layers` constructor, its contextual recognition and
> refusals, lowering into `LayeredPlotSpec`, and the routing/label separation in
> §7. If the implementation requires changing PRODUCT-0013's model, compatibility
> rules, domain resolution, renderer behavior, or bridge — or requires a new mark,
> a transport change, or a change to panel identity, placement, or staleness — the
> work stops and the requirement returns as its own record.

The substrate is proved. Any pressure to adjust it while writing the surface is
evidence that the surface is proposing semantics, which §6 forbids it from having.

### 11. What this record does not authorize

No new mark. No layer names or addresses. No `bar` layers, no color on any layer,
no dual axis, no facet, no scale composition, no guide or interaction decision. No
change to Toron, the transport, execution, panel identity, placement, or
staleness. **No change to the dashboard.** No `.points()` sugar and no
`r.layers([...])` relation-method sugar. No retirement of any existing spelling.

The §7 presentation seam is the single authorized change to how a plot reaches a
host panel, and it is bounded by that section's own terms.

## Rejected

- **`r.layers([...])` as a relation-method sugar:** would be a strict expansion of
  `r.views([layers([...])])` and therefore legal under PRODUCT-0008 §2, but it
  adds a spelling without adding a capability. PRODUCT-0008 §6 keeps spellings
  earned by use rather than added in advance of it.
- **Naming individual layers:** a name is an address, an address resolves to a
  panel, and a layer that has a panel is a view. `.views([...])` already spells
  that, so layer names would be a second way to say something the language says.
- **Permitting `bar` as a layer:** requires a categorical-plus-continuous shared
  domain, which is the scale composition PRODUCT-0013 defers. The refusal is
  inherited, not chosen here.
- **Passing scatter's category through as a layer color:** PRODUCT-0013's
  compatibility rule requires `Absent`; a surface that forwarded it would be
  proposing the color-scale semantics that record deliberately deferred.
- **Restating PRODUCT-0013's compatibility rules in the envelope:** two copies of
  one rule, guaranteed to diverge, and it would make the envelope hold opinions
  about scales that PRODUCT-0003 keeps it ignorant of.
- **Deeper nesting:** costs the flatness that makes paint order a property of the
  form, and buys an expressiveness no motivating case wants.
- **`line("x","y").points()`:** methods on sink constructors are a new grammatical
  position — PRODUCT-0008's sugars are methods on relations, which already had
  methods — and the position invites the private options PRODUCT-0008 §2 prohibits.

## Consequences and sequencing

- PRODUCT-0013's substrate acquires its first production caller, and the arc that
  began with two panes showing the same points in two panels ends with one view
  that overlays them.
- `RuneViewForm` stops serving both routing and description, which PRODUCT-0010 §4
  anticipated when it declared form non-identifying.
- The envelope gains one constructor and one lowering, and no knowledge of scales,
  domains, or marks beyond which two names are admissible as layers.
- A future record may add marks to `LayerMark`, and each addition becomes
  spellable here with no surface change beyond the admissible-name set — which is
  the property that makes the two-record split worth its cost.
- Scale composition, guides, interaction, facets, cross-origin layering, and any
  sink-method sugar remain unaccepted, each inheriting this normal form.
