# Implement the plot specification contract

Scope: implement PRODUCT-0007 in three vertical stages — authored/resolved
artifacts and the resolver; kernels narrowed to consume only resolved channels;
then surface lowering and durability — with a baseline-capture commit between the
first two.

Four commits, in this order:

```text
impl: adr 0007 plot specification artifacts and resolver     (stage 1)
test: adr 0007 capture plot thermal baseline                 (stage 1b)
impl: adr 0007 kernels consume resolved channels             (stage 2)
impl: adr 0007 plot surface lowering and durability          (stage 3)
```

Stage 1b is a `test:` commit that changes no behavior. It is separate because
"captured before any kernel edit" must be visible in the history rather than
asserted in prose.

The staging is deliberate about where the scatter substitution actually closes.
Stage 1 builds the artifacts and proves the resolver at its own boundary; the
rendered defect persists until stage 2 removes the inference that causes it.
Anyone reading stage 1 as a fix will be disappointed, so it says so.

## Stage 1 — artifacts and the channel resolver

### Types

New module `plot/spec/`, product-side, depending on no kernel and no engine.

```text
AuthoredPlotSpec   requested field, declared-or-automatic semantic type,
                   scale policy, styling; portable across data
ResolvedPlotSpec   exact field identity, schema-derived semantic type,
                   chosen scale family, domain decisions, provenance
ResolvedLayout     viewport pixel ranges, ticks, geometry
```

Supporting vocabulary:

- `AuthoredChannel { Named(String), Auto }` and
  `OptionalAuthoredChannel { Named(String), Auto, Absent }` — omission is not
  representable, so authority is always written down;
- `SemanticType { Nominal, Ordinal, Quantitative, Temporal }`;
- `ScaleFamily { Point, Band, Linear, Log, Time, … }`, admissible sets declared
  per geometry;
- `Override<T> { Inherit, Set(T) }` and `Presence<T> { Inherit, Show(T), Hide }`,
  each with a right-biased `overlay` whose unit is `Inherit`;
- `ColorJob { Categorical, Sequential, Diverging, Status }`;
- `ColorIdentity` — per-plot, monotonic, an *input* to domain resolution.

Property tests prove the cascade is a monoid: `overlay` is associative and
`Inherit` is a two-sided identity. A synthetic non-associative implementation is
the positive control.

### The resolver is two phases

Domains and categorical identity require values, not just a schema, so a
single schema-only entry point cannot produce a `ResolvedPlotSpec`:

```text
resolve_channels(&AuthoredPlotSpec, &Schema)
    -> Result<ChannelBinding, ChannelError>

resolve_domains(ChannelBinding, &StampedResult, &ColorIdentity)
    -> Result<(ResolvedPlotSpec, ColorIdentity), DomainError>
```

`ChannelBinding` holds field identity, semantic type, and scale family. It is
deliberately **not** called a specification and is not accepted by any kernel.
Only the output of the second phase is a `ResolvedPlotSpec`.

Channel-phase rules, each with a test:

- a `Named` field that is absent, or whose semantic type is incompatible with the
  channel, or whose scale is inadmissible for the geometry, is a `ChannelError`
  naming channel, requested field, and reason;
- a `Named` channel never resolves to a field other than the one named;
- the resolver never originates a repetition by copying a resolved field into a
  channel the author did not name it for;
- `Auto` is the only state that authorizes inference; `Named` and `Absent` never
  infer;
- every inferred outcome is written into the resolved spec explicitly;
- no phase inspects values to decide what a field means.

Domain-phase rules:

- the display domain derives from the stamped result;
- `ColorIdentity` is threaded in and returned possibly extended, never smaller —
  values are assigned on first sighting and never removed or reordered;
- the returned `ResolvedPlotSpec` carries the **exact category-to-color
  assignments** needed to render, so a renderer never consults `ColorIdentity`
  and a retained spec is self-sufficient;
- `ColorIdentity` is keyed by resolved field and layer identity plus category
  value; changing the categorical field starts a new lineage rather than
  inheriting a prior field's assignments;
- exhausting the geometry's declared categorical cardinality is a `DomainError`
  naming the channel and the limit. Nothing is evicted, reassigned, or cycled.

**First acceptance fixture.** `scatter("cat", "val")` over a frame of
`cat: String, val: Float64` resolves `cat` → nominal → point and `val` →
quantitative → linear. It asserts resolved channels **by field name**, so a
resolver that substituted or sniffed fails it.

**Deliberate repetition works.** `scatter("val", "val")` resolves `val` to both
channels. This is the companion to the substitution tests and stops them from
being satisfied by a blanket uniqueness rule, which would forbid a legitimate
authored request.

**Monotonic identity.** Two successive domain resolutions — the second over a
filtered subset — leave every surviving category on the color it had. A control
that rebuilds identity from the current result must fail this test.

**Identity lifecycle.** Three further fixtures: a resolved spec renders with no
access to `ColorIdentity`; changing the categorical field starts a new lineage
rather than inheriting the previous field's assignments; and exceeding the
declared cardinality yields a `DomainError` naming channel and limit, with a
control proving no reassignment or cycling occurs at the boundary.

### Producers build authored specs

Both producer paths construct an `AuthoredPlotSpec` and carry the resolved spec
forward:

- the local Rune host, `rune_host.rs:316` `plot_scatter` and `:353` `plot_show`,
  which already has explicit columns in `PlotRequest.projection`
  (`plot/request.rs:25`);
- the relation fan-out path, `gui/state.rs:636-677` `rune_view_outcome`, which
  projects `[x, y, category?]` at `:649-658`.

`ResolvedPlot` (`plot/request.rs:35`) gains the resolved specification. This is
the structural correction: today it holds `source_name`, `composition`, and
`data`, so channel identity survives only as column order, which is precisely
why `factory.rs` re-infers it.

`plan` (`plot/request.rs:47`) keeps its projection behavior; its three existing
tests (`:82`, `:95`, `:113`) must pass unchanged.

### Failure attribution

A `ChannelError` reaching the relation path fails **one sink at its Rune span**
and leaves siblings rendering, reusing the isolation proved by
`bad_plot_field_fails_only_its_spanned_sink_while_table_survives`
(`gui/state.rs:2135`). A test asserts a two-sink program where one channel fails.

### Stage 1 acceptance

Resolver-level. The rendered output is unchanged, and the scatter substitution is
still present because `factory.rs` still infers. Stage 1 is complete when the
resolver is proven and every producer carries a resolved spec that nothing has
yet consumed.

## Stage 1b — capture the thermal baseline

**Its own preceding commit: `test: adr 0007 capture plot thermal baseline`.**
"Before kernel edits" is not demonstrable inside the commit that also edits
kernels; a separate commit is what makes the ordering reviewable in the history
rather than asserted in prose.

There are no benches in the crate and no test covers the scatter thresholds
`scatter_max_vector_points: 50_000`, `scatter_downsample_target: 200_000`,
`scatter_raster_threshold: 1_000_000` (`plot/common.rs:331-333`) or
`resolve_render_mode` (`kernels/scatter.rs:407`). Capture mode selection at each
threshold boundary and the downsample grid's output size, against today's
behavior, with no change to any of it.

## Stage 2 — kernels consume only resolved channels

### Narrow the input contract

Kernels take a `ResolvedPlotSpec` and a frame. They do not choose channels.

Delete the dtype-partition inference at `plot/factory.rs:45-56` and every
`unwrap_or(previous)` chain that depends on it — Scatter (`:102-108`), Line
(`:74-76`), Bubble (`:153-156`), RadialDial (`:193-194`), Candlestick
(`:201-205`), FillBetween (`:229-232`), scatter-with-line (`:254-256`), and
fill-between-with-line (`:269-272`).

A guard asserts no kernel or factory source partitions columns by dtype or
selects a channel positionally, with a synthetic violating source as the positive
control, citing PRODUCT-0007.

### Scatter gains a categorical position scale

`kernels/scatter.rs:43-56` always returns `PlotLayout::Cartesian`, so scatter has
no categorical-x path at all; `prepare_scatter_data` (`:473`) casts x and y to
`Float64` unconditionally. Scatter must accept a nominal or ordinal x resolved to
a point scale, as bar already does via `PlotLayout::CategoricalX`
(`kernels/bar.rs:25-42`).

`AxisType` (`plot/common.rs:24`) and `polars_type_to_axis_type` (`:1627`) are
dtype-derived and become downstream of the resolved scale rather than a parallel
inference. Domain currently lives inside each `prepare_*` (scatter's padded range
at `:504-509`); domain decisions move into `ResolvedPlotSpec`, and `PlotLayout`
becomes a projection of resolved scale plus viewport — the `ResolvedLayout` role.

### Color identity

Series color binds to the resolved scale domain, not render order. A test filters
a relation to a subset of categories and asserts every surviving series keeps its
original color. `ColorJob` selects among the existing ramps (`plot/colors.rs:4`,
37 variants); a job mismatch is a specification error.

### Stage 2 acceptance

The end-to-end fixture that closes the defect: `plot.scatter(r, "cat", "val")`
renders `cat` on x with a point scale and `val` on y with a linear scale. Note
that `live_rune_relation_reaches_table_bar_and_scatter_surfaces`
(`adapters/toron/mod.rs:152`) asserts only `svg.contains("<path")` and would not
have caught this; it is strengthened to assert axis identity.

## Stage 3 — surface lowering and durability

### Surface

Gated: PRODUCT-0007 defines what a spelling produces but accepts none. If a
record accepts the corpus's `.views([...])` proposal (v0.8), lowering targets
`AuthoredPlotSpec` and the corpus's equivalence obligations are re-proved through
surface syntax against `EnvelopeSemantics`, which already compares the right
shape. If no spelling is accepted, stage 3 ships durability alone.

### Theme

`PlotThemeSpec` is introduced as a separate artifact. The 37 ramps
(`plot/colors.rs:45`) do not move into `UiPalette`
(`libaryon/src/engine/ui_colors.rs:495`); a bridge maps chrome roles only —
background, axis, gridline, label. `PlotSettings` (`plot/common.rs:221-336`)
currently mixes theme, typography, padding, ticks, grid, and scatter tuning in
one struct and is decomposed along the cascade's five layers.

Default themes are validated at build time against the project's visualization
guidance checks; a default failing contrast or ordering fails the build.

### Durability

- **live** — re-runs both resolution phases after every execution, following
  data, schema, and current revision;
- **pinned** — retains `ResolvedPlotSpec`, recomputes `ResolvedLayout` on resize.

Tests:

- a pinned plot survives a viewport change with identical resolved scales and a
  recomputed layout;
- a **live** plot whose source column changes `Int64` → `String` re-resolves from
  a linear to a point scale — this is the live case, not a pinned failure;
- a **pinned** plot is unaffected by a later current revision, because a pin names
  an exact revision and later ones are irrelevant to it;
- a pinned plot whose **pinned revision** can no longer be fetched or verified
  renders the last stamped result with an explicit stale or unverifiable banner
  (PRODUCT-0002 §7) and does not re-resolve against whatever is current;
- a pinned plot resolved under one set of inference defaults keeps its resolved
  scales when those defaults change, which is the drift pinning exists to stop.

Saved authored specs and durable plots cannot ship before the workspace artifact
store record exists (PRODUCT-0002). Stage 3 implements the behavior against
session-scoped artifacts and stops at persistence.

## Cross-stage guards

- Product-side only: `plot/spec/` depends on no engine crate, and the existing
  architecture tests continue to pass.
- No presentation identifier is usable as a relation handle; pane, layer, and
  view identity stay product-local.
- No local data transformation: binning, aggregation, and calculated fields lower
  into relation operations before a specification exists. A guard asserts the
  spec module performs no frame computation beyond projection, with a positive
  control.
- Every negative guard carries a synthetic violating control and cites
  PRODUCT-0007 on failure.

## Non-goals

- No new geometry; the 20 `PlotType` variants (`plot/core.rs:2`) are unchanged.
- No dual Cartesian y axis, in any stage.
- No deferred relations; `PlotSource::Materialized` (`plot/request.rs:19`)
  remains v1.
- No workspace artifact store, and no persisted authored specs or durable plots.
- No pane-override round-trip policy; PRODUCT-0007 leaves it to the editing
  model, and it is not settled here.
- No billion-point rendering work; the thermal fixtures exist to protect the
  current baseline, not to move it.
- No change to Toron, `toron-client`, the envelope, or the fan-out scheduler.

## Review checklist

**Stage 1**

- [ ] Three artifacts exist with distinct responsibilities; `ResolvedPlot` carries
      the resolved specification.
- [ ] Resolution is two-phase; `ChannelBinding` is never called a specification
      and no kernel accepts one.
- [ ] Channels are `Named | Auto | Absent`; omission is unrepresentable, and
      legacy automatic plots lower to explicit `Auto`.
- [ ] Every failure is a typed error naming channel, field, and reason.
- [ ] No substitution, no resolver-originated repetition, no value sniffing, no
      inference for `Named` or `Absent` — each asserted by field name.
- [ ] `scatter("cat","val")` resolves nominal/point and quantitative/linear;
      `scatter("val","val")` resolves both channels to `val`.
- [ ] `ColorIdentity` grows monotonically across two resolutions, with a
      rebuild-from-result control that fails.
- [ ] The resolved spec carries exact category-to-color assignments and renders
      without consulting `ColorIdentity`.
- [ ] Changing the categorical field starts a new lineage; exceeding declared
      cardinality is a `DomainError`, with nothing evicted, reassigned, or cycled.
- [ ] Cascade monoid laws proved with a non-associative positive control.
- [ ] A channel failure fails one sink at its span; siblings render.
- [ ] `plan`'s three existing tests pass unchanged.

**Stage 1b**

- [ ] Thermal baseline lands as its own commit, before any kernel edit, changing
      no behavior.

**Stage 2**
- [ ] All dtype-partition inference and `unwrap_or(previous)` chains deleted;
      guard passes with its positive control.
- [ ] Scatter accepts a categorical x resolved to a point scale.
- [ ] Domain decisions live in the resolved spec; `PlotLayout` is derived.
- [ ] Filtering does not recolor surviving series.
- [ ] The end-to-end scatter fixture asserts axis identity, not path presence.

**Stage 3**

- [ ] `PlotThemeSpec` is separate from `UiPalette`; only chrome roles bridge.
- [ ] `PlotSettings` is decomposed along the five precedence layers.
- [ ] Default themes pass the build-time visualization checks.
- [ ] Pinned plots keep resolved scales across resize and across a changed
      current revision, and across changed inference defaults.
- [ ] Staleness for a pinned plot means its pinned revision is unavailable or
      unverifiable, never that a later revision differs.
- [ ] Live plots re-resolve after every execution, including the `Int64` →
      `String` scale change.
- [ ] Surface lowering lands only if a record accepts a spelling; otherwise
      durability ships alone.
