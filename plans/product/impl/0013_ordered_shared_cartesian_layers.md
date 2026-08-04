# Ordered shared-Cartesian plot layers

Scope: implement PRODUCT-0013. Add a layered plot specification beside
`AuthoredPlotSpec`, extract line and scatter mark renderers without changing what
they draw, and compose them into one ordered painting kernel over a shared
domain. Deliverable is four `impl:` commits, one per stage, each green on its own.
After those four, one separate `evidence:` commit records the gate ledger and
updates the corpus; it contains no implementation change.

**No Rune surface.** Nothing in this plan makes a layered plot reachable from a
buffer; PRODUCT-0014 supplies the first production caller. Until then the layered
path is exercised by tests only, which is a deliberate consequence of the
two-record sequence and not an oversight.

The arc is judged by PRODUCT-0013 §8's eight gates. Each stage names the gates it
discharges.

## Halt rule

PRODUCT-0013 §9 binds this implementation:

> This record may add a layered specification, per-layer encoding resolution,
> mark renderers for line and scatter, geometric hit-testing on those renderers,
> the §3 bridge, and an ordered painting composite. If the implementation
> requires a Rune surface change, a scale-composition rule, a guide or
> interaction decision beyond the §3 bridge, a change to any existing kernel's
> rendering, or a change to `AuthoredPlotSpec` or its consumers, the work stops
> and the requirement returns as its own record.

Concretely, the work stops if it requires editing `rune_relation_envelope.rs`,
anything under `gui/`, `editor.rs`, `plot/durable.rs`, `plot/theme.rs`,
`plot/composition.rs`, or any kernel other than `line.rs` and `scatter.rs`; if it
changes `AuthoredPlotSpec`'s shape or any of its consumers; or if it begins
unifying the two specifications. Stopping means reporting the finding.

### Where work may land

- `crates/polariton/src/plot/spec/mod.rs` — `LayeredPlotSpec`, `LayerSpec`,
  `NonEmpty`, compatibility resolution;
- `crates/polariton/src/plot/common.rs` — the `MarkRenderer` trait, beside
  `PlotKernel`;
- `crates/polariton/src/plot/kernels/line.rs` and `.../scatter.rs` — renderer
  extraction and delegation only;
- `crates/polariton/src/plot/kernels/layered.rs` — new composite;
- `crates/polariton/src/plot/kernels/mod.rs`, `crates/polariton/src/plot/mod.rs`
  — module registration and re-exports;
- `crates/polariton/src/plot/factory.rs` — lowering a `LayeredPlotSpec`;
- this plan;
- `plans/product/impl/0013_ordered_shared_cartesian_layers_evidence.md` and the
  corpus, in the final evidence-only commit.

Any other file is a halt. `plot/composition.rs` is deliberately excluded:
`PlotComposition` remains exactly as it is for `AuthoredPlotSpec`, and its
`add_layer` normalization and `can_add_layer_type` table simply have no
counterpart on the layered path.

## Stage 1: the layered specification and atomic compatibility

### Representation

```text
LayeredPlotSpec { layers: NonEmpty<LayerSpec> }
LayerSpec       { mark: LayerMark, encoding }
LayerMark       = Line | Scatter
```

Two things are unrepresentable by construction rather than refused at runtime,
because §8 gate 8 requires the first and §5's mark restriction is cheaper to hold
the same way:

- **`NonEmpty<T>`** is a first element plus a rest, not a validated `Vec`.
  `rune_relation_envelope.rs`'s `NonEmptySinks` is the existing precedent in this
  repository and the shape to follow.
- **`LayerMark`** is its own two-variant enum, not `PlotType`. `PlotType` admits
  radar, violin, candlestick, and the rest; using it would turn §5's "Cartesian
  line and scatter only" into a runtime check over a type that says otherwise.

`encoding` is an ordinary PRODUCT-0007 encoding. It can express color, which is
exactly why the compatibility rule must refuse it rather than stay silent.

### Compatibility resolution

Resolve every layer's channels through PRODUCT-0007 §2's total, non-substituting
resolution — unchanged, applied once per layer — then check the accepted rule:

- identical resolved x and y **field identities** across all layers;
- scales in the same family;
- color channel `Absent` on every layer; `Named` and `Auto` both refuse.

Failure is atomic and names the offending layer. No partial artifact is
constructed, and a failing layer never falls back to a sibling's binding — that
would be the substitution PRODUCT-0007 §2 forbids, arriving through a side door.

**Gates discharged: 3, 4, 5, 8.** Gate 4 needs its `Absent` control alongside the
two refusals, or it proves only that something refuses. Gate 8 is discharged by
the type, and the evidence should say so rather than adding a test that cannot
fail.

Before Stage 2 moves any drawing body, add deterministic line and scatter SVG
fixtures using today's kernels. Preserve their exact output as the pre-extraction
baseline for gate 6; a comparison performed only after both paths share one
implementation cannot prove that the extraction preserved today's rendering.

## Stage 2: mark renderers, extracted by delegation

### The trait

```text
MarkRenderer
    draw(backend, bounds, transform, settings)
    hit_test(transform, cursor) -> Option<MarkHit>

MarkHit
    cartesian position and mark-local geometric identity
```

It receives a transform and bounds it does not own, and it exposes geometry
without deciding what a hit means or how it is worded. `MarkHit` is not hover
content and must not contain a ready-made user-facing string. The §3 bridge
formats the existing line/scatter coordinate wording from the hit and the
composite's common axis types. The trait has no layout, legend, or axis labels.

### Extraction discipline

`LinePlotKernel` and `ScatterPlotKernel` keep their `PlotKernel` implementations,
public prepared-data fields, and existing hover behavior. Their mark-drawing
bodies **move** into the line and scatter renderer path, and the kernels delegate
only mark painting to that path. Geometric hit-testing is new support for the
layered composite; it must not retarget the existing kernels' hover contract.

Legend delegation does not enter `MarkRenderer`. Private line/scatter bridge
adapters expose the existing legend drawing code so the composite can invoke the
last prepared layer's output unchanged. Existing kernels and the bridge delegate
to the same legend helpers; a second legend implementation would violate the
bridge's “unchanged” rule even though legend behavior itself is non-normative.

This is the whole reason gate 6 is cheap: byte-identity holds because there is one
code path, not because two implementations were compared and found to agree. A
second parallel drawing implementation for the layered path is a halt — it would
be a change to an existing kernel's rendering the first time the two drifted.

`ScatterMarkRenderer` owns the `raster_cache: Mutex<Option<ScatterRasterCache>>`
and the Vector/Downsampled/Rasterized mode selection. Cache state belongs with
the marks it caches; leaving it only on the kernel would leave the layered path
uncached and make the two paths differ in behavior under exactly the large-data
case the cache exists for. `ScatterPlotKernel` retains its public
`prepared_data` field and privately owns the renderer/cache it delegates to; the
extraction must not force changes on a consumer.

**Gates discharged: 6.** One-layer `line` and one-layer `scatter` specifications
each lower to their existing kernel in this stage and retain the Stage 1 SVG
baseline byte-for-byte. This one-layer factory branch must land here, not in
Stage 3, or the commit claiming to discharge gate 6 has no layered lowering to
exercise.

## Stage 3: shared domain and the ordered composite

### The domain comes from source columns, before preparation

The composite resolves one Cartesian domain from the layers' common x and y
**source columns** before preparing either mark. Preparation may consume that
resolved domain, but it may not contribute a replacement domain back to the
composite.

It must not union prepared ranges. Today's preparation paths already demonstrate
why: `prepare_line_data` derives and pads its own ranges by 5%, while resolved
scatter preparation takes its range from `ResolvedLayout::from_spec`, whose
padding is 10%. Scatter's downsample/raster choice happens later, during drawing,
and its raster cache is keyed by the supplied transform. A union would therefore
let mark-specific preparation policy choose the shared scale, and later rendering
policy would consume a domain it indirectly helped define. PRODUCT-0013 instead
resolves the common channel domain once, before either policy runs.

### The composite

`LayeredPlotKernel` implements `PlotKernel`:

- `layout` — the shared Cartesian domain, computed once;
- `plot` — each renderer's `draw` in authored order, first to last;
- `hover` — layers searched back-to-front, with the first `MarkHit` formatted into
  the existing coordinate wording using the composite's common axis types;
- `draw_legend` — delegated to the last authored layer, unmerged;
- `x_label` / `y_label` — from the common bindings, which are identical by §2.

`hover` and `draw_legend` are §3 bridge behavior. Mark them as such in the source
so the later Guide and interaction record can find them. Axis labels are not part
of the bridge; they follow mechanically from the accepted common bindings.

### Multi-layer lowering

In `factory.rs`, extend Stage 2's one-layer branch so that two or more layers
construct the composite. **Do not unify these branches.** The one-layer branch
exists so that gate 6 is about today's kernels and so PRODUCT-0007's landed
evidence is never rerouted.

**Gates discharged: 1, 7.** Gate 1 asserts authored order survives resolution into
the composite with no normalization step anywhere between; gate 7 asserts the
shared domain derives from source columns and matches an independently computed
column range, not a union of prepared ranges.

## Stage 4: painting order, bridge coverage, regression

### Gate 2 is the record's reason for existing

Render a two-layer `[line, scatter]` specification through `SvgBackend` and assert
the scatter marks appear **after** the line path in document order.

A test that inspects `layers` does not discharge this gate. The vector was already
almost right; the defect lives between the vector and the canvas, and today's
`ScatterLinePlotKernel` paints these two in the opposite order, so a passing
vector assertion would coexist with a wrong picture.

### Bridge coverage is not a gate

Cover the delegated legend and the topmost-hit hover with ordinary tests, named so
they are recognizable as bridge coverage. They pin current behavior against
accidental drift and make no promise; a later record replaces them without
amending PRODUCT-0013. Record them in the evidence file as bridge coverage rather
than as gate evidence.

### Regression

Every existing plot test passes unmodified. `ScatterLinePlotKernel` keeps its
tests and its behavior, including its current paint order — it is not retired and
not corrected here. No test cited by an `executable` corpus row changes.

**Gates discharged: 2.**

## Non-goals

- No Rune syntax, no envelope change, no panel or dashboard change.
- No new mark, no facet, no dual axis, no scale composition, no guide merging, no
  interaction policy beyond the bridge.
- No change to `AuthoredPlotSpec`, `PlotComposition`, `durable.rs`, `theme.rs`, or
  any kernel other than the line and scatter extractions.
- No retirement of `ScatterLinePlotKernel`, and no correction of its paint order.
- No unification of the layered and single-plot specifications.

## Verification

```text
cargo fmt --all -- --check
cargo test -p polariton --features toron
cargo check -p polariton --no-default-features
```

The evidence-only commit maps each of the eight gates to its behavioral or
structural evidence, lists bridge coverage separately as non-gating, records that
the halt rule did not fire, and updates the corpus without altering Rust code.

## Review checklist

- [ ] `NonEmpty` and `LayerMark` make emptiness and unaccepted marks
      unrepresentable; neither is a runtime check.
- [ ] Per-layer resolution is PRODUCT-0007 resolution applied once per layer, with
      no substitution or fallback between layers.
- [ ] Color refuses for both `Named` and `Auto`, with an `Absent` control.
- [ ] Compatibility failure is atomic and names the offending layer.
- [ ] Line and scatter kernels **delegate** to extracted renderers; no second
      drawing implementation exists.
- [ ] `MarkHit` carries geometry rather than hover prose, and existing
      single-kernel hover behavior is unchanged.
- [ ] The scatter raster cache moved with the marks.
- [ ] The shared domain derives from source columns before preparation, never from
      a union of prepared ranges.
- [ ] Paint order is authored order, proved through SVG document order.
- [ ] One-layer specifications lower through the existing path, and the branches
      are not unified.
- [ ] Bridge behavior is marked as such in source and listed as non-gating in
      evidence.
- [ ] The bridge legend invokes the last authored layer through the same
      line/scatter legend helpers used by existing kernels.
- [ ] `PlotComposition`, `AuthoredPlotSpec`, and every other kernel are untouched.
- [ ] Corpus updated in a separate commit, not in any of the four.
