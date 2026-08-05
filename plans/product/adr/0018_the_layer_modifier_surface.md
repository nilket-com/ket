# ADR 0018: the layer-modifier surface

Status: accepted 2026-08-04. This record decides one thing: Rune mark forms may
carry settings from PRODUCT-0007's Layer stage through a closed postfix modifier
register, whose first member is `point_size` for scatter marks. It lowers only
into PRODUCT-0017's proved `LayerSettings`; it adds no rendering, cascade,
identity, or layout semantics.

## Context

PRODUCT-0017 made relative scatter size a typed layer-local setting and proved
it through vector, downsampled, raster, cache, and geometric-hit paths. It
deliberately added no Rune spelling. Consequently the intended source

```rune
scatter("x", "y").point_size(2.0)
```

currently reaches PRODUCT-0014's layer parser as one call whose callee is a
field access and refuses as an unknown layer.

PRODUCT-0007 §5 rejects terminal verbs with accumulating private options, but
also names Layer as a governed grammar stage. PRODUCT-0017 now supplies that
stage with a typed, flat register and a five-tier precedence slot. This is new
evidence: a postfix setting is no longer private scatter vocabulary if its name,
type, validation, and lowering are owned by the closed Layer register.

## Decision

### 1. Mark forms admit a closed postfix LayerModifier register

```text
MarkForm := MarkConstructor ("." LayerModifier)*
LayerModifier := point_size(NumericLiteral)
```

The first accepted spelling is:

```rune
layers([
    line("x", "y"),
    scatter("x", "y").point_size(2.0),
])
```

The register is closed. An unknown postfix method refuses; it is never retained
as a runtime method call after its receiver has been recognized as a mark in a
proven presentation position. Figure metadata, encodings, identity, composition,
paint order, Scale-stage vocabulary, and Guide-stage vocabulary cannot enter the
register. In particular, `.title(...)` refuses rather than moving figure-wide
metadata onto a mark.

The word is `point_size`, not `point_scale`: Scale is already a distinct
PRODUCT-0007 grammar stage, while this value is Layer-stage mark styling.

### 2. Recognition remains contextual and atomic

A modified mark is recognized only where the unmodified constructor already has
a proven role:

- as a sink in a proven `.views([...])` list;
- as the sole body of `figure([...])`; or
- as a mark inside `layers([...])`.

Everywhere else the same call remains ordinary Rune. Once a mark root is proven,
the complete postfix chain either lowers or the enclosing view refuses. No
prefix, modifier node, or partial view survives a refusal.

Postfix syntax does not add a semantic tree level. The chain flattens before the
normal form, so PRODUCT-0016's bounded
`view -> figure -> layers -> mark` tree remains exact.

### 3. `point_size` is a unitless relative linear factor

The argument is one numeric literal. It is passed through
`PointSize::new`; PRODUCT-0017's exact finite `0.25..=8.0` domain is the only
validation rule. `1.0` preserves the established three-pixel radius, and `2.0`
selects a six-pixel radius. The surface neither clamps nor interprets the value
as pixels or area.

Wrong arity, a computed expression, a non-number, and an invalid factor refuse
at the modified mark. The diagnostic reports the accepted domain.

### 4. The chain flattens into one typed layer

```text
scatter(x, y).point_size(2.0)
  -> LayerSpec {
       mark: Scatter,
       encoding: xy(x, y),
       settings: LayerSettings {
         point_size: Set(PointSize(2.0)),
       },
     }
```

No modifier AST reaches execution. `LayerSettings` stays typed and flat, not a
string-keyed bag or nested decorator tree. Modifier order is semantically
irrelevant. Repeating the same modifier refuses instead of creating an
order-sensitive last-wins language.

`point_size` is accepted only for a two-field scatter mark in this cut.
`line(...).point_size(...)`, a modified three-field category scatter, and any
other mark refuse at the modified mark. This is the surface's closed
mark/modifier admissibility rule; it does not copy PRODUCT-0013's resolved
field, scale, or color compatibility rules.

### 5. A modified single mark remains a single view

A modified scatter may appear directly in `.views([...])` or as a figure body.
It uses the layer-aware one-layer renderer so its setting cannot be discarded,
but remains one sink, one panel, and one identity. Its structural display label
is exactly `plot.scatter`, matching the bare single-mark spelling. Inside an
authored `layers([...])` composition, the established ordered composition label
remains unchanged.

The envelope may carry a styled-single body distinct from the existing
disposable single-plot form, then construct a one-layer `LayeredPlotSpec` at the
existing output handoff. That carrier is presentation-normal-form plumbing; it
does not add another plot specification or kernel.

### 6. Identity and all other stages are unchanged

Adding, removing, or changing `point_size` does not alter named or positional
view identity, content kind, presentation order, paint order, title/subtitle,
chrome, canvas fallback label, panel placement, guide output, or execution
grouping. It changes only PRODUCT-0017's layer setting.

The dashboard remains untouched. No modifier value is sent to Toron: the
envelope prunes presentation syntax before dispatch and applies the setting to
the stamped result's plot artifact afterward.

## Acceptance gates

1. `scatter("x", "y").point_size(2.0)` lowers to one scatter `LayerSpec`
   whose encoding is unchanged and whose point size is exactly `Set(2.0)`.
2. The spelling works in a `layers` list, directly as a view sink, and as a
   figure body; all three use the layer-aware path and one stamped result.
3. The complete `0.25..=8.0` domain is inherited from `PointSize`; zero,
   negatives, out-of-range, NaN/infinity spellings, computed values, and wrong
   arity refuse without clamping.
4. An unknown modifier and `.title(...)` refuse at the modified mark; no
   modifier call reaches Toron or retained source from a recognized view.
5. Duplicate `.point_size(...)` refuses atomically rather than selecting by
   chain order.
6. `line(...).point_size(...)` and a modified three-field scatter refuse, while
   their established unmodified controls retain their prior behavior.
7. The same method call outside a proven presentation position remains ordinary
   Rune and is not recognized or rewritten by the envelope.
8. A factor of `1.0` renders identically to the bare scatter control, and `2.0`
   renders six-pixel-radius points through the production host-panel path.
9. Changing only point size preserves view identity and exact structural label;
   named and anonymous destinations are replaced in place.
10. Existing layered order, figure headings, execution sharing, guide output,
    panel grouping, stale behavior, and no-dashboard behavior remain unchanged.

## Stop condition

> This record may add the closed postfix recognizer, numeric-literal decoding,
> a styled-single envelope carrier, and lowering into PRODUCT-0017's existing
> `LayerSettings`. If implementation requires a new renderer rule, cascade
> algebra, plot specification, Toron syntax or transport, computed modifier
> evaluation, an open modifier map, a figure-wide size default, identity or
> placement change, guide behavior, or dashboard edit, the work stops.

The expected trigger is trying to make the runtime evaluate the modifier. It is
presentation syntax and must be pruned with the rest of the view envelope.

## Not authorized

No `layer([...])` constructor, wrapper spelling, figure-wide point size,
data-driven size encoding, absolute pixels, area scaling, legend sizing, pane
override surface, arbitrary method forwarding, or modifier on bar/line. No
change to the accepted bounds or PRODUCT-0017 renderer behavior.

## Rejected

- **`point_size(scatter(...), 2.0)`:** decorator nesting grows with every
  setting and makes semantic depth a function of option count.
- **`layer([point_size(...), scatter(...)])`:** adds ceremony and a fifth
  semantic level when the target is already supplied by the receiver.
- **An extra scatter argument:** places Layer vocabulary in a mark's encoding
  list and recreates the accumulating-verb ceiling PRODUCT-0007 rejects.
- **`point_scale`:** collides with the accepted Scale stage.
- **Open method forwarding:** turns a governed register back into private,
  runtime scatter options.
- **Last duplicate wins:** makes postfix order semantic even though the flat
  normal form has no chain order.

## Consequences

- The intended source becomes dense without becoming an ungoverned options API.
- PRODUCT-0016's semantic depth bound remains unchanged.
- A future Layer-stage setting can extend the closed register without changing
  the grammar or normal-form shape.
- A future figure-wide default may use the authored tier; the existing cascade
  already makes a per-layer modifier override it. That surface is deferred.

