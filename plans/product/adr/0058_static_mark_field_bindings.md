# ADR 0058: static mark-field bindings

Status: accepted 2026-08-08. This record closes DB-03 by applying
PRODUCT-0040's accepted static-literal rule to every established mark-field
position. A bound field resolves to its concrete string in the envelope before
schema-based channel and bar-pair resolution. It adds no evaluation, mark,
encoding, modifier, or product pixel. Implementation evidence is recorded in
`../impl/0058_static_mark_field_bindings_evidence.md`.

## Context

PRODUCT-0040 made a general decision: a literal in a recognized presentation
position may be replaced by a preceding top-level name that resolves directly
or through aliases to the required literal kind. Its implementation cut stopped
at figure elements and named mark arguments as a coverage limit rather than a
semantic boundary. DB-03 retained that exact extension.

The first dogfood use has arrived. `test16.rn` repeats the same categorical,
quantitative, and series fields across its grouped and faceted bars. Attempting
to name the repeated series field produced the current envelope refusal:

```rune
let category_field = "region";
let value_field = "revenue";
let series_field = "product";

bar(category_field, value_field, series_field)
```

The existing static resolver is sufficient. `analyze` already collects every
top-level `LocalBinding` before it classifies output roots.
`resolve_static_literal` already implements most-recent-preceding selection,
alias traversal, cycle refusal, literal-kind discrimination, and attributed
diagnostics. Resolved bindings remain in the Rune program. The missing work is
that seven established surface positions still terminate in parsers that
decode mark fields directly:

```text
legacy plot.<mark>(relation, fields...)
relation.<mark>(fields...) sugar
canonical views([<mark>(fields...)])
named view("name", <mark>(fields...))
figure([<mark>(fields...)])
modified standalone <mark>(fields...).modifier(...)
layers([line(fields...), scatter(fields...)])
```

This would have remained a mechanical coverage extension before
PRODUCT-0055. It now carries one phase-ordering consequence. Rune authors one
`bar(x, y, series?)` family, and the returned schema selects `Bar` or
`HorizontalBar` from the semantic x/y pair. A bound first or second field
therefore participates in choosing orientation. Sending the binding identifier
into channel resolution would ask the schema for a column named
`category_field`; evaluating the binding after channel resolution would be too
late to repair the refusal. The concrete string must exist before the authored
plot specification reaches the schema.

The normal form already makes that boundary natural. The envelope lowers mark
fields into owned strings in `DisposableRunePlotSink` or `LayerSpec`.
`AuthoredPlotSpec` is built from those strings, and `resolve_channels` later
looks them up in the completed relation schema. No downstream type needs to
carry a binding reference, and no schema or data value is available or needed
at static resolution time.

Register-argument bindings are adjacent but independent. Table skin names now
resolve because their surface repair needed them; numeric mark modifiers such
as `.point_size(size)` still require inline literals. DB-03 is about the field
arguments that name channels, not every literal inside a modified mark.

## Decision

### 1. Every established mark-field slot accepts a static string binding

Wherever `bar`, `line`, or `scatter` accepts an inline string field today, it
also accepts a top-level name that resolves under PRODUCT-0040:

```rune
let x = "revenue";
let y = "region";
let series = "product";

plot.bar(relation, x, y, series)
relation.bar(x, y, series)
relation.views([bar(x, y, series)])
relation.views([view("bars", bar(x, y, series))])
relation.views([figure([bar(x, y, series).category_gap(0.2)])])

let time = "month";
let value = "revenue";
relation.views([layers([line(time, value), scatter(time, value)])])
```

The rule covers x, y, and the optional color/series/category field in every
standalone form. Shared-Cartesian layers cover their accepted x/y fields. It
does not add a third field to a layer, admit bar into `layers`, or create a new
mark position. Coverage follows accepted syntax rather than widening it.

The literal and bound spellings lower to exactly equal plot intents. The
binding's source name is factoring syntax, not channel identity, description,
lineage, or provenance. Only its resolved string crosses the envelope.

### 2. Static field resolution is a phase boundary

Mark lowering follows this order:

```text
1. recognize constructor, context, arity, and modifier-register shape
2. resolve every accepted field argument to a concrete string literal
3. build DisposableRunePlotSink or LayerSpec from those strings
4. dispatch and complete the relation
5. resolve exact schema fields and semantic types
6. for bar, resolve the x/y semantic pair to Bar or HorizontalBar
7. resolve domains, colors, geometry, guides, and rendering
```

Step 1 remains source-structural. A wrong arity or forbidden modifier refuses
without attempting to reinterpret the call through bindings. Step 2 is static
lookup only: it executes no Rune and has no schema. After step 3, downstream
code cannot distinguish
`let field = "region"; bar(field, "revenue")` from
`bar("region", "revenue")`.

This is the only new phase pin. Every rule after it remains owned by its
existing record. In particular, PRODUCT-0055's pair resolver receives the
resolved field strings and returned semantic types unchanged; this record does
not add an orientation branch or inspect dtypes in the envelope.

### 3. Bound bar fields participate in orientation without entering identity

These are exact equivalents:

```rune
let value = "revenue";
let category = "region";
bar(value, category, "product")

bar("revenue", "region", "product")
```

After relation completion, both select canonical `HorizontalBar` because x is
quantitative and y is non-quantitative. Reversing the resolved strings selects
vertical `Bar`. The authored family remains `Bar`; its display label,
family-keyed `LayerIdentity`, and color lineage remain independent of
orientation per PRODUCT-0055.

An invalid pair refusal must name the concrete fields and their returned
semantic types. For example, bindings resolving to `revenue` and `profit`
produce PRODUCT-0055's complete-sink reason with x field `revenue` and y field
`profit`. That observable diagnostic is the phase-order gate: it cannot be
satisfied by passing only the identifiers into schema resolution or by
resolving them after the pair rule.

The diagnostic may additionally name the authored binding as source
provenance, for example `x_field` → `revenue` (`Quantitative`). That is useful
when the complete-sink span highlights a call containing only factored names.
The resolved field and semantic type are mandatory; the binding identifier may
explain how the source reached them but may not replace either fact. This
permission does not retain binding identity in `AuthoredPlotSpec` or give it
channel, lineage, or lifecycle meaning.

### 4. Static and schema refusals retain separate attribution

Failure before a concrete string exists follows PRODUCT-0040:

- an unbound name, computed initializer, arithmetic expression, alias cycle,
  or numeric literal binding refuses during envelope analysis;
- the refusal attributes to the exact field argument span;
- a named binding is included in the diagnostic, together with the wrong
  literal kind or nonliteral resolution where available.

Once the static string exists, the inline field's existing rules take over. A
binding resolving to `"missing"` passes the envelope and later receives the
same exact missing-channel refusal and sink attribution as inline
`"missing"`. Categorical compatibility, pair validity, explicit scale
compatibility, palette capacity, and sink atomicity are unchanged.

This division matters: static resolution proves what field name the author
wrote; channel resolution proves whether the returned relation can supply and
render it. A schema diagnostic may echo the authored binding as supplementary
provenance, but its controlling reason remains the resolved field and schema
fact. That echo is reconstructed only when the diagnostic is rendered, from
the attributed source span and the envelope's existing static lookup; no
binding identifier crosses the envelope inside `AuthoredPlotSpec`,
`ChannelBinding`, or a resolved channel.

### 5. Binding lookup and retention are inherited unchanged

Direct aliases, transitive aliases, most-recent preceding shadowing, cycle
refusal, top-level scope, and use-site attribution are PRODUCT-0040's exact
rules. Constructor-name collisions remain positional:

```rune
let bar = "region";
relation.views([bar(bar, "revenue")])
```

The first `bar` is the recognized constructor and the second is a field
binding. No `ViewShadows` widening is needed.

Literal bindings remain in the program sent to Toron. A field binding may also
be ordinary relation-program input, and proving it presentation-only would
require the whole-program analysis PRODUCT-0040 rejected. Editing a binding
and successfully re-running the source is equivalent to editing each inline
literal it factors; no new live runtime binding or artifact lifecycle exists.

### 6. The boundary is mark fields only

This record does not cover:

- numeric mark-register arguments such as `.bar_width(width)`,
  `.category_gap(gap)`, `.stroke_width(width)`, or `.point_size(size)`;
- a bound `view(...)` identity or a binding for an entire mark, figure, layer
  list, or modifier chain;
- computed strings, concatenation, calls, constant folding, mutation, or
  non-top-level bindings;
- runtime values returned by Rune or column names chosen from relation data;
- any new mark arity, modifier, layer admission, scale, or channel.

Register-argument bindings remain their separately recorded coverage item. A
use for one can trigger the same analysis without making DB-03 own it
preemptively.

## Implementation sequence

1. Add one mark-field helper over `resolve_static_literal` that requires a
   string result and emits PRODUCT-0040's argument-attributed diagnostic.
2. Thread the already-collected `LocalBinding` slice through legacy root,
   relation-sugar, contextual, figure-body, modified-mark, and layered parsers.
   Replace each direct field-literal decoder with the helper; do not add a
   second resolver or retain binding names in the normal form.
3. Bank literal-versus-bound intent equality across the complete position and
   mark matrix, then drive bound vertical, horizontal, and invalid bar pairs
   through production completion.
4. Refactor `/home/me/work/uat/test16.rn` to bind its repeated category, value,
   and series field names once. Confirm the source-only factoring produces the
   same completed figures and pixels.
5. Update the language-as-built, corpus, and deferred ledger to close DB-03
   while leaving register-argument bindings open.

The implementation is one zero-pixel commit. There is no substrate migration
or baseline premise to bank separately: the concrete-string normal form and
all downstream behavior already exist.

## Acceptance gates

1. A table-driven envelope matrix proves inline and bound fields lower to
   exactly equal intents for bar, line, and scatter at every accepted two- and
   three-field standalone spelling: legacy `plot.<mark>`, relation sugar,
   canonical view, named view, figure body, and modified mark. Every field
   ordinal is exercised as a binding rather than only the optional third.
2. Bound x/y fields in unmodified and modified `line` and `scatter` members of
   `layers([...])` lower to the same `LayerSpec` as inline literals. A third
   field, bar layer, nested layers, and every existing layer-context refusal
   remain unchanged.
3. Before relation execution, a bound bar intent contains concrete x
   `"revenue"`, y `"region"`, and series `"product"`, with no authored binding
   identifier retained. After completion against an asymmetric schema, it
   canonicalizes to `HorizontalBar`; reversing the two binding initializers
   canonicalizes to `Bar`.
4. A bound two-quantitative bar pair reaches PRODUCT-0055's complete-sink
   refusal. Its exact reason names resolved fields `revenue` and `profit` and
   their semantic types. Binding identifiers may appear as supplementary
   source provenance but cannot replace those concrete facts. Missing and
   wrong-dtype single-field failures retain their existing channel and sink
   attribution. Even when the rendered diagnostic shows an identifier,
   `AuthoredPlotSpec`, `ChannelBinding`, and every resolved channel are asserted
   to contain only the resolved field string; the renderer reconstructs the
   echo from the attributed source span rather than semantic transport.
5. Bound third fields on bar, line, and scatter lower to the same exact named
   color channel as inline strings. Resolved colors, category/series domains,
   legends, facets, capacity, and family-keyed lineage are byte-identical; a
   bound-field rotation preserves PRODUCT-0055's color identity.
6. Bound fields compose with every accepted inline mark register:
   `.bar_width`, `.category_gap`, `.stroke_width`, and `.point_size` retain
   their current mark membership, values, order irrelevance, duplicate
   refusal, and cross-register refusal. Bound numeric modifier arguments still
   refuse as the separately deferred coverage item.
7. Direct and transitive aliases, most-recent preceding shadowing, and
   constructor-name collisions resolve under PRODUCT-0040. A cycle, unbound
   name, computed initializer, arithmetic expression, and numeric binding each
   refuse at the exact field-argument span with the binding-aware reason; no
   Rune evaluation occurs.
8. Every field binding remains in the transformed Rune program. Source maps,
   relation-origin resolution, view identity, output order, sibling-sink
   atomicity, and successful-analysis lifecycle are unchanged.
9. `/home/me/work/uat/test16.rn` binds `region`, `revenue`, and `product` once
   and reuses them in both bar marks. Its two completed figures, resolved
   specifications, draw operations, SVG, and live pixels equal the inline
   version; no rendering baseline changes.
10. Existing inline-literal mark behavior remains unchanged except for the two
    named recognition and attribution premise updates. No plot/table geometry,
    theme, exporter, draw-operation, screenshot, or refusal baseline changes
    outside the binding-specific argument diagnostics. The full workspace suite
    passes.
11. Documentation closes DB-03 as PRODUCT-0040 coverage now completed across
    all accepted mark positions. It continues to list register-argument
    bindings, computed presentation values, and non-top-level bindings under
    their existing dispositions.

## Stop condition

> This record may route accepted mark-field arguments through
> PRODUCT-0040's static string resolver, thread the existing binding inventory
> to every established mark parser, refactor `test16.rn`, and add the named
> gates and documentation. If implementation requires evaluating Rune,
> carrying a runtime binding into the plot normal form, consulting schema before
> static resolution, peeling bindings, changing channel or bar-pair resolution,
> changing a mark arity or layer/register boundary, accepting a bound modifier
> value, or refreshing any product pixel, work stops and the requirement
> returns to its owning record.

## Supersession

This record supersedes only PRODUCT-0040 §6's implementation coverage limit
and the corresponding DB-03 ledger entry. PRODUCT-0040's general rule,
resolution algorithm, diagnostics, retention, evaluation refusal, scope, and
lifecycle stand unchanged.

PRODUCT-0047's temporary inline-only premise and PRODUCT-0054's phrase “under
the existing mark-argument rule” now resolve to this completed coverage. Their
bar and line surface, arity, channel, legend, and color decisions are not
superseded. PRODUCT-0055's pair resolution and identity pins remain consumers,
not amendments.

## Rejected

- **Resolve bindings after relation execution:** turns a source literal lookup
  into runtime value transport and reaches PRODUCT-0055's pair rule too late.
- **Pass binding identifiers into channel resolution:** asks the schema for
  columns named `category_field` or `value_field` and makes factoring change
  meaning.
- **Special-case only `test16`'s third bar field:** leaves identical x/y and
  line/scatter positions arbitrarily divergent from PRODUCT-0040's general
  rule.
- **Cover standalone marks but not layers or legacy spelling:** makes static
  factoring depend on compatibility syntax even though all forms share one
  concrete-string normal form.
- **Retain binding identity in `AuthoredPlotSpec`:** gives factoring syntax
  semantic weight in channel lineage, descriptions, or persistence and forces
  every downstream consumer to resolve a source concern.
- **Infer or substitute a field when a binding fails:** violates exact named
  channel resolution. Static failure refuses before dispatch; schema failure
  refuses the completed sink.
- **Peel field bindings:** a binding may also serve the retained relation
  program, and proving otherwise requires whole-program analysis.
- **Fold register-argument bindings into this cut:** mark fields now have a
  concrete orientation-dependent consumer. No requested numeric modifier
  binding supplies equivalent evidence for widening the second coverage item.
- **Skip a record because PRODUCT-0040 pre-authorized coverage:** the semantic
  permission is already settled, but PRODUCT-0055 added a phase-ordering
  invariant and seven consumers make complete coverage worth a falsifiable
  ledger rather than an implementation note.

## Consequences

- Repeated field names become factorable without making the language more
  dynamic. The completed plot sees the same owned strings and schema facts as
  an inline program.
- Binding resolution gains mark fields as its next consumer; channel and
  bar-pair resolution remain downstream and unchanged.
- `test16.rn` gets shorter while remaining visually identical, the direct
  evidence that binding names express factoring rather than new intent.
- Changing one field binding can rotate every bar that uses it after the next
  successful run. That is the same observable change as editing the factored
  inline literals and does not alter identity.
- DB-03 closes. Register-argument bindings, computed values, and non-top-level
  scope remain explicit separate decisions rather than accidental omissions.
