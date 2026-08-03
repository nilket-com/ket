# Lower the view fan-out surface

Scope: implement PRODUCT-0008. Recognize `.views([...])` and its three single-view
sugars, lower them into the existing `ViewSet` normal form, and prove equality
against `EnvelopeSemantics`. Deliverable is one `impl:` commit.

This plan authorizes parsing, provenance analysis, and lowering. Nothing else.

## Halt rule

PRODUCT-0008 §7 binds this implementation:

> Accepting `.views([...])` may add only parsing, provenance analysis, and
> lowering into the existing `ViewSet` normal form. If implementation requires
> changing execution, channel resolution, durability, or rendering — or
> invalidates existing executable evidence — the work stops: the proposal is not
> a respelling and requires a broader record.

Concretely, the work stops if it requires touching `plot/spec/`, `plot/durable.rs`,
`plot/theme.rs`, any kernel, `plot/factory.rs`, the scheduler, `toron-client`, or
the transport. It also stops if any test currently cited by an `executable`
corpus row has to change to stay green. Stopping means reporting the finding, not
working around it.

All work is confined to `crates/polariton/src/rune_relation_envelope.rs`, whose
containment is already guarded by
`product_0003_terminal_envelope_is_the_only_relation_view_transformation_site`.

## Kind analysis

Extend the existing binding-and-kind analysis with a receiver-provenance kind.
It answers only which expressions descend from `workspace` and which names are
bound — never what any call computes. The PRODUCT-0003 relation-verb assay
continues to apply unchanged and must not be relaxed.

```text
RelationCandidate   rooted at the `workspace` entrypoint parameter;
                    preserved through any unknown method
ViewSet             produced by `.views([...])`; preserved by further
                    `.views([...])`, which appends
```

- sugars `.scatter(x, y)`, `.bar(x, y)`, `.table()` on a `RelationCandidate`
  expand to the canonical single-element list before anything else observes them,
  so exactly one lowering path exists;
- bindings carry kind and accumulated sinks, per step 1's existing binding
  resolution;
- only a top-level expression of `ViewSet` kind is an output root;
- sink constructors are recognized **only** inside a `.views([...])` argument
  list. Elsewhere they are ordinary calls and are retained.

## Lowering and the relation span

Today a root is a whole statement and the relation is a call argument. Under the
accepted spelling the root is a method call whose **receiver** is the relation, so
the relation span is the receiver chain and the retained remote program keeps
everything up to but excluding `.views(...)`.

This is a new span shape, not new semantics: `analyze` still yields an
`ExecutionEnvelope` of retained statement spans, one relation span, and ordered
roots, and `render` still splices retained regions and appends the relation as
the trailing expression. A receiver that is a binding resolves to that binding's
initializer text, exactly as step 1 already does, so `let r = …; r.views([…])`
needs no special case.

`diagnostic_fallback()` continues to return the relation span.

## Sink intents

`.views([...])` produces the same `RuneViewIntent` values the free-function
spelling produces, so downstream code cannot tell the spellings apart. Arity and
string-literal rules for `scatter`, `bar`, and `table` are reused rather than
reimplemented, and `MixedRelations` and `DuplicateForm` keep their meaning: all
sinks in one program share one relation origin, and each form appears once.

Sink order is list order, then chain order, which falls out of appending.

## Tests

The proof burden is three items. Everything else is regression.

1. **Direct versus factored equivalence.** These four programs produce equal
   `EnvelopeSemantics`:

   ```rune
   workspace.table("t").sort([asc(col("c"))]).views([scatter("c","v"), bar("c","v")])
   ```
   ```rune
   let r = workspace.table("t").sort([asc(col("c"))]);
   r.views([scatter("c","v"), bar("c","v")])
   ```
   ```rune
   let v = workspace.table("t").sort([asc(col("c"))]).views([scatter("c","v")]);
   v.views([bar("c","v")])
   ```
   ```rune
   let r = workspace.table("t").sort([asc(col("c"))]);
   plot.scatter(r, "c", "v"); plot.bar(r, "c", "v")
   ```

   The fourth is the load-bearing one: the accepted spelling must equal the
   **existing** spelling, not merely equal itself. Their `ExecutionEnvelope`s
   differ and the test asserts that too, so the projection is visibly doing the
   work.

2. **Sink ordering.** List order then chain order is the sink order; reordering
   the list or the chain yields unequal semantics, while reordering unrelated
   statements yields equal semantics. Both directions asserted.

3. **Contextual constructor recognition.** `scatter("c","v")` as a bare statement,
   in a binding, and inside a non-`views` call is ordinary Rune, retained and
   unrefused. A user-defined `fn scatter(a, b)` alongside a real `.views([...])`
   root is untouched. `.scatter()` on a receiver not descended from `workspace` is
   never a root. Positive controls prove each form is recognized in the sanctioned
   position, so the negatives cannot pass by nothing being recognized.

**Sugar equivalence.** `r.scatter(x, y)` equals `r.views([scatter(x, y)])` in
semantics, and likewise for `bar` and `table`, asserted per form.

**Regression.** Every existing envelope test passes unchanged, including both
`Starting`-era shadow fixtures, the interleaved-roots test, and
`a_view_before_a_non_view_statement_is_now_an_output_root`. No test cited by an
`executable` corpus row is modified.

**Live.** `live_rune_relation_reaches_table_bar_and_scatter_surfaces` gains a
sibling that runs the accepted spelling against the deployment and asserts the
same axis identity. It seeds nothing new; `test2` already exists and the existing
gate's shape is reused.

## Non-goals

- No change to `plot/spec/`, `plot/durable.rs`, `plot/theme.rs`, kernels,
  `plot/factory.rs`, the scheduler, the transport, or Toron.
- No retirement or deprecation of `plot.bar`, `plot.scatter`, or `table(r)`.
- No shared-encoding sugar, no bindable sink specifications, no new sink form.
- No relaxation of the PRODUCT-0003 relation-verb assay or the containment guard.
- No change to `EnvelopeSemantics`, `ExecutionEnvelope`, or `render`'s contract
  beyond the receiver-chain span shape.

## Review checklist

- [ ] Work confined to `rune_relation_envelope.rs`; the halt rule was not
      triggered, and if it was, the work stopped and reported.
- [ ] Kind analysis consults only provenance and bound names; the relation-verb
      assay passes unchanged.
- [ ] Sugars expand before observation, so exactly one lowering path exists.
- [ ] Sink constructors recognized only in `.views([...])` argument position;
      every other position retained without refusal.
- [ ] The four-program equivalence passes, including equality with the existing
      free-function spelling.
- [ ] Sink ordering proved in both directions.
- [ ] Contextual recognition proved with positive controls.
- [ ] Every existing envelope test passes unmodified; no `executable` row's cited
      test changed.
- [ ] Live gate for the accepted spelling green against the running deployment.
- [ ] Corpus updated in a separate commit, not this one.
