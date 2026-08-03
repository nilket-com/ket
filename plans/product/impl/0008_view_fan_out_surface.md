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

Concretely, the work stops if it requires touching `plot/spec/`,
`plot/durable.rs`, `plot/theme.rs`, any kernel, `plot/factory.rs`, the scheduler,
`toron-client`, or the transport. It also stops if any test currently cited by an
`executable` corpus row has to change to stay green. Stopping means reporting the
finding, not working around it.

### Where work may land

The earlier draft claimed all work is confined to `rune_relation_envelope.rs`.
That was wrong, and its own plan contradicted it. Permitted:

- `rune_relation_envelope.rs` — recognition, kind analysis, lowering, refusals;
- `gui/state.rs` — the dispatch decision only, because a recognized envelope with
  no emitted root must stop rather than fall through to the local Rune host
  (today `NoTerminalView` falls through at `state.rs:529`). Routing only; no new
  execution, scheduling, or result semantics;
- `adapters/toron/mod.rs` — the live sibling test;
- `tests/architecture_test.rs` — **solely** to retire `.views(` from
  PRODUCT-0007's unaccepted-spelling list at `:458`, optionally with a comment
  citing PRODUCT-0008 so the retirement is not later undone by inspection. The
  `EncodedViewSet` and `SinkSpec` prohibitions in that same list are preserved
  unchanged: the first was withdrawn from the proposal in corpus v0.7, the second
  remains an explicit non-decision.

Any other file is a halt.

### Why the stale guard is not a stop-condition violation

The halt rule fired correctly on this guard, and the resolution is an amendment
rather than an override. The rule asks whether the work requires changing
execution, resolution, durability, or rendering, or invalidates existing
executable evidence. Retiring `.views(` does none of those.

That assertion is a **negative guard whose premise PRODUCT-0007 stated and
PRODUCT-0008 retired**. It said "no record accepts this spelling, so nothing may
lower it." A record now accepts it. The guard is not being weakened to accommodate
an inconvenient implementation; it has become false by decision, and leaving it in
place would assert something the repository has decided against.

It also protects no `executable` corpus row. Every such row cites behavioral
fixtures, and none cites this guard. Nothing that was proved becomes unproved.

The distinction worth preserving: a guard that fires because the work is doing
something forbidden is a halt, and a guard that fires because its own premise was
superseded is a citation to update. Only the second is amendable, and only by a
record that superseded the premise.

## Representation: sinks decouple from removable regions

`ParsedView` today is simultaneously *a removable statement* and *one sink*, and
`retained_regions()` requires the removal spans not to overlap. One
`.views([a, b, c])` statement carries three sinks, so that conflation cannot
survive and the earlier claim that `ExecutionEnvelope` is unchanged was
impossible.

Split it into three independent things:

```text
peeled regions   presentation-bearing source spans removed from the remote
                 program; non-overlapping; one per statement or binding
relation origin  one span, resolved as today (binding -> initializer text,
                 inline -> its own text)
sink intents     ordered, each individually spanned for diagnostics
```

`EnvelopeSemantics` — relation origin, ordered unspanned sink intents, execution
count — is **unchanged**, and so is every downstream consumer: `RuneViewIntent`
values, the scheduler, per-sink failure isolation, and stamped-result identity.
This is permitted lowering, not a stop-condition violation: the change is to how
a program's presentation is located and removed, not to what any program means.

`diagnostic_fallback()` continues to return the relation span.

## Kind analysis

Extend step 1's binding-and-kind analysis with receiver provenance. It answers
only which expressions descend from `workspace` and which names are bound — never
what any call computes. The PRODUCT-0003 relation-verb assay applies unchanged and
must not be relaxed.

```text
RelationCandidate   rooted at the `workspace` entrypoint parameter;
                    preserved through any unknown method
ViewSet             produced by `.views([...])`; preserved by further
                    `.views([...])`, which appends
```

Sugars `.scatter(x, y)`, `.bar(x, y)`, `.table()` on a `RelationCandidate` expand
to the canonical single-element list before anything else observes them, so
exactly one lowering path exists.

If `workspace` is locally shadowed, no chain is workspace-rooted, no `ViewSet`
exists, sink constructors are not recognized, and the whole buffer is ordinary
Rune. Shadowing is a retain path, never a refusal path.

### Presentation bindings are peeled

Step 1 resolves a binding to its initializer *text* in order to identify a
relation. That is insufficient here, because a binding may hold presentation:

```rune
let v = r.views([scatter("c", "v")]);
v.views([bar("c", "v")])
```

`let v = …views(…)` is presentation-bearing. Retaining it would send `.views` and
`scatter(...)` to Toron. Any binding whose initializer resolves to `ViewSet` kind
is therefore itself peeled, and the relation origin is taken from the underlying
`RelationCandidate` receiver chain rather than from the binding's text.

This applies transitively to chained aliases: `let a = r.views([…]); let b =
a.views([…]); b` peels both bindings and resolves one origin from `r`.

### A recognized envelope with no emitted root

A buffer whose `.views(...)` boundary is recognized but which emits no root — the
corpus's laziness rule — must schedule no Toron execution, emit no view, and
**not** fall through to the local Rune host. Falling through would hand a
`workspace`-rooted program to a host that has no `workspace`, producing a
confusing failure for a program that is simply lazy.

This is distinct from `NoTerminalView`, which means no envelope was recognized at
all and must keep falling through unchanged.

## Atomicity and refusals

Once a workspace-rooted receiver establishes the boundary, every element of the
list is presentation. An element that is unknown, malformed, or unsupported is an
attributed local refusal naming its span. There is no partial lowering: either
the whole statement is peeled and every element becomes a sink, or the program
refuses.

Refusable inside a proven list:

- an **empty list**, refused at its own span, per PRODUCT-0008 §3;
- a constructor with wrong arity or non-literal field arguments;
- an unknown callee;
- a bound sink specification, such as `let s = scatter("c","v"); r.views([s])`,
  which PRODUCT-0008 leaves undecided and which therefore refuses rather than
  guessing;
- a `ViewSet` binding used anywhere other than a further `.views(...)` or a
  terminal output position.

`MixedRelations` and `DuplicateForm` keep their meaning across both spellings:
all sinks in one program share one relation origin, and each form appears once.

These refusals are new, where step 1 introduced none. The justification is
PRODUCT-0008 §3: the receiver has already proven intent, so retaining an
unrecognized element would ship presentation syntax to a relation engine rather
than preserve an ambiguous user program.

## Tests

The proof burden is three items; everything else is regression.

1. **Direct versus factored equivalence.** These four produce equal
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

   The fourth is load-bearing: the accepted spelling must equal the **existing**
   spelling, not merely equal itself. Their `ExecutionEnvelope`s differ and the
   test asserts that too. The third additionally asserts that no peeled region
   overlaps another and that no `.views` or constructor text survives into the
   remote program.

2. **Sink ordering, at both levels.** Within one emitted `ViewSet`, list order
   then chain order is sink order: reordering the list or the chain yields
   unequal semantics, while reordering unrelated statements yields equal
   semantics. Across roots, order is **emission position, not definition
   position**:

   ```rune
   let v = r.views([scatter("c","v")]);
   table(r);
   v
   ```

   yields `table`, then `scatter`. The reversal — `v` before `table(r)` — yields
   `scatter`, then `table`. A third fixture defines two bound sets and emits them
   in reverse definition order, asserting emission order wins. Without these an
   implementation could order by constructor span, pass every other test, and
   silently contradict binding laziness.

3. **Contextual constructor recognition.** `scatter("c","v")` as a bare
   statement, in a binding, and inside a non-`views` call is ordinary Rune,
   retained and unrefused. A user-defined `fn scatter(a, b)` alongside a real
   `.views([...])` root is untouched. `.scatter()` on a receiver not descended
   from `workspace` is never a root. Positive controls prove each form is
   recognized in the sanctioned position.

**Atomicity controls.** An empty list, malformed arity, non-literal fields, an
unknown callee, a bound sink spec, and a misused `ViewSet` binding each refuse
with a span; a counter proves no partial peel and no dispatch occurred. Both
`.views([])` positions are covered — terminal and intermediate. A shadowed
`workspace` retains instead of refusing, proving the two paths are distinct.

**Laziness through the real caller.** A recognized envelope with no emitted root
schedules nothing, emits nothing, and does not reach the local Rune host; a
buffer with no envelope at all still falls through. Both asserted at the
`gui/state.rs` dispatch decision, not only inside the envelope module.

**Regression.** Every existing envelope test passes unchanged, including the two
shadow fixtures, the interleaved-roots test, and
`a_view_before_a_non_view_statement_is_now_an_output_root`. No test cited by an
`executable` corpus row is modified.

**Live.** A sibling of `live_rune_relation_reaches_table_bar_and_scatter_surfaces`
runs the accepted spelling against the deployment and asserts the same axis
identity. It seeds nothing; `test2` already exists.

## Non-goals

- No change to `plot/spec/`, `plot/durable.rs`, `plot/theme.rs`, kernels,
  `plot/factory.rs`, the scheduler, the transport, or Toron.
- No retirement or deprecation of `plot.bar`, `plot.scatter`, or `table(r)`.
- No shared-encoding sugar, no bindable sink specifications, no new sink form.
- No relaxation of the PRODUCT-0003 relation-verb assay or the containment guard.
- No change to `EnvelopeSemantics` or to any downstream consumer of
  `RuneViewIntent`. `ExecutionEnvelope`'s internal shape does change, as recorded
  above, because one statement can now carry several sinks.

## Review checklist

- [ ] Work stayed within the four permitted files; the halt rule was not
      triggered, or the work stopped and reported.
- [ ] `.views(` retired from the PRODUCT-0007 spelling guard with
      `EncodedViewSet` and `SinkSpec` preserved; no other assertion in
      `tests/architecture_test.rs` touched.
- [ ] Peeled regions, relation origin, and sink intents are three independent
      things; no two peeled regions overlap.
- [ ] `EnvelopeSemantics` and every downstream consumer are unchanged.
- [ ] Presentation bindings are peeled, including chained aliases; origin comes
      from the underlying relation chain.
- [ ] A recognized envelope with no emitted root stops; `NoTerminalView` still
      falls through.
- [ ] Atomicity holds: no partial lowering, no presentation syntax in the remote
      program, refusals attributed by span.
- [ ] A shadowed `workspace` retains rather than refuses.
- [ ] Kind analysis consults only provenance and bound names; the relation-verb
      assay passes unchanged.
- [ ] The four-program equivalence passes, including equality with the existing
      free-function spelling.
- [ ] Sink ordering proved at both levels: within a `ViewSet` by list then chain,
      across roots by emission position rather than definition position.
- [ ] `.views([])` refuses in terminal and intermediate positions.
- [ ] Contextual recognition proved with positive controls.
- [ ] Every existing envelope test passes unmodified; no `executable` row's cited
      test changed.
- [ ] Live gate for the accepted spelling green against the running deployment.
- [ ] Corpus updated in a separate commit, not this one.
