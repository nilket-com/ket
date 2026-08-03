# ADR 0008: the view fan-out surface

Status: accepted 2026-08-03. This record decides one thing: the spelling by which
a Rune buffer fans one relation into several presentation sinks. It accepts the
surface proposed in `plans/product/corpus/rune_workspace_corpus.md` v0.10 and
inherits that section's invariant, recognition rules, normal form, and stop
condition. It authorizes no execution, resolution, durability, or rendering work.

## Context

The semantics this spelling describes are already executable and cited. Corpus
v0.10 marks the three view sinks and shared-execution fan-out `executable`, with
a live gate that asserts axis identity and an automated fixture proving every
sink holds the same `Arc` rather than equivalent copies.

The machinery a surface needs is also in place. PRODUCT-0003's envelope was
rebuilt on typed output-root discovery, which replaced positional trailing-block
recognition with a binding analysis over top-level statements and produced
`EnvelopeSemantics` — resolved relation origin, ordered unspanned sink intents,
execution count. PRODUCT-0007 gave channels identity that survives to a kernel.

What remains is a spelling. The current one works and is proved:

```rune
let r = workspace.table("test2").sort([col("cat").asc()]);
plot.scatter(r, "cat", "val");
plot.bar(r, "cat", "val");
table(r);
```

It has two costs. The relation must be named and then mentioned once per sink,
and the sink names are free functions a user may legitimately own, which is why
recognition needs whole-buffer shadow analysis. Neither is fatal; both are
avoidable.

## Decision

### 1. `.views([...])` is the accepted surface

```rune
workspace
    .table("test2")
    .sort([col("cat").asc()])
    .views([
        scatter("cat", "val"),
        bar("cat", "val"),
        table(),
    ])
```

`.views([...])` is the transition from a relation to a set of sinks, and is
therefore simultaneously the type-state boundary, the fan-out boundary, and the
Toron–Polariton process boundary. Everything at or below the receiver executes on
Toron; everything the call produces stays in Polariton.

The name states the operation that occurs — fan one relation into several
presentation sinks — without asserting that every sink consumes an encoding,
which is false for `table()`.

### 2. Single-view methods are strict sugar

```text
r.scatter(x, y)  ≡  r.views([scatter(x, y)])
r.bar(x, y)      ≡  r.views([bar(x, y)])
r.table()        ≡  r.views([table()])
```

Each is *defined as* its expansion and may carry no semantics the canonical form
cannot express. A sugared form that acquires a private option is a second
language and is prohibited by this record, not merely discouraged.

Together with the existing free-function spelling this yields one equivalence
that must hold in every direction:

```text
plot.scatter(r, x, y)  ≡  r.scatter(x, y)  ≡  r.views([scatter(x, y)])
```

All three lower to the same relation origin and the same ordered sink intent
before execution.

### 3. Sink constructors are recognized only in argument position

`scatter(...)`, `bar(...)`, and `table()` are sink constructors **only as
arguments to `.views([...])`**. The identical call anywhere else is ordinary Rune
and reaches the executor untouched.

Reservation is confined to a position that has already been proven to belong to a
relation-to-sinks transition, rather than claimed over a namespace. A
user-defined `fn table(x)` and a bare `table(1)` remain legal.

Inside a proven `.views([...])` list, recognition is **atomic**. Once a
workspace-rooted receiver has established the boundary, every element of that list
is presentation, and an element that is unknown, malformed, or not yet supported
is an attributed local refusal naming its span. Partial lowering is prohibited:
no program may reach Toron with some elements peeled and others left in place,
and no presentation syntax may reach the executor.

An **empty list refuses** at its own span. A terminal `.views([])` would create an
output root carrying no sinks and make the normal form's execution count
ambiguous — one execution producing nothing is not distinguishable from no
execution — and an intermediate `.views([])` adds no expressive power that
appending does not already provide.

This is deliberately stricter than PRODUCT-0003's free-function recognition,
which retains everything it does not recognize. The difference is evidence, not
appetite. There, a bare `table(1)` is genuinely ambiguous between a view and a
user's own function, so retaining is the only safe reading. Here the receiver has
already proven the transition, so an unrecognized element is unambiguously a
mistake inside a presentation list, and retaining it would ship presentation
syntax to a relation engine.

### 4. Recognition is by receiver provenance, not by name

Recognition runs on a locally tracked kind named `RelationCandidate` — provenance,
not a claim about any Rune type, since rune 0.14 exposes no type model and Toron
remains the authority on whether a peeled expression is a relation.

```text
workspace-rooted chain
  unknown method        -> RelationCandidate
  .views([...])         -> ViewSet
  .scatter(x, y)        -> sugar for .views([scatter(x, y)])
  .bar(x, y)            -> sugar for .views([bar(x, y)])
  .table()              -> sugar for .views([table()])

ViewSet
  .views([...])         -> ViewSet with the further sinks appended
```

Unknown methods are kind-preserving. The analysis must never enumerate which
methods return relations, because those are the relation verbs PRODUCT-0003
forbids it from knowing; stating propagation negatively keeps it ignorant by
construction. `.views(...)` is likewise kind-preserving on a `ViewSet`, so sinks
accumulate in list order and then chain order and sink ordering needs no separate
rule.

Bindings preserve kind and accumulated specification. Binding alone emits
nothing; only a top-level expression of `ViewSet` kind is an output root.

This is narrower than the free-function matching it joins: a sink method is
recognized only on a receiver already traced to `workspace`, so a user-defined
`.scatter()` on an unrelated value is never a root.

### 5. The normal form is unchanged, and is the oracle

```text
ViewSet { relation_origin, sinks: flat ordered list }
```

Every surface operation appends. Nesting never enters the representation, so list
order followed by chain order is execution order by construction.

Ordering has two levels, and factoring makes the distinction observable. **Output
roots order globally by source position**, and **list order then chain order
applies within each emitted `ViewSet`**. Constructor spans carry diagnostic
attribution only and never contribute to ordering. So in

```rune
let v = r.views([scatter("c", "v")]);
table(r);
v
```

the sink order is `table`, then `scatter`. The binding emits nothing, so `v`'s
sinks take the position of the expression that emits them rather than the
position of their constructors. Ordering by constructor span would reverse this
and would silently contradict the laziness rule that says a binding emits
nothing — the two rules are the same rule seen from two sides.

This is the shape `EnvelopeSemantics` already compares. The accepted surface is
therefore judged by lowering alone: its proof obligation is that the new spelling
produces semantics equal to the old one, using a comparison that exists and is
already proved.

### 6. The accepted surface is additive

The free-function spelling remains executable and its corpus evidence remains
valid. This record adds a spelling; it does not retire one.

Retiring `plot.bar`, `plot.scatter`, and `table(r)` would be a breaking change to
a working surface carrying landed live evidence, and would oblige a migration
whose risk has nothing to do with the merits of the new spelling. It requires its
own record, made when there is use to migrate rather than in advance of any.

Corpus rows therefore gain the accepted spelling beside the existing one, each
retaining its own citations.

### 7. The stop condition

> Accepting `.views([...])` may add only parsing, provenance analysis, and
> lowering into the existing `ViewSet` normal form. If implementation requires
> changing execution, channel resolution, durability, or rendering — or
> invalidates existing executable evidence — the work stops: the proposal is not
> a respelling and requires a broader record.

This is normative and binds the implementation of this record. It is a halt rule,
not a checklist: it acts the moment a forbidden change is required, which is when
the discovery is cheapest and before this record's stated scope has quietly
become untrue.

### 8. What this record does not authorize

No change to Toron, the transport, channel resolution, `PlotSpec` artifacts,
durability, theming, kernels, or the scheduler. No new geometry, no new sink
form, no bindable sink specifications, no shared-encoding sugar, and no retirement
of the current spelling.

## Rejected

- **`.encode(x, y)` as the boundary:** asserts that every sink consumes an
  encoding, which is false for `table()`, and promotes a shared-channel
  convenience into the process boundary. Withdrawn in corpus v0.7; shared
  encoding may return later as presentation sugar over the sink list.
- **A terminal render verb:** fuses query and view so the relation is never a
  value, which is the ceiling reached by comparable systems and the reason this
  product needs a binding or a list in the first place.
- **Recognizing sink constructors anywhere:** reserves names over a namespace a
  user may legitimately own.
- **Recognizing sink methods by name without receiver provenance:** the same
  defect one level out, and the reason the current spelling needs whole-buffer
  shadow analysis.
- **Sugar with private semantics:** two languages wearing one name.
- **Retiring the free-function spelling in this record:** a breaking migration
  bundled with a spelling decision, so a problem in either becomes a problem in
  both.
- **Nesting sinks in the representation:** would make execution order a rule
  someone maintains rather than a property of the form.

## Consequences and sequencing

- The shortest correct spelling of the working script loses its binding and its
  repetition, without changing what the script means.
- Recognition gains a kind analysis and therefore no longer needs shadow analysis
  for the accepted spelling. Shadow analysis remains for the free-function
  spelling, which this record keeps.
- The corpus's proposed section becomes accepted, and the view rows gain the new
  spelling beside their existing citations.
- The implementation receives its own plan and is bound by section 7. Its proof
  burden is three items: direct-versus-factored equivalence, sink ordering, and
  contextual constructor recognition.
- A future record may retire the free-function spelling, add shared-encoding
  sugar, or make sink specifications bindable. None of them is authorized here,
  and each inherits this normal form.
