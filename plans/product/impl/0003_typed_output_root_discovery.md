# Typed output-root discovery

Scope: replace the envelope's positional trailing-block recognizer with an
output-root analysis over the top-level statement list, keeping today's
`table` / `plot.bar` / `plot.scatter` spelling unchanged. This is step 1 of the
three-step sequence recorded in `../corpus/rune_workspace_corpus.md` v0.5: it
establishes the semantic boundary so that `.views(...)` and `.encode(...)` can
later be lowering rules over a sound analysis rather than parser tricks.

No new spelling is introduced. Existing **unshadowed** top-level view forms
become position-independent; every non-view statement retains its existing
meaning. Position-independence is the feature, so some programs do change — a
non-trailing `table(r)` reaches the executor today and becomes a local sink
after this step. What must not change is the meaning of anything the envelope
does not recognize. The step introduces no new surface syntax and no new
refusal. No wire change, no Toron change. Deliverable is one `impl:` commit.

## What "typed" means here, and what it must not become

Rune 0.14.1 exposes no type or semantic model to a consumer. The envelope parses
with `rune::parse::parse_all::<ast::EmptyBlock>` and no `Context`
(`rune_relation_envelope.rs:206`); the only local compile in the crate
(`rune_host.rs:118`) returns a `Unit` and `Diagnostics`, which is bytecode and
errors, not types. There is no compiler question to ask.

"Typed" therefore names a **Polariton-owned binding-and-kind analysis** over the
top-level statement list. It answers exactly two questions:

1. which top-level statements are **output roots** — expression statements whose
   value is displayed; and
2. which **local bindings** flow into those roots.

It must answer nothing else. In particular it does not resolve, interpret, or
branch on any relation verb. PRODUCT-0003 assigns relation verbs, typing,
planning, and lowering exclusively to Toron's native frontend, and a binding
analysis is not relation typing: knowing that `r` is defined at statement 0 and
referenced at statement 3 requires no knowledge of what `sort` does.

The distinction is guard-enforced. An assay proves the analysis source contains
no relation verb name (`project`, `filter`, `aggregate`, `sort`, `limit`, `join`,
`window`) outside test fixtures, with a synthetic violating source as a positive
control, and cites PRODUCT-0003 on failure. The existing containment test
(`tests/architecture_test.rs:45`) keeps the whole analysis inside
`rune_relation_envelope.rs`.

## Recognition is total, and unrecognized means retained

Callee recognition remains a raw source-text comparison (`:315-317`). The
analysis cannot distinguish a view call from a user-defined function of the same
name, and no amount of scoping fixes that. This program is valid today and must
stay valid:

```rune
fn table(x) { x }
let n = table(1);

let r = workspace.table("t");
plot.bar(r, "cat", "val");
```

A genuine `plot.bar` root does not license the analysis to reinterpret
`table(1)`. Having one real root limits such a collision; it does not make raw
name matching safe.

Retaining non-expression positions is not sufficient either, because a direct
call in expression position collides just as readily:

```rune
fn table(x) { x }

table(1);
let answer = 42;
answer
```

Today the reverse walk sees the trailing `answer`, returns `NoTerminalView`, and
the local Rune host runs this program correctly. A forward scan sees a top-level
one-argument `table(1)` — a complete name-and-shape match — and would claim it as
an output root, converting a working local program into a broken remote one.
`NoTerminalView` is currently what protects this buffer, and position-independence
removes that protection.

The rule is therefore as narrow as it can be:

> A **top-level expression statement** whose callee text and call shape both
> match an **unshadowed** known view form is an output root. **Everything else is
> retained** and passed through unchanged.

"Everything else" includes view-named calls in binding position, inside
functions, inside blocks, and inside control flow — and view-named calls at top
level whose shape does not match, such as wrong arity or non-literal field
arguments. Retained statements behave exactly as they do today: the buffer either
has another valid root, in which case they reach Toron, or it has none, in which
case it falls through to the local Rune host. Legitimate same-named user
functions keep working in both cases.

### Shadowing

Recognition consults the binding analysis before claiming any root:

- a visible user binding or item named `table` prevents `table(...)` recognition;
- a visible binding or item named `plot` prevents `plot.bar(...)` and
  `plot.scatter(...)` recognition;
- only unshadowed exact forms are presentation roots.

This is still binding analysis — which names are bound — and not Rune relation
typing. It asks nothing about what any call computes.

Visibility is evaluated conservatively for this step: a top-level item or binding
of a shadowing name anywhere in the buffer disables that form for the **whole**
buffer, rather than from its definition onward. Rune items are file-scoped
regardless of position, and for `let` bindings the conservative rule errs toward
retention, which is the safe direction under a total recognizer. A precise
lexical rule can tighten this later without changing any program that works under
the conservative one.

This makes recognition **total and monotone**: recognized becomes a root,
unrecognized is left alone, and no third outcome exists.

### The diagnostic this costs

Today a top-level trailing `plot.bar(r, "x")` refuses with "received 2
arguments" (`:318-332`, asserted by
`no_view_stays_outside_the_envelope_and_invalid_view_refuses` at `:581`).

Under a total recognizer, wrong arity means "not a view," so that call is
retained and the user sees a **downstream-executor** error instead of a precise
local one — from the local Rune host when the buffer has no other root, or from
Toron when another valid root establishes an envelope. A standalone malformed
call never reaches Toron at all; it yields `NoTerminalView` and falls through.

That is a real regression on a likely typo, and it is accepted deliberately
rather than overlooked. Keeping the refusal would require treating a name match
with a shape mismatch as evidence of intent, which is exactly the inference the
collisions above forbid. The precise diagnostic can return once the view names
are formally reserved — the same condition already deferring a nested-call
diagnostic. The arity half of `:581` is rewritten accordingly, with a citation.

`MixedRelations` (`:223-232`) and `DuplicateForm` (`:233-241`) are unaffected:
both compare roots that were already recognized, and neither widens to any new
position.

## Analysis

Replace the reverse walk with `None => break` (`rune_relation_envelope.rs:209-216`)
with a forward pass classifying every top-level statement:

| classification | example | effect |
|---|---|---|
| output root | `table(r)` · `plot.bar(r,"cat","val");` | contributes a sink; removed from the remote program |
| everything else | `let v = table(r);` · `let r = workspace.table("t");` · `fn table(x) { x }` | retained in the remote program, in order |

Position stops mattering: roots are collected wherever they appear and ordered by
source position, preserving today's source-order sink contract. No statement
acquires a new meaning, and no statement that reaches Toron today stops reaching
it — only recognized roots are removed, and those were already removed when they
appeared in a trailing block.

### Relation identity

`MixedRelations` compares source-text slices today, and that behavior is
preserved exactly. Identity is resolved to a **relation origin** before
comparison:

- an argument that is a binding resolves to the source text of that binding's
  initializer;
- an inline argument resolves to its own source text.

Comparison is then source-text equality on the resolved origin, for both cases.
Spans are never compared: two identical inline expressions occupy different spans
and must remain the same relation, which is today's behavior.

Resolving a binding to its initializer text is also what lets an inline program
and its factored equivalent compare equal, since both yield the same origin. No
normalization is introduced — equivalence fixtures write the relation
identically, and adding whitespace or formatting normalization would turn the
suite into a formatter test.

## Two stages, and an envelope that cannot be empty

```text
analyze(source)                      -> Result<OutputRootAnalysis, RuneEnvelopeError>
OutputRootAnalysis::into_envelope()  -> Option<ExecutionEnvelope>
ExecutionEnvelope::semantics(source) -> EnvelopeSemantics
render(source, &ExecutionEnvelope)   -> TransformedRuneView
```

`OutputRootAnalysis` is the classification of the whole statement list and may
have **zero** roots. Zero roots means there is no envelope: `into_envelope`
yields `None`, the caller reports `NoTerminalView`, and the buffer falls through
to the local Rune host (`gui/state.rs:512, 531`) exactly as today.

`ExecutionEnvelope` is therefore **nonempty by construction** — it exists only
with at least one root — which is what makes `EnvelopeSemantics::execution_count`
meaningfully always one. An empty envelope is unrepresentable rather than merely
untested, and the empty case is exercised through the real production transition
instead of by hand-building an invalid value.

`ExecutionEnvelope` holds what rendering needs, including spans: the ordered
spans of retained top-level statements, the span of the relation expression that
becomes the trailing expression, and the ordered roots as
`SpannedRuneViewIntent`.

**Equivalence is asserted on `EnvelopeSemantics`**, not on `ExecutionEnvelope`
and not on rendered bytes. Whole-envelope equality is unachievable by
construction: an inline program and its factored form differ in retained
statements — one keeps a `let` — and in every span. The projection drops what
rendering needs and keeps what meaning requires:

- the resolved relation origin;
- the ordered sink intents, **unspanned**; and
- the execution count, always one.

Sink **order is part of meaning**. Reordering ordinary statements preserves
semantics; reordering roots does not.

Byte comparison is excluded for the same reason spans are: it would make the
equivalence suite fail on formatting. Serialization keeps its own narrow tests.

`render` generalizes today's three-segment splice (`:247-277`). Removing roots
from arbitrary positions produces N retained regions rather than one prefix and
one suffix, so `RuneEnvelopeSourceMap` becomes an ordered segment list built from
the retained spans. `map_span` (`:83`) keeps its signature and its `None` for
wrapper-only spans.

### Diagnostic fallback

`view_block()` (`:98`) is renamed `diagnostic_fallback()` and returns **the
relation span**, not a view's span.

The fallback exists for a server diagnostic that cannot be mapped through the
source map. Toron only ever receives the relation program; it has never seen a
presentation sink. Pointing an unmappable remote error at the first view is
deterministic but causally wrong, and the old name asserted a contiguous block
that no longer exists.

## Tests

**Surface — falsify the positional recognizer.**

1. **Inline versus bound relation** produce equal `EnvelopeSemantics`:
   `table(workspace.table("t").sort([asc(col("c"))]))` against the same relation
   bound to `r` and referenced. Their `ExecutionEnvelope`s differ, and the test
   asserts that too, so the projection is shown to be doing the work.
2. **Interleaved view statements** — a view, an ordinary statement, another view
   — yield both sinks in source order, with the ordinary statement retained.
3. **Views before unrelated statements** — the direct inversion of
   `a_view_before_a_non_view_statement_is_not_in_the_trailing_block` (`:569`).
   That test asserts today's contiguity rule and must be rewritten, not deleted;
   the rewrite asserts the view is now recognized and the trailing statement
   retained, and cites this plan so the reversal is deliberate rather than
   looking like a regression.
4. **Trailing block still works** — the existing three-view case (`:500`) passes
   unchanged, proving the generalization is a superset.

**Meaning is preserved for everything unrecognized.**

5. **The binding-position collision is untouched**: `fn table(x) { x }` and
   `let n = table(1);` are retained verbatim in the remote program while
   `plot.bar` is recognized as the sole root.
6. **The direct-call collision is untouched**: the `fn table(x) { x }` /
   `table(1);` / trailing `answer` program still yields `NoTerminalView` and
   reaches the local Rune host. This is the case position-independence would
   otherwise break, so it is the load-bearing shadowing proof.
7. **Shadowing disables each form independently:** a `let plot = ...;` binding
   suppresses `plot.bar` and `plot.scatter` recognition while an unshadowed
   `table(r)` in the same buffer is still a root, and the mirror case with
   `table` shadowed. Positive controls without the shadowing binding prove both
   forms are otherwise recognized.
8. **View-named calls in binding position, in functions, in blocks, and in
   control flow** are retained and neither refused nor removed.
9. **A top-level view-named call with wrong arity or non-literal fields is
   retained**, replacing the arity-refusal half of `:581` with a citation. The
   name-only half — `NoTerminalView` for a buffer with no view at all — is
   unchanged.

**Internal.**

10. **Semantic equality respects root order:** reordering ordinary statements
    yields equal `EnvelopeSemantics`; reordering roots yields **unequal**
    semantics. Both directions are asserted, since order is meaning.
11. **Zero roots produce no envelope:** an `OutputRootAnalysis` over a
    view-free buffer yields `None` from `into_envelope`, the caller reports
    `NoTerminalView`, and no execution is scheduled. A one-root positive control
    yields an envelope and one execution, so the test cannot pass by finding
    nothing anywhere.
12. **One remote execution feeds ordered local sinks:** an envelope with three
    roots submits exactly one scheduler execution and yields three sinks in
    source order sharing one result identity, re-asserting the existing `Arc`
    invariant (`gui/state.rs:2103`) against position-independent roots.

**Preserved behavior and containment.**

13. `MixedRelations` and `DuplicateForm` (`:546`) keep their behavior under
    origin-resolved identity, including two identical inline expressions at
    different spans comparing as the same relation.
14. `diagnostic_fallback()` returns the relation span; an unmappable server error
    is reported against the relation, not against a view.
15. The relation-verb assay, with its positive control. Shadow-awareness must not
    trip it: the analysis consults bound names, never relation verbs.
16. `tests/architecture_test.rs:45` passes unchanged.

## Non-goals

- No `.views(...)`, `.encode(...)`, `EncodedViewSet`, or any new spelling; today's
  `table` / `plot.bar` / `plot.scatter` grammar is preserved exactly.
- **No new surface semantics and no new refusal.** No construct that reaches
  Toron today stops reaching it, and none acquires a local meaning it lacked.
- No local diagnostic for nested or malformed view-named calls; both wait on
  reserved names.
- No relation typing, verb resolution, planning, or lowering in Polariton.
- No normalization of relation source text.
- No wire, protocol, `PlotSpec`, scheduler, or pane-routing change.
- No change to Toron, `toron-client`, or the local Rune host (`rune_host.rs`).

## Review checklist

- [ ] The analysis answers only output-root and binding questions; the
      relation-verb assay passes with its positive control.
- [ ] Recognition is total: unshadowed matched top-level expression statements
      become roots, everything else is retained, and no third outcome exists.
- [ ] Recognition is shadow-aware per form, conservatively over the whole buffer,
      with positive controls proving each form is otherwise recognized.
- [ ] Both collision programs are proven untouched, including the direct-call one
      that `NoTerminalView` protects today.
- [ ] No new refusal is introduced; the arity refusal is withdrawn with a
      citation rather than widened to new positions.
- [ ] `ExecutionEnvelope` is nonempty by construction; the empty case runs
      through `OutputRootAnalysis::into_envelope` returning `None`.
- [ ] Equivalence compares `EnvelopeSemantics` — resolved origin, unspanned
      intents, order, execution count — never spans and never rendered bytes.
- [ ] Root order is proven to be part of semantic equality.
- [ ] Relation identity is source-text equality on the resolved origin.
- [ ] `diagnostic_fallback()` replaces `view_block()` and returns the relation
      span.
- [ ] Serialization tests remain narrow: wrapper, trailing expression without
      `;`, and source-map offsets over N segments.
- [ ] Existing envelope, fan-out, and architecture tests pass unchanged.
- [ ] Corpus rows are untouched; this step changes no status and no spelling.
