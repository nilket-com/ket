# Typed output-root discovery

Scope: replace the envelope's positional trailing-block recognizer with an
output-root analysis over the top-level statement list, keeping today's
`table` / `plot.bar` / `plot.scatter` spelling unchanged. This is step 1 of the
three-step sequence recorded in `../corpus/rune_workspace_corpus.md` v0.5: it
establishes the semantic boundary so that `.views(...)` and `.encode(...)` can
later be lowering rules over a sound analysis rather than parser tricks.

No new surface syntax, no new surface semantics, no wire change, no Toron
change. Deliverable is one `impl:` commit.

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

### The limit this places on refusals

Callee recognition remains a raw source-text comparison (`:315-317`). The
analysis cannot tell a view call from a user-defined function of the same name.
That is tolerable where the envelope has already claimed a buffer, and dangerous
anywhere else, because a plain Rune program that defines its own `table` must
keep running on the local host (`gui/state.rs:512, 531`).

Every new refusal in this plan is therefore **scoped to buffers that are already
envelopes** — those yielding at least one top-level output root. A buffer with no
top-level root still returns `NoTerminalView` and falls through unchanged. Where
that scoping is not sufficient to make a refusal safe, this plan retains today's
behavior instead of inventing one.

## Scope boundary

Output roots are **top-level statements only**, regardless of position. View-named
calls nested inside a function body, a block, or any control flow are not roots.

Nested calls are **retained as ordinary statements**, exactly as today. They are
not refused and not removed. A nested call that is genuinely a view fails
honestly downstream — as an unknown callee in Toron, or in the local host — and a
nested call that is a user-defined function of the same name keeps working. A
local refusal here would have to key on raw callee spelling inside arbitrary
nested scopes, which is precisely where that spelling carries the least
information.

A better nested diagnostic waits until either the view names are formally
reserved, or the analysis carries enough context to distinguish a view call from
a same-named local function. Neither is in this step.

## Analysis

Replace the reverse walk with `None => break` (`rune_relation_envelope.rs:209-216`)
with a forward pass classifying every top-level statement:

| classification | example | effect |
|---|---|---|
| output root | `table(r)` · `plot.bar(r,"cat","val");` | contributes a sink; removed from the remote program |
| view binding | `let v = table(r);` | typed refusal, scoped to envelope buffers |
| ordinary statement | `let r = workspace.table("t");` · `let n = 1;` | retained in the remote program, in order |

Consequences:

- **Position stops mattering.** Roots are collected wherever they appear and
  ordered by source position, preserving today's source-order sink contract.
- **A view call in binding position refuses.** Today that program reaches Toron
  and dies as an unknown callee. Step 1 must not convert it into a silently bound
  value that emits nothing — that is surface semantics, and assigning it here
  would pre-empt step 3. The refusal names the span and states that a view cannot
  be bound in this release. It fires only in a buffer that already has a top-level
  root, so a plain program with its own `table` is untouched.
- **Laziness is not proved through surface syntax in this step.** The
  bound-but-unemitted property is asserted against a directly constructed
  `ExecutionEnvelope`, where a root list can be empty without any program having
  to mean it. Step 3 exposes the behavior in syntax and re-proves it there.

`DuplicateForm` (`:233-241`) and the arity and string-literal rules
(`:318-332`, `:349-391`) are unchanged.

### Relation identity

`MixedRelations` (`:223-232`) compares source-text slices today, and that
behavior is preserved exactly. Identity is resolved to a **relation origin**
before comparison:

- an argument that is a binding resolves to the source text of that binding's
  initializer;
- an inline argument resolves to its own source text.

Comparison is then source-text equality on the resolved origin, for both cases.
Spans are never compared: two identical inline expressions occupy different spans
and must remain identical relations, which is today's behavior.

Resolving a binding to its initializer text is also what lets an inline program
and its factored equivalent compare equal, since both yield the same origin. No
normalization is introduced — equivalence fixtures write the relation identically,
and adding whitespace or formatting normalization would turn the suite into a
formatter test.

## Split analysis from serialization

`transform` (`:205`) currently parses, recognizes, and splices in one pass, and
its product is a `String`. Split it:

```text
analyze(source)                    -> Result<ExecutionEnvelope, RuneEnvelopeError>
ExecutionEnvelope::semantics(source) -> EnvelopeSemantics
render(source, &ExecutionEnvelope)  -> TransformedRuneView
```

`ExecutionEnvelope` holds what rendering needs, including spans: the ordered
spans of retained top-level statements, the span of the relation expression that
becomes the trailing expression, and the ordered roots as
`SpannedRuneViewIntent`.

**Equivalence is asserted on `EnvelopeSemantics`, not on `ExecutionEnvelope` and
not on rendered bytes.** Whole-envelope equality is unachievable by construction:
an inline program and its factored form differ in retained statements — one keeps
a `let` — and in every span. `EnvelopeSemantics` is the projection that drops
what rendering needs and keeps what meaning requires:

- the resolved relation origin;
- the ordered sink intents, **unspanned**; and
- the execution count implied by the envelope, which is always one.

Byte comparison is excluded for the same reason spans are: it would make the
equivalence suite fail on formatting. Serialization keeps its own narrow tests —
wrapper prefix and suffix, the trailing expression carrying no `;`, and
source-map offsets.

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
that no longer exists. The relation span is the region the diagnostic is actually
about.

## Tests

**Surface — falsify the positional recognizer.** Each is unrepresentable under
the current implementation and needs no new syntax.

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

**Internal — properties whose surface syntax does not exist yet.**

5. **Equivalent sink sets:** envelopes built from differently arranged programs
   over one relation compare equal under `EnvelopeSemantics`, independent of
   statement order.
6. **An empty root list emits nothing:** a directly constructed
   `ExecutionEnvelope` with zero roots produces no sinks and schedules no
   execution. A positive control with one root produces one sink, so the test
   cannot pass by finding nothing anywhere.
7. **One remote execution feeds ordered local sinks:** an envelope with three
   roots submits exactly one scheduler execution and yields three sinks in source
   order sharing one result identity, re-asserting the existing `Arc` invariant
   (`gui/state.rs:2103`) against position-independent roots.

**Refusals and preserved behavior.**

8. A view call in binding position refuses with its span **in an envelope
   buffer**; the identical binding in a buffer with no top-level root still
   returns `NoTerminalView` and reaches the local host. Both directions are
   asserted, since the scoping is the whole safety argument.
9. Nested view-named calls — inside `fn`, inside a block, inside `if` — are
   retained in the remote program and neither refused nor removed, preserving
   today's behavior.
10. `MixedRelations` and `DuplicateForm` (`:546`) keep their behavior under
    origin-resolved identity. Includes two identical inline expressions at
    different spans comparing as the same relation.
11. `diagnostic_fallback()` returns the relation span; an unmappable server error
    is reported against the relation, not against a view.

**Containment.**

12. The relation-verb assay described above, with its positive control.
13. `tests/architecture_test.rs:45` passes unchanged.

## Non-goals

- No `.views(...)`, `.encode(...)`, `EncodedViewSet`, or any new spelling; today's
  `table` / `plot.bar` / `plot.scatter` grammar is preserved exactly.
- **No new surface semantics.** No construct that is a Toron error today becomes
  a meaningful local value. Binding a view refuses; it does not silently bind.
- No views inside functions or control flow, and no local refusal for them.
- No relation typing, verb resolution, planning, or lowering in Polariton.
- No normalization of relation source text.
- No wire, protocol, `PlotSpec`, scheduler, or pane-routing change.
- No change to Toron, `toron-client`, or the local Rune host (`rune_host.rs`),
  which keeps its separate compile path for plain buffers.

## Review checklist

- [ ] The analysis answers only output-root and binding questions; the
      relation-verb assay passes with its positive control.
- [ ] Recognition is position-independent for top-level statements; nested
      view-named calls are retained, not refused.
- [ ] Every new refusal is scoped to envelope buffers, proven in both directions.
- [ ] Binding a view refuses; no program acquires a new meaning in this step.
- [ ] Equivalence compares `EnvelopeSemantics` — resolved origin, unspanned
      intents, order, execution count — never spans and never rendered bytes.
- [ ] Relation identity is source-text equality on the resolved origin;
      identical inline expressions at different spans are the same relation.
- [ ] `diagnostic_fallback()` replaces `view_block()` and returns the relation
      span.
- [ ] Serialization tests remain narrow: wrapper, trailing expression without
      `;`, and source-map offsets over N segments.
- [ ] `a_view_before_a_non_view_statement_is_not_in_the_trailing_block` is
      rewritten with a citation, not deleted.
- [ ] Existing envelope, fan-out, and architecture tests pass unchanged.
- [ ] Corpus rows are untouched; this step changes no status and no spelling.
