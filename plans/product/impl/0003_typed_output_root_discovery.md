# Typed output-root discovery

Scope: replace the envelope's positional trailing-block recognizer with an
output-root analysis over the top-level statement list, keeping today's
`table` / `plot.bar` / `plot.scatter` spelling unchanged. This is step 1 of the
three-step sequence recorded in `../corpus/rune_workspace_corpus.md` v0.5: it
establishes the semantic boundary so that `.views(...)` and `.encode(...)` can
later be lowering rules over a sound analysis rather than parser tricks.

No new surface syntax, no wire change, no Toron change. Deliverable is one
`impl:` commit.

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

## Scope boundary

Output roots are **top-level statements only**, regardless of position. A view
call nested inside a function body, a block, or any control flow is out of scope
for this step.

Nested view calls must **refuse explicitly**, not be silently ignored. Today a
non-trailing view simply terminates the block and is shipped to Toron, where it
fails as an unknown callee; after this change, position no longer excuses
silence. A recognized view callee appearing anywhere other than a top-level
statement is a typed refusal naming the span and stating that views are
top-level in this release. Silent omission would let a user write `if x {
table(r) }` and receive an empty pane with no explanation.

This keeps the change contained while decisively falsifying the trailing-block
recognizer: the acceptance cases below are unrepresentable under positional
recognition and require no new syntax.

## Analysis

Replace the reverse walk with `None => break` (`rune_relation_envelope.rs:209-216`)
with a forward pass classifying every top-level statement into one of:

| classification | example | effect |
|---|---|---|
| output root | `table(r)` · `plot.bar(r,"cat","val");` | contributes a sink; removed from the remote program |
| view binding | `let v = table(r);` | contributes no sink; removed from the remote program |
| ordinary statement | `let r = workspace.table("t");` · `let n = 1;` | retained in the remote program, in order |

Consequences of the classification:

- **Position stops mattering.** Roots are collected wherever they appear and
  ordered by source position, preserving today's source-order sink contract.
- **Binding is lazy.** A view in binding position emits nothing. This is the
  laziness rule stated in the corpus, expressible today because `let v = table(r);`
  already parses; only its meaning changes.
- **A reference to a view binding is a typed refusal.** The analysis knows the
  binding's kind, so `table(v)` where `v` is a view refuses locally with both
  spans rather than shipping a nonsense program to Toron. Step 3 gives that
  reference a meaning; step 1 must not guess one.
- **Relation identity moves from text to binding.** `MixedRelations`
  (`:223-232`) currently compares source-text slices. It becomes: two roots agree
  when their relation arguments resolve to the same binding, or — when the
  argument is an inline expression — to the same span. Text equality is retained
  only as the inline-expression case, so no existing acceptance changes.

`DuplicateForm` (`:233-241`) and the arity and string-literal rules
(`:318-332`, `:349-391`) are unchanged.

## Split analysis from serialization

`transform` (`:205`) currently parses, recognizes, and splices in one pass, and
its product is a `String`. Split it:

```text
analyze(source) -> Result<ExecutionEnvelope, RuneEnvelopeError>
render(source, &ExecutionEnvelope) -> TransformedRuneView
```

`ExecutionEnvelope` is the comparable structure and holds no rendered bytes:

- the ordered spans of top-level statements retained for the remote program;
- the span of the relation expression that becomes the trailing expression; and
- the ordered output roots as `SpannedRuneViewIntent`.

**Equivalence is asserted on `ExecutionEnvelope`, before serialization.** Two
programs are equivalent when their retained statement structure, relation, and
root list agree — not when their rendered Rune bytes match. Byte comparison would
make the equivalence suite a formatter test that fails for reasons nobody cares
about. Serialization keeps its own narrow tests: wrapper prefix and suffix, the
trailing expression carrying no `;`, and source-map offsets.

`render` generalizes today's three-segment splice (`:247-277`). Removing roots
from arbitrary positions produces N retained regions rather than one prefix and
one suffix, so `RuneEnvelopeSourceMap` becomes an ordered segment list built from
the retained spans. `map_span` (`:83`) keeps its signature and its `None` for
wrapper-only spans; `view_block()` (`:98`) becomes the span of the first root,
since there is no longer a single contiguous block.

## Tests

**Surface — falsify the positional recognizer.** Each is unrepresentable under
the current implementation and needs no new syntax.

1. **Inline versus bound relation** produce equivalent envelopes:
   `table(workspace.table("t").sort([asc(col("c"))]))` against the same relation
   bound to `r` and referenced. Compared on `ExecutionEnvelope`.
2. **Interleaved view statements** — a view, an ordinary statement, another view
   — yield both sinks in source order, with the ordinary statement retained in
   the remote program.
3. **Views before unrelated statements** — the direct inversion of
   `a_view_before_a_non_view_statement_is_not_in_the_trailing_block` (`:569`).
   That test asserts today's contiguity rule and must be rewritten, not deleted;
   the rewrite asserts the view is now recognized and the trailing statement
   retained. Its replacement cites this plan so the reversal is deliberate and
   reviewable rather than looking like a regression.
4. **Trailing block still works** — the existing three-view case (`:500`) passes
   unchanged, proving the generalization is a superset.

**Internal — properties whose surface syntax does not exist yet.**

5. **Equivalent sink sets:** envelopes built from differently arranged programs
   over one relation compare equal on root list and relation, independent of
   statement order.
6. **Bound-but-unemitted emits nothing:** `let v = table(r);` produces zero
   output roots, and the statement does not appear in the retained region list.
   A positive control with `table(r)` as an expression statement produces one
   root, so the test cannot pass by finding nothing.
7. **One remote execution feeds ordered local sinks:** an envelope with three
   roots submits exactly one scheduler execution and yields three sinks in source
   order sharing one result identity. This re-asserts the existing `Arc`
   invariant (`gui/state.rs:2103`) against position-independent roots.

**Refusals.**

8. Nested view calls — inside `fn`, inside a block, inside `if` — refuse with the
   span and a message naming the top-level restriction. A top-level positive
   control proves the refusal is not blanket.
9. A reference to a view binding refuses locally with both spans and never
   reaches the transport.
10. `MixedRelations` and `DuplicateForm` (`:546`) keep their behavior under
    binding-based identity, including the inline-expression case.

**Containment.**

11. The relation-verb assay described above, with its positive control.
12. `tests/architecture_test.rs:45` passes unchanged.

## Non-goals

- No `.views(...)`, `.encode(...)`, `EncodedViewSet`, or any new spelling; today's
  `table` / `plot.bar` / `plot.scatter` grammar is preserved exactly.
- No views inside functions or control flow; those refuse.
- No meaning for a bound view beyond "emits nothing"; step 3 assigns one.
- No relation typing, verb resolution, planning, or lowering in Polariton.
- No wire, protocol, `PlotSpec`, scheduler, or pane-routing change.
- No change to Toron, `toron-client`, or the local Rune host (`rune_host.rs`),
  which keeps its separate compile path for plain buffers.

## Review checklist

- [ ] The analysis answers only output-root and binding questions; the
      relation-verb assay passes with its positive control.
- [ ] Recognition is position-independent for top-level statements; nested views
      refuse with spans rather than being ignored.
- [ ] `analyze` and `render` are separate, and equivalence tests compare
      `ExecutionEnvelope`, never rendered bytes.
- [ ] Serialization tests remain narrow: wrapper, trailing expression without
      `;`, and source-map offsets over N segments.
- [ ] `a_view_before_a_non_view_statement_is_not_in_the_trailing_block` is
      rewritten with a citation, not deleted.
- [ ] Bound views emit nothing and are excluded from the remote program, proven
      against a positive control.
- [ ] Three roots still produce one execution and three source-ordered sinks
      sharing one result identity.
- [ ] Existing envelope, fan-out, and architecture tests pass unchanged.
- [ ] Corpus rows are untouched; this step changes no status and no spelling.
