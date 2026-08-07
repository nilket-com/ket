# ADR 0040: static literal binding resolution

Status: accepted 2026-08-06. This record decides one thing: that a recognized
presentation argument may be written as a name bound to a literal, resolved
statically in the envelope. It authorizes no Rune evaluation, no new element, and
no change to what any element means once its value is known.

## Context

`../uat/test13.rn` factors two figures' shared headings into bindings and refuses:

```rune
let plot_title = "🌟SQL";
let plot_subtitle = "Cut-Paste-Plot";

figure([ title(plot_title), subtitle(plot_subtitle), … ])
```

PRODUCT-0016 §4 appears to forbid this, but its argument does not reach here. It
deferred *computed* headings because "the envelope lowers statically, before
dispatch, so a computed heading would require both evaluating Rune and carrying
the resulting value back into presentation — a value that does not exist when the
panel is established."

A name bound to a literal is not that value. It exists in the source, at parse
time, as a literal. What is missing is a lookup, not an evaluation. This record
separates two cases PRODUCT-0016 §4 lumped together:

```text
let t = "SQL";     title(t)      statically resolvable — a named literal
let t = compute(); title(t)      not — requires evaluation
                   title(a + b)  not — requires evaluation
```

The machinery also exists. `resolve_relation_origin` (`:2502`) already resolves a
relation binding to its initializer text, selecting the most recent
`LocalBinding` (`:600`) whose `statement_start` precedes the use site, with
`ViewShadows` handling collisions. This record applies the same lookup to a
different initializer shape.

## Decision

### 1. A presentation argument may be a name bound to a literal

> A literal argument in a recognized presentation position may be written as a
> name that resolves — through preceding top-level bindings and aliases — to a
> literal of the required kind. Resolution is static. No Rune is evaluated.

```rune
let heading = "🌟SQL";
let ceiling = 7;
let step    = 1;

figure([
    title(heading),
    y_bounds(0, ceiling),
    y_tick_step(step),
    bar("cat", "val"),
])
```

### 2. A literal initializer includes a negated numeric literal

An initializer qualifies when it is an unsuffixed string or numeric literal, or
one unary negation directly wrapping an unsuffixed numeric literal — the grammar
PRODUCT-0036 §2 fixed for call arguments, applied to the right-hand side of a
`let`.

Without the negation clause, `let floor = -1; y_bounds(floor, 8)` would refuse
and every negative bound would be unfactorable. That is the same shape as the
finding PRODUCT-0036's Stage 0 made about call arguments, and it is cheaper to
inherit than to rediscover.

### 3. Aliases resolve transitively, with a bound

`let a = "x"; let b = a; title(b)` resolves. Each step follows §1's rule, and the
selection rule is `resolve_relation_origin`'s: the most recent binding of that
name whose statement precedes the use site.

Resolution carries a visited set, or an equivalent depth bound. A chain that
revisits a name refuses rather than recursing. Rune's scoping makes a true cycle
unlikely, but `let a = a;` under shadowing is reachable, and a bound turns a
potential hang into a refusal — PRODUCT-0037 §6's reasoning one level down.

### 4. Refusal attributes to the use site and names the binding

A name that resolves to the wrong kind, to a non-literal, or to nothing refuses
**at the argument's span**, and the message names the binding, its span, and what
it resolved to.

```text
y_bounds(0, ceiling): `ceiling` is bound at 12..28 to a string literal;
                      a numeric literal is required
```

The argument span attributes because that is where the author's figure is; the
binding is named because that is where the mistake is. Two spans in one refusal
follows `DuplicateName { first, duplicate }` (`:425`), the existing precedent.

"Requires a numeric literal" alone would describe the call site while the fix is
three lines up.

### 5. Bindings are retained, never peeled

PRODUCT-0008's peeling removes presentation-bearing statements from the program
sent to Toron. A `let` bound to a literal is not presentation-bearing: it is an
ordinary binding that the relation program may also use, and proving otherwise
would require exactly the whole-program analysis the envelope is built to avoid.

It is therefore retained. An unused string binding in the remote program does
nothing, and a used one keeps working.

### 6. Figure elements first; the boundary is coverage, not meaning

§1's rule is stated over any recognized presentation position. This cut
implements it for **figure elements** — `title`, `subtitle`, `skin`, `palette`,
`x_bounds`, `y_bounds`, `x_tick_step`, `y_tick_step`, `x_minor_ticks`,
`y_minor_ticks`.

Mark arguments — `bar(cat_field, "val")` — are **not** implemented here, and
neither are register modifier arguments: `table().skin(s)` and numeric modifier
arguments such as `.point_size(sz)`. Each is a coverage limit rather than a
semantic boundary. (The register cases were unnamed when this record was first
accepted; named 2026-08-06 by the architectural review's record-accuracy pass
so their exclusion reads as coverage, not decision.) A bound field name resolves
to its literal before PRODUCT-0007 §2's channel resolution runs, so nothing
downstream can distinguish it from the literal spelled inline. Extending
coverage later is the same rule applied further and needs no record, the way
PRODUCT-0028 left curated categorical palettes addable without one.

### 7. Nothing about any element's meaning changes

Once a value is resolved, every rule that governs it is unchanged: PRODUCT-0016's
heading semantics, PRODUCT-0036's `min < max` and finiteness, PRODUCT-0038's
positive step, PRODUCT-0039's non-negative integer count and capacity,
PRODUCT-0025's theme identity, and every applicability and capacity refusal.

A resolved value is refused by exactly the rule that would have refused it spelled
inline, at the same phase. `let step = -1; y_tick_step(step)` fails
PRODUCT-0038's positivity rule at lowering, not a new one.

### 8. A shadowing name in argument position is fine

`let title = "x"; figure([title(title)])` resolves: callee position is the
constructor, argument position is the name, and PRODUCT-0014 §2's
position-determines-role rule disambiguates them without widening `ViewShadows`.

Recorded because it looks like it should need special handling and does not.

### 9. What this record does not authorize

No evaluation of any Rune expression, including constant folding, string
concatenation, arithmetic, and calls. No non-top-level bindings. No mutation or
reassignment semantics beyond the existing most-recent-before-use selection. No
new figure element, mark, or modifier. No change to peeling, execution,
identity, placement, lifecycle, or any element's semantics.

## Acceptance gates

1. `../uat/test13.rn` runs as written, with both figures resolving the same two
   heading bindings.
2. Every figure element in §6 accepts a bound name of the correct kind, asserted
   per element.
3. `let floor = -1; y_bounds(floor, 8)` resolves to `-1.0`.
4. `let a = "x"; let b = a; title(b)` resolves through the alias chain; a chain
   that revisits a name refuses rather than recursing.
5. The most recent binding preceding the use site wins, matching
   `resolve_relation_origin`'s existing selection.
6. Each of these refuses at the **argument's** span, naming the binding, its
   span, and what it resolved to: a wrong-kind literal, a call initializer, an
   arithmetic initializer, and an unbound name.
7. A resolved value is then governed by its element's existing rules —
   `let step = -1; y_tick_step(step)` fails PRODUCT-0038's positivity rule with
   that rule's diagnostic, and `let m = 5; y_bounds(m, m)` fails PRODUCT-0036's
   ordering rule.
8. `let title = "x"; figure([title(title)])` resolves.
9. Literal bindings are retained in the program sent to Toron; no peeling
   behavior changes.
10. Inline literals lower identically to before; every existing envelope test
    passes unmodified.

## Rejected

- **Restricting this to headings:** `let ceiling = 7; y_bounds(0, ceiling)` is
  the same factoring, and refusing it would be arbitrary. One rule, stated once.
- **Evaluating expressions:** PRODUCT-0016 §4's deferral stands for anything that
  is not already a literal in the source. Constant folding is the thin end of it —
  `1 + 1` requires an evaluator, and an evaluator requires deciding what runs
  where.
- **Peeling literal bindings:** would require proving a binding is
  presentation-only, which is whole-program analysis the envelope avoids by
  construction.
- **Attributing refusal to the binding's span:** the author's figure is the use
  site, and a diagnostic that points only at a `let` three lines up loses the
  context that makes it actionable. §4 names both.
- **Widening `ViewShadows` for element names:** unnecessary; §8's positional rule
  already disambiguates.
- **Non-top-level bindings:** the existing collection is top-level, and block
  scoping introduces a resolution question this record does not need to answer.

## Consequences and sequencing

- The first record in this arc that makes the language *smaller* to write rather
  than more capable: nothing new can be expressed, and repetition stops being
  mandatory.
- `resolve_relation_origin`'s binding machinery gains a second consumer, having
  served relations alone since PRODUCT-0003.
- Mark arguments become a coverage extension available at any time, needing no
  record.
- A future properties panel must represent "this value is shared" rather than
  silently resolving it — editing a bound heading changes every figure that uses
  it, and replacing the reference with a literal un-factors what the author
  deliberately factored. That is a real cost of this record, and it is smaller
  than mandatory repetition.
