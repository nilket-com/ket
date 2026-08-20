# ADR 570 - SIMILAR TO

**Status:** Proposed 2026-08-20.

## Context

`SIMILAR TO` is the standard's regular-expression predicate and the last core
predicate the planner refuses outright. It is not LIKE: the pattern language
adds alternation, grouping, repetition, and character classes; and it is not
POSIX search: the match is anchored to the whole value. The engine carries no
regex dependency, and this record does not add one — the closed vocabulary is
small enough to compile ourselves, and owning the matcher is what makes the
next decision enforceable.

## Decision

### 1. A closed, constant-only vocabulary

The accepted pattern language is exactly: `%` (any string), `_` (any
character), `[...]`/`[^...]` character classes with ranges, `|` alternation,
`(...)` grouping, `*`, `+`, `?`, and bounded repetition `{n}`, `{n,}`,
`{n,m}`. Everything else matches literally. `ESCAPE` is supported in SQL with
the single-character rule LIKE uses; the escaped character always matches
literally. POSIX named classes (`[[:alpha:]]` and kin) are refused by name
with a directive naming the supported class syntax — refused, not silently
treated as literals, because a pattern that silently changes meaning is worse
than one that fails.

The pattern and the escape are plan-time string literals; a non-literal
pattern is rejected where TO_CHAR's format already is. Compilation happens at
bind, so a malformed pattern fails the statement before execution, once, with
a position-bearing message.

### 2. Matching is a non-backtracking Thompson construction

The pattern compiles to a Thompson-style NFA and matching is simulated with a
state set — worst case `O(states × input length)`, no backtracking, so no
pattern can exhibit catastrophic runtime. Nested repetition — `(a*)*b`, the
classical backtracking bomb — is the discriminating control: it must answer
correctly on a long non-matching input within a generous wall-clock bound
that a backtracking implementation would exceed by astronomical orders.

Bounded repetition expands by duplication at compile, so the automaton's size
is a resource commitment: the compiled program is capped at 4,096 states, and
a pattern that expands past the cap — `a{5000}` — is rejected at bind with a
directive naming the cap. The cap is a named constant with a boundary
control, not an implementation accident.

### 3. Both spellings share one matcher; matching is anchored and char-based

`NOT SIMILAR TO` negates the boolean of the same compiled program — there is
no second automaton to diverge. The match consumes the entire value (both
ends anchored); positions and classes are Unicode characters, not bytes. NULL
input yields NULL for both spellings; the compiled pattern is constant and
non-null by rule 1.

### 4. One bound predicate node

`Expr::SimilarTo` and `BoundExprNode::SimilarTo` mirror LIKE's node shape —
grammar, not a registry scalar — carrying the pattern, the escape, and the
negation. Rune spells it `similar_to(pattern)` / `not_similar_to(pattern)` on
expressions; the Rune surface takes no escape argument, since an author
controls the pattern text directly.

## Acceptance

- Vocabulary battery: each operator alone and composed — `%`/`_` mixing with
  alternation, classes with ranges and negation, grouping under repetition,
  `{n}`/`{n,}`/`{n,m}` — against matching and non-matching values, both
  spellings.
- Full-string anchoring: `'abc' SIMILAR TO 'b'` is false while `'%b%'` is
  true; a pattern matching a prefix only does not match.
- ESCAPE control: an escaped `%` matches a literal percent; the escape
  character itself is escapable.
- Named-class refusal: `[[:alpha:]]` fails at plan/bind with the directive;
  malformed patterns (unclosed class, dangling repetition, bad bounds) fail
  at bind with position-bearing messages.
- The nested-repetition control: `(a*)*b` against a long `aaaa…a` input
  answers false within the wall-clock bound.
- The state-cap boundary: a pattern at the cap compiles; one past it is
  rejected with the directive.
- NULL propagation over a nullable column, both spellings; SQL and Rune reach
  one node, digest-verified.

## Guardrails

An architecture guard pins the matcher module against backtracking: the
simulation must not contain recursive self-calls over pattern positions, and
the compile must reference the state cap. The vocabulary is closed — a new
operator enters through its own decision, not through the compiler quietly
accepting more syntax.

## Risks and deferrals

Case-insensitive SIMILAR TO (no standard spelling exists) is out of scope.
Byte-positioned matching and binary patterns wait for a binary type.
`SUBSTRING(... SIMILAR ... ESCAPE ...)` (regex substring extraction) is a
separate admission. RLIKE/REGEXP stay rejected: they name a different, larger
language whose vocabulary would be its own record.

## Forward

The compiled-automaton module is the seam a future regex family (RLIKE,
regex extraction) would extend — with its own vocabulary record and the same
non-backtracking obligation.
