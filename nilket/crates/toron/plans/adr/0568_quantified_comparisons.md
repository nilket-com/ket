# ADR 568 - Quantified comparisons: ANY, SOME, ALL

**Status:** Proposed 2026-08-20.

## Context

`x op ANY (subquery)` and `x op ALL (subquery)` are the last core-standard
predicate family with no arm — they die in the generic unsupported-expression
fallthrough. The machinery they reduce to already exists: `IN (subquery)` and
`NOT IN (subquery)` lower to a semi join and the null-aware anti-join desugar
(ADR 256/258), and correlated scalar aggregates decorrelate to grouped
aggregates keyed on the correlation with a `LeftOuter` recombine (ADR 260),
with COUNT's empty group coalesced to zero.

The feasibility gate for this record was composition: the ordering forms need
two or three correlated scalar aggregates in one predicate, each keyed on its
own correlation. The gate probe — two correlated scalars (COUNT and MIN) in
one WHERE over per-key-varying groups, where a key-blending defect would
compute a global MIN and admit extra rows — passes against a hand-computed
expectation and rides this cut as a named regression test. The scalar pull-up
walker also descends into CASE, which is where the desugar places its
subqueries. No narrowing is needed: correlated ordering forms are in scope.

## Decision

### 1. SOME is the canonical alias of ANY

The parser folds `SOME` into the ANY form; both spellings reach one desugar
and cannot diverge.

### 2. `= ANY` is IN and `<> ALL` is NOT IN, by rewrite

A pre-lowering rewrite turns `x = ANY (S)` into the `IN (subquery)` form and
`x <> ALL (S)` into `NOT IN (subquery)`, so both inherit ADR 256/258 wholesale:
the semi join, the null-aware anti desugar, and the existing boundary that a
correlated membership subquery is deferred. No second membership
implementation exists to drift.

### 3. The eight ordering forms reduce to keyed scalar aggregates under
explicit three-valued tables

`x op ANY/ALL (S)` for `op ∈ {<, <=, >, >=}` desugars — before scalar
pull-up, in SQL-expression space — to a CASE over three scalar aggregates of
the same subquery: `COUNT(*)` (cnt), `COUNT(v)` (cntv), and the witnessing
extreme m — MIN for `> / >= ANY` and `< / <= ALL`, MAX for the other four.
The tables, with UNKNOWN spelled NULL:

- **ANY**: empty → FALSE; `x op m` → TRUE; `NOT (x op m)` and `cnt = cntv` →
  FALSE; otherwise NULL. (A NULL probe, or a NULL-bearing set with no
  witness, falls through the two comparisons into NULL.)
- **ALL**: empty → TRUE; counterexample `NOT (x op m)` → FALSE; `x op m` and
  `cnt = cntv` → TRUE; otherwise NULL.

Each table's TRUE/FALSE/NULL corners — empty set, NULL probe, NULL-bearing
set with and without a witness, and the plain hit and miss — are acceptance
rows. Correlated subqueries ride ADR 260's keyed decorrelation; uncorrelated
ones stay the keyless cross recombine.

### 4. `= ALL` and `<> ANY` are refused with directives

Their reductions ("every value equals x", "some value differs") are the
fiddly, rarely-written corner of the family. Per the ADR 563 precedent they
are refused by name — the error explains the meaning and suggests the
rewrite — rather than shipped with a half-argued table. A name leaves the
refusal only through its own decision.

## Acceptance

- `x = ANY (S)` returns exactly `x IN (S)`'s rows on the same data; the
  `SOME` spelling matches the `ANY` spelling; `x <> ALL (S)` matches
  `NOT IN`, including the poisoned case where S contains NULL.
- Every ordering form's table corners, uncorrelated: empty → FALSE/TRUE by
  quantifier; hit; miss with and without NULLs in S; NULL probe → NULL row,
  projected so NULL and FALSE are distinguishable.
- A correlated ordering form with per-key variance returns hand-computed
  per-row results, where key blending would return different rows — the gate
  probe generalized.
- `= ALL` and `<> ANY` are refused with their directives; a typo still gets
  the generic error.
- The desugared plan contains no new execution operator: joins, aggregates,
  and CASE only.

## Guardrails

The desugar is the only implementation — no new `Expr` variant, no executor
surface. The refusal set is exact-set guarded like ADR 563's.

## Risks and deferrals

The CASE references the probe expression and the count subquery more than
once, and each written subquery pulls up as its own join. Both duplications
are the latent hazard class already recorded for the null-handling sugars
(no `PerCall` function can express a live defect today); the shared-binding
fix and aggregate fusion are one named follow-up serving both. Expression-
position quantified comparisons (SELECT list) stay rejected with the rest of
expression-position subqueries. Row-valued probes and quantified comparisons
over VALUES lists are out of scope.

## Forward

This closes the quantified-predicate family; the remaining core structural
gaps are expression-position subqueries and `SIMILAR TO`.
