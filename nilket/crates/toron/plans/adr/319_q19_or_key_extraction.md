# ADR 319 — q19 OR-key extraction

## Status
Proposed.

## Context

Atlas engine cut #2 (post-q15). q19's measured wall (ADR 316): `unsupported_cross_join@bind` because the equi-key
`p_partkey = l_partkey` is buried *inside* each OR-branch — `recognize_cross_equi_joins` splits on **AND**
(`split_logical_and_conjuncts`), so a top-level `Expr::Or` is one opaque conjunct, the `lineitem × part` comma-join
stays a keyless Cross, and the broadcast-build-side guard rejects. The fix is not a general boolean normalizer:
extract only a bridging equality key that is present in every OR branch, exposing the key so the existing recognizer
can build the inner join.

```sql
WHERE (p_partkey = l_partkey AND A) OR (p_partkey = l_partkey AND B) OR (p_partkey = l_partkey AND C)
```

## Decision — extract common OR-branch join keys

A `crates/toron` binder cut in `recognize_cross_equi_joins` (outer_join_demotion.rs).

### D1 — common bridging-key extraction
- Keep the extraction inside `lift_cross_equi`, after the left/right relation scopes are available. A pre-scope
  textual rewrite would either miss reversed spellings (`l_partkey = p_partkey`) or factor predicates that are not
  join keys.
- For an `Expr::Or` conjunct, flatten the n-ary OR (`Or(Or(A,B),C)` → `[A,B,C]`) with a new
  `split_logical_or_disjuncts`, split each disjunct into AND-conjuncts, and use `orient_bridging_equi` to find
  left/right bridging equality candidates.
- Compute the intersection of **oriented key pairs** present in every disjunct. This is deliberately narrower than
  intersecting arbitrary `Expr` values: side-local filters, literals, and other common predicates are left in the
  residual for a later optimizer cut.
- If the oriented-key intersection is non-empty, lift those keys once and remove only those key conjuncts from every
  branch. Rebuild the remaining branch predicates as `residual_1 OR residual_2 OR ...` and leave that OR as a
  post-join residual filter.
- **Equivalence (load-bearing):** `(k∧A) OR (k∧B) OR (k∧C)` ≡ `k ∧ (A OR B OR C)` — valid only because `k` is in
  every disjunct. Extract strictly the global key intersection (flatten-then-intersect, not pairwise). A key present
  in only some branches must NOT be lifted. If a branch residual is empty, the residual OR collapses to `TRUE`;
  handle the shape even though q19's branches all have residuals.

### D2 — placement + residual
- The residual `(A OR B OR C)` references **both** `part` and `lineitem` columns (cross-side) → stays a Filter
  *above* the keyed join (not pushed to an input). q19 becomes `Filter(A OR B OR C)` over `Inner(lineitem ⋈ part on
  p_partkey = l_partkey)` → `SUM(discounted)`. Base relations only — rides q15's clearer keying substrate, no
  composite work.

## Acceptance
- **q19's `lineitem ⋈ part` keys as `Inner` (`left_keys=1 right_keys=1`), not `Cross`** — keyed-plan assertion
  (ADR 311 habit), the anti-fake-green-cartesian gate. Keyed memory/time at sf=0.01 (not a multi-GB cartesian).
- **q19 dev+strict move from `blocked(unsupported_cross_join@bind)` to `pass@execute`** — or to a deeper measured
  blocker (record + name the next ADR). Reach → 19.
- **Engine unit tests:** `(k∧A) OR (k∧B) OR (k∧C)` lifts `k` and keeps `A OR B OR C` as the residual; revert →
  keyless Cross (discriminating). **Safety:** a key common to only *some* disjuncts is NOT lifted; common non-key
  conjuncts are NOT factored; no-common-key OR and no-OR predicates are untouched.
- Full `-p toron --no-fail-fast` + `--all-targets` + `architecture_test` green (the recognizer is a shared path);
  **q15/q18/all other reach + correctness cells unchanged** (the extraction fires only for top-level
  OR-of-ANDs-with-a-common-bridging-key).

## Risks
- **Intersection-only safety** — lift strictly oriented bridging keys common to *every* disjunct
  (flatten-then-intersect); over-lifting corrupts the predicate. A conservative under-match leaves q19 red, which is
  safer than a wrong answer.
- **Shared recognizer** — the extraction runs for every `Filter`-over-Cross; a no-op for non-OR /
  no-common-key predicates. Full suite is the net.
- **Residual placement** — the cross-side `(A OR B OR C)` stays a Filter above the keyed join; don't push it into
  an input.

## Forward arc
q19 → keyed-green (reach 19) or a deeper measured blocker. Keyless-cross family then fully closed (q15 composite +
q19 disjunction). Atlas remaining: q11 (HAVING-scalar) → q18 (grouped EXISTS-body, then re-measure the composite
path against q15's A1 substrate) → q21 (non-equi EXISTS).

## Why toron-home
A `crates/toron` binder cut (join-key extraction in `recognize_cross_equi_joins`). q19's row flips in
`toron-tpch` with no harness logic change. No Wendelon / cross-ring surface.

## Grounding notes
The seam is `recognize_cross_equi_joins`: `lift_cross_equi` already promotes top-level bridging equalities, and the
q19 failure is that OR nesting hides the equality from that existing path. n-ary disjunctions (`Or(Or(A,B),C)`) must
be handled by flatten-then-global-intersect rather than pairwise bottom-up, or the implementation can accidentally
lift a key that is absent from a later branch.
