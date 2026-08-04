# ADR 319 — Implementation plan: q19 OR-key extraction

Pairs with `plans/adr/0319_q19_or_key_extraction.md`. Engine cut (`crates/toron` binder:
`recognize_cross_equi_joins` + a common OR-branch key extraction helper). Anchors current-tree.

## Phase 0 — confirm the shape
- q19's WHERE is a top-level `Expr::Or`, binary-nested for 3 disjuncts (`Or(Or(A,B),C)`), each an
  AND-conjunction containing `p_partkey = l_partkey`. `recognize_cross_equi_joins` splits only AND, so the OR is
  opaque, the comma join remains a keyless Cross, and the keyless-cross admission guard rejects it.
- Confirm via the q19 keyed probe (`Cross left_keys=0 right_keys=0`).

## D1 — common OR-branch key extraction (`binder/outer_join_demotion.rs`)
```text
fn split_logical_or_disjuncts(&Expr) -> Vec<Expr> // sibling of split_logical_and_conjuncts

fn extract_common_or_bridging_keys(expr, left_scope, right_scope) -> Option<(keys, residual_or)>:
  disjuncts = split_logical_or_disjuncts(expr)
  if disjuncts.len() < 2: return None
  per_branch = disjuncts.map(split_logical_and_conjuncts)
  branch_keys = per_branch.map(|conjuncts| orient Eq conjuncts with orient_bridging_equi)
  common_keys = oriented key pairs present in EVERY branch
  if common_keys.is_empty(): return None
  residuals = per_branch.map(|cs| AND(cs minus common key conjuncts) or TRUE if empty)
  return (common_keys, OR(residuals))
```
- Call this from `lift_cross_equi` after `left_scope` and `right_scope` are available. That keeps the cut
  relation-aware and lets `p_partkey = l_partkey` and `l_partkey = p_partkey` match as the same oriented key.
- Append extracted keys to the existing `keys` vector and append `residual_or` to the existing residual vector.
- Pure helper (no async); flatten-then-global-intersect (not pairwise). No-op outside OR-of-ANDs-with-common-key.
- Do not factor arbitrary common conjuncts. Side-local common filters stay in the residual OR in this cut.

## D2 — residual placement
- The factored `OR(residuals)` (cross-side: `part` + `lineitem`) stays a Filter **above** the keyed `Inner` join —
  not pushed to an input. Confirm `recognize_cross_equi_joins` leaves it as the residual conjunct above the lifted
  join.

## Tests
- **q19 keyed** — the q19 harness keyed-plan assertion flips to `Inner left_keys=1 right_keys=1` (not `Cross`).
- **Engine unit (`binder/tests.rs`):**
  - `(k∧A) OR (k∧B) OR (k∧C)` over a comma-join → keyed `Inner` on `k` + post-join `Filter(A OR B OR C)`; revert →
    keyless Cross (discriminating).
  - Reversed spelling in one branch (`left.k = right.k` vs `right.k = left.k`) still matches by oriented key pair.
  - **Safety:** `(k∧A) OR (j∧B)` (no common key) → unchanged; `(k∧A) OR (B)` (k in one branch) → `k` NOT lifted; a
    common non-key conjunct remains in the residual; a plain `k ∧ A` (no OR) → untouched; n-ary
    `Or(Or(k∧A, k∧B), k∧C)` flattens + lifts `k`.
  - Equivalence sanity: a small dataset where extracted-key vs explicitly factored select identical rows, including
    NULL join-key cases.
- **Architecture:** full `architecture_test` (recognizer is shared + guarded).

## Phase 3 — re-measure q19 + the matrix
```
scripts/cargo-cgroup run -p toron-tpch -- run --query q19 --mode dev --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- run --query q19 --mode strict --sf 0.01
```
- Expect `pass@execute` both modes (keyed memory/time) → reach 19; or a deeper measured blocker (record + name the
  next ADR). Confirm keyed via the assertion.
- Re-run `matrix` + `correctness-matrix`: **q19 flips; q15/q18 and all others unchanged**.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`) — FULL toron (shared binder path)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- run --query q19 --mode dev --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- run --query q19 --mode strict --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- matrix
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
git diff --check
```

## Results doc
`crates/toron/plans/impl/0319_q19_or_key_extraction_results.md`: q19 before/after keyed-plan summary
(`Cross`→`Inner`); q19 dev+strict new rows (pass@execute or deeper blocker, keyed memory/time); the matrix delta
(q19 only); the equivalence + safety tests; confirmation q15/q18/others unchanged.

## Risk / blast radius
- **Intersection-only** — lift strictly oriented bridging keys common to every disjunct (flatten-then-intersect);
  pin the safety tests (no-common-key, partial-common-key, common-non-key, no-OR all no-op).
- **Shared recognizer** — full `--no-fail-fast` + `architecture_test` are the net; the helper is a no-op outside
  the target shape.
- **Fake-green cartesian** — the keyed-plan assertion (Inner, not Cross) + sf=0.01 memory/time are mandatory.
- **Residual stays above** the keyed join (cross-side); don't push it into an input.

## Seal
`impl: adr 319 q19 or key extraction`. Do not commit wip drafts.
