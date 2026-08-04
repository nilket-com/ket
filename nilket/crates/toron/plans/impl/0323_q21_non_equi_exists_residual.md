# ADR 323 — Implementation plan: q21 non-equi correlated EXISTS residual

Pairs with `plans/adr/0323_q21_non_equi_exists_residual.md`. The final reach engine cut — classifier → bind → exec
(`crates/toron`), + the shared `o_orderstatus` fixture. **D2 (the in-probe residual) is the center.**

## Phase 0 — ground the exec residual path (before coding)
1. **Confirm the gap:** `on_filter` is absent from `exec.rs`; the semi/anti exec (exec.rs:3241/3575) is key-only.
   For inner joins a residual lowers to a post-join Filter; for semi/anti the right columns aren't emitted, so a
   cross-side residual is not output-filterable.
2. **The bound carrier:** trace how a keyed `LeftSemi`/`LeftAnti` is bound (the bound join + its probe); identify
   where to carry an optional `residual` match expression bound against *both* sides (left outer + right inner).
3. **Binder gate:** trace `push_on_clause_residual` (ADR 308). Today it rejects cross-side residuals; this cut must
   let the supported keyed `LeftSemi`/`LeftAnti` case bind as a join match residual while preserving the existing
   side-local pushdown behavior for ordinary joins.
4. **NOT-EXISTS match semantics:** locate the `LeftAnti` probe path; confirm where the residual composes so a left
   row is excluded only on `key && residual && inner-residual`, while the residual-absent ADR 258/259 NOT-IN
   null-guard/membership joins stay unchanged.
5. **Fixture:** confirm `o_orderstatus` absent; it resolves once D1/D2 move the `@plan` wall.

## D1 — classification (`sql_planner.rs` + binder deferred classifier)
- Add `ExistsConjunct::CorrelatedResidual(predicate)`: a conjunct that references the outer (so not `InnerResidual`)
  but is not an equi `CorrelatedPair`. Collect these alongside the equi `correlation_pairs` and the inner residuals.
- Mirror that state in `DeferredExistsConjunct` / `classify_deferred_exists_predicate`, so qualified frontend
  classification and binder-side unqualified/deferred classification stay semantically aligned.
- Require **≥1 equi `CorrelatedPair`** to admit; a non-equi-only correlation rejects with
  `unsupported_non_equi_exists_correlation` (a new precise reason in the classifier / telemetry).

## D2 — the executable residual (the center: bind + exec)
- **Logical carrier:** reuse `build_join_plan_with_filter`'s `on_filter` for the correlated residual, but retarget
  ADR 308's `push_on_clause_residual`: it must keep pushing side-local residuals as today, reject unsupported theta
  cases, and hand the supported keyed `LeftSemi`/`LeftAnti` cross-side residual to binding instead of rejecting it.
- **Bind:** carry the correlated-residual match expression onto the keyed `LeftSemi`/`LeftAnti`, bound against the
  combined left+right schema for in-probe evaluation.
- **Exec:** in the keyed semi/anti probe loop (exec.rs:3241/3575), for each probe key group evaluate `matched =
  key_matches && residual_is_true(left_row, right_row)`; emit/suppress the left row on that. **No cartesian, no
  full inner join** — the residual is checked within the already-keyed match, not by widening the join.
- Leave the **key-only path unchanged** when there is no residual (the optional residual is `None` for existing
  shapes).

## D3 — fixture
- Add `o_orderstatus` to the orders fixture (`OrderGenerator`, declared physical type) **after** D1/D2; rerun the
  full `toron-tpch` + goldens for no fixture regression.

## Tests
- **q21 keyed** — the q21 harness keyed-plan assertion: the EXISTS/NOT-EXISTS decorrelate to keyed semi/anti
  (`left_keys>=1`) on `l_orderkey`, not `Cross left_keys=0`.
- **NOT-EXISTS values test (the correctness gate):** a small dataset where `NOT EXISTS (… equi-key AND
  non-equi-residual AND inner-residual)` excludes a row **only** when a right row matches key+residual+inner —
  matched against a brute-force correlated reference. A key-only anti join gives a *different* (too-strong) answer
  (discriminating). Same for the EXISTS/`LeftSemi` case.
- **`unsupported_non_equi_exists_correlation`** — a non-equi-only correlated EXISTS rejects with that exact reason.
- **No-regression:** equi-only EXISTS (q04/q22) + the NOT-IN ADR 258/259 stack unchanged (the residual is `None`).
- **ADR 308 guard reconcile:** update any ON-residual architecture guard/comment that currently asserts all
  cross-side residuals reject; the new invariant is narrower: ordinary joins still reject theta semantics, while
  keyed semi/anti EXISTS residuals bind into the match predicate.
- **Architecture:** full `architecture_test`.

## Phase 3 — re-measure q21 + the matrix
```
scripts/cargo-cgroup run -p toron-tpch -- run --query q21 --mode dev    --sf 0.01 --allow-blocked
scripts/cargo-cgroup run -p toron-tpch -- run --query q21 --mode strict --sf 0.01 --allow-blocked
```
- **Success = q21 moves OFF `unsupported_query_surface@plan`** → keyed `pass@execute` both modes → **reach 22/22**;
  or a deeper measured blocker (record + name). Confirm keyed + tractable memory/time.
- Re-run `matrix` + `correctness-matrix`: q21 advances; **q11/q15/q18/q19 + q04/q22 + all others unchanged**.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`) — FULL toron (shared classifier + exec semi/anti + fixture)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- run --query q21 --mode dev --sf 0.01 --allow-blocked
scripts/cargo-cgroup run -p toron-tpch -- run --query q21 --mode strict --sf 0.01 --allow-blocked
scripts/cargo-cgroup run -p toron-tpch -- matrix
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
git diff --check
```

## Results doc
`crates/toron/plans/impl/0323_q21_non_equi_exists_residual_results.md`: the exec residual-aware semi/anti path; the
NOT-EXISTS values-test result; q21's new measured rows (off the wall → keyed green or deeper blocker);
`unsupported_non_equi_exists_correlation` for the non-equi-only case; the matrix delta (q21 only → reach 22);
confirmation q04/q22 / NOT-IN / `o_orderstatus` queries / q11/q15/q18/q19 unchanged.

## Risk / blast radius
- **D2 correctness** — the in-probe residual + anti-join match composition, while the residual-absent NOT-IN
  null-guard/membership path stays unchanged; the NOT-EXISTS values test is mandatory (key-only too strong;
  post-filter impossible).
- **Keyed-not-cartesian** — ≥1 equi pair; residual in-probe, never the sole key; assert keyed + sf=0.01 mem/time.
- **Shared exec semi/anti** — the optional residual leaves the key-only path unchanged; full `--no-fail-fast`.
- **Shared `o_orderstatus` fixture** — full `-p toron-tpch` + goldens.

## Seal
`impl: adr 323 q21 non equi exists residual`. Do not commit wip drafts.
