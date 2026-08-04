# ADR 331 - Results: q08/q09/q02 scale-dependent keyless broadcast-Cross

Pairs with `0331_scale_cut_q08_q09_keyed_cross.md`.

## Outcome

q08, q09, **and q02** now stay keyed at the scaled bind surface and all three official strict runs pass through
execute at `sf=1` under the default cap:

- q08: `phase=execute status=pass elapsed_ms=35868.5`
- q09: `phase=execute status=pass elapsed_ms=41044.6`
- q02: `phase=execute status=pass elapsed_ms=17040` (1.05 GB RSS - keyed, not a cartesian)

**q02 is a free beneficiary**, not separate work: it carries the same `(A x B) join F` non-key-edge shape, so the
same reassociation keyed it. It moved `bind-fail(unsupported_cross_join)@sf=1` -> `pass@execute`, and is guarded by
`strict_q02_stays_keyed_at_small_and_scaled` (same `assert_no_cross_join` canary as q08/q09). Net: this cut clears
**3** of the 3 cartesian cells in the ADR 330 atlas (q08/q09/q02), zero additional fix code.

## Grounding

The durable symptom was correct: q08/q09 were green at `sf=0.01` but could surface a keyless Cross at scale. The
implementation pass narrowed the decision site from "join-order cost model picked a bad broadcast edge" to the
comma-equi recognizer layer:

- The mediated 3-table shape `A, B, F WHERE F.a = A.k AND F.b = B.k` must remain all keyed, with no user-written
  predicateless Cross.
- A lifted parent join can still have a Cross child when both sides of that Cross independently key to the other
  relation. That is the `(A x B) join F` shape: the parent has usable keys, but leaving `A x B` as a child lets the
  broadcast admission path see a keyless Cross at scale.
- q08 also has side-local predicates/projection wrappers around the dimension side. Recognition had to push
  passthrough project filters down and then reassociate the lifted Cross child before strategy/admission.
- q05 caught the main regression risk: if a side-local predicate actually contains a nested Cross equi-key, hoisting it
  too early can strand the child Cross. The final cut guards early reassociation with a nested-Cross-equi check, so
  true child keys are pushed down first while simple side-local filters can still ride with the reassociated parent.
- `bind_with_explain_trace` was not applying the same comma-equi normalization as the normal bind path, so the
  compact trace probe could describe the wrong pre-normalized shape.

## Fix

- `outer_join_demotion.rs`
  - Push filter conjuncts through passthrough `Project`s before comma-equi recognition.
  - Reassociate lifted Cross children when both Cross children have equi-keys to the other relation.
  - Guard early reassociation when side-local predicates contain nested Cross equi-keys; this preserves q05's
    `customer/orders/lineitem` key discovery while still fixing q08/q09's lifted Cross child.
  - Preserve user-visible output order; the mediated `SELECT *` guard pins `A, B, F` order after reassociation.
- `binder.rs`
  - Run `recognize_cross_equi_joins` in `bind_with_explain_trace`, matching the normal bind path.
- `binder/explain.rs`
  - Add `bind_with_explain_summary` as a compact, doc-hidden probe helper for tests; no full trace dumps.
- `toron-tpch`
  - Add q08/q09 strict bind canaries at `sf=0.01` and `sf=1`: physical plan has no `Cross`, explain-summary edges
    have no `Cross`, and default cap binds.
  - Add a q05 strict bind canary at `sf=0.01` so the nested-Cross-equi path cannot regress silently.

No q08/q09 SQL constants changed. No broadcast cap was raised.

## Verification

```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup test -p toron mediated_join_stays_keyed_at_scale
scripts/cargo-cgroup test -p toron-tpch stays_keyed
scripts/cargo-cgroup test -p toron --test architecture_test
timeout 900s scripts/cargo-cgroup test -p toron --no-fail-fast
timeout 600s scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup fmt -- --check
git diff --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup build -p toron-tpch
timeout 300s target/debug/toron-tpch run --query q08 --mode strict --sf 1
timeout 300s target/debug/toron-tpch run --query q09 --mode strict --sf 1
target/debug/toron-tpch correctness --query q05 --sf 0.01
target/debug/toron-tpch correctness --query q08 --sf 0.01
target/debug/toron-tpch correctness --query q09 --sf 0.01
target/debug/toron-tpch correctness-matrix
target/debug/toron-tpch matrix
```

All passed.

`correctness-matrix` renders all 22 strict cells as `pass@compare`. The reachability `matrix` log renders q08/q09
strict as `pass@execute`; unrelated scale-atlas rows remain for q02/q04/q12/q20 and were not part of this cut.

## Not Run

Full live higher-SF sweeps were not run in this pass. Only the targeted official `sf=1` q08/q09 run commands were
refreshed after the fix.
