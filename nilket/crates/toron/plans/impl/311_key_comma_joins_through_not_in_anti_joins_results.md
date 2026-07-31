# ADR 311 results - key comma joins through NOT IN anti-joins

## Outcome

q16 is now a true keyed reach cell.

```text
q16 dev    sf=0.01 pass@execute elapsed_ms=41.3 digest=ArrowIpcDigest([164, 183, 72, 40, 46, 101, 81, 163, 233, 202, 133, 17, 254, 134, 141, 48, 25, 145, 205, 121, 138, 19, 238, 208, 235, 133, 137, 224, 90, 240, 14, 132])
q16 strict sf=0.01 pass@execute elapsed_ms=35.8 digest=ArrowIpcDigest([164, 183, 72, 40, 46, 101, 81, 163, 233, 202, 133, 17, 254, 134, 141, 48, 25, 145, 205, 121, 138, 19, 238, 208, 235, 133, 137, 224, 90, 240, 14, 132])
```

ADR 310 measured the same digest, but through a materialized cartesian: about 8.25 GB RSS and 75.9s/101.3s under
`scripts/cargo-cgroup` at `sf=0.01`. ADR 311 keeps the digest and changes the shape: the q16 default-scale harness
run now completes in tens of milliseconds, and the focused engine test proves the underlying comma edge keys.

## Corrected mechanism

The first ADR 311 draft framed q16 as `DeferredExists`. Current lowering shows the real shape:

1. `ps_suppkey NOT IN (...)` lowers through ADR 258 to a null-`x` filter, a null-guard `LeftAnti`, and a
   membership `LeftAnti`.
2. The residual `p_partkey = ps_partkey` is above that anti-join stack.
3. Without this cut, `recognize_cross_equi_joins` recurses into the anti-join children but never pushes the
   left-output-only residual through the anti joins, so `partsupp, part` remains a keyless `Cross`.

The fix adds a `Filter`-over-`LeftSemi`/`LeftAnti` path in `recognize_cross_equi_joins`: unambiguous left-only
conjuncts move into the semi/anti left child, through exchanges and stacked filters, then recognition recurses.
Ambiguous, right-only, or cross-side conjuncts stay above.

## Tests

New or retargeted gates:

- `cross_join_test::adr311_not_in_filter_pushes_comma_equi_through_left_anti_stack`
  - uses oversized build inputs;
  - fails without the push-through as a keyless CROSS JOIN broadcast-eligibility error;
  - now binds with one keyed `Inner`, zero `Cross`, and the two `LeftAnti` joins preserved.
- `exists_decorrelation_test::not_in_over_comma_join_matches_explicit_keyed_join`
  - proves the comma form and explicit keyed join form produce the same digest under `NOT IN`.
- `harness::q16_executes_in_both_modes_at_default_scale`
  - now runs q16 at `sf=0.01`, not `sf=0.0001`;
  - pins the unchanged default-scale digest for dev and strict.

Existing fixture fact retained:

- `harness::q16_default_fixture_is_nonempty_but_anti_join_is_not_live`
  - q16 result groups are non-empty at `sf=0.01`;
  - `%Customer%Complaints%` excludes no suppliers at this scale, so the correctness oracle still needs a larger
    scale or synthetic coverage for the exclusion branch.

## Board impact

q16 moves from "pass@execute via cartesian" to keyed `pass@execute`.

```text
reach: q01/q02/q03/q04/q05/q06/q07/q08/q09/q10/q12/q13/q14/q16/q17/q20/q22  (17)
correctness: unchanged
```

## Gate

Focused gates run during implementation:

```text
scripts/cargo-cgroup test -p toron --test cross_join_test adr311_not_in_filter_pushes_comma_equi_through_left_anti_stack
scripts/cargo-cgroup test -p toron --test exists_decorrelation_test not_in_over_comma_join_matches_explicit_keyed_join
scripts/cargo-cgroup test -p toron-tpch --test harness q16
scripts/cargo-cgroup run -p toron-tpch -- run --query q16 --mode dev --sf 0.01 --allow-blocked
scripts/cargo-cgroup run -p toron-tpch -- run --query q16 --mode strict --sf 0.01 --allow-blocked
```

Full seal gate:

```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron --lib                  # 1118 passed
scripts/cargo-cgroup test -p toron --test architecture_test # 2055 passed
scripts/cargo-cgroup test -p toron --no-fail-fast          # green
scripts/cargo-cgroup test -p toron-tpch                    # green
scripts/cargo-cgroup run -p toron-tpch -- matrix           # q16 dev/strict pass@execute
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix # unchanged
git diff --check
```

## Forward arc

q16 correctness is the natural rail catchup. q11/q15 remain likely-green reach coverage; q19/q21 remain the
heavier reach frontier.
