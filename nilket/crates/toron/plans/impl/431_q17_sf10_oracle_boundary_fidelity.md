# Implementation Plan — ADR 431: q17 sf=10 oracle boundary fidelity

## Phase 0 — structural census

1. Add separate per-part aggregate and q17-selected-row diagnostic queries to `toron-tpch`, and run
   them through the existing harness execute path.
2. Extend the generator oracle with typed per-part aggregates, the rounded selected-row multiset, and
   an oracle-only exact-rational-versus-rounded boundary receipt.
3. Sort and compare the common typed structures exactly. Persist deterministic FNV-1a summaries only
   as evidence.
4. Add focused tests proving exact comparison catches a changed aggregate or selected row, and a
   synthetic boundary case increments the membership-difference count.
5. Record the census and branch according to ADR 431. Only clean outcome 1 continues.

## Phase 1 — scale policy

1. Add a pure `q17_correctness_scale(Option<f64>)` validator: absent → 0.02; explicit 0.02 and 10 →
   accepted; all other scales → `q17_sf10_oracle_policy_gap`.
2. Route the CLI selector and data-source correctness gate through it.
3. Parameterize the memory q17 correctness route so it no longer silently hardcodes 0.02.
4. Replace the empty-data-root policy-gap test with pure policy tests. The live sf=10 comparison is
   not a unit test.

## Phase 2 — live gate

Under the unchanged 4 GiB / 600 second envelope and fresh scratch, run:

```text
target/release/toron-tpch perf-run-one --engine toron --query q17 --mode strict --sf 10 \
  --data-source parquet --data-dir target/tpch/data
target/release/toron-tpch correctness --query q17 --sf 10 \
  --data-source parquet --data-dir target/tpch/data
```

Require one bounded output row and persisted `pass@compare` with exact `Decimal128(20,6)` schema and
value before moving the board to 21/22.

## Verification

```text
scripts/cargo-cgroup fmt -p toron -- --check
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch --no-fail-fast
git diff --check
git ls-files '*.perf.data'
```

Write `plans/impl/431_q17_sf10_oracle_boundary_fidelity_results.md` with both exact census results,
their FNV-1a summaries, the boundary counts, scale-policy coverage, live receipts, and the honest board
state.
