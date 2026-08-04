# ADR 402 - Implementation plan: grouped-aggregate exchange placement (q03)

Pairs with `plans/adr/0402_grouped_aggregate_exchange_placement.md`. Diagnose the last q03 SQL/Rune shape residual - the
`parts=4` pre-aggregate hash exchange SQL emits (`sql_planner.rs:3974`/`:6091`) that Rune has no equivalent for
(`rune_engine.rs:393`). Strict Phase 0 stop-line, then a generic pre-aggregate exchange-placement rule **or** a diagnostic
that pivots to Thread 3. Shape closure, not only speedup. No q03 special case.

## Phase 0 - strict stop-line (both gates before any real fix)
Record all in the results doc **before** the Phase 1 rule.

1. **Materiality probe.** Temporary probe: force the grouped-aggregate exchange to `partition_count=1` as a lower-bound
   cost probe, and, if feasible, perform a true throwaway elision that removes the exchange entirely on the
   measured q03 path (a throwaway edit at `sql_planner.rs:3974`/`:6091` or a local plan rewrite). Build release,
   run q03 sf=1 perf same-machine (baseline worktree at current seal `b785f58`, current = probe), and record the SQL
   median delta and the SQL/Rune ratio for `parts=4` vs `parts=1` vs true elision. `parts=1` is not shape closure unless
   the executor proves it bypasses repartition work for this shape.
   - **Stop-line:** if the SQL median moves within the runs=7 noise band, the exchange is not the residual - seal a
     diagnostic and pivot to Thread 3. Revert the probe (targeted Edit, not `git checkout`).
2. **Elidability proof.** Determine whether a **structural co-location proof** is available at the collapse locus
   (single-partition, or hash partitioning by columns/equi-key classes covered by the aggregate group columns) and that
   the distributed/multi-partition path is separable. First census the actual facts available at the candidate locus:
   current binder `PlanOptimizerFacts` are row/byte/origin facts, not partitioning proof, so they are insufficient by
   themselves.
   - **Stop-line:** if a correct collapse requires a partial/final two-phase aggregate (or any cardinality estimate),
     seal a diagnostic and pivot to Thread 3. No two-phase mechanism is built here.
   - **Allowed narrow prerequisite:** if the only missing piece is an explicit partitioning/co-location proof, it may
     be added only if it is minimal, generic, behavior-neutral, and locked by distributed tests proving multi-partition
     aggregate inputs without such proof still keep the exchange.

If a probe proves a load-bearing claim, keep it as a clean named regression test (it rides the impl commit), not a
reverted throwaway.

## Phase 1 - the rule (only if both Phase 0 gates pass)
Generic pre-aggregate exchange-placement rule: remove a hash `Exchange(parts=N, keys)` feeding an `Aggregate`
(and, at the binder locus, `Distinct`) **only when the exchange input is provably co-located for the group keys**. A
`partition_count=1` rewrite is acceptable only if the executor bypasses repartition work for this shape; otherwise it is
only the Phase 0 probe. Pick the locus where the co-location proof is soundly available:
- **binder normalization** (preferred if the proof is available at bind) - collapses grouped-aggregate **and**
  `Distinct` uniformly (`binder.rs:1256-1318`); does not fire for Rune (no exchange), so Rune is unchanged by
  construction - **verify** the Rune q03 fingerprint is byte-identical;
- **SQL grouped-aggregate lowering** (`sql_planner.rs:3974`/`:6091`) - skip when co-located (grouped-aggregate
  only; replicate for `Distinct` at `binder.rs:1279` if chosen).

Generic; no q03 branch. Gate on the partitioning/co-location proof, never cardinality.

## Tests (behavior-neutral)
- q03 SQL oracle/order correctness unchanged;
- **shape-closure test:** SQL bound q03 plan no longer carries the pre-aggregate exchange, converging to Rune's
  direct-aggregate shape (fingerprint/shape assertion). A `parts=1` exchange only passes if the executor has a recorded
  no-op/bypass proof for that shape;
- **distributed-correctness test:** a genuinely multi-partition grouped-aggregate input **keeps** the hash exchange
  unless co-location is structurally proven - this is the discriminating test that would fail if the rule over-collapsed;
- Rune q03 bound plan unchanged (control fingerprint `666e0ac028036f70`);
- q05/q10 correctness + shape (grouped aggregates) - no regression; if binder locus, a `Distinct` shape check;
- prior greens (q03 filter placement from 401, q04/q06/q14/q19) carried unchanged.

## Phase 2 - re-measure (SQL/Rune primary, both Toron/stable, same-machine)
```bash
# baseline = current seal (pre-402)
git worktree add --detach /tmp/nilket-adr402-base b785f58
CARGO_TARGET_DIR=.../target/adr402_base CARGO_PROFILE_RELEASE_DEBUG=true CARGO_PROFILE_RELEASE_LTO=thin \
  CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup build -p toron-tpch --release
# current = 402
CARGO_PROFILE_RELEASE_DEBUG=true CARGO_PROFILE_RELEASE_LTO=thin CARGO_CGROUP_BACKEND=affinity \
  scripts/cargo-cgroup build -p toron-tpch --release
# perf (both), 4G scope, runs=7 warmup=1
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --setenv=TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/1.5.4/duckdb \
  <binary> perf --sf 1 --mode strict --runs 7 --warmup 1 --query q03 \
    --engines toron,rune,duckdb --data-source parquet --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr402_q03_sf1_<base|current>
```
- Primary: SQL q03 absolute median improves beyond the runs=7 band **and** `SQL/Rune` closes below `1.78x` toward 1x;
  Rune unchanged (control). Within-noise or no shape closure -> not banked.
- Record before/after shape-delta (closure verdict) + the same-machine SQL/Rune before/after.

## Verification
```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test        # distributed grouped-aggregate coverage
scripts/cargo-cgroup test -p toron --no-fail-fast                  # REQUIRED (planner/core cut)
scripts/cargo-cgroup test -p toron-tpch --no-fail-fast             # REQUIRED
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q03 --sf 0.01   # strict pass@compare
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q05 --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q10 --sf 0.01
git diff --check
find . -name '*.perf.data' -print
```

## Deliverable (results doc)
`crates/toron/plans/impl/0402_grouped_aggregate_exchange_placement_results.md`:
- Phase 0: materiality delta (parts=4 vs parts=1, same-machine) + elidability classification, before code;
- If measured, distinguish `parts=1` from true elision: `parts=1` is a lower-bound cost probe unless proven no-op.
- **either** the generic rule (locus, the co-location gate, why Rune/multi-partition are unaffected) **or** the
  stop-line diagnostic (named reason + Thread 3 pivot);
- Phase 2: SQL absolute median before/after + `SQL/Rune` closure + Rune-unchanged control + shape-closure verdict;
- distributed-aggregate correctness proof + q05/q10 (+ Distinct) no-regression;
- confirmation: no cap bump, no q03 branch, full gates green, prior greens unchanged, scope-honesty.

## Risk / blast radius
- **Exchange immaterial** -> Phase 0 gate 1 stop-line.
- **Two-phase required** -> Phase 0 gate 2 stop-line, Thread 3.
- **Over-collapse breaks distributed correctness** -> multi-partition keep-the-exchange test.
- **Rune moved** -> control fingerprint assertion; binder locus is unchanged-by-construction, verify it.
- **Sibling regression** -> q05/q10 + Distinct checks + `--no-fail-fast`.

## Seal
`plan: adr 402 grouped aggregate exchange placement` (the dual). Implementation (rule or diagnostic + evidence +
re-measure): `impl: adr 402 grouped aggregate exchange placement`. Do not commit `.perf.data`.
