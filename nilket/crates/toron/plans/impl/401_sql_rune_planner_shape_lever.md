# ADR 401 - Implementation plan: SQL/Rune planner-shape lever (q03)

Pairs with `plans/adr/401_sql_rune_planner_shape_lever.md`. Diagnose the SQL-vs-Rune bound-plan shape delta on the
persisted sf=1 path (with layer attribution + optimizer evidence), then a generic planner-rule fix OR a stop-line
diagnostic. Shape closure, not only speedup. No q03 special case.

## Phase 0 - persisted-path diagnosis (before any fix)
1. **Persisted isolation proof:** the 391 `rune_q03_isolation_proof` uses `build_perf_query_spec` (memory,
   lib.rs:3356). Add a **persisted-data-source** SQL-vs-Rune bind for q03 at sf=1 (or prove the memory and persisted
   bound plans are byte-identical). Dump + fingerprint both **current** bound `ExecutionPlan`s.
2. **Shape-delta table** with the **layer** column (SQL lowering / binder join-order policy / pushdown /
   physicalization) per aspect: join order, build side per join, filter placement, projection timing, exchange
   placement.
3. **Optimizer evidence:** the soft-margin ladder inputs (`join_order_edges`, per-relation row/byte/fact estimates) and
   the decision/reason for q03's ordering. Note explicitly if post-filter cardinality / multi-relation cost is
   unavailable (-> cost-model gap -> stop-line).
Record all in the results doc **before** code.

## Phase 1 - generic rule fix OR stop-line
- **If the delta maps to a generic rule** (ladder ordering / build-side / placement): fix it so SQL converges to the
  Rune shape. Generic; no q03 branch. **Layer-fence:** a SQL-lowering fix must leave Rune's bound plan unchanged; a
  binder-policy fix that necessarily touches Rune must be fenced or the Rune control re-baselined with a documented,
  correctness-neutral reason.
- **If it needs a cost model Toron lacks:** stop-line - seal a diagnostic (named delta + gap), no q03-shaped heuristic.

## Tests (behavior-neutral)
- SQL q03 oracle/order correctness unchanged;
- **shape-closure test:** SQL bound plan converges to Rune on the classified delta (a plan-shape/fingerprint assertion
  for that aspect), or the residual is asserted + explained;
- Rune q03 bound plan unchanged (control) - unless a fenced binder-policy fix documents a re-baseline;
- q05/q10 correctness + a shape check - no regression (ideally improved);
- prior greens (q04/q06/q14/q19) + carried unchanged.

## Phase 2 - re-measure (SQL/Rune primary, both Toron/stable)
```bash
CARGO_PROFILE_RELEASE_DEBUG=true CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup build -p toron-tpch \
  --features polars-sql-reference --release
systemd-run ... target/release/toron-tpch perf --sf 1 --mode strict --runs 7 --warmup 1 --query q03 \
  --engines toron,rune,polars_sql,duckdb --data-source parquet --data-dir target/tpch/data \
  --output-dir target/tpch/perf_adr401_q03_sf1
```
- Primary: **SQL q03 absolute median improves beyond the runs=7 band AND `SQL/Rune` closes toward 1x**; Rune unchanged
  (control). Within-noise or no shape closure -> not banked.
- Record the before/after shape-delta (closure verdict).

## Verification
```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast            # REQUIRED (planner/core cut)
scripts/cargo-cgroup test -p toron-tpch --no-fail-fast       # REQUIRED
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q03 --sf 0.01   # strict pass@compare
git diff --check
```

## Deliverable (results doc)
`crates/toron/plans/impl/401_sql_rune_planner_shape_lever_results.md`:
- Phase 0: the persisted-path shape-delta table (layer attribution) + optimizer-evidence rows + current fingerprints;
- the classified delta + fixability;
- **either** the generic-rule fix (locus, the layer fence, why Rune is/n't the control) **or** the stop-line diagnostic
  (named delta + cost-model gap);
- Phase 2: SQL absolute median before/after + `SQL/Rune` closure toward 1x + Rune-unchanged control + the shape-closure
  verdict (SQL shape == Rune on the delta, or residual explained);
- q05/q10 no-regression (correctness + shape);
- confirmation: no cap bump, no qNN, full gates green, prior greens unchanged, scope-honesty (not a sf=10 heavy-cell fix).

## Risk / blast radius
- **Memory vs persisted plan mismatch** -> persisted isolation proof.
- **Binder-policy fix moves Rune** -> fence or re-baseline the control.
- **Cost-model gap** -> stop-line diagnostic.
- **Speedup without shape closure** -> not banked (acceptance requires convergence).
- **Sibling regression** -> q05/q10 checks + `--no-fail-fast`.

## Seal
`plan: adr 401 sql rune planner shape lever` (the dual). Implementation (fix or diagnostic + evidence + re-measure):
`impl: adr 401 sql rune planner shape lever`. Do not commit `.perf.data`.
