# ADR 391 - Implementation plan: Rune perf-decomposition probe (q03)

Pairs with `plans/adr/0391_rune_perf_decomposition_probe.md`. Diagnostic-only: a Rune-authored q03 logical shape, bound by
the same binder + executor, measured three-way (SQL / Rune / DuckDB) at sf=10. **Rune does not bypass the binder** -
the split is logical-shape vs substrate. Harness-only; no engine optimization.

## Phase 0 - isolation proof (before any timing)
- Author q03 in Rune (`RuneEngine::execute_script` -> LogicalPlan, rune_engine.rs:1705) with the intended logical shape:
  filter customer(BUILDING) -> join orders -> join lineitem -> group; pin join order/build side via
  `JoinStrategyPinSource::Rune` as needed.
- **Bind both** SQL q03 and Rune q03 at small scale; **dump physical plan fingerprints/shapes** (the bound
  `ExecutionPlan`).
- **Prove they differ as intended** (join order / build side / exchange / filter placement). **If identical -> the probe
  is void**; re-author the Rune shape until the bound plans differ. Record both fingerprints in the results.

## Phase 1 - the Rune perf path (harness-only)
- Add an `engine="rune"` path to the gauge perf rail (alongside `toron` SQL and `duckdb`): same `perf`/`perf-run-one`
  rail, same persisted sf=10 data, same fixed 4 GB scope, emitting a comparable `execute_ms` row.
- `crates/toron-tpch` only; no `crates/toron/src` engine change.

## Phase 2 - three-way measurement + correctness
- Run SQL q03, Rune q03, DuckDB q03 at sf=10 persisted, fixed 4 GB, perf rail (bounded window).
- **Correctness (oracle/order, not digest):** Rune q03 == q03 oracle including ORDER BY at sf=0.01/sf=1; persisted sf=10
  if Rune completes. Reject faster-because-wrong.
- **Ratios:** `SQL/Rune` (logical-shape penalty), `Rune/DuckDB` (substrate), `SQL/DuckDB` (end-to-end). SQL censored
  (time/no-row) -> lower bounds (`> timeout/Rune_ms`), not fake exact.
- **If Rune still slow:** symbol-retaining profile (381 method, `--scope`, debug+no-strip, `perf report -g`) on the Rune
  path -> attribute the substrate cost (Arrow kernel / materialize / spill frame).

## Deliverable (results doc)
`crates/toron/plans/impl/0391_rune_perf_decomposition_probe_results.md`:
- **Phase 0 isolation proof:** SQL vs Rune bound physical plan fingerprints + the intended differences (or "void -
  re-authored");
- the three rows (engine=toron/rune/duckdb): status, execute_ms (or time/no-row), spill, producer;
- the **ratio split** (logical-shape penalty / substrate / end-to-end), SQL censored as lower bounds;
- correctness: Rune q03 oracle/order pass at the scales reached;
- if Rune slow: the symbol-retaining profile + named substrate frame;
- **the named verdict** (logical-shape-dominated / substrate-dominated / both, quantified), with the planner-CPU caveat;
- confirmation harness-only, no engine change, no cap bump, `--no-fail-fast` unchanged, no qNN, no committed perf.data.

## Verification
```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron-tpch                       # the Rune perf path + correctness
scripts/cargo-cgroup test -p toron --no-fail-fast             # unchanged engine
# Phase 0: bind SQL vs Rune q03, dump + compare physical plan fingerprints (must differ as intended)
# Phase 2: 3-way sf=10 perf rows (engine=toron/rune/duckdb); Rune oracle/order correctness; ratios as bounds if censored
git diff --check
```

## Risk / blast radius
- **Bound plans identical (void)** -> isolation proof stops it; re-author.
- **Mislabel as planner cost** -> labeled logical-shape; plan_ms/bind_ms secondary.
- **Faster-because-wrong** -> oracle/order comparator.
- **SQL censored** -> lower-bound ratios.
- **Harness leak into engine** -> `crates/toron/src` unchanged; if an engine change is tempting, stop and split an ADR.

## Seal
`plan: adr 391 rune perf decomposition probe` (the dual). The harness Rune-perf path + results seal as
`impl: adr 391 ...` (harness-only). Do not commit large `.perf.data`.
