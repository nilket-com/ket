# ADR 277 — Results: tpch.q05 (measured row — 6th reach cell)

## Measured row
```
q05 dev=pass@execute  strict=pass@execute   (keyed at the default sf=0.01 scale)
Reachability matrix (all six reach cells dev+strict pass@execute):
  q01/q03/q05/q06/q10/q14
Correctness: q06 + q01 strict=pass@compare (unchanged)
```

q05 — the first **6-table** query — is in the gauge, green in both modes. No engine feature was added; this is
a reachability attempt and the measured row is a pass.

## The candidate wall was *not* a wall (verify-first)
The ADR predicted the likely stressor as the **cyclic / multi-predicate join graph**: q05's supplier edge
carries multiple equalities (`l_suppkey = s_suppkey` plus `c_nationkey = s_nationkey`), giving two paths from
customer to supplier. Framed verify-first — the impl measured the single-node `plan → bind → execute` run, and
both modes **reached execute**. The comma-join recognizer + the single-node executor handle the 6-table cyclic
graph; the extra equalities resolve (as additional join keys / residual filters) without a new wall. (As with
q10, "the predicted wall, measured, was not the first wall" — here it was no wall at all.) No join-graph
follow-on ADR is needed.

Keyed, not a 6-table cartesian: dev q05 completes at sf=0.01 in ~134 ms (51 ms at sf=0.0001). A keyless
6-table join would be astronomically large and never return, so completing at the default scale is itself the
proof — pinned by the `q05_runs ... default scale` assertion inside the q05 test.

## What was built (harness only)
- **`l_suppkey` append** (Int64) to the END of both `lineitem` fixtures (dev + strict) — append-only;
  existing queries reference no new column, so their outputs/digests and the q06/q01 correctness cells are
  unchanged (verified: harness 16, correctness 11, all green).
- **`supplier`** (`s_suppkey`, `s_nationkey`) via `SupplierGenerator` and **`region`** (`r_regionkey`,
  `r_name`) via `RegionGenerator` — no decimals → identical dev/strict. **`nation` reused as-is** (its
  `n_regionkey`, added in ADR 274, serves `n_regionkey = r_regionkey`).
- **SQL** (`Q5_DEV_SQL` / `STRICT_Q5_SQL`): tpchgen-2.0.2's exact Q5, `:1` → `'ASIA'`, `:2` → `'1994-01-01'`,
  interval upper bound → `DATE '1995-01-01'`, `ORDER BY revenue DESC` only (no LIMIT). Same text both modes.
- **Wiring**: `run_dev_q05` / `run_strict_q05`; `q05_tables` = `q10_tables` + `supplier` (`TableId(5)`) +
  `region` (`TableId(6)`); CLI `run --query q05`; `status_rows` → q05 `implemented`.

## Tests
- `harness.rs::q05_executes_in_both_modes` — dev + strict `pass@execute` (+ digest), q05 `implemented`, **plus
  the keyed default-scale (sf=0.01) dev guard** so a missed join edge / cartesian can't hide behind tiny-scale
  success. On the explicit 64 MB `std::thread` stack (a 6-table future overflows libtest's default test stack —
  the q10 pattern; recurs for 6-table).
- q01/q03/q06/q10/q14 reachability + the q06/q01 correctness tests stay green (the `l_suppkey` append is
  transparent).

## Gate (harness-only)
fmt clean · `check -p toron-tpch --all-targets` clean · `test -p toron-tpch` green (harness 16, correctness 11,
probes) · live `matrix` shows q05 dev+strict `pass@execute` (six reach cells), `correctness-matrix` still q01 +
q06 `pass@compare` · `git diff --check` clean. No `-p toron` engine change.

## Forward arc
Six reach cells + two correctness cells. q05 passing means the join engine covers the gauge's join shapes
through 6 tables (chain, branch, cyclic/multi-key). The remaining reachability frontier is the harder arcs:
**correlated subqueries** (q04/q17/q20/q22) and **date-interval arithmetic**. A q05 (or q03) correctness oracle
remains available later.
