# ADR 397 - Implementation plan: post-396 q03 substrate re-profile + next precision strike

Pairs with `plans/adr/397_q03_substrate_reprofile_strike.md`. Re-profile the post-396 Rune q03 materialized sf=1 path,
build a scan-request/materialization ledger, name the dominant Toron-owned producer, strike only that (or name the
irreducible residual). Behavior-neutral; not q03-shaped in core.

## Phase 0 - re-profile + ledger (before any cut)
1. **Re-profile** (395/396 method): `CARGO_PROFILE_RELEASE_DEBUG=true CARGO_PROFILE_RELEASE_STRIP=false` build; the
   off-by-default Rune q03 execute loop under `perf record --call-graph dwarf` inside the fixed-cap `--scope`; `readelf`/
   `nm` symbol gate; `perf report -g`; verify DWARF overhead vs unprofiled wall.
2. **396-persistence check:** overflow heap 0, join-build time still reduced, plan fingerprint `2b72...`, spill 0,
   materialized_path true.
3. **Scan-request / materialization ledger** (temporary/env-gated instrumentation, e.g. under the 395/396 profile path):
   - the q03 Rune `ScanRequest`: **projected columns requested**, **`pbc_predicate`**, **rows in/out of scan**, **batches
     emitted (count + row sizes)**;
   - **post-scan sites:** filter / take / concat / rebatch calls and their bytes;
   - the **dominant copy producer** (frames %, counts/bytes).
4. **Class the dominant producer:** missing/insufficient pushdown | post-scan var-width filter materialization |
   concat/rebatch | join-probe-output materialization | irreducible decode. Record in the results doc **before** code.

## Phase 1 - the evidenced strike (one producer)
Per Phase 0's class:
- **missing pushdown (proven absent):** route the projection/predicate the ledger shows unreached into the scan;
- **post-scan var-width filter materialization:** avoid the redundant rematerialization (e.g. filter indices once, or
  project before materialize);
- **concat/rebatch:** remove the unnecessary batch concat/`execute_bound -> Vec` copy;
- **join/probe/output materialization:** the narrow Toron-owned copy;
- **irreducible decode dominant:** no strike - name the residual (Polars 108 ms fixable bound) and re-target.
Narrow, behavior-neutral, generic. If a shared helper is touched, generic across key plans/columns.

## Tests (behavior-neutral)
- if scan/filter/materialization path changes: batch/digest/schema/**column-order** parity on a synthetic q03-shaped
  plan; projected-column + predicate parity (no dropped rows/cols);
- if a shared join helper is touched: Int32/Int64/generic keys, nulls, unique + duplicate keys, output-order (as 396);
- Rune q03 value gate (395) still passes;
- prior greens (q04/q06/q14/q19) + carried unchanged; sf=3 streaming path unchanged.

## Phase 2 - re-measure (absolute primary, ratio context)
```bash
CARGO_PROFILE_RELEASE_DEBUG=true CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup build -p toron-tpch \
  --features polars-sql-reference --release
systemd-run ... target/release/toron-tpch perf --sf 1 --mode strict --runs 7 --warmup 1 --query q03 \
  --engines toron,rune,polars_sql,duckdb --data-source parquet --data-dir target/tpch/data \
  --output-dir target/tpch/perf_adr397_q03_sf1
```
- **Primary:** Rune median beyond the runs=7 band (396 baseline 1135.07 ms). Within-noise -> not banked; name residual.
- **Secondary:** `Rune/Polars`. Re-profile to confirm the struck producer's share fell.

## Verification
```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast            # REQUIRED (engine cut if core touched)
scripts/cargo-cgroup test -p toron-tpch --no-fail-fast       # REQUIRED
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q03 --sf 0.01   # strict pass@compare
git diff --check
```

## Deliverable (results doc)
`crates/toron/plans/impl/397_q03_substrate_reprofile_strike_results.md`:
- Phase 0: the re-profile + 396-persistence check + the **scan-request/materialization ledger** (columns, predicate,
  rows/batches in-out, post-scan filter/take/concat sites) + the named/classed dominant producer;
- the strike (which producer, the narrow change) or the named irreducible residual (fixable-vs-irreducible split);
- Phase 2: Rune absolute median before/after (beyond-noise verdict) + `Rune/Polars` context + re-profile share drop;
- behavior-neutral parity + q03 value gate;
- confirmation: no cap bump, no qNN, full gates green, prior greens + sf=3 unchanged, instrumentation off-by-default.

## Risk / blast radius
- **Presumed pushdown-missing** -> ledger proves present/absent first.
- **Irreducible decode dominant** -> named residual (Polars 108 ms bound), re-target.
- **Broad materialization strike** -> narrow specific copy/rebatch/pushdown.
- **Shared-helper regression** -> parity + `--no-fail-fast`.
- **Polars-noise false win** -> absolute-median-primary.

## Seal
`plan: adr 397 q03 substrate reprofile strike` (the dual). Implementation (ledger + strike + re-measure):
`impl: adr 397 q03 substrate reprofile strike`. Do not commit `.perf.data`.
