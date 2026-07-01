# ADR 399 - Implementation plan: post-397 q03 substrate re-profile + next precision strike

Pairs with `plans/adr/399_post_397_q03_substrate_strike.md`. Re-profile post-397 Rune q03 materialized sf=1, name the
dominant producer with class-appropriate cause work, strike only a cause-identified Toron-owned producer OR seal a
diagnostic with the named residual. Secondary cheap sf=10 reach probe. Behavior-neutral; not q03-shaped in core.

## Phase 0 - re-profile + cause-level analysis (before any cut)
1. **Re-profile** (395/397 method): debug+no-strip build; off-by-default Rune q03 execute loop under `perf record
   --call-graph dwarf` in the fixed-cap `--scope`; `readelf`/`nm` symbol gate; `perf report -g`; DWARF-overhead verified.
2. **Confirm baseline:** fingerprint `666e0ac028036f70`, spill 0, materialized_path true, ~410 ms, `Rune/Polars` 3.97x.
3. **Name the dominant Toron-owned producer**, then per class:
   - **hash path (`build_in_memory_join_rows`/hashbrown):** cause-level ledger - key encoding cost, hash count,
     insert/probe cardinality, resize/growth, duplicate-list behavior, memory-guard overhead;
   - **Parquet/RLE/dictionary decode:** irreducible-vs-Toron-redundant classification (397-style);
   - **residual memmove/memcpy:** trace the owning caller.
   - Ledger only where the class needs it (no 397-ledger by habit for a hash-path dominant).
4. **Stop-line check:** is there a Toron-owned producer with a **behavior-neutral narrow lever**? If not -> **seal
   diagnostic** (named residual + fixable-vs-Polars-floor split), no strike.

## Phase 1 - the cause-identified strike (only if the stop-line clears)
- Strike the one producer, narrow + behavior-neutral. Examples by class:
  - hash: a specific key-encoding / insert pattern the cause ledger proves costly (not "swap the hash map");
  - decode: a proven redundant decode/materialization (not the RLE kernel);
  - copy: the removable Toron-owned copy the caller trace names.
- Generic; no q03 branch. Shared-helper parity (396 set) if `build_rows.rs`/`build_in_memory_join_rows` touched;
  fold the `BUILD_ROW_INLINE_OVERFLOW_CAP = 6` comment then.

## Tests (behavior-neutral)
- shared join helper touched: Int32/Int64/generic keys, nulls, unique + duplicate keys, output-order (396 set);
- scan/decode/materialization touched: batch/digest/schema/column-order parity on a synthetic q03-shaped plan;
- q03 value gate (395) still passes;
- prior greens (q04/q06/q14/q19) + carried unchanged; sf=3 streaming path unchanged.

## Phase 2 - re-measure + sf=10 reach signal
```bash
CARGO_PROFILE_RELEASE_DEBUG=true CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup build -p toron-tpch \
  --features polars-sql-reference --release
# primary: sf=1 four-way (absolute Rune median beyond the runs=7 band; baseline ~410 ms)
systemd-run ... target/release/toron-tpch perf --sf 1 --mode strict --runs 7 --warmup 1 --query q03 \
  --engines toron,rune,polars_sql,duckdb --data-source parquet --data-dir target/tpch/data \
  --output-dir target/tpch/perf_adr399_q03_sf1
# secondary/cheap: sf=10 reach signal (status + peak RSS + time/window), NOT the metric
systemd-run ... target/release/toron-tpch perf-run-one --engine rune --query q03 --mode strict --sf 10 \
  --data-source parquet --data-dir target/tpch/data
```
- Primary: Rune sf=1 median beyond noise; within-noise -> not banked, name residual. Re-profile: struck share fell.
- Secondary: sf=10 status/peak RSS/time - over-time curve datapoint.

## Verification
```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast            # REQUIRED if core touched
scripts/cargo-cgroup test -p toron-tpch --no-fail-fast       # REQUIRED
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q03 --sf 0.01   # strict pass@compare
git diff --check
```

## Deliverable (results doc)
`crates/toron/plans/impl/399_post_397_q03_substrate_strike_results.md`:
- Phase 0: re-profile + baseline confirm + the named dominant producer + its **class-appropriate cause analysis**
  (hash cause-ledger / decode irreducible-vs-redundant / memmove owning caller);
- **either** the strike (producer, cause, narrow change) **or** the stop-line diagnostic (named residual + floor split);
- Phase 2: Rune sf=1 absolute median before/after (beyond-noise) + `Rune/Polars` + re-profile share drop; **sf=10 reach
  signal** (status/peak RSS/time) for the over-time curve;
- behavior-neutral parity + q03 value gate;
- confirmation: no cap bump, no qNN, full gates green, prior greens + sf=3 unchanged, instrumentation off-by-default.

## Risk / blast radius
- **No narrow lever** -> stop-line diagnostic, named residual.
- **Hash-hot without cause** -> cause ledger gates the strike.
- **Irreducible decode** -> named residual (Polars 108 ms bound).
- **Shared-helper regression** -> 396 parity + `--no-fail-fast`.
- **Polars-noise false win** -> absolute-median-primary.

## Seal
`plan: adr 399 post 397 q03 substrate strike` (the dual). Implementation (strike or diagnostic + re-measure + sf=10
signal): `impl: adr 399 post 397 q03 substrate strike`. Do not commit `.perf.data`.
