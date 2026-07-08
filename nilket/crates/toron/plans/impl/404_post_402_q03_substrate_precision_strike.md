# ADR 404 - Implementation plan: post-402 q03 substrate precision strike

Pairs with `plans/adr/404_post_402_q03_substrate_precision_strike.md`. 402 closed q03 SQL == Rune (fingerprint
`666e0ac028036f70`, ~398 ms). 404 re-profiles that clean post-402 path and does a precision strike on **one named
Toron-owned, behavior-neutral producer**, or seals a diagnostic that feeds the big rock. Headline: Toron SQL / Polars,
Rune as same-shape control. No generic library-primitive strike.

## Phase 0 - symbol-gated profile + cause ledger (before any code)
1. **Fresh four-way baseline before code** (same machine/profile as post-cut):
   ```bash
   rm -rf target/tpch/perf_adr404_q03_sf1_baseline && \
   systemd-run --user --same-dir --wait --collect --quiet --pipe \
     --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
     --setenv=TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/1.5.4/duckdb \
     target/release/toron-tpch perf --sf 1 --mode strict --runs 7 --warmup 1 --query q03 \
       --engines toron,rune,polars_sql,duckdb --data-source parquet --data-dir target/tpch/data \
       --output-dir target/tpch/perf_adr404_q03_sf1_baseline \
     > target/tpch/adr404_q03_sf1_baseline.out
   ```
   This is the pre-cut Toron SQL / Polars denominator for acceptance. Inherited 401/402 rows are context, not the
   banked before/after metric.
2. **Symbol-gated build:**
   ```bash
   CARGO_PROFILE_RELEASE_DEBUG=true CARGO_PROFILE_RELEASE_STRIP=false \
   CARGO_CGROUP_BACKEND=affinity CARGO_CGROUP_JOBS=4 \
     scripts/cargo-cgroup build -p toron-tpch --features polars-sql-reference --release
   ```
3. **Profile loop** (existing Rune-q03 harness; valid because SQL == Rune post-402):
   ```bash
   systemd-run --user --same-dir --wait --collect --quiet --pipe \
     --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
     --setenv=TORON_TPCH_PROFILE_LOOP=16 --setenv=TORON_JOIN_BUILD_PROFILE=1 \
     perf record -D 1000 -F 999 --call-graph dwarf \
       -o target/tpch/adr404_q03_loop_phase0.perf.data \
       -- target/release/toron-tpch perf-run-one --engine rune --query q03 \
         --mode strict --sf 1 --run-index 0 --data-source parquet --data-dir target/tpch/data
   perf report --stdio -i target/tpch/adr404_q03_loop_phase0.perf.data --no-children --sort symbol \
     > target/tpch/adr404_q03_phase0_flat.txt
   ```
   Confirm the profiled path: `materialized_path=true`, `spill=0`, fingerprint `666e0ac028036f70`.
4. **Cause ledger:** classify the dominant residual - hash-table entry/probe / Parquet-RLE/dict decode /
   copy-materialization / **diffuse** - and attribute the top self-frames to a **named Toron-owned producer** (a
   function/struct in `crates/toron/src`, not a library primitive). Record before code.

## Phase 1 - precision strike OR stop-line (Phase 0 decides)
- **Precision strike:** if a named Toron-owned, behavior-neutral producer is behind the dominant frame, strike it
  (generic; fingerprint/rows/spill unchanged). **No "optimize hashbrown/memmove"** strike - a library-primitive frame
  with no owning Toron producer is not a target.
- **Stop-line:** if diffuse or requires vectorized/pipelined execution, seal a diagnostic naming the residual and feed
  the big-rock execution-model thread. No forced strike.

## Tests (behavior-neutral where code changes)
- q03 SQL oracle/order correctness unchanged (`pass@compare`);
- plan fingerprint stays `666e0ac028036f70`; rows `11620`; `spill=0` (behavior-neutral proof);
- if the strike touches a join-build/decoder/materialization producer, its targeted unit tests stay green (e.g. the
  399 join-build suite for a hash-path producer);
- q04/q19 correctness + shape unchanged (generic substrate change);
- if stop-line: no core-code test; the results doc carries the profile + named residual.

## Phase 2 - re-measure (four-way, SQL headline, Rune control)
```bash
rm -rf target/tpch/perf_adr404_q03_sf1 && \
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --setenv=TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/1.5.4/duckdb \
  target/release/toron-tpch perf --sf 1 --mode strict --runs 7 --warmup 1 --query q03 \
    --engines toron,rune,polars_sql,duckdb --data-source parquet --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr404_q03_sf1 \
  > target/tpch/adr404_q03_sf1.out
```
- Headline: **Toron SQL / Polars** improves beyond the runs=7 band, **or** the residual is named (stop-line).
- Compare against the fresh `perf_adr404_q03_sf1_baseline` row, not just inherited 401/402 numbers.
- Control: **SQL and Rune move together, SQL/Rune stays ~1.00x**, fingerprints identical (substrate not shape).
- Record before/after profile (frame moved) + the four-way scorecard.

## Verification
```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p toron-tpch --features polars-sql-reference --all-targets
scripts/cargo-cgroup test -p toron --lib                            # incl. targeted producer tests
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast                   # REQUIRED (execution/core cut)
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast # REQUIRED
target/release/toron-tpch correctness --query q03 --sf 1 --mode strict --data-source parquet --data-dir target/tpch/data
git diff --check
find . -name '*.perf.data' -print          # ensure none staged/committed
```
Note: cap rustc jobs (`CARGO_CGROUP_JOBS=4`) - the large `toron` crate has tripped systemd-oomd at higher job counts.

## Deliverable (results doc)
`crates/toron/plans/impl/404_post_402_q03_substrate_precision_strike_results.md`:
- Phase 0: fresh pre-cut four-way baseline, symbol-gated profile (materialized path, `spill=0`, fingerprint
  `666e0ac...`) + cause-ledger classification + the named Toron-owned producer - before code;
- **either** the behavior-neutral precision strike (producer, before/after profile frame, why generic) **or** the
  stop-line diagnostic (named residual + big-rock hand-off);
- Phase 2: four-way scorecard, Toron SQL / Polars before/after, SQL/Rune ~1.00x control, fingerprint unchanged;
- q04/q19 no-regress; confirmation: behavior-neutral, no reach claim/regression, full gates green, no `.perf.data`.

## Risk / blast radius
- **Diffuse / no narrow producer** -> stop-line; feeds the big rock.
- **Needs vectorized/pipelined execution** -> stop-line; execution-model change, not this cut.
- **Profile moves but median flat** -> banked only if Toron/Polars beats noise; else named.
- **Behavior drift** (fingerprint/rows/spill) -> rejected.
- **One-path speedup** (SQL xor Rune moves) -> red flag; both share substrate.
- **Sibling regression** -> q04/q19 + `--no-fail-fast`.

## Seal
`plan: adr 404 post 402 q03 substrate precision strike` (the dual). Implementation (strike or diagnostic + profile +
re-measure): `impl: adr 404 post 402 q03 substrate precision strike`. Do not commit `.perf.data`.
