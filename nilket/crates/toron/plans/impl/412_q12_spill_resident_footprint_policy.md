# ADR 412 - Implementation plan: q12 spill resident-footprint policy

Pairs with `plans/adr/412_q12_spill_resident_footprint_policy.md`. Component breakdown first: instrument the
`guard_resident_state` failure with the four-component split, reproduce q12, classify (pinning decline mismatch /
writer_peak or reserve accounting / resident-index pressure / genuine pressure), then cut the decided policy. No cap
bump; declinable optimizations must decline, not fail the query; the guard stays armed.

## Phase 0 - breakdown + classification (before any fix)
1. **Component payload:** extend the guard's failure diagnostics (or an off-by-default hook) to carry
   `resident_index_bytes` / `writer_peak` / page reserve / `pinned_decoded_bytes` individually. Keep it - the wall
   reported only the sum for two ADRs. Producer string stays `streamed inner join build spill resident state`.
2. **Reproduce q12** (fresh clean release build; ~8 s):
   ```bash
   systemd-run --user --same-dir --wait --collect --quiet --pipe \
     --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
     --property=RuntimeMaxSec=360s \
     /usr/bin/time -v target/release/toron-tpch perf-run-one --engine toron --query q12 \
       --mode strict --sf 10 --data-source parquet --data-dir target/tpch/data \
     > target/tpch/adr412_q12_sf10_pre.json 2> target/tpch/adr412_q12_sf10_pre.err || true
   ```
   Record: failing **call site** (write `:815` / rebuild `:847` / post-pin `:903` / finish `:924`), the four
   component values, and whether the pinning path was attempted, declined, committed, or not involved. Current code
   already includes `writer_peak` and `JOIN_SPILL_PAGE_SIZE` in the pinning pre-check and checks actual decoded bytes
   page-by-page before commit; Phase 0 must verify the actual failing path rather than assume a missing reserve.
3. **Classify** per the ADR: (a) pinning decline mismatch -> decline-and-unpin; (b) writer_peak staleness or
   non-live reserve accounting -> release/re-scope only if bytes are provably no longer live; (c) resident-index edge
   or reserve double-count only (a "reserve is too big" story = cap bump in disguise -> rejected); (d) genuine
   pressure -> small shedding policy or sealed diagnostic + named mechanism.
4. Record everything before code.

## Phase 1 - the decided cut
- **(a):** convert any post-pin guard failure into decline-and-unpin (restore LRU-read fallback, clear
  `pinned_decoded_bytes`/`pinned_decoded_pages`); the guard error remains for non-declinable sites.
- **(b):** reset/re-scope `writer_peak` or reserve accounting only when the writer/reserve bytes are proven not live;
  the write-path call sites keep the live high-water.
- **(d small):** the named shedding step (e.g. drop pinned pages under pressure) with its own test; **(d large):**
  no code - diagnostic seals, mechanism named.
- Generic; no q12/table branch; no cap/multiplier/scope change anywhere.

## Tests (discriminating)
- **The classified case, reproduced:** e.g. a pin that commits at exactly guard-failing size now declines and the
  join completes via LRU reads - **fails on pre-412 code** (errors);
- **Guard stays armed:** a retained state that is legitimately over cap with no declinable component **still fails**
  with the same producer;
- **Per-call-site coverage:** the write-path guards (`:815`/`:847`) behavior unchanged for genuinely-over writes;
- **Component payload:** failure diagnostics carry the split (assert on a forced failure);
- carry the spill/join suites green (q19's machinery).

## Phase 2 - re-measure
```bash
# driver: q12 sf=10
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --property=RuntimeMaxSec=360s \
  /usr/bin/time -v target/release/toron-tpch perf-run-one --engine toron --query q12 \
    --mode strict --sf 10 --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr412_q12_sf10_post.json 2> target/tpch/adr412_q12_sf10_post.err || true
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --property=RuntimeMaxSec=360s \
  target/release/toron-tpch correctness --query q12 --sf 10 --mode strict \
    --data-source parquet --data-dir target/tpch/data
# controls: all green cells + the working-spill sentinel
for q in q03 q04 q08 q10 q19; do
  systemd-run --user --same-dir --wait --collect --quiet --pipe \
    --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
    --property=RuntimeMaxSec=360s \
    target/release/toron-tpch correctness --query "$q" --sf 10 --mode strict \
      --data-source parquet --data-dir target/tpch/data
done
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  target/release/toron-tpch perf --sf 1 --mode strict --runs 7 --warmup 1 --query q03 \
    --engines toron,rune,polars_sql,duckdb --data-source parquet --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr412_q03_sf1_control
```
- Primary: q12 pass@compare or named + reproduced next wall.
- Controls: q03/q04/q08/q10/q19 pass@compare (q19 spill byte-sane); q03 sf=1 four-way in band (fp `666e0ac028036f70`).

## Verification
```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron spill --lib
scripts/cargo-cgroup test -p toron --test architecture_test        # after fmt
scripts/cargo-cgroup test -p toron --no-fail-fast                   # REQUIRED (executor cut); JOBS=4 if oomd
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast   # REQUIRED (full crate)
git diff --check
git ls-files '*.perf.data'
```

## Deliverable (results doc)
`crates/toron/plans/impl/412_q12_spill_resident_footprint_policy_results.md`:
- Phase 0: failing call site + four-component split + pinning-path status + classification - before code;
- the decided cut (decline-and-unpin / writer_peak or reserve release / small shed) with the discriminating
  test pair (classified case completes; genuinely-over still fails), **or** the sealed diagnostic + named mechanism;
- q12 reach-board move + all controls + component-payload retention;
- confirmation: no cap/multiplier/scope change, guard armed, full gates green, no `.perf.data`, scope honesty (q05
  reservoir next).

## Risk / blast radius
- **Genuine pressure, mechanism-sized** -> sealed diagnostic + named cut.
- **Next wall behind the guard** -> reach-board honesty.
- **Write-path coupling** (shared guard) -> per-call-site tests + q19 sentinel.
- **Payload churn** -> producer string stable; extend diagnostics only.

## Seal
`plan: adr 412 q12 spill resident footprint policy` (the dual). Implementation:
`impl: adr 412 q12 spill resident footprint policy`. Do not commit `.perf.data`.
