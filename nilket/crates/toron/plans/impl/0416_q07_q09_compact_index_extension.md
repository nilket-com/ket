# ADR 416 - Implementation plan: q07/q09 compact spilled-index extension

Pairs with `plans/adr/0416_q07_q09_compact_index_extension.md`. Enable 414's compact resident index on the creation
path q07/q09 route through (byte-identical pre-414 wall payloads). Two tightenings: (1) prove compact is enabled
**before the first append** and record **measured** per-build `resident_index_bytes` on both drivers - no q12
inheritance; (2) flag retirement stays out unless the Phase 0 census proves every path compact-safe - else named
separately.

## Phase 0 - creation-path census (before code)
1. Reproduce both walls (fast; scratch-clean between):
   ```bash
   for q in q07 q09; do
     systemd-run --user --same-dir --wait --collect --quiet --pipe \
       --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
       --property=RuntimeMaxSec=360s \
       /usr/bin/time -v target/release/toron-tpch perf-run-one --engine toron --query "$q" \
         --mode strict --sf 10 --data-source parquet --data-dir target/tpch/data \
       > "target/tpch/adr416/${q}_pre.json" 2> "target/tpch/adr416/${q}_pre.err" || true
     rm -rf /tmp/toron_join_spill
   done
   ```
   Confirm both payloads + identify the walling stage/creation site (trace or code-read from the producer).
2. **Census every `SpilledInnerJoinBuild` creation path:** flag state, key-plan availability at construction, and
   compact-safety (omission vs semantic fence). This is the retirement evidence base - retirement itself is out of
   416 unless all-safe AND separately opted.
3. Record before code.

## Phase 1 - the opt-in
- Enable `compact_index_enabled` (with the key plan / null-safe wiring 414's helpers use) at q07/q09's creation
  site(s), primitive plans only; Generic/null-safe fallback unchanged. No rescue-window change; no other flags moved.

## Tests (discriminating)
- **Compact-from-first-append:** exercise the actual newly opted production helper for at least one batch, assert the
  compact representation, and assert a monotonic `legacy_index_insert_count == 0` - unlike inspecting the drained
  map, this fails if the path stays legacy or relies on rescue conversion;
- **Per-build accounting observation:** use a temporary finish-time profile on the real q07/q09 fixed-scope runs to
  record each completed compact `resident_index_bytes`; a low-cap first crossing is not accepted as final headroom
  (accounted == computed fidelity remains re-asserted in the path test);
- carry the full 414 suite (fidelity, guard-armed, lookup-equality, truncation-bounds, fence) green;
- carry spill/join/reservoir suites green.

## Phase 2 - re-measure
```bash
mkdir -p target/tpch/adr416
# drivers (bounded; scratch-clean between; .meta protocol from 415 for oom/timeout separability)
for q in q07 q09; do
  unit="toron-adr416-${q}-post"
  systemd-run --user --unit="$unit" --same-dir --wait --quiet --pipe \
    --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
    --property=RuntimeMaxSec=360s \
    /usr/bin/time -v target/release/toron-tpch perf-run-one --engine toron --query "$q" \
      --mode strict --sf 10 --data-source parquet --data-dir target/tpch/data \
    > "target/tpch/adr416/${q}_post.json" 2> "target/tpch/adr416/${q}_post.err"
  { echo "systemd_run_rc=$?"; systemctl --user show "$unit" --property=Result --property=MemoryPeak; } \
    > "target/tpch/adr416/${q}_post.meta"; systemctl --user reset-failed "$unit" 2>/dev/null || true
  rm -rf /tmp/toron_join_spill
done
# correctness for any completing driver (600s window)
# measured compact index bytes on both drivers: temporary finish-time profile on THESE completed builds.
# controls: spill-bearing greens
for q in q03 q10 q12 q14 q16 q19 q20; do
  systemd-run --user --same-dir --wait --collect --quiet --pipe \
    --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
    --property=RuntimeMaxSec=600s \
    target/release/toron-tpch correctness --query "$q" --sf 10 --mode strict \
      --data-source parquet --data-dir target/tpch/data \
    >> target/tpch/adr416/controls.out 2>&1
  rm -rf /tmp/toron_join_spill
done
# remaining greens (q01 q04 q06 q08 q15) cheap-optional; q03 sf=1 four-way if any shared code moved beyond the flag
```
- Primary: q07 + q09 pass@compare, or named + reproduced next walls (q09's 3.4 GiB-at-wall makes an OOM-behind
  plausible - recorded honestly, likely joining the q05 residency family).
- Measured per-build compact `resident_index_bytes` recorded for both drivers.
- Controls: spill-bearing greens pass@compare, forensic spill bytes in band (q03 233,338,304 / q10 215,703,848 /
  q12 159,510,976 / q19 51,471,552); q20 residency watch noted.

## Verification
```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron spill --lib
scripts/cargo-cgroup test -p toron join --lib
scripts/cargo-cgroup test -p toron --test architecture_test        # after fmt
scripts/cargo-cgroup test -p toron --no-fail-fast                   # REQUIRED (executor cut); JOBS=4 if oomd
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast   # REQUIRED (full crate)
git diff --check
git ls-files '*.perf.data'
```

## Deliverable (results doc)
`crates/toron/plans/impl/0416_q07_q09_compact_index_extension_results.md`:
- Phase 0: creation-site identification (omission vs fence) + the all-paths census + eligibility - before code;
- the opt-in (site, wiring) + the compact-from-first-append test + **measured per-build index bytes for q07 and q09**;
- both drivers' reach-board moves (pass@compare or named + reproduced next walls) + controls + forensic spill bands;
- the retirement verdict from the census: all-safe (cleanup named as its own cut) or not (which paths block, why);
- confirmation: no cap/scope change, no rescue widening, 414 apparatus green, full gates, no `.perf.data`, scope
  honesty (remaining wall families named).

## Risk / blast radius
- **OOM behind the wall (q09)** -> reach honesty; q05-family residency follow-on.
- **Compact-yet-over on these builds** -> measured evidence -> sharded/deferred index named; no cap tweak.
- **Semantic fence** -> stop-line; finding sealed.
- **Sibling regression** -> 414 suite + spill-bearing controls + full gates.

## Seal
`plan: adr 416 q07 q09 compact index extension` (the dual). Implementation:
`impl: adr 416 q07 q09 compact index extension`. Do not commit `.perf.data`.
