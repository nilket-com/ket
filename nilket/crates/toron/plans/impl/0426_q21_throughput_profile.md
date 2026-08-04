# ADR 426 - Implementation plan: q21 throughput profile

Pairs with `plans/adr/0426_q21_throughput_profile.md`. Land an off-by-default durable seven-stage
throughput receipt for the reoriented semi/anti chain drive and use it to produce the grounded
per-stage wall-time split through q21's 600-second gate, discharging the standing "no q21
speedup claim without the profile" rule. `CPUUsageNSec` stays an outer saturation fact; the
`Instant` receipt does not call itself a CPU-time split. Diagnosis is the guaranteed deliverable;
a same-seal fix is conditional on the profile naming a bounded existing-mechanism (probe-side)
extension. Instrument lands in `exec/join/streaming.rs`, never `exec.rs` (at its 7,500-line
ratchet).

## Phase 0 - confirm the wall and the timing gap (before code)

1. **Reproduce q21's timeout** at the board envelope:
   ```bash
   scripts/cargo-cgroup build -p toron-tpch --release
   mkdir -p target/tpch/adr426
   unit="toron-adr426-q21-pre"
   systemd-run --user --unit="$unit" --same-dir --wait --quiet --pipe \
     --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
     --property=RuntimeMaxSec=600s \
     /usr/bin/time -v target/release/toron-tpch perf-run-one --engine toron --query q21 \
       --mode strict --sf 10 --data-source parquet --data-dir target/tpch/data \
     > target/tpch/adr426/q21_pre.json 2> target/tpch/adr426/q21_pre.err
   { echo "systemd_run_rc=$?"; systemctl --user show "$unit" --property=Result \
       --property=CPUUsageNSec --property=MemoryPeak; } > target/tpch/adr426/q21_pre.meta
   systemctl --user reset-failed "$unit" 2>/dev/null || true
   rm -rf /tmp/toron_join_spill
   ```
   Expect `Result=timeout`, `CPUUsageNSec ~= 603 s`, `MemoryPeak ~= 2.24 GB`, no terminal row.
2. **Pin the timing gap in source:** the single `probe_started`/post-loop `probe_micros` flush
   (`streaming.rs:10995`/`:11102`) proves the current instrument is aggregate + terminal-only and
   is lost on the kill. Confirm the chain owner is `try_streaming_inner_prefix_semi_anti_*` and
   enumerate the seven stage boundaries (base scan, inner replay 0/1/2, semi replay 3, anti
   replay 4, grouped fold).
3. **Confirm q21 correctness holds** at sf=1/sf=0.01 (unchanged) so the cut is purely
   throughput diagnosis, not a correctness question.
4. Record everything before code.

## Phase 1 - the durable seven-stage receipt

1. **Receipt structure** (off-by-default env path, e.g. a query-neutral
   `TORON_CHAIN_STAGE_PROFILE_PATH`; query-neutral field names; in `streaming.rs`):
   - seven non-overlapping per-stage accumulators; time only the stage-owned operation, with no
     timed parent stage around it;
   - one independent total-drive timer; require `sum(stage_micros) <= total_drive_micros` and
     define `unattributed_residual` as the checked difference so the integer receipt sums exactly;
   - for every stage: calls/batches, input rows, output rows, and exclusive micros; for the base
     stage additionally record raw scan rows and post-filter rows;
   - snapshot sequence, completed base-batch count, `drive_completed`, and optional terminal
     error (`drive_completed` means the scan/probe/fold drive returned, not that later aggregate
     finalization or the whole query returned).
   Stage ownership is exact: base owns `stream.next().await`, row limiting, and the scan filter;
   replay 0 owns key extraction/probe, selection materialization, its post-filter, and
   repartition; replay 1/2 each own their complete runtime probe plus post-filter; replay 3/4
   each own the complete semi/anti probe including per-batch key grouping and selection
   materialization; fold owns `fold_bound_grouped_aggregate_batch`. Loop/vector bookkeeping and
   snapshot writes remain visible in the residual.
2. **Atomic start/every-base-batch snapshot:** write sequence 0 before the first base scan await;
   after every completed base scan batch, write one complete JSON snapshot to a sibling temporary
   path and rename it over the configured path. Flush once more on normal/error exit. A kill
   therefore preserves at least the start receipt and normally the last completed batch, with no
   trailing-record ambiguity. Enabled snapshot write/rename failures return a labeled error
   after normal owner cleanup; they never silently leave stale evidence advertised as current.
   Calibrate this exact cadence at sf=1; if its overhead is material, stop and redesign rather
   than weakening the cadence without council evidence.
3. **Off-by-default inertness:** absent the env var, no timer, no flush, no allocation - the
   drive output and decisions are identical. Resolve the production env path once (the 419/423
   pattern), but expose a private injected path/sink constructor for tests so
   instrumented/uninstrumented cases never race on process-global env mutation or `OnceLock`
   initialization order.
4. **No `exec.rs` change; no behavior change; no comparator/schema/guard change.**

## Phase 1b - conditional fix (only if Phase 2 proves it bounded)

Not implemented until the Phase 2 profile is in hand. If a **probe** stage dominates and its
remedy is a bounded extension of an existing recognizer/streaming path, implement it here with
its own instrumented before/after per-stage evidence. If the **grouped fold** dominates (Utf8
COUNT), stop - that is a new primitive, named ADR 427, not this seal. If the pull interface is
implicated, stop - C-spec evidence for q02.

## Tests (instrument locks)

- **Off-by-default byte parity:** with the env var unset, an instrumented-build q21-shaped
  chain-drive fixture produces byte-identical output and makes zero receipt writes.
- **sf=1 answer parity:** separate instrumented and uninstrumented release processes at sf=1
  produce equal output digest/schema/row count; unit fixtures use the injected sink and do not
  toggle the process environment.
- **Sum-to-total lock:** on a synthetic multi-stage drive, sum(exclusive stage timers) +
  `unattributed_residual` == measured total; `sum(stage_micros) > total` fails loudly rather than
  saturating or wrapping.
- **Durability lock:** a simulated stop after N completed base batches leaves one parseable JSON
  snapshot whose sequence/progress equal N; a deliberately interrupted temporary write leaves
  the previous snapshot intact.
- **Receipt-error lock:** an injected snapshot write/rename failure is labeled and loud after
  runtime cleanup; with profiling disabled the same fixture succeeds and makes no file calls.
- **Overhead-calibration lock:** the sf=1 instrumented-vs-uninstrumented wall-clock delta is
  recorded (a bound, not an assertion of zero) so sf=10 attribution is reported net of it.
- Carry the streaming/join suites green.

## Phase 2 - the profile (the deliverable)

```bash
scripts/cargo-cgroup build -p toron-tpch --release
mkdir -p target/tpch/adr426
# sf=1 inertness/overhead: separate processes, identical persisted input.
/usr/bin/time -v target/release/toron-tpch perf-run-one --engine toron --query q21 \
  --mode strict --sf 1 --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr426/q21_sf1_unprofiled.json \
  2> target/tpch/adr426/q21_sf1_unprofiled.err
rm -f target/tpch/adr426/q21_sf1_stages.json \
  target/tpch/adr426/q21_sf1_stages.tmp
/usr/bin/env TORON_CHAIN_STAGE_PROFILE_PATH=target/tpch/adr426/q21_sf1_stages.json \
  /usr/bin/time -v target/release/toron-tpch perf-run-one --engine toron --query q21 \
    --mode strict --sf 1 --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr426/q21_sf1_profiled.json \
  2> target/tpch/adr426/q21_sf1_profiled.err
# Compare digest, output row count, and output schema fingerprint before sf=10 attribution.
# Canonical gate first: must leave a parseable cumulative snapshot after the kill.
rm -f target/tpch/adr426/q21_gate.json target/tpch/adr426/q21_gate.tmp
unit="toron-adr426-q21-gate"
systemd-run --user --unit="$unit" --same-dir --wait --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --property=RuntimeMaxSec=600s \
  /usr/bin/env TORON_CHAIN_STAGE_PROFILE_PATH=target/tpch/adr426/q21_gate.json \
  target/release/toron-tpch perf-run-one --engine toron --query q21 --mode strict --sf 10 \
    --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr426/q21_gate.out 2>&1
{ echo "systemd_run_rc=$?"; systemctl --user show "$unit" --property=Result \
    --property=CPUUsageNSec --property=MemoryPeak; } > target/tpch/adr426/q21_gate.meta
systemctl --user reset-failed "$unit" 2>/dev/null || true
rm -rf /tmp/toron_join_spill
# One completion attempt: raised time budget, but completion is not assumed or credited.
rm -f target/tpch/adr426/q21_stages.json target/tpch/adr426/q21_stages.tmp
unit="toron-adr426-q21-full"
systemd-run --user --unit="$unit" --same-dir --wait --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --property=RuntimeMaxSec=1800s \
  /usr/bin/env TORON_CHAIN_STAGE_PROFILE_PATH=target/tpch/adr426/q21_stages.json \
  /usr/bin/time -v target/release/toron-tpch perf-run-one --engine toron --query q21 \
    --mode strict --sf 10 --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr426/q21_full.json 2> target/tpch/adr426/q21_full.err
{ echo "systemd_run_rc=$?"; systemctl --user show "$unit" --property=Result \
    --property=CPUUsageNSec --property=MemoryPeak; } > target/tpch/adr426/q21_full.meta
systemctl --user reset-failed "$unit" 2>/dev/null || true
rm -rf /tmp/toron_join_spill
```
- **Deliverable:** the canonical-gate cumulative seven-stage table (stage -> exclusive micros,
  % of total, rows/batches), exact progress/row flow, `unattributed_residual`, and dominant
  measured term. If the raised run completes, add the full-drive table. If it times out again or
  OOMs, record the exact terminal condition and last snapshot; never present it as completed or
  guess a mechanism from unstable partial shares.
- **Honesty:** the 1800 s completion (if any) is diagnostic-only; the board stays 18/22. Only a
  same-seal bounded fix bringing q21 under both 4 GiB and 600 s at pass@compare moves it to
  19/22.
- **Conditional fix proof:** if a same-seal fix lands, rerun q21 strict correctness under the
  canonical 4 GiB / 600 s gate; only `pass@compare` is green. Also rerun every control/alias that
  shares the changed recognizer or streaming path. A single light control is insufficient for a
  behavior change. Diagnosis-only leaves all controls byte-identical with the env var unset.

## Verification

```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron streaming --lib
scripts/cargo-cgroup test -p toron join --lib
scripts/cargo-cgroup test -p toron --test architecture_test        # FULL, after fmt
scripts/cargo-cgroup test -p toron --no-fail-fast                   # REQUIRED (engine cut, even if off-by-default)
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast
git diff --check
git ls-files '*.perf.data'
```

## Deliverable (results doc)

`crates/toron/plans/impl/0426_q21_throughput_profile_results.md`:
- Phase 0: reproduced timeout + the timing-gap pin - before code;
- the instrument: exclusive seven-stage timers, unattributed residual, atomic every-base-batch
  snapshot, off-by-default inertness; the sf=1 parity + overhead calibration;
- the profile: canonical-gate cumulative split (exclusive micros/%/rows), exact progress and
  residual; completed-drive split only if the raised attempt completes; dominant measured term;
- the named next mechanism when evidence is decisive (Utf8-COUNT primitive if the fold
  dominates; bounded probe extension if a probe dominates; C-spec evidence if the pull
  interface is implicated); unstable partial shares remain explicitly inconclusive;
- honesty: over-gate completion diagnostic-only, board stays 18/22 unless a same-seal bounded
  fix lands; instrument off-by-default output parity confirmed;
- controls untouched; `exec.rs` ratchet untouched; no `.perf.data`; scope honesty (q17/q02/q05
  named).

## Risk / blast radius

- **Memory-behind-time wall** -> reach honesty; partial receipt + new finding.
- **Residual dominates** -> the sum-to-total property surfaces the unattributed cost as the real
  driver.
- **Overhead distorts attribution** -> sf=1 calibration bounds it; timers are per-stage, not
  per-row.
- **Instrument leaks into behavior** -> off-by-default byte-parity lock.
- **exec.rs ratchet** -> instrument is confined to `streaming.rs`.

## Seal

`plan: adr 426 q21 throughput profile` (the dual). Implementation:
`impl: adr 426 q21 throughput profile`. Do not commit `.perf.data`.
