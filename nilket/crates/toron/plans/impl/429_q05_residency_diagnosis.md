# ADR 429 - Implementation plan: q05 residency diagnosis

Pairs with `plans/adr/429_q05_residency_diagnosis.md`. Add a query-neutral, off-by-default,
kill-surviving residency receipt to the generic grouped partition-reservoir path and use it to
explain q05's ADR 413 green -> ADR 414/415 hard-OOM transition. Diagnosis only; ADR 430 owns the
fix. No `exec.rs`, budget, comparator, schema, or query-specific engine branch change.

## Phase 0 - baseline before instrument code

1. **Pin source and history.** Record the promoted-plan parent (`5a2f7be`, ADR 428 implementation)
   as the current uninstrumented source. Keep these distinct:
   - ADR 413 perf: pass, 190,034 ms, 5,353,985,328 spill bytes, 3,051,064 KiB process RSS,
     fingerprint `757d7c625669b21a`;
   - ADR 413 correctness: `pass@compare`, with the systemd cgroup peak exactly 4 GiB;
   - ADR 414 clean-plan/current-worktree non-reproduction;
   - ADR 415: three 4 GiB OOM kills at about 42 s and about 1.3 GiB scratch.
2. **Reproduce on the pinned uninstrumented source.** Use a detached baseline build if the main
   worktree already contains instrument code. For each of three fresh-scratch runs, retain
   stdout/stderr, systemd `Result`/`ExecMainStatus`/`MemoryPeak`, wall time, surviving scratch
   bytes, and `memory.events` while the completed unit's cgroup remains readable. If systemd has
   already removed it, record that unavailability and retain the journal's OOM-kill evidence.
   Never infer process RSS when `/usr/bin/time` dies with the unit.
3. **Map the owning path.** Record the current plan fingerprint from a completing same-plan
   scale, then map:
   `base accumulation -> writer-set creation -> reservoir write -> finish -> base concat ->
   reservoir probe -> suffix -> fold`. Confirm the base remains live through reservoir build.
   Treat partial scratch as evidence consistent with an incomplete reservoir, not proof of the
   kill phase.
4. **Map instrumentable high-water sites.** Identify writer-set creation, reservoir append/index
   growth, phase transitions, and base concat. Record which projected deltas are exact and which
   are conservative estimates. Record before code.

Canonical baseline command shape (run from the pinned baseline build):

```bash
scripts/cargo-cgroup build -p toron-tpch --release
mkdir -p target/tpch/adr429
for i in 1 2 3; do
  rm -rf /tmp/toron_join_spill
  unit="toron-adr429-q05-pre-$i"
  systemd-run --user --unit="$unit" --same-dir --wait --quiet --pipe \
    --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
    --property=RuntimeMaxSec=600s \
    /usr/bin/time -v target/release/toron-tpch perf-run-one --engine toron --query q05 \
      --mode strict --sf 10 --data-source parquet --data-dir target/tpch/data \
    > "target/tpch/adr429/q05_pre_$i.json" \
    2> "target/tpch/adr429/q05_pre_$i.err"
  rc=$?
  {
    echo "systemd_run_rc=$rc"
    systemctl --user show "$unit" --property=Result --property=ExecMainStatus \
      --property=MemoryPeak
  } > "target/tpch/adr429/q05_pre_$i.meta"
  control_group=$(systemctl --user show "$unit" --property=ControlGroup --value)
  if [[ -n "$control_group" && -r "/sys/fs/cgroup${control_group}/memory.events" ]]; then
    cp "/sys/fs/cgroup${control_group}/memory.events" \
      "target/tpch/adr429/q05_pre_$i.memory.events"
  fi
  du -sb /tmp/toron_join_spill > "target/tpch/adr429/q05_pre_$i.scratch" 2>/dev/null || true
  systemctl --user reset-failed "$unit" >/dev/null 2>&1 || true
done
```

## Phase 1 - durable dual-ledger receipt

### 1. Query-neutral profile surface

- Add one env-gated path such as `TORON_GROUPED_RESERVOIR_RESIDENCY_PROFILE_PATH`; no `Q05` or
  TPC-H vocabulary in engine source.
- Resolve it once at admission. Disabled: no receipt object, buffers, filesystem calls, or
  sampling; one inactive branch is allowed. Tests inject a sink/config and do not mutate the
  process-global environment.
- Place implementation in `exec/join/streaming.rs` or a semantic join submodule if the source
  ceiling requires it. Do not touch `exec.rs`.

### 2. Snapshot schema and checked accounting

Every versioned JSON snapshot contains sequence, event (`start`, `progress`, `intent`, `phase`,
`terminal`), phase, completed build batches/rows, active partition if known, cadence and intent
threshold, and availability/error markers.

Maintain two explicitly non-additive ledgers:

- **Engine:** base owned/accounted bytes and collection capacity; every reservoir writer's
  resident-index, writer-scratch/peak, cache, page/path metadata, handle/collection capacity;
  aggregate, maximum, and writer count; receipt-owned bytes; checked engine total and applicable
  residual/overlap label. Record spill bytes separately as disk progress.
- **OS/cgroup:** process RSS; cgroup `memory.current`; available `memory.stat` categories
  (`anon`, `file`, `file_dirty`, `file_writeback`, `shmem`, `kernel`, `slab`, with missing keys
  explicit). The live receipt does not pretend to know final `MemoryPeak`; the runner appends the
  systemd postmortem and `memory.events` to the results evidence.

Do not add engine bytes to cgroup category bytes. Do not sum spill disk bytes into either memory
total. All arithmetic is checked; overlap or a component sum above its comparison total is an
explicit state/error, never a saturating residual.

### 3. Bounded durable cadence

- Atomic sibling-temp write then rename at start, terminal/error, each phase transition, and
  every 64 completed reservoir-build batches.
- Before writer-set creation and base concat, persist an intent snapshot.
- During reservoir writes, persist an intent before any instrumentable allocation/capacity event
  whose projected resident high-water advances by >=16 MiB. Store current, projected delta,
  projected total, owner, and exact-vs-estimated status before performing the operation.
- Smaller changes are covered by the 64-batch cadence. Store the last completed boundary so the
  results document can state the maximum attribution gap rather than claiming exact kill-site
  precision.
- Enabled sampling/serialization/write/rename failures are labeled and loud. Missing cgroup
  fields are recorded unavailable, not zero.

### 4. Tests

- **Disabled parity:** same fixture output/digest/telemetry, zero receipt writes and no receipt
  state when disabled.
- **Dual-ledger lock:** engine, disk, RSS, and cgroup fields remain separately labeled and are
  never cross-summed; checked residual/overlap behavior is discriminating.
- **Writer aggregate lock:** all writers and collection capacity contribute; one nominal writer
  cannot masquerade as the aggregate.
- **Intent ordering:** injected stop after intent but before a >=16 MiB allocation leaves that
  intent as the last durable snapshot; sub-threshold changes remain covered by progress cadence.
- **Cadence lock:** no gap exceeds 64 completed build batches, and all phase transitions flush.
- **Atomicity:** interrupted temp write preserves the prior parseable snapshot.
- **Parser/error locks:** cgroup-v2 parsing handles missing/unknown keys explicitly; enabled
  snapshot failures are loud.
- Carry streaming, join, and spill suites green.

## Phase 2 - calibration stop line

Build release and run three fresh-process sf=1 persisted q05 samples disabled and three enabled,
under the same cgroup properties. For each retain output digest, plan fingerprint, wall time,
maximum RSS, receipt count/size, and scratch cleanup.

Proceed to sf=10 only when:

- every enabled result matches disabled output/digest/telemetry and plan fingerprint;
- enabled median wall overhead is <=10%;
- enabled maximum-RSS delta is <=32 MiB;
- the receipt is parseable and its configured 64-batch / 16 MiB bounds are present.

If a bound fails, reduce serialization buffers, sampling work, or other receipt overhead and
recalibrate without weakening the 64-batch / 16 MiB attribution bounds. If the fixed bounds
cannot be met without material perturbation, re-converge the instrument design; do not waive the
stop line.

## Phase 3 - canonical diagnosis

```bash
scripts/cargo-cgroup build -p toron-tpch --release
mkdir -p target/tpch/adr429
rm -rf /tmp/toron_join_spill
unit="toron-adr429-q05-diag"
systemd-run --user --unit="$unit" --same-dir --wait --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --property=RuntimeMaxSec=600s \
  /usr/bin/env \
    TORON_GROUPED_RESERVOIR_RESIDENCY_PROFILE_PATH=target/tpch/adr429/q05_residency.json \
  /usr/bin/time -v target/release/toron-tpch perf-run-one --engine toron --query q05 \
    --mode strict --sf 10 --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr429/q05_diag.json 2> target/tpch/adr429/q05_diag.err
rc=$?
{
  echo "systemd_run_rc=$rc"
  systemctl --user show "$unit" --property=Result --property=ExecMainStatus \
    --property=MemoryPeak
} > target/tpch/adr429/q05_diag.meta
control_group=$(systemctl --user show "$unit" --property=ControlGroup --value)
if [[ -n "$control_group" && -r "/sys/fs/cgroup${control_group}/memory.events" ]]; then
  cp "/sys/fs/cgroup${control_group}/memory.events" \
    target/tpch/adr429/q05_diag.memory.events
fi
du -sb /tmp/toron_join_spill > target/tpch/adr429/q05_diag.scratch 2>/dev/null || true
systemctl --user reset-failed "$unit" >/dev/null 2>&1 || true
rm -rf /tmp/toron_join_spill
```

The last receipt plus postmortem must name the phase, engine ownership state, cgroup category
shape, and bounded gap to the kill. Compare it with both ADR 413 rows: process-RSS perf and exact
4 GiB correctness cgroup peak. If file cache, allocator residual, or distributed co-residency
dominates, say so; do not force the result into base-vs-writer.

If the receipt is inconclusive, add the minimum checkpoint at the last ambiguous owning boundary
and rerun. Do not seal an inferred peak owner. Once discriminating, name ADR 430's mechanism;
do not implement it in ADR 429. The board remains 19/22.

### Disabled controls

Run focused persisted sf=10 controls for the shared reoriented/grouped reservoir family with the
profile env absent (at minimum q03, q10, and q19, plus any additional query shown by the Phase 0
path census to enter the instrumented helper). Full Toron and toron-tpch gates remain required.
Do not claim all thirteen controls were rerun unless they actually were.

## Verification

```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron streaming --lib
scripts/cargo-cgroup test -p toron join --lib
scripts/cargo-cgroup test -p toron spill --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast
git diff --check
git ls-files '*.perf.data'
```

## Results document

`crates/toron/plans/impl/429_q05_residency_diagnosis_results.md` records:

- promoted-parent baseline and the four distinct historical evidence classes (ADR 413 perf,
  ADR 413 correctness/cgroup, ADR 414 non-reproduction, ADR 415/current kills);
- path and high-water map, without inferring phase solely from scratch bytes;
- versioned receipt schema, dual-ledger/non-additive semantics, cadence, intent threshold,
  atomic durability, disabled behavior, and calibration results;
- final snapshot + cgroup postmortem, bounded attribution gap, named peak owner, and precise ADR
  430 mechanism—or the refined rerun if the first receipt was inconclusive;
- diagnosis-only honesty, board 19/22, focused disabled controls, full gates, untouched
  `exec.rs` ratchet, no engine q05 branding, and no `.perf.data`.

## Risk / blast radius

- Cgroup file cache can dominate despite modest process RSS; the OS ledger is required.
- Logical engine accounting may overlap shared Arrow buffers; overlap is labeled, not hidden.
- Snapshot I/O can perturb the wall; quantitative calibration is a hard stop line.
- A distributed peak is a valid result; a fictional single allocation is not required.
- Scope is observational and query-neutral; ADR 430 owns all execution changes.

## Seal

Plan dual: `plan: adr 429 q05 residency diagnosis`. Implementation seal:
`impl: adr 429 q05 residency diagnosis`. Do not commit `.perf.data`.
