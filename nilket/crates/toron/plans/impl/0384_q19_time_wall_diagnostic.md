# ADR 384 - Diagnostic-execution plan: q19 time-wall diagnostic

Pairs with `plans/adr/0384_q19_time_wall_diagnostic.md`. Diagnostic-only: name q19's exact frame, **prove or disprove
spill-page repetition with a counted artifact**, control-compare to q03/q07, classify, name the 385 branch. No engine
change (instrumentation off-by-default + removed before seal).

## Phase 0 - build + HARD symbol gate (before sampling)
```bash
CARGO_PROFILE_RELEASE_DEBUG=true CARGO_PROFILE_RELEASE_STRIP=false \
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup build -p toron-tpch --release
file target/release/toron-tpch                                   # with debug_info, not stripped
readelf -S target/release/toron-tpch | rg 'debug|symtab'
nm -C target/release/toron-tpch | rg 'read_page|read_batch_page|read_plain_at|decode_record_batch_ipc|probe_partitioned'
```
Absent/unresolved -> STOP, fix the build; no coarse profile.

## Phase 1 - profile q19 (inside --scope, delayed window)
```bash
systemd-run --user --scope --same-dir --collect --quiet \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  perf record -e task-clock -F 99 --call-graph dwarf -D 60000 \
    -o target/tpch/profiles/adr384_q19.perf.data -- \
    timeout --signal=INT 180s \
    target/release/toron-tpch perf-run-one \
      --query q19 --mode strict --sf 10 --run-index 0 \
      --data-source parquet --data-dir target/tpch/data
perf script -i ...adr384_q19.perf.data | inferno-collapse-perf | inferno-flamegraph > ...adr384_q19.svg
perf report --stdio -g -i ...adr384_q19.perf.data
```
Name the exact dominant Toron frame.

## Phase 1 - the read-count HARD ARTIFACT (prove repetition, don't infer)
Establish unique-once vs repeated with **one of**:
- **syscall counts:** `perf stat -e syscalls:sys_enter_pread64` (or `strace -c -f`) over the sample window - `pread64`
  count vs the unique spill-page count. Syscall counts alone show read volume; they prove repetition only when paired
  with a measured unique-page denominator from spill metadata/instrumentation; or
- **temporary/off-by-default page-read counter** keyed by `(spill_id, page_index)` in `read_page` (env-gated, e.g.
  `TORON_SPILL_READ_COUNTERS=1`), reporting reads-per-page; any page with reads > 1 = repeated. **Removed before seal.**
- If neither obtained -> record **"spill-read dominant, repetition unproven"** (no inference-only re-read claim).

## Phase 1 - control comparison
Profile q03 and q07 the same way (or reuse adr381 data for hot-frame context only) for the control table. The
`reads vs unique pages` column must come from the same read-count artifact used for q19; if it is not measured for a
control, mark that control's read pattern as not comparable instead of borrowing the old flamegraph-only result.

`query | dominant hot frame | RSS | spill_bytes | reads vs unique pages | window`

q19 vs q03/q07: same-floor-larger-volume vs distinct-pathology.

## Classification + 385 branch
- **Repeated-read (proven):** 385 = re-read elimination.
- **Same floor, larger volume:** 385 = spill-format/read-path substrate (not query-specific).
- **Join-algorithm / OR-branch blowup:** 385 = the named join/filter algorithm cut.
- **Repetition unproven / harness artifact:** named; 385 starts by obtaining the missing evidence.

## Deliverable (results doc)
`crates/toron/plans/impl/0384_q19_time_wall_diagnostic_results.md`:
- symbol-gate evidence; the exact q19 dominant frame;
- **the read-count artifact** (syscall counts or per-page-id counter) -> unique-once vs repeated, or "repetition
  unproven";
- the control table (q19 vs q03/q07);
- the classification + the named ADR 385 branch + evidence;
- confirmation no engine change (instrumentation removed/off-by-default), `--no-fail-fast` unchanged, no qNN,
  `.perf.data` uncommitted, sample window labeled.

## Verification
```text
# symbol gate must pass before sampling
scripts/cargo-cgroup test -p toron --no-fail-fast    # unchanged (diagnostic ships nothing)
git diff --check
```

## Risk / blast radius
- **Re-read inferred not counted** -> the syscall/per-page-id artifact gates it; "unproven" if absent.
- **Symbols absent** -> hard gate stops.
- **Window misses steady state** -> delayed `-D` past startup; steady CPU.
- **Inherent floor / harness artifact** -> named via the control + a non-spill check.

## Seal
`plan: adr 384 q19 time wall diagnostic` (the dual). Results carry the decision; any retained counter is off-by-default
and seals `impl: adr 384 ...`, otherwise removed. Do not commit large `.perf.data`.
