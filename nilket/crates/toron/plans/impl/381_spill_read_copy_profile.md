# ADR 381 - Diagnostic-execution plan: symbol-retaining spill read/copy profile (q03/q07)

Pairs with `plans/adr/381_spill_read_copy_profile.md`. Diagnostic-only: name the exact spill read/copy/alloc Toron frame
for q03/q07 with a **symbol-verified** release build (the 380 gap), plus a non-spill control + a parallelism side table.
No engine change.

## Phase 0 - build + HARD symbol gate (before sampling)
```bash
CARGO_PROFILE_RELEASE_DEBUG=true CARGO_PROFILE_RELEASE_STRIP=false \
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup build -p toron-tpch --release
file target/release/toron-tpch                                  # expect: with debug_info, not stripped
readelf -S target/release/toron-tpch | rg 'debug|symtab'        # expect .debug_* + .symtab
nm -C target/release/toron-tpch | rg 'read_page|decode_record_batch_ipc|open_in_place|read_all_batch_pages'
```
**Gate:** if `.debug_*`/`.symtab` are absent OR the spill-read functions don't resolve -> STOP, fix the release profile
(strip/debug settings), do NOT collect a coarse profile. Record the `readelf`/`nm` evidence in the results doc.

## Phase 1 - profile q03/q07 (inside --scope, bounded window)
```bash
mkdir -p target/tpch/profiles
systemd-run --user --scope --same-dir --collect --quiet \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  perf record -F 99 --call-graph dwarf -D 60000 \
    -o target/tpch/profiles/adr381_<q>.perf.data -- \
    timeout --signal=INT 120s \
    target/release/toron-tpch perf --sf 10 --mode strict --runs 1 --warmup 0 \
      --query <q> --engines toron --data-source parquet --data-dir target/tpch/data \
      --output-dir target/tpch/perf_adr381_<q>
perf script -i target/tpch/profiles/adr381_<q>.perf.data | inferno-collapse-perf | inferno-flamegraph > .../adr381_<q>.svg
perf report --stdio -g -i target/tpch/profiles/adr381_<q>.perf.data   # recover inlined Toron callers
```
On hybrid CPUs, do not let `perf report` silently show only one event stream (`cpu_atom/cycles` vs `cpu_core/cycles`).
Either record a single explicit event, or report every recorded event and state the denominator in the results doc.
For q03 and q07: rank the spill read/copy/alloc frames - `.to_vec()` decrypt copy (spill.rs:337/950/1565) vs
`decode_record_batch_ipc` vs `read_plain_at` (page read) vs RecordBatch alloc - with sample %. Name the dominant.

## Phase 1 - non-spill control
Same build settings, profile **q06 or q14**. Confirm the spill-read frames do NOT dominate it (artifact guard).

## Phase 2 - parallelism side table
```bash
# q03/q07: default vs forced sequential, wall + K
... target/release/toron-tpch perf --sf 10 --query q03,q07 ... (default)
TORON_PARTITION_PARALLELISM=1 ... target/release/toron-tpch perf --sf 10 --query q03,q07 ...
```
Record wall-times + computed K; if default is neutral/negative, name the engagement-policy follow-up. Side table only.

## Deliverable (results doc)
`crates/toron/plans/impl/381_spill_read_copy_profile_results.md`:
- **symbol-gate evidence** (`file`/`readelf`/`nm` output proving symbols);
- q03/q07 ranked spill read/copy/alloc table with **exact Toron frames** + sample % + **the named dominant frame**;
- the perf event stream/denominator used for the sample percentages;
- the **non-spill control** (q06/q14): spill-read frames do not dominate;
- the **parallelism side table** (`=1` vs default wall + K) + the follow-up if neutral/negative;
- **382's named lever** (which read-path copy/alloc to reduce);
- confirmation no engine change, profiled inside `--scope`, sample-window labeled, `--no-fail-fast` green, no qNN,
  `.perf.data` uncommitted.

## Verification
```text
# the symbol gate (above) MUST pass before sampling
scripts/cargo-cgroup test -p toron --no-fail-fast    # unchanged (diagnostic ships nothing)
git diff --check
```

## Risk / blast radius
- **Symbols absent** - hard gate stops; fix the build first.
- **Artifact vs spill cost** - the non-spill control distinguishes.
- **Inlined frame unresolved** - `perf report -g` + the candidate shortlist.
- **Scope creep** - 381 names only; 382 reduces.

## Seal
`plan: adr 381 spill read copy profile` (the dual). Diagnostic results carry the decision; if any helper code is retained
it is off-by-default and seals `impl: adr 381 ...`. Do not commit large `.perf.data`.
