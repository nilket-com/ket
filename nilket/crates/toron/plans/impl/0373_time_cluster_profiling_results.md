# ADR 373 results - TIME-cluster profiling

ADR 373 was diagnostic-only. No executor or harness behavior changed. The profiling run produced fixed-cap flamegraph
artifacts for q04 and the q07/q17/q19/q21 controls, then named the first perf cut from measured hot stacks.

## Method

Tooling:

- `cargo-flamegraph` 0.6.13 and `inferno` 0.12.6 were installed.
- `perf_event_paranoid=1` and `kptr_restrict=0`.
- `perf record -F 99 --call-graph dwarf -- true` succeeded before the workload profiles.
- The release binary was rebuilt with debug symbols and without stripping:

```bash
CARGO_PROFILE_RELEASE_DEBUG=true \
CARGO_PROFILE_RELEASE_STRIP=false \
CARGO_CGROUP_BACKEND=affinity \
scripts/cargo-cgroup build -p toron-tpch --release
```

`file target/release/toron-tpch` reported `with debug_info, not stripped`, and `readelf -S` showed `.debug_*` sections
plus `.symtab`.

All workload profiles ran `perf record` inside the fixed-cap unit:

```bash
systemd-run --user --scope --same-dir --collect --quiet \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  perf record -F 99 --call-graph dwarf \
    -o target/tpch/profiles/adr373_<query>.perf.data -- \
    timeout --signal=INT <window>s \
    target/release/toron-tpch perf --sf 10 --mode strict --runs 1 --warmup 0 \
      --query <query> --engines toron --data-source parquet \
      --data-dir target/tpch/data --output-dir target/tpch/perf_adr373_<query>
```

The q04 whole-window profile used a 120s timeout with no delay. The engine-focused q04 profile and the controls used
`perf record -D 60000` to skip the persisted-file validation startup and sample the execution window. Timeout exit
status `124` is expected for these non-completing sample windows; each run produced renderable `perf.data`.

Concrete q04 whole-window command:

```bash
systemd-run --user --scope --same-dir --collect --quiet \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  perf record -F 99 --call-graph dwarf \
    -o target/tpch/profiles/adr373_q04.perf.data -- \
    timeout --signal=INT 120s \
    target/release/toron-tpch perf --sf 10 --mode strict --runs 1 --warmup 0 \
      --query q04 --engines toron --data-source parquet \
      --data-dir target/tpch/data --output-dir target/tpch/perf_adr373_q04
```

Concrete q04 delayed engine-window command:

```bash
systemd-run --user --scope --same-dir --collect --quiet \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  perf record -F 99 --call-graph dwarf -D 60000 \
    -o target/tpch/profiles/adr373_q04_engine.perf.data -- \
    timeout --signal=INT 180s \
    target/release/toron-tpch perf --sf 10 --mode strict --runs 1 --warmup 0 \
      --query q04 --engines toron --data-source parquet \
      --data-dir target/tpch/data --output-dir target/tpch/perf_adr373_q04_engine
```

The control commands were the same delayed form with `<query>` replaced by `q07`, `q17`, `q19`, or `q21`, a 120s
timeout, and output paths `target/tpch/profiles/adr373_<query>.*`.

Rendered with:

```bash
perf script -i target/tpch/profiles/adr373_<query>.perf.data > target/tpch/profiles/adr373_<query>.perf.script
inferno-collapse-perf target/tpch/profiles/adr373_<query>.perf.script > target/tpch/profiles/adr373_<query>.folded
inferno-flamegraph target/tpch/profiles/adr373_<query>.folded > target/tpch/profiles/adr373_<query>.svg
```

Artifacts are local under `target/tpch/profiles/` and are not committed. The primary SVGs:

| profile | samples | artifact |
| --- | ---: | --- |
| q04 whole window | 11,913 | `target/tpch/profiles/adr373_q04.svg` |
| q04 delayed engine window | 11,903 | `target/tpch/profiles/adr373_q04_engine.svg` |
| q07 delayed control | 5,977 | `target/tpch/profiles/adr373_q07.svg` |
| q17 delayed control | 6,057 | `target/tpch/profiles/adr373_q17.svg` |
| q19 delayed control | 6,067 | `target/tpch/profiles/adr373_q19.svg` |
| q21 delayed control | 5,948 | `target/tpch/profiles/adr373_q21.svg` |

## Cores Utilized

q04 was single-hot-thread in both snapshots. During the delayed execution-window sample:

```text
PID      TID      PSR %CPU RSS    COMMAND
1913420  1913420  15  100 128504 toron-tpch
1913395  1913422  19  0.0 7708   toron-tpch
1913395  1913421  18  0.0 7708   toron-tpch
1913395  1913395  17  0.0 7708   toron-tpch
```

The leading parallelism hypothesis is confirmed as a property of the current execution: the hot path is one saturated
worker thread. The flamegraph also shows the more specific immediate cost inside that thread.

## q04 Findings

The first q04 whole-window profile was intentionally kept because it exposed a harness cost: persisted dataset loading
rehashes the Parquet files before execution. That whole-window profile was dominated by
`toron_tpch::stable_file_hash`, not by the engine:

| q04 whole-window category | samples |
| --- | ---: |
| harness dataset validation (`stable_file_hash`) | 75.07% |
| kernel/timer/perf/system overhead | 12.48% |
| allocator/memcpy/drop/native overhead | 6.60% |
| existence spill resident accounting/index scan | 3.52% |
| parquet scan/decode/filter/take | 0.97% |
| unclassified | 1.35% |

This is outside `execute_ms`, so it is a harness/perf-rail overhead finding, not the engine time wall. The delayed q04
profile is the engine sample used for the CPU-cost table:

| q04 engine-window category | samples |
| --- | ---: |
| existence spill resident accounting/index scan | 97.60% |
| kernel/timer/perf/system overhead | 1.19% |
| allocator/memcpy/drop/native overhead | 0.54% |
| parquet scan/decode/filter/take | 0.28% |
| hash map insert/probe/key encode | 0.16% |
| unclassified | 0.23% |

The dominant stack is:

```text
SpilledSemiAntiExistenceBuild::index_membership_keys
  -> guard_resident_state
  -> semi_anti_existence_spill_index_bytes
  -> HashMap<JoinKey, Vec<SpilledExistenceRowRef>>::iter().fold(...)
```

This points at `crates/toron/src/exec/join/spill.rs`: `guard_resident_state` recomputes resident index bytes by folding
the whole index after indexing work. q04 is not spending its sampled execution window in the semi probe, residual
evaluation, grouped fold, or scan.

DuckDB reference from ADR 366: q04 passes at sf=10 in about 130 ms. Toron q04 is CPU-bound for minutes after ADR 372
removed the spill-volume wall.

## Controls

| query | delayed profile dominant category | samples | DuckDB sf=10 reference | verdict |
| --- | --- | ---: | ---: | --- |
| q07 | inner spill resident accounting/index scan | 98.40% | 210 ms | same resident-accounting family, inner-build variant |
| q17 | inner spill resident accounting/index scan | 84.02% | 204 ms | same resident-accounting family, with some spill page reads |
| q19 | spill page read/decrypt/decompress + syscall overhead | 50.34% read/decrypt/decompress, 41.61% kernel/system | 161 ms | distinct spill-read/I/O time wall |
| q21 | existence spill resident accounting/index scan | 98.11% | 581 ms | same resident-accounting family, semi/anti variant |

Control details:

| query | secondary categories |
| --- | --- |
| q07 | hash map insert/probe/key encode 0.41%, unclassified 0.57% |
| q17 | spill page read/decrypt/decompress 5.31%, kernel/system 3.33%, unclassified 4.57% |
| q19 | unclassified 7.68%, allocator/native 0.37% |
| q21 | kernel/system 0.67%, hash map insert/probe/key encode 0.28%, unclassified 0.57% |

## Classification

The TIME cluster is not one uniform operator shape:

- q04 and q21 are dominated by semi/anti existence-set resident accounting.
- q07 and q17 are dominated by regular inner spilled-build resident accounting.
- q19 is dominated by spill page read/decrypt/decompress and syscall overhead.

The common systemic issue is still one-thread execution, but the highest-confidence first cause is more specific:
the resident guards repeatedly compute spill-index byte sizes by walking large hash maps. Parallelism would only spread
that repeated accounting over more cores; it would not remove the wasted O(index-size) scan on every guard. The first
perf cut should remove the redundant accounting before a broad parallelism cut.

No wall-vs-operator-sum is reported from ADR 373, because no explicit timing instrumentation was added. Percentages are
CPU sample shares from flamegraphs.

## Named First Perf Cut

ADR 374 should be **incremental spill resident accounting**:

- Maintain resident index byte counters as `SpilledInnerJoinBuild` and `SpilledSemiAntiExistenceBuild` mutate their
  indexes.
- Replace `spilled_batch_join_index_bytes(&self.index)` and
  `semi_anti_existence_spill_index_bytes(&self.index)` inside `guard_resident_state` with O(1) maintained counters.
- Preserve the existing budget semantics and diagnostic producer names; this is a perf cut, not a cap change.
- Lock with focused tests that counters match the old fold on synthetic indexes and that guard decisions are unchanged.
- Re-profile q04/q07/q17/q21 after the cut. Expected result: the resident-accounting stacks disappear; if the cells stay
  time-bound, the next hot stack becomes the next evidence-backed perf cut.

q19 is queued as a separate follow-up: spill page read/decrypt/decompress throughput. It should not drive ADR 374.

## Verification

- No source code changed in ADR 373.
- No cap changed.
- No executor-core names or comments were added.
- Large profile artifacts remain under `target/tpch/profiles/` and are uncommitted.

```bash
git diff --check
```
