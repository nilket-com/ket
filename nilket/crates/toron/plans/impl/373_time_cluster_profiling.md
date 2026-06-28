# ADR 373 - Implementation plan: TIME-cluster profiling (q04 canary)

Pairs with `plans/adr/373_time_cluster_profiling.md`. **Diagnostic-only**: flamegraph q04 sf=10 inside the fixed-cap
scope, rank the CPU cost, classify the time wall, name the first perf cut. No engine behavior change. q04 canary;
q07/q17/q19/q21 controls.

## Phase 0 - tooling + method check
- Confirm: `cargo-flamegraph` 0.6.13, `inferno` 0.12.6, `perf_event_paranoid=1`, `kptr_restrict=0`.
- Confirm the actual call-graph mode: `perf record -F 99 --call-graph dwarf -- true` succeeds. A `-g` smoke alone is
  not enough for optimized Rust symbol attribution.
- Confirm the persisted sf=10 dataset (`target/tpch/data/strict/sf10/manifest.json`).
- Confirm the render path: `perf script | inferno-collapse-perf | inferno-flamegraph`. If `inferno-*` binaries are not
  exposed, use `flamegraph`/`cargo flamegraph` directly or install inferno. Record which renderer was used.

## D1 - profile q04 (the canary), inside the cap scope
Build release with debug symbols, then sample the workload INSIDE a `systemd-run --scope` cap (never sample the wrapper):

```bash
CARGO_PROFILE_RELEASE_DEBUG=true \
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup build -p toron-tpch --release

mkdir -p target/tpch/profiles

systemd-run --user --scope --same-dir --collect --quiet \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  perf record -F 99 --call-graph dwarf \
    -o target/tpch/profiles/adr373_q04.perf.data -- \
    timeout --signal=INT 120s \
    target/release/toron-tpch perf --sf 10 --mode strict --runs 1 --warmup 0 \
      --query q04 --engines toron --data-source parquet \
      --data-dir target/tpch/data --output-dir target/tpch/perf_adr373_q04

perf script -i target/tpch/profiles/adr373_q04.perf.data \
  | inferno-collapse-perf \
  | inferno-flamegraph > target/tpch/profiles/adr373_q04.svg
```

- `perf record` is INSIDE the `--scope` unit -> it samples `toron-tpch` under the real 4 GB cap. **Sampling
  `systemd-run` itself is invalid.**
- q04 need NOT complete; `timeout 120s` gives a bounded **sample window**. Valid only if CPU is steady; label it "sample
  window", not "query runtime".
- The timeout exit status is expected for a non-completing sample window. Treat it as success only if `perf.data` exists,
  renders, and the sampled process was the workload.
- **Cores-utilized:** observe hot threads during the run (for example, `ps -L -p <perf-run-one-pid> -o pid,tid,psr,pcpu,comm`
  sampled repeatedly) - cross-check the parallelism hypothesis and the flamegraph's per-thread stacks.

## D2 - controls (systemic vs isolated)
Repeat for q07/q17/q19/q21 (lighter passes acceptable). Compare the dominant hot path: same across the cluster
(systemic) or different (per-query).

## D3 - distill + classify
- Map the flamegraph stacks to operator kinds -> the ranked CPU-cost table by sample percentage, with an unclassified
  stack share.
- Add wall-vs-operator-sum only if an off-by-default timing complement is added; flamegraph samples alone are not a
  wall-time accounting surface.
- Classify the time wall (algorithmic/quadratic, missing parallelism, slow hash/probe, scan throughput, expression/
  filter overhead, expected-but-too-slow) with the flamegraph evidence + cores-utilized.
- Name one first perf cut + the evidence that justifies it.

## No engine change
The flamegraph path needs no source change. If a tiny obvious instrumentation fix is warranted, it is off-by-default +
behavior-neutral (kept) or removed before seal. No optimization in 373.

## Verification
```text
# tooling
command -v cargo-flamegraph; command -v inferno-collapse-perf; command -v inferno-flamegraph
perf record -F 99 --call-graph dwarf -- true   # succeeds
# the profiling runs above (q04 + controls), inside systemd-run --scope
git diff --check
# if any (instrumentation) source change:
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
```

## Results doc
`crates/toron/plans/impl/373_time_cluster_profiling_results.md`:
- **the exact profiling command(s)** and confirmation `systemd-run --scope` was used (wrapper profiling rejected);
  renderer used; sample-window length + steady-CPU note;
- q04 ranked CPU-cost table (operator kind, % of samples) + unclassified stack share + cores-utilized;
- wall-vs-operator-sum only if explicit timing instrumentation was added;
- the flamegraph artifact path(s) (`target/tpch/profiles/*.svg`);
- controls q07/q17/q19/q21: dominant hot path each + systemic-vs-isolated verdict;
- DuckDB reference number(s);
- the time-wall classification + **one named first perf cut** with evidence;
- confirmation no engine behavior change, no cap bump, no qNN in core, instrumentation (if any) removed/off-by-default.

## Risk / blast radius
- **Wrapper profiling** - guarded by the `--scope` requirement + recorded command.
- **Biased/short window** - require steady CPU + a representative length; label sample-window.
- **Symbol gaps** - `CARGO_PROFILE_RELEASE_DEBUG=true` required.
- **Two co-causes** - name the higher-leverage cut; queue the other.

## Seal
`impl: adr 373 time cluster profiling`. Commit the results doc and any off-by-default instrumentation only. The large
`.perf.data` files stay uncommitted under `target/`; SVG flamegraphs may be committed only if intentionally small and
reviewable, otherwise reference their `target/tpch/profiles/` paths from the results doc. Do not commit wip drafts.
