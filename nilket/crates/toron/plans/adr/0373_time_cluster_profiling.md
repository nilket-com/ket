# ADR 373 - TIME-cluster profiling (q04 canary, diagnostic-only)

## Context

The pivot from the memory arc to the perf arc. ADR 372 is the deciding evidence: q04 is keys-only with **zero spill
pages**, yet still CPU-bound at sf=10 - so at least one heavy path is no longer a memory problem. Grinding the remaining
9 memory walls first risks more "reachable but minutes-long" cells without knowing whether the execution substrate does
the wrong amount of work. **373 is diagnostic-only**: profile where q04's CPU goes, rank it, classify the time wall, name
the first perf cut. **No optimization** (unless a tiny obvious instrumentation fix). q04 canary; q07/q17/q19/q21 controls.

Leading hypothesis (to confirm, not assume): **missing parallelism** - the executor has no parallelism primitives
(`par_iter`/`rayon`/`tokio::spawn`/`join_all`/`available_parallelism` absent from `exec.rs`/`exec/`), so Toron likely
runs single-threaded while DuckDB uses all cores (q04 DuckDB=130ms, reference only).

## Decision

### Method - flamegraph, sampled inside the fixed-cap scope
Profiling tooling is set up: `cargo-flamegraph` 0.6.13, `inferno` 0.12.6, `perf_event_paranoid=1`,
`kptr_restrict=0`, and `perf record` verified. The profiling smoke must verify the same call-graph mode used for the
real run (`--call-graph dwarf`), because a frame-pointer-only smoke can pass while optimized Rust stacks are still
unusable. The flamegraph is the primary method (unbiased CPU sampling); its per-thread stacks answer both the
hot-operator breakdown and the **single-thread question**.

**The workload must be profiled inside the fixed-cap unit, not the wrapper.** Run `perf record` *inside* a
`systemd-run --scope` cap so it samples the `toron-tpch` process under the real 4 GB cap - never sample `systemd-run`
itself (wrapper profiling is invalid). q04 need NOT complete: a bounded `timeout 120s` **sample window** is valid if CPU
is steady; the report must label it "sample window", not "query runtime". A timeout exit status is expected for a
non-completing sample window and is not a profiling failure if `perf.data` renders and the sampled process was the
workload.

### Deliverables
1. **Flamegraph + ranked CPU-cost table** for q04 by operator kind (scan/filter, existence key build, semi probe,
   hash/key encoding, grouped fold, residual eval, exchange/partition), plus **cores-utilized** (cross-check the
   parallelism hypothesis) and an unclassified-stack share. If wall-vs-operator-sum is needed beyond the flamegraph's
   sample percentages, add only an off-by-default timing complement; do not invent wall-time accounting from samples.
2. **Controls:** q07/q17/q19/q21 profiled the same way - is the dominant cost **systemic** (shared: single-thread /
   hash-probe substrate) or **per-query** (isolated)?
3. **Classification** with evidence: algorithmic bug/quadratic, missing parallelism, slow hash/probe substrate, scan
   throughput, expression/filter overhead, or expected-but-too-slow baseline.
4. **One named first perf cut**, justified by the evidence.

### No optimization
373 changes no engine behavior. The only allowed code is a tiny obvious instrumentation fix; any kept profile hook is
off-by-default + behavior-neutral; a temporary hook is removed before seal. The flamegraph path needs no engine change.

## Acceptance
- **Flamegraph artifact(s)** for q04 (+ controls), produced **inside `systemd-run --scope`** at fixed 4 GB, persisted,
  release-with-debug-symbols; the **exact profiling command and whether `--scope` was used are recorded** (wrapper
  profiling is invalid and must be rejected).
- **Ranked CPU-cost table** + **cores-utilized** + unclassified-stack share, distilled from the flamegraph. Any
  wall-vs-operator-sum claim must come from explicit timing instrumentation, not flamegraph samples alone.
- **Time-wall classification** with evidence; **one named first perf cut**.
- **Controls** q07/q17/q19/q21 profiled; systemic-vs-isolated verdict.
- **Sample-window validity:** a non-completing 120s window is valid only if CPU is steady and the result is labeled
  "sample window", not "query runtime".
- **Memory arc paused only for this diagnostic.** Then decide: systemic -> fix before clearing more memory walls;
  isolated -> resume the memory board.
- **No cap bump; no engine behavior change; de-branded core; no qNN in executor core; prior greens unchanged.**

## Guardrails
- **Profile the workload, not the wrapper** - `perf record` inside `systemd-run --scope`; sampling `systemd-run` is
  invalid.
- **Confirm the parallelism hypothesis, don't assume it** - cores-utilized + per-thread flamegraph stacks are the
  evidence.
- **Sample window != query runtime** - label it; q04 needn't complete; require steady CPU.
- **Diagnostic-only** - the named cut is the deliverable, not an implementation; no optimization beyond a tiny obvious
  instrumentation fix.
- **DuckDB is a reference number, not a design.** No cap bump; no qNN in core.

## Risks
- **Wrapper-profiling artifact** - sampling systemd-run instead of toron-tpch; the recorded command + `--scope`
  requirement guards it.
- **Truncated/biased window** - a too-short or ramp-phase window misattributes cost; require steady CPU + a window long
  enough to be representative.
- **Two co-causes** - single-thread AND a slow hash/probe; the named cut picks the higher-leverage one, the other is
  queued.
- **Symbol gaps** - missing debug symbols flatten the flamegraph; `CARGO_PROFILE_RELEASE_DEBUG=true` is required.

## Forward arc
373 names the first perf cut. If **systemic** (missing parallelism / shared substrate): that cut greens multiple
time-cluster cells AND narrows the DuckDB gap on already-green cells - highest leverage, do it before more memory walls.
If **isolated/per-query**: resume the memory board (q08/q09, q18/q22, inner-build-spill x4, q02), time cluster queued.
The profile evidence decides. (Tooling hygiene: restore the loosened `perf_event_paranoid`/`kptr_restrict` after the
perf arc - operator's call.)
