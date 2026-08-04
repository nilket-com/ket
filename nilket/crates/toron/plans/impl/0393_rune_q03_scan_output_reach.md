# ADR 393 - Implementation plan: Rune q03 scan-output reach unblock

Pairs with `plans/adr/0393_rune_q03_scan_output_reach.md`.

Objective: make the ADR 391 Rune-authored q03 shape reach at persisted sf=3, or remove the `scan output accumulator` and
name the next reach producer. This is a reach cut. Do not bundle substrate optimization.

## Phase 0 - reproduce and map the miss

Use the existing persisted data and release harness:

```bash
CARGO_PROFILE_RELEASE_DEBUG=true CARGO_CGROUP_BACKEND=affinity \
  scripts/cargo-cgroup build -p toron-tpch --release

test -f target/tpch/data/strict/sf3/manifest.json || \
  systemd-run --user --same-dir --wait --collect --quiet --pipe \
    --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
    target/release/toron-tpch prepare --data-source parquet --mode strict --sf 3 --data-dir target/tpch/data

systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  target/release/toron-tpch perf --sf 3 --mode strict --runs 1 --warmup 0 --query q03 \
    --engines rune --data-source parquet --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr393_phase0_rune_q03
```

Record the raw failure row and producer:

```text
expected current producer: scan output accumulator
expected current requested/available: 1,073,790,848 / 1,073,741,824
```

Then map the bound plan and executor recognizer:

- SQL q03 physical fingerprint/control from `rune_q03_isolation_proof(...)`.
- Rune q03 physical fingerprint/control from `rune_q03_isolation_proof(...)`.
- Reconcile ADR 391's Rune isolation fingerprint (`0fd3784049e71920`) with ADR 392's Rune perf-row fingerprint
  (`2b72c561c2595512`):
  - if scale-gated physical path selection explains it, record the scale/path gate;
  - if isolation-proof bind and perf-path bind diverge, record the bind difference before implementation;
  - if either value is stale, update the evidence trail before relying on it.
- Whether `extract_reoriented_inner_chain` recognizes the Rune-bound q03 plan.
- Stage count, base/probe shape, and each stage build edge.
- For the failing edge:
  - table/scan identity;
  - `decompose_streaming_scan_probe_input` result;
  - scan-output estimate and cap;
  - selected path at sf=1 and sf=3;
  - consumer path (`probe_reoriented_inner_join_streaming` or materialized fallback);
  - complete `ScanRequest` field map.

Add a focused diagnostic test if needed so the miss is reproducible without a full sf=3 run.

## Phase 1 - implement only the measured topology

Follow Phase 0:

- If the Rune q03 plan is a reoriented-inner-chain variant:
  - widen the existing stage/base/probe gate only for the measured topology;
  - reuse `bound_scan_request` and direct `storage.scan`;
  - use the existing memory-aware scan-output gate so sf=1 remains materialized;
  - route non-in-memory stage/base runtime through `probe_reoriented_inner_join_streaming`;
  - add a shape-fence test proving unrelated plans still materialize.
- If the Rune q03 plan misses the chain recognizer entirely:
  - either add the smallest topology recognition needed for the bound Rune shape; or
  - stop and record the recognizer miss as the next design cut if the required widening is broad.
- Do **not** branch on Rune/q03/engine in `crates/toron/src`.
- Do **not** change `RUNE_Q3_SCRIPT` unless Phase 0 proves the script is semantically wrong; a shape rewrite that
  collapses to SQL does not answer ADR 393.

## Phase 2 - live measurement

Run the canary:

```bash
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --setenv=TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/latest/duckdb \
  target/release/toron-tpch perf --sf 3 --mode strict --runs 7 --warmup 1 --query q03 \
    --engines rune,duckdb --data-source parquet --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr393_q03_sf3_rune_duckdb
```

Interpretation:

- Rune `phase=compare status=pass` + stable DuckDB -> compute the 392-redux `Rune/DuckDB` ratio and label it as a
  streaming-substrate ratio, not a pure kernel/materialize number.
- Rune `scan output accumulator` gone but another producer appears -> record wall-move; substrate remains unmeasured.
- Rune unchanged -> record why the measured topology was not covered; no false green.

Run the small-scale control:

```bash
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  target/release/toron-tpch perf --sf 1 --mode strict --runs 7 --warmup 1 --query q03 \
    --engines rune --data-source parquet --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr393_q03_sf1_rune_control
```

The sf=1 control should stay on the materialized path or show no material regression against ADR 392's Rune median
(`1580.6 ms`, spread `0.013`, spill `0`).

## Deliverable

`crates/toron/plans/impl/0393_rune_q03_scan_output_reach_results.md`:

- Phase 0 producer table with the exact Rune q03 miss.
- Code-path summary: which recognizer/gate/consumer was widened, and why it is topology-based.
- sf=3 q03 result table for Rune and DuckDB:
  - status, phase, median/min/spread, producer, correctness, ratio if valid.
- sf=1 Rune control result and regression verdict.
- Whether substrate is now measurable:
  - if yes, `Rune/DuckDB` streaming-substrate ratio and next substrate-profile ADR;
  - that follow-up profile must split kernel/materialization time from streaming/spill time before naming precision
    strikes;
  - if no, named next reach producer.
- Confirmation: no cap bump, no qNN/frontend branch in core, no large `.perf.data` committed.

## Verification

```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup test -p toron --no-fail-fast
systemd-run ... perf --sf 3 --engines rune,duckdb --query q03 --data-source parquet
systemd-run ... perf --sf 1 --engines rune --query q03 --data-source parquet
git diff --check
```

If code touches only `crates/toron-tpch` diagnostics, explain why engine gates still passed. If it touches
`crates/toron/src`, the full `toron --no-fail-fast` gate is mandatory.

## Seal

Plan commit: `plan: adr 393 rune q03 scan output reach`.
Implementation commit: `impl: adr 393 rune q03 scan output reach`.
