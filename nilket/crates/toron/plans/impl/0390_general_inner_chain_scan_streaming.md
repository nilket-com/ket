# ADR 390 - Implementation plan: general inner-chain stage-build scan streaming (q03/q05/q10)

Pairs with `plans/adr/0390_general_inner_chain_scan_streaming.md`. The all-stage loop + per-stage memory-aware gate
already exist; 390 **diagnoses why they decline q03/q05/q10 and widens only that gate**, shape-fenced to the
reoriented-inner-chain. Stage build scans are the target; probe only if proven. No cap bump; full engine gates.

## Phase 0 - diagnose the decline (before code)
For q03/q05/q10, walk the reoriented-inner-chain and fill, per stage:
- `base_scan_probe_mode` (`reoriented_inner_chain_grouped_base_scan_probe_mode`:757): FilteredJoin0 / BareJoin0 / None;
- `decompose_streaming_scan_probe_input(stage.build)` (655): Some / None;
- scan-output estimate vs cap (the memory-aware gate input);
- base traversal / probe API: streamed base traversal + `probe_reoriented_inner_join_streaming`, or fallback base
  traversal + synchronous `probe_reoriented_inner_join`;
- runtime today: InMemory / Spilled / PartitionedScan;
- **reason not streamed:** base-mode gate / undecomposable / under-cap / consumer-mismatch.

Expected: q03/q05/q10 stages decompose + exceed cap but the **global base mode is None/declined**, gating the whole
streamed path. Confirm before code.

## D1 - widen the declining gate (shape-fenced)
- If the decline is the base-mode global gate: **allow a stage's build-scan to stream when the stage is itself eligible**
  (`decompose_streaming_scan_probe_input(stage.build).is_some()` AND scan-output estimate > cap), **independent of the
  global base mode**, but still inside the reoriented-inner-chain path (shape-fence: the chain context + stage
  eligibility, not a global recognizer).
- **Stop line:** do not prepare a non-in-memory stage runtime unless the downstream traversal will consume it via
  `probe_reoriented_inner_join_streaming`. The current fallback traversal uses synchronous
  `probe_reoriented_inner_join` and rejects non-in-memory runtimes; if q03/q05/q10 only reach that path, either wire the
  traversal to the streaming probe with parity tests or name the edge as consumer-mismatch residual.
- Reuse `reoriented_inner_stage_scan_build_exceeds_scan_output_cap` per stage (memory-aware: sf=1 materialized).
- Undecomposable / consumer-mismatch edges: name residual, don't force.
- Topology-named; no qNN.

## D2 - probe scan (only if Phase 0 proves it)
- If Phase 0 shows the probe/base scan is the remaining scan-output producer **and** the existing base streaming path
  covers it cleanly, stream it; else name it residual (separate semantics - do not materialize-vs-stream one side
  inconsistently).

## D3 - measured coverage
- Re-measure q03/q05/q10 live at sf=10: per cell, every chain-stage build edge - producer gone / advanced to named
  downstream / residual. Success = no covered stage-build edge still on `scan output accumulator`.

## Tests
- **Per widened edge:** streamed == materialized on a synthetic reoriented-inner-chain whose base mode is None but a
  stage build is eligible (digest, column order, schema, residual/filter);
- **No stranded streamed runtime:** a covered non-in-memory stage runtime is consumed by
  `probe_reoriented_inner_join_streaming`, not prepared and then rejected by the synchronous fallback path;
- **Shape fence:** a non-chain / non-eligible shape stays materialized (no over-match from relaxing base-mode);
- **memory-aware gate per edge:** sf=1/small-scale materialized (control);
- ScanRequest parity for every streamed edge;
- prior greens (q04/q06/q14/q19) + q08/q12 wall-moved states unchanged.

## Verification (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast            # REQUIRED (engine cut)
scripts/cargo-cgroup test -p toron-tpch --no-fail-fast       # REQUIRED
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q03 --sf 0.01   # strict pass@compare
# fixed-cap sf=10 re-measure q03/q05/q10: every chain-stage build edge - gone / advanced / residual;
#   persisted sf=10 correctness for greens; sf=1 control proves materialized path unchanged
git diff --check
#   decline diagnosed (base-mode/undecomposable/under-cap/consumer); gate widened shape-fenced; sf=1 no-regression;
#   q03/q05/q10 stage-build producers gone or residual named; ScanRequest parity; no cap bump; full gates; no qNN
```

## Results doc
`crates/toron/plans/impl/0390_general_inner_chain_scan_streaming_results.md`:
- the Phase-0 decline table per query/stage (base_scan_probe_mode, decompose, estimate vs cap, runtime, reason);
- the widened gate (base-mode relaxation for eligible stages), the consumer path used for non-in-memory runtimes, and
  the shape-fence;
- q03/q05/q10 live re-measure: per stage-build edge gone / advanced / residual; cycle broken or residual named;
- probe scan: streamed (if proven) or named residual;
- sf=1 materialized-path control; ScanRequest parity; persisted correctness for greens;
- confirmation no cap bump, full gates green, no qNN, prior greens + q08/q12/q19 unchanged.

## Risk / blast radius
- **Base-mode relaxation over-matches** -> shape-fence test (non-chain/non-eligible stays materialized).
- **Undecomposable/consumer-mismatch edge** -> named residual.
- **Probe pulled in prematurely** -> gated behind Phase-0 proof.
- **Small-scale regression** -> memory-aware gate + sf=1 control.
- **Regression** -> `--no-fail-fast` + `toron-tpch`.

## Seal
`impl: adr 390 general inner chain scan streaming`. Do not commit wip drafts or large `.perf.data`.
