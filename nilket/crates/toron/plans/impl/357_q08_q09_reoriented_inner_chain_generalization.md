# ADR 357 - Implementation plan: q08/q09 reoriented-inner-chain generalization

Pairs with `plans/adr/357_q08_q09_reoriented_inner_chain_generalization.md`. This is the engine behavior fix split out
of ADR 356: generalize the existing shape-named reoriented inner-chain streaming path so q08/q09 pass live sf=1 without
materializing the ~1.98 GB intermediate.

## Phase 0 - reproduce and fence

- Verify pre-fix live release q08/q09 at sf=1 fail at `join output materialization`, not bind or correctness.
- Capture the plan topologies:
  - grouped aggregate above `Project(Filter(Join))` for q08;
  - grouped aggregate above `Project(Join)` for q09;
  - chain replay ids differ from the original q07 shape;
  - the growing streamed side can be the original right child.
- Confirm q08/q09 are all-Inner / HashPartitioned along the matched chain. Do not widen non-inner shapes.

## D1 - topology-general extraction

- Replace fixed replay-id matching with inferred consecutive inner-chain matching down to base `Join(0)`.
- Track which child is the growing streamed chain at each join.
- Preserve lower filters found under `Exchange(Filter(Join))`.
- Accept grouped aggregate input shaped as either `Project(Filter(Join))` or `Project(Join)`.
- Keep shape fences strict enough that q21 semi/anti, q17 LeftOuter, and unrelated joins fall back to existing paths.

## D2 - chunk visitor and materialization

- Let the base `Join(0)` visitor stream either filtered or unfiltered chunks.
- Thread `Option<&BoundExprProgram>` for filters that are absent in the q09 shape.
- Extend reoriented materialization with a side-order flag so it can emit:
  - probe columns first when probe is the original left side;
  - build columns first when probe is the original right side.
- Preserve row order and schema exactly relative to the materialized path.

## D3 - tests

- Add a focused unit test proving swapped physical sides can restore original build-first schema.
- Keep existing q07 reoriented-chain parity/order tests passing.
- Keep existing q08/q09 scale probes passing.
- Add or preserve live-scale proof in the results doc:
  - release `run --query q08 --mode strict --sf 1`;
  - release `run --query q09 --mode strict --sf 1`;
  - release perf smoke/baseline containing q08/q09 pass rows.
- Run architecture guard to verify no qNN branding in core executor modules.

## D4 - perf baseline handoff

- Re-run `perf --sf 1 --mode strict --runs 7 --warmup 1 --query all`.
- If a non-q08/q09 row is unstable, follow ADR 356's noise policy: rerun the unstable query with the same 7 measured /
  1 warmup policy and record the replacement summary row instead of weakening the stability gate.
- Record the stable all-22 baseline as the ADR 357 outcome and as ADR 356's first usable baseline artifact.

## Seal gate

Run through `scripts/cargo-cgroup`:

```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch --release -- run --query q08 --mode strict --sf 1
scripts/cargo-cgroup run -p toron-tpch --release -- run --query q09 --mode strict --sf 1
scripts/cargo-cgroup run -p toron-tpch --release -- perf --sf 1 --mode strict --runs 7 --warmup 1 --query all --output-dir target/tpch/perf-sf1-baseline-adr357
git diff --check
```

## Results doc

Add `plans/impl/357_q08_q09_reoriented_inner_chain_generalization_results.md` with:
- the pre-fix q08/q09 live failure;
- the topology-generalization summary;
- the q08/q09 live release pass evidence;
- the all-22 stable perf baseline summary;
- the seal gate results.
