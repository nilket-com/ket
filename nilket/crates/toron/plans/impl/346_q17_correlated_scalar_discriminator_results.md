# ADR 346 results - q17 correlated-scalar discriminator

## Summary

ADR 346 lands the incremental verdict for q17: the measured sf=1 wall was toolkit-covered, and q17 now passes
`sf=1` strict reachability.

The core executor implementation is shape-named. `crates/toron/src/exec.rs` introduces a correlated-scalar
LeftOuter/global-aggregate streaming path and a spilled-capable filtered Join(0) visitor; it does not add q17 or TPC-H
query-number identifiers to core executor symbols.

## Wall-boundary table

| boundary | tier | source_of_measurement | bytes | toolkit_coverage | verdict |
| --- | --- | --- | --- | --- | --- |
| JoinReplayId(0) inner output feeding the preserved branch | measured first wall at sf=1 | Baseline `target/release/toron-tpch run --query q17 --mode strict --sf 1` | requested 1,651,834,429; available 1,073,741,824 | covered by spilled-capable filtered Join(0) streaming plus LeftOuter/global fold | covered; removed |
| Downstream q17 boundary after removing the first wall | reachable measured downstream | ADR 346 sf=1 run after the cut | none surfaced | covered by the new correlated-scalar LeftOuter streaming path | q17 green |
| LeftOuter recombine | structural-only predicted wall | plan shape from ADR 346 / prior probes; not a separate sf=1 wall after the cut | n/a | implemented as a shape-fenced streaming recombine with preserved-side null fill | not used to justify green from structural bytes |

The strict provenance rule held: only the first row has measured failure bytes. The LeftOuter recombine row remains
structural-only for bytes; q17 green is justified by the post-cut sf=1 execution result, not by predicted recombine
size.

## Implementation

- Added a shape-fenced path for:
  `Aggregate(global) <- Filter(EvaluateInToron) <- Join(LeftOuter)`, with the preserved side shaped as
  `Exchange <- Filter <- Join(0)`.
- Materialized only the scalar grouped side into a keyed AVG map, with a budgeted producer:
  `correlated scalar recombine build accumulator`.
- Extended the existing Join(0) filtered visitor so spilled build routes can also stream filtered output chunks instead
  of returning `None`.
- Repartitioned preserved-side filtered chunks incrementally, with a budgeted producer:
  `correlated scalar preserved-side exchange accumulator`.
- Probed the LeftOuter recombine chunk-by-chunk, preserving left rows and null-filling missing scalar matches, then
  folded the global aggregate without materializing the full recombine output.

## q17 before and after

Baseline, before ADR 346:

```text
scripts/cargo-cgroup run -p toron-tpch --release -- run --query q17 --mode strict --sf 1
```

Result:

```text
tpch_reachability query=q17 sf=1 mode=strict phase=execute status=fail reason=memory_budget_exhausted
diagnostic_message=producer=join output materialization requested=1651834429 available=1073741824
```

After ADR 346:

```text
scripts/cargo-cgroup run -p toron-tpch --release -- run --query q17 --mode strict --sf 1
```

Result:

```text
tpch_reachability query=q17 sf=1 mode=strict phase=execute status=pass reason=none elapsed_ms=14809.0 digest=ArrowIpcDigest([216, 14, 242, 105, 51, 195, 217, 142, 57, 149, 184, 200, 207, 100, 195, 115, 67, 98, 108, 22, 156, 89, 254, 229, 106, 252, 79, 191, 168, 49, 120, 59])
```

## Correctness

Focused executor tests:

```text
scripts/cargo-cgroup test -p toron --lib correlated_scalar_left_outer -- --nocapture
```

Result:

```text
correlated_scalar_left_outer_stream_matches_materialized_avg_recombine ... ok
correlated_scalar_left_outer_stream_removes_join0_materialization_wall ... ok
```

The q17 oracle is intentionally fixed to the first non-empty correctness scale:

```text
scripts/cargo-cgroup run -p toron-tpch --release -- correctness --query q17 --sf 0.02
```

Result:

```text
tpch_correctness query=q17 sf=0.02 mode=strict phase=compare status=pass reason=none elapsed_ms=798.6
```

No q17 `sf=1` correctness claim is made; the q17 correctness harness remains fixed-scale by design.

## Matrix

The sf=1 reachability matrix now renders q17 green:

```text
q17 dev=pass@execute strict=pass@execute
```

The default correctness matrix remains fully green:

```text
q01..q22 strict=pass@compare
```

The current strict sf=1 reachability board still has unrelated failures:

- q02: `fail(unrecognized@execute)`.
- q04, q12, q18, q20: `fail(memory_budget_exhausted@execute)`.

## Discriminator verdict

The q17 discriminator verdict is **incremental**:

- the measured first wall was the inner Join(0) output materialization, not an uncovered LeftOuter wall;
- the spilled-capable Join(0) visitor plus shape-fenced LeftOuter/global fold covered the measured wall;
- no reachable downstream q17 wall surfaced after the cut.

The remaining q12/q18/q04/q20 walls are therefore not proven new by q17. q17 shows the streaming toolkit can cover the
correlated-scalar / LeftOuter recombine shape when the measured wall is the preserved-branch output.

## Seal

Commands run:

```text
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --lib
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron --lib correlated_scalar_left_outer -- --nocapture
scripts/cargo-cgroup test -p toron --lib streaming_ -- --nocapture
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch --release -- correctness --query q17 --sf 0.02
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
scripts/cargo-cgroup run -p toron-tpch --release -- run --query q17 --mode strict --sf 1
scripts/cargo-cgroup run -p toron-tpch --release -- matrix --query q17 --sf 1
```

Results:

- `toron --lib`: 1151 passed.
- `architecture_test`: 2057 passed.
- `toron --no-fail-fast`: all test targets passed; one pre-existing storage test remains ignored.
- `toron-tpch`: all tests passed.
- Correctness matrix: 22/22 `pass@compare`.
- q17 `sf=1`: `pass@execute`.

## Atlas update

ADR 346 moves q17 from `memory_budget_exhausted@execute` at the `join output materialization` producer to
`pass@execute` at `sf=1`.

Remaining strict sf=1 memory walls after this cut: q04, q12, q18, and q20. q02 remains a separate
`unrecognized@execute` wall and is not part of this memory-family result.
