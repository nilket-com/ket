# ADR 387 implementation plan - scan-output streaming for the inner/join family

Pairs with `plans/adr/0387_scan_output_inner_join_family.md`.

This is an engine cut. It starts from q08 as the canary, diagnoses the exact `scan output accumulator` edge, and extends
the scan-streaming consumer path only for the measured q03/q05/q08/q10 family. No cap bump, no cached matrix evidence,
and no broad recognizer rewrite.

## Phase 0 - Diagnose the q08 edge

Before implementation, run q08 at sf=10 with enough trace/local instrumentation to answer:

1. Which scan output hits `scan output accumulator`?
2. Which consumer receives the materialized scan?
3. Did an existing streaming recognizer match and leave one edge materialized, or did it miss q08's plan shape?
4. Is the persisted Parquet plan shape different from the lower-scale/in-memory path?
5. Which `ScanRequest` fields are needed: projection, PBC predicate, runtime filter, row identity, profile hint, morsel,
   row limit, downstream cancellation?

Record the Phase 0 producer table in the results doc before landing the fix.

## D1 - q08 canary

Implement the smallest shape-fenced extension that removes q08's measured miss:

- reuse `bound_scan_request`;
- use direct `storage.scan` / `ScanStream`;
- stream the scan into the measured join/aggregate consumer;
- preserve output column order, residual/filter semantics, and projection.

If q08 moves to a downstream producer, stop and record the wall-move unless the same cut has an obvious measured sibling
inside the Phase 0 table. Do not chase a second wall inside ADR 387 without re-scoping the results.

## D2 - measured coverage

After q08, remeasure:

```text
q03 q05 q08 q10
```

Classify each as:

- green;
- advanced to named downstream producer;
- unchanged scan-output producer.

Only extend beyond q08 when Phase 0 shows the miss is shared or the additional shape is mechanically the same
ScanRequest/consumer path.

## Tests

Add focused tests for the new shape:

- streamed path equals materialized path on a small synthetic plan matching the q08 canary;
- column order and schema are identical;
- residual/filter semantics match;
- ScanRequest parity covers every bound scan field used by the direct path;
- if q03/q05/q10 share the shape, include a grouped/join variant that exercises that shared consumer.

Existing regression family must remain green:

```bash
scripts/cargo-cgroup test -p toron --no-fail-fast
```

## sf=10 Verification

Run fixed-cap persisted sf=10 rows:

```bash
for q in q03 q05 q08 q10; do
  systemd-run --user --scope --same-dir --collect --quiet \
    --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
    timeout --signal=INT 360s \
    target/release/toron-tpch perf-run-one \
      --query "$q" --mode strict --sf 10 --run-index 0 \
      --data-source parquet --data-dir target/tpch/data
done
```

For every pass row:

```bash
CARGO_CGROUP_BACKEND=affinity target/release/toron-tpch correctness \
  --query <query> --sf 10 --data-source parquet --data-dir target/tpch/data
```

Run q04/q06/q14/q19 as green controls if the implementation touches shared scan-streaming paths.

## Required Verification

```bash
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
git diff --check
rg -n "\\b[qQ][0-9]{1,2}\\b" crates/toron/src/exec crates/toron/src/exec.rs
```

## Results Doc

Create `crates/toron/plans/impl/0387_scan_output_inner_join_family_results.md` with:

- Phase 0 producer table;
- q08 miss class and exact scan/consumer edge;
- implementation summary;
- q03/q05/q08/q10 live remeasure table;
- correctness provenance for new pass rows;
- prior green controls;
- next producer ranking if cells advance rather than green;
- verification commands.

## Seal

Plan commit:

```text
plan: adr 387 scan output inner join family
```

Implementation commit:

```text
impl: adr 387 scan output inner join family
```
