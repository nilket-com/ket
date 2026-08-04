# ADR 345 results - spill/stream the semi/anti existence-set build

## Summary

ADR 345 clears q21's ADR 344 semi existence-set wall and q21 now passes at `sf=1`.

The implementation is still shape-named in `crates/toron/src/exec.rs`: the core executor does not carry TPC-H query
number symbols. The TPC-H-specific interpretation lives here and in the `toron-tpch` seal commands.

## Implementation

- Replaced the matched semi/anti existence-set prepare path with a streamed-spilled build:
  - the existence side is decomposed as a bounded `Scan` plus optional `Filter`, `Project`, and `Exchange`;
  - scan batches are processed through those operators batch-by-batch;
  - each resulting existence batch is written to an encrypted spill page and dropped;
  - the path never calls `execute_bound` or `concat_or_empty` on the existence subtree.
- Added a semi/anti existence spill payload:
  - pages store bounded existence `RecordBatch` chunks as Arrow IPC;
  - pages are encrypted using the existing join-spill page machinery;
  - an index maps join key -> `(page, row)` references;
  - probing reads only candidate pages and applies the `match_filter` residual per candidate.
- Kept direct residual tests on an explicit in-memory runtime variant, while production prepare builds the spilled
  existence store.
- q21's first uncompressed run cleared the memory wall but hit the spill-byte cap by ~0.7 MB. The final cut uses
  compressed IPC pages for the existence spill, keeping the spill payload under the existing budget rather than raising
  the cap.

## Gate 1 - no full existence concat

Focused proof:

```text
scripts/cargo-cgroup test -p toron --lib streaming_semi_anti -- --nocapture
```

Result:

```text
streaming_semi_anti_existence_build_streams_scan_batches_without_concat ... ok
streaming_semi_anti_existence_set_spill_resident_state_is_budgeted ... ok
streaming_semi_anti_probe_applies_residual_and_preserves_left_rows ... ok
```

The no-concat test uses a two-batch existence scan and asserts the spilled builder writes two input batches. The old
`execute_bound + concat_or_empty` path would collapse that scan to one retained existence batch before building.

## Gate 2 - sequential anti outcome

q21 did not surface a LeftAnti wall after the LeftSemi wall cleared. It greened:

```text
scripts/cargo-cgroup run -p toron-tpch --release -- run --query q21 --mode strict --sf 1
```

Result:

```text
tpch_reachability query=q21 sf=1 mode=strict phase=execute status=pass reason=none elapsed_ms=21210.3 digest=ArrowIpcDigest([94, 189, 64, 185, 77, 200, 230, 42, 50, 106, 192, 70, 174, 205, 32, 191, 199, 227, 224, 135, 158, 223, 162, 207, 160, 204, 93, 64, 162, 97, 97, 11])
```

No anti-side byte table is claimed from structural evidence; the measured outcome is simply that the streamed-spilled
semi/anti path completes q21.

## Correctness

Default-scale q21 still passes the ADR 329 oracle:

```text
scripts/cargo-cgroup run -p toron-tpch --release -- correctness --query q21 --sf 0.01
```

Result:

```text
tpch_correctness query=q21 sf=0.01 mode=strict phase=compare status=pass reason=none elapsed_ms=792.7
```

Scale correctness also passes:

```text
scripts/cargo-cgroup run -p toron-tpch --release -- correctness --query q21 --sf 1
```

Result:

```text
tpch_correctness query=q21 sf=1 mode=strict phase=compare status=pass reason=none elapsed_ms=26593.8
```

The focused residual tests still cover both load-bearing branches:

- `streaming_semi_anti_probe_applies_residual_and_preserves_left_rows`
- `streaming_left_anti_probe_applies_late_residual_conjunct`

## Matrix

The sf=1 reachability matrix now renders q21 green:

```text
q21 dev=pass@execute strict=pass@execute
```

The default correctness matrix remains fully green:

```text
q01..q22 strict=pass@compare
```

## Seal

Commands run:

```text
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --lib
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron --lib streaming_semi_anti -- --nocapture
scripts/cargo-cgroup test -p toron --lib streaming_left_anti -- --nocapture
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch --release -- correctness --query q21 --sf 0.01
scripts/cargo-cgroup run -p toron-tpch --release -- run --query q21 --mode strict --sf 1
scripts/cargo-cgroup run -p toron-tpch --release -- correctness --query q21 --sf 1
scripts/cargo-cgroup run -p toron-tpch --release -- matrix --query q21 --sf 1
scripts/cargo-cgroup run -p toron-tpch --release -- correctness-matrix
```

Results:

- `toron --lib`: 1149 passed.
- `architecture_test`: 2057 passed.
- `toron --no-fail-fast`: passed.
- `toron-tpch`: all tests passed.
- q21 `sf=1`: `pass@execute` and `pass@compare`.

## Atlas update

ADR 345 moves q21 from ADR 344's `semi/anti existence-set build/spill accumulator` wall to green at `sf=1`.
Remaining strict sf=1 memory walls after this cut: q04, q12, q18, q20.
