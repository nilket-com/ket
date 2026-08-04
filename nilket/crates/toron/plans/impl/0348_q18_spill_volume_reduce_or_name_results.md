# ADR 348 Results - q18 Spill Volume: Reduce or Name

## Verdict

**Reduced, no cap-bump.** q18 now passes at sf=1 under the unchanged 512 MiB per-query spill budget.

The reducible lever was **column projection** on the streamed preserved-side spill for a grouped aggregate over a
residual-free left-semi membership join. The spill no longer writes every preserved input column when the immediate
consumer only needs the grouped aggregate keys/arguments plus join keys.

## Phase 0 Measurement

Baseline q18 failed at sf=1 with:

```text
BudgetExhausted { dimension: SpillBytes, producer: "streamed inner join build spill writer",
requested: ByteCount(537631256), available: ByteCount(536870912) }
```

Temporary spill-page tracing showed:

- 513 IPC pages, page indexes 0..512.
- Full preserved-side schema: 30 columns.
- Final page pushed cumulative spill bytes to 537,631,256 B.
- Encryption/tag overhead was 16 B/page, about 8 KiB total, not enough to explain or fix the
  760,344 B (~743 KiB) excess.
- The aggregate needs only the preserved-side group/aggregate columns plus the semi join key; most preserved columns
  were dead after the membership filter.

Per-lever classification:

- **Column projection:** measured-reducible. The spill was writing all preserved columns; pruning to referenced
  columns removes the excess under the same budget.
- **Page/layout:** measured-not-enough. Per-page encryption overhead was only about 8 KiB total.
- **Dedup-before-spill:** not-applicable as a local lever. The preserved side is written before the membership probe,
  so early dedup would require a separate two-pass/side-flip mechanism.

## Implementation

Added a shape-named executor path for grouped aggregate over exchange over residual-free left-semi:

- Collect referenced columns from `group_expr`, aggregate arguments, exchange key, and semi join keys.
- Spill only those retained columns via `SpilledInnerJoinBuild::write_projected_batch_chunks`.
- Reconstruct sparse full-schema chunks for expression ordinal compatibility, with real arrays only for retained
  columns.
- Fold the grouped aggregate chunk-by-chunk; ordinary left-semi joins still return their full schema.

Core code stays de-branded: no `q18`/`Q18` symbols or comments in `crates/toron/src/exec.rs`.

## Scale Proof

```text
scripts/cargo-cgroup run -p toron-tpch --release -- run --query q18 --mode strict --sf 1
tpch_reachability query=q18 sf=1 mode=strict phase=execute status=pass reason=none elapsed_ms=13800.7
```

```text
scripts/cargo-cgroup run -p toron-tpch --release -- correctness --query q18 --sf 1
tpch_correctness query=q18 sf=1 mode=strict phase=compare status=pass reason=none elapsed_ms=17706.0
```

Default-scale oracle regression:

```text
scripts/cargo-cgroup run -p toron-tpch --release -- correctness --query q18 --sf 0.01
tpch_correctness query=q18 sf=0.01 mode=strict phase=compare status=pass reason=none elapsed_ms=777.2
```

## Tests

Focused regression tests:

```text
scripts/cargo-cgroup test -p toron --lib projected -- --nocapture
8 passed
```

Full checks run:

```text
scripts/cargo-cgroup test -p toron --lib
1157 passed

scripts/cargo-cgroup test -p toron --test architecture_test
2057 passed

scripts/cargo-cgroup test -p toron-tpch --lib
13 passed

scripts/cargo-cgroup test -p toron-tpch --test architecture
12 passed

scripts/cargo-cgroup test -p toron --no-fail-fast
passed
```

Correctness board:

```text
scripts/cargo-cgroup run -p toron-tpch --release -- correctness-matrix
q01..q22 strict=pass@compare
```

Other checks:

```text
scripts/cargo-cgroup check -p toron --lib
git diff --check
```

## Remaining Work

q18 is no longer a spill-volume wall at sf=1. The remaining sf=1 memory-family work is the q04/q20
data-proportional spill-writer index family, which remains ADR 349 scope.
