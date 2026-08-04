# ADR 495 implementation receipt — bounded compact Int64 grouped COUNT

ADR 494 proved that m05's generic grouped fold held 3,377,742,008 bytes of
GroupTable state plus 384,000,000 bytes of finalize-sort scratch while the task
coordinate was 2 GiB. The table's growth guard was live but production supplied no
budget. Simply wiring the guard would turn a passing query into a resource failure.

## Decision

m05's admitted shape is narrower than generic grouping: one non-null Int64 group
column and one row-count aggregate. It now reuses the existing compact Int64 grouped
accumulator previously used by the sibling SUM path:

- contiguous `Vec<i64>` keys, `Vec<i128>` accumulators, and `Vec<u32>` buckets;
- guarded before every growth allocation against the task-memory-derived retained
  cap;
- no generic `Vec<Vec<AggState>>`, per-group state allocation, or exchange
  repartition materialization;
- the same checked Int64 output conversion and output-materialization guard.

Admission is structural and deliberately narrow: scan plus matching exchange,
exactly one non-null Int64 group key, exactly one `COUNT(*)` or non-null literal
`COUNT`, no filter/project, and a two-column non-null Int64 output. Nullable,
computed, multi-key, and other aggregate shapes remain generic.

The final policy smoke's accumulator receipt records 47,989,007 input rows,
12,000,000 groups, 536,870,912 accounted bytes, an 805,306,368-byte allocation
peak, and `cap=2147483648`. Both growth and the Int64 output-conversion peak use
that literal task cap; they do not inherit the join subsystem's larger retained
allowance.

`TORON_EXEC_PRIMITIVE_INT64_COUNT=0` restores the generic path for same-binary A/B;
the engine-neutral name avoids adding another gauge-named executor knob.

## Matched alternating A/B

Coordinate: sf=8, strict Parquet, 2 GiB task, 2 GiB spill reserve, release. The same
binary (`build_commit=44b0c9489754-dirty`, `binary_modified_unix=1784701691`)
alternated five control/candidate pairs.

| arm | execute (5 runs) | median | peak RSS median | spill |
|---|---|---:|---:|---:|
| generic grouped fold | 8,158.6 / 8,012.1 / 7,974.9 / 8,841.2 / 8,475.2 ms | 8,158.6 ms | 4,765,076 KiB | 0 B |
| compact guarded COUNT | 2,480.8 / 2,472.7 / 2,468.9 / 2,527.2 / 2,622.1 ms | 2,480.8 ms | 611,300 KiB | 0 B |

**Effect: -69.6% execute median and -87.2% peak RSS, with 5/5 paired wins.** All
ten rows have the same `6a231a3cb1e232c1` plan fingerprint, standing Arrow IPC
digest, 12,000,000-row output, binary identity, and zero spill. The result is now
comfortably inside the explicit task coordinate instead of passing through absent
policy wiring.

Raw rows are retained under timestamped
`target/tpch/adr495_ab_{control,candidate}_*/` directories.

## Rails

- Selection tests admit literal COUNT and COUNT ALL while rejecting a column COUNT.
- A parity test compares the compact and generic grouped COUNT digests across
  duplicates, distinct keys, and the full Int64 key domain.
- Existing compact-accumulator tests pin preallocation budget rejection, growth
  accounting, checked accumulator/output behavior, and SUM parity.
- The generic GroupTable guard remains unchanged. Shapes not proven compact do not
  receive an optimistic admission.

## Forward

The generic streaming GroupTable still lacks production budget wiring. ADR 495
closes the largest ladder cell through a provably fitting representation; it does
not claim generic high-cardinality grouping is bounded. The remaining contract work
is a spill or governed-admission strategy for non-compact shapes. The next ladder
refresh should precede another performance mechanism because m05 has moved from the
largest gap to roughly the one-thread reference range.
