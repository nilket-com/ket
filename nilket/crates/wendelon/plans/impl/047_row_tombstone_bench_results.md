# ADR 047 Row Tombstone Bench Results

Bench target:

```bash
cargo bench --bench row_tombstones -- --quick
```

The bench reports three isolated cold-path costs for each configured
tombstone density:

- `latest-as-of-classify`: latest-version visibility classification
  with and without tombstone tails.
- `tombstone-pack`: tombstone-only multi-row record packing.
- `visible-digest`: visible-domain `StateDigest` add/subtract cost.

Results are environment-sensitive. The quick run below is a smoke
check only; it does not satisfy the implementation plan's 100k / 1M
row promotion matrix or any percentage threshold. Refresh this file
with the full matrix before ADR 047 is declared shipped.

## 2026-04-28 Quick Smoke

Command:

```bash
cargo bench --bench row_tombstones -- --quick
```

Output:

```text
row_tombstones case=latest-as-of-classify density_percent=0 rows=4096 iterations=2 total_ns=7755 ns_per_row=0 rows_per_sec=1056350741.46
row_tombstones case=tombstone-pack density_percent=0 rows=4096 tombstones=0 skipped=true
row_tombstones case=visible-digest density_percent=0 rows=4096 logical_ops=8192 total_ns=57195599 ns_per_op=6981 ops_per_sec=143227.80
row_tombstones case=latest-as-of-classify density_percent=1 rows=4096 iterations=2 total_ns=4081 ns_per_row=0 rows_per_sec=2007351139.43
row_tombstones case=tombstone-pack density_percent=1 rows=4096 tombstones_per_iter=41 records=82 total_ns=360760 ns_per_tombstone=4399 tombstones_per_sec=227297.93
row_tombstones case=visible-digest density_percent=1 rows=4096 logical_ops=8274 total_ns=59174720 ns_per_op=7151 ops_per_sec=139823.22
row_tombstones case=latest-as-of-classify density_percent=10 rows=4096 iterations=2 total_ns=3998 ns_per_row=0 rows_per_sec=2049024512.26
row_tombstones case=tombstone-pack density_percent=10 rows=4096 tombstones_per_iter=410 records=820 total_ns=3730709 ns_per_tombstone=4549 tombstones_per_sec=219797.36
row_tombstones case=visible-digest density_percent=10 rows=4096 logical_ops=9012 total_ns=63816111 ns_per_op=7081 ops_per_sec=141218.26
```
