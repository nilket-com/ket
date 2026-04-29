# ADR 047 Row Tombstone Bench Results

Bench target:

```bash
cargo bench --bench row_tombstones -- --rows 100000
cargo bench --bench row_tombstones -- --rows 1000000
```

The bench reports three isolated cold-path costs for each configured
tombstone density:

- `latest-as-of-classify`: latest-version visibility classification
  with and without tombstone tails.
- `tombstone-pack`: tombstone-only multi-row record packing.
- `visible-digest`: visible-domain `StateDigest` add/subtract cost.

Results are environment-sensitive. The 2026-04-29 run below satisfies
the ADR 047 100k / 1M promotion matrix.

## 2026-04-29 Promotion Gate

Commands:

```bash
cargo bench --bench row_tombstones -- --rows 100000
cargo bench --bench row_tombstones -- --rows 1000000
```

Output:

```text
row_tombstones case=latest-as-of-classify density_percent=0 rows=100000 iterations=5 total_ns=890750 ns_per_row=1 rows_per_sec=561324726.35
row_tombstones case=tombstone-pack density_percent=0 rows=100000 tombstones=0 skipped=true
row_tombstones case=visible-digest density_percent=0 rows=100000 logical_ops=500000 total_ns=3759133263 ns_per_op=7518 ops_per_sec=133009.38
row_tombstones case=latest-as-of-classify density_percent=1 rows=100000 iterations=5 total_ns=636043 ns_per_row=1 rows_per_sec=786110373.04
row_tombstones case=tombstone-pack density_percent=1 rows=100000 tombstones_per_iter=1000 records=5000 total_ns=22360511 ns_per_tombstone=4472 tombstones_per_sec=223608.49
row_tombstones case=visible-digest density_percent=1 rows=100000 logical_ops=505000 total_ns=3742526462 ns_per_op=7410 ops_per_sec=134935.59
row_tombstones case=latest-as-of-classify density_percent=10 rows=100000 iterations=5 total_ns=652665 ns_per_row=1 rows_per_sec=766089801.05
row_tombstones case=tombstone-pack density_percent=10 rows=100000 tombstones_per_iter=10000 records=50000 total_ns=249925037 ns_per_tombstone=4998 tombstones_per_sec=200059.99
row_tombstones case=visible-digest density_percent=10 rows=100000 logical_ops=550000 total_ns=4123240263 ns_per_op=7496 ops_per_sec=133390.24
row_tombstones case=latest-as-of-classify density_percent=0 rows=1000000 iterations=5 total_ns=18026296 ns_per_row=3 rows_per_sec=277372567.28
row_tombstones case=tombstone-pack density_percent=0 rows=1000000 tombstones=0 skipped=true
row_tombstones case=visible-digest density_percent=0 rows=1000000 logical_ops=5000000 total_ns=38033132072 ns_per_op=7606 ops_per_sec=131464.32
row_tombstones case=latest-as-of-classify density_percent=1 rows=1000000 iterations=5 total_ns=18989517 ns_per_row=3 rows_per_sec=263303168.80
row_tombstones case=tombstone-pack density_percent=1 rows=1000000 tombstones_per_iter=10000 records=50000 total_ns=243606403 ns_per_tombstone=4872 tombstones_per_sec=205249.12
row_tombstones case=visible-digest density_percent=1 rows=1000000 logical_ops=5050000 total_ns=42917526576 ns_per_op=8498 ops_per_sec=117667.55
row_tombstones case=latest-as-of-classify density_percent=10 rows=1000000 iterations=5 total_ns=24646944 ns_per_row=4 rows_per_sec=202864906.90
row_tombstones case=tombstone-pack density_percent=10 rows=1000000 tombstones_per_iter=100000 records=500000 total_ns=3677019762 ns_per_tombstone=7354 tombstones_per_sec=135979.69
row_tombstones case=visible-digest density_percent=10 rows=1000000 logical_ops=5500000 total_ns=46773732623 ns_per_op=8504 ops_per_sec=117587.37
```

Interpretation:

- Latest-as-of classification remains sub-5 ns/row at 1M rows.
- Tombstone packing remains above 135k tombstones/sec at the 10%
  1M-row density case.
- Visible-domain digest cost is the dominant cold-path cost,
  roughly 117k-134k logical ops/sec across the full matrix.
