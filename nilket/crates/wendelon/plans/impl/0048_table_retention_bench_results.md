# ADR 048 Table Retention Bench Results

Bench target:

```bash
cargo bench --bench table_retention
```

The bench reports isolated cold-path costs for ADR 048:

- `read-admission`: latest, valid historical, expired historical,
  multi-table conjunction, and CSB straddle-gate admission checks.
- `pk-cluster-retention`: unchanged ADR 013 prefix pruning for
  PK-keyed version chains.
- `pair-gc-composition`: ADR 048 retention composed with ADR 047
  row-tombstone pair reclaim.
- `l1-prune`: table-aware append-only retention classification.
- `l2-rewrite`: mixed-policy survivor classification plus actual
  multi-row repacking.
- `visible-digest-reverse`: visible-domain add/subtract reversal for
  retained rows evicted by retention.
- `visible-digest-add`: visible-domain add cost for stored rows.

Results are environment-sensitive. The 2026-04-29 run below satisfies
the ADR 048 100k / 1M promotion matrix.

## 2026-04-29 Promotion Gate

Command:

```bash
cargo bench --bench table_retention
```

Output:

```text
table_retention case=read-admission mode=latest rows=100000 iterations=5 admitted=500000 rejected=0 filtered_rows=375000 csb_direct=0 csb_zero=0 csb_row_filter=0 total_ns=2772873 ns_per_op=5 ops_per_sec=180318391.79
table_retention case=read-admission mode=valid-as-of rows=100000 iterations=5 admitted=500000 rejected=0 filtered_rows=375000 csb_direct=0 csb_zero=0 csb_row_filter=0 total_ns=2859787 ns_per_op=5 ops_per_sec=174838195.99
table_retention case=read-admission mode=expired-as-of rows=100000 iterations=5 admitted=125000 rejected=375000 filtered_rows=375000 csb_direct=0 csb_zero=0 csb_row_filter=0 total_ns=2795757 ns_per_op=5 ops_per_sec=178842438.74
table_retention case=read-admission mode=multi-table-conjunction rows=100000 iterations=5 admitted=0 rejected=500000 filtered_rows=0 csb_direct=0 csb_zero=0 csb_row_filter=0 total_ns=1447296 ns_per_op=0 ops_per_sec=1381887326.43
table_retention case=read-admission mode=csb-straddle-gate rows=100000 iterations=5 admitted=0 rejected=0 filtered_rows=0 csb_direct=250005 csb_zero=125000 csb_row_filter=124995 total_ns=2863654 ns_per_op=5 ops_per_sec=174602099.28
table_retention case=pk-cluster-retention rows=100000 chains=100000 versions=300000 iterations=5 pruned_versions=500000 total_ns=828879 ns_per_version=0 versions_per_sec=1809673064.46
table_retention case=pair-gc-composition rows=100000 pairs=100000 iterations=5 pair_drops=333335 pruned_data=333335 pruned_tombstones=333335 total_ns=376655 ns_per_pair=0 pairs_per_sec=1327474744.79
table_retention case=l1-prune policy=single-fixed rows=100000 stored_rows=100000 density_percent=0 iterations=5 pruned_data=333340 pruned_tombstones=0 pair_drops=0 ambiguous_retained=0 reclaimed_bytes=53334400 total_ns=117698680 ns_per_row=235 rows_per_sec=4248136.00
table_retention case=visible-digest-reverse policy=single-fixed rows=100000 evicted_per_iter=66668 iterations=5 total_ns=2921016282 ns_per_eviction=8762 evictions_per_sec=114117.82
table_retention case=visible-digest-add policy=single-fixed rows=100000 stored_rows=100000 iterations=5 total_ns=4296051620 ns_per_row=8592 rows_per_sec=116385.94
table_retention case=l1-prune policy=disabled rows=100000 stored_rows=100000 density_percent=0 iterations=5 pruned_data=0 pruned_tombstones=0 pair_drops=0 ambiguous_retained=0 reclaimed_bytes=0 total_ns=112858287 ns_per_row=225 rows_per_sec=4430334.83
table_retention case=l2-rewrite policy=disabled-retain-all rows=100000 stored_rows=100000 density_percent=0 iterations=5 retired=false frames=500000 encoded_bytes=804000000 survivors=500000 pruned_data=0 pruned_tombstones=0 ambiguous_retained=0 total_ns=4517974115 ns_per_row=9035 rows_per_sec=110669.07
table_retention case=l2-rewrite policy=retire-all rows=100000 stored_rows=100000 density_percent=0 iterations=5 retired=true frames=0 encoded_bytes=0 survivors=0 pruned_data=500000 pruned_tombstones=0 ambiguous_retained=0 total_ns=118351055 ns_per_row=236 rows_per_sec=4224719.42
table_retention case=l1-prune policy=ambiguous-retain rows=100000 stored_rows=100000 density_percent=0 iterations=5 pruned_data=0 pruned_tombstones=0 pair_drops=0 ambiguous_retained=500000 reclaimed_bytes=0 total_ns=1464775 ns_per_row=2 rows_per_sec=341349354.00
table_retention case=l2-rewrite policy=ambiguous-retain-all rows=100000 stored_rows=100000 density_percent=0 iterations=5 retired=false frames=500000 encoded_bytes=708000000 survivors=500000 pruned_data=0 pruned_tombstones=0 ambiguous_retained=500000 total_ns=3808897217 ns_per_row=7617 rows_per_sec=131271.59
table_retention case=l1-prune policy=mixed rows=100000 stored_rows=100000 density_percent=0 iterations=5 pruned_data=166670 pruned_tombstones=0 pair_drops=0 ambiguous_retained=0 reclaimed_bytes=26667200 total_ns=118108412 ns_per_row=236 rows_per_sec=4233398.72
table_retention case=l2-rewrite policy=mixed-partial rows=100000 stored_rows=100000 density_percent=0 iterations=5 retired=false frames=333330 encoded_bytes=535994640 survivors=333330 pruned_data=166670 pruned_tombstones=0 ambiguous_retained=0 total_ns=2940326283 ns_per_row=5880 rows_per_sec=170049.16
table_retention case=visible-digest-reverse policy=mixed rows=100000 evicted_per_iter=33334 iterations=5 total_ns=1424095032 ns_per_eviction=8544 evictions_per_sec=117035.73
table_retention case=visible-digest-add policy=mixed rows=100000 stored_rows=100000 iterations=5 total_ns=4310370392 ns_per_row=8620 rows_per_sec=115999.31
table_retention case=l1-prune policy=mixed rows=100000 stored_rows=101000 density_percent=1 iterations=5 pruned_data=168335 pruned_tombstones=5000 pair_drops=5000 ambiguous_retained=0 reclaimed_bytes=27733600 total_ns=118559759 ns_per_row=234 rows_per_sec=4259455.35
table_retention case=l2-rewrite policy=mixed-partial rows=100000 stored_rows=101000 density_percent=1 iterations=5 retired=false frames=331665 encoded_bytes=533317320 survivors=331665 pruned_data=168335 pruned_tombstones=5000 ambiguous_retained=0 total_ns=2913061639 ns_per_row=5768 rows_per_sec=173357.13
table_retention case=visible-digest-reverse policy=mixed rows=100000 evicted_per_iter=34667 iterations=5 total_ns=1464109303 ns_per_eviction=8446 evictions_per_sec=118389.39
table_retention case=visible-digest-add policy=mixed rows=100000 stored_rows=101000 iterations=5 total_ns=4292959153 ns_per_row=8500 rows_per_sec=117634.48
table_retention case=l1-prune policy=mixed rows=100000 stored_rows=110000 density_percent=10 iterations=5 pruned_data=163335 pruned_tombstones=15000 pair_drops=15000 ambiguous_retained=0 reclaimed_bytes=28533600 total_ns=117311458 ns_per_row=213 rows_per_sec=4688374.09
table_retention case=l2-rewrite policy=mixed-partial rows=100000 stored_rows=110000 density_percent=10 iterations=5 retired=false frames=336665 encoded_bytes=541357320 survivors=371665 pruned_data=163335 pruned_tombstones=15000 ambiguous_retained=0 total_ns=3322570727 ns_per_row=6041 rows_per_sec=165534.47
table_retention case=visible-digest-reverse policy=mixed rows=100000 evicted_per_iter=35667 iterations=5 total_ns=1529258930 ns_per_eviction=8575 evictions_per_sec=116615.31
table_retention case=visible-digest-add policy=mixed rows=100000 stored_rows=110000 iterations=5 total_ns=4792401320 ns_per_row=8713 rows_per_sec=114765.01
table_retention case=read-admission mode=latest rows=1000000 iterations=5 admitted=5000000 rejected=0 filtered_rows=3750000 csb_direct=0 csb_zero=0 csb_row_filter=0 total_ns=26245485 ns_per_op=5 ops_per_sec=190508958.02
table_retention case=read-admission mode=valid-as-of rows=1000000 iterations=5 admitted=5000000 rejected=0 filtered_rows=3750000 csb_direct=0 csb_zero=0 csb_row_filter=0 total_ns=27986137 ns_per_op=5 ops_per_sec=178659884.36
table_retention case=read-admission mode=expired-as-of rows=1000000 iterations=5 admitted=1250000 rejected=3750000 filtered_rows=3750000 csb_direct=0 csb_zero=0 csb_row_filter=0 total_ns=27157785 ns_per_op=5 ops_per_sec=184109271.06
table_retention case=read-admission mode=multi-table-conjunction rows=1000000 iterations=5 admitted=0 rejected=5000000 filtered_rows=0 csb_direct=0 csb_zero=0 csb_row_filter=0 total_ns=13192797 ns_per_op=0 ops_per_sec=1515978757.20
table_retention case=read-admission mode=csb-straddle-gate rows=1000000 iterations=5 admitted=0 rejected=0 filtered_rows=0 csb_direct=2500005 csb_zero=1250000 csb_row_filter=1249995 total_ns=26969458 ns_per_op=5 ops_per_sec=185394901.15
table_retention case=pk-cluster-retention rows=1000000 chains=1000000 versions=3000000 iterations=5 pruned_versions=5000000 total_ns=19353017 ns_per_version=1 versions_per_sec=775072951.16
table_retention case=pair-gc-composition rows=1000000 pairs=1000000 iterations=5 pair_drops=3333335 pruned_data=3333335 pruned_tombstones=3333335 total_ns=9275263 ns_per_pair=1 pairs_per_sec=539068272.24
table_retention case=l1-prune policy=single-fixed rows=1000000 stored_rows=1000000 density_percent=0 iterations=5 pruned_data=3333340 pruned_tombstones=0 pair_drops=0 ambiguous_retained=0 reclaimed_bytes=533334400 total_ns=2427266909 ns_per_row=485 rows_per_sec=2059930.03
table_retention case=visible-digest-reverse policy=single-fixed rows=1000000 evicted_per_iter=666668 iterations=5 total_ns=28330463311 ns_per_eviction=8499 evictions_per_sec=117659.21
table_retention case=visible-digest-add policy=single-fixed rows=1000000 stored_rows=1000000 iterations=5 total_ns=42742924928 ns_per_row=8548 rows_per_sec=116978.42
table_retention case=l1-prune policy=disabled rows=1000000 stored_rows=1000000 density_percent=0 iterations=5 pruned_data=0 pruned_tombstones=0 pair_drops=0 ambiguous_retained=0 reclaimed_bytes=0 total_ns=2361495603 ns_per_row=472 rows_per_sec=2117302.27
table_retention case=l2-rewrite policy=disabled-retain-all rows=1000000 stored_rows=1000000 density_percent=0 iterations=5 retired=false frames=5000000 encoded_bytes=8040000000 survivors=5000000 pruned_data=0 pruned_tombstones=0 ambiguous_retained=0 total_ns=56304683559 ns_per_row=11260 rows_per_sec=88802.56
table_retention case=l2-rewrite policy=retire-all rows=1000000 stored_rows=1000000 density_percent=0 iterations=5 retired=true frames=0 encoded_bytes=0 survivors=0 pruned_data=5000000 pruned_tombstones=0 ambiguous_retained=0 total_ns=2307925369 ns_per_row=461 rows_per_sec=2166447.87
table_retention case=l1-prune policy=ambiguous-retain rows=1000000 stored_rows=1000000 density_percent=0 iterations=5 pruned_data=0 pruned_tombstones=0 pair_drops=0 ambiguous_retained=5000000 reclaimed_bytes=0 total_ns=23620567 ns_per_row=4 rows_per_sec=211679931.31
table_retention case=l2-rewrite policy=ambiguous-retain-all rows=1000000 stored_rows=1000000 density_percent=0 iterations=5 retired=false frames=5000000 encoded_bytes=7080000000 survivors=5000000 pruned_data=0 pruned_tombstones=0 ambiguous_retained=5000000 total_ns=46928086525 ns_per_row=9385 rows_per_sec=106546.00
table_retention case=l1-prune policy=mixed rows=1000000 stored_rows=1000000 density_percent=0 iterations=5 pruned_data=1666670 pruned_tombstones=0 pair_drops=0 ambiguous_retained=0 reclaimed_bytes=266667200 total_ns=2419518756 ns_per_row=483 rows_per_sec=2066526.65
table_retention case=l2-rewrite policy=mixed-partial rows=1000000 stored_rows=1000000 density_percent=0 iterations=5 retired=false frames=3333330 encoded_bytes=5359994640 survivors=3333330 pruned_data=1666670 pruned_tombstones=0 ambiguous_retained=0 total_ns=37212738021 ns_per_row=7442 rows_per_sec=134362.59
table_retention case=visible-digest-reverse policy=mixed rows=1000000 evicted_per_iter=333334 iterations=5 total_ns=14726763516 ns_per_eviction=8836 evictions_per_sec=113172.86
table_retention case=visible-digest-add policy=mixed rows=1000000 stored_rows=1000000 iterations=5 total_ns=42650331232 ns_per_row=8530 rows_per_sec=117232.38
table_retention case=l1-prune policy=mixed rows=1000000 stored_rows=1010000 density_percent=1 iterations=5 pruned_data=1683335 pruned_tombstones=50000 pair_drops=50000 ambiguous_retained=0 reclaimed_bytes=277333600 total_ns=2533926837 ns_per_row=501 rows_per_sec=1992954.15
table_retention case=l2-rewrite policy=mixed-partial rows=1000000 stored_rows=1010000 density_percent=1 iterations=5 retired=false frames=3316665 encoded_bytes=5333197320 survivors=3316665 pruned_data=1683335 pruned_tombstones=50000 ambiguous_retained=0 total_ns=37052400052 ns_per_row=7337 rows_per_sec=136293.47
table_retention case=visible-digest-reverse policy=mixed rows=1000000 evicted_per_iter=346667 iterations=5 total_ns=14632423228 ns_per_eviction=8441 evictions_per_sec=118458.51
table_retention case=visible-digest-add policy=mixed rows=1000000 stored_rows=1010000 iterations=5 total_ns=43388541277 ns_per_row=8591 rows_per_sec=116390.18
table_retention case=l1-prune policy=mixed rows=1000000 stored_rows=1100000 density_percent=10 iterations=5 pruned_data=1633335 pruned_tombstones=150000 pair_drops=150000 ambiguous_retained=0 reclaimed_bytes=285333600 total_ns=2577329622 ns_per_row=468 rows_per_sec=2133991.69
table_retention case=l2-rewrite policy=mixed-partial rows=1000000 stored_rows=1100000 density_percent=10 iterations=5 retired=false frames=3366665 encoded_bytes=5413597320 survivors=3716665 pruned_data=1633335 pruned_tombstones=150000 ambiguous_retained=0 total_ns=42337374141 ns_per_row=7697 rows_per_sec=129908.86
table_retention case=visible-digest-reverse policy=mixed rows=1000000 evicted_per_iter=356667 iterations=5 total_ns=15337501370 ns_per_eviction=8600 evictions_per_sec=116272.85
table_retention case=visible-digest-add policy=mixed rows=1000000 stored_rows=1100000 iterations=5 total_ns=47380763861 ns_per_row=8614 rows_per_sec=116080.86
```

Interpretation:

- Read-admission checks stay around 5 ns/op at both 100k and 1M.
- L1 table-retention classification remains above 1.9M rows/sec at
  the 1M mixed-policy cases.
- L2 rewrite packing is the dominant storage-shape cost, as expected;
  the slowest 1M retain-all case still clears roughly 88k rows/sec.
- Visible-domain digest add/reverse remains the dominant algebraic
  cost, roughly 113k-118k ops/sec at 1M.
