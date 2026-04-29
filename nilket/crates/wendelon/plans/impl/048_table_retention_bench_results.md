# ADR 048 Table Retention Bench Results

Bench target:

```bash
cargo bench --bench table_retention -- --quick
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

Results are environment-sensitive. A quick smoke run is enough for
code review; refresh this file with the full 100k / 1M matrix before
ADR 048 is declared shipped.

## 2026-04-29 Quick Smoke

Command:

```bash
cargo bench --bench table_retention -- --quick
```

Output:

```text
table_retention case=read-admission mode=latest rows=4096 iterations=2 admitted=8192 rejected=0 filtered_rows=6144 csb_direct=0 csb_zero=0 csb_row_filter=0 total_ns=72340 ns_per_op=8 ops_per_sec=113243019.08
table_retention case=read-admission mode=valid-as-of rows=4096 iterations=2 admitted=8192 rejected=0 filtered_rows=6144 csb_direct=0 csb_zero=0 csb_row_filter=0 total_ns=79112 ns_per_op=9 ops_per_sec=103549398.32
table_retention case=read-admission mode=expired-as-of rows=4096 iterations=2 admitted=2048 rejected=6144 filtered_rows=6144 csb_direct=0 csb_zero=0 csb_row_filter=0 total_ns=76695 ns_per_op=9 ops_per_sec=106812699.65
table_retention case=read-admission mode=multi-table-conjunction rows=4096 iterations=2 admitted=0 rejected=8192 filtered_rows=0 csb_direct=0 csb_zero=0 csb_row_filter=0 total_ns=32316 ns_per_op=0 ops_per_sec=1013986879.56
table_retention case=read-admission mode=csb-straddle-gate rows=4096 iterations=2 admitted=0 rejected=0 filtered_rows=0 csb_direct=4098 csb_zero=2048 csb_row_filter=2046 total_ns=79282 ns_per_op=9 ops_per_sec=103327363.08
table_retention case=pk-cluster-retention rows=4096 chains=4096 versions=12288 iterations=2 pruned_versions=8192 total_ns=7400 ns_per_version=0 versions_per_sec=3321081081.08
table_retention case=pair-gc-composition rows=4096 pairs=4096 iterations=2 pair_drops=5462 pruned_data=5462 pruned_tombstones=5462 total_ns=3096 ns_per_pair=0 pairs_per_sec=2645994832.04
table_retention case=l1-prune policy=single-fixed rows=4096 stored_rows=4096 density_percent=0 iterations=2 pruned_data=5464 pruned_tombstones=0 pair_drops=0 ambiguous_retained=0 reclaimed_bytes=874240 total_ns=793379 ns_per_row=96 rows_per_sec=10325456.06
table_retention case=visible-digest-reverse policy=single-fixed rows=4096 evicted_per_iter=2732 iterations=2 total_ns=42735470 ns_per_eviction=7821 evictions_per_sec=127856.32
table_retention case=visible-digest-add policy=single-fixed rows=4096 stored_rows=4096 iterations=2 total_ns=57820779 ns_per_row=7058 rows_per_sec=141679.17
table_retention case=l1-prune policy=disabled rows=4096 stored_rows=4096 density_percent=0 iterations=2 pruned_data=0 pruned_tombstones=0 pair_drops=0 ambiguous_retained=0 reclaimed_bytes=0 total_ns=777426 ns_per_row=94 rows_per_sec=10537337.32
table_retention case=l2-rewrite policy=disabled-retain-all rows=4096 stored_rows=4096 density_percent=0 iterations=2 retired=false frames=8192 encoded_bytes=13172736 survivors=8192 pruned_data=0 pruned_tombstones=0 ambiguous_retained=0 total_ns=44702324 ns_per_row=5456 rows_per_sec=183256.69
table_retention case=l2-rewrite policy=retire-all rows=4096 stored_rows=4096 density_percent=0 iterations=2 retired=true frames=0 encoded_bytes=0 survivors=0 pruned_data=8192 pruned_tombstones=0 ambiguous_retained=0 total_ns=800707 ns_per_row=97 rows_per_sec=10230958.39
table_retention case=l1-prune policy=ambiguous-retain rows=4096 stored_rows=4096 density_percent=0 iterations=2 pruned_data=0 pruned_tombstones=0 pair_drops=0 ambiguous_retained=8192 reclaimed_bytes=0 total_ns=6462 ns_per_row=0 rows_per_sec=1267718972.45
table_retention case=l2-rewrite policy=ambiguous-retain-all rows=4096 stored_rows=4096 density_percent=0 iterations=2 retired=false frames=8192 encoded_bytes=11599872 survivors=8192 pruned_data=0 pruned_tombstones=0 ambiguous_retained=8192 total_ns=39766121 ns_per_row=4854 rows_per_sec=206004.50
table_retention case=l1-prune policy=mixed rows=4096 stored_rows=4096 density_percent=0 iterations=2 pruned_data=2732 pruned_tombstones=0 pair_drops=0 ambiguous_retained=0 reclaimed_bytes=437120 total_ns=841707 ns_per_row=102 rows_per_sec=9732602.91
table_retention case=l2-rewrite policy=mixed-partial rows=4096 stored_rows=4096 density_percent=0 iterations=2 retired=false frames=5460 encoded_bytes=8779680 survivors=5460 pruned_data=2732 pruned_tombstones=0 ambiguous_retained=0 total_ns=28625790 ns_per_row=3494 rows_per_sec=286175.51
table_retention case=visible-digest-reverse policy=mixed rows=4096 evicted_per_iter=1366 iterations=2 total_ns=19269012 ns_per_eviction=7053 evictions_per_sec=141782.05
table_retention case=visible-digest-add policy=mixed rows=4096 stored_rows=4096 iterations=2 total_ns=57289188 ns_per_row=6993 rows_per_sec=142993.82
table_retention case=l1-prune policy=mixed rows=4096 stored_rows=4137 density_percent=1 iterations=2 pruned_data=2760 pruned_tombstones=82 pair_drops=82 ambiguous_retained=0 reclaimed_bytes=454720 total_ns=829714 ns_per_row=100 rows_per_sec=9972110.87
table_retention case=l2-rewrite policy=mixed-partial rows=4096 stored_rows=4137 density_percent=1 iterations=2 retired=false frames=5432 encoded_bytes=8734656 survivors=5432 pruned_data=2760 pruned_tombstones=82 ambiguous_retained=0 total_ns=28405625 ns_per_row=3433 rows_per_sec=291280.34
table_retention case=visible-digest-reverse policy=mixed rows=4096 evicted_per_iter=1421 iterations=2 total_ns=20042560 ns_per_eviction=7052 evictions_per_sec=141798.25
table_retention case=visible-digest-add policy=mixed rows=4096 stored_rows=4137 iterations=2 total_ns=58401748 ns_per_row=7058 rows_per_sec=141673.84
table_retention case=l1-prune policy=mixed rows=4096 stored_rows=4506 density_percent=10 iterations=2 pruned_data=2676 pruned_tombstones=246 pair_drops=246 ambiguous_retained=0 reclaimed_bytes=467520 total_ns=958154 ns_per_row=106 rows_per_sec=9405586.16
table_retention case=l2-rewrite policy=mixed-partial rows=4096 stored_rows=4506 density_percent=10 iterations=2 retired=false frames=5516 encoded_bytes=8869728 survivors=6090 pruned_data=2676 pruned_tombstones=246 ambiguous_retained=0 total_ns=32201973 ns_per_row=3573 rows_per_sec=279858.63
table_retention case=visible-digest-reverse policy=mixed rows=4096 evicted_per_iter=1461 iterations=2 total_ns=20457123 ns_per_eviction=7001 evictions_per_sec=142835.33
table_retention case=visible-digest-add policy=mixed rows=4096 stored_rows=4506 iterations=2 total_ns=62806264 ns_per_row=6969 rows_per_sec=143488.87
table_retention case=read-admission mode=latest rows=16384 iterations=2 admitted=32768 rejected=0 filtered_rows=24576 csb_direct=0 csb_zero=0 csb_row_filter=0 total_ns=162610 ns_per_op=4 ops_per_sec=201512822.09
table_retention case=read-admission mode=valid-as-of rows=16384 iterations=2 admitted=32768 rejected=0 filtered_rows=24576 csb_direct=0 csb_zero=0 csb_row_filter=0 total_ns=161265 ns_per_op=4 ops_per_sec=203193501.38
table_retention case=read-admission mode=expired-as-of rows=16384 iterations=2 admitted=8192 rejected=24576 filtered_rows=24576 csb_direct=0 csb_zero=0 csb_row_filter=0 total_ns=146508 ns_per_op=4 ops_per_sec=223660141.43
table_retention case=read-admission mode=multi-table-conjunction rows=16384 iterations=2 admitted=0 rejected=32768 filtered_rows=0 csb_direct=0 csb_zero=0 csb_row_filter=0 total_ns=74615 ns_per_op=0 ops_per_sec=1756644106.41
table_retention case=read-admission mode=csb-straddle-gate rows=16384 iterations=2 admitted=0 rejected=0 filtered_rows=0 csb_direct=16386 csb_zero=8192 csb_row_filter=8190 total_ns=171640 ns_per_op=5 ops_per_sec=190911209.51
table_retention case=pk-cluster-retention rows=16384 chains=16384 versions=49152 iterations=2 pruned_versions=32768 total_ns=35881 ns_per_version=0 versions_per_sec=2739722973.16
table_retention case=pair-gc-composition rows=16384 pairs=16384 iterations=2 pair_drops=21846 pruned_data=21846 pruned_tombstones=21846 total_ns=11902 ns_per_pair=0 pairs_per_sec=2753150730.97
table_retention case=l1-prune policy=single-fixed rows=16384 stored_rows=16384 density_percent=0 iterations=2 pruned_data=21848 pruned_tombstones=0 pair_drops=0 ambiguous_retained=0 reclaimed_bytes=3495680 total_ns=3933127 ns_per_row=120 rows_per_sec=8331284.50
table_retention case=visible-digest-reverse policy=single-fixed rows=16384 evicted_per_iter=10924 iterations=2 total_ns=155800560 ns_per_eviction=7131 evictions_per_sec=140230.56
table_retention case=visible-digest-add policy=single-fixed rows=16384 stored_rows=16384 iterations=2 total_ns=233356976 ns_per_row=7121 rows_per_sec=140420.06
table_retention case=l1-prune policy=disabled rows=16384 stored_rows=16384 density_percent=0 iterations=2 pruned_data=0 pruned_tombstones=0 pair_drops=0 ambiguous_retained=0 reclaimed_bytes=0 total_ns=3822465 ns_per_row=116 rows_per_sec=8572478.75
table_retention case=l2-rewrite policy=disabled-retain-all rows=16384 stored_rows=16384 density_percent=0 iterations=2 retired=false frames=32768 encoded_bytes=52690944 survivors=32768 pruned_data=0 pruned_tombstones=0 ambiguous_retained=0 total_ns=183855655 ns_per_row=5610 rows_per_sec=178226.77
table_retention case=l2-rewrite policy=retire-all rows=16384 stored_rows=16384 density_percent=0 iterations=2 retired=true frames=0 encoded_bytes=0 survivors=0 pruned_data=32768 pruned_tombstones=0 ambiguous_retained=0 total_ns=3769805 ns_per_row=115 rows_per_sec=8692226.79
table_retention case=l1-prune policy=ambiguous-retain rows=16384 stored_rows=16384 density_percent=0 iterations=2 pruned_data=0 pruned_tombstones=0 pair_drops=0 ambiguous_retained=32768 reclaimed_bytes=0 total_ns=35907 ns_per_row=1 rows_per_sec=912579719.83
table_retention case=l2-rewrite policy=ambiguous-retain-all rows=16384 stored_rows=16384 density_percent=0 iterations=2 retired=false frames=32768 encoded_bytes=46399488 survivors=32768 pruned_data=0 pruned_tombstones=0 ambiguous_retained=32768 total_ns=162836282 ns_per_row=4969 rows_per_sec=201232.79
table_retention case=l1-prune policy=mixed rows=16384 stored_rows=16384 density_percent=0 iterations=2 pruned_data=10924 pruned_tombstones=0 pair_drops=0 ambiguous_retained=0 reclaimed_bytes=1747840 total_ns=4014853 ns_per_row=122 rows_per_sec=8161693.59
table_retention case=l2-rewrite policy=mixed-partial rows=16384 stored_rows=16384 density_percent=0 iterations=2 retired=false frames=21844 encoded_bytes=35125152 survivors=21844 pruned_data=10924 pruned_tombstones=0 ambiguous_retained=0 total_ns=121620660 ns_per_row=3711 rows_per_sec=269427.91
table_retention case=visible-digest-reverse policy=mixed rows=16384 evicted_per_iter=5462 iterations=2 total_ns=78678226 ns_per_eviction=7202 evictions_per_sec=138844.00
table_retention case=visible-digest-add policy=mixed rows=16384 stored_rows=16384 iterations=2 total_ns=231800306 ns_per_row=7073 rows_per_sec=141363.06
table_retention case=l1-prune policy=mixed rows=16384 stored_rows=16548 density_percent=1 iterations=2 pruned_data=11034 pruned_tombstones=328 pair_drops=328 ambiguous_retained=0 reclaimed_bytes=1817920 total_ns=4080340 ns_per_row=123 rows_per_sec=8111088.78
table_retention case=l2-rewrite policy=mixed-partial rows=16384 stored_rows=16548 density_percent=1 iterations=2 retired=false frames=21734 encoded_bytes=34948272 survivors=21734 pruned_data=11034 pruned_tombstones=328 ambiguous_retained=0 total_ns=120761650 ns_per_row=3648 rows_per_sec=274060.52
table_retention case=visible-digest-reverse policy=mixed rows=16384 evicted_per_iter=5681 iterations=2 total_ns=80124286 ns_per_eviction=7051 evictions_per_sec=141804.70
table_retention case=visible-digest-add policy=mixed rows=16384 stored_rows=16548 iterations=2 total_ns=234953628 ns_per_row=7099 rows_per_sec=140861.84
table_retention case=l1-prune policy=mixed rows=16384 stored_rows=18024 density_percent=10 iterations=2 pruned_data=10704 pruned_tombstones=984 pair_drops=984 ambiguous_retained=0 reclaimed_bytes=1870080 total_ns=4464314 ns_per_row=123 rows_per_sec=8074700.84
table_retention case=l2-rewrite policy=mixed-partial rows=16384 stored_rows=18024 density_percent=10 iterations=2 retired=false frames=22064 encoded_bytes=35478912 survivors=24360 pruned_data=10704 pruned_tombstones=984 ambiguous_retained=0 total_ns=137018291 ns_per_row=3800 rows_per_sec=263088.96
table_retention case=visible-digest-reverse policy=mixed rows=16384 evicted_per_iter=5844 iterations=2 total_ns=82400818 ns_per_eviction=7050 evictions_per_sec=141843.25
table_retention case=visible-digest-add policy=mixed rows=16384 stored_rows=18024 iterations=2 total_ns=250824201 ns_per_row=6958 rows_per_sec=143718.19
```
