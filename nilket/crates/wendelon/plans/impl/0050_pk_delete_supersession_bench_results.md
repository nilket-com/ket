# ADR 050 PK Delete Supersession Bench Results

Command:

```sh
cargo bench --bench pk_delete_supersession
```

Benchmark target:

- `delete-classification`: latest-chain classification into deleted /
  already-deleted / not-found buckets.
- `tombstone-pack`: tombstone-only multi-row marker packing with the
  default record body compression policy used by the bench.
- `visible-digest-transitions`: visible-row digest add/subtract/no-op
  transition cost for delete and resurrection discipline.

Results:

| Case | Rows | Ops | Total ns | ns/op | Ops/sec |
| --- | ---: | ---: | ---: | ---: | ---: |
| delete-classification | 100,000 | 500,000 | 939,468 | 1 | 532,216,105.29 |
| tombstone-pack | 100,000 | 500,000 | 7,612,222,274 | 15,224 | 65,683.84 |
| visible-digest-transitions | 100,000 | 875,000 | 6,500,894,603 | 7,429 | 134,596.86 |
| delete-classification | 1,000,000 | 5,000,000 | 19,518,576 | 3 | 256,166,228.52 |
| tombstone-pack | 1,000,000 | 5,000,000 | 63,846,847,811 | 12,769 | 78,312.40 |
| visible-digest-transitions | 1,000,000 | 8,750,000 | 62,534,893,560 | 7,146 | 139,921.88 |

Notes:

- This is the initial ADR 050 implementation baseline.
- The benchmark isolates classification, marker packing, and digest
  transitions so future regressions identify the slow component.
- The full 100k / 1M matrix completed successfully.
