# ADR 008 Bench Results

Date: 2026-05-14

Commands:

```bash
cargo bench -p toron --bench distributed_inner_equi_join --no-run
cargo bench -p toron --bench distributed_inner_equi_join -- --quick --distributed-inner-equi-join-only --strategy=hash
cargo bench -p toron --bench distributed_inner_equi_join -- --quick --kind=left-outer --kind=full-outer --kind=left-semi --kind=left-anti
```

All runs used `workers=2`, `build_rows=2048`, `probe_rows=8192`,
`probe_key_domain=2048`, `key=single`, `strategy=hash_partitioned`.

| kind       | wall_ms | task_wall_ms | result_rows | digest                                                           |
|------------|---------|--------------|-------------|------------------------------------------------------------------|
| inner      | 4.670   | 3.039        | 8192        | `0d96a56d48ff397ece0003ef5cb2a6d45782bb9f34221be544b15ad77ed4ce3b` |
| left_outer | 4.283   | 2.912        | 8192        | `ba73b3f3d5f5fe3ac9f01cae566f721b0646ecb82f6b05113972b89fc4f80ee2` |
| full_outer | 2.702   | 2.071        | 8192        | `04037920699c8ed7565ec4d3fb07d92a8f20664813841450e2659fc3b5881b1f` |
| left_semi  | 2.349   | 1.890        | 2048        | `328168297f62d287890677ea2a1cd9bb5cce826394d2c3a22b5be5532e6fada3` |
| left_anti  | 1.669   | 1.275        | 0           | `88ecfe6fa7999f7f32ba45446a4738402c3c67b19d7e31a241af7cfb9b6700b3` |

The repository does not contain a pinned post-ADR-006 numeric baseline
artifact. The warmed inner hash-partitioned quick row above is the local
comparison baseline for this implementation pass. All promoted quick rows
are below the ADR 008 pathological threshold of 3x the warmed inner wall
time.

`cargo bench --no-run` for the ADR benchmark target passed.
