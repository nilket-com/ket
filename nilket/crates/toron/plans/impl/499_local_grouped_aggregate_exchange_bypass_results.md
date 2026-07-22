# ADR 499 — local grouped-aggregate exchange bypass results

Sealed implementation: generic local streaming grouped aggregates now bypass the planner exchange
only when they serially fold into one shared table. `TORON_LOCAL_GROUPED_EXCHANGE_BYPASS=0` restores
the historical exchange path for a faithful control; the default is enabled. Distributed exchange
ownership and the ADR 495 primitive Int64 m05 path are unchanged.

## Acceptance

Release sf=8 m04, persisted parquet, 2 GiB task and spill reservations, receipt armed, two alternating
pairs:

| | control (`=0`) | bypass (default) |
|---|---:|---:|
| execute, pair 1 | 1,993.0 ms | 973.6 ms |
| execute, pair 2 | 2,007.9 ms | 983.8 ms |
| median | 2,000.4 ms | 978.7 ms |

The bypass reduces execute by **~51.1%**. Every run produced the same digest, three rows, and zero
spill. The control receipt recorded the generic local serial route and ~762–772 ms routing/index plus
~94–95 ms Arrow take. The candidate receipt recorded
`generic_local_serial_shared_table_bypassed`, `exchange_bypassed=true`, and zero exchange buckets.
The final post-refactor candidate reproduced 999.0 ms with the same receipt.

m05 remained on `primitive_int64_count`, with `exchange_micros=0`, its established digest, 12M output
rows, and zero spill.

## Gates

- Focused default/control parser test passed.
- Full `cargo test -p toron --test architecture_test --quiet`: 2093 passed.
- The earlier Phase 1 receipt remains the attribution baseline; the named non-exchange residual is a
  separate follow-up target.
