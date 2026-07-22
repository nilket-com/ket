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

## Surplus attribution (why −51% exceeds the ~855 ms exchange bucket)

The recovery is larger than the exchange bucket alone; the armed-receipt reconcile (sf=8, decode is
upstream of the exchange and cancels) attributes every millisecond:

| bucket | control (`=0`) | bypass | Δ |
|---|---:|---:|---:|
| parquet decode | ~156 ms | 155.5 ms | ~0 |
| exchange (routing/index 759 + take 96) | 854.7 ms | 0 | −854.7 |
| fold | 456.2 ms | 437.7 ms | −18.5 |
| residual | ~497.7 ms | ~374.7 ms | −123.0 |
| execute | 1,964.6 ms | 967.9 ms | −996.7 |

The surplus over the ~855 ms exchange bucket is **not** a cheaper fold — `fold_micros` is flat
(456 → 438 ms), which refutes the "fold no longer re-hashes partitioned input" guess; the fold does
the same per-row work. It is the **residual** (~123 ms), driven by batch count: the bypass folds the
**46,865 source batches** directly instead of the exchange's **93,730 partition batches** (exact 2×),
halving the downstream per-batch framework/dispatch/projection overhead that lives in the residual,
not in the fold timer. Recovery = exchange 855 + residual 123 + fold 18 ≈ 997 ms.

The remaining ~375 ms bypass residual is the named non-exchange target carried forward from Phase 1.
