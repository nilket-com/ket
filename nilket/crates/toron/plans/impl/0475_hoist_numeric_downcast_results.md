# ADR 475 implementation receipt

The hoist landed and beat its own prediction: per-row accumulate cost fell from
25.2 ns to **0.9 ns** (decimal) and 10.8 ns to **0.8 ns** (Int64). m02 went 1,588.6 ms
→ 430.6 ms (**3.69×**), moving from 19.8× the best reference to 5.3×.

## The change

`NumericArrayReader` (`aggregate.rs`) resolves an array's concrete type **once** and
exposes a typed `value(row)`; the seven per-row call sites hoist it out of the row
loop. `numeric_value` is retained for cold callers and shares the same type set, so
the two cannot drift.

Invariants preserved by construction: the null check moved into the reader (same
`is_null(row)` → `None` contract, so all-null/empty results are unchanged);
Decimal128 still enters as raw unscaled `i128` with finalize re-applying
(precision, scale) per ADR 263; overflow paths untouched; grouped and global take
the same reader. **Error semantics needed care**: grouped loops carry mixed arrays
where non-numeric inputs are legitimate (Count/Min/Max over other types), so eager
reader construction would have errored where the old code succeeded — non-numeric
inputs resolve to `None` and fall back to the original per-row path, preserving the
exact message and the exact row at which it fires.

## Measured (sf=8, ADR 473 conditions, references on one thread)

| primitive | before | after | speedup | Toron/best after |
|---|---:|---:|---:|---:|
| m02 decimal decode+sum | 1,588.6 | **430.6** | 3.69× | 5.3× |
| m09 Int64 decode+sum | 942.9 | **473.1** | 1.99× | 4.9× |
| m10 two-col mul+sum | 1,816.9 | **648.0** | 2.80× | 3.4× (beats Polars) |
| m07 decimal decode only | 380.9 | 386.7 | 1.00× | 32.2× |
| m08 Int64 decode only | 422.4 | 436.2 | 0.97× | 35.2× |
| m04 group by 3 | 4,046.9 | 3,936.6 | 1.03× | 21.7× |
| m05 group by 12M | 12,012.3 | 12,006.3 | 1.00× | 5.0× |

Accumulate per row: decimal **25.2 → 0.9 ns**, Int64 **10.8 → 0.8 ns** — now below
the 3.8 ns multiply kernel, which is the correct ordering for an add.

## What the unchanged rows establish

m07/m08 are decode-only and flat: the fix landed on the accumulate layer and
nowhere else — the 474 diagnosis was precise. **m04/m05 are also flat**, and the
hoist did reach the grouped paths, so grouped aggregation's cost is *not* type
dispatch. The remaining candidate is visible in the same loops: every row builds a
fresh `Vec<GroupValue>` key (`group_arrays.iter().map(group_value).collect()`) and
hashes it through `entry_or_init` — a per-row allocation plus hash over 48M rows.
That is the next cut's hypothesis, and m04 (21.7×, three groups, no spill, no join)
is now the cleanest target on the board.

## Gates

Aggregate unit tests 39/39; full `toron --no-fail-fast` suite green; architecture
2,093/0; **armed 22-query parquet correctness sweep 22/22** — the decisive gate,
since this cut touches every aggregate on the board. No board wall claim is made
here; the ladder's arithmetic is the evidence.
