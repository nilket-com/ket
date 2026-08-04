# ADR 476 implementation receipt

The probe-by-slice change landed and is correct, but it is a **minor win**: m04
3,936.6 → 3,650.2 ms (1.08×), m05 12,006.3 → 10,909.5 ms (1.10×). Per-row grouping
overhead fell 74.7 → 68.8 ns. The `Vec` allocation was ~6 ns of ~75 ns — the
hypothesis was right in kind and wrong in magnitude, and the receipt says so.

## The change

`entry_or_init` probes on `&[GroupValue]` and owns the key (`to_vec()`) only on the
insert path; both grouped loops reuse one key buffer per batch. Resident accounting
is byte-identical: the key is owned before `group_key_resident_bytes` is computed,
and `to_vec()` yields capacity == len exactly as `collect()` did, so the guard —
and therefore spill behaviour — sees the same numbers for the same insertions.

## Measured (sf=8, ADR 473 conditions)

| primitive | before | after | speedup | Toron/best |
|---|---:|---:|---:|---:|
| m04 group by 3 | 3,936.6 | 3,650.2 | 1.08× | 21.7× → 20.2× |
| m05 group by 12M | 12,006.3 | 10,909.5 | 1.10× | 5.0× → 4.5× |
| m02/m07/m09/m10 | — | — | 1.00× | unchanged (correctly untouched) |

## Where the remaining 69 ns/row is

`group_value` (`aggregate.rs:1800`) is the **same anti-pattern ADR 475 fixed one
function over**: up to twelve `as_any().downcast_ref::<T>()` tests per row, called
once per group column per row. For m04's `l_returnflag` (Utf8) the string variant
sits deep in that chain and additionally allocates a `String` per row —
`GroupValue::Utf8(String)` owns its bytes.

So grouped keys carry two costs the value path no longer has: per-row type
dispatch, and per-row string allocation for Utf8 keys. ADR 475's fix shape (resolve
the array type once per batch, typed extraction loop) applies directly to the
first; the second needs either a borrowed/interned key representation or a
fixed-width key encoding, which is a larger design question.

## Gates

group_table 7/7, aggregate 39/39, full engine suite green, architecture 2,093/0,
armed 22-query parquet correctness sweep 22/22.
