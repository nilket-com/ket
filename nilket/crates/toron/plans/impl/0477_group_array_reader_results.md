# ADR 477 implementation receipt

Group-column types now resolve once per batch. m04 fell 3,650.2 → **2,501.1 ms
(1.46×)**, from 20.2× to **13.2×** the best reference; per-row grouping overhead
fell 68.8 → 44.8 ns. Combined with ADR 476, m04 is 3,936.6 → 2,501.1 ms since the
grouped work began.

## The change

`GroupArrayReader` mirrors `group_value`'s variant set, conversions, float
canonicalisation, null contract, and error text, resolved once per group column per
batch; both grouped loops extract through it. `group_value` is retained for cold
callers and shares the construction so the two cannot drift.

**`GroupValue` ownership deliberately unchanged** (ADR 477 scope): a Utf8 key still
allocates an owned `String` per row. That is the separate ownership/layout decision,
now priced below.

## Measured (sf=8, ADR 473 conditions, references on one thread)

| primitive | 476 | 477 | speedup | Toron/best |
|---|---:|---:|---:|---:|
| m04 group by 3 (Utf8 key) | 3,650.2 | **2,501.1** | **1.46×** | 20.2× → **13.2×** |
| m05 group by 12M (Int64 key) | 10,909.5 | 10,671.2 | 1.02× | 4.5× → 4.3× |
| m02 / m07 / m09 / m10 | — | — | 1.00× | unchanged (correctly untouched) |

**The m04/m05 asymmetry is the mechanism's own signature.** m05 groups on
`l_orderkey` — Int64, third in the type chain, no allocation — so it had almost no
dispatch to remove. m04 groups on `l_returnflag` — Utf8, tenth in the chain — and
paid the most failed type tests per row. A broad regression would have moved both;
this moved the one the diagnosis predicted.

## The residual, priced

m04 retains **44.8 ns/row** of grouping overhead. With dispatch removed, the
remaining per-row costs are the `GroupValue::Utf8(String)` allocation, the key hash,
and the bucket probe. That number is the price tag the string-key decision
(borrowed keys, interning, or fixed-width key encoding) now has to beat — measured,
not assumed.

m05's regime differs: 12M groups mean hash-table growth and probe pressure dominate,
so its residual is not the same problem and should not be attacked with the same
fix.

## Gates

`adr477_reader_matches_group_value_for_every_supported_type` — full variant-set
parity including nulls, `-0.0` canonicalisation, and byte-identical unsupported-type
error text. Aggregate 40/40, group_table 7/7, architecture 2,093/0, full engine
suite green, **armed 22-query parquet correctness sweep 22/22**. Group-table
accounting and spill behaviour unchanged: identical `GroupValue`s with identical
capacities reach `entry_or_init`.
