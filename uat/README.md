# Polariton UAT exhibits

Seed the four-quarter sales fixture, then open the twelve-panel bar showcase:

```sh
nilket toron sql --file uat/demo_sales.sql
cargo run -p polariton -- uat/test21.rn
```

`demo_sales.sql` is idempotent. Its quarterly values preserve the annual totals
used by the unfaceted panels while giving each region a distinct seasonal shape,
so the faceted panels can expose a quarter-assignment defect.

`test21.rn` is both a runnable product exhibit and a PRODUCT-0076 regression
input. Keep this repository copy authoritative; files in a neighboring `uat`
directory are local conveniences and are not build inputs.

Open the PRODUCT-0079 histogram exhibit after seeding the same sales relation:

```sh
cargo run -p polariton -- uat/test25.rn
```

`test25.rn` uses Toron's canonical alias-first `FLOOR` bucketing directly. Its
second panel deliberately removes one returned interval, making the sparse-bin
contract visible rather than asking Polariton to synthesize a zero-count bin.

Seed both relations before opening the PRODUCT-0082/0083 box-plot and
PRODUCT-0085 axis-title exhibit:

```sh
nilket toron sql --file uat/demo_sales.sql
nilket toron sql --file uat/box_probe.sql
cargo run -p polariton -- uat/test26.rn
```

`box_probe.sql` is idempotent and retains duplicate observations deliberately.
Its five groups cover two-sided outliers, no outliers, coincident duplicate
outliers, a singleton, and a flat distribution.

Seed the PRODUCT-0092 heatmap relations, then open the sparse-cell exhibit:

```sh
nilket toron sql --file uat/heat_probe.sql
cargo run -p polariton -- uat/test27.rn
```

`heat_probe` is pre-aggregated to one row per observed cell, with one deliberate
cross-product gap and a signed domain. `heat_probe_raw` retains a duplicate
North/Q1 key; its companion view must refuse and direct the author to aggregate
in the relation.
