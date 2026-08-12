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
