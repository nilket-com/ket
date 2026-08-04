# ADR 290 results — tpch.q12 reachability

## Measured row

q12 is now wired into the reachability harness and reaches execute at the default reachability scale:

```text
q12 dev    = pass@execute
q12 strict = pass@execute
```

Live CLI measurement at `sf=0.01`:

```text
tpch_reachability query=q12 sf=0.01 mode=dev phase=execute status=pass reason=none
digest=ArrowIpcDigest([204, 125, 215, 116, 156, 249, 89, 196, 240, 23, 240, 87, 44, 235, 89, 135, 88, 78, 94, 45, 232, 172, 91, 177, 77, 159, 214, 163, 239, 146, 124, 54])

tpch_reachability query=q12 sf=0.01 mode=strict phase=execute status=pass reason=none
digest=ArrowIpcDigest([204, 125, 215, 116, 156, 249, 89, 196, 240, 23, 240, 87, 44, 235, 89, 135, 88, 78, 94, 45, 232, 172, 91, 177, 77, 159, 214, 163, 239, 146, 124, 54])
```

The identical digest is expected: q12 references only shared key/date/string columns and integer CASE literals, not the mode-specific Decimal columns.

## Fixture delta

`l_shipmode` was appended to the dev and strict `lineitem` schemas and generators. No existing lineitem field was reordered.

`orders` already carried `o_orderkey` and `o_orderpriority`, and `lineitem` already carried `l_orderkey`, `l_shipdate`, `l_commitdate`, and `l_receiptdate`, so q12's only new data surface is the appended `l_shipmode` column.

## Interpretation

q12 verifies another already-built query surface rather than adding an engine feature:

- the `orders, lineitem` comma join is keyed at the default scale;
- string `IN ('MAIL', 'SHIP')` binds and executes;
- `SUM(CASE ... THEN 1 ELSE 0 END)` count-style aggregates execute;
- Date32 column-to-column inequalities execute.

The reach board moves from 10 to 11 pass@execute cells. Correctness remains unchanged; q12 is not answer-checked in this ADR.

## Gate

Executed gate:

```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- run --query q12 --mode dev --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- run --query q12 --mode strict --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- matrix
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
git diff --check
```

Results:

- `check -p toron-tpch --all-targets`: green.
- `test -p toron-tpch`: green, including `q12_executes_in_both_modes_at_default_scale`.
- `matrix`: q12 renders `dev=pass@execute strict=pass@execute`.
- `correctness-matrix`: q12 remains `strict=unchecked`; existing correctness cells remain pass.
