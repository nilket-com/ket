# ADR 302 Results — Decimal add/sub operand alignment

## Measured row

At `sf=0.01`:

```text
q09 dev    = pass@execute
q09 strict = pass@execute
```

Strict q09 moved from `blocked(unsupported_decimal_arithmetic@bind)` to `pass@execute`.

Live digests:

```text
dev    ArrowIpcDigest([224, 98, 56, 122, 227, 78, 227, 70, 125, 98, 148, 242, 139, 204, 75, 154, 233, 251, 31, 217, 236, 118, 51, 71, 2, 60, 132, 58, 183, 169, 25, 232])
strict ArrowIpcDigest([144, 215, 35, 203, 184, 98, 176, 26, 200, 117, 185, 90, 106, 173, 66, 161, 66, 139, 78, 60, 44, 69, 47, 32, 16, 60, 22, 40, 79, 106, 51, 57])
```

The reachability matrix now has q09 green in both modes. q09 correctness remains unchecked.

## Implementation

The binder now types Decimal `+`/`-` for mixed precision and scale operands with Arrow's add/sub rule:

```text
scale = max(s1, s2)
integer_digits = max(p1 - s1, p2 - s2)
precision = min(38, integer_digits + scale + 1)
```

Operands are not cast. The executor path is unchanged: Decimal add/sub still routes to Arrow `numeric::add` /
`numeric::sub`, and the probe pins that Arrow stamps the same output type the binder derives.

The q09 profit expression now binds as:

```text
Decimal128(32,4) - Decimal128(31,4) -> Decimal128(33,4)
```

The same rule subsumes ADR 266's same-`(p,s)` case:

```text
Decimal128(p,s) +/- Decimal128(p,s) -> Decimal128(min(38,p+1),s)
```

## Coverage

- `arrow_decimal128_add_sub_aligns_mixed_precision_and_scale` pins Arrow's output typing for the q09 shape and a
  different-scale add.
- `decimal_add_sub_aligns_mixed_precision_and_scale` binds and executes the q09-shaped subtraction through Toron,
  with raw Decimal128 values checked.
- `q09_executes_in_both_modes` confirms q09 dev and strict both reach execute at the default scale.
- The old ADR 301 strict wall probe is reconciled to the new invariant: strict q09 is non-empty at `sf=0.01`.

## Gate

Green after `fmt`:

```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- run --query q09 --mode dev --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- run --query q09 --mode strict --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- matrix
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
```
