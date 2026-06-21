# ADR 301 Results — tpch.q09 reachability

## Measured row

At `sf=0.01`:

```text
q09 dev    = pass@execute
q09 strict = blocked(unsupported_decimal_arithmetic@bind)
```

Dev executed at the default reachability scale with digest:

```text
ArrowIpcDigest([224, 98, 56, 122, 227, 78, 227, 70, 125, 98, 148, 242, 139, 204, 75, 154, 233, 251, 31, 217, 236, 118, 51, 71, 2, 60, 132, 58, 183, 169, 25, 232])
```

The dev result is non-empty at `sf=0.01`, pinned by `adr301_q09_probe::dev_q09_is_non_empty_at_default_scale`.

## Strict wall

Strict q09 reaches bind, then stops at the profit expression:

```text
TypeMismatch { message: "arithmetic operands must be Int32, Int64, Float32, or Float64, got Decimal128(32, 4) and Decimal128(31, 4)" }
```

The existing `unsupported_decimal_arithmetic` classifier is precise for this wall. No new telemetry reason was added.

## Interpretation

The dev pass proves the six-table derived comma join is tractable at the default scale, so q09 is not riding a hidden cartesian in the Float64 mode. It also exercises the `%green%` contains-LIKE path.

The strict red row names ADR 302: Decimal arithmetic operand alignment for `+`/`-` over Decimal operands with compatible scale but different precision. This is distinct from ADR 283/286 comparison alignment.

Existing reachability and correctness rows are unchanged by this harness-only cut.
