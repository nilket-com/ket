# ADR 297 - Results: tpch.q08 reachability

## Final row

q08 is now in the reachability gauge:

```text
q08 dev=pass@execute
q08 strict=pass@execute
```

Live rows at `sf=0.01`:

```text
tpch_reachability query=q08 sf=0.01 mode=dev    phase=execute status=pass reason=none elapsed_ms=1081.4 digest=ArrowIpcDigest([93, 248, 152, 29, 181, 252, 145, 172, 195, 153, 198, 200, 234, 56, 218, 136, 35, 77, 188, 175, 231, 210, 133, 234, 128, 54, 232, 230, 173, 40, 228, 127])
tpch_reachability query=q08 sf=0.01 mode=strict phase=execute status=pass reason=none elapsed_ms=1100.8 digest=ArrowIpcDigest([13, 235, 184, 192, 201, 20, 132, 252, 193, 115, 231, 254, 121, 113, 110, 224, 81, 212, 20, 9, 5, 67, 152, 95, 202, 34, 180, 64, 242, 13, 232, 144])
```

A temporary row-count probe measured 2 output rows in both modes at `sf=0.01`, so the pass is not an empty
result.

## What landed

- `Q8_DEV_SQL` uses the tpchgen q08 shape with `BRAZIL`, `AMERICA`, and `ECONOMY ANODIZED STEEL` inlined.
  `STRICT_Q8_SQL` is the same text.
- `q08_tables(strict, sf)` reuses the q05 table family and appends `part`.
- `nation` is registered once. The SQL aliases `nation n1` and `nation n2` remain aliases over that one physical
  harness table; no `nation_1` / `nation_2` fixture was added.
- `run_dev_q08`, `run_strict_q08`, CLI routing, status inventory, and a 64 MB-stack harness test were added.

## Measured behavior

q08 exercises the largest derived-body join in the gauge so far: eight logical relation factors, seven physical
harness tables, a `nation` self-join by SQL alias, `EXTRACT(year ...)`, `CASE`, Decimal arithmetic, and an outer
ratio aggregate.

The query completed at the default `sf=0.01` scale in both modes, which is the practical keyed-join tripwire. An
unrecognized cartesian in this shape would be intractable at that scale.

## Board

Reachability after this cut:

```text
q01/q02/q03/q04/q05/q06/q07/q08/q10/q12/q14/q17/q20  dev=pass@execute strict=pass@execute
q09/q11/q13/q15/q16/q18/q19/q21/q22                 not_run
```

Correctness is unchanged:

```text
q01/q02/q03/q05/q06/q10/q12/q14/q17/q20  strict=pass@compare
q04/q07/q08 and remaining queries        unchecked
```

## Forward

q08 is a reachability pass only. The later q08 correctness oracle must compare the market-share ratio directly
and should keep the non-empty output guard, because this cut proves reachability but not answer correctness.
