# ADR 262 — Strict Q6 First Red Row: results

**Not a TPC-H result.** Strict mode here uses official physical *types* (`Decimal128(15,2)`) and the Q6
*shape* over synthetic `tpchgen` data with a Toron-authored proxy query and literal date bounds — no
official provenance, no answer-set comparison.

## The first measured strict red row

The gauge now reads a real strict blocker (no longer inventory state):

```
tpch_reachability query=q06 sf=0.001 mode=dev    phase=execute status=pass    reason=none                     digest=ArrowIpcDigest([233, 38, 184, …])
tpch_reachability query=q06 sf=0.001 mode=strict phase=bind    status=blocked reason=unsupported_decimal_scalar digest=none
```

`toron-tpch matrix` (regenerated from `target/tpch/reachability.jsonl`):

```
q06  dev=pass@execute   strict=blocked(unsupported_decimal_scalar@bind)
…    (q01–q05, q07–q22)  dev=not_run  strict=not_run
```

The Decimal128 strict path gets **past descriptor registration and planning** and blocks at **bind**:
the decimal literals (`0.05`, `0.07`, `24`) lower to `Float64` (Toron has no Decimal `ScalarValue`), so
`l_discount BETWEEN 0.05 AND 0.07` compares a `Decimal128(15,2)` column against `Float64` — a
type-unification failure, *before* the `SUM(l_extendedprice * l_discount)` decimal arithmetic.

## Second (probe/evidence) row: the SQL-surface gap

Retained as `decimal_probe.rs::decimal_ddl_blocks_at_plan`:

```
plan("CREATE TABLE lineitem (… DECIMAL(15,2) …)")  ->  phase=plan  reason=unsupported_decimal_ddl
```

So Toron has **two** distinct Decimal gaps at two phases: the DDL parse gap (plan) and the load-bearing
scalar/compute gap (bind). The bind gap is the one that informs the next ADR.

## What this measures → ADR 263

This *measured* gap names ADR 263 precisely: **a Decimal `ScalarValue`** so that (a) decimal literals
carry a decimal type and (b) Decimal-vs-Decimal comparison/arithmetic unify at bind. Not "accept
DECIMAL DDL" — the binder already tolerates `Decimal128` columns; it's the literal/scalar type system
that's the wall.

## Honesty invariant (unchanged, pinned)

`blocked` exits 0 (under `--allow-blocked`) only for an allowlisted reason
(`…`, `unsupported_decimal_ddl`, `unsupported_decimal_scalar`, …). An unrecognized error is `fail` even
under `--allow-blocked` (`unrecognized_error_is_fail_not_blocked`). `status` stays static capability
inventory (`q06 … strict=implemented`); only the JSONL-backed `matrix` reports the measured red row
(`adr262_status_is_inventory_not_last_result`).
