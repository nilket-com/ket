# ADR 275 — Decimal group-key support (turns strict q10 green)

## Status
Proposed.

## Context
ADR 274 brought tpch.q10 into the gauge and measured: dev `pass@execute`, strict
`blocked(unsupported_decimal_group_key@execute)`. The raw wall is `unsupported exchange partition key type:
Decimal128(15, 2)` — q10 groups by `c_acctbal`, a `Decimal128(15,2)` column in strict mode, and the executor's
repartition + grouping path has no `Decimal128` arm. dev (Float64) groups/partitions fine. q10 reached
*execute*, so bind and plan already handle the Decimal group key; the gap is purely in the executor. This is
the engine cut that closes ADR 274's named wall.

## Decision
Add `Decimal128` support to the executor's repartition + grouping path (`crates/toron/src/exec.rs`), in four
additive changes:

1. **`encode_partition_key_value`** — add a `DataType::Decimal128(_, _)` arm: downcast to `Decimal128Array`,
   push the next unused type tag (`17`; `13` is already `Date32`, and `14..16` are UTC timestamp units), then
   the raw `i128` value as little-endian bytes. All rows in a column share one `(p,s)`, so the raw `i128`
   partitions exactly (equal Decimal value → equal bytes → same partition).
2. **`GroupValue`** — add a `Decimal128(i128)` variant. The enum derives `Ord`/`Hash`/`Eq` and `i128` satisfies
   all three, so grouping is **exact** — no float-bit canonicalization (unlike `Float64(u64)`).
3. **`group_value`** — add a `Decimal128Array` branch returning `GroupValue::Decimal128` of the row's `i128`
   (the existing leading null-check covers NULL).
4. **`group_column`** — add a `DataType::Decimal128(p, s)` arm reconstructing a `Decimal128Array` with
   `with_precision_and_scale(p, s)` (the function already receives the column `DataType`, so the exact `(p,s)`
   is carried over, never dropped or hardcoded).

## Scope boundary — defer the Decimal *literal/stat* key arm (grounded)
This cut is **executor-only**. It does **not** add `ScalarLiteralKey::Decimal128` (`scalar_literal_key` keeps
classifying a Decimal *literal* as `Unsupported`). Two grounded reasons:

- **q10 doesn't exercise it.** q10 reached `execute`; `scalar_literal_key` is a bind/plan-time classification of
  a *literal* value, and q10's group key is a *column*, not a literal. It is not on q10's measured path.
- **It is a separate, cross-ring-adjacent surface.** `ScalarLiteralKey` is the storage-fact / optimizer-fact
  stat-key path (`scalar_literal_key_from_storage_fact`, the `BTreeMap<ScalarLiteralKey, u64>` value→row-count
  fact entries, `scalar_literal_key_matches_data_type`). Those facts are Wendelon-produced, so a Decimal
  stat-key arm touches the cross-ring fact decode — a genuinely separate cut (the interior/exterior + ADR 136
  cross-ring disciplines), not a free add-on.

Instead of implementing it, **retarget the now-stale comment** in `ir.rs` (`"Decimal in GROUP BY / join keys is
out of the Q6 slice — keep it unsupported"`): after this cut, Decimal **GROUP BY keys** work via the executor;
only Decimal **literal/stat keys** (`ScalarLiteralKey`, the optimizer-fact path) remain out. The comment should
pin that remaining gap accurately. (Council-converged on this executor-only scope.)

## Watch-items (operator) → guards
- **Exact `(p,s)` round-trip.** `group_column` rebuilds the `Decimal128Array` from the column `DataType`'s
  `(p,s)` — never hardcoded or dropped. A test reads back the exact group-key values + type.
- **Additive only.** The new arms are pure additions; existing-type grouping/partitioning behavior and all
  current result digests are unchanged (q01/q03/q06/q14).
- **Exact grouping.** Decimal groups on the raw `i128` (no float canonicalization); equal Decimal values group
  together and reconstruct to the same value.
- **No literal/stat-key change.** `ScalarLiteralKey::Decimal128` stays absent; the only `ir.rs` edit is the
  comment retarget.
- **Shared encoder side effect.** `encode_partition_key_value` also backs generic hash-join key bytes. If
  same-typed Decimal equi-joins start working through that shared encoder, pin a focused positive test or
  record the distinct wall; do not market it as Decimal optimizer/stat support.

## Acceptance
- strict q10 moves `blocked(unsupported_decimal_group_key@execute)` → `pass@execute`; dev q10 unchanged. The
  reachability matrix + the (flipped) ADR 274 harness test render it.
- Focused executor regressions: the partition bucket uses stable Decimal tag `17` + raw `i128` bytes, and
  GROUP BY a `Decimal128` column produces the correct distinct groups and an exact-value `Decimal128(p,s)`
  output column (would fail without the new arms).
- q01/q03/q06/q14 reachability + the q06 correctness cell unchanged.
- Engine cut → the full `-p toron` gate: `fmt --check`, `check -p toron --all-targets`, `test -p toron --lib`,
  the **full `architecture_test`** and `test -p toron --no-fail-fast` after fmt, then `-p toron-tpch`.

## Forward arc
With strict q10 green, the cadence points to the **q01 correctness oracle** (grouped output — the first
multi-row answer-check) before q05 (6-table). The deferred Decimal **literal/stat-key** gap (`ScalarLiteralKey`,
the Wendelon-produced optimizer-fact path) remains named for whenever a query measures it.

## Why toron-home
`crates/toron/src/exec.rs` — the executor's repartition + grouping path. A pure engine capability; no cross-ring
surface (and the cut is deliberately drawn to *avoid* the cross-ring stat-key path).
