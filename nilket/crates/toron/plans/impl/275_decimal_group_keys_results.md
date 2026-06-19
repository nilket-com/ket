# ADR 275 — Results: Decimal group-key support (strict q10 flips green)

## Measured row
```
q10 strict:  blocked(unsupported_decimal_group_key@execute)  →  pass@execute
Reachability matrix (all five reach cells now dev + strict pass@execute):
  q01/q03/q06/q10/q14  dev=pass@execute  strict=pass@execute
q06 strict=pass@compare unchanged
```

The engine cut ADR 274's measured wall named. Verified live:
```
tpch_reachability query=q10 sf=0.0001 mode=strict phase=execute status=pass
```
dev q10's digest is unchanged; strict q10 now produces its own (Decimal128-typed) digest. q10 groups by
`c_acctbal`, a `Decimal128(15,2)` column — the executor now partitions and groups it.

## What was built (executor-only, `crates/toron/src/exec.rs`)
Four additive changes (the wall + everything behind it; q10 already bound/planned):
- **`encode_partition_key_value`** — a `DataType::Decimal128(_, _)` arm: tag `17` (the next unused after the
  Timestamp units 14–16; my iter-0 "13" was wrong — `13`=Date32) + the raw `i128` little-endian. One `(p,s)`
  per column, so the raw `i128` partitions exactly.
- **`GroupValue::Decimal128(i128)`** — exact grouping on the raw `i128` (the enum derives `Ord`/`Hash`/`Eq`),
  no float-bit canonicalization.
- **`group_value`** — a `Decimal128Array` downcast branch → `GroupValue::Decimal128`.
- **`group_column`** — a `DataType::Decimal128(p, s)` arm rebuilding a `Decimal128Array` with
  `with_precision_and_scale(p, s)` (the exact `(p,s)` carried from the column `DataType`).

Plus the `ir.rs` comment retarget: the `scalar_literal_key` Decimal arm stays `Unsupported`, and its comment now
pins the *remaining* gap (Decimal GROUP BY keys work in the executor; Decimal **literal/stat** keys feed the
Wendelon-produced storage-fact decode — a separate cross-ring cut). No `ScalarLiteralKey::Decimal128` added.

## Shared-encoder side effect — measured (it cuts two ways)
`encode_partition_key_value` also backs generic hash-join key bytes, and there are **two** join paths with
**different** Decimal gates (the review caught that the first revision checked only one):
- **Explicit `JOIN ... ON` — now enabled by this cut.** `binder::join::validate_join_key_types` rejects only
  floats (`Float16/32/64`), **not `Decimal128`**, so an explicit Decimal equi-join binds; with the encoder's
  new Decimal arm it now **executes** end-to-end. Confirmed by a positive probe
  (`explicit_decimal_equi_join_executes`, 2 matching rows). This is a real, intended-adjacent capability gain —
  pinned by a test, but *not* Decimal optimizer/stat support.
- **Comma-join `FROM a, b WHERE a.k = b.k` — still gated.** The recognizer
  (`cross_equi_join_key_type_supported`) excludes `Decimal128`, so a Decimal equality there stays residual
  (not lifted to a join key). Unchanged by this cut.

## Tests
- `exec.rs::group_value_reads_decimal128_raw_i128` — `group_value` reads the raw `i128` (and NULL).
- `exec.rs::group_column_rebuilds_decimal128_with_exact_precision_scale_and_values` — the **exact `(p,s)`
  round-trip** (review Finding 2): asserts the rebuilt key column is `Decimal128(15,2)` with the exact raw
  values `12345` / `-67800` / NULL. This is the core `group_column` behavior the q10 digest alone can't pin.
- `exec.rs::exchange_partition_bucket_uses_stable_xxh3_64_encoding_for_decimal128` — pins the tag `17` + raw
  `i128` partition encoding (would fail if the tag/serialization drifts).
- `lib.rs::explicit_decimal_equi_join_executes` (toron-tpch, review Finding 1) — an explicit `JOIN ... ON
  dec = dec` over two Decimal-keyed tables executes (2 matching rows); records the wall if it ever regresses.
- `harness.rs::q10_executes_in_both_modes` (the flipped ADR 274 test; renamed) — strict q10 now `pass@execute`
  (+ digest), the end-to-end grouped-Decimal integration. Still on the explicit large-stack thread (the 4-table
  future exceeds libtest's default test stack — ADR 274).
- `harness.rs::decimal_group_key_error_classifies_precisely` — kept (the recognized-blocker classifier stays
  correct for the historical/forward record).

## Gate (engine cut — full `-p toron`)
fmt clean (both crates) · `check -p toron --all-targets` clean · `test -p toron --lib` 1108 (incl. the new
Decimal partition test) · **full `architecture_test` 2053** (after fmt) · `test -p toron --no-fail-fast`
**all green, 0 failed** across every binary · `test -p toron-tpch` green (harness 15, incl. the flipped q10) ·
live `matrix` shows q10 strict `pass@execute`, `correctness-matrix` still q06 `pass@compare` · `git diff
--check` clean. Additive only — no existing digest moved.

## Forward arc
With strict q10 green, the cadence points to the **q01 correctness oracle** (grouped output — the first
multi-row answer-check) before q05 (6-table). Decimal-key coverage after this cut: GROUP BY / partition keys ✓,
explicit `JOIN ... ON` keys ✓ (the measured side effect). Still deferred: the **literal/stat key**
(`ScalarLiteralKey`, the Wendelon-produced optimizer-fact path) and **comma-join** Decimal keys
(`cross_equi_join_key_type_supported`), each surfacing when a query measures it.
