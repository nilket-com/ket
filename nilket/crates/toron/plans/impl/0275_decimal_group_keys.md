# Implementation Plan — ADR 275: Decimal group-key support

## Summary
Add `Decimal128` to the executor's repartition + grouping path so strict q10's measured wall
(`unsupported_decimal_group_key@execute`) lifts and strict q10 reaches execute. Four additive `exec.rs`
changes + one `ir.rs` comment retarget; no new literal/stat-key support. Scope: `crates/toron/src/exec.rs`
(+ the comment in `crates/toron/src/ir.rs`), and the ADR 274 harness test flips.

## Phase 0 — verify the board
`run --query q10 --mode strict` → `blocked(unsupported_decimal_group_key@execute)`; dev → `pass@execute`;
q01/q03/q06/q14 `pass@execute`; q06 `pass@compare`.

## Phase 1 — the four executor arms (`crates/toron/src/exec.rs`)
- **`encode_partition_key_value`**: add `DataType::Decimal128(_, _)` → downcast `Decimal128Array`,
  `encoded.push(17)` (next unused tag; `13` is `Date32`, `14..16` are UTC timestamp units),
  `encoded.extend_from_slice(&values.value(row).to_le_bytes())` (raw `i128`, 16 bytes).
- **`GroupValue`**: add variant `Decimal128(i128)` (the derive `Ord`/`Hash`/`Eq` still holds — `i128`
  satisfies them). `grep` for any consuming `match` on `GroupValue` without a fallthrough; `group_column`'s has
  an `other =>` arm, and the `group_value` sites are constructors — confirm nothing else breaks.
- **`group_value`**: add a `Decimal128Array` downcast branch → `Ok(GroupValue::Decimal128(array.value(row)))`
  (NULL handled by the leading `array.is_null(row)` check).
- **`group_column`**: add `DataType::Decimal128(precision, scale)` → collect the `GroupValue::Decimal128`
  (and `Null`) into a `Decimal128Array`, then `.with_precision_and_scale(*precision, *scale)?`; non-matching
  variants fall to the existing `group_type_error`.

## Phase 2 — retarget the stale comment (`crates/toron/src/ir.rs`)
The comment by the `ScalarValue::Decimal128 => Unsupported` arm (`"Decimal in GROUP BY / join keys is out of the
Q6 slice — keep it unsupported"`) is now wrong for GROUP BY keys. Retarget it: Decimal **GROUP BY keys** are
supported in the executor (ADR 275); a Decimal **literal/stat key** stays `Unsupported` here because it feeds
the Wendelon-produced storage-fact decode (a separate cross-ring cut). Do **not** add
`ScalarLiteralKey::Decimal128`.

## Phase 3 — measure + flip the discriminating test
- Re-run `run --query q10 --mode strict --allow-blocked` → expect `pass@execute` (a digest). If a new wall
  appears behind grouping, classify *that* (don't force the predicted pass).
- Update `tests/harness.rs::q10_dev_executes_and_strict_blocks_at_decimal_group_key`: strict now asserts
  `Status::Pass` at `Phase::Execute` (+ digest), not `blocked`. Rename to reflect the green row (e.g.
  `q10_executes_in_both_modes`, mirroring q03/q14). It still runs on the explicit large-stack thread (the
  4-table future overflows libtest's default test stack — same as ADR 274).
- Keep `decimal_group_key_error_classifies_precisely` (the recognized-blocker classifier stays correct for the
  historical/forward record).

## Phase 4 — exec-level regressions
- Add a partition-bucket unit test beside the existing stable-encoding tests: build a `Decimal128Array` with
  raw `12345` and `(15,2)`, compute expected bytes as `[17] + 12345_i128.to_le_bytes()`, and assert
  `partition_bucket(..., 17) == XxHash3_64::oneshot(&encoded) % 17`.
- Add a grouped-aggregate test over a `Decimal128(p,s)` key column: distinct Decimal keys → the correct
  distinct groups, per-group aggregate correct, and the rebuilt key column is `Decimal128(p,s)` with the
  **exact** raw values (would fail without the `group_value`/`group_column` arms). Cover a repeated key (two
  rows, same Decimal → one group) to exercise grouping equality.
- Probe the shared encoder side effect: `encode_partition_key_value` also backs generic hash-join key bytes.
  If a focused same-typed Decimal equi-join now passes, keep a positive regression; if it hits a distinct wall,
  record that wall in the results doc without expanding this cut.

## Verification gate (engine cut — full `-p toron`, via scripts/cargo-cgroup, AFTER fmt)
```
scripts/cargo-cgroup fmt   -p toron -- --check
scripts/cargo-cgroup fmt   -p toron-tpch -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL guard suite, after fmt
scripts/cargo-cgroup test  -p toron --no-fail-fast
scripts/cargo-cgroup test  -p toron-tpch
scripts/cargo-cgroup run   -p toron-tpch -- run --query q10 --mode strict --allow-blocked   # -> pass@execute
scripts/cargo-cgroup run   -p toron-tpch -- matrix
scripts/cargo-cgroup run   -p toron-tpch -- correctness-matrix
git diff --check
```

## Seal
`impl: adr 275 decimal group keys`, `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`. Add
`crates/toron/plans/impl/0275_decimal_group_keys_results.md` with the flipped strict-q10 row + the four arms.

## Risk notes
- **Additive, not behavior-changing.** New arms only; existing-type grouping/partitioning + all current digests
  unchanged. If any q01/q03/q06/q14 digest moves, stop — something non-additive happened.
- **`(p,s)` must round-trip.** Pull `(precision, scale)` from the `group_column` `DataType` arg; don't hardcode.
- **Verify-first.** strict q10 `pass@execute` is the expected row (ORDER BY a Decimal aggregate already works —
  strict q03), but the impl measures it; classify any wall behind grouping honestly.
- **No cross-ring drift.** The only `ir.rs` change is the comment; `ScalarLiteralKey`/`catalog.rs` stat-key
  decode is untouched (that's the deferred, separate cut).
- **Run the FULL `architecture_test` after fmt** (engine cut); a new `exec.rs` variant/arm can shift
  fmt-sensitive guard scans even though `GroupValue` itself is not guard-referenced.
