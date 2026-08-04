# Implementation Plan — ADR 262: Strict Q6 First Red Row

## Summary

Extend `crates/toron-tpch` so strict Q6 *attempts* a Toron-authored `Decimal128(15,2)` fixture (official
physical types + Q6 shape, literal date bounds) through `plan → bind → execute` and records the first
real block, instead of returning `not_attempted`. Add a static `matrix` command that regenerates a
query × mode table from `target/tpch/reachability.jsonl`. No Decimal implementation, no answer
comparison, no official-text/data path, no `toron`-library change.

## Verify-first probe — DONE (grounded; harden the retained test)

The planning probe recorded in `crates/toron-tpch/tests/decimal_probe.rs` is the verify-first. Result:

- **DDL**: `plan("CREATE TABLE lineitem (… DECIMAL(15,2) …)")` → `Err Unsupported "unsupported CREATE
  TABLE column type: DECIMAL"` → **phase=plan**, reason `unsupported_decimal_ddl`.
- **Decimal128 compute** (load-bearing): `Decimal128(15,2)` descriptor **Ok**, register **Ok**, plan
  **Ok**, **bind `Err(TypeMismatch "comparison operands have different types: Decimal128(15, 2) and
  Float64")`** → **phase=bind**. Root cause: decimal literals lower to `Float64` (no Decimal
  `ScalarValue`), so `l_discount BETWEEN 0.05 AND 0.07` mismatches the `Decimal128` column at bind,
  before the `SUM` arithmetic.

Harden this file from `eprintln` probes to **assertions** (DDL → plan `Unsupported`; Decimal128 → bind
`TypeMismatch` containing `"Decimal128(15, 2) and Float64"`).

## Phase 1 — strict `Decimal128` fixture (`lib.rs`)

- `strict_lineitem_schema()`: `Decimal128(15,2)` for `l_quantity` / `l_extendedprice` / `l_discount`;
  `Date32` for `l_shipdate`.
- `generate_strict_lineitem(sf)`: drive `tpchgen::generators::LineItemGenerator`, building
  `Decimal128Array::from(…).with_precision_and_scale(15, 2)` directly — `TPCHDecimal::into_inner()`
  already returns the scale-2 raw `i64` (cast `i128`) for price/discount; `l_quantity as i128 * 100`
  for the integer quantity; `l_shipdate.to_unix_epoch()` for `Date32`. (No `Float64` conversion — that
  is the *dev* path.)
- `strict_lineitem_descriptor()`: `new_with_layout(… AppendOnly …
  RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()) …)` (proven in the probe).
- `STRICT_Q6_SQL`: the dev Q6 shape with decimal-looking numeric literals (`0.05`, `0.07`) that
  currently lower to `Float64`, plus **literal** date bounds (no `INTERVAL`) — isolating Decimal from
  the `Date32 ± Interval` gap while preserving the measured scalar/literal blocker.

## Phase 2 — strict run attempts the path (`lib.rs`)

`run_strict_q6` becomes `async` and mirrors `run_dev_q6`'s stage tracking over the strict fixture:
`generate → load → plan → bind → execute`, classifying the first stage `Err` via the allowlist.
Grounded expectation: `phase=bind status=blocked reason=unsupported_decimal_scalar`.

- Add `unsupported_decimal_scalar` (the true root cause — no Decimal `ScalarValue`) to
  `RECOGNIZED_BLOCKERS`.
- Tighten `reason_for_error`: map the bind comparison/type-unification shape
  (`"comparison operands"`, `"Decimal128"`, `"Float64"` / decimal type mismatch) to
  `unsupported_decimal_scalar`; leave `unsupported_decimal_arrow` for Decimal carrier/load gaps.
  The honesty rule still holds because unmatched messages classify as `fail`, not `blocked`.
- Keep the DDL gap as a **separate retained probe/evidence row**: a helper/test that plans
  `CREATE TABLE … DECIMAL` and records/asserts `phase=plan reason=unsupported_decimal_ddl`. Do not make
  the default strict Q6 run emit multiple attempts.

## Phase 3 — `matrix` artifact (`lib.rs` + `main.rs`)

- `matrix` command: read `target/tpch/reachability.jsonl`, parse each row (`serde_json`), print a
  query × mode table (`q06 dev=pass@execute  strict=blocked(unsupported_decimal_scalar@bind)`). Pure
  read/summarize — **no hand-maintained matrix state**; absent file → the static inventory.
- `status` stays the static inventory but reflects q06 strict as `implemented`, not `input_gated` and
  not `blocked`. Keep `run --all` **out**.

## Phase 4 — tests, guards, reconcile, evidence, seal

### Tests (`crates/toron-tpch/tests/`)

- `decimal_probe.rs` — hardened to assertions (the retained verify-first).
- `strict_q6_blocks_at_bind_on_decimal` — `run_strict_q6().await` → `Blocked / Bind /
  unsupported_decimal_scalar`.
- `matrix_regenerates_from_jsonl` — write a small `reachability.jsonl`, run `matrix`, assert the table
  reflects it.
- **Reconcile** `strict_without_inputs_is_not_attempted` (ADR 261) → `strict_without_inputs_blocks_on_decimal`
  (asserts `Blocked`, not `NotAttempted`).
- `status_marks_q06_strict_implemented_not_last_result` — `status_rows()` reports q06 strict as
  `implemented` inventory and does not hardcode the measured `blocked` row.
- `unrecognized_error_is_fail_not_blocked` — unchanged.

### Guards (`tests/architecture.rs`)

`adr262_strict_q6_attempts_decimal128_compute`, `adr262_first_red_row_is_grounded_not_predeclared`,
`adr262_matrix_regenerated_from_jsonl`, `adr262_status_is_inventory_not_last_result`; carry the ADR
261 guards.

### Evidence

Add `plans/impl/0262_strict_q6_first_red_row_results.md` with the new matrix rows:
`q06 strict=blocked phase=bind reason=unsupported_decimal_scalar` + the DDL evidence row, plus a note
that this *measured* gap (decimal-literal type unification at bind) selects ADR 263. Leave the ADR 261
results as historical first-run evidence unless a small cross-reference is useful.

### Verification gate (cargo through `scripts/cargo-cgroup`)

```
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test  -p toron-tpch
scripts/cargo-cgroup run   -p toron-tpch -- run --query q06 --mode strict --allow-blocked
scripts/cargo-cgroup run   -p toron-tpch -- matrix
scripts/cargo-cgroup check -p toron --lib            # toron unchanged
cargo fmt -p toron-tpch -- --check
git diff --check
```

### Seal

`feat: adr 262 strict q6 first red row`,
`Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`. Generated data stays under
`target/tpch/`.

## Risk notes

- **The block phase is grounded, not assumed** — bind, `TypeMismatch`. A future binder change (e.g.
  coercing decimal literals) would move it; the probe test catches the shift — update the expected
  phase/reason then (a refinement, not a surprise).
- **`l_quantity < 24` also mismatches** (`Decimal128` vs integer literal); the binder blocks on the
  first such comparison. The reason is the same decimal-type-unification gap; don't depend on *which*
  comparison blocks first.
- **Strict ≠ official** in this cut — Toron-authored official-*typed* SQL, not QGEN text; keep the "not
  a TPC-H result" statement. The embedded `tpchgen::q_and_a` official-text + SF=1 answer path is the
  deferred reachability+correctness follow-on.
- **Reconcile the 261 strict test** — it asserts `not_attempted`; strict now blocks. Don't leave it
  failing.
