# Impl Plan — ADR 149: Temporal Date32 Selectivity Breadth

**Migration for** `plans/adr/0149_date32_scalar_breadth.md`. **Toron-only** (scalar +
SQL/Rune + binder + exec + optimizer consumer). **No Wendelon change** — the Date32
producer + range math exist. Anchors verified against `main`.

## Surfaces
| Concern | Anchor |
|---|---|
| Scalar | `ir.rs:451` `ScalarValue` → add `Date32(Option<i32>)`; `data_type()` |
| Date parse (shared by literal + coercion) | new `parse_date_ymd(&str) -> Result<i32>` (local, dependency-light) |
| SQL explicit literal | `sql_planner.rs` `lower_expr` — `SqlExpr::TypedString(TypedString { data_type: Date/Date32, value, .. })` |
| SQL bare-string coercion (D2) | `binder.rs:7664` `coerce_comparison_literals` → `:7687` `coerce_literal_value` (add target-gated `Utf8 → Date32` arm) |
| DDL carrier (direct) | `sql_planner.rs:1178` `lower_create_table_data_type` → `SqlDataType::Date => Physical(DataType::Date32)` |
| Exec (per-type arms) | `exec.rs:867/1731` `ScalarValue::Int32 → Int32Array`; `:526/1302` downcasts; `:1785` `match data_type`; compare path |
| Rune literal | rune_engine `lit_date("YYYY-MM-DD")` |
| Selectivity consumer (trivial) | `binder.rs:4447` `decode_range_scalar` (add `ARROW_STATS_TAG_DATE32`); `range_scalar_for_fact`; equality/IN keys |
| Producer (unchanged) | `stats.rs:1774` Date32 min/max already emitted |

## Phase 0 — failing gates first
Headline (+ bare-string lower-bound twin) — fail on `main`. The D3-bound gates
(`int_literal_does_not_coerce_to_date32`, `utf8_column_vs_date32_literal_stays_type_mismatch`)
may already reject on `main` for the coarse "no Date32" reason; after implementation
they must reject for the named no-unit-leakage fence.

## Phase 1 — scalar + parser
- `ScalarValue::Date32(Option<i32>)`; `data_type()`→`Date32`; typed-null; structural
  eq/hash; literal-array; **every exhaustive `match`** (compile + `--all-targets`).
- `parse_date_ymd`: strict `YYYY-MM-DD`, proleptic-Gregorian, reject invalid leap /
  time/zone / whitespace / signed year / out-of-`i32`. Unit-test edges (1970-01-01→0,
  leap years, 2026-02-29 reject, 9999-12-31).

## Phase 2 — literals + coercion (D2/D3)
- `lower_expr`: `SqlExpr::TypedString(TypedString { data_type: Date/Date32, value, .. })`
  → `parse_date_ymd` →
  `ScalarValue::Date32` (confirm sqlparser 0.62 surfaces `DATE '…'` as `TypedString`).
- Make the comparison/IN literal-coercion helper fallible:
  `coerce_comparison_literals(...) -> Result<(), BindingError>` and
  `coerce_literal_value(...) -> Result<ScalarValue, BindingError>`.
- `coerce_literal_value`: `(Utf8(Some(s)), DataType::Date32) => parse_date_ymd(s)`;
  parse `Err` ⇒ surface a `BindingError` (not a silent uncoerced pass that defaults).
  **Literal-only + target-gated** by the existing hook. Rune `lit_date`.
- **D3:** no `Int32→Date32`/`Date32→Int32`/`Date32→Utf8-column` arm; pin the mismatch gates.

## Phase 3 — DDL carrier + exec (D4/D5/D7)
- `lower_create_table_data_type`: `SqlDataType::Date | Date32 => Physical(DataType::Date32)`
  (direct — **not** via `LogicalTypePhysical`). INSERT/CTAS → Date32; info_schema → `DATE`.
- exec Date32 arms: `ScalarValue::Date32 → Date32Array` (mirror Int32 at `:867/1731`);
  `Date32Array` downcasts where Int32 is (`:526/1302`); projection/output → `YYYY-MM-DD`.
  **Comparison:** check whether the binary-compare path calls Arrow `cmp` kernels
  (Date32-native → free) or hand-types per array; add an arm only if hand-typed.
- **PBC (D7):** Date32 predicate → push only if the PBC program carries Date32 type
  evidence; else **decline PBC** (filter runs in Toron) — never emit a raw-int opcode.

## Phase 4 — selectivity consumer (trivial) + tests
- `decode_range_scalar`: `ARROW_STATS_TAG_DATE32 => signed-4-byte i32 → RangeScalar::Integral`.
  `range_scalar_for_fact`: Date32 fact accepts only `ScalarValue::Date32` (reject Int32).
  Equality (141)/IN-list (148) Date32 literal keys.
- Tests: headline + bare-string twin; the D2/D3 bound gates; parse-reject; DDL/INSERT/CTAS;
  exec comparison + ISO output; `date32_range_reuses_integer_interpolation` (assert
  byte-identical estimate to the equivalent integer range); estimate-channel-only;
  `date32_pbc_declines_or_carries_type_not_plain_int`; `date64_timestamp_still_fail_open`.
- Architecture: the `adr149_*` guard; retarget the DDL guard excluding Date32.

## Phase 5 — verification
```sh
scripts/cargo-cgroup test -p toron
scripts/cargo-cgroup test -p toron --test architecture_test adr149
scripts/cargo-cgroup test -p toron --test architecture_test adr144 adr148   # range/IN consumers intact
scripts/cargo-cgroup check -p toron --all-targets
git diff --check
```
No `wendelon` change. 129–148 regressions green.

## Risk register
- **`ScalarValue::Date32` exhaustiveness** — widely-matched enum; compile + `--all-targets`
  forces every arm (bench/DML/PBC/system-table matches are the easy-to-miss ones,
  [[feedback_verify_all_targets_on_new_ir_field]]).
- **Coercion bounds (D2/D3)** — `Utf8→Date32` must be target-gated + literal-only;
  `utf8_column_vs_date32` + `int_literal` gates pin it; invalid date → `BindingError`.
- **Exec comparison** — if the compare path is hand-typed (not Arrow `cmp`), Date32 needs
  an explicit arm or it mis-binds; `date32_comparison_executes` is load-bearing.
- **PBC type leak (D7)** — a Date32 predicate emitted as a plain-int PBC opcode would let
  an int literal match a date column at the scan; decline-or-carry-type guards it.
- **`LogicalTypePhysical` deferral** — verify no literal/refinement path forces it; if
  one appears to, reroute through the direct `DataType::Date32` carrier or split a
  follow-on ADR rather than quietly adding the cross-ring contract here.
