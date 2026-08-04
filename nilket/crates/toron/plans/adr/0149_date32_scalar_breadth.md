# ADR 149: Temporal Date32 Selectivity Breadth

**Status:** Promoted. **Genuine 3-of-3 (Claude, Codex, Gemini).** Date range filters
(`WHERE order_date >= '2024-01-01'`) are the #1 analytical predicate, but they are
**dark** in Toron's CBO: Wendelon already maintains Date32 min/max range facts
(`stats.rs:1774`, signed-4-byte day numbers; `ARROW_STATS_TAG_DATE32`), yet Toron's
`ScalarValue`/SQL/Rune cannot express a `Date32` value, so date predicates can't bind,
execute, or reach the range-fact consumer. ADR 149 makes **`Date32` a first-class
Toron scalar** — the producer and the integral range math (144) are already done, so
this is a **scalar-foundation cut** (literal, comparison, execution, display) with
range/equality/IN selectivity falling out for free. **No Wendelon change;
estimate-channel only.**

**Placement:** Toron scalar + SQL/Rune frontend + binder + executor + optimizer
consumer. No Wendelon producer/schema change.
**Predecessors:** 144 (range facts — Date32 tags produced but dark in Toron), 141
(equality NDV), 147 (float range consumer + typed `RangeScalar`), 148 (IN-list),
[[rune_is_primary_interface]], and the **rejected 149 hard-byte branch** (deferred:
Wendelon contributor framing isn't hard in Toron's runtime IPC unit, and
`BroadcastInferred` does not retire replay — a future hard-byte ADR needs a measured
encoded-size ledger or a runtime batch-count contract).
**Migration:** `plans/impl/0149_date32_scalar_breadth.md`.

## Decision

**D1 — `ScalarValue::Date32(Option<i32>)`** (Arrow `DataType::Date32`, signed days
since 1970-01-01). `data_type()` → `Date32`; update typed-null, structural eq/hash,
scalar display, literal arrays, result/system-row + write materialization, and **every
exhaustive scalar match** (compile-driven).

**D2 — Both literal forms (bounded-lenient — the resolved divergence).** (a) Explicit
`DATE 'YYYY-MM-DD'` / `Date32 '…'` via `SqlExpr::TypedString` (the sqlparser 0.62 AST,
*not* `Value::Date`); Rune `lit_date("YYYY-MM-DD")`. (b) A **bare `Utf8` string literal
coerces to `Date32`** only when the opposite comparison/`IN` target is already
`DataType::Date32`. Both lower through one strict `parse_date_ymd` (proleptic-Gregorian
validated; reject invalid leap days, time/zone components, whitespace, signed years,
out-of-`i32`); invalid → `BindingError`, never a silent default.

> **Why bounded-lenient (3-of-3, grounded):** the #1 date filter is written
> `WHERE d >= '2024-01-01'` (bare string), so strict (`DATE '…'`-only) makes the
> headline value barely fire. The coercion **extends an existing mechanism** —
> `coerce_comparison_literals` → `coerce_literal_value(value, target_type)`
> (`binder.rs:7664/7687`) — adding one `Utf8 → Date32` arm that **reuses the same
> `parse_date_ymd` helper** the explicit literal needs anyway. It is **literal-only by
> construction** (the hook rewrites literals, never columns) and **target-typed** (fires
> only against a `Date32` column), so it does not become a general coercion model.
> Codex iter-4's strict dissent was conceded in iter-5 on exactly this grounding.

**D3 — Date32 is not Int32.** No `Int32↔Date32` coercion (despite the i32 carrier).
Range/equality/IN fact spending requires `ScalarValue::Date32`; a raw integer day
number cannot spend a Date32 fact. A `Date32` column vs a non-date string literal is
a **bind error**; a `Utf8` *column* vs a `Date32` literal stays a **type mismatch**
(the coercion is literal-only).

**D4 — Execution via Arrow Date32 arrays.** Date32 literal eval → `Date32Array`;
equality/ordering comparison uses Arrow's existing kernels once both operands are
`Date32Array` (exec dispatches per-type with explicit downcasts, so add the `Date32`
arms — literal-array, downcasts, projection/output). Three-valued Arrow nulls, no
custom date truth table. Output renders `Date32` as canonical `YYYY-MM-DD`, not raw
day numbers.

**D5 — Basic SQL/Rune carrier.** `CREATE TABLE t (d DATE | Date32)` →
`CreateTableColumnType::Physical(DataType::Date32)` (the DDL path is **direct** —
`lower_create_table_data_type`; no `LogicalTypePhysical` needed). `INSERT … VALUES
(DATE '…')` + CTAS materialize Date32; `information_schema` renders `DATE`.
`CAST`/`TRY_CAST` may recognize Date32 as a target for identity/typed-null paths only;
non-identity casts + temporal arithmetic are out of scope.

**D6 — `LogicalTypePhysical::Date32` deferred (grounded).** The DDL carrier is direct
`DataType::Date32`, so the logical-type-system integration (`LogicalTypePhysical`,
Date32 logical-refinement values — a cross-ring compute contract) is **not required**
and is a named follow-on. (The impl verifies no literal/refinement path forces it.)

**D7 — PBC/runtime pushdown preserves type or fails open (Codex D8).** A Date32
predicate may enter the PBC scan-pushdown program only if the program carries enough
type evidence to distinguish Date32 from a raw Int32 opcode. The acceptable v1
fallback is **no PBC pushdown for Date32** (the filter executes in Toron) — the
optimizer estimate still works. **Never** emit Date32 as an untyped integer opcode.

**D8 — Selectivity is the trivial consumer; estimate-channel only.** Binder imports
`ARROW_STATS_TAG_DATE32`; `decode_range_scalar` decodes it as a signed-4-byte
`RangeScalar::Integral`; `range_scalar_for_fact` accepts only `ScalarValue::Date32`
for Date32 facts. Range (144), equality NDV (141), IN-list NDV (148) reuse their math
with Date32 literal keys. Writes **only** `estimated_row_count` — never
`row_upper_bound`/`byte_upper_bound`/`EmptyRelation`; 141 `min` combination.

**D9 — No Wendelon contract bump.** Producers, optimizer-fact schema,
`LogicalTypePhysical`, and refinement values unchanged.

**D10 — Date32 only.** Date64/Timestamp (units/tz), decimal, date arithmetic,
`EXTRACT`/`date_trunc`/`CURRENT_DATE`, non-identity casts → named follow-ons.

## Headline gate
`date32_range_filtered_dim_with_wendelon_min_max_infers_broadcast_by_estimate` — a
Date32 dim with live min/max; `WHERE d.order_date >= '2026-01-01' AND d.order_date <
DATE '2026-02-01'` binds (coercing the bare lower bound, D2), executes, derives
`estimated_row_count` via integral interpolation → `BroadcastEstimated`. **Fails on
`main`** (no Date32 scalar/literal).

## Supporting gates
`sql_date_literal_lowers_to_date32_scalar`; `rune_lit_date_lowers_to_date32_scalar`;
`bare_string_literal_coerces_to_date32_against_date_column`;
`invalid_string_date_against_date32_column_is_bind_error`;
`utf8_column_vs_date32_literal_stays_type_mismatch`;
`int_literal_does_not_coerce_to_date32` / `date32_fact_rejects_int32_literal_spend`;
`create_table_date_column_uses_arrow_date32`; `insert_and_ctas_materialize_date32`;
`date32_comparison_executes_with_arrow_null_semantics`;
`date32_select_output_renders_iso_date`;
`date32_range_reuses_integer_interpolation`;
`date32_equality_and_in_list_use_date32_literal_keys`;
`date32_pbc_declines_or_carries_type_not_plain_int` (D7);
`date32_estimates_are_estimate_only_no_hard_bounds_no_elision`;
`date64_timestamp_facts_still_fail_open`.
Architecture: `adr149_date32_scalar_breadth_is_bounded_and_estimate_only`; retarget
the CTAS/DDL guard that excludes `DataType::Date32`.

## Forward arc
`144 range → 147 float range → 148 IN-list → **149 Date32 scalar/range breadth** →
Date64/Timestamp → decimal breadth → measured-skew sketches → Theta join-key
intersection → join selectivity → reordering`.

## Consensus trail
Pivoted from hard materialized bytes (rejected: relabeled-estimate + replay-is-
unconditional — Claude's two groundings, endorsed by Codex iter-2 + the operator) to
Date32. Claude reframed the producer + range math as done (the cut is the scalar
foundation). The **literal-coercion** divergence — Codex strict vs Claude/Gemini
bounded-lenient — resolved 3-of-3 to bounded-lenient on the grounded existing
`coerce_literal_value` hook (Codex conceded iter-5). `LogicalTypePhysical` deferral
grounded on the direct DDL carrier. No remaining divergence.
