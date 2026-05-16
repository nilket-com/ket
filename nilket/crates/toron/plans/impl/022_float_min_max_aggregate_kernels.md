# Implementation Plan: ADR 022 — Float MIN / MAX Aggregate Kernels

**Companion ADR:**
`plans/adr/022_float_min_max_aggregate_kernels.md`

## Status

Promoted.

Five-phase incremental migration. Each phase
compiles green and keeps the existing test suite
green except where ADR 022's contract changes are
intentional. The contract is **additive at the SQL
surface** (Float MIN / MAX previously rejected at
the binder; now binds and executes) and
**internally renaming** at the catalog layer
(`IntMinMax` → `OrderedMinMax`).

Implementation rules:

> Catalog metadata moves; row-update behavior stays.
> ADR 021's executor-boundary discipline applies.

> No broad `_ =>` shortcuts in targeted `AggState`
> routing / fallback match sites. Error catch-alls
> that deliberately reject non-MIN / MAX states may
> remain, but the new variants must be listed wherever
> state kind controls behavior.

> Sticky-NaN check precedes `total_cmp`; never use
> `f64::min` / `f64::max` / `f32::min` / `f32::max`
> as a substitute.

## Goal

Land Float MIN / MAX across `aggregate_kernel.rs`,
`exec.rs`, `binder.rs`, and the architecture / test
suite without:

- Adding a new IR variant.
- Adding a new `StableAggregateKernel` row.
- Adding a new `AggregateOutputRule` variant.
- Removing `AggState: Copy`.
- Introducing row-path trait dispatch.
- Changing plan digests for integer-only queries.
- Changing fragmenter / runtime-filter / cross-ring
  surfaces.

## Substrate

### Modified files

- `crates/toron/src/aggregate_kernel.rs` — Phase 1.
- `crates/toron/src/exec.rs` — Phase 2.
- `crates/toron/src/binder.rs` — Phase 3.
- `crates/toron/tests/architecture_test.rs` —
  Phase 4.
- `crates/toron/tests/group_by_test.rs` — Phase 5
  (replace existing Float MIN / MAX rejection
  expectations).
- `crates/toron/tests/distributed_process_test.rs`
  — Phase 5 (new fixture).
- `crates/toron/benches/storage_execution.rs` —
  Phase 5 (new bench rows).

### New files

- `crates/toron/tests/float_min_max_test.rs` —
  semantic regression file (Phase 5).
- `crates/toron/plans/impl/022_float_min_max_aggregate_kernels_bench_results.md`
  — bench artifact (Phase 5).

### Code-grounded touchpoint sweep

Verified file:line references:

| Site | File:Line | Phase | Change |
|---|---|---|---|
| `AggregateInputDomain` enum | `aggregate_kernel.rs:22-28` | 1 | Rename `IntMinMax` → `OrderedMinMax` |
| `input_domain_accepts` arm | `aggregate_kernel.rs:185-192` | 1 | Rename match arm |
| `is_int_min_max_input` fn | `aggregate_kernel.rs:238-240` | 1 | Rename to `is_ordered_min_max_input`; widen body |
| `STABLE_AGGREGATE_KERNELS` MIN row | `aggregate_kernel.rs:82-90` | 1 | Field rename |
| `STABLE_AGGREGATE_KERNELS` MAX row | `aggregate_kernel.rs:91-100` | 1 | Field rename |
| `AggState` enum | `exec.rs:2902-2913` | 2 | Add 4 Float variants |
| `init_states` (cfg(test)) | `exec.rs:2922-2946` | 2 | NO CHANGE (see Phase 2 § Out of scope) |
| `init_bound_state_for_call` | `exec.rs:2954-2991` | 2 | NO CHANGE (existing data-type dispatch covers Float arms) |
| `min_state_for_data_type` | `exec.rs:3027-3035` | 2 | Add Float32 + Float64 arms |
| `max_state_for_data_type` | `exec.rs:3037-3045` | 2 | Add Float32 + Float64 arms |
| `update_min_state` | `exec.rs:3149-3184` | 2 | Add 4 Float arms via `f64/f32_min_sticky_total` |
| `update_max_state` | `exec.rs:3186-3221` | 2 | Symmetric MAX arms |
| `min_max_state_array` | `exec.rs:3223-3235` | 2 | Add 4 Float variant pairs → `FloatXXArray` |
| `min_max_column_from_groups` | `exec.rs:3317-3347` | 2 | Add `DataType::Float32` + `DataType::Float64` arms |
| Scalar dispatch state-match | `exec.rs:1100-1116` | 2 | Extend `MinInt32 \| MinInt64` arms; add `MaxFloat32 \| MaxFloat64` |
| Scalar dispatch state-match | `exec.rs:1142-1149` | 2 | Extend "expected 0" routing arms with 4 new variants |
| Grouped dispatch state-match | `exec.rs:1255-1297` | 2 | Symmetric to scalar |
| New helper functions | `exec.rs` (near `update_min_state`) | 2 | `f32_min_sticky_total`, `f32_max_sticky_total`, `f64_min_sticky_total`, `f64_max_sticky_total` |
| `coerce_literal_value` | `binder.rs:1296-1304` | 3 | Add 4 coercion arms |
| MIN / MAX type-mismatch error strings | `binder.rs:1170-1180` | 3 | Enumerated wording |
| `adr019_min_max_aggregate_kernels_guards_are_narrow` | `tests/architecture_test.rs` | 4 | `OrderedMinMax` assertion |
| `aggregate_kernel_catalog_guards_are_narrow` | `tests/architecture_test.rs` | 4 | Preserved-string list + variant rename + `AggState` count = 13 |
| `tests/float_min_max_test.rs` | new file | 5 | Sticky-NaN + signed-zero + Float32 HAVING regression suite |
| `tests/group_by_test.rs` Float MIN / MAX binding / rejection cases | `group_by_test.rs:824+`, `:932+` | 5 | Replace selected Float rejections with successful binding expectations |
| `tests/distributed_process_test.rs` integer MIN / MAX digest fixture | `distributed_process_test.rs:405`, `:663+` | 5 | Add grouped Float64 fixture (2/4 workers, finite only) |
| `benches/storage_execution.rs` fixture / cases | `storage_execution.rs:416+`, `:483+` | 5 | Add `min_float64`, `max_float64`, `min_nullable_f32`, `group_float_min_max` rows |

## Phases

### Phase 1 — Catalog widening

**Goal.** Rename `AggregateInputDomain::IntMinMax`
→ `OrderedMinMax`; widen acceptance predicate;
update catalog rows. No `exec.rs` or `binder.rs`
changes; build stays green.

**Tasks.**

1. In `aggregate_kernel.rs`:
   - Rename enum variant
     `AggregateInputDomain::IntMinMax` →
     `OrderedMinMax`.
   - Rename helper `is_int_min_max_input` →
     `is_ordered_min_max_input`.
   - Widen helper body:
     ```rust
     fn is_ordered_min_max_input(input: &DataType) -> bool {
         matches!(
             input,
             DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64
         )
     }
     ```
   - Update `input_domain_accepts` match arm name.
   - Update `STABLE_AGGREGATE_KERNELS` MIN and MAX
     rows (lines 82-100): field
     `input_domain: AggregateInputDomain::IntMinMax`
     → `input_domain: AggregateInputDomain::OrderedMinMax`.

2. In `binder.rs`: find any direct references to
   `IntMinMax` (grep). Likely only the error-string
   match site near `binder.rs:1173-1180` which
   references `StableAggregateKernel::Min` / `Max`
   not the domain enum directly — verify.

3. Run `cargo build -p toron`. Compiler catches all
   call sites.

4. Run `cargo test -p toron`. The widened
   acceptance predicate means MIN(Float64) /
   MIN(Float32) no longer rejects at the binder
   gate. **Expected test failures after Phase 1
   alone** (remedied by Phase 2 + Phase 4):
   - Existing tests asserting MIN(Float) rejection
     fail.
   - Architecture guards asserting `IntMinMax`
     fail.

   **Phase 1 + Phase 2 should merge as one atomic
   commit** to avoid an intermediate broken-tests
   state. Do not land ignored tests; if split
   locally, keep Phase 1 as an unmerged checkpoint
   until Phase 2 is included.

**Definition of Phase 1 done.**
- `aggregate_kernel.rs` references `OrderedMinMax`;
  no `IntMinMax` references remain.
- `is_ordered_min_max_input` admits the four types.
- Build compiles.

### Phase 2 — Executor widening

**Goal.** Add four `AggState` variants, two
helper-function pairs, Float arms in all six
executor-side MIN / MAX helpers; state-match
exhaustiveness sweep. `AggState: Copy` preserved.

**Tasks.**

1. In `exec.rs`, extend `AggState`
   (`exec.rs:2902-2913`):
   ```rust
   #[derive(Debug, Clone, Copy)]
   enum AggState {
       Count(i64),
       SumInt { total: i128, seen: bool },
       SumFloat { total: f64, seen: bool },
       MinInt32 { value: Option<i32> },
       MinInt64 { value: Option<i64> },
       MinFloat32 { value: Option<f32> },   // NEW
       MinFloat64 { value: Option<f64> },   // NEW
       MaxInt32 { value: Option<i32> },
       MaxInt64 { value: Option<i64> },
       MaxFloat32 { value: Option<f32> },   // NEW
       MaxFloat64 { value: Option<f64> },   // NEW
       AvgInt { total: i128, count: u64 },
       AvgFloat { total: f64, count: u64 },
   }
   ```

2. Add four helper functions near
   `update_min_state` / `update_max_state` (around
   `exec.rs:3145`):

   ```rust
   // total_cmp pins signed-zero ordering; the explicit NaN branch
   // preserves ADR 022 sticky-NaN semantics.

   #[inline]
   fn f64_min_sticky_total(current: f64, candidate: f64) -> f64 {
       if current.is_nan() || candidate.is_nan() {
           return f64::NAN;
       }
       match current.total_cmp(&candidate) {
           std::cmp::Ordering::Greater => candidate,
           _ => current,
       }
   }

   #[inline]
   fn f64_max_sticky_total(current: f64, candidate: f64) -> f64 {
       if current.is_nan() || candidate.is_nan() {
           return f64::NAN;
       }
       match current.total_cmp(&candidate) {
           std::cmp::Ordering::Less => candidate,
           _ => current,
       }
   }

   #[inline]
   fn f32_min_sticky_total(current: f32, candidate: f32) -> f32 {
       if current.is_nan() || candidate.is_nan() {
           return f32::NAN;
       }
       match current.total_cmp(&candidate) {
           std::cmp::Ordering::Greater => candidate,
           _ => current,
       }
   }

   #[inline]
   fn f32_max_sticky_total(current: f32, candidate: f32) -> f32 {
       if current.is_nan() || candidate.is_nan() {
           return f32::NAN;
       }
       match current.total_cmp(&candidate) {
           std::cmp::Ordering::Less => candidate,
           _ => current,
       }
   }
   ```

   The single inline-doc comment above the first
   helper carries the policy explanation; the other
   three helpers follow the same pattern without
   repeating the comment.

3. Extend `min_state_for_data_type`
   (`exec.rs:3027-3035`):
   ```rust
   fn min_state_for_data_type(data_type: &DataType) -> Result<AggState, ExecutionError> {
       match data_type {
           DataType::Int32 => Ok(AggState::MinInt32 { value: None }),
           DataType::Int64 => Ok(AggState::MinInt64 { value: None }),
           DataType::Float32 => Ok(AggState::MinFloat32 { value: None }),  // NEW
           DataType::Float64 => Ok(AggState::MinFloat64 { value: None }),  // NEW
           _ => Err(ExecutionError::InvalidExpression {
               message: format!("MIN does not support input type {:?}", data_type),
           }),
       }
   }
   ```
   Symmetric `max_state_for_data_type`.

4. Extend `update_min_state` (`exec.rs:3149-3184`)
   with four Float arms:
   ```rust
   AggState::MinFloat32 { value } => {
       let candidate = array
           .as_any()
           .downcast_ref::<Float32Array>()
           .ok_or_else(|| ExecutionError::InvalidExpression {
               message: "MIN(Float32) array type mismatch".to_string(),
           })?
           .value(row);
       *value = Some(value.map_or(candidate, |current| {
           f32_min_sticky_total(current, candidate)
       }));
       Ok(())
   }
   AggState::MinFloat64 { value } => {
       let candidate = array
           .as_any()
           .downcast_ref::<Float64Array>()
           .ok_or_else(|| ExecutionError::InvalidExpression {
               message: "MIN(Float64) array type mismatch".to_string(),
           })?
           .value(row);
       *value = Some(value.map_or(candidate, |current| {
           f64_min_sticky_total(current, candidate)
       }));
       Ok(())
   }
   ```
   Symmetric arms in `update_max_state` calling the
   `_max_sticky_total` helpers.

5. Extend `min_max_state_array`
   (`exec.rs:3223-3235`):
   ```rust
   AggState::MinFloat32 { value } | AggState::MaxFloat32 { value } => {
       Ok(Arc::new(Float32Array::from(vec![*value])) as ArrayRef)
   }
   AggState::MinFloat64 { value } | AggState::MaxFloat64 { value } => {
       Ok(Arc::new(Float64Array::from(vec![*value])) as ArrayRef)
   }
   ```

6. Extend `min_max_column_from_groups`
   (`exec.rs:3317-3347`):
   ```rust
   DataType::Float32 => {
       let values = groups
           .values()
           .map(|states| match states[agg_idx] {
               AggState::MinFloat32 { value } | AggState::MaxFloat32 { value } => value,
               _ => None,
           })
           .collect::<Vec<_>>();
       Ok(Arc::new(Float32Array::from(values)) as ArrayRef)
   }
   DataType::Float64 => {
       let values = groups
           .values()
           .map(|states| match states[agg_idx] {
               AggState::MinFloat64 { value } | AggState::MaxFloat64 { value } => value,
               _ => None,
           })
           .collect::<Vec<_>>();
       Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
   }
   ```

7. **State-match exhaustiveness sweep.** Audit all
   `match state` and `match (state, ...)` sites in
   `exec.rs`. Each
   `AggState::MinInt32 { .. } | AggState::MinInt64 { .. }`
   pattern extends to include the two new Float MIN
   variants; each
   `AggState::MaxInt32 { .. } | AggState::MaxInt64 { .. }`
   pattern extends to include the two new Float MAX
   variants. Known sites from the touchpoint sweep:
   - `exec.rs:1110-1116`: scalar update dispatch.
   - `exec.rs:1142-1149`: scalar "expected 0"
     routing.
   - `exec.rs:1265-1269`: grouped update dispatch.
   - `exec.rs:1292-1297`: grouped routing.
   - `exec.rs:3094-3097`: per-state match.

   **No broad routing wildcards** — every match in
   this sweep that routes by aggregate state should
   enumerate the 13 variants. Narrow reject
   catch-alls inside helper functions may remain when
   they preserve diagnostics. Use `cargo check` to
   compiler-catch missed sites.

8. Run `cargo build -p toron` → green.
9. Run focused suites:
   ```sh
   cargo test -p toron --test group_by_test
   cargo test -p toron --test distributed_process_test
   cargo test -p toron aggregate_projection_accounting
   ```
   Existing tests pass; new MIN(Float) / MAX(Float)
   tests fail until Phase 5.

**Out of scope (Phase 2).**
- `init_states` (cfg(test) at
  `exec.rs:2922-2946`) stays type-blind. Production
  path is `init_bound_state_for_call` which already
  inspects `call.args[0].data_type()`. ADR 022
  §Scale boundary "Explicitly unchanged"
  documents this.

**Definition of Phase 2 done.**
- 13 `AggState` variants, `Copy` preserved.
- All six executor-side MIN / MAX helpers extended.
- State-match sweep complete; no wildcards.
- Build green; integer-only tests pass.

### Phase 3 — Binder companion

**Goal.** Enumerated type-mismatch error strings;
four new `coerce_literal_value` arms enabling
Float32 / Float64 HAVING comparisons.

**Tasks.**

1. In `binder.rs` near lines 1170-1180, update
   MIN / MAX type-mismatch error strings:
   ```rust
   StableAggregateKernel::Min => {
       format!("MIN expects Int32, Int64, Float32, or Float64 input, got {data_type:?}")
   }
   StableAggregateKernel::Max => {
       format!("MAX expects Int32, Int64, Float32, or Float64 input, got {data_type:?}")
   }
   ```
   Arity strings (`"MIN requires one argument"`,
   `"MAX requires one argument"`) unchanged.

2. In `binder.rs`, extend `coerce_literal_value`
   (`binder.rs:1296-1304`):
   ```rust
   fn coerce_literal_value(value: ScalarValue, target_type: &DataType) -> ScalarValue {
       match (value, target_type) {
           (ScalarValue::Int32(v), DataType::Int64) => ScalarValue::Int64(v.map(i64::from)),
           (ScalarValue::Int32(v), DataType::Float64) => ScalarValue::Float64(v.map(f64::from)),
           // ADR 022 additions:
           (ScalarValue::Int32(v), DataType::Float32) => {
               ScalarValue::Float32(v.map(|i| i as f32))
           }
           (ScalarValue::Int64(v), DataType::Float32) => {
               ScalarValue::Float32(v.map(|i| i as f32))
           }
           (ScalarValue::Int64(v), DataType::Float64) => {
               ScalarValue::Float64(v.map(|i| i as f64))
           }
           (ScalarValue::Float64(v), DataType::Float32) => {
               ScalarValue::Float32(v.map(|f| f as f32))
           }
           (value, _) => value,
       }
   }
   ```
   Do not change the helper return type. Do not add
   column-column coercion, join-key coercion, or a
   general promotion lattice.

3. Run `cargo test -p toron`. Binder-side tests
   pass; the executor + binder + catalog stack
   compiles as a coherent ADR 022 surface.

**Definition of Phase 3 done.**
- Enumerated error strings present.
- Four new coercion arms compile and route
  correctly through `coerce_comparison_literals`.

### Phase 4 — Architecture guard adjustment

**Goal.** ADR 019 guard updates for `OrderedMinMax`;
ADR 021 guard updates for preserved-string list,
catalog source-read, and `AggState` variant count
assertion.

**Tasks.**

1. In `tests/architecture_test.rs`, locate
   `adr019_min_max_aggregate_kernels_guards_are_narrow`.
   Update:
   - `MIN.input_domain` assertion: `IntMinMax` →
     `OrderedMinMax`.
   - `MAX.input_domain` assertion: same.
   - Add: source-read of
     `is_ordered_min_max_input` body asserts
     admitted types (`Int32`, `Int64`, `Float32`,
     `Float64`) and a representative sample of
     rejected types (e.g. `UInt32`, `Decimal128`,
     `Date32`, `Boolean`, `Utf8`) remains absent.

2. Locate
   `aggregate_kernel_catalog_guards_are_narrow`.
   Update:
   - Catalog source-read asserts `OrderedMinMax`
     variant present; `IntMinMax` not present.
   - Preserved-string list:
     - Remove: `"MIN expects Int32 or Int64 input"`,
       `"MAX expects Int32 or Int64 input"`.
     - Add: `"MIN expects Int32, Int64, Float32, or
       Float64 input"`, `"MAX expects Int32, Int64,
       Float32, or Float64 input"`.
   - `AggState` source-read: assert variant count
     = 13 (was 9 in ADR 021).
   - `AggState: Copy` derive assertion: unchanged.

3. No new architecture guard added. Sticky-NaN and
   signed-zero behavior covered by semantic
   regression tests (Phase 5).

4. Run `cargo test -p toron --test architecture_test`.
   All guards pass.

**Definition of Phase 4 done.**
- ADR 019 + ADR 021 guards green.
- Catalog source-reads find `OrderedMinMax`.
- Preserved-string list reflects enumerated
  wording.
- `AggState` count assertion = 13.

### Phase 5 — Verification + bench

**Goal.** Comprehensive regression suite;
distributed digest fixture; bench artifact
recording per-row timings and digest parity.

**Tasks.**

1. Create `crates/toron/tests/float_min_max_test.rs`
   covering:
   1. Scalar Float64 MIN / MAX over finite values.
   2. Scalar Float32 MIN / MAX over finite values.
   3. Scalar all-NULL Float64 → NULL.
   4. Scalar empty-table Float64 → one NULL row.
   5. Scalar Float64 with NaN → NaN (sticky).
   6. Scalar Float64 `[1.0, NaN, 2.0]` MIN → NaN.
   7. Scalar Float64 `[NaN, 1.0]` MAX → NaN.
   8. Scalar Float64 `[-0.0, +0.0]` MIN → bit
      pattern `-0.0` (`is_sign_negative()`).
   9. Scalar Float64 `[-0.0, +0.0]` MAX → bit
      pattern `+0.0` (`is_sign_positive()`).
   10. Scalar Float32 `[-0.0, +0.0]` MIN / MAX —
       symmetric.
   11. Grouped Float64: group with NaN → NaN;
       group without → finite min / max.
   12. Grouped Float32 with `HAVING MIN(...) > 0`
       (Int32 literal).
   13. Grouped Float32 with `HAVING MIN(...) > 0.0`
       (Float64 literal).
   14. Composition: `AVG, MIN, MAX` same query over
       NaN column → all NaN.

   Test assertions use `is_nan()` predicate, not
   `assert_eq!(NaN, NaN)`. Signed-zero assertions
   use `is_sign_negative()` /
   `is_sign_positive()` on the output bit pattern.

2. Update `tests/group_by_test.rs`: grep for
   existing Float64 / Float32 MIN / MAX rejection
   expectations; replace with successful binding +
   execution expectations.

3. Update `tests/distributed_process_test.rs`: add
   one grouped Float64 MIN / MAX process query
   fixture mirroring the ADR 019 integer fixture
   across 2-worker and 4-worker configurations.
   **Finite values only** in the fixture; digest
   parity assertion is the gate. NaN policy
   verification lives in `float_min_max_test.rs`.

4. Update `benches/storage_execution.rs`: add
   rows mirroring the ADR 021 bench shape:
   - `min_float64`
   - `max_float64`
   - `min_nullable_f32`
   - `group_float_min_max`

5. Run focused tests and broader checks:
   ```sh
   cargo fmt --check
   cargo test -p toron --test architecture_test
   cargo test -p toron --test group_by_test
   cargo test -p toron --test float_min_max_test
   cargo test -p toron --test distributed_process_test
   cargo test -p toron aggregate_projection_accounting
   cargo test -p toron
   cargo bench -p toron --bench storage_execution -- --quick --storage-execution-only
   ```

6. Write
   `crates/toron/plans/impl/022_float_min_max_aggregate_kernels_bench_results.md`
   matching ADR 021's bench artifact shape:
   - Environment block (commit, rustc, cargo,
     Arrow).
   - Fixture description (new Float32 + Float64
     columns added to the harness if needed).
   - Per-row baseline_ms / engaged_ms / speedup /
     bytes / cols / digest.
   - `pbc_engaged: false` for aggregate rows (no
     pushdown; ADR 02X territory).
   - Comparison to nearby integer MIN / MAX rows
     (`min_nullable_i32`, `max_i64`) for sanity.
   - Readout: digest parity asserted; no minimum-
     speedup gate; investigate any row regressing
     > 10%.

**Definition of Phase 5 done.**
- `float_min_max_test.rs` green; all 14+ cases
  pass.
- Updated `group_by_test.rs` green; Float
  rejection cases now expect success.
- `distributed_process_test.rs` Float64 fixture
  green; digest parity asserted across worker
  counts.
- Bench artifact recorded; no row regresses
  > 10%.
- Full `cargo test -p toron` green.
- `cargo fmt --check` clean.

## Risk register

| ID | Risk | Mitigation |
|---|---|---|
| R1 | Accidental `f64::min` / `f64::max` substitution masks sticky-NaN | Helper functions named `*_sticky_total` (not `*_min` / `*_max`); inline doc cites ADR 022; code review explicit. Phase 5 sticky-NaN test catches if substitution lands. |
| R2 | `total_cmp` signed-zero behavior drifts across rustc versions | Rust std contractually implements IEEE 754 totalOrder; `total_cmp` documentation pins behavior. Phase 5 signed-zero test asserts bit pattern. |
| R3 | Wendelon Float MIN / MAX policy diverges from Toron sticky-NaN | Deferred to ADR 02X (cross-ring AGG consumer wiring). ADR 022 §Future work documents the divergence-check requirement. Wendelon protocol layer is type-agnostic; no current divergence to reconcile. |
| R4 | `OrderedMinMax` rename misses a call site | `cargo build` catches; ≤ 5 sites by touchpoint sweep. |
| R5 | Enumerated error strings break downstream tests asserting on string content | Phase 4 architecture guard preserved-string list updates in step; grep before merge. |
| R6 | New `AggState` variants missed in a `match state` site | No broad `_ =>` shortcuts in target routing sites; compiler-caught by exhaustive matches where state variants control behavior. |
| R7 | Plan digest changes from output-name or output-type drift | `output_rule: SameAsInput` preserved; Float input → Float output; planner names unchanged. Phase 5 distributed digest fixture asserts parity. |
| R8 | NaN comparison tests fail via `assert_eq!(NaN, NaN) == false` | Tests use `is_nan()` predicate. NaN payload non-contractual per ADR. |
| R9 | NaN behavior diverges between AVG / MIN / MAX in same query | Phase 5 composition test asserts `[1.0, NULL, NaN]` → (NaN, NaN, NaN) for (AVG, MIN, MAX). |
| R10 | Catalog rename confuses ADR 019 archaeology | ADR 022 §Status block names the rename and reason; ADR 019 historical record unchanged. |
| R11 | Float32 HAVING coercion missing edge (without companion: `HAVING MIN(f32) > 0` fails to bind) | Phase 3 adds the four coercion arms; Phase 5 tests cover both `> 0` (Int32 literal) and `> 0.0` (Float64 literal) paths. |
| R12 | `min_max_column_from_groups` defaults grouped Float MIN / MAX to "unsupported MIN/MAX output type" reject | Phase 2 explicitly adds `DataType::Float32` and `DataType::Float64` arms. |
| R13 | `total_cmp` semantic surface less familiar to reviewers | Inline doc comment above first `*_sticky_total` helper cites IEEE 754 totalOrder + ADR 022 contract; companion regression test reinforces. |
| R14 | Lossy `Int64 → Float32` / `Float64 → Float32` coercion arms surprise users | `as` cast semantics are standard Rust; SQL coercion to a narrower type is implementation-defined; documented in ADR 022 §Binder coercion companion. No saturating-cast wrapper added. |
| R15 | Phase 1 lands as a broken-tests intermediate state if not bundled with Phase 2 | Bundle Phase 1+2 as one commit, or keep Phase 1 local-only until Phase 2 is included; do not land ignored tests. |

## Test plan

Existing test files that must stay green:

- `crates/toron/tests/architecture_test.rs` —
  ADR 019 + ADR 021 guards adjust (Phase 4).
- `crates/toron/tests/group_by_test.rs` — replaces
  existing Float MIN / MAX rejection expectations
  with successful binding (Phase 5).
- `crates/toron/tests/distributed_process_test.rs`
  — Float64 grouped fixture added (Phase 5).
- `crates/toron/tests/aggregate_projection_accounting_test.rs`
  — projection accounting unaffected.
- Binder + planner unit tests — error-string
  changes verified.

New test files:

- `crates/toron/tests/float_min_max_test.rs` —
  14+ cases covering sticky-NaN, signed-zero,
  empty / all-NULL, Float32 HAVING coercion,
  composition with AVG.

Bench artifact:

- `crates/toron/plans/impl/022_float_min_max_aggregate_kernels_bench_results.md`
  with `min_float64`, `max_float64`,
  `min_nullable_f32`, `group_float_min_max` rows.

## Definition of done

The refactor lands when:

1. `aggregate_kernel.rs` uses `OrderedMinMax`;
   `is_ordered_min_max_input` admits the four
   types; catalog row count = 6.
2. `exec.rs` has 13 `AggState` variants;
   `AggState: Copy` preserved; four
   `_sticky_total` helpers present; six executor-
   side MIN / MAX helpers extended; state-match
   sweep complete.
3. `binder.rs` has enumerated MIN / MAX error
   strings; four new `coerce_literal_value` arms.
4. `architecture_test.rs` ADR 019 + ADR 021 guards
   green with adjusted assertions.
5. `float_min_max_test.rs` exists and is green.
6. `group_by_test.rs` Float rejection cases
   replaced with successful binding.
7. `distributed_process_test.rs` Float64 grouped
   fixture green across 2 and 4 workers.
8. Bench artifact recorded; no row regresses
   > 10%.
9. Plan digests for integer-only MIN / MAX queries
   byte-identical.
10. `cargo fmt --check` clean;
    `cargo test -p toron` green.

## Phase ordering rationale

Phase 1 (catalog widening) lands first as the
metadata-layer change; with no executor backing,
MIN / MAX(Float) would compile but fail at
execution. Phase 2 (executor widening) follows
immediately to close the binder-accepts /
executor-rejects gap; practically Phase 1 + 2 may
merge as one commit. Phase 3 (binder companion) is
parallel-safe with Phase 2 — the coercion arms
touch independent code. Phase 4 (architecture
guard) follows the substrate so the guard's
assertions reference the post-migration state
(consistent with ADR 021 §Phase ordering
rationale). Phase 5 (verification + bench) is the
final correctness gate.
