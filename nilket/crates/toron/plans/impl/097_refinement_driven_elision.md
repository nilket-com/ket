# Impl Plan — ADR 097: Refinement-Driven Read-Time Elision

**Migration for** `plans/adr/097_refinement_driven_elision.md`. **Toron-only, binder-local.** Add one
`AlwaysReject` proof to 095's `static_filter_class` — *the whole-root filter atom contradicts the
durable refinement of a direct stored scan column* — reusing 095's `EmptyRelation`. No Wendelon /
096-contract / exec / PBC / wire / SQL-grammar change.

## Pre-flight anchors (grounded)

| Concern | Anchor |
|---|---|
| Classifier (095) | `static_filter_class(&BoundExprProgram)` `binder.rs:1169`; `StaticFilterClass { AlwaysPass, AlwaysReject, Unknown }` |
| Hook + reuse | `plan_filter_with_predicate_pushdown(input, predicate)` `binder.rs:1097`; elision → `EmptyRelation { schema: input.schema() }` `:1103-1113` (before PBC) |
| Callers (catalog-accessible) | binder Filter arm `:111`; update-source path `:704` |
| Durable refinement | `Catalog::logical_type_registry()`; `LogicalTypeSpec.refinement` `ir.rs:38` |
| Seed trap (do NOT use) | seed-only `logical_type_spec` `binder.rs:3351` |
| Provenance | `BoundExprNode::Column` + `node_logical_type` (087 sidecar); `ExecutionPlan::Scan { projection: Option<…> }` |
| Refinement AST (096) | `LogicalTypeRefinementPredicate { Comparison{left,op,right}, And }`; `LogicalTypeRefinementValue { Value, Int64, …, Float64(u64 bits) }` |

## Phase 1 — Thread the durable registry into the seam

`plan_filter_with_predicate_pushdown` gains a `logical_types: &LogicalTypeRegistry` param;
`static_filter_class(input: &ExecutionPlan, predicate: &BoundExprProgram, logical_types:
&LogicalTypeRegistry)`. Callers (Filter arm `:111`, update-source path `:704`) pass
`self.catalog.logical_type_registry()`. Test-only direct calls take a registry fixture. **Never**
`logical_type_spec` (seed — the 091/092 trap).

## Phase 2 — Preserve 095, add the refinement arm

```rust
fn static_filter_class(input, predicate, logical_types) -> StaticFilterClass {
    if structural_non_nullable_is_null(predicate) { return AlwaysReject; }            // 095
    if let Some(c) = refinement_static_filter_class(input, predicate, logical_types) { return c; } // 097
    if predicate.single_root().is_some() && !program_has_column_references(predicate) {
        return closed_root_filter_class(predicate);                                   // 095
    }
    Unknown
}
```
`refinement_static_filter_class` returns `Some(AlwaysReject)` only on a proven contradiction; every
other shape → `None` (→ falls through to 095/`Unknown`). No new `AlwaysPass`.

## Phase 3 — Provenance gate (D3 — the soundness linchpin)

In `refinement_static_filter_class`, require **all**:
- `input` is `ExecutionPlan::Scan { projection: None, .. }` (unprojected — keeps ordinals unambiguous;
  filter binding precedes scan-projection pushdown).
- `predicate.single_root()` is a binary comparison using `BoundBinaryOp::{Eq, Neq, Lt, Lte, Gt, Gte}`.
- exactly one side is `BoundExprNode::Column(col)` addressing the scan schema; the other is a non-null
  literal.
- `predicate.node_logical_type(col_node)` is non-naked and resolves in `logical_types` to a spec with
  `refinement: Some(_)`.
- the literal is carrier-compatible.

Any failure → `None`. A `Project(CAST(...))`/computed column fails (not a direct scan `Column`, or
`input` not a `Scan`) → no elision. Pin with the manual-plan CAST fixture.

## Phase 4 — Normalize the atom

`enum FilterCmpOp { Eq, Ne, Lt, Le, Gt, Ge }`. Map `BoundBinaryOp::Neq/Lte/Gte` to the normalized
helper's `Ne/Le/Ge` names. Flip operator when the literal is on the left so the normalized atom is
always `VALUE op literal` (`literal < column` → `VALUE > literal`, etc.). Reject
null literals / carrier-mismatched literals (→ `None`).

## Phase 5 — The bounded 1D `CarrierDomain` reasoner (module-local, no solver)

```rust
enum CarrierLiteral {
    Int32(i32),
    Int64(i64),
    Float32(FiniteF32),
    Float64(FiniteF64),
    Utf8(String),
    Boolean(bool),
}
struct OrderedDomain<T> { lower: Option<Bound<T>>, upper: Option<Bound<T>>, excluded: Vec<T> }
enum CarrierDomain { Int32(OrderedDomain<i32>), Int64(OrderedDomain<i64>),
    Float32(OrderedDomain<FiniteF32>), Float64(OrderedDomain<FiniteF64>),
    Utf8(DiscreteDomain<String>), Boolean(DiscreteDomain<bool>) }
```
Capabilities: build a domain from one `VALUE op literal`; build the refinement domain from
`Comparison`/`And(...)` (intersect children); intersect two same-carrier domains; decide empty.
- `Eq` → singleton point; `Ne` → **excluded point** (`CHECK (VALUE != 0)` + `id = 0` elides);
  ordered ops → interval bounds.
- Bool/Utf8 → equality/inequality singletons only (no Utf8 range/collation).
- **Floats: `f64::from_bits`/`f32::from_bits`; reject NaN/non-finite → `None`; compare decoded values.**
Empty intersection → `AlwaysReject`; anything unrepresentable → `None`.

## Phase 6 — 3VL + error fences (D6/D7)

Structural only — never evaluate casts/arithmetic/functions/programs/row-values/PBC. Nullable refined
columns: `col op literal` may still elide (NULL rows excluded by the comparison); refinement alone
**never** rejects `col IS NULL` (095's structural rule is the only `IS NULL` elision). Any uncertain
conversion / unsupported carrier → `None`.

## Phase 7 — Behavior fixtures (`predicate_widening_test.rs` + `logical_type_ddl_test.rs`)

`refined_positive_column_eq_negative_elides_to_empty_relation` (spy scan = 0);
`refined_positive_column_range_disjoint_elides`; `refined_positive_column_overlapping_range_does_not_elide`;
`refinement_exclusion_point_elides` (`!= 0` + `= 0`); **`cast_ascribed_projected_column_does_not_elide`**
(manual `Project(CAST(raw AS PersonId))`-over-scan + `Filter` plan — no SQL-subquery dependency);
`refinement_on_non_scan_input_does_not_elide`; `nullable_refined_column_is_null_does_not_elide`;
`naked_or_unrefined_column_does_not_elide`; **`refinement_elision_uses_durable_registry_not_seed_lookup`**
(durable type created via the 096 path elides; seed/static does not — reuse `logical_type_writer_coordinator()`);
`float_refinement_decodes_bits_for_ordering` (`Price CHECK (VALUE >= 0.0)` + `price = -1.0`);
`count_star_over_refinement_elided_filter_returns_single_zero` (095 cardinality);
optional `update_source_filter_over_refined_column_elides`.

## Phase 8 — Architecture guards (incl. the 096-guard inversion)

`adr097_refinement_elision_uses_durable_registry_not_seed` (helper body has no `logical_type_spec`);
`adr097_refinement_elision_requires_stored_scan_column_provenance` (checks `Scan { projection: None }`
+ direct `Column`); **`adr097_refinement_elision_only_from_durable_stored_scan_receipts`** (retarget
ADR 096's `adr096_no_refinement_elision_yet` — the new invariant, **don't leave it red**);
`adr097_refinement_elision_reuses_095_empty_relation` (no new exec-plan variant);
`adr097_static_filter_class_is_structural_3vl_and_error_conservative`;
`adr097_cast_try_cast_remain_ephemeral_not_receipts`;
`adr097_no_wendelon_096_contract_pbc_executor_storage_or_wire_change`.

## Phase 9 — Verification

```
cargo test -p toron --test logical_type_ddl_test --test predicate_widening_test   # elision + non-elision + provenance
cargo test -p toron --test update_test                                            # source-filter elision
cargo test -p toron --test architecture_test                                      # adr097_* + inverted 096 guard
cargo test -p toron                                                               # full suite; 095 cardinality + no regression
```

**Acceptance:** stored refined-column impossible filters elide with zero scan; CAST-ascribed /
non-scan / overlapping / naked / `IS NULL` / seed-only do **not** elide; durable registry used; floats
ordered via decoded values; the `!=` excluded-point case elides; 096's no-elision guard inverted (not
red); 095 cardinality + error conservatism hold; full suite green.

## Risks / notes

- **Provenance is the soundness linchpin** — `input == Scan { projection: None }` + direct scan
  `Column` keeps elision to genuine 096 receipts; the CAST-ascribed fixture pins it.
- **Durable-not-seed** — `logical_type_spec` (seed) would silently never elide on real durable
  refinements.
- **Floats compare on decoded finite values**, not storage bits; NaN/non-finite → no elision.
- **Structural proof only** — no expression evaluation (095 error fence); unrepresentable → `Unknown`.
- **Invert, don't delete** 096's no-elision guard (discipline #4).
- **Deferred** (follow-ons): pass-through-projection provenance, top-level-`AND` decomposition,
  multi-column, Utf8 collation ranges.
