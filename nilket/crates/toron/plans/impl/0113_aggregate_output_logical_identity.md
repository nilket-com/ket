# Impl Plan — ADR 113: Aggregate-Output Logical-Identity Preservation

**Migration for** `plans/adr/0113_aggregate_output_logical_identity.md`. **Scope:** `binder.rs` (a named
`aggregate_output_logical_type` helper + `aggregate_field` stamps `MIN`/`MAX` identity via the 089 writer) + tests
+ guards (1 retarget, 2 new). **Non-scope:** no IR/executor/PBC/Wendelon/storage/`aggregate_kernel` change; no
SUM/AVG additive-type identity; no contextless NULL; no NVL2. The partition is the whole decision — production is
one helper + one call site.

## Pre-flight anchors

| Concern | Anchor |
|---|---|
| Single aggregate-schema path | `aggregate_schema` (`binder.rs:5620`) → `aggregate_field` (`:5688`); callers `:175`, `:3849` |
| Bound arg identity / type | `BoundAggregateCall { function, args }`; `args.root_logical_type(0)` / `args.root_type(0)` (`ir/execution.rs:627`/`:584`) |
| Base field (physical-only) | `aggregate_kernel::stable_output_field` (`aggregate_kernel.rs:177`) |
| MIN/MAX = SameAsInput | spec `aggregate_kernel.rs:82`/`:92`; `stable_output_data_type` `:171` |
| 089 writer (reuse) | `field_with_logical_type_metadata` (used at `binder.rs:5653`) |
| `is_naked` | `LogicalTypeId::is_naked()` |
| Retarget (guard) | `adr089_group_keys_preserve_identity_aggregate_outputs_naked` (`nominal_type_identity.rs:459`) |
| Retarget (behavior) | `aggregate_output_stays_naked` (`catalog_logical_identity_test.rs:392`) |

## Phase 1 — Helper + stamp (`binder.rs`)

```rust
/// ADR 113: selection preserves identity, computation erases it.
/// MIN/MAX return an element of the input domain → carry its logical identity, but only when the output
/// physical exactly equals the input root physical (defensive mirror of ADR 110's implicit-widening reject;
/// guaranteed by SameAsInput for MIN/MAX). All other aggregates compute a new value → NAKED.
fn aggregate_output_logical_type(call: &BoundAggregateCall, output_type: &DataType) -> LogicalTypeId {
    match call.function {
        AggregateFunction::Min | AggregateFunction::Max => {
            let Some(logical) = call.args.root_logical_type(0) else { return LogicalTypeId::NAKED };
            let Some(input_type) = call.args.root_type(0) else { return LogicalTypeId::NAKED };
            if logical.is_naked() || input_type != output_type {
                LogicalTypeId::NAKED
            } else {
                logical
            }
        }
        AggregateFunction::CountAll
        | AggregateFunction::Count
        | AggregateFunction::Sum
        | AggregateFunction::Avg
        | AggregateFunction::LinearRegress
        | AggregateFunction::ApproxQuantile(_) => LogicalTypeId::NAKED,
    }
}
```

In `aggregate_field`, capture the base field, then stamp when the helper returns non-naked:

```rust
fn aggregate_field(call: &BoundAggregateCall) -> Field {
    let base = aggregate_kernel::stable_output_field(call).unwrap_or_else(|| match call.function {
        AggregateFunction::LinearRegress => Field::new("linreg", DataType::Float64, true),
        AggregateFunction::ApproxQuantile(_) => Field::new("quantile", DataType::Float64, true),
        AggregateFunction::CountAll
        | AggregateFunction::Count
        | AggregateFunction::Sum
        | AggregateFunction::Min
        | AggregateFunction::Max
        | AggregateFunction::Avg => unreachable!("stable aggregate metadata missing"),
    });
    let logical = aggregate_output_logical_type(call, base.data_type());
    if logical.is_naked() {
        base
    } else {
        field_with_logical_type_metadata(
            base.name().to_string(),
            base.data_type().clone(),
            base.is_nullable(),
            logical,
        )
    }
}
```

That is the entire production change. Group-key identity stays handled by `schema_for_exprs` in `aggregate_schema`
(089 D6) — untouched.

## Phase 2 — Behavior tests (`tests/catalog_logical_identity_test.rs` + the nominal/identity-flow suite)

Per ADR §Behavior gates: `min_max_preserve_logical_identity`, `grouped_min_max_preserves`,
`scalar_min_max_preserves`, `sum_count_avg_stay_naked`, `min_of_computed_or_cast_is_naked`,
`min_of_ascribed_computed_preserves`, `min_of_naked_column_stays_naked` (regression),
`min_max_identity_flows_through_ctas` (persist + resync), `min_customer_id_vs_max_alt_customer_id_binds`,
`min_customer_id_vs_max_product_id_rejects`. **Retarget** `aggregate_output_stays_naked` (`:392`): split into the
MIN/MAX-preserve cases + the SUM/COUNT/AVG-naked cases (the assertion that *all* aggregate outputs are naked is
no longer true).

## Phase 3 — Architecture guards (`tests/architecture/nominal_type_identity.rs`)

- Add `adr113_min_max_preserve_aggregate_identity` — `aggregate_output_logical_type` reads
  `call.args.root_logical_type(0)` + `call.args.root_type(0)`, preserves under `Min | Max` with the
  `input_type != output_type → NAKED` physical-equality check, keeps the six others naked; `aggregate_field` calls
  `field_with_logical_type_metadata`.
- Add `adr113_aggregate_kernel_stays_physical_only` — `aggregate_kernel.rs` / `exec.rs` / `pbc.rs` write no
  `field_with_logical_type_metadata` / `LogicalTypeId` stamping.
- **Retarget** `adr089_group_keys_preserve_identity_aggregate_outputs_naked` (`:459`) → group keys **and** selector
  aggregates (`Min`/`Max`) preserve; computed/cardinality/statistical aggregates stay naked. Reuse the 089
  `…_logical_metadata_writer_is_centralized` guard (no new emitter).

## Phase 4 — Verification

```sh
cargo fmt
cargo test -p toron --test architecture_test adr113
cargo test -p toron --test architecture_test adr089
cargo test -p toron --test catalog_logical_identity_test
cargo test -p toron --test group_by_test
cargo test -p toron
git diff --check
```

## Acceptance

`MIN`/`MAX` over an identity-bearing root carry that identity into the output schema (grouped + scalar), flowing
through projection / CTAS persist-resync / alias / join-key nominal checks; `SUM`/`COUNT`/`AVG`/computed/cast-
erased/naked inputs stay naked; the physical-equality guard keeps stamping to output-physical = input-physical (089
round-trip holds); the `adr113_*` guards pin MIN/MAX-only stamping + the kernel staying physical-only; the
retargeted 089 guard + behavior test + writer-centralization hold; **no** IR/executor/PBC/Wendelon/storage change;
Toron suite green.
