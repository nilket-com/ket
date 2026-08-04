# ADR 113: Aggregate-Output Logical-Identity Preservation

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini)** on the partition; the `SUM` sub-decision
was a grounded 2-of-3 (Claude + Codex) that Gemini conceded at iter-2 → now unanimous. **Toron-only, binder
schema construction; zero IR/executor/PBC/Wendelon/storage/kernel change.** **Predecessors:** 087 (sidecar), 088
(catalog-carried identity + fail-closed decode), 089 (schema-metadata propagation, the
`field_with_logical_type_metadata` writer, the round-trip invariant), 110 (CASE identity + implicit-widening
reject). **Migration:** `plans/impl/0113_aggregate_output_logical_identity.md`.

## Doctrine — selection preserves identity; computation erases it

After 087–110 (identity through columns, casts, comparisons, CASE) and 112 (NULL typed late), the **last big
identity-flow leak** is aggregate outputs: `aggregate_field` emits every aggregate result NAKED (089 left it so
deliberately), so `MIN(customer_id)` drops `CustomerId` exactly when a query rolls a domain up. The rule that
closes it in one total partition:

> **`MIN`/`MAX` return an actual element of the input domain** — `MIN(customer_id)` *is* a `CustomerId` (or NULL,
> still a `CustomerId`), so they **preserve** identity. **`SUM`/`AVG`/`COUNT`/`COUNT(*)`/`LinearRegress`/
> `ApproxQuantile` compute a new value** (an accumulation, statistic, or cardinality), and identity-closure under
> those operations is **not a declared property** of a logical type, so they emit **NAKED**.

Total over all 8 `AggregateFunction` variants (`ir.rs:591`), no per-type judgment:

| Preserve input identity | NAKED |
|---|---|
| `Min`, `Max` | `CountAll`, `Count`, `Sum`, `Avg`, `LinearRegress`, `ApproxQuantile` |

## Scale boundary

**In:** stamp `MIN`/`MAX` output fields with the input argument's logical identity, via the existing 089 writer,
in the single `aggregate_field` schema path (grouped + scalar). **Out — genuine boundaries, not granularity:**
`SUM`/`AVG` over a *declared-additive/unit* logical type (needs a new "additive" property — a focused follow-on);
contextless NULL substrate; `NVL2`. **No** IR variant, executor, PBC, Wendelon, storage, or `aggregate_kernel`
identity logic (the kernel stays physical-only; the binder owns identity, per 087/088).

## Grounding (verified)

- **Single schema path:** `aggregate_schema` → `aggregate_field(call)` (`binder.rs:5620`/`:5688`) builds every
  aggregate output field, for grouped (`binder.rs:175`, `:3849`) and scalar/global aggregates. Stamping there
  covers all cases. Group keys already preserve via `schema_for_exprs(group_expr)` (089 D6).
- **Identity is reachable:** `BoundAggregateCall { function, args: BoundExprProgram }`; `call.args.root_logical_type(0)`
  is `CustomerId` for `MIN(customer_id)`, NAKED for `MIN(customer_id + 0)` (084/087 erases) or
  `MIN(CAST(x AS BIGINT))` (087 cast-erase).
- **No widening conflict:** `MIN`/`MAX` are `SameAsInput` (`aggregate_kernel.rs:82`/`:92`); `stable_output_data_type`
  returns the input physical unchanged (`:171`). Output physical **equals** input physical → identity rides with
  no widening, and the **089 round-trip invariant holds** (the emitted field decodes via 088 because its physical
  matches the registry).
- **`SUM` is computation, not selection:** `Sum` is `SumNumeric` and may widen (`Int32` → `Int64`, `:172`), so
  any physical-preservation rule would be partial even before the semantic issue. Even when input/output
  physical types match (`Int64` → `Int64`), `SUM(x)` computes a new value rather than selecting one input-domain
  value, so it stays NAKED.
- **089 writer:** `field_with_logical_type_metadata(name, data_type, nullable, logical_type)` is the existing sole
  emitter; aggregate identity reuses it (no new writer — the 089 centralization guard holds). `stable_output_field`
  returns a plain `Field::new(...)` (physical-only); identity is added in the binder.
- **Existing pins to retarget:** `adr089_group_keys_preserve_identity_aggregate_outputs_naked`
  (`nominal_type_identity.rs:459`) and the behavior test `aggregate_output_stays_naked`
  (`catalog_logical_identity_test.rs:392`).

## Decisions (promotion-locked)

**D1 — Partition (the whole decision).** `Min`/`Max` preserve the input argument's logical identity; the six other
variants emit NAKED (unchanged). Doctrine = selection-preserves / computation-erases.

**D2 — Mechanism: a named binder helper.** `aggregate_output_logical_type(call: &BoundAggregateCall, output_type:
&DataType) -> LogicalTypeId` reads `call.args.root_logical_type(0)` and `call.args.root_type(0)`; returns the input
identity for `Min`/`Max` when it is non-naked **and** `output_type` equals the input root physical; NAKED for all
other variants and all other cases. `aggregate_field` calls it after `aggregate_kernel::stable_output_field`, and
stamps the field via `field_with_logical_type_metadata` when the result is non-naked. One helper, one call site;
covers grouped + scalar.

**D3 — Physical-equality defensive guard.** Stamp only when the aggregate output physical *exactly* equals the
input root physical — a mirror of 110's refusal to preserve identity across implicit widening. For `MIN`/`MAX`
(`SameAsInput`) this is guaranteed by construction, but the explicit check future-proofs the rule and makes the
"no widening" property local to the helper.

**D4 — `SUM` stays naked (grounded sub-decision).** Physical sameness is not nominal identity: `SUM(customer_id)`
is a computed total, not a selected `CustomerId`. 110 preserved identity through CASE because CASE *selects* a
branch value; it did not bless arithmetic over an identity domain. A future additive-unit doctrine could add
`SUM(Money) -> Money`; Toron has no such property today. (Gemini's iter-1 "preserve SUM when not widened" was
resolved against on this grounding; `SumNumeric` widening independently confirms that physical sameness cannot
be the aggregate identity rule.)

**D5 — Computed/naked inputs naturally naked; ascription is the escape hatch.** `MIN(amount)` (naked),
`MIN(customer_id + 0)`, `MIN(CAST(x AS BIGINT))` → arg root identity is NAKED → no stamp. `MIN(CAST(customer_id +
0 AS CustomerId))` carries `CustomerId` (the author restored identity explicitly). No special-casing.

**D6 — Downstream flow inherits the schema rule.** Once a selector-aggregate output field carries metadata,
projection / alias column lists / CTAS / derived-table binding use the existing 088/089 decode-reemit loop. No
separate derived-table rule. Group-key behavior unchanged (089 D6).

**D7 — Behavior-additive, Toron-only.** All-naked queries unchanged; no IR/executor/PBC/Wendelon/storage/kernel
change; no new 089 writer.

## Behavior gates

- `min_max_preserve_logical_identity` — `MIN(customer_id)`/`MAX(customer_id)` output fields carry `CustomerId`;
  `MIN(product_id)` carries `ProductId`.
- `grouped_min_max_preserves` / `scalar_min_max_preserves` — both `aggregate_field` paths.
- `sum_count_avg_stay_naked` — `SUM(customer_id)` (input and output both `Int64`), `COUNT(customer_id)`,
  `COUNT(*)`, `AVG(customer_id)` → naked.
- `min_of_computed_or_cast_is_naked` — `MIN(customer_id + 0)`, `MIN(CAST(customer_id AS BIGINT))` → naked.
- `min_of_ascribed_computed_preserves` — `MIN(CAST(customer_id + 0 AS CustomerId))` → `CustomerId` (D5 escape).
- `min_of_naked_column_stays_naked` — regression.
- `min_max_identity_flows_through_ctas` — `CREATE TABLE hi AS SELECT MAX(customer_id) AS m …` persists + resync
  round-trips `CustomerId` (089 invariant); derived alias-column rename preserves it.
- `min_customer_id_vs_max_alt_customer_id_binds` — both `CustomerId` → a post-aggregate comparison binds (the
  positive cross-check).
- `min_customer_id_vs_max_product_id_rejects` — post-aggregate `MIN(customer_id) = MAX(product_id)` rejects
  `NominalTypeMismatch` (the integrity payoff).

## Architecture guards

- `adr113_min_max_preserve_aggregate_identity` — `aggregate_output_logical_type` reads
  `call.args.root_logical_type(0)` + `call.args.root_type(0)`, preserves under `Min | Max` with the physical-equality
  check, keeps the six others naked; `aggregate_field` stamps via `field_with_logical_type_metadata`.
- `adr113_aggregate_kernel_stays_physical_only` — `aggregate_kernel.rs`, `exec.rs`, `pbc.rs` write no logical
  metadata.
- **Retarget** `adr089_group_keys_preserve_identity_aggregate_outputs_naked` → group keys **and** selector
  aggregates preserve; computed/cardinality/statistical aggregates stay naked (discipline #4). Reuse the 089
  writer-centralization guard (no new emitter). 087/088/110 guards unchanged.

## Council history

iter-0: aggregate-identity = Claude + Gemini lean (2-of-3); Codex leaned contextless-NULL and deferred aggregate-
identity ("SUM/COUNT need doctrine"). iter-1: Claude resolved the objection with the selection-vs-computation
partition (SUM→naked answers the `SUM(CustomerId)` worry); operator locked the topic. iter-2: genuine 3-of-3 on
the partition; the `SUM` sub-decision (Claude + Codex reject Gemini's iter-1 preserve-when-not-widened) became
unanimous when Gemini conceded on the grounding. The physical-equality defensive guard (D3) and the named
`aggregate_output_logical_type` helper (D2) came from the Codex/Gemini iter-2 convergence. No open fork.

## What to do next

`SUM`/`AVG` over a declared-additive/unit logical type (the deferred half); contextless NULL substrate
(`DataType::Null`/`NullArray`); `NVL2`; composable derived `ORDER BY`/`LIMIT` + Rune sort/limit; logical-type
`DROP`/`ALTER`; information_schema constraints/views (once catalog modeling exists).

## Promotion criteria

`MIN`/`MAX` over an identity-bearing root carry that identity into the output schema (grouped + scalar), flowing
through projection / CTAS persist-resync / alias / join-key nominal checks; `SUM`/`COUNT`/`AVG`/computed/cast-
erased/naked inputs stay naked; output physical = input physical (089 round-trip holds); the `adr113_*` guards
pin MIN/MAX-only stamping + the kernel staying physical-only; the retargeted 089 guard + writer-centralization
hold; **no** IR/executor/PBC/Wendelon/storage change; targeted + Toron suite green. Migration:
`plans/impl/0113_aggregate_output_logical_identity.md`.
