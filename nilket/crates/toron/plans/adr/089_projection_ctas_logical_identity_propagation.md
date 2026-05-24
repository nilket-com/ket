# ADR 089: Projection / CTAS Logical Identity Propagation

**Status:** Promoted (plan). **Genuine 3-of-3 (Codex, Claude, Gemini)** — verified
against the latest peer drafts (Codex iter-2, Claude iter-1, Gemini iter-1). The writer
mechanism, four production writer call sites plus aggregate group-key propagation, the
explicit-computed escape, exact CTAS `IF NOT EXISTS` equality, and the idempotent-re-issue
round-trip requirement are all unanimous.
**Category:** Toron interior substrate / binder→schema type-system continuation.
**Direct successor to ADR 088** — closes the identity loop so nominal protection survives
multi-hop pipelines.
**Scope:** Toron-only; `ir.rs` (writer), `ir/execution.rs` (`schema_for_exprs` + root
accessor), `binder.rs` (named/pruned/CTAS rebuilds), tests. **`coordinator.rs` unchanged.
No Wendelon/wire/storage, no SQL DDL/domain, no executor kernel or PBC opcode, no Rune
surface, no dynamic registry.**

## Doctrine — 088 and 089 are inverse halves of one isomorphism

Logical identity has two encodings — the `node_logical_types` **sidecar** (inside a bound
program) and **Arrow field metadata** (on a schema) — and the binder converts between them
at layer boundaries:

```
088:  field metadata  --decode_column_logical_type-->       sidecar         (at resolve_column)
089:  sidecar         --field_with_logical_type_metadata-->  field metadata  (at schema_for_exprs)
```

088 reads identity *in* at the source; 089 writes it *out* at the projection boundary so the
next hop's `resolve_column` (088) reads it back. The loop closes; multi-hop pipelines work
(`SELECT customer_id AS cid …` then a downstream `cid = product_id` rejects). This is a
**schema-construction writer, not expression-graph propagation and not lineage inference** —
"computed stays naked" is automatic because a computed root's sidecar is NAKED (087 D6), so
the writer emits nothing; only direct columns / explicit ascriptions carry a non-naked
sidecar to emit.

## Summary

A single central writer turns a bound expression root's logical sidecar back into the
reserved Arrow field-metadata key 088 reads. Projection, aliasing, pruning, aggregate group
keys, and CTAS-created descriptors preserve identity by carrying the root sidecar to the
output field; aggregate results and unascribed computed expressions stay naked.

## Grounding ledger (verified)

- **The production writer call sites are the `Field::new` rebuilds 088 stopped at.**
  `schema_for_exprs` (`ir/execution.rs:792`) builds output fields with `Field::new` from
  `root_type`/`root_nullable`/`root_display_name` (it already has those accessors);
  `schema_for_exprs_with_output_names` (`binder.rs:3559`),
  `schema_for_projected_exprs_with_existing_names` (`:3582`), and `infer_ctas_schema`
  (`:760`) all rebuild with bare `Field::new`. `schema_for_exprs` has exactly **two**
  callers (a derived projection schema + `aggregate_schema`'s group schema) — contained
  blast radius.
- **Aggregate group-key preservation is automatic.** `aggregate_schema` (`:3544`) builds
  group fields via `schema_for_exprs(group_expr)` then appends `aggregate_field(call)`
  (`:3610`). Once `schema_for_exprs` emits (D3), group keys carry identity with **no change
  to `aggregate_schema`**, and `aggregate_field` stays untouched → aggregate outputs naked.
- **Round-trip invariant (safety property).** Every non-naked field 089 emits *round-trips
  cleanly* through 088's `decode_column_logical_type` — 089 can never create a mid-pipeline
  fail-closed. A node's sidecar is non-naked only via the validated column-stamp (physical =
  registry physical) or a validated ascription (`validate_logical_ascription_physical` ran
  before the set), so the emitted field's `data_type` always equals the registry physical and
  the decoder's physical-match check always passes.
- **CTAS exact-equality round-trip.** `coordinator.rs:1106` gates `CTAS IF NOT EXISTS`
  idempotency with `existing.schema.as_ref() == schema.as_ref()` (Arrow `Field` equality
  compares metadata as an order-independent map). Keeping it exact (D12) makes idempotency
  depend on the persist→decode path reproducing exactly `{reserved_key: "<id>"}` — pinned by
  the idempotent-re-issue fixture.

## Decisions (12, promotion-locked)

**D1** one central `field_with_logical_type_metadata(name, data_type, nullable, logical_type)
-> Field` beside the 088 decoder; emits the reserved key only for non-naked, plain
`Field::new` for naked; the **only** production writer of `LOGICAL_TYPE_ID_METADATA_KEY`.
**D2** read identity from the bound root sidecar only (`BoundExprProgram::root_logical_type(idx)`
= `node_logical_type(roots[idx])`); no names/lineage/child-traversal/descriptor-lookup. **D3**
`schema_for_exprs` builds each field via the writer with the root sidecar. **D4**
`schema_for_exprs_with_output_names` renames but carries the root sidecar. **D5**
`schema_for_projected_exprs_with_existing_names` preserves selected-root identity (scan-projection
/ pruning path). **D6** aggregate group keys inherit D3 (no second emission path); aggregate
outputs stay naked (`aggregate_field` unchanged). **D7** `infer_ctas_schema`
`decode_column_logical_type(source_field)` then re-emit via the writer — reserved key only, no
blanket metadata clone. **D8** RETURNING result schemas may carry identity (shared helper —
acceptable); DML target/replacement schemas unchanged. **D9** explicit ascription is the only
computed escape hatch (no `x + 0` inference; do not suppress an explicitly-set computed
sidecar). **D10** no SQL DDL/Rune/Wendelon/executor/wire-PBC/dynamic-registry change. **D11**
retarget 088's no-writer guard (below). **D12** CTAS `IF NOT EXISTS` keeps exact logical-schema
equality (`coordinator.rs:1106` unchanged) — a physically-compatible but **naked** existing
target is *not* compatible with an identity-bearing CTAS schema → `SchemaMismatch`.

## CTAS `IF NOT EXISTS` — exact equality (resolved)

Claude's iter-0 floated "physical equality + per-column logical reconcile"; **withdrawn and
ratified to exact-equality.** Reconcile would force one of two dishonest outcomes against a
naked existing target — *mutate* the existing descriptor during a supposed no-op (a write
outside 089's scope), or *silently accept* that the requested logical contract was never
installed. Exact equality is the honest answer and is consistent with 088 (logical identity
is already part of the schema/fingerprint). The cost: idempotency now depends on round-trip
metadata fidelity, so an identity-bearing CTAS must round-trip through persistence without
perturbing the reserved-key entry-set (fixture 12). If persistence ever injects/strips a
field-metadata entry, the comparison must be normalized to the reserved key — but that is to
be grounded, not assumed.

## Behavior gate (15)

1. projection alias preserves identity (`customer_id AS cid`; `cid = raw_id` rejects);
2. same-identity alias comparison binds; 3. different-identity alias rejects;
4. computed projection stays naked (`customer_id + 0 AS v`); 5. explicitly-ascribed computed
projection carries identity; 6. group key preserves identity; 7. aggregate output stays
naked; 8. project-pruning preserves identity; 9. CTAS direct projection materializes identity
into the created descriptor; 10. CTAS computed projection stays naked in the descriptor;
**11. CTAS `IF NOT EXISTS` over a physically-compatible naked target → `SchemaMismatch`**;
**12. identical identity-bearing CTAS `IF NOT EXISTS` re-issue is a no-op** (round-trip
fidelity); 13. CTAS-created logical projection rebinds as a source without fail-closed decode;
**14. round-trip-no-fail-closed** (alias-preserved identity binds downstream — the invariant);
15. all-naked projection/group-by/CTAS/Rune/lib suites stay green.

## Architecture Guards

`adr089_logical_metadata_writer_is_centralized`; `..._schema_for_exprs_writes_root_sidecar`;
`..._named_projection_preserves_metadata_on_rename`; `..._project_pruning_preserves_root_identity`;
`..._group_keys_preserve_identity_aggregate_outputs_naked`;
`..._ctas_reemits_reserved_logical_metadata_only`; `..._ctas_if_not_exists_exact_equality_policy`
(`coordinator.rs:1106` unchanged); `..._no_lineage_or_name_heuristic_inference`. The
one-writer guard scans **production `crates/toron/src/**` only** — test fixtures / catalog
helpers may still author Arrow metadata to simulate persisted 088 descriptors, and the guard
must not ban that (Codex iter-2 R4). Carry 087/088 guards (sidecar arity, `ResolvedColumn`
physical-only, no `TableBinding` vector, single `nominal_compatible`, single
`reconcile_identity`, fail-closed decode).

## Council History

iter-0: all three lock the topic (Codex seed). **Claude frames the 088⇄089 isomorphism + the
round-trip invariant + flags the CTAS-`IF NOT EXISTS` schema-equality interaction.** iter-1:
Codex pins exact-equality with a grounded honesty argument; **Claude concedes the reconcile
route and retargets the verify-first to the round-trip-fidelity risk exact-equality
introduces, adding the idempotent-re-issue fixture**; Gemini adopts the isomorphism doctrine.
iter-2: converged — Codex ratifies (R1), folds the idempotent-re-issue fixture (R2), confirms
automatic group-key (R3), and scopes the one-writer guard to production (R4).

## What to do next

*(Per the roadmap convention: this section is the next-topic source.)*

- **ADR 090 — SQL / domain authoring + dynamic registry** (the direct successor). Declare
  logical identity through SQL (`CAST(id AS CustomerId)` / domain-style DDL) instead of
  test-built Arrow metadata, backed by a dynamic/tenant marker registry (087–089 use the
  static 2-entry registry). This is what makes catalog identity *authorable* in production,
  the last gap before identity is a first-class, user-declared contract.
- **ADR 091+ — Refinement contracts.** With a durable, *propagating* handle (088 minted it,
  089 makes it flow), attach value predicates to a `LogicalTypeId` (`PersonId(Int64, value >
  0)`) → static plan elision (`WHERE id = -5` → `EmptyRelation`). The brainstorm-007 horizon.
- **Aggregate-output identity semantics** — should `MIN(customer_id)`/`MAX` carry `CustomerId`
  (a representative of the set) while `SUM`/`COUNT` stay naked? 089 keeps all aggregate
  outputs naked; this is the natural follow-on once propagation is proven.
- **Carried Rune leads:** Rune `Like`/`ILike`; Rune joins (relation-alias + qualified-column).
- **Non-Rune lead:** residual minimization for factored shapes (078 follow-on).

## Promotion criteria

One centralized production writer (test fixtures exempt); projection/alias/pruning/group-key
identity survives rebinding; computed stays naked unless explicitly ascribed; CTAS
materializes direct/ascribed identity, drops computed-naked; CTAS-IF-NOT-EXISTS exact-equality
with the idempotent-re-issue round-trip fixture green; 088 no-writer guard retargeted honestly;
`coordinator.rs` + Wendelon/executor/PBC/DDL/Rune unchanged; 15 fixtures + all-naked suites
green. Migration: `plans/impl/089_projection_ctas_logical_identity_propagation.md`.
