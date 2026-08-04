# ADR 087: Nominal Type Identity Spine

**Status:** Promoted (plan). **Genuine 3-of-3 (Codex, Claude, Gemini)** — verified
against the latest peer drafts (Codex iter-3, Claude iter-2, Gemini iter-2). The
brainstorm's full `DataType` replacement was scoped down to a sidecar; the
one-side-cast bypass resolved to the strict rule (Gemini conceded);
`NominalTypeMismatch`, the shared gate helper,
the sidecar-ripple, the serialized-payload caveat, and the literal-preservation detail
are all unanimous.
**Category:** Toron interior substrate / type system, piloted via Rune. **Substrate
leap** — the first IR + binder cut since the Rune authoring run (079–086).
**Scope:** Toron-only; `ir.rs`, `ir/execution.rs`, `binder.rs`, `error.rs`,
`rune_engine.rs` + tests. **No Arrow `DataType` replacement, no Wendelon wire/storage,
no SQL DDL, no executor kernel or PBC opcode change.** *(Nuance: the Toron-internal
coordinator→worker plan payload gains a field — same-version Toron assumed.)*

## Doctrine — database `PhantomData<T>`

Logical identity is a **`PhantomData`-like sidecar over physical Arrow layout.** The
relational analogue of Rust's newtype:

```rust
struct Id<T> { raw: i64, _marker: PhantomData<T> }   // Id<Customer> != Id<Product>
```

becomes, in Toron:

```
physical: Int64        // the layout — unchanged, what executes
logical:  CustomerId   // the phantom marker — what the binder type-checks
```

The runtime representation stays `Int64` — **no executor cost, no Arrow layout change,
no Wendelon storage change.** The **binder becomes the type checker**: it refuses to
mix `Id<Customer>` and `Id<Product>` even though both are physically `Int64`. And
`cast(type_i64())` is the explicit **"erase the phantom marker"** operation — the
deliberate, source-visible escape hatch. This moves Nilket from a bucket-of-bits into
a statically-protected relational engine, catching a class of data-modeling bugs at
plan-authoring time, before a row is read.

## Position on the type ladder

Four terms get conflated — nominal, logical, refinement, dependent — but they are not
synonyms, and only three are *rungs*. There are two orthogonal axes:

- **Layering (where a type lives): physical vs logical.** Physical = the Arrow bit
  layout (`Int64`). *Logical* = a semantic meaning mapped onto a physical layout —
  Arrow's own word (a `Timestamp` is logically a timestamp, physically an `Int64`). Our
  `LogicalTypeId` sidecar lives on this axis: it is the **mechanism**, not a behavior.
- **Power (how much a type constrains): nominal → refinement → dependent.** The ladder,
  in increasing strength:
  - **Nominal** *(ADR 087, here)* — identity by name. `PersonId` ≠ `ProductId` though
    both are `Int64`; the newtype/"who." Decides *compatibility*.
  - **Refinement** — a base type + a value-predicate, `{ PersonId | value > 0 }`.
    Decides *which values are admissible*, and lets the binder statically prove
    `WHERE person_id = -5` is empty (zero I/O). A *restricted* dependent type.
  - **Dependent** — a type indexed by a runtime value (e.g. a column whose physical
    type is chosen by another column's value); the far horizon. Refinement ⊂ dependent.

So ADR 087 delivers **nominal** typing, implemented *via* a **logical-type** sidecar —
the behavior vs the mechanism. That is precisely why "nominal" and "logical" are not the
same word, and why "logical" is not itself a rung: it is the layer the rungs are built on.

**Why this baby step, in this order:** you cannot hang a refinement on a raw physical
type — `Int64 WHERE value > 0` would break every count, offset, and id at once. The
contract needs a *named handle* to attach to. ADR 087 mints that handle
(`LogicalTypeId` → `PersonId`); refinement then hangs `value > 0` on it; dependent types
are the horizon past that. We build the spine today so we can do the math tomorrow. Full
vision: `plans/brainstorms/007_nominal_type_identity.md`.

## Summary

Toron treats physically-equal IDs as compatible, permitting structural-oops relations
(`CustomerId(Int64) = ProductId(Int64)`). ADR 087 adds nominal identity as a
**binder-level contract**: a `LogicalTypeId` sidecar travels beside physical Arrow
types in bound programs; `Expr::LogicalAscription` ascribes identity at bind time (and
**disappears into sidecar metadata — no runtime node**); comparison, `IN`, and
equi-join binding reject physically-equal-but-logically-distinct operands. Pilot
surface: `col("customer_id").as_logical_type::<CustomerId>()`.

## Grounding ledger (verified)

- **Turbofish surface is real** — `rune::Params` exists (`rune-0.14.1/src/params.rs`);
  `.method::<T>()` is in rune's own stdlib (`collect::<Vec<i64>>`); `external_generic.rs`
  proves `derive(Any)` markers + per-instantiation `associated_function`.
  `Params::new("as_logical_type", [CustomerId::HASH])` registers the surface.
- **Binder insertion points exist** — `bind_comparison` (`binder.rs:3130`) +
  `coerce_comparison_literals` (`:3221`); `bind_in_list`; `bind_join_key_pair`
  (`:1844`) + `validate_join_key_types` (`:1904`); execution stores only
  `left_keys`/`right_keys` ordinals (`:557-582`).
- **The sidecar ripple is the implementation bulk** (all anchors verified) — the
  `node_logical_types` parallel-vector invariant must be preserved through
  `BoundExprProgramBuilder::build` (`execution.rs:478`), `::empty` (`:504`),
  `::validate` (`:599`), `clone_bound_subgraph` (`binder.rs:1369`),
  `project_program_roots` (`:2414`), `rewrite_program_ordinals` (`:2511`),
  `merge_aggregate_args` (`:2614`), and direct `BoundExprProgram { … }` construction.
- **Serialized-payload caveat (verified)** — `BoundExprProgram` is `Serialize/Deserialize`
  (`execution.rs:98`) and crosses the coordinator→worker boundary
  (`distributed/task.rs:174`). Adding `node_logical_types` is a Toron-internal plan
  payload change (not a Wendelon wire change), assuming **same-version Toron**; a
  `serde(default)` can't rescue mixed versions (the arity invariant would fail on
  legacy payloads).

## Decisions (12, promotion-locked)

**D1** `LogicalTypeId(u32)` (NAKED=0) sidecar — the phantom marker. **D2**
`LogicalTypeSpec { id, name, physical }` static pilot registry (`CustomerId(Int64)`=1,
`ProductId(Int64)`=2; dynamic/catalog markers deferred). **D3** keep Arrow `DataType`
physical (no replacement). **D4** `Expr::LogicalAscription { expr, logical_type }` =
bind-time annotation: bind inner → require `inner_physical == spec.physical` → set the
bound root's sidecar → same physical root; **no `BoundExprNode`, no executor branch,
no PBC opcode.** **D5** `BoundExprProgram.node_logical_types` + `NodeLogicalTypeArityMismatch`
invariant + NAKED defaults; preserved through the ripple sites (above). **D6 output
identity policy:** new nodes naked; `cast`/`try_cast` output naked (the marker-erase
escape hatch); **implicit literal coercion preserves the node's existing identity** (a
naked literal stays naked; an ascribed literal stays ascribed — Codex iter-3 subtlety);
boolean/comparison/`IN`/`LIKE`/null/time-bucket outputs naked; projection/schema
propagation deferred. **D7 strict nominal rule** (after physical coercion + equality):
naked/naked OK; same-non-naked OK; non-naked vs naked **literal** OK; different
non-naked → `NominalTypeMismatch`; **non-naked vs naked-non-literal → `NominalTypeMismatch`**
(blocks one-side-cast bypass — the escape hatch is relationship-explicit, both sides).
**D8** `BindingError::NominalTypeMismatch { left, right, physical }` (distinct from
physical `TypeMismatch`). **D9** one shared `nominal_compatible` helper reused by
comparison/`IN`/join (no policy drift). **D10** `ResolvedJoinKey { column, logical_type }`
join gate; `bind_join_key_pair` peels `LogicalAscription(Column, id)`; other computed
keys still rejected; execution stores key ordinals only. **D11** Rune turbofish
`.as_logical_type::<T>()` via `rune::Params` host-marker registration (no string/parser/
normalizer — 081/085 doctrine). **D12** behavior-additive: the gate engages only when a
non-naked identity is present; **every existing all-naked SQL/Rune query is unaffected.**

## Behavior gate (13)

turbofish binds vs literal; same-identity binds; different-identity →
`NominalTypeMismatch`; logical-vs-naked-non-literal rejects; physical-mismatch rejects
at ascription; **both-side cast binds** (marker erased on both); **one-side cast still
rejects** (the resolved strict rule); `IN` same/different identity; **ascribed-literal
identity preserved through implicit coercion** (the D6 subtlety); two pure hand-built
`LogicalPlan::Join` fixtures (mismatched → reject, matching → bind); **all-naked
SQL/Rune nets stay green** (the behavior-additivity safety proof).

## Architecture Guards (10)

sidecar-not-Arrow-replacement; bound-program logical-arity invariant;
`LogicalAscription`-has-no-runtime-node; sidecar-preserved-through-clone-and-merge
(the ripple guard); nominal-comparison-gate-after-coercion; nominal-`IN`-reuses-shared-helper;
nominal-join-key-gate-extends-existing; dedicated-`NominalTypeMismatch`;
Rune-surface-uses-`Params`-not-strings; cast-outputs-naked.

## Council History

iter-0: brainstorm 007 (Gemini) proposed nominal identity via full `DataType`
replacement; **Codex scoped it to a sidecar; Claude verified the turbofish, flagged
the `BoundExprProgram` ripple as the bulk, and proposed `NominalTypeMismatch`.**
iter-1: **Gemini conceded the sidecar + the one-side-cast strict rule.** iter-2:
**Claude verified the serialized-plan-payload caveat.** iter-3: Codex added the
literal-identity-preservation subtlety; genuine 3-of-3. The
`PhantomData`-over-physical-layout framing (operator) is the doctrine.

## What to do next

*(Per the roadmap convention: this section is the next-topic source.)*

- **Catalog-carried column identity** — let table columns declare a `LogicalTypeId` in
  their descriptor so identity flows from the source, not just explicit ascription.
  The natural successor: makes the gate protect un-annotated queries.
- **Projection / CTAS identity propagation** — carry the sidecar through outer plan
  nodes + materialized outputs (needs a typed-schema or Arrow field-metadata decision).
- **Refinement contracts** — the `logical_id` becomes a registry key for value
  predicates (`PersonId(Int64, value > 0)`), enabling static plan elision
  (`WHERE person_id = -5` → `EmptyRelation`, zero I/O) — the brainstorm's long horizon.
- **SQL nominal surface** — `CAST(id AS CustomerId)` / domain-style DDL once the Rune
  pilot proves the spine.
- **`.as_naked()`** explicit Rune helper (if `cast`-to-erase proves awkward); dynamic
  tenant/catalog marker registration.
- Non-Rune lead carried: **residual minimization for factored shapes** (078 follow-on).

## Promotion criteria

Unanimous D1–D12; strict one-side-cast rule + literal-preservation; static marker
pilot; the sidecar ripple preserved (arity invariant + clone/merge guard); dedicated
error; turbofish via `Params`; 13 fixtures (incl. both-side-binds / one-side-rejects /
literal-preserved / pure-join / all-naked-green); no Arrow-replacement / Wendelon /
executor / PBC change; serialized-payload same-version assumption recorded. Migration:
`plans/impl/0087_nominal_type_identity_spine.md`.
