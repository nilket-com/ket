# No V1/V2 type suffixes

Do not name Rust types, structs, enums, traits, or IR variants
with `V1` / `V2` / `V3` suffixes. Reserve version-byte hedging for
*actual wire-protocol envelopes* — never as a default cosmetic
naming pattern on in-memory types.

## Rule

- New type: `JoinStrategy`, not `JoinStrategyV1`.
- New IR variant: `ExecutionPlan::Sort`, not `ExecutionPlan::SortV2`.
- New enum: `BudgetDimension`, not `BudgetDimensionV1`.
- Wire envelope with a real version field: the field itself can
  be named `protocol_version: u16`, but the Rust struct wrapping
  it does not also inherit a `V1` suffix.

## Why

Suffix-versioning every type pre-allocates namespace for
hypothetical futures that may never need distinct types. It
encourages the sibling anti-pattern of "reserved for V2" /
"follow-up ADR will pin this" hedging in spec text (see
[no-premature-backward-compat.md](no-premature-backward-compat.md)).
Both are forms of treating in-flight design as if it had a
stability contract that has not been earned.

When a real protocol break later requires a distinct shape,
*that* is the right time to introduce a versioned type — and even
then, prefer adding fields, deprecating cleanly, or renaming with
a full migration rather than accumulating `V1` / `V2` / `V3`
namespace siblings.

## Variant naming: family-first ordering, explicit on both sides

Two sub-rules apply when a type, function, or constant has
multiple variants distinguished by a specific property. They
compose: each variant carries its discriminating property
explicitly (be-explicit-on-both-sides) and the family name leads
the identifier (family-first ordering).

### Family-first ordering

Family-first means the algorithm / category / parent-concept name
comes first in the identifier; the specialization / discriminating
property follows.

- ✅ `ExecutionError::BudgetExhausted` — the error family leads,
  the specific condition follows.
- ✅ A hypothetical exchange-mode enum:
  `ExchangeMode::HashPartitioned`, `ExchangeMode::BroadcastReplicated`
  — `Exchange*` family-grouped.
- ❌ `PartitionedHashExchange`, `ReplicatedBroadcastExchange` —
  qualifier-first breaks family-grouped grep and lexical sort.

Three reasons:

1. **Sortable.** Family-first orders specializations together in
   alphabetical sort. Mental hierarchy matches the namespace.
2. **Greppable.** `grep Exchange` finds all exchange variants
   together; qualifier-first scatters the family across the
   alphabet.
3. **Family-context-first reading.** The family name carries
   primary semantic weight; the specialization is the
   differentiator. Reading left-to-right mirrors the call site.

### Be explicit on both sides

When introducing a new variant of an existing family, also rename
the existing baseline so its discriminating property is visible.
Don't leave one side implicit while qualifying the other:

- ❌ `Spill` (implicit) and `SpillEncrypted` (explicit) —
  asymmetric. Reads as "spill vs modified spill" when both are
  spill at different protection grains.
- ✅ `SpillPlaintext` and `SpillEncrypted` — both name the
  variant property explicitly.

Two reasons:

1. **Future-cell-matrix.** As variants proliferate, an
   unqualified baseline becomes ambiguous and forces every future
   variant to carry context the baseline doesn't.
2. **No "default" smuggling.** The implicit-baseline pattern
   suggests one variant is canonical and others are deviations.
   Variants are equals; their names should reflect that.

When this rule requires renaming a previously-shipped variant,
the rename is a mechanical Rust identifier change and bundles
into a cleanup ADR alongside related work.

### Wire tags and discriminants follow the same rule

A serialized discriminant (a `MessageKind` variant name on the
wire, a tagged-enum string) is an external compatibility surface
once published, but the *naming* rule is unchanged: name a new
wire variant by its discriminating property, and rename the
existing one to match — never `*_v1` / `*_v2` ordinals. A wire
tag rename is a format change and follows the clean-break posture
in [no-premature-backward-compat.md](no-premature-backward-compat.md).

## How to apply

- When *creating* a new type, drop the suffix by default.
- When *reviewing* a diff, flag suffix-versioned new types and
  ask whether the suffix is load-bearing or cosmetic. Cosmetic
  suffixes should be removed before merge.
