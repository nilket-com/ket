# No V1/V2 type suffixes

Do not name Rust types, structs, enums, traits, or capability bits with
`V1` / `V2` / `V3` suffixes. Reserve version-byte hedging for *actual
wire-protocol envelopes* — never as a default cosmetic naming pattern on
in-memory types.

## Rule

- New type: `ColumnStatsPolicy`, not `ColumnStatsPolicyV1`.
- New capability: `SlabStatsPruning`, not `SlabStatsPruningV1`.
- New descriptor: `TableSchemaDescriptor`, not `TableSchemaDescriptorV1`.
- Wire envelope with a real version byte: the byte itself can be named
  `format_version: u16`, but the Rust struct wrapping it does not also
  inherit a `V1` suffix.

## Why

Suffix-versioning every type pre-allocates namespace for hypothetical
futures that may never need distinct types. It encourages the
sibling anti-pattern of "reserved for V2" / "follow-up ADR will pin this"
hedging in spec text (see [no-premature-backward-compat.md](no-premature-backward-compat.md)).
Both are forms of treating in-flight design as if it had a stability
contract that has not been earned.

When a real protocol break later requires a distinct shape, *that* is
the right time to introduce a versioned type — and even then, prefer
adding fields, deprecating cleanly, or renaming with a full migration
rather than accumulating `V1` / `V2` / `V3` namespace siblings.

## How to apply

- When *creating* a new type, drop the suffix by default.
- When *reviewing* a diff, flag suffix-versioned new types and ask
  whether the suffix is load-bearing or cosmetic. Cosmetic suffixes
  should be removed before merge.
- A sweeping rename pass to remove existing `V1` / `V2` suffixes from
  the codebase is planned work; treat it as expected, not surprising.

## Variant naming: family-first ordering, explicit on both sides

Two sub-rules apply when a type, function, or constant has multiple
variants distinguished by a specific property. They compose: each
variant carries its discriminating property explicitly (be-explicit-
on-both-sides) and the family name leads the identifier (family-
first ordering).

### Family-first ordering

Family-first means the algorithm / category / parent-concept name
comes first in the identifier; the specialization / discriminating
property follows. Existing precedents in the codebase already follow
this:

- ✅ `EncodingCascade::DictionaryPlain`,
  `EncodingCascade::DictionaryBitPacked` — Dictionary family;
  inner-cascade specialization second.
- ✅ `EncodingCascade::FsstRowSafe` — FSST family; row-safety
  property second.
- ✅ `RowGroupSizingProfile::PointRead`,
  `RowGroupSizingProfile::Scan` — sizing-profile family;
  access-pattern second.
- ❌ `RowSafeFsst`, `WholeBufferFsst` — qualifier-first breaks
  family-grouped grep and lexical sort.

Three reasons:

1. **Sortable.** Family-first orders specializations together in
   alphabetical sort, giving an ISO-date-style read where the more
   specific variant comes later in the string. Mental hierarchy
   matches the namespace.
2. **Greppable.** `grep Fsst` finds all FSST variants together;
   `grep Dictionary` finds all Dictionary variants. Qualifier-first
   scatters the family across the alphabet.
3. **Family-context-first reading.** The family name carries primary
   semantic weight; the specialization is the differentiator. Reading
   `FsstRowSafe` left-to-right mirrors the call site: "this is an
   FSST cascade, specifically the row-safe variant." Qualifier-first
   inverts the natural conceptual flow.

### Be explicit on both sides

When introducing a new variant of an existing family, also rename
the existing baseline so its discriminating property is visible.
Don't leave one side implicit while qualifying the other:

- ❌ `Fsst` (implicit) and `FsstRowSafe` (explicit) — asymmetric.
  Reads as "FSST vs modified FSST" when in fact both are FSST
  applied at different grains.
- ✅ `FsstWholeBuffer` and `FsstRowSafe` — both name the variant
  property explicitly. Reads as "FSST applied at two different
  grains, each named explicitly."

Two reasons:

1. **Future-cell-matrix.** As variants proliferate, an unqualified
   baseline becomes ambiguous. Today's two FSST variants foreshadow
   a four-cell matrix once page-grain FSST ships (whole-buffer ×
   row-safety / page-grain × row-safety). An unqualified `Fsst`
   forces every future variant to carry context the baseline doesn't,
   propagating the asymmetry.
2. **No "default" smuggling.** The implicit-baseline pattern
   suggests one variant is canonical and others are deviations.
   Variants are equals; their names should reflect that. The cascade
   catalog is a closed-by-addition catalog (per ADR 057's discipline);
   no variant deserves preferred-name status.

When this rule requires renaming a previously-shipped variant, the
rename is mechanical (Rust identifier change with no on-disk
discriminant change required) and bundles into a layout-cleanup ADR
alongside related cleanup work.

### On-disk magic bytes follow the same rule

Frame magic bytes are bytes, not Rust identifiers, so they live
under a different constraint (industry convention: short, pronounceable
4-byte codes). But the same be-explicit-on-both-sides rule applies:
when a new frame variant ships, name its magic to reflect the
discriminating property, and rename the existing frame's magic
correspondingly:

- ❌ `b"WFS1"` and `b"WFS2"` — version ordinals; rotting under
  forward extension.
- ✅ `b"WBFS"` (Whole-Buffer FSST) and `b"RSFS"` (Row-Safe FSST) —
  both name the encoding grain; future page-grain variants take
  similarly-shaped codes.

Magic byte renames are on-disk format changes; they require a
clean format-version break per `no-premature-backward-compat.md`
(pre-launch posture: old binaries reject the new magic, new
binaries reject the old magic, no tolerant decoder).
