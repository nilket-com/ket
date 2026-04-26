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
