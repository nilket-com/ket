# No premature backward-compat

Do not add backward-compatibility scaffolding to Toron — neither
in code (versioned compat structs, tolerant decoders, legacy-
decode tests, migration shims) nor in spec text (ADR phrases like
"reserved for V2", "follow-up ADR will pin this", "writers must
not emit X yet"). Both shapes treat in-flight design as if it had
an external stability contract.

## Rule

- When a shape changes — an IR variant, a `MessageKind`, a
  `TaskSpec` field, a wire envelope — just change it. Delete the
  old shape rather than wrapping it in a `PreFooCompat` struct or
  keeping a retired variant as a reserved slot.
- If a test exists only to decode an old wire shape, delete the
  test along with the old shape.
- When reviewing an ADR or wire-format spec, treat phrases like
  "reserved for V2", "follow-up ADR will pin this", or "V1
  writers must not emit" as design smells. Push back and ask
  whether the design can collapse the special case now — pin the
  encoding, delete the reserved slot, or unify with an existing
  case.
- Do not reserve placeholder fields, message slots, enum
  variants, `JoinKind`s, or `BudgetDimension`s for deferred
  features. The future ADR adds the surface when it pins the
  contract.

## Why

Backward-compatibility scaffolding only earns its keep when there
is deployed state to preserve or external consumers to honor.
Carrying it ahead of those constraints adds surface area, splits
codepaths, and ossifies design choices that the compat shim then
quietly defends against being revisited.

The same logic applies at the spec layer: reserving tag-numbers,
field slots, or enum variants for a hypothetical follow-up ADR is
the design-text equivalent of a compat struct. It encodes a
future that may never need its own representation, and the
reservation itself becomes a constraint future ADRs feel obliged
to honor.

## Examples already in the codebase

Toron applies this rule rather than aspiring to it:

- The legacy `ToronMessage` wire wrapper was **retired**, not
  kept as a compat shape; `tests/architecture_test.rs`
  (`wire_contract_has_no_legacy_toron_message_wrapper`) keeps it
  gone.
- A retired `MessageKind` variant was **removed**, not left as a
  reserved slot; the architecture test
  (`message_kind_retired_morsel_complete`) pins the removal.
- ADR 008 shipped at a single workspace version bump with
  full-fleet upgrade — *"mixed old/new binary support explicitly
  not promised"* — rather than carrying a serde fallback variant
  for rolling upgrades.

## When this changes

Revisit when there is real deployed state to preserve or named
external consumers depending on a stable shape — a published
Toron SDK, a long-lived persisted spill format, a cross-version
cluster contract. At that point the discipline inverts: changes
need explicit migration plans, and version hedges become
legitimate engineering. Until then, the cost of carrying compat
scaffolding outweighs the cost of changing the shape directly.

## See also

- [no-version-suffixes.md](no-version-suffixes.md) — the same
  anti-pattern at the type-naming level.
