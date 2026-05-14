# Conventions

Recurring style, naming, and design rules that hold across the
Toron codebase — the compute ring of the Nilket stack. Distinct
from ADRs (which decide *what* the architecture is): conventions
are *how* code, types, and specs should be written when those
higher-level decisions don't dictate a specific shape.

These rules apply to anyone editing the crate — human or AI
collaborator. Read them before introducing new types, naming new
fields, or amending an ADR; flag violations in code review.

## Index

- [newtype-units.md](newtype-units.md) — wrap fundamental
  quantities (ids, ordinals, counts) as distinct newtypes; never
  pass raw `u32` / `Uuid` across module boundaries unwrapped.
- [no-version-suffixes.md](no-version-suffixes.md) — drop
  `V1` / `V2` / `V3` suffixes from Rust types; reserve version
  fields for actual wire-protocol envelopes only. Includes the
  family-first / be-explicit-on-both-sides variant-naming rules.
- [no-planning-phase-names.md](no-planning-phase-names.md) — no
  `phase_1` / `g17_` / `iter2` / `wip_*` in code identifiers;
  name by what the thing does, not by where it sat in a writeup.
- [no-premature-backward-compat.md](no-premature-backward-compat.md)
  — no compat structs, tolerant decoders, legacy-decode tests, or
  spec-level "reserved for V2" / "future ADR" hedges.
- [no-project-name-in-protocols.md](no-project-name-in-protocols.md)
  — keep "Toron" out of protocol identifiers, `MessageKind`
  variants, metric prefixes, module names, and struct prefixes;
  brand lives at the cargo crate / repo / README level only.
- [capability-vs-feature-naming.md](capability-vs-feature-naming.md)
  — code uses "capability" precisely (the Rune sandbox's gated
  abilities); "feature" is for marketing or casual prose.

## Adding a convention

A convention earns its place when the same correction has been
given more than once, or when an inconsistency has produced
concrete cost. Add a new file here, register it in this index,
and link to it from any ADR that codifies a related decision.
