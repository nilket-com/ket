# Conventions

Recurring style, naming, and design rules that hold across the Wendelon
codebase. Distinct from ADRs (which decide *what* the architecture is) and
from `plans/philosophy.md` (which states the project's foundational
worldview). Conventions are *how* code, types, and specs should be written
when those higher-level decisions don't dictate a specific shape.

These rules apply to anyone editing the repo — human or AI collaborator.
Read them before introducing new types, naming new fields, or amending an
ADR; flag violations in code review.

## Index

- [newtype-units.md](newtype-units.md) — wrap fundamental quantities as distinct newtypes; never pass raw `u64` / `[u8; N]` across module boundaries.
- [no-version-suffixes.md](no-version-suffixes.md) — drop `V1` / `V2` / `V3` suffixes from Rust types; reserve version bytes for actual wire-protocol envelopes only.
- [no-planning-phase-names.md](no-planning-phase-names.md) — no `phase_a` / `phase_b` / `iter2` / `wip_*` etc. in code identifiers; name by what the thing does, not by where it sat in someone's writeup.
- [no-premature-backward-compat.md](no-premature-backward-compat.md) — no compat structs, tolerant decoders, legacy-decode tests, or spec-level "future ADR" hedges.
- [capability-vs-feature-naming.md](capability-vs-feature-naming.md) — code uses "capability" precisely; "feature" is for marketing or casual prose.
- [no-project-name-in-protocols.md](no-project-name-in-protocols.md) — keep the project's brand name out of protocol acronyms, reject codes, metric prefixes, module names, and struct prefixes; brand lives at the cargo crate / repo / README level only.

## Adding a convention

A convention earns its place when the same correction has been given more
than once, or when an inconsistency has produced concrete cost. Add a new
file here, register it in this index, and link to it from any ADR that
codifies a related decision.
