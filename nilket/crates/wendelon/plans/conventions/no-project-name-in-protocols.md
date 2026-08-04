# No project name in protocols, types, or wire identifiers

The project's brand name (currently "Wendelon") does not appear in
internal protocol acronyms, wire-format identifiers, reject-code
prefixes, metric prefixes, module names, or struct prefixes.
Brand identity lives at the cargo-crate / repo / README level
only. The Rust module system carries the project namespace; types
within don't repeat it.

## Rule

- New protocol acronym: `AGG`, `PBC`, `CSB`, `HLC`. Not `WAP`,
  `WPBC`, `WCSB`, `WHLC`.
- New reject-code prefix: `<protocol>_REJECT_*` where `<protocol>`
  is the functional acronym, not the brand-prefixed acronym
  (`AGG_REJECT_*`, `PBC_REJECT_*`).
- New metric prefix: functional, not brand-prefixed (`agg_*`,
  `pbc_*`).
- New module: `src/schema/aggregate.rs`, not
  `src/schema/wendelon_aggregate.rs` or `src/schema/wap.rs`.
- New struct: `Node`, `Client`, `Config`, `Error` (in their
  scoping module). Not `WendelonNode`, `WendelonClient`, etc.
- Cargo crate name, repo name, README, and top-level marketing
  prose still carry the brand. The line is between project-level
  identity (brand belongs) and protocol-level identity (brand
  doesn't).

## Why

Brand and protocol live on different timescales. Brand decisions
are reversible: rebrand, sunset, fork, acquire. Wire-protocol
decisions, once published, are not — every byte in every frame
becomes an external compatibility contract.

Coupling them means brand decisions become protocol decisions,
and protocol decisions ossify the brand. A future rename
(Wendelon → Zirkelon, hypothetically) becomes a wire-format break
rather than a metadata change. A future protocol revision becomes
a marketing question rather than an engineering question.

The same anti-pattern shows up in two ways the project already
calls out:

- `no-version-suffixes.md` flags the same leak at the
  type-versioning layer (don't bake version numbers into Rust
  types).
- `no-premature-backward-compat.md` flags the same leak at the
  compat-shim layer (don't bake hypothetical future migrations
  into spec text).

This convention extends the discipline to the brand-naming layer.

## Examples already in the codebase

Brand-clean (good):

- `Identity`, `Locality`, `SlabId`, `LogIndex`, `Epoch`,
  `TenantId`, `TableObjectId` — newtype-units with functional
  names.
- `NodeCapability::SlabStatsPruning`,
  `NodeCapability::PartialAggregatePushdown` — capability
  variants with functional names.
- `CSB` (Column Statistics Block, ADR 039), `HLC` (Hybrid
  Logical Clock) — functional acronyms.

Formerly brand-leaked surfaces renamed by ADR 041:

- `WPBC` (Wendelon Pushdown Bytecode) → `PBC`.
- `WPBC_REJECT_*` reject codes → `PBC_REJECT_*`.
- `wpbc_*` metric prefix → `pbc_*`.
- `src/schema/wpbc.rs` → `src/schema/pbc.rs`.
- Any `Wendelon`-prefixed struct types → strip the prefix where
  the surrounding module disambiguates.

## How to apply in review

When a diff introduces a new protocol acronym, reject code,
metric prefix, module name, or struct type:

- If the identifier contains the project's brand name as a
  prefix — flag it. Ask whether the prefix is load-bearing or
  cosmetic. Cosmetic prefixes should be stripped before merge.
- If the identifier is brand-clean — note it positively. The
  norm is "no brand prefix"; reinforce by acknowledging when the
  norm is met.
- For protocol acronyms: prefer three-letter functional acronyms
  matching the existing pattern (`AGG`, `PBC`, `CSB`, `HLC`).
- For reject codes: `<protocol>_REJECT_*` where `<protocol>` is
  the functional acronym.
- For protocol-specific metric prefixes: `<protocol>_*` (e.g.,
  `agg_*`, `pbc_*`), not `<brand><protocol>_*`. If the metrics
  registry applies a repo-level Prometheus namespace, keep the
  protocol segment functional: `wendelon_pbc_*`, not
  `wendelon_wpbc_*`.
- For module names: descriptive, not brand-prefixed
  (`src/schema/aggregate.rs`, not `src/schema/wendelon_aggregate.rs`).

## Architectural enforcement

`tests/architecture_test.rs` enforces ADR 041 Tier-1 by scanning
code surfaces for the predicate-bytecode protocol's former
brand-leaked tokens (`WPBC`, `Wpbc`, `wpbc`) and for the old
`src/schema/wpbc.rs` module path. The Tier-2 struct-prefix rule
for `Wendelon[A-Z]` lands after the ADR 041 follow-up audit.

The architectural test makes the rule load-bearing rather than
aspirational. The same enforcement pattern that keeps "Control
Plane" out of the codebase keeps brand prefixes out of internal
protocol identifiers.

## When this changes

- The project remains 14-day-old fresh-start territory: the cost
  of refactoring is low and the cost of carrying the leak
  compounds with every new ADR. The rule applies strictly.
- If the project ever ships an external SDK with brand-leaked
  identifiers in deployed wire formats, the rule for *those
  specific identifiers* changes (deployed state must be honored;
  see `no-premature-backward-compat.md` for when compat
  scaffolding earns its keep). The rule still applies to *new*
  identifiers; existing ones get migration plans.

## See also

- `plans/conventions/no-version-suffixes.md` — same anti-pattern
  at the type-versioning layer.
- `plans/conventions/no-premature-backward-compat.md` — same
  anti-pattern at the compat-shim layer.
- `plans/conventions/capability-vs-feature-naming.md` — same
  discipline applied to a different vocabulary axis (capability
  vs. feature).
- `plans/philosophy.md` — "Linguistic Forbidden Zones" section
  on `Control Plane` / `Data Plane`; same enforcement pattern,
  applied to a different forbidden vocabulary.
- `plans/adr/0041_protocol_naming_decoupling.md` — ADR
  establishing this convention, with the migration plan for
  existing brand-leaked identifiers.
