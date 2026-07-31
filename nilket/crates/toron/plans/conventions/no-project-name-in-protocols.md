# No project name in protocols, types, or wire identifiers

The project's brand name ("Toron") does not appear in internal
protocol acronyms, wire-format identifiers, `MessageKind` variant
names, reject-code prefixes, metric prefixes, module names, or
struct prefixes. Brand identity lives at the cargo-crate / repo /
README level only. The Rust module system carries the project
namespace; types within don't repeat it.

## Rule

- New struct / trait: `Coordinator`, `Worker`, `Transport`,
  `Config`, `Error` (in their scoping module). Not
  `ToronCoordinator`, `ToronWorker`, `ToronTransport`.
- New `MessageKind` variant / wire identifier: named for what it
  carries (`StartTask`, `PullPartition`), never brand-prefixed.
- New reject-code or failure-label prefix: functional, tied to
  the surface it describes, not brand-prefixed.
- New metric prefix: functional (`task_*`, `shuffle_*`,
  `join_*`), not `toron_*`. If a repo-level metrics namespace is
  applied, keep the surface segment functional.
- New module: descriptive (`src/distributed/fragment.rs`,
  `src/exec.rs`), not `src/toron_fragment.rs`.
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
and protocol decisions ossify the brand. A future rename becomes
a wire-format break rather than a metadata change; a future
protocol revision becomes a marketing question rather than an
engineering question.

The same anti-pattern shows up in two ways this directory already
calls out:

- [no-version-suffixes.md](no-version-suffixes.md) flags the same
  leak at the type-versioning layer.
- [no-premature-backward-compat.md](no-premature-backward-compat.md)
  flags the same leak at the compat-shim layer.

This convention extends the discipline to the brand-naming layer.

## Examples already in the codebase

Brand-clean (good):

- `QueryId`, `StageId`, `TaskId`, `PartitionId`, `ExchangeId`,
  `ColumnOrdinal`, `PartitionCount` — newtype-units with
  functional names.
- `MessageKind::StartTask`, `MessageKind::PullPartition`,
  `TaskFailureKind::BudgetExhausted` — wire and failure surfaces
  named for what they do.
- `LogicalPlan`, `ExecutionPlan`, `JoinKind`, `JoinStrategy` —
  IR types whose scoping module supplies the namespace.

The wire layer was actively de-branded: the legacy `ToronMessage`
envelope wrapper was retired, and `tests/architecture_test.rs`
(`wire_contract_has_no_legacy_toron_message_wrapper`) keeps the
brand prefix out of the wire contract.

Existing leak (rename pass expected, not surprising):

- `ToronCoordinator`, `ToronWorker`, `ToronTransport` in
  `src/server/` — brand-prefixed struct and trait names where the
  scoping module (`server`, `server::transport`) already
  disambiguates. New code must not extend the pattern; stripping
  the prefix on these is candidate cleanup, mechanical and
  low-risk.

## How to apply in review

When a diff introduces a new protocol acronym, `MessageKind`
variant, reject code, metric prefix, module name, or struct type:

- If the identifier contains "Toron" as a prefix — flag it. Ask
  whether the prefix is load-bearing or cosmetic. Cosmetic
  prefixes should be stripped before merge.
- If the identifier is brand-clean — note it positively. The norm
  is "no brand prefix"; reinforce by acknowledging when the norm
  is met.
- For module names: descriptive, not brand-prefixed.

## Architectural enforcement

`tests/architecture_test.rs` already enforces the wire-layer half
of this rule (`wire_contract_has_no_legacy_toron_message_wrapper`).
A broader source-scan guard for `Toron[A-Z]`-prefixed struct and
trait names lands with the `ToronCoordinator` / `ToronWorker` /
`ToronTransport` cleanup. The architectural test makes the rule
load-bearing rather than aspirational.

## When this changes

Toron is early; the cost of refactoring is low and the cost of
carrying the leak compounds with every new ADR. The rule applies
strictly. If Toron ever ships an external SDK with brand-leaked
identifiers in deployed wire formats, the rule for *those
specific identifiers* changes (deployed state must be honored;
see [no-premature-backward-compat.md](no-premature-backward-compat.md)).
The rule still applies to *new* identifiers; existing ones get
migration plans.

## See also

- [no-version-suffixes.md](no-version-suffixes.md) — same
  anti-pattern at the type-versioning layer.
- [no-premature-backward-compat.md](no-premature-backward-compat.md)
  — same anti-pattern at the compat-shim layer.
- [capability-vs-feature-naming.md](capability-vs-feature-naming.md)
  — same discipline applied to a different vocabulary axis.
