# ADR 041: Project-name decoupling from internal protocols and types

## Status

Accepted

## Context

Wendelon's project name has leaked into internal-layer
identifiers across multiple surfaces:

| Surface | Current shape | Brand-leaked? |
|---|---|---|
| Predicate-bytecode acronym | `WPBC` (Wendelon Pushdown Bytecode) | yes |
| Module path | `src/schema/wpbc.rs` | yes |
| Reject-code prefix | `WPBC_REJECT_*` | yes |
| Metric prefix | `wpbc_*` (e.g., `wpbc_slab_stats_pruned_total`) | yes |
| Spec doc filename | `plans/schema/compute_ast.md` (refers to "WPBC" throughout) | yes (in content) |
| Top-level types/structs | `WendelonNode`, `WendelonClient`, etc. (where they exist) | yes |
| Capability variants | `SlabStatsPruning`, `PartialAggregatePushdown` | no (brand-clean) |
| Newtype-units | `Identity`, `Locality`, `SlabId`, `TenantId`, `Epoch` | no |
| ADR titles | "ADR 035: Vectorized WPBC Executor" | yes (transitively) |

The leak is the kind that compounds with deployment age: every
byte of every wire format that names "WPBC" becomes a stability
liability the moment any external system depends on it.

The project owner is also evaluating a possible brand rename
(Wendelon → Zirkelon or similar) and has flagged that
brand-leakage at the protocol level would make any such rename a
massive disruptive refactor rather than a metadata change. The
right answer is to decouple now, while the project is 14 days old
and has no deployed external state.

This ADR establishes the rule, captures the rename scope, and
lists the migration plan.

The companion convention doc
`plans/conventions/no-project-name-in-protocols.md` codifies the
rule going forward so future ADRs don't reintroduce the leak.

## Decision

Strip the project name from internal protocols, types, reject
codes, metric prefixes, and module names. Brand identity lives at
the cargo-crate / repo / README level only.

### 1. Specific renames (Tier 1: protocol surfaces)

These are wire-protocol-visible or operator-visible identifiers
where the brand leak has the highest compounding cost:

| Old | New |
|---|---|
| `WPBC` (acronym in prose) | `PBC` (Pushdown Bytecode) |
| `WPBC_REJECT_*` reject codes | `PBC_REJECT_*` |
| `wpbc_*` metric prefix | `pbc_*` |
| `src/schema/wpbc.rs` module | `src/schema/pbc.rs` |
| `WpbcProgram`, `WpbcExecutor`, etc. | `PbcProgram`, `PbcExecutor`, etc. |
| `plans/schema/compute_ast.md` content | references "PBC" throughout (filename stays — already brand-clean) |

Metric names may still carry the repo-level Prometheus namespace
where the metrics registry already uses one. The protocol segment
must be functional: `wendelon_pbc_*` is fine; `wendelon_wpbc_*`
is not. See
`plans/conventions/no-project-name-in-protocols.md` for the
review rule.

The acronym `PBC` is chosen over alternatives (`PDX`, `BC`,
spelled-out "Bytecode") for these reasons:

- **`PBC` preserves meaning.** "Pushdown Bytecode" is the precise
  description; dropping the W keeps the meaning intact.
- **Three letters parallels existing acronyms.** `AGG` for
  aggregate pushdown; `HLC` for Hybrid Logical Clock; `CSB` for
  Column Statistics Block. Three-letter functional acronyms are
  the house pattern.
- **Avoids `BC`'s collision** with the year-disambiguator pattern.
- **Avoids `PDX`'s** Portland-airport-code accidental association.

### 2. Specific renames (Tier 2: struct prefixes)

Struct names that gratuitously prefix "Wendelon" are renamed to
their functional form. The Rust module system carries the
namespace via the cargo crate root; types within don't repeat it.

Examples (subject to audit during migration):

| Old | New |
|---|---|
| `WendelonNode` | `Node` (in `wendelon::node`) |
| `WendelonClient` | `Client` (in `wendelon-client`) |
| `WendelonError` | `Error` (in module-appropriate location) |
| `WendelonConfig` | `Config` |

The rule: if the type name contains "Wendelon" and dropping it
doesn't create ambiguity in the surrounding module, drop it. If
ambiguity arises (e.g., two different `Error` types in the same
namespace), use a functional prefix rather than the brand prefix.

The Tier-2 rename is a follow-up effort after this ADR ratifies.
The audit (per-type decision: rename / functional-rename / keep
when genuinely disambiguating an external import) lands as a
separate PR or PR series. The architectural-test enforcement for
Tier-2 (`Wendelon[A-Z]`) extends after that audit completes with
the audit's allowlist of disambiguation-justified types.

### 3. Out of scope (Tier 3: top-level identifiers)

- Cargo crate name (`wendelon`).
- Cargo workspace name.
- Repository name.
- README.md / top-level prose.
- Domain names, trademarks, marketing copy.

These carry the brand intentionally. A future brand rename
(Wendelon → Zirkelon or similar) operates at this tier; after
this ADR lands, that rename becomes a metadata change rather than
a protocol break.

### 4. ADR text and filename updates

ADRs 005, 035, 036, 037, 038, 039 reference WPBC throughout. The
migration updates ADR text *and* filenames in place — these are
living documents, and the repo doctrine is "no historical
artifacts in the current state — git history is the place for
history":

- ADR 005: filename `005_compute_pushdown_wpbc.md` →
  `005_compute_pushdown_pbc.md`; title and prose updated to PBC.
- ADR 035: filename `035_vectorized_wpbc_executor.md` →
  `035_vectorized_pbc_executor.md`; title and prose updated.
- ADR 036: filename `036_wpbc_pushdown_contract.md` →
  `036_pbc_pushdown_contract.md`; title and prose updated,
  including the §"Capability negotiation contract" framing.
- ADR 037: §"Appendix A" reject codes renamed.
- ADR 038: §"Appendix A" reject codes renamed.
- ADR 039: §"Appendix A" reject codes renamed.

All renames are done via `git mv` so blame is preserved; the git
log carries the rename context for anyone digging into history.

ADR text changes are *not* historical revisionism: the codebase
has not deployed externally; ADRs document the *current*
architecture, not its history.

### 5. Convention adoption

This ADR establishes
`plans/conventions/no-project-name-in-protocols.md` as a binding
convention. Reviewers must flag new code that reintroduces brand
prefixes on protocol-level identifiers, reject codes, metric
prefixes, or module names.

## Rationale

**Why now (14 days in).** Past 6 months, "rename" becomes "freeze
the protocol and live with the wrong name forever." The project
has no deployed external state; refactoring is mechanical
find/replace plus tests. Cost is minimal; cost-of-delay is high.

**Why decouple at all.** Brand and protocol live on different
timescales. Brand decisions are reversible (rebrand, sunset,
fork); wire protocols, once published, are not. Coupling them
means brand decisions become protocol decisions, and protocol
decisions ossify the brand.

**Why three tiers.** The cost of changing each tier is
proportional to its visibility:

- Tier 1 (wire protocols, metrics, reject codes): visible to
  operators and external integrators. Highest compounding cost.
  Strip the brand.
- Tier 2 (struct prefixes): visible to project contributors.
  Moderate cost. Strip when not load-bearing.
- Tier 3 (cargo crate, repo, top-level prose): visible to
  everyone, but carries the brand intentionally. Keep.

Cargo's namespace boundary is the natural cut: inside the crate,
modules give you scoping for free; outside the crate, the brand
identifies the project.

**Why `PBC` not `PDX` or spelled-out.** Three-letter functional
acronym matches the house pattern (`AGG`, `HLC`, `CSB`).
"Bytecode" alone is too generic; "Pushdown Bytecode" is the
meaningful phrase; `PBC` preserves it.

**Why ADR text and filenames update rather than superseding.**
The codebase is 14 days old; no external consumers depend on the
WPBC name in any documented way. ADRs are living architecture
documents in this project's discipline (per existing ADR practice
— ADR 036 got updated to add §"Capability negotiation contract"
without a new ADR). Updating prose and filenames in place is
consistent with that discipline. `git mv` preserves blame; `git
log` preserves the rename history.

**Why split Tier-1 and Tier-2 into separate PRs.** Tier-1 is
concrete and complete: the WPBC rename touches a defined set of
identifiers that can be replaced mechanically and verified by an
architectural test. Tier-2 requires a per-type audit decision
(rename / functional-rename / keep) across ~629 occurrences of
`Wendelon`-prefixed identifiers; many disambiguate against
external types (e.g., `WendelonRaft` =
`openraft::Raft<WendelonTypeConfig>`). Bundling Tier-2 into the
same merge would either delay Tier-1 by the audit duration or
force the audit into a single sweeping PR with thousands of
mechanical changes mixed with judgment calls. Splitting keeps
each PR's review surface coherent.

## Consequences (the negatives)

- **Mechanical refactor across the codebase.** Every file that
  references WPBC needs updating: source code, tests, ADRs,
  docs, metric registries, integration tests. Single PR is
  feasible at 14 days in; will take longer the more we wait.
- **Documentation churn.** Six existing ADRs need updates; three
  ADR filenames are renamed via `git mv`. Their cross-references
  must stay coherent.
- **Operator dashboards.** Any internal dashboards using `wpbc_*`
  metrics need updating. At 14 days these are unlikely to exist
  outside development; verify before merge.
- **Test suite churn.** Test names referencing WPBC are renamed;
  golden test files mentioning WPBC in fixture names are
  renamed.
- **Discomfort for collaborators with WPBC muscle memory.** Two
  weeks of "WPBC" in everyone's mental cache; rebuilding takes
  a few days. Worth the long-run cost.
- **No external compat layer.** Per
  `plans/conventions/no-premature-backward-compat.md`, we do not
  ship a `WpbcProgram` type alias as a deprecation hatch.
  Deployments don't exist; the rename is total.
- **Tier-2 follow-up still pending.** The ~629 `Wendelon`-prefixed
  identifiers across `src/` are not renamed in this merge;
  architectural-test enforcement for them comes after the
  Tier-2 audit.

## Rejected alternatives

- **Leave WPBC as-is.** Bakes brand into wire format and metric
  names; compounds future rename cost; reinforces the leak in
  every new ADR (the iteration-1 draft of ADR 040 used `WAP` for
  symmetric reasons).
- **Half-measure: rename modules but keep metric prefixes.**
  Inconsistent. Operators see one namespace; code uses another.
  Mismatch will reintroduce the leak under load.
- **Half-measure: rename metric prefixes but keep WPBC in code.**
  Same problem inverted.
- **Use `WB` (Wendelon Bytecode) as a shorter brand-leaked
  acronym.** Doesn't fix the leak; just compresses it.
- **Use `Wendelon::Bytecode` namespace path with type name
  `Bytecode`.** Type-name-only is too generic; collides with any
  generic `Bytecode` import. `PBC` keeps the descriptive prefix.
- **Use full spelled-out "PushdownBytecode" type prefix.** Too
  long for the volume of references. Three-letter functional
  acronym is the existing pattern.
- **Defer to "after the brand decision is made".** Reverses the
  dependency. The brand decision is *easier* after the
  decoupling; deferring means the brand decision drives the
  protocol naming, which is exactly the leak this ADR removes.
- **Make the rename a deprecation rather than a sweep.**
  Conflicts with `plans/conventions/no-premature-backward-compat.md`
  for an undeployed system.
- **Bundle Tier-1 and Tier-2 into a single merge.** Per-type
  audit overhead would either delay Tier-1 or balloon the PR
  beyond reviewable.
- **Keep ADR filenames as-is and only update prose.** Conflicts
  with the "no historical artifacts in the current state"
  doctrine. Filenames are surfaces too; `git mv` preserves blame.

## Migration plan

### Phase 1 — Code refactor (Tier-1, single PR)

- `src/schema/wpbc.rs` → `src/schema/pbc.rs`.
- `Wpbc*` types → `Pbc*` (or generic functional names where
  appropriate).
- All `wpbc_*` metric registrations renamed to `pbc_*`.
- All `WPBC_REJECT_*` codes renamed to `PBC_REJECT_*`.
- Tests updated alongside.
- `cargo check`, `cargo test`, `cargo clippy` green.
- Single PR, atomic, no transitional aliases.

### Phase 2 — ADR text and filename updates

- ADR 005: `git mv` filename + title + prose.
- ADR 035: `git mv` filename + title + prose.
- ADR 036: `git mv` filename + title + prose; §"Capability
  negotiation contract" prose alignment.
- ADR 037: §"Appendix A" rename.
- ADR 038: §"Appendix A" rename.
- ADR 039: §"Appendix A" rename.
- All cross-references between ADRs updated for consistency.

This phase lands in the same merge as Phase 1 so the
documentation matches the current code state.

### Phase 3 — Spec doc updates

- `plans/schema/compute_ast.md`: WPBC → PBC throughout.
- Any other prose docs in `plans/` that mention WPBC.

### Phase 4 — Convention enforcement (Tier-1 only)

- New convention `plans/conventions/no-project-name-in-protocols.md`
  registered in `plans/conventions/README.md` index (already in
  place).
- Architectural test (parallel to the existing
  "control plane / data plane" forbidden-words test in
  `tests/architecture_test.rs`): forbid the literal substring
  `WPBC` and the `wpbc_` prefix in metric/reject-code string
  literals across `src/`, `tests/`, `benches`, `examples`,
  `crates`, and CI helper scripts. Exempt cargo manifest,
  README, and `plans/`.
- The test does **not** forbid the `Wendelon[A-Z]` pattern in
  this merge; that rule lands after the Tier-2 audit.

### Phase 5 — Tier-2 audit + rename (follow-up PR series)

- Enumerate ~629 `Wendelon`-prefixed identifiers across `src/`.
- Decide per-type: rename / functional-rename / keep (only when
  genuinely disambiguating against an external import).
- Mechanical refactor per audit decisions.
- Architectural test extended to forbid `Wendelon[A-Z]` with the
  audit's allowlist of disambiguation-justified types.

## Coordination with other in-flight work

- **ADR 040 (aggregate pushdown).** Ratifies in the same merge
  as this ADR. ADR 040 uses brand-clean naming (`AGG_REJECT_*`,
  `agg_*` metrics, `AggregateRequest` / `AggregateResponse`)
  from day one and references the predicate protocol as `PBC`
  throughout.
- **Possible Wendelon → Zirkelon brand rename.** Out of scope for
  this ADR. After this ADR lands, that rename is a Tier-3
  metadata change (cargo manifest, README, repo name, possibly a
  sweep of `Wendelon` references in prose) without touching wire
  formats, reject codes, metrics, or struct prefixes.

## Open questions

1. **Tier-2 audit scope and timing.** The ~629 Wendelon-prefixed
   identifiers need a per-type audit before bulk rename. Audit
   PR is the natural opener for the Tier-2 follow-up series.

2. **`Wendelon::Identity` and `Wendelon::Locality` newtype
   prefixes.** Currently brand-clean (`Identity`, `Locality`).
   Audit should confirm no rename needed; flagged here for
   future-reviewer attention.

## References

- ADR 005, 035, 036, 037, 038, 039 — predicate protocol ADRs
  whose surfaces this ADR renames.
- ADR 040: Aggregate Pushdown Protocol — uses brand-clean
  naming from day one.
- `plans/conventions/no-project-name-in-protocols.md` — the
  convention this ADR establishes.
- `plans/conventions/no-version-suffixes.md` — sibling convention
  on the same anti-pattern at a different layer (versioning vs.
  branding).
- `plans/conventions/no-premature-backward-compat.md` — same
  anti-pattern at the compat-shim layer; explains why this ADR
  doesn't ship a `WpbcProgram` deprecation alias.
- `plans/philosophy.md` — the "Linguistic Forbidden Zones"
  section already establishes a precedent for forbidden-words
  enforcement (Control Plane / Data Plane); this ADR extends the
  same architectural-test discipline to brand prefixes on
  internal protocols.
