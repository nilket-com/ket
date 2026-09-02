# ADR 581 - Content-addressed evidence identity

**Status:** OPEN. Decided; no implementation has landed. Applies before the next
authoritative performance run. Does not touch the Toron operator-surface record
queued for ADR 0097 decision 4.


## Context

Every authoritative run artifact identifies the build it measured by
`build_commit`: the abbreviated git HEAD, suffixed `-dirty` when tracked files
were modified. The authority gates in `toron-tpch` refuse a stamp that contains
`dirty`, and the receipt matcher compares stamps between arms.

That choice has two consequences that were not decided, only inherited.

First, a measurable state must be a commit. Every arm of every A/B has to be
committed before it can be timed, so the history of a perf arc fills with commits
whose only job is to exist at the moment a binary was built. On 2026-09-01 the
554 and 580 arcs carried fifteen such build commits, none of which could be
squashed because sealed results documents cite them as evidence. History is being
kept for the stamp, not for the reader.

Second, the stamp does not identify what ran. The build script re-stamps only when
`HEAD` or the index moves, so an unstaged source edit rebuilds Rust code under the
previous stamp. The `-dirty` rule counts tracked modifications only, on the
assertion that an untracked file cannot change what was compiled; that assertion
is false for untracked source files, generated inputs and submodule contents. And
two clean commits can build two different executables from one tree, through
profile, features, target, toolchain, rustflags or the lock file, while one dirty
tree builds exactly one. The gate is at once too strict, forcing commits, and too
weak, admitting builds it cannot distinguish.

The results documents already record, by hand, the thing that actually settles
identity: the executable's hash and modification time next to the commit.


## Decision

Evidence identifies content, not history. An authoritative artifact carries an
identity in this order of authority, and the gates consult it in this order.

1. **Executable hash.** SHA-256 of the exact bytes that ran. The wrapper copies
   each executable - the harness and the resolved Toron binary, separately - into
   a content-addressed staging path named by its hash, makes the copy read-only,
   and executes THAT copy. Hashing a mutable path before timing would prove only
   what the path held at that moment, not what ran or what is later archived;
   the staged copy is the same file at every step. The running process never
   describes itself. Arms must match on executable hash. `dirty` stops being an
   authority input.
2. **Build recipe digest.** A digest over profile, enabled features, target
   triple, `rustc -vV`, a digest of the effective rustflags, and `Cargo.lock`.
   Distinguishes the executables one source tree can produce. The recipe and the
   evidence manifest are both versioned schemas with explicit version bytes,
   sorted feature lists and one canonical encoding, so that two recipes are
   comparable by digest and a schema change is a declared version, never an
   implicit incompatibility.
3. **Source tree id.** The git tree id of the source actually built, written from
   a temporary index that includes uncommitted tracked changes. This is the
   recoverable snapshot, and a tree object can be tagged directly, so anchoring
   evidence never requires a commit.
4. **Commit hash.** Navigation hint only. Never compared by a gate.

**The stamp is produced by the build wrapper, not inferred by the build script.**
`bench build` constructs the source snapshot, records the recipe, builds, hashes
both executables, and passes the identity into the build explicitly. The existing
build script's opportunistic stamping is retired rather than extended, because a
watch on `HEAD` and the index cannot see the edits that matter.

**Authoritative builds may contain uncommitted tracked changes and must contain
no untracked build inputs.** The wrapper enforces the rule: an untracked file
under a declared source root voids authority with a named reason, as a dirty
commit does today. This replaces the broad assertion that untracked files cannot
affect compilation with a rule the wrapper can check.

**A successful run emits an evidence manifest** carrying all four identities,
the artifact path and its own digest. Sealing is a separate, explicit
`seal-evidence` command that archives the staged executables, creates and pushes
an annotated tag on the source tree id, and writes the sealed manifest. The timed
harness never mutates git. **Final authority belongs to the sealed manifest**, not
to the pre-seal run artifact: a run that was never sealed is a measurement, not
evidence.

**The executable is retained.** A hash proves identity; it cannot reconstruct
the binary. `seal-evidence` archives the same read-only staged file that ran, and
refuses to seal as authoritative without it; a seal without an archived
executable is recorded as unretained.


## Acceptance

- An A/B run whose two arms are the same source tree with a differing untracked
  source file is refused with a reason naming the file. The current gate admits
  it; this is the discriminating case for the untracked-input rule.
- Negative control for the same rule: an untracked log or draft note OUTSIDE the
  declared build-input closure does not void authority. Without this control the
  rule would be the broad hygiene check that once voided a complete atlas run,
  rebuilt under a new name.
- An A/B run whose two arms are one commit built with different features is
  refused on the recipe digest. The current gate admits it.
- An arm built from an uncommitted tracked edit is admitted when its executable
  hash and recipe match its receipt, and `dirty` appears nowhere in the gate's
  reasons. This is the case the commit-per-arm cadence existed to satisfy.
- An unstaged source edit changes the source tree id and the executable hash of
  the next build even though `HEAD` and the index did not move.
- The executable that ran is the read-only staged copy: its path is under the
  staging root, its hash is its name, and a write to it fails. A sealed
  manifest's executable hash equals the hash of the archived file it names,
  checked by the seal command rather than trusted from the manifest.
- A recipe or manifest with an unknown version byte is refused, not read as the
  current version.
- The perf arc that follows this record lands with no commit whose purpose is to
  be a stamp: the results document cites tree ids and executable hashes, and its
  history is one plan and one impl commit per decision.


## Guardrails

- The gates compare identities; they do not compute them. Hashing happens in the
  wrapper before timing, so the receipt is never self-referential and never on
  the timed path.
- The commit hash stays in the manifest for `git log` navigation and nothing
  reads it for a decision. A future contributor who reintroduces a `dirty` check
  as an authority input reopens this record.
- Sealing is never automatic. The timed harness writes the manifest; a person
  runs `seal-evidence`, which is the only step that touches git.
- Existing sealed artifacts keep their `build_commit` stamps and their
  `artifact/<commit>` tags. Nothing is retroactively re-identified.


## Risks

- A tree id written from a temporary index still omits inputs outside the source
  roots the wrapper declares. The declared roots are the closure; an input that
  is not under one is not covered, and the record says so rather than claiming
  completeness.
- Archiving executables costs space. Retention is bounded by the seal command's
  archive policy, and an unretained seal is a weaker claim, not a hidden one.
- Two executables can differ in bytes for reasons that do not affect timing.
  The gate is strict on purpose: a false refusal costs one rebuild, a false
  admission costs a sealed number that measured the wrong thing.


## Forward

Two cuts, and authority moves only at the second.

The first cut is the wrapper and the manifest, in SHADOW mode: `bench build`
stages and hashes the executables, records the recipe, writes the manifest, and
the gates evaluate the new identities and report what they would have decided,
while the existing `build_commit` gates keep deciding. The first cut cannot grant
the new authority, because that authority requires an archived executable and
archiving arrives with sealing.

The second cut is `seal-evidence` with executable archiving. When it lands the
new gates decide, the `build_commit` gates are retired, and the results-document
convention changes: cite tree id and executable hash, not commit. Old and new
receipts overlap for the one arc that runs in shadow mode, so the two verdicts
can be compared before the switch.
