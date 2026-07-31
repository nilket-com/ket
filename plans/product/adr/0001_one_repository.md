# ADR 0001: one repository, one state vector

Status: accepted and executed 2026-07-31 from frozen nilket commit
`7d978dbf` (`plan: adr 531 global aggregate gather`). This is the first
record in the PRODUCT namespace: decisions spanning the complete system. The
ket lineage through ADR 0005 and the Toron lineage through ADR 531 remain
distinct and unchanged. This merge joins two mature architectural
civilizations; it does not absorb a side project or pretend the database
appeared yesterday.

## Context

One product vision -- an editor, workbench, notebook, plotting system, and
analytics engine as a single system (`ket`, the complete state vector) --
currently lives in two repositories. The separation has produced measured
friction: Polariton's duplicated wire-protocol client exists because Cargo's
optional path dependencies cannot tolerate an absent sibling checkout; the
Toron dogfood loop requires atomic changes spanning client, server, and UI
that two commit chains cannot express; plan numbering has collided across the
boundary; and every cross-repository session pays review and push-order
choreography.

## Decision

1. **One Git repository, by graft -- not relocation.** The existing ket
   repository is the monorepo. Nilket is imported under the `nilket/` prefix.
   Every existing ket path and commit remains byte-identical. There is no
   `product/{ket,nilket}` reshuffle.
2. **History is prefix-rewritten, never copied or squashed.** A disposable
   mirror of the frozen nilket remote is rewritten with `git filter-repo` so
   every historical nilket path carries the `nilket/` prefix. A non-squashed
   `git subtree add` is not equivalent: its ancestor commits retain root-level
   paths, weakening path archaeology across the graft. Rewriting necessarily
   changes nilket commit object IDs, so the generated old-to-new commit map is
   retained as a migration artifact.
3. **Multiple Cargo workspaces remain.** Ket and `nilket/` keep independent
   manifests, lockfiles, dependency resolution, and build graphs. Monorepo for
   coordination; multiple workspaces for isolation. A root super-workspace is
   adopted only if measurements later justify its coupling cost. Submodules
   are rejected because they cannot express the atomic product commits that
   motivate the merge.
4. **Ownership follows the protocol.** The public `toron-client` crate is born
   in the nilket workspace beside the protocol and server it represents;
   Polariton may depend on that client across the workspace boundary, never on
   Toron server internals. A root `shared/` workspace is introduced only for a
   genuinely jointly-owned abstraction, such as a future interchange and plot
   contract currently called `boson`, and only with an explicit owner and
   dependency guard. It is not a miscellaneous-crate directory or a second
   analytics engine. Toron owns almost all analytical execution; a future
   workbench library may own composition and presentation.
5. **ADR namespaces remain federated.** Existing names and files are not
   rewritten. New prose qualifies cross-lineage references as `KET-NNNN`,
   `TORON-NNNN`, or `PRODUCT-NNNN`; the directory supplies the namespace for
   filenames such as this one. There is no global counter because the imported
   histories developed concurrently. Git is the shared ledger; qualified
   namespaces preserve each lineage.
6. **Dependency guards extend before product work resumes.** Libaryon rejects
   Toron, Wendelon, connectors, and analytics. Baryon rejects engines and
   connectors but retains KET-0005's explicit permission to consume shared
   result and plot contracts as an optional plot-raster head. Nilket engine crates
   reciprocally reject editor and document-engine crates. Polariton receives
   one narrow nilket allowance: the public Toron client. Any wider dependency
   requires amending the applicable ADR first.
7. **CI follows workspaces.** Ket-only changes do not build the distributed
   database; nilket-only changes do not build the editor heads. One
   cross-product dogfood job covers the client/server/UI path. Path filters are
   an optimization only: a scheduled or manually invokable complete run
   remains available so filter mistakes cannot permanently hide failures.
8. **The linear chain admits one explicit exception.** The graft is a single,
   intentional two-parent merge commit joining the ket tip and rewritten
   nilket tip. This preserves both ancestries without asserting a fictional
   order between them. The first-parent plan/impl chain continues linearly
   afterward.

## Freeze and preservation contract

No migration command runs until the user declares nilket frozen. At that
point the graft records and verifies all of the following:

1. The nilket worktree is clean, its local main equals `origin/main`, and the
   frozen tip includes the completed Toron ADR 531 work.
2. A fresh mirror is cloned from the authoritative remote. Its source URL,
   frozen main SHA, complete ref inventory, reachable commit count, and
   `git fsck --full` result form the freeze certificate. The current sibling
   checkout is never used as the import source.
3. Before rewriting, an all-refs Git bundle and its SHA-256 digest are stored
   outside the destination repository. The original `nilket-com/nilket`
   remote remains a read-only historical archive after the graft.
4. The disposable mirror is prefix-rewritten. Existing tags, if any, are
   namespaced under `nilket/`; non-main branch refs are either imported under
   archival refs or explicitly inventoried in the freeze certificate. They
   are never silently discarded.
5. The filter-repo commit map is committed under
   `plans/product/migration/`. Verification proves that every source commit
   reachable from the frozen main maps exactly once; author and committer
   identities, timestamps, messages, and parent topology agree modulo mapped
   object IDs; and the mapped main has the same reachable commit count.
6. The rewritten main is fetched into ket and joined by the deliberate
   two-parent merge. `git fsck --full`, representative `git log --follow`
   checks across old nilket files, both workspace metadata checks, and both
   pre-existing validation suites pass before the merged repository is
   pushed.

The preservation promise covers Git commits, ancestry, authorship, messages,
timestamps, and refs recorded by the freeze certificate. GitHub issues,
settings, Actions history, and other forge metadata are not Git objects; the
old remote remains the archive for those records unless separately exported.

## Sequencing

The Toron dogfood implementation is paused. After the explicit freeze signal:

1. capture the freeze certificate and recovery bundle;
2. rewrite and verify nilket history in isolation;
3. graft it with the one two-parent merge and publish the migration evidence;
4. extend dependency guards;
5. install path-filtered and cross-product CI; and
6. resume the dogfood work as the monorepo's first cross-product pair.

Toron development resumes only after the graft. The dogfood pair may then
touch Polariton, the public client, and the engine atomically.

## Consequences

- The repository remains `nilket-com/ket`, now accurately named: a ket denotes
  the complete state of a system.
- Existing ket paths and their documentation links do not churn. Nilket paths
  gain the one prefix required by the graft, throughout their rewritten
  history.
- Libaryon's text indexes remain document infrastructure, not precedent for
  data analytics. Query planning, aggregation, joins, and transforms belong in
  Toron. Product-side crates compose sources and present results.
- The plotting contract may become PRODUCT-0002 if its ownership crosses the
  editor/analytics boundary; its author decides from ownership, not prestige.
- Cross-system findings may migrate to `plans/product/` when touched. Nothing
  is force-moved for cosmetic consistency.
- Splitting later remains mechanically possible with `git filter-repo` if a
  genuine boundary -- licensing, ownership, team, or release cadence --
  materializes. Until then, the daily coordination tax of separation is
  removed without coupling every Cargo build.
