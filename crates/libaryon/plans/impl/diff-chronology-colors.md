# Diff chronology: colors follow time, geometry follows panes

Scope: diff semantics become chronology-anchored and layout-independent, and
diff colors become semantic palette entries instead of hardcoded RGB.
Deliverable is one `impl:` commit reviewed against this plan.

## The bug (verified 2026-07-29)

`recompute_all_diffs` derives diff direction from **numeric `PaneId` order**
(`diff.rs:16-24`: the lower pane id becomes the algorithm's "left/old"
input). When the worktree pane was created first, `:panediff` treats the
live document as *before* and the HEAD snapshot as *after* — so brand-new
worktree code is classified `Removed` and painted red, and HEAD-only content
turns green. The screenshot evidence: the freshly written generation-tagged
`id.rs` rendered as a wall of deletions.

Compounding it, the per-pane mirror `swap_sides()` (`diff.rs:53`) flips
`Added ↔ Removed` along with coordinates, so no post-hoc color fix at the
render layer can be correct for both panes.

## Semantics: persistent Before/After roles

`DiffPair` gains an explicit chronology role, assigned **once at pair
creation** and never derived from pane ids, pane positions, or tab moves:

```rust
pub(crate) struct DiffPair {
    pub(crate) partner_pane_id: PaneId,
    /// The role of the pane that owns this entry, fixed at creation. Pane ids
    /// may still deduplicate recomputation; they never determine direction.
    pub(crate) role: DiffRole,   // Before | After
    pub(crate) blocks: Vec<DiffBlock>,
}
```

The construction API names chronology rather than geometry:
`compute_pane_diff(registry, session, before_pane_id, after_pane_id)`.
Callers must not pass `left`/`right` arguments. The reciprocal entries carry
`Before` and `After` respectively; recomputation recovers the canonical
argument order from those roles after using pane ids only to deduplicate the
pair.

Role assignment per command:

- **`:panediff` / `:diffstep` (git)**: the provenance-pinned snapshot
  (`SnapshotProvenance`, immutable OID) is **Before**; the live document is
  **After** — wherever either pane sits, and across every `:diffstep`
  movement (stepping changes which commit is Before, never which side is
  Before).
- **Explicit target forms** (`:pd TARGET`-style, if/when present): TARGET is
  Before, the focused document is After — command direction is the
  chronology. Filesystem timestamps are explicitly *not* consulted; they do
  not establish meaningful chronology.
- **Future commit-vs-commit pairs**: may use git ancestry when one endpoint
  is an ancestor of the other; otherwise command direction. (Extension note
  only — not in scope.)

## Canonical computation

The diff is computed exactly once, canonically **Before → After**:

- `Added` — exists only in After.
- `Removed` — exists only in Before.
- `Modified` — envelope on both sides; intra-line tokens keep direction
  (removed tokens on the Before side, inserted tokens on the After side).
- `Moved` — both sides.

`swap_sides()` becomes a **coordinate-only** mirror: it swaps pane-local
geometry for the partner pane's block list and *never* flips kinds. This is
the second half of the fix — with canonical chronology, the old kind-flip
would turn inserted tokens red whenever the After document renders on the
mirrored side. A regression test targets exactly that: After-side insertions
stay `Added` in both panes' block lists.

## Colors: semantic palette entries

Baryon's hardcoded fills (`draw.rs:603-606`) and intra-line RGB
(`:630-636`) are replaced by palette entries on `UiPalette`, flowing through
the existing projection protocol:

```
diff.added    = the colorblind-safe green already used for GitAdded
diff.removed  = GitRemoved red
diff.modified = GitModified blue
diff.moved    = colorblind-safe purple (new; pick to remain distinguishable
                from the other defaults under simple deuteranopia and
                protanopia simulations)
```

Block backgrounds, stronger intra-line emphasis, and the Bezier connector
bridge all derive as alpha blends of the same four entries — one source of
truth, themeable, consistent between terminal cell fills and the raster
connector overlay. Cell fills blend against the pane's effective editor
background (including a per-view background override); intra-line fills use a
stronger ratio. The bridge request carries the four resolved colors and its
fingerprint hashes them, so a live UI-color change cannot leave a stale
raster in the worker cache.

Baryon is currently the only frontend that draws diff overlays; Polariton
does not consume `DiffPair` yet. The palette fields remain frontend-neutral
on `UiPalette`, so Polariton can consume the same semantics when its diff
surface arrives, but adding that surface is out of scope. Extend the
established source-audit family to reject the legacy numeric RGB literals (or
new literal diff colors) in Baryon's draw/bridge paths. The audit must still
allow the legitimate `Color::Rgb(color.r, color.g, color.b)` conversion from
the resolved palette.

## Tests and proof obligations

1. **The screenshot scenario as acceptance**: worktree pane created first
   (lower PaneId), `:panediff` against HEAD → worktree-only lines are
   `Added`/green in the live pane; HEAD-only lines are `Removed`/red in the
   snapshot pane. Then the mirrored arrangement (snapshot pane created
   first) asserts identical classification.
2. **Role persistence**: reorder/move panes after pairing; recomputation
   (edit → graft → `recompute_all_diffs`) preserves roles and colors.
3. **`:diffstep`**: stepping older/newer keeps snapshot=Before; colors
   stable across steps.
4. **swap_sides regression**: intra-line inserted tokens on the After side
   remain `Added` in both panes' mirrored block lists.
5. **Palette flow**: Baryon's cell and bridge surfaces render from the
   palette entries; a UI-color override changes both end to end (including a
   bridge fingerprint change); source audit rejects hardcoded diff RGB.
6. Existing diff-engine tests updated only where they encoded PaneId-order
   chronology; the hybrid line/AST diff algorithm itself is untouched.

## Non-goals

- No diff algorithm changes (hybrid line/AST model untouched).
- No new comparison commands; commit-vs-commit ancestry is a noted
  extension only.
- No theme-system redesign — four new entries plus blends, nothing more.

## Review checklist

- [ ] `DiffRole` assigned at every pair-creation site; the construction API
      names `before_pane_id`/`after_pane_id`; no consumer of
      pane-id ordering for direction remains (recompute dedup may keep it).
- [ ] `swap_sides` swaps coordinates only; kind-flip gone.
- [ ] Git pairs: snapshot always Before, across creation order and steps.
- [ ] All four semantic entries in `UiPalette`; cell and bridge blends
      derived; bridge cache keys include the palette; audits extended; no
      hardcoded diff RGB remains in Baryon's diff surfaces.
- [ ] Acceptance test matches the screenshot scenario in both pane orders.
- [ ] The four built-in defaults remain pairwise distinguishable under
      deuteranopia/protanopia sanity transforms. User overrides round-trip
      exactly; arbitrary custom colors cannot carry a contrast guarantee.
