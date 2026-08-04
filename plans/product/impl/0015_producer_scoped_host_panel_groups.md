# Producer-scoped host panel groups

Scope: implement PRODUCT-0015 as one libaryon layout capability and one
Polariton policy. Add atomic advisory host-panel groups over libaryon's existing
split tree, preserve author-owned group geometry, reconcile it only on explicit
reset, and make Rune views the first production caller. Deliverable is four
`impl:` commits, one per implementation stage, followed by one evidence-only
commit.

The implementation is judged by PRODUCT-0015 §10's thirteen gates. Every gate is
assigned to exactly one stage below. No implementation commit may leave a known
broken intermediate state.

## Halt rule

PRODUCT-0015 §11 binds this work:

> This record may add opaque host-panel group identities, atomic advisory group
> establishment, engine-owned session layout state, generic Compact/Expansive
> shape requests, reset, and the Polariton policy in §3. If implementation
> requires a second pane tree, frontend-owned geometry, inferred tabs, a Rune
> syntax or semantic change, result-dependent reflow, layout persistence, durable
> document identity, or a change to PRODUCT-0010's panel-content lifecycle, the
> work stops and the requirement returns as its own record.

Concretely, the work stops if it adds a second hierarchy of rows, columns, panes,
or bounds beside `LayoutNode`; if group state stores a copied topology; if any
projection field is added; if Polariton supplies ratios, rows, columns, or pixel
bounds; if a query result, table frame, title length, or plot kernel feeds layout;
if any automatic path requests `TabWith`; or if work enters the Rune envelope,
Toron, the transport, dashboard state, or layout persistence.

### Where work may land

- `crates/libaryon/src/engine/host_event.rs` — opaque group identity beside
  `HostPanelId` only;
- `crates/libaryon/src/engine/protocol.rs` and `engine/mod.rs` — public group
  request types and commands;
- `crates/libaryon/src/engine/session.rs` — the sole split tree, its group-root
  tag, session group registry, tiling and reconciliation helpers;
- `crates/libaryon/src/engine/runtime.rs` — command dispatch and the existing
  pane/tab mutation boundaries which update group state;
- `crates/libaryon/src/engine/runtime/system.rs` — live protocol gates;
- `crates/polariton/src/editor.rs` — producer-to-group allocation, forward group
  emission, Retile outputs action, and integration gates;
- this plan, its evidence file, and the Rune workspace corpus in the final
  evidence-only commit.

`crates/libaryon/src/uast/projection.rs`, `crates/polariton/src/gui/dashboard/`,
`crates/polariton/src/rune_relation_envelope.rs`, every plot module, and every
Toron/transport file are excluded. A required edit in one of them is a halt.

## Stage 0: feasibility finding — one annotated split tree is sufficient

No protocol or Polariton work begins until the following finding has been
reviewed as part of this plan. Stage 0 is the pre-implementation audit recorded
here; it does not produce an implementation commit or discharge an acceptance
gate. The final evidence file repeats the chosen branch and records whether any
implementation fact contradicted it.

### 0a. A group root can live on `LayoutNode`

The engine has one authoritative topology:

```text
LayoutNode = Split { direction, children, ratios } | Leaf(LayoutLeaf)
```

Every operation PRODUCT-0015 needs already mutates that object:

- `split_leaf_inner_with_order` replaces a leaf with a split;
- `take_leaf` removes a leaf and collapses a one-child split;
- `resize_boundary_inner` walks the actual ancestor chain and mutates its
  ratios;
- `move_pane` exchanges leaf contents or removes and reinserts one existing
  pane; and
- `collect_pane_layouts` derives the only projected geometry from this tree.

The feasible representation is an optional **group-root tag on the existing
node**, present on both shapes so a one-member group remains representable:

```text
LayoutLeaf {
    group_root: Option<HostPanelGroupId>,
    pane_id,
    pane,
}

LayoutNode::Split {
    group_root: Option<HostPanelGroupId>,
    direction,
    children,
    ratios,
}
```

Exactly one live node carries a materialized group's tag. Splitting a tagged
leaf moves the tag to the new parent split. Collapsing a tagged split moves the
tag to its surviving child. Taking or rebuilding a whole group moves the tagged
node itself. Removing the last attached member may leave the registry with no
live root until reset or a later append materializes one.

The session registry keyed by `HostPanelGroupId` stores only non-geometric facts:

```text
HostPanelGroupState {
    producer_buffer_id,
    layout_state: Automatic | Manual,
    current_members: ordered panel identities and shapes,
    retained_members,
    detached_members,
    latest_valid_anchor,
}
```

It stores no child edges, row/column tree, pane rectangles, or copied ratios.
Queries locate the one tagged root in `LayoutNode` and resolve member panel ids
through the live buffer/view/pane relationships the engine already owns. This is
metadata over the split tree, not a second pane tree.

Add debug/test invariants before relying on the representation:

- no group id tags more than one node;
- no node carries two group ids;
- a current panel belongs to at most one group;
- a materialized root's attached member panes occur beneath that root; and
- a detached member does not count as an attached leaf even when physically
  placed inside another group's subtree.

If preserving the tag through split, collapse, swap, edge move, tab change, and
close cannot be expressed on this representation, Stage 0 fails and the work
stops. Do not add a registry of parent/child edges as a workaround.

### 0b. Atomic construction needs no intermediate pane system

`EditorSession::create_host_panel_view` already constructs a host-panel buffer
and detached `View` before placement. Stage 1 can therefore:

1. resolve existing member views and construct every missing view off-tree;
2. wrap each new view in its ordinary `EditorPane`;
3. build a candidate `LayoutNode` subtree from those panes using the same split
   and ratio representation as the workspace;
4. validate the candidate against the prospective output bounds; and
5. insert the tagged subtree beside the source with one
   `split_leaf_with_subtree` mutation.

Command dispatch completes before the next render, so no temporary split chain is
projected. On advisory decline, the candidate is not inserted; every missing
panel is instead placed through the existing visible split fallback. No
partially-tagged group survives.

The fallback placements are recorded as detached placements of an
**unmaterialized** group. Rerun must not later gather them implicitly, because
that would relocate first appearances PRODUCT-0011 promises to preserve. A later
valid anchor may be remembered, but only explicit reset may reclaim fallback
placements and materialize the group.

This is the branch Stage 1 must take. Building an inner dashboard/pane grid and
projecting it as one outer pane is forbidden even if it would make atomicity
easier.

### 0c. Producer closure is observable inside the engine

A valid first anchor identifies an ordinary source pane. The engine can capture
its active `BufferId` as the group's generic owner without receiving Rune origin,
view identity, file path, or a new projection field.

“Producer buffer closes” means that `release_buffer` actually removes that
`BufferId`. Merely closing its last view is not necessarily closure: jump history
may retain the live buffer, in which case reopening correctly reuses both its
`BufferId` and group. Once the buffer is actually released, the group becomes
producerless, reset declines, no further ensure for that group is accepted, and a
later file load necessarily receives a new `BufferId` and Polariton group id.

The ordinary buffer-release path can mark matching groups producerless before
removal. The host-panel release path can discard producerless group bookkeeping
after its last member placement disappears, clearing any surviving root tag
without touching foreign panes. No `HostEvent`, lifecycle projection, durable
identity, or persisted mapping is needed.

### Stage 0 result

The record is feasible on the existing split tree. The group-root tag is the only
new topology annotation; the session registry carries no geometry. Detached view
construction supplies atomicity, and generic buffer ownership supplies the
producer-closure boundary. If implementation evidence contradicts any part of
this finding, the halt fires before Stage 1 is committed.

## Representation and protocol

Add these public request types beside the existing host-panel protocol:

```text
HostPanelGroupId(u64)

HostPanelGroupShape = Compact | Expansive
HostPanelGroupLayout = RowMajorTile

HostPanelGroupMember {
    panel_id: HostPanelId,
    kind: String,
    title: String,
    shape: HostPanelGroupShape,
}

EditorCommand::EnsureHostPanelGroup {
    group_id: HostPanelGroupId,
    anchor: Option<PaneId>,
    members: Vec<HostPanelGroupMember>,
    layout: HostPanelGroupLayout,
}

EditorCommand::ResetHostPanelGroup {
    group_id: HostPanelGroupId,
}
```

The ids and shapes are requests, not projected state. Add no acknowledgement,
decline event, group viewport, pane rectangle, or frontend-chosen ratio.

`members` is the complete current program from the latest successful analysis.
It may be empty for an existing group, which moves prior current members to
retained without closing or reflowing them. An empty request for an unknown group
is a no-op and allocates no panels or tree node.

## Stage 1: protocol, tagged roots, and atomic first tiling

### Group construction

Implement the public types and command routing, then add group-root-aware
construction to `LayoutNode` and `EditorSession`.

`RowMajorTile` uses the existing split directions:

- rows are children of a horizontal split;
- members within a row are children of a vertical split;
- a one-member row is an ordinary leaf;
- row traversal is request order;
- the engine selects a balanced row/column count from member count and available
  bounds; the four-member fixture is exactly two rows of two; and
- within one row, ideal ratios are `Compact = 1` and `Expansive = 3`, constrained
  by the existing minimum-pane extents.

Exact pixels remain engine policy. The tests pin the four-member requirement and
the relative Compact/Expansive width, not one universal ratio after minimum-size
clamping.

On first materialization, insert the candidate to the right of the valid source
anchor with one vertical source/output split, retain source focus, and tag only
the candidate root. If the anchor is unavailable, the group bounds cannot meet
minimum extents, a member id conflicts, or candidate insertion fails, decline the
group atomically and use visible non-focusing splits in member order. Do not infer
tabs under constraint pressure.

Known live panels retain their exact buffers and views. Missing identities use
the same buffer/view creation and eventual `PanelDisposed` lifecycle as
`OpenHostPanel`. `OpenHostPanel` itself remains unchanged for independent panels.

Stage 1 adds **no Polariton caller**. Its existing reverse-emitted per-panel
splits remain correct until Stage 4. This distinction is load-bearing: adding an
unused protocol capability does not change presentation, while switching the
production caller without retiring reversal would.

### Behavioral evidence

Drive commands through a live engine:

- establish four new host panels with one request and observe one source split,
  one tagged output subtree, a 2x2 visible projection, request order, source
  focus, and no tabs;
- assert table/plot vocabulary never enters the engine types by exercising only
  generic shapes;
- request a stale anchor and an intentionally unsatisfiable candidate, observing
  visible ordered fallback, no partial tag, and no placement failure; and
- rerun the same request to prove fallback panels are not silently gathered.

**Gates discharged: 1 and 4.**

Commit: `impl: add atomic host panel groups`.

## Stage 2: Automatic/Manual state and membership updates

### Detect author mutations at the session boundary

Only successful workspace mutations change group state. Centralize the checks at
the existing engine-owned boundaries rather than teaching GUI hosts to report
dirty state:

- a successful internal `resize_pane_boundary` or keyboard pane resize marks the
  containing group Manual;
- a successful swap/reorder or insertion within a group marks the destination
  group Manual;
- adding, closing, or changing a tab within a group marks it Manual;
- closing a group member pane marks it Manual before the leaf is removed; and
- focus, tab activation, scrolling, descriptor/revision changes, terminal resize,
  and a source/group boundary resize do not mark it Manual.

Classify a divider as internal only when the tagged group root contains both
sides and the mutated ancestor lies beneath that root. This prevents the outer
source/output boundary from becoming dirty merely because it shares a group
member on one side.

Every existing split/collapse helper must preserve the unique root tag. Add
targeted structural tests for a one-member tagged leaf becoming a split, a tagged
split collapsing to one child, internal center swap, edge insertion, tab close,
and closing the last attached member.

### Update membership without reinterpreting producer state

On every successful `EnsureHostPanelGroup`:

- replace `current_members` with request order;
- move omitted live current members to retained, preserving their tile exactly;
- turn returning retained members current in their existing tiles;
- leave detached current members detached;
- create missing current panels; and
- never inspect content, revision outcome, descriptor length, or result state.

An Automatic group may rebuild its attached current-plus-retained tile when a
genuinely new attached member appears. Omission alone never rebuilds. Reissuing
the same set is a structural no-op.

A Manual group appends new or reopened panels by wrapping the untouched existing
group subtree and a new visible branch in a new tagged root. Existing descendants
keep their exact topology and ratios; only the outer bounds available to that
subtree may shrink. Do not recursively rebalance it to make the result prettier.

Closing a current member removes its placement and enters Manual. It does not add
a hidden/suppressed state. The next successful ensure sees one missing current
panel and reopens it through the Manual append path.

### Behavioral evidence

- Resize an internal divider, rerun, and compare the tagged subtree topology and
  ratios exactly. Resize the source/group boundary and terminal bounds as
  controls; neither changes the state.
- In Automatic, add a fifth member and observe a recomputed visible tile. Omit a
  member and observe its retained tile and every ratio unchanged; return its
  named id and observe the same pane.
- In Manual, add a member and close/reopen one. Assert the old subtree remains
  structurally identical while each missing panel appears visibly on a new
  branch.

**Gates discharged: 7, 8, and 9.**

Commit: `impl: preserve authored host panel group layout`.

## Stage 3: detachment, reset, and producer lifecycle

### Detachment preserves ownership

Before `move_pane` mutates the tree, resolve the source and destination group
relationships from live host-panel identities:

- an edge move out of the owner root marks the member detached and leaves the
  remaining owner state unchanged;
- a center swap with an outside pane keeps the root at its position, marks the
  moved member detached, and treats the incoming pane as foreign;
- placing a detached panel inside another group's subtree makes that destination
  group Manual but never adds the panel to its producer membership; and
- later ensure requests from the owner preserve the detached placement.

If the last attached member leaves, the registry remains authoritative with no
live root. The detached panel does not carry the group tag to its new location.

### Reset is a preflighted reconciliation

`ResetHostPanelGroup` first resolves the latest current set, producer liveness,
tagged root, current placements, retained placements, detached placements,
missing panels, and foreign panes. It then either performs the entire
reconciliation or leaves the workspace unchanged.

For an available producer:

1. extract detached current panels from wherever they live, including another
   group's subtree;
2. remove retained orphan placements from the group without touching any other
   reference to the same host panel;
3. construct missing current panels;
4. extract foreign panes from the old group and keep them visible in a sibling
   subtree at the old group location;
5. build a fresh automatic tile from current members in request order;
6. insert or replace the tagged root atomically; and
7. set the group to Automatic with no retained or detached members.

Vacated source/destination trees collapse through the existing rule and preserve
their own group tags. Removed panels use ordinary buffer reference counting, so
only a last workspace reference emits `PanelDisposed`.

Before taking any pane, check the normative no-op: if the group is Automatic,
producer is live, current membership and order match, every current panel is
attached, and retained/detached/missing/foreign sets are empty, return without
rebuilding or normalizing a ratio.

### Producer closure

When actual text-buffer release removes a group's captured producer `BufferId`,
mark the group producerless. Its group root, Manual/Automatic state, membership,
and panels remain; user layout operations and ordinary panel close still work.
Further ensure and reset commands for that id decline without moving anything.

When the last live member placement of a producerless group closes, clear its
root tag while leaving any foreign subtree exactly where it is, then permit the
session registry entry to be dropped. If the buffer was retained by jump history
and therefore not released, this transition does not occur; the producer and
group remain live by engine identity.

### Behavioral evidence

- Detach a current member, rerun, and observe its external placement unchanged;
  move it into a second producer's group and reset the owner, observing reclaim
  without ownership transfer.
- Create retained, detached, missing, and foreign cases together, reset once, and
  observe exact current membership in presentation order, foreign visibility,
  ordinary disposal, and Automatic state.
- After one authoritative member set, simulate parse/envelope failure by sending
  no replacement ensure, then reset and observe that the prior successful set
  remains authoritative.
- Snapshot an already-current Automatic group, reset, and compare the complete
  split topology, ratios, pane ids, focus, and active tabs exactly.
- Actually release a producer buffer, observe live output panels and rejected
  reset, close the last member, and observe safe bookkeeping collection. Use a
  jump-history-retained buffer as the control that is not yet producer closure.

**Gates discharged: 10, 11, and 12.**

Commit: `impl: reconcile and retire host panel groups`.

## Stage 4: Polariton policy, direct order, and regression

### One production request replaces per-panel first appearances

Polariton owns a session counter and table:

```text
producer BufferId -> HostPanelGroupId
```

The group type is distinct from `HostPanelId`, so the counters cannot collide.
Allocate once per live producer identity. The panel identity map remains
unchanged; group identity is placement ownership and never enters
`ViewIdentity`, `ViewPresentation`, or the Rune envelope.

Refactor `prepare_rune_view_panels` to:

1. resolve every panel identity and descriptor in presentation order;
2. map `Table -> Compact` and `Plot -> Expansive` from
   `ViewPresentation.content_kind` before dispatch;
3. capture the source pane as the optional anchor;
4. send exactly one `EnsureHostPanelGroup` with the **forward** member list;
5. refresh known panels through `RefreshHostPanel` as today; and
6. mark local panel entries Running without sending any per-view
   `OpenHostPanel` first appearance.

In this same commit, delete `resolved.into_iter().rev()` and its repeated-split
comment. The production group request and reverse-emission retirement are one
indivisible change: neither may appear in an earlier/later commit, be hidden
behind a temporary flag, or be split into “cleanup” afterward. Stages 1–3 expose
an unused engine capability, so the old path remains correct until this exact
switch.

The `Ensure` member list carries descriptor titles, but shape reads only content
kind. No completion handler sends a group command. Out-of-order origins, title
changes, table widths, plot labels, success, failure, stale, and orphan status
therefore cannot reach geometry.

### Retile outputs

Add a Polariton-owned Retile outputs control which sends
`ResetHostPanelGroup { group_id }`. Resolve the group from either:

- the focused/source pane's producer `BufferId`; or
- a view panel's existing panel-to-producer identity mapping.

The control chooses no layout values. A producerless group may still have visible
panels, but the engine declines reset and Polariton must not manufacture a new
group from those panels. Exact icon/key binding remains presentation detail; its
accessible label is `Retile outputs`.

`RESULT_PANEL`, `DASHBOARD_PANEL`, direct independent host panels, and manual
`TabWith` continue through `OpenHostPanel` unchanged. No dashboard file changes.

### Gate 5 regression anchor

Gate 5 receives its own integration assay rather than being inferred from group
unit tests:

1. run one successful four-view program and record each panel/view/pane identity,
   the split topology and ratios, source focus, and every active tab;
2. rerun the unchanged program and complete its origins in the opposite order;
3. assert exact equality of every recorded property; and
4. assert that result completion emitted no placement command.

A projection-only rectangle comparison is insufficient for topology/ratio
identity, and a session-only structure comparison is insufficient for focus and
tab activation. The assay must cross both.

### Remaining integration evidence

- Drive the `test6.rn` four-view shape through `CodeEditor`: one group, forward
  table/layered/table/bar presentation order, 2x2 panes, Compact tables,
  Expansive plots, source focus, and no tabs. Do not read frame column counts.
- Run two producer buffers and prove independent group ids/subtrees. Release one
  producer and observe its panels remain, reset becomes inert, and the other
  producer is unchanged.
- Complete origins out of order while changing result frame width, plot label,
  and failure/stale state; compare group geometry before and after.
- Keep the PRODUCT-0011 direct `Split`/`TabWith`, result-panel, dashboard,
  PRODUCT-0010 identity/orphan, PRODUCT-0014 layered host-panel, and no-dashboard
  tests green without weakening their behavioral expectations.

**Gates discharged: 2, 3, 5, 6, and 13.**

Commit: `impl: tile Rune view panels by producer`.

## Final evidence-only commit

Write `plans/product/impl/0015_producer_scoped_host_panel_groups_evidence.md` and
update the Rune workspace corpus's view-placement row. Map all thirteen gates to
behavioral or structural evidence, record the Stage 0 branch and exact four
implementation commits, and state that the halt did not fire.

Advance the corpus from “one split per view” to producer-scoped automatic tiling
only after the implementation and installed `test6.rn` dogfood run both confirm
the visible 2x2 result. Record manual evidence separately from automated gates.
The evidence commit contains no Rust changes.

Commit: `evidence: close producer-scoped host panel groups`.

## Gate assignment ledger

| Gate | Stage | Proof burden |
| --- | --- | --- |
| 1 | Stage 1 | one atomic request, one source split, one visible group subtree |
| 2 | Stage 4 | forward 2x2 dogfood order and generic shape policy |
| 3 | Stage 4 | two producer groups plus producer-close behavior |
| 4 | Stage 1 | advisory all-visible fallback with no partial group |
| 5 | Stage 4 | exact unchanged-rerun identity/topology/focus/tab assay |
| 6 | Stage 4 | no completion/content/title property changes geometry |
| 7 | Stage 2 | internal dirtying; external/window resize controls |
| 8 | Stage 2 | Automatic add, retained omission, named return |
| 9 | Stage 2 | Manual append and temporary close/reopen |
| 10 | Stage 3 | detachment and cross-producer reclaim |
| 11 | Stage 3 | full reset reconciliation and parse-failure authority |
| 12 | Stage 3 | exact reset no-op |
| 13 | Stage 4 | independent panels, tabs, dashboard, and Rune regressions |

Every gate appears exactly once. Stage 0 is feasibility evidence and discharges
none.

## Verification

Before every implementation commit:

```text
cargo fmt --all -- --check
cargo test -p libaryon
cargo test -p polariton --features toron
cargo check -p polariton --no-default-features
git diff --check
```

Also inspect `git diff --name-only` against the permitted-file list and confirm:

- there is still one `LayoutNode` topology;
- no projection shape changed;
- no automatic `TabWith` caller exists;
- no result/completion path emits group placement;
- no Rune, Toron, plot, or dashboard file changed; and
- Stage 4 contains both the first Polariton `EnsureHostPanelGroup` caller and the
  deletion of reverse emission.

## Review checklist

- [ ] Stage 0 finding is recorded before Stage 1 and no second topology exists.
- [ ] A unique group-root tag survives leaf split, split collapse, close, swap,
      edge move, tab changes, and reset.
- [ ] Session group state stores membership and intent, never child edges, bounds,
      or copied ratios.
- [ ] First tiling is built off-tree and inserted atomically after one source
      split.
- [ ] Decline leaves every panel visible, no partial tag, and no later implicit
      gather.
- [ ] Compact/Expansive are generic; Polariton alone maps content kind to them.
- [ ] Internal mutations mark Manual; window and source-boundary resize do not.
- [ ] Omission retains a tile and does not reflow; parse/failure is not
      membership.
- [ ] Manual append wraps the old subtree without rebuilding its descendants.
- [ ] Closing is temporary; the next successful run reopens through Manual
      append.
- [ ] Detachment preserves external placement and never transfers ownership.
- [ ] Reset reclaims detached current members, removes retained orphans, preserves
      foreign panes, and is exactly inert when no reconciliation exists.
- [ ] Actual producer-buffer release, not mere view absence, makes a group
      producerless.
- [ ] PRODUCT-0010/0011 identity, focus, tab, stale/orphan, and refcount behavior
      remain green.
- [ ] Reverse emission and the first production group request land in one commit.
- [ ] Gate 5 compares identities, topology, ratios, focus, and active tabs across
      opposite completion order.
- [ ] Title length, frame width, plot preparation, and completion status never
      reach layout.
- [ ] Corpus and gate ledger land only in the evidence commit.
