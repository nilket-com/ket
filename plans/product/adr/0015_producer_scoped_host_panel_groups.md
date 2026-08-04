# ADR 0015: producer-scoped host panel groups

Status: accepted 2026-08-03; §5's producer-reopen sentence amended the same day
after the implementation feasibility audit distinguished last-view closure from
actual buffer release. This record decides one thing: that the host-panel
protocol may request one engine-owned output group for a producer, and that the
engine may automatically tile that group's views until the author changes its
internal layout. It adds no Rune surface, infers no tabs, and authorizes no
layout persistence.

## Context

PRODUCT-0010 made every Rune view an ordinary host panel. PRODUCT-0011 then
correctly refused to infer that those panels were alternatives: automatically
tabbing one run hid three of the four outputs that the multi-origin work existed
to show together. Its §4 therefore leaves every view visible in a separate pane,
and says that bounding the split count waits for evidence rather than a grouping
guess.

The evidence is now repetitive. The buffers `../uat/test3.rn` through
`../uat/test6.rn` each require their output panes to be arranged by hand, and a
new producer receives a fresh chain of splits even when the author wants the same
ordinary comparison layout. Rerunning one producer already preserves live panel
identity and placement, so the recurring cost is principally **arranging a new
program at all**, not restoring an old arrangement after every query.

Tiling does not reopen PRODUCT-0011's tab decision. Tabbing hides content; tiling
keeps every member visible and changes only geometry. The asymmetry behind
PRODUCT-0011 §4 therefore still holds: no view may disappear behind an inferred
tab, and an engine that cannot honor a tile request must fall back visibly.

The dogfood arrangement also corrects one tempting inference. Its four panels
appear as table 1 | layered plot 2 over bar plot 4 | table 3, so positions 3 and
4 are **not** a row-major projection of presentation order. What the screenshot
does support is shape: both tables are narrow and both plots are wide. The record
therefore chooses row-major order as a stable default derived from an existing
oracle, not as a claim about that hand-built arrangement. The automatic/manual
rule below makes a wrong default cost one correction, once.

The existing split tree is already the only component allowed to realize this
layout. `OpenHostPanel` says the engine owns panel geometry and tab membership
(`protocol.rs:335`), `Session::move_pane` says the engine alone mutates the split
tree (`session.rs:4805`), and divider resizing is likewise a session operation
(`session.rs:4791`). The missing object is not another pane grid. It is an
engine-owned tag over a subtree of the existing one, plus an atomic request that
can name all members before any are placed.

Finally, placement happens before results exist. `prepare_rune_view_panels`
establishes panels *"in presentation order before any query completes"*
(`editor.rs:798`) and only then dispatches independent origins. At that point
`ViewPresentation.content_kind` is known; a table's frame and column count are
not. Any layout derived from completed content would reflow asynchronously as
origins finished, which is precisely the instability this record exists to
remove.

## Decision

### 1. A host-panel group is an engine-owned subtree

A host-panel group is a session-scoped logical owner of placements in the
engine's existing pane split tree. On first materialization, the engine splits
the source pane once and realizes the group in the sibling subtree. Members tile
inside that subtree; they are not nested inside a dashboard, private pane grid,
or frontend-owned layout object.

The group is identified by an opaque `HostPanelGroupId`. It communicates neither
Rune relation origin nor view identity to libaryon. Polariton allocates one group
identity per producer and maintains the producer-to-group association exactly as
it maintains producer-to-panel associations today. A `HostPanelId` may be a
current member of at most one group.

Two producers have two groups. Their presentation orders never interleave, and
neither producer may cause the other's subtree to reflow. This remains true when
two source buffers happen to contain identical Rune text or relation origins;
execution identity and layout ownership are different axes.

The source-pane split is outside the group. Resizing that boundary changes the
space available to the group but does not change the group's internal layout
state.

### 2. Group establishment is one atomic, advisory request

The protocol gains an operation with this semantic shape:

```text
EnsureHostPanelGroup {
    group_id: HostPanelGroupId,
    anchor: Option<PaneId>,
    members: [
        HostPanelGroupMember {
            panel_id: HostPanelId,
            kind: String,
            title: String,
            shape: Compact | Expansive,
        },
        ...
    ],
    layout: RowMajorTile,
}
```

`members` is the complete view set from the producer's latest successfully
analyzed revision, in PRODUCT-0010 §2 presentation order. The descriptor fields
give the engine everything `OpenHostPanel` needs to create a missing panel; a
frontend must not open a chain of temporary splits and ask the engine to gather
them afterward. Existing panels are rebound through the existing revision
protocol and keep their content and identity.

The request is atomic with respect to layout: the engine either realizes one
coherent group update or declines the group placement. It does not leave half the
new members grouped and half individually split. As in PRODUCT-0011 §2, the
request is advisory. On decline, every missing member still opens visibly by the
engine's split fallback, in request order, and no placement failure reaches the
frontend. A group request never steals focus.

The optional anchor is the source pane from which the run was requested, captured
before dispatch. It is consulted only when no live subtree exists. A missing,
stale, or otherwise unusable anchor is a normal reason to use the visible
fallback, not an error and not permission to relocate an existing group beside
whatever happens to be focused later.

`OpenHostPanel` remains the operation for independent panels. A per-panel
`HostPanelPlacement::Tile` cannot implement this record: until the engine sees the
complete membership and anchor together, each placement decision changes the
geometry against which the next one would be made.

### 3. Polariton supplies order and generic shape, not geometry

Polariton's group policy is fixed:

- one opaque group per Rune producer;
- members in PRODUCT-0008/0010 presentation order: output-root source position,
  then list order, then chain order;
- `ViewContentKind::Table` requests `Compact`;
- `ViewContentKind::Plot` requests `Expansive`.

The member list is emitted in that order, without compensation for the engine's
old per-panel insertion behavior. `prepare_rune_view_panels` currently iterates
`resolved.into_iter().rev()` because repeated splits beside the focused source
would otherwise project backwards (`editor.rs:834`). An atomic group request
receives the final order directly, so this record retires both the reversal and
the per-panel first-appearance loop. Preserving the workaround would reverse the
order a second time and violate the contract rather than protect it.

`Compact` and `Expansive` are generic presentation classes, not table and plot
semantics in libaryon. The engine owns the exact row count, split ratios, minimum
extents, and degradation under constrained bounds. For the four-member dogfood
fixture, an honored request produces a 2x2 row-major tile in which an Expansive
member is wider than a Compact sibling when the available bounds permit it.

Presentation order remains a logical guarantee; exact pixels do not become one.
For other member counts the engine chooses a balanced visible tiling within its
minimum-pane constraints, traversed row-major. If it cannot keep every member
legible, it may decline and use the visible fallback. It may not satisfy
constraint pressure by inventing tabs.

No column count, rendered bounds, kernel preference, or completed frame enters a
shape request. In particular, a wide table is still `Compact` in this first cut.
Changing that rule requires evidence and a timing design that does not move panes
as independent origins complete.

### 4. Automatic and Manual are group layout states

Each materialized group has one layout state:

```text
Automatic
  author has not changed geometry inside the group;
  membership changes may recompute the automatic tile

Manual
  author changed geometry inside the group;
  automatic updates preserve every surviving placement
```

Creation and explicit reset enter `Automatic`. These user actions inside the
group enter `Manual`:

- dragging an internal divider;
- swapping or reordering its panes;
- inserting a pane into a different position in the subtree;
- creating or changing tab membership inside the subtree;
- closing a member pane.

Window resizing and resizing the source/group boundary do **not** enter
`Manual`. They resize the bounds in which the same split ratios are rendered.
Focus changes, tab activation, content scrolling, result completion, revision
refresh, stale/error state, and title changes do not enter `Manual` either.

Moving a member out of the group is **detachment**, not an edit to the geometry
that remains. The member keeps its producer ownership but leaves automatic
placement membership; the remaining group does not become Manual solely because
of that removal. A destination group does not acquire the moved panel as one of
its producer members, even if the panel is physically placed in its subtree.

This state is engine-owned because only the engine observes every operation that
mutates the split tree. A frontend-maintained dirty bit would miss user actions
or race the authoritative geometry.

### 5. A successful analysis updates current membership, not retained placement

The engine distinguishes two sets:

- **current members:** the ordered members in the latest successful
  `EnsureHostPanelGroup` request;
- **retained placements:** former members whose panels still occupy group tiles.

Omitting a prior member from a successful request moves its live placement from
current to retained. It does not close the panel and does not reflow the group.
This is the layout realization of PRODUCT-0010 §6's orphan rule: commenting out a
view must not make every neighboring pane jump while the author is editing.

A parse or envelope failure sends no new authoritative membership. It leaves both
sets untouched, matching PRODUCT-0010's rule that failure is not deletion. An
origin or sink failure likewise changes panel content/status, never membership or
geometry.

If a retained named view returns, its existing `HostPanelId` becomes current and
reuses its tile. Anonymous lifecycle remains exactly as weak as PRODUCT-0010
allows: if Polariton closes or recycles an excess anonymous destination, the
group observes the ordinary panel disposal rather than upgrading anonymity into a
persistence guarantee.

Closing the producer buffer is not a successful membership update and does not
dispose its outputs. The group and its placements remain live but receive no
automatic membership update; its panels continue under libaryon's ordinary
reference-counted host-panel lifecycle and may still be arranged or closed by the
author. With no producer there can be no later `EnsureHostPanelGroup`, and
**Retile outputs is unavailable** because there is no authoritative current
program to reconcile.

Once the producer buffer has actually been released, reopening the file creates
a new session `BufferId` and therefore a new group; reattaching it to the
producerless group would require the durable identity and layout persistence that
§9 defers. Once the producer is gone and its last panel placement is closed, the
engine may discard the now-unobservable group bookkeeping.

### 6. Membership changes preserve intent according to state

In an `Automatic` group, genuinely new attached members may recompute the
automatic tile. Current and retained placements participate, so an orphan stays
visible rather than being ejected as an incidental cleanup. Reissuing the same
current set, completing queries in a different order, or changing only producer
state is not a membership change and must preserve the split tree exactly.

In a `Manual` group, a new current panel is appended visibly within the group
without moving or resizing any surviving placement. The resulting arrangement
need not look automatic; preserving the author's work is more important than
making a dirty group look as though it were never touched.

Closing a member removes that placement and enters `Manual`, but does not
normalize the remaining subtree. This record adds no persistent hide state: if a
later successful run needs that current panel again, it is a missing current
member and follows the Manual append rule. **Closing hides a member only until
the next successful run; durable hiding is not authorized here.** Moving a panel
out is different: its detached placement is deliberately preserved outside the
group across reruns and is reclaimed only by reset.

### 7. Reset means the automatic layout of the current program

The protocol gains `ResetHostPanelGroup { group_id }`, exposed by Polariton as a
**Retile outputs** action. The exact command spelling and binding are not decided
here, but while the producer is live the action must be reachable from either the
source pane or one of its output panels. It is unavailable after producer
closure, as §5 requires.

“Current program” means the latest successfully analyzed authoritative member
set. A malformed edit cannot turn reset into deletion. Reset performs one
reconciliation:

1. retained orphan placements are removed from the group;
2. detached current panels are reclaimed, including a panel the author moved
   into another producer's subtree;
3. missing current panels are reopened;
4. foreign placements are preserved visibly outside the reclaimed subtree;
5. current members are tiled in presentation order; and
6. the group returns to `Automatic`.

Reclaim wins because a moved panel still realizes a view owned by its producer;
physical placement inside another subtree never transferred that ownership. The
other group has no competing membership claim and collapses the vacated leaf by
the engine's normal split-tree rule.

Reset is the sole moment at which layout is re-derived from exact program
correspondence. This supplies an exit from detachment without inventing a second
“reattach” command, and it gives retained orphan tiles a deliberate end of life.
Ordinary `PanelDisposed` reference-counting still governs when removed orphan
content may be released; reset does not create a parallel content-lifecycle
event.

One no-op is normative: resetting an `Automatic` group with no detached current
members, no retained placements, no foreign placements, and no missing current
panels leaves its split topology and ratios exactly unchanged. Reset is not a
button for perturbing an already-current automatic layout.

### 8. Tabs remain explicit

No automatic path in this record issues `TabWith` or creates a tab stack. A user
may tab group members manually, which enters `Manual` and is then preserved.

A future language record may say that some views are alternatives and request a
tab grouping. That signal would compose with this group model and finally give
`TabWith` an automatic caller, but neither relation origin, sink form, source
adjacency, nor shared execution is such a signal today.

### 9. Session scope is deliberate

Group identity, Automatic/Manual state, retained placements, detachment, and
reset bookkeeping are session-scoped. No part of this record writes layout to
disk or produces `PanelRestored`.

This does not weaken the immediate behavior: named panels and groups remain
stable across reruns in one session, which is where the repeated arranging cost
is observed. Across-restart restoration requires general workspace layout
persistence first. Only then does PRODUCT-0010 §5's durable document key become
observable enough to gate.

### 10. Acceptance gates

1. Four first-seen view panels are established by one group request as a sibling
   output subtree after one source-pane split, with all four visible and no
   inferred tab stack.
2. The four-member dogfood fixture is emitted directly, with no reverse-emission
   compensation, and tiles 2x2 in presentation order; its table members request
   `Compact`, its plot members request `Expansive`, and each Expansive member is
   wider than a Compact sibling when bounds permit.
3. Two producers receive distinct opaque group identities and distinct subtrees;
   running either producer cannot reorder or resize the other's members. Closing
   a producer leaves its group and panels live without transferring them or
   applying further automatic membership updates, makes reset unavailable, and
   permits unreachable bookkeeping to be collected only after the last panel
   placement closes.
4. A stale anchor or an unsatisfiable tile is advisory: every missing panel still
   opens visibly in request order, no partial group survives, and no placement
   error reaches Polariton.
5. Rerunning an unchanged successful program preserves pane identities, split
   topology, ratios, focus, and tab activation, including when origins complete
   in the opposite order.
6. Query completion, table column count, plot preparation, descriptor title or
   title length, origin failure, sink failure, and stale/error transitions never
   change group geometry. No post-completion content property is consulted for
   placement.
7. Resizing an internal divider enters `Manual`; a rerun then preserves all
   surviving ratios. Window resizing and the source/group boundary do not enter
   `Manual`.
8. Adding a member to an `Automatic` group recomputes its visible tile. Removing
   a member from the successful program retains its orphan tile without reflow,
   and a returning named member reuses that tile.
9. Adding or reopening a member in a `Manual` group appends it visibly without
   changing any surviving pane's topology or ratios. Closing a current member
   hides it only until the next successful run, which reopens it by this rule.
10. Moving a current panel out detaches it without dirtying the remaining group;
    reruns preserve that placement. Reset reclaims it even from another
    producer's subtree, without transferring producer ownership.
11. Reset removes retained orphan placements, reopens missing current members,
    preserves foreign panes visibly, tiles the latest successfully analyzed
    current set, and returns the group to `Automatic`. Parse failure cannot alter
    the set it uses.
12. Resetting an already-current `Automatic` group with no reconciliation work is
    an exact layout no-op.
13. `RESULT_PANEL`, `DASHBOARD_PANEL`, independent `OpenHostPanel` placement,
    manual `TabWith`, dashboard state, and every Rune envelope/lowering behavior
    remain unchanged.

### 11. The stop condition

> This record may add opaque host-panel group identities, atomic advisory group
> establishment, engine-owned session layout state, generic Compact/Expansive
> shape requests, reset, and the Polariton policy in §3. If implementation
> requires a second pane tree, frontend-owned geometry, inferred tabs, a Rune
> syntax or semantic change, result-dependent reflow, layout persistence, durable
> document identity, or a change to PRODUCT-0010's panel-content lifecycle, the
> work stops and the requirement returns as its own record.

The expected boundary is persistence. Serializing group topology or making an
opaque group identity survive restart is editor-wide workspace work, not a
convenient extension of this session feature.

### 12. What this record does not authorize

No Rune grouping or sizing argument. No author-supplied geometry. No automatic
tabs. No content-derived table width after dispatch. No dashboard migration,
retirement, or export change. No layout persistence and no `PanelRestored`
producer. No durable document identity. No cross-producer group. No scale, guide,
interaction, facet, execution, or transport change. No change to panel naming,
revision, stale, failed, or orphaned content semantics.

## Rejected

- **A `Tile` variant on each `OpenHostPanel`:** placement is order-dependent and
  cannot know final membership or reserve one source split. It recreates the
  current chain one request at a time before attempting to repair it.
- **Automatically tabbing a producer's views:** hides comparisons and repeats the
  PRODUCT-0011 §4 failure. Tiling exists precisely because visibility and compact
  geometry are separable.
- **One workspace-wide output region:** interleaves independent programs and
  makes one producer's membership changes rearrange another's results.
- **A frontend-owned dashboard or pane grid:** duplicates the engine's split tree,
  drag/resize behavior, focus, and tab model. The dashboard is evidence of that
  cost, not the substrate for repeating it.
- **Inferring the exact hand-built screenshot layout:** positions 3 and 4 disprove
  row-major order, while shape alone cannot explain their swap. The record uses
  presentation order as a deterministic default and lets one user correction
  enter Manual.
- **Sizing tables from column count:** the frame does not exist when the group is
  placed. Applying the hint later would move panes as independent queries finish.
- **Author-supplied `compact`, `wide`, row, or column hints:** puts host geometry
  into Rune before content-kind defaults have produced evidence of a miss. It
  also violates PRODUCT-0010 §4's separation between a view and its realization.
- **Reflowing Manual groups on membership change:** silently destroys deliberate
  geometry. Appending a new member may be less tidy and is the only choice that
  preserves what the author actually did.
- **Removing orphan tiles immediately:** makes the layout jump while a view is
  commented out or temporarily deleted. Reset is the explicit reconciliation
  point.
- **Resetting only what remains in the subtree:** makes detachment a one-way door
  and requires a second reattach command. Reset means the automatic layout of the
  current program, not normalization of an accidental remainder.
- **Persisting this first:** does not reduce the repeated first-arrangement cost
  for new scripts and cannot be gated until the editor restores workspace
  layouts. Session behavior is observable and useful now.

## Consequences and sequencing

- A new Rune producer gets one visible output region instead of a chain of
  unrelated source splits, while every view remains simultaneously visible.
- A close default converges after one manual adjustment and remains stable for
  the session. Reset is the only automatic operation permitted to discard that
  adjustment.
- `EnsureHostPanelGroup` is the production caller that earns a protocol concept
  beyond PRODUCT-0011's per-panel first-placement request. `TabWith` remains
  manual until a language grouping signal exists.
- Polariton retires `prepare_rune_view_panels`' reverse-emission workaround. The
  final presentation order crosses the protocol directly, removing a coupling to
  the engine's old repeated-split insertion behavior.
- Libaryon gains session-only group metadata over its existing split tree;
  Polariton gains producer-to-group allocation and one Retile outputs action.
  Neither gains a second content registry or layout model.
- Workspace layout persistence remains next in its own editor-wide arc. It must
  decide restoration of ordinary document panes, tabs, panel groups, group state,
  and detached placements together; durable named-view identity follows that
  observable capability rather than preceding it.
- If Compact tables repeatedly need widening, or row-major repeatedly needs the
  same manual rearrangement, those misses become evidence for a later generic
  layout signal. This record does not guess that language in advance.
