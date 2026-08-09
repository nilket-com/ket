# ADR 0061: libaryon logical workspace restoration

Status: accepted 2026-08-08. Implemented with evidence in
`../impl/0061_libaryon_logical_workspace_restoration_evidence.md`. This record
makes libaryon persist and restore the
head-neutral workspace topology it already owns: logical splits, ratios, panes,
tab order, active and focused placement, host-panel placeholders, and
host-panel group arrangement. It also adds the single bounded application-owner
handoff required for Polariton to join its semantic meanings to that layout in
the following record. It persists no Polariton object and does not choose a
session surface.

## Context

PRODUCT-0059 established a locked, owner-partitioned session store whose
generations publish atomically. PRODUCT-0060 made the first owner real:
`libaryon_documents` restores exact ordinary-document content under durable
`DocumentId`s before the engine thread starts. When no later layout owner is
present, PRODUCT-0060 exposes those documents through one deterministic pane
whose tabs are ordered by ascending `DocumentId`.

That fallback is intentionally not layout persistence. It loses split
direction and ratios, duplicate views of one document, tab membership and
order, the active tab in each pane, focused placement, host panels, and
host-panel group arrangement. PRODUCT-0060 assigned the eventual exact
pane/tab topology to Polariton because the persistence partition had not yet
been traced through production.

Stage 0 shows that assignment is wrong. Polariton never receives a logical
editor layout. `PaneViewport` and `PaneDividerViewport` contain `PaneRect`s
already resolved for one current viewport and exist to paint and hit-test the
engine's answer. Reconstructing a split tree from those rectangles would make
the frontend infer engine-owned authority, encode one head's pixel geometry,
and change meaning at a different terminal or window size.

The logical representation already exists privately in libaryon:

```text
EditorSession.layout: LayoutNode

LayoutNode::Split {
    group_root,
    direction,
    children,
    ratios,
}

LayoutNode::Leaf {
    group_root,
    pane_id,
    pane: EditorPane { tabs, active_tab_index },
}
```

`LayoutNode` is head-neutral. Its ratios are integer weights, not rendered
widths or heights. The engine computes `PaneRect`s later from that tree and the
current viewport. Baryon and Polariton already consume the same projection, so
libaryon is the only owner that can restore the topology for both heads.

Stage 0 also found three unrelated runtime identity spaces that the word
"pane" had concealed:

| space | runtime type | owner | durable disposition |
|---|---|---|---|
| editor panes | `libaryon::core::PaneId(u32)` | libaryon | never serialize; mint on restore |
| host panels | `HostPanelId(u64)` | frontend | retain as the frontend's opaque durable join |
| nested dashboard panes | `gui::pane_grid::PaneId(usize)` | Polariton | out of scope |

`HostPanelId` needs no replacement. Polariton already owns its ID space:
`RESULT_PANEL` is 1, `DASHBOARD_PANEL` is 2, and dynamic Rune-view panels are
allocated from `1 << 63`. Libaryon's public documentation says the value is
meaningful only to the frontend that created it; `OpenHostPanel` accepts that
frontend-authored token and the engine treats it as opaque. In a saved session,
the same token can therefore join two sibling owners without becoming a global
or libaryon-minted identity:

```text
libaryon layout owner:
    HostPanelId -> descriptor and logical placement

later Polariton owner:
    (DocumentId, LocalViewIdentity) -> HostPanelId
```

Deriving identity from `HostPanelDescriptor { kind, title }` would be wrong.
Titles are presentation and can change; two panels may legitimately share both
fields. A second durable panel newtype would duplicate the identity Polariton
already controls.

Host-panel groups have the same partition. `HostPanelGroupId` is also a
frontend-authored opaque token. Libaryon owns its group root, automatic versus
manual arrangement, current/retained/detached membership, shapes, and latest
valid placement. Polariton owns which durable Rune producer the group means.
Runtime `producer_buffer_id` and `latest_valid_anchor: PaneId` cannot cross a
restart verbatim.

The nested dashboard is not a smaller copy of this problem. Its
`DashboardState` owns a separate pane grid whose leaves contain
`PlotPaneState`, and every `PlotPaneState` contains an `Arc<dyn PlotKernel>`.
Manual plots depend on the transient `AppState.data_frame`; notebook pane plots
may depend on an unrestored Toron `:memory:` database; only the legacy plain
Rune-buffer path has an obvious durable document producer. Persisting only the
tree would restore meaningless empty panes, while persisting kernels or
DataFrames would make derived execution products authoritative. The existing
dashboard-fate cluster remains the honest place to decide whether that surface
becomes a curated pinboard or disappears in favor of ordinary host panels.

One additional substrate gap blocks the later Polariton owner. PRODUCT-0060
moves the sole `SavedSession<Writable>` into the engine for its lifetime.
`EngineHandle::checkpoint()` accepts no application bytes. A frontend can read
an existing sibling owner before moving the session into the engine, but it
cannot update that owner in the same generation as the documents and layout.
Giving it the writable session back would violate PRODUCT-0059's one-writer
contract.

Passing opaque bytes through the checkpoint request is necessary but not
sufficient. The frontend builds those bytes before the request reaches the
engine FIFO head. Commands already ahead of it can open or close a document,
panel, or group. Publishing the frontend's earlier identity map with the
engine's later layout would create one internally inconsistent generation even
though each owner is individually valid.

The engine must not solve that race by parsing Polariton's schema. Owner
partitioning exists precisely so libaryon does not understand another crate's
meaning. Instead, the application supplies a small explicit identity witness
beside its opaque bytes. The engine compares that witness with the durable
identities it is about to commit and either publishes the complete generation
or refuses it.

Three identity sets are sufficient for the planned Polariton payload, but only
because two invariants hold together:

1. Polariton persists meanings only. Placement, tab order, ratios, descriptors,
   and physical group arrangement/membership remain exclusively in libaryon's
   owner and are captured at the FIFO head.
2. Polariton's semantic panel and group mappings are append-only and never
   reassign an existing identity. A new `(producer, LocalViewIdentity)` mints a
   fresh dynamic `HostPanelId`; a new producer group mints a fresh
   `HostPanelGroupId`. Removing a view marks its panel stale without deleting
   or recycling the mapping. Fixed `RESULT_PANEL` and `DASHBOARD_PANEL` roles
   do not participate in that semantic map.

Therefore an engine-side identity change relevant to the opaque mapping changes
at least one of the document, host-panel, or host-panel-group censuses. Moving a
surviving panel does not, and need not: the application asserted no placement
fact. This sufficiency disappears if either partition or append-only rule is
relaxed.

## Decision

### 1. Libaryon owns one strict logical-layout payload

Libaryon adds owner `libaryon_layout`, schema version 1. It is captured and
published in the same ordered checkpoint as `libaryon_documents`. It contains
logical workspace state only:

```text
LayoutCheckpoint {
    root: Option<LayoutCheckpointNode>,
    focused_leaf: Option<LeafPath>,
    panels: [HostPanelCheckpoint],
    groups: [HostPanelGroupCheckpoint],
}

LayoutCheckpointNode =
    Split {
        direction,
        positive integer ratios,
        children,
        group_root?,
    }
  | Leaf {
        tabs: [DurableTab],
        active_tab,
        group_root?,
    }

DurableTab =
    Document(DocumentId)
  | HostPanel(HostPanelId)
```

`HostPanelCheckpoint` stores one descriptor for one unique `HostPanelId`.
Content, scroll, column widths, plot kernels, DataFrames, textures,
`PanelRevision`, and frontend widget state do not enter the payload. Restored
host panels begin unresolved at revision zero; the descriptor supplies honest
placeholder chrome until their frontend producer reattaches.

The group table stores `HostPanelGroupId`, automatic/manual layout state,
ordered current members, retained members, detached members, member shapes,
and the structural group-root/anchor information needed to preserve placement.
It stores no `BufferId`, `PaneId`, producer `DocumentId`, or other semantic
producer binding. Restored groups begin with no runtime producer; the owning
frontend reattaches one explicitly through the same opaque group ID and a live
document anchor. Persisting `DocumentId -> HostPanelGroupId` here would duplicate
the meaning PRODUCT-0062 assigns to Polariton and would make the three-set
witness insufficient. Libaryon does not infer a producer from a title, kind,
tree position, or restored runtime ID.

The codec is strict and bounded. It rejects unknown fields, unsupported schema,
zero or duplicate identities, duplicate panel descriptors, inconsistent group
membership, invalid active indices, empty or singleton split nodes, child/ratio
length mismatch, zero ratios, impossible leaf paths, missing document
references, missing panel references, group roots without group records,
multiple roots for one group, and trailing bytes. It validates all counts and
extents before allocating their collections. The complete layout owner is
limited to 16 MiB; exceeding that limit refuses the checkpoint or restore
without changing the committed generation.

The payload contains no `BufferId`, `PaneId`, `ViewId`, `PaneRect`, viewport
size, rendered divider, cell coordinate, pointer position, or head-specific
widget ID. Capturing one logical workspace at substantially different terminal
or window sizes produces byte-identical layout-owner bytes.

### 2. Persistence filters tabs positively and canonicalizes the remainder

The layout capture walks the engine-owned `LayoutNode`; it never reconstructs
from a `Viewport`. A tab enters the durable topology only when it references:

- an ordinary document carrying a committed `DocumentId`; or
- a host panel carrying a `HostPanelId` and descriptor.

Output-panel text, Git snapshot partners, file browsers, Markdown previews,
UAST inspectors, picker state, diff partners, and any other recomputable or
ephemeral buffer are excluded. This is the layout counterpart of PRODUCT-0060's
positive document predicate.

Filtering may remove an active tab, an entire leaf, or a split child. Capture
normalizes that result deterministically before encoding:

1. retain durable tabs in their existing order;
2. keep the active tab when it survives, otherwise choose the first retained
   tab;
3. remove empty leaves;
4. remove empty split children and the corresponding ratio entries;
5. collapse a one-child split to its child without inventing a ratio;
6. preserve surviving child order and ratio weights exactly; and
7. keep focus on its surviving leaf, otherwise choose the first leaf in
   depth-first display order.

If nothing durable remains, the layout owner records an empty root. Restoration
then uses PRODUCT-0060's deterministic document fallback. It never manufactures
an empty pane merely to preserve a split shape, and it never attaches a durable
tab to a guessed neighbor.

Duplicate views of one ordinary document are allowed and meaningful. Each tab
occurrence names the same `DocumentId`; restoration creates a fresh `ViewId`
for every occurrence while sharing the one restored document buffer. A
`HostPanelId` likewise names one backing host object that may have multiple
fresh views. Duplicate occurrences do not cause duplicate `PanelRestored`
events.

The normalization algorithm is part of the schema contract, not an incidental
serializer behavior. A future durable tab class can widen the positive set in
its own record. It may not be smuggled in by serializing every current
`BufferContent` variant.

### 3. Restoration constructs one complete logical workspace before startup

`spawn_saved` decodes and constructs `libaryon_documents` first, then decodes
`libaryon_layout` against the resulting complete `DocumentId -> BufferId`
bijection. No engine, resolver bridge, viewport, or host event becomes visible
until both owners validate and the complete layout has been built.

Every restored leaf receives a fresh runtime `PaneId`. Every tab occurrence
receives a fresh `ViewId`. Document tabs bind through the restored
`DocumentId`; host-panel tabs bind through the retained `HostPanelId` and one
new host-panel backing buffer per unique panel identity. Split ratios and
directions then feed the existing layout computation at the current viewport
size.

An absent `libaryon_layout` is a valid PRODUCT-0060 predecessor and continues
to use the one-pane ascending-`DocumentId` fallback. An empty v1 root uses the
same fallback. A present malformed or unsupported owner refuses before startup;
it does not silently discard the layout and claim continuity. Generic
read-only `SavedSession` access continues to preserve unknown future schema
bytes exactly.

Restoration queues exactly one `PanelRestored { panel_id, descriptor }` event
per unique restored host panel, even when that panel has several views. The
event is ordered before the first viewport that exposes the panel. Polariton's
existing defensive projected-ID fallback may remain, but it does not discharge
this producer contract. No `PanelDisposed` event is emitted for state that
never existed in the new process.

Transient interaction state resets. Divider drags, pane drags, pointer capture,
picker state, resize-in-progress state, viewport scroll, cursor and selection,
hover, maximization, animation clocks, and pending asynchronous work are not
restored. Active tabs and focused logical placement are retained because they
are settled workspace state rather than an in-progress gesture.

Restored host-panel groups retain their logical arrangement and manual versus
automatic status, but no frontend content is fabricated. If producer meaning
has not yet been rebound by the owning frontend, the group remains an honest
unresolved placement. PRODUCT-0062 will reattach Polariton's stable
`DocumentId -> HostPanelGroupId` meaning; this record does not invent that
semantic relation. The first post-restore `EnsureHostPanelGroup` for an
unbound restored group may establish its runtime producer from a valid live
anchor. Reattachment preserves a manual group's existing placement and detached
members; it is not permission to auto-retile the group. A group already bound
to a different live producer refuses reattachment.

### 4. One registered application owner can join an ordered checkpoint

Libaryon adds a separately named saved-engine entrypoint that registers at most
one application-owned sibling for the engine lifetime, equivalent in shape to:

```text
EngineHandle::spawn_saved_with_application_owner(
    EngineConfig,
    SavedSession<Writable>,
    ApplicationOwnerId,
) -> Result<EngineHandle, SessionRestoreError>
```

`ApplicationOwnerId` wraps a validated `OwnerId` but refuses every name whose
string begins with `libaryon_`. The reservation is enforced at this application
mutation boundary, not in `OwnerId::new` or generic snapshot decoding. Existing
and future `libaryon_*` owners remain legal for libaryon to author, read, and
preserve. Unknown owner bytes remain forward-compatible.

Before moving the writable session into this entrypoint, the caller may inspect
its registered owner through the existing read-only `owner_payload` method and
decode its own schema. Invalid application bytes are the application's
pre-start refusal; libaryon never interprets them.

The registered engine exposes an ordered operation equivalent to:

```text
checkpoint_with_application_owner(
    schema_version,
    bytes,
    ApplicationCheckpointWitness,
) -> Result<(), SessionCheckpointError>
```

The owner identity is fixed by startup registration and cannot change per
request. V1 admits one application owner, one payload of at most 16 MiB, and one
witness. The request and reply travel through the same bounded engine FIFO as
commands and ordinary checkpoints. The payload is owned data; no callback into
the blocked frontend runs from the engine thread.

The 16 MiB application limit is checked before a request enters the FIFO. If a
registered owner already exists in the opened generation, registration also
refuses it before engine startup when its bytes exceed that limit. Ordinary
generic open still preserves a larger unknown owner; only the coupled mutation
capability adopts this bound.

Once an engine is registered as coupled, ordinary `checkpoint()` refuses with
an application-owner-required error. That includes the first checkpoint after
restart, before any new application payload has been published. Otherwise the
engine could advance documents and layout while silently preserving an older
application meaning owner. Unregistered saved engines retain PRODUCT-0060's
ordinary `checkpoint()` behavior byte-for-byte.

Registration does not select a session, discover a last session, checkpoint on
shutdown, or make the application owner mandatory for Baryon. It supplies a
capability only to a caller that already chose and exclusively opened a saved
session.

### 5. The witness is three complete identity censuses

`ApplicationCheckpointWitness` contains sorted, duplicate-free complete sets
of:

```text
DocumentId
HostPanelId
HostPanelGroupId
```

These are censuses of the application snapshot's whole shared identity
neighborhood, not merely the IDs referenced by one convenient map. At the FIFO
head, the engine captures documents and normalized logical layout, computes the
three restorable identity sets, and requires exact equality independently for
each set. A missing or extra identity in any one set returns a retryable stale
application-snapshot refusal. The checkpoint request has not mutated either new
owner at that point, and the previously committed generation remains
authoritative.

The same three sets are projected on every saved-engine `Viewport` as
`SharedIdentityCensus`. This is a purely observational retry instrument: it
contains exactly `documents`, `host_panels`, and `host_panel_groups`, each as a
set. It contains no descriptor, membership, pane, tab, order, ratio, revision,
or other placement/relationship fact. An ephemeral engine projects `None`
because it has no durable shared-identity neighborhood.

Projection makes a refusal actionable without weakening the witness. The
frontend copies one census into `ApplicationCheckpointWitness`, submits it, and
on staleness can copy the next complete projection. This matters especially for
host-panel groups: libaryon may collect an unreachable group internally, and no
group lifecycle event exists from which a frontend could otherwise repair its
set. The checkpoint path compares the projected census against its freshly
captured document/layout census before it compares the application witness, so
the projection and publication definitions cannot drift silently.

The host-panel census includes **every** restorable host panel in the
application snapshot, including fixed-role `RESULT_PANEL` and
`DASHBOARD_PANEL`. Those fixed IDs deliberately remain absent from
`view_panel_ids`; that map is a semantic Rune-view map, not a complete panel
census. PRODUCT-0062 must build the witness from the complete panel registry or
equivalent authoritative inventory and use `view_panel_ids` only when encoding
Rune-view meaning.

The application owns the relationship between its opaque bytes and the witness
it supplies. PRODUCT-0062 must prove that every `DocumentId`, `HostPanelId`, and
`HostPanelGroupId` encoded in the Polariton owner belongs to its corresponding
witness set. Libaryon proves the other half: the supplied sets exactly equal the
state captured for this generation. Neither crate parses the other's schema.

Set equality is a complete cross-owner condition only under the two Context
invariants:

- no application-owned persisted fact depends on pane position, tab order,
  ratio, descriptor, group membership, or another libaryon layout relation;
  and
- a semantic panel or group identity is never rebound or recycled.

The second invariant becomes a behavioral gate now. Re-running a producer with
one semantic view removed and a different view added must retain the exact
`HostPanelId` of every surviving view, retain the removed view's mapping as
stale, allocate the new view above the previous dynamic high-water mark, and
keep fixed-role panel IDs outside the semantic map. The corresponding producer
group identity likewise remains stable. A memory-hygiene cleanup may not delete
or recycle these mappings while this witness design is in force.

If a future application owner stores placement-dependent state, or if an
existing panel/group identity can be rebound to a different semantic meaning,
work stops for a stronger witness such as a relationship digest or shared
revision. Adding more set comparisons does not prove a relation.

### 6. Capture, validation, staging, and publication have one order

The coupled checkpoint transaction is:

```text
application builds opaque bytes + witness
    -> one synchronous FIFO request
    -> engine captures document candidates and normalized logical layout
    -> engine computes and compares all three identity censuses
    -> engine validates both libaryon codecs and all size limits
    -> engine stages base identities, documents, layout, and application owner
    -> one SavedSession::commit publishes the generation
```

No call to `set_owner_payload` for the new layout or application owner occurs
before capture and witness validation succeed. Ordinary document lifecycle may
already have updated the writable session's staged base, as PRODUCT-0060
requires; refusal still leaves the application owner bytes unchanged and the
previously committed generation authoritative. This is load-bearing: a refused
request must not leave stale application bytes staged for a later coupled
checkpoint.

The existing PRODUCT-0060 publication boundary remains. Failure before
`CURRENT` preserves the complete previous generation and permits a fresh
coupled retry. Failure after `CURRENT` leaves publication uncertain, retains
the lock, and poisons later checkpoints through that engine. Documents, layout,
and application owner are old together or new together; no fault point may
publish a mixed subset.

The stale-witness refusal is retryable because checkpoints remain explicit and
rare. The caller rebuilds its payload from the next complete projection/event
state and submits another request; libaryon adds no internal retry loop. If a
later surface adds debounced or continuous automatic checkpointing, work stops
to decide bounded retry, coalescing, and starvation behavior. Continuous
mutation can otherwise keep a well-formed snapshot stale forever.

### 7. The Polariton and dashboard consumers remain later

This record adds no Polariton production owner, changes no Rune spelling, and
persists no categorical allocation itself. It only guards the existing
append-only semantic panel identity behavior because that behavior is now a
premise of the handoff.

PRODUCT-0062 will use the registered application owner to persist:

```text
(DocumentId, LocalViewIdentity) -> HostPanelId
DocumentId -> HostPanelGroupId
(DocumentId, LocalViewIdentity, palette length) -> ColorIdentity
```

Its codec will retain authored `LocalViewIdentity`, keep anonymous
`output_order` semantics explicit, and decide the exact continuing-session
reattachment behavior. DataFrames, completed results, kernels, SVG/raster
output, SQL connection state, and Toron `:memory:` contents remain recomputed or
unavailable.

The legacy `rune_plot_panes: BufferId -> gui::pane_grid::PaneId` and nested
dashboard tree do not enter this record or PRODUCT-0062 merely because they are
Polariton-owned. Their content authority is unresolved. The dashboard-fate
record remains their trigger.

## Implementation sequence

1. Bank the current logical-layout behavior with an asymmetric workspace
   containing n-ary splits, unequal ratios, duplicate document views, inactive
   tabs, host panels, a manual group, a detached member, and transient tabs.
   Capture it at two substantially different viewport sizes before adding any
   codec.
2. Add dependency-free checkpoint DTOs and a strict bounded
   `libaryon_layout` codec. Prove validation and canonical filtering directly;
   do not expose `LayoutNode` or make the projection authoritative.
3. Restore the DTO into fresh buffers/views/panes after document restoration
   and before thread startup. Add ordered unique `PanelRestored` emission and
   keep the PRODUCT-0060 predecessor fallback intact.
4. Add `ApplicationOwnerId`, startup registration, the coupled checkpoint
   request, three-set witness comparison, reserved-prefix refusal, and the
   ordinary-checkpoint guard. Keep the application body opaque.
5. Unify document, layout, and optional application-owner staging behind one
   capture/validate/publish function. Extend every pre/post-`CURRENT` fault
   oracle across all owners.
6. Add the Polariton append-only identity premise test without changing
   Polariton production behavior. Run downstream Baryon/Polariton suites and
   enumerate only structurally necessary projection fixture updates.
7. Record evidence, accept this ADR, and update the persistence queue to
   PRODUCT-0062. Do not add session discovery, automatic checkpointing, or a
   dashboard payload while demonstrating the restored layout.

The first implementation commit must be the byte-identical logical-layout
oracle. Codec and restoration work do not begin until the same runtime tree has
been captured at two viewport sizes and shown to differ only in derived
geometry, never in logical structure.

## Acceptance gates

1. **Predecessor fallback is unchanged.** A PRODUCT-0060 generation with no
   `libaryon_layout` owner restores through the existing one-pane,
   ascending-`DocumentId` fallback byte-for-byte. Empty v1 layout uses the same
   fallback. No existing ephemeral or saved-document baseline changes merely
   because this owner exists in production.

2. **The codec is strict and bounded.** Direct tests round-trip an asymmetric
   maximum-shape fixture and refuse unknown fields, future schema, duplicate or
   zero identities, inconsistent panel/group references, invalid active/focus
   paths, empty/singleton splits, ratio mismatch/zero, missing documents,
   trailing bytes, count overflow, and payloads over an injected small budget.
   A constant assertion pins the production layout-owner limit to 16 MiB.

3. **Logical bytes are head-neutral.** The same runtime tree captured at two
   viewport sizes at least 3x apart produces byte-identical `libaryon_layout`
   bytes. Rendered `PaneRect`s differ as a control. Architecture tests assert
   the DTO vocabulary contains no `BufferId`, `PaneId`, `ViewId`, `PaneRect`,
   screen coordinate, or viewport extent.

4. **A complex logical workspace round-trips by meaning.** An end-to-end saved
   engine fixture contains horizontal and vertical n-ary splits, unequal
   ratios, two views of one `DocumentId`, inactive ordered tabs, two host
   panels, one manual group, and one detached member. Reopen preserves split
   directions, ratios, tab order, active tabs, focus, group roots, membership,
   and detached/manual state exactly.

5. **Filtering cannot create phantom layout.** A fixture interleaves ordinary
   documents and host panels with output text, Git snapshot, browser, Markdown
   preview, UAST inspector, and diff-only tabs across several leaves. Capture
   removes only the transient tabs, preserves surviving order/ratios, collapses
   singleton splits, removes empty leaves, and deterministically rehomes active
   tab and focus. An all-transient control yields the documented fallback.

6. **Runtime identity is fresh; durable correspondence is exact.** Across
   reopen, every `PaneId` and `ViewId` is newly allocated, duplicate document
   views share the one `DocumentId`/buffer, and each `HostPanelId` retains its
   exact value while receiving one backing buffer. No serialized byte contains
   the predecessor runtime IDs.

7. **Restored panel events are ordered and non-vacuous.** A panel present in
   two views emits exactly one `PanelRestored` with its exact descriptor before
   the first viewport exposing either view. Both views render an unresolved
   placeholder at revision zero; a control proves the host panel is actually
   present. No `PanelDisposed` is emitted during startup.

8. **Group arrangement survives without fabricated producer meaning.** A
   manual group with retained and detached members reopens with one group root,
   the same logical placement and shapes, and no runtime producer/anchor ID in
   its payload. A later explicit reattachment using the same group ID can bind
   its durable producer without reallocating or retile-resetting the manual
   arrangement; no descriptor-derived identity is consulted.

9. **The application namespace is closed at mutation, not parsing.** Creating
   an `ApplicationOwnerId` for `libaryon_documents`, `libaryon_layout`, and a
   synthetic future `libaryon_*` name refuses. A normal `polariton_*` owner
   registers. Generic read-only open still loads and byte-preserves an unknown
   `libaryon_future` owner, proving the reservation did not weaken forward
   compatibility.

10. **Each witness set fails independently and the projected shape stays
    closed.** Starting from one valid coupled
    snapshot, tests supply a missing and an extra document, panel, and group ID
    one class at a time. All six requests refuse as stale before staging; the
    committed generation and staged application bytes remain unchanged. A
    rebuilt exact witness then succeeds. A compile-time architecture test
    exhaustively destructures `SharedIdentityCensus` into exactly three
    `BTreeSet`s, so adding a placement or relationship field breaks the gate.

11. **FIFO races become refusals, not mixed generations.** Deterministic
    barriers queue document close/open, panel close/open, and group removal/add
    ahead of coupled checkpoints whose payloads were built from the preceding
    projection. Each stale request refuses. Rebuilt requests publish, and
    reopen proves all three owners refer to one identity census. A production
    fixture closes a grouped panel and its producer, observes libaryon's silent
    unreachable-group collection make the old census stale, then copies the
    next projected census and proves the retry succeeds.

12. **Rune semantic panel identity is append-only.** A Polariton behavioral
    test prepares one producer with named and anonymous views, reruns with one
    view removed and a new one added, and asserts: surviving views retain exact
    panel IDs; the removed mapping remains; the new panel is strictly above the
    prior dynamic high-water mark; the producer group ID is unchanged; and
    fixed IDs 1 and 2 never occur in the semantic map. The passing behavior,
    not source shape, discharges the invariant. The `view_panel_ids` field has a
    production-site comment naming PRODUCT-0061's cross-owner witness as the
    reason deletion, recycling, or rebinding is forbidden, and the test's
    assertion messages repeat that dependency so a future hygiene change finds
    the architectural consequence where it is made.

13. **Coupled capability cannot silently fall back.** A registered engine's
    ordinary `checkpoint()` refuses before mutation both before and after its
    first coupled publication. `checkpoint_with_application_owner` succeeds.
    An unregistered saved engine's ordinary checkpoint continues to succeed and
    preserve unknown owners exactly.

14. **Publication remains one fault boundary.** Fault injection before
    `CURRENT` leaves the old base, documents, layout, and application owner all
    authoritative and permits a fresh coupled retry. Failure after `CURRENT`
    exposes the complete new four-part generation on reopen and poisons later
    checkpoints through the old engine. No fault produces a mixed generation.

15. **No application meaning is inferred.** Libaryon tests restore two panels
    with identical `{kind, title}` and prove their distinct `HostPanelId`s and
    placements survive. The layout codec never imports `LocalViewIdentity`,
    `ColorIdentity`, a Polariton module, or application payload fields.

16. **Transient and derived state stay absent.** Reopen resets cursor,
    selection, scroll, divider/pane drag, hover, picker, animation, resize
    suspension, pending work, and panel content. No DataFrame, kernel, rendered
    image, SQL connection, Toron database, dashboard pane, or dashboard
    `PlotPaneState` enters either libaryon owner.

17. **The default and downstream surfaces remain still.** Ordinary ephemeral
    startup, Baryon rendering, Polariton rendering, Rune execution, and all
    existing image/SVG/draw baselines pass without refresh. Any fixture change
    is enumerated as a type/projection premise update, never absorbed as
    unexplained baseline churn. No test is ignored.

18. **Evidence is complete.** Full libaryon unit, integration, architecture,
    trybuild, and rustdoc suites pass; the full workspace all-target suite,
    formatting, diff check, and warning-clean targeted clippy pass. Evidence
    records predecessor fallback, both viewport sizes, every normalized
    difference class, the six stale-witness refusals, both publication fault
    sides, and the exact runtime adapter/head used for any viewport assertion.

## Stop condition

> If exact logical restoration requires serializing or reconstructing
> `BufferId`, `PaneId`, `ViewId`, `PaneRect`, or head-specific widget identity,
> work stops. Stable meaning and head portability outrank pixel-exact placement.

> If filtering a transient tab makes the surviving topology ambiguous rather
> than deterministically normalizable by §2, work stops for a durable tab-kind
> or explicit placeholder decision. The implementation may not guess a pane or
> silently drop a durable object.

> If a host-panel group cannot restore its manual/detached arrangement without
> persisting runtime producer or anchor IDs, work stops for a stable logical
> group-reference design. A title, kind, or tree position is not an identity.

> If libaryon must parse application bytes to decide whether a coupled
> generation is consistent, work stops. The owner partition is more important
> than landing the first consumer. The remedy is a stronger explicit witness,
> not schema leakage.

> If Polariton gains placement-dependent durable state, or any semantic panel
> or group ID can be rebound/recycled, the three-set witness is no longer
> licensed. Work stops for a relationship digest or shared revision before
> that behavior ships.

> If the projected census needs a fourth field, or any field other than an
> identity set, work stops for a stronger witness decision. A descriptor or
> ordering added merely for frontend convenience would let an opaque owner
> depend on a relationship the three-set comparison is not licensed to prove.

> If one coupled application owner is insufficient for an actual consumer,
> work stops for a multi-owner transaction design. V1 may not accept an
> unbounded vector of opaque payloads as an implementation convenience.

> If the 16 MiB logical-layout or application-owner bound rejects an ordinary
> observed workspace, work stops for a measured streaming/chunking record.
> Raising or removing the bound without an exhibit is not allowed.

> If automatic or debounced checkpointing is introduced, work stops to decide
> bounded retry, coalescing, and starvation under continuous mutation. This
> record's explicit retryable refusal is not silently promoted into an
> unbounded autosave loop.

> If a restored secret-bearing panel descriptor exposes content rather than
> presentation metadata, work stops for a locked application-owner design.
> Host placeholders may not become an alternate plaintext persistence path.

## Supersession

This record supersedes PRODUCT-0060's assignment of ordinary pane/tab topology
to the Polariton extension. The earlier record's ownership premise was based on
the visible frontend projection; Stage 0 found the private head-neutral
`LayoutNode` authority in libaryon. PRODUCT-0060's document codec, restoration
matrix, explicit checkpoint rule, security refusal, and predecessor fallback
remain accepted and unchanged.

It refines PRODUCT-0059 §7's fixed order to:

```text
session/document identity + locked atomic store       PRODUCT-0059
    -> libaryon document content and fallback         PRODUCT-0060
    -> libaryon logical workspace + owner handoff     this record
    -> Polariton Rune-view/color continuation owner  PRODUCT-0062
    -> durable named-panel surface                    later
```

The owner partition itself is strengthened, not weakened. Libaryon persists
where documents and opaque host panels sit. Polariton will persist what its
panel and group IDs mean. Neither owner reconstructs or interprets the other.

The record widens `HostPanelId` lifetime only for an explicitly saved session:
the frontend-authored opaque token may survive process restart as a join across
sibling owners. It does not become globally unique, path-derived, meaningful
to another frontend, or allocated by libaryon. Existing ephemeral panel
lifecycle is unchanged.

It preserves PRODUCT-0059's unknown-owner forwarding. The `libaryon_`
reservation applies only when an application attempts to author a coupled
owner through the new engine boundary. Generic parsing and byte-preserving
copy-forward continue to admit unknown names in that prefix.

The dashboard exclusion preserves the deferred ledger's dashboard-fate entry.
This record does not reclassify an `Arc<dyn PlotKernel>` as authored state merely
because its containing tree has ratios.

## Rejected

- **Persist the `Viewport` rectangles:** serializes one rendered answer, loses
  tab/group structure, and changes meaning across heads and window sizes.
- **Reconstruct a tree from pane/divider geometry:** makes an observer infer the
  owner's authority and is ambiguous under equal edges, collapsed extents, and
  rounding.
- **Put editor topology in `polariton_layout`:** prevents Baryon from restoring
  the same engine-owned workspace and crosses the 0059 ownership partition.
- **Serialize `PaneId`, `ViewId`, or `BufferId`:** restores counter values that
  identify nothing in the new process.
- **Derive host-panel identity from `{kind, title}`:** confuses mutable,
  non-unique presentation with identity.
- **Mint a second durable panel ID:** duplicates Polariton's existing opaque ID
  and creates a three-way rebind where one join already suffices.
- **Persist every tab and buffer:** captures output, snapshots, previews,
  browsers, inspectors, and other derived state as authored truth.
- **Keep empty leaves to preserve shape:** creates panes with no durable meaning
  and forces a guessed attachment later.
- **Silently fall back on malformed layout:** turns corruption or unsupported
  schema into plausible but false continuity. Only an absent predecessor owner
  earns fallback.
- **Persist panel content or revision:** makes frontend execution products and
  process-local freshness counters durable. Restored panels are unresolved.
- **Persist the nested dashboard tree now:** its leaves have no uniform durable
  producer and contain derived kernels; topology alone is not a usable state.
- **Return the writable session to Polariton:** creates a second writer and
  bypasses ordered engine capture.
- **Call back into the frontend at the FIFO head:** risks deadlock because the
  caller is synchronously waiting for the checkpoint reply and complicates the
  one-request transaction.
- **Parse Polariton bytes in libaryon:** destroys owner opacity and makes schema
  migration a cross-crate lockstep.
- **Check only `DocumentId`s:** misses the panel and group joins PRODUCT-0062
  depends on.
- **Use only set equality after allowing ID recycling:** two meanings can swap
  while all censuses remain identical. Append-only identity is load-bearing.
- **Reserve only today's two libaryon owner names:** requires changing the API
  every time libaryon adds an internal owner and permits future spoofing.
- **Reject `libaryon_*` during generic parsing:** breaks forward compatibility
  with the very internal owners the namespace reserves.
- **Let a registered engine use plain checkpoint:** silently advances engine
  truth while preserving an older application meaning owner.
- **Retry stale snapshots inside libaryon:** the engine cannot rebuild opaque
  frontend bytes and an unbounded loop can starve under continuous mutation.
- **Add session discovery or autosave for the demo:** combines substrate
  consistency with consent, contention UX, and retry policy before the surface
  record decides them.

## Consequences

- A saved libaryon workspace can reopen with the same logical split tree, tab
  membership/order, ratios, active tabs, focus, opaque host-panel identities,
  and group arrangement at a different viewport size.
- Runtime pane, view, and buffer numbers remain process-local. Durable identity
  is visible through correspondence, not counter preservation.
- Host panels return as explicit unresolved placeholders. Their location is
  libaryon truth; their content and semantic meaning wait for their owning
  frontend.
- Baryon receives layout restoration automatically because the substrate is
  head-neutral. Polariton does not become the authority for editor panes merely
  because it renders them.
- One registered application owner can join the same atomic generation without
  receiving the writable store or leaking its schema into libaryon.
- The coupled witness has a precise license: it proves identity-census
  agreement under placement ownership and append-only semantic mappings. It is
  not a generic relational consistency proof.
- Explicit checkpoints may occasionally refuse when the frontend snapshot
  loses a race to queued lifecycle work. That refusal is honest, bounded by the
  user action today, and retryable from the projected shared-identity census.
- The projected census couples no new lifetime or ownership. It observes the
  three identities at one moment; it does not expose where they sit, how they
  are ordered, or what they mean.
- The `libaryon_` namespace can grow without exposing each future owner name at
  the application boundary, while generic readers retain byte-forward
  compatibility.
- PRODUCT-0062 now has the complete substrate it needs: stable documents,
  restored opaque panels/groups, and one atomic channel for meanings and color
  continuation.
- The dashboard remains unresolved by design. Persisting editor layout does not
  answer whether a second nested plotting workspace should survive.
- No default startup, shutdown, Rune, plot, table, SVG, or pixel behavior
  changes. Visible restoration still requires an explicitly opened saved
  session; surface policy remains later.
