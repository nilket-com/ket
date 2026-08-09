# ADR 0062: Polariton Rune-view and colour continuation meanings

Status: accepted 2026-08-09. Implemented and verified in
[`../impl/0062_polariton_rune_view_and_color_continuation_evidence.md`](../impl/0062_polariton_rune_view_and_color_continuation_evidence.md).
This record makes Polariton the second owner in a
saved session. It persists what its opaque panel and group identities *mean* —
which durable document and semantic view produced them — together with the
categorical colour allocation that PRODUCT-0007 §8 promises for the life of a
session. It publishes through PRODUCT-0061's registered application owner, in
the same generation as libaryon's documents and layout. It persists no
layout, no rendered artifact, and no dashboard state.

**Prerequisite satisfied.** This record was drafted blocked: the coupled
witness compares three identity sets, but Polariton could observe only two —
host-panel groups were projected nowhere and are collected internally, so a
stale refusal would have been unactionable and the retry could never converge.
PRODUCT-0061 was amended to project `SharedIdentityCensus`, exactly three
identity sets with no placement, ordering, or descriptor
(`uast/projection.rs:394`), exposed as `Option` so an ephemeral engine reports
`None`. §6 consumes it.

## Context

PRODUCT-0059 established the locked owner-partitioned store. PRODUCT-0060 made
documents durable under `DocumentId`. PRODUCT-0061 made libaryon's head-neutral
logical workspace durable and added the one coupled channel through which a
frontend can publish a sibling owner atomically: registration at
`spawn_saved_with_application_owner`, then
`checkpoint_with_application_owner(schema_version, bytes, witness)` validated
against three identity censuses at the engine FIFO head.

Everything restored so far is placement without meaning. A reopened session
returns documents to their panes and returns host panels to their tabs, but
each restored panel is an unresolved placeholder at revision zero. Nothing
records that panel `1<<63` was the figure produced by the view named `"bars"`
in `analysis.rn`, or that within that figure Atlas holds palette position 0.

Polariton already owns all three facts at runtime, in two different structures:

```text
CodeEditor  (editor.rs:376)  view_panel_ids:
                (BufferId, LocalViewIdentity) -> HostPanelId
CodeEditor  (editor.rs:388)  view_group_ids:
                BufferId -> HostPanelGroupId
AppState    (state.rs:92)    rune_categorical_identities:
                (BufferId, LocalViewIdentity, palette length) -> ColorIdentity
```

Two of the three live on `CodeEditor` and one on `AppState`, so the payload is
assembled from two Polariton owners rather than read from one place. All three
are keyed on `BufferId`, a per-process monotonic counter, and all three must be
re-keyed onto `DocumentId` exactly as PRODUCT-0060 anticipated.

`LocalViewIdentity` needs no replacement. Its `Named(String)` case is authored
content and its `Anonymous(usize)` case is a deterministic presentation
ordinal; both reproduce when identical source completes identically. The
anonymous case remains positionally unstable by design, as PRODUCT-0060 §7
recorded: inserting a view above another shifts later ordinals and therefore
changes their identity. Persistence makes that visible across restart without
creating it.

The colour payload is not a scalar. `ColorIdentity` (`plot/spec/mod.rs:1062`)
is itself two maps:

```text
ColorIdentity {
    assignments: ColorIdentityKey -> PalettePosition,
    spaces:      ColorLineageKey  -> ColorAllocationSpace,
}

ColorIdentityKey  { field, layer: LayerIdentity, category }
ColorLineageKey   { field, layer: LayerIdentity }
ColorAllocationSpace = Legacy { len } | CategoricalPalette { len }
PalettePosition(usize)   // validated against palette length at construction
```

`LayerIdentity` (`plot/spec/mod.rs:598`) carries `{ ordinal, plot_type }`, and
`PlotType` is the renderer's vocabulary — a large enum the plotting arc extends
freely, including `HorizontalBar`, `StackedBar`, `FillBetween`, and `Heatmap`.
Serializing that enum's Rust spelling would make durable colour data depend on
a rendering vocabulary with no schema discipline. Adding a variant would not by
itself break an existing payload, since no stored document names it; renaming,
removing, or re-spelling one would, and nothing in the plotting arc treats that
enum as a schema subject to such a decision.

PRODUCT-0055 already bounds the exposure. Layer identity for a Rune-authored
plot keys on the *authored family*, and only three authored mark words exist —
`scatter`, `bar`, `line` — enforced by the `unreachable!` at the authored
channel projection. Because this owner persists Rune-view continuation only, no
other plot type can reach it. The durable spelling can therefore be a small
closed vocabulary rather than the renderer's enum.

Two mechanical facts shape the rest. First, Polariton has no serialization
dependency at all; `configuration.rs` is a Rune startup script, not a data
format. Second, the semantic identity allocators are process-local:
`next_view_panel_id` starts at `DYNAMIC_VIEW_PANEL_BASE = 1 << 63` and
`next_view_group_id` starts at 1 (`editor.rs:65`, `:584-588`). If those
allocators reset while restored mappings retain their old identities, the next
minted panel collides with a restored one. That would violate the append-only
invariant PRODUCT-0061 §5 made load-bearing for the three-set witness — not
merely produce a duplicate. The high-water marks are durable state, not a
runtime detail.

Finally, PRODUCT-0061 §5 instructs this record on witness construction: build
the censuses from a complete authoritative inventory rather than from
`view_panel_ids`, which supplies encoded meaning only. Polariton's own
`PanelRegistry` (`editor.rs:372`) is complete for panels but there is no
equivalent for groups, which is why §6 requires a projected census instead.
Fixed-role `RESULT_PANEL` and
`DASHBOARD_PANEL` belong to the census and deliberately not to the semantic
map.

## Decision

### 1. One strict, small application owner

Polariton registers application owner `polariton_session`, schema version 1,
through PRODUCT-0061's `ApplicationOwnerId`. The name is outside the reserved
`libaryon_` prefix and is fixed at engine registration for the process
lifetime.

The payload is a single strict UTF-8 JSON document. Unlike PRODUCT-0060's
document owner it carries no arbitrary byte extents — every value is structured
metadata — so it needs no binary envelope. Polariton gains `serde` and
`serde_json` as ordinary dependencies for this purpose only; the crate acquires
a data format, not a general serialization posture, and no existing type gains
a derive merely because the dependency now exists.

Validation happens at three distinct moments, and conflating them is a defect:

```text
pre-start decode   structural validity only: schema version, unknown fields,
                   duplicate keys, zero identities, palette positions outside
                   their recorded length, allocator marks below their floor or
                   not above every encoded identity, trailing content
capture            every encoded shared identity belongs to the witness this
                   request supplies
post-spawn         decoded meanings join the first complete projected census
```

Only the first is available before startup. PRODUCT-0061 commits the
application schema version and bytes but not the witness
(`engine/session_checkpoint.rs:525`), and the restored panel and group
censuses cannot be recovered pre-start without interpreting `libaryon_layout`,
which the ownership boundary forbids. A decoder that claimed to check
identities against a witness would therefore be checking against nothing.

The decoder validates every count before allocating.

The owner is bounded at 1 MiB, well inside PRODUCT-0061's 16 MiB application
ceiling. The payload is small by construction: a few maps of integers and
authored field names. Exceeding 1 MiB refuses the checkpoint and is the trigger
for a measured bound revision, not a silent raise.

### 2. Three meanings, re-keyed onto durable identity

The payload persists exactly three relations:

```text
panels:  (DocumentId, LocalViewIdentity) -> HostPanelId
groups:  DocumentId                      -> HostPanelGroupId
colours: (DocumentId, LocalViewIdentity, palette length) -> ColorIdentity
```

`LocalViewIdentity` serializes as its authored form — the exact name string for
`Named`, the exact ordinal for `Anonymous` — with the two cases tagged
distinctly so a view named `"3"` can never collide with anonymous ordinal 3.

`HostPanelId` and `HostPanelGroupId` serialize as their exact opaque values.
They are Polariton's own tokens; PRODUCT-0061 established that they may cross a
restart as the join between sibling owners without becoming libaryon-minted or
globally unique.

No `BufferId`, `PaneId`, `ViewId`, pane position, tab index, split ratio, or
descriptor enters this payload. Placement is libaryon's owner. This is the
first invariant on which PRODUCT-0061's three-set witness is licensed, and it
is a schema rule here, not a convention.

### 3. Semantic allocators persist their high-water marks

The payload carries `next_view_panel_id` and `next_view_group_id` as durable
fields. Restoration resumes both allocators at the stored values.

This is not bookkeeping convenience. PRODUCT-0061 §5's witness is sufficient
only because a semantic identity is never rebound or recycled. A reset
allocator would mint `1 << 63` again for a *different* `(producer, view)` while
a restored mapping still claims it — two meanings sharing one identity, with
all three censuses potentially unchanged. Append-only must hold across the
restart boundary, not merely within a process.

Restoration validates three properties of each mark, not one:

- **floor** — the panel mark is at or above `DYNAMIC_VIEW_PANEL_BASE`, so a
  restored allocator can never mint into the fixed-role range that holds
  `RESULT_PANEL` and `DASHBOARD_PANEL`; the group mark is at or above 1;
- **dominance** — the mark is strictly greater than every identity of its kind
  the payload encodes;
- **headroom** — the mark has not exhausted its namespace.

Exhaustion refuses at load rather than panicking at the next mint, which is
today's runtime behaviour (`editor.rs`, `checked_add(...).expect(...)`). A
payload failing any of the three does not open the session.

### 4. Shared-identity relations are census-filtered; colour lifetime is not

Capture filters the two *shared-identity* relations against the live census
before encoding. A panel mapping whose `HostPanelId` is no longer a restorable
host panel — because the panel was closed, not merely marked stale — is omitted
from the payload, and a group mapping whose `HostPanelGroupId` libaryon has
collected is omitted with it.

PRODUCT-0061 §5 requires that every shared identity encoded here belongs to its
witness set, and the witness must exactly equal libaryon's captured census. A
retained mapping for a closed panel would make the two irreconcilable: either
the witness includes an identity libaryon no longer has, or the payload
references one the witness omits. Both refuse.

The colour relation is not subject to that filter, because it names no shared
identity. Its key is `(DocumentId, LocalViewIdentity, palette length)` and its
value is a `ColorIdentity`; no `HostPanelId` or `HostPanelGroupId` appears in
either. The runtime already keeps it independently of panel state
(`gui/state.rs:92`). Closing a panel therefore drops its panel mapping and
**retains its colour allocation** for as long as the producer document remains
durable.

The two lifetimes are deliberately separate:

```text
panel/group meaning   lives as long as the shared identity is in the census
colour allocation     lives as long as its producer DocumentId is durable
```

So a Rune view whose panel is closed and later reappears mints a fresh
`HostPanelId` above the high-water mark, and recovers its exact palette
positions. Tying the two together would forfeit the PRODUCT-0007 §8
continuation this record exists to preserve, in exchange for nothing the
witness contract requires.

A durable tombstone class remains unwritten, and its trigger is now narrower:
it is needed only to recover a *closed panel's identity or placement*, never to
recover colour lineage.

### 5. Colour continuation is the stated exception, over a closed family
vocabulary

PRODUCT-0059 identified categorical colour allocation as the single
"persist intent only" exception: if reopening a saved session is *continuing*
it, PRODUCT-0007 §8's promise that identities are never evicted, reassigned, or
silently cycled must survive the restart it never contemplated. This record
implements that.

`ColorIdentity` serializes both of its maps. `PalettePosition` serializes as
its integer and is revalidated against the recorded allocation-space length on
load, exactly as `PalettePosition::new` validates at runtime
(`plot/spec/mod.rs:694`). `ColorAllocationSpace` serializes its variant and
length.

`LayerIdentity` serializes as `{ ordinal, family }`, where `family` is a closed
durable vocabulary of the three authored mark words:

```text
"scatter" | "bar" | "line"
```

The renderer's `PlotType` never enters the payload. Restoration maps the family
back to the authored plot type that PRODUCT-0055 pins for lineage, which is
independent of resolved orientation — a rotated bar keeps `"bar"`, because
0055's whole point is that orientation may change geometry but not identity.
An unrecognised family refuses the payload rather than resolving to a default
that would silently recolour a figure.

Continuation is scoped to continuing a session. A newly created `SessionId`
begins with no allocation state, and an ephemeral engine is unchanged. The
palette-length component of the colour key already discriminates a palette
change, so switching palettes begins a fresh allocation space rather than
reinterpreting stored positions.

### 6. Publication follows PRODUCT-0061's coupled transaction exactly

Polariton builds its payload and witness, then issues one
`checkpoint_with_application_owner`. It does not receive the writable session,
does not call `set_owner_payload`, and does not publish independently.

The witness is built from `Viewport::shared_identity_census`, not reconstructed
from Polariton's own structures.

That census exists because Polariton can observe documents and panels but not
groups. `HostPanelGroupId` is a type in `engine/host_event.rs` and was a field
of no viewport and no host event, while `collect_unreachable_host_panel_groups`
(`engine/session.rs:5747`) removes groups whose producer is gone and which hold
no placement. Polariton's append-only `view_group_ids` is therefore neither
complete nor live, and without a projected census a stale refusal would leave
no authoritative input from which to rebuild the exact set that
`engine/session_checkpoint.rs:480` demands — the retry could never converge.

PRODUCT-0061's amendment projects the same three sets the witness compares:
documents, host panels, host-panel groups. It exposes no placement, ordering,
ratio, or descriptor, so it does not weaken the partition that licenses the
witness. It exists so a refusal is actionable. Polariton consumes it verbatim
and converts it to a witness by set identity; it may not filter, extend, or
reorder it, and an engine reporting `None` has no saved session and therefore
no coupled owner to publish.

Given that census, a stale refusal is retryable: Polariton rebuilds from the
next projected census and submits again, adding no retry loop, matching
PRODUCT-0061 §6. `view_panel_ids` supplies encoded meaning only, never the
census — it is a semantic map and deliberately omits fixed-role panels.

Because the engine is registered as coupled, ordinary `checkpoint()` already
refuses. Documents, layout, and meanings are old together or new together.

### 7. Restoration rebinds meanings after libaryon publishes its bindings

On a saved start, Polariton decodes its owner through the read-only session
before moving it into the engine, as PRODUCT-0061 §4 permits, and holds the
decoded meanings. Invalid bytes are Polariton's pre-start refusal; libaryon
never interprets them.

After `spawn_saved_with_application_owner` returns and libaryon has published
its restored `DocumentId -> BufferId` bindings and one `PanelRestored` per
restored panel, Polariton rebuilds its runtime maps by joining the decoded
meanings to those bindings. Both allocators resume above their stored marks.

For each restored group, Polariton performs the one-shot reattachment
PRODUCT-0061 §3 provides, supplying the live anchor derived from the restored
producer document. Reattachment preserves a manual group's arrangement and
detached members and does not retile.

Restored panels stay unresolved until their producer runs. This record does not
re-execute Rune, rebuild a relation, or fabricate a figure. Re-running the
producer buffer resolves the panel in place — with its original colours,
because the allocation was restored before the run — rather than opening a
second panel beside it.

### 8. Everything derived remains derived

Absent from this payload and recomputed or unavailable: dashboard pane trees
and `PlotPaneState`, `Arc<dyn PlotKernel>`, `DataFrame`s, the result registry,
completed relations, resolved specifications, SVG and raster output, execution
snapshots, notebook outputs, preview blocks, scroll and column widths, and
Toron `:memory:` contents, which PRODUCT-0059 already requires to restore as an
unavailable dependency rather than a replayed database.

`rune_plot_panes: BufferId -> gui::pane_grid::PaneId` stays out. It addresses
the nested dashboard, whose leaves hold derived kernels and whose content
authority the dashboard-fate cluster owns.

## Implementation sequence

1. Add `serde`/`serde_json` to Polariton and the dependency-free DTOs for the
   three relations, both high-water marks, and the durable `LayerIdentity`
   spelling. Prove round-trip, strictness, bound, and every refusal directly,
   with no engine or session involved.
2. Add re-keying helpers that project the two `CodeEditor` maps and the one
   `AppState` map onto `DocumentId`, and the inverse. Prove them against
   synthetic bindings before any disk state exists.
3. Add capture: shared-identity filtering per §4, witness construction from the
   projected census added by the PRODUCT-0061 amendment, and the coupled
   checkpoint call. Prove that an encoded shared identity absent from the
   witness refuses locally, before the request is issued, and that a refusal
   followed by a census rebuild converges.
4. Add restoration: pre-start decode, post-binding rebuild, allocator resume,
   and one-shot group reattachment. Prove allocator monotonicity across the
   restart boundary.
5. Add the end-to-end oracle: a saved session with two named and two anonymous
   views across two documents, checkpointed, reopened, and re-run, with colours
   and panel attachment preserved.
6. Update the persistence ledger to PRODUCT-0063 and record which continuation
   gaps this record deliberately leaves open.

Steps 1 and 2 must be independently green and change no observable behaviour.
A re-keying error must not be discoverable only as a wrong colour three steps
later.

## Acceptance gates

1. **The codec is strict and bounded.** Round-trip of a maximal fixture —
   named and anonymous views, both allocation-space variants, all three
   families, multiple palette lengths — with refusals for unknown fields,
   future schema, duplicate keys, zero identities, out-of-range palette
   positions, unrecognised families, allocator marks below floor, not
   dominating, or exhausted, and trailing content. A constant assertion pins
   the 1 MiB bound; an injected small bound proves refusal without publication.
   Witness membership is **not** among the decoder's checks and is proven at
   capture by gate 7 instead.

2. **No placement fact is representable.** The DTO vocabulary contains no
   `BufferId`, `PaneId`, `ViewId`, pane position, tab index, ratio, descriptor,
   or `PlotType`. An architecture test asserts this over the serialized form,
   not only over the Rust types.

3. **Re-keying is exact and order-independent.** Three documents opened in
   reverse order across a restart reproduce the same
   `(DocumentId, LocalViewIdentity) -> HostPanelId` meanings despite different
   `BufferId`s. A control with a different session proves no meaning is
   inherited from a path.

4. **Both allocators resume above every restored identity.** After reopening a
   payload whose highest panel identity is `1<<63 + 7`, the next minted panel
   is strictly greater, and likewise for groups. A payload whose stored mark is
   not strictly greater than an encoded identity refuses at load. A control
   proves that without the persisted mark the next mint would collide.

5. **Closed panels are dropped; stale panels are kept; colours outlive both.**
   A fixture closes one panel and marks another stale, then checkpoints. The
   closed panel mapping is absent, the stale mapping present, the witness
   exactly equals libaryon's census, and the coupled checkpoint succeeds.
   Retaining the closed mapping is shown to refuse. In the same fixture the
   closed view's **colour relation remains present**, and after reopen its
   returning view mints a fresh `HostPanelId` while recovering its exact prior
   palette positions. A colour relation is dropped only when its producer
   `DocumentId` stops being durable.

6. **The witness comes from the projected census, not the semantic map.** A
   session with `RESULT_PANEL`, `DASHBOARD_PANEL`, and two Rune-view panels
   produces a host-panel census of four while the encoded semantic map contains
   two. A witness built from `view_panel_ids` is shown to refuse. A group
   collected by `collect_unreachable_host_panel_groups` disappears from the
   projected census, and a witness rebuilt from that census after the refusal
   succeeds — proving the retry converges, which it cannot from
   `view_group_ids` alone.

7. **Encoded shared identities are checked at capture, against a real
   witness.** A payload encoding a `HostPanelId` or `HostPanelGroupId` absent
   from the supplied witness refuses before the coupled request is issued. The
   same payload decodes successfully in isolation, proving the check lives at
   capture rather than in the decoder.

8. **A payload that cannot join the census refuses without partial install.**
   A structurally valid payload naming a document, panel, or group absent
   from the first projected census refuses at post-spawn reconciliation and
   installs no runtime map — not the panel meanings, not the group
   meanings, not the colour allocations. A coupled checkpoint cannot
   produce this state, but generic `SavedSession::set_owner_payload` can
   author it directly, so the fixture builds it that way. A control proves
   the same payload joins cleanly when its census matches.

9. **Colour continuation survives a continuing session.** Two named views with
   distinct categorical fields checkpoint, reopen, and re-run; every category
   retains its exact palette position and resolved RGBA. A newly created
   session opening the same file begins fresh, and switching palette length
   begins a fresh allocation space rather than reinterpreting stored positions.

10. **Family spelling is closed and orientation-independent.** A bar rotated by
   PRODUCT-0055's pair resolution serializes `"bar"` in both orientations and
   preserves every series colour across restart. `"horizontalbar"`,
   `"stackedbar"`, and an unknown token each refuse.

11. **Anonymous instability is reproduced, not hidden.** Identical source
   reproduces anonymous colour continuation exactly. Inserting a view above an
   anonymous view shifts later identities and begins new lineages — asserted as
   the documented PRODUCT-0060 §7 semantics with a message naming it, so the
   behaviour cannot later be misread as a persistence defect.

12. **Publication is coupled and atomic.** The registered engine's ordinary
    `checkpoint()` refuses. A stale witness refuses without staging, and a
    rebuilt one succeeds. Pre-`CURRENT` faults leave all three owners
    authoritative; post-`CURRENT` faults expose the complete new generation and
    poison later checkpoints. No fault yields documents and meanings from
    different generations.

13. **Restored groups reattach once, without retiling.** A manual group with a
    detached member reopens, reattaches through its restored producer document,
    and retains arrangement and detached membership. A second reattachment
    attempt refuses, proving the one-shot capability PRODUCT-0061 added is
    consumed rather than reopened.

14. **Re-running resolves in place.** A restored unresolved panel, after its
    producer buffer re-runs, resolves to the same `HostPanelId` with its
    original colours rather than opening a second panel. Panel count is
    asserted before and after.

15. **Derived state stays absent.** No dashboard pane, `PlotPaneState`, kernel,
    `DataFrame`, result-registry entry, rendered image, execution snapshot, or
    Toron database appears in the payload. Reopening leaves the scratch
    database unavailable rather than replayed.

16. **The defaults remain still.** Ephemeral startup, Baryon, Rune execution,
    and every existing plot, table, SVG, draw-operation, and screenshot
    baseline pass without refresh. No test is ignored. Any fixture change is
    enumerated as a premise update.

## Stop condition

> This record may add Polariton's data-format dependency, its
> `polariton_session` owner and codec, the three re-keyed relations, both
> durable allocator marks, census-filtered capture, witness construction from
> the projected shared-identity census, pre-start decode, post-binding rebuild
> with post-spawn reconciliation, and one-shot group reattachment. If implementation requires persisting placement, a
> dashboard payload, a kernel or DataFrame, a runtime ID, a `PlotType`, or
> Toron state; returning the writable session to Polariton; publishing outside
> the coupled transaction; or re-executing Rune to restore a figure, work stops
> and the requirement returns to its owning record.

> If the projected shared-identity census is unavailable, incomplete, or
> carries any placement fact, work stops. A witness Polariton cannot rebuild
> after a refusal is a livelock, and a census that leaks ordering or placement
> would void the partition that licenses the three-set comparison.

> If a semantic identity must be recycled or rebound to make restoration work,
> work stops. PRODUCT-0061's three-set witness is licensed only by append-only
> identity, and defeating that invariant to land this consumer would silently
> invalidate the transaction that publishes it.

> If recovering a closed panel's prior identity or placement is required, work
> stops for a durable tombstone decision rather than encoding identities absent
> from the witness. Colour continuation is not that trigger: §4 preserves it
> independently of panel lifetime.

> If the 1 MiB owner bound rejects an ordinary observed session, work stops for
> a measured revision. Raising a bound without an exhibit is not allowed.

## Supersession

This record consumes PRODUCT-0061 §7's description of the planned Polariton
owner and makes it concrete. It supersedes no shipped behaviour: no Polariton
session state exists today.

It resolves the continuation exception PRODUCT-0059 identified and
PRODUCT-0007 §8 requires, for continuing sessions only. PRODUCT-0007's
within-session guarantee is unchanged.

It inherits, and does not attempt to repair, PRODUCT-0060 §7's anonymous
positional identity semantics. Stable anonymous-view identity remains its own
language decision with its own trigger.

`rune_plot_panes` and the nested dashboard remain with the dashboard-fate
cluster. PRODUCT-0061's exclusion is preserved for the same reason: their
content authority is unresolved.

## Rejected

- **Persist `BufferId` and remap on load:** the counter identifies nothing in a
  new process, and a remap table would need a durable key anyway — which is
  `DocumentId`.
- **Persist the renderer's `PlotType`:** lets a rendering vocabulary the
  plotting arc extends freely enter the durable schema without a decision.
  Adding a variant would not by itself invalidate existing payloads — no old
  document contains it — but renaming, removing, or re-spelling one would, and
  nothing in the plotting arc treats that enum as a schema. The authored family
  is a closed three-word set and is what PRODUCT-0055 keys lineage on, so the
  durable vocabulary changes only when a mark word does.
- **Derive the durable family from the resolved plot type:** a rotated bar
  resolves to `HorizontalBar`, so the lineage key would change and recolour a
  figure for a presentation-only change — the exact defect PRODUCT-0055 §4
  exists to prevent.
- **Reset the allocators on restore:** attractive because the maps are restored
  anyway, but it lets a fresh mint collide with a restored identity and
  destroys the append-only property PRODUCT-0061's witness depends on.
- **Encode stale mappings for closed panels:** preserves more continuation, but
  makes the payload reference identities absent from the census, so either the
  witness or the payload must lie. A tombstone class is the honest remedy and
  needs its own record.
- **Build the witness from `view_panel_ids`:** the obvious source, and wrong —
  it deliberately omits fixed-role panels, so the census would never equal
  libaryon's and every coupled checkpoint would refuse.
- **Persist the dashboard tree:** its leaves hold derived kernels, its manual
  plots depend on a transient `DataFrame`, and its notebook panes depend on an
  unrestored database. Topology alone restores empty panes.
- **Re-execute Rune during restoration to rebuild figures:** turns opening a
  session into running the user's code, with side effects, latency, and
  failures during startup. A restored panel is honestly unresolved until the
  user runs it.
- **Give Polariton its own generation:** permits documents, layout, and
  meanings to disagree after a crash, which is the defect PRODUCT-0061's
  coupled transaction exists to prevent.
- **Add a general serialization posture to Polariton:** the crate needs one
  data format for one payload, not derives across its domain types. A broad
  posture would make every plot and theme type a de facto schema.

## Consequences

- A reopened session shows the same figure in the same panel with the same
  colours, once its producer re-runs. That is the first user-visible result of
  the chain that began with an opaque 128-bit integer in PRODUCT-0059.
- Continuing a session is now structural for colour too: same `SessionId`, same
  `DocumentId`, restored allocation. A new session begins fresh, and the
  distinction is decidable rather than definitional.
- Polariton acquires a data format. That is a real posture change for a crate
  that previously persisted only through a Rune startup script, and it is
  deliberately scoped to one owner.
- Append-only semantic identity becomes a cross-restart property rather than a
  per-process one. The durable high-water marks are load-bearing for
  PRODUCT-0061's witness, not an optimisation.
- Panel meaning and colour lineage have separate lifetimes. A closed panel
  loses its identity mapping because the witness contract requires it, but the
  view keeps its palette positions as long as its producer document is durable.
  A future tombstone record therefore starts from a narrower problem: restoring
  a closed panel's identity and placement, not its colours.
- Durable named-panel identity is now unblocked. PRODUCT-0063 can name a panel
  because there is finally a durable thing to name — a meaning that survives
  restart, joined to a document that survives restart, in a generation that
  publishes atomically.
