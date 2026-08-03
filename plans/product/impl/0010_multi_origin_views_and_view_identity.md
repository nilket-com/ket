# Multi-origin views, view identity, and host-panel realization

Scope: implement PRODUCT-0010. Establish two preconditions, then normalize the
envelope to a multi-origin program, dispatch origins independently with per-origin
failure isolation, and realize views as ordinary libaryon host panels. Deliverable
is four `impl:` commits, one per stage, each green on its own.

The arc is judged by PRODUCT-0010 §8's thirteen gates. Every stage below names the
gates it discharges; a gate is discharged once, by a behavioral test, and no gate
may be satisfied by a test that inspects only the analysis.

## Halt rule

PRODUCT-0010 §9 binds this implementation:

> This record may add multi-origin dispatch, view naming, view-identity
> resolution to host panels, and the producer-state mapping in section 6. If the
> implementation requires a new libaryon protocol capability, a change to Toron or
> the transport, a new sink form, or a layout placement policy, the work stops and
> the requirement returns as its own record.

Concretely, the work stops if it requires editing anything under
`crates/libaryon/`, `crates/toron*`, or `toron-client`; if it requires a new
`EditorCommand`, `HostEvent`, or projection field; if it adds a sink constructor
beyond `view`; or if it decides where panels are placed relative to one another.
Stopping means reporting the finding, not working around it.

Stage 0 exists because the most likely trigger is a precondition, and it is
cheaper to discover that before any of the language work is written.

### Where work may land

- `crates/polariton/src/rune_relation_envelope.rs` — recognition, normalization,
  view identity, scheduler;
- `crates/polariton/src/gui/state.rs` — dispatch, per-origin result handling,
  identity resolution;
- `crates/polariton/src/gui/messages.rs` — per-origin message payloads;
- `crates/polariton/src/editor.rs` — `PanelContent`, `PanelRegistry`, panel-id
  allocation;
- `crates/polariton/src/gui/plot_pane/` — reuse only, to host an existing
  `PlotPaneState` inside a panel; no new plot behavior;
- `crates/polariton/src/adapters/toron/mod.rs` — live gates;
- `crates/polariton/tests/` — arc-level gates;
- `plans/product/impl/0010_multi_origin_views_and_view_identity_evidence.md`
  — Stage 0 findings and the final gate ledger;
- `plans/product/corpus/rune_workspace_corpus.md` — post-implementation status
  update in its separate evidence commit.

Any other file is a halt. `crates/polariton/src/gui/dashboard/` is explicitly not
on this list: PRODUCT-0010 §7 demotes the dashboard to a consumer and authorizes no
change to it.

## Stage 0: preconditions

No language work begins until both are answered in writing in
`0010_multi_origin_views_and_view_identity_evidence.md`. Each has a decision
gate, and each gate has a documented fallback that keeps the arc moving without
violating the halt rule.

### 0a. Durable document identity

PRODUCT-0010 §5 keys named panels on `(workspace identity, durable document
identity, view name)`. What the head can see today:

- `session.rs:778` has `fn file_path(&self) -> Option<&str>`, but it is
  `pub(crate)` and therefore invisible outside libaryon.
- `PaneViewport` (`projection.rs:239`) carries `file_name: Option<String>`. It is
  a **path string, not a basename** — `host.rs:1965` asserts it equal to a full
  `to_string_lossy()` path — but it is of **unspecified normalization**. A
  document opened as `./test5.rn` and as `/home/me/work/uat/test5.rn` would key
  differently.
- `source_path` exists only on `MarkdownPreviewSnapshot`, so it is not a general
  document identity.
- `PaneViewport` is per **pane**. A buffer with no visible pane projects no
  `file_name`, and view identity must resolve for a buffer that is running but
  not currently on screen.

Establish: whether a stable, normalized document identity is obtainable for an
arbitrary `BufferId` from the head, using only what libaryon already projects.

**Gate.** If yes, Stage 3 keys the durable table on it and gate 6 is proved across
a simulated restart. If no — and the honest reading of the evidence above is that
it probably is no — then obtaining one requires a new libaryon projection field or
command, which §9 halts on. The fallback is explicit and is not a workaround:
**the first cut keys the table on a session identity only**, every named view
behaves identically within the session, gate 6 is proved across reruns rather than
across restarts, and durable keying returns as its own record together with the
layout persistence that `PanelRestored` is already waiting for. Record that
outcome in the evidence file rather than leaving it implied by the code.

### 0b. Dynamic `HostPanelId` allocation

`HostPanelId::new` is a `const fn` over a `u64`, and Polariton's entire panel
population is two constants: `RESULT_PANEL = 1` and `DASHBOARD_PANEL = 2`.
Allocation is therefore mechanically trivial; the risks are elsewhere.

Establish: that the engine accepts arbitrary frontend-chosen panel ids; that
reopening an existing id focuses or reuses rather than duplicating a panel — the
`known` check at `editor.rs:754` suggests Polariton must decide this itself; and
that ids allocated for views cannot collide with the two reserved constants or
with each other across a session.

**Gate.** Allocation must be a single owned counter or table, not an ad-hoc
`new(n)` at each call site. Define a reserved dynamic range above the existing
constants and require future fixed ids to stay outside it; merely starting at
`3` does not prevent a later constant from colliding.

**Gates discharged: none.** Stage 0 produces evidence, not behavior.

## Stage 1: envelope normalization

Turn the single-relation envelope into `BufferViewProgram`. This is a language
front-end stage and is provable without a running Toron; the duplicate-name gate
also exercises the existing head routing of a local refusal.

### Representation

```text
BufferViewProgram {
    executions:         canonical origin -> ordered sinks   (grouping)
    presentation_order: ordered view references             (PRODUCT-0008 §5)
}

LocalViewIdentity = Named(name) | Anonymous(position)
ViewIdentity      = (producer, LocalViewIdentity)
```

`executions` and `presentation_order` are **two independent orderings over one set
of views**, and conflating them is the defect gate 3 exists to catch. Grouping is
a property of dispatch; presentation order is output-root source position, then
list order, then chain order, exactly as ADR 0008 fixed it.

`MixedRelations` loses its meaning and is removed — differing origins are now the
normal case, not a refusal. `DuplicateForm` is **deleted**, not generalized;
`RuneViewForm` survives only as a label for diagnostics and content selection, and
must not appear in any identity or key.

`EnvelopeSemantics` gains the origin grouping and view identities, and its
`execution_count` stops being hardcoded to `1` — it becomes the count of distinct
canonical origins, which is what that field was reserved for.

### Recognition

`view(name, sink)` is a contextual identity **wrapper**, not a sink form. It is
recognized only as an element of a proven `.views([...])` list, under ADR 0008 §3's
atomicity: `view("n", scatter("x","y"))` wraps the sink that `scatter("x","y")`
already produces, and the wrapper contributes identity and nothing else. It takes
exactly two arguments, the first a string literal; anything else refuses at its own
span. A nested `view(...)` refuses.

Duplicate explicit names refuse naming **both** spans, which is a new error variant
carrying two ranges rather than the one that `InvalidView` carries.

Anonymous positions are assigned in `presentation_order`, so a view's anonymous
identity is a function of the program's presentation order alone and never of its
execution grouping.

The envelope carries `LocalViewIdentity`; it has source text but no runtime
`BufferId` and must not acquire one. Stage 3 stamps the producing buffer/session
identity to obtain `ViewIdentity` before resolving a host panel.

**Gate discharged: 5.** Exercise the refusal through `run_rune_code`, not by
calling `analyze` directly, and assert the user-visible diagnostic names both
spans. Gates 3 and 4 receive their analysis halves here but are not discharged
until Stages 3 and 2 respectively exercise the behavior beyond analysis.

Gate 3's analysis half uses the interleaved fixture from PRODUCT-0010 §2 —
`table(a); plot.scatter(b, …); plot.bar(a, …)` — asserting two origins in
`executions` and the presentation order table-`a`, scatter-`b`, bar-`a`. Gate 4's
analysis half asserts two anonymous bars over two origins lower without refusal,
and that no `DuplicateForm` variant exists to be constructed.

**Regression.** Every existing envelope test passes unmodified except those
asserting `MixedRelations` and `DuplicateForm`, whose premises this record
retired; `origin_resolved_identity_keeps_mixed_duplicate_and_inline_behavior`
splits accordingly. Its `repeated_inline` half is load-bearing and must stay
green: two separately written identical origins remain **one** execution, which is
gate 1's analysis half.

## Stage 2: independent dispatch and failure isolation

### Scheduling

`RuneViewScheduler` keys `in_flight` by `BufferId` and holds one `pending` program
per buffer. Preserve that supersession model — newest program wins, and the
existing `submitted` / `executed` / `superseded_before_execution` counters keep
their meaning — and change only the unit of tracking:

- in-flight is tracked per `(BufferId, canonical origin)`;
- a newly submitted program for a buffer with any origin in flight is held as
  `pending`, superseding an already-held one exactly as today;
- the held program dispatches when the **last** in-flight origin for that buffer
  completes;
- `cancel` clears every origin for the buffer, preserving the invariant that an
  unavailable workspace cannot leave a buffer permanently in flight.

The `complete()` assertion that a completing buffer was in flight stays a hard
invariant, now per origin.

### Results

Each origin dispatch carries `(source_buffer, revision, origin key)` and produces
its own message, applied on arrival rather than batched. The existing revision
guard drops stale results unchanged. Sinks within one origin continue to share one
`Arc`, and the `Arc::ptr_eq` assertion in `state.rs` survives — narrowed from *all
sinks in the buffer* to *all sinks of one origin*, which is what the evidence
actually proved.

Origins may complete out of order, and nothing may depend on their order. A test
that only ever exercises sequential completion has not proved gate 2; drive
out-of-order completion explicitly.

**Gates discharged: 1, 2, 4.** Gate 1 needs the live half: two
roots over one binding issue exactly **one** `dispatch_rune` and share one `Arc`.
Gate 2 asserts two distinct origins produce two dispatches with no ordering
dependency between them. Gate 4 drives two anonymous bars over distinct origins
through dispatch and observes two sink outcomes; lowering without refusal alone
is not enough. Gate 10 gets a dispatch prerequisite here — a failing origin does
not cancel its sibling — but is discharged only when Stage 3 proves the successful
sibling refreshes while the failed origin's existing panel remains stale.

## Stage 3: host-panel realization

### Plot content in an ordinary panel

`PanelContent` is Polariton-owned — *"None of this is representable in libaryon:
the engine only knows the panel exists"* — and already renders `Table(Box<DataFrame>)`
directly. Add a `Plot` variant hosting the existing `PlotPaneState`, which
`DashboardState` already constructs via `PlotPaneState::with_kernel`. The `surface`
function at `editor.rs:2162` gains a `Plot` arm beside its `Table` arm.

Do not route an ordinary panel through `DashboardMsg`: the existing plot-pane
view maps its interaction and close chrome into the private dashboard tree. Add
or factor a host-panel plot surface whose `PlotPaneMsg` updates the
`PanelContent::Plot` state directly. Host-panel close remains engine chrome, and
plot interaction must not mutate `DashboardState`.

This is the piece PRODUCT-0010 §7 authorizes and the reason removing
`open_dashboard()` is necessary but not sufficient: without a plot arm, a plot
routed to an ordinary panel renders as `Unresolved`.

### Identity resolution

Polariton owns `ViewIdentity -> HostPanelId`. `HostPanelId` never enters the
envelope module; the resolver lives in the head and is the only place the two
vocabularies meet. A named view resolves to the same id on every rerun, including
when its sink form changes — the resolver keys on the name, never on
`RuneViewForm`.

Panels open via `OpenHostPanel` with `focus: false`, so a running buffer never
steals focus from the author's cursor, and with `kind` distinguishing plot from
table content for a future layout policy that this record does not write.

Resolve and open the authoritative program's panels in **presentation order when
dispatch begins**, before any origin completes. Completion order therefore cannot
choose split order or otherwise leak execution grouping into presentation. The
panels begin as `Running`; origin results only refresh their already-established
identities.

### Producer state

Map onto the existing protocol rather than beside it. `PanelEntry` today holds
`revision`, `content`, and `scroll`. Freshness is **not** a `PanelContent` variant:
stale means *previous content, visibly marked*, while `Failed` means *replaced by
an error*. Add stale/orphan metadata beside `content`, render its badge or overlay
on the panel surface, and set it per PRODUCT-0010 §6:

| Event | Effect |
| --- | --- |
| Successful analysis | new named set authoritative; absent names orphaned |
| Parse / envelope failure | existing panels marked stale; nothing orphaned |
| Origin failure | only that origin's views marked stale |
| Sink resolution failure | only that sink marked stale |
| Name returns | reattaches to preserved panel and layout |

Orphaning marks; it never sends a close. Panel disposal is the engine's to decide
when its last workspace reference goes, and Polariton is not a second authority on
panel lifetime.

Failure preserves and marks prior successful content. A first-run origin or sink
failure has no prior content to preserve, so its already-open `Running` panel
becomes `Failed(message)` instead. Anonymous identities are session-local ordinal
slots keyed only by producer and presentation position; a structural edit may
retarget a slot, and an excess slot may be marked orphaned and recycled if that
position returns. No anonymous mapping is persisted.

**Gates discharged: 3, 6, 7, 8, 9, 10, 11, 12, 13.**

Gate 3 drives the interleaved two-origin fixture through the head, completes the
origins out of order, and asserts that panel opening and view realization retain
table-`a`, scatter-`b`, bar-`a` presentation order.

Gate 9 uses a committed behavioral fixture reproducing `../uat/test5.rn` as
written, both `table()` sinks included, and asserts two distinct panels with two
distinct frames. Running the external dogfood file is useful manual evidence but
is not an automated-test dependency.

Gate 13 is behavioral, not a grep: run a view to completion and assert
`DashboardState` is untouched — `panes` still `None` and `content` empty. A
secondary architecture assertion that the Rune view path contains no
`open_dashboard` call is a cheap guard against reintroduction, but it does not
discharge the gate on its own.

Gate 11 is proved within a session: delete a named view, rerun, and assert the
panel survives with its marker set and no disposal request sent. Across a restart
it is gated on Stage 0a and, under the documented fallback, is explicitly out of
scope for this arc.

## Non-goals

- No change to libaryon, Toron, the transport, `PlotSpec` artifacts, kernels, or
  theming.
- No new sink form. `view` is an identity wrapper, and the sink set stays
  `scatter`, `bar`, `table`.
- No relation names or handles.
- No tab placement, requested or automatic. Views open as ordinary panels and the
  author arranges them.
- No dashboard change of any kind, including migrating its export bounds or
  resize-suspension behavior.
- No layout persistence mechanism.
- No relaxation of the PRODUCT-0003 relation-verb assay. The origin grouping is
  keyed on text the analysis already computes and must not acquire an opinion
  about what any relation verb means.

## Review checklist

- [ ] Stage 0 answered both preconditions in writing before Stage 1 began, and
      recorded which durable-identity branch was taken.
- [ ] Panel ids come from one owned allocator, reserved above the existing
      constants.
- [ ] `executions` and `presentation_order` are independent; grouping is invisible
      to presentation order.
- [ ] `MixedRelations` and `DuplicateForm` are gone, and `RuneViewForm` appears in
      no identity or key.
- [ ] `execution_count` reports distinct canonical origins.
- [ ] `view(name, sink)` is recognized only in `.views([...])` argument position,
      refuses nesting, and refuses a non-literal name.
- [ ] Duplicate names refuse with both spans.
- [ ] `repeated_inline` stays green: identical canonical origins are one execution.
- [ ] Out-of-order origin completion is exercised, not assumed.
- [ ] The `Arc::ptr_eq` assertion survives, narrowed to one origin.
- [ ] Staleness preserves previous content; `Failed` remains distinct from stale.
- [ ] Orphaning never sends a close request.
- [ ] The committed `test5.rn` behavioral fixture yields two frames in two
      panels; the external dogfood file also runs as written for manual evidence.
- [ ] Completing a view leaves `DashboardState` untouched, proved behaviorally.
- [ ] Work stayed within the permitted files; the halt rule was not triggered, or
      the work stopped and reported.
- [ ] Corpus updated in a separate commit, not in any of the four.
