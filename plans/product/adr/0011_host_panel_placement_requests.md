# ADR 0011: host panel placement requests

Status: accepted 2026-08-03. This record decides one thing: that a frontend may
*request* where a host panel first appears, and that the engine remains the
authority on whether to honor the request. It is the libaryon protocol extension
PRODUCT-0010 §9 halted on. It authorizes no layout persistence, no durable
identity, no dashboard change, and no new language surface.

## Context

PRODUCT-0010 landed views as ordinary host panels, and the dogfood buffer now
renders two tables and two plots in four panels with the dashboard untouched.
Every one of those panels arrived by splitting: `plans/baryon/output-panel.md`
§48 states the current behavior deliberately — *"Panels open as a horizontal
split at the bottom of the current tab's pane column"* — and `OpenHostPanel`
carries a `split_direction` and nothing else.

That is correct for one output panel and wrong for a view collection. Four views
produce four splits; six would produce six. The buffer that motivated PRODUCT-0010
is already at the edge of what splitting can present, and every additional named
view makes it worse.

The engine already has everything the fix needs except a way to ask for it:

- a pane holds a stack of tabs (`session.rs:2502`, `Pane { tabs: Vec<View> }`),
  established by `plans/baryon/doctab.md`;
- tabs are projected with stable identity including inactive ones
  (`projection.rs:434`, `DocTabSummary`);
- tab activation and closure are already protocol commands (`ActivateDocTab`,
  `CloseDocTab`);
- a host panel is an ordinary buffer (`BufferContent::HostPanel`) and therefore
  already representable as a tab rather than a split;
- `OpenHostPanel`'s own documentation already assigns the engine *"identity,
  geometry, focus, and tab membership."*

So a host panel appearing as a tab is not a new layout concept. It is an
arrangement the model already expresses and the protocol has no word for.

## Decision

### 1. `OpenHostPanel` carries a placement request

`split_direction` is replaced by a placement request describing where the panel
should first appear:

```text
Split { direction }        as today: a new split beside the focused pane
TabWith { panel_id }       a tab in whichever pane already holds that panel
```

The set is deliberately small. It grows when a use exists, not in advance of one.

### 2. Requests are advisory; the engine decides

The engine may decline any request and place the panel by its own rules. A
declined request is not an error and produces no failure path in the frontend.

This is the load-bearing half of the record. `OpenHostPanel` already states that
the engine owns geometry and tab membership, and a frontend that could *command*
placement would take that ownership by the back door — one panel at a time, until
layout is jointly owned by two components and neither can reason about it alone.
A request says what the frontend wants and why it is a coherent group; the engine
stays the only thing that knows what the workspace can accommodate.

The most obvious decline is a `TabWith` naming a panel that no longer has a pane.
Falling back to a split is correct there, and a frontend that treated it as a
failure would be asserting an authority this section denies it.

### 3. Placement applies only at first appearance

Reopening an existing `panel_id` continues to focus or reuse the live panel, and
its placement request is ignored. Placement describes where a panel is born, not
where it lives; a panel the author has since moved must never be relocated by a
rerun.

When `focus: true`, reopening a panel that is currently an inactive tab activates
that exact tab and focuses its pane. When `focus: false`, neither pane focus nor
the active tab changes. This was implicit while every host panel occupied its own
pane; tab placement makes it observable and therefore part of the contract.

This preserves the idempotent reopen that PRODUCT-0010's identity resolution
depends on.

### 4. Polariton groups a producer's views into one collection

The first view of a buffer's run opens with `Split`. Every subsequent view of the
same run opens with `TabWith` naming the first. Tab order follows PRODUCT-0010 §2's
presentation order: output-root source position, then list order, then chain order.

This is a **frontend policy**, stated here because it is the observable behavior
this record exists to produce, and it lives in Polariton rather than in libaryon.
The engine learns that four panels are one group; it does not learn what a view is.

`RESULT_PANEL` and `DASHBOARD_PANEL` keep their current placement. This record
changes where *view* panels appear and nothing else.

### 5. No new tab rendering

Heads already render tabs from `DocTabSummary`. A host panel joining a tab stack
is drawn by the machinery that already draws document tabs, and this record adds
no tab bar, no tab chrome, and no per-kind tab presentation.

The existing summary title must use `HostPanelDescriptor.title` for a host-panel
buffer. Today it falls through the file-path logic to `_`, which was invisible
while panels lived alone but would make a tab collection illegible. This is not a
new projection field or new rendering; it gives the existing `DocTabSummary.title`
the presentation metadata the engine already owns.

### 6. What this record does not authorize

No layout persistence and no `PanelRestored` producer. No durable document
identity. No change to Toron, the transport, the plotting language, or the
dashboard. No panel closing or moving commands. No placement request beyond the
two in section 1, and no expression of placement in Rune — PRODUCT-0010 §4 keeps
`HostPanelId` out of the language, and placement is one level further out still.

## Rejected

- **A separate `OpenHostPanelTab` command:** splits one operation across two
  commands whose only difference is an argument, and doubles the surface that
  every future placement rule must be stated over.
- **Frontend-chosen geometry:** contradicts `OpenHostPanel`'s existing assignment
  of geometry and tab membership to the engine, and makes layout jointly owned.
- **Requests the engine must honor:** the same defect wearing a politer name. An
  obligation to honor is authority transferred.
- **Placement expressed in the plotting language:** PRODUCT-0010 §4 keeps the
  head's realization out of the normal form, and a `view(...)` that could name a
  pane would put layout in the language permanently.
- **Auto-tabbing every host panel, including the result table:** changes behavior
  that is working and that no evidence complains about. Section 4 confines the
  change to view panels.
- **Waiting for layout persistence:** placement is observable in one session and
  provable now; persistence is a separate capability with a separate cost, and
  binding them would make a small protocol addition wait on an editor-wide
  decision.

## Consequences and sequencing

- A buffer with many views produces one pane holding a tab stack rather than N
  splits, which is what makes a view collection usable and is the first half of
  the addressable-tabs model.
- `OpenHostPanel`'s signature changes, and its two existing call sites in
  Polariton move to `Split`, preserving today's behavior exactly.
- Baryon inherits the capability without using it, since it opens no view panels.
  `plans/baryon/output-panel.md` §48 remains accurate for the output panel.
- PRODUCT-0010 §9's halt fired correctly and this record is its resolution. The
  remaining §9 deferral — durable identity — is unblocked by neither this record
  nor any record that does not also decide layout persistence, because a durable
  address has no observable consequence until a layout survives to reattach it.
- The named-view address becomes visibly useful: a named view is a tab whose
  identity survives reruns and form changes, which is the behavior PRODUCT-0010
  proved and this record makes legible.
- A future record may add placement requests, decide layout persistence, or give
  panels closing and moving commands. None is authorized here.

## Acceptance gates

1. `Split` preserves current host-panel behavior, including the new pane and its
   projected divider.
2. `TabWith` a live panel creates no new pane, appends one tab, preserves focus
   when `focus: false`, and projects descriptor titles in request order.
3. `TabWith` an unavailable panel is advisory: the new panel still opens by the
   engine's fallback policy and no placement error reaches the frontend.
4. Reopening an existing panel ignores the new placement request; `focus: true`
   activates its exact tab without moving it.
5. Closing one host-panel tab disposes only that panel and leaves its sibling
   tabs and pane live.
6. Polariton's four-view dogfood shape produces one new pane with four tabs in
   PRODUCT-0010 presentation order. `RESULT_PANEL` and `DASHBOARD_PANEL` still
   request their existing splits.
