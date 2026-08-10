# ADR 0071: durable-session attachment in status chrome

Status: proposed 2026-08-09. This record repairs PRODUCT-0063's visible-session
claim after the live saved-session exhibit showed that the application model and
the custom window chrome disagreed. Window titles remain product identity;
durable-session attachment appears in the status bar; opaque session identities
remain CLI and clipboard machinery.

## Context

PRODUCT-0063 §2 required a saved window title of the form:

```text
Polariton — session <SessionId>
```

and gate 4 asserted `application_title(&state)`. Production has two title paths.
`gui/state.rs:447` supplies `application_title` to iced's native window title,
while Polariton disables native decorations and renders its visible title bar
itself. `gui/components.rs:40` takes no session state and draws the literal
`Polariton`. The accepted gate therefore proved a model/native-window string,
not the title visible in the application. A live screenshot was the first
evidence that the two disagreed.

This is PRODUCT-0057's **assertion-gap** class in cause: the visible property
was available upstream and the accepted gate asked an object that the custom
chrome did not consume. It is not, however, covered by PRODUCT-0057's existing
instrument table. Polariton produces no backend-independent draw-operation
list for iced widget chrome. The custom title and `StatusBar::view` become iced
widgets directly; the named `PlotBackend`, SVG, raster, and wgpu instruments do
not observe them. The classifier identifies the gap but does not manufacture a
licensed recorder.

The available status oracle is stronger than PRODUCT-0063's helper assertion
for a different reason: `StatusBar { left: Vec<Segment>, ... }` is the model
that `StatusBar::view` consumes. Its saved/ephemeral segment difference is the
fact the production widget tree renders. A narrow structural guard can prove
that the application status view remains built solely from that model. The
custom title is simpler still: both title consumers become the same constant,
guarded at their production construction sites. If chrome-text assertions
multiply, a recording `iced_core::Renderer` and a sixth PRODUCT-0057 instrument
become their own substrate decision; this repair does not pretend that
instrument already exists.

The original remedy considered displaying a shortened `SessionId` in the title
and the complete identity in the Session menu. That solves the assertion gap
while solving the wrong product problem. `SessionId` is a 128-bit opaque local
store key, canonically rendered as 32 lowercase hexadecimal digits. It is useful
to `--session` and to exact copy/paste, not as ordinary human-facing chrome.

There is also a second opaque local identity in the current status prose:
`Attaching local workspace session <UUID>` and
`Joined local workspace session <UUID>`. That dashed UUID is Toron's workspace
nonce, not Polariton's durable `SessionId`. Showing both as generic “session”
identifiers would make unrelated authorities appear interchangeable.

Finally, a label such as `Saved session` would overclaim freshness. PRODUCT-0063
deliberately added neither background checkpointing nor a session-level dirty
bit. A crash may lose every mutation after the last explicit or successful
close-time checkpoint. The persistent fact Polariton can state is attachment
to durable-session authority, not that the current state is saved.

## Decision

### 1. The window title is always `Polariton`

The native application title and the custom title bar both render exactly:

```text
Polariton
```

for ephemeral and durable sessions alike. The application builder uses the
constant directly. `application_title(&AppState)` and its session-dependent
branch are deleted; no dormant function may continue constructing
`Polariton — session <id>`.

The title names the application, not persistence authority. This deliberately
supersedes PRODUCT-0063 §2's full-identity title and the title half of its gate
4. It does not abbreviate the identity; it removes identity from that surface.

### 2. The status bar names attachment, never freshness

A window attached to a saved session gains one persistent left-lane status
segment with the exact text:

```text
durable session
```

An ephemeral window has no such segment. The segment contains no `SessionId`,
generation, checkpoint time, checkmark, “saved” adjective, or dirty/clean
state. It means only that this process holds durable-session authority and that
explicit and close-time checkpoint actions are available.

The indicator is independent of transient success and error prose. A completed
checkpoint may still report `Session checkpointed` through the existing
transient status path, but that message does not mutate the attachment segment.
A failed checkpoint likewise leaves the attachment fact visible without
claiming the current state is durable.

Adding freshness later requires a record that defines a session-level dirty
predicate across documents, logical layout, panel meanings, colour allocation,
and every other owner in the coupled generation. Changing this label to
`saved`, adding a freshness icon, or deriving state from document dirtiness
alone is not a cosmetic follow-up.

### 3. Exact identity remains available for CLI use without being displayed

The saved-only Session menu retains `Checkpoint`, `Copy session ID`, and the
failed-close-only action from PRODUCT-0063. It gains no identity row. `Copy
session ID` still places the exact canonical 32-byte spelling on the clipboard,
and `polariton --session <copied-id>` still selects the same authority.

No prefix is displayed or accepted. Consequently this record needs no prefix
length, collision policy, lookup index, friendly-name registry, or migration.
Session discovery and human naming remain the deferred surface PRODUCT-0063
left open.

### 4. Toron workspace status names its authority without its nonce

The local-workspace progress and completion messages become:

```text
Attaching Toron workspace…
Joined Toron workspace.
```

They display neither the workspace nonce nor the generic phrase `local
workspace session`. Diagnostics and lower-level CLI/logging may retain the
opaque identity where it is operationally useful; ordinary Polariton chrome
does not.

This wording distinguishes the two durable-looking concepts by ownership:
`durable session` is Polariton/libaryon state, while `Toron workspace` is the
database workspace connection. Neither label claims the other one's lifetime,
checkpoint boundary, or identity.

### 5. PRODUCT-0063 gate 4 moves from a disconnected helper to consumed chrome models

The replacement gate asserts the two production-owned models that the chrome
actually consumes:

- the native application title and custom title construction sites both use
  the literal `Polariton`, with no dynamic title helper remaining; and
- `app_status_bar` produces exactly one `durable session` segment for saved
  authority and none for ephemeral authority, while `main_screen` constructs
  the status widget exclusively by calling `.view()` on that returned
  `StatusBar`.

The same saved/ephemeral split is asserted on `build_menu_roots`, which is the
model consumed by `MenuBar`. An assertion against `AppState.session_id` or a
new unconsumed presentation helper does not discharge the claim. The segment
and menu models do, because production views consume those exact values rather
than reconstructing their labels elsewhere.

The structural checks are intentionally narrow source-shape guards. Here the
property being guarded is itself a wiring fact: the view must not bypass the
tested model with a second label producer. They complement behavioral model
assertions; they are not substitutes for a renderer claim.

Because `StatusBar` owns private segment vectors, it may expose one
`#[cfg(test)] pub(crate)` observation that returns the already-stored segment
text and lane without formatting or reconstructing it. That seam observes the
production model; it is not a second presentation model and is unavailable to
production code.

Clipboard round-trip remains a separate behavioral assertion because clipboard
content is not a chrome-model property. The proofs meet at the boundary:
chrome says only that durable authority exists; Copy supplies the exact token
when the user chooses to take that machinery to the CLI.

## Implementation sequence

1. Bank the production custom-title literal and the `StatusBar` model produced
   for an ephemeral state, adding only the test-scoped segment observation
   required to inspect the private model. Add a narrow guard that `main_screen`
   obtains its status widget only from `app_status_bar(...).view()`; do not
   introduce an unlicensed text-recording renderer.
2. Replace the dynamic native title provider with the literal `Polariton` and
   delete `application_title`. Preserve the custom title's existing literal.
3. Thread the existing saved-session boolean into `app_status_bar` and append
   the exact persistent attachment segment only for saved authority.
4. Remove opaque Toron workspace nonces from the two ordinary progress/status
   messages while leaving connection identity and behavior unchanged.
5. Replace PRODUCT-0063's title test with consumed-model saved/ephemeral
   evidence; retain its exact clipboard/parser round trip as an independent
   assertion. Add implementation evidence naming the PRODUCT-0057
   reclassification and the current instrument boundary.

No session-store, checkpoint, close controller, persistence payload, menu
action, connection, or renderer behavior changes in this sequence.

## Acceptance gates

1. **The title is constant at both production consumers.** The application
   builder and custom title bar both use exactly `Polariton`. No production
   source constructs `Polariton — session`, `application_title` no longer
   exists, and neither construction site receives session presentation state.
   A helper-string assertion alone does not discharge this source/wiring gate.
2. **The durable indicator is a consumed attachment fact.** `app_status_bar`
   returns a `StatusBar` whose left segments contain exactly one `durable
   session` for saved authority. The otherwise-identical ephemeral call
   contains none, observed through a test-only projection of the stored text
   and lane. A structural guard proves `main_screen` obtains the status widget
   solely from that returned model's `.view()` and does not reconstruct the
   label. The positive and negative fixtures prove the gate cannot pass because
   a segment is produced unconditionally.
3. **Attachment does not impersonate freshness.** Before and after successful
   checkpoint, after a pre-publication checkpoint failure, and with mutations
   newer than the current generation, the persistent segment remains exactly
   `durable session`. No persistent segment contains `saved`, `clean`, `dirty`,
   a checkpoint age, or a generation number.
4. **Opaque identities do not enter ordinary chrome models.** With deliberately
   recognizable Polariton and Toron identities, inspect the production title
   construction, returned status segments, transient workspace status, and
   saved `build_menu_roots` result. Neither full identity nor any prefix occurs
   in those consumed values. The menu contains no identity row; its action
   label remains `Copy session ID`. Guards prove the title, status, and menu
   views consume those models rather than formatting either identity again.
5. **The exact reopening token remains exact.** Invoking `Copy session ID`
   writes all 32 canonical lowercase hexadecimal bytes, and feeding that value
   to the production startup parser selects the same `SessionId`. No shortened
   display value is accepted as input.
6. **The two authorities are named distinctly.** Local-workspace attachment
   emits exactly `Attaching Toron workspace…`; successful attachment emits
   exactly `Joined Toron workspace.`. Neither message contains `session`, the
   workspace nonce, or the Polariton `SessionId`. The durable attachment
   segment remains independently present when both facilities are active.
7. **PRODUCT-0063's remaining surface is unchanged.** Saved windows alone retain
   the Session menu, manual checkpoint, exact copy action, checkpoint-before-
   close controller, failure/discard behavior, crash boundary, and durable
   named-panel continuation. Ephemeral windows retain no Session menu and no
   durable attachment segment.
8. **The oracle repair and boundary are explicit.** Implementation evidence
   classifies the retired unconsumed-helper assertion as a PRODUCT-0057
   assertion gap, states that PRODUCT-0057 has no licensed iced-widget chrome
   recorder, and names the consumed `StatusBar` and `MenuRoot` models plus their
   narrow wiring guards as the available instruments. Clipboard/parser behavior
   stays under its non-rendering oracle. A passing assertion against a new
   helper string or state field alone fails this gate.
9. **The bounded suites remain green.** Polariton's default, no-toron,
   all-feature, architecture, and PRODUCT-0057 opt-in render-evidence targets
   pass. No plot, table, SVG, GPU, dashboard, persistence-schema, session-store,
   document, layout, colour, or Toron protocol baseline changes.

## Consequences

A saved and an ephemeral Polariton window now share the same application title.
Their persistence authority is distinguishable at a glance in the status bar,
and the existing Session menu supplies actions. Two saved windows are not
distinguishable from each other by chrome; that requires friendly naming or a
session selector, not exposure of opaque keys.

No chrome claims that the current generation contains the latest visible
state. Users still receive explicit checkpoint success/failure messages, and
successful close still checkpoints before teardown under PRODUCT-0063.

The screenshot-found discrepancy closes without making the screenshot's absent
full title become the new requirement. More importantly, PRODUCT-0057's
classifier repairs an accepted gate without being stretched into a license it
never granted. The defect is an assertion gap; the proportionate evidence is a
consumed chrome model plus a wiring guard. A general iced-widget recorder waits
until multiple chrome claims justify that substrate.

## Rejected

- **Show the complete `SessionId` in the title or menu:** exposes machine
  identity as permanent visual noise and confuses presentation with the CLI
  reopening token.
- **Show an eight-character prefix:** still exposes machinery, does not
  guarantee two saved windows differ, and invites a future lookup policy for a
  value this record does not accept as input.
- **Title the window `Polariton — saved`:** assigns persistence state to the
  application title and falsely suggests current visible work is checkpointed.
- **Label the status segment `Saved session`:** ambiguous between durable
  attachment and fresh publication; PRODUCT-0063 deliberately supplies no
  session-wide dirty predicate.
- **Display both Polariton and Toron identifiers with different punctuation:**
  formatting does not explain ownership, and ordinary chrome needs neither
  opaque key.
- **Keep the dynamic `application_title` as an unused model helper:** preserves
  the exact false lead that allowed PRODUCT-0063 gate 4 to pass while the visible
  title disagreed.
- **Build a recording iced renderer here:** no such PRODUCT-0057 instrument
  exists, and creating one for a title/status repair would turn a bounded
  product correction into renderer-test substrate. Repeated chrome-text claims
  are the trigger for that separate record.
- **Use GPU screenshot evidence:** title and status wording are not GPU
  compositing properties. A raster would be a larger, less precise instrument
  than the production models that feed the widgets.

## Stop condition

> This record may make the window title constant, add one saved-only attachment
> segment, remove opaque IDs from ordinary workspace status prose, replace the
> model-only title assertion with consumed-model evidence and narrow wiring
> guards, add one test-only observation of stored status segments, and update
> the bounded PRODUCT-0063 premise and documentation. If
> implementation needs a friendly name, session index, prefix lookup, dirty
> predicate, checkpoint scheduler, persistence-schema change, menu identity
> row, connection-protocol change, recording iced renderer, or GPU raster
> oracle, work stops and that requirement returns as its own record.
