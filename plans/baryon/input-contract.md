# Specification: Input Contract

## Overview

Move modal input into `libaryon`. The engine gains a portable input ingress, owns mode, the
pending-input state machine, and the prompt; resolves the keymap; and projects enough input state for
a frontend to render. The frontend becomes a terminal driver: crossterm translation, rendering, mouse
hit-testing against rendered geometry, and terminal lifecycle.

Semantic `EditorCommand` submission is preserved for programmatic and headless embedders. The engine
has **two ingresses and one execution path**:

> Portable input resolves to an **internal input action**. That action may change modal state, emit
> zero or more `EditorCommand`s, or invoke a script callback. All actual editor mutation continues to
> flow through the existing command handlers.

Clients that submit `EditorCommand` directly bypass modal input, and therefore bypass script-defined
mappings. That is correct: a programmatic client asking for `DeleteLine` wants `DeleteLine`, not
whatever a plugin rebound `dd` to.

This is item 2 of `plans/rune-scripting.md` §5. It is a prerequisite for script-defined keymaps but is
**justified independently**: the engine is not a modal editor today, so any second frontend must
reimplement modal editing from scratch — and the frontend/engine mode split races (§1.4).

**Implementation status: complete.** The historical ground truth in §1 records the pre-migration
state; §2-§6 now describe the implemented contract.

## 1. Ground truth

### 1.1 The pending-input state machine

`InputState` (`baryon/src/ui/keymap.rs:9-20`) is
`Normal(NormalPending) | Visual{anchor, kind, pending} | Insert | Command | Search | Confirm`.
`NormalPending` (`keymap.rs:80-94`) has 13 variants; `VisualPending` (`keymap.rs:118`) has five.

**There are no counts.** Maximum sequence depth is five keystrokes (`"a` `d` `i` `w`). `PaneResize` is
the sole unbounded state, re-arming itself (`dispatch.rs:343-347`) until `Esc`.

State retained between keystrokes lives in exactly two places: `InputState` itself, and
`Frontend::last_char_search` (`core.rs:24-28,39`) — the sticky `f`/`t` target that `;` and `,` repeat.

### 1.2 No ambiguity, and therefore no timeout — structurally

**No key is both a complete binding and the prefix of a longer sequence.** Enforced, not merely true:

- a `BindingSpec` has an arm action *or* an effect action, never both (`keymap.rs:2742`);
- `(context, pattern)` pairs are asserted unique per context (`keymap.rs:2894`);
- the arm graph is asserted acyclic and fully reachable (`keymap.rs:2971`).

Consequently **no timeout exists anywhere in the input path**; pending states wait indefinitely. Keys
are read with a blocking `event::read()` (`core.rs:178`) inside a drain loop guarded by
`event::poll(0)` (`core.rs:198`).

The frontend has timers — a 16 ms tick (`core.rs:176`), an 80 ms spinner (`core.rs:16`), a 1500 ms
reload probe (`core.rs:172`), a 500 ms mouse `DOUBLE_CLICK_INTERVAL` (`mouse.rs:17`) — but **none touch
`InputState`**. Double-click is the only time-based input disambiguation, and it is a mouse concern
that stays in the frontend.

### 1.3 Modifier matching is incoherent

`KeyPattern::matches` (`keymap.rs:203-217`):

| pattern | modifier handling |
|---|---|
| `Ctrl(c)` | `modifiers.contains(CONTROL)` — **subset, not exact**: `Ctrl+Alt-d` matches `Ctrl(d)` |
| `Alt(c)` | `modifiers.contains(ALT)` — same |
| `Special(k)` | **ignores modifiers entirely** |
| `Char(c)` | **ignores modifiers entirely** |
| `AnyChar` | **ignores modifiers entirely** |

Resolution is by pattern precedence (`keymap.rs:192`, `min_by_key` in `resolve_*`), so in a context
that *has* a `Ctrl-d` binding, `Ctrl-d` correctly beats `d`. But in a context with no Ctrl bindings the
plain-`Char` binding wins: **after `d`, pressing `Ctrl-d` executes `dd`**. Same for `y`, `z`, `s`, `g`
sub-contexts. There is no coherent matching contract to port. §4.1 defines one.

### 1.4 Mode is frontend state, and the split races

`session.ui.mode_override: Option<EditorMode>` (`session.rs:516`) is *drained* each render
(`render.rs:173,212`), shipped on `Viewport.mode_override` (`projection.rs:223`), and applied by the
frontend only if the projected mode differs (`core.rs:221`). `Confirm` is already engine-authoritative:
no keymap binding produces it.

**The race.** The channel is async and carries no ordering token. Between the frontend sending
`ChangeInnerWord` and receiving the `Viewport` bearing `mode_override: Insert`, the frontend is still
in `Normal`; keys typed in that window are resolved as Normal-mode commands and sent to an engine about
to declare Insert. Same for engine-initiated `Confirm` during `:s///c`. **Engine-authoritative mode
eliminates this class outright** — a correctness argument for this spec independent of scripting.

### 1.5 The frontend renders from input state directly

- `draw.rs:1363-1373` renders the mode indicator as `"NOR sr"` / `"NOR s"` / `"NOR g"` / `"NOR"` —
  **read straight off `InputState`'s pending variant.**
- `draw.rs:1132-1134,1429,1449` render the prompt from `command_buffer`, `command_cursor`, and
  `search_buffer`.

A `Viewport` that carried only `mode: EditorMode` would silently break the status line and the prompt.
See §2.3.

### 1.6 Prompt, paste, mouse, history

**Prompt state is frontend-owned** (`core.rs:30-59`): `command_buffer` + `command_cursor` (full line
editing); `search_buffer` (**append-only — no cursor**; `prompt_delete` and `prompt_move_cursor` are
no-ops for Search, `command.rs:201,216`); `command_history` / `search_history`;
`visual_command_range`; `language_completion_candidates` pushed from the engine each frame.

**`PromptAction::Complete`** (`command.rs:232-285`) performs **filesystem I/O** for path completion,
with no entry or byte cap.

**`HistoryStore` performs heavy blocking I/O on every append** (`ui/history.rs`): `create_dir_all`
(`:100`), a lock file with `lock.lock()` (`:103,110-128`), `repair_incomplete_history_file` (`:157`,
reads the entire file), append + `flush` (`:131-143`), then `compact_history_file_if_needed` (`:146`,
reads and rewrites). Naively moving the prompt into the session moves all of this onto the engine
thread.

**Paste bypasses the keymap entirely.** Bracketed paste (`app.rs:33`) arrives as `Event::Paste(String)`
(`core.rs:193`) → `handle_paste` (`command.rs:13`), mode-dependent: `Insert` → `InsertText`; `Command`
→ strip newlines, insert at cursor; `Search` → strip newlines, append; `Normal` / `Visual` / `Confirm`
→ **silently dropped**.

**Mouse** hit-tests against the last rendered `Viewport` (`mouse.rs:217`), never touches `InputState`,
and reads mode only to choose click-vs-add-cursor (`mouse.rs:239`).

## 2. The contract

### 2.1 One ordered request stream

Keys, paste, direct commands, mouse-derived commands, internal refreshes, and future job completions
must be **totally ordered** with respect to one another. Two differently-typed senders cannot provide
that. The engine therefore receives a single stream:

```
// crate-private. Not part of the embedding API.
enum EngineRequest {
    Input(InputEvent),          // portable, resolved against engine mode
    Command(EditorCommand),     // semantic; the headless/programmatic ingress
    Internal(InternalEvent),    // refresh, job completion, deferred-event wakeups
}
```

**`EngineRequest` is crate-private.** `InternalEvent` is an implementation capability — job
completions, refresh wakeups, deferred-event drains — and an embedder must not be able to forge one.
Exposing the enum would make every internal wakeup a public API surface.

**Input-generated commands are dispatched synchronously and in order, then rendered once.** They are
*not* re-enqueued onto the channel, where unrelated requests could interleave between the two commands
of a `ViewportLine` or the three of an `Esc` (§1.3 of the prior draft; `dispatch.rs:268-292,506-524`).
This requires extracting the command dispatcher from the run loop's `match` so that it can be invoked
both from `Command(..)` and from a resolved `InputAction`.

**Implemented.** `EngineRequest::{Command, Input, Internal}` is crate-private and all three typed
adapters share one bounded ordered sender. Input actions invoke the extracted `dispatch_command` seam
synchronously for each emitted command and render once after the batch. The synchronous write path
retains its intentional intermediate busy frame.

**Correction to the prior draft.** The run loop's *blocking* behavior is unchanged — §2.5's
prefix-free invariant means no `recv_timeout` and no deadline. But the claim that the control structure
is "untouched" was wrong: `Receiver<EditorCommand>` becomes `Receiver<EngineRequest>` and the giant
`match` must be reorganized around an extracted dispatcher.

**`EditorCommand` compatibility is semantic, not literal.** `EngineHandle::command_sender` previously
handed out `mpsc::Sender<EditorCommand>`. It now returns a typed adapter that wraps each command in
`EngineRequest::Command`, preserving the public embedding API of `README`'s example. The input adapter
lands with portable input (§2.2); both use the same ordered sender underneath.

`EngineHandle` exposes **two permanent typed adapters** — a command sender and an input sender — both
wrapping the same internal `Sender<EngineRequest>`, and therefore both totally ordered against each
other. This is an **encapsulation boundary, not a temporary shim**: embedders express what they mean
(a semantic command, or a portable input event) and the engine keeps sole authority over the internal
event space.

### 2.2 `InputEvent`

Portable, crossterm-free, in `libaryon`:

```
InputEvent::Key { code: KeyCode, modifiers: Modifiers }
InputEvent::Paste(String)
```

`Paste` is first-class because paste behavior is mode-dependent (§1.6) and mode becomes engine state.
It cannot be modelled as a sequence of `Key` events without changing behavior.

Mouse and resize are **not** `InputEvent`s (§2.6).

**Implemented.** The public, crossterm-free `InputEvent`, `KeyCode`, and exact-set `Modifiers` types
live in `libaryon`; `EngineHandle::input_sender` feeds them into the ordered stream. The TUI translates
crossterm once at its event boundary, including `BackTab → Tab+Shift` and removal of character `Shift`
already represented by the character. Resolution happens against live engine state.

### 2.3 `InputProjection` on the `Viewport`

The frontend must render prompt text, prompt cursor, the pending-prefix indicator, and
completion-in-progress state (§1.5) — all of which become engine state. The `Viewport` therefore gains
an **`InputProjection`**: mode, prompt buffer and cursor, a pending-sequence *label* (the `"sr"` of
`"NOR sr"`), and completion status.

**`InputState` itself is not exposed.** The projection is derived, not a leak of the internal enum;
plugins and frontends see a rendering contract, not the state machine.

`Viewport.mode_override: Option<EditorMode>` becomes `InputProjection.mode: EditorMode` —
authoritative, always present. `reset_global_transients` (`runtime.rs:113`) stops draining it, and
`apply_mode_override` (`core.rs:221`) disappears with the frontend's `InputState`.

**A pure input transition that changes the projection must request a render even though it emits no
`EditorCommand`.** `CommandEffect { needs_render }` (`runtime.rs:42-51`) already carries exactly this;
arming `s` must set it. Missing this is how the pending indicator silently stops updating.

**Implemented.** `Viewport.input` is authoritative. It projects mode, prompt text/cursor, pending
sequence label, and completion state; the frontend owns none of the underlying modal or prompt state.
Pure transitions render, including prefix arm/cancel and prompt edits.

### 2.4 Modifier matching contract

Replacing §1.3's incoherence. Every portable key match is defined by:

- **Modifier sets are exact.** `Ctrl-d` does not match `Ctrl+Alt-d`. `.contains()` becomes equality.
- **Shift is normalized away** where it is already reflected in the character: `Shift-a` arrives as
  `Char('A')` with no `SHIFT` bit. Shift is only meaningful on `Special` keys (`Shift-Tab`).
- **`Ctrl+Alt` and other combinations are distinct patterns**, matchable only by an exact-set binding.
- **Wildcard capture (`AnyChar`) accepts unmodified characters only**, plus explicitly-listed
  modified forms. Today `AnyChar` matches on `KeyCode::Char(_)` while ignoring modifiers
  (`keymap.rs:215`), which is why `r` followed by `Ctrl-c` replaces the character under the cursor
  with a literal `c` rather than cancelling.

Without this, runtime mappings cannot have predictable collision semantics — §2.5 depends on it.

### 2.5 Ambiguity is forbidden at registration, not resolved at input

**A key is a prefix or a binding, never both** — §1.2's invariant is promoted from an emergent property
of the static table to a rule the engine enforces on every mapping, static or runtime.

- The invariant is defined against the **effective map within one mode/context**, not globally.
- **Registration is atomic**: a mapping set that would introduce a conflict is rejected whole, leaving
  the previous map intact. There is no partially-applied plugin keymap.
- **A maximum sequence length** is enforced.

A deliberate divergence from Vim, which permits ambiguity and resolves it with `timeoutlen`:

- **The engine's run loop stays blocking.** No `recv_timeout`, no deadline, no timer.
- No `timeoutlen`, and no class of bug where a mapping fires because the user typed slowly.
- Cost: a plugin cannot bind `gd` if `g` is a standalone binding. In Baryon it is not, and the arm
  graph already guarantees no such conflict exists. Plugins get a registration error rather than
  timing-dependent behavior.

One-way door: ambiguity plus a timeout can be added later. It cannot be removed later, because scripts
would by then depend on the timing semantics.

### 2.6 Mouse: click intent, and a stale-hit-test guard

Keys need no sequence number — §2.1's single ordered stream gives them ordering by construction. Mouse
is a genuinely different problem, and the prior draft wrongly bundled them.

Mouse resolves against **rendered geometry** from a `Viewport` that may be stale. Therefore:

- The `Viewport` carries a **`ViewportId`: a hit-test revision**.
- Mouse-derived requests carry the revision they were hit-tested against.
- **The engine rejects a stale hit-test** rather than applying it to a document that has since changed.

**The `ViewportId` must change only when the geometry-to-content mapping becomes invalid** — a resize,
a scroll, a pane change, a document edit — **not on every render.** Bumping it per frame would reject
perfectly valid queued clicks, and would break the second half of a double-click across an intervening
cursor-only or status-only render. The revision tracks *what a click would resolve to*, not *what was
drawn*.

Additionally, **mode-dependent click interpretation moves into the engine**. The frontend hit-tests
geometry and sends a *click intent*; the engine decides `ClickCursor` versus `AddCursorAt` from its
own current mode (`mouse.rs:239` consults frontend mode today, which will be gone). Mouse stays
outside `InputEvent`, but the frontend no longer consults stale mode.

Resize is unchanged: `Event::Resize` → `EditorCommand::Resize` (`core.rs:189-192`).

**Implemented.** `ViewportId` is a monotonic hit-test revision derived from the projected
geometry-to-content mapping. Cursor-, status-, and pending-input-only renders retain it; document and
layout changes invalidate it. `MouseIntent` carries the revision for discrete viewport hit-tests and
preserves Alt intent for text clicks; the engine rejects stale revisions before consulting its
authoritative mode.

### 2.7 Runtime mappings do not expose armed states

Plugins register **sequences** targeting a command or a callback. The engine **synthesizes the prefix
nodes** and gives every pending sequence an **unconditional `Esc` cancellation path**.

`InputAction`, the `Arm*` actions, and the concrete pending states (`NormalPending`, `VisualPending`)
remain **private to the engine**. A plugin cannot construct an armed state, and therefore cannot
create a mode the user is unable to escape — which is what `PaneResize`'s unbounded self-re-arming
(`dispatch.rs:343-347`) would otherwise invite.

## 3. What moves

**Into `libaryon`:** `InputState` and the pending machine; `last_char_search` (§1.1 — `;` and `,` break
without it); keymap tables and `resolve_*`; the ex-command registry and dispatch; prompt buffers,
cursor, and history; `visual_command_range`; the `is_file_browser_view()` context selection
(`core.rs:265`), which becomes a plain engine-state read.

**Stays in `baryon`:** crossterm → `InputEvent` translation; mouse hit-testing and geometry; resize;
rendering and cursor style; terminal lifecycle; the tick, spinner, and reload probe.

**Becomes engine-internal, and gets *more* correct:** the viewport-derived arithmetic in `dispatch.rs`
— scroll amount (`:250-266`), `ViewportLine` (`:268-292`), `EnterVisual`'s byte reads (`:486-491`),
`Esc`'s conditionals on `search_pattern` / `cursor_count` (`:507-515`). These compute from the *last
delivered* `Viewport` today; in the engine they read live state. That is a behavior change and a fix,
called out per-site rather than absorbed silently.

## 4. Prerequisite work

Separated by kind, because they are not the same kind of change and should not share a commit.

### 4.1 Bugs — fixed as explicit prior commits

1. **Modifier matching** (§1.3, §2.4). The full contract, not a patch to `KeyPattern::Char`.
2. **Operator-pending fallthrough** (`keymap.rs:630-638`). `d` then `j` does not cancel — it
   re-resolves `j` against `Ready` and moves the cursor. `d` then `Esc` falls through to
   `NormalAction::Esc` and emits `ClearVisualSelection` (+ conditionally `ClearSearch`,
   `CollapseCursors`) rather than cancelling (`dispatch.rs:506-524`).
   **Decision: cancel-and-swallow.** An unmatched key in operator-pending cancels the operator and is
   swallowed. `Esc` is a silent, pure cancellation that emits nothing.
3. **The operator register is hardcoded** (`dispatch.rs:75`):
   `NormalContext::Operator(op) => NormalPending::Operator { op, register: '"' }`. `"add` and `"acw`
   silently drop the register; Yank and Put honor it (`dispatch.rs:396,405`).

### 4.2 Register continuation — live behavior, ported

`RegisterContinuation::Yank` and `::Goto` look unreachable if only direct binding applicability is
considered: `binding_is_applicable` forbids `ArmRegisterCapture` in those contexts. They are reached
through unmatched-prefix fallthrough, however: the dispatcher resolves `"` against `Ready` while
retaining the old `Yank` or `Goto` pending state. Existing characterization tests cover `y"ay` and
`g"at`. Preserve the continuation states during the move; deleting them changes working input
semantics.

### 4.3 A behavior decision

**The search prompt gets the same cursor and editing model as `:`.** Today `search_buffer` is
append-only with no cursor (§1.6). Unifying is a behavior change, but it simplifies the prompt
abstraction being moved and removes an asymmetry that would otherwise be enshrined in the engine.

### 4.4 Explicitly out of scope

**`cc` is missing functionality, not a bug.** `OperatorApply` with `(Change, Line)` returns without
sending (`dispatch.rs:389`) and `c`+`c` is not bound. It is a feature; it does not belong in a
zero-behavior-change move.

## 5. The minimal job substrate

Both `PromptAction::Complete` and `HistoryStore` (§1.6) perform blocking I/O that cannot move onto the
engine thread. This spec needs a **minimal job substrate** — not the full plugin-job API of
`rune-scripting.md` §3.7:

- a **request id / generation**;
- an **engine completion message** (`InternalEvent::JobDone`, arriving on the ordered stream of §2.1);
- **cancellation, or stale-result rejection**;
- a **serialized history writer**;
- a **bounded path scan**.

**Async completion must capture the prompt generation plus a buffer/cursor snapshot.** The result is
applied only if that generation is still active: typing, cancelling the prompt, or requesting another
completion invalidates it.

**Directory results need entry and byte caps.** Moving an unbounded collection off-thread prevents
*latency*, not *resource exhaustion* — the same mistake as an unbounded document read, which Baryon
already declines.

**Implemented.** History appends enqueue onto one bounded, serialized writer; entries have an explicit
byte cap, and dropping the last store drains and joins the writer. Path completion has hard
directory-entry and candidate-byte caps and refuses to mutate the prompt from an incomplete set. Its
lazy latest-request worker returns `InternalEvent::PathCompletion` on the ordered engine stream. A
result applies only when request id, prompt generation, buffer, and cursor all still match. Delivery
is lossless while the engine runs and cancellation-aware during shutdown, so a full queue cannot
deadlock worker teardown.

## 6. Backpressure

Once the engine resolves keys, the frontend can outrun it. The drain loop (`core.rs:198`) currently
reads all available events before rendering.

**A bounded, lossless, ordered queue.** Key and paste events are **never dropped**. When the queue is
full, the frontend **stops polling terminal input** while continuing to render and service its timers,
resuming when capacity returns. Losing keystrokes is not an acceptable failure mode for an editor;
input latency under load is.

**Implemented.** Commands, input, and internal events share one bounded queue. The TUI uses one local
FIFO retry path for command and input adapters, so a full queue neither blocks terminal rendering nor
lets a later mouse/resize/timer request overtake an already-read key. Periodic engine requests are
deferred until capacity returns.

## 7. Non-goals

- Counts (`3dd`). They do not exist today (§1.1); adding them is new functionality.
- `timeoutlen` and ambiguous mappings (§2.5).
- Script-defined mappings. This spec makes them possible; the runtime registry overlay
  (`rune-scripting.md` §5.5) makes them real.
- Moving mouse hit-testing into the engine (§2.6 moves only the *interpretation*).
- `cc` (§4.4).
