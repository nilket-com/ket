# Specification: Rune Scripting

## Overview

Make Rune to Baryon what Lua is to NeoVim: an embedded, first-class extension language in which
configuration, keymaps, ex-commands, and plugins are written. The end state is a plugin runtime, not
a config format — a Rune plugin can define `:commands`, bind keys, react to editor events, read and
mutate buffers, and do so identically under the TUI, under a GUI embedder, and headless.

This document is an **issue inventory and a record of settled decisions**, not an implementation
plan. It exists so that the specs that follow it — each of which is implementable on its own — do not
relitigate the architecture. Nothing here is scheduled; §5 is a dependency ordering, not a roadmap.

The central finding is that **the language is not the problem**. Rune's compiler and VM are already
linked into `libaryon`. What blocks scripting is that Baryon's engine is a write-only command sink
whose state is unreachable, whose extension tables are `static` and live in the wrong crate, which
has no events to hook, and which has no stable identity for most of the objects a script would name.
Most of the work described here does not mention Rune.

## 1. Ground truth

### Rune is present, and only the parser is used

- `rune = { version = "0.14.1", default-features = false, features = ["std"] }` (`Cargo.toml:35`),
  a dependency of `libaryon` only.
- Exactly three `use rune::` lines exist in the workspace: `svp/rune_ast.rs:30-31` and
  `svp/rune_semantic.rs:14`. They import `rune::SourceId` and `rune::ast`. `svp/rune_parse.rs` is a
  hand-written lexer whose keyword list is manually mirrored from `rune::ast::Kind`
  (`rune_parse.rs:11,84`) — a drift hazard, noted and out of scope.
- `rune::Vm`, `rune::Context`, `rune::Module`, `rune::Diagnostics`, and `rune::prepare` appear
  nowhere. There is no script execution of any kind.
- **The compiler and VM are nonetheless already compiled in.** `build`, `compile::Context`,
  `module::Module`, `diagnostics`, and `runtime::{Unit, Value, Vm}` are ungated in rune's
  `lib.rs:207-260`. `default-features = false, features = ["std"]` differs from rune's default set by
  exactly one feature, `emit`, which only adds `codespan-reporting`-backed diagnostic rendering.
  Baryon pays the binary-size cost of the full compiler and interpreter today and uses the parser.
  **Executing scripts requires no new dependency and no new Cargo feature.**

### The engine is write-only and its state is unreachable

- `EditorCommand` has 113 variants (`engine/protocol.rs:102-289`), all plain data, all
  fire-and-forget. No variant carries a reply channel or a request id.
- Handlers return `CommandEffect { needs_render: bool }` (`engine/runtime.rs:42-51`). A bool is the
  entire handler return channel. There is no way for a handler to return a value or an error object.
- The single query-shaped variant, `QueryUiColor` (`protocol.rs:234`), does not reply. It sets
  `session.ui.status_message` (`runtime/system.rs:719`), which surfaces on the next frame. This is
  not a mechanism a script return value can be built on.
- There is exactly one egress: `tx_view: mpsc::Sender<Viewport>` (`engine/core.rs:211`), exposed as
  `EngineHandle::take_viewports` (`engine/host.rs:130`). `Viewport` (`uast/projection.rs:207-225`)
  is the entire outbound API surface.
- **`EditorSession` is a stack local** — `let mut session = EditorSession::new(...)` at
  `engine/runtime.rs:848`, inside `run_engine`. Never in an `Arc`, never behind a lock, never stored
  in a struct. Even `engine/test_support.rs:363` drives it through the channel. The only point in the
  process with synchronous access to editor state is on the engine thread while `EditorSession` is in
  scope.
- The run loop is `while let Ok(cmd) = rx_cmd.recv()` (`engine/runtime.rs:881-1560`) — blocking, one
  command at a time, render inline.

### There is nothing to hook

- No callbacks, observers, listeners, or events exist anywhere in `engine/`. Autocommands do not
  exist to be extended; they must be invented.
- The only dynamic-dispatch extension point in the engine is `LanguageProvider`
  (`engine/language.rs:56`), registered once at startup (`engine/host.rs:166-181`). Providers run on
  the semantic worker thread, see `&[u8]`, and never see `EditorSession`.
- Background workers signal the engine by re-injecting `EditorCommand::InternalRefresh`
  (`engine/host.rs:72-81`). This is a wake-up nudge with no payload, not an event bus.

### The extension tables are `static`, and they are in the wrong crate

- `COMMANDS: &[CommandSpec]` (`baryon/src/ui/command_registry.rs:124-426`) is a `static` slice.
  `type Handler = fn(&mut CommandCtx<'_>, Invocation<'_>)` (`:44`) is a bare fn pointer, which
  cannot close over a VM handle. Handlers deliberately do not receive `&mut Frontend` — the comment
  at `:13-15` states this is what "keeps the registry a `static` table of fn pointers."
- `KEYMAP: &[KeyGroup]` (`baryon/src/ui/keymap.rs:817-2623`) is a `static` slice of a closed
  algebraic `BindingSpec` (`:588`) over closed per-mode action enums (`NormalAction:513` et al.),
  with no script escape variant. **`resolve_normal`/`resolve_visual`/… return
  `Option<&'static BindingSpec>`** (`:2625-2740`) — the resolver signature itself hard-codes that
  bindings outlive the program. `plans/keymap.md` lists runtime remapping as an explicit non-goal.
- `BOOL_OPTIONS` / `VALUE_OPTIONS` (`baryon/src/ui/option_registry.rs:48-121,123-156`) are `static`,
  and every option's effect is a variant of the closed `EditorCommand` enum.
- All of the above are `pub(super)` inside `baryon/src/ui` — a script layer in another crate cannot
  name the types.
- Help, completion, and dispatch all derive from these tables, which is the property worth
  preserving: a new row is automatically dispatchable, completable, and documented.

### Mode, and everything attached to it, is frontend state

- `EditorMode` is owned by `InputState` in `baryon/src/ui/keymap.rs:9`, derived at `:27`. The engine
  holds only an advisory `ui.mode_override: Option<EditorMode>` (`engine/session.rs:516`) used to
  *push* mode changes outward. The engine is not a modal editor; it is a command sink.
- Mode does not travel alone. The command and search buffers, prompt cursor, completion state, and
  history orchestration live in the frontend beside it (`baryon/src/ui/core.rs:30`,
  `baryon/src/ui/command.rs:50`).
- `InputState` (`keymap.rs:9`) is `Normal(NormalPending) | Visual{anchor,kind,pending} | Insert |
  Command | Search | Confirm`. **There are no counts** — `NormalPending::Ready { register }` is the
  whole of it. Prefixes such as `s`, `d`, and `g` are explicit states that wait indefinitely. There is
  no ambiguity in the current keymap and therefore no timeout anywhere.
- Paste behavior is mode-dependent (`baryon/src/ui/command.rs:13`).
- Consequence, independent of scripting: **any second frontend must reimplement modal editing from
  scratch.** The `libaryon`/`baryon` split is not as clean as it looks.

### The object model is thinner than it looks

- Document state is `DocumentSession.root_id` plus `UndoLedger.current_state_id`.
- **Ordinary sparse edits mutate the existing root in place and write the same rid back**
  (`engine/document/edit.rs:464`, where `let rid = (*root_id).ok_or(..)`, and `:522`, where
  `*root_id = Some(rid)`). Semantic grafting likewise mutates that root in place
  (`engine/semantic.rs:82`). Loads, reloads, generation swaps, and some substitutes *can* replace it.
- So a root pointer is not invalidated by every keystroke. But it is still not an identity: it is
  replaced by lifecycle events, and it names a UAST root, not a document.
- **`PaneId` is stable. There is no tab id and no document id at all.**
- **`split_focused_pane` creates an empty pane rather than sharing the focused document**
  (`engine/session.rs:1758`, `EditorPane::new_empty(..)`). Baryon has no notion of two views onto one
  buffer — which is the thing NeoVim's window/buffer split *is*.
- An identity spec must therefore distinguish four things that are currently conflated or absent:
  **buffer/content identity**, **tab or document-view identity**, **pane identity**, and **the current
  UAST root and revision**.

### Other constraints

- `panic = "abort"` in release (`Cargo.toml`, `[profile.release]`). A panic in a host function called
  from script kills the process with no unwind. **Every host function must be total.**
- Rune supports async. **Baryon has no executor and no job abstraction** — no `tokio`, no `async fn`,
  no task layer anywhere in the workspace; `std::thread` + `std::sync::mpsc` only. That is the
  constraint: async host functions and background work have nothing to run on until §3.7 answers it.
  A `!Send` VM is fine — the engine thread owns everything.
- `crates/{baryon,libaryon}/tests/architecture_test.rs` enforce that every `mod.rs` is a manifest
  only. A new `engine/script/` module must put all code in non-`mod.rs` files.
- The settings path is resolved per platform — XDG, macOS, and Windows
  (`engine/settings.rs:312-368`). `init.rn` lives **beside the resolved settings path**, not always
  under `~/.config`.

## 2. Settled decisions

1. **Full plugin runtime.** Plugins are engine features. They work headless; a GUI embedder gets them
   for free. This is the NeoVim analogy taken literally.

2. **Rune executes synchronously on the engine thread.** The VM runs on the engine thread while
   `EditorSession` is in scope — during a command, at startup, and when draining deferred events, not
   only inside the `match cmd` arm. This is the only place in the process with synchronous state
   access, so it is the only place a `let line = editor.cursor_line()` can be served. A slow script
   blocks the editor, as it does in NeoVim.

   **Confirmed by spike** (`plans/rune-host-access-spike.md`), with no unsafe code and no engine
   refactor. A `'static` owning facade — `EditorHost { session: EditorSession, host_budget }`,
   exposed to Rune as `baryon::Editor` — replaces `EditorSession` as the engine's stack local and is
   passed as `&mut` into each VM call and each stored-callback invocation. Nothing moves per callback,
   `EditorSession` stays private, and rune invalidates the reference when the call returns. The API
   rule this forces:

   > **A handler receives the editor as a parameter. It never captures it.**

   Capturing is refused by rune itself, at call time. A script cannot smuggle the handle out: five
   retention paths (closure capture, direct return, `Vec`, object, nested tuple) were tested in child
   processes and none aborted — escaped values are invalidated through nested containers and every
   later use is a clean `VmError`.

3. **`settings.toml` remains persisted state; `init.rn` is executable configuration layered over it.**
   `:set` keeps writing TOML. `init.rn` runs after settings load and may override. Both surfaces stay
   honest about what they are: one is machine-written state, the other is code.

4. **Rune callbacks must never re-enter the VM.** A host function that mutates the session must not
   synchronously fire a hook that calls back into script — that re-enters with the host facade already
   borrowed. Host functions *enqueue* events; the engine drains them. This is structural, not a
   runtime guard like NeoVim's `textlock`.

   Precisely: **no re-entrant path exists through the proposed host boundary**, because host functions
   receive only `&mut EditorHost` and never a VM handle. That is a property of the boundary, not a law
   of the universe — it holds only so long as nothing hands a host function the means to call back in.
   The deferred-event rule is what keeps it true, and it stays structural for that reason.

5. **Deferred events drain after the current callback returns and before rendering or blocking on the
   next command.** Draining only at the top of the receive loop would leave events pending
   indefinitely when no further command arrives. Draining before `recv()` is sufficient; this does
   **not** by itself require a deadline-aware loop.

6. **The engine accepts portable input alongside semantic `EditorCommand`, owns modal state and
   runtime mappings, and reports the resulting mode through `Viewport`.** The contract is:

   > Portable input resolves to an **internal input action**. That action may change modal state, emit
   > zero or more `EditorCommand`s, or invoke a script callback. All actual editor mutations continue
   > to flow through the existing command handlers.

   A key does not map to one command: some keys only transition input state, others edit prompts or
   invoke completion, others emit several commands. Portable input is
   **`InputEvent::{Key, Paste}`**, not `KeyEvent` alone — paste behavior is mode-dependent, so once
   the engine owns mode, paste must cross the boundary too.

   Programmatic and headless clients keep submitting `EditorCommand` directly and thereby bypass modal
   input — and therefore bypass script-defined mappings, which is correct behavior, not a gap. Moving
   modal input into `libaryon` is advisable on its own merits (§1, "Mode … is frontend state"); it is
   not made logically unavoidable by scripting.

7. **Script output does not need rune's `capture-io` or `disable-io` features.**
   `Context::with_config(false)` already omits stdout/stderr functions — the `stdio` flag gates
   `modules::io::module(stdio)` (`rune/src/compile/context.rs:366`). Baryon installs its own `print`,
   `println`, and `dbg` writing to an editor message log. This matters: rune's `println!` would
   otherwise write to real stdout and corrupt the ratatui screen.

8. **The buffer API is bounded by construction, bytes-first.**
   - No implicit `buf.text()`, no unbounded `buf.lines()`.
   - Reads require a range and enforce a maximum returned size.
   - The primitive API is bytes-first; UTF-8 conversion is explicit and fallible.
   - A line accessor must also accept a byte cap or expose chunks — **a single line can itself be
     tens of gigabytes.**
   - Iterators page lazily. We do not claim they cannot be collected; a script can always concatenate
     results manually. Budgets are the real enforcement. The API shape is guidance, not a guarantee
     the runtime provides.
   - `buf.snapshot(max_bytes)` is the sanctioned convenience: the caller states the bound, and the
     call fails *before reading* if the document exceeds it. No plugin manifest privilege is needed.

   This preserves the huge-file guarantees — 80–200 GiB address spaces, whole-document operations
   declined above 256 MiB — while keeping small-buffer plugins pleasant to write.

9. **Resource enforcement is four mechanisms, not one.** `rune::runtime::budget`
   (`rune/src/runtime/budget.rs:86`) bounds **VM instructions only**. The record must keep these
   distinct:
   - the VM instruction budget — bounds runaway *script* loops, and does so correctly,
   - rune's allocation limit,
   - per-operation Baryon caps (the `max_bytes` of §2.8),
   - background job and subprocess timeouts.

   **A host function cannot be interrupted; it can only decline in advance.** Rune documents that
   native functions must cooperate with the budget via `budget::acquire()`, but in 0.14.1 they
   *cannot*: the VM calls `budget::acquire()` at the top of its own instruction loop (`vm.rs:3028`),
   moving the budget out of the thread-local into a VM-local guard, so a native function that calls
   `acquire()` gets `usize::MAX` and never yields. Measured: a host function running 10⁹ iterations
   under a 100,000-instruction budget ran to completion.

   Two distinct rules follow. **Hard per-operation caps are mandatory, not defensive**: every
   operation whose work scales with script-controlled input must have an intrinsic cap and preflight
   it *before mutating*. This is not a blanket requirement on every host function — constant-time
   operations need no artificial limit. Separately, **a Baryon-owned host budget** carried on the
   `EditorHost` facade (§2.2) charges a per-callback quota across native calls, bounding a script that
   issues a million cheap-but-not-free host calls. Rune-budget cooperation is unmeetable; host-budget
   cooperation is not. Neither can interrupt a native operation already in flight, which is why the
   hard caps remain mandatory alongside the quota.

   It follows that **filesystem, subprocess, and network work cannot run synchronously on the engine
   thread.** A full plugin runtime therefore needs background jobs and completion callbacks; see §3.

## 3. Open questions

1. ~~**How does a host function reach the session?**~~ **RESOLVED** by
   `plans/rune-host-access-spike.md`. Direct `&mut` as a per-invocation argument, zero unsafe, no
   engine refactor. See §2.2. The premise that ruled it out — that a retained borrow aborts under
   `panic = "abort"` — was false. The scoped-ambient-host, generation-token, snapshot/effect-queue,
   and state-ownership designs are all unnecessary and are dropped.

2. **Byte and text semantics.** What a range means, what a line means when a line is 40 GiB, where
   UTF-8 validation happens and what it costs, and what a script sees at an invalid boundary.

3. **Stable object identity.** The four-way distinction of §1 — buffer/content, tab/document-view,
   pane, and UAST root/revision. When each is minted; what survives a save, a reload, a generation
   swap, a substitute, a diff split, a tab close. Whether two panes can share one buffer at all
   (`session.rs:1758` says they currently cannot), which decides whether Baryon has NeoVim's
   window/buffer model or something else.

4. **Buffer, tab, and pane lifetime, and stale-handle behavior.** What a script holding a handle to a
   closed tab or a replaced root observes. An error, a null, a resurrection.

5. **Mapping semantics.** Collision and precedence between static defaults and runtime mappings;
   recursion (`map` vs `noremap`); and whether **ambiguous sequences are permitted at all**. Today
   they are not: prefixes are explicit states that wait indefinitely, so no timeout exists anywhere.
   Permitting a runtime mapping of both `s` and `sh` would introduce ambiguity and *then* require a
   timeout and a deadline-aware run loop. **That is a policy choice for the input-contract spec, not a
   settled consequence.**

6. **The pending-input state machine.** Operator-pending (`d` awaiting a motion), the `s` pane prefix,
   register prefixes (`"a`), and `f`/`t` awaiting a target live in the frontend's `InputState` today.
   The engine cannot resolve `diw` from three unrelated key events, so this moves wholesale — and it
   drags the command/search buffers, prompt cursor, completion, visual command ranges, and history
   orchestration with it (§1).

7. **Background jobs.** Cancellation, delivery of completion events, and the ordering guarantees
   between a job completion and the deferred event queue. Required by §2.9; a plugin that shells out
   to `git` or `ripgrep` is the motivating case.

8. **Plugin ownership and cleanup.** Who owns a registered mapping, command, or event handler; what
   reload and unload remove; what happens to a handler whose plugin is gone.

9. **Startup policy for embedders.** The TUI may load the default `init.rn`. A headless library host
   should probably opt in explicitly through `EngineConfig` rather than silently executing user code.

10. **Event taxonomy.** Which events exist, what payload each carries, ordering and coalescing
    guarantees, and which are cheap enough to fire per keystroke or per render. Per-token or
    per-render script callbacks are almost certainly too slow; that boundary needs drawing before the
    API is published rather than after.

11. **Sandboxing.** What of the outside world a script gets — filesystem, process spawn, environment.
    Half of NeoVim's useful plugins shell out. Deferred, but the plugin manifest should not be
    designed in a way that forecloses it.

12. **Error surfacing and callback transactionality.** Where a compile diagnostic, a runtime
    `VmError`, a budget exhaustion, and a failed `init.rn` each appear — status line, message log,
    diagnostics gutter — and what happens to the editor when `init.rn` does not compile.

    On transactionality, the working position is: **callbacks are not transactional.** Host operations
    that already completed remain applied after a later error or budget exhaustion; there is no
    rollback. What is guaranteed instead is that **every individual host operation preserves editor
    invariants**, so a half-finished script leaves the editor consistent, merely not idempotent. The
    alternative — journaling every callback as an undoable transaction — is not worth its cost, and a
    script that wants atomicity can build it from an undo group.

## 4. Non-goals

- Vimscript compatibility, or any compatibility layer with NeoVim's Lua API. Baryon's buffer semantics
  differ at the root (§2.8); pretending otherwise would produce plugins that silently only work on
  small files.
- A plugin package manager. Discovery and `runtimepath` are late; distribution is not in scope.
- Replacing `settings.toml` (§2.3).
- Preserving unknown keys in `settings.toml`. This is only needed if plugins persist configuration
  there, and `init.rn` is that surface. Not a scripting prerequisite.
- Rewriting `svp/rune_parse.rs` to use rune's own lexer. Noted as a drift hazard in §1; unrelated.

## 5. Issue inventory

A dependency ordering, not a schedule. Each item is a spec of its own.

**Falsify first**

1. ~~**Host access prototype.**~~ **DONE** — `plans/rune-host-access-spike.md`. §2.2 confirmed with
   zero unsafe. Two findings changed the plan: host functions cannot cooperate with rune's
   instruction budget (§2.9), and callbacks take the editor as a parameter rather than capturing it.

**Enabling work — no scripting**

2. ~~**The input contract.**~~ **DONE** — `plans/input-contract.md`. `InputEvent::{Key, Paste}` ingress; the internal
   input-action boundary of §2.6; mode, the pending-input state machine, prompt and completion
   ownership, and the keymap move into `libaryon`; input state projected through `Viewport`;
   `EditorCommand` submission preserved for headless embedders. Resolves §3.5 by **forbidding
   ambiguous mappings at registration**, so the run loop needs no deadline.

   Three pieces, in order: **(a, complete)** the ordered `EngineRequest` stream and the extracted
   command dispatcher; **(b, complete)** a **minimal completion/history job
   substrate** — request generation,
   engine completion message, stale-result rejection, a serialized history writer, a bounded path scan; then
   **(c, complete)** the modal-input migration itself. (b) is internal prerequisite
   work, not the plugin-facing job system: prompt completion and `HistoryStore` both perform blocking
   I/O that cannot move onto the engine thread, so the substrate must exist before the prompt does.
3. **Stable object identity.** The four-way model of §3.3, and the lifetime and stale-handle rules of
   §3.4. Prerequisite for naming anything from script.
4. **Byte and text semantics.** §3.2 and the bounded read model of §2.8, specified independently of
   Rune and exercised through the existing engine API.
5. **Runtime registry overlay** (`plans/runtime-registry-overlay.md`). The `static` command, keymap,
   and option tables become defaults-plus-overlay. `resolve_*` stops returning `&'static`. Actions
   gain a script escape variant. Help and completion chain the overlay rather than being rewritten.
   *The spec must state the required capability — dynamic registration of handlers owned by something
   other than the binary — and not prejudge the representation; a boxed closure and a callback id
   owned by the Rune host are both open.*
6. **Engine event bus.** The fire points, the deferred queue, and the drain point of §2.5 — with zero
   subscribers.
7. **Background jobs — the plugin-facing system.** §3.7. Expands the minimal substrate from item 2(b)
   with plugin-facing jobs, subprocess and network support, ownership, cancellation, and callback
   delivery. Independent of Rune; the engine needs an answer regardless. Item 2(b) is the internal
   floor this builds on, not a competing design.

**Rune host**

8. **VM lifecycle.** The `EditorHost` facade of §2.2, `Context` construction, `Module` registration,
   the enforcement mechanisms of §2.9, message log, diagnostics, `:source`, `init.rn` at startup, and
   the embedder opt-in of §3.9.

   Must also carry **acceptance tests for the guarded-borrow behavior the host design rests on** —
   stored-callback invocation with a guarded `&mut`, and invalidation of an escaped handle through
   nested containers. The spike that established these was thrown away, and `rune = "0.14.1"` permits
   Cargo to resolve a later `0.14.x`. These are dependency-upgrade tripwires: if either regresses in a
   point release, the host design fails silently.

**The API**

9. **The `baryon::*` Rune module.** Buffers, cursors, panes, options, keymaps, commands, autocmds.

**Ecosystem**

10. **Plugin discovery and lifecycle.** `runtimepath`, load order, lazy loading, manifests, and the
    ownership/cleanup rules of §3.8.
