# Specification: Data-Driven Keymap

## Overview

Replace the hand-written modal key dispatch (`src/ui/normal.rs`, `visual.rs`, `insert.rs`, and the prompt/confirm handlers in `command.rs`) with a static table of documented bindings plus a single executor, following the `command_registry.rs` precedent. Goals, in priority order:

1. **Invalid input states unrepresentable; binding vocabulary narrowed** — pending key state is indexed by mode, so Normal-only pendings cannot exist in Insert. Mode-specific binding variants and closed action enums make cross-mode action/context pairings impossible; structural validation covers the remaining within-mode applicability rules. No raw `KeyCode`/`EditorCommand` appears in the table.
2. **Docs derived, never parallel** — `:help keys` / `--help keys` labels are computed by traversing the binding graph. Only prose summaries are human-authored. `src/ui/keys_catalog.rs` and its banner comments are deleted.
3. **Zero behavior change**, with exactly one explicitly-committed behavior fix (mode-override pending reset, §4.2) landed *before* the representation refactor so the refactor preserves its baseline honestly. All other semantics — including modifier handling and register/prefix stacking — are modeled exactly.

Non-goals: key remapping (a static table that mixes help layout with default bindings would fight a runtime keymap; do not design for it here), mouse dispatch, paste handling.

## 1. Ground truth the design must preserve

Verified against the current tree; characterization tests (§5.1) pin all of it.

- **Modifier semantics**: the only Ctrl-guarded arms in the codebase are Ctrl-`z`/`u`/`d`/`b`/`f`/`w`/`v`/`r`, always ordered before unguarded `Char` arms that ignore modifiers entirely (SHIFT/ALT are never inspected; `gT` arrives as SHIFT'd `Char('T')`; `"` + Ctrl-z names register `z`; Ctrl-y answers Yes in Confirm; Ctrl-h moves left in Normal/Visual and collapses in the browser; `d` + Ctrl-`w` deletes a word motion).
- **Register/prefix stacking**: the `"` arm (normal.rs:453-456) sets register-capture *without clearing any prefix*, and the Yank guard (normal.rs:362-375) and Goto guard fall through unmatched keys to the base match without clearing. Hence `y"ay` yanks line into register `a`, `g"ap` puts from `a`, and `g"at` still switches tabs. The ordinary buffer `g` arm explicitly clears a previously captured register, so `"agp` puts from the unnamed register. The file-browser `g` arm does not clear it; this is observable if focus moves to a buffer before the next key. Operator pending does *not* stack: `d` then `"` clears the operator first (`clear_operator_pending`), and the `d`/`c` arms call `clear_prefixes()` (which clears a captured register), so `"add` deletes into the **unnamed** register while `"ayy` and `"ap` honor `a`. `p` reads the register before the main match.
- **Fallthrough vs swallow**: operator/yank/goto guards fall through to the base match on unmatched keys (base action then decides what survives — see stacking above); `z`/`]`/pane-prefix guards swallow-and-clear; pane-resize swallows and stays sticky (only Esc leaves); every Visual guard swallows; Insert/Confirm/prompts swallow unknown keys.
- **Char search**: `dispatch_find_char` (core.rs:304-315) is the only writer of `last_char_search`; `;` repeats through it (re-records); `,` sends the flipped `FindChar` directly *without* recording.
- **Mode plumbing**: `A`/`o`/`O` and the Change family enter Insert via engine `mode_override` (engine/runtime/navigation.rs:399, edit.rs:823-1408), consumed at core.rs:159-164. Mode changes are effectful (`apply_cursor_style` performs terminal I/O). The event loop pre-sets `status_message = None; needs_redraw = true` for every keypress (core.rs:182-184).
- **External readers**: draw.rs:977-979/1187-1191 renders the "NOR s"/"NOR sr"/"NOR g" status suffix from prefix fields; mouse.rs reads neither mode-pending state nor prefixes; `handle_paste` matches on the mode.

## 2. Input state: mode-indexed ADT

Replaces `current_mode` plus the eleven pending fields on `Frontend` (core.rs:42-62). `EditorMode` remains the engine-protocol type; `Frontend::editor_mode()` derives it, and engine `mode_override` converts through `InputState::from_editor_mode` (pending resets at that boundary — the §4.2 behavior fix makes that today's behavior first).

```rust
pub(super) enum InputState {
	Normal(NormalPending),
	Visual { anchor: DocByte, kind: VisualKind, pending: VisualPending },
	Insert,
	Command,
	Search,
	Confirm,
}

pub(super) enum Operator { Delete, Change }

pub(super) enum CharSearchKind {
	ForwardInclusive, BackwardInclusive, ForwardTill, BackwardTill,
}

pub(super) enum NormalPending {
	Ready { register: Option<char> },                 // register captured by `"x`
	RegisterCapture { continuation: RegisterContinuation },
	FindChar(CharSearchKind),
	Operator { op: Operator, register: char },        // register resolved to '"' at arm time —
	OperatorInner { op: Operator, register: char },   //   the `"add`-uses-unnamed quirk, typed
	OperatorAround { op: Operator, register: char },
	Replace,
	Yank { register: Option<char> },                  // `"` stacks: y`"`ay works (see below)
	Goto { register: Option<char> },                  // `"` stacks: g`"`ap works
	Fold, Bracket, Pane, PaneResize,
}

/// Which pending state a captured register returns to. `"` preserves the
/// Yank and Goto prefixes today; everything else reaches capture only from
/// Ready (operators clear themselves before falling through).
pub(super) enum RegisterContinuation { Ready, Yank, Goto }

pub(super) enum VisualPending {
	Ready,
	FindChar(CharSearchKind),
	Inner, Around, Goto,
}
```

Capture of char `c` returns to `Ready { register: Some(c) }`, `Yank { register: Some(c) }`, or `Goto { register: Some(c) }` per the continuation. A shared helper `NormalPending::captured_register(&self) -> Option<char>` feeds `p`/`yy`/etc. `last_char_search` stays a separate field (sticky across states by design). `Frontend` keeps: `state: InputState`, `last_char_search`.

## 3. The binding table

### 3.1 Patterns

```rust
pub(super) enum SpecialKey { Esc, Enter, Tab, Backspace, Delete, Up, Down, Left, Right, Home, End, PageUp, PageDown }

pub(super) enum KeyPattern {
	Ctrl(char),           // Char key with CONTROL held
	Special(SpecialKey),  // non-char keys; modifiers ignored
	Char(char),           // Char key; modifiers IGNORED (matches today's unguarded arms)
	AnyChar,              // any Char key, any modifiers (capture semantics)
}
```

Resolution precedence within a context: `Ctrl` > `Special` > `Char` > `AnyChar`. This reproduces the guard-before-arm ordering exactly; there is no modifier behavior change of any kind.

### 3.2 Mode-specific bindings and actions

A single `(ContextKind, Action)` product would let `Confirm + ArmOperator` or `Insert + VisualApply` typecheck. Bindings are therefore split by mode, each with its own closed context and action enums:

```rust
pub(super) enum BindingSpec {
	Normal { context: NormalContext, pattern: KeyPattern, action: NormalAction },
	Visual { context: VisualContext, pattern: KeyPattern, action: VisualAction },
	Insert { pattern: KeyPattern, action: InsertAction },
	Confirm { pattern: KeyPattern, action: ConfirmKeyAction },
	Prompt { kind: PromptKind, pattern: KeyPattern, action: PromptAction },
}

pub(super) enum NormalContext {
	Ready, FileBrowser,
	RegisterCapture, FindChar(CharSearchKind), Replace,
	Operator(Operator), OperatorInner(Operator), OperatorAround(Operator),
	Yank, Goto, Fold, Bracket, Pane, PaneResize,
}
pub(super) enum VisualContext { Ready, FindChar(CharSearchKind), Inner, Around, Goto }
pub(super) enum PromptKind { Command, Search }

/// Tagged sums used by generic resolution, execution, and graph traversal;
/// they preserve the mode-specific context/action types above.
pub(super) enum Context {
	Normal(NormalContext), Visual(VisualContext), Insert, Confirm, Prompt(PromptKind),
}
pub(super) enum ActionRef<'a> {
	Normal(&'a NormalAction), Visual(&'a VisualAction), Insert(&'a InsertAction),
	Confirm(&'a ConfirmKeyAction), Prompt(&'a PromptAction),
}
```

Action enums are closed and semantic (no `EditorCommand` payloads in the table); shared sub-enums (`MotionOp`, `ScrollOp`, `WordMotion`) are reused across modes. Sketch:

```rust
pub(super) enum NormalAction {
	Quit,
	Motion(MotionOp), Scroll(ScrollOp),
	ViewportLine { pos: ViewportPos, first_non_blank: bool },   // H/M/L (browser: false)
	Edit(EditOp),          // Undo, Redo, DeleteChar, BackspaceChar, DeleteToLineEnd, JoinLine,
	                       // ToggleCase, OpenBelow, OpenAbove, AppendAtLineEnd (engine flips mode)
	Search(SearchOp), Fold(FoldOp), Pane(PaneOp), Tab(TabOp), Browser(BrowserOp),
	OperatorApply(OperatorTarget),  // Line | Word(WordMotion) | InnerWord | AroundFunction
	YankApply(YankTarget),          // Line | Word(WordMotion)
	Put,
	ArmOperator(Operator), ArmYank, ArmGoto, ArmFold, ArmBracket, ArmPane, ArmPaneResize,
	ArmReplace, ArmRegisterCapture, ArmFind(CharSearchKind),
	ArmOperatorObject(TextObject),  // i / a while an operator is pending
	CaptureRegister, CaptureFindChar, CaptureReplaceChar,
	RepeatFind, RepeatFindReversed,
	EnterInsert(InsertEntry),       // AtCursor | FirstNonBlank | AfterCursor  (i/I/a)
	EnterVisual(VisualKind), EnterCommandPrompt, EnterSearchPrompt,
	Esc,                            // ClearVisualSelection + conditional ClearSearch
	ReadOnlyStatus,                 // browser editing keys
	ClearPending,                   // Ctrl-w
}
// VisualAction: Quit, Motion, Scroll, Apply(Yank|Delete|Change), Esc, CommandPrompt (range
//   snapshot + "'<,'>"), ArmInner/ArmAround/ArmGoto/ArmFind, CaptureFindChar, RepeatFind{,Reversed}, GotoTop
// InsertAction: Quit, Leave, InsertChar, Newline, TabChar, Backspace, Delete, Motion(MotionOp)
// ConfirmKeyAction: Quit, Answer(protocol::ConfirmAction)
// PromptAction: Quit, InsertChar, Enter, Cancel, HistoryUp, HistoryDown, Backspace, Complete
```

### 3.3 Groups own the bindings

```rust
pub(super) struct KeyGroup {
	pub category: Category,          // closed enum (Motion, Editing, YankPut, Panes, ...)
	pub summary: &'static str,       // the ONLY human-authored doc text
	pub bindings: &'static [BindingSpec],
}
pub(super) static KEYMAP: &[KeyGroup] = &[ /* every key in the editor */ ];
```

A binding exists only inside a group; the group's rendered key labels come from the bindings (§6), so a label that disagrees with dispatch is unrepresentable. Plain struct literals, no macro to start (the relationships are already typed); introduce a macro later only if literal noise warrants it.

## 4. Dispatch and execution

### 4.1 The executor is the single transition authority

`next_state(action, state)` cannot be total over runtime facts (captured characters, viewport presence for Visual entry, prompt-buffer emptiness for Backspace, `:q` control flow). One executor consumes the old state and returns the resolved transition:

```rust
pub(super) enum DispatchControl { Continue, Quit }
pub(super) struct ExecutionOutcome { pub next: InputState, pub control: DispatchControl }

impl<B: Backend + io::Write> Frontend<B> {
	fn execute(&mut self, old: InputState, input: KeyInput, binding: &BindingSpec) -> ExecutionOutcome;
}
```

- All effects live in `execute`: command sends, viewport reads (scroll magnitudes, H/M/L, Visual anchors — `EnterVisual` stays in Normal when no viewport exists), `dispatch_find_char` vs the no-record `,` repeat, mode transitions paired with `apply_cursor_style`, `visual_command_range` snapshot for Visual `:`, and prompt delegation. Prompt helpers in command.rs (completion, history, `dispatch_ex_command` glue) become named methods returning a small typed outcome (`Stay | ToNormal | Quit`) that `execute` maps into `ExecutionOutcome` — preserving the `:q` early-return and the Backspace-on-empty-exits behavior.
- Register semantics are per-variant in `execute`: `ArmOperator` produces `Operator { op, register: '"' }` (the quirk, explicit); `ArmYank` carries `captured_register()` through. `ArmGoto` inspects the matched binding context: from `NormalContext::Ready` it produces `Goto { register: None }`, matching the buffer `g` arm's explicit clear; from `NormalContext::FileBrowser` it carries the captured register, matching the browser arm. `ArmRegisterCapture` from an already-pending Goto can also produce `Goto { register: Some(c) }`. Capture maps the *old* state to its continuation (`Ready`→`Ready`, `Yank`→`Yank`, `Goto`→`Goto`); ordinary actions return to `Ready { register: None }` (today's `clear_prefixes`); pane-resize actions return `PaneResize` (stickiness is part of the action's meaning). No `keep_register`/`retains_pending` booleans anywhere.
- `BindingSpec::context() -> Context` and `BindingSpec::action() -> ActionRef<'_>` expose tagged views without erasing the mode-specific types. The transition targets of arm actions are provided by one shared pure function, `arm_target(binding: &BindingSpec) -> Option<Context>`, used by both `execute` and the doc traversal (§6) — one source of truth for the graph's edges.

### 4.2 Explicit behavior fix, before the refactor

Today an armed prefix survives an engine `mode_override` round-trip (views drain at core.rs:152 before multiple queued keys process at :179, so `A` `d` queued together arm an operator that survives into and out of Insert). Resetting pending state on an **actual mode change** is the desired behavior and lands as its own commit **before** the representation change: extract `apply_mode_override(mode)` from the run loop, retain today's `current_mode != mode` guard, and call `clear_prefixes()` only inside that branch. Test both cases: a changed mode clears pending; a same-mode override preserves it. The InputState refactor then preserves this new baseline exactly, and "zero behavior change" stays true for every refactor commit.

### 4.3 Dispatch loop

```
fn dispatch_key(&mut self, code, mods, should_quit) -> bool:
    ctx = context_of(&self.state, self.is_file_browser_view())
    binding = resolve(ctx, code, mods)             // precedence: Ctrl > Special > Char > AnyChar
    if binding is None:
        match on_unmatched(ctx):
            FallThrough  -> resolve in base context (Ready, or FileBrowser when browsing)
                            WITHOUT touching state; if still unmatched, apply the base
                            context's swallow policy. The executed base action receives the
                            ORIGINATING state, so `"` preserves Yank/Goto while motions clear.
            SwallowClear -> state = mode's Ready; return false
            SwallowKeep  -> return false
    outcome = self.execute(old_state, input, binding)
    self.state = outcome.next
    return outcome.control == Quit  (also sets *should_quit)
```

`context_of` mirrors today's guard precedence (RegisterCapture > FindChar > PaneResize > Pane > Goto > browser reroute > operator states > Yank/Fold/Bracket > base). `on_unmatched` is a total match: FallThrough for Operator*/Yank/Goto; SwallowClear for Fold/Bracket/Pane, capture contexts on non-matching keys, and Visual pendings; SwallowKeep for PaneResize, Visual Ready, Insert, Confirm, prompts.

## 5. Testing

### 5.1 Characterization first, through a stable wrapper

Commit 1 adds `Frontend::dispatch_key` as a thin wrapper (matches the mode, delegates to today's handlers) plus an `editor_mode()` accessor, and writes every characterization test against them — never against `current_mode`, prefix fields, or the soon-to-die handlers, so the tests survive all later commits unedited. State is probed only via observable behavior (emitted commands, `editor_mode()`, follow-up keypresses).

Coverage (all against current behavior): `yy`; `"ayy`→`a`; **`y"ay`→`a`; `g"ap`→`a`; `g"at`→DocTabNext** (the stacking sequences); **`"agp`→`"`** (buffer `g` clears the captured register); `"add`→`"` (operator quirk); `p`/`"+p`; y-then-`j` and d-then-`l` fallthroughs; `d a` then `w` fallthrough; `diw`/`ciw`/`daf`/`caf`; `r`/`f` then Esc; folds `za/zc/zo/zM/zR`; `]m` and `]x` swallowed; `u`/Ctrl-r; `gg`; `D`; `v`/`V`/Ctrl-v anchors and kinds; `:` and `/` entry; Esc-in-pane-prefix; Ctrl-w; pane sweep `ss/sv/sh/sj/sk/sl/sc/sr`+resize+Esc; browser sweep (navigation, H/M/L without FirstNonWhitespace, read-only keys, Esc, `g` prefixes, and captured-register preservation across browser `g` followed by a focus change); Visual sweep (`y`/`d`/`c` emissions + return to Normal, motions staying Visual, Ctrl scrolls, `gg`, `af`, swallowed unmatched); all of Insert (including Ctrl-modified chars inserting); Confirm `y/n/a/q`/Esc **and Ctrl-y answering Yes**; modifier pins (Ctrl-h moves left in Normal, `d`+Ctrl-w deletes a word motion); prompts (Backspace-on-empty exits, Esc cancels + Command-Esc clears the visual range, history recall).

### 5.2 Structural tests, staged as they become true

Per-mode checks land with each migration commit (patterns unique per context and precedence class; at most one `AnyChar` per context and only in capture contexts; groups non-empty; every binding passes a total mode-specific context/action applicability predicate used by the executor). Global checks land in the final migration commit (every context populated and reachable from a root through `arm_target` edges; the arm graph is acyclic; a quit binding exists in every base context). The old substring "core keys" test is **replaced** by semantic reachability assertions — e.g. "from Normal Ready there is a path to `OperatorApply(InnerWord)` under `Operator(Delete)`" — not string matching on rendered output.

## 6. Derived help

`:help keys` / `--help keys` labels are computed by traversing the binding graph. Roots are tagged help sections plus their base contexts: Normal/Ready, FileBrowser/FileBrowser, Visual/Ready, Insert, Command prompt, Search prompt, and Confirm. Arm bindings (via the shared `arm_target`) are edges; non-arm bindings terminate a path and render the concatenated keystrokes (`NormalReady --d--> Operator(Delete) --w-->` renders `dw`; `--i--> OperatorInner --w-->` renders `diw`). The traversal root, not the terminal context, determines the help section: the shared Goto and Pane contexts can therefore render paths under both Normal and FileBrowser when reachable from both roots. `CharSearchKind` keeps `f/F` and `t/T` capture contexts distinct, allowing their different summaries to own separate terminal `AnyChar` bindings. An `AnyChar` capture binding renders each incoming arm path with a `{c}` suffix (`f{c}`, `F{c}`, `"{c}`, `r{c}`); arm bindings themselves emit no separate label. Separators and spacing (e.g. `sr h`) are renderer policy; the keystrokes always come from the graph, so rebinding an arm key automatically corrects every derived sequence. Groups supply the prose summary and category; a group's rendered labels are the union of paths terminating at its bindings. Structural test: every group renders at least one label; rendered pages keep the 80-column bound. Cosmetic drift from today's curated labels is expected and acceptable; compare output at the final commit and tune binding order for readability.

## 7. Commit sequence (full suite + `cargo fmt` after each)

1. `test: characterization tests via a dispatch_key wrapper` — wrapper + `editor_mode()`; the §5.1 suite; zero behavior change.
2. `fix: reset pending key state when the engine overrides the mode` — extract `apply_mode_override`, clear pending, test (§4.2). The only behavior change in the sequence, isolated and explicit.
3. `refactor: mode-indexed InputState` — §2 types replace `current_mode` + 11 fields; handlers become matches on the state; draw.rs indicator/suffix, `handle_paste`, run loop updated; field-asserting legacy tests rewritten against behavior or state enum. Dispatch still imperative.
4. `feat: keymap registry; migrate confirm mode` — §3 types, resolution precedence, `execute`/`arm_target`/`on_unmatched`/`context_of`; Confirm bindings only; per-mode structural tests. Proves table, executor, and modifier model on the smallest surface.
5. `feat: migrate insert mode` — delete the insert handler.
6. `feat: migrate visual mode` — anchor/kind from state; delete the visual handler.
7. `feat: migrate normal-mode base and file browser` — base + Goto groups; remaining guard contexts still imperative, consuming `state` written by arm actions.
8. `feat: migrate normal prefix and operator contexts` — remaining contexts (register capture with continuations, operators, yank, fold, bracket, pane, resize); delete normal.rs handlers.
9. `feat: migrate command and search prompts` — prompt methods extracted with typed outcomes; per-mode handlers deleted; `dispatch_key` is now table-only; global structural tests land.
10. `refactor: derive :help keys from the keymap; delete keys_catalog` — graph-traversal labels; semantic reachability tests; keys_catalog.rs and banner comments removed; `--help keys`/`:help keys` output diffed against pre-refactor and binding order tuned.

Any further intentional behavior changes: separate commits after all of this. None are required.

## 8. Verification

- Suite green after every commit; characterization tests unedited after commit 1.
- After commit 8: manual session (`cargo run -- README.md`): `dd`, `d j`, `"ayy`/`"ap`, **`y"ay`**, `diw`, `f x ; ,`, Ctrl-h moves, `sv`/`sr`+resize+Esc, `gt`, Visual `v`+motions+`y`, `:tree` browser, status-bar suffixes ("NOR s"/"NOR sr"/"NOR g").
- After commit 10: diff `--help keys` and `:help keys` output against pre-refactor; 80-column test still enforced.
