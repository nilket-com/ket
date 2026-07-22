# Spike Report: Rune Host Access

Falsification spike for `plans/rune-scripting.md` §3.1 — how a Rune host function reaches a
stack-local `EditorSession` on the engine thread under `panic = "abort"`.

**Result: the premise was wrong, and the answer is simpler than any of the three candidates.**
Direct `&mut` as a per-invocation argument is sound, supports stored callbacks, and needs **zero
unsafe**. Recommend it. Two secondary findings change the plan independently of that choice.

Spike code was throwaway and is not retained. The findings below are therefore **claims, not standing
evidence** — nothing in the tree currently proves them, and `rune = "0.14.1"` permits Cargo to resolve
a later `0.14.x`. Two behaviors are load-bearing enough that they must be re-established as
acceptance tests in the VM-lifecycle spec, where they will also serve as dependency-upgrade
tripwires: **stored-callback invocation with a guarded `&mut`**, and **invalidation of an escaped
handle through nested containers**. If either regresses in a point release, the host design fails
silently.

## 1. Recommendation — Design D: `&mut` per invocation

Not one of the three designs we set out to compare. It emerged from the negative controls, which
were supposed to *justify* excluding it.

```
engine thread:  vm.call(["on_save"], (&mut session,))          // command / startup
                handler.call((&mut session,))                  // deferred event drain
script:         pub fn on_save(editor) { editor.insert_line("x") }
```

- **Zero `unsafe`.** No thread-local, no raw pointer, no generation counter, no scope guard.
- Rune encodes the reference behind a guard (`guarded_args.rs:11-20`) and invalidates it when the
  call returns.
- **A stored callback can be handed a `&mut` at invocation time.** `Function::call` takes
  `impl GuardedArgs` (`rune/src/runtime/function.rs:185`), not just owned `Args`. This was the
  load-bearing question — an event bus must invoke stored script handlers — and it is the single
  fact that makes Design D viable at all. Verified end to end: a stored Rune closure mutated the live
  session and enqueued a deferred event.

### The API rule this forces

> **A handler receives the editor as a parameter. It never captures it.**

```rune
baryon::on_event("BufWritePre", |editor| { editor.insert_line("ok") });   // works
baryon::on_event("BufWritePre", move || { editor.insert_line("no") });    // refused at call time
```

Capturing is rejected by **rune**, not by us — `Cannot use owned operations for ::baryon::Editor` —
before any handler is registered. The rule that makes the borrow sound is enforced by the runtime,
which is exactly the property we wanted from a generation token and did not have to build.

## 2. The premise that excluded `&mut` is false

§3.1 assumed a retained borrowed argument "can panic when the call finishes," which under
`panic = "abort"` is process death. **It does not.** Five retention paths, each in a child process:

| Retention path | Outcome | Abort? |
|---|---|---|
| capture in a closure | refused at call time: `Cannot use owned operations` | no |
| return the handle directly | escapes; **rejected on reuse**: `Cannot write, value is M-000000` | no |
| push into a `Vec`, return it | escapes; rejected on reuse | no |
| store in an object, return it | escapes; rejected on reuse | no |
| nest inside a tuple, return it | escapes; rejected on reuse | no |

Zero signals. Four paths let the *value* escape the call, but rune's guard invalidates it on drop —
including **through nested containers** — so every subsequent use is a clean `VmError`. Use-after-
invalidate is an error, not undefined behavior.

The only cost versus a generation token is message quality: `Cannot write, value is M-000000` is
worse than `stale editor handle: it belongs to an earlier callback`. That does not justify a
thread-local and an `unsafe` block.

## 3. `budget::acquire()` does not work in host functions

**This is a defect in the mechanism `plans/rune-scripting.md` §2.9 relies on, and it holds for every
design.**

`vm.rs:3028` — the VM calls `budget::acquire()` at the top of its instruction loop, which *replaces*
the thread-local budget with `usize::MAX` and moves the real remaining budget into the VM's own local
`BudgetGuard`. A native function invoked from that loop calling `budget::acquire()` therefore gets
`usize::MAX` back, and `BudgetGuard::take()` returns `true` forever.

Measured: a host function performing 10⁹ iterations, with `guard.take()` checked every iteration,
under a **100,000-instruction budget**, ran to completion (208 ms) and returned `Ok`.

Rune's documentation says native functions must cooperate with the budget. In 0.14.1 they **cannot**
— not through the documented API, when called from a budgeted VM.

**Consequence.** §2.9's "per-operation Baryon caps" is promoted from belt-and-braces to load-bearing.
A host function cannot be interrupted; it can only **decline in advance**. The VM instruction budget
bounds *script* loops only, and it does so correctly — a runaway `while true {}` in script was cut off
as expected.

Two rules follow, and they are not the same rule:

- **Hard per-operation caps.** Every operation whose work scales with script-controlled input must
  have an intrinsic cap and **preflight it before mutating** (the `max_bytes` of §2.8 is the model).
  This is not a blanket requirement on every host function: constant-time operations need no
  artificial limit.
- **A Baryon-owned host budget.** Rune-budget cooperation is unmeetable, but *host*-budget
  cooperation is not. The host facade (§7) can carry a per-callback quota, charged across native
  calls, so that a script issuing a million cheap-but-not-free host calls is still bounded. This
  cannot interrupt a native operation already in flight, which is why hard per-operation caps remain
  mandatory alongside it.

## 4. Non-transactionality confirmed

Budget and allocation exhaustion mid-mutation, with `&mut` live on the VM stack: the VM errors, the
session invariant holds, **and completed writes remain applied**. No rollback, no abort. This is the
behavior §3.12 already commits to; it is now observed rather than assumed.

## 5. Comparison

| | **D: `&mut` per call** | A: scoped host + token | B: snapshot + effects | C: `Arc<Mutex>` session |
|---|---|---|---|---|
| `unsafe` surface | **none** | 1 audited point + thread-local | none | none |
| Script observes its own writes | **yes** | yes | **no** — reads a stale snapshot | yes |
| Stored callback | **works** (editor passed in) | errors (stale generation) | "works", **silently stale** | works indefinitely |
| Capturing the editor | refused by rune, at call time | allowed, errors at use | allowed, silently wrong | allowed and valid |
| Re-entrancy | no re-entrant path through the host boundary | refused by scope guard | none exposed | **runtime lock error, or silent deadlock** |
| Engine refactor | none | none | none | **`EditorSession` stops being a stack local** |
| Worst failure mode | clean `VmError` | clean `VmError` | **silent wrongness** | **deadlock on the editor thread** |

Design B's failure mode is the one to avoid: a stored callback reads an old snapshot and enqueues
effects against a session that has moved on, with **no error at all**. Design C has the best
ergonomics — a captured handle stays valid forever — but buys them with an engine-wide ownership
refactor and converts re-entrancy from a compile-time borrow error into a runtime lock failure, or,
with `lock()` instead of `try_lock()`, a silent deadlock of the editor thread.

## 6. Acceptance criteria

| Criterion | Result |
|---|---|
| Script misuse cannot abort the process | **met** — 0 aborts across 5 retention paths, budget and allocation exhaustion |
| At most one audited unsafe access point; no raw-pointer logic in host functions | **exceeded** — Design D has zero |
| Missing / stale / wrong-generation / nested tokens return `VmError` | **met** — Design D has no tokens; the equivalents (capture, use-after-invalidate) are `VmError` |
| A retained token is harmless after its scope ends | **met** — invalidated on guard drop, including through nested containers |
| Events enqueue during callbacks, run only after the outer call returns | **met** |
| Instruction and allocation exhaustion leave the session valid; no rollback promised | **met** |
| Native host work demonstrates explicit budget cooperation | **NOT met, and unmeetable** — see §3 |
| Comparison records unsafe surface, ergonomics, re-entry, stored-callback compatibility | **met** — §5 |

## 7. The host facade

`#[derive(Any)]` requires `'static`, so `Editor<'a> { session: &'a mut EditorSession }` is not
expressible. That does **not** force `EditorSession` to become the script-visible type — an earlier
draft of this report claimed it did, and presented a false choice between exposing the engine's
internal type and moving the session in and out on every call. Neither is necessary.

A **long-lived owning facade** satisfies both constraints:

```rust
#[derive(rune::Any)]
#[rune(item = ::baryon, name = Editor)]
struct EditorHost {
    session: EditorSession,
    host_budget: HostBudget,
}
```

The engine holds `EditorHost` as its stack local — where `EditorSession` lives today — and passes
`&mut EditorHost` into each VM call and each stored-callback invocation. Nothing moves per callback.
`EditorSession` stays private, the script-visible type is a stable `baryon::Editor`, and the
guarded-borrow result is unchanged because the mechanism is identical: a `'static` `Any` type passed
by `&mut`.

This is also the natural home for the per-callback host budget of §3, which needs to live somewhere
that outlives a single native call but not the session.

Two properties still worth stating explicitly in the VM-lifecycle spec: the script API surface is
defined entirely by **what is registered in the `Module`**, not by what is `pub` on the type; and the
`name = Editor` rename decouples the Rune-facing name from the Rust type, so the facade can be
refactored without breaking scripts.

## 8. Changes required to `plans/rune-scripting.md`

1. **§3.1 resolved.** Replace the open question with Design D and the "editor is a parameter, never
   captured" rule. Delete the scoped-ambient-host and generation-token candidates; they are
   unnecessary. §2.2 ("Rune executes synchronously on the engine thread") is **no longer contingent**
   — it is confirmed, with no unsafe and no engine refactor.
2. **§2.9 amended.** Record that `budget::acquire()` is unavailable to host functions in rune 0.14.1;
   that hard per-operation caps are mandatory for operations whose work scales with script-controlled
   input, and must preflight before mutating; and that a Baryon-owned per-callback host budget on the
   `EditorHost` facade bounds call *volume*, which rune's budget cannot. The VM instruction budget
   bounds script loops only.
3. **§3.12 confirmed** rather than proposed: callbacks are not transactional; individual host
   operations preserve invariants; there is no rollback.
4. **§2.2 carries the `EditorHost` facade** of §7 — `EditorSession` does not become the script-visible
   type.
5. **§2.4 qualified.** "Re-entrancy impossible by construction" overstates it: no re-entrant path
   exists *through the proposed host boundary*, because host functions never receive a VM handle. The
   deferred-event rule is what preserves that, and remains structural.
6. **§5.8 (VM lifecycle) gains acceptance tests** for stored-callback invocation and nested
   invalidation, as dependency-upgrade tripwires. The spike code is gone and `0.14.1` can resolve
   forward.
7. **§5.1 (host prototype) closes.** The next spec is the input contract.
