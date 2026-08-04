# ADR 332 - name the `unrecognized` execute-fails (q04/q12/q20 @ sf=1)

## Status
Proposed.

## Context

q04/q12/q20 fail at sf=1 with `execute status=fail reason=unrecognized`; the **same bucket recurs at scale**
(q02 @ sf=10/100 = `unrecognized`). `unrecognized` is the soft bucket: `reason_for_error` has no pattern for the
error (telemetry.rs:181), so it is a `fail` **with the real cause dropped from the row.**

The reframe that scopes this cut: **`unrecognized` is telemetry debt, not (yet) a scale root cause.** Today the
harness cannot tell us *why* these cells fail - so we cannot route them. The first, bounded job is to pay that debt
(retain + name the cause); whatever scale root cause (bug or unsupported feature) underlies it is then scoped from
the **measured** message, not guessed.

### The swallow (grounded)
- `reason_for_error(msg)` -> a recognized reason, else `"unrecognized"` (telemetry.rs:90/181); `unrecognized` ->
  `fail` (the honesty rule: an unrecognized reason is never a `blocked`).
- `StageError` is `{ phase, reason }` (lib.rs:1342) - **no message field.** The constructions compute
  `reason_for_error(&format!("{error:?}"))` (lib.rs:1361/1367) and **drop the raw `{error:?}` after classifying** -
  only `reason` survives. (The `message: format!("{error:?}")` at lib.rs:1280 is `StorageError::InvalidPbcProgram`,
  a different type.) So the cause is **not retained**; the run shows `unrecognized` and the raw error is gone.

## Decision - retain the cause, measure it, then name it (route from the measurement)

1. **Pay the telemetry debt (durable):** `StageError` retains the raw `{error:?}`. The cause becomes available in
   **JSONL / results / debug output only** - this **does not change `matrix` rendering**, which still shows the
   classified `reason` cell. The extra field is **conditional / clearly diagnostic** (e.g. present only on a failed
   row, in a diagnostic-named field) so it does not become a noisy public contract by accident.
2. **Measure (Phase 0):** run q04/q12/q20 strict at sf=1 (release build, fast); record the raw messages. Classify:
   one error class (shared root) or several? (q02 @ sf=10/100 noted as same-family-or-not - banked, not chased.)
3. **Name (deliberate second step, after measuring):** add **narrow** `reason_for_error` classifier(s) for the
   measured message(s); `unrecognized` is gone for these cells. **Blocker-vs-fail is chosen from the measured
   message** - a legitimate unsupported-feature wall becomes a recognized `blocked(precise_reason)`; a genuine bug
   stays `fail` with a precise reason.
4. **Route:** fold a tiny fix **only if Phase 0 proves it tiny**; otherwise the results doc **names the next ADR**
   (the scale root cause / feature), scoped from the named error.

## Acceptance
- q04/q12/q20's raw error **surfaced + classified + named**: a precise `reason_for_error` entry per measured class;
  `unrecognized` no longer appears for these cells.
- Each cell **routed**: recognized `blocked(precise_reason)` + named feature cut, or `fail(precise_reason)` + the
  fix (folded only if Phase 0 proves tiny, else the next ADR named).
- **Retention is durable + diagnostic, not a semantics change:** the raw cause appears in JSONL/results/debug;
  `matrix` still renders the classified `reason` cell; the new field is conditional/diagnostic (not a public
  contract).
- The new reason(s) are covered by `reason_for_error` (a future regression is named, not silently bucketed). Only
  legitimate unsupported-feature reasons enter `RECOGNIZED_BLOCKERS`; bug reasons must stay out of that allowlist so
  they remain `fail(precise_reason@execute)`.
- **q02 @ sf=10/100 noted** (banked evidence for the sf=10+ arc; not chased - sf=1 is the scope).
- sf=0.01 reach 22 / correctness 22 unchanged; full `-p toron-tpch` green.

## Risks
- **Route after measuring, not before** - no blind fix; the bug-vs-feature decision waits for the raw message
  (the ADR 331 discipline).
- **Don't grow a noisy contract** - the retained message is conditional/diagnostic; downstream consumers key off
  the classified `reason`, not the raw string.
- **Matrix semantics frozen** - retention must be invisible to `matrix`; the classifier change (step 3) is the only
  thing that moves a cell, and only after measurement.
- **Over-fix temptation** - if Phase 0 shows a real feature/bug arc, it is the *next* ADR, not folded here.

## Forward arc
The named wall(s) scope the next cut (a fix, or a feature/scale ADR). The durable retention makes **every future
`unrecognized`** - at any scale - arrive with its cause attached, so the sf=10+ arc opens with a named first wall
(q02 @ sf=10/100 already in the bank). Telemetry stops lying-by-omission about why a cell failed.

## Why toron-home
The retention + the precise reasons live in `toron-tpch` (`telemetry.rs` / the run emit); any fix is `crates/toron`.
No Wendelon / cross-ring surface.

## Council provenance
Operator + Codex + Claude. Codex's corrections folded: `StageError` has **no** message today (the raw error is
classified then dropped - Claude's first read conflated it with `StorageError::InvalidPbcProgram`); the retention
must **not** change `matrix` semantics (JSONL/results/debug only); the classifier change is a **deliberate second
step** after Phase 0 measures; the JSONL extra field is **conditional/diagnostic** so it isn't a noisy contract.
Measure-don't-predict, applied to telemetry debt: name the cause before scoping the cure.
