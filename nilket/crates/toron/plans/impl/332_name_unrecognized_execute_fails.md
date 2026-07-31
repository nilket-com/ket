# ADR 332 - Implementation plan: name the `unrecognized` execute-fails (q04/q12/q20 @ sf=1)

Pairs with `plans/adr/332_name_unrecognized_execute_fails.md`. A `toron-tpch` telemetry + grounding cut (retain the
cause, measure it, name it), plus **at most** a tiny `crates/toron` fix - only if Phase 0 proves it tiny. Release
build (fast); measure-don't-predict; route from the measured message.

## Phase 0 - retain the cause + measure (steps 1-2)
1. **Retain the raw message (durable).** Add `message: String` to `StageError` (lib.rs:1342); store
   `format!("{error:?}")` alongside the classified `reason` at the construction sites (lib.rs:1361/1367 + peers).
   Surface it in **JSONL / results / debug output only**, as a **conditional / clearly-diagnostic** field (present
   on a failed row, in a diagnostic-named key) - **not** a change to `matrix` rendering, which still emits the
   classified `reason` cell. The retention is invisible to any consumer keying off `reason`.
2. **Measure q04/q12/q20 strict @ sf=1** (release build; `target/release/toron-tpch run --query qNN --mode strict
   --sf 1`). Record the raw `{error:?}` for each. Classify: one shared error class or several? Note whether
   q02 @ sf=10/100 is the same class (banked - not chased).
- Record the raw messages + the named class **in the results doc before** writing any classifier or fix (the
  ADR 331 ground-first discipline).

## Phase 1 - name (deliberate second step, after Phase 0)
- Add **narrow** `reason_for_error` classifier arm(s) for the measured message(s) (telemetry.rs:90). One precise
  reason per measured class; `unrecognized` gone for q04/q12/q20.
- **Status from the measured message:** a legitimate unsupported-feature wall -> a recognized blocker reason (add
  to the allowlist; `status_for_reason` -> `blocked`); a genuine bug -> keep `fail`, precise reason.
- Keep the arms tight (match the specific message signature, not a broad substring that could mis-bucket unrelated
  errors).

## Phase 2 - route (step 4)
- **If Phase 0 proves a fix is tiny** (a one-line guard / obvious gap): fold it; the cell moves to `pass@execute`
  and the results doc shows before/after.
- **Otherwise:** the results doc **names the next ADR** (the scale root cause / feature), scoped from the named
  error. No blind fix; no over-reach.

## Tests (`toron-tpch`)
- **Classifier:** a unit test that `reason_for_error(<measured message>)` -> the precise reason (per class); the new
  reason(s) are classified by `reason_for_error` (a future regression is **named**, not bucketed). Only
  unsupported-feature reasons enter `RECOGNIZED_BLOCKERS`; bug reasons stay out so `status_for_reason` keeps them
  as `fail`.
- **Retention is diagnostic, not semantics:** a test that a failed row carries the raw message in the
  JSONL/diagnostic field, **and** that `matrix` still renders the classified `reason` cell (semantics frozen).
- **Status routing:** `status_for_reason(<new reason>)` is `blocked` (feature wall) or `fail` (bug) per the measured
  classification.
- sf=0.01 reach/correctness stay green (unchanged).

## Phase 3 - the board (release build, capped, sf=1)
```
scripts/cargo-cgroup build -p toron-tpch --release
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=4G \
  --property=RuntimeMaxSec=300s /bin/sh -lc 'target/release/toron-tpch run --query q04 --mode strict --sf 1'
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=4G \
  --property=RuntimeMaxSec=300s /bin/sh -lc 'target/release/toron-tpch run --query q12 --mode strict --sf 1'
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=4G \
  --property=RuntimeMaxSec=300s /bin/sh -lc 'target/release/toron-tpch run --query q20 --mode strict --sf 1'
```
- Each shows a **precise reason** (not `unrecognized`); blocker or fail per the measured class; a folded tiny fix
  (if any) shows `pass@execute`. Update the ADR 330 atlas cells (q04/q12/q20 sf=1: `unrecognized` -> named).

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q04 --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q12 --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q20 --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix   # correctness 22/22 still renders
scripts/cargo-cgroup run -p toron-tpch -- matrix               # reach cells render the classified reason
git diff --check
# If a tiny crates/toron fix is folded:
#   scripts/cargo-cgroup check -p toron --all-targets
#   scripts/cargo-cgroup test -p toron --lib
#   scripts/cargo-cgroup test -p toron --test architecture_test
#   scripts/cargo-cgroup test -p toron --no-fail-fast
# Phase 3 release/cgroup q04/q12/q20 sf=1 probes must also be recorded.
```

## Results doc
`crates/toron/plans/impl/332_name_unrecognized_execute_fails_results.md`: the raw `{error:?}` for q04/q12/q20 @ sf=1
(the measured cause); the named reason + status (blocker/fail) per query; confirmation the retention is
diagnostic-only (matrix semantics unchanged); the q02 @ sf=10/100 same-family note; any folded tiny fix
(before/after) **or** the next ADR named; sf=0.01 boards unchanged.

## Risk / blast radius
- **Ground first** - record the raw messages before any classifier/fix; route bug-vs-feature from the measurement.
- **No noisy contract** - the retained message is conditional/diagnostic; consumers key off `reason`.
- **Matrix semantics frozen** - retention invisible to `matrix`; only the named classifier moves a cell.
- **Scope** - sf=1 only; sf=10/100 banked, not opened; a real feature/bug arc is the next ADR.

## Seal
`impl: adr 332 name unrecognized execute fails`. Do not commit wip drafts.
