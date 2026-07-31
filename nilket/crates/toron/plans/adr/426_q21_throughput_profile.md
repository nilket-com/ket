# ADR 426 - q21 throughput profile: a durable seven-stage receipt that survives the 600 s kill

## Context

Post-425 board: 18/22 green. The residency family and both cheap oracle-instrument cells are
closed; the four remaining cells are all genuinely hard. q21 is the highest-information next
cut, but it is gated on a measurement that has been named for five cycles and never taken. Its
proximity to green is unknown: 603 CPU seconds at an external 600-second kill proves saturation
through the gate, not that completion was only three seconds away.

q21 is **engine-complete and correct** - it passes strict compare at sf=1/sf=0.01, and its
memory is bounded (`MemoryPeak=2,242,555,904`, well under 4 GiB). Its only observed wall is
wall-clock: ADR 421's durable profiles prove both existence summaries finished in 6.87 s before
the external 600 s kill; the whole process reported `CPUUsageNSec=603,224,021,000`. q21 is
correct on the completing smaller scales, but the killed run does not say how far the sf=10
drive progressed or which downstream stage is slow.

Three grounded facts define the cut:

1. **The current instrument cannot answer the question.** The chain-drive probe loop times the
   whole loop with a single `probe_started = Instant::now()` (`exec/join/streaming.rs:10995`) and
   writes one aggregate `record_task_telemetry(... probe_micros += ...)` **after the loop
   completes** (`:11102`). On q21's 600 s external kill that flush never runs, so nothing
   survives; and even on completion it is one total, not a per-stage split. The whole-loop
   timing gap is the reason five cycles of "profile first" produced no profile.
2. **The chain is replay 0-4: seven stages.** q21's plan is
   `LeftAnti(replay4)( LeftSemi(replay3)( Inner(replay2)( Inner(replay1)( Inner(replay0)(
   supplier, filtered lineitem), filtered orders), filtered nation), semi-summary),
   anti-summary)`, then the grouped fold. The semi/anti stages **are** the 421 resident
   summaries. The honest decomposition is **base scan; inner replay 0; inner replay 1; inner
   replay 2; semi replay 3; anti replay 4; grouped fold** - seven stages.
3. **No ready fix is banked.** ADR 423's guarded primitive fold is Int64-key `SUM`; q21's fold
   is `COUNT(*)` over `s_name` (Utf8) (`toron-tpch/src/lib.rs:215`). It is the open-addressed
   *lineage* (414 -> 420 -> 421 -> 423), not a drop-in. So a fold-side fix would be **new
   machinery** (a Utf8-key COUNT primitive), and only a *probe*-side dominant term with a bounded
   existing-recognizer remedy could ride this seal.

(q17, the other reach-only cell, was grounded this cycle as an **oracle boundary-precision
fidelity** gap - its oracle is already O(eligible-parts) frugal, so the 425 template does not
apply; carried, not this ADR.)

## Decision

ADR 426 lands a **durable seven-stage throughput receipt** for the reoriented semi/anti chain
drive and uses it to produce the grounded per-stage **wall-time** split through q21's 600-second
gate. (`CPUUsageNSec` remains an outer saturation fact; `Instant` timers do not claim a CPU-time
split.) **Diagnosis is the guaranteed deliverable;** a same-seal fix is conditional and
profile-determined.

### The instrument (off-by-default, engine-side, reusable)

An env-gated, query-neutral, durable receipt (the 421/423 receipt lineage) covering the chain
drive, with three load-bearing properties:

1. **Exclusive seven-stage timers plus an explicit unattributed residual.** Each stage (base
   scan, inner replay 0/1/2, semi replay 3, anti replay 4, grouped fold) gets its own
   **non-overlapping** timer around only that stage's work; there is no timed parent interval to
   double-count a child. One total-drive timer runs independently. At each snapshot require
   `sum(stage_micros) <= total_drive_micros`, then define **`unattributed_residual`** as the
   checked difference so the integer fields sum exactly to the observed total. A large residual
   is itself a finding, not a rounding error.
2. **One atomic snapshot at drive start and after every completed base batch, surviving external
   termination.** Resolve one receipt path, write a complete JSON snapshot to its sibling
   temporary path, then rename over the prior snapshot once before the first scan await, after
   every completed base-scan batch, and once on normal/error exit. The snapshot carries a
   strictly increasing sequence, completed-base-batch count, raw/post-filter base rows, and
   all stage accumulators. Thus a 600 s kill leaves one parseable last-completed-batch receipt -
   no JSONL/overwrite ambiguity, corrupt trailing record, or unspecified wall-time cadence. An
   enabled profiler's snapshot I/O failure is loud; it never silently claims a complete profile,
   while the uninstrumented path cannot encounter that I/O. The measured sf=1 overhead decides
   whether every-batch flush is acceptable; if not, stop and redesign the cadence rather than
   silently loosening it.
3. **sf=1 parity and overhead calibration before any sf=10 attribution is trusted.** The
   instrumented binary must produce the **same q21 output digest/schema/row count** as the
   uninstrumented one at sf=1 (instrumentation changes no result), and its **profiling overhead
   is measured** at sf=1
   (instrumented vs uninstrumented wall-clock) so the sf=10 per-stage attribution is reported
   net of, or with, a known overhead - never as a distorted artifact.

The receipt is off-by-default (absent env var -> zero receipt work and byte-identical output),
query-neutral (no q21/ADR branding), and lands in `exec/join/streaming.rs` (not `exec.rs`, which
is at its 7,500-line ratchet). Production resolves the env path once; tests use a private
injected path/sink seam and never mutate process-global environment or depend on `OnceLock`
initialization order. It is reusable by ADR 427 to measure any fix's per-stage before/after.

### The diagnosis (the deliverable)

First run q21 sf=10 under the unchanged 4 GiB / **600 s board gate**, proving the snapshot
survives the kill and reporting its exact progress (`completed_base_batches`, raw/post-filter
rows) with the cumulative partial split. Then make one **raised-time-budget completion attempt**
under the same memory cap. Completion is not assumed: a second timeout or OOM is a valid finding
and the last atomic snapshot remains the evidence. Report each stage's exclusive micros and
percentage, the unattributed residual, progress, and the **dominant measured term**; do not
extrapolate a partial split to unobserved rows without stating that limitation. Any over-gate
completion is **diagnostic-only and earns no board credit** (the q18-6 GiB / q17-sf0.02 honesty
rule); the board stays 18/22 and q21's cell records the grounded split plus the named next
mechanism when the evidence is decisive.

### The conditional fix

- If a **probe** stage dominates and its remedy is a **bounded extension of an existing
  recognizer/streaming path**, that fix rides this seal with its own instrumented before/after
  per-stage evidence and a re-measured sf=10 outcome.
- If the **grouped fold** dominates (the likely case), its fix is a **new Utf8-key COUNT
  open-addressed primitive** - new machinery, named as ADR 427, not folded here.
- If the dominant term needs the general pull interface, that is C-spec evidence for the q02
  rock, named, not half-built.

Diagnosis-only is the expected outcome and is a complete seal.

## Acceptance

- **Guaranteed:** a kill-surviving cumulative seven-stage snapshot from the canonical q21 sf=10
  gate (exclusive per-stage micros + %, exact unattributed residual, progress and row flow),
  with the dominant measured term named. A completed full-drive split is claimed only if the
  raised-budget attempt actually completes; an inconclusive/unstable partial profile is recorded
  as such and never forced into a guessed 427 mechanism.
- **Instrument locks:** off-by-default byte-identical parity (absent env var); sf=1
  instrumented-vs-uninstrumented answer parity; measured profiling overhead; exclusive timers +
  residual sum-to-total; a simulated mid-drive kill leaves a parseable receipt at the last
  completed base batch.
- **Honesty:** over-600 s completion earns no board credit; the board stays 18/22 unless a
  same-seal bounded fix brings q21 under both the 4 GiB cap and the 600 s gate at pass@compare.
- **Controls:** the twelve established sf=10 greens and q18's clean 425 green are untouched (the
  instrument is off by default; the drive path is behavior-identical without the env var).
- **Full engine gate** after fmt (`--no-fail-fast` + architecture source-scan guards); `exec.rs`
  ratchet not touched; de-branded; no `.perf.data`.

## Guardrails

- **Diagnosis is the floor** - the seal ships the grounded split even if no fix is bounded.
- **The receipt sums to the total** - exclusive timers + explicit residual; no hidden time.
- **Durable and atomic** - one complete every-base-batch snapshot survives the kill the current
  instrument loses.
- **Enabled write failures are loud** - stale/absent evidence cannot masquerade as a completed
  profile; the env-unset path performs no receipt I/O.
- **Instrumentation proves it is inert** - off-by-default byte parity + sf=1 answer parity +
  measured overhead before sf=10 attribution is trusted.
- **No pre-committed fix** - the profile names the mechanism; a new-machinery fold fix is 427.
- **Over-envelope earns no credit** - raised-time-budget completion is diagnostic only.
- **Won ground is a control** - the twelve greens + q18 untouched.

## Risks

- **A memory wall behind the time wall** - q21 run past 600 s might OOM rather than complete ->
  reach honesty: record it as a new named finding; the partial receipt still gives the split up
  to that point.
- **The raised run still times out** -> record its exact progress and final snapshot; do not
  raise the gate repeatedly or call the partial profile a completed-drive split.
- **A dominated-by-residual result** - if the seven stages under-cover the total, the residual
  names an unattributed cost (scheduling, exchange, repartition) that becomes the real 427
  driver; the receipt's sum-to-total property makes that visible rather than hidden.
- **Profiling overhead distorts the split** - the sf=1 calibration bounds it; timers are coarse
  (per-stage, per-flush), not per-row.
- **Instrument leaks into behavior** - off-by-default byte-parity lock is the fence.

## Forward Arc

426 discharges the standing "no q21 speedup claim without the profile" rule and names 427
concretely from measured evidence: most likely the Utf8-key COUNT open-addressed primitive (if
the grouped fold dominates), possibly a bounded probe-recognizer extension (if a probe
dominates), or C-spec evidence for q02 (if the general pull interface is implicated). The durable
receipt is reused to verify whichever fix lands. q17 (oracle boundary-precision fidelity), q02
(C-spec rock), and q05 (diagnosis-first OOM; the receipt pattern here is a template) remain the
other named threads.

## Grounding evidence (file:line)

Whole-loop timing gap: `exec/join/streaming.rs:10995` (`probe_started` before the drive loop),
`:11102` (single aggregate `probe_micros` flush after the loop - lost on external kill). Chain
shape: 417 q21 artifact (replay 0-4) + 421 resident summaries. Grouped fold shape:
`toron-tpch/src/lib.rs:215` (`SELECT s_name, COUNT(*) ... GROUP BY s_name`). q21 last-known wall:
421 results (`CPUUsageNSec=603,224,021,000`, `MemoryPeak=2,242,555,904`, 600 s timeout, summaries
6.87 s). Receipt lineage: ADR 421 build profiles / ADR 423-424 scheduler receipt (durable,
off-by-default, query-neutral). q17 grounding: `toron-tpch/src/lib.rs:8570` (O(eligible-parts)
oracle), `:8663` (exact Decimal128 boundary), `:4431`/`:8318` (sf=0.02 pin).
