# ADR 0096 - Drop-reclaim drain observability

## Context

ADR 0095 made `DROP TABLE` return at durable logical completion, which
removed the only cleanup signal the product side had. Nothing observable
now distinguishes a settled drop from a stalled one: a still
`drop_reclaim_progress.redb` is *expected* after settlement (settlement
neither rewrites nor removes the node-local progress entry, and the
sweeper never reloads it once replicated debt is gone), and flat slab
bytes are ambiguous wherever dropped and surviving rows shared a slab.
No metric, routine log line, or admin query touches debt, cursors, or
settlement — only the post-cursor-restart and failed-pass paths log
today. The live evidence run that confirmed 0095 (all six PRODUCT-0137
cardinalities, total runtime 478.9 s → 137.0 s) filed exactly this gap:
its operators could not tell whether reclaim had completed or was not
progressing, and both readings fit every externally visible artifact.

Three facts bound what a truthful surface can promise without new
consensus state:

- Reclaim debt and retained drop outcomes carry **no wall timestamps** —
  operation identity and log indexes only. Age and historical drain
  duration are not derivable; adding them is consensus-format work.
- Participant completion persists **only** as `phase = Complete` in each
  node's local progress store. Slice responses return `{examined, done,
  corruption_reset}`; the leader folds `done` into a per-sweep local and
  no leader-side receipt catalog exists.
- Those progress stores are node-local, so any cluster-wide
  per-operation status read requires participant fan-out (or replicated
  progress, which 0095 deliberately rejected).

The waiting consumer's actual need is a **barrier** — proof that
accepted cleanup debt has drained, plus an externally measurable drain
time. That requires no per-operation history, no timestamps in consensus
state, no fan-out, and no format bump. The wider per-operation status
API is a named follow-on, not this cut.

## Decision

**1. An authoritative outstanding-debt gauge, refreshed at
state-publication boundaries.** `wendelon_table_drop_reclaim_debts` —
unlabeled — always equals the replicated debt catalog's size, via one
`set_drop_reclaim_debts(catalog.len())` function with **set-only
semantics** called at the places state is published: the live-apply
publication boundary (once per applied batch, so a future debt-touching
mutation inherits the refresh structurally rather than by a hand-added
call), the end of recovery construction (snapshot plus WAL replay), and
after snapshot installation is validated and published; fresh empty
construction sets zero. Leadership change is **not** a refresh site —
an election mutates nothing, followers stay correct because they
live-apply, and acceptance asserts equality across an election rather
than demanding a callback. Exposed on the existing authenticated
`/metrics` listener; Toron needs no SQL relation for this evidence
gate.

**2. A settlement counter and structured settlement log, emitted from
the live-apply side-effect queue.** The emission seam matters: leader
proposal confirmation can be lost after commit (the next leader sees no
debt and has nothing to re-emit), and logging inside the apply function
would re-emit during recovery, which invokes the pure apply directly.
The state-machine apply loop's existing pattern — apply under the lock,
collect side effects on `Success`, emit after release — is the seam: a
settlement observation is queued only when a **live-applied**
`TableDropSettlement` returns `Success`. Cardinality, stated honestly:
one line and one counter increment per operation **per daemon that
live-applies the committed settlement**, with the local node id in the
line's fields (operation id, object, finalize log index, participant
count, leadership epoch, node id). A rejected duplicate settlement and
WAL recovery emit neither. Companion counter
`wendelon_table_drop_settlements_total`.

**3. Per-participant phase-transition logs.** Each participant logs its
phase transitions with operation, **from_phase, to_phase**, the
post-transition cursor, and the cumulative examined count — bounded (a
few per phase per operation), structured, and the local answer to "is
my node's pass progressing". Emitted at `info`: `DEFAULT_LOG_FILTER` is
`info` and `build_env_filter` reads `WENDELON_LOG`, so the routine
settlement and phase lines are on in the stock configuration.

**4. The drain-time recipe requires zero debt and exclusive DROP
activity — an arbitrary baseline is not a barrier.** An aggregate gauge
cannot distinguish "my debt settled" from "someone else's did": with an
unrelated debt at baseline, the gauge can return to its starting value
while the run's own debt is still outstanding. The recipe is therefore:
wait for **zero** debt before the run; admit **no unrelated DROP**
during the measured cleanup window; issue the run's drops; wait for
zero again. The acceptance-to-zero interval is the drain time.
Return-to-a-nonzero-baseline is **rejected as ambiguous**, never
reported as drained. If zero/exclusivity cannot be obtained, the
aggregate metric is not a per-run receipt and the wider per-operation
API (held out below) is the correct instrument. Wall-clock truth lives
with the observer, where it survives restarts.

**Held out, named:** the cluster-wide per-operation status API (leader
endpoint for replicated debt/outcome state, node endpoint for local
cursor state, fan-out over the stamped set with partial-result
semantics, legacy-outcome semantics, and durable finalize/settlement
timestamps if historical duration becomes contract). **Per-operation
Prometheus labels are forbidden** — operation identity belongs in
structured logs or a bounded admin response; a guard pins the gauge
unlabeled.

## Acceptance

- **C1 (barrier semantics):** the gauge rises at logical finalize,
  stays nonzero through incomplete participant work **and through a
  restart with outstanding debt**, and reaches zero only after
  settlement.
- **C2 (withheld completion):** a deterministically withheld participant
  completion keeps the gauge nonzero across a **configurable number of
  reconciliation cycles** — with no settlement and no progress
  corruption — where raising that number is independent of production
  timeouts ("indefinitely" is not a provable assertion; N cycles is).
  The stalled-reclaim state the evidence run could not detect becomes
  the detectable state.
- **C3 (drift-proofing):** WAL replay, snapshot installation, and an
  induced leadership change each leave the gauge equal to the catalog —
  asserted by comparison; the election case asserts equality without
  any election-triggered refresh existing.
- **C4 (signals and emission seam):** settlement emission controls
  cover commit-then-lost-proposer-response (the line still appears,
  from live apply), a rejected duplicate settlement (no emission), and
  restart recovery (no re-emission); the lines carry the documented
  fields under the stock configuration (`WENDELON_LOG`; `RUST_LOG` is
  inert).
- **C5 (barrier honesty):** the discriminating recipe control — with an
  unrelated debt present at baseline, return-to-that-baseline is
  rejected as ambiguous, not reported as drained; with the zero/
  exclusive precondition met, the settlement line falls within the
  externally measured acceptance-to-zero interval. The live drain
  measurement is **forward evidence after these controls**, not a unit
  control equating log timestamps with the external clock.

## Guardrails

- The gauge is unlabeled and state-sourced; the guard pins **one
  unlabeled renderer, set-only semantics, and the three
  state-publication families** (live-apply boundary, recovery
  construction, snapshot installation) — not an enumeration of the
  current mutation variants, which would make the census brittle
  without adding correctness.
- No consensus-format change, no new wire opcode, no change to 0095's
  reclaim semantics — observation only.
- Log fields are structured and named; the settlement line is the
  routine signal, not an error-path artifact.

## Risks

- **A gauge is an aggregate**, and an aggregate can lie about a
  particular operation — hence the zero/exclusive precondition in
  decision 4 and the C5 rejection of nonzero baselines. Where
  exclusivity is unobtainable, the aggregate is explicitly not a
  per-run receipt; per-operation diagnosis is the named follow-on.
- **Log-based phase visibility depends on log retention/level** at the
  deployment; C4 pins the stock configuration so the signal is on by
  default.

## Forward

1. Cluster-wide per-operation reclaim status API (the wider rail, with
   its honest costs enumerated above).
2. Durable finalize/settlement timestamps if historical drain duration
   becomes part of a contract.
3. Index footprint accounting / profile compaction (separate topic; the
   observed 6× index-to-slab ratio stays an observation — no constant
   or defect claim attached).
