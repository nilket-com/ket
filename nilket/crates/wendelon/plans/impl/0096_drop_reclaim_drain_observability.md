# Impl — Wendelon ADR 0096: Drop-reclaim drain observability

Pairs with `plans/adr/0096_drop_reclaim_drain_observability.md`.
Wendelon-only cut (no Toron code change; the consumer receipt is
documentation plus the live evidence exhibit). One `impl: adr 0096`
commit.

## Step 0 — grounding probes

- **P1 (state-publication boundaries):** anchor the three publication
  families — the live-apply loop's post-batch publication point (and
  its existing side-effect queue), the end of recovery construction
  (snapshot + WAL replay), and post-validation snapshot installation —
  plus fresh empty construction. **Not** a mutation-variant census, and
  **not** leadership change (an election mutates nothing).
- **P2 (metrics surface):** anchor the existing authenticated
  `/metrics` registry and its gauge idiom; confirm follower scrape
  behavior.
- **P3 — closed at review:** `DEFAULT_LOG_FILTER` is `info` and
  `build_env_filter` reads `WENDELON_LOG` (`RUST_LOG` inert). Routine
  settlement and phase lines emit at `info`; recorded as grounding.

## Step 1 — the gauge

- `wendelon_table_drop_reclaim_debts`, unlabeled, registered in the
  existing registry (P2 idiom).
- One `set_drop_reclaim_debts(catalog.len())` function, **set-only**,
  called at the P1 publication boundaries: once per live-applied batch
  (so future debt-touching mutations inherit it structurally), at
  recovery-construction end, after snapshot installation; fresh empty
  construction sets zero. The source-scan guard pins one unlabeled
  renderer, set-only semantics, and the three publication families —
  no `inc`/`dec`, no labels, no per-variant call sites.

**Controls (C1, C3):** gauge rises on finalize and falls only on
settlement; restart with outstanding debt shows nonzero immediately;
WAL replay, snapshot install, and an induced leadership change each
leave gauge == catalog size by direct comparison — the election case
asserting equality with no election-triggered refresh in existence.

## Step 2 — settlement counter and log

- `wendelon_table_drop_settlements_total` counter.
- Structured settlement line at `info`, fields: operation id, object
  id, finalize log index, participant count, leadership epoch, **local
  node id**. Emission seam: queue the observation in the state-machine
  apply loop's **live side-effect queue** when a live-applied
  `TableDropSettlement` returns `Success`; increment and log after the
  state lock releases. Not proposal confirmation (lost proposer
  response would drop a committed settlement's signal with nothing to
  re-emit) and not inside the pure apply function (recovery invokes it
  directly and would duplicate). Cardinality: one line and one
  increment per operation **per daemon that live-applies it**.

**Controls:** commit followed by lost proposer response → the line
still appears (from live apply); a rejected duplicate settlement →
no emission; restart recovery over the same WAL → no re-emission;
singleness per live-applying daemon asserted by count.

## Step 3 — participant phase-transition logs

Structured line per phase transition in the participant slice path:
operation id, **from_phase, to_phase**, post-transition cursor,
cumulative examined count. Bounded by construction (phases ×
participants); `info` level.

**Controls (C2, C4, C5):** a deterministically withheld participant
completion (ADR 0095's existing seam) keeps the gauge nonzero across a
**configurable number of reconciliation cycles** — no settlement, no
progress corruption, the number raisable independent of production
timeouts — and the phase logs show the stall's location; the C4
emission-seam arms (step 2); the C5 discriminating recipe control —
an unrelated planted debt at baseline makes return-to-baseline
rejected as ambiguous, while under the zero/exclusive precondition the
settlement line falls within the externally measured
acceptance-to-zero interval.

## Step 4 — the consumer receipt

Document the recipe beside the metrics surface (ops-facing doc, not a
council note), with the **zero/exclusive protocol**: wait for zero debt
before the run; admit no unrelated DROP during the measured window;
issue the drops; wait for zero again — acceptance-to-zero is the drain
time. Return to a **nonzero** baseline is rejected as ambiguous, never
reported as drained; where exclusivity is unobtainable, the aggregate
is not a per-run receipt (the per-operation API is the named follow-on
for that case). The live drain measurement is **forward evidence
after the controls**, requiring only that the settlement line fall
within the external interval.

## Step 5 — seal

```
scripts/cargo-cgroup fmt  -p wendelon -- --check
scripts/cargo-cgroup test -p wendelon --lib
scripts/cargo-cgroup test -p wendelon --test architecture_test   # FULL
scripts/cargo-cgroup test -p wendelon --no-fail-fast
scripts/cargo-cgroup test -p toron --lib                          # contract unchanged
scripts/cargo-cgroup test -p toron --test architecture_test       # FULL
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
git diff --check
```

(Toron legs retained because the crates share the workspace and the
seal convention is inherited; no Toron source change is expected — a
nonempty Toron diff is itself a finding.) Evidence receipt rides the
`impl:` commit: P1–P3 results, the C1–C5 outcomes, and one live drain
measurement from the documented recipe against a real drop on the
local deployment.

## Guard reconciliation

None inverted. Additions: gauge unlabeled + set-only + publication-
family source-scan. ADR 0095's seams are reused for C2; no new hooks
expected (state so in the receipt if it holds, per the ADR 0094
precedent).

## Implementation receipt — 2026-08-24

ADR 0096 is implemented as an observation-only Wendelon cut. The
authoritative debt gauge is derived from published replicated state, a
successful live-applied settlement emits one process-lifetime counter event
and structured line after the state lock is released, and participants emit
bounded structured lines only when their durable reclaim phase changes. No
consensus format, wire opcode, Toron source, or reclaim behavior changed.

### Grounding probes

- **P1:** the state-publication families are fresh construction, recovery
  construction after snapshot plus WAL replay, the post-batch live-apply
  publication point, and post-validation snapshot installation. Fresh state
  sets zero; the other three set the gauge from
  `table_drop_reclaim_debts.len()`. Leadership change is not a publication
  site and no node, control-RPC, or data-RPC lifecycle path calls the setter.
- **P2:** both new metrics use the existing node-local authenticated metrics
  registry and mTLS `/metrics` listener. The listener is not leader-gated;
  followers expose the state they live-apply. Live optiplex scrapes used the
  operator client certificate and node CA rather than a test-only endpoint.
- **P3:** `DEFAULT_LOG_FILTER` is `info`, and Wendelon's filter reads
  `WENDELON_LOG`; `RUST_LOG` is not the control. Settlement and participant
  phase-transition lines therefore use `info` and are present under the stock
  configuration.

### C1–C5 outcomes

- **C1 — barrier semantics:** live apply leaves the gauge at zero for prepare,
  raises it to one for logical finalize, preserves one on a rejected stale
  settlement, and returns it to zero only on the exact settlement. Recovery
  from an outstanding-debt snapshot reports one immediately.
- **C2 — withheld completion:** the ADR 0095 RF=1 control now withholds a
  participant for four independently configurable reconciliation cycles.
  Every cycle refuses the stale coordinator, retains the debt, and leaves
  progress valid. The subsequently admitted slices emit all documented phase
  fields and settle normally.
- **C3 — drift-proofing:** recovery and validated snapshot installation each
  compare the gauge directly with a one-entry catalog. The architecture guard
  proves there is no election-triggered setter; during the optiplex startup
  election, the recovered five-debt value remained five. Live apply and
  snapshot publication are the only locked-state refresh sites.
- **C4 — emission seam:** a successful live state-machine apply emits exactly
  once even though no proposer-response path is involved; a stale settlement
  and a rejected duplicate emit nothing; direct recovery replay of the same
  settlement emits nothing. The one live line carries operation/object,
  finalize log index, participant count, ownership epoch, and local node id.
- **C5 — barrier honesty:** the source guard constructs the historical false
  receipt: with one unrelated debt, another debt can arrive and the unrelated
  one can settle, returning the gauge to the same nonzero value while the run's
  debt remains. The runbook therefore rejects every nonzero baseline. The live
  zero/exclusive run below observed its settlement line inside the external
  acceptance-to-zero interval.

### Live drain evidence

The existing optiplex soak profile was checked first. Its authenticated scrape
reported five debts and zero process-lifetime settlements; polling from
14:45:36 through 14:55:07 CDT remained at five while the reclaim process was
actively reading storage. No new DROP was issued: the run was rejected because
five is not an exclusive baseline. This is the exact ambiguity the metric now
makes visible.

The evidence exhibit then used a fresh isolated profile with the same release
binary. A deliberately oversized 200,000-row trial demonstrated 0 -> 1 but did
not return to zero during its 67-second observation window; it was not reported
as drained. The final bounded fixture contained 160 rows and began at
`debts=0, settlements=0`, with no unrelated DROP activity:

- external DROP start: `2026-08-24T15:00:02.437826926-05:00`;
- DROP accepted/returned: `15:00:02.464867548` (27 ms statement time);
- authenticated scrape observed `debts=1, settlements=0` at
  `15:00:02.543647168`;
- participant transitions were logged at `15:00:02.606707`
  (`LocalL1 -> TieredL2`), `15:00:02.784829`
  (`TieredL2 -> VerifyIndexes`), and `15:00:02.983740`
  (`VerifyIndexes -> Complete`), with post-transition cursor and cumulative
  examined fields;
- the single settlement line was emitted at `15:00:02.988064`; and
- the authenticated scrape observed `debts=0, settlements=1` at
  `15:00:03.023631880`.

The acceptance-to-zero drain interval was therefore 0.559 seconds, and the
settlement line lies inside it. Both isolated profiles were stopped and
removed. No new DROP, schema mutation, or reset touched the soak profile; its
already-owed background reclaim ran while observed and may have advanced its
node-local durable cursors. Optiplex was restored to its pre-exhibit inactive
service state and original installed-binary checksum.

An incidental operational finding is separate from ADR 0096: the standard
local wrapper has a fixed 45-second readiness timeout, while recovery of the
enlarged soak profile took about 88 seconds. Transient units were used for the
exhibit; this does not affect the observability mechanism or receipt.

### Seal and implementation surface

- formatting and `git diff --check`: clean;
- Wendelon: library 1,278 passed / 0 failed / 9 ignored, architecture 177/0,
  and the full `--no-fail-fast` target and documentation sweep green;
- Toron: library 1,697/0, architecture 2,145/0, and the full
  `--no-fail-fast` target and documentation sweep green;
- `toron-tpch`: 511 passed / 0 failed / 6 ignored across its default target
  sweep; and
- no Toron source diff.

No production or cross-crate test hook was added. One private local
`#[cfg(test)]` helper, `with_observability_metrics_for_test`, injects an
isolated registry into state-machine controls; it is absent from production
and feature-enabled builds. ADR 0095's participant withholding seams were
reused unchanged.
