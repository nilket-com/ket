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
