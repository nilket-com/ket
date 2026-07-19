# ADR 459 - close the 446 spill-scope gap: attribute q05's out-of-root spill

Re-measure the ADR 446 "12x" spill-scope gap — whole-query task spill vs grouped-root
spill — at the scale where it was observed (sf=10), on the post-458 page format, and
either seal it closed with evidence or attribute the out-of-root producer. This is the
oldest open item in the arc's queue, carried in every forward arc since 446.

## Context — what the gap is, and what is already known

- 446 (sf=10, pre-coalescing, test budget): whole-query task spill ~5.35 GB in the
  measured OFF configuration, while the grouped-chain root scope accounted only
  ~279 MB (`task_spill_delta`; reservoir 252.7 MB + runtime build 26.1 MB). Roughly
  12× of q05's spill had no attributed producer. 448 typed the two scopes
  (`whole_query_total` vs root-span delta) — a category fix, explicitly named a
  partial explanation only.
- New evidence in hand (458's sf=8 gate-zero profiles + cohort rows): whole-query
  `spill_bytes` equals `task_spill_delta` **exactly** in both arms (ON 231,643,984;
  OFF 735,589,072) — **zero out-of-root spill at the sf=8 cell**. The gap is not a
  property of the committed reservoir shape per se; if it survives anywhere, it is
  scale- or configuration-dependent.
- The profile already carries the checkpoints (`task_spill_at_*`), role fields
  (`reservoir_spill_bytes`, `runtime_build_spill_bytes`, `spill_roles_reconciled`)
  and the `spill_io` inventory — the instruments 446 lacked.

## Decision — probe first, attribute only if the gap survives

**Rail 1 — the probe at the gap's home coordinates.** One profiled child per arm at
(64 MiB task, 4 GiB reserve — predeclared non-binding headroom for sf=10 volumes,
455-style — sf=10, strict parquet, fresh spill scratch, fresh profile dirs). Per arm,
record whole-query `spill_bytes` (child row) against `task_spill_delta` (profile) and
compute the out-of-root remainder. sf=10's dataset is prepared on demand; prep cost is
part of the probe, not a hang. This is reachability/accounting classification in the
450 mold — no wall or materiality claims.

**Rail 2 — predeclared readings.**

- **Remainder ≈ 0 at sf=10 too** (as at sf=8): the gap is sealed closed — the 446
  observation is explained as the pre-448 scope confusion compounded by the pre-458
  page format, and the queue item retires with this receipt as its closure. (The 446
  numbers are not re-judged; they were measurements of that era's format and scopes.)
- **Remainder materially > 0**: the gap is real and now attributable — proceed to
  Rail 3.
- **`BudgetExhausted` at 4 GiB**: raise once to 8 GiB (explicit, recorded), the
  predeclared bounded response; a second wall halts as apparatus-inconclusive.

**Rail 3 — attribution, only if needed.** The out-of-root producer is identified by
inventorying the spill directory during the run (file names/roles/sizes by producer)
and reconciling against the profile's role fields; if the existing fields cannot name
the producer, the cut adds the minimal observability (a per-producer spill counter in
the child row, provenance-stamped like the budget fields) rather than guessing. Any
mechanism fix for the attributed producer is a follow-on cut, not a rider.

## Acceptance

- Both arms probed at (64 MiB, 4 GiB, sf=10); rows and profiles carry explicit
  coordinates; identities pass the scale-parametric gate-zero checks (OFF ==
  `generate_strict_lineitem(10.0)` count, ON strictly less) — reusing the oracle,
  not a constant.
- The receipt states, per arm: whole-query spill, root-scope delta, remainder, and
  the reading it lands in; the sf=8 zero-remainder evidence is cited alongside.
- If Rail 3 runs: the producer is named with reconciled byte counts, or the added
  counter is tested and the attribution re-run.
- No wall/materiality claims; no re-judgment of 446/451/456/457/458.

## Guardrails and risks

- **Do not conflate closure with absence.** Zero remainder at two cells retires the
  queue item for the measured configurations; the receipt scopes the claim to them.
- **sf=10 is the arc's most expensive rung** (dataset prep + the deepest reservoir);
  one child per arm, no cohort, detached with logs.
- **The reserve is held out of the way** (4 GiB predeclared; single bounded raise).
- **Do not re-judge sealed verdicts.**

## Forward arc

Closed → the queue item retires; reorientation grounding proceeds from the post-458
ledger with spill fully attributed. Attributed → the producer becomes a named
mechanism target with its own cut. The engine-side `Default` retarget,
`MAX_FILTER_BYTES` cap raise, ADR 132's comparator, q17 remeasurement, q21 residual,
q19 re-diagnosis + 438 widening, q13, Cut A (439 mirror), and the generic O(1) LRU
rewrite remain queued.
