# impl 0538 - correction: the first published atlas discharges timing deltas only

`target/tpch/adr538_timing_atlas/timing_atlas_0e5f8a11_run2.jsonl` was published with
`timing_authority=true` from binary `0e5f8a119c78` on a quiescent host. **It does not
discharge ADR 538's acceptance contract.** This record states what it does and does not
establish. The artifact is preserved unchanged: it is not hand-edited, resealed, or
republished.

## What it establishes

Matched-envelope paired timing over the 21 `executes_identical` cells, strict sf=8 persisted,
2 GiB aggregate envelope on both arms, two workers over loopback TCP, 7 measured pairs per
cell after 2 discarded warmups, arm order parity-fixed and alternating, host quiescent at
start and at seal, replay clean, 21 cells finalized.

- **18 distinguished, 3 indistinguishable** (q02, q13, q16).
- **15 distinguished-slower**, so *S* clears the |*S*| ≥ 5 minimum.
- **3 distinguished-faster**: q17 (−785 ms, 0.81×), q04 (−201 ms, 0.94×), q22 (−170 ms, 0.74×).
- Widest gaps: q07 6.09× (+23.7 s), q01 4.05× (+16.1 s), q12 3.78×, q03 2.92×, q05 2.36×.

## What it does not establish

**No dominance verdict is authorized.** ADR 538 Decision 4 requires overhead priced from
receipts under an additivity rule, and the Forward selects a branch by which component is
*leading* — at least 50% of a cell's median paired delta — in at least ⌈2/3·|S|⌉ cells. The
journal contains **zero attribution rows**: pair records carry elapsed, delta and ratio;
child records carry elapsed, row count and validity. Nothing describes where the time goes.

Acceptance 4 (overhead attribution reconciles additively) and Acceptance 6 (the fork is
answered from the data) are therefore **unmet**. Inferring "dispatch/transport dominance"
from a 6× ratio would be exactly the unreceipted inference this ADR forbids.

This is an **incomplete implementation, not grounds to rescope the ADR**. Acceptance 4 and the
dominance arithmetic were fixed before measurement; amending them after seeing an 18/15 result
would be a post-hoc weakening of a promoted contract.

## Apparatus defect closed

The deeper fault is that the apparatus *published* an artifact it should have withheld: a
journal with no component receipts satisfied every authority condition. Replay now carries an
`attribution_receipts_missing` veto, and a component claiming additivity without a
`non_overlapping` or `critical_path` basis raises `non_additive_components`. Three tests pin
it: complete 21-cell timing with no receipts is withheld; a concurrent-task-sum basis cannot
produce a dominance verdict; only receipt-backed components with an admissible basis satisfy
the requirement. The first of those reproduces the published artifact's exact shape.

## Forward

A replacement authoritative run follows, with component receipts, from a rebuilt clean binary
on a re-quiesced host. This artifact remains as preliminary timing evidence and as the
provenance of the veto that now prevents its recurrence. It is not superseded data — its
deltas are sound — but it is not the ADR's deliverable.
