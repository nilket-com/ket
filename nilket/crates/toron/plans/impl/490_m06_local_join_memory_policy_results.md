# ADR 490 implementation receipt

The raw-memory policy boundary and its safety scaffolding are retained, but local
hash admission did **not** ship.  The first live candidate cell at `(2 GiB task,
2 GiB spill reserve, sf=8)` remained on the exchanged `HashPartitioned` plan, so
the predeclared **not admitted** outcome fired and the binder reader was reverted.

## Live policy rail

The fresh parent-run row at the exact coordinate passed correctness but did not prove
the policy fired:

| plan fingerprint | output | peak RSS | spill | execute |
|---|---:|---:|---:|---:|
| `144a330ac4a17555` (standing exchanged shape) | 1 row, standing digest | 475,512 KiB | 88,559,536 B | 20,597.0 ms |

The row carries explicit `memory_per_task=2,147,483,648` and
`spill_bytes_reserved=2,147,483,648` provenance.  It is not a local-admission
success: nonzero spill and the unchanged fingerprint fail Rail 3's structural plan
proof.  No A/B pricing was therefore authorized.

## Kept rails

- `estimated_hash_join_build_bytes` remains the shared raw estimate used by the
  executor and future admission code; its m06 `12,000,000 * (112 + 8) =
  1,440,000,000` boundary test remains green.
- ADR 489's coordinator clear and three-worker `JoinInputNotExchanged` negative rail
  remain intact.  No local shape was allowed to reach distributed fragmentation.
- The attempted binder reader was committed only after a structural unit test, then
  reverted when the actual m06 cell did not admit.  The reverted candidate and its
  architecture-bound update are retained in history as `faea4b31` / `04688b2e`.

## Forward

Before a renewed admission attempt, diagnose which precondition declines the live m06
bind (strategy intent, selected build facts, or probe-linearity) with an explicit
bound-plan receipt.  Do not relax the raw estimate, coordinate, or distributed rails
to make the cell pass.
