# ADR 412 - q12 spill resident-footprint policy (component breakdown first)

## Context

The sf=10 board after 411: q03 and q10 green; **q12 fails deterministically in ~8 s** at
`streamed inner join build spill resident state`, 1,073,809,552 / 1,073,741,824 - **67,728 bytes over** the x16
retained-footprint cap. q05 (reservoir) remains the capstone. 410 classified q12's edge as a genuine retained-state
shortfall at `SpilledInnerJoinBuild::guard_resident_state`; 412 does the component breakdown and cuts the decided
policy - **not** a cap bump.

**Grounded mechanics.** The guard (`exec/join/spill.rs:927`) sums four components against
`join_spill_retained_footprint_memory_cap` (64 MiB task default x16 = 1 GiB):

| component | what it is | lifecycle |
|---|---|---|
| `resident_index_bytes` | in-memory row index over spilled pages | zeroed + rebuilt at load (`:827`) |
| `writer_peak` | max spill-page plaintext length high-water (`:783`) | never reset after `finish` |
| `JOIN_SPILL_PAGE_SIZE` | one-page reserve, **64 KiB** (`:4`) | constant |
| `pinned_decoded_bytes` | decoded pages pinned by an **optional** optimization (`:901`) | set only if pinning commits |

Guard call sites: during write (`:815`), during index rebuild (`:847`), **after the pinning optimization commits
(`:903`)**, and at finish (`:924`).

**The load-bearing hypothesis (Phase 0 decides, not assumes):** the overshoot is suspiciously close to one page
reserve - 67,728 ~= 65,536 + 2,192. Current code already includes both `writer_peak` and `JOIN_SPILL_PAGE_SIZE` in
the pinning pre-check, and the read loop checks the exact decoded-byte formula before committing pinned pages. So the
question is **not** "did the pre-check forget the reserve?" The question is which guard call site trips and which
component owns the edge:

- write/rebuild/finish guard -> resident index / writer peak / reserve accounting;
- post-pin guard -> a declinable optimization still needs to decline-and-unpin, because the LRU fallback exists;
- genuine pressure -> a real shedding mechanism, not a cap tweak.

This keeps the sharp arithmetic but grounds it in the current code. Pinning remains a ranked suspect only if Phase 0
proves the failing call site is post-pin or that estimated page bytes and actual decoded bytes diverge in a way the
existing loop does not safely decline.

Ranked alternatives: `resident_index_bytes` barely exceeds the retained footprint, `writer_peak` remains accounted
after the writer is no longer live (release accounting), reserve double-counting, pinning decline semantics, or
genuine retained pressure (a real shedding/eviction policy - bigger, named honestly).

## Decision

### Phase 0 - component breakdown at the failure point (before any fix)
1. **Instrument the split durably:** extend the guard's failure payload (or an off-by-default 408-style hook) so the
   error/diagnostic carries the four component values, not just the sum. This is a keepable diagnostic improvement -
   the wall told us the sum for two ADRs; it should have told us the split.
2. **Reproduce q12** under the fixed scope and record: the failing **call site** (write / rebuild / post-pin `:903` /
   finish), the four component values, and whether the pinning path was attempted, declined, committed, or was not in
   the failing path.
3. **Classify:**
   - **(a) pinning decline mismatch** -> consistency fix: if post-pin can still trip the guard, make it
     decline-and-unpin (fall back to LRU reads) instead of erroring - pinning is an optimization with a defined
     fallback;
   - **(b) `writer_peak` staleness or non-live reserve accounting** -> release/re-scope only when the bytes are
     provably no longer live (accounting correction,
     not a cap change);
   - **(c) reserve granularity** -> only if the reserve is provably double-counted; a "reserve is too big" conclusion
     is a cap bump in disguise -> rejected;
   - **(d) genuine retained pressure** -> a real shedding policy (evict pinned decoded pages under pressure, spill the
     index, ...) - if small, cut it; if mechanism-sized, seal the diagnostic and name it.

### Stop-lines
- **No cap bump:** the x16 multiplier, 64 MiB task default, and 4 GB scope are untouched; "just raise the retained
  cap" is rejected even though 67,728 bytes would vanish into any bump.
- **Optimization must not fail the query:** whatever lands, a *declinable* optimization (pinning) erroring a query
  that its fallback could complete is treated as the bug, not the budget.
- **Mechanism-sized shedding** -> seal diagnostic, name the cut; do not smuggle an eviction subsystem into a policy
  ADR.

## Acceptance
- **Phase 0 recorded:** failing call site + four-component split + pinning-path status, before code.
- **Primary: q12 sf=10 moves off the resident-state wall**: persisted correctness `pass@compare`, **or a named +
  reproduced next wall** (q12 was time/no-row two ADRs ago; walls behind walls remain possible).
- **Discriminating tests:** the classified case reproduced in a unit/integration test that fails on pre-412 code
  (e.g. pin-commit-then-guard-fail becomes decline-and-complete); a genuinely-over retained state (all components
  legitimately large) **still fails** with the same producer - the guard is corrected, not removed.
- **Component payload kept:** the guard's failure diagnostics carry the component split from now on.
- **Controls:** q03/q04/q08/q10/q19 sf=10 pass@compare (q10 newly green - now a control); q03 sf=1 four-way in band
  (fp `666e0ac028036f70`, spill 0). q19 especially: it exercises this exact spill machinery and must keep its working
  spill.
- **Full engine gate:** `--no-fail-fast` both crates + architecture_test after fmt; de-branded; no committed
  `.perf.data`.
- **Scope honesty:** one guard's policy/consistency corrected; genuine-pressure shedding (if that's the finding)
  named as its own mechanism; q05 reservoir unchanged as the capstone.

## Guardrails
- **Breakdown before cut** - the fix is decided by the component split, not by which fix is smallest.
- **No cap/multiplier/scope change** - the budget surfaces are frozen; only formulas/accounting/decline behavior may
  change.
- **Graceful-fallback doctrine** - declinable optimizations decline; they do not fail queries their fallback could
  serve.
- **Guard stays armed** - a legitimately-over retained state must still fail; lock it with a test.
- **Won ground is a control** - q03 (both scales), q10, q04/q08/q19 (q19 = the working-spill sentinel).

## Risks
- **Hypothesis wrong, pressure genuine** -> (d): small shedding policy if bounded, else sealed diagnostic + named
  mechanism - an honest outcome.
- **Next wall behind q12's guard** -> reach-board honesty: name + reproduce.
- **Accounting or guard changes affect write-path behavior** (same guard used at `:815`/`:847`) -> component tests
  per call site; q19 control.
- **Diagnostic payload churn** (error text scanned by tests/harness) -> keep the producer string stable; extend, don't
  rename.

## Forward Arc
q12 landing leaves **q05 as the last heavy cell**, with the reservoir/non-terminal completion (413) closing the sf=10
board - every other >360 s cell from 398's atlas now green or this one. Then the named backlog: external sort, q08/q10
execution speed, decode residual, and the article's third act is complete: opaque timeouts -> named walls ->
completions, in size order. 412 also leaves the spill guard permanently more diagnosable (component payload).

## Grounding evidence (file:line)
Guard + components: `exec/join/spill.rs:927` (`guard_resident_state`: `resident_index_bytes + writer_peak +
JOIN_SPILL_PAGE_SIZE + pinned_decoded_bytes` vs x16 cap), `:4` (`JOIN_SPILL_PAGE_SIZE = 64 * 1024`), `:783`
(`writer_peak` high-water), `:827` (index zero+rebuild at load), `:872-903` (pinning pre-check includes
`resident_index_bytes + writer_peak + JOIN_SPILL_PAGE_SIZE + loaded_page_bytes_estimate`; read loop checks the same
fixed terms plus actual decoded bytes before commit; post-pin guard `:903` should be redundant unless Phase 0 proves
otherwise), call sites `:815`/`:847`/`:903`/`:924`. Cap: `join_spill_retained_footprint_memory_cap` =
`memory_cap x 16` (`exec/join.rs:3691`, multiplier `:33`), 64 MiB task default (`distributed/budget.rs:95`). q12 wall:
1,073,809,552 / 1,073,741,824 = 67,728 over ~= 65,536 (one page reserve) + 2,192; deterministic ~8 s
(410/411 results). 410 classification: genuine retained-state shortfall at the guard
(`plans/impl/410_q10_reoriented_build_accumulator_completion_results.md`).
