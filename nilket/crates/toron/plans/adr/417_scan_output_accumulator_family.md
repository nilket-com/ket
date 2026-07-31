# ADR 417 - Scan-output accumulator family (q13/q17/q18/q21): diagnostic-first eligibility restoration

## Context

Post-416 board: 14/22 green. The largest remaining family is **four cells failing at `scan output accumulator`** -
q13 (19,016 over), q17 (26,112 over), q18 (512 over), q21 (12,416 over). The producer sits at the execution model's
foundation: `execute_bound`'s `Scan` arm materializes every scan's full output via
`materialize_scan_stream_with_budget` (`exec.rs:848-856`, guard at `:329-338`) before any parent sees a batch. These
cells fail because an over-1-GiB scan output is consumed through the **vanilla materialized arm** rather than one of
the engine's streaming bypasses. The barely-over magnitudes are a red herring - these scans sit at ~the cap at sf=10
and would blow far past it at sf=15; the cap-nudge temptation is doctrine-rejected.

**This ADR does not pre-classify the four cells as one consumer family.** The shared error label is only the *leaf
producer*. The four queries are structurally different until Phase 0 proves otherwise: q13 LeftOuter-grouped, q17
correlated-global, q18 grouped-membership semi, q21 semi/anti-chain. A recognizer is extended only where the miss is
*genuinely shared*.

**Two of the four require a historical-coverage audit, not a fresh-cut assumption:**
- **q13** was green through **ADR 369's streamed LeftOuter grouped path** - Phase 0 must explain why that recognizer
  no longer catches the current plan (plan shape drifted? eligibility fence tightened? a later ADR's normalization
  changed the shape the recognizer matches?).
- **q21** had **advanced beyond scan output by ADR 372** - Phase 0 must identify whether today's wall is a *new
  downstream scan* (progress) or *lost eligibility* (regression).
**Restoring proven machinery may be much smaller than designing a new interface** - the first lever is recognizer
restoration, not invention.

## Decision

### Phase 0 - per-query classification table (before any code)
For **each** of q13/q17/q18/q21, a row with:

| column | content |
|---|---|
| overflowing table/scan | which scan hits the accumulator (progress hooks name it instantly; all four fail in 0.5-6 s) |
| projection | the scan's projected columns/bytes |
| immediate consumer | the operator that called `execute_bound` on the scan |
| enclosing recognizer | which streaming recognizer *should* (or used to) catch this topology |
| exact recognizer miss | the specific condition that fails (shape mismatch, fence, eligibility gap) |

The table must come from the **same persisted sf=10 bind and execution path as `perf-run-one`**, not an in-memory
fixture or a reconstructed SQL shape. Capture a temporary/off-by-default bound-plan parent path for the exact
overflowing scan (plan fingerprint + scan node + parent chain through the candidate recognizer) so "immediate
consumer" and "exact miss" are artifact-backed rather than inferred from the query text.

Plus the **history audit**: q13 vs ADR 369's streamed LeftOuter grouped path (why the recognizer misses now); q21 vs
ADR 372's state (new downstream scan vs lost eligibility). Name the exact drift commit/ADR when the artifacts prove
it. If historical artifacts do not isolate one commit, record the narrowest proven commit interval and the current
failed condition rather than guessing. Record all before code.

### The cut - restore/extend eligibility, per cell, measured
- **Restoration first:** where Phase 0 shows a recognizer that previously caught the plan (q13, possibly q21), fix the
  miss (shape drift, fence) rather than building new machinery.
- **Extension second:** where a streaming-capable consumer exists but the topology was never admitted, extend that
  recognizer's eligibility - **one recognizer per genuinely-shared miss**; no umbrella rewrite across structurally
  different cells.
- **Implementation scope cap:** 417 may land one bounded recognizer mechanism (including multiple aliases that feed
  that same machinery), plus pure restoration of an already-proven path only when it introduces no second execution
  mechanism. If Phase 0 finds unrelated recognizer/mechanism changes, choose the highest-coverage bounded subset and
  name the others as follow-on ADRs. Do not turn the four-row diagnostic into four executor cuts in one seal.
- **ScanRequest parity (hard requirement):** any bypass builds its scan via `bound_scan_request` and preserves
  projection, pushed predicates, row limit, row identity, cancellation, and profiling behavior. Every newly admitted
  topology gets a **materialized-vs-streamed parity test** (same rows, same order semantics as the vanilla arm).
- **Scan spill is rejected** unless a consumer can *pull* the spilled batches - writing the scan to disk and then
  reconstructing `Vec<RecordBatch>` merely relocates the same execution-model boundary.

### Stop-line (C-spec)
If Phase 0 proves the consumers require a **general pull interface** - i.e. no per-recognizer restoration/extension
can admit these topologies without a generic streaming executor change - **417 seals as the execution-model (C)
design specification** with the four drivers' evidence. **No partial generic executor rewrite lands.** A subset
outcome is acceptable: cells whose seams are bounded get cut; cells that need the pull interface stay named and feed
the spec.

## Acceptance
- **Phase 0 recorded:** the four-row classification table + the q13/q21 history audit - before code.
- **Per-cell primary** (no family rounding): each cell independently either
  - moves off `scan output accumulator` **and** lands persisted correctness, **or**
  - advances to a **named + reproduced next wall**, **or**
  - is classified as requiring the pull interface (feeds the C-spec).
- **q17 cannot claim sf=10 pass@compare:** if it reaches output, it is recorded `reach_only` **plus** the existing
  `q17_sf10_oracle_policy_gap`; its supported oracle scale (sf=0.02) is run as **secondary** correctness evidence
  only. The board cell stays honest.
- **Parity proven:** materialized-vs-streamed parity tests for every newly admitted topology; ScanRequest behavior
  (projection/pushdown/limit/identity/cancellation/profiling) preserved. Compare all observable request fields in
  tests; opaque cancellation/trace handles are preserved structurally by using the shared `bound_scan_request`
  constructor, not rebuilt in the bypass.
- **Controls:** the 14 greens' spill-bearing set re-proven (`q03 q07 q09 q10 q12 q14 q16 q19 q20` pass@compare;
  forensic spill bands); remaining greens cheap-optional; q05/q11/q02/q22 stay named (untouched).
- **No cap/multiplier/scope change** - the four small overshoots are not nudged away; **full engine gate**
  (`--no-fail-fast` both crates + architecture after fmt); de-branded; no committed `.perf.data`.
- **Scope honesty:** per-cell outcomes stated separately; any C-spec classification is the finding, not a failure;
  q02/q22/q05/q11 remain the other named families.

## Guardrails
- **Diagnostic before cut; per-cell, not family** - the shared label is the leaf producer, nothing more, until the
  table proves a shared miss.
- **Persisted-path proof** - the classification table describes the exact persisted sf=10 plan and overflowing scan,
  not a fixture-equivalent plan.
- **Restore before invent** - ADR 369/372 machinery first; new eligibility only where restoration doesn't apply.
- **One mechanism per seal** - split unrelated recognizer changes after Phase 0.
- **ScanRequest parity** - `bound_scan_request` everywhere; parity tests per topology.
- **No scan spill without a puller** - relocating the boundary is not a fix.
- **No partial pull-interface** - the C-spec is a sealed design, not half-landed code.
- **q17 oracle honesty** - `reach_only` + policy gap; sf=0.02 secondary only.
- **Won ground is a control** - spill-bearing greens + forensic bands.

## Risks
- **The four misses are four different seams** -> per-cell outcomes; possibly a multi-ADR family - honest subsets
  beat an umbrella rewrite.
- **q13 drift or q21 history runs deep** (a later ADR's normalization changed shapes broadly) -> the history audit
  names the proven change boundary; restoration may itself need care not to regress that change's win.
- **Streamed paths change row order at a consumer that cares** -> parity tests per topology; order-bearing consumers
  audited before admission.
- **A wall behind the wall** -> reach honesty; all four were never-green-at-415 cells.
- **C-spec is the outcome for some/all** -> the spec-with-four-drivers is the most valuable version of that finding.

## Forward Arc
Cells that green here move the board toward 18/22 with q02 (join-output), q22 (writer residency), q05 (residency
diagnosis), and q11 (oracle reconciliation) remaining. Any C-spec classification becomes the opening evidence of the
pull-interface design ADR - the true execution-model rock, approached with four measured drivers instead of
speculation. The speed thread (q10's probable compact-index side-win, 12-416x DuckDB spread) stays parallel.

## Grounding evidence (file:line)
Producer: `exec.rs:299` (`materialize_scan_stream_with_budget`), `:329-338` (guard, x16 cap), `:848-856`
(`execute_bound` Scan arm; `bound_scan_request` at `:850`). 415 board: q13 1,073,760,840 / q17 1,073,767,936 / q18
1,073,742,336 / q21 1,073,754,240 vs 1,073,741,824 (`plans/impl/415_sf10_full_board_live_sweep_results.md`), q17
oracle-policy gap (same doc). Historical coverage anchors (verify in Phase 0): ADR 369 streamed LeftOuter grouped
path (q13 green), ADR 372 q21 beyond-scan-output state. Streaming bypass census starting points:
`try_streaming_join0_filter` (`exec.rs:863`), the reoriented streamed-inner chain (`exec/join/streaming.rs`), the
grouped fold + reservoir (413), scan-global-aggregate streaming (`tests/scan_global_aggregate_streaming_test.rs`).
