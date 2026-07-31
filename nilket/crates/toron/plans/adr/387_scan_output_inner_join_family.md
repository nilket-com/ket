# ADR 387 - scan-output streaming for the inner/join family

## Status
Proposed.

## Context

ADR 386 refreshed the post-385 sf=10 board and named the next repeated producer:

| query | class | producer | bytes | notes |
| --- | --- | --- | --- | --- |
| q03 | scan-output memory wall | `scan output accumulator` | 1,073,790,848 / 1,073,741,824 | spills 1.43 GiB before wall |
| q05 | scan-output memory wall | `scan output accumulator` | 1,073,790,848 / 1,073,741,824 | spills 1.43 GiB before wall |
| q08 | scan-output memory wall | `scan output accumulator` | 1,073,762,000 / 1,073,741,824 | no spill; cleanest canary |
| q10 | scan-output memory wall | `scan output accumulator` | 1,073,790,848 / 1,073,741,824 | spills 1.43 GiB before wall |

q08 is the cleanest canary: it walls quickly, has no spill confounder, and validates the carried-forward inner-chain
memory-tail class. q03/q05/q10 share the same producer but have downstream spill work, so they are coverage targets
after q08 proves the miss.

This is the next scan-output-family cut after ADRs 361, 367, 368, 369, 370, and 371. The discipline stays the same:
diagnose the exact scan edge first, remove only the measured miss, then claim only live measured coverage.

## Decision

Implement a diagnostic-first scan-output streaming extension for the q03/q05/q08/q10 inner/join family.

### Phase 0 - producer table before code

Create a per-query producer table before changing executor code:

| query | current producer | canary/control | suspected consumer shape | miss class | producer after | verdict |
| --- | --- | --- | --- | --- | --- | --- |
| q08 | `scan output accumulator` | canary | inner-chain / join+aggregate | Phase 0 | re-measure | canary |
| q03 | `scan output accumulator` | coverage | join/filter/group with spilled inner build | Phase 0 | re-measure | covered-if-shared |
| q05 | `scan output accumulator` | coverage | join/filter/group with spilled inner build | Phase 0 | re-measure | covered-if-shared |
| q10 | `scan output accumulator` | coverage | join/filter/group with spilled inner build | Phase 0 | re-measure | covered-if-shared |

For each row, identify:

- which scan materializes into the accumulator;
- which consumer receives the materialized scan;
- whether an existing streamed path matched but left one edge materialized, or the recognizer missed the plan shape;
- whether the persisted Parquet plan shape differs from the in-memory/lower-scale path;
- whether the scan has projection, filter, exchange, runtime filter, row identity, or order requirements beyond the
  prior scan-streaming paths.

The fix is valid only if it removes the measured q08 miss. q03/q05/q10 are covered only if they share the miss or the
implementation explicitly maps their distinct miss.

### Mechanism

Reuse the existing shared `bound_scan_request` / direct `storage.scan` machinery and the existing streaming join/fold
substrate. The implementation should stream the materialized scan edge into its join/aggregate consumer rather than
calling `materialize_scan_stream -> Vec`.

This ADR is not a general scan-streaming recognizer rewrite. It is a shape-fenced extension for the measured
q03/q05/q08/q10 family.

### Tight Green Definition

For each target cell:

- **green** requires scan-output producer gone, no downstream producer, and live persisted sf=10 correctness;
- **advanced** means scan-output producer is gone but a named downstream producer appears (join-output, spill-volume,
  spill resident, exchange accumulator, aggregate/group state, or another 332 producer);
- **unchanged** means the same scan-output producer remains after the attempted extension.

Wall-moved-to-named-producer is a successful outcome. It names the next real producer; it is not a green claim.

## Acceptance

- Phase 0 producer table recorded before implementation.
- q08 canary:
  - green with persisted sf=10 correctness; OR
  - advanced to a named downstream producer; OR
  - unchanged with the measured reason recorded.
- q03/q05/q10 remeasured live after the cut and classified as green / advanced / unchanged. Claim only measured cells.
- Persisted sf=10 correctness for every new pass row.
- Small-scale strict/dev parity for the new streaming shape, including column order and residual/filter semantics.
- ScanRequest parity through the shared builder; no dropped projection/PBC/runtime-filter/row-identity/morsel/limit fields.
- No cap bump; no budget constant changes.
- `scripts/cargo-cgroup test -p toron --no-fail-fast` required because this is an engine cut.
- De-branded executor code; prior greens q04/q06/q14/q19 and carried greens unchanged.

## Guardrails

- **Diagnostic-first:** no code until the q08 scan edge and miss class are known.
- **No overclaim:** q03/q05/q10 are covered only by live remeasure.
- **No cached evidence:** no `matrix` or `correctness-matrix`.
- **Tight green definition:** scan-output-gone alone is not green.
- **Downstream honesty:** if q03/q05/q10 move to spill/join-output, record that producer rather than forcing green.
- **No general recognizer:** keep the extension shape-fenced to the measured family.

## Risks

- **q08 is a different miss from q03/q05/q10:** then 387 may be q08-only plus a split table. That is acceptable.
- **Scan streaming moves wall to join-output/spill:** successful wall-move; next ADR targets the repeated downstream
  producer.
- **Persisted-plan wrapper drift:** the shared `bound_scan_request` and parity tests must catch dropped fields or wrong
  answers.
- **Small-scale perf regression:** strict/dev parity and sf=1 controls should detect path-selection mistakes if the new
  streamed path is enabled where materialization was cheaper.

## Forward Arc

ADR 387 should either green part of the q03/q05/q08/q10 family or name the downstream producer now hidden behind the
scan-output accumulator. After that, choose from the fresh wall ranking: remaining scan-output siblings, q21's tiny
resident wall, q12's partition-group residual, or the q07/q17/q19 performance arc.
