# ADR 372 - semi/anti existence-set spill-volume (q04 + q21 common wall)

## Context

ADR 371 made the q04 scan-output miss observable and advanced it to the same producer family q21 reached after ADR 364:
the **semi/anti existence-set spill writer**. The convergence is measured, not assumed:

| query | producer | requested | available | over |
| --- | --- | ---: | ---: | ---: |
| q04 | `semi/anti existence-set spill writer` | 536,881,184 | 536,870,912 | 10,272 |
| q21 | `semi/anti existence-set spill writer` | 536,876,880 | 536,870,912 | 5,968 |

This is now a common-wall cut, not q21 momentum. Both cells are just over the unchanged 512 MiB spill budget, and q04's
sf=10 runtime is dominated by spilling. The right next step is ADR 348/363 discipline on the existence-set writer:
measure by producer/page/column, reduce if the excess is real dead payload, or name the irreducible wall. No cap bump.

q18/q22 remain at `scan output accumulator` after ADR 371 and are **not in this ADR**. They may later join this producer
after their own scan-output misses are removed.

## Decision

### Phase 0 - prove the spill-volume source before code

For q04 and q21, record a per-cell spill breakdown for the `semi/anti existence-set spill writer`:

| query | semi/anti shape | rows | pages | columns written | columns required for keys/residual | page/layout bytes | lever verdict |
| --- | --- | ---: | ---: | --- | --- | ---: | --- |
| q04 | semi (EXISTS) | (measure) | (measure) | (measure) | (measure) | (measure) | (classify) |
| q21 | semi/anti suffix | (measure) | (measure) | (measure) | (measure) | (measure) | (classify) |

Classify each lever, per ADR 348:

1. **Column projection first.** The existence set only needs join keys plus any residual columns used by
   `match_filter`. q04 is expected to be keys-only; q21 may require extra residual columns. If the current writer
   stores unused columns, add a projected existence writer and route only the measured cells through it.
2. **Page/layout second.** If the writer is already minimal, measure whether IPC/page/compression overhead can reduce
   the 6-10 KiB excess. A page/layout lever must be real and measured, not speculative.
3. **Dedup-before-spill is not free.** The existence rows are written before probing the preserved side, so early dedup
   is not a local tweak unless Phase 0 proves a two-pass membership or side-flip mechanism. Default verdict:
   not-applicable.
4. **Partitioning is fallback, not first choice.** Use a 364-style partition only if projection/layout is not enough or
   if Phase 0 shows a resident term, not spill volume, has become the true producer. Partitioning must bound the whole
   resident path if used.

### The cut

Apply only the measured-reducible lever:

- If dead columns exist: add `SpilledSemiAntiExistenceBuild` projected writes, remap existence keys into the projected
  schema, and preserve residual evaluation correctness by retaining exactly the columns the residual needs.
- If page/layout is the reducible term: apply the minimal measured encoding/layout change.
- If no lever is enough: do not force green; record a named irreducible spill-volume wall for the cell(s).

The implementation is shape-named and core de-branded. q04/q21 references belong only in plans/results/tests outside
the executor core.

## Acceptance

- **q04 and q21 sf=10, fixed 4 GB:** each is green (`pass@execute`) under the unchanged 512 MiB spill budget, OR a
  named irreducible `spill_budget_exhausted` wall with the Phase-0 evidence.
- **No cap bump:** `spill_bytes_reserved_per_query` remains 512 MiB.
- **Correctness:** persisted sf=10 `pass@compare` for any newly green cell; q04/q21 small-scale strict correctness
  remains green. Focused dev parity covers projected-vs-unprojected semi/anti existence probing, including residuals.
- **Residual contract:** q21-style non-key residuals still evaluate per candidate from the projected payload; q04's
  no-residual semi path remains keys-only.
- **Coverage table:** q04 and q21 are reported independently. One may green while the other names a different wall.
  q18/q22 are explicitly out of scope.
- **Seal gate:** `scripts/cargo-cgroup test -p toron --no-fail-fast` is mandatory; regression family remains green.
- **No qNN references in executor core; no temporary instrumentation remains; prior sf=10 greens unchanged.**

## Guardrails

- **Measure first.** No projected writer until the current written column set and required column set are recorded.
- **No cap bump.** A just-over-budget wall is not a reason to move the budget.
- **Projection must preserve residual correctness.** Keys-only is valid only for shapes with no residual payload need.
- **No q18/q22 overclaim.** They are still scan-output misses and do not enter this producer table yet.
- **Green-or-named-wall.** Moving to a new named producer is acceptable; silent OOM or unlabeled spill growth is not.
- **Engine-cut regression gate.** Full `--no-fail-fast` is required because this touches semi/anti execution and spill
  materialization.

## Risks

- **Already minimal payload.** Then the excess may be page/layout or irreducible; name it honestly.
- **Residual column pruning bug.** A projected existence payload can wrong-answer q21 if residual columns are dropped.
  Focused residual tests and persisted correctness gate it.
- **Two cells diverge.** q04 may be keys-only and q21 may need residual payload; the results table records split
  outcomes rather than forcing one mechanism.
- **I/O time remains high.** Greening memory/spill reach may not fix q04's 283s runtime. Perf remains a separate arc,
  but reduced spill volume should be recorded because it may also reduce I/O time.

## Forward arc

372 (existence-set spill-volume for q04/q21) -> remaining scan-output misses (q18/q22 and q08/q09) -> streamed inner
build spill-writer x4 (q03/q05/q10/q12) -> q02 join-output -> q11 oracle guard -> the time/perf arc (q07/q17/q19 and
green-but-slow cells vs DuckDB). The next cut is chosen by measured common producers, not by query momentum.
