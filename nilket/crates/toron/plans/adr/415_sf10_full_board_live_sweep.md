# ADR 415 - sf=10 full-board live sweep (closing proof artifact)

## Context

The 401-414 arc closed the 398 mini-atlas family: q03/q10/q12 walls fell (adaptive derived build, spilled derived
build + sort cap, reservoir, index compaction), q04/q08/q19 held green, and q05 went green in 413 but was
**inconclusive at 414's re-probe** (OOM at baseline too - a reproducibility gap, not a regression). That family is
**7 of 22 cells**. The other 15 queries' sf=10 statuses are stale or have never been live-probed at this scale - and
the standing lesson from ADR 356 applies: the cached matrix is not evidence; **only live runs count**.

415 is the closing measurement: a **live, fresh, full 22-query sf=10 sweep** under the fixed scope, producing the
final board. It has three jobs:

1. **Resolve q05** - green, flaky-green (with attempt evidence), OOM-with-evidence, or a newly named deterministic
   wall; the 414 caveat must not be carried forward by implication.
2. **Sweep the other 15 cells** - refresh stale or absent evidence and discover unknown walls *before* the article
   claims the board.
3. **State the final board from fresh measurements** - per-cell classification with evidence, article-grade.

**This is a measurement ADR (398/400 pattern): no engine changes.** Any wall found is *named* - the 412 component
payloads and 408 progress hooks make walls self-describing now - and becomes a follow-on ADR, not a fix smuggled into
a sweep. An honest red row beats a lying green.

## Decision

### Preflight
- Clean release rebuild (stale-binary hazard - the standing 407/408 lesson).
- **Scratch hygiene:** clear `/tmp/toron_join_spill` before the sweep and between heavy cells; check disk headroom
  (the 414 finding: killed probes leak scratch - 21 GiB by 414, and it has already re-accumulated since). Record the
  cleanup in the doc.
- Confirm sf=10 persisted data + manifest.

### The sweep
- **Reach pass:** for each of q01..q22, one bounded run (`perf-run-one --engine toron --sf 10`, fixed 4 GB scope,
  `RuntimeMaxSec=360s`, `/usr/bin/time -v`), recording status / wall time / peak RSS / spill bytes / and on failure
  the **named producer + component payload when the engine emits one**. The command exit status and systemd result
  are retained separately so a hard OOM (`Result=oom-kill`) and timeout remain classifiable even when the killed
  process emits no JSON. Classify each cell: `pass` / `fail(named wall)` / `oom` / `timeout` /
  `blocked(plan-bind)` - a plan/bind-level reject is recorded as-is, never dodged (the honest-red doctrine).
- **q05 special handling:** run at least 2 and at most 3 attempts with fresh scratch. Two consecutive passes -> green
  (stop after the second); any pass mixed with an OOM/failure -> `flaky-green`; three OOMs -> OOM with systemd/RSS
  evidence and "q05 RSS headroom" named as the follow-on; a repeated engine failure keeps its named producer. One
  lucky pass is not enough to erase 414's reproducibility gap.
- **Correctness pass:** every completing cell gets a separately-artifacted, bounded persisted
  `correctness --sf 10 --mode strict` run - **a green without pass@compare is not banked** (the 413
  cached-partition lesson: completing is not correct). q17's known `q17_sf10_oracle_policy_gap` is re-probed live
  and classified `reach_only_no_oracle`, never green.
- **DuckDB reference pass:** collect the cheap DuckDB baseline for all 22 cells (or record a named CLI/harness gap).
  It does not block Toron classification, but the board records DuckDB time and Toron/DuckDB ratio (or a censored
  lower bound) so the closing artifact retains the normalization used by ADR 386 onward.

### The board
The results doc states the final 22-row board: per cell - status, time, peak RSS, spill, producer/payload for engine
failures, systemd evidence for hard OOMs/timeouts, correctness provenance, DuckDB time, ratio/lower bound, and the
398-baseline comparison for the atlas family. Deltas against the arc's prior greens (q03 ~16-19 s,
q10 ~105-113 s, q12 fresh, q04/q08/q19 bands) are recorded; regressions beyond band are **findings, not
re-classifications** - they name follow-ups.

### Stop-lines
- **No fixes in the sweep** - new walls are named with evidence; each becomes its own cut.
- **No cap/scope changes** - the fixed 4 GB / 64 MiB-default envelope is the whole point of the claim.
- **No cached rows** - every board cell in the doc comes from this sweep's live runs.

## Acceptance
- **All 22 cells carry a live, fresh classification** with evidence (time/RSS/spill; producer + component payload for
  engine failures; systemd result for hard OOMs/timeouts; pass@compare for greens; explicit oracle gap for q17).
- **q05 resolved** per the 2-to-3-attempt protocol - green / flaky-green / OOM / named deterministic wall - with the
  evidence table either way.
- **The atlas family re-proven live:** q03/q04/q08/q10/q12/q19 pass@compare within their bands (regressions = named
  findings).
- **All 22 DuckDB rows attempted:** times + Toron/DuckDB ratios/lower bounds are recorded, or a named reference-run
  gap explains a missing value. DuckDB availability does not change a Toron status.
- **No engine source changes** (results doc + any run scripts only); no cap bump; scratch cleaned before/after;
  full-gate battery not required (measurement-only) but `fmt --check` + `git diff --check` on the doc.
- **The final board table** - the closing, article-grade artifact - with every non-green cell's follow-on named.

## Guardrails
- **Live-only** - no matrix/cache rows (ADR 356 lesson).
- **Honest classification** - blocked/unsupported recorded as-is; no query rewriting, no canonical dodges.
- **pass@compare or it isn't green** - correctness rail on every completing cell; q17's live oracle-policy rejection
  is `reach_only_no_oracle`, not green.
- **Named walls only** - producer + payload for engine-reported failures; systemd result + RSS for hard OOMs/timeouts;
  no diagnosis-free generic `fail` rows.
- **Scratch + disk hygiene** - clear before, monitor between heavy cells, clear after; disk-quota ghosts are a known
  failure mode.
- **Measurement discipline** - one bounded attempt per cell (q05 excepted); this is a census, not a perf rail; timing
  is recorded but bands, not medians, are claimed.

## Risks
- **Unknown walls in the 15 refreshed cells** -> the point of the sweep; each named with available evidence ->
  follow-on ADRs.
- **q05 stays OOM** -> honest reclassification + "q05 RSS headroom" named (413-lineage follow-on); the board says
  "6 green + q05 borderline," not a rounded-up 7.
- **Sweep duration** (worst case ~22 x 6 min reach + bounded correctness for greens) -> bounded windows; heavy
  correctness runs (q19 ~100 s+, q05 ~230 s) budgeted; the sweep can checkpoint per-cell artifacts so a session
  break loses nothing.
- **Scratch/disk blowup mid-sweep** -> hygiene between heavy cells; a disk failure is retried after cleanup, not
  recorded as a query wall (the 414 red-herring lesson).
- **Band drift on prior greens** (cross-session variance) -> bands not medians; only out-of-band moves become
  findings.

## Forward Arc
415's board is the article's evidence base and the arc's closing state: what is green (with proof), what walls remain
(with payloads), and what each residual's named next cut is. After it: the speed thread (q05 exchange legs, q10
~105 s, q08/q19), external sort, decode residual, q05 RSS headroom if the sweep confirms it, and any new walls the
other 15 cells surface. The reach story - opaque timeouts to a fully-classified board under a fixed 4 GB envelope -
is complete either way.

## Grounding evidence (file:line)
All 22 queries dispatch in the harness (`toron-tpch/src/lib.rs` query table, q01..q22). Live-not-cached:
`reference_matrix_command_is_sf_agnostic_cache_render` / ADR 356 lesson. The 398 mini-atlas pattern:
`plans/impl/398_sf10_post_397_mini_atlas.md`. q05 caveat + baseline OOM isolation + 21 GiB scratch finding:
`plans/impl/414_q12_spilled_index_compaction_results.md`. Component payloads: 412 (`BudgetExhaustedDetail`); scan
progress hooks: 408. Fixed scope: `MemoryMax=4G, MemoryHigh=4G, TasksMax=512, RuntimeMaxSec=360s` (398). Scratch:
`/tmp/toron_join_spill` (1.3 GB re-accumulated at 415 planning, cleared pre-sweep).
